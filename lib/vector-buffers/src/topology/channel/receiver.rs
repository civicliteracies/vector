use std::{
    mem,
    pin::Pin,
    task::{Context, Poll, ready},
};

use async_recursion::async_recursion;
use futures::Stream;
use tokio::select;
use tokio_util::sync::ReusableBoxFuture;
use vector_common::internal_event::emit;

use super::limited_queue::LimitedReceiver;
use crate::{
    Bufferable,
    buffer_usage_data::BufferUsageHandle,
    variants::disk_v2::{self, ProductionFilesystem},
};

/// Adapter for papering over various receiver backends.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum ReceiverAdapter<T: Bufferable> {
    /// The in-memory channel buffer.
    InMemory(LimitedReceiver<T>),

    /// The disk v2 buffer.
    DiskV2(disk_v2::BufferReader<T, ProductionFilesystem>),
}

impl<T: Bufferable> From<LimitedReceiver<T>> for ReceiverAdapter<T> {
    fn from(v: LimitedReceiver<T>) -> Self {
        Self::InMemory(v)
    }
}

impl<T: Bufferable> From<disk_v2::BufferReader<T, ProductionFilesystem>> for ReceiverAdapter<T> {
    fn from(v: disk_v2::BufferReader<T, ProductionFilesystem>) -> Self {
        Self::DiskV2(v)
    }
}

impl<T> ReceiverAdapter<T>
where
    T: Bufferable,
{
    pub(crate) async fn next(&mut self) -> Option<T> {
        match self {
            ReceiverAdapter::InMemory(rx) => rx.next().await,
            ReceiverAdapter::DiskV2(reader) => loop {
                match reader.next().await {
                    Ok(result) => break result,
                    Err(e) => match e.as_recoverable_error() {
                        Some(re) => {
                            // If we've hit a recoverable error, we'll emit an event to indicate as much but we'll still
                            // keep trying to read the next available record.
                            emit(re);
                        }
                        None => panic!("Reader encountered unrecoverable error: {e:?}"),
                    },
                }
            },
        }
    }

    /// Waits until every item this receiver returned has been acknowledged, once `next` has
    /// returned `None`.
    ///
    /// An in-memory channel holds no record of what it returned, so it has nothing to wait for.
    /// A disk buffer's reader applies acknowledgements to its ledger, and records left
    /// unacknowledged when it is dropped are read again when the buffer is reopened. An I/O error
    /// there is unrecoverable, as it is to `next`.
    pub(crate) async fn settle(&mut self) {
        match self {
            ReceiverAdapter::InMemory(_) => {}
            ReceiverAdapter::DiskV2(reader) => {
                if let Err(e) = reader.settle().await {
                    panic!("Reader encountered unrecoverable error: {e:?}");
                }
            }
        }
    }
}

/// A buffer receiver.
///
/// The receiver handles retrieving events from the buffer, regardless of the overall buffer configuration.
///
/// If a buffer was configured to operate in "overflow" mode, then the receiver will be responsible
/// for querying the overflow buffer as well.  The ordering of events when operating in "overflow"
/// is undefined, as the receiver will try to manage polling both its own buffer, as well as the
/// overflow buffer, in order to fairly balance throughput.
#[derive(Debug)]
pub struct BufferReceiver<T: Bufferable> {
    base: ReceiverAdapter<T>,
    overflow: Option<Box<BufferReceiver<T>>>,
    instrumentation: Option<BufferUsageHandle>,
}

impl<T: Bufferable> BufferReceiver<T> {
    /// Creates a new [`BufferReceiver`] wrapping the given channel receiver.
    pub fn new(base: ReceiverAdapter<T>) -> Self {
        Self {
            base,
            overflow: None,
            instrumentation: None,
        }
    }

    /// Creates a new [`BufferReceiver`] wrapping the given channel receiver and overflow receiver.
    pub fn with_overflow(base: ReceiverAdapter<T>, overflow: BufferReceiver<T>) -> Self {
        Self {
            base,
            overflow: Some(Box::new(overflow)),
            instrumentation: None,
        }
    }

    /// Converts this receiver into an overflowing receiver using the given `BufferSender<T>`.
    ///
    /// Note: this resets the internal state of this sender, and so this should not be called except
    /// when initially constructing `BufferSender<T>`.
    #[cfg(test)]
    pub fn switch_to_overflow(&mut self, overflow: BufferReceiver<T>) {
        self.overflow = Some(Box::new(overflow));
    }

    /// Configures this receiver to instrument the items passing through it.
    pub fn with_usage_instrumentation(&mut self, handle: BufferUsageHandle) {
        self.instrumentation = Some(handle);
    }

    #[async_recursion]
    pub async fn next(&mut self) -> Option<T> {
        // We want to poll both our base and overflow receivers without waiting for one or the
        // other to entirely drain before checking the other.  This ensures that we're fairly
        // servicing both receivers, and avoiding stalls in one or the other.
        //
        // This is primarily important in situations where an overflow-triggering event has
        // occurred, and is over, and items are flowing through the base receiver.  If we waited to
        // entirely drain the overflow receiver, we might cause another small stall of the pipeline
        // attached to the base receiver.
        let overflow = self.overflow.as_mut().map(Pin::new);

        let (item, from_base) = match overflow {
            None => match self.base.next().await {
                Some(item) => (item, true),
                None => return None,
            },
            Some(mut overflow) => {
                select! {
                    Some(item) = overflow.next() => (item, false),
                    Some(item) = self.base.next() => (item, true),
                    else => return None,
                }
            }
        };

        // If instrumentation is enabled, and we got the item from the base receiver, then and only
        // then do we track sending the event out.
        if let Some(handle) = self.instrumentation.as_ref()
            && from_base
        {
            handle.increment_sent_event_count_and_byte_size(
                item.event_count() as u64,
                item.size_of() as u64,
            );
        }

        Some(item)
    }

    /// Waits until every item this receiver and its overflow returned has been acknowledged.
    ///
    /// Call this once `next` has returned `None`. A disk buffer ends its reader's input as soon as
    /// the writer is done and every record has been read, while the records it returned may still
    /// be unacknowledged; acknowledgements reach its ledger only through its reader, so a receiver
    /// dropped without settling leaves those records to be read again when the buffer is reopened.
    #[async_recursion]
    pub async fn settle(&mut self) {
        self.base.settle().await;
        if let Some(overflow) = self.overflow.as_mut() {
            overflow.settle().await;
        }
    }

    pub fn into_stream(self) -> BufferReceiverStream<T> {
        BufferReceiverStream::new(self)
    }
}

#[allow(clippy::large_enum_variant)]
enum StreamState<T: Bufferable> {
    Idle(BufferReceiver<T>),
    Polling,
    /// The receiver returned `None`. It is kept so that [`BufferReceiverStream::settle`] can wait
    /// for the acknowledgements of the items it returned.
    Ended(BufferReceiver<T>),
}

pub struct BufferReceiverStream<T: Bufferable> {
    state: StreamState<T>,
    recv_fut: ReusableBoxFuture<'static, (Option<T>, BufferReceiver<T>)>,
}

impl<T: Bufferable> BufferReceiverStream<T> {
    pub fn new(receiver: BufferReceiver<T>) -> Self {
        Self {
            state: StreamState::Idle(receiver),
            recv_fut: ReusableBoxFuture::new(make_recv_future(None)),
        }
    }

    /// Waits until every item the stream returned has been acknowledged, if the stream has ended.
    ///
    /// A consumer that read the stream to its end calls this before dropping the stream, after it
    /// has delivered or dropped every item: a disk buffer ends the stream once its writer is done
    /// and every record has been read, and applies the acknowledgements of the records it returned
    /// only while its reader is alive. A stream that has not ended is left as it is: its reader
    /// applies those acknowledgements when it is read again.
    pub async fn settle(&mut self) {
        if let StreamState::Ended(receiver) = &mut self.state {
            receiver.settle().await;
        }
    }
}

impl<T: Bufferable> Stream for BufferReceiverStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match mem::replace(&mut self.state, StreamState::Polling) {
                s @ StreamState::Ended(_) => {
                    self.state = s;
                    return Poll::Ready(None);
                }
                StreamState::Idle(receiver) => {
                    self.recv_fut.set(make_recv_future(Some(receiver)));
                }
                StreamState::Polling => {
                    let (result, receiver) = ready!(self.recv_fut.poll(cx));
                    self.state = if result.is_none() {
                        StreamState::Ended(receiver)
                    } else {
                        StreamState::Idle(receiver)
                    };

                    return Poll::Ready(result);
                }
            }
        }
    }
}

async fn make_recv_future<T: Bufferable>(
    receiver: Option<BufferReceiver<T>>,
) -> (Option<T>, BufferReceiver<T>) {
    match receiver {
        None => panic!("invalid to poll future in uninitialized state"),
        Some(mut receiver) => {
            let result = receiver.next().await;
            (result, receiver)
        }
    }
}
