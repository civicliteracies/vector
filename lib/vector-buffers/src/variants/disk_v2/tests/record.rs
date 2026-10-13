use std::{
    io::{self, Cursor},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::{
    test::SizedRecord,
    variants::disk_v2::{
        io::{AsyncFile, Metadata},
        reader::RecordReader,
        writer::RecordWriter,
    },
};

/// A file whose writes land only when it is flushed, as a `tokio::fs::File` accepts a write and
/// carries it out on a blocking thread after `write` has returned. `landed` holds what a reader of
/// the file can see.
#[derive(Default)]
struct WrittenAtFlush {
    accepted: Vec<u8>,
    landed: Arc<Mutex<Vec<u8>>>,
}

impl AsyncRead for WrittenAtFlush {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for WrittenAtFlush {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.accepted.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let accepted = std::mem::take(&mut self.accepted);
        self.landed
            .lock()
            .expect("the test holds the lock on one thread")
            .extend(accepted);
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl AsyncFile for WrittenAtFlush {
    async fn metadata(&self) -> io::Result<Metadata> {
        Ok(Metadata { len: 0 })
    }

    async fn truncate(&self, _size: u64) -> io::Result<()> {
        Ok(())
    }

    async fn sync_all(&self) -> io::Result<()> {
        Ok(())
    }
}

fn landed_len(landed: &Mutex<Vec<u8>>) -> u64 {
    let landed = landed
        .lock()
        .expect("the test holds the lock on one thread");
    u64::try_from(landed.len()).expect("a test record is far smaller than 2^64 bytes")
}

/// The writer reports the bytes of a flush to the ledger and wakes the reader, which then reads
/// the data file: the bytes it reports must have landed in the file by then. A reader woken before
/// they land finds nothing to read and waits for a wake-up that only the next record brings.
#[tokio::test]
async fn bytes_reported_flushed_have_landed_in_the_file() {
    let file = WrittenAtFlush::default();
    let landed = Arc::clone(&file.landed);
    let mut record_writer = RecordWriter::new(file, 0, 16_384, u64::MAX, 2048);

    let (bytes_written, implicit_flush) = record_writer
        .write_record(1, SizedRecord::new(73))
        .await
        .expect("write should not fail");
    assert_eq!(implicit_flush, None);

    let flushed = record_writer
        .flush()
        .await
        .expect("flush should not fail")
        .expect("the buffered record is flushed");

    assert_eq!(
        flushed.bytes_flushed,
        u64::try_from(bytes_written).expect("a test record is far smaller than 2^64 bytes")
    );
    assert_eq!(landed_len(&landed), flushed.bytes_flushed);
}

/// A record at least as large as the write buffer bypasses it and is reported flushed by the write
/// itself: it must have landed in the file by then too.
#[tokio::test]
async fn a_record_written_past_the_write_buffer_has_landed_when_it_is_reported_flushed() {
    let file = WrittenAtFlush::default();
    let landed = Arc::clone(&file.landed);
    let mut record_writer = RecordWriter::new(file, 0, 64, u64::MAX, 2048);

    let (bytes_written, implicit_flush) = record_writer
        .write_record(1, SizedRecord::new(73))
        .await
        .expect("write should not fail");
    let flushed = implicit_flush.expect("a record larger than the write buffer is written through");

    assert_eq!(
        flushed.bytes_flushed,
        u64::try_from(bytes_written).expect("a test record is far smaller than 2^64 bytes")
    );
    assert_eq!(landed_len(&landed), flushed.bytes_flushed);
}

#[tokio::test]
async fn roundtrip_through_record_writer_and_record_reader() {
    // Create a duplex stream that's more than big enough to ship a record through.
    let (writer_io, reader_io) = tokio::io::duplex(4096);

    let mut record_writer = RecordWriter::new(writer_io, 0, 16_384, u64::MAX, 2048);
    let mut record_reader = RecordReader::new(reader_io);

    let record = SizedRecord::new(73);

    let (bytes_written, flush_result) = record_writer
        .write_record(314, record.clone())
        .await
        .expect("write should not fail");
    record_writer.flush().await.expect("flush should not fail");

    let read_token = record_reader
        .try_next_record(false)
        .await
        .expect("read should not fail");
    assert!(read_token.is_some());

    let read_token = read_token.unwrap();
    assert_eq!(bytes_written, read_token.record_bytes());
    assert_eq!(flush_result, None);
    assert_eq!(314, read_token.record_id());

    let roundtrip_record = record_reader
        .read_record(read_token)
        .expect("read should not fail");
    assert_eq!(record, roundtrip_record);
}

#[tokio::test]
async fn record_reader_always_returns_none_when_no_data() {
    let reader_io = Cursor::new(Vec::new());

    let mut record_reader = RecordReader::<_, SizedRecord>::new(reader_io);
    let read_token = record_reader
        .try_next_record(false)
        .await
        .expect("read should not fail");
    assert!(read_token.is_none());
}
