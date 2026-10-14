use async_trait::async_trait;
use futures_util::{FutureExt, StreamExt, future, stream::BoxStream};
use vector_lib::{
    config::{AcknowledgementsConfig, Input},
    configurable::configurable_component,
    event::{Event, EventFinalizers},
    finalization::Finalizable,
    sink::{StreamSink, VectorSink},
};

use crate::{
    SourceSender,
    config::{SinkConfig, SinkContext},
    sinks::Healthcheck,
};

/// Configuration for the `test_holding` sink.
///
/// The sink forwards each event as it reads it, but holds the event's finalizers until its input
/// ends, as a batching sink holds its open batch until a flush. Its events are acknowledged only
/// once its input has ended.
#[configurable_component(sink("test_holding", "Test (holding)."))]
#[derive(Clone, Debug, Default)]
pub struct HoldingSinkConfig {
    #[serde(skip)]
    sink: Option<SourceSender>,
}

impl_generate_config_from_default!(HoldingSinkConfig);

impl HoldingSinkConfig {
    pub const fn new(sink: SourceSender) -> Self {
        Self { sink: Some(sink) }
    }
}

#[async_trait]
#[typetag::serde(name = "test_holding")]
impl SinkConfig for HoldingSinkConfig {
    async fn build(&self, _cx: SinkContext) -> crate::Result<(VectorSink, Healthcheck)> {
        let sink = HoldingSink {
            sink: self
                .sink
                .clone()
                .expect("a holding sink forwards to a sender"),
        };
        let healthcheck = future::ok(()).boxed();

        Ok((VectorSink::from_event_streamsink(sink), healthcheck))
    }

    fn input(&self) -> Input {
        Input::all()
    }

    fn acknowledgements(&self) -> &AcknowledgementsConfig {
        &AcknowledgementsConfig::DEFAULT
    }
}

struct HoldingSink {
    sink: SourceSender,
}

#[async_trait]
impl StreamSink<Event> for HoldingSink {
    async fn run(mut self: Box<Self>, mut input: BoxStream<'_, Event>) -> Result<(), ()> {
        let mut held = EventFinalizers::default();
        while let Some(mut event) = input.next().await {
            held.merge(event.take_finalizers());
            if let Err(error) = self.sink.send_event(event).await {
                error!(message = "Ingesting an event failed at holding sink.", %error);
            }
        }

        // The input has ended: deliver the held events by dropping their finalizers.
        drop(held);
        Ok(())
    }
}
