// Copyright 2025 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! `BackgroundEventHandler` — channel wrapper used by the consumer
//! background task to enqueue [`BackgroundEvent`]s for the app thread to
//! drain.
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler`.
//! The `AsyncConsumerMetrics.recordBackgroundEventQueueSize` calls are
//! dropped per the Phase-5 PLAN ("Out of scope: AsyncConsumerMetrics
//! instrumentation"). The matching app-side `drain_events` lives on the
//! consumer struct itself (Phase 10).
//!
//! As with [`super::application_event_handler::ApplicationEventHandler`],
//! the channel is **unbounded** to match Java's `LinkedBlockingQueue`.

use tokio::sync::mpsc;

use crate::common::KafkaError;

use super::background_event::{BackgroundEvent, BackgroundEventEnvelope};

/// Channel-side adapter: the bg task calls
/// [`BackgroundEventHandler::add`] to enqueue an event; the matching
/// `UnboundedReceiver` lives on the app side (held inside the consumer
/// struct in Phase 10).
pub(crate) struct BackgroundEventHandler {
    sender: mpsc::UnboundedSender<BackgroundEventEnvelope>,
}

impl BackgroundEventHandler {
    /// Constructor. Takes the **sender** half of the unbounded channel —
    /// the app side owns the receiver and drains it via
    /// `process_background_events` in Phase 10.
    pub(crate) fn new(sender: mpsc::UnboundedSender<BackgroundEventEnvelope>) -> Self {
        Self { sender }
    }

    /// Java: `add(BackgroundEvent event)`. Stamps `enqueued_ms` and
    /// sends.
    ///
    /// Returns `Err(KafkaError::illegal_state(...))` if the receiver has
    /// already been dropped — equivalent to Java's `IllegalStateException`
    /// thrown by a closed queue.
    pub(crate) fn add(&self, event: BackgroundEvent, now_ms: i64) -> Result<(), KafkaError> {
        let envelope = BackgroundEventEnvelope { event, enqueued_ms: now_ms };
        self.sender.send(envelope).map_err(|err| {
            KafkaError::illegal_state(format!(
                "App-side background-event receiver is closed; cannot enqueue {}",
                err.0.event.type_name()
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::oneshot;

    use crate::common::TopicPartition;
    use crate::consumer::consumer_rebalance_listener_method_name::ConsumerRebalanceListenerMethodName;

    use super::*;

    /// Java `BackgroundEventHandlerTest#testRecordBackgroundEventQueueSize`
    /// — adapted to drop the (out-of-scope) metric assertions. Verifies
    /// that `add` enqueues an event with the supplied timestamp and that
    /// the receiver's `recv()` resolves with it.
    #[tokio::test]
    async fn add_enqueues_error_event_with_timestamp() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let handler = BackgroundEventHandler::new(tx);

        handler
            .add(BackgroundEvent::Error { error: KafkaError::timeout("boom") }, 7)
            .expect("send ok");

        let env = rx.recv().await.expect("got envelope");
        assert_eq!(env.enqueued_ms, 7);
        match env.event {
            BackgroundEvent::Error { error } => {
                assert!(matches!(error, KafkaError::Timeout(_)));
            },
            other => panic!("unexpected variant {}", other.type_name()),
        }
    }

    /// Verifies that a `ConsumerRebalanceListenerCallbackNeeded` event
    /// carries its oneshot ack through the channel intact.
    #[tokio::test]
    async fn callback_needed_event_round_trips_ack() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let handler = BackgroundEventHandler::new(tx);

        let (ack_tx, ack_rx) = oneshot::channel::<Result<(), KafkaError>>();
        handler
            .add(
                BackgroundEvent::ConsumerRebalanceListenerCallbackNeeded {
                    method_name: ConsumerRebalanceListenerMethodName::OnPartitionsAssigned,
                    partitions: vec![TopicPartition::new("t".to_string(), 0)],
                    ack: ack_tx,
                },
                10,
            )
            .expect("send ok");

        let env = rx.recv().await.expect("got envelope");
        match env.event {
            BackgroundEvent::ConsumerRebalanceListenerCallbackNeeded { method_name, partitions, ack } => {
                assert_eq!(method_name, ConsumerRebalanceListenerMethodName::OnPartitionsAssigned);
                assert_eq!(partitions, vec![TopicPartition::new("t".to_string(), 0)]);
                ack.send(Ok(())).expect("ack ok");
            },
            other => panic!("unexpected variant {}", other.type_name()),
        }

        assert!(matches!(ack_rx.await.expect("ack ok"), Ok(())));
    }

    #[tokio::test]
    async fn add_returns_error_when_receiver_dropped() {
        let (tx, rx) = mpsc::unbounded_channel::<BackgroundEventEnvelope>();
        drop(rx);
        let handler = BackgroundEventHandler::new(tx);
        let err = handler
            .add(BackgroundEvent::Error { error: KafkaError::timeout("x") }, 0)
            .expect_err("must fail");
        assert!(matches!(err, KafkaError::IllegalState(_)));
    }
}
