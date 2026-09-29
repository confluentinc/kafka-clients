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
//! The `AsyncConsumerMetrics.recordBackgroundEventQueueSize` call is wired
//! in Phase M6 (via a shared `Arc<AtomicI64>` queue-depth mirror, since the
//! tokio mpsc sender has no `len()`); it is recorded only when the metrics
//! have been wired post-construction by the live consumer.
//!
//! # Sender-only by design
//!
//! Java's `BackgroundEventHandler` exposes a `drainEvents(...)` method
//! used by the app side to pull pending events at once. The Rust handler
//! deliberately omits `drain_events`: the app side holds the raw
//! [`tokio::sync::mpsc::UnboundedReceiver`] and drains it directly via
//! `try_recv` in a `while let` loop (`consumer-threading.md` §31). The
//! handler is therefore **sender-only by design** — do NOT add a
//! `drain_events` method here in Phase 10.
//!
//! As with [`super::ApplicationEventHandler`],
//! the channel is **unbounded** to match Java's `LinkedBlockingQueue`.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use tokio::sync::mpsc;

use crate::common::Error;
use crate::consumer::internals::AsyncConsumerMetrics;

use super::{BackgroundEvent, BackgroundEventEnvelope};

/// Channel-side adapter: the bg task calls
/// [`BackgroundEventHandler::add`] to enqueue an event; the matching
/// `UnboundedReceiver` lives on the app side (held inside the consumer
/// struct in Phase 10).
///
/// **Sender-only by design** — see the module-level docs for why
/// `drain_events` is not provided.
pub(crate) struct BackgroundEventHandler {
    sender: mpsc::UnboundedSender<BackgroundEventEnvelope>,
    /// Async-consumer metrics (`AsyncConsumerMetrics`). `None` until wired
    /// post-construction by the live consumer (M4/M5 setter precedent).
    async_consumer_metrics: Option<Arc<AsyncConsumerMetrics>>,
    /// Shared mirror of the background-event queue depth. Java reads
    /// `backgroundEventQueue.size()`; the tokio mpsc sender exposes no
    /// `len()`, so this `AtomicI64` is incremented here on enqueue and
    /// reset to 0 by the app side's drain (`processBackgroundEvents`,
    /// folding Java's `drainEvents` `recordBackgroundEventQueueSize(0)`).
    queue_size: Option<Arc<AtomicI64>>,
}

impl BackgroundEventHandler {
    /// Constructor. Takes the **sender** half of the unbounded channel —
    /// the app side owns the receiver and drains it via
    /// `process_background_events` in Phase 10.
    pub(crate) fn new(sender: mpsc::UnboundedSender<BackgroundEventEnvelope>) -> Self {
        Self { sender, async_consumer_metrics: None, queue_size: None }
    }

    /// Wires the `AsyncConsumerMetrics` and shared queue-depth counter
    /// post-construction (M4/M5 setter precedent).
    pub(crate) fn set_async_consumer_metrics(
        &mut self,
        metrics: Arc<AsyncConsumerMetrics>,
        queue_size: Arc<AtomicI64>,
    ) {
        self.async_consumer_metrics = Some(metrics);
        self.queue_size = Some(queue_size);
    }

    /// Java: `add(BackgroundEvent event)`. Stamps `enqueued_ms` and
    /// sends.
    ///
    /// Returns `Err(Error::local_illegal_state(...))` if the receiver has
    /// already been dropped — equivalent to Java's `IllegalStateException`
    /// thrown by a closed queue.
    pub(crate) fn add(&self, event: BackgroundEvent, now_ms: i64) -> Result<(), Error> {
        let envelope = BackgroundEventEnvelope { event, enqueued_ms: now_ms };
        // Java records `backgroundEventQueue.size() + 1` before adding.
        if let (Some(metrics), Some(queue_size)) = (&self.async_consumer_metrics, &self.queue_size) {
            let new_size = queue_size.fetch_add(1, Ordering::SeqCst) + 1;
            metrics.record_background_event_queue_size(new_size as i32);
        }
        self.sender.send(envelope).map_err(|err| {
            if let Some(queue_size) = &self.queue_size {
                queue_size.fetch_sub(1, Ordering::SeqCst);
            }
            Error::local_illegal_state(format!(
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
    use crate::consumer::ConsumerRebalanceListenerMethodName;

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
            .add(BackgroundEvent::Error { error: Error::timeout("boom") }, 7)
            .expect("send ok");

        let env = rx.recv().await.expect("got envelope");
        assert_eq!(env.enqueued_ms, 7);
        match env.event {
            BackgroundEvent::Error { error } => {
                assert!(matches!(error, Error::Timeout(_)));
            },
            other => panic!("unexpected variant {}", other.type_name()),
        }
    }

    /// Verifies that a `PartitionsRemoved` event carries its oneshot ack
    /// through the channel intact.
    #[tokio::test]
    async fn partitions_removed_event_round_trips_ack() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let handler = BackgroundEventHandler::new(tx);

        let (ack_tx, ack_rx) = oneshot::channel::<Result<(), Error>>();
        handler
            .add(
                BackgroundEvent::PartitionsRemoved {
                    method_name: ConsumerRebalanceListenerMethodName::OnPartitionsRevoked,
                    partitions: vec![TopicPartition::new("t".to_string(), 0)],
                    ack: ack_tx,
                },
                10,
            )
            .expect("send ok");

        let env = rx.recv().await.expect("got envelope");
        match env.event {
            BackgroundEvent::PartitionsRemoved { method_name, partitions, ack } => {
                assert_eq!(method_name, ConsumerRebalanceListenerMethodName::OnPartitionsRevoked);
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
            .add(BackgroundEvent::Error { error: Error::timeout("x") }, 0)
            .expect_err("must fail");
        assert!(matches!(err, Error::LocalIllegalState(_)));
    }

    /// M6 wiring: `add` records the background-event queue size against the
    /// `AsyncConsumerMetrics` and bumps the shared queue-depth counter
    /// (Java records `backgroundEventQueue.size() + 1` before adding).
    #[tokio::test]
    async fn add_records_queue_size_when_metrics_wired() {
        use crate::common::Metric;
        use crate::common::metrics::Metrics;
        use crate::consumer::internals::AsyncConsumerMetrics;
        use crate::consumer::internals::ConsumerUtils;

        let (tx, mut rx) = mpsc::unbounded_channel();
        let metrics = Arc::new(Metrics::new());
        let acm = Arc::new(AsyncConsumerMetrics::new(
            Arc::clone(&metrics),
            ConsumerUtils::CONSUMER_METRIC_GROUP,
        ));
        let queue_size = Arc::new(AtomicI64::new(0));

        let mut handler = BackgroundEventHandler::new(tx);
        handler.set_async_consumer_metrics(Arc::clone(&acm), Arc::clone(&queue_size));

        handler
            .add(BackgroundEvent::Error { error: Error::timeout("a") }, 0)
            .expect("send ok");
        handler
            .add(BackgroundEvent::Error { error: Error::timeout("b") }, 0)
            .expect("send ok");

        assert_eq!(queue_size.load(Ordering::SeqCst), 2);
        let mn = metrics.metric_name("background-event-queue-size", ConsumerUtils::CONSUMER_METRIC_GROUP);
        assert_eq!(metrics.metric(&mn).unwrap().metric_value().as_double(), Some(2.0));

        assert!(rx.recv().await.is_some());
        assert!(rx.recv().await.is_some());
    }
}
