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

//! `ApplicationEventHandler` — channel wrapper used by the app side to
//! enqueue [`ApplicationEvent`]s for the background task.
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.events.ApplicationEventHandler`,
//! BUT this Phase-5 cut covers ONLY the channel surface. The Java class
//! also constructs and runs the `ConsumerNetworkThread`; that part is
//! deferred to Phase 10 (see `design/history/Milestone-8/Phase-5/PLAN.md`
//! "Out of scope").
//!
//! # Channel choice
//!
//! Uses [`tokio::sync::mpsc::unbounded_channel`] to match Java's
//! `LinkedBlockingQueue` default (no capacity). A bounded channel would
//! deadlock under bursty `add()` calls (`consumer-threading.md` §10
//! "Drain unbounded (Java does)"). The matching
//! [`tokio::sync::mpsc::UnboundedSender::send`] is sync, so [`add`] is a
//! synchronous `fn`.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use tokio::sync::{Notify, mpsc, oneshot};

use crate::common::Error;
use crate::consumer::internals::async_consumer_metrics::AsyncConsumerMetrics;

use super::application_event::{ApplicationEvent, ApplicationEventEnvelope};

/// Channel-side adapter: the app thread calls
/// [`ApplicationEventHandler::add`] to enqueue an event; the matching
/// `UnboundedReceiver` lives on the consumer background task (handed in
/// at construction by Phase 10).
pub(crate) struct ApplicationEventHandler {
    sender: mpsc::UnboundedSender<ApplicationEventEnvelope>,
    /// Wakes the background task as soon as an event is enqueued.
    ///
    /// Java's `add()` calls `wakeupNetworkThread()` →
    /// `networkClientDelegate.wakeup()` → `Selector.wakeup()` right after
    /// pushing the event, so the I/O thread breaks out of its blocking
    /// `poll(...)` immediately instead of waiting up to
    /// `MAX_POLL_TIMEOUT_MS`. Sending on the unbounded channel does NOT
    /// wake the bg task (it drains via non-blocking `try_recv`, not
    /// `recv().await`, per `consumer-threading.md` §10), so this `Notify`
    /// is the Rust analog of that wakeup. It IS the client's own wakeup handle
    /// (`KafkaClient::wakeup_handle()`) — the primitive the network poll itself
    /// awaits — so poking it returns the in-progress poll directly, with no
    /// forwarding hop through the bg loop.
    event_notify: Arc<Notify>,
    /// Async-consumer metrics (`AsyncConsumerMetrics`). `None` until wired
    /// post-construction by the live consumer (M4/M5 setter precedent);
    /// tests that don't care leave it unset and `add` records nothing.
    async_consumer_metrics: Option<Arc<AsyncConsumerMetrics>>,
    /// Shared mirror of the application-event queue depth. Java reads
    /// `applicationEventQueue.size()`; the tokio mpsc sender exposes no
    /// `len()`, so this `AtomicI64` is incremented here on enqueue and
    /// reset to 0 by the bg task's drain (`processApplicationEvents`).
    /// See Phase-M6 PLAN.
    queue_size: Option<Arc<AtomicI64>>,
}

impl ApplicationEventHandler {
    /// Constructor. Takes the **sender** half of the unbounded channel —
    /// the background task constructed in Phase 10 owns the receiver — and
    /// the shared [`Notify`] used to wake that task on each `add()`.
    pub(crate) fn new(sender: mpsc::UnboundedSender<ApplicationEventEnvelope>, event_notify: Arc<Notify>) -> Self {
        Self { sender, event_notify, async_consumer_metrics: None, queue_size: None }
    }

    /// Wires the `AsyncConsumerMetrics` and the shared queue-depth counter
    /// post-construction (M4/M5 setter precedent — keeps the no-arg `new`
    /// and all existing test call sites untouched).
    pub(crate) fn set_async_consumer_metrics(
        &mut self,
        metrics: Arc<AsyncConsumerMetrics>,
        queue_size: Arc<AtomicI64>,
    ) {
        self.async_consumer_metrics = Some(metrics);
        self.queue_size = Some(queue_size);
    }

    /// Java: `add(ApplicationEvent event)`.
    ///
    /// Stamps `enqueued_ms` onto the envelope and sends to the channel.
    /// Returns `Err(Error::illegal_state(...))` if the receiver
    /// (background task) has already been dropped — equivalent to Java's
    /// `IllegalStateException` thrown by a closed queue.
    pub(crate) fn add(&self, event: ApplicationEvent, now_ms: i64) -> Result<(), Error> {
        let envelope = ApplicationEventEnvelope { event, enqueued_ms: now_ms };
        // Java records the updated queue size (`size() + 1`) BEFORE adding to
        // the queue to avoid racing the background thread's removals. We bump
        // the shared depth counter first and record the post-increment value
        // (== Java's `size() + 1`).
        if let (Some(metrics), Some(queue_size)) = (&self.async_consumer_metrics, &self.queue_size) {
            let new_size = queue_size.fetch_add(1, Ordering::SeqCst) + 1;
            metrics.record_application_event_queue_size(new_size as i32);
        }
        self.sender.send(envelope).map_err(|err| {
            // The send failed; undo the optimistic depth increment.
            if let Some(queue_size) = &self.queue_size {
                queue_size.fetch_sub(1, Ordering::SeqCst);
            }
            Error::illegal_state(format!(
                "Background task is shut down; cannot enqueue {}",
                err.0.event.type_name()
            ))
        })?;
        // Java: `wakeupNetworkThread()` — alert the I/O thread that it has
        // something to process so it breaks out of its blocking poll
        // immediately rather than after MAX_POLL_TIMEOUT_MS. `notify_one()`
        // stores a permit if the bg task is not currently parked on
        // `notified()`, so a wake is never lost in the gap between the
        // bg task's `try_recv` drain and its `select!`.
        self.event_notify.notify_one();
        Ok(())
    }

    /// Java: `wakeupNetworkThread()` on its own, with nothing enqueued.
    ///
    /// Breaks the bg task out of its blocking selector poll so it runs
    /// another `run_once` iteration promptly. This is the *only* correct
    /// primitive for "make the bg loop iterate": it must NOT be confused
    /// with [`WakeupTrigger::wakeup`] /
    /// [`NetworkThreadCloseHandle::wakeup`], which cancel the wakeup token
    /// and therefore arm a **user-visible** `Error::Wakeup` on the next
    /// public API call (§11). This one goes straight to the selector's wakeup
    /// handle, which has no user-visible effect and cannot be silenced by
    /// `WakeupTrigger::disable()`.
    ///
    /// Used by the rebalance-listener ack path in
    /// `AsyncKafkaConsumer::process_background_events` (§31 step 4).
    pub(crate) fn wake_background_task(&self) {
        self.event_notify.notify_one();
    }

    /// Java: `addAndGet(event)`.
    ///
    /// Enqueues the event and awaits the matching
    /// [`oneshot::Receiver`] for the typed result. Callers pre-create
    /// the receiver via [`super::completable_event::make_completable_event`]
    /// so the typed `T` parameter can flow without erasure.
    ///
    /// If the receiver is dropped before completion (only possible if
    /// the bg task panicked / shut down without completing the event),
    /// returns `Error::illegal_state(...)`.
    pub(crate) async fn add_and_get<T: Send + 'static>(
        &self,
        event: ApplicationEvent,
        receiver: oneshot::Receiver<Result<T, Error>>,
        now_ms: i64,
    ) -> Result<T, Error> {
        let event_name = event.type_name();
        self.add(event, now_ms)?;
        match receiver.await {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(err)) => Err(err),
            Err(_recv_err) => Err(Error::illegal_state(format!(
                "Background task dropped the completion sender for {} without completing it",
                event_name
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::completable_event::make_completable_event;
    use super::*;

    #[tokio::test]
    async fn add_enqueues_event_with_timestamp() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let handler = ApplicationEventHandler::new(tx, Arc::new(Notify::new()));
        handler.add(ApplicationEvent::CommitOnClose, 42).expect("send ok");

        let env = rx.recv().await.expect("got envelope");
        assert_eq!(env.enqueued_ms, 42);
        assert_eq!(env.event.type_name(), "CommitOnClose");
    }

    #[tokio::test]
    async fn add_wakes_the_shared_notify() {
        // Java: `add()` calls `wakeupNetworkThread()`. The Rust analog is
        // `event_notify.notify_one()`. A clone of the same `Notify` held by
        // the (would-be) bg task must observe a wake after `add()`.
        let (tx, _rx) = mpsc::unbounded_channel();
        let notify = Arc::new(Notify::new());
        let handler = ApplicationEventHandler::new(tx, Arc::clone(&notify));

        handler.add(ApplicationEvent::CommitOnClose, 0).expect("send ok");

        // `notify_one()` stored a permit before anyone awaited, so
        // `notified()` resolves immediately. Guard with a timeout so a
        // missing wake fails the test instead of hanging.
        tokio::time::timeout(std::time::Duration::from_secs(1), notify.notified())
            .await
            .expect("add() must wake the shared Notify");
    }

    #[tokio::test]
    async fn add_returns_error_when_receiver_dropped() {
        let (tx, rx) = mpsc::unbounded_channel::<ApplicationEventEnvelope>();
        drop(rx);
        let handler = ApplicationEventHandler::new(tx, Arc::new(Notify::new()));
        let err = handler.add(ApplicationEvent::CommitOnClose, 0).expect_err("must fail");
        assert!(matches!(err, Error::IllegalState(_)));
    }

    /// M6 wiring: `add` records the application-event queue size against the
    /// `AsyncConsumerMetrics` and bumps the shared queue-depth counter
    /// (Java records `applicationEventQueue.size() + 1` before adding).
    #[tokio::test]
    async fn add_records_queue_size_when_metrics_wired() {
        use crate::common::metric::Metric;
        use crate::common::metrics::Metrics;
        use crate::consumer::internals::async_consumer_metrics::AsyncConsumerMetrics;
        use crate::consumer::internals::consumer_utils::CONSUMER_METRIC_GROUP;

        let (tx, mut rx) = mpsc::unbounded_channel();
        let metrics = Arc::new(Metrics::new());
        let acm = Arc::new(AsyncConsumerMetrics::new(Arc::clone(&metrics), CONSUMER_METRIC_GROUP));
        let queue_size = Arc::new(AtomicI64::new(0));

        let mut handler = ApplicationEventHandler::new(tx, Arc::new(Notify::new()));
        handler.set_async_consumer_metrics(Arc::clone(&acm), Arc::clone(&queue_size));

        handler.add(ApplicationEvent::CommitOnClose, 0).expect("send ok");
        handler.add(ApplicationEvent::CommitOnClose, 0).expect("send ok");

        // The shared counter reflects two enqueued events; the recorded size
        // metric reflects the latest `size()+1` value (2).
        assert_eq!(queue_size.load(Ordering::SeqCst), 2);
        let mn = metrics.metric_name_group("application-event-queue-size", CONSUMER_METRIC_GROUP);
        assert_eq!(metrics.metric(&mn).unwrap().metric_value().as_double(), Some(2.0));

        // Drain so the channel does not leak the senders.
        assert!(rx.recv().await.is_some());
        assert!(rx.recv().await.is_some());
    }

    /// M6 wiring: a failed `add` (receiver dropped) rolls back the optimistic
    /// queue-depth increment so the counter stays consistent.
    #[tokio::test]
    async fn add_rolls_back_queue_size_on_send_failure() {
        use crate::common::metrics::Metrics;
        use crate::consumer::internals::async_consumer_metrics::AsyncConsumerMetrics;
        use crate::consumer::internals::consumer_utils::CONSUMER_METRIC_GROUP;

        let (tx, rx) = mpsc::unbounded_channel::<ApplicationEventEnvelope>();
        drop(rx);
        let metrics = Arc::new(Metrics::new());
        let acm = Arc::new(AsyncConsumerMetrics::new(Arc::clone(&metrics), CONSUMER_METRIC_GROUP));
        let queue_size = Arc::new(AtomicI64::new(0));

        let mut handler = ApplicationEventHandler::new(tx, Arc::new(Notify::new()));
        handler.set_async_consumer_metrics(acm, Arc::clone(&queue_size));

        let _ = handler.add(ApplicationEvent::CommitOnClose, 0).expect_err("must fail");
        assert_eq!(queue_size.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn add_and_get_returns_completed_value() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let handler = ApplicationEventHandler::new(tx, Arc::new(Notify::new()));

        let (handle, receiver, _erased) = make_completable_event::<()>(0);
        let event = ApplicationEvent::CreateFetchRequests { handle };

        let send_task = tokio::spawn(async move { handler.add_and_get::<()>(event, receiver, 10).await });

        // Drain the envelope from the channel and complete the event via
        // the handle inside.
        let env = rx.recv().await.expect("got envelope");
        assert_eq!(env.enqueued_ms, 10);
        match env.event {
            ApplicationEvent::CreateFetchRequests { handle } => {
                assert!(handle.complete(()));
            },
            other => panic!("unexpected variant {}", other.type_name()),
        }

        // `add_and_get::<()>` returns `Result<(), Error>`; both expects
        // unwrap the success path, no further assertion needed.
        send_task.await.expect("task ok").expect("add_and_get ok");
    }

    #[tokio::test]
    async fn add_and_get_propagates_kafka_error_from_handle() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let handler = ApplicationEventHandler::new(tx, Arc::new(Notify::new()));

        let (handle, receiver, _erased) = make_completable_event::<()>(0);
        let event = ApplicationEvent::CreateFetchRequests { handle };

        let send_task = tokio::spawn(async move { handler.add_and_get::<()>(event, receiver, 0).await });

        let env = rx.recv().await.expect("got envelope");
        match env.event {
            ApplicationEvent::CreateFetchRequests { handle } => {
                let err = Error::illegal_state("boom");
                assert!(handle.complete_with_error(err));
            },
            _ => panic!("unexpected variant"),
        }

        let result = send_task.await.expect("task ok");
        assert!(matches!(result, Err(Error::IllegalState(_))));
    }

    #[tokio::test]
    async fn add_and_get_returns_error_when_handle_dropped_without_completion() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let handler = ApplicationEventHandler::new(tx, Arc::new(Notify::new()));

        let (handle, receiver, erased) = make_completable_event::<()>(0);
        let event = ApplicationEvent::CreateFetchRequests { handle };

        let send_task = tokio::spawn(async move { handler.add_and_get::<()>(event, receiver, 0).await });

        let env = rx.recv().await.expect("got envelope");
        // Drop ALL handle clones: the envelope-held handle AND the reaper's
        // erased copy. The shared `Arc<HandleInner>` holds the oneshot
        // sender, so we must release every clone before the sender is
        // dropped and the receiver returns `RecvError`. Mirrors what the
        // reaper does in production when it expires a deadline-exceeded
        // event without anybody completing it.
        drop(erased);
        drop(env);

        let result = send_task.await.expect("task ok");
        assert!(matches!(result, Err(Error::IllegalState(_))));
    }
}
