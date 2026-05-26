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

use tokio::sync::{mpsc, oneshot};

use crate::common::KafkaError;

use super::application_event::{ApplicationEvent, ApplicationEventEnvelope};

/// Channel-side adapter: the app thread calls
/// [`ApplicationEventHandler::add`] to enqueue an event; the matching
/// `UnboundedReceiver` lives on the consumer background task (handed in
/// at construction by Phase 10).
pub(crate) struct ApplicationEventHandler {
    sender: mpsc::UnboundedSender<ApplicationEventEnvelope>,
}

impl ApplicationEventHandler {
    /// Constructor. Takes the **sender** half of the unbounded channel —
    /// the background task constructed in Phase 10 owns the receiver.
    pub(crate) fn new(sender: mpsc::UnboundedSender<ApplicationEventEnvelope>) -> Self {
        Self { sender }
    }

    /// Java: `add(ApplicationEvent event)`.
    ///
    /// Stamps `enqueued_ms` onto the envelope and sends to the channel.
    /// Returns `Err(KafkaError::illegal_state(...))` if the receiver
    /// (background task) has already been dropped — equivalent to Java's
    /// `IllegalStateException` thrown by a closed queue.
    pub(crate) fn add(&self, event: ApplicationEvent, now_ms: i64) -> Result<(), KafkaError> {
        let envelope = ApplicationEventEnvelope { event, enqueued_ms: now_ms };
        self.sender.send(envelope).map_err(|err| {
            KafkaError::illegal_state(format!(
                "Background task is shut down; cannot enqueue {}",
                err.0.event.type_name()
            ))
        })
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
    /// returns `KafkaError::illegal_state(...)`.
    pub(crate) async fn add_and_get<T: Send + 'static>(
        &self,
        event: ApplicationEvent,
        receiver: oneshot::Receiver<Result<T, KafkaError>>,
        now_ms: i64,
    ) -> Result<T, KafkaError> {
        let event_name = event.type_name();
        self.add(event, now_ms)?;
        match receiver.await {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(err)) => Err(err),
            Err(_recv_err) => Err(KafkaError::illegal_state(format!(
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
        let handler = ApplicationEventHandler::new(tx);
        handler.add(ApplicationEvent::CommitOnClose, 42).expect("send ok");

        let env = rx.recv().await.expect("got envelope");
        assert_eq!(env.enqueued_ms, 42);
        assert_eq!(env.event.type_name(), "CommitOnClose");
    }

    #[tokio::test]
    async fn add_returns_error_when_receiver_dropped() {
        let (tx, rx) = mpsc::unbounded_channel::<ApplicationEventEnvelope>();
        drop(rx);
        let handler = ApplicationEventHandler::new(tx);
        let err = handler.add(ApplicationEvent::CommitOnClose, 0).expect_err("must fail");
        assert!(matches!(err, KafkaError::IllegalState(_)));
    }

    #[tokio::test]
    async fn add_and_get_returns_completed_value() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let handler = ApplicationEventHandler::new(tx);

        let (handle, receiver, _erased) = make_completable_event::<()>(0);
        let event = ApplicationEvent::AsyncPoll { handle };

        let send_task = tokio::spawn(async move { handler.add_and_get::<()>(event, receiver, 10).await });

        // Drain the envelope from the channel and complete the event via
        // the handle inside.
        let env = rx.recv().await.expect("got envelope");
        assert_eq!(env.enqueued_ms, 10);
        match env.event {
            ApplicationEvent::AsyncPoll { handle } => {
                assert!(handle.complete(()));
            },
            other => panic!("unexpected variant {}", other.type_name()),
        }

        let result = send_task.await.expect("task ok").expect("add_and_get ok");
        assert_eq!(result, ());
    }

    #[tokio::test]
    async fn add_and_get_propagates_kafka_error_from_handle() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let handler = ApplicationEventHandler::new(tx);

        let (handle, receiver, _erased) = make_completable_event::<()>(0);
        let event = ApplicationEvent::AsyncPoll { handle };

        let send_task = tokio::spawn(async move { handler.add_and_get::<()>(event, receiver, 0).await });

        let env = rx.recv().await.expect("got envelope");
        match env.event {
            ApplicationEvent::AsyncPoll { handle } => {
                let err = KafkaError::illegal_state("boom");
                assert!(handle.complete_exceptionally(err));
            },
            _ => panic!("unexpected variant"),
        }

        let result = send_task.await.expect("task ok");
        assert!(matches!(result, Err(KafkaError::IllegalState(_))));
    }

    #[tokio::test]
    async fn add_and_get_returns_error_when_handle_dropped_without_completion() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let handler = ApplicationEventHandler::new(tx);

        let (handle, receiver, erased) = make_completable_event::<()>(0);
        let event = ApplicationEvent::AsyncPoll { handle };

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
        assert!(matches!(result, Err(KafkaError::IllegalState(_))));
    }
}
