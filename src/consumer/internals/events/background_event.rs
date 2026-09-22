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

//! `BackgroundEvent` enum and envelope.
//!
//! Translates Java's `BackgroundEvent` abstract class + the in-scope
//! concrete subclasses (`ErrorEvent`, `PartitionsRemovedEvent`,
//! `PartitionsAssignedEvent`) into a single Rust enum. Streams variants
//! (`StreamsTasksAssignedEvent`, `StreamsOn*CallbackNeededEvent`) and share
//! variants are out of scope per
//! `design/history/Milestone-8/Phase-5/PLAN.md`.
//!
//! `BackgroundEvent`s flow from the consumer background task to the app
//! side. The bg task posts them via
//! [`crate::consumer::internals::events::BackgroundEventHandler::add`];
//! the app side, inside `poll()` / `commit_*()` / etc., drains the
//! channel and dispatches to the appropriate listener.

use tokio::sync::oneshot;

use crate::common::{Error, TopicPartition};
use crate::consumer::ConsumerRebalanceListenerMethodName;

/// Single-enum translation of Java's `BackgroundEvent` hierarchy.
pub(crate) enum BackgroundEvent {
    /// `ErrorEvent` — surfaces a non-fatal error from the bg task to the
    /// app side. The app side returns this through the next `poll()` /
    /// `commit_*()` call.
    Error { error: Error },
    /// `PartitionsRemovedEvent` — bg → app half of the rebalance-listener
    /// handshake for the **revoke / lost** path (renamed in AK 4.3.1 from
    /// `ConsumerRebalanceListenerCallbackNeededEvent`; see
    /// `consumer-threading.md` §31 and PLAN §2.1).
    ///
    /// The bg reconcile enqueues this event carrying the callback
    /// `method_name` (`ON_PARTITIONS_REVOKED` / `ON_PARTITIONS_LOST`) and
    /// the affected partitions. It holds the matching `ack` receiver as
    /// cross-iteration state (Phase 41) so the membership-state transition
    /// does not advance until the app side has invoked the listener method
    /// and reported back — while the bg loop keeps spinning.
    PartitionsRemoved {
        method_name: ConsumerRebalanceListenerMethodName,
        /// Partitions affected by the rebalance step.
        partitions: Vec<TopicPartition>,
        /// One-shot back-channel: the app side, after running the
        /// listener, sends the result here. The bg task awaits this
        /// receiver before advancing the membership-state machine.
        ack: oneshot::Sender<Result<(), Error>>,
    },
    /// `PartitionsAssignedEvent` (AK 4.3.1, KAFKA-20106) — bg → app half of
    /// the **assign** path. Sent by `signal_partitions_assigned` at the end
    /// of a bg reconcile, EVEN WHEN NO LISTENER is registered, carrying the
    /// full reconciled `assigned_partitions` plus the newly-`added_partitions`.
    ///
    /// The app thread (inside `poll()`) processes it by first sending an
    /// [`crate::consumer::internals::events::ApplicationEvent::ApplyAssignment`]
    /// (app → bg) and awaiting it — so `SubscriptionState` mutates on the bg
    /// side but is triggered/awaited by the app thread, guaranteeing
    /// `consumer.assignment()` changes only within `poll()` — then runs
    /// `on_partitions_assigned` (if a listener exists) and finally replies
    /// on `ack`. The bg holds the `ack` receiver as cross-iteration state,
    /// resuming the reconcile (enabling fetching for the added partitions)
    /// only once it arrives.
    PartitionsAssigned {
        /// Full assignment to apply in the subscription state.
        assigned_partitions: Vec<TopicPartition>,
        /// Newly added partitions (passed to `on_partitions_assigned`).
        added_partitions: Vec<TopicPartition>,
        /// One-shot back-channel: the app side replies here after applying
        /// the assignment and running the callback. The bg task awaits this
        /// receiver before advancing the reconciliation.
        ack: oneshot::Sender<Result<(), Error>>,
    },
}

impl BackgroundEvent {
    /// Java equivalent: `BackgroundEvent.type().name()` — used in log /
    /// `toString()` output.
    pub(crate) fn type_name(&self) -> &'static str {
        match self {
            Self::Error { .. } => "Error",
            Self::PartitionsRemoved { .. } => "PartitionsRemoved",
            Self::PartitionsAssigned { .. } => "PartitionsAssigned",
        }
    }
}

impl std::fmt::Debug for BackgroundEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Error { error } => write!(f, "Error{{error={}}}", error),
            Self::PartitionsRemoved { method_name, partitions, .. } => {
                write!(f, "PartitionsRemoved{{method={}, partitions={:?}}}", method_name, partitions)
            },
            Self::PartitionsAssigned { assigned_partitions, added_partitions, .. } => write!(
                f,
                "PartitionsAssigned{{assigned={:?}, added={:?}}}",
                assigned_partitions, added_partitions
            ),
        }
    }
}

impl std::fmt::Display for BackgroundEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// Wraps a [`BackgroundEvent`] with the timestamp at which it was
/// enqueued. Mirrors the structure of `ApplicationEventEnvelope`.
pub(crate) struct BackgroundEventEnvelope {
    pub event: BackgroundEvent,
    /// Wall-clock timestamp (milliseconds) at which the event was added
    /// to the channel.
    pub enqueued_ms: i64,
}

impl std::fmt::Debug for BackgroundEventEnvelope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BackgroundEventEnvelope")
            .field("event", &self.event)
            .field("enqueued_ms", &self.enqueued_ms)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_name_for_error_event() {
        let ev = BackgroundEvent::Error { error: Error::timeout("boom") };
        assert_eq!(ev.type_name(), "Error");
    }

    #[test]
    fn type_name_for_partitions_removed() {
        let (tx, _rx) = oneshot::channel();
        let ev = BackgroundEvent::PartitionsRemoved {
            method_name: ConsumerRebalanceListenerMethodName::OnPartitionsRevoked,
            partitions: vec![TopicPartition::new("t".to_string(), 0)],
            ack: tx,
        };
        assert_eq!(ev.type_name(), "PartitionsRemoved");
    }

    #[test]
    fn type_name_for_partitions_assigned() {
        let (tx, _rx) = oneshot::channel();
        let ev = BackgroundEvent::PartitionsAssigned {
            assigned_partitions: vec![TopicPartition::new("t".to_string(), 0)],
            added_partitions: vec![TopicPartition::new("t".to_string(), 0)],
            ack: tx,
        };
        assert_eq!(ev.type_name(), "PartitionsAssigned");
    }

    #[test]
    fn envelope_records_enqueued_ms() {
        let env =
            BackgroundEventEnvelope { event: BackgroundEvent::Error { error: Error::timeout("x") }, enqueued_ms: 999 };
        assert_eq!(env.enqueued_ms, 999);
        assert_eq!(env.event.type_name(), "Error");
    }

    #[test]
    fn debug_print_includes_error_message() {
        let ev = BackgroundEvent::Error { error: Error::timeout("hello") };
        let s = format!("{:?}", ev);
        assert!(s.contains("Error"), "got: {}", s);
        assert!(s.contains("hello"), "got: {}", s);
    }
}
