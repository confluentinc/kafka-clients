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
//! Translates Java's `BackgroundEvent` abstract class + the two in-scope
//! concrete subclasses (`ErrorEvent`,
//! `ConsumerRebalanceListenerCallbackNeededEvent`) into a single Rust
//! enum. Streams variants (`StreamsOn*CallbackNeededEvent`) and share
//! variants are out of scope per
//! `design/history/Milestone-8/Phase-5/PLAN.md`.
//!
//! `BackgroundEvent`s flow from the consumer background task to the app
//! side. The bg task posts them via
//! [`crate::consumer::internals::events::BackgroundEventHandler::add`];
//! the app side, inside `poll()` / `commit_*()` / etc., drains the
//! channel and dispatches to the appropriate listener.

use tokio::sync::oneshot;

use crate::common::{KafkaError, TopicPartition};
use crate::consumer::consumer_rebalance_listener_method_name::ConsumerRebalanceListenerMethodName;

/// Single-enum translation of Java's `BackgroundEvent` hierarchy.
pub(crate) enum BackgroundEvent {
    /// `ErrorEvent` — surfaces a non-fatal error from the bg task to the
    /// app side. The app side returns this through the next `poll()` /
    /// `commit_*()` call.
    Error { error: KafkaError },
    /// `ConsumerRebalanceListenerCallbackNeededEvent` — bg → app half of
    /// the bidirectional rebalance-listener handshake
    /// (see `consumer-threading.md` §31).
    ///
    /// The bg task creates this event with one half of a oneshot, holds
    /// the other half, and `.await`s it after enqueueing the event so
    /// the rebalance state machine does not advance until the app side
    /// has invoked the listener method and reported back.
    ConsumerRebalanceListenerCallbackNeeded {
        method_name: ConsumerRebalanceListenerMethodName,
        /// Partitions affected by the rebalance step.
        partitions: Vec<TopicPartition>,
        /// One-shot back-channel: the app side, after running the
        /// listener, sends the result here. The bg task awaits this
        /// receiver before advancing the membership-state machine.
        ack: oneshot::Sender<Result<(), KafkaError>>,
    },
}

impl BackgroundEvent {
    /// Java equivalent: `BackgroundEvent.type().name()` — used in log /
    /// `toString()` output.
    pub(crate) fn type_name(&self) -> &'static str {
        match self {
            Self::Error { .. } => "Error",
            Self::ConsumerRebalanceListenerCallbackNeeded { .. } => "ConsumerRebalanceListenerCallbackNeeded",
        }
    }
}

impl std::fmt::Debug for BackgroundEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Error { error } => write!(f, "Error{{error={}}}", error),
            Self::ConsumerRebalanceListenerCallbackNeeded { method_name, partitions, .. } => write!(
                f,
                "ConsumerRebalanceListenerCallbackNeeded{{method={}, partitions={:?}}}",
                method_name, partitions
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
        let ev = BackgroundEvent::Error { error: KafkaError::timeout("boom") };
        assert_eq!(ev.type_name(), "Error");
    }

    #[test]
    fn type_name_for_callback_needed() {
        let (tx, _rx) = oneshot::channel();
        let ev = BackgroundEvent::ConsumerRebalanceListenerCallbackNeeded {
            method_name: ConsumerRebalanceListenerMethodName::OnPartitionsRevoked,
            partitions: vec![TopicPartition::new("t".to_string(), 0)],
            ack: tx,
        };
        assert_eq!(ev.type_name(), "ConsumerRebalanceListenerCallbackNeeded");
    }

    #[test]
    fn envelope_records_enqueued_ms() {
        let env = BackgroundEventEnvelope {
            event: BackgroundEvent::Error { error: KafkaError::timeout("x") },
            enqueued_ms: 999,
        };
        assert_eq!(env.enqueued_ms, 999);
        assert_eq!(env.event.type_name(), "Error");
    }

    #[test]
    fn debug_print_includes_error_message() {
        let ev = BackgroundEvent::Error { error: KafkaError::timeout("hello") };
        let s = format!("{:?}", ev);
        assert!(s.contains("Error"), "got: {}", s);
        assert!(s.contains("hello"), "got: {}", s);
    }
}
