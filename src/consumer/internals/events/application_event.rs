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

//! `ApplicationEvent` enum and envelope.
//!
//! Translates Java's `ApplicationEvent` abstract class + ~25 concrete
//! `*Event` subclasses (`AssignmentChangeEvent`, `CommitAsyncEvent`, …) into
//! a single Rust enum. The Java `Type` enum becomes the Rust variant
//! discriminant.
//!
//! **Scope** (per `design/history/Milestone-8/Phase-5/PLAN.md` "Out of
//! scope"):
//!
//!   * Share-consumer (`KIP-932`) variants are NOT translated.
//!   * Streams (`Streams*Event`) variants are NOT translated.
//!
//! See [`consumer-threading.md` §20](../../../../../../.claude/rules/consumer-threading.md)
//! for the full scope decision.

use std::collections::{HashMap, HashSet};

use regex::Regex;

use crate::common::{KafkaError, PartitionInfo, TopicPartition};
use crate::consumer::consumer_rebalance_listener_method_name::ConsumerRebalanceListenerMethodName;
use crate::consumer::internals::auto_offset_reset_strategy::AutoOffsetResetStrategy;
use crate::consumer::{OffsetAndMetadata, OffsetAndTimestamp, SubscriptionPattern};

use super::completable_event::CompletableEventHandle;

/// Single-enum translation of Java's `ApplicationEvent` hierarchy.
///
/// The variant discriminant takes the place of Java's `Type` enum.
/// `enqueued_ms` is stored on the [`ApplicationEventEnvelope`] wrapper
/// rather than on each variant, so the bg-task pattern matches stay free
/// of bookkeeping fields.
pub(crate) enum ApplicationEvent {
    // ─── Non-completable events ───
    /// `AssignmentChangeEvent` — `consumer.assign(...)`. Replaces the
    /// current assignment with `all_partitions`.
    AssignmentChange {
        all_partitions: HashSet<TopicPartition>,
    },
    /// `CommitOnCloseEvent` — fire-and-forget commit during close.
    CommitOnClose,
    /// `LeaveGroupOnCloseEvent` — instructs the membership manager to
    /// send a final heartbeat with the `leave-group` epoch.
    LeaveGroupOnClose {
        reason: String,
    },
    /// `StopFindCoordinatorOnCloseEvent` — tells the coordinator-finder
    /// to stop sending `FindCoordinator` requests during close.
    StopFindCoordinatorOnClose,
    /// `NewTopicsMetadataUpdateRequestEvent` — instructs the metadata
    /// manager to request metadata for newly-discovered topics. Java
    /// has no payload here either.
    NewTopicsMetadataUpdate,
    /// `ConsumerRebalanceListenerCallbackCompletedEvent` — app → bg
    /// half of the rebalance-listener handshake. The app side, after
    /// invoking the listener, sends this with the result.
    ConsumerRebalanceListenerCallbackCompleted {
        method_name: ConsumerRebalanceListenerMethodName,
        error: Option<KafkaError>,
    },
    /// `UpdatePatternSubscriptionEvent` — forces a re-evaluation of the
    /// subscribed regex against the latest metadata.
    UpdatePatternSubscription,

    // ─── Completable events ───
    /// `AsyncCommitEvent` — `consumer.commit_async(offsets)`.
    CommitAsync {
        handle: CompletableEventHandle<()>,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
    },
    /// `SyncCommitEvent` — `consumer.commit_sync(offsets, timeout)`.
    CommitSync {
        handle: CompletableEventHandle<()>,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
    },
    /// `AsyncPollEvent` — pumps the membership / fetch state machine.
    AsyncPoll {
        handle: CompletableEventHandle<()>,
    },
    /// `FetchCommittedOffsetsEvent`.
    FetchCommittedOffsets {
        handle: CompletableEventHandle<HashMap<TopicPartition, OffsetAndMetadata>>,
        partitions: HashSet<TopicPartition>,
    },
    /// `ListOffsetsEvent`.
    ListOffsets {
        handle: CompletableEventHandle<HashMap<TopicPartition, OffsetAndTimestamp>>,
        timestamps_to_search: HashMap<TopicPartition, i64>,
        require_timestamps: bool,
    },
    /// `CheckAndUpdatePositionsEvent` — verifies fetch positions or
    /// resets them per `auto.offset.reset`.
    CheckAndUpdatePositions {
        handle: CompletableEventHandle<()>,
    },
    /// `ResetOffsetEvent`.
    ResetOffset {
        handle: CompletableEventHandle<()>,
        partitions: HashSet<TopicPartition>,
        offset_reset_strategy: AutoOffsetResetStrategy,
    },
    /// `TopicMetadataEvent` — metadata request for a single topic.
    TopicMetadata {
        handle: CompletableEventHandle<HashMap<String, Vec<PartitionInfo>>>,
        topic: String,
    },
    /// `AllTopicsMetadataEvent` — metadata request for all topics.
    AllTopicsMetadata {
        handle: CompletableEventHandle<HashMap<String, Vec<PartitionInfo>>>,
    },
    /// `TopicSubscriptionChangeEvent` — concrete-topics subscribe.
    TopicSubscriptionChange {
        handle: CompletableEventHandle<()>,
        topics: HashSet<String>,
    },
    /// `TopicPatternSubscriptionChangeEvent` — client-side regex subscribe.
    TopicPatternSubscriptionChange {
        handle: CompletableEventHandle<()>,
        pattern: Regex,
    },
    /// `TopicRe2JPatternSubscriptionChangeEvent` — server-side regex
    /// subscribe (KIP-848).
    TopicRe2JPatternSubscriptionChange {
        handle: CompletableEventHandle<()>,
        pattern: SubscriptionPattern,
    },
    /// `UnsubscribeEvent`.
    Unsubscribe {
        handle: CompletableEventHandle<()>,
    },
    /// `CreateFetchRequestsEvent` — sometimes called eagerly to populate
    /// the fetch buffer ahead of `poll()`.
    CreateFetchRequests {
        handle: CompletableEventHandle<()>,
    },
    /// `PausePartitionsEvent`.
    PausePartitions {
        handle: CompletableEventHandle<()>,
        partitions: HashSet<TopicPartition>,
    },
    /// `ResumePartitionsEvent`.
    ResumePartitions {
        handle: CompletableEventHandle<()>,
        partitions: HashSet<TopicPartition>,
    },
    /// `CurrentLagEvent`. Result is `Some(lag)` when the lag is known,
    /// otherwise `None` (Java returns `OptionalLong`).
    CurrentLag {
        handle: CompletableEventHandle<Option<i64>>,
        partition: TopicPartition,
    },
    /// `SeekUnvalidatedEvent`.
    SeekUnvalidated {
        handle: CompletableEventHandle<()>,
        partition: TopicPartition,
        offset: i64,
        /// Java's `Optional<Integer> offsetEpoch`.
        offset_epoch: Option<i32>,
    },
}

impl ApplicationEvent {
    /// Java equivalent: `ApplicationEvent.type().name()` — used in log /
    /// `toString()` output. Returns the variant name verbatim.
    pub(crate) fn type_name(&self) -> &'static str {
        match self {
            Self::AssignmentChange { .. } => "AssignmentChange",
            Self::CommitOnClose => "CommitOnClose",
            Self::LeaveGroupOnClose { .. } => "LeaveGroupOnClose",
            Self::StopFindCoordinatorOnClose => "StopFindCoordinatorOnClose",
            Self::NewTopicsMetadataUpdate => "NewTopicsMetadataUpdate",
            Self::ConsumerRebalanceListenerCallbackCompleted { .. } => "ConsumerRebalanceListenerCallbackCompleted",
            Self::UpdatePatternSubscription => "UpdatePatternSubscription",
            Self::CommitAsync { .. } => "CommitAsync",
            Self::CommitSync { .. } => "CommitSync",
            Self::AsyncPoll { .. } => "AsyncPoll",
            Self::FetchCommittedOffsets { .. } => "FetchCommittedOffsets",
            Self::ListOffsets { .. } => "ListOffsets",
            Self::CheckAndUpdatePositions { .. } => "CheckAndUpdatePositions",
            Self::ResetOffset { .. } => "ResetOffset",
            Self::TopicMetadata { .. } => "TopicMetadata",
            Self::AllTopicsMetadata { .. } => "AllTopicsMetadata",
            Self::TopicSubscriptionChange { .. } => "TopicSubscriptionChange",
            Self::TopicPatternSubscriptionChange { .. } => "TopicPatternSubscriptionChange",
            Self::TopicRe2JPatternSubscriptionChange { .. } => "TopicRe2JPatternSubscriptionChange",
            Self::Unsubscribe { .. } => "Unsubscribe",
            Self::CreateFetchRequests { .. } => "CreateFetchRequests",
            Self::PausePartitions { .. } => "PausePartitions",
            Self::ResumePartitions { .. } => "ResumePartitions",
            Self::CurrentLag { .. } => "CurrentLag",
            Self::SeekUnvalidated { .. } => "SeekUnvalidated",
        }
    }
}

impl std::fmt::Debug for ApplicationEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // `CompletableEventHandle` doesn't implement `Debug` (its inner
        // `oneshot::Sender` doesn't either), so we print only the variant
        // name — matching the spirit of Java's
        // `event.getClass().getSimpleName()`.
        write!(f, "{}", self.type_name())
    }
}

impl std::fmt::Display for ApplicationEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Java prints `ClassName{type=X, enqueuedMs=Y}`. We have no
        // `enqueuedMs` here (it's on the envelope), so we print just the
        // variant name.
        write!(f, "{}{{type={}}}", self.type_name(), self.type_name())
    }
}

/// Wraps an [`ApplicationEvent`] with the timestamp at which it was
/// enqueued. Java stores `enqueuedMs` on the event itself; we keep it on
/// the envelope so the variant payload stays minimal and the reaper /
/// debug-logging concerns are isolated to the channel layer.
pub(crate) struct ApplicationEventEnvelope {
    pub event: ApplicationEvent,
    /// Wall-clock timestamp (milliseconds) at which the event was added to
    /// the channel. Set by [`crate::consumer::internals::events::ApplicationEventHandler::add`].
    pub enqueued_ms: i64,
}

impl std::fmt::Debug for ApplicationEventEnvelope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApplicationEventEnvelope")
            .field("event", &self.event)
            .field("enqueued_ms", &self.enqueued_ms)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::super::completable_event::make_completable_event;
    use super::*;

    #[test]
    fn type_name_covers_every_variant() {
        let (h, _rx, _erased) = make_completable_event::<()>(0);
        let ev = ApplicationEvent::AsyncPoll { handle: h };
        assert_eq!(ev.type_name(), "AsyncPoll");

        let ev = ApplicationEvent::CommitOnClose;
        assert_eq!(ev.type_name(), "CommitOnClose");

        let ev = ApplicationEvent::NewTopicsMetadataUpdate;
        assert_eq!(ev.type_name(), "NewTopicsMetadataUpdate");
    }

    #[test]
    fn envelope_records_enqueued_ms() {
        let (h, _rx, _erased) = make_completable_event::<()>(0);
        let env = ApplicationEventEnvelope {
            event: ApplicationEvent::AsyncPoll { handle: h },
            enqueued_ms: 123,
        };
        assert_eq!(env.enqueued_ms, 123);
        assert_eq!(env.event.type_name(), "AsyncPoll");
    }

    #[test]
    fn display_emits_variant_name() {
        let ev = ApplicationEvent::CommitOnClose;
        assert_eq!(format!("{}", ev), "CommitOnClose{type=CommitOnClose}");
    }
}
