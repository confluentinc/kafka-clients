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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use regex::Regex;

use crate::common::{Error, IsolationLevel, PartitionInfo, TopicPartition};
use crate::consumer::consumer_rebalance_listener::ConsumerRebalanceListener;
use crate::consumer::consumer_rebalance_listener_method_name::ConsumerRebalanceListenerMethodName;
use crate::consumer::internals::auto_offset_reset_strategy::AutoOffsetResetStrategy;
use crate::consumer::internals::offset_and_timestamp_internal::OffsetAndTimestampInternal;
use crate::consumer::{GroupMembershipOperation, OffsetAndMetadata, SubscriptionPattern};

use super::completable_event::CompletableEventHandle;

/// Single-enum translation of Java's `ApplicationEvent` hierarchy.
///
/// The variant discriminant takes the place of Java's `Type` enum.
/// `enqueued_ms` is stored on the [`ApplicationEventEnvelope`] wrapper
/// rather than on each variant, so the bg-task pattern matches stay free
/// of bookkeeping fields.
pub(crate) enum ApplicationEvent {
    // ─── Non-completable events ───
    /// `CommitOnCloseEvent` — fire-and-forget commit during close.
    CommitOnClose,
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
        error: Option<Error>,
    },
    /// `AsyncPollEvent` — pumps the membership / fetch state machine.
    ///
    /// Java's `AsyncPollEvent extends ApplicationEvent` (NOT
    /// `CompletableApplicationEvent`) and implements
    /// [`MetadataErrorNotifiable`]. It is a non-blocking two-stage state
    /// machine: the app side submits it but does not block on a future;
    /// instead it polls `state.is_complete()` and `state.error()` between
    /// iterations. See `AsyncPollEvent.java`.
    AsyncPoll {
        deadline_ms: i64,
        poll_time_ms: i64,
        state: Arc<AsyncPollState>,
    },

    // ─── Completable events ───
    /// `AssignmentChangeEvent` — `consumer.assign(...)`. Replaces the
    /// current assignment with `partitions`. Completable: the app side
    /// waits for the bg task to acknowledge the assignment change
    /// before returning from `assign()`.
    AssignmentChange {
        handle: CompletableEventHandle<()>,
        current_time_ms: i64,
        partitions: HashSet<TopicPartition>,
    },
    /// `LeaveGroupOnCloseEvent` — instructs the membership manager to
    /// send a final heartbeat with the `leave-group` epoch. The event
    /// is considered complete when the membership manager receives the
    /// heartbeat response that it has left the group.
    LeaveGroupOnClose {
        handle: CompletableEventHandle<()>,
        membership_operation: GroupMembershipOperation,
    },
    /// `UpdatePatternSubscriptionEvent` — forces a re-evaluation of the
    /// subscribed regex against the latest metadata. The app-side
    /// `subscribe(pattern)` path waits for completion before returning.
    UpdatePatternSubscription { handle: CompletableEventHandle<()> },
    /// `AsyncCommitEvent` — `consumer.commit_async(offsets)`.
    ///
    /// Mirrors Java's `CommitEvent` base class: `handle` carries the
    /// `Map<TopicPartition, OffsetAndMetadata>` that was actually
    /// committed (so the app side can re-confirm), `offsets_ready` is
    /// the secondary `CompletableFuture<Void>` the bg task completes
    /// once it has resolved which offsets to commit, and `offsets`
    /// being `None` means "commit all consumed" (Java's `Optional<Map>`).
    CommitAsync {
        handle: CompletableEventHandle<HashMap<TopicPartition, OffsetAndMetadata>>,
        offsets_ready: CompletableEventHandle<()>,
        offsets: Option<HashMap<TopicPartition, OffsetAndMetadata>>,
    },
    /// `SyncCommitEvent` — `consumer.commit_sync(offsets, timeout)`.
    /// Same shape as [`CommitAsync`](Self::CommitAsync) — see its docs.
    CommitSync {
        handle: CompletableEventHandle<HashMap<TopicPartition, OffsetAndMetadata>>,
        offsets_ready: CompletableEventHandle<()>,
        offsets: Option<HashMap<TopicPartition, OffsetAndMetadata>>,
    },
    /// `FetchCommittedOffsetsEvent`.
    FetchCommittedOffsets {
        handle: CompletableEventHandle<HashMap<TopicPartition, OffsetAndMetadata>>,
        partitions: HashSet<TopicPartition>,
    },
    /// `ListOffsetsEvent`. Implements [`MetadataErrorNotifiable`].
    ///
    /// Returns
    /// `HashMap<TopicPartition, Option<OffsetAndTimestampInternal>>` —
    /// matching Java's
    /// `Map<TopicPartition, OffsetAndTimestampInternal>` payload.
    /// `OffsetAndTimestampInternal` permits the broker-returned
    /// `timestamp == -1` sentinel for `EARLIEST` / `LATEST` queries
    /// (see Java's `OffsetAndTimestampInternal.java` class doc, and
    /// COMMENTS.DONE.1.md Issue 6 for the regression where translating
    /// this as the public-class `OffsetAndTimestamp` silently elided
    /// every `endOffsets(tp)` entry). The `Option` wrapper mirrors
    /// Java's nullable-map-value sentinel
    /// (`ListOffsetsEvent.emptyResults()` populates `null` per
    /// partition) that the bg task uses when no offset is found at
    /// all.
    ///
    /// App side: `offsets_for_times` calls
    /// `OffsetAndTimestampInternal::build_offset_and_timestamp` per
    /// entry to convert to the public-class
    /// [`crate::consumer::OffsetAndTimestamp`].
    /// `beginning_or_end_offsets` reads `.offset()` directly off this
    /// payload (the broker may return `timestamp == -1` for
    /// `LATEST` / `EARLIEST`, but the public surface returns only the
    /// offset value).
    ListOffsets {
        handle: CompletableEventHandle<HashMap<TopicPartition, Option<OffsetAndTimestampInternal>>>,
        timestamps_to_search: HashMap<TopicPartition, i64>,
        require_timestamps: bool,
    },
    /// `CheckAndUpdatePositionsEvent` — verifies fetch positions or
    /// resets them per `auto.offset.reset`. Implements
    /// [`MetadataErrorNotifiable`].
    CheckAndUpdatePositions { handle: CompletableEventHandle<()> },
    /// `ResetOffsetEvent`.
    ResetOffset {
        handle: CompletableEventHandle<()>,
        partitions: HashSet<TopicPartition>,
        offset_reset_strategy: AutoOffsetResetStrategy,
    },
    /// `TopicMetadataEvent` — metadata request for a single topic.
    /// Implements [`MetadataErrorNotifiable`].
    TopicMetadata {
        handle: CompletableEventHandle<HashMap<String, Vec<PartitionInfo>>>,
        topic: String,
    },
    /// `AllTopicsMetadataEvent` — metadata request for all topics.
    /// Implements [`MetadataErrorNotifiable`].
    AllTopicsMetadata {
        handle: CompletableEventHandle<HashMap<String, Vec<PartitionInfo>>>,
    },
    /// `TopicSubscriptionChangeEvent` — concrete-topics subscribe.
    ///
    /// `listener` mirrors Java's `SubscriptionChangeEvent.listener` —
    /// `Optional<ConsumerRebalanceListener>` carried so the bg-task
    /// `subscribe_topics(...)` call can register it against
    /// `SubscriptionState`.
    TopicSubscriptionChange {
        handle: CompletableEventHandle<()>,
        topics: HashSet<String>,
        listener: Option<Arc<dyn ConsumerRebalanceListener>>,
    },
    /// `TopicPatternSubscriptionChangeEvent` — client-side regex subscribe.
    ///
    /// `listener` mirrors Java's `SubscriptionChangeEvent.listener`.
    TopicPatternSubscriptionChange {
        handle: CompletableEventHandle<()>,
        pattern: Regex,
        listener: Option<Arc<dyn ConsumerRebalanceListener>>,
    },
    /// `TopicRe2JPatternSubscriptionChangeEvent` — server-side regex
    /// subscribe (KIP-848).
    ///
    /// `listener` mirrors Java's `SubscriptionChangeEvent.listener`.
    TopicRe2JPatternSubscriptionChange {
        handle: CompletableEventHandle<()>,
        pattern: SubscriptionPattern,
        listener: Option<Arc<dyn ConsumerRebalanceListener>>,
    },
    /// `UnsubscribeEvent`.
    Unsubscribe { handle: CompletableEventHandle<()> },
    /// `CreateFetchRequestsEvent` — sometimes called eagerly to populate
    /// the fetch buffer ahead of `poll()`.
    CreateFetchRequests { handle: CompletableEventHandle<()> },
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
    /// otherwise `None` (Java returns `OptionalLong`). `isolation_level`
    /// determines whether the lag is HW-based (`ReadUncommitted`) or
    /// LSO-based (`ReadCommitted`).
    CurrentLag {
        handle: CompletableEventHandle<Option<i64>>,
        partition: TopicPartition,
        isolation_level: IsolationLevel,
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
            Self::UpdatePatternSubscription { .. } => "UpdatePatternSubscription",
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

    /// Dispatches a metadata-error notification to any variant that
    /// implements Java's `MetadataErrorNotifiableEvent` interface.
    ///
    /// Returns `true` if THIS variant was metadata-error-notifiable and
    /// the error was recorded — the caller can use this to decide
    /// whether to also skip the event's normal processing (see Java's
    /// contract: a notified event is NOT subsequently passed to
    /// `ApplicationEventProcessor#process`).
    ///
    /// Translated from
    /// `org.apache.kafka.clients.consumer.internals.events.MetadataErrorNotifiableEvent.onMetadataError`.
    /// Java has the trait per-class; in Rust we centralise the dispatch
    /// on the enum so the processor doesn't need a runtime down-cast.
    pub(crate) fn on_metadata_error(&self, error: Error) -> bool {
        match self {
            Self::AsyncPoll { state, .. } => {
                state.complete_exceptionally(error);
                true
            },
            Self::CheckAndUpdatePositions { handle } => {
                handle.complete_exceptionally(error);
                true
            },
            Self::ListOffsets { handle, .. } => {
                handle.complete_exceptionally(error);
                true
            },
            Self::TopicMetadata { handle, .. } => {
                handle.complete_exceptionally(error);
                true
            },
            Self::AllTopicsMetadata { handle } => {
                handle.complete_exceptionally(error);
                true
            },
            _ => false,
        }
    }

    /// Returns `true` if this variant implements Java's
    /// `MetadataErrorNotifiableEvent` interface — i.e. would be a
    /// candidate for [`Self::on_metadata_error`] dispatch.
    ///
    /// The set is kept in sync with [`Self::on_metadata_error`]: any
    /// variant that returns `true` from this predicate must also have
    /// a non-`_` arm in `on_metadata_error`. The `metadata_error_notifiable_predicate_matches_on_metadata_error`
    /// test below enforces this with `assert_eq!(predicate, dispatched)`.
    ///
    /// Used by [`crate::consumer::internals::consumer_network_thread::ConsumerNetworkThread::process_application_events`]
    /// to decide whether to query `network_client_delegate.get_and_clear_metadata_error()`
    /// in the per-event arm (Java mirrors this with `event instanceof
    /// MetadataErrorNotifiableEvent`).
    pub(crate) fn is_metadata_error_notifiable(&self) -> bool {
        matches!(
            self,
            Self::AsyncPoll { .. }
                | Self::CheckAndUpdatePositions { .. }
                | Self::ListOffsets { .. }
                | Self::TopicMetadata { .. }
                | Self::AllTopicsMetadata { .. }
        )
    }

    /// Returns the type-erased completable handle for variants that
    /// extend Java's `CompletableApplicationEvent<T>` (i.e. carry a
    /// `handle: CompletableEventHandle<T>` field). Non-completable
    /// variants return `None`.
    ///
    /// Used by [`crate::consumer::internals::consumer_network_thread::ConsumerNetworkThread::process_application_events`]
    /// to register the handle with the [`super::completable_event_reaper::CompletableEventReaper`]
    /// — mirrors Java's
    /// `if (event instanceof CompletableEvent) applicationEventReaper.add((CompletableEvent<?>) event)`.
    pub(crate) fn erased_handle(
        &self,
    ) -> Option<std::sync::Arc<dyn super::completable_event::CompletableEventErasedHandle>> {
        match self {
            // Completable variants — return the erased handle.
            Self::AssignmentChange { handle, .. } => Some(handle.erased()),
            Self::LeaveGroupOnClose { handle, .. } => Some(handle.erased()),
            Self::UpdatePatternSubscription { handle } => Some(handle.erased()),
            Self::CommitAsync { handle, .. } => Some(handle.erased()),
            Self::CommitSync { handle, .. } => Some(handle.erased()),
            Self::FetchCommittedOffsets { handle, .. } => Some(handle.erased()),
            Self::ListOffsets { handle, .. } => Some(handle.erased()),
            Self::CheckAndUpdatePositions { handle } => Some(handle.erased()),
            Self::ResetOffset { handle, .. } => Some(handle.erased()),
            Self::TopicMetadata { handle, .. } => Some(handle.erased()),
            Self::AllTopicsMetadata { handle } => Some(handle.erased()),
            Self::TopicSubscriptionChange { handle, .. } => Some(handle.erased()),
            Self::TopicPatternSubscriptionChange { handle, .. } => Some(handle.erased()),
            Self::TopicRe2JPatternSubscriptionChange { handle, .. } => Some(handle.erased()),
            Self::Unsubscribe { handle } => Some(handle.erased()),
            Self::CreateFetchRequests { handle } => Some(handle.erased()),
            Self::PausePartitions { handle, .. } => Some(handle.erased()),
            Self::ResumePartitions { handle, .. } => Some(handle.erased()),
            Self::CurrentLag { handle, .. } => Some(handle.erased()),
            Self::SeekUnvalidated { handle, .. } => Some(handle.erased()),
            // Non-completable variants — Java: not `instanceof CompletableEvent`.
            Self::CommitOnClose
            | Self::StopFindCoordinatorOnClose
            | Self::NewTopicsMetadataUpdate
            | Self::ConsumerRebalanceListenerCallbackCompleted { .. }
            | Self::AsyncPoll { .. } => None,
        }
    }

    /// Returns the type-erased completable handle for variants that are
    /// BOTH `is_metadata_error_notifiable()` AND extend
    /// `CompletableApplicationEvent<T>` — i.e. would appear in Java's
    /// `applicationEventReaper.uncompletedEvents()` filtered for
    /// `instanceof MetadataErrorNotifiableEvent`.
    ///
    /// This is exactly the set used by Java's post-poll
    /// `maybeFailOnMetadataError(uncompletedEvents)` arm. Note `AsyncPoll`
    /// is `is_metadata_error_notifiable()` but is NOT a `CompletableEvent`
    /// in Java — so it does NOT appear in `uncompletedEvents()` and is
    /// correctly excluded here. The per-event arm inside
    /// `processApplicationEvents` (which handles "immediately completed
    /// events") still notifies `AsyncPoll` via `on_metadata_error`.
    ///
    /// Used by [`crate::consumer::internals::consumer_network_thread::ConsumerNetworkThread`]
    /// to track in-flight notifiable events for the post-poll
    /// `maybeFailOnMetadataError(uncompletedEvents)` arm.
    pub(crate) fn metadata_error_notifiable_handle(
        &self,
    ) -> Option<std::sync::Arc<dyn super::completable_event::CompletableEventErasedHandle>> {
        match self {
            Self::CheckAndUpdatePositions { handle } => Some(handle.erased()),
            Self::ListOffsets { handle, .. } => Some(handle.erased()),
            Self::TopicMetadata { handle, .. } => Some(handle.erased()),
            Self::AllTopicsMetadata { handle } => Some(handle.erased()),
            // AsyncPoll is metadata-error-notifiable but is NOT a
            // CompletableApplicationEvent in Java — so it never appears
            // in `uncompletedEvents()`.
            _ => None,
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
        // Java prints `ClassName{type=X, enqueuedMs=Y}` — the class name
        // and the `Type` enum are conceptually distinct. In Rust the
        // variant name plays both roles, so we drop the redundant
        // `type=` field. The envelope's `Display` adds `enqueued_ms`.
        write!(f, "{}{{}}", self.type_name())
    }
}

/// `AsyncPollEvent`'s mutable state.
///
/// Translated from the `volatile` fields on
/// `org.apache.kafka.clients.consumer.internals.events.AsyncPollEvent`.
/// The struct is shared between the app side and the background task via
/// `Arc<AsyncPollState>`; the app side observes
/// [`is_complete`](Self::is_complete) and [`error`](Self::error) between
/// `poll()` iterations, while the bg task drives the state forward via
/// [`complete_successfully`](Self::complete_successfully),
/// [`complete_exceptionally`](Self::complete_exceptionally), and
/// [`mark_validate_positions_complete`](Self::mark_validate_positions_complete).
pub(crate) struct AsyncPollState {
    /// `volatile boolean isComplete` — Java's primary completion flag.
    is_complete: AtomicBool,
    /// `volatile boolean isValidatePositionsComplete` — first-stage
    /// marker, set by the bg task once `CheckAndUpdatePositionsEvent`
    /// logic has finished.
    is_validate_positions_complete: AtomicBool,
    /// `volatile KafkaException error` — set on exceptional completion.
    /// Wrapped in `Mutex<Option<...>>` because `Error` is not
    /// trivially `AtomicPtr`-shareable.
    error: Mutex<Option<Error>>,
}

impl AsyncPollState {
    /// Fresh state — all flags clear, no error.
    pub(crate) fn new() -> Self {
        Self {
            is_complete: AtomicBool::new(false),
            is_validate_positions_complete: AtomicBool::new(false),
            error: Mutex::new(None),
        }
    }

    /// Java: `isComplete()`.
    pub(crate) fn is_complete(&self) -> bool {
        self.is_complete.load(Ordering::Acquire)
    }

    /// Java: `isValidatePositionsComplete()`.
    pub(crate) fn is_validate_positions_complete(&self) -> bool {
        self.is_validate_positions_complete.load(Ordering::Acquire)
    }

    /// Java: `markValidatePositionsComplete()`.
    pub(crate) fn mark_validate_positions_complete(&self) {
        self.is_validate_positions_complete.store(true, Ordering::Release);
    }

    /// Java: `completeSuccessfully()`.
    pub(crate) fn complete_successfully(&self) {
        self.is_complete.store(true, Ordering::Release);
    }

    /// Java: `completeExceptionally(KafkaException e)` — stores the
    /// error AND sets `is_complete`.
    pub(crate) fn complete_exceptionally(&self, err: Error) {
        let mut guard = match self.error.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        // Java overwrites unconditionally; do the same for fidelity.
        *guard = Some(err);
        drop(guard);
        self.is_complete.store(true, Ordering::Release);
    }

    /// Java: `error()` — returns `Optional<KafkaException>`.
    ///
    /// Clones the error if present (Java returns a shared reference; in
    /// Rust the receiver may need to inspect/raise it independently of
    /// the bg task observing it again).
    pub(crate) fn error(&self) -> Option<Error> {
        let guard = match self.error.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.clone()
    }
}

impl Default for AsyncPollState {
    fn default() -> Self {
        Self::new()
    }
}

impl ApplicationEvent {
    /// Java: `AsyncPollEvent.isExpired(time)` — only meaningful for the
    /// `AsyncPoll` variant. Returns `false` for any other variant.
    pub(crate) fn async_poll_is_expired(&self, current_time_ms: i64) -> bool {
        match self {
            Self::AsyncPoll { deadline_ms, .. } => current_time_ms >= *deadline_ms,
            _ => false,
        }
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

impl std::fmt::Display for ApplicationEventEnvelope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Java: `ClassName{type=X, enqueuedMs=Y}` — see
        // `ApplicationEvent.toString()`. We emit `<variant>{enqueued_ms=Y}`
        // (no redundant `type=` since the variant name plays both roles).
        write!(f, "{}{{enqueued_ms={}}}", self.event.type_name(), self.enqueued_ms)
    }
}

#[cfg(test)]
mod tests {
    use super::super::completable_event::make_completable_event;
    use super::*;

    fn new_async_poll(deadline_ms: i64, poll_time_ms: i64) -> ApplicationEvent {
        ApplicationEvent::AsyncPoll { deadline_ms, poll_time_ms, state: Arc::new(AsyncPollState::new()) }
    }

    #[test]
    fn type_name_covers_every_variant() {
        let ev = new_async_poll(0, 0);
        assert_eq!(ev.type_name(), "AsyncPoll");

        let ev = ApplicationEvent::CommitOnClose;
        assert_eq!(ev.type_name(), "CommitOnClose");

        let ev = ApplicationEvent::NewTopicsMetadataUpdate;
        assert_eq!(ev.type_name(), "NewTopicsMetadataUpdate");
    }

    #[test]
    fn envelope_records_enqueued_ms() {
        let env = ApplicationEventEnvelope { event: new_async_poll(0, 0), enqueued_ms: 123 };
        assert_eq!(env.enqueued_ms, 123);
        assert_eq!(env.event.type_name(), "AsyncPoll");
    }

    #[test]
    fn display_emits_variant_name() {
        let ev = ApplicationEvent::CommitOnClose;
        assert_eq!(format!("{}", ev), "CommitOnClose{}");
    }

    #[test]
    fn envelope_display_includes_enqueued_ms() {
        let env = ApplicationEventEnvelope { event: ApplicationEvent::CommitOnClose, enqueued_ms: 7 };
        assert_eq!(format!("{}", env), "CommitOnClose{enqueued_ms=7}");
    }

    #[test]
    fn async_poll_state_completes_successfully() {
        let state = AsyncPollState::new();
        assert!(!state.is_complete());
        assert!(!state.is_validate_positions_complete());
        assert!(state.error().is_none());

        state.mark_validate_positions_complete();
        assert!(state.is_validate_positions_complete());
        assert!(!state.is_complete());

        state.complete_successfully();
        assert!(state.is_complete());
        assert!(state.error().is_none());
    }

    #[test]
    fn async_poll_state_completes_exceptionally() {
        let state = AsyncPollState::new();
        state.complete_exceptionally(Error::timeout("boom"));
        assert!(state.is_complete());
        let err = state.error().expect("error present");
        assert!(matches!(err, Error::Timeout(_)));
    }

    #[test]
    fn async_poll_is_expired_compares_deadline() {
        let ev = new_async_poll(100, 50);
        assert!(!ev.async_poll_is_expired(50));
        assert!(ev.async_poll_is_expired(100));
        assert!(ev.async_poll_is_expired(200));

        // Non-AsyncPoll variants always return false.
        let other = ApplicationEvent::CommitOnClose;
        assert!(!other.async_poll_is_expired(i64::MAX));
    }

    #[test]
    fn on_metadata_error_dispatches_only_for_notifiable_variants() {
        // `AsyncPoll` is notifiable: state.error is populated.
        let state = Arc::new(AsyncPollState::new());
        let ev = ApplicationEvent::AsyncPoll { deadline_ms: 0, poll_time_ms: 0, state: Arc::clone(&state) };
        assert!(ev.on_metadata_error(Error::timeout("md")));
        assert!(state.is_complete());
        assert!(matches!(state.error().unwrap(), Error::Timeout(_)));

        // `CheckAndUpdatePositions` is notifiable.
        let (handle, mut rx, _erased) = make_completable_event::<()>(0);
        let ev = ApplicationEvent::CheckAndUpdatePositions { handle };
        assert!(ev.on_metadata_error(Error::timeout("md")));
        assert!(matches!(rx.try_recv().unwrap(), Err(Error::Timeout(_))));

        // `ListOffsets` is notifiable.
        let (handle, mut rx, _erased) =
            make_completable_event::<HashMap<TopicPartition, Option<OffsetAndTimestampInternal>>>(0);
        let ev =
            ApplicationEvent::ListOffsets { handle, timestamps_to_search: HashMap::new(), require_timestamps: false };
        assert!(ev.on_metadata_error(Error::timeout("md")));
        assert!(matches!(rx.try_recv().unwrap(), Err(Error::Timeout(_))));

        // `TopicMetadata` is notifiable.
        let (handle, mut rx, _erased) = make_completable_event::<HashMap<String, Vec<PartitionInfo>>>(0);
        let ev = ApplicationEvent::TopicMetadata { handle, topic: "t".to_string() };
        assert!(ev.on_metadata_error(Error::timeout("md")));
        assert!(matches!(rx.try_recv().unwrap(), Err(Error::Timeout(_))));

        // `AllTopicsMetadata` is notifiable.
        let (handle, mut rx, _erased) = make_completable_event::<HashMap<String, Vec<PartitionInfo>>>(0);
        let ev = ApplicationEvent::AllTopicsMetadata { handle };
        assert!(ev.on_metadata_error(Error::timeout("md")));
        assert!(matches!(rx.try_recv().unwrap(), Err(Error::Timeout(_))));

        // Non-notifiable variants return false and do not consume the
        // handle's sender.
        let ev = ApplicationEvent::CommitOnClose;
        assert!(!ev.on_metadata_error(Error::timeout("md")));
    }

    /// Verifies `is_metadata_error_notifiable()` agrees with the set of
    /// variants for which `on_metadata_error()` returns `true`. The
    /// predicate is consumed by the bg-task drain in
    /// `consumer_network_thread.rs` to decide whether to query the
    /// delegate's metadata-error slot; drift between the two would
    /// silently change the behaviour at the per-event arm.
    #[test]
    fn metadata_error_notifiable_predicate_matches_on_metadata_error() {
        // For each variant we know is notifiable, the predicate must
        // return true and `on_metadata_error` must return true.
        let state = Arc::new(AsyncPollState::new());
        let ev = ApplicationEvent::AsyncPoll { deadline_ms: 0, poll_time_ms: 0, state };
        assert!(ev.is_metadata_error_notifiable());
        assert!(ev.on_metadata_error(Error::timeout("md")));

        let (handle, _rx, _erased) = make_completable_event::<()>(0);
        let ev = ApplicationEvent::CheckAndUpdatePositions { handle };
        assert!(ev.is_metadata_error_notifiable());
        assert!(ev.on_metadata_error(Error::timeout("md")));

        // Non-notifiable variants: predicate false, dispatch false.
        let ev = ApplicationEvent::CommitOnClose;
        assert!(!ev.is_metadata_error_notifiable());
        assert!(!ev.on_metadata_error(Error::timeout("md")));

        let ev = ApplicationEvent::NewTopicsMetadataUpdate;
        assert!(!ev.is_metadata_error_notifiable());
        assert!(!ev.on_metadata_error(Error::timeout("md")));

        let ev = ApplicationEvent::StopFindCoordinatorOnClose;
        assert!(!ev.is_metadata_error_notifiable());
        assert!(!ev.on_metadata_error(Error::timeout("md")));
    }

    /// Verifies `erased_handle()` returns `Some` for variants extending
    /// Java's `CompletableApplicationEvent<T>` and `None` for
    /// non-completable variants.
    #[test]
    fn erased_handle_returns_some_for_completable_variants() {
        // Completable variants.
        let (h, _rx, _e) = make_completable_event::<()>(0);
        let ev = ApplicationEvent::AssignmentChange { handle: h, current_time_ms: 0, partitions: HashSet::new() };
        assert!(ev.erased_handle().is_some());

        let (h, _rx, _e) = make_completable_event::<()>(0);
        let ev = ApplicationEvent::Unsubscribe { handle: h };
        assert!(ev.erased_handle().is_some());

        let (h, _rx, _e) = make_completable_event::<()>(0);
        let ev =
            ApplicationEvent::LeaveGroupOnClose { handle: h, membership_operation: GroupMembershipOperation::Default };
        assert!(ev.erased_handle().is_some());

        // Non-completable variants.
        assert!(ApplicationEvent::CommitOnClose.erased_handle().is_none());
        assert!(ApplicationEvent::StopFindCoordinatorOnClose.erased_handle().is_none());
        assert!(ApplicationEvent::NewTopicsMetadataUpdate.erased_handle().is_none());

        // `AsyncPoll` is bare (NOT CompletableApplicationEvent) — see
        // §28 of consumer-threading.md. Despite being metadata-error-
        // notifiable, it must NOT register with the reaper.
        let state = Arc::new(AsyncPollState::new());
        let ev = ApplicationEvent::AsyncPoll { deadline_ms: 0, poll_time_ms: 0, state };
        assert!(
            ev.erased_handle().is_none(),
            "AsyncPoll extends ApplicationEvent (not CompletableApplicationEvent) — must not register"
        );
    }

    /// Verifies `metadata_error_notifiable_handle()` returns `Some` for
    /// the intersection of `is_metadata_error_notifiable()` and
    /// `erased_handle().is_some()`, and `None` everywhere else.
    ///
    /// Java analog: post-poll `maybeFailOnMetadataError(uncompletedEvents)`
    /// iterates `applicationEventReaper.uncompletedEvents()` (which is the
    /// `CompletableEvent` set) and filters for
    /// `MetadataErrorNotifiableEvent`. The intersection is exactly the
    /// 4 events: `CheckAndUpdatePositions`, `ListOffsets`,
    /// `TopicMetadata`, `AllTopicsMetadata` — NOT `AsyncPoll` (which is
    /// notifiable but not completable).
    #[test]
    fn metadata_error_notifiable_handle_returns_intersection() {
        // CheckAndUpdatePositions — notifiable AND completable.
        let (h, _rx, _e) = make_completable_event::<()>(0);
        let ev = ApplicationEvent::CheckAndUpdatePositions { handle: h };
        assert!(ev.metadata_error_notifiable_handle().is_some());

        // ListOffsets — notifiable AND completable.
        let (h, _rx, _e) = make_completable_event(0);
        let ev = ApplicationEvent::ListOffsets {
            handle: h,
            timestamps_to_search: HashMap::new(),
            require_timestamps: false,
        };
        assert!(ev.metadata_error_notifiable_handle().is_some());

        // TopicMetadata — notifiable AND completable.
        let (h, _rx, _e) = make_completable_event(0);
        let ev = ApplicationEvent::TopicMetadata { handle: h, topic: "t".to_string() };
        assert!(ev.metadata_error_notifiable_handle().is_some());

        // AllTopicsMetadata — notifiable AND completable.
        let (h, _rx, _e) = make_completable_event(0);
        let ev = ApplicationEvent::AllTopicsMetadata { handle: h };
        assert!(ev.metadata_error_notifiable_handle().is_some());

        // AsyncPoll — notifiable but NOT completable. Must return None.
        let state = Arc::new(AsyncPollState::new());
        let ev = ApplicationEvent::AsyncPoll { deadline_ms: 0, poll_time_ms: 0, state };
        assert!(
            ev.metadata_error_notifiable_handle().is_none(),
            "AsyncPoll is notifiable but not CompletableEvent — must not appear in uncompletedEvents()"
        );

        // Completable but NOT notifiable — must return None.
        let (h, _rx, _e) = make_completable_event::<()>(0);
        let ev = ApplicationEvent::AssignmentChange { handle: h, current_time_ms: 0, partitions: HashSet::new() };
        assert!(ev.metadata_error_notifiable_handle().is_none());

        // Neither notifiable nor completable.
        assert!(ApplicationEvent::CommitOnClose.metadata_error_notifiable_handle().is_none());
        assert!(
            ApplicationEvent::NewTopicsMetadataUpdate
                .metadata_error_notifiable_handle()
                .is_none()
        );
    }

    #[test]
    fn assignment_change_carries_current_time_ms() {
        let (handle, _rx, _erased) = make_completable_event::<()>(0);
        let ev = ApplicationEvent::AssignmentChange { handle, current_time_ms: 1234, partitions: HashSet::new() };
        if let ApplicationEvent::AssignmentChange { current_time_ms, .. } = ev {
            assert_eq!(current_time_ms, 1234);
        } else {
            panic!("unexpected variant");
        }
    }

    #[test]
    fn leave_group_on_close_carries_membership_operation() {
        let (handle, _rx, _erased) = make_completable_event::<()>(0);
        let ev =
            ApplicationEvent::LeaveGroupOnClose { handle, membership_operation: GroupMembershipOperation::LeaveGroup };
        if let ApplicationEvent::LeaveGroupOnClose { membership_operation, .. } = ev {
            assert_eq!(membership_operation, GroupMembershipOperation::LeaveGroup);
        } else {
            panic!("unexpected variant");
        }
    }

    #[test]
    fn current_lag_carries_isolation_level() {
        let (handle, _rx, _erased) = make_completable_event::<Option<i64>>(0);
        let ev = ApplicationEvent::CurrentLag {
            handle,
            partition: TopicPartition::new("t".to_string(), 0),
            isolation_level: IsolationLevel::ReadCommitted,
        };
        if let ApplicationEvent::CurrentLag { isolation_level, .. } = ev {
            assert_eq!(isolation_level, IsolationLevel::ReadCommitted);
        } else {
            panic!("unexpected variant");
        }
    }

    #[test]
    fn commit_async_carries_offsets_ready_and_optional_offsets() {
        // Variant accepts `None` for "commit all consumed".
        let (handle, _rx, _erased) = make_completable_event::<HashMap<TopicPartition, OffsetAndMetadata>>(0);
        let (offsets_ready, mut ready_rx, _ready_erased) = make_completable_event::<()>(0);
        let ev = ApplicationEvent::CommitAsync { handle, offsets_ready, offsets: None };
        if let ApplicationEvent::CommitAsync { offsets, offsets_ready, .. } = ev {
            assert!(offsets.is_none());
            // Completing offsets_ready propagates to the receiver.
            assert!(offsets_ready.complete(()));
            assert!(matches!(ready_rx.try_recv().unwrap(), Ok(())));
        } else {
            panic!("unexpected variant");
        }
    }

    #[test]
    fn update_pattern_subscription_is_completable() {
        let (handle, _rx, _erased) = make_completable_event::<()>(0);
        let ev = ApplicationEvent::UpdatePatternSubscription { handle };
        assert_eq!(ev.type_name(), "UpdatePatternSubscription");
        if let ApplicationEvent::UpdatePatternSubscription { handle } = ev {
            assert!(handle.complete(()));
        } else {
            panic!("unexpected variant");
        }
    }
}
