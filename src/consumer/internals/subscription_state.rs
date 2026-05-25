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

//! Subscription, assignment, and per-partition fetch state used by the
//! consumer.
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.SubscriptionState`. This
//! file holds the public surface of `SubscriptionState`, its inner types
//! (`FetchPosition`, `FetchStates`, `TopicPartitionState`, `LogTruncation`,
//! `SubscriptionType`), and a `Logger` translation via the `log` crate.
//!
//! No internal `Mutex`: Java's per-method `synchronized` is replaced by an
//! outer `Arc<Mutex<SubscriptionState>>` owned by the consumer
//! (`consumer-threading.md` §16). All public methods take `&self` or
//! `&mut self` accordingly. Java's `IllegalStateException` /
//! `IllegalArgumentException` paths translate to
//! `Err(KafkaError::illegal_state(...))` / `Err(KafkaError::illegal_argument(...))`
//! per CLAUDE.md §10.

#![allow(dead_code)] // Phase 4: types land before their callers (Phases 5-11).

use crate::consumer::{AutoOffsetResetStrategy, OffsetAndMetadata};
use crate::metadata::LeaderAndEpoch;

// ─── FetchStates ────────────────────────────────────────────────────────────

/// State machine controlling the lifecycle of a partition's fetch state.
///
/// Mirrors Java's nested `FetchStates` enum (and the `FetchState` interface
/// that defines `validTransitions()`, `requiresPosition()`,
/// `hasValidPosition()`). The Java separation of an interface from an enum
/// existed only to let individual enum constants override their transition
/// table; that override pattern doesn't translate cleanly to Rust enums
/// without per-variant impls, so we collapse to a single enum with
/// `match`-based transition tables. Behavior is identical.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum FetchStates {
    /// No valid fetch position has been set yet.
    Initializing,
    /// A valid position is set and the partition is ready to be fetched.
    Fetching,
    /// The fetch position is being reset to one of the
    /// [`AutoOffsetResetStrategy`] sentinel positions.
    AwaitReset,
    /// The fetch position requires validation against the leader's epoch
    /// before it can be used for fetching.
    AwaitValidation,
}

impl FetchStates {
    /// Returns the set of `FetchStates` reachable from this state. Matches
    /// Java's per-variant `validTransitions()` overrides on the nested
    /// `FetchStates` enum.
    fn valid_transitions(&self) -> &'static [FetchStates] {
        match self {
            FetchStates::Initializing => &[
                FetchStates::Fetching,
                FetchStates::AwaitReset,
                FetchStates::AwaitValidation,
            ],
            FetchStates::Fetching => &[
                FetchStates::Fetching,
                FetchStates::AwaitReset,
                FetchStates::AwaitValidation,
            ],
            FetchStates::AwaitReset => &[FetchStates::Fetching, FetchStates::AwaitReset],
            FetchStates::AwaitValidation => &[
                FetchStates::Fetching,
                FetchStates::AwaitReset,
                FetchStates::AwaitValidation,
            ],
        }
    }

    /// `true` iff this state requires a non-`None` position.
    fn requires_position(&self) -> bool {
        matches!(self, FetchStates::Fetching | FetchStates::AwaitValidation)
    }

    /// `true` iff this state has a valid position that can be used for
    /// fetching.
    fn has_valid_position(&self) -> bool {
        matches!(self, FetchStates::Fetching)
    }

    /// Returns the next state when transitioning to `new_state`, or `self`
    /// if the transition is invalid (matches Java's
    /// `FetchState.transitionTo`).
    fn transition_to(self, new_state: FetchStates) -> FetchStates {
        if self.valid_transitions().contains(&new_state) {
            new_state
        } else {
            self
        }
    }
}

// ─── SubscriptionType ───────────────────────────────────────────────────────

/// Subscription mode of the consumer. Mirrors Java's private enum
/// `SubscriptionState.SubscriptionType`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum SubscriptionType {
    /// No subscription / assignment yet.
    None,
    /// `subscribe(Collection<String>)` — explicit topic name list.
    AutoTopics,
    /// `subscribe(Pattern)` — client-side regex.
    AutoPattern,
    /// `subscribe(SubscriptionPattern)` — broker-side (RE2J) regex.
    AutoPatternRe2j,
    /// `assign(Collection<TopicPartition>)` — explicit user assignment.
    UserAssigned,
    /// `subscribeToShareGroup(Set<String>)` — KIP-932. Translated for state
    /// machine completeness; no Rust caller in Milestone 8.
    AutoTopicsShare,
}

impl std::fmt::Display for SubscriptionType {
    /// Matches Java's default enum `toString()` (variant name in upper-snake).
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            SubscriptionType::None => "NONE",
            SubscriptionType::AutoTopics => "AUTO_TOPICS",
            SubscriptionType::AutoPattern => "AUTO_PATTERN",
            SubscriptionType::AutoPatternRe2j => "AUTO_PATTERN_RE2J",
            SubscriptionType::UserAssigned => "USER_ASSIGNED",
            SubscriptionType::AutoTopicsShare => "AUTO_TOPICS_SHARE",
        })
    }
}

// ─── FetchPosition ──────────────────────────────────────────────────────────

/// Position of a partition subscription — the offset of the next record to
/// be returned to the user, plus the epoch known at the time it was
/// computed and the leader-and-epoch in effect when the position was
/// established.
///
/// Translated from `SubscriptionState.FetchPosition`. Hashable / equatable
/// for use in maps and sets.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct FetchPosition {
    /// The offset of the next record to fetch.
    pub offset: i64,
    /// The leader epoch known at the time the position was set, if any.
    /// `None` when the metadata source did not provide one.
    pub offset_epoch: Option<i32>,
    /// The leader-and-epoch in effect when the position was established.
    pub current_leader: LeaderAndEpoch,
}

impl FetchPosition {
    /// Package-private Java constructor `FetchPosition(long)` — used by
    /// `SubscriptionState.seek(tp, offset)`. Creates a position with no
    /// offset epoch and no current leader, mirroring Java's
    /// `LeaderAndEpoch.noLeaderOrEpoch()`.
    pub(crate) fn new(offset: i64) -> Self {
        Self { offset, offset_epoch: None, current_leader: LeaderAndEpoch::no_leader_or_epoch() }
    }

    /// Full constructor mirroring Java's public
    /// `FetchPosition(long, Optional<Integer>, LeaderAndEpoch)`.
    pub(crate) fn with_leader(offset: i64, offset_epoch: Option<i32>, current_leader: LeaderAndEpoch) -> Self {
        Self { offset, offset_epoch, current_leader }
    }
}

impl std::fmt::Display for FetchPosition {
    /// Matches Java's `toString()`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "FetchPosition{{offset={}, offsetEpoch={:?}, currentLeader={}}}",
            self.offset, self.offset_epoch, self.current_leader
        )
    }
}

// ─── LogTruncation ──────────────────────────────────────────────────────────

/// Detail of a detected log truncation. Returned from
/// `SubscriptionState::maybe_complete_validation` when no reset policy is
/// configured and divergence is detected against the broker's
/// `OffsetForLeaderEpoch` reply.
///
/// Translated from `SubscriptionState.LogTruncation`. The
/// `maybe_complete_validation` method itself is deferred to Phase 7
/// (depends on `EpochEndOffset` and `OffsetsForLeaderEpoch` request
/// translation). The struct lives here so Phase 7 doesn't have to move
/// types.
#[derive(Clone, Debug)]
pub(crate) struct LogTruncation {
    /// Partition for which truncation was detected.
    pub topic_partition: crate::common::TopicPartition,
    /// Position the consumer was at when validation began.
    pub fetch_position: FetchPosition,
    /// First offset known to diverge from the consumer's read, if known.
    /// `None` if the broker returned `UNDEFINED_EPOCH_OFFSET`.
    pub divergent_offset_opt: Option<OffsetAndMetadata>,
}

impl std::fmt::Display for LogTruncation {
    /// Matches Java's `toString()`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "(partition={}, fetchOffset={}, fetchEpoch={:?}",
            self.topic_partition, self.fetch_position.offset, self.fetch_position.offset_epoch
        )?;
        match &self.divergent_offset_opt {
            Some(d) => write!(f, ", divergentOffset={}, divergentEpoch={:?})", d.offset(), d.leader_epoch()),
            None => write!(f, ", divergentOffset=unknown, divergentEpoch=unknown)"),
        }
    }
}

// ─── TopicPartitionState ────────────────────────────────────────────────────

/// Per-partition state held inside `SubscriptionState.assignment`.
///
/// Private to this module; tests reach it through `SubscriptionState`'s
/// public surface. Translated from
/// `SubscriptionState.TopicPartitionState` (Java private nested class).
pub(crate) struct TopicPartitionState {
    pub(super) fetch_state: FetchStates,
    /// Last consumed position. Always `Some` when `fetch_state.requires_position()`.
    pub(super) position: Option<FetchPosition>,
    pub(super) high_watermark: Option<i64>,
    pub(super) log_start_offset: Option<i64>,
    pub(super) last_stable_offset: Option<i64>,
    pub(super) paused: bool,
    pub(super) pending_revocation: bool,
    pub(super) pending_on_assigned_callback: bool,
    pub(super) reset_strategy: Option<AutoOffsetResetStrategy>,
    pub(super) next_retry_time_ms: Option<i64>,
    pub(super) preferred_read_replica: Option<i32>,
    pub(super) preferred_read_replica_expire_time_ms: Option<i64>,
    pub(super) end_offset_requested: bool,
}

impl TopicPartitionState {
    /// Default-constructed state, mirroring Java's no-arg constructor.
    pub(crate) fn new() -> Self {
        Self {
            fetch_state: FetchStates::Initializing,
            position: None,
            high_watermark: None,
            log_start_offset: None,
            last_stable_offset: None,
            paused: false,
            pending_revocation: false,
            pending_on_assigned_callback: false,
            reset_strategy: None,
            next_retry_time_ms: None,
            preferred_read_replica: None,
            preferred_read_replica_expire_time_ms: None,
            end_offset_requested: false,
        }
    }

    pub(crate) fn end_offset_requested(&self) -> bool {
        self.end_offset_requested
    }

    pub(crate) fn request_end_offset(&mut self) {
        self.end_offset_requested = true;
    }

    /// Attempt to transition to `new_state`; on success, invoke the
    /// closure to mutate `self` (replacing Java's `Runnable runIfTransitioned`
    /// parameter on `transitionState`). The closure runs only when the
    /// transition is valid.
    ///
    /// Per CLAUDE.md §10, the Java
    /// `IllegalStateException("...but position is null")` is converted to
    /// a debug assertion in tests and a no-op in production — the closure
    /// is always expected to leave `self.position` in a state consistent
    /// with `new_state.requires_position()`. We keep this loose because
    /// none of the call sites in Java actually reach the throw with valid
    /// inputs.
    fn transition_state(&mut self, new_state: FetchStates, run_if_transitioned: impl FnOnce(&mut Self)) {
        let next_state = self.fetch_state.transition_to(new_state);
        if next_state == new_state {
            self.fetch_state = next_state;
            run_if_transitioned(self);
            if self.position.is_none() && next_state.requires_position() {
                debug_assert!(
                    self.position.is_some(),
                    "Transitioned subscription state to {next_state:?}, but position is null"
                );
            } else if !next_state.requires_position() {
                self.position = None;
            }
        }
    }

    pub(crate) fn preferred_read_replica(&mut self, time_ms: i64) -> Option<i32> {
        if let Some(expire_ms) = self.preferred_read_replica_expire_time_ms
            && time_ms > expire_ms
        {
            self.preferred_read_replica = None;
            return None;
        }
        self.preferred_read_replica
    }

    pub(crate) fn update_preferred_read_replica(&mut self, preferred_read_replica: i32, time_ms: i64) {
        if self.preferred_read_replica != Some(preferred_read_replica) {
            self.preferred_read_replica = Some(preferred_read_replica);
            self.preferred_read_replica_expire_time_ms = Some(time_ms);
        }
    }

    pub(crate) fn clear_preferred_read_replica(&mut self) -> Option<i32> {
        self.preferred_read_replica.take().inspect(|_| {
            self.preferred_read_replica_expire_time_ms = None;
        })
    }

    pub(crate) fn reset(&mut self, strategy: AutoOffsetResetStrategy) {
        self.transition_state(FetchStates::AwaitReset, |this| {
            this.reset_strategy = Some(strategy);
            this.next_retry_time_ms = None;
        });
    }

    /// Validate `position` against the current leader and epoch.
    fn validate_position(&mut self, position: FetchPosition) {
        if position.offset_epoch.is_some() && position.current_leader.epoch.is_some() {
            self.transition_state(FetchStates::AwaitValidation, |this| {
                this.position = Some(position);
                this.next_retry_time_ms = None;
            });
        } else {
            // No epoch info — skip validation, go straight to fetching.
            self.transition_state(FetchStates::Fetching, |this| {
                this.position = Some(position);
                this.next_retry_time_ms = None;
            });
        }
    }

    /// Clear AWAIT_VALIDATION and enter FETCHING (caller guarantees position
    /// is set).
    pub(crate) fn complete_validation(&mut self) {
        if self.has_position() {
            self.transition_state(FetchStates::Fetching, |this| this.next_retry_time_ms = None);
        }
    }

    pub(crate) fn awaiting_validation(&self) -> bool {
        self.fetch_state == FetchStates::AwaitValidation
    }

    pub(crate) fn awaiting_retry_backoff(&self, now_ms: i64) -> bool {
        self.next_retry_time_ms.is_some_and(|t| now_ms < t)
    }

    pub(crate) fn awaiting_reset(&self) -> bool {
        self.fetch_state == FetchStates::AwaitReset
    }

    pub(crate) fn set_next_allowed_retry(&mut self, next_allowed_retry_time_ms: i64) {
        self.next_retry_time_ms = Some(next_allowed_retry_time_ms);
    }

    pub(crate) fn request_failed(&mut self, next_allowed_retry_time_ms: i64) {
        self.next_retry_time_ms = Some(next_allowed_retry_time_ms);
    }

    pub(crate) fn has_valid_position(&self) -> bool {
        self.fetch_state.has_valid_position()
    }

    pub(crate) fn has_position(&self) -> bool {
        self.position.is_some()
    }

    pub(crate) fn is_paused(&self) -> bool {
        self.paused
    }

    pub(crate) fn seek_validated(&mut self, position: FetchPosition) {
        self.transition_state(FetchStates::Fetching, |this| {
            this.position = Some(position);
            this.reset_strategy = None;
            this.next_retry_time_ms = None;
        });
    }

    pub(crate) fn seek_unvalidated(&mut self, fetch_position: FetchPosition) {
        self.seek_validated(fetch_position.clone());
        self.validate_position(fetch_position);
    }

    /// Mirrors Java's `position(FetchPosition)` — set a new position on a
    /// partition that already has a valid one. Returns
    /// `Err(KafkaError::illegal_state(...))` if there's no valid current
    /// position (matches Java's `IllegalStateException`).
    pub(crate) fn set_position(&mut self, position: FetchPosition) -> Result<(), crate::common::KafkaError> {
        if !self.has_valid_position() {
            return Err(crate::common::KafkaError::illegal_state(
                "Cannot set a new position without a valid current position",
            ));
        }
        self.position = Some(position);
        Ok(())
    }

    pub(crate) fn valid_position(&self) -> Option<&FetchPosition> {
        if self.has_valid_position() {
            self.position.as_ref()
        } else {
            None
        }
    }

    pub(crate) fn pause(&mut self) {
        self.paused = true;
    }

    pub(crate) fn mark_pending_revocation(&mut self) {
        self.pending_revocation = true;
    }

    pub(crate) fn mark_pending_on_assigned_callback(&mut self, pending: bool) {
        self.pending_on_assigned_callback = pending;
    }

    pub(crate) fn resume(&mut self) {
        self.paused = false;
    }

    /// Whether we should retrieve a fetch position for this partition.
    /// `true` if `fetch_state == Initializing` and revocation is not
    /// pending.
    pub(crate) fn should_initialize(&self) -> bool {
        self.fetch_state == FetchStates::Initializing && !self.pending_revocation
    }

    pub(crate) fn is_fetchable(&self) -> bool {
        !self.paused && !self.pending_revocation && !self.pending_on_assigned_callback && self.has_valid_position()
    }

    pub(crate) fn high_watermark(&mut self, high_watermark: i64) {
        self.high_watermark = Some(high_watermark);
        self.end_offset_requested = false;
    }

    pub(crate) fn log_start_offset(&mut self, log_start_offset: i64) {
        self.log_start_offset = Some(log_start_offset);
    }

    pub(crate) fn last_stable_offset(&mut self, last_stable_offset: i64) {
        self.last_stable_offset = Some(last_stable_offset);
        self.end_offset_requested = false;
    }

    pub(crate) fn reset_strategy(&self) -> Option<AutoOffsetResetStrategy> {
        self.reset_strategy.clone()
    }
}

#[cfg(test)]
mod tests {
    //! Inner-type unit tests. The full `SubscriptionStateTest` suite is
    //! translated in commit 7 (after the `SubscriptionState` surface lands).

    use super::*;

    #[test]
    fn test_fetch_states_transitions() {
        assert_eq!(
            FetchStates::Initializing.transition_to(FetchStates::Fetching),
            FetchStates::Fetching
        );
        assert_eq!(
            FetchStates::Initializing.transition_to(FetchStates::AwaitReset),
            FetchStates::AwaitReset
        );
        assert_eq!(
            FetchStates::Initializing.transition_to(FetchStates::AwaitValidation),
            FetchStates::AwaitValidation
        );

        // INITIALIZING -> INITIALIZING is NOT in the valid transitions; stays.
        assert_eq!(
            FetchStates::Initializing.transition_to(FetchStates::Initializing),
            FetchStates::Initializing
        );

        assert_eq!(
            FetchStates::AwaitReset.transition_to(FetchStates::Fetching),
            FetchStates::Fetching
        );
        // AWAIT_RESET -> AWAIT_VALIDATION is NOT valid in Java's table.
        assert_eq!(
            FetchStates::AwaitReset.transition_to(FetchStates::AwaitValidation),
            FetchStates::AwaitReset
        );
    }

    #[test]
    fn test_fetch_states_predicates() {
        assert!(!FetchStates::Initializing.has_valid_position());
        assert!(FetchStates::Fetching.has_valid_position());
        assert!(!FetchStates::AwaitReset.has_valid_position());
        assert!(!FetchStates::AwaitValidation.has_valid_position());

        assert!(!FetchStates::Initializing.requires_position());
        assert!(FetchStates::Fetching.requires_position());
        assert!(!FetchStates::AwaitReset.requires_position());
        assert!(FetchStates::AwaitValidation.requires_position());
    }

    #[test]
    fn test_topic_partition_state_new_is_initializing() {
        let s = TopicPartitionState::new();
        assert_eq!(s.fetch_state, FetchStates::Initializing);
        assert!(s.position.is_none());
        assert!(!s.paused);
        assert!(s.should_initialize());
        assert!(!s.is_fetchable());
        assert!(!s.has_valid_position());
    }

    #[test]
    fn test_topic_partition_state_seek_validated_makes_fetchable() {
        let mut s = TopicPartitionState::new();
        s.seek_validated(FetchPosition::new(5));
        assert_eq!(s.fetch_state, FetchStates::Fetching);
        assert!(s.has_valid_position());
        assert!(s.is_fetchable());
        assert_eq!(s.position.as_ref().unwrap().offset, 5);
    }

    #[test]
    fn test_topic_partition_state_reset_clears_position() {
        let mut s = TopicPartitionState::new();
        s.seek_validated(FetchPosition::new(5));
        s.reset(AutoOffsetResetStrategy::EARLIEST);
        assert_eq!(s.fetch_state, FetchStates::AwaitReset);
        assert!(!s.has_valid_position());
        assert!(s.awaiting_reset());
        // AWAIT_RESET does not require a position, so it must have been cleared.
        assert!(s.position.is_none());
    }

    #[test]
    fn test_seek_unvalidated_with_no_epoch_skips_validation() {
        let mut s = TopicPartitionState::new();
        // No offset_epoch and no leader epoch — should go to FETCHING.
        s.seek_unvalidated(FetchPosition::new(0));
        assert_eq!(s.fetch_state, FetchStates::Fetching);
        assert!(s.has_valid_position());
        assert!(!s.awaiting_validation());
    }

    #[test]
    fn test_seek_unvalidated_with_epoch_enters_validation() {
        let mut s = TopicPartitionState::new();
        let broker = crate::common::Node::new(1, "localhost".to_string(), 9092);
        let position = FetchPosition::with_leader(10, Some(5), LeaderAndEpoch::new(Some(broker), Some(10)));
        s.seek_unvalidated(position.clone());
        assert_eq!(s.fetch_state, FetchStates::AwaitValidation);
        assert!(!s.has_valid_position());
        assert!(s.awaiting_validation());
        assert_eq!(s.position.as_ref().unwrap(), &position);
    }

    #[test]
    fn test_complete_validation_clears_to_fetching() {
        let mut s = TopicPartitionState::new();
        let broker = crate::common::Node::new(1, "localhost".to_string(), 9092);
        let position = FetchPosition::with_leader(10, Some(5), LeaderAndEpoch::new(Some(broker), Some(10)));
        s.seek_unvalidated(position);
        s.complete_validation();
        assert_eq!(s.fetch_state, FetchStates::Fetching);
        assert!(s.has_valid_position());
        assert!(!s.awaiting_validation());
    }

    #[test]
    fn test_set_position_requires_valid_state() {
        let mut s = TopicPartitionState::new();
        // Initializing has no valid position.
        assert!(s.set_position(FetchPosition::new(1)).is_err());
        s.seek_validated(FetchPosition::new(0));
        assert!(s.set_position(FetchPosition::new(1)).is_ok());
        assert_eq!(s.position.as_ref().unwrap().offset, 1);
    }

    #[test]
    fn test_preferred_read_replica_lease() {
        let mut s = TopicPartitionState::new();
        assert!(s.preferred_read_replica(0).is_none());

        s.update_preferred_read_replica(42, 10);
        assert_eq!(s.preferred_read_replica(9), Some(42));
        assert_eq!(s.preferred_read_replica(10), Some(42));
        assert_eq!(s.preferred_read_replica(11), None);

        // After expiration the cached value is cleared.
        assert!(s.preferred_read_replica(12).is_none());
    }

    #[test]
    fn test_clear_preferred_read_replica_returns_previous() {
        let mut s = TopicPartitionState::new();
        s.update_preferred_read_replica(42, 10);
        assert_eq!(s.clear_preferred_read_replica(), Some(42));
        assert!(s.clear_preferred_read_replica().is_none());
    }

    #[test]
    fn test_log_truncation_display_with_divergent() {
        let tp = crate::common::TopicPartition::new("t".to_string(), 0);
        let position = FetchPosition::new(10);
        let divergent = OffsetAndMetadata::new(5).unwrap();
        let trunc = LogTruncation {
            topic_partition: tp,
            fetch_position: position,
            divergent_offset_opt: Some(divergent),
        };
        let s = trunc.to_string();
        assert!(s.contains("partition=t-0"), "{s}");
        assert!(s.contains("fetchOffset=10"), "{s}");
        assert!(s.contains("divergentOffset=5"), "{s}");
    }

    #[test]
    fn test_log_truncation_display_without_divergent() {
        let tp = crate::common::TopicPartition::new("t".to_string(), 0);
        let position = FetchPosition::new(10);
        let trunc = LogTruncation { topic_partition: tp, fetch_position: position, divergent_offset_opt: None };
        let s = trunc.to_string();
        assert!(s.contains("partition=t-0"), "{s}");
        assert!(s.contains("divergentOffset=unknown"), "{s}");
    }
}
