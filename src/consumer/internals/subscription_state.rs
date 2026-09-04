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
//! `Err(Error::local_illegal_state(...))` / `Err(Error::local_illegal_argument(...))`
//! per CLAUDE.md §10.

#![allow(dead_code)] // Phase 4: types land before their callers (Phases 5-11).

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use log::{debug, info};
use regex::Regex;

use crate::common::IsolationLevel;
use crate::common::internals::PartitionStates;
use crate::common::{Error, TopicPartition, Uuid};
use crate::consumer::ConsumerNoOffsetForPartitionError;
use crate::consumer::{AutoOffsetResetStrategy, ConsumerRebalanceListener, OffsetAndMetadata, SubscriptionPattern};
use crate::metadata::LeaderAndEpoch;

const SUBSCRIPTION_ERROR_MESSAGE: &str = "Subscription to topics, partitions and pattern are mutually exclusive";

/// Java's `Pattern.matcher(s).matches()` requires the regex to match the
/// *whole* string. Rust's `regex::Regex::is_match` only requires a partial
/// match (equivalent to Java's `find()`). To preserve Java semantics for
/// the consumer's client-side regex subscription, we check that:
/// 1. The regex matches the input at all (`find`), AND
/// 2. The match spans the entire string (start == 0 && end == len).
///
/// We do this without modifying the user-provided regex string — anchoring
/// with `^...$` would silently alter behavior for patterns that already
/// contain alternation or anchors.
pub(crate) fn regex_full_match(re: &Regex, s: &str) -> bool {
    re.find(s).is_some_and(|m| m.start() == 0 && m.end() == s.len())
}

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
/// `maybe_complete_validation` method itself is translated below; the
/// struct lives here alongside the rest of `SubscriptionState`'s public
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

    /// Java: `TopicPartitionState.clearEndOffset()`.
    pub(crate) fn clear_end_offset(&mut self) {
        self.end_offset_requested = false;
    }

    /// Attempt to transition to `new_state`; on success, invoke the
    /// closure to mutate `self` (replacing Java's `Runnable runIfTransitioned`
    /// parameter on `transitionState`). The closure runs only when the
    /// transition is valid.
    ///
    /// Per CLAUDE.md §10.1, Java's
    /// `IllegalStateException("...but position is null")` is translated to
    /// a `panic!` — this is a programmer-error path that cannot be reached
    /// on the happy path (the closure must leave `self.position` consistent
    /// with `new_state.requires_position()`), and the consumer cannot
    /// continue with an inconsistent fetch state.
    fn transition_state(&mut self, new_state: FetchStates, run_if_transitioned: impl FnOnce(&mut Self)) {
        let next_state = self.fetch_state.transition_to(new_state);
        if next_state == new_state {
            self.fetch_state = next_state;
            run_if_transitioned(self);
            if self.position.is_none() && next_state.requires_position() {
                panic!("Transitioned subscription state to {next_state:?}, but position is null");
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

    /// Re-enter position validation if the leader has changed.
    ///
    /// Translates Java's private
    /// `TopicPartitionState.maybeValidatePosition(LeaderAndEpoch)`.
    /// Returns `true` if the partition is now awaiting validation.
    fn maybe_validate_position(&mut self, current_leader_and_epoch: &LeaderAndEpoch) -> bool {
        if self.fetch_state == FetchStates::AwaitReset {
            return false;
        }
        if current_leader_and_epoch.leader.is_none() {
            return false;
        }
        if let Some(position) = &self.position
            && &position.current_leader != current_leader_and_epoch
        {
            let new_position =
                FetchPosition::with_leader(position.offset, position.offset_epoch, current_leader_and_epoch.clone());
            self.validate_position(new_position);
            self.preferred_read_replica = None;
        }
        self.fetch_state == FetchStates::AwaitValidation
    }

    /// For older versions of the API, we cannot perform offset validation
    /// so we simply transition directly to FETCHING.
    ///
    /// Translates Java's private
    /// `TopicPartitionState.updatePositionLeaderNoValidation(LeaderAndEpoch)`.
    fn update_position_leader_no_validation(&mut self, current_leader_and_epoch: &LeaderAndEpoch) {
        if let Some(position) = self.position.clone() {
            let new_position =
                FetchPosition::with_leader(position.offset, position.offset_epoch, current_leader_and_epoch.clone());
            self.transition_state(FetchStates::Fetching, |this| {
                this.position = Some(new_position);
                this.next_retry_time_ms = None;
            });
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
    /// `Err(Error::local_illegal_state(...))` if there's no valid current
    /// position (matches Java's `IllegalStateException`).
    pub(crate) fn set_position(&mut self, position: FetchPosition) -> Result<(), crate::common::Error> {
        if !self.has_valid_position() {
            return Err(crate::common::Error::local_illegal_state(
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

// ─── SubscriptionState ──────────────────────────────────────────────────────

/// Subscription, assignment, and per-partition fetch state held by the
/// consumer.
///
/// Translated from
/// `org.apache.kafka.clients.consumer.internals.SubscriptionState`. Holds
/// no internal `Mutex`: callers wrap the whole struct in
/// `Arc<Mutex<SubscriptionState>>` per `consumer-threading.md` §16. Each
/// Java `synchronized` method maps to a `&self` (read) or `&mut self`
/// (write) Rust method; the borrow checker enforces single-writer /
/// multi-reader statically.
pub(crate) struct SubscriptionState {
    subscription_type: SubscriptionType,
    subscribed_pattern: Option<Regex>,
    subscribed_re2j_pattern: Option<SubscriptionPattern>,
    /// Java uses `TreeSet` for stable logging — Rust uses `BTreeSet` for
    /// the same property (sorted, deterministic iteration).
    subscription: BTreeSet<String>,
    /// Topic IDs received in an assignment from the coordinator when using
    /// the KIP-848 consumer rebalance protocol.
    assigned_topic_ids: BTreeSet<Uuid>,
    /// Set of topics the *group* has subscribed to (leader's view).
    group_subscription: HashSet<String>,
    assignment: PartitionStates<TopicPartitionState>,
    default_reset_strategy: AutoOffsetResetStrategy,
    /// Listener invoked on rebalance events. `Arc<dyn>` (not `Box<dyn>`)
    /// per `consumer-threading.md` §31 — the listener must be cloneable out
    /// of the lock before the caller awaits it.
    rebalance_listener: Option<Arc<dyn ConsumerRebalanceListener>>,
    /// Monotonically-increasing id incremented after every assignment
    /// change. Wrapping addition matches Java's `int` overflow semantics.
    assignment_id: u32,
}

impl SubscriptionState {
    /// Construct an empty `SubscriptionState` with the given default
    /// reset strategy.
    pub(crate) fn new(default_reset_strategy: AutoOffsetResetStrategy) -> Self {
        Self {
            subscription_type: SubscriptionType::None,
            subscribed_pattern: None,
            subscribed_re2j_pattern: None,
            subscription: BTreeSet::new(),
            assigned_topic_ids: BTreeSet::new(),
            group_subscription: HashSet::new(),
            assignment: PartitionStates::new(),
            default_reset_strategy,
            rebalance_listener: None,
            assignment_id: 0,
        }
    }

    /// Monotonically-increasing id incremented after every assignment
    /// change. Used by callers to detect when an assignment has changed.
    pub(crate) fn assignment_id(&self) -> u32 {
        self.assignment_id
    }

    fn set_subscription_type(&mut self, subscription_type: SubscriptionType) -> Result<(), Error> {
        if self.subscription_type == SubscriptionType::None {
            self.subscription_type = subscription_type;
            Ok(())
        } else if self.subscription_type == subscription_type {
            Ok(())
        } else {
            Err(Error::local_illegal_state(SUBSCRIPTION_ERROR_MESSAGE))
        }
    }

    fn register_rebalance_listener(&mut self, listener: Option<Arc<dyn ConsumerRebalanceListener>>) {
        // Java uses `Objects.requireNonNull(listener)` to reject a null
        // `Optional<T>` (different from a present-but-null value). The Rust
        // equivalent is `Option<Arc<dyn>>` which cannot be `null`; treat
        // `None` as "no listener". Matches Java's `Optional.empty()`
        // behavior.
        self.rebalance_listener = listener;
    }

    fn change_subscription(&mut self, topics_to_subscribe: BTreeSet<String>) -> bool {
        if self.subscription == topics_to_subscribe {
            return false;
        }
        self.subscription = topics_to_subscribe;
        true
    }

    /// Translates Java's `subscribe(Set<String>, Optional<ConsumerRebalanceListener>)`.
    pub(crate) fn subscribe_topics(
        &mut self,
        topics: HashSet<String>,
        listener: Option<Arc<dyn ConsumerRebalanceListener>>,
    ) -> Result<bool, Error> {
        self.register_rebalance_listener(listener);
        self.set_subscription_type(SubscriptionType::AutoTopics)?;
        Ok(self.change_subscription(topics.into_iter().collect()))
    }

    /// Translates Java's `subscribe(Pattern, Optional<ConsumerRebalanceListener>)`.
    pub(crate) fn subscribe_pattern(
        &mut self,
        pattern: Regex,
        listener: Option<Arc<dyn ConsumerRebalanceListener>>,
    ) -> Result<(), Error> {
        self.register_rebalance_listener(listener);
        self.set_subscription_type(SubscriptionType::AutoPattern)?;
        self.subscribed_pattern = Some(pattern);
        Ok(())
    }

    /// Translates Java's `subscribe(SubscriptionPattern, Optional<ConsumerRebalanceListener>)`.
    pub(crate) fn subscribe_subscription_pattern(
        &mut self,
        pattern: SubscriptionPattern,
        listener: Option<Arc<dyn ConsumerRebalanceListener>>,
    ) -> Result<(), Error> {
        self.register_rebalance_listener(listener);
        self.set_subscription_type(SubscriptionType::AutoPatternRe2j)?;
        self.subscribed_re2j_pattern = Some(pattern);
        Ok(())
    }

    /// Translates Java's `subscribeFromPattern(Set<String>)`. Only valid
    /// when subscription type is `AutoPattern` — Java throws
    /// `IllegalArgumentException` otherwise.
    pub(crate) fn subscribe_from_pattern(&mut self, topics: HashSet<String>) -> Result<bool, Error> {
        if self.subscription_type != SubscriptionType::AutoPattern {
            return Err(Error::local_illegal_argument(format!(
                "Attempt to subscribe from pattern while subscription type set to {}",
                self.subscription_type
            )));
        }
        Ok(self.change_subscription(topics.into_iter().collect()))
    }

    /// Translates Java's `subscribeToShareGroup(Set<String>)` (KIP-932).
    pub(crate) fn subscribe_to_share_group(&mut self, topics: HashSet<String>) -> Result<bool, Error> {
        self.register_rebalance_listener(None);
        self.set_subscription_type(SubscriptionType::AutoTopicsShare)?;
        Ok(self.change_subscription(topics.into_iter().collect()))
    }

    /// Translates Java's `assignFromUser(Set<TopicPartition>)`.
    ///
    /// Sets subscription type to `UserAssigned` (errors if already set to
    /// any other type). Returns `true` iff the assignment changed.
    pub(crate) fn assign_from_user(&mut self, partitions: HashSet<TopicPartition>) -> Result<bool, Error> {
        self.set_subscription_type(SubscriptionType::UserAssigned)?;

        let current: HashSet<TopicPartition> = self.assignment.partition_set().cloned().collect();
        if current == partitions {
            return Ok(false);
        }

        self.assignment_id = self.assignment_id.wrapping_add(1);

        let mut manual_subscribed_topics: BTreeSet<String> = BTreeSet::new();
        let mut partition_to_state: HashMap<TopicPartition, TopicPartitionState> = HashMap::new();
        for partition in partitions {
            // Mirror Java: reuse the existing state for retained partitions,
            // construct fresh state for new ones. `remove_and_take` moves
            // the existing `TopicPartitionState` out — no clone required.
            let state = self
                .assignment
                .remove_and_take(&partition)
                .unwrap_or_else(TopicPartitionState::new);
            manual_subscribed_topics.insert(partition.topic().to_string());
            partition_to_state.insert(partition, state);
        }
        self.assignment.set(partition_to_state);
        Ok(self.change_subscription(manual_subscribed_topics))
    }

    /// Translates Java's `checkAssignmentMatchedSubscription(Collection<TopicPartition>)`.
    pub(crate) fn check_assignment_matched_subscription(&self, assignments: &[TopicPartition]) -> bool {
        for tp in assignments {
            if let Some(pat) = &self.subscribed_pattern {
                if !regex_full_match(pat, tp.topic()) {
                    log::info!(
                        "Assigned partition {tp} for non-subscribed topic regex pattern; subscription pattern is {pat}"
                    );
                    return false;
                }
            } else if !self.subscription.contains(tp.topic()) {
                log::info!(
                    "Assigned partition {tp} for non-subscribed topic; subscription is {:?}",
                    self.subscription
                );
                return false;
            }
        }
        true
    }

    /// Translates Java's `assignFromSubscribed(Collection<TopicPartition>)`.
    pub(crate) fn assign_from_subscribed(&mut self, assignments: &[TopicPartition]) -> Result<(), Error> {
        if !self.has_auto_assigned_partitions() {
            return Err(Error::local_illegal_argument(
                "Attempt to dynamically assign partitions while manual assignment in use",
            ));
        }

        let mut assigned_partition_states: HashMap<TopicPartition, TopicPartitionState> = HashMap::new();
        for tp in assignments {
            // Mirror Java: reuse existing state for retained partitions,
            // construct fresh for new. Critical for tests that re-assign a
            // previously-fetchable partition and expect its position to be
            // retained.
            let state = self.assignment.remove_and_take(tp).unwrap_or_else(TopicPartitionState::new);
            assigned_partition_states.insert(tp.clone(), state);
        }

        self.assignment_id = self.assignment_id.wrapping_add(1);
        self.assignment.set(assigned_partition_states);
        Ok(())
    }

    /// Translates Java's `assignFromSubscribedAwaitingCallback`.
    pub(crate) fn assign_from_subscribed_awaiting_callback(
        &mut self,
        full_assignment: &[TopicPartition],
        added_partitions: &[TopicPartition],
    ) -> Result<(), Error> {
        self.assign_from_subscribed(full_assignment)?;
        self.mark_pending_on_assigned_callback(added_partitions, true)
    }

    /// Translates Java's `hasPatternSubscription`.
    pub(crate) fn has_pattern_subscription(&self) -> bool {
        self.subscription_type == SubscriptionType::AutoPattern
    }

    /// Translates Java's `hasRe2JPatternSubscription`.
    pub(crate) fn has_re2j_pattern_subscription(&self) -> bool {
        self.subscription_type == SubscriptionType::AutoPatternRe2j
    }

    /// Translates Java's `hasNoSubscriptionOrUserAssignment`.
    pub(crate) fn has_no_subscription_or_user_assignment(&self) -> bool {
        self.subscription_type == SubscriptionType::None
    }

    /// Translates Java's `hasAutoAssignedPartitions`.
    pub(crate) fn has_auto_assigned_partitions(&self) -> bool {
        matches!(
            self.subscription_type,
            SubscriptionType::AutoTopics
                | SubscriptionType::AutoPattern
                | SubscriptionType::AutoTopicsShare
                | SubscriptionType::AutoPatternRe2j
        )
    }

    /// Translates Java's `matchesSubscribedPattern(String)`.
    ///
    /// Uses a *full-match* check (mirroring Java's
    /// `Matcher.matches()` rather than `Matcher.find()`) — see
    /// [`regex_full_match`].
    pub(crate) fn matches_subscribed_pattern(&self, topic: &str) -> bool {
        if self.has_pattern_subscription()
            && let Some(p) = &self.subscribed_pattern
        {
            return regex_full_match(p, topic);
        }
        false
    }

    /// Translates Java's `unsubscribe`.
    pub(crate) fn unsubscribe(&mut self) {
        self.subscription = BTreeSet::new();
        self.group_subscription = HashSet::new();
        self.assignment.clear();
        self.assigned_topic_ids = BTreeSet::new();
        self.subscribed_pattern = None;
        self.subscription_type = SubscriptionType::None;
        self.assignment_id = self.assignment_id.wrapping_add(1);
    }

    /// Translates Java's `subscription()`. Returns an owned (cloneable)
    /// snapshot of the subscription so the caller can drop the outer
    /// mutex.
    pub(crate) fn subscription(&self) -> HashSet<String> {
        if self.has_auto_assigned_partitions() {
            self.subscription.iter().cloned().collect()
        } else {
            HashSet::new()
        }
    }

    /// Test-only: directly set (or clear) the RE2J subscription pattern,
    /// keeping the subscription type at `AutoPatternRe2j` so
    /// [`Self::subscription_pattern`] reflects the value. Mirrors Java tests
    /// that stub `when(subscriptions.subscriptionPattern()).thenReturn(...)`
    /// across a regex lifecycle (set / change / clear) without driving the
    /// full subscribe pipeline.
    #[cfg(test)]
    pub(crate) fn set_subscription_pattern_for_test(&mut self, pattern: Option<SubscriptionPattern>) {
        self.subscription_type = SubscriptionType::AutoPatternRe2j;
        self.subscribed_re2j_pattern = pattern;
    }

    /// Translates Java's `subscriptionPattern()`.
    pub(crate) fn subscription_pattern(&self) -> Option<&SubscriptionPattern> {
        if self.has_re2j_pattern_subscription() {
            self.subscribed_re2j_pattern.as_ref()
        } else {
            None
        }
    }

    /// Translates Java's `assignedPartitions()`.
    pub(crate) fn assigned_partitions(&self) -> HashSet<TopicPartition> {
        self.assignment.partition_set().cloned().collect()
    }

    /// Translates Java's `assignedPartitionsList()`.
    pub(crate) fn assigned_partitions_list(&self) -> Vec<TopicPartition> {
        self.assignment.partition_set().cloned().collect()
    }

    /// Translates Java's `numAssignedPartitions()`.
    pub(crate) fn num_assigned_partitions(&self) -> usize {
        self.assignment.size()
    }

    /// Translates Java's `isAssigned(TopicPartition)`.
    pub(crate) fn is_assigned(&self, tp: &TopicPartition) -> bool {
        self.assignment.contains(tp)
    }

    /// Translates Java's `assignedTopicIds()`.
    pub(crate) fn assigned_topic_ids(&self) -> &BTreeSet<Uuid> {
        &self.assigned_topic_ids
    }

    /// Translates Java's `setAssignedTopicIds(Set<Uuid>)`.
    pub(crate) fn set_assigned_topic_ids(&mut self, ids: HashSet<Uuid>) {
        self.assigned_topic_ids = ids.into_iter().collect();
    }

    /// Translates Java's `isAssignedFromRe2j(Uuid)`.
    pub(crate) fn is_assigned_from_re2j(&self, topic_id: Uuid) -> bool {
        if !self.has_re2j_pattern_subscription() {
            return false;
        }
        self.assigned_topic_ids.contains(&topic_id)
    }

    /// Translates Java's `rebalanceListener()`. Clones the `Arc` so the
    /// caller can drop the lock before awaiting / invoking the listener.
    pub(crate) fn rebalance_listener(&self) -> Option<Arc<dyn ConsumerRebalanceListener>> {
        self.rebalance_listener.clone()
    }

    /// Translates Java's `metadataTopics()`.
    pub(crate) fn metadata_topics(&self) -> HashSet<String> {
        if self.group_subscription.is_empty() {
            self.subscription.iter().cloned().collect()
        } else if self.subscription.iter().all(|t| self.group_subscription.contains(t)) {
            self.group_subscription.iter().cloned().collect()
        } else {
            let mut topics: HashSet<String> = self.group_subscription.iter().cloned().collect();
            topics.extend(self.subscription.iter().cloned());
            topics
        }
    }

    /// Translates Java's `needsMetadata(String)`.
    pub(crate) fn needs_metadata(&self, topic: &str) -> bool {
        self.subscription.contains(topic) || self.group_subscription.contains(topic)
    }

    /// Translates Java's `groupSubscribe(Collection<String>)`.
    ///
    /// Returns `true` iff the group's subscription contains topics that are
    /// not part of the local subscription (i.e. the group leader needs
    /// metadata for topics the local member is not directly subscribed to).
    /// Java: `!subscription.containsAll(groupSubscription)`.
    pub(crate) fn group_subscribe(&mut self, topics: &[String]) -> Result<bool, Error> {
        if !self.has_auto_assigned_partitions() {
            return Err(Error::local_illegal_state(SUBSCRIPTION_ERROR_MESSAGE));
        }
        self.group_subscription = topics.iter().cloned().collect();
        Ok(!self.group_subscription.iter().all(|t| self.subscription.contains(t)))
    }

    /// Translates Java's `resetGroupSubscription`.
    pub(crate) fn reset_group_subscription(&mut self) {
        self.group_subscription = HashSet::new();
    }

    /// Java's private `assignedState(tp)` — `&TopicPartitionState` or an
    /// `IllegalStateException` when the partition isn't assigned. Per
    /// CLAUDE.md §10 we return `Err` instead of panicking.
    fn assigned_state(&self, tp: &TopicPartition) -> Result<&TopicPartitionState, Error> {
        self.assignment
            .state_value(tp)
            .ok_or_else(|| Error::local_illegal_state(format!("No current assignment for partition {tp}")))
    }

    fn assigned_state_mut(&mut self, tp: &TopicPartition) -> Result<&mut TopicPartitionState, Error> {
        self.assignment
            .state_value_mut(tp)
            .ok_or_else(|| Error::local_illegal_state(format!("No current assignment for partition {tp}")))
    }

    fn assigned_state_or_null(&self, tp: &TopicPartition) -> Option<&TopicPartitionState> {
        self.assignment.state_value(tp)
    }

    fn assigned_state_or_null_mut(&mut self, tp: &TopicPartition) -> Option<&mut TopicPartitionState> {
        self.assignment.state_value_mut(tp)
    }

    // ── seek / position ─────────────────────────────────────────────────

    /// Translates Java's `seekValidated(TopicPartition, FetchPosition)`.
    pub(crate) fn seek_validated(&mut self, tp: &TopicPartition, position: FetchPosition) -> Result<(), Error> {
        self.assigned_state_mut(tp)?.seek_validated(position);
        Ok(())
    }

    /// Convenience: `seek(tp, offset) == seekValidated(tp, FetchPosition::new(offset))`.
    pub(crate) fn seek(&mut self, tp: &TopicPartition, offset: i64) -> Result<(), Error> {
        self.seek_validated(tp, FetchPosition::new(offset))
    }

    /// Translates Java's `seekUnvalidated(TopicPartition, FetchPosition)`.
    pub(crate) fn seek_unvalidated(&mut self, tp: &TopicPartition, position: FetchPosition) -> Result<(), Error> {
        self.assigned_state_mut(tp)?.seek_unvalidated(position);
        Ok(())
    }

    /// Translates Java's `maybeSeekUnvalidated(TopicPartition, FetchPosition, AutoOffsetResetStrategy)`.
    pub(crate) fn maybe_seek_unvalidated(
        &mut self,
        tp: &TopicPartition,
        position: FetchPosition,
        requested_reset_strategy: Option<&AutoOffsetResetStrategy>,
    ) {
        let state = match self.assigned_state_or_null_mut(tp) {
            Some(s) => s,
            None => {
                debug!("Skipping reset of partition {tp} since it is no longer assigned");
                return;
            },
        };
        if !state.awaiting_reset() {
            debug!("Skipping reset of partition {tp} since reset is no longer needed");
            return;
        }
        // Java checks `requestedResetStrategy != null && !requestedResetStrategy.equals(state.resetStrategy)`.
        // We can match that with `requested_reset_strategy.is_some_and(|r| Some(r) != state.reset_strategy.as_ref())`.
        if let Some(req) = requested_reset_strategy
            && Some(req) != state.reset_strategy.as_ref()
        {
            debug!("Skipping reset of partition {tp} since an alternative reset has been requested");
            return;
        }
        info!("Resetting offset for partition {tp} to position {position}.");
        state.seek_unvalidated(position);
    }

    /// Translates Java's `position(TopicPartition)`. Returns `Err` when the
    /// partition is not assigned. The successful return value is borrowed
    /// from the internal state — callers needing to outlive the lock must
    /// clone.
    pub(crate) fn position(&self, tp: &TopicPartition) -> Result<Option<&FetchPosition>, Error> {
        Ok(self.assigned_state(tp)?.position.as_ref())
    }

    /// Translates Java's `positionOrNull(TopicPartition)`. Returns `None`
    /// when the partition is not assigned (matches Java's `null` return).
    pub(crate) fn position_or_null(&self, tp: &TopicPartition) -> Option<&FetchPosition> {
        self.assigned_state_or_null(tp).and_then(|s| s.position.as_ref())
    }

    /// Translates Java's `position(TopicPartition, FetchPosition)`.
    pub(crate) fn set_position(&mut self, tp: &TopicPartition, position: FetchPosition) -> Result<(), Error> {
        self.assigned_state_mut(tp)?.set_position(position)
    }

    /// Test-only: clears `tp`'s position to `None`, mirroring Java's
    /// `subscriptions.position(tp, null)` (which sets the assigned-partition
    /// state's `position` field to `null` without going through
    /// `set_position`'s valid-position precondition). Used by the
    /// missing-position fetch test to reproduce Java's
    /// `testFetchRequestWithBufferedPartitionMissingPosition` scenario
    /// (a genuinely-null position on a still-buffered, still-assigned
    /// partition). Returns `Err` when the partition is not assigned.
    #[cfg(test)]
    pub(crate) fn clear_position_for_test(&mut self, tp: &TopicPartition) -> Result<(), Error> {
        self.assigned_state_mut(tp)?.position = None;
        Ok(())
    }

    /// Translates Java's `validPosition(TopicPartition)`. The `Result`
    /// covers the not-assigned case (Java's `IllegalStateException`).
    pub(crate) fn valid_position(&self, tp: &TopicPartition) -> Result<Option<&FetchPosition>, Error> {
        Ok(self.assigned_state(tp)?.valid_position())
    }

    /// Translates Java's `awaitingValidation(TopicPartition)`.
    pub(crate) fn awaiting_validation(&self, tp: &TopicPartition) -> Result<bool, Error> {
        Ok(self.assigned_state(tp)?.awaiting_validation())
    }

    /// Translates Java's `completeValidation(TopicPartition)`.
    pub(crate) fn complete_validation(&mut self, tp: &TopicPartition) -> Result<(), Error> {
        self.assigned_state_mut(tp)?.complete_validation();
        Ok(())
    }

    /// Enter the offset validation state if the leader for this partition
    /// is known to support a usable version of the `OffsetsForLeaderEpoch`
    /// API. If the leader node does not support the API, simply complete
    /// the offset validation.
    ///
    /// Translates Java's
    /// `SubscriptionState.maybeValidatePositionForCurrentLeader(
    /// ApiVersions, TopicPartition, LeaderAndEpoch)`. Returns `true` if
    /// the partition is now awaiting validation.
    pub(crate) fn maybe_validate_position_for_current_leader(
        &mut self,
        api_versions: &crate::api_versions::ApiVersions,
        tp: &TopicPartition,
        leader_and_epoch: &LeaderAndEpoch,
    ) -> bool {
        let Some(state) = self.assigned_state_or_null_mut(tp) else {
            debug!("Skipping validating position for partition {tp} which is not currently assigned.");
            return false;
        };

        if let Some(leader) = leader_and_epoch.leader.as_ref() {
            let node_api_versions = api_versions.get(leader.id_string());
            match node_api_versions {
                None => state.maybe_validate_position(leader_and_epoch),
                Some(versions) => {
                    if crate::consumer::internals::offset_fetcher_utils::has_usable_offset_for_leader_epoch_version(
                        &versions,
                    ) {
                        state.maybe_validate_position(leader_and_epoch)
                    } else {
                        // If the broker does not support a newer version of
                        // OffsetsForLeaderEpoch, we skip validation.
                        state.update_position_leader_no_validation(leader_and_epoch);
                        false
                    }
                },
            }
        } else {
            state.maybe_validate_position(leader_and_epoch)
        }
    }

    /// Attempt to complete validation with the end offset returned from the
    /// `OffsetsForLeaderEpoch` request.
    ///
    /// Translates Java's
    /// `SubscriptionState.maybeCompleteValidation(TopicPartition,
    /// FetchPosition, EpochEndOffset)`.
    ///
    /// Returns `Some(LogTruncation)` when truncation is detected and no
    /// reset policy is defined; otherwise `None` (the side effect is
    /// either a `request_offset_reset` call or a `seek_validated` to the
    /// epoch's end offset).
    pub(crate) fn maybe_complete_validation(
        &mut self,
        tp: &TopicPartition,
        request_position: &FetchPosition,
        epoch_end_offset: &crate::offset_for_leader_epoch_response_data::EpochEndOffset,
    ) -> Option<LogTruncation> {
        let has_default_reset = self.has_default_offset_reset_policy();
        // Capture without holding any later mutable borrows.
        let Some(state) = self.assigned_state_or_null_mut(tp) else {
            debug!("Skipping completed validation for partition {tp} which is not currently assigned.");
            return None;
        };
        if !state.awaiting_validation() {
            debug!("Skipping completed validation for partition {tp} which is no longer expecting validation.");
            return None;
        }

        let current_position = state.position.clone()?;
        if &current_position != request_position {
            debug!(
                "Skipping completed validation for partition {tp} since the current position {current_position} \
                 no longer matches the position {request_position} when the request was sent"
            );
            return None;
        }

        let undefined_epoch_offset = crate::common::requests::offsets_for_leader_epoch_response::UNDEFINED_EPOCH_OFFSET;
        let undefined_epoch = crate::common::requests::offsets_for_leader_epoch_response::UNDEFINED_EPOCH;

        if epoch_end_offset.end_offset == undefined_epoch_offset || epoch_end_offset.leader_epoch == undefined_epoch {
            if has_default_reset {
                log::info!("Truncation detected for partition {tp} at offset {current_position}, resetting offset");
                // request_offset_reset borrows self mutably — drop state borrow first by re-acquiring.
                let _ = state; // explicit drop of the local borrow before next call
                self.request_offset_reset_default(tp).ok();
                return None;
            } else {
                log::warn!(
                    "Truncation detected for partition {tp} at offset {current_position}, but no reset policy is set"
                );
                return Some(LogTruncation {
                    topic_partition: tp.clone(),
                    fetch_position: request_position.clone(),
                    divergent_offset_opt: None,
                });
            }
        }

        if epoch_end_offset.end_offset < current_position.offset {
            if has_default_reset {
                let new_position = FetchPosition::with_leader(
                    epoch_end_offset.end_offset,
                    Some(epoch_end_offset.leader_epoch),
                    current_position.current_leader.clone(),
                );
                log::info!(
                    "Truncation detected for partition {tp} at offset {current_position}, resetting offset to \
                     the first offset known to diverge {new_position}"
                );
                state.seek_validated(new_position);
                return None;
            } else {
                // Java passes `null` for metadata; Rust represents that as
                // the empty string. The constructor only errors on negative
                // offsets, and we've already excluded UNDEFINED_EPOCH_OFFSET
                // above — so this `ok()` collapses an impossible Err to
                // `None`, which is treated identically to "no divergent
                // offset known".
                let divergent_offset = crate::consumer::OffsetAndMetadata::with_leader_epoch(
                    epoch_end_offset.end_offset,
                    Some(epoch_end_offset.leader_epoch),
                    "",
                )
                .ok();
                log::warn!(
                    "Truncation detected for partition {tp} at offset {current_position} (the end offset from the \
                     broker is {}), but no reset policy is set",
                    epoch_end_offset.end_offset,
                );
                return Some(LogTruncation {
                    topic_partition: tp.clone(),
                    fetch_position: request_position.clone(),
                    divergent_offset_opt: divergent_offset,
                });
            }
        }

        state.complete_validation();
        None
    }

    /// Translates Java's `hasValidPosition(TopicPartition)`.
    pub(crate) fn has_valid_position(&self, tp: &TopicPartition) -> bool {
        self.assigned_state_or_null(tp).is_some_and(|s| s.has_valid_position())
    }

    /// Translates Java's `hasAllFetchPositions()`.
    pub(crate) fn has_all_fetch_positions(&self) -> bool {
        self.assignment.state_iter().all(|s| s.has_valid_position())
    }

    // ── Offset reset ────────────────────────────────────────────────────

    /// Translates Java's `requestOffsetReset(TopicPartition, AutoOffsetResetStrategy)`.
    pub(crate) fn request_offset_reset(
        &mut self,
        partition: &TopicPartition,
        strategy: AutoOffsetResetStrategy,
    ) -> Result<(), Error> {
        self.assigned_state_mut(partition)?.reset(strategy);
        Ok(())
    }

    /// Translates Java's `requestOffsetReset(Collection<TopicPartition>, AutoOffsetResetStrategy)`.
    pub(crate) fn request_offset_reset_all(
        &mut self,
        partitions: &[TopicPartition],
        strategy: AutoOffsetResetStrategy,
    ) -> Result<(), Error> {
        for tp in partitions {
            info!("Seeking to {strategy} offset of partition {tp}");
            self.assigned_state_mut(tp)?.reset(strategy.clone());
        }
        Ok(())
    }

    /// Translates Java's `requestOffsetReset(TopicPartition)` (single-arg,
    /// uses default strategy).
    pub(crate) fn request_offset_reset_default(&mut self, partition: &TopicPartition) -> Result<(), Error> {
        let strategy = self.default_reset_strategy.clone();
        self.request_offset_reset(partition, strategy)
    }

    /// Translates Java's `requestOffsetResetIfPartitionAssigned`.
    pub(crate) fn request_offset_reset_if_assigned(&mut self, partition: &TopicPartition) {
        let default_strategy = self.default_reset_strategy.clone();
        if let Some(state) = self.assigned_state_or_null_mut(partition) {
            state.reset(default_strategy);
        }
    }

    /// Translates Java's `isOffsetResetNeeded(TopicPartition)`.
    pub(crate) fn is_offset_reset_needed(&self, partition: &TopicPartition) -> Result<bool, Error> {
        Ok(self.assigned_state(partition)?.awaiting_reset())
    }

    /// Translates Java's `resetStrategy(TopicPartition)`. Returns
    /// `Option<AutoOffsetResetStrategy>` since Java may return `null`; the
    /// outer `Result` wraps the not-assigned case.
    pub(crate) fn reset_strategy(&self, partition: &TopicPartition) -> Result<Option<AutoOffsetResetStrategy>, Error> {
        Ok(self.assigned_state(partition)?.reset_strategy())
    }

    /// Translates Java's `hasDefaultOffsetResetPolicy()` (package-private).
    pub(crate) fn has_default_offset_reset_policy(&self) -> bool {
        self.default_reset_strategy != AutoOffsetResetStrategy::NONE
    }

    /// Translates Java's `initializingPartitions`.
    pub(crate) fn initializing_partitions(&self) -> HashSet<TopicPartition> {
        self.assignment
            .iter()
            .filter_map(|(tp, s)| if s.should_initialize() { Some(tp.clone()) } else { None })
            .collect()
    }

    /// Translates Java's `resetInitializingPositions(Predicate<TopicPartition>)`.
    ///
    /// Returns `Err(Error::ConsumerNoOffsetForPartition(..))` (Java's
    /// `NoOffsetForPartitionException`) when the default reset strategy is
    /// `NONE` and any assigned partitions still require positions.
    pub(crate) fn reset_initializing_positions(
        &mut self,
        init_partitions_to_include: impl Fn(&TopicPartition) -> bool,
    ) -> Result<(), Error> {
        let mut partitions_with_no_offsets: HashSet<TopicPartition> = HashSet::new();
        // Collect partitions that need resetting first to avoid borrowing
        // self mutably twice.
        let mut to_reset: Vec<TopicPartition> = Vec::new();
        for (tp, state) in self.assignment.iter() {
            if state.should_initialize() && init_partitions_to_include(tp) {
                if self.default_reset_strategy == AutoOffsetResetStrategy::NONE {
                    partitions_with_no_offsets.insert(tp.clone());
                } else {
                    to_reset.push(tp.clone());
                }
            }
        }
        for tp in to_reset {
            self.request_offset_reset_default(&tp)?;
        }
        if !partitions_with_no_offsets.is_empty() {
            return Err(Error::ConsumerNoOffsetForPartition(
                ConsumerNoOffsetForPartitionError::for_partitions(partitions_with_no_offsets),
            ));
        }
        Ok(())
    }

    /// Translates Java's `resetInitializingPositions()` (no predicate).
    pub(crate) fn reset_initializing_positions_all(&mut self) -> Result<(), Error> {
        self.reset_initializing_positions(|_| true)
    }

    /// Translates Java's `partitionsNeedingReset(long)`.
    pub(crate) fn partitions_needing_reset(&self, now_ms: i64) -> HashSet<TopicPartition> {
        self.assignment
            .iter()
            .filter_map(|(tp, s)| {
                if s.awaiting_reset() && !s.awaiting_retry_backoff(now_ms) {
                    Some(tp.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    /// Translates Java's `partitionsNeedingValidation(long)`.
    pub(crate) fn partitions_needing_validation(&self, now_ms: i64) -> HashMap<TopicPartition, FetchPosition> {
        let mut result = HashMap::new();
        for (tp, s) in self.assignment.iter() {
            if s.awaiting_validation()
                && !s.awaiting_retry_backoff(now_ms)
                && let Some(p) = &s.position
            {
                result.insert(tp.clone(), p.clone());
            }
        }
        result
    }

    /// Translates Java's `hasPartitionsNeedingValidation(long)`.
    pub(crate) fn has_partitions_needing_validation(&self, now_ms: i64) -> bool {
        self.assignment
            .state_iter()
            .any(|s| s.awaiting_validation() && !s.awaiting_retry_backoff(now_ms) && s.position.is_some())
    }

    // ── Pause / resume / fetchable ──────────────────────────────────────

    /// Translates Java's `pausedPartitions()`.
    pub(crate) fn paused_partitions(&self) -> HashSet<TopicPartition> {
        self.assignment
            .iter()
            .filter_map(|(tp, s)| if s.is_paused() { Some(tp.clone()) } else { None })
            .collect()
    }

    /// Translates Java's `isPaused(TopicPartition)`.
    pub(crate) fn is_paused(&self, tp: &TopicPartition) -> bool {
        self.assigned_state_or_null(tp).is_some_and(|s| s.is_paused())
    }

    fn is_fetchable_and_subscribed(&self, tp: &TopicPartition, state: &TopicPartitionState) -> bool {
        if self.subscription_type == SubscriptionType::AutoTopics && !self.subscription.contains(tp.topic()) {
            log::trace!(
                "Assigned partition {tp} is not in the subscription {:?} so will be considered not fetchable.",
                self.subscription
            );
            return false;
        }
        state.is_fetchable()
    }

    /// Translates Java's `isFetchable(TopicPartition)`.
    pub(crate) fn is_fetchable(&self, tp: &TopicPartition) -> bool {
        match self.assigned_state_or_null(tp) {
            Some(s) => self.is_fetchable_and_subscribed(tp, s),
            None => false,
        }
    }

    /// Translates Java's `fetchablePartitions(Predicate<TopicPartition>)`.
    pub(crate) fn fetchable_partitions(&self, is_available: impl Fn(&TopicPartition) -> bool) -> Vec<TopicPartition> {
        let mut result: Vec<TopicPartition> = Vec::new();
        for (tp, state) in self.assignment.iter() {
            let cheap_ok = self.subscription_type == SubscriptionType::AutoTopicsShare
                || self.is_fetchable_and_subscribed(tp, state);
            if cheap_ok && is_available(tp) {
                result.push(tp.clone());
            }
        }
        result
    }

    /// Translates Java's `pause(TopicPartition)`.
    pub(crate) fn pause(&mut self, tp: &TopicPartition) -> Result<(), Error> {
        self.assigned_state_mut(tp)?.pause();
        Ok(())
    }

    /// Translates Java's `resume(TopicPartition)`.
    pub(crate) fn resume(&mut self, tp: &TopicPartition) -> Result<(), Error> {
        self.assigned_state_mut(tp)?.resume();
        Ok(())
    }

    /// Translates Java's `markPendingRevocation(Set<TopicPartition>)`.
    pub(crate) fn mark_pending_revocation(&mut self, tps: &[TopicPartition]) -> Result<(), Error> {
        for tp in tps {
            self.assigned_state_mut(tp)?.mark_pending_revocation();
        }
        Ok(())
    }

    /// Translates Java's `markPendingOnAssignedCallback` (package-private).
    pub(crate) fn mark_pending_on_assigned_callback(
        &mut self,
        tps: &[TopicPartition],
        pending: bool,
    ) -> Result<(), Error> {
        for tp in tps {
            self.assigned_state_mut(tp)?.mark_pending_on_assigned_callback(pending);
        }
        Ok(())
    }

    /// Translates Java's `enablePartitionsAwaitingCallback`.
    pub(crate) fn enable_partitions_awaiting_callback(&mut self, partitions: &[TopicPartition]) -> Result<(), Error> {
        self.mark_pending_on_assigned_callback(partitions, false)
    }

    /// Translates Java's `movePartitionToEnd(TopicPartition)`.
    pub(crate) fn move_partition_to_end(&mut self, tp: &TopicPartition) {
        self.assignment.move_to_end(tp);
    }

    // ── Lag / end offset / high watermark ───────────────────────────────

    /// Translates Java's `partitionLag(TopicPartition, IsolationLevel)`.
    pub(crate) fn partition_lag(
        &self,
        tp: &TopicPartition,
        isolation_level: IsolationLevel,
    ) -> Result<Option<i64>, Error> {
        let state = self.assigned_state(tp)?;
        let Some(position) = state.position.as_ref() else {
            return Ok(None);
        };
        match isolation_level {
            IsolationLevel::ReadCommitted => Ok(state.last_stable_offset.map(|lso| lso - position.offset)),
            IsolationLevel::ReadUncommitted => Ok(state.high_watermark.map(|hw| hw - position.offset)),
        }
    }

    /// Translates Java's `partitionEndOffset(TopicPartition, IsolationLevel)`.
    pub(crate) fn partition_end_offset(
        &self,
        tp: &TopicPartition,
        isolation_level: IsolationLevel,
    ) -> Result<Option<i64>, Error> {
        let state = self.assigned_state(tp)?;
        match isolation_level {
            IsolationLevel::ReadCommitted => Ok(state.last_stable_offset),
            IsolationLevel::ReadUncommitted => Ok(state.high_watermark),
        }
    }

    /// Translates Java's `requestPartitionEndOffset(TopicPartition)`.
    pub(crate) fn request_partition_end_offset(&mut self, tp: &TopicPartition) -> Result<(), Error> {
        self.assigned_state_mut(tp)?.request_end_offset();
        Ok(())
    }

    /// Translates Java's `partitionEndOffsetRequested(TopicPartition)`.
    pub(crate) fn partition_end_offset_requested(&self, tp: &TopicPartition) -> Result<bool, Error> {
        Ok(self.assigned_state(tp)?.end_offset_requested())
    }

    /// Clears the partition's 'end offset requested' flag if the partition is
    /// still assigned and the flag is set. Returns `true` if it was cleared.
    ///
    /// Translates Java's `maybeClearPartitionEndOffsetRequested(TopicPartition)`
    /// (AK 4.3.1).
    pub(crate) fn maybe_clear_partition_end_offset_requested(&mut self, tp: &TopicPartition) -> bool {
        match self.assigned_state_or_null_mut(tp) {
            Some(state) if state.end_offset_requested() => {
                state.clear_end_offset();
                true
            },
            _ => false,
        }
    }

    /// Translates Java's package-private `partitionLead(TopicPartition)`.
    /// Visible only for tests that read it via lag computations.
    ///
    /// Matches Java's
    /// `logStartOffset == null ? null : position.offset - logStartOffset`.
    /// If `log_start_offset` is set but `position` is `None`, Java NPEs;
    /// Rust panics. This combination is unreachable on the happy path (the
    /// state machine guarantees `position.is_some()` whenever
    /// `log_start_offset` is updated via a fetch response).
    pub(crate) fn partition_lead(&self, tp: &TopicPartition) -> Result<Option<i64>, Error> {
        let state = self.assigned_state(tp)?;
        Ok(state.log_start_offset.map(|lso| {
            state
                .position
                .as_ref()
                .expect("position is null but logStartOffset is set")
                .offset
                - lso
        }))
    }

    /// Translates Java's `updateHighWatermark(TopicPartition, long)`.
    pub(crate) fn update_high_watermark(&mut self, tp: &TopicPartition, hw: i64) -> Result<(), Error> {
        self.assigned_state_mut(tp)?.high_watermark(hw);
        Ok(())
    }

    /// Translates Java's `tryUpdatingHighWatermark`.
    pub(crate) fn try_updating_high_watermark(&mut self, tp: &TopicPartition, hw: i64) -> bool {
        match self.assigned_state_or_null_mut(tp) {
            Some(s) => {
                s.high_watermark(hw);
                true
            },
            None => false,
        }
    }

    /// Translates Java's `tryUpdatingLogStartOffset`.
    pub(crate) fn try_updating_log_start_offset(&mut self, tp: &TopicPartition, lso: i64) -> bool {
        match self.assigned_state_or_null_mut(tp) {
            Some(s) => {
                s.log_start_offset(lso);
                true
            },
            None => false,
        }
    }

    /// Translates Java's `updateLastStableOffset(TopicPartition, long)`.
    pub(crate) fn update_last_stable_offset(&mut self, tp: &TopicPartition, lso: i64) -> Result<(), Error> {
        self.assigned_state_mut(tp)?.last_stable_offset(lso);
        Ok(())
    }

    /// Translates Java's `tryUpdatingLastStableOffset`.
    pub(crate) fn try_updating_last_stable_offset(&mut self, tp: &TopicPartition, lso: i64) -> bool {
        match self.assigned_state_or_null_mut(tp) {
            Some(s) => {
                s.last_stable_offset(lso);
                true
            },
            None => false,
        }
    }

    // ── Preferred read replica ──────────────────────────────────────────

    /// Translates Java's `updatePreferredReadReplica(TopicPartition, int, LongSupplier)`.
    /// The Java `LongSupplier` collapses to an eager `i64` (called exactly
    /// once at the same point in Java; see the plan §LongSupplier).
    pub(crate) fn update_preferred_read_replica(
        &mut self,
        tp: &TopicPartition,
        replica_id: i32,
        time_ms: i64,
    ) -> Result<(), Error> {
        self.assigned_state_mut(tp)?.update_preferred_read_replica(replica_id, time_ms);
        Ok(())
    }

    /// Translates Java's `tryUpdatingPreferredReadReplica`.
    pub(crate) fn try_updating_preferred_read_replica(
        &mut self,
        tp: &TopicPartition,
        replica_id: i32,
        time_ms: i64,
    ) -> bool {
        match self.assigned_state_or_null_mut(tp) {
            Some(s) => {
                s.update_preferred_read_replica(replica_id, time_ms);
                true
            },
            None => false,
        }
    }

    /// Translates Java's `preferredReadReplica(TopicPartition, long)`.
    pub(crate) fn preferred_read_replica(&mut self, tp: &TopicPartition, time_ms: i64) -> Option<i32> {
        self.assigned_state_or_null_mut(tp)
            .and_then(|s| s.preferred_read_replica(time_ms))
    }

    /// Translates Java's `clearPreferredReadReplica(TopicPartition)`.
    pub(crate) fn clear_preferred_read_replica(&mut self, tp: &TopicPartition) -> Option<i32> {
        self.assigned_state_or_null_mut(tp)
            .and_then(|s| s.clear_preferred_read_replica())
    }

    // ── Retry tracking ──────────────────────────────────────────────────

    /// Translates Java's `setNextAllowedRetry(Set<TopicPartition>, long)`.
    pub(crate) fn set_next_allowed_retry(&mut self, partitions: &HashSet<TopicPartition>, next_ms: i64) {
        for tp in partitions {
            if let Some(s) = self.assigned_state_or_null_mut(tp) {
                s.set_next_allowed_retry(next_ms);
            }
        }
    }

    /// Translates Java's `requestFailed(Set<TopicPartition>, long)`.
    pub(crate) fn request_failed(&mut self, partitions: &HashSet<TopicPartition>, next_retry_ms: i64) {
        for tp in partitions {
            if let Some(s) = self.assigned_state_or_null_mut(tp) {
                s.request_failed(next_retry_ms);
            }
        }
    }

    /// Translates Java's `allConsumed()`.
    pub(crate) fn all_consumed(&self) -> HashMap<TopicPartition, OffsetAndMetadata> {
        let mut result = HashMap::new();
        for (tp, state) in self.assignment.iter() {
            if state.has_valid_position()
                && let Some(pos) = &state.position
            {
                // `OffsetAndMetadata::with_leader_epoch(offset, epoch, "")` —
                // empty metadata mirrors Java's `new OffsetAndMetadata(offset, epoch, "")`.
                match OffsetAndMetadata::with_leader_epoch(pos.offset, pos.offset_epoch, String::new()) {
                    Ok(om) => {
                        result.insert(tp.clone(), om);
                    },
                    Err(_e) => {
                        // OffsetAndMetadata::with_leader_epoch only fails on
                        // negative offsets — `position.offset` is always
                        // non-negative on the happy path. Skip the entry on
                        // the unexpected case rather than propagating: the
                        // Java contract doesn't surface this error either.
                    },
                }
            }
        }
        result
    }

    // ── Display ─────────────────────────────────────────────────────────

    /// Translates Java's `prettyString()`.
    pub(crate) fn pretty_string(&self) -> String {
        match self.subscription_type {
            SubscriptionType::None => "None".to_string(),
            SubscriptionType::AutoTopics => {
                format!("Subscribe({})", self.subscription.iter().cloned().collect::<Vec<_>>().join(","))
            },
            SubscriptionType::AutoPattern => match &self.subscribed_pattern {
                Some(p) => format!("Subscribe({})", p.as_str()),
                None => "Subscribe(<none>)".to_string(),
            },
            SubscriptionType::AutoPatternRe2j => match &self.subscribed_re2j_pattern {
                Some(p) => format!("Subscribe({p})"),
                None => "Subscribe(<none>)".to_string(),
            },
            SubscriptionType::UserAssigned => {
                // Matches Java: `"Assign(" + assignedPartitions() + " , id=" + ... + ")"`.
                // Java's `HashSet<TopicPartition>.toString()` produces `[topic-0, topic-1]`
                // (no quotes, comma + space).
                let parts: Vec<String> = self.assignment.partition_set().map(|tp| tp.to_string()).collect();
                format!("Assign([{}] , id={})", parts.join(", "), self.assignment_id)
            },
            SubscriptionType::AutoTopicsShare => {
                format!(
                    "Subscribe to Share Group({})",
                    self.subscription.iter().cloned().collect::<Vec<_>>().join(",")
                )
            },
        }
    }

    /// Translates Java's `toString()`. Used by tests that match on
    /// `state.toString().contains(...)`.
    fn to_string_impl(&self) -> String {
        let pattern_in_use = match self.subscription_type {
            SubscriptionType::AutoPatternRe2j => self
                .subscribed_re2j_pattern
                .as_ref()
                .map(|p| p.to_string())
                .unwrap_or_else(|| "null".to_string()),
            SubscriptionType::AutoPattern => self
                .subscribed_pattern
                .as_ref()
                .map(|p| p.as_str().to_string())
                .unwrap_or_else(|| "null".to_string()),
            _ => "null".to_string(),
        };
        // Matches Java: `"assignment=" + assignment.partitionStateValues() + " (id=" + ... + ")"`.
        // Java's `Collection.toString()` produces `[elem0, elem1]` (no quotes, comma + space).
        // We print partition names instead of `TopicPartitionState`'s default `Class@hash`
        // toString — more useful for logs and still surrounded by `[...]` like Java.
        let assignment_str: Vec<String> = self.assignment.partition_set().map(|tp| tp.to_string()).collect();
        format!(
            "SubscriptionState{{type={}, subscribedPattern={pattern_in_use}, subscription={}, groupSubscription={}, defaultResetStrategy={}, assignment=[{}] (id={})}}",
            self.subscription_type,
            self.subscription.iter().cloned().collect::<Vec<_>>().join(","),
            self.group_subscription.iter().cloned().collect::<Vec<_>>().join(","),
            self.default_reset_strategy,
            assignment_str.join(", "),
            self.assignment_id,
        )
    }
}

impl std::fmt::Display for SubscriptionState {
    /// Matches Java's `toString()`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_string_impl())
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

    // ─── SubscriptionStateTest translation ──────────────────────────────
    //
    // Translated from `org.apache.kafka.clients.consumer.internals.SubscriptionStateTest`.
    //
    // All Phase-7d-deferred tests are now translated (see the
    // `test_maybe_*_validation`, `test_truncation_detection_*`, and
    // `reset_offset_no_validation` tests at the end of this module).

    use regex::Regex;

    use crate::common::IsolationLevel;
    use crate::consumer::{ConsumerRebalanceListener, SubscriptionPattern};

    const TOPIC: &str = "test";
    const TOPIC1: &str = "test1";

    fn tp_test_0() -> crate::common::TopicPartition {
        crate::common::TopicPartition::new(TOPIC.to_string(), 0)
    }
    fn tp_test_1() -> crate::common::TopicPartition {
        crate::common::TopicPartition::new(TOPIC.to_string(), 1)
    }
    fn tp_test1_0() -> crate::common::TopicPartition {
        crate::common::TopicPartition::new(TOPIC1.to_string(), 0)
    }

    fn no_leader_no_epoch() -> LeaderAndEpoch {
        LeaderAndEpoch::no_leader_or_epoch()
    }

    /// Translation of Java's `MockRebalanceListener`
    /// (`SubscriptionStateTest.java:980-996`): counts callback invocations.
    /// Currently no Phase-4 test asserts on the counters, but Phase-11
    /// rebalance-listener regression tests will inspect them.
    struct MockListener {
        revoked_count: std::sync::atomic::AtomicI32,
        assigned_count: std::sync::atomic::AtomicI32,
    }

    impl MockListener {
        fn new() -> Self {
            Self {
                revoked_count: std::sync::atomic::AtomicI32::new(0),
                assigned_count: std::sync::atomic::AtomicI32::new(0),
            }
        }
    }

    #[async_trait::async_trait]
    impl ConsumerRebalanceListener for MockListener {
        async fn on_partitions_revoked(
            &self,
            _partitions: &[crate::common::TopicPartition],
        ) -> Result<(), crate::common::Error> {
            self.revoked_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(())
        }
        async fn on_partitions_assigned(
            &self,
            _partitions: &[crate::common::TopicPartition],
        ) -> Result<(), crate::common::Error> {
            self.assigned_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(())
        }
    }

    fn listener() -> Option<Arc<dyn ConsumerRebalanceListener>> {
        Some(Arc::new(MockListener::new()))
    }

    fn new_state() -> SubscriptionState {
        SubscriptionState::new(AutoOffsetResetStrategy::EARLIEST)
    }

    /// `maybeClearPartitionEndOffsetRequested`: clears the flag only when the
    /// partition is assigned AND the flag is set; returns whether it cleared.
    #[test]
    fn test_maybe_clear_partition_end_offset_requested() {
        let mut state = new_state();
        state.assign_from_user(HashSet::from([tp_test_0()])).unwrap();

        // Flag not set yet → no clear.
        assert!(!state.maybe_clear_partition_end_offset_requested(&tp_test_0()));

        // Set the flag, then clearing succeeds and resets it.
        state.request_partition_end_offset(&tp_test_0()).unwrap();
        assert!(state.partition_end_offset_requested(&tp_test_0()).unwrap());
        assert!(state.maybe_clear_partition_end_offset_requested(&tp_test_0()));
        assert!(!state.partition_end_offset_requested(&tp_test_0()).unwrap());

        // A second clear is a no-op (flag already cleared).
        assert!(!state.maybe_clear_partition_end_offset_requested(&tp_test_0()));

        // Unassigned partition → false, never panics.
        let unassigned = crate::common::TopicPartition::new("unassigned".to_string(), 0);
        assert!(!state.maybe_clear_partition_end_offset_requested(&unassigned));
    }

    /// Translated from `partitionAssignment`.
    #[test]
    fn test_partition_assignment() {
        let mut state = new_state();
        state.assign_from_user(HashSet::from([tp_test_0()])).unwrap();
        assert_eq!(state.assigned_partitions(), HashSet::from([tp_test_0()]));
        assert_eq!(state.num_assigned_partitions(), 1);
        assert!(!state.has_all_fetch_positions());
        state.seek(&tp_test_0(), 1).unwrap();
        assert!(state.is_fetchable(&tp_test_0()));
        assert_eq!(state.position(&tp_test_0()).unwrap().unwrap().offset, 1);

        state.assign_from_user(HashSet::new()).unwrap();
        assert!(state.assigned_partitions().is_empty());
        assert_eq!(state.num_assigned_partitions(), 0);
        assert!(!state.is_assigned(&tp_test_0()));
        assert!(!state.is_fetchable(&tp_test_0()));
    }

    /// Translated from `partitionAssignmentChangeOnTopicSubscription`.
    #[test]
    fn test_partition_assignment_change_on_topic_subscription() {
        let mut state = new_state();
        state.assign_from_user(HashSet::from([tp_test_0(), tp_test_1()])).unwrap();
        assert_eq!(state.assigned_partitions().len(), 2);
        assert!(state.assigned_partitions().contains(&tp_test_0()));
        assert!(state.assigned_partitions().contains(&tp_test_1()));

        state.unsubscribe();
        assert!(state.assigned_partitions().is_empty());
        assert_eq!(state.num_assigned_partitions(), 0);

        state.subscribe_topics(HashSet::from([TOPIC1.to_string()]), listener()).unwrap();
        assert!(state.assigned_partitions().is_empty());

        assert!(state.check_assignment_matched_subscription(&[tp_test1_0()]));
        state.assign_from_subscribed(&[tp_test1_0()]).unwrap();
        assert_eq!(state.assigned_partitions(), HashSet::from([tp_test1_0()]));
        assert_eq!(state.num_assigned_partitions(), 1);

        state.subscribe_topics(HashSet::from([TOPIC.to_string()]), listener()).unwrap();
        // Subscription changes don't immediately clear the assignment.
        assert_eq!(state.assigned_partitions(), HashSet::from([tp_test1_0()]));

        state.unsubscribe();
        assert!(state.assigned_partitions().is_empty());
    }

    /// Translated from `testIsFetchableOnManualAssignment`.
    #[test]
    fn test_is_fetchable_on_manual_assignment() {
        let mut state = new_state();
        state.assign_from_user(HashSet::from([tp_test_0(), tp_test_1()])).unwrap();
        assert_assigned_partition_is_fetchable(&mut state);
    }

    /// Translated from `testIsFetchableOnAutoAssignment`.
    #[test]
    fn test_is_fetchable_on_auto_assignment() {
        let mut state = new_state();
        state.subscribe_topics(HashSet::from([TOPIC.to_string()]), listener()).unwrap();
        state.assign_from_subscribed(&[tp_test_0(), tp_test_1()]).unwrap();
        assert_assigned_partition_is_fetchable(&mut state);
    }

    fn assert_assigned_partition_is_fetchable(state: &mut SubscriptionState) {
        assert_eq!(state.assigned_partitions().len(), 2);
        assert!(state.assigned_partitions().contains(&tp_test_0()));
        assert!(state.assigned_partitions().contains(&tp_test_1()));
        assert!(!state.is_fetchable(&tp_test_0()));
        assert!(!state.is_fetchable(&tp_test_1()));
        state.seek(&tp_test_0(), 1).unwrap();
        state.seek(&tp_test_1(), 1).unwrap();
        assert!(state.is_fetchable(&tp_test_0()));
        assert!(state.is_fetchable(&tp_test_1()));
    }

    /// Translated from `testIsFetchableConsidersExplicitTopicSubscription`.
    #[test]
    fn test_is_fetchable_considers_explicit_topic_subscription() {
        let mut state = new_state();
        state.subscribe_topics(HashSet::from([TOPIC1.to_string()]), listener()).unwrap();
        state.assign_from_subscribed(&[tp_test1_0()]).unwrap();
        state.seek(&tp_test1_0(), 1).unwrap();

        assert_eq!(state.assigned_partitions(), HashSet::from([tp_test1_0()]));
        assert!(state.is_fetchable(&tp_test1_0()));

        // Change subscription. Assigned partition remains, no longer fetchable.
        state.subscribe_topics(HashSet::from([TOPIC.to_string()]), listener()).unwrap();
        assert_eq!(state.assigned_partitions(), HashSet::from([tp_test1_0()]));
        assert!(!state.is_fetchable(&tp_test1_0()));

        state.unsubscribe();
        assert!(state.assigned_partitions().is_empty());
        assert!(!state.is_fetchable(&tp_test1_0()));
    }

    /// Translated from `testGroupSubscribe`.
    #[test]
    fn test_group_subscribe() {
        let mut state = new_state();
        state.subscribe_topics(HashSet::from([TOPIC1.to_string()]), listener()).unwrap();
        assert_eq!(state.metadata_topics(), HashSet::from([TOPIC1.to_string()]));

        assert!(!state.group_subscribe(&[TOPIC1.to_string()]).unwrap());
        assert_eq!(state.metadata_topics(), HashSet::from([TOPIC1.to_string()]));

        assert!(state.group_subscribe(&[TOPIC.to_string(), TOPIC1.to_string()]).unwrap());
        assert_eq!(state.metadata_topics(), HashSet::from([TOPIC.to_string(), TOPIC1.to_string()]));

        // `group_subscribe` does not accumulate.
        assert!(!state.group_subscribe(&[TOPIC1.to_string()]).unwrap());
        assert_eq!(state.metadata_topics(), HashSet::from([TOPIC1.to_string()]));

        state
            .subscribe_topics(HashSet::from(["anotherTopic".to_string()]), listener())
            .unwrap();
        assert_eq!(
            state.metadata_topics(),
            HashSet::from([TOPIC1.to_string(), "anotherTopic".to_string()])
        );

        assert!(!state.group_subscribe(&["anotherTopic".to_string()]).unwrap());
        assert_eq!(state.metadata_topics(), HashSet::from(["anotherTopic".to_string()]));
    }

    /// Translated from `partitionAssignmentChangeOnPatternSubscription`.
    #[test]
    fn test_partition_assignment_change_on_pattern_subscription() {
        let mut state = new_state();
        state.subscribe_pattern(Regex::new(".*").unwrap(), listener()).unwrap();
        assert!(state.assigned_partitions().is_empty());

        state.subscribe_from_pattern(HashSet::from([TOPIC.to_string()])).unwrap();
        assert!(state.assigned_partitions().is_empty());

        assert!(state.check_assignment_matched_subscription(&[tp_test_1()]));
        state.assign_from_subscribed(&[tp_test_1()]).unwrap();
        assert_eq!(state.assigned_partitions(), HashSet::from([tp_test_1()]));
        assert_eq!(state.subscription(), HashSet::from([TOPIC.to_string()]));

        // checkAssignmentMatchedSubscription against the *pattern* (not the
        // current subscribeFromPattern set): the pattern is ".*" so any
        // topic matches.
        assert!(state.check_assignment_matched_subscription(&[tp_test1_0()]));
        state.assign_from_subscribed(&[tp_test1_0()]).unwrap();
        assert_eq!(state.assigned_partitions(), HashSet::from([tp_test1_0()]));
        assert_eq!(state.subscription(), HashSet::from([TOPIC.to_string()]));

        state.subscribe_pattern(Regex::new(".*t").unwrap(), listener()).unwrap();
        assert_eq!(state.assigned_partitions(), HashSet::from([tp_test1_0()]));

        state.subscribe_from_pattern(HashSet::from([TOPIC.to_string()])).unwrap();
        assert_eq!(state.assigned_partitions(), HashSet::from([tp_test1_0()]));

        assert!(state.check_assignment_matched_subscription(&[tp_test_0()]));
        state.assign_from_subscribed(&[tp_test_0()]).unwrap();
        assert_eq!(state.assigned_partitions(), HashSet::from([tp_test_0()]));
        assert_eq!(state.subscription(), HashSet::from([TOPIC.to_string()]));

        state.unsubscribe();
        assert!(state.assigned_partitions().is_empty());
    }

    /// Translated from `verifyAssignmentId`.
    #[test]
    fn test_verify_assignment_id() {
        let mut state = new_state();
        assert_eq!(state.assignment_id(), 0);

        let user_assignment: HashSet<crate::common::TopicPartition> = HashSet::from([tp_test_0(), tp_test_1()]);
        state.assign_from_user(user_assignment.clone()).unwrap();
        assert_eq!(state.assignment_id(), 1);
        assert_eq!(state.assigned_partitions(), user_assignment);

        state.unsubscribe();
        assert_eq!(state.assignment_id(), 2);
        assert!(state.assigned_partitions().is_empty());

        let auto_assignment: HashSet<crate::common::TopicPartition> = HashSet::from([tp_test1_0()]);
        state.subscribe_topics(HashSet::from([TOPIC1.to_string()]), listener()).unwrap();
        assert!(state.check_assignment_matched_subscription(&[tp_test1_0()]));
        state.assign_from_subscribed(&[tp_test1_0()]).unwrap();
        assert_eq!(state.assignment_id(), 3);
        assert_eq!(state.assigned_partitions(), auto_assignment);
    }

    /// Translated from `partitionReset`.
    #[test]
    fn test_partition_reset() {
        let mut state = new_state();
        state.assign_from_user(HashSet::from([tp_test_0()])).unwrap();
        state.seek(&tp_test_0(), 5).unwrap();
        assert_eq!(state.position(&tp_test_0()).unwrap().unwrap().offset, 5);
        state.request_offset_reset_default(&tp_test_0()).unwrap();
        assert!(!state.is_fetchable(&tp_test_0()));
        assert!(state.is_offset_reset_needed(&tp_test_0()).unwrap());
        // Java returns `null` (Rust: `None`). Position was cleared by the
        // transition to AWAIT_RESET.
        assert!(state.position(&tp_test_0()).unwrap().is_none());

        // Seek should clear the reset and make the partition fetchable.
        state.seek(&tp_test_0(), 0).unwrap();
        assert!(state.is_fetchable(&tp_test_0()));
        assert!(!state.is_offset_reset_needed(&tp_test_0()).unwrap());
    }

    /// Translated from `topicSubscription`.
    #[test]
    fn test_topic_subscription() {
        let mut state = new_state();
        state.subscribe_topics(HashSet::from([TOPIC.to_string()]), listener()).unwrap();
        assert_eq!(state.subscription().len(), 1);
        assert!(state.assigned_partitions().is_empty());
        assert!(state.has_auto_assigned_partitions());

        assert!(state.check_assignment_matched_subscription(&[tp_test_0()]));
        state.assign_from_subscribed(&[tp_test_0()]).unwrap();
        state.seek(&tp_test_0(), 1).unwrap();
        assert_eq!(state.position(&tp_test_0()).unwrap().unwrap().offset, 1);

        assert!(state.check_assignment_matched_subscription(&[tp_test_1()]));
        state.assign_from_subscribed(&[tp_test_1()]).unwrap();
        assert!(state.is_assigned(&tp_test_1()));
        assert!(!state.is_assigned(&tp_test_0()));
        assert!(!state.is_fetchable(&tp_test_1()));
        assert_eq!(state.assigned_partitions(), HashSet::from([tp_test_1()]));
    }

    /// Translated from `partitionPause`.
    #[test]
    fn test_partition_pause() {
        let mut state = new_state();
        state.assign_from_user(HashSet::from([tp_test_0()])).unwrap();
        state.seek(&tp_test_0(), 100).unwrap();
        assert!(state.is_fetchable(&tp_test_0()));
        state.pause(&tp_test_0()).unwrap();
        assert!(!state.is_fetchable(&tp_test_0()));
        state.resume(&tp_test_0()).unwrap();
        assert!(state.is_fetchable(&tp_test_0()));
    }

    /// Translated from `testMarkingPendingRevocation`.
    #[test]
    fn test_marking_pending_revocation() {
        let mut state = new_state();
        state.assign_from_user(HashSet::from([tp_test_0()])).unwrap();
        state.seek(&tp_test_0(), 100).unwrap();
        assert!(state.is_fetchable(&tp_test_0()));
        assert!(!state.is_paused(&tp_test_0()));
        state.mark_pending_revocation(&[tp_test_0()]).unwrap();
        assert!(!state.is_fetchable(&tp_test_0()));
        assert!(!state.is_paused(&tp_test_0()));
    }

    /// Translated from `testMarkingPendingRevocationPreventsInitializingPosition`.
    #[test]
    fn test_marking_pending_revocation_prevents_initializing_position() {
        let mut state = new_state();
        state.assign_from_user(HashSet::from([tp_test_0()])).unwrap();
        assert!(state.initializing_partitions().contains(&tp_test_0()));
        state.mark_pending_revocation(&[tp_test_0()]).unwrap();
        assert!(!state.initializing_partitions().contains(&tp_test_0()));
    }

    /// Translated from `testAssignedPartitionsAwaitingCallbackKeepPositionDefinedInCallback`.
    #[test]
    fn test_assigned_partitions_awaiting_callback_keep_position_defined_in_callback() {
        let mut state = new_state();
        state.subscribe_topics(HashSet::from([TOPIC.to_string()]), listener()).unwrap();
        state
            .assign_from_subscribed_awaiting_callback(&[tp_test_0()], &[tp_test_0()])
            .unwrap();
        assert_assignment_applied_awaiting_callback(&state, &tp_test_0());
        assert_eq!(state.subscription(), HashSet::from([tp_test_0().topic().to_string()]));

        // Callback sets position.
        state.seek(&tp_test_0(), 100).unwrap();
        state.enable_partitions_awaiting_callback(&[tp_test_0()]).unwrap();

        assert_eq!(state.initializing_partitions().len(), 0);
        assert!(state.is_fetchable(&tp_test_0()));
        assert!(state.has_all_fetch_positions());
        assert_eq!(state.position(&tp_test_0()).unwrap().unwrap().offset, 100);
    }

    /// Translated from `testAssignedPartitionsAwaitingCallbackInitializePositionsWhenCallbackCompletes`.
    #[test]
    fn test_assigned_partitions_awaiting_callback_initialize_positions_when_callback_completes() {
        let mut state = new_state();
        state.subscribe_topics(HashSet::from([TOPIC.to_string()]), listener()).unwrap();
        state
            .assign_from_subscribed_awaiting_callback(&[tp_test_0()], &[tp_test_0()])
            .unwrap();
        assert_assignment_applied_awaiting_callback(&state, &tp_test_0());

        state.enable_partitions_awaiting_callback(&[tp_test_0()]).unwrap();
        assert_eq!(state.initializing_partitions().len(), 1);
        state.seek(&tp_test_0(), 100).unwrap();
        assert!(state.is_fetchable(&tp_test_0()));
        assert!(state.has_all_fetch_positions());
        assert_eq!(state.position(&tp_test_0()).unwrap().unwrap().offset, 100);
    }

    /// Translated from `testAssignedPartitionsAwaitingCallbackDoesNotAffectPreviouslyOwnedPartitions`.
    #[test]
    fn test_assigned_partitions_awaiting_callback_does_not_affect_previously_owned_partitions() {
        let mut state = new_state();
        state.subscribe_topics(HashSet::from([TOPIC.to_string()]), listener()).unwrap();
        state
            .assign_from_subscribed_awaiting_callback(&[tp_test_0()], &[tp_test_0()])
            .unwrap();
        state.enable_partitions_awaiting_callback(&[tp_test_0()]).unwrap();
        state.seek(&tp_test_0(), 100).unwrap();
        assert!(state.is_fetchable(&tp_test_0()));

        // Add a second partition to assignment, with tp1 in `added`.
        state
            .assign_from_subscribed_awaiting_callback(&[tp_test_0(), tp_test_1()], &[tp_test_1()])
            .unwrap();
        assert!(state.is_fetchable(&tp_test_0()));
        assert!(!state.is_fetchable(&tp_test_1()));
        assert_eq!(state.initializing_partitions().len(), 1);

        // Callback completes; tp1 still needs a position.
        state.enable_partitions_awaiting_callback(&[tp_test_1()]).unwrap();
        assert_eq!(state.initializing_partitions().len(), 1);
        assert!(state.initializing_partitions().contains(&tp_test_1()));
        state.seek(&tp_test_1(), 200).unwrap();
        assert!(state.is_fetchable(&tp_test_1()));
    }

    fn assert_assignment_applied_awaiting_callback(state: &SubscriptionState, tp: &crate::common::TopicPartition) {
        assert_eq!(state.assigned_partitions(), HashSet::from([tp.clone()]));
        assert_eq!(state.num_assigned_partitions(), 1);
        assert!(!state.is_fetchable(tp));
        assert_eq!(state.initializing_partitions().len(), 1);
        assert!(!state.is_paused(tp));
    }

    /// Translated from `invalidPositionUpdate`.
    #[test]
    fn test_invalid_position_update() {
        let mut state = new_state();
        state.subscribe_topics(HashSet::from([TOPIC.to_string()]), listener()).unwrap();
        assert!(state.check_assignment_matched_subscription(&[tp_test_0()]));
        state.assign_from_subscribed(&[tp_test_0()]).unwrap();
        let err = state
            .set_position(&tp_test_0(), FetchPosition::with_leader(0, None, no_leader_no_epoch()))
            .unwrap_err();
        // Java's IllegalStateException -> Rust's Error::LocalIllegalState.
        assert!(matches!(err, crate::common::Error::LocalIllegalState(_)));
    }

    /// Translated from `cantAssignPartitionForUnsubscribedTopics`.
    #[test]
    fn test_cant_assign_partition_for_unsubscribed_topics() {
        let mut state = new_state();
        state.subscribe_topics(HashSet::from([TOPIC.to_string()]), listener()).unwrap();
        assert!(!state.check_assignment_matched_subscription(&[tp_test1_0()]));
    }

    /// Translated from `cantAssignPartitionForUnmatchedPattern`.
    #[test]
    fn test_cant_assign_partition_for_unmatched_pattern() {
        let mut state = new_state();
        state.subscribe_pattern(Regex::new(".*t").unwrap(), listener()).unwrap();
        state.subscribe_from_pattern(HashSet::from([TOPIC.to_string()])).unwrap();
        assert!(!state.check_assignment_matched_subscription(&[tp_test1_0()]));
    }

    /// Translated from `cantChangePositionForNonAssignedPartition`.
    #[test]
    fn test_cant_change_position_for_non_assigned_partition() {
        let mut state = new_state();
        let err = state
            .set_position(&tp_test_0(), FetchPosition::with_leader(1, None, no_leader_no_epoch()))
            .unwrap_err();
        assert!(matches!(err, crate::common::Error::LocalIllegalState(_)));
    }

    /// Translated from `cantSubscribeTopicAndPattern`.
    #[test]
    fn test_cant_subscribe_topic_and_pattern() {
        let mut state = new_state();
        state.subscribe_topics(HashSet::from([TOPIC.to_string()]), listener()).unwrap();
        let err = state.subscribe_pattern(Regex::new(".*").unwrap(), listener()).unwrap_err();
        assert!(matches!(err, crate::common::Error::LocalIllegalState(_)));
    }

    /// Translated from `cantSubscribePartitionAndPattern`.
    #[test]
    fn test_cant_subscribe_partition_and_pattern() {
        let mut state = new_state();
        state.assign_from_user(HashSet::from([tp_test_0()])).unwrap();
        let err = state.subscribe_pattern(Regex::new(".*").unwrap(), listener()).unwrap_err();
        assert!(matches!(err, crate::common::Error::LocalIllegalState(_)));
    }

    /// Translated from `cantSubscribePatternAndTopic`.
    #[test]
    fn test_cant_subscribe_pattern_and_topic() {
        let mut state = new_state();
        state.subscribe_pattern(Regex::new(".*").unwrap(), listener()).unwrap();
        let err = state
            .subscribe_topics(HashSet::from([TOPIC.to_string()]), listener())
            .unwrap_err();
        assert!(matches!(err, crate::common::Error::LocalIllegalState(_)));
    }

    /// Translated from `cantSubscribePatternAndPartition`.
    #[test]
    fn test_cant_subscribe_pattern_and_partition() {
        let mut state = new_state();
        state.subscribe_pattern(Regex::new(".*").unwrap(), listener()).unwrap();
        let err = state.assign_from_user(HashSet::from([tp_test_0()])).unwrap_err();
        assert!(matches!(err, crate::common::Error::LocalIllegalState(_)));
    }

    /// Translated from `patternSubscription`.
    #[test]
    fn test_pattern_subscription_two_topics() {
        let mut state = new_state();
        state.subscribe_pattern(Regex::new(".*").unwrap(), listener()).unwrap();
        state
            .subscribe_from_pattern(HashSet::from([TOPIC.to_string(), TOPIC1.to_string()]))
            .unwrap();
        assert_eq!(state.subscription().len(), 2, "Expected subscribed topics count is incorrect");
    }

    /// Translated from `testSubscribeToRe2JPattern`.
    #[test]
    fn test_subscribe_to_re2j_pattern() {
        let mut state = new_state();
        let pattern = "t.*";
        state
            .subscribe_subscription_pattern(SubscriptionPattern::new(pattern), listener())
            .unwrap();
        let s = state.to_string();
        assert!(s.contains("type=AUTO_PATTERN_RE2J"), "{s}");
        assert!(s.contains(&format!("subscribedPattern={pattern}")), "{s}");
        assert!(state.assigned_topic_ids().is_empty());
    }

    /// Translated from `testIsAssignedFromRe2j`.
    ///
    /// Java's `isAssignedFromRe2j(null)` is replaced by checking an
    /// arbitrary UUID before subscribing (functionally equivalent: when no
    /// subscription is set the function returns false unconditionally).
    #[test]
    fn test_is_assigned_from_re2j() {
        let mut state = new_state();
        let assigned_uuid = crate::common::Uuid::random_uuid();
        assert!(!state.is_assigned_from_re2j(assigned_uuid));

        state
            .subscribe_subscription_pattern(SubscriptionPattern::new("foo.*"), None)
            .unwrap();
        assert!(state.has_re2j_pattern_subscription());
        assert!(!state.is_assigned_from_re2j(assigned_uuid));

        state.set_assigned_topic_ids(HashSet::from([assigned_uuid]));
        assert!(state.is_assigned_from_re2j(assigned_uuid));

        state.unsubscribe();
        assert!(!state.is_assigned_from_re2j(assigned_uuid));
        assert!(!state.has_re2j_pattern_subscription());
    }

    /// Translated from `testAssignedPartitionsWithTopicIdsForRe2Pattern`.
    #[test]
    fn test_assigned_partitions_with_topic_ids_for_re2_pattern() {
        let mut state = new_state();
        state
            .subscribe_subscription_pattern(SubscriptionPattern::new("t.*"), listener())
            .unwrap();
        assert!(state.assigned_topic_ids().is_empty());

        state
            .assign_from_subscribed_awaiting_callback(&[tp_test_0()], &[tp_test_0()])
            .unwrap();
        assert_assignment_applied_awaiting_callback(&state, &tp_test_0());

        state.seek(&tp_test_0(), 100).unwrap();
        state.enable_partitions_awaiting_callback(&[tp_test_0()]).unwrap();
        assert_eq!(state.initializing_partitions().len(), 0);
        assert!(state.is_fetchable(&tp_test_0()));
        assert!(state.has_all_fetch_positions());
        assert_eq!(state.position(&tp_test_0()).unwrap().unwrap().offset, 100);
    }

    /// Translated from `testAssignedTopicIdsPreservedWhenReconciliationCompletes`.
    #[test]
    fn test_assigned_topic_ids_preserved_when_reconciliation_completes() {
        let mut state = new_state();
        state
            .subscribe_subscription_pattern(SubscriptionPattern::new("t.*"), listener())
            .unwrap();
        assert!(state.assigned_topic_ids().is_empty());

        let first = crate::common::Uuid::random_uuid();
        state.set_assigned_topic_ids(HashSet::from([first]));

        let second = crate::common::Uuid::random_uuid();
        state.set_assigned_topic_ids(HashSet::from([first, second]));

        state
            .assign_from_subscribed_awaiting_callback(&[tp_test_0()], &[tp_test_0()])
            .unwrap();
        assert_assignment_applied_awaiting_callback(&state, &tp_test_0());

        let ids: HashSet<crate::common::Uuid> = state.assigned_topic_ids().iter().copied().collect();
        assert_eq!(ids, HashSet::from([first, second]));
    }

    /// Translated from `testMixedPatternSubscriptionNotAllowed`.
    #[test]
    fn test_mixed_pattern_subscription_not_allowed() {
        let mut state = new_state();
        state.subscribe_pattern(Regex::new(".*").unwrap(), listener()).unwrap();
        let err = state
            .subscribe_subscription_pattern(SubscriptionPattern::new("t.*"), listener())
            .unwrap_err();
        assert!(matches!(err, crate::common::Error::LocalIllegalState(_)));

        state.unsubscribe();

        state
            .subscribe_subscription_pattern(SubscriptionPattern::new("t.*"), listener())
            .unwrap();
        let err = state.subscribe_pattern(Regex::new(".*").unwrap(), listener()).unwrap_err();
        assert!(matches!(err, crate::common::Error::LocalIllegalState(_)));
    }

    /// Translated from `testSubscriptionPattern`.
    #[test]
    fn test_subscription_pattern_getter() {
        let mut state = new_state();
        let pattern = SubscriptionPattern::new("t.*");
        state.subscribe_subscription_pattern(pattern.clone(), listener()).unwrap();
        assert!(state.has_re2j_pattern_subscription());
        assert_eq!(state.subscription_pattern(), Some(&pattern));
        assert!(state.has_auto_assigned_partitions());

        state.unsubscribe();
        assert!(!state.has_re2j_pattern_subscription());
        assert!(state.subscription_pattern().is_none());
    }

    /// Translated from `unsubscribeUserAssignment`.
    #[test]
    fn test_unsubscribe_user_assignment() {
        let mut state = new_state();
        state.assign_from_user(HashSet::from([tp_test_0(), tp_test_1()])).unwrap();
        state.unsubscribe();
        state.subscribe_topics(HashSet::from([TOPIC.to_string()]), listener()).unwrap();
        assert_eq!(state.subscription(), HashSet::from([TOPIC.to_string()]));
    }

    /// Translated from `unsubscribeUserSubscribe`.
    #[test]
    fn test_unsubscribe_user_subscribe() {
        let mut state = new_state();
        state.subscribe_topics(HashSet::from([TOPIC.to_string()]), listener()).unwrap();
        state.unsubscribe();
        state.assign_from_user(HashSet::from([tp_test_0()])).unwrap();
        assert_eq!(state.assigned_partitions(), HashSet::from([tp_test_0()]));
        assert_eq!(state.num_assigned_partitions(), 1);
    }

    /// Translated from `unsubscription`.
    #[test]
    fn test_unsubscription() {
        let mut state = new_state();
        state.subscribe_pattern(Regex::new(".*").unwrap(), listener()).unwrap();
        state
            .subscribe_from_pattern(HashSet::from([TOPIC.to_string(), TOPIC1.to_string()]))
            .unwrap();
        assert!(state.check_assignment_matched_subscription(&[tp_test_1()]));
        state.assign_from_subscribed(&[tp_test_1()]).unwrap();
        assert_eq!(state.assigned_partitions(), HashSet::from([tp_test_1()]));

        state.unsubscribe();
        assert!(state.subscription().is_empty());
        assert!(state.assigned_partitions().is_empty());

        state.assign_from_user(HashSet::from([tp_test_0()])).unwrap();
        assert_eq!(state.assigned_partitions(), HashSet::from([tp_test_0()]));

        state.unsubscribe();
        assert!(state.subscription().is_empty());
        assert!(state.assigned_partitions().is_empty());
    }

    /// Translated from `testPreferredReadReplicaLease`.
    #[test]
    fn test_subscription_state_preferred_read_replica_lease() {
        let mut state = new_state();
        state.assign_from_user(HashSet::from([tp_test_0()])).unwrap();

        assert!(state.preferred_read_replica(&tp_test_0(), 0).is_none());

        state.update_preferred_read_replica(&tp_test_0(), 42, 10).unwrap();
        assert_eq!(state.preferred_read_replica(&tp_test_0(), 9), Some(42));
        assert_eq!(state.preferred_read_replica(&tp_test_0(), 10), Some(42));
        assert!(state.preferred_read_replica(&tp_test_0(), 11).is_none());

        state.clear_preferred_read_replica(&tp_test_0());
        assert!(state.preferred_read_replica(&tp_test_0(), 9).is_none());
        assert!(state.preferred_read_replica(&tp_test_0(), 11).is_none());

        state.update_preferred_read_replica(&tp_test_0(), 43, 20).unwrap();
        assert_eq!(state.preferred_read_replica(&tp_test_0(), 11), Some(43));
        assert_eq!(state.preferred_read_replica(&tp_test_0(), 20), Some(43));
        assert!(state.preferred_read_replica(&tp_test_0(), 21).is_none());

        state.update_preferred_read_replica(&tp_test_0(), 44, 30).unwrap();
        assert_eq!(state.preferred_read_replica(&tp_test_0(), 30), Some(44));
        assert!(state.preferred_read_replica(&tp_test_0(), 31).is_none());
    }

    /// Translated from `testSeekUnvalidatedWithNoOffsetEpoch`. The
    /// `maybeValidatePositionForCurrentLeader` half is covered by
    /// [`test_maybe_validate_position_for_current_leader`].
    #[test]
    fn test_subscription_state_seek_unvalidated_with_no_offset_epoch() {
        let mut state = new_state();
        state.assign_from_user(HashSet::from([tp_test_0()])).unwrap();

        let broker1 = crate::common::Node::new(1, "localhost".to_string(), 9092);
        state
            .seek_unvalidated(
                &tp_test_0(),
                FetchPosition::with_leader(0, None, LeaderAndEpoch::new(Some(broker1), Some(5))),
            )
            .unwrap();
        assert!(state.has_valid_position(&tp_test_0()));
        assert!(!state.awaiting_validation(&tp_test_0()).unwrap());
    }

    /// Translated from `testSeekUnvalidatedWithNoEpochClearsAwaitingValidation`.
    #[test]
    fn test_seek_unvalidated_with_no_epoch_clears_awaiting_validation() {
        let mut state = new_state();
        state.assign_from_user(HashSet::from([tp_test_0()])).unwrap();
        let broker1 = crate::common::Node::new(1, "localhost".to_string(), 9092);

        // With an offset epoch -> AWAIT_VALIDATION.
        state
            .seek_unvalidated(
                &tp_test_0(),
                FetchPosition::with_leader(0, Some(2), LeaderAndEpoch::new(Some(broker1.clone()), Some(5))),
            )
            .unwrap();
        assert!(!state.has_valid_position(&tp_test_0()));
        assert!(state.awaiting_validation(&tp_test_0()).unwrap());

        // Now without -> back to FETCHING.
        state
            .seek_unvalidated(
                &tp_test_0(),
                FetchPosition::with_leader(0, None, LeaderAndEpoch::new(Some(broker1), Some(5))),
            )
            .unwrap();
        assert!(state.has_valid_position(&tp_test_0()));
        assert!(!state.awaiting_validation(&tp_test_0()).unwrap());
    }

    /// Translated from `testSeekUnvalidatedWithOffsetEpoch`. The
    /// `maybeValidatePositionForCurrentLeader` half is covered by
    /// [`test_maybe_validate_position_for_current_leader`].
    #[test]
    fn test_subscription_state_seek_unvalidated_with_offset_epoch_enters_validation() {
        let mut state = new_state();
        state.assign_from_user(HashSet::from([tp_test_0()])).unwrap();
        let broker1 = crate::common::Node::new(1, "localhost".to_string(), 9092);

        state
            .seek_unvalidated(
                &tp_test_0(),
                FetchPosition::with_leader(0, Some(2), LeaderAndEpoch::new(Some(broker1), Some(5))),
            )
            .unwrap();
        assert!(!state.has_valid_position(&tp_test_0()));
        assert!(state.awaiting_validation(&tp_test_0()).unwrap());
    }

    /// Translated from `testSeekValidatedShouldClearAwaitingValidation`.
    #[test]
    fn test_seek_validated_should_clear_awaiting_validation() {
        let mut state = new_state();
        state.assign_from_user(HashSet::from([tp_test_0()])).unwrap();
        let broker1 = crate::common::Node::new(1, "localhost".to_string(), 9092);

        state
            .seek_unvalidated(
                &tp_test_0(),
                FetchPosition::with_leader(10, Some(5), LeaderAndEpoch::new(Some(broker1.clone()), Some(10))),
            )
            .unwrap();
        assert!(!state.has_valid_position(&tp_test_0()));
        assert!(state.awaiting_validation(&tp_test_0()).unwrap());
        assert_eq!(state.position(&tp_test_0()).unwrap().unwrap().offset, 10);

        state
            .seek_validated(
                &tp_test_0(),
                FetchPosition::with_leader(8, Some(4), LeaderAndEpoch::new(Some(broker1), Some(10))),
            )
            .unwrap();
        assert!(state.has_valid_position(&tp_test_0()));
        assert!(!state.awaiting_validation(&tp_test_0()).unwrap());
        assert_eq!(state.position(&tp_test_0()).unwrap().unwrap().offset, 8);
    }

    /// Translated from `testCompleteValidationShouldClearAwaitingValidation`.
    #[test]
    fn test_complete_validation_should_clear_awaiting_validation() {
        let mut state = new_state();
        state.assign_from_user(HashSet::from([tp_test_0()])).unwrap();
        let broker1 = crate::common::Node::new(1, "localhost".to_string(), 9092);

        state
            .seek_unvalidated(
                &tp_test_0(),
                FetchPosition::with_leader(10, Some(5), LeaderAndEpoch::new(Some(broker1), Some(10))),
            )
            .unwrap();
        assert!(state.awaiting_validation(&tp_test_0()).unwrap());

        state.complete_validation(&tp_test_0()).unwrap();
        assert!(state.has_valid_position(&tp_test_0()));
        assert!(!state.awaiting_validation(&tp_test_0()).unwrap());
        assert_eq!(state.position(&tp_test_0()).unwrap().unwrap().offset, 10);
    }

    /// Translated from `testOffsetResetWhileAwaitingValidation`.
    #[test]
    fn test_offset_reset_while_awaiting_validation() {
        let mut state = new_state();
        state.assign_from_user(HashSet::from([tp_test_0()])).unwrap();
        let broker1 = crate::common::Node::new(1, "localhost".to_string(), 9092);

        state
            .seek_unvalidated(
                &tp_test_0(),
                FetchPosition::with_leader(10, Some(5), LeaderAndEpoch::new(Some(broker1), Some(10))),
            )
            .unwrap();
        assert!(state.awaiting_validation(&tp_test_0()).unwrap());

        state
            .request_offset_reset(&tp_test_0(), AutoOffsetResetStrategy::EARLIEST)
            .unwrap();
        assert!(!state.awaiting_validation(&tp_test_0()).unwrap());
        assert!(state.is_offset_reset_needed(&tp_test_0()).unwrap());
    }

    /// Translated from `nullPositionLagOnNoPosition`.
    #[test]
    fn test_null_position_lag_on_no_position() {
        let mut state = new_state();
        state.assign_from_user(HashSet::from([tp_test_0()])).unwrap();

        assert!(
            state
                .partition_lag(&tp_test_0(), IsolationLevel::ReadUncommitted)
                .unwrap()
                .is_none()
        );
        assert!(
            state
                .partition_lag(&tp_test_0(), IsolationLevel::ReadCommitted)
                .unwrap()
                .is_none()
        );

        state.update_high_watermark(&tp_test_0(), 1).unwrap();
        state.update_last_stable_offset(&tp_test_0(), 1).unwrap();

        assert!(
            state
                .partition_lag(&tp_test_0(), IsolationLevel::ReadUncommitted)
                .unwrap()
                .is_none()
        );
        assert!(
            state
                .partition_lag(&tp_test_0(), IsolationLevel::ReadCommitted)
                .unwrap()
                .is_none()
        );
    }

    /// Translated from `testPositionOrNull`.
    #[test]
    fn test_position_or_null() {
        let mut state = new_state();
        state.assign_from_user(HashSet::from([tp_test_0()])).unwrap();
        let unassigned = crate::common::TopicPartition::new("unassigned".to_string(), 0);
        state.seek(&tp_test_0(), 5).unwrap();

        assert_eq!(state.position_or_null(&tp_test_0()).unwrap().offset, 5);
        assert!(state.position_or_null(&unassigned).is_none());
    }

    /// Translated from `testTryUpdatingHighWatermark`.
    #[test]
    fn test_try_updating_high_watermark() {
        let mut state = new_state();
        state.assign_from_user(HashSet::from([tp_test_0()])).unwrap();
        let unassigned = crate::common::TopicPartition::new("unassigned".to_string(), 0);

        let hw = 10;
        assert!(state.try_updating_high_watermark(&tp_test_0(), hw));
        assert_eq!(
            state
                .partition_end_offset(&tp_test_0(), IsolationLevel::ReadUncommitted)
                .unwrap(),
            Some(hw)
        );
        assert!(!state.try_updating_high_watermark(&unassigned, hw));
    }

    /// Translated from `testTryUpdatingLogStartOffset`.
    #[test]
    fn test_try_updating_log_start_offset() {
        let mut state = new_state();
        state.assign_from_user(HashSet::from([tp_test_0()])).unwrap();
        let unassigned = crate::common::TopicPartition::new("unassigned".to_string(), 0);
        let position = 25;
        state.seek(&tp_test_0(), position).unwrap();

        let log_start_offset = 10;
        assert!(state.try_updating_log_start_offset(&tp_test_0(), log_start_offset));
        assert_eq!(state.partition_lead(&tp_test_0()).unwrap(), Some(position - log_start_offset));
        assert!(!state.try_updating_log_start_offset(&unassigned, log_start_offset));
    }

    /// Translated from `testTryUpdatingLastStableOffset`.
    #[test]
    fn test_try_updating_last_stable_offset() {
        let mut state = new_state();
        state.assign_from_user(HashSet::from([tp_test_0()])).unwrap();
        let unassigned = crate::common::TopicPartition::new("unassigned".to_string(), 0);

        let lso = 10;
        assert!(state.try_updating_last_stable_offset(&tp_test_0(), lso));
        assert_eq!(
            state.partition_end_offset(&tp_test_0(), IsolationLevel::ReadCommitted).unwrap(),
            Some(lso)
        );
        assert!(!state.try_updating_last_stable_offset(&unassigned, lso));
    }

    /// Translated from `testTryUpdatingPreferredReadReplica`.
    #[test]
    fn test_try_updating_preferred_read_replica() {
        let mut state = new_state();
        state.assign_from_user(HashSet::from([tp_test_0()])).unwrap();
        let unassigned = crate::common::TopicPartition::new("unassigned".to_string(), 0);

        let replica = 10;
        let now: i64 = 1000;
        let expiration = now + 60_000;
        assert!(state.try_updating_preferred_read_replica(&tp_test_0(), replica, expiration));
        assert_eq!(state.preferred_read_replica(&tp_test_0(), now), Some(replica));
        assert!(!state.try_updating_preferred_read_replica(&unassigned, replica, expiration));
        assert!(state.preferred_read_replica(&unassigned, now).is_none());
    }

    /// Translated from `testRequestOffsetResetIfPartitionAssigned`.
    #[test]
    fn test_request_offset_reset_if_partition_assigned() {
        let mut state = new_state();
        state.assign_from_user(HashSet::from([tp_test_0()])).unwrap();
        let unassigned = crate::common::TopicPartition::new("unassigned".to_string(), 0);

        state.request_offset_reset_if_assigned(&tp_test_0());
        assert!(state.is_offset_reset_needed(&tp_test_0()).unwrap());

        // No-op on unassigned; subsequent `is_offset_reset_needed` errors
        // because the partition is not in the assignment.
        state.request_offset_reset_if_assigned(&unassigned);
        let err = state.is_offset_reset_needed(&unassigned).unwrap_err();
        assert!(matches!(err, crate::common::Error::LocalIllegalState(_)));
    }

    /// Translated from `testFetchablePartitionsPerformsCheapChecksFirst`.
    #[test]
    fn test_fetchable_partitions_performs_cheap_checks_first() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let mut state = new_state();
        state.assign_from_user(HashSet::from([tp_test_0()])).unwrap();
        state.seek(&tp_test_0(), 100).unwrap();
        assert!(state.is_fetchable(&tp_test_0()));
        state.pause(&tp_test_0()).unwrap();

        let predicate_evaluated = Arc::new(AtomicBool::new(false));
        let pe = Arc::clone(&predicate_evaluated);
        let fetchable = state.fetchable_partitions(move |_| {
            pe.store(true, Ordering::SeqCst);
            true
        });
        assert!(fetchable.is_empty());
        assert!(
            !predicate_evaluated.load(Ordering::SeqCst),
            "Custom predicate should not be evaluated when partitions are not fetchable"
        );

        state.resume(&tp_test_0()).unwrap();
        predicate_evaluated.store(false, Ordering::SeqCst);
        let pe = Arc::clone(&predicate_evaluated);
        let fetchable = state.fetchable_partitions(move |_| {
            pe.store(true, Ordering::SeqCst);
            true
        });
        assert!(predicate_evaluated.load(Ordering::SeqCst));
        assert_eq!(fetchable[0], tp_test_0());
    }

    // ─── Phase 7d: maybe_validate_position_for_current_leader /
    // maybe_complete_validation translation ─────────────────────────

    use crate::api_versions::ApiVersions as ApiVersionsType;
    use crate::common::protocol::ApiKeys;
    use crate::node_api_versions::NodeApiVersions;
    use crate::offset_for_leader_epoch_response_data::EpochEndOffset;

    fn epoch_end_offset(leader_epoch: i32, end_offset: i64) -> EpochEndOffset {
        let mut e = EpochEndOffset::new();
        e.set_leader_epoch(leader_epoch);
        e.set_end_offset(end_offset);
        e
    }

    /// Translated from `testMaybeCompleteValidation`.
    #[test]
    fn test_maybe_complete_validation() {
        let mut state = new_state();
        let broker1 = crate::common::Node::new(1, "localhost".to_string(), 9092);
        state.assign_from_user(HashSet::from([tp_test_0()])).unwrap();

        let current_epoch = 10;
        let initial_offset = 10;
        let initial_offset_epoch = 5;

        let initial_position = FetchPosition::with_leader(
            initial_offset,
            Some(initial_offset_epoch),
            LeaderAndEpoch::new(Some(broker1.clone()), Some(current_epoch)),
        );
        state.seek_unvalidated(&tp_test_0(), initial_position.clone()).unwrap();
        assert!(state.awaiting_validation(&tp_test_0()).unwrap());

        let truncation_opt = state.maybe_complete_validation(
            &tp_test_0(),
            &initial_position,
            &epoch_end_offset(initial_offset_epoch, initial_offset + 5),
        );
        assert!(truncation_opt.is_none());
        assert!(!state.awaiting_validation(&tp_test_0()).unwrap());
        assert_eq!(state.position(&tp_test_0()).unwrap(), Some(&initial_position));
    }

    /// Translated from `testMaybeValidatePositionForCurrentLeader`.
    #[test]
    fn test_maybe_validate_position_for_current_leader() {
        let mut state = new_state();
        let broker1 = crate::common::Node::new(1, "localhost".to_string(), 9092);
        state.assign_from_user(HashSet::from([tp_test_0()])).unwrap();

        // Old API: skip validation.
        let api_versions = ApiVersionsType::new();
        let old_apis = NodeApiVersions::create_single(ApiKeys::OFFSET_FOR_LEADER_EPOCH.id(), 0, 2);
        api_versions.update("1", old_apis);

        state
            .seek_unvalidated(
                &tp_test_0(),
                FetchPosition::with_leader(10, Some(5), LeaderAndEpoch::new(Some(broker1.clone()), Some(10))),
            )
            .unwrap();

        assert!(!state.maybe_validate_position_for_current_leader(
            &api_versions,
            &tp_test_0(),
            &LeaderAndEpoch::new(Some(broker1.clone()), Some(10)),
        ));
        assert!(state.has_valid_position(&tp_test_0()));

        // New API: enter validation.
        api_versions.update("1", NodeApiVersions::create());
        state
            .seek_unvalidated(
                &tp_test_0(),
                FetchPosition::with_leader(10, Some(5), LeaderAndEpoch::new(Some(broker1.clone()), Some(10))),
            )
            .unwrap();
        // The Java test asserts true here because after the second
        // seek_unvalidated the partition is in AWAIT_VALIDATION; with a
        // new-API broker the maybe-validate call either re-validates or
        // sees `position.current_leader == new_leader_and_epoch` and
        // leaves AWAIT_VALIDATION as-is — so the helper returns true.
        assert!(state.maybe_validate_position_for_current_leader(
            &api_versions,
            &tp_test_0(),
            &LeaderAndEpoch::new(Some(broker1.clone()), Some(10)),
        ));
        assert!(!state.has_valid_position(&tp_test_0()));

        // tp_test_1 isn't assigned: skip.
        assert!(!state.maybe_validate_position_for_current_leader(
            &api_versions,
            &tp_test_1(),
            &LeaderAndEpoch::new(Some(broker1.clone()), Some(10)),
        ));
        assert!(!state.assigned_partitions().contains(&tp_test_1()));
    }

    /// Translated from `testMaybeCompleteValidationAfterPositionChange`.
    #[test]
    fn test_maybe_complete_validation_after_position_change() {
        let mut state = new_state();
        let broker1 = crate::common::Node::new(1, "localhost".to_string(), 9092);
        state.assign_from_user(HashSet::from([tp_test_0()])).unwrap();

        let current_epoch = 10;
        let initial_offset = 10;
        let initial_offset_epoch = 5;
        let update_offset = 20;
        let update_offset_epoch = 8;

        let initial_position = FetchPosition::with_leader(
            initial_offset,
            Some(initial_offset_epoch),
            LeaderAndEpoch::new(Some(broker1.clone()), Some(current_epoch)),
        );
        state.seek_unvalidated(&tp_test_0(), initial_position.clone()).unwrap();
        assert!(state.awaiting_validation(&tp_test_0()).unwrap());

        let update_position = FetchPosition::with_leader(
            update_offset,
            Some(update_offset_epoch),
            LeaderAndEpoch::new(Some(broker1.clone()), Some(current_epoch)),
        );
        state.seek_unvalidated(&tp_test_0(), update_position.clone()).unwrap();

        let truncation_opt = state.maybe_complete_validation(
            &tp_test_0(),
            &initial_position,
            &epoch_end_offset(initial_offset_epoch, initial_offset + 5),
        );
        assert!(truncation_opt.is_none());
        assert!(state.awaiting_validation(&tp_test_0()).unwrap());
        assert_eq!(state.position(&tp_test_0()).unwrap(), Some(&update_position));
    }

    /// Translated from `testMaybeCompleteValidationAfterOffsetReset`.
    #[test]
    fn test_maybe_complete_validation_after_offset_reset() {
        let mut state = new_state();
        let broker1 = crate::common::Node::new(1, "localhost".to_string(), 9092);
        state.assign_from_user(HashSet::from([tp_test_0()])).unwrap();

        let current_epoch = 10;
        let initial_offset = 10;
        let initial_offset_epoch = 5;

        let initial_position = FetchPosition::with_leader(
            initial_offset,
            Some(initial_offset_epoch),
            LeaderAndEpoch::new(Some(broker1.clone()), Some(current_epoch)),
        );
        state.seek_unvalidated(&tp_test_0(), initial_position.clone()).unwrap();
        assert!(state.awaiting_validation(&tp_test_0()).unwrap());

        state.request_offset_reset_default(&tp_test_0()).unwrap();

        let truncation_opt = state.maybe_complete_validation(
            &tp_test_0(),
            &initial_position,
            &epoch_end_offset(initial_offset_epoch, initial_offset + 5),
        );
        assert!(truncation_opt.is_none());
        assert!(!state.awaiting_validation(&tp_test_0()).unwrap());
        assert!(state.is_offset_reset_needed(&tp_test_0()).unwrap());
        // Java asserts position is null after reset.
        assert!(state.position(&tp_test_0()).unwrap().is_none());
    }

    /// Translated from `testTruncationDetectionWithResetPolicy`.
    #[test]
    fn test_truncation_detection_with_reset_policy() {
        let mut state = new_state(); // EARLIEST policy.
        let broker1 = crate::common::Node::new(1, "localhost".to_string(), 9092);
        state.assign_from_user(HashSet::from([tp_test_0()])).unwrap();

        let current_epoch = 10;
        let initial_offset = 10;
        let initial_offset_epoch = 5;
        let divergent_offset = 5;
        let divergent_offset_epoch = 7;

        let initial_position = FetchPosition::with_leader(
            initial_offset,
            Some(initial_offset_epoch),
            LeaderAndEpoch::new(Some(broker1.clone()), Some(current_epoch)),
        );
        state.seek_unvalidated(&tp_test_0(), initial_position.clone()).unwrap();
        assert!(state.awaiting_validation(&tp_test_0()).unwrap());

        let truncation_opt = state.maybe_complete_validation(
            &tp_test_0(),
            &initial_position,
            &epoch_end_offset(divergent_offset_epoch, divergent_offset),
        );
        assert!(truncation_opt.is_none());
        assert!(!state.awaiting_validation(&tp_test_0()).unwrap());

        let updated_position = FetchPosition::with_leader(
            divergent_offset,
            Some(divergent_offset_epoch),
            LeaderAndEpoch::new(Some(broker1.clone()), Some(current_epoch)),
        );
        assert_eq!(state.position(&tp_test_0()).unwrap(), Some(&updated_position));
    }

    /// Translated from `testTruncationDetectionWithoutResetPolicy`.
    #[test]
    fn test_truncation_detection_without_reset_policy() {
        let mut state = SubscriptionState::new(AutoOffsetResetStrategy::NONE);
        let broker1 = crate::common::Node::new(1, "localhost".to_string(), 9092);
        state.assign_from_user(HashSet::from([tp_test_0()])).unwrap();

        let current_epoch = 10;
        let initial_offset = 10;
        let initial_offset_epoch = 5;
        let divergent_offset = 5;
        let divergent_offset_epoch = 7;

        let initial_position = FetchPosition::with_leader(
            initial_offset,
            Some(initial_offset_epoch),
            LeaderAndEpoch::new(Some(broker1.clone()), Some(current_epoch)),
        );
        state.seek_unvalidated(&tp_test_0(), initial_position.clone()).unwrap();
        assert!(state.awaiting_validation(&tp_test_0()).unwrap());

        let truncation_opt = state.maybe_complete_validation(
            &tp_test_0(),
            &initial_position,
            &epoch_end_offset(divergent_offset_epoch, divergent_offset),
        );
        let truncation = truncation_opt.expect("truncation must be reported");
        let expected_divergent =
            crate::consumer::OffsetAndMetadata::with_leader_epoch(divergent_offset, Some(divergent_offset_epoch), "")
                .unwrap();
        assert_eq!(truncation.divergent_offset_opt, Some(expected_divergent));
        assert_eq!(truncation.fetch_position, initial_position);
        assert!(state.awaiting_validation(&tp_test_0()).unwrap());
    }

    /// Translated from `testTruncationDetectionUnknownDivergentOffsetWithResetPolicy`.
    #[test]
    fn test_truncation_detection_unknown_divergent_offset_with_reset_policy() {
        let mut state = SubscriptionState::new(AutoOffsetResetStrategy::EARLIEST);
        let broker1 = crate::common::Node::new(1, "localhost".to_string(), 9092);
        state.assign_from_user(HashSet::from([tp_test_0()])).unwrap();

        let current_epoch = 10;
        let initial_offset = 10;
        let initial_offset_epoch = 5;

        let initial_position = FetchPosition::with_leader(
            initial_offset,
            Some(initial_offset_epoch),
            LeaderAndEpoch::new(Some(broker1.clone()), Some(current_epoch)),
        );
        state.seek_unvalidated(&tp_test_0(), initial_position.clone()).unwrap();
        assert!(state.awaiting_validation(&tp_test_0()).unwrap());

        let undefined_epoch = crate::common::requests::offsets_for_leader_epoch_response::UNDEFINED_EPOCH;
        let undefined_epoch_offset = crate::common::requests::offsets_for_leader_epoch_response::UNDEFINED_EPOCH_OFFSET;
        let truncation_opt = state.maybe_complete_validation(
            &tp_test_0(),
            &initial_position,
            &epoch_end_offset(undefined_epoch, undefined_epoch_offset),
        );
        assert!(truncation_opt.is_none());
        assert!(!state.awaiting_validation(&tp_test_0()).unwrap());
        assert!(state.is_offset_reset_needed(&tp_test_0()).unwrap());
        assert_eq!(
            state.reset_strategy(&tp_test_0()).unwrap(),
            Some(AutoOffsetResetStrategy::EARLIEST)
        );
    }

    /// Translated from `testTruncationDetectionUnknownDivergentOffsetWithoutResetPolicy`.
    #[test]
    fn test_truncation_detection_unknown_divergent_offset_without_reset_policy() {
        let mut state = SubscriptionState::new(AutoOffsetResetStrategy::NONE);
        let broker1 = crate::common::Node::new(1, "localhost".to_string(), 9092);
        state.assign_from_user(HashSet::from([tp_test_0()])).unwrap();

        let current_epoch = 10;
        let initial_offset = 10;
        let initial_offset_epoch = 5;

        let initial_position = FetchPosition::with_leader(
            initial_offset,
            Some(initial_offset_epoch),
            LeaderAndEpoch::new(Some(broker1.clone()), Some(current_epoch)),
        );
        state.seek_unvalidated(&tp_test_0(), initial_position.clone()).unwrap();
        assert!(state.awaiting_validation(&tp_test_0()).unwrap());

        let undefined_epoch = crate::common::requests::offsets_for_leader_epoch_response::UNDEFINED_EPOCH;
        let undefined_epoch_offset = crate::common::requests::offsets_for_leader_epoch_response::UNDEFINED_EPOCH_OFFSET;
        let truncation_opt = state.maybe_complete_validation(
            &tp_test_0(),
            &initial_position,
            &epoch_end_offset(undefined_epoch, undefined_epoch_offset),
        );
        let truncation = truncation_opt.expect("truncation must be reported");
        assert!(truncation.divergent_offset_opt.is_none());
        assert_eq!(truncation.fetch_position, initial_position);
        assert!(state.awaiting_validation(&tp_test_0()).unwrap());
    }

    /// Translated from `resetOffsetNoValidation`.
    #[test]
    fn reset_offset_no_validation() {
        let mut state = new_state();
        let broker1 = crate::common::Node::new(1, "localhost".to_string(), 9092);
        state.assign_from_user(HashSet::from([tp_test_0()])).unwrap();

        // Reset offsets.
        state
            .request_offset_reset(&tp_test_0(), AutoOffsetResetStrategy::EARLIEST)
            .unwrap();

        // Attempt to validate with older API version: do nothing.
        let old_apis = ApiVersionsType::new();
        old_apis.update("1", NodeApiVersions::create_single(ApiKeys::OFFSET_FOR_LEADER_EPOCH.id(), 0, 2));
        assert!(!state.maybe_validate_position_for_current_leader(
            &old_apis,
            &tp_test_0(),
            &LeaderAndEpoch::new(Some(broker1.clone()), None),
        ));
        assert!(!state.has_valid_position(&tp_test_0()));
        assert!(!state.awaiting_validation(&tp_test_0()).unwrap());
        assert!(state.is_offset_reset_needed(&tp_test_0()).unwrap());

        // Complete the reset via unvalidated seek.
        state.seek_unvalidated(&tp_test_0(), FetchPosition::new(10)).unwrap();
        assert!(state.has_valid_position(&tp_test_0()));
        assert!(!state.awaiting_validation(&tp_test_0()).unwrap());
        assert!(!state.is_offset_reset_needed(&tp_test_0()).unwrap());

        // Next call to validate offsets does nothing.
        assert!(!state.maybe_validate_position_for_current_leader(
            &old_apis,
            &tp_test_0(),
            &LeaderAndEpoch::new(Some(broker1.clone()), None),
        ));
        assert!(state.has_valid_position(&tp_test_0()));
        assert!(!state.awaiting_validation(&tp_test_0()).unwrap());
        assert!(!state.is_offset_reset_needed(&tp_test_0()).unwrap());

        // Reset again, complete with a seek that requires validation.
        state
            .request_offset_reset(&tp_test_0(), AutoOffsetResetStrategy::EARLIEST)
            .unwrap();
        state
            .seek_unvalidated(
                &tp_test_0(),
                FetchPosition::with_leader(10, Some(10), LeaderAndEpoch::new(Some(broker1.clone()), Some(2))),
            )
            .unwrap();
        // AWAIT_VALIDATION state.
        assert!(!state.has_valid_position(&tp_test_0()));
        assert!(state.awaiting_validation(&tp_test_0()).unwrap());
        assert!(!state.is_offset_reset_needed(&tp_test_0()).unwrap());

        // Next call to validate clears the validation state.
        assert!(!state.maybe_validate_position_for_current_leader(
            &old_apis,
            &tp_test_0(),
            &LeaderAndEpoch::new(Some(broker1.clone()), Some(2)),
        ));
        assert!(state.has_valid_position(&tp_test_0()));
        assert!(!state.awaiting_validation(&tp_test_0()).unwrap());
        assert!(!state.is_offset_reset_needed(&tp_test_0()).unwrap());
    }
}
