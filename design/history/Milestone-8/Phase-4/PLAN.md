# Phase 4: `SubscriptionState` + `ConsumerMetadata`

## Goal

Translate the consumer's shared mutable state container — the partition /
subscription / offset state machine that every later phase reads and writes
— and the consumer-specific `Metadata` subclass that scopes metadata
requests to the subscribed topics.

This is the largest pure-state phase in the milestone: ~1.4K LOC of
production code, ~1.6K LOC of tests, and one common-package prerequisite
(`PartitionStates<S>`). After this lands, Phase 3 (`MockConsumer`) becomes
unblocked, and Phases 5–11 can lock-acquire `Arc<Mutex<SubscriptionState>>`
to read/mutate subscription, position, and pause state.

No networking. No tokio tasks. Just data structures, state transitions,
and the consumer-side `Metadata` override hooks (using the
`MetadataOverrides` precedent set by `ProducerMetadata`).

## Branch

`consumer-impl`. All commits land here.

## Java sources

All paths relative to `kafka/clients/src/main/java/`, at submodule commit
`a18251bae0b825c69794a50dffd4c3100cf5ca5b`.

Production:

- `org/apache/kafka/clients/consumer/internals/SubscriptionState.java` (1445)
- `org/apache/kafka/clients/consumer/internals/ConsumerMetadata.java` (144)
- `org/apache/kafka/common/internals/PartitionStates.java` (155) —
  prerequisite (`SubscriptionState` field type), no translation precedent
  in Rust codebase

Tests (translate each — DoD §3):

- `clients/consumer/internals/SubscriptionStateTest.java` (1161) →
  `tests/consumer/internals/subscription_state_test.rs`
- `clients/consumer/internals/ConsumerMetadataTest.java` (453) →
  `tests/consumer/internals/consumer_metadata_test.rs`
- `common/internals/PartitionStatesTest.java` (212) →
  `tests/common/internals/partition_states_test.rs`

## Out of scope (deferred to later phases)

- `maybeValidatePositionForCurrentLeader(ApiVersions, TopicPartition, LeaderAndEpoch)`
  and `maybeCompleteValidation(TopicPartition, FetchPosition, EpochEndOffset)`:
  both require `EpochEndOffset` (from `OffsetForLeaderEpochResponseData`)
  and `OffsetFetcherUtils.hasUsableOffsetForLeaderEpochVersion` — neither
  exists yet in Rust; both are owned by Phase 7 (Fetch path /
  `OffsetForLeaderEpochClient`). These methods, *and their dedicated
  `SubscriptionStateTest` cases* (`testMaybeCompleteValidation`,
  `testMaybeCompleteValidationAfterPositionChange`,
  `testMaybeCompleteValidationAfterOffsetReset`,
  `testMaybeValidatePositionForCurrentLeader`), are deferred to Phase 7.
  Rationale: the methods have no caller until `OffsetForLeaderEpochClient`
  lands in Phase 7; translating them now would require translating
  `EpochEndOffset` + `OffsetsForLeaderEpochRequest.supportsTopicPermission`
  in isolation, with no downstream consumer to validate the wiring. Phase 7
  takes them together. Per CLAUDE.md §5 we do **not** ship stubs / panics
  / `unimplemented!()` — we just don't define the methods yet.

  *Knock-on effect on the test surface:* tests using
  `seekUnvalidated(tp, position)` to enter `AWAIT_VALIDATION` state and
  observe transitions (`testSeekUnvalidatedWithNoOffsetEpoch`,
  `testSeekUnvalidatedWithNoEpochClearsAwaitingValidation`,
  `testSeekUnvalidatedWithOffsetEpoch`, `testSeekValidatedShouldClearAwaitingValidation`,
  `testCompleteValidationShouldClearAwaitingValidation`,
  `testOffsetResetWhileAwaitingValidation`) are *kept* in Phase 4 because
  they only call `seekUnvalidated` / `seekValidated` / `completeValidation`
  / `awaitingValidation` — all of which are local state methods that do
  NOT need `EpochEndOffset`. Only the four `maybe*Validation*` tests above
  defer to Phase 7.

- `ShareConsumer` paths: `subscribeToShareGroup(Set<String>)` and the
  `AUTO_TOPICS_SHARE` `SubscriptionType` variant are translated for
  internal completeness (the `fetchablePartitions` filter checks the
  variant) — but no public Rust caller exists for `subscribeToShareGroup`
  yet, and Share consumer files (`KafkaShareConsumer`, etc.) remain
  out-of-milestone per `consumer-threading.md` §20. Translating these
  members now keeps the state machine logic identical to Java; removing
  them would create a behavior difference in `hasAutoAssignedPartitions()`
  and `isFetchableAndSubscribed()` that we'd have to remember to revert
  later.

- `Metadata`-subclass machinery: Rust already uses composition via
  `MetadataOverrides` (see `ProducerMetadata`). `ConsumerMetadata` follows
  that precedent. No changes to `src/metadata.rs`.

## Module structure produced by this phase

```
src/common/internals/
└── partition_states.rs              # NEW: PartitionStates<S>

src/consumer/internals/
├── subscription_state.rs            # NEW: SubscriptionState + inner types
└── consumer_metadata.rs             # NEW: ConsumerMetadata over Arc<Metadata>

tests/common/internals/
└── partition_states_test.rs         # NEW

tests/consumer/internals/
├── subscription_state_test.rs       # NEW
└── consumer_metadata_test.rs        # NEW
```

`src/common/internals/mod.rs` gets `pub(crate) mod partition_states;` and
a `pub(crate) use` re-export per CLAUDE.md §2.

`src/consumer/internals/mod.rs` gets two new `pub(crate) mod` lines and
re-exports.

The Phase 2 `tests/consumer/internals/consumer_interceptors_test.rs` was
left to be added when those tests are translated (see COMMENTS.2.md
follow-up #2 / #3); Phase 4 adds the sibling files but does not touch
`consumer_interceptors_test.rs` (separate workstream).

## Type-by-type spec

### `PartitionStates<S>` (`src/common/internals/partition_states.rs`)

`pub(crate)` per CLAUDE.md §2. Translates `org.apache.kafka.common.internals.PartitionStates`.

```rust
use indexmap::IndexMap; // or std impl over LinkedHashMap-equivalent
use crate::common::TopicPartition;

pub(crate) struct PartitionStates<S> {
    map: IndexMap<TopicPartition, S>,
}

impl<S> PartitionStates<S> {
    pub(crate) fn new() -> Self;

    /// Java: `set(Map<TopicPartition, S>)` — clears then re-inserts
    /// grouped by topic to preserve fetch-serialization locality.
    pub(crate) fn set(&mut self, partition_to_state: HashMap<TopicPartition, S>);

    pub(crate) fn update(&mut self, tp: TopicPartition, state: S);

    pub(crate) fn move_to_end(&mut self, tp: &TopicPartition);

    pub(crate) fn update_and_move_to_end(&mut self, tp: TopicPartition, state: S);

    pub(crate) fn remove(&mut self, tp: &TopicPartition);

    pub(crate) fn clear(&mut self);

    pub(crate) fn contains(&self, tp: &TopicPartition) -> bool;

    pub(crate) fn state_value(&self, tp: &TopicPartition) -> Option<&S>;

    pub(crate) fn state_value_mut(&mut self, tp: &TopicPartition) -> Option<&mut S>;

    pub(crate) fn partition_set(&self) -> impl Iterator<Item = &TopicPartition>;

    pub(crate) fn state_iter(&self) -> impl Iterator<Item = &S>;

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&TopicPartition, &S)>;

    pub(crate) fn iter_mut(&mut self) -> impl Iterator<Item = (&TopicPartition, &mut S)>;

    pub(crate) fn partition_state_values(&self) -> Vec<&S>;

    pub(crate) fn size(&self) -> usize;
}
```

Translation notes:

- **`indexmap` crate**: already a workspace dep
  (`Cargo.toml:29 — indexmap = "2"`). Use `IndexMap` directly — no new
  dependency needed.

- **Thread-safety**: Java's `size()` is `volatile int`; Java's other
  methods are explicitly documented as not thread-safe. Rust translation
  drops the volatile-int gymnastics — the entire struct is wrapped in an
  external `Mutex` by `SubscriptionState`, and the `size()` thread-safety
  doc comment is dropped from rustdoc (it was an artifact of Java
  consumers reading `size` without the outer lock; in Rust no such caller
  exists).

- **`set()` grouping by topic**: Java preserves insertion order while
  also batching by topic ("a0, a1, b1, b0, c0, c1"). The Rust translation
  must preserve this: group the incoming `HashMap` entries by topic into
  a `LinkedHashMap<String, Vec<TopicPartition>>`-equivalent (Rust:
  `IndexMap<&str, Vec<TopicPartition>>` over the input), then re-insert
  into the main map in that batched order. The `PartitionStatesTest.testSet`
  test asserts this ordering.

- **No `BiConsumer`**: Java's `forEach(BiConsumer)` becomes Rust's normal
  `for (tp, state) in &states` iteration via the `iter()` method.

### `SubscriptionState` (`src/consumer/internals/subscription_state.rs`)

`pub(crate)` per CLAUDE.md §2. The single largest type in the milestone.

#### Public surface

```rust
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use regex::Regex;

use crate::common::{IsolationLevel, TopicPartition, Uuid};
use crate::consumer::{ConsumerRebalanceListener, OffsetAndMetadata, SubscriptionPattern};
use crate::consumer::internals::AutoOffsetResetStrategy;
use crate::common::internals::PartitionStates;
use crate::metadata::LeaderAndEpoch;
use crate::api_versions::ApiVersions;
use crate::errors::KafkaError;

pub(crate) struct SubscriptionState {
    subscription_type: SubscriptionType,
    subscribed_pattern: Option<Regex>,
    subscribed_re2j_pattern: Option<SubscriptionPattern>,
    subscription: BTreeSet<String>, // Java: TreeSet for stable logging
    assigned_topic_ids: BTreeSet<Uuid>,
    group_subscription: HashSet<String>,
    assignment: PartitionStates<TopicPartitionState>,
    default_reset_strategy: AutoOffsetResetStrategy,
    rebalance_listener: Option<Arc<dyn ConsumerRebalanceListener>>,
    assignment_id: u32,
}

impl SubscriptionState {
    pub(crate) fn new(default_reset_strategy: AutoOffsetResetStrategy) -> Self;

    // ── Subscription mutation (Java: synchronized) ───────────────────────
    pub(crate) fn subscribe_topics(
        &mut self,
        topics: HashSet<String>,
        listener: Option<Arc<dyn ConsumerRebalanceListener>>,
    ) -> Result<bool, KafkaError>;

    pub(crate) fn subscribe_pattern(
        &mut self,
        pattern: Regex,
        listener: Option<Arc<dyn ConsumerRebalanceListener>>,
    ) -> Result<(), KafkaError>;

    pub(crate) fn subscribe_re2j_pattern(
        &mut self,
        pattern: SubscriptionPattern,
        listener: Option<Arc<dyn ConsumerRebalanceListener>>,
    ) -> Result<(), KafkaError>;

    pub(crate) fn subscribe_from_pattern(
        &mut self,
        topics: HashSet<String>,
    ) -> Result<bool, KafkaError>;

    pub(crate) fn subscribe_to_share_group(
        &mut self,
        topics: HashSet<String>,
    ) -> Result<bool, KafkaError>;

    pub(crate) fn assign_from_user(
        &mut self,
        partitions: HashSet<TopicPartition>,
    ) -> Result<bool, KafkaError>;

    pub(crate) fn assign_from_subscribed(
        &mut self,
        assignments: &[TopicPartition],
    ) -> Result<(), KafkaError>;

    pub(crate) fn assign_from_subscribed_awaiting_callback(
        &mut self,
        full_assignment: &[TopicPartition],
        added_partitions: &[TopicPartition],
    ) -> Result<(), KafkaError>;

    pub(crate) fn check_assignment_matched_subscription(
        &self,
        assignments: &[TopicPartition],
    ) -> bool;

    pub(crate) fn unsubscribe(&mut self);

    // ── Read-only accessors (Java: synchronized) ─────────────────────────
    pub(crate) fn assignment_id(&self) -> u32;
    pub(crate) fn has_pattern_subscription(&self) -> bool;
    pub(crate) fn has_re2j_pattern_subscription(&self) -> bool;
    pub(crate) fn has_no_subscription_or_user_assignment(&self) -> bool;
    pub(crate) fn has_auto_assigned_partitions(&self) -> bool;
    pub(crate) fn matches_subscribed_pattern(&self, topic: &str) -> bool;
    pub(crate) fn subscription(&self) -> HashSet<String>;
    pub(crate) fn subscription_pattern(&self) -> Option<&SubscriptionPattern>;
    pub(crate) fn paused_partitions(&self) -> HashSet<TopicPartition>;
    pub(crate) fn assigned_partitions(&self) -> HashSet<TopicPartition>;
    pub(crate) fn assigned_partitions_list(&self) -> Vec<TopicPartition>;
    pub(crate) fn num_assigned_partitions(&self) -> usize;
    pub(crate) fn is_assigned(&self, tp: &TopicPartition) -> bool;
    pub(crate) fn is_paused(&self, tp: &TopicPartition) -> bool;
    pub(crate) fn has_valid_position(&self, tp: &TopicPartition) -> bool;
    pub(crate) fn has_all_fetch_positions(&self) -> bool;
    pub(crate) fn assigned_topic_ids(&self) -> &BTreeSet<Uuid>;
    pub(crate) fn is_assigned_from_re2j(&self, topic_id: Uuid) -> bool;
    pub(crate) fn rebalance_listener(&self) -> Option<Arc<dyn ConsumerRebalanceListener>>;

    // ── Mutators on per-partition state (Java: synchronized) ─────────────
    pub(crate) fn seek_validated(
        &mut self,
        tp: &TopicPartition,
        position: FetchPosition,
    ) -> Result<(), KafkaError>;

    pub(crate) fn seek_unvalidated(
        &mut self,
        tp: &TopicPartition,
        position: FetchPosition,
    ) -> Result<(), KafkaError>;

    /// Convenience: `seekValidated(tp, FetchPosition::new(offset))`.
    pub(crate) fn seek(
        &mut self,
        tp: &TopicPartition,
        offset: i64,
    ) -> Result<(), KafkaError>;

    pub(crate) fn maybe_seek_unvalidated(
        &mut self,
        tp: &TopicPartition,
        position: FetchPosition,
        requested_reset_strategy: Option<&AutoOffsetResetStrategy>,
    );

    pub(crate) fn pause(&mut self, tp: &TopicPartition) -> Result<(), KafkaError>;
    pub(crate) fn resume(&mut self, tp: &TopicPartition) -> Result<(), KafkaError>;
    pub(crate) fn mark_pending_revocation(&mut self, tps: &[TopicPartition]) -> Result<(), KafkaError>;
    pub(crate) fn enable_partitions_awaiting_callback(&mut self, partitions: &[TopicPartition]) -> Result<(), KafkaError>;
    pub(crate) fn set_assigned_topic_ids(&mut self, ids: HashSet<Uuid>);

    pub(crate) fn position(&self, tp: &TopicPartition) -> Result<Option<&FetchPosition>, KafkaError>;
    pub(crate) fn position_or_null(&self, tp: &TopicPartition) -> Option<&FetchPosition>;
    pub(crate) fn set_position(&mut self, tp: &TopicPartition, position: FetchPosition) -> Result<(), KafkaError>;
    pub(crate) fn valid_position(&self, tp: &TopicPartition) -> Result<Option<&FetchPosition>, KafkaError>;
    pub(crate) fn awaiting_validation(&self, tp: &TopicPartition) -> Result<bool, KafkaError>;
    pub(crate) fn complete_validation(&mut self, tp: &TopicPartition) -> Result<(), KafkaError>;

    pub(crate) fn partition_lag(&self, tp: &TopicPartition, isolation_level: IsolationLevel) -> Result<Option<i64>, KafkaError>;
    pub(crate) fn partition_end_offset(&self, tp: &TopicPartition, isolation_level: IsolationLevel) -> Result<Option<i64>, KafkaError>;
    pub(crate) fn request_partition_end_offset(&mut self, tp: &TopicPartition) -> Result<(), KafkaError>;
    pub(crate) fn partition_end_offset_requested(&self, tp: &TopicPartition) -> Result<bool, KafkaError>;

    pub(crate) fn update_preferred_read_replica(&mut self, tp: &TopicPartition, replica_id: i32, time_ms: i64) -> Result<(), KafkaError>;
    pub(crate) fn try_updating_preferred_read_replica(&mut self, tp: &TopicPartition, replica_id: i32, time_ms: i64) -> bool;
    pub(crate) fn preferred_read_replica(&mut self, tp: &TopicPartition, time_ms: i64) -> Option<i32>;
    pub(crate) fn clear_preferred_read_replica(&mut self, tp: &TopicPartition) -> Option<i32>;

    pub(crate) fn all_consumed(&self) -> HashMap<TopicPartition, OffsetAndMetadata>;

    pub(crate) fn request_offset_reset(
        &mut self,
        partition: &TopicPartition,
        strategy: AutoOffsetResetStrategy,
    ) -> Result<(), KafkaError>;

    pub(crate) fn request_offset_reset_all(
        &mut self,
        partitions: &[TopicPartition],
        strategy: AutoOffsetResetStrategy,
    ) -> Result<(), KafkaError>;

    pub(crate) fn request_offset_reset_default(
        &mut self,
        partition: &TopicPartition,
    ) -> Result<(), KafkaError>;

    pub(crate) fn request_offset_reset_if_assigned(&mut self, partition: &TopicPartition);

    pub(crate) fn is_offset_reset_needed(&self, partition: &TopicPartition) -> Result<bool, KafkaError>;
    pub(crate) fn reset_strategy(&self, partition: &TopicPartition) -> Result<Option<AutoOffsetResetStrategy>, KafkaError>;

    pub(crate) fn initializing_partitions(&self) -> HashSet<TopicPartition>;

    pub(crate) fn reset_initializing_positions(
        &mut self,
        init_partitions_to_include: impl Fn(&TopicPartition) -> bool,
    ) -> Result<(), KafkaError>; // returns NoOffsetForPartitionError on failure

    pub(crate) fn reset_initializing_positions_all(&mut self) -> Result<(), KafkaError>;

    pub(crate) fn partitions_needing_reset(&self, now_ms: i64) -> HashSet<TopicPartition>;
    pub(crate) fn partitions_needing_validation(&self, now_ms: i64) -> HashMap<TopicPartition, FetchPosition>;
    pub(crate) fn has_partitions_needing_validation(&self, now_ms: i64) -> bool;

    pub(crate) fn fetchable_partitions(
        &self,
        is_available: impl Fn(&TopicPartition) -> bool,
    ) -> Vec<TopicPartition>;

    pub(crate) fn update_high_watermark(&mut self, tp: &TopicPartition, hw: i64) -> Result<(), KafkaError>;
    pub(crate) fn try_updating_high_watermark(&mut self, tp: &TopicPartition, hw: i64) -> bool;
    pub(crate) fn try_updating_log_start_offset(&mut self, tp: &TopicPartition, lso: i64) -> bool;
    pub(crate) fn update_last_stable_offset(&mut self, tp: &TopicPartition, lso: i64) -> Result<(), KafkaError>;
    pub(crate) fn try_updating_last_stable_offset(&mut self, tp: &TopicPartition, lso: i64) -> bool;

    pub(crate) fn set_next_allowed_retry(&mut self, partitions: &HashSet<TopicPartition>, next_ms: i64);
    pub(crate) fn request_failed(&mut self, partitions: &HashSet<TopicPartition>, next_retry_ms: i64);

    pub(crate) fn move_partition_to_end(&mut self, tp: &TopicPartition);

    pub(crate) fn metadata_topics(&self) -> HashSet<String>;
    pub(crate) fn needs_metadata(&self, topic: &str) -> bool;
    pub(crate) fn group_subscribe(&mut self, topics: &[String]) -> Result<bool, KafkaError>;
    pub(crate) fn reset_group_subscription(&mut self);

    pub(crate) fn is_fetchable(&self, tp: &TopicPartition) -> bool;

    pub(crate) fn pretty_string(&self) -> String;
}
```

Notes for the Actor / Critic:

- **No internal `Mutex`** (`consumer-threading.md` §16): every method is
  plain `&self` / `&mut self`. The Java `synchronized` keyword translates
  to "expect the caller to hold the outer `Arc<Mutex<...>>`". The
  borrow-checker enforces single-writer/multi-reader at compile time,
  matching what `synchronized` enforces dynamically in Java.

- **`assigned_state` helper**: Java has private `assignedState(tp)` that
  panics (`IllegalStateException`) if `tp` is not assigned, and private
  `assignedStateOrNull(tp)` that returns `Optional<T>`. In Rust, both are
  internal helpers — `assigned_state(&self, &TopicPartition) ->
  Result<&TopicPartitionState, KafkaError>` returning
  `KafkaError::illegal_state(...)` on missing key, and
  `assigned_state_or_null(&self, &TopicPartition) ->
  Option<&TopicPartitionState>`. The `Result`-returning variant exposes
  the error as `Result<...>` on the public methods that wrap it. Per
  CLAUDE.md §10 — *no* `panic!`; convert Java's
  `IllegalStateException` paths to `KafkaError`.

- **`assignment_id`**: u32, not Java's int. Wrapping addition is fine —
  Java does the same (signed int overflow, but never reached in
  practice). Document that the field is a sequence number, not arithmetic.

- **Rebalance listener storage**: `Option<Arc<dyn ConsumerRebalanceListener>>`
  per `consumer-threading.md` §31's "the listener is stored in
  `SubscriptionState` as `Arc<dyn ConsumerRebalanceListener>`" rule. NOT
  `Box<dyn>` — must be `Arc` so the listener can be cloned out of the
  lock before the caller awaits it on the app task.

- **`Option<AutoOffsetResetStrategy>` for `reset_strategy(partition)`**:
  Java returns nullable `AutoOffsetResetStrategy`. Rust returns
  `Option<AutoOffsetResetStrategy>`. The outer `Result<...>` wraps the
  not-assigned-error case.

- **`reset_initializing_positions`**: Java throws
  `NoOffsetForPartitionException(Set)` when the default reset strategy is
  `NONE` and some partitions lack offsets. Translate as
  `Err(KafkaError::no_offset_for_partitions(partitions))` (the
  consumer error variant already exists per `src/consumer/errors.rs:133`).

- **`Predicate<TopicPartition>` callbacks**: become `impl Fn(&TopicPartition) -> bool`.
  Avoid the `Box<dyn Fn>` allocation per call.

- **`LongSupplier`**: becomes `i64` (eager). The Java parameter is
  effectively "get current time ms" but always invoked exactly once at
  the same point; Rust just takes the value. Saves a closure-call layer.

- **Logger**: use `log::debug!`, `log::info!`, `log::warn!` directly
  matching `src/producer/internals/producer_metadata.rs` precedent. No
  `LogContext` translation — the macro-level `log` crate already attaches
  module path automatically.

- **`Pattern` (Java)** → **`regex::Regex` (Rust)**. The `regex` crate is
  already in `Cargo.toml` (workspace dep line 18) — no new dependency
  needed.

- **`SubscriptionType::AUTO_TOPICS_SHARE` and `subscribe_to_share_group`**:
  translated for state-machine completeness (used by `fetchable_partitions`
  filter and `has_auto_assigned_partitions`) but no Rust caller exists in
  the milestone. See "Out of scope" above. Tag with
  `#[allow(dead_code)]` if needed.

#### Inner types

All `pub(crate)` (private to module would be cleaner but the test file
needs `FetchPosition` constructors and `FetchStates` discriminants):

```rust
#[derive(Clone, PartialEq, Eq, Debug, Hash)]
pub(crate) struct FetchPosition {
    pub offset: i64,
    pub offset_epoch: Option<i32>,
    pub current_leader: LeaderAndEpoch,
}

impl FetchPosition {
    /// Java's package-private `FetchPosition(long)` ctor — offset only,
    /// no epoch, no leader. Used by `seek(tp, offset)`.
    pub(crate) fn new(offset: i64) -> Self;

    pub(crate) fn with_leader(
        offset: i64,
        offset_epoch: Option<i32>,
        current_leader: LeaderAndEpoch,
    ) -> Self;
}

pub(crate) struct LogTruncation {
    pub topic_partition: TopicPartition,
    pub fetch_position: FetchPosition,
    pub divergent_offset_opt: Option<OffsetAndMetadata>,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum SubscriptionType {
    None,
    AutoTopics,
    AutoPattern,
    AutoPatternRe2j,
    UserAssigned,
    AutoTopicsShare,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum FetchStates {
    Initializing,
    Fetching,
    AwaitReset,
    AwaitValidation,
}

impl FetchStates {
    fn valid_transitions(&self) -> &'static [FetchStates];
    fn requires_position(&self) -> bool;
    fn has_valid_position(&self) -> bool;
    fn transition_to(self, new_state: FetchStates) -> FetchStates;
}

struct TopicPartitionState {
    fetch_state: FetchStates,
    position: Option<FetchPosition>,
    high_watermark: Option<i64>,
    log_start_offset: Option<i64>,
    last_stable_offset: Option<i64>,
    paused: bool,
    pending_revocation: bool,
    pending_on_assigned_callback: bool,
    reset_strategy: Option<AutoOffsetResetStrategy>,
    next_retry_time_ms: Option<i64>,
    preferred_read_replica: Option<i32>,
    preferred_read_replica_expire_time_ms: Option<i64>,
    end_offset_requested: bool,
}
```

Notes:

- **`FetchState` trait vs `FetchStates` enum**: Java separates them so
  individual enum constants can override `validTransitions()`. Rust enums
  can't have per-variant method overrides without per-variant methods on
  `Self`. Collapse to a single enum `FetchStates` with `match`-based
  transition tables — semantically identical, simpler to reason about,
  and the `SubscriptionStateTest` cases only exercise the public surface.
  Drop the public `FetchState` trait (would be three-line match).

- **`SubscriptionType` and `FetchStates` are private** to the module.
  Tests that need to assert state transitions exercise them through
  public methods (`awaiting_validation`, `is_offset_reset_needed`,
  `has_valid_position`, etc.) — they do not match on enum variants
  directly. Match the Java test pattern.

- **`TopicPartitionState` is private**. Tests reach it via
  `SubscriptionState` methods only.

- **`transitionState` runIfTransitioned closure** (Java line 1040): in
  Rust, inline the body at every call site (only 5 callers:
  `reset`, `seekValidated`, `seekUnvalidated`'s outer call, `validatePosition`,
  `updatePositionLeaderNoValidation`, `completeValidation`). Per CLAUDE.md
  §11 "Outside hot paths, prefer the simpler type" — and inlining 5
  call-sites is simpler than a closure parameter.

### `ConsumerMetadata` (`src/consumer/internals/consumer_metadata.rs`)

`pub(crate)`. Translates `org.apache.kafka.clients.consumer.internals.ConsumerMetadata`.

Java `extends Metadata`. Rust uses composition over `Arc<Metadata>` +
`MetadataOverrides`, exactly like `ProducerMetadata`
(`src/producer/internals/producer_metadata.rs`). The Critic should diff
the construction shape against `ProducerMetadata` for consistency.

```rust
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use crate::common::internals::ClusterResourceListeners;
use crate::common::requests::MetadataRequestBuilder;
use crate::common::Uuid;
use crate::consumer::ConsumerConfig;
use crate::consumer::internals::subscription_state::SubscriptionState;
use crate::metadata::{Metadata, MetadataOverrides};

struct ConsumerMetadataInner {
    transient_topics: HashSet<String>,
}

pub(crate) struct ConsumerMetadata {
    metadata: Arc<Metadata>,
    inner: Arc<Mutex<ConsumerMetadataInner>>,
    allow_auto_topic_creation: bool,
}

impl ConsumerMetadata {
    pub(crate) fn new(
        refresh_backoff_ms: i64,
        refresh_backoff_max_ms: i64,
        metadata_expire_ms: i64,
        include_internal_topics: bool,
        allow_auto_topic_creation: bool,
        subscription: Arc<Mutex<SubscriptionState>>,
        cluster_resource_listeners: ClusterResourceListeners,
    ) -> Self;

    pub(crate) fn from_config(
        config: &ConsumerConfig,
        subscription: Arc<Mutex<SubscriptionState>>,
        cluster_resource_listeners: ClusterResourceListeners,
    ) -> Self;

    pub(crate) fn allow_auto_topic_creation(&self) -> bool;

    pub(crate) fn add_transient_topics(&self, topics: HashSet<String>);
    pub(crate) fn clear_transient_topics(&self);

    pub(crate) fn metadata_arc(&self) -> Arc<Metadata>;
}

impl Deref for ConsumerMetadata { type Target = Metadata; ... }
```

Override hooks installed at construction:

| Java override | Rust hook | Closure body |
|---|---|---|
| `retainTopic(topic, isInternal, nowMs)` | `retain_topic_fn` | Lock subscription, check pattern, internal-topics flag, transient topics. |
| `retainTopic(topic, topicId, ...)` | — currently no `MetadataOverrides` field for the topic-id variant | **Action item:** add `retain_topic_with_id_fn` to `MetadataOverrides` *if* the producer didn't need it. Verify by reading `src/metadata.rs` `retain_topic_default` call sites. If `Metadata` only calls the topic-name variant in Rust today, leave it — translate by detecting topic-id at `MetadataResponse` parse time and routing through `retain_topic_fn`. Document the discrepancy in the plan and confirm with `MetadataResponse::topic_metadata` users. |
| `newMetadataRequestBuilder()` | `request_builder_fn` | Lock subscription, decide between `all_topics`, `for_topic_ids(assigned_topic_ids)`, `for_topic_names(metadata_topics + transient)`. |

Notes:

- **`retainTopic` two-arg variant**: Java has both `retainTopic(topic, isInternal, nowMs)`
  (line 119) and `retainTopic(topicName, topicId, isInternal, nowMs)`
  (line 140). The latter checks `isAssignedFromRe2j(topicId)` after the
  former. In Rust, `Metadata::retain_topic_fn`'s signature is
  `Box<dyn Fn(&str, bool, i64) -> bool>` — no `Uuid` parameter. Check
  whether `Metadata.update(...)` flows the topic ID through to retain.
  **Two paths:**
  1. Extend `MetadataOverrides` with an optional second variant taking
     `(topic, topic_id, is_internal, now_ms)` — touches Phase 2 producer
     scope. Risky.
  2. Inside the `retain_topic_fn` closure, perform the topic-id check
     via a side-channel — but the closure doesn't know the topic ID
     because `Metadata` doesn't pass it.

  **Decision for the Actor:** read `src/metadata.rs` Metadata::update
  carefully, determine which retain-path is invoked. If the two-arg
  variant is plumbed through `MetadataResponse::topic_metadata` →
  `Metadata::retain_topic`, extend `MetadataOverrides`. If not, the
  topic-id check is only reachable via `MetadataResponseTopic.topicId` in
  the `update()` flow itself; in that case, the `retain_topic_fn` can
  read `assigned_topic_ids()` and *combined-with* the topic-name check
  conservatively (false positives are acceptable since the name-based
  check already retains all subscribed topics). **Critic confirms the
  chosen path matches Java's semantics on the
  `testSubscriptionToBrokerRegexRetainsAssignedTopics` test.**

- **No `extends Metadata`**: `Deref<Target = Metadata>` impl gives the
  `metadata.foo()` ergonomics. Matches `ProducerMetadata`.

- **`subscription: Arc<Mutex<SubscriptionState>>`**: the consumer holds
  the outer `Arc<Mutex<SubscriptionState>>` and *clones the Arc* into
  `ConsumerMetadata::new` so both can see the same state. The override
  closures grab the lock on each call. Critic verifies no clone of
  `SubscriptionState`'s contents — only the `Arc`.

## Cross-cutting requirements

- **License header**: 14-line Apache 2.0 header on every new file
  (CLAUDE.md §7).
- **No new dependencies** without explicit user approval. `regex` is
  already a workspace dep. `indexmap` — check `Cargo.toml`; if not
  already a dep, the Actor pauses and asks.
- **No `panic!` / `unimplemented!()` / `todo!()`** anywhere in production
  code (CLAUDE.md §5, §10). All Java `IllegalStateException` /
  `IllegalArgumentException` sites translate to
  `Err(KafkaError::illegal_state(msg))` or
  `Err(KafkaError::illegal_argument(msg))`. Confirm both error
  constructors exist on `KafkaError`; if not, add them in a separate
  prep commit.
- **Method receivers** (Critic checklist):
  | Java method | Rust receiver |
  |---|---|
  | mutates state (Java: synchronized + mutates field) | `&mut self` |
  | reads only (Java: synchronized + returns field) | `&self` |
  | mutates per-partition state on an internal `&mut TopicPartitionState` | wrapping method is `&mut self` |
- **Listener `Arc<dyn>`**: not `Box<dyn>`. Required by
  `consumer-threading.md` §31 — listener must be cloneable out of the
  lock.
- **Logger**: `log::*` macros directly. No `LogContext` translation.

## Verification

1. `cargo build` — clean
2. `cargo test` — all Phase 1, 2, 4 tests pass
3. `cargo xtask format-check` — clean
4. `cargo xtask lint` — clean
5. `cargo doc --no-deps` — builds
6. Specific test coverage (`cargo test --test main subscription_state`):
   - All `SubscriptionStateTest` cases except the four `maybe*Validation*`
     cases deferred to Phase 7 (rationale documented in code comment on
     the test module).
   - `PartitionStatesTest` (5 cases).
   - `ConsumerMetadataTest` (10 cases).

## Commit plan

Suggested commit granularity (one commit per logical bundle):

1. `Phase 4 (1/N): PartitionStates<S> + PartitionStatesTest in common::internals`
2. `Phase 4 (2/N): SubscriptionState inner types (FetchPosition, FetchStates, TopicPartitionState, LogTruncation)`
3. `Phase 4 (3/N): SubscriptionState subscription / assignment surface`
4. `Phase 4 (4/N): SubscriptionState position / offset-reset / validation surface (sans Phase-7-deferred methods)`
5. `Phase 4 (5/N): SubscriptionState pause/resume/fetchable/preferred-replica/lag accessors`
6. `Phase 4 (6/N): ConsumerMetadata over Arc<Metadata> + MetadataOverrides`
7. `Phase 4 (7/N): SubscriptionStateTest translation (skip Phase-7-deferred cases with rationale)`
8. `Phase 4 (8/N): ConsumerMetadataTest translation`

Each commit must individually pass `cargo build`. Adjust the split if a
logical unit ends up too small or too large.

## Workflow

Per `.claude/rules/agent-roles.md`:

1. Actor 1 implements per this plan, commits incrementally, runs the full
   verification matrix before declaring done.
2. Critic 1 reviews commits, writes findings into
   `design/history/Milestone-8/Phase-4/COMMENTS.1.md`.
3. Actor 1 fixes comments, moves resolved items to
   `COMMENTS.DONE.1.md`, commits with `fixup!` messages.
4. Repeat 2-3 until COMMENTS.1.md is empty.
