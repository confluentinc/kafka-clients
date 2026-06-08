# Phase 3: `MockConsumer<K, V>`

## Goal

Translate the user-facing test helper `org.apache.kafka.clients.consumer.MockConsumer`
and its test suite. This phase produces no broker-facing behavior — it is a
pure in-memory shell around the `SubscriptionState` machine landed in
Phase 4, with hooks tests use to drive scenarios (`addRecord`,
`schedulePollTask`, `rebalance`, `updateBeginningOffsets`, etc.).

After this phase lands, Rust tests can construct
`MockConsumer<K, V>` directly and pass it as `&mut dyn Consumer<K, V>` to
code-under-test — the same shape Java tests use (consumer-threading.md §2).

## Branch

`consumer-impl`. All commits land here.

## Why Phase 3 came after Phase 4 (departure from `Milestone-8/PLAN.md`)

The milestone table at `Milestone-8/PLAN.md:60` lists Phase 3 as depending
on Phases 1–2 only. That is wrong as written — `MockConsumer.java` imports
`SubscriptionState` on line 21, holds it as a field on line 63, and
delegates virtually every method to `this.subscriptions.subscribe/...`.
The Phase 4 plan landed `SubscriptionState` (`535cac1..b601797`); Phase 3
now picks up the unblocked work.

## Java sources

All paths relative to `kafka/clients/src/main/java/` (`a18251bae0b825c69794a50dffd4c3100cf5ca5b`).

Production:

- `org/apache/kafka/clients/consumer/MockConsumer.java` (721)

Tests (translate each — DoD §3):

- `clients/consumer/MockConsumerTest.java` (233) →
  `tests/consumer/mock_consumer_test.rs` (or inline `#[cfg(test)] mod
  tests` if the Phase-4 precedent applies — see "Test location" below)

## Out of scope (deferred / dropped from this phase)

- **`MockConsumer(OffsetResetStrategy)` deprecated constructor** (Java
  line 91). Java retains it `@Deprecated` for source compatibility; Rust
  drops it per CLAUDE.md §5 (don't carry dead variants).
- **`MockConsumer(String)` constructor**: drop in favor of
  `MockConsumer::new(AutoOffsetResetStrategy)`. Tests already need an
  `AutoOffsetResetStrategy` value (Phase 1's `from_string` does the
  parse), so taking the typed enum is cleaner and avoids a string-parse
  failure surface inside the constructor.
- **`registerMetricForSubscription` / `unregisterMetricFromSubscription`
  / `metrics()` / `clientInstanceId(Duration)` / `disableTelemetry()` /
  `addedMetrics()`** — Java methods touch `KafkaMetric` and telemetry
  state that have no Rust analog in this codebase. The Phase-2 `Consumer`
  trait deliberately omits these (`consumer-threading.md` §1). MockConsumer
  drops them too. `setClientInstanceId` / `injectTimeoutException` stay
  as no-ops (or get dropped) — they only matter for the
  `clientInstanceId` accessor, which we don't ship.
- **`scheduleNopPollTask()`**: drop. Java provides it as a one-line
  convenience over `schedulePollTask(() -> {})`. Rust callers can write
  the equivalent inline; carrying the convenience method adds nothing.
- **`offsetsForTimes()`**: matches Java by returning
  `Err(KafkaError::unsupported_version("MockConsumer::offsets_for_times
  is not implemented"))`. Java throws `UnsupportedOperationException`;
  the Rust analog is `KafkaError::unsupported_version` (already the
  pattern used elsewhere in the codebase).

## Test location

Phase 4 placed all unit tests inline as `#[cfg(test)] mod tests` because
the tested types are `pub(crate)`. `MockConsumer<K, V>` is **public**
(it has to be — users import it for testing), so its tests *can* live in
`tests/consumer/mock_consumer_test.rs` and exercise the public surface.

**Decision**: put the `MockConsumerTest` translation in
`tests/consumer/mock_consumer_test.rs` (NOT inline), following the
Phase-1 precedent for public types (`consumer_record_test.rs`,
`consumer_records_test.rs`, etc.). The Actor adds the new file to
`tests/consumer/main.rs`. If the Actor finds a test that genuinely needs
a private constructor / internal field, fall back to inline — but the
8 `MockConsumerTest` cases use only the public API.

## Module structure produced by this phase

```
src/consumer/
└── mock_consumer.rs                 # NEW: MockConsumer<K, V>

tests/consumer/
└── mock_consumer_test.rs            # NEW: 8 test cases from MockConsumerTest.java
```

`src/consumer/mod.rs` gets `pub mod mock_consumer;` and a `pub use
mock_consumer::MockConsumer;` re-export — `MockConsumer` lives at
`crate::consumer::MockConsumer` per CLAUDE.md §2.

`tests/consumer/main.rs` gets `mod mock_consumer_test;`.

## Type-by-type spec

### `MockConsumer<K, V>` (`src/consumer/mock_consumer.rs`)

`pub`. Implements `crate::consumer::Consumer<K, V>`. Mirrors the Java
class field-for-field; only the obvious naming and types change.

#### Fields

```rust
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crate::common::{KafkaError, PartitionInfo, TopicPartition, Uuid};
use crate::consumer::internals::{AutoOffsetResetStrategy, SubscriptionState};
use crate::consumer::{
    ConsumerGroupMetadata, ConsumerRebalanceListener, ConsumerRecord,
    ConsumerRecords, OffsetAndMetadata, OffsetCommitCallback, SubscriptionPattern,
};

pub struct MockConsumer<K, V> {
    partitions: HashMap<String, Vec<PartitionInfo>>,
    subscriptions: SubscriptionState,
    beginning_offsets: HashMap<TopicPartition, i64>,
    end_offsets: HashMap<TopicPartition, i64>,
    duration_reset_offsets: HashMap<TopicPartition, i64>,
    committed: HashMap<TopicPartition, OffsetAndMetadata>,
    poll_tasks: VecDeque<Box<dyn FnOnce(&mut MockConsumer<K, V>) + Send>>,
    paused: HashSet<TopicPartition>,
    wakeup: AtomicBool,

    records: HashMap<TopicPartition, Vec<ConsumerRecord<K, V>>>,

    poll_exception: Option<KafkaError>,
    offsets_exception: Option<KafkaError>,
    last_poll_timeout: Option<Duration>,
    closed: bool,
    should_rebalance: bool,
    max_poll_records: i64, // Java: long, sentinel Long.MAX_VALUE
}
```

Notes:

- **`subscriptions: SubscriptionState`** (plain, NOT `Arc<Mutex<...>>`).
  `consumer-threading.md` §16 requires `Arc<Mutex<>>` only when state is
  shared across tasks. `MockConsumer` is single-task by design (the
  `Consumer` trait API is `&mut self` — only one task can call methods
  at a time per Phase-2 §1). A plain `SubscriptionState` field satisfies
  the trait's `&self` accessors via the borrow checker. The Critic should
  verify there is no place where MockConsumer hands a `&SubscriptionState`
  reference out to a spawned task.

- **`wakeup: AtomicBool`**. Java's `AtomicBoolean`. Translate as
  `AtomicBool` even though MockConsumer is single-task — `Consumer::wakeup(&self)`
  is `&self` (sync, callable from any task per `consumer-threading.md`
  §11). Rust's borrow checker lets `&self` access `AtomicBool` without
  `&mut self`, so the field needs interior-mutable atomicity even if no
  cross-task call ever materializes. `Ordering::SeqCst` for the set/get
  pair — wakeup is rare, cost is negligible, and `SeqCst` removes any
  reasoning about reordering.

- **`max_poll_records: i64`**, default `i64::MAX`. Java uses
  `Long.MAX_VALUE` as a "no cap" sentinel; the same in Rust.

- **`poll_tasks`** stores `Box<dyn FnOnce(&mut MockConsumer<K, V>) + Send>`,
  NOT Java's parameterless `Runnable`. Rationale: Java's `Runnable`
  captures the outer `MockConsumer` reference, which Rust closures
  cannot do safely without `Arc<Mutex<...>>` wrapping. Passing `&mut
  self` explicitly is the idiomatic Rust translation — tests write
  `consumer.schedule_poll_task(Box::new(|c| c.add_record(...)));`. This
  is a deviation from Java's signature; document it in rustdoc with a
  one-line explanation.

- **`telemetry_disabled`, `client_instance_id`,
  `inject_timeout_exception_counter`, `added_metrics`** — dropped per
  "Out of scope" above.

- **No `LogContext`** — Phase 4 already established that `log::*`
  macros are the translation for Java's `LoggerFactory`. MockConsumer's
  Java code calls `log.info("Seeking to {} offset of partition {}", ...)`
  from `requestOffsetReset` (indirect via `SubscriptionState`); the Rust
  side already logs via the `log` crate inside `SubscriptionState`.

#### Constructor

```rust
impl<K, V> MockConsumer<K, V> {
    pub fn new(offset_reset_strategy: AutoOffsetResetStrategy) -> Self;
}
```

One constructor. Drop the Java string-based variant — callers parse via
`AutoOffsetResetStrategy::from_string("by_duration:PT1H")?` if they want
to. Drop the deprecated `OffsetResetStrategy` enum variant.

#### MockConsumer-specific methods

These are NOT on the `Consumer<K, V>` trait — they live on the concrete
`MockConsumer<K, V>` type. Tests hold `MockConsumer` directly and call
them; production code that takes `&mut dyn Consumer<K, V>` never sees
them.

```rust
impl<K, V> MockConsumer<K, V> {
    // Driver methods — populate state from outside

    /// Java: `addRecord(ConsumerRecord<K, V> record)`. Java line 322.
    /// Throws `IllegalStateException` if the partition is not assigned.
    pub fn add_record(&mut self, record: ConsumerRecord<K, V>) -> Result<(), KafkaError>;

    pub fn update_beginning_offsets(&mut self, offsets: HashMap<TopicPartition, i64>);
    pub fn update_end_offsets(&mut self, offsets: HashMap<TopicPartition, i64>);
    pub fn update_duration_offsets(&mut self, offsets: HashMap<TopicPartition, i64>);
    pub fn update_partitions(&mut self, topic: &str, partitions: Vec<PartitionInfo>) -> Result<(), KafkaError>;

    // Exception injection
    pub fn set_poll_exception(&mut self, exception: KafkaError);
    pub fn set_offsets_exception(&mut self, exception: KafkaError);

    // Configuration knobs
    pub fn set_max_poll_records(&mut self, max_poll_records: i64) -> Result<(), KafkaError>;

    // Rebalance simulation — invokes the rebalance listener
    pub async fn rebalance(&mut self, new_assignment: &[TopicPartition]) -> Result<(), KafkaError>;

    // Poll scripting
    pub fn schedule_poll_task(
        &mut self,
        task: Box<dyn FnOnce(&mut MockConsumer<K, V>) + Send>,
    );

    // State accessors for tests
    pub fn closed(&self) -> bool;
    pub fn should_rebalance(&self) -> bool;
    pub fn reset_should_rebalance(&mut self);
    pub fn last_poll_timeout(&self) -> Option<Duration>;
}
```

Notes:

- **`rebalance` is `async`** because invoking
  `ConsumerRebalanceListener::on_partitions_revoked` /
  `on_partitions_assigned` is `async` per Phase 2. Java's `rebalance` is
  sync because Java's listener methods are sync. Tests use
  `#[tokio::test]` (already the project convention).

- **`set_max_poll_records` returns `Result`** because Java throws
  `IllegalArgumentException` on `< 1`. Translate to
  `Err(KafkaError::illegal_argument("MaxPollRecords must be strictly superior to 0"))`.

- **`add_record` returns `Result`** because Java throws
  `IllegalStateException` if the partition is unassigned. Translate to
  `Err(KafkaError::illegal_state(...))`.

- **`update_partitions` returns `Result`** for `ensureNotClosed` check —
  Java throws `IllegalStateException("This consumer has already been closed")`.

- Helper methods that don't have a Java "not closed" check (e.g.
  `update_beginning_offsets`, `set_poll_exception`) take `&mut self`
  with no return value — they don't error.

#### `Consumer<K, V>` trait impl

Every method on the trait maps to a (Java method, optional MockConsumer
quirk) pair. Key implementations:

- **`poll(&mut self, timeout: Duration) -> Result<ConsumerRecords<K, V>, KafkaError>`**:
  the centerpiece. Mirrors Java's `poll(Duration)` line 249:
  1. `ensureNotClosed` → `Err(KafkaError::illegal_state(...))`.
  2. `last_poll_timeout = Some(timeout)`.
  3. Pop one queued poll task off `poll_tasks` and run it with `&mut self`.
     (Java pops one; we match.)
  4. Check `wakeup.compare_exchange(true, false, ...)`; if true, return
     `Err(KafkaError::wakeup(...))`. The wakeup error constructor must
     exist or be added (mirror the producer side).
  5. Take `poll_exception` (Option::take) — if `Some`, return it.
  6. For each assigned partition without a valid position, call
     `update_fetch_position(tp)` (private helper, mirrors Java
     `updateFetchPosition` lines 622-631).
  7. Drain `records` into the result map up to `max_poll_records`,
     skipping paused partitions, validating beginning-offset bounds
     (`OffsetOutOfRangeException` if record offset is below beginning
     offset), advancing each partition's `SubscriptionState::set_position`,
     building `nextOffsetAndMetadata` for the returned `ConsumerRecords`.
  8. Return `Ok(ConsumerRecords::new(results, next_offsets))`.

  **Key behavior** to preserve: Java's poll mutates `records` in place
  via iterator removal. Rust's `HashMap` can't be mutated mid-iteration;
  use `drain_filter`-style pattern or rebuild a copy with retained
  values.

- **`subscribe(&mut self, topics: Vec<String>)`**: clears `committed`,
  delegates to `self.subscriptions.subscribe_topics(topics.into_iter().collect(), None)`.
  Java line 196-200.

- **`subscribe_with_listener(&mut self, topics, listener)`**: Java line
  189-194 — null check (in Rust: the `Arc<dyn>` is non-nullable by
  type, so the null-check tests in `MockConsumerTest` cannot trigger
  IllegalArgumentException for null listener — see "Test translation
  notes" below).

- **`subscribe_pattern(&mut self, pattern: SubscriptionPattern)` /
  `subscribe_pattern_with_listener(...)`**: Java has both `Pattern`
  (java.util.regex) overloads and `SubscriptionPattern` overloads. The
  Rust Consumer trait only has `SubscriptionPattern`-based methods (RE2J
  is the future). Translate the `SubscriptionPattern` arm. Validate
  pattern is non-empty per Java line 181.

  Java pattern (java.util.regex) overload is dropped — covered by the
  `subscribe_pattern` `SubscriptionPattern` arm in Rust trait.

- **`assign(&mut self, partitions: Vec<TopicPartition>)`**: clears
  `committed`, delegates. Java line 235.

- **`unsubscribe(&mut self)`**: clears `committed`, delegates. Java line
  242.

- **`commit_sync(&mut self)`** / `commit_sync_offsets(...)` /
  `commit_async(...)`: all call into a private `commit_async_impl` that
  merges into `self.committed` and invokes the callback if present.
  Java line 353-380. Note the callback is `async fn on_complete(...)` in
  Rust — `commit_async_with_callback` must `.await` the callback
  invocation before returning, mirroring Java's synchronous callback
  invocation.

- **`seek(&mut self, partition: TopicPartition, offset: i64)`**:
  delegates to `self.subscriptions.seek(&partition, offset)`. Java line
  393.

- **`seek_with_metadata(&mut self, partition, offset_and_metadata)`**:
  Java line 398 — delegates to `subscriptions.seek(&partition, offset_and_metadata.offset())`.

- **`committed(&mut self, partitions)`**: Java line 405 — filter
  `committed` map by passed partitions, mapping unassigned partitions to
  `OffsetAndMetadata::new(0)` (Java's odd behavior; preserve it).

- **`position(&mut self, partition: &TopicPartition) -> Result<i64, KafkaError>`**:
  Java line 420 — illegal_argument if not assigned; otherwise call
  `subscriptions.position(partition)`; if `None`, call
  `update_fetch_position(partition)` and re-read.

- **`seek_to_beginning(&mut self, partitions)` / `seek_to_end(...)`**:
  delegate to
  `self.subscriptions.request_offset_reset_all(partitions, AutoOffsetResetStrategy::EARLIEST/LATEST)`.
  Java lines 438, 448.

- **`partitions_for(&mut self, topic: &str)`**: returns
  `self.partitions.get(topic).cloned().unwrap_or_default()`. Java line 502.

- **`list_topics(&mut self)`**: returns a clone of `self.partitions`.
  Java line 508.

- **`pause(&mut self, partitions)` / `resume(...)`**: iterate, call
  `subscriptions.pause(tp)` / `resume(tp)`, mirror `paused: HashSet`
  bookkeeping. Java lines 519, 527.

- **`beginning_offsets(&mut self, partitions)`** / `end_offsets(...)`:
  Java lines 540, 557. Take `offsets_exception` first (mirrors Java
  exception throwing); if every partition is present in
  `beginning_offsets`/`end_offsets`, return their mapped values; else
  `Err(KafkaError::illegal_state("The partition X does not have a
  beginning offset"))`.

- **`offsets_for_times(&mut self, ...)`**: Return
  `Err(KafkaError::unsupported_version("MockConsumer::offsets_for_times
  is not implemented"))`. Java line 535-537 throws
  `UnsupportedOperationException`.

- **`close(&mut self)` / `close_with_options(&mut self, ...)`**: set
  `self.closed = true`. Java lines 574, 594. No async work needed in
  the mock.

- **All `*_timeout` variants**: drop the timeout, delegate to the
  no-timeout version. Java does the same (see `commitSync(Duration)`
  line 383, `position(TopicPartition, Duration)` line 433, etc.).

- **`wakeup(&self)`**: `self.wakeup.store(true, Ordering::SeqCst)`. Java
  line 589.

- **`assignment(&self)`**: `self.subscriptions.assigned_partitions()`.

- **`subscription(&self)`**: `self.subscriptions.subscription()`.

- **`paused(&self)`**: clone `self.paused`.

- **`group_metadata(&self)`**: returns
  `ConsumerGroupMetadata::new("dummy.group.id", 1, "1", None)`. Match
  Java line 692-693.

- **`client_id(&self) -> &str`**: hardcoded `"mock-consumer"` (or
  `""`). Java has no equivalent on MockConsumer; the trait method
  requires a value. Pick a constant `"mock-consumer"`. Rustdoc says
  "Mock consumers use a fixed client id."

- **`current_lag(&self, tp) -> Option<i64>`**: Java line 681 — return
  `end_offsets.get(tp) - position(tp)` if end offset known, else
  `Some(0)` (model "caught up"). Use `self.subscriptions.position(tp)`
  for the read.

- **`enforce_rebalance(&mut self, reason: Option<&str>)`**: set
  `should_rebalance = true`. Match Java line 702.

#### Private helpers

```rust
impl<K, V> MockConsumer<K, V> {
    fn ensure_not_closed(&self) -> Result<(), KafkaError>;
    fn update_fetch_position(&mut self, tp: &TopicPartition) -> Result<(), KafkaError>;
    fn reset_offset_position(&mut self, tp: &TopicPartition) -> Result<(), KafkaError>;
}
```

`update_fetch_position` and `reset_offset_position` mirror Java lines
622-651 — handle `SubscriptionState::is_offset_reset_needed`, missing
committed offset, and the strategy switch (EARLIEST → `beginning_offsets`,
LATEST → `end_offsets`, BY_DURATION → `duration_reset_offsets`, NONE →
`Err(KafkaError::no_offset_for_partition(tp))`).

## Test translation notes

8 cases from `MockConsumerTest.java` (lines 47-231). Each must be
translated unless deferred with rationale per DoD §3.

1. **`testSimpleMock`** (line 46) — direct translation. Use
   `#[tokio::test]`. Build `ConsumerRecord` via the existing public
   constructor (Phase 1). Note the `recs.next_offsets()` assertion —
   Phase 1's `ConsumerRecords` has this method.

2. **`testConsumerRecordsIsEmptyWhenReturningNoRecords`** (line 76) —
   uses `assign(Collections.singleton(partition))` + `addRecord` +
   `updateEndOffsets` + `seekToEnd` + `poll`. Translate as-is.

3. **`shouldNotClearRecordsForPausedPartitions`** (line 88) — pause /
   poll / resume / poll cycle. Translate as-is.

4. **`endOffsetsShouldBeIdempotent`** (line 106) — direct translation.

5. **`testDurationBasedOffsetReset`** (line 121) — uses
   `MockConsumer::new(AutoOffsetResetStrategy::from_string("by_duration:PT1H")?)`
   ctor variant. Translate as-is.

6. **`testRebalanceListener`** (line 143) — the trickiest. Java uses
   `final List<TopicPartition> revoked = new ArrayList<>();` captured
   by an inline `ConsumerRebalanceListener`. In Rust, the listener
   methods are `async` (per Phase 2), so the test needs a listener
   struct holding `Arc<Mutex<Vec<TopicPartition>>>` fields and an
   `#[async_trait]` impl. Pattern:

   ```rust
   struct RecorderListener {
       revoked: Arc<Mutex<Vec<TopicPartition>>>,
       assigned: Arc<Mutex<Vec<TopicPartition>>>,
   }
   #[async_trait]
   impl ConsumerRebalanceListener for RecorderListener {
       async fn on_partitions_revoked(&self, partitions: &[TopicPartition])
           -> Result<(), KafkaError>
       {
           let mut g = self.revoked.lock().unwrap();
           g.clear();
           g.extend_from_slice(partitions);
           Ok(())
       }
       async fn on_partitions_assigned(&self, partitions: &[TopicPartition])
           -> Result<(), KafkaError>
       {
           if partitions.is_empty() { return Ok(()); }
           let mut g = self.assigned.lock().unwrap();
           g.clear();
           g.extend_from_slice(partitions);
           Ok(())
       }
   }
   ```

   `consumer.rebalance(...).await` is the call that triggers listener
   invocation (matching Java's `mockConsumer.rebalance(...)` line 168).
   Test asserts on the `Arc<Mutex<Vec>>` contents after each rebalance.

7. **`testRe2JPatternSubscription`** (line 191) — the three Java
   `assertThrows(IllegalArgumentException.class, ...)` calls:
   - `consumer.subscribe((SubscriptionPattern) null)` — in Rust the
     `subscribe_pattern` parameter is `SubscriptionPattern` by value,
     not `Option`. **A null-pattern call is not expressible in Rust** —
     the type system rules it out. **Drop this assertion**, with a
     one-line rationale comment. The behavior (Java throws) is replaced
     by "the compiler refuses".
   - `consumer.subscribe(new SubscriptionPattern(""))` — empty-string
     pattern. Must still error: `Err(KafkaError::illegal_argument(...))`.
   - `consumer.subscribe(pattern, null)` — null listener. Rust's
     `subscribe_pattern_with_listener(pattern, listener: Arc<dyn
     ConsumerRebalanceListener>)` is non-null by type. **Drop this
     assertion** with rationale, same as the first.

   The last assertion (`assertThrows(IllegalStateException, () ->
   consumer.subscribe(List.of("topic1")))`) translates as
   `assert!(matches!(consumer.subscribe(vec!["topic1".to_string()]).await,
   Err(KafkaError::IllegalState { .. })))` — the mixed-subscription
   error.

   Translate the test as a single `#[tokio::test]` with the three null
   assertions removed (rationale: Rust type system rules them out at
   compile time; the behavioral contract is preserved by type
   non-nullability).

8. **`shouldReturnMaxPollRecords`** (line 207) — direct translation.

**`MockConsumerTest` is a single Java class with eight `@Test` methods;
no nested test classes, no parametrized tests, no `@RepeatedTest`. Each
method translates 1:1 (with the noted three null-input assertions
dropped from test 7).**

## Cross-cutting requirements

- **License header**: 14-line Apache 2.0 header on the new files
  (CLAUDE.md §7).
- **No new dependencies**. `async-trait`, `tokio`, `regex` are already
  in.
- **No `panic!` / `unimplemented!` / `todo!`** in production code
  (CLAUDE.md §5, §10). All Java unchecked exceptions translate to
  `Err(KafkaError::illegal_state(...))` / `Err(KafkaError::illegal_argument(...))`
  / `Err(KafkaError::unsupported_version(...))`. Phase 4's audit found
  these constructors exist on `KafkaError`. The `wakeup` error
  constructor — confirm before use; if missing, add it (mirror the
  producer wakeup path or add a new variant).
- **Listener `Arc<dyn>` not `Box<dyn>`** — Phase 2/4 invariant.
- **Trait surface check (DoD §11)** — `Box<dyn Consumer<K, V>>`
  upcast still works after MockConsumer lands. Add a one-line
  `MockConsumer` test that builds a `Box<dyn Consumer<String, String>>
  = Box::new(MockConsumer::new(...))` to keep the trait object-safe
  invariant tracked. (Phase 2 already has the `trait_surface_check.rs`
  for the trait alone; this adds a concrete impl-side check.)

## Verification

1. `cargo build` — clean
2. `cargo test --lib` — 1000-test baseline holds
3. `cargo test --test consumer` — picks up the new
   `mock_consumer_test.rs` cases; all 8 pass
4. `cargo xtask format-check` — clean
5. `cargo xtask lint` — clean
6. `cargo doc --no-deps` — builds (exercises rustdoc on the public
   `MockConsumer` type)

## Commit plan

Suggested commit granularity (3 logical bundles):

1. `Phase 3 (1/3): MockConsumer fields + constructor + state-mutation helpers`
2. `Phase 3 (2/3): MockConsumer Consumer<K, V> trait impl + poll loop`
3. `Phase 3 (3/3): MockConsumerTest translation (8 cases, 3 null-arg assertions dropped)`

Each commit must individually pass `cargo build`. Adjust the split if a
logical unit ends up too small or too large.

## Workflow

Per `.claude/rules/agent-roles.md`:

1. Actor 1 implements per this plan, commits incrementally, runs the
   verification matrix before declaring done.
2. Critic 1 reviews commits, writes findings to
   `design/history/Milestone-8/Phase-3/COMMENTS.1.md`.
3. Actor 1 fixes comments, moves resolved items to
   `COMMENTS.DONE.1.md`, commits with `fixup!` messages.
4. Repeat 2-3 until COMMENTS.1.md is empty.
