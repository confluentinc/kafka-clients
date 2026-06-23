# Phase 5: Event channels, `WakeupTrigger`, `CompletableEventReaper`

## Goal

Translate the event-passing layer the consumer uses between the app side
(`AsyncKafkaConsumer`) and the background task (`ConsumerNetworkThread`),
plus the cancellation primitive (`WakeupTrigger`) and the deadline
enforcement helper (`CompletableEventReaper`).

This phase ships **data + plumbing**, not behavior:

- The `ApplicationEvent` / `BackgroundEvent` enums (one Rust enum each
  collapses Java's abstract class + ~25 subclasses).
- The `ApplicationEventHandler` / `BackgroundEventHandler` channel
  wrappers (`mpsc` based) — minimal scope; the network-thread wiring
  belongs to Phase 10.
- The completable-event handle (`oneshot::Sender` + done flag) and
  `CompletableEventReaper`.
- `WakeupTrigger` — the rotating-`CancellationToken` primitive prescribed
  by `consumer-threading.md` §11 (NOT a mechanical translation of Java's
  `AtomicReference<Wakeupable>` state machine).

After this phase lands, Phase 6 (`NetworkClientDelegate`,
`CoordinatorRequestManager`, etc.) can `mpsc::Receiver::recv` events and
the eventual Phase 10 `ApplicationEventProcessor` can route them.

## Branch

`consumer-impl`. All commits land here.

## Java sources

All paths relative to `kafka/clients/src/main/java/`, at submodule commit
`a18251bae0b825c69794a50dffd4c3100cf5ca5b`.

Production:

- `org/apache/kafka/clients/consumer/internals/events/ApplicationEvent.java` (80) — abstract base + Type enum (see in-scope filter below)
- `org/apache/kafka/clients/consumer/internals/events/BackgroundEvent.java` (68) — abstract base + Type enum
- `org/apache/kafka/clients/consumer/internals/events/CompletableEvent.java` (127) — interface + static deadline helpers
- `org/apache/kafka/clients/consumer/internals/events/CompletableApplicationEvent.java` (55) — abstract base for completable app events
- `org/apache/kafka/clients/consumer/internals/events/CompletableBackgroundEvent.java` (55) — abstract base for completable bg events
- `org/apache/kafka/clients/consumer/internals/events/CompletableEventReaper.java` (211)
- `org/apache/kafka/clients/consumer/internals/events/ApplicationEventHandler.java` (158) — **channel surface only** (see "Out of scope" below)
- `org/apache/kafka/clients/consumer/internals/events/BackgroundEventHandler.java` (~)
- `org/apache/kafka/clients/consumer/internals/WakeupTrigger.java` (210)

Plus the in-scope event subtype files (production):

| Java file | Variant |
|---|---|
| `AssignmentChangeEvent.java` | `AssignmentChange` |
| `AsyncCommitEvent.java` | `CommitAsync` |
| `AsyncPollEvent.java` | `AsyncPoll` |
| `CheckAndUpdatePositionsEvent.java` | `CheckAndUpdatePositions` |
| `CommitEvent.java` (abstract; provides base for AsyncCommitEvent + SyncCommitEvent) | shared fields |
| `CommitOnCloseEvent.java` | `CommitOnClose` |
| `ConsumerRebalanceListenerCallbackCompletedEvent.java` | `ConsumerRebalanceListenerCallbackCompleted` |
| `CreateFetchRequestsEvent.java` | `CreateFetchRequests` |
| `CurrentLagEvent.java` | `CurrentLag` |
| `FetchCommittedOffsetsEvent.java` | `FetchCommittedOffsets` |
| `LeaveGroupOnCloseEvent.java` | `LeaveGroupOnClose` |
| `ListOffsetsEvent.java` | `ListOffsets` |
| `NewTopicsMetadataUpdateRequestEvent.java` (if present) or inline | `NewTopicsMetadataUpdate` |
| `PausePartitionsEvent.java` | `PausePartitions` |
| `ResetOffsetEvent.java` | `ResetOffset` |
| `ResumePartitionsEvent.java` | `ResumePartitions` |
| `SeekUnvalidatedEvent.java` | `SeekUnvalidated` |
| `StopFindCoordinatorOnCloseEvent.java` | `StopFindCoordinatorOnClose` |
| `SubscriptionChangeEvent.java` (abstract base) | shared fields |
| `SyncCommitEvent.java` | `CommitSync` |
| `TopicMetadataEvent.java` | `TopicMetadata` |
| `AllTopicsMetadataEvent.java` | `AllTopicsMetadata` |
| `AbstractTopicMetadataEvent.java` (abstract base) | shared fields |
| `TopicPatternSubscriptionChangeEvent.java` | `TopicPatternSubscriptionChange` |
| `TopicRe2JPatternSubscriptionChangeEvent.java` | `TopicRe2JPatternSubscriptionChange` |
| `TopicSubscriptionChangeEvent.java` | `TopicSubscriptionChange` |
| `UnsubscribeEvent.java` | `Unsubscribe` |
| `UpdatePatternSubscriptionEvent.java` | `UpdatePatternSubscription` |

BackgroundEvent subtypes (in scope):

| Java file | Variant |
|---|---|
| `ErrorEvent.java` | `Error` |
| `ConsumerRebalanceListenerCallbackNeededEvent.java` | `ConsumerRebalanceListenerCallbackNeeded` |

Plus the `MetadataErrorNotifiableEvent.java` interface (~marker) — verify
whether it's a marker trait or carries behavior; translate accordingly.

`EventProcessor.java` (41 LOC) is the generic processor interface;
translate as a sealed `pub(crate) trait EventProcessor` with `process`
method. Phase 10 implements it in `ApplicationEventProcessor`.

Tests (translate each — DoD §3):

- `clients/consumer/internals/WakeupTriggerTest.java` (227) →
  `tests/consumer/internals/wakeup_trigger_test.rs` (or inline if visibility forces).
  **Note**: most cases need rewriting against the rotating-token design;
  see "WakeupTrigger" section below for the test-rewrite policy.
- `clients/consumer/internals/events/CompletableEventReaperTest.java` (201) →
  `tests/consumer/internals/events/completable_event_reaper_test.rs`
- `clients/consumer/internals/BackgroundEventHandlerTest.java` (66) →
  `tests/consumer/internals/events/background_event_handler_test.rs`
- `clients/consumer/internals/ApplicationEventHandlerTest.java` (125) —
  see "Out of scope" below. Phase 5 ships the subset that exercises the
  channel only; the network-thread cases are deferred to Phase 10.

## Out of scope (deferred to later phases)

- **`ApplicationEventProcessor.java` (853 LOC)** — the dispatcher that
  pops events from the queue and routes to request managers. Phase 10
  per the milestone PLAN.md (rows 5 and 10).
- **`ApplicationEventHandler.java`'s `ConsumerNetworkThread` construction
  + lifecycle**. The Java class constructs and starts the network thread
  in its constructor (line 65-85). In Rust, the analog is a separate
  Phase-10 task. Phase 5 ships `ApplicationEventHandler` as a thin
  channel wrapper — `add(event)`, `add_and_get(event)`, `close()` —
  but the network thread, `RequestManagers` supplier, `NetworkClientDelegate`
  supplier, `AsyncConsumerMetrics`, and `IdempotentCloser` all defer.
  Translate the channel surface; leave a clear seam for Phase 10 to
  hand in the thread / processor.
- **`ApplicationEventHandlerTest.java`'s `maximumTimeToWait` and
  network-thread-coupled assertions** — defer to Phase 10. Translate
  only the cases that assert `add()` enqueues the event and
  `add_and_get()` completes the future.
- **All `Share*Event*` files (KIP-932)** per `consumer-threading.md` §20.
- **All `Streams*Event*` files** (Kafka Streams integration) — not in
  Milestone 8 scope.
- **`ShareAcknowledgementEventHandler.java`** — share-consumer only.
- **`AsyncConsumerMetrics` instrumentation** in `add()` — there is no
  Rust metrics framework yet. Drop the `recordApplicationEventQueueSize`
  call site; revisit when the metrics layer lands.
- **`InterruptException` propagation in `addAndGet`** — Java's
  `Thread.interrupted()` check has no idiomatic Rust analog (tokio tasks
  are not interrupted; they are cancelled via the wakeup token). Drop
  the check; cancellation flows through the wakeup token instead.

## Module structure produced by this phase

```
src/consumer/internals/events/
├── mod.rs                              # pub use re-exports
├── application_event.rs                # ApplicationEvent enum + completable wrapper
├── background_event.rs                 # BackgroundEvent enum + completable wrapper
├── completable_event.rs                # CompletableEventHandle trait + helpers
├── completable_event_reaper.rs         # CompletableEventReaper
├── application_event_handler.rs        # ApplicationEventHandler (channel only)
├── background_event_handler.rs         # BackgroundEventHandler
└── event_processor.rs                  # EventProcessor trait (Phase 10 impls)

src/consumer/internals/
└── wakeup_trigger.rs                   # NEW: WakeupTrigger over CancellationToken

tests/consumer/internals/
├── wakeup_trigger_test.rs              # rewrites of WakeupTriggerTest
└── events/
    ├── completable_event_reaper_test.rs
    ├── background_event_handler_test.rs
    └── application_event_handler_test.rs   # subset only
```

`src/consumer/internals/mod.rs` gets `pub(crate) mod events;` and
`pub(crate) mod wakeup_trigger;`. The Phase-2 precedent is to use
`pub(crate)` for `internals/` modules.

## Type-by-type spec

### `WakeupTrigger` (`src/consumer/internals/wakeup_trigger.rs`)

`pub(crate)`. **Designed per `consumer-threading.md` §11, NOT a mechanical
translation of Java's `AtomicReference<Wakeupable>` state machine.**

```rust
use std::sync::Arc;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::common::KafkaError;

pub(crate) struct WakeupTrigger {
    sender: watch::Sender<CancellationToken>,
}

impl WakeupTrigger {
    pub(crate) fn new() -> Self;

    /// Subscribe to the current cancellation token. The returned
    /// receiver yields the *current* token via `borrow()` and updates
    /// when `rotate()` is called.
    pub(crate) fn subscribe(&self) -> watch::Receiver<CancellationToken>;

    /// Read the current token without subscribing — used by sync
    /// `select!` paths that re-read on each loop iteration (matches
    /// `consumer-threading.md` §11 "bg task re-reads
    /// `wakeup_rx.borrow().clone()` at top of each `run_once`").
    pub(crate) fn current_token(&self) -> CancellationToken;

    /// Java's `wakeup()`. Cancels the current token. Callable from
    /// any task (sync, `&self`, takes no `&mut`). Idempotent — a second
    /// call before `rotate()` is a no-op (the token is already cancelled).
    pub(crate) fn wakeup(&self);

    /// Replace the current token with a fresh `CancellationToken`. Called
    /// by the app side after a public method returns `KafkaError::wakeup()`
    /// so the next call gets a fresh signal slot.
    pub(crate) fn rotate(&self);

    /// Disable wakeups (called from `close()`). Replaces the token with
    /// a "permanently cancelled" sentinel; subsequent `wakeup()` /
    /// `rotate()` calls are no-ops. Mirrors Java's `disableWakeups()`.
    pub(crate) fn disable(&self);

    /// Java's `maybeTriggerWakeup()` — synchronous check. Returns
    /// `Err(KafkaError::wakeup(...))` if the current token is already
    /// cancelled.
    pub(crate) fn maybe_trigger_wakeup(&self) -> Result<(), KafkaError>;
}
```

**Why not a mechanical translation:**

Java's `WakeupTrigger` exposes `setActiveTask(CompletableFuture)`,
`setFetchAction(FetchBuffer)`, `setShareFetchAction(...)`, `clearTask()`
because Java's `CompletableFuture` cannot be cancelled without a
back-channel — the trigger has to hold the future reference and call
`completeExceptionally(WakeupException)` from inside `wakeup()`. Rust's
`tokio::sync::watch::channel<CancellationToken>` + `select!` solves this
natively: any await point `select!`s on `token.cancelled()` and an
external `token.cancel()` wakes the await. No back-channel future
needed. Per CLAUDE.md §11 — async-native primitives over manual
mechanical translation.

The `FetchAction` / `ShareFetchAction` variants are dropped — the fetch
path will `select!` on the wakeup token directly (Phase 7). The
`ActiveFuture` / `WakeupFuture` distinction is subsumed by the
"rotate after returning `Wakeup` to caller" pattern (`consumer-threading.md`
§11 explicit).

**Test policy for `WakeupTriggerTest.java`:**

Java tests assert `setActiveTask` / `clearTask` / `getPendingTask` —
these methods don't exist in the Rust API. Each test method:
- If the test asserts on `wakeup()` causing a future to complete with
  `WakeupException`: rewrite as "spawn a task that `select!`s on
  `wakeup_trigger.current_token().cancelled()` AND a stalled
  `pending()` future, call `wakeup_trigger.wakeup()`, assert the
  select! takes the cancelled arm".
- If the test asserts on `setActiveTask` semantics: rewrite as the
  equivalent token-based scenario, or drop the test with a rationale
  comment ("Java state-machine quirk subsumed by §11 design").
- If the test asserts on `disableWakeups()` / `maybeTriggerWakeup()` /
  `clearTask`: translate directly to the equivalent Rust methods.
- Translate `WakeupTriggerTest` cases case-by-case. Each skip carries
  a one-line rationale.

### `ApplicationEvent` enum (`src/consumer/internals/events/application_event.rs`)

`pub(crate)`. One Rust enum subsumes Java's abstract class + ~25 in-scope
subclasses.

```rust
use std::collections::{HashMap, HashSet};
use std::time::Duration;

use crate::common::{TopicPartition, Uuid};
use crate::consumer::{
    OffsetAndMetadata, OffsetCommitCallback, OffsetAndTimestamp, SubscriptionPattern,
};
use crate::consumer::internals::AutoOffsetResetStrategy;
use crate::consumer::consumer_rebalance_listener_method_name::ConsumerRebalanceListenerMethodName;
use super::completable_event::CompletableEventHandle;

pub(crate) enum ApplicationEvent {
    // Non-completable
    AssignmentChange {
        all_partitions: HashSet<TopicPartition>,
    },
    CommitOnClose,
    LeaveGroupOnClose {
        reason: String,
    },
    StopFindCoordinatorOnClose,
    NewTopicsMetadataUpdate {
        enqueued_ms: i64, // see "Common fields" below
    },
    ConsumerRebalanceListenerCallbackCompleted {
        method_name: ConsumerRebalanceListenerMethodName,
        error: Option<KafkaError>,
    },
    UpdatePatternSubscription,

    // Completable (each carries a typed CompletableEventHandle<T>)
    CommitAsync {
        handle: CompletableEventHandle<()>,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
    },
    CommitSync {
        handle: CompletableEventHandle<()>,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
    },
    AsyncPoll {
        handle: CompletableEventHandle<()>,
    },
    FetchCommittedOffsets {
        handle: CompletableEventHandle<HashMap<TopicPartition, OffsetAndMetadata>>,
        partitions: HashSet<TopicPartition>,
    },
    ListOffsets {
        handle: CompletableEventHandle<HashMap<TopicPartition, OffsetAndTimestamp>>,
        timestamps_to_search: HashMap<TopicPartition, i64>,
        require_timestamps: bool,
    },
    CheckAndUpdatePositions {
        handle: CompletableEventHandle<()>,
    },
    ResetOffset {
        handle: CompletableEventHandle<()>,
        partitions: HashSet<TopicPartition>,
        offset_reset_strategy: AutoOffsetResetStrategy,
    },
    TopicMetadata {
        handle: CompletableEventHandle<HashMap<String, Vec<crate::common::PartitionInfo>>>,
        topic: String,
    },
    AllTopicsMetadata {
        handle: CompletableEventHandle<HashMap<String, Vec<crate::common::PartitionInfo>>>,
    },
    TopicSubscriptionChange {
        handle: CompletableEventHandle<()>,
        topics: HashSet<String>,
    },
    TopicPatternSubscriptionChange {
        handle: CompletableEventHandle<()>,
        pattern: regex::Regex,
    },
    TopicRe2JPatternSubscriptionChange {
        handle: CompletableEventHandle<()>,
        pattern: SubscriptionPattern,
    },
    Unsubscribe {
        handle: CompletableEventHandle<()>,
    },
    CreateFetchRequests {
        handle: CompletableEventHandle<()>,
    },
    PausePartitions {
        handle: CompletableEventHandle<()>,
        partitions: HashSet<TopicPartition>,
    },
    ResumePartitions {
        handle: CompletableEventHandle<()>,
        partitions: HashSet<TopicPartition>,
    },
    CurrentLag {
        handle: CompletableEventHandle<Option<i64>>,
        partition: TopicPartition,
    },
    SeekUnvalidated {
        handle: CompletableEventHandle<()>,
        partition: TopicPartition,
        offset: i64,
        offset_epoch: Option<i32>,
    },
}
```

**Common fields**:

Java's abstract `ApplicationEvent` has `type` and `enqueuedMs`. In Rust:
- `type` is implicit in the enum discriminant.
- `enqueuedMs` is recorded *outside* the event by the channel wrapper:
  `ApplicationEventHandler::add(event)` stamps it as `time.milliseconds()`
  into a parallel `EnvelopeEnqueuedMs` field. **Decision: keep
  `enqueued_ms: i64` as a field on the wrapper, NOT on each variant.**
  Rationale: only the reaper / debug logging cares about it; carrying it
  per variant pollutes match-arms. The `ApplicationEventHandler` emits
  `(ApplicationEvent, i64 enqueued_ms)` via the channel.

  Alternative: a wrapper struct `pub(crate) struct ApplicationEventEnvelope { pub event: ApplicationEvent, pub enqueued_ms: i64 }`.
  **Pick the wrapper struct** — cleaner downstream pattern-matching.

- `toString()` → `impl Display for ApplicationEvent` printing the variant
  name + key fields. Java prints `ClassName{type=X, enqueuedMs=Y}`; Rust
  matches the spirit.

### `CompletableEventHandle<T>` (`src/consumer/internals/events/completable_event.rs`)

The Rust analog of Java's `CompletableEvent<T>` interface, owned by each
completable event variant. `pub(crate)`.

```rust
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

use crate::common::KafkaError;

/// Sending end of a completable event. Held inside the event variant;
/// the receiver lives on the app side and is awaited via `addAndGet`.
///
/// The handle is **Send**, so the background task that processes the
/// event can hand it off (e.g. to a request manager) and have the
/// request manager complete it later.
pub(crate) struct CompletableEventHandle<T: Send + 'static> {
    inner: Arc<HandleInner<T>>,
    pub(crate) deadline_ms: i64,
}

struct HandleInner<T> {
    // `Mutex` not async — locking the slot is sub-microsecond. Per
    // CLAUDE.md §9 — never `await` while holding the guard.
    sender: Mutex<Option<oneshot::Sender<Result<T, KafkaError>>>>,
}

impl<T: Send + 'static> CompletableEventHandle<T> {
    /// Creates a (handle, receiver) pair. The handle lives inside the
    /// `ApplicationEvent` (queued to the bg task); the receiver lives on
    /// the app side and is awaited.
    pub(crate) fn new(deadline_ms: i64) -> (Self, oneshot::Receiver<Result<T, KafkaError>>);

    /// Java's `future.complete(value)` — best effort. If already
    /// completed (e.g. timed out by the reaper), this is a no-op.
    /// Returns `true` if THIS call performed the completion.
    pub(crate) fn complete(&self, value: T) -> bool;

    /// Java's `future.completeExceptionally(error)`. Same idempotency
    /// semantics.
    pub(crate) fn complete_exceptionally(&self, error: KafkaError) -> bool;

    /// `true` if the inner sender has already been consumed (via either
    /// completion path or a reaper-induced timeout).
    pub(crate) fn is_done(&self) -> bool;

    pub(crate) fn deadline_ms(&self) -> i64;
}

/// Object-safe trait for the reaper to iterate over heterogeneous
/// completable handles. The reaper does NOT need to know `T`; it only
/// needs `deadline_ms`, `is_done`, and a way to fail with a timeout.
pub(crate) trait CompletableEventErasedHandle: Send + Sync + 'static {
    fn deadline_ms(&self) -> i64;
    fn is_done(&self) -> bool;
    fn fail_with_timeout(&self, error: KafkaError) -> bool;
    fn type_name(&self) -> &'static str;
}

impl<T: Send + 'static> CompletableEventErasedHandle for CompletableEventHandle<T> {
    fn deadline_ms(&self) -> i64 { self.deadline_ms }
    fn is_done(&self) -> bool { self.is_done() }
    fn fail_with_timeout(&self, error: KafkaError) -> bool {
        self.complete_exceptionally(error)
    }
    fn type_name(&self) -> &'static str { std::any::type_name::<T>() }
}
```

`pub(crate) fn calculate_deadline_ms(now_ms: i64, timeout_ms: i64) -> i64`
mirrors Java's saturating-add helper (`MockConsumer.java` precedent for
`i64::MAX` handling).

**Reaper integration:**

The `CompletableEventReaper` stores `Vec<Arc<dyn CompletableEventErasedHandle>>`.
When a completable event is created, the app side gets the
`Receiver<Result<T, KafkaError>>` and **also** hands an
`Arc<dyn CompletableEventErasedHandle>` (cloned from the handle's
`inner` Arc) to the reaper. The handle inside the event variant holds
the same `Arc<HandleInner<T>>`.

To keep this ergonomic, the plan exposes a helper:

```rust
pub(crate) fn make_completable_event<T: Send + 'static>(
    deadline_ms: i64,
) -> (CompletableEventHandle<T>, oneshot::Receiver<Result<T, KafkaError>>, Arc<dyn CompletableEventErasedHandle>);
```

### `BackgroundEvent` enum (`src/consumer/internals/events/background_event.rs`)

`pub(crate)`. Two in-scope variants:

```rust
pub(crate) enum BackgroundEvent {
    Error {
        error: KafkaError,
    },
    ConsumerRebalanceListenerCallbackNeeded {
        method_name: ConsumerRebalanceListenerMethodName,
        partitions: Vec<TopicPartition>,
        // oneshot for the bg task to await listener-completion
        // (see consumer-threading.md §31 — bidirectional handshake).
        ack: tokio::sync::oneshot::Sender<Result<(), KafkaError>>,
    },
}

pub(crate) struct BackgroundEventEnvelope {
    pub event: BackgroundEvent,
    pub enqueued_ms: i64,
}
```

Note the `ack: oneshot::Sender<Result<(), KafkaError>>` — `consumer-threading.md`
§31 requires the bg task to await the listener's completion via this
oneshot. The bg task creates the BackgroundEvent with one half of the
oneshot, holds the other half, awaits it after enqueueing the event.
The app side, when draining background events, invokes the listener and
sends the result on the `ack` half.

### `ConsumerRebalanceListenerMethodName` (new helper)

`pub(crate)`. Three-variant enum: `OnPartitionsRevoked`,
`OnPartitionsAssigned`, `OnPartitionsLost`. Used by both
`ConsumerRebalanceListenerCallbackNeededEvent` (bg → app) and
`ConsumerRebalanceListenerCallbackCompletedEvent` (app → bg).

```rust
// src/consumer/consumer_rebalance_listener_method_name.rs
pub enum ConsumerRebalanceListenerMethodName { ... }
```

Place at `crate::consumer::ConsumerRebalanceListenerMethodName` — public
because it appears in the future `ConsumerRebalanceListener` invoker
API; tests may want to assert which method was invoked.

Java has this as a separate file at
`kafka/clients/src/main/java/org/apache/kafka/clients/consumer/internals/ConsumerRebalanceListenerMethodName.java`
(check the LOC). Translate it as part of Phase 5 since both events
reference it.

### `CompletableEventReaper` (`src/consumer/internals/events/completable_event_reaper.rs`)

`pub(crate)`. Translates `CompletableEventReaper.java:39-211`.

```rust
use std::sync::Arc;
use std::collections::VecDeque;

use super::completable_event::CompletableEventErasedHandle;
use crate::common::KafkaError;

pub(crate) struct CompletableEventReaper {
    tracked: Vec<Arc<dyn CompletableEventErasedHandle>>,
}

impl CompletableEventReaper {
    pub(crate) fn new() -> Self;

    pub(crate) fn add(&mut self, handle: Arc<dyn CompletableEventErasedHandle>);

    /// Java: `reap(long currentTimeMs) -> long`. Returns the number of
    /// events that were expired. Removes done events from `tracked`.
    pub(crate) fn reap(&mut self, current_time_ms: i64) -> u64;

    /// Java: `reap(Collection<?> events) -> long`. Expires both the
    /// tracked list AND any undrained items in the channel (passed in
    /// as a `VecDeque<ApplicationEventEnvelope>` from
    /// `mpsc::Receiver::try_recv` loop drain). Used on close. Does NOT
    /// consider deadlines — closes everything.
    pub(crate) fn reap_on_close(
        &mut self,
        unprocessed_events: impl IntoIterator<Item = Arc<dyn CompletableEventErasedHandle>>,
    ) -> u64;

    pub(crate) fn size(&self) -> usize;

    pub(crate) fn contains(&self, handle: &Arc<dyn CompletableEventErasedHandle>) -> bool {
        // pointer-eq via Arc::ptr_eq
    }

    pub(crate) fn uncompleted_events(&self) -> Vec<Arc<dyn CompletableEventErasedHandle>>;
}
```

Notes:

- **No `LogContext`** — `log::*` macros directly.
- **`Arc::ptr_eq`** for `contains` (Java uses object identity via
  `List.contains` on `CompletableEvent` references — same semantics).
- **`reap_on_close`** takes an iterator of handles, not a
  `Collection<ApplicationEvent>` — the channel-receiver loop is the
  caller's responsibility (Phase 10). This keeps `CompletableEventReaper`
  unaware of the channel type.
- **Race with concurrent completion**: Java handles this via
  `CompletableFuture.completeExceptionally` returning `false` if already
  completed. Rust's `complete_exceptionally(error) -> bool` mirrors this.
- **Iterator-remove pattern**: Java uses `Iterator.remove()`; Rust uses
  `Vec::retain_mut(...)` or `drain_filter` (stable as `Vec::extract_if`
  in recent Rust). Pick whichever is on the project's MSRV — check.

### `ApplicationEventHandler` (`src/consumer/internals/events/application_event_handler.rs`)

`pub(crate)`. **Minimal scope**: a channel wrapper, NOT the
network-thread-starting class Java has.

```rust
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

use crate::common::KafkaError;
use super::application_event::{ApplicationEvent, ApplicationEventEnvelope};
use super::completable_event::CompletableEventErasedHandle;

pub(crate) struct ApplicationEventHandler {
    sender: mpsc::Sender<ApplicationEventEnvelope>,
}

impl ApplicationEventHandler {
    /// Constructor takes only the channel; the Phase-10 background task
    /// owns the matching `mpsc::Receiver`. Wakeup is handled separately
    /// via `WakeupTrigger` (consumer-threading.md §11).
    pub(crate) fn new(
        sender: mpsc::Sender<ApplicationEventEnvelope>,
    ) -> Self;

    /// Java: `add(ApplicationEvent event)`. Enqueues with current
    /// timestamp. Per `consumer-threading.md` §10 the bg task drains
    /// with `try_recv` and the channel send is non-blocking from the
    /// app side. **Unbounded channel** matches Java's
    /// `LinkedBlockingQueue` default-capacity behavior; bounded would
    /// risk deadlock under bursty `add()` calls.
    pub(crate) async fn add(&self, event: ApplicationEvent, now_ms: i64) -> Result<(), KafkaError>;

    /// Java: `addAndGet(event)`. Caller awaits the receiver and gets
    /// `T` back. Takes a `Receiver<Result<T, KafkaError>>` so callers
    /// can pre-create it via `make_completable_event(...)`.
    pub(crate) async fn add_and_get<T: Send + 'static>(
        &self,
        event: ApplicationEvent,
        receiver: oneshot::Receiver<Result<T, KafkaError>>,
        now_ms: i64,
    ) -> Result<T, KafkaError>;
}
```

Notes:

- **`mpsc` over `bounded`**: Rust `tokio::sync::mpsc` is bounded by
  default; for unbounded, use `mpsc::unbounded_channel`. Pick
  `mpsc::UnboundedSender` to match Java's `LinkedBlockingQueue` (no
  capacity limit) — bounded would deadlock if the bg task is
  back-pressured. (`consumer-threading.md` §10 explicitly says "Drain
  unbounded (Java does)".) Use `tokio::sync::mpsc::unbounded_channel`.
  Replace types accordingly: `mpsc::UnboundedSender<...>` etc.
- **`add()` async vs sync**: with `unbounded_channel`, `send()` is sync
  (`mpsc::UnboundedSender::send(t) -> Result<(), SendError<T>>`). So
  `add` can be sync (`fn`). Update spec.
- **`add_and_get` async**: yes — awaits the receiver.
- **`asyncConsumerMetrics.recordApplicationEventQueueSize` call** —
  drop per "Out of scope" above.

### `BackgroundEventHandler` (`src/consumer/internals/events/background_event_handler.rs`)

`pub(crate)`. Symmetric to `ApplicationEventHandler`.

```rust
pub(crate) struct BackgroundEventHandler {
    sender: mpsc::UnboundedSender<BackgroundEventEnvelope>,
}

impl BackgroundEventHandler {
    pub(crate) fn new(sender: mpsc::UnboundedSender<BackgroundEventEnvelope>) -> Self;
    pub(crate) fn add(&self, event: BackgroundEvent, now_ms: i64) -> Result<(), KafkaError>;
}
```

### `EventProcessor` trait (`src/consumer/internals/events/event_processor.rs`)

`pub(crate)`. Phase-10 plumbing seam:

```rust
pub(crate) trait EventProcessor<E>: Send + 'static {
    fn process(&mut self, event: E);
}
```

No impls in Phase 5 — just the trait. Phase 10 supplies
`ApplicationEventProcessor` and the bg-side `BackgroundEventProcessor`.

## Cross-cutting requirements

- **License header**: 14-line Apache 2.0 header on every new file
  (CLAUDE.md §7).
- **New dependency: `tokio-util`** (for `tokio_util::sync::CancellationToken`,
  prescribed by `consumer-threading.md` §11). Not currently a workspace
  dep. Per CLAUDE.md §1.2 — pause and ask before adding. The Actor opens
  with a question to the Manager confirming the add before writing code.
  Alternative considered (and rejected): rebuilding the cancellation
  primitive on top of `tokio::sync::watch<u64>` + `Notify` — works but
  duplicates a well-tested external primitive. Recommend the add.
- **No `panic!` / `unimplemented!` / `todo!`** in production code
  (CLAUDE.md §5, §10).
- **`KafkaError::wakeup(...)`** already exists from Phase 3 — use it
  for the rotated-token cancellation paths.
- **`KafkaError::timeout(...)`** already exists at
  `src/common/kafka_error.rs:250-253` / `:324` — use it directly.
- **No `MutexGuard` held across `.await`** (CLAUDE.md §9). All
  completable handle locks are short — take the sender out, drop the
  guard, then send.
- **`#[async_trait]`**: NOT used in this phase. The traits introduced
  (`CompletableEventErasedHandle`, `EventProcessor<E>`) are sync. Per
  DoD §11 — async only for top-level surface; these are per-event
  internals.

## Verification

1. `cargo build` — clean
2. `cargo test --lib` — Phase 1–4 lib-test baseline holds + new event /
   reaper / wakeup tests pass
3. `cargo test --test consumer` — 36-test baseline holds + any
   integration tests added in this phase
4. `cargo xtask format-check` — clean
5. `cargo xtask lint` — clean
6. `cargo doc --no-deps` — builds
7. **Specific test coverage** (`cargo test --lib events::...`):
   - `CompletableEventReaperTest` (5+ cases)
   - `BackgroundEventHandlerTest` (~2 cases) — channel send/recv
   - `WakeupTriggerTest` subset (cases that translate to the
     rotating-token design)
8. **Loom test** (optional, defer to Critic decision): the rotating
   wakeup-token design has a subtle race between `wakeup()` and
   `rotate()`. Java has the same race (per §11 explicit
   acknowledgement). If the Critic requests a stress-mode test, add a
   `tokio::time::timeout`-wrapped scenario; otherwise the deterministic
   tests above suffice.

## Commit plan

Suggested commit granularity (one commit per logical bundle):

1. `Phase 5 (1/N): add tokio-util dependency` + license/scaffolding (only after user approves the dep)
2. `Phase 5 (2/N): ConsumerRebalanceListenerMethodName enum`
3. `Phase 5 (3/N): CompletableEventHandle + erased trait + helpers`
4. `Phase 5 (4/N): ApplicationEvent enum + envelope`
5. `Phase 5 (5/N): BackgroundEvent enum + envelope`
6. `Phase 5 (6/N): CompletableEventReaper + tests`
7. `Phase 5 (7/N): ApplicationEventHandler + BackgroundEventHandler + tests`
8. `Phase 5 (8/N): WakeupTrigger over rotating CancellationToken + tests`
9. `Phase 5 (9/N): EventProcessor trait`

Each commit must individually pass `cargo build`. Adjust the split if a
logical unit ends up too small or too large.

## Workflow

Per `.claude/rules/agent-roles.md`:

1. Actor 1 implements per this plan, commits incrementally, runs the
   verification matrix before declaring done.
2. Critic 1 reviews commits, writes findings to
   `design/history/Milestone-8/Phase-5/COMMENTS.1.md`.
3. Actor 1 fixes comments, moves resolved items to `COMMENTS.DONE.1.md`,
   commits with `fixup!` messages.
4. Repeat 2-3 until COMMENTS.1.md is empty.
