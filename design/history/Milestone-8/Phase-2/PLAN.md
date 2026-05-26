# Phase 2: Public Traits & Deserializers

## Goal

Define the public trait surface that `MockConsumer` (Phase 3) and
`AsyncKafkaConsumer` (Phase 11) both implement. This phase produces no
runtime behavior — only types, traits, and the `new_consumer` factory.

This is the **most read-then-frozen file in the milestone**: every later
phase calls methods on these traits. Changing them later means touching
every implementor.

## Branch

`consumer-impl`. All commits land here.

## Java sources

All paths relative to `kafka/clients/src/main/java/`, at submodule commit
`a18251bae0b825c69794a50dffd4c3100cf5ca5b`.

Production:

- `org/apache/kafka/clients/consumer/Consumer.java` (294)
- `org/apache/kafka/clients/consumer/ConsumerRebalanceListener.java` (220)
- `org/apache/kafka/clients/consumer/OffsetCommitCallback.java` (55)
- `org/apache/kafka/clients/consumer/ConsumerInterceptor.java` (85)
- `org/apache/kafka/common/serialization/Deserializer.java` (126)
- `org/apache/kafka/clients/consumer/internals/Deserializers.java` (98)
- `org/apache/kafka/clients/consumer/internals/ConsumerInterceptors.java` (112)

Tests (translate each — DoD §3):

- `clients/consumer/internals/ConsumerInterceptorsTest.java` (209) →
  `tests/consumer/internals/consumer_interceptors_test.rs`

There is no standalone `DeserializersTest.java`, `ConsumerTest.java`,
`ConsumerRebalanceListenerTest.java`, or `OffsetCommitCallbackTest.java`
in the Java repo at this pinned commit. No skips required.

## Out of scope (deferred to later phases)

- Concrete `Deserializer` impls (`StringDeserializer`, `IntegerDeserializer`,
  …). These live in `org.apache.kafka.common.serialization` and are not
  needed by the trait surface itself. Translate on demand later (Phase 11
  integration test will need at minimum a `BytesDeserializer` and a
  `StringDeserializer`).
- `Serializer<T>` and `Serializers` — producer-side; out of scope for the
  consumer milestone.
- `ClusterResourceListener`, `Configurable`, `Plugin` wrapper, `Monitorable`,
  `Metrics`. The Java `Deserializers` / `ConsumerInterceptors` constructors
  take a `Metrics` parameter and wrap implementations in `Plugin`. Rust
  drops these — there is no metrics framework in this milestone and
  `Configurable` reflection has no Rust analog. Constructors take the
  deserializer / interceptor directly.
- `MockConsumer` and `AsyncKafkaConsumer` impls (Phase 3 / 11).
- Factory's `Classic` group-protocol arm beyond returning
  `KafkaError::unsupported_version(...)` per `consumer-threading.md` §20.

## Module structure produced by this phase

```
src/consumer/
├── mod.rs                              # Consumer<K,V> trait + new_consumer factory
├── consumer_rebalance_listener.rs
├── offset_commit_callback.rs
├── deserializer.rs                     # public re-export from common
├── interceptor.rs                      # ConsumerInterceptor<K, V>
└── internals/
    ├── deserializers.rs
    └── consumer_interceptors.rs

src/common/serialization/
├── mod.rs
└── deserializer.rs                     # Deserializer<T> trait

tests/consumer/internals/
└── consumer_interceptors_test.rs
```

Update `src/lib.rs` to add `pub mod common::serialization;` (if it
doesn't already exist — check `src/common/mod.rs`). Update
`src/consumer/mod.rs` from Phase 1 to add the new `pub mod` lines.

## Type-by-type spec

### `Deserializer<T>` (`src/common/serialization/deserializer.rs`)

Lives in `common::serialization` (not `consumer::`) because the same trait
will be used by future serializer / consumer-side metrics work. Public.

Per `consumer-threading.md` §27 and DoD §11 — **sync trait, no
`#[async_trait]`, takes `&[u8]`**:

```rust
use crate::common::KafkaError;
use crate::common::header::Headers;

pub trait Deserializer<T>: Send + Sync + 'static {
    fn deserialize(&self, topic: &str, data: &[u8]) -> Result<T, KafkaError>;

    fn deserialize_with_headers(
        &self,
        topic: &str,
        headers: &Headers,
        data: &[u8],
    ) -> Result<T, KafkaError> {
        self.deserialize(topic, data)
    }

    fn configure(&mut self, _configs: &HashMap<String, String>, _is_key: bool) {
        // intentionally left blank — Java default
    }

    fn close(&mut self) {
        // intentionally left blank — Java default
    }
}
```

Notes for the Actor / Critic:

- The Java `default T deserialize(String topic, Headers headers, ByteBuffer
  data)` overload is **not** translated. Rust callers always pass
  `&[u8]`; the receive path slices into the `CompletedFetch` buffer
  (`consumer-threading.md` §27). A `ByteBuffer` overload would force
  exactly the kind of indirection §27 forbids.
- Java returns `T` and accepts null `byte[]` returning null `T`. Rust uses
  `Result<T, KafkaError>` — `KafkaError` for deserialization errors, and
  caller-side handling for "no data" (the receive path never passes a
  null slice; it passes an empty slice or skips the call entirely).
- The bounds `Send + Sync + 'static` are mandatory: the deserializer is
  stored as `Box<dyn Deserializer<T>>` inside `Deserializers<K, V>`,
  which is shared via `Arc<Deserializers<K, V>>` between the app side
  and the bg task (see `Deserializers` section below). `Arc<T>: Send`
  requires `T: Send + Sync`; that requirement transits the `Box<dyn>`
  boundary to the trait.
- **Async needs.** `deserialize` is synchronous to keep the receive path
  zero-copy and allocation-free (one `Pin<Box<Future>>` per record would
  dominate hot-path cost per CLAUDE.md §11). Deserializers that depend
  on external state — most notably a schema registry — should
  pre-populate an in-memory cache before the consumer starts polling.
  Add a rustdoc note pointing users at this pattern. For rare blocking
  calls inside `deserialize`, users can wrap with
  `tokio::task::block_in_place` on the multi-thread runtime; this is
  not free and should not be the per-record default.

### `ConsumerInterceptor<K, V>` (`src/consumer/interceptor.rs`)

Per `consumer-threading.md` §2: "per-batch but with per-record processing
inside; keep generic dispatch (`<I: ConsumerInterceptor<K, V>>`) where
feasible." This trait is invoked once per batch from `poll()`, not per
record, so the per-batch boxed-dyn cost is negligible. **No
`#[async_trait]`** — Java's `onConsume`/`onCommit` are sync.

```rust
pub trait ConsumerInterceptor<K, V>: Send + 'static {
    /// Java: `ConsumerRecords<K, V> onConsume(ConsumerRecords<K, V> records)`.
    /// Takes ownership of the batch and returns a (possibly modified) batch.
    fn on_consume(&self, records: ConsumerRecords<K, V>) -> ConsumerRecords<K, V>;

    /// Java: `void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets)`.
    fn on_commit(&self, offsets: &HashMap<TopicPartition, OffsetAndMetadata>);

    /// Java's `configure(Map<String, ?>)`. Default is no-op.
    fn configure(&mut self, _configs: &HashMap<String, String>) {}

    /// Java's `close()`. Default is no-op.
    fn close(&mut self) {}
}
```

Notes:

- `on_consume` takes `ConsumerRecords<K, V>` by value (matching Java's
  semantics — the chain can replace the batch entirely). The container
  `ConsumerInterceptors` is responsible for catching panics / errors and
  passing the previous-success batch along (matching Java's behavior).
- `on_commit` takes `&HashMap` because Java's signature is also read-only
  for the interceptor (interceptors should not mutate the commit map; if
  they do, behavior is undefined in Java too).
- Stored inside `ConsumerInterceptors<K, V>` as
  `Vec<Box<dyn ConsumerInterceptor<K, V>>>` — see below. This is OK on the
  per-batch granularity.
- **Bound is `Send + 'static`, NOT `Send + Sync + 'static`.** The
  interceptor lives in a `Box<dyn>` with a single owner (the
  `ConsumerInterceptors` container on the app side); no `Arc<dyn>` or
  shared `&dyn` storage exists. Dropping `Sync` lets users use interior
  mutability (`RefCell`, `Cell`) inside their interceptors without
  wrapping in `Mutex`. The Critic verifies the bound matches.

### `ConsumerInterceptors<K, V>` (`src/consumer/internals/consumer_interceptors.rs`)

Container. `pub(crate)` per CLAUDE.md §2 (it lives in `internals/`).

```rust
pub(crate) struct ConsumerInterceptors<K, V> {
    interceptors: Vec<Box<dyn ConsumerInterceptor<K, V>>>,
}

impl<K, V> ConsumerInterceptors<K, V> {
    pub(crate) fn new(interceptors: Vec<Box<dyn ConsumerInterceptor<K, V>>>) -> Self;

    pub(crate) fn is_empty(&self) -> bool;

    /// Java: chains `onConsume` through every interceptor. A panicking
    /// interceptor is caught (via `std::panic::catch_unwind` or by the
    /// container itself swallowing `Err` if interceptors evolve to return
    /// `Result`) — but per Java behavior, the *previous* successful
    /// `records` value is forwarded to the next interceptor. The
    /// translation must preserve this "pass last good value along"
    /// semantics, even on panic.
    pub(crate) fn on_consume(&self, records: ConsumerRecords<K, V>) -> ConsumerRecords<K, V>;

    /// Java: calls `onCommit` on every interceptor. Panics are caught
    /// per-interceptor; the next interceptor still gets called.
    pub(crate) fn on_commit(&self, offsets: &HashMap<TopicPartition, OffsetAndMetadata>);
}

impl<K, V> Drop for ConsumerInterceptors<K, V> {
    fn drop(&mut self) {
        // Java's close() logs errors but does not propagate.
        // Rust: call close() on each interceptor; ignore panics.
    }
}
```

Translation note on panic handling: Java catches `Exception` (not
`Throwable`), logs at WARN, and continues. Rust analog is
`std::panic::catch_unwind` around each `on_consume` / `on_commit` /
`close` call. The Critic should confirm the Actor doesn't omit
`catch_unwind` — without it, a panicking interceptor poisons the whole
poll loop. Use `log::warn!` for the logged-and-continued case, matching
the Java `LoggerFactory.getLogger(...).warn(...)` pattern.

**`AssertUnwindSafe` placement:** required only for the `&mut self`
methods (`configure`, `close`). `&dyn ConsumerInterceptor` is
auto-`UnwindSafe` since the trait is `Send`, so `on_consume` /
`on_commit` calls don't need the assertion. The Critic should
specifically check the implementation does NOT wrap `&self` calls in
`AssertUnwindSafe` (sloppy; misleading the reader about which calls
have the assertion).

**Caveats to document in rustdoc on the `ConsumerInterceptors` struct
(not on the user-facing `ConsumerInterceptor` trait):**

1. **`panic = "abort"`**: under this profile setting, panics call
   `abort()` directly; `catch_unwind` cannot recover. A panicking
   interceptor crashes the process. Rust-wide limitation, not specific
   to this code. Document, do not try to enforce.
2. **Interior mutability + panic.** Interceptors using `RefCell`,
   `Cell`, atomics, or `Mutex` are responsible for their own state
   consistency on panic. A panic mid-mutation may leave a `RefCell`
   borrowed or a `Mutex` poisoned; subsequent calls on the same
   interceptor are undefined-by-the-framework. Java's analog is
   "behavior is undefined if onConsume throws mid-modification" — same
   guarantee, different mechanism.

### `ConsumerRebalanceListener` (`src/consumer/consumer_rebalance_listener.rs`)

Per `consumer-threading.md` §31, **`#[async_trait]`** — callbacks may
nest into `consumer.commit_sync()` etc., which are themselves `async fn`.
The `Send` bound is mandatory (default `#[async_trait]` behavior).

```rust
use async_trait::async_trait;
use crate::common::{KafkaError, TopicPartition};

#[async_trait]
pub trait ConsumerRebalanceListener: Send + Sync + 'static {
    /// Java: `void onPartitionsRevoked(Collection<TopicPartition> partitions)`.
    async fn on_partitions_revoked(
        &self,
        partitions: &[TopicPartition],
    ) -> Result<(), KafkaError>;

    /// Java: `void onPartitionsAssigned(Collection<TopicPartition> partitions)`.
    async fn on_partitions_assigned(
        &self,
        partitions: &[TopicPartition],
    ) -> Result<(), KafkaError>;

    /// Java: `default void onPartitionsLost(Collection<TopicPartition> partitions) {
    ///           onPartitionsRevoked(partitions);
    ///       }`
    async fn on_partitions_lost(
        &self,
        partitions: &[TopicPartition],
    ) -> Result<(), KafkaError> {
        self.on_partitions_revoked(partitions).await
    }
}
```

Notes for the Critic:

- Java methods are `void` but throw checked / unchecked exceptions; in
  Rust we convert via `Result<(), KafkaError>` per CLAUDE.md §10.
- `partitions` is `&[TopicPartition]`, **not** `Vec<TopicPartition>`. Per
  CLAUDE.md §12 — accept the most general borrowed form. The implementor
  can `to_vec()` if they need ownership.
- **The bound `Send + Sync + 'static` is required**, not merely
  convenient, by §16+§31 combined. The listener is stored in
  `SubscriptionState` as `Arc<dyn ConsumerRebalanceListener>`.
  `SubscriptionState` lives behind `Arc<Mutex<...>>` per §16; §31
  invokes the listener on the app task by **cloning the Arc out of the
  lock** before any `.await` (§16 forbids holding the guard across
  await). Cloning an Arc out of a lock requires `Arc<dyn>: Send`, which
  requires the trait to be `Send + Sync`. With `Box<dyn>` the clone is
  impossible and the design collapses; `Arc<dyn>` is structurally
  required.
- Phase 11 ships the two regression tests required by `consumer-threading.md`
  §31. We do not write them here because there's no consumer to test
  against yet.

### `OffsetCommitCallback` (`src/consumer/offset_commit_callback.rs`)

Per `consumer-threading.md` §31: "OffsetCommitCallback follows the same
pattern" as the rebalance listener — invoked on the caller's task, allows
nested calls. Make it `#[async_trait]`.

```rust
use async_trait::async_trait;

#[async_trait]
pub trait OffsetCommitCallback: Send + Sync + 'static {
    /// Java: `void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception)`.
    async fn on_complete(
        &self,
        offsets: &HashMap<TopicPartition, OffsetAndMetadata>,
        error: Option<&KafkaError>,
    );
}
```

Notes:

- Returns `()` not `Result<(), KafkaError>` because Java's `onComplete` is
  `void` and **does not allow** the callback to fail the commit — the
  commit has already happened. The `Exception` parameter is informational.
- `error: Option<&KafkaError>` matches Java's pattern of "exception == null
  means success".
- `Send + Sync + 'static` because the callback travels through events
  on the bg task and is invoked on the app task after the bg task has
  already moved on — `Arc<dyn>` storage is required for the same
  clone-out-of-channel pattern as the listener. `Box<dyn>` would forbid
  the symmetric "send through channel, invoke later on app side"
  pipeline.

### `Deserializers<K, V>` (`src/consumer/internals/deserializers.rs`)

`pub(crate)`. Holds the key + value deserializer as concrete trait
objects. The consumer, `Fetcher`, and `FetchCollector` all need to call
the same deserializers; in Rust this is modeled as **one struct shared
via `Arc<Deserializers<K, V>>`** (Shape B), not three copies of a
`Clone`-able struct:

```rust
pub(crate) struct Deserializers<K, V> {
    key: Box<dyn Deserializer<K>>,
    value: Box<dyn Deserializer<V>>,
}

impl<K, V> Deserializers<K, V> {
    pub(crate) fn new(
        key: Box<dyn Deserializer<K>>,
        value: Box<dyn Deserializer<V>>,
    ) -> Self;

    pub(crate) fn key_deserializer(&self) -> &dyn Deserializer<K> { &*self.key }
    pub(crate) fn value_deserializer(&self) -> &dyn Deserializer<V> { &*self.value }
}

// NOT Clone. Sharing happens at the outer `Arc<Deserializers<K, V>>`.
```

The consumer holds `Arc<Deserializers<K, V>>` and clones the Arc into
`Fetcher` and `FetchCollector` at construction (3 Arc bumps total).
Cloning the deserializer trait objects themselves never happens.

**Why Shape B (one Arc-shared struct) instead of Shape A
(`Deserializers: Clone` over inner `Arc<dyn Deserializer<T>>`):**

The alternative — making `Deserializers` itself `Clone` by storing
`Arc<dyn Deserializer<K>>` inside — forces every owner to hold their own
`Deserializers` instance that happens to point at the same inner Arcs.
That is operationally identical to `Arc<Deserializers>` but adds a
redundant type-shape concept (`Deserializers: Clone` whose only purpose
is to share the same inner Arcs). With Shape B, there is exactly one
`Deserializers` struct per consumer; sharing happens at the outer Arc.
One concept instead of two.

**Rationale on hot-path cost (CLAUDE.md §11 / DoD §10):**

The receive path calls `Deserializer::deserialize` once per key and once
per value per record. With Shape B's `Box<dyn Deserializer<T>>` inside
`Arc<Deserializers>`:

- One pointer-indirection per call (vtable lookup through `Box<dyn>`):
  ~1 ns
- Zero atomic ops per record — the outer Arc is dereferenced once per
  poll (or once at fetcher construction), not per record.

This is acceptable because (a) actual deserializer bodies are 100ns+
(parsing UTF-8, parsing protobuf, etc.) and (b) the alternative —
threading `KD: Deserializer<K>` and `VD: Deserializer<V>` through every
type that touches the fetch path — would propagate two type parameters
through `Fetcher`, `FetchCollector`, `CompletedFetch`,
`ApplicationEventProcessor`, and ultimately `AsyncKafkaConsumer<K, V, KD,
VD>`, defeating the `Box<dyn Consumer<K, V>>` factory return.

The Critic should specifically NOT flag the boxed-dyn dispatch here as a
hot-path allocation issue. The §11 hot-path rule targets `Pin<Box<dyn
Future>>` per call (full heap alloc + virtual dispatch) — `Box<dyn
Deserializer>` is a one-time setup cost with vtable dispatch only.

No `From<ConsumerConfig>` constructor in Phase 2 — that's reflection-style
config loading and depends on a class registry we don't have. Phase 11
wires it explicitly via the consumer config builder.

### `Consumer<K, V>` trait (`src/consumer/mod.rs`)

The single `#[async_trait]` trait that `MockConsumer` and
`AsyncKafkaConsumer` both implement. Method-by-method translation of
`Consumer.java`. Async marker per `consumer-threading.md` §1:

- blocking-in-Java → `async fn` (network call, timed wait, listener
  invocation chain)
- non-blocking-in-Java → `fn` (pure state read / write under
  `SubscriptionState` lock)

The full enumeration is below. Where Java has multiple overloads with
different timeouts, the Rust trait keeps the `Duration`-taking one and
documents the fallback at the trait level (callers pick a sensible
default). Where Java has a deprecated `long`-millis overload, Rust drops
it (CLAUDE.md §5 — finish the work, don't carry dead variants).

```rust
use async_trait::async_trait;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use crate::common::{KafkaError, PartitionInfo, TopicPartition};
use crate::consumer::{
    CloseOptions, ConsumerGroupMetadata, ConsumerRebalanceListener,
    ConsumerRecords, OffsetAndMetadata, OffsetAndTimestamp,
    OffsetCommitCallback, SubscriptionPattern,
};

/// # Parameter conventions
///
/// The trait uses three argument shapes deliberately:
///
/// - **Owned collections (`Vec<T>`, `HashMap<K, V>`):** the
///   implementation stores or forwards the input long-term
///   (subscription state, request payload). Ownership transfer avoids a
///   per-element clone.
///
/// - **Borrowed slices / maps (`&[T]`, `&HashMap<K, V>`):** the
///   implementation iterates but does not retain the input. Callers can
///   pass `&Vec<T>`, `&[T; N]`, or any slice without conversion.
///
/// - **Borrowed scalars (`&TopicPartition`, `&str`):** read-only access
///   to a single value.
///
/// Methods that take `Vec<T>` are the ones that *consume* the input;
/// methods that take `&[T]` only *iterate* it. This rule is mechanical:
/// if the impl retains, it owns; if the impl reads, it borrows.
///
/// # Bounds: `Send + 'static`, NOT `Send + Sync`
///
/// The trait is `Send + 'static` so it can be stored as
/// `Box<dyn Consumer<K, V>>` and moved between tokio tasks (required
/// for multi-thread runtime support). `Sync` is intentionally NOT
/// required because the API is `&mut self` — only one task can call
/// methods at a time, no shared `&Consumer` reference exists.
///
/// Users who need cross-task sharing wrap in `Arc<Mutex<dyn Consumer>>`,
/// which works without `Sync` on the trait itself. Dropping `Sync` lets
/// users plug in `K`/`V` types that are `Send` but not `Sync` (e.g.
/// types containing `Cell`) without artificial restrictions.
#[async_trait]
pub trait Consumer<K, V>: Send + 'static
where
    K: Send + 'static,
    V: Send + 'static,
{
    // ── State reads (sync — Java: non-blocking accessors)

    fn assignment(&self) -> HashSet<TopicPartition>;
    fn subscription(&self) -> HashSet<String>;
    fn paused(&self) -> HashSet<TopicPartition>;
    fn group_metadata(&self) -> ConsumerGroupMetadata;
    fn client_id(&self) -> &str;
    fn current_lag(&self, topic_partition: &TopicPartition) -> Option<i64>;

    // ── Subscription / assignment (async per §1 — may interact with bg task)

    async fn subscribe(&mut self, topics: Vec<String>) -> Result<(), KafkaError>;

    async fn subscribe_with_listener(
        &mut self,
        topics: Vec<String>,
        listener: Arc<dyn ConsumerRebalanceListener>,
    ) -> Result<(), KafkaError>;

    async fn subscribe_pattern(
        &mut self,
        pattern: SubscriptionPattern,
    ) -> Result<(), KafkaError>;

    async fn subscribe_pattern_with_listener(
        &mut self,
        pattern: SubscriptionPattern,
        listener: Arc<dyn ConsumerRebalanceListener>,
    ) -> Result<(), KafkaError>;

    fn assign(&mut self, partitions: Vec<TopicPartition>) -> Result<(), KafkaError>;

    async fn unsubscribe(&mut self) -> Result<(), KafkaError>;

    // ── Poll

    async fn poll(
        &mut self,
        timeout: Duration,
    ) -> Result<ConsumerRecords<K, V>, KafkaError>;

    // ── Commit

    async fn commit_sync(&mut self) -> Result<(), KafkaError>;

    async fn commit_sync_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<(), KafkaError>;

    async fn commit_sync_offsets(
        &mut self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
    ) -> Result<(), KafkaError>;

    async fn commit_sync_offsets_timeout(
        &mut self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        timeout: Duration,
    ) -> Result<(), KafkaError>;

    async fn commit_async(&mut self) -> Result<(), KafkaError>;

    async fn commit_async_with_callback(
        &mut self,
        callback: Arc<dyn OffsetCommitCallback>,
    ) -> Result<(), KafkaError>;

    async fn commit_async_offsets_with_callback(
        &mut self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        callback: Arc<dyn OffsetCommitCallback>,
    ) -> Result<(), KafkaError>;

    // ── Seek (sync — Java: pure SubscriptionState mutation)

    fn seek(&mut self, partition: TopicPartition, offset: i64) -> Result<(), KafkaError>;

    fn seek_with_metadata(
        &mut self,
        partition: TopicPartition,
        offset_and_metadata: OffsetAndMetadata,
    ) -> Result<(), KafkaError>;

    fn seek_to_beginning(
        &mut self,
        partitions: &[TopicPartition],
    ) -> Result<(), KafkaError>;

    fn seek_to_end(
        &mut self,
        partitions: &[TopicPartition],
    ) -> Result<(), KafkaError>;

    // ── Position / committed (async — may fetch from broker)

    async fn position(
        &mut self,
        partition: &TopicPartition,
    ) -> Result<i64, KafkaError>;

    async fn position_timeout(
        &mut self,
        partition: &TopicPartition,
        timeout: Duration,
    ) -> Result<i64, KafkaError>;

    async fn committed(
        &mut self,
        partitions: &[TopicPartition],
    ) -> Result<HashMap<TopicPartition, OffsetAndMetadata>, KafkaError>;

    async fn committed_timeout(
        &mut self,
        partitions: &[TopicPartition],
        timeout: Duration,
    ) -> Result<HashMap<TopicPartition, OffsetAndMetadata>, KafkaError>;

    // ── Metadata (async — may fetch from broker)

    async fn partitions_for(
        &mut self,
        topic: &str,
    ) -> Result<Vec<PartitionInfo>, KafkaError>;

    async fn partitions_for_timeout(
        &mut self,
        topic: &str,
        timeout: Duration,
    ) -> Result<Vec<PartitionInfo>, KafkaError>;

    async fn list_topics(
        &mut self,
    ) -> Result<HashMap<String, Vec<PartitionInfo>>, KafkaError>;

    async fn list_topics_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<HashMap<String, Vec<PartitionInfo>>, KafkaError>;

    async fn offsets_for_times(
        &mut self,
        timestamps_to_search: HashMap<TopicPartition, i64>,
    ) -> Result<HashMap<TopicPartition, OffsetAndTimestamp>, KafkaError>;

    async fn offsets_for_times_timeout(
        &mut self,
        timestamps_to_search: HashMap<TopicPartition, i64>,
        timeout: Duration,
    ) -> Result<HashMap<TopicPartition, OffsetAndTimestamp>, KafkaError>;

    async fn beginning_offsets(
        &mut self,
        partitions: &[TopicPartition],
    ) -> Result<HashMap<TopicPartition, i64>, KafkaError>;

    async fn beginning_offsets_timeout(
        &mut self,
        partitions: &[TopicPartition],
        timeout: Duration,
    ) -> Result<HashMap<TopicPartition, i64>, KafkaError>;

    async fn end_offsets(
        &mut self,
        partitions: &[TopicPartition],
    ) -> Result<HashMap<TopicPartition, i64>, KafkaError>;

    async fn end_offsets_timeout(
        &mut self,
        partitions: &[TopicPartition],
        timeout: Duration,
    ) -> Result<HashMap<TopicPartition, i64>, KafkaError>;

    // ── Pause / resume (sync — Java: pure SubscriptionState mutation)

    fn pause(&mut self, partitions: &[TopicPartition]) -> Result<(), KafkaError>;

    fn resume(&mut self, partitions: &[TopicPartition]) -> Result<(), KafkaError>;

    // ── Lifecycle

    /// Java: `void enforceRebalance(String reason)` — javadoc says this
    /// method is classic-protocol-only; under the KIP-848 protocol it
    /// returns an unsupported-version error. Match Java behavior.
    async fn enforce_rebalance(
        &mut self,
        reason: Option<&str>,
    ) -> Result<(), KafkaError>;

    async fn close(&mut self) -> Result<(), KafkaError>;

    async fn close_with_options(
        &mut self,
        options: CloseOptions,
    ) -> Result<(), KafkaError>;

    /// Sync — translates Java's `void wakeup()`. Callable from any task.
    fn wakeup(&self);
}
```

**Methods deliberately NOT translated in Phase 2 (consistency with the
producer, which has no metrics framework either):**

- `metrics() -> Map<MetricName, ? extends Metric>`
- `registerMetricForSubscription(KafkaMetric metric)`
- `unregisterMetricFromSubscription(KafkaMetric metric)`
- `clientInstanceId(Duration timeout)` (KIP-714 telemetry)

When the metrics framework lands as its own milestone, the `Producer`
and `Consumer` traits gain these methods together. Until then, the
`Metric` / `MetricName` types are not introduced.

Notes for the Critic:

- Every method takes `&mut self` *except* the sync accessors and
  `wakeup()`. Java relies on internal `synchronized` blocks; Rust uses
  the borrow checker. The `&mut self` requirement on async methods means
  the consumer is one-active-call-at-a-time on a single task — which is
  exactly Java's model (Java's `KafkaConsumer` is documented as
  not-thread-safe). `wakeup()` is `&self` so any task can signal cancellation.
- `client_id() -> &str` returns a borrowed reference (CLAUDE.md §12).
- `current_lag` returns `Option<i64>`, not `OptionalLong` — the natural
  Rust analog.
- Sync `seek` etc. return `Result` because Java throws
  `IllegalArgumentException` / `IllegalStateException` on invalid input;
  per CLAUDE.md §10 we surface those as `Result`.
- **Owned `Vec` only when stored.** `subscribe(Vec<String>)` and
  `assign(Vec<TopicPartition>)` take ownership because the impl moves
  the elements into `SubscriptionState`. Same logic for
  `commit_sync_offsets(HashMap<TP, OAM>)` and `offsets_for_times(...)`
  — the impl forwards the map as a request payload.
- **Borrowed `&[T]` for read-only methods.** `pause`, `resume`,
  `seek_to_beginning`, `seek_to_end`, `committed`, `beginning_offsets`,
  `end_offsets` all iterate the input without retaining it. `&[T]` is
  strictly more general than `&HashSet<T>` (accepts slices, Vec deref,
  arrays); the impl can dedup internally if the operation requires it.
- The Java overload `poll(long timeoutMs)` is `@Deprecated`; we don't
  translate it.
- The Java `close(Duration timeout)` is `@Deprecated`; users call
  `close_with_options(CloseOptions::timeout(d))` instead. We do not
  translate the deprecated overload.

### `new_consumer` factory (`src/consumer/mod.rs`)

Per `consumer-threading.md` §2:

```rust
pub fn new_consumer<K, V>(
    config: ConsumerConfig,
    key_deserializer: Box<dyn Deserializer<K>>,
    value_deserializer: Box<dyn Deserializer<V>>,
) -> Result<Box<dyn Consumer<K, V>>, KafkaError>
where
    K: Send + 'static,
    V: Send + 'static,
{
    match config.group_protocol() {
        GroupProtocol::Consumer => {
            Err(KafkaError::unsupported_version(
                "AsyncKafkaConsumer is not yet implemented (Milestone-8 Phase 11). \
                 Phase 2 only ships the trait surface.",
            ))
        }
        GroupProtocol::Classic => Err(KafkaError::unsupported_version(
            "Classic group protocol is not yet supported in this client; \
             set group.protocol=consumer (KIP-848).",
        )),
    }
}
```

Phase 11 replaces the `Consumer` arm with
`Ok(Box::new(AsyncKafkaConsumer::<K, V>::new(config, key_deserializer,
value_deserializer)?))`. The signature stays the same.

The factory takes `key_deserializer` and `value_deserializer` as
`Box<dyn>` parameters — caller hands over ownership exactly once
(Shape B: the consumer wraps them in `Arc<Deserializers<K, V>>`
internally for sharing with `Fetcher`/`FetchCollector`). Java passes
them via `ConsumerConfig` reflection; in Rust this is explicit per
Phase 1's decision not to translate reflection machinery.

The K/V bounds match the trait (`Send + 'static`, no `Sync`). See the
bounds rationale above.

`MockConsumer` (Phase 3) does NOT come through `new_consumer` — it has
its own constructor. The factory is for the production consumer only.

## Cross-cutting requirements

- **License header**: 14-line Apache 2.0 header on every new file
  (CLAUDE.md §7).
- **`#[async_trait]`** is used on `Consumer`, `ConsumerRebalanceListener`,
  `OffsetCommitCallback`. It is **NOT** used on `Deserializer` or
  `ConsumerInterceptor`. The Critic must verify the surface check
  (DoD §11):
  - `Consumer<K, V>` → `#[async_trait]` ✓
  - `Deserializer<T>` → sync fn, no `#[async_trait]` ✓
  - `ConsumerInterceptor<K, V>` → sync fn, no `#[async_trait]` ✓
  - No `?Send` anywhere.
- **Trait bounds checklist** (Critic verifies the exact bounds):
  | Trait | Bound |
  |---|---|
  | `Deserializer<T>` | `Send + Sync + 'static` (Arc storage forces Sync) |
  | `ConsumerRebalanceListener` | `Send + Sync + 'static` (Arc storage forces Sync) |
  | `OffsetCommitCallback` | `Send + Sync + 'static` (Arc storage forces Sync) |
  | `ConsumerInterceptor<K, V>` | `Send + 'static` (Box single-owner; **no Sync**) |
  | `Consumer<K, V>` trait | `Send + 'static` (**no Sync**) |
  | `K, V` bounds on `Consumer` | `Send + 'static` (**no Sync**) |
- **No `block_on`-wrapped sync façade** anywhere — only the async trait
  is offered (DoD §11 / consumer-threading.md §1).
- **No new dependency** without explicit user approval. `async-trait` is
  already a workspace dep (used by existing code? Check
  `Cargo.toml`). If not — pause and ask.

## Verification

1. `cargo build` — clean
2. `cargo test` — Phase 1 tests still pass; new `ConsumerInterceptorsTest`
   passes
3. `cargo xtask format-check` — clean
4. `cargo xtask lint` — clean
5. `cargo doc --no-deps` builds — exercises the trait rustdoc
6. **Compile-time consumer-trait surface check**: a hidden test file
   `tests/consumer/trait_surface_check.rs` that does:
   ```rust
   fn _assert_object_safe<K, V>(_: Box<dyn Consumer<K, V>>)
   where K: Send + 'static, V: Send + 'static {}
   fn _assert_send<K, V>(_: Box<dyn Consumer<K, V>>)
   where K: Send + 'static, V: Send + 'static {}
   // Intentionally NO _assert_sync — Consumer<K, V> is Send-only.
   ```
   This catches accidental `Self: Sized` bounds, non-`Send` async
   futures, or other object-safety regressions at compile time. Failure
   here means DoD §11 is broken. If a future change tries to add
   `Sync` to the trait, the bounds-checklist row above flags it; this
   surface check does NOT enforce `Sync`.
7. `ConsumerInterceptors::on_consume` panic-recovery test specifically
   asserts that a panicking interceptor (using
   `std::panic::catch_unwind` and `AssertUnwindSafe`) does not poison
   the chain — matches Java's exception-swallowing behavior.

## Commit plan

Suggested commit granularity (one commit per logical bundle):

1. `Phase 2 (1/N): Deserializer<T> sync trait in common::serialization`
2. `Phase 2 (2/N): ConsumerRebalanceListener + OffsetCommitCallback async traits`
3. `Phase 2 (3/N): ConsumerInterceptor + ConsumerInterceptors with panic-safe chain`
4. `Phase 2 (4/N): Deserializers container`
5. `Phase 2 (5/N): Consumer<K, V> async trait + new_consumer factory (stub returning UnsupportedVersion)`
6. `Phase 2 (6/N): ConsumerInterceptorsTest translation + trait_surface_check test`

Each commit must individually pass `cargo build`. Adjust the split if a
logical unit ends up too small or too large.
