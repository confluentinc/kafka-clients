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
  held inside `AsyncKafkaConsumer<K, V>` which is itself `Send + Sync` and
  spawned into a runtime task transitively.

### `ConsumerInterceptor<K, V>` (`src/consumer/interceptor.rs`)

Per `consumer-threading.md` §2: "per-batch but with per-record processing
inside; keep generic dispatch (`<I: ConsumerInterceptor<K, V>>`) where
feasible." This trait is invoked once per batch from `poll()`, not per
record, so the per-batch boxed-dyn cost is negligible. **No
`#[async_trait]`** — Java's `onConsume`/`onCommit` are sync.

```rust
pub trait ConsumerInterceptor<K, V>: Send + Sync + 'static {
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
- The bound `Send + Sync + 'static` is required because the listener is
  stored on the consumer (Phase 11) as `Arc<dyn ConsumerRebalanceListener>`.
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
- `Send + Sync + 'static` for the same Arc-storage reason as the listener.

### `Deserializers<K, V>` (`src/consumer/internals/deserializers.rs`)

`pub(crate)`. Holds the key + value deserializer. The deserializers are
stored as **trait objects** behind `Arc`, not as type-erased parametric
types:

```rust
pub(crate) struct Deserializers<K, V> {
    key: Arc<dyn Deserializer<K>>,
    value: Arc<dyn Deserializer<V>>,
}

impl<K, V> Deserializers<K, V> {
    pub(crate) fn new(
        key: Arc<dyn Deserializer<K>>,
        value: Arc<dyn Deserializer<V>>,
    ) -> Self;

    pub(crate) fn key_deserializer(&self) -> &dyn Deserializer<K> { &*self.key }
    pub(crate) fn value_deserializer(&self) -> &dyn Deserializer<V> { &*self.value }
}

impl<K, V> Clone for Deserializers<K, V> {
    fn clone(&self) -> Self {
        Self { key: Arc::clone(&self.key), value: Arc::clone(&self.value) }
    }
}
```

**Rationale on hot-path cost (CLAUDE.md §11 / DoD §10):**

The receive path calls `Deserializer::deserialize` once per key and once
per value per record. With `Arc<dyn Deserializer<K>>`:

- One pointer-indirection per call (vtable lookup): ~1 ns
- One `Arc` deref per call (atomic load of the `Arc` strong count is NOT
  triggered here — `&*self.key` is just a pointer deref): ~0 ns

This is acceptable because (a) actual deserializer bodies are 100ns+
(parsing UTF-8, parsing protobuf, etc.) and (b) the alternative —
threading `KD: Deserializer<K>` and `VD: Deserializer<V>` through every
type that touches the fetch path — would propagate two type parameters
through `Fetcher`, `FetchCollector`, `CompletedFetch`,
`ApplicationEventProcessor`, and ultimately `AsyncKafkaConsumer<K, V, KD,
VD>`, defeating the `Box<dyn Consumer<K, V>>` factory return.

The Critic should specifically NOT flag the boxed-dyn dispatch here as a
hot-path allocation issue. The §11 hot-path rule targets `Pin<Box<dyn
Future>>` per call (full heap alloc + virtual dispatch) — `Arc<dyn
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
use std::time::Duration;

use crate::common::{
    KafkaError, Metric, MetricName, Node, PartitionInfo, TopicPartition, Uuid,
};
use crate::consumer::{
    CloseOptions, ConsumerGroupMetadata, ConsumerRebalanceListener,
    ConsumerRecords, OffsetAndMetadata, OffsetAndTimestamp,
    OffsetCommitCallback, SubscriptionPattern,
};

#[async_trait]
pub trait Consumer<K, V>: Send + Sync + 'static
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    // ── State reads (sync — Java: non-blocking accessors)

    fn assignment(&self) -> HashSet<TopicPartition>;
    fn subscription(&self) -> HashSet<String>;
    fn paused(&self) -> HashSet<TopicPartition>;
    fn metrics(&self) -> HashMap<MetricName, Metric>;
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
        partitions: Vec<TopicPartition>,
    ) -> Result<(), KafkaError>;

    fn seek_to_end(
        &mut self,
        partitions: Vec<TopicPartition>,
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
        partitions: &HashSet<TopicPartition>,
    ) -> Result<HashMap<TopicPartition, OffsetAndMetadata>, KafkaError>;

    async fn committed_timeout(
        &mut self,
        partitions: &HashSet<TopicPartition>,
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
        partitions: &HashSet<TopicPartition>,
    ) -> Result<HashMap<TopicPartition, i64>, KafkaError>;

    async fn beginning_offsets_timeout(
        &mut self,
        partitions: &HashSet<TopicPartition>,
        timeout: Duration,
    ) -> Result<HashMap<TopicPartition, i64>, KafkaError>;

    async fn end_offsets(
        &mut self,
        partitions: &HashSet<TopicPartition>,
    ) -> Result<HashMap<TopicPartition, i64>, KafkaError>;

    async fn end_offsets_timeout(
        &mut self,
        partitions: &HashSet<TopicPartition>,
        timeout: Duration,
    ) -> Result<HashMap<TopicPartition, i64>, KafkaError>;

    async fn client_instance_id(
        &mut self,
        timeout: Duration,
    ) -> Result<Uuid, KafkaError>;

    // ── Pause / resume (sync — Java: pure SubscriptionState mutation)

    fn pause(&mut self, partitions: Vec<TopicPartition>) -> Result<(), KafkaError>;

    fn resume(&mut self, partitions: Vec<TopicPartition>) -> Result<(), KafkaError>;

    // ── Metric subscription (sync — local registry mutation)

    fn register_metric_for_subscription(&mut self, metric: Metric);

    fn unregister_metric_from_subscription(&mut self, metric: &MetricName);

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
- `Vec<TopicPartition>` (not `&[TopicPartition]`) on `assign` / `pause` /
  `resume` because the implementation typically takes ownership and
  builds a `HashSet`. `&[TopicPartition]` would force a copy. Same
  reasoning as Java accepting `Collection<TopicPartition>` (consumed).
- `&HashSet<TopicPartition>` on `committed` / `*_offsets` because the
  implementation only reads from it.
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
    key_deserializer: Arc<dyn Deserializer<K>>,
    value_deserializer: Arc<dyn Deserializer<V>>,
) -> Result<Box<dyn Consumer<K, V>>, KafkaError>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
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
parameters (Java passes them via `ConsumerConfig` reflection); in Rust
this is explicit per Phase 1's decision not to translate reflection
machinery.

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
   fn _assert_object_safe<K, V>(_: Box<dyn Consumer<K, V>>) {}
   fn _assert_send_sync<K, V>(_: &dyn Consumer<K, V>) {}
   ```
   This catches accidental `Self: Sized` bounds, non-`Send` async
   futures, or other object-safety regressions at compile time. Failure
   here means DoD §11 is broken.
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
