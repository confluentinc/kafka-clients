# Phase 7a: Receive-path foundation + prereqs

## Goal

Translate the receive-path foundation that the fetch loop (7b), topic
metadata (7c), and offset fetch (7d) all depend on:

- Three Rust-side prerequisites (not yet translated):
  - `BufferSupplier` (decompression-buffer reuse pool)
  - `FetchRequest` + `FetchResponse` Java wrappers (the `*Data` structs
    are auto-generated; the wrapper class with `Builder` and helper
    accessors is not)
  - `FetchSessionHandler` (628 LOC — manages incremental-fetch session
    state, used by `AbstractFetch`)
- Then the actual Phase 7a content:
  - `FetchConfig` (105 LOC) — immutable bundle of fetch-related config
  - `FetchBuffer` (274 LOC) — `ConcurrentLinkedQueue<CompletedFetch>` +
    wakeup primitive
  - `CompletedFetch` (387 LOC) — per-partition batch state + record
    iteration
  - `AbstractFetch` (650 LOC) — abstract base shared by
    `FetchRequestManager` (7b in scope) and `Fetcher` (out of scope —
    classic-protocol only)

After 7a lands, Phase 7b (`FetchCollector` + `FetchRequestManager`),
Phase 7c (`TopicMetadataRequestManager`), and Phase 7d (`OffsetsRequestManager`
+ utilities + the deferred-from-Phase-4 `SubscriptionState::maybe_validate_position_for_current_leader`
/ `maybe_complete_validation`) can run in parallel on worktrees.

This is the **largest single phase in the milestone** at the production
layer (~2K Rust LOC including the 3 prereqs), but it's also the most
foundational. Get the receive-path zero-copy contract right per
`consumer-threading.md` §27 and everything downstream falls into place.

## Branch

`consumer-impl`. HEAD: `6d27a6b` (Phase 6 closed). All 7a commits land on
the same branch (no worktree for 7a — it's the serial foundation).

## Java sources

All paths relative to `kafka/clients/src/main/java/`, at submodule commit
`a18251bae0b825c69794a50dffd4c3100cf5ca5b`.

### Prerequisites (translate first)

- `org/apache/kafka/common/utils/BufferSupplier.java` (124) — decompression-buffer
  pool with a free-list to amortize allocation cost.
- `org/apache/kafka/common/requests/FetchRequest.java` (486) — wraps
  `FetchRequestData` (auto-generated), provides a `Builder` and
  per-version partition list helpers.
- `org/apache/kafka/common/requests/FetchResponse.java` (309) — wraps
  `FetchResponseData`, exposes `responseData()`, `error()`, `throttleTimeMs()`,
  `recordsOrFail()`, etc.
- `org/apache/kafka/clients/FetchSessionHandler.java` (628) — incremental-fetch
  session state machine.

### Phase 7a production

- `org/apache/kafka/clients/consumer/internals/FetchConfig.java` (105)
- `org/apache/kafka/clients/consumer/internals/FetchBuffer.java` (274)
- `org/apache/kafka/clients/consumer/internals/CompletedFetch.java` (387)
- `org/apache/kafka/clients/consumer/internals/AbstractFetch.java` (650)

### Tests (translate each — DoD §3)

- `clients/FetchSessionHandlerTest.java` (788) →
  `tests/clients/fetch_session_handler_test.rs` (or inline)
- `clients/consumer/internals/FetchConfigTest.java` (59) → inline
- `clients/consumer/internals/FetchBufferTest.java` (207) → inline
- `clients/consumer/internals/CompletedFetchTest.java` (307) → inline
- **No `AbstractFetchTest.java` exists** in the Java repo — abstract
  base behavior is tested through `FetchRequestManagerTest` (Phase 7b) and
  `FetcherTest` (out of scope). The Phase 7b plan picks up `AbstractFetch`
  coverage via `FetchRequestManagerTest`.

For `BufferSupplier`, `FetchRequest`, `FetchResponse`: check the Java
test directory. Translate the in-scope subset; tests that exercise
classic-protocol-only paths or share-consumer paths defer with rationale.

## Out of scope (deferred / dropped)

- **`Fetcher.java`** (211 LOC) — used only by `ClassicKafkaConsumer`. Out of
  scope per `consumer-threading.md` §20. Drop entirely.
- **`OffsetFetcher.java`** (440), **`TopicMetadataFetcher.java`** (167) —
  same reason. Drop.
- **`FetcherTest.java`** (3895 LOC), **`OffsetFetcherTest.java`** (1726),
  **`TopicMetadataFetcherTest.java`** (260) — drop with rationale.
- **`FetchMetricsManager`, `FetchMetricsAggregator`, `FetchMetricsRegistry`,
  `SensorBuilder`** — no Rust metrics framework. Drop the metrics
  parameter from constructors; replace with `()` placeholders or omit.
  `CompletedFetch::metricAggregator` is dropped from the Rust struct.
- **`InterruptException`** propagation in `FetchBuffer::awaitWakeup` —
  tokio tasks aren't interrupted; cancellation flows through the wakeup
  token. Drop the `Thread.interrupted()` check.
- **`FetchSessionHandler.FetchRequestData` member-level node Builder**
  used by Share consumer — share-consumer is out of scope. Translate
  only the non-share path.

## Module structure produced by this phase

```
src/common/memory/
└── buffer_supplier.rs                # NEW

src/common/requests/
├── fetch_request.rs                  # NEW: FetchRequest::Builder wrapper
└── fetch_response.rs                 # NEW: FetchResponse wrapper

src/
└── fetch_session_handler.rs          # NEW: top-level (Java is at
                                      #      org.apache.kafka.clients,
                                      #      mirroring src/network_client.rs etc.)

src/consumer/internals/
├── fetch_config.rs                   # NEW
├── fetch_buffer.rs                   # NEW
├── completed_fetch.rs                # NEW
└── abstract_fetch.rs                 # NEW
```

`src/lib.rs` gets `pub mod fetch_session_handler;` (mirror existing
`pub mod kafka_client;` etc.).
`src/common/memory/mod.rs` gets `pub(crate) mod buffer_supplier;`.
`src/common/requests/mod.rs` gets two new lines.
`src/consumer/internals/mod.rs` gets four new lines.

## Type-by-type spec

### `BufferSupplier` (`src/common/memory/buffer_supplier.rs`)

Java: `org.apache.kafka.common.utils.BufferSupplier`. Decompression-buffer
pool. Calls to `get(size)` return a `ByteBuffer` from the free list (or a
newly allocated one); `release(buffer)` returns it. `close()` empties
the pool.

The Java class has two flavors:
- The default impl pools buffers by size in a `HashMap<Integer, Deque<ByteBuffer>>`.
- A `static BufferSupplier NO_CACHING` that doesn't pool.

**Translation decisions**:

- `pub struct BufferSupplier` with `Mutex<HashMap<usize, VecDeque<Vec<u8>>>>`.
  `get(size) -> Vec<u8>` returns a (possibly recycled) zero-filled buffer.
  `release(buffer)` returns it.
- `BufferSupplier::no_caching()` constructor returns a variant that
  always allocates and drops.
- **NOT** translated as `Bytes`/`BytesMut` — these don't support resize
  and the Java code uses fixed-size scratch buffers. `Vec<u8>` is the
  right Rust analog.

`pub(crate)` if internal to the consumer; the `RecordBatch::decompress`
path probably already needs one — check existing producer-side code at
`src/producer/internals/` for any precedent. If it exists, use it; if
not, translate fresh.

### `FetchRequest` (`src/common/requests/fetch_request.rs`) + `FetchResponse` (`src/common/requests/fetch_response.rs`)

Mirror the `MetadataRequest`/`MetadataResponse` precedent
(`src/common/requests/metadata_request.rs:510 LOC`,
`metadata_response.rs:643 LOC`).

`FetchRequest::Builder`: takes `FetchRequestData` (the auto-generated
type), picks version based on `ApiVersions`, exposes
`Builder::for_consumer(max_wait_ms, min_bytes, fetchable_data)` /
`Builder::for_follower(...)` (we only need the consumer flavor — follower
is broker-side, out of scope).

`FetchResponse`: wraps `FetchResponseData`, exposes:
- `error() -> Errors` — top-level error.
- `response_data(topic_ids: &HashMap<Uuid, String>, version: i16) -> HashMap<TopicPartition, FetchResponseData::PartitionData>` —
  per-partition response.
- `throttle_time_ms() -> i32`.
- `records_or_fail(partition_data: &PartitionData) -> &Records` — the
  zero-copy record-iterator hook.

Translation: only translate the methods Phase 7a-d actually call. Refer
to `MetadataRequest`/`Response` precedent for the version-handling
pattern.

### `FetchSessionHandler` (`src/fetch_session_handler.rs`)

`pub`. Java: `org.apache.kafka.clients.FetchSessionHandler` (628 LOC).
Mirrors Java's package structure — at `src/fetch_session_handler.rs`,
not `src/consumer/...`.

Manages an incremental-fetch session per node. State:
- `next_metadata: FetchMetadata` (session ID + epoch).
- `session_partitions: LinkedHashMap<TopicPartition, PartitionData>` —
  insertion order matters for the wire protocol.
- `session_topic_names: HashMap<Uuid, String>` — topic-id-to-name resolution.

Methods:
- `new_builder() -> Builder` — start building the next fetch's session diff.
- `node_id() -> i32`.
- `session_topic_names() -> HashMap<Uuid, String>`.
- `handle_response(...)` — apply a `FetchResponse` to the session.

The inner `Builder` collects:
- `to_send: Vec<(TopicPartition, PartitionData)>`
- `to_forget: Vec<TopicIdPartition>` (replaced partitions)
- And builds the `FetchRequestData` from these.

**Translation notes**:

- **`LinkedHashMap` → `IndexMap`**. The crate is already a workspace
  dep (Phase 4 used it).
- **`AtomicInteger nextMetadata` epoch counter** → `AtomicI32`.
- **No `LogContext`** — `log::*` macros.
- This class is THREAD-SAFE in Java (synchronized methods). The Rust
  translation can be single-task per node — but per `consumer-threading.md`
  §16 precedent, keep `&self` / `&mut self` and let the caller wrap.

### `FetchConfig` (`src/consumer/internals/fetch_config.rs`)

`pub(crate)`. Java: `FetchConfig.java:30-105`. Trivial — immutable bundle.

```rust
pub(crate) struct FetchConfig {
    pub min_bytes: i32,
    pub max_bytes: i32,
    pub max_wait_ms: i32,
    pub fetch_size: i32,
    pub max_poll_records: i32,
    pub check_crcs: bool,
    pub client_rack_id: String,
    pub isolation_level: IsolationLevel,
}

impl FetchConfig {
    pub(crate) fn new(/* 8 params */) -> Self;
    pub(crate) fn from_consumer_config(config: &ConsumerConfig) -> Self;
}
```

Notes:
- **`pub` fields** matching Java's `public final` — the consumer reads
  them directly. Don't add getters.
- Translate `FetchConfigTest.java` (59 LOC) inline. Skip cases that
  reference classic-protocol-only `ConsumerConfig` keys (if any).

### `FetchBuffer` (`src/consumer/internals/fetch_buffer.rs`)

`pub(crate)`. Java: `FetchBuffer.java:50-274`. Thread-safe queue +
wakeup primitive.

**Translation decisions** per `consumer-threading.md` §16 / §11:

- Inner state behind `Mutex<FetchBufferInner>` (the buffer is shared
  between the bg task that adds and the app task that polls).
- `wokenup: AtomicBool` for the wakeup flag.
- `await_wakeup(timer)` is `async`-replaced by `select!` on a
  `Notify` and a timeout — the Java `Condition.await(timer)` translates
  to a tokio `Notify` (one waiter, multiple notifiers) or
  `tokio::sync::watch` for fan-out.

**Specifically**:

```rust
pub(crate) struct FetchBuffer {
    inner: Mutex<FetchBufferInner>,
    notify: Notify,
    wokenup: AtomicBool,
}

struct FetchBufferInner {
    completed_fetches: VecDeque<CompletedFetch>,
    next_in_line_fetch: Option<CompletedFetch>,
    closed: bool,
}

impl FetchBuffer {
    pub(crate) fn new() -> Self;
    pub(crate) fn is_empty(&self) -> bool;
    pub(crate) fn has_completed_fetches(&self, predicate: impl Fn(&CompletedFetch) -> bool) -> bool;
    pub(crate) fn add(&self, completed_fetch: CompletedFetch);
    pub(crate) fn add_all(&self, completed_fetches: Vec<CompletedFetch>);
    pub(crate) fn next_in_line_fetch(&self) -> Option<CompletedFetch>;
    pub(crate) fn set_next_in_line_fetch(&self, fetch: Option<CompletedFetch>);
    pub(crate) fn peek(&self) -> Option<CompletedFetch>; // clone or expose via callback
    pub(crate) fn poll(&self) -> Option<CompletedFetch>;
    pub(crate) async fn await_wakeup(&self, timeout: Duration);
    pub(crate) fn wakeup(&self);
    pub(crate) fn retain_all(&self, partitions: &HashSet<TopicPartition>);
    pub(crate) fn buffered_partitions(&self) -> HashSet<TopicPartition>;
    pub(crate) fn close(&self);
}
```

Notes:
- **`Mutex<FetchBufferInner>` not `tokio::Mutex`** — critical sections
  are short and never await.
- **`await_wakeup` is async** — uses `tokio::time::timeout` +
  `Notify::notified()`.
- **Don't return `&CompletedFetch` from `peek`** — would force the lock
  to be held; instead return `Option<CompletedFetch>` (clone) OR expose
  the predicate-based check (`has_completed_fetches`). The Critic
  evaluates.
- Translate `FetchBufferTest.java` (207 LOC) inline.

### `CompletedFetch` (`src/consumer/internals/completed_fetch.rs`)

`pub(crate)`. Java: `CompletedFetch.java:59-387`. The per-partition
batch state + record iteration. **THIS IS THE ZERO-COPY HOT PATH** per
`consumer-threading.md` §27.

```rust
pub(crate) struct CompletedFetch {
    pub partition: TopicPartition,
    pub partition_data: PartitionData,  // from FetchResponseData
    // ... (full field list below)
}
```

Full field translation:
- `partition: TopicPartition` (Java line 61).
- `partition_data: FetchResponseData::PartitionData` — from the auto-generated message data.
- `subscriptions: Arc<Mutex<SubscriptionState>>` — for position updates during iteration.
- `decompression_buffer_supplier: Arc<BufferSupplier>` — shared with peers.
- `batches: Vec<RecordBatch>` — Java uses `Iterator<? extends RecordBatch>`; Rust can either iterate lazily over the `partition_data.records` slice or eagerly collect. Lazy is better for §27 — see below.
- `aborted_producer_ids: HashSet<i64>` (per-batch READ_COMMITTED state).
- `aborted_transactions: BinaryHeap<AbortedTransaction>` — Java uses
  `PriorityQueue` ordered by `firstOffset`.
- `records_read: i32`, `bytes_read: i32` (metrics — drop or keep as
  stats fields without `FetchMetricsAggregator` integration).
- `current_batch: Option<RecordBatch>`.
- `last_record: Option<Record>`.
- `records: Option<CloseableIterator<Record>>` — Rust uses an owned
  iterator type from the record module.
- `cached_record_exception: Option<KafkaError>`.
- `corrupt_last_record: bool`.
- `next_fetch_offset: i64`.
- `last_epoch: Option<i32>`.
- `is_consumed: bool`.
- `initialized: bool`.

**`fetch_records(config, deserializers, max_records) -> Vec<ConsumerRecord<K, V>>`** — the central
method. Per §27:
- The `PartitionData::records: Records` byte buffer is owned by `partition_data`.
- `RecordBatch` and `Record` iterators borrow slices from that buffer.
- `Deserializer<T>::deserialize(topic, data: &[u8])` takes a borrowed slice — already correct from Phase 2.
- `ConsumerRecord<K, V>` owns its key/value `T` (decoded), but the
  topic is `Arc<str>` cloned from `SubscriptionState`. The headers are
  owned `Vec<RecordHeader>` per the §27 milestone-8 ruling.

**Anti-patterns to flag in review**:
- `Bytes::copy_from_slice(records_bytes)` — should be `Bytes::slice(...)`
  or zero-copy slice ref.
- Eager `records_vec: Vec<ConsumerRecord>` field — should be a lazy
  iterator + cursor.
- `String::from_utf8(topic_bytes.clone())` per record — clone the
  `Arc<str>` instead.
- Per-record `tokio::spawn` — forbidden by CLAUDE.md §11.

Translate `CompletedFetchTest.java` (307 LOC) inline.

### `AbstractFetch` (`src/consumer/internals/abstract_fetch.rs`)

`pub(crate)`. Java: `AbstractFetch.java:64-650`. Abstract base for
`FetchRequestManager` (7b). Holds:
- `LogContext` → `log::*`
- `time` → use `crate::common::utils::Time` if it exists, else direct
  `std::time::Instant` / explicit i64 timestamps.
- `metadata: Arc<ConsumerMetadata>` (Phase 4).
- `subscriptions: Arc<Mutex<SubscriptionState>>` (Phase 4).
- `metrics_manager: ()` — placeholder, no Rust metrics.
- `fetch_config: FetchConfig`.
- `fetch_buffer: Arc<FetchBuffer>`.
- `decompression_buffer_supplier: Arc<BufferSupplier>`.
- `session_handlers: HashMap<i32 /* node id */, FetchSessionHandler>` — keyed by node id.
- `nodes_with_pending_fetch_requests: HashSet<i32>` — single-fetch-per-node throttling.

Methods (from Java):
- `prepare_fetch_requests() -> Map<Node, FetchSessionHandler.FetchRequestData>` —
  build the next round of fetch requests. Skip nodes with pending fetches.
- `create_fetch_requests(...)` — build the actual `FetchRequest::Builder`
  + `UnsentRequest` (from Phase 6).
- `handle_fetch_response(response, fetch_target, data)` — apply response,
  enqueue `CompletedFetch` instances into `fetch_buffer`.
- `handle_initialize_completed_fetch_success(completed_fetch)` /
  `handle_initialize_completed_fetch_errors(...)` — error dispatch.
- `close_session_handler(node_id)`.
- `close(timer)` — Closeable.

**Why this is an abstract class in Java**: subclasses override `pollResult()`
(returns `PollResult` for `RequestManager`) and `fetch()` (returns the
fetch builder iteration). In Rust, **the natural translation is concrete
shared struct + protected-equivalent methods, with the subclass
behavior encoded as constructor parameters or closures**. Java's
inheritance-based polymorphism doesn't map cleanly to Rust.

**Decision for the Actor**: translate `AbstractFetch` as a **concrete
`pub(crate) struct AbstractFetch`** (no trait). Phase 7b's
`FetchRequestManager` composes it as a field
(`abstract_fetch: AbstractFetch`) and implements `RequestManager` by
delegating. This mirrors the `ProducerMetadata` / `ConsumerMetadata`
composition-over-inheritance precedent.

The Java `protected` fields become `pub(crate)` to give 7b access.

## Cross-cutting requirements

- **License header**: Apache 2.0 on every new file (CLAUDE.md §7).
- **`consumer-threading.md` §27 — zero-copy contract** is the critical
  rule for this phase. The Critic specifically audits:
  - Per-record allocation budget test (described in §27 — written in
    Phase 7 per the §27 text, lives with this phase or 7b).
  - No `Bytes::copy_from_slice` or `Vec::clone` of fetch buffer bytes.
  - `Deserializer<T>::deserialize(&[u8])` — borrows.
- **No new dependencies**. `indexmap`, `tokio`, `tokio-util`, `regex`
  are all already in.
- **No `panic!` / `unimplemented!` / `todo!`** in production code.
- **`#[async_trait]`** — NONE in this phase. Per DoD §11. Both
  `Deserializer` and the receive-path types are sync.

## Verification

1. `cargo build` clean
2. `cargo test --lib` — 1113 baseline holds; new tests pass
3. `cargo test --test consumer` — 36 baseline holds
4. `cargo xtask format-check` clean
5. `cargo xtask lint` clean
6. `cargo doc --no-deps` builds
7. `cargo test --lib -- --test-threads=1` no hangs

## Commit plan

Suggested commit granularity:

1. `Phase 7a (1/N): BufferSupplier in common::memory + tests`
2. `Phase 7a (2/N): FetchRequest wrapper + tests`
3. `Phase 7a (3/N): FetchResponse wrapper + tests`
4. `Phase 7a (4/N): FetchSessionHandler + tests`
5. `Phase 7a (5/N): FetchConfig + tests`
6. `Phase 7a (6/N): FetchBuffer + tests`
7. `Phase 7a (7/N): CompletedFetch + tests (zero-copy contract)`
8. `Phase 7a (8/N): AbstractFetch (concrete struct, no trait)`

Each commit must individually pass `cargo build`. Some bundles can split
further if a logical unit is too large (e.g. 7/N can split into
"CompletedFetch struct + fields" and "fetch_records iteration").

## Workflow

Per `.claude/rules/agent-roles.md`:

1. Actor 1 implements per this plan, commits incrementally, runs the
   verification matrix.
2. Critic 1 reviews commits, writes findings to
   `design/history/Milestone-8/Phase-7a/COMMENTS.1.md`.
3. Actor 1 fixes comments, moves resolved items to `COMMENTS.DONE.1.md`,
   `fixup!` commits.
4. Repeat 2-3 until COMMENTS.1.md is empty.

## After 7a closes

Three parallel sub-plans (7b, 7c, 7d) will be written and three Actors
spawned on worktrees:

- **7b**: `FetchCollector` + `FetchRequestManager` (production ~575
  LOC, tests ~5.3K). Skip `FetcherTest` (classic-protocol).
- **7c**: `TopicMetadataRequestManager` (production 283, tests 305).
- **7d**: `OffsetFetcherUtils` + `OffsetsForLeaderEpochClient` +
  `OffsetsRequestManager` + the deferred-from-Phase-4
  `SubscriptionState::maybe_validate_position_for_current_leader` /
  `maybe_complete_validation` methods + their 4 deferred tests.

These three parallelize cleanly because they each depend only on 7a +
Phase 4-6, not on each other.
