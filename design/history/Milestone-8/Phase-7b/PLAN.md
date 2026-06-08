# Phase 7b: `FetchCollector` + `FetchRequestManager`

## Goal

Translate the fetch loop that turns broker `FetchResponse` payloads into
user-visible `ConsumerRecords<K, V>`:

- `FetchCollector` (384 LOC) — drains `FetchBuffer` and produces
  `ConsumerRecords<K, V>` using `CompletedFetch::fetch_records` on each
  buffered partition.
- `FetchRequestManager` (191 LOC) — implements `RequestManager` (Phase 6)
  by composing `AbstractFetch` (Phase 7a). On `poll(current_time_ms)` it
  returns the set of `UnsentRequest`s for the next fetch round.

Plus the §27-required per-record allocation-budget test, which lives here
(`consumer-threading.md` §27: "Tests required ... follow the existing
producer hot-path allocation test precedent (Phase 6)").

## Branch / worktree

Runs on a **worktree** of `consumer-impl` for parallel execution with
Phase 7c and 7d. The Manager spawns three Actors in parallel; each lands
its work on its own branch (e.g. `phase-7b`), which is then merged back
to `consumer-impl` after all three close. Base SHA: `14f18b7` (Phase 7a
closed).

## Java sources

- `org/apache/kafka/clients/consumer/internals/FetchCollector.java` (384)
- `org/apache/kafka/clients/consumer/internals/FetchRequestManager.java` (191)

Tests (translate each — DoD §3):

- `clients/consumer/internals/FetchCollectorTest.java` (989) → inline or `tests/consumer/internals/fetch_collector_test.rs`
- `clients/consumer/internals/FetchRequestManagerTest.java` (4310) → inline or `tests/consumer/internals/fetch_request_manager_test.rs`
- **Per-record allocation-budget regression test** (`consumer-threading.md`
  §27): assert per-record allocation count matches the user-deserializer
  budget — no topic-name, no key/value buffer clones, no batch
  traversal allocations.

## Out of scope (deferred to later phases)

- **`Fetcher.java`** (211 LOC) and **`FetcherTest.java`** (3895 LOC) —
  classic-protocol only per `consumer-threading.md` §20.
- **`prepareCloseFetchSessionRequests`** if it requires `ControlRecordType`
  semantics — but the close-session response handlers
  `handle_close_fetch_session_success` / `handle_close_fetch_session_failure`
  ARE in Phase 7a (`d043160`); 7b just wires them via `pollOnClose`.
- **`FetchMetricsManager` / `FetchMetricsAggregator` / `FetchMetricsRegistry`
  / `SensorBuilder`** — no Rust metrics framework. Drop entirely.
- **Specific `FetchRequestManagerTest` cases** that test classic-protocol
  paths or share-consumer paths: defer with rationale per case.
- **`CreateFetchRequestsEvent` integration** — the event type was
  shipped by Phase 5 (`application_event.rs:CreateFetchRequests`). 7b
  wires `FetchRequestManager` to receive it via the bg task in Phase
  10. The current scope is just the `RequestManager::poll` surface.

## Module structure produced by this phase

```
src/consumer/internals/
├── fetch_collector.rs                # NEW
└── fetch_request_manager.rs          # NEW
```

`src/consumer/internals/mod.rs` gets two new `pub(crate) mod` lines.

## Type-by-type spec

### `FetchCollector` (`src/consumer/internals/fetch_collector.rs`)

`pub(crate)`. Java: `FetchCollector.java:50-384`.

```rust
pub(crate) struct FetchCollector {
    log_context: LogContext,
    subscriptions: Arc<Mutex<SubscriptionState>>,
    metadata: Arc<ConsumerMetadata>,
    fetch_config: FetchConfig,
    deserializers: Arc<Deserializers<K, V>>, // generic — see below
}

impl<K, V> FetchCollector<K, V>
where
    K: Send + 'static,
    V: Send + 'static,
{
    pub(crate) fn new(...) -> Self;

    /// Java: `Fetch<K, V> collectFetch(FetchBuffer fetchBuffer)`.
    /// Loops over buffered partitions, calling `CompletedFetch::fetch_records`
    /// up to `max_poll_records`. Returns the assembled `ConsumerRecords<K, V>`.
    pub(crate) fn collect_fetch(&self, fetch_buffer: &FetchBuffer) -> ConsumerRecords<K, V>;
}
```

Notes:

- **`FetchCollector<K, V>` is generic** over the key/value types since it
  invokes `Deserializers<K, V>` (Phase 2). The struct does NOT use
  `#[async_trait]` per DoD §11 — all methods are sync.
- **`collect_fetch` is sync `fn`** — Java's is too. No I/O, just buffer
  drain + deserialize.
- **`ConsumerRecords<K, V>` construction** must reuse the topic `Arc<str>`
  fields established by Phase 7a's `CompletedFetch::topic_arc` (§27 #1
  fix). Verify the per-record path stays zero-copy.
- **`max_poll_records` cap** is split across multiple buffered partitions
  (Java: track decreasing budget). Translate exactly.
- **The `Fetch<K, V>` Java return type** is a small wrapper around
  `Map<TopicPartition, List<ConsumerRecord>> + nextOffsets`. Rust
  returns `ConsumerRecords<K, V>` (Phase 1 already supplies it).

### `FetchRequestManager` (`src/consumer/internals/fetch_request_manager.rs`)

`pub(crate)`. Java: `FetchRequestManager.java:38-191`. Implements
`RequestManager` (Phase 6) by composing `AbstractFetch` (Phase 7a).

```rust
pub(crate) struct FetchRequestManager {
    abstract_fetch: AbstractFetch,
    pending_fetch_requests: VecDeque<oneshot::Sender<Result<(), KafkaError>>>,
    // Queue of pending CreateFetchRequestsEvent acks. Each is completed
    // when its fetch round finishes (success or failure).
}

impl FetchRequestManager {
    pub(crate) fn new(/* same params as AbstractFetch */) -> Self;

    /// Java: `enqueue(CompletableFuture<Void> future)` — called by the bg
    /// task when a CreateFetchRequestsEvent arrives.
    pub(crate) fn enqueue_create_fetch_requests(
        &mut self,
        ack: oneshot::Sender<Result<(), KafkaError>>,
    );

    /// Wire-up for `whenComplete` after a fetch response: feed the
    /// response into `AbstractFetch::handle_fetch_success` /
    /// `handle_fetch_failure`, then ack any pending requests.
    pub(crate) fn on_fetch_response(&mut self, response: ClientResponse);
    pub(crate) fn on_fetch_failure(&mut self, node_id: i32, error: &KafkaError);
}

impl RequestManager for FetchRequestManager {
    fn poll(&mut self, current_time_ms: i64) -> PollResult;
    fn poll_on_close(&mut self, current_time_ms: i64) -> PollResult;
    fn signal_close(&mut self);
}
```

Notes:

- **Composition over inheritance**: Java `extends AbstractFetch`; Rust
  composes. Per Phase 7a's plan — `AbstractFetch` is a concrete struct,
  not a trait, and 7b's `FetchRequestManager` owns it as a field.
- **`enqueue_create_fetch_requests`** receives the oneshot ack from the
  Phase 5 `ApplicationEvent::CreateFetchRequests` event. On the next
  successful `poll(...)` that produces requests, the ack is satisfied
  (the request is in-flight); on `poll_on_close`, the ack is failed
  with timeout.
- **`pending_fetch_requests` ordering matters**: FIFO (`VecDeque`). Java
  uses `ConcurrentLinkedQueue` because Java has cross-thread access
  here; Rust single-task ownership simplifies.
- **`on_fetch_response` / `on_fetch_failure`** are the Phase 10 wiring
  hooks. Bg task calls these after the response receiver resolves.
  Phase 10 wires them via the `whenComplete` callback on each
  `UnsentRequest`.

## Per-record allocation-budget regression test

Per `consumer-threading.md` §27. Pattern:

1. Construct a `MockFetchResponse` carrying a known batch (e.g., 100
   records, key + value of fixed sizes).
2. Build a `CompletedFetch` from it.
3. Use a `MaybeFailingDeserializer` (or `BorrowingDeserializer`) that
   records every `&[u8]` it sees but allocates ONLY the decoded `T`.
4. Call `collect_fetch(buffer)`.
5. Assert per-record allocation count == 2 × user_deserializer_T
   (key + value) + 1 × `RecordHeaders::from_slice` per record.
6. Assert NO `String::from_utf8` allocations.
7. Assert NO `Vec<u8>` clones of fetch buffer.

If `dhat-rs` or `tracking-allocator` is already a workspace dep, use
it. Otherwise add a custom global allocator stub gated on
`cfg(test, feature = "alloc-tracking")`. Verify with the Critic before
adding any dep.

**Decision for the Actor**: check `Cargo.toml` for an existing tracking
allocator; if none, write a thin wrapper over `std::alloc::System` with
a counter, gated to test builds. Don't add `dhat-rs` as a workspace
dep without explicit approval.

## Cross-cutting requirements

- **License header**: Apache 2.0 on every new file (CLAUDE.md §7).
- **No `#[async_trait]`** per DoD §11. `RequestManager::poll` is sync.
- **No `panic!` / `unimplemented!` / `todo!`** in production code.
- **§27 zero-copy** is the critical rule. The allocation-budget test
  ENFORCES it.
- **No new workspace dependencies** without user approval. If an
  allocation-tracking crate is needed, ask.
- **`ConsumerRecords<K, V>` construction** must reuse `Arc<str>` topic
  refs (no per-record `String` allocation).

## Verification

1. `cargo build` clean
2. `cargo test --lib` — must not regress 1208 baseline; new tests pass
3. `cargo test --test consumer` — 36 baseline holds
4. `cargo xtask format-check` clean
5. `cargo xtask lint` clean
6. `cargo test --lib -- --test-threads=1` no hangs
7. **Allocation-budget test passes**

## Commit plan

Suggested commit granularity:

1. `Phase 7b (1/N): FetchCollector + tests` (collect_fetch + max_poll_records cap)
2. `Phase 7b (2/N): FetchRequestManager + RequestManager impl`
3. `Phase 7b (3/N): FetchCollectorTest translation (subset of 989 LOC)`
4. `Phase 7b (4/N): FetchRequestManagerTest translation (subset of 4310 LOC)`
5. `Phase 7b (5/N): per-record allocation-budget regression test (§27)`

`FetchRequestManagerTest` is 4310 LOC, 88 tests. Translate in priority
order: send-path correctness, response-handling correctness, error
dispatch, session lifecycle, edge cases. Defer purely-mockistic tests
where the test scaffolding cost outweighs the coverage value; document
each skipped Java test with a one-line rationale per DoD §3.

## Workflow

Per `.claude/rules/agent-roles.md`:

1. Actor 1 implements per this plan on the 7b worktree, commits
   incrementally, runs the verification matrix.
2. Critic 1 reviews, writes findings to
   `design/history/Milestone-8/Phase-7b/COMMENTS.1.md`.
3. Actor 1 fixes, moves resolved items to `COMMENTS.DONE.1.md`,
   `fixup!` commits.
4. Repeat 2-3 until COMMENTS.1.md is empty.

When 7b/7c/7d all close, merge each worktree branch back to
`consumer-impl`. Conflicts are unlikely because each sub-phase touches
different files (7b: fetch_collector + fetch_request_manager; 7c:
topic_metadata_request_manager; 7d: offsets_request_manager +
offset_fetcher_utils + offsets_for_leader_epoch_client +
subscription_state.rs maybe_validate_*).
