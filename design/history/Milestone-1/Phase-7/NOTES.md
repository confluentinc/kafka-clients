# Phase 7 — Public Surface

**Goal:** Wire the Phase 6 internals into the public `KafkaProducer` API.

**Plan reference:** `design/history/Milestone-1/PLAN.md` lines 314–334.

## Sub-phase ladder

| # | Scope |
|---|---|
| 7a | `ProducerConfig` + `ProducerConfigTest` |
| 7b | `Producer` trait + gap-fill `ProducerRecordTest` / `RecordMetadataTest` against Java |
| 7c | `KafkaProducer` skeleton: constructor, validation, dependency wiring, Sender spawn, Drop/close skeleton |
| 7d | `KafkaProducer::send` hot path: serialize → interceptors → partition → accumulator |
| 7e | `KafkaProducer::flush/close/partitions_for/list_topics/metrics-stub`; transactional methods → `UnsupportedOperation` |
| 7f | `KafkaProducerTest` non-transactional translation |

Each sub-phase ends green on
`cargo build && cargo test && cargo xtask format-check && cargo xtask lint`.

## Skip list (rejected at construction or stubbed)

- `enable.idempotence=true` → `ConfigException`
- `transactional.id` set → `ConfigException`
- `security.protocol ∈ {SASL_PLAINTEXT, SASL_SSL}` → rejected (Phase 9 re-enables)
- `init/begin/commit/abortTransaction`, `sendOffsetsToTransaction` → `KafkaError::UnsupportedOperation`
- `MockProducer` — out of milestone
- `metrics()` → empty map (metric-stub pattern)
- `clientTelemetryReporter` → `None`

## Comment files

- Open: `COMMENTS.7.md`
- Resolved: `COMMENTS.DONE.7.md`

## Phase 7c carry-over notes

Verified against `kafka/clients/src/main/java/org/apache/kafka/clients/producer/KafkaProducer.java`
during the Phase 7b Round 1 fixup (Critic 7 Suggestion 1):

- `KafkaProducer.java` defines exactly one transaction-init method:
  `public void initTransactions()` at line 648. **No** `initTransactions(boolean keepPreparedTxn)`
  overload exists in this 4.2 source.
- `prepareTransaction` does **not** exist on `KafkaProducer` — it only
  appears on `internals/TransactionManager.java:342`, which is package-
  private internal API and is not exposed on either the `Producer`
  interface or `KafkaProducer`.

Phase 7c **MUST** re-verify against `KafkaProducer.java` whether either
of these methods has appeared since (e.g. via a back-port). If yes,
they translate as inherent `impl KafkaProducer` methods (not trait
methods on `Producer`). If no, they remain absent. In either case the
Milestone-1 behavior is `KafkaError::UnsupportedOperation` per the skip
list above.

## Phase 7c — landed (3 commits)

### 1. Public constructor strategy

`KafkaProducer<K, V, C: KafkaClient>` is generic over the client type.
The 8-arg pkg-private `new_for_test` (matching `KafkaProducer.java:332`)
is the working construction path. The public `new(props)` and
`with_serializers(props, ...)` constructors return
`KafkaError::UnsupportedOperation` with a deferred-error message that
points callers at the `DefaultMetadataUpdater` translation gap (which
is required to wire the production `NetworkClient`).

### 2. `Producer` trait NOT yet implemented

Per the Phase 7c brief, `impl Producer<K, V> for KafkaProducer<K, V, C>`
is left for Phase 7d. The kafka_producer.rs test module includes a
`_phase_7c_does_not_impl_producer` compile-only stub that pins this
contract — if a future commit adds the impl prematurely, the stub will
need adjusting.

### 3. Cross-module trait change: `+ Send` on poll futures

`KafkaClient::poll` and `Selectable::poll` were changed from
`async fn` to
`fn poll(...) -> impl Future<...> + Send`
so the spawned `Sender::run_loop` task is itself `Send`. Existing impls
required no change (their futures are already Send). This is a
documented async-fn-in-trait pattern.

### 4. Sender accessors `running_arc` / `force_close_arc` ungated

Both were `#[cfg(test)]` on Sender. They're now `pub(crate)` non-test
because `KafkaProducer::Drop` (production code) needs them to flip the
flags before aborting the JoinHandle.

### 5. Phase 7d carryover

When Phase 7d adds `impl Producer for KafkaProducer`:

- `accumulator: Arc<RecordAccumulator>` field is `pub(crate)` —
  Phase 7d's `send` body needs append access. Verify before merging.
- `partitioner: Option<Arc<dyn Partitioner>>` — the producer holds the
  user-injected partitioner, but Phase 7c always sets `None`. Phase 7d
  needs to thread the config-time partitioner instance through
  (Java's `getConfiguredInstance(PARTITIONER_CLASS_CONFIG, ...)` is
  reflective; Rust will need a builder pattern). Document the
  divergence then.
- The local `StubKafkaClient` test mock is intentionally minimal —
  Phase 7f's `KafkaProducerTest` translation may need to extend it or
  switch to `sender::tests::MockClientImpl` (currently `pub(super)`).

## Phase 7d — landed

### `sender.wakeup()` is currently a no-op on the producer

Java's `KafkaProducer.waitOnMetadata` (`KafkaProducer.java:1129`) calls
`sender.wakeup()` between `metadata.requestUpdateForTopic(topic)` and
`metadata.awaitUpdate(version, remainingWaitMs)`. The Rust translation
spawns the Sender into a `tokio::spawn` task at construction time and
moves the [`KafkaClient`] into the task, so the producer no longer
owns a reference it can call `client.wakeup()` on.

**Impact:** A missed wake-up does NOT hang the producer — the Sender's
`run_once` re-fetches the metadata snapshot on every iteration and the
`await_update` deadline is bounded by `max.block.ms`. The worst-case
latency penalty is one Sender tick (`linger.ms + request.timeout.ms`).

**Resolution path** (deferred to a follow-up): expose an
`Arc<dyn Fn() + Send + Sync>` wake handle from the Sender at
construction time (captured pre-spawn from the underlying
`KafkaClient::wakeup` if the client is `Arc`-wrapped, or via a
`tokio::sync::Notify` driven into the run loop via a `select!` arm).

### `partitioner: Option<Arc<dyn Partitioner>>` still always `None`

The Java `partition.class` reflective instantiation has no Rust
analogue and Phase 7d does not yet add a builder API to inject one.
The accumulator's built-in adaptive partitioner handles every code
path Phase 7d needs; custom partitioners land with the public-ctor
builder in Phase 7e/8.

### `Producer` trait methods covered: `send` and `send_with_callback`

Every other trait method (`init_transactions`, `flush`,
`partitions_for`, `close`, …) returns
`KafkaError::UnsupportedOperation("…Phase 7e")` until the matching
sub-phase lands. This mirrors Java's "rejected at construction" stance
for transactional methods and the "wait for the impl" stance for the
non-transactional ones.
