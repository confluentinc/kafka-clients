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

**Phase 7d Round 1 add-on:** `new_for_test` now emits a `log::warn!`
when the user supplies a non-default `partitioner.class` so the silent
deferral is visible in operator logs. Once Phase 7e wires `new(props)`
to a real `NetworkClient`, this warn becomes either:

1. a hard `KafkaError::Config(...)` rejection if the builder API does
   not ship in 7e, OR
2. a routing fallback ("supply your `Partitioner` via the builder") if
   the builder API DOES ship in 7e.

**Phase 7e MUST wire `partitioner.class`** — either as a hard rejection
or as a routed-through-builder error. A user with a custom partitioner
left silently dropped is a real correctness gap once `new(props)`
becomes the production constructor.

### `do_send` catch fan-out: per-arm user-callback parity

Phase 7d Round 1 (Critic 7 Suggestion 1) split the single Rust catch
arm into Java's four-arm fan-out using a new
[`KafkaError::is_api_exception`] classifier:

* `ApiException` subclasses (Timeout, RecordTooLarge, InvalidTopic,
  Disconnect, etc.) — fire user callback + `interceptors.on_send_error`.
* `KafkaException` direct subclasses (`Serialization`, `Config`,
  `Interrupt`, bare `Generic`), `InterruptException`, and stdlib
  `RuntimeException` (`IllegalArgument`, `IllegalState`,
  `UnsupportedOperation`) — fire `interceptors.on_send_error` only.

This mirrors Java's per-arm callback rule at `KafkaProducer.java:1056-1081`
(only `catch (ApiException)` invokes `callback.onCompletion`; the
other three arms re-throw without invoking the user callback).

Test-pinned by:
- `send_returns_record_too_large_and_fires_interceptor_on_send_error`
  (ApiException path — user callback fires once).
- `send_does_not_fire_user_callback_for_non_api_exception`
  (non-ApiException path — user callback does NOT fire).

### `Producer` trait methods covered: `send` and `send_with_callback`

Every other trait method (`init_transactions`, `flush`,
`partitions_for`, `close`, …) returns
`KafkaError::UnsupportedOperation("…Phase 7e")` until the matching
sub-phase lands. This mirrors Java's "rejected at construction" stance
for transactional methods and the "wait for the impl" stance for the
non-transactional ones.

## Phase 7e — landed (6 commits)

After Phase 7e, `impl Producer for KafkaProducer` is complete for
every non-transactional, non-telemetry method:

| Method | Status |
|--------|--------|
| `send` / `send_with_callback` | wired (Phase 7d) |
| `flush` | wired (Phase 7e) |
| `partitions_for` | wired (Phase 7e) |
| `metrics` | empty-map stub (Milestone-1 metric stub pattern) |
| `close` / `close_with_timeout` | wired (Phase 7e) |
| `init/begin/commit/abort_transaction` | `KafkaError::UnsupportedOperation` (Phase 9) |
| `client_instance_id` | `KafkaError::UnsupportedOperation` (Milestone-1 telemetry stub) |

### `partitioner.class` wired (commit 1/N)

The Phase 7d `log::warn!` placeholder is removed. A new private
`configure_partitioner` helper maps known class strings to translated
partitioner instances:

* `null` / unset / empty → built-in adaptive partitioner (`None`).
* `org.apache.kafka.clients.producer.RoundRobinPartitioner` (Java FQCN)
  AND `RoundRobinPartitioner` (simple-name alias) → `RoundRobinPartitioner`.
* anything else → `KafkaError::Config(...)`.

The simple-name alias is a Rust ergonomic add-on; the FQCN is kept for
Java-config compatibility. Once Phase 8 lifts the public ctor the
factory threads through unchanged.

### `flush` / `partitions_for` / `metrics` (commits 2-3/N)

* `flush` calls `accumulator.begin_flush()`, the Phase 7d `sender_wakeup`
  no-op, and `accumulator.await_flush_completion().await`. The Java
  "called inside callback" guard has no Tokio analogue — documented
  in the rustdoc as a Phase 8 hazard (a user calling `flush().await`
  from inside a `Callback` body would deadlock).
* `partitions_for` calls `wait_on_metadata` and returns
  `cluster.partitions_for_topic(topic).to_vec()`. Drops Java's
  `Objects.requireNonNull(topic)` (`&str` cannot be null in Rust) and
  the `InterruptedException` catch.
* `metrics` returns an empty `ProducerMetrics` (= `HashMap<String, ()>`).
  The proper translation of `MetricName` / `KafkaMetric` is deferred;
  callers that inspect `is_empty()` / `len()` will keep compiling once
  the proper type lands.

### `close` (commit 4/N) — async, idempotent, bounded

* `JoinHandle` storage flipped from `Option<JoinHandle<()>>` to
  `Mutex<Option<JoinHandle<()>>>` so `&self async fn close` can
  `take()` the handle for awaiting.
* Idempotency via `Arc<AtomicBool> closed.swap(true, AcqRel)` — a
  second call returns `Ok(())` immediately without touching the
  JoinHandle.
* Graceful path (timeout > 0): inline `Sender::initiate_close`
  semantics from the producer side (the Sender was moved into
  `tokio::spawn`; the producer no longer holds it). Then
  `tokio::time::timeout(timeout, JoinHandle).await`.
* Force-close path (timeout == 0): `force_close=true`,
  `accumulator.close()`, `JoinHandle::abort()`, await the cancelled
  handle so observers see the task fully terminated.
* The `Utils.closeQuietly(serializers, partitioner, interceptors, ...)`
  chain is a no-op in Milestone-1 (every plug-in has a no-op default
  `close()`); will be added when a non-trivial impl lands.

### `DefaultMetadataUpdater` deferred to Phase 8 (commit 5/N)

Java's `DefaultMetadataUpdater` is a >300-LOC inner class on
`NetworkClient`. It drives the metadata-negotiation request/response
loop and pulls in `MetadataRequest` / `MetadataResponse` plumbing
through the in-flight tracker. Translating in isolation would touch
four other modules; better paired with Phase 8 broker-loopback testing.

The public `KafkaProducer::new(props)` and `with_serializers(...)`
constructors return `KafkaError::UnsupportedOperation` with a Phase 8
marker. Unit tests use `KafkaProducer::new_for_test` (with a
`ManualMetadataUpdater` or a stub mock client) — every Phase 7e
trait-method test exercises the public surface end-to-end through
this path.

### Caveats for Phase 7f / Phase 8

* `close` idempotency uses an `AtomicBool` flag. Java's
  `testCloseIsIdempotent` (if it exists) might depend on a specific
  exception when close is called twice — confirm Phase 7f sees the
  same `Ok(())` shape rather than a Java-specific re-throw.
* `flush()` from inside a `Callback` body deadlocks the runtime —
  Phase 8 may add a `tokio::task::id()` guard once the spawned-task
  comparator API stabilises.
* `wait_on_metadata` still calls the no-op `sender_wakeup` (Phase 7d
  deferred). Phase 8 should expose an `Arc<dyn Fn() + Send + Sync>`
  wake handle from the Sender at construction time.

### Phase 7e Round 1 carry-overs

* **(Resolved in Round 1 fixup `5e3c2b5`)** Graceful-close
  timeout-elapsed branch now matches Java's "ioThread terminated"
  post-condition: `tokio::select!` between the `JoinHandle` and a
  `tokio::time::sleep(timeout)` arm; on elapse, `force_close=true`,
  wake, `handle.abort()`, then `await` the cancelled handle. No
  Phase 8 work outstanding for this divergence — pinned by
  `close_with_short_timeout_force_closes_and_waits_for_termination`.
* **Phase 7f carry-over** — hoist `MockClientImpl` visibility from
  `pub(super)` (sender.rs) to `pub(crate)` and translate
  `testFlushCompleteSendOfInflightBatches` (50 records, all per-record
  futures must resolve after `flush().await`). Phase 7e tests cover
  the single-record variant only because `MockClientImpl` is not
  reachable from `kafka_producer.rs`.

## Phase 7f — landed (3 commits)

### `MockClientImpl` visibility hoist (commit `11c83cb`)

`pub(super)` → `pub(crate)` on the `sender::tests` submodule and on
every `MockClientImpl` method. No production logic changed; no symbols
leak to non-test builds (`#[cfg(test)]` gating preserved). Unblocks
`KafkaProducerTest` translations that need to drive broker-response
choreography from `kafka_producer.rs`.

### `KafkaProducerTest` non-transactional translation (commit `c5109d0`)

24 Java tests translated + 1 cross-reference sentinel + 56 skipped
with rationale (transactional / metrics-telemetry / null-rejection /
Duration-non-negative / reflective-load / other). One production fix
landed: `KafkaProducer::close_inner` calls `self.metadata.close()` in
both arms (graceful + force-close) so blocked `send`s in
`wait_on_metadata.await_update` unblock promptly on close. Test count
1176 → 1201 (+25). See the skip block at the bottom of
`kafka_producer.rs`'s test module for the full enumerated audit.

### Phase 7f carry-overs (Phase 8 obligations)

* **`metadata.close()` ordering**: when `DefaultMetadataUpdater` lands,
  move `metadata.close()` back into the `client.close() →
  DefaultMetadataUpdater.close() → metadata.close()` chain
  (`NetworkClient.java:1325-1326`), and remove the explicit
  `metadata.close()` calls from `KafkaProducer::close_inner` (lines
  1213 and 1286 at time of writing). Java's chain places
  `metadata.close()` AFTER the run-loop drains via `client.close()`;
  Rust currently calls it BEFORE the JoinHandle await in
  graceful-close. Functionally equivalent in Milestone-1 (both paths
  set the metadata closed flag and the Sender's `wait_on_metadata`
  await unblocks either way), but the exact ordering only matches
  Java's contract once `DefaultMetadataUpdater` is translated.
  Restore Java ordering then.
* **50-record flush test fidelity**: rewrite
  `test_flush_complete_send_of_inflight_batches_50_records` to drive
  records through `producer.send().await` rather than
  `accumulator.append()` directly, once the `MockClient` harness has
  a multi-record helper that handles per-send Sender-tick
  coordination. The flush semantic itself is already pinned;
  Phase 8 lifts the test to exercise the full public-surface hot
  path.
