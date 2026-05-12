# Phase 7g — Revert the Phase-7b `send()` collapse

**Goal:** Restore Java parity on `Producer::send` / `send_with_callback`
by returning a `KafkaFuture<RecordMetadata>` (the inner broker-ack
future) wrapped in a `Result` (the outer enqueue-error). Matches what
the `master` branch shipped and what the perf test was written
against.

**User-approved:** 2026-05-12.

## Why this revisit

Phase 7b's commit (`c63329c`) collapsed Java's `Future<RecordMetadata>
send(...) throws ...` into:

```rust
async fn send(&self, record: ProducerRecord<K, V>) -> Result<RecordMetadata, KafkaError>
```

Phase 7b's stated rationale: "callers wanting fire-and-forget can
`tokio::spawn` the returned future." This contradicts **CLAUDE.md
rule 11** ("Per-message `tokio::spawn` on the send path: avoid — use
a shared completion task with a channel instead").

Master's signature is rule-11 compliant **and** Java-parity:

```rust
async fn send(&self, record: ProducerRecord<K, V>) -> Result<KafkaFuture<RecordMetadata>, KafkaError>
```

- Outer `async fn -> Result<_, KafkaError>` covers the sync-throw
  equivalent (serialization, accumulator wait, metadata fetch).
- Inner `KafkaFuture<RecordMetadata>` covers the broker-ack — caller
  decides what to do with it (drop / await / push to `FuturesUnordered` /
  push to a completion-task channel).

The one heap allocation per send for the `KafkaFuture` mirrors Java's
per-send `Future<RecordMetadata>` allocation. Rule 11 forbids
*type-erased boxed futures where an `impl Future` would do* — it does
not forbid the fundamental one-alloc-per-send that the Kafka contract
requires.

## Why now

Phase 8a.1 (perf-test re-enable) hit this directly: the perf test was
written against the master-branch shape (`Result<KafkaFuture, _>`).
With Phase 7b's collapse, the test fails to compile with 8 distinct
API mismatches — not patchable in scope. See Phase-8a.1 Actor 8
stop-and-report (2026-05-12).

Every Java test translated from now on will face the same
re-shape cost. Fixing 7b now is cheaper than carrying the cost
across the rest of Milestone-1 and into Milestone-2.

## Scope

| Area | Change | Files |
|------|--------|-------|
| `KafkaFuture<T>` wrapper | New / port-from-master | `src/common/kafka_future.rs` |
| `Producer` trait signature | `async fn send -> Result<KafkaFuture<RecordMetadata>, _>` (and `send_with_callback`) | `src/producer/producer.rs` |
| `impl Producer for KafkaProducer::send` | `do_send` returns the future instead of `.await`-ing it inline; wrap in `KafkaFuture::new` | `src/producer/kafka_producer.rs` |
| `FutureRecordMetadata` | Verify it implements / can be wrapped by `KafkaFuture<T>` (it should — Phase 6 produced it; master uses it the same way) | `src/producer/internals/future_record_metadata.rs` |
| Internal call sites | Every `producer.send(r).await` in tests adds `.get().await` to wait for broker ack | many; mechanical |
| Phase 7d send-path tests | Re-verify (interceptor double-fire fix, partitioner consistency, hot-path allocation tests) | `src/producer/kafka_producer.rs::tests` |
| Phase 7e `flush` / `close` | Re-verify interactions with un-awaited `KafkaFuture`s — `flush` should drive in-flights to completion; `close(timeout=0)` should resolve outstanding futures with `IllegalState` if forced | `src/producer/kafka_producer.rs` |

## Out of scope (do not change)

- `RecordAccumulator::append` shape (already returns `Arc<ProduceRequestResult>`-backed `FutureRecordMetadata`).
- `Sender::run_loop` / network client / wire path.
- `ProducerConfig`, partitioners, interceptors.
- `MockProducer` — still out of Milestone-1.
- The Phase 8a.0 wake-on-read / Notify wiring (load-bearing — leave alone).
- The Phase 8.0 `DefaultMetadataUpdater` translation.

## Reference materials (Java only — do **not** look at the `master` branch)

User-stated constraint (2026-05-12): "do it based on md files and
java, dont look at master." The translation must be derived from
the Java sources and CLAUDE.md rules, not copied from
`master`'s existing implementation. This produces a clean, principled
translation rather than an inherited one.

Authoritative Java sources:

- `kafka/clients/src/main/java/org/apache/kafka/common/KafkaFuture.java`
  — abstract class implementing `java.util.concurrent.Future<T>`.
  Defines the public method surface: `get()`, `get(timeout, unit)`,
  `isDone()`, `isCancelled()`, `cancel()`. Plus additional methods
  (`thenApply`, `whenComplete`, etc.) that are **not** part of the
  Milestone-1 producer surface and should be deferred.
- `kafka/clients/src/main/java/org/apache/kafka/clients/producer/Producer.java`
  — the interface defining `send(ProducerRecord<K, V>): Future<RecordMetadata>`
  and `send(ProducerRecord<K, V>, Callback): Future<RecordMetadata>`.
  Both methods are declared `throws InterruptException` (an
  unchecked exception); Java callers also expect `KafkaException`
  to surface on enqueue failure.
- `kafka/clients/src/main/java/org/apache/kafka/clients/producer/KafkaProducer.java`
  — concrete `send` impl. Examine the synchronous-throw vs.
  Future-resolution split: `KafkaProducer.send` does
  serialization → interceptors → partition resolution →
  `accumulator.append`, then returns the `FutureRecordMetadata`
  (or a synchronously-completed failed `FutureRecordMetadata`).
  **No `.get()` is called inside `send`** — the broker-ack wait
  happens later when the caller chooses to wait.
- `kafka/clients/src/main/java/org/apache/kafka/clients/producer/internals/FutureRecordMetadata.java`
  — the package-private implementation of `Future<RecordMetadata>`.
  Holds a `ProduceRequestResult` (shared per batch) plus per-record
  fields. Phase 6 already translated this; do **not** rewrite it.
- `kafka/clients/src/test/java/org/apache/kafka/clients/producer/KafkaProducerTest.java`
  — call patterns. Look for uses of `send(...).get()`,
  `send(...).get(timeout, unit)`, and patterns that drop the
  returned `Future` (fire-and-forget).

## Design principles (derive from CLAUDE.md, not from master)

1. **CLAUDE.md rule 4**: "Never change the contract of public API."
   Java's `send` returns a `Future<RecordMetadata>` immediately
   after enqueue (sync part) and throws on enqueue failure. The
   Rust translation must surface the same contract:
   `async fn send(...) -> Result<KafkaFuture<RecordMetadata>, KafkaError>`.
   - `async` because the enqueue itself can `.await` (buffer-pool
     wait, metadata fetch) — Java blocks the calling thread, Rust
     yields.
   - `Result<_, KafkaError>` because Java throws on enqueue failure.
   - `KafkaFuture<RecordMetadata>` because Java returns
     `Future<RecordMetadata>` (a heap-allocated handle the caller
     keeps).
2. **CLAUDE.md rule 11**: "avoid `Pin<Box<dyn Future>>` per call —
   prefer concrete `async fn` return types." This is about avoiding
   type-erased boxed futures *where an `impl Future` would do*. It is
   **not** a prohibition on the one heap allocation per send that
   the Kafka contract requires (Java has the same cost via JVM
   `Future` allocation). The `KafkaFuture<T>` wrapper holds one
   `Arc<dyn KafkaFutureOps<T>>` per send; this is the intended
   per-send cost.
3. **CLAUDE.md rule 9.5**: "If Java guarantees exactly-once
   callback invocation per record at a specific lifecycle point …
   the Rust translation must invoke the equivalent at the same
   point — not defer it or silently drop it." Phase 7d's interceptor
   double-fire fix invariant must continue to hold: the
   `on_acknowledgement` interceptor fires from the Sender loop
   after broker ack, exactly once. The caller awaiting the returned
   future doesn't change this.
4. **CLAUDE.md rule 9.1**: "If a method is blocking in Java it
   should async in Rust." Java's `send` is non-blocking (returns
   the `Future` quickly), but the synchronous part (accumulator
   append) can block on the buffer pool. The Rust `async fn send`
   yields where Java blocks; the outer `Result` resolves before the
   broker-ack future is awaited.

## Translation map (Java → Rust)

| Java member of `KafkaFuture<T>` / `Future<T>` | Rust equivalent | Where |
|---|---|---|
| `T get() throws InterruptedException, ExecutionException` | `pub async fn get(&self) -> Result<T, KafkaError>` | `src/common/kafka_future.rs` |
| `T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException` | `pub async fn get_timeout(&self, timeout: Duration) -> Result<T, KafkaError>` | same |
| `boolean isDone()` | `pub fn is_done(&self) -> bool` | same |
| `boolean isCancelled()` | **Defer** — Java's `FutureRecordMetadata.isCancelled()` always returns `false`. Add as a TODO-free `pub fn is_cancelled(&self) -> bool { false }` or omit entirely. | — |
| `boolean cancel(boolean mayInterruptIfRunning)` | **Defer** — Java's `FutureRecordMetadata.cancel()` always returns `false`. Same disposition as `isCancelled()`. | — |
| `thenApply`, `whenComplete`, `complete`, `completeExceptionally` | **Out of Milestone-1** — none used by `KafkaProducer.send` callers. Do not translate. | — |

The internal trait that wraps a concrete future type (like
`FutureRecordMetadata`) into a `KafkaFuture` is a Rust-idiom-only
abstraction — Java uses inheritance (abstract methods + subclass
override). Translate that abstraction shape to a `pub(crate) trait
KafkaFutureOps<T>` with the same abstract method set Java's
`KafkaFuture` defines; wrap it in `pub struct KafkaFuture<T> { inner:
Arc<dyn KafkaFutureOps<T>> }`. The trait is `pub(crate)` because
Java's abstract methods are not public-API extension points for the
producer use case.

## Out of scope reminder

- Compaction methods (`thenApply`, etc.) — defer to a future
  milestone when the consumer / admin client need them.
- Cancellation (`cancel`, `isCancelled`) — Java's
  `FutureRecordMetadata` doesn't support it; Milestone-1 doesn't need
  it. Document as deferred.
- `MockProducer` — out of Milestone-1.
- Master-branch implementation — **do not read**.

## DoD additions on top of `definition-of-done.md`

1. `cargo build`, `cargo build --features integration-tests` clean.
2. `cargo test --lib` green — count must rise (the new `KafkaFuture`
   has its own tests) or hold steady; **no test may be lost**.
3. `cargo test --features integration-tests producer_smoke` green
   **3 consecutive runs**. (Already passing in 8a.0; must not regress.)
4. Every Phase 7d / 7e / 7f `KafkaProducer` test that exercised
   `producer.send(...).await` is updated to `.send(...).await?.get().await`
   (or equivalent). The Java-parity round-trip becomes more direct,
   not less.
5. `cargo xtask format-check` + `cargo xtask lint` clean.
6. Hot-path allocation audit: confirm the new `KafkaFuture` allocation
   is one `Arc<dyn KafkaFutureOps<T>>` per send (no more, no fewer).
   Java pays one `Future` allocation per send too — this is the
   intended cost, not regression.

## Comment files

- Open: `COMMENTS.7g.md`
- Resolved: `COMMENTS.DONE.7g.md`

## Workflow

1. ✅ Plan approved (user, 2026-05-12).
2. ✅ This NOTES.md created.
3. Spawn **Actor 8** for Phase 7g (port `KafkaFuture`, change trait + impl,
   update internal call sites). Sub-phases as needed.
4. Spawn **Critic 8** to review → comments to `COMMENTS.7g.md`.
5. Fixup loop until `COMMENTS.7g.md` is empty.
6. Resume **Phase 8a.1** — at this point a one-line uncomment.

Agent number for this phase: **N = 8** (same as Phase 8 sub-phases — Phase 7g is a Phase-8-driven revisit of Phase 7b).
