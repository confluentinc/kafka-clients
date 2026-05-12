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

## Reference materials

- `master:src/producer/kafka_producer.rs:798` — target signature.
- `master:src/common/kafka_future.rs` — `KafkaFuture<T>` wrapper.
- `master:src/producer/internals/future_record_metadata.rs` — internal future type.
- `master:tests/integration/performance_test.rs:367` — example call site.

## Skip list (do not pull from master)

Master has additional surface (`KafkaFutureOps::get_timeout`, `is_done`,
chaining for batch splits, etc.). Pull **only** what's needed to
unblock Phase 8a.1's perf test plus what the trait signature requires.
A line-by-line port of master's `kafka_future.rs` is acceptable; a
line-by-line port of master's `kafka_producer.rs` is **not** —
fresh-impl has diverged on hundreds of unrelated lines, and only the
`send` / `send_with_callback` bodies should change.

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
