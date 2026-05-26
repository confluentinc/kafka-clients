# Phase 6 — Producer Internals (Manager plan)

Phase 6 translates the producer-side internal machinery between
`KafkaProducer.send` and the wire: buffer pool, batch & future
plumbing, partitioners, interceptors, the `RecordAccumulator`, and the
`Sender` task. See `design/history/Milestone-1/PLAN.md` lines 285–311
for the goal.

`ProducerMetadata` (lines 290 in PLAN.md) was already translated in
Phase 4b — only re-confirm wiring against the new `RecordAccumulator`
and `Sender` here.

## Scope deviation from PLAN.md

PLAN.md Phase 7 lists `ProducerRecord`, `Callback`, `Partitioner`,
`ProducerInterceptor`, `RecordMetadata` under the public surface.
Three of them are **hard build-time prerequisites** of Phase-6
internals:

- `ProducerInterceptors::on_send` takes a `ProducerRecord` — pull
  `ProducerRecord` into Phase 6c.
- `FutureRecordMetadata` resolves to `RecordMetadata` — pull
  `RecordMetadata` into Phase 6a.
- `ProducerBatch` calls user `Callback` after each record's
  acknowledgement — pull the `Callback` trait into Phase 6a.
- `RoundRobinPartitioner` implements the user-facing `Partitioner`
  trait — pull `Partitioner` trait into Phase 6c.
- `ProducerInterceptors` wraps `Vec<Box<dyn ProducerInterceptor>>` —
  pull the `ProducerInterceptor` trait into Phase 6c.

Phase 7 then becomes purely the `KafkaProducer` shell + `Producer`
trait + `ProducerConfig` (no leaf data classes left to translate).
This deviation is local to Phase 6/7 and does not change the
milestone scope.

## Sub-phase split

Phase 6 is ~5,300 lines of Java production + ~7,500 lines of tests
(Sender + RecordAccumulator alone are ~6k of test LOC). Splitting
into 5 Actor/Critic rounds keeps each commit reviewable and gates
the very large `RecordAccumulator` and `Sender` translations behind
their dependencies.

### Phase 6a — Buffer pool & future plumbing (~1000 LOC prod + ~660 LOC tests)

**Java sources:**
- `clients/producer/BufferExhaustedException.java` (37)
- `clients/producer/internals/BufferPool.java` (356)
- `clients/producer/internals/ErrorLoggingCallback.java` (56)
- `clients/producer/Callback.java` (62) — trait
- `clients/producer/RecordMetadata.java` (124) — leaf data class
- `clients/producer/internals/ProduceRequestResult.java` (205)
- `clients/producer/internals/FutureRecordMetadata.java` (120)
- `clients/producer/internals/IncompleteBatches.java` (66)

**Tests:**
- `BufferPoolTest` (410)
- `RecordMetadataTest` (62)
- `RecordSendTest` (104) — exercises ProduceRequestResult/FutureRecordMetadata
- `FutureRecordMetadataTest` (82)

**Why first:** No `MemoryRecords` or wire interaction yet. Pure
in-memory data structures. Everything later in Phase 6 builds on
these primitives.

**Tokio-specific structure:**
- `BufferPool::allocate` is `async` — Java's `Condition.await(timeout)`
  becomes `tokio::sync::Notify::notified()` raced with
  `tokio::time::sleep` for `max.block.ms`. The wait queue is a
  `Mutex<VecDeque<Arc<Notify>>>` so wakers are FIFO-fair (matching
  Java's `ArrayDeque<Condition>`).
- `ProduceRequestResult` replaces Java's `CountDownLatch(1)` with a
  `tokio::sync::Notify` + `OnceLock<Result<...>>`. `await()` becomes
  an `async fn`; `await(timeout)` wraps in `tokio::time::timeout`.
- `FutureRecordMetadata` implements
  `Future<Output = Result<RecordMetadata, KafkaError>>` directly —
  no `oneshot` channel because the result is shared across siblings
  in a split batch (Java uses `CompletableFuture` plus a parent
  `relativeOffset` index; we keep the same shape but back it with
  `Arc<ProduceRequestResult>`).

**DoD additions for 6a:**
- `BufferPool` waits with `max.block.ms` and surfaces
  `KafkaError::BufferExhausted` on timeout — matches Java's
  `BufferExhaustedException` byte-for-byte error message.
- Holding `MutexGuard` across `.await` is forbidden (CLAUDE.md rule
  9.6). The `BufferPool` test must include a producer/consumer
  starvation scenario.

### Phase 6b — ProducerBatch (~620 LOC prod + ~380 LOC tests)

**Java sources:**
- `clients/producer/internals/ProducerBatch.java` (612)

**Tests:** `ProducerBatchTest` (374)

**Why second:** `ProducerBatch` is the join point between
`MemoryRecordsBuilder` (Phase 3), `BufferPool` (6a), `Callback` (6a),
and `FutureRecordMetadata` (6a). It is the unit the `RecordAccumulator`
manages and the `Sender` drains.

**Hot-path constraints (CLAUDE.md rule 12):**
- `tryAppend(timestamp, key, value, headers, callback, time)` writes
  serialized bytes **directly into the underlying `MemoryRecordsBuilder`
  buffer** — no intermediate `Vec<u8>` per record. Already enforced
  in Phase 3; re-confirm at the call site.
- `done()` does not copy the recordCount/baseOffset fields out of the
  shared `ProduceRequestResult`; it mutates in place under a single
  brief lock acquisition.
- The split path (`split()`) reuses the original record bytes by
  iterating `MemoryRecords` and re-encoding into smaller batches —
  no per-record key/value clone (Java does `record.key()/value()`
  which returns `ByteBuffer` slices; we mirror with `&[u8]`).

**DoD additions for 6b:**
- `tryAppend` returns `None` (Java: `null`) when the batch is full —
  not an `Err`. `KafkaError` only on serialization failure.
- `done(baseOffset, logAppendTime, exception)` is idempotent on
  retry — second call is a no-op (Java's `finalState` AtomicReference
  pattern). Translated as `OnceLock<FinalState>`.

### Phase 6c — Partitioners, interceptors, ProducerRecord (~970 LOC prod + ~720 LOC tests)

**Java sources:**
- `clients/producer/ProducerRecord.java` (225) — pulled fwd from Phase 7
- `clients/producer/Partitioner.java` (48) — trait
- `clients/producer/RoundRobinPartitioner.java` (72)
- `clients/producer/ProducerInterceptor.java` (123) — trait
- `clients/producer/internals/ProducerInterceptors.java` (157)
- `clients/producer/internals/BuiltInPartitioner.java` (349)

**Tests:**
- `ProducerRecordTest` (78)
- `RoundRobinPartitionerTest` (99)
- `ProducerInterceptorsTest` (268)
- `BuiltInPartitionerTest` (208)

**Why third:** `RecordAccumulator::append` (Phase 6d) calls
`BuiltInPartitioner` to pick a partition and `ProducerInterceptors`
to mutate the record. Both must exist before 6d.

**Hot-path constraints:**
- `ProducerRecord<K, V>` is generic over `K`, `V`. In Rust, hold
  borrowed slices on the send path (CLAUDE.md rule 12). The
  translation uses concrete byte slices on the accumulator boundary
  (`ProducerRecord<&[u8], &[u8]>` post-serialization) and a generic
  user-facing record pre-serialization. The hot path does not clone
  the value byte slice.
- `BuiltInPartitioner::partition` returns `(partition_id, sticky_remaining)`
  — no allocation per call (returns small struct on stack).

**DoD additions for 6c:**
- `ProducerRecord::topic()` returns `&str` (`Arc<str>` storage
  internally) — no `String` clone per send.
- `BuiltInPartitioner` uses `AtomicI32` for the sticky-partition
  index, not `Mutex<i32>` (CLAUDE.md rule 11).
- `ProducerInterceptors::on_send` returns the (possibly modified)
  record by **value**, not boxed — Java mutates in place via the
  return value, Rust mirrors with owned-record return.

### Phase 6d — RecordAccumulator (~1305 LOC prod + ~1892 LOC tests)

**Java sources:**
- `clients/producer/internals/RecordAccumulator.java` (1305)

**Tests:** `RecordAccumulatorTest` (1892)

**Why fourth:** Largest single class in Phase 6 by LOC and the
join point between everything before it (BufferPool, ProducerBatch,
BuiltInPartitioner, ProducerInterceptors, ProducerMetadata) and the
Sender. Translated alone to keep the diff reviewable.

**Tokio-specific structure:**
- `append` is the producer hot path. It is **synchronous** in spirit
  (Java holds `synchronized (deque)` then writes into the open batch)
  but `BufferPool::allocate` is `async`, so `append` becomes
  `async fn`. The lock-then-await pattern Java uses (`synchronized` +
  `BufferPool.allocate` which itself releases the synchronized
  monitor on `Condition.await`) is replaced with: take the deque
  lock, try to append into the open batch; if full, drop the lock,
  call `BufferPool::allocate().await`, re-take the lock, allocate
  the new batch.
  - This sequence must NEVER hold a `MutexGuard` across `.await`
    (CLAUDE.md rule 9.6).
- `ready(now)`, `drain(now, max_size)`, `expiredBatches(...)` are
  pure synchronous functions of the snapshot — translate as `fn`.
- `awaitFlushCompletion()` becomes `async fn` driven by
  `tokio::sync::Notify` (one shared notifier, woken by `done()` on
  the batch).
- The `nodesWithData` `Set<Node>` returned by `ready()` is a
  `HashSet<i32>` over node ids (CLAUDE.md hot-path interning rule).
- `transaction_manager: Option<TransactionManager>` field is
  **always `None`** this milestone. All Java
  `if (transactionManager != null)` branches translate to
  `if let Some(tm) = &self.transaction_manager { … }` with empty
  bodies (or `unreachable!()` for compiler-visible dead paths).
  Producer config validation rejects `enable.idempotence=true` and
  `transactional.id` (PLAN.md line 24, line 331). See the Phase 6e
  "Plug-in contract for future transactions" section below.

**DoD additions for 6d:**
- The "drain max size" calculation must respect `max.request.size`
  AND `request.timeout.ms` AND batch boundaries — assert against the
  Java drain test fixtures byte-for-byte (RecordAccumulatorTest has
  fixtures we translate verbatim).
- The producer-task `tokio::spawn` count must be O(1) per producer,
  not O(records) (CLAUDE.md rule 11). The accumulator does NOT spawn.
- `appendCallbacks` invocation point must match Java's
  `completeFutureAndFireCallbacks` lifecycle (CLAUDE.md rule 9.5):
  inside the sender task, after the partition's record is
  acknowledged/failed, before completing the future.

### Phase 6e — Sender + MockClient subset (~1143 LOC prod + ~4002 LOC tests + ~400 LOC mock fixture)

**Java sources:**
- `clients/producer/internals/Sender.java` (1143)
- `clients/MockClient.java` (845) — translate the **non-transactional,
  non-coordinator** subset needed by `SenderTest`. State the
  divergence in module rustdoc (mirroring Phase 5c's `MockSelector`
  pattern). Coordinator/`FindCoordinator` paths and transaction
  request matchers can be stubbed.

**Tests:** `SenderTest` (4002) — translate the **non-transactional**
test cases. Cases driving `TransactionManager` are skipped with a
clear comment listing each one. SenderTest cases driving the
idempotent producer path (sequence numbers, producer ID) are also
skipped — `enable.idempotence=true` is rejected at config time per
PLAN.md line 24.

**Tokio-specific structure (PLAN.md 298–304):**
- `Sender::run_loop()` is `async fn` running on a single
  `tokio::spawn` task. It drives the loop:
  `accumulator.ready -> drain -> client.send -> client.poll
  -> handle_responses -> complete_batches`.
- The user `Callback` is invoked **inside the sender task**, after
  the partition's record is acknowledged/failed, **before** the
  `FutureRecordMetadata` is completed — exactly matching Java's
  `ProducerBatch.completeFutureAndFireCallbacks` (CLAUDE.md rule 9.5).
- `wakeup()` becomes `Notify::notify_one()` so the sender loop's
  `client.poll` returns early.
- **No per-message `tokio::spawn`** anywhere (CLAUDE.md rule 11).
- `tokio::select!` arms inside the loop do not contain side effects
  (CLAUDE.md rule 9.6); side-effect work happens after the
  `client.poll().await` returns its `Vec<ClientResponse>`.

**Skip explicitly (Java code paths driven by `transactionManager` or idempotence):**
- `maybeSendAndPollTransactionalRequest`
- `addToTransactionManagerSendQueue`
- All `transactionManager.maybeUpdateProducerIdAndEpoch` calls
- `InitProducerId`, `AddPartitionsToTxn`, `EndTxn`, `TxnOffsetCommit`,
  `AddOffsetsToTxn`, `WriteTxnMarkers` request handling
- `bumpProducerEpochOnSequenceMismatch`
- `transactionManager.failIfNotReadyForSend`
- The retry path's `transactionManager.adjustSequencesDueToFailedBatch`

**Plug-in contract for future transactions (option (a) — selected):**
- `Sender` and `RecordAccumulator` both carry a
  `transaction_manager: Option<TransactionManager>` constructor
  parameter and field, always wired to `None` this milestone (Phase 7
  config validation rejects `enable.idempotence=true` and
  `transactional.id`).
- `TransactionManager` is translated as a **unit struct with no
  methods** this milestone, gated behind a clear rustdoc comment
  stating it is a placeholder for a future transactions milestone.
  This is **not** a `TODO` against CLAUDE.md rule 5 — the runtime
  contract is unambiguous: `None` → non-tx path; reaching the
  `Some(_)` arms is impossible because config validation rejects the
  inputs that would set them. CLAUDE.md rule 5 forbids silently
  hanging futures or unfinished records — a never-`Some` field
  exposes no such failure mode.
- Each Java `if (transactionManager != null) { … }` branch translates
  to `if let Some(tm) = &self.transaction_manager { … }` with an
  **empty body** today (or `unreachable!()` for paths the compiler
  cannot prove dead). Wiring the future translation = filling those
  bodies; no constructor or call-site signature churn.
- The `MockClient` subset translated this milestone is the
  **non-transactional, non-coordinator** subset. The matchers that
  exist only for transactional/coordinator response staging
  (`FindCoordinator`, `InitProducerId`, `AddPartitionsToTxn`,
  `EndTxn`, `TxnOffsetCommit`, `AddOffsetsToTxn`,
  `WriteTxnMarkers`) are **skipped** because the corresponding
  `SenderTest` cases are not translated this milestone. Everything
  else `MockClient` provides (queue-based response staging,
  `prepareMetadataUpdate`, default responses,
  `setNodeApiVersions`, disconnect & network/auth exception
  injection, connection state tracking) is required for `SenderTest`
  and **is** translated.

**Tests:** `SenderTest` non-transactional cases. The transactional /
idempotent cases are NOT translated this milestone — list each
skipped case in a module-level rustdoc with the Java test name.

**DoD additions for 6e:**
- A round-trip test: build a `Sender` over `MockKafkaClient` (already
  in Phase 5d) + `MockSelector`, send 3 records, assert all three
  futures resolve with the expected `RecordMetadata`.
- A retry test: first response is `NetworkError`, second response is
  `Errors::NONE` — assert the batch is retried, the user callback
  fires exactly once, the future completes successfully.
- A timeout test: `request.timeout.ms` elapses with no response —
  the batch fails with `KafkaError::Timeout`, callback fires once,
  future completes with the timeout error.
- Hot-path allocation audit (CLAUDE.md DoD line 10): the sender's
  per-poll allocation count is O(in-flight responses), not O(records).
  No `Box<dyn Future>` per record, no `String` clone per topic
  partition lookup.

## Module layout

Per CLAUDE.md naming rules:

```
src/
  producer/
    mod.rs
    producer_record.rs           // 6c (pulled fwd from Phase 7)
    record_metadata.rs           // 6a
    callback.rs                  // 6a (trait)
    partitioner.rs               // 6c (trait)
    round_robin_partitioner.rs   // 6c
    producer_interceptor.rs      // 6c (trait)
    buffer_exhausted_error.rs    // 6a (Java BufferExhaustedException)
    internals/
      mod.rs
      producer_metadata.rs       // already in Phase 4b
      buffer_pool.rs             // 6a
      error_logging_callback.rs  // 6a
      produce_request_result.rs  // 6a
      future_record_metadata.rs  // 6a
      incomplete_batches.rs      // 6a
      producer_batch.rs          // 6b
      built_in_partitioner.rs    // 6c
      producer_interceptors.rs   // 6c
      record_accumulator.rs      // 6d
      sender.rs                  // 6e
```

## Skip / defer notes (carried through all sub-phases)

Per PLAN.md line 24, line 296, line 322:
- `TransactionManager`, `TxnPartitionEntry`, `TxnPartitionMap`,
  `PreparedTxnState`, `TransactionalRequestResult` — **skip**
  (transactional/EOS out of milestone scope).
- `KafkaProducerMetrics`, `ProducerMetrics`, `SenderMetricsRegistry` —
  **skip** (metrics out of milestone scope; stub call sites with
  `// metric stub` no-ops).
- `MockProducer` — **skip** (not required for end-to-end production
  sends; optional follow-up).
- `enable.idempotence=true` and any `transactional.id` config —
  rejected at Phase 7 config validation, NOT silently accepted.
- All idempotent-producer state (producer ID, epoch, sequence
  numbers) — **skip** in Phase 6 entirely. Translated `Sender` does
  not carry a `producerIdAndEpoch` field; if a future milestone
  adds idempotence, re-introduce.

## Java equivalence guards

- `RecordAccumulator::append` returns an `AppendResult`-equivalent
  struct (`{batch_is_full: bool, new_batch_created: bool, future,
  abort_for_new_batch: bool}` — Java uses an inner `RecordAppendResult`).
- `Sender::sendProducerData` is the per-iteration drain-and-send
  routine — translate it as a private `async fn` on the `Sender`.
- `Sender::sendProduceRequest` builds the `ProduceRequest` from a
  `Map<TopicPartition, ProducerBatch>`. Use `HashMap<TopicPartition,
  ProducerBatch>` (`TopicPartition` already interned via Phase 4
  `Arc<str>` rule).
- `Sender::handleProduceResponse` walks each
  `ProduceResponseData.TopicProduceResponse.PartitionResponse` and
  calls `completeBatch` per partition. The Rust translation must
  preserve the order so callbacks fire in the order the broker
  returned them (matters for topic ordering tests).

## Hot-path identifier interning (CLAUDE.md rule 11)

- `ProducerRecord::topic` storage is `Arc<str>` (cloned per record
  send is just a refcount bump).
- `TopicPartition` keys reuse the `Arc<str>` from Phase 4.
- The `BufferPool` allocates `Vec<u8>` blocks of `batch.size`;
  callers obtain `&mut [u8]` slices into the block, never owned
  copies.
- Sender per-iteration `HashMap<i32, Vec<ProducerBatch>>` for the
  drain — `i32` node ids (Phase 5c convention), not `String`.

## Workflow per sub-phase

Each sub-phase ends with a clean
`cargo build && cargo test && cargo xtask format-check && cargo xtask lint`,
empty `COMMENTS.<N>.md`, and a fixup chain referencing the original
commit per Critic Round-1 disposition.

Agent number: **N=6** for this entire phase (carries through 6a–6e).

## Approval checklist

Before spawning Actor 6 for sub-phase 6a, please confirm:

- [ ] The 5-sub-phase split (6a–6e) matches your priorities, or tell
  me to merge/split.
- [ ] The deviation pulling `ProducerRecord` / `RecordMetadata` /
  `Callback` / `Partitioner` / `ProducerInterceptor` from PLAN.md
  Phase 7 into Phase 6 is acceptable. (Alternative: carry these as
  forward-declared traits in Phase 6 and translate the data classes
  in Phase 7 — more churn.)
- [x] `MockClient` is translated, with only its coordinator /
  transactional **matcher subset** skipped — the corresponding
  `SenderTest` cases (transactional + idempotent producer paths) are
  not translated this milestone, enumerated in module rustdoc. All
  other MockClient features remain (queue-based response staging,
  metadata updates, disconnect / network-exception injection,
  `setNodeApiVersions`, default responses, connection state tracking).
- [x] Choice (a) for the `Sender` / `RecordAccumulator` translation:
  carry a `transaction_manager: Option<TransactionManager>` field,
  always `None` this milestone. `TransactionManager` is a unit
  struct placeholder; `if let Some(tm) = …` branches are empty
  bodies today, ready for a future transactions milestone to fill
  in. No constructor or call-site signature churn when transactions
  land. Confirmed by user.
