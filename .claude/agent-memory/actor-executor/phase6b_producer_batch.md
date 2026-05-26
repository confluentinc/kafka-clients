---
name: Phase 6b ProducerBatch translation
description: ProducerBatch design choices Send/Sync, OnceLock<FinalState>, Mutex<MutState>, panic::catch_unwind on user callbacks
type: project
---

Phase 6b translates `ProducerBatch.java` into `src/producer/internals/producer_batch.rs`. Three commits: core (1/6 = d3bab5a), behavior (2/6 = 6a21ea3), tests (3/6 = ba7f10b). 989 lib tests (978 baseline + 11 new from `ProducerBatchTest`).

**Why:** Memory + design notes for any 6c/6d/6e work that needs to plumb through the batch — and reviewer reasoning that needs to anticipate "why this particular shape."

**How to apply:**

1. **`unsafe impl Send + Sync` is required** because `MemoryRecordsBuilder` is `!Send + !Sync` (raw pointer self-reference). The batch will be held by `Arc<ProducerBatch>` across the producer/sender threads — `IncompleteBatches`, `RecordAccumulator`, `Sender` all need it. Mediation is via `Mutex<MutState>` which provides Java's "external synchronization" contract. Keep the SAFETY comment.

2. **`OnceLock<FinalState>` is the CAS-once primitive** — Java's `AtomicReference<FinalState>` with `compareAndSet(null, X)` semantics. `OnceLock::set` returns `Err` if already set; that's exactly the `if (CAS-fails)` Java branch. Don't reach for `Mutex<Option<FinalState>>` here.

3. **Callbacks fire under `panic::catch_unwind`.** If a user callback panics, the *future must still complete* for the other waiters. Java catches `Throwable` for the same reason (`catch (Exception e) { log.error(...) }`). Don't omit — losing the wake-up is much worse than a swallowed panic.

4. **`MutState` drains thunks before firing callbacks** (`std::mem::take`). This means the callback runs *outside* the `mut_state` lock, so user code can't re-enter and deadlock. The lock is taken only briefly to extract the thunks vector.

5. **Split path zero-copy.** The `MemoryRecords::batches()` iteration returns batches that yield `&dyn Record + 'a` borrowing from the source `Bytes` payload. `Record::key()/value()` returns `&[u8]` slices into that payload. Forwarding into a fresh `MemoryRecordsBuilder::append` keeps it zero-copy through the split.

6. **`try_append_with_time` overload exists for clippy.** `try_append` itself has 7 args (max allowed); adding `time: Arc<dyn Time>` would push it to 8. We expose `try_append` (uses `system_time()`) as the production entry point and `try_append_with_time` (with `#[allow(clippy::too_many_arguments)]`) for tests that need a `MockTime`.

7. **Plug-in stubs for transactions.** `assign_producer_state_to_batches` is wired through but `has_sequence()` is `false` by construction this milestone, so the loop body is never reached. `set_producer_state` returns `Result` (forwarded from `MemoryRecordsBuilder`) but the call from `assign_producer_state_to_batches` ignores it via `let _ =` because the path is statically dead.

8. **`KafkaError::RecordTooLarge` substitutes for `RecordBatchTooLargeException`** (which extends `RecordTooLargeException` in Java but doesn't have its own variant in our error enum). The split path uses it via `ErrorsByIndex`.

9. **`AbstractRecords::estimate_size_in_bytes_upper_bound` was added** in this phase to match Java's call-site signature `(magic, compression, key, value, headers)`. Internally it dispatches to `default_record_batch::estimate_batch_size_upper_bound` (v2-only); v0/v1 conservatively use the same upper bound (Java path goes through `LegacyRecord`, which is out of scope).

10. **`FutureRecordMetadata` accessors were added**: `create_timestamp`, `serialized_key_size`, `serialized_value_size`. Used by `ProducerBatch::metadata_for` to rebuild a `RecordMetadata` for the callback without going through `value()` (which would walk the chain — wrong here, the parent batch's metadata is what we want).

**Java ProducerBatchTest cases skipped:** None this milestone (all 11 translated). Note that `testCompleteExceptionallyWithNullRecordErrors` (the NPE branch) has no Rust mirror because the signature requires non-Option `ErrorsByIndex`; we test the equivalent fallthrough via `done_inner` directly. The v0/v1 split test branches in `testSplitPreservesMagicAndCompressionType` also have no fixture: Phase 3 only emits magic v2.
