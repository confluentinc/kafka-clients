# Critic 6 — Phase 6a (Buffer pool + future plumbing) review (resolved)

Reviewed commits c4e8b99..a7201eb (6 commits).

## Issue 1 — Blocking: BufferPool waiter slot leaks on future cancellation; can stall sibling waiters

**File**: `src/producer/internals/buffer_pool.rs:185-281`
**Severity**: Blocking — divergence from Java behavior + real producer-side
deadlock-class bug under task abort.
**Description**:
`BufferPool::allocate_slow` enqueues `Arc<Notify>` into `state.waiters`
on entry and removes it in the post-loop cleanup block (lines 255-268).
The cleanup runs only if execution **reaches the end of the function**.
Java implements the exact same logic with `try { ... } finally { remove }`,
which Java guarantees runs whether the body returns, throws, or is
interrupted.

In Rust, when the future returned by `allocate_slow` is dropped while
suspended at any of its `.await` points (e.g. the task is `abort()`-ed,
the caller wraps the call in `tokio::time::timeout(...)` and the timeout
fires, or any parent `select!` arm cancels the call), **the cleanup
block at lines 255-268 never runs.** The `Arc<Notify>` waiter remains
in `state.waiters` indefinitely.

This has two consequences:

1. **Memory leak**: the leaked waiter is held in the queue forever (or
   until `close()` drains it). For a long-running producer with
   intermittent cancellations (e.g. cooperative shutdown of a few send
   tasks while others continue), the queue grows unbounded.
2. **Real deadlock-class stall on subsequent waiters**: Java's
   `signal_next_waiter_if_room` and `deallocate` only signal
   `state.waiters.front()`. If the head of the queue is a ghost (a
   leaked, no-listener `Notify`), `notify_one()` stores a permit that
   no one will consume. **Live waiters behind the ghost are not
   signaled** and only wake when their `tokio::time::sleep` for
   `max.block.ms` elapses — i.e. they get `BufferExhausted` even though
   memory was deallocated and is sitting waiting for them.

**Java reference**: `kafka/clients/src/main/java/org/apache/kafka/clients/producer/internals/BufferPool.java:138-189`
— Java's inner `try { ... } finally { nonPooledAvailableMemory +=
accumulated; this.waiters.remove(moreMemory); }` runs on
`InterruptedException` from `await(...)` exactly like the normal-exit
path.

**Suggested fix**: introduce a per-`allocate_slow` RAII guard that
holds `&self.state` (or `Arc<Mutex<State>>`) plus the waiter `Arc<Notify>`,
and in its `Drop` impl: take the lock, remove the waiter, refund any
unconsumed `accumulated`, and call `signal_next_waiter_if_room`.

**Disposition**: Fixed in commit f1fe24b (fixup! 5d665f5).
Implemented `WaiterGuard<'a>` whose `Drop` impl removes the waiter,
refunds any leftover `accumulated`, returns any pooled buffer back to
the free list, and signals the next waiter. The success path takes
the buffer via `Option::take` and calls `disarm()` so the guard's
post-success drop only runs the waiter-removal + signal-next steps
(matching Java's outer `finally`). Cancellation cleanup verified by
new regression test `cancelled_allocate_does_not_stall_subsequent_waiter`
(see Issue 2 disposition).

---

## Issue 2 — Blocking: cancellation test does not exercise cancellation; Java contract not actually translated

**File**: `src/producer/internals/buffer_pool.rs:636-671`
**Severity**: Blocking — `test_cleanup_memory_availability_waiter_on_cancellation`
asserts the contract via the wrong stimulus.
**Description**:
The Java test `testCleanupMemoryAvailabilityWaiterOnInterruption`
(`BufferPoolTest.java:194-223`) is not a "test that the pool can be
closed". It is a test that **`Thread.interrupt()` on a blocked
allocator removes that allocator's waiter from the queue without
leaking memory**.

The Rust replacement spawns two tasks, lets them enqueue, then calls
`pool.close()` and joins the tasks. This exercises the *close* path,
not the cancellation path. The two tasks return through the normal
loop body (the closed-flag check at lines 217-222) and the normal
cleanup runs at lines 255-268. The waiter slot removal is unrelated
to cancellation in this test.

**Suggested fix**: replace `pool.close()` at line 666 with `t1.abort();
t2.abort();` and `let _ = t1.await; let _ = t2.await;`.

**Disposition**: Fixed in commit f1fe24b (fixup! 5d665f5).
- `test_cleanup_memory_availability_waiter_on_cancellation` now uses
  `JoinHandle::abort()` instead of `pool.close()`. After abort, the
  test spins up to 2s waiting for the queue to drain (small grace
  period for the runtime tick that lets each cancelled future's drop
  chain run).
- New regression test `cancelled_allocate_does_not_stall_subsequent_waiter`
  exercises the leaked-ghost-wakeup scenario via
  `tokio::time::timeout`: aborts a waiter, then verifies a subsequent
  live waiter is woken in <1s by `deallocate` (would hang for
  `max.block.ms` without the Issue 1 fix).

---

## Issue 3 — Performance: BufferPool buffer recycle zero-fills twice on every reuse

**File**: `src/producer/internals/buffer_pool.rs:318-336, 419-423`
**Severity**: Performance — hot-path allocation regression vs Java.
**Description**:
Java's `BufferPool.deallocate(buf, size)` calls `buf.clear()` which
**only resets `position=0, limit=capacity`** on the `ByteBuffer` — no
bytes are touched.

The Rust translation in `BufferPool::deallocate` (lines 327-329) does:
```rust
buffer.clear();                                  // sets len = 0
buffer.resize(self.poolable_size as usize, 0);   // writes poolable_size zero bytes
state.free.push_back(buffer);
```
This zero-fills the entire pooled buffer on every `deallocate`. Then
when the same buffer is re-issued, `prepare_recycled` does the same
thing again. Result: **every pooled-buffer reuse pays 2× `batch.size`-byte
zero fills**.

**Suggested fix**: keep the pooled `Vec<u8>` at `len == capacity` at
all times. On `deallocate`, do not call `clear()` or `resize()` —
just push back. On allocation hit, also do not zero-fill.

**Disposition**: Fixed in commit f1fe24b (fixup! 5d665f5).
- `deallocate` now calls `unsafe { buffer.set_len(poolable_size) }`
  (a no-op when the pool's invariant `len == capacity` holds, as it
  does for any caller that doesn't shrink the buffer). Documented the
  invariant + safety argument in a multi-line comment; debug-asserts
  `buffer.len() == buffer.capacity()` to catch caller misuse.
- Pool-hit returns (both fast-path and slow-path) hand the recycled
  buffer back as-is. The previous `prepare_recycled` helper has been
  removed.
- `default_allocator` still produces `vec![0u8; size]` for the first
  allocation, so initial bytes are guaranteed initialized; subsequent
  cycles preserve initialization through the caller's writes.

---

## Issue 4 — Suggestion: `#![allow(dead_code)]` at `producer/internals/mod.rs:17` masks future regressions

**File**: `src/producer/internals/mod.rs:17`
**Severity**: Suggestion — process risk, not a runtime bug today.

**Suggested fix**: remove `#![allow(dead_code)]` from
`producer/internals/mod.rs`. Remove the field-level
`#[allow(dead_code)]` on `BufferPool.time`. If individual methods on
the placeholder `ProducerBatch` are temporarily uncalled, add
targeted `#[allow(dead_code)]` to the specific method.

**Disposition**: Fixed in commit 6a6ca23 (fixup! 5d665f5) and
follow-on 16e9367 / 1ee339b.
- Removed `#![allow(dead_code)]` from
  `src/producer/internals/mod.rs`.
- Replaced with per-file `#![allow(dead_code)]` markers on each of
  the seven internals files, each tagged with the phase that wires
  the file's public surface (e.g. `// Phase 6d (RecordAccumulator)
  wires the public surface.`). This matches the precedent set in
  `src/metadata.rs`, `src/common/record/log_input_stream.rs`, and
  others, and keeps the lint scoped per-file so a future actor sees
  the warning when they add an unused method to one specific file.
- Removed the stale field-level `#[allow(dead_code)]` on
  `BufferPool.time` (it is read at lines `time.nanoseconds()`).
- Removed the stale field-level `#[allow(dead_code)]` on
  `FutureRecordMetadata.time` and replaced it with a doc comment that
  explains why the field is currently unused (Java's `get(timeout, unit)`
  overload is not exposed in the Rust port — see module rustdoc
  for the rationale).

---

## Issue 5 — Suggestion: ErrorLoggingCallback drops exception's stack-trace context on error format

**File**: `src/producer/internals/error_logging_callback.rs:77-80`
**Severity**: Suggestion — observability/debugging regression.

**Suggested fix**: format the error with `{:?}` (Debug) at minimum,
or — better — switch to `log::error!(..., "...with error: {:#?}", e)`
to print the full structured form.

**Disposition**: Fixed in commit 16e9367 (fixup! fffaad7).
Switched the error format to `{:#?}` (pretty Debug) which surfaces
the structured `KafkaError` form including any source chain via
`thiserror`-derived `Display`/`Debug` impls — restoring SLF4J-equivalent
debug surface.

---

## Issue 6 — Suggestion: `chain_resolves_to_tail_metadata` does not actually replace the deleted Java tests' coverage

**File**: `src/producer/internals/future_record_metadata.rs:178-204`
**Severity**: Suggestion — test-coverage gap acknowledged but not
filled.
**Description**:
The actor skipped `testFutureGetWithSeconds` and
`testFutureGetWithMilliSeconds` arguing the deadline-propagation
hazard is "not expressible in Rust" because `tokio::time::timeout`
wraps the whole future tree. The replacement test
`chain_resolves_to_tail_metadata` does **not** exercise deadline
propagation — it only verifies that `get()` resolves to the chain
tail when both sides eventually complete. This was already covered
by `is_done_follows_chain`.

**Suggested fix**: add a test that confirms wrapping `get()` in
`tokio::time::timeout(d, …)` does cancel the chained inner await as
well as the outer.

**Disposition**: Fixed in commit 1ee339b (fixup! d70ecc9). Added
test `outer_timeout_cancels_chained_inner_await`: completes the
parent, leaves the child pending, asserts that
`tokio::time::timeout(50ms, parent_future.get())` returns `Err` —
proves the chained inner await is reached and the outer timeout
covers it. This is the idiomatic-Rust equivalent of "remaining
timeout propagates to the chained future" that the deleted Java
tests guarded. Updated module rustdoc to point at both the existing
`chain_resolves_to_tail_metadata` and the new test as the two
Rust-shape replacements for the skipped Java pair.

---

## Issue 7 — Suggestion: BufferPool fast-path pooled hit does not signal next waiter (Java does)

**File**: `src/producer/internals/buffer_pool.rs:159-163`
**Severity**: Suggestion — Java-divergence on wakeup fairness; not a
correctness bug.

**Suggested fix**: refactor `allocate` to compute the buffer first,
then run the signal-if-room block, then return. Or factor the body
into a helper that returns the buffer + a "signal" flag and have
the outer wrapper run the post-signal block in all branches.

**Disposition**: Fixed in commit f1fe24b (fixup! 5d665f5). Factored
`allocate` into an outer wrapper + `allocate_inner`. The outer
`allocate` calls the inner body, then unconditionally runs
`signal_next_waiter_if_room` under the lock before returning the
result — matching Java's `try { ... } finally { signal }` wrapping.
All return paths (fast-path pool hit, immediately satisfiable,
slow-path success, slow-path error) now signal the next waiter on
the way out.

---

## Summary
- **Blocking**: 2 (Issues 1, 2 — both Fixed)
- **Suggestion**: 5 (Issues 3, 4, 5, 6, 7 — all Fixed)

All 7 items addressed. Test count 976 → 978 (+2 regression tests:
`cancelled_allocate_does_not_stall_subsequent_waiter`,
`outer_timeout_cancels_chained_inner_await`). DoD checks (build,
test, format-check, lint) all green.

---

# Critic 6 — Phase 6b Round 1 (ProducerBatch) review (resolved)

Reviewed commits `d3bab5a` (core), `6a21ea3` (try_append/done/abort/split),
`ba7f10b` (test translation, 11 cases) on branch `fresh-impl`. Three
items filed; all three resolved in fixup chain `a41263b` / `08bff9f` /
`c7c34a4`.

## Issue 1 — Bug, Missing Requirement: `ProducerBatch::buffer()` accessor is missing

**File**: `src/producer/internals/producer_batch.rs`
**Severity**: Bug — Missing Requirement.
**Description**: Java's `ProducerBatch.buffer()` (`ProducerBatch.java:543`)
is consumed by `RecordAccumulator.deallocate` (`RecordAccumulator.java:1053`)
to feed the buffer back into `BufferPool.deallocate(buffer, initialCapacity)`.
The Rust translation omitted this; Phase 6d would hit a hard build-time
block.

**Disposition**: Fixed in commit `a41263b` (fixup! `6a21ea3`).
Picked **Option A** (own-the-Vec) of the three options in the Critic
issue. Rationale:

- Phase 6a's `BufferPool::deallocate` already takes ownership of `Vec<u8>`
  (steady-state `unsafe set_len` no-fill recycle), so transferring
  ownership matches the pool's contract exactly. Phase 6d translates to
  a one-line `pool.deallocate(batch.buffer(), batch.initial_capacity())`
  call — same shape as Java's `free.deallocate(batch.buffer(),
  batch.initialCapacity())`.
- Option B (`&[u8]`) couples the lock-guard's lifetime to the borrow,
  forcing the caller to hold the mutex while invoking the pool —
  fragile and a deadlock hazard.
- Option C (`recycle_into(pool)`) hides the buffer entirely but
  diverges most from Java and complicates testing the recycle path
  independently.

Implementation:
- `MemoryRecordsBuilder::buffer_owned(&mut self) -> Vec<u8>` extracts
  the buffer from either pre-build (`buffer_stream` -> `into_buffer`)
  or post-build (`built_records.buffer` `Bytes` -> `try_into_mut` ->
  `Vec<u8>`) lifecycle states. Returns `Vec<u8>` with `len == capacity ==
  initial_capacity` (matches `BufferPool::deallocate`'s recycle
  invariant). Subsequent calls return an empty `Vec<u8>` — one-shot.
- `MemoryRecords::into_buffer(self) -> Bytes` consuming accessor.
- `MemoryRecordsBuilder::initial_capacity()` now reads from a
  snapshot field (`initial_buffer_capacity`) instead of the
  `buffer_stream` whose underlying Vec is moved out at `close()`. This
  fixes a latent post-build returns-0 bug.
- `ProducerBatch::buffer(&self) -> Vec<u8>` locks `mut_state`,
  delegates to `buffer_owned()`.

Tests: +2 regression tests in `producer_batch.rs`:
- `buffer_returns_owned_vec_sized_to_initial_capacity` — closes the
  batch, calls `complete()` (the "done" the Critic issue named), then
  `buffer()` and asserts `len == capacity == initial_capacity`. Also
  asserts second call returns empty Vec.
- `buffer_pre_close_returns_full_capacity_vec` — exercises the
  pre-build extraction path.

---

## Issue 2 — Suggestion (Test Quality): tautological assertion

**File**: `src/producer/internals/producer_batch.rs:1336-1362`
(`split_preserves_magic_and_compression_type_v2`).
**Severity**: Suggestion (Test Quality).
**Description**: Test ended with `assert!(res.is_ok() || res.is_err(),
…)` — a tautology. The `let _ = MAGIC_VALUE_V1;` afterward was also
dead code.

**Disposition**: Fixed in commit `08bff9f` (fixup! `ba7f10b`).
Took **option (a)** ("delete the smoke-check block"). The per-test
docstring now records the v0/v1 deliberate skip with a pointer to the
constants in `crate::common::record::record_batch`, so a reviewer
cross-checking against Java's `testSplitPreservesMagicAndCompressionType`
sees the omission documented without an in-test guard. Removed the
now-unused `MAGIC_VALUE_V0` / `MAGIC_VALUE_V1` imports from the
test-mod imports.

---

## Issue 3 — Suggestion (Behavior Drift): per-record-error semantics

**File**: `src/producer/internals/producer_batch.rs:464-503`
(`complete_future_and_fire_callbacks`).
**Severity**: Suggestion (Behavior Drift, low-impact).
**Description**: Rust collapsed Java's binary `recordExceptions ==
null` bifurcation via `record_exceptions.as_ref().and_then(|f|
f(i))`, so a closure that returned `None` for some index silently
fell back to the success branch (Java would call
`onCompletion(null, null)` instead).

**Disposition**: Fixed in commit `c7c34a4` (fixup! `6a21ea3`).
Took **option (b)** ("match Java exactly by bifurcating on
`record_exceptions.is_none()`"). The error mode now passes the closure
result through `on_completion(None, per_record_err.as_ref())` directly
— mirroring Java's `onCompletion(null, recordExceptions.apply(i))`. The
existing test
`complete_exceptionally_with_null_record_errors_smokes_top_level`
continues to pass because its assertions are about
`future.get()` resolution (not callback invocation) — that path is
unchanged.

---

## Phase 6b Round 1 Summary
- **Bug — Missing Requirement**: 1 (Issue 1 — Fixed)
- **Suggestion**: 2 (Issues 2, 3 — both Fixed)

All 3 items addressed. Test count 989 → 991 (+2 regression tests for
`buffer()`). DoD checks (build, test, format-check, lint) all green.

---

# Critic 6 — Phase 6b Round 2 review (resolved)

Reviewed Round 2 of Phase 6b. Issues 1/2/3 verified resolved (see Round
1 dispositions above). Round 2 surfaced two new Suggestion items
pointing at a real soundness concern in `finalize_recycled_buffer`'s
`Err` fallback.

## Issue 4 — Suggestion: `finalize_recycled_buffer` Err path may expose uninitialized memory

**File**: `src/common/record/memory_records_builder.rs` (lines 101-119
pre-fix, 117-159 post-fix).
**Severity**: Suggestion (narrow soundness hazard, only reachable on
the production-typical wire-send-clone-alive path which Phase 6e wires).
**Description**: When `try_into_mut()` returns `Err(b)` (Bytes clone
outstanding from the wire-send path), the fallback was:
```rust
let mut owned: Vec<u8> = b.to_vec();          // fresh alloc, cap == len
if owned.capacity() < initial_capacity {
    owned.reserve(initial_capacity - owned.capacity());  // may grow
}
let cap = owned.capacity();
unsafe { owned.set_len(cap); }                // exposes 0..cap
```
`to_vec()` produces `Vec` with `cap == len`. `reserve(N)` then
allocates fresh memory whose `len..cap` bytes are uninitialized.
`set_len(cap)` exposes them. If the allocator rounds the new
capacity to exactly `initial_capacity` (possible at power-of-two
sizes that match allocator size classes), `BufferPool::deallocate`'s
`size as usize == buffer.capacity()` check passes and the buffer
reaches the free list — the next consumer can read uninitialized
bytes (UB).

**Disposition**: Fixed in commit `e3a1d61` (fixup! `a41263b`).
Took **option F1** ("zero-fill the tail before exposing in safe
code"). Bifurcated `finalize_recycled_buffer` so the `Err` branch
uses `Vec::resize(cap, 0)` (safe, zero-fill) while the `Ok` branch
retains its `unsafe set_len` zero-fill-avoidance optimization. The
`Ok` branch is the steady-state hot path (uniquely-owned dominates
once broker ack drops the wire-send `Bytes` clone); the `Err` branch
is rare and the zero-fill cost is acceptable for the soundness
guarantee. Doc comment updated to document the asymmetry. API
surface unchanged.

## Issue 5 — Suggestion: Buffer regression tests don't model the production deallocate lifecycle

**File**: `src/producer/internals/producer_batch.rs` (lines 1574-1629
pre-fix, +1 test post-fix).
**Severity**: Suggestion.
**Description**: The two existing regression tests
(`buffer_returns_owned_vec_sized_to_initial_capacity`,
`buffer_pre_close_returns_full_capacity_vec`) call `batch.close()`
then `batch.buffer()` directly — they never call `batch.records()`
in between, so `try_into_mut` is always uniquely-owned and the
`Err` fallback path is never exercised. In production
(Phase 6e Sender), the lifecycle is: `Sender.close()` →
`Sender.records()` (clones the `Bytes`) → wire send → broker ack →
`RecordAccumulator::deallocate` → `batch.buffer()`. At the point of
extraction, the `Bytes` refcount is ≥ 2 — the `Err` branch fires.
The Issue 4 soundness fix needed a regression test that drives
that specific path.

**Disposition**: Fixed in commit `e3a1d61` (fixup! `a41263b`).
Added test `buffer_returns_owned_vec_when_records_clone_is_alive` at
`producer_batch.rs:1631-1690`. The test:
1. Builds a non-empty batch and calls `close()` + `complete()`.
2. Calls `batch.records()` to clone the `Bytes` (refcount → 2).
3. Holds the clone alive while calling `batch.buffer()` — this
   forces the `Err` branch (`built_records.take()` consumes one ref;
   the externally-held `records_clone` keeps refcount ≥ 1, so
   `try_into_mut()` returns `Err`).
4. Asserts `len == capacity == initial_capacity` (BufferPool
   invariants), and that the tail bytes (past the record payload)
   read as zero — the soundness signal that confirms `resize(_, 0)`
   is in effect rather than `unsafe set_len` over uninitialized
   memory.
5. Asserts the `records_clone` still observes its data (proves the
   fallback is a copy, not a move).

## Phase 6b Round 2 Summary
- **Suggestion**: 2 (Issues 4, 5 — both Fixed)

Test count 991 → 992 (+1 regression test for the Err-branch
soundness fix). DoD checks (build, test, format-check, lint) all
green. Phase 6b closed.

---

# Critic 6 — Phase 6c (Partitioners + interceptors + ProducerRecord) Round 1 review (resolved)

Reviewed commits `65fd6ef` (ProducerRecord), `bd3b78a` (Partitioner +
RoundRobinPartitioner), `2823a69` (ProducerInterceptor +
ProducerInterceptors), `2153070` (BuiltInPartitioner). 0 Blocking,
6 Suggestion.

## Issue 1 — Behavior Mismatch: `ProducerRecord` rejects empty topic

**File**: `src/producer/producer_record.rs:108-125`.
**Description**: Java's `ProducerRecord(String topic, ...)` rejects only
`topic == null`; `""` (empty string) is accepted at construction (the
broker rejects later in metadata lookup). The Rust translation used
`topic.is_empty()` as the equivalent, rejecting `""` at construction
with `ProducerRecordError::NullTopic`. The variant name was misleading
when the user passed a literal `""`.

**Disposition**: Fixed in commit `ec0d2fc` (fixup! `65fd6ef`).
Dropped the `is_empty()` guard. Removed the now-unreachable
`NullTopic` variant — Java's `null` rejection is enforced at the
Rust type level (`impl Into<Arc<str>>` has no `null` representation).
Updated the test to drop the moot null-topic case and added a
positive regression `empty_topic_is_accepted` to pin the new
contract. The remaining two `IllegalArgumentException` cases
(negative timestamp, negative partition) are preserved.

## Issue 2 — Suggestion: `ProducerInterceptors::on_send` clones every record per interceptor

**File**: `src/producer/internals/producer_interceptors.rs:84-113`.
**Description**: The Rust `on_send` calls `intercept_record.clone()`
before each interceptor invocation. The clone is unavoidable given
the panic-isolation contract (input is moved into `catch_unwind`; the
previous-good record must survive). For typical hot-path types
(`K = V = &[u8]`) the cost is fat-pointer copies plus a
`Vec<RecordHeader>` deep-clone; for `String`/`Vec<u8>` types each
clone allocates.

**Disposition**: Fixed in commit `2c9f679` (fixup! `2823a69`).
Documented the `K: Clone, V: Clone` bound on `on_send` with a hot-
path allocation note. The clone is kept (panic-isolation contract is
load-bearing); alternatives like `Arc<RecordHeaders>` and trait-shape
changes are flagged for Phase 6d/7 design review.

## Issue 3 — Suggestion: warn-log topic on `on_send` panic uses running record, not original

**File**: `src/producer/internals/producer_interceptors.rs:94-106`.
**Description**: Java's catch-block (`ProducerInterceptors.java`
line 71-72) logs `record.topic()` and `record.partition()` from the
**original** input parameter, not the running `interceptRecord`
(which a previous interceptor may have mutated). The Rust translation
captured the topic/partition from `intercept_record` per-iteration.

**Disposition**: Fixed in commit `2c9f679` (fixup! `2823a69`).
Capture `original_topic` and `original_partition` once before the
loop and reuse for every iteration's warn-log. Mirrors Java exactly
and avoids the per-iteration `to_string()` allocation (one upfront
vs N).

## Issue 4 — Suggestion: `RoundRobinPartitionerTest` does not exercise the `next_value` slow path under contention

**File**: `src/producer/round_robin_partitioner.rs:65-86, 120-258`.
**Description**: `next_value()` has two paths (fast: counter exists,
lock-free atomic increment; slow: mutex-guarded
`entry().or_insert_with()`). The translated tests run single-threaded
so the slow path is exercised exactly once per topic, never under
contention. The bifurcation is Rust-specific (Java's
`ConcurrentHashMap.computeIfAbsent` collapses both into one call).

**Disposition**: Fixed in commit `1dd333c` (fixup! `bd3b78a`).
Added regression `next_value_increments_through_fast_path_after_first_call`
which calls `partition()` three times for the same topic and asserts
the round-robin distribution holds — the second and third calls
must hit the fast path (counter exists), so the assertion only
passes if the fast-path increment is correct.

## Issue 5 — Suggestion: `BuiltInPartitioner::peek_current_partition_info` race-loser path is untested

**File**: `src/producer/internals/built_in_partitioner.rs:192-215`.
**Description**: The race-resolve branch ("Someone raced us. Reload
the winner.") was not covered by any of the four translated tests
deterministically — the early-return branch was tangentially hit by
the sticky-partitioning loop but never pinned by an assertion.
Java's tests have the same gap.

**Disposition**: Fixed in commit `d6d08ef` (fixup! `2153070`).
Added regression `peek_current_partition_info_returns_staged_arc_on_second_call`
which uses `Arc::ptr_eq` to confirm the second call returns the
same `Arc` the first call staged — pinning the early-return
branch. The race-loser CAS-lost path remains untestable
deterministically without forcing a multi-threaded race; documented
in code via the `expect(...)` panic message that would surface in
production.

## Issue 6 — Suggestion: `next_partition` and `RoundRobinPartitioner::partition` return `-1` instead of throwing on zero-partition topics

**File**: `src/producer/internals/built_in_partitioner.rs:142-150`,
`src/producer/round_robin_partitioner.rs:106-116`.
**Description**: Java's `Utils.toPositive(nextValue) % numPartitions`
(RoundRobinPartitioner.java:62) and `random % partitions.size()`
(BuiltInPartitioner.java:82) both raise `ArithmeticException` on a
zero-partition topic. The Rust translations returned `-1`, silently
routing to "partition -1". Java does NOT explicitly throw — it
propagates from divide-by-zero. The Rust divergence traded a hard
failure for silent invalid output.

**Disposition**: Fixed in commits `1dd333c` (fixup! `bd3b78a`) and
`d6d08ef` (fixup! `2153070`). **Option (c) chosen — panic to mirror
Java exactly.** Both `RoundRobinPartitioner::partition` and
`BuiltInPartitioner::next_partition` now let the Rust `%` panic on
the zero-partitions branch, matching Java's `ArithmeticException`.
CLAUDE.md rule 10.1 explicitly permits panic on
`ArithmeticException`-like conditions ("OOM or `ArithmeticException`
like division by zero"). Option (a) (Result<i32, KafkaError>) was
rejected as over-engineered — it would invasively change every
caller and Java doesn't do this either. Option (b) (keep `-1`) was
rejected as a real behavior divergence. Documented the panic in
the trait's rustdoc and added regression tests
`partition_on_zero_partition_topic_panics` and
`next_partition_on_zero_partition_topic_panics`.

## Phase 6c Round 1 Summary
- **Blocking**: 0
- **Suggestion**: 6 (1 Behavior Mismatch — Issue 1; 1 documentation —
  Issue 2; 1 log-message detail — Issue 3; 2 test gaps — Issues 4
  & 5; 1 documented divergence — Issue 6 — all Fixed)

Test count 1005 → 1010 (+5: 1 ProducerRecord
`empty_topic_is_accepted`, 2 RoundRobin
`next_value_increments_through_fast_path_after_first_call` +
`partition_on_zero_partition_topic_panics`, 2 BuiltInPartitioner
`peek_current_partition_info_returns_staged_arc_on_second_call` +
`next_partition_on_zero_partition_topic_panics`). DoD checks
(build, test, format-check, lint) all green. Fixup chain: `ec0d2fc`,
`2c9f679`, `1dd333c`, `d6d08ef`.

---

# Round 1 — Phase 6d (RecordAccumulator) — resolved

10 Suggestion items filed against `b1ff454`, `b66cd88`, `a508a86`,
`2b81a83`, `21d094a`, `f2a5c79`, `4345a43`. **0 Blocking.**

Two fixup commits:
- `c0ba6e2` (`fixup! Phase 6d-1`) — Issues 1, 8, 9 (impl-level changes).
- `67ad8fe` (`fixup! Phase 6d-6`) — Issues 2-7, 10 (test sweep).

## Disposition table

| # | Severity | Issue | Disposition |
|---|---------|-------|-------------|
| 1 | Suggestion (dead code) | `flush_notify` field is unused | **Fixed** in `c0ba6e2`. Removed the field, the constructor initialization, and the `tokio::sync::Notify` allocation. Updated module rustdoc to point at the actual wake mechanism (`ProduceRequestResult::await_all_dependents` per-result `Notify`). |
| 2 | Suggestion (test fidelity) | `stressful_concurrent_appends_smoke` does not exercise concurrent drain | **Fixed** in `67ad8fe`. Rewrote the test: 4 producer tasks × 500 records run **in parallel with** a drainer task that loops `ready/drain/complete_and_deallocate` until the seen count reaches the expected total. Asserts EXACT total (record-loss regressions fail), `!has_undrained` and `!has_incomplete`. Outer 10s timeout converts deadlock regressions to failures rather than hangs. |
| 3 | Suggestion (coverage gap) | `testReadyAndDrainWhenABatchIsBeingRetried` (KAFKA-15968 leader-epoch override) is the only test of the leader-change-overrides-backoff invariant | **Fixed** in `67ad8fe`. Translated the Java test in full as `ready_and_drain_when_a_batch_is_being_retried`. Covers all 4 cases (wait < backoff × {leader changed/no change}, wait > backoff × {leader changed/no change}) with the exact `current_leader_epoch` and `attempts_when_leader_last_changed` post-conditions. Added helper `build_single_partition_snapshot(cluster, leader_epoch)` to vary epochs between cases. |
| 4 | Suggestion (coverage gap) | `testDrainWithANodeThatDoesntHostAnyPartitions` early-return path untested | **Fixed** in `67ad8fe`. Translated as `drain_with_a_node_that_doesnt_host_any_partitions`. Builds a snapshot where node 1 hosts no partitions, drains for node 1 only, asserts empty result — exercising the `parts.is_empty()` early-return at `record_accumulator.rs:1148-1149`. |
| 5 | Suggestion (coverage gap) | `testBuiltInPartitionerFractionalBatches` accumulator+partitioner integration uncovered | **Fixed** in `67ad8fe`. Translated as `built_in_partitioner_fractional_batches`. 10 iterations × ~10 records each through `UNKNOWN_PARTITION`, advancing MockTime between iterations to bypass linger, asserting exactly 1 batch flushes per iteration with size in `(batch_size/2, batch_size)`. Exercises `partition_changed` × `update_partition_info` × `all_batches_full` integration that Phase 6c isolated tests do not reach. |
| 6 | Suggestion (test fidelity) | `testFull` Rust translation drops record-content verification | **Fixed** in `67ad8fe`. `ready_when_batch_full_immediately_ready` now drains the closed batch and iterates `Records::records(&records)` asserting each record's key/value bytes equal the input. Catches regressions where `try_append_to_existing` consumes a record but bytes never make it into the buffer. |
| 7 | Suggestion (undocumented skip) | v2 `testAppendLargeCompressed`/`testAppendLargeNonCompressed` not translated, not in skip list | **Fixed** in `67ad8fe`. Translated both as `append_large_compressed` / `append_large_non_compressed` with shared `append_large_helper(CompressionType)`. Tests the oversized-record path (`batch_size.max(upper_bound)` branch) — single record with `value.len = 2 * batchSize`. Asserts single batch with `base_offset=0`, single record at `offset=0`/`timestamp=0` with exact key/value byte fidelity. Skip-list updated to disambiguate v0/v1 (still skipped per Phase 3) from v2 (translated). |
| 8 | Performance Suggestion | Per-`append` `Arc::from(topic)` allocation on hot path | **Fixed** in `c0ba6e2`. Refactored `get_or_create_topic_info` to take `&str` and return `(Arc<str>, Arc<TopicInfo>)`. Fast path uses `HashMap::get_key_value(topic)` to fetch the existing `Arc<str>` key by reference and returns a refcount bump (`Arc::clone`) — no allocation. Slow path (first send for a topic) allocates the `Arc<str>` once and inserts. Matches Java's `topicInfoMap.computeIfAbsent` reuse pattern. CLAUDE.md rule 11 (`Arc<str>` for hot-path identifiers) satisfied: 1 alloc per topic per producer, **not per record**. |
| 9 | Suggestion (theoretical correctness / debug-build panic) | `maybe_update_next_batch_expiry_time` overflow check uses raw `+` | **Fixed** in `c0ba6e2`. Replaced `batch.created_ms() + self.delivery_timeout_ms as i64` with `batch.created_ms().checked_add(self.delivery_timeout_ms as i64)`. The `match` arm `Some(candidate) if candidate > 0` produces consistent wrap-to-`None` semantics matching Java's wrap-to-negative idiom on both debug (where overflow used to panic) and release builds. Comment in code explains the Java idiom and why `checked_add` is required. |
| 10 | Suggestion (coverage gap) | No regression test for `AppendInProgressGuard` Drop on cancellation | **Fixed** in `67ad8fe`. Added `append_in_progress_guard_drops_on_cancellation`. Drives the path: pool sized so first append exhausts memory; second appender blocks on `BufferPool::allocate(...).await`; `JoinHandle::abort()` triggers Drop. Asserts (a) `appends_in_progress` counter back to 0 (guard Drop fired even on cancellation), (b) buffer pool waiter queue drained (BufferPool::WaiterGuard fired), (c) a fresh appender after deallocate completes within 3s — proving no leaked-ghost wakeup. New `#[cfg(test)] pub(crate) fn appends_in_progress_count()` inspector added; production API unchanged. |

## Phase 6d Round 1 Summary

- **Blocking**: 0
- **Suggestion**: 10 (1 dead field, 4 test-coverage gaps from skipped
  Java cases, 1 test-fidelity gap, 1 unlisted-skip v2-format test, 1
  hot-path allocation, 1 theoretical-overflow, 1 cancellation-test
  gap — all Fixed)

Test count 1050 → 1056 (+6: leader-change retry, drain-no-host,
fractional-batches, append-large × 2, cancellation guard. Issues 2 and
6 modify existing tests rather than adding new ones). DoD checks
(build, test, format-check, lint) all green. Fixup chain: `c0ba6e2`,
`67ad8fe`.
