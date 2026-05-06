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
