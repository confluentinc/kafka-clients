---
name: Phase 6a Round 1 review-fix patterns
description: Cancellation-safe RAII guard, zero-fill audit, dead_code scoping for placeholder phases
type: project
---

Patterns learned addressing Critic 6's Phase 6a Round 1 (7 issues, 4 fixup commits).

**Cancellation-safe RAII for Java try/finally with .await inside.** When
the Java original wraps an `.await`-equivalent (`Condition.await(...)`)
in `try { ... } finally { remove }`, the Rust translation needs a
`Drop`-implementing guard struct so cleanup runs whether the future
completes, errors, or is cancelled (`abort()`, parent
`tokio::time::timeout`, losing `select!` arm). Plain post-await cleanup
DOES NOT RUN on future drop.

Hand-rolled guard shape:
```rust
struct WaiterGuard<'a> {
    state: &'a Mutex<State>,    // borrow of pool state
    waiter: &'a Arc<Notify>,    // identity-key for queue removal
    accumulated: i32,           // refunded on drop unless caller zeroes it
    buffer: Option<Vec<u8>>,    // returned on drop unless caller takes it
    armed: bool,                // disarmed on success
}
impl Drop for WaiterGuard<'_> {
    fn drop(&mut self) {
        let mut state = self.state.lock().unwrap();
        // 1. always: remove from queue
        // 2. if armed: refund + return buffer
        // 3. always: signal next waiter (Java's outer `finally` did this)
    }
}
```

Success path: `guard.buffer.take(); guard.accumulated = 0; guard.disarm();`
— extracts the success value AND zeroes refund-source AND flags drop
to skip the conditional cleanup. Drop still runs and does waiter
removal + signal-next, matching Java's outer `finally`.

**Cancellation regression test that ACTUALLY exercises the path.** A
test that says "cancellation" but uses `pool.close()` does not
exercise cancellation — it exercises the closed-flag check (a normal
return path). Real cancellation tests:

```rust
let pool_cancel = Arc::clone(&pool);
let cancel_handle = tokio::spawn(async move {
    let _ = tokio::time::timeout(Duration::from_millis(50),
                                  pool_cancel.allocate(2, 60_000)).await;
});
// wait for the timeout to fire (drops the inner future mid-await)
cancel_handle.await.unwrap();
// then assert queue drained / live waiters still woken
```

Or `JoinHandle::abort(); join.await;` for the more brutal abort path.

After abort, the runtime needs a tick before drop chains complete.
Spin-poll the assertion target with a 2s deadline, not a fixed sleep.

**The "leaked-ghost wakeup" stall pattern.** When `notify_one()`
targets a queue head that's a leaked `Arc<Notify>` (no listener), the
permit is stored on the dropped Notify and goes nowhere. Live waiters
behind the ghost only wake when their own `max.block.ms` elapses.
This is a real producer-side stall — Phase 6e's `KafkaProducer::flush`
with a deadline is the typical surface. Test fixture:

1. Fill the pool.
2. Spawn a cancellable allocator (use `tokio::time::timeout`).
3. Wait for it to enqueue.
4. Wait for the timeout to fire.
5. Spawn a "live" allocator on the same exhausted pool.
6. Wait for it to enqueue.
7. `pool.deallocate_full(buf)` — should signal the live waiter.
8. Assert the live waiter resolves Ok within 1s (not its own
   `block_time`).

Without a Drop guard, step 7's signal targets the leaked ghost and
the test fails at step 8 (live waiter hangs).

**ByteBuffer.clear() ≠ Vec::clear() + resize(0).** Java's
`ByteBuffer.clear()` is a position/limit reset, no bytes touched.
Rust's `Vec::clear()` sets `len = 0`, then `resize(N, 0)` writes N
zero bytes. On the producer send path that's a hot-path regression
for every `batch.size`-byte recycle.

The fix: keep pooled `Vec<u8>` blocks at `len == capacity ==
poolable_size` always. On deallocate, `unsafe { buf.set_len(capacity) }`
(no-op when invariant holds). On allocation hit, return as-is. The
`unsafe` is sound under the pool's invariant: callers don't shrink
buffers, and the `(len..capacity)` bytes were initialized by the
prior `vec![0u8; size]` allocation or by the caller's own writes.
Document the invariant + safety argument in a multi-line block, plus
a `debug_assert_eq!(buffer.len(), buffer.capacity())` to catch
caller misuse.

**`#![allow(dead_code)]` scope for placeholder phases.** Module-wide
`#![allow(dead_code)]` masks the warning that catches future-wired
methods that are silently uncalled. The reviewer's preference: the
allow stays per-FILE with a comment pointing at the phase that wires
it (`// Phase 6d (RecordAccumulator) wires this set.`). Precedent
files in this repo: `src/metadata.rs`,
`src/common/record/log_input_stream.rs`,
`src/common/record/byte_buffer_log_input_stream.rs`. The per-file
allow is no looser than module-wide for a single-file module, and
the phase-pointer comment forces deliberate review when the next
sub-phase lands. Field-level allows on read-elsewhere fields (e.g.
`BufferPool.time` which IS read at `time.nanoseconds()`) are stale —
remove them.

**Java `ByteBuffer.clear()` audit applies to all Vec-backed buffer
pools/builders.** When translating a class that recycles or clears
ByteBuffers on the send path, audit for analog Rust code that
zero-fills. The translation toolkit's instinct is `vec.clear();
vec.resize(N, 0)` — that's the WRONG translation for `ByteBuffer.clear()`.
The right translation is `unsafe set_len` or, if the caller doesn't
need length-tracking, `Box<[u8]>` of fixed size with a separate
position cursor.

**`error!("...{:#?}", e)` for SLF4J `log.error(format, ..., e)` parity.**
SLF4J detects a trailing throwable argument and appends the stack
trace. Rust's `log::error!` has no equivalent. Closest substitute:
`{:#?}` (pretty Debug) on the error. With `thiserror`-derived errors,
this surfaces the structured form including any `#[source]` chain.
`{}` (Display) only shows the leaf message — observability regression.
