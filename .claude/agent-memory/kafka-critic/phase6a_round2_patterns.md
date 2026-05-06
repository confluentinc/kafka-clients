---
name: Phase-6a Round-2 verification patterns
description: Verified-good fix shapes from Phase 6a Round 2 — RAII WaiterGuard for cancellation cleanup, unsafe set_len for zero-fill avoidance, signal-on-exit refactor for fairness; new-defect-scan checklist for these idioms
type: feedback
---

Phase 6a Round 2 verified the BufferPool fixups; below are the verified-good
shapes and the new-defect checks that mattered for each.

## Verified-good shapes — carry forward to future RAII-cancellation reviews

### RAII guard for Java try/finally → Rust async-cancellation safety

For any allocate_slow-style function that:
1. Enqueues a waiter in shared state under a lock
2. Awaits a notification
3. Cleans up the waiter slot in a Java `try { ... } finally { remove }`

The Rust translation needs an RAII guard whose Drop runs the same
cleanup. Verified-good guard shape (`buffer_pool.rs:498-576`):

- `Guard` borrows `&Mutex<State>` + `&Arc<Notify>` (lifetime-tied to
  the parent function so it can't outlive the state).
- Holds mutable `accumulated`/`buffer` fields the loop body updates
  via `guard.field = ...`.
- `armed: bool` flag distinguishes success from cancellation; success
  path calls `guard.disarm()` then sets accumulated=0 + takes buffer
  via Option::take.
- `Drop::drop` always: (1) takes the lock (sync, no await), (2)
  removes the waiter by `Arc::ptr_eq` identity match (NOT just any
  waiter), (3) signals next waiter unconditionally. The armed-only
  branch refunds accumulated + returns pooled buffer.

Verification checklist:
- Construct guard IMMEDIATELY after enqueue, BEFORE any `.await`.
- Drop must use sync mutex `lock().unwrap()` — never `.await`-able
  primitives. CLAUDE.md rule 9.6 holds because Drop is synchronous.
- Removal by identity (`Arc::ptr_eq`), not by any matching waiter, so
  multiple concurrent guards don't remove each other's slots.
- Mentally simulate: future polled once (waiter enqueued), dropped
  before next poll → does Drop fire and clean up? Walk the
  `tokio::time::timeout(d, allocate(...))` scenario explicitly.

### `unsafe set_len` for zero-fill avoidance on Vec recycle

When Java's `ByteBuffer.clear()` (position/limit reset only) maps to
Rust where `Vec::clear() + resize(N, 0)` would zero-fill on every
recycle. Verified-good idiom (`buffer_pool.rs:401-408`):

```rust
debug_assert_eq!(buffer.len(), buffer.capacity(), "...invariant...");
unsafe { buffer.set_len(self.poolable_size as usize); }
```

Verification checklist:
- The `debug_assert` catches invariant violations in debug builds.
- The SAFETY comment must justify two scenarios: (a) the no-op case
  when `len == capacity` already; (b) the rare case if invariant is
  violated, where bytes from `len..capacity` MUST have been written
  by some prior code path (initial `vec![0u8; size]` or prior
  caller's writes). If the allocator hook could return a `Vec` with
  `len < capacity` and uninitialized memory in `len..capacity`, this
  is UB.
- Cross-check all allocator hooks: `default_allocator`, test hooks.
  All must return `len == capacity` — usually `vec![0u8; size]` or
  `Vec::new()` (capacity 0, falls into non-pooled branch).
- The `pub(crate)` visibility constrains the invariant contract to
  in-crate callers — acceptable for producer-internal types.
- Worth noting (Suggestion-level, not Blocking): the
  `ByteBufferAllocator` type's doc comment may not explicitly state
  the `len == capacity` contract. Filed as documentation gap, not a
  defect.

### Signal-on-exit refactor for Java try/finally fairness

When Java wraps the function body in `try { ... } finally { signal }`,
the Rust idiom is to factor into outer wrapper + inner body. Outer
wrapper calls inner, then runs the signal step under the lock,
returning the inner's result. Verified-good shape
(`buffer_pool.rs:176-198`):

```rust
pub async fn allocate(&self, ...) -> Result<...> {
    // sync precondition checks
    let outcome = self.allocate_inner(...).await;
    {
        let mut state = self.state.lock().unwrap();
        self.signal_next_waiter_if_room(&mut state);
    }
    outcome
}
```

Verification checklist:
- Every return path from `allocate_inner` (fast-path early return,
  immediately-satisfiable, slow-path success, slow-path error) flows
  back through the outer wrapper. No bypass.
- The slow-path Drop also signals — verify the resulting
  double-signal is harmless (Notify::notify_one stores at most 1
  permit, idempotent).

## New-defect-scan checklist for these idioms

When reviewing a Round-2 fix that uses the patterns above, scan for:

1. **Drop with poisoned mutex**: `lock().unwrap()` panics on
   poisoning, and a panic in Drop aborts the process. Acceptable if
   the rest of the file `unwrap()`s the same mutex (consistency); not
   acceptable if Drop should be infallible. Document the trade-off.
2. **Drop holding lock across await**: must not happen — Drop is
   synchronous. Verify by reading the Drop body for `.await`.
3. **Signal-on-exit thundering herd**: `Notify::notify_one()` wakes
   at most one listener. If both inner-body Drop and outer wrapper
   signal, that's one wake-up, not many. Not a thundering herd.
4. **Test fixup integrity**: when a test was modified in-place (not
   duplicated), verify the OLD `pool.close()`-style stimulus is gone
   from the cancellation test, while legitimate `close()` tests
   remain. Run `grep -n "pool.close()"` and audit each call site.
5. **Test name overclaim**: regression tests should state in their
   doc-comment what hazard they guard against AND mentally revert
   the fix to confirm the test fails. Reject "tests that always pass"
   even if they pass under the fixed code.

## Phase-6a-specific landmines avoided

- The `cancelled_allocate_does_not_stall_subsequent_waiter` test
  needed multi-thread runtime + 3s timeout headroom (originally 1s
  but bumped in `d035209` for slow CI). The 3s vs 60s block_time
  asymmetry means the test fails clearly if the guard regresses, not
  flakily.
- The deadline-propagation test
  (`outer_timeout_cancels_chained_inner_await`) leverages the fact
  that completing only the parent forces `get()` to await the child
  next — so `tokio::time::timeout` MUST cancel the inner await,
  proving chain propagation. The test does not just verify
  `tokio::time::timeout` itself; it verifies the chain semantic.
- Per-file `#![allow(dead_code)]` with phase-pointer comments is the
  correct alternative to module-wide allows. Each comment names the
  phase that wires the file's public surface (e.g. "Phase 6d
  (RecordAccumulator) wires the public surface."), giving the next
  actor an actionable hint.
