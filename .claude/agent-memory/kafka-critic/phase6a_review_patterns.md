---
name: Phase-6a review patterns
description: BufferPool + future plumbing translation traps — try/finally→Drop gap, ByteBuffer.clear vs Vec zero-fill, false-passing cleanup tests
type: project
---

Phase 6a (BufferExhaustedError, BufferPool, Callback, RecordMetadata,
ProduceRequestResult, FutureRecordMetadata, ErrorLoggingCallback,
IncompleteBatches, ProducerBatch placeholder) review yielded 2 Blocking
+ 5 Suggestion findings. Patterns to carry forward:

**Why:** these traps are subtle, compile cleanly, pass naive unit
tests, and surface only under cancellation/load. Naming each so the
next phase's review starts from them.

**How to apply:** when reviewing producer/internals code in 6b–6e,
apply this checklist before declaring "looks good".

### 1. Java `try/finally` → Rust must use `Drop` guard, not straight-line cleanup

If the Java code uses `try { ... await ... } finally { remove from
queue / refund accounting }`, **the equivalent Rust async fn must use
a Drop guard**, not straight-line code after the await loop. Future
cancellation drops the future at the await suspension point and skips
all post-await statements — so `state.waiters.remove(&waiter)` on
line N+5 never runs if the await on line N is cancelled. Look for:

- `try { ... } finally { ... }` blocks in Java that include
  `XYZ.remove(...)` or counter refunds.
- The matching Rust function — does the cleanup happen via a `struct
  Guard { ... } impl Drop`? Or just inline after the loop?

Test for the bug: replace the cleanup test's stimulus from "successful
return from the function" to `JoinHandle::abort()`. If the test still
passes, the cleanup runs; if it fails (or the queue grows), the
cleanup is missing.

### 2. False-passing cancellation tests

Watch for cancellation/interruption tests that use `pool.close()`,
`channel.send(quit)`, or any other "graceful shutdown" stimulus
instead of the actual cancellation primitive (`JoinHandle::abort()`,
`drop(future)`, `tokio::time::timeout(d, future)` with d so small the
inner future never completes). The Java test typically uses
`Thread.interrupt()` — the *only* faithful Rust translation is to
abort the task. If the Rust test docstring claims to translate an
interruption test but uses `close()`, the test is structurally
flawed.

Concrete signature to grep for: a test named
`*cancellation*`/`*interrupt*` whose body does not contain
`.abort()` or a tightly-bounded `tokio::time::timeout`.

### 3. Java `ByteBuffer.clear()` ≠ `Vec::clear() + resize(N, 0)`

`ByteBuffer.clear()` is a metadata-only reset (`position=0,
limit=capacity`); the underlying bytes are not touched. The naive Rust
translation `vec.clear(); vec.resize(N, 0)` writes N zeros to memory.
For a buffer pool, this is a 2× zero-fill per recycle (once on
deallocate, once on next allocation). Per CLAUDE.md rule 12 and DoD
line 10, this is a hot-path regression on the producer send path.

Better Rust patterns: keep `Vec` at `len == capacity` always; or use
`Box<[u8]>` of fixed size and hand out `&mut [u8]`; or
`unsafe { vec.set_len(size) }` after asserting capacity.

### 4. `#![allow(dead_code)]` in module headers + `#![deny(warnings)]` in
`lib.rs` is a regression-masking trap

Module-wide `#![allow(dead_code)]` defeats the value of
`#[deny(warnings)]` in `lib.rs`. New methods added in subsequent
sub-phases compile without anyone calling them. Prefer per-item
`#[allow(dead_code)]` with a comment naming the sub-phase that will
remove it. Field-level `#[allow(dead_code)]` on a field that *is*
read (e.g. `BufferPool.time` read by `self.time.nanoseconds()`) is
already-incorrect noise and a hint that the actor copy-pasted the
attribute defensively without checking.

### 5. Mock-based deadline-propagation tests are not always
expressible — but the replacement should still cover the spirit

Java's `testFutureGetWith{Seconds,MilliSeconds}` use Mockito to verify
the chained future is awaited with the *remaining* deadline. That
specific hazard ("re-passing the original timeout to the child") is
not expressible in Rust because the Rust call shape is
`tokio::time::timeout(d, future.get())` — the timeout is enforced at
the outer level, not internally. The actor's deferral is sound.

**However**, the replacement test must still cover the spirit: when
a parent `await` resolves immediately (because it was already done)
but the child is still pending, the outer timeout must cancel the
child too. The actor's replacement test in this phase
(`chain_resolves_to_tail_metadata`) only tests that the chain
resolves to the tail metadata — which was already covered by
`is_done_follows_chain`. A real spirit-replacement: parent done +
child never completes + `tokio::time::timeout(50ms, get())` returns
`Err(Elapsed)`.

### 6. `tokio::sync::Notify` permit semantics + missed-wakeup audit

For an `await_completion`-style helper that uses
`Notify::notify_waiters()` (mass-wakeup, no permit storage), the
correct missed-wakeup-safe poll pattern is:

```rust
if completed.load(Acquire) { return; }
let notified = self.notify.notified();   // captures notify_waiters_calls
if completed.load(Acquire) { return; }
notified.await;                            // sees counter mismatch on poll
```

Tokio's `Notified` future captures the `notify_waiters_calls` count at
creation. If `notify_waiters` runs between creation and the first
poll, the future returns `Ready` immediately. So the second
`completed.load(Acquire)` after `notified()` creation is the critical
recheck. Verify this exact pattern, not a single-check version.

For per-waiter `Notify::notify_one`: a permit *is* stored, so this
is missed-wakeup-safe by construction. But beware leaked-Notify
heads in queues (Issue 1 above): `notify_one()` to a head whose
`notified()` future is dead just stores a useless permit, and live
waiters behind the dead head are not signaled.

### 7. Identity-keyed sets in Java (`HashSet<X>` with default hashCode)

When Java uses `HashSet<X>` with default `Object.equals`/`hashCode`
(i.e. identity), the Rust translation must preserve identity-key
semantics. Pattern that works: `HashMap<usize, Arc<X>>` with
`Arc::as_ptr(&x) as usize` as the key. Verify with a "two
distinct-but-structurally-equal Arcs are kept distinct" test (the
phase 6a `IncompleteBatches` translation does this correctly).

### 8. Forward-declared placeholder types

When a phase introduces a placeholder for a type that will be filled
in next phase (here: `ProducerBatch`), verify:

- Public-side: only the surface used by callers in the same phase is
  exposed.
- Java field name preservation: e.g. Rust's `produce_future` mirrors
  Java's `produceFuture` — important so the next phase's refill is a
  strict superset, not a rewrite.
- The placeholder does not encode a constraint that the full type
  cannot satisfy (e.g. `#[derive(Copy)]` on a type that must own
  unique resources later).
- Tests that exercise the placeholder do not depend on
  placeholder-specific behavior.
