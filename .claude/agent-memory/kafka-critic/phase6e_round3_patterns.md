---
name: Phase-6e Round-3 verified patterns
description: Verified-good fix shapes for catch_unwind fidelity testing, async-from-sync callback bridge, and metadata-update test sequence
type: project
---

# Phase-6e Round-3 verified-good fix shapes

When Round 2 turns up "test does not actually exercise the contract"
(false-passing test), the Round-3 fix pattern that worked here:

## 1. Empirical fidelity check via in-place revert

**Why**: A claim of "I tested by reverting catch_unwind and it failed"
is checkable. Don't take it on faith.

**How to apply**: For any contract-guard test (panic-swallow, retry,
ordering invariant), run the in-place revert yourself:
1. `git stash` any local changes.
2. Edit the production code to remove the wrapper/guard under test.
3. Run the specific test.
4. Verify it fails with the *expected assertion message* (not just any
   failure — e.g. "panic propagated" not "timeout").
5. Edit-revert back to original.
6. `git status` should be clean (or `git diff` empty).
7. `git stash pop` to restore your changes.

If the test passes when the guard is removed, file a Blocking comment.

## 2. Test-only Arc<AtomicBool> handles for spawn-from-test pattern

**Why**: When `run_loop` takes `&mut self` and the test wants to
`tokio::spawn(sender.run_loop())`, the test loses the ability to call
`initiate_close()` directly. Need an outside handle to drive shutdown.

**How to apply**: Verified-safe pattern:
```rust
#[cfg(test)]
pub(crate) fn running_arc(&self) -> Arc<AtomicBool> { Arc::clone(&self.running) }
#[cfg(test)]
pub(crate) fn force_close_arc(&self) -> Arc<AtomicBool> { Arc::clone(&self.force_close) }
```
Both `#[cfg(test)]` at the function level — production API surface
unchanged. Test does:
```rust
let running = sender.running_arc();
let force_close = sender.force_close_arc();
let join = tokio::spawn(async move { sender.run_loop().await; });
// ... assertions ...
force_close.store(true, Ordering::Release);
running.store(false, Ordering::Release);
let r = tokio::time::timeout(Duration::from_secs(2), join).await;
assert!(matches!(&r, Ok(Ok(()))));
```

## 3. Tripwire-counter for sync-to-async observability

**Why**: A spawned task that panics (without `catch_unwind`) doesn't
synchronously surface the panic to the test — `JoinHandle::is_finished()`
needs to be polled. A separately-bumped counter, incremented BEFORE
the panic, gives a deterministic "panic was reached" signal that's
independent of the loop's state.

**How to apply**:
```rust
panic_trip_count: Arc<AtomicUsize>,  // inside MockClientImpl

// In poll():
if let Some(msg) = self.panic_on_next_poll.take() {
    self.panic_trip_count.fetch_add(1, Ordering::Relaxed);
    panic!("{msg}");
}
```
Test polls `trip_count >= 1` to know the panic fired, then asserts
`!join.is_finished()` to know it was caught.

## 4. tokio::task::yield_now() in mocks without real I/O

**Why**: Real `KafkaClient::poll` involves I/O readiness which
implicitly yields. A mock with no genuine await point becomes a tight
CPU loop on `current_thread` runtime, starving any other task —
including the test task that wants to observe the trip counter.

**How to apply**: Add `tokio::task::yield_now().await` at the top of
mock methods that would otherwise be tight loops. Audit: this only
affects spawned-task tests; inline-drive tests (the dominant pattern)
see it as a no-op.

## 5. async-from-sync callback bridge audit

When a test pattern needs an async API call (`accumulator.append`)
inside a sync callback (`Callback::on_completion`), the only legal
bridge is `tokio::spawn`. Verify three things:

- The runtime handle is `Handle::current()`-resolvable (i.e., the
  callback fires inside a task already on a runtime).
- Both the spawning task and spawned task share the same runtime — no
  cross-runtime deadlock risk.
- The shared state used inside the spawned task does NOT need a lock
  that the spawner holds across the spawn point.
- The test awaits all spawned `JoinHandle`s before its assertions.

For test code, per-message `tokio::spawn` is acceptable (CLAUDE.md
rule 11.4 forbids it on the *production* send path). Document this
explicitly in the rustdoc.

## 6. Metadata-update test sequence

For tests like `testMetadataTopicExpiry` that rely on Java
`MockClient.updateMetadata()`: the Rust translation does not need a
mock — call `metadata.update_with_current_request_version(&resp,
false, time.milliseconds())` directly with a hand-built
`MetadataResponse`. Java's MockClient is a convenience layer; the
underlying API is public.

The Round-1 deferral rationale "needs Mockito-style harness" was
incorrect. **Always check whether the Java test relies on
*MockClient-specific* behavior or just on the *public Metadata API*
that MockClient exercises through.** If the latter, no harness needed.
