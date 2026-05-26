---
name: Phase 6e Round 2 patterns
description: Test-fidelity patterns from Round 2 fixups — panic-swallow loop test, sync→async callback bridge, current_thread runtime starvation
type: feedback
---

Three patterns from Round 2 of Phase 6e.

## 1. "Test passes for the wrong reason" — verify by reverting the production change

**Rule**: if a test claims to verify a defensive code path, mentally revert the
defense and re-run the test. If the test still passes, it does not actually
exercise the defense.

**Why**: Round 2 / Issue 10. The Round-1 panic-swallow test called
`initiate_close()` *before* `run_loop()`, which short-circuited both the main
loop (`running=false`) and the drain loop (`!has_undrained &&
!has_in_flight`). `run_once` was never called, so the armed panic never
tripped — the test passed because `run_loop` exited cleanly via
`running=false`, not because `catch_unwind` caught anything. Reverting
`catch_unwind` would have left the test green.

**How to apply**:
- For any "regression test" that asserts a code path was exercised: include
  *observable evidence* the path actually fired (counter, atomic, log probe).
- Before calling the work done, *empirically* revert the production change
  and re-run the test — confirm the test fails. The fix is verified only when
  removing the production change breaks the test.

## 2. current_thread runtime + sync mock = task starvation

**Rule**: a `tokio::spawn`-ed background task that calls `await` on futures
without genuine yield points will starve other tasks on the
`#[tokio::test]` (current_thread) runtime.

**Why**: the spawned `run_loop` task called `client.poll(...).await`, but
`MockClientImpl::poll` had no real I/O await. Each iteration ran to
completion synchronously and re-entered the loop. The main test task
(spinning on the trip counter) never got CPU.

**How to apply**:
- Add an explicit `tokio::task::yield_now().await` at the top of any mock
  `async fn` whose real implementation would yield on I/O (poll, accept,
  read).
- Production code rarely needs this — real network calls naturally yield.
- The yield should mirror the real behavior, not introduce artificial
  scheduling. Document the rationale (e.g. "real `KafkaClient::poll`
  involves I/O readiness which implicitly yields").

## 3. Sync trait callback that needs to call an async API → tokio::spawn + JoinHandle gather

**Rule**: when translating a Java callback that synchronously calls an
async-equivalent Rust API, spawn the call into a `tokio::task` and gather
the `JoinHandle`s in a shared `Mutex<Vec<_>>`. Test code awaits the handles
before final assertions.

**Why**: Round 2 / Issue 12. Java's `Callback.onCompletion(...)` invokes
`accumulator.append(...)` which is blocking-with-timeout in Java but `async
fn` in Rust. The sync `Callback::on_completion` cannot `.await`. Bridging
via `block_on` would deadlock on the same runtime; `block_in_place` requires
multi-threaded runtime; only `tokio::spawn` composes safely.

**How to apply**:
- Define the callback struct with `Arc`-cloned dependencies (accumulator,
  cluster, time, output channel).
- Inside `on_completion`, push a `JoinHandle` into a shared `Mutex<Vec<_>>`.
- After the synchronous trigger event (e.g. `sender.run_once()` returns),
  `for h in handles { h.await }` to drain.
- *Then* assert on the post-callback state (`record_count == 10` etc.).
- CLAUDE.md rule 11.4 forbids per-message spawn on the *production* send
  path, but test code is exempt — document the rationale inline.

## Side note: `running_arc` + `force_close_arc` test handles

When a test moves a `Sender` into `tokio::spawn(run_loop)`, the test can no
longer call `&self` methods on it. Expose `Arc<AtomicBool>` getters on the
sender (cfg(test)) for `running` and `force_close` so the test can flip the
shutdown flags from outside the spawned task. The accumulator's
`Arc<RecordAccumulator>` is already shared via `TestSetup.accum`, so
`accum.close()` is reachable too — but the `running` and `force_close`
booleans are private, so explicit handles are required.

This pattern repeats for any background-task test that needs to drive
shutdown from outside the spawn. Worth generalizing: "if a Sender-like
struct has `Arc<atomic>` shutdown flags, expose `cfg(test)` getters
*proactively* so tests can spawn the loop into a task without ceremony."
