---
name: review_m8_phase12_5
description: Phase-12.5 response-routing wire-up patterns — unconditional pre-branch side effects in Java whenComplete callbacks, fragile yield_now-based test patterns
type: feedback
---

# Phase-12.5 patterns for future audits

## Pre-branch side effects in Java `whenComplete` are easy to miss

Java's `unsentRequest.whenComplete((response, throwable) -> { ... })`
callbacks often perform an action **before** branching on `response != null`.
The most subtle case in CoordinatorRequestManager is `getAndClearFatalError()`
at line 124 (just before the if-else), which:
- Runs on BOTH the success and failure branches
- Has user-visible state effects (`fatal_error` field cleared)
- Is consumed downstream by `AbstractHeartbeatRequestManager.maybe_propagate_coordinator_fatal_error_event`

**How to apply:** When translating a `whenComplete` callback to a
`tokio::spawn(async move { match response_rx.await { ... } })` forwarder,
read the Java callback **end-to-end before the if/else** and check what
runs unconditionally. Mirror those side effects either:
- At the top of the async block (before `response_rx.await`), or
- Inside BOTH `on_response` AND `on_failed_response` helpers.

If the Rust version only puts the side effect inside `on_response`, the
failure path silently diverges.

## `yield_now` is fragile but acceptable for current_thread runtime tests

The Phase-12.5 regression tests use:
```rust
unsent.handler().on_complete(response);
tokio::task::yield_now().await;
tokio::task::yield_now().await;
assert!(...);
```

This works on `#[tokio::test]`'s default current-thread runtime because:
- `tx.send(Ok(response))` makes `response_rx.await` ready immediately
- `yield_now` re-queues the current task; the scheduler picks the
  ready forwarder before resuming
- Two yields leave headroom if the forwarder itself awaits internally

This **breaks** on multi-thread runtime or if the forwarder body adds
more `.await` points. Future critics should flag tests that:
- Use `yield_now` but assert on state that requires N+1 yields to settle
- Use `yield_now` in a `#[tokio::test(flavor = "multi_thread")]` test
- Have a comment like "two leaves headroom" — admission of uncertainty

Prefer `tokio::time::timeout(...)` on a `oneshot::Receiver` when the
final state can be observed via a user-facing future.

## `Arc<Mutex<Inner>>` interior-mutability pattern verification checklist

When reviewing `Arc<Mutex<Inner>>` refactors (a la commit_request_manager
canonical), check:

1. The trait `poll(&mut self)` impl is a one-line delegate to
   `poll_shared(&self)`.
2. `poll_shared` acquires Mutex slots in a non-overlapping discipline
   (no held guard across an `.await`).
3. The spawned forwarder calls `Arc::clone(&self.inner)` (not `&self.inner`
   which won't outlive the spawn).
4. The forwarder has all 3 match arms: `Ok(Ok(response))`, `Ok(Err(err))`,
   `Err(_recv_err)` — never silently dropping.
5. `take_response_receiver()` is called BEFORE the `UnsentRequest` is
   moved into the `PollResult`.
6. Any `&mut self` method that the cascade deleted (e.g.
   `coord.lock().unwrap().mark_coordinator_unknown(...)` → `coord.mark_coordinator_unknown(...)`)
   has its body converted to interior-mutability via inner Mutex slots.
7. Test-only helpers like `set_coordinator_for_test` are now `&self` and
   the tests no longer need `Arc::new(Mutex::new(...))` wrapping.

## SystemTime vs mock clock divergence in spawned forwarders

Forwarders that spawn at `poll_shared(current_time_ms)` time can't capture
the production `Time` source easily. The codebase consistently uses
`std::time::SystemTime::now()` inside the spawn body
(`offsets_request_manager.rs:1548-1554` documents this explicitly).

This means tests with a mock clock (e.g. `consumer_network_thread.rs`'s
`Mutex<i64>`-backed `ThreadTime`) get **divergent** clock readings between
`poll_shared(mock_now)` and the forwarder's `SystemTime::now()`. Tests
that rely on consistent timing must avoid going through the production
forwarder — call the manager's `on_response` / `on_failure` directly.

This is an acknowledged limitation. Don't file as a bug; flag as a
nit if the actor introduces a new spawned forwarder without explaining
the time-source choice.

## Round-2 patterns (commits 3+4 + Issue 1/2 fixups)

### Java `onResponse` vs `onErrorResponse` vs `onFailure` — three categories, not two

`AbstractHeartbeatRequestManager.java` has THREE error categories that
Phase 12.5 PLAN conflated into two:

1. Transport failure (`onFailure(Throwable, long)`) — network blip,
   timeout, recv-err. Resets heartbeat state at top.
2. Error in response body (`onErrorResponse(R response, long)`) — broker
   returned a non-`NONE` error code. **Also resets heartbeat state at
   top** (line 356) before `onFailedAttempt`.
3. Success (`onResponse(R, long)` → calls `onSuccessfulAttempt`).

When PLAN documents a two-arm `PendingHeartbeatCompletion { Success |
Failure }` split, the implementation must STILL branch on the error code
inside the response body. The risk: the error-response branch may miss
setup steps Java runs at the TOP of `onErrorResponse`. Check
`reset_heartbeat_state()` / similar pre-classification side effects.

**How to apply**: When reviewing a `Pending*Completion::Response` drain,
read the Java `onErrorResponse` line-by-line. Lines BEFORE the
per-error switch are the high-risk gaps.

### Async transition wiring claims need grep verification

When Rust code emits a `BackgroundEvent::Error` and the comment claims
"the bg-task drives the async transition" — VERIFY with:
```
grep -rn "\.transition_to_fatal\|\.transition_to_fenced" src/ | grep -v test
```

If this returns only test callsites, the wiring is incomplete. The
common pattern is to surface the error to user code via
`process_background_events::record_first_error`, which makes `poll()`
return `Err(...)` but never advances the membership state machine.

Phase 12.5 introduced this gap on heartbeat fenced/fatal — the comment
claims bg-task drives the transition; no production code does.

### Test design quirk: close-path re-insertion

`AbstractFetch::create_fetch_request` always inserts the target node
into `nodes_with_pending_fetch_requests` (line 290). So a test that
calls `poll_on_close(now)` in a wait-loop to drain the channel will
see the node RE-INSERTED each iteration, masking the drain's removal.

**Fix**: drive `poll(now)` (not `poll_on_close`) in the wait loop —
`poll(now)` short-circuits without re-inserting. This is a real
test-design quirk documented in the Actor's
`phase12_5_commit_4_notes.md`, not a production bug.

### Test-count drift detection

Actor reported +5 tests; actual count was +8 (9 new − 1 deleted).
When verifying a "+N tests" claim:
1. `git stash; git checkout <baseline_sha>; cargo test --lib | grep "test result"`
2. `git checkout <head>; cargo test --lib | grep "test result"`
3. Compute delta yourself.
Don't trust Actor's tallies blindly.

### Forwarder time source: `SystemTime::now()` vs `client_response.received_time_ms()`

The spawned forwarders compute `now_ms` via `SystemTime::now()` (e.g.
`consumer_heartbeat_request_manager.rs:344-347`). Java uses
`response.requestLatencyMs()` / `request.handler().completionTimeMs()`
which is set from the receive time in `ClientResponse`. Drift is
typically microseconds; not worth filing. Mention if reviewing.

