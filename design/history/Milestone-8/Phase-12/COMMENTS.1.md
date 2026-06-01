# Phase 12 Review — Critic N=1

Reviewing commits (1/N) `a4378ce`, (2/N) `143f30a`, (3/N) `d3e9e15`.

| Batch | Issues | Status |
|---|---|---|
| 1 | 1, 4 | Open |

---

## Issue 1: `CommitRequestManager` is never polled in production — pending commit/offset-fetch requests will sit forever in `unsent_offset_commits` / `unsent_offset_fetches`

**Commit**: `143f30a` (Phase 12 commit 2/N)
**File**: `src/consumer/internals/request_managers.rs:170-242` (entries skip), `src/consumer/internals/consumer_network_thread.rs:382-386` (bg-task skips commit), `src/consumer/internals/commit_request_manager.rs:1292-1300` (dyn-trait `poll` returns empty)
**Severity**: **blocking**
**Java reference**: `kafka/clients/src/main/java/org/apache/kafka/clients/consumer/internals/CommitRequestManager.java:181-209` (`poll(currentTimeMs)`); `kafka/.../RequestManagers.java:91-101` (entries includes commit)

### Description

The slot-sharing refactor changed `RequestManagers.commit` to `Option<Arc<CommitRequestManager>>` and SKIPPED it from `entries()`. The bg-task's `run_once` polls `coordinator` explicitly via `coordinator_handle().lock().poll(now)` but does NOT call any analog for `commit`. The dyn-trait `CommitRequestManager::poll` impl returns `PollResult::empty()` (with a comment "Phase 10 wires `poll_with_coordinator` directly") and the inherent `poll_with_coordinator` is **only called from `#[cfg(test)]` code** — production code never invokes it.

Concretely, the following operations enqueue an `OffsetCommitRequestState` or `OffsetFetchRequestState` on `unsent_offset_commits` / `unsent_offset_fetches` and return a `oneshot::Receiver`:

- `CommitRequestManager::commit_async_no_callback` (line 847)
- `CommitRequestManager::commit_sync` (line 613)
- `CommitRequestManager::fetch_offsets` (line 917) — also used indirectly via `OffsetsRequestManager::fetch_committed_offsets` (`offsets_request_manager.rs:1004-1005`)
- `CommitRequestManager::maybe_auto_commit_async` (auto-commit firing, called by `update_timer_and_maybe_commit`)

Nothing in production then dequeues from these and converts them into `UnsentRequest` for the network client delegate. The receivers will await forever.

Cross-check: `grep -rn "poll_with_coordinator" src/` produces zero hits outside `commit_request_manager.rs` itself; the comment at `consumer_network_thread.rs:382-386` says "commit work is driven via `ApplicationEventProcessor` event arms which call `poll_with_coordinator` directly", but no AEP arm actually calls `poll_with_coordinator` — they call `commit_async_no_callback` / `commit_sync` / `fetch_offsets`, all of which only ENQUEUE.

Java's `CommitRequestManager.poll(currentTimeMs)` is the **only** code path that:
1. Drains `unsentOffsetCommits` into `UnsentRequest`s (Java line 203).
2. Calls `maybeAutoCommitAsync(currentTimeMs)` to fire the auto-commit timer (Java line `maybeAutoCommitAsync` invocation).
3. Handles `closing && coordinator unknown` by failing pending commits with `CommitFailedException` (Java line 186-191).
4. Drains pending offset commits during close via `drainPendingOffsetCommitRequests()` (Java line 197).

### Why this is a blocking bug for Phase 12 integration tests

- `test_commit_sync_then_resume_in_same_group` (PLAN.md commit 5) calls `commit_sync().await` — the `oneshot::Receiver` returned by `CommitRequestManager::commit_sync` will block forever because the enqueued request never gets shipped. The test will hit its 30-second deadline and fail.
- `test_subscribe_and_poll_records` calls `poll()` which internally drives the membership reconcile path. Phase-10 `CommitRequestManager::poll_with_coordinator` carries the auto-commit timer firing logic (`maybe_auto_commit_async`). With `enable.auto.commit=false` in the integration tests, this particular firing is moot — but `update_timer_and_maybe_commit` is invoked by `process_assignment_change` / `process_async_poll`, which still enqueues commits when auto-commit is enabled. If a downstream test enables auto-commit, the commit never ships.
- Any test calling `consumer.committed(...).await` (or `position(...)` which transitively triggers an `OFFSET_FETCH` via `OffsetsRequestManager::fetch_committed_offsets`) hangs for the same reason: the OffsetFetch RPC sits in `unsent_offset_fetches` and never gets drained.

### Expected

Match Java's `RequestManagers.entries()` walk: every `run_once` iteration must call into the commit manager's poll path, draining `unsent_offset_commits` and `unsent_offset_fetches` into `UnsentRequest`s, and forwarding them to the network client delegate.

### Suggested fix

The cleanest fix is to call `commit.poll_with_coordinator(coord, now)` from the bg-task's `run_once` between the coordinator poll and the membership reconcile, mirroring Java's `entries()` order. Since `coordinator` is `Arc<Mutex<...>>` and `commit` is `Arc<CommitRequestManager>` (with interior mutability), the call shape is roughly:

```rust
if let (Some(coord_arc), Some(commit_arc)) = (coord_handle.clone(), rm_guard.commit_handle()) {
    let mut coord = coord_arc.lock().unwrap();
    let poll_result = commit_arc.poll_with_coordinator(&mut coord, current_time_ms);
    before.push(poll_result);
}
```

Plumb this into the same `try_connect` + `add_all_from_poll_result` machinery as the other before-membership entries. Add a regression test that produces 1 record, commits synchronously, and asserts the commit `oneshot::Receiver` resolves within a bounded deadline against a `mock_broker` that ACKs the OffsetCommit response.

Alternative: change the dyn-trait `CommitRequestManager::poll` impl from `PollResult::empty()` to actually delegate to `poll_with_coordinator(coord, ...)` — but that requires the coordinator handle to be accessible, which it isn't from `&mut self` alone. The explicit bg-task wire is simpler.

---

## Issue 4: `FetchRequestManager::is_unavailable` / `maybe_throw_auth_failure` no-ops swallow auth errors silently — observable behavior gap from Java

**Commit**: `143f30a` (Phase 12 commit 2/N)
**File**: `src/consumer/async_kafka_consumer.rs:853-861` (closure construction)
**Severity**: nit (in Phase 12 PLAINTEXT-only scope) → upgrade to **blocking** for any phase that wires SSL/SASL
**Java reference**: `kafka/clients/src/main/java/org/apache/kafka/clients/consumer/internals/AbstractFetch.java:115, 123, 452-457` (`isUnavailable` guards send, `maybeThrowAuthFailure` propagates auth errors synchronously inside the per-partition fetch loop); `kafka/.../FetchRequestManager.java:63-69` (delegates both to `networkClientDelegate.isUnavailable / maybeThrowAuthFailure`).

### Description

The closures are wired as:

```rust
let is_unavailable: ...IsUnavailableFn = Arc::new(|_n: &Node| false);
let maybe_auth: ...MaybeAuthFailureFn = Arc::new(|_n: &Node| Ok::<(), KafkaError>(()));
```

The Actor's commit message reads "the behavior gap is one extra round-trip per disconnected-node fetch attempt — not a correctness bug; the delegate filters at send-time."

That characterization is incomplete for `maybe_throw_auth_failure`: Java's `AbstractFetch.java:452-457` calls `isUnavailable(node)` AND THEN `maybeThrowAuthFailure(node)` to surface auth errors observable on the consumer side **before** the broker connection has even been established (auth state is cached on `NetworkClientDelegate` from prior connect attempts). With the no-op `|_| Ok(())`, an auth error stays invisible until the broker actually RSTs the next reconnect attempt, then surfaces via the background event queue with delayed semantics.

For Phase 12's PLAINTEXT integration tests, this path is not exercised. So it is correctly characterized as a non-blocker for Phase 12. But the framing in the commit message ("one extra round-trip… not a correctness bug") understates the gap — it IS a correctness gap for the auth path, just one that Phase 12 happens not to hit.

### Expected

Either:
- (a) Defer to a follow-up commit that wires a sync snapshot of the delegate's node-availability + auth-failure maps (preferred). Document the deferral with a code-level `TODO(M9-SSL/SASL)` marker pointing at this comment.
- (b) Have the closures pull from a sync-readable atomic / Mutex snapshot maintained by the delegate (the delegate writes on `add_all_from_poll_result` / `handle_disconnections` paths, the closures read).

### Suggested fix

Add the deferral marker (option a) for Phase 12. **Before SSL/SASL wiring lands**, the closures MUST be replaced with delegate-backed snapshots — leaving the `|_| Ok(())` in place when auth is plausible is a real bug. Add a test that constructs a `mock_broker` that returns `SASL_AUTHENTICATION_FAILED` on the first connect and asserts the consumer surfaces the auth error before the second reconnect attempt would have completed.

Also: update the closure construction's inline comment to acknowledge that the auth-failure no-op IS a correctness gap (Phase 12 doesn't hit it solely because it's PLAINTEXT-only), not just a "one extra round-trip" performance hit.

---

## Notes on what was NOT filed

- **Single `tokio::spawn`** (`consumer-threading.md` §10): verified — exactly one `tokio::spawn` in the production ctor (line 1024). No per-event / per-RM spawns.
- **State-notifier registration happens before bg-task spawn**: verified — `register_state_listener` at line 914, `tokio::spawn` at line 1024.
- **Group-protocol gate**: verified — when `group_id.is_none()`, all four group-conditional managers are `None` (coordinator, commit, membership, heartbeat). The `.map(...)` chain at lines 752-826 handles both arms correctly.
- **Receive-path zero-copy** (`consumer-threading.md` §27): the new ctor doesn't touch the deserializer path — `Deserializers::new(key_deserializer, value_deserializer)` is wrapped in `Arc::new` once and threaded through. No new copies introduced.
- **Field-init order vs Java lines 390-508**: walked side-by-side. Order matches Java with the documented deferrals (telemetry, metrics, JMX). No silent re-ordering.
- **`Arc::clone(&metadata).metadata_arc()` extra clone** (line 700): performance nit only — `Arc::clone` then immediately calling `.metadata_arc()` is one extra atomic refcount op. Not worth filing.
- **Closure `signal_close_fn` lifetime**: verified — captures `signal_close_running: Arc<AtomicBool>` (clone of the bg-task's running flag) and `signal_close_wakeup: WakeupTrigger` (clone, which is itself an `Arc` internally). Closure outlives the spawn closure as required.
- **Two-reaper "unified into one" decision** (Phase 10 design pattern #2): the comment at line 956-958 acknowledges Java has two reapers; Rust uses one. Verified consistent with Phase 10 PLAN.

## Possible false-positive risk (not filed; flagging for confirmation)

- **`OffsetCommitCallbackInvoker` is given a FRESH `ConsumerInterceptors` instance instead of sharing with the `_interceptors` field** (line 729-731). Java line 446 passes the same `interceptors` reference. In current state both end up with empty Vecs, so behaviorally identical. If a future commit wires interceptor loading (Phase 12 PLAN.md "Out of scope"), this becomes a real divergence — the invoker would dispatch through its empty list while the consumer's interceptors held the loaded list. Worth a follow-up `TODO` marker but not worth filing as a Phase 12 bug.
