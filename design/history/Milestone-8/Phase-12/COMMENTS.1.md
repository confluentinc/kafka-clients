# Phase 12 Review — Critic N=1

Reviewing commits (1/N) `a4378ce`, (2/N) `143f30a`, (3/N) `d3e9e15`.

| Batch | Issues | Status |
|---|---|---|
| 1 | 1, 2, 4, 5 | Open |

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

## Issue 2: Dual `ConsumerStateNotifier` instances — bg-task writes to a slot that no app-side accessor reads

**Commit**: `143f30a` (Phase 12 commit 2/N) + `d3e9e15` (Phase 12 commit 3/N)
**File**: `src/consumer/async_kafka_consumer.rs:903-925` (registers notifier #A), `src/consumer/async_kafka_consumer.rs:1093-1144` (`new_with_components` builds notifier #B)
**Severity**: **blocking**
**Java reference**: `kafka/clients/src/main/java/org/apache/kafka/clients/consumer/internals/AsyncKafkaConsumer.java:289` (the single `AtomicReference<Optional<ConsumerGroupMetadata>> groupMetadata`), `343-353` (the single `memberStateListener` that writes to it), `447` (`groupMetadata.set(initializeGroupMetadata(...))`), `462` (the same `memberStateListener` passed to `RequestManagers.supplier`).

### Description

Java keeps a **single** `AtomicReference<Optional<ConsumerGroupMetadata>> groupMetadata` field and a **single** `MemberStateListener` instance (`memberStateListener` at line 343). Both the constructor's initial `groupMetadata.set(initializeGroupMetadata(...))` write (line 447) and the listener's `updateGroupMetadata(...)` writes (via `onMemberEpochUpdated`, line 345) target the same slot. The public `groupMetadata()` accessor (line 1929) reads from that same slot.

The Rust translation builds **two** `ConsumerStateNotifier` instances against **two distinct** `Arc<Mutex<Option<ConsumerGroupMetadata>>>` slots:

1. **Notifier #A** (commit 2/N, `async_kafka_consumer.rs:903-924`): built in the production ctor's group-id-present branch. Its `group_metadata` and `group_assignment_snapshot` Arcs are LOCAL to the `if let Some(membership)` block and dropped at end-of-scope. The notifier itself is moved into the membership manager's listener list via `register_state_listener`, so it stays alive — but the only external reference to its backing `Arc<Mutex<Option<ConsumerGroupMetadata>>>` is held inside the notifier itself.

2. **Notifier #B** (Phase 11, `new_with_components`, `async_kafka_consumer.rs:1103-1110`): built inside `new_with_components`. Its backing Arcs become the consumer struct's `self.group_metadata` and `self.group_assignment_snapshot` fields. The `Consumer::group_metadata()` impl reads from `self.group_metadata`.

Result: the bg task drives `onMemberEpochUpdated` → `update_group_metadata` on **notifier #A**, writing into a slot only reachable through #A. The user's `consumer.group_metadata()` reads **notifier #B**'s slot, which stays at the initial `None`. The app-side observation diverges from Java's contract.

### Why this is blocking

- The Phase 12 PLAN.md `Risks` section line 553-555 explicitly notes "the test assertion observing `group_metadata().member_epoch() > 0` would be flaky" if registration is wrong — and here registration is on the *wrong* notifier. The assertion will be persistently `0`, not flaky.
- The Phase-11-deferred unit test at `async_kafka_consumer.rs:3967` (group_metadata bg-task wire-up assertion, PLAN.md commit 7) cannot pass with this wiring — its whole point is verifying the bg task drives group_metadata updates that the app side observes.
- `Consumer::group_assignment_snapshot()` (KIP-848) is affected identically — `onGroupAssignmentUpdated` writes to notifier #A's slot, not the one the consumer reads.

### Expected

Match Java: a single `MemberStateListener` instance writes to a single `Arc<Mutex<Option<ConsumerGroupMetadata>>>` slot that is shared between (a) the membership manager's listener list and (b) the consumer struct's `group_metadata` field. The same applies to `group_assignment_snapshot`.

### Suggested fix

Either of:

1. **Consolidate via components**: build the `ConsumerStateNotifier` (and its two backing `Arc<Mutex<...>>` slots) once in the production ctor, register it on the membership manager, AND pass the notifier + the two slots into `AsyncKafkaConsumerComponents`. Modify `new_with_components` to consume the passed-in notifier/slots instead of constructing new ones (add an optional `state_notifier: Option<Arc<ConsumerStateNotifier>>` field on `AsyncKafkaConsumerComponents`; if `Some`, use it; if `None`, fall back to the current Phase-11 test-rig behavior).

2. **Build outside, inject in**: extract notifier construction into a helper called from both code paths; have the production ctor call it, then thread the resulting (notifier, slot, slot) tuple into both the membership registration and the components struct.

Option 1 is closer to the existing component-struct contract. Add a regression test where: the bg-task receives a synthetic heartbeat response that drives `onMemberEpochUpdated`, then the app side reads `consumer.group_metadata().member_epoch()` and asserts the new epoch is visible.

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

## Issue 5: Listener-registered `ConsumerStateNotifier`'s backing `Arc<Mutex<Option<ConsumerGroupMetadata>>>` is unreachable — writes will succeed but go to an orphaned slot

**Commit**: `143f30a` (Phase 12 commit 2/N)
**File**: `src/consumer/async_kafka_consumer.rs:903-925`
**Severity**: nit (already captured as part of Issue 2's root cause, but worth a separate note because the explicit `let _ = (group_metadata, group_assignment_snapshot);` (line 924) makes the orphaning intentional yet incorrect)
**Java reference**: `kafka/.../AsyncKafkaConsumer.java:289, 343-353, 447`.

### Description

The block at `async_kafka_consumer.rs:904-911` builds `group_metadata: Arc<Mutex<Option<ConsumerGroupMetadata>>>` and `group_assignment_snapshot: Arc<Mutex<HashSet<TopicPartition>>>`, wraps them in a `ConsumerStateNotifier`, registers the notifier on the membership manager, and then explicitly drops the local Arc references on line 924 via `let _ = (group_metadata, group_assignment_snapshot);`.

The comment claims "the membership manager's listener list holds the only remaining strong reference… the notifier outlives this scope via the membership manager's listener list." That is true for the notifier itself — but the backing `Arc<Mutex<Option<ConsumerGroupMetadata>>>` slot becomes reachable ONLY through the notifier's `group_metadata` field, which is private (`pub(crate) struct ConsumerStateNotifier { group_metadata: Arc<Mutex<Option<ConsumerGroupMetadata>>>, ... }` at lines 393-395).

So when the bg-task receives a heartbeat response and triggers `onMemberEpochUpdated` → `update_group_metadata`, the write to `*self.group_metadata.lock().unwrap() = Some(next)` SUCCEEDS, but no other code path reads from that slot — the slot is reachable only through the membership manager's listener list, and nothing on the listener-callback path traverses back through the listener to read its private field.

### Why this is a nit, not a blocker (separate from Issue 2)

Issue 2 is the actual user-visible bug (`consumer.group_metadata()` returns stale data). This issue is the structural reason — the orphaned-Arc pattern at line 924 is a code smell that, while it isn't strictly wrong on its own (Java has private state inside its listener too), telegraphs the dual-notifier mistake. Fixing Issue 2 will also remove the orphan.

### Expected

After Issue 2's fix (single notifier), this block should disappear or change shape: instead of building a notifier locally and dropping the Arcs, the ctor should build the notifier as part of the eventual `AsyncKafkaConsumerComponents` payload, register it on the membership manager, and pass it through to `new_with_components`.

### Suggested fix

Subsumed by Issue 2's fix. When closing Issue 2, also remove the `let _ = (group_metadata, group_assignment_snapshot);` line and the "intentionally dropped" comment — the new wiring should not need them.

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
