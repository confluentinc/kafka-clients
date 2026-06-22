---
name: phase9-design-notes
description: CommitRequestManager + OffsetCommitCallbackInvoker translation patterns from Milestone-8 Phase 9
metadata:
  type: feedback
---

# Phase 9 (Commit) translation notes

## Wire wrappers: OffsetCommit + OffsetFetch builders

- Java's `OffsetCommitRequest.Builder` has two static factories
  (`forTopicIdsOrNames`, `forTopicNames`) and validates version-vs-id
  invariants in `build(short)`. Translate by wrapping validation in
  `RequestBuilder::build_version` and returning `io::Error::Unsupported`
  on protocol-version mismatches.
- `OffsetFetchRequest.Builder` has a `maybeDowngrade(short)` step that
  re-shapes v8+ batched data into v<8 single-group form. Faithfully
  translate this — don't fall back to "use latest version always".
- `OffsetFetchResponse` has a lazy `groups` cache (`Map<String, Group>`)
  for v8+ responses; clone it on demand under a `Mutex` (`group(&self)`
  is Java's sync signature and the mutex-protected lookup matches it).

## OffsetCommitCallbackInvoker ownership

- Java holds `ConsumerInterceptors` directly (not via `Arc`). The Rust
  trait `ConsumerInterceptor<K, V>` is `Send + 'static` (not `Sync`),
  so `Arc<ConsumerInterceptors>` cannot be used to share across tasks.
  Translation: own the chain via `Mutex<ConsumerInterceptors>` inside
  the invoker, and share the invoker itself via
  `Arc<OffsetCommitCallbackInvoker>`.
- Cache `interceptors_empty` at construction so the enqueue fast path
  doesn't take the mutex. Matches Java's `isEmpty()` check on the
  immutable list.

## CommitRequestManager threading

- Java has one-shared-thread synchronisation on `synchronized(this)`
  and `PendingRequests`. Translation: own everything behind a single
  `Mutex<CommitRequestManagerState>` inside an `Arc<Inner>`.
- The app side calls `commit_sync` / `commit_async` / `fetch_offsets`
  and gets a `oneshot::Receiver<Result<...>>` back. The bg task drains
  via `poll_with_coordinator(coordinator, now_ms)`.
- The trait `RequestManager::poll(current_time_ms)` cannot reach the
  coordinator (the trait method doesn't carry it), so the impl returns
  empty. The bg task wiring lives in `poll_with_coordinator` and is
  called explicitly by Phase 10 once it owns both managers.

## Idempotent oneshot completion

- Wrap `Sender` in `Arc<Mutex<Option<Sender>>>` (Phase 5/6/7 precedent).
- For multi-cycle requests (commit, fetch — Java has `resetFuture()`),
  expose `reset_future(&mut self)` that swaps in a fresh
  `(tx, rx)` pair. The retry driver awaits the inner `rx` and
  re-issues if needed.

## Phase 7d carry-over: `init_with_committed_offsets_if_needed`

- Java places this in `OffsetsRequestManager`, not `CommitRequestManager`.
- The Phase 9 plan asks us to land it on `CommitRequestManager` as the
  integration point for Phase 7d's deferred `update_fetch_positions`
  / `fetch_offsets` work.
- Implementation is a thin delegate to `fetch_offsets(...)`. Phase 10
  wires `OffsetsRequestManager::update_fetch_positions` to call this.

## Test deferrals

- `CommitRequestManagerTest.java` has 50 test methods over 1975 LOC.
  Many depend on Mockito mocks of `BackgroundEventHandler`, `Metrics`,
  or `MembershipManager` — defer to Phase 11 with one-line rationale
  per test in the tests module comment.
- The 15 tests landed in Phase 9 cover the core state-machine and
  request-building behaviour that's testable without parallel-phase
  wiring.

## MemberStateListener trait deferral

- Phase 8 ships the trait in
  `src/consumer/internals/member_state_listener.rs`. The worktree base
  (`9f79bc6`) does NOT contain Phase 8's trait yet.
- Decision (per the Phase-9 task brief): leave a
  `TODO(Phase 11 merge): impl MemberStateListener` comment on
  `CommitRequestManager` and provide `on_member_epoch_updated` as a
  free-standing method. Phase 11 wires the trait impl during the
  `AsyncKafkaConsumer` integration.
