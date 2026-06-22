---
name: phase33-commit-test-parity-notes
description: Milestone-8 Phase 33 — CommitRequestManagerTest parity; 4 production bugs surfaced by tests + Rust-vs-Java commit-manager test mechanics
metadata:
  type: feedback
---

# Phase 33 — CommitRequestManagerTest parity (73 tests, 0 ignored)

Translating CommitRequestManagerTest surfaced FOUR production bugs in
`commit_request_manager.rs` — the test-parity work is where these are caught.

## Production bugs surfaced (all fixed in Phase 33)

1. **§31 auto-commit interceptor wiring missing.** Java CRM holds
   `OffsetCommitCallbackInvoker` and fires `enqueueInterceptorInvocation` on
   auto-commit success (`autoCommitCallback`, CommitRequestManager.java:380),
   both for interval auto-commit AND rebalance flush. Rust CRM held no invoker.
   Fix: type-erased `AutoCommitInterceptorHook` trait (CRM is not generic over
   `<K,V>`; the invoker is) impl'd for `OffsetCommitCallbackInvoker`, held as
   `Option<Arc<dyn AutoCommitInterceptorHook>>`, wired post-construction in
   AsyncKafkaConsumer ctor. Fire from BOTH auto-commit success arms.

2. **Send-time member-info staleness.** Java's `OffsetCommitRequestState` holds
   a *reference* to the manager's mutable `MemberInfo` (line 889), so an
   `onMemberEpochUpdated` between enqueue and send is reflected in the built
   request + `lastEpochSentOnCommit`. Rust stored a clone at enqueue.
   Fix: `poll_with_coordinator` re-syncs `commit.member_info = guard.member_info
   .clone()` at SEND time before building (and writes lastEpochSentOnCommit).

3. **Coordinator-disconnect not handled on commit/fetch transport failure.**
   Java's shared `RequestState.handleClientResponse` error arm calls
   `handleCoordinatorDisconnect` (line 947) for BOTH commit and fetch.
   Rust `build_*_unsent_request` `Ok(Err(err))` arms only completed the future.
   Fix: call `inner.handle_coordinator_disconnect(&err, now)` in both arms.

4. **OffsetFetch dedup/coalescing absent.** Java's
   `PendingRequests.addOffsetFetchRequest` chains a duplicate fetch's future to
   the existing identical request (`chainFuture`) → 1 wire request. Rust
   enqueued one request per call (was a documented deviation). Fix:
   `chained_public_senders: Arc<Mutex<Vec<Sender>>>` on the request, carried
   forward across retries; dup detection pushes onto it; the retry driver fans
   the final result out via `clone_fetch_result`.

Also: the rebalance-flush retry driver must re-read `member_info` from
`inner.state` per-iteration (not capture once at entry) — Java re-reads
`memberInfo` each attempt. The driver no longer takes a `member_info` param.

## Rust-vs-Java test mechanics for CommitRequestManager

- **Retry drivers use a LOCAL clock**, not poll time: each advances
  `current_time_ms += retry_backoff_ms` per retriable failure vs `deadline_ms`.
  To trip a Timeout deterministically, set `deadline_ms = retry_backoff_ms*2+1`
  and loop poll/fail while advancing poll-time by `retry_backoff_max_ms*2` so
  the re-queued request's seeded backoff has elapsed and it ships each cycle.
- **No stable `numAttempts` across retries** (Java reuses one object; Rust
  re-enqueues a fresh state seeded via `seed_failed_attempts`). Java's
  `commitRequest.numAttempts` → peek head of `unsent_offset_commits` /
  `unsent_offset_fetches` and read `state.num_attempts()`.
- **Drive responses** via `poll_with_coordinator(&coord, now)` → `unsent` →
  `unsent.handler().on_complete(client_response)` / `on_failure(now, err)`. The
  spawned handler routes into the retry driver; use
  `#[tokio::test(flavor="current_thread")]` + `yield_now()` loops.
- **`updateLastSeenEpochIfNewer` only REPLACES an existing epoch** (no insert on
  None). To observe the epoch update Java verifies on a mock, seed a lower epoch
  first via `metadata_update_with_ids` + epoch_supplier, THEN assert the bump.
- **Error-class assertions**: CommitFailedException → `KafkaError::IllegalState`
  (msg contains "OffsetCommit failed"); RetriableCommitFailedException →
  `err.is_retriable()` true; KafkaException → `err.error()==UnknownServerError`;
  typed (GroupAuth/OffsetMetadataTooLarge/InvalidCommitOffsetSize/TopicAuth) →
  match variant or `err.error()`. The OffsetFetch supplier maps more errors to
  bare KafkaException than the commit supplier — keep separate assert helpers.
- **Async commit asserts**: Java only asserts RetriableCommitFailedException for
  retriable; for non-retriable it just checks completed-exceptionally (no class).
- **§31 interceptor test**: wire `Arc<OffsetCommitCallbackInvoker>` as the CRM
  hook; use a `SharedRecorder(Arc<RecordingInterceptor>)` forwarding wrapper so
  one recorder is both registered (chain takes `Box`) and inspected; drive
  auto-commit success, yield, then `invoker.invoke_pending_callbacks().await`.

## Test-only seams added
- `CoordinatorRequestManager::set_fatal_error_for_test` (#[cfg(test)]).
- `KafkaError::group_authorization_with_message` / `GroupAuthorizationError::
  with_message` (production, not test-only) — Java's GroupAuthorizationException
  carries a custom message ("Fatal error") the close-path test asserts.

## Clippy gotcha
`collapsible_if` fires on `if let`+`if` too. Combine via
`if let (true, Some(x)) = (cond, opt.as_mut())`. A "never read" dead-store on a
loop variable assigned-before-read: move the `let` INSIDE the loop arm instead
of declaring it mutable outside.
