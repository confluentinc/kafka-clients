# Phase 9 review — resolutions for COMMENTS.1.md

Reviewed at SHA `b30aa82` by Critic agent N=3. 10 findings; 3 blocking.
Manager applied all 3 blocker fixes directly (sub-Actor sandbox blocks
worktree writes — Phase 7c/d/8 precedent). The 7 remaining findings
carry forward as tracked tech-debt for Phase 10/11.

## Fixed (in `b0f7e11`)

### #1 (BLOCKING Bug) — `commit_async` callback enqueue order
Java enqueues interceptor first (on success), then user callback.
Rust had it reversed. Swapped to match `AsyncKafkaConsumer.commitAsync`
lines 1019-1032.

### #2 (BLOCKING Bug) — `GroupAuthorizationFailed` group id loss
Java's `GroupAuthorizationException.forGroupId(groupId)` embeds the
real group id. Rust's `_borrow_group_id_or_default` returned
`String::new()`. Threaded `inner.group_id` into
`classify_and_complete_commit`; removed the dead helper.

### #4 (BLOCKING Test) — Zero-assertion test
`test_no_on_commit_on_empty_interceptors` had a comment promising
"verify by enqueueing follow-up..." but no actual assertion. Rewrote
to enqueue interceptor entries + a user callback, drain, assert user
callback fired (queue made progress past no-op entries), drain again,
assert no further invocations.

## Tracked for Phase 10 / 11

### #3 — `inflight_offset_fetches` memory leak
Dormant in Phase 9 (no real bg task). Will leak in Phase 10
proportional to lifetime `fetch_offsets` calls. **Phase 10 MUST fix**
— either thread a removal closure into the spawned response handler
or switch `Vec<OffsetFetchRequestState>` → `HashMap<UUID, _>`.

### #5 — `signal_close_flips_closing_flag` doesn't verify drain
Today's test is a tautology over the bool flag. Java's `testSignalClose`
asserts `res.unsentRequests.size() == 1` after `signal_close + poll`.
**Phase 11 or 8b** plan should pick up the actual drain test with a
mock coordinator.

### #6 — `init_with_committed_offsets_if_needed` placed on wrong class
Java places it on `OffsetsRequestManager`, not `CommitRequestManager`.
The Phase-9 stub is a thin pass-through. **Phase 10 MUST relocate**
to the correct manager with Java's full semantics (`pendingOffsetFetchEvent`
reuse, `defaultApiTimeoutMs` floor, `refreshOffsets` on completion).

### #7 — `commit_sync` retry deferral
Java retries on retriable errors until deadline; Rust performs one
attempt. **Phase 10 wires** the in-manager retry counter (same
pattern as Phase 6's `CoordinatorRequestManager`).

### #8 — `RequestManager::poll` returns empty
`entries()` will iterate and call `poll(now)`; commit manager
silently emits nothing. **Phase 10 plan MUST explicitly note** that
`entries()` skips the commit slot and `poll_with_coordinator` is
called separately. Without that note, the commit manager will be
polled via the trait method and emit nothing, masking regressions.

### #9 — Non-finding (style verified clean)

### #10 — Metadata `updateLastSeenEpochIfNewer` not tested
Phase 9 calls `maybe_update_last_seen_epoch_if_newer(&offsets)` but
no test verifies the side effect. **Phase 11 or 8b** can add a
focused test.

## Per-deviation verdicts (after fixes)

- D1 (MemberStateListener deferred): PASS.
- D2 (commit_async combined invoker): PASS after #1 fix.
- D3 (`poll_with_coordinator` workaround): PARTIAL — accepted, but
  Phase 10 plan MUST add the `entries()` skip note (#8).
- D4 (retry deferred): PARTIAL — Phase 10 wires (#7).
- D5 (auto_commit stub): PASS.
- D6 (35/50 tests deferred): PARTIAL — Phase 11 will pick up the
  testable subset that needs coordinator stubs.

## Final state on worktree (HEAD: `b0f7e11`)

- Lib tests: 1356 (unchanged — fixes touch existing tests).
- `cargo test --lib commit_request_manager`: 15 passed.
- `cargo test --lib offset_commit_callback`: 4 passed.
- `cargo build`, `cargo xtask format-check`, `cargo xtask lint`: all clean.
- §31 callback discipline: verified (bg-task enqueues; app-task drains).
- `ConcreteRequest`/`Response` enum extensions: additive (no overlap
  with 7b/7d/8 partial).
- Phase 9 ready for merge back to `consumer-impl` alongside Phase 8
  partial.
