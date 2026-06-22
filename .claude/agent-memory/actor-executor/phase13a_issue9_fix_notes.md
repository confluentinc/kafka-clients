---
name: phase13a-issue9-fix-notes
description: Phase 13a Issue 9 fix — KIP-848 GroupIdNotFound retry across OffsetFetch/OffsetCommit/Heartbeat + poll-timer-defer init + STALE→JOINING transition wiring
metadata:
  type: project
---

# Phase 13a Issue 9 fix — patterns landed

**Why this matters going forward**: Five reusable patterns that future
KIP-848 work should reference. See commit `2c0ea9e` for the full diff
and `design/history/Milestone-8/Phase-13/COMMENTS.DONE.1.md` Issue 9
entry for the prose write-up.

## Pattern 1: Surgical retriable-error extension in commit-manager retry drivers

When extending the retriable-error set in a retry driver, prefer adding
the specific error code to the gate rather than modifying `is_retriable()`
globally. The retry gate looks like:

```rust
let is_group_creation_in_progress = err.error() == Errors::GroupIdNotFound;
let is_retriable = err.is_retriable() || is_stale_epoch_retriable || is_group_creation_in_progress;
```

Applied to three drivers in `commit_request_manager.rs`:
- `fetch_offsets_with_retries`
- `commit_sync_with_retries`
- `auto_commit_sync_before_rebalance_with_retries`

**Why surgical not global**: `Errors::is_retriable()` is consumed by many
other code paths (heartbeat, fetch, metadata) — adding `GroupIdNotFound`
globally would propagate behavior changes everywhere. The retry-driver-local
gate keeps the blast radius small.

**Do NOT call `mark_coordinator_unknown` for `GroupIdNotFound`**: the
coordinator is correct; the group simply doesn't exist yet on the broker.

## Pattern 2: Epoch-conditional handling in consumer-specific heartbeat handlers

When the abstract heartbeat layer's `classify_response_error` defers to the
consumer-specific `handle_specific_exception_in_response` for an error, the
specific handler can branch on `self.membership_manager.member_epoch()` to
make epoch-aware decisions. Pattern used for `GroupIdNotFound`:

```rust
Errors::GroupIdNotFound => {
    let member_epoch = self.membership_manager.member_epoch();
    if member_epoch == 0 {
        // First heartbeat: backoff + retry. classify_response_error
        // already called on_failed_attempt, so backoff is natural.
        Some(HeartbeatErrorAction::Handled)
    } else {
        // Rejoining after group reap: reset epoch to 0 via Fenced path.
        self.inner.heartbeat_request_state.reset();
        Some(HeartbeatErrorAction::Fenced)
    }
}
```

**Why the abstract layer defers**: the abstract layer doesn't have a
back-reference to the membership manager (membership is held by the
composing consumer). The consumer-specific layer DOES.

## Pattern 3: Poll-timer deferred-arm via `i64::MAX` sentinel

When Java arms a timer at construction but Rust's bg-task spin-up makes
this race with `max.poll.interval.ms` expiry, defer the arm to the first
event that should reset the timer:

```rust
// At construction:
poll_timer_expires_at_ms: i64::MAX,

// `poll_timer_is_expired(now)` returns false because now < i64::MAX.
// `poll_timer_remaining_ms` and `poll_timer_is_expired_by` must use
// saturating_sub to avoid overflow with i64::MAX.
```

The first `reset_poll_timer(now)` call arms it at `now + max_poll_interval_ms`.
Subsequent calls behave per Java's `Timer.reset()` semantics. Document the
deviation in a doc-comment on the field.

## Pattern 4: Synchronous-listener equivalents of Java's whenComplete

Java's `staleMemberAssignmentRelease.whenComplete((__, error) -> transitionToJoining())`
runs after the onPartitionsLost callback completes (in the bg thread). In
Rust the §31 handshake invokes listeners synchronously on the caller's
task; by the time `maybe_rejoin_stale_member` is reached from the next
`consumer.poll()`, the callback has already finished. We therefore
transition inline:

```rust
pub(crate) fn maybe_rejoin_stale_member(&self, join_group_epoch: i32) {
    let should_transition = {
        let mut guard = self.inner.lock()...;
        guard.is_poll_timer_expired = false;
        guard.state == MemberState::Stale
    };
    if should_transition {
        // Re-acquire lock — transition_to_joining takes its own guard.
        self.transition_to_joining(join_group_epoch)?;
    }
}
```

This pattern recurs anywhere Java uses `CompletableFuture.whenComplete`
to defer follow-up state machine work; in Rust the §31 handshake
collapses the two phases.

## Pattern 5: AEP `AsyncPoll` arm mirrors Java's `resetPollTimer(pollMs)` shape

Java's `AbstractHeartbeatRequestManager.resetPollTimer(pollMs)` (lines
265-274) does THREE things in order: `update`, check expired, `reset`.
Rust's `reset_poll_timer` only does the reset. The check + maybe_rejoin
step must be added at the call site:

```rust
if hrm.inner().poll_timer_is_expired(poll_time_ms) {
    mm.abstract_mm.maybe_rejoin_stale_member(join_epoch);
}
hrm.inner_mut().reset_poll_timer(poll_time_ms);
```

Without this, the STALE → JOINING transition never fires after a poll-timer
fence, and the consumer is stuck forever.

## Test scope tightening pattern

When a single ignore-rationale was filed for N tests but the underlying
gap turns out to be N independent issues, file N separate Issues in
COMMENTS.1.md when closing. Don't conflate a test-fixture bug with a
production-code gap with a broker-latency tuning issue. Phase-13 Issue 9
originally bundled all four together; closing it required filing Issues
10 (broker latency) and 11 (provisioner record) as separate follow-ups.
