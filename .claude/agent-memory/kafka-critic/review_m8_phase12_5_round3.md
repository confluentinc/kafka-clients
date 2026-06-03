---
name: review_m8_phase12_5_round3
description: Phase-12.5 round-3 fixup audit — heartbeat default-arm fatal fallback gap and Fenced spurious-ErrorEvent divergence; verify classification tables against the Java switch default arm
type: feedback
---

# Phase-12.5 round-3 fixup patterns (Issues 3+4 fixups)

## Java switch `default:` arms are silent in classification tables

When the Actor provides a classification table mapping Rust enum
variants to Java error codes, the table usually enumerates the
**explicit** Java switch arms. The `default:` arm (catch-all
unrecognised error code) is easy to miss.

Java's `AbstractHeartbeatRequestManager.java:435-441`:
```java
default:
    if (!handleSpecificExceptionInResponse(response, currentTimeMs)) {
        logger.error("{} failed due to unexpected error {}: {}", ...);
        handleFatalFailure(error.exception(errorMessage));
    }
    break;
```

Rust (commit `1950caf`):
```rust
HeartbeatErrorAction::DelegateToSpecific => self
    .handle_specific_exception_in_response(error, &error_message, completion_time_ms)
    .unwrap_or(HeartbeatErrorAction::Handled),  // ← silently Handled
```

If `handle_specific_exception_in_response` returns `None`, Rust
defaults to `Handled` (no-op). Java defaults to **Fatal**. This is
the silent-swallow pattern that gets missed because the explicit
Java enums all map correctly.

**How to apply** when reviewing a Java-to-Rust switch translation:
1. Locate the Java `default:` arm — it's almost always the LAST case.
2. Verify the Rust `_` arm has equivalent behavior, not a no-op.
3. Specifically check `unwrap_or(...)` calls on `Option<Action>` —
   the default arm of the Option-collapse is the easiest fatal-fallback
   to omit.

## `BackgroundEvent::Error` on the Fenced arm is a Java divergence

Java's `FENCED_MEMBER_EPOCH` / `UNKNOWN_MEMBER_ID` arms do **not**
call `backgroundEventHandler.add(new ErrorEvent(...))`. The fence is
an internal state-machine event handled by `transitionToFenced` +
`onPartitionsLost` callback.

Rust's Fenced arm in `consumer_heartbeat_request_manager.rs` (Issue 4
fixup `0e72ee7`) emits `BackgroundEvent::Error` alongside the
side-channel transition. This causes `consumer.poll()` to return
`Err(FencedMemberEpoch)` to the user when Java would return records
normally.

The Fatal arm is correct — Java's `handleFatalFailure` emits ErrorEvent
on line 456. Only the Fenced arm has the spurious ErrorEvent.

**How to apply**: when reviewing the §31 mechanism for error propagation,
distinguish:
- Fatal: user-visible error (Java ErrorEvent + transitionToFatal).
- Fenced: internally handled, no user-visible error (Java's only
  effect is transitionToFenced; the rebalance is invisible to `poll()`).

A Rust ErrorEvent emission on the Fenced arm changes the user-visible
contract.

## §16 audit shortcut for "side-channel drain + bg-task await" pattern

When the Actor introduces a sync→async bridge via a side-channel +
bg-task drain, the §16 audit pattern is:

```rust
let pending = {
    let mut rm_guard = self.request_managers.lock().unwrap();
    rm_guard.take_pending(...)  // sync drain
};  // ← guard dropped here
for transition in pending {
    membership.transition_to_*(now).await;  // ← await outside guard
}
```

Verify:
1. The `let pending = { ... };` block has its inner braces closed
   BEFORE the `.await`.
2. The `Vec` returned by `take_pending_*` is owned (no borrow into
   the guard).
3. No other Mutex is acquired between the guard drop and the `.await`.

This is the standard pattern across Phase 8/10/12.5; flag deviations.

## Concurrent transition idempotency to check

When a side-channel can queue multiple `PendingMembershipTransition`
envelopes per iteration (e.g. two failed heartbeats):

1. **Fatal twice**: `transition_to(Fatal)?` is a no-op when state is
   already Fatal (same-state guard in `transition_to`). After first
   call, `partitions` is empty → listener skipped on second call.
   Safe.
2. **Fenced twice**: first call ends in JOINING. Second call: JOINING
   → FENCED is valid, partitions empty → listener skipped → JOINING
   again. Safe.
3. **Fenced then Fatal**: first FENCED-JOINING; second sets FATAL.
   Java semantics preserved.

Idempotency is NOT something to test exhaustively — but the Critic
should verify the membership-manager's transition methods don't have
hidden "first call has side effect, second call drops it" patterns.
