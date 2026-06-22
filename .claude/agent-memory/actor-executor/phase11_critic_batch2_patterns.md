---
name: phase11-critic-batch2-patterns
description: Phase 11 Critic round-2 fixup patterns — submit_and_drain helper, per-API wakeup matrix, MemberStateListener bridge, group_assignment_snapshot, close-timeout cap
metadata:
  type: feedback
---

# Phase 11 Critic batch-2 fixup patterns (Issues 10-15)

Six reusable patterns surfaced while fixing AsyncKafkaConsumer deadlock,
wakeup, and close-path issues from the Critic batch-2 review.

## 1. `submit_and_drain<T>` is the right primitive for every blocking-style consumer API

**Why**: Java's `addAndGet(...)` blocks the caller's thread; Rust's
`add_and_get(...).await` blocks the caller's task. When the bg task's
`invoke_rebalance_callback` blocks on the listener-callback ack (per
[[phase8b_design_notes]] / consumer-threading.md §31), a deadlock
forms — the bg task can't complete the typed future and the app side
can't service the listener callback because it's blocked on the typed
future.

**How to apply**: Don't call `application_event_handler.add_and_get(...)`
directly from any blocking-style API. Use `submit_and_drain<T>(event,
receiver, deadline_ms, msg, enable_wakeup)` which does
`add(event)` → `process_background_events_until(receiver, ...)`. The
iterative loop interleaves bg-event draining (invokes listener
callbacks on the caller's task) with bounded receiver waits.

## 2. Per-API wakeup matrix MUST match Java's `setActiveTask` call sites

**Why**: Java's `wakeupTrigger.setActiveTask(future)` /
`clearTask()` pattern is per-API — only APIs that Java registers the
future on observe `wakeup()`. The Rust analog is the
`enable_wakeup: bool` parameter on `process_background_events_until` /
`submit_and_drain`. Setting it `true` for an API Java doesn't
`setActiveTask` is a Java divergence; setting it `false` for an API
Java DOES setActiveTask breaks Java contract.

**How to apply**: Grep Java for `wakeupTrigger.setActiveTask` and use
that as the source of truth. Per-API matrix:
  - `commit_sync` / `commit_sync_internal` typed wait: **true** (Java line 1716)
  - `committed_timeout`: **true** (Java line 1176)
  - `partitions_for_timeout`: **true** (Java line 1223)
  - `list_topics_timeout`: **true** (Java line 1251)
  - `position` CheckAndUpdatePositions wait: **true** (Java line 1963)
  - `await_pending_async_commits`: per-call (Java line 1738)
  - everything else (subscribe, assign, seek, pause, resume,
    current_lag, beginning/end_offsets, offsets_for_times,
    leave_group_on_close): **false**

The `commit_inner` offsets-ready wait is a deliberate deviation:
`enable_wakeup=true` even though Java's `setActiveTask(commitFuture)`
fires only AFTER `commit(...)` returns. Document the deviation in a
code comment — uniform wakeup-observable semantic across all phases of
`commit_sync` is more useful than strict Java parity here.

## 3. `MemberStateListener` bridge is a small struct with `Arc<Mutex<...>>` fields shared 1:1 with the consumer

**Why**: Java's anonymous inner class
`memberStateListener = new MemberStateListener() { ... }`
(`AsyncKafkaConsumer.java:343-353`) closes over `groupMetadata` and
the `setGroupAssignmentSnapshot` method. The natural Rust translation
is a struct (`ConsumerStateNotifier`) that holds Arcs of the same
slots, shared 1:1 with the consumer struct.

**How to apply**: When Java has a `MemberStateListener` /
`AssignmentListener` anonymous inner class:
  1. Add a struct in the consumer module holding the relevant Arcs.
  2. Implement the listener trait on it.
  3. Construct it in the consumer ctor, share Arcs with the consumer
     struct fields.
  4. Expose via an accessor (`Self::state_notifier()`) so production
     wire-up (next-phase ctor) and tests can register it on the
     membership manager.

## 4. `group_assignment_snapshot` ≠ `subscriptions.assigned_partitions()`

**Why**: Java's `AtomicReference<Set<TopicPartition>> groupAssignmentSnapshot`
(`AsyncKafkaConsumer.java:317`) is updated only by the
`MemberStateListener.onGroupAssignmentUpdated` callback during
reconciliation. It deliberately excludes manual `assign(...)`
partitions AND captures partitions before they're propagated to
`SubscriptionState` (for partitions revoked mid-reconciliation).
Reading from `subscriptions.assigned_partitions()` in
`runRebalanceCallbacksOnClose` produces wrong dispatch for both edge
cases.

**How to apply**: Add a dedicated `Arc<Mutex<HashSet<TopicPartition>>>
group_assignment_snapshot` field on the consumer struct. Update it via
the `MemberStateListener` bridge (pattern #3 above). Java line
1626-1628's early-return on empty snapshot is the canonical handling
for manual-assign consumers.

## 5. Close-path timeout must be capped at `request.timeout.ms`

**Why**: Java's `createTimerForCloseRequests(timeout)` at
`AsyncKafkaConsumer.java:1590-1594` returns
`time.timer(Math.min(timeout.toMillis(), requestTimeoutMs))`. With
default config (timeout=30s, requestTimeoutMs=30s) the cap is a no-op,
but a user calling `close(Duration::from_secs(300))` would otherwise
block the consumer for 5 minutes per close-step. Each close-step is a
broker RPC bounded by `request.timeout.ms` regardless of the user's
close timeout.

**How to apply**: In `close_internal`, compute the deadline AFTER
applying the cap:
```rust
let request_timeout_ms = self.config.request_timeout_ms() as i64;
let capped_timeout_ms = std::cmp::min(timeout.as_millis() as i64, request_timeout_ms);
let close_deadline_ms = calculate_deadline_ms(close_start_ms, capped_timeout_ms);
```

## 6. `.await.ok()` on a fallible blocking-style API is ALWAYS a bug

**Why**: Java's `try/catch (TimeoutException)` is narrow — it catches
only the timeout, not other errors. The Rust equivalent
`add_and_get(...).await.ok()` silently swallows ALL errors:
`IllegalState` from a dead bg task, authorization failures, etc. The
user sees a misleading downstream Timeout instead of the root cause.

**How to apply**: Always match the error explicitly:
```rust
match drain_result {
    Ok(v) => use(v),
    Err(KafkaError::Timeout(_)) => { /* loop / continue / return Ok */ },
    Err(err) => return Err(err),
}
```

This mirrors Java's `catch (TimeoutException e) { ... }` semantics.
Test the non-Timeout-error propagation explicitly: complete the
handle with a deterministic non-Timeout error and assert the
consumer surfaces THAT error, not a generic Timeout.

## 7. Reaching `oneshot::Receiver` closure: handle drop isn't enough

**Why**: `make_completable_event` returns `(handle, receiver, erased)`.
The `erased` Arc holds a clone of the inner sender. Even if a test
drops the `handle`, the `_erased` (held by the caller's local variable
until end-of-scope) keeps the sender alive — the receiver never sees
RecvError.

**How to apply**: To test bg-side error propagation, don't try to
drop-the-handle-to-close-the-receiver. Instead, call
`handle.complete_exceptionally(KafkaError::illegal_state("test"))`
explicitly — that consumes the sender slot and sends the error.
