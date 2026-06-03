---
name: phase10-commit-3b-notes
description: Phase 10 (3b/N) — OffsetsRequestManager::update_fetch_positions translation; spawn-async chain that needs &mut self deferred via channel; mid-flight partition capture
metadata:
  type: project
---

Phase 10 commit 3b translated
`OffsetsRequestManager.updateFetchPositions(long deadlineMs)`
(Java `OffsetsRequestManager.java:235`). Reusable patterns:

## Pattern 1: `&mut self` work scheduled by a spawned task → defer via channel

Java's `whenComplete` chains can call manager methods that mutate
`requestsToSend` from inside the response callback because Java
single-threads the bg thread. Rust spawned futures can't borrow
`&mut self` from the manager owner.

**Solution**: a dedicated `mpsc::UnboundedSender<PendingFollowupReset>`
held by the manager. The spawned response callback sends a message; the
manager's `poll(&mut self)` drains the channel and runs the deferred
`&mut self` work (here: `reset_positions_if_needed` to enqueue
ListOffsets requests).

Trade-off: ListOffsets requests are enqueued on the **next** poll, not
inline with the response. Tests don't assert "ListOffsets enqueued
immediately after OffsetFetch response" so this is invisible. Java
ALSO can't enqueue immediately — its enqueueing happens at "the next
runOnce iteration after the response handler", which is structurally
the same thing.

## Pattern 2: capture the initializing-partition set at call entry

`updateFetchPositions` captures `subscriptionState.initializingPartitions()`
once, before issuing the OffsetFetch. The captured set is used **twice**:
(a) as the OffsetFetch request payload, (b) as the FILTER passed to
`resetInitializingPositions` in the response handler.

If you read `initializing_partitions()` AGAIN in the response handler
(after the response arrives), you can include partitions added to the
assignment mid-flight — which is wrong. Java's
`testUpdatePositionsDoesNotResetPositionBeforeRetrievingOffsetsForNewlyAddedPartition`
asserts the filter restricts to the captured set.

In Rust, clone the `HashSet<TopicPartition>` once at entry, move into
the spawned task, and use it as the filter predicate
`|tp| captured.contains(tp)`.

## Pattern 3: synchronous error path also goes through `cacheExceptionIfEventExpired`

Java's `cacheExceptionIfEventExpired` is a `whenComplete` on the
inner `updatePositionsWithOffsets` result. The `whenComplete` fires
on ANY completion — including immediate synchronous failures (caught
by the outer `try` and propagated via `completeExceptionally`).

In Rust the synchronous error path must explicitly call
`maybe_cache_update_positions_exception` before firing the outer
oneshot with `Err(...)`. Easy to miss: the spawned-task path has the
cache logic at the end; the sync-error path is a separate code branch
and must duplicate it.

## Pattern 4: separate "fetch reuse" testing from "fetch completion" testing

`init_with_committed_offsets_if_needed` has two distinct branches:
- New OffsetFetch (issues request via commit manager).
- Reuse (registers a new waiter on existing pending event).

Tests at the `update_fetch_positions` level should verify both branches
fire `reset_initializing_positions` correctly when the underlying fetch
completes. To drive the fetch to completion in unit tests without
standing up a network client, add a `#[cfg(test)] pub(crate) fn
complete_first_unsent_fetch_for_test(offsets)` helper on
`CommitRequestManager` that pops the unsent request and resolves its
inner oneshot directly. Mirrors the existing `inner_state_for_test`
pattern.

## Pattern 5: `yield_until` helper for chained spawned futures

After driving an inner oneshot to completion, the chain of awakening
futures (commit-mgr's `fetch_offsets_with_retries` → init-with-committed's
fan-out task → `spawn_committed_offsets_followup`) needs multiple
runtime ticks to propagate on a current-thread runtime.

Tests can use:

```rust
async fn yield_until<F: Fn() -> bool>(predicate: F) {
    for _ in 0..16 {
        if predicate() { return; }
        tokio::task::yield_now().await;
    }
}
```

Better than `sleep` because deterministic and fast. Predicate observes
the manager state via the shared `Arc<Mutex<SubscriptionState>>`.

## Wall-clock time in spawned tasks

The spawned `cacheExceptionIfEventExpired` callback reads
`time.milliseconds()` in Java — captured AT THE TIME the callback
runs, not at entry. Rust uses `std::time::SystemTime::now()` inside
the spawned task; tests passing `deadline_ms = i64::MAX` always
exercise the non-expired branch, which is what existing tests want.

A test-controllable time source would require threading a `Time`
trait through the manager (large surface area). Deferred to a later
phase if needed.

## Deferred from this commit (out of scope)

- `cachedUpdatePositionsException` cache-set path triggered by the
  spawned `cacheExceptionIfEventExpired` is exercised at the
  cache-CONSUMPTION side (`maybe_complete_with_previous_exception`)
  but NOT at the cache-SET side. Would require a wall-clock-mock
  fixture to test directly.
- ListOffsets request completion future chain — Rust's
  `reset_positions_if_needed` is fire-and-forget, no completion
  future. Java's `resetPositionsIfNeeded` returns
  `CompletableFuture<Void>` that completes when all ListOffsets
  responses arrive. This is a known existing limitation predating
  Phase 10 (3b); flag for Phase 11+.
