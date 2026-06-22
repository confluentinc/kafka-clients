---
name: phase10-commit-3a-notes
description: Phase 10 (3a/N) — OffsetsRequestManager relocate of init_with_committed_offsets_if_needed; reusable patterns for CompletableFuture fan-out via oneshot
metadata:
  type: project
---

Phase 10 commit 3a (Milestone-8) relocated
`init_with_committed_offsets_if_needed` from `CommitRequestManager` to
`OffsetsRequestManager`, mirroring Java placement
(`OffsetsRequestManager.java:364`). Reusable patterns:

## CompletableFuture fan-out → oneshot + waiters vec

Java's `pendingOffsetFetchEvent` reuse logic relies on a single
`CompletableFuture` with N downstream `whenComplete` handlers. Rust's
`oneshot::Receiver` is single-consumer, so the pattern in commit 3a is:

```rust
struct PendingFetchCommittedRequest {
    requested_partitions: HashSet<TopicPartition>,
    waiters: Vec<oneshot::Sender<Result<(), KafkaError>>>,
}
```

The driver task takes the inner fetch oneshot, awaits it, then drains
the waiters vec, cloning the result (`KafkaError` is `Clone`) into each
sender. The `pending_offset_fetch_event` slot is taken/cleared inside
the same critical section that drains the waiters — this mirrors Java
clearing the slot inside `whenComplete`.

**Why:** Avoids the `Pin<Box<Future + Shared>>` pattern, which would
complicate the test surface (Shared future cancellation semantics differ
from oneshot's). Adds ~5 LOC per reuse path; readable.

## Test fixtures: `inner_state_for_test` accessor

When a sibling module's tests need to assert state inside a
`pub(crate)`-private struct (here: counting unsent OffsetFetch requests
on `CommitRequestManager`), add a `#[cfg(test)] pub(crate)` accessor on
the owning struct rather than making fields pub.

This avoids leaking test-only state into the public API surface while
still letting cross-module integration tests assert without reaching
through `Arc<Mutex<...>>` indirection chains.

## `Option<Arc<DependencyManager>>` for nullable Java deps

Java passes `commitRequestManager` as non-null to the
`OffsetsRequestManager` constructor; internal logic short-circuits when
not in a group. Rust collapses both cases into
`Option<Arc<CommitRequestManager>>` — passing `None` for the group-less
path. The Java null-check moves to the call site
(`init_with_committed_offsets_if_needed` returns `Ok(())` immediately
when `None`).

Arc-cycle audit: search the dependency direction
(`grep -rn "OtherManager" path/to/this_manager.rs`) before adding an
`Arc<Other>` field — if Other holds Arc<Self> anywhere, switch one side
to `Weak<...>` or break the cycle architecturally.

## Default-API-timeout arithmetic with saturating_add

Java's `Math.max(deadlineMs, time.milliseconds() + defaultApiTimeoutMs)`
overflows silently in Java. Rust must use `saturating_add` (i64::MAX
math overflows panic in debug builds). Pattern:

```rust
let fetch_deadline_ms = deadline_ms.max(
    current_time_ms.saturating_add(self.default_api_timeout_ms),
);
```

The saturating_add is required if `deadline_ms == i64::MAX` (caller
passes "no deadline") and `current_time_ms` is also large.
