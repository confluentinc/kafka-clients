# Phase 7d review — resolutions for COMMENTS.1.md

Reviewed at SHA `8c36517` by Critic agent N=4. 7 findings; 2 fixed by
Manager in `be2fdba` (sub-Actor sandbox blocked worktree access on 7c;
Manager applied mechanical fixes directly on 7d too). 5 are
non-blocking tracking items for Phase 8-10.

## Fixed

### #2 — Reset path HW/LSO corruption removed

Dropped `update_subscription_state(&result.fetched_offsets, ...)` from
`drain_pending_completions::ListOffsetsForReset` arm. Matches Java
`OffsetsRequestManager.java:613-650` which does NOT call
`updateSubscriptionState` in the reset path (that's only for the
multi-node `fetchOffsets` flow at `:570-573`, currently deferred).

Inline comment added explaining why.

Commit: `be2fdba` (fixup of `a7d7f66`).

### #4 — NodeIsEmptyExt dead code removed

Deleted the `NodeIsEmptyExt` trait + impl (whose `id() < 0` logic was
wrong, AND was shadowed by `Node`'s inherent `is_empty()` anyway). Call
site updated to use `node.is_empty()` directly. Dropped unused `Node`
import.

Commit: `be2fdba` (same).

## Not actionable / tracked for Phase 8-10

### #1 — `fetch_offsets` deferral rationale incorrect

Actor's notes claim `fetch_offsets` depends on `CommitRequestManager`.
Reading Java: only `updateFetchPositions` and `initWithCommittedOffsetsIfNeeded`
depend on commit-manager. `fetchOffsets` itself does NOT.

The deferral is still defensible (the `requests_to_retry` queue +
`ListOffsetsRequestState` aren't implemented), but the rationale needs
updating. **Phase 8 plan should pick this up:** translate `fetch_offsets`
when adding `OffsetsClusterListener::on_update` (which the queue feeds).

### #3 — `?` operator in `update_subscription_state`

`update_subscription_state` returns `Result<...>`; used with `?` in the
completions-drain loop. If `?` fires, remaining completions drop. Java
is `void`. Today the inner `?` is unreachable behind `is_assigned`
guard, but the pattern is brittle.

**Phase 8/9 should:** make `update_subscription_state` infallible
(internal `try_updating_*` + log-and-skip).

### #5 — Lock released between snapshot and per-iter validation

Java holds `synchronized SubscriptionState` across the iteration; Rust
snapshots and re-locks. Partitions newly assigned during the cycle are
missed in the current metadata-change pass (next pass catches them).
Benign, but a real divergence.

**Phase 8+ should:** expose `*_locked` variant of
`maybe_validate_position_for_current_leader` that works on existing
`&mut SubscriptionState`.

### #6 — Missing-API-versions branch silently skips

Java calls `networkClientDelegate.tryConnect(node)` when API versions
aren't yet known. Rust just logs. Partition stays in `AWAIT_VALIDATION`
until something else triggers a connection.

**Phase 8/9 should:** plumb a `try_connect` channel/handle into
`OffsetsRequestManager`.

### #7 — `OffsetsClusterListener::on_update` no-op

Java drains `requestsToRetry` and re-prepares deferred requests on
metadata updates. Rust no-op. Correct today (no `fetch_offsets` →
no retry-queue entries). **Phase 8+ MUST** wire this listener when
`fetch_offsets` lands.

---

## Final state

- Lib tests: 1245 (baseline preserved).
- 5 OffsetsRequestManager tests still pass.
- `cargo build`, `cargo xtask format-check`, `cargo xtask lint`: all clean.
- 9 deferred-from-Phase-4 `SubscriptionStateTest` cases all pass.
- §27 not in scope for 7d.
- Phase 7d ready for merge back to `consumer-impl` once 7b also closes.
