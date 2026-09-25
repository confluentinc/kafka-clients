# Critic 63 — Milestone 13 Phase 3 (resolved)

## Finding 1 — `committed()` path drops the last-seen-epoch update; recorded-skip line 81 overstates equivalence — RESOLVED

- **File**: `src/consumer/internals/commit_request_manager.rs` (driver +
  `events/application_event_processor.rs` FetchCommittedOffsets handler);
  `src/consumer/internals/offsets_request_manager.rs::refresh_offsets`;
  `design/history/Milestone-13/PLAN.md:81`.
- **Severity**: Behavior Mismatch (pre-existing — NOT introduced by this
  phase; reported for accuracy of the Phase-3 skip record, low priority).
- **Java Reference**: `CommitRequestManager.java` `handleSuccessfulOffsetFetch`
  / `handleRetriablePartitionErrors` both call
  `maybeUpdateLastSeenEpochIfNewer(res.offsets())` **inside the driver**, on
  **all** fetched offsets, regardless of caller.
- **Description**: Java updates the `Metadata` last-seen leader-epoch cache
  for every fetched offset on **both** the `updateFetchPositions` path (via
  `OffsetsRequestManager`) **and** the public `committed()` path (via
  `FetchCommittedOffsetsEvent`). The Rust translation relocated the update to
  `OffsetsRequestManager::refresh_offsets`, where it is (a) reached **only**
  by the position-init path — the AEP `FetchCommittedOffsetsEvent` handler
  just strips + completes and never touches metadata — and (b) gated on
  `currently_initializing.contains(tp)`, so it is narrower than Java's
  "all `res.offsets()`". Net: `consumer.committed(..)` in Rust does not
  refresh the leader-epoch cache that Java refreshes.
- **Related, same root cause — public-API contract**: Java's `committed()`
  returns `toOffsetMapWithNulls()`, i.e. a map with the requested partition
  **present with a `null` value** for uncommitted **and** (now, KAFKA-20165)
  retriable-errored partitions. The Rust AEP handler's target type is
  `HashMap<TopicPartition, OffsetAndMetadata>` (no `Option`), so it strips
  those entries — the partition is **absent**.

**Resolution (Manager decision):** the underlying behavior mismatch is
**pre-existing** — it predates this milestone and is not part of the
4.2→4.3.1 delta, so it is NOT fixed in this phase; it is recorded
accurately instead.

1. Tightened the overstated recorded-skip wording at
   `design/history/Milestone-13/PLAN.md` (~line 81): removed the "for both
   full-success and partial-result paths, idempotent" equivalence claim and
   replaced it with a NOTE stating the two limitations (position-init-only
   path + `currently_initializing` gate; `committed()` never refreshes
   epochs), cross-referencing §5.
2. Added `## 5. Known pre-existing divergences (not part of the 4.2→4.3.1
   delta — follow-up candidates)` at the bottom of the PLAN recording both
   the leader-epoch-cache gap and the present-with-null vs absent contract
   divergence, with the Rust and Java locations cited.
