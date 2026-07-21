---
name: milestone11-phase5-carryover
description: Two ownership-deviation consequences from M11 Phase 2 that Phase 5 (ShareConsumeRequestManager / ShareFetch) must handle
metadata:
  type: project
---

Phase 2 Critic (N=1) flagged two mechanical consequences of the `ShareInFlightBatch`
ownership-consuming deviations (`ConsumerRecord` is not `Clone`). Phase 5 must handle
these explicitly so they aren't discovered late:

1. **`ShareFetch.add` ordering**: Java reads `getAcquisitionLockTimeoutMs()` *after*
   `merge`. Because Rust's `merge` consumes `other` by value, the Rust translation must
   read `getAcquisitionLockTimeoutMs()` *before* the consuming `merge`.
2. **Owned-record delivery path**: delivering owned `ConsumerRecord`s to the user needs
   a drain/take path — a borrow-only `get_in_flight_records` cannot transfer ownership,
   and `take_acknowledged_records` drops acknowledged records. Design the collector→user
   handoff accordingly.

See [[milestone11-share-consumer]]. Applies when spawning the Phase 5 actor
(ShareConsumeRequestManager + events).
