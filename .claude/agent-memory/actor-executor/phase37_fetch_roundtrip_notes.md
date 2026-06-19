---
name: phase37-fetch-roundtrip-notes
description: Milestone-8 Phase 37 — FetchRequestManager round-trip harness, KIP-951 leadership fix, transaction/OOR test limitations
metadata:
  type: project
---

Phase 37 closed report-01 finding #1 (no MockClient-driven FetchRequestManager
behavioral harness). Landed as: harness-prep+production-fix, harness, 7a, 7b.
Harness + 57 round-trip tests live in `fetch_request_manager.rs` `mod round_trip`.

**Why:** integration-level fetch behaviors (topic-id session, buffered exclusion,
transactions, preferred-replica, leadership) were untested at any layer.

**How to apply (reusable patterns):**

- **Round-trip harness shape**: don't need the MockClient send leg for decode/
  position/leadership. Drive `AbstractFetch::prepare_fetch_requests(now, |_|false,
  |_|Ok(()))` → build `FetchRequest` via `create_fetch_request(node,data).build()`
  for wire-field asserts → `handle_fetch_success/_failure` (= what
  `networkClientDelegate.poll` invokes) → `FetchCollector::collect_fetch`. The
  existing `test_response_routing_*` already cover the MockClient UnsentRequest
  handler dispatch.
- **Metadata seeding with topic-ids + leader epoch**: `metadata_update_with_ids`
  (Java's `metadataUpdateWithIds`) + `metadata_arc().update_with_current_request_version`.
  Validated position via `FetchPosition::with_leader(off, Some(epoch),
  LeaderAndEpoch::new(cluster.leader_for(tp).cloned(), Some(epoch)))` then
  `seek_validated`. A plain `seek(tp, off)` is unvalidated and won't fetch.
- **Wire-field asserts**: `req.version()` (latest if topic-id present else 12);
  `req.data().topics[..].topic_id`, `.partitions[..].{fetch_offset,
  current_leader_epoch}`; `req.data().forgotten_topics_data` (forget list);
  session id/epoch via `req.metadata()`.
- **Full fetch requires ALL session partitions in the response** — a response
  missing a requested partition makes `FetchSessionHandler::handle_response`
  return false → nothing buffered. Deliver every requested partition (use empty
  records `build_records(off,0,off)` for the ones you don't want collected).
- **collectSelectedPartition / partial collection**: a consumed CF is evicted
  lazily at the START of the NEXT collect, and `is_consumed` only flips after an
  empty pull (max_poll_records == count leaves it un-flagged). To collect ONE
  partition and leave another buffered, PAUSE the others before collect and
  resume after (Java's `collectSelectedPartition`); the collector re-enqueues
  paused CFs un-consumed.
- **buffered-partition exclusion**: `compute_buffered_nodes` only counts
  fetchable buffered partitions. Buffer tp0+tp1 on one node, collect tp0
  (pause-trick), mutate tp1 (unassign / leaderless via `set_position` with
  `LeaderAndEpoch::no_leader_or_epoch()` / pause / `request_offset_reset_default`
  / `mark_pending_on_assigned_callback(&[tp],true)`) → next build issues only tp0.

**Production fix (KIP-951, perf-neutral)**: `AbstractFetch::handle_fetch_success`
was missing Java's `AbstractFetch.java:205-251` per-partition leadership-change
branch — for NOT_LEADER_OR_FOLLOWER/FENCED_LEADER_EPOCH with current_leader
id/epoch != -1, accumulate `partitions_with_updated_leader_info`, then after the
loop build leader nodes from `response.data().node_endpoints` and call
`metadata.update_partition_leadership(...)` + `maybe_validate_position_for_current_leader`.
Re-added the `ApiVersions` param to `AbstractFetch::new` / `FetchRequestManager::new`
(Phase 7a had dropped it). Hot-path cost: one per-partition i16 error compare +
empty-Vec node_endpoints clone (no alloc) + is_empty() check; §27 alloc-budget
test still passes.

**Documented Rust limitations hit (faithful where reachable):**
- ControlRecordType (ABORT vs COMMIT marker translation) NOT implemented:
  a READ_COMMITTED control batch from an ABORTED producer returns
  `KafkaError::UnsupportedVersion`. So control-marker-dependent transaction tests
  (the abort-marker half) are omitted; aborted-txn skipping is tested via the
  `aborted_transactions` metadata list + `partition_with_aborted_txns`. A COMMIT
  marker from a NON-aborted producer is fine (just a skipped control batch).
  Build control batches via flip-control-flag-bit + recompute crc32c (mirror
  completed_fetch.rs `control_batch`).
- OFFSET_OUT_OF_RANGE flattens to `KafkaError::IllegalState`, which the collector
  ALWAYS propagates (line ~349 `is_illegal_state`), unlike Java's KafkaException
  swallow-while-non-empty. Deliver the erroring partition in a SEPARATE fetch from
  the records-bearing one.
- An OOR/error CF with 0 record bytes is DISCARDED (not raised) — give erroring
  OOR partitions non-empty records so the CF survives to raise.

Skips (OUT_OF_SCOPE, documented in PLAN.md): all metric tests (no metrics
framework), testFetchDuring{Eager,Cooperative}Rebalance (classic §20).
