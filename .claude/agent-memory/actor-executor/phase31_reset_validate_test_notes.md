---
name: phase31-reset-validate-test-notes
description: Phase 31 reset/validate/LogTruncation test parity — production bug in reset send path (missing-leader + leader-epoch), structured-payload-via-OFU pattern, OFLE/ListOffsets response-driving helpers
metadata:
  type: project
---

# Phase 31 — reset / validate / LogTruncation test parity

Test-only phase (Actor 31) closing the report-03 reset/validate/LogTruncation
gap. 27 new test fns across ORM (23), OFU (3), OFLE (1). All on
`consumer-impl`, not a worktree.

## Production bug found and fixed (commit b406b1a)
`send_list_offsets_requests_and_reset_positions` (offsets_request_manager.rs)
did NOT mirror Java's `groupListOffsetRequests`
(OffsetsRequestManager.java:892-914). Two divergences, both Java-asserted:
1. Partitions with an unknown leader were silently dropped via
   `regroup_partition_map_by_node` WITHOUT calling
   `metadata.requestUpdate(true)` (testResetPositionsMissingLeader).
2. The `ListOffsetsPartition` was built WITHOUT `set_current_leader_epoch`
   (testGetOffsetsIncludesLeaderEpoch wants epoch 99 on the wire, not the
   UNKNOWN_EPOCH sentinel).
Fix: iterate per-partition like Java — `metadata.current_leader(tp)`; if
leader None → `request_update(true)` + skip; else stamp
`current_leader_epoch` (epoch.unwrap_or(UNKNOWN_EPOCH)). The fetchOffsets
path was already correct; only the reset path had this bug.

## LogTruncation structured payload: test at OFU level, not ORM
`KafkaError::from(ConsumerError::log_truncation(..))` FLATTENS to
`KafkaError::IllegalState(message)` — the structured fields
(offset_out_of_range_partitions, divergent_offsets) are LOST after the
conversion (documented design choice, src/consumer/errors.rs:237). So assert
the structured payload directly against the `LogTruncation` struct returned by
`OffsetFetcherUtilsState::on_successful_response_for_validating_positions`
(OFU-level test), and assert only the re-raised message string at ORM level
(`err.message().contains("Truncated partitions detected with divergent
offsets")`). `LogTruncation` fields: `topic_partition`, `fetch_position`
(`.offset` == offsetOutOfRange value), `divergent_offset_opt`
(`Option<OffsetAndMetadata>`; None for UNDEFINED epoch/offset cases).

## Driving response paths through the ORM (reuse, don't reinvent)
- ListOffsets reset response: `complete_first_unsent_with_response(mgr,
  build_list_offsets_response(topic, vec![(part, Errors, ts, offset, epoch)]),
  now)` then `RequestManager::poll(mgr, now)` to drain the PendingCompletion.
- OffsetsForLeaderEpoch validate response: NEW helpers added —
  `build_offsets_for_leader_epoch_response(topic, vec![(part, Errors,
  leader_epoch, end_offset)])` + `build_oitle_client_response(resp)` +
  `complete_first_oitle_with_response(mgr, resp, now)`. Mirror the ListOffsets
  helpers exactly (wrap in ConcreteResponse::OffsetsForLeaderEpoch).
- Both forwarders spawn a task; after `on_complete` you MUST
  `for _ in 0..16 { yield_now().await }` then `poll` to drain.

## Test fixtures
- Reset: `assign_and_request_reset(subs, tp, strategy)` → assign +
  `request_offset_reset(tp, strategy)`. `bootstrap_metadata_with_topic` gives
  leader = node 0 (id), `bootstrap_metadata_with_epoch` adds a per-partition
  leader epoch via `metadata_update_with_cluster_id` epoch supplier.
- Validate: `seek_unvalidated_and_install_api_versions(mgr, subs, tp, offset,
  epoch)` → seek_unvalidated into AWAITING_VALIDATION + install
  `NodeApiVersions::create()` for node 0. For old-broker skip, override OFLE
  to v0-v2 via `create_with_overrides(&[api_version])` (ApiVersion from
  `crate::api_versions_response_data::ApiVersion`).
- NONE reset policy needs `new_manager_none_reset()`; `new_manager_with_commit`
  hardcodes EARLIEST.

## Gotchas
- `build_disconnected_client_response` carries an AUTH exception → maps to
  non-retriable SaslAuthenticationFailed. For a plain retriable disconnect
  (Java DisconnectException → NetworkException), build a ClientResponse inline
  with `disconnected=true` and authentication_exception=None.
- Retry-backoff between reset attempts: `set_next_allowed_retry` pushes to
  `now + request_timeout_ms` (30_000 in new_manager_with_commit), so advance
  `now` by 60_000 between attempts in multi-error tests, NOT retry_backoff_ms.
- `Node::no_node()` returns `&'static Node` — `.clone()` it.
- in-flight reset family: stale-discard guard is
  `SubscriptionState::maybe_seek_unvalidated` (skips if not AWAIT_RESET or
  strategy changed). seek-discard + idempotent-apply cover the shared path;
  the other 3 variants (assignment/strategy-change/earlier-late) assert the
  same outcome — documented as folded in PLAN.md.

## Documented skips
- testresetPositionsSkipsBlackedOutConnections (OUT_OF_SCOPE, classic
  connection blackout).
- testOffsetValidationSkippedForOldResponse (needs forging a sub-v9 metadata
  response version through ConsumerMetadata — no test-builder hook; skip logic
  is a SubscriptionState concern unit-tested elsewhere).
- testOffsetValidationFencing (epoch fencing re-validation is a
  SubscriptionState concern; structurally covered by seek-with-inflight +
  metadata-change validate path).

## See also
- [[phase7d_design_notes]] — OffsetsRequestManager / OFU / OFLE structure
- [[phase13a-fetch-test-notes]] — auto-create N-partition cluster, response
  swallow gap
