# Critic 32 — Phase 32 review (offset-query test parity) — RESOLVED

All three issues from the Critic 32 review have been addressed by Actor 32.
Build / `cargo test --lib` / `cargo xtask lint` / `cargo xtask format-check` all
green; ORM test count 72 → 74 (+2 new tests). Resolutions below.

---

## Issue 1 (RESOLVED): build-time partial park + merge path untested
- **Resolution**: Translated `testGetOffsetsForTimesWhenSomeTopicPartitionLeadersNotKnownInitially`
  as `fetch_offsets_build_time_partial_park_merges_after_metadata_update`
  (`src/consumer/internals/offsets_request_manager.rs`). Drives the
  `Ok`-with-non-empty-`remaining_to_search` branch of
  `build_list_offsets_requests` (Java `OffsetsRequestManager.java:575-583`): two
  known-leader partitions (t1-p0, t1-p1 on 3 nodes) build into requests while a
  third partition on an initially-unknown topic (t2-p0) parks in
  `remaining_to_search` and triggers `requestUpdate(true)`. Round-1 responses
  merge → re-park (`requestUpdate(false)`) → second metadata refresh adds t2 →
  parked request replays → t2-p0 resolves → the global result MERGES all three
  offsets (11/32/54). Asserts concrete values + the partial-build merge.
  New helper `bootstrap_metadata_multi_topic`.

## Issue 2 (RESOLVED — chose option (a)): requestUpdate(true) vs (false) now pinned
- **Resolution / option chosen**: option (a) — the distinction IS meaningful
  (the boolean governs the `equivalent_response_count` backoff reset, real
  production behavior that Java verifies exactly via Mockito). Added two
  `#[cfg(test)] pub(crate)` `Metadata` hooks: `equivalent_response_count_for_test()`
  and `set_equivalent_response_count_for_test(n)` (`src/metadata.rs`). Every
  `requestUpdate`-asserting test now seeds the counter to a known non-zero value,
  then asserts it was RESET to 0 for `requestUpdate(true)` paths
  (`fetch_offsets_unknown_leader_parks_on_retry`,
  `fetch_offsets_metadata_update_retries_successfully`,
  `fetch_offsets_build_time_partial_park_*`) or NOT reset for `requestUpdate(false)`
  paths (`fetch_offsets_partial_retriable_error_merges_after_retry`,
  `fetch_offsets_retriable_error_retries_after_metadata_update` ×10,
  `fetch_offsets_unknown_leader_epoch_is_retriable`,
  `offsets_for_times_retriable_retry_triggers_metadata_update` ×7). A regression
  flipping true↔false is now caught. Overstated test comments corrected; PLAN.md
  "requestUpdate observability" section rewritten.

## Issue 3 (RESOLVED — translated, not just re-documented): disconnect-skip rationale
- **Resolution**: Translated the in-scope ORM fetch-path behavior as
  `fetch_offsets_disconnect_fails_global_result_without_reparking`
  (`src/consumer/internals/offsets_request_manager.rs`). A per-node disconnect
  routes to `fail_request_state` → fails the whole `fetch_offsets` future with
  `NetworkException`, leaving NOTHING parked for retry (Java
  `OffsetsRequestManager.java:586`/`:600`). The Java test's retry-and-succeed is
  a classic-`OffsetFetcher`/`ConsumerNetworkClient` property (OUT_OF_SCOPE §20)
  and is not reproducible on the ORM. Asserts the typed `NetworkException`
  variant + message content + `requests_to_retry_count() == 0`. This branch was
  previously covered only on the reset path (Phase 31). New helper
  `build_network_disconnect_client_response()` (pure disconnect, no auth
  annotation → maps to `NetworkException`). PLAN.md rationale corrected.

## Minor / non-blocking observations (RESOLVED — acknowledged in PLAN.md)
- single-leader vs two-leader batching in `offsets_for_times_multi_partition_mixed_errors`:
  acknowledged in PLAN.md "Minor / non-blocking notes" — fidelity reduction, not
  a coverage hole (two-leader merge covered elsewhere).
- destructive `build()` in `complete_all_unsent_with_per_partition_response`:
  acknowledged in PLAN.md — no test rebuilds the same unsent twice; completion
  goes through the handler/receiver captured at request-creation time.
