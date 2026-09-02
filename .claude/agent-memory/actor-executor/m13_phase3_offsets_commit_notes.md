---
name: m13-phase3-offsets-commit-notes
description: M13 Phase 3 — CommitRequestManager OffsetFetchResult, retriable partition errors, OffsetFetcherUtils lag helpers
metadata:
  type: project
---

Milestone-13 Phase 3 (AK 4.3.1 consumer offsets/commit) landed on `milestone-12-ak-4.3.1`.

**KAFKA-20165 (CommitRequestManager, `src/consumer/internals/commit_request_manager.rs`)**
- New Rust `pub(crate) struct OffsetFetchResult { offsets: HashMap<TP, Option<OAM>>, retriable_partition_errors: HashMap<TP, Errors> }` mirrors Java's inner class (DoD #7 legit). `FetchResult` Ok payload changed from the bare map to `OffsetFetchResult`. Methods: `offsets()`, `retriable_partition_errors()`, `has_retriable_partition_errors()`, `to_offset_map_with_nulls()`.
- `handle_offset_fetch_response`: UNKNOWN_TOPIC_OR_PARTITION / UNKNOWN_TOPIC_ID now tracked as retriable partition errors (continue loop), NOT `send(Err("Topic does not exist"))`.
- Retry driver `fetch_offsets_with_retries` gained a partition-error branch in `Ok(Ok(value))`: retry until deadline reached (`current + backoff >= deadline`), then return PARTIAL results as `Ok(value)` (not a TimeoutException — that's only the group-level path).
- Callers apply `to_offset_map_with_nulls()`: `OffsetsRequestManager::init_with_committed_offsets_if_needed` spawn and AEP `process_fetch_committed_offsets` (the only FetchCommittedOffsetsEvent hunk I was allowed to touch in AEP).
- `maybeUpdateLastSeenEpochIfNewer` on the fetch path is applied DOWNSTREAM in `OffsetsRequestManager::refresh_offsets` (idempotent), not in the driver — pre-4.3.1 structural choice, documented at the driver site.
- Test classification flip: `offset_fetch_request_partition_data_error` UNKNOWN_TOPIC_* → retriable. Added `offset_fetch_returns_partial_results_on_retriable_partition_errors_when_deadline_reached`.

**OffsetFetcherUtils (`offset_fetcher_utils.rs`)**: added `update_subscription_state` else-warn branches, `maybe_set_partition_end_offset_request`, `clear_partition_end_offset_requests`. Sole Java caller is classic `OffsetFetcher` (untranslated §20); file has module `#![allow(dead_code)]` so unused is fine. Async lag path stays inline in AEP `process_current_lag`.

**Blocker pulled forward from Phase 4**: `SubscriptionState::maybe_clear_partition_end_offset_requested` + `TopicPartitionState::clear_end_offset` (new AK 4.3.1 methods). Phase 4 will find them already present when it applies the SubscriptionState.java diff — do NOT re-add.

**Wrappers**: `OffsetFetchRequest::request_all_offsets(&group)` = `group.topics.is_none()` (broker-only callers, wire-parity). `ConsumerGroupHeartbeatResponse::create_assignment` input widened `HashMap<Uuid, HashSet<i32>>` → `HashMap<Uuid, HashMap<i32,i32>>` (KIP-1251), uses `into_keys()`.

**Import-only (record→record::internal, N/A)**: OffsetFetchResponse, OffsetsForLeaderEpochUtils, OffsetFetchRequestTest/OffsetFetchResponseTest, OffsetFetcherTest (also classic §20).
