---
name: milestone11-phase3-share-fetch-path
description: KIP-932 Phase 3 share fetch data path — ShareFetch/ShareCompletedFetch/ShareFetchBuffer/ShareFetchCollector translation decisions and gotchas
metadata:
  type: project
---

Phase 3 of Milestone 11 (KIP-932 share consumer): translated the share fetch data
path on branch `milestone11-share-consumer`. Files under
`src/consumer/internals/`: `share_fetch.rs`, `share_completed_fetch.rs`,
`share_fetch_buffer.rs`, `share_fetch_collector.rs`, `share_fetch_exception.rs`,
plus blocker `node_acknowledgements.rs`.

**Why:** builds on Phases 1/2 (wire + ack core) toward the KIP-932 share
consumer; ShareConsumeRequestManager / AsyncKafkaShareConsumer consume these in
a later phase.

**How to apply (decisions to keep consistent in later share phases):**
- `ShareConsumerMetadata` NOT translated (out of scope). `ShareFetchCollector`
  holds `Arc<ConsumerMetadata>` instead — only used for `request_metadata_update`.
  Swap the field type when `ShareConsumerMetadata` lands; collect logic unchanged.
- `ShareFetchException<K,V>` is a standalone generic struct (Java: extends
  SerializationException) carrying `share_fetch` + `cause`. `collect` returns
  `Result<ShareFetch, ShareFetchException>`; ALL error exits wrap into it (Java
  throws bare KafkaException from initialize AND ShareFetchException from records
  branch — unified). Needs `#[allow(clippy::result_large_err)]` on `collect`.
- `ShareCompletedFetch` is NOT generic; `fetch_records<K,V>` is. Zero-copy cursor
  mirrors `completed_fetch.rs` (move buffer into MemoryRecords, borrow per-record
  via `DefaultRecord::read_ref_from_buffer`, decompress-once for compressed).
  KEY DIFFERENCE: interleaves broker "acquired records" (offset+delivery_count)
  with actual records, emitting gaps. Records consumed eagerly in
  `next_fetched_record`; re-parsed on offset-match via stored
  `pending_record_offset` (avoids holding a borrow across the acquired loop).
  Control batches skipped whole at header level in `load_next_batch`.
- Carryover note #1 (Phase 2): `ShareFetch::add` reads
  `get_acquisition_lock_timeout_ms()` BEFORE the consuming `merge`.
- Carryover note #2 (Phase 2): owned-record delivery — added
  `ShareInFlightBatch::take_in_flight_records` (drain) + `ShareFetch::take_records`;
  `records()` stays a borrowed read view.
- No `RecordDeserializationException` type in Rust (follows completed_fetch
  precedent): deser errors are `KafkaError::serialization` with origin+offset in
  the message; tests assert message content + exception offset set.
- Builder gotcha: appending a normal record to a control batch panics
  ("Control records can only be appended to control batches"). Build control
  batches with ZERO records to exercise the skip path.
- Generated `share_fetch_response_data::{PartitionData, AcquiredRecords}` have
  `::new()` not `::default()`; fields are pub (direct assignment ok).
- §27 allocation-budget test in share_completed_fetch.rs measured ~2.17
  allocs/record (key+value deser only) via `crate::test_alloc_tracker`.
