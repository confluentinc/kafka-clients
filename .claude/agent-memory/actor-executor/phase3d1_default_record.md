---
name: Phase 3d-1 default_record landing notes
description: Where DefaultRecord/PartialDefaultRecord live, what scope was deferred, and gotchas for Phase 3d-2 to follow up on
type: project
---

Phase 3d-1 status. **Why:** First sub-chunk of the v2 record codec; subsequent
3d sub-chunks build on this. **How to apply:** When Phase 3d-2
(`DefaultRecordBatch`) starts, follow these breadcrumbs to avoid duplication.

## What landed

- `src/common/record/default_record.rs` — `DefaultRecord` struct + free
  static helpers (`write_to`, `read_from_buffer`, `read_from_stream`,
  `size_in_bytes`, `size_in_bytes_with_sizes`, `size_of_body_in_bytes`,
  `record_size_upper_bound`). 21 tests; all 20 Java `@Test` methods from
  `DefaultRecordTest.java` translated, plus byte-level fixture and
  zero-copy assertion required by PLAN.md DoD.
- `src/common/record/partial_default_record.rs` — `PartialDefaultRecord` +
  `read_partially_from`. Tests cover all 6 `*Partial` cases plus 2 extras.
- Header / RecordHeader / RecordHeaders module already existed from earlier
  phase — re-used as-is (no rework needed).
- `byte_utils` already had every varint/varlong helper needed (read/write,
  buffer/stream forms, size_of). No new helpers required.
- `KafkaError::InvalidRecord(String)` already existed (Phase 2). Re-used.

## Surprising bits worth recording

- `DefaultRecordBatch.incrementSequence` (a 4-line static helper Java places
  on the batch class) is **temporarily inlined** in `default_record.rs` as
  `pub(crate) fn increment_sequence` because `read_from_buffer` calls it but
  `DefaultRecordBatch` isn't translated yet (Phase 3d-2). When 3d-2 lands,
  *move* this fn to `default_record_batch.rs` and update the import in both
  `default_record.rs` and `partial_default_record.rs`. The function body is
  spec-correct (matches Java byte-for-byte; verified by test
  `increment_sequence_wraps_at_i32_max`).

- `record_size_upper_bound` is `pub(crate)` + `#[allow(dead_code)]` because
  Phase 3d-2's `DefaultRecordBatch.estimateBatchSizeUpperBound` will be the
  first non-test caller. The annotation will be removed when 3d-2 lands.

- **Zero-copy on the read path** is achieved via `Bytes::split_to`. The
  caller passes `&mut Bytes` and we slice off the body, then key/value via
  successive `split_to` calls — every returned slice aliases the source
  buffer. Verified by `read_from_buffer_is_zero_copy` test (asserts pointer
  inclusion within source buffer range).

- **Zero-copy on the write path** is achieved by taking `&mut Vec<u8>` for
  the output and `Option<&[u8]>` for key/value. The caller's `Vec` is the
  batch buffer (Phase 3d-4's `MemoryRecordsBuilder` will pass its own
  buffer). No intermediate Vec allocation per record — verified by reading
  the `write_to` body: every byte goes straight to `out` via varint helpers
  or `extend_from_slice`.

- The Java `readFrom(InputStream, ...)` overload necessarily allocates a
  body-sized scratch buffer (`ByteBuffer.allocate(sizeOfBodyInBytes)` in
  Java; `vec![0u8; n]` in Rust). This is the consumer code path and is *not*
  on the producer hot path, so the allocation is acceptable. Producer goes
  through `write_to` directly.

- The `PartialDefaultRecord` Java class's `key()`/`value()`/`headers()`
  methods throw `UnsupportedOperationException`. Per CLAUDE.md rule 10 we
  return `None`/`&[]` instead — surfaces the absence without panicking in
  public API. Documented in the file's struct rustdoc.

## Tests that intentionally diverge from Java

- Java's `testBasicSerde` uses `System.currentTimeMillis()` as the base
  timestamp. The Rust translation pins it (`base_timestamp = 1_700_000_000_000`)
  for reproducibility — the test exercises offset/sequence/timestamp deltas
  identically.

- The `byte_level_fixture` test is a *new* test (not in Java) required by
  PLAN.md DoD ("Wire protocol types have byte-level encoding tests against
  known vectors"). Hand-computed from the v2 record format spec; round-trips
  through `read_from_buffer` to confirm both encoder and decoder match the
  hex literal.

## Test count delta

Baseline before 3d-1: 446 lib tests.
After 3d-1: 475 lib tests (+29: 21 in default_record, 8 in partial_default_record).

## Files added

- `src/common/record/default_record.rs` (~1,100 lines incl. tests)
- `src/common/record/partial_default_record.rs` (~440 lines incl. tests)

## Files modified

- `src/common/record/mod.rs` — added 2 modules + 2 re-exports.

## Phase 3d-2 follow-ups

1. Translate `DefaultRecordBatch`. Move `increment_sequence` and
   `decrementSequence` from `default_record.rs` into the new file. Drop the
   local `pub(crate) fn increment_sequence` and update imports in
   `default_record.rs::read_from_buffer_inner` and
   `partial_default_record.rs::read_partially_from_inner`.
2. Wire `DefaultRecordBatch.estimateBatchSizeUpperBound` to call
   `default_record::record_size_upper_bound` and remove its
   `#[allow(dead_code)]`.
