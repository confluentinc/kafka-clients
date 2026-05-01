---
name: Phase 3d-2 default_record_batch landing notes
description: DefaultRecordBatch + log-input-stream + record-batch-iterator + RecordValidationStats translation; what landed, what's deferred for 3d-3+
type: project
---

Phase 3d-2 status. **Why:** Second sub-chunk of the v2 record codec; the
class is the v2 batch wire codec and the gateway between Phase 3d-1
(`DefaultRecord`) and the upcoming Phase 3d-3 (`MemoryRecords`). **How to
apply:** When Phase 3d-3 starts, follow these breadcrumbs to avoid
duplication and find the helpers ready for `MemoryRecords::batches()`.

## What landed

- `src/common/record/default_record_batch.rs` — `DefaultRecordBatch` struct
  + free static helpers (`write_empty_header`, `write_header`,
  `size_in_bytes_records`, `size_in_bytes_simple`,
  `estimate_batch_size_upper_bound`, `increment_sequence`,
  `decrement_sequence`). Implements `RecordBatch` and `MutableRecordBatch`.
  ~990 lines + ~700 lines of tests. The `increment_sequence` helper moved
  here from Phase 3d-1's `default_record.rs` per the deferred follow-up
  note.
- `src/common/record/byte_buffer_log_input_stream.rs` —
  `ByteBufferLogInputStream` impl of the `LogInputStream` trait. Walks a
  `&[u8]` buffer and yields successive `DefaultRecordBatch` instances.
  Legacy v0/v1 magic bytes are explicitly rejected with
  `KafkaError::CorruptRecord` (PLAN.md scope: producer client only handles
  magic >= 2).
- `src/common/record/log_input_stream.rs` — `LogInputStream<T>` trait
  (Java's package-private interface). Generic over the batch type so the
  iterator wrapper composes.
- `src/common/record/record_batch_iterator.rs` — `RecordBatchIterator<S, T>`
  wrapping any `LogInputStream`. Iterator yields `Result<T, KafkaError>`;
  errors poison the iterator (matches Java's "throw and stop").
- `src/common/record/record_validation_stats.rs` —
  `RecordValidationStats` plain data class. 5 tests.

## Surprising bits worth recording

### Storage model: `Vec<u8>`, not `bytes::Bytes`

Java backs `DefaultRecordBatch` with a `ByteBuffer` because it needs both
random-access reads (header offsets) AND in-place mutation
(`set_max_timestamp`/`set_partition_leader_epoch`/`set_last_offset`). The
`bytes` crate's `Bytes` is immutable and `BytesMut` doesn't share storage
on clone. So we use a plain `Vec<u8>` for the owned buffer. On
iteration we materialize a `Bytes::copy_from_slice(&buffer[RECORDS_OFFSET..])`
ONCE — per-record key/value slices then alias inside that single `Bytes`,
preserving zero-copy within a batch (1 alloc per `iter()`, 0 allocs per
record).

Phase 3d-3 (`MemoryRecords`) and 3d-4 (`MemoryRecordsBuilder`) will own
their own buffers and pass `&[u8]` slices into `DefaultRecordBatch::new`.

### `MutableRecordBatch::set_max_timestamp` recomputes CRC

`set_last_offset` and `set_partition_leader_epoch` modify fields that lie
*outside* the CRC range (CRC covers `ATTRIBUTES_OFFSET..end-of-batch`).
Setting them does NOT invalidate the CRC, matching Java exactly. Only
`set_max_timestamp` rewrites the CRC because it touches both attributes
(timestamp-type bit) and the max-timestamp field, which are inside the CRC
range.

### Compressed iteration synthesizes a fresh `BufferSupplier`

`Compression::wrap_for_input` takes a `BufferSupplier` by value (Phase 3c
trait shape). Java's `streamingIterator(BufferSupplier)` passes the
caller's supplier by reference. Rust workaround: `mem::replace` the
caller's supplier with a fresh `NoCaching` supplier, hand the original
to the codec. The codec keeps it alive for the iteration. When the
caller's supplier is dropped (or reused for another batch) the original
caches are gone. **Behavioural difference vs Java:** the supplier's cache
doesn't survive across calls if the iterator is fully consumed and the
codec drops the supplier. Acceptable for now — Phase 3d-3 callers will
typically use `BufferSupplier::create()` per batch anyway.

### Test helper `build_uncompressed_batch` lives in 3 test modules

The same helper logic is duplicated across `default_record_batch::tests`,
`byte_buffer_log_input_stream::tests`, and `record_batch_iterator::tests`
because Rust's test modules can't see each other's helpers without a
shared `#[cfg(test)]` module. When `MemoryRecordsBuilder` lands in 3d-4,
all three test modules can re-route through it and the duplication
disappears. Until then, the duplication is documented in each file.

### Tests that diverge from Java's `MemoryRecords`-driven path

Java's `DefaultRecordBatchTest` builds batches with `MemoryRecords.builder(...)`
(Phase 3d-3/3d-4 in our plan). Our translation builds equivalent batches
manually via `default_record::write_to` + `write_header`. The byte
layout produced is identical (`MemoryRecordsBuilder` will use the same
helpers under the hood), so when 3d-3/3d-4 land the assertions stay valid
without modification.

### Tests deferred to Phase 3d-3/3d-4

- `testReadAndWriteControlBatch` — needs `EndTransactionMarker` (txn
  scope, Phase 3d+/Phase 6). Skipped, document at file level if needed.
- `testZstdJniForSkipKeyValueIterator` — Java-specific (Mockito spying
  on JNI calls). Not translatable; skip with rationale.
- `testBufferReuseInSkipKeyValueIterator` — needs Mockito-style spies on
  `BufferSupplier`. Not translatable directly; superseded by our
  `skip_key_value_iterator_yields_correct_count` correctness test that
  exercises every codec.

### `is_valid` uses `size_in_bytes()`, not `buffer.len()`

Java's `isValid` is `sizeInBytes() >= RECORD_BATCH_OVERHEAD && checksum
== computeChecksum`. The first check uses the **on-wire `Length` field**
(via `LOG_OVERHEAD + Length`), not the actual buffer length. A batch
whose header lies about its length is corrupt by construction. Fixed
during 3d-2 testing — initial implementation used `buffer.len()` and
the `testInvalidRecordSize` assertion failed.

## Tests that intentionally diverge from Java

- `testInvalidRecordCountTooManyNonCompressedV2` /
  `*TooLittleNonCompressedV2`: Java throws `InvalidRecordException` from
  `forEach Record::ensureValid`. The Rust iterator surfaces the error by
  terminating early (poison flag); we assert `collected.len() < declared`
  for the too-many case and `collected.len() == declared` for the
  too-little case. Documented in the iterator's `next()` body.

- `byte_level_fixture_empty_batch` is Rust-only. Builds an empty v2 batch
  via `write_empty_header` and asserts every byte of the 61-byte header
  matches a hand-computed expected layout. Required by PLAN.md DoD
  ("byte-level encoding test").

- `decompression_round_trip_for_each_codec` is Rust-only. Builds a
  3-record batch, compresses with each Phase 3c codec, decompresses
  through `DefaultRecordBatch::iter`, asserts payload equality. Required
  by Phase 3d-2 DoD ("decompression round-trip tested for each codec").

## Test count delta

Baseline before 3d-2: 475 lib tests.
After 3d-2: **508 lib tests (+33: 23 in default_record_batch, 4 in
byte_buffer_log_input_stream, 3 in record_batch_iterator, 5 in
record_validation_stats, +1 BufferSupplierTest translation in
buffer_supplier)** -2 (the duplicate `increment_sequence` test was
removed from `default_record::tests` since the function moved here).

Generator tests unchanged at 74.

## Files added

- `src/common/record/default_record_batch.rs`
- `src/common/record/byte_buffer_log_input_stream.rs`
- `src/common/record/log_input_stream.rs`
- `src/common/record/record_batch_iterator.rs`
- `src/common/record/record_validation_stats.rs`

## Files modified

- `src/common/record/mod.rs` — added 5 new module declarations + 2
  re-exports (`DefaultRecordBatch`, `RecordValidationStats`).
- `src/common/record/default_record.rs` — moved `increment_sequence`
  out, dropped `#[allow(dead_code)]` deferral comment on
  `record_size_upper_bound` (replaced with new doc text), removed
  duplicate `increment_sequence_wraps_at_i32_max` test.
- `src/common/record/partial_default_record.rs` — updated
  `increment_sequence` import path to point at `default_record_batch`.
- `src/common/utils/buffer_supplier.rs` — translated
  `BufferSupplierTest.testGrowableBuffer` into the existing test module.

## Phase 3d-3 follow-ups

1. `MemoryRecords` will own a buffer and expose `batches()` returning a
   `RecordBatchIterator<ByteBufferLogInputStream, DefaultRecordBatch>`.
   The pieces are ready; no further plumbing needed.
2. The 3 test modules duplicate `build_batch` helpers. Once
   `MemoryRecordsBuilder` lands, replace with calls into the builder and
   move the duplicated helper to a single shared `#[cfg(test)]` module.
3. `RecordValidationStats` is currently used only by Phase 3d-2 tests;
   broker-side code is out of scope. The class is still needed because
   broker responses (Phase 4-5) reference its semantics.
