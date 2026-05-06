---
name: Phase 3d-4 MemoryRecordsBuilder landing notes
description: MemoryRecordsBuilder + MemoryRecords::with_records factories; what landed, what's deferred for Phase 6+
type: project
---

Phase 3d-4 status: APPROVED-pending-review.

**Why:** Final sub-chunk of Phase 3d, the centerpiece of the producer
write path's zero-copy contract.

**How to apply:** When Phase 4+ work needs `MemoryRecords::with_*`
factories, the entry points are:

* `MemoryRecords::with_records(magic, initial_offset, compression,
  ts_type, pid, epoch, base_seq, partition_leader_epoch,
  is_transactional, &[SimpleRecord])` — full-args
* `MemoryRecords::with_records_default(compression, &[SimpleRecord])` —
  CURRENT_MAGIC_VALUE + CreateTime + no producer state
* `MemoryRecords::with_idempotent_records(...)` and
  `with_transactional_records(...)`

For tests that need control over the batch buffer's allocation,
construct a `MemoryRecordsBuilder` directly via
`MemoryRecordsBuilder::from_stream_no_delete_horizon(...)`.

## Architecture decisions worth recording

### Compression: buffer-and-finalize, not streaming

Java keeps `appendStream` as a long-lived `DataOutputStream(codec)` so
records are streaming-compressed as they're appended. In Rust the
self-borrow against the bufferStream makes that pattern hostile. The
Rust translation uses an explicit `uncompressed_buf: Option<Vec<u8>>`
that records are streamed into (uncompressed); on `close()` the codec
runs over the whole buffer once and writes into `bufferStream`. This
produces identical wire bytes — only the timing of the codec work
differs. `actualCompressionRatio` is computed correctly from the
post-close `pos - initial_position` size.

For uncompressed batches, `uncompressed_buf` stays `None` and records
write **directly** into `bufferStream` via
`default_record::write_to_stream(&mut self.buffer_stream, ...)`,
satisfying the zero-copy DoD on the no-compression path.

### `default_record::write_to_stream(&mut dyn Write, ...)`

Sibling of the existing `write_to(&mut Vec<u8>, ...)`. Used by the
builder's no-compression direct-write path (writes through
`ByteBufferOutputStream`'s `Write` impl) AND would be used by a
streaming-compressed path if we ever switch. Both signatures preserved
because `&mut Vec<u8>` lets `default_record_batch`'s test/builder helpers
pre-size and `extend_from_slice` (cheaper than `Write::write_all`'s
length tracking).

### `close()` consumes the bufferStream's Vec to satisfy strict zero-copy

Originally close() did `buffer_stream.buffer()[initial..].to_vec()`
which copies. Refactored to `mem::replace(&mut buffer_stream,
empty_stub).into_buffer()` which MOVES the Vec, then
`Vec::drain(0..initial_position)` for the prefix shift (drain
preserves the backing allocation). The final
`MemoryRecords::buffer().as_ptr()` equals the captured pre-build
allocation pointer — proven by the
`append_writes_directly_into_batch_buffer_uncompressed` test.

### `KafkaError::IllegalState` added

For Java `IllegalStateException`-equivalent producer state errors
(build/append after close/abort). Mirrors the
`IllegalArgumentException -> KafkaError::IllegalArgument` pattern.

### `record_batch_header_size_in_bytes` re-implemented locally

`AbstractRecords.recordBatchHeaderSizeInBytes(magic, compression)`
needs to dispatch on magic; for v2 returns `RECORD_BATCH_OVERHEAD`,
for v0/v1 with compression returns `LOG_OVERHEAD + LegacyRecord
overhead`. Phase 3 doesn't ship LegacyRecord, so the v0/v1 branch
returns just `LOG_OVERHEAD` as a conservative estimate. v0/v1 writes
are rejected at the builder constructor anyway — this branch is
hasRoomFor's estimate code only.

## Tests translated

### From `MemoryRecordsBuilderTest.java` (17/24)

* testUnsupportedCompress (top-level @Test)
* testWriteEmptyRecordSet (v2 portion of @ParameterizedTest)
* testWriteTransactionalRecordSet (v2 portion)
* testWriteTransactionalWithInvalidPID
* testWriteIdempotentWithInvalidEpoch
* testWriteIdempotentWithInvalidBaseSequence
* testEstimatedSizeInBytes
* buildUsingLogAppendTime
* buildUsingCreateTime
* testAppendedChecksumConsistency
* testSmallWriteLimit
* writePastLimit
* testAppendAtInvalidOffset
* shouldThrowIllegalStateExceptionOnBuildWhenAborted
* shouldResetBufferToInitialPositionOnAbort
* shouldThrowIllegalStateExceptionOnCloseWhenAborted
* shouldThrowIllegalStateExceptionOnAppendWhenAborted
* shouldThrowIllegalStateExceptionOnAppendWhenClosed

### From `MemoryRecordsTest.java` (deferred-to-3d-4 set, 7/7)

All 6 listed in `phase3d3_memory_records.md` deferral list, plus the
builder-construction half of `testNextBatchSize`:

* testIterator (v2 portion)
* testHasRoomForMethod
* testHasRoomForMethodWithHeaders (v2 portion)
* testChecksum (v2 uncompressed byte-level lock — used Java's expected
  3851219455 value)
* testWithRecords (v2 via with_records factory)
* testUnsupportedCompress (factory-level)
* testNextBatchSize (builder-construction half)

### Zero-copy DoD verification (Rust-only, PLAN.md)

* `append_writes_directly_into_batch_buffer_uncompressed` — pointer
  equality on the no-realloc, no-copy path
* `append_writes_directly_into_batch_buffer_with_initial_offset` —
  pointer equality after `Vec::drain` shifts the prefix

## Tests deferred to Phase 6+

### Need control records / KRaft (`MemoryRecordsBuilderTest`)

* testWriteEndTxnMarkerNonTransactionalBatch (needs EndTransactionMarker)
* testWriteEndTxnMarkerNonControlBatch
* testWriteLeaderChangeControlBatchWithoutLeaderEpoch (needs LeaderChangeMessage)
* testWriteLeaderChangeControlBatch

### Need legacy v0/v1 (`MemoryRecordsBuilderTest`)

* testLegacyCompressionRate

### Need delete-horizon-aware iter (`MemoryRecordsBuilderTest`)

* testRecordTimestampsWithDeleteHorizon

### Need filterTo (`MemoryRecordsTest`)

The complete filterTo family — see `memory_records.rs` test module
docstring for the full list.

## Methods deferred to Phase 6+ on the builder

* appendControlRecord, appendControlRecordWithOffset
* appendEndTxnMarker (needs EndTransactionMarker)
* appendLeaderChangeMessage, appendSnapshot{Header,Footer}Message,
  appendKRaftVersionMessage, appendVotersMessage (KRaft)
* appendUncheckedWithOffset(LegacyRecord), appendWithOffset(LegacyRecord),
  append(LegacyRecord) — legacy
* append(Record), appendWithOffset(Record) — used by filterTo

## Test count delta

Baseline before 3d-4: 547 lib tests.
After 3d-4: **574 lib tests (+27: 27 in memory_records_builder)**.

Generator tests unchanged at 74.

## Commits

* `Phase 3d-4: MemoryRecordsBuilder + MemoryRecords::with_records factories` (initial 25 tests)
* `fixup! Phase 3d-4 ... ` (zero-copy refactor: build moves Vec, +1 test)
* `fixup! Phase 3d-4 ... ` (memory_records.rs docstring tidy)
* `fixup! Phase 3d-4 ... ` (testNextBatchSize builder-construction half, +1 test)

## Files added

* `src/common/record/memory_records_builder.rs` (~1900 lines incl. tests)

## Files modified

* `src/common/errors.rs` — `KafkaError::IllegalState` variant
* `src/common/record/default_record.rs` — `write_to_stream` sibling
* `src/common/record/default_record_batch.rs` (no semantic change; allow
  marker comments tightened)
* `src/common/record/memory_records.rs` — added 3 factory functions
  (`with_records`, `with_records_default`, `with_idempotent_records`,
  `with_transactional_records`) + `estimate_size_in_bytes` helper
* `src/common/record/mod.rs` — module declaration + re-export

## Phase 5 / 6 follow-ups

1. Wire up control records / EndTransactionMarker → completes
   `appendControlRecord` and the deferred Builder tests.
2. Wire up KRaft control messages → completes the
   `appendLeaderChangeMessage` family.
3. Translate `MemoryRecords::filterTo` and the RecordFilter / FilterResult
   nested classes → completes the deferred MemoryRecordsTest filterTo
   tests.
4. (Optional) If profiling shows the buffer-and-finalize compressed
   path is too memory-heavy for typical batch sizes, switch to a
   streaming codec via `Rc<RefCell<ByteBufferOutputStream>>` shared
   between the builder and the codec writer.

## Phase 7+ producer accumulator follow-ups

* `record_size_upper_bound` and `estimate_batch_size_upper_bound`
  remain `#[allow(dead_code)]`. They are NOT called from `hasRoomFor`
  (which uses `default_record::size_in_bytes` per Java). They are used
  by `AbstractRecords.estimateSizeInBytesUpperBound` which is the
  producer accumulator's sizing API → wire up in Phase 7+.
