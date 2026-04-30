---
name: Phase 3a record-format base layer translation summary
description: What landed in Phase 3a, what was deferred, and where Phase 3c/3d picks up
type: project
---

## Phase 3a scope (delivered)

`src/common/record/` now holds the trait base layer plus the four stable
identifier enums:

- `record_version.rs` — `RecordVersion` enum (V0/V1/V2) with
  `lookup`, `current`, `value`.
- `timestamp_type.rs` — `TimestampType` enum with `id`, `name`,
  `for_name`.
- `compression_type.rs` — `CompressionType` enum with `id`, `name`,
  `for_id`, `for_name`, `default_level`, `min_level`, `max_level`.
  Codec-dispatch is deferred to Phase 3c (see
  `phase3a_compression_dispatch_gap.md`).
- `control_record_type.rs` — full `ControlRecordType` translation.
  `recordKey()` (Java) is a `Struct`-based serialization helper that the
  producer client doesn't use; deferred to whenever `Struct` lands.
- `record.rs` — `Record` trait.
- `record_batch.rs` — `RecordBatch` trait + magic/sentinel constants
  (`MAGIC_VALUE_V0..V2`, `NO_TIMESTAMP`, `NO_PRODUCER_ID`,
  `NO_PRODUCER_EPOCH`, `NO_SEQUENCE`, `NO_PARTITION_LEADER_EPOCH`).
- `mutable_record_batch.rs` — `MutableRecordBatch: RecordBatch`.
- `abstract_record_batch.rs` — documentation file; the three default
  methods Java factors out (`hasProducerId`, `nextOffset`, `isCompressed`)
  live as default trait methods on `RecordBatch`.
- `base_records.rs` — `BaseRecords` trait, `size_in_bytes()` only.
- `transferable_records.rs` — empty trait extending `BaseRecords`. The
  `writeTo(TransferableChannel)` method waits for Phase 5's
  `common/network`.
- `records.rs` — `Records: TransferableRecords` trait + offset/length
  constants (`OFFSET_OFFSET`, `OFFSET_LENGTH`, `SIZE_OFFSET`,
  `SIZE_LENGTH`, `LOG_OVERHEAD`, `MAGIC_OFFSET`, `MAGIC_LENGTH`,
  `HEADER_SIZE_UP_TO_MAGIC`).
- `abstract_records.rs` — documentation file; `last_batch`,
  `has_matching_magic` defaults live on the `Records` trait directly.
  Adds `first_batch` as a free helper.
- `simple_record.rs` — `SimpleRecord` value type with
  `Arc<[u8]>` storage for zero-copy clone.

## Tests

- `ControlRecordTypeTest` — fully translated (3 Java cases including
  the parameterized round-trip across all variants, plus 3 additional
  Rust-only cases for too-short / negative-version / unknown-type-id).
- No standalone Java tests for `RecordVersion`, `TimestampType`,
  `CompressionType`, or `SimpleRecord`. Inline tests cover them.

Total new lib-test count Phase 3a: 33 (24 enum + 9 SimpleRecord).
Baseline 316 → Phase 3a 349.

## Deferred to Phase 3c

- `CompressionType` codec-dispatch (`Compression`, `NoCompression`,
  `GzipCompression`, etc.) — see `phase3a_compression_dispatch_gap.md`.
- `AbstractRecords` static estimators
  (`estimateSizeInBytes`, `estimateSizeInBytesUpperBound`,
  `recordBatchHeaderSizeInBytes`) — depend on `LegacyRecord` (out of
  scope) and `DefaultRecordBatch` (Phase 3c).
- `DefaultRecord`, `DefaultRecordBatch`, `MemoryRecords`,
  `MemoryRecordsBuilder` — concrete impls of the base traits.
- `MemoryRecordsBuilder::append` zero-copy hot path (CLAUDE.md
  rule 12). Phase 3a does *not* attempt to enforce that yet — the
  trait surface deliberately doesn't expose any "intermediate buffer"
  type for the implementor to copy through. Phase 3c implementors
  should serialize directly into the batch buffer (see PLAN.md DoD
  bullet for Phase 3).

## Deferred to Phase 3d

- `Records::records()` default impl — needs a self-referential
  per-batch iterator that's hard to express without GAT through trait
  objects. Concrete Phase 3d types provide their own impl.
- `Records::slice` — needs `LogInputStream` / `ByteBufferLogInputStream`
  (Phase 3d).
- `BaseRecords::to_send` — needs `RecordsSend` (Phase 3d).
- `MutableRecordBatch::skip_key_value_iterator` concrete impls.
- `RecordsSend`, `DefaultRecordsSend`, `RecordValidationStats`,
  `RecordBatchIterator`, `LogInputStream`, `ByteBufferLogInputStream`,
  `CompressionRatioEstimator`, `UnalignedRecords`,
  `UnalignedMemoryRecords`.

## Deferred to Phase 5

- `TransferableRecords::write_to(TransferableChannel, ...)` — channel
  type lives in `common/network/*` (Phase 5).

## Permanently out of scope

- `AbstractLegacyRecordBatch`, `LegacyRecord`, `EndTransactionMarker`,
  `ControlRecordUtils`, `FileRecords`, `FileLogInputStream`,
  `RemoteLogInputStream`, `UnalignedFileRecords`,
  `PartialDefaultRecord` — broker-side / consumer-side / legacy v0/v1.
