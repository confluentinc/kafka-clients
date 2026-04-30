# Phase 3a Review — Critic N=0

Scope: 4 commits since `6a5ddfe` (`0721e86`, `147a328`, `f2e9ded`, `fd21a99`).

DoD checks I re-ran (all green):
- `cargo build --lib` — finished in 13.81s, no warnings
- `cargo test --lib` — `test result: ok. 349 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s`
- `cargo xtask format-check` — all formatted
- `cargo xtask lint` — no clippy issues

Java vs. Rust comparisons against `kafka/clients/src/main/java/org/apache/kafka/common/record/*.java`.

Numbering follows Phase 2 convention. Severity: **BLOCKER / MAJOR / MINOR / DEFERRED-OK**.

Issues 1, 2, 3, 4 have been resolved and moved to `COMMENTS.DONE.0.md`.
This file retains only the **DEFERRED-OK** items (5–8): they require no
code change and are tracked here so future phases pick them up at the
correct phase boundary.

---

## 5. `BaseRecords::to_send` deferred — DEFERRED-OK

- **Severity:** DEFERRED-OK
- **File:** `src/common/record/base_records.rs`
- **Java reference:** `BaseRecords.java:33` — `RecordsSend<? extends BaseRecords> toSend();`

Java's `BaseRecords` interface has two methods. The Rust trait keeps only
`size_in_bytes()`; `to_send()` is deferred until Phase 3d adds
`RecordsSend`. The actor's docstring on the trait calls this out
explicitly. Acceptable per the actor-direction note that
`RecordsSend`/`DefaultRecordsSend` are 3d concerns.

---

## 6. `TransferableRecords::write_to` deferred — DEFERRED-OK

- **Severity:** DEFERRED-OK
- **File:** `src/common/record/transferable_records.rs`
- **Java reference:** `TransferableRecords.java:38`

`TransferableChannel` is a Phase 5 (network) concern. Deferring with a
docstring note is correct per CLAUDE.md DoD #7 (no inventing new types
ahead of their phase).

---

## 7. `Records::records()` is required (no default) — DEFERRED-OK

- **Severity:** DEFERRED-OK
- **File:** `src/common/record/records.rs:85`
- **Java reference:** `AbstractRecords.java:64-66, 73-91`

Java's `AbstractRecords` provides a default chained iterator over batches.
Rust's `Records::records()` is a required method without a default. The
actor's docstring explains why (self-referential per-batch iterator
requires the concrete batch type to express). Concrete Phase 3d types
(`MemoryRecords`, `FileRecords`) will provide their own. Acceptable for
a trait surface phase.

---

## 8. `CompressionType::levelValidator` deferred — DEFERRED-OK

- **Severity:** DEFERRED-OK
- **File:** `src/common/record/compression_type.rs`
- **Java reference:** `CompressionType.java:54-63, 95-97, 127-129, 188-190`

`level_validator()` returns a `ConfigDef.Validator` lambda in Java,
used by `CommonClientConfigs`/`ProducerConfig` to enforce per-codec
level ranges at config-parse time. The actor deferred this to Phase 3c
(documented in `phase3a_compression_dispatch_gap.md`) when the
`Compression` codec dispatch lands. The level constants
(`*_MIN_LEVEL`, `*_MAX_LEVEL`, `*_DEFAULT_LEVEL`) are already in place
so the Phase 3c addition is straightforward. Acceptable.

---

## Verified clean (no issue)

The following review items were checked and found correct:

- **Enum codes match Java exactly:**
  `CompressionType` ids (NONE=0, GZIP=1, SNAPPY=2, LZ4=3, ZSTD=4),
  `TimestampType` ids (NO_TIMESTAMP_TYPE=-1, CREATE_TIME=0,
  LOG_APPEND_TIME=1), `RecordVersion` magic bytes (V0=0, V1=1, V2=2),
  `ControlRecordType` type ids (ABORT=0, COMMIT=1, LEADER_CHANGE=2,
  SNAPSHOT_HEADER=3, SNAPSHOT_FOOTER=4, KRAFT_VERSION=5, KRAFT_VOTERS=6,
  UNKNOWN=-1).
- **Per-codec compression levels match Java:** GZIP (1, 9, -1), LZ4 (1,
  17, 9), ZSTD (-131072, 22, 3).
- **`MutableRecordBatch: RecordBatch`** supertrait is correctly
  declared (`mutable_record_batch.rs:26`).
- **`Records: TransferableRecords`** supertrait is correctly declared
  (`records.rs:55`).
- **`TransferableRecords: BaseRecords`** supertrait is correctly
  declared (`transferable_records.rs:35`).
- **No legacy translation:** `AbstractLegacyRecordBatch.java`,
  `LegacyRecord.java`, `EndTransactionMarker.java`,
  `ControlRecordUtils.java`, `MemoryRecords.java`,
  `MemoryRecordsBuilder.java`, `FileRecords.java`, `FileLogInputStream.java`,
  `RemoteLogInputStream.java`, `DefaultRecord.java`, `DefaultRecordBatch.java`,
  `DefaultRecordsSend.java`, `MultiRecordsSend.java`,
  `PartialDefaultRecord.java`, `RecordBatchIterator.java`,
  `UnalignedFileRecords.java`, `UnalignedMemoryRecords.java`,
  `UnalignedRecords.java`, `RecordValidationStats.java`, and
  `CompressionRatioEstimator.java` are correctly absent from the diff.
- **Apache 2.0 / Confluent Inc. headers** present on all 13 new files.
- **Magic-byte / sentinel constants** in `record_batch.rs` match Java
  (`MAGIC_VALUE_V0..V2`, `CURRENT_MAGIC_VALUE`, `NO_TIMESTAMP=-1`,
  `NO_PRODUCER_ID=-1`, `NO_PRODUCER_EPOCH=-1`, `NO_SEQUENCE=-1`,
  `NO_PARTITION_LEADER_EPOCH=-1`).
- **`Records` length constants** match Java (`OFFSET_OFFSET=0`,
  `OFFSET_LENGTH=8`, `SIZE_OFFSET=8`, `SIZE_LENGTH=4`,
  `LOG_OVERHEAD=12`, `MAGIC_OFFSET=16`, `MAGIC_LENGTH=1`,
  `HEADER_SIZE_UP_TO_MAGIC=17`).
- **`ControlRecordTypeTest` translation** covers all three Java
  scenarios (`testParseUnknownType`, `testParseUnknownVersion`,
  `testRoundTrip` over every variant including UNKNOWN — Java's
  `@EnumSource` with no `mode=EXCLUDE` does include UNKNOWN). Two
  bonus error-path tests added.
- **`parseTypeId` byte order:** Java's `ByteBuffer` defaults to
  big-endian; Rust uses `i16::from_be_bytes`. Match.
- **No unrelated standalone Java tests for the four enum modules**:
  `ls kafka/clients/src/test/java/.../record/` confirms only
  `ControlRecordTypeTest.java` exists in the four-enum scope.
- **`mod.rs` re-exports** follow the Phase 2 pattern: each translated
  type and trait is re-exported at the parent module level so
  consumers can `use crate::common::record::Foo;`.
- **`AbstractRecordBatch` and `AbstractRecords`** correctly collapse
  Java's abstract-class default bodies into trait default methods
  (`has_producer_id`, `next_offset`, `is_compressed`,
  `last_batch`, `has_matching_magic`); the documentation files preserve
  the file-per-class mapping required by CLAUDE.md rule 2.

---

Round 1 verdict: **needs minor fixes**. Two MAJOR (#1 contract divergence, #2 zero-copy regression), one MINOR-deferral that should be tracked (#3), one MINOR design call to confirm (#4), and four DEFERRED-OK items (#5–8).

Round 1 outcome: Issues 1–4 resolved by fixup commits `9fa1f99`
(Issue 1), `c9d89f9` + `1fdfb8c` (Issue 2), `1f965ec` (Issue 3),
`041ffca` (Issue 4); see `COMMENTS.DONE.0.md` for the resolutions.
DEFERRED-OK items 5–8 retained here as tracking records for later
phases.
