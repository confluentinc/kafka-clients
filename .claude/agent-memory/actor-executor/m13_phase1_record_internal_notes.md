---
name: m13-phase1-record-internal
description: Milestone-13 Phase 1 — common::record -> common::record::internal move (KAFKA-20128) layout + ControlRecordType generated-schema
metadata:
  type: project
---

Milestone-13 Phase 1 (AK 4.2.0->4.3.1 common) landed the KAFKA-20128 record
package move and KAFKA-10863 ControlRecordType schema rewrite.

**Why:** AK 4.3.1 moved everything under `org.apache.kafka.common.record`
except `TimestampType` (+ package-info) to `...record.internal`.

**How to apply (future phases importing record types):**
- Moved types now live at `crate::common::record::internal::*` (module is
  `pub(crate) mod internal;`). Import moved types from there:
  `RecordBatch, Record, CompressionType, DefaultRecord{,Ref},
  DefaultRecordBatch{,Ref}, MemoryRecords, MemoryRecordsBuilder, SimpleRecord,
  CompressionRatioEstimator, ControlRecordType, BatchIterator, RecordVersion`,
  plus file-path items `internal::abstract_records::{LOG_OVERHEAD,
  record_batch_header_size_in_bytes}` and
  `internal::default_record_batch::increment_sequence`.
- STAYS at `crate::common::record`: `TimestampType` and `InvalidRecordError`
  (the latter's Java class is `common.InvalidRecordException`, never a
  record-package type).
- DoD #7 deviation: `MemoryRecords` and `SimpleRecord` ALSO keep a public
  re-export at `common::record` — needed by the crate-external
  `tests/common/message/records_serde_test.rs` (its generated
  `SimpleRecordsMessageData` lives only in the test crate's OUT_DIR, so the
  test can't move in-crate). Java-faithful (those classes are `public` even in
  the internal package).
- Inner item visibilities left as declared (`pub`) — the `pub(crate) mod`
  gate enforces crate-only, matching existing internal modules
  (`producer/internals` has 160 bare `pub`). Did NOT flip every item.
- Translated-for-completeness methods with no crate caller after the move
  carry `#[allow(dead_code)]` (RecordVersion V0/V1/lookup/current,
  CompressionRatioEstimator::reset_estimation, ControlRecordType
  type_id/key_buffer/record_key/control_record_key_size).

**ControlRecordType (KAFKA-10863):** now derives the key from generated
`crate::control_record_type_schema_data::ControlRecordTypeSchemaData` (spec
`ControlRecordTypeSchema.json`, one `Type` int16 field; version is the
`to_version_prefixed_byte_buffer` prefix -> 4-byte key). `record_key()` +
`control_record_key_size()` newly translated (were write-path-only, needed by
the +118 `ControlRecordTypeTest`). parse_type_id error text dropped "end":
"Invalid value size found for control record key. ...".

**Skip-with-reason (no Rust counterpart / untranslated):** `Type.java`
(+180/-132, all runtime `DocumentedType` Schema hierarchy;
`SchemaType`/`Field`/`Schema` metadata subset unaffected — generator maps
records fields to `SchemaType::Records`/`CompactRecords` with no nullable-
records tag); `TypeTest`/`ProtocolSerializationTest` (same runtime Schema);
`FeaturesTest` + `BaseVersionRange` comment (Rust has no `Features` type;
`SupportedVersionRange::to_string` uses fixed field order not map iteration);
`GroupState` (annotation-only); `RecordValidationStats` (moved to storage, no
Rust counterpart). Requests wrappers (`Fetch*`, `Produce*`, `ListOffsets*`,
`OffsetsForLeaderEpoch*`, `RequestUtils`, `InitProducerId`,
`TxnOffsetCommit`), `Readable/Writable/SendBuilder`, `RecordHeaders`,
`ProducerIdAndEpoch`: pure `record.*`->`record.internal.*` import churn,
handled by the move. `PartitionInfo` got a one-line replicas javadoc;
`TopicPartitionInfo` already matched the new "None" wording.
