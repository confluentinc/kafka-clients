# Phase 1: RecordBatch Wire Format Serialization

## Goal
Translate Java's record batch serialization so ProducerBatch can produce broker-compatible bytes (Magic v2).

## Java Classes Translated
| Java Class | Rust Module |
|---|---|
| `Record.java` | `common/record/mod.rs` (trait) |
| `RecordBatch.java` | `common/record/mod.rs` (trait + constants) |
| `CompressionType.java` | `common/record/compression_type.rs` |
| `TimestampType.java` | `common/record/timestamp_type.rs` |
| `DefaultRecord.java` | `common/record/default_record.rs` |
| `DefaultRecordBatch.java` | `common/record/default_record_batch.rs` |
| `MemoryRecords.java` | `common/record/memory_records.rs` |
| `MemoryRecordsBuilder.java` | `common/record/memory_records_builder.rs` |

## Java Tests Translated
| Java Test | Tests Translated | Skipped (reason) |
|---|---|---|
| `DefaultRecordTest.java` | 16 | 6 — InputStream API (no Rust equivalent) |
| `DefaultRecordBatchTest.java` | 16 | compression/transaction tests (out of scope) |
| `MemoryRecordsTest.java` | 9 | filter/slice/transaction tests (out of scope) |
| `MemoryRecordsBuilderTest.java` | 13 | control-record/legacy tests (out of scope) |

## Scope Limits
- Only Magic v2 (no legacy v0/v1)
- CompressionType::NONE only
- No transactions
- No FileRecords
