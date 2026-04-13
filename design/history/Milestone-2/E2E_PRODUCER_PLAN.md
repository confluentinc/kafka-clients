# Milestone 2: End-to-End Producer

## Overview
Implement the minimum set of classes needed to produce records from KafkaProducer through to a real Kafka 4.2 broker. Builds on Milestone 1 (network client infrastructure) and the existing producer skeleton (KafkaProducer, Sender, RecordAccumulator, ProducerBatch).

## Agent Assignment
- **Agent Number**: N=1
- **Comment Files**: COMMENTS.1.md / COMMENTS.DONE.1.md

## Phase 1 — RecordBatch Wire Format Serialization

### Goal
Translate Java's record batch serialization so ProducerBatch can produce broker-compatible bytes.

### Java Classes to Translate
| Java Class | Rust Module | Purpose |
|---|---|---|
| `Record.java` | `common/record/mod.rs` (trait) | Record trait |
| `RecordBatch.java` | `common/record/mod.rs` (trait) | RecordBatch trait + constants |
| `CompressionType.java` | `common/record/compression_type.rs` | Compression enum (NONE for MVP) |
| `TimestampType.java` | `common/record/timestamp_type.rs` | CreateTime vs LogAppendTime |
| `DefaultRecord.java` | `common/record/default_record.rs` | Single record varint serialization |
| `DefaultRecordBatch.java` | `common/record/default_record_batch.rs` | Batch framing, CRC32C, magic v2 |
| `MemoryRecords.java` | `common/record/memory_records.rs` | Immutable record batch container |
| `MemoryRecordsBuilder.java` | `common/record/memory_records_builder.rs` | Incremental batch builder |
| `AbstractRecords.java` | minimal needed helpers | Size estimation |

### Java Tests to Translate
| Java Test | Rust Test |
|---|---|
| `DefaultRecordTest.java` | `tests/default_record_test.rs` |
| `DefaultRecordBatchTest.java` | `tests/default_record_batch_test.rs` |
| `MemoryRecordsTest.java` | `tests/memory_records_test.rs` |
| `MemoryRecordsBuilderTest.java` | `tests/memory_records_builder_test.rs` |

### Scope Limits
- Only Magic v2 (no legacy v0/v1)
- CompressionType::NONE only (stub others)
- No transactions (skip EndTransactionMarker, producerId/epoch logic)
- No FileRecords (memory-only)

---

## Phase 2 — ProduceRequest/Response Wrappers

### Goal
Create the request/response types so the network client can send produce requests.

### Java Classes to Translate
| Java Class | Rust Module |
|---|---|
| `ProduceRequest.java` | `common/requests/produce_request.rs` |
| `ProduceResponse.java` | `common/requests/produce_response.rs` |
| Add `Produce` variant to `ConcreteRequest` | Edit `abstract_request.rs` |
| Add `Produce` variant to `ConcreteResponse` | Edit `abstract_response.rs` |

### Java Tests to Translate
| Java Test | Rust Test |
|---|---|
| `ProduceRequestTest.java` | `tests/produce_request_test.rs` |
| `ProduceResponseTest.java` | `tests/produce_response_test.rs` |

---

## Phase 3 — Wire Up Producer to NetworkClient

### Goal
Replace custom batch format with RecordBatch, implement real ProduceClient bridging to NetworkClient.

### Changes
- Rewrite `ProducerBatch::try_append()` to use `MemoryRecordsBuilder`
- Implement `NetworkProduceClient` struct implementing `ProduceClient` using `NetworkClient`
- Update `Sender` to build `ProduceRequestData` from batch bytes
- Update existing producer unit tests for new format

---

## Phase 4 — End-to-End Integration Tests

### Goal
Verify the full pipeline against a real Kafka broker.

### New File: `tests/integration_producer_test.rs`
1. `test_produce_single_record` — produce 1 record, verify offset
2. `test_produce_multiple_records` — produce N records, verify sequential offsets
3. `test_produce_with_key_and_headers` — verify key/headers round-trip
4. `test_produce_to_nonexistent_topic` — verify error handling

Uses existing testcontainers infrastructure.
