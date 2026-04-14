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

---

## Phase 5 — Make KafkaClient Trait Async-Compatible

### Goal
Solve the async/sync impedance mismatch in `NetworkClient`.  Currently, `NetworkClient`
calls `block_on()` for every `Selector` operation, which panics if the future is not
immediately ready.  Make `KafkaClient` trait methods (`ready`, `poll`, `disconnect`,
`close_connection`, `close`) async so they can be called from Tokio tasks.

### Key Changes
- `src/clients/kafka_client.rs` — add `async` to 5 methods
- `src/clients/network_client.rs` — replace `block_on()` with `.await`
- `src/clients/network_client_utils.rs` — update for async methods
- Remove `block_on()` and `noop_waker()` helper functions

### Scope
Pure refactor — no producer changes, no new classes.  All existing `NetworkClient`
tests must pass under `#[tokio::test]`.

See `Phase-5/PHASE5_PLAN.md` for full details.

---

## Phase 6 — Rewrite Sender to Use KafkaClient (Eliminate Mutex Bottleneck)

### Goal
Rewrite `Sender` to own a `KafkaClient` implementation directly (matching Java's
`Sender` which owns `KafkaClient`).  Delete the invented `ProduceClient` trait and
`KafkaProduceClient`.  Implement Java's `sendProducerData()` + `client.poll()` loop
with `RequestCompletionHandler` callbacks for response handling.

### Key Changes
- DELETE `src/clients/producer/kafka_produce_client.rs`
- `src/clients/producer/sender.rs` — remove `ProduceClient`, implement `run_once()` with `send_producer_data()` + `client.poll()`, implement `send_produce_request()` with callback, implement `handle_produce_response()`
- `src/clients/producer/kafka_producer.rs` — remove generic `<C: ProduceClient>`, use `NetworkClient` via `KafkaClient` trait
- `src/clients/producer/mod.rs` — remove `KafkaProduceClient`, `ProduceClient` exports
- `performance_tests/src/producer_perf.rs` — update construction
- Update all unit tests to mock `KafkaClient` instead of `ProduceClient`

### Java Classes Being Aligned With
- `Sender.runOnce()`, `Sender.sendProducerData()`, `Sender.sendProduceRequest()`, `Sender.handleProduceResponse()`

See `Phase-6/PHASE6_PLAN.md` for full details.

---

## Phase 7 — ProducerMetadata and Integration Test Update

### Goal
Translate Java's `ProducerMetadata` class.  Update `KafkaProducer` to use it for
topic metadata management instead of the ad-hoc `metadata_cache`.  Update integration
and performance tests to use the refactored producer with `NetworkClient`.

### Key Changes
- ADD `src/clients/producer/producer_metadata.rs` — `ProducerMetadata` wrapping `Metadata`
- `src/clients/producer/kafka_producer.rs` — use `ProducerMetadata`, rewrite `wait_on_metadata()`
- `src/clients/producer/sender.rs` — use `ProducerMetadata` for metadata snapshots
- `performance_tests/src/producer_perf.rs` — full update for new architecture
- Translate `ProducerMetadataTest.java` tests

### Java Classes Being Aligned With
- `ProducerMetadata` (extends `Metadata`)
- `KafkaProducer.waitOnMetadata()` using `ProducerMetadata.awaitUpdate()`

See `Phase-7/PHASE7_PLAN.md` for full details.
