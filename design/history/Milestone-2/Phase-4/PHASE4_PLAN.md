# Phase 4: End-to-End Integration Tests

## Goal
Implement `KafkaProduceClient` (adapter bridging `ProduceClient` trait to `NetworkClient`) and create integration tests that produce records to a real Kafka 4.2 broker.

## Components

### 1. KafkaProduceClient adapter
New struct implementing `ProduceClient` using `NetworkClient`:
- `send_produce_request()`: Build `ProduceRequestData` from batch bytes, wrap in `ClientRequest`, call `NetworkClient::send()` + `poll()`, parse `ProduceResponseData` into `Vec<PartitionResponse>`
- `partitions_for()`: Query metadata via `NetworkClient`
- Handles `ready()` check before sending, connection lifecycle

### 2. Integration tests (`tests/integration_producer_test.rs`)
Feature-gated behind `integration-tests`, using existing testcontainers infra:
1. `test_produce_single_record` — produce 1 record, verify offset
2. `test_produce_multiple_records` — produce N records, verify sequential offsets
3. `test_produce_with_key_and_headers` — verify key/headers survive round-trip
4. `test_produce_to_nonexistent_topic` — verify error handling

## Design Decision
Uses adapter pattern: `Sender → ProduceClient trait ← KafkaProduceClient → NetworkClient`
- Keeps existing MockProduceClient unit tests working
- Same end behavior as Java's Sender using KafkaClient directly
- Hand-written adapter, no mockall needed

## Dependencies
- Phase 1: RecordBatch wire format (complete)
- Phase 2: ProduceRequest/ProduceResponse (complete)
- Phase 3: ProducerBatch with MemoryRecordsBuilder (complete)
- Milestone 1: NetworkClient, Selector, testcontainers infra (complete)
