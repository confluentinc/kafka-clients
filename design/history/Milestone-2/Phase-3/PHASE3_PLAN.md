# Phase 3: Wire Up Producer to NetworkClient

## Goal
Replace the custom batch serialization format with proper RecordBatch wire format, and implement a real `ProduceClient` that bridges the producer's `Sender` to the `NetworkClient` for sending produce requests to a broker.

## Changes Required

### 1. Rewrite ProducerBatch to use MemoryRecordsBuilder
- `src/clients/producer/batch.rs`: Replace custom `[key_len:4][key][value_len:4][value]...` format with `MemoryRecordsBuilder` from Phase 1
- `try_append()` should write records via `MemoryRecordsBuilder::append()`
- `buffer()` should return proper RecordBatch bytes (call `MemoryRecordsBuilder::build()`)
- Update size estimation to use `DefaultRecord::size_of()` 

### 2. Implement NetworkProduceClient
- New struct implementing `ProduceClient` trait using `NetworkClient`
- `send_produce_request()`: Build `ProduceRequestData` from batch bytes, create `ClientRequest`, send via `NetworkClient::send()` + `poll()`, parse `ProduceResponseData`
- `partitions_for()`: Query metadata from `NetworkClient`

### 3. Update Sender
- May need adjustments for how batches are grouped by leader node
- For MVP: still use node_id=0 (single broker), but structure for future multi-broker

### 4. Update existing producer tests
- All existing tests in `batch.rs`, `accumulator.rs`, `kafka_producer.rs` must still pass
- Tests should now produce valid RecordBatch bytes

## Java References
- `kafka/clients/src/main/java/org/apache/kafka/clients/producer/internals/ProducerBatch.java`
- `kafka/clients/src/main/java/org/apache/kafka/clients/producer/internals/Sender.java`

## Dependencies
- Phase 1: `MemoryRecordsBuilder`, `DefaultRecord`, `DefaultRecordBatch` (complete)
- Phase 2: `ProduceRequest`, `ProduceResponse` (complete)
- Milestone 1: `NetworkClient`, `Selector`, `KafkaClient` trait (complete)
