# Milestone 4: Producer Trait, MockProducer, and C FFI

## Goal

Translate Java's `Producer<K,V>` interface and `MockProducer` to Rust, translate all applicable
tests, and expose the Producer via C headers (cbindgen) for calling from Python and other languages.

## Scope

**In scope:**
- `Producer` trait (simplified — core send/flush/close/partitions_for)
- `MockProducer` implementation (auto-complete, manual completion, error injection)
- Supporting types: `ProducerRecord`, `RecordMetadata`, `FutureRecordMetadata`, `Header`
- 8 tests from `MockProducerTest.java` (non-transactional, non-callback subset)
- C FFI layer with cbindgen behind `ffi` feature flag
- Batch send API (`kafka_producer_send_batch`) for amortized FFI crossing

**Out of scope (deferred):**
- Serializers (`Serializer<T>` trait, `StringSerializer`, etc.)
- Partitioners (`Partitioner` trait, `RoundRobinPartitioner`)
- `send_with_callback` (Callback interface)
- Transactional features (`initTransactions`, `beginTransaction`, `commitTransaction`,
  `abortTransaction`, `sendOffsetsToTransaction`)
- Fencing (`fenceProducer`, `ProducerFencedException`)
- Metrics (`metrics()`, `clientInstanceId()`, `registerMetricForSubscription`)
- `ConsumerGroupMetadata`, `OffsetAndMetadata`

## Java Source Reference

- `kafka/clients/src/main/java/org/apache/kafka/clients/producer/Producer.java` (117 lines)
- `kafka/clients/src/main/java/org/apache/kafka/clients/producer/MockProducer.java` (597 lines)
- `kafka/clients/src/test/java/org/apache/kafka/clients/producer/MockProducerTest.java` (751 lines)
- `kafka/clients/src/main/java/org/apache/kafka/clients/producer/ProducerRecord.java`
- `kafka/clients/src/main/java/org/apache/kafka/clients/producer/RecordMetadata.java`
- `kafka/clients/src/main/java/org/apache/kafka/clients/producer/internals/FutureRecordMetadata.java`
- `kafka/clients/src/main/java/org/apache/kafka/clients/producer/internals/ProduceRequestResult.java`

## Phases

| Phase | Description | Files | Dependencies |
|-------|-------------|-------|-------------|
| 1 | Foundation types (ProducerRecord, RecordMetadata, FutureRecordMetadata, Header) | `src/clients/producer/` | Existing TopicPartition, Cluster |
| 2 | Producer trait | `src/clients/producer/producer.rs` | Phase 1 |
| 3 | MockProducer | `src/clients/producer/mock_producer.rs` | Phases 1-2 |
| 4 | Tests (8 from MockProducerTest.java) | `tests/clients/producer/mock_producer_test.rs` | Phases 1-3 |
| 5 | C FFI layer + cbindgen | `src/ffi/`, `cbindgen.toml`, `Cargo.toml` | Phases 1-3 |

## Module Structure

```
src/clients/producer/
├── mod.rs                    # pub mod + re-exports
├── record.rs                 # ProducerRecord, Header
├── record_metadata.rs        # RecordMetadata
├── future_record_metadata.rs # FutureRecordMetadata
├── producer.rs               # Producer trait
└── mock_producer.rs          # MockProducer

src/ffi/                      # Behind feature = "ffi"
├── mod.rs
└── producer.rs               # extern "C" functions

tests/clients/producer/
├── main.rs
└── mock_producer_test.rs     # 8 tests from MockProducerTest.java
```

## Definition of Done

Per phase: `cargo build`, `cargo test`, `cargo xtask format-check`, `cargo xtask lint`.
Phase 5 additionally: `cargo build --features ffi`, verify header generation.
