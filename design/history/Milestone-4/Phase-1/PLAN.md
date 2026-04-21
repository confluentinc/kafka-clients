# Phase 1: Foundation Types

## Goal

Create the core producer data types needed by the Producer trait and MockProducer.

## Java Source

- `org.apache.kafka.clients.producer.ProducerRecord` → `src/clients/producer/record.rs`
- `org.apache.kafka.clients.producer.RecordMetadata` → `src/clients/producer/record_metadata.rs`
- `org.apache.kafka.clients.producer.internals.FutureRecordMetadata` → `src/clients/producer/future_record_metadata.rs`
- `org.apache.kafka.clients.producer.internals.ProduceRequestResult` — inlined into FutureRecordMetadata
- `org.apache.kafka.common.header.Header` — inlined into record.rs
- `org.apache.kafka.common.header.internals.RecordHeaders` — `Vec<Header>` in Rust

## Types to Create

### Header (`record.rs`)

Java's `Header` interface + `RecordHeader` class, simplified:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    key: String,
    value: Option<Vec<u8>>,
}
```

Methods: `new(key, value)`, `key() -> &str`, `value() -> Option<&[u8]>`.

### ProducerRecord (`record.rs`)

Byte-oriented (no generics — serializers are out of scope). Java's `ProducerRecord<K,V>` where
K and V are pre-serialized bytes.

```rust
#[derive(Debug, Clone)]
pub struct ProducerRecord {
    topic: String,
    partition: Option<i32>,
    timestamp: Option<i64>,
    key: Option<Vec<u8>>,
    value: Option<Vec<u8>>,
    headers: Vec<Header>,
}
```

Builder-style construction matching Java's 6 constructors:
- `ProducerRecord::new(topic)` — minimal, only topic
- `.partition(i32)` — set partition
- `.timestamp(i64)` — set timestamp
- `.key(impl Into<Vec<u8>>)` — set key
- `.value(impl Into<Vec<u8>>)` — set value
- `.header(key, value)` — add a header

Getters (borrowed):
- `topic() -> &str`
- `partition() -> Option<i32>`
- `timestamp() -> Option<i64>`
- `key() -> Option<&[u8]>`
- `value() -> Option<&[u8]>`
- `headers() -> &[Header]`

Validations (in constructor or builder):
- topic must not be empty → panic (Java throws IllegalArgumentException in constructor)
- partition must be >= 0 if set
- timestamp must be >= 0 if set

Implement: `PartialEq`, `Eq`, `Display`.

### RecordMetadata (`record_metadata.rs`)

```rust
pub struct RecordMetadata {
    topic_partition: TopicPartition,
    offset: i64,
    timestamp: i64,
    serialized_key_size: i32,
    serialized_value_size: i32,
}

pub const UNKNOWN_PARTITION: i32 = -1;
```

Constructor: `new(topic_partition, base_offset, batch_index, timestamp, serialized_key_size, serialized_value_size)`
- `offset = base_offset + batch_index` (matching Java)

Getters: `offset()`, `has_offset()`, `timestamp()`, `has_timestamp()`, `topic() -> &str`,
`partition() -> i32`, `serialized_key_size()`, `serialized_value_size()`.

Implement: `Debug`, `Display` (format: `topic-partition@offset`).

### FutureRecordMetadata (`future_record_metadata.rs`)

Translates Java's `FutureRecordMetadata` + `ProduceRequestResult` combined. Uses
`tokio::sync::oneshot` for async completion.

```rust
pub struct FutureRecordMetadata {
    receiver: oneshot::Receiver<Result<RecordMetadata, KafkaError>>,
}
```

Methods:
- `async fn get(&mut self) -> Result<RecordMetadata, KafkaError>` — await completion
- `fn is_done(&self) -> bool` — check if completed (non-blocking)

Also provide `impl Future for FutureRecordMetadata` so it can be `.await`ed directly.

The sender half (`CompletionSender`) is a type alias:
```rust
pub type CompletionSender = oneshot::Sender<Result<RecordMetadata, KafkaError>>;
```

This is used by MockProducer's `Completion` struct to complete or error a send.

### Module file (`mod.rs`)

```rust
pub mod future_record_metadata;
pub mod record;
pub mod record_metadata;

pub use future_record_metadata::FutureRecordMetadata;
pub use record::{Header, ProducerRecord};
pub use record_metadata::RecordMetadata;
```

Update `src/clients/mod.rs` to add `pub mod producer;`.

## Verification

1. `cargo build`
2. `cargo test`
3. `cargo xtask format-check`
4. `cargo xtask lint`

All existing 579+ tests must continue to pass.
