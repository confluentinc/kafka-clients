# Phase 3: MockProducer

## Goal

Translate Java's `MockProducer<K,V>` to Rust (non-transactional subset).

## Java Source

`org.apache.kafka.clients.producer.MockProducer` (597 lines)

## Implementation (`src/clients/producer/mock_producer.rs`)

### Struct

```rust
use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

pub struct MockProducer {
    inner: Mutex<MockProducerInner>,
}

struct MockProducerInner {
    cluster: Cluster,
    auto_complete: bool,
    sent: Vec<ProducerRecord>,
    completions: VecDeque<Completion>,
    offsets: HashMap<TopicPartition, i64>,
    closed: bool,

    // Error injection fields (matching Java's public fields)
    send_error: Option<KafkaError>,
    flush_error: Option<KafkaError>,
    partitions_for_error: Option<KafkaError>,
    close_error: Option<KafkaError>,
}
```

Thread safety: `Mutex<MockProducerInner>` matches Java's `synchronized` methods.

### Completion Inner Struct

```rust
struct Completion {
    offset: i64,
    metadata: RecordMetadata,
    sender: Option<CompletionSender>,  // oneshot::Sender
    topic_partition: TopicPartition,
}
```

`complete(error: Option<KafkaError>)`:
- If error is None: send `Ok(metadata)` through the channel
- If error is Some: send `Err(error)` through the channel

### Constructors

```rust
impl MockProducer {
    /// Full constructor matching Java's MockProducer(Cluster, boolean, ...).
    pub fn new(cluster: Cluster, auto_complete: bool) -> Self;

    /// Convenience: empty cluster, specified auto_complete.
    /// Matches Java's MockProducer(boolean, Partitioner, Serializer, Serializer).
    pub fn with_auto_complete(auto_complete: bool) -> Self;
}

impl Default for MockProducer {
    /// Matches Java's MockProducer() — empty cluster, auto_complete=false.
    fn default() -> Self;
}
```

### Producer Trait Implementation

```rust
impl Producer for MockProducer {
    fn send(&self, record: ProducerRecord) -> Result<FutureRecordMetadata, KafkaError>;
    fn flush(&self) -> Result<(), KafkaError>;
    fn partitions_for(&self, topic: &str) -> Result<Vec<PartitionInfo>, KafkaError>;
    fn close(&mut self);
    fn close_with_timeout(&mut self, timeout: Duration);
}
```

**`send()` flow** (matching Java lines 278-330, simplified):
1. Lock inner
2. If closed → `return Err(KafkaError::with_message(Errors::IllegalState, "MockProducer is already closed."))`
3. If `send_error` is set → return the error
4. Determine partition: `record.partition().unwrap_or(0)` (no partitioner)
5. Create `TopicPartition`
6. Get next offset via `next_offset(tp)`
7. Create `RecordMetadata` with the offset
8. Create oneshot channel
9. Create `Completion { offset, metadata, sender, tp }`
10. Add record to `sent`
11. If auto_complete → call `completion.complete(None)` immediately
12. Else → push to `completions` deque
13. Return `Ok(FutureRecordMetadata::new(receiver))`

**`flush()` flow** (matching Java lines 347-356):
1. Lock inner
2. If closed → `return Err(IllegalState)`
3. If `flush_error` is set → return the error
4. Complete all pending completions: `while let Some(c) = completions.pop_front() { c.complete(None); }`
5. Return Ok(())

**`partitions_for()` flow** (matching Java lines 358-363):
1. If `partitions_for_error` is set → return the error
2. Return `cluster.partitions_for_topic(topic)` (clone the Vec)

**`close()` / `close_with_timeout()` flow** (matching Java lines 412-423):
1. If `close_error` is set → log/ignore (Java throws, but close shouldn't fail in Rust)
2. Set `closed = true`

### Test Inspection Methods

```rust
impl MockProducer {
    /// Get the list of sent records since the last call to clear().
    /// Matches Java's history().
    pub fn history(&self) -> Vec<ProducerRecord>;

    /// Clear sent records, completions, and offsets.
    /// Matches Java's clear().
    pub fn clear(&self);

    /// Complete the earliest uncompleted call successfully.
    /// Returns true if there was a completion to process.
    /// Matches Java's completeNext().
    pub fn complete_next(&self) -> bool;

    /// Complete the earliest uncompleted call with the given error.
    /// Returns true if there was a completion to process.
    /// Matches Java's errorNext(RuntimeException).
    pub fn error_next(&self, error: KafkaError) -> bool;

    /// Returns true if the producer is closed.
    /// Matches Java's closed().
    pub fn closed(&self) -> bool;

    /// Returns true if there are no pending completions.
    /// Matches Java's flushed().
    pub fn flushed(&self) -> bool;

    /// Set an error to be returned on the next send() call.
    pub fn set_send_error(&self, error: Option<KafkaError>);

    /// Set an error to be returned on the next flush() call.
    pub fn set_flush_error(&self, error: Option<KafkaError>);

    /// Set an error to be returned on the next partitions_for() call.
    pub fn set_partitions_for_error(&self, error: Option<KafkaError>);

    /// Set an error to be returned on the next close() call.
    pub fn set_close_error(&self, error: Option<KafkaError>);
}
```

### Private Helpers

```rust
/// Get the next offset for this topic/partition.
/// Matches Java's nextOffset(TopicPartition).
fn next_offset(offsets: &mut HashMap<TopicPartition, i64>, tp: &TopicPartition) -> i64;
```

First call for a tp returns 0 and stores 1. Subsequent calls increment.

## Module Updates

Add to `src/clients/producer/mod.rs`:
```rust
pub mod mock_producer;
pub use mock_producer::MockProducer;
```

## Verification

1. `cargo build`
2. `cargo test`
3. `cargo xtask format-check`
4. `cargo xtask lint`
