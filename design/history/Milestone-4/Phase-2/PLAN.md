# Phase 2: Producer Trait

## Goal

Translate Java's `Producer<K,V>` interface to a Rust trait (simplified — no transactions,
no callback, no metrics, no serializers).

## Java Source

`org.apache.kafka.clients.producer.Producer` (117 lines, 16 methods)

## Trait Definition (`src/clients/producer/producer.rs`)

```rust
use crate::common::kafka_error::KafkaError;
use crate::common::partition_info::PartitionInfo;
use super::future_record_metadata::FutureRecordMetadata;
use super::record::ProducerRecord;
use std::time::Duration;

/// The interface for the KafkaProducer.
///
/// Translated from `org.apache.kafka.clients.producer.Producer`.
pub trait Producer: Send + Sync {
    /// Send a record to Kafka.
    ///
    /// Returns a future that will eventually contain the record metadata
    /// (offset, timestamp, partition) assigned by the broker.
    fn send(&self, record: ProducerRecord) -> Result<FutureRecordMetadata, KafkaError>;

    /// Flush all accumulated records, blocking until all sends are complete.
    fn flush(&self) -> Result<(), KafkaError>;

    /// Get the partition metadata for a topic.
    fn partitions_for(&self, topic: &str) -> Result<Vec<PartitionInfo>, KafkaError>;

    /// Close this producer. Blocks until all previously sent records are acknowledged.
    fn close(&mut self);

    /// Close this producer with a timeout.
    fn close_with_timeout(&mut self, timeout: Duration);
}
```

## Design Decisions

1. **`&self` for send/flush** — Java uses `synchronized` blocks inside MockProducer. In Rust,
   interior mutability via `Mutex` allows `&self` methods. This also matches how `Arc<dyn Producer>`
   is used from the FFI layer.

2. **`&mut self` for close** — Closing is a terminal operation. `&mut self` prevents concurrent
   use after close begins. Matches Java's `Closeable.close()` semantics.

3. **`Result` return types** — Java methods throw unchecked exceptions (`IllegalStateException`,
   `KafkaException`). Per CLAUDE.md rule 10.2: "Return a Result when Java code throws an
   exception even if unchecked but recoverable."

4. **`ProducerRecord` by value** — Ownership transfers to the producer. The caller constructs
   a record and hands it off. This avoids lifetime complexity and matches Java's semantics where
   the producer holds a reference via GC.

5. **No generics** — Without serializers, the trait works with raw bytes. If serializers are
   added later, the trait can become `Producer<K, V>` with a `Serializer<K>` constraint.

## Methods Excluded (with rationale)

| Java Method | Reason |
|-------------|--------|
| `initTransactions()` | Transactional — out of scope |
| `beginTransaction()` | Transactional — out of scope |
| `commitTransaction()` | Transactional — out of scope |
| `abortTransaction()` | Transactional — out of scope |
| `sendOffsetsToTransaction(...)` | Transactional — out of scope |
| `send(record, callback)` | Callback — out of scope |
| `metrics()` | Metrics — out of scope |
| `clientInstanceId(timeout)` | Telemetry — out of scope |
| `registerMetricForSubscription(metric)` | Metrics — out of scope |
| `unregisterMetricFromSubscription(metric)` | Metrics — out of scope |

## Module Updates

Add to `src/clients/producer/mod.rs`:
```rust
pub mod producer;
pub use producer::Producer;
```

## Verification

1. `cargo build`
2. `cargo test`
3. `cargo xtask format-check`
4. `cargo xtask lint`
