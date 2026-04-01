# Confluent Kafka Rust Client — Producer Design Document

| **Author** | Shivsundar R |
|---|---|
| **Status** | Draft |
| **Created** | 2026-04-01 |
| **Component** | KafkaProducer, RecordAccumulator |
| **Java Reference** | Apache Kafka 4.2 — `org.apache.kafka.clients.producer` |

---

## 1. Overview

This document describes the design of the Rust KafkaProducer and RecordAccumulator, translated from the Java Kafka client. The Rust implementation preserves the same functional behavior (batching, memory bounding, background sending) while applying Rust-idiomatic patterns for ownership, concurrency, and error handling.

**Key principle:** The Java client is a reference for *what* the producer does. The Rust implementation optimizes *how* it does it, leveraging Rust's ownership model, zero-cost abstractions, and async/await.

---

## 2. Architecture

### 2.1 Component Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                          User Code                                  │
│                                                                     │
│   let record = ProducerRecord::new("topic").key(b"k").value(b"v"); │
│   let future = producer.send(&record).await?;                      │
│   let metadata = future.await?;                                    │
└───────────────────────────┬─────────────────────────────────────────┘
                            │
                            ▼
┌───────────────────────────────────────────────────┐
│               KafkaProducer<C>                    │
│                                                   │
│  - Validates topic                                │
│  - Resolves partition (hardcoded = 0 for now)     │
│  - Generates timestamp if not provided            │
│  - Delegates to RecordAccumulator.append()        │
│  - Returns SendFuture to caller                   │
│                                                   │
│  Public API:                                      │
│    send(&ProducerRecord) -> Result<SendFuture>    │
│    flush() -> Result<()>                          │
│    close() -> Result<()>                          │
│    partitions_for(topic) -> Result<Vec<Info>>     │
└───────────────────────────┬───────────────────────┘
                            │
                            ▼
┌───────────────────────────────────────────────────┐
│             RecordAccumulator                     │
│                                                   │
│  Responsibilities:                                │
│    1. Memory bounding via Semaphore               │
│    2. Batch management per TopicPartition         │
│    3. Notifying sender when batches are ready     │
│                                                   │
│  Internal state (behind Mutex):                   │
│    - current_batches: HashMap<TP, ProducerBatch>  │
│    - ready_batches: Vec<ProducerBatch>            │
│    - closed: bool                                 │
└──────────┬───────────────────────┬────────────────┘
           │                       │
           │ Notify                │ drain()
           ▼                       ▼
┌───────────────────────────────────────────────────┐
│              Sender (tokio::spawn)                │
│                                                   │
│  Background task loop:                            │
│    1. Expire lingering batches (linger.ms)        │
│    2. Drain ready batches from accumulator        │
│    3. Send via ProduceClient trait                │
│    4. Complete batches (resolve SendFutures)      │
│    5. Release memory permits                      │
│    6. Wait for next batch or timeout              │
└───────────────────────────┬───────────────────────┘
                            │
                            ▼
┌───────────────────────────────────────────────────┐
│          ProduceClient (trait — mockable)         │
│                                                   │
│  send_produce_request(node, acks, timeout,        │
│                        batches) -> Result<Vec<R>> │
│  partitions_for(topic) -> Result<Vec<Info>>       │
│                                                   │
│  Implementations:                                 │
│    - MockProduceClient (testing)                  │
│    - [Real NetworkClient — future]                │
└───────────────────────────────────────────────────┘
```

### 2.2 Data Flow

```
User's key/value bytes (on stack or heap, owned by user)
        │
        │  borrow (&[u8])  ← zero copy
        ▼
ProducerRecord<'a> (borrows key, value, headers)
        │
        │  borrow (&[u8])  ← still zero copy
        ▼
RecordAccumulator.append()
        │
        │  memcpy into Vec<u8>  ← SINGLE COPY (serialization)
        ▼
ProducerBatch.buffer (owns the serialized bytes)
        │
        │  borrow (&[u8])  ← zero copy to network
        ▼
ProduceClient.send_produce_request()
        │
        │  response
        ▼
batch.complete() → oneshot::send(RecordMetadata)
        │
        │  resolves
        ▼
Caller's SendFuture.await → RecordMetadata
```

---

## 3. Design Decisions

### 3.1 Zero-Copy Record Ownership

| | Java | Rust |
|---|---|---|
| Record creation | `new ProducerRecord(topic, key, value)` copies `byte[]` | `ProducerRecord::new("topic").key(&bytes)` borrows `&[u8]` |
| Memory cost at send | 1 extra copy (record owns data) | 0 extra copies (record borrows data) |
| When data is copied | At record creation AND at batch append | Only at batch append (single copy) |
| Total copies user→network | 2-3 | 1 |

**Rationale:** CLAUDE.md rule 12 requires "Don't copy byte arrays holding the key, value or headers passed to ProduceRecord." The lifetime parameter `'a` on `ProducerRecord<'a>` enforces at compile time that the borrowed data lives long enough.

### 3.2 Callbacks → async/await with SendFuture

| | Java | Rust |
|---|---|---|
| Async result | `Callback` interface with `onCompletion(metadata, exception)` | `SendFuture` (wraps `oneshot::Receiver`) |
| Usage pattern | Register callback, hope it fires | `let meta = future.await?` |
| Error handling | Exception parameter in callback | `Result<RecordMetadata, KafkaError>` |
| Thread safety | Callback runs on Sender thread | Future resolves on awaiting task |

**Rationale:** CLAUDE.md rule 9 — "Translate callbacks to code that is executed after awaiting the corresponding call in Rust."

**Implementation:** Each record gets a `oneshot::channel()`. The `Sender` half is stored in `ProducerBatch`, the `Receiver` half is wrapped in `SendFuture` and returned to the caller.

### 3.3 BufferPool → Semaphore

| | Java | Rust |
|---|---|---|
| Mechanism | `BufferPool` recycles `ByteBuffer` objects | `tokio::sync::Semaphore` (permits = bytes) |
| Allocation | Returns pooled buffer or allocates new | Always allocates fresh `Vec<u8>` |
| Deallocation | Returns buffer to pool free-list | Drops `Vec<u8>`, adds permits back |
| Why recycling | JVM GC pressure, expensive direct buffers | Not needed — Rust has no GC, `Vec` alloc is cheap |
| Backpressure | `allocate()` blocks until memory available | `semaphore.acquire()` blocks until permits available |
| Timeout | `max.block.ms` | `tokio::time::timeout(max_block, acquire)` |

**Rationale:** Buffer pooling solves a JVM-specific problem (GC pressure from allocating/freeing many `ByteBuffer` objects). Rust's deterministic memory management (`Drop`) makes recycling unnecessary complexity.

### 3.4 Sender Thread → Tokio Task

| | Java | Rust |
|---|---|---|
| Execution | `new Thread(sender).start()` | `tokio::spawn(sender.run())` |
| I/O model | `Selector.select()` (NIO) | `tokio` async runtime |
| Wakeup | `selector.wakeup()` | `Notify.notify_one()` |
| Shutdown | `Thread.interrupt()` + `join()` | `accumulator.close()` + `handle.await` |

**Rationale:** CLAUDE.md rule 8 — "Use non-blocking IO (Tokio) with a single Selector for multiple TCP connections."

### 3.5 Properties Map → Typed Builder

| | Java | Rust |
|---|---|---|
| Config input | `Properties` / `Map<String, Object>` | `ProducerConfig::builder()` |
| Key validation | Runtime — typo in key = silent misconfiguration | Compile time — typo = compile error |
| Value validation | Runtime — wrong type = `ClassCastException` | Compile time — wrong type = type error |
| Enum values | Strings (`"all"`, `"1"`, `"0"`) | `Acks::All`, `Acks::Leader`, `Acks::None` |

### 3.6 Exception Hierarchy → ErrorCode Enum

| | Java | Rust |
|---|---|---|
| Structure | `TimeoutException extends RetriableException extends KafkaException` | `KafkaError { code: ErrorCode, message, source }` |
| Classification | `instanceof RetriableException` | `error.is_retriable()` |
| Fatal check | `instanceof AuthenticationException` | `error.is_fatal()` |
| Txn abort | `instanceof ProducerFencedException` | `error.txn_requires_abort()` |

**Rationale:** Rust has no class inheritance. An enum + classification methods provides the same behavior with exhaustive match checking.

### 3.7 Concurrency: Single Mutex vs Per-Partition Locks

| | Java | Rust (current) |
|---|---|---|
| Lock granularity | Per-partition (`synchronized(deque)`) | Single `Mutex<AccumulatorInner>` |
| Contention | Tasks on different partitions never block each other | All tasks contend on one lock |
| Complexity | Higher (many locks, careful ordering) | Lower (one lock, simple reasoning) |
| Performance | Better under high concurrency | Sufficient for moderate throughput |

**Rationale:** Start simple. The lock is held only for microseconds (appending bytes to a `Vec`). Profile before optimizing. A `DashMap` or sharded lock can be added later if contention is measured.

---

## 4. Module Structure

```
src/
├── lib.rs                              # Root — declares clients, common, errors
├── errors/
│   └── mod.rs                          # KafkaError, ErrorCode, Result<T>
├── common/
│   ├── mod.rs
│   ├── topic_partition.rs              # TopicPartition
│   ├── uuid.rs                         # UUID type
│   └── protocol/                       # Wire protocol (existing)
│       ├── readable.rs
│       ├── writable.rs
│       ├── byte_buffer_accessor.rs
│       └── varint.rs
└── clients/
    ├── mod.rs
    └── producer/
        ├── mod.rs                      # Re-exports public types
        ├── config.rs                   # ProducerConfig + ProducerConfigBuilder
        ├── record.rs                   # ProducerRecord<'a>, Header<'a>, RecordMetadata
        ├── batch.rs                    # ProducerBatch, SendFuture
        ├── accumulator.rs             # RecordAccumulator
        ├── sender.rs                  # Sender, ProduceClient trait, PartitionResponse
        └── kafka_producer.rs          # KafkaProducer<C>
```

---

## 5. Key Type Signatures

### ProducerRecord (borrows user data)
```rust
pub struct ProducerRecord<'a> {
    topic: &'a str,
    partition: Option<i32>,
    key: Option<&'a [u8]>,
    value: Option<&'a [u8]>,
    timestamp: Option<i64>,
    headers: Vec<Header<'a>>,
}
```

### KafkaProducer (generic over network client)
```rust
pub struct KafkaProducer<C: ProduceClient> {
    inner: Arc<ProducerInner<C>>,
}

// Public API:
pub async fn send(&self, record: &ProducerRecord<'_>) -> Result<SendFuture>;
pub async fn flush(&self) -> Result<()>;
pub async fn close(&self) -> Result<()>;
pub async fn partitions_for(&self, topic: &str) -> Result<Vec<PartitionInfo>>;
```

### ProduceClient (mock boundary)
```rust
#[async_trait]
pub trait ProduceClient: Send + Sync + 'static {
    async fn send_produce_request(
        &self, node_id: i32, acks: Acks, timeout: Duration,
        batches: Vec<(TopicPartition, Vec<u8>)>,
    ) -> Result<Vec<PartitionResponse>>;
    async fn partitions_for(&self, topic: &str) -> Result<Vec<PartitionInfo>>;
}
```

---

## 6. Configuration Defaults

| Config | Default | Java Default | Notes |
|---|---|---|---|
| `batch.size` | 16384 bytes | 16384 bytes | Same |
| `linger.ms` | 0 ms | 0 ms | Same |
| `buffer.memory` | 33554432 (32 MB) | 33554432 (32 MB) | Same |
| `max.block.ms` | 60000 ms | 60000 ms | Same |
| `acks` | All | all | Same (typed enum vs string) |
| `retries` | 2147483647 | 2147483647 | Same (i32::MAX) |
| `max.in.flight.requests.per.connection` | 5 | 5 | Same (not yet enforced) |
| `request.timeout.ms` | 30000 ms | 30000 ms | Same |
| `delivery.timeout.ms` | 120000 ms | 120000 ms | Same |
| `retry.backoff.ms` | 100 ms | 100 ms | Same |

---

## 7. Test Coverage

| Module | Tests | What's Covered |
|---|---|---|
| `errors` | 5 | Retriable/fatal/txn classification, Display, From\<io::Error\> |
| `topic_partition` | 2 | Construction, equality, hash set membership |
| `config` | 4 | Defaults, custom values, missing/empty bootstrap servers |
| `record` | 4 | Builder pattern, minimal record, size estimation, metadata |
| `batch` | 6 | Append, full batch rejection, closed batch, headers, complete with success/error |
| `accumulator` | 5 | Batch creation/reuse, drain, flush, close prevents appends |
| `kafka_producer` | 5 | Send + receive metadata, empty topic rejected, multiple records, clone shares state, partitions_for |
| **Total new** | **31** | |
| **Total (with existing)** | **107** | |

---

## 8. Known Gaps vs Java Client

### 8.1 Not Yet Implemented

| Feature | Java Component | Priority | Notes |
|---|---|---|---|
| Retry logic | `Sender.completeBatch()` | High | Transient failures should be retried automatically |
| Reliable flush | `Sender.awaitFlushCompletion()` | High | Current flush is best-effort, needs barrier |
| Close/shutdown race | `KafkaProducer.close(Duration)` | High | In-flight batches may be lost |
| Pipelined requests | `InFlightRequests` | Medium | Currently sequential (max.in.flight=1 effective) |
| Partitioner | `DefaultPartitioner` | Medium | Hardcoded to partition 0 |
| Serializers | `Serializer<K>`, `Serializer<V>` | Medium | Users provide raw bytes directly |
| Interceptors | `ProducerInterceptor` chain | Low | |
| Metrics | `Metrics`, `Sensor`, JMX | Low | |
| Transactions | `TransactionManager` | Low | |
| Idempotence | `ProducerIdAndEpoch` | Low | |

### 8.2 Known Design Issues

| Issue | Severity | Description |
|---|---|---|
| Semaphore permit drift | Low | Acquire uses estimated size, release uses actual written bytes. Over many records, permits may leak. Fix: track and release the exact acquired amount. |
| Single Mutex contention | Low | All partitions share one lock. Fix: use `DashMap` or per-partition locks if profiling shows contention. |

---

## 9. Dependencies Added

| Crate | Version | Purpose |
|---|---|---|
| `tokio` | 1.x | Async runtime, Mutex, Semaphore, Notify, channels, timers |
| `async-trait` | 0.1.x | `async fn` in trait definitions (`ProduceClient`) |

---

## 10. Future Work

1. **Implement retries in Sender** — respect `retries`, `retry.backoff.ms`, and `delivery.timeout.ms`
2. **Reliable flush** — add a flush barrier so `flush()` blocks until all acked
3. **Per-partition locking** — replace single Mutex with DashMap if benchmarks warrant
4. **Partitioner trait** — murmur2 hash for keyed records, round-robin/sticky for unkeyed
5. **Real NetworkClient** — implement `ProduceClient` with TCP connections and wire protocol
6. **Wire format** — use the existing `Writable` trait in `ProducerBatch` for proper Kafka record batch format instead of simplified serialization
