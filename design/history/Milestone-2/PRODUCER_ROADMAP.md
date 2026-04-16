# Milestone 2: Kafka Producer Implementation Plan

## Context

Milestone 1 is complete: NetworkClient, Selector, wire protocol, metadata, and 197 generated message types (including ProduceRequest/ProduceResponse data structs) are all working with 447 unit tests + 11 integration tests against Kafka 4.2.0.

Milestone 2 goal: **"The Producer should be able to accumulate messages into batches to produce in a Produce RPC."**

This plan translates the Java `KafkaProducer` and its dependency tree into idiomatic Rust, following CLAUDE.md translation rules.

**Important prerequisite:** The current `KafkaClient` trait is sync, with `NetworkClient` using a `block_on()` noop-waker shim to call async `Selectable` methods. The Sender needs real async I/O, so Phase 7 includes making `KafkaClient` async (5 methods: `ready`, `poll`, `disconnect`, `close_connection`, `close`).

## Scope

**In scope:** ProducerRecord, RecordMetadata, Serializer, Headers, Record format (DefaultRecord/DefaultRecordBatch), MemoryRecords/MemoryRecordsBuilder, Compression (gzip/snappy/lz4/zstd), BufferPool, ProducerBatch, RecordAccumulator, BuiltInPartitioner, Sender, ProducerConfig, KafkaProducer, ProduceRequest/Response integration, all corresponding Java tests.

**Out of scope (deferred):** TransactionManager, MockProducer, ProducerInterceptors, SSL/SASL, full metrics framework.

---

## Design Decisions: Java vs Rust Evaluation

Each decision below evaluates what Java does, why, whether it applies to Rust, and what the idiomatic Rust alternative is.

### Decision 1: Compression — Use Rust Crates

**Java:** `Compression` interface with subclasses (`GzipCompression`, `SnappyCompression`, `Lz4Compression`, `ZstdCompression`). Each wraps a JDK or third-party codec, providing `wrapForOutput(ByteBufferOutputStream)` and `wrapForInput(ByteBuffer)`.

**Why Java does it this way:** Java has no built-in compression beyond `java.util.zip`. Each codec ships as a separate library (snappy-java, lz4-java, zstd-jni) with JNI bindings. The `Compression` interface abstracts over the differing APIs.

**Does this apply to Rust?** The abstraction is still needed (same 5 compression types in the Kafka protocol), but the implementation is simpler because Rust crates all follow `std::io::Read`/`Write` patterns.

**Rust approach — Enum with `std::io::Write`/`Read`:**
```rust
pub enum CompressionType {
    None = 0,
    Gzip = 1,
    Snappy = 2,
    Lz4 = 3,
    Zstd = 4,
}

impl CompressionType {
    /// Wraps a writer with the appropriate compression.
    /// Returns a Box<dyn Write> that compresses data written to it.
    pub fn wrap_for_output(&self, output: Vec<u8>) -> Box<dyn Write> {
        match self {
            CompressionType::None => Box::new(output),
            CompressionType::Gzip => Box::new(GzEncoder::new(output, ...)),
            CompressionType::Snappy => ...,
            CompressionType::Lz4 => ...,
            CompressionType::Zstd => ...,
        }
    }
}
```

**Why enum over trait:** The set of compression types is fixed by the Kafka wire protocol (5 values, encoded in a 3-bit field in the batch header). An enum matches this closed set. A trait would suggest extensibility that doesn't exist. Enum dispatch is also cheaper (no vtable indirection).

**Crate choices:**
- `flate2` (gzip) — 150M+ downloads, de facto standard
- `snap` (snappy) — Rust-native, no C dependency
- `lz4_flex` (lz4) — pure Rust, competitive perf, no unsafe
- `zstd` (zstd) — wraps Facebook's C library, best-in-class compression
- All crates implement `std::io::Write`/`Read`, making them plug-and-play

**Verdict:** Use crates, implement as enum. Matches CLAUDE.md rule 1.2 ("popular Rust crate with same behavior").

---

### Decision 2: BufferPool — `tokio::sync::Semaphore` (NOT Java pattern)

**Java:** `BufferPool` uses `ReentrantLock` + `Deque<Condition>` (per-waiter fairness) + `Deque<ByteBuffer>` (free list for recycling). Two purposes: (1) bounded memory enforcement with backpressure, (2) ByteBuffer object recycling to reduce GC pressure.

**Why Java does it this way:**
1. **GC pressure:** Allocating `ByteBuffer` on the heap creates GC pressure. Recycling fixed-size buffers avoids GC pauses under high throughput. This is the *primary* motivation.
2. **Bounded memory:** The `buffer.memory` config limits total producer memory. When exhausted, `allocate()` blocks until another thread calls `deallocate()`, providing natural backpressure.
3. **FIFO fairness:** The `Deque<Condition>` ensures waiters are served in order, preventing starvation.

**Does this apply to Rust?**
- **GC pressure (purpose 1): NO.** Rust has no GC. `Vec<u8>` allocation is `malloc` — deterministic, fast (~20ns), no pauses. Recycling `Vec<u8>` saves one `malloc`+`free` per batch, which is negligible. The *primary* motivation for Java's pool doesn't exist in Rust.
- **Bounded memory (purpose 2): YES.** The producer must enforce `buffer.memory` limits regardless of language. Without bounds, a fast producer would OOM.
- **FIFO fairness (purpose 3): YES.** Waiters should be served in order.

**Rust approach — `tokio::sync::Semaphore`:**
```rust
pub struct BufferPool {
    semaphore: Arc<Semaphore>,  // permits = bytes of memory budget
    total_memory: i64,
    poolable_size: usize,
}

impl BufferPool {
    pub fn new(total_memory: i64, poolable_size: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(total_memory as usize)),
            total_memory,
            poolable_size,
        }
    }

    /// Acquire memory budget, then allocate a fresh Vec<u8>.
    pub async fn allocate(
        &self, size: usize, max_block_ms: i64
    ) -> Result<Vec<u8>, KafkaError> {
        match tokio::time::timeout(
            Duration::from_millis(max_block_ms as u64),
            self.semaphore.acquire_many(size as u32),
        ).await {
            Ok(Ok(permit)) => {
                permit.forget();  // manually release on deallocate
                Ok(Vec::with_capacity(size))
            }
            Ok(Err(_)) => Err(KafkaError::buffer_exhausted()),
            Err(_) => Err(KafkaError::buffer_exhausted()),
        }
    }

    /// Free the buffer and return memory budget to the semaphore.
    pub fn deallocate(&self, buffer: Vec<u8>) {
        let capacity = buffer.capacity();
        drop(buffer);  // free the actual memory (deterministic in Rust)
        self.semaphore.add_permits(capacity);
    }

    pub fn available_memory(&self) -> usize {
        self.semaphore.available_permits()
    }

    pub fn total_memory(&self) -> i64 { self.total_memory }
    pub fn poolable_size(&self) -> usize { self.poolable_size }
}
```

**Why this is better than translating the Java pattern:**

| Aspect | Java pattern (Mutex+Notify+FreeList) | Semaphore approach |
|--------|-------------------------------------|--------------------|
| Lines of code | ~150 | ~40 |
| Correctness | Manual lock/notify coordination, easy to deadlock | Correct by construction |
| FIFO fairness | Manual `Deque<Condition>` | Built into Tokio's Semaphore |
| Buffer recycling | Yes (saves GC pressure) | No (unnecessary — no GC in Rust) |
| Memory bounding | Yes | Yes |
| Backpressure | Yes (blocks on lock condition) | Yes (awaits semaphore) |
| Lock contention | One global lock | Lock-free semaphore internals |

**Potential concern:** `Semaphore::acquire_many` takes `u32`, limiting individual allocations to ~4GB. This is fine — no single batch should be 4GB. The total semaphore permits can exceed `u32::MAX` by using multiple semaphores if needed, but `buffer.memory` defaults to 32MB.

**Future optimization path:** If profiling shows `malloc` is a bottleneck (unlikely), layer a `VecDeque<Vec<u8>>` free list for the common `poolable_size` on top of the semaphore. The semaphore still handles the budget; the free list just avoids one `malloc` call.

**Verdict:** Semaphore approach. Simpler, correct, idiomatic Rust. Preserves all user-facing behavior (`buffer.memory`, `max.block.ms`, backpressure, fairness).

---

### Decision 3: Sender — `tokio::spawn` async task

**Java:** `Sender implements Runnable`, run on a dedicated `KafkaThread` (daemon thread). The `run()` loop calls `sendProducerData()` then `client.poll()` (which blocks on NIO Selector). Wakeup via `selector.wakeup()`.

**Why Java does it this way:** Java NIO requires a dedicated thread to call `Selector.select()`. The thread blocks on I/O, wakes up when data is ready or `wakeup()` is called.

**Does this apply to Rust?** The concept of a background I/O loop is the same, but Rust/Tokio uses cooperative async scheduling instead of dedicated OS threads for I/O.

**Alternatives evaluated:**

| Approach | Pros | Cons |
|----------|------|------|
| `tokio::spawn` (async task) | Lightweight, shares runtime thread pool, natural `.await` for I/O, composes with `tokio::select!` for wakeup | Shares CPU with other tasks (acceptable for I/O-bound work) |
| `std::thread::spawn` + dedicated Tokio runtime | Full CPU isolation, matches Java's dedicated thread | Overhead of a separate runtime, harder to share resources, overkill for I/O-bound work |
| `spawn_blocking` | Runs on blocking thread pool | Designed for sync/CPU-bound work, not long-running async loops |
| Inline with `send()` (drive I/O on each send call) | No background task | Doesn't match Java behavior, blocks the caller, breaks linger.ms semantics |

**Rust approach — `tokio::spawn` with `Notify` wakeup:**
```rust
struct Sender<C: KafkaClient> {
    running: Arc<AtomicBool>,
    force_close: Arc<AtomicBool>,
    wakeup: Arc<Notify>,
    client: C,
    accumulator: Arc<RecordAccumulator>,
    metadata: Arc<ProducerMetadata>,
    // ...config fields
}

impl<C: KafkaClient> Sender<C> {
    pub async fn run(&mut self) {
        while self.running.load(Ordering::Acquire) {
            self.run_once().await;
        }
        // Drain remaining batches on graceful shutdown
        while !self.force_close.load(Ordering::Acquire)
              && self.accumulator.has_undrained() {
            self.run_once().await;
        }
        if self.force_close.load(Ordering::Acquire) {
            self.accumulator.abort_incomplete_batches();
        }
        self.client.close().await;
    }

    async fn run_once(&mut self) {
        let now = current_time_ms();
        let poll_timeout = self.send_producer_data(now).await;

        // Interruptible wait: either I/O completes or producer wakes us
        tokio::select! {
            _ = self.client.poll(poll_timeout, now) => {}
            _ = self.wakeup.notified() => {}
        }
    }
}

// In KafkaProducer constructor:
let sender_handle: JoinHandle<()> = tokio::spawn(async move {
    sender.run().await;
});
```

**Key Rust-specific adaptations:**
1. **Wakeup:** Java's `selector.wakeup()` → `tokio::sync::Notify::notify_one()`. The `tokio::select!` in `run_once` allows the sender to wake immediately when new data arrives, matching `linger.ms` behavior.
2. **Shutdown:** Java's `volatile boolean running` → `Arc<AtomicBool>`. Producer sets `running = false`, calls `wakeup.notify_one()`, then `sender_handle.await` (joins the task).
3. **Ownership:** The Sender *moves* the `NetworkClient` into the spawned task. Only the Sender calls `client.poll()`, matching Java's single-thread ownership model. No `Arc<Mutex<NetworkClient>>` needed.

**Prerequisite:** The `KafkaClient` trait must be made async. Currently `poll()` is sync and uses `block_on()` with a noop waker. This is addressed in Phase 7 — the 5 I/O-touching methods (`ready`, `poll`, `disconnect`, `close_connection`, `close`) become `async fn`. All existing `NetworkClient` tests already use `#[tokio::test]` so the migration is mechanical.

**Verdict:** `tokio::spawn`. Idiomatic, lightweight, composable. Matches CLAUDE.md rule 8 (non-blocking IO with Tokio) and rule 9.1 (callbacks → await).

---

### Decision 4: FutureRecordMetadata — `tokio::sync::watch`

**Java:** `ProduceRequestResult` holds a `CountDownLatch(1)` and result fields. All records in a batch share the same `ProduceRequestResult`. Each record gets a `FutureRecordMetadata` wrapping the shared result + its own `batchIndex`. When the batch completes, `latch.countDown()` wakes all waiters. Each waiter reads the shared result and combines with its own `batchIndex` to compute `offset = baseOffset + batchIndex`.

**Why Java does it this way:** `CountDownLatch` is a one-shot broadcast mechanism — one writer signals, many readers wake. This avoids creating a `Future` per record when a batch may contain hundreds of records.

**Alternatives evaluated:**

| Approach | How it works | Pros | Cons |
|----------|-------------|------|------|
| `tokio::sync::watch` | One sender per batch, clone receiver per record. `send(Some(result))` wakes all receivers | Exact semantic match to CountDownLatch, no extra deps, built into tokio, efficient | `watch` is designed for "latest value" (multi-write), slightly over-engineered for one-shot |
| `tokio::sync::oneshot` per record | One channel per record, batch iterates and sends to each | Simple per-record semantics | Batch with 100 records = 100 channels + 100 sends. O(n) completion cost |
| `Arc<Notify>` + `Arc<OnceLock<Result>>` | `OnceLock` stores result once, `Notify::notify_waiters()` wakes all | Close to CountDownLatch, minimal overhead | Two primitives to coordinate, `Notify::notified()` has a race (must subscribe before notify) |
| `futures::future::Shared<oneshot::Receiver>` | `oneshot` + `.shared()` to clone the future | Idiomatic futures API, `.await` returns result directly | Requires `futures` crate dep, internal `Arc` + `Mutex` overhead per `.clone().await` |
| `tokio::sync::broadcast` | One sender, many receivers | Built for one-to-many | Requires capacity, receivers must subscribe before send, overkill |

**Rust approach — `tokio::sync::watch`:**
```rust
/// Shared result for all records in a batch
struct ProduceResult {
    base_offset: i64,
    log_append_time: i64,
    error: Option<KafkaError>,
    topic_partition: TopicPartition,
}

/// One per batch — owned by ProducerBatch
struct ProduceRequestResult {
    tx: watch::Sender<Option<ProduceResult>>,
}

impl ProduceRequestResult {
    fn new() -> (Self, watch::Receiver<Option<ProduceResult>>) {
        let (tx, rx) = watch::channel(None);  // None = pending
        (Self { tx }, rx)
    }

    fn done(&self, result: ProduceResult) {
        let _ = self.tx.send(Some(result));
        // All receivers wake up automatically
    }
}

/// One per record — returned to the user from producer.send()
struct FutureRecordMetadata {
    rx: watch::Receiver<Option<ProduceResult>>,
    batch_index: i32,
    create_timestamp: i64,
    serialized_key_size: i32,
    serialized_value_size: i32,
}

impl FutureRecordMetadata {
    pub async fn get(&mut self) -> Result<RecordMetadata, KafkaError> {
        // wait_for checks current value first — no race condition
        self.rx.wait_for(|v| v.is_some()).await
            .map_err(|_| KafkaError::producer_closed())?;

        let result = self.rx.borrow();
        let result = result.as_ref().unwrap();

        if let Some(ref err) = result.error {
            return Err(err.clone());
        }

        Ok(RecordMetadata::new(
            result.topic_partition.clone(),
            result.base_offset,
            self.batch_index,
            self.create_timestamp,
            self.serialized_key_size,
            self.serialized_value_size,
        ))
    }
}
```

**Why `watch` wins:**
1. **Semantic match:** One write, many reads — exactly `CountDownLatch` behavior
2. **No race condition:** `wait_for()` checks current value before subscribing, unlike `Notify::notified()` which must be called before the notification
3. **Efficient:** Cloning a `watch::Receiver` is cheap (Arc increment). Batch with 100 records = 1 sender + 100 cloned receivers
4. **No extra deps:** `tokio::sync::watch` is already available (tokio `sync` feature is enabled)
5. **Correct on drop:** If the sender is dropped (batch aborted), `wait_for` returns `Err(RecvError)`, which we map to an error. No leaked futures.

**Verdict:** `tokio::sync::watch`. Best semantic match, correct by construction, no extra deps.

---

### Decision 5: ProducerRecord Generics — Generic `<K, V>` with Serializer Trait

**Java:** `ProducerRecord<K, V>` is generic. `KafkaProducer<K, V>` holds `Serializer<K>` and `Serializer<V>`. On `send()`, key and value are serialized to `byte[]` immediately, and all downstream code works with `byte[]`.

**Why Java does it this way:** Type safety at the API boundary. Users work with `ProducerRecord<String, MyAvroType>` and the serializer handles conversion.

**Alternatives evaluated:**

| Approach | Pros | Cons |
|----------|------|------|
| Generic `ProducerRecord<K, V>` + `Serializer<T>` trait (match Java) | Type-safe, familiar API, catches serialization errors early | Monomorphization: each `(K,V)` pair generates a new `KafkaProducer` in the binary |
| Pre-serialized `ProducerRecord` with `Option<Vec<u8>>` | Simpler, no generics, no Serializer trait, single monomorphization | User must serialize manually, loses type safety |
| `ProducerRecord<K, V>` generic but `KafkaProducer` non-generic (serialize in `send()`) | Producer internals are monomorphic, only `send()` is generic | `send()` needs serializer args or `KafkaProducer` holds `Box<dyn Serializer<K>>` |

**Rust approach — Generic `<K, V>` with trait objects for serializers:**
```rust
pub struct ProducerRecord<K, V> {
    topic: String,
    partition: Option<i32>,
    key: Option<K>,
    value: Option<V>,
    headers: RecordHeaders,
    timestamp: Option<i64>,
}

pub trait Serializer<T: ?Sized>: Send + Sync {
    fn serialize(&self, topic: &str, data: &T) -> Result<Option<Vec<u8>>, KafkaError>;
}

pub struct KafkaProducer<K, V> {
    key_serializer: Box<dyn Serializer<K> + Send + Sync>,
    value_serializer: Box<dyn Serializer<V> + Send + Sync>,
    accumulator: Arc<RecordAccumulator>,  // works with serialized bytes only
    // ...
}
```

**Why keep generics:**
1. **CLAUDE.md says "Preserve original architecture and logical structure"** — the generic API is part of the logical structure
2. **Type safety is valuable** — catching `Serializer<String>` vs `Serializer<i64>` mismatches at compile time
3. **Monomorphization cost is minimal** — most apps have 1-3 producer instances total
4. **All downstream code is monomorphic** — serialization happens at the top of `send()`, everything below works with `Option<Vec<u8>>` (no generic infection through the internals)

**Key Rust adaptation:** Per CLAUDE.md rule 12, "Don't copy byte arrays holding the key, value or headers." After `serialize()` returns `Vec<u8>`, ownership moves through the pipeline (→ `RecordAccumulator.append()` → `MemoryRecordsBuilder.append()` → written to batch buffer) without copying. The `Serializer` trait returns `Option<Vec<u8>>` (owned), not `&[u8]`, to enable this move.

**Verdict:** Generic `<K, V>` with `Box<dyn Serializer>`. Matches Java, type-safe, no generic infection into internals.

---

### Decision 6: CRC32C — `crc32c` Crate

**Java:** `java.util.zip.CRC32C` (added in Java 9), hardware-accelerated via intrinsics on x86.

**Does this apply to Rust?** CRC32C is required by the Kafka wire protocol (record batch header, byte 17-20). Must use the same algorithm.

**Rust approach:** The `crc32c` crate (6M+ downloads) provides hardware-accelerated CRC32C using SSE4.2 on x86 and ARM CRC instructions. API is trivial: `crc32c::crc32c(&[u8]) -> u32`.

**Why not implement from scratch:** Zero benefit. CLAUDE.md rule 1.2 applies — "popular Rust crate with same behavior and equal or better performance."

**Verdict:** Use `crc32c` crate. No evaluation needed — it's the standard.

---

### Decision 7: ByteBufferOutputStream — Use `Vec<u8>` Directly (NO wrapper needed)

**Java:** `ByteBufferOutputStream` wraps a `ByteBuffer` with auto-expansion (1.1x growth factor) and implements `OutputStream`. Needed because `ByteBuffer` has fixed capacity and doesn't implement `OutputStream`.

**Why Java needs this:**
1. `ByteBuffer.allocate(n)` creates a fixed-size buffer — no auto-expansion
2. Compression codecs expect `OutputStream`, not `ByteBuffer`
3. `ByteBufferOutputStream` bridges these gaps

**Does this apply to Rust?** NO on both counts:
1. `Vec<u8>` auto-expands (doubles capacity when full)
2. `Vec<u8>` already implements `std::io::Write` (the Rust equivalent of `OutputStream`)

**What MemoryRecordsBuilder needs from its output buffer:**
1. Append bytes (via compression stream → `Write`) — `Vec<u8>` does this
2. Random access to overwrite batch header fields (e.g., CRC at offset 17) — `Vec<u8>` supports indexing
3. Know the final size — `Vec::len()`
4. Take ownership of the final bytes — just move the `Vec<u8>`

**Rust approach:** Use `Vec<u8>` directly. No wrapper struct needed.

```rust
// MemoryRecordsBuilder construction
let mut buffer = Vec::with_capacity(batch_size);

// Write 61-byte batch header placeholder
buffer.resize(RECORD_BATCH_OVERHEAD, 0);

// Create compression stream writing into the buffer
let compression_stream = compression_type.wrap_for_output(&mut buffer);

// After all records written, close compression stream
drop(compression_stream);

// Overwrite batch header fields
buffer[CRC_OFFSET..CRC_OFFSET+4].copy_from_slice(&crc.to_be_bytes());
buffer[RECORDS_COUNT_OFFSET..RECORDS_COUNT_OFFSET+4].copy_from_slice(&count.to_be_bytes());
```

**Verdict:** Use `Vec<u8>` directly. `ByteBufferOutputStream` is a Java-ism that solves problems Rust doesn't have. This eliminates one entire source file and its tests.

---

### Decision 8: RecordAccumulator Concurrent Map — `DashMap` vs `RwLock<HashMap>`

**Java:** `CopyOnWriteMap<String, TopicInfo>` — a concurrent map optimized for reads (clones the entire map on writes). Topics change rarely but lookups happen on every `send()`.

**Alternatives in Rust:**

| Approach | Read perf | Write perf | Complexity |
|----------|-----------|------------|------------|
| `DashMap<String, TopicInfo>` | Excellent (sharded, lock per shard) | Good | Low (drop-in HashMap replacement) |
| `RwLock<HashMap<String, TopicInfo>>` | Good (shared read lock) | Good (exclusive write lock) | Low |
| `ArcSwap<HashMap<...>>` | Best (lock-free reads) | Expensive (clone entire map) | Medium |

**Analysis:** The access pattern is many reads (one per `send()` call), rare writes (new topic added). All three approaches handle this well. `DashMap` is the simplest and provides sharded locking which reduces contention under high throughput. `RwLock<HashMap>` is equally valid but requires manual lock scope management.

**Verdict:** `DashMap`. Simplest API, good performance for read-heavy workloads, avoids lock scope bugs.

---

### Decision 9: KafkaClient Async Conversion (Prerequisite)

**Current state:** `KafkaClient` trait methods are sync. `NetworkClient` uses `block_on()` with a noop waker to call async `Selectable::poll()`. This works only if the async future resolves immediately (true for `MockSelector`, false for real TCP I/O).

**Problem for Producer:** The Sender needs to actually await I/O. `block_on()` with noop waker panics if the future is `Pending`.

#### Why Java doesn't need async KafkaClient but Rust does

**Java NIO threading model:**
```
User thread                          Sender thread (dedicated OS thread)
-----------                          -----------------------------------
producer.send(record)                while (running) {
  -> accumulator.append()                sendProducerData();    // drain batches
  -> sender.wakeup()  ──────────────>    client.poll(timeout);  // BLOCKS here
                                     }
```

In Java, `client.poll()` calls `selector.select(timeout)`, which is a **blocking OS call**. Under the hood, Java NIO's `Selector.select()` calls `epoll_wait()` (Linux) or `kqueue()` (macOS). This **blocks the entire OS thread** until:
1. I/O is ready on any registered channel (data to read/write)
2. `selector.wakeup()` is called from another thread (writes to an internal pipe to break the `epoll_wait`)
3. The timeout expires

Java's sync approach works because **it dedicates an entire OS thread** to the Sender. Blocking one thread is acceptable overhead.

**Tokio threading model:**
```
Tokio runtime (e.g., 4 worker threads shared by ALL tasks)
├── Task A (user work)
├── Task B (HTTP server)
├── Task C (Sender)        <-- if this blocks, it starves Tasks A, B, D
└── Task D (other work)
```

In Tokio, tasks share a thread pool via **cooperative scheduling**. If the Sender blocks a worker thread waiting for I/O, it **starves all other tasks** on that thread. Tokio's golden rule: **never block inside an async context**.

Tokio's equivalent of `selector.select()`:
1. Register all TcpStreams with epoll/kqueue
2. Return `Poll::Pending` — the runtime parks this task and frees the thread
3. When I/O is ready, the runtime wakes the task
4. The task resumes from the `.await` point

This is truly non-blocking — **zero threads consumed** while waiting for I/O.

**Comparison:**

| | Java NIO | Tokio |
|---|----------|-------|
| `select()`/`poll()` | **Blocks the OS thread** (calls `epoll_wait` synchronously) | **Yields the task** (returns `Pending`, frees the thread) |
| Thread model | Dedicated thread per client (blocking is OK) | Shared thread pool (blocking is fatal) |
| Wakeup mechanism | `selector.wakeup()` writes to a pipe → breaks `epoll_wait` | `Notify::notify_one()` → wakes the parked task |
| Cost of waiting | One OS thread parked in kernel | Zero threads consumed |

**Current codebase hack:**
```rust
fn block_on<F: Future>(fut: F) -> F::Output {
    // Polls the future ONCE with a noop waker
    // Panics if the future returns Pending
    match fut.poll(&mut cx) {
        Poll::Ready(val) => val,
        Poll::Pending => panic!("not immediately ready"),
    }
}
```
This works for `MockSelector` (all futures resolve immediately in tests) but would **panic with real TCP I/O** where `Selector::poll()` returns `Pending` while waiting for network data.

#### Solution

Make 5 KafkaClient methods async:
- `ready(&mut self, node, now)` → `async fn ready(...)`
- `poll(&mut self, timeout, now)` → `async fn poll(...)`
- `disconnect(&mut self, node_id)` → `async fn disconnect(...)`
- `close_connection(&mut self, node_id)` → `async fn close_connection(...)`
- `close(&mut self)` → `async fn close(...)`

The remaining methods stay sync (they don't touch Selectable I/O): `is_ready`, `send`, `least_loaded_node`, `connection_delay`, `poll_delay_ms`, `connection_failed`, `authentication_error`, `in_flight_request_count`, `has_in_flight_requests`, `has_ready_nodes`, `wakeup`, `new_client_request`, `new_client_request_with_timeout`, `initiate_close`, `active`.

This removes the `block_on()`/`noop_waker()` hack from `NetworkClient` and replaces it with proper `.await` calls to the async `Selectable` methods. All existing NetworkClient tests already use `#[tokio::test]`, so the migration is mechanical (add `.await` to call sites).

This is done in Phase 7 alongside the Sender, since the Sender is the first consumer that needs real async.

---

### Decision 10: Existing `MemoryPool` trait — Relationship to BufferPool

**Current state:** `src/common/memory/memory_pool.rs` defines a `MemoryPool` trait with `try_allocate()`/`release()` and a `NoopMemoryPool` implementation. This is used by the network layer (Selector/KafkaChannel).

**Java relationship:** `MemoryPool` (non-blocking, for the network layer) and `BufferPool` (blocking, for the producer) are separate classes. `MemoryPool` is a simple interface; `BufferPool` is a complex producer-specific implementation with backpressure.

**Rust approach:** Keep them separate, matching Java. The existing `MemoryPool` trait is sync and non-blocking (`try_allocate` returns `Option`). The producer's `BufferPool` is async and blocking (`allocate` awaits semaphore). They serve different layers and should not be unified.

---

## Phase Structure (8 Phases)

### Phase 1: Foundation Types (Headers, Compression, Serializers)
**Depends on:** Nothing  
**Complexity:** Low-Medium

New files:
- `src/common/header/mod.rs` — `Header` trait, `Headers` trait
- `src/common/header/internals/mod.rs`
- `src/common/header/internals/record_header.rs` — `RecordHeader` struct
- `src/common/header/internals/record_headers.rs` — `RecordHeaders` struct
- `src/common/record/mod.rs`
- `src/common/record/timestamp_type.rs` — `TimestampType` enum
- `src/common/record/compression_type.rs` — `CompressionType` enum
- `src/common/record/compression_ratio_estimator.rs` — per-topic ratio tracking
- `src/common/record/record_batch.rs` — constants (`MAGIC_VALUE_V2`, `NO_PRODUCER_ID`, `RECORD_BATCH_OVERHEAD = 61`, etc.)
- `src/common/record/record_version.rs` — `RecordVersion` enum
- `src/common/compress/mod.rs` — CompressionType enum with wrap_for_output/wrap_for_input
- `src/common/serialization/mod.rs` — `Serializer<T>` trait
- `src/common/serialization/string_serializer.rs`
- `src/common/serialization/byte_array_serializer.rs`

Modified: `src/common/mod.rs` (add `header`, `record`, `compress`, `serialization` modules)  
Modified: `Cargo.toml` (add `flate2`, `snap`, `lz4_flex`, `zstd`, `crc32c`)

Java tests to translate: `RecordHeader`/`RecordHeaders` tests, `CompressionTypeTest`, `TimestampTypeTest`, serializer tests, `CompressionRatioEstimatorTest`

### Phase 2: Record Format (DefaultRecord, SimpleRecord)
**Depends on:** Phase 1  
**Complexity:** High

New files:
- `src/common/record/record.rs` — `Record` trait (offset, timestamp, key, value, headers)
- `src/common/record/simple_record.rs` — simple record wrapper
- `src/common/record/default_record.rs` — varint-encoded record format, `write_to()`, `read_from()`, `size_of()`

Reuses: existing `src/common/protocol/varint.rs` for varint encoding  
No `ByteBufferOutputStream` — using `Vec<u8>` directly (Decision 7)

Java tests: `DefaultRecordTest.java` (511 lines)

### Phase 3: Record Batch (DefaultRecordBatch, MemoryRecordsBuilder, MemoryRecords)
**Depends on:** Phase 2  
**Complexity:** Very High (most complex phase)

New files:
- `src/common/record/default_record_batch.rs` — 61-byte batch header format, CRC32C computation
- `src/common/record/memory_records_builder.rs` — core batch builder (compression stream, state machine)
- `src/common/record/memory_records.rs` — readonly record container, `builder()` factory
- `src/common/record/abstract_records.rs` — utility functions (size estimation)

Java tests: `DefaultRecordBatchTest.java` (583 lines), `MemoryRecordsBuilderTest.java` (642 lines), `MemoryRecordsTest.java` (write-path tests from 1246 lines)

### Phase 4: Producer API Types (ProducerRecord, RecordMetadata, ProduceRequest/Response)
**Depends on:** Phase 1, Phase 3  
**Complexity:** Medium

New files:
- `src/clients/producer/mod.rs`
- `src/clients/producer/producer_record.rs` — `ProducerRecord<K, V>` struct
- `src/clients/producer/record_metadata.rs` — `RecordMetadata` struct
- `src/clients/producer/producer_config.rs` — `ProducerConfig` with named fields
- `src/common/requests/produce_request.rs` — `ProduceRequest` + `ProduceRequestBuilder`
- `src/common/requests/produce_response.rs` — `ProduceResponse`

Modified: `src/common/requests/abstract_request.rs` — add `Produce` variant to `ConcreteRequest`  
Modified: `src/common/requests/abstract_response.rs` — add `Produce` variant to `ConcreteResponse`  
Modified: `src/clients/mod.rs` — add `pub mod producer;`

Java tests: `ProducerRecordTest.java`, `RecordMetadataTest.java`

### Phase 5: Producer Internals Foundation (BufferPool, FutureRecordMetadata, ProduceRequestResult)
**Depends on:** Phase 4  
**Complexity:** Medium-High

New files:
- `src/clients/producer/internals/mod.rs`
- `src/clients/producer/internals/buffer_pool.rs` — Semaphore-based (Decision 2)
- `src/clients/producer/internals/produce_request_result.rs` — `watch::Sender` (Decision 4)
- `src/clients/producer/internals/future_record_metadata.rs` — `watch::Receiver` (Decision 4)
- `src/clients/producer/internals/incomplete_batches.rs` — `Mutex<HashSet<BatchId>>`

Modified: `src/common/kafka_error.rs` — add `BufferExhausted` variant

Java tests: `BufferPoolTest.java` (410 lines), `FutureRecordMetadataTest.java`

### Phase 6: ProducerBatch, BuiltInPartitioner, RecordAccumulator
**Depends on:** Phase 3, Phase 5  
**Complexity:** Very High

New files:
- `src/clients/producer/internals/producer_batch.rs` — batch lifecycle (try_append, done, complete, abort)
- `src/clients/producer/internals/built_in_partitioner.rs` — sticky partitioning with adaptive stats
- `src/clients/producer/internals/record_accumulator.rs` — `DashMap`-backed per-partition queues (Decision 8)
- `src/clients/producer/internals/producer_metadata.rs` — extends Metadata with topic expiry

Java tests: `ProducerBatchTest.java` (374 lines), `BuiltInPartitionerTest.java`, `RecordAccumulatorTest.java` (1892 lines), `ProducerMetadataTest.java`

### Phase 7: Sender + KafkaClient Async Conversion
**Depends on:** Phase 6, Phase 4  
**Complexity:** High

New files:
- `src/clients/producer/internals/sender.rs` — async run loop (Decision 3)

Modified (async conversion, Decision 9):
- `src/clients/kafka_client.rs` — make 5 methods async
- `src/clients/network_client.rs` — remove `block_on`/`noop_waker`, add `.await`
- All NetworkClient tests — already use `#[tokio::test]`, add `.await` calls

Java tests: `SenderTest.java` (non-transactional tests, ~40-50% of 4002 lines)

### Phase 8: KafkaProducer & Integration Tests
**Depends on:** Phase 7  
**Complexity:** High

New files:
- `src/clients/producer/producer.rs` — `Producer` trait (send, flush, close, partitions_for)
- `src/clients/producer/kafka_producer.rs` — main producer implementation
- `tests/integration/producer_test.rs` — produce to real Kafka 4.2.0 broker

Java tests: `KafkaProducerTest.java` (non-transactional tests, ~30-40% of 2952 lines)

Integration tests: produce single record, produce with key, produce multiple records (ordering), flush semantics, close semantics

## Dependency Graph

```
Phase 1 (Foundation Types)
    |
Phase 2 (Record Format)
    |
Phase 3 (Record Batch)
    |
    +--------+--------+
    |                 |
Phase 4 (API Types)   Phase 5 (Internals Foundation)
    |                 |
    +--------+--------+
             |
Phase 6 (RecordAccumulator)
             |
Phase 7 (Sender + Async KafkaClient)
             |
Phase 8 (KafkaProducer + Integration)
```

Phases 4 and 5 can be parallelized after Phase 3.

## New Dependencies (Cargo.toml)

```toml
flate2 = "1"        # gzip compression
snap = "1"          # snappy compression
lz4_flex = "0.11"   # lz4 compression
zstd = "0.13"       # zstd compression
crc32c = "0.6"      # CRC32C checksums (hardware-accelerated)
dashmap = "6"       # concurrent map for RecordAccumulator
```

## Execution Model

Each phase follows the Actor-Critic loop from `agent-roles.md`:
1. Actor agent N implements the phase, commits incrementally
2. Critic agent N reviews commits, writes issues to `COMMENTS.N.md`
3. Actor fixes issues, moves resolved comments to `COMMENTS.DONE.N.md`
4. Loop until no open comments remain

Design docs go in `design/history/Milestone-2/Phase-{N}/`.

## Verification

After each phase:
- `cargo build` succeeds
- `cargo test` passes
- `cargo xtask format-check` passes
- `cargo xtask lint` passes

After Phase 8 (end-to-end):
- Integration tests produce records to Kafka 4.2.0 in Docker
- Verify records arrive with correct key/value/headers/partition
- Verify batching behavior (multiple records in one ProduceRequest)
- Verify flush/close semantics

## Estimated File Count

~40 new files + ~8 modified files across 8 phases.
