# C FFI for the Rust Kafka Consumer (`src/ffi/consumer.rs`)

## Context

The repo already has a C FFI for the **producer** (`src/ffi/producer.rs`, ~3700
lines) exposing sync and async (callback-based) functions, with all
callbacks delivered from a single per-producer **dispatcher thread** draining a
**completion queue** (`CompletionJob = Box<dyn FnOnce()+Send>`). The Rust
**consumer** (`Consumer<K,V>` trait, `AsyncKafkaConsumer`, `MockConsumer`) is now
implemented but has **no C FFI**. This task adds `src/ffi/consumer.rs` exposing
the full implemented consumer surface to C, sync + async, reusing a shared
completion/dispatcher abstraction extracted out of `producer.rs`.

Key architectural difference from the producer: `KafkaProducer` is `Sync`, so the
producer FFI shares `&'static` refs into one submission task. The `Consumer`
trait is `Send + 'static` but **NOT `Sync`** and every blocking method takes
`&mut self`, so C threads cannot share a reference the way they share the
producer.

We expose the consumer through a **single-owner access guard** that mirrors
Java's `KafkaConsumer.acquire()/release()`: the handle owns the consumer directly
(behind an `UnsafeCell` + an `owner` atomic), and every FFI call must `acquire()`
before touching it. Concurrent access from a second thread — or, for the async
surface, a second operation while one is already in flight — **fails fast with a
`ConcurrentModificationError`**, exactly as Java throws
`ConcurrentModificationException` ("KafkaConsumer is not safe for multi-threaded
access"). `wakeup()` is the one method that bypasses the guard, matching Java.

(An earlier revision of this plan used an **actor model** — a single tokio task
owning the consumer and processing commands serially off a channel. That was
correct and memory-safe, but it *silently serialized* concurrent C access
instead of rejecting it, diverging from Java's fail-fast contract. The guard
replaces it. The Rust `AsyncKafkaConsumer` deliberately did not translate Java's
guard because `&mut self` makes exclusivity a compile-time guarantee — but that
guarantee does not cross the `*mut` FFI handle, which is precisely the gap the
guard closes.)

## Decisions locked in (from clarification)

1. **Method surface**: the *full implemented* public `Consumer` API — **except**
   listener/callback-taking variants (see #4).
2. **MockConsumer**: included, with driver methods exposed as FFI functions
   (mirrors producer's `MockProducer_*`), so C Unity tests run broker-less.
3. **Shared machinery**: extract the completion-queue + dispatcher-thread +
   error-handle code out of `producer.rs` into a new `src/ffi/common.rs`;
   refactor producer to use it; consumer reuses it.
4. **No listener bridging this pass**: skip `subscribe_with_listener`,
   `subscribe_pattern_with_listener`, `commit_async_with_callback`,
   `commit_async_offsets_with_callback`, and any `ConsumerRebalanceListener` /
   `OffsetCommitCallback` C adapters. Expose only listener-less variants
   (`subscribe`, `subscribe_pattern`, `commit_async`). Because no C
   listener/callback is ever invoked while a guarded call holds the consumer,
   **no guarded call can re-enter** — which is what lets the access guard be a
   simple *non-reentrant* single-owner guard (see #6) instead of Java's reentrant
   `currentThread`+`refcount`.
5. **Delivery**: Rust FFI + cbindgen header exports + Unity C tests (no Python).
6. **Concurrency model — access guard, not an actor**: the consumer is owned
   directly by the handle behind a non-reentrant single-owner guard
   (`owner: AtomicU64`). Concurrent access fails fast with
   `ConcurrentModificationError` (Java's `acquire()/release()` contract) rather
   than being silently serialized by an actor task. The async surface is kept,
   in a **one-operation-in-flight** form: an async op holds the guard from
   submission until its completion callback fires, so any concurrent op (sync or
   async, same or other thread) is rejected until it completes.

## Prerequisite gap: byte-array `Deserializer<Vec<u8>>`

`src/common/serialization/` has `byte_array_serializer.rs` but **no** byte-array
deserializer (only the `Deserializer` trait + test doubles).
`new_consumer::<Vec<u8>,Vec<u8>>` needs two `Box<dyn Deserializer<Vec<u8>>>`.

Add a production **`ByteArrayDeserializer`** (translation of Apache Kafka's
`org.apache.kafka.common.serialization.ByteArrayDeserializer`) in
`src/common/serialization/byte_array_deserializer.rs`, `impl Deserializer<Vec<u8>>`
returning `data.to_vec()`, re-exported from `serialization/mod.rs` (and the
`common` re-export used by the producer FFI). Apache-2.0 header, Confluent
copyright. This belongs in `common` regardless.

## Prerequisite gap: `KafkaError::ConcurrentModification`

The access guard returns a `ConcurrentModificationError` on concurrent access,
but `KafkaError` (`src/common/kafka_error.rs`) currently has no such variant —
only `IllegalState(String)`. Java's `ConcurrentModificationException` is a JDK
runtime exception (not a `KafkaException`); model it exactly like `IllegalState`
(CLAUDE.md naming: `…Exception` → `…Error`):

  - Add variant `ConcurrentModification(String)` + constructor
    `pub fn concurrent_modification(message: impl Into<String>)`.
  - Wire it into the same match arms as `IllegalState`: `message()`,
    `is_retriable()` → `false`, `is_fatal()` → `false`, `kafka_error()` → `None`,
    `Display`, and the FFI error-code mapping used by
    `kafka_common_KafkaError_code` (mirror `IllegalState`'s code).

This is a faithful addition of a Java consumer-contract error, not a new
abstraction. Together with `ByteArrayDeserializer` these are the only non-FFI
source additions.

---

## Phase A — Extract shared FFI machinery → `src/ffi/common.rs`

Create `src/ffi/common.rs` and **move** out of `producer.rs` (cut, not copy):

1. Error handle: opaque `kafka_common_KafkaError_t`, `KafkaErrorInner`,
   `box_error`, `error_ref`, and exported `kafka_common_KafkaError_{code,message,
   is_retriable,is_fatal,destroy}`. (`kafka_common_*` is shared verbatim — a
   second definition would make cbindgen emit a duplicate type.) `pub(crate)` for
   Rust helpers; keep `extern "C"` fns + the `_t` type `pub`.
2. `pub(crate) type CompletionJob = Box<dyn FnOnce() + Send>;`
3. Generic dispatcher spawn:
   `pub(crate) fn spawn_dispatcher(name: &str) -> (std::sync::mpsc::Sender<CompletionJob>, std::thread::JoinHandle<()>)`
   (loop body `while let Ok(job) = rx.recv() { job(); }`).
4. `pub(crate) type OperationCallbackFn`, `OperationCompletion`,
   `OperationCallbackTarget` (+ their `unsafe impl Send`) — reused for
   void-returning consumer ops.
5. `pub(crate) fn enqueue_or_run_inline(tx, job)` — the "if dispatcher gone, run
   inline" pattern.
6. `pub(crate) fn init_default_logger()` (move; identical).

Refactor `producer.rs`: delete moved defs, `use crate::ffi::common::{...}`,
`build_producer_handle` calls `common::spawn_dispatcher("kafka-producer-callback-dispatcher")`.
Producer-specific record/future/metadata types stay in `producer.rs`.

`src/ffi/mod.rs`:
```rust
pub(crate) mod common;
pub(crate) mod producer;
pub(crate) mod consumer;
```

**Acceptance**: `cargo build --features ffi` + existing producer Unity C tests
pass unchanged (pure refactor); cbindgen emits exactly one `kafka_common_KafkaError_t`.

## Phase B — Consumer handle + access guard + lifecycle

Opaque types (`#[repr(C)] { _private: [u8;0] }`): `kafka_consumer_Consumer_t`,
`kafka_consumer_ConsumerProperties_t`, and the marshaling handles from Phase E.

The handle owns the consumer **directly** (no actor task, no command channel) and
guards access with a non-reentrant single-owner atomic:

```rust
const NO_OWNER: u64 = u64::MAX;            // free sentinel

enum ConsumerKind {
    Async(Box<dyn Consumer<Vec<u8>, Vec<u8>>>),
    Mock(Box<MockConsumer<Vec<u8>, Vec<u8>>>),
}

struct ConsumerHandle {
    consumer: UnsafeCell<ConsumerKind>,      // exclusive access enforced by `owner`
    owner: AtomicU64,                        // NO_OWNER, or the OS-thread id holding it
    runtime: tokio::runtime::Runtime,        // drives app-side async methods (block_on)
    runtime_handle: tokio::runtime::Handle,  // async-variant awaiter spawns
    completion_tx: std::sync::mpsc::Sender<CompletionJob>,  // reused from common.rs
    dispatcher: Mutex<Option<std::thread::JoinHandle<()>>>,
    wakeup_handle: WakeupHandle,             // captured at construction
    is_mock: bool,
}
// SAFETY: `acquire()` guarantees at most one thread/future accesses
// `*consumer.get()` at any instant, and `ConsumerKind: Send`, so exclusive
// cross-thread access is sound. `UnsafeCell` is needed to hand out `&mut` from
// a shared `&ConsumerHandle`.
unsafe impl Send for ConsumerHandle {}
unsafe impl Sync for ConsumerHandle {}
```

There is **no `ConsumerCommand` enum and no `consumer_actor` task** — FFI methods
call the consumer in place under the guard. Mock-only driver methods
(`add_record`, `update_*_offsets`, `update_partitions`, `set_poll_exception`,
`set_offsets_exception`, `set_max_poll_records`, `rebalance`, `closed`,
`should_rebalance`/reset) are inherent methods on `MockConsumer`; the FFI matches
on `ConsumerKind::Mock` and returns `KafkaError::illegal_state(...)` for the
`Async` arm.

### Access guard — single-owner, non-reentrant

```rust
fn acquire(h: &ConsumerHandle) -> Result<(), KafkaError> {
    let tid = current_os_thread_id();        // stable per-thread u64
    match h.owner.compare_exchange(NO_OWNER, tid, AcqRel, Acquire) {
        Ok(_)  => Ok(()),
        Err(_) => Err(KafkaError::concurrent_modification(
            "KafkaConsumer is not safe for multi-threaded access.")),
    }
}
fn release(h: &ConsumerHandle) { h.owner.store(NO_OWNER, Release); }
```

**Why non-reentrant (deviation from Java's reentrant `currentThread`+`refcount`):**
Java's guard is reentrant so a rebalance listener running on the poll thread may
call back into the consumer. The FFI bridges **no** listeners/callbacks into Rust
(decision #4), so no guarded call ever re-enters — reentrancy is unnecessary. It
is also actively *unsafe* for the async one-in-flight model: a detached async
future keeps the consumer busy past the C-function return, so even the submitting
thread must be blocked from starting a second op to avoid aliasing `&mut`. A
strict single-owner guard delivers both the one-in-flight rejection and the
multi-thread rejection with the same error.

`build_consumer_handle(kind, wakeup_handle, is_mock)`: build a runtime,
`common::spawn_dispatcher("kafka-consumer-callback-dispatcher")`, store `kind` in
the `UnsafeCell`, init `owner = NO_OWNER`, box & leak the handle.

### Sync FFI dispatch

```rust
acquire(h)?;                                 // -> ConcurrentModificationError on conflict
let _g = ReleaseGuard(h);                    // RAII: release on return/panic (Java's finally)
let r = h.runtime.block_on(unsafe { (*h.consumer.get()).poll(timeout) });
// _g drops -> release; spawns nothing per call
```

### Async FFI dispatch (one-in-flight)

```rust
acquire(h)?;                                 // held across the whole submit->callback window
let hs: &'static ConsumerHandle = h;         // handle is leaked (Send+Sync); future stays Send
h.runtime_handle.spawn(async move {
    let r = unsafe { (*hs.consumer.get()).poll(timeout).await };
    let job: CompletionJob = Box::new(move || { fire_c_callback(r); release(hs); });
    common::enqueue_or_run_inline(hs.completion_tx.clone(), job);  // callback on dispatcher thread
});
// NOTE: no release here — release runs inside the completion job, so `owner`
// stays held (and rejects any concurrent op) until the callback fires.
```

Capture the `&'static ConsumerHandle` (Send+Sync via the unsafe impls), **not** a
bare `*mut` (raw pointers are `!Send` and would make the spawned future
non-`Send`).

**Both poll variants are first-class.** Async `poll` exists for callers that
cannot block; for a **zero-overhead poll loop**, use **sync poll** — it spawns
nothing per iteration (`block_on` drives the future in place; the consumer's own
bg task is spawned once at construction). Async `poll`'s one task spawn per call
is per-RPC (batch) granularity and acceptable under CLAUDE.md §11; it is **not**
to be "optimized" with a resident driver task (routing the future through a
channel to a resident task costs a comparable `Box` + channel push and just
reintroduces the deleted long-lived task). A librdkafka-style continuous
background-poll + ready-queue model is rejected as it diverges from Java's
user-driven poll semantics (`max.poll.interval.ms`, pause/resume).

### `kafka_consumer_Consumer_destroy`

No actor/channel to shut down. `Box::from_raw`, then **drop/shutdown the
`runtime` first** (cancels any in-flight async future that borrows the consumer),
**then** drop the consumer (its own `Drop` joins the internal bg task), drop
`completion_tx`, and **detach** the dispatcher (do NOT join — outstanding
completion jobs may hold a cloned `completion_tx`). Destroy does not `acquire()`
(mirrors Java); calling it concurrently with an in-flight op is the standard C
lifetime precondition (CLAUDE.md FFI §3 "don't check failing preconditions"),
identical to the producer's existing exposure.

## Phase C — Config + constructors

Mirror producer's `ProducerProperties`: `kafka_consumer_ConsumerProperties_{new,
from_configs,put,destroy}` over `Box<HashMap<String,String>>`.
- `kafka_consumer_KafkaConsumer_new(props, out_error)`: `ConsumerConfig::from_properties`
  → `new_consumer::<Vec<u8>,Vec<u8>>(config, Box::new(ByteArrayDeserializer),
  Box::new(ByteArrayDeserializer))` → capture `wakeup_handle()` →
  `build_consumer_handle(Async, .., false)`. Classic protocol → `unsupported_version`
  error (Java parity).
- `kafka_consumer_MockConsumer_new(auto_offset_reset: *const c_char)`:
  `AutoOffsetResetStrategy::from_string` (default LATEST), `MockConsumer::new`,
  `build_consumer_handle(Mock, .., true)`.

## Phase D — Method groups (sync + async signatures)

Naming per CLAUDE.md §3: `kafka_consumer_Consumer_<method>` (sync) and
`kafka_consumer_Consumer_<method>_async` + `kafka_consumer_Consumer_<method>_callback_t`.
Fallible sync fns return `*mut kafka_common_KafkaError_t` (null=success) or write
`*out_error` and return data (producer convention).

- **Void ops** (sync + async via `OperationCallbackFn`): `subscribe`,
  `subscribe_pattern`, `assign`, `unsubscribe`, `seek`, `seek_with_metadata`,
  `seek_to_beginning`, `seek_to_end`, `pause`, `resume`, `enforce_rebalance`,
  `commit_sync`(+`_timeout`/`_offsets`/`_offsets_timeout`), `commit_async`,
  `close`, `close_with_options`. Topics: `const char* const*` + count → `Vec<String>`.
- **poll** (sync returns `kafka_consumer_ConsumerRecords_t*` + out_error; async
  callback `(records, error, user_data)`). `timeout_ms` → `Duration::from_millis`.
- **scalar/map/list returns** (sync + typed async callbacks): `position`(`i64`),
  `committed`→`OffsetMap_t*`, `offsets_for_times`→`OffsetAndTimestampMap_t*`,
  `beginning_offsets`/`end_offsets`→`LongOffsetMap_t*`, `partitions_for`→
  `PartitionInfoList_t*`, `list_topics`→`TopicPartitionInfoMap_t*` (+ `_timeout` variants).
- **Sync state reads** (sync only, under the access guard): `assignment`,
  `subscription`, `paused`, `group_metadata`, `client_id` (owned `char*` freed by
  `kafka_consumer_string_destroy`), `current_lag` (out-bool present + `i64`).
  These `acquire()`/`release()` like any sync call.
- **wakeup** (sync, **bypasses the guard**): calls `handle.wakeup_handle.wakeup()`
  directly so it interrupts an in-flight `poll().await` (parked in `block_on` or
  on the runtime) held by another thread. Must NOT `acquire()` (that is its whole
  purpose — Java's `wakeup()` likewise does not acquire).

## Phase E — Data marshaling (opaque handles + accessors, zero-copy poll §27)

`ConsumerRecords` handle owns the polled batch:
```rust
struct ConsumerRecordsInner {
    records: ConsumerRecords<Vec<u8>, Vec<u8>>,            // owns buffers
    flat: Vec<*const ConsumerRecord<Vec<u8>, Vec<u8>>>,    // index→record (insertion order)
}
```
Accessors: `count`, `is_empty`, `get(i) -> *const kafka_consumer_ConsumerRecord_t`
(borrowed, valid until records handle destroyed), `destroy`.

`ConsumerRecord` = **opaque handle + getters** (NOT a flat `#[repr(C)]` struct —
key/value are variable-length, getters give true zero-copy ptr+len borrowing the
batch): `partition`, `offset`, `timestamp`, `timestamp_type`(i32),
`serialized_key_size`, `serialized_value_size`, `key`/`value`(`*const u8` +
`*out_len`, null/-1 if None), `leader_epoch`/`delivery_count`(out-bool present),
`topic`/header keys returned as ptr+len (avoid per-record `CString` on hot path;
document non-NUL-terminated), `header_count`/`header_key(i)`/`header_value(i,*out_len)`.

Single-value opaque handles + getters + `_destroy`: `TopicPartition`,
`OffsetAndMetadata`, `OffsetAndTimestamp`, `ConsumerGroupMetadata`,
`PartitionInfo` (+ nested `kafka_common_Node_t` indexed getters for
leader/replicas/isr/offline). Non-hot-path strings returned as cached `CString`.

Map/list result handles own the map/`Vec` + a key `Vec` for stable indexing,
each with `count`, indexed `get_*` returning borrowed sub-handles, `_destroy`:
`OffsetMap`, `OffsetAndTimestampMap`, `LongOffsetMap` (i64 value getter),
`TopicPartitionInfoMap` (list_topics), `PartitionInfoList`, plus
`TopicPartitionList`/`StringList` for `assignment`/`subscription`/`paused`.

Input marshaling (C → Rust): arrays of `(topic, partition)` → `Vec<TopicPartition>`;
`commit_sync_offsets`/`seek_with_metadata` take parallel arrays of tp + offset
(+ optional metadata/leader_epoch); `offsets_for_times` takes tp + timestamp arrays.

## Phase F — cbindgen exports

Append every new `pub` `_t` type and every `*_callback_t` typedef to
`cbindgen.toml` `[export].include` (Consumer, ConsumerProperties, ConsumerRecords,
ConsumerRecord, TopicPartition[List], StringList, OffsetAndMetadata,
OffsetAndTimestamp, ConsumerGroupMetadata, PartitionInfo[List], Node, the four
map handles, and all callback typedefs). `kafka_common_KafkaError_t` already
present (from producer, now in common.rs) — do not duplicate. Header regenerates
via the existing `build.rs` ffi step; verify `confluent_kafka.h` compiles.

## Phase G — Unity C tests + CMake

`bindings/c/tests/test_mock_consumer.c` (primary, broker-less): MockConsumer_new
→ assign → mock add_record/update_*_offsets → sync `poll` iterate
`ConsumerRecords` and assert topic/partition/offset/key/value bytes → async
`poll` (pump until callback fires, mirror producer async test) → `wakeup`
interrupts a long poll → commit_sync/committed round-trip → seek/position →
beginning/end_offsets → pause/resume → assignment/subscription/paused →
group_metadata → set_poll_exception error path → close → destroy.

Concurrency guard tests (the new contract): (a) two threads calling a guarded
method concurrently — one succeeds, the other gets a `kafka_common_KafkaError_t`
whose code maps to `ConcurrentModification`; (b) an async op outstanding + a
second call (any thread) → `ConcurrentModificationError` until the callback
fires; (c) `wakeup()` from another thread still interrupts an in-flight `poll`
(not rejected by the guard); (d) single-threaded sequential use (sync + async)
passes unchanged.

`bindings/c/tests/test_kafka_consumer.c` (mirrors `test_kafka_producer.c`):
properties build + `KafkaConsumer_new` + `subscribe` + `wakeup`-interrupted
`poll` returns cleanly; classic-protocol config asserts `unsupported_version`.

`bindings/c/CMakeLists.txt`: add `add_executable`/`target_link_libraries`
(`unity::framework ${CONFLUENT_KAFKA_RUST} ${SYSTEM_LIBS}`)/`add_test` blocks for
both, after the producer blocks.

## Methods deliberately not exposed (with rationale)

- `subscribe_with_listener`, `subscribe_pattern_with_listener`,
  `commit_async_with_callback`, `commit_async_offsets_with_callback`, and
  `ConsumerRebalanceListener`/`OffsetCommitCallback` adapters — **deferred**
  (decision #4): keeps the access guard re-entry-free (no C callback runs while
  a guarded call holds the consumer); listener-less variants cover the core
  consume loop.
- `wakeup_handle()` — no separate C fn; `Consumer_wakeup` uses the stored handle
  (the `Consumer_t*` is itself shareable for cross-thread wakeup).
- `MockConsumer::schedule_poll_task` — takes a Rust closure; no C analog. Mock
  coverage uses `add_record`/`set_poll_exception` instead.
- `client_id()` returns owned `char*` (the borrow cannot outlive the guarded
  call, so the value is copied out).

## Sequencing

A (extract + producer refactor, gated by producer tests) → ByteArrayDeserializer
+ `KafkaError::ConcurrentModification` gap fixes → B+C (handle/guard/lifecycle +
constructors, minimal MockConsumer_new + poll + destroy end-to-end) → E (records
marshaling, makes poll useful) → D (all method groups, sync then async) → F
(cbindgen, incremental + final pass) → G (mock test alongside D/E, kafka test
last).

## Verification

- `cargo build --features ffi` and `cargo test` (Rust + ByteArrayDeserializer unit test).
- `cargo xtask format-check` and `cargo xtask lint` (clippy warnings-as-errors).
- `make verify` (builds Rust w/ ffi → cbindgen header → C tests via CMake/CTest →
  runs `ctest --output-on-failure`); the new `mock_consumer` C test is the primary
  end-to-end gate. Producer C tests must still pass (Phase A refactor regression).

## Critical files

- Create: `src/ffi/common.rs`, `src/ffi/consumer.rs`,
  `src/common/serialization/byte_array_deserializer.rs`,
  `bindings/c/tests/test_mock_consumer.c`, `bindings/c/tests/test_kafka_consumer.c`
- Edit: `src/ffi/producer.rs`, `src/ffi/mod.rs`,
  `src/common/serialization/mod.rs`, `src/common/kafka_error.rs`
  (add `ConcurrentModification` variant), `cbindgen.toml`,
  `bindings/c/CMakeLists.txt`
- Reference (read, don't change): `src/consumer/mod.rs`,
  `src/consumer/async_kafka_consumer.rs`, `src/consumer/mock_consumer.rs`,
  `src/consumer/consumer_record.rs`, `consumer_records.rs`,
  `.claude/rules/consumer-threading.md` (§§1,2,11,16,27,31)
