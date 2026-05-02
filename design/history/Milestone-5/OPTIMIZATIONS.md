# CPU Performance Optimization Plan for Kafka Rust Producer

## Context

The Rust Kafka producer uses ~2x more CPU than librdkafka. Profiling identified 7 bottleneck categories in the hot path. Additionally, a bug causes "The version of API is not supported." errors against Kafka 4.0.0 brokers — the `build_version()` error path masks the real error behind a generic `Errors::UnsupportedVersion`.

This plan addresses both the performance gap and the bug, phased so each change is independently verifiable.

---

## Phase 0: Fix UnsupportedVersion error masking (correctness)

**Problem:** When `build_version()` fails in `network_client.rs:481` (e.g., `validate_records` returns error for empty records), the original `io::Error` is discarded (`Err(_e) =>`) and replaced with a generic `"UnsupportedVersionError"` string. The user sees "The version of API is not supported." with no clue about the actual cause.

**Changes:**
- `src/network_client.rs:481-498` — Change `Err(_e)` to `Err(e)`, include `e` in the `warn!` log and in the `version_mismatch` string (e.g., `format!("UnsupportedVersionError: {e}")`)
- `src/producer/internals/sender.rs:526-536` — When `response.version_mismatch().is_some()`, include the mismatch message in the `PartitionResponse` error so the user sees the real cause

**Risk:** Low. Improves error messages only.

---

## Phase 1: Synchronous write path — eliminate Pin<Box<dyn Future>> (HIGH impact, ~15-25%)

**Problem:** Every write attempt allocates 3 `Box::pin(...)` futures: `NetworkSend::write_to` → `ByteBufferSend::write_to` → `PlaintextTransportLayer::write_vectored`. At thousands of writes/sec, this dominates allocation pressure.

**Key insight:** `PlaintextTransportLayer::write_vectored` does `stream.writable().await; stream.try_write_vectored(srcs)`. The selector already wraps this in `Duration::ZERO` timeout, so the await never actually suspends — the socket is either ready or not. We can call `try_write_vectored` directly without awaiting readiness.

**Changes:**
1. `src/common/network/transport_layer.rs` — Add `fn try_write_vectored(&mut self, srcs: &[IoSlice]) -> io::Result<usize>` with a default impl that returns `WouldBlock`
2. `src/common/network/plaintext_transport_layer.rs` — Implement `try_write_vectored`: call `self.stream_mut()?.try_write_vectored(srcs)` directly (no async)
3. `src/common/network/ssl_transport_layer.rs` — Implement `try_write_vectored`: for SSL, use `tls_stream.get_ref().0.try_write_vectored(srcs)` if possible, or fallback to `WouldBlock` (SSL may need the async path; can be optimized later)
4. `src/common/network/send.rs` — Add `fn try_write_to(&mut self, channel: &mut dyn TransportLayer) -> io::Result<usize>` with a default impl calling the async version via block_on (backward compat)
5. `src/common/network/byte_buffer_send.rs` — Implement `try_write_to` synchronously: build IoSlice array, call `channel.try_write_vectored()`, advance offsets. No Future/Box/Pin.
6. `src/common/network/network_send.rs` — Implement `try_write_to` delegating to inner send
7. `src/common/network/kafka_channel.rs` — Change `write()` to call `try_write_to` instead of `write_to().await`
8. `src/common/network/selector.rs` — In `write_channel()`, remove `tokio::time::timeout(Duration::ZERO, ...)` wrapper since write is now natively non-blocking
9. Update mock `TransportLayer` implementations in test modules

**Risk:** Medium. Core I/O path change. Must verify all network tests pass.

---

## Phase 2: Selector poll loop optimizations (HIGH impact, ~10-15%)

**Problem:** Poll loop at `selector.rs:743` clones all channel IDs (`Vec<String>`) on every iteration. `write_channel()` only writes once per channel per poll.

**Changes:**
- `selector.rs:743` — Replace `self.channels.keys().cloned().collect()` with iteration over `self.channels` by reference (use `IndexMap` which is already a dependency, or collect IDs into a persistent cache)
- `write_channel()` — Loop calling `try_write_to` until send completes or `WouldBlock`, matching Java's behavior of draining the write buffer in one pass

**Depends on:** Phase 1 (uses synchronous `try_write_to`)

**Risk:** Medium. Must verify no infinite loop on partial writes.

---

## Phase 3: Stack-allocate IoSlice array (MEDIUM-HIGH impact, ~3-5%)

**Problem:** `byte_buffer_send.rs:87-92` creates `Vec<IoSlice>` on every write. Typical buffer count is 2-4.

**Changes:**
- `src/common/network/byte_buffer_send.rs` — Replace `Vec<IoSlice>` with a fixed-size stack array `[IoSlice; 8]` initialized with `IoSlice::new(&[])`. Count active slices and pass `&slices[..count]`.

**Risk:** Very low. No API change.

---

## Phase 4: AtomicI64 for `next_batch_expiry_time_ms` (MEDIUM impact, ~2-3%)

**Problem:** `record_accumulator.rs:159` uses `Mutex<i64>` for a single timestamp, acquired 3x per produce cycle.

**Changes:**
- `src/producer/internals/record_accumulator.rs` — Replace `Mutex<i64>` with `AtomicI64`. Use `store(i64::MAX, Release)` for reset, `fetch_min(val, AcqRel)` for update, `load(Acquire)` for read.

**Risk:** Very low. Semantic equivalent.

---

## Phase 5: TopicPartition Arc\<str\> (MEDIUM impact, ~3-5%)

**Problem:** `topic_partition.rs:23` has `topic: String`. Every `.clone()` heap-allocates. ~10-15 clones per batch in sender/accumulator hot paths.

**Changes:**
- `src/common/topic_partition.rs` — Change `topic: String` to `topic: Arc<str>`. Update `new()` to accept `impl Into<Arc<str>>`. `topic()` still returns `&str` via `Deref`.
- Update callers across `sender.rs`, `record_accumulator.rs`, `kafka_producer.rs`, and test files that construct `TopicPartition::new(topic.to_string(), ...)`.

**Risk:** Low. Public API return type (`&str`) unchanged. Internal optimization.

---

## Phase 6: Move sender-only fields out of Mutex (MEDIUM impact, ~3-5%)

**Problem:** `muted`, `nodes_drain_index` in `record_accumulator.rs` are behind `Mutex` but only accessed from the sender thread. 50-200 lock acquisitions per cycle.

**Changes:**
- `src/producer/internals/sender.rs` — Add `muted: HashSet<TopicPartition>` and `nodes_drain_index: HashMap<String, usize>` as fields on `Sender`
- `src/producer/internals/record_accumulator.rs` — Change `ready()` and `drain()` signatures to accept `&HashSet<TopicPartition>` for muted and `&mut HashMap<String, usize>` for drain index. Remove `Mutex` wrappers for these fields. Keep `mute_partition`/`unmute_partition` on accumulator but have them take `&mut HashSet<TopicPartition>` instead of using internal state.

**Depends on:** Phase 5 (TopicPartition in HashSet changes)

**Risk:** Medium. Signature changes on `ready()`/`drain()` require updating all callers and tests.

---

## Phase 7: Varint single-write optimization (LOW-MEDIUM impact, ~1-3%)

**Problem:** `varint.rs:96-119` makes up to 5 `write_all(&[byte])` calls per varint. With 3000+ varints per request, this adds function call overhead.

**Changes:**
- `src/common/protocol/varint.rs` — Encode varint into a `[u8; 5]` stack buffer first, then call `write_all(&buf[..len])` once. Same for varlong with `[u8; 10]`.

**Risk:** Very low. Pure algorithmic improvement with existing tests.

---

## Verification

Each phase independently:
1. `cargo build` — compiles
2. `cargo test` — all unit tests pass
3. `make verify` — format, lint, integration tests, Python/C bindings all pass
4. Performance: Run `TEST_DURATION_SECONDS=10 CLIENT_VERSION=3 python3 bindings/python/test/performance/producer_performance_test.py` before/after to measure CPU improvement

## Execution Order

Phases 0, 3, 4, 5, 7 are independent — can be done in any order.
Phase 1 must precede Phase 2. Phase 5 should precede Phase 6.

Recommended sequence: **0 → 7 → 4 → 3 → 5 → 1 → 2 → 6** (easy wins first, then the big rewrites)
