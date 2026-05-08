# CPU Performance Optimization Plan for Kafka Rust Producer

## Context

The Rust Kafka producer uses ~2x more CPU than librdkafka. Flamegraph shows `Metadata::fetch` as the top CPU consumer — it deep-clones the entire `Cluster` struct (8+ HashMaps, Vecs of Nodes/PartitionInfo, multiple Strings) on every call. Java's `Metadata.fetch()` returns a reference (zero-cost). This is called on every poll cycle (`network_client.rs:947`), every `send_producer_data` (`sender.rs:345`), and every produce call (`kafka_producer.rs:643,692`).

Additional bottlenecks and an "UnsupportedVersion" error masking bug are also addressed.

---

## Phase 1: Wrap MetadataSnapshot in Arc (CRITICAL — flamegraph hotspot)

**Problem:** `Metadata::fetch()` at `src/metadata.rs:354-356` acquires a mutex and clones the entire `Cluster`. `fetch_metadata_snapshot()` at line 360-362 clones the entire `MetadataSnapshot` (which contains the `Cluster` plus more HashMaps). These are called on every sender loop iteration and every produce call.

Java returns a shared reference (`synchronized MetadataSnapshot fetch() { return metadataSnapshot; }`). Rust must use `Arc` to achieve the same zero-copy sharing.

**Changes:**
- `src/metadata.rs`:
  - Change `MetadataInner.metadata_snapshot` from `MetadataSnapshot` to `Arc<MetadataSnapshot>`
  - `fetch_metadata_snapshot()` → return `Arc<MetadataSnapshot>` (just `Arc::clone`, no deep copy)
  - `fetch()` → return `Arc<Cluster>` or access cluster through the `Arc<MetadataSnapshot>`
  - `update()` creates a new `Arc<MetadataSnapshot>` when metadata changes; old references remain valid
  - `MetadataSnapshot::empty()` → wrap in `Arc`

- `src/metadata_snapshot.rs`:
  - Wrap `cluster_instance: Cluster` in `Arc<Cluster>` inside `MetadataSnapshot` so both `fetch()` and `fetch_metadata_snapshot()` can return cheap Arc clones
  - Add `pub fn cluster_arc(&self) -> Arc<Cluster>` method

- Callers that need to change return type handling:
  - `src/network_client.rs:947` — `metadata.fetch().nodes().to_vec()` → use `Arc<Cluster>` ref
  - `src/network_client.rs:1104` — `let cluster = metadata.fetch()` → now `Arc<Cluster>`
  - `src/producer/internals/sender.rs:345` — `metadata.fetch_metadata_snapshot()` → `Arc<MetadataSnapshot>`
  - `src/producer/kafka_producer.rs:643,692` — `metadata.fetch()` → `Arc<Cluster>`
  - Various test files in `metadata.rs`, `sender.rs`, `kafka_producer.rs`

**Risk:** Low-Medium. The `MetadataSnapshot` and `Cluster` are already used as read-only after creation. `Arc` adds ~8 bytes overhead per reference but eliminates all deep cloning. Callers receive shared immutable references, matching Java semantics exactly.

**Expected impact:** Largest single improvement. Eliminates O(n) deep copies of all metadata structures on every poll cycle.

---

## Phase 2: Fix UnsupportedVersion error masking (correctness)

**Problem:** When `build_version()` fails in `network_client.rs:481`, the original `io::Error` is discarded (`Err(_e) =>`) and replaced with generic `"UnsupportedVersionError"`. The user sees "The version of API is not supported." with no hint about the real cause (e.g., empty records from a serialization bug).

**Changes:**
- `src/network_client.rs:481-498` — Change `Err(_e)` to `Err(e)`, include `e` in the `warn!` log and in the `version_mismatch` string
- `src/producer/internals/sender.rs:526-536` — Include the `version_mismatch()` message in the error so the user sees the root cause

**Risk:** Low.

---

## Phase 3: Synchronous write path — eliminate Pin<Box<dyn Future>> (HIGH impact)

**Problem:** Every write allocates 3 `Box::pin(...)` futures: `NetworkSend::write_to` → `ByteBufferSend::write_to` → `PlaintextTransportLayer::write_vectored`. The selector wraps writes in `Duration::ZERO` timeout, so the async never actually suspends.

**Changes:**
1. `src/common/network/transport_layer.rs` — Add `fn try_write_vectored(&mut self, srcs: &[IoSlice]) -> io::Result<usize>` with default `WouldBlock`
2. `src/common/network/plaintext_transport_layer.rs` — Implement: `self.stream_mut()?.try_write_vectored(srcs)` (no async)
3. `src/common/network/ssl_transport_layer.rs` — Implement or fallback to `WouldBlock`
4. `src/common/network/send.rs` — Add `fn try_write_to(&mut self, channel: &mut dyn TransportLayer) -> io::Result<usize>`
5. `src/common/network/byte_buffer_send.rs` — Implement `try_write_to` synchronously
6. `src/common/network/network_send.rs` — Delegate to inner send
7. `src/common/network/kafka_channel.rs` — Use `try_write_to` instead of `write_to().await`
8. `src/common/network/selector.rs` — Remove `tokio::time::timeout(Duration::ZERO, ...)` wrapper
9. Update mock `TransportLayer` in test modules

**Risk:** Medium. Core I/O path.

---

## Phase 4: Selector poll loop optimizations (HIGH impact)

**Problem:** Poll loop at `selector.rs:743` clones all channel IDs (`Vec<String>`) every iteration. `write_channel()` writes only once per channel per poll.

**Changes:**
- Replace `keys().cloned().collect()` with `IndexMap` iteration (already a dependency)
- Loop `try_write_to` until send completes or `WouldBlock`

**Depends on:** Phase 3

**Risk:** Medium.

---

## Phase 5: Stack-allocate IoSlice array (MEDIUM impact)

**Problem:** `byte_buffer_send.rs:87-92` heap-allocates `Vec<IoSlice>` per write. Typical count is 2-4.

**Changes:**
- Replace with `[IoSlice::new(&[]); 8]` stack array, pass `&slices[..count]`

**Risk:** Very low.

---

## Phase 6: AtomicI64 for `next_batch_expiry_time_ms` (MEDIUM impact)

**Problem:** `record_accumulator.rs:159` uses `Mutex<i64>`, acquired 3x per cycle.

**Changes:**
- Replace with `AtomicI64`. Use `fetch_min` for update, `store`/`load` for reset/read.

**Risk:** Very low.

---

## Phase 7: TopicPartition Arc\<str\> (MEDIUM impact)

**Problem:** `topic: String` causes heap alloc per clone. ~10-15 clones per batch.

**Changes:**
- `src/common/topic_partition.rs` — Change to `topic: Arc<str>`. `topic()` still returns `&str`.

**Risk:** Low.

---

## Phase 8: Move sender-only fields out of Mutex (MEDIUM impact)

**Problem:** `muted`, `nodes_drain_index` behind `Mutex` but single-threaded access. 50-200 lock acquisitions per cycle.

**Changes:**
- Move to `Sender` struct, pass as params to `ready()`/`drain()`

**Depends on:** Phase 7

**Risk:** Medium.

---

## Phase 9: Varint single-write optimization (LOW impact)

**Problem:** Up to 5 `write_all(&[byte])` calls per varint.

**Changes:**
- Encode into `[u8; 5]` stack buffer, single `write_all`.

**Risk:** Very low.

---

## Verification

Each phase independently:
1. `cargo build`
2. `cargo test`
3. `make verify` — format, lint, all tests including Python/C bindings
4. Performance: `TEST_DURATION_SECONDS=10 CLIENT_VERSION=3 python3 bindings/python/test/performance/producer_performance_test.py`

## Execution Order

**Phase 1 → 2 → 9 → 6 → 5 → 7 → 3 → 4 → 8**

Phase 1 (Arc metadata) first — it's the flamegraph hotspot and likely the single biggest win. Then bug fix (Phase 2), then easy wins (9, 6, 5, 7), then the I/O path rewrite (3 → 4), then mutex removal (8).
