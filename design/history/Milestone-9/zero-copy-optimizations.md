# Plan: Remove avoidable copies/allocations on the consumer poll path

## Context
Profiling the sync v3 consumer (Python perf test, all-thread on-CPU `perf` capture at
10k msg/s) showed the Rust client's own CPU (~20% of total) is dominated by **memory
traffic on the receive/decode path**, not by useful decode work:

- `__memmove` ~17%, `__memset` ~4%, `malloc`/`free` ~8% of total CPU.
- Each record's value/key bytes are **copied twice**: (1) the whole `records` payload is
  copied out of the socket buffer during FetchResponse parsing, then (2) each record's
  key/value is copied again by `ByteArrayDeserializer::deserialize` (`data.to_vec()`).
- The wire decoder additionally does redundant work: every `bytes`/`string` field is
  `let mut b = vec![0u8; n]` (alloc + **zero-fill**) then `read_bytes` which internally
  `read_array`s (alloc + zero-fill + copy) then copies again — **2 allocs + 2 memsets +
  2 copies per field**.

Goal: make the receive path zero-copy per `consumer-threading.md` §27 (one buffer owns the
fetched bytes; everything downstream borrows/refcounts), eliminate the redundant
decoder allocations, and clean up smaller per-poll costs — verified by re-running the
perf flamegraph and confirming `memmove`/`memset`/alloc drop.

## Key code (verified)
- Decoder: `src/common/protocol/readable.rs` (`read_array:43`, default `read_bytes:64`,
  `read_string:58`); `src/common/protocol/byte_buffer_accessor.rs`
  (`struct:27` buffer `Vec<u8>`, `read_array:175` `vec![0u8;n]`+copy, `Writable` impl ~209
  — this struct is used for BOTH read and write, so the read path needs a separate
  Bytes-backed accessor).
- Generator templates emitting the decode: `generator/src/lib.rs` — bytes fields ~3418-3492,
  string ~3299-3416, array-of-string ~3646-3704, tagged `read_array` site ~3020, plus
  the string default-impl branches ~2045-2069.
- Receive→parse: `src/common/network/network_receive.rs`
  (`into_source_and_payload:128` already moves the `Vec<u8>`, no copy);
  `src/network_client.rs` (~308 `ByteBufferAccessor::from_bytes(payload)` → `parse_response`).
- Consumer collect: `src/consumer/internals/fetch_collector.rs`
  (`collect_fetch:194`, per-poll `IndexMap<TopicPartition, Vec<ConsumerRecord>>:195`,
  locks taken per-batch not per-record — good).
- Record emit: `src/consumer/internals/completed_fetch.rs`
  (`ensure_cursor:380` already *moves* records into `MemoryRecords`,
  `fetch_records:410`, deserialize calls `:497-507`, `headers_owned:496`);
  `src/common/record/memory_records.rs` (`buffer: Vec<u8>:44`);
  `src/common/record/default_record.rs` (`read_ref_from_buffer:292`, `parse_body:467`
  borrows key/value as `&[u8]` — already zero-copy; `headers:648` allocs only on access).
- Deserializer copy: `src/common/serialization/byte_array_deserializer.rs:46` (`data.to_vec()`).
- FFI: `src/ffi/consumer.rs` (`type Bytes = Vec<u8>:70`, build `:421-424`,
  `box_records:715`); `bindings/python/_confluentkafka.c`
  (`borrowed_memoryview:1028`, `ConsumerRecord_get_value:1095` — Python side already
  zero-copy via memoryview over the owning batch).
- Secondary: `src/common/protocol/varint.rs:34` (byte-at-a-time);
  `src/consumer/internals/subscription_state.rs:1434` `move_partition_to_end` →
  `src/common/internals/partition_states.rs:96` `move_to_end` (IndexMap O(N) shift;
  error/cleanup path, not steady-state).

## Execution mode (confirmed with user)
Do **all three phases**, but **one phase at a time**: implement a phase, re-run the perf
flamegraph + relevant tests, report the before/after delta, then proceed to the next.
Stop and surface if a phase regresses correctness or doesn't move the expected metric.

## Approach — phased (each phase independently shippable + verifiable)

### Phase 0 — Decoder redundancy (quick, low risk, keeps `Vec<u8>`)
Removes the ~4% `memset` and the redundant alloc + second copy, no type/API changes.
1. `byte_buffer_accessor.rs`: rewrite `read_array` to `self.buffer[pos..pos+n].to_vec()`
   (one alloc + one copy, **no zero-fill**); override `read_bytes` to copy straight from
   `self.buffer` into the caller slice (no temp `Vec`).
2. `generator/src/lib.rs`: change the `bytes`/`string`/array-string decode templates from
   `let mut b = vec![0u8;n]; read_bytes(&mut b)` to `let b = readable.read_array(n)?;`
   (for strings, `String::from_utf8(b)`). Regenerate via `cargo build`.

### Phase 1 — Zero-copy receive path via `bytes::Bytes` (the big win)
Eliminates the remaining records-payload copy (~7%) AND the per-record deserializer copy
(~2.9%) by making all record bytes refcounted slices of ONE buffer per fetch.
1. Add `bytes = "1"` as a direct dependency (already transitively present).
2. Introduce a read-only `Bytes`-backed reader (new `BytesReader` implementing `Readable`,
   or a `Readable for Bytes`-cursor) — do NOT change the read/write `ByteBufferAccessor`.
   Add `read_bytes_owned(&mut self, n) -> Bytes` returning `self.buf.slice(pos..pos+n)`
   (O(1) refcount, zero copy). Network parse converts the moved `Vec<u8>` payload with
   `Bytes::from(vec)` (O(1)) and parses through this reader.
3. Generator: emit `read_bytes_owned` for `bytes` fields and change the `records` field
   type (`FetchResponseData` `PartitionData.records`) from `Option<Vec<u8>>` to
   `Option<Bytes>`. Scope the Bytes field-type change to the fetch/records path; other
   responses keep `Vec<u8>` to bound blast radius.
4. `MemoryRecords.buffer: Vec<u8>` → `Bytes`; `CompletedFetch::ensure_cursor` moves the
   `Bytes` in (still no copy). `DefaultRecordRef` keeps borrowing `&[u8]`.
5. Per-record key/value as zero-copy `Bytes`: add a deserializer fast path so the emitted
   value/key are `source_bytes.slice_ref(record_value_slice)` instead of `to_vec()`.
   Recommended: add `Deserializer::deserialize_from_shared(&self, topic, source: &Bytes,
   data: &[u8]) -> T` with a default that calls `deserialize(topic, data)`; the FFI's
   bytes deserializer overrides it to `source.slice_ref(data)`. `fetch_records` passes the
   `MemoryRecords` `Bytes`.
6. FFI: `type Bytes = bytes::Bytes` (real), consumer becomes
   `ConsumerRecords<Bytes, Bytes>`; the Python `borrowed_memoryview` path already borrows
   from the owning batch, so it keeps working (now borrowing the shared `Bytes`).

### Phase 2 — Secondary per-poll costs
- Reuse the per-poll `IndexMap<TopicPartition, Vec<ConsumerRecord>>` (pool/clear-and-reuse
  across polls) to cut the ~2.3% build+drop+memset (`fetch_collector.rs:195`).
- `varint.rs`: single up-front bounds check + unchecked byte reads in the decode loop
  (~1.8%); keep behavior identical, add fuzz/round-trip test.
- Avoid `FetchPosition`/`LeaderAndEpoch` `String` clones on position update
  (`subscription_state.rs:401/414/465`) — use `Arc<str>` for broker id if it shows up.

## Verification
- Re-run the on-CPU flamegraph harness used in this session
  (`/tmp/.../flame/run_perf.sh`, real perf binary, 10k msg/s) before/after each phase;
  assert `__memset` ≈ 0 after Phase 0, and `__memmove` + deserializer `malloc` drop
  sharply after Phase 1. Compare DSO self-time (libc share should fall).
- Correctness: `cargo test` (consumer unit/integration), the wire-protocol byte-level
  encoding tests (DoD #3 — ensure `records` Bytes change stays wire-compatible), the
  receive-path per-record allocation-budget test (§27 "Tests required"; add/extend it to
  assert no per-record key/value copy after Phase 1), Python + C binding tests.
- Full gate: `make verify` (build + unit + integration + Python + C + format + lint).
- Behavior parity: a real consume run (local broker) still returns identical records;
  Python memoryview values remain valid for the lifetime of the batch.

## Risks / notes
- Phase 1 touches generated wire code → keep byte-level encode/decode tests green and
  limit the `Bytes` field-type change to the records path.
- `ByteBufferAccessor` is read+write; keep writes on `Vec`/`BytesMut`, add a separate
  read-only Bytes reader rather than mutating the existing type.
- `Deserializer<T>` is a §27 public contract; add the shared-buffer method as a defaulted
  extension so existing impls are unaffected.
