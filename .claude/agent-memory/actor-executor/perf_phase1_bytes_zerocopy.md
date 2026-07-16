---
name: perf-phase1-bytes-zerocopy
description: Consumer receive-path zero-copy via bytes::Bytes — Readable::read_bytes_owned, BytesReader, FieldType::Records→Bytes, BytesDeserializer slice_ref, producer-builder BytesMut deviation
metadata:
  type: project
---

Phase 1 of the consumer poll-path zero-copy plan (`now-run-again-the-quirky-thimble.md`).
Goal: kill the FetchResponse records-payload copy (~7%) and per-record key/value
`to_vec()` copy (~2.9%) by threading `bytes::Bytes` as the single owning buffer.

**Why / how it landed:**
- `bytes = "1"` in root Cargo.toml. Single crate, so generated code can `use bytes::Bytes`.
- `Readable::read_bytes_owned(len) -> io::Result<Bytes>` defaulted to copy via `read_array`;
  `BytesReader` (src/common/protocol/bytes_reader.rs) overrides it to `self.buf.slice(..)` (O(1)).
  network_client.rs parses responses through `BytesReader::new(Bytes::from(payload_vec))`.
- Generator: ONLY `FieldType::Records` → `Bytes` (type/defaults/read via `read_bytes_owned`).
  Split records out of the shared `generate_bytes_read` into new `generate_records_read`.
  `FieldType::Bytes` stays `Vec<u8>`. Added `use bytes::Bytes;` to every generated file.
- `Writable::write_records` now takes `Bytes`; ByteBufferSend/SendBuilderWritable hold
  `Vec<Bytes>`/`Bytes` (IoSlice borrows via Deref, vectored send still zero-copy).
- `MemoryRecords.buffer: Bytes`; `new(Bytes)`, `into_buffer()->Bytes`, `buffer()->&[u8]` (deref),
  added `buffer_bytes()->&Bytes`, removed `buffer_mut()` (no non-test callers), `slice()` uses
  `Bytes::slice`. ~60 test sites: `MemoryRecords::new(vec)`→`new(vec.into())`,
  `set_records(Some(vec))`→`set_records(Some(Bytes::from(vec)))`.
- BytesDeserializer (src/common/serialization/bytes_deserializer.rs): impl `Deserializer<Bytes>`;
  `deserialize` copies (fallback), `deserialize_from_shared`+`_with_headers` do `source.slice_ref(data)`.
  completed_fetch.rs `fetch_records` captures `source_bytes` (cheap clone of owning Bytes:
  Borrowed→`memory_records.buffer_bytes().clone()`, Owned→decompressed Bytes clone) and calls
  `deserialize_from_shared_with_headers`. `RecordSource::Owned(Bytes)`.
- FFI: `type Bytes = bytes::Bytes`; consumer built with BytesDeserializer; key()/value() return
  ptr+len via Bytes Deref; add_record uses `Bytes::copy_from_slice`.

**KEY DEVIATION (documented in code at memory_records_builder.rs take_batch_data):**
The plan's step 6 said producer builder buffer `Vec<u8>`→`BytesMut` with freeze()/split for a
zero-copy producer finalization. NOT done. Reason: `bytes` 1.x has NO public zero-copy
`Vec<u8>→BytesMut` (`BytesMut::from_vec` is crate-private). Switching would either (a) copy the
pooled Vec at builder construction, or (b) break BufferPool reuse (the pool reclaims the
original-capacity `Vec` via `take_buffer`; `deallocate_with_size` drops buffers whose capacity
!= poolable_size). So the builder keeps `Vec<u8>` and `take_batch_data` retains its single
finalization copy, now wrapped as `Bytes::from(vec)` (adopts, no extra copy). Net producer
copies unchanged (plan claimed -1; not achievable cleanly). Receive path (the real win) unaffected.

**Trait default chain gotcha:** `deserialize_from_shared_with_headers` default delegates to
`deserialize_with_headers` (preserves header-inspection deserializers' copy path). A byte-typed
deserializer wanting zero-copy must override BOTH `deserialize_from_shared` AND
`deserialize_from_shared_with_headers` (BytesDeserializer does). Don't make the headers-variant
default route to `deserialize_from_shared` — that would silently drop a custom deserializer's
header logic on the receive path.

**slice_ref precondition:** the `data: &[u8]` must lie within `source: &Bytes`'s allocation.
Guaranteed here because key/value slices are borrowed from the same Bytes (Borrowed: subslice of
memory_records buffer; Owned: subslice of decompressed Bytes). Never use `unsafe` to fabricate.

**Verification:** new test `test_collect_fetch_bytes_deserializer_zero_copy_budget` in
fetch_collector.rs → 0.11 allocs/record (was ~2.2 with copying String/ByteArray deserializer).
2022 lib tests pass, all targets green, FFI release builds, Python unit tests (76) pass.
`ResponseHeader::parse` + `ConcreteResponse::parse_response` now take `&mut dyn Readable`
(consumed bytes tracked via `remaining()` not `position()`) so BytesReader works for header parse.
