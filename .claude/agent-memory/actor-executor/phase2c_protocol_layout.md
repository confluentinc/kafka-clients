---
name: Phase 2c protocol module layout
description: Where wire-protocol primitives live after Phase 2c (Readable/Writable/ByteBufferAccessor)
type: project
---

Phase 2c moved the wire-protocol primitive traits out of
`protocol/types/io.rs` (Phase-2b shim, now deleted) into top-level
modules under `src/common/protocol/`:

- `readable.rs` — full `Readable` trait (Java-equivalent contract)
- `writable.rs` — full `Writable` trait (also `impl Writable for Vec<u8>` for tests / simple builders)
- `byte_buffer_accessor.rs` — canonical `ByteBufferAccessor` plus `SliceReadable` for borrowed reads
- `message.rs`, `api_message.rs` — generator trait surface
- `message_size_accumulator.rs`, `object_serialization_cache.rs` — generator support
- `message_util.rs`, `data_output_stream_writable.rs`, `send_builder.rs` — top-level helpers
- `errors.rs` — `Errors` catalogue (1:1 with Java enum, separate from `KafkaError`)
- `api_keys.rs` — hand-coded `ApiKey` catalogue (Phase 2d will replace with generator-driven table)

`types/type.rs` `Type::read/write` now take `&mut dyn Readable` / `&mut dyn Writable`. Same for `Schema::read_struct/write_struct` and `Struct::write_to`. The old `ReadBuffer` and `Vec<u8>`-only signatures are gone.

`ApiKey` is a struct with `&'static [ListenerType]` listeners — *not* a Rust enum. Tests that need request/response schemas are deferred to Phase 2d (commented in api_keys.rs and listed in NOTES.md).

`Errors` is a separate enum from `KafkaError`. `Errors::for_code` → catalogue, `Errors::exception()` → `KafkaError`.

Test count: 185 (Phase 2b) → 225 (Phase 2c).
