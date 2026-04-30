---
name: Phase 2d generator/runtime API gap (resolved in 2d-1)
description: How the generator's emit code was aligned with the Phase 2c runtime traits — captured for Phase 2d-2/3/4 actors
type: project
---

Phase 2d-1 (commit `ae6055f`) closed the systemic gap between the
generator's emitted code and the Phase 2c runtime traits. Phase 2d-2/3/4
actors should NOT need to make any further generator-side changes — the
emit code now produces files that compile against the Phase 2c traits.

**What changed (generator emit-side only — runtime is Phase 2c verbatim):**

- File header: plain `//` comments (not `//!`) + per-`use` outer
  `#[allow(unused_imports)]`. Required because the generated file is
  included via `include!()` inside a `mod { ... }` block.
- Imports: `Field`, `Schema` from `crate::common::protocol::types`;
  `byte_utils` from `crate::common::utils::byte_utils` (the runtime
  has no `protocol::varint` module).
- Inherent `read`/`write` methods on each `Data` struct return
  `Result<_, KafkaError>` (not `std::io::Result`).
- `Message::add_size` impl returns `()` (matching the runtime trait).
- `Message::read`/`write` impls return `Result<(), KafkaError>`.
- `writable.write_*(...)` is infallible — no `?` after these calls
  (the runtime trait methods return `()`).
- `read_bytes(&mut buf)?` is replaced by `buf = read_array(buf.len())?`.
- `write_bytes` is renamed to `write_byte_array`.
- `std::io::Error::new(InvalidData, ...)` → `KafkaError::Generic(...)`.
- `Field { name, field_type, about }` literal → `Field::with_doc(name, type, doc)`.
- `Schema::new(fields)` returns `Result<Schema, KafkaError>` and the
  emit body returns it as-is.
- `SchemaType::*` → `Type::*` (with variant rename: `Uint16` → `UInt16`,
  `Uint32` → `UnsignedInt32`).
- `RawTaggedField::new(tag, data)` always casts `tag` to `i32`.
- `field.tag()` is cast to `u32` when passed to
  `write_unsigned_varint`.
- `byte_utils::size_of_unsigned_varint(...) as i32` — every emit site
  appends the cast because `add_bytes` takes `i32` while the byte-utils
  helper returns `usize`.
- Stylistic clippy lints (`manual_range_contains`,
  `vec_init_then_push`, `new_without_default`) are suppressed via
  `#[allow]` on each generated `impl` block.

**Runtime-side additions:**

- `RawTaggedField` now derives `Hash` (necessary because every
  generated `Data` struct derives `Hash` and contains
  `Vec<RawTaggedField>`).
- `RawTaggedField` is re-exported from `crate::common::protocol::`
  (the generator emits `use crate::common::protocol::{..., RawTaggedField};`).

**API surface that's still **stub** in 2d-1:**

- `ApiMessageType::request_schema(version)` and `response_schema(version)`
  return an empty schema. They will dispatch through every
  `crate::common::message::*_data::*Data::schema(version)` once Phase
  2d-4 has wired all message specs in. Until then, users of
  `request_schema`/`response_schema` will get a useless empty `Schema`.

**Generator emit fixes added in Phase 2d-2 (additive, none of these
are new traits or runtime APIs):**

- `KafkaError::Generic("...")` for null/negative-length string and
  array errors now wraps the literal in `.to_string()` to match the
  `Generic(String)` variant. (lib.rs lines emitting "Null string not
  allowed" / "Negative string length" / "Null array not allowed" /
  "Negative array length")
- `add_size(...)` callsites no longer use the `?` operator — the
  `Message::add_size` trait method returns `()`, not `Result`.
  Affected emit sites: any `add_size(size, cache, version)` call,
  including loops over array elements and tagged-field struct sizing.
- Tagged-field write paths that pre-encode into a temporary
  `ByteBufferAccessor` now use `allocate(N)` (not the non-existent
  `new(N)`), and after writing call `position()` for the size,
  `flip()`, then `buffer()` for the bytes. (`buffer()` returns
  `&buf[..limit]`; without the flip, limit equals capacity and the
  emitted bytes would include trailing zeros.)
- Two new clippy allows on the inherent `impl X { ... }` block:
  `collapsible_if` (because `if version >= N { if cond { ... } }` is
  emitted by independent generator passes — the version gate and the
  field-presence gate are orthogonal), and `bool_comparison` (the
  generator emits `x != false` for the boolean tagged-field default
  check). Same allows are also placed on the `impl Message for X`
  block because `add_size` and `write` share these patterns.

**For Phase 2d-3/4:**

Adding a new generated module is now a 2-line change in
`src/common/message/mod.rs` plus a round-trip test in
`src/common/message/tests.rs`. No generator changes should be needed
for arrays-of-structs, struct-typed tagged fields, int64/bool tagged
fields, or non-flexible specs (Phase 2d-2 verified these). If a
generator change is needed, it indicates the emit code has a new
field type / encoding edge case the previous specs didn't exercise —
note it explicitly in the commit message.
