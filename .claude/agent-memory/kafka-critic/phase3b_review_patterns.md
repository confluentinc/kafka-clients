---
name: Phase 3b serialization review patterns
description: What to grep for when reviewing serializer/deserializer translations — UUID text-form, NaN bits, constant re-export rule, hot-path zero-copy
type: project
---

## High-yield review checks for serializer/deserializer translations

1. **UUID text vs binary**: Java's `UUIDSerializer` uses `data.toString().getBytes()` — 36-byte text form, NOT 16-byte binary. Common mistake is `uuid.into_bytes()` / `as_bytes()`. Verify via test that bytes length is exactly 36.
2. **Float NaN preservation**: Java uses `Float.floatToRawIntBits` (raw, preserves payload). Rust must use `f32::to_bits()` + `to_be_bytes()`, NOT `f32::to_int_unchecked` and NOT `floatToIntBits` (which collapses NaN payloads). Same for f64.
3. **Endianness**: Every primitive is BIG-ENDIAN in Java's serializers. Verify `to_be_bytes` / `from_be_bytes` (not `to_le_bytes`).
4. **Error message text**: Java error strings are part of the behavioral contract. Verify exact strings:
   - `"Size of data received by IntegerDeserializer is not 4"` (etc.)
   - Float/Double byte[] overload uses `"Size of data received by Deserializer is not N"` — NOT `"FloatDeserializer"`/`"DoubleDeserializer"` (those are only in the Headers/ByteBuffer overload).
   - `"Unexpected byte received by BooleanDeserializer: {b}"`
5. **Identity ByteArray copy**: `ByteArrayDeserializer` returning `data.map(<[u8]>::to_vec)` is a copy not in Java — but it's *unavoidable* in Rust because `Option<&[u8]> → Option<Vec<u8>>` requires owning. Not a bug.

## CLAUDE.md rules to grep for

- **Constant re-export rule** (rule 2): `pub use mod::CONSTANT_NAME` at parent module level is FORBIDDEN. Constants must be reached via `module::file::CONSTANT_NAME`. Easy to miss when reviewing `mod.rs`.
- **Headers overload omission**: serializer/deserializer trait can correctly skip the `(String, Headers, T)` overload because Java's `Headers` trait (with `&mut Self` builder methods) breaks dyn-compatibility. The omission must be documented in trait doc + actor memory.

## Hot-path zero-copy verification

For every concrete `Serializer<T>` impl, check that `serialize_to(&self, ..., out: &mut Vec<u8>) -> Result<bool, ...>` is overridden to write directly via `extend_from_slice` / `push` (no intermediate `Vec`). The default impl falling back to `serialize` + `extend_from_slice` is acceptable for non-hot-path types but a regression for primitives on the producer send path.

## ListSerializer/ListDeserializer translation gotchas

- Java relies on `inner.getClass()` reflection — Rust replaces with explicit `InnerKind::FixedSize(n)` / `InnerKind::VariableSize` tag. This is a justified Rust addition (DoD #7).
- `ListSerializerTest.java` and `ListDeserializerTest.java` are entirely about the `configure(Map)` runtime-class-loading path and are correctly skipped. The wire-format tests are in `SerializationTest.java` (`listSerde*` methods) and MUST be translated.
- `assertInstanceOf(LinkedList.class, ...)` / `assertInstanceOf(Stack.class, ...)` are about Java's listClass parameter and don't apply to Rust (`Vec<Option<T>>` is the only return type). Skipping is correct.

## ByteBuffer mapping

Java's `ByteBuffer` (with position/limit cursor) has no Rust stdlib analog. `Vec<u8>` is the actor's chosen substitute, with the convention that the `Vec<u8>` length is the populated payload size (mirroring `ByteBuffer.flip() + array()`). Acceptable as long as documented.
