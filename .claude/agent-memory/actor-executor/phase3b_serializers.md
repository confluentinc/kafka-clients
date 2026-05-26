---
name: Phase 3b serializers translation summary
description: What landed in Phase 3b, what was skipped, what's deferred to later phases
type: project
---

## Translated (24 files in `src/common/serialization/`)

Trait base layer:
- `serializer.rs` — `Serializer<T>` trait with dual `serialize` / `serialize_to` shape (see `phase3b_serializer_design.md`).
- `deserializer.rs` — `Deserializer<T>` trait.
- `serde.rs` — `Serde<T>` trait combining the two.
- `serdes.rs` — factory functions (`serdes::Long()`, `serdes::Integer()`, etc.) + `WrapperSerde<T, S, D>` + `serde_from()` constructor + `ByteArrayOwnedSerializer` and `StringOwnedSerializer` adapters.

Per-type serializers/deserializers (one file each):
- `byte_array_*` — identity over `[u8]` / `Vec<u8>`
- `string_*` — UTF-8 default with UTF-16/16BE/16LE support; `StringEncoding` enum exposed
- `integer_*`, `long_*`, `short_*` — BE 4/8/2-byte signed
- `float_*`, `double_*` — IEEE-754 BE with raw NaN preservation (`f32::to_bits`)
- `boolean_*` — 1-byte 0x00/0x01
- `bytes_*` — wraps `bytes::Bytes` (per CLAUDE.md rule 1.2; Java's `org.apache.kafka.common.utils.Bytes` skipped — see design note)
- `byte_buffer_*` — `Vec<u8>` (no Rust stdlib `ByteBuffer`)
- `uuid_*` — `uuid::Uuid` (Java's `java.util.UUID`, NOT Kafka's `common::Uuid`); 36-byte text form
- `void_*` — `()` always serializes to `None`
- `list_*` — `Vec<Option<T>>` with `InnerKind::FixedSize(n)` / `InnerKind::VariableSize` (see `phase3b_list_serde_gap.md`)

## Tests (66 in `src/common/serialization/tests.rs`)

Translated from Java's `SerializationTest`:
- `allSerdesShouldRoundtripInput` × 10 types (split per type).
- `allSerdesShouldSupportNull` × 10 types (split per type).
- `stringSerdeShouldSupportDifferentEncodings` (UTF-8, UTF-16).
- `stringSerdeConfigureThrowsOnUnknownEncoding`.
- `stringDeserializerSupportByteBuffer` — Rust simplification (no separate ByteBuffer overload).
- `floatDeserializerShouldThrow*` × 3 (zero/too-few/too-many bytes).
- `floatSerdeShouldPreserveNaNValues`.
- `testSerializeVoid` / `testDeserializeVoid` / `voidDeserializerShouldThrowOnNotNullValues`.
- `testBooleanSerializer`/`testBooleanDeserializer` × {true, false} (Java `@ParameterizedTest`).
- `booleanDeserializerShouldThrowOnEmptyInput`.
- `testSerdeFromNotNull` (compile-time-enforced in Rust).
- All `listSerde*` tests — round-trip + byte-count for Int/Short/Long/Float/Double/UUID, plus null-entry round-trip for Int/String.

Bonus tests (CLAUDE.md DoD #3 — byte-level encoding fixtures):
- `integer_wire_bytes`, `long_wire_bytes`, `short_wire_bytes` — known BE encodings.
- `boolean_wire_bytes`, `float_wire_bytes`, `double_wire_bytes`.
- `byte_array_wire_bytes`.
- `integer_serialize_to_writes_directly`, `integer_serialize_to_null_writes_nothing`,
  `long_serialize_to_appends` — exercise the zero-copy `serialize_to` path.
- `uuid_serializer_produces_36_bytes_utf8` — verifies the length contract that
  the List-UUID byte-count test (117 bytes) depends on.
- `list_serializer_strategy_is_constant_for_fixed_inner` /
  `list_serializer_strategy_is_variable_for_variable_inner` — verify the
  `InnerKind` → `SerializationStrategy` mapping.

## Skipped Java tests (with rationale)

- `testSerdeFromUnknown` — Java's `Serdes.serdeFrom(Class<T>)` factory has
  no Rust analog (no class-keyed factory). Per-type factory functions
  replace it; the unknown-type case is compile-time-impossible.
- `listSerdeShouldReturnLinkedList` / `listSerdeShouldReturnStack` —
  Java preserves the concrete `List` subclass via reflection; Rust always
  returns `Vec<Option<T>>`. Documented in `phase3b_list_serde_gap.md`.
- `ListSerializerTest.java` (12 tests) and `ListDeserializerTest.java`
  (16 tests) — exercise runtime class-loading paths
  (`Utils.newInstance(class_name)`, `Class.forName`) that have no Rust
  analog. The list-serde tests above cover the round-trip surface that
  *does* translate. See `phase3b_list_serde_gap.md`.

## Deferred to later phases

- **Phase 3c/3d:** `MemoryRecordsBuilder::append` will call
  `Serializer::serialize_to` exclusively to satisfy CLAUDE.md rule 12.
  The trait surface is in place; the call site lands in 3c/3d.
- **Phase 5:** Headers-aware serializer overload (Java's
  `serialize(String, Headers, T)`). The producer send path will route
  headers through the record itself, not through a serializer overload —
  see `phase3b_serializer_design.md`.

## Test count

Baseline 350 → Phase 3b 416 (+66 new tests).

## Files added

```
src/common/serialization/
  mod.rs
  serializer.rs
  deserializer.rs
  serde.rs
  serdes.rs
  byte_array_serializer.rs / byte_array_deserializer.rs
  string_serializer.rs / string_deserializer.rs
  integer_serializer.rs / integer_deserializer.rs
  long_serializer.rs / long_deserializer.rs
  short_serializer.rs / short_deserializer.rs
  float_serializer.rs / float_deserializer.rs
  double_serializer.rs / double_deserializer.rs
  boolean_serializer.rs / boolean_deserializer.rs
  void_serializer.rs / void_deserializer.rs
  bytes_serializer.rs / bytes_deserializer.rs
  byte_buffer_serializer.rs / byte_buffer_deserializer.rs
  uuid_serializer.rs / uuid_deserializer.rs
  list_serializer.rs / list_deserializer.rs
  tests.rs (cfg-test only)
```

## Apache 2.0 license headers

Confluent Inc. copyright on all 31 files. No GPL+CPE files in Phase 3b
(no OpenJDK code translated).
