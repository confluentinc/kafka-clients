---
name: Phase 3b serializer/deserializer trait design
description: Why the Serializer trait has both `serialize` and `serialize_to`, and the Headers-overload omission
type: project
---

## Why the dual `serialize` / `serialize_to` shape

Java's `Serializer<T>` has one signature: `byte[] serialize(String, T)`.
That always returns an owned byte array. On the producer hot path
(`MemoryRecordsBuilder::append`, Phase 3c/3d), allocating a new `Vec<u8>`
per record just to memcpy it into the batch buffer would violate
CLAUDE.md rule 12.

The Rust trait therefore exposes both:

1. `fn serialize(&self, topic: &str, data: Option<&T>) -> Result<Option<Vec<u8>>, KafkaError>`
   — direct Java analog. Used by the `Serde` factory and tests.
2. `fn serialize_to(&self, topic: &str, data: Option<&T>, out: &mut Vec<u8>) -> Result<bool, KafkaError>`
   — zero-copy hot-path API. Concrete impls override to write directly
   into the caller's buffer with no intermediate allocation. Returns
   `Ok(false)` for `None` input (matching Java's null contract); the
   default impl falls back to `serialize` + `extend_from_slice`.

`MemoryRecordsBuilder::append` (Phase 3d) will call `serialize_to`
exclusively. Every primitive serializer in Phase 3b overrides
`serialize_to` to skip the intermediate allocation:
`ByteArraySerializer`, `IntegerSerializer`, `LongSerializer`,
`ShortSerializer`, `FloatSerializer`, `DoubleSerializer`,
`BooleanSerializer`, `StringSerializer`, `BytesSerializer`,
`ByteBufferSerializer`, `UUIDSerializer`, `VoidSerializer`.

**Why not generic `BufMut`?** `dyn Serializer<T>` is needed because
`Serde<T>::serializer()` returns `&dyn Serializer<T>`. Generic parameters
on trait methods break dyn-compatibility. `&mut Vec<u8>` keeps the
trait `dyn`-compatible while still avoiding the per-record allocation
(the buffer is reused across `append` calls, so it amortizes to one
allocation per batch, not per record).

## Why no `Headers`-overload methods on the traits

Java's `Serializer<T>` has a default `serialize(String topic, Headers
headers, T data)` that forwards to `serialize(String, T)`. The Rust
`Headers` trait carries `&mut Self` builder methods (`add`, `add_kv`,
`remove`) that prevent it from being dyn-compatible. Adding
`fn serialize_with_headers(&self, ..., headers: &mut dyn Headers, ...)`
to the trait would refuse to compile.

We therefore omitted the headers overload from both `Serializer` and
`Deserializer`. Justification:

- Every Java concrete impl in Phase 3b's scope just ignores the headers
  argument and forwards. The contract is preserved at the trait level
  even without the overload.
- The producer send path (Phase 5) routes headers through the record
  itself (`ProducerRecord.headers()`), not through a serializer
  overload. A future phase that needs serializer-headers coupling can
  introduce a separate `HeaderAwareSerializer<T, H: Headers>` trait
  without touching `Serializer<T>`.

## Type mapping summary

| Java type | Rust type | Notes |
|-----------|-----------|-------|
| `byte[]` | `[u8]` (serialize) / `Vec<u8>` (deserialize, owned-Vec serializer in `Serdes`) | Java's `byte[]` is always owned; Rust serializer takes a slice for zero-copy |
| `String` | `&str` (serialize) / `String` (deserialize, owned wrapper in `Serdes`) | hot path uses `&str` |
| `Short`/`Integer`/`Long` | `i16`/`i32`/`i64` | matches Java's signed semantics |
| `Float`/`Double` | `f32`/`f64` | uses `to_bits()` for raw NaN preservation (Float) |
| `Boolean` | `bool` | 1-byte 0x00/0x01 |
| `Void` | `()` | always serializes to `None` |
| `ByteBuffer` | `Vec<u8>` | Rust has no stdlib `ByteBuffer`; `Vec<u8>` is the closest analog |
| `Bytes` (`org.apache.kafka.common.utils.Bytes`) | `bytes::Bytes` | popular crate replaces Java's `Bytes` wrapper class per CLAUDE.md rule 1.2 |
| `UUID` (`java.util.UUID`) | `uuid::Uuid` | uses 36-char hyphenated form (`Uuid::hyphenated().to_string()`) — NOT Kafka's `org.apache.kafka.common.Uuid` (which is a separate type used in the wire protocol) |

## Skipped Java classes

- `org.apache.kafka.common.utils.Bytes` — superseded by `bytes::Bytes`.
  Rationale: equality, hashing, and `byte[]`-wrapper semantics are all
  provided by the popular `bytes` crate (already a direct dep). CLAUDE.md
  rule 1.2 says "use a popular Rust crate when one exists with same
  behaviour".
