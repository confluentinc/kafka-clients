---
name: Phase 2b types module design
description: How protocol/types is modeled in Rust — Type enum, Value enum, Schema id, ReadBuffer cursor
type: project
---

Phase 2b translation of `org.apache.kafka.common.protocol.types`.

**Why this design**: Java's Type ladder dispatches on `java.lang.Object` and uses anonymous singleton subclasses; the cleanest Rust translation is two enums — `Type` (one variant per primitive + Box for ArrayOf/CompactArrayOf/Schema/TaggedFields containers) and `Value` (Bool/Int*/Float64/String/Bytes/Array/Struct/Uuid/TaggedFields/RawTagged). All read/write/size_of/validate dispatch is a `match` instead of dynamic dispatch.

**How to apply**: When extending in Phase 2c+, add new types as new `Type` variants and matching `Value` variants — never add a trait-object Type. Schema has a monotonic `u64` id stamped via `AtomicU64` so cross-schema BoundField misuse is caught at runtime, mirroring Java's reference equality.

`SchemaException` collapses to `KafkaError::Generic` per CLAUDE.md rule 10; tests assert on `err.message()` exactly. The text matches Java's `e.getMessage()` verbatim.

`ReadBuffer<'a>` is a position-tracking borrow over `&[u8]`; this is a Phase-2b stand-in. Phase 2c will introduce the proper `ByteBufferAccessor` (and the `Readable`/`Writable` traits in `protocol::types::io` should move to `protocol::{readable, writable}`).

Records / CompactRecords are present in `Type` but only handle the bytes-level encoding; integration with `MemoryRecords` is Phase 2c+.
