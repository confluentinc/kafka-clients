---
name: M2 Phase 2 record format patterns
description: Negative varint as usize cast causes panic — Java try/catch guards vs Rust missing size validation
type: project
---

Negative signed varint values cast to usize wrap to huge values, causing OOM/panic instead of graceful error.

**Why:** Java's `ByteBuffer.allocate(negative)` throws `IllegalArgumentException` caught by `try/catch`, but Rust has no equivalent safety net — `vec![0u8; negative_i32 as usize]` panics.

**How to apply:** When reviewing any `read_varint` / `read_varlong` result that is used as a size for allocation or slice indexing, verify there is a `< 0` guard before the cast to `usize`. This pattern recurs in `DefaultRecord.read_from_stream`, `read_from_buffer`, and `read_from_body`, and will likely appear again in `DefaultRecordBatch`, `MemoryRecords`, and any other deserialization code that reads sizes from the wire.
