---
name: ProducerBatch MemoryRecordsBuilder swap pattern
description: Pattern for extracting built bytes from MemoryRecordsBuilder in ProducerBatch since build() consumes self
type: project
---

ProducerBatch uses MemoryRecordsBuilder internally. Since build() consumes self, extracting the built bytes requires a swap-with-dummy pattern:
1. Call close() on the real builder (stores built_records internally)
2. Cache finalized_size from estimated_size_in_bytes() (returns exact size after close)
3. Replace the builder with a dummy via std::mem::replace
4. Call build() on the extracted real builder to get MemoryRecords

**Why:** MemoryRecordsBuilder::build() takes ownership (consumes self), but ProducerBatch needs to retain the struct after extracting bytes (for complete() to resolve futures). The cached finalized_size ensures written_bytes() returns the correct value even after the builder is replaced.

**How to apply:** When ProducerBatch.buffer() or finalized_bytes() is called, use this pattern. The Sender must call buffer() before written_bytes() in the response handling flow.
