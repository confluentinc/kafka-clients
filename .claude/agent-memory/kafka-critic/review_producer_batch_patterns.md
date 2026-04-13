---
name: ProducerBatch rewrite patterns
description: Memory accounting asymmetry and error swallowing patterns found in ProducerBatch MemoryRecordsBuilder integration
type: project
---

ProducerBatch Phase 3 rewrite to use MemoryRecordsBuilder introduced two pattern categories:

1. **Memory accounting asymmetry**: Per-record semaphore acquire includes batch header overhead (61 bytes) for every record, but batch-level release only counts it once. This is a systemic risk whenever per-record estimates are used for batch-level resource tracking. Java avoids this by managing memory at the buffer/batch level via BufferPool, not per-record.

**Why:** The Rust accumulator uses a different memory management approach (per-record Semaphore permits) than Java (BufferPool with whole-batch allocation). When adapting Java's `estimateSizeInBytesUpperBound` (designed for initial buffer sizing) to per-record permit accounting, the batch overhead gets double-counted.

**How to apply:** When reviewing any size estimation used for resource accounting, verify that acquire and release are symmetric. Check that batch-level overhead is not included in per-record estimates unless the release path also accounts for it per-record.

2. **Error swallowing via Option return**: When `MemoryRecordsBuilder::append()` returns Err (e.g., invalid timestamp), the error is converted to None, indistinguishable from "batch full." Java throws exceptions from append that propagate to callers. Watch for `Result -> Option` conversions that lose error information.

**How to apply:** When reviewing try_append or similar methods that return Option, check if any error path converts Err to None. The return type should be `Result<Option<T>>` to distinguish "no capacity" from "invalid input."
