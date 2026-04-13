---
name: ProducerBatch rewrite patterns
description: Memory accounting asymmetry and error swallowing patterns found in ProducerBatch MemoryRecordsBuilder integration
type: project
---

ProducerBatch Phase 3 rewrite to use MemoryRecordsBuilder introduced two pattern categories:

1. **Memory accounting asymmetry**: The per-batch allocation model (commit e51b9c6) fixed the N-1 batch header double-counting issue. However, `estimate_size()` still uses `DefaultRecord::record_size_upper_bound()` (record-only) instead of `DefaultRecordBatch::estimate_batch_size_upper_bound()` (record + 61-byte batch overhead). Java's `estimateSizeInBytesUpperBound` includes `RECORD_BATCH_OVERHEAD`. This causes under-accounting by 61 bytes when a single oversized record exceeds batch_size.

**Why:** The Rust accumulator originally used per-record estimates and was corrected to per-batch allocation matching Java. But the estimate function was not updated to include batch overhead, which Java's equivalent always includes.

**How to apply:** When reviewing size estimation for memory allocation, verify the estimate matches Java's `AbstractRecords.estimateSizeInBytesUpperBound()` which for V2 is `RECORD_BATCH_OVERHEAD + recordSizeUpperBound()`. The existing Rust function `DefaultRecordBatch::estimate_batch_size_upper_bound()` correctly implements this.

2. **Error swallowing via Option return**: When `MemoryRecordsBuilder::append()` returns Err (e.g., invalid timestamp), the error is converted to None, indistinguishable from "batch full." Java throws exceptions from append that propagate to callers. Watch for `Result -> Option` conversions that lose error information.

**How to apply:** When reviewing try_append or similar methods that return Option, check if any error path converts Err to None. The return type should be `Result<Option<T>>` to distinguish "no capacity" from "invalid input."
