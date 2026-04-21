---
name: M2 Phase 3 patterns
description: Assert-vs-clamp in slice(), missing validation in first_batch_size(), untranslated edge-case test dimensions for record batch layer
type: feedback
---

Phase 3 (Record Batch layer) common translation issues:

1. **Assert panics where Java clamps**: `MemoryRecords::slice()` has `assert!(size <= buffer.len())` but Java silently clamps via `Math.min(size, limit - position)`. Pattern: Java methods that accept "oversized" parameters and truncate them. Rust translations add defensive asserts that change the contract from clamping to panicking.

2. **Delegated validation lost in translation**: Java's `firstBatchSize()` delegates to `ByteBufferLogInputStream.nextBatchSize()` which validates magic byte range and minimum record size. Rust inlined the logic but dropped the validations. Pattern: when Java delegates to helper classes for validation, the Rust translation sometimes inlines only the happy path.

3. **Negative i32-to-usize cast wrapping**: `read_i32(...) as usize` for corrupt data with negative length fields wraps to huge values (~4 billion) on 64-bit. This doesn't crash the BatchIterator (bounds check catches it) but `first_batch_size()` and `size_in_bytes()` return garbage values. This is the same pattern seen in M2 Phase 2.

4. **Missing test parameterization dimensions**: Java tests are parameterized with `bufferOffset` in {0, 15} and `magic` in {v0, v1, v2}. Rust tests only use initial_position=0 and magic v2. The magic v0/v1 exclusion is deliberate (producer-only scope), but non-zero initial_position is a legitimate dimension that could hide offset arithmetic bugs.

**Why:** These patterns recur across phases. Assert-vs-clamp is particularly insidious because it looks like the right defensive measure but changes API semantics.

**How to apply:** When reviewing slice/view/window operations, check whether Java accepts "oversized" parameters and clamps. When reviewing methods that delegate to helper classes in Java, verify all validations are preserved. Always check i32-to-usize casts for negative values.
