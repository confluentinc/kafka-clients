---
name: review-compression-perf-zerocopy
description: Critic 67 review of LZ4-liblz4 swap + per-batch zero-copy (BufferPool clear, BatchRefIterator, Bytes adoption/reclaim); verification recipes
metadata:
  type: project
---

Branch `compression-perf-improvements`, commits 34ed0e3a (LZ4→lzzzz/liblz4) +
0193f121 (three per-batch memcpy/memset eliminations). Reviewed clean except one
test-coverage finding.

**Why (perf context):** producer CPU decomposition on EC2 x86 — memmove ~15.6%,
LZ4 encode ~18% under lz4. These commits target both.

**How to apply / verification recipes that paid off here:**
- `Bytes::from(Vec)` capacity/realloc behavior is version-specific: check
  `bytes-<ver>/src/bytes.rs` `impl From<Vec<u8>>`. In 1.11.1 it preserves cap
  (no shrink realloc) for both len<cap (Shared) and len==cap (no-op
  into_boxed_slice) — so `try_into_mut`→`Vec` round-trips capacity, which is what
  keeps `BufferPool::deallocate_with_size(buf, initial_capacity)` recycling.
- lzzzz finish path: `WriteCompressor::flush()` (LZ4F_flush) emits all blocks +
  propagates errors; only `into_inner()`'s `LZ4F_compressEnd` end-marker write is
  swallowed. Safe ONLY because the sole `wrap_for_output` caller
  (memory_records_builder.rs) passes an in-memory Vec. Grep wrap_for_output
  callers to confirm W is in-memory before accepting the swallow.
- BufferPool memset removal (`buf.clear()` only, len 0): safe because
  MemoryRecordsBuilder::new normalizes both len==0 (resize-up) and len==size
  (truncate for None; separate append_buf for compressed) to header_end, and
  `Bytes::from(vec)` serializes `len` not `capacity` so tail stale bytes never
  reach the wire. Java BufferPool fast path is `buffer.clear()` (no re-zero).
- BufferPoolTest.testSimple mapping: Java clear() → position==0 && limit==capacity
  maps to Rust len==0 && capacity==size (recycled); first (fresh) allocate keeps
  len==size ≡ Java limit==size.

**The finding (Issue 1):** new reclaim branches — take_buffer `try_into_mut`
reclaim + shared fallback, `reopen_and_rewrite_producer_state` reclaiming an
adopted (empty self.buffer) builder, `build()`→empty after take_buffer — have no
direct test. txn_partition_entry reopen tests use a `batch()` helper that never
closes the builder, so `self.buffer.is_empty()` is false → new branch skipped;
they assert only the builder `base_sequence` field, not the rewritten wire
header. Production hits the opposite (built+adopted batches reopened during
idempotent epoch bump per producer-transactions.md §7).
TRAP for future critics: "reopen path is tested" is false — check whether the
test's batch was BUILT (close/records() called) before reopen, else the adoption
branch is dead in the test.

**Latent sub-note (unreachable):** take_buffer on a num_records==0 CLOSED builder
returns a capacity-0 Vec (reclaims from empty Bytes), abandoning the poolable
allocation the old `std::mem::take` returned. Accounting stays correct.
Unreachable on producer path (batches always ≥1 record; aborted batches take the
built_records.is_none() early-return).

**Faithful, not bugs:** LZ4 level map (9→fast LZ4F 0, else highCompressor(level);
levels 1-2 lzzzz-fast vs lz4-java-LZ4HC divergence = valid interop frames);
records()=build() refcount retain ≡ ProducerBatch.records(); reopen resets
self.closed (Rust's ≡ Java builtRecords!=null sentinel);
ensure_open_for_record_batch_write keys off self.closed not append_stream.
