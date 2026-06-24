---
name: phase16-batch-loading-notes
description: Phase 16 — O(1)-amortized copy-free batch loading on consumer receive path (DefaultRecordBatchRef, incremental cursor, move-not-clone)
metadata:
  type: project
---

Phase 16 closed the consumer static-backlog throughput ceiling that the per-record §27 zero-copy decode (395ca2f/46fcf95) did NOT move.

**Why:** profiling proved the dominant cost was _platform_memmove in `load_next_batch`, not per-record and not decompression. `CompletedFetch::load_next_batch` re-walked `memory_records.batches()` from index 0 each call (O(N²) over a fetch), and `BatchIterator::next()` `to_vec`s each batch → O(N²) batch *copies* of the whole payload.

**How to apply / what landed (commit c03e1d4):**
- `BatchCursor` tracks `next_batch_start: Option<usize>` (absolute byte offset), advanced by `batch.size_in_bytes()` per consumed batch — NOT a batch index re-walked from 0. Locating next batch is O(1).
- New `DefaultRecordBatchRef<'a>` in `default_record_batch.rs` (re-exported in `record/mod.rs`): borrowing batch-header view over `&[u8]`, same read-only accessors as owned `DefaultRecordBatch` (pure offset reads). Owned `DefaultRecordBatch` now delegates ALL its read accessors to `self.as_ref()` (single source of truth for wire offsets). `compute_checksum` on the ref bounds CRC to `[ATTRIBUTES_OFFSET..size_in_bytes()]` (the slice may extend into following batches); owned case is identical because its buffer is exactly one batch.
- `load_next_batch` parses the header in place via `DefaultRecordBatchRef::new(&buffer[batch_start..])` — no per-batch `to_vec`. Uncompressed → `RecordSource::Borrowed(range)`; compressed → `RecordSource::Owned(decompress once)`.
- Stretch DONE: `ensure_cursor` MOVES `partition_data.records.take()` into `MemoryRecords::new(...)` instead of cloning a slice. Safe because all readers of the records bytes (FetchCollector::initialize → records_size snapshot) run strictly before the first `fetch_records`/`ensure_cursor`. Dropped §27 alloc budget from ~2.17 to 2.15/record.

**Verification:** `test_multi_batch_ordering_and_offsets` (≥3 batches end-to-end, uncompressed + gzip). Coordinator runs the live-broker ceiling drain (sandbox blocks broker net). 1715 lib tests pass, clippy + format clean.

Pattern worth remembering: when adding a borrowing analog of an owned wire type, make the owned type delegate its readers to the borrow view (`fn as_ref(&self) -> XRef<'_>`) so offset constants live in one place and there's zero behavior drift. See [[phase7a_design_notes]] §27 lazy iteration.
