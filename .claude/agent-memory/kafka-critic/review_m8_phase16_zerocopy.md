---
name: review-m8-phase16-zerocopy
description: Phase-16 §27 zero-copy receive-path decode review patterns (header-count vs bytes divergence, move-not-clone audit)
metadata:
  type: project
---

Phase-16 rewrote the consumer receive path to decode records as borrowing
`DefaultRecordRef<'a>` / `DefaultRecordBatchRef<'a>` instead of materializing
owned `Vec<DefaultRecord>`. Key review findings/patterns:

**Header-count-as-loop-bound divergence (the real bug).** The new cursor
derives `records_remaining` from the batch header's declared `records_count`
and walks bytes, bypassing `DefaultRecordBatch::iter_records()` which validated
count-vs-bytes and returned a recoverable error. Result:
- declared count > actual → `peek_current_record` returns None while
  `records_remaining > 0` → `.expect("cursor verified non-empty above")`
  **panics** (was recoverable KafkaError; Java throws InvalidRecordException
  "premature EOF").
- declared count < actual → cursor stops early, **silently drops** remaining
  records (Java throws "records still remaining" via `ensureNoneRemaining()`).
- Masked by default `check.crcs=true` (CRC gate at load), but Java's count
  validation is CRC-independent → genuine divergence under `check.crcs=false`.
- **Lesson**: when a translation replaces an iterator that does count/size
  validation with a header-count-driven byte walk, check what validation was
  lost. Java's `RecordIterator` (DefaultRecordBatch.java ~597-607) validates
  count both directions. `.expect()` on a peek whose None-conditions are wider
  than the guard that "verified" it is a panic smell — check ALL None-return
  branches of the peeked fn against the guard.

**Stale-comment trap**: a comment justified `.ok()?`/`.expect()` as infallible
"because load_next_batch walks the records once to find ranges" — but a LATER
commit (c03e1d4) removed that walk (incremental next_batch_start). The
justification became false; the panic became reachable. Re-read justification
comments against the CURRENT code, not the commit that introduced them.

**Move-not-clone (`ensure_cursor` take) audit method**: grep every read of the
moved field (`partition_data.records` via `records_size`/`records_or_fail`) and
confirm each runs on a still-uninitialized CompletedFetch BEFORE the first
`fetch_records`. Here both `records_size` sites (FetchCollector collect_fetch +
handle_initialize_success) run pre-init; `build_aborted_transactions` runs at
ctor. No post-take read → safe. This pattern (take-once buffer) needs a
call-site invariant comment.

**Incremental batch-offset arithmetic check**: compare new `next_batch_start`
advance against the old `BatchIterator::next` (memory_records.rs) line-for-line:
LOG_OVERHEAD precheck, size = LOG_OVERHEAD + length, end-bounds partial-tail
termination, advance by size. RECORD_BATCH_OVERHEAD == RECORDS_OFFSET == 61;
LOG_OVERHEAD == 12. compute_checksum bounding `..size_in_bytes()` (was
`..buffer.len()`) is required for multi-batch ref, equal for owned single-batch.

**Test-gap predictor**: multi-batch test added covered uncompressed+gzip happy
path only. No multi-batch test for control-batch skip, aborted-txn skip, or
invalid-count through `fetch_records` — and the existing invalid-count tests
call `iter_records()` which the consumer no longer uses, so they stopped
covering the receive path. Batch-level tests going stale when the caller stops
using that API is a recurring DoD #3 gap.
