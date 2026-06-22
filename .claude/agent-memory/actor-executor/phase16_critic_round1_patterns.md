---
name: phase16-critic-round1-patterns
description: Phase-16 Critic round-1 fixes — recoverable-error-not-panic on the zero-copy receive cursor, O(1) count validation without re-walk, malformed-batch test construction
metadata:
  type: feedback
---

Phase-16 (§27 zero-copy receive-path decode) Critic round-1 fixes, on
`src/consumer/internals/completed_fetch.rs`. Reusable patterns:

1. **A throughput rewrite that drops a validation step is a behavior bug, not
   just a perf change.** The O(1) `BatchCursor` replaced
   `DefaultRecordBatch::iter_records()` (which validated declared-count vs
   actual-bytes in BOTH directions, CRC-independent, matching Java
   `DefaultRecordBatch.RecordIterator`). Removing the walk silently removed the
   validation → panic (`.expect`) or silent record drop.
   **Why:** masked by default `check.crcs=true` (CRC catches it first) but
   reachable under the supported `check.crcs=false`. **How to apply:** when a
   rewrite bypasses a Java helper, re-audit what that helper validated and
   re-add it cheaply on the new data path.

2. **Recoverable Java error → `KafkaError::illegal_state`, verify `!is_fatal()`.**
   On the fetch path, `InvalidRecordError` is wrapped as
   `KafkaError::illegal_state(...)` (same as the existing CRC/decompression
   failure wrapping). `illegal_state` is NOT classified fatal, so it propagates
   out of `poll()` as a recoverable error rather than aborting. Tests assert the
   error message content AND `!err.is_fatal()` (CLAUDE.md §10.1 + DoD §3).

3. **Detect count mismatch in O(1) without re-walking.** "Declared > actual"
   (premature EOF): in the per-record loop, when `records_remaining > 0` but the
   peek yields `None` → error. "Declared < actual" (ensureNoneRemaining): a new
   `ensure_current_batch_fully_consumed(&self)` called at batch-exhaustion
   compares the already-walked `record_byte_offset` against the known
   record-section length. Both are integer comparisons on bytes the cursor
   already advanced past — no re-walk, no copy, throughput win intact.

4. **Make an "infallible" peek fallible by changing its return to `Result<Option<..>>`.**
   `peek_current_record` went `Option<..>` → `Result<Option<..>, KafkaError>`:
   `Ok(None)` = no record positioned, `Ok(Some)` = parsed, `Err` = malformed
   body (replace `.ok()?` with `.map_err(...)?`). All call sites then use `?`.
   A `let Some(..) = peek()? else { ... }` inside a value-assigning `{ }` block
   cannot `break` (not a loop) — return the error instead.

5. **Constructing malformed/control/aborted batches for receive-path tests:**
   - invalid declared count: build a normal batch, overwrite
     `buf[RECORDS_COUNT_OFFSET..+4]` with `bad_count.to_be_bytes()`, drive with
     `check.crcs=false` (no CRC recompute needed).
   - control batch: the producer builder PANICS on appending a control record to
     a non-control batch and vice versa, and exposes no public control-append.
     Build a data batch via `MemoryRecords::builder_full(... is_control=false)`,
     flip the control bit `0x20` in the attributes low byte
     (`buf[ATTRIBUTES_OFFSET+1] |= 0x20`, big-endian i16), then recompute CRC
     over `[ATTRIBUTES_OFFSET..]` with `crc32c::crc32c(...)` written
     big-endian at `CRC_OFFSET` → valid under `check.crcs=true`.
   - aborted-txn batch: `MemoryRecords::builder_full(... producer_id=pid,
     is_transactional=true, is_control=false)`, then on `PartitionData` call
     `set_aborted_transactions(Some(vec![txn]))` where `AbortedTransaction::new()`
     + `set_producer_id(pid).set_first_offset(..)`. Drive with READ_COMMITTED.
     NOTE: a *control* batch from an aborted pid hits the un-translated
     `containsAbortMarker` branch (returns `unsupported_version` error), so use a
     *non-control transactional* batch to test the clean skip path.

   Generated message structs (`fetch_response_data`) have BOTH `pub` fields and
   chainable `set_<field>(&mut self) -> &mut Self` setters.
