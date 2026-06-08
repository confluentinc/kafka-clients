# Phase-16 §27 zero-copy receive-path decode — resolved Critic comments (Critic 1)

Reviewed commits `395ca2f`, `46fcf95`, `c03e1d4` on `consumer-impl`.

---

## Issue 1: Malformed batch record-count now panics or silently drops records (was a recoverable error / matches-Java error)
- **File**: `src/consumer/internals/completed_fetch.rs:596` (`advance_to_next_fetched_record`), `:640` (`peek_current_record`)
- **Severity**: Bug / Behavior Mismatch
- **Java Reference**: `kafka/clients/src/main/java/org/apache/kafka/common/record/DefaultRecordBatch.java:597-607` (`RecordIterator.next` / `hasNext`: `readRecords < numRecords`, then `ensureNoneRemaining()` → `InvalidRecordException("Incorrect declared batch size, records still remaining")`; reading past EOF → `InvalidRecordException("...premature EOF reached")`)
- **Description**: The new cursor derived `records_remaining` from the batch
  header's declared record count and byte-walked the records section instead of
  materializing via `DefaultRecordBatch::iter_records()`, which had validated the
  declared count against actual bytes (recoverable error in BOTH directions,
  CRC-independent — matching Java). The new path:
    - **declared count > actual ("too many")**: `peek_current_record().expect(...)`
      **panicked** when bytes were exhausted while `records_remaining > 0`
      (CLAUDE.md §10.1 violation), and the `.ok()?` on `read_ref_from_buffer`
      silently swallowed a malformed record body.
    - **declared count < actual ("too little")**: cursor stopped early and
      **silently dropped** remaining valid records.
    - Masked by default `check.crcs=true` but reachable under the supported
      `check.crcs=false` config, where Java still errors.
  - **Stale comment** at `:659-661` justified the `.expect`/`.ok()?` as infallible
    "because load_next_batch walks the batch's records once" — false after
    `c03e1d4` removed that walk.

### Resolution (fixup of `c03e1d4`)
- **Error type**: `KafkaError::illegal_state(...)`, matching the existing wrapping
  of `InvalidRecordError` already used on this exact path for CRC failures
  (`load_next_batch`) and header/decompression failures. `illegal_state` is NOT
  classified fatal (`KafkaError::is_fatal` returns false for it), so the error
  propagates out of `poll()` as a recoverable error rather than aborting the
  process — same recoverable disposition as the old `iter_records()` path. The
  two new regression tests assert `!err.is_fatal()`.
- **`peek_current_record` signature change**: now returns
  `Result<Option<(DefaultRecordRef, &BatchMetadata)>, KafkaError>`:
    - `Ok(None)` = no record positioned (count exhausted or bytes exhausted),
    - `Ok(Some(..))` = parsed record,
    - `Err(..)` = malformed record body (the `read_ref_from_buffer` `.ok()?` is
      replaced with `.map_err(|e| KafkaError::illegal_state(...))?`).
  All three call sites (`fetch_records` cached-exception peek, `fetch_records`
  read block, `advance_to_next_fetched_record`) now use `?` to propagate.
- **Declared count > actual ("too many")**: in `advance_to_next_fetched_record`,
  when `records_remaining > 0` (so the `needs_new_batch` branch was NOT taken)
  but `peek_current_record()?` yields `None`, return the premature-EOF
  `illegal_state` error (the `.expect(...)` is gone). The read-block peek in
  `fetch_records` has the same `else` guard for defence in depth.
- **Declared count < actual ("too little")**: new helper
  `ensure_current_batch_fully_consumed(&self)` is called at the top of
  `advance_to_next_fetched_record` whenever `needs_new_batch` is true. It is the
  `ensureNoneRemaining()` analog: once the declared count is exhausted
  (`records_remaining <= 0`) with a batch loaded, it compares the already-walked
  `record_byte_offset` against the known record-section length and errors if
  bytes remain. **No re-walk, no copy** — it is an O(1) integer comparison on the
  bytes the cursor already advanced past, so the §27/O(1) throughput win is
  preserved (allocation budget unchanged; no per-record re-validation pass).
- **Comment fix**: the stale `:659-661` comment is replaced with an accurate
  description ("a parse failure here is bad input … surface as a recoverable
  error … matching Java `DefaultRecordBatch.RecordIterator`, CRC-independent").

---

## Issue 2: Missing tests for the new cursor's control-batch, aborted-transaction, and invalid-count paths
- **File**: `src/consumer/internals/completed_fetch.rs` (tests)
- **Severity**: Missing Requirement (test coverage; DoD #3)
- **Description**: the batch-level invalid-count tests call `iter_records()`,
  which the consumer receive path no longer uses, so they no longer guard it;
  `test_multi_batch_ordering_and_offsets` covered only the happy path.

### Resolution (fixup of `c03e1d4`) — four `fetch_records`/`collect_fetch`-level tests added
- `test_invalid_record_count_too_many_through_fetch_records` (regression for
  Issue 1, `check.crcs=false`): batch declares 5 records, contains 3 → asserts a
  recoverable `KafkaError` whose message contains "premature EOF" and the
  partition, and `!is_fatal()`. Was a panic before the fix.
- `test_invalid_record_count_too_little_through_fetch_records` (regression for
  Issue 1, `check.crcs=false`): batch declares 2 records, contains 3 → asserts
  exactly 2 records are returned and then a recoverable `KafkaError`
  ("records still remaining") is raised — NOT silent truncation.
- `test_control_batch_skipped_mid_payload`: 3-batch payload (data / control /
  data) → asserts offsets 0,1,3,4 returned in order, control offset 2 skipped.
  (The producer builder forbids appending genuine control records, so the helper
  builds a data batch and flips the control-flag bit in the attributes,
  recomputing the CRC so it is valid under `check.crcs=true`; the cursor's
  `is_control_batch` branch keys off exactly that flag.)
- `test_aborted_transaction_batch_skipped_mid_payload` (READ_COMMITTED): 3-batch
  payload (committed / aborted-txn / committed) with the middle batch's
  producer id listed in `aborted_transactions` → asserts offsets 0,1,4,5
  returned in order, aborted offsets 2,3 skipped through the new
  `next_batch_start`-advance path of `load_next_batch`.

All four assert correct offsets/ordering and surrounding-record behavior, and the
two invalid-count tests assert the error message/kind (DoD #3), not just
`is_err()`.

---

## No-regression bar
- `cargo build` clean; `cargo xtask lint` clean; `cargo xtask format-check` clean.
- `cargo test --lib`: 1719 passed, 0 failed (baseline 1715 + 4 new tests).
- Incremental-cursor / borrowing-batch design intact: the malformed-count
  handling is an O(1) integer comparison on already-walked bytes, NOT a re-walk
  or per-record re-validation copy. Receive-path allocation budget unchanged.

---

## Verified OK (no action needed) — from original review
- **Borrow soundness / no `unsafe`**: No `unsafe` in any touched file. Borrowed
  key/value/header slices from `DefaultRecordRef`/`RecordSource` are consumed by
  the deserializer inside the `{ ... }` scope in `fetch_records` and never stored
  in `ConsumerRecord` (which has no lifetime param). Compiler-enforced; sound.
- **`ensure_cursor` move-not-clone**: `partition_data.records.take()` is safe for
  the current call graph. The only reads of `partition_data.records` are
  `records_size(...)` in `FetchCollector::collect_fetch:212` and
  `handle_initialize_success:544/552`, both of which run on a still-uninitialized
  `CompletedFetch` before the first `fetch_records`/`ensure_cursor`.
- **Incremental cursor arithmetic**: `next_batch_start` advance and bounds checks
  match `BatchIterator::next` exactly.
- **Owned `DefaultRecordBatch` delegating to `as_ref()`**: pure offset reads, no
  behavior change.
- **Compressed path**: `RecordSource::Owned(decompress_records())` decompresses
  once per batch into a cursor-owned `Vec`; records borrow from it per-record.
- **§27 compliance**: Single owning buffer (moved, not cloned); per-record
  allocation is only the user `T` + §27-sanctioned owned `RecordHeaders`.
