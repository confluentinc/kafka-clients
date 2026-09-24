# Critic 78 — resolved (APPSEC-7665 / NONJAVACLI-4520 decode hardening)

Review of `f00b57df` (plan §2.1), `1ebbed96` (§2.2) and `7e171207` (§2.3);
spec `design/current/appsec-7665-4520-decode-hardening.md`. One finding and four
notes. The Manager decided to act on all four notes; each is recorded below with
how it was handled (Actor 78, round 2).

## Issue: The control-batch D3 test passes without the control-batch D3 check
- **File**: `src/consumer/internals/completed_fetch.rs` (`test_negative_record_count_in_a_control_batch_is_invalid`, :2789; check under test at :1042-1048)
- **Severity**: Missing Requirement (test coverage, low; the production code is correct)
- **Java Reference**: `CompletedFetch.java:376-386` (`containsAbortMarker` → `batch.iterator()`), `DefaultRecordBatch.java:321-326`, `:584-587`
- **Description**: The test is meant to pin plan §6 item 5, the
  `records_count < 0` rejection inside `contains_abort_marker`. It cannot detect that
  check's removal.
  - Its fixture `control_batch(2, 1, 1000)` writes a record with a null key
    (`append_with_offset_bytes(offset, 0, None, ..)`, :1906), so without the check
    `contains_abort_marker` returns `Ok(false)`.
  - The fetch has no aborted transactions, so the batch is not skipped.
  - The batch then reaches the install-point D3 check (:1261-1272), which raises the
    identical `Found invalid record count -1 in magic v2 batch`.

  I verified this by mutation in a scratch copy of the crate: I changed :1046 to
  `if false && records_count < 0 {` and the test still passes.

  The check only makes a difference for a non-ABORT control batch whose producer id is in
  the aborted set. Without the check, that batch is silently skipped by the `is_transactional
  && aborted_producer_ids.contains(..)` arm, where Java throws. I wrote a scratch test for
  exactly that case:
  - buffer: batch 1, then `control_batch(2, 1, 1000)` with count −1 and the CRC recomputed;
  - partition data: `partition_data_with_aborted_txn(buf, 1000, 2)`;
  - config: READ_COMMITTED, `check.crcs=true`.

  It passes on the real code. On the mutant it fails: the second `fetch_records` returns
  `[]` instead of the error.
- **Expected**: A test that fails when the `contains_abort_marker` D3 check is removed.
  Either build the existing test's `CompletedFetch` with
  `partition_data_with_aborted_txn(buf, 1000, 2)` (via `CompletedFetch::new_full`, as
  `test_negative_record_count_in_an_aborted_batch_is_skipped` does), or add the scenario
  above as its own test. Keep asserting the exact message.
- **Actual**: The only test for the control-batch path passes because of the
  install-point check, so a regression in `contains_abort_marker` would go unnoticed.

**Resolution (Actor 78, round 2 — `fixup!` of `f00b57df`).** The existing test
was rebuilt as the Critic described rather than adding a second one: the fetch
is built with `CompletedFetch::new_full` over
`partition_data_with_aborted_txn(buf, 1000, 2)`, READ_COMMITTED,
`check.crcs=true`, so the control batch (a non-ABORT marker: its record has no
key) belongs to a producer id in the aborted set. The exact message
`Found invalid record count -1 in magic v2 batch` is still asserted, and the
first call now asserts offsets `[0, 1]` rather than a count. Its doc comment
says why the aborted transaction is there.

Teeth: with the check in `contains_abort_marker` (`completed_fetch.rs:1046` at
`f00b57df`, `:1056` after this fixup) changed to `if false && records_count < 0 {`
the test fails — the second `fetch_records` returns `Ok([])` (the batch is
skipped as aborted) where the error is expected; with the line restored it
passes. Plan §6 item 5 records why only that fetch can see the check.

## Notes

### Note 3 — the class-name prefix outside `load_next_batch`

Handled in the same `fixup!` of `f00b57df`. `peek_current_record` and
`contains_abort_marker` now append the cause's `message()` instead of its
`Display` (which prefixes `InvalidRecordError: `). A third wrapper of the same
shape — the headers wrap in `fetch_records` — got the same one-token change.
No existing test asserted the prefixed text; two new tests pin the first two
wrappers' texts exactly (`test_malformed_record_is_reported_by_the_cause_message`,
`test_malformed_control_record_is_reported_by_the_cause_message`) and fail with
the `Display` form put back. The wrapper structure and error class are
unchanged; plan §6 item 3 records that Java propagates the
`InvalidRecordException` unwrapped at these sites (a pre-existing structural
deviation outside this ticket) and why the headers wrap has no test (its only
trigger, a header key that is not UTF-8, is itself a pre-existing divergence:
Java decodes it with replacement characters).

