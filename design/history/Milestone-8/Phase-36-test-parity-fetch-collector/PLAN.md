# Phase 36 — Test-parity: FetchCollectorTest + CompletedFetchTest gaps

Actor 36. Branch `consumer-impl`. Closes the MISSING / REDUCED / CHANGED rows
in `design/current/test-translation-review/01-fetch-path.md` for
`FetchCollectorTest` and `CompletedFetchTest`.

## Hard constraint

Test-parity effort: TEST-ONLY changes preferred. A production change is allowed
ONLY for a genuine Java-fidelity bug, and only if perf/CPU-neutral (no new
per-record / hot-path allocation or dynamic dispatch — the fetch path is the
receive HOT PATH). **Outcome: NO production change is required.** All gaps are
closed with tests. (See "Production change" section at the bottom.)

## In-scope Java tests and their Rust mapping

### FetchCollectorTest

| Java test | Rust test(s) | Approach |
|---|---|---|
| `testErrorInInitialize` (×4 params) | `test_error_in_initialize_*` (4 cases, loop) | Inject an `initialize`-throws collector and assert the `recordCount==0 ? queue-empty : queue-not-empty` contract. Java overrides `initialize()` via an anonymous subclass; Rust has no subclass, so we add a `#[cfg(test)]` test hook on `FetchCollector` that forces `initialize` to fail. See "Test hook" below. |
| `testCollectFetchInitializationWithUpdateHighWatermarkOnNotAssignedPartition` | `test_update_partition_state_high_watermark_not_assigned` + `test_collect_fetch_not_assigned_partition_yields_nothing` | Java mocks `tryUpdatingHighWatermark→false`. In real Rust state `try_updating_*` returns false **iff the partition is not assigned** (`assigned_state_or_null_mut → None`). We drive `update_partition_state` directly with an unassigned partition and ONLY the high-watermark field set (others negative ⇒ skipped), asserting it returns `false`. Mutation-resistant: deleting the HW short-circuit makes the fn return `true`. |
| `...UpdateLogStartOffsetOnNotAssignedPartition` | `test_update_partition_state_log_start_offset_not_assigned` | Same, but `high_watermark=-1` (skipped), `log_start_offset=0` set ⇒ only the LSO branch runs. |
| `...UpdateLastStableOffsetOnNotAssignedPartition` | `test_update_partition_state_last_stable_offset_not_assigned` | `hw=-1, log_start=-1, last_stable=0` ⇒ only the LastStable branch runs. |
| `...UpdatePreferredReplicaOnNotAssignedPartition` | `test_update_partition_state_preferred_replica_not_assigned` | `hw=-1, others=-1, preferred=21` ⇒ only the preferred-replica branch runs. |
| `testCollectFetchInitializationWithNullPosition` | `test_collect_fetch_null_position_yields_nothing` | Java mocks `hasValidPosition→true, positionOrNull→null`. In real state `is_fetchable`/`has_valid_position` are false for a partition with no position, so `initialize` exits at the `!has_valid_position` guard. This is the SAME observable behavior the Java test asserts (empty fetch, next-in-line cleared). Folded with the assigned-without-seek fixture (cf existing `test_no_results_if_initializing`), strengthened to also assert next-in-line is cleared. |
| `testCollectFetchInitializationOffsetOutOfRangeErrorWithNullPosition` | `test_collect_fetch_oor_null_position_yields_nothing` | OOR error + no position. `handle_offset_out_of_range` with `position_or_null → None` discards (stale) and returns empty. Driven through `collect_fetch` with an OOR completed fetch and an assigned-but-unseeked partition... see note. |
| `testCollectFetchInitializationOffsetOutOfRangeErrorWithOffsetReset` | `test_collect_fetch_oor_offset_reset_requests_reset` | Asserts `request_offset_reset_if_assigned` is invoked. Drive through `collect_fetch` with default reset policy (LATEST) and assert the partition's reset is requested (observe via subscription state: `is_offset_reset_needed`/awaiting reset), empty fetch. |
| `testReadCommittedWithAbortedTransaction` | `test_read_committed_with_aborted_transaction` | All-aborted batch ⇒ 0 records but `next_offsets` advances. Build an aborted transactional batch (READ_COMMITTED), assert `collect_fetch` returns non-empty Fetch, 0 records, 1 next_offset advanced past the batch. |
| `testFetchWithOtherErrors` (all "other" Errors) | `test_fetch_with_other_errors` (EXPANDED) | Was REDUCED to 3 sampled errors. EXPAND to iterate the full `Errors` set minus the explicitly-handled ones (mirroring Java's `Errors.values()` minus the exclusion list). |

### CompletedFetchTest

| Java test | Rust test(s) | Approach |
|---|---|---|
| `testCorruptedMessage` | `test_corrupted_message_key_fails_after_valid_record` + `test_corrupted_message_value_fails_after_valid_record` (STRENGTHENED) | Already exist; strengthen assertions. See "testCorruptedMessage assertion decision". |
| `testAbortedTransactionRecordsRemoved` | `test_aborted_transaction_records_removed_direct` (NEW) | Direct port: ABORT marker, READ_COMMITTED ⇒ 0 records; READ_UNCOMMITTED ⇒ all `numRecords`. (Currently only covered indirectly by the mid-payload fixture.) |
| `testCommittedTransactionRecordsIncluded` | `test_committed_transaction_records_included_direct` (NEW) | Direct port: COMMIT marker, READ_COMMITTED ⇒ all records returned. |

## Test hook (testErrorInInitialize)

Java overrides `protected CompletedFetch initialize(...)` in an anonymous
subclass to throw. Rust `FetchCollector` is a concrete struct (no inheritance).
To translate the BEHAVIOR (the `collect_fetch` loop's reaction to an
`initialize` failure and the resulting queue state) without a production change
to the hot path, we add a `#[cfg(test)]`-only field
`force_initialize_error: Option<...>` consulted at the very top of `initialize`.
This is gated behind `#[cfg(test)]` so it is **compiled out of release builds**
— zero production code-size/perf impact, mirroring the project's established
`#[cfg(test)]` accessor/injection pattern (e.g. CommitRequestManager diff
trackers). The non-test constructor path is unchanged.

Rationale for hook vs. real fixture: Java's `initialize` throwing a raw
`RuntimeException`/`KafkaException` is precisely a Mockito/subclass artifact —
there is no real partition-data input that makes `initialize` throw a *bare*
`RuntimeException`. The contract under test is the `collect_fetch` queue-state
bookkeeping (`recordCount==0 ⇒ poll()s the empty entry; recordCount>0 ⇒ leaves
it`), which is what we assert. The injected error reproduces the throw at the
same lifecycle point.

## testCorruptedMessage assertion decision (vs Java, with rationale)

Java asserts the full structured `RecordDeserializationException`:
`origin` (KEY/VALUE), `offset`, `topicPartition` (topic + partition),
`timestamp`, raw `keyBuffer`, raw `valueBuffer`, and `headers`.

Rust collapses deserialization failures to `KafkaError::Serialization(String)`
(see `kafka_error.rs:274`). This string carries:
- **origin** — "KEY"/"VALUE" (asserted)
- **partition** — `topic-partition` string e.g. `test-0` (asserted)
- **offset** — embedded in the message (asserted)

The collapsed `Serialization(String)` type **cannot** carry, and the Rust tests
therefore **do NOT** assert:
- **timestamp** — not present in the error.
- **raw key/value buffers** — not present (the byte slices are not retained in
  the error string; only a human-readable cause).
- **headers** — not present.

This is the documented reduction (report-01 Key finding #6). We do NOT change
the error type to carry these fields because:
1. It is not a fidelity *bug* — the consumer behavior (which call raises, the
   cached re-raise, KEY-vs-VALUE classification, offset, partition) is correct
   and fully asserted. Only the error's *introspection surface* is reduced.
2. A `RecordDeserializationException`-equivalent struct holding owned
   key/value `Vec<u8>` + `RecordHeaders` would only allocate on the *error*
   path (not per good record), so it is not a hot-path perf regression — but it
   is a non-trivial production change (new error variant threaded through
   `wrap_deserialization_error`, the `Deserializer` failure sites, and every
   `KafkaError::Serialization` matcher) that is out of scope for a test-parity
   phase and not justified by a behavioral defect.

We STRENGTHEN beyond the current string-match by additionally asserting the
partition string and offset for both KEY and VALUE cases, and the cached
re-raise.

## Documented skips (OUT_OF_SCOPE)

- Fetch metrics (lead/lag/quota/response-metrics): no Rust metrics framework in
  Milestone-8 (report-01 Key finding #2). Not in this phase.
- The Mockito `verify(fetchBuffer).setNextInLineFetch(null)` assertions are
  translated as observable buffer state (`!has_next_in_line_fetch()`), not as
  call-count verification (Rust has no Mockito).

## Verification

After each commit group: `cargo build`, `cargo test --lib`,
`cargo xtask lint`, `cargo xtask format-check` — all green.

## Commit groups

1. "Phase 36: collector init/not-assigned" — testErrorInInitialize (+hook),
   update_partition_state per-branch, null-position, OOR-null-position,
   OOR-offset-reset.
2. "Phase 36: collector errors/txn" — testFetchWithOtherErrors expansion,
   testReadCommittedWithAbortedTransaction.
3. "Phase 36: completed-fetch corrupted+txn" — strengthen corrupted-message,
   add direct aborted/committed transaction tests.

## Production change

NONE. The only non-test addition is a `#[cfg(test)]`-gated `initialize`-error
injection field on `FetchCollector`, compiled out of release builds.
</content>
</invoke>
