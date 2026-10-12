# Critic 62 — Milestone-13 Phase 2 (producer) — RESOLVED

Range reviewed: `0ee0bda5..HEAD` = commits `dfa0090e` (2PC-revert internals +
await overload) and `fad98fca` (SENDER_TIMEOUT_MSG assertion).

---

## Issue 1: `test_drop_commit_on_batch_expiry` drops the 4.3.1 commit-cause message assertion behind an inaccurate comment — RESOLVED
- **File**: `src/producer/internals/sender.rs` (`test_drop_commit_on_batch_expiry`)
- **Severity**: Missing Requirement (test fidelity) — Low
- **Java Reference**: `clients/.../internals/TransactionManagerTest.java:2984-2986` (4.3.1)
- **Description**: AK 4.3.1 added a second SENDER_TIMEOUT_MSG assertion to
  `testDropCommitOnBatchExpiry`, on the commit result's error cause. The Rust
  test asserts only `error.error() == Errors::TransactionAbortable` and never
  inspects the message. The old code comment falsely claimed the cause's message
  "is checked inside it."
- **Fix**: Replaced the comment with an accurate skip-with-reason. The second
  assertion is not translatable: batch expiry flows through
  `maybe_transition_to_error_state` (transaction_manager.rs:2643), whose
  retriable arm replaces the batch-expiry message with the fixed "Transaction
  Request was aborted after exhausting retries." `RequestTimedOut` is retriable,
  so the flat `KafkaError`'s message does not contain `SENDER_TIMEOUT_MSG`, and
  the timeout is not preserved as a cause (no cause chain — flat-error
  consequence per producer-transactions.md §10.5 deviation 5). The comment now
  cites Java:2986 and notes the record-future assertion (Java's first check) is
  still covered by `assert_produce_future_expired(..., EXPIRED_BATCH_MESSAGE_TP0)`.

## Issue 2: `testAppendInExpiryCallback` 4.3.1 delta unaddressed (pre-existing untranslated test) — RESOLVED
- **File**: `src/producer/internals/sender.rs`
- **Severity**: Completeness note — Low (pre-existing, NOT a Phase-2 regression)
- **Java Reference**: `clients/.../internals/SenderTest.java:414-430` (4.3.1)
- **Description**: `SenderTest.testAppendInExpiryCallback` is not translated on
  this branch (pre-existing M8 skip). Its sole 4.3.1 delta is the
  SENDER_TIMEOUT_MSG assertion at Java:430.
- **Fix**: Recorded both skips as a "Recorded skips" bullet list under Phase 2
  in `design/history/Milestone-13/PLAN.md`, noting that the behavior is covered
  by the four translated batch-expiry tests.
