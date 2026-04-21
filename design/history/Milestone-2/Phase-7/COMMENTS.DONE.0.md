# Phase 7 Review -- Critic 0 (RESOLVED)

All 5 issues resolved in commit 043f258.

---

## Issue 1: reenqueue_batch does not actually re-enqueue the batch into the accumulator -- RESOLVED

- **File**: `src/clients/producer/internals/sender.rs`
- **Severity**: Bug
- **Resolution**: Introduced `BatchAction` enum (Done, Reenqueue, SplitAndReenqueue) returned by `complete_batch`. The caller, which owns the batches, processes these deferred actions and transfers ownership to `accumulator.reenqueue()`. The broken `reenqueue_batch` method was removed.

---

## Issue 2: split_and_reenqueue not implemented -- MESSAGE_TOO_LARGE silently drops records -- RESOLVED

- **File**: `src/clients/producer/internals/sender.rs`
- **Severity**: Bug
- **Resolution**: Added `BatchAction::SplitAndReenqueue` variant. When MESSAGE_TOO_LARGE is received for a multi-record batch, `complete_batch` returns `SplitAndReenqueue` and the caller passes the owned batch to `accumulator.split_and_reenqueue()`, which splits and re-enqueues sub-batches.

---

## Issue 3: send_produce_request marks ALL in-flight batches per TP as inflight, not just the ones being sent -- RESOLVED

- **File**: `src/clients/producer/internals/sender.rs`
- **Severity**: Behavior Mismatch
- **Resolution**: Changed the inflight marking loop to only mark the last batch per TP (the one just added by `add_to_inflight_batches`), matching Java behavior where `batch.setInflight(true)` is called on each specific batch as it is added to the request.

---

## Issue 4: Missing non-transactional SenderTest translations -- RESOLVED

- **File**: `src/clients/producer/internals/sender.rs` (tests section)
- **Severity**: Missing Requirement
- **Resolution**: Added 5 new tests: `test_retries`, `test_send_in_order`, `test_no_double_deallocation`, `test_reset_next_batch_expiry`, `test_node_latency_stats`. Three Java tests (`testNodeNotReady`, `testTooLargeBatchesAreSafelyRemoved`, `testSenderShouldRetryWithBackoffOnRetriableError`) require `TransactionManager` and are deferred to transaction support.

---

## Issue 5: Unnecessary double-copy of record data in produce request path -- RESOLVED

- **File**: `src/clients/producer/internals/sender.rs`
- **Severity**: Design Flaw
- **Resolution**: Changed `for info in &batch_infos` to `for mut info in batch_infos` (consuming by value) and used `info.records_data.take()` instead of `.clone()`, eliminating the second copy.
