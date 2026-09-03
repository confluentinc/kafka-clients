---
name: m13-phase2-producer-notes
description: Milestone-13 Phase 2 producer 2PC-revert + AK 4.3.1 message deltas — what had a Rust counterpart vs skipped
metadata:
  type: project
---

Milestone-13 Phase 2 (AK 4.2.0→4.3.1 producer delta, agent 62). The 2PC public-API
revert (kafka c41ff4de0e) is a *revert*: the public 2PC methods
(`initTransactions(boolean)`/`prepareTransaction`/`completeTransaction`) came and
went **between** the tags, so the 4.2.0..4.3.1 tree diff shows NO public-method
removals on Producer/KafkaProducer/MockProducer — Rust never had them.

**Why:** what remained with a Rust counterpart, all in one commit (coupled by the
await-signature change):
- `TransactionManager`: added private `TransactionOperation` enum (Java 208-225);
  `throw_if_pending_state` takes it by value instead of `&str`. Display returns the
  same displayNames → message byte-identical.
- `TransactionalRequestResult.await`: 3 overloads consolidated to one. Rust
  `await_result_timeout(timeout, expected_timeout_reason)` appends ". {reason}".
  Kept the no-arg `await_result` as the Rust await-forever primitive (Java's removed
  no-arg was only a convenience wrapper; the ~55 test `.await_result()` sites stay).
  Only the ~8 `await_result_timeout(Duration)` sites needed the 2nd arg.
- `KafkaProducer`: pass 4 timeout-reason consts (INIT/SEND_OFFSETS/COMMIT/ABORT);
  **removed** `throw_if_in_prepared_state` + its 3 call sites (begin_transaction,
  do_send, do_send_bytes) — mirrors Java deleting `throwIfInPreparedState`. Net send-path
  win: one fewer mutex lock. `prepare_transaction`/`is_prepared` stay on TransactionManager.
- `Sender`: expiry TimeoutException message gained ". The request has not been sent,
  or no server response has been received yet." suffix (updated EXPIRED_BATCH_MESSAGE
  consts which assert the FULL message).

**How to apply / skipped:** MockProducer's `TimeoutException("...injected for test.")`
change is in `clientInstanceId(Duration)` — a KIP-714 telemetry method the Rust mock
DEFERS (PLAN §9.23, out-of-scope per §1.1), so it cannot be asserted; skip-with-reason.
Everything else (ProducerConfig, RecordMetadata, BufferExhaustedException, ProducerBatch,
RecordAccumulator, ProduceRequestResult, Producer.java, TxnPartitionEntry, all 3
test files MockProducer/ProducerBatch/RecordAccumulator) is import-only (Phase 1
record→record::internal) or javadoc-only — Rust rustdoc already equivalent. Message
assertions asserted the FULL new text (stronger than Java's contains).
