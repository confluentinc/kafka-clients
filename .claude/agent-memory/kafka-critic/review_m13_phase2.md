---
name: review-m13-phase2
description: M13 Phase 2 producer 2PC-revert + await-overload review; flat-error cause-message loss, string line-continuation byte-check, add+revert tree-diff nets zero
metadata:
  type: project
---

# M13 Phase 2 (producer, agent 62) review — CLEAN except 2 low findings

Commits `dfa0090e` (2PC-revert internals + `await` overload) + `fad98fca`
(SENDER_TIMEOUT_MSG assert). All 9 checklist items verified; build/format/clippy/
607 producer tests green. Findings both LOW (test-fidelity/comment).

**Why:** captures reusable heuristics for 4.2→4.3.1 producer-delta reviews.

**How to apply:**

- **await() overload consolidation**: Java 4.3.1 removed no-arg `await()` +
  2-arg `await(long,TimeUnit)`, keeping only `await(long,TimeUnit,String
  expectedTimeoutReason)`. Rust keeping a no-arg `await_result()` as the
  await-forever primitive is FAITHFUL **iff** no production caller uses it —
  verify by grepping `await_result(` minus `await_result_timeout`/`fn `; all hits
  must be in `mod tests`. Production KafkaProducer must use
  `await_result_timeout(.., CONST)` at all 4 sites (init→INIT, send_offsets→
  SEND_OFFSETS, commit(beginCommit)→COMMIT, abort(beginAbort)→ABORT).

- **Flat KafkaError discards Java's exception CAUSE** (PLAN §10.5 dev 5). A 4.3.1
  test asserting `assertThrows(X).getCause().getMessage().contains(MSG)` is only
  translatable if the Rust error carries MSG. WATCH the retriable→abortable
  conversion in `maybe_transition_to_error_state` (transaction_manager.rs ~2643):
  it REPLACES the message with the fixed `"Transaction Request was aborted after
  exhausting retries."` (mirrors Java `new TransactionAbortableException(fixedMsg,
  causeException)` at TransactionManager.java:796). So `RequestTimedOut`
  (retriable) batch-expiry, at the COMMIT-result level, loses SENDER_TIMEOUT_MSG.
  Record-future level KEEPS it (EXPIRED_BATCH_MESSAGE constant). testDropCommit-
  OnBatchExpiry has BOTH asserts — record-future one covered, commit-cause one
  legitimately un-translatable but was hidden behind a comment falsely claiming
  "the cause's message is checked inside it" → Issue 1.

- **Rust string line-continuation byte-check**: `"...foo " \`+newline+`   bar"`
  strips the newline AND next line's leading whitespace, keeping the trailing
  space before `\`. So a multi-`+` Java string concat round-trips to one space.
  Verified INIT/SEND_OFFSETS timeout constants + Sender expiry message +
  throw_if_pending_state message all byte-identical this way.

- **MockProducer 2PC "removals" net to ZERO in a tree diff**: c41ff4de0e ADDED
  then REVERTED 2PC between 4.2.0 and 4.3.1 tags, so `git diff 4.2.0..4.3.1 --
  MockProducer.java` shows NO 2PC `-` lines — the +83 is ~all javadoc on existing
  methods + one telemetry `TimeoutException("...injected for test.")` change. Do
  NOT expect deletion hunks; PLAN §2 "2PC removals" wording is misleading for a
  tree diff.

- **Item 7 telemetry adjudication (LEGITIMATE SKIP)**: the injected-TimeoutException
  message change lives entirely in `clientInstanceId(Duration)` (KIP-714), which
  Rust MockProducer doesn't implement (documented mock_producer.rs:107-110, PLAN
  §9.23; M13 §1.1 excludes telemetry). "assert message text" plan instruction N/A.

- **TransactionOperation enum**: private, `Copy`, Display returns displayName; only
  the CURRENT-operation placeholder in throw_if_pending_state uses the enum — the
  pending `operation` stays a `String`. 4 variants/4 call sites, all matched.

- **testAppendInExpiryCallback**: a pre-existing M8 skip (never carried onto this
  branch; was in squashed fixup 56e9c31a). Its sole 4.3.1 delta (one SENDER_TIMEOUT
  assert) is unaddressed but covered in spirit by the 4 batch-expiry tests. Flag as
  completeness note, NOT a Phase-2 regression → Issue 2.
