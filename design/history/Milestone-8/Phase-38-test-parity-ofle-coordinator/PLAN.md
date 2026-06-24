# Phase 38 — Test parity: OffsetForLeaderEpochClient + CoordinatorRequestManager

Actor 38. Test-parity effort closing the small remaining fidelity gaps in report
`design/current/test-translation-review/03-commit-offsets-coordinator.md` for two
files. TEST-ONLY preferred; one pure perf-neutral refactor in production (see below).

## Scope (from report 03)

### OffsetForLeaderEpochClientTest (5 Java tests)
- `testUnexpectedEmptyResponse` — **ALREADY COVERED** by Phase 31's
  `handle_response_requested_partition_absent_stays_in_retry`
  (offsets_for_leader_epoch_client.rs). Asserts the requested-but-absent partition
  stays in `partitions_to_retry` and `end_offsets` empty. Verified faithful; not
  duplicated.
- `testEmptyResponse` — was REDUCED (covered conceptually, not standalone). ADDED
  `handle_response_empty_request_and_response_are_both_empty`: empty request +
  empty response ⇒ both `partitions_to_retry` and `end_offsets` empty. Distinct
  from the unexpected-empty case (which seeds a requested partition).
- `testOkResponse` / `testUnauthorizedTopic` / `testRetriableError` — PRESERVED
  (pre-existing), not touched.

### CoordinatorRequestManagerTest (9 Java tests)
- `testMarkCoordinatorUnknownLoggingAccuracy` — was CHANGED (counters only; exact
  warning string content dropped). FIXED: now asserts the EXACT formatted warning
  string content (the millis-since value: 60000 at one minute, 120000 at two
  minutes), mirroring Java's `LogCaptureAppender` + `millisecondsFromLog` regex
  parse. See production refactor below.
- `verifyNoInteractions(backgroundEventHandler)` (in `testBackoffAfterRetriableFailure`)
  — had no direct counterpart. ADDED assertion: after a retriable FindCoordinator
  failure, NO fatal error is recorded. Java's manager (like Rust's) holds no
  `BackgroundEventHandler`; `verifyNoInteractions` confirms nothing is emitted. The
  observable Rust equivalent is `fatal_error().is_none()` — only a fatal error is
  later propagated as an `ErrorEvent` by the heartbeat manager.
- All other 8 tests — PRESERVED/faithful per report; not touched.

## Production change (1) — perf/CPU-neutral, Java-fidelity

`coordinator_request_manager.rs` `mark_coordinator_unknown_inner`: extracted the
inline warning-message construction into a pure static helper
`disconnect_warning_message(duration_ms, curr_min, total_min) -> Option<String>`.

- Java line: `CoordinatorRequestManager.java:177-179` — the `if (currDisconnectMin
  > totalDisconnectedMin) { log.warn("... for {}ms", durationOfOngoingDisconnectMs); ... }`.
- Why: the report flags that the exact warning content (DoD §3) is untested because
  there is no log-capture facility. Per the worklist, the chosen approach is to test
  the message-formatting helper directly (no heavy logging dependency). Factoring the
  decision + string into one pure function lets the test assert the exact string the
  production path logs.
- Perf justification: identical predicate and identical `format!` call as before —
  the same single allocation only on the (rare, ≤ once/minute, off the per-record /
  per-RPC hot path) warning branch. No new allocation, no extra work on any hot path
  (CLAUDE.md §11). The `log::warn!` still fires with the same string.

No other production changes.

## Skips / already-covered (rationale)
- `testUnexpectedEmptyResponse`: already covered by Phase 31 — not duplicated.
- OFLE async `sendAsyncRequest` future-resolution wiring: not re-exercised here
  (pure helpers tested directly), acceptable per §1/§10 and already noted in report.

## DoD
- Mirror Java names snake_case + comment citing the Java test.
- Assert error/message content (logging-accuracy: exact string; OFLE: structural).
- Mutation-resistant: logging test asserts the literal millis and full wording;
  empty-response test asserts both collections empty for the no-partition case.
- cargo build / test --lib / xtask lint / xtask format-check all green.

## New test count
- OFLE: +1 (`handle_response_empty_request_and_response_are_both_empty`).
- Coordinator: +0 new fns; 2 existing tests strengthened
  (`test_mark_coordinator_unknown_logging_accuracy` now asserts exact string;
  `test_backoff_after_retriable_failure` now asserts no-fatal-event).
