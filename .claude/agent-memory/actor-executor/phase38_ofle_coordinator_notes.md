---
name: phase38-ofle-coordinator-notes
description: Phase 38 test parity — log-message-content assertion via extracted pure helper, verifyNoInteractions(BEH) == fatal_error().is_none(), testUnexpectedEmptyResponse already in Phase 31
metadata:
  type: project
---

# Phase 38 — OFLE + CoordinatorRequestManager test parity (report 03)

Test-only phase (Actor 38) on `consumer-impl`. Closed the two small report-03
gaps. Patterns worth reusing:

## Asserting EXACT log-message content with no log-capture crate
When a Java test uses `LogCaptureAppender` + a regex on the logged string (here
`testMarkCoordinatorUnknownLoggingAccuracy`'s `millisecondsFromLog`), extract the
inline message construction into a PURE static helper that returns the decision +
string (`disconnect_warning_message(duration, curr_min, total_min) -> Option<String>`),
then assert on that helper. The production `log::warn!("{message}")` calls it, so
the test asserts the exact string the prod path logs. This is a legit perf-neutral
production refactor (same predicate, same single `format!`, only on the rare
off-hot-path warning branch) — call it out with the Java line (CoordinatorRequestManager.java:177-179).
Test parses the helper output by stripping the literal prefix/suffix and parsing
the middle so WRONG WORDING (not just wrong number) fails the test.

## verifyNoInteractions(backgroundEventHandler) translation
Java's `CoordinatorRequestManager` has NO `backgroundEventHandler` field — the mock
is passed to setup but the real manager never touches it. `verifyNoInteractions`
just confirms nothing is emitted. Rust equivalent on a RETRIABLE FindCoordinator
failure: assert `manager.fatal_error().is_none()` (only a fatal error is later
propagated as an ErrorEvent by the heartbeat manager; a retriable error is logged
and dropped).

## testUnexpectedEmptyResponse was ALREADY done by Phase 31
`handle_response_requested_partition_absent_stays_in_retry` (offsets_for_leader_epoch_client.rs)
already covers it faithfully. Don't duplicate. Only `testEmptyResponse` standalone
(empty request + empty response ⇒ both collections empty; partitions_to_retry seeds
from request keys so 0 requested ⇒ nothing to retry) was newly added.

## See also
- [[phase31_reset_validate_test_notes]] — OFLE response-driving helpers, ORM reset/validate
