---
name: review-m9-phase5-patterns
description: M9 Phase 5 ShareConsumeRequestManager review — timer-reset now=0 bug, per-node ack slot routing, undocumented single-node test drop
metadata:
  type: project
---

Milestone 9 Phase 5 (`share_consume_request_manager.rs`, ~4945 lines incl. tests;
Java 1571). Findings and durable review heuristics:

**BUG found — `now_ms = 0` hardcode in a reset path.** Java
`TimedRequestState.resetTimeout(timeoutMs)` = `timer.updateAndReset(timeoutMs)`
which sets deadline = *current time* + timeoutMs. Rust `processing_complete()`
called `maybe_reset_timer_and_request_state(0)` → `reset_deadline(0 + timeout_ms)`
= absolute `timeout_ms` (~60000), i.e. distant past for a real clock. Latent
because `reset()` zeroes `num_attempts` and `maybe_expire = num_attempts>0 &&
is_expired`; bites only on the *reused* COMMIT_ASYNC state's first retry.
**Heuristic:** any `reset_deadline`/`reset_timer` translation of Java
`updateAndReset`/`resetTimeout` MUST use the current time, never 0/a constant.
When a Rust method drops the `Time` param Java threads, check every deadline
recomputed inside it. **Near-zero MockClock masks all absolute-vs-relative-time
bugs** — tests passing does not clear this class; reason about production
`SystemTime` (~1.7e12 ms).

**Per-node in-flight-ack slot routing (design deviation, verified SOUND).** Java
captures `this` (the exact AcknowledgeRequestState) in the `whenComplete` lambda.
Rust can't (poll takes &mut self), so it resolves the state at response time via
`find_in_flight_ack_slot`: async/sync matched by non-empty `in_flight_acks`,
close matched by `!is_processed` (a close can be in flight with zero acks). Unique
because `nodes_with_pending_requests` enforces one-in-flight-per-node AND
async/sync-in-flight ⟹ non-empty in_flight. A not-yet-sent close (is_processed
false) can't be mis-picked because no response arrives unless something was sent.

**Test-fidelity heuristic that paid off:** diff Java `@Test`/`@ParameterizedTest`
names against Rust `fn test_*`, then cross-check the delta against the Actor's
*documented* deferral list. Here 9 Java tests were untranslated; 8 documented, but
`testPiggybackAcknowledgementsOnInitialShareSessionErrorSubscriptionChange` was
silently dropped — and it is SINGLE-node (only tp0), so the "multi-node
LinkedHashSet order" blanket rationale did not cover it. It uniquely tests
`poll_fetch`'s *second* loop (session handlers holding acks for dropped
partitions) hitting `maybe_add_acknowledgements(is_new=true)` →
INVALID_SHARE_SESSION_EPOCH. Lesson: verify each deferral individually; a
single-node test hiding among multi-node deferrals is a real coverage gap.

**Deferred multi-node/leadership tests were legit** — but only confirmable by
reading the PRODUCTION multi-node routing + `update_partition_leadership` path and
finding it faithful. Deferring the *test* for harness-ordering reasons is fine
iff the production code it would exercise is independently verified correct.

**Disconnect deviation:** ack-path disconnect → Rust `NetworkException`
(retriable) vs Java `Errors.forException(DisconnectException)` = UnknownServer
(non-retriable). Control flow unaffected (no retry on that path); only the
user-callback exception + its is_retriable flips. Documented, non-blocking.
