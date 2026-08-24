---
name: phase8-parity-sweep-notes
description: Milestone-11 Phase 8 — MockClient RequestMatcher was the unlock for the 140-method TransactionManagerTest sweep; two defects surfaced (one fixed, one filed); "blocked on missing surface" notes expire
metadata:
  type: project
---

Milestone 11 Phase 8 closed `TransactionManagerTest` (140/140) and the `SenderTest`
hand-off. The reusable lessons, in the order they cost time:

**A missing test-harness capability can masquerade as a translation problem.** The 47
owed `TransactionManagerTest` methods looked like they needed production surface. They
needed `MockClient::prepareResponse(RequestMatcher, ..)` / `respond(RequestMatcher, ..)`
— real Java `MockClient` surface this port had skipped. Every `TransactionManagerTest`
`prepare*` helper puts its assertions *inside* the matcher, so without matchers the
translations would have compiled, passed, and asserted much less than Java.

**Why:** when a Java test class's helpers are all `client.prepareResponse(matcher, resp)`,
the matcher *is* the test. Check the mock's surface before concluding the production code
is missing something.

**How to apply:** before starting a large test-parity sweep, diff the Java test class's
helper list against the Rust mock's method list. Two gaps turned up this way
(`RequestMatcher`, `disconnect(node, allowLateResponses)`).

**"Blocked on missing surface" notes expire — re-verify, don't re-read.** Two entries were
recorded as blocked. One (`testTransactionalSplitBatchAndSend`, PLAN §9.18) still was;
running the reproducer confirmed it in seconds. The other
(`testSenderShouldCloseWhenTransactionManagerInErrorState`) was not: the note itself had
named two possible routes, and the second route already existed *and was already exercised
by a test in the same file under a Rust-only name*. Naming it after the Java method plus
one `#[cfg(test)]` call counter closed it.

**How to apply:** treat a blockage note as a claim with a shelf life. Look for the surface
it says is missing; do not re-read the note and believe it.

**A rarity claim used to justify a deferral is a factual claim about a code path.** The
consumer's `containsAbortMarker` deferral was justified with "production readers will hit
it only if their producers reuse producer IDs after an abort, which is rare". A producer id
is stable across a producer's transactions, so abort-then-commit reuses it by construction —
i.e. always. It survived four phases because no test in either the producer or consumer
suite reached the branch. PLAN §9.27.

**How to apply:** when deferring with a frequency argument, check the *client* behaviour the
argument is about. One look at where the value is assigned settles it.

**Fix-or-file, and the criterion that decides.** Two defects surfaced. The consumer one was
fixed because an integration test proved an ordinary case broken and the fix was contained
(translate one class + one method). The producer one (PLAN §9.25, `Sender::can_retry`
passing an empty batch pool) was filed because the fix needs a signature change on the
produce-response path — `can_retry` takes the failing batch by `&` and the pool by `&mut`,
and the batch is still tracked at that point, so including it aliases. The §9.18 precedent
is the yardstick: a send/receive-path change with its own DoD §10 audit does not belong in
a transactions *test* phase. Both times the reproducer was left in place and `#[ignore]`d
with its section cited.

**Re-run shipped verification sweeps; they earn their keep.** The rustdoc-header sweep in
`sender.rs`'s accounting block caught two of this phase's *own* citation ranges off by one
at the end — both because the Java body is wrapped in `try (Metrics m = ..)` whose
`        }` precedes the real `    }`. Re-running a sweep whose count you are about to
update is not ceremony.

**Counts from memory are the mistake the accounting blocks exist to prevent.** I wrote
"2 `#[ignore]`d tests" into `design/current/status.md` from memory of this phase's own two.
`cargo test -- --ignored --list` found a third, pre-dating the milestone. Enumerate.
