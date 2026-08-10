---
name: m11-phase3-transaction-manager
description: Milestone-11 Phase 3 (TransactionManager idempotence core) — reachability-audit pattern, Caller enum threading, InFlightBatchPool per-partition keying, Sender-substitute test harness
metadata:
  type: project
---

Milestone 11 Phase 3 landed `src/producer/internals/transaction_manager.rs`
(idempotence slice of `TransactionManager.java`), 4 commits on
`milestone11-producer-transactions`.

**Why:** the producer advertised `enable.idempotence=true` and delivered
at-least-once. Phase 3 is the state machine; Phase 4 wires it to the send path.

**How to apply** — five reusable lessons:

1. **A reachability correction has two halves — the entries AND the exits — and
   the second is the one that gets forgotten.**

   *Entries:* a "state X is unreachable" claim must be checked against every
   writer of the state, not the entry-point guards. The plan said
   `ABORTABLE_ERROR` is unreachable without a `transactionalId`, derived from
   `ensureTransactional()` / `if (isTransactional())` on the transaction-only
   entry points. But `InitProducerIdHandler.handleResponse` (Java 1524) calls
   `abortableError` for `CLUSTER_AUTHORIZATION_FAILED` with **no** such guard,
   and that code is what an idempotent producer gets when it lacks
   `IdempotentWrite`.

   *Exits:* the first fix added only the three methods that enter the state, so
   the translated state machine had no exit from `ABORTABLE_ERROR` — from Phase 4,
   once the Sender wires it in, `maybe_add_partition` would reject every send
   forever. Critic 43 issue 1 found it. `Sender.java:325` →
   `shouldHandleAuthorizationError` (`:351-360`) → `failPendingRequests` +
   `maybeAbortBatches` + `transitionToUninitialized` is Java's recovery, and it
   always fires idempotently because the only entry sets `lastError` to exactly
   the exception the `instanceof` matches. That is *why* the table has
   `UNINITIALIZED ← ABORTABLE_ERROR`: an unexplained arm in a transition table is
   a hint that a path exists that you have not traced.

   Two corollaries worth reusing: an all-green suite proves nothing about an exit
   path nobody wrote a test for, so pin the exit with a mutation
   (no-op the exit method, watch the test fail); and when a phase's plan schedules
   an exit method for a *later* phase than its entry, that mismatch is the smell.
   Recorded as PLAN §9.15; a DoD clause for this is proposed in
   `COMMENTS.DONE.43.md`. Same failure shape as §9.8 (shared blind spot,
   invisible to repetition).

   *Third half, learned the hard way over three rounds:* when a method is pulled
   into a phase earlier than its call site, **do not reach for a present-tense
   behavioural payoff**. `close`'s justification was wrong twice — first a hanging
   future, then a `FATAL_ERROR` transition "stopping a later `runOnce`" — because
   each time the instinct was to find some effect *in this phase* rather than
   conclude there is none. There often is none, and "unscheduled anywhere, N lines,
   no later-phase dependency, call site reachable, payoff in phase M" is a complete
   justification on its own. Phrase such claims in the tense of the phase that
   makes them true, and grep the phase's own records for "stops/prevents/would
   leave" before declaring the phase done.

2. **When a plan clause contradicts itself, prefer the faithful translation and
   say so in the PLAN.** "only the 4 reachable states" + "the full 9-variant
   transition table" cannot both hold. Declaring all nine is 1:1 with Java and
   makes the next phase additive.

3. **`InFlightBatchKey` is NOT partition-scoped.** `(producer_id, epoch,
   base_sequence)` collides across partitions routinely (two partitions both at
   sequence 0). Any batch pool handed to a `TxnPartitionEntry` must therefore be
   keyed by partition — hence `InFlightBatchPool =
   HashMap<TopicPartition, Vec<&mut ProducerBatch>>`. A flat slice would let one
   entry rewrite another partition's batch.

4. **Test-harness substitution when the transport phase is not landed yet:**
   don't skip the test, drive the same manager path directly. Java's
   `initializeIdempotentProducerId` spins `Sender.runOnce` against `MockClient`;
   the Rust helper calls `next_request()` (what `Sender.java:472` calls) and
   `on_complete()` (what `NetworkClient.poll` calls) with a hand-built
   `ClientResponse`. Identical manager code, no Phase-4 dependency. Same for
   `runUntil(.. epoch == N)` → call `maybe_resolve_sequences()` then
   `bump_idempotent_epoch_and_reset_id_if_needed()` in `Sender.runOnce` order.

5. **Mutation-check the finding, not just the code.** Two mutations proved the
   tests are load-bearing: skipping a queued partition with no in-flight batches
   fails 3 tests; deleting `INITIALIZING` from the `AbortableError` transition
   arm fails the cluster-authorization test — which is the *evidence* for
   finding 1, not just a passing assertion.

**Environment:** `make verify` cannot complete on this machine —
`build-python`'s C extension includes `<threads.h>`, absent from the Apple SDK
(exit 2). Use `make verify-sandbox` (needs Docker). Its parallel integration run
is load-flaky: a first run failed 7 tests on rebalance/latency deadlines, all 7
passed serially, and two later full runs were clean.
