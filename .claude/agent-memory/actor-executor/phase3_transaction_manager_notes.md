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

1. **A "state X is unreachable" claim must be checked against every writer of
   the state, not the entry-point guards.** The plan said `ABORTABLE_ERROR` is
   unreachable without a `transactionalId`, derived from `ensureTransactional()`
   / `if (isTransactional())` on the transaction-only entry points. But
   `InitProducerIdHandler.handleResponse` (Java 1524) calls `abortableError` for
   `CLUSTER_AUTHORIZATION_FAILED` with **no** such guard, and that code is what
   an idempotent producer gets when it lacks `IdempotentWrite`. Recorded as
   PLAN §9.15. Same failure shape as §9.8 (shared blind spot, invisible to
   repetition).

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
