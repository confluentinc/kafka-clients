---
name: review-m11-phase1
description: Milestone 11 Phase 1 (producer txn support types) review patterns — caller-supplied-collection rebuild, guard placed in fewer constructors than planned, exception-hierarchy flattening
metadata:
  type: feedback
---

Highest-value checks discovered reviewing the producer transaction support types
(`TxnPartitionEntry`, `TxnPartitionMap`, `TransactionalRequestResult`,
`ProducerIdAndEpoch`, `TransactionResult`, idempotence config validation).

**1. Java rebuilds a collection from itself; Rust rebuilds it from a caller
argument.** When Rust cannot own the elements (non-`Clone` type owned elsewhere)
and takes `&mut [&mut T]` instead, check whether the *membership* of the tracked
collection is preserved. Java's `for (X x : myCollection) { mutate(x); newSet.add(x); }`
keeps elements invariant. A Rust loop over the caller's slice makes membership
equal to whatever the caller passed — an empty slice silently clears the set, a
superset silently adds. Grep for `self.<field> = new_<field>;` where
`new_<field>` was built from a parameter.

**Why:** the failure is silent state corruption, not a panic, and the covering
tests are phases away. To prove it will bite, trace the *only* owner that can
supply the argument and compare its lifecycle against the tracked set's — in
this codebase `Sender::in_flight_batches` is deliberately removed at a
*different* point than the txn map's set (`Sender.java:846` calls
`handleFailedBatch` before `:854` removes from `inFlightBatches`, while
`TransactionManager.java:790` already removed from the txn set), so a verbatim
hand-off is wrong.

**2. Approved plan says "guard in A and B"; implementation guards only A.**
Check every constructor/entry point the plan named. The rationale "B returns
`Self` not `Result` so it can't error" is not a constraint — it's a design
choice. Check whether B is `pub` and whether its own rustdoc contradicts the
"test-only plumbing" claim.

**3. "No typed error struct needed — the subclass carries no extra payload" is
only half the test.** Also check whether the Java *hierarchy* is used for
dispatch. `UnknownProducerIdException extends OutOfOrderSequenceException`
(`TransactionManager.java:799` vs `:806`) and
`TransactionalIdAuthorizationException extends AuthorizationException` both
matter; flat `Errors` codes are unrelated peers, so an `if/else if` chain
translates wrongly.

**4. Doc comment stranded by an insertion.** When new `pub fn`s are inserted into
an `impl` block, check the item immediately *after* the insertion point still has
its doc comment. `git diff` shows this as a `+` block wedged between a `///` run
and its `fn`.

**5. Log level fidelity.** `kafka_debug!` / `kafka_trace!` / `kafka_info!` all
exist; check each translated log site against Java's `log.<level>`. A file that
gets one right and one wrong is a slip, not a convention.

**How to apply:** for any Milestone-11 phase, run these five before reading
anything else. Also verify plan-override rationales against the source rather
than accepting them: in Phase 1 both overrides (non-`Clone` `ProducerBatch`
blocking the `BTreeMap<_, ProducerBatch>` shape; `TxnPartitionEntry.decrementSequence`
not wrapping) were **correct and the approved plan was wrong**.

## Second-pass (fix-verification) lessons

**6. `pub fn` whose parameters are `pub(crate)` types is NOT externally
reachable.** Before calling anything a "public bypass", check argument
constructibility: can an outside caller *name* the parameter types, and is there
any `pub fn` returning a value of that type (which would let inference dodge the
naming problem)? In Phase 1 I wrongly flagged `KafkaProducer::with_client` — its
`Arc<ProducerMetadata>` / `Arc<RecordAccumulator>` params live under
`pub(crate) mod internals`, and nothing public returns them. Recorded as a false
positive in `COMMENTS.FP.md`. The *documentation* half of that finding was real,
so the pattern is: split reachability claims from doc/record-keeping claims.

**7. "Owned by X" is a lifecycle claim, not a static one — check every ownership
transfer.** Rules §7 asserted `Sender::in_flight_batches` "becomes the sole
owner" of drained batches. False on the retry path: `Sender.reenqueueBatch`
(`Sender.java:750-752`) hands the batch back to the accumulator and removes it
from `inFlightBatches`, while the txn map keeps tracking it — Java *asserts* this
at `RecordAccumulator.java:558-560`. So Java has two different meanings of "in
flight" (`transactionManager.hasInflightBatches` reads the txn map;
`Sender.inFlightBatches` is the Sender's list) that diverge exactly there. When
an Actor's call-site audit concludes "safe because the Sender holds them",
enumerate the ownership *transfers* (reenqueue, split-and-reenqueue, expiry,
abort/clear), not just the removal points.

**8. A fix that adds strictness needs the reachability audit checked in the
opposite direction.** Turning a silent divergence into an `Err` is only an
improvement if the `Err` cannot fire on a legitimate path. Look for the matrix
cell the audit declared unreachable — that is the cell with no regression test,
by construction.

## Third-pass lessons

**9. Derive Java line numbers with `grep -n`, never by counting inside `sed`
output.** My first two passes cited `Sender.java:846`/`:684-685`/`:749-753` and
`RecordAccumulator.java:556-558`; the authoritative numbers are `:848`/`:686`/
`:750-752` and `:558-560`. The submodule had not moved (`a18251b` throughout) — I
had miscounted. Off-by-two citations in a review look like a submodule drift
scare and cost a round-trip to disprove. Also: check
`git submodule status kafka` before concluding the Java source changed.

**10. When an Actor keeps one error path non-atomic and makes another atomic,
verify the Java behaviour for *both* before flagging inconsistency.** In
`TxnPartitionEntry` the negative-sequence path must stay non-atomic
(`TxnPartitionEntry.java:154-161` — the lambda throws mid-iteration, earlier
elements stay mutated, the set swap is skipped) while the Rust-only missing-batch
check should be atomic (Java has no counterpart). "Fixing" the first would have
been the divergence. Asymmetric atomicity can be the correct answer.

**11. An atomicity regression test only discriminates if the missing element is
NOT first.** If the first tracked key is the unresolvable one, resolve-then-mutate
and mutate-as-you-go behave identically, so the test passes either way. Check
which position the test puts the missing element in before crediting it.

Related: [[review-notify-waiters-race]]
