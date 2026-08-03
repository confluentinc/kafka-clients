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

Related: [[review-notify-waiters-race]]
