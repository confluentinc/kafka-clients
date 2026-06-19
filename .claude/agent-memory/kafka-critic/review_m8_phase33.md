---
name: review-m8-phase33
description: Phase 33 CommitRequestManagerTest parity + 4 prod fixes — perf-neutrality audit method, supplier-collapse under-assertion, by-ref vs clone
metadata:
  type: project
---

Phase 33 (CommitRequestManagerTest +45 tests, CRM 28→73) shipped 4 production
Java-fidelity fixes alongside tests. Review method that worked:

**Perf-neutrality audit for test-driven prod fixes**: classify each prod change
by call granularity, not by whether it allocates. CRM commit/fetch RPCs are
per-RPC (auto-commit interval default 5s; OffsetFetch is committed()/position-
init driven) — NOT per-record/per-poll, so they sit OUTSIDE CLAUDE.md §11's
hot-path definition. A String/HashMap clone there is negligible; don't flag it
as a steady-state regression. Only the fetch/poll loop is the tuned hot path.

**Supplier-collapse under-assertion (real, recurring)**: when a Java
@MethodSource maps an error to a SPECIFIC exception subclass
(UnknownMemberIdException, StaleMemberEpochException) but the Rust supplier
collapses it to a generic `ExpectedClass::KafkaException` whose assert is a
permissive disjunction (`surfaced == source || surfaced == UnknownServerError
|| ...`), the `|| UnknownServerError` escape hatch removes the teeth: a
regression remapping to UnknownServerError still passes. Flag it. BUT first
confirm Java actually pins the class on THAT test — Java's `testNonRetriable`
only asserts `isCompletedExceptionally()` (no class), so the errored-requests
test legitimately skips the class while the timeout-requests test asserts it.

**by-reference vs clone divergence**: Java BiConsumers capture maps by
reference (autoCommitCallback(request.offsets)); the Rust translation must
`.clone()` to move into a spawned task. When the consumer is the empty/no-op
case (interceptors_empty short-circuit lives inside the invoker), the clone is
done unconditionally before the short-circuit → an allocation Java never pays.
Minor at per-RPC granularity; note it, gate it behind a has_X() probe if cheap.

**Send-time vs enqueue-time state**: Java request states often hold a REFERENCE
to mutable manager state (MemberInfo, CommitRequestManager.java:889), so a
mutation (onMemberEpochUpdated) between enqueue and send is reflected. A Rust
clone-at-enqueue silently freezes it. Fix = re-sync at send under the same lock
the bg-task already holds. testLastEpochSentOnCommit pins this: first Some(1)
assertion (member had no epoch at enqueue) fails if re-sync reverted.

Java refs that recur: CommitRequestManager.java:259/376 (autoCommitCallback),
:889 (MemberInfo reference), :947 (handleCoordinatorDisconnect on transport
error, shared by commit+fetch), :1257 (addOffsetFetchRequest chainFuture dedup),
:188 ("Failed to commit offsets: Coordinator unknown and consumer is closing").
