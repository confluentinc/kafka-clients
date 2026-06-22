# Critic 33 — Phase 33 review — RESOLVED items

## Issue 1 — OffsetFetch timeout test under-asserts the exception class for UNKNOWN_MEMBER_ID / STALE_MEMBER_EPOCH (test fidelity) — RESOLVED

Fix: added two specific `ExpectedClass` variants (`UnknownMemberId`,
`StaleMemberEpoch`) to the test matrix. `offset_fetch_exception_supplier()` now
maps `UNKNOWN_MEMBER_ID → ExpectedClass::UnknownMemberId` and
`STALE_MEMBER_EPOCH → ExpectedClass::StaleMemberEpoch`. `assert_fetch_error_class`
asserts the **exact** error code (`err.error() == Errors::UnknownMemberId` /
`Errors::StaleMemberEpoch`) for those two rows, dropping the
`|| surfaced == UnknownServerError` tolerance that previously let a wrong mapping
pass. The generic `KafkaException` arm (with the `UnknownServerError` tolerance)
remains for the genuinely-wrapped rows (OFFSET_METADATA_TOO_LARGE,
INVALID_COMMIT_OFFSET_SIZE, UNKNOWN_SERVER_ERROR, TopicAuthorization), which Java
itself only asserts as `KafkaException.class`. Mirrors
CommitRequestManagerTest.java:1499/1502.

Production (`classify_fetch_group_error`) was already correct — test-only change.

Mutation check: temporarily removed `UnknownMemberId`/`StaleMemberEpoch` from the
explicit `KafkaError::new(error)` arm in `classify_fetch_group_error` (so they
fell through to the `UnknownServerError` wrap). `offset_fetch_request_timeout_requests`
then FAILED with `left: UnknownServerError, right: UnknownMemberId`. Mutation
reverted; test passes again.

## Issue 2 — Unconditional `offsets.clone()` per auto-commit even when no interceptor is wired (minor perf) — RESOLVED

Fix: made the auto-commit interceptor path zero-clone when no interceptor is
wired.

- Added `AutoCommitInterceptorHook::has_interceptors()` (backed by the invoker's
  existing `interceptors_empty` flag — cheap, no lock on the chain).
- Added `CommitRequestManagerInner::has_auto_commit_interceptors()` (probes the
  hook `Option` and its `has_interceptors()`).
- Interval auto-commit path (`maybe_auto_commit_*`): the offsets snapshot is now
  `self.inner.has_auto_commit_interceptors().then(|| offsets.clone())` — clones
  only when an interceptor is wired; `None` (no clone) otherwise. The success arm
  does `if let Some(offsets) = offsets_for_interceptor { ... }`.
- Rebalance-flush path (commit retry driver): passes `&last_offsets` by reference;
  `enqueue_interceptor_invocation(&...)` now takes `&HashMap` and clones only when
  `hook.has_interceptors()` is true.

Result: a consumer with NO interceptor configured performs zero extra allocation
on auto-commit relative to before Phase 33 (matches Java's by-reference capture in
its `autoCommitCallback` BiConsumer). Behavior when an interceptor IS wired is
identical — confirmed by `autocommit_interceptors_invoked` /
`autocommit_interceptors_not_invoked_on_error` and the four
`offset_commit_callback_invoker` tests all passing. Hot poll/fetch path untouched.
