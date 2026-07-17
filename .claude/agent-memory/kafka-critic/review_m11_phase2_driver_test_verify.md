---
name: review-m11-phase2-driver-test-verify
description: Verification heuristics for AdminApiDriver branch-coverage test fixes (unmap re-lookup, disconnect maybe_retry hook)
metadata:
  type: project
---

Verifying the M11 Phase 2 AdminApiDriver branch-coverage fix (commits 0079e91/e136593) surfaced reusable heuristics for judging whether a "new test" fix is genuine vs superficial. See [[review-m11-phase2-admin-driver]].

**Hook reachability check.** A test that calls a retry/failure hook directly (`call.maybe_retry(...)`) only proves the hook works — not that production ever reaches it. Confirm the production caller path: the admin runnable calls `maybe_retry` only after `!error.is_retriable()` passes through. So verify the trigger error (`NetworkException`) is in `errors.rs` `is_retriable()` list — otherwise the hook is dead code and the test is testing an unreachable path. It is retriable, so the disconnect→lookup-retry hook is genuinely reachable.

**No double-handling.** For disconnect, `maybe_retry` returns `MaybeRetryOutcome::Handled` and the runnable does NOT also call `handle_failure` (match arm `Handled => {}`). So the bridge test calling only `maybe_retry` (not handle_failure) faithfully represents the real single-dispatch scenario.

**Fake-seeding parity.** The Rust `test_support::FakeFuture` uses the default `cached_key_broker_id_mapping` (all `UNKNOWN_BROKER_ID`), so even static keys reach fulfillment via the constructor's `unmap` → `lookup_strategy.lookup_scope(key)` returning `Fulfillment(broker)`. This is exactly Java's `SimpleAdminApiFuture` (`AdminApiFuture.forKeys`) + `MockLookupStrategy` behavior — NOT a divergence. Java's TestContext also has no cached mapping.

**Teeth check (mutation-reasoning).** `TestContext::poll` asserts `specs.len() == expected_lookups.len()+expected_requests.len()` and the fakes' `build_request` panic on an unexpected key-set. So if `retry_lookup`/`unmap` regressed to a no-op, a key would stay mapped and produce a fulfillment request against an empty expectation table → panic. Real teeth, not trivially-passing.

Verdict: fix genuine and complete; Phase 2 clean. 10 tests (8 driver-level + 2 Call-bridge) pass.
