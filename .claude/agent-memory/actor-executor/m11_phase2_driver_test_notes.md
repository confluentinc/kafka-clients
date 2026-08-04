---
name: m11-phase2-driver-test-notes
description: How AdminApiDriverTest was translated + the shared test-support fake pattern for the admin driver
metadata:
  type: project
---

AdminApiDriverTest.java translated as direct driver unit tests (Critic N=1
should-fix on the untested unmap + disconnect-retry branches).

**Shared fake pattern**: `#[cfg(test)] #[allow(dead_code)] pub(crate) mod test_support`
in `admin_api_driver.rs` holds the fake `AdminApiHandler`/`AdminApiFuture`/
`AdminApiLookupStrategy` + a `TestContext` fixture. Because the driver owns the
handler/future by `Box`, expectation tables are shared via `Arc<Mutex<HashMap<
BTreeSet<String>, ...>>>` (both Send). `TestContext` keeps clones of those Arcs.
`pub(crate)` (not private `mod tests`) so `kafka_admin_client.rs` tests can reuse
the fixture to build a real `new_driver_call` and exercise the `maybe_retry` hook.

**Key gotchas solved**:
- Added `#[cfg(test)] AdminApiDriver::key_to_broker_id` mirroring Java `keyToBrokerId`
  (reads `fulfillment_map.reverse_map`); the driver has no public state accessor.
- Completed keys are CLEARED from both maps → `key_to_broker_id` returns None after
  completion. Don't assert "mapped" post-completion; assert the future value instead.
- `ApiRequestScope` is a closed enum (SingleLookup/Fulfillment). All dynamic keys
  coalesce into ONE SingleLookup request; Java's MockRequestScope id can split them.
  Stage-transition logic identical — only initial lookup fan-out coalesces. Document
  per test where it changes a request count.
- Fulfillment retry backoff = `now + backoff((tries-1).max(0))` = one jittered step
  (assert range `[now+80, now+120]` for 100ms/0.2 jitter); lookup retry = `now` (no
  backoff). tries captured BEFORE set_inflight increments.
- `MaybeRetryOutcome::Handled` for NetworkException (drives driver.on_failure +
  maybe_send_requests), `Requeue` otherwise — hook lives in `new_driver_call`.
- NOT_LEADER_OR_FOLLOWER→unmapped CLASSIFICATION is tested in delete_records_handler;
  driver tests cover the REACTION to `unmapped_keys` (Java splits it the same way).
