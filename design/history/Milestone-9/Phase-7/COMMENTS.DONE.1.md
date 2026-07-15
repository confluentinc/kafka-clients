# Critic 1 — Milestone 9 Phase 7 review: RESOLVED findings (fixups `e1f8a8e`, `89e15f4`)

Both NON-BLOCKING Phase-7 findings are resolved and moved here from `COMMENTS.1.md`.

## Finding 1 — bg-loop share reconcile orchestration now has a test with teeth — RESOLVED (fixup `89e15f4`)

Added `run_once_drives_share_membership_reconcile_and_member_id_propagation`
(`consumer_network_thread.rs`): wires a share `ConsumerNetworkThread` via
`RequestManagers::for_share` + `set_share_membership`, receives an (empty)
assignment so the member is `Reconciling`, drives ONE `run_once`, and asserts
(a) `share_membership.reconcile(now)` advanced `Reconciling -> Acknowledging`
(Phase 2.5s) and (b) the member id was propagated into the
`ShareConsumeRequestManager` (`propagate_share_member_id`, Phase 2.4s). Verified
the test FAILS if the `reconcile(now)` call, the `propagate_share_member_id`
call, or the `set_share_membership` wiring is removed (the
`if let Some(share_membership)` block is then skipped: state stays `Reconciling`,
member id stays `None`). Remaining broker-only gap documented on the test: the
PRODUCTION `mod.rs` `set_share_membership` line's end-to-end group JOIN needs the
deferred MockClient request-matcher harness / a share-group-enabled broker; this
test guards the mechanism that line feeds. New test accessors:
`ShareConsumeRequestManager::member_id_for_test`,
`RequestManagers::share_consume_member_id`.

## Finding 2 — ack-failure completion time — RESOLVED (fixup `e1f8a8e`)

The `PendingShareCompletion::AckFailure` variant now carries
`response_completion_time_ms`, captured in the spawned forwarder at the instant
the failure resolves (the time source is cloned into the forwarder; the
wrong-response-body case reuses the `ClientResponse` received time). The drain
threads it into `handle_share_acknowledge_failure` instead of the drain-time
`self.time.milliseconds()`, matching Java's `handler().completionTimeMs()` used
for BOTH the success and failure paths.

Original finding text follows:

## NON-BLOCKING — Issue: the load-bearing bg-loop share reconcile orchestration has no test with teeth
- **File**: `src/consumer/internals/consumer_network_thread.rs:605-663` (Phase 2.4s/2.5s),
  `src/consumer/internals/request_managers.rs:262-266` (`propagate_share_member_id`),
  `src/consumer/mod.rs:794` (`set_share_membership`)
- **Severity**: Missing test coverage (DoD / test-teeth)
- **Java Reference**: `ConsumerNetworkThread.runOnce` / share `RequestManagers.entries()`
- **Description**: Commit `e50dc7e` is described as "load-bearing, must let the
  consumer JOIN." The `reconcile` logic itself is well unit-tested in isolation
  (`share_membership_manager.rs`, 18 reconcile calls in its test module), and the
  response-routing handlers are unit-tested (`test_response_routing_fetch_success_path`).
  But the Phase-7 **orchestration** that ties them together — `set_share_membership`,
  `propagate_share_member_id`, `take_pending_share_membership_transitions`, and the
  per-iteration `share_membership.reconcile(now)` in `run_once` Phase 2.4s/2.5s — is
  exercised by NO automated test. `set_share_membership` has exactly one caller
  (`mod.rs:794`, production); `for_share` and `propagate_share_member_id` have zero
  test references. The only test touching the real bg loop is the mod-level smoke test
  `new_share_consumer_builds_working_pipeline_and_subscribes`, and its `subscribe`
  completes through `ApplicationEventProcessor::process_share_subscription_change`
  (`application_event_processor.rs:1246-1273`), which updates `SubscriptionState` +
  calls `membership.on_subscription_updated()` and completes the handle synchronously
  — **it never depends on `reconcile` being driven, nor on `set_share_membership`
  having been called** (`membership` is obtained via `share_heartbeat.membership_manager()`,
  present regardless of the bg-loop wiring).
- **Failure scenario**: If the `set_share_membership(...)` call at `mod.rs:794` were
  dropped, or the Phase 2.4s/2.5s block were skipped/mis-ordered, the smoke test would
  STILL PASS — the KIP-932 membership state machine would never advance and the consumer
  would never join, but no test would catch it. The milestone therefore has no automated
  proof that the central "share consumer can JOIN" capability works end-to-end; the
  join handshake is only reachable against a live share-group broker (integration tests
  `#[ignore]`d for AdminClient) or a request-matching MockClient (`KafkaShareConsumerTest`
  deferred — see below). Recommend a bg-loop unit test that installs a fake/spy
  `ShareMembershipManager` via `set_share_membership` and asserts `reconcile` is invoked
  each `run_once` iteration (analog of the consumer-path reconcile driving), so a
  regression in the orchestration is caught.

## NON-BLOCKING — Issue: ack-failure path uses drain-time instead of response-completion time
- **File**: `src/consumer/internals/share_consume_request_manager.rs:455-462` (drain,
  `AckFailure` arm passes `self.time.milliseconds()`)
- **Severity**: Behavior Mismatch (minor)
- **Java Reference**: `ShareConsumeRequestManager.java:1263` —
  `handleShareAcknowledgeFailure(..., unsentRequest.handler().completionTimeMs())`
- **Description**: The success path captures the real response completion time in the
  spawned forwarder (`client_response.received_time_ms()`, line 1073) and threads it
  through `response_completion_time_ms`. The failure path's `PendingShareCompletion::AckFailure`
  variant carries no timestamp, so the drain passes `self.time.milliseconds()` (drain
  time) into `handle_share_acknowledge_failure` → `on_failed_attempt(...)`, which seeds
  the retry-backoff deadline. Java uses `handler().completionTimeMs()` for BOTH success
  and failure. Because the forwarder fires `notify_one()` immediately and the next
  `run_once`/drain follows within microseconds, the divergence shifts the retry deadline
  by well under a millisecond — negligible in practice, but it is an internal
  inconsistency (success captures real time, failure does not) and a divergence from
  Java. Not blocking. (The forwarder has no `ClientResponse` on the transport-failure
  path, so a faithful fix would capture `self.time` into the forwarder closure.)
