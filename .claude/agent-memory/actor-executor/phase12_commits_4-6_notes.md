---
name: phase12_commits_4-6_notes
description: Milestone-8 Phase 12 commits 4-6 — factory swap, smoke test, integration test scaffolding with documented response-routing gap
metadata:
  type: project
---

# Phase 12 commits 4-6 close-out notes (Actor 1, round 3)

## What landed

- **(4/N)** Flipped `new_consumer()` factory's `GroupProtocol::Consumer` arm to `Ok(Box::new(AsyncKafkaConsumer::new(...)?))`. Docker-free smoke test in `tests/consumer/async_kafka_consumer_test.rs` proves the ctor + bg-task spawn + clean close against `127.0.0.1:1` (refused). Folded Issue 6 (stale `max_time_to_wait_ms` comment) per Critic round 2.
- **(5/N)** Added `tests/integration/consumer_test.rs` + module wire-up in `tests/integration/main.rs`. First test (`test_subscribe_and_poll_records`) is `#[ignore]`-gated.
- **(6/N)** Appended remaining three flows (assign+poll, commit-resume, seek-to-beginning), all `#[ignore]`-gated.

## Why **Why:** Phase-10 response-routing gap

**Why:** A pre-existing design gap surfaced when running the integration tests against a real Kafka 4.2.0 broker:
`CoordinatorRequestManager::make_find_coordinator_request` returns `UnsentRequest::new(builder, None)` and no production code path calls `take_response_receiver()`. The FindCoordinator response fires `FutureCompletionHandler::on_complete_ref`, but the receiver was dropped at request build time. The consumer is stuck in JOINING; no heartbeat (gated on `coordinator_known`); no assignment; `poll()` returns zero records forever.

Same gap for `ConsumerHeartbeatRequestManager::build_heartbeat_request`. Both managers' rustdoc claims "the bg task (Phase 10) takes the response receiver via `take_response_receiver`" — but the bg task at `consumer_network_thread.rs::run_once` does NOT do this for `coordinator` or `consumer_heartbeat`.

Verified with `RUST_LOG=confluent_kafka=trace`: request reaches broker (`FindCoordinator v6` with `coordinator_keys=["..."]`), `RequestState{lastSentMs=X, lastReceivedMs=-1, requestInFlight=true}` forever, broker silent (request lands but the response is being discarded client-side).

**How to apply:** Phase 12 cannot land the response router without substantial bg-task refactor work. Path to un-ignore documented in `tests/integration/consumer_test.rs` module docstring. The smoke test in `tests/consumer/async_kafka_consumer_test.rs` (ctor + spawn + close, no membership) does NOT hit the gap — that test passes docker-free and is the unit-level safety net.

## Patterns reused from prior phases

- **`#[ignore]` with explicit rationale + module-docstring path-to-unignore**: matches the Phase-7c precedent (`testSubscribePatternAgainstBrokerNotSupportingRegex`). Tests stay in tree, regress automatically when the bug is fixed.
- **Local `StringDeserializer` impl**: `tests/consumer/async_kafka_consumer_test.rs` and `tests/integration/consumer_test.rs` each inline one. Crate doesn't export a shared `StringDeserializer` (only `StringSerializer`). Acceptable until a crate-wide impl lands.
- **Per-test ClusterConfig variant** (not `ClusterConfig::default()` mutation): each new broker env-var override needs a fresh `ClusterConfig::with_properties(...)` so the cluster-pool hash key is distinct from the producer suite's default. PLAN.md guards this explicitly.

## What's left for commit (7/N)

- Translate the 5 Phase-11-deferred unit tests in `async_kafka_consumer.rs` (`async_kafka_consumer.rs:3875, 3967, 5295, 5655, 6203`) where the production ctor unblocks them.
- Flip PLAN.md row 12 to CLOSED.
- Agent-memory consolidated patterns entry.

## Critic targets for round 3

- Verify the bg-task response-routing claim in the module docstring (the rustdoc on `coordinator_request_manager.rs:230` and the absence of `take_response_receiver` in `consumer_network_thread.rs::run_once` for coordinator/heartbeat).
- Confirm `cluster_config_with_kip848()` is the right shape (broker env var, not server.properties — `KafkaDockerWrapper` converts `KAFKA_*` to server.properties at container init).
- Whether to upgrade any of the four `#[ignore]` tests to actual test fixes in Phase 12, or punt to a follow-up milestone. Likely the latter — this is real Phase-10 carry-over work.
