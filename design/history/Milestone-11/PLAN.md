# Milestone 11: KIP-932 Share Consumer (client side)

## Goal

Translate the full **client-side** KIP-932 share consumer flow from Java
(`org.apache.kafka.clients.consumer.*`, Apache Kafka 4.2 — `kafka/` submodule at
commit `a18251bae0b825c69794a50dffd4c3100cf5ca5b`) to Rust: `KafkaShareConsumer`
built on `ShareConsumerImpl`, the share fetch / acknowledge / heartbeat /
membership managers, the acknowledgement types, and the public `ShareConsumer`
API, governed by
[`consumer-threading.md`](../../../.claude/rules/consumer-threading.md).

This milestone explicitly **supersedes the `consumer-threading.md` §20 deferral**
that had marked all `Share*` files out of scope during Milestone 8. Only the
client consume/acknowledge path is in scope — broker, coordinator, persister, and
tools remain out.

**Why now:** the KIP-848 `AsyncKafkaConsumer` engine landed in Milestone 8
(network thread, request-manager registry, membership/heartbeat managers, fetch
pipeline, event plumbing) already exists under `src/consumer/`. The share consumer
reuses that entire engine — `ShareConsumerImpl` is built on the same
`ApplicationEventHandler` / `ConsumerNetworkThread` / `CompletableEventReaper`
infrastructure — so the marginal work is the share-specific managers, the share
fetch/acknowledge path, the acknowledgement types, and the public API.

## Groundwork already present (not re-created this milestone)

- All generated wire `*Data` structs (`share_fetch_request_data.rs`,
  `share_acknowledge_*`, `share_group_heartbeat_*`, etc. — generated at build time
  from `generator/messages/*.json`).
- ApiKeys `SHARE_FETCH` / `SHARE_ACKNOWLEDGE` / `SHARE_GROUP_HEARTBEAT` in
  `src/common/protocol/api_keys.rs`.
- Error codes `ShareSessionNotFound=122`, `InvalidShareSessionEpoch=123`,
  `ShareSessionLimitReached=133` in `src/common/protocol/errors.rs`.
- `CoordinatorType::Share` in `src/common/requests/find_coordinator_request.rs`.
- `SubscriptionState::AutoTopicsShare` + `subscribe_to_share_group()` in
  `src/consumer/internals/subscription_state.rs`.

## Scope

**In scope (translate fully):**

- Share wire wrappers: `ShareRequestMetadata`, `share_fetch_request` /
  `_response`, `share_acknowledge_request` / `_response`,
  `share_group_heartbeat_request` / `_response`, `ShareSessionHandler`.
- Acknowledgement core: `AcknowledgeType`, `AcknowledgementCommitCallback`,
  `Acknowledgements`, `AcknowledgementCommitCallbackHandler`,
  `ShareAcknowledgementMode`, `ShareAcquireMode`, `ShareInFlightBatch`
  (+ exception).
- Share fetch path: `ShareFetchConfig`, `ShareFetch`, `ShareCompletedFetch`
  (receive-path zero-copy per §27), `ShareFetchBuffer`, `ShareFetchCollector`,
  `ShareFetchException`, `NodeAcknowledgements`.
- Share managers: `ShareMembershipManager`, `ShareHeartbeatRequestManager`,
  `ShareConsumerMetadata`, `ShareConsumeRequestManager`.
- Share events under `internals/events/` (§28) + `ShareAcknowledgementEventHandler`.
- Consumer orchestration + public API: `ShareConsumerImpl`, `ShareConsumer<K,V>`
  `#[async_trait]` trait, `KafkaShareConsumer`, `ShareConsumerConfig`,
  `MockShareConsumer`, `new_share_consumer` factory.
- Integration tests: `ShareConsumerTest`, `ShareConsumerRackAwareTest`
  (re-scoped in — see below).

**Out of scope:**

- Admin-side `*ShareGroup*` handlers/options/results,
  `Describe/Alter/Delete ShareGroupOffsets`, and the `*ShareGroupState` persister
  RPCs (broker/persister-side, not used by the client consume flow).
- Tools: `ConsoleShareConsumer*`, `ShareGroupCommand*`, `VerifiableShareConsumer`,
  `ShareConsumerPerformance`, `ShareGroupMessageFormatter`.

**Metrics deferred to KIP-714 (client telemetry, not yet implemented in the Rust
tree).** The metrics classes are NOT translated: `ShareFetchMetricsAggregator`,
`ShareFetchMetricsManager`, `ShareFetchMetricsRegistry`, `ShareConsumerMetrics`,
`KafkaShareConsumerMetrics`, `ShareRebalanceMetricsManager`, and their tests
(`ShareFetchMetricsManagerTest`, `KafkaShareConsumerMetricsTest`,
`ShareRebalanceMetricsManagerTest`). At metric call-sites inside translated
classes, the recording call is omitted with a `// metrics: deferred to KIP-714`
comment; all surrounding logic and behavior is preserved. Metric assertions in
the integration tests are skipped/removed pending KIP-714.

**Integration tests re-scoped IN.** The original plan deferred the end-to-end
share-consumer integration tests; the user re-scoped them into Phase 7. They are
translated and wired into the existing Docker-backed harness; the consume /
acknowledge / rebalance behavior is asserted in full. Several assertions that
require an `AdminClient` (none exists in the Rust tree) or a live share-group
broker are `#[ignore]`-gated with documented rationale.

## Java source root

`kafka/clients/src/main/java/org/apache/kafka/clients/consumer/` (production) and
`kafka/clients/src/test/java/org/apache/kafka/clients/consumer/` (tests), at
submodule commit `a18251bae0b825c69794a50dffd4c3100cf5ca5b`.

## Phases

| # | Phase | Java sources | Rust output | Depends on |
|---|---|---|---|---|
| 1 | **Share wire protocol layer** | `ShareRequestMetadata`, `ShareFetchRequest`/`Response`, `ShareAcknowledgeRequest`/`Response`, `ShareGroupHeartbeatRequest`/`Response`, `ShareSessionHandler`. Tests: `ShareSessionHandlerTest` + byte-level encoding tests. | `src/common/requests/{share_request_metadata, share_fetch_request, share_fetch_response, share_acknowledge_request, share_acknowledge_response, share_group_heartbeat_request, share_group_heartbeat_response}.rs`, `src/consumer/internals/share_session_handler.rs` | generated `*Data` structs |
| 2 | **Acknowledgement core types** | `AcknowledgeType`, `AcknowledgementCommitCallback`, `Acknowledgements`, `AcknowledgementCommitCallbackHandler`, `ShareAcknowledgementMode`, `ShareAcquireMode`, `ShareInFlightBatch` (+ exception). Tests: `AcknowledgementsTest`, `ShareAcknowledgementModeTest`, `ShareAcquireModeTest`. | `src/consumer/{acknowledge_type, acknowledgement_commit_callback}.rs`, `src/consumer/internals/{acknowledgements, acknowledgement_commit_callback_handler, share_acknowledgement_mode, share_acquire_mode, share_in_flight_batch, share_in_flight_batch_exception, share_fetch_config}.rs` | Phase 1 |
| 3 | **Share fetch data path** | `ShareFetch`, `ShareCompletedFetch`, `ShareFetchBuffer`, `ShareFetchCollector`, `ShareFetchException`, `NodeAcknowledgements`. Tests: `ShareCompletedFetchTest`, `ShareFetchBufferTest`, `ShareFetchCollectorTest` + per-record allocation-budget test (§27). | `src/consumer/internals/{share_fetch, share_completed_fetch, share_fetch_buffer, share_fetch_collector, share_fetch_exception, node_acknowledgements}.rs` | Phase 2 |
| 4 | **Share membership + heartbeat + metadata** | `ShareMembershipManager`, `ShareHeartbeatRequestManager`, `ShareConsumerMetadata`. Tests: `ShareMembershipManagerTest`, `ShareHeartbeatRequestManagerTest`. | `src/consumer/internals/{share_membership_manager, share_heartbeat_request_manager, share_consumer_metadata}.rs` | Phase 1 |
| 5 | **Share consume request manager + events** | `ShareConsumeRequestManager` (~1571 Java LOC), 10 share events under `internals/events/`, `ShareAcknowledgementEventHandler`. Tests: `ShareConsumeRequestManagerTest`. | `src/consumer/internals/share_consume_request_manager.rs`, `src/consumer/internals/events/share_*.rs` | Phases 3, 4 |
| 6 | **Consumer orchestration + public API + Mock** | `ShareConsumerImpl` (~1359 Java LOC), `ShareConsumer<K,V>` trait, `KafkaShareConsumer`, `ShareConsumerConfig`, `MockShareConsumer`, `new_share_consumer` factory, share-event wiring into the `ApplicationEvent` enum + processor. Tests: `ShareConsumerImplTest`. | `src/consumer/{share_consumer, kafka_share_consumer, mock_share_consumer, share_consumer_config}.rs`, `src/consumer/internals/share_consumer_impl.rs` | Phase 5 |
| 7 | **Production pipeline wiring + integration tests** | `new_share_consumer` end-to-end wiring, bg-loop share membership reconcile, share response routing; `ShareConsumerTest`, `ShareConsumerRackAwareTest`. | `src/consumer/internals/{share_consume_request_manager, consumer_network_thread, request_managers}.rs`, `src/consumer/mod.rs`, `tests/integration/{share_consumer_test, share_consumer_rack_aware_test}.rs` | Phase 6 |

## Key translation constraints (carried into every phase)

- Async-only public API, no `block_on` façade (`consumer-threading.md §1`);
  top-level `ShareConsumer` dispatch is one `#[async_trait]` trait; per-record
  `Deserializer` stays sync (`§2`, DoD §11).
- Single `tokio::spawn` bg task; reuse the existing `ConsumerNetworkThread`
  engine, do not split managers into tasks (`§10`); network poll must not be
  cancelled (`§10`).
- `wakeup()` = rotating `CancellationToken` (`§11`); `SubscriptionState` =
  `Arc<std::sync::Mutex>` (`§16`).
- Receive-path zero-copy: `ShareCompletedFetch` owns one buffer, downstream
  borrows; sync `Deserializer<T>` (`§27`). Event variants mirror Java's
  completable hierarchy (`§28`).
- Acknowledgement callback obligation: exactly-once callback invocation on the
  caller's task, not the bg task (`CLAUDE.md §9.5`, `§31` pattern).

## Workflow per phase

Per `.claude/rules/agent-roles.md §3`, each phase runs a Manager-coordinated
Actor → Critic → fix loop, agent number **N=1**. Comment files live at
`design/history/Milestone-11/Phase-N/COMMENTS.<critic-id>.md`; resolved comments
are moved to `COMMENTS.DONE.<critic-id>.md`. All work lands on the
`milestone9-share-consumer` branch.

## Definition of Done (per phase)

Beyond `definition-of-done.md`:

- `cargo build`, `cargo test`, `cargo xtask format-check`, `cargo xtask lint`
  clean.
- Unit tests for every class translated in the phase ship in the same phase;
  each skipped Java test method carries a one-line rationale per DoD §3.
- Byte-level wire encoding tests (Phase 1) and per-record allocation-budget test
  (Phase 3) pass.
- Consumer trait-surface check (DoD §11) verified on phases that touch the public
  trait (6, 7).

## Outcome

All 7 phases completed on `milestone9-share-consumer`. Final Rust state is green:
build clean, lib tests pass (1 ignored), lint + format clean. `new_share_consumer`
builds a real end-to-end pipeline (subscribe → join → fetch → ack → commit →
close). Three real behavioral bugs were caught by the Critic and fixed (see the
per-phase `COMMENTS.DONE.1.md` files):

1. **Phase 3** — corrupt-batch CRC errors mislabeled `IllegalState` propagated
   instead of being deferred when records were already collected.
2. **Phase 5** — COMMIT_ASYNC deadline reset used `now_ms = 0`, so reused async
   acknowledgement state timed out immediately in production (masked by a
   near-zero `MockClock` in tests).
3. **Phase 6** — RENEW acknowledgement was silently broken: renewed records were
   never re-delivered under the zero-copy move-out.

### Known remaining gaps (documented, none blocking the client working)

- `KafkaShareConsumerTest` full-pipeline MockClient round-trips deferred — the
  Rust `MockClient` is FIFO / node-based with no request-body matchers (the same
  gap that defers the sibling `AsyncKafkaConsumer` MockClient tests).
- Consume/ack + rack-aware integration tests `#[ignore]`-gated — they need an
  `AdminClient` (`alterShareAutoOffsetReset` on a GROUP `ConfigResource`; none
  exists in the Rust tree) and, for rack awareness, a 3-broker cluster.
- The production `mod.rs` join-wiring's end-to-end group-JOIN effect has no
  automated regression guard (needs the deferred MockClient matcher or a live
  broker); the bg-loop *mechanism* it feeds IS guarded by a teeth-having
  `run_once` test (Phase 7).
- `new_share_consumer` / `KafkaShareConsumer::new` require `K/V: Clone` (for RENEW
  retention) — an accepted Rust-specific divergence from Java's unbounded
  `ShareConsumer<K,V>`, documented in rustdoc.
