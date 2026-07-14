---
name: milestone9-phase7-production-pipeline
description: KIP-932 Phase 7 — new_share_consumer production pipeline, share bg-loop reconcile, response routing, Clone bound, integration test gating
metadata:
  type: project
---

Milestone 9 Phase 7 (branch `milestone9-share-consumer`): wired the real
production share-consumer pipeline end to end. All 4 DoD checks green
(build, `cargo test --lib` = 2256 pass, format-check, lint). Commits 1-6/N.

**Response routing added to ShareConsumeRequestManager** (was missing —
poll built UnsentRequests but nothing routed responses back). Copied the
FetchRequestManager forwarder+drain pattern: `PendingShareCompletion` enum,
`pending_completion_tx/rx`, `attach_forwarders` (spawns one forwarder per
UnsentRequest, per-broker not per-record §11) + `drain_pending_completions`
at top of `poll`. GATED on `set_completion_notify` being set (production
only) — else the 52 existing unit tests (which drive `handle_share_*`
directly + drop requests) would get spurious NetworkException failures from
dropped senders. `build_ack_request` now returns
`(UnsentRequest, ShareAcknowledgeRequestData, Node)` so the ack forwarder
can route. Fetch request_data captured via `builder.data().clone()`.

**Share bg-loop reconcile**: `ConsumerNetworkThread` gained a
`share_membership: Option<Arc<ShareMembershipManager>>` field + setter
(mutually exclusive with the KIP-848 `membership` field; used a setter not
a ctor param to avoid touching 7 test fixtures). run_once Phase 2.4s/2.5s
drives it exactly like the consumer path: (1) `propagate_share_member_id`
(the Rust analog of Java's
`shareMembershipManager.registerStateListener(shareConsumeRequestManager)` —
share_consume is a `&mut` entries() slot, not Arc, so it can't be a `&self`
MemberStateListener; propagate the stable member id each iteration instead),
(2) drain share heartbeat's pending fenced/fatal/stale transitions,
(3) `reconcile(now)` (share reconcile takes NO can_commit). Without this the
membership state machine never advances → never joins.

**RequestManagers::for_share(coordinator, share_consume, share_heartbeat,
share_membership)** mirrors Java's share ctor. Plus `share_membership_handle`,
`take_pending_share_membership_transitions`, `propagate_share_member_id`.

**Metadata sharing gotcha**: Rust has no `ShareConsumerMetadata extends
ConsumerMetadata`. `new_share_consumer` builds ONE `ShareConsumerMetadata`
(the source of truth, share-scoped overrides) and gives its `metadata_arc()`
(shared `Arc<Metadata>` M) to the NetworkClient. Membership + fetch collector
need `Arc<ConsumerMetadata>`, so added
`ConsumerMetadata::from_shared_metadata(M, subs, allow_auto_create)` that
WRAPS M (installs no overrides of its own). All three views + NetworkClient
read/write the same M — consistent.

**Clone bound decision (Part A req 3)**: `new_share_consumer` /
`KafkaShareConsumer::new` require `K: Clone, V: Clone` because
`ShareConsumerImpl` impls `ShareConsumer` only for Clone K/V (RENEW retention
path). Documented as an accepted, rarely-visible Rust-specific divergence
from Java's unbounded `ShareConsumer<K,V>` (added as a `where` clause on the
`new` method only, not the impl block).

**ProductionShareApplicationEventHandler** (in share_consumer_impl.rs): the
production impl behind the mockable `ShareApplicationEventHandler` trait.
Wraps `ApplicationEventHandler` (add+notify), reads max_time_to_wait Arc,
fires event_notify for wakeup_network_thread, and holds the
`NetworkThreadCloseHandle` in a `tokio::sync::Mutex` for the `&self async
close` (signal_close + wakeup + timeout-bounded await_join).

**Share events flow through the SAME bg task / ApplicationEventProcessor**
as the consumer — no separate share channel. `process_share_subscription_change`
completes the subscribe event LOCALLY (no broker), which is why the docker-free
smoke test (`src/consumer/mod.rs::share_pipeline_smoke_tests`) can prove
subscribe round-trips through the real bg task.

**Config**: `ShareConsumerConfig` gained a parsed `share.acknowledgement.mode`
(default implicit, validated at parse via `ShareAcknowledgementMode::from_string`).
Ctor errors wrap as exact message "Failed to construct Kafka share consumer"
(Java's KafkaException wrapper), cause logged (KafkaError has no cause chain).

**Tests**:
- `test_response_routing_fetch_success_path` (share_consume RM) — builds a
  ClientResponse, completes the request handler, asserts the drain delivers
  to the buffer.
- smoke test + testGroupIdNull/Empty/OnlyWhitespaces (mod.rs).
- `factory_builds_working_consumer` (kafka_share_consumer.rs) — replaced the
  stale unsupported_version test.
- Integration: `tests/integration/share_consumer_test.rs` (+ rack_aware). Wired
  into `tests/integration/main.rs`. `test_poll_no_subscribe_fails` RUNS GREEN
  against a real Docker broker (share cluster config accepted). Consume/ack +
  rack-aware tests `#[ignore]`d — need an AdminClient (for
  `alterShareAutoOffsetReset` on a GROUP ConfigResource — Rust has NO
  AdminClient) and, for rack, a 3-broker cluster + server-side RackAwareAssignor.

**DEFERRED with rationale**:
- `KafkaShareConsumerTest` (MockClient full heartbeat/fetch/ack round-trips):
  needs (a) a MockClient injection seam into the share ConsumerNetworkThread
  pipeline and (b) request-matcher-based MockClient responses (Java's
  `prepareResponseFrom(body -> ..., resp, node)` with member-id capture +
  epoch chaining). The Rust MockClient is FIFO/node-based, no matchers. This
  is the SAME gap that keeps the sibling AsyncKafkaConsumer MockClient
  full-pipeline tests deferred (see the many "Deferred to Phase 12.5 ...
  requires MockClient-backed bg task" notes in async_kafka_consumer.rs). Not
  built here; the smoke + integration tests provide the pipeline coverage.
- `testFailConstructor` / `testConstructorFailsOnNetworkClientConstructorFailure`:
  Java reflective metric-reporter class / SASL JAAS LoginModule — no Rust
  equivalent.
- Full 4373-line ShareConsumerTest: only a faithful subset translated.

`make verify` runs the full C+Python+Docker multilanguage suites (unrelated
to this Rust-only change) and exceeds a practical time bound in-env; the
Rust-relevant portions all pass.
