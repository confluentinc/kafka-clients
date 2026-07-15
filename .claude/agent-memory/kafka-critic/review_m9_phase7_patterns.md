---
name: review-m9-phase7-patterns
description: M9 Phase 7 share-consumer production wiring — response routing, bg-loop reconcile orchestration, test-teeth gap
metadata:
  type: project
---

# M9 Phase 7 (new_share_consumer pipeline + response routing + bg-loop reconcile)

**Load-bearing orchestration wiring can be test-teeth-free even when its pieces are
unit-tested.** Phase 7's central change (bg-loop drives `ShareMembershipManager::reconcile`
+ `propagate_share_member_id` + `take_pending_share_membership_transitions`, so the
consumer can JOIN) has NO test that would fail if the wiring were removed. `reconcile`
itself is well unit-tested in `share_membership_manager.rs`; the response-routing
handlers have a unit test. But `set_share_membership` has exactly one caller (production
`mod.rs`), and the only bg-loop test — the mod-level smoke test — completes `subscribe`
via `ApplicationEventProcessor::process_share_subscription_change` (updates
SubscriptionState + `on_subscription_updated()` + completes handle synchronously), which
NEVER depends on reconcile or on `set_share_membership`. **Heuristic:** for any "wire X
into the bg loop so the state machine advances" change, grep for non-production callers
of the setter; if the only caller is the factory, and the sole integration test's
success path bypasses the wired code (subscribe completes without reconcile), the change
has no regression test. Recommend a spy-membership bg-loop test asserting reconcile is
invoked each `run_once`.

**Share response routing faithfully clones FetchRequestManager.** `poll` =
`drain_pending_completions()` → `poll_body()` → `attach_forwarders()`. One `tokio::spawn`
per broker request (NOT per record), response by ownership (§27), gated on
`completion_notify: Option<Arc<Notify>>` (None in the 52 unit tests → no forwarder, no
spurious dropped-sender failure). The `pending_request_meta`↔`unsent_requests` 1:1
invariant holds because `poll_body` returns EITHER the ack result OR the fetch result
(mutually exclusive) — `process_acknowledgements` pushes meta only paired with an
`unsent_requests.push`, and its `None` fall-through leaves meta empty so poll_fetch's
meta stays 1:1. Double-apply prevented by `nodes_with_pending_requests`. Verify the
production notify is shared across `set_completion_notify` + `ApplicationEventHandler` +
`ConsumerNetworkThread` (consumer path does this at `async_kafka_consumer.rs:1179/1301/1355`).

**Ack-FAILURE completion time divergence (minor, real).** Success path captures
`client_response.received_time_ms()` in the forwarder; failure path's `AckFailure`
variant carries no timestamp so drain passes `self.time.milliseconds()` (drain time).
Java uses `handler().completionTimeMs()` for BOTH (`ShareConsumeRequestManager.java:1263/1265`).
Sub-ms shift in retry-backoff seed; non-blocking but note the success/failure asymmetry.
Root cause: transport-failure forwarder has no ClientResponse; faithful fix captures
`self.time` into the closure.

**Poll-before-reconcile is NOT a Phase-7 bug.** `share_consume.poll()` runs during the
`entries()` walk BEFORE Phase 2.5s reconcile — same structural pattern as consumer
`fetch.poll()`. The before/after split defers only network submission of the PollResult,
not the poll() call. One-iteration convergence lag, accepted pre-existing infra. Don't
flag.

**Member-id propagation faithfulness.** Rust `on_member_epoch_updated` ignores epoch and
sets member_id — matches Java `onMemberEpochUpdated` (also ignores `memberEpochOpt`).
Per-iteration propagation idempotent (stable member id); empty pre-assignment string →
`None` via `.ok()` (defensive; Java's listener just wouldn't fire). `poll_body` guards
`member_id.is_none()` before the `expect("member id set")`.

**Deferral verification (all legit).** No `AdminClient` anywhere (`find`/`grep` empty) →
consume/ack + rack-aware integration tests correctly `#[ignore]`d (need
`alterShareAutoOffsetReset` on GROUP ConfigResource). `src/mock_client.rs` is a
node-filtered FIFO VecDeque with NO request-body matcher → `KafkaShareConsumerTest`
deferral genuine; sibling AsyncKafkaConsumer MockClient tests also deferred
(`async_kafka_consumer.rs:5023,5344`). Group-id tests match Java
`ShareConsumerImplTest.java:762-793` message assertions exactly. The separate
`testInvalidGroupId` (line 222, asserts cause = InvalidGroupIdException) is untranslated
but redundant given KafkaError has no cause chain — reasonable skip.
