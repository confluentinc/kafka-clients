---
name: milestone9-share-consumer
description: KIP-932 share consumer is now an in-scope milestone (M9), superseding the consumer-threading.md §20 out-of-scope deferral
metadata:
  type: project
---

The client-side KIP-932 share consumer (KafkaShareConsumer / ShareConsumerImpl / share request managers / share fetch+ack path) is being translated as a new milestone (Milestone 9), agent number N=1.

**Why:** `consumer-threading.md` §20 explicitly deferred all Share* files as out-of-scope for Milestone 8. The user explicitly re-scoped it in and said this instruction supersedes the §20 deferral. Only the CLIENT side is in scope — broker/coordinator/tools/admin share-group code stays out.

**How to apply:** When §20's "share consumer out of scope" wording comes up, treat it as overridden for this milestone's client-side work. Much wire-level groundwork already existed: Share*Request/Response `*Data` structs are generated, `SHARE_*` API keys defined, `CoordinatorType::Share`, share error codes (122/123/133) in errors.rs, and `SubscriptionState::AutoTopicsShare` + `subscribe_to_share_group()` — so the new work was the request-manager/consumer/event orchestration layer plus hand-written request/response wrappers.

**STATUS: COMPLETE (branch `milestone9-share-consumer`, not yet pushed/PR'd as of 2026-07-15).** All 7 phases done via Actor(N=1)/Critic(N=1) loop; 46 commits; 43 new share/ack `src/` files. Final Rust state green: build clean, 2257 lib tests pass (1 ignored), lint + format clean. `new_share_consumer` builds a real end-to-end pipeline (subscribe→join→fetch→ack→commit→close). Metrics deferred to KIP-714 (all `Share*Metrics*`/`KafkaShareConsumerMetrics`/`ShareRebalanceMetricsManager` omitted, call-sites stubbed `// metrics: deferred to KIP-714`) — user decision. Integration tests (`ShareConsumerTest`, `ShareConsumerRackAwareTest`) were RE-SCOPED IN (user override of the original deferral) and translated in Phase 7.

**Real bugs the Critic caught & the Actor fixed** (not just style): (1) corrupt-batch CRC errors mislabeled `IllegalState` → propagated instead of deferred (Phase 3); (2) COMMIT_ASYNC deadline reset used `now_ms=0` → async ack retries time out immediately in production, masked by MockClock starting at 0 (Phase 5); (3) RENEW acknowledgement silently broken — renewed records never re-delivered under the zero-copy move-out (Phase 6). See [[milestone9-phase5-carryover]].

**Known remaining gaps (all documented, none blocking the client working):**
- `KafkaShareConsumerTest` full-pipeline MockClient round-trips deferred — Rust `MockClient` is FIFO/node-based with no request-body matchers (SAME gap that defers the sibling `AsyncKafkaConsumer` MockClient tests).
- Consume/ack + rack-aware integration tests `#[ignore]`d — need an `AdminClient` (none exists in the Rust tree: `alterShareAutoOffsetReset` on a GROUP ConfigResource) and, for rack, a 3-broker cluster.
- The production `mod.rs` join-wiring's end-to-end group-JOIN effect has no automated regression guard (needs the deferred MockClient matcher or a live broker); the bg-loop *mechanism* it feeds IS now guarded by a teeth-having `run_once` test.
- `new_share_consumer`/`KafkaShareConsumer::new` require `K/V: Clone` (RENEW retention) — accepted Rust-specific divergence from Java's unbounded `ShareConsumer<K,V>`, documented in rustdoc.
- `make verify` (C/Python/Docker multilanguage) not run in-session; Rust-relevant checks all pass.
