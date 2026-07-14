---
name: milestone9-share-consumer
description: KIP-932 share consumer is now an in-scope milestone (M9), superseding the consumer-threading.md §20 out-of-scope deferral
metadata:
  type: project
---

The client-side KIP-932 share consumer (KafkaShareConsumer / ShareConsumerImpl / share request managers / share fetch+ack path) is being translated as a new milestone (Milestone 9), agent number N=1.

**Why:** `consumer-threading.md` §20 explicitly deferred all Share* files as out-of-scope for Milestone 8. The user explicitly re-scoped it in and said this instruction supersedes the §20 deferral. Only the CLIENT side is in scope — broker/coordinator/tools/admin share-group code stays out.

**How to apply:** When §20's "share consumer out of scope" wording comes up, treat it as overridden for this milestone's client-side work. Admin-side `*ShareGroup*` handlers/options and integration tests (`ShareConsumerTest`, `ShareConsumerRackAwareTest`) remain out of scope. Much wire-level groundwork already exists: Share*Request/Response `*Data` structs are generated, `SHARE_*` API keys defined, `CoordinatorType::Share`, share error codes (122/123/133) in errors.rs, and `SubscriptionState::AutoTopicsShare` + `subscribe_to_share_group()` are already present — so the new work is mostly the request-manager/consumer/event orchestration layer plus hand-written request/response wrappers.
