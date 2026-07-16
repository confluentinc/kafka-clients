---
name: m11-phase1-admin-notes
description: Milestone 11 Tier 1 Phase 1 (Admin) — foundation done + scope conflicts vs PLAN that need Manager decisions before FFI/Python
metadata:
  type: project
---

Milestone 11 (AdminClient), Tier 1 Phase 1 "Foundation + Topics CRUD", Actor N=1,
branch `dev/admin-client-implementation`. Plan: `design/history/Milestone-11/PLAN.md`.
Admin design rules now live in `.claude/rules/admin-client.md` (a Phase-1 deliverable).

**Why (scope reality):** this phase is a multi-week effort (KafkaAdminClient.java is
5170 lines, its test 11,752 lines) spanning Rust core + unit + integration + C FFI +
Python. It is NOT completable in one session. Do it in green, committed increments in
dependency order; the plan itself says "Rust core + tests FIRST, get green, then C FFI,
then Python" and "one phase at a time with a check-in".

**How to apply — foundation DONE (3 green commits on top of 28e9e89):**
- `.claude/rules/admin-client.md` + completable `KafkaFuture` (the hard hidden
  prerequisite): `KafkaFutureImpl<T>` (pub(crate): complete/complete_exceptionally/
  when_complete/future) + `all_of`/`then_apply`/`then_apply_try`. `KafkaFutureImpl`
  carries `#[allow(dead_code)]` until src/admin lands its first caller — REMOVE that
  allow when admin uses it.
- common leaf types: `common::acl::{AclOperation,AclPermissionType}` (enums only),
  `common::TopicCollection` (enum: TopicIds/TopicNames), `common::TopicPartitionInfo`,
  `common::utils::{from_32_bit_field,to_32_bit_field}`.

**Scope conflicts that need a Manager/user DECISION before proceeding (surfaced by
reading the actual branch, not the plan's assumptions):**
1. `ConcreteRequest`/`ConcreteResponse` (src/common/requests/) are ENUMS, not traits.
   Each new admin wire type (CreateTopics, DeleteTopics, DescribeCluster,
   DescribeTopicPartitions) needs a new enum variant + wiring EVERY match arm
   (version/api_key/to_send/serialize/get_error_response/parse). Larger than plan implied.
2. FFI/Python premise is FALSE on this branch: there is NO consumer FFI, NO consumer.py,
   the Rust FFI (`src/ffi/producer.rs`) is fully SYNCHRONOUS `block_on` under a Mutex,
   and there is NO async C dispatcher/CompletionJob in Rust (the batching/threading
   dispatcher lives in the hand-written C ext `_confluentkafka.c`). The plan's "async C
   API + Python async over it, mirror consumer" cannot be followed literally. Admin
   FFI/Python must mirror the PRODUCER (sync) pattern actually present. Flag loudly.
3. Pre-existing clippy failures on the branch base (present at 28e9e89, unrelated to
   admin): `src/common/config/sasl_configs.rs:147` and
   `src/consumer/internals/subscription_state.rs:1120` (both `match`→`?`). These block
   the global `cargo xtask lint` gate independent of admin work — not mine to fix silently.

**Remaining Rust-core work, dependency order:** Config/ConfigEntry → admin POJOs
(NewTopic, TopicListing, TopicDescription, 4×Options, 4×Result) → 4 wire wrappers
(enum wiring per #1) → Call engine + NodeProviders + TimeoutProcessor +
AdminMetadataManager + AdminClientRunnable (one tokio::spawn; see KafkaAdminClient.java
853-1694) → AdminClientConfig → KafkaAdminClient 4 RPCs + Admin trait + factory →
MockAdminClient → unit tests (KafkaAdminClientTest slices) → integration
(tests/integration/admin_topics_test.rs; TestContext::cleanup is a no-op awaiting admin).
describeTopics-by-names is intricate (DescribeTopicPartitions cursor pagination + Metadata
fallback + describeCluster prerequisite via when_complete) — hardest RPC.
