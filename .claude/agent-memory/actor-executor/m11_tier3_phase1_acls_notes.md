---
name: m11-tier3-phase1-acls-notes
description: M11 Tier 3 Phase 1 ACLs — LeastLoaded retry divergence, module_inception, authorizer fixture, custom aggregate future
metadata:
  type: project
---

Milestone 11 Tier 3 Phase 1 (ACLs) landed on `dev/adminclient_translation_and_bindings`.
Prereqs already present: `AclOperation`, `AclPermissionType` (Tier 1), `common::utils::from_32_bit_field`.
Translated new: `common/resource/*` (ResourceType, PatternType, Resource, ResourcePattern,
ResourcePatternFilter), `common/acl/{access_control_entry(_data)(_filter), acl_binding(_filter)}`,
3 wire wrappers, 3 admin options/results, 3 Admin RPCs, mock stubs.

**Non-obvious gotchas:**

- **NOT_CONTROLLER retry differs by NodeProvider.** ACL RPCs use
  `NodeProvider::LeastLoadedBrokerOrActiveKController`. Because
  `AdminMetadataManager::using_bootstrap_controllers()` is stubbed `false`, it behaves as
  `LeastLoaded` — the NOT_CONTROLLER retry goes straight to a least-loaded broker WITHOUT an
  interposed metadata refresh (unlike `NodeProvider::Controller`, which holds the retry pending
  until a Metadata/DescribeCluster response arrives). **How to apply:** `*ToController` unit tests
  prepare `[NOT_CONTROLLER, OK]`, NOT `[NOT_CONTROLLER, Metadata, OK]` — the latter mismatches
  because the retry consumes the Metadata prepared response (MockClient is FIFO at send time).
  Java's real test sets bootstrap.controllers and expects a DescribeCluster refresh; document the
  stub deviation in the test.

- **`resource/resource.rs` needs `#[allow(clippy::module_inception)]`** on `pub mod resource;` in
  `resource/mod.rs` (first same-name nesting in the tree; CLAUDE.md one-class-per-file forces it).

- **Fallible constructors return `Result<_, KafkaError::illegal_argument>`**: `ResourcePattern::new`
  (rejects ANY resourceType, MATCH/ANY patternType) and `AccessControlEntry::new` (rejects ANY
  op/perm). Java's null-name NPE test on `ResourcePattern` is N/A (Rust `name: String`).

- **`DeleteAclsResult::all()`** needs a custom `KafkaFutureOps<Vec<AclBinding>>` impl (in
  delete_acls_result.rs) that awaits each per-filter future and flattens FilterResults, surfacing
  the first per-ACL exception — `then_apply_try` can't because it only receives `()` from `all_of`
  and there's no sync value getter on KafkaFuture. `KafkaFutureOps` + `KafkaFuture::new` are
  `pub(crate)`, usable from admin.

- **Authorizer integration fixture:** `cluster_config::authorizer_single_broker()` sets
  `KAFKA_AUTHORIZER_CLASS_NAME=org.apache.kafka.metadata.authorizer.StandardAuthorizer` +
  `KAFKA_SUPER_USERS=User:ANONYMOUS`. Server props become `KAFKA_*` env vars on the container.
  Live wire-level `TopicAuthorizationException` is NOT observable for the test client: it connects
  over PLAINTEXT as anonymous = super user, which StandardAuthorizer never gates. The end-to-end
  test uses a create→describe→delete DENY-rule proxy instead; a real denial needs a second SASL
  principal. Poll `describe_acls` after create/delete (KRaft metadata propagation delay).
