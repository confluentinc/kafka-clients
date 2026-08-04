---
name: review-m11-tier3-phase1-acls
description: M11 Tier3 Phase1 ACLs review — NOT-CONTROLLER routing is call-body not node-provider; response byte-vectors adjudicated non-defect; table-driven test 1:1 fidelity gap
metadata:
  type: project
---

Milestone 11 Tier 3 Phase 1 (ACLs: create/describe/delete + common.acl/common.resource primitives). Range `c767725..214760f`. One LOW finding written.

**NOT_CONTROLLER retry is CALL-BODY logic, not node-provider logic (risk-3 trap).** `handleNotControllerError` runs at the top of `createAcls`/`deleteAcls` `handleResponse` (NOT `describeAcls`) and fires regardless of the node provider / `usingBootstrapControllers`. It does clear_controller + request_update + throw(NotController) → retry. So the retry-after-NOT_CONTROLLER contract is preserved even when `bootstrap.controllers` is stubbed off. The `*ToController` Java tests set `bootstrap.controllers="dummy"` (route-to-controller + interposed DescribeCluster); with the documented Tier-1 stub (`using_bootstrap_controllers=false`, kafka_admin_client.rs:222) the provider behaves as LeastLoaded and retries straight to a broker with no interposed metadata call. The Actor "adjusted" the two tests to drop the DescribeCluster step — this is a faithful consequence of the pre-existing documented stub, NOT papering over a supported-path divergence. Adjusted tests still assert NOT_CONTROLLER→retry→success with teeth. Verdict: not a defect. **Lesson: before flagging a node-provider/routing "divergence", check whether the behavior actually lives in the Call's handle_response (call body) vs the NodeProvider.provide() — they are independent.**

**Response byte-vector tests are adjudicated NON-DEFECT in this repo.** No response wrapper anywhere (create_topics_response, describe_cluster_response, ... , *acls_response) has a byte-level decode test; only some requests do (describe_cluster_request, the 3 new *acls_request). DoD #3's byte-vector rule is applied to the send/request path; responses rely on round-trip + generated-codegen tests. Prior M11 Phase 4 review already noted "response byte-vectors" as a non-defect deviation. Do NOT flag missing response byte-vector tests. The 3 ACL request byte-vectors are hand-computed, correct, and cover the flexible v3 path (compact strings/arrays + tagged trailer, compact-nullable-string null = 0x00).

**Table-driven dedicated tests: check 1:1 fidelity per file, they diverge.** `ResourceTypeTest` was translated faithfully (INFOS table, full test_name/test_is_unknown/test_code/test_exhaustive loops). `AclOperationTest`/`AclPermissionTypeTest` were rewritten as spot-checks (from_string on 3 of 16 / 3 of 4; is_unknown on 2 variants), dropping the exhaustive `testName`/`testIsUnknown` loops. That is the one finding filed (Missing Requirement / plan says "1:1"). `from_string` matches on to_uppercase() against N hard-coded arms — spot-checks leave most arms unverified. Impl was correct on inspection, so coverage gap not live bug.

**Verified faithful (no finding):**
- `from_32_bit_field` (utils/mod.rs:55): uses `int_value as u32` for Java's `>>>` unsigned shift. Correct. Single def, no dup.
- Reused `AclOperation`/`AclPermissionType` code values byte-identical to Java (0..15 / 0..3). Only Display added (returns enum-constant NAME, matches Java default enum toString).
- `find_indefinite_field`: AccessControlEntryData order = principal/host/op-ANY/op-UNKNOWN/perm-ANY/perm-UNKNOWN; ResourcePatternFilter order = rtype-ANY/rtype-UNKNOWN/name-null/ptype-MATCH/ptype-UNKNOWN. NOTE: Java's ResourcePatternFilter does NOT check ptype==ANY (easy to mis-flag as "missing ANY branch" — it isn't). Messages match exactly.
- describe_acls short-circuit (is_unknown → InvalidRequest "The AclBindingFilter must not contain UNKNOWN elements.", no Call). Test asserts request_count() unchanged after pump — real no-network assertion.
- create_acls per-binding find_indefinite rejection ("Invalid ACL creation: {indefinite}") fails only its own future; valid bindings still sent (Vacant-entry dedup mirrors Java `futures.get==null`).
- DeleteAclsResult::all() via bespoke `AclBindingsFuture: KafkaFutureOps` (DoD #7 justified helper). Awaits per-filter futures, propagates first filter-level error via `?`, then first per-ACL exception. HashMap iteration order differs from Java allOf but both surface "an" error — Java allOf doesn't pin which. Not a divergence.
- Full ConcreteRequest/ConcreteResponse enum wiring for all 3 pairs (version/api_key/to_send/serialize/get_error_response/parse/error_counts/throttle/display). error_counts aggregates result-level codes only (not matching-acl codes) — matches Java.
- AclBinding derives Clone/Debug/PartialEq/Eq/Hash (HashMap dedup key).

**Integration test (d) authorizer-gating: defensible, not a DoD gap.** Anonymous PLAINTEXT is a super user (KAFKA_SUPER_USERS), StandardAuthorizer never gates it, so no TopicAuthorizationException is provocable without a 2nd SASL principal the harness doesn't spin up. Actor uses a create→describe→delete DENY-rule proxy. The ideal end-to-end gating asserts BROKER authorizer behavior, not Rust client code; the client wire path is already exercised by (a)/(c) round-trips through the live authorizer. Documented clearly. Not a client defect.

**risk-7 confirmed:** `AclBindingFilterTest`/`AccessControlEntry*Test`/`PatternTypeTest` genuinely absent in Java (find on common/acl + common/resource test dirs). `ResourcePatternTest.shouldThrowIfResourceNameIsNull` not representable (Rust name: String non-nullable) — legitimate documented N/A.
