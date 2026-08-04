---
name: review-m11-phase3-cluster-configs
description: M11 Tier1 Phase3 (describeCluster/Configs, incrementalAlterConfigs, listConfigResources) review findings + mock-parity trap
metadata:
  type: project
---

M11 Tier 1 Phase 3 "Cluster & configs" Critic review (commits 30e0b38, 3fc923d,
e569f69, 240a04b). Builds on [[review-m11-phase2-admin-driver]] /
[[review-m11-phase1-admin]].

**Network RPCs were clean.** describe_cluster (DescribeCluster→Metadata UV
fallback via Arc<AtomicBool> flag), describe_configs (per-node routing via
node_for, tested with prepare_response_for_node), incremental_alter_configs
(node_for routing + unified least-loaded), list_config_resources all faithful.
Wire wrappers have byte-level vectors (v1/v4) with correct field order
(AlterableConfig = name,configOperation,value; AlterConfigsResource =
resourceType,resourceName,configs). OpType ids SET0/DEL1/APP2/SUB3 and
ConfigResourceType ids GROUP32/CM16/BL8/BROKER4/TOPIC2/UNK0 verified. UV-for-
fenced-brokers test present. valid_acl_operations_or_null → None on
AUTHORIZED_OPERATIONS_OMITTED (matches Java null).

**Two real findings, both in MockAdminClient (mock_admin_client.rs):**
1. describe_cluster timeout branch uses `let state` (immutable) and never
   decrements timeout_next_requests; Java does `--timeoutNextRequests` and every
   OTHER rust mock method does `let mut state; state.timeout_next_requests-=1`.
   → infinite timeout. Behavior mismatch.
2. Mock describe_configs / incremental_alter_configs / list_config_resources all
   return unsupported_version("Not implemented yet"), BUT Java's MockAdminClient
   FULLY IMPLEMENTS all three (describeConfigs 821-845, incrementalAlterConfigs
   898-915, listConfigResources 1398-1427 — reads in-memory config maps). §9
   only grants unsupported-error for "Tier 3 methods Java leaves as
   UnsupportedOperationException". The code comment claiming Java's
   listConfigResources "throws UnsupportedOperationException" is FACTUALLY FALSE.

**Trap for future mock reviews:** do NOT trust the Actor's "Java's mock throws
UnsupportedOperationException / no in-scope test exercises it" justification —
grep MockAdminClient.java for the actual `public ... methodName` and read the
body. Java's MockAdminClient implements far more than §9's "Tier 3 stubs" list
implies. Verify each mock method's Java body before accepting an unsupported
deviation.

**Skipped test correctly out-of-scope:** testIncrementalAlterConfigsToController
uses BOOTSTRAP_CONTROLLERS_CONFIG (KIP-919, out of scope). NOT_CONTROLLER retry
path for alter is therefore untested but shares handle_not_controller_error with
createTopics (tested P1/P2). testDescribeClusterHandleUnsupportedVersionFor
IncludingFencedBrokers IS translated (not bootstrap.controllers).
