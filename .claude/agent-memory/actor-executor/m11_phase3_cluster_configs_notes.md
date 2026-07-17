---
name: m11-phase3-cluster-configs-notes
description: M11 Tier 1 Phase 3 (Admin describeCluster/describeConfigs/incrementalAlterConfigs/listConfigResources) — COMPLETE
metadata:
  type: project
---

Milestone 11 (AdminClient) Tier 1 Phase 3 "Cluster & configs", Actor N=1. COMPLETE + green
(build + `cargo test --lib` 2233 pass + format-check + xtask lint clean + 5 real-broker
integration tests). Rust core + tests ONLY (NO C FFI / Python, same banner as Phases 1/2).
Builds on [[m11-phase2-admin-driver-notes]] / [[m11-phase1-admin-notes]].

**Dispatch paths (verified from Java 4.2 KafkaAdminClient source):** all four are plain `Call`
path (NO AdminApiDriver).
- `describeCluster` → `DescribeClusterRequest` (endpoint_type=BROKER since bootstrap.controllers
  unsupported) with **UnsupportedVersion fallback to MetadataRequest** (empty topics,
  allow_auto_topic_creation=true). Node provider = `LeastLoadedBrokerOrActiveKController`.
  Fallback gated by an `Arc<AtomicBool>` `use_metadata_request` flag shared across
  create_request / handle_response / handle_uv (same pattern as describeTopics'
  `supports_disabling` flag). handle_uv returns false if includeFencedBrokers (v2+ only) so it
  does NOT fall back. Response-error branch fails the 4 futures directly (mirrors Java
  `handleFailure`), NOT via HandleResult::Retry.
- `describeConfigs` → per-resource-type routing via `node_for(resource)`: BROKER(non-default) +
  BROKER_LOGGER go to `ConstantNodeId(Integer.valueOf(name))`; everything else (TOPIC/GROUP/
  CLIENT_METRICS/default-BROKER) → `LeastLoadedBrokerOrActiveKController`. One `Call` per node.
  `node_for` parse-failure degrades to None (any broker) not panic.
- `incrementalAlterConfigs` → same `node_for` routing; groups resources into per-node
  ConstantNodeId calls + one unified least-loaded call. `submit_incremental_alter_configs` is a
  **method** on KafkaAdminClient (needs &self for submit/now). Response errors via
  `errors_by_resource()` → api_error per resource; also runs handle_not_controller_error.
- `listConfigResources` → `ListConfigResourcesRequest` (resource_types = set ids, empty = all),
  `LeastLoadedNode`. Wire wrapper reused later by Tier-3 listClientMetricsResources.

**NodeProvider::LeastLoadedBrokerOrActiveKController** ADDED (call.rs). provide() == LeastLoaded
(bootstrap.controllers always false here) but supports_use_controllers()==true. Comment explains
the equivalence.

**New common type: `common::config::{ConfigResource, ConfigResourceType}`** (config_resource.rs).
Java nested `ConfigResource.Type` → top-level `ConfigResourceType` (deviation: `Type` too generic
at module scope; documented). Type ids: GROUP=32 CLIENT_METRICS=16 BROKER_LOGGER=8 BROKER=4
TOPIC=2 UNKNOWN=0. `AlterConfigOp`+`OpType` in admin (OpType ids SET=0 DELETE=1 APPEND=2
SUBTRACT=3; for_id→Option). Added `ConfigType::for_id` (config_entry.rs).

**Wire wrappers (all flexible):** DescribeConfigs v1-4 (flex 4+), IncrementalAlterConfigs v0-1
(flex 1+), ListConfigResources v0-1 (flex 0+), DescribeCluster v0-2 (flex 0+). ListConfigResources
Builder has a v0 guard (CLIENT_METRICS only, empty data) mirroring Java. DescribeCluster
endpoint_type is v1+, include_fenced_brokers v2+. Constants ENDPOINT_TYPE_BROKER=1/CONTROLLER=2 in
describe_cluster_request.rs. IncrementalAlterConfigs Builder(resources,configs,validateOnly) folds
admin types → data; in Rust that assembly is in KafkaAdminClient (common::requests must not depend
on admin), builder is from_data only — documented deviation.

**Results:** DescribeClusterResult(4 futures: nodes Vec<Node>, controller Option<Node>, cluster_id
String, authorized_operations Option<BTreeSet<AclOperation>>). authorizedOperations completes with
None when AUTHORIZED_OPERATIONS_OMITTED (helper `valid_acl_operations_or_null`; existing
`valid_acl_operations` returns EMPTY set for omitted so it can't distinguish null — needed the
_or_null variant). DescribeConfigsResult/AlterConfigsResult keyed by ConfigResource with
values()/all() (join_map / all_of). ListConfigResourcesResult single future.

**Tests:** 39 wire/POJO/options unit tests + 13 KafkaAdminClientTest slices (describeCluster base/
error/failback/fenced-UV; describeConfigs broker/broker+logger/partial/unrequested/client-metrics;
incrementalAlterConfigs error-matrix+success; listConfigResources full/empty/not-supported) + 5
real-broker integration tests (admin_cluster_configs_test.rs).
**SKIPPED:** testIncrementalAlterConfigsToController (requires bootstrap.controllers, out of scope).
MockAdminClient config methods (describeConfigs/incrementalAlterConfigs/listConfigResources) are
unsupported-error deviations (§9) — only describeCluster is real in the mock; Java's mock config
storage is substantial and no in-scope test exercises it.

**Test-harness reuse:** same `env()` (mock_cluster(3,0), MockTime@1000) + `pump`/`pump_until`
+ `client_mut().prepare_response[_for_node]` as Phases 1/2. describeConfigs broker routing needs
`prepare_response_for_node(resp, &nodes[i])`; ConstantNodeId(i) resolves via seeded cluster nodes.
describeCluster response nodes come from the RESPONSE not the seeded cluster, so 4-broker assertions
work with a 3-node send cluster. prepare_unsupported_version_response() drives the UV failback.

**Clippy gotcha:** `&[x.clone()]` for a single-element slice arg → clippy wants
`std::slice::from_ref(&x)`. xtask lint does NOT compile integration tests; the integration clippy
warnings seen are pre-existing in other files (admin_partitions_records_test, common).
