---
name: m11-phase4-log-dirs-notes
description: M11 Tier 1 Phase 4 (Admin describeLogDirs/alterReplicaLogDirs/describeReplicaLogDirs) — COMPLETE
metadata:
  type: project
---

Milestone 11 (AdminClient) Tier 1 Phase 4 "Log dirs", Actor N=1. COMPLETE + green
(build + `cargo test --lib` 2289 pass + format-check + xtask lint clean + 4 real-broker
integration tests all green). Rust core + tests ONLY (NO C FFI / Python). Builds on
[[m11-phase3-cluster-configs-notes]].

**Dispatch paths (verified from Java 4.2 KafkaAdminClient source):** all three are the plain
`Call` path with **per-broker fan-out via `NodeProvider::ConstantNodeId(broker_id)`** (NO
AdminApiDriver).
- `describeLogDirs(Collection<Integer> brokers)` → one `Call` per broker id;
  `DescribeLogDirsRequest` with `topics=null` (all partitions). Empty-result branch: top-level
  error None → CLUSTER_AUTHORIZATION_FAILED, else the coded error (up to v3 there was no
  top-level error code). Futures keyed by broker id (`i32`).
- `alterReplicaLogDirs(Map<TPR,String>)` → grouped by destination broker into one
  `AlterReplicaLogDirsRequestData` each ("find-or-create" AlterReplicaLogDir by path, then
  AlterReplicaLogDirTopic by name, then push partition). One `Call` per broker; futures shared
  (`Arc<HashMap<TPR, KafkaFutureImpl<()>>>`), each call completes/fails only its own broker's
  replicas + a completeUnrealizedFutures sanity sweep (UnknownServerError).
- `describeReplicaLogDirs(Collection<TPR>)` → **built on `DescribeLogDirsRequest`** (per broker,
  with the specific topic-partitions), then reshapes the response into per-replica
  `ReplicaLogDirInfo`. Seeds `replica_dir_info_by_partition` (default ReplicaLogDirInfo) per
  broker in the method, captured mutably in the FnMut handle_response closure. KafkaStorage error
  → skip that dir; any other dir error → illegal-state fail-all (mirrors Java). handleFailure
  fails ALL futures (not just this broker's) — so futures are a shared `Arc`.

**Wire wrappers (src/common/requests/):** DescribeLogDirs (apiKey 35, v1-4, flex 2+) +
AlterReplicaLogDirs (apiKey 34, v1-2, flex 2+). Both already had generated data structs from
`generator/messages/*.json`. `describe_log_dirs_response.rs` exports `INVALID_OFFSET_LAG=-1` and
`UNKNOWN_VOLUME_BYTES=-1` consts (Java's `DescribeLogDirsResponse` statics). errorCounts:
DescribeLogDirs includes top-level code + one per per-dir result; AlterReplicaLogDirs aggregates
per partition. `AlterReplicaLogDirsRequest.partition_dirs()` + `get_error_response` (flat-map
dirs→topics→partitions) translated for the *RequestTest. Wired all 12 ConcreteRequest arms + 11
ConcreteResponse arms.

**New common type:** `common::TopicPartitionReplica` (topic/partition/broker_id; derives
Hash/Eq/Clone; Display = `topic-partition-brokerId`). Re-exported from `common::`.

**Admin POJOs/results:** `ReplicaInfo`(size/offset_lag/is_future), `LogDirDescription`
(error: Option<KafkaError>, replica_infos: HashMap<TP,ReplicaInfo>, total/usable bytes as
Option<i64> — raw -1 → None via `with_volume_bytes`), `DescribeLogDirsResult`
(descriptions()+all_descriptions() via join_map), `AlterReplicaLogDirsResult`
(values()+all() via all_of), `DescribeReplicaLogDirsResult`+`ReplicaLogDirInfo`
(Default = (None,-1,None,-1); values()+all()). Options are the trivial timeout-only pattern.

**MockAdminClient (§9 — Java's mock implements ALL THREE, so all translated faithfully):**
added State fields `broker_log_dirs: Vec<Vec<String>>` (seeded `DEFAULT_LOG_DIRS=["/tmp/kafka-logs"]`)
+ `replica_moves: HashMap<TPR, ReplicaLogDirInfo>`; TopicMetadata gained `partition_log_dirs`
(from each partition leader's first log dir, populated in `add_topic` + `create_topics`). Added
inherent `set_broker_log_dirs(broker_id, dirs)` (mirrors Java Builder.brokerLogDirs) for
multi-log-dir tests. `complete`/`complete_exceptionally` return `bool` → match arms need `;`.

**Tests:** wire byte-vector + round-trip + error-response/error-counts (13) + POJO unit tests +
12 KafkaAdminClientTest slices (describeLogDirs base/volume-bytes/offline-dir/partial-failure;
describeReplicaLogDirs base/unexpected/non-exist-replica; alterReplicaLogDirs
success/log-dir-not-found/unrequested/partial-response/partial-failure) + 3 mock tests.
Timeout partial-failure tests: `env_with_props(default.api.timeout.ms=60000, retries=0)`,
prepare only one node's response, pump until no pending, `time.sleep(timeout+1)`, pump until
done, assert `matches!(err, KafkaError::Timeout(_))`. `prepare_response_for_node(resp, &nodes[i])`
+ ConstantNodeId(i) resolves via seeded cluster nodes (3-node env).

**Integration (`tests/integration/admin_log_dirs_test.rs`, 4 tests green on real 4.2.0 broker):**
describe_log_dirs sizes; describe_replica_log_dirs current dir; alter to unknown dir → error
(LogDirNotFound|KafkaStorageError|ReplicaNotAvailable); **genuine cross-dir move IS testable** —
started a dedicated 1-broker cluster with `KAFKA_LOG_DIRS=/tmp/kafka-logs-0,/tmp/kafka-logs-1`
via `ClusterConfig::with_properties`, moved the replica to the other dir, asserted future/current
dir == target. Broker id obtained via `describe_cluster().nodes()`. xtask lint does NOT compile
integration tests; my file had zero clippy warnings (others pre-existing).
