# M18 P0 — Harness spike: share groups on the Docker test broker

Scope: what the Rust integration harness (`rust/tests/common/kafka_cluster.rs`) needs so the
translated `clients-integration-tests` share suites run, on the default broker image
`apache/kafka:4.2.0` and on 4.4.0 (`INTEGRATION_TEST_BROKER_TAG=4.4.0`). No harness code is written
in P0; §6 is the proposed P10a change.

Evidence is broker-side Java read from the `kafka/` submodule: tag `4.4.0`, and `git show 4.2.0:<path>`
/ `4.3.0:<path>` for the older releases. Citations are `Class#member`.

**Not verified live.** The Docker daemon was not running during the spike, so no broker was
started. Every claim below comes from the source. P10a's first step is to confirm §2 on a real
container (see §7).

## 1. How the harness configures the broker today

- `KafkaAllProtocols::new` builds the environment with `KAFKA_*` variables (node id, roles, quorum,
  listeners, `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR = min(brokers, 3)`, `KAFKA_LOG_RETENTION_MS=-1`).
  `ClusterConfig::server_properties` entries are added as extra env vars **by env-var name**
  (`KAFKA_FOO_BAR`, not `foo.bar`); see the `for (key, value) in &server_props` loop in
  `KafkaCluster` start-up.
- The image's `KafkaDockerWrapper#prepareServerConfigs` writes `server.properties` from the `KAFKA_*`
  env vars and copies the image default `docker/server.properties` **only when no env-derived config
  exists**. The harness always sets env vars, so the image defaults are not used. That matters:
  `docker/server.properties` sets `share.coordinator.state.topic.replication.factor=1` and
  `...min.isr=1`, and the harness gets neither.
- Storage is formatted by `KafkaDockerWrapper#formatStorageCmd` as
  `format --cluster-id=<id> -c server.properties`: no `--release-version`, no `--feature`.
  `Formatter#calculateEffectiveReleaseVersion` therefore picks `MetadataVersion.latestProduction()`,
  and `Formatter#calculateEffectiveFeatureLevels` picks each feature's
  `Feature#defaultLevel(releaseVersion)`.
- Version gating precedent: `kafka_cluster::broker_release()` returns `(major, minor)` of the image
  tag. `consumer_bounce_test.rs` (`ASSIGNMENT_INTERVAL_RELEASE`) skips arms with an `eprintln!`
  reason, and `consumer_test.rs` documents the untranslatable `testRackAwareAssignment`.

## 2. Is the share group feature on? — yes on both, with no extra setting

| | 4.2.0 | 4.4.0 |
|---|---|---|
| `MetadataVersion.LATEST_PRODUCTION` | `IBP_4_2_IV1` | `IBP_4_4_IV2` |
| `ShareVersion` levels | `SV_0`, `SV_1` (bootstrap MV `IBP_4_2_IV0`) | adds `SV_2` (bootstrap MV `IBP_4_4_IV0`, DLQ, KIP-1191) |
| `share.version` after the image's `format` (`Feature#defaultVersion`: the highest level whose bootstrap MV ≤ release MV) | **1** | **2** |
| Share RPCs enabled (`KafkaApis#isShareGroupProtocolEnabled`) | `group.share.enable \|\| shareVersion().supportsShareGroups` → true | `shareVersion().supportsShareGroups` → true |
| DLQ enabled (`ShareVersion#supportsShareGroupDLQ`, passed to `SharePartitionManager` in `BrokerServer#startup`) | no (no `SV_2`) | yes |

- `group.coordinator.rebalance.protocols` is **not needed** on either. Its default
  (`GroupCoordinatorConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_DEFAULT`) is
  `classic, consumer, streams` in both 4.2.0 and 4.4.0, and nothing in the share path reads it. Share
  enablement is gated only by `share.version` (`KafkaApis#isShareGroupProtocolEnabled` for
  `ShareGroupHeartbeat`, `ShareGroupDescribe`, `ShareFetch`, `ShareAcknowledge` and
  `DescribeShareGroupOffsets`; `ShareCoordinatorService#isShareGroupsEnabled` for the share
  coordinator).
- No `KAFKA_*` feature override is needed. Setting `share.version` explicitly would need a harness
  `--feature` flag that the image's format command does not take.
- **Required on a 1-broker cluster:** `share.coordinator.state.topic.replication.factor` and
  `...min.isr`. The broker defaults are 3 and 2 (`ShareCoordinatorConfig.STATE_TOPIC_REPLICATION_FACTOR_DEFAULT`,
  `STATE_TOPIC_MIN_ISR_DEFAULT`, the same in 4.2.0 and 4.4.0), so on one broker `__share_group_state`
  can't be created and the share coordinator never loads. Every Java share class except
  `ShareConsumerRackAwareTest` (3 brokers, so the defaults fit) sets them (§3). The harness does not set them today (§1).
- Java's test kit is not identical to the image: `ClusterTest#metadataVersion` defaults to
  `IBP_4_5_IV0`, and `KafkaClusterTestKit` sets `unstable.feature.versions.enable` and
  `unstable.api.versions.enable` to `true`. For share this is **no difference**: 4.4.0 has no
  `ShareVersion` above `SV_2`, and the only spec with `"latestVersionUnstable": true` is
  `InitProducerIdRequest.json`. The ShareFetch, ShareAcknowledge, ShareGroupHeartbeat,
  ShareGroupDescribe and DescribeShareGroupOffsets version ranges are the same in 4.2.0 and 4.4.0
  (`1-2`, `1-2`, `1`, `1`, `0-1`).

## 3. `serverProperties` used by the Java share suites

Common `@ClusterTestDefaults` (Callback, Delivery, Lag, Renew and ShareConsumerTest; `types = KRAFT`,
1 broker, 1 controller):

| Key | Value | Exists in 4.2.0? | Note |
|---|---|---|---|
| `auto.create.topics.enable` | false | yes | |
| `group.share.max.partition.max.record.locks` | 10000 | **no** (added 4.3) | 4.2.0 ignores an unknown broker key (`AbstractConfig#logUnused`); on 4.3+ it is **required** alongside the next row, since `ShareGroupConfig` checks `maxPartitionMaxRecordLocks >= partitionMaxRecordLocks` (default max 4000) |
| `group.share.partition.max.record.locks` | 10000 | yes (`between(100, 10000)`) | |
| `group.share.record.lock.duration.ms` | 15000 | yes | |
| `offsets.topic.replication.factor` | 1 | yes | the harness already sets `min(brokers, 3)` |
| `share.coordinator.state.topic.min.isr` | 1 | yes | required, see §2 |
| `share.coordinator.state.topic.num.partitions` | 3 | yes | default 50; keeps start-up fast |
| `share.coordinator.state.topic.replication.factor` | 1 | yes | required, see §2 |
| `transaction.state.log.min.isr` / `.replication.factor` | 1 / 1 | yes | |

`ShareConsumerDLQTest` drops the two `transaction.state.log.*` keys and adds
`group.share.min.heartbeat.interval.ms=1500` and `group.share.heartbeat.interval.ms=1500`.
`ShareConsumerRackAwareTest` uses `brokers = 3`, per-broker `broker.rack` (`@ClusterConfigProperty(id = …)`)
and `group.share.assignors=org.apache.kafka.clients.consumer.RackAwareTestAssignor`.

Per-test overrides:

| Key | Tests | 4.2.0 | 4.3 | 4.4 |
|---|---|---|---|---|
| `group.share.assignment.interval.ms` (0 / 1000 arms of `@ClusterTests`) | 8 methods in `ShareConsumerTest` (`testSubscriptionAndPoll`, `testSubscriptionAndPollMultiple`, `testMultipleConsumersInGroupSequentialConsumption`, `testMultipleConsumersInGroupConcurrentConsumption`, `testMultipleConsumersInMultipleGroupsConcurrentConsumption`, `testMultipleConsumersInGroupFailureConcurrentConsumption`, `testSubscriptionFollowedByTopicCreation`, `testSubscriptionAndPollFollowedByTopicDeletion`); `ShareConsumerRackAwareTest` | absent (ignored) | yes | yes |
| `group.share.delivery.count.limit` (2, 3, 10) | 5 Delivery tests | yes (`between(2, 10)`) | yes | yes |
| `group.share.min.record.lock.duration.ms` + `group.share.record.lock.duration.ms` | `ShareConsumerTest#testAcquisitionLockTimeoutOnConsumer` (5000), `ShareConsumerRenewTest#testRenewAcknowledgementNoResultInPoll` (12000) | yes | yes | yes |
| `group.share.max.size`, `group.share.max.share.sessions` | `testShareGroupMaxSizeConfigExceeded`, `testShareGroupShareSessionCacheIsFull` | yes | yes | yes |
| `brokers = 3` + 3-way offsets / share-state topics | `testShareConsumerAfterCoordinatorMovement`, `testComplexShareConsumer`, Lag `...OnGroupCoordinatorMovement`, `...OnShareCoordinatorMovement` | yes | yes | yes |
| `errors.deadletterqueue.auto.create.topics.enable` | DLQ `testRejectedRecordsWrittenToAutoCreatedDlq` | absent | absent | yes |
| `remote.log.storage.*` with `LocalTieredStorage`, `remote.log.metadata.manager.listener.name=EXTERNAL`, `rlmm.config.*`, `log.retention.check.interval.ms`, `log.initial.task.delay.ms` | DLQ `testDlqCopiesRecordsReadFromRemoteStorage`, `testDlqCopiesRecordsReadFromRemoteAndLocalStorage` | — | — | see §4 |

Group-level configs the tests set at runtime through `Admin#incrementalAlterConfigs` on a `GROUP`
resource (`ShareConsumerTestBase#alterShareGroupConfig`). An unknown key there **fails**, it is not
ignored: `GroupConfig#validateNames` throws `InvalidConfigurationException("Unknown group config
name: …")`.

| Group config | 4.2.0 | 4.3 | 4.4 | Used by |
|---|---|---|---|---|
| `share.auto.offset.reset`, `share.isolation.level`, `share.record.lock.duration.ms` | yes | yes | yes | most tests; 5 isolation-level Delivery tests; `ShareConsumerRenewTest#testRenewAcknowledgementOnCommitSync` |
| `share.delivery.count.limit` | no | yes | yes | Delivery `testDynamicDeliveryCountLimitDecreaseArchivesRecords`, `testDynamicDeliveryCountLimitIncreaseAllowsMoreDeliveries`; DLQ `testMixedAcknowledgementTypesWrittenToDlq`, `testReleasedRecordsExceedingDeliveryCountWrittenToDlq` |
| `share.partition.max.record.locks` | no | yes | yes | Delivery `testDynamicPartitionMaxRecordLocks` |
| `share.renew.acknowledge.enable` (KAFKA-20161) | no | yes | yes | Renew `testRenewAcknowledgementDisabled` |
| `errors.deadletterqueue.topic.name`, `errors.deadletterqueue.copy.record.enable` | no | no | yes | all DLQ tests |

The DLQ suite also sets the topic config `errors.deadletterqueue.group.enable`
(`TopicConfig.ERRORS_DEADLETTERQUEUE_GROUP_ENABLE_CONFIG`), which does not exist in 4.2.0.

## 4. Which tests need which broker

Counts, from the annotations: methods annotated `@ClusterTest` / `@ClusterTests`, and the cluster
runs (one per `@ClusterTest`, including the arms inside `@ClusterTests`).

| Class | Methods | Runs |
|---|---|---|
| `ShareConsumerTest` | 54 | 62 |
| `ShareConsumerDeliveryTest` | 26 | 26 |
| `ShareConsumerDLQTest` | 13 | 13 |
| `ShareConsumerLagTest` | 8 | 8 |
| `ShareConsumerCallbackTest` | 8 | 8 |
| `ShareConsumerRenewTest` | 5 | 5 |
| `ShareConsumerRackAwareTest` | 1 | 2 |
| **Total** | **115** | **124** |

**These differ from PLAN §1.2 / §4** (140 total; 71 / 27 / 14 / 9 / 9 / 6 / 4). The plan numbers
look like raw `grep -c '@ClusterTest'` counts, which also match the import line and the
`@ClusterTests` wrapper. Use the table above for P10 sizing.

Broker requirements, derived from §2 and §3:

| Requirement | Tests | Evidence |
|---|---|---|
| **≥ 4.4** (`share.version=2` / DLQ) | all 13 `ShareConsumerDLQTest` | `ShareVersion#supportsShareGroupDLQ` (`SV_2`, bootstrap MV `IBP_4_4_IV0`); DLQ group/topic/broker configs exist only in 4.4.0 |
| **≥ 4.3** (group configs) | Delivery `testDynamicDeliveryCountLimitDecreaseArchivesRecords`, `testDynamicDeliveryCountLimitIncreaseAllowsMoreDeliveries`, `testDynamicPartitionMaxRecordLocks`; Renew `testRenewAcknowledgementDisabled` | `GroupConfig` in 4.2.0 lacks `share.delivery.count.limit`, `share.partition.max.record.locks`, `share.renew.acknowledge.enable` → `validateNames` rejects |
| **≥ 4.3, one arm only on 4.2** | the 8 `@ClusterTests` methods in `ShareConsumerTest` | `group.share.assignment.interval.ms` was added in 4.3.0. On 4.2.0 both arms run identical brokers; follow the `consumer_bounce_test.rs` precedent (one default-behaviour test on < 4.3, the two Java arms on ≥ 4.3) |
| any (4.2.0 works) | everything else, including Lag (8), Callback (8), the other 4 Renew tests, and the isolation-level Delivery tests | RENEW is handled by the 4.2.0 broker (`SharePartition#acknowledgePerOffsetBatchRecords` branches on `AcknowledgeType.RENEW`); Lag is computed by 4.2.0's `GroupCoordinatorService#readShareGroupStateSummary` (DescribeShareGroupOffsets v1, KIP-1226). The ShareFetch v2 fields (`IsRenewAck`, acquire mode, KIP-1206/KIP-1222) are in the 4.2.0 spec |
| **not runnable on the Docker image** | `ShareConsumerRackAwareTest` (2 runs) | `RackAwareTestAssignor` is a class in `clients-integration-tests` test sources, which the image cannot load. Same reason as the documented `consumer_test.rs` skip of `testRackAwareAssignment` |
| **not runnable on the Docker image** (high confidence, unverified) | DLQ `testDlqCopiesRecordsReadFromRemoteStorage`, `testDlqCopiesRecordsReadFromRemoteAndLocalStorage` | `LocalTieredStorage` lives in `storage/src/testFixtures`, which the release tarball does not ship. They also set `remote.log.metadata.manager.listener.name=EXTERNAL`, Java's test-kit listener name, which the harness doesn't have (its broker listeners are `BROKER`, `PLAINTEXT`, …) |

Uncertainty: "4.2.0 works" means the broker accepts every request and config the test uses. It does
not prove 4.2.0 broker *behaviour* matches 4.4.0 for every assertion: KIP-1222 renew and the lag
semantics changed between 4.2 and 4.4 (e.g. KAFKA-20161 is 4.3+). Running the suite on both images
in P10 is the check.

Cluster-lifecycle needs (independent of version). These tests stop or restart brokers, so they need
`dedicated: true` clusters:
`ShareConsumerTest#testConsumerCloseOnBrokerShutdown`,
`#testLeaderRestartWithoutLeadershipChangeExplicitAcknowledgementSync`,
`#…ExplicitAcknowledgementAsync`, `#…ImplicitAcknowledgement`,
`#testShareConsumerAfterCoordinatorMovement`, and
`ShareConsumerLagTest#testSharePartitionLagOnGroupCoordinatorMovement`,
`#testSharePartitionLagOnShareCoordinatorMovement`.
Java uses `Type.KRAFT` (isolated controller), so stopping the only broker keeps the quorum.
`KafkaCluster::shutdown_broker` / `start_broker` and `Type::Kraft` already exist.

`ShareConsumerTestBase#waitForMetadataCache` reads `cluster.brokers().get(0).metadataCache()` on the
`EXTERNAL` listener. That is broker-internal; translate it with `Admin#describeCluster`.

Admin dependency (decision D1): the Lag suite and every test that calls
`sharePartitionOffsetInfo`, `verifySharePartitionStartOffset/Lag`, `alterShareGroupOffsets`,
`deleteShareGroupOffsets` or `describeShareGroups` needs the P9 admin share-group RPCs. That is a
separate gate from the broker version.

## 5. Conclusion

- The default `apache/kafka:4.2.0` image supports share groups out of the box (`share.version=1` after
  format). The only mandatory broker settings are the share-state topic replication factor and
  min ISR on clusters with fewer than 3 brokers. The Java suites set them anyway.
- On 4.2.0, 97 tests can run: the 124 runs minus 13 DLQ, minus 2 rack-aware, minus the 4 that
  need the 4.3+ group configs (= 105), with the 8 two-arm assignment-interval methods collapsed to
  one test each (−8).
  The DLQ suite needs a 4.4 broker. Rack-aware and the 2 tiered-storage DLQ tests can't run on the
  image at all.

## 6. Proposed P10a harness change (not implemented in P0)

1. **Share-cluster config helper per test file.** Translate each class's `@ClusterTestDefaults`
   `serverProperties` verbatim into a `cluster_config()` helper, as `KAFKA_*` env-var names
   (`KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR`, `KAFKA_GROUP_SHARE_MAX_PARTITION_MAX_RECORD_LOCKS`,
   …), with `Type::Kraft`, `controllers: 1`. Per-test overrides extend that map. Keep
   `group.share.max.partition.max.record.locks`: 4.3+ needs it, and 4.2.0 ignores it.
2. **Harness safety default (optional, behaviour-neutral).** In `KafkaAllProtocols::new`, next to
   `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR`, set
   `KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR = min(brokers, 3)` and
   `KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR = min(brokers, 2)`, overridable by
   `server_properties`. The image would have given 1/1 if the harness didn't override its defaults.
   It only affects a cluster where a share group is used, and it makes ad-hoc share use (e.g. an
   admin test from P9) work on a 1-broker pooled cluster.
3. **Release gates** in a shared test module, using `kafka_cluster::broker_release()` and the
   existing skip-with-reason style:
   `SHARE_GROUP_CONFIGS_RELEASE = (4, 3)` (dynamic group configs, renew-enable),
   `SHARE_ASSIGNMENT_INTERVAL_RELEASE = (4, 3)` (the 8 `@ClusterTests` methods: one default arm on
   < 4.3, the two Java arms on ≥ 4.3), `SHARE_DLQ_RELEASE = (4, 4)` (the whole DLQ suite). Each
   gate's doc comment cites the broker class that introduces it (§3, §4).
4. **Documented skips** (module docs, as `consumer_test.rs` does): `ShareConsumerRackAwareTest`
   (broker-side test assignor), and the 2 tiered-storage DLQ tests (`LocalTieredStorage` is a
   test fixture, plus the `EXTERNAL` listener). A substitute that reads the member's rack back via
   `describe_share_groups` (`ShareMemberDescription`) is possible if D1 lands; it is not a
   translation of the Java test. Record both in the deferral ledger (R2).
5. **`dedicated: true`** for the 7 broker-restart tests in §4.
6. **CI matrix.** Run the share suites on the default 4.2.0 and on `INTEGRATION_TEST_BROKER_TAG=4.4.0`
   (check that the tag is published on Docker Hub when P10a starts). DLQ coverage exists only in the
   4.4 run.

## 7. Open items for P10a

- Confirm on a live 4.2.0 and 4.4.0 container that `kafka-features.sh --bootstrap-server … describe`
  reports `share.version` `FinalizedVersionLevel` 1 and 2 respectively, and that a share group
  heartbeat succeeds on a 1-broker cluster with only items 1–2 applied. Wrap the run as
  `lockf -t 120 -k /private/tmp/claude-501/m18-broker.lock timeout 900 …`.
- Confirm `LocalTieredStorage` is absent from `/opt/kafka/libs` in the image.
- P10 sizing: use 115 methods / 124 runs, not 140 (§4).
