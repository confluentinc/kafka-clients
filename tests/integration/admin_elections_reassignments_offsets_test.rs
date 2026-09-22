// Copyright 2025 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Integration tests for the admin elections / reassignments / offsets RPCs
//! against a real Kafka 4.2.0 broker.
//!
//! Mirrors the electLeaders / alterPartitionReassignments /
//! listPartitionReassignments / listOffsets scenarios of Java's
//! `PlaintextAdminIntegrationTest`, exercising the real network engine end to
//! end rather than the `MockClient` unit-test harness. `listOffsets` is the
//! only one of the four that reaches the broker through the `AdminApiDriver` /
//! `PartitionLeaderStrategy` multi-step engine (a partition-leader lookup
//! before the real request) rather than the simple `Call`/retry path.
//!
//! Each scenario is a body generic over
//! [`AdminBackendFactory`](crate::common::backend_factory::AdminBackendFactory)
//! and registered with [`multilanguage_admin_test!`], so it runs against the
//! native Rust client, the Python sync binding, the Python asyncio binding and
//! the C FFI — a disagreement between them shows up as three backends agreeing
//! and one not (see `design/history/Milestone-11/PLAN-multilanguage-admin.md`).
//! With only `integration-tests` enabled the `__rust` arm is the whole
//! expansion, and it drives the same production `Admin` trait against the same
//! broker as the single-backend tests these scenarios were converted from.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::time::Duration;

use confluent_kafka::admin::{
    AlterConfigOp, AlterConfigsOptions, AlterPartitionReassignmentsOptions, ConfigEntry, DescribeClusterOptions,
    DescribeTopicsOptions, ElectLeadersOptions, ListOffsetsOptions, ListPartitionReassignmentsOptions,
    NewPartitionReassignment, OffsetSpec, OpType,
};
use confluent_kafka::common::Errors;
use confluent_kafka::common::config::{ConfigResource, ConfigResourceType};
use confluent_kafka::common::serialization::ByteArraySerializer;
use confluent_kafka::common::{ElectionType, IsolationLevel, TopicPartition, TopicPartitionInfo};
use confluent_kafka::producer::{KafkaProducer, Producer, ProducerConfig, ProducerRecord};

use crate::common::admin_backend::{AdminBackend, admin_for, all_of_exactly, create_topic};
use crate::common::backend_factory::AdminBackendFactory;
use crate::common::cluster_config::ClusterConfig;
use crate::common::test_context::TestContext;
use crate::common::test_utils::{
    TOPIC_METADATA_PROPAGATION_WAIT_MS, retry_on_error_with_timeout, wait_until_true_with_timeout,
};
use crate::multilanguage_admin_test;

/// Records produced before the offset scenarios read them back.
const NUM_RECORDS: usize = 10;

/// Produce `num` records to `(topic, partition)`, waiting for the broker acks.
///
/// Always native and always against the *host* listener: the record producer is
/// not the object under test, so it does not go through the backend under test
/// (which for the container backends could not reach the host loopback anyway).
async fn produce_records(
    ctx: &TestContext,
    bootstrap: &str,
    topic: &str,
    partition: i32,
    num: usize,
    value_len: usize,
) {
    let mut props = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap.to_string()),
        ("client.id".to_string(), "integration-test-offsets-producer".to_string()),
        ("acks".to_string(), "all".to_string()),
        ("max.block.ms".to_string(), "30000".to_string()),
    ]);
    ctx.apply_security(&mut props);
    let config = ProducerConfig::new(&props).expect("valid producer config");
    let producer: KafkaProducer<Vec<u8>, Vec<u8>> =
        KafkaProducer::new(config, Box::new(ByteArraySerializer), Box::new(ByteArraySerializer))
            .expect("build producer");
    let value = vec![b'x'; value_len];
    let mut last = None;
    for i in 0..num {
        let record = ProducerRecord::with_partition_key(
            topic.to_string(),
            Some(partition),
            Some(format!("key {i}").into_bytes()),
            Some(value.clone()),
        )
        .expect("build record");
        last = Some(
            <KafkaProducer<Vec<u8>, Vec<u8>> as Producer<Vec<u8>, Vec<u8>>>::send(&producer, record)
                .await
                .expect("send"),
        );
    }
    producer.flush().await.expect("flush");
    if let Some(f) = last {
        f.get_with_timeout(Duration::from_secs(30)).await.expect("last send");
    }
    producer.close().await.expect("producer close");
}

/// Looks up one partition's offset for `spec`, returning the per-partition
/// outcome so a scenario can assert either arm.
///
/// Each spec goes in a call of its own, so a scenario asserting a per-key
/// failure never risks batching it into the same request as a success (see
/// [`list_offsets_covers_every_offset_spec_variant`] for why `earliestPendingUpload`
/// turned out not to be such a case against a real 4.2 broker — no offset spec
/// currently reaches this helper's error arm, but the isolation is kept for the
/// next one that does).
async fn offset_of<B: AdminBackend>(
    admin: &B,
    tp: &TopicPartition,
    spec: OffsetSpec,
    options: ListOffsetsOptions,
) -> Result<confluent_kafka::admin::ListOffsetsResultInfo, confluent_kafka::common::Error> {
    let outcomes = admin
        .list_offsets(&HashMap::from([(tp.clone(), spec)]), options)
        .await
        .unwrap_or_else(|e| panic!("{} backend: list offsets {spec:?}: {e}", admin.name()));
    outcomes
        .get(tp)
        .unwrap_or_else(|| panic!("{} backend: {tp} missing from the {spec:?} result", admin.name()))
        .clone()
}

/// Same, panicking on a per-partition failure.
async fn offset_ok<B: AdminBackend>(
    admin: &B,
    tp: &TopicPartition,
    spec: OffsetSpec,
) -> confluent_kafka::admin::ListOffsetsResultInfo {
    offset_of(admin, tp, spec, ListOffsetsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{} backend: list offsets {spec:?} for {tp}: {e}", admin.name()))
}

/// The broker ids of a multi-broker cluster, via `describeCluster`.
async fn broker_ids<B: AdminBackend>(admin: &B) -> Vec<i32> {
    let description = admin
        .describe_cluster(DescribeClusterOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{} backend: describe cluster: {e}", admin.name()));
    assert!(
        description.nodes.len() >= 2,
        "{} backend: this scenario requires a multi-broker cluster, saw {:?}",
        admin.name(),
        description.nodes
    );
    description.nodes.iter().map(|n| n.id()).collect()
}

/// Describes `topic` and returns its partition 0, retrying while the answering
/// broker still reports `UNKNOWN_TOPIC_OR_PARTITION` because a freshly created
/// topic has not reached it.
///
/// # Why the retry, rather than a longer wait before the call
///
/// `DescribeTopicPartitions` is one of the few admin APIs a broker answers
/// **itself** rather than forwarding to the controller
/// (`KafkaApis.scala:228` → `handleDescribeTopicPartitionsRequest`, versus
/// `ALTER_PARTITION_REASSIGNMENTS` / `LIST_PARTITION_REASSIGNMENTS` /
/// topic-scoped `INCREMENTAL_ALTER_CONFIGS`, which are all `forwardToController`
/// and so cannot lag the metadata log). It reads the receiving broker's own
/// metadata cache, so on the three-broker clusters in this file it answers
/// `UNKNOWN_TOPIC_OR_PARTITION` for a short window after `create_topics`
/// returns.
///
/// The wait inside [`create_topic`] cannot close that window: it also goes
/// through `describeTopics`, whose `Call` uses `NodeProvider::LeastLoaded`, so
/// it proves *one arbitrary* broker has the topic — not the (possibly
/// different) broker that answers the next describe. Java closes the window by
/// reading every broker's cache directly
/// (`TestUtils.waitForAllPartitionsMetadata` is
/// `brokers.forall { _.metadataCache.numPartitions(topic) == n }`,
/// `core/src/test/scala/unit/kafka/utils/TestUtils.scala:832-853`), which a
/// client cannot reproduce. So this uses Java's other idiom for the same
/// problem, `TestUtils.retryOnExceptionWithTimeout`: re-run the read until it
/// stops failing, bounded by the propagation bound.
///
/// The client is *correct* not to retry this itself — Java's describe-by-names
/// `Call` completes the per-topic future exceptionally on any topic-level error
/// (`KafkaAdminClient.java:2253-2254`, `if (error != Errors.NONE)` →
/// `future.completeExceptionally(error.error())`), with no retry — so the
/// wait belongs in the test.
///
/// Only `UNKNOWN_TOPIC_OR_PARTITION` is retried. Every other outcome — a failed
/// call, a missing key, any other per-topic error — panics on the first attempt,
/// because `retry_on_error_with_timeout` catches the `Err` return and not
/// panics. So this cannot turn a genuine backend defect into a 60-second
/// timeout.
///
/// One consequence worth naming: `alter_and_list_partition_reassignments` calls
/// this inside a 15s `wait_until_true_with_timeout`, so a topic that really had
/// vanished would now fail that poll after 60s rather than 15s. It still fails,
/// and no assertion is weakened.
async fn partition_zero_of<B: AdminBackend>(admin: &B, topic: &str) -> TopicPartitionInfo {
    let found: RefCell<Option<TopicPartitionInfo>> = RefCell::new(None);
    retry_on_error_with_timeout(Duration::from_millis(TOPIC_METADATA_PROPAGATION_WAIT_MS), || async {
        let backend = admin.name();
        let described = admin
            .describe_topics_with_topics(std::slice::from_ref(&topic.to_string()), DescribeTopicsOptions::new())
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: describe topics: {e}"));
        match described
            .get(topic)
            .unwrap_or_else(|| panic!("{backend} backend: {topic} missing from the describe result"))
        {
            Err(e) if e.error() == Errors::UnknownTopicOrPartition => Err(format!(
                "{backend} backend: describe topic {topic}: {e} \
                 (the topic has not reached the broker that answered describeTopics yet)"
            )),
            Err(e) => panic!("{backend} backend: describe topic {topic}: {e}"),
            Ok(description) => {
                *found.borrow_mut() = Some(description.partitions()[0].clone());
                Ok(())
            },
        }
    })
    .await;
    found.into_inner().expect("the attempt that returned Ok stored partition 0")
}

/// The broker currently leading `topic`'s only partition.
async fn sole_leader_of<B: AdminBackend>(admin: &B, topic: &str) -> i32 {
    partition_zero_of(admin, topic)
        .await
        .leader()
        .unwrap_or_else(|| panic!("{} backend: {topic}-0 has no leader", admin.name()))
        .id()
}

/// The ids of the brokers in `topic`-0's replica set, in `replicas()` order.
///
/// The reassignment-completion predicate reads *this* rather than the leader:
/// what a completed move means is that `removingReplicas` drained and the source
/// broker is gone from the replica set, which is the property
/// `assert_reassignment_completed` promises and the one that would differ for
/// RF > 1. (The leader-only check the first conversion of this file used happens
/// to become true at the same instant for an RF-1 → RF-1 move, because
/// `PartitionChangeBuilder` truncates `replicas` and moves the leader in one
/// completion step — but it is not the stated property.)
async fn replica_ids_of<B: AdminBackend>(admin: &B, topic: &str) -> Vec<i32> {
    partition_zero_of(admin, topic)
        .await
        .replicas()
        .iter()
        .map(|node| node.id())
        .collect()
}

/// Sets one config on `resource` and asserts the alteration succeeded.
async fn set_config<B: AdminBackend>(admin: &B, resource: &ConfigResource, name: &str, value: &str) {
    let ops = vec![AlterConfigOp::new(
        ConfigEntry::new(name.to_string(), Some(value.to_string())),
        OpType::Set,
    )];
    let altered = admin
        .incremental_alter_configs(&HashMap::from([(resource.clone(), ops)]), AlterConfigsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{} backend: set {name}={value} on {resource:?}: {e}", admin.name()));
    // `all_of_exactly` rather than `all_of`: the fold alone returns Ok for an
    // empty map, so a backend that answered with no entries at all would look like
    // a successful alteration.
    all_of_exactly(
        admin,
        &altered,
        std::slice::from_ref(resource),
        &format!("incrementalAlterConfigs setting {name}={value}"),
    );
}

// ---------------------------------------------------------------------------
// listOffsets
// ---------------------------------------------------------------------------

/// `listOffsets` for a produced topic returns the expected earliest / latest /
/// max-timestamp offsets. Exercises the `AdminApiDriver` +
/// `PartitionLeaderStrategy` lookup→fulfillment path end to end.
async fn list_offsets_earliest_latest_max_timestamp<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let admin = admin_for(factory, ctx).await;
    let bootstrap = ctx.protocol_bootstrap_servers().to_string();
    let backend = admin.name();

    let topic = ctx.topic("admin_list_offsets");
    create_topic(&admin, &topic, 1, 1).await;
    produce_records(ctx, &bootstrap, &topic, 0, NUM_RECORDS, 16).await;

    let tp = TopicPartition::new(topic.clone(), 0);

    // Earliest: offset 0. The broker sends no timestamp for a sentinel lookup, so
    // Java reports -1 — asserted because it is the contract, not incidental.
    let earliest = offset_ok(&admin, &tp, OffsetSpec::earliest()).await;
    assert_eq!(earliest.offset(), 0, "{backend} backend: earliest offset should be 0");
    assert_eq!(
        earliest.timestamp(),
        -1,
        "{backend} backend: an earliest lookup carries no timestamp"
    );
    assert!(
        earliest.leader_epoch().is_some(),
        "{backend} backend: the broker reports a leader epoch for a live partition"
    );

    // Latest: log end offset == number of produced records.
    let latest = offset_ok(&admin, &tp, OffsetSpec::latest()).await;
    assert_eq!(
        latest.offset(),
        NUM_RECORDS as i64,
        "{backend} backend: latest offset should equal the record count"
    );

    // MaxTimestamp: the offset of the record with the largest timestamp. Records
    // produced back-to-back can share the same millisecond timestamp, in which
    // case the broker returns the *earliest* offset carrying that max timestamp,
    // so the exact offset is timing-dependent — assert only that a valid offset
    // in range and a real timestamp are returned (this still drives the v7+
    // MAX_TIMESTAMP spec path end to end).
    let max_ts = offset_ok(&admin, &tp, OffsetSpec::max_timestamp()).await;
    assert!(
        max_ts.offset() >= 0 && max_ts.offset() < NUM_RECORDS as i64,
        "{backend} backend: max-timestamp offset should be a valid offset in [0, {NUM_RECORDS})"
    );
    assert!(
        max_ts.timestamp() >= 0,
        "{backend} backend: max-timestamp offset carries a real timestamp"
    );

    admin.close(Some(Duration::from_secs(5))).await.expect("close");
    ctx.cleanup().await;
}

/// Every one of Java's seven `OffsetSpec` variants crosses correctly.
///
/// Added, not converted. `OffsetSpec` is the only enum-shaped input in slice G3,
/// and it crosses as a *named* variant rather than as the C boundary's signed
/// sentinel precisely so that each server reaches the six `ListOffsets`
/// sentinels through its own binding's table (`admin.py`'s factories,
/// `server.cc`'s `offset_spec_columns`). A scenario that only ever asked for
/// `latest` would leave six of the seven tables unexecuted on every backend.
///
/// Two of the variants are load-bearing beyond their own path:
///
///   - `latestTiered` on a topic with no remote storage answers with offset -1
///     and **no leader epoch**, which is the only route this suite has to Java's
///     `Optional.empty()` leader epoch. A backend that decoded an absent epoch as
///     0 would pass every other assertion here.
///   - `forTimestamp(0)` returns a *real* timestamp where `earliest()` returns
///     -1, which is what proves the `forTimestamp` discriminant reached the
///     broker rather than being folded into a sentinel.
///
/// `earliestPendingUpload` does **not** exercise `ListOffsetsEntry`'s per-key
/// **error** arm — see the comment at its assertion below for why, and for the
/// state of that coverage gap.
async fn list_offsets_covers_every_offset_spec_variant<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let admin = admin_for(factory, ctx).await;
    let bootstrap = ctx.protocol_bootstrap_servers().to_string();
    let backend = admin.name();

    let topic = ctx.topic("admin_offset_specs");
    create_topic(&admin, &topic, 1, 1).await;
    produce_records(ctx, &bootstrap, &topic, 0, NUM_RECORDS, 16).await;
    let tp = TopicPartition::new(topic.clone(), 0);

    // earliestLocal is the local log start offset. With no remote storage
    // configured that is the log start offset, i.e. the same answer as earliest —
    // but a *different* wire sentinel (-4 rather than -2).
    let earliest_local = offset_ok(&admin, &tp, OffsetSpec::earliest_local()).await;
    assert_eq!(
        earliest_local.offset(),
        0,
        "{backend} backend: earliestLocal is the log start offset when no tier is configured"
    );

    // latestTiered: nothing has been uploaded, so the broker answers with the
    // unknown-offset sentinel and omits the leader epoch.
    let latest_tiered = offset_ok(&admin, &tp, OffsetSpec::latest_tiered()).await;
    assert_eq!(
        latest_tiered.offset(),
        -1,
        "{backend} backend: latestTiered on a non-tiered topic is the unknown-offset sentinel"
    );
    assert_eq!(
        latest_tiered.timestamp(),
        -1,
        "{backend} backend: latestTiered carries no timestamp"
    );
    assert_eq!(
        latest_tiered.leader_epoch(),
        None,
        "{backend} backend: latestTiered on a non-tiered topic reports Java's Optional.empty() leader epoch, \
         which must not decode as epoch 0"
    );

    // earliestPendingUpload: a real `apache/kafka:4.2.0` broker fully supports
    // `ListOffsets` v11 (verified via `kafka-broker-api-versions.sh` against a
    // live 4.2.0 container: `ListOffsets(2): 1 to 11 [usable: 11]`, and
    // `ListOffsetsRequest.json`'s `"validVersions": "1-11"` /
    // `"latestVersionUnstable": false` mark v11 as a stable, released part of
    // 4.2 — not gated behind an unstable-versions flag). Our client already
    // negotiates v11 for this spec (`ListOffsetsRequestBuilder::
    // for_consumer_options` sets `min_version = 11` for
    // `require_earliest_pending_upload_timestamp`, covered by
    // `for_consumer_require_earliest_pending_upload_forces_v11`), so the
    // request goes through as an ordinary, successful v11 call. The broker's
    // own defense-in-depth check (`ReplicaManager.
    // isListOffsetsTimestampUnsupported`, comparing the request's wire version
    // against a per-sentinel minimum of 11 for this sentinel) never fires
    // either, since 11 < 11 is false. With no remote/tiered storage configured
    // for this topic, the broker legitimately resolves "earliest
    // pending-upload offset" to "none" and returns the same unknown-offset
    // sentinel shape already asserted for `latestTiered` above: offset -1,
    // timestamp -1, no leader epoch. (An earlier version of this test asserted
    // `UnsupportedVersion` here on the theory that a 4.2 broker does not offer
    // the version this spec needs — that theory was empirically wrong for the
    // broker version this suite actually runs against, so the assertion below
    // was wrong too; fixed to match the real, verified broker behavior.)
    let pending = offset_ok(&admin, &tp, OffsetSpec::earliest_pending_upload()).await;
    assert_eq!(
        pending.offset(),
        -1,
        "{backend} backend: earliestPendingUpload on a non-tiered topic is the unknown-offset sentinel"
    );
    assert_eq!(
        pending.timestamp(),
        -1,
        "{backend} backend: earliestPendingUpload carries no timestamp"
    );
    assert_eq!(
        pending.leader_epoch(),
        None,
        "{backend} backend: earliestPendingUpload on a non-tiered topic reports Java's Optional.empty() \
         leader epoch"
    );

    // Coverage gap, left honest rather than silently dropped: nothing in this
    // suite currently drives `ListOffsetsEntry`'s per-key **error** arm (the
    // `oneof outcome`'s error variant, both the Python/C gRPC server's own
    // encoding of it in `_to_list_offsets_response` / `server.cc`'s
    // `offset_spec_columns`, and this harness's decode of
    // `proto::list_offsets_entry::Outcome::Error` in
    // `tests/common/multilanguage_admin.rs`). This spec used to be believed to
    // exercise it via `UnsupportedVersion`, but that was never actually
    // reached (see above) — so the coverage was never really happening even
    // before this fix, just silently assumed to be. No lower-level (non-real-
    // broker) test covers it either: `list_offsets_handler.rs`'s
    // `handle_unexpected_partition_error_response` /
    // `handle_response_unsupported_version` cover the *client-side driver's*
    // decode of a partition error, not the Python/C translation layer's own
    // encoding of one. Two avenues to reach a real per-partition error on this
    // single-node/no-ACL/no-tiered-storage broker were investigated and ruled
    // out: (a) requesting a partition index that does not exist on an
    // existing topic does not produce a fast in-band error —
    // `PartitionLeaderStrategy.handlePartitionError` treats partition-level
    // `UNKNOWN_TOPIC_OR_PARTITION` as retriable at the metadata-lookup stage,
    // so it retries until a slow timeout without ever reaching the
    // `ListOffsets` response body; (b) `FENCED_LEADER_EPOCH` needs a
    // caller-supplied `current_leader_epoch`, which is not exposed on the
    // public `list_offsets` / `ListOffsetsOptions` surface (mirroring Java,
    // which does not expose it on `Admin` either), so it is unreachable
    // through this API. `TOPIC_AUTHORIZATION_FAILED` needs ACLs, out of scope
    // for this plaintext single-node cluster; manufacturing tiered storage or
    // ACL config solely to hit this one assertion would be a disproportionate
    // change. This is a documented, deliberate gap, not a silently dropped
    // one — revisit if a cheap real-broker error path for `ListOffsets`
    // surfaces later.

    // forTimestamp(0): the first record at or after the epoch, i.e. offset 0 —
    // but unlike earliest() the broker reports that record's real timestamp.
    let for_epoch = offset_ok(&admin, &tp, OffsetSpec::for_timestamp(0)).await;
    assert_eq!(for_epoch.offset(), 0, "{backend} backend: forTimestamp(0) is the first record");
    assert!(
        for_epoch.timestamp() > 0,
        "{backend} backend: forTimestamp reports the matched record's timestamp ({}), which is what \
         distinguishes it from the earliest() sentinel's -1",
        for_epoch.timestamp()
    );

    // forTimestamp far in the future matches nothing: the unknown-offset
    // sentinel, again with no leader epoch.
    let for_future = offset_ok(&admin, &tp, OffsetSpec::for_timestamp(4_000_000_000_000)).await;
    assert_eq!(
        for_future.offset(),
        -1,
        "{backend} backend: forTimestamp past the log end matches no record"
    );
    assert_eq!(
        for_future.leader_epoch(),
        None,
        "{backend} backend: an unmatched forTimestamp reports no leader epoch"
    );

    admin.close(Some(Duration::from_secs(5))).await.expect("close");
    ctx.cleanup().await;
}

/// `listOffsets` carries a non-default `isolationLevel` and a per-call timeout.
///
/// Added, not converted. Every scenario in slices G1–G3 otherwise passes a bare
/// `XOptions::new()`, which leaves `isolation_level` at its 0 default and
/// `timeout_ms` absent on the wire — so the two fields were plumbed through four
/// layers and never populated. This scenario populates both.
///
/// It cannot detect a *small* error in the timeout: a 20 001 ms budget and a
/// 20 000 ms one both let a healthy call finish, so the 1 ms truncation the two
/// Python servers used to apply is invisible from here by construction. That
/// regression is pinned by a unit test on the conversion itself
/// (`bindings/python/test/unit/test_admin.py`,
/// `test_timeout_conversion_is_exact_for_whole_milliseconds`); what this scenario
/// proves is that a non-default value crosses at all.
///
/// `READ_COMMITTED`'s latest offset is the last stable offset, which equals the
/// high watermark for a topic with no in-flight transactions — so it must agree
/// with `READ_UNCOMMITTED` here, and a backend on which it does not has either
/// dropped the field or sent a different one.
async fn list_offsets_honours_isolation_level_and_timeout<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let admin = admin_for(factory, ctx).await;
    let bootstrap = ctx.protocol_bootstrap_servers().to_string();
    let backend = admin.name();

    let topic = ctx.topic("admin_offsets_options");
    create_topic(&admin, &topic, 1, 1).await;
    produce_records(ctx, &bootstrap, &topic, 0, NUM_RECORDS, 16).await;
    let tp = TopicPartition::new(topic.clone(), 0);

    // A deliberately odd millisecond count, so the value is not one a truncating
    // conversion would land on by chance.
    let options = ListOffsetsOptions::with_isolation_level(IsolationLevel::ReadCommitted).set_timeout_ms(Some(20_001));
    assert_eq!(
        options.isolation_level(),
        IsolationLevel::ReadCommitted,
        "{backend} backend: the options object should carry the isolation level it was built with"
    );

    let latest = offset_of(&admin, &tp, OffsetSpec::latest(), options)
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: read-committed latest offset: {e}"));
    assert_eq!(
        latest.offset(),
        NUM_RECORDS as i64,
        "{backend} backend: with no in-flight transactions the last stable offset is the high watermark"
    );

    admin.close(Some(Duration::from_secs(5))).await.expect("close");
    ctx.cleanup().await;
}

// ---------------------------------------------------------------------------
// electLeaders
// ---------------------------------------------------------------------------

/// `electLeaders(PREFERRED, {tp})` on a healthy topic returns a per-partition
/// result.
///
/// On a single-broker cluster the sole replica is always the preferred leader, so
/// `ReplicationControlManager.electLeader`
/// (`metadata/src/main/java/org/apache/kafka/controller/ReplicationControlManager.java:1575-1579`)
/// returns `ELECTION_NOT_NEEDED` — deterministically, which is why this asserts
/// the exact error rather than tolerating either outcome. `ELECTION_NOT_NEEDED`
/// is the only state a healthy single-node PLAINTEXT cluster can reach here; a
/// *successful* election needs a partition whose leader is not its preferred
/// replica, which needs a broker to have gone down and come back.
async fn elect_preferred_leaders<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let admin = admin_for(factory, ctx).await;
    let backend = admin.name();

    let topic = ctx.topic("admin_elect_leaders");
    create_topic(&admin, &topic, 1, 1).await;

    let tp = TopicPartition::new(topic.clone(), 0);
    let outcomes = admin
        .elect_leaders(
            ElectionType::Preferred,
            Some([tp.clone()].into_iter().collect()),
            ElectLeadersOptions::new(),
        )
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: elect leaders: {e}"));

    // The election was attempted for our partition, and the explicit-set branch
    // always returns one result per requested partition.
    let outcome = outcomes
        .get(&tp)
        .unwrap_or_else(|| panic!("{backend} backend: a result for the requested partition"));
    let error = outcome
        .as_ref()
        .expect_err(&format!("{backend} backend: the preferred replica is already the leader"));
    assert_eq!(
        error.error(),
        Errors::ElectionNotNeeded,
        "{backend} backend: on a single-broker cluster the only expected error is ELECTION_NOT_NEEDED, got {error}"
    );

    admin.close(Some(Duration::from_secs(5))).await.expect("close");
    ctx.cleanup().await;
}

/// An explicit partition set is not widened to a cluster-wide election.
///
/// Added, not converted. Java's partition set is **nullable** and the two states
/// are different requests: `ReplicationControlManager.electLeaders`
/// (`ReplicationControlManager.java:1507-1533`) branches on
/// `topicPartitions() == null`, and in that branch **omits** every partition
/// whose outcome is `ELECTION_NOT_NEEDED` ("we do not return partitions which
/// already have the desired leader"), whereas the explicit branch always emits
/// one result per requested partition.
///
/// So on a healthy cluster: an explicit `{tp}` yields exactly one entry, and a
/// cluster-wide election yields none. That pair is what catches an encoder that
/// widened an explicit selection into "every partition in the cluster" — the
/// dangerous direction, since it would run an election over every partition the
/// caller never named.
///
/// **The opposite direction is not observable on this fixture** and is not
/// claimed to be: absent and `Some(empty)` both produce an empty result here,
/// because the only partitions the null branch would report are ones needing an
/// election, and a healthy cluster has none. What establishes that the
/// distinction is *preserved* is the code path rather than this scenario — every
/// layer keeps it as an explicit discriminant (`optional TopicPartitionList` on
/// the wire, the C entry points' `all_partitions` flag, `admin.py`'s
/// `partitions is None` column, `ElectLeadersRequestBuilder`'s
/// `set_topic_partitions(None)`), never as an emptiness test.
///
/// Both election types are exercised. On a healthy partition they agree — Java
/// returns `ELECTION_NOT_NEEDED` for `PREFERRED` when the preferred replica
/// leads and for `UNCLEAN` when the partition merely has a leader
/// (`ReplicationControlManager.java:1575-1579`) — so this proves the
/// `election_type` value crosses and is accepted, **not** that the two values
/// are distinguished. Telling them apart needs a leaderless partition, which
/// needs a broker outage.
async fn elect_leaders_explicit_set_is_not_cluster_wide<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let admin = admin_for(factory, ctx).await;
    let backend = admin.name();

    let topic = ctx.topic("admin_elect_scope");
    create_topic(&admin, &topic, 2, 1).await;
    let tp = TopicPartition::new(topic.clone(), 0);

    for election_type in [ElectionType::Preferred, ElectionType::Unclean] {
        // Explicit selection: exactly one entry, for exactly our partition.
        let explicit = admin
            .elect_leaders(
                election_type,
                Some([tp.clone()].into_iter().collect()),
                ElectLeadersOptions::new(),
            )
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: elect {election_type:?} for {{tp}}: {e}"));
        assert_eq!(
            explicit.len(),
            1,
            "{backend} backend: an explicit one-partition {election_type:?} election returns exactly one result, \
             got {explicit:?}"
        );
        let error = explicit
            .get(&tp)
            .unwrap_or_else(|| panic!("{backend} backend: {tp} missing from the explicit result"))
            .as_ref()
            .expect_err(&format!("{backend} backend: {election_type:?} election is not needed"));
        assert_eq!(
            error.error(),
            Errors::ElectionNotNeeded,
            "{backend} backend: {election_type:?} on a healthy partition is ELECTION_NOT_NEEDED, got {error}"
        );

        // Cluster-wide (Java's null set): our healthy partition is omitted.
        let cluster_wide = admin
            .elect_leaders(election_type, None, ElectLeadersOptions::new())
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: cluster-wide {election_type:?} election: {e}"));
        assert!(
            !cluster_wide.contains_key(&tp),
            "{backend} backend: a cluster-wide {election_type:?} election omits partitions that already have \
             their desired leader, but {tp} was reported as {:?}",
            cluster_wide.get(&tp)
        );
        assert!(
            cluster_wide
                .values()
                .all(|outcome| !matches!(outcome.as_ref().err().map(|e| e.error()), Some(Errors::ElectionNotNeeded))),
            "{backend} backend: no entry of a cluster-wide election may be ELECTION_NOT_NEEDED, got {cluster_wide:?}"
        );
    }

    // An explicitly empty selection is a no-op, not a cluster-wide election.
    let empty = admin
        .elect_leaders(ElectionType::Preferred, Some(HashSet::new()), ElectLeadersOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: elect leaders for an empty set: {e}"));
    assert!(
        empty.is_empty(),
        "{backend} backend: an empty partition set elects nothing, got {empty:?}"
    );

    admin.close(Some(Duration::from_secs(5))).await.expect("close");
    ctx.cleanup().await;
}

// ---------------------------------------------------------------------------
// alterPartitionReassignments / listPartitionReassignments
// ---------------------------------------------------------------------------

/// `alterPartitionReassignments` moves a partition's replica set on a
/// multi-broker cluster, then `listPartitionReassignments` reflects the
/// in-progress reassignment (or an empty map once it has already completed).
async fn alter_and_list_partition_reassignments<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let admin = admin_for(factory, ctx).await;
    let backend = admin.name();

    let ids = broker_ids(&admin).await;
    let topic = ctx.topic("admin_reassignments");
    // Replication factor 1: a single replica we can move between brokers.
    create_topic(&admin, &topic, 1, 1).await;
    let tp = TopicPartition::new(topic.clone(), 0);

    // Find the current leader (its sole replica) and pick a different target.
    let current_leader = sole_leader_of(&admin, &topic).await;
    let target = *ids
        .iter()
        .find(|&&id| id != current_leader)
        .unwrap_or_else(|| panic!("{backend} backend: a broker other than {current_leader}"));

    // Initiate the reassignment to the target broker.
    let initiated = admin
        .alter_partition_reassignments(
            &HashMap::from([(
                tp.clone(),
                Some(NewPartitionReassignment::new(vec![target]).expect("non-empty replicas")),
            )]),
            AlterPartitionReassignmentsOptions::new(),
        )
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: alter partition reassignments: {e}"));
    all_of_exactly(
        &admin,
        &initiated,
        std::slice::from_ref(&tp),
        "alterPartitionReassignments initiating the move",
    );

    // List the reassignments. A single RF-1 move of an empty partition may
    // complete before we observe it, so accept either an in-progress entry for
    // our partition or an empty map; the call itself must succeed without error.
    // (`list_partition_reassignments_reports_an_ongoing_move` below is the
    // scenario that pins the in-progress shape deterministically.)
    let reassignments = admin
        .list_partition_reassignments(None, ListPartitionReassignmentsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: list partition reassignments: {e}"));
    if let Some(ongoing) = reassignments.get(&tp) {
        assert!(
            ongoing.replicas().contains(&target),
            "{backend} backend: the in-progress reassignment targets the destination broker, got {ongoing}"
        );
    }

    // Eventually the partition's replica set reflects the move. Poll describe for
    // the *replica set*, not the leader: "the move completed" means the source
    // broker left `replicas`, which is what the original single-backend test
    // asserted (`replicas == vec![target]`).
    wait_until_true_with_timeout(
        || async { replica_ids_of(&admin, &topic).await == vec![target] },
        &format!("{backend} backend: {topic}-0's replica set should eventually be exactly [{target}]"),
        15_000,
        500,
    )
    .await;

    admin.close(Some(Duration::from_secs(5))).await.expect("close");
    ctx.cleanup().await;
}

/// A `None` reassignment reaches the broker as a *cancellation*.
///
/// Added, not converted. Java's map value is an `Optional`, and an empty one
/// reverts an ongoing reassignment (`Admin.java:1142-1143`) — which is a
/// different request from a `NewPartitionReassignment` with no replicas, a value
/// Java rejects before the RPC is issued (and that
/// `NewPartitionReassignment::new` therefore cannot construct).
///
/// Cancelling a partition with nothing in flight is the deterministic proof:
/// the broker answers `NO_REASSIGNMENT_IN_PROGRESS`. An encoder that turned the
/// absent `Optional` into a present-but-empty replica list would instead produce
/// a client-side `IllegalArgument` or the broker's
/// `INVALID_REPLICA_ASSIGNMENT` — a different error, on that backend alone.
///
/// This runs on a single-broker cluster: no move is needed, only the cancelling
/// request.
async fn alter_partition_reassignments_cancel_reaches_the_broker<F: AdminBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
) {
    let admin = admin_for(factory, ctx).await;
    let backend = admin.name();

    let topic = ctx.topic("admin_cancel_reassignment");
    create_topic(&admin, &topic, 1, 1).await;
    let tp = TopicPartition::new(topic.clone(), 0);

    let cancelled = admin
        .alter_partition_reassignments(&HashMap::from([(tp.clone(), None)]), AlterPartitionReassignmentsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: cancel partition reassignment: {e}"));
    let error = cancelled
        .get(&tp)
        .unwrap_or_else(|| panic!("{backend} backend: {tp} missing from the cancellation result"))
        .as_ref()
        .expect_err(&format!(
            "{backend} backend: cancelling a partition with no reassignment in progress must fail"
        ));
    assert_eq!(
        error.error(),
        Errors::NoReassignmentInProgress,
        "{backend} backend: a cancellation with nothing in flight is NO_REASSIGNMENT_IN_PROGRESS; \
         an INVALID_REPLICA_ASSIGNMENT or IllegalArgument here would mean the absent Optional was \
         encoded as an empty replica list. Got {error}"
    );

    admin.close(Some(Duration::from_secs(5))).await.expect("close");
    ctx.cleanup().await;
}

/// `listPartitionReassignments` reports a fully populated `PartitionReassignment`
/// for a move that is still in flight, and honours its partition selection.
///
/// Added, not converted, and it closes a limitation
/// `PLAN-multilanguage-admin.md` §D3 recorded as unreachable: "a single RF-1 move
/// on one broker never has an in-flight reassignment, so
/// `PartitionReassignment{replicas, adding, removing}` is never populated". It is
/// reachable — with a three-broker cluster **and** replication throttled to
/// 1 KiB/s over ~2 MiB of data, which stretches the move to roughly half an hour
/// of wall clock and makes observing it deterministic rather than a race. This is
/// how Kafka's own `ReassignPartitionsCommand` keeps a reassignment observable
/// (its `--throttle` flag sets exactly these four configs).
///
/// Three things are only testable in this state:
///
///   - all three of `replicas` / `addingReplicas` / `removingReplicas` non-empty
///     and *different from each other*, so a backend that transposed two of them
///     fails here (with an already-completed move every one of them is empty);
///   - the partition selection's null-vs-empty distinction in **both**
///     directions, which the electLeaders scenario could only manage in one:
///     absent lists the ongoing move, `Some(empty)` lists nothing;
///   - a cancellation of a reassignment that really is in progress, i.e. the
///     success arm of the `None` value whose failure arm
///     `alter_partition_reassignments_cancel_reaches_the_broker` covers.
async fn list_partition_reassignments_reports_an_ongoing_move<F: AdminBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
) {
    let admin = admin_for(factory, ctx).await;
    let bootstrap = ctx.protocol_bootstrap_servers().to_string();
    let backend = admin.name();

    let ids = broker_ids(&admin).await;
    let topic = ctx.topic("admin_ongoing_reassignment");
    create_topic(&admin, &topic, 1, 1).await;
    let tp = TopicPartition::new(topic.clone(), 0);

    // Throttle replication before there is anything to replicate. Both halves are
    // required: the rate is a dynamic *broker* config and the replica list is a
    // *topic* config, and a rate with no throttled replicas throttles nothing.
    let topic_resource = ConfigResource::new(ConfigResourceType::Topic, topic.clone());
    set_config(&admin, &topic_resource, "leader.replication.throttled.replicas", "*").await;
    set_config(&admin, &topic_resource, "follower.replication.throttled.replicas", "*").await;
    for id in &ids {
        let broker_resource = ConfigResource::new(ConfigResourceType::Broker, id.to_string());
        set_config(&admin, &broker_resource, "leader.replication.throttled.rate", "1024").await;
        set_config(&admin, &broker_resource, "follower.replication.throttled.rate", "1024").await;
    }

    // ~2 MiB at 1 KiB/s. The move cannot finish while the scenario runs.
    produce_records(ctx, &bootstrap, &topic, 0, 200, 10_240).await;

    let source = sole_leader_of(&admin, &topic).await;
    let target = *ids
        .iter()
        .find(|&&id| id != source)
        .unwrap_or_else(|| panic!("{backend} backend: a broker other than {source}"));

    let initiated = admin
        .alter_partition_reassignments(
            &HashMap::from([(
                tp.clone(),
                Some(NewPartitionReassignment::new(vec![target]).expect("non-empty replicas")),
            )]),
            AlterPartitionReassignmentsOptions::new(),
        )
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: alter partition reassignments: {e}"));
    all_of_exactly(
        &admin,
        &initiated,
        std::slice::from_ref(&tp),
        "alterPartitionReassignments initiating the move",
    );

    // The controller applies the reassignment records asynchronously, so poll
    // rather than assuming the very next list already shows it.
    wait_until_true_with_timeout(
        || async {
            admin
                .list_partition_reassignments(None, ListPartitionReassignmentsOptions::new())
                .await
                .is_ok_and(|listed| listed.contains_key(&tp))
        },
        &format!("{backend} backend: the throttled move of {tp} should be listed as in progress"),
        30_000,
        250,
    )
    .await;

    // Absent selection (Java's Optional.empty()): the whole cluster, so our move
    // is there, fully populated.
    let all = admin
        .list_partition_reassignments(None, ListPartitionReassignmentsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: list all reassignments: {e}"));
    let ongoing = all
        .get(&tp)
        .unwrap_or_else(|| panic!("{backend} backend: {tp} missing from the cluster-wide listing {all:?}"));
    // During a move the replica set is the union of source and target, and the
    // two deltas name exactly one broker each. Asserting all three against
    // distinct expected values is what makes a transposition detectable.
    let mut replicas = ongoing.replicas().to_vec();
    replicas.sort_unstable();
    let mut expected = vec![source, target];
    expected.sort_unstable();
    assert_eq!(
        replicas, expected,
        "{backend} backend: an in-flight move's replica set is the union of source and target, got {ongoing}"
    );
    assert_eq!(
        ongoing.adding_replicas(),
        &[target],
        "{backend} backend: addingReplicas is the destination broker, got {ongoing}"
    );
    assert_eq!(
        ongoing.removing_replicas(),
        &[source],
        "{backend} backend: removingReplicas is the source broker, got {ongoing}"
    );

    // Explicit selection naming our partition: the same entry.
    let restricted = admin
        .list_partition_reassignments(
            Some([tp.clone()].into_iter().collect()),
            ListPartitionReassignmentsOptions::new(),
        )
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: list reassignments for {{tp}}: {e}"));
    assert!(
        restricted.contains_key(&tp),
        "{backend} backend: a listing restricted to {tp} reports it, got {restricted:?}"
    );

    // Explicitly empty selection: nothing — the other half of the null-vs-empty
    // distinction, and the half electLeaders cannot show. An encoder that dropped
    // the present-but-empty list would answer with the cluster-wide listing here.
    let none_selected = admin
        .list_partition_reassignments(Some(HashSet::new()), ListPartitionReassignmentsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: list reassignments for an empty set: {e}"));
    assert!(
        none_selected.is_empty(),
        "{backend} backend: an empty partition selection lists no reassignment, got {none_selected:?}"
    );

    // Cancel the in-flight move: the success arm of the absent Optional.
    let cancelled = admin
        .alter_partition_reassignments(&HashMap::from([(tp.clone(), None)]), AlterPartitionReassignmentsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: cancel the in-flight reassignment: {e}"));
    all_of_exactly(
        &admin,
        &cancelled,
        std::slice::from_ref(&tp),
        "alterPartitionReassignments cancelling a reassignment in progress",
    );

    wait_until_true_with_timeout(
        || async {
            admin
                .list_partition_reassignments(None, ListPartitionReassignmentsOptions::new())
                .await
                .is_ok_and(|listed| !listed.contains_key(&tp))
        },
        &format!("{backend} backend: {tp} should no longer be listed after the cancellation"),
        30_000,
        250,
    )
    .await;

    admin.close(Some(Duration::from_secs(5))).await.expect("close");
    ctx.cleanup().await;
}

multilanguage_admin_test!(
    test_ml_admin_list_offsets_earliest_latest_max_timestamp,
    list_offsets_earliest_latest_max_timestamp
);
multilanguage_admin_test!(
    test_ml_admin_list_offsets_covers_every_offset_spec_variant,
    list_offsets_covers_every_offset_spec_variant
);
multilanguage_admin_test!(
    test_ml_admin_list_offsets_honours_isolation_level_and_timeout,
    list_offsets_honours_isolation_level_and_timeout
);
multilanguage_admin_test!(test_ml_admin_elect_preferred_leaders, elect_preferred_leaders);
multilanguage_admin_test!(
    test_ml_admin_elect_leaders_explicit_set_is_not_cluster_wide,
    elect_leaders_explicit_set_is_not_cluster_wide
);
multilanguage_admin_test!(
    test_ml_admin_alter_partition_reassignments_cancel_reaches_the_broker,
    alter_partition_reassignments_cancel_reaches_the_broker
);
multilanguage_admin_test!(
    test_ml_admin_alter_and_list_partition_reassignments,
    alter_and_list_partition_reassignments,
    ClusterConfig::with_brokers(3)
);
multilanguage_admin_test!(
    test_ml_admin_list_partition_reassignments_reports_an_ongoing_move,
    list_partition_reassignments_reports_an_ongoing_move,
    ClusterConfig::with_brokers(3)
);
