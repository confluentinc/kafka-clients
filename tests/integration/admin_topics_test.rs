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

//! Integration tests for the admin topic CRUD RPCs against a real Kafka 4.2.0
//! broker.
//!
//! Mirrors the topic-management scenarios in Java's
//! `KafkaAdminClientIntegrationTest` (create / list / describe / delete),
//! exercising the real network engine end to end rather than the `MockClient`
//! unit-test harness.
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

use std::collections::BTreeMap;
use std::time::Duration;

use confluent_kafka::admin::{
    CreateTopicsOptions, DeleteTopicsOptions, DescribeTopicsOptions, ListTopicsOptions, NewTopic,
};
use confluent_kafka::common::Errors;

use crate::common::admin_backend::{
    AdminBackend, admin_config, admin_for, all_of, bootstrap_for, create_topic, wait_for_all_partitions_metadata,
    wait_until_listed,
};
use crate::common::backend_factory::AdminBackendFactory;
use crate::common::test_context::TestContext;
use crate::multilanguage_admin_test;

/// How long `create_topics_validate_only_does_not_create` keeps re-checking that
/// nothing was created. Long enough to cover the metadata-propagation window a
/// real creation needs (the other scenarios in this file see propagation inside a
/// second), short enough not to dominate the suite.
const VALIDATE_ONLY_NON_CREATION_WINDOW: Duration = Duration::from_secs(3);

/// The topics the scenario created, removed so the next scenario on a pooled
/// cluster starts clean. Deletion is asserted, as it was in the originals: it is
/// the `deleteTopics` coverage, not just cleanup.
async fn delete_and_close<B: AdminBackend>(admin: &B, topics: &[String]) {
    let deleted = admin
        .delete_topics(topics, DeleteTopicsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{} backend: delete topics: {e}", admin.name()));
    all_of(&deleted).unwrap_or_else(|e| panic!("{} backend: delete topics should succeed: {e}", admin.name()));
    admin
        .close(Some(Duration::from_secs(5)))
        .await
        .unwrap_or_else(|e| panic!("{} backend: close: {e}", admin.name()));
}

// ---------------------------------------------------------------------------
// Test bodies — generic over AdminBackendFactory
// ---------------------------------------------------------------------------

async fn create_then_list_and_describe_topics<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let admin = admin_for(factory, ctx).await;
    let backend = factory.name();

    let topic = ctx.topic("admin_create_list");
    let created = admin
        .create_topics(
            &[NewTopic::new_num_partitions_replication_factor(
                topic.clone(),
                Some(2),
                Some(1),
            )],
            CreateTopicsOptions::new(),
        )
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: create topics: {e}"));
    all_of(&created).unwrap_or_else(|e| panic!("{backend} backend: create topics should succeed: {e}"));

    // The topic shows up in list_topics.
    wait_until_listed(&admin, &topic, true).await;
    // As above: being listed does not imply the describe target's metadata cache
    // is populated, and the assertions below depend on the partition count.
    wait_for_all_partitions_metadata(&admin, &topic, 2).await;

    // describe_topics reports the partition count and replication factor.
    let described = admin
        .describe_topics_with_topics(std::slice::from_ref(&topic), DescribeTopicsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: describe topics: {e}"));
    let desc = described[&topic]
        .as_ref()
        .unwrap_or_else(|e| panic!("{backend} backend: describe topics should succeed: {e}"));
    assert_eq!(desc.name(), topic, "{backend} backend");
    assert_eq!(desc.partitions().len(), 2, "{backend} backend: should have 2 partitions");
    for partition in desc.partitions() {
        assert_eq!(
            partition.replicas().len(),
            1,
            "{backend} backend: replication factor should be 1"
        );
    }

    delete_and_close(&admin, &[topic]).await;
    ctx.cleanup().await;
}

async fn describe_nonexistent_topic_is_unknown<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let admin = admin_for(factory, ctx).await;
    let backend = factory.name();

    let topic = ctx.topic("admin_nonexistent");
    let described = admin
        .describe_topics_with_topics(std::slice::from_ref(&topic), DescribeTopicsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: describe topics: {e}"));
    // The *call* succeeds and the failure is per key — a nonexistent topic does
    // not fail the batch.
    let err = described[&topic]
        .as_ref()
        .expect_err(&format!("{backend} backend: describing a nonexistent topic should fail"));
    assert_eq!(
        err.error(),
        Errors::UnknownTopicOrPartition,
        "{backend} backend: expected UNKNOWN_TOPIC_OR_PARTITION, got {err:?}"
    );

    admin
        .close(Some(Duration::from_secs(5)))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: close: {e}"));
    ctx.cleanup().await;
}

async fn delete_topics_removes_them<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let admin = admin_for(factory, ctx).await;
    let backend = factory.name();

    let topic = ctx.topic("admin_delete");
    create_topic(&admin, &topic, 1, 1).await;
    wait_until_listed(&admin, &topic, true).await;

    // Delete and verify it is gone.
    let deleted = admin
        .delete_topics(std::slice::from_ref(&topic), DeleteTopicsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: delete topics: {e}"));
    all_of(&deleted).unwrap_or_else(|e| panic!("{backend} backend: delete topics should succeed: {e}"));
    wait_until_listed(&admin, &topic, false).await;

    // Describing the deleted topic now fails with UNKNOWN_TOPIC_OR_PARTITION.
    let described = admin
        .describe_topics_with_topics(std::slice::from_ref(&topic), DescribeTopicsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: describe topics: {e}"));
    let err = described[&topic]
        .as_ref()
        .expect_err(&format!("{backend} backend: describing a deleted topic should fail"));
    assert_eq!(err.error(), Errors::UnknownTopicOrPartition, "{backend} backend");

    admin
        .close(Some(Duration::from_secs(5)))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: close: {e}"));
    ctx.cleanup().await;
}

async fn create_multiple_topics_partition_round_trip<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let admin = admin_for(factory, ctx).await;
    let backend = factory.name();

    let topic_a = ctx.topic("admin_multi_a");
    let topic_b = ctx.topic("admin_multi_b");
    let created = admin
        .create_topics(
            &[
                NewTopic::new_num_partitions_replication_factor(topic_a.clone(), Some(3), Some(1)),
                NewTopic::new_num_partitions_replication_factor(topic_b.clone(), Some(1), Some(1)),
            ],
            CreateTopicsOptions::new(),
        )
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: create topics: {e}"));
    all_of(&created).unwrap_or_else(|e| panic!("{backend} backend: create topics should succeed: {e}"));
    // Both keys are reported, not just the failing or the first one.
    assert_eq!(created.len(), 2, "{backend} backend: one entry per requested topic");

    wait_until_listed(&admin, &topic_a, true).await;
    wait_until_listed(&admin, &topic_b, true).await;
    // Appearing in `list_topics` does not guarantee that the broker answering
    // the `describe_topics` below has the topic in its metadata cache yet, so
    // wait on the partition counts the assertions rely on.
    wait_for_all_partitions_metadata(&admin, &topic_a, 3).await;
    wait_for_all_partitions_metadata(&admin, &topic_b, 1).await;

    let described = admin
        .describe_topics_with_topics(&[topic_a.clone(), topic_b.clone()], DescribeTopicsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: describe topics: {e}"));
    let a = described[&topic_a]
        .as_ref()
        .unwrap_or_else(|e| panic!("{backend} backend: describe {topic_a}: {e}"));
    let b = described[&topic_b]
        .as_ref()
        .unwrap_or_else(|e| panic!("{backend} backend: describe {topic_b}: {e}"));
    assert_eq!(a.partitions().len(), 3, "{backend} backend");
    assert_eq!(b.partitions().len(), 1, "{backend} backend");

    delete_and_close(&admin, &[topic_a, topic_b]).await;
    ctx.cleanup().await;
}

/// `describeTopics` and `deleteTopics` keyed by **topic id** rather than by name
/// (`TopicCollection.ofTopicIds`).
///
/// Not a conversion of a committed test — none covers the id-keyed collection —
/// but slice G1 has to prove `ResultKey`'s `topic_id` variant end to end before
/// the later slices depend on the envelope, and Java covers the same ground in
/// `PlaintextAdminIntegrationTest.testDescribeTopicsWithIds`
/// (`core/src/test/scala/integration/kafka/api/PlaintextAdminIntegrationTest.scala:794-809`).
/// The id itself comes from `list_topics`, which is the only public route to it
/// before a describe.
///
/// An **unknown** id is requested alongside the known one, as Java does. That is
/// not covered by `describe_nonexistent_topic_is_unknown`: production routes the
/// two collection kinds down different broker requests
/// (`src/admin/kafka_admin_client.rs`, mirroring `KafkaAdminClient.java`'s
/// `handleDescribeTopicsByIds` via Metadata versus
/// `handleDescribeTopicsByNamesWithDescribeTopicPartitionsApi`), and they answer
/// with different errors — `UNKNOWN_TOPIC_ID` (100) rather than
/// `UNKNOWN_TOPIC_OR_PARTITION` (3). It is also the only exercise of the
/// *id-keyed* per-key `oneof outcome`'s error arm, which every later slice's
/// id-keyed result depends on.
async fn describe_and_delete_topics_by_ids<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let admin = admin_for(factory, ctx).await;
    let backend = factory.name();

    let topic = ctx.topic("admin_by_ids");
    create_topic(&admin, &topic, 2, 1).await;
    wait_until_listed(&admin, &topic, true).await;

    let listings = admin
        .list_topics(ListTopicsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: list topics: {e}"));
    let topic_id = listings
        .get(&topic)
        .unwrap_or_else(|| panic!("{backend} backend: {topic} should be listed"))
        .topic_id();
    assert_ne!(
        topic_id,
        confluent_kafka::common::Uuid::zero(),
        "{backend} backend: list_topics must report a real topic id"
    );

    // Describing by id reports the same topic, and the description carries the
    // id it was looked up by. A random id in the same batch takes the per-key
    // error arm, with the id-specific error rather than the by-name one.
    let unknown_id = confluent_kafka::common::Uuid::random_uuid();
    let described = admin
        .describe_topics_by_ids(&[topic_id, unknown_id], DescribeTopicsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: describe topics by ids: {e}"));
    let desc = described[&topic_id]
        .as_ref()
        .unwrap_or_else(|e| panic!("{backend} backend: describe by id should succeed: {e}"));
    assert_eq!(desc.name(), topic, "{backend} backend: id-keyed describe must name the topic");
    assert_eq!(desc.topic_id(), topic_id, "{backend} backend");
    assert_eq!(desc.partitions().len(), 2, "{backend} backend");

    let unknown_err = described
        .get(&unknown_id)
        .unwrap_or_else(|| {
            panic!(
                "{backend} backend: describeTopics(ofTopicIds) must report every requested id, {unknown_id} \
                 missing from {:?}",
                described.keys().collect::<Vec<_>>()
            )
        })
        .as_ref()
        .expect_err(&format!("{backend} backend: a random topic id cannot describe"));
    assert_eq!(
        unknown_err.error(),
        Errors::UnknownTopicId,
        "{backend} backend: an unknown *id* is UNKNOWN_TOPIC_ID, not the by-name \
         UNKNOWN_TOPIC_OR_PARTITION. Got {unknown_err}"
    );

    // Deleting by id removes it, and that result is id-keyed too.
    let deleted = admin
        .delete_topics_by_ids(&[topic_id], DeleteTopicsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: delete topics by ids: {e}"));
    assert!(
        deleted.contains_key(&topic_id),
        "{backend} backend: deleteTopics(ofTopicIds) must key its result by topic id, got {:?}",
        deleted.keys().collect::<Vec<_>>()
    );
    all_of(&deleted).unwrap_or_else(|e| panic!("{backend} backend: delete by id should succeed: {e}"));
    wait_until_listed(&admin, &topic, false).await;

    admin
        .close(Some(Duration::from_secs(5)))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: close: {e}"));
    ctx.cleanup().await;
}

/// `createTopics` reports the created topic's metadata and effective configs,
/// and honours an explicit replica assignment.
///
/// Not a conversion either: no committed test reads `CreateTopicsResult`'s
/// metadata views (`topicId` / `numPartitions` / `replicationFactor` /
/// `config`), which are the whole of the value type G1 has to prove crosses
/// faithfully. Java covers the config echo in
/// `PlaintextAdminIntegrationTest.testCreateTopicsReturnsConfigs`.
///
/// Only the five `ConfigEntry` fields every binding exposes for this result are
/// asserted — see `comparable_config` in `tests/common/admin_backend.rs`.
async fn create_topics_reports_metadata_and_configs<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let admin = admin_for(factory, ctx).await;
    let backend = factory.name();

    // A non-default topic config, so the echoed entry cannot be confused with a
    // broker default.
    let retention = "604800001";
    let topic = ctx.topic("admin_create_metadata");
    let created = admin
        .create_topics(
            &[
                NewTopic::new_num_partitions_replication_factor(topic.clone(), Some(2), Some(1))
                    .set_configs(BTreeMap::from([("retention.ms".to_string(), retention.to_string())])),
            ],
            CreateTopicsOptions::new(),
        )
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: create topics: {e}"));
    let metadata = created[&topic]
        .as_ref()
        .unwrap_or_else(|e| panic!("{backend} backend: create topics should succeed: {e}"));

    // A single PLAINTEXT node with no authorizer always returns the metadata
    // (ReplicationControlManager only sets topicConfigErrorCode when the caller
    // lacks DESCRIBE_CONFIGS, and User:ANONYMOUS is a super user), so the
    // accessors must succeed on every backend.
    assert_eq!(
        metadata
            .num_partitions()
            .unwrap_or_else(|e| panic!("{backend} backend: numPartitions: {e}")),
        2,
        "{backend} backend"
    );
    assert_eq!(
        metadata
            .replication_factor()
            .unwrap_or_else(|e| panic!("{backend} backend: replicationFactor: {e}")),
        1,
        "{backend} backend"
    );
    let topic_id = metadata
        .topic_id()
        .unwrap_or_else(|e| panic!("{backend} backend: topicId: {e}"));
    assert_ne!(
        topic_id,
        confluent_kafka::common::Uuid::zero(),
        "{backend} backend: createTopics must report a real topic id"
    );
    let config = metadata.config().unwrap_or_else(|e| panic!("{backend} backend: config: {e}"));
    let entry = config
        .get("retention.ms")
        .unwrap_or_else(|| panic!("{backend} backend: createTopics should echo retention.ms"));
    assert_eq!(entry.value(), Some(retention), "{backend} backend");
    assert!(
        !entry.is_default(),
        "{backend} backend: an explicitly set config is not a default"
    );

    // The same id the create reported is the one the topic is described by.
    wait_for_all_partitions_metadata(&admin, &topic, 2).await;
    let described = admin
        .describe_topics_with_topics(std::slice::from_ref(&topic), DescribeTopicsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: describe topics: {e}"));
    let desc = described[&topic]
        .as_ref()
        .unwrap_or_else(|e| panic!("{backend} backend: describe: {e}"));
    assert_eq!(
        desc.topic_id(),
        topic_id,
        "{backend} backend: createTopics and describeTopics must agree on the topic id"
    );

    delete_and_close(&admin, &[topic]).await;
    ctx.cleanup().await;
}

/// `createTopics` with an explicit replica assignment, i.e. Java's
/// `NewTopic(String, Map<Integer, List<Integer>>)`.
///
/// That constructor sends a *different* broker request from
/// `NewTopic(name, numPartitions, replicationFactor)` — the assignments travel
/// instead of the counts — and all three bindings have a distinct code path for
/// it (`kafka_admin_NewTopic_set_replicas_assignment`, admin.py's
/// `replicas_assignments`). Without this scenario that path would be wired
/// through the harness and never executed. The broker id comes from describing a
/// probe topic, since a single-node cluster's id is not fixed by config.
async fn create_topics_with_replica_assignment<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let admin = admin_for(factory, ctx).await;
    let backend = factory.name();

    // Learn this cluster's broker id from a topic the broker assigned itself.
    let probe = ctx.topic("admin_assign_probe");
    create_topic(&admin, &probe, 1, 1).await;
    let described = admin
        .describe_topics_with_topics(std::slice::from_ref(&probe), DescribeTopicsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: describe probe: {e}"));
    let broker_id = described[&probe]
        .as_ref()
        .unwrap_or_else(|e| panic!("{backend} backend: describe probe: {e}"))
        .partitions()[0]
        .replicas()[0]
        .id();

    // Two partitions, both on the only broker, placed explicitly.
    let topic = ctx.topic("admin_assign");
    let assignments = BTreeMap::from([(0, vec![broker_id]), (1, vec![broker_id])]);
    let created = admin
        .create_topics(
            &[NewTopic::new_replicas_assignments(topic.clone(), assignments)],
            CreateTopicsOptions::new(),
        )
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: create topics: {e}"));
    all_of(&created).unwrap_or_else(|e| panic!("{backend} backend: assigned create should succeed: {e}"));

    wait_for_all_partitions_metadata(&admin, &topic, 2).await;
    let described = admin
        .describe_topics_with_topics(std::slice::from_ref(&topic), DescribeTopicsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: describe topics: {e}"));
    let desc = described[&topic]
        .as_ref()
        .unwrap_or_else(|e| panic!("{backend} backend: describe: {e}"));
    assert_eq!(desc.partitions().len(), 2, "{backend} backend");
    for partition in desc.partitions() {
        let replicas: Vec<i32> = partition.replicas().iter().map(|n| n.id()).collect();
        assert_eq!(
            replicas,
            vec![broker_id],
            "{backend} backend: partition {} must sit on the requested broker",
            partition.partition()
        );
    }

    delete_and_close(&admin, &[probe, topic]).await;
    ctx.cleanup().await;
}

/// `createTopics(validateOnly = true)` validates without creating.
///
/// Not a conversion: the committed tests never set the flag, and it is the one
/// `CreateTopicsOptions` field whose effect is observable without a second
/// broker.
///
/// **Proved by non-change over a window, not by one immediate probe.** This file
/// establishes twice that an absent describe is satisfiable even for a topic that
/// *was* created — being listed does not imply the broker answering the describe
/// has it cached yet — so a single describe right after the call would also pass
/// if a backend had silently dropped `validate_only`. Java proves the same
/// property the same way for the sibling RPC: `testCreatePartitions`
/// (`core/src/test/scala/integration/kafka/api/PlaintextAdminIntegrationTest.scala:1188-1191`)
/// sends `increaseTo(3)` with `validateOnly` and then waits for the partition
/// count to still be 1. Note that Java has **no** `createTopics`-with-validateOnly
/// non-creation test; the closest is the controller unit test
/// `ReplicationControlManagerTest.testCreateTopicsWithValidateOnlyFlag`
/// (`metadata/src/test/java/org/apache/kafka/controller/ReplicationControlManagerTest.java:859`).
/// An earlier revision of this comment cited
/// `PlaintextAdminIntegrationTest.testCreateTopicsWithValidateOnly`, which does
/// not exist.
///
/// A bounded poll rather than one sleep: the assertion is that the topic never
/// appears, so re-checking through the window also catches a topic that appears
/// and is then reaped, and it fails on the first violation instead of at the end.
async fn create_topics_validate_only_does_not_create<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let admin = admin_for(factory, ctx).await;
    let backend = factory.name();

    let topic = ctx.topic("admin_validate_only");
    let created = admin
        .create_topics(
            &[NewTopic::new_num_partitions_replication_factor(
                topic.clone(),
                Some(1),
                Some(1),
            )],
            CreateTopicsOptions::new().set_validate_only(true),
        )
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: create topics: {e}"));
    all_of(&created).unwrap_or_else(|e| panic!("{backend} backend: validate_only create should still succeed: {e}"));

    // Nothing was created, so the topic never becomes listable or describable —
    // asserted repeatedly across the metadata-propagation window a real creation
    // would need.
    let deadline = tokio::time::Instant::now() + VALIDATE_ONLY_NON_CREATION_WINDOW;
    while tokio::time::Instant::now() < deadline {
        let listings = admin
            .list_topics(ListTopicsOptions::new())
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: list topics: {e}"));
        assert!(
            !listings.contains_key(&topic),
            "{backend} backend: validate_only must not create {topic}, but it was listed"
        );

        let described = admin
            .describe_topics_with_topics(std::slice::from_ref(&topic), DescribeTopicsOptions::new())
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: describe topics: {e}"));
        let err = described[&topic]
            .as_ref()
            .expect_err(&format!("{backend} backend: validate_only must not create the topic"));
        assert_eq!(err.error(), Errors::UnknownTopicOrPartition, "{backend} backend");

        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    admin
        .close(Some(Duration::from_secs(5)))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: close: {e}"));
    ctx.cleanup().await;
}

/// The whole call fails rather than a key, when the admin client cannot reach a
/// broker at all.
///
/// This is the scenario that proves the harness actually talks to Kafka: G0's
/// create/close vertical passes against an unreachable bootstrap, because
/// `new_admin_client` does not connect eagerly and `close` succeeds regardless.
/// A `createTopics` cannot.
async fn create_topics_against_unreachable_broker_fails<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    // Port 1 is reserved and never listening; a short api timeout keeps the
    // scenario fast, since the failure is a timeout rather than a refusal (the
    // client retries metadata until the deadline, as Java does).
    let mut config = admin_config(&bootstrap_for(factory, ctx));
    config.insert("bootstrap.servers".to_string(), "127.0.0.1:1".to_string());
    config.insert("default.api.timeout.ms".to_string(), "3000".to_string());
    config.insert("request.timeout.ms".to_string(), "1000".to_string());
    let admin = factory
        .create(config)
        .await
        .unwrap_or_else(|e| panic!("{} backend: create admin client: {e}", factory.name()));
    let backend = factory.name();

    let topic = ctx.topic("admin_unreachable");
    let created = admin
        .create_topics(
            &[NewTopic::new_num_partitions_replication_factor(
                topic.clone(),
                Some(1),
                Some(1),
            )],
            CreateTopicsOptions::new(),
        )
        .await;
    // Either shape is a legitimate failure: the batch may fail as a whole or per
    // key, depending on where the deadline hits. What must not happen is
    // success.
    match created {
        Err(_) => {},
        Ok(outcomes) => {
            let err = all_of(&outcomes)
                .expect_err(&format!("{backend} backend: createTopics must not succeed with no broker"));
            assert!(
                err.is_retriable_error() || matches!(err.error(), Errors::RequestTimedOut),
                "{backend} backend: expected a timeout, got {err:?}"
            );
        },
    }

    admin
        .close(Some(Duration::from_secs(5)))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: close: {e}"));
    ctx.cleanup().await;
}

multilanguage_admin_test!(test_create_then_list_and_describe_topics, create_then_list_and_describe_topics);
multilanguage_admin_test!(
    test_describe_nonexistent_topic_is_unknown,
    describe_nonexistent_topic_is_unknown
);
multilanguage_admin_test!(test_delete_topics_removes_them, delete_topics_removes_them);
multilanguage_admin_test!(
    test_create_multiple_topics_partition_round_trip,
    create_multiple_topics_partition_round_trip
);
multilanguage_admin_test!(test_describe_and_delete_topics_by_ids, describe_and_delete_topics_by_ids);
multilanguage_admin_test!(
    test_create_topics_reports_metadata_and_configs,
    create_topics_reports_metadata_and_configs
);
multilanguage_admin_test!(
    test_create_topics_with_replica_assignment,
    create_topics_with_replica_assignment
);
multilanguage_admin_test!(
    test_create_topics_validate_only_does_not_create,
    create_topics_validate_only_does_not_create
);
multilanguage_admin_test!(
    test_create_topics_against_unreachable_broker_fails,
    create_topics_against_unreachable_broker_fails
);
