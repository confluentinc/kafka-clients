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

//! Integration tests for a producer sending while its topic is deleted,
//! recreated or reassigned.
//!
//! Translated from
//! `org.apache.kafka.clients.producer.ProducerSendWhileDeletionTest`
//! (`clients-integration-tests`, AK 4.3.1):
//! - `testSendWithTopicDeletionMidWay`
//! - `testSendWithRecreatedTopic`
//! - `testSendWhileTopicGetRecreated`
//! - `testSendWithTopicReassignmentIsMidWay`
//!
//! All four are native-only: they drive admin topic deletion / recreation /
//! reassignment around the producer (and `testSendWhileTopicGetRecreated` runs
//! a concurrent send loop counting callbacks), which is harness-side control
//! the multilanguage gRPC backends add nothing to — their `Send` RPC blocks
//! until delivery, so the concurrent send-with-callback loop cannot be
//! expressed through them.
//!
//! Deviations shared by all tests:
//! - Broker ids are 1-based in this harness (`kafka_cluster.rs`), so Java's
//!   broker `0` is `1` here and Java's broker `1` is `2`.
//! - Java's `verifyTopicDeletion` inspects broker internals (replica manager,
//!   log manager, cleaner checkpoints, log dirs on disk), which a client-side
//!   test cannot reach. [`verify_topic_deletion`] replaces them with admin
//!   polling: the topic has left the cluster metadata (`listTopics`) and no
//!   broker reports a replica of its partitions in `describeLogDirs` (the
//!   observable form of "logs from all replicas are deleted").
//! - Per-test topic names (`ctx.topic`) instead of Java's fixed `"topic"`,
//!   because clusters are pooled across tests.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use confluent_kafka::admin::Admin;
use confluent_kafka::admin::AdminClientConfig;
use confluent_kafka::admin::KafkaAdminClient;
use confluent_kafka::admin::NewPartitionReassignment;
use confluent_kafka::admin::NewTopic;
use confluent_kafka::admin::TopicDescription;
use confluent_kafka::common::TopicCollection;
use confluent_kafka::common::TopicPartition;
use confluent_kafka::common::Uuid;
use confluent_kafka::common::serialization::ByteArraySerializer;
use confluent_kafka::common::serialization::StringSerializer;
use confluent_kafka::producer::KafkaProducer;
use confluent_kafka::producer::Producer;
use confluent_kafka::producer::ProducerConfig;
use confluent_kafka::producer::ProducerRecord;

use crate::common::cluster_config::ClusterConfig;
use crate::common::test_context::TestContext;
use crate::common::test_utils;

/// `ProducerSendWhileDeletionTest.BROKER_COUNT`.
const BROKER_COUNT: u16 = 2;
/// `ProducerSendWhileDeletionTest.DEFAULT_LINGER_MS`.
const DEFAULT_LINGER_MS: i64 = 5;
/// `ProducerSendWhileDeletionTest.numRecords`.
const NUM_RECORDS: usize = 10;
/// `org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS`, the bound of every
/// `TestUtils.waitForCondition` in the Java test.
const DEFAULT_MAX_WAIT: Duration = Duration::from_millis(15_000);

/// The broker ids of this cluster (1-based, see the module docs).
const BROKER_IDS: [i32; 2] = [1, 2];

type TestProducer = KafkaProducer<String, Vec<u8>>;

/// `@ClusterTestDefaults` of `ProducerSendWhileDeletionTest.java:66-76`:
/// two brokers with `num.partitions=2`, `default.replication.factor=2`,
/// `auto.leader.rebalance.enable=false`, `log.segment.delete.delay.ms=1000`
/// and `log.initial.task.delay.ms=100`. Topic auto-creation stays at the broker
/// default (enabled), which `testSendWithTopicDeletionMidWay` relies on.
fn deletion_cluster_config() -> ClusterConfig {
    let mut cfg = ClusterConfig::with_brokers(BROKER_COUNT);
    for (key, value) in [
        ("KAFKA_NUM_PARTITIONS", "2"),
        ("KAFKA_DEFAULT_REPLICATION_FACTOR", "2"),
        ("KAFKA_AUTO_LEADER_REBALANCE_ENABLE", "false"),
        ("KAFKA_LOG_SEGMENT_DELETE_DELAY_MS", "1000"),
        ("KAFKA_LOG_INITIAL_TASK_DELAY_MS", "100"),
    ] {
        cfg.server_properties.insert(key.to_string(), value.to_string());
    }
    cfg
}

/// `cluster.admin()`.
fn create_admin(ctx: &TestContext) -> KafkaAdminClient {
    let props = HashMap::from([
        ("bootstrap.servers".to_string(), ctx.bootstrap_servers().to_string()),
        ("client.id".to_string(), "producer-send-while-deletion-admin".to_string()),
    ]);
    KafkaAdminClient::new(AdminClientConfig::new(&props).expect("valid admin config")).expect("admin client")
}

/// `ProducerSendWhileDeletionTest.createProducer` (`:258-265`) through
/// `ClusterInstance.producer`: string key serializer, byte-array value
/// serializer, everything else at the producer defaults.
fn create_producer(bootstrap_servers: &str) -> TestProducer {
    let props = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap_servers.to_string()),
        ("max.block.ms".to_string(), "5000".to_string()),
        ("request.timeout.ms".to_string(), "10000".to_string()),
        ("delivery.timeout.ms".to_string(), (10_000 + DEFAULT_LINGER_MS).to_string()),
    ]);
    KafkaProducer::new(
        ProducerConfig::new(&props).expect("valid producer config"),
        Box::new(StringSerializer::new()),
        Box::new(ByteArraySerializer),
    )
    .expect("producer")
}

/// `new ProducerRecord<>(topic, null, value)`.
fn record(topic: &str, value: &str) -> ProducerRecord<String, Vec<u8>> {
    ProducerRecord::with_key(topic.to_string(), None, Some(value.as_bytes().to_vec()))
}

/// `producer.send(record).get()`.
async fn send_and_get(producer: &TestProducer, topic: &str, value: &str) -> confluent_kafka::producer::RecordMetadata {
    // UFCS: `KafkaProducer` has an inherent `send` shadowing the trait method.
    <TestProducer as Producer<String, Vec<u8>>>::send(producer, record(topic, value))
        .await
        .unwrap_or_else(|e| panic!("send of {value} to {topic} failed: {e:?}"))
        .get()
        .await
        .unwrap_or_else(|e| panic!("delivery of {value} to {topic} failed: {e:?}"))
}

/// `admin.createTopics(List.of(new NewTopic(topic, assignment)))`.
async fn create_topic_with_assignment(admin: &dyn Admin, topic: &str, assignment: BTreeMap<i32, Vec<i32>>) {
    let num_partitions = assignment.len() as i32;
    admin
        .create_topics(&[NewTopic::with_replicas_assignments(topic.to_string(), assignment)])
        .all()
        .get()
        .await
        .unwrap_or_else(|e| panic!("create topic {topic}: {e:?}"));
    // Java's `cluster.admin().createTopics` is not awaited in
    // `testSendWithTopicDeletionMidWay`; awaiting it here, plus the leader
    // wait, removes the metadata-propagation race on this two-broker cluster
    // (see `test_utils::wait_for_partition_leaders`) without changing what the
    // test asserts.
    test_utils::wait_for_partition_leaders(admin, topic, 0..num_partitions).await;
}

/// `cluster.createTopic(topic, partitions, replicas)`, which waits until every
/// broker knows the topic; the leader wait is the client-observable half.
async fn cluster_create_topic(admin: &dyn Admin, topic: &str, num_partitions: i32, replication_factor: i16) {
    test_utils::create_topic(admin, topic, num_partitions, replication_factor).await;
    test_utils::wait_for_partition_leaders(admin, topic, 0..num_partitions).await;
}

/// `admin.alterPartitionReassignments(...).all().get()` moving every listed
/// partition to `target_replicas`.
async fn reassign(admin: &dyn Admin, topic: &str, partitions: &[i32], target_replicas: &[i32]) {
    let reassignments: HashMap<TopicPartition, Option<NewPartitionReassignment>> = partitions
        .iter()
        .map(|&p| {
            (
                TopicPartition::new(topic.to_string(), p),
                Some(NewPartitionReassignment::new(target_replicas.to_vec()).expect("non-empty replicas")),
            )
        })
        .collect();
    admin
        .alter_partition_reassignments(&reassignments)
        .all()
        .get()
        .await
        .unwrap_or_else(|e| panic!("alter partition reassignments of {topic}: {e:?}"));
}

/// `admin.deleteTopics(List.of(topic)).all().get()`.
async fn delete_topic(admin: &dyn Admin, topic: &str) {
    admin
        .delete_topics(TopicCollection::of_topic_names(vec![topic.to_string()]))
        .all()
        .get()
        .await
        .unwrap_or_else(|e| panic!("delete topic {topic}: {e:?}"));
}

/// `ProducerSendWhileDeletionTest.topicMetadata` (`:326-334`).
async fn topic_metadata(admin: &dyn Admin, topic: &str) -> Result<TopicDescription, String> {
    let mut all = admin
        .describe_topics_with_topic_names(&[topic.to_string()])
        .all_topic_names()
        .expect("described by name")
        .get()
        .await
        .map_err(|e| format!("describe {topic}: {e:?}"))?;
    all.remove(topic)
        .ok_or_else(|| format!("describe {topic}: topic missing from the result"))
}

/// `ProducerSendWhileDeletionTest.verifyTopicDeletion` (`:267-306`) for the
/// partitions `0` and `1`, replaced by admin polling (see the module docs).
async fn verify_topic_deletion(admin: &dyn Admin, topic: &str) {
    let topic_partitions: Vec<TopicPartition> = (0..2).map(|p| TopicPartition::new(topic.to_string(), p)).collect();

    // Replaces "Replica manager's should have deleted all of this topic's
    // partitions": the controller has removed the topic from the metadata.
    test_utils::retry_on_error_with_timeout(DEFAULT_MAX_WAIT, || async {
        let names = admin
            .list_topics()
            .names()
            .get()
            .await
            .map_err(|e| format!("list topics: {e:?}"))?;
        if names.contains(topic) {
            Err(format!("topic {topic} is still listed after delete topic is complete"))
        } else {
            Ok(())
        }
    })
    .await;

    // Replaces "Replica logs not deleted after delete topic is complete" and
    // the log-dir checks: no broker reports a replica of the topic's
    // partitions in any of its log directories.
    test_utils::retry_on_error_with_timeout(DEFAULT_MAX_WAIT, || async {
        let described = admin
            .describe_log_dirs(&BROKER_IDS)
            .all_descriptions()
            .get()
            .await
            .map_err(|e| format!("describe log dirs: {e:?}"))?;
        for (broker, dirs) in &described {
            for (dir, description) in dirs {
                if let Some(tp) = topic_partitions
                    .iter()
                    .find(|tp| description.replica_infos().contains_key(*tp))
                {
                    return Err(format!(
                        "Replica logs not deleted after delete topic is complete: broker {broker} still has {tp:?} in {dir}"
                    ));
                }
            }
        }
        Ok(())
    })
    .await;
}

/// `ProducerSendWhileDeletionTest.assertLeader` (`:336-350`): wait until the
/// leader of `partition` is `expected_leader`, treating an unknown topic as
/// "not yet".
async fn assert_leader(admin: &dyn Admin, topic: &str, partition: i32, expected_leader: i32) {
    test_utils::retry_on_error_with_timeout(DEFAULT_MAX_WAIT, || async {
        let description = topic_metadata(admin, topic).await?;
        let current = description
            .partitions()
            .iter()
            .find(|p| p.partition() == partition)
            .and_then(|p| p.leader())
            .map(|n| n.id());
        if current == Some(expected_leader) {
            Ok(())
        } else {
            Err(format!(
                "Waiting for leader to become {expected_leader}: leader of {topic}-{partition} is {current:?}"
            ))
        }
    })
    .await;
}

/// Translated from `ProducerSendWhileDeletionTest.testSendWithTopicDeletionMidWay`
/// (`ProducerSendWhileDeletionTest.java:95-131`): the producer recovers when
/// its topic is deleted mid-way through producing and then auto-created by the
/// producer's own metadata request.
#[tokio::test(flavor = "multi_thread")]
async fn test_send_with_topic_deletion_mid_way() {
    let mut ctx = TestContext::new(deletion_cluster_config()).await;
    let topic = ctx.topic("topic");
    let admin = create_admin(&ctx);
    let producer = create_producer(ctx.bootstrap_servers());

    // Create topic with leader as 0 (here 1) for the 2 partitions.
    create_topic_with_assignment(&admin, &topic, BTreeMap::from([(0, vec![1, 2]), (1, vec![1, 2])])).await;

    // Change leader to 1 (here 2) for both the partitions to increase leader
    // epoch from 0 -> 1.
    reassign(&admin, &topic, &[0, 1], &[2, 1]).await;

    for i in 1..=NUM_RECORDS {
        let resp = send_and_get(&producer, &topic, &format!("value{i}")).await;
        assert_eq!(topic, resp.topic());
    }

    // Start topic deletion
    delete_topic(&admin, &topic).await;
    // Verify that the topic is deleted when no metadata request comes in
    verify_topic_deletion(&admin, &topic).await;

    // Producer should be able to send messages even after topic gets deleted
    // and auto-created
    let final_resp = send_and_get(&producer, &topic, "value").await;
    assert_eq!(topic, final_resp.topic());

    Producer::close(&producer).await.expect("close producer");
    admin.close().await;
}

/// Translated from `ProducerSendWhileDeletionTest.testSendWithRecreatedTopic`
/// (`ProducerSendWhileDeletionTest.java:139-166`): after the topic is deleted
/// and recreated with a new topic id, the producer produces to the new topic,
/// starting at offset 0.
#[tokio::test(flavor = "multi_thread")]
async fn test_send_with_recreated_topic() {
    let mut ctx = TestContext::new(deletion_cluster_config()).await;
    let topic = ctx.topic("topic");
    let admin = create_admin(&ctx);
    let producer = create_producer(ctx.bootstrap_servers());

    cluster_create_topic(&admin, &topic, 1, 1).await;
    let topic_id = topic_metadata(&admin, &topic).await.expect("topic metadata").topic_id();

    for i in 1..=NUM_RECORDS {
        let resp = send_and_get(&producer, &topic, &format!("value{i}")).await;
        assert_eq!(topic, resp.topic());
    }

    // Start topic deletion
    delete_topic(&admin, &topic).await;

    // Verify that the topic is deleted when no metadata request comes in
    verify_topic_deletion(&admin, &topic).await;
    cluster_create_topic(&admin, &topic, 1, 1).await;
    assert_ne!(
        topic_id,
        topic_metadata(&admin, &topic).await.expect("topic metadata").topic_id()
    );

    // Producer should be able to send messages even after topic gets recreated
    let record_metadata = send_and_get(&producer, &topic, "value").await;
    assert_eq!(topic, record_metadata.topic());
    assert_eq!(0, record_metadata.offset());

    Producer::close(&producer).await.expect("close producer");
    admin.close().await;
}

/// Translated from `ProducerSendWhileDeletionTest.testSendWhileTopicGetRecreated`
/// (`ProducerSendWhileDeletionTest.java:168-202`): while one task deletes and
/// recreates the topic five times, another sends ten records with callbacks
/// and flushes; every record's callback fires exactly once (success or error).
///
/// Java runs both halves with `CompletableFuture.*Async` and joins them; here
/// each is a spawned task that is awaited. The Java recreation loop has no
/// bound besides the JUnit timeout; the joins here are bounded by
/// [`RECREATE_TEST_TIMEOUT`] so a stuck loop fails instead of hanging the suite.
#[tokio::test(flavor = "multi_thread")]
async fn test_send_while_topic_get_recreated() {
    const MAX_NUM_TOPIC_RECREATION_ATTEMPTS: usize = 5;
    const RECREATE_TEST_TIMEOUT: Duration = Duration::from_secs(180);

    let mut ctx = TestContext::new(deletion_cluster_config()).await;
    let topic = ctx.topic("topic");
    let bootstrap = ctx.bootstrap_servers().to_string();

    let recreate_topic_future = {
        let topic = topic.clone();
        let admin_props = HashMap::from([
            ("bootstrap.servers".to_string(), bootstrap.clone()),
            ("client.id".to_string(), "producer-send-while-deletion-admin".to_string()),
        ]);
        tokio::spawn(async move {
            let mut topic_ids: HashSet<Uuid> = HashSet::new();
            while topic_ids.len() < MAX_NUM_TOPIC_RECREATION_ATTEMPTS {
                // Java opens a fresh `cluster.admin()` per attempt and ignores
                // any failure of the attempt.
                let admin = KafkaAdminClient::new(AdminClientConfig::new(&admin_props).expect("valid admin config"))
                    .expect("admin client");
                let attempt = async {
                    let names = admin.list_topics().names().get().await?;
                    if names.contains(&topic) {
                        admin
                            .delete_topics(TopicCollection::of_topic_names(vec![topic.clone()]))
                            .all()
                            .get()
                            .await?;
                    }
                    admin
                        .create_topics(&[NewTopic::with_num_partitions_replication_factor(
                            topic.clone(),
                            Some(2),
                            Some(1),
                        )])
                        .topic_id(&topic)
                        .get()
                        .await
                };
                if let Ok(topic_id) = attempt.await {
                    topic_ids.insert(topic_id);
                }
                admin.close().await;
            }
            topic_ids
        })
    };

    let num_acks = Arc::new(AtomicUsize::new(0));
    let producer_future = {
        let topic = topic.clone();
        let num_acks = Arc::clone(&num_acks);
        tokio::spawn(async move {
            let producer = create_producer(&bootstrap);
            for i in 1..=NUM_RECORDS {
                let num_acks = Arc::clone(&num_acks);
                // Java ignores the returned future; a send that fails before
                // reaching the accumulator still fires the callback
                // (`KafkaProducer.doSend`'s ApiException branch), so it is
                // counted there, not here.
                let _ = <TestProducer as Producer<String, Vec<u8>>>::send_with_callback(
                    &producer,
                    record(&topic, &format!("value{i}")),
                    Some(Box::new(move |_metadata, _error| {
                        num_acks.fetch_add(1, Ordering::SeqCst);
                    })),
                )
                .await;
            }
            Producer::flush(&producer).await.expect("flush");
            Producer::close(&producer).await.expect("close producer");
        })
    };

    let topic_ids = tokio::time::timeout(RECREATE_TEST_TIMEOUT, recreate_topic_future)
        .await
        .expect("topic recreation loop timed out")
        .expect("topic recreation task panicked");
    tokio::time::timeout(RECREATE_TEST_TIMEOUT, producer_future)
        .await
        .expect("producer task timed out")
        .expect("producer task panicked");
    assert_eq!(MAX_NUM_TOPIC_RECREATION_ATTEMPTS, topic_ids.len());
    assert_eq!(NUM_RECORDS, num_acks.load(Ordering::SeqCst));
}

/// Translated from `ProducerSendWhileDeletionTest.testSendWithTopicReassignmentIsMidWay`
/// (`ProducerSendWhileDeletionTest.java:204-235`): the producer keeps producing
/// after the partition's only replica (and so its leader) moves to another
/// broker, and the topic id is unchanged.
#[tokio::test(flavor = "multi_thread")]
async fn test_send_with_topic_reassignment_is_mid_way() {
    let mut ctx = TestContext::new(deletion_cluster_config()).await;
    let topic = ctx.topic("topic");
    let admin = create_admin(&ctx);
    let producer = create_producer(ctx.bootstrap_servers());

    // Create topic with leader as 0 (here 1) for the 1 partition.
    create_topic_with_assignment(&admin, &topic, BTreeMap::from([(0, vec![1])])).await;
    assert_leader(&admin, &topic, 0, 1).await;

    let topic_details = topic_metadata(&admin, &topic).await.expect("topic metadata");
    for i in 1..=NUM_RECORDS {
        let resp = send_and_get(&producer, &topic, &format!("value{i}")).await;
        assert_eq!(topic, resp.topic());
    }

    // Change replica assignment from 0 to 1 (here 1 to 2). Leadership moves
    // to 1 (here 2).
    reassign(&admin, &topic, &[0], &[2]).await;

    assert_leader(&admin, &topic, 0, 2).await;
    assert_eq!(
        topic_details.topic_id(),
        topic_metadata(&admin, &topic).await.expect("topic metadata").topic_id()
    );

    // Producer should be able to send messages even after topic gets reassigned
    let record_metadata = send_and_get(&producer, &topic, "value").await;
    assert_eq!(topic, record_metadata.topic());

    Producer::close(&producer).await.expect("close producer");
    admin.close().await;
}
