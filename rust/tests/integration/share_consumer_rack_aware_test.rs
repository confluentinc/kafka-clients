// Copyright 2026 Confluent Inc.
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

//! Integration tests translated from
//! `org.apache.kafka.clients.consumer.ShareConsumerRackAwareTest` (Apache Kafka
//! 4.4.0, `clients-integration-tests`).
//!
//! The suite does not extend `ShareConsumerTestBase` in Java, so it has no
//! warmup; it borrows only the fixture's helpers.
//!
//! # The broker assignor
//!
//! The cluster is configured with
//! `group.share.assignors=org.apache.kafka.clients.consumer.RackAwareTestAssignor`,
//! a class of the `clients-integration-tests` test sources. Java's test kit has
//! it on the broker's classpath; here the published test jar
//! (`kafka-clients-integration-tests-4.4.0-test.jar`) is copied into the
//! brokers' `/opt/kafka/libs` ([`TestJar::ClientsIntegrationTests`]). The jar is
//! built against the 4.4 group-coordinator assignor API, so both arms run only
//! on a 4.4+ broker ([`RACK_AWARE_ASSIGNOR_RELEASE`]); as both Java arms set
//! `group.share.assignment.interval.ms`, which every such broker has, there is
//! no plain-named default-behaviour test as in `share_consumer_test.rs`.
//!
//! # Deviations from Java
//!
//! - The topic and group names get the per-test prefix of the shared fixture.
//! - The cluster is dedicated (Java starts a fresh cluster per test): it needs
//!   Java's `KRAFT` node ids 0..2, which the test's replica assignments name.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::time::Duration;

use confluent_kafka::admin::Admin;
use confluent_kafka::admin::AdminClient;
use confluent_kafka::admin::AdminClientConfig;
use confluent_kafka::admin::NewPartitionReassignment;
use confluent_kafka::admin::NewPartitions;
use confluent_kafka::admin::NewTopic;
use confluent_kafka::admin::ShareGroupDescription;
use confluent_kafka::common::TopicPartition;
use confluent_kafka::common::serialization::ByteArrayDeserializer;
use confluent_kafka::common::serialization::ByteArraySerializer;
use confluent_kafka::consumer::KafkaShareConsumer;
use confluent_kafka::consumer::ShareConsumer;
use confluent_kafka::producer::KafkaProducer;
use confluent_kafka::producer::Producer;
use confluent_kafka::producer::ProducerConfig;
use confluent_kafka::producer::ProducerRecord;

use super::share_consumer_test_base::*;
use crate::common::cluster_config::ClusterConfig;
use crate::common::cluster_config::Type;
use crate::common::test_context::TestContext;
use crate::common::test_jars::TestJar;
use crate::common::test_utils;

/// The broker release the 4.4.0 test jar holding `RackAwareTestAssignor` is
/// built for.
const RACK_AWARE_ASSIGNOR_RELEASE: (u32, u32) = (4, 4);

/// The class-level `@ClusterTestDefaults` (`types = KRAFT`, 3 brokers, a
/// `broker.rack` per broker id, the test assignor) plus one arm's
/// `group.share.assignment.interval.ms`.
fn rack_aware_cluster_config(assignment_interval_ms: &str) -> ClusterConfig {
    let broker_properties = (0..3)
        .map(|id| (id, BTreeMap::from([("KAFKA_BROKER_RACK".to_string(), format!("rack{id}"))])))
        .collect();
    ClusterConfig {
        brokers: 3,
        cluster_type: Type::Kraft,
        controllers: 1,
        dedicated: true,
        server_properties: BTreeMap::from([
            (
                "KAFKA_GROUP_SHARE_ASSIGNORS".to_string(),
                "org.apache.kafka.clients.consumer.RackAwareTestAssignor".to_string(),
            ),
            (
                "KAFKA_GROUP_SHARE_ASSIGNMENT_INTERVAL_MS".to_string(),
                assignment_interval_ms.to_string(),
            ),
        ]),
        broker_properties,
        broker_libs: vec![TestJar::ClientsIntegrationTests],
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_share_consumer_with_rack_aware_assignor_assignment_interval_0() {
    if skip_below(
        RACK_AWARE_ASSIGNOR_RELEASE,
        "testShareConsumerWithRackAwareAssignor (interval 0)",
    ) {
        return;
    }
    share_consumer_with_rack_aware_assignor(rack_aware_cluster_config("0")).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_share_consumer_with_rack_aware_assignor_assignment_interval_1000() {
    if skip_below(
        RACK_AWARE_ASSIGNOR_RELEASE,
        "testShareConsumerWithRackAwareAssignor (interval 1000)",
    ) {
        return;
    }
    share_consumer_with_rack_aware_assignor(rack_aware_cluster_config("1000")).await;
}

/// Java's `testShareConsumerWithRackAwareAssignor`.
async fn share_consumer_with_rack_aware_assignor(config: ClusterConfig) {
    let mut ctx = TestContext::new(config).await;
    ctx.cluster().wait_for_ready_brokers().await;
    let group_id = ctx.group_id("group0");
    let topic = ctx.topic("test-topic");
    let admin = admin(&ctx);
    let producer = producer(&ctx);
    let mut consumer0 = share_consumer(&ctx, &group_id, "client0", "rack0");
    let mut consumer1 = share_consumer(&ctx, &group_id, "client1", "rack1");
    let mut consumer2 = share_consumer(&ctx, &group_id, "client2", "rack2");

    // Create a new topic with 1 partition on broker 0.
    admin
        .create_topics(&[NewTopic::with_replicas_assignments(
            topic.clone(),
            BTreeMap::from([(0, vec![0])]),
        )])
        .all()
        .get()
        .await
        .expect("createTopics");
    test_utils::wait_for_all_partitions_metadata(admin.as_ref(), &topic, 1).await;

    send_record(
        &producer,
        ProducerRecord::with_key(topic.clone(), Some(b"key".to_vec()), Some(b"value".to_vec())),
    )
    .await
    .expect("send");
    producer.flush().await.expect("flush");

    consumer0.subscribe(vec![topic.clone()]).await.expect("subscribe");
    consumer1.subscribe(vec![topic.clone()]).await.expect("subscribe");
    consumer2.subscribe(vec![topic.clone()]).await.expect("subscribe");

    let tp = |partition: i32| TopicPartition::new(topic.clone(), partition);
    let mut consumers = [&mut consumer0, &mut consumer1, &mut consumer2];
    wait_for_expected_assignment(
        admin.as_ref(),
        &group_id,
        &mut consumers,
        HashMap::from([
            ("client0", HashSet::from([tp(0)])),
            ("client1", HashSet::new()),
            ("client2", HashSet::new()),
        ]),
        "Consumer 0 should be assigned to topic partition 0",
    )
    .await;

    // Add a new partition 1 and 2 to broker 1.
    admin
        .create_partitions(&HashMap::from([(
            topic.clone(),
            NewPartitions::increase_to_with_new_assignments(3, vec![vec![1], vec![1]]),
        )]))
        .all()
        .get()
        .await
        .expect("createPartitions");
    test_utils::wait_for_all_partitions_metadata(admin.as_ref(), &topic, 3).await;

    wait_for_expected_assignment(
        admin.as_ref(),
        &group_id,
        &mut consumers,
        HashMap::from([
            ("client0", HashSet::from([tp(0)])),
            ("client1", HashSet::from([tp(1), tp(2)])),
            ("client2", HashSet::new()),
        ]),
        "Consumer 1 should be assigned to topic partition 1 and 2",
    )
    .await;

    // Add a new partition 3, 4, and 5 to broker 2.
    admin
        .create_partitions(&HashMap::from([(
            topic.clone(),
            NewPartitions::increase_to_with_new_assignments(6, vec![vec![2], vec![2], vec![2]]),
        )]))
        .all()
        .get()
        .await
        .expect("createPartitions");
    test_utils::wait_for_all_partitions_metadata(admin.as_ref(), &topic, 6).await;

    wait_for_expected_assignment(
        admin.as_ref(),
        &group_id,
        &mut consumers,
        HashMap::from([
            ("client0", HashSet::from([tp(0)])),
            ("client1", HashSet::from([tp(1), tp(2)])),
            ("client2", HashSet::from([tp(3), tp(4), tp(5)])),
        ]),
        "Consumer 2 should be assigned to topic partition 3, 4, and 5",
    )
    .await;

    // Change partitions to different brokers.
    // partition 0 -> broker 2
    // partition 1 -> broker 2
    // partition 2 -> broker 2
    // partition 3 -> broker 1
    // partition 4 -> broker 1
    // partition 5 -> broker 0
    let reassignment = |broker: i32| Some(NewPartitionReassignment::new(vec![broker]).expect("a valid reassignment"));
    admin
        .alter_partition_reassignments(&HashMap::from([
            (tp(0), reassignment(2)),
            (tp(1), reassignment(2)),
            (tp(2), reassignment(2)),
            (tp(3), reassignment(1)),
            (tp(4), reassignment(1)),
            (tp(5), reassignment(0)),
        ]))
        .all()
        .get()
        .await
        .expect("alterPartitionReassignments");
    wait_for_expected_assignment(
        admin.as_ref(),
        &group_id,
        &mut consumers,
        HashMap::from([
            ("client0", HashSet::from([tp(5)])),
            ("client1", HashSet::from([tp(3), tp(4)])),
            ("client2", HashSet::from([tp(0), tp(1), tp(2)])),
        ]),
        "Consumer with topic partition mapping should be 0 -> 5 | 1 -> 3, 4 | 2 -> 0, 1, 2",
    )
    .await;

    close_share_consumer(consumer0).await;
    close_share_consumer(consumer1).await;
    close_share_consumer(consumer2).await;
    close_producer(producer).await;
    close_admin(admin).await;
}

/// Java's `TestUtils.waitForCondition(() -> { consumer0.poll(1 s);
/// consumer1.poll(1 s); consumer2.poll(1 s); ... isExpectedAssignment(...) },
/// message)`, with the default 15 s and 100 ms.
async fn wait_for_expected_assignment(
    admin: &dyn Admin,
    group_id: &str,
    consumers: &mut [&mut BytesShareConsumer; 3],
    expected_assignments: HashMap<&str, HashSet<TopicPartition>>,
    message: &str,
) {
    let mut deadline = WaitDeadline::new(DEFAULT_MAX_WAIT_MS, DEFAULT_POLL_INTERVAL_MS, message);
    loop {
        for consumer in consumers.iter_mut() {
            consumer.poll(Duration::from_millis(1000)).await.expect("poll");
        }
        // Java's `assertDoesNotThrow` around the describe throws an
        // `AssertionError` that `waitForCondition` retries until its deadline
        // (a group with no member yet is `GroupIdNotFound`), so an error is
        // "not yet" here too.
        match admin.describe_share_groups(&[group_id.to_string()]).all().get().await {
            Ok(groups) => {
                if is_expected_assignment(groups.get(group_id), 3, &expected_assignments) {
                    return;
                }
                deadline.clear_error();
            },
            Err(e) => deadline.record_error(format!("describeShareGroups: {e}")),
        }
        deadline.fail_or_pause().await;
    }
}

/// Java's `isExpectedAssignment`.
fn is_expected_assignment(
    group_description: Option<&ShareGroupDescription>,
    member_count: usize,
    expected_assignments: &HashMap<&str, HashSet<TopicPartition>>,
) -> bool {
    group_description.is_some_and(|description| {
        description.members().len() == member_count
            && description.members().iter().all(|member| {
                expected_assignments.get(member.client_id()) == Some(member.assignment().topic_partitions())
            })
    })
}

/// The client configuration of this run's listener.
fn configured(ctx: &TestContext) -> HashMap<String, String> {
    let mut props = HashMap::new();
    ctx.configure(&mut props);
    props
}

/// Java's `clusterInstance.admin()`.
fn admin(ctx: &TestContext) -> Box<dyn Admin> {
    AdminClient::create(AdminClientConfig::new(&configured(ctx)).expect("valid admin config")).expect("admin client")
}

/// Java's `clusterInstance.producer()`: byte-array serializers.
fn producer(ctx: &TestContext) -> BytesProducer {
    KafkaProducer::new(
        ProducerConfig::new(&configured(ctx)).expect("valid producer config"),
        ByteArraySerializer::new(),
        ByteArraySerializer::new(),
    )
    .expect("KafkaProducer::new")
}

/// Java's `clusterInstance.shareConsumer(Map.of(GROUP_ID, CLIENT_ID,
/// CLIENT_RACK))`: byte-array deserializers.
fn share_consumer(ctx: &TestContext, group_id: &str, client_id: &str, client_rack: &str) -> BytesShareConsumer {
    let mut props = configured(ctx);
    props.insert("group.id".to_string(), group_id.to_string());
    props.insert("client.id".to_string(), client_id.to_string());
    props.insert("client.rack".to_string(), client_rack.to_string());
    KafkaShareConsumer::new(&props, ByteArrayDeserializer::new(), ByteArrayDeserializer::new())
        .expect("KafkaShareConsumer::new")
}
