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
//! `kafka/clients/clients-integration-tests/src/test/java/org/apache/kafka/clients/consumer/ConsumerBounceTest.java`
//! (Apache Kafka 4.3.1).
//!
//! # Methods classification
//!
//! ## Translated (KIP-848 / `GroupProtocol.CONSUMER` arm only)
//!
//! - `testAsyncConsumerReceivesFatalExceptionWhenGroupPassesMaxSize` (line 414).
//!   Java parameterizes it over two `@ClusterTest` arms that set the broker's
//!   `group.consumer.assignment.interval.ms` to `0` and `1000`. That broker
//!   config is new in 4.3 and does not exist on the `apache/kafka:4.2.0` image
//!   the harness runs, so:
//!     - `test_async_consumer_receives_fatal_exception_when_group_passes_max_size`
//!       runs the same body with the broker's default assignment behaviour
//!       (the only behaviour a 4.2 broker has);
//!     - `..._assignment_interval_0` and `..._assignment_interval_1000` carry
//!       the two Java arms verbatim and are `#[ignore]`d until the image is
//!       bumped to 4.3.x (on 4.2 the property would be silently ignored and
//!       the arms would duplicate the default one).
//!
//! ## SKIPped
//!
//! - `testClassic*` twins — classic-protocol-only (`consumer-threading.md` §20).
//! - `testAsyncConsumerConsumptionWithBrokerFailures`,
//!   `testAsyncConsumerSeekAndCommitWithBrokerFailures`,
//!   `testAsyncSubscribeWhenTopicUnavailable`, `testAsyncClose` — they shut
//!   down / restart brokers, and the pooled Docker harness has no broker
//!   stop/restart.
//! - `testAsyncCloseDuringRebalance` — not part of this translation slice.

use std::collections::HashMap;
use std::collections::HashSet;

use confluent_kafka::admin::AdminClientConfig;
use confluent_kafka::admin::KafkaAdminClient;
use confluent_kafka::common::Error;
use confluent_kafka::common::TopicPartition;
use confluent_kafka::common::serialization::ByteArrayDeserializer;
use confluent_kafka::common::serialization::ByteArraySerializer;
use confluent_kafka::consumer::ConsumerConfig;
use confluent_kafka::consumer::KafkaConsumer;
use confluent_kafka::producer::KafkaProducer;
use confluent_kafka::producer::Producer;
use confluent_kafka::producer::ProducerConfig;
use confluent_kafka::producer::ProducerRecord;

use crate::common::cluster_config::ClusterConfig;
use crate::common::consumer_assignment_poller::BytesConsumer;
use crate::common::consumer_assignment_poller::ConsumerAssignmentPoller;
use crate::common::test_context::TestContext;
use crate::common::test_utils;

/// Java's `BROKER_COUNT`.
const BROKER_COUNT: u16 = 3;
/// Java's `MAX_GROUP_SIZE`.
const MAX_GROUP_SIZE: usize = 5;

/// The class-level `@ClusterTestDefaults` of `ConsumerBounceTest` (lines 79-103).
///
/// Deviations, all for properties this test does not exercise:
/// - `unclean.leader.election.enable`, `unclean.leader.election.interval.ms`,
///   `controlled.shutdown.enable`, `broker.heartbeat.interval.ms=50` and
///   `broker.session.timeout.ms=300` exist to speed up the *broker bounces* of
///   the sibling tests; there is no bounce here, and a 300 ms broker session
///   on shared Docker hosts would only fence healthy brokers.
/// - `file.delete.delay.ms` is a topic-level config Java sets at broker level,
///   where Kafka ignores it.
///
/// `group.coordinator.rebalance.protocols=classic,consumer` is set explicitly
/// (it is already the 4.2 default) to keep KIP-848 enabled on this dedicated
/// container regardless of image defaults.
fn cluster_config() -> ClusterConfig {
    let mut cfg = ClusterConfig::with_brokers(BROKER_COUNT);
    for (key, value) in [
        ("KAFKA_GROUP_COORDINATOR_REBALANCE_PROTOCOLS", "classic,consumer"),
        ("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "3"),
        ("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", "1"),
        ("KAFKA_GROUP_MIN_SESSION_TIMEOUT_MS", "10"),
        ("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0"),
        ("KAFKA_GROUP_CONSUMER_MAX_SIZE", "5"),
        ("KAFKA_GROUP_MAX_SIZE", "5"),
        ("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false"),
        ("KAFKA_LOG_INITIAL_TASK_DELAY_MS", "100"),
    ] {
        cfg.server_properties.insert(key.to_string(), value.to_string());
    }
    cfg
}

/// [`cluster_config`] plus one `@ClusterTest(serverProperties = ...)` arm of
/// `testAsyncConsumerReceivesFatalExceptionWhenGroupPassesMaxSize`.
fn cluster_config_with_assignment_interval(interval_ms: &str) -> ClusterConfig {
    let mut cfg = cluster_config();
    cfg.server_properties.insert(
        "KAFKA_GROUP_CONSUMER_ASSIGNMENT_INTERVAL_MS".to_string(),
        interval_ms.to_string(),
    );
    cfg
}

/// `clusterInstance.admin()`.
fn create_admin(ctx: &TestContext) -> KafkaAdminClient {
    let props = HashMap::from([("bootstrap.servers".to_string(), ctx.bootstrap_servers().to_string())]);
    KafkaAdminClient::new(AdminClientConfig::new(&props).expect("valid admin config")).expect("admin client")
}

/// `clusterInstance.consumer(configs)` (`ClusterInstance.java:162-170`):
/// byte-array deserializers and `auto.offset.reset=earliest` unless
/// overridden, on the KIP-848 protocol this suite translates.
fn create_consumer(ctx: &TestContext, configs: &HashMap<String, String>) -> BytesConsumer {
    let mut props = HashMap::from([
        ("bootstrap.servers".to_string(), ctx.bootstrap_servers().to_string()),
        ("group.protocol".to_string(), "consumer".to_string()),
        ("auto.offset.reset".to_string(), "earliest".to_string()),
    ]);
    props.extend(configs.iter().map(|(k, v)| (k.clone(), v.clone())));
    KafkaConsumer::new::<Vec<u8>, Vec<u8>>(
        ConsumerConfig::new(&props).expect("valid consumer config"),
        Box::new(ByteArrayDeserializer),
        Box::new(ByteArrayDeserializer),
    )
    .expect("KafkaConsumer::new should succeed")
}

/// `clusterInstance.producer()`: byte-array serializers, producer defaults.
fn create_producer(ctx: &TestContext) -> KafkaProducer<Vec<u8>, Vec<u8>> {
    let props = HashMap::from([("bootstrap.servers".to_string(), ctx.bootstrap_servers().to_string())]);
    KafkaProducer::new(
        ProducerConfig::new(&props).expect("valid producer config"),
        Box::new(ByteArraySerializer),
        Box::new(ByteArraySerializer),
    )
    .expect("KafkaProducer::new should succeed")
}

/// Java's `addConsumersToGroup` (line 593): create `num_of_consumers_to_add`
/// consumers in `group` and start a poller for each.
async fn add_consumers_to_group(
    ctx: &TestContext,
    consumer_pollers: &mut Vec<ConsumerAssignmentPoller>,
    num_of_consumers_to_add: usize,
    topics_to_subscribe: &[String],
    group: &str,
    consumer_configs: &HashMap<String, String>,
) {
    let mut configs = consumer_configs.clone();
    configs.insert("group.id".to_string(), group.to_string());
    for _ in 0..num_of_consumers_to_add {
        let consumer = create_consumer(ctx, &configs);
        consumer_pollers.push(ConsumerAssignmentPoller::start(consumer, topics_to_subscribe.to_vec()).await);
    }
}

/// Java's `isPartitionAssignmentValid` (line 517) with no expected assignment.
fn is_partition_assignment_valid(
    assignments: &[HashSet<TopicPartition>],
    partitions: &HashSet<TopicPartition>,
) -> bool {
    // 1. Check that every consumer has non-empty assignment
    if assignments.iter().any(HashSet::is_empty) {
        return false;
    }
    // 2. Check that total assigned partitions equals number of unique partitions
    let all_assigned_partitions: HashSet<&TopicPartition> = assignments.iter().flatten().collect();
    if all_assigned_partitions.len() != partitions.len() {
        // Either some partitions were assigned multiple times or some were not assigned
        return false;
    }
    // 3. Check that assigned partitions exactly match the expected set
    partitions.iter().all(|tp| all_assigned_partitions.contains(tp))
}

/// Java's two-argument `validateGroupAssignment` (line 579): wait up to 10 s
/// for a valid assignment across `consumer_pollers`.
async fn validate_group_assignment(
    consumer_pollers: &[ConsumerAssignmentPoller],
    subscriptions: &HashSet<TopicPartition>,
) {
    test_utils::wait_until_true_with_timeout(
        || {
            let assignments: Vec<HashSet<TopicPartition>> = consumer_pollers
                .iter()
                .map(ConsumerAssignmentPoller::consumer_assignment)
                .collect();
            let valid = is_partition_assignment_valid(&assignments, subscriptions);
            if !valid {
                eprintln!(
                    "Did not get valid assignment for partitions {subscriptions:?}. Instead got: {assignments:?}"
                );
            }
            async move { valid }
        },
        &format!("Did not get valid assignment for partitions {subscriptions:?}"),
        10_000,
        100,
    )
    .await;
}

/// Java's `addConsumersToGroupAndWaitForGroupAssignment` (line 486).
async fn add_consumers_to_group_and_wait_for_group_assignment(
    ctx: &TestContext,
    consumer_pollers: &mut Vec<ConsumerAssignmentPoller>,
    num_of_consumers_to_add: usize,
    topics_to_subscribe: &[String],
    subscriptions: &HashSet<TopicPartition>,
    group: &str,
    consumer_config: &HashMap<String, String>,
) {
    // Validation: number of consumers should not exceed number of partitions
    assert!(
        consumer_pollers.len() + num_of_consumers_to_add <= subscriptions.len(),
        "Total consumers exceed number of partitions"
    );
    add_consumers_to_group(
        ctx,
        consumer_pollers,
        num_of_consumers_to_add,
        topics_to_subscribe,
        group,
        consumer_config,
    )
    .await;
    validate_group_assignment(consumer_pollers, subscriptions).await;
}

/// Java's `testConsumerReceivesFatalExceptionWhenGroupPassesMaxSize(CONSUMER)`
/// (line 418), run against `config`.
async fn test_consumer_receives_fatal_exception_when_group_passes_max_size(config: ClusterConfig) {
    let mut ctx = TestContext::new(config).await;
    let group = ctx.group_id("fatal-exception-test");
    let topic = ctx.topic("fatal-exception-test");
    let admin = create_admin(&ctx);

    let num_partition = MAX_GROUP_SIZE;
    let consumer_config = HashMap::from([
        ("max.poll.interval.ms".to_string(), "60000".to_string()),
        ("enable.auto.commit".to_string(), "false".to_string()),
    ]);

    test_utils::create_topic(&admin, &topic, MAX_GROUP_SIZE as i32, BROKER_COUNT as i16).await;
    let partitions: HashSet<TopicPartition> = (0..MAX_GROUP_SIZE as i32)
        .map(|i| TopicPartition::new(topic.clone(), i))
        .collect();
    let topics = vec![topic.clone()];

    let mut consumer_pollers: Vec<ConsumerAssignmentPoller> = Vec::new();
    add_consumers_to_group_and_wait_for_group_assignment(
        &ctx,
        &mut consumer_pollers,
        MAX_GROUP_SIZE,
        &topics,
        &partitions,
        &group,
        &consumer_config,
    )
    .await;

    add_consumers_to_group(&ctx, &mut consumer_pollers, 1, &topics, &group, &consumer_config).await;

    let mut rejected_consumer = consumer_pollers.pop().expect("the extra consumer's poller");

    // Java: `TestUtils.waitForCondition(..., "Extra consumer did not throw an
    // exception")` with the default 15 s bound.
    test_utils::wait_until_true_with_timeout(
        || {
            let thrown = rejected_consumer.thrown_error().is_some();
            async move { thrown }
        },
        "Extra consumer did not throw an exception",
        15_000,
        100,
    )
    .await;

    // Java: `assertInstanceOf(GroupMaxSizeReachedException.class, ...)`.
    let thrown = rejected_consumer.thrown_error().expect("thrown error recorded");
    let Error::GroupMaxSizeReached(thrown) = thrown else {
        panic!("expected GroupMaxSizeReached, got {thrown:?}");
    };
    // Stricter than Java (which asserts only the type): the broker's message
    // (`GroupMetadataManager.java:1576`, `throwIfConsumerGroupIsFull`) is part of the
    // contract the client surfaces verbatim (DoD §3).
    assert_eq!(
        thrown.message(),
        format!("The consumer group has reached its maximum capacity of {MAX_GROUP_SIZE} members.")
    );
    rejected_consumer.shutdown().await;

    // assert group continues to live and the records to be distributed across all partitions.
    let data = b"data".to_vec();
    let producer = create_producer(&ctx);
    for index in 0..num_partition * 100 {
        let record = ProducerRecord::with_partition_key(
            topic.clone(),
            Some((index % num_partition) as i32),
            Some(data.clone()),
            Some(data.clone()),
        )
        .expect("valid producer record");
        <KafkaProducer<Vec<u8>, Vec<u8>> as Producer<Vec<u8>, Vec<u8>>>::send(&producer, record)
            .await
            .expect("send should not fail");
    }
    // Java's try-with-resources `close()` flushes the outstanding sends.
    <KafkaProducer<Vec<u8>, Vec<u8>> as Producer<Vec<u8>, Vec<u8>>>::close(&producer)
        .await
        .expect("producer close");

    test_utils::wait_until_true_with_timeout(
        || {
            let all = consumer_pollers.iter().all(|p| p.received_messages() >= 100);
            async move { all }
        },
        "The consumers in the group could not fetch the expected records",
        10_000,
        100,
    )
    .await;

    // Java's `tearDown`: shut every poller down (each closes its consumer).
    for poller in &mut consumer_pollers {
        poller.shutdown().await;
    }
    ctx.cleanup().await;
}

/// Translates `testAsyncConsumerReceivesFatalExceptionWhenGroupPassesMaxSize`
/// (`ConsumerBounceTest.java:414`) with the broker's default assignment
/// behaviour — see the module docs for why the two Java arms are separate.
#[tokio::test(flavor = "multi_thread")]
async fn test_async_consumer_receives_fatal_exception_when_group_passes_max_size() {
    test_consumer_receives_fatal_exception_when_group_passes_max_size(cluster_config()).await;
}

/// The `group.consumer.assignment.interval.ms=0` arm of
/// `testAsyncConsumerReceivesFatalExceptionWhenGroupPassesMaxSize`
/// (`ConsumerBounceTest.java:407-409`).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "group.consumer.assignment.interval.ms is a 4.3 broker config; the harness image is apache/kafka:4.2.0, \
            which ignores it"]
async fn test_async_consumer_receives_fatal_exception_when_group_passes_max_size_assignment_interval_0() {
    test_consumer_receives_fatal_exception_when_group_passes_max_size(cluster_config_with_assignment_interval("0"))
        .await;
}

/// The `group.consumer.assignment.interval.ms=1000` arm of
/// `testAsyncConsumerReceivesFatalExceptionWhenGroupPassesMaxSize`
/// (`ConsumerBounceTest.java:410-412`).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "group.consumer.assignment.interval.ms is a 4.3 broker config; the harness image is apache/kafka:4.2.0, \
            which ignores it"]
async fn test_async_consumer_receives_fatal_exception_when_group_passes_max_size_assignment_interval_1000() {
    test_consumer_receives_fatal_exception_when_group_passes_max_size(cluster_config_with_assignment_interval("1000"))
        .await;
}
