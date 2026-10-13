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
//!   `group.consumer.assignment.interval.ms` to `0` and `1000` (KIP-1263
//!   assignment batching). That broker config is new in 4.3; a 4.2 broker
//!   ignores it and always assigns at once. The arms are selected at runtime
//!   from the broker tag (`INTEGRATION_TEST_BROKER_TAG`, default 4.2.0):
//!     - on a broker older than 4.3,
//!       `test_async_consumer_receives_fatal_exception_when_group_passes_max_size`
//!       runs the body with the broker's only assignment behaviour (no
//!       batching, the `0` arm's behaviour), and the two Java arms skip
//!       themselves, since there they would duplicate it;
//!     - on 4.3 and later, `..._assignment_interval_0` and
//!       `..._assignment_interval_1000` run the two Java arms verbatim, and the
//!       default-config test skips itself: on 4.4 its default interval is 1000,
//!       so it would duplicate the `1000` arm. This keeps exactly Java's two
//!       arms wherever the broker has the config.
//!
//!   **Deviation (the `1000` arm's assignment bound):** Java waits 10 s for a
//!   valid group assignment (`validateGroupAssignment`, line 579); the `1000`
//!   arm waits 20 s. Under KIP-848 with batching, a target assignment is
//!   recomputed only on a heartbeat that arrives after the interval, so the
//!   four members joining just after the first get the stale (empty) target
//!   and need two `group.consumer.heartbeat.interval.ms` rounds (5 s each) to
//!   converge: measured at about 10.1–10.5 s for the Java KIP-848 client and
//!   for this one alike, on a 4.4 broker (Critic 99, Phase 9 notes). Java's 10 s
//!   holds only because its test runs classic (next paragraph).
//!
//!   **Upstream quirk:** Java's "async" test never sets `group.protocol`.
//!   `testConsumerReceivesFatalExceptionWhenGroupPassesMaxSize(GroupProtocol)`
//!   uses the parameter only for `heartbeat.interval.ms`, and
//!   `ClusterInstance.consumer(...)` adds no protocol, so it runs the default,
//!   classic, for which the assignment interval is inert. These Rust arms are
//!   the only KIP-848 runs of this body (the same quirk as
//!   `testAsyncCloseDuringRebalance` below).
//!
//! The broker-bounce tests (Phase 17) run on a dedicated `Type.KRAFT` cluster
//! (3 brokers + 1 isolated controller, [`bounce_cluster_config`]) and stop /
//! restart brokers through `KafkaCluster::shutdown_broker` / `start_broker`:
//!
//! - `testAsyncConsumerConsumptionWithBrokerFailures` (line 154)
//!   → `test_async_consumer_consumption_with_broker_failures`
//! - `testAsyncConsumerSeekAndCommitWithBrokerFailures` (line 208)
//!   → `test_async_consumer_seek_and_commit_with_broker_failures`
//! - `testAsyncSubscribeWhenTopicUnavailable` (line 258)
//!   → `test_async_subscribe_when_topic_unavailable`
//! - `testAsyncClose` (line 306), including its coordinator-failure and
//!   cluster-failure parts → `test_async_close`
//!
//! ## SKIPped
//!
//! - `testClassic*` twins — classic-protocol-only (`consumer-threading.md` §20).
//! - `testAsyncCloseDuringRebalance` (line 618) — despite its name it runs the
//!   classic protocol: `testCloseDuringRebalance` builds its consumers (and
//!   `createConsumerToRebalance` its extra members) without
//!   `group.protocol`, so `clusterInstance.consumer(...)` defaults to
//!   `classic`, and only `heartbeat.interval.ms` is gated on the arm. The
//!   classic protocol is out of scope (`consumer-threading.md` §20).

use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use async_trait::async_trait;
use confluent_kafka::admin::Admin;
use confluent_kafka::admin::AdminClient;
use confluent_kafka::admin::AdminClientConfig;
use confluent_kafka::admin::OffsetSpec;
use confluent_kafka::common::Error;
use confluent_kafka::common::TopicPartition;
use confluent_kafka::common::serialization::ByteArrayDeserializer;
use confluent_kafka::common::serialization::ByteArraySerializer;
use confluent_kafka::consumer::CloseOptions;
use confluent_kafka::consumer::ConsumerConfig;
use confluent_kafka::consumer::ConsumerRebalanceListener;
use confluent_kafka::consumer::KafkaConsumer;
use confluent_kafka::producer::KafkaProducer;
use confluent_kafka::producer::Producer;
use confluent_kafka::producer::ProducerConfig;
use confluent_kafka::producer::ProducerRecord;
use rand::Rng;
use rand::SeedableRng;
use rand::seq::IndexedRandom;

use crate::common::cluster_config::ClusterConfig;
use crate::common::consumer_assignment_poller::BytesConsumer;
use crate::common::consumer_assignment_poller::ConsumerAssignmentPoller;
use crate::common::kafka_cluster;
use crate::common::kafka_cluster::KafkaCluster;
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
fn create_admin(ctx: &TestContext) -> Box<dyn Admin> {
    let props = HashMap::from([("bootstrap.servers".to_string(), ctx.bootstrap_servers().to_string())]);
    AdminClient::create(AdminClientConfig::new(&props).expect("valid admin config")).expect("admin client")
}

/// `clusterInstance.consumer(configs)` (`ClusterInstance.java:162-170`):
/// byte-array deserializers, `auto.offset.reset=earliest` and a fresh
/// `group_<random>` group id unless overridden, on the KIP-848 protocol this
/// suite translates.
fn create_consumer(ctx: &TestContext, configs: &HashMap<String, String>) -> BytesConsumer {
    static NEXT_DEFAULT_GROUP: AtomicUsize = AtomicUsize::new(0);
    let default_group = ctx.group_id(&format!("group_{}", NEXT_DEFAULT_GROUP.fetch_add(1, Ordering::SeqCst)));
    let mut props = HashMap::from([
        ("bootstrap.servers".to_string(), ctx.bootstrap_servers().to_string()),
        ("group.protocol".to_string(), "consumer".to_string()),
        ("auto.offset.reset".to_string(), "earliest".to_string()),
        ("group.id".to_string(), default_group),
    ]);
    props.extend(configs.iter().map(|(k, v)| (k.clone(), v.clone())));
    KafkaConsumer::new::<Vec<u8>, Vec<u8>>(
        ConsumerConfig::new(&props).expect("valid consumer config"),
        Box::new(ByteArrayDeserializer::default()),
        Box::new(ByteArrayDeserializer::default()),
    )
    .expect("KafkaConsumer::new should succeed")
}

/// `clusterInstance.producer()`: byte-array serializers, producer defaults.
fn create_producer(ctx: &TestContext) -> KafkaProducer<Vec<u8>, Vec<u8>> {
    let props = HashMap::from([("bootstrap.servers".to_string(), ctx.bootstrap_servers().to_string())]);
    KafkaProducer::new(
        ProducerConfig::new(&props).expect("valid producer config"),
        Box::new(ByteArraySerializer::default()),
        Box::new(ByteArraySerializer::default()),
    )
    .expect("KafkaProducer::new should succeed")
}

/// Java's `tearDown` `consumers.forEach(Consumer::close)` for one consumer a
/// poller handed back (or the test held directly). As in Java, where a
/// throwing `close` fails the test's `tearDown`, a close error fails the test.
/// The close is bounded so a wedged consumer fails the test instead of hanging
/// it.
async fn close_consumer(consumer: Option<BytesConsumer>) {
    if let Some(mut consumer) = consumer {
        tokio::time::timeout(Duration::from_secs(60), consumer.close())
            .await
            .expect("consumer close did not return within 60 s")
            .expect("consumer close");
    }
}

/// [`close_consumer`] for the max-size test's rejected consumer only: its
/// close error is not asserted, because that consumer may surface its fatal
/// `GroupMaxSizeReached` error again on close. Still bounded.
async fn close_consumer_ignoring_error(consumer: Option<BytesConsumer>) {
    if let Some(mut consumer) = consumer {
        let _ = tokio::time::timeout(Duration::from_secs(60), consumer.close())
            .await
            .expect("consumer close did not return within 60 s");
    }
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

/// Java's `validateGroupAssignment` default bound (line 579).
const DEFAULT_ASSIGNMENT_TIMEOUT_MS: u64 = 10_000;
/// The bound of the `group.consumer.assignment.interval.ms=1000` arm: two
/// broker heartbeat rounds plus reconcile slack (deviation, see the module docs).
const BATCHED_ASSIGNMENT_TIMEOUT_MS: u64 = 20_000;

/// Java's two-argument `validateGroupAssignment` (line 579): wait up to
/// `timeout_ms` (Java: 10 s) for a valid assignment across `consumer_pollers`.
async fn validate_group_assignment(
    consumer_pollers: &[ConsumerAssignmentPoller],
    subscriptions: &HashSet<TopicPartition>,
    timeout_ms: u64,
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
        timeout_ms,
        100,
    )
    .await;
}

/// Java's `addConsumersToGroupAndWaitForGroupAssignment` (line 486), with the
/// assignment bound passed through to [`validate_group_assignment`].
#[expect(clippy::too_many_arguments)]
async fn add_consumers_to_group_and_wait_for_group_assignment(
    ctx: &TestContext,
    consumer_pollers: &mut Vec<ConsumerAssignmentPoller>,
    num_of_consumers_to_add: usize,
    topics_to_subscribe: &[String],
    subscriptions: &HashSet<TopicPartition>,
    group: &str,
    consumer_config: &HashMap<String, String>,
    assignment_timeout_ms: u64,
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
    validate_group_assignment(consumer_pollers, subscriptions, assignment_timeout_ms).await;
}

/// Java's `testConsumerReceivesFatalExceptionWhenGroupPassesMaxSize(CONSUMER)`
/// (line 418), run against `config`, waiting up to `assignment_timeout_ms` for
/// the group's assignment.
async fn test_consumer_receives_fatal_exception_when_group_passes_max_size(
    config: ClusterConfig,
    assignment_timeout_ms: u64,
) {
    let mut ctx = TestContext::new(config).await;
    let group = ctx.group_id("fatal-exception-test");
    let topic = ctx.topic("fatal-exception-test");
    let admin = create_admin(&ctx);

    let num_partition = MAX_GROUP_SIZE;
    let consumer_config = HashMap::from([
        ("max.poll.interval.ms".to_string(), "60000".to_string()),
        ("enable.auto.commit".to_string(), "false".to_string()),
    ]);

    test_utils::create_topic(admin.as_ref(), &topic, MAX_GROUP_SIZE as i32, BROKER_COUNT as i16).await;
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
        assignment_timeout_ms,
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
    close_consumer_ignoring_error(rejected_consumer.shutdown().await).await;

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

    // Java's `tearDown`: shut every poller down, then close its consumer.
    for poller in &mut consumer_pollers {
        close_consumer(poller.shutdown().await).await;
    }
    ctx.cleanup().await;
}

/// The first broker release with `group.consumer.assignment.interval.ms`
/// (KIP-1263).
const ASSIGNMENT_INTERVAL_RELEASE: (u32, u32) = (4, 3);

/// Whether this run's broker has `group.consumer.assignment.interval.ms`; the
/// three arms below pick themselves from it (see the module docs).
fn broker_has_assignment_interval() -> bool {
    kafka_cluster::broker_release() >= ASSIGNMENT_INTERVAL_RELEASE
}

/// Translates `testAsyncConsumerReceivesFatalExceptionWhenGroupPassesMaxSize`
/// (`ConsumerBounceTest.java:414`) with the broker's default assignment
/// behaviour, on a broker older than 4.3 only: there the broker has no
/// assignment interval and the two Java arms skip themselves. On 4.3+ this
/// test skips itself, since the Java arms run and the default interval would
/// duplicate the `1000` arm. See the module docs.
#[tokio::test(flavor = "multi_thread")]
async fn test_async_consumer_receives_fatal_exception_when_group_passes_max_size() {
    if broker_has_assignment_interval() {
        eprintln!(
            "skipped: the broker {:?} has group.consumer.assignment.interval.ms; the two Java arms run instead",
            kafka_cluster::broker_release()
        );
        return;
    }
    test_consumer_receives_fatal_exception_when_group_passes_max_size(cluster_config(), DEFAULT_ASSIGNMENT_TIMEOUT_MS)
        .await;
}

/// The `group.consumer.assignment.interval.ms=0` arm of
/// `testAsyncConsumerReceivesFatalExceptionWhenGroupPassesMaxSize`
/// (`ConsumerBounceTest.java:407-409`). Skips itself on a broker older than 4.3,
/// which ignores the config.
#[tokio::test(flavor = "multi_thread")]
async fn test_async_consumer_receives_fatal_exception_when_group_passes_max_size_assignment_interval_0() {
    if !broker_has_assignment_interval() {
        eprintln!(
            "skipped: group.consumer.assignment.interval.ms is a 4.3 broker config; the broker is {:?}",
            kafka_cluster::broker_release()
        );
        return;
    }
    test_consumer_receives_fatal_exception_when_group_passes_max_size(
        cluster_config_with_assignment_interval("0"),
        DEFAULT_ASSIGNMENT_TIMEOUT_MS,
    )
    .await;
}

/// The `group.consumer.assignment.interval.ms=1000` arm of
/// `testAsyncConsumerReceivesFatalExceptionWhenGroupPassesMaxSize`
/// (`ConsumerBounceTest.java:410-412`), with the 20 s assignment bound
/// (deviation, see the module docs). Skips itself on a broker older than 4.3,
/// which ignores the config.
#[tokio::test(flavor = "multi_thread")]
async fn test_async_consumer_receives_fatal_exception_when_group_passes_max_size_assignment_interval_1000() {
    if !broker_has_assignment_interval() {
        eprintln!(
            "skipped: group.consumer.assignment.interval.ms is a 4.3 broker config; the broker is {:?}",
            kafka_cluster::broker_release()
        );
        return;
    }
    test_consumer_receives_fatal_exception_when_group_passes_max_size(
        cluster_config_with_assignment_interval("1000"),
        BATCHED_ASSIGNMENT_TIMEOUT_MS,
    )
    .await;
}

// ── Broker bounces (Phase 17) ──────────────────────────────────────────

/// Java's `topic` field, created in `setUp` (line 129).
const TOPIC: &str = "topic";
/// Java's `numPartitions` (line 115).
const NUM_PARTITIONS: i32 = 3;
/// Java's `numReplica` (line 116).
const NUM_REPLICA: i16 = 3;
/// The seed of Java's `TestUtils.SEEDED_RANDOM` (`TestUtils.java:97`). The
/// generator differs (Java's LCG vs `StdRng`), so the sequence is not Java's,
/// but it is just as deterministic.
const SEEDED_RANDOM_SEED: u64 = 192_348_092_834;

/// The full class-level `@ClusterTestDefaults` of `ConsumerBounceTest` (lines
/// 79-103) on a dedicated `Type.KRAFT` cluster (`BROKER_COUNT` brokers plus one
/// isolated controller, Java's `@ClusterTest` default), since these tests stop
/// and restart brokers.
///
/// Deviation: `file.delete.delay.ms` is dropped — it is a topic-level config
/// Java sets at broker level, where Kafka ignores it.
fn bounce_cluster_config() -> ClusterConfig {
    let props = [
        ("KAFKA_GROUP_COORDINATOR_REBALANCE_PROTOCOLS", "classic,consumer"),
        ("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "3"),
        ("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", "1"),
        ("KAFKA_GROUP_MIN_SESSION_TIMEOUT_MS", "10"),
        ("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0"),
        ("KAFKA_GROUP_CONSUMER_MAX_SIZE", "5"),
        ("KAFKA_GROUP_MAX_SIZE", "5"),
        ("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false"),
        ("KAFKA_LOG_INITIAL_TASK_DELAY_MS", "100"),
        ("KAFKA_CONTROLLED_SHUTDOWN_ENABLE", "false"),
        ("KAFKA_UNCLEAN_LEADER_ELECTION_ENABLE", "true"),
        ("KAFKA_UNCLEAN_LEADER_ELECTION_INTERVAL_MS", "50"),
        ("KAFKA_BROKER_HEARTBEAT_INTERVAL_MS", "50"),
        ("KAFKA_BROKER_SESSION_TIMEOUT_MS", "300"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect();
    ClusterConfig::kraft_dedicated(BROKER_COUNT, 1).set_server_properties(props)
}

/// Java's `setUp` (line 129): `createTopic(topic, numPartitions, numReplica)`.
/// Returns the per-test topic name.
async fn set_up(ctx: &mut TestContext) -> String {
    let topic = ctx.topic(TOPIC);
    let admin = create_admin(ctx);
    test_utils::create_topic(admin.as_ref(), &topic, NUM_PARTITIONS, NUM_REPLICA).await;
    admin.close_with_timeout(Duration::from_secs(5)).await;
    topic
}

/// Milliseconds since the Unix epoch — Java's `System.currentTimeMillis()`.
fn current_time_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before Unix epoch")
        .as_millis() as i64
}

/// `ClientsTestUtils.sendRecords(cluster, tp, numRecords)`
/// (`ClientsTestUtils.java:242`): a fresh producer sends `numRecords` records
/// (`key i` / `value i`, 1 ms timestamp increments), then flushes and closes.
async fn send_records(ctx: &TestContext, tp: &TopicPartition, num_records: usize) {
    let producer = create_producer(ctx);
    let starting_timestamp = current_time_ms();
    for i in 0..num_records {
        let record = ProducerRecord::with_partition_timestamp_key(
            tp.topic().to_string(),
            Some(tp.partition()),
            Some(starting_timestamp + i as i64),
            Some(format!("key {i}").into_bytes()),
            Some(format!("value {i}").into_bytes()),
        )
        .expect("valid producer record");
        <KafkaProducer<Vec<u8>, Vec<u8>> as Producer<Vec<u8>, Vec<u8>>>::send(&producer, record)
            .await
            .expect("send should not fail");
    }
    producer.flush().await.expect("producer flush");
    <KafkaProducer<Vec<u8>, Vec<u8>> as Producer<Vec<u8>, Vec<u8>>>::close(&producer)
        .await
        .expect("producer close");
}

/// Java's `BounceBrokerScheduler` (line 784), a `ShutdownableThread` that
/// kills a random broker, restarts it after 500 ms, and repeats `numIters`
/// times with a 500 ms pause in between.
///
/// The thread becomes a future ([`Self::run`]) the test drives concurrently
/// with its consumer loop through `tokio::join!` in the same task (the harness
/// hands out the cluster by reference), and `isRunning()` becomes an atomic
/// flag the consumer loop reads. `join!` never cancels either side, so a
/// broker stop/start is never abandoned half-way.
struct BounceBrokerScheduler {
    num_iters: usize,
    running: AtomicBool,
}

impl BounceBrokerScheduler {
    fn new(num_iters: usize) -> Self {
        Self { num_iters, running: AtomicBool::new(true) }
    }

    /// `ShutdownableThread.isRunning()`.
    fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// `killRandomBroker()`: `shutdownBroker(TestUtils.randomSelect(brokerIds()))`.
    async fn kill_random_broker(cluster: &KafkaCluster) {
        let ids: Vec<i32> = cluster.broker_ids().into_iter().collect();
        let id = *ids.choose(&mut rand::rng()).expect("at least one broker");
        cluster.shutdown_broker(id).await;
    }

    /// `ShutdownableThread.run()` looping `doWork()` until `initiateShutdown()`.
    async fn run(&self, cluster: &KafkaCluster) {
        for iter in 1..=self.num_iters {
            Self::kill_random_broker(cluster).await;
            tokio::time::sleep(Duration::from_millis(500)).await;
            restart_dead_brokers(cluster).await;

            if iter == self.num_iters {
                self.running.store(false, Ordering::SeqCst);
            } else {
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
}

/// Java's `restartDeadBrokers()` (lines 713 and 800): start every broker that
/// is shut down.
async fn restart_dead_brokers(cluster: &KafkaCluster) {
    let alive = cluster.alive_broker_ids();
    for id in cluster.broker_ids().difference(&alive) {
        cluster.start_broker(*id).await;
    }
}

/// Java's `consumeWithBrokerFailures(numIters, CONSUMER)` (line 162):
/// produce 1000 records, then consume them while brokers are killed and
/// restarted at random, committing after every non-empty poll and checking
/// that the position and the committed offset agree.
async fn consume_with_broker_failures(num_iters: usize) {
    let mut ctx = TestContext::new(bounce_cluster_config()).await;
    let topic = set_up(&mut ctx).await;
    let topic_partition = TopicPartition::new(topic.clone(), 0);
    let num_records: usize = 1000;
    send_records(&ctx, &topic_partition, num_records).await;

    let mut consumer = create_consumer(&ctx, &HashMap::new());
    consumer
        .subscribe_with_topics(vec![topic.clone()])
        .await
        .expect("subscribe should succeed");

    let scheduler = BounceBrokerScheduler::new(num_iters);
    let consume = async {
        let mut consumed: i64 = 0;
        while scheduler.is_running() {
            let records = consumer.poll(Duration::from_millis(100)).await.expect("poll should succeed");

            for record in &records {
                assert_eq!(consumed, record.offset());
                consumed += 1;
            }

            if !records.is_empty() {
                consumer.commit_sync().await.expect("commitSync should succeed");

                let current_position = consumer.position(&topic_partition).await.expect("position");
                let committed_offset = consumer
                    .committed(std::slice::from_ref(&topic_partition))
                    .await
                    .expect("committed")
                    .get(&topic_partition)
                    .expect("a committed offset for the partition")
                    .offset();
                assert_eq!(current_position, committed_offset);

                if current_position == num_records as i64 {
                    consumer.seek_to_beginning(&[]).await.expect("seekToBeginning");
                    consumed = 0;
                }
            }
        }
    };
    tokio::join!(scheduler.run(ctx.cluster()), consume);

    close_consumer(Some(consumer)).await;
}

/// Translates `testAsyncConsumerConsumptionWithBrokerFailures`
/// (`ConsumerBounceTest.java:154`).
#[tokio::test(flavor = "multi_thread")]
async fn test_async_consumer_consumption_with_broker_failures() {
    consume_with_broker_failures(10).await;
}

/// Java's wait (lines 220-222) until every broker's local log of `tp` has a
/// high watermark of `num_records`, within 30 s.
///
/// Deviation: Java reads each broker's `replicaManager` in-process. Over the
/// wire only the leader's high watermark is visible (`ListOffsets` `LATEST`),
/// so the wait also requires every replica to be in the ISR (`describeTopics`):
/// an in-sync follower has fetched up to the leader's log end, and the
/// leader's high watermark only reaches `num_records` once every ISR member
/// has, so a follower elected after a bounce serves the same log end.
async fn wait_for_high_watermark(admin: &dyn Admin, tp: &TopicPartition, num_records: i64) {
    let specs = HashMap::from([(tp.clone(), OffsetSpec::latest())]);
    test_utils::wait_until_true_with_timeout(
        || {
            let offsets = admin.list_offsets(&specs);
            let described = admin.describe_topics_with_topic_names(&[tp.topic().to_string()]);
            async move {
                let high_watermark = match offsets.all().get().await {
                    Ok(offsets) => offsets.get(tp).map(|info| info.offset()),
                    Err(_) => None,
                };
                let isr_size = match described.all_topic_names() {
                    Some(future) => future.get().await.ok().and_then(|topics| {
                        topics.get(tp.topic()).and_then(|description| {
                            description
                                .partitions()
                                .iter()
                                .find(|info| info.partition() == tp.partition())
                                .map(|info| info.isr().len())
                        })
                    }),
                    None => None,
                };
                high_watermark == Some(num_records) && isr_size == Some(BROKER_COUNT as usize)
            }
        },
        "Failed to update high watermark for followers after timeout.",
        30_000,
        100,
    )
    .await;
}

/// Java's `seekAndCommitWithBrokerFailures(numIters, CONSUMER)` (line 212):
/// with a manually assigned partition, randomly seek to the end, seek to a
/// random offset, or commit, while brokers are killed and restarted.
async fn seek_and_commit_with_broker_failures(num_iters: usize) {
    let mut ctx = TestContext::new(bounce_cluster_config()).await;
    let topic = set_up(&mut ctx).await;
    let topic_partition = TopicPartition::new(topic.clone(), 0);
    let num_records: usize = 1000;
    send_records(&ctx, &topic_partition, num_records).await;

    let mut consumer = create_consumer(&ctx, &HashMap::new());
    consumer
        .assign(vec![topic_partition.clone()])
        .await
        .expect("assign should succeed");
    consumer
        .seek_with_offset(topic_partition.clone(), 0)
        .await
        .expect("seek should succeed");

    let admin = create_admin(&ctx);
    wait_for_high_watermark(admin.as_ref(), &topic_partition, num_records as i64).await;
    admin.close_with_timeout(Duration::from_secs(5)).await;

    let scheduler = BounceBrokerScheduler::new(num_iters);
    let seek_and_commit = async {
        // Java's `TestUtils.SEEDED_RANDOM`: a fixed-seed generator.
        let mut random = rand::rngs::StdRng::seed_from_u64(SEEDED_RANDOM_SEED);
        while scheduler.is_running() {
            let coin = random.random_range(0..3);

            if coin == 0 {
                eprintln!("Seeking to end of log.");
                consumer.seek_to_end(&[]).await.expect("seekToEnd");
                assert_eq!(num_records as i64, consumer.position(&topic_partition).await.expect("position"));
            } else if coin == 1 {
                let pos = random.random_range(0..num_records as i64);
                eprintln!("Seeking to {pos}");
                consumer.seek_with_offset(topic_partition.clone(), pos).await.expect("seek");
                assert_eq!(pos, consumer.position(&topic_partition).await.expect("position"));
            } else {
                eprintln!("Committing offset.");
                consumer.commit_sync().await.expect("commitSync should succeed");
                let position = consumer.position(&topic_partition).await.expect("position");
                let committed = consumer
                    .committed(std::slice::from_ref(&topic_partition))
                    .await
                    .expect("committed")
                    .get(&topic_partition)
                    .expect("a committed offset for the partition")
                    .offset();
                assert_eq!(position, committed);
            }
        }
    };
    tokio::join!(scheduler.run(ctx.cluster()), seek_and_commit);

    close_consumer(Some(consumer)).await;
}

/// Translates `testAsyncConsumerSeekAndCommitWithBrokerFailures`
/// (`ConsumerBounceTest.java:208`).
#[tokio::test(flavor = "multi_thread")]
async fn test_async_consumer_seek_and_commit_with_broker_failures() {
    seek_and_commit_with_broker_failures(5).await;
}

/// Java's `receiveExactRecords(poller, numRecords, timeoutMs)` (line 778):
/// wait until the poller has received exactly `num_records` records.
async fn receive_exact_records(poller: &ConsumerAssignmentPoller, num_records: usize, timeout_ms: u64) {
    test_utils::wait_until_true_with_timeout(
        || {
            let received = poller.received_messages() == num_records;
            async move { received }
        },
        &format!("Consumer did not receive expected {num_records}."),
        timeout_ms,
        100,
    )
    .await;
    // Java's lazy message also reports the count; the helper takes a fixed
    // message, so report it here (the wait above already passed).
    assert_eq!(
        poller.received_messages(),
        num_records,
        "Consumer did not receive expected {num_records}. It received {}",
        poller.received_messages()
    );
}

/// Translates `testAsyncSubscribeWhenTopicUnavailable`
/// (`ConsumerBounceTest.java:258`, body `testSubscribeWhenTopicUnavailable`
/// :262): subscribe to a topic that does not exist yet, create it 2 s later,
/// consume 1000 records; then stop every broker, restart them all, and consume
/// 1000 more on the same consumer.
#[tokio::test(flavor = "multi_thread")]
async fn test_async_subscribe_when_topic_unavailable() {
    let mut ctx = TestContext::new(bounce_cluster_config()).await;
    set_up(&mut ctx).await;
    let new_topic = ctx.topic("new-topic");
    let new_topic_partition = TopicPartition::new(new_topic.clone(), 0);
    let num_records: usize = 1000;

    let mut consumer = create_consumer(
        &ctx,
        &HashMap::from([
            ("max.poll.interval.ms".to_string(), "6000".to_string()),
            ("metadata.max.age.ms".to_string(), "100".to_string()),
        ]),
    );
    consumer
        .subscribe_with_topics(vec![new_topic.clone()])
        .await
        .expect("subscribe should succeed");
    consumer.poll(Duration::ZERO).await.expect("poll should succeed");
    // Schedule topic creation after 2 seconds
    let create_new_topic = {
        let bootstrap = ctx.bootstrap_servers().to_string();
        let new_topic = new_topic.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(2)).await;
            let props = HashMap::from([("bootstrap.servers".to_string(), bootstrap)]);
            let admin =
                AdminClient::create(AdminClientConfig::new(&props).expect("valid admin config")).expect("admin client");
            test_utils::create_topic(admin.as_ref(), &new_topic, NUM_PARTITIONS, NUM_REPLICA).await;
            admin.close_with_timeout(Duration::from_secs(5)).await;
        })
    };

    // Start first poller
    let mut poller = ConsumerAssignmentPoller::start(consumer, vec![new_topic.clone()]).await;
    send_records(&ctx, &new_topic_partition, num_records).await;
    receive_exact_records(&poller, num_records, 60_000).await;
    let consumer = poller.shutdown().await.expect("first poller owned the consumer");
    // Java's `assertDoesNotThrow` around the scheduled creation: surface a
    // failure of the creation task (it finished before the records arrived).
    tokio::time::timeout(Duration::from_secs(60), create_new_topic)
        .await
        .expect("topic creation task did not finish")
        .expect("topic creation should not fail");

    // Simulate broker failure and recovery
    let cluster = ctx.cluster();
    for id in cluster.broker_ids() {
        cluster.shutdown_broker(id).await;
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    for id in cluster.broker_ids() {
        cluster.start_broker(id).await;
    }

    // Start second poller after recovery
    let mut poller2 = ConsumerAssignmentPoller::start(consumer, vec![new_topic.clone()]).await;

    send_records(&ctx, &new_topic_partition, num_records).await;
    receive_exact_records(&poller2, num_records, 60_000).await;

    // Java's `tearDown`.
    close_consumer(poller2.shutdown().await).await;
}

/// Java's `gracefulCloseTimeMs` (line 111).
const GRACEFUL_CLOSE_TIME_MS: u64 = 1000;
/// Java's `Long.MAX_VALUE` close timeout, in milliseconds.
const LONG_MAX_VALUE_MS: u64 = i64::MAX as u64;

/// Java's `createConsumerAndReceive` (line 692): a consumer in `group_id`
/// that either subscribes to `topic` or is manually assigned `topic_partition`,
/// polled by a [`ConsumerAssignmentPoller`] until it has received exactly
/// `num_records` records; the poller is then shut down and the consumer
/// returned.
async fn create_consumer_and_receive(
    ctx: &TestContext,
    topic: &str,
    topic_partition: &TopicPartition,
    group_id: &str,
    manual_assign: bool,
    num_records: usize,
    consumer_config: &HashMap<String, String>,
) -> BytesConsumer {
    let mut configs = consumer_config.clone();
    configs.insert("group.id".to_string(), group_id.to_string());
    let consumer = create_consumer(ctx, &configs);
    let mut poller = if manual_assign {
        ConsumerAssignmentPoller::start_with_partitions_to_assign(consumer, vec![topic_partition.clone()]).await
    } else {
        ConsumerAssignmentPoller::start(consumer, vec![topic.to_string()]).await
    };
    receive_exact_records(&poller, num_records, 60_000).await;
    poller.shutdown().await.expect("the poller owned the consumer")
}

/// Java's `submitCloseAndValidate` (line 752) followed by `.get()` — every
/// Java call site waits for the close at once, so the executor hop is
/// dropped: close with `close_timeout_ms` and check the elapsed time against
/// the optional bounds (with Java's 2 s `closeGraceTimeMs` on the upper one).
///
/// A failing `close()` fails the test, as the rethrow from Java's `get()`
/// does. The close is bounded by the upper limit, so a close that would take
/// too long fails with Java's message instead of hanging the test.
async fn close_and_validate(
    mut consumer: BytesConsumer,
    close_timeout_ms: u64,
    min_close_time_ms: Option<u64>,
    max_close_time_ms: Option<u64>,
) {
    const CLOSE_GRACE_TIME_MS: u64 = 2000;
    let start = std::time::Instant::now();
    eprintln!("Closing consumer with timeout {close_timeout_ms} ms.");
    let close = consumer.close_with_options(CloseOptions::new_timeout(Duration::from_millis(close_timeout_ms)));
    let result = match max_close_time_ms {
        Some(ms) => tokio::time::timeout(Duration::from_millis(ms + CLOSE_GRACE_TIME_MS), close)
            .await
            .unwrap_or_else(|_| panic!("Close took too long {}", start.elapsed().as_millis())),
        None => close.await,
    };
    let time_taken_ms = start.elapsed().as_millis() as u64;
    result.expect("consumer close should succeed");

    if let Some(ms) = max_close_time_ms {
        assert!(time_taken_ms < ms + CLOSE_GRACE_TIME_MS, "Close took too long {time_taken_ms}");
    }
    if let Some(ms) = min_close_time_ms {
        assert!(time_taken_ms >= ms, "Close finished too quickly {time_taken_ms}");
    }
    eprintln!("consumer.close() completed in {time_taken_ms} ms.");
}

/// The anonymous `ConsumerRebalanceListener` of Java's `checkClosedState`
/// (line 727): releases a permit of `assignSemaphore` per assignment.
struct AssignSemaphoreListener {
    permits: Arc<AtomicUsize>,
}

#[async_trait]
impl ConsumerRebalanceListener for AssignSemaphoreListener {
    async fn on_partitions_revoked(&self, _partitions: &[TopicPartition]) -> Result<(), Error> {
        // Do nothing
        Ok(())
    }

    async fn on_partitions_assigned(&self, _partitions: &[TopicPartition]) -> Result<(), Error> {
        self.permits.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// Java's `checkClosedState` (line 721): a new consumer in `group_id` must be
/// assigned partitions promptly and, when `committed_records > 0`, see that
/// offset committed for `topic_partition`.
///
/// Deviation: Java's `clusterInstance.consumer(...)` here leaves
/// `group.protocol` at its `classic` default; this client implements only the
/// KIP-848 protocol (`consumer-threading.md` §20), so the checking consumer
/// joins with `consumer`.
async fn check_closed_state(
    ctx: &TestContext,
    topic: &str,
    topic_partition: &TopicPartition,
    group_id: &str,
    committed_records: i64,
) {
    // Check that close was graceful with offsets committed and leave group sent.
    // New instance of consumer should be assigned partitions immediately and should see committed offsets.
    let assign_semaphore = Arc::new(AtomicUsize::new(0));
    let mut consumer = create_consumer(ctx, &HashMap::from([("group.id".to_string(), group_id.to_string())]));
    consumer
        .subscribe_with_topics_listener(
            vec![topic.to_string()],
            Arc::new(AssignSemaphoreListener { permits: Arc::clone(&assign_semaphore) }),
        )
        .await
        .expect("subscribe should succeed");

    // Java: `TestUtils.waitForCondition(() -> { poll(100ms); return
    // assignSemaphore.tryAcquire(); }, ...)` with the default 15 s bound.
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        consumer.poll(Duration::from_millis(100)).await.expect("poll should succeed");
        if assign_semaphore
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |permits| permits.checked_sub(1))
            .is_ok()
        {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "Assignment did not complete on time");
    }

    if committed_records > 0 {
        let committed = consumer
            .committed(std::slice::from_ref(topic_partition))
            .await
            .expect("committed should succeed");
        let offset = committed
            .get(topic_partition)
            .expect("a committed offset for the partition")
            .offset();
        assert_eq!(committed_records, offset, "Committed offset does not match expected value.");
    }
    // Java's try-with-resources `close()`.
    consumer.close().await.expect("consumer close should succeed");
}

/// Java's `findCoordinators` (line 376): the ids of the brokers coordinating
/// `groups`, retried until every group has one (default 15 s bound).
///
/// Deviation: Java sends a raw `FindCoordinator`, which answers for any group
/// key whether or not the group exists. The coordinator of a group is the
/// leader of `__consumer_offsets` partition `abs(hash(groupId)) %
/// offsets.topic.num.partitions`; with the class's
/// `offsets.topic.num.partitions=1` that is partition 0 for every group, so
/// its leader (per `describeTopics`) is exactly what `FindCoordinator`
/// returns here. `describeConsumerGroups` is not used: it fails with
/// `GROUP_ID_NOT_FOUND` for a group the coordinator does not know, such as a
/// manual-assignment group that has not committed.
async fn find_coordinators(admin: &dyn Admin, groups: &[String]) -> BTreeSet<i32> {
    const OFFSETS_TOPIC: &str = "__consumer_offsets";
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        let leader = match admin
            .describe_topics_with_topic_names(&[OFFSETS_TOPIC.to_string()])
            .all_topic_names()
        {
            Some(future) => future.get().await.ok().and_then(|topics| {
                topics.get(OFFSETS_TOPIC).and_then(|description| {
                    description
                        .partitions()
                        .iter()
                        .find(|info| info.partition() == 0)
                        .and_then(|info| info.leader().map(|node| node.id()))
                })
            }),
            None => None,
        };
        if let Some(id) = leader {
            // Every group in `groups` maps to this one coordinator.
            return BTreeSet::from([id]);
        }
        assert!(
            std::time::Instant::now() < deadline,
            "Failed to find coordinator for group {groups:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Java's `checkCloseGoodPath` (line 324): closed while the cluster is
/// healthy, the consumer commits and leaves; a new member joins at once and
/// sees the committed offset.
async fn check_close_good_path(
    ctx: &TestContext,
    topic: &str,
    topic_partition: &TopicPartition,
    num_records: usize,
    group_id: &str,
) {
    let consumer =
        create_consumer_and_receive(ctx, topic, topic_partition, group_id, false, num_records, &HashMap::new()).await;
    close_and_validate(consumer, LONG_MAX_VALUE_MS, None, Some(GRACEFUL_CLOSE_TIME_MS)).await;
    check_closed_state(ctx, topic, topic_partition, group_id, num_records as i64).await;
}

/// Java's `checkCloseWithCoordinatorFailure` (line 336): closed while the
/// coordinator is down, the group-managed consumer's close completes after its
/// commit attempt, and the manually assigned consumer's commit succeeds since
/// a broker is available.
async fn check_close_with_coordinator_failure(
    ctx: &TestContext,
    topic: &str,
    topic_partition: &TopicPartition,
    num_records: usize,
    dynamic_group: &str,
    manual_group: &str,
) {
    let dynamic_consumer =
        create_consumer_and_receive(ctx, topic, topic_partition, dynamic_group, false, num_records, &HashMap::new())
            .await;
    let manual_consumer =
        create_consumer_and_receive(ctx, topic, topic_partition, manual_group, true, num_records, &HashMap::new())
            .await;

    let admin = create_admin(ctx);
    let coordinators = find_coordinators(admin.as_ref(), &[dynamic_group.to_string(), manual_group.to_string()]).await;
    admin.close_with_timeout(Duration::from_secs(5)).await;
    for id in coordinators {
        ctx.cluster().shutdown_broker(id).await;
    }

    close_and_validate(dynamic_consumer, LONG_MAX_VALUE_MS, None, Some(GRACEFUL_CLOSE_TIME_MS)).await;
    close_and_validate(manual_consumer, LONG_MAX_VALUE_MS, None, Some(GRACEFUL_CLOSE_TIME_MS)).await;

    restart_dead_brokers(ctx.cluster()).await;
    check_closed_state(ctx, topic, topic_partition, dynamic_group, 0).await;
    check_closed_state(ctx, topic, topic_partition, manual_group, num_records as i64).await;
}

/// Java's `checkCloseWithClusterFailure` (line 355): closed while every broker
/// is down, nothing can be committed; close must honour a short timeout, and
/// with a very large timeout return after the request timeout.
async fn check_close_with_cluster_failure(
    ctx: &TestContext,
    topic: &str,
    topic_partition: &TopicPartition,
    num_records: usize,
    group1: &str,
    group2: &str,
) {
    let consumer1 =
        create_consumer_and_receive(ctx, topic, topic_partition, group1, false, num_records, &HashMap::new()).await;

    // Java also sets session/heartbeat timeouts for the CLASSIC arm only.
    let request_timeout: u64 = 6000;
    let consumer_config = HashMap::from([("request.timeout.ms".to_string(), request_timeout.to_string())]);
    let consumer2 =
        create_consumer_and_receive(ctx, topic, topic_partition, group2, true, num_records, &consumer_config).await;

    let cluster = ctx.cluster();
    for id in cluster.broker_ids() {
        cluster.shutdown_broker(id).await;
    }

    let close_timeout: u64 = 2000;
    close_and_validate(consumer1, close_timeout, None, Some(close_timeout)).await;
    close_and_validate(consumer2, LONG_MAX_VALUE_MS, None, Some(request_timeout)).await;
}

/// Translates `testAsyncClose` (`ConsumerBounceTest.java:306`, body
/// `testClose` :310): the good path, coordinator failure and whole-cluster
/// failure close scenarios, in Java's order on one cluster.
#[tokio::test(flavor = "multi_thread")]
async fn test_async_close() {
    let mut ctx = TestContext::new(bounce_cluster_config()).await;
    let topic = set_up(&mut ctx).await;
    let topic_partition = TopicPartition::new(topic.clone(), 0);
    let num_records: usize = 10;
    send_records(&ctx, &topic_partition, num_records).await;

    check_close_good_path(&ctx, &topic, &topic_partition, num_records, &ctx.group_id("group1")).await;
    check_close_with_coordinator_failure(
        &ctx,
        &topic,
        &topic_partition,
        num_records,
        &ctx.group_id("group2"),
        &ctx.group_id("group3"),
    )
    .await;
    check_close_with_cluster_failure(
        &ctx,
        &topic,
        &topic_partition,
        num_records,
        &ctx.group_id("group4"),
        &ctx.group_id("group5"),
    )
    .await;
}
