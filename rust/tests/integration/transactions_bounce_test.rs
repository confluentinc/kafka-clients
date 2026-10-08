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
//! `kafka/core/src/test/scala/integration/kafka/api/TransactionsBounceTest.scala`
//! (Apache Kafka 4.3.1).
//!
//! # Methods classification
//!
//! ## Translated (KIP-848 / `GroupProtocol.CONSUMER` arm only)
//!
//! - `testWithGroupMetadata` (line 80) → `test_with_group_metadata`
//!
//! ## SKIPped
//!
//! - The `classic` arm of `testWithGroupMetadata`'s
//!   `getTestGroupProtocolParametersAll` — classic protocol
//!   (`consumer-threading.md` §20).
//!
//! # Harness mapping
//!
//! Java's `IntegrationTestHarness` runs four in-JVM brokers plus an isolated
//! KRaft controller; here that is [`ClusterConfig::kraft_dedicated`]`(4, 1)`
//! with the class's `overridingProps` as broker environment. Java pre-allocates
//! fixed ports (`FixedPortTestUtils`) so a bounced broker comes back on the same
//! address; the harness gives that for free (each broker's advertised ports are
//! held by its `BrokerProxy` across a restart). `server.shutdown()` +
//! `awaitShutdown()` / `server.startup()` become
//! `KafkaCluster::shutdown_broker` / `start_broker` (a `docker stop` sends
//! SIGTERM, so `controlled.shutdown.enable=true` still applies).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use confluent_kafka::admin::{Admin, AdminClient, AdminClientConfig};
use confluent_kafka::common::TopicPartition;
use confluent_kafka::common::serialization::{ByteArrayDeserializer, ByteArraySerializer};
use confluent_kafka::consumer::{Consumer, ConsumerConfig, ConsumerRecord, KafkaConsumer};
use confluent_kafka::producer::{KafkaProducer, Producer, ProducerConfig};

use crate::common::cluster_config::ClusterConfig;
use crate::common::kafka_cluster::KafkaCluster;
use crate::common::test_context::TestContext;
use crate::common::test_utils;
use crate::producer_transactions_test::{
    assert_committed_and_get_value, consumer_positions, producer_record_with_expected_transaction_status,
    reset_to_committed_positions, seed_topic_with_numbered_records, send_record,
};

/// Java `consumeRecordTimeout`.
const CONSUME_RECORD_TIMEOUT_MS: u64 = 30000;
/// Java `producerBufferSize`.
const PRODUCER_BUFFER_SIZE: i32 = 65536;
/// Java `serverMessageMaxBytes`.
const SERVER_MESSAGE_MAX_BYTES: i32 = PRODUCER_BUFFER_SIZE / 2;
/// Java `numPartitions`.
const NUM_PARTITIONS: i32 = 3;
/// Java `outputTopic`.
const OUTPUT_TOPIC: &str = "output-topic";
/// Java `inputTopic`.
const INPUT_TOPIC: &str = "input-topic";
/// Java `brokerCount`.
const BROKER_COUNT: u16 = 4;

/// Java's `overridingProps` (`TransactionsBounceTest.scala:46-60`) on a
/// dedicated cluster of `brokerCount` brokers plus one isolated controller.
fn bounce_cluster_config() -> ClusterConfig {
    let props: BTreeMap<String, String> = [
        ("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false".to_string()),
        ("KAFKA_MESSAGE_MAX_BYTES", SERVER_MESSAGE_MAX_BYTES.to_string()),
        ("KAFKA_CONTROLLED_SHUTDOWN_ENABLE", "true".to_string()),
        ("KAFKA_UNCLEAN_LEADER_ELECTION_ENABLE", "false".to_string()),
        ("KAFKA_AUTO_LEADER_REBALANCE_ENABLE", "false".to_string()),
        ("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", "1".to_string()),
        ("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "3".to_string()),
        ("KAFKA_MIN_INSYNC_REPLICAS", "2".to_string()),
        ("KAFKA_GROUP_MIN_SESSION_TIMEOUT_MS", "10".to_string()),
        ("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0".to_string()),
        ("KAFKA_TRANSACTION_STATE_LOG_NUM_PARTITIONS", "1".to_string()),
        ("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "3".to_string()),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v))
    .collect();
    ClusterConfig::kraft_dedicated(BROKER_COUNT, 1).set_server_properties(props)
}

/// Java `createAdminClient()`.
fn create_admin(bootstrap: &str) -> Box<dyn Admin> {
    let props = HashMap::from([("bootstrap.servers".to_string(), bootstrap.to_string())]);
    AdminClient::create(AdminClientConfig::new(&props).expect("valid admin config")).expect("admin client")
}

/// Java `createTopics()` (`TransactionsBounceTest.scala:179-184`):
/// `createTopic(topic, numPartitions, 3, {min.insync.replicas=2})` for both
/// topics, which waits until every partition has a leader.
async fn create_topics(bootstrap: &str, input_topic: &str, output_topic: &str) {
    let admin = create_admin(bootstrap);
    for topic in [input_topic, output_topic] {
        test_utils::create_topic_with_configs(
            admin.as_ref(),
            topic,
            NUM_PARTITIONS,
            3,
            BTreeMap::from([("min.insync.replicas".to_string(), 2.to_string())]),
        )
        .await;
        test_utils::wait_for_partition_leaders(admin.as_ref(), topic, 0..NUM_PARTITIONS).await;
    }
    admin.close().await;
}

/// Java `createTransactionalProducer(transactionalId)`
/// (`TransactionsBounceTest.scala:157-164`) over `IntegrationTestHarness`'s
/// producer defaults (`acks=-1`, byte-array serializers).
fn create_transactional_producer(bootstrap: &str, transactional_id: &str) -> KafkaProducer<Vec<u8>, Vec<u8>> {
    let props = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap.to_string()),
        ("acks".to_string(), "all".to_string()),
        ("batch.size".to_string(), "512".to_string()),
        ("transactional.id".to_string(), transactional_id.to_string()),
        ("enable.idempotence".to_string(), "true".to_string()),
    ]);
    KafkaProducer::new(
        ProducerConfig::new(&props).expect("valid producer config"),
        Box::new(ByteArraySerializer::default()),
        Box::new(ByteArraySerializer::default()),
    )
    .expect("build producer")
}

/// Java `createConsumerAndSubscribe(groupId, topics, readCommitted)`
/// (`TransactionsBounceTest.scala:166-177`) over `IntegrationTestHarness`'s
/// consumer defaults (`auto.offset.reset=earliest`, byte-array deserializers),
/// CONSUMER arm.
async fn create_consumer_and_subscribe(
    bootstrap: &str,
    group_id: &str,
    topics: Vec<String>,
    read_committed: bool,
) -> Box<dyn Consumer<Vec<u8>, Vec<u8>>> {
    let props = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap.to_string()),
        ("group.protocol".to_string(), "consumer".to_string()),
        ("auto.offset.reset".to_string(), "earliest".to_string()),
        ("group.id".to_string(), group_id.to_string()),
        ("enable.auto.commit".to_string(), "false".to_string()),
        (
            "isolation.level".to_string(),
            if read_committed {
                "read_committed"
            } else {
                "read_uncommitted"
            }
            .to_string(),
        ),
    ]);
    let mut consumer = KafkaConsumer::new::<Vec<u8>, Vec<u8>>(
        ConsumerConfig::new(&props).expect("valid consumer config"),
        Box::new(ByteArrayDeserializer::default()),
        Box::new(ByteArrayDeserializer::default()),
    )
    .expect("build consumer");
    consumer.subscribe_with_topics(topics).await.expect("subscribe");
    consumer
}

/// `TestUtils.pollUntilAtLeastNumRecords(consumer, numRecords, waitTimeMs)`
/// (`TestUtils.scala:1184-1196`): poll (100 ms each) until at least
/// `num_records` arrive within `wait_time_ms`.
async fn poll_until_at_least_num_records(
    consumer: &mut Box<dyn Consumer<Vec<u8>, Vec<u8>>>,
    num_records: usize,
    wait_time_ms: u64,
) -> Vec<ConsumerRecord<Vec<u8>, Vec<u8>>> {
    let start = Instant::now();
    let mut records = Vec::new();
    while records.len() < num_records {
        assert!(
            start.elapsed() < Duration::from_millis(wait_time_ms),
            "Consumed {} records before timeout instead of the expected {num_records} records",
            records.len()
        );
        records.extend(consumer.poll(Duration::from_millis(100)).await.expect("poll"));
    }
    records
}

/// Java's `BounceScheduler` (`TransactionsBounceTest.scala:186-205`), a
/// `ShutdownableThread` whose `doWork()` bounces every broker in turn and then
/// waits for a leader on every output partition.
///
/// The thread becomes a future ([`Self::run`]) the test drives concurrently
/// with its copy loop through `tokio::join!`; `shutdown()` becomes
/// [`Self::initiate_shutdown`] plus the `join!` completing. As in
/// `ShutdownableThread.run()`, the flag is checked only between `doWork()`
/// rounds, so a round in progress always finishes (every stopped broker is
/// restarted) before the scheduler exits.
struct BounceScheduler {
    running: AtomicBool,
}

impl BounceScheduler {
    fn new() -> Self {
        Self { running: AtomicBool::new(true) }
    }

    /// `ShutdownableThread.initiateShutdown()`.
    fn initiate_shutdown(&self) {
        self.running.store(false, Ordering::SeqCst);
    }

    /// `ShutdownableThread.run()`: `while (isRunning) doWork()`.
    async fn run(&self, cluster: &KafkaCluster, bootstrap: &str, output_topic: &str) {
        // Java creates a new admin client per round (and never closes it); one
        // client for the scheduler's lifetime is equivalent.
        let admin = create_admin(bootstrap);
        while self.running.load(Ordering::SeqCst) {
            self.do_work(cluster, admin.as_ref(), output_topic).await;
        }
        admin.close().await;
    }

    /// `doWork()`.
    async fn do_work(&self, cluster: &KafkaCluster, admin: &dyn Admin, output_topic: &str) {
        let broker_ids: BTreeSet<i32> = cluster.broker_ids();
        for broker_id in broker_ids {
            cluster.shutdown_broker(broker_id).await;
            tokio::time::sleep(Duration::from_millis(500)).await;
            cluster.start_broker(broker_id).await;
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        // `TestUtils.waitUntilLeaderIsElectedOrChangedWithAdmin(admin, outputTopic, partition)`
        // for every partition.
        test_utils::wait_for_partition_leaders(admin, output_topic, 0..NUM_PARTITIONS).await;
    }
}

/// Java's `testBrokerFailure(commit)` (`TransactionsBounceTest.scala:85-155`)
/// with `testWithGroupMetadata`'s `commit`:
/// `producer.sendOffsetsToTransaction(consumerPositions(consumer), consumer.groupMetadata())`.
async fn test_broker_failure(ctx: &mut TestContext) {
    // basic idea is to seed a topic with 10000 records, and copy it transactionally while bouncing brokers
    // constantly through the period.
    let consumer_group = "myGroup";
    let num_input_records: usize = 10000;
    let input_topic = ctx.topic(INPUT_TOPIC);
    let output_topic = ctx.topic(OUTPUT_TOPIC);
    let bootstrap = ctx.bootstrap_servers().to_string();
    create_topics(&bootstrap, &input_topic, &output_topic).await;

    seed_topic_with_numbered_records(&bootstrap, &input_topic, num_input_records).await;
    let mut consumer =
        create_consumer_and_subscribe(&bootstrap, consumer_group, vec![input_topic.clone()], false).await;
    let producer = create_transactional_producer(&bootstrap, "test-txn");

    producer.init_transactions().await.expect("initTransactions");

    let scheduler = BounceScheduler::new();
    let copy = async {
        let mut num_messages_processed = 0;
        let mut iteration = 0;

        while num_messages_processed < num_input_records {
            let to_read = 200.min(num_input_records - num_messages_processed);
            let records = poll_until_at_least_num_records(&mut consumer, to_read, CONSUME_RECORD_TIMEOUT_MS).await;

            producer.begin_transaction().expect("beginTransaction");
            let should_abort = iteration % 3 == 0;
            for record in &records {
                let key = String::from_utf8(record.key().expect("seeded key").clone()).expect("UTF-8 key");
                let value = String::from_utf8(record.value().expect("seeded value").clone()).expect("UTF-8 value");
                // Java passes an `ErrorLoggingCallback`, which only logs a failed
                // send; a failure also surfaces from `commitTransaction`.
                send_record(
                    &producer,
                    producer_record_with_expected_transaction_status(&output_topic, None, &key, &value, !should_abort),
                )
                .await
                .expect("send");
            }
            // commit(producer, consumerGroup, consumer)
            let offsets = consumer_positions(&mut consumer).await;
            producer
                .send_offsets_to_transaction(offsets, &*consumer.group_metadata())
                .await
                .expect("sendOffsetsToTransaction");

            if should_abort {
                producer.abort_transaction().await.expect("abortTransaction");
                reset_to_committed_positions(&mut consumer).await;
            } else {
                producer.commit_transaction().await.expect("commitTransaction");
                num_messages_processed += records.len();
            }
            iteration += 1;
        }
        // `finally { scheduler.shutdown() }`: the `join!` below awaits the
        // scheduler's current round, as `awaitShutdown()` does.
        scheduler.initiate_shutdown();
    };
    tokio::join!(scheduler.run(ctx.cluster(), &bootstrap, &output_topic), copy);

    let mut verifying_consumer =
        create_consumer_and_subscribe(&bootstrap, "randomGroup", vec![output_topic.clone()], true).await;
    let mut records_by_partition: HashMap<TopicPartition, Vec<i32>> = HashMap::new();
    for record in poll_until_at_least_num_records(&mut verifying_consumer, num_input_records, CONSUME_RECORD_TIMEOUT_MS)
        .await
        .iter()
    {
        let value: i32 = assert_committed_and_get_value(record).parse().expect("numbered value");
        let topic_partition = TopicPartition::new(record.topic().to_string(), record.partition());
        records_by_partition.entry(topic_partition).or_default().push(value);
    }

    let mut output_records = Vec::new();
    for partition_values in records_by_partition.values() {
        let mut sorted = partition_values.clone();
        sorted.sort();
        assert_eq!(*partition_values, sorted, "Out of order messages detected");
        output_records.extend_from_slice(partition_values);
    }

    let record_set: BTreeSet<i32> = output_records.into_iter().collect();
    assert_eq!(num_input_records, record_set.len());

    let expected_values: BTreeSet<i32> = (0..num_input_records as i32).collect();
    assert_eq!(
        expected_values,
        record_set,
        "Missing messages: {:?}",
        expected_values.difference(&record_set).collect::<Vec<_>>()
    );

    verifying_consumer.close().await.expect("verifying consumer close");
    consumer.close().await.expect("consumer close");
    producer.close().await.expect("producer close");
}

/// Translated from `TransactionsBounceTest.testWithGroupMetadata`
/// (`TransactionsBounceTest.scala:80`), the `consumer` group-protocol arm.
#[tokio::test(flavor = "multi_thread")]
async fn test_with_group_metadata() {
    let mut ctx = TestContext::new(bounce_cluster_config()).await;
    test_broker_failure(&mut ctx).await;
}
