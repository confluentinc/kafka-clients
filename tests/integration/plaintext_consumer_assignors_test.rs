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
//! `kafka/core/src/test/scala/integration/kafka/api/PlaintextConsumerAssignorsTest.scala`
//! (Apache Kafka 4.3.1).
//!
//! # Methods classification
//!
//! ## Translated (KIP-848 server-side assignors)
//!
//! - `testRemoteAssignorInvalid` (line 239) → `test_remote_assignor_invalid`
//! - `testRemoteAssignorRange` (line 267) → `test_remote_assignor_range`
//!
//! ## SKIPped (client-side assignors — `consumer-threading.md` §20)
//!
//! - `testRoundRobinAssignment`, `testMultiConsumerRoundRobinAssignor`,
//!   `testMultiConsumerStickyAssignor`,
//!   `testMultiConsumerDefaultAssignorAndVerifyAssignment`,
//!   `testMultiConsumerDefaultAssignor`, `testRebalanceAndRejoin` — all
//!   parameterized over the classic group protocol only
//!   (`getTestGroupProtocolParametersClassicGroupProtocolOnly`) and exercise
//!   client-side `partition.assignment.strategy` assignors, which KIP-848
//!   replaces with server-side assignment.

use std::collections::HashMap;
use std::collections::HashSet;
use std::time::Duration;
use std::time::Instant;

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

use crate::common::cluster_config::kip848_3_broker;
use crate::common::consumer_assignment_poller::BytesConsumer;
use crate::common::test_context::TestContext;
use crate::common::test_utils;

/// `AbstractConsumerTest.brokerCount`.
const BROKER_COUNT: i16 = 3;

/// `TestUtils.pollUntilTrue`'s default `waitTimeMs`
/// (`JTestUtils.DEFAULT_MAX_WAIT_MS`).
const DEFAULT_MAX_WAIT: Duration = Duration::from_millis(15_000);

/// `clusterInstance.admin()` / `createTopic`'s admin client.
fn create_admin(ctx: &TestContext) -> KafkaAdminClient {
    let props = HashMap::from([("bootstrap.servers".to_string(), ctx.bootstrap_servers().to_string())]);
    KafkaAdminClient::new(AdminClientConfig::new(&props).expect("valid admin config")).expect("admin client")
}

/// `createConsumer()` with `AbstractConsumerTest`'s class-level
/// `consumerConfig` (lines 62-67) plus the test's own `setProperty` overrides,
/// on the consumer group protocol (the only parameter these tests run with).
fn create_consumer(ctx: &TestContext, overrides: &[(&str, &str)]) -> BytesConsumer {
    let mut props = HashMap::from([
        ("bootstrap.servers".to_string(), ctx.bootstrap_servers().to_string()),
        ("group.protocol".to_string(), "consumer".to_string()),
        ("client.id".to_string(), "ConsumerTestConsumer".to_string()),
        ("auto.offset.reset".to_string(), "earliest".to_string()),
        ("enable.auto.commit".to_string(), "false".to_string()),
        ("metadata.max.age.ms".to_string(), "100".to_string()),
        ("max.poll.interval.ms".to_string(), "6000".to_string()),
    ]);
    for (k, v) in overrides {
        props.insert((*k).to_string(), (*v).to_string());
    }
    KafkaConsumer::new::<Vec<u8>, Vec<u8>>(
        ConsumerConfig::new(&props).expect("valid consumer config"),
        Box::new(ByteArrayDeserializer),
        Box::new(ByteArrayDeserializer),
    )
    .expect("KafkaConsumer::new should succeed")
}

/// `createProducer()` with `AbstractConsumerTest`'s `producerConfig`
/// (lines 60-61).
fn create_producer(ctx: &TestContext) -> KafkaProducer<Vec<u8>, Vec<u8>> {
    let props = HashMap::from([
        ("bootstrap.servers".to_string(), ctx.bootstrap_servers().to_string()),
        ("acks".to_string(), "all".to_string()),
        ("client.id".to_string(), "ConsumerTestProducer".to_string()),
    ]);
    KafkaProducer::new(
        ProducerConfig::new(&props).expect("valid producer config"),
        Box::new(ByteArraySerializer),
        Box::new(ByteArraySerializer),
    )
    .expect("KafkaProducer::new should succeed")
}

fn current_time_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before Unix epoch")
        .as_millis() as i64
}

/// `AbstractConsumerTest.sendRecords` (line 186) with the default
/// `startingTimestamp = System.currentTimeMillis()` and 1 ms increments.
async fn send_records(producer: &KafkaProducer<Vec<u8>, Vec<u8>>, num_records: usize, tp: &TopicPartition) {
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
        <KafkaProducer<Vec<u8>, Vec<u8>> as Producer<Vec<u8>, Vec<u8>>>::send(producer, record)
            .await
            .expect("send should not fail");
    }
    producer.flush().await.expect("producer flush");
}

/// `AbstractConsumerTest.createTopicAndSendRecords` (line 262).
async fn create_topic_and_send_records(
    admin: &KafkaAdminClient,
    producer: &KafkaProducer<Vec<u8>, Vec<u8>>,
    topic_name: &str,
    num_partitions: i32,
    records_per_partition: usize,
) -> HashSet<TopicPartition> {
    test_utils::create_topic(admin, topic_name, num_partitions, BROKER_COUNT).await;
    let mut parts = HashSet::new();
    for partition in 0..num_partitions {
        let tp = TopicPartition::new(topic_name.to_string(), partition);
        send_records(producer, records_per_partition, &tp).await;
        parts.insert(tp);
    }
    parts
}

/// `AbstractConsumerTest.awaitAssignment` (line 87) through
/// `TestUtils.pollUntilTrue`: `poll(100ms)` until the assignment equals
/// `expected_assignment`, propagating any `poll` error (Java's `assertThrows`
/// in `testRemoteAssignorInvalid` relies on that) and failing after 15 s.
async fn await_assignment(
    consumer: &mut BytesConsumer,
    expected_assignment: &HashSet<TopicPartition>,
) -> Result<(), Error> {
    let deadline = Instant::now() + DEFAULT_MAX_WAIT;
    loop {
        consumer.poll(Duration::from_millis(100)).await?;
        if &consumer.assignment() == expected_assignment {
            return Ok(());
        }
        if Instant::now() >= deadline {
            panic!(
                "Timed out while awaiting expected assignment {expected_assignment:?}. The current assignment is {:?}",
                consumer.assignment()
            );
        }
    }
}

/// Translates `testRemoteAssignorInvalid` (`PlaintextConsumerAssignorsTest.scala:239`).
#[tokio::test(flavor = "multi_thread")]
async fn test_remote_assignor_invalid() {
    let mut ctx = TestContext::new(kip848_3_broker(2)).await;
    // 1 consumer using invalid remote assignor
    let group = ctx.group_id("invalid-assignor-group");
    let mut consumer = create_consumer(&ctx, &[("group.id", &group), ("group.remote.assignor", "invalid")]);

    // create two new topics, each having 2 partitions
    let topic1 = ctx.topic("topic1");
    let admin = create_admin(&ctx);
    let producer = create_producer(&ctx);
    let expected_assignment = create_topic_and_send_records(&admin, &producer, &topic1, 2, 100).await;

    assert_eq!(0, consumer.assignment().len());

    // subscribe to two topics
    consumer
        .subscribe_with_topics(vec![topic1.clone()])
        .await
        .expect("subscribe should succeed");

    let e = await_assignment(&mut consumer, &expected_assignment)
        .await
        .expect_err("awaitAssignment should fail with UnsupportedAssignor");
    let Error::UnsupportedAssignor(e) = e else {
        panic!("expected UnsupportedAssignor, got {e:?}");
    };
    assert!(
        e.message()
            .starts_with("ServerAssignor invalid is not supported. Supported assignors: "),
        "unexpected message: {}",
        e.message()
    );

    let _ = consumer.close().await;
    <KafkaProducer<Vec<u8>, Vec<u8>> as Producer<Vec<u8>, Vec<u8>>>::close(&producer)
        .await
        .expect("producer close");
    ctx.cleanup().await;
}

/// Translates `testRemoteAssignorRange` (`PlaintextConsumerAssignorsTest.scala:267`).
#[tokio::test(flavor = "multi_thread")]
async fn test_remote_assignor_range() {
    let mut ctx = TestContext::new(kip848_3_broker(2)).await;
    // 1 consumer using range assignment
    let group = ctx.group_id("range-group");
    let mut consumer = create_consumer(
        &ctx,
        &[
            ("group.id", &group),
            ("group.remote.assignor", "range"),
            ("max.poll.interval.ms", "30000"),
            ("enable.auto.commit", "false"),
        ],
    );

    // create two new topics, each having 2 partitions
    let topic1 = ctx.topic("topic1");
    let topic2 = ctx.topic("topic2");
    let admin = create_admin(&ctx);
    let producer = create_producer(&ctx);
    let mut expected_assignment = create_topic_and_send_records(&admin, &producer, &topic1, 2, 100).await;
    expected_assignment.extend(create_topic_and_send_records(&admin, &producer, &topic2, 2, 100).await);

    assert_eq!(0, consumer.assignment().len());

    // subscribe to two topics
    consumer
        .subscribe_with_topics(vec![topic1.clone(), topic2.clone()])
        .await
        .expect("subscribe should succeed");
    await_assignment(&mut consumer, &expected_assignment)
        .await
        .expect("poll should succeed");

    // add one more topic with 2 partitions
    let topic3 = ctx.topic("topic3");
    let additional_assignment = create_topic_and_send_records(&admin, &producer, &topic3, 2, 100).await;

    let new_expected_assignment: HashSet<TopicPartition> =
        expected_assignment.union(&additional_assignment).cloned().collect();
    consumer
        .subscribe_with_topics(vec![topic1.clone(), topic2.clone(), topic3.clone()])
        .await
        .expect("subscribe should succeed");
    await_assignment(&mut consumer, &new_expected_assignment)
        .await
        .expect("poll should succeed");

    // remove the topic we just added
    consumer
        .subscribe_with_topics(vec![topic1.clone(), topic2.clone()])
        .await
        .expect("subscribe should succeed");
    await_assignment(&mut consumer, &expected_assignment)
        .await
        .expect("poll should succeed");

    consumer.unsubscribe().await.expect("unsubscribe should succeed");
    assert_eq!(0, consumer.assignment().len());

    consumer.close().await.expect("consumer close");
    <KafkaProducer<Vec<u8>, Vec<u8>> as Producer<Vec<u8>, Vec<u8>>>::close(&producer)
        .await
        .expect("producer close");
    ctx.cleanup().await;
}
