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
//! The translated tests run as multilanguage scenarios: each one runs against
//! the native Rust consumer and, through the gRPC backends, the Python (sync
//! and asyncio) and C consumers ([`multilanguage_consumer_test!`]); each name
//! below gains the macro's backend suffix (`__rust`, `__grpc_python`,
//! `__grpc_python_async`, `__grpc_c`). The setup producer and admin client
//! are native fixtures; only the consumer is under test.
//!
//! # Methods classification
//!
//! ## Translated (KIP-848 server-side assignors)
//!
//! - `testRemoteAssignorInvalid` → `test_remote_assignor_invalid`
//! - `testRemoteAssignorRange` → `test_remote_assignor_range`
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

use confluent_kafka::admin::Admin;
use confluent_kafka::admin::AdminClient;
use confluent_kafka::admin::AdminClientConfig;
use confluent_kafka::common::Error;
use confluent_kafka::common::TopicPartition;
use confluent_kafka::common::serialization::ByteArraySerializer;
use confluent_kafka::producer::KafkaProducer;
use confluent_kafka::producer::Producer;
use confluent_kafka::producer::ProducerConfig;
use confluent_kafka::producer::ProducerRecord;

use crate::common::backend_factory::ConsumerBackendFactory;
use crate::common::cluster_config::kip848_3_broker;
use crate::common::consumer_assignment_poller::BytesConsumer;
use crate::common::test_context::TestContext;
use crate::common::test_utils;
use crate::multilanguage_consumer_test;

/// `AbstractConsumerTest.brokerCount`.
const BROKER_COUNT: i16 = 3;

/// `TestUtils.pollUntilTrue`'s default `waitTimeMs`
/// (`JTestUtils.DEFAULT_MAX_WAIT_MS`).
const DEFAULT_MAX_WAIT: Duration = Duration::from_millis(15_000);

/// `clusterInstance.admin()` / `createTopic`'s admin client.
fn create_admin(ctx: &TestContext) -> Box<dyn Admin> {
    let props = HashMap::from([("bootstrap.servers".to_string(), ctx.bootstrap_servers().to_string())]);
    AdminClient::create(AdminClientConfig::new(&props).expect("valid admin config")).expect("admin client")
}

/// `createConsumer()` with the class-level `consumerConfig` settings of
/// `kafka.api.AbstractConsumerTest` plus the test's own `setProperty` overrides,
/// on the consumer group protocol (the only parameter these tests run with),
/// built by `factory` on the bootstrap its backend reaches.
async fn create_consumer<F: ConsumerBackendFactory>(
    ctx: &TestContext,
    factory: &F,
    overrides: &[(&str, &str)],
) -> BytesConsumer {
    let mut props = HashMap::from([
        (
            "bootstrap.servers".to_string(),
            ctx.protocol_bootstrap_servers_for(factory.needs_container_bootstrap())
                .to_string(),
        ),
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
    ctx.apply_security(&mut props);
    factory
        .create(props)
        .await
        .unwrap_or_else(|e| panic!("{} backend: create consumer: {e}", factory.name()))
}

/// `createProducer()` with the `producerConfig` settings of
/// `kafka.api.AbstractConsumerTest`.
fn create_producer(ctx: &TestContext) -> KafkaProducer<Vec<u8>, Vec<u8>> {
    let props = HashMap::from([
        ("bootstrap.servers".to_string(), ctx.bootstrap_servers().to_string()),
        ("acks".to_string(), "all".to_string()),
        ("client.id".to_string(), "ConsumerTestProducer".to_string()),
    ]);
    KafkaProducer::new(
        ProducerConfig::new(&props).expect("valid producer config"),
        ByteArraySerializer::default(),
        ByteArraySerializer::default(),
    )
    .expect("KafkaProducer::new should succeed")
}

fn current_time_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before Unix epoch")
        .as_millis() as i64
}

/// `kafka.api.AbstractConsumerTest#sendRecords` with the default
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

/// `kafka.api.AbstractConsumerTest#createTopicAndSendRecords`.
async fn create_topic_and_send_records(
    admin: &dyn Admin,
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

/// `kafka.api.AbstractConsumerTest#awaitAssignment` through
/// `TestUtils.pollUntilTrue`: `poll(100ms)` until the assignment equals
/// `expected_assignment`, propagating any `poll` error (Java's `assertThrows`
/// in `testRemoteAssignorInvalid` relies on that) and failing after 15 s.
async fn await_assignment(
    consumer: &mut BytesConsumer,
    expected_assignment: &HashSet<TopicPartition>,
    backend: &str,
) -> Result<(), Error> {
    let deadline = Instant::now() + DEFAULT_MAX_WAIT;
    loop {
        consumer.poll(Duration::from_millis(100)).await?;
        if &consumer.assignment() == expected_assignment {
            return Ok(());
        }
        if Instant::now() >= deadline {
            panic!(
                "{backend} backend: Timed out while awaiting expected assignment {expected_assignment:?}. The current assignment is {:?}",
                consumer.assignment()
            );
        }
    }
}

/// Translates `kafka.api.PlaintextConsumerAssignorsTest#testRemoteAssignorInvalid`.
async fn remote_assignor_invalid<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let backend = factory.name();
    // 1 consumer using invalid remote assignor
    let group = ctx.group_id("invalid-assignor-group");
    let mut consumer =
        create_consumer(ctx, factory, &[("group.id", &group), ("group.remote.assignor", "invalid")]).await;

    // create two new topics, each having 2 partitions
    let topic1 = ctx.topic("topic1");
    let admin = create_admin(ctx);
    let producer = create_producer(ctx);
    let expected_assignment = create_topic_and_send_records(admin.as_ref(), &producer, &topic1, 2, 100).await;

    assert_eq!(0, consumer.assignment().len(), "initial assignment ({backend} backend)");

    // subscribe to two topics
    consumer
        .subscribe_with_topics(vec![topic1.clone()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: subscribe should succeed: {e}"));

    let e = await_assignment(&mut consumer, &expected_assignment, backend)
        .await
        .expect_err("awaitAssignment should fail with UnsupportedAssignor");
    let Error::UnsupportedAssignor(e) = e else {
        panic!("{backend} backend: expected UnsupportedAssignor, got {e:?}");
    };
    assert!(
        e.message()
            .starts_with("ServerAssignor invalid is not supported. Supported assignors: "),
        "unexpected message: {} ({backend} backend)",
        e.message()
    );

    let _ = consumer.close().await;
    <KafkaProducer<Vec<u8>, Vec<u8>> as Producer<Vec<u8>, Vec<u8>>>::close(&producer)
        .await
        .expect("producer close");
    ctx.cleanup().await;
}

multilanguage_consumer_test!(test_remote_assignor_invalid, remote_assignor_invalid, kip848_3_broker(2));

/// Translates `kafka.api.PlaintextConsumerAssignorsTest#testRemoteAssignorRange`.
async fn remote_assignor_range<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let backend = factory.name();
    // 1 consumer using range assignment
    let group = ctx.group_id("range-group");
    let mut consumer = create_consumer(
        ctx,
        factory,
        &[
            ("group.id", &group),
            ("group.remote.assignor", "range"),
            ("max.poll.interval.ms", "30000"),
            ("enable.auto.commit", "false"),
        ],
    )
    .await;

    // create two new topics, each having 2 partitions
    let topic1 = ctx.topic("topic1");
    let topic2 = ctx.topic("topic2");
    let admin = create_admin(ctx);
    let producer = create_producer(ctx);
    let mut expected_assignment = create_topic_and_send_records(admin.as_ref(), &producer, &topic1, 2, 100).await;
    expected_assignment.extend(create_topic_and_send_records(admin.as_ref(), &producer, &topic2, 2, 100).await);

    assert_eq!(0, consumer.assignment().len(), "initial assignment ({backend} backend)");

    // subscribe to two topics
    consumer
        .subscribe_with_topics(vec![topic1.clone(), topic2.clone()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: subscribe should succeed: {e}"));
    await_assignment(&mut consumer, &expected_assignment, backend)
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: poll should succeed: {e}"));

    // add one more topic with 2 partitions
    let topic3 = ctx.topic("topic3");
    let additional_assignment = create_topic_and_send_records(admin.as_ref(), &producer, &topic3, 2, 100).await;

    let new_expected_assignment: HashSet<TopicPartition> =
        expected_assignment.union(&additional_assignment).cloned().collect();
    consumer
        .subscribe_with_topics(vec![topic1.clone(), topic2.clone(), topic3.clone()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: subscribe should succeed: {e}"));
    await_assignment(&mut consumer, &new_expected_assignment, backend)
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: poll should succeed: {e}"));

    // remove the topic we just added
    consumer
        .subscribe_with_topics(vec![topic1.clone(), topic2.clone()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: subscribe should succeed: {e}"));
    await_assignment(&mut consumer, &expected_assignment, backend)
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: poll should succeed: {e}"));

    consumer
        .unsubscribe()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: unsubscribe should succeed: {e}"));
    assert_eq!(
        0,
        consumer.assignment().len(),
        "assignment after unsubscribe ({backend} backend)"
    );

    consumer
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer close: {e}"));
    <KafkaProducer<Vec<u8>, Vec<u8>> as Producer<Vec<u8>, Vec<u8>>>::close(&producer)
        .await
        .expect("producer close");
    ctx.cleanup().await;
}

multilanguage_consumer_test!(test_remote_assignor_range, remote_assignor_range, kip848_3_broker(2));
