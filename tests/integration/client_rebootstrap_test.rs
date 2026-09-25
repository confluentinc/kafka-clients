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
//! `kafka/clients/clients-integration-tests/src/test/java/org/apache/kafka/clients/ClientRebootstrapTest.java`
//! (Apache Kafka 4.3.1).
//!
//! Every Java test is a `@ClusterTest(brokers = 2, types = {Type.KRAFT})`: two
//! broker-only nodes plus one isolated controller, which here is
//! [`ClusterConfig::kraft_dedicated`]`(2, 1)`. The clients bootstrap against
//! both brokers; each test stops broker 0 before the client bootstraps (so the
//! client only learns about broker 1), then stops broker 1 and restarts
//! broker 0. Only `metadata.recovery.strategy=rebootstrap` (the default) lets
//! the client find broker 0 again through the bootstrap list.
//!
//! # Methods classification
//!
//! ## Translated
//!
//! - `testProducerRebootstrap` (line 116) → `test_producer_rebootstrap`
//! - `testProducerRebootstrapDisabled` (line 151) → `test_producer_rebootstrap_disabled`
//! - `testConsumerRebootstrap` (line 231) → `test_consumer_rebootstrap`
//! - `testConsumerRebootstrapDisabled` (line 293) → `test_consumer_rebootstrap_disabled`
//!
//! ## SKIPped
//!
//! - `testClassicConsumerRebootstrap` / `testClassicConsumerRebootstrapDisabled`
//!   — classic-protocol arms (`consumer-threading.md` §20).
//! - `testAdminRebootstrap` / `testAdminRebootstrapDisabled` — not in this
//!   phase's scope (test-parity PLAN Phase 19 covers the producer and consumer).

use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

use confluent_kafka::admin::{Admin, AdminClientConfig, KafkaAdminClient};
use confluent_kafka::common::Error;
use confluent_kafka::common::TopicPartition;
use confluent_kafka::common::serialization::{ByteArrayDeserializer, ByteArraySerializer};
use confluent_kafka::consumer::{Consumer, ConsumerConfig, KafkaConsumer};
use confluent_kafka::producer::{KafkaProducer, Producer, ProducerConfig, ProducerRecord};

use crate::common::cluster_config::ClusterConfig;
use crate::common::test_context::TestContext;
use crate::common::test_utils::create_topic;

/// Java `TOPIC`.
const TOPIC: &str = "topic";
/// Java `PARTITIONS`.
const PARTITIONS: i32 = 1;
/// Java `REPLICAS` (also the broker count).
const REPLICAS: i16 = 2;
const BROKER0: i32 = 0;
const BROKER1: i32 = 1;

/// The `@ClusterTest` of the producer and consumer tests: two brokers, one
/// isolated controller, unclean leader election enabled (broker 0 comes back
/// with a stale or empty log while broker 1, the only ISR member, is down) and
/// `offsets.topic.replication.factor=2`.
fn rebootstrap_cluster_config() -> ClusterConfig {
    ClusterConfig::kraft_dedicated(REPLICAS as u16, 1).set_server_properties(BTreeMap::from([
        ("KAFKA_UNCLEAN_LEADER_ELECTION_ENABLE".to_string(), "true".to_string()),
        ("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR".to_string(), "2".to_string()),
    ]))
}

/// Java `clusterInstance.admin()`.
fn admin(bootstrap: &str) -> KafkaAdminClient {
    let props = HashMap::from([("bootstrap.servers".to_string(), bootstrap.to_string())]);
    KafkaAdminClient::new(AdminClientConfig::new(&props).expect("valid admin config")).expect("admin client")
}

/// Java `clusterInstance.producer(overrides)` — byte-array serializers,
/// bootstrapping against every broker.
fn producer(bootstrap: &str, overrides: &[(&str, &str)]) -> KafkaProducer<Vec<u8>, Vec<u8>> {
    let mut props = HashMap::from([("bootstrap.servers".to_string(), bootstrap.to_string())]);
    props.extend(overrides.iter().map(|(k, v)| (k.to_string(), v.to_string())));
    let config = ProducerConfig::new(&props).expect("valid producer config");
    KafkaProducer::new(config, Box::new(ByteArraySerializer), Box::new(ByteArraySerializer)).expect("build producer")
}

/// Java `clusterInstance.consumer(overrides)` — byte-array deserializers,
/// bootstrapping against every broker. The Java arm under test sets
/// `group.protocol=consumer`.
fn consumer(bootstrap: &str, overrides: &[(&str, &str)]) -> Box<dyn Consumer<Vec<u8>, Vec<u8>>> {
    let mut props = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap.to_string()),
        ("group.protocol".to_string(), "consumer".to_string()),
    ]);
    props.extend(overrides.iter().map(|(k, v)| (k.to_string(), v.to_string())));
    KafkaConsumer::new::<Vec<u8>, Vec<u8>>(
        ConsumerConfig::new(&props).expect("valid consumer config"),
        Box::new(ByteArrayDeserializer),
        Box::new(ByteArrayDeserializer),
    )
    .expect("build consumer")
}

fn record(topic: &str, value: &str) -> ProducerRecord<Vec<u8>, Vec<u8>> {
    ProducerRecord::new(topic.to_string(), Some(value.as_bytes().to_vec()))
}

/// `producer.send(record).get()`, returning the record's offset.
async fn send_and_get_offset(producer: &KafkaProducer<Vec<u8>, Vec<u8>>, topic: &str, value: &str) -> i64 {
    <KafkaProducer<Vec<u8>, Vec<u8>> as Producer<Vec<u8>, Vec<u8>>>::send(producer, record(topic, value))
        .await
        .expect("send")
        .get()
        .await
        .expect("record acknowledged")
        .offset()
}

/// Java `TestUtils.waitForCondition(() -> consumer.poll(Duration.ofMillis(100)).count() == 1,
/// 10 * 1000, "Failed to poll data.")`.
async fn wait_for_one_polled_record(consumer: &mut dyn Consumer<Vec<u8>, Vec<u8>>) {
    let deadline = Instant::now() + Duration::from_millis(10 * 1000);
    loop {
        let count = consumer.poll(Duration::from_millis(100)).await.expect("poll").count();
        if count == 1 {
            return;
        }
        assert!(Instant::now() < deadline, "Failed to poll data.");
    }
}

/// Translated from `ClientRebootstrapTest.testProducerRebootstrap`
/// (`ClientRebootstrapTest.java:116`).
#[tokio::test(flavor = "multi_thread")]
async fn test_producer_rebootstrap() {
    let mut ctx = TestContext::new(rebootstrap_cluster_config()).await;
    let topic = ctx.topic(TOPIC);
    let bootstrap = ctx.bootstrap_servers().to_string();
    {
        let admin = admin(&bootstrap);
        create_topic(&admin, &topic, PARTITIONS, REPLICAS).await;
        admin.close().await;
    }

    // It's ok to shut the leader down, cause the reelection is small enough to the producer timeout.
    ctx.cluster().shutdown_broker(BROKER0).await;

    let producer = producer(&bootstrap, &[]);
    // Only the broker 1 is available for the producer during the bootstrap.
    assert_eq!(0, send_and_get_offset(&producer, &topic, "value 0").await);

    ctx.cluster().shutdown_broker(BROKER1).await;
    ctx.cluster().start_broker(BROKER0).await;

    // Current broker 1 is offline.
    // However, the broker 0 from the bootstrap list is online.
    // Should be able to produce records.
    assert_eq!(0, send_and_get_offset(&producer, &topic, "value 1").await);
    producer.close().await.expect("producer close");
}

/// Translated from `ClientRebootstrapTest.testProducerRebootstrapDisabled`
/// (`ClientRebootstrapTest.java:151`).
#[tokio::test(flavor = "multi_thread")]
async fn test_producer_rebootstrap_disabled() {
    let mut ctx = TestContext::new(rebootstrap_cluster_config()).await;
    let topic = ctx.topic(TOPIC);
    let bootstrap = ctx.bootstrap_servers().to_string();
    {
        let admin = admin(&bootstrap);
        create_topic(&admin, &topic, PARTITIONS, REPLICAS).await;
        admin.close().await;
    }

    // It's ok to shut the leader down, cause the reelection is small enough to the producer timeout.
    ctx.cluster().shutdown_broker(BROKER0).await;

    let producer = producer(&bootstrap, &[("metadata.recovery.strategy", "none")]);

    // Only the broker 1 is available for the producer during the bootstrap.
    assert_eq!(0, send_and_get_offset(&producer, &topic, "value 0").await);

    ctx.cluster().shutdown_broker(BROKER1).await;
    ctx.cluster().start_broker(BROKER0).await;

    // The broker 1, originally cached during the bootstrap, is offline.
    // As a result, the producer will throw a TimeoutException when trying to send a message.
    //
    // Java's `Future.get(5, SECONDS)` throws `java.util.concurrent.TimeoutException`,
    // whose Rust counterpart is `Error::LocalTimeout` (see
    // `FutureRecordMetadata::get_with_timeout`).
    let future =
        <KafkaProducer<Vec<u8>, Vec<u8>> as Producer<Vec<u8>, Vec<u8>>>::send(&producer, record(&topic, "value 1"))
            .await
            .expect("send");
    match future.get_with_timeout(Duration::from_secs(5)).await {
        Err(err @ Error::LocalTimeout(_)) => assert_eq!(
            err.to_string(),
            Error::local_timeout("Timeout after waiting for 5000 ms.").to_string()
        ),
        other => panic!("expected a local TimeoutException, got {other:?}"),
    }
    // Since the brokers cached during the bootstrap are offline, the producer needs to wait the default timeout for other threads.
    producer.close_with_timeout(Duration::ZERO).await.expect("producer close");
}

/// Java `consumerRebootstrap(clusterInstance, GroupProtocol.CONSUMER)`
/// (`ClientRebootstrapTest.java:178`).
async fn consumer_rebootstrap(ctx: &mut TestContext) {
    let topic = ctx.topic(TOPIC);
    let bootstrap = ctx.bootstrap_servers().to_string();
    {
        let admin = admin(&bootstrap);
        create_topic(&admin, &topic, PARTITIONS, REPLICAS).await;
        admin.close().await;
    }
    let partitions = vec![TopicPartition::new(topic.clone(), 0)];

    {
        let producer = producer(&bootstrap, &[("acks", "-1")]);
        assert_eq!(0, send_and_get_offset(&producer, &topic, "value 0").await);
        producer.close().await.expect("producer close");
    }

    ctx.cluster().shutdown_broker(BROKER0).await;

    let mut consumer = consumer(&bootstrap, &[]);
    // Only the server 1 is available for the consumer during the bootstrap.
    consumer.assign(partitions.clone()).await.expect("assign");
    consumer.seek_to_beginning(&partitions).await.expect("seek to beginning");
    wait_for_one_polled_record(consumer.as_mut()).await;

    // Bring back the server 0 and shut down 1.
    ctx.cluster().shutdown_broker(BROKER1).await;
    ctx.cluster().start_broker(BROKER0).await;

    {
        let producer = producer(&bootstrap, &[("acks", "-1")]);
        assert_eq!(1, send_and_get_offset(&producer, &topic, "value 1").await);
        producer.close().await.expect("producer close");
    }

    // The server 1 originally cached during the bootstrap, is offline.
    // However, the server 0 from the bootstrap list is online.
    wait_for_one_polled_record(consumer.as_mut()).await;
    consumer.close().await.expect("consumer close");
}

/// Translated from `ClientRebootstrapTest.testConsumerRebootstrap`
/// (`ClientRebootstrapTest.java:231`), the `GroupProtocol.CONSUMER` arm.
#[tokio::test(flavor = "multi_thread")]
async fn test_consumer_rebootstrap() {
    let mut ctx = TestContext::new(rebootstrap_cluster_config()).await;
    consumer_rebootstrap(&mut ctx).await;
}

/// Java `consumerRebootstrapDisabled(clusterInstance, GroupProtocol.CONSUMER)`
/// (`ClientRebootstrapTest.java:235`).
async fn consumer_rebootstrap_disabled(ctx: &mut TestContext) {
    let topic = ctx.topic(TOPIC);
    let bootstrap = ctx.bootstrap_servers().to_string();
    {
        let admin = admin(&bootstrap);
        create_topic(&admin, &topic, PARTITIONS, REPLICAS).await;
        admin.close().await;
    }
    let tp = TopicPartition::new(topic.clone(), 0);

    {
        let producer = producer(&bootstrap, &[("acks", "-1")]);
        assert_eq!(0, send_and_get_offset(&producer, &topic, "value 0").await);
        producer.close().await.expect("producer close");
    }

    ctx.cluster().shutdown_broker(BROKER0).await;

    let mut consumer = consumer(&bootstrap, &[("metadata.recovery.strategy", "none")]);
    // Only the server 1 is available for the consumer during the bootstrap.
    consumer.assign(vec![tp.clone()]).await.expect("assign");
    consumer.seek_to_beginning(&[tp]).await.expect("seek to beginning");
    wait_for_one_polled_record(consumer.as_mut()).await;

    // Bring back the server 0 and shut down 1.
    ctx.cluster().shutdown_broker(BROKER1).await;
    ctx.cluster().start_broker(BROKER0).await;

    {
        let producer = producer(&bootstrap, &[("acks", "-1")]);
        assert_eq!(1, send_and_get_offset(&producer, &topic, "value 1").await);
        producer.close().await.expect("producer close");
    }

    // The server 1 originally cached during the bootstrap, is offline.
    // However, the server 0 from the bootstrap list is online.
    assert_eq!(0, consumer.poll(Duration::from_millis(100)).await.expect("poll").count());
    consumer.close().await.expect("consumer close");
}

/// Translated from `ClientRebootstrapTest.testConsumerRebootstrapDisabled`
/// (`ClientRebootstrapTest.java:293`), the `GroupProtocol.CONSUMER` arm.
#[tokio::test(flavor = "multi_thread")]
async fn test_consumer_rebootstrap_disabled() {
    let mut ctx = TestContext::new(rebootstrap_cluster_config()).await;
    consumer_rebootstrap_disabled(&mut ctx).await;
}
