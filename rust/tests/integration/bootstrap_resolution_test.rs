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

//! KIP-909 (`bootstrap.resolve.timeout.ms`) through the public API of all three
//! clients.
//!
//! Java has no integration test for KIP-909: its coverage is the unit tests in
//! `KafkaProducerTest`, `KafkaConsumerTest` and `KafkaAdminClientTest`, which
//! the Rust unit tests translate. This file is Rust-only (Milestone 16 Phase 2)
//! and covers what those cannot:
//!
//! - **Unresolvable bootstrap host, positive timeout:** every client is created,
//!   then fails with Java's `BootstrapResolutionException` message once the
//!   budget runs out, and keeps failing. These need no broker, but they run the
//!   production clients through the published crate surface.
//! - **Resolvable bootstrap host, positive timeout:** against a real broker, the
//!   asynchronous path bootstraps the client, which then works normally (a
//!   produce, a consume and an admin call).

use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

use confluent_kafka::admin::{AdminClient, AdminClientConfig};
use confluent_kafka::common::Error;
use confluent_kafka::common::serialization::{ByteArrayDeserializer, ByteArraySerializer};
use confluent_kafka::consumer::{ConsumerConfig, KafkaConsumer};
use confluent_kafka::producer::{KafkaProducer, Producer, ProducerConfig, ProducerRecord};

use crate::common::cluster_config::ClusterConfig;
use crate::common::test_context::TestContext;
use crate::common::test_utils::create_topic;

/// An RFC 6761 reserved `.invalid` name, which never resolves.
const UNRESOLVABLE: &str = "unresolvable.invalid:9092";
/// The budget the unresolvable-host tests give the resolution.
const TIMEOUT_MS: &str = "3000";
/// Java's message for that budget (`NetworkClient.checkBootstrapTimeout`).
const EXPECTED: &str = "Failed to resolve bootstrap servers after 3000ms. \
                        Please check your bootstrap.servers configuration and DNS settings.";
/// How long a test waits for the failure before giving up (Java's 15 s).
const MAX_WAIT: Duration = Duration::from_secs(15);

fn assert_bootstrap_resolution_error<T: std::fmt::Debug>(result: Result<T, Error>) {
    match result {
        Err(Error::BootstrapResolution(e)) => assert_eq!(e.message(), EXPECTED),
        other => panic!("expected the bootstrap resolution failure, got {other:?}"),
    }
}

/// The producer's `partitions_for` waits on metadata, which the failure
/// wakes; it and every later call return `BootstrapResolutionError`.
#[tokio::test(flavor = "multi_thread")]
async fn test_producer_unresolvable_bootstrap_with_positive_timeout() {
    let props = HashMap::from([
        (ProducerConfig::BOOTSTRAP_SERVERS_CONFIG.to_string(), UNRESOLVABLE.to_string()),
        (
            ProducerConfig::BOOTSTRAP_RESOLVE_TIMEOUT_MS_CONFIG.to_string(),
            TIMEOUT_MS.to_string(),
        ),
    ]);
    let producer = KafkaProducer::<Vec<u8>, Vec<u8>>::new(
        ProducerConfig::new(&props).expect("valid config"),
        Box::new(ByteArraySerializer::default()),
        Box::new(ByteArraySerializer::default()),
    )
    .expect("a positive timeout defers resolution, so construction succeeds");

    let first = tokio::time::timeout(MAX_WAIT, producer.partitions_for("topic"))
        .await
        .expect("the bootstrap failure must end the metadata wait");
    assert_bootstrap_resolution_error(first);
    assert_bootstrap_resolution_error(producer.partitions_for("topic").await);
    producer.close_with_timeout(Duration::ZERO).await.expect("close");
}

/// The consumer's `poll` and every later call return `BootstrapResolutionError`;
/// `close` still succeeds.
#[tokio::test(flavor = "multi_thread")]
async fn test_consumer_unresolvable_bootstrap_with_positive_timeout() {
    let props = HashMap::from([
        (ConsumerConfig::BOOTSTRAP_SERVERS_CONFIG.to_string(), UNRESOLVABLE.to_string()),
        (
            ConsumerConfig::BOOTSTRAP_RESOLVE_TIMEOUT_MS_CONFIG.to_string(),
            TIMEOUT_MS.to_string(),
        ),
        (ConsumerConfig::GROUP_PROTOCOL_CONFIG.to_string(), "consumer".to_string()),
        (
            ConsumerConfig::GROUP_ID_CONFIG.to_string(),
            "bootstrap-resolution-test".to_string(),
        ),
    ]);
    let mut consumer = KafkaConsumer::new::<Vec<u8>, Vec<u8>>(
        ConsumerConfig::new(&props).expect("valid config"),
        Box::new(ByteArrayDeserializer::default()),
        Box::new(ByteArrayDeserializer::default()),
    )
    .expect("a positive timeout defers resolution, so construction succeeds");
    consumer
        .subscribe_with_topics(vec!["topic".to_string()])
        .await
        .expect("subscribe");

    let start = Instant::now();
    let first = loop {
        assert!(start.elapsed() < MAX_WAIT, "no bootstrap failure within {MAX_WAIT:?}");
        match consumer.poll(Duration::from_millis(100)).await {
            Ok(_) => {},
            Err(e) => break Err::<(), _>(e),
        }
    };
    assert_bootstrap_resolution_error(first);
    assert_bootstrap_resolution_error(consumer.poll(Duration::from_millis(100)).await);
    assert_bootstrap_resolution_error(consumer.list_topics().await);
    consumer.close().await.expect("close");
}

/// The admin client fails the call in flight when the budget runs out and
/// every later call with the same error.
#[tokio::test(flavor = "multi_thread")]
async fn test_admin_unresolvable_bootstrap_with_positive_timeout() {
    let props = HashMap::from([
        (
            AdminClientConfig::BOOTSTRAP_SERVERS_CONFIG.to_string(),
            UNRESOLVABLE.to_string(),
        ),
        (
            AdminClientConfig::BOOTSTRAP_RESOLVE_TIMEOUT_MS_CONFIG.to_string(),
            TIMEOUT_MS.to_string(),
        ),
    ]);
    let admin = AdminClient::create(AdminClientConfig::new(&props).expect("valid config"))
        .expect("a positive timeout defers resolution, so creation succeeds");

    let first = tokio::time::timeout(MAX_WAIT, admin.list_topics().names().get())
        .await
        .expect("the bootstrap failure must fail the call in flight");
    assert_bootstrap_resolution_error(first);
    assert_bootstrap_resolution_error(admin.list_topics().names().get().await);
    admin.close().await;
}

/// KIP-848 consumer groups need the `consumer` rebalance protocol on the broker
/// (see `consumer_test.rs`).
fn cluster_config_with_kip848() -> ClusterConfig {
    ClusterConfig::with_properties(BTreeMap::from([(
        "KAFKA_GROUP_COORDINATOR_REBALANCE_PROTOCOLS".to_string(),
        "classic,consumer".to_string(),
    )]))
}

/// With a positive timeout and a resolvable bootstrap list, the asynchronous
/// path bootstraps every client and they work as in mode 0: the admin client
/// creates a topic, the producer writes to it and the consumer reads it back.
#[tokio::test(flavor = "multi_thread")]
async fn test_clients_bootstrap_asynchronously_with_positive_timeout() {
    let mut ctx = TestContext::new(cluster_config_with_kip848()).await;
    let topic = ctx.topic("bootstrap_resolve_async");
    let async_bootstrap = |props: &mut HashMap<String, String>| {
        ctx.configure(props);
        props.insert("bootstrap.resolve.timeout.ms".to_string(), "30000".to_string());
    };

    let mut admin_props = HashMap::new();
    async_bootstrap(&mut admin_props);
    let admin = AdminClient::create(AdminClientConfig::new(&admin_props).expect("valid config")).expect("admin");
    create_topic(admin.as_ref(), &topic, 1, 1).await;
    let names = admin.list_topics().names().get().await.expect("list_topics");
    assert!(names.contains(&topic), "{names:?}");

    let mut producer_props = HashMap::new();
    async_bootstrap(&mut producer_props);
    let producer = KafkaProducer::<Vec<u8>, Vec<u8>>::new(
        ProducerConfig::new(&producer_props).expect("valid config"),
        Box::new(ByteArraySerializer::default()),
        Box::new(ByteArraySerializer::default()),
    )
    .expect("producer");
    let record = ProducerRecord::with_key(topic.clone(), Some(b"key".to_vec()), Some(b"value".to_vec()));
    <KafkaProducer<Vec<u8>, Vec<u8>> as Producer<Vec<u8>, Vec<u8>>>::send(&producer, record)
        .await
        .expect("send")
        .get_with_timeout(Duration::from_secs(30))
        .await
        .expect("delivery");
    producer.close().await.expect("producer close");

    let mut consumer_props = HashMap::new();
    async_bootstrap(&mut consumer_props);
    consumer_props.insert("group.protocol".to_string(), "consumer".to_string());
    consumer_props.insert("group.id".to_string(), ctx.group_id("bootstrap_resolve_async"));
    consumer_props.insert("auto.offset.reset".to_string(), "earliest".to_string());
    let mut consumer = KafkaConsumer::new::<Vec<u8>, Vec<u8>>(
        ConsumerConfig::new(&consumer_props).expect("valid config"),
        Box::new(ByteArrayDeserializer::default()),
        Box::new(ByteArrayDeserializer::default()),
    )
    .expect("consumer");
    consumer.subscribe_with_topics(vec![topic.clone()]).await.expect("subscribe");
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut values = Vec::new();
    while values.is_empty() {
        assert!(Instant::now() < deadline, "no record consumed within 60 s");
        let records = consumer.poll(Duration::from_millis(500)).await.expect("poll");
        values.extend((&records).into_iter().map(|r| r.value().cloned()));
    }
    assert_eq!(values, vec![Some(b"value".to_vec())]);
    consumer.close().await.expect("consumer close");
    admin.close().await;
    ctx.cleanup().await;
}
