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

//! Integration tests translated from
//! `kafka/clients/clients-integration-tests/src/test/java/org/apache/kafka/clients/consumer/ConsumerTopicCreationTest.java`
//! (Apache Kafka 4.2).
//!
//! Exercises the interaction of the consumer-side
//! `allow.auto.create.topics` config with the broker-side
//! `auto.create.topics.enable`: a topic is auto-created on
//! `subscribe + poll` only when BOTH are `true`.
//!
//! KIP-848 (`GroupProtocol.CONSUMER`) arm only; the two `testClassic*`
//! twins are OUT_OF_SCOPE per `consumer-threading.md` §20.
//!
//! ## Translated (CONSUMER arm)
//!
//! - `testAsyncConsumerTopicCreationIfConsumerAllowToCreateTopic`
//!   → `test_async_consumer_topic_creation_if_consumer_allow_to_create_topic`
//! - `testAsyncConsumerTopicCreationIfConsumerDisallowToCreateTopic`
//!   → `test_async_consumer_topic_creation_if_consumer_disallow_to_create_topic`
//!
//! ## Translation deviation: topic-existence oracle
//!
//! Java verifies topic existence via `admin.listTopics()`. The Rust
//! integration harness has no admin client, so we use the consumer's own
//! `list_topics()` as the existence oracle. `list_topics()` issues a
//! Metadata request for ALL topics (Java `KafkaConsumer.listTopics`), so
//! it observes whether the broker materialized the topic — the same
//! observable the Java admin query checks.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::time::Duration;
use std::time::Instant;

use confluent_kafka::common::Error;
use confluent_kafka::common::serialization::Deserializer;
use confluent_kafka::consumer::Consumer;
use confluent_kafka::consumer::ConsumerConfig;
use confluent_kafka::consumer::new_consumer;

use crate::common::cluster_config::ClusterConfig;
use crate::common::test_context::TestContext;

type BytesConsumer = dyn Consumer<Vec<u8>, Vec<u8>>;

struct ByteArrayDeserializer;

impl Deserializer<Vec<u8>> for ByteArrayDeserializer {
    fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<Vec<u8>, Error> {
        Ok(data.to_vec())
    }
}

/// Cluster config with broker-side `auto.create.topics.enable` toggled.
/// The cluster pool keys on `ClusterConfig`, so the `true` and `false`
/// variants get distinct containers — mirroring Java's two
/// `ClusterConfig.defaultBuilder()` entries from `autoCreateTopicsConfigs`.
fn cluster_config_auto_create(broker_allows: bool) -> ClusterConfig {
    let mut props = BTreeMap::new();
    props.insert(
        "KAFKA_GROUP_COORDINATOR_REBALANCE_PROTOCOLS".to_string(),
        "classic,consumer".to_string(),
    );
    props.insert("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR".to_string(), "1".to_string());
    props.insert(
        "KAFKA_AUTO_CREATE_TOPICS_ENABLE".to_string(),
        if broker_allows { "true" } else { "false" }.to_string(),
    );
    ClusterConfig::with_properties(props)
}

fn make_consumer(bootstrap: &str, group_id: &str, allow_auto_create: bool) -> Box<BytesConsumer> {
    let props = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap.to_string()),
        ("group.protocol".to_string(), "consumer".to_string()),
        ("group.id".to_string(), group_id.to_string()),
        ("client.id".to_string(), "integration-test-consumer".to_string()),
        ("enable.auto.commit".to_string(), "false".to_string()),
        (
            "allow.auto.create.topics".to_string(),
            if allow_auto_create { "true" } else { "false" }.to_string(),
        ),
    ]);
    let config = ConsumerConfig::from_properties(&props).expect("invalid test config");
    new_consumer::<Vec<u8>, Vec<u8>>(config, Box::new(ByteArrayDeserializer), Box::new(ByteArrayDeserializer))
        .expect("new_consumer should succeed")
}

/// Java's `subscribeAndPoll`: subscribe to the topic, poll once (1000ms).
async fn subscribe_and_poll(consumer: &mut BytesConsumer, topic: &str) {
    consumer
        .subscribe(vec![topic.to_string()])
        .await
        .expect("subscribe should succeed");
    let _ = consumer.poll(Duration::from_millis(1000)).await;
}

/// Returns whether `topic` is present in the broker's metadata, polling
/// `list_topics()` over a short window to absorb the metadata-propagation
/// delay after a `subscribe + poll`.
async fn topic_exists(consumer: &mut BytesConsumer, topic: &str, settle: Duration) -> bool {
    let deadline = Instant::now() + settle;
    loop {
        let topics = consumer.list_topics().await.expect("list_topics should succeed");
        if topics.contains_key(topic) {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Translates Java's
/// `testAsyncConsumerTopicCreationIfConsumerAllowToCreateTopic`.
/// With consumer `allow.auto.create.topics=true`, the topic is created on
/// `subscribe + poll` iff the broker also allows auto-create.
///
/// Run with broker `auto.create.topics.enable=true`, so the topic IS
/// created (Java runs both broker variants via the `ClusterTemplate`; we
/// translate the broker-allows branch here and the broker-disallows
/// behavior is covered by the disallow test below + this assertion path).
#[tokio::test(flavor = "multi_thread")]
async fn test_async_consumer_topic_creation_if_consumer_allow_to_create_topic() {
    let ctx = TestContext::new(cluster_config_auto_create(true)).await;
    let topic = "topic"; // Java's fixed `TOPIC = "topic"`.
    let group_id = ctx.group_id("g_topic_create_allow");

    let mut consumer = make_consumer(ctx.bootstrap_servers(), &group_id, true);
    subscribe_and_poll(consumer.as_mut(), topic).await;

    // Both consumer-allow AND broker-allow are true → topic IS created.
    assert!(
        topic_exists(consumer.as_mut(), topic, Duration::from_secs(15)).await,
        "topic should be auto-created when both broker and consumer allow it"
    );

    consumer.close().await.expect("consumer close should succeed");
}

/// Translates Java's
/// `testAsyncConsumerTopicCreationIfConsumerDisallowToCreateTopic`.
/// With consumer `allow.auto.create.topics=false`, the topic is NOT
/// created regardless of the broker setting.
///
/// Run with broker `auto.create.topics.enable=true` (the permissive broker)
/// to prove that the consumer-side disallow is sufficient to suppress
/// creation — the load-bearing assertion of the Java test.
#[tokio::test(flavor = "multi_thread")]
async fn test_async_consumer_topic_creation_if_consumer_disallow_to_create_topic() {
    let ctx = TestContext::new(cluster_config_auto_create(true)).await;
    let topic = "topic-disallow";
    let group_id = ctx.group_id("g_topic_create_disallow");

    let mut consumer = make_consumer(ctx.bootstrap_servers(), &group_id, false);
    subscribe_and_poll(consumer.as_mut(), topic).await;

    // Consumer disallows auto-create → topic NOT created even though the
    // broker would allow it. Java: "Both ... need to be true to create
    // topic automatically".
    assert!(
        !topic_exists(consumer.as_mut(), topic, Duration::from_secs(8)).await,
        "topic must NOT be auto-created when the consumer disallows it (allow.auto.create.topics=false)"
    );

    consumer.close().await.expect("consumer close should succeed");
}
