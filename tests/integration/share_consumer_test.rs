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

//! Integration tests for the KIP-932 share consumer, translated from
//! `org.apache.kafka.clients.consumer.ShareConsumerTest` (Apache Kafka 4.2,
//! `clients-integration-tests`).
//!
//! # Scope, harness fit, and Docker gating
//!
//! Java's `ShareConsumerTest` (4373 lines) runs against the JVM `@ClusterTest`
//! framework with an in-process KRaft broker + an `Admin` client. The Rust
//! harness provisions a broker via testcontainers and has **no `AdminClient`**.
//! Two Java helpers used by almost every consume/acknowledge test have no Rust
//! equivalent yet:
//!
//! * `alterShareAutoOffsetReset(group, "earliest")` — sets the per-group
//!   `share.auto.offset.reset` config via `Admin.incrementalAlterConfigs` on a
//!   `GROUP` `ConfigResource`. Without it the share group keeps the broker
//!   default (`latest`), so records produced *before* the first poll are not
//!   delivered. There is no client-side share-group config API.
//! * `verifyShareGroupStateTopicRecordsProduced()` — reads the internal
//!   `__share_group_state` topic through `Admin` to assert coordinator state
//!   was persisted.
//!
//! Additionally, KIP-932 share groups must be enabled broker-side
//! (`group.coordinator.rebalance.protocols` must include `share`, plus the
//! share-coordinator state-topic settings below). Whether the testcontainers
//! `apache/kafka` image build in CI has share groups fully functional is not
//! guaranteed.
//!
//! Therefore this file translates a faithful **subset**:
//!   * Tests exercising pure client-side behavior that needs no records and no
//!     `Admin` (e.g. poll-without-subscribe → `IllegalState`) run normally.
//!   * Tests that require produced records with `earliest` reset, acknowledge
//!     round-trips, or share-group-state verification are translated but
//!     `#[ignore]`d with a precise reason; they need an `AdminClient` (for
//!     `alterShareAutoOffsetReset`) and a share-group-enabled broker. Run them
//!     manually once those land:
//!     `cargo test --features integration-tests --test integration -- --ignored share_consumer`.
//!
//! The docker-free proof that the production share pipeline is wired
//! (build → subscribe → poll → close, driving the real bg task) lives at
//! `src/consumer/mod.rs::share_pipeline_smoke_tests` and in
//! `KafkaShareConsumerTest`'s deferral note.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::time::Duration;
use std::time::Instant;

use confluent_kafka::common::KafkaError;
use confluent_kafka::common::serialization::Deserializer;
use confluent_kafka::common::serialization::StringSerializer;
use confluent_kafka::consumer::ShareConsumer;
use confluent_kafka::consumer::ShareConsumerConfig;
use confluent_kafka::consumer::new_share_consumer;
use confluent_kafka::producer::KafkaProducer;
use confluent_kafka::producer::Producer;
use confluent_kafka::producer::ProducerConfig;
use confluent_kafka::producer::ProducerRecord;

use crate::common::cluster_config::ClusterConfig;
use crate::common::test_context::TestContext;

/// Byte-array deserializer (`org.apache.kafka.common.serialization.ByteArrayDeserializer`).
struct ByteArrayDeserializer;
impl Deserializer<Vec<u8>> for ByteArrayDeserializer {
    fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<Vec<u8>, KafkaError> {
        Ok(data.to_vec())
    }
}

/// Cluster config enabling KIP-932 share groups. Mirrors the Java
/// `@ClusterTestDefaults` share-coordinator server properties, plus the
/// `share` rebalance protocol (the broker default excludes it).
fn share_cluster_config() -> ClusterConfig {
    let mut props = BTreeMap::new();
    props.insert(
        "KAFKA_GROUP_COORDINATOR_REBALANCE_PROTOCOLS".to_string(),
        "classic,consumer,share".to_string(),
    );
    props.insert("KAFKA_GROUP_SHARE_ENABLE".to_string(), "true".to_string());
    props.insert("KAFKA_GROUP_SHARE_PARTITION_MAX_RECORD_LOCKS".to_string(), "10000".to_string());
    props.insert("KAFKA_GROUP_SHARE_RECORD_LOCK_DURATION_MS".to_string(), "15000".to_string());
    props.insert("KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR".to_string(), "1".to_string());
    props.insert(
        "KAFKA_SHARE_COORDINATOR_STATE_TOPIC_NUM_PARTITIONS".to_string(),
        "3".to_string(),
    );
    props.insert(
        "KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR".to_string(),
        "1".to_string(),
    );
    props.insert("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR".to_string(), "1".to_string());
    props.insert("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR".to_string(), "1".to_string());
    props.insert("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR".to_string(), "1".to_string());
    ClusterConfig::with_properties(props)
}

/// Java: `createShareConsumer(groupId)`. Byte-array key/value consumer.
fn create_share_consumer(bootstrap: &str, group_id: &str) -> Box<dyn ShareConsumer<Vec<u8>, Vec<u8>>> {
    create_share_consumer_with(bootstrap, group_id, HashMap::new())
}

/// Java: `createShareConsumer(groupId, extraConfig)`.
fn create_share_consumer_with(
    bootstrap: &str,
    group_id: &str,
    extra: HashMap<String, String>,
) -> Box<dyn ShareConsumer<Vec<u8>, Vec<u8>>> {
    let mut props = HashMap::new();
    props.insert("bootstrap.servers".to_string(), bootstrap.to_string());
    props.insert("group.id".to_string(), group_id.to_string());
    props.insert("client.id".to_string(), format!("share-consumer-{group_id}"));
    props.extend(extra);
    let config = ShareConsumerConfig::from_properties(&props).expect("valid share consumer config");
    new_share_consumer::<Vec<u8>, Vec<u8>>(config, Box::new(ByteArrayDeserializer), Box::new(ByteArrayDeserializer))
        .expect("share consumer construction")
}

/// Java: `createProducer()`. Sends `Vec<u8>` key/value records.
fn make_producer_config(bootstrap: &str) -> ProducerConfig {
    let props: HashMap<String, String> = [
        ("bootstrap.servers".to_string(), bootstrap.to_string()),
        ("client.id".to_string(), "share-integration-producer".to_string()),
        ("acks".to_string(), "all".to_string()),
    ]
    .into_iter()
    .collect();
    ProducerConfig::from_properties(&props).expect("valid producer config")
}

/// Produce a single `(key, value)` record to `topic`/partition 0. Uses a
/// `String` producer (the share consumer reads the raw UTF-8 bytes via its
/// `ByteArrayDeserializer`).
async fn produce_record(bootstrap: &str, topic: &str, key: &str, value: &str) {
    let producer: KafkaProducer<String, String> = KafkaProducer::from_config(
        make_producer_config(bootstrap),
        Box::new(StringSerializer),
        Box::new(StringSerializer),
    )
    .expect("build producer");
    let record = ProducerRecord::with_key(topic.to_string(), Some(key.to_string()), Some(value.to_string()));
    let future = producer.send(record).await.expect("send");
    future.get_timeout(Duration::from_secs(30)).await.expect("ack");
    producer.close().await.expect("producer close");
}

/// Java: `waitedPoll(consumer, timeoutMs, numRecords)` — poll repeatedly until
/// `num_records` are collected or the deadline elapses. Returns the total
/// number of records collected.
async fn waited_poll(
    consumer: &mut dyn ShareConsumer<Vec<u8>, Vec<u8>>,
    per_poll_ms: u64,
    deadline_secs: u64,
    num_records: usize,
) -> usize {
    let deadline = Instant::now() + Duration::from_secs(deadline_secs);
    let mut count = 0usize;
    while Instant::now() < deadline {
        let records = consumer.poll(Duration::from_millis(per_poll_ms)).await.expect("poll ok");
        count += records.count();
        if count >= num_records {
            break;
        }
    }
    count
}

// ── Runnable: pure client-side behavior (no records, no Admin) ─────────────

/// Java `ShareConsumerTest.testPollNoSubscribeFails`. Polling before any
/// subscription raises `IllegalStateException` — a pure client-side check that
/// happens before any share RPC, so it runs against any broker.
#[tokio::test]
async fn test_poll_no_subscribe_fails() {
    let mut ctx = TestContext::new(share_cluster_config()).await;
    let bootstrap = ctx.bootstrap_servers().to_string();
    let mut consumer = create_share_consumer(&bootstrap, "group1");

    assert!(consumer.subscription().expect("subscription ok").is_empty());
    let err = consumer
        .poll(Duration::from_millis(500))
        .await
        .expect_err("poll without subscription must fail");
    assert!(
        err.message().contains("not subscribed"),
        "expected 'not subscribed' IllegalState, got: {}",
        err.message()
    );
    consumer.close().await.expect("close");
    ctx.cleanup().await;
}

// ── Admin/share-broker-gated: consume + acknowledge round-trips ────────────
//
// These require `alterShareAutoOffsetReset(group, "earliest")` (an AdminClient
// call on a GROUP ConfigResource) and a share-group-enabled broker. The Rust
// client has no AdminClient, so they are `#[ignore]`d until one lands. The
// bodies are translated faithfully so they compile and are ready to run.

/// Java `ShareConsumerTest.testSubscriptionAndPoll`.
#[tokio::test]
#[ignore = "needs AdminClient for alterShareAutoOffsetReset(earliest) + share-group-enabled broker"]
async fn test_subscription_and_poll() {
    let mut ctx = TestContext::new(share_cluster_config()).await;
    let bootstrap = ctx.bootstrap_servers().to_string();
    let topic = ctx.topic("topic");
    // NOTE: Java calls alterShareAutoOffsetReset("group1", "earliest") here.
    // Not translatable without an AdminClient — see the module docs.
    produce_record(&bootstrap, &topic, "key", "value").await;

    let mut consumer = create_share_consumer(&bootstrap, "group1");
    consumer.subscribe(vec![topic.clone()]).await.expect("subscribe");
    assert_eq!(consumer.acquisition_lock_timeout_ms().expect("ok"), None);
    let count = waited_poll(consumer.as_mut(), 2500, 15, 1).await;
    assert_eq!(count, 1);
    assert_eq!(consumer.acquisition_lock_timeout_ms().expect("ok"), Some(15000));
    consumer.close().await.expect("close");
    ctx.cleanup().await;
}

/// Java `ShareConsumerTest.testAcknowledgementCommitCallbackSuccessfulAcknowledgementOnCommitSync`
/// (implicit-acknowledge + commit_sync round-trip).
#[tokio::test]
#[ignore = "needs AdminClient for alterShareAutoOffsetReset(earliest) + share-group-enabled broker"]
async fn test_fetch_and_commit_sync_implicit() {
    let mut ctx = TestContext::new(share_cluster_config()).await;
    let bootstrap = ctx.bootstrap_servers().to_string();
    let topic = ctx.topic("topic");
    produce_record(&bootstrap, &topic, "key", "value").await;

    let mut consumer = create_share_consumer(&bootstrap, "group1");
    consumer.subscribe(vec![topic.clone()]).await.expect("subscribe");
    let count = waited_poll(consumer.as_mut(), 2500, 15, 1).await;
    assert_eq!(count, 1);
    // Implicit mode: poll auto-acknowledges the prior batch; commit_sync flushes.
    let result = consumer.commit_sync().await.expect("commit_sync");
    assert!(
        result.values().all(|e| e.is_none()),
        "all acknowledgements must succeed: {result:?}"
    );
    consumer.close().await.expect("close");
    ctx.cleanup().await;
}

/// Java `ShareConsumerTest.testSubscribePollUnsubscribe` — subscription()
/// reflects subscribe then unsubscribe; poll returns 0. Needs a
/// share-group-enabled broker to accept the subscribe/heartbeat without a
/// fatal `UNSUPPORTED_VERSION`, so it is gated.
#[tokio::test]
#[ignore = "needs share-group-enabled broker (share heartbeat) — poll surfaces bg errors"]
async fn test_subscribe_poll_unsubscribe() {
    let mut ctx = TestContext::new(share_cluster_config()).await;
    let bootstrap = ctx.bootstrap_servers().to_string();
    let topic = ctx.topic("topic");
    let mut consumer = create_share_consumer(&bootstrap, "group1");

    consumer.subscribe(vec![topic.clone()]).await.expect("subscribe");
    let subscription = consumer.subscription().expect("subscription");
    assert!(subscription.contains(&topic));
    let records = consumer.poll(Duration::from_millis(500)).await.expect("poll");
    consumer.unsubscribe().await.expect("unsubscribe");
    assert!(consumer.subscription().expect("subscription").is_empty());
    assert_eq!(records.count(), 0);
    consumer.close().await.expect("close");
    ctx.cleanup().await;
}
