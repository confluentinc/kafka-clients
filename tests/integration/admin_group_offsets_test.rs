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

//! Integration tests for the `KafkaAdminClient` consumer-group-offset RPCs
//! (`listConsumerGroupOffsets` / `alterConsumerGroupOffsets` /
//! `deleteConsumerGroupOffsets`) against a real Kafka 4.2.0 broker.
//!
//! Mirrors the offset-management scenarios in Java's
//! `KafkaAdminClientIntegrationTest`, exercising the real network engine and the
//! `CoordinatorStrategy` lookup end to end rather than the `MockClient`
//! unit-test harness.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use confluent_kafka::admin::{
    Admin, AdminClientConfig, AlterConsumerGroupOffsetsOptions, CreateTopicsOptions, DeleteConsumerGroupOffsetsOptions,
    ListConsumerGroupOffsetsOptions, ListConsumerGroupOffsetsSpec, NewTopic, new_admin_client,
};
use confluent_kafka::common::protocol::Errors;
use confluent_kafka::common::serialization::{ByteArraySerializer, Deserializer};
use confluent_kafka::common::{KafkaError, TopicPartition};
use confluent_kafka::consumer::{Consumer, ConsumerConfig, OffsetAndMetadata, new_consumer};
use confluent_kafka::producer::{KafkaProducer, Producer, ProducerConfig, ProducerRecord};

use crate::common::cluster_config::kip848_3_broker;
use crate::common::test_context::TestContext;

const NUM_PARTITIONS: i32 = 2;

/// Local byte-array deserializer (the crate exports `ByteArraySerializer` but no
/// symmetric `ByteArrayDeserializer`).
struct ByteArrayDeserializer;

impl Deserializer<Vec<u8>> for ByteArrayDeserializer {
    fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<Vec<u8>, KafkaError> {
        Ok(data.to_vec())
    }
}

type BytesConsumer = Box<dyn Consumer<Vec<u8>, Vec<u8>>>;

fn admin_for(bootstrap_servers: &str) -> Box<dyn Admin> {
    let props = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap_servers.to_string()),
        ("client.id".to_string(), "integration-test-admin".to_string()),
        ("request.timeout.ms".to_string(), "30000".to_string()),
        ("default.api.timeout.ms".to_string(), "30000".to_string()),
    ]);
    let config = AdminClientConfig::from_properties(&props).expect("valid admin config");
    new_admin_client(config).expect("admin client")
}

fn consumer_config(bootstrap: &str, group_id: &str) -> ConsumerConfig {
    let props = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap.to_string()),
        ("group.protocol".to_string(), "consumer".to_string()),
        ("auto.offset.reset".to_string(), "earliest".to_string()),
        ("client.id".to_string(), "integration-test-consumer".to_string()),
        ("enable.auto.commit".to_string(), "false".to_string()),
        ("group.id".to_string(), group_id.to_string()),
    ]);
    ConsumerConfig::from_properties(&props).expect("invalid consumer test config")
}

fn new_bytes_consumer(bootstrap: &str, group_id: &str) -> BytesConsumer {
    new_consumer::<Vec<u8>, Vec<u8>>(
        consumer_config(bootstrap, group_id),
        Box::new(ByteArrayDeserializer),
        Box::new(ByteArrayDeserializer),
    )
    .expect("new_consumer should succeed")
}

/// Subscribe and poll until the KIP-848 group has reconciled (partitions
/// assigned).
async fn subscribe_and_join(consumer: &mut BytesConsumer, topic: &str) {
    consumer
        .subscribe(vec![topic.to_string()])
        .await
        .expect("subscribe should succeed");
    for _ in 0..60 {
        let _ = consumer.poll(Duration::from_millis(500)).await;
        if !consumer.assignment().is_empty() {
            return;
        }
    }
    panic!("consumer never received a partition assignment for topic {topic}");
}

async fn create_topic(admin: &dyn Admin, topic: &str) {
    admin
        .create_topics(
            &[NewTopic::new(topic.to_string(), NUM_PARTITIONS, 1)],
            CreateTopicsOptions::new(),
        )
        .all()
        .get()
        .await
        .expect("create topic");
}

async fn produce_records(bootstrap: &str, tp: &TopicPartition, num: usize) {
    let props = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap.to_string()),
        ("client.id".to_string(), "integration-test-producer".to_string()),
        ("acks".to_string(), "all".to_string()),
        ("linger.ms".to_string(), "5".to_string()),
    ]);
    let producer: KafkaProducer<Vec<u8>, Vec<u8>> = KafkaProducer::from_config(
        ProducerConfig::from_properties(&props).expect("producer config"),
        Box::new(ByteArraySerializer),
        Box::new(ByteArraySerializer),
    )
    .expect("build producer");
    let mut last = None;
    for i in 0..num {
        let record = ProducerRecord::with_timestamp(
            tp.topic().to_string(),
            Some(tp.partition()),
            Some(1_700_000_000_000 + i as i64),
            Some(format!("k{i}").into_bytes()),
            Some(format!("v{i}").into_bytes()),
        )
        .expect("valid producer record");
        last = Some(
            <KafkaProducer<Vec<u8>, Vec<u8>> as Producer<Vec<u8>, Vec<u8>>>::send(&producer, record)
                .await
                .expect("send"),
        );
    }
    producer.flush().await.expect("flush");
    if let Some(f) = last {
        f.get_timeout(Duration::from_secs(30)).await.expect("last send");
    }
    producer.close().await.expect("producer close");
}

/// Lists a single group's committed offsets, retrying while the coordinator
/// settles.
async fn list_offsets(admin: &dyn Admin, group_id: &str) -> HashMap<TopicPartition, Option<OffsetAndMetadata>> {
    let spec = HashMap::from([(group_id.to_string(), ListConsumerGroupOffsetsSpec::new())]);
    let result = admin.list_consumer_group_offsets(&spec, ListConsumerGroupOffsetsOptions::new());
    result
        .partitions_to_offset_and_metadata()
        .expect("single group")
        .get_timeout(Duration::from_secs(30))
        .await
        .expect("list offsets")
}

/// (a) A live consumer commits explicit offsets; `list_consumer_group_offsets`
/// reports them.
#[tokio::test(flavor = "multi_thread")]
async fn test_list_consumer_group_offsets_matches_committed() {
    let mut ctx = TestContext::new(kip848_3_broker(NUM_PARTITIONS as u16)).await;
    let admin = admin_for(ctx.bootstrap_servers());
    let topic = ctx.topic("admin_offsets_list");
    let group_id = ctx.group_id("g_offsets_list");
    create_topic(admin.as_ref(), &topic).await;

    let mut consumer = new_bytes_consumer(ctx.bootstrap_servers(), &group_id);
    subscribe_and_join(&mut consumer, &topic).await;

    let tp0 = TopicPartition::new(topic.clone(), 0);
    let tp1 = TopicPartition::new(topic.clone(), 1);
    let committed = HashMap::from([
        (tp0.clone(), OffsetAndMetadata::new(5).unwrap()),
        (tp1.clone(), OffsetAndMetadata::new(3).unwrap()),
    ]);
    consumer.commit_sync_offsets(committed).await.expect("commit_sync");

    let listed = list_offsets(admin.as_ref(), &group_id).await;
    assert_eq!(
        listed.get(&tp0).and_then(|o| o.as_ref()).map(OffsetAndMetadata::offset),
        Some(5)
    );
    assert_eq!(
        listed.get(&tp1).and_then(|o| o.as_ref()).map(OffsetAndMetadata::offset),
        Some(3)
    );

    drop(consumer);
    ctx.cleanup().await;
}

/// (b) After the consumer leaves, `alter_consumer_group_offsets` rewinds the
/// committed offset, and a fresh consumer resumes from the altered position.
#[tokio::test(flavor = "multi_thread")]
async fn test_alter_consumer_group_offsets_and_resume() {
    let mut ctx = TestContext::new(kip848_3_broker(NUM_PARTITIONS as u16)).await;
    let admin = admin_for(ctx.bootstrap_servers());
    let topic = ctx.topic("admin_offsets_alter");
    let group_id = ctx.group_id("g_offsets_alter");
    create_topic(admin.as_ref(), &topic).await;
    let tp0 = TopicPartition::new(topic.clone(), 0);

    produce_records(ctx.bootstrap_servers(), &tp0, 10).await;

    // Consumer A joins and commits offset 10 (the log end), then leaves.
    let mut consumer_a = new_bytes_consumer(ctx.bootstrap_servers(), &group_id);
    subscribe_and_join(&mut consumer_a, &topic).await;
    consumer_a
        .commit_sync_offsets(HashMap::from([(tp0.clone(), OffsetAndMetadata::new(10).unwrap())]))
        .await
        .expect("commit");
    consumer_a.close().await.expect("close A");
    drop(consumer_a);
    // Give the coordinator time to observe the member leave (empty group).
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Alter the committed offset back to 5 while the group is empty.
    admin
        .alter_consumer_group_offsets(
            &group_id,
            &HashMap::from([(tp0.clone(), OffsetAndMetadata::new(5).unwrap())]),
            AlterConsumerGroupOffsetsOptions::new(),
        )
        .all()
        .get_timeout(Duration::from_secs(30))
        .await
        .expect("alter offsets on empty group");

    let listed = list_offsets(admin.as_ref(), &group_id).await;
    assert_eq!(
        listed.get(&tp0).and_then(|o| o.as_ref()).map(OffsetAndMetadata::offset),
        Some(5)
    );

    // Consumer B resumes from the altered offset (5) and reads records 5..10.
    // Subscribe and poll in one loop so the very first partition-0 record is
    // captured (the join itself drives fetching).
    let mut consumer_b = new_bytes_consumer(ctx.bootstrap_servers(), &group_id);
    consumer_b.subscribe(vec![topic.clone()]).await.expect("subscribe B");
    let mut first_offset = None;
    for _ in 0..60 {
        let records = consumer_b.poll(Duration::from_millis(500)).await.expect("poll B");
        if let Some(record) = (&records).into_iter().find(|r| r.partition() == 0) {
            first_offset = Some(record.offset());
            break;
        }
    }
    assert_eq!(first_offset, Some(5), "consumer B should resume from the altered offset");

    drop(consumer_b);
    ctx.cleanup().await;
}

/// (c) `delete_consumer_group_offsets` on an inactive group removes the
/// committed offset.
#[tokio::test(flavor = "multi_thread")]
async fn test_delete_consumer_group_offsets_on_inactive_group() {
    let mut ctx = TestContext::new(kip848_3_broker(NUM_PARTITIONS as u16)).await;
    let admin = admin_for(ctx.bootstrap_servers());
    let topic = ctx.topic("admin_offsets_delete");
    let group_id = ctx.group_id("g_offsets_delete");
    create_topic(admin.as_ref(), &topic).await;
    let tp0 = TopicPartition::new(topic.clone(), 0);

    let mut consumer = new_bytes_consumer(ctx.bootstrap_servers(), &group_id);
    subscribe_and_join(&mut consumer, &topic).await;
    consumer
        .commit_sync_offsets(HashMap::from([(tp0.clone(), OffsetAndMetadata::new(7).unwrap())]))
        .await
        .expect("commit");
    consumer.close().await.expect("close");
    drop(consumer);
    tokio::time::sleep(Duration::from_secs(2)).await;

    // The committed offset is present before deletion.
    let before = list_offsets(admin.as_ref(), &group_id).await;
    assert_eq!(
        before.get(&tp0).and_then(|o| o.as_ref()).map(OffsetAndMetadata::offset),
        Some(7)
    );

    admin
        .delete_consumer_group_offsets(
            &group_id,
            &HashSet::from([tp0.clone()]),
            DeleteConsumerGroupOffsetsOptions::new(),
        )
        .all()
        .get_timeout(Duration::from_secs(30))
        .await
        .expect("delete offsets on inactive group");

    // After deletion the partition has no committed offset.
    let after = list_offsets(admin.as_ref(), &group_id).await;
    assert!(
        after.get(&tp0).map(Option::is_none).unwrap_or(true),
        "committed offset for {tp0} should be gone after delete, got {:?}",
        after.get(&tp0)
    );

    ctx.cleanup().await;
}

/// (d) `delete_consumer_group_offsets` on an ACTIVE (subscribed) group fails.
#[tokio::test(flavor = "multi_thread")]
async fn test_delete_consumer_group_offsets_on_active_group_errors() {
    let mut ctx = TestContext::new(kip848_3_broker(NUM_PARTITIONS as u16)).await;
    let admin = admin_for(ctx.bootstrap_servers());
    let topic = ctx.topic("admin_offsets_active");
    let group_id = ctx.group_id("g_offsets_active");
    create_topic(admin.as_ref(), &topic).await;
    let tp0 = TopicPartition::new(topic.clone(), 0);

    let mut consumer = new_bytes_consumer(ctx.bootstrap_servers(), &group_id);
    subscribe_and_join(&mut consumer, &topic).await;
    // Keep the member alive across the delete attempt.
    let _ = consumer.poll(Duration::from_millis(200)).await;

    let err = admin
        .delete_consumer_group_offsets(
            &group_id,
            &HashSet::from([tp0.clone()]),
            DeleteConsumerGroupOffsetsOptions::new(),
        )
        .all()
        .get_timeout(Duration::from_secs(30))
        .await
        .expect_err("deleting offsets of a partition an active group is subscribed to should fail");
    // The broker rejects deletion of a partition the group is actively
    // subscribed to (GROUP_SUBSCRIBED_TO_TOPIC).
    assert_eq!(
        err.error(),
        Errors::GroupSubscribedToTopic,
        "unexpected error deleting active-group offsets: {err:?}"
    );

    drop(consumer);
    ctx.cleanup().await;
}
