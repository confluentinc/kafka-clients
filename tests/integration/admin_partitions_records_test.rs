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

//! Integration tests for `KafkaAdminClient::create_partitions` and
//! `KafkaAdminClient::delete_records` against a real Kafka 4.2.0 broker.
//!
//! Mirrors the `createPartitions` / `deleteRecords` scenarios in Java's
//! `KafkaAdminClientIntegrationTest`, exercising the real network engine (and,
//! for `deleteRecords`, the `AdminApiDriver` / `PartitionLeaderStrategy` lookup
//! + fulfillment pipeline) end to end.

use std::collections::HashMap;
use std::time::Duration;

use confluent_kafka::admin::{
    Admin, AdminClientConfig, CreatePartitionsOptions, DeleteRecordsOptions, DeleteTopicsOptions,
    DescribeTopicsOptions, NewPartitions, RecordsToDelete, new_admin_client,
};
use confluent_kafka::common::TopicCollection;
use confluent_kafka::common::TopicPartition;
use confluent_kafka::common::protocol::Errors;
use confluent_kafka::common::serialization::ByteArraySerializer;
use confluent_kafka::producer::{KafkaProducer, Producer, ProducerConfig, ProducerRecord};

use crate::common::cluster_config::ClusterConfig;
use crate::common::test_context::TestContext;
use crate::common::test_utils::{create_topic, wait_for_all_partitions_metadata};

/// Build an admin client pointed at the cluster's PLAINTEXT listener.
fn admin_for(bootstrap_servers: &str) -> Box<dyn Admin> {
    admin_for_with_timeout(bootstrap_servers, 30000)
}

fn admin_for_with_timeout(bootstrap_servers: &str, api_timeout_ms: i64) -> Box<dyn Admin> {
    let props = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap_servers.to_string()),
        ("client.id".to_string(), "integration-test-admin".to_string()),
        ("request.timeout.ms".to_string(), "30000".to_string()),
        ("default.api.timeout.ms".to_string(), api_timeout_ms.to_string()),
    ]);
    let config = AdminClientConfig::from_properties(&props).expect("valid admin config");
    new_admin_client(config).expect("admin client")
}

/// Build a byte-array producer with `acks=all`.
fn build_producer(bootstrap: &str) -> KafkaProducer<Vec<u8>, Vec<u8>> {
    let props = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap.to_string()),
        ("client.id".to_string(), "integration-test-admin-producer".to_string()),
        ("acks".to_string(), "all".to_string()),
        ("max.block.ms".to_string(), "30000".to_string()),
    ]);
    let config = ProducerConfig::from_properties(&props).expect("valid producer config");
    KafkaProducer::from_config(config, Box::new(ByteArraySerializer), Box::new(ByteArraySerializer))
        .expect("build producer")
}

/// Produce `num` records to `(topic, partition)`, waiting for the broker acks.
async fn produce_records(bootstrap: &str, topic: &str, partition: i32, num: usize) {
    let producer = build_producer(bootstrap);
    let mut last = None;
    for i in 0..num {
        let record = ProducerRecord::with_partition(
            topic.to_string(),
            Some(partition),
            Some(format!("key {i}").into_bytes()),
            Some(format!("value {i}").into_bytes()),
        )
        .expect("build record");
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

/// Returns the current partition count for `topic` via `describe_topics`.
async fn partition_count(admin: &dyn Admin, topic: &str) -> usize {
    let described = admin
        .describe_topics(
            TopicCollection::of_topic_names(vec![topic.to_string()]),
            DescribeTopicsOptions::new(),
        )
        .all_topic_names()
        .expect("described by name")
        .get()
        .await
        .expect("describe topics");
    described[topic].partitions().len()
}

/// `create_partitions` increases the partition count of an existing topic.
#[tokio::test]
async fn test_create_partitions_increases_count() {
    let mut ctx = TestContext::new(ClusterConfig::default()).await;
    let admin = admin_for(ctx.bootstrap_servers());

    let topic = ctx.topic("admin_create_partitions");
    create_topic(admin.as_ref(), &topic, 1, 1).await;
    assert_eq!(
        partition_count(admin.as_ref(), &topic).await,
        1,
        "topic should start with 1 partition"
    );

    let mut counts = HashMap::new();
    counts.insert(topic.clone(), NewPartitions::increase_to(3));
    admin
        .create_partitions(&counts, CreatePartitionsOptions::new())
        .all()
        .get()
        .await
        .expect("create partitions should succeed");

    // The new partition count is observable via describe_topics once the
    // metadata has propagated. Mirrors Java's
    // `TestUtils.waitForAllPartitionsMetadata(brokers, topic1, expectedNumPartitions = 3)`.
    wait_for_all_partitions_metadata(admin.as_ref(), &topic, 3).await;
    assert_eq!(
        partition_count(admin.as_ref(), &topic).await,
        3,
        "partition count should have increased to 3"
    );

    admin
        .delete_topics(TopicCollection::of_topic_names(vec![topic.clone()]), DeleteTopicsOptions::new())
        .all()
        .get()
        .await
        .expect("delete topic");
    admin.close(Duration::from_secs(5)).await;
    ctx.cleanup().await;
}

/// `create_partitions` to a lower count than the topic currently has fails with
/// `INVALID_PARTITIONS`.
#[tokio::test]
async fn test_create_partitions_decreasing_count_fails() {
    let mut ctx = TestContext::new(ClusterConfig::default()).await;
    let admin = admin_for(ctx.bootstrap_servers());

    let topic = ctx.topic("admin_create_partitions_decrease");
    create_topic(admin.as_ref(), &topic, 3, 1).await;

    let mut counts = HashMap::new();
    counts.insert(topic.clone(), NewPartitions::increase_to(1));
    let result = admin.create_partitions(&counts, CreatePartitionsOptions::new());
    let err = result.values()[&topic]
        .get()
        .await
        .expect_err("decreasing partitions should fail");
    assert_eq!(
        err.error(),
        Errors::InvalidPartitions,
        "expected INVALID_PARTITIONS, got {err:?}"
    );

    admin
        .delete_topics(TopicCollection::of_topic_names(vec![topic.clone()]), DeleteTopicsOptions::new())
        .all()
        .get()
        .await
        .expect("delete topic");
    admin.close(Duration::from_secs(5)).await;
    ctx.cleanup().await;
}

/// `delete_records` truncates a partition and advances its low watermark.
#[tokio::test]
async fn test_delete_records_advances_low_watermark() {
    let mut ctx = TestContext::new(ClusterConfig::default()).await;
    let admin = admin_for(ctx.bootstrap_servers());
    let bootstrap = ctx.bootstrap_servers().to_string();

    let topic = ctx.topic("admin_delete_records");
    create_topic(admin.as_ref(), &topic, 1, 1).await;

    // Produce 10 records (offsets 0..9) to partition 0.
    produce_records(&bootstrap, &topic, 0, 10).await;

    // Delete everything before offset 5; the low watermark advances to 5.
    let tp = TopicPartition::new(topic.clone(), 0);
    let mut records = HashMap::new();
    records.insert(tp.clone(), RecordsToDelete::before_offset(5));
    let result = admin.delete_records(&records, DeleteRecordsOptions::new());
    let deleted = result.low_watermarks()[&tp].get().await.expect("delete records should succeed");
    assert_eq!(
        deleted.low_watermark(),
        5,
        "low watermark should advance to the deletion offset"
    );

    admin
        .delete_topics(TopicCollection::of_topic_names(vec![topic.clone()]), DeleteTopicsOptions::new())
        .all()
        .get()
        .await
        .expect("delete topic");
    admin.close(Duration::from_secs(5)).await;
    ctx.cleanup().await;
}

/// `delete_records` with an offset beyond the high watermark fails that
/// partition with `OFFSET_OUT_OF_RANGE` (a clean per-partition fulfillment
/// error, exercising the driver's error path).
#[tokio::test]
async fn test_delete_records_offset_out_of_range_fails() {
    let mut ctx = TestContext::new(ClusterConfig::default()).await;
    let admin = admin_for(ctx.bootstrap_servers());
    let bootstrap = ctx.bootstrap_servers().to_string();

    let topic = ctx.topic("admin_delete_records_oor");
    create_topic(admin.as_ref(), &topic, 1, 1).await;
    produce_records(&bootstrap, &topic, 0, 5).await;

    let tp = TopicPartition::new(topic.clone(), 0);
    let mut records = HashMap::new();
    records.insert(tp.clone(), RecordsToDelete::before_offset(1000));
    let result = admin.delete_records(&records, DeleteRecordsOptions::new());
    let err = result.low_watermarks()[&tp]
        .get()
        .await
        .expect_err("out-of-range delete should fail");
    assert_eq!(
        err.error(),
        Errors::OffsetOutOfRange,
        "expected OFFSET_OUT_OF_RANGE, got {err:?}"
    );

    admin
        .delete_topics(TopicCollection::of_topic_names(vec![topic.clone()]), DeleteTopicsOptions::new())
        .all()
        .get()
        .await
        .expect("delete topic");
    admin.close(Duration::from_secs(5)).await;
    ctx.cleanup().await;
}

/// `delete_records` on a partition that does not exist never resolves a leader,
/// so the request fails once the API timeout elapses. A short
/// `default.api.timeout.ms` keeps the test fast.
#[tokio::test]
async fn test_delete_records_nonexistent_partition_fails() {
    let mut ctx = TestContext::new(ClusterConfig::default()).await;
    let admin = admin_for_with_timeout(ctx.bootstrap_servers(), 8000);

    let topic = ctx.topic("admin_delete_records_missing");
    create_topic(admin.as_ref(), &topic, 1, 1).await;

    // Partition 5 does not exist (topic has only partition 0).
    let tp = TopicPartition::new(topic.clone(), 5);
    let mut records = HashMap::new();
    records.insert(tp.clone(), RecordsToDelete::before_offset(0));
    let result = admin.delete_records(&records, DeleteRecordsOptions::new());
    let err = result.low_watermarks()[&tp]
        .get()
        .await
        .expect_err("deleting records for a nonexistent partition should fail");
    // The leader lookup never succeeds, so the driver fails the key when the
    // API timeout elapses.
    assert!(
        err.is_retriable() || matches!(err.error(), Errors::RequestTimedOut),
        "expected a timeout, got {err:?}"
    );

    admin
        .delete_topics(TopicCollection::of_topic_names(vec![topic.clone()]), DeleteTopicsOptions::new())
        .all()
        .get()
        .await
        .expect("delete topic");
    admin.close(Duration::from_secs(5)).await;
    ctx.cleanup().await;
}
