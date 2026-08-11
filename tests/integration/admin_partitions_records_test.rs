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

//! Integration tests for `createPartitions` and `deleteRecords` against a real
//! Kafka 4.2.0 broker.
//!
//! Mirrors the `createPartitions` / `deleteRecords` scenarios in Java's
//! `KafkaAdminClientIntegrationTest`, exercising the real network engine (and,
//! for `deleteRecords`, the `AdminApiDriver` / `PartitionLeaderStrategy` lookup
//! + fulfillment pipeline) end to end.
//!
//! Each scenario is generic over
//! [`AdminBackendFactory`](crate::common::backend_factory::AdminBackendFactory)
//! and registered with [`multilanguage_admin_test!`], so it runs against all
//! four backends. The producer used to seed records is always the native
//! in-process Rust one — it is fixture, not system under test, exactly as in
//! `multilanguage_consumer_test`.

use std::collections::HashMap;
use std::time::Duration;

use confluent_kafka::admin::{
    CreatePartitionsOptions, DeleteRecordsOptions, DeleteTopicsOptions, DescribeTopicsOptions, NewPartitions,
    RecordsToDelete,
};
use confluent_kafka::common::TopicPartition;
use confluent_kafka::common::protocol::Errors;
use confluent_kafka::common::serialization::ByteArraySerializer;
use confluent_kafka::producer::{KafkaProducer, Producer, ProducerConfig, ProducerRecord};

use crate::common::admin_backend::{
    AdminBackend, admin_config, admin_for, all_of, bootstrap_for, create_topic, try_partition_count,
    wait_for_all_partitions_metadata,
};
use crate::common::backend_factory::AdminBackendFactory;
use crate::common::test_context::TestContext;

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
async fn partition_count<B: AdminBackend>(admin: &B, topic: &str) -> usize {
    try_partition_count(admin, topic)
        .await
        .unwrap_or_else(|| panic!("{} backend: describe {topic}", admin.name()))
}

/// Delete `topic` and close the client, asserting the deletion as the originals
/// did.
async fn delete_and_close<B: AdminBackend>(admin: &B, topic: &str) {
    let deleted = admin
        .delete_topics(&[topic.to_string()], DeleteTopicsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{} backend: delete topic: {e}", admin.name()));
    all_of(&deleted).unwrap_or_else(|e| panic!("{} backend: delete topic: {e}", admin.name()));
    admin
        .close(Some(Duration::from_secs(5)))
        .await
        .unwrap_or_else(|e| panic!("{} backend: close: {e}", admin.name()));
}

// ---------------------------------------------------------------------------
// Test bodies — generic over AdminBackendFactory
// ---------------------------------------------------------------------------

/// `create_partitions` increases the partition count of an existing topic.
async fn create_partitions_increases_count<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let admin = admin_for(factory, ctx).await;
    let backend = factory.name();

    let topic = ctx.topic("admin_create_partitions");
    create_topic(&admin, &topic, 1, 1).await;
    assert_eq!(
        partition_count(&admin, &topic).await,
        1,
        "{backend} backend: topic should start with 1 partition"
    );

    let counts = HashMap::from([(topic.clone(), NewPartitions::increase_to(3))]);
    let created = admin
        .create_partitions(&counts, CreatePartitionsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: create partitions: {e}"));
    all_of(&created).unwrap_or_else(|e| panic!("{backend} backend: create partitions should succeed: {e}"));

    // The new partition count is observable via describe_topics once the
    // metadata has propagated. Mirrors Java's
    // `TestUtils.waitForAllPartitionsMetadata(brokers, topic1, expectedNumPartitions = 3)`.
    wait_for_all_partitions_metadata(&admin, &topic, 3).await;
    assert_eq!(
        partition_count(&admin, &topic).await,
        3,
        "{backend} backend: partition count should have increased to 3"
    );

    delete_and_close(&admin, &topic).await;
    ctx.cleanup().await;
}

/// `create_partitions` to a lower count than the topic currently has fails with
/// `INVALID_PARTITIONS`.
async fn create_partitions_decreasing_count_fails<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let admin = admin_for(factory, ctx).await;
    let backend = factory.name();

    let topic = ctx.topic("admin_create_partitions_decrease");
    create_topic(&admin, &topic, 3, 1).await;

    let counts = HashMap::from([(topic.clone(), NewPartitions::increase_to(1))]);
    let created = admin
        .create_partitions(&counts, CreatePartitionsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: create partitions: {e}"));
    let err = created[&topic]
        .as_ref()
        .expect_err(&format!("{backend} backend: decreasing partitions should fail"));
    assert_eq!(
        err.error(),
        Errors::InvalidPartitions,
        "{backend} backend: expected INVALID_PARTITIONS, got {err:?}"
    );

    delete_and_close(&admin, &topic).await;
    ctx.cleanup().await;
}

/// `create_partitions` with an explicit assignment for the new partitions, i.e.
/// Java's `NewPartitions.increaseTo(int, List<List<Integer>>)`.
///
/// Not a conversion of a committed test. That overload is a different broker
/// request from `increaseTo(int)` and has its own path in every binding
/// (`kafka_admin_NewPartitions_add_assignment`, admin.py's `new_assignments`),
/// so without this scenario it would be wired through the harness and never
/// executed. One inner list per *new* partition; existing partitions are not
/// reassigned.
async fn create_partitions_with_assignment<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let admin = admin_for(factory, ctx).await;
    let backend = factory.name();

    let topic = ctx.topic("admin_create_partitions_assigned");
    create_topic(&admin, &topic, 1, 1).await;

    // Learn this cluster's broker id from the partition the broker placed itself.
    let described = admin
        .describe_topics(std::slice::from_ref(&topic), DescribeTopicsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: describe topics: {e}"));
    let broker_id = described[&topic]
        .as_ref()
        .unwrap_or_else(|e| panic!("{backend} backend: describe: {e}"))
        .partitions()[0]
        .replicas()[0]
        .id();

    // 1 -> 2 partitions, so exactly one assignment for the one new partition.
    let counts = HashMap::from([(
        topic.clone(),
        NewPartitions::increase_to_with_assignments(2, vec![vec![broker_id]]),
    )]);
    let created = admin
        .create_partitions(&counts, CreatePartitionsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: create partitions: {e}"));
    all_of(&created).unwrap_or_else(|e| panic!("{backend} backend: assigned create_partitions: {e}"));

    wait_for_all_partitions_metadata(&admin, &topic, 2).await;
    let described = admin
        .describe_topics(std::slice::from_ref(&topic), DescribeTopicsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: describe topics: {e}"));
    let desc = described[&topic]
        .as_ref()
        .unwrap_or_else(|e| panic!("{backend} backend: describe: {e}"));
    let new_partition = desc
        .partitions()
        .iter()
        .find(|p| p.partition() == 1)
        .unwrap_or_else(|| panic!("{backend} backend: partition 1 should exist"));
    assert_eq!(
        new_partition.replicas().iter().map(|n| n.id()).collect::<Vec<_>>(),
        vec![broker_id],
        "{backend} backend: the new partition must sit on the requested broker"
    );

    delete_and_close(&admin, &topic).await;
    ctx.cleanup().await;
}

/// `delete_records` truncates a partition and advances its low watermark.
async fn delete_records_advances_low_watermark<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let admin = admin_for(factory, ctx).await;
    let backend = factory.name();
    let bootstrap = ctx.bootstrap_servers().to_string();

    let topic = ctx.topic("admin_delete_records");
    create_topic(&admin, &topic, 1, 1).await;

    // Produce 10 records (offsets 0..9) to partition 0.
    produce_records(&bootstrap, &topic, 0, 10).await;

    // Delete everything before offset 5; the low watermark advances to 5.
    let tp = TopicPartition::new(topic.clone(), 0);
    let records = HashMap::from([(tp.clone(), RecordsToDelete::before_offset(5))]);
    let deleted = admin
        .delete_records(&records, DeleteRecordsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: delete records: {e}"));
    let low_watermark = deleted[&tp]
        .as_ref()
        .unwrap_or_else(|e| panic!("{backend} backend: delete records should succeed: {e}"));
    assert_eq!(
        low_watermark.low_watermark(),
        5,
        "{backend} backend: low watermark should advance to the deletion offset"
    );

    delete_and_close(&admin, &topic).await;
    ctx.cleanup().await;
}

/// `delete_records` with an offset beyond the high watermark fails that
/// partition with `OFFSET_OUT_OF_RANGE` (a clean per-partition fulfillment
/// error, exercising the driver's error path).
async fn delete_records_offset_out_of_range_fails<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let admin = admin_for(factory, ctx).await;
    let backend = factory.name();
    let bootstrap = ctx.bootstrap_servers().to_string();

    let topic = ctx.topic("admin_delete_records_oor");
    create_topic(&admin, &topic, 1, 1).await;
    produce_records(&bootstrap, &topic, 0, 5).await;

    let tp = TopicPartition::new(topic.clone(), 0);
    let records = HashMap::from([(tp.clone(), RecordsToDelete::before_offset(1000))]);
    let deleted = admin
        .delete_records(&records, DeleteRecordsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: delete records: {e}"));
    let err = deleted[&tp]
        .as_ref()
        .expect_err(&format!("{backend} backend: out-of-range delete should fail"));
    assert_eq!(
        err.error(),
        Errors::OffsetOutOfRange,
        "{backend} backend: expected OFFSET_OUT_OF_RANGE, got {err:?}"
    );

    delete_and_close(&admin, &topic).await;
    ctx.cleanup().await;
}

/// `delete_records` on a partition that does not exist never resolves a leader,
/// so the request fails once the API timeout elapses. A short
/// `default.api.timeout.ms` keeps the test fast.
async fn delete_records_nonexistent_partition_fails<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    // The original built its client with `admin_for_with_timeout(.., 8000)`; the
    // shared config plus one override is the same thing.
    let mut config = admin_config(&bootstrap_for(factory, ctx));
    config.insert("default.api.timeout.ms".to_string(), "8000".to_string());
    let admin = factory
        .create(config)
        .await
        .unwrap_or_else(|e| panic!("{} backend: create admin client: {e}", factory.name()));
    let backend = factory.name();

    let topic = ctx.topic("admin_delete_records_missing");
    create_topic(&admin, &topic, 1, 1).await;

    // Partition 5 does not exist (topic has only partition 0).
    let tp = TopicPartition::new(topic.clone(), 5);
    let records = HashMap::from([(tp.clone(), RecordsToDelete::before_offset(0))]);
    let deleted = admin
        .delete_records(&records, DeleteRecordsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: delete records: {e}"));
    let err = deleted[&tp].as_ref().expect_err(&format!(
        "{backend} backend: deleting records for a nonexistent partition should fail"
    ));
    // The leader lookup never succeeds, so the driver fails the key when the
    // API timeout elapses.
    assert!(
        err.is_retriable() || matches!(err.error(), Errors::RequestTimedOut),
        "{backend} backend: expected a timeout, got {err:?}"
    );

    delete_and_close(&admin, &topic).await;
    ctx.cleanup().await;
}

crate::multilanguage_admin_test!(test_create_partitions_increases_count, create_partitions_increases_count);
crate::multilanguage_admin_test!(
    test_create_partitions_decreasing_count_fails,
    create_partitions_decreasing_count_fails
);
crate::multilanguage_admin_test!(test_create_partitions_with_assignment, create_partitions_with_assignment);
crate::multilanguage_admin_test!(
    test_delete_records_advances_low_watermark,
    delete_records_advances_low_watermark
);
crate::multilanguage_admin_test!(
    test_delete_records_offset_out_of_range_fails,
    delete_records_offset_out_of_range_fails
);
crate::multilanguage_admin_test!(
    test_delete_records_nonexistent_partition_fails,
    delete_records_nonexistent_partition_fails
);
