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
//! `org.apache.kafka.clients.consumer.ShareConsumerLagTest` (Apache Kafka 4.4.0,
//! `clients-integration-tests`): the share partition lag reported by
//! `Admin#listShareGroupOffsets` (KIP-1226).
//!
//! The fixture is [`super::share_consumer_test_base`]; its module docs list the
//! deviations every share suite shares (pooled clusters, per-test names).
//!
//! All 8 Java methods are translated. The two coordinator-movement tests stop a
//! broker, so they run on a dedicated three-broker `Type::Kraft` cluster with
//! Java's per-test `serverProperties` added to the class defaults, as Java's
//! `@ClusterTest` merges them.

use std::collections::HashMap;

use confluent_kafka::admin::Admin;
use confluent_kafka::consumer::AcknowledgeType;
use confluent_kafka::consumer::ShareConsumer;
use confluent_kafka::producer::Producer;
use confluent_kafka::producer::ProducerRecord;

use super::share_consumer_test_base::*;
use crate::common::cluster_config::ClusterConfig;

/// `Topic#GROUP_METADATA_TOPIC_NAME`.
const GROUP_METADATA_TOPIC_NAME: &str = "__consumer_offsets";

/// The record every Java test produces:
/// `new ProducerRecord<>(topic, partition, null, "key".getBytes(), "Message".getBytes())`.
fn message_record(topic: &str, partition: i32) -> ProducerRecord<Vec<u8>, Vec<u8>> {
    producer_record(
        topic.to_string(),
        Some(partition),
        Some(b"key".to_vec()),
        Some(b"Message".to_vec()),
    )
}

/// The explicit-acknowledgement consumer properties.
fn explicit() -> HashMap<String, String> {
    HashMap::from([("share.acknowledgement.mode".to_string(), EXPLICIT.to_string())])
}

/// The `@ClusterTest(brokers = 3, serverProperties = ...)` of the two
/// coordinator-movement tests, on a dedicated cluster since they stop a broker.
fn coordinator_movement_config() -> ClusterConfig {
    ClusterConfig {
        brokers: 3,
        dedicated: true,
        ..share_cluster_config_with(&[
            ("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", "1"),
            ("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "3"),
            ("KAFKA_SHARE_COORDINATOR_STATE_TOPIC_NUM_PARTITIONS", "1"),
            ("KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR", "3"),
        ])
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_share_partition_lag_for_single_share_consumer() {
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group_id = base.group("group1");
    base.alter_share_auto_offset_reset(&group_id, "earliest").await;
    let producer = base.create_producer().await;
    let mut share_consumer = base.create_share_consumer(&group_id);
    let admin_client = base.create_admin_client();
    let tp = &base.tp;

    send_record(&producer, message_record(tp.topic(), tp.partition()))
        .await
        .expect("send");
    producer.flush().await.expect("flush");
    share_consumer.subscribe(vec![tp.topic().to_string()]).await.expect("subscribe");
    // Polling share consumer to make sure the share partition is created and the record is consumed.
    base.waited_poll(&mut share_consumer, 2500, 1).await;
    // Acknowledge and commit the consumed record to update the share partition state.
    share_consumer.commit_sync().await.expect("commitSync");
    // After the acknowledgement is successful, the share partition lag should be 0 because the only produced record has been consumed.
    base.verify_share_partition_lag(admin_client.as_ref(), &group_id, tp, 0).await;
    // Producing another record to the share partition.
    send_record(&producer, message_record(tp.topic(), tp.partition()))
        .await
        .expect("send");
    producer.flush().await.expect("flush");
    // Since the new record has not been consumed yet, the share partition lag should be 1.
    base.verify_share_partition_lag(admin_client.as_ref(), &group_id, tp, 1).await;

    close_producer(producer).await;
    close_share_consumer(share_consumer).await;
    close_admin(admin_client).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_share_partition_lag_for_multiple_share_consumers() {
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group_id = base.group("group1");
    base.alter_share_auto_offset_reset(&group_id, "earliest").await;
    let producer = base.create_producer().await;
    let mut share_consumer1 = base.create_share_consumer(&group_id);
    let mut share_consumer2 = base.create_share_consumer(&group_id);
    let admin_client = base.create_admin_client();
    let tp = &base.tp;

    send_record(&producer, message_record(tp.topic(), tp.partition()))
        .await
        .expect("send");
    producer.flush().await.expect("flush");
    share_consumer1
        .subscribe(vec![tp.topic().to_string()])
        .await
        .expect("subscribe");
    share_consumer2
        .subscribe(vec![tp.topic().to_string()])
        .await
        .expect("subscribe");
    // Polling share consumer 1 to make sure the share partition is created and the records are consumed.
    base.waited_poll(&mut share_consumer1, 2500, 1).await;
    // Acknowledge and commit the consumed records to update the share partition state.
    share_consumer1.commit_sync().await.expect("commitSync");
    // After the acknowledgement is successful, the share partition lag should be 0 because the all produced records have been consumed.
    base.verify_share_partition_lag(admin_client.as_ref(), &group_id, tp, 0).await;
    // Producing more records to the share partition.
    send_record(&producer, message_record(tp.topic(), tp.partition()))
        .await
        .expect("send");
    // Polling share consumer 2 this time.
    base.waited_poll(&mut share_consumer2, 2500, 1).await;
    // Since the consumed record hasn't been acknowledged yet, the share partition lag should be 1.
    base.verify_share_partition_lag(admin_client.as_ref(), &group_id, tp, 1).await;
    // Acknowledge and commit the consumed records to update the share partition state.
    share_consumer2.commit_sync().await.expect("commitSync");
    // After the acknowledgement is successful, the share partition lag should be 0 because the all produced records have been consumed.
    base.verify_share_partition_lag(admin_client.as_ref(), &group_id, tp, 0).await;
    // Producing another record to the share partition.
    send_record(&producer, message_record(tp.topic(), tp.partition()))
        .await
        .expect("send");
    producer.flush().await.expect("flush");
    // Since the new record has not been consumed yet, the share partition lag should be 1.
    base.verify_share_partition_lag(admin_client.as_ref(), &group_id, tp, 1).await;

    close_producer(producer).await;
    close_share_consumer(share_consumer1).await;
    close_share_consumer(share_consumer2).await;
    close_admin(admin_client).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_share_partition_lag_with_release_acknowledgement() {
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group_id = base.group("group1");
    base.alter_share_auto_offset_reset(&group_id, "earliest").await;
    let producer = base.create_producer().await;
    let mut share_consumer = base.create_share_consumer_with_properties(&group_id, &explicit());
    let admin_client = base.create_admin_client();
    let tp = &base.tp;

    send_record(&producer, message_record(tp.topic(), tp.partition()))
        .await
        .expect("send");
    producer.flush().await.expect("flush");
    share_consumer.subscribe(vec![tp.topic().to_string()]).await.expect("subscribe");
    // Polling share consumer to make sure the share partition is created and the record is consumed.
    let records = base.waited_poll(&mut share_consumer, 2500, 1).await;
    // Accept the record first to move the offset forward and register the state with persister.
    for r in &records {
        share_consumer
            .acknowledge_with_record_type(r, AcknowledgeType::Accept)
            .expect("acknowledge");
    }
    share_consumer.commit_sync().await.expect("commitSync");
    // After accepting, the lag should be 0 because the record is consumed successfully.
    base.verify_share_partition_lag(admin_client.as_ref(), &group_id, tp, 0).await;
    // Producing another record to the share partition.
    send_record(&producer, message_record(tp.topic(), tp.partition()))
        .await
        .expect("send");
    producer.flush().await.expect("flush");
    // The produced record is consumed.
    let records = base.waited_poll(&mut share_consumer, 2500, 1).await;
    // Now release the record - it should be available for redelivery.
    for r in &records {
        share_consumer
            .acknowledge_with_record_type(r, AcknowledgeType::Release)
            .expect("acknowledge");
    }
    share_consumer.commit_sync().await.expect("commitSync");
    // After releasing the lag should be 1, because the record is released for redelivery.
    base.verify_share_partition_lag(admin_client.as_ref(), &group_id, tp, 1).await;
    // The record is now consumed again.
    let records = base.waited_poll(&mut share_consumer, 2500, 1).await;
    // Accept the record to mark it as consumed.
    for r in &records {
        share_consumer
            .acknowledge_with_record_type(r, AcknowledgeType::Accept)
            .expect("acknowledge");
    }
    share_consumer.commit_sync().await.expect("commitSync");
    // After accepting the record, the lag should be 0 because all the produced records have been consumed.
    base.verify_share_partition_lag(admin_client.as_ref(), &group_id, tp, 0).await;

    close_producer(producer).await;
    close_share_consumer(share_consumer).await;
    close_admin(admin_client).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_share_partition_lag_with_reject_acknowledgement() {
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group_id = base.group("group1");
    base.alter_share_auto_offset_reset(&group_id, "earliest").await;
    let producer = base.create_producer().await;
    let mut share_consumer = base.create_share_consumer_with_properties(&group_id, &explicit());
    let admin_client = base.create_admin_client();
    let tp = &base.tp;

    send_record(&producer, message_record(tp.topic(), tp.partition()))
        .await
        .expect("send");
    producer.flush().await.expect("flush");
    share_consumer.subscribe(vec![tp.topic().to_string()]).await.expect("subscribe");
    // Polling share consumer to make sure the share partition is created and the record is consumed.
    let records = base.waited_poll(&mut share_consumer, 2500, 1).await;
    // Accept the record first to move the offset forward and register the state with persister.
    for r in &records {
        share_consumer
            .acknowledge_with_record_type(r, AcknowledgeType::Accept)
            .expect("acknowledge");
    }
    share_consumer.commit_sync().await.expect("commitSync");
    // After accepting, the lag should be 0 because the record is consumed successfully.
    base.verify_share_partition_lag(admin_client.as_ref(), &group_id, tp, 0).await;
    // Producing another record to the share partition.
    send_record(&producer, message_record(tp.topic(), tp.partition()))
        .await
        .expect("send");
    producer.flush().await.expect("flush");
    // The produced record is consumed.
    let records = base.waited_poll(&mut share_consumer, 2500, 1).await;
    // Now reject the record - it should not be available for redelivery.
    for r in &records {
        share_consumer
            .acknowledge_with_record_type(r, AcknowledgeType::Reject)
            .expect("acknowledge");
    }
    share_consumer.commit_sync().await.expect("commitSync");
    // After rejecting the lag should be 0, because the record is permanently rejected and offset moves forward.
    base.verify_share_partition_lag(admin_client.as_ref(), &group_id, tp, 0).await;

    close_producer(producer).await;
    close_share_consumer(share_consumer).await;
    close_admin(admin_client).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_share_partition_lag_on_group_coordinator_movement() {
    share_partition_lag_on_coordinator_movement(GROUP_METADATA_TOPIC_NAME).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_share_partition_lag_on_share_coordinator_movement() {
    share_partition_lag_on_coordinator_movement(SHARE_GROUP_STATE_TOPIC_NAME).await;
}

/// The shared body of Java's `testSharePartitionLagOnGroupCoordinatorMovement`
/// (`coordinator_topic` = `__consumer_offsets`) and
/// `testSharePartitionLagOnShareCoordinatorMovement` (`__share_group_state`):
/// the two Java methods differ only in which coordinator's broker they stop,
/// the leader of partition 0 of `coordinator_topic`.
async fn share_partition_lag_on_coordinator_movement(coordinator_topic: &str) {
    let mut base = ShareConsumerTestBase::setup(coordinator_movement_config()).await;
    let group_id = base.group("group1");
    base.alter_share_auto_offset_reset(&group_id, "earliest").await;
    let topic_name = base.topic("testTopicWithReplicas");
    let producer = base.create_producer().await;
    let mut share_consumer = base.create_share_consumer(&group_id);
    let admin_client = base.create_admin_client();
    // Create a topic with replication factor 3
    base.create_topic_with_partitions_replication_factor(&topic_name, 1, 3).await;
    let tp = confluent_kafka::common::TopicPartition::new(topic_name.clone(), 0);
    // Produce first record and consume it
    send_record(&producer, message_record(tp.topic(), tp.partition()))
        .await
        .expect("send");
    producer.flush().await.expect("flush");
    share_consumer.subscribe(vec![tp.topic().to_string()]).await.expect("subscribe");
    // Polling share consumer to make sure the share partition is created and the record is consumed.
    base.waited_poll(&mut share_consumer, 2500, 1).await;
    // Acknowledge and commit the consumed record to update the share partition state.
    share_consumer.commit_sync().await.expect("commitSync");
    // After the acknowledgement is successful, the share partition lag should be 0 because the only produced record has been consumed.
    base.verify_share_partition_lag(admin_client.as_ref(), &group_id, &tp, 0).await;
    // Producing another record to the share partition.
    send_record(&producer, message_record(tp.topic(), tp.partition()))
        .await
        .expect("send");
    producer.flush().await.expect("flush");
    // Since the new record has not been consumed yet, the share partition lag should be 1.
    base.verify_share_partition_lag(admin_client.as_ref(), &group_id, &tp, 1).await;
    // Find the broker which is the coordinator (the group coordinator for the share group, or the share
    // coordinator for the share partition).
    let cur_coord_node_id = base
        .topic_partition_leader(admin_client.as_ref(), coordinator_topic, 0)
        .await
        .expect("describeTopics");
    assert_eq!(1, cur_coord_node_id.len());
    // Shut down the coordinator broker; this returns once it is completely shut down (Java's
    // `shutdownBroker` + `awaitShutdown`).
    base.ctx().cluster().shutdown_broker(cur_coord_node_id[0]).await;
    // Wait for the leaders of share coordinator, group coordinator and topic partition to be elected, if needed, on a different broker.
    let admin_ref: &dyn Admin = admin_client.as_ref();
    let base_ref = &base;
    let tp_ref = &tp;
    let stopped = cur_coord_node_id[0];
    wait_for_condition_with_pause(
        || async move {
            let new_share_coord_node_id = base_ref
                .topic_partition_leader(admin_ref, SHARE_GROUP_STATE_TOPIC_NAME, 0)
                .await
                .unwrap_or_default();
            let new_group_coord_node_id = base_ref
                .topic_partition_leader(admin_ref, GROUP_METADATA_TOPIC_NAME, 0)
                .await
                .unwrap_or_default();
            let new_topic_partition_leader = base_ref
                .topic_partition_leader(admin_ref, tp_ref.topic(), tp_ref.partition())
                .await
                .unwrap_or_default();
            new_share_coord_node_id.len() == 1
                && new_share_coord_node_id[0] != stopped
                && new_group_coord_node_id.len() == 1
                && new_group_coord_node_id[0] != stopped
                && new_topic_partition_leader.len() == 1
                && new_topic_partition_leader[0] != stopped
        },
        DEFAULT_MAX_WAIT_MS,
        DEFAULT_POLL_INTERVAL_MS,
        "Failed to elect new leaders after broker shutdown",
    )
    .await;
    // After the coordinator shutdown and new leader's election, check that lag is still 1
    base.verify_share_partition_lag(admin_client.as_ref(), &group_id, &tp, 1).await;

    close_producer(producer).await;
    close_share_consumer(share_consumer).await;
    close_admin(admin_client).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_share_partition_lag_after_alter_share_group_offsets() {
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group_id = base.group("group1");
    let producer = base.create_producer().await;
    let admin_client = base.create_admin_client();
    let tp = &base.tp;

    // Producing 100 records to the topic partition.
    for _ in 0..100 {
        send_record(&producer, message_record(tp.topic(), tp.partition()))
            .await
            .expect("send");
    }
    producer.flush().await.expect("flush");

    // Create a new share consumer. Since the share.auto.offset.reset is not altered, it should be latest by default.
    let mut share_consumer = base.create_share_consumer_with_properties(&group_id, &explicit());
    share_consumer.subscribe(vec![tp.topic().to_string()]).await.expect("subscribe");
    // Polling share consumer to make sure it joins the group and subscribes to the topic.
    base.waited_poll_with_assignment(
        &mut share_consumer,
        2500,
        0,
        true,
        &group_id,
        &[confluent_kafka::common::TopicPartition::new(tp.topic().to_string(), 0)],
    )
    .await;
    // Producing 5 additional records to the topic partition.
    for _ in 0..5 {
        send_record(&producer, message_record(tp.topic(), tp.partition()))
            .await
            .expect("send");
    }
    producer.flush().await.expect("flush");
    // Polling share consumer to make sure the records are consumed.
    let records = base.waited_poll(&mut share_consumer, 2500, 5).await;
    assert_eq!(5, records.count());
    // Accept the record first to move the offset forward and register the state with persister.
    for r in &records {
        share_consumer
            .acknowledge_with_record_type(r, AcknowledgeType::Accept)
            .expect("acknowledge");
    }
    share_consumer.commit_sync().await.expect("commitSync");
    // After accepting, the lag should be 0 because the record is consumed successfully.
    base.verify_share_partition_lag(admin_client.as_ref(), &group_id, tp, 0).await;
    // Closing the share consumer so that the offsets can be altered.
    close_share_consumer(share_consumer).await;
    // Alter the start offset of the share partition to 40.
    base.alter_share_group_offsets(admin_client.as_ref(), &group_id, tp, 40)
        .await
        .unwrap_or_else(|e| panic!("Test failed with exception: {e}"));
    // After altering, the share partition start offset should be 40.
    base.verify_share_partition_start_offset(admin_client.as_ref(), &group_id, tp, 40)
        .await;
    // Verify that the lag is now 65 since the start offset is altered to 40 and there are total 105 records in the partition.
    base.verify_share_partition_lag(admin_client.as_ref(), &group_id, tp, 65).await;

    close_producer(producer).await;
    close_admin(admin_client).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_share_partition_lag_after_delete_share_group_offsets() {
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group_id = base.group("group1");
    base.alter_share_auto_offset_reset(&group_id, "earliest").await;
    let producer = base.create_producer().await;
    let admin_client = base.create_admin_client();
    let tp = &base.tp;

    // Producing 5 records to the topic partition.
    for _ in 0..5 {
        send_record(&producer, message_record(tp.topic(), tp.partition()))
            .await
            .expect("send");
    }
    producer.flush().await.expect("flush");
    // Create a new share consumer.
    let mut share_consumer = base.create_share_consumer_with_properties(&group_id, &explicit());
    share_consumer.subscribe(vec![tp.topic().to_string()]).await.expect("subscribe");
    // Polling share consumer to make sure it joins the group and consumes the produced records.
    let records = base.waited_poll(&mut share_consumer, 2500, 5).await;
    assert_eq!(5, records.count());
    // Accept the records first to move the offset forward and register the state with persister.
    for r in &records {
        share_consumer
            .acknowledge_with_record_type(r, AcknowledgeType::Accept)
            .expect("acknowledge");
    }
    share_consumer.commit_sync().await.expect("commitSync");
    // After accepting, the lag should be 0 because the record is consumed successfully.
    base.verify_share_partition_lag(admin_client.as_ref(), &group_id, tp, 0).await;
    // Closing the share consumer so that the offsets can be deleted.
    close_share_consumer(share_consumer).await;
    // Delete the share group offsets.
    base.delete_share_group_offsets(admin_client.as_ref(), &group_id, tp.topic())
        .await
        .unwrap_or_else(|e| panic!("Test failed with exception: {e}"));
    // Verify that the share partition offsets are deleted.
    base.verify_share_partition_offsets_deleted(admin_client.as_ref(), &group_id, tp)
        .await;
    // Create a new share consumer.
    let mut share_consumer2 = base.create_share_consumer_with_properties(&group_id, &explicit());
    share_consumer2
        .subscribe(vec![tp.topic().to_string()])
        .await
        .expect("subscribe");
    // Since the offsets are deleted, the share consumer should consume from the beginning (share.auto.offset.reset is earliest).
    // Thus, the consumer should consume all 5 records again.
    let records = base.waited_poll(&mut share_consumer2, 2500, 5).await;
    assert_eq!(5, records.count());
    // Accept the records first to move the offset forward and register the state with persister.
    for r in &records {
        share_consumer2
            .acknowledge_with_record_type(r, AcknowledgeType::Accept)
            .expect("acknowledge");
    }
    share_consumer2.commit_sync().await.expect("commitSync");
    // After accepting, the lag should be 0 because the records are consumed successfully.
    base.verify_share_partition_lag(admin_client.as_ref(), &group_id, tp, 0).await;
    // Closing the share consumer so that the offsets can be deleted.
    close_share_consumer(share_consumer2).await;

    close_producer(producer).await;
    close_admin(admin_client).await;
}
