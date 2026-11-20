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
//! `org.apache.kafka.clients.consumer.ShareConsumerDeliveryTest` (Apache Kafka
//! 4.4.0, `clients-integration-tests`): `share.auto.offset.reset`, delivery
//! counts, isolation levels, the acquire modes and delivery throttling.
//!
//! The fixture is [`super::share_consumer_test_base`]; its module docs list the
//! deviations every share suite shares (pooled clusters, per-test names, the
//! share-state check). Per-test `@ClusterTest(serverProperties = ...)` are
//! added to the class defaults, as `ClusterTestExtensions` merges them (a
//! method's key replaces a default of the same key).
//!
//! # Deviations
//!
//! - **Transactional ids.** Java's isolation-level tests use the transactional
//!   id `T1` on a fresh cluster. Tests share pooled clusters here, so the id is
//!   prefixed per test like the group names; a shared `T1` would fence the
//!   producers of concurrent tests.
//!
//! # Broker-version gates
//!
//! `testDynamicDeliveryCountLimitDecreaseArchivesRecords`,
//! `testDynamicDeliveryCountLimitIncreaseAllowsMoreDeliveries` and
//! `testDynamicPartitionMaxRecordLocks` set the group configs
//! `share.delivery.count.limit` / `share.partition.max.record.locks`, which a
//! broker older than 4.3 rejects (`GroupConfig#validateNames`), so they skip
//! themselves there ([`SHARE_GROUP_CONFIGS_RELEASE`]).

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use confluent_kafka::admin::AlterConfigOp;
use confluent_kafka::admin::ConfigEntry;
use confluent_kafka::admin::OpType;
use confluent_kafka::admin::RecordsToDelete;
use confluent_kafka::common::config::ConfigResource;
use confluent_kafka::common::config::config_resource;
use confluent_kafka::consumer::AcknowledgeType;
use confluent_kafka::consumer::ShareConsumer;
use confluent_kafka::producer::Producer;
use confluent_kafka::producer::ProducerRecord;

use super::share_consumer_test_base::*;
use crate::common::cluster_config::ClusterConfig;

/// A `key`/`value` record for `base.tp`.
fn key_value_record(base: &ShareConsumerTestBase) -> ProducerRecord<Vec<u8>, Vec<u8>> {
    producer_record(
        base.tp.topic().to_string(),
        Some(base.tp.partition()),
        Some(b"key".to_vec()),
        Some(b"value".to_vec()),
    )
}

/// A `key`/`Message <i>` record for `base.tp`.
fn message_record(base: &ShareConsumerTestBase, i: usize) -> ProducerRecord<Vec<u8>, Vec<u8>> {
    producer_record(
        base.tp.topic().to_string(),
        Some(base.tp.partition()),
        Some(b"key".to_vec()),
        Some(format!("Message {i}").into_bytes()),
    )
}

/// `share.acknowledgement.mode=explicit` plus `extra`.
fn explicit_with(extra: &[(&str, &str)]) -> HashMap<String, String> {
    let mut props = HashMap::from([("share.acknowledgement.mode".to_string(), EXPLICIT.to_string())]);
    props.extend(extra.iter().map(|(k, v)| ((*k).to_string(), (*v).to_string())));
    props
}

/// `share.acknowledgement.mode=explicit`.
fn explicit() -> HashMap<String, String> {
    explicit_with(&[])
}

/// The cluster of a `@ClusterTest(serverProperties =
/// group.share.delivery.count.limit=<limit>)`.
fn delivery_count_limit_config(limit: &str) -> ClusterConfig {
    share_cluster_config_with(&[("KAFKA_GROUP_SHARE_DELIVERY_COUNT_LIMIT", limit)])
}

fn value_string(record: &BytesRecord) -> String {
    String::from_utf8(record.value().expect("a value").clone()).expect("UTF-8 value")
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("after the epoch")
        .as_millis() as i64
}

/// Asserts the commit has one entry, the successful one of `tp`.
fn assert_commit_succeeded(
    base: &ShareConsumerTestBase,
    result: &HashMap<confluent_kafka::common::TopicIdPartition, Option<confluent_kafka::common::Error>>,
) {
    assert_eq!(1, result.len());
    assert!(
        result.get(&base.tip()).expect("tp in the commit result").is_none(),
        "{result:?}"
    );
}

/// Java's `assertTrue(records.isEmpty(), "Records should be empty as all
/// records have reached delivery limit. ...")` after a 2.5 s poll.
async fn assert_delivery_limit_reached(share_consumer: &mut BytesShareConsumer) {
    let records = share_consumer.poll(Duration::from_millis(2500)).await.expect("poll");
    assert!(
        records.is_empty(),
        "Records should be empty as all records have reached delivery limit. But received: {}",
        records.count()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_share_auto_offset_reset_default_value() {
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group1 = base.group("group1");
    let mut share_consumer = base.create_share_consumer(&group1);
    let producer = base.create_producer().await;

    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");
    // Producing a record.
    send_record(&producer, key_value_record(&base)).await.expect("send");
    producer.flush().await.expect("flush");
    let records = base
        .waited_poll_with_assignment(&mut share_consumer, 2500, 0, true, &group1, std::slice::from_ref(&base.tp))
        .await;
    // No records should be consumed because share.auto.offset.reset has a default of "latest". Since the record
    // was produced before share partition was initialized (which happens after the first share fetch request
    // in the poll method), the start offset would be the latest offset, i.e. 1 (the next offset after the already
    // present 0th record)
    assert_eq!(0, records.count());
    // Producing another record.
    send_record(&producer, key_value_record(&base)).await.expect("send");
    producer.flush().await.expect("flush");
    let records = share_consumer.poll(Duration::from_millis(5000)).await.expect("poll");
    // Now the next record should be consumed successfully
    assert_eq!(1, records.count());
    base.verify_share_group_state_topic_records_produced().await;
    close_share_consumer(share_consumer).await;
    close_producer(producer).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_share_auto_offset_reset_earliest() {
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let mut share_consumer = base.create_share_consumer(&group1);
    let producer = base.create_producer().await;

    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");
    // Producing a record.
    send_record(&producer, key_value_record(&base)).await.expect("send");
    producer.flush().await.expect("flush");
    let records = base.waited_poll(&mut share_consumer, 2500, 1).await;
    // Since the value for share.auto.offset.reset has been altered to "earliest", the consumer should consume
    // all messages present on the partition
    assert_eq!(1, records.count());
    // Producing another record.
    send_record(&producer, key_value_record(&base)).await.expect("send");
    producer.flush().await.expect("flush");
    let records = share_consumer.poll(Duration::from_millis(5000)).await.expect("poll");
    // The next records should also be consumed successfully
    assert_eq!(1, records.count());
    base.verify_share_group_state_topic_records_produced().await;
    close_share_consumer(share_consumer).await;
    close_producer(producer).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_share_auto_offset_reset_earliest_after_lso_movement() {
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let producer = base.create_producer().await;
    let admin_client = base.create_admin_client();

    // We write 10 records to the topic, so they would be written from offsets 0-9 on the topic.
    for _ in 0..10 {
        let future = send_record(&producer, key_value_record(&base))
            .await
            .unwrap_or_else(|e| panic!("Failed to send records: {e}"));
        future.get().await.unwrap_or_else(|e| panic!("Failed to send records: {e}"));
    }

    // We delete records before offset 5, so the LSO should move to 5.
    admin_client
        .delete_records(&HashMap::from([(base.tp.clone(), RecordsToDelete::with_offset(5))]))
        .all()
        .get()
        .await
        .unwrap_or_else(|e| panic!("Failed to delete records: {e}"));

    let consumed_message_count = base.consume_messages(&AtomicUsize::new(0), 5, &group1, 1, 10, true).await;
    // The records returned belong to offsets 5-9.
    assert_eq!(5, consumed_message_count);
    base.verify_share_group_state_topic_records_produced().await;
    close_producer(producer).await;
    close_admin(admin_client).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_share_auto_offset_reset_multiple_groups_with_different_value() {
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group1 = base.group("group1");
    let group2 = base.group("group2");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    base.alter_share_auto_offset_reset(&group2, "latest").await;
    let mut share_consumer_earliest = base.create_share_consumer(&group1);
    let mut share_consumer_latest = base.create_share_consumer(&group2);
    let producer = base.create_producer().await;

    share_consumer_earliest
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");
    share_consumer_latest
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");

    // Producing a record.
    send_record(&producer, key_value_record(&base)).await.expect("send");
    producer.flush().await.expect("flush");
    let records1 = base.waited_poll(&mut share_consumer_earliest, 2500, 1).await;
    // Since the value for share.auto.offset.reset has been altered to "earliest", the consumer should consume
    // all messages present on the partition
    assert_eq!(1, records1.count());

    let records2 = base
        .waited_poll_with_assignment(
            &mut share_consumer_latest,
            2500,
            0,
            true,
            &group2,
            std::slice::from_ref(&base.tp),
        )
        .await;
    // Since the value for share.auto.offset.reset has been altered to "latest", the consumer should not consume
    // any message
    assert_eq!(0, records2.count());

    // Producing another record.
    send_record(&producer, key_value_record(&base)).await.expect("send");

    let records1 = share_consumer_earliest.poll(Duration::from_millis(5000)).await.expect("poll");
    // The next record should also be consumed successfully by group1
    assert_eq!(1, records1.count());

    let records2 = share_consumer_latest.poll(Duration::from_millis(5000)).await.expect("poll");
    // The next record should also be consumed successfully by group2
    assert_eq!(1, records2.count());
    base.verify_share_group_state_topic_records_produced().await;
    close_share_consumer(share_consumer_earliest).await;
    close_share_consumer(share_consumer_latest).await;
    close_producer(producer).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_share_auto_offset_reset_by_duration() {
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group1 = base.group("group1");
    // Set auto offset reset to 1 hour before current time
    base.alter_share_auto_offset_reset(&group1, "by_duration:PT1H").await;

    let mut share_consumer = base.create_share_consumer(&group1);
    let producer = base.create_producer().await;

    let current_time = now_ms();
    let two_hours_ago = current_time - Duration::from_secs(2 * 3600).as_millis() as i64;
    let thirty_mins_ago = current_time - Duration::from_secs(30 * 60).as_millis() as i64;

    // Produce messages with different timestamps
    let topic = base.tp.topic().to_string();
    let partition = Some(base.tp.partition());
    let old_record = producer_record_with_timestamp(
        topic.clone(),
        partition,
        Some(two_hours_ago),
        Some(b"old_key".to_vec()),
        Some(b"old_value".to_vec()),
    );
    let recent_record = producer_record_with_timestamp(
        topic.clone(),
        partition,
        Some(thirty_mins_ago),
        Some(b"recent_key".to_vec()),
        Some(b"recent_value".to_vec()),
    );
    let current_record = producer_record_with_timestamp(
        topic,
        partition,
        Some(current_time),
        Some(b"current_key".to_vec()),
        Some(b"current_value".to_vec()),
    );

    for record in [old_record, recent_record, current_record] {
        send_record(&producer, record)
            .await
            .expect("send")
            .get()
            .await
            .expect("send result");
    }
    producer.flush().await.expect("flush");

    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");

    // Should only receive messages from last hour (recent and current)
    let records = consume_records(&mut share_consumer, 2).await;
    assert_eq!(2, records.len());

    // Verify timestamps and order
    assert_eq!(thirty_mins_ago, records[0].timestamp());
    assert_eq!(Some(&b"recent_key".to_vec()), records[0].key());
    assert_eq!(current_time, records[1].timestamp());
    assert_eq!(Some(&b"current_key".to_vec()), records[1].key());
    close_share_consumer(share_consumer).await;
    close_producer(producer).await;

    // Set the auto offset reset to 3 hours before current time
    // so the consumer should consume all messages (3 records)
    let group2 = base.group("group2");
    base.alter_share_auto_offset_reset(&group2, "by_duration:PT3H").await;
    let mut share_consumer = base.create_share_consumer(&group2);
    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");
    let records = consume_records(&mut share_consumer, 3).await;
    assert_eq!(3, records.len());
    base.verify_share_group_state_topic_records_produced().await;
    close_share_consumer(share_consumer).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_share_auto_offset_reset_by_duration_invalid_format() {
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    // Test invalid duration format
    let config_resource = ConfigResource::new(config_resource::Type::Group, base.group("group1"));

    // Test invalid duration format
    let admin_client = base.create_admin_client();
    for invalid in ["by_duration:1h", "by_duration:-PT1H"] {
        let alter_entries = HashMap::from([(
            config_resource.clone(),
            vec![AlterConfigOp::new(
                ConfigEntry::new("share.auto.offset.reset".to_string(), Some(invalid.to_string())),
                OpType::Set,
            )],
        )]);
        let error = admin_client
            .incremental_alter_configs(&alter_entries)
            .all()
            .get()
            .await
            .expect_err("an invalid share.auto.offset.reset is rejected");
        assert!(error.is_invalid_configuration_error(), "{invalid}: {error:?}");
        // The broker's text (`ShareGroupAutoOffsetResetStrategy.Validator`), the
        // same on 4.2.0 and 4.4.0; Java asserts only the class.
        assert_eq!(
            format!(
                "Invalid value {invalid} for configuration share.auto.offset.reset: Invalid value `{invalid}` for \
                 configuration share.auto.offset.reset. The value must be either 'earliest', 'latest' or of the \
                 format 'by_duration:<PnDTnHnMn.nS>'."
            ),
            error.message()
        );
    }
    close_admin(admin_client).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_delivery_count_different_behavior_when_closing_session_with_explicit_acknowledgement() {
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let producer = base.create_producer().await;
    let mut share_consumer = base.create_share_consumer_with_properties(&group1, &explicit());

    send_record(&producer, key_value_record(&base)).await.expect("send");
    send_record(&producer, key_value_record(&base)).await.expect("send");
    producer.flush().await.expect("flush");

    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");
    let records = base.waited_poll(&mut share_consumer, 2500, 2).await;
    assert_eq!(2, records.count());
    // Acknowledge the first record with AcknowledgeType.RELEASE
    share_consumer
        .acknowledge_with_record_type(&records.records_with_partition(&base.tp)[0], AcknowledgeType::Release)
        .expect("acknowledge");
    let result = share_consumer.commit_sync().await.expect("commitSync");
    assert_eq!(1, result.len());
    close_producer(producer).await;
    close_share_consumer(share_consumer).await;

    // Test delivery count
    let mut share_consumer = base.create_share_consumer_with_properties(&group1, &HashMap::new());
    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");
    let records = base.waited_poll(&mut share_consumer, 2500, 2).await;
    assert_eq!(2, records.count());
    assert_eq!(Some(2), records.records_with_partition(&base.tp)[0].delivery_count());
    assert_eq!(Some(2), records.records_with_partition(&base.tp)[1].delivery_count());
    close_share_consumer(share_consumer).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_behavior_on_delivery_count_boundary() {
    let base = ShareConsumerTestBase::setup(delivery_count_limit_config("3")).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let producer = base.create_producer().await;
    let mut share_consumer = base.create_share_consumer_with_properties(&group1, &explicit());

    send_record(&producer, key_value_record(&base)).await.expect("send");
    producer.flush().await.expect("flush");

    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");
    let records = base.waited_poll(&mut share_consumer, 2500, 1).await;
    assert_eq!(1, records.count());
    assert_eq!(Some(1), records.records_with_partition(&base.tp)[0].delivery_count());
    // Acknowledge the record with AcknowledgeType.RELEASE.
    share_consumer
        .acknowledge_with_record_type(&records.records_with_partition(&base.tp)[0], AcknowledgeType::Release)
        .expect("acknowledge");
    let result = share_consumer.commit_sync().await.expect("commitSync");
    assert_eq!(1, result.len());

    // Consume again, the delivery count should be 2.
    let records = base.waited_poll(&mut share_consumer, 2500, 1).await;
    assert_eq!(1, records.count());
    assert_eq!(Some(2), records.records_with_partition(&base.tp)[0].delivery_count());
    close_producer(producer).await;
    close_share_consumer(share_consumer).await;

    // Start again and same record should be delivered
    let mut share_consumer = base.create_share_consumer_with_properties(&group1, &HashMap::new());
    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");
    let records = base.waited_poll(&mut share_consumer, 2500, 1).await;
    assert_eq!(1, records.count());
    assert_eq!(Some(3), records.records_with_partition(&base.tp)[0].delivery_count());
    close_share_consumer(share_consumer).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_read_committed_isolation_level() {
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    base.alter_share_isolation_level(&group1, "read_committed").await;
    let transactional_producer = base.create_producer_with_transactional_id(&base.group("T1"));
    let mut share_consumer = base.create_share_consumer(&group1);
    base.produce_committed_and_aborted_transactions_in_interval(transactional_producer, 10, 5)
        .await;
    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");
    let records = base.waited_poll(&mut share_consumer, 5000, 8).await;
    // 5th and 10th message transaction was aborted, hence they won't be included in the fetched records.
    assert_eq!(8, records.count());
    let mut message_counter = 1;
    for record in &records {
        assert_eq!(base.tp.topic(), record.topic());
        assert_eq!(base.tp.partition(), record.partition());
        if message_counter % 5 == 0 {
            message_counter += 1;
        }
        assert_eq!(format!("Message {message_counter}"), value_string(record));
        message_counter += 1;
    }
    close_share_consumer(share_consumer).await;
    base.verify_share_group_state_topic_records_produced().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_read_uncommitted_isolation_level() {
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    base.alter_share_isolation_level(&group1, "read_uncommitted").await;
    let transactional_producer = base.create_producer_with_transactional_id(&base.group("T1"));
    let mut share_consumer = base.create_share_consumer(&group1);
    base.produce_committed_and_aborted_transactions_in_interval(transactional_producer, 10, 5)
        .await;
    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");
    let records = base.waited_poll(&mut share_consumer, 5000, 10).await;
    // Even though 5th and 10th message transaction was aborted, they will be included in the fetched records since IsolationLevel is READ_UNCOMMITTED.
    assert_eq!(10, records.count());
    for (message_counter, record) in (1..).zip(&records) {
        assert_eq!(base.tp.topic(), record.topic());
        assert_eq!(base.tp.partition(), record.partition());
        assert_eq!(format!("Message {message_counter}"), value_string(record));
    }
    close_share_consumer(share_consumer).await;
    base.verify_share_group_state_topic_records_produced().await;
}

/// Acknowledges every record of `records` with `acknowledge_type`.
fn acknowledge_all(
    share_consumer: &mut BytesShareConsumer,
    records: &confluent_kafka::consumer::ConsumerRecords<Vec<u8>, Vec<u8>>,
    acknowledge_type: AcknowledgeType,
) {
    for record in records {
        share_consumer
            .acknowledge_with_record_type(record, acknowledge_type)
            .expect("acknowledge");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_alter_read_uncommitted_to_read_committed_isolation_level() {
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    base.alter_share_isolation_level(&group1, "read_uncommitted").await;
    let transactional_producer = base.create_producer_with_transactional_id(&base.group("T1"));
    let mut share_consumer = base.create_share_consumer_with_properties(&group1, &explicit());
    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");
    transactional_producer.init_transactions().await.expect("initTransactions");

    // First transaction is committed.
    base.produce_committed_transaction(&transactional_producer, "Message 1").await;

    let records = base.waited_poll(&mut share_consumer, 5000, 1).await;
    assert_eq!(1, records.count());
    let record = (&records).into_iter().next().expect("a record");
    assert_eq!("Message 1", value_string(record));
    assert_eq!(base.tp.topic(), record.topic());
    assert_eq!(base.tp.partition(), record.partition());
    acknowledge_all(&mut share_consumer, &records, AcknowledgeType::Accept);
    share_consumer.commit_sync().await.expect("commitSync");

    // Second transaction is aborted.
    base.produce_aborted_transaction(&transactional_producer, "Message 2").await;

    let records = base.waited_poll(&mut share_consumer, 5000, 1).await;
    assert_eq!(1, records.count());
    let record = (&records).into_iter().next().expect("a record");
    assert_eq!("Message 2", value_string(record));
    acknowledge_all(&mut share_consumer, &records, AcknowledgeType::Accept);
    share_consumer.commit_sync().await.expect("commitSync");

    // Third transaction is committed.
    base.produce_committed_transaction(&transactional_producer, "Message 3").await;
    // Fourth transaction is aborted.
    base.produce_aborted_transaction(&transactional_producer, "Message 4").await;

    let records = base.waited_poll(&mut share_consumer, 5000, 2).await;
    // Message 3 and Message 4 would be returned by this poll.
    assert_eq!(2, records.count());
    let mut record_iterator = (&records).into_iter();
    assert_eq!("Message 3", value_string(record_iterator.next().expect("a record")));
    assert_eq!("Message 4", value_string(record_iterator.next().expect("a record")));
    // We will make Message 3 and Message 4 available for re-consumption.
    acknowledge_all(&mut share_consumer, &records, AcknowledgeType::Release);
    share_consumer.commit_sync().await.expect("commitSync");

    // We are altering IsolationLevel to READ_COMMITTED now. We will only read committed transactions now.
    base.alter_share_isolation_level(&group1, "read_committed").await;

    // Fifth transaction is committed.
    base.produce_committed_transaction(&transactional_producer, "Message 5").await;
    // Sixth transaction is aborted.
    base.produce_aborted_transaction(&transactional_producer, "Message 6").await;
    // Seventh transaction is aborted.
    base.produce_aborted_transaction(&transactional_producer, "Message 7").await;
    // Eighth transaction is committed.
    base.produce_committed_transaction(&transactional_producer, "Message 8").await;

    // Since isolation level is READ_COMMITTED, we can consume Message 3 (committed transaction that was released), Message 5 and Message 8.
    // We cannot consume Message 4 (aborted transaction that was released), Message 6 and Message 7 since they were aborted.
    let mut messages = Vec::new();
    let mut deadline = WaitDeadline::new(
        DEFAULT_MAX_WAIT_MS,
        100,
        "Failed to consume all records post altering share isolation level",
    );
    loop {
        let poll_records = share_consumer.poll(Duration::from_millis(5000)).await.expect("poll");
        if poll_records.count() > 0 {
            messages.extend((&poll_records).into_iter().map(value_string));
            acknowledge_all(&mut share_consumer, &poll_records, AcknowledgeType::Accept);
            share_consumer.commit_sync().await.expect("commitSync");
        }
        if messages.len() == 3 {
            break;
        }
        deadline.fail_or_pause().await;
    }

    assert_eq!("Message 3", messages[0]);
    assert_eq!("Message 5", messages[1]);
    assert_eq!("Message 8", messages[2]);
    close_producer(transactional_producer).await;
    close_share_consumer(share_consumer).await;
    base.verify_share_group_state_topic_records_produced().await;
}

/// Polls (500 ms each) until `offsets_map` holds `offset` of `tp`, with
/// `on_records` applied to every non-empty poll, failing with `details` after
/// `DEFAULT_MAX_WAIT_MS`. With `require_empty`, a poll that returned records
/// does not satisfy the condition (Java's `pollRecords.count() == 0 && ...`).
async fn wait_for_acknowledged_offset(
    base: &ShareConsumerTestBase,
    share_consumer: &mut BytesShareConsumer,
    offsets_map: &PartitionOffsetsMap,
    offset: i64,
    require_empty: bool,
    release_received: bool,
    details: &str,
) {
    let mut deadline = WaitDeadline::new(DEFAULT_MAX_WAIT_MS, 100, details);
    loop {
        let poll_records = share_consumer.poll(Duration::from_millis(500)).await.expect("poll");
        if release_received && poll_records.count() > 0 {
            // We will release Message 3 again if it was received in this poll().
            acknowledge_all(share_consumer, &poll_records, AcknowledgeType::Release);
        }
        let acknowledged = offsets_map
            .lock()
            .expect("offsets map lock")
            .get(&base.tp)
            .is_some_and(|offsets| offsets.contains(&offset));
        if (!require_empty || poll_records.count() == 0) && acknowledged {
            return;
        }
        deadline.fail_or_pause().await;
    }
}

/// A [`TestableAcknowledgementCommitCallback`] over a fresh offsets map, set on
/// `share_consumer`; returns the map. Java passes an immutable `Map.of()` as
/// the error map, which the callback never writes on success.
fn set_testable_callback(share_consumer: &mut BytesShareConsumer) -> PartitionOffsetsMap {
    let partition_offsets_map: PartitionOffsetsMap = Arc::new(Mutex::new(HashMap::new()));
    share_consumer.set_acknowledgement_commit_callback(Some(TestableAcknowledgementCommitCallback::new(
        Arc::clone(&partition_offsets_map),
        Arc::new(Mutex::new(HashMap::new())),
    )));
    partition_offsets_map
}

#[tokio::test(flavor = "multi_thread")]
async fn test_alter_read_committed_to_read_uncommitted_isolation_level_with_release_ack() {
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    base.alter_share_isolation_level(&group1, "read_committed").await;
    let transactional_producer = base.create_producer_with_transactional_id(&base.group("T1"));
    let mut share_consumer = base.create_share_consumer_with_properties(&group1, &explicit());
    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");
    transactional_producer.init_transactions().await.expect("initTransactions");

    // First transaction is committed.
    base.produce_committed_transaction(&transactional_producer, "Message 1").await;

    let records = base.waited_poll(&mut share_consumer, 5000, 1).await;
    assert_eq!(1, records.count());
    let record = (&records).into_iter().next().expect("a record");
    assert_eq!("Message 1", value_string(record));
    assert_eq!(base.tp.topic(), record.topic());
    assert_eq!(base.tp.partition(), record.partition());
    acknowledge_all(&mut share_consumer, &records, AcknowledgeType::Accept);
    share_consumer.commit_sync().await.expect("commitSync");

    // Second transaction is aborted.
    base.produce_aborted_transaction(&transactional_producer, "Message 2").await;

    // Setting the acknowledgement commit callback to verify acknowledgement completion.
    let partition_offsets_map = set_testable_callback(&mut share_consumer);

    // We will not receive any records since the transaction for Message 2 was aborted. Wait for the
    // aborted marker offset for Message 2 (3L) to be fetched and acknowledged by the consumer.
    wait_for_acknowledged_offset(
        &base,
        &mut share_consumer,
        &partition_offsets_map,
        3,
        true,
        false,
        "Failed to consume abort transaction and marker offset for Message 2",
    )
    .await;

    // Third transaction is committed.
    base.produce_committed_transaction(&transactional_producer, "Message 3").await;
    // Fourth transaction is aborted.
    base.produce_aborted_transaction(&transactional_producer, "Message 4").await;

    // Setting the acknowledgement commit callback to verify acknowledgement completion.
    let partition_offsets_map2 = set_testable_callback(&mut share_consumer);

    let records = base.waited_poll(&mut share_consumer, 5000, 1).await;
    // Message 3 would be returned by this poll.
    assert_eq!(1, records.count());
    assert_eq!("Message 3", value_string((&records).into_iter().next().expect("a record")));
    // We will make Message 3 available for re-consumption.
    acknowledge_all(&mut share_consumer, &records, AcknowledgeType::Release);
    share_consumer.commit_sync().await.expect("commitSync");

    // Wait for the aborted marker offset for Message 4 (7L) to be fetched and acknowledged by the consumer.
    wait_for_acknowledged_offset(
        &base,
        &mut share_consumer,
        &partition_offsets_map2,
        7,
        false,
        true,
        "Failed to consume abort transaction marker offset for Message 4",
    )
    .await;

    // We are altering IsolationLevel to READ_UNCOMMITTED now. We will read both committed/aborted transactions now.
    base.alter_share_isolation_level(&group1, "read_uncommitted").await;

    // Fifth transaction is committed.
    base.produce_committed_transaction(&transactional_producer, "Message 5").await;
    // Sixth transaction is aborted.
    base.produce_aborted_transaction(&transactional_producer, "Message 6").await;
    // Seventh transaction is aborted.
    base.produce_aborted_transaction(&transactional_producer, "Message 7").await;
    // Eighth transaction is committed.
    base.produce_committed_transaction(&transactional_producer, "Message 8").await;

    // Since isolation level is READ_UNCOMMITTED, we can consume Message 3 (committed transaction that was released), Message 5, Message 6, Message 7 and Message 8.
    let mut final_messages = HashSet::new();
    let mut deadline = WaitDeadline::new(
        DEFAULT_MAX_WAIT_MS,
        100,
        "Failed to consume all records post altering share isolation level",
    );
    loop {
        let poll_records = share_consumer.poll(Duration::from_millis(5000)).await.expect("poll");
        if poll_records.count() > 0 {
            final_messages.extend((&poll_records).into_iter().map(value_string));
            acknowledge_all(&mut share_consumer, &poll_records, AcknowledgeType::Accept);
            share_consumer.commit_sync().await.expect("commitSync");
        }
        if final_messages.len() == 5 {
            break;
        }
        deadline.fail_or_pause().await;
    }

    let expected: HashSet<String> = ["Message 3", "Message 5", "Message 6", "Message 7", "Message 8"]
        .into_iter()
        .map(String::from)
        .collect();
    assert_eq!(expected, final_messages);
    close_producer(transactional_producer).await;
    close_share_consumer(share_consumer).await;
    base.verify_share_group_state_topic_records_produced().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_alter_read_committed_to_read_uncommitted_isolation_level_with_reject_ack() {
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    base.alter_share_isolation_level(&group1, "read_committed").await;
    let transactional_producer = base.create_producer_with_transactional_id(&base.group("T1"));
    let mut share_consumer = base.create_share_consumer_with_properties(&group1, &explicit());
    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");
    transactional_producer.init_transactions().await.expect("initTransactions");

    // First transaction is committed.
    base.produce_committed_transaction(&transactional_producer, "Message 1").await;

    let records = base.waited_poll(&mut share_consumer, 5000, 1).await;
    assert_eq!(1, records.count());
    let record = (&records).into_iter().next().expect("a record");
    assert_eq!("Message 1", value_string(record));
    assert_eq!(base.tp.topic(), record.topic());
    assert_eq!(base.tp.partition(), record.partition());
    acknowledge_all(&mut share_consumer, &records, AcknowledgeType::Accept);
    share_consumer.commit_sync().await.expect("commitSync");

    // Second transaction is aborted.
    base.produce_aborted_transaction(&transactional_producer, "Message 2").await;

    // Setting the acknowledgement commit callback to verify acknowledgement completion.
    let partition_offsets_map = set_testable_callback(&mut share_consumer);

    // We will not receive any records since the transaction for Message 2 was aborted. Wait for the
    // aborted marker offset for Message 2 (3L) to be fetched and acknowledged by the consumer.
    wait_for_acknowledged_offset(
        &base,
        &mut share_consumer,
        &partition_offsets_map,
        3,
        true,
        false,
        "Failed to consume abort transaction and marker offset for Message 2",
    )
    .await;

    // Third transaction is committed.
    base.produce_committed_transaction(&transactional_producer, "Message 3").await;
    // Fourth transaction is aborted.
    base.produce_aborted_transaction(&transactional_producer, "Message 4").await;

    // Setting the acknowledgement commit callback to verify acknowledgement completion.
    let partition_offsets_map2 = set_testable_callback(&mut share_consumer);

    let records = base.waited_poll(&mut share_consumer, 5000, 1).await;
    // Message 3 would be returned by this poll.
    assert_eq!(1, records.count());
    assert_eq!("Message 3", value_string((&records).into_iter().next().expect("a record")));
    // We will make Message 3 available for re-consumption.
    acknowledge_all(&mut share_consumer, &records, AcknowledgeType::Reject);
    share_consumer.commit_sync().await.expect("commitSync");

    // Wait for the aborted marker offset for Message 4 (7L) to be fetched and acknowledged by the consumer.
    wait_for_acknowledged_offset(
        &base,
        &mut share_consumer,
        &partition_offsets_map2,
        7,
        false,
        false,
        "Failed to consume abort transaction marker offset for Message 4",
    )
    .await;

    // We are altering IsolationLevel to READ_UNCOMMITTED now. We will read both committed/aborted transactions now.
    base.alter_share_isolation_level(&group1, "read_uncommitted").await;

    // Fifth transaction is committed.
    base.produce_committed_transaction(&transactional_producer, "Message 5").await;
    // Sixth transaction is aborted.
    base.produce_aborted_transaction(&transactional_producer, "Message 6").await;
    // Seventh transaction is aborted.
    base.produce_aborted_transaction(&transactional_producer, "Message 7").await;
    // Eighth transaction is committed.
    base.produce_committed_transaction(&transactional_producer, "Message 8").await;

    // Since isolation level is READ_UNCOMMITTED, we can consume Message 5, Message 6, Message 7 and Message 8.
    let mut final_messages = Vec::new();
    let mut deadline = WaitDeadline::new(
        DEFAULT_MAX_WAIT_MS,
        100,
        "Failed to consume all records post altering share isolation level",
    );
    loop {
        let poll_records = share_consumer.poll(Duration::from_millis(5000)).await.expect("poll");
        if poll_records.count() > 0 {
            final_messages.extend((&poll_records).into_iter().map(value_string));
            acknowledge_all(&mut share_consumer, &poll_records, AcknowledgeType::Accept);
            share_consumer.commit_sync().await.expect("commitSync");
        }
        if final_messages.len() == 4 {
            break;
        }
        deadline.fail_or_pause().await;
    }

    assert_eq!("Message 5", final_messages[0]);
    assert_eq!("Message 6", final_messages[1]);
    assert_eq!("Message 7", final_messages[2]);
    assert_eq!("Message 8", final_messages[3]);
    close_producer(transactional_producer).await;
    close_share_consumer(share_consumer).await;
    base.verify_share_group_state_topic_records_produced().await;
}

/// Sends `count` `key`/`value` records to `base.tp` and flushes.
async fn send_key_value_records(base: &ShareConsumerTestBase, producer: &BytesProducer, count: usize) {
    for _ in 0..count {
        send_record(producer, key_value_record(base)).await.expect("send");
    }
    producer.flush().await.expect("flush");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_poll_in_batch_optimized_mode() {
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let producer = base.create_producer().await;
    let mut share_consumer = base.create_share_consumer_with_properties(
        &group1,
        &HashMap::from([
            ("max.poll.records".to_string(), "5".to_string()),
            ("share.acquire.mode".to_string(), BATCH_OPTIMIZED.to_string()),
        ]),
    );
    send_key_value_records(&base, &producer, 10).await;
    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");

    // although max.poll.records is set to 5, in batch optimized mode we will still get all 10 records.
    let records = base.waited_poll(&mut share_consumer, 2500, 10).await;
    assert_eq!(10, records.count());
    base.verify_share_group_state_topic_records_produced().await;
    close_producer(producer).await;
    close_share_consumer(share_consumer).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_poll_in_record_limit_mode() {
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let producer = base.create_producer().await;
    let mut share_consumer = base.create_share_consumer_with_properties(
        &group1,
        &HashMap::from([
            ("max.poll.records".to_string(), "5".to_string()),
            ("share.acquire.mode".to_string(), RECORD_LIMIT.to_string()),
        ]),
    );
    send_key_value_records(&base, &producer, 10).await;
    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");

    // In record limit mode we will get only up to max.poll.records number of records.
    let records = base.waited_poll(&mut share_consumer, 2500, 5).await;
    assert_eq!(5, records.count());
    base.verify_share_group_state_topic_records_produced().await;
    close_producer(producer).await;
    close_share_consumer(share_consumer).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_poll_and_explicit_acknowledge_single_message_in_record_limit_mode() {
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let producer = base.create_producer().await;
    let mut share_consumer = base.create_share_consumer_with_properties(
        &group1,
        &explicit_with(&[("max.poll.records", "1"), ("share.acquire.mode", RECORD_LIMIT)]),
    );
    send_key_value_records(&base, &producer, 10).await;
    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");

    for _ in 0..10 {
        let records = base.waited_poll(&mut share_consumer, 2500, 1).await;
        assert_eq!(1, records.count());
        acknowledge_all(&mut share_consumer, &records, AcknowledgeType::Accept);
        share_consumer.commit_sync().await.expect("commitSync");
    }
    base.verify_share_group_state_topic_records_produced().await;
    close_producer(producer).await;
    close_share_consumer(share_consumer).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_explicit_acknowledge_success_in_record_limit_mode() {
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let producer = base.create_producer().await;
    let mut share_consumer = base.create_share_consumer_with_properties(
        &group1,
        &explicit_with(&[("max.poll.records", "10"), ("share.acquire.mode", RECORD_LIMIT)]),
    );
    send_key_value_records(&base, &producer, 15).await;
    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");
    let records = base.waited_poll(&mut share_consumer, 2500, 10).await;
    assert_eq!(10, records.count());
    acknowledge_all(&mut share_consumer, &records, AcknowledgeType::Accept);
    share_consumer.commit_sync().await.expect("commitSync");

    let records = base.waited_poll(&mut share_consumer, 2500, 5).await;
    assert_eq!(5, records.count());
    base.verify_share_group_state_topic_records_produced().await;
    close_producer(producer).await;
    close_share_consumer(share_consumer).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_explicit_acknowledge_release_accept_in_record_limit_mode() {
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let producer = base.create_producer().await;
    let mut share_consumer = base.create_share_consumer_with_properties(
        &group1,
        &explicit_with(&[("max.poll.records", "10"), ("share.acquire.mode", RECORD_LIMIT)]),
    );
    for i in 0..20 {
        let record = producer_record(
            base.tp2.topic().to_string(),
            Some(base.tp2.partition()),
            Some(b"key".to_vec()),
            Some(format!("Message {i}").into_bytes()),
        );
        send_record(&producer, record).await.expect("send");
    }
    producer.flush().await.expect("flush");
    share_consumer
        .subscribe(vec![base.tp2.topic().to_string()])
        .await
        .expect("subscribe");
    let records = base.waited_poll(&mut share_consumer, 2500, 10).await;
    assert_eq!(10, records.count());

    for (count, record) in (&records).into_iter().enumerate() {
        if count % 2 == 0 {
            share_consumer
                .acknowledge_with_record_type(record, AcknowledgeType::Accept)
                .expect("acknowledge");
        } else {
            share_consumer
                .acknowledge_with_record_type(record, AcknowledgeType::Release)
                .expect("acknowledge");
        }
        let result = share_consumer.commit_sync().await.expect("commitSync");
        assert_eq!(1, result.len());
    }

    // Poll again to get 10 records.
    let records = base.waited_poll(&mut share_consumer, 2500, 10).await;
    assert_eq!(10, records.count());
    acknowledge_all(&mut share_consumer, &records, AcknowledgeType::Accept);

    // Get the rest of all 5 records.
    let records = base.waited_poll(&mut share_consumer, 2500, 5).await;
    assert_eq!(5, records.count());
    base.verify_share_group_state_topic_records_produced().await;
    close_producer(producer).await;
    close_share_consumer(share_consumer).await;
}

/// Sends `count` `key`/`Message <i>` records to `base.tp` and flushes.
async fn send_message_records(base: &ShareConsumerTestBase, producer: &BytesProducer, count: usize) {
    for i in 0..count {
        send_record(producer, message_record(base, i)).await.expect("send");
    }
    producer.flush().await.expect("flush");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fetch_with_throttled_delivery() {
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let producer = base.create_producer().await;
    let mut share_consumer = base.create_share_consumer_with_properties(&group1, &explicit());
    // Produce a batch of 100 messages
    send_message_records(&base, &producer, 100).await;

    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");
    // Fetch records in 5 iterations, each time acknowledging with RELEASE. 5 is the default
    // delivery limit hence we should see throttling from Math.ceil(5/2) = 3 fetches.
    let throttle_delivery_limit = 3;
    for i in 0..5 {
        // Adjust expected fetch count based on throttling. If i < throttleDeliveryLimit, we get full batch of 100.
        // If i == 4 i.e. the last delivery, then we get 1 record.
        // Otherwise, we get half the previous fetch count due to throttling. In this case, 100 >> (i - throttleDeliveryLimit + 1) it is 50 for i=3.
        let expected_fetch_count = if i < throttle_delivery_limit {
            100
        } else if i == 4 {
            1
        } else {
            50
        };
        let records = base.waited_poll(&mut share_consumer, 2500, expected_fetch_count).await;
        assert_eq!(expected_fetch_count, records.count());

        acknowledge_all(&mut share_consumer, &records, AcknowledgeType::Release);
        let result = share_consumer.commit_sync().await.expect("commitSync");
        assert_commit_succeeded(&base, &result);
    }

    // Offset 0 has already reached the delivery limit hence shall be archived.
    // Offset 1 to 49 shall be in last delivery attempt and hence 1 record per poll.
    base.validate_expected_records_in_each_poll_and_release(&mut share_consumer, 1, 50, 1)
        .await;
    // Delivery limit 4.
    base.validate_expected_records_in_each_poll_and_release(&mut share_consumer, 50, 100, 50)
        .await;
    // Delivery limit 5.
    base.validate_expected_records_in_each_poll_and_release(&mut share_consumer, 50, 100, 1)
        .await;
    // Next poll should not have any records as all records have reached delivery limit.
    assert_delivery_limit_reached(&mut share_consumer).await;
    close_producer(producer).await;
    close_share_consumer(share_consumer).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fetch_with_throttled_delivery_batches_with_increased_delivery_limit() {
    let base = ShareConsumerTestBase::setup(delivery_count_limit_config("10")).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let producer = base.create_producer().await;
    let mut share_consumer =
        base.create_share_consumer_with_properties(&group1, &explicit_with(&[("max.poll.records", "512")]));
    // Produce records in complete power of 2 to fully test the throttling behavior.
    let produced_message_count = 512;
    // Produce a batch of 512 messages
    send_message_records(&base, &producer, produced_message_count).await;

    // Map which defines expected fetch count for each delivery attempt from 1 to 10.
    let expected_fetch_count_map: HashMap<usize, usize> = HashMap::from([
        (1, 512),
        (2, 512),
        (3, 512),
        (4, 512),
        (5, 512),
        (6, 256),
        (7, 128),
        (8, 64),
        (9, 32),
        (10, 1),
    ]);
    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");
    // Fetch records in 10 iterations, each time acknowledging with RELEASE. 10 is the
    // delivery limit hence we should see throttling from Math.ceil(10/2) = 5 fetches.
    for i in 0..10 {
        let expected_fetch_count = expected_fetch_count_map[&(i + 1)];
        let records = base.waited_poll(&mut share_consumer, 2500, expected_fetch_count).await;
        assert_eq!(expected_fetch_count, records.count());
        // Acknowledge all records with RELEASE.
        acknowledge_all(&mut share_consumer, &records, AcknowledgeType::Release);
        let result = share_consumer.commit_sync().await.expect("commitSync");
        assert_commit_succeeded(&base, &result);
    }

    // Offset 0 is already verified above, so start from offset 1 and as it's last delivery cycle
    // hence expectedRecords is 1 for each poll till offset 32.
    // The (start offset, last offset, records in each poll) steps of Java's
    // validateExpectedRecordsInEachPollAndRelease calls, with its delivery comments.
    const STEPS: &[(i64, i64, usize)] = &[
        (1, 32, 1),      // Delivery 10
        (32, 64, 32),    // Delivery 9
        (32, 64, 1),     // Delivery 10
        (64, 128, 64),   // Delivery 8
        (64, 96, 32),    // Delivery 9
        (64, 96, 1),     // Delivery 10
        (96, 128, 32),   // Delivery 9
        (96, 128, 1),    // Delivery 10
        (128, 256, 128), // Delivery 7
        (128, 192, 64),  // Delivery 8
        (128, 160, 32),  // Delivery 9
        (128, 160, 1),   // Delivery 10
        (160, 192, 32),  // Delivery 9
        (160, 192, 1),   // Delivery 10
        (192, 256, 64),  // Delivery 8
        (192, 224, 32),  // Delivery 9
        (192, 224, 1),   // Delivery 10
        (224, 256, 32),  // Delivery 9
        (224, 256, 1),   // Delivery 10
        (256, 512, 256), // Delivery 6
        (256, 384, 128), // Delivery 7
        (256, 320, 64),  // Delivery 8
        (256, 288, 32),  // Delivery 9
        (256, 288, 1),   // Delivery 10
        (288, 320, 32),  // Delivery 9
        (288, 320, 1),   // Delivery 10
        (320, 384, 64),  // Delivery 8
        (320, 352, 32),  // Delivery 9
        (320, 352, 1),   // Delivery 10
        (352, 384, 32),  // Delivery 9
        (352, 384, 1),   // Delivery 10
        (384, 512, 128), // Delivery 7
        (384, 448, 64),  // Delivery 8
        (384, 416, 32),  // Delivery 9
        (384, 416, 1),   // Delivery 10
        (416, 448, 32),  // Delivery 9
        (416, 448, 1),   // Delivery 10
        (448, 512, 64),  // Delivery 8
        (448, 480, 32),  // Delivery 9
        (448, 480, 1),   // Delivery 10
        (480, 512, 32),  // Delivery 9
        (480, 512, 1),   // Delivery 10
    ];
    for &(start_offset, last_offset, expected_records_in_each_poll) in STEPS {
        base.validate_expected_records_in_each_poll_and_release(
            &mut share_consumer,
            start_offset,
            last_offset,
            expected_records_in_each_poll,
        )
        .await;
    }
    // Next poll should not have any records as all records have reached delivery limit.
    assert_delivery_limit_reached(&mut share_consumer).await;
    close_producer(producer).await;
    close_share_consumer(share_consumer).await;
}

/// Counts one more delivery of every record of `records` in
/// `offset_to_delivery_count_map`.
fn count_deliveries(
    offset_to_delivery_count_map: &mut HashMap<i64, i32>,
    records: &confluent_kafka::consumer::ConsumerRecords<Vec<u8>, Vec<u8>>,
) {
    for record in records {
        *offset_to_delivery_count_map.entry(record.offset()).or_insert(0) += 1;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fetch_with_throttled_delivery_validate_delivery_count() {
    let base = ShareConsumerTestBase::setup(delivery_count_limit_config("10")).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let producer = base.create_producer().await;
    let mut share_consumer = base.create_share_consumer_with_properties(&group1, &explicit());
    let produced_message_count = 500;
    // Produce a batch of 500 messages
    send_message_records(&base, &producer, produced_message_count).await;

    // Map to track delivery count for each offset.
    let mut offset_to_delivery_count_map: HashMap<i64, i32> = HashMap::new();
    // Map which defines expected fetch count for each delivery attempt from 1 to 10.
    let expected_fetch_count_map: HashMap<usize, usize> = HashMap::from([
        (1, 500),
        (2, 500),
        (3, 500),
        (4, 500),
        (5, 500),
        (6, 250),
        (7, 125),
        (8, 62),
        (9, 31),
        (10, 1),
    ]);
    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");
    // Fetch records in 10 iterations, each time acknowledging with RELEASE. 10 is the
    // delivery limit hence we should see throttling from Math.ceil(10/2) = 5 fetches.
    for i in 0..10 {
        let expected_fetch_count = expected_fetch_count_map[&(i + 1)];
        let records = base.waited_poll(&mut share_consumer, 2500, expected_fetch_count).await;
        assert_eq!(expected_fetch_count, records.count());
        // Update delivery count for each offset.
        count_deliveries(&mut offset_to_delivery_count_map, &records);
        // Acknowledge with RELEASE.
        acknowledge_all(&mut share_consumer, &records, AcknowledgeType::Release);
        let result = share_consumer.commit_sync().await.expect("commitSync");
        assert_commit_succeeded(&base, &result);
    }

    // Validate every offset is delivered at most till delivery limit.
    let mut deadline = WaitDeadline::new(120_000, 50, "failed to get records till delivery limit");
    loop {
        let records = share_consumer.poll(Duration::from_millis(2500)).await.expect("poll");
        if !records.is_empty() {
            count_deliveries(&mut offset_to_delivery_count_map, &records);
            acknowledge_all(&mut share_consumer, &records, AcknowledgeType::Release);
            let result = share_consumer.commit_sync().await.expect("commitSync");
            assert_commit_succeeded(&base, &result);
        }
        if offset_to_delivery_count_map.len() == 500
            && offset_to_delivery_count_map
                .values()
                .all(|delivery_count| *delivery_count == 10)
        {
            break;
        }
        deadline.fail_or_pause().await;
    }

    // Next poll should not have any records as all records have reached delivery limit.
    assert_delivery_limit_reached(&mut share_consumer).await;
    close_producer(producer).await;
    close_share_consumer(share_consumer).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fetch_with_throttled_delivery_batches_with_decreased_delivery_limit() {
    let base = ShareConsumerTestBase::setup(delivery_count_limit_config("2")).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let producer = base.create_producer().await;
    let mut share_consumer =
        base.create_share_consumer_with_properties(&group1, &explicit_with(&[("max.poll.records", "512")]));
    // Produce records in complete power of 2 to fully test the throttling behavior.
    let produced_message_count = 512;
    // Produce a batch of 512 messages
    send_message_records(&base, &producer, produced_message_count).await;

    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");
    // Fetch records in 2 iterations, each time acknowledging with RELEASE. As throttling
    // currently applies for delivery limit > 2, hence we should get full batch in both fetches.
    base.validate_expected_records_in_each_poll_and_release(&mut share_consumer, 0, 512, 512)
        .await;
    base.validate_expected_records_in_each_poll_and_release(&mut share_consumer, 0, 512, 512)
        .await;
    // Next poll should not have any records as all records have reached delivery limit.
    assert_delivery_limit_reached(&mut share_consumer).await;
    close_producer(producer).await;
    close_share_consumer(share_consumer).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fetch_with_throttled_delivery_batches_multiple_consumers() {
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let producer = base.create_producer().await;
    let mut share_consumer1 =
        base.create_share_consumer_with_properties(&group1, &explicit_with(&[("max.poll.records", "1")]));
    let mut share_consumer2 =
        base.create_share_consumer_with_properties(&group1, &explicit_with(&[("max.poll.records", "2")]));
    // Produce 2 records in separate batches.
    for i in 0..2 {
        send_record(&producer, message_record(&base, i)).await.expect("send");
        // Flush immediately to create 2 different batches.
        producer.flush().await.expect("flush");
    }

    share_consumer1
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");
    share_consumer2
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");
    // Fetch from consumer1 - should get 1 record as max.poll.records=1.
    let records = base.waited_poll(&mut share_consumer1, 2500, 1).await;
    assert_eq!(1, records.count());
    // Verify the first offset of the fetched records.
    assert_eq!(0, (&records).into_iter().next().expect("a record").offset());
    // Fetch from consumer2 - should get 1 record as offset 0 is Acquired by consumer1.
    // Release the record from consumer2 after fetching until the last delivery attempt.
    for _ in 0..4 {
        base.validate_expected_records_in_each_poll_and_release(&mut share_consumer2, 1, 2, 1)
            .await;
    }

    // Now release the record from consumer1. Fetch again from consumer2 to verify it gets the released record.
    // And should only get 1 record at offset 0 as offset 1 record is in final delivery attempt.
    acknowledge_all(&mut share_consumer1, &records, AcknowledgeType::Release);
    let result = share_consumer1.commit_sync().await.expect("commitSync");
    assert_commit_succeeded(&base, &result);
    // Fetch from consumer2 - should get the released record at offset 0. Accept the record after fetching.
    base.validate_expected_records_in_each_poll_and_acknowledge(&mut share_consumer2, 0, 1, 1, AcknowledgeType::Accept)
        .await;
    // Now fetch the last record at offset 1 from consumer2 in its final delivery attempt.
    base.validate_expected_records_in_each_poll_and_acknowledge(&mut share_consumer2, 1, 2, 1, AcknowledgeType::Accept)
        .await;

    // Next poll from consumer1 should not have any records as all records have reached delivery limit.
    assert_delivery_limit_reached(&mut share_consumer1).await;
    close_producer(producer).await;
    close_share_consumer(share_consumer1).await;
    close_share_consumer(share_consumer2).await;
}

/// Polls one record, asserts its delivery count (when given), releases it and
/// commits: one step of the dynamic delivery-count-limit tests.
async fn poll_and_release_one(
    base: &ShareConsumerTestBase,
    share_consumer: &mut BytesShareConsumer,
    expected_delivery_count: Option<i16>,
) {
    let records = base.waited_poll(share_consumer, 2500, 1).await;
    assert_eq!(1, records.count());
    if let Some(expected_delivery_count) = expected_delivery_count {
        assert_eq!(
            Some(expected_delivery_count),
            records.records_with_partition(&base.tp)[0].delivery_count()
        );
    }
    acknowledge_all(share_consumer, &records, AcknowledgeType::Release);
    share_consumer.commit_sync().await.expect("commitSync");
}

/// Java's `assertTrue(records.isEmpty(), "Records should be empty as the record
/// was archived. ...")` after a 2.5 s poll.
async fn assert_archived(share_consumer: &mut BytesShareConsumer) {
    let records = share_consumer.poll(Duration::from_millis(2500)).await.expect("poll");
    assert!(
        records.is_empty(),
        "Records should be empty as the record was archived. But received: {}",
        records.count()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_dynamic_delivery_count_limit_decrease_archives_records() {
    if skip_below(
        SHARE_GROUP_CONFIGS_RELEASE,
        "testDynamicDeliveryCountLimitDecreaseArchivesRecords",
    ) {
        return;
    }
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let producer = base.create_producer().await;
    let mut share_consumer = base.create_share_consumer_with_properties(&group1, &explicit());
    // Produce 1 record.
    send_key_value_records(&base, &producer, 1).await;

    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");

    // Consume and release twice (deliveryCount becomes 2).
    // Delivery 1: acquire (deliveryCount=1) → release (1 < default limit 5 → AVAILABLE).
    poll_and_release_one(&base, &mut share_consumer, None).await;

    // Delivery 2: acquire (deliveryCount=2) → release (2 < 5 → AVAILABLE).
    poll_and_release_one(&base, &mut share_consumer, Some(2)).await;

    // Dynamically decrease delivery count limit to 3 via group config.
    base.alter_share_delivery_count_limit(&group1, "3").await;

    // Delivery 3: acquire (deliveryCount=3) → release (3 >= 3 → ARCHIVED).
    poll_and_release_one(&base, &mut share_consumer, Some(3)).await;

    // Next poll should have no records because the record was archived.
    assert_archived(&mut share_consumer).await;
    close_producer(producer).await;
    close_share_consumer(share_consumer).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_dynamic_delivery_count_limit_increase_allows_more_deliveries() {
    if skip_below(
        SHARE_GROUP_CONFIGS_RELEASE,
        "testDynamicDeliveryCountLimitIncreaseAllowsMoreDeliveries",
    ) {
        return;
    }
    let base = ShareConsumerTestBase::setup(delivery_count_limit_config("3")).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let producer = base.create_producer().await;
    let mut share_consumer = base.create_share_consumer_with_properties(&group1, &explicit());
    // Produce 1 record.
    send_key_value_records(&base, &producer, 1).await;

    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");

    // Consume and release twice (deliveryCount becomes 2).
    // Delivery 1: acquire (deliveryCount=1) → release (1 < broker limit 3 → AVAILABLE).
    poll_and_release_one(&base, &mut share_consumer, None).await;

    // Delivery 2: acquire (deliveryCount=2) → release (2 < 3 → AVAILABLE).
    poll_and_release_one(&base, &mut share_consumer, Some(2)).await;

    // Dynamically increase delivery count limit to 5 via group config.
    base.alter_share_delivery_count_limit(&group1, "5").await;

    // Delivery 3: acquire (deliveryCount=3) → release (3 < 5 → AVAILABLE).
    // Without the config increase, 3 >= 3 would have caused archival.
    poll_and_release_one(&base, &mut share_consumer, Some(3)).await;

    // Delivery 4: acquire (deliveryCount=4) → release (4 < 5 → AVAILABLE).
    poll_and_release_one(&base, &mut share_consumer, Some(4)).await;

    // Delivery 5: acquire (deliveryCount=5) → release (5 >= 5 → ARCHIVED).
    poll_and_release_one(&base, &mut share_consumer, Some(5)).await;

    // Next poll should have no records because the record was archived.
    assert_archived(&mut share_consumer).await;
    close_producer(producer).await;
    close_share_consumer(share_consumer).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_dynamic_partition_max_record_locks() {
    if skip_below(SHARE_GROUP_CONFIGS_RELEASE, "testDynamicPartitionMaxRecordLocks") {
        return;
    }
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group1 = base.group("group1");
    // Verify that the group-level share.partition.max.record.locks config can be
    // dynamically set and read back via describe configs.
    base.alter_share_partition_max_record_locks(&group1, "500").await;

    // Verify the config can be updated dynamically.
    base.alter_share_partition_max_record_locks(&group1, "1000").await;
}
