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
//! `org.apache.kafka.clients.consumer.ShareConsumerTest` (Apache Kafka 4.4.0,
//! `clients-integration-tests`).
//!
//! The fixture is [`super::share_consumer_test_base`]; its module docs list the
//! deviations every share suite shares (pooled clusters, per-test names, the
//! share-state check).
//!
//! # Two-arm `@ClusterTests` methods
//!
//! Eight methods run twice in Java, with the broker's
//! `group.share.assignment.interval.ms` at `0` and at `1000`. That broker
//! config is new in 4.3, and a 4.2 broker ignores it, so the arms are chosen
//! from the broker tag (`INTEGRATION_TEST_BROKER_TAG`), as in
//! `consumer_bounce_test.rs`: on a broker older than 4.3 the plain-named test
//! runs once on the defaults and the `_assignment_interval_0` /
//! `_assignment_interval_1000` arms skip themselves; from 4.3 on the two arms
//! run Java's configurations and the plain-named test skips itself.
//!
//! # Not translated
//!
//! - `testPollThrowsInterruptExceptionIfInterrupted`: it interrupts the test
//!   thread and expects `poll` to throw `InterruptException`. Rust has no thread
//!   interrupts; a blocking call is abandoned by dropping its future (see the
//!   translation notes of `KafkaShareConsumer`), so there is no
//!   `InterruptException` path to test.
//!
//! # Broker-version gates
//!
//! - `testAddedPartitionsOfKnownTopicStartAtOffsetZero` tests the 4.4.0 broker
//!   fix KAFKA-20742 (a partition added to a topic the share group already knows
//!   starts at offset 0); on an older broker it skips itself
//!   ([`ADDED_PARTITIONS_START_AT_ZERO_RELEASE`]). Measured on 4.2.0: the new
//!   partition starts at its log end and none of the ten records arrive.

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use bytes::Bytes;
use confluent_kafka::common::Error;
use confluent_kafka::common::errors::RecordDeserializationError;
use confluent_kafka::common::header::Header;
use confluent_kafka::common::header::Headers;
use confluent_kafka::common::header::RecordHeaders;
use confluent_kafka::common::record::TimestampType;
use confluent_kafka::common::serialization::ByteArrayDeserializer;
use confluent_kafka::common::serialization::ByteArraySerializer;
use confluent_kafka::common::serialization::Deserializer;
use confluent_kafka::consumer::AcknowledgeOptions;
use confluent_kafka::consumer::AcknowledgeOptionsBuilder;
use confluent_kafka::consumer::AcknowledgeType;
use confluent_kafka::consumer::ShareConsumer;
use confluent_kafka::producer::KafkaProducer;
use confluent_kafka::producer::Producer;
use confluent_kafka::producer::ProducerConfig;
use confluent_kafka::producer::ProducerRecord;
use confluent_kafka::producer::RecordMetadata;

use super::share_consumer_test_base::*;
use crate::common::test_utils;
use confluent_kafka::admin::Admin;
use confluent_kafka::admin::NewPartitions;
use confluent_kafka::admin::RecordsToDelete;
use confluent_kafka::common::TopicIdPartition;
use confluent_kafka::common::TopicPartition;
use confluent_kafka::common::serialization::StringDeserializer;
use confluent_kafka::common::serialization::StringSerializer;
use confluent_kafka::consumer::AcknowledgementCommitCallback;
use confluent_kafka::consumer::acknowledgement_commit_callback;
use std::sync::atomic::AtomicBool;

/// A `key`/`value` record for `tp`, Java's
/// `new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes())`.
fn key_value_record(base: &ShareConsumerTestBase) -> ProducerRecord<Vec<u8>, Vec<u8>> {
    ProducerRecord::with_partition_key(
        base.tp.topic().to_string(),
        Some(base.tp.partition()),
        Some(b"key".to_vec()),
        Some(b"value".to_vec()),
    )
    .expect("a valid record")
}

/// The cluster of a `@ClusterTest(serverProperties =
/// group.share.assignment.interval.ms=<interval_ms>)` arm.
fn assignment_interval_config(interval_ms: &str) -> crate::common::cluster_config::ClusterConfig {
    share_cluster_config_with(&[("KAFKA_GROUP_SHARE_ASSIGNMENT_INTERVAL_MS", interval_ms)])
}

/// The error of a `poll` on a consumer that is not subscribed (Java's
/// `IllegalStateException`, "Consumer is not subscribed to any topics.").
fn assert_not_subscribed(
    result: Result<confluent_kafka::consumer::ConsumerRecords<Vec<u8>, Vec<u8>>, confluent_kafka::common::Error>,
) {
    let error = result.expect_err("poll of an unsubscribed consumer");
    assert!(error.is_local_illegal_state_error(), "{error:?}");
    assert_eq!("Consumer is not subscribed to any topics.", error.message());
}

/// The `share.acknowledgement.mode` property map of an explicit-mode consumer.
fn acknowledgement_mode(mode: &str) -> HashMap<String, String> {
    HashMap::from([("share.acknowledgement.mode".to_string(), mode.to_string())])
}

/// Java's `assertThrows(IllegalStateException.class, ...)`, with the message
/// the Java code throws.
fn assert_illegal_state<T: std::fmt::Debug>(result: Result<T, Error>, message: &str) {
    let error = result.expect_err("expected an IllegalStateException");
    assert!(error.is_local_illegal_state_error(), "{error:?}");
    assert_eq!(message, error.message());
}

/// Java's `assertThrows(RecordDeserializationException.class, ...)`: the error
/// `poll` returns for the record the deserializer failed on.
fn assert_record_deserialization_error<T: std::fmt::Debug>(result: Result<T, Error>) -> RecordDeserializationError {
    match result.expect_err("expected a RecordDeserializationException") {
        Error::RecordDeserialization(rde) => *rde,
        other => panic!("expected a RecordDeserializationException, got {other:?}"),
    }
}

/// Java's `mockErrorDeserializer(recordNumber)`: a byte-array deserializer
/// that fails with a `SerializationException` on the `record_number`-th
/// record and on every record after it (Java's counter only advances on
/// success).
struct MockErrorDeserializer {
    record_index: usize,
    i: AtomicUsize,
}

fn mock_error_deserializer(record_number: usize) -> MockErrorDeserializer {
    MockErrorDeserializer { record_index: record_number - 1, i: AtomicUsize::new(0) }
}

impl Deserializer<Vec<u8>> for MockErrorDeserializer {
    async fn deserialize<'a>(&'a self, _topic: &'a str, data: &'a [u8]) -> Result<Vec<u8>, Error> {
        Ok(data.to_vec())
    }

    async fn deserialize_with_headers_bytes<'a>(
        &'a self,
        _topic: &'a str,
        _headers: &'a mut RecordHeaders,
        data: &'a Bytes,
    ) -> Result<Vec<u8>, Error> {
        if self.i.load(Ordering::SeqCst) == self.record_index {
            Err(Error::serialization(""))
        } else {
            self.i.fetch_add(1, Ordering::SeqCst);
            Ok(data.to_vec())
        }
    }
}

/// [`AcknowledgeOptions`] for Java's
/// `acknowledge(topic, partition, offset, type)`.
fn acknowledge_options(topic: &str, partition: i32, offset: i64, r#type: AcknowledgeType) -> AcknowledgeOptions {
    AcknowledgeOptionsBuilder::new()
        .set_topic(topic)
        .set_partition(partition)
        .set_offset(offset)
        .set_type(r#type)
        .build()
        .expect("every parameter is set")
}

/// A fresh offsets map and error map for a
/// [`TestableAcknowledgementCommitCallback`].
fn callback_maps() -> (PartitionOffsetsMap, PartitionErrorMap) {
    (Arc::new(Mutex::new(HashMap::new())), Arc::new(Mutex::new(HashMap::new())))
}

#[tokio::test(flavor = "multi_thread")]
async fn test_poll_no_subscribe_fails() {
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let mut share_consumer = base.create_share_consumer(&base.group("group1"));
    assert_eq!(HashSet::<String>::new(), share_consumer.subscription());
    // "Consumer is not subscribed to any topics."
    assert_not_subscribed(share_consumer.poll(Duration::from_millis(500)).await);
    close_share_consumer(share_consumer).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_subscribe_and_poll_no_records() {
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let mut share_consumer = base.create_share_consumer(&group1);
    let subscription = HashSet::from([base.tp.topic().to_string()]);
    share_consumer
        .subscribe(subscription.iter().cloned().collect())
        .await
        .expect("subscribe");
    assert_eq!(subscription, share_consumer.subscription());
    let records = share_consumer.poll(Duration::from_millis(500)).await.expect("poll");
    assert_eq!(0, records.count());
    base.verify_share_group_state_topic_records_produced().await;
    close_share_consumer(share_consumer).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_subscribe_poll_unsubscribe() {
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let mut share_consumer = base.create_share_consumer(&group1);
    let subscription = HashSet::from([base.tp.topic().to_string()]);
    share_consumer
        .subscribe(subscription.iter().cloned().collect())
        .await
        .expect("subscribe");
    assert_eq!(subscription, share_consumer.subscription());
    let records = share_consumer.poll(Duration::from_millis(500)).await.expect("poll");
    share_consumer.unsubscribe().await.expect("unsubscribe");
    assert_eq!(HashSet::<String>::new(), share_consumer.subscription());
    assert_eq!(0, records.count());
    base.verify_share_group_state_topic_records_produced().await;
    close_share_consumer(share_consumer).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_subscribe_poll_subscribe() {
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let mut share_consumer = base.create_share_consumer(&group1);
    let subscription = HashSet::from([base.tp.topic().to_string()]);
    share_consumer
        .subscribe(subscription.iter().cloned().collect())
        .await
        .expect("subscribe");
    assert_eq!(subscription, share_consumer.subscription());
    let records = share_consumer.poll(Duration::from_millis(500)).await.expect("poll");
    assert_eq!(0, records.count());
    share_consumer
        .subscribe(subscription.iter().cloned().collect())
        .await
        .expect("subscribe");
    assert_eq!(subscription, share_consumer.subscription());
    let records = share_consumer.poll(Duration::from_millis(500)).await.expect("poll");
    assert_eq!(0, records.count());
    base.verify_share_group_state_topic_records_produced().await;
    close_share_consumer(share_consumer).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_subscribe_unsubscribe_poll_fails() {
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let mut share_consumer = base.create_share_consumer(&group1);
    let subscription = HashSet::from([base.tp.topic().to_string()]);
    share_consumer
        .subscribe(subscription.iter().cloned().collect())
        .await
        .expect("subscribe");
    assert_eq!(subscription, share_consumer.subscription());
    let records = share_consumer.poll(Duration::from_millis(500)).await.expect("poll");
    share_consumer.unsubscribe().await.expect("unsubscribe");
    assert_eq!(HashSet::<String>::new(), share_consumer.subscription());
    // "Consumer is not subscribed to any topics."
    assert_not_subscribed(share_consumer.poll(Duration::from_millis(500)).await);
    assert_eq!(0, records.count());
    base.verify_share_group_state_topic_records_produced().await; // due to leader epoch in read
    close_share_consumer(share_consumer).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_subscribe_subscribe_empty_poll_fails() {
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let mut share_consumer = base.create_share_consumer(&group1);
    let subscription = HashSet::from([base.tp.topic().to_string()]);
    share_consumer
        .subscribe(subscription.iter().cloned().collect())
        .await
        .expect("subscribe");
    assert_eq!(subscription, share_consumer.subscription());
    let records = share_consumer.poll(Duration::from_millis(500)).await.expect("poll");
    share_consumer.subscribe(Vec::new()).await.expect("subscribe");
    assert_eq!(HashSet::<String>::new(), share_consumer.subscription());
    // "Consumer is not subscribed to any topics."
    assert_not_subscribed(share_consumer.poll(Duration::from_millis(500)).await);
    assert_eq!(0, records.count());
    base.verify_share_group_state_topic_records_produced().await; // due to leader epoch in read
    close_share_consumer(share_consumer).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_subscription_and_poll() {
    if skip_from(SHARE_ASSIGNMENT_INTERVAL_RELEASE, "testSubscriptionAndPoll") {
        return;
    }
    subscription_and_poll(default_share_cluster_config()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_subscription_and_poll_assignment_interval_0() {
    if skip_below(SHARE_ASSIGNMENT_INTERVAL_RELEASE, "testSubscriptionAndPoll (interval 0)") {
        return;
    }
    subscription_and_poll(assignment_interval_config("0")).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_subscription_and_poll_assignment_interval_1000() {
    if skip_below(SHARE_ASSIGNMENT_INTERVAL_RELEASE, "testSubscriptionAndPoll (interval 1000)") {
        return;
    }
    subscription_and_poll(assignment_interval_config("1000")).await;
}

/// Java's `testSubscriptionAndPoll`.
async fn subscription_and_poll(config: crate::common::cluster_config::ClusterConfig) {
    let base = ShareConsumerTestBase::setup(config).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let producer = base.create_producer().await;
    let mut share_consumer = base.create_share_consumer(&group1);

    send_record(&producer, key_value_record(&base)).await.expect("send");
    producer.flush().await.expect("flush");
    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");
    assert_eq!(None, share_consumer.acquisition_lock_timeout_ms());
    let records = base.waited_poll(&mut share_consumer, 2500, 1).await;
    assert_eq!(1, records.count());
    assert_eq!(Some(15000), share_consumer.acquisition_lock_timeout_ms());
    base.verify_share_group_state_topic_records_produced().await;
    close_producer(producer).await;
    close_share_consumer(share_consumer).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_subscription_and_poll_multiple() {
    if skip_from(SHARE_ASSIGNMENT_INTERVAL_RELEASE, "testSubscriptionAndPollMultiple") {
        return;
    }
    subscription_and_poll_multiple(default_share_cluster_config()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_subscription_and_poll_multiple_assignment_interval_0() {
    if skip_below(
        SHARE_ASSIGNMENT_INTERVAL_RELEASE,
        "testSubscriptionAndPollMultiple (interval 0)",
    ) {
        return;
    }
    subscription_and_poll_multiple(assignment_interval_config("0")).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_subscription_and_poll_multiple_assignment_interval_1000() {
    if skip_below(
        SHARE_ASSIGNMENT_INTERVAL_RELEASE,
        "testSubscriptionAndPollMultiple (interval 1000)",
    ) {
        return;
    }
    subscription_and_poll_multiple(assignment_interval_config("1000")).await;
}

/// Java's `testSubscriptionAndPollMultiple`.
async fn subscription_and_poll_multiple(config: crate::common::cluster_config::ClusterConfig) {
    let base = ShareConsumerTestBase::setup(config).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let producer = base.create_producer().await;
    let mut share_consumer = base.create_share_consumer(&group1);

    send_record(&producer, key_value_record(&base)).await.expect("send");
    producer.flush().await.expect("flush");
    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");
    assert_eq!(None, share_consumer.acquisition_lock_timeout_ms());
    let records = base.waited_poll(&mut share_consumer, 2500, 1).await;
    assert_eq!(1, records.count());
    assert_eq!(Some(15000), share_consumer.acquisition_lock_timeout_ms());
    send_record(&producer, key_value_record(&base)).await.expect("send");
    let records = share_consumer.poll(Duration::from_millis(5000)).await.expect("poll");
    assert_eq!(1, records.count());
    send_record(&producer, key_value_record(&base)).await.expect("send");
    let records = share_consumer.poll(Duration::from_millis(5000)).await.expect("poll");
    assert_eq!(1, records.count());
    base.verify_share_group_state_topic_records_produced().await;
    close_producer(producer).await;
    close_share_consumer(share_consumer).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_poll_records_greater_than_max_bytes() {
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let producer = base.create_producer().await;
    let mut share_consumer = base.create_share_consumer_with_properties(
        &group1,
        &HashMap::from([("fetch.max.bytes".to_string(), "1".to_string())]),
    );
    send_record(&producer, key_value_record(&base)).await.expect("send");
    producer.flush().await.expect("flush");
    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");
    let records = base.waited_poll(&mut share_consumer, 2500, 1).await;
    assert_eq!(1, records.count());
    base.verify_share_group_state_topic_records_produced().await;
    close_producer(producer).await;
    close_share_consumer(share_consumer).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_headers() {
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let producer = base.create_producer().await;
    let mut share_consumer = base.create_share_consumer(&group1);

    let num_records = 1;
    let mut headers = RecordHeaders::new();
    headers
        .add_with_key_value("headerKey", Some(b"headerValue"))
        .expect("writable headers");
    headers
        .add_with_key_value("headerKey2", Some(b"headerValue2"))
        .expect("writable headers");
    headers
        .add_with_key_value("headerKey3", Some(b"headerValue3"))
        .expect("writable headers");
    let record = ProducerRecord::with_partition_key_headers(
        base.tp.topic().to_string(),
        Some(base.tp.partition()),
        Some(b"key".to_vec()),
        Some(b"value".to_vec()),
        headers,
    )
    .expect("a valid record");
    send_record(&producer, record).await.expect("send");
    producer.flush().await.expect("flush");

    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");

    let records = consume_records(&mut share_consumer, num_records).await;
    assert_eq!(num_records, records.len());

    let header = records[0].headers().last_header("headerKey").expect("headerKey");
    assert_eq!(Some(b"headerValue".as_slice()), header.value());

    // Test the order of headers in a record is preserved when producing and consuming
    let headers = records[0].headers().to_array();
    assert_eq!("headerKey", headers[0].key());
    assert_eq!("headerKey2", headers[1].key());
    assert_eq!("headerKey3", headers[2].key());

    base.verify_share_group_state_topic_records_produced().await;
    close_producer(producer).await;
    close_share_consumer(share_consumer).await;
}

/// Java's `testHeadersSerializeDeserialize(Serializer, Deserializer)`: the
/// producer's value serializer and the consumer's key deserializer are the
/// given ones (Java sets them as `value.serializer` and `key.deserializer`
/// class configs).
async fn headers_serialize_deserialize(base: &ShareConsumerTestBase) {
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let mut producer_config = HashMap::new();
    base.ctx().configure(&mut producer_config);
    let producer = KafkaProducer::new(
        ProducerConfig::new(&producer_config).expect("valid producer config"),
        ByteArraySerializer::new(),
        SerializerImpl,
    )
    .expect("KafkaProducer::new");
    let mut share_consumer = base.create_share_consumer_with_deserializers(
        &group1,
        &HashMap::new(),
        DeserializerImpl,
        ByteArrayDeserializer::new(),
    );

    let num_records = 1;
    send_record(&producer, key_value_record(base)).await.expect("send");
    producer.flush().await.expect("flush");

    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");

    let records = consume_records(&mut share_consumer, num_records).await;
    assert_eq!(num_records, records.len());
    close_producer(producer).await;
    close_share_consumer(share_consumer).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_headers_serializer_deserializer() {
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    headers_serialize_deserialize(&base).await;
    base.verify_share_group_state_topic_records_produced().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_max_poll_records() {
    let num_records = 10000;
    let max_poll_records = 2;

    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let mut share_consumer = base.create_share_consumer_with_properties(
        &group1,
        &HashMap::from([("max.poll.records".to_string(), max_poll_records.to_string())]),
    );

    let starting_timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("after the epoch")
        .as_millis() as i64;
    base.produce_messages_with_timestamp(num_records, starting_timestamp).await;

    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");

    let records = consume_records(&mut share_consumer, num_records).await;
    for (i, record) in records.iter().enumerate() {
        assert_eq!(base.tp.topic(), record.topic());
        assert_eq!(base.tp.partition(), record.partition());
        assert_eq!(TimestampType::CreateTime, record.timestamp_type());
        assert_eq!(starting_timestamp + i as i64, record.timestamp());
        assert_eq!(format!("key {i}").as_bytes(), record.key().expect("key").as_slice());
        assert_eq!(format!("value {i}").as_bytes(), record.value().expect("value").as_slice());
        // this is true only because K and V are byte arrays
        assert_eq!(format!("key {i}").len() as i32, record.serialized_key_size());
        assert_eq!(format!("value {i}").len() as i32, record.serialized_value_size());
    }
    base.verify_share_group_state_topic_records_produced().await;
    close_share_consumer(share_consumer).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_control_records_skipped() {
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    // Java's transactional id "T1", prefixed per test as the group names are.
    let transactional_producer = base.create_producer_with_transactional_id(&base.group("T1"));
    let non_transactional_producer = base.create_producer().await;
    let mut share_consumer = base.create_share_consumer(&group1);

    transactional_producer.init_transactions().await.expect("initTransactions");
    transactional_producer.begin_transaction().expect("beginTransaction");
    let transactional1 = send_and_get(&transactional_producer, key_value_record(&base)).await;

    let non_transactional1 = send_and_get(&non_transactional_producer, key_value_record(&base)).await;

    transactional_producer.commit_transaction().await.expect("commitTransaction");

    transactional_producer.begin_transaction().expect("beginTransaction");
    let transactional2 = send_and_get(&transactional_producer, key_value_record(&base)).await;
    transactional_producer.abort_transaction().await.expect("abortTransaction");

    let non_transactional2 = send_and_get(&non_transactional_producer, key_value_record(&base)).await;

    close_producer(transactional_producer).await;
    close_producer(non_transactional_producer).await;

    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");

    let records = base.waited_poll(&mut share_consumer, 2500, 4).await;
    assert_eq!(4, records.count());
    let tp_records = records.records_with_partition(&base.tp);
    assert_eq!(transactional1.offset(), tp_records[0].offset());
    assert_eq!(non_transactional1.offset(), tp_records[1].offset());
    assert_eq!(transactional2.offset(), tp_records[2].offset());
    assert_eq!(non_transactional2.offset(), tp_records[3].offset());

    // There will be control records on the topic-partition, so the offsets of the non-control records
    // are not 0, 1, 2, 3. Just assert that the offset of the final one is not 3.
    assert_ne!(3, non_transactional2.offset());

    let records = share_consumer.poll(Duration::from_millis(500)).await.expect("poll");
    assert_eq!(0, records.count());
    base.verify_share_group_state_topic_records_produced().await;
    close_share_consumer(share_consumer).await;
}

/// Java's `producer.send(record).get()`.
async fn send_and_get(producer: &BytesProducer, record: ProducerRecord<Vec<u8>, Vec<u8>>) -> RecordMetadata {
    send_record(producer, record)
        .await
        .expect("send")
        .get()
        .await
        .expect("send result")
}

#[tokio::test(flavor = "multi_thread")]
async fn test_explicit_acknowledge_success() {
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let producer = base.create_producer().await;
    let mut share_consumer = base.create_share_consumer_with_properties(&group1, &acknowledgement_mode(EXPLICIT));

    send_record(&producer, key_value_record(&base)).await.expect("send");
    producer.flush().await.expect("flush");

    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");
    let records = base.waited_poll(&mut share_consumer, 2500, 1).await;
    assert_eq!(1, records.count());
    for record in &records {
        share_consumer.acknowledge_with_record(record).expect("acknowledge");
    }
    send_record(&producer, key_value_record(&base)).await.expect("send");
    let records = share_consumer.poll(Duration::from_millis(5000)).await.expect("poll");
    assert_eq!(1, records.count());
    base.verify_share_group_state_topic_records_produced().await;
    close_producer(producer).await;
    close_share_consumer(share_consumer).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_explicit_acknowledge_commit_success() {
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let producer = base.create_producer().await;
    let mut share_consumer = base.create_share_consumer_with_properties(
        &group1,
        &HashMap::from([("share.acknowledgement.mode".to_string(), EXPLICIT.to_string())]),
    );

    send_record(&producer, key_value_record(&base)).await.expect("send");
    producer.flush().await.expect("flush");

    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");
    let records = base.waited_poll(&mut share_consumer, 2500, 1).await;
    assert_eq!(1, records.count());
    for record in &records {
        share_consumer.acknowledge_with_record(record).expect("acknowledge");
    }
    send_record(&producer, key_value_record(&base)).await.expect("send");
    let result = share_consumer.commit_sync().await.expect("commitSync");
    assert_eq!(1, result.len());
    let records = share_consumer.poll(Duration::from_millis(5000)).await.expect("poll");
    assert_eq!(1, records.count());
    base.verify_share_group_state_topic_records_produced().await;
    close_producer(producer).await;
    close_share_consumer(share_consumer).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_explicit_acknowledgement_commit_async() {
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let producer = base.create_producer().await;
    let mut share_consumer1 = base.create_share_consumer_with_properties(&group1, &acknowledgement_mode(EXPLICIT));
    let mut share_consumer2 = base.create_share_consumer(&group1);

    send_record(&producer, key_value_record(&base)).await.expect("send");
    send_record(&producer, key_value_record(&base)).await.expect("send");
    send_record(&producer, key_value_record(&base)).await.expect("send");
    producer.flush().await.expect("flush");

    share_consumer1
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");
    share_consumer2
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");

    let (partition_offsets_map1, partition_error_map1) = callback_maps();
    share_consumer1.set_acknowledgement_commit_callback(Some(TestableAcknowledgementCommitCallback::new(
        Arc::clone(&partition_offsets_map1),
        Arc::clone(&partition_error_map1),
    )));

    let records = base.waited_poll(&mut share_consumer1, 2500, 3).await;
    assert_eq!(3, records.count());
    let mut iterator = (&records).into_iter();

    // Acknowledging 2 out of the 3 records received via commitAsync.
    let first_record = iterator.next().expect("first record");
    let second_record = iterator.next().expect("second record");
    let third_record = iterator.next().expect("third record");
    assert_eq!(0, first_record.offset());
    assert_eq!(1, second_record.offset());

    share_consumer1.acknowledge_with_record(first_record).expect("acknowledge");
    share_consumer1.acknowledge_with_record(second_record).expect("acknowledge");
    share_consumer1
        .acknowledge_with_record_type(third_record, AcknowledgeType::Release)
        .expect("acknowledge");
    share_consumer1.commit_async().await.expect("commitAsync");

    // The 3rd record should be reassigned to 2nd consumer when it polls.
    let mut deadline = WaitDeadline::new(30_000, 100, "Didn't receive timed out record");
    loop {
        let records2 = share_consumer2.poll(Duration::from_millis(1000)).await.expect("poll");
        if records2.count() == 1 && (&records2).into_iter().next().expect("a record").offset() == 2 {
            break;
        }
        deadline.fail_or_pause().await;
    }

    assert!(!partition_error_map1.lock().expect("lock").contains_key(&base.tp));

    // The callback will receive the acknowledgement responses asynchronously after the next poll.
    let mut deadline = WaitDeadline::new(30_000, 100, "Didn't receive call to callback");
    loop {
        share_consumer1.poll(Duration::from_millis(1000)).await.expect("poll");
        if partition_offsets_map1.lock().expect("lock").contains_key(&base.tp) {
            break;
        }
        deadline.fail_or_pause().await;
    }

    assert!(!partition_error_map1.lock().expect("lock").contains_key(&base.tp));
    base.verify_share_group_state_topic_records_produced().await;
    close_producer(producer).await;
    close_share_consumer(share_consumer1).await;
    close_share_consumer(share_consumer2).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_explicit_acknowledgement_commit_async_partial_batch() {
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let producer = base.create_producer().await;
    let mut share_consumer1 = base.create_share_consumer_with_properties(&group1, &acknowledgement_mode(EXPLICIT));

    send_record(&producer, key_value_record(&base)).await.expect("send");
    send_record(&producer, key_value_record(&base)).await.expect("send");
    send_record(&producer, key_value_record(&base)).await.expect("send");
    producer.flush().await.expect("flush");

    share_consumer1
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");

    let (partition_offsets_map, partition_error_map) = callback_maps();
    share_consumer1.set_acknowledgement_commit_callback(Some(TestableAcknowledgementCommitCallback::new(
        Arc::clone(&partition_offsets_map),
        Arc::clone(&partition_error_map),
    )));

    let records = base.waited_poll(&mut share_consumer1, 2500, 3).await;
    assert_eq!(3, records.count());
    let mut iterator = (&records).into_iter();

    // Acknowledging 2 out of the 3 records received via commitAsync.
    let first_record = iterator.next().expect("first record");
    let second_record = iterator.next().expect("second record");
    let third_record = iterator.next().expect("third record");
    assert_eq!(0, first_record.offset());
    assert_eq!(1, second_record.offset());

    share_consumer1.acknowledge_with_record(first_record).expect("acknowledge");
    share_consumer1.acknowledge_with_record(second_record).expect("acknowledge");
    share_consumer1.commit_async().await.expect("commitAsync");

    send_record(&producer, key_value_record(&base)).await.expect("send");
    producer.flush().await.expect("flush");

    // The next poll() should throw an IllegalStateException as there is still 1 unacknowledged record.
    // In EXPLICIT acknowledgement mode, we are not allowed to have unacknowledged records from a batch.
    assert_illegal_state(
        share_consumer1.poll(Duration::from_millis(5000)).await,
        "All records must be acknowledged in explicit acknowledgement mode.",
    );

    // Acknowledging the 3rd record
    share_consumer1.acknowledge_with_record(third_record).expect("acknowledge");
    share_consumer1.commit_async().await.expect("commitAsync");

    // The next poll() will not throw an exception, it would continue to fetch more records.
    let records = share_consumer1.poll(Duration::from_millis(5000)).await.expect("poll");
    assert_eq!(1, records.count());
    let fourth_record = (&records).into_iter().next().expect("fourth record");
    assert_eq!(3, fourth_record.offset());

    share_consumer1.acknowledge_with_record(fourth_record).expect("acknowledge");

    // The callback will receive the acknowledgement responses after polling. The callback is
    // called on entry to the poll method or during close. The commit is being performed asynchronously, so
    // we can only rely on the completion once the consumer has closed because that waits for the response.
    share_consumer1.poll(Duration::from_millis(500)).await.expect("poll");

    close_share_consumer(share_consumer1).await;

    assert!(!partition_error_map.lock().expect("lock").contains_key(&base.tp));
    assert!(
        partition_offsets_map
            .lock()
            .expect("lock")
            .get(&base.tp)
            .is_some_and(|offsets| offsets.len() == 4)
    );
    base.verify_share_group_state_topic_records_produced().await;
    close_producer(producer).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_explicit_acknowledge_release_poll_accept() {
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let producer = base.create_producer().await;
    let mut share_consumer = base.create_share_consumer_with_properties(&group1, &acknowledgement_mode(EXPLICIT));

    send_record(&producer, key_value_record(&base)).await.expect("send");
    producer.flush().await.expect("flush");

    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");
    let records = base.waited_poll(&mut share_consumer, 2500, 1).await;
    assert_eq!(1, records.count());
    for consumed_record in &records {
        share_consumer
            .acknowledge_with_record_type(consumed_record, AcknowledgeType::Release)
            .expect("acknowledge");
    }
    let records = share_consumer.poll(Duration::from_millis(5000)).await.expect("poll");
    assert_eq!(1, records.count());
    for consumed_record in &records {
        share_consumer
            .acknowledge_with_record_type(consumed_record, AcknowledgeType::Accept)
            .expect("acknowledge");
    }
    let records = share_consumer.poll(Duration::from_millis(500)).await.expect("poll");
    assert_eq!(0, records.count());
    base.verify_share_group_state_topic_records_produced().await;
    close_producer(producer).await;
    close_share_consumer(share_consumer).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_explicit_acknowledge_release_accept() {
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let producer = base.create_producer().await;
    let mut share_consumer = base.create_share_consumer_with_properties(&group1, &acknowledgement_mode(EXPLICIT));

    send_record(&producer, key_value_record(&base)).await.expect("send");
    producer.flush().await.expect("flush");
    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");
    let records = base.waited_poll(&mut share_consumer, 2500, 1).await;
    assert_eq!(1, records.count());
    for consumed_record in &records {
        share_consumer
            .acknowledge_with_record_type(consumed_record, AcknowledgeType::Release)
            .expect("acknowledge");
    }
    for consumed_record in &records {
        share_consumer
            .acknowledge_with_record_type(consumed_record, AcknowledgeType::Accept)
            .expect("acknowledge");
    }
    let records = share_consumer.poll(Duration::from_millis(500)).await.expect("poll");
    assert_eq!(0, records.count());
    base.verify_share_group_state_topic_records_produced().await;
    close_producer(producer).await;
    close_share_consumer(share_consumer).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_explicit_acknowledge_release_close() {
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let producer = base.create_producer().await;
    let mut share_consumer = base.create_share_consumer_with_properties(&group1, &acknowledgement_mode(EXPLICIT));

    send_record(&producer, key_value_record(&base)).await.expect("send");
    producer.flush().await.expect("flush");

    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");
    let records = base.waited_poll(&mut share_consumer, 2500, 1).await;
    assert_eq!(1, records.count());
    for consumed_record in &records {
        share_consumer
            .acknowledge_with_record_type(consumed_record, AcknowledgeType::Release)
            .expect("acknowledge");
    }
    base.verify_share_group_state_topic_records_produced().await;
    close_producer(producer).await;
    close_share_consumer(share_consumer).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_explicit_acknowledge_throws_not_in_batch() {
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let producer = base.create_producer().await;
    let mut share_consumer = base.create_share_consumer_with_properties(&group1, &acknowledgement_mode(EXPLICIT));

    send_record(&producer, key_value_record(&base)).await.expect("send");
    producer.flush().await.expect("flush");

    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");
    let first_records = base.waited_poll(&mut share_consumer, 2500, 1).await;
    assert_eq!(1, first_records.count());
    let consumed_record = &first_records.records_with_partition(&base.tp)[0];
    share_consumer.acknowledge_with_record(consumed_record).expect("acknowledge");
    let records = share_consumer.poll(Duration::from_millis(500)).await.expect("poll");
    assert_eq!(0, records.count());
    assert_illegal_state(
        share_consumer.acknowledge_with_record(consumed_record),
        "The record cannot be acknowledged.",
    );
    base.verify_share_group_state_topic_records_produced().await;
    close_producer(producer).await;
    close_share_consumer(share_consumer).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_explicit_override_acknowledge_corrupted_message() {
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let producer = base.create_producer().await;
    let mut share_consumer = base.create_share_consumer_with_deserializers(
        &group1,
        &acknowledgement_mode(EXPLICIT),
        ByteArrayDeserializer::new(),
        mock_error_deserializer(3),
    );

    send_record(&producer, key_value_record(&base)).await.expect("send");
    send_record(&producer, key_value_record(&base)).await.expect("send");
    send_record(&producer, key_value_record(&base)).await.expect("send");
    producer.flush().await.expect("flush");

    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");

    let records = share_consumer.poll(Duration::from_secs(60)).await.expect("poll");
    assert_eq!(2, records.count());
    let mut iterator = (&records).into_iter();

    let first_record = iterator.next().expect("first record");
    let second_record = iterator.next().expect("second record");
    assert_eq!(0, first_record.offset());
    assert_eq!(1, second_record.offset());
    share_consumer.acknowledge_with_record(first_record).expect("acknowledge");
    share_consumer.acknowledge_with_record(second_record).expect("acknowledge");

    let rde = assert_record_deserialization_error(share_consumer.poll(Duration::from_secs(60)).await);
    assert_eq!(2, rde.offset());
    share_consumer.commit_sync().await.expect("commitSync");

    // The corrupted record was automatically released, so we can still obtain it.
    let rde = assert_record_deserialization_error(share_consumer.poll(Duration::from_secs(60)).await);
    assert_eq!(2, rde.offset());

    // Reject this record
    share_consumer
        .acknowledge_with_options(acknowledge_options(
            rde.topic_partition().topic(),
            rde.topic_partition().partition(),
            rde.offset(),
            AcknowledgeType::Reject,
        ))
        .expect("acknowledge");
    share_consumer.commit_sync().await.expect("commitSync");

    let records = share_consumer.poll(Duration::ZERO).await.expect("poll");
    assert_eq!(0, records.count());
    base.verify_share_group_state_topic_records_produced().await;
    close_producer(producer).await;
    close_share_consumer(share_consumer).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_explicit_acknowledge_offset_throws_not_exception() {
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let producer = base.create_producer().await;
    let mut share_consumer = base.create_share_consumer_with_properties(&group1, &acknowledgement_mode(EXPLICIT));

    send_record(&producer, key_value_record(&base)).await.expect("send");
    producer.flush().await.expect("flush");

    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");

    let records = share_consumer.poll(Duration::from_secs(60)).await.expect("poll");
    assert_eq!(1, records.count());
    let consumed_record = &records.records_with_partition(&base.tp)[0];
    assert_eq!(0, consumed_record.offset());

    assert_illegal_state(
        share_consumer.acknowledge_with_options(acknowledge_options(
            base.tp.topic(),
            base.tp.partition(),
            consumed_record.offset(),
            AcknowledgeType::Accept,
        )),
        "The record cannot be acknowledged.",
    );

    share_consumer.acknowledge_with_record(consumed_record).expect("acknowledge");
    base.verify_share_group_state_topic_records_produced().await;
    close_producer(producer).await;
    close_share_consumer(share_consumer).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_explicit_acknowledge_offset_throws_parameters_error() {
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let producer = base.create_producer().await;
    let mut share_consumer = base.create_share_consumer_with_deserializers(
        &group1,
        &acknowledgement_mode(EXPLICIT),
        ByteArrayDeserializer::new(),
        mock_error_deserializer(2),
    );

    send_record(&producer, key_value_record(&base)).await.expect("send");
    send_record(&producer, key_value_record(&base)).await.expect("send");
    producer.flush().await.expect("flush");

    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");

    let records = share_consumer.poll(Duration::from_secs(60)).await.expect("poll");
    assert_eq!(1, records.count());
    let first_record = (&records).into_iter().next().expect("first record");
    assert_eq!(0, first_record.offset());
    share_consumer.acknowledge_with_record(first_record).expect("acknowledge");

    let rde = assert_record_deserialization_error(share_consumer.poll(Duration::from_secs(60)).await);
    assert_eq!(1, rde.offset());

    let rde_topic = rde.topic_partition().topic();
    let rde_partition = rde.topic_partition().partition();
    assert_illegal_state(
        share_consumer.acknowledge_with_options(acknowledge_options(
            "foo",
            rde_partition,
            rde.offset(),
            AcknowledgeType::Reject,
        )),
        "The record cannot be acknowledged.",
    );
    assert_illegal_state(
        share_consumer.acknowledge_with_options(acknowledge_options(
            rde_topic,
            1,
            rde.offset(),
            AcknowledgeType::Reject,
        )),
        "The record cannot be acknowledged.",
    );
    assert_illegal_state(
        share_consumer.acknowledge_with_options(acknowledge_options(
            rde_topic,
            base.tp2.partition(),
            0,
            AcknowledgeType::Reject,
        )),
        "The record cannot be acknowledged.",
    );

    // Reject this record.
    share_consumer
        .acknowledge_with_options(acknowledge_options(
            rde_topic,
            rde_partition,
            rde.offset(),
            AcknowledgeType::Reject,
        ))
        .expect("acknowledge");
    share_consumer.commit_sync().await.expect("commitSync");

    // The next acknowledge() should throw an IllegalStateException as the record has been acked.
    assert_illegal_state(
        share_consumer.acknowledge_with_options(acknowledge_options(
            rde_topic,
            rde_partition,
            rde.offset(),
            AcknowledgeType::Reject,
        )),
        "The record cannot be acknowledged.",
    );

    let records = share_consumer.poll(Duration::ZERO).await.expect("poll");
    assert_eq!(0, records.count());
    base.verify_share_group_state_topic_records_produced().await;
    close_producer(producer).await;
    close_share_consumer(share_consumer).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_implicit_acknowledge_fails_explicit() {
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let producer = base.create_producer().await;
    let mut share_consumer = base.create_share_consumer(&group1);

    send_record(&producer, key_value_record(&base)).await.expect("send");
    producer.flush().await.expect("flush");

    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");
    let first_records = base.waited_poll(&mut share_consumer, 2500, 1).await;
    assert_eq!(1, first_records.count());
    let consumed_record = &first_records.records_with_partition(&base.tp)[0];
    let records = share_consumer.poll(Duration::from_millis(500)).await.expect("poll");
    assert_eq!(0, records.count());
    assert_illegal_state(
        share_consumer.acknowledge_with_record(consumed_record),
        "Implicit acknowledgement of delivery is being used.",
    );
    base.verify_share_group_state_topic_records_produced().await;
    close_producer(producer).await;
    close_share_consumer(share_consumer).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_implicit_acknowledge_commit_sync() {
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let producer = base.create_producer().await;
    let mut share_consumer = base.create_share_consumer(&group1);

    send_record(&producer, key_value_record(&base)).await.expect("send");
    producer.flush().await.expect("flush");

    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");
    let mut deadline = WaitDeadline::new(DEFAULT_MAX_WAIT_MS, 500, "records not found");
    let records = loop {
        let recs = share_consumer.poll(Duration::from_millis(2500)).await.expect("poll");
        if recs.count() == 1 {
            break recs;
        }
        deadline.fail_or_pause().await;
    };
    assert_eq!(1, records.count());
    let result = share_consumer.commit_sync().await.expect("commitSync");
    assert_eq!(1, result.len());
    let result = share_consumer.commit_sync().await.expect("commitSync");
    assert_eq!(0, result.len());
    let records = share_consumer.poll(Duration::from_millis(500)).await.expect("poll");
    assert_eq!(0, records.count());
    base.verify_share_group_state_topic_records_produced().await;
    close_producer(producer).await;
    close_share_consumer(share_consumer).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_implicit_acknowledgement_commit_async() {
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let producer = base.create_producer().await;
    let mut share_consumer = base.create_share_consumer(&group1);

    send_record(&producer, key_value_record(&base)).await.expect("send");
    send_record(&producer, key_value_record(&base)).await.expect("send");
    send_record(&producer, key_value_record(&base)).await.expect("send");
    producer.flush().await.expect("flush");

    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");

    let (partition_offsets_map1, partition_error_map1) = callback_maps();

    share_consumer.set_acknowledgement_commit_callback(Some(TestableAcknowledgementCommitCallback::new(
        Arc::clone(&partition_offsets_map1),
        Arc::clone(&partition_error_map1),
    )));

    let records = base.waited_poll(&mut share_consumer, 2500, 3).await;
    assert_eq!(3, records.count());

    // Implicitly acknowledging all the records received.
    share_consumer.commit_async().await.expect("commitAsync");

    assert!(!partition_error_map1.lock().expect("lock").contains_key(&base.tp));
    // The callback will receive the acknowledgement responses after the next poll.
    let mut deadline = WaitDeadline::new(
        DEFAULT_MAX_WAIT_MS,
        100,
        "Acknowledgement commit callback did not receive the response yet",
    );
    loop {
        share_consumer.poll(Duration::from_millis(1000)).await.expect("poll");
        if partition_offsets_map1.lock().expect("lock").contains_key(&base.tp) {
            break;
        }
        deadline.fail_or_pause().await;
    }

    assert!(!partition_error_map1.lock().expect("lock").contains_key(&base.tp));
    base.verify_share_group_state_topic_records_produced().await;
    close_producer(producer).await;
    close_share_consumer(share_consumer).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_configured_explicit_acknowledge_commit_success() {
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let producer = base.create_producer().await;
    let mut share_consumer = base.create_share_consumer_with_properties(&group1, &acknowledgement_mode("explicit"));

    send_record(&producer, key_value_record(&base)).await.expect("send");
    producer.flush().await.expect("flush");

    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");
    let records = base.waited_poll(&mut share_consumer, 2500, 1).await;
    assert_eq!(1, records.count());
    for record in &records {
        share_consumer.acknowledge_with_record(record).expect("acknowledge");
    }
    send_record(&producer, key_value_record(&base)).await.expect("send");
    let result = share_consumer.commit_sync().await.expect("commitSync");
    assert_eq!(1, result.len());
    let records = share_consumer.poll(Duration::from_millis(5000)).await.expect("poll");
    assert_eq!(1, records.count());
    base.verify_share_group_state_topic_records_produced().await;
    close_producer(producer).await;
    close_share_consumer(share_consumer).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_configured_implicit_acknowledge_explicit_acknowledge_fails() {
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let producer = base.create_producer().await;
    let mut share_consumer = base.create_share_consumer_with_properties(&group1, &acknowledgement_mode(IMPLICIT));

    send_record(&producer, key_value_record(&base)).await.expect("send");
    producer.flush().await.expect("flush");

    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");
    let records = base.waited_poll(&mut share_consumer, 2500, 1).await;
    assert_eq!(1, records.count());
    assert_illegal_state(
        share_consumer.acknowledge_with_record((&records).into_iter().next().expect("a record")),
        "Implicit acknowledgement of delivery is being used.",
    );
    close_producer(producer).await;
    close_share_consumer(share_consumer).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fetch_record_larger_than_max_partition_fetch_bytes() {
    let max_partition_fetch_bytes = 10000;

    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let producer = base.create_producer().await;
    let mut share_consumer = base.create_share_consumer_with_properties(
        &group1,
        &HashMap::from([("max.partition.fetch.bytes".to_string(), max_partition_fetch_bytes.to_string())]),
    );

    let small_record = key_value_record(&base);
    let big_record = producer_record(
        base.tp.topic().to_string(),
        Some(base.tp.partition()),
        Some(b"key".to_vec()),
        Some(vec![0u8; max_partition_fetch_bytes]),
    );
    send_and_get(&producer, small_record).await;
    send_and_get(&producer, big_record).await;

    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");

    let records = base.waited_poll(&mut share_consumer, 2500, 2).await;
    assert_eq!(2, records.count());
    base.verify_share_group_state_topic_records_produced().await;
    close_producer(producer).await;
    close_share_consumer(share_consumer).await;
}

/// Java's `CompletableFuture.allOf(futures).get(timeout)` followed by `join`:
/// waits for every task within `timeout` and returns their results in order.
/// A task that panicked fails the test with its panic, as Java's `join`
/// rethrows the task's exception.
async fn join_all_within<T: Send + 'static>(
    what: &str,
    timeout: Duration,
    handles: Vec<tokio::task::JoinHandle<T>>,
) -> Vec<T> {
    let joined = tokio::time::timeout(timeout, futures_util::future::join_all(handles))
        .await
        .unwrap_or_else(|_| panic!("{what} did not complete within {timeout:?}"));
    joined
        .into_iter()
        .map(|result| result.unwrap_or_else(|e| std::panic::resume_unwind(e.into_panic())))
        .collect()
}

/// A dedicated cluster with the class defaults, for the tests that stop the
/// broker (Java starts a fresh cluster per test).
fn dedicated_share_cluster_config() -> crate::common::cluster_config::ClusterConfig {
    let mut config = default_share_cluster_config();
    config.dedicated = true;
    config
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiple_consumers_with_different_group_ids() {
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group1 = base.group("group1");
    let group2 = base.group("group2");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    base.alter_share_auto_offset_reset(&group2, "earliest").await;
    let producer = base.create_producer().await;
    let mut share_consumer1 = base.create_share_consumer(&group1);
    let mut share_consumer2 = base.create_share_consumer(&group2);

    share_consumer1
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");
    share_consumer2
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");

    // producing 3 records to the topic
    for _ in 0..3 {
        send_record(&producer, key_value_record(&base)).await.expect("send");
    }
    producer.flush().await.expect("flush");

    // Both the consumers should read all the messages, because they are part of different share groups (both have different group IDs)
    let mut share_consumer1_records = 0;
    let mut share_consumer2_records = 0;
    let mut deadline = WaitDeadline::new(DEFAULT_MAX_WAIT_MS, 100, "Failed to consume records for both consumers");
    loop {
        share_consumer1_records += share_consumer1.poll(Duration::from_millis(2000)).await.expect("poll").count();
        share_consumer2_records += share_consumer2.poll(Duration::from_millis(2000)).await.expect("poll").count();
        if share_consumer1_records == 3 && share_consumer2_records == 3 {
            break;
        }
        deadline.fail_or_pause().await;
    }

    send_record(&producer, key_value_record(&base)).await.expect("send");
    send_record(&producer, key_value_record(&base)).await.expect("send");

    share_consumer1_records = 0;
    let mut deadline = WaitDeadline::new(DEFAULT_MAX_WAIT_MS, 100, "Failed to consume records for share consumer 1");
    loop {
        share_consumer1_records += share_consumer1.poll(Duration::from_millis(2000)).await.expect("poll").count();
        if share_consumer1_records == 2 {
            break;
        }
        deadline.fail_or_pause().await;
    }

    for _ in 0..3 {
        send_record(&producer, key_value_record(&base)).await.expect("send");
    }

    share_consumer1_records = 0;
    share_consumer2_records = 0;
    let mut deadline = WaitDeadline::new(
        DEFAULT_MAX_WAIT_MS,
        100,
        "Failed to consume records for both consumers for the last batch",
    );
    loop {
        share_consumer1_records += share_consumer1.poll(Duration::from_millis(2000)).await.expect("poll").count();
        share_consumer2_records += share_consumer2.poll(Duration::from_millis(2000)).await.expect("poll").count();
        if share_consumer1_records == 3 && share_consumer2_records == 5 {
            break;
        }
        deadline.fail_or_pause().await;
    }
    base.verify_share_group_state_topic_records_produced().await;
    close_producer(producer).await;
    close_share_consumer(share_consumer1).await;
    close_share_consumer(share_consumer2).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_consumer_close_on_broker_shutdown() {
    let base = ShareConsumerTestBase::setup(dedicated_share_cluster_config()).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let mut share_consumer = base.create_share_consumer(&group1);
    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");

    // To ensure coordinator discovery is complete before shutting down the broker
    share_consumer.poll(Duration::from_millis(100)).await.expect("poll");

    // Shutdown the broker.
    assert_eq!(1, base.ctx().cluster().broker_ids().len());
    // Java's `cluster.shutdownBroker(0)` then `broker.awaitShutdown()`: the
    // harness returns once the broker container has stopped.
    base.ctx().cluster().shutdown_broker(0).await;

    // Assert that close completes in less than 5 seconds, not the full 30-second timeout.
    tokio::time::timeout(Duration::from_secs(5), share_consumer.close())
        .await
        .expect("Consumer close should not wait for full timeout when broker is already shut down")
        .expect("close");
}

/// The acknowledgement commit callback of the leader-restart tests: Java's
/// lambda `assertInstanceOf(NetworkException.class, exception);
/// callbackCalled.set(true)`. The Rust consumer catches a panicking callback
/// (as Java catches an `Exception`, though not the `AssertionError` its
/// assertion throws), so the callback records what it was called with and the
/// test asserts it afterwards.
struct NetworkErrorCallback {
    callback_called: Arc<AtomicBool>,
    unexpected: Arc<Mutex<Vec<String>>>,
}

impl NetworkErrorCallback {
    fn new() -> Self {
        Self {
            callback_called: Arc::new(AtomicBool::new(false)),
            unexpected: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn callback(&self) -> Arc<dyn AcknowledgementCommitCallback> {
        let callback_called = Arc::clone(&self.callback_called);
        let unexpected = Arc::clone(&self.unexpected);
        Arc::new(acknowledgement_commit_callback::new(
            move |_offsets_by_topic_partition, error| match error {
                Some(error) if error.is_network_error() => callback_called.store(true, Ordering::SeqCst),
                other => unexpected.lock().expect("lock").push(format!("{other:?}")),
            },
        ))
    }

    /// Java's `assertTrue(callbackCalled.get())`, plus the callback's own
    /// `assertInstanceOf(NetworkException.class, exception)`.
    fn assert_called(&self) {
        let unexpected = self.unexpected.lock().expect("lock");
        assert!(
            unexpected.is_empty(),
            "the callback expected a NetworkException, got {unexpected:?}"
        );
        assert!(self.callback_called.load(Ordering::SeqCst));
    }
}

/// The shared opening of the three leader-restart tests: a consumer of
/// `group1` with `properties` and the [`NetworkErrorCallback`], one record
/// produced and polled (20 s), then the broker shut down and restarted.
async fn leader_restart_consume_one_record(
    base: &ShareConsumerTestBase,
    properties: &HashMap<String, String>,
    callback: &NetworkErrorCallback,
) -> (BytesShareConsumer, BytesRecord) {
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let producer = base.create_producer().await;
    let mut share_consumer = base.create_share_consumer_with_properties(&group1, properties);
    share_consumer.set_acknowledgement_commit_callback(Some(callback.callback()));

    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");

    send_record(&producer, key_value_record(base)).await.expect("send");
    producer.flush().await.expect("flush");

    let records = share_consumer.poll(Duration::from_millis(20000)).await.expect("poll");
    assert_eq!(1, records.count());
    let consumer_record = records.into_iter().next().expect("a record");

    // Shutdown the broker
    assert_eq!(1, base.ctx().cluster().broker_ids().len());
    base.ctx().cluster().shutdown_broker(0).await;

    // Restart the broker
    base.ctx().cluster().start_broker(0).await;

    close_producer(producer).await;
    (share_consumer, consumer_record)
}

#[tokio::test(flavor = "multi_thread")]
async fn test_leader_restart_without_leadership_change_explicit_acknowledgement_sync() {
    let base = ShareConsumerTestBase::setup(dedicated_share_cluster_config()).await;
    let callback = NetworkErrorCallback::new();
    let explicit = HashMap::from([("share.acknowledgement.mode".to_string(), EXPLICIT.to_string())]);
    let (mut share_consumer, consumer_record) = leader_restart_consume_one_record(&base, &explicit, &callback).await;

    share_consumer.acknowledge_with_record(&consumer_record).expect("acknowledge");
    let commit_result = share_consumer
        .commit_sync_with_timeout(Duration::from_millis(30000))
        .await
        .expect("commitSync");
    assert_eq!(1, commit_result.len());
    let (_tidp, error) = commit_result.iter().next().expect("one partition");
    let error = error.as_ref().expect("the acknowledgement failed");
    assert!(error.is_network_error(), "{error:?}");

    callback.assert_called();
    close_share_consumer(share_consumer).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_leader_restart_without_leadership_change_explicit_acknowledgement_async() {
    let base = ShareConsumerTestBase::setup(dedicated_share_cluster_config()).await;
    let callback = NetworkErrorCallback::new();
    let explicit = HashMap::from([("share.acknowledgement.mode".to_string(), EXPLICIT.to_string())]);
    let (mut share_consumer, consumer_record) = leader_restart_consume_one_record(&base, &explicit, &callback).await;

    share_consumer.acknowledge_with_record(&consumer_record).expect("acknowledge");
    share_consumer.commit_async().await.expect("commitAsync");

    let max_retries = 15;
    let mut retries = 0;
    while retries < max_retries {
        share_consumer.poll(Duration::from_millis(2000)).await.expect("poll");
        if callback.callback_called.load(Ordering::SeqCst) {
            break;
        }
        retries += 1;
    }

    callback.assert_called();
    close_share_consumer(share_consumer).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_leader_restart_without_leadership_change_implicit_acknowledgement() {
    let base = ShareConsumerTestBase::setup(dedicated_share_cluster_config()).await;
    let callback = NetworkErrorCallback::new();
    let (mut share_consumer, _consumer_record) =
        leader_restart_consume_one_record(&base, &HashMap::new(), &callback).await;

    let max_retries = 15;
    let mut retries = 0;
    while retries < max_retries {
        share_consumer.poll(Duration::from_millis(2000)).await.expect("poll");
        if callback.callback_called.load(Ordering::SeqCst) {
            break;
        }
        retries += 1;
    }

    callback.assert_called();
    close_share_consumer(share_consumer).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiple_consumers_in_group_sequential_consumption() {
    if skip_from(
        SHARE_ASSIGNMENT_INTERVAL_RELEASE,
        "testMultipleConsumersInGroupSequentialConsumption",
    ) {
        return;
    }
    multiple_consumers_in_group_sequential_consumption(default_share_cluster_config()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiple_consumers_in_group_sequential_consumption_assignment_interval_0() {
    if skip_below(
        SHARE_ASSIGNMENT_INTERVAL_RELEASE,
        "testMultipleConsumersInGroupSequentialConsumption (interval 0)",
    ) {
        return;
    }
    multiple_consumers_in_group_sequential_consumption(assignment_interval_config("0")).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiple_consumers_in_group_sequential_consumption_assignment_interval_1000() {
    if skip_below(
        SHARE_ASSIGNMENT_INTERVAL_RELEASE,
        "testMultipleConsumersInGroupSequentialConsumption (interval 1000)",
    ) {
        return;
    }
    multiple_consumers_in_group_sequential_consumption(assignment_interval_config("1000")).await;
}

/// Java's `testMultipleConsumersInGroupSequentialConsumption`.
async fn multiple_consumers_in_group_sequential_consumption(config: crate::common::cluster_config::ClusterConfig) {
    let base = ShareConsumerTestBase::setup(config).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let producer = base.create_producer().await;
    let mut share_consumer1 = base.create_share_consumer(&group1);
    let mut share_consumer2 = base.create_share_consumer(&group1);

    share_consumer1
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");
    share_consumer2
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");

    let total_messages = 2000;
    for _ in 0..total_messages {
        send_record(&producer, key_value_record(&base)).await.expect("send");
    }
    producer.flush().await.expect("flush");

    let mut consumer1_message_count = 0;
    let mut consumer2_message_count = 0;

    let max_retries = 10;
    let mut retries = 0;
    while retries < max_retries {
        let records1 = share_consumer1.poll(Duration::from_millis(2000)).await.expect("poll");
        consumer1_message_count += records1.count();
        let records2 = share_consumer2.poll(Duration::from_millis(2000)).await.expect("poll");
        consumer2_message_count += records2.count();
        if records1.count() + records2.count() == 0 {
            break;
        }
        retries += 1;
    }

    assert_eq!(total_messages, consumer1_message_count + consumer2_message_count);
    close_producer(producer).await;
    close_share_consumer(share_consumer1).await;
    close_share_consumer(share_consumer2).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiple_consumers_in_group_concurrent_consumption() {
    if skip_from(
        SHARE_ASSIGNMENT_INTERVAL_RELEASE,
        "testMultipleConsumersInGroupConcurrentConsumption",
    ) {
        return;
    }
    multiple_consumers_in_group_concurrent_consumption(default_share_cluster_config()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiple_consumers_in_group_concurrent_consumption_assignment_interval_0() {
    if skip_below(
        SHARE_ASSIGNMENT_INTERVAL_RELEASE,
        "testMultipleConsumersInGroupConcurrentConsumption (interval 0)",
    ) {
        return;
    }
    multiple_consumers_in_group_concurrent_consumption(assignment_interval_config("0")).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiple_consumers_in_group_concurrent_consumption_assignment_interval_1000() {
    if skip_below(
        SHARE_ASSIGNMENT_INTERVAL_RELEASE,
        "testMultipleConsumersInGroupConcurrentConsumption (interval 1000)",
    ) {
        return;
    }
    multiple_consumers_in_group_concurrent_consumption(assignment_interval_config("1000")).await;
}

/// Java's `testMultipleConsumersInGroupConcurrentConsumption`.
async fn multiple_consumers_in_group_concurrent_consumption(config: crate::common::cluster_config::ClusterConfig) {
    let base = Arc::new(ShareConsumerTestBase::setup(config).await);
    let total_messages_consumed = Arc::new(AtomicUsize::new(0));

    let consumer_count = 4;
    let producer_count = 4;
    let messages_per_producer = 5000;

    let group_id = base.group("group1");
    base.alter_share_auto_offset_reset(&group_id, "earliest").await;

    let mut producer_futures = Vec::new();
    for _ in 0..producer_count {
        let base = Arc::clone(&base);
        producer_futures.push(tokio::spawn(async move { base.produce_messages(messages_per_producer).await }));
    }

    let max_bytes = 100000;
    let mut consumer_futures = Vec::new();
    for i in 0..consumer_count {
        let consumer_number = i + 1;
        let base = Arc::clone(&base);
        let total_messages_consumed = Arc::clone(&total_messages_consumed);
        let group_id = group_id.clone();
        consumer_futures.push(tokio::spawn(async move {
            base.consume_messages_with_max_fetch_bytes(
                &total_messages_consumed,
                producer_count * messages_per_producer,
                &group_id,
                consumer_number,
                30,
                true,
                max_bytes,
            )
            .await
        }));
    }

    join_all_within("the producers", Duration::from_secs(60), producer_futures).await;
    let total_result: usize = join_all_within("the consumers", Duration::from_secs(60), consumer_futures)
        .await
        .into_iter()
        .sum();
    assert_eq!(producer_count * messages_per_producer, total_result);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiple_consumers_in_multiple_groups_concurrent_consumption() {
    if skip_from(
        SHARE_ASSIGNMENT_INTERVAL_RELEASE,
        "testMultipleConsumersInMultipleGroupsConcurrentConsumption",
    ) {
        return;
    }
    multiple_consumers_in_multiple_groups_concurrent_consumption(default_share_cluster_config()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiple_consumers_in_multiple_groups_concurrent_consumption_assignment_interval_0() {
    if skip_below(
        SHARE_ASSIGNMENT_INTERVAL_RELEASE,
        "testMultipleConsumersInMultipleGroupsConcurrentConsumption (interval 0)",
    ) {
        return;
    }
    multiple_consumers_in_multiple_groups_concurrent_consumption(assignment_interval_config("0")).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiple_consumers_in_multiple_groups_concurrent_consumption_assignment_interval_1000() {
    if skip_below(
        SHARE_ASSIGNMENT_INTERVAL_RELEASE,
        "testMultipleConsumersInMultipleGroupsConcurrentConsumption (interval 1000)",
    ) {
        return;
    }
    multiple_consumers_in_multiple_groups_concurrent_consumption(assignment_interval_config("1000")).await;
}

/// Java's `testMultipleConsumersInMultipleGroupsConcurrentConsumption`.
async fn multiple_consumers_in_multiple_groups_concurrent_consumption(
    config: crate::common::cluster_config::ClusterConfig,
) {
    let base = Arc::new(ShareConsumerTestBase::setup(config).await);
    let total_messages_consumed_group1 = Arc::new(AtomicUsize::new(0));
    let total_messages_consumed_group2 = Arc::new(AtomicUsize::new(0));
    let total_messages_consumed_group3 = Arc::new(AtomicUsize::new(0));

    let producer_count = 4;
    let messages_per_producer = 2000;
    let total_messages_sent = producer_count * messages_per_producer;

    let group_id1 = base.group("group1");
    let group_id2 = base.group("group2");
    let group_id3 = base.group("group3");

    base.alter_share_auto_offset_reset(&group_id1, "earliest").await;
    base.alter_share_auto_offset_reset(&group_id2, "earliest").await;
    base.alter_share_auto_offset_reset(&group_id3, "earliest").await;

    let mut producer_futures = Vec::new();
    for _ in 0..producer_count {
        let base = Arc::clone(&base);
        producer_futures.push(tokio::spawn(async move { base.produce_messages(messages_per_producer).await }));
    }
    // Wait for the producers to run
    let actual_message_sent: usize =
        join_all_within("Exception awaiting produceMessages", Duration::from_secs(15), producer_futures)
            .await
            .into_iter()
            .sum();

    let mut consume_messages_futures = Vec::new();
    let max_bytes = 100000;
    for i in 0..2 {
        let consumer_number = i + 1;
        for (total, group) in [
            (&total_messages_consumed_group1, &group_id1),
            (&total_messages_consumed_group2, &group_id2),
            (&total_messages_consumed_group3, &group_id3),
        ] {
            let base = Arc::clone(&base);
            let total = Arc::clone(total);
            let group = group.clone();
            consume_messages_futures.push(tokio::spawn(async move {
                base.consume_messages_with_max_fetch_bytes(
                    &total,
                    total_messages_sent,
                    &group,
                    consumer_number,
                    100,
                    true,
                    max_bytes,
                )
                .await
            }));
        }
    }

    // The futures alternate group1, group2, group3 per consumer number.
    let results = join_all_within("the consumers", Duration::from_secs(120), consume_messages_futures).await;
    let total_result = |group: usize| -> usize { results.iter().skip(group).step_by(3).sum() };

    assert_eq!(total_messages_sent, total_result(0));
    assert_eq!(total_messages_sent, total_result(1));
    assert_eq!(total_messages_sent, total_result(2));
    assert_eq!(total_messages_sent, actual_message_sent);
    base.verify_share_group_state_topic_records_produced().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_consumer_close_in_group_sequential() {
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let producer = base.create_producer().await;
    let mut share_consumer1 = base.create_share_consumer(&group1);
    let mut share_consumer2 = base.create_share_consumer(&group1);

    share_consumer1
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");
    share_consumer2
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");

    let total_messages = 1500;
    for _ in 0..total_messages {
        send_record(&producer, key_value_record(&base)).await.expect("send");
    }
    close_producer(producer).await;

    let mut consumer1_message_count = 0;
    let mut consumer2_message_count = 0;

    // Poll until we receive all the records. The second poll acknowledges the records
    // from the first poll, and so on.
    // The last poll's records are not acknowledged
    // because the consumer is closed, which makes the broker release the records fetched.
    let max_retries = 10;
    let mut retries = 0;
    let mut last_poll_record_count = 0;
    while consumer1_message_count < total_messages && retries < max_retries {
        last_poll_record_count = share_consumer1.poll(Duration::from_millis(5000)).await.expect("poll").count();
        consumer1_message_count += last_poll_record_count;
        retries += 1;
    }
    assert_eq!(total_messages, consumer1_message_count);
    close_share_consumer(share_consumer1).await;

    // These records are released after the first consumer closes.
    consumer1_message_count -= last_poll_record_count;

    retries = 0;
    while consumer1_message_count + consumer2_message_count < total_messages && retries < max_retries {
        let records2 = share_consumer2.poll(Duration::from_millis(5000)).await.expect("poll");
        consumer2_message_count += records2.count();
        retries += 1;
    }
    close_share_consumer(share_consumer2).await;
    assert_eq!(total_messages, consumer1_message_count + consumer2_message_count);
    base.verify_share_group_state_topic_records_produced().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiple_consumers_in_group_failure_concurrent_consumption() {
    if skip_from(
        SHARE_ASSIGNMENT_INTERVAL_RELEASE,
        "testMultipleConsumersInGroupFailureConcurrentConsumption",
    ) {
        return;
    }
    multiple_consumers_in_group_failure_concurrent_consumption(default_share_cluster_config()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiple_consumers_in_group_failure_concurrent_consumption_assignment_interval_0() {
    if skip_below(
        SHARE_ASSIGNMENT_INTERVAL_RELEASE,
        "testMultipleConsumersInGroupFailureConcurrentConsumption (interval 0)",
    ) {
        return;
    }
    multiple_consumers_in_group_failure_concurrent_consumption(assignment_interval_config("0")).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiple_consumers_in_group_failure_concurrent_consumption_assignment_interval_1000() {
    if skip_below(
        SHARE_ASSIGNMENT_INTERVAL_RELEASE,
        "testMultipleConsumersInGroupFailureConcurrentConsumption (interval 1000)",
    ) {
        return;
    }
    multiple_consumers_in_group_failure_concurrent_consumption(assignment_interval_config("1000")).await;
}

/// Java's `testMultipleConsumersInGroupFailureConcurrentConsumption`.
async fn multiple_consumers_in_group_failure_concurrent_consumption(
    config: crate::common::cluster_config::ClusterConfig,
) {
    let base = Arc::new(ShareConsumerTestBase::setup(config).await);
    let total_messages_consumed = Arc::new(AtomicUsize::new(0));

    let consumer_count = 4;
    let producer_count = 4;
    let messages_per_producer = 5000;

    let group_id = base.group("group1");

    base.alter_share_auto_offset_reset(&group_id, "earliest").await;

    let mut produce_message_futures = Vec::new();
    for _ in 0..producer_count {
        let base = Arc::clone(&base);
        produce_message_futures.push(tokio::spawn(async move { base.produce_messages(messages_per_producer).await }));
    }

    let max_bytes = 1000000;

    // The "failing" consumer polls but immediately closes, which releases the records for the other consumers
    let failed_messages_consumed_future = {
        let base = Arc::clone(&base);
        let group_id = group_id.clone();
        tokio::spawn(async move {
            base.consume_messages(
                &AtomicUsize::new(0),
                producer_count * messages_per_producer,
                &group_id,
                0,
                1,
                false,
            )
            .await
        })
    };

    // Wait for the failed consumer to run
    join_all_within(
        "Exception awaiting consumeMessages",
        Duration::from_secs(15),
        vec![failed_messages_consumed_future],
    )
    .await;

    let mut consume_messages_futures = Vec::new();
    for i in 0..consumer_count {
        let consumer_number = i + 1;
        let base = Arc::clone(&base);
        let total_messages_consumed = Arc::clone(&total_messages_consumed);
        let group_id = group_id.clone();
        consume_messages_futures.push(tokio::spawn(async move {
            base.consume_messages_with_max_fetch_bytes(
                &total_messages_consumed,
                producer_count * messages_per_producer,
                &group_id,
                consumer_number,
                40,
                true,
                max_bytes,
            )
            .await
        }));
    }

    join_all_within("the producers", Duration::from_secs(60), produce_message_futures).await;
    let total_success_result: usize =
        join_all_within("the consumers", Duration::from_secs(60), consume_messages_futures)
            .await
            .into_iter()
            .sum();
    assert_eq!(producer_count * messages_per_producer, total_success_result);
    base.verify_share_group_state_topic_records_produced().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_acquisition_lock_timeout_on_consumer() {
    let base = ShareConsumerTestBase::setup(share_cluster_config_with(&[
        ("KAFKA_GROUP_SHARE_MIN_RECORD_LOCK_DURATION_MS", "5000"),
        ("KAFKA_GROUP_SHARE_RECORD_LOCK_DURATION_MS", "5000"),
    ]))
    .await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let producer = base.create_producer().await;
    let mut share_consumer = base.create_share_consumer(&group1);

    let producer_record1 = producer_record(
        base.tp.topic().to_string(),
        Some(base.tp.partition()),
        Some(b"key_1".to_vec()),
        Some(b"value_1".to_vec()),
    );
    let producer_record2 = producer_record(
        base.tp.topic().to_string(),
        Some(base.tp.partition()),
        Some(b"key_2".to_vec()),
        Some(b"value_2".to_vec()),
    );
    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");

    // Produce a first record which is consumed and acknowledged normally.
    send_record(&producer, producer_record1).await.expect("send");
    producer.flush().await.expect("flush");

    // Poll twice to receive records. The first poll fetches the record and starts the acquisition lock timer.
    // Since, we are only sending one record and the acquisition lock hasn't timed out, the second poll only
    // acknowledges the record from the first poll and does not fetch any more records.
    let consumer_records = base.waited_poll(&mut share_consumer, 2500, 1).await;
    let consumer_record = &consumer_records.records_with_partition(&base.tp)[0];
    assert_eq!(Some(&b"key_1".to_vec()), consumer_record.key());
    assert_eq!(Some(&b"value_1".to_vec()), consumer_record.value());
    assert_eq!(1, consumer_records.count());

    let consumer_records = share_consumer.poll(Duration::from_millis(1000)).await.expect("poll");
    assert_eq!(0, consumer_records.count());

    // Produce a second record which is fetched, but not acknowledged before it times out. The record will
    // be released automatically by the broker. It is then fetched again and acknowledged normally.
    send_record(&producer, producer_record2).await.expect("send");
    producer.flush().await.expect("flush");

    // Poll three more times. The first poll fetches the second record and starts the acquisition lock timer.
    // Before the second poll, acquisition lock times out and hence the consumer needs to fetch the record again.
    // The acquisition lock doesn't time out between the second and third polls, so the third poll only acknowledges
    // the record from the second poll and does not fetch any more records.
    let consumer_records = share_consumer.poll(Duration::from_millis(5000)).await.expect("poll");
    let consumer_record = &consumer_records.records_with_partition(&base.tp)[0];
    assert_eq!(Some(&b"key_2".to_vec()), consumer_record.key());
    assert_eq!(Some(&b"value_2".to_vec()), consumer_record.value());
    assert_eq!(1, consumer_records.count());

    // Allow the acquisition lock to time out.
    tokio::time::sleep(Duration::from_millis(8000)).await;

    let consumer_records = share_consumer.poll(Duration::from_millis(5000)).await.expect("poll");
    let consumer_record = &consumer_records.records_with_partition(&base.tp)[0];
    // By checking the key and value before the count, we get a bit more information if too many records are returned.
    // This test has been observed to fail very occasionally because of this.
    assert_eq!(Some(&b"key_2".to_vec()), consumer_record.key());
    assert_eq!(Some(&b"value_2".to_vec()), consumer_record.value());
    assert_eq!(1, consumer_records.count());

    let consumer_records = share_consumer.poll(Duration::from_millis(1000)).await.expect("poll");
    assert_eq!(0, consumer_records.count());
    base.verify_share_group_state_topic_records_produced().await;
    close_producer(producer).await;
    close_share_consumer(share_consumer).await;
}

// testPollThrowsInterruptExceptionIfInterrupted is not translated: Rust has no
// thread interrupts (see the module docs).

/// Java's `testSubscribeOnInvalidTopicThrowsInvalidTopicException`: the
/// error depends upon a metadata response which arrives asynchronously, so the
/// poll waits long enough for it.
#[tokio::test(flavor = "multi_thread")]
async fn test_subscribe_on_invalid_topic_throws_invalid_topic_exception() {
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let mut share_consumer = base.create_share_consumer(&group1);

    share_consumer
        .subscribe(vec!["topic abc".to_string()])
        .await
        .expect("subscribe");

    // The exception depends upon a metadata response which arrives asynchronously. If the delay is
    // too short, the poll might return before the error is known.
    let error = share_consumer
        .poll(Duration::from_millis(10000))
        .await
        .expect_err("poll of an invalid topic");
    assert!(error.is_invalid_topic_error(), "{error:?}");
    assert_eq!("Invalid topics: [topic abc]", error.message());
    close_share_consumer(share_consumer).await;
}

/// Java's `testWakeupWithFetchedRecordsAvailable`: a wakeup when records are
/// buffered doesn't prevent the records being returned on the next poll.
#[tokio::test(flavor = "multi_thread")]
async fn test_wakeup_with_fetched_records_available() {
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let producer = base.create_producer().await;
    let mut share_consumer = base.create_share_consumer(&group1);

    send_record(&producer, key_value_record(&base)).await.expect("send");
    producer.flush().await.expect("flush");

    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");

    share_consumer.wakeup();
    let error = share_consumer.poll(Duration::ZERO).await.expect_err("poll after wakeup");
    assert!(error.is_wakeup_error(), "{error:?}");
    // Java's `WakeupException` has no message.
    assert_eq!("", error.message());

    let records = base.waited_poll(&mut share_consumer, 2500, 1).await;
    assert_eq!(1, records.count());
    base.verify_share_group_state_topic_records_produced().await;
    close_producer(producer).await;
    close_share_consumer(share_consumer).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_subscription_followed_by_topic_creation() {
    if skip_from(SHARE_ASSIGNMENT_INTERVAL_RELEASE, "testSubscriptionFollowedByTopicCreation") {
        return;
    }
    subscription_followed_by_topic_creation(default_share_cluster_config()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_subscription_followed_by_topic_creation_assignment_interval_0() {
    if skip_below(
        SHARE_ASSIGNMENT_INTERVAL_RELEASE,
        "testSubscriptionFollowedByTopicCreation (interval 0)",
    ) {
        return;
    }
    subscription_followed_by_topic_creation(assignment_interval_config("0")).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_subscription_followed_by_topic_creation_assignment_interval_1000() {
    if skip_below(
        SHARE_ASSIGNMENT_INTERVAL_RELEASE,
        "testSubscriptionFollowedByTopicCreation (interval 1000)",
    ) {
        return;
    }
    subscription_followed_by_topic_creation(assignment_interval_config("1000")).await;
}

/// Java's `testSubscriptionFollowedByTopicCreation`.
async fn subscription_followed_by_topic_creation(config: crate::common::cluster_config::ClusterConfig) {
    let mut base = ShareConsumerTestBase::setup(config).await;
    let topic = base.topic("foo");
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let producer = base.create_producer().await;
    let mut share_consumer = base.create_share_consumer(&group1);

    share_consumer.subscribe(vec![topic.clone()]).await.expect("subscribe");

    // Topic is created post creation of share consumer and subscription
    base.create_topic(&topic).await;

    let record = || producer_record(topic.clone(), Some(0), Some(b"key".to_vec()), Some(b"value".to_vec()));
    send_record(&producer, record()).await.expect("send");
    producer.flush().await.expect("flush");

    let mut deadline = WaitDeadline::new(
        DEFAULT_MAX_WAIT_MS,
        100,
        "Failed to consume records for share consumer, metadata sync failed",
    );
    while share_consumer.poll(Duration::from_millis(2000)).await.expect("poll").count() != 1 {
        deadline.fail_or_pause().await;
    }

    send_record(&producer, record()).await.expect("send");
    let records = share_consumer.poll(Duration::from_millis(5000)).await.expect("poll");
    assert_eq!(1, records.count());
    send_record(&producer, record()).await.expect("send");
    let records = share_consumer.poll(Duration::from_millis(5000)).await.expect("poll");
    assert_eq!(1, records.count());
    base.verify_share_group_state_topic_records_produced().await;
    close_producer(producer).await;
    close_share_consumer(share_consumer).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_subscription_and_poll_followed_by_topic_deletion() {
    if skip_from(
        SHARE_ASSIGNMENT_INTERVAL_RELEASE,
        "testSubscriptionAndPollFollowedByTopicDeletion",
    ) {
        return;
    }
    subscription_and_poll_followed_by_topic_deletion(default_share_cluster_config()).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_subscription_and_poll_followed_by_topic_deletion_assignment_interval_0() {
    if skip_below(
        SHARE_ASSIGNMENT_INTERVAL_RELEASE,
        "testSubscriptionAndPollFollowedByTopicDeletion (interval 0)",
    ) {
        return;
    }
    subscription_and_poll_followed_by_topic_deletion(assignment_interval_config("0")).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_subscription_and_poll_followed_by_topic_deletion_assignment_interval_1000() {
    if skip_below(
        SHARE_ASSIGNMENT_INTERVAL_RELEASE,
        "testSubscriptionAndPollFollowedByTopicDeletion (interval 1000)",
    ) {
        return;
    }
    subscription_and_poll_followed_by_topic_deletion(assignment_interval_config("1000")).await;
}

/// Polls (2 s each, every 100 ms) until one poll returns exactly one record,
/// Java's `TestUtils.waitForCondition(() -> shareConsumer.poll(Duration.ofMillis(2000)).count() == 1,
/// DEFAULT_MAX_WAIT_MS, 100L, () -> details)`.
async fn wait_for_one_record(share_consumer: &mut BytesShareConsumer, details: &str) {
    let mut deadline = WaitDeadline::new(DEFAULT_MAX_WAIT_MS, 100, details);
    while share_consumer.poll(Duration::from_millis(2000)).await.expect("poll").count() != 1 {
        deadline.fail_or_pause().await;
    }
}

/// Java's `testSubscriptionAndPollFollowedByTopicDeletion`.
async fn subscription_and_poll_followed_by_topic_deletion(config: crate::common::cluster_config::ClusterConfig) {
    let mut base = ShareConsumerTestBase::setup(config).await;
    let topic1 = base.topic("bar");
    let topic2 = base.topic("baz");
    base.create_topic(&topic1).await;
    base.create_topic(&topic2).await;

    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let producer = base.create_producer().await;
    let mut share_consumer = base.create_share_consumer(&group1);

    let record_topic1 = || producer_record(topic1.clone(), Some(0), Some(b"key".to_vec()), Some(b"value".to_vec()));
    let record_topic2 = || producer_record(topic2.clone(), Some(0), Some(b"key".to_vec()), Some(b"value".to_vec()));

    // Consumer subscribes to the topics -> bar and baz.
    share_consumer
        .subscribe(vec![topic1.clone(), topic2.clone()])
        .await
        .expect("subscribe");

    send_record(&producer, record_topic1())
        .await
        .expect("send")
        .get()
        .await
        .expect("sent");
    wait_for_one_record(&mut share_consumer, "incorrect number of records").await;

    send_record(&producer, record_topic2())
        .await
        .expect("send")
        .get()
        .await
        .expect("sent");
    wait_for_one_record(&mut share_consumer, "incorrect number of records").await;

    // Topic bar is deleted, hence poll should not give any results.
    base.delete_topic(&topic1).await;
    let records = share_consumer.poll(Duration::from_millis(500)).await.expect("poll");
    assert_eq!(0, records.count());

    send_record(&producer, record_topic2())
        .await
        .expect("send")
        .get()
        .await
        .expect("sent");
    // Poll should give the record from the non-deleted topic baz.
    wait_for_one_record(&mut share_consumer, "incorrect number of records").await;

    send_record(&producer, record_topic2())
        .await
        .expect("send")
        .get()
        .await
        .expect("sent");
    wait_for_one_record(&mut share_consumer, "incorrect number of records").await;
    base.verify_share_group_state_topic_records_produced().await;
    close_producer(producer).await;
    close_share_consumer(share_consumer).await;
}

/// `adminClient.deleteRecords(Map.of(tp, RecordsToDelete.beforeOffset(offset))).all().get()`.
async fn delete_records_before(admin: &dyn Admin, tp: &TopicPartition, offset: i64) {
    admin
        .delete_records(&HashMap::from([(tp.clone(), RecordsToDelete::with_offset(offset))]))
        .all()
        .get()
        .await
        .unwrap_or_else(|e| panic!("Failed to delete records: {e}"));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_lso_movement_by_records_deletion() {
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group_id = base.group("group1");

    base.alter_share_auto_offset_reset(&group_id, "earliest").await;
    let producer = base.create_producer().await;
    let admin_client = base.create_admin_client();

    let record = || {
        producer_record(
            base.tp.topic().to_string(),
            Some(0),
            Some(b"key".to_vec()),
            Some(b"value".to_vec()),
        )
    };

    // We write 10 records to the topic, so they would be written from offsets 0-9 on the topic.
    for _ in 0..10 {
        send_record(&producer, record())
            .await
            .expect("Failed to send records")
            .get()
            .await
            .expect("Failed to send records");
    }

    // We delete records before offset 5, so the LSO should move to 5.
    delete_records_before(admin_client.as_ref(), &base.tp, 5).await;

    let message_count = base.consume_messages(&AtomicUsize::new(0), 5, &group_id, 1, 10, true).await;
    // The records returned belong to offsets 5-9.
    assert_eq!(5, message_count);

    // We write 5 records to the topic, so they would be written from offsets 10-14 on the topic.
    for _ in 0..5 {
        send_record(&producer, record())
            .await
            .expect("Failed to send records")
            .get()
            .await
            .expect("Failed to send records");
    }

    // We delete records before offset 14, so the LSO should move to 14.
    delete_records_before(admin_client.as_ref(), &base.tp, 14).await;

    let consume_messages_count = base.consume_messages(&AtomicUsize::new(0), 1, &group_id, 1, 10, true).await;
    // The record returned belong to offset 14.
    assert_eq!(1, consume_messages_count);

    // We delete records before offset 15, so the LSO should move to 15 and now no records should be returned.
    delete_records_before(admin_client.as_ref(), &base.tp, 15).await;

    let message_count = base.consume_messages(&AtomicUsize::new(0), 0, &group_id, 1, 5, true).await;
    assert_eq!(0, message_count);
    base.verify_share_group_state_topic_records_produced().await;
    close_producer(producer).await;
    close_admin(admin_client).await;
}

/// The `@ClusterTest(brokers = 3, serverProperties = ...)` of
/// `testShareConsumerAfterCoordinatorMovement` and `testComplexShareConsumer`,
/// merged with the class defaults as `ClusterTestExtensions` merges them (the
/// method's value wins for a key both set).
fn three_broker_config() -> crate::common::cluster_config::ClusterConfig {
    let mut config = share_cluster_config_with(&[
        ("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false"),
        ("KAFKA_GROUP_SHARE_MAX_PARTITION_MAX_RECORD_LOCKS", "10000"),
        ("KAFKA_GROUP_SHARE_PARTITION_MAX_RECORD_LOCKS", "10000"),
        ("KAFKA_GROUP_SHARE_RECORD_LOCK_DURATION_MS", "15000"),
        ("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", "3"),
        ("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "3"),
        ("KAFKA_SHARE_COORDINATOR_STATE_TOPIC_NUM_PARTITIONS", "3"),
        ("KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR", "3"),
    ]);
    config.brokers = 3;
    config
}

/// `java.lang.String#hashCode`: `s[0]*31^(n-1) + ... + s[n-1]` over the UTF-16
/// code units, with `int` overflow.
fn java_string_hash_code(s: &str) -> i32 {
    s.encode_utf16()
        .fold(0i32, |hash, unit| hash.wrapping_mul(31).wrapping_add(i32::from(unit)))
}

/// `org.apache.kafka.common.utils.Utils#abs`: `0` for `Integer.MIN_VALUE`.
fn java_utils_abs(n: i32) -> i32 {
    if n == i32::MIN { 0 } else { n.abs() }
}

/// The producer of `testShareConsumerAfterCoordinatorMovement`: string keys and
/// values (Java's `StringSerializer` configs).
fn create_string_producer(base: &ShareConsumerTestBase) -> KafkaProducer<String, String> {
    let mut props = HashMap::new();
    base.ctx().configure(&mut props);
    KafkaProducer::new(
        ProducerConfig::new(&props).expect("valid producer config"),
        StringSerializer::new(),
        StringSerializer::new(),
    )
    .expect("KafkaProducer::new")
}

/// Waits up to Java's 5 s `awaitTermination` for each task of a
/// `shutdownExecutorService`, aborting the rest (`shutdownNow`). A task that
/// panicked fails the test.
async fn shutdown_tasks(tasks: Vec<tokio::task::JoinHandle<()>>) {
    for task in tasks {
        let abort = task.abort_handle();
        match tokio::time::timeout(Duration::from_secs(5), task).await {
            Ok(Ok(())) => {},
            Ok(Err(e)) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
            Ok(Err(_)) => {},
            Err(_) => abort.abort(),
        }
    }
}

/// Java's `testShareConsumerAfterCoordinatorMovement` (`@Timeout(90)`): the
/// share coordinator of the group's partition is shut down while a producer
/// and a share consumer run, and every produced record is still consumed.
///
/// Java runs the producer, the consumer and the shutdown on a scheduled
/// executor and never reads their futures, so an assertion failing on the
/// shutdown task is lost there; here a panicking task fails the test.
#[tokio::test(flavor = "multi_thread")]
async fn test_share_consumer_after_coordinator_movement() {
    let mut config = three_broker_config();
    config.dedicated = true;
    let mut base = ShareConsumerTestBase::setup(config).await;
    let topic_name = base.topic("multipart");
    let group_id = base.group("multipartGrp");
    let base = Arc::new(base);
    test_utils::with_timeout("testShareConsumerAfterCoordinatorMovement", Duration::from_secs(90), async {
        let topic_id = base.create_topic_with_partitions_replication_factor(&topic_name, 3, 3).await;
        base.alter_share_auto_offset_reset(&group_id, "earliest").await;

        let tp_multi = TopicPartition::new(topic_name.clone(), 0);

        // produce some messages
        let prod_state = Arc::new(ClientState::default());
        let produced: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
        let producer_task = tokio::spawn({
            let base = Arc::clone(&base);
            let prod_state = Arc::clone(&prod_state);
            let produced = Arc::clone(&produced);
            let tp_multi = tp_multi.clone();
            async move {
                let mut i = 0;
                let producer = create_string_producer(&base);
                while !prod_state.done().load(Ordering::SeqCst) {
                    let key = format!("key-{i}");
                    i += 1;
                    let record = ProducerRecord::with_partition_key(
                        tp_multi.topic().to_string(),
                        Some(tp_multi.partition()),
                        Some(key.clone()),
                        Some("value".to_string()),
                    )
                    .expect("a valid record");
                    let sent: Result<(), Error> = async {
                        <KafkaProducer<String, String> as Producer<String, String>>::send(&producer, record).await?;
                        producer.flush().await
                    }
                    .await;
                    if sent.is_ok() {
                        // count only correctly produced records
                        prod_state.count().fetch_add(1, Ordering::SeqCst);
                        produced.lock().expect("produced lock").insert(key);
                    }
                }
                tokio::time::timeout(Duration::from_secs(60), producer.close())
                    .await
                    .expect("producer close did not return in time")
                    .expect("producer close");
            }
        });

        // consume messages - start after small delay
        let cons_state = Arc::new(ClientState::default());
        // using map here if we want to debug specific keys
        let consumed: Arc<Mutex<HashMap<String, usize>>> = Arc::new(Mutex::new(HashMap::new()));
        let consumer_task = tokio::spawn({
            let base = Arc::clone(&base);
            let prod_state = Arc::clone(&prod_state);
            let cons_state = Arc::clone(&cons_state);
            let consumed = Arc::clone(&consumed);
            let group_id = group_id.clone();
            let topic_name = topic_name.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(100)).await;
                let mut share_consumer = base.create_share_consumer_with_deserializers(
                    &group_id,
                    &HashMap::new(),
                    StringDeserializer::new(),
                    StringDeserializer::new(),
                );
                share_consumer.subscribe(vec![topic_name]).await.expect("subscribe");
                while !cons_state.done().load(Ordering::SeqCst) {
                    let records = share_consumer.poll(Duration::from_millis(2000)).await.expect("poll");
                    cons_state.count().fetch_add(records.count(), Ordering::SeqCst);
                    let mut consumed = consumed.lock().expect("consumed lock");
                    for record in &records {
                        *consumed.entry(record.key().cloned().unwrap_or_default()).or_insert(0) += 1;
                    }
                    drop(consumed);
                    if prod_state.done().load(Ordering::SeqCst) && records.count() == 0 {
                        cons_state.done().store(true, Ordering::SeqCst);
                    }
                }
                close_share_consumer(share_consumer).await;
            }
        });

        // To be closer to real world scenarios, we will execute after
        // some time has elapsed since the producer and consumer started
        // working.
        let shutdown_task = tokio::spawn({
            let base = Arc::clone(&base);
            let group_id = group_id.clone();
            let tp_multi = tp_multi.clone();
            async move {
                tokio::time::sleep(Duration::from_secs(5)).await;
                // Get the current node hosting the __share_group_state partition
                // on which tpMulti is hosted. Then shut down this node and wait
                // for it to be gracefully shutdown. Then fetch the coordinator again
                // and verify that it has moved to some other broker.
                let admin = base.create_admin_client();
                let key = format!("{group_id}:{topic_id}:{}", tp_multi.partition());
                let share_group_state_tp = java_utils_abs(java_string_hash_code(&key)) % 3;
                let cur_share_coord_node_id = base
                    .topic_partition_leader(admin.as_ref(), SHARE_GROUP_STATE_TOPIC_NAME, share_group_state_tp)
                    .await
                    .expect("topicPartitionLeader");
                assert_eq!(1, cur_share_coord_node_id.len());

                // shutdown the coordinator, and wait for it to be completely shutdown
                base.ctx().cluster().shutdown_broker(cur_share_coord_node_id[0]).await;

                let new_share_coord_node_id = base
                    .topic_partition_leader(admin.as_ref(), SHARE_GROUP_STATE_TOPIC_NAME, share_group_state_tp)
                    .await
                    .expect("topicPartitionLeader");

                assert_eq!(1, new_share_coord_node_id.len());
                assert_ne!(cur_share_coord_node_id[0], new_share_coord_node_id[0]);
                close_admin(admin).await;
            }
        });

        // stop the producer after some time (but after coordinator shutdown)
        let stop_task = tokio::spawn({
            let prod_state = Arc::clone(&prod_state);
            async move {
                tokio::time::sleep(Duration::from_secs(10)).await;
                prod_state.done().store(true, Ordering::SeqCst);
            }
        });

        // wait for both producer and consumer to finish
        wait_for_condition_with_pause(
            || {
                let done = prod_state.done().load(Ordering::SeqCst) && cons_state.done().load(Ordering::SeqCst);
                async move { done }
            },
            45_000,
            500,
            "prod/cons not done yet",
        )
        .await;

        // Make sure we consumed all records. Consumed records could be higher
        // due to re-delivery but that is expected since we are only guaranteeing
        // at least once semantics.
        assert!(prod_state.count().load(Ordering::SeqCst) <= cons_state.count().load(Ordering::SeqCst));
        let consumed_keys: HashSet<String> = consumed.lock().expect("consumed lock").keys().cloned().collect();
        let produced = produced.lock().expect("produced lock").clone();
        assert!(produced.is_superset(&consumed_keys) && consumed_keys.is_superset(&produced));

        shutdown_tasks(vec![producer_task, consumer_task, shutdown_task, stop_task]).await;

        base.verify_share_group_state_topic_records_produced().await;
    })
    .await;
}

/// Java's `testComplexShareConsumer` (`@Timeout(150)`).
#[tokio::test(flavor = "multi_thread")]
async fn test_complex_share_consumer() {
    let mut base = ShareConsumerTestBase::setup(three_broker_config()).await;
    let topic_name = base.topic("multipart");
    let group_id = base.group("multipartGrp");
    let base = Arc::new(base);
    test_utils::with_timeout("testComplexShareConsumer", Duration::from_secs(150), async {
        base.create_topic_with_partitions_replication_factor(&topic_name, 3, 3).await;
        let multi_tp = TopicPartition::new(topic_name.clone(), 0);

        let prod_state = Arc::new(ClientState::default());

        // Produce a fixed number of messages for deterministic testing.
        let target_record_count = 2000;
        let producer_task = tokio::spawn({
            let base = Arc::clone(&base);
            let prod_state = Arc::clone(&prod_state);
            async move {
                let producer = base.create_producer().await;
                loop {
                    let record = producer_record(
                        multi_tp.topic().to_string(),
                        Some(multi_tp.partition()),
                        Some(b"key".to_vec()),
                        Some(b"value".to_vec()),
                    );
                    send_record(&producer, record).await.expect("send");
                    producer.flush().await.expect("flush");
                    if prod_state.count().fetch_add(1, Ordering::SeqCst) + 1 >= target_record_count {
                        break;
                    }
                }
                prod_state.done().store(true, Ordering::SeqCst);
                close_producer(producer).await;
            }
        });

        // Init a complex share consumer. Java passes only the bootstrap servers;
        // the security keys of this run's listener go with them.
        let mut properties = HashMap::from([("share.acknowledgement.mode".to_string(), EXPLICIT.to_string())]);
        base.ctx().apply_security(&mut properties);
        let complex_cons1 = Arc::new(ComplexShareConsumer::new(
            &base.bootstrap_servers(),
            &topic_name,
            &group_id,
            &properties,
        ));
        base.register_group(&group_id);
        base.alter_share_auto_offset_reset(&group_id, "earliest").await;

        let consumer_task = tokio::spawn({
            let complex_cons1 = Arc::clone(&complex_cons1);
            async move {
                tokio::time::sleep(Duration::from_millis(100)).await;
                complex_cons1.run().await;
            }
        });

        // All messages which can be read are read, some would be redelivered (roughly 2 times the records produced).
        wait_for_condition_with_pause(
            || {
                let done = complex_cons1.is_done();
                async move { done }
            },
            45_000,
            DEFAULT_POLL_INTERVAL_MS,
            "did not close!",
        )
        .await;
        let produced = prod_state.count().load(Ordering::SeqCst);
        // 2 times with margin of error (5%).
        let delta = complex_cons1.records_read() as i64 - (produced as f64 * 2.0 * 0.95) as i64;

        assert!(
            delta > 0,
            "Producer ({}) and share consumer ({}) record count mismatch.",
            produced,
            complex_cons1.records_read()
        );

        shutdown_tasks(vec![producer_task, consumer_task]).await;

        base.verify_share_group_state_topic_records_produced().await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_share_group_max_size_config_exceeded() {
    // Setting max group size to 3
    let base = ShareConsumerTestBase::setup(share_cluster_config_with(&[("KAFKA_GROUP_SHARE_MAX_SIZE", "3")])).await;
    let group1 = base.group("group1");
    // creating 3 consumers in the group1
    let mut share_consumer1 = base.create_share_consumer(&group1);
    let mut share_consumer2 = base.create_share_consumer(&group1);
    let mut share_consumer3 = base.create_share_consumer(&group1);

    let topics = vec![base.tp.topic().to_string()];
    share_consumer1.subscribe(topics.clone()).await.expect("subscribe");
    share_consumer2.subscribe(topics.clone()).await.expect("subscribe");
    share_consumer3.subscribe(topics.clone()).await.expect("subscribe");

    share_consumer1.poll(Duration::from_millis(5000)).await.expect("poll");
    share_consumer2.poll(Duration::from_millis(5000)).await.expect("poll");
    share_consumer3.poll(Duration::from_millis(5000)).await.expect("poll");

    let mut share_consumer4 = base.create_share_consumer(&group1);
    share_consumer4.subscribe(topics).await.expect("subscribe");

    let mut deadline = WaitDeadline::new(30000, 200, "The 4th consumer was not kicked out of the group");
    loop {
        match share_consumer4.poll(Duration::from_millis(5000)).await {
            Err(e) if e.is_group_max_size_reached_error() => {
                assert_eq!("The share group has reached its maximum capacity of 3 members.", e.message());
                break;
            },
            _ => deadline.fail_or_pause().await,
        }
    }

    close_share_consumer(share_consumer1).await;
    close_share_consumer(share_consumer2).await;
    close_share_consumer(share_consumer3).await;
    close_share_consumer(share_consumer4).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_share_group_share_session_cache_is_full() {
    let base = ShareConsumerTestBase::setup(share_cluster_config_with(&[
        // Setting max group size to 1
        ("KAFKA_GROUP_SHARE_MAX_SIZE", "1"),
        // Setting max share sessions value to 1
        ("KAFKA_GROUP_SHARE_MAX_SHARE_SESSIONS", "1"),
    ]))
    .await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let producer = base.create_producer().await;
    let mut share_consumer1 = base.create_share_consumer(&group1);
    let mut share_consumer2 = base.create_share_consumer(&base.group("group2"));

    send_record(&producer, key_value_record(&base)).await.expect("send");
    producer.flush().await.expect("flush");
    share_consumer1
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");
    share_consumer2
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");

    let records = base.waited_poll(&mut share_consumer1, 2500, 1).await;
    assert_eq!(1, records.count());

    send_record(&producer, key_value_record(&base)).await.expect("send");
    producer.flush().await.expect("flush");

    // The second share consumer should not throw any exception, but should not receive any records as well.
    let records = share_consumer2.poll(Duration::from_millis(1000)).await.expect("poll");

    assert_eq!(0, records.count());

    close_share_consumer(share_consumer1).await;
    close_share_consumer(share_consumer2).await;
    close_producer(producer).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_commit_sync_fails_for_deleted_topic() {
    let mut base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let baz = base.topic("baz");
    let topic_id = base.create_topic_with_partitions_replication_factor(&baz, 1, 1).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let producer = base.create_producer().await;
    let mut share_consumer = base.create_share_consumer_with_properties(
        &group1,
        &HashMap::from([("share.acknowledgement.mode".to_string(), EXPLICIT.to_string())]),
    );
    for i in 0..10 {
        let record = producer_record(
            baz.clone(),
            Some(0),
            Some(b"key".to_vec()),
            Some(format!("Message {i}").into_bytes()),
        );
        send_record(&producer, record).await.expect("send");
    }
    producer.flush().await.expect("flush");

    share_consumer.subscribe(vec![baz.clone()]).await.expect("subscribe");
    let records = base.waited_poll(&mut share_consumer, 2500, 10).await;
    assert_eq!(10, records.count());

    for record in &records {
        share_consumer.acknowledge_with_record(record).expect("acknowledge");
    }

    // Topic deletion does not necessarily become apparent across the cluster immediately, so sleep a short while
    base.delete_topic(&baz).await;
    tokio::time::sleep(Duration::from_millis(5000)).await;

    let commit_result = share_consumer.commit_sync().await.expect("commitSync");
    assert_eq!(1, commit_result.len());
    let error = commit_result
        .get(&TopicIdPartition::new(topic_id, TopicPartition::new(baz.clone(), 0)))
        .expect("baz-0 in the commit result")
        .as_ref()
        .expect("baz-0 failed");
    assert!(error.is_unknown_topic_id_error(), "{error:?}");
    assert_eq!("This server does not host this topic ID.", error.message());
    close_producer(producer).await;
    close_share_consumer(share_consumer).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_describe_share_group_offsets_for_empty_share_partition() {
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group_id = base.group("group1");
    let mut share_consumer = base.create_share_consumer(&group_id);
    let admin_client = base.create_admin_client();
    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");
    // Polling share consumer to make sure the share partition is created.
    share_consumer.poll(Duration::from_millis(2000)).await.expect("poll");
    let share_partition_offset_info = base
        .share_partition_offset_info(admin_client.as_ref(), &group_id, &base.tp)
        .await
        .expect("listShareGroupOffsets");
    // Since the partition is empty, and no records have been consumed, the share partition startOffset will be
    // -1. Thus, there will be no description for the share partition.
    assert!(share_partition_offset_info.is_none(), "{share_partition_offset_info:?}");
    close_share_consumer(share_consumer).await;
    close_admin(admin_client).await;
}

/// The broker release whose group coordinator initializes a partition added to
/// a topic the share group already knows at offset 0 rather than by
/// `share.auto.offset.reset` (KAFKA-20742, `GroupMetadataManager`'s
/// `InitializeShareGroupStateParameters`, 4.4.0). An older broker resolves the
/// new partition with the group's `latest` reset and skips the records produced
/// before it initializes it, which is what the test asserts does not happen.
const ADDED_PARTITIONS_START_AT_ZERO_RELEASE: (u32, u32) = (4, 4);

/// Java's `testAddedPartitionsOfKnownTopicStartAtOffsetZero`, gated on
/// [`ADDED_PARTITIONS_START_AT_ZERO_RELEASE`]: it tests a broker fix.
#[tokio::test(flavor = "multi_thread")]
async fn test_added_partitions_of_known_topic_start_at_offset_zero() {
    if skip_below(
        ADDED_PARTITIONS_START_AT_ZERO_RELEASE,
        "testAddedPartitionsOfKnownTopicStartAtOffsetZero",
    ) {
        return;
    }
    let mut base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group_id = base.group("expand-group");
    let topic = base.topic("expand-topic");
    base.create_topic_with_partitions_replication_factor(&topic, 1, 1).await;

    base.alter_share_auto_offset_reset(&group_id, "latest").await;

    let producer = base.create_producer().await;
    let admin = base.create_admin_client();
    let mut share_consumer = base.create_share_consumer(&group_id);

    // Produce 10 records to partition 0 before anyone subscribes.
    for i in 0..10 {
        let record = producer_record(
            topic.clone(),
            Some(0),
            Some(format!("p0-key-{i}").into_bytes()),
            Some(format!("p0-value-{i}").into_bytes()),
        );
        send_record(&producer, record).await.expect("send");
    }
    producer.flush().await.expect("flush");

    // The consumer joins and polls: with "latest" it starts partition 0 at the log end, so it receives
    // none of the pre-existing records. This also makes the group aware of (initializes) the topic.
    share_consumer.subscribe(vec![topic.clone()]).await.expect("subscribe");
    let mut received = 0;
    let deadline = std::time::Instant::now() + Duration::from_millis(10000);
    while std::time::Instant::now() < deadline {
        received += share_consumer.poll(Duration::from_millis(1000)).await.expect("poll").count();
    }
    assert_eq!(
        0, received,
        "latest reset should skip records produced before the topic was known"
    );

    // Expand the topic and produce 10 records to the brand-new partition 1, each with a distinct,
    // known value so we can assert all of them are delivered (none skipped by the latest reset).
    admin
        .create_partitions(&HashMap::from([(topic.clone(), NewPartitions::increase_to(2))]))
        .all()
        .get()
        .await
        .expect("createPartitions");
    test_utils::wait_for_all_partitions_metadata(admin.as_ref(), &topic, 2).await;

    let mut expected_values = Vec::new();
    for i in 0..10 {
        let value = format!("p1-value-{i}");
        expected_values.push(value.clone());
        let record = producer_record(
            topic.clone(),
            Some(1),
            Some(format!("p1-key-{i}").into_bytes()),
            Some(value.into_bytes()),
        );
        send_record(&producer, record).await.expect("send");
    }
    producer.flush().await.expect("flush");

    // Partition 1 belongs to an already-known topic, so it must be initialized at offset 0 and deliver
    // every record produced above — even though it was created under a "latest" reset. Share consumers
    // deliver at-least-once, so records may be redelivered; track distinct values in a set so
    // duplicates don't cause spurious failures, then assert the count of distinct records.
    let mut received_values = HashSet::new();
    let mut deadline = WaitDeadline::new(30000, 500, "did not receive all records from the newly added partition");
    loop {
        let records = share_consumer.poll(Duration::from_millis(1000)).await.expect("poll");
        for record in &records {
            if record.partition() == 1 {
                received_values.insert(String::from_utf8(record.value().cloned().unwrap_or_default()).expect("utf-8"));
            }
        }
        if received_values.len() >= expected_values.len() {
            break;
        }
        deadline.fail_or_pause().await;
    }
    assert_eq!(expected_values.len(), received_values.len());
    close_producer(producer).await;
    close_admin(admin).await;
    close_share_consumer(share_consumer).await;
}
