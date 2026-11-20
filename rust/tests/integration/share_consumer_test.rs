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

use std::collections::HashMap;
use std::collections::HashSet;
use std::time::Duration;

use confluent_kafka::consumer::ShareConsumer;
use confluent_kafka::producer::Producer;
use confluent_kafka::producer::ProducerRecord;

use super::share_consumer_test_base::*;

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
