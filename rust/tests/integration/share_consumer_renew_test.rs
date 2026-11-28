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
//! `org.apache.kafka.clients.consumer.ShareConsumerRenewTest` (Apache Kafka
//! 4.4.0, `clients-integration-tests`): `AcknowledgeType::Renew` (KIP-1222).
//!
//! The fixture is [`super::share_consumer_test_base`]; its module docs list the
//! deviations every share suite shares (pooled clusters, per-test names).
//!
//! # Deviations from Java
//!
//! - **The broker's renew meter.** Java's `verifyYammerMetricCount("ackType=Renew", n)`
//!   reads the broker-wide `ShareGroupMetrics` meter
//!   `RecordAcknowledgementsPerSec,ackType=Renew` from the broker's Yammer
//!   registry in the test JVM. Here the broker runs in a container: the tests
//!   that check it enable the broker's JMX ([`with_broker_jmx`]) and read the
//!   meter with [`broker_meter_count`]. The meter counts every group on the
//!   broker, so those tests run on a cluster of their own
//!   ([`ClusterConfig::dedicated`]), as every Java `@ClusterTest` does; a pooled
//!   cluster would add the renewals of the concurrent tests.
//! - **Renewed records are new values** (decision D5,
//!   [`AcknowledgeType::Renew`](confluent_kafka::consumer::AcknowledgeType::Renew)):
//!   a renewed record comes back from a later `poll` as a new `ConsumerRecord`,
//!   deserialized again, where Java returns the same object. No test here
//!   compares records by identity, so no assertion changes.
//!
//! # Broker version
//!
//! `testRenewAcknowledgementDisabled` sets the group config
//! `share.renew.acknowledge.enable` (KAFKA-20161), which a 4.2 broker rejects
//! (`GroupConfig#validateNames`), so it skips itself below
//! [`SHARE_GROUP_CONFIGS_RELEASE`]. The others run on 4.2.0 too.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use confluent_kafka::consumer::AcknowledgeType;
use confluent_kafka::consumer::ShareConsumer;
use confluent_kafka::consumer::acknowledgement_commit_callback;
use confluent_kafka::producer::Producer;

use super::share_consumer_test_base::*;
use crate::common::cluster_config::ClusterConfig;

/// The suite's class defaults on a cluster of the test's own with the brokers'
/// JMX on, for [`verify_yammer_metric_count`].
fn metered_cluster_config(overrides: &[(&str, &str)]) -> ClusterConfig {
    let mut config = with_broker_jmx(share_cluster_config_with(overrides));
    config.dedicated = true;
    config
}

/// `share.acknowledgement.mode=explicit`.
fn explicit_mode() -> HashMap<String, String> {
    HashMap::from([("share.acknowledgement.mode".to_string(), EXPLICIT.to_string())])
}

/// Java's lambda callback counting every acknowledged offset into `acknowledgements_committed`.
fn counting_callback(
    acknowledgements_committed: &Arc<AtomicUsize>,
) -> Arc<dyn confluent_kafka::consumer::AcknowledgementCommitCallback> {
    let acknowledgements_committed = Arc::clone(acknowledgements_committed);
    Arc::new(acknowledgement_commit_callback::new(
        move |offsets_by_topic_partition, _error| {
            for offsets in offsets_by_topic_partition.values() {
                acknowledgements_committed.fetch_add(offsets.len(), Ordering::SeqCst);
            }
        },
    ))
}

/// Produces `count` records `"key"` / `"Message <i>"` for `i` in `from..from + count` to `tp`, without
/// flushing.
async fn send_messages(base: &ShareConsumerTestBase, producer: &BytesProducer, from: usize, count: usize) {
    for i in from..from + count {
        let record = producer_record(
            base.tp.topic().to_string(),
            Some(base.tp.partition()),
            Some(b"key".to_vec()),
            Some(format!("Message {i}").into_bytes()),
        );
        send_record(producer, record).await.expect("send");
    }
}

/// Acknowledges the even-indexed records of `records` with ACCEPT and the odd-indexed ones with RENEW.
fn accept_even_renew_odd(
    share_consumer: &mut BytesShareConsumer,
    records: &confluent_kafka::consumer::ConsumerRecords<Vec<u8>, Vec<u8>>,
) {
    for (count, record) in records.into_iter().enumerate() {
        let acknowledge_type = if count % 2 == 0 {
            AcknowledgeType::Accept
        } else {
            AcknowledgeType::Renew
        };
        share_consumer
            .acknowledge_with_record_type(record, acknowledge_type)
            .expect("acknowledge");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_renew_acknowledgement_on_poll() {
    let base = ShareConsumerTestBase::setup(metered_cluster_config(&[])).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let producer = base.create_producer().await;
    let mut share_consumer = base.create_share_consumer_with_properties(&group1, &explicit_mode());
    let acknowledgements_committed = Arc::new(AtomicUsize::new(0));
    share_consumer.set_acknowledgement_commit_callback(Some(counting_callback(&acknowledgements_committed)));

    send_messages(&base, &producer, 0, 10).await;
    producer.flush().await.expect("flush");

    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");
    let records = base.waited_poll(&mut share_consumer, 2500, 10).await;
    assert_eq!(10, records.count());

    accept_even_renew_odd(&mut share_consumer, &records);

    // Get the rest of all 5 records.
    let records = base.waited_poll(&mut share_consumer, 2500, 5).await;
    assert_eq!(5, records.count());
    for record in &records {
        share_consumer
            .acknowledge_with_record_type(record, AcknowledgeType::Accept)
            .expect("acknowledge");
    }

    share_consumer.commit_sync().await.expect("commitSync");
    assert_eq!(15, acknowledgements_committed.load(Ordering::SeqCst));
    close_producer(producer).await;
    close_share_consumer(share_consumer).await;
    verify_yammer_metric_count(&base, "Renew", 5).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_renew_acknowledgement_on_commit_sync() {
    let base = ShareConsumerTestBase::setup(metered_cluster_config(&[])).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let producer = base.create_producer().await;
    let mut share_consumer = base.create_share_consumer_with_properties(&group1, &explicit_mode());
    let acknowledgements_committed = Arc::new(AtomicUsize::new(0));
    share_consumer.set_acknowledgement_commit_callback(Some(counting_callback(&acknowledgements_committed)));

    send_messages(&base, &producer, 0, 10).await;
    producer.flush().await.expect("flush");

    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");
    let records = base.waited_poll(&mut share_consumer, 2500, 10).await;
    assert_eq!(10, records.count());
    assert_eq!(Some(15000), share_consumer.acquisition_lock_timeout_ms());

    // The updated acquisition lock timeout is only applied when the next poll is called.
    base.alter_share_record_lock_duration_ms(&group1, 25000).await;

    for (count, record) in (&records).into_iter().enumerate() {
        let acknowledge_type = if count % 2 == 0 {
            AcknowledgeType::Accept
        } else {
            AcknowledgeType::Renew
        };
        share_consumer
            .acknowledge_with_record_type(record, acknowledge_type)
            .expect("acknowledge");
        let result = share_consumer.commit_sync().await.expect("commitSync");
        assert_eq!(1, result.len());
        assert_eq!(Some(15000), share_consumer.acquisition_lock_timeout_ms());
        assert!(result.get(&base.tip()).expect("tp in the commit result").is_none());
    }

    // Get the rest of all 5 records.
    let records = base.waited_poll(&mut share_consumer, 2500, 5).await;
    assert_eq!(5, records.count());
    assert_eq!(Some(25000), share_consumer.acquisition_lock_timeout_ms());
    for record in &records {
        share_consumer
            .acknowledge_with_record_type(record, AcknowledgeType::Accept)
            .expect("acknowledge");
    }
    close_producer(producer).await;
    close_share_consumer(share_consumer).await;
    verify_yammer_metric_count(&base, "Renew", 5).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_renew_acknowledgement_invalid_state_record() {
    let base = ShareConsumerTestBase::setup(metered_cluster_config(&[])).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let producer = base.create_producer().await;
    let mut share_consumer = base.create_share_consumer_with_properties(&group1, &explicit_mode());
    let acknowledgements_committed = Arc::new(AtomicUsize::new(0));
    share_consumer.set_acknowledgement_commit_callback(Some(counting_callback(&acknowledgements_committed)));

    let record = producer_record(
        base.tp.topic().to_string(),
        Some(base.tp.partition()),
        Some(b"key".to_vec()),
        Some(b"Message ".to_vec()),
    );
    send_record(&producer, record).await.expect("send");
    producer.flush().await.expect("flush");

    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");
    let records = base.waited_poll(&mut share_consumer, 2500, 1).await;
    assert_eq!(1, records.count());

    for rec in &records {
        share_consumer
            .acknowledge_with_record_type(rec, AcknowledgeType::Reject)
            .expect("acknowledge");
        share_consumer.commit_sync().await.expect("commitSync");
        let error = share_consumer
            .acknowledge_with_record_type(rec, AcknowledgeType::Renew)
            .expect_err("renewing an acknowledged record");
        assert!(error.is_local_illegal_state_error(), "{error:?}");
        assert_eq!("The record cannot be acknowledged.", error.message());
    }
    close_producer(producer).await;
    close_share_consumer(share_consumer).await;
    verify_yammer_metric_count(&base, "Renew", 0).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_renew_acknowledgement_disabled() {
    if skip_below(SHARE_GROUP_CONFIGS_RELEASE, "testRenewAcknowledgementDisabled") {
        return;
    }
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    base.alter_share_renew_acknowledge_enable(&group1, false).await;
    let producer = base.create_producer().await;
    let mut share_consumer = base.create_share_consumer_with_properties(&group1, &explicit_mode());

    let record = producer_record(
        base.tp.topic().to_string(),
        Some(base.tp.partition()),
        Some(b"key".to_vec()),
        Some(b"Message".to_vec()),
    );
    send_record(&producer, record).await.expect("send");
    producer.flush().await.expect("flush");

    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");
    let records = base.waited_poll(&mut share_consumer, 2500, 1).await;
    assert_eq!(1, records.count());

    for rec in &records {
        share_consumer
            .acknowledge_with_record_type(rec, AcknowledgeType::Renew)
            .expect("acknowledge");
    }

    let result = share_consumer.commit_sync().await.expect("commitSync");
    assert_eq!(1, result.len());
    let error = result.get(&base.tip()).expect("tp in the commit result");
    let error = error.as_ref().expect("the renew failed");
    assert!(error.is_invalid_record_state_error(), "{error:?}");
    close_producer(producer).await;
    close_share_consumer(share_consumer).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_renew_acknowledgement_no_result_in_poll() {
    let base = ShareConsumerTestBase::setup(metered_cluster_config(&[
        ("KAFKA_GROUP_SHARE_RECORD_LOCK_DURATION_MS", "12000"),
        ("KAFKA_GROUP_SHARE_MIN_RECORD_LOCK_DURATION_MS", "12000"),
    ]))
    .await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let producer = base.create_producer().await;
    let mut share_consumer = base.create_share_consumer_with_properties(&group1, &explicit_mode());
    let acknowledgements_committed = Arc::new(AtomicUsize::new(0));
    share_consumer.set_acknowledgement_commit_callback(Some(counting_callback(&acknowledgements_committed)));

    send_messages(&base, &producer, 0, 10).await;
    producer.flush().await.expect("flush");

    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");
    let records = base.waited_poll(&mut share_consumer, 2500, 10).await;
    assert_eq!(10, records.count());

    accept_even_renew_odd(&mut share_consumer, &records);

    // 5 more records (total 15 produced).
    send_messages(&base, &producer, 10, 5).await;

    // Get the rest of all 5 records.
    let records = base.waited_poll(&mut share_consumer, 11500, 0).await; // This will send the acks but not return next 5 records (10-15)
    assert_eq!(10, acknowledgements_committed.load(Ordering::SeqCst));
    assert_eq!(0, records.count());
    verify_yammer_metric_count(&base, "Renew", 5).await;

    // Renewal duration passed, now records will be back.
    let records = base.waited_poll(&mut share_consumer, 2500, 5).await; // Renewed records as well as 10-15 records.
    assert_eq!(5, records.count());
    for record in &records {
        share_consumer
            .acknowledge_with_record_type(record, AcknowledgeType::Accept)
            .expect("acknowledge");
    }

    share_consumer.commit_sync().await.expect("commitSync");

    let records = base.waited_poll(&mut share_consumer, 2500, 5).await; // 10-15 records.
    assert_eq!(5, records.count());
    for record in &records {
        share_consumer
            .acknowledge_with_record_type(record, AcknowledgeType::Accept)
            .expect("acknowledge");
    }

    share_consumer.commit_sync().await.expect("commitSync");

    // Initial - 5 renew + 5 accept, Subsequent - 5 renewed accepted + 5 fresh accepted (10-15)
    assert_eq!(20, acknowledgements_committed.load(Ordering::SeqCst));
    close_producer(producer).await;
    close_share_consumer(share_consumer).await;
    verify_yammer_metric_count(&base, "Renew", 5).await;
}

/// Java's `verifyYammerMetricCount("ackType=<ack_type>", count)`: the count of the broker's
/// `ShareGroupMetrics` meter `RecordAcknowledgementsPerSec,ackType=<ack_type>` is `count`. Java fails
/// with "metric not found" when the meter is not registered; the broker registers one per acknowledge type
/// at start-up.
async fn verify_yammer_metric_count(base: &ShareConsumerTestBase, ack_type: &str, count: i64) {
    let meter = broker_meter_count(
        base.ctx(),
        &format!("kafka.server:type=ShareGroupMetrics,name=RecordAcknowledgementsPerSec,ackType={ack_type}"),
    )
    .await;
    assert_ne!(-1, meter, "metric not found");
    assert_eq!(count, meter);
}
