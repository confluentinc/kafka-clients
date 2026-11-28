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
//! `org.apache.kafka.clients.consumer.ShareConsumerCallbackTest` (Apache Kafka
//! 4.4.0, `clients-integration-tests`): the acknowledgement commit callback.
//!
//! The fixture is [`super::share_consumer_test_base`]; its module docs list the
//! deviations every share suite shares (pooled clusters, per-test names, the
//! share-state check).
//!
//! # Methods classification
//!
//! Seven of the eight Java methods are translated.
//!
//! - `testAcknowledgementCommitCallbackCallsShareConsumerDisallowed` is
//!   **skipped**: its callback calls `close`, `subscribe` and `poll` on the
//!   consumer and asserts each throws `IllegalStateException` ("KafkaShareConsumer
//!   methods are not accessible from user-defined acknowledgement commit
//!   callback", `ShareConsumerImpl#acquire`). In Rust the consumer is borrowed
//!   mutably for the whole `poll` that runs the callback, so a callback cannot
//!   hold the consumer at all: the misuse does not compile (decision D6,
//!   `share-consumer.md` §3).
//! - `testAcknowledgementCommitCallbackCallsShareConsumerWakeup` calls
//!   `wakeup` from the callback through a [`ShareConsumerHandle`], the Rust
//!   view that exposes only `wakeup` (decision D6).
//! - `testAcknowledgementCommitCallbackThrowsException` panics in the callback:
//!   `on_complete` returns nothing (decision D8), so a panic is the only way a
//!   Rust callback can fail, and the consumer catches it as Java catches the
//!   throw (`AcknowledgementCommitCallbackHandler#onComplete`).
//!
//! [`ShareConsumerHandle`]: confluent_kafka::consumer::ShareConsumerHandle

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use confluent_kafka::consumer::ShareConsumer;
use confluent_kafka::consumer::acknowledgement_commit_callback;
use confluent_kafka::producer::Producer;
use confluent_kafka::producer::ProducerRecord;

use super::share_consumer_test_base::*;

/// Java's `new ProducerRecord<>(tp.topic(), tp.partition(), null, "key".getBytes(), "value".getBytes())`.
fn key_value_record(tp: &confluent_kafka::common::TopicPartition) -> ProducerRecord<Vec<u8>, Vec<u8>> {
    producer_record(
        tp.topic().to_string(),
        Some(tp.partition()),
        Some(b"key".to_vec()),
        Some(b"value".to_vec()),
    )
}

/// Java's two `partitionOffsetsMap` / `partitionExceptionMap` locals and the
/// `TestableAcknowledgementCommitCallback` over them.
fn testable_callback() -> (
    PartitionOffsetsMap,
    PartitionErrorMap,
    Arc<TestableAcknowledgementCommitCallback>,
) {
    let partition_offsets_map: PartitionOffsetsMap = Arc::new(Mutex::new(HashMap::new()));
    let partition_error_map: PartitionErrorMap = Arc::new(Mutex::new(HashMap::new()));
    let callback = TestableAcknowledgementCommitCallback::new(
        Arc::clone(&partition_offsets_map),
        Arc::clone(&partition_error_map),
    );
    (partition_offsets_map, partition_error_map, callback)
}

/// Java's `TestUtils.waitForCondition(() -> shareConsumer.poll(Duration.ofMillis(2000)).count() == 1,
/// DEFAULT_MAX_WAIT_MS, 100L, () -> "Failed to consume records for share consumer")`.
async fn wait_for_one_record(share_consumer: &mut BytesShareConsumer) {
    let mut deadline = WaitDeadline::new(DEFAULT_MAX_WAIT_MS, 100, "Failed to consume records for share consumer");
    while share_consumer.poll(Duration::from_millis(2000)).await.expect("poll").count() != 1 {
        deadline.fail_or_pause().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_acknowledgement_sent_on_subscription_change() {
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let producer = base.create_producer().await;
    let mut share_consumer = base.create_share_consumer(&group1);
    let (partition_offsets_map, partition_error_map, callback) = testable_callback();

    send_record(&producer, key_value_record(&base.tp)).await.expect("send");
    send_record(&producer, key_value_record(&base.tp2))
        .await
        .expect("send")
        .get()
        .await
        .expect("record2 sent");
    producer.flush().await.expect("flush");
    share_consumer.set_acknowledgement_commit_callback(Some(callback));

    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");
    assert_eq!(None, share_consumer.acquisition_lock_timeout_ms());

    wait_for_one_record(&mut share_consumer).await;

    assert_eq!(Some(15000), share_consumer.acquisition_lock_timeout_ms());
    share_consumer
        .subscribe(vec![base.tp2.topic().to_string()])
        .await
        .expect("subscribe");
    assert_eq!(Some(15000), share_consumer.acquisition_lock_timeout_ms());

    // Waiting for heartbeat to propagate the subscription change.
    let mut deadline = WaitDeadline::new(
        DEFAULT_MAX_WAIT_MS,
        100,
        "Failed to consume records from the updated subscription",
    );
    loop {
        share_consumer.poll(Duration::from_millis(500)).await.expect("poll");
        let both = {
            let offsets = partition_offsets_map.lock().expect("offsets map");
            offsets.contains_key(&base.tp) && offsets.contains_key(&base.tp2)
        };
        if both {
            break;
        }
        deadline.fail_or_pause().await;
    }

    // Verifying if the callback was invoked without exceptions for the partitions for both topics.
    assert!(!partition_error_map.lock().expect("error map").contains_key(&base.tp));
    assert!(!partition_error_map.lock().expect("error map").contains_key(&base.tp2));
    base.verify_share_group_state_topic_records_produced().await;
    close_producer(producer).await;
    close_share_consumer(share_consumer).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_acknowledgement_commit_callback_successful_acknowledgement() {
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let producer = base.create_producer().await;
    let mut share_consumer = base.create_share_consumer(&group1);
    let (partition_offsets_map, partition_error_map, callback) = testable_callback();

    send_record(&producer, key_value_record(&base.tp)).await.expect("send");
    producer.flush().await.expect("flush");

    share_consumer.set_acknowledgement_commit_callback(Some(callback));
    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");

    wait_for_one_record(&mut share_consumer).await;

    // The callback should be called before the return of the poll, even when there are no more records.
    let records = share_consumer.poll(Duration::from_millis(2000)).await.expect("poll");
    assert_eq!(0, records.count());
    assert!(partition_offsets_map.lock().expect("offsets map").contains_key(&base.tp));

    // We expect no exception as the acknowledgement error code is null.
    assert!(!partition_error_map.lock().expect("error map").contains_key(&base.tp));
    base.verify_share_group_state_topic_records_produced().await;
    close_producer(producer).await;
    close_share_consumer(share_consumer).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_acknowledgement_commit_callback_successful_acknowledgement_on_commit_sync() {
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let producer = base.create_producer().await;
    let mut share_consumer = base.create_share_consumer(&group1);
    let (partition_offsets_map, partition_error_map, callback) = testable_callback();

    send_record(&producer, key_value_record(&base.tp)).await.expect("send");
    producer.flush().await.expect("flush");

    share_consumer.set_acknowledgement_commit_callback(Some(callback));
    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");

    wait_for_one_record(&mut share_consumer).await;

    // The acknowledgement commit callback should be called before the commitSync returns
    // once the records have been confirmed to have been acknowledged.
    let result = share_consumer.commit_sync().await.expect("commitSync");
    assert_eq!(1, result.len());
    assert!(partition_offsets_map.lock().expect("offsets map").contains_key(&base.tp));

    // We expect no exception as the acknowledgement error code is null.
    assert!(!partition_error_map.lock().expect("error map").contains_key(&base.tp));
    base.verify_share_group_state_topic_records_produced().await;
    close_producer(producer).await;
    close_share_consumer(share_consumer).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_acknowledgement_commit_callback_on_close() {
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let producer = base.create_producer().await;
    let mut share_consumer = base.create_share_consumer(&group1);
    let (_partition_offsets_map, partition_error_map, callback) = testable_callback();

    send_record(&producer, key_value_record(&base.tp)).await.expect("send");
    producer.flush().await.expect("flush");
    share_consumer.set_acknowledgement_commit_callback(Some(callback));
    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");

    let records = base.waited_poll(&mut share_consumer, 2500, 1).await;
    assert_eq!(1, records.count());

    // Now in the second poll, we implicitly acknowledge the record received in the first poll.
    // We get back the acknowledgement error code asynchronously after the second poll.
    // The acknowledgement commit callback is invoked in close.
    share_consumer.poll(Duration::from_millis(1000)).await.expect("poll");
    close_share_consumer(share_consumer).await;

    // We expect no exception as the acknowledgement error code is null.
    assert!(!partition_error_map.lock().expect("error map").contains_key(&base.tp));
    base.verify_share_group_state_topic_records_produced().await;
    close_producer(producer).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_acknowledgement_commit_callback_invalid_record_state_exception() {
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let producer = base.create_producer().await;
    let mut share_consumer = base.create_share_consumer(&group1);
    let (_partition_offsets_map, partition_error_map, callback) = testable_callback();

    send_record(&producer, key_value_record(&base.tp)).await.expect("send");
    producer.flush().await.expect("flush");

    share_consumer.set_acknowledgement_commit_callback(Some(callback));
    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");

    let records = base.waited_poll(&mut share_consumer, 2500, 1).await;
    assert_eq!(1, records.count());

    // Waiting until the acquisition lock expires.
    tokio::time::sleep(Duration::from_millis(20000)).await;

    let mut deadline =
        WaitDeadline::new(DEFAULT_MAX_WAIT_MS, 100, "Failed to be notified by InvalidRecordStateException");
    loop {
        share_consumer.poll(Duration::from_millis(500)).await.expect("poll");
        if partition_error_map
            .lock()
            .expect("error map")
            .get(&base.tp)
            .is_some_and(confluent_kafka::common::Error::is_invalid_record_state_error)
        {
            break;
        }
        deadline.fail_or_pause().await;
    }
    close_producer(producer).await;
    close_share_consumer(share_consumer).await;
}

/// Java's `testAcknowledgementCommitCallbackCallsShareConsumerWakeup`: the
/// acknowledgement commit callback can call `wakeup`, and it wakes up the
/// enclosing poll. The callback reaches the consumer through its
/// [`ShareConsumerHandle`](confluent_kafka::consumer::ShareConsumerHandle).
#[tokio::test(flavor = "multi_thread")]
async fn test_acknowledgement_commit_callback_calls_share_consumer_wakeup() {
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let producer = base.create_producer().await;
    let mut share_consumer = base.create_share_consumer(&group1);

    send_record(&producer, key_value_record(&base.tp)).await.expect("send");
    producer.flush().await.expect("flush");

    // The acknowledgement commit callback will try to call a method of ShareConsumer
    let handle = share_consumer.handle();
    share_consumer.set_acknowledgement_commit_callback(Some(Arc::new(acknowledgement_commit_callback::new(
        move |_, _| handle.wakeup(),
    ))));
    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");

    wait_for_one_record(&mut share_consumer).await;

    // The second poll sends the acknowledgements implicitly.
    share_consumer.poll(Duration::from_millis(2000)).await.expect("poll");

    // Till now acknowledgement commit callback has not been called, so no exception thrown yet.
    // On 3rd poll, the acknowledgement commit callback will be called and the exception is thrown.
    let mut exception_thrown = false;
    let mut deadline = WaitDeadline::new(DEFAULT_MAX_WAIT_MS, 100, "Failed to receive expected exception");
    loop {
        match share_consumer.poll(Duration::from_millis(500)).await {
            Err(e) if e.is_wakeup_error() => exception_thrown = true,
            Err(e) => panic!("unexpected poll error: {e:?}"),
            Ok(_) => {},
        }
        if exception_thrown {
            break;
        }
        deadline.fail_or_pause().await;
    }
    base.verify_share_group_state_topic_records_produced().await;
    close_producer(producer).await;
    close_share_consumer(share_consumer).await;
}

/// Java's `testAcknowledgementCommitCallbackThrowsException`: a callback that
/// fails does not fail the `poll` that ran it. The Rust callback fails by
/// panicking (see the module docs).
#[tokio::test(flavor = "multi_thread")]
async fn test_acknowledgement_commit_callback_throws_exception() {
    let base = ShareConsumerTestBase::setup(default_share_cluster_config()).await;
    let group1 = base.group("group1");
    base.alter_share_auto_offset_reset(&group1, "earliest").await;
    let producer = base.create_producer().await;
    let mut share_consumer = base.create_share_consumer(&group1);

    send_record(&producer, key_value_record(&base.tp)).await.expect("send");
    producer.flush().await.expect("flush");

    let callback_called = Arc::new(AtomicBool::new(false));
    let called = Arc::clone(&callback_called);
    share_consumer.set_acknowledgement_commit_callback(Some(Arc::new(acknowledgement_commit_callback::new(
        move |_, _| {
            called.store(true, Ordering::SeqCst);
            panic!("Exception thrown in AcknowledgementCommitCallback.onComplete");
        },
    ))));
    share_consumer
        .subscribe(vec![base.tp.topic().to_string()])
        .await
        .expect("subscribe");

    wait_for_one_record(&mut share_consumer).await;

    // Java's `NoRetryException`: any error from `poll` fails the test at once.
    let mut deadline =
        WaitDeadline::new(DEFAULT_MAX_WAIT_MS, 100, "Received unexpected exception or callback not called");
    loop {
        share_consumer
            .poll(Duration::from_millis(500))
            .await
            .unwrap_or_else(|e| panic!("Received unexpected exception: {e:?}"));
        if callback_called.load(Ordering::SeqCst) {
            break;
        }
        deadline.fail_or_pause().await;
    }
    base.verify_share_group_state_topic_records_produced().await;
    close_producer(producer).await;
    close_share_consumer(share_consumer).await;
}
