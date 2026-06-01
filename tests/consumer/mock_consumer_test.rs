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

//! Translated from `org.apache.kafka.clients.consumer.MockConsumerTest`.
//!
//! All eight `@Test` methods translate 1:1, with three null-input assertions
//! in `testRe2JPatternSubscription` dropped because Rust's type system rules
//! out passing null for `SubscriptionPattern` and `Arc<dyn
//! ConsumerRebalanceListener>`. See the test-level comment for details.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use confluent_kafka::common::header::RecordHeaders;
use confluent_kafka::common::record::TimestampType;
use confluent_kafka::common::{KafkaError, TopicPartition};
use confluent_kafka::consumer::{
    AutoOffsetResetStrategy, Consumer, ConsumerRebalanceListener, ConsumerRecord, MockConsumer, OffsetAndMetadata,
    SubscriptionPattern,
};

/// Compile-time check that [`MockConsumer<K, V>`] is object-safe and can be
/// used as `Box<dyn Consumer<K, V>>` (DoD §11). A regression that introduces
/// a `Self: Sized` bound would fail this file at compile time.
#[test]
fn mock_consumer_is_consumer_trait_object() {
    let _: Box<dyn Consumer<String, String>> =
        Box::new(MockConsumer::<String, String>::new(AutoOffsetResetStrategy::EARLIEST));
}

/// Builder helper: matches Java's `new ConsumerRecord<>(topic, partition,
/// offset, ts, tsType, sizeK, sizeV, key, value, headers, leaderEpoch)`.
fn build_record(topic: &str, partition: i32, offset: i64, key: &str, value: &str) -> ConsumerRecord<String, String> {
    ConsumerRecord::with_headers(
        topic.to_string(),
        partition,
        offset,
        0,
        TimestampType::CreateTime,
        0,
        0,
        Some(key.to_string()),
        Some(value.to_string()),
        RecordHeaders::new(),
        None,
    )
}

/// Builder helper: matches Java's 5-arg `new ConsumerRecord<>(topic,
/// partition, offset, null, null)`.
fn build_null_record(topic: &str, partition: i32, offset: i64) -> ConsumerRecord<String, String> {
    ConsumerRecord::new(topic.to_string(), partition, offset, None, None)
}

/// Translated from `MockConsumerTest.testSimpleMock`.
#[tokio::test]
async fn test_simple_mock() {
    let mut consumer: MockConsumer<String, String> = MockConsumer::new(AutoOffsetResetStrategy::EARLIEST);

    consumer.subscribe(vec!["test".to_string()]).await.unwrap();
    assert_eq!(0, consumer.poll(std::time::Duration::ZERO).await.unwrap().count());

    consumer
        .rebalance(&[
            TopicPartition::new("test".to_string(), 0),
            TopicPartition::new("test".to_string(), 1),
        ])
        .await
        .unwrap();

    // Mock consumers need to seek manually since they cannot automatically reset offsets.
    let mut beginning_offsets = HashMap::new();
    beginning_offsets.insert(TopicPartition::new("test".to_string(), 0), 0i64);
    beginning_offsets.insert(TopicPartition::new("test".to_string(), 1), 0i64);
    consumer.update_beginning_offsets(beginning_offsets);
    consumer.seek(TopicPartition::new("test".to_string(), 0), 0).await.unwrap();

    consumer.add_record(build_record("test", 0, 0, "key1", "value1")).unwrap();
    consumer.add_record(build_record("test", 0, 1, "key2", "value2")).unwrap();

    let expected1 = build_record("test", 0, 0, "key1", "value1");
    let expected2 = build_record("test", 0, 1, "key2", "value2");

    let recs = consumer.poll(std::time::Duration::from_millis(1)).await.unwrap();
    let mut iter = (&recs).into_iter();
    assert_eq!(Some(&expected1), iter.next());
    assert_eq!(Some(&expected2), iter.next());
    assert!(iter.next().is_none());

    let tp = TopicPartition::new("test".to_string(), 0);
    assert_eq!(2, consumer.position(&tp).await.unwrap());

    assert_eq!(1, recs.next_offsets().len());
    assert_eq!(
        &OffsetAndMetadata::with_leader_epoch(2, None, String::new()).unwrap(),
        recs.next_offsets().get(&tp).unwrap(),
    );

    consumer.commit_sync().await.unwrap();
    let committed = consumer.committed(std::slice::from_ref(&tp)).await.unwrap();
    assert_eq!(2, committed.get(&tp).unwrap().offset());
}

/// Translated from `MockConsumerTest.testConsumerRecordsIsEmptyWhenReturningNoRecords`.
#[tokio::test]
async fn test_consumer_records_is_empty_when_returning_no_records() {
    let mut consumer: MockConsumer<String, String> = MockConsumer::new(AutoOffsetResetStrategy::EARLIEST);
    let partition = TopicPartition::new("test".to_string(), 0);

    consumer.assign(vec![partition.clone()]).await.unwrap();
    consumer.add_record(build_null_record("test", 0, 0)).unwrap();

    let mut end_offsets = HashMap::new();
    end_offsets.insert(partition.clone(), 1i64);
    consumer.update_end_offsets(end_offsets);

    consumer.seek_to_end(&[partition]).await.unwrap();

    let records = consumer.poll(std::time::Duration::from_millis(1)).await.unwrap();
    assert_eq!(0, records.count());
    assert!(records.is_empty());
}

/// Translated from `MockConsumerTest.shouldNotClearRecordsForPausedPartitions`.
#[tokio::test]
async fn should_not_clear_records_for_paused_partitions() {
    let mut consumer: MockConsumer<String, String> = MockConsumer::new(AutoOffsetResetStrategy::EARLIEST);
    let partition0 = TopicPartition::new("test".to_string(), 0);
    let test_partition_list = vec![partition0.clone()];

    consumer.assign(test_partition_list.clone()).await.unwrap();
    consumer.add_record(build_null_record("test", 0, 0)).unwrap();

    let mut beginning_offsets = HashMap::new();
    beginning_offsets.insert(partition0.clone(), 0i64);
    consumer.update_beginning_offsets(beginning_offsets);

    consumer.seek_to_beginning(&test_partition_list).await.unwrap();

    consumer.pause(&test_partition_list).await.unwrap();
    let _ = consumer.poll(std::time::Duration::from_millis(1)).await.unwrap();
    consumer.resume(&test_partition_list).await.unwrap();

    let records_second_poll = consumer.poll(std::time::Duration::from_millis(1)).await.unwrap();
    assert_eq!(1, records_second_poll.count());
    assert_eq!(1, records_second_poll.next_offsets().len());
    assert_eq!(
        &OffsetAndMetadata::with_leader_epoch(1, None, String::new()).unwrap(),
        records_second_poll
            .next_offsets()
            .get(&TopicPartition::new("test".to_string(), 0))
            .unwrap(),
    );
}

/// Translated from `MockConsumerTest.endOffsetsShouldBeIdempotent`.
#[tokio::test]
async fn end_offsets_should_be_idempotent() {
    let mut consumer: MockConsumer<String, String> = MockConsumer::new(AutoOffsetResetStrategy::EARLIEST);
    let partition = TopicPartition::new("test".to_string(), 0);

    let mut end_offsets = HashMap::new();
    end_offsets.insert(partition.clone(), 10i64);
    consumer.update_end_offsets(end_offsets);

    // end_offsets should NOT change the stored end offsets.
    for _ in 0..3 {
        let got = consumer.end_offsets(std::slice::from_ref(&partition)).await.unwrap();
        assert_eq!(10, *got.get(&partition).unwrap());
    }

    let mut end_offsets = HashMap::new();
    end_offsets.insert(partition.clone(), 11i64);
    consumer.update_end_offsets(end_offsets);

    for _ in 0..3 {
        let got = consumer.end_offsets(std::slice::from_ref(&partition)).await.unwrap();
        assert_eq!(11, *got.get(&partition).unwrap());
    }
}

/// Translated from `MockConsumerTest.testDurationBasedOffsetReset`.
#[tokio::test]
async fn test_duration_based_offset_reset() {
    let strategy = AutoOffsetResetStrategy::from_string("by_duration:PT1H").unwrap();
    let mut consumer: MockConsumer<String, String> = MockConsumer::new(strategy);

    consumer.subscribe(vec!["test".to_string()]).await.unwrap();
    consumer
        .rebalance(&[
            TopicPartition::new("test".to_string(), 0),
            TopicPartition::new("test".to_string(), 1),
        ])
        .await
        .unwrap();

    let mut duration_based_offsets = HashMap::new();
    duration_based_offsets.insert(TopicPartition::new("test".to_string(), 0), 10i64);
    duration_based_offsets.insert(TopicPartition::new("test".to_string(), 1), 11i64);
    consumer.update_duration_offsets(duration_based_offsets);

    consumer.add_record(build_record("test", 0, 10, "key1", "value1")).unwrap();
    consumer.add_record(build_record("test", 0, 11, "key2", "value2")).unwrap();

    let expected1 = build_record("test", 0, 10, "key1", "value1");
    let expected2 = build_record("test", 0, 11, "key2", "value2");

    let records = consumer.poll(std::time::Duration::from_millis(1)).await.unwrap();
    let mut iter = (&records).into_iter();
    assert_eq!(Some(&expected1), iter.next());
    assert_eq!(Some(&expected2), iter.next());
    assert!(iter.next().is_none());
}

/// Records a stream of `on_partitions_revoked` / `on_partitions_assigned`
/// callback invocations from inside an `async fn`. Equivalent to Java's
/// inline `ConsumerRebalanceListener` in `testRebalanceListener`.
struct RecorderListener {
    revoked: Arc<Mutex<Vec<TopicPartition>>>,
    assigned: Arc<Mutex<Vec<TopicPartition>>>,
}

#[async_trait]
impl ConsumerRebalanceListener for RecorderListener {
    async fn on_partitions_revoked(&self, partitions: &[TopicPartition]) -> Result<(), KafkaError> {
        let mut g = self.revoked.lock().unwrap();
        g.clear();
        g.extend_from_slice(partitions);
        Ok(())
    }

    async fn on_partitions_assigned(&self, partitions: &[TopicPartition]) -> Result<(), KafkaError> {
        // Java line 156-158: skip if empty (preserves the previous list).
        if partitions.is_empty() {
            return Ok(());
        }
        let mut g = self.assigned.lock().unwrap();
        g.clear();
        g.extend_from_slice(partitions);
        Ok(())
    }
}

/// Translated from `MockConsumerTest.testRebalanceListener`.
#[tokio::test]
async fn test_rebalance_listener() {
    let mut consumer: MockConsumer<String, String> = MockConsumer::new(AutoOffsetResetStrategy::EARLIEST);

    let revoked = Arc::new(Mutex::new(Vec::<TopicPartition>::new()));
    let assigned = Arc::new(Mutex::new(Vec::<TopicPartition>::new()));
    let listener: Arc<dyn ConsumerRebalanceListener> =
        Arc::new(RecorderListener { revoked: revoked.clone(), assigned: assigned.clone() });

    consumer
        .subscribe_with_listener(vec!["test".to_string()], listener)
        .await
        .unwrap();
    assert_eq!(0, consumer.poll(std::time::Duration::ZERO).await.unwrap().count());

    let tp0 = TopicPartition::new("test".to_string(), 0);
    let tp1 = TopicPartition::new("test".to_string(), 1);
    let topic_partition_list = vec![tp0.clone(), tp1.clone()];

    consumer.rebalance(&topic_partition_list).await.unwrap();

    assert!(revoked.lock().unwrap().is_empty());
    {
        let assigned_g = assigned.lock().unwrap();
        assert_eq!(2, assigned_g.len());
        assert!(assigned_g.contains(&tp0));
        assert!(assigned_g.contains(&tp1));
    }

    // Rebalance away both — onPartitionsAssigned(empty) is suppressed (no
    // overwrite); onPartitionsRevoked carries both.
    consumer.rebalance(&[]).await.unwrap();
    assert_eq!(2, assigned.lock().unwrap().len());
    {
        let revoked_g = revoked.lock().unwrap();
        assert!(revoked_g.contains(&tp0));
        assert!(revoked_g.contains(&tp1));
    }

    consumer.rebalance(std::slice::from_ref(&tp0)).await.unwrap();
    {
        let assigned_g = assigned.lock().unwrap();
        assert_eq!(1, assigned_g.len());
        assert!(assigned_g.contains(&tp0));
    }

    consumer.rebalance(std::slice::from_ref(&tp1)).await.unwrap();
    {
        let assigned_g = assigned.lock().unwrap();
        assert_eq!(1, assigned_g.len());
        assert!(assigned_g.contains(&tp1));
    }
    {
        let revoked_g = revoked.lock().unwrap();
        assert_eq!(1, revoked_g.len());
        assert!(revoked_g.contains(&tp0));
    }
}

/// Translated from `MockConsumerTest.testRe2JPatternSubscription`.
///
/// **Dropped null-input assertions:**
/// - Java's `assertThrows(IllegalArgumentException.class, () ->
///   consumer.subscribe((SubscriptionPattern) null))` cannot be expressed
///   in Rust — the `subscribe_pattern` parameter is `SubscriptionPattern`
///   by value (not `Option<SubscriptionPattern>`), so a null call is a
///   compile-time error. The behavioral contract is preserved by type
///   non-nullability.
/// - Java's `assertThrows(IllegalArgumentException.class, () ->
///   consumer.subscribe(pattern, null))` (null listener) cannot be
///   expressed — `subscribe_pattern_with_listener` takes `Arc<dyn
///   ConsumerRebalanceListener>` (not `Option<...>`).
///
/// The remaining two assertions — empty-pattern and mixed-subscription
/// error — DO translate.
#[tokio::test]
async fn test_re2j_pattern_subscription() {
    let mut consumer: MockConsumer<String, String> = MockConsumer::new(AutoOffsetResetStrategy::EARLIEST);

    // Empty pattern → IllegalArgumentException (Java line 194).
    let err = consumer.subscribe_pattern(SubscriptionPattern::new("")).await.unwrap_err();
    assert!(matches!(err, KafkaError::IllegalArgument(_)));

    let pattern = SubscriptionPattern::new("t.*");
    consumer.subscribe_pattern(pattern).await.unwrap();
    assert!(consumer.subscription().is_empty());

    // Mixed subscription → IllegalStateException (Java line 203).
    let err = consumer.subscribe(vec!["topic1".to_string()]).await.unwrap_err();
    assert!(matches!(err, KafkaError::IllegalState(_)));
}

/// Translated from `MockConsumerTest.shouldReturnMaxPollRecords`.
#[tokio::test]
async fn should_return_max_poll_records() {
    let mut consumer: MockConsumer<String, String> = MockConsumer::new(AutoOffsetResetStrategy::EARLIEST);
    let partition = TopicPartition::new("test".to_string(), 0);

    consumer.assign(vec![partition.clone()]).await.unwrap();

    let mut beginning_offsets = HashMap::new();
    beginning_offsets.insert(partition.clone(), 0i64);
    consumer.update_beginning_offsets(beginning_offsets);

    for offset in 0..10 {
        consumer.add_record(build_null_record("test", 0, offset)).unwrap();
    }

    consumer.set_max_poll_records(2).unwrap();

    let records = consumer.poll(std::time::Duration::from_millis(1)).await.unwrap();
    assert_eq!(2, records.count());

    let records = consumer.poll(std::time::Duration::from_millis(1)).await.unwrap();
    assert_eq!(2, records.count());

    consumer.set_max_poll_records(i64::MAX).unwrap();

    let records = consumer.poll(std::time::Duration::from_millis(1)).await.unwrap();
    assert_eq!(6, records.count());

    let records = consumer.poll(std::time::Duration::from_millis(1)).await.unwrap();
    assert!(records.is_empty());
}
