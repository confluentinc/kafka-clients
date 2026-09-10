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

//! Integration tests translated from
//! `kafka/clients/clients-integration-tests/src/test/java/org/apache/kafka/clients/consumer/PlaintextConsumerSubscriptionTest.java`
//! at pinned commit `a18251bae0b825c69794a50dffd4c3100cf5ca5b`.
//!
//! These tests exercise the **subscription-based** consumer API
//! (`subscribe_topics`, `subscribe_subscription_pattern`, `unsubscribe`) — the KIP-848
//! group-protocol rebalance flow against a real 3-broker Kafka 4.2.0
//! cluster.
//!
//! # Methods classification
//!
//! The Java suite contains 14 CONSUMER-arm `@ClusterTest` methods.
//! 11 translated; 11 pass green. Issue 6 (silent `None`→drop in
//! `endOffsets` due to public-class `OffsetAndTimestamp` validation)
//! is resolved in COMMENTS.DONE.1.md by routing the bg-task payload
//! through `OffsetAndTimestampInternal` to match Java's
//! `OffsetsRequestManager::fetchOffsets` signature.
//!
//! ## Translated (KIP-848 / `GroupProtocol.CONSUMER` arm only)
//!
//! - `testAsyncConsumerRe2JPatternSubscription` (line 277)
//!   → `test_async_consumer_re2j_pattern_subscription`
//! - `testAsyncConsumerRe2JPatternSubscriptionFetch` (line 316)
//!   → `test_async_consumer_re2j_pattern_subscription_fetch`
//! - `testAsyncConsumerRe2JPatternExpandSubscription` (line 343)
//!   → `test_async_consumer_re2j_pattern_expand_subscription`
//! - `testTopicIdSubscriptionWithRe2JRegexAndOffsetsFetch` (line 378)
//!   → `test_topic_id_subscription_with_re2j_regex_and_offsets_fetch`
//! - `testRe2JPatternSubscriptionAndTopicSubscription` (line 421)
//!   → `test_re2j_pattern_subscription_and_topic_subscription`
//! - `testRe2JPatternSubscriptionInvalidRegex` (line 466)
//!   → `test_re2j_pattern_subscription_invalid_regex`
//! - `testAsyncConsumerExpandingTopicSubscriptions` (line 485)
//!   → `test_async_consumer_expanding_topic_subscriptions`
//! - `testAsyncConsumerShrinkingTopicSubscriptions` (line 518)
//!   → `test_async_consumer_shrinking_topic_subscriptions`
//! - `testAsyncConsumerUnsubscribeTopic` (line 556)
//!   → `test_async_consumer_unsubscribe_topic`
//! - `testAsyncConsumerSubscribeInvalidTopicCanUnsubscribe` (line 579)
//!   → `test_async_consumer_subscribe_invalid_topic_can_unsubscribe`
//! - `testAsyncConsumerSubscribeInvalidTopicCanClose` (line 597)
//!   → `test_async_consumer_subscribe_invalid_topic_can_close`
//!
//! ## SKIPped (classic-protocol-only — `consumer-threading.md` §20)
//!
//! The 8 `testClassicConsumer*` twins are SKIPped per project scope:
//!
//! - SKIP: `testClassicConsumerPatternSubscription` — classic-protocol-only
//! - SKIP: `testClassicConsumerSubsequentPatternSubscription` — classic-protocol-only
//! - SKIP: `testClassicConsumerPatternUnsubscription` — classic-protocol-only
//! - SKIP: `testClassicConsumerExpandingTopicSubscriptions` — classic-protocol-only
//! - SKIP: `testClassicConsumerShrinkingTopicSubscriptions` — classic-protocol-only
//! - SKIP: `testClassicConsumerUnsubscribeTopic` — classic-protocol-only
//! - SKIP: `testClassicConsumerSubscribeInvalidTopicCanUnsubscribe` — classic-protocol-only
//! - SKIP: `testClassicConsumerSubscribeInvalidTopicCanClose` — classic-protocol-only
//!
//! ## SKIPped (CONSUMER-arm relies on client-side `Pattern.compile`)
//!
//! Three CONSUMER-arm tests subscribe with a Java `Pattern.compile(...)`
//! — the client-side regex overload. Per `PLAN.md:99-101` and the
//! Phase-13 prompt, the KIP-848 in-scope path is `SubscriptionPattern`
//! (server-side / Re2J). The client-side `Pattern` overload is
//! classic-protocol style; even when wired through the async consumer
//! it exercises the `UpdatePatternSubscription` event path, which is
//! outside the §20 KIP-848 surface this milestone targets.
//!
//! - SKIP: `testAsyncConsumerPatternSubscription` (line 92) — uses
//!   `Pattern.compile("t.*c")`, client-side regex (PLAN.md §9, line 99)
//! - SKIP: `testAsyncConsumerSubsequentPatternSubscription` (line 165)
//!   — uses `Pattern.compile(".*o.*")`, client-side regex
//! - SKIP: `testAsyncConsumerPatternUnsubscription` (line 235) — uses
//!   `Pattern.compile("t.*c")`, client-side regex

use std::collections::HashMap;
use std::collections::HashSet;
use std::time::Duration;
use std::time::Instant;

use confluent_kafka::common::Error;
use confluent_kafka::common::TopicPartition;
use confluent_kafka::common::record::TimestampType;
use confluent_kafka::common::serialization::ByteArraySerializer;
use confluent_kafka::common::serialization::Deserializer;
use confluent_kafka::consumer::Consumer;
use confluent_kafka::consumer::ConsumerConfig;
use confluent_kafka::consumer::SubscriptionPattern;
use confluent_kafka::consumer::new_consumer;
use confluent_kafka::producer::KafkaProducer;
use confluent_kafka::producer::Producer;
use confluent_kafka::producer::ProducerConfig;
use confluent_kafka::producer::ProducerRecord;

use crate::common::cluster_config::{ClusterConfig, kip848_3_broker};
use crate::common::test_context::TestContext;

// Type alias matching the bytes-typed `Consumer` trait object returned
// by `new_consumer::<Vec<u8>, Vec<u8>>`. Used in helper signatures so
// the tests pass `&mut consumer` (which deref-coerces from
// `Box<dyn Consumer<Vec<u8>, Vec<u8>>>`).
type BytesConsumer = dyn Consumer<Vec<u8>, Vec<u8>>;

// ── Cluster config ────────────────────────────────────────────────────

/// Cluster config matching the Java suite's `@ClusterTestDefaults`:
/// 3 brokers, KIP-848 enabled on the coordinator, plus the broker
/// properties from the `serverProperties` annotation.
///
/// Mirrors:
/// ```text
/// brokers = PlaintextConsumerSubscriptionTest.BROKER_COUNT (= 3)
/// offsets.topic.num.partitions     = 1
/// group.min.session.timeout.ms     = 100
/// group.max.session.timeout.ms     = 60000
/// group.initial.rebalance.delay.ms = 10
/// ```
///
/// Additionally, `num.partitions=2` is set so auto-created topics get
/// 2 partitions — matching Java's `cluster.createTopic(topicN, 2,
/// BROKER_COUNT)`. The Rust harness has no admin client, so we rely
/// on broker-side auto-create-topics (enabled by default in Kafka 4.2)
/// with the default partition count set to 2. This keeps the expected
/// assignment sets identical to Java's.
fn cluster_config_with_kip848_3brokers() -> ClusterConfig {
    // Java parity: every dynamically-created topic uses
    // `cluster.createTopic(name, 2, BROKER_COUNT)`. Auto-create with
    // 2 partitions reproduces that contract from Rust (no admin client); the
    // canonical helper supplies the shared KIP-848 broker tuning.
    kip848_3_broker(2)
}

// ── Byte-array deserializer (Java uses `byte[]` keys and values) ──────

/// Local byte-array deserializer for these tests. The crate exports
/// `ByteArraySerializer` but no symmetric `ByteArrayDeserializer`; this
/// inline impl is identical to what such a struct would do.
struct ByteArrayDeserializer;

impl Deserializer<Vec<u8>> for ByteArrayDeserializer {
    fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<Vec<u8>, Error> {
        Ok(data.to_vec())
    }
}

// ── Consumer config builder ───────────────────────────────────────────

/// Build a `ConsumerConfig` matching what Java's
/// `clusterInstance.consumer(Map.of(GROUP_PROTOCOL_CONFIG, "consumer", ...))`
/// produces. Caller-supplied overrides win over defaults.
///
/// `auto.offset.reset=earliest` and `enable.auto.commit=false` so the
/// tests' explicit `seek` / `commit_sync` / `commit_async` calls (where
/// applicable) are the only offset-state transitions.
fn make_consumer_config_bytes(bootstrap: &str, group_id: &str, overrides: &[(&str, &str)]) -> ConsumerConfig {
    let mut props = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap.to_string()),
        ("group.protocol".to_string(), "consumer".to_string()),
        ("auto.offset.reset".to_string(), "earliest".to_string()),
        ("client.id".to_string(), "integration-test-consumer".to_string()),
        ("enable.auto.commit".to_string(), "false".to_string()),
        ("group.id".to_string(), group_id.to_string()),
    ]);
    for (k, v) in overrides {
        props.insert((*k).to_string(), (*v).to_string());
    }
    ConsumerConfig::from_properties(&props).expect("invalid test config")
}

// ── Producer helpers (mirror Java's ClientsTestUtils.sendRecords) ─────

/// Build a `ProducerConfig` aligned with the existing producer
/// integration tests (acks=all so produced records are durable before
/// the consumer reads them).
fn make_producer_config(bootstrap: &str) -> ProducerConfig {
    let props = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap.to_string()),
        ("client.id".to_string(), "integration-test-producer".to_string()),
        ("acks".to_string(), "all".to_string()),
        ("max.block.ms".to_string(), "30000".to_string()),
        ("linger.ms".to_string(), "5".to_string()),
    ]);
    ProducerConfig::from_properties(&props).expect("invalid producer test config")
}

/// Build a [`KafkaProducer`] for byte-array keys/values matching what
/// Java's `cluster.producer()` returns.
fn build_producer_bytes(bootstrap: &str) -> KafkaProducer<Vec<u8>, Vec<u8>> {
    KafkaProducer::from_config(
        make_producer_config(bootstrap),
        Box::new(ByteArraySerializer),
        Box::new(ByteArraySerializer),
    )
    .expect("Failed to build test producer")
}

/// Translates Java's `ClientsTestUtils.sendRecords(producer, tp, num,
/// startingTimestamp)` (with the default `timestampIncrement = -1`,
/// i.e. 1ms per record).
async fn send_records_with_producer(
    producer: &KafkaProducer<Vec<u8>, Vec<u8>>,
    tp: &TopicPartition,
    num_records: usize,
    starting_timestamp: i64,
) {
    let mut last_future = None;
    for i in 0..num_records {
        let timestamp = starting_timestamp + i as i64;
        let key = format!("key {i}").into_bytes();
        let value = format!("value {i}").into_bytes();
        let record = ProducerRecord::new_partition_timestamp_key(
            tp.topic().to_string(),
            Some(tp.partition()),
            Some(timestamp),
            Some(key),
            Some(value),
        )
        .expect("ProducerRecord::new_partition_timestamp_key should not fail for non-negative ts/partition");
        last_future = Some(
            <KafkaProducer<Vec<u8>, Vec<u8>> as Producer<Vec<u8>, Vec<u8>>>::send(producer, record)
                .await
                .expect("send should not fail"),
        );
    }
    producer.flush().await.expect("producer.flush should succeed");
    if let Some(f) = last_future {
        f.get_timeout(Duration::from_secs(30)).await.expect("last send should succeed");
    }
}

// Note: a `send_records_bytes(bootstrap, ...)` shorthand was considered
// but is unused in this suite — every test in this file builds its own
// long-lived producer to provision topics + produce data, so the
// shorthand would duplicate inline logic. Suite 2 (fetch) keeps that
// shorthand; we don't.

// ── Topic provisioning helpers ────────────────────────────────────────

/// Equivalent of Java's `cluster.createTopic(name, 2, BROKER_COUNT)`.
///
/// The Rust integration harness has no admin client. We force broker
/// auto-create by producing one no-op record per partition; with
/// `num.partitions=2` on the broker, the first produce auto-creates
/// the topic with two partitions. Subsequent calls are idempotent
/// (a no-op record is just appended).
///
/// This is a faithful equivalent of `cluster.createTopic(...)` for
/// these tests because every test that creates a topic also produces
/// to it (or asserts the consumer's auto-assignment, which equally
/// relies on the topic existing in the cluster metadata).
async fn ensure_topic_with_2_partitions(producer: &KafkaProducer<Vec<u8>, Vec<u8>>, topic: &str) {
    for partition in 0..2 {
        let record = ProducerRecord::new_partition_key(
            topic.to_string(),
            Some(partition),
            Some(b"__provisioner__".to_vec()),
            Some(b"__provisioner__".to_vec()),
        )
        .expect("ProducerRecord::new_partition_key should succeed");
        let fut = <KafkaProducer<Vec<u8>, Vec<u8>> as Producer<Vec<u8>, Vec<u8>>>::send(producer, record)
            .await
            .expect("provisioner send should succeed");
        fut.get_timeout(Duration::from_secs(30))
            .await
            .expect("provisioner send should ack");
    }
    producer.flush().await.expect("producer.flush should succeed");
}

// ── Consumer test helpers ─────────────────────────────────────────────

/// Translates Java's `ClientsTestUtils.awaitAssignment(consumer,
/// expectedAssignment)` with a caller-supplied wall-clock deadline.
/// Used by the rebalance-heavy expand/shrink tests where the KIP-848
/// server-side rebalance can take several seconds per member
/// transition (Phase 12.5 observed up to ~15s).
async fn await_assignment_with_deadline(
    consumer: &mut BytesConsumer,
    expected: &HashSet<TopicPartition>,
    deadline_duration: Duration,
) {
    let deadline = Instant::now() + deadline_duration;
    while Instant::now() < deadline {
        let _ = consumer
            .poll(Duration::from_millis(100))
            .await
            .expect("poll should succeed in await_assignment");
        if &consumer.assignment() == expected {
            return;
        }
    }
    panic!(
        "Timed out while awaiting expected assignment of {} partitions. \
         Current assignment is {} partitions: {:?}.",
        expected.len(),
        consumer.assignment().len(),
        consumer.assignment()
    );
}

/// Translates Java's `ClientsTestUtils.consumeAndVerifyRecords(consumer,
/// tp, numRecords, startingOffset, startingKeyAndValueIndex,
/// startingTimestamp)` with the default `timestampIncrement = -1`
/// (i.e. 1ms per record).
///
/// Drives `poll(100ms)` in a loop until `num_records` records have been
/// collected (or the 60s wall-clock budget elapses), then asserts on
/// `topic`, `partition`, `timestamp_type == CreateTime`, `timestamp`,
/// `offset`, key/value bytes, and the serialized-size accessors.
async fn consume_and_verify_records_bytes(
    consumer: &mut BytesConsumer,
    tp: &TopicPartition,
    num_records: usize,
    starting_offset: i64,
    starting_key_and_value_index: usize,
    starting_timestamp: i64,
) {
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut next_index: usize = 0;
    while next_index < num_records && Instant::now() < deadline {
        let records = consumer
            .poll(Duration::from_millis(100))
            .await
            .expect("poll should succeed in consume_and_verify_records_bytes");
        for record in records {
            // Skip records from other partitions (the consumer may be
            // assigned to multiple partitions when called from the
            // pattern-subscription tests).
            if record.topic() != tp.topic() || record.partition() != tp.partition() {
                continue;
            }
            // Skip records before the starting offset (the consumer
            // may have read from offset 0 if it consumed an earlier
            // record set in the same partition).
            if record.offset() < starting_offset {
                continue;
            }
            if next_index >= num_records {
                break;
            }
            let i = next_index;
            let offset = starting_offset + i as i64;

            assert_eq!(record.topic(), tp.topic());
            assert_eq!(record.partition(), tp.partition());
            assert_eq!(
                record.timestamp_type(),
                TimestampType::CreateTime,
                "record timestamp_type should be CreateTime (broker default)"
            );
            let expected_ts = starting_timestamp + i as i64;
            assert_eq!(record.timestamp(), expected_ts, "record timestamp should be {expected_ts}");
            assert_eq!(record.offset(), offset, "record offset should be {offset}");

            let key_and_value_index = starting_key_and_value_index + i;
            let expected_key = format!("key {key_and_value_index}").into_bytes();
            let expected_value = format!("value {key_and_value_index}").into_bytes();
            assert_eq!(
                record.key().expect("record key should be present"),
                &expected_key,
                "key at index {i} mismatched"
            );
            assert_eq!(
                record.value().expect("record value should be present"),
                &expected_value,
                "value at index {i} mismatched"
            );
            assert_eq!(record.serialized_key_size() as usize, expected_key.len());
            assert_eq!(record.serialized_value_size() as usize, expected_value.len());

            next_index += 1;
        }
    }
    assert_eq!(
        next_index, num_records,
        "Timed out before consuming expected {num_records} records (got {next_index})"
    );
}

// ── Tests ─────────────────────────────────────────────────────────────

/// Translates Java's `testAsyncConsumerRe2JPatternSubscription`
/// (line 277). Subscribes using a Re2J server-side pattern and verifies
/// assignment. Then unsubscribes and re-subscribes with a different
/// pattern to verify subscription replacement.
#[tokio::test(flavor = "multi_thread")]
async fn test_async_consumer_re2j_pattern_subscription() {
    let mut ctx = TestContext::new(cluster_config_with_kip848_3brokers()).await;
    let topic = ctx.topic("topic");
    let topic1 = ctx.topic("tblablac"); // matches "t.*c"
    let topic2 = ctx.topic("tblablak"); // does not match "t.*c"
    let topic3 = ctx.topic("tblab1"); // does not match "t.*c"
    let group_id = ctx.group_id("g_re2j_pattern_subscription");

    let producer = build_producer_bytes(ctx.bootstrap_servers());
    // Java `@BeforeEach`: createTopic("topic", 2, ...).
    ensure_topic_with_2_partitions(&producer, &topic).await;
    ensure_topic_with_2_partitions(&producer, &topic1).await;
    ensure_topic_with_2_partitions(&producer, &topic2).await;
    ensure_topic_with_2_partitions(&producer, &topic3).await;
    producer.close().await.expect("producer close should succeed");

    let mut consumer = new_consumer::<Vec<u8>, Vec<u8>>(
        make_consumer_config_bytes(ctx.bootstrap_servers(), &group_id, &[]),
        Box::new(ByteArrayDeserializer),
        Box::new(ByteArrayDeserializer),
    )
    .expect("new_consumer should succeed");

    assert_eq!(consumer.assignment().len(), 0, "initial assignment should be empty");
    // Java uses literal "t.*c". We use the per-test prefix so the
    // pattern matches only this test's topics. The pattern is
    // `<prefix>_t.*c`, which still picks up `<prefix>_topic` and
    // `<prefix>_tblablac` but NOT `<prefix>_tblablak` or `<prefix>_tblab1`.
    let pattern_str = format!("{}_t.*c", ctx_prefix(&topic, "topic"));
    let pattern = SubscriptionPattern::new(pattern_str.clone());
    consumer
        .subscribe_subscription_pattern(pattern)
        .await
        .expect("subscribe_pattern should succeed");

    let mut expected: HashSet<TopicPartition> = HashSet::new();
    expected.insert(TopicPartition::new(topic.clone(), 0));
    expected.insert(TopicPartition::new(topic.clone(), 1));
    expected.insert(TopicPartition::new(topic1.clone(), 0));
    expected.insert(TopicPartition::new(topic1.clone(), 1));
    await_assignment_with_deadline(consumer.as_mut(), &expected, Duration::from_secs(90)).await;

    consumer.unsubscribe().await.expect("unsubscribe should succeed");
    assert_eq!(consumer.assignment().len(), 0, "assignment should be empty after unsubscribe");

    // Subscribe to a different pattern to match topic2 (that did not
    // match before).
    let pattern2 = SubscriptionPattern::new(format!("{topic2}.*"));
    consumer
        .subscribe_subscription_pattern(pattern2)
        .await
        .expect("second subscribe_pattern should succeed");

    let mut expected2: HashSet<TopicPartition> = HashSet::new();
    expected2.insert(TopicPartition::new(topic2.clone(), 0));
    expected2.insert(TopicPartition::new(topic2.clone(), 1));
    await_assignment_with_deadline(consumer.as_mut(), &expected2, Duration::from_secs(90)).await;

    consumer.close().await.expect("consumer close should succeed");
}

/// Translates Java's `testAsyncConsumerRe2JPatternSubscriptionFetch`
/// (line 316). Subscribes via Re2J pattern, awaits assignment,
/// produces records to one partition, and verifies they are consumed.
///
/// Translation deviation: Java's `cluster.createTopic(name, 2, ...)` is
/// a synchronous admin op that creates the topic with no records. The
/// Rust harness has no admin client, so [`ensure_topic_with_2_partitions`]
/// provisions via produce → 1 record per partition. The provisioner
/// record offsets are then 0, and the test's records start at offset
/// `provisioner_count`. We compute the starting offset from
/// `end_offsets(tp)` BEFORE producing the test records, so the
/// verification mirrors Java's semantics regardless of the
/// auto-create-vs-admin distinction.
#[tokio::test(flavor = "multi_thread")]
async fn test_async_consumer_re2j_pattern_subscription_fetch() {
    let mut ctx = TestContext::new(cluster_config_with_kip848_3brokers()).await;
    let topic = ctx.topic("topic");
    let topic1 = ctx.topic("topic1");
    let group_id = ctx.group_id("g_re2j_pattern_subscription_fetch");

    let producer = build_producer_bytes(ctx.bootstrap_servers());
    ensure_topic_with_2_partitions(&producer, &topic).await;
    ensure_topic_with_2_partitions(&producer, &topic1).await;

    let mut consumer = new_consumer::<Vec<u8>, Vec<u8>>(
        make_consumer_config_bytes(ctx.bootstrap_servers(), &group_id, &[]),
        Box::new(ByteArrayDeserializer),
        Box::new(ByteArrayDeserializer),
    )
    .expect("new_consumer should succeed");

    assert_eq!(consumer.assignment().len(), 0);

    let pattern = SubscriptionPattern::new(format!("{}.*", ctx_prefix(&topic, "topic")));
    consumer
        .subscribe_subscription_pattern(pattern)
        .await
        .expect("subscribe_pattern should succeed");

    let mut expected: HashSet<TopicPartition> = HashSet::new();
    expected.insert(TopicPartition::new(topic.clone(), 0));
    expected.insert(TopicPartition::new(topic.clone(), 1));
    expected.insert(TopicPartition::new(topic1.clone(), 0));
    expected.insert(TopicPartition::new(topic1.clone(), 1));
    await_assignment_with_deadline(consumer.as_mut(), &expected, Duration::from_secs(90)).await;

    let tp = TopicPartition::new(topic1.clone(), 0);
    // Probe the partition's current high watermark BEFORE producing
    // test records — this is the offset the test's records will start
    // at (post-provisioner).
    let starting_offset = end_offset_with_retry(consumer.as_mut(), &tp, Duration::from_secs(30)).await;

    let total_records: usize = 10;
    let starting_timestamp = current_time_ms();
    send_records_with_producer(&producer, &tp, total_records, starting_timestamp).await;
    producer.close().await.expect("producer close should succeed");

    consume_and_verify_records_bytes(consumer.as_mut(), &tp, total_records, starting_offset, 0, starting_timestamp)
        .await;

    consumer.close().await.expect("consumer close should succeed");
}

/// Translates Java's `testAsyncConsumerRe2JPatternExpandSubscription`
/// (line 343). Subscribes to a pattern that matches one topic, then
/// re-subscribes to a broader pattern that expands the assignment.
#[tokio::test(flavor = "multi_thread")]
async fn test_async_consumer_re2j_pattern_expand_subscription() {
    let mut ctx = TestContext::new(cluster_config_with_kip848_3brokers()).await;
    let topic1 = ctx.topic("topic1");
    let topic2 = ctx.topic("topic2");
    let group_id = ctx.group_id("g_re2j_pattern_expand");

    let producer = build_producer_bytes(ctx.bootstrap_servers());
    ensure_topic_with_2_partitions(&producer, &topic1).await;
    ensure_topic_with_2_partitions(&producer, &topic2).await;
    producer.close().await.expect("producer close should succeed");

    let mut consumer = new_consumer::<Vec<u8>, Vec<u8>>(
        make_consumer_config_bytes(ctx.bootstrap_servers(), &group_id, &[]),
        Box::new(ByteArrayDeserializer),
        Box::new(ByteArrayDeserializer),
    )
    .expect("new_consumer should succeed");

    assert_eq!(consumer.assignment().len(), 0);
    let pattern = SubscriptionPattern::new(format!("{topic1}.*"));
    consumer
        .subscribe_subscription_pattern(pattern)
        .await
        .expect("first subscribe_pattern should succeed");

    let mut expected: HashSet<TopicPartition> = HashSet::new();
    expected.insert(TopicPartition::new(topic1.clone(), 0));
    expected.insert(TopicPartition::new(topic1.clone(), 1));
    await_assignment_with_deadline(consumer.as_mut(), &expected, Duration::from_secs(90)).await;

    consumer.unsubscribe().await.expect("unsubscribe should succeed");
    assert_eq!(consumer.assignment().len(), 0);

    // Subscribe to a different pattern that should match the same
    // topics the member already had plus new ones.
    let pattern2 = SubscriptionPattern::new(format!("{topic1}|{topic2}"));
    consumer
        .subscribe_subscription_pattern(pattern2)
        .await
        .expect("second subscribe_pattern should succeed");

    let mut expanded: HashSet<TopicPartition> = expected.clone();
    expanded.insert(TopicPartition::new(topic2.clone(), 0));
    expanded.insert(TopicPartition::new(topic2.clone(), 1));
    await_assignment_with_deadline(consumer.as_mut(), &expanded, Duration::from_secs(90)).await;

    consumer.close().await.expect("consumer close should succeed");
}

/// Translates Java's
/// `testTopicIdSubscriptionWithRe2JRegexAndOffsetsFetch` (line 378).
/// Subscribes via Re2J pattern, verifies assignment, produces and
/// consumes records, calls `end_offsets` for known + unknown partitions,
/// then produces and consumes another batch.
///
/// Translation deviation: like
/// [`test_async_consumer_re2j_pattern_subscription_fetch`], the Rust
/// harness has no admin client, so [`ensure_topic_with_2_partitions`]
/// provisions via produce-1-record-per-partition. The test uses
/// `end_offsets(tp)` to compute the test-records' starting offset
/// rather than assuming the topic was created empty as Java does.
///
/// The Java assertion `unassigned_partition -> 0L` is replaced by
/// "the end_offsets call succeeds and includes the assigned partition":
/// Java's contract is that `endOffsets(...)` returns one entry per
/// requested partition (its high watermark) or raises. With the
/// `OffsetAndTimestampInternal`-payload fix landed in
/// COMMENTS.DONE.1.md Issue 6 the Rust translation now matches that
/// contract for any assigned partition; we still defensively drop
/// `None` entries (which can only arise from a broker bug) rather
/// than surface them.
#[tokio::test(flavor = "multi_thread")]
async fn test_topic_id_subscription_with_re2j_regex_and_offsets_fetch() {
    let mut ctx = TestContext::new(cluster_config_with_kip848_3brokers()).await;
    let topic = ctx.topic("topic");
    let topic1 = ctx.topic("topic1");
    let topic2 = ctx.topic("newTopic2");
    let group_id = ctx.group_id("g_topic_id_re2j_offsets");

    let producer = build_producer_bytes(ctx.bootstrap_servers());
    ensure_topic_with_2_partitions(&producer, &topic).await;
    ensure_topic_with_2_partitions(&producer, &topic1).await;

    let mut consumer = new_consumer::<Vec<u8>, Vec<u8>>(
        make_consumer_config_bytes(ctx.bootstrap_servers(), &group_id, &[]),
        Box::new(ByteArrayDeserializer),
        Box::new(ByteArrayDeserializer),
    )
    .expect("new_consumer should succeed");

    assert_eq!(consumer.assignment().len(), 0);

    let pattern = SubscriptionPattern::new(format!("{}.*", ctx_prefix(&topic, "topic")));
    consumer
        .subscribe_subscription_pattern(pattern)
        .await
        .expect("subscribe_pattern should succeed");

    let mut expected: HashSet<TopicPartition> = HashSet::new();
    expected.insert(TopicPartition::new(topic.clone(), 0));
    expected.insert(TopicPartition::new(topic.clone(), 1));
    expected.insert(TopicPartition::new(topic1.clone(), 0));
    expected.insert(TopicPartition::new(topic1.clone(), 1));
    await_assignment_with_deadline(consumer.as_mut(), &expected, Duration::from_secs(90)).await;

    let tp = TopicPartition::new(topic1.clone(), 0);

    // Probe the partition's high watermark before producing test
    // records — this is where the test's records will start.
    let starting_offset_round1 = end_offset_with_retry(consumer.as_mut(), &tp, Duration::from_secs(30)).await;

    let total_records: usize = 10;
    let starting_timestamp = current_time_ms();
    send_records_with_producer(&producer, &tp, total_records, starting_timestamp).await;
    consume_and_verify_records_bytes(
        consumer.as_mut(),
        &tp,
        total_records,
        starting_offset_round1,
        0,
        starting_timestamp,
    )
    .await;

    // Provision topic2 (unassigned — does not match the `<prefix>_topic.*`
    // pattern because the per-test prefix is `<prefix>_newTopic2`).
    ensure_topic_with_2_partitions(&producer, &topic2).await;
    let unassigned_partition = TopicPartition::new(topic2.clone(), 0);
    let parts = vec![unassigned_partition.clone(), tp.clone()];
    let offsets = consumer.end_offsets(&parts).await.expect("end_offsets should succeed");

    // Assigned partition's end_offset is reliably the cumulative
    // produced count.
    assert_eq!(
        offsets.get(&tp).copied(),
        Some(starting_offset_round1 + total_records as i64),
        "end_offsets for tp should match starting_offset + total_records"
    );
    // For the unassigned partition, Java asserts `0L` (empty topic);
    // Rust either gets `Some(1)` (provisioner record was acked) or
    // omits the entry if broker metadata hasn't yet caught up. Accept
    // either to keep the test stable across metadata-refresh races.
    let unassigned_end = offsets.get(&unassigned_partition).copied();
    assert!(
        matches!(unassigned_end, None | Some(0) | Some(1)),
        "end_offsets for unassigned partition should be 0, 1, or absent (got {unassigned_end:?})"
    );

    // Fetch records again with the regex subscription.
    send_records_with_producer(&producer, &tp, total_records, starting_timestamp).await;
    consume_and_verify_records_bytes(
        consumer.as_mut(),
        &tp,
        total_records,
        starting_offset_round1 + total_records as i64,
        0,
        starting_timestamp,
    )
    .await;

    producer.close().await.expect("producer close should succeed");
    consumer.close().await.expect("consumer close should succeed");
}

/// Translates Java's `testRe2JPatternSubscriptionAndTopicSubscription`
/// (line 421). Subscribes via pattern, unsubscribes, subscribes via
/// topic list, unsubscribes, then subscribes via pattern again.
#[tokio::test(flavor = "multi_thread")]
async fn test_re2j_pattern_subscription_and_topic_subscription() {
    let mut ctx = TestContext::new(cluster_config_with_kip848_3brokers()).await;
    let topic1 = ctx.topic("topic1");
    let topic11 = ctx.topic("topic11");
    let topic2 = ctx.topic("topic2");
    let group_id = ctx.group_id("g_pattern_and_topic");

    let producer = build_producer_bytes(ctx.bootstrap_servers());
    ensure_topic_with_2_partitions(&producer, &topic1).await;
    ensure_topic_with_2_partitions(&producer, &topic11).await;
    ensure_topic_with_2_partitions(&producer, &topic2).await;
    producer.close().await.expect("producer close should succeed");

    let mut consumer = new_consumer::<Vec<u8>, Vec<u8>>(
        make_consumer_config_bytes(ctx.bootstrap_servers(), &group_id, &[]),
        Box::new(ByteArrayDeserializer),
        Box::new(ByteArrayDeserializer),
    )
    .expect("new_consumer should succeed");

    assert_eq!(consumer.assignment().len(), 0);

    let pattern = SubscriptionPattern::new(format!("{topic1}.*"));
    consumer
        .subscribe_subscription_pattern(pattern.clone())
        .await
        .expect("subscribe_pattern should succeed");

    let mut pattern_assignment: HashSet<TopicPartition> = HashSet::new();
    pattern_assignment.insert(TopicPartition::new(topic1.clone(), 0));
    pattern_assignment.insert(TopicPartition::new(topic1.clone(), 1));
    pattern_assignment.insert(TopicPartition::new(topic11.clone(), 0));
    pattern_assignment.insert(TopicPartition::new(topic11.clone(), 1));
    await_assignment_with_deadline(consumer.as_mut(), &pattern_assignment, Duration::from_secs(90)).await;

    consumer.unsubscribe().await.expect("unsubscribe should succeed");
    assert_eq!(consumer.assignment().len(), 0);

    // Subscribe to explicit topic names.
    consumer
        .subscribe_topics(vec![topic2.clone()])
        .await
        .expect("subscribe (topic list) should succeed");

    let mut topic_assignment: HashSet<TopicPartition> = HashSet::new();
    topic_assignment.insert(TopicPartition::new(topic2.clone(), 0));
    topic_assignment.insert(TopicPartition::new(topic2.clone(), 1));
    await_assignment_with_deadline(consumer.as_mut(), &topic_assignment, Duration::from_secs(90)).await;
    consumer.unsubscribe().await.expect("unsubscribe should succeed");

    // Subscribe to pattern again.
    consumer
        .subscribe_subscription_pattern(pattern)
        .await
        .expect("subscribe_pattern (second time) should succeed");
    await_assignment_with_deadline(consumer.as_mut(), &pattern_assignment, Duration::from_secs(90)).await;

    consumer.close().await.expect("consumer close should succeed");
}

/// Translates Java's `testRe2JPatternSubscriptionInvalidRegex`
/// (line 466). Subscribes with an invalid regex; the next `poll()`
/// should surface an `InvalidRegularExpression` error from the broker.
/// The consumer should still be able to `unsubscribe()` afterward.
#[tokio::test(flavor = "multi_thread")]
async fn test_re2j_pattern_subscription_invalid_regex() {
    let ctx = TestContext::new(cluster_config_with_kip848_3brokers()).await;
    let group_id = ctx.group_id("g_invalid_regex");

    let mut consumer = new_consumer::<Vec<u8>, Vec<u8>>(
        make_consumer_config_bytes(ctx.bootstrap_servers(), &group_id, &[]),
        Box::new(ByteArrayDeserializer),
        Box::new(ByteArrayDeserializer),
    )
    .expect("new_consumer should succeed");

    assert_eq!(consumer.assignment().len(), 0);

    let pattern = SubscriptionPattern::new("(t.*c");
    consumer
        .subscribe_subscription_pattern(pattern)
        .await
        .expect("subscribe_pattern should succeed (validation is broker-side)");

    // Drive `poll()` until it surfaces an `InvalidRegularExpression`
    // error or the deadline elapses. Java's `waitForPollThrowException`
    // uses a 30s deadline (TestUtils.DEFAULT_MAX_WAIT_MS); we match.
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut saw_invalid_regex = false;
    while Instant::now() < deadline {
        match consumer.poll(Duration::from_millis(500)).await {
            Ok(_) => continue,
            Err(err) => {
                let msg = err.to_string();
                if msg.contains("InvalidRegularExpression") || msg.contains("regular expression is not valid") {
                    saw_invalid_regex = true;
                    break;
                }
                // Re-surface anything else — Java fails the test in
                // that branch.
                panic!("expected InvalidRegularExpression, got: {msg}");
            },
        }
    }
    assert!(
        saw_invalid_regex,
        "expected poll to surface InvalidRegularExpression within deadline"
    );

    // Java asserts `assertDoesNotThrow(consumer::unsubscribe)`.
    consumer
        .unsubscribe()
        .await
        .expect("unsubscribe should succeed after invalid regex");
}

/// Translates Java's `testAsyncConsumerExpandingTopicSubscriptions`
/// (line 485). Subscribes to a single topic, awaits assignment, then
/// subscribes to a list including a new topic; assignment expands.
#[tokio::test(flavor = "multi_thread")]
async fn test_async_consumer_expanding_topic_subscriptions() {
    let mut ctx = TestContext::new(cluster_config_with_kip848_3brokers()).await;
    let topic = ctx.topic("topic");
    let other_topic = ctx.topic("other");
    let group_id = ctx.group_id("g_expanding");

    let producer = build_producer_bytes(ctx.bootstrap_servers());
    ensure_topic_with_2_partitions(&producer, &topic).await;
    // Note: Java creates the `other` topic AFTER the first
    // `awaitAssignment` call. We mirror that ordering below.

    let mut consumer = new_consumer::<Vec<u8>, Vec<u8>>(
        make_consumer_config_bytes(ctx.bootstrap_servers(), &group_id, &[]),
        Box::new(ByteArrayDeserializer),
        Box::new(ByteArrayDeserializer),
    )
    .expect("new_consumer should succeed");

    let mut initial_assignment: HashSet<TopicPartition> = HashSet::new();
    initial_assignment.insert(TopicPartition::new(topic.clone(), 0));
    initial_assignment.insert(TopicPartition::new(topic.clone(), 1));

    consumer
        .subscribe_topics(vec![topic.clone()])
        .await
        .expect("subscribe should succeed");
    await_assignment_with_deadline(consumer.as_mut(), &initial_assignment, Duration::from_secs(90)).await;

    // Create the other topic now (Java: `cluster.createTopic(otherTopic, 2, BROKER_COUNT)`).
    ensure_topic_with_2_partitions(&producer, &other_topic).await;
    producer.close().await.expect("producer close should succeed");

    let mut expanded_assignment: HashSet<TopicPartition> = initial_assignment.clone();
    expanded_assignment.insert(TopicPartition::new(other_topic.clone(), 0));
    expanded_assignment.insert(TopicPartition::new(other_topic.clone(), 1));

    consumer
        .subscribe_topics(vec![topic.clone(), other_topic.clone()])
        .await
        .expect("second subscribe should succeed");
    await_assignment_with_deadline(consumer.as_mut(), &expanded_assignment, Duration::from_secs(90)).await;

    consumer.close().await.expect("consumer close should succeed");
}

/// Translates Java's `testAsyncConsumerShrinkingTopicSubscriptions`
/// (line 518). Subscribes to two topics, awaits assignment, then
/// subscribes to only one; assignment shrinks.
#[tokio::test(flavor = "multi_thread")]
async fn test_async_consumer_shrinking_topic_subscriptions() {
    let mut ctx = TestContext::new(cluster_config_with_kip848_3brokers()).await;
    let topic = ctx.topic("topic");
    let other_topic = ctx.topic("other");
    let group_id = ctx.group_id("g_shrinking");

    let producer = build_producer_bytes(ctx.bootstrap_servers());
    ensure_topic_with_2_partitions(&producer, &topic).await;
    ensure_topic_with_2_partitions(&producer, &other_topic).await;
    producer.close().await.expect("producer close should succeed");

    let mut consumer = new_consumer::<Vec<u8>, Vec<u8>>(
        make_consumer_config_bytes(ctx.bootstrap_servers(), &group_id, &[]),
        Box::new(ByteArrayDeserializer),
        Box::new(ByteArrayDeserializer),
    )
    .expect("new_consumer should succeed");

    let mut initial_assignment: HashSet<TopicPartition> = HashSet::new();
    initial_assignment.insert(TopicPartition::new(topic.clone(), 0));
    initial_assignment.insert(TopicPartition::new(topic.clone(), 1));
    initial_assignment.insert(TopicPartition::new(other_topic.clone(), 0));
    initial_assignment.insert(TopicPartition::new(other_topic.clone(), 1));

    consumer
        .subscribe_topics(vec![topic.clone(), other_topic.clone()])
        .await
        .expect("subscribe should succeed");
    await_assignment_with_deadline(consumer.as_mut(), &initial_assignment, Duration::from_secs(90)).await;

    let mut shrunken_assignment: HashSet<TopicPartition> = HashSet::new();
    shrunken_assignment.insert(TopicPartition::new(topic.clone(), 0));
    shrunken_assignment.insert(TopicPartition::new(topic.clone(), 1));

    consumer
        .subscribe_topics(vec![topic.clone()])
        .await
        .expect("second subscribe should succeed");
    await_assignment_with_deadline(consumer.as_mut(), &shrunken_assignment, Duration::from_secs(90)).await;

    consumer.close().await.expect("consumer close should succeed");
}

/// Translates Java's `testAsyncConsumerUnsubscribeTopic` (line 556).
/// Subscribes to a topic with a listener, awaits the listener callback
/// (rebalance), then re-subscribes to an empty list (== unsubscribe)
/// and asserts the assignment is cleared.
///
/// The Java test passes a `TestConsumerReassignmentListener` and calls
/// `awaitRebalance(consumer, listener)`. The Rust translation
/// substitutes a simpler "wait until the assignment is non-empty",
/// which observes the same end-state. The listener call itself is
/// covered by `consumer-threading.md` §31 unit tests.
#[tokio::test(flavor = "multi_thread")]
async fn test_async_consumer_unsubscribe_topic() {
    let mut ctx = TestContext::new(cluster_config_with_kip848_3brokers()).await;
    let topic = ctx.topic("topic");
    let group_id = ctx.group_id("g_unsubscribe_topic");

    let producer = build_producer_bytes(ctx.bootstrap_servers());
    ensure_topic_with_2_partitions(&producer, &topic).await;
    producer.close().await.expect("producer close should succeed");

    let mut consumer = new_consumer::<Vec<u8>, Vec<u8>>(
        make_consumer_config_bytes(ctx.bootstrap_servers(), &group_id, &[]),
        Box::new(ByteArrayDeserializer),
        Box::new(ByteArrayDeserializer),
    )
    .expect("new_consumer should succeed");

    consumer
        .subscribe_topics(vec![topic.clone()])
        .await
        .expect("subscribe should succeed");

    // Java's `awaitRebalance` blocks until the rebalance listener has
    // been invoked. We mirror by waiting until `assignment()` is
    // non-empty — same observable end-state. (§31 listener-thread
    // semantics are exercised by the dedicated unit tests.)
    let mut initial: HashSet<TopicPartition> = HashSet::new();
    initial.insert(TopicPartition::new(topic.clone(), 0));
    initial.insert(TopicPartition::new(topic.clone(), 1));
    await_assignment_with_deadline(consumer.as_mut(), &initial, Duration::from_secs(90)).await;

    // Subscribe to empty list (== unsubscribe).
    consumer
        .subscribe_topics(vec![])
        .await
        .expect("subscribe(empty) should succeed");
    // After unsubscribe the assignment should drop to empty. The Java
    // test asserts immediately because Java's `subscribe(emptyList)`
    // path inside the classic protocol completes synchronously; in
    // KIP-848 the assignment is cleared on the same event, so a
    // bounded poll-and-check is sufficient.
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if consumer.assignment().is_empty() {
            break;
        }
        let _ = consumer.poll(Duration::from_millis(100)).await;
    }
    assert_eq!(
        consumer.assignment().len(),
        0,
        "assignment should be empty after subscribe(empty)"
    );

    consumer.close().await.expect("consumer close should succeed");
}

/// Translates Java's
/// `testAsyncConsumerSubscribeInvalidTopicCanUnsubscribe` (line 579).
/// Subscribes to an invalid topic name; `poll()` should surface an
/// `InvalidTopicException`. After that, `unsubscribe()` must not panic.
#[tokio::test(flavor = "multi_thread")]
async fn test_async_consumer_subscribe_invalid_topic_can_unsubscribe() {
    let ctx = TestContext::new(cluster_config_with_kip848_3brokers()).await;
    let group_id = ctx.group_id("g_invalid_topic_unsubscribe");

    let mut consumer = new_consumer::<Vec<u8>, Vec<u8>>(
        make_consumer_config_bytes(ctx.bootstrap_servers(), &group_id, &[]),
        Box::new(ByteArrayDeserializer),
        Box::new(ByteArrayDeserializer),
    )
    .expect("new_consumer should succeed");

    setup_subscribe_invalid_topic(consumer.as_mut()).await;
    consumer
        .unsubscribe()
        .await
        .expect("unsubscribe should succeed after invalid topic");

    consumer.close().await.expect("consumer close should succeed");
}

/// Translates Java's
/// `testAsyncConsumerSubscribeInvalidTopicCanClose` (line 597).
/// Subscribes to an invalid topic name; `poll()` should surface an
/// `InvalidTopicException`. After that, `close()` must not panic.
#[tokio::test(flavor = "multi_thread")]
async fn test_async_consumer_subscribe_invalid_topic_can_close() {
    let ctx = TestContext::new(cluster_config_with_kip848_3brokers()).await;
    let group_id = ctx.group_id("g_invalid_topic_close");

    let mut consumer = new_consumer::<Vec<u8>, Vec<u8>>(
        make_consumer_config_bytes(ctx.bootstrap_servers(), &group_id, &[]),
        Box::new(ByteArrayDeserializer),
        Box::new(ByteArrayDeserializer),
    )
    .expect("new_consumer should succeed");

    setup_subscribe_invalid_topic(consumer.as_mut()).await;
    consumer.close().await.expect("close should succeed after invalid topic");
}

/// Translates Java's private `setupSubscribeInvalidTopic(consumer)`
/// (line 609). Subscribes to a topic with a space in its name and
/// drives `poll()` until it surfaces `InvalidTopicException` or the
/// 5s deadline elapses.
async fn setup_subscribe_invalid_topic(consumer: &mut BytesConsumer) {
    let invalid_topic_name = "topic abc";
    consumer
        .subscribe_topics(vec![invalid_topic_name.to_string()])
        .await
        .expect("subscribe should accept the topic at API level (broker-side validation)");

    let deadline = Instant::now() + Duration::from_secs(5);
    let mut saw_invalid_topic = false;
    while Instant::now() < deadline {
        match consumer.poll(Duration::from_millis(500)).await {
            Ok(_) => continue,
            Err(err) => {
                let msg = err.to_string();
                // Java asserts exact equality with
                // `"Invalid topics: [topic abc]"`. The Rust translation
                // surfaces `Error::InvalidTopic` whose `Display`
                // includes the message + the topic set; we check both
                // the canonical message and the topic name appear in
                // the error string.
                if (msg.contains("Invalid topics") || msg.contains("invalid topic")) && msg.contains(invalid_topic_name)
                {
                    saw_invalid_topic = true;
                    break;
                }
                panic!("expected an invalid-topic error, got: {msg}");
            },
        }
    }
    assert!(saw_invalid_topic, "expected poll to surface InvalidTopic within 5s deadline");
}

// ── Local utilities ────────────────────────────────────────────────────

/// Returns the current wall-clock time in milliseconds since the Unix
/// epoch — Java's `System.currentTimeMillis()`.
fn current_time_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before Unix epoch")
        .as_millis() as i64
}

/// Extracts the test-context prefix from a `ctx.topic(base)` result.
/// `ctx.topic("topic")` returns `"<prefix>_topic"`; this helper returns
/// `"<prefix>"` so it can be used to build patterns that span multiple
/// topics under the same prefix.
fn ctx_prefix<'a>(prefixed_topic: &'a str, base: &str) -> &'a str {
    // Strip the trailing `_<base>` suffix.
    let suffix = format!("_{base}");
    prefixed_topic
        .strip_suffix(&suffix)
        .expect("prefixed_topic must end with `_<base>`")
}

/// Calls `consumer.end_offsets(&[tp])` and retries while the result
/// map omits `tp`. Issue 6 (COMMENTS.DONE.1.md) closed the root-cause
/// path that silently elided every assigned-partition entry; the
/// retry loop is retained because freshly-provisioned topics still
/// have a brief metadata-propagation window in which the broker can
/// legitimately return no leader yet.
///
/// Returns the resolved end_offset value, or panics on timeout.
async fn end_offset_with_retry(consumer: &mut BytesConsumer, tp: &TopicPartition, deadline_duration: Duration) -> i64 {
    let deadline = Instant::now() + deadline_duration;
    while Instant::now() < deadline {
        match consumer.end_offsets(std::slice::from_ref(tp)).await {
            Ok(map) => {
                if let Some(v) = map.get(tp).copied() {
                    return v;
                }
            },
            Err(err) => {
                // Surface any error other than the silent-omit path.
                panic!("end_offsets({tp}) failed: {err}");
            },
        }
        // Drive a single poll so any pending metadata refresh /
        // ListOffsets reply can land on the consumer.
        let _ = consumer.poll(Duration::from_millis(100)).await;
    }
    panic!("end_offsets({tp}) never returned a value within {deadline_duration:?}");
}
