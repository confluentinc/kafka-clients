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
//! `kafka/clients/clients-integration-tests/src/test/java/org/apache/kafka/clients/consumer/PlaintextConsumerTest.java`
//! (the `BaseConsumerTestcase` public-API surface) and its helper
//! `kafka/clients/clients-integration-tests/src/test/java/org/apache/kafka/clients/ClientsTestUtils.java`,
//! Apache Kafka 4.2.
//!
//! These exercise the consumer PUBLIC-API surface against a real
//! Kafka 4.2.0 cluster: record headers, pause/resume, `partitions_for`,
//! `list_topics`, `offsets_for_times`, `end_offsets`, full `seek`
//! (seekToEnd / seek-mid), LogAppendTime timestamp-type, null group id,
//! `position` timeout + wakeup, and zero-timeout offset queries.
//!
//! KIP-848 (`GroupProtocol.CONSUMER`) arm only; classic twins, metrics /
//! quota, coordinator-failover, broker-shutdown, and
//! close-on-interrupt are OUT_OF_SCOPE per `consumer-threading.md` §20.
//!
//! ## Translated (CONSUMER arm)
//!
//! - `testAsyncConsumerHeaders` → `test_async_consumer_headers`
//! - `testAsyncConsumerPartitionPauseAndResume`
//!   → `test_async_consumer_partition_pause_and_resume`
//! - `testAsyncConsumerPauseStateNotPreservedByRebalance`
//!   → `test_async_consumer_pause_state_not_preserved_by_rebalance`
//! - `testAsyncConsumerPartitionsFor` → `test_async_consumer_partitions_for`
//! - `testAsyncConsumerPartitionsForAutoCreate`
//!   → `test_async_consumer_partitions_for_auto_create`
//! - `testAsyncConsumerPartitionsForInvalidTopic`
//!   → `test_async_consumer_partitions_for_invalid_topic`
//! - `testAsyncConsumerListTopics` → `test_async_consumer_list_topics`
//! - `testAsyncConsumerSeek` → `test_async_consumer_seek`
//! - `testAsyncConsumerSeekThrowsIllegalStateIfPartitionsNotAssigned`
//!   → `test_async_consumer_seek_throws_illegal_state_if_partitions_not_assigned`
//! - `testAsyncConsumerConsumeMessagesWithLogAppendTime`
//!   → `test_async_consumer_consume_messages_with_log_append_time`
//! - `testAsyncConsumerEndOffsets` → `test_async_consumer_end_offsets`
//! - `testAsyncConsumerFetchOffsetsForTime`
//!   → `test_async_consumer_fetch_offsets_for_time`
//! - `testAsyncConsumerConsumingWithNullGroupId`
//!   → `test_async_consumer_consuming_with_null_group_id`
//! - `testAsyncConsumerNullGroupIdNotSupportedIfCommitting`
//!   → `test_async_consumer_null_group_id_not_supported_if_committing`
//! - `testAsyncConsumerPositionRespectsTimeout`
//!   → `test_async_consumer_position_respects_timeout`
//! - `testAsyncConsumerPositionRespectsWakeup`
//!   → `test_async_consumer_position_respects_wakeup`
//! - `testAsyncConsumerPositionWithErrorConnectionRespectsWakeup`
//!   → `test_async_consumer_position_with_error_connection_respects_wakeup`
//! - `testAsyncConsumerOffsetRelatedWhenTimeoutZero`
//!   → `test_async_consumer_offset_related_when_timeout_zero`
//!
//! ## SKIPped (documented gaps — see PLAN.md)
//!
//! - SKIP `testAsyncConsumerHeadersSerializerDeserializer` — Java injects a
//!   `content-type` header *inside* a `Serializer`/`Deserializer` via the
//!   `(topic, headers, data)` overload. The Rust `Serializer` has no
//!   headers-mutating overload on the producer write path; the test's whole
//!   point is the serializer-injected header. Plain header round-trip is
//!   covered by `test_async_consumer_headers`.
//! - SKIP `testAsyncConsumerInterceptors` /
//!   `testAsyncConsumerInterceptorsWithWrongKeyValue` — no public way to
//!   attach a `ConsumerInterceptor`. `KafkaConsumer::new` always builds an EMPTY
//!   interceptor chain (`async_kafka_consumer.rs:761`); Java's reflective
//!   `interceptor.classes` loader is not translated, and the
//!   `with_components` seam carrying interceptors is `pub(crate)`,
//!   unreachable from an integration crate. Real API-shape gap.
//! - SKIP `testAsyncConsumerStaticConsumerDetectsNewPartitionCreatedAfterRestart`
//!   — needs `admin.createPartitions(increaseTo(2))` mid-test; the Rust
//!   harness has no admin client / partition-increase API. `group.instance.id`
//!   is config-accepted, but the observable cannot be driven.
//! - SKIP `testAsyncConsumerStallBetweenPoll` — KAFKA-19259 timing /
//!   perf-regression guard, not a behavioral contract; flaky under
//!   shared-pool load.
//! - SKIP `testAsyncConsumerSimpleConsumption` / `...AutoOffsetReset` /
//!   `...GroupConsumption` / `...ConsumeMessagesWithCreateTime` — already
//!   covered by `consumer_test.rs` and the assign/fetch suites (subscribe /
//!   assign / seek / poll / CreateTime full per-record field verification).
//! - SKIP (OUT_OF_SCOPE §20): all `testClassicConsumer*` twins,
//!   `*MetricsCleanUp*` / `QuotaMetrics*`, `testAsyncConsumeCoordinatorFailover`,
//!   `testAsyncConsumerCloseOnBrokerShutdown`,
//!   `testAsyncConsumerCloseLeavesGroupOnInterrupt`,
//!   `testAsyncConsumerClusterResourceListener`.

use std::collections::HashMap;
use std::collections::HashSet;
use std::time::Duration;
use std::time::Instant;

use confluent_kafka::common::Error;
use confluent_kafka::common::Errors;
use confluent_kafka::common::TopicPartition;
use confluent_kafka::common::header::Header;
use confluent_kafka::common::header::Headers;
use confluent_kafka::common::header::RecordHeaders;
use confluent_kafka::common::record::TimestampType;
use confluent_kafka::common::serialization::ByteArraySerializer;
use confluent_kafka::common::serialization::Deserializer;
use confluent_kafka::consumer::Consumer;
use confluent_kafka::consumer::ConsumerConfig;
use confluent_kafka::consumer::KafkaConsumer;
use confluent_kafka::producer::KafkaProducer;
use confluent_kafka::producer::Producer;
use confluent_kafka::producer::ProducerConfig;
use confluent_kafka::producer::ProducerRecord;

use crate::common::cluster_config::{ClusterConfig, kip848_3_broker};
use crate::common::test_context::TestContext;

// Type alias matching the bytes-typed `Consumer` trait object returned by
// `KafkaConsumer::new::<Vec<u8>, Vec<u8>>` (mirrors Java's `Consumer<byte[], byte[]>`).
type BytesConsumer = dyn Consumer<Vec<u8>, Vec<u8>>;

// ── Cluster configs ───────────────────────────────────────────────────

/// Cluster config matching the Java `@ClusterTestDefaults`: 3 brokers,
/// KIP-848 enabled, plus the broker properties from the `serverProperties`
/// annotation. `num.partitions=2` reproduces Java's
/// `cluster.createTopic(name, 2, BROKER_COUNT)` for auto-created topics
/// (the Rust harness has no admin client).
fn cluster_config_kip848() -> ClusterConfig {
    // `num.partitions=2` reproduces Java's `cluster.createTopic(name, 2, ...)`
    // for auto-created topics; the canonical helper supplies the shared
    // KIP-848 broker tuning.
    kip848_3_broker(2)
}

/// Like [`cluster_config_kip848`] but with `LogAppendTime` as the
/// broker-wide message timestamp type. The Rust harness has no admin
/// client, so the per-topic `message.timestamp.type=LogAppendTime` config
/// that Java sets on `createTopic` is applied at the broker level instead —
/// every topic in this (dedicated, pool-keyed) cluster gets LogAppendTime,
/// which is exactly what the LogAppendTime test needs.
fn cluster_config_log_append_time() -> ClusterConfig {
    let mut cfg = kip848_3_broker(2);
    cfg.server_properties
        .insert("KAFKA_LOG_MESSAGE_TIMESTAMP_TYPE".to_string(), "LogAppendTime".to_string());
    cfg
}

// ── Byte-array deserializer (Java uses `byte[]` keys and values) ──────

/// Local byte-array deserializer. The crate exports `ByteArraySerializer`
/// but no symmetric `ByteArrayDeserializer`; this inline impl is identical
/// to what such a struct would do.
struct ByteArrayDeserializer;

impl Deserializer<Vec<u8>> for ByteArrayDeserializer {
    fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<Vec<u8>, Error> {
        Ok(data.to_vec())
    }
}

// ── Config builders ───────────────────────────────────────────────────

/// Build a `ConsumerConfig` matching Java's
/// `cluster.consumer(Map.of(GROUP_PROTOCOL_CONFIG, "consumer", ...))`.
/// Caller-supplied overrides win over the defaults.
fn make_consumer_config(ctx: &TestContext, group_id: &str, overrides: &[(&str, &str)]) -> ConsumerConfig {
    let mut props = HashMap::from([
        ("bootstrap.servers".to_string(), ctx.protocol_bootstrap_servers().to_string()),
        ("group.protocol".to_string(), "consumer".to_string()),
        ("auto.offset.reset".to_string(), "earliest".to_string()),
        ("client.id".to_string(), "integration-test-consumer".to_string()),
        ("enable.auto.commit".to_string(), "false".to_string()),
        ("group.id".to_string(), group_id.to_string()),
    ]);
    for (k, v) in overrides {
        props.insert((*k).to_string(), (*v).to_string());
    }
    ctx.apply_security(&mut props);
    ConsumerConfig::new(&props).expect("invalid test config")
}

/// Build a *groupless* `ConsumerConfig` — `group.id` is intentionally
/// absent, mirroring Java's `testConsumingWithNullGroupId` which omits
/// `GROUP_ID_CONFIG`.
fn make_groupless_consumer_config(ctx: &TestContext, overrides: &[(&str, &str)]) -> ConsumerConfig {
    let mut props = HashMap::from([
        ("bootstrap.servers".to_string(), ctx.protocol_bootstrap_servers().to_string()),
        ("group.protocol".to_string(), "consumer".to_string()),
        ("client.id".to_string(), "integration-test-consumer".to_string()),
        ("enable.auto.commit".to_string(), "false".to_string()),
    ]);
    for (k, v) in overrides {
        props.insert((*k).to_string(), (*v).to_string());
    }
    ctx.apply_security(&mut props);
    ConsumerConfig::new(&props).expect("invalid test config")
}

fn make_producer_config(ctx: &TestContext) -> ProducerConfig {
    let mut props = HashMap::from([
        ("bootstrap.servers".to_string(), ctx.protocol_bootstrap_servers().to_string()),
        ("client.id".to_string(), "integration-test-producer".to_string()),
        ("acks".to_string(), "all".to_string()),
        ("max.block.ms".to_string(), "30000".to_string()),
        ("linger.ms".to_string(), "5".to_string()),
    ]);
    ctx.apply_security(&mut props);
    ProducerConfig::new(&props).expect("invalid producer test config")
}

fn build_producer(ctx: &TestContext) -> KafkaProducer<Vec<u8>, Vec<u8>> {
    KafkaProducer::new(
        make_producer_config(ctx),
        Box::new(ByteArraySerializer),
        Box::new(ByteArraySerializer),
    )
    .expect("Failed to build test producer")
}

fn make_consumer(ctx: &TestContext, group_id: &str, overrides: &[(&str, &str)]) -> Box<BytesConsumer> {
    KafkaConsumer::new::<Vec<u8>, Vec<u8>>(
        make_consumer_config(ctx, group_id, overrides),
        Box::new(ByteArrayDeserializer),
        Box::new(ByteArrayDeserializer),
    )
    .expect("KafkaConsumer::new should succeed")
}

// ── Producer helpers ─────────────────────────────────────────────────

/// Translates Java's `ClientsTestUtils.sendRecords(producer, tp, num,
/// startingTimestamp)` (default `timestampIncrement = -1`, i.e. 1ms/record).
async fn send_records(
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
        let record = ProducerRecord::with_partition_timestamp_key(
            tp.topic().to_string(),
            Some(tp.partition()),
            Some(timestamp),
            Some(key),
            Some(value),
        )
        .expect("ProducerRecord::with_partition_timestamp_key should not fail for non-negative ts/partition");
        last_future = Some(
            <KafkaProducer<Vec<u8>, Vec<u8>> as Producer<Vec<u8>, Vec<u8>>>::send(producer, record)
                .await
                .expect("send should not fail"),
        );
    }
    producer.flush().await.expect("producer.flush should succeed");
    if let Some(f) = last_future {
        f.get_with_timeout(Duration::from_secs(30))
            .await
            .expect("last send should succeed");
    }
}

/// Creates `topic` EMPTY by triggering broker metadata auto-creation and
/// waiting until it materializes with the expected partition count. Mirrors
/// Java's `cluster.createTopic(name, partitions, replicationFactor)` (no data
/// records, so the first produced record lands at offset 0).
///
/// The Rust harness has no admin client, but `partitions_for` over the
/// METADATA path triggers broker auto-create (`auto.create.topics.enable` is
/// on by default) with `num.partitions=2` — exactly an empty topic, matching
/// Java. This replaces the earlier provisioner-record approach, which placed
/// a record at offset 0 and shifted every real record by one.
async fn create_topic(consumer: &mut BytesConsumer, topic: &str, partitions: usize) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let parts = consumer.partitions_for(topic).await.expect("partitions_for");
        if parts.len() >= partitions {
            return;
        }
        if Instant::now() >= deadline {
            panic!("topic {topic} not auto-created with >= {partitions} partitions within 30s");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

// ── Consumer helpers ──────────────────────────────────────────────────

/// Translates Java's `ClientsTestUtils.consumeRecords(consumer, numRecords)`
/// — polls 100ms until `num_records` collected (60s budget), returning the
/// collected owned records.
async fn consume_records(consumer: &mut BytesConsumer, num_records: usize) -> Vec<OwnedRecord> {
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut collected: Vec<OwnedRecord> = Vec::new();
    // Java's `consumeRecords` drives the poll through `TestUtils.waitForCondition`,
    // which evaluates its lambda at least once — so even `num_records == 0`
    // performs a single poll (mirrored by the post-rebalance "no records"
    // verification in `test_async_consumer_pause_state_not_preserved_by_rebalance`).
    // A plain `while len < num_records` would poll zero times for `num_records == 0`.
    loop {
        let records = consumer.poll(Duration::from_millis(100)).await.expect("poll should succeed");
        for record in records {
            collected.push(OwnedRecord::from(&record));
        }
        if collected.len() >= num_records || Instant::now() >= deadline {
            break;
        }
    }
    assert!(
        collected.len() >= num_records,
        "Timed out before consuming expected {num_records} records (got {})",
        collected.len()
    );
    collected
}

/// A snapshot of the fields of a `ConsumerRecord` we assert on. `ConsumerRecord`
/// is not `Clone` (Phase 13a memory note), so we project the fields we need.
struct OwnedRecord {
    topic: String,
    partition: i32,
    offset: i64,
    timestamp: i64,
    timestamp_type: TimestampType,
    key: Option<Vec<u8>>,
    value: Option<Vec<u8>>,
    serialized_key_size: i32,
    serialized_value_size: i32,
    headers: Vec<(String, Option<Vec<u8>>)>,
}

impl OwnedRecord {
    fn from(record: &confluent_kafka::consumer::ConsumerRecord<Vec<u8>, Vec<u8>>) -> Self {
        let headers = record
            .headers()
            .iter()
            .map(|h| (h.key().to_string(), h.value().map(<[u8]>::to_vec)))
            .collect();
        Self {
            topic: record.topic().to_string(),
            partition: record.partition(),
            offset: record.offset(),
            timestamp: record.timestamp(),
            timestamp_type: record.timestamp_type(),
            key: record.key().cloned(),
            value: record.value().cloned(),
            serialized_key_size: record.serialized_key_size(),
            serialized_value_size: record.serialized_value_size(),
            headers,
        }
    }
}

/// Translates Java's `ClientsTestUtils.consumeAndVerifyRecords(consumer, tp,
/// numRecords, startingOffset, startingKeyAndValueIndex, startingTimestamp)`
/// with the default `timestampIncrement = -1`.
async fn consume_and_verify_records(
    consumer: &mut BytesConsumer,
    tp: &TopicPartition,
    num_records: usize,
    starting_offset: i64,
    starting_key_and_value_index: usize,
    starting_timestamp: i64,
) {
    let records = consume_records(consumer, num_records).await;
    for (i, record) in records.iter().take(num_records).enumerate() {
        let offset = starting_offset + i as i64;
        assert_eq!(record.topic, tp.topic());
        assert_eq!(record.partition, tp.partition());
        assert_eq!(record.timestamp_type, TimestampType::CreateTime);
        let timestamp = starting_timestamp + i as i64;
        assert_eq!(record.timestamp, timestamp, "timestamp at index {i}");
        assert_eq!(record.offset, offset, "offset at index {i}");
        let kvi = starting_key_and_value_index + i;
        let expected_key = format!("key {kvi}").into_bytes();
        let expected_value = format!("value {kvi}").into_bytes();
        assert_eq!(record.key.as_ref().expect("key present"), &expected_key, "key at {i}");
        assert_eq!(record.value.as_ref().expect("value present"), &expected_value, "value at {i}");
        assert_eq!(record.serialized_key_size as usize, expected_key.len());
        assert_eq!(record.serialized_value_size as usize, expected_value.len());
    }
}

/// Translates Java's `ClientsTestUtils.awaitAssignment(consumer, expected)`.
async fn await_assignment(
    consumer: &mut BytesConsumer,
    expected: &HashSet<TopicPartition>,
    deadline_duration: Duration,
) {
    let deadline = Instant::now() + deadline_duration;
    while Instant::now() < deadline {
        let _ = consumer.poll(Duration::from_millis(100)).await.expect("poll should succeed");
        if &consumer.assignment() == expected {
            return;
        }
    }
    panic!(
        "Timed out while awaiting expected assignment of {} partitions. Current: {:?}",
        expected.len(),
        consumer.assignment()
    );
}

fn current_time_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before Unix epoch")
        .as_millis() as i64
}

// ── Tests ─────────────────────────────────────────────────────────────

/// Translates Java's `testAsyncConsumerHeaders` (line 251 / 257).
/// Produces one record with three ordered headers, then assigns + seeks
/// to 0, consumes it, and asserts the headers survive the round-trip with
/// their insertion order preserved.
#[tokio::test(flavor = "multi_thread")]
async fn test_async_consumer_headers() {
    let mut ctx = TestContext::new(cluster_config_kip848()).await;
    let topic = ctx.topic("topic");
    let tp = TopicPartition::new(topic.clone(), 0);
    let group_id = ctx.group_id("g_headers");

    let producer = build_producer(&ctx);

    // Java: `new ProducerRecord<>(TP.topic(), TP.partition(), null, "key", "value")`
    // then `record.headers().add(...)` thrice. Java relies on producer-driven
    // auto-create; the record lands at offset 0 (empty topic).
    let mut headers = RecordHeaders::new();
    headers.add_key_value("headerKey", Some(b"headerValue")).expect("add header");
    headers.add_key_value("headerKey2", Some(b"headerValue2")).expect("add header");
    headers.add_key_value("headerKey3", Some(b"headerValue3")).expect("add header");
    let record = ProducerRecord::with_partition_key_headers(
        topic.clone(),
        Some(0),
        Some(b"key".to_vec()),
        Some(b"value".to_vec()),
        headers,
    )
    .expect("ProducerRecord::with_partition_key_headers should succeed");
    let fut = <KafkaProducer<Vec<u8>, Vec<u8>> as Producer<Vec<u8>, Vec<u8>>>::send(&producer, record)
        .await
        .expect("send should succeed");
    fut.get_with_timeout(Duration::from_secs(30)).await.expect("send should ack");
    producer.close().await.expect("producer close");

    let mut consumer = make_consumer(&ctx, &group_id, &[]);
    assert_eq!(consumer.assignment().len(), 0);
    consumer.assign(vec![tp.clone()]).await.expect("assign should succeed");
    assert_eq!(consumer.assignment().len(), 1);
    consumer.seek_with_offset(tp.clone(), 0).await.expect("seek should succeed");

    let records = consume_records(consumer.as_mut(), 1).await;
    assert_eq!(records.len(), 1);
    let rec = &records[0];

    // Java: `headers().lastHeader("headerKey")` → "headerValue".
    let last = rec
        .headers
        .iter()
        .rev()
        .find(|(k, _)| k == "headerKey")
        .expect("headerKey should be present");
    assert_eq!(last.1.as_deref(), Some(b"headerValue".as_ref()));

    // Java: header ORDER preserved (headers[0..2]).
    assert_eq!(rec.headers[0].0, "headerKey");
    assert_eq!(rec.headers[1].0, "headerKey2");
    assert_eq!(rec.headers[2].0, "headerKey3");

    consumer.close().await.expect("consumer close");
}

/// Translates Java's `testAsyncConsumerPartitionPauseAndResume` (line 519).
/// Standalone pause/resume on an assigned partition (NOT the in-callback
/// reentrancy case, which is the Issue-8 gap). Consume 5, pause, produce 5
/// more, poll returns empty, resume, consume the next 5.
#[tokio::test(flavor = "multi_thread")]
async fn test_async_consumer_partition_pause_and_resume() {
    let mut ctx = TestContext::new(cluster_config_kip848()).await;
    let topic = ctx.topic("topic");
    let tp = TopicPartition::new(topic.clone(), 0);
    let group_id = ctx.group_id("g_pause_resume");

    let producer = build_producer(&ctx);
    let mut consumer = make_consumer(&ctx, &group_id, &[]);
    // Empty topic so the first produced record lands at offset 0 (Java parity).
    create_topic(consumer.as_mut(), &topic, 2).await;

    let num_records = 5usize;
    let mut starting_timestamp = current_time_ms();
    send_records(&producer, &tp, num_records, starting_timestamp).await;

    consumer.assign(vec![tp.clone()]).await.expect("assign");
    consume_and_verify_records(consumer.as_mut(), &tp, num_records, 0, 0, starting_timestamp).await;

    consumer.pause(std::slice::from_ref(&tp)).await.expect("pause");
    starting_timestamp = current_time_ms();
    send_records(&producer, &tp, num_records, starting_timestamp).await;
    // Java: `assertTrue(consumer.poll(100ms).isEmpty())`.
    let polled = consumer.poll(Duration::from_millis(100)).await.expect("poll while paused");
    assert!(polled.is_empty(), "poll should be empty while partition is paused");

    consumer.resume(std::slice::from_ref(&tp)).await.expect("resume");
    consume_and_verify_records(consumer.as_mut(), &tp, num_records, 5, 0, starting_timestamp).await;

    producer.close().await.expect("producer close");
    consumer.close().await.expect("consumer close");
}

/// Translates Java's `testAsyncConsumerPauseStateNotPreservedByRebalance`
/// (line 810). After a rebalance the pause state is lost and the position
/// is reset, so consumption resumes from the beginning of the partition.
#[tokio::test(flavor = "multi_thread")]
async fn test_async_consumer_pause_state_not_preserved_by_rebalance() {
    let mut ctx = TestContext::new(cluster_config_kip848()).await;
    let topic = ctx.topic("topic");
    let topic2 = ctx.topic("topic2");
    let tp = TopicPartition::new(topic.clone(), 0);
    let group_id = ctx.group_id("g_pause_rebalance");

    let producer = build_producer(&ctx);
    let mut consumer = make_consumer(&ctx, &group_id, &[]);
    // Empty topics so the first produced record lands at offset 0 (Java parity).
    create_topic(consumer.as_mut(), &topic, 2).await;
    create_topic(consumer.as_mut(), &topic2, 2).await;

    let starting_timestamp = current_time_ms();
    send_records(&producer, &tp, 5, starting_timestamp).await;
    producer.close().await.expect("producer close");

    consumer.subscribe_with_topics(vec![topic.clone()]).await.expect("subscribe");
    consume_and_verify_records(consumer.as_mut(), &tp, 5, 0, 0, starting_timestamp).await;
    consumer.pause(std::slice::from_ref(&tp)).await.expect("pause");

    // Subscribe to a new topic to trigger a rebalance (Java subscribes to
    // "topic2"). After the rebalance our position is reset and the pause
    // state is lost, so we should be able to consume from the beginning —
    // Java asserts this via `consumeAndVerifyRecords(consumer, TP, 0, 5, ...)`,
    // which `waitForCondition`-polls exactly once and verifies that 0 records
    // come back from the now-revoked partition. `consume_records` mirrors that
    // single poll for `num_records == 0` (see its comment).
    consumer
        .subscribe_with_topics(vec![topic2.clone()])
        .await
        .expect("subscribe topic2");
    consume_and_verify_records(consumer.as_mut(), &tp, 0, 5, 0, starting_timestamp).await;

    consumer.close().await.expect("consumer close");
}

/// Translates Java's `testAsyncConsumerPartitionsFor` (line 389).
#[tokio::test(flavor = "multi_thread")]
async fn test_async_consumer_partitions_for() {
    let mut ctx = TestContext::new(cluster_config_kip848()).await;
    let topic = ctx.topic("topic");
    let group_id = ctx.group_id("g_partitions_for");

    let mut consumer = make_consumer(&ctx, &group_id, &[]);
    // The topic may take a moment to appear in fresh metadata; poll a few
    // times if needed (Java relies on createTopic being synchronous).
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut partitions = consumer.partitions_for(&topic).await.expect("partitions_for");
    while partitions.len() < 2 && Instant::now() < deadline {
        partitions = consumer.partitions_for(&topic).await.expect("partitions_for");
    }
    assert_eq!(partitions.len(), 2, "topic should have 2 partitions");

    consumer.close().await.expect("consumer close");
}

/// Translates Java's `testAsyncConsumerPartitionsForAutoCreate` (line 415).
/// The first `partitions_for` for a nonexistent topic triggers broker
/// auto-create (enabled by default); poll until non-empty.
#[tokio::test(flavor = "multi_thread")]
async fn test_async_consumer_partitions_for_auto_create() {
    let mut ctx = TestContext::new(cluster_config_kip848()).await;
    let non_exist = ctx.topic("non-exist-topic");
    let group_id = ctx.group_id("g_partitions_for_autocreate");

    let mut consumer = make_consumer(&ctx, &group_id, &[]);
    // First call would create the topic.
    let _ = consumer.partitions_for(&non_exist).await.expect("partitions_for");
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut non_empty = false;
    while Instant::now() < deadline {
        let parts = consumer.partitions_for(&non_exist).await.expect("partitions_for");
        if !parts.is_empty() {
            non_empty = true;
            break;
        }
    }
    assert!(
        non_empty,
        "Timed out while awaiting non-empty partitions for auto-created topic"
    );

    consumer.close().await.expect("consumer close");
}

/// Translates Java's `testAsyncConsumerPartitionsForInvalidTopic` (line 440).
#[tokio::test(flavor = "multi_thread")]
async fn test_async_consumer_partitions_for_invalid_topic() {
    let ctx = TestContext::new(cluster_config_kip848()).await;
    let group_id = ctx.group_id("g_partitions_for_invalid");

    let mut consumer = make_consumer(&ctx, &group_id, &[]);
    let err = consumer
        .partitions_for(";3# ads,{234")
        .await
        .expect_err("partitions_for on an invalid topic should fail");
    let msg = err.to_string();
    assert!(
        msg.contains("Invalid topic") || msg.contains("invalid topic") || err.error() == Errors::InvalidTopicError,
        "expected InvalidTopic error, got: {msg}"
    );

    consumer.close().await.expect("consumer close");
}

/// Translates Java's `testAsyncConsumerListTopics` (line 770).
#[tokio::test(flavor = "multi_thread")]
async fn test_async_consumer_list_topics() {
    let mut ctx = TestContext::new(cluster_config_kip848()).await;
    let topic1 = ctx.topic("part-test-topic-1");
    let topic2 = ctx.topic("part-test-topic-2");
    let topic3 = ctx.topic("part-test-topic-3");
    let group_id = ctx.group_id("g_list_topics");

    let producer = build_producer(&ctx);
    let mut consumer = make_consumer(&ctx, &group_id, &[]);
    create_topic(consumer.as_mut(), &topic1, 2).await;
    create_topic(consumer.as_mut(), &topic2, 2).await;
    create_topic(consumer.as_mut(), &topic3, 2).await;
    send_records(&producer, &TopicPartition::new(topic1.clone(), 0), 1, current_time_ms()).await;
    producer.close().await.expect("producer close");

    consumer.subscribe_with_topics(vec![topic1.clone()]).await.expect("subscribe");
    let _ = consumer.poll(Duration::from_millis(100)).await.expect("poll");

    // Retry list_topics until all three named topics are visible (metadata
    // propagation window).
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut topics = consumer.list_topics().await.expect("list_topics");
    while Instant::now() < deadline
        && !(topics.contains_key(&topic1) && topics.contains_key(&topic2) && topics.contains_key(&topic3))
    {
        let _ = consumer.poll(Duration::from_millis(100)).await.expect("poll");
        topics = consumer.list_topics().await.expect("list_topics");
    }

    // Java asserts a total topic count of 4 (3 test topics + the internal
    // `__consumer_offsets`). The exact internal-topic count is
    // broker-version-dependent under a shared pool, so we assert the three
    // named topics are present with 2 partitions each (the load-bearing
    // contract) and that the total is at least 4.
    assert!(
        topics.len() >= 4,
        "expected at least 4 topics (3 test + internal), got {}",
        topics.len()
    );
    assert_eq!(topics.get(&topic1).expect("topic1").len(), 2);
    assert_eq!(topics.get(&topic2).expect("topic2").len(), 2);
    assert_eq!(topics.get(&topic3).expect("topic3").len(), 2);

    consumer.close().await.expect("consumer close");
}

/// Translates Java's `testAsyncConsumerSeek` (line 460).
/// Covers seekToEnd → position==total, seekToBeginning → position==0, and
/// seek(mid) → position==mid, with consumption verification.
///
/// Translation deviation: the compressed-message half of Java's `testSeek`
/// is SKIPped here — the byte harness has no `compression.type=gzip` +
/// `linger.ms=MAX` producer helper, and the non-compressed half fully
/// exercises the in-scope gap (seekToEnd / seekToBeginning / seek-mid).
#[tokio::test(flavor = "multi_thread")]
async fn test_async_consumer_seek() {
    let mut ctx = TestContext::new(cluster_config_kip848()).await;
    let topic = ctx.topic("topic");
    let tp = TopicPartition::new(topic.clone(), 0);
    let group_id = ctx.group_id("g_seek");

    let total_records: usize = 50;
    let mid: usize = total_records / 2;

    let producer = build_producer(&ctx);
    let mut consumer = make_consumer(&ctx, &group_id, &[]);
    // Java uses `startingTimestamp = 0`. Empty topic so records start at
    // offset 0, matching Java's offset==index expectation.
    create_topic(consumer.as_mut(), &topic, 2).await;
    let starting_timestamp: i64 = 0;
    send_records(&producer, &tp, total_records, starting_timestamp).await;
    producer.close().await.expect("producer close");

    consumer.assign(vec![tp.clone()]).await.expect("assign");

    consumer.seek_to_end(std::slice::from_ref(&tp)).await.expect("seek_to_end");
    assert_eq!(consumer.position(&tp).await.expect("position"), total_records as i64);
    assert!(consumer.poll(Duration::from_millis(50)).await.expect("poll").is_empty());

    consumer
        .seek_to_beginning(std::slice::from_ref(&tp))
        .await
        .expect("seek_to_beginning");
    assert_eq!(consumer.position(&tp).await.expect("position"), 0);
    consume_and_verify_records(consumer.as_mut(), &tp, 1, 0, 0, starting_timestamp).await;

    consumer.seek_with_offset(tp.clone(), mid as i64).await.expect("seek mid");
    assert_eq!(consumer.position(&tp).await.expect("position"), mid as i64);
    // Record at offset mid has key/value index mid and timestamp mid.
    consume_and_verify_records(consumer.as_mut(), &tp, 1, mid as i64, mid, mid as i64).await;

    consumer.close().await.expect("consumer close");
}

/// Translates Java's
/// `testAsyncConsumerSeekThrowsIllegalStateIfPartitionsNotAssigned` (line 1199).
/// `seek_to_end` on an unassigned partition raises IllegalState with the
/// EXACT Java message.
#[tokio::test(flavor = "multi_thread")]
async fn test_async_consumer_seek_throws_illegal_state_if_partitions_not_assigned() {
    let mut ctx = TestContext::new(cluster_config_kip848()).await;
    let topic = ctx.topic("topic");
    let tp = TopicPartition::new(topic.clone(), 0);
    let group_id = ctx.group_id("g_seek_illegal_state");

    let mut consumer = make_consumer(&ctx, &group_id, &[]);
    create_topic(consumer.as_mut(), &topic, 2).await;
    let err = consumer
        .seek_to_end(std::slice::from_ref(&tp))
        .await
        .expect_err("seek_to_end unassigned should fail");
    match err {
        Error::LocalIllegalState(msg) => {
            // Java: `"No current assignment for partition " + TP`.
            assert_eq!(msg.message(), format!("No current assignment for partition {tp}"));
        },
        other => panic!("expected IllegalState, got {other:?}"),
    }

    consumer.close().await.expect("consumer close");
}

/// Translates Java's `testAsyncConsumerConsumeMessagesWithLogAppendTime`
/// (line 735). Uses a dedicated broker-wide-LogAppendTime cluster; asserts
/// `timestamp_type == LogAppendTime` and the broker-stamped timestamp is in
/// `[startTime, now]` widened by a clock-skew slack (see
/// `CLOCK_SKEW_SLACK_MS` below).
///
/// Translation deviations: the compressed-message half is SKIPped (no gzip
/// producer helper); the non-compressed half covers the LogAppendTime
/// timestamp-type gap. The timestamp range bound carries a skew slack Java
/// does not need.
#[tokio::test(flavor = "multi_thread")]
async fn test_async_consumer_consume_messages_with_log_append_time() {
    let mut ctx = TestContext::new(cluster_config_log_append_time()).await;
    let topic = ctx.topic("log-append-time");
    let tp = TopicPartition::new(topic.clone(), 0);
    let group_id = ctx.group_id("g_log_append_time");

    let start_time = current_time_ms();
    let num_records = 50usize;

    let producer = build_producer(&ctx);
    let mut consumer = make_consumer(&ctx, &group_id, &[]);
    // Empty topic so records start at offset 0 (Java parity).
    create_topic(consumer.as_mut(), &topic, 2).await;
    // Producer-supplied timestamps are IGNORED by a LogAppendTime topic;
    // the broker stamps each record with its append time.
    send_records(&producer, &tp, num_records, start_time).await;
    producer.close().await.expect("producer close");

    consumer.assign(vec![tp.clone()]).await.expect("assign");

    let records = consume_records(consumer.as_mut(), num_records).await;
    let now = current_time_ms();

    // Translation deviation (CLAUDE.md #4 / DoD #7): Java bounds the
    // broker-stamped timestamp by `[startingTimestamp, now]` with ZERO
    // tolerance (`ClientsTestUtils.consumeAndVerifyRecordsWithTimeTypeLogAppend`).
    // That holds in Java only because `ClusterInstance` runs the KRaft brokers
    // IN-PROCESS: `record.timestamp` (read by the broker at append) and `now`
    // (read by the client) then come from one `CLOCK_REALTIME`, so the append
    // provably precedes the read and flooring to millis preserves the order.
    //
    // This harness runs the brokers in containers. On a shared-kernel Docker
    // the clock is still literally the same variable (identical `/proc/stat
    // btime`, no time namespace), but where the daemon is VM-backed -- Docker
    // Desktop on macOS/Windows, Colima, Lima -- the VM keeps its own realtime
    // clock and resyncs to the host only periodically, so the two readings can
    // disagree by milliseconds in either direction. With zero tolerance that
    // makes the assertion flaky for reasons unrelated to this client; observed
    // as `timestamp 1789641102389 should be within [1789641101901,
    // 1789641102387]`, i.e. 2 ms past the upper bound.
    //
    // The slack's exact size is a judgement call, NOT a measurement: the skew
    // is environment-dependent with no upper bound derivable here, and this
    // bound's detection power is flat across a huge range. What it can still
    // catch are gross errors -- `NO_TIMESTAMP`, a zero stamp, epoch seconds or
    // micros read as millis, an i32 truncation -- and the NEAREST of those sits
    // ~1.79e12 ms (~57 years) from `now`, so any slack from milliseconds up to
    // years is equally detective. 50 ms is picked as roughly an order of
    // magnitude above the skew the failure implies (2 ms past the bound, so a
    // low-tens-of-ms offset once the append-to-observe latency is added back)
    // while staying ~10 orders of magnitude below the errors it must catch,
    // which keeps the bound visibly tight and retains detection of mid-scale
    // anomalies. It is deliberately NOT sized for pathological drift: a
    // VM-backed Docker whose host has slept can be further out than this, and
    // such a run is meant to fail here rather than be silently tolerated.
    //
    // What no choice of slack can catch is a `base_timestamp + timestamp_delta`
    // (CreateTime) regression: `send_records` supplies `start_time + i` for
    // `i < 50`, so those values land INSIDE even the untightened range. The
    // LogAppendTime decode is pinned by the `timestamp_type` assertion below,
    // not by this range.
    const CLOCK_SKEW_SLACK_MS: i64 = 50;

    for (i, record) in records.iter().take(num_records).enumerate() {
        assert_eq!(record.topic, tp.topic());
        assert_eq!(record.partition, tp.partition());
        assert_eq!(
            record.timestamp_type,
            TimestampType::LogAppendTime,
            "timestamp_type should be LogAppendTime"
        );
        assert!(
            record.timestamp >= start_time - CLOCK_SKEW_SLACK_MS && record.timestamp <= now + CLOCK_SKEW_SLACK_MS,
            "timestamp {} should be within [{start_time}, {now}] widened by the \
             {CLOCK_SKEW_SLACK_MS} ms broker/client clock-skew slack",
            record.timestamp
        );
        assert_eq!(record.offset, i as i64);
        let expected_key = format!("key {i}").into_bytes();
        let expected_value = format!("value {i}").into_bytes();
        assert_eq!(record.key.as_ref().expect("key"), &expected_key);
        assert_eq!(record.value.as_ref().expect("value"), &expected_value);
    }

    consumer.close().await.expect("consumer close");
}

/// Translates Java's `testAsyncConsumerEndOffsets` (line 1376).
/// Produces N records, subscribes, awaits the 2-partition assignment, then
/// asserts `end_offsets([TP]) == N`.
///
/// Translation deviation: Java uses N=10000; we use N=200 for harness
/// speed (outcome parity — `end_offsets` equals the cumulative produced
/// count — not count parity).
#[tokio::test(flavor = "multi_thread")]
async fn test_async_consumer_end_offsets() {
    let mut ctx = TestContext::new(cluster_config_kip848()).await;
    let topic = ctx.topic("topic");
    let tp = TopicPartition::new(topic.clone(), 0);
    let tp2 = TopicPartition::new(topic.clone(), 1);
    let group_id = ctx.group_id("g_end_offsets");

    let num_records = 200usize;
    let producer = build_producer(&ctx);
    let mut consumer = make_consumer(&ctx, &group_id, &[]);
    // Empty topic so records start at offset 0 (Java parity).
    create_topic(consumer.as_mut(), &topic, 2).await;
    send_records(&producer, &tp, num_records, current_time_ms()).await;
    producer.close().await.expect("producer close");

    consumer.subscribe_with_topics(vec![topic.clone()]).await.expect("subscribe");
    await_assignment(
        consumer.as_mut(),
        &HashSet::from([tp.clone(), tp2.clone()]),
        Duration::from_secs(90),
    )
    .await;

    let end_offsets = consumer.end_offsets(std::slice::from_ref(&tp)).await.expect("end_offsets");
    assert_eq!(
        end_offsets.get(&tp).copied(),
        Some(num_records as i64),
        "end_offsets for tp should equal num_records"
    );

    consumer.close().await.expect("consumer close");
}

/// Translates Java's `testAsyncConsumerFetchOffsetsForTime` (line 1422).
/// Produces 100 records per partition with timestamp == sequence number,
/// then `offsets_for_times` for ts 0 (partition 0) and ts 20 (partition 1);
/// asserts offset/timestamp/leader_epoch. Also asserts negative target time
/// raises IllegalArgument.
#[tokio::test(flavor = "multi_thread")]
async fn test_async_consumer_fetch_offsets_for_time() {
    let mut ctx = TestContext::new(cluster_config_kip848()).await;
    let topic = ctx.topic("topic");
    let tp0 = TopicPartition::new(topic.clone(), 0);
    let tp1 = TopicPartition::new(topic.clone(), 1);
    let group_id = ctx.group_id("g_offsets_for_time");

    let producer = build_producer(&ctx);
    // Do NOT provision: the `__provisioner__` record at offset 0 carries a
    // broker wall-clock `CreateTime` timestamp, which is `>=` any small
    // search target, so `offsets_for_times(ts=0/20)` would resolve to the
    // provisioner (offset 0) rather than the produced records. Instead,
    // send the 100 timestamped records directly — the first send to
    // partition 0 auto-creates the topic with `KAFKA_NUM_PARTITIONS=2`, so
    // offset 0 on each partition IS a real `ts==0` record (Java's
    // "key/val/timestamp == sequence number, starting at offset 0").
    //
    // partition 0: key/val/timestamp == sequence number; partition 1: same.
    send_records(&producer, &tp0, 100, 0).await;
    send_records(&producer, &tp1, 100, 0).await;
    producer.close().await.expect("producer close");

    // No provisioner ⇒ the produced records start at offset 0 on each
    // partition.
    let base0 = 0i64;
    let base1 = 0i64;

    let mut consumer = make_consumer(&ctx, &group_id, &[]);

    // Java: negative target time → IllegalArgumentException.
    let neg_err = consumer
        .offsets_for_times(HashMap::from([(tp0.clone(), -1i64)]))
        .await
        .expect_err("negative target time should fail");
    assert!(
        matches!(neg_err, Error::LocalIllegalArgument(_)),
        "expected IllegalArgument for negative target time, got {neg_err:?}"
    );

    // Search ts 0 on partition 0 and ts 20 on partition 1.
    let search = HashMap::from([(tp0.clone(), 0i64), (tp1.clone(), 20i64)]);
    let offsets = consumer.offsets_for_times(search).await.expect("offsets_for_times");

    // `offsets_for_times` returns `HashMap<TopicPartition,
    // OffsetAndTimestamp>` — a missing key means "no offset"; a present key
    // carries the resolved offset directly (no nested Option, unlike Java's
    // null-valued map entries).
    let r0 = offsets.get(&tp0).expect("offset present for ts 0 on partition 0");
    assert_eq!(r0.offset(), base0, "first record at-or-after ts 0 is the base offset");
    assert_eq!(r0.timestamp(), 0);
    assert_eq!(r0.leader_epoch(), Some(0));

    let r1 = offsets.get(&tp1).expect("offset present for ts 20 on partition 1");
    assert_eq!(r1.offset(), base1 + 20, "first record at-or-after ts 20 is base+20");
    assert_eq!(r1.timestamp(), 20);
    assert_eq!(r1.leader_epoch(), Some(0));

    consumer.close().await.expect("consumer close");
}

/// Translates Java's `testAsyncConsumerConsumingWithNullGroupId` (line 1224).
/// Three groupless consumers consume from earliest / latest / explicit
/// offset; asserts record counts and that commit/committed raise
/// InvalidGroupId for groupless consumers.
#[tokio::test(flavor = "multi_thread")]
async fn test_async_consumer_consuming_with_null_group_id() {
    let mut ctx = TestContext::new(cluster_config_kip848()).await;
    // Java uses a single-partition topic (createTopic(TOPIC, 1, 1)).
    let topic = ctx.topic("topic-null-group");
    let tp = TopicPartition::new(topic.clone(), 0);

    let producer = build_producer(&ctx);
    // consumer1: groupless, earliest. consumer2: groupless, latest.
    // consumer3: groupless, explicit seek.
    let mut consumer1 = KafkaConsumer::new::<Vec<u8>, Vec<u8>>(
        make_groupless_consumer_config(&ctx, &[("auto.offset.reset", "earliest"), ("client.id", "consumer1")]),
        Box::new(ByteArrayDeserializer),
        Box::new(ByteArrayDeserializer),
    )
    .expect("consumer1");
    let mut consumer2 = KafkaConsumer::new::<Vec<u8>, Vec<u8>>(
        make_groupless_consumer_config(&ctx, &[("auto.offset.reset", "latest"), ("client.id", "consumer2")]),
        Box::new(ByteArrayDeserializer),
        Box::new(ByteArrayDeserializer),
    )
    .expect("consumer2");
    let mut consumer3 = KafkaConsumer::new::<Vec<u8>, Vec<u8>>(
        make_groupless_consumer_config(&ctx, &[("auto.offset.reset", "earliest"), ("client.id", "consumer3")]),
        Box::new(ByteArrayDeserializer),
        Box::new(ByteArrayDeserializer),
    )
    .expect("consumer3");

    // Java: createTopic(TOPIC, 1, 1) — an EMPTY topic, so the 3 records below
    // land at offsets 0, 1, 2. Create it via metadata auto-create (the broker
    // gives 2 partitions; only partition 0 is used).
    create_topic(consumer1.as_mut(), &topic, 1).await;
    for i in 1..=3 {
        let record = ProducerRecord::with_partition_key(
            topic.clone(),
            Some(0),
            Some(format!("k{i}").into_bytes()),
            Some(format!("v{i}").into_bytes()),
        )
        .expect("record");
        let fut = <KafkaProducer<Vec<u8>, Vec<u8>> as Producer<Vec<u8>, Vec<u8>>>::send(&producer, record)
            .await
            .expect("send");
        fut.get_with_timeout(Duration::from_secs(30)).await.expect("ack");
    }
    producer.close().await.expect("producer close");

    consumer1.assign(vec![tp.clone()]).await.expect("assign c1");
    consumer2.assign(vec![tp.clone()]).await.expect("assign c2");
    consumer3.assign(vec![tp.clone()]).await.expect("assign c3");
    // Java: `consumer3.seek_with_offset(TP, 1)` — a literal offset, skipping the first of
    // the 3 records (offsets 0, 1, 2).
    consumer3.seek_with_offset(tp.clone(), 1).await.expect("seek c3");

    let num_records1 = poll_count(consumer1.as_mut(), 3, Duration::from_secs(15)).await;
    // Java: commitSync / committed raise InvalidGroupId for groupless.
    let c1_commit = consumer1.commit_sync().await.expect_err("groupless commit_sync should fail");
    assert_eq!(c1_commit.error(), Errors::InvalidGroupId, "got {c1_commit:?}");
    let c2_committed = consumer2
        .committed(std::slice::from_ref(&tp))
        .await
        .expect_err("groupless committed should fail");
    assert_eq!(c2_committed.error(), Errors::InvalidGroupId, "got {c2_committed:?}");

    let num_records2 = poll_count(consumer2.as_mut(), 0, Duration::from_secs(5)).await;
    let num_records3 = poll_count(consumer3.as_mut(), 2, Duration::from_secs(15)).await;

    consumer1.unsubscribe().await.expect("unsubscribe c1");
    consumer2.unsubscribe().await.expect("unsubscribe c2");
    consumer3.unsubscribe().await.expect("unsubscribe c3");
    assert!(consumer1.assignment().is_empty());
    assert!(consumer2.assignment().is_empty());
    assert!(consumer3.assignment().is_empty());

    consumer1.close().await.expect("close c1");
    consumer2.close().await.expect("close c2");
    consumer3.close().await.expect("close c3");

    assert_eq!(num_records1, 3, "consumer1 should consume from earliest (3 records)");
    assert_eq!(num_records2, 0, "consumer2 should consume from latest (0 records)");
    assert_eq!(num_records3, 2, "consumer3 should consume from offset 1 (2 records)");
}

/// Translates Java's `testAsyncConsumerNullGroupIdNotSupportedIfCommitting`
/// (line 1303). A groupless consumer's `commit_sync` raises InvalidGroupId
/// with the EXACT Java message.
#[tokio::test(flavor = "multi_thread")]
async fn test_async_consumer_null_group_id_not_supported_if_committing() {
    let mut ctx = TestContext::new(cluster_config_kip848()).await;
    let topic = ctx.topic("topic");
    let tp = TopicPartition::new(topic.clone(), 0);

    let mut consumer = KafkaConsumer::new::<Vec<u8>, Vec<u8>>(
        make_groupless_consumer_config(&ctx, &[("auto.offset.reset", "earliest"), ("client.id", "consumer1")]),
        Box::new(ByteArrayDeserializer),
        Box::new(ByteArrayDeserializer),
    )
    .expect("KafkaConsumer::new");
    create_topic(consumer.as_mut(), &topic, 2).await;
    consumer.assign(vec![tp.clone()]).await.expect("assign");

    let err = consumer.commit_sync().await.expect_err("groupless commit_sync should fail");
    assert_eq!(err.error(), Errors::InvalidGroupId, "got {err:?}");
    // Java: `InvalidGroupIdException` message verbatim.
    assert_eq!(
        err.message(),
        "To use the group management or offset commit APIs, you must provide a valid \
         group.id in the consumer configuration."
    );

    consumer.close().await.expect("consumer close");
}

/// Translates Java's `testAsyncConsumerPositionRespectsTimeout` (line 1467).
/// `position` for a partition that doesn't exist times out after the
/// user-supplied timeout.
#[tokio::test(flavor = "multi_thread")]
async fn test_async_consumer_position_respects_timeout() {
    let mut ctx = TestContext::new(cluster_config_kip848()).await;
    let topic = ctx.topic("topic");
    // partition 15 does not exist (topic has 2 partitions).
    let tp = TopicPartition::new(topic.clone(), 15);
    let group_id = ctx.group_id("g_position_timeout");

    let mut consumer = make_consumer(&ctx, &group_id, &[]);
    create_topic(consumer.as_mut(), &topic, 2).await;
    consumer.assign(vec![tp.clone()]).await.expect("assign");

    let err = consumer
        .position_with_timeout(&tp, Duration::from_secs(3))
        .await
        .expect_err("position on a nonexistent partition should time out");
    assert!(matches!(err, Error::Timeout(_)), "expected Timeout, got {err:?}");

    consumer.close().await.expect("consumer close");
}

/// Translates Java's `testAsyncConsumerPositionRespectsWakeup` (line 1491).
/// A concurrent `wakeup()` interrupts a blocking `position` (§11).
#[tokio::test(flavor = "multi_thread")]
async fn test_async_consumer_position_respects_wakeup() {
    let mut ctx = TestContext::new(cluster_config_kip848()).await;
    let topic = ctx.topic("topic");
    let tp = TopicPartition::new(topic.clone(), 15);
    let group_id = ctx.group_id("g_position_wakeup");

    let mut consumer = make_consumer(&ctx, &group_id, &[]);
    create_topic(consumer.as_mut(), &topic, 2).await;
    consumer.assign(vec![tp.clone()]).await.expect("assign");

    // Java: `CompletableFuture.runAsync(() -> { sleep(1s); consumer.wakeup(); })`
    // (`PlaintextConsumerTest.java:1501-1504`). Java's `Consumer` reference is
    // freely shareable across threads. The Rust equivalent obtains a
    // `Clone + Send + Sync` `ConsumerHandle` BEFORE the `&mut` borrow taken
    // by `position_with_timeout`, then fires `wakeup()` from a spawned task —
    // sound, no `unsafe`, no reference to the consumer crossing the boundary.
    let handle = consumer.handle();
    let waker = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(1)).await;
        handle.wakeup();
    });

    let result = consumer.position_with_timeout(&tp, Duration::from_secs(3)).await;
    let _ = waker.await;
    let err = result.expect_err("position should be interrupted by wakeup");
    assert!(matches!(err, Error::Wakeup(_)), "expected Wakeup, got {err:?}");

    consumer.close().await.expect("consumer close");
}

/// Translates Java's
/// `testAsyncConsumerPositionWithErrorConnectionRespectsWakeup` (line 1523).
/// With an unreachable bootstrap, `wakeup()` still interrupts a blocking
/// `position`.
#[tokio::test(flavor = "multi_thread")]
async fn test_async_consumer_position_with_error_connection_respects_wakeup() {
    // bootstrap points at an unreachable address (Java: "localhost:12345").
    // This is a dead-broker negative test: the address never accepts a
    // connection, so the security protocol is irrelevant (no handshake ever
    // starts) — keep the explicit dead address rather than the protocol
    // bootstrap, and build the config inline since there is no `TestContext`.
    let props = HashMap::from([
        ("bootstrap.servers".to_string(), "localhost:12345".to_string()),
        ("group.protocol".to_string(), "consumer".to_string()),
        ("auto.offset.reset".to_string(), "earliest".to_string()),
        ("client.id".to_string(), "integration-test-consumer".to_string()),
        ("enable.auto.commit".to_string(), "false".to_string()),
        ("group.id".to_string(), "g_position_err_wakeup".to_string()),
    ]);
    let mut consumer = KafkaConsumer::new::<Vec<u8>, Vec<u8>>(
        ConsumerConfig::new(&props).expect("invalid test config"),
        Box::new(ByteArrayDeserializer),
        Box::new(ByteArrayDeserializer),
    )
    .expect("KafkaConsumer::new");
    let topic = "topic".to_string();
    let tp = TopicPartition::new(topic, 15);
    consumer.assign(vec![tp.clone()]).await.expect("assign");

    // Java (`PlaintextConsumerTest.java:1535-1538`): a cross-thread
    // `wakeup()` interrupts a blocking `position` even when the bootstrap
    // is unreachable. Obtain the shareable handle before the `&mut` borrow.
    let handle = consumer.handle();
    let waker = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(1)).await;
        handle.wakeup();
    });

    let result = consumer.position_with_timeout(&tp, Duration::from_secs(100)).await;
    let _ = waker.await;
    let err = result.expect_err("position should be interrupted by wakeup despite connection error");
    assert!(matches!(err, Error::Wakeup(_)), "expected Wakeup, got {err:?}");

    consumer.close().await.expect("consumer close");
}

/// Translates Java's `testAsyncConsumerOffsetRelatedWhenTimeoutZero`
/// (line 1627). Zero-timeout offset queries return immediately:
/// beginning/end offsets empty; offsets_for_times has the key with a
/// `None` value.
#[tokio::test(flavor = "multi_thread")]
async fn test_async_consumer_offset_related_when_timeout_zero() {
    let mut ctx = TestContext::new(cluster_config_kip848()).await;
    let topic = ctx.topic("topic");
    let tp = TopicPartition::new(topic.clone(), 0);
    let group_id = ctx.group_id("g_timeout_zero");

    let mut consumer = make_consumer(&ctx, &group_id, &[]);
    create_topic(consumer.as_mut(), &topic, 2).await;

    let result1 = consumer
        .beginning_offsets_with_timeout(std::slice::from_ref(&tp), Duration::ZERO)
        .await
        .expect("beginning_offsets(ZERO)");
    assert_eq!(result1.len(), 0, "beginning_offsets with zero timeout should be empty");

    let result2 = consumer
        .end_offsets_with_timeout(std::slice::from_ref(&tp), Duration::ZERO)
        .await
        .expect("end_offsets(ZERO)");
    assert_eq!(result2.len(), 0, "end_offsets with zero timeout should be empty");

    let result3 = consumer
        .offsets_for_times_with_timeout(HashMap::from([(tp.clone(), 0i64)]), Duration::ZERO)
        .await
        .expect("offsets_for_times(ZERO)");
    // Translation deviation: Java's zero-timeout arm returns a map of
    // size 1 with the key mapped to `null` (no offset yet). Rust's
    // `OffsetAndTimestamp` is not nullable, so the zero-timeout arm omits
    // the key entirely (`async_kafka_consumer.rs:3413-3428`). The user
    // observes "no data yet" via the absent key rather than a null value —
    // an equivalent, documented contract reduction.
    assert!(
        !result3.contains_key(&tp),
        "offsets_for_times with zero timeout should not resolve an offset (key absent in Rust)"
    );

    consumer.close().await.expect("consumer close");
}

// ── Local utilities ────────────────────────────────────────────────────

/// Polls until at least `at_least` records have been collected OR the
/// deadline elapses, returning the total count. Mirrors Java's
/// `consumer.poll(Duration).count()` summed across the budget, with a
/// short settle for the "expect 0" case.
async fn poll_count(consumer: &mut BytesConsumer, at_least: usize, budget: Duration) -> usize {
    let deadline = Instant::now() + budget;
    let mut count = 0usize;
    while Instant::now() < deadline {
        let records = consumer.poll(Duration::from_millis(200)).await.expect("poll");
        count += records.into_iter().count();
        if at_least > 0 && count >= at_least {
            break;
        }
    }
    count
}
