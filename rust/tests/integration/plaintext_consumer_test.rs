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
//! quota, and close-on-interrupt are OUT_OF_SCOPE per
//! `consumer-threading.md` §20.
//!
//! The translated tests run as multilanguage scenarios: each one runs against
//! the native Rust consumer and, through the gRPC backends, the Python (sync
//! and asyncio) and C consumers ([`multilanguage_consumer_test!`]); each name
//! below gains the macro's backend suffix (`__rust`, `__grpc_python`,
//! `__grpc_python_async`, `__grpc_c`). The setup producer and admin client
//! are native fixtures; every consumer is built by the backend factory. Java's
//! `TestConsumerReassignmentListener` and `RetryCommitCallback` become the
//! backend's own logging listener and commit callback
//! ([`ConsumerCallbackLog`]): `callsToAssigned` is the number of `assigned`
//! entries and `callsToRevoked` the number of `revoked` plus `lost` entries
//! (Java's listener does not override `onPartitionsLost`, whose interface
//! default calls `onPartitionsRevoked`), and a commit completion is a `commit`
//! entry. A wakeup from another task goes through
//! [`ConsumerCallbackLog::wakeup_handle`].
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
//! - `testAsyncConsumeCoordinatorFailover`
//!   → `test_async_consume_coordinator_failover` (Phase 16; dedicated
//!   `Type::Kraft` cluster, stops the group coordinator)
//! - `testAsyncConsumerCloseOnBrokerShutdown`
//!   → `test_async_consumer_close_on_broker_shutdown` (Phase 16; dedicated
//!   single-broker `Type::Kraft` cluster, stops the broker)
//! - `testAsyncStaticMemberCloseWithLeaveGroupTriggersRebalance` (4.4,
//!   45c4bdc48a) → `test_async_static_member_close_with_leave_group_triggers_rebalance`
//! - `testAsyncConsumerUnsubscribeDoesNotCommitOffsetsWithAutoCommitEnabled`
//!   (KAFKA-20119) → `test_async_consumer_unsubscribe_does_not_commit_offsets_with_auto_commit_enabled`
//!
//! ## Translated on both assignment intervals (KIP-1263, KAFKA-20291)
//!
//! Java runs these on two broker configs, `group.consumer.assignment.interval.ms`
//! `0` and `1000`. Each is three scenarios ([`assignment_interval_consumer_test!`],
//! as `consumer_bounce_test.rs` does): `<name>_assignment_interval_0` and
//! `<name>_assignment_interval_1000` are Java's two arms and run on a 4.3+
//! broker; `<name>` runs the default config on an older broker, which has no
//! interval (the `0` behaviour), and skips itself on 4.3+.
//!
//! - `testAsyncConsumerGroupConsumption` → `test_async_consumer_group_consumption`
//! - `testAsyncConsumerGroupConsumptionWithTwoMembers`
//!   → `test_async_consumer_group_consumption_with_two_members`
//! - `testAsyncConsumerStaticConsumerDetectsNewPartitionCreatedAfterRestart`
//!   → `test_async_consumer_static_consumer_detects_new_partition_created_after_restart`
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
//! - SKIP `testAsyncConsumerStallBetweenPoll` — KAFKA-19259 timing /
//!   perf-regression guard, not a behavioral contract; flaky under
//!   shared-pool load.
//! - SKIP `testAsyncConsumerSimpleConsumption` / `...AutoOffsetReset` /
//!   `...ConsumeMessagesWithCreateTime` — already
//!   covered by `consumer_test.rs` and the assign/fetch suites (subscribe /
//!   assign / seek / poll / CreateTime full per-record field verification).
//! - SKIP (OUT_OF_SCOPE §20): all `testClassicConsumer*` twins,
//!   `*MetricsCleanUp*` / `QuotaMetrics*`,
//!   `testAsyncConsumerClusterResourceListener`.
//! - `testAsyncConsumerCloseLeavesGroupOnInterrupt` no longer exists: Kafka
//!   4.4 removed it (KAFKA-20522, 096cc96fee) in favour of unit tests in
//!   `KafkaConsumerTest` / `AsyncKafkaConsumerTest`.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::time::Duration;
use std::time::Instant;

use confluent_kafka::common::Error;
use confluent_kafka::common::TopicPartition;
use confluent_kafka::common::header::Header;
use confluent_kafka::common::header::Headers;
use confluent_kafka::common::header::RecordHeaders;
use confluent_kafka::common::record::TimestampType;
use confluent_kafka::common::serialization::ByteArraySerializer;
use confluent_kafka::consumer::CloseOptions;
use confluent_kafka::consumer::DynConsumer;
use confluent_kafka::consumer::GroupMembershipOperation;
use confluent_kafka::producer::KafkaProducer;
use confluent_kafka::producer::Producer;
use confluent_kafka::producer::ProducerConfig;
use confluent_kafka::producer::ProducerRecord;

use crate::assignment_interval_consumer_test;
use crate::common::backend_factory::ConsumerBackendFactory;
use crate::common::callback_log::{
    CallbackLogEntry, ConsumerCallbackLog, KIND_ASSIGNED, KIND_COMMIT, KIND_LOST, KIND_REVOKED,
};
use crate::common::cluster_config::{ClusterConfig, kip848_3_broker};
use crate::common::test_context::TestContext;
use crate::common::test_utils::with_timeout;
use crate::multilanguage_consumer_test;

// The bytes-typed consumer trait object every backend factory returns (mirrors
// Java's `Consumer<byte[], byte[]>`).
type BytesConsumer = dyn DynConsumer<Vec<u8>, Vec<u8>>;

// ── Cluster configs ───────────────────────────────────────────────────

/// Cluster config matching the Java `@ClusterTestDefaults`: 3 brokers,
/// KIP-848 enabled, plus the broker properties from the `serverProperties`
/// annotation. Topics Java creates explicitly are created by
/// [`create_test_topic`]; `num.partitions=2` applies to auto-created topics.
fn cluster_config_kip848() -> ClusterConfig {
    // The canonical helper supplies the shared KIP-848 broker tuning.
    kip848_3_broker(2)
}

// ── Config builders ───────────────────────────────────────────────────

/// The bootstrap the backend under test reaches.
fn consumer_bootstrap<F: ConsumerBackendFactory>(ctx: &TestContext, factory: &F) -> String {
    ctx.protocol_bootstrap_servers_for(factory.needs_container_bootstrap())
        .to_string()
}

/// Build the consumer properties matching Java's
/// `cluster.consumer(Map.of(GROUP_PROTOCOL_CONFIG, "consumer", ...))`, on the
/// bootstrap the backend under test reaches. Caller-supplied overrides win
/// over the defaults.
fn make_consumer_config<F: ConsumerBackendFactory>(
    ctx: &TestContext,
    factory: &F,
    group_id: &str,
    overrides: &[(&str, &str)],
) -> HashMap<String, String> {
    let mut props = HashMap::from([
        ("bootstrap.servers".to_string(), consumer_bootstrap(ctx, factory)),
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
    props
}

/// Build *groupless* consumer properties — `group.id` is intentionally
/// absent, mirroring Java's `testConsumingWithNullGroupId` which omits
/// `GROUP_ID_CONFIG`.
fn make_groupless_consumer_config<F: ConsumerBackendFactory>(
    ctx: &TestContext,
    factory: &F,
    overrides: &[(&str, &str)],
) -> HashMap<String, String> {
    let mut props = HashMap::from([
        ("bootstrap.servers".to_string(), consumer_bootstrap(ctx, factory)),
        ("group.protocol".to_string(), "consumer".to_string()),
        ("client.id".to_string(), "integration-test-consumer".to_string()),
        ("enable.auto.commit".to_string(), "false".to_string()),
    ]);
    for (k, v) in overrides {
        props.insert((*k).to_string(), (*v).to_string());
    }
    ctx.apply_security(&mut props);
    props
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
        ByteArraySerializer::default(),
        ByteArraySerializer::default(),
    )
    .expect("Failed to build test producer")
}

/// Create a consumer under test through `factory`.
async fn make_consumer<F: ConsumerBackendFactory>(
    ctx: &TestContext,
    factory: &F,
    group_id: &str,
    overrides: &[(&str, &str)],
) -> Box<BytesConsumer> {
    create_from(factory, make_consumer_config(ctx, factory, group_id, overrides)).await
}

/// Create a consumer under test from `props` through `factory`.
async fn create_from<F: ConsumerBackendFactory>(factory: &F, props: HashMap<String, String>) -> Box<BytesConsumer> {
    factory
        .create(props)
        .await
        .unwrap_or_else(|e| panic!("{} backend: create consumer: {e}", factory.name()))
}

/// [`make_consumer`] together with the consumer's callback log, for the tests
/// that observe the rebalance listener or the commit callback, or wake the
/// consumer from another task.
async fn make_consumer_with_log<F: ConsumerBackendFactory>(
    ctx: &TestContext,
    factory: &F,
    group_id: &str,
    overrides: &[(&str, &str)],
) -> (Box<BytesConsumer>, ConsumerCallbackLog) {
    factory
        .create_with_callback_log(make_consumer_config(ctx, factory, group_id, overrides))
        .await
        .unwrap_or_else(|e| panic!("{} backend: create consumer: {e}", factory.name()))
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

/// Java `PlaintextConsumerTest.BROKER_COUNT`.
const BROKER_COUNT: i16 = 3;

/// Translates Java's `cluster.createTopic(name, partitions, replicationFactor)`:
/// creates `topic` EMPTY through the admin client (the first produced record
/// lands at offset 0), then waits until every partition leader accepts
/// leader-only requests, before the test produces. Auto-creating the topic
/// instead races leader election on the 3-broker cluster
/// (`NOT_LEADER_OR_FOLLOWER` retries reorder non-idempotent sends).
async fn create_test_topic(ctx: &TestContext, topic: &str, num_partitions: i32, replication_factor: i16) {
    create_test_topic_with_configs(ctx, topic, num_partitions, replication_factor, BTreeMap::new()).await;
}

/// [`create_test_topic`] with topic-level configs — Java's
/// `cluster.createTopic(name, partitions, replicationFactor, configs)`.
async fn create_test_topic_with_configs(
    ctx: &TestContext,
    topic: &str,
    num_partitions: i32,
    replication_factor: i16,
    configs: BTreeMap<String, String>,
) {
    use confluent_kafka::admin::Admin;
    use confluent_kafka::admin::AdminClient;
    use confluent_kafka::admin::AdminClientConfig;

    use crate::common::test_utils;

    let mut props = HashMap::from([
        ("client.id".to_string(), "plaintext-consumer-test-admin".to_string()),
        ("request.timeout.ms".to_string(), "30000".to_string()),
        ("default.api.timeout.ms".to_string(), "30000".to_string()),
    ]);
    ctx.configure(&mut props);
    let config = AdminClientConfig::new(&props).expect("valid admin config");
    let admin: Box<dyn Admin> = AdminClient::create(config).expect("admin client");
    test_utils::create_topic_with_configs(admin.as_ref(), topic, num_partitions, replication_factor, configs).await;
    test_utils::wait_for_partition_leaders(admin.as_ref(), topic, 0..num_partitions).await;
    admin.close_with_timeout(Duration::from_secs(5)).await;
}

/// Creates `topic` EMPTY by triggering broker metadata auto-creation
/// (`partitions_for` over the METADATA path; `auto.create.topics.enable` is on
/// by default, `num.partitions=2`) and waits until it materializes with the
/// expected partition count. Used only by tests whose Java counterpart relies
/// on broker auto-create rather than `cluster.createTopic`.
async fn create_topic(consumer: &mut BytesConsumer, backend: &str, topic: &str, partitions: usize) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let parts = consumer
            .partitions_for(topic)
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: partitions_for: {e}"));
        if parts.len() >= partitions {
            return;
        }
        if Instant::now() >= deadline {
            panic!("{backend} backend: topic {topic} not auto-created with >= {partitions} partitions within 30s");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

// ── Consumer helpers ──────────────────────────────────────────────────

/// Translates Java's `ClientsTestUtils.consumeRecords(consumer, numRecords)`
/// — polls 100ms until `num_records` collected (60s budget), returning the
/// collected owned records.
async fn consume_records(consumer: &mut BytesConsumer, backend: &str, num_records: usize) -> Vec<OwnedRecord> {
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut collected: Vec<OwnedRecord> = Vec::new();
    // Java's `consumeRecords` drives the poll through `TestUtils.waitForCondition`,
    // which evaluates its lambda at least once — so even `num_records == 0`
    // performs a single poll (mirrored by the post-rebalance "no records"
    // verification in `test_async_consumer_pause_state_not_preserved_by_rebalance`).
    // A plain `while len < num_records` would poll zero times for `num_records == 0`.
    loop {
        let records = consumer
            .poll(Duration::from_millis(100))
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: poll should succeed: {e}"));
        for record in records {
            collected.push(OwnedRecord::from(&record));
        }
        if collected.len() >= num_records || Instant::now() >= deadline {
            break;
        }
    }
    assert!(
        collected.len() >= num_records,
        "Timed out before consuming expected {num_records} records (got {}) ({backend} backend)",
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
    backend: &str,
    tp: &TopicPartition,
    num_records: usize,
    starting_offset: i64,
    starting_key_and_value_index: usize,
    starting_timestamp: i64,
) {
    let records = consume_records(consumer, backend, num_records).await;
    for (i, record) in records.iter().take(num_records).enumerate() {
        let offset = starting_offset + i as i64;
        assert_eq!(record.topic, tp.topic(), "topic ({backend} backend)");
        assert_eq!(record.partition, tp.partition(), "partition ({backend} backend)");
        assert_eq!(
            record.timestamp_type,
            TimestampType::CreateTime,
            "timestamp type ({backend} backend)"
        );
        let timestamp = starting_timestamp + i as i64;
        assert_eq!(record.timestamp, timestamp, "timestamp at index {i} ({backend} backend)");
        assert_eq!(record.offset, offset, "offset at index {i} ({backend} backend)");
        let kvi = starting_key_and_value_index + i;
        let expected_key = format!("key {kvi}").into_bytes();
        let expected_value = format!("value {kvi}").into_bytes();
        assert_eq!(
            record.key.as_ref().expect("key present"),
            &expected_key,
            "key at {i} ({backend} backend)"
        );
        assert_eq!(
            record.value.as_ref().expect("value present"),
            &expected_value,
            "value at {i} ({backend} backend)"
        );
        assert_eq!(
            record.serialized_key_size as usize,
            expected_key.len(),
            "serialized key size ({backend} backend)"
        );
        assert_eq!(
            record.serialized_value_size as usize,
            expected_value.len(),
            "serialized value size ({backend} backend)"
        );
    }
}

/// Translates Java's `ClientsTestUtils.awaitAssignment(consumer, expected)`.
async fn await_assignment(
    consumer: &mut BytesConsumer,
    backend: &str,
    expected: &HashSet<TopicPartition>,
    deadline_duration: Duration,
) {
    let deadline = Instant::now() + deadline_duration;
    while Instant::now() < deadline {
        let _ = consumer
            .poll(Duration::from_millis(100))
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: poll should succeed: {e}"));
        if &consumer.assignment() == expected {
            return;
        }
    }
    panic!(
        "{backend} backend: Timed out while awaiting expected assignment of {} partitions. Current: {:?}",
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

/// Translates Java's `testAsyncConsumerHeaders`.
/// Produces one record with three ordered headers, then assigns + seeks
/// to 0, consumes it, and asserts the headers survive the round-trip with
/// their insertion order preserved.
async fn async_consumer_headers<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let backend = factory.name();
    let topic = ctx.topic("topic");
    let tp = TopicPartition::new(topic.clone(), 0);
    let group_id = ctx.group_id("g_headers");

    let producer = build_producer(ctx);

    // Java: `new ProducerRecord<>(TP.topic(), TP.partition(), null, "key", "value")`
    // then `record.headers().add(...)` thrice. Java relies on producer-driven
    // auto-create; the record lands at offset 0 (empty topic).
    let mut headers = RecordHeaders::new();
    headers
        .add_with_key_value("headerKey", Some(b"headerValue"))
        .expect("add header");
    headers
        .add_with_key_value("headerKey2", Some(b"headerValue2"))
        .expect("add header");
    headers
        .add_with_key_value("headerKey3", Some(b"headerValue3"))
        .expect("add header");
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
        .unwrap_or_else(|e| panic!("{backend} backend: send should succeed: {e}"));
    fut.get_with_timeout(Duration::from_secs(30))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: send should ack: {e}"));
    producer.close().await.expect("producer close");

    let mut consumer = make_consumer(ctx, factory, &group_id, &[]).await;
    assert_eq!(consumer.assignment().len(), 0, "assignment should be empty ({backend} backend)");
    consumer
        .assign(vec![tp.clone()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: assign should succeed: {e}"));
    assert_eq!(consumer.assignment().len(), 1, "values ({backend} backend)");
    consumer
        .seek_with_offset(tp.clone(), 0)
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: seek should succeed: {e}"));

    let records = consume_records(consumer.as_mut(), backend, 1).await;
    assert_eq!(records.len(), 1, "values ({backend} backend)");
    let rec = &records[0];

    // Java: `headers().lastHeader("headerKey")` → "headerValue".
    let last = rec
        .headers
        .iter()
        .rev()
        .find(|(k, _)| k == "headerKey")
        .expect("headerKey should be present");
    assert_eq!(last.1.as_deref(), Some(b"headerValue".as_ref()), "values ({backend} backend)");

    // Java: header ORDER preserved (headers[0..2]).
    assert_eq!(rec.headers[0].0, "headerKey", "values ({backend} backend)");
    assert_eq!(rec.headers[1].0, "headerKey2", "values ({backend} backend)");
    assert_eq!(rec.headers[2].0, "headerKey3", "values ({backend} backend)");

    consumer
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer close: {e}"));
}

multilanguage_consumer_test!(test_async_consumer_headers, async_consumer_headers, cluster_config_kip848());

/// Translates Java's `testAsyncConsumerPartitionPauseAndResume`.
/// Standalone pause/resume on an assigned partition (NOT the in-callback
/// reentrancy case, which is the Issue-8 gap). Consume 5, pause, produce 5
/// more, poll returns empty, resume, consume the next 5.
async fn async_consumer_partition_pause_and_resume<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let backend = factory.name();
    let topic = ctx.topic("topic");
    let tp = TopicPartition::new(topic.clone(), 0);
    let group_id = ctx.group_id("g_pause_resume");

    let producer = build_producer(ctx);
    let mut consumer = make_consumer(ctx, factory, &group_id, &[]).await;
    // Empty topic so the first produced record lands at offset 0 (Java parity).
    create_topic(consumer.as_mut(), backend, &topic, 2).await;

    let num_records = 5usize;
    let mut starting_timestamp = current_time_ms();
    send_records(&producer, &tp, num_records, starting_timestamp).await;

    consumer
        .assign(vec![tp.clone()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: assign: {e}"));
    consume_and_verify_records(consumer.as_mut(), backend, &tp, num_records, 0, 0, starting_timestamp).await;

    consumer
        .pause(std::slice::from_ref(&tp))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: pause: {e}"));
    starting_timestamp = current_time_ms();
    send_records(&producer, &tp, num_records, starting_timestamp).await;
    // Java: `assertTrue(consumer.poll(100ms).isEmpty())`.
    let polled = consumer
        .poll(Duration::from_millis(100))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: poll while paused: {e}"));
    assert!(
        polled.is_empty(),
        "poll should be empty while partition is paused ({backend} backend)"
    );

    consumer
        .resume(std::slice::from_ref(&tp))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: resume: {e}"));
    consume_and_verify_records(consumer.as_mut(), backend, &tp, num_records, 5, 0, starting_timestamp).await;

    producer.close().await.expect("producer close");
    consumer
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer close: {e}"));
}

multilanguage_consumer_test!(
    test_async_consumer_partition_pause_and_resume,
    async_consumer_partition_pause_and_resume,
    cluster_config_kip848()
);

/// Translates Java's `testAsyncConsumerPauseStateNotPreservedByRebalance`.
/// After a rebalance the pause state is lost and the position
/// is reset, so consumption resumes from the beginning of the partition.
async fn async_consumer_pause_state_not_preserved_by_rebalance<F: ConsumerBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
) {
    let backend = factory.name();
    let topic = ctx.topic("topic");
    let topic2 = ctx.topic("topic2");
    let tp = TopicPartition::new(topic.clone(), 0);
    let group_id = ctx.group_id("g_pause_rebalance");

    let producer = build_producer(ctx);
    let mut consumer = make_consumer(ctx, factory, &group_id, &[]).await;
    // Empty topics so the first produced record lands at offset 0 (Java parity).
    create_topic(consumer.as_mut(), backend, &topic, 2).await;
    create_topic(consumer.as_mut(), backend, &topic2, 2).await;

    let starting_timestamp = current_time_ms();
    send_records(&producer, &tp, 5, starting_timestamp).await;
    producer.close().await.expect("producer close");

    consumer
        .subscribe_with_topics(vec![topic.clone()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: subscribe: {e}"));
    consume_and_verify_records(consumer.as_mut(), backend, &tp, 5, 0, 0, starting_timestamp).await;
    consumer
        .pause(std::slice::from_ref(&tp))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: pause: {e}"));

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
        .unwrap_or_else(|e| panic!("{backend} backend: subscribe topic2: {e}"));
    consume_and_verify_records(consumer.as_mut(), backend, &tp, 0, 5, 0, starting_timestamp).await;

    consumer
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer close: {e}"));
}

multilanguage_consumer_test!(
    test_async_consumer_pause_state_not_preserved_by_rebalance,
    async_consumer_pause_state_not_preserved_by_rebalance,
    cluster_config_kip848()
);

/// Translates Java's `testAsyncConsumerPartitionsFor`.
async fn async_consumer_partitions_for<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let backend = factory.name();
    let topic = ctx.topic("topic");
    let group_id = ctx.group_id("g_partitions_for");

    // Java's org.apache.kafka.clients.consumer.PlaintextConsumerTest#testPartitionsFor:
    // `cluster.createTopic(TOPIC, 2, BROKER_COUNT)` and
    // `cluster.createTopic("part-test", numParts, 1)`.
    let part_test = ctx.topic("part-test");
    create_test_topic(ctx, &topic, 2, BROKER_COUNT).await;
    create_test_topic(ctx, &part_test, 2, 1).await;

    let mut consumer = make_consumer(ctx, factory, &group_id, &[]).await;
    let partitions = consumer
        .partitions_for(&topic)
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: partitions_for: {e}"));
    assert_eq!(partitions.len(), 2, "topic should have 2 partitions ({backend} backend)");

    consumer
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer close: {e}"));
}

multilanguage_consumer_test!(
    test_async_consumer_partitions_for,
    async_consumer_partitions_for,
    cluster_config_kip848()
);

/// Translates Java's `testAsyncConsumerPartitionsForAutoCreate`.
/// The first `partitions_for` for a nonexistent topic triggers broker
/// auto-create (enabled by default); poll until non-empty.
async fn async_consumer_partitions_for_auto_create<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let backend = factory.name();
    let non_exist = ctx.topic("non-exist-topic");
    let group_id = ctx.group_id("g_partitions_for_autocreate");

    let mut consumer = make_consumer(ctx, factory, &group_id, &[]).await;
    // First call would create the topic.
    let _ = consumer
        .partitions_for(&non_exist)
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: partitions_for: {e}"));
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut non_empty = false;
    while Instant::now() < deadline {
        let parts = consumer
            .partitions_for(&non_exist)
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: partitions_for: {e}"));
        if !parts.is_empty() {
            non_empty = true;
            break;
        }
    }
    assert!(
        non_empty,
        "Timed out while awaiting non-empty partitions for auto-created topic ({backend} backend)"
    );

    consumer
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer close: {e}"));
}

multilanguage_consumer_test!(
    test_async_consumer_partitions_for_auto_create,
    async_consumer_partitions_for_auto_create,
    cluster_config_kip848()
);

/// Translates Java's `PlaintextConsumerTest#testAsyncConsumerPartitionsForInvalidTopic`.
async fn async_consumer_partitions_for_invalid_topic<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let backend = factory.name();
    let group_id = ctx.group_id("g_partitions_for_invalid");

    let mut consumer = make_consumer(ctx, factory, &group_id, &[]).await;
    let err = consumer
        .partitions_for(";3# ads,{234")
        .await
        .expect_err("partitions_for on an invalid topic should fail");
    // Java: `assertThrows(InvalidTopicException.class, ...)`, raised as
    // `new InvalidTopicException("Topic '" + topic + "' is invalid")` by
    // `TopicMetadataRequestState#handleTopicMetadataResponse`, nested in
    // org.apache.kafka.clients.consumer.internals.TopicMetadataRequestManager.
    assert!(
        matches!(err, Error::InvalidTopic(_)),
        "expected Error::InvalidTopic, got: {err:?} ({backend} backend)"
    );
    assert_eq!(err.message(), "Topic ';3# ads,{234' is invalid", "values ({backend} backend)");

    consumer
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer close: {e}"));
}

multilanguage_consumer_test!(
    test_async_consumer_partitions_for_invalid_topic,
    async_consumer_partitions_for_invalid_topic,
    cluster_config_kip848()
);

/// Translates Java's `testAsyncConsumerListTopics`.
async fn async_consumer_list_topics<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let backend = factory.name();
    let topic1 = ctx.topic("part-test-topic-1");
    let topic2 = ctx.topic("part-test-topic-2");
    let topic3 = ctx.topic("part-test-topic-3");
    let group_id = ctx.group_id("g_list_topics");

    let producer = build_producer(ctx);
    let mut consumer = make_consumer(ctx, factory, &group_id, &[]).await;
    create_test_topic(ctx, &topic1, 2, 1).await;
    create_test_topic(ctx, &topic2, 2, 1).await;
    create_test_topic(ctx, &topic3, 2, 1).await;
    send_records(&producer, &TopicPartition::new(topic1.clone(), 0), 1, current_time_ms()).await;
    producer.close().await.expect("producer close");

    consumer
        .subscribe_with_topics(vec![topic1.clone()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: subscribe: {e}"));
    let _ = consumer
        .poll(Duration::from_millis(100))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: poll: {e}"));

    // Retry list_topics until all three named topics are visible (metadata
    // propagation window).
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut topics = consumer
        .list_topics()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: list_topics: {e}"));
    while Instant::now() < deadline
        && !(topics.contains_key(&topic1) && topics.contains_key(&topic2) && topics.contains_key(&topic3))
    {
        let _ = consumer
            .poll(Duration::from_millis(100))
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: poll: {e}"));
        topics = consumer
            .list_topics()
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: list_topics: {e}"));
    }

    // Java asserts a total topic count of 4 (3 test topics + the internal
    // `__consumer_offsets`). The exact internal-topic count is
    // broker-version-dependent under a shared pool, so we assert the three
    // named topics are present with 2 partitions each (the load-bearing
    // contract) and that the total is at least 4.
    assert!(
        topics.len() >= 4,
        "expected at least 4 topics (3 test + internal), got {} ({backend} backend)",
        topics.len()
    );
    assert_eq!(topics.get(&topic1).expect("topic1").len(), 2, "values ({backend} backend)");
    assert_eq!(topics.get(&topic2).expect("topic2").len(), 2, "values ({backend} backend)");
    assert_eq!(topics.get(&topic3).expect("topic3").len(), 2, "values ({backend} backend)");

    consumer
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer close: {e}"));
}

multilanguage_consumer_test!(
    test_async_consumer_list_topics,
    async_consumer_list_topics,
    cluster_config_kip848()
);

/// Translates Java's `testAsyncConsumerSeek`.
/// Covers seekToEnd → position==total, seekToBeginning → position==0, and
/// seek(mid) → position==mid, with consumption verification.
///
/// Translation deviation: the compressed-message half of Java's `testSeek`
/// is SKIPped here — the byte harness has no `compression.type=gzip` +
/// `linger.ms=MAX` producer helper, and the non-compressed half fully
/// exercises the in-scope gap (seekToEnd / seekToBeginning / seek-mid).
async fn async_consumer_seek<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let backend = factory.name();
    let topic = ctx.topic("topic");
    let tp = TopicPartition::new(topic.clone(), 0);
    let group_id = ctx.group_id("g_seek");

    let total_records: usize = 50;
    let mid: usize = total_records / 2;

    let producer = build_producer(ctx);
    let mut consumer = make_consumer(ctx, factory, &group_id, &[]).await;
    // Java uses `startingTimestamp = 0`. Empty topic so records start at
    // offset 0, matching Java's offset==index expectation.
    create_test_topic(ctx, &topic, 2, BROKER_COUNT).await;
    let starting_timestamp: i64 = 0;
    send_records(&producer, &tp, total_records, starting_timestamp).await;
    producer.close().await.expect("producer close");

    consumer
        .assign(vec![tp.clone()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: assign: {e}"));

    consumer
        .seek_to_end(std::slice::from_ref(&tp))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: seek_to_end: {e}"));
    assert_eq!(
        consumer
            .position(&tp)
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: position: {e}")),
        total_records as i64,
        "values ({backend} backend)"
    );
    assert!(
        consumer
            .poll(Duration::from_millis(50))
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: poll: {e}"))
            .is_empty(),
        "assertion ({backend} backend)"
    );

    consumer
        .seek_to_beginning(std::slice::from_ref(&tp))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: seek_to_beginning: {e}"));
    assert_eq!(
        consumer
            .position(&tp)
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: position: {e}")),
        0,
        "values ({backend} backend)"
    );
    consume_and_verify_records(consumer.as_mut(), backend, &tp, 1, 0, 0, starting_timestamp).await;

    consumer
        .seek_with_offset(tp.clone(), mid as i64)
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: seek mid: {e}"));
    assert_eq!(
        consumer
            .position(&tp)
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: position: {e}")),
        mid as i64,
        "values ({backend} backend)"
    );
    // Record at offset mid has key/value index mid and timestamp mid.
    consume_and_verify_records(consumer.as_mut(), backend, &tp, 1, mid as i64, mid, mid as i64).await;

    consumer
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer close: {e}"));
}

multilanguage_consumer_test!(test_async_consumer_seek, async_consumer_seek, cluster_config_kip848());

/// Upper bound on the body of
/// [`test_async_consumer_seek_throws_illegal_state_if_partitions_not_assigned`].
const SEEK_TEST_TIMEOUT: Duration = Duration::from_secs(120);

/// Upper bound on a consumer `close()` in a bounded test.
const CONSUMER_CLOSE_TIMEOUT: Duration = Duration::from_secs(30);

/// Translates Java's
/// `testAsyncConsumerSeekThrowsIllegalStateIfPartitionsNotAssigned`.
/// `seek_to_end` on an unassigned partition raises IllegalState with the
/// EXACT Java message.
async fn async_consumer_seek_throws_illegal_state_if_partitions_not_assigned<F: ConsumerBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
) {
    let backend = factory.name();
    let topic = ctx.topic("topic");
    let tp = TopicPartition::new(topic.clone(), 0);
    let group_id = ctx.group_id("g_seek_illegal_state");

    // Bounded so that a hang fails this test promptly; it has previously stalled
    // a CI job until the job's time limit.
    with_timeout("seek_to_end on an unassigned partition", SEEK_TEST_TIMEOUT, async {
        let mut consumer = make_consumer(ctx, factory, &group_id, &[]).await;
        create_test_topic(ctx, &topic, 2, BROKER_COUNT).await;
        let err = consumer
            .seek_to_end(std::slice::from_ref(&tp))
            .await
            .expect_err("seek_to_end unassigned should fail");
        match err {
            Error::LocalIllegalState(msg) => {
                // Java: `"No current assignment for partition " + TP`.
                assert_eq!(
                    msg.message(),
                    format!("No current assignment for partition {tp}"),
                    "values ({backend} backend)"
                );
            },
            other => panic!("expected IllegalState, got {other:?}"),
        }

        with_timeout("consumer close", CONSUMER_CLOSE_TIMEOUT, consumer.close())
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: consumer close: {e}"));
    })
    .await;
}

multilanguage_consumer_test!(
    test_async_consumer_seek_throws_illegal_state_if_partitions_not_assigned,
    async_consumer_seek_throws_illegal_state_if_partitions_not_assigned,
    cluster_config_kip848()
);

/// Translates Java's `testAsyncConsumerConsumeMessagesWithLogAppendTime`.
/// Creates the topic with `message.timestamp.type=LogAppendTime`
/// (2 partitions, RF 2, `PlaintextConsumerTest#testConsumeMessagesWithLogAppendTime`); asserts
/// `timestamp_type == LogAppendTime` and the broker-stamped timestamp is in
/// `[startTime, now]` widened by a clock-skew slack (see
/// `CLOCK_SKEW_SLACK_MS` below).
///
/// Translation deviations: the compressed-message half is SKIPped (no gzip
/// producer helper); the non-compressed half covers the LogAppendTime
/// timestamp-type gap. The timestamp range bound carries a skew slack Java
/// does not need.
async fn async_consumer_consume_messages_with_log_append_time<F: ConsumerBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
) {
    let backend = factory.name();
    let topic = ctx.topic("log-append-time");
    let tp = TopicPartition::new(topic.clone(), 0);
    let group_id = ctx.group_id("g_log_append_time");

    let start_time = current_time_ms();
    let num_records = 50usize;

    let producer = build_producer(ctx);
    let mut consumer = make_consumer(ctx, factory, &group_id, &[]).await;
    // Empty topic so records start at offset 0 (Java parity).
    create_test_topic_with_configs(
        ctx,
        &topic,
        2,
        2,
        BTreeMap::from([("message.timestamp.type".to_string(), "LogAppendTime".to_string())]),
    )
    .await;
    // Producer-supplied timestamps are IGNORED by a LogAppendTime topic;
    // the broker stamps each record with its append time.
    send_records(&producer, &tp, num_records, start_time).await;
    producer.close().await.expect("producer close");

    consumer
        .assign(vec![tp.clone()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: assign: {e}"));

    let records = consume_records(consumer.as_mut(), backend, num_records).await;
    let now = current_time_ms();

    // Translation deviation (CLAUDE.md #6 / DoD #7): Java bounds the
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
        assert_eq!(record.topic, tp.topic(), "values ({backend} backend)");
        assert_eq!(record.partition, tp.partition(), "values ({backend} backend)");
        assert_eq!(
            record.timestamp_type,
            TimestampType::LogAppendTime,
            "timestamp_type should be LogAppendTime ({backend} backend)"
        );
        assert!(
            record.timestamp >= start_time - CLOCK_SKEW_SLACK_MS && record.timestamp <= now + CLOCK_SKEW_SLACK_MS,
            "timestamp {} should be within [{start_time}, {now}] widened by the \
             {CLOCK_SKEW_SLACK_MS} ms broker/client clock-skew slack ({backend} backend)",
            record.timestamp
        );
        assert_eq!(record.offset, i as i64, "values ({backend} backend)");
        let expected_key = format!("key {i}").into_bytes();
        let expected_value = format!("value {i}").into_bytes();
        assert_eq!(record.key.as_ref().expect("key"), &expected_key, "values ({backend} backend)");
        assert_eq!(
            record.value.as_ref().expect("value"),
            &expected_value,
            "values ({backend} backend)"
        );
        assert_eq!(
            record.serialized_key_size,
            expected_key.len() as i32,
            "serialized key size at {i} ({backend} backend)"
        );
        assert_eq!(
            record.serialized_value_size,
            expected_value.len() as i32,
            "serialized value size at {i} ({backend} backend)"
        );
    }

    consumer
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer close: {e}"));
}

multilanguage_consumer_test!(
    test_async_consumer_consume_messages_with_log_append_time,
    async_consumer_consume_messages_with_log_append_time,
    cluster_config_kip848()
);

/// Translates Java's `testAsyncConsumerEndOffsets`.
/// Produces N records, subscribes, awaits the 2-partition assignment, then
/// asserts `end_offsets([TP]) == N`.
///
/// Translation deviation: Java uses N=10000; we use N=200 for harness
/// speed (outcome parity — `end_offsets` equals the cumulative produced
/// count — not count parity).
async fn async_consumer_end_offsets<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let backend = factory.name();
    let topic = ctx.topic("topic");
    let tp = TopicPartition::new(topic.clone(), 0);
    let tp2 = TopicPartition::new(topic.clone(), 1);
    let group_id = ctx.group_id("g_end_offsets");

    let num_records = 200usize;
    let producer = build_producer(ctx);
    let mut consumer = make_consumer(ctx, factory, &group_id, &[]).await;
    // Empty topic so records start at offset 0 (Java parity).
    create_test_topic(ctx, &topic, 2, BROKER_COUNT).await;
    send_records(&producer, &tp, num_records, current_time_ms()).await;
    producer.close().await.expect("producer close");

    consumer
        .subscribe_with_topics(vec![topic.clone()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: subscribe: {e}"));
    await_assignment(
        consumer.as_mut(),
        backend,
        &HashSet::from([tp.clone(), tp2.clone()]),
        Duration::from_secs(90),
    )
    .await;

    let end_offsets = consumer
        .end_offsets(std::slice::from_ref(&tp))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: end_offsets: {e}"));
    assert_eq!(
        end_offsets.get(&tp).copied(),
        Some(num_records as i64),
        "end_offsets for tp should equal num_records ({backend} backend)"
    );

    consumer
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer close: {e}"));
}

multilanguage_consumer_test!(
    test_async_consumer_end_offsets,
    async_consumer_end_offsets,
    cluster_config_kip848()
);

/// Translates Java's `testAsyncConsumerFetchOffsetsForTime`.
/// Produces 100 records per partition with timestamp == sequence number,
/// then `offsets_for_times` for ts 0 (partition 0) and ts 20 (partition 1);
/// asserts offset/timestamp/leader_epoch. Also asserts negative target time
/// raises IllegalArgument.
async fn async_consumer_fetch_offsets_for_time<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let backend = factory.name();
    let topic = ctx.topic("topic");
    let tp0 = TopicPartition::new(topic.clone(), 0);
    let tp1 = TopicPartition::new(topic.clone(), 1);
    let group_id = ctx.group_id("g_offsets_for_time");

    // Java's org.apache.kafka.clients.consumer.PlaintextConsumerTest#testFetchOffsetsForTime:
    // `cluster.createTopic(TOPIC, 2, BROKER_COUNT)` — an EMPTY topic, so offset 0
    // on each partition is a real `ts==0` record.
    create_test_topic(ctx, &topic, 2, BROKER_COUNT).await;
    let producer = build_producer(ctx);
    // partition 0: key/val/timestamp == sequence number; partition 1: same.
    send_records(&producer, &tp0, 100, 0).await;
    send_records(&producer, &tp1, 100, 0).await;
    producer.close().await.expect("producer close");

    // Empty topic ⇒ the produced records start at offset 0 on each
    // partition.
    let base0 = 0i64;
    let base1 = 0i64;

    let mut consumer = make_consumer(ctx, factory, &group_id, &[]).await;

    // Java: negative target time → IllegalArgumentException.
    let neg_err = consumer
        .offsets_for_times(HashMap::from([(tp0.clone(), -1i64)]))
        .await
        .expect_err("negative target time should fail");
    assert!(
        matches!(neg_err, Error::LocalIllegalArgument(_)),
        "expected IllegalArgument for negative target time, got {neg_err:?} ({backend} backend)"
    );
    // The message of org.apache.kafka.clients.consumer.internals.AsyncKafkaConsumer#offsetsForTimes.
    assert_eq!(
        neg_err.message(),
        format!("The target time for partition {tp0} is -1. The target time cannot be negative."),
        "negative target time message ({backend} backend)"
    );

    // Search ts 0 on partition 0 and ts 20 on partition 1.
    let search = HashMap::from([(tp0.clone(), 0i64), (tp1.clone(), 20i64)]);
    let offsets = consumer
        .offsets_for_times(search)
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: offsets_for_times: {e}"));

    let r0 = offsets
        .get(&tp0)
        .and_then(Option::as_ref)
        .expect("offset present for ts 0 on partition 0");
    assert_eq!(
        r0.offset(),
        base0,
        "first record at-or-after ts 0 is the base offset ({backend} backend)"
    );
    assert_eq!(r0.timestamp(), 0, "values ({backend} backend)");
    assert_eq!(r0.leader_epoch(), Some(0), "values ({backend} backend)");

    let r1 = offsets
        .get(&tp1)
        .and_then(Option::as_ref)
        .expect("offset present for ts 20 on partition 1");
    assert_eq!(
        r1.offset(),
        base1 + 20,
        "first record at-or-after ts 20 is base+20 ({backend} backend)"
    );
    assert_eq!(r1.timestamp(), 20, "values ({backend} backend)");
    assert_eq!(r1.leader_epoch(), Some(0), "values ({backend} backend)");

    consumer
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer close: {e}"));
}

multilanguage_consumer_test!(
    test_async_consumer_fetch_offsets_for_time,
    async_consumer_fetch_offsets_for_time,
    cluster_config_kip848()
);

/// Translates Java's `testAsyncConsumerConsumingWithNullGroupId`.
/// Three groupless consumers consume from earliest / latest / explicit
/// offset; asserts record counts and that commit/committed raise
/// InvalidGroupId for groupless consumers.
async fn async_consumer_consuming_with_null_group_id<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let backend = factory.name();
    // Java uses a single-partition topic (createTopic(TOPIC, 1, 1)).
    let topic = ctx.topic("topic-null-group");
    let tp = TopicPartition::new(topic.clone(), 0);

    let producer = build_producer(ctx);
    // consumer1: groupless, earliest. consumer2: groupless, latest.
    // consumer3: groupless, explicit seek.
    let mut consumer1 = create_from(
        factory,
        make_groupless_consumer_config(ctx, factory, &[("auto.offset.reset", "earliest"), ("client.id", "consumer1")]),
    )
    .await;
    let mut consumer2 = create_from(
        factory,
        make_groupless_consumer_config(ctx, factory, &[("auto.offset.reset", "latest"), ("client.id", "consumer2")]),
    )
    .await;
    let mut consumer3 = create_from(
        factory,
        make_groupless_consumer_config(ctx, factory, &[("auto.offset.reset", "earliest"), ("client.id", "consumer3")]),
    )
    .await;

    // Java: createTopic(TOPIC, 1, 1) — an EMPTY topic, so the 3 records below
    // land at offsets 0, 1, 2. Create it via metadata auto-create (the broker
    // gives 2 partitions; only partition 0 is used).
    create_test_topic(ctx, &topic, 1, 1).await;
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
            .unwrap_or_else(|e| panic!("{backend} backend: send: {e}"));
        fut.get_with_timeout(Duration::from_secs(30))
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: ack: {e}"));
    }
    producer.close().await.expect("producer close");

    consumer1
        .assign(vec![tp.clone()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: assign c1: {e}"));
    consumer2
        .assign(vec![tp.clone()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: assign c2: {e}"));
    consumer3
        .assign(vec![tp.clone()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: assign c3: {e}"));
    // Java: `consumer3.seek_with_offset(TP, 1)` — a literal offset, skipping the first of
    // the 3 records (offsets 0, 1, 2).
    consumer3
        .seek_with_offset(tp.clone(), 1)
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: seek c3: {e}"));

    let num_records1 = poll_count(consumer1.as_mut(), backend, 3, Duration::from_secs(15)).await;
    // Java: commitSync / committed raise InvalidGroupId for groupless.
    let c1_commit = consumer1.commit_sync().await.expect_err("groupless commit_sync should fail");
    assert!(
        matches!(c1_commit, Error::InvalidGroupId(_)),
        "got {c1_commit:?} ({backend} backend)"
    );
    let c2_committed = consumer2
        .committed(std::slice::from_ref(&tp))
        .await
        .expect_err("groupless committed should fail");
    assert!(
        matches!(c2_committed, Error::InvalidGroupId(_)),
        "got {c2_committed:?} ({backend} backend)"
    );

    let num_records2 = poll_count(consumer2.as_mut(), backend, 0, Duration::from_secs(5)).await;
    let num_records3 = poll_count(consumer3.as_mut(), backend, 2, Duration::from_secs(15)).await;

    consumer1
        .unsubscribe()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: unsubscribe c1: {e}"));
    consumer2
        .unsubscribe()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: unsubscribe c2: {e}"));
    consumer3
        .unsubscribe()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: unsubscribe c3: {e}"));
    assert!(consumer1.assignment().is_empty(), "assertion ({backend} backend)");
    assert!(consumer2.assignment().is_empty(), "assertion ({backend} backend)");
    assert!(consumer3.assignment().is_empty(), "assertion ({backend} backend)");

    consumer1
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: close c1: {e}"));
    consumer2
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: close c2: {e}"));
    consumer3
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: close c3: {e}"));

    assert_eq!(
        num_records1, 3,
        "consumer1 should consume from earliest (3 records) ({backend} backend)"
    );
    assert_eq!(
        num_records2, 0,
        "consumer2 should consume from latest (0 records) ({backend} backend)"
    );
    assert_eq!(
        num_records3, 2,
        "consumer3 should consume from offset 1 (2 records) ({backend} backend)"
    );
}

multilanguage_consumer_test!(
    test_async_consumer_consuming_with_null_group_id,
    async_consumer_consuming_with_null_group_id,
    cluster_config_kip848()
);

/// Translates Java's `testAsyncConsumerNullGroupIdNotSupportedIfCommitting`.
/// A groupless consumer's `commit_sync` raises InvalidGroupId
/// with the EXACT Java message.
async fn async_consumer_null_group_id_not_supported_if_committing<F: ConsumerBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
) {
    let backend = factory.name();
    let topic = ctx.topic("topic");
    let tp = TopicPartition::new(topic.clone(), 0);

    let mut consumer = create_from(
        factory,
        make_groupless_consumer_config(ctx, factory, &[("auto.offset.reset", "earliest"), ("client.id", "consumer1")]),
    )
    .await;
    create_test_topic(ctx, &topic, 2, BROKER_COUNT).await;
    consumer
        .assign(vec![tp.clone()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: assign: {e}"));

    let err = consumer.commit_sync().await.expect_err("groupless commit_sync should fail");
    assert!(matches!(err, Error::InvalidGroupId(_)), "got {err:?} ({backend} backend)");
    // Java: `InvalidGroupIdException` message verbatim.
    assert_eq!(
        err.message(),
        "To use the group management or offset commit APIs, you must provide a valid \
         group.id in the consumer configuration.",
        "values ({backend} backend)"
    );

    consumer
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer close: {e}"));
}

multilanguage_consumer_test!(
    test_async_consumer_null_group_id_not_supported_if_committing,
    async_consumer_null_group_id_not_supported_if_committing,
    cluster_config_kip848()
);

/// Translates Java's `testAsyncConsumerPositionRespectsTimeout`.
/// `position` for a partition that doesn't exist times out after the
/// user-supplied timeout.
async fn async_consumer_position_respects_timeout<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let backend = factory.name();
    let topic = ctx.topic("topic");
    // partition 15 does not exist (topic has 2 partitions).
    let tp = TopicPartition::new(topic.clone(), 15);
    let group_id = ctx.group_id("g_position_timeout");

    let mut consumer = make_consumer(ctx, factory, &group_id, &[]).await;
    create_topic(consumer.as_mut(), backend, &topic, 2).await;
    consumer
        .assign(vec![tp.clone()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: assign: {e}"));

    let err = consumer
        .position_with_timeout(&tp, Duration::from_secs(3))
        .await
        .expect_err("position on a nonexistent partition should time out");
    assert!(
        matches!(err, Error::Timeout(_)),
        "expected Timeout, got {err:?} ({backend} backend)"
    );

    consumer
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer close: {e}"));
}

multilanguage_consumer_test!(
    test_async_consumer_position_respects_timeout,
    async_consumer_position_respects_timeout,
    cluster_config_kip848()
);

/// Translates Java's `testAsyncConsumerPositionRespectsWakeup`.
/// A concurrent `wakeup()` interrupts a blocking `position` (§11).
async fn async_consumer_position_respects_wakeup<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let backend = factory.name();
    let topic = ctx.topic("topic");
    let tp = TopicPartition::new(topic.clone(), 15);
    let group_id = ctx.group_id("g_position_wakeup");

    let (mut consumer, log) = make_consumer_with_log(ctx, factory, &group_id, &[]).await;
    create_topic(consumer.as_mut(), backend, &topic, 2).await;
    consumer
        .assign(vec![tp.clone()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: assign: {e}"));

    // Java's org.apache.kafka.clients.consumer.PlaintextConsumerTest#testPositionRespectsWakeup:
    // `CompletableFuture.runAsync(() -> { sleep(1s); consumer.wakeup(); })`. Java's `Consumer` reference is
    // freely shareable across threads. The Rust equivalent obtains a
    // `Clone + Send + Sync` `ConsumerHandle` BEFORE the `&mut` borrow taken
    // by `position_with_timeout`, then fires `wakeup()` from a spawned task —
    // sound, no `unsafe`, no reference to the consumer crossing the boundary.
    // A gRPC backend's handle is its `Wakeup` RPC (`wakeup_handle`).
    let handle = log.wakeup_handle(consumer.as_ref());
    let waker = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(1)).await;
        handle.wakeup().await;
    });

    let result = consumer.position_with_timeout(&tp, Duration::from_secs(3)).await;
    let _ = waker.await;
    let err = result.expect_err("position should be interrupted by wakeup");
    assert!(
        matches!(err, Error::Wakeup(_)),
        "expected Wakeup, got {err:?} ({backend} backend)"
    );

    consumer
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer close: {e}"));
}

multilanguage_consumer_test!(
    test_async_consumer_position_respects_wakeup,
    async_consumer_position_respects_wakeup,
    cluster_config_kip848()
);

/// Translates Java's
/// `testAsyncConsumerPositionWithErrorConnectionRespectsWakeup`.
/// With an unreachable bootstrap, `wakeup()` still interrupts a blocking
/// `position`.
async fn async_consumer_position_with_error_connection_respects_wakeup<F: ConsumerBackendFactory>(
    _ctx: &mut TestContext,
    factory: &F,
) {
    let backend = factory.name();
    // bootstrap points at an unreachable address (Java: "localhost:12345").
    // This is a dead-broker negative test: the address never accepts a
    // connection, so the security protocol is irrelevant (no handshake ever
    // starts) — keep the explicit dead address rather than the protocol
    // bootstrap. It is unreachable from a backend container as from the host.
    let props = HashMap::from([
        ("bootstrap.servers".to_string(), "localhost:12345".to_string()),
        ("group.protocol".to_string(), "consumer".to_string()),
        ("auto.offset.reset".to_string(), "earliest".to_string()),
        ("client.id".to_string(), "integration-test-consumer".to_string()),
        ("enable.auto.commit".to_string(), "false".to_string()),
        ("group.id".to_string(), "g_position_err_wakeup".to_string()),
    ]);
    let (mut consumer, log) = factory
        .create_with_callback_log(props)
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: create consumer: {e}"));
    let topic = "topic".to_string();
    let tp = TopicPartition::new(topic, 15);
    consumer
        .assign(vec![tp.clone()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: assign: {e}"));

    // Java's org.apache.kafka.clients.consumer.PlaintextConsumerTest#testPositionWithErrorConnectionRespectsWakeup:
    // a cross-thread
    // `wakeup()` interrupts a blocking `position` even when the bootstrap
    // is unreachable. Obtain the shareable handle before the `&mut` borrow.
    let handle = log.wakeup_handle(consumer.as_ref());
    let waker = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(1)).await;
        handle.wakeup().await;
    });

    let result = consumer.position_with_timeout(&tp, Duration::from_secs(100)).await;
    let _ = waker.await;
    let err = result.expect_err("position should be interrupted by wakeup despite connection error");
    assert!(
        matches!(err, Error::Wakeup(_)),
        "expected Wakeup, got {err:?} ({backend} backend)"
    );

    consumer
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer close: {e}"));
}

multilanguage_consumer_test!(
    test_async_consumer_position_with_error_connection_respects_wakeup,
    async_consumer_position_with_error_connection_respects_wakeup
);

/// Translates Java's `testAsyncConsumerOffsetRelatedWhenTimeoutZero`.
/// Zero-timeout offset queries return immediately:
/// beginning/end offsets empty; offsets_for_times has the key with a
/// `None` value.
async fn async_consumer_offset_related_when_timeout_zero<F: ConsumerBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
) {
    let backend = factory.name();
    let topic = ctx.topic("topic");
    let tp = TopicPartition::new(topic.clone(), 0);
    let group_id = ctx.group_id("g_timeout_zero");

    let mut consumer = make_consumer(ctx, factory, &group_id, &[]).await;
    create_test_topic(ctx, &topic, 2, BROKER_COUNT).await;

    let result1 = consumer
        .beginning_offsets_with_timeout(std::slice::from_ref(&tp), Duration::ZERO)
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: beginning_offsets(ZERO): {e}"));
    assert_eq!(
        result1.len(),
        0,
        "beginning_offsets with zero timeout should be empty ({backend} backend)"
    );

    let result2 = consumer
        .end_offsets_with_timeout(std::slice::from_ref(&tp), Duration::ZERO)
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: end_offsets(ZERO): {e}"));
    assert_eq!(
        result2.len(),
        0,
        "end_offsets with zero timeout should be empty ({backend} backend)"
    );

    let result3 = consumer
        .offsets_for_times_with_timeout(HashMap::from([(tp.clone(), 0i64)]), Duration::ZERO)
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: offsets_for_times(ZERO): {e}"));
    // Java: `assertEquals(1, result3.size())` and `assertNull(result3.get(tp))`.
    assert_eq!(
        result3.len(),
        1,
        "offsets_for_times with zero timeout: {result3:?} ({backend} backend)"
    );
    assert_eq!(
        result3.get(&tp),
        Some(&None),
        "offsets_for_times with zero timeout should not resolve an offset ({backend} backend)"
    );

    consumer
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer close: {e}"));
}

multilanguage_consumer_test!(
    test_async_consumer_offset_related_when_timeout_zero,
    async_consumer_offset_related_when_timeout_zero,
    cluster_config_kip848()
);

// ── Broker fault injection (Phase 16) ─────────────────────────────────

/// The class-level `@ClusterTestDefaults(serverProperties = ...)` of
/// `PlaintextConsumerTest`, plus the
/// KIP-848 opt-in the rest of this file's clusters carry.
fn plaintext_consumer_test_defaults() -> BTreeMap<String, String> {
    BTreeMap::from([
        (
            "KAFKA_GROUP_COORDINATOR_REBALANCE_PROTOCOLS".to_string(),
            "classic,consumer".to_string(),
        ),
        ("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS".to_string(), "1".to_string()),
        ("KAFKA_GROUP_MIN_SESSION_TIMEOUT_MS".to_string(), "100".to_string()),
        ("KAFKA_GROUP_MAX_SESSION_TIMEOUT_MS".to_string(), "60000".to_string()),
        ("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS".to_string(), "10".to_string()),
    ])
}

/// Java's `ClientsTestUtils.TestConsumerReassignmentListener` counts its
/// `onPartitionsAssigned` / `onPartitionsRevoked` invocations; the backend's
/// logging listener logs one `assigned` / `revoked` / `lost` entry per
/// invocation, so a count is the number of entries of the given kinds.
async fn listener_calls(log: &ConsumerCallbackLog, backend: &str, kinds: &[&str]) -> usize {
    log.entries()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: read consumer callback log: {e}"))
        .iter()
        .filter(|e| kinds.contains(&e.kind.as_str()))
        .count()
}

/// Java's `callsToRevoked`: the listener keeps the interface's default
/// `onPartitionsLost`, which calls `onPartitionsRevoked`, so a `lost` entry
/// counts as a revocation.
const REVOKED_KINDS: &[&str] = &[KIND_REVOKED, KIND_LOST];

/// The `commit` entries of `log`, oldest first.
async fn commit_entries(log: &ConsumerCallbackLog, backend: &str) -> Vec<CallbackLogEntry> {
    log.entries()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: read consumer callback log: {e}"))
        .into_iter()
        .filter(|e| e.kind == KIND_COMMIT)
        .collect()
}

/// Java's `TestUtils.waitForCondition` default bound.
const DEFAULT_MAX_WAIT: Duration = Duration::from_secs(15);

/// Translates `ClientsTestUtils#awaitRebalance`, called as
/// `awaitRebalance(consumer, listener)`: `poll(100ms)` until `callsToAssigned`
/// advances, within `waitForCondition`'s default 15 s.
async fn await_rebalance(consumer: &mut BytesConsumer, backend: &str, log: &ConsumerCallbackLog) {
    let num_reassignments = listener_calls(log, backend, &[KIND_ASSIGNED]).await;
    let deadline = Instant::now() + DEFAULT_MAX_WAIT;
    loop {
        consumer
            .poll(Duration::from_millis(100))
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: poll should succeed: {e}"));
        if listener_calls(log, backend, &[KIND_ASSIGNED]).await > num_reassignments {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "Timed out before expected rebalance completed ({backend} backend)"
        );
    }
}

/// Translates `ClientsTestUtils#sendAndAwaitAsyncCommit`, called as
/// `sendAndAwaitAsyncCommit(consumer, Optional.empty())`, with its
/// `RetryCommitCallback`: `commitAsync(callback)`, `poll(100ms)` until the
/// callback completes (15 s), then assert no error. A
/// `RetriableCommitFailedException` makes Java's callback resend itself; here
/// the logged retriable failure makes this helper resend, as the callback
/// cannot hold the consumer while the caller polls it.
async fn send_and_await_async_commit(consumer: &mut Box<BytesConsumer>, backend: &str, log: &ConsumerCallbackLog) {
    let mut seen = commit_entries(log, backend).await.len();
    log.commit_async_with_logging_callback(consumer)
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: commitAsync should enqueue: {e}"));
    let deadline = Instant::now() + DEFAULT_MAX_WAIT;
    loop {
        consumer
            .poll(Duration::from_millis(100))
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: poll should succeed: {e}"));
        let entries = commit_entries(log, backend).await;
        for entry in &entries[seen..] {
            match &entry.error_value {
                Some(e) if e.is_consumer_retriable_commit_failed_error() => {
                    log.commit_async_with_logging_callback(consumer)
                        .await
                        .unwrap_or_else(|e| panic!("{backend} backend: retriable resend should enqueue: {e}"));
                },
                other => {
                    assert!(other.is_none(), "async commit failed: {other:?} ({backend} backend)");
                    return;
                },
            }
        }
        seen = entries.len();
        assert!(
            Instant::now() < deadline,
            "Failed to observe commit callback before timeout ({backend} backend)"
        );
    }
}

/// Translates `ClientsTestUtils#ensureNoRebalance`, called as
/// `ensureNoRebalance(consumer, listener)`.
async fn ensure_no_rebalance(consumer: &mut Box<BytesConsumer>, backend: &str, log: &ConsumerCallbackLog) {
    // The best way to verify that the current membership is still active is
    // to commit offsets. This would fail if the group had rebalanced.
    let initial_revoke_calls = listener_calls(log, backend, REVOKED_KINDS).await;
    send_and_await_async_commit(consumer, backend, log).await;
    assert_eq!(
        initial_revoke_calls,
        listener_calls(log, backend, REVOKED_KINDS).await,
        "revoke calls ({backend} backend)"
    );
}

/// Translates `PlaintextConsumerTest#testAsyncConsumeCoordinatorFailover` (body
/// `ClientsTestUtils.BaseConsumerTestcase#testCoordinatorFailover`) on the class
/// shape: `Type.KRAFT`, `BROKER_COUNT` = 3 brokers, the `@ClusterTestDefaults`
/// broker properties — as a dedicated cluster with an isolated controller, since
/// the test stops a broker.
///
/// The consumer joins, then the broker leading the single `__consumer_offsets`
/// partition (the group coordinator) is shut down; the consumer must find the
/// new coordinator and keep its membership, proven by a successful async
/// commit with no revocation.
///
/// **Deviation:** Java never creates `topic` (it relies on auto-creation, or
/// on the empty first assignment firing the listener). The Rust test creates
/// it the way the class's other tests do (`createTopic(TOPIC, 2,
/// BROKER_COUNT)`) so the first assignment is deterministic.
async fn async_consume_coordinator_failover<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    use confluent_kafka::admin::AdminClient;
    use confluent_kafka::admin::AdminClientConfig;

    use crate::common::test_utils;

    let backend = factory.name();
    let bootstrap = ctx.bootstrap_servers().to_string();
    let topic = ctx.topic("topic");
    let admin = AdminClient::create(
        AdminClientConfig::new(&HashMap::from([("bootstrap.servers".to_string(), bootstrap.clone())]))
            .expect("valid admin config"),
    )
    .expect("admin client");
    test_utils::create_topic(admin.as_ref(), &topic, 2, 3).await;
    admin.close_with_timeout(Duration::from_secs(5)).await;

    // Use higher poll timeout to avoid consumer leaving the group due to timeout
    let (mut consumer, log) = make_consumer_with_log(
        ctx,
        factory,
        &ctx.group_id("coordinator_failover"),
        &[("max.poll.interval.ms", "15000")],
    )
    .await;
    log.subscribe_with_logging_listener(&mut consumer, vec![topic.clone()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: subscribe should succeed: {e}"));
    // the initial subscription should cause a callback execution
    await_rebalance(consumer.as_mut(), backend, &log).await;
    assert_eq!(
        1,
        listener_calls(&log, backend, &[KIND_ASSIGNED]).await,
        "calls to assigned ({backend} backend)"
    );

    // get metadata for the topic. `Topic.GROUP_METADATA_TOPIC_NAME`; the Rust
    // `Topic` lives in a crate-internal module, so the name is spelled out.
    // Java loops while `partitionsFor` returns null; the Rust API returns a
    // `Result`, so the loop retries a failed or empty lookup, bounded at 60 s.
    let deadline = Instant::now() + Duration::from_secs(60);
    let parts = loop {
        match consumer.partitions_for("__consumer_offsets").await {
            Ok(parts) if !parts.is_empty() => break parts,
            other => assert!(
                Instant::now() < deadline,
                "no metadata for __consumer_offsets: {other:?} ({backend} backend)"
            ),
        }
        // Same pause as `create_topic`'s metadata retry loop.
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    assert_eq!(1, parts.len(), "__consumer_offsets partitions ({backend} backend)");
    let coordinator = parts[0].leader().expect("offsets partition has a leader").id();

    // shutdown the coordinator
    ctx.cluster().shutdown_broker(coordinator).await;

    // the failover should not cause a rebalance
    ensure_no_rebalance(&mut consumer, backend, &log).await;

    consumer
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer close should succeed: {e}"));
}

multilanguage_consumer_test!(
    test_async_consume_coordinator_failover,
    async_consume_coordinator_failover,
    ClusterConfig::kraft_dedicated(3, 1).set_server_properties(plaintext_consumer_test_defaults())
);

/// The cluster of [`test_async_consumer_close_on_broker_shutdown`]: the class
/// defaults merged with the method's `serverProperties`, one broker and an
/// isolated controller.
fn close_on_broker_shutdown_cluster() -> ClusterConfig {
    let mut props = plaintext_consumer_test_defaults();
    props.extend([
        ("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS".to_string(), "1".to_string()),
        ("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR".to_string(), "1".to_string()),
        ("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR".to_string(), "1".to_string()),
        ("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR".to_string(), "1".to_string()),
    ]);
    ClusterConfig::kraft_dedicated(1, 1).set_server_properties(props)
}

/// Translates `PlaintextConsumerTest#testAsyncConsumerCloseOnBrokerShutdown` (body
/// `PlaintextConsumerTest#testConsumerCloseOnBrokerShutdown`) on its
/// `@ClusterTest(brokers = 1, ...)` shape — the class defaults merged with the
/// method's `serverProperties` (offsets topic 1 partition / RF 1, transaction log
/// RF 1 / min ISR 1) — as a dedicated `Type.KRAFT` cluster with an isolated
/// controller.
///
/// After the consumer has polled once, the only broker is shut down; one more
/// poll must not fail, and `close()` must return within 5 s instead of
/// retrying the coordinator lookup for its full default timeout. Auto-commit
/// is disabled so that commitSync() does not block the close timeout.
async fn async_consumer_close_on_broker_shutdown<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let backend = factory.name();
    let topic = ctx.topic("topic");
    // `make_consumer` already sets `enable.auto.commit=false`, as Java's config does.
    let mut consumer = make_consumer(ctx, factory, &ctx.group_id("close_on_broker_shutdown"), &[]).await;
    consumer
        .subscribe_with_topics(vec![topic])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: subscribe should succeed: {e}"));

    // Force consumer to discover coordinator by doing a poll
    // This ensures coordinator is discovered before we shutdown the broker
    consumer
        .poll(Duration::from_millis(100))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: poll should succeed: {e}"));

    // Now shutdown broker.
    assert_eq!(1, ctx.cluster().broker_ids().len(), "broker count ({backend} backend)");
    let broker = *ctx.cluster().broker_ids().first().expect("one broker");
    ctx.cluster().shutdown_broker(broker).await;

    // Do another poll to force the consumer to retry finding the coordinator.
    consumer
        .poll(Duration::from_millis(100))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: poll after broker shutdown should not fail: {e}"));

    // Close should not hang waiting for retries when broker is already down
    // (Java: `assertTimeoutPreemptively(Duration.ofSeconds(5), () -> consumer.close(), ...)`).
    tokio::time::timeout(Duration::from_secs(5), consumer.close())
        .await
        .unwrap_or_else(|_| {
            panic!("{backend} backend: Consumer close should not wait for full timeout when broker is already shutdown")
        })
        .unwrap_or_else(|e| panic!("{backend} backend: consumer close should succeed: {e}"));
}

multilanguage_consumer_test!(
    test_async_consumer_close_on_broker_shutdown,
    async_consumer_close_on_broker_shutdown,
    close_on_broker_shutdown_cluster()
);

// ── Local utilities ────────────────────────────────────────────────────

/// Polls until at least `at_least` records have been collected OR the
/// deadline elapses, returning the total count. Mirrors Java's
/// `consumer.poll(Duration).count()` summed across the budget, with a
/// short settle for the "expect 0" case.
async fn poll_count(consumer: &mut BytesConsumer, backend: &str, at_least: usize, budget: Duration) -> usize {
    let deadline = Instant::now() + budget;
    let mut count = 0usize;
    while Instant::now() < deadline {
        let records = consumer
            .poll(Duration::from_millis(200))
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: poll: {e}"));
        count += records.into_iter().count();
        if at_least > 0 && count >= at_least {
            break;
        }
    }
    count
}

/// Translates Java's `testAsyncConsumerUnsubscribeDoesNotCommitOffsetsWithAutoCommitEnabled`
/// (KAFKA-20119, 40e9fcd742). The `testClassicConsumer...` twin is out of scope
/// (classic protocol, consumer-threading.md §20).
///
/// `unsubscribe()` does not commit offsets even with `enable.auto.commit`
/// enabled: a second consumer in the same group sees no committed offset after
/// the first one consumed records and unsubscribed.
async fn async_consumer_unsubscribe_does_not_commit_offsets_with_auto_commit_enabled<F: ConsumerBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
) {
    let backend = factory.name();
    let num_records = 10;
    let topic = ctx.topic("topic");
    let tp = TopicPartition::new(topic.clone(), 0);
    let group_id = ctx.group_id("unsubscribe-no-commit-test");
    let overrides = [("enable.auto.commit", "true")];

    let producer = build_producer(ctx);
    // Consumer 1: subscribe, consume records, then unsubscribe (without an
    // explicit commit).
    let mut consumer1 = make_consumer(ctx, factory, &group_id, &overrides).await;
    create_topic(consumer1.as_mut(), backend, &topic, 1).await;
    send_records(&producer, &tp, num_records, current_time_ms()).await;
    producer.close().await.expect("producer close");

    consumer1
        .subscribe_with_topics(vec![topic.clone()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: subscribe: {e}"));
    consume_records(consumer1.as_mut(), backend, num_records).await;
    // Unsubscribe - this should NOT commit offsets even though auto-commit is
    // enabled.
    consumer1
        .unsubscribe()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: unsubscribe: {e}"));
    consumer1
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer1 close: {e}"));

    // Consumer 2: use the same group id to check the committed offsets.
    let mut consumer2 = make_consumer(ctx, factory, &group_id, &overrides).await;
    consumer2
        .subscribe_with_topics(vec![topic.clone()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: subscribe: {e}"));
    let committed = consumer2
        .committed(std::slice::from_ref(&tp))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: committed: {e}"));
    assert_eq!(
        Some(&None),
        committed.get(&tp),
        "unsubscribe() should not commit offsets even when auto-commit is enabled ({backend} backend)"
    );
    consumer2
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer2 close: {e}"));
}

multilanguage_consumer_test!(
    test_async_consumer_unsubscribe_does_not_commit_offsets_with_auto_commit_enabled,
    async_consumer_unsubscribe_does_not_commit_offsets_with_auto_commit_enabled,
    cluster_config_kip848()
);

/// Translates Java's `PlaintextConsumerTest#testAsyncStaticMemberCloseWithLeaveGroupTriggersRebalance`
/// (added by 45c4bdc48a).
///
/// Tests that when a static member closes with
/// [`GroupMembershipOperation::LeaveGroup`], the other members in the group
/// receive a rebalance callback. This is in contrast to the default behavior
/// where static members remain in the group on close (no rebalance triggered).
/// Both group members are subjects, so both come from the backend factory,
/// each with its own logging listener.
async fn async_static_member_close_with_leave_group_triggers_rebalance<F: ConsumerBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
) {
    let backend = factory.name();
    let topic_name = ctx.topic("test-static-member-leave-group");
    let group_id = ctx.group_id("test-group");
    create_test_topic(ctx, &topic_name, 2, 1).await;

    let (mut consumer1, log1) =
        make_consumer_with_log(ctx, factory, &group_id, &[("group.instance.id", "instance-1")]).await;
    let (mut consumer2, log2) =
        make_consumer_with_log(ctx, factory, &group_id, &[("group.instance.id", "instance-2")]).await;

    log1.subscribe_with_logging_listener(&mut consumer1, vec![topic_name.clone()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer1 subscribe: {e}"));
    log2.subscribe_with_logging_listener(&mut consumer2, vec![topic_name.clone()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer2 subscribe: {e}"));

    await_rebalance(consumer1.as_mut(), backend, &log1).await;
    await_rebalance(consumer2.as_mut(), backend, &log2).await;

    let initial_assigned_calls = listener_calls(&log2, backend, &[KIND_ASSIGNED]).await;

    // Consumer 1 closes with LEAVE_GROUP - this should trigger a rebalance
    consumer1
        .close_with_options(CloseOptions::with_operation(GroupMembershipOperation::LeaveGroup))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer1 close with LEAVE_GROUP: {e}"));
    await_rebalance(consumer2.as_mut(), backend, &log2).await;

    // Consumer 2 should have received another assignment callback due to the rebalance
    let current = listener_calls(&log2, backend, &[KIND_ASSIGNED]).await;
    assert!(
        current > initial_assigned_calls,
        "Consumer 2 should have received a rebalance after static consumer 1 left the group permanently. \
         Initial assigned calls: {initial_assigned_calls}, current: {current} ({backend} backend)"
    );

    consumer2
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer2 close: {e}"));
}

multilanguage_consumer_test!(
    test_async_static_member_close_with_leave_group_triggers_rebalance,
    async_static_member_close_with_leave_group_triggers_rebalance,
    cluster_config_kip848()
);

// ── Group consumption on both assignment intervals (KIP-1263, KAFKA-20291) ──

/// Translates `PlaintextConsumerTest#testAsyncConsumerGroupConsumption` (body
/// `PlaintextConsumerTest#testGroupConsumption`): 10 records to `tp`, a
/// subscription, and the first record consumed and verified.
async fn async_consumer_group_consumption<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let backend = factory.name();
    let topic = ctx.topic("topic");
    let tp = TopicPartition::new(topic.clone(), 0);
    let group_id = ctx.group_id("g_group_consumption");
    // Empty topic so the records start at offset 0, as the class's other tests
    // create it (`createTopic(TOPIC, 2, BROKER_COUNT)`).
    create_test_topic(ctx, &topic, 2, BROKER_COUNT).await;

    let producer = build_producer(ctx);
    // `cluster.consumer(...)` keeps Java's default `enable.auto.commit=true`.
    let mut consumer = make_consumer(ctx, factory, &group_id, &[("enable.auto.commit", "true")]).await;
    let starting_timestamp = current_time_ms();
    send_records(&producer, &tp, 10, starting_timestamp).await;
    producer.close().await.expect("producer close");

    consumer
        .subscribe_with_topics(vec![topic.clone()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: subscribe: {e}"));
    consume_and_verify_records(consumer.as_mut(), backend, &tp, 1, 0, 0, starting_timestamp).await;

    consumer
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer close: {e}"));
}

assignment_interval_consumer_test!(
    test_async_consumer_group_consumption,
    async_consumer_group_consumption,
    cluster_config_kip848()
);

/// Translates `PlaintextConsumerTest#testAsyncConsumerGroupConsumptionWithTwoMembers`
/// (body `PlaintextConsumerTest#testGroupConsumptionWithTwoMembers`). Both
/// group members are subjects, so both come from the backend factory.
///
/// `consumer1` alone gets both partitions of `foo` and consumes 10 records from
/// each; once `consumer2` joins, each member owns exactly one partition, and each
/// consumes the next 10 records of its own partition.
async fn async_consumer_group_consumption_with_two_members<F: ConsumerBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
) {
    let backend = factory.name();
    // Java's fixed `foo` and `group_two_members`, namespaced per test run.
    let foo_topic = ctx.topic("foo");
    let foo0 = TopicPartition::new(foo_topic.clone(), 0);
    let foo1 = TopicPartition::new(foo_topic.clone(), 1);
    create_test_topic(ctx, &foo_topic, 2, BROKER_COUNT).await;
    let group_id = ctx.group_id("group_two_members");

    let producer = build_producer(ctx);
    // `cluster.consumer(...)` keeps Java's default `enable.auto.commit=true`:
    // the revoked partition's position is committed, so the member that gets
    // it next starts at offset 10.
    let overrides = [("enable.auto.commit", "true")];
    let mut consumer1 = make_consumer(ctx, factory, &group_id, &overrides).await;
    let mut consumer2 = make_consumer(ctx, factory, &group_id, &overrides).await;
    let starting_timestamp = current_time_ms();

    consumer1
        .subscribe_with_topics(vec![foo_topic.clone()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer1 subscribe: {e}"));
    await_assignment(
        consumer1.as_mut(),
        backend,
        &HashSet::from([foo0.clone(), foo1.clone()]),
        DEFAULT_MAX_WAIT,
    )
    .await;

    send_records(&producer, &foo0, 10, starting_timestamp).await;
    consume_and_verify_records(consumer1.as_mut(), backend, &foo0, 10, 0, 0, starting_timestamp).await;

    send_records(&producer, &foo1, 10, starting_timestamp).await;
    consume_and_verify_records(consumer1.as_mut(), backend, &foo1, 10, 0, 0, starting_timestamp).await;

    consumer2
        .subscribe_with_topics(vec![foo_topic.clone()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer2 subscribe: {e}"));
    // Java: `TestUtils.waitForCondition(() -> { consumer1.poll(100ms);
    // consumer2.poll(100ms); return both assignments have size 1; },
    // "Timed out waiting for rebalance to complete")`, 15 s.
    let deadline = Instant::now() + DEFAULT_MAX_WAIT;
    loop {
        consumer1
            .poll(Duration::from_millis(100))
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: consumer1 poll: {e}"));
        consumer2
            .poll(Duration::from_millis(100))
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: consumer2 poll: {e}"));
        if consumer1.assignment().len() == 1 && consumer2.assignment().len() == 1 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "Timed out waiting for rebalance to complete ({backend} backend)"
        );
    }

    let assignment1 = consumer1.assignment();
    let assignment2 = consumer2.assignment();
    assert!(
        assignment1.contains(&foo0) || assignment1.contains(&foo1),
        "consumer1 assignment {assignment1:?} ({backend} backend)"
    );
    assert!(
        assignment2.contains(&foo0) || assignment2.contains(&foo1),
        "consumer2 assignment {assignment2:?} ({backend} backend)"
    );
    assert_ne!(assignment1, assignment2, "the two members' assignments ({backend} backend)");

    send_records(&producer, &foo0, 10, starting_timestamp).await;
    send_records(&producer, &foo1, 10, starting_timestamp).await;
    producer.close().await.expect("producer close");
    let tp1 = assignment1.into_iter().next().expect("consumer1 has one partition");
    let tp2 = assignment2.into_iter().next().expect("consumer2 has one partition");
    consume_and_verify_records(consumer1.as_mut(), backend, &tp1, 10, 10, 0, starting_timestamp).await;
    consume_and_verify_records(consumer2.as_mut(), backend, &tp2, 10, 10, 0, starting_timestamp).await;

    consumer1
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer1 close: {e}"));
    consumer2
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer2 close: {e}"));
}

assignment_interval_consumer_test!(
    test_async_consumer_group_consumption_with_two_members,
    async_consumer_group_consumption_with_two_members,
    cluster_config_kip848()
);

/// Translates `PlaintextConsumerTest#testAsyncConsumerStaticConsumerDetectsNewPartitionCreatedAfterRestart`
/// (body `PlaintextConsumerTest#testStaticConsumerDetectsNewPartitionCreatedAfterRestart`).
/// Both static members (one `group.instance.id`) are subjects, so both come
/// from the backend factory.
///
/// `consumer1` gets `foo-0` and closes; `consumer2`, the same static member
/// restarted, gets `foo-0`; an admin then grows `foo` to 2 partitions and
/// `consumer2`'s assignment grows to both.
async fn async_consumer_static_consumer_detects_new_partition_created_after_restart<F: ConsumerBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
) {
    use confluent_kafka::admin::{Admin, AdminClient, AdminClientConfig, NewPartitions};

    let backend = factory.name();
    // Java's fixed `foo` and `my-group-id`, namespaced per test run.
    let foo_topic = ctx.topic("foo");
    let foo0 = TopicPartition::new(foo_topic.clone(), 0);
    let foo1 = TopicPartition::new(foo_topic.clone(), 1);
    create_test_topic(ctx, &foo_topic, 1, 1).await;
    let group_id = ctx.group_id("my-group-id");
    let overrides = [
        ("group.instance.id", "my-instance-id"),
        ("metadata.max.age.ms", "100"),
        ("max.poll.interval.ms", "6000"),
        // `cluster.consumer(...)` keeps Java's default `enable.auto.commit=true`.
        ("enable.auto.commit", "true"),
    ];

    let mut consumer1 = make_consumer(ctx, factory, &group_id, &overrides).await;
    let mut consumer2 = make_consumer(ctx, factory, &group_id, &overrides).await;
    let mut props = HashMap::from([("client.id".to_string(), "plaintext-consumer-test-admin".to_string())]);
    ctx.configure(&mut props);
    let admin: Box<dyn Admin> =
        AdminClient::create(AdminClientConfig::new(&props).expect("valid admin config")).expect("admin client");

    consumer1
        .subscribe_with_topics(vec![foo_topic.clone()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer1 subscribe: {e}"));
    await_assignment(consumer1.as_mut(), backend, &HashSet::from([foo0.clone()]), DEFAULT_MAX_WAIT).await;
    consumer1
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer1 close: {e}"));

    consumer2
        .subscribe_with_topics(vec![foo_topic.clone()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer2 subscribe: {e}"));
    await_assignment(consumer2.as_mut(), backend, &HashSet::from([foo0.clone()]), DEFAULT_MAX_WAIT).await;

    admin
        .create_partitions(&HashMap::from([(foo_topic.clone(), NewPartitions::increase_to(2))]))
        .all()
        .get()
        .await
        .expect("create_partitions should succeed");
    await_assignment(
        consumer2.as_mut(),
        backend,
        &HashSet::from([foo0.clone(), foo1.clone()]),
        DEFAULT_MAX_WAIT,
    )
    .await;

    consumer2
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer2 close: {e}"));
    admin.close_with_timeout(Duration::from_secs(5)).await;
}

assignment_interval_consumer_test!(
    test_async_consumer_static_consumer_detects_new_partition_created_after_restart,
    async_consumer_static_consumer_detects_new_partition_created_after_restart,
    cluster_config_kip848()
);
