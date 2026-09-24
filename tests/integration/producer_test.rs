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

//! Integration tests for the KafkaProducer against a real Kafka 4.2.0 broker.
//!
//! Translated from:
//! - `org.apache.kafka.clients.producer.ProducerCompressionTest`: `testCompression`
//!   (native only)
//! - `org.apache.kafka.clients.producer.ProducerFailureHandlingTest`: among
//!   others `testTooLargeRecordWithAckZero`,
//!   `testPartitionTooLargeForReplicationWithAckAll`,
//!   `testResponseTooLargeForReplicationWithAckAll`,
//!   `testCannotSendToInternalTopic`
//! - `kafka.api.BaseProducerSendTest` / `kafka.api.PlaintextProducerSendTest`
//!   (`core/src/test/scala/integration/kafka/api/`): `testSendOffset`,
//!   `testSendToPartition`, `testSendBeforeAndAfterPartitionExpansion`,
//!   `testBatchSizeZero`, `testBatchSizeZeroNoPartitionNoRecordKey`,
//!   `testCloseWithZeroTimeoutFromSenderThread` (native only),
//!   `testCloseWithZeroTimeoutFromCallerThread` (native only),
//!   `testFlush` (as `test_flush_sends_pending_records`; not-done half native only),
//!   `testWrongSerializer` (native only),
//!   `testSendCompressedMessageWithCreateTime` (native only),
//!   `testSendNonCompressedMessageWithCreateTime` (native only),
//!   `testSendCompressedMessageWithLogAppendTime` (native only),
//!   `testSendNonCompressedMessageWithLogAppendTime` (native only),
//!   `testSendWithInvalidBeforeAndAfterTimestamp`,
//!   `testValidBeforeAndAfterTimestampsAtThreshold`,
//!   `testValidBeforeAndAfterTimestampsWithinThreshold` (each of the last three
//!   runs both `timestampConfigProvider` cases in one test),
//!   `testNonBlockingProducer` (buffer-exhaustion half native only),
//!   `testSendRecordBatchWithMaxRequestSizeAndHigher`,
//!   `testPartitionsForTimeoutErrorWhenTopicDoesNotExist`. The exact
//!   `waitOnMetadata` messages that `testSendTimeoutErrorMessageWhenTopicDoesNotExist`
//!   and `testSendTimeoutErrorWhenPartitionDoesNotExist` pin are asserted by
//!   `test_produce_to_non_existent_topic` / `test_produce_invalid_partition`.
//!
//! When the `multilanguage-tests` feature is enabled, each test is
//! instantiated three times via [`multilanguage_test!`] — once per
//! backend (rust / python / c). Test bodies are generic over
//! [`ProducerBackendFactory`] so the same scenario exercises the native
//! Rust producer, the Python binding, and the C binding through their
//! gRPC server containers.
//!
//! See `design/history/MILESTONE-6/DESIGN-multilanguage-tests.md`.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use confluent_kafka::admin::Admin;
use confluent_kafka::admin::AdminClientConfig;
use confluent_kafka::admin::KafkaAdminClient;
use confluent_kafka::admin::NewPartitions;
use confluent_kafka::admin::NewTopic;
use confluent_kafka::common::Error;
use confluent_kafka::common::KafkaFuture;
use confluent_kafka::common::TopicPartition;
use confluent_kafka::common::header::Headers;
use confluent_kafka::common::header::RecordHeader;
use confluent_kafka::common::header::RecordHeaders;
use confluent_kafka::common::record::TimestampType;
use confluent_kafka::common::serialization::ByteArrayDeserializer;
use confluent_kafka::consumer::Consumer;
use confluent_kafka::consumer::ConsumerConfig;
use confluent_kafka::consumer::KafkaConsumer;
use confluent_kafka::producer::Callback;
use confluent_kafka::producer::Producer;
use confluent_kafka::producer::ProducerRecord;
use confluent_kafka::producer::ProducerRecordOptionsBuilder;
use confluent_kafka::producer::RecordMetadata;

use crate::common::backend_factory::ProducerBackendFactory;
use crate::common::callback_log::KIND_DELIVERY;
use crate::common::cluster_config::ClusterConfig;
use crate::common::test_context::TestContext;
use crate::common::test_utils;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a baseline producer config with sensible test defaults.
fn make_config(bootstrap_servers: &str) -> HashMap<String, String> {
    HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap_servers.to_string()),
        ("client.id".to_string(), "integration-test-producer".to_string()),
        // Use acks=all for reliability in integration tests.
        ("acks".to_string(), "all".to_string()),
        // Use a short max_block_ms so tests don't hang.
        ("max.block.ms".to_string(), "30000".to_string()),
        // Short linger to avoid waiting.
        ("linger.ms".to_string(), "0".to_string()),
    ])
}

/// Pick the bootstrap address the factory's backend can actually reach.
/// gRPC backends run in containers and need the broker's CONTAINER
/// listener; native rust uses the host loopback.
fn bootstrap_for<F: ProducerBackendFactory>(factory: &F, ctx: &TestContext) -> String {
    if factory.needs_container_bootstrap() {
        ctx.container_bootstrap_servers().to_string()
    } else {
        ctx.bootstrap_servers().to_string()
    }
}

fn b(s: &str) -> Vec<u8> {
    s.as_bytes().to_vec()
}

// ---------------------------------------------------------------------------
// Test bodies — each is generic over ProducerBackendFactory
// ---------------------------------------------------------------------------

/// Test: Create a producer, send a single record with key and value, verify
/// the future completes successfully with valid RecordMetadata.
async fn produce_single_record_inner<F: ProducerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let topic = ctx.topic("single_record");
    let producer = factory
        .create(make_config(&bootstrap_for(factory, ctx)))
        .await
        .expect("Failed to create producer");

    let record = ProducerRecord::with_key(topic.clone(), Some(b("test-key")), Some(b("test-value")));
    let future = producer.send(record).await.expect("send should succeed");

    let metadata = future
        .get_with_timeout(Duration::from_secs(30))
        .await
        .expect("produce should succeed");

    assert!(
        metadata.offset() >= 0,
        "Offset should be non-negative, got: {}",
        metadata.offset()
    );
    assert_eq!(metadata.topic(), topic, "Topic should match");
    assert!(metadata.partition() >= 0, "Partition should be non-negative");

    producer.close().await.expect("close should succeed");
}

/// Test: Send records with a specific key, verify they go to the same
/// partition.
async fn produce_with_key_inner<F: ProducerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let topic = ctx.topic("with_key");
    let producer = factory
        .create(make_config(&bootstrap_for(factory, ctx)))
        .await
        .expect("Failed to create producer");

    let key = b("deterministic-key");
    let mut partitions = Vec::new();

    for i in 0..5 {
        let record = ProducerRecord::with_key(topic.clone(), Some(key.clone()), Some(b(&format!("value-{i}"))));
        let future = producer.send(record).await.expect("send should succeed");
        let metadata = future
            .get_with_timeout(Duration::from_secs(30))
            .await
            .expect("produce should succeed");
        partitions.push(metadata.partition());
    }

    let first_partition = partitions[0];
    for (i, &p) in partitions.iter().enumerate() {
        assert_eq!(
            p, first_partition,
            "Record {i} went to partition {p} but expected {first_partition} (same key should mean same partition)"
        );
    }

    producer.close().await.expect("close should succeed");
}

// ---------------------------------------------------------------------------
// BaseProducerSendTest / PlaintextProducerSendTest fixtures
// ---------------------------------------------------------------------------

/// `BaseProducerSendTest.numRecords` (`BaseProducerSendTest.scala:73`).
const NUM_RECORDS: usize = 100;

/// Bound substituted for Java's unbounded `Future.get()`, so a regression fails
/// the test instead of hanging the whole binary.
const UNBOUNDED_GET_TIMEOUT: Duration = Duration::from_secs(120);

/// Broker shape of `BaseProducerSendTest.generateConfigs` / `brokerOverrides`
/// (`BaseProducerSendTest.scala:50-65`): two brokers, `num.partitions=4`, and
/// `offsets.topic.replication.factor=2` (the harness already sets it to
/// `min(brokers, 3)` = 2, see `kafka_cluster.rs`).
fn producer_send_cluster_config() -> ClusterConfig {
    let mut cfg = ClusterConfig::with_brokers(2);
    cfg.server_properties
        .insert("KAFKA_NUM_PARTITIONS".to_string(), "4".to_string());
    cfg
}

/// The parameters of `BaseProducerSendTest.createProducer`
/// (`BaseProducerSendTest.scala:103-108`), with the same defaults.
struct SendTestProducerOpts {
    linger_ms: i64,
    delivery_timeout_ms: i64,
    batch_size: i32,
    compression_type: &'static str,
    max_block_ms: i64,
    buffer_size: i64,
}

impl Default for SendTestProducerOpts {
    fn default() -> Self {
        Self {
            linger_ms: 0,
            delivery_timeout_ms: 2 * 60 * 1000,
            batch_size: 16384,
            compression_type: "none",
            max_block_ms: 60 * 1000,
            buffer_size: 1024 * 1024,
        }
    }
}

/// Java's `Int.MaxValue`, as passed for `lingerMs` / `deliveryTimeoutMs`.
const INT_MAX_VALUE: i64 = i32::MAX as i64;

/// The producer config `BaseProducerSendTest.createProducer` builds through
/// `TestUtils.createProducer` (`TestUtils.scala:516-545`), including the
/// defaults `createProducer` does not override: `acks=-1`,
/// `retries=Int.MaxValue`, `request.timeout.ms=20000`,
/// `enable.idempotence=false`.
fn send_test_producer_config(bootstrap_servers: &str, opts: &SendTestProducerOpts) -> HashMap<String, String> {
    HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap_servers.to_string()),
        ("acks".to_string(), "-1".to_string()),
        ("max.block.ms".to_string(), opts.max_block_ms.to_string()),
        ("buffer.memory".to_string(), opts.buffer_size.to_string()),
        ("retries".to_string(), i32::MAX.to_string()),
        ("delivery.timeout.ms".to_string(), opts.delivery_timeout_ms.to_string()),
        ("request.timeout.ms".to_string(), "20000".to_string()),
        ("linger.ms".to_string(), opts.linger_ms.to_string()),
        ("batch.size".to_string(), opts.batch_size.to_string()),
        ("compression.type".to_string(), opts.compression_type.to_string()),
        ("enable.idempotence".to_string(), "false".to_string()),
    ])
}

/// Admin client for topic provisioning — the suite's `admin`
/// (`BaseProducerSendTest.scala:78`). Always native: it is harness
/// infrastructure, not the client under test.
fn send_test_admin(ctx: &TestContext) -> Box<dyn Admin> {
    let props = HashMap::from([
        ("bootstrap.servers".to_string(), ctx.bootstrap_servers().to_string()),
        ("client.id".to_string(), "producer-send-test-admin".to_string()),
        ("request.timeout.ms".to_string(), "30000".to_string()),
        ("default.api.timeout.ms".to_string(), "30000".to_string()),
    ]);
    let config = AdminClientConfig::new(&props).expect("valid admin config");
    Box::new(KafkaAdminClient::new(config).expect("admin client"))
}

/// `TestUtils.createTopicWithAdmin` plus a wait until every partition's leader
/// serves requests for it.
///
/// Java's `createTopicWithAdmin` returns only after `waitForAllPartitionsMetadata`
/// sees the topic in **every** broker's metadata cache (`TestUtils.scala:832-853`),
/// so the leader has applied its leadership before the first produce.
/// [`test_utils::create_topic`] can only prove that one broker knows the topic;
/// on this two-broker cluster the first produce requests then intermittently
/// get `NOT_LEADER_OR_FOLLOWER`, and since these producers are non-idempotent
/// with 5 in-flight requests, the retries reorder records and break the
/// consecutive-offset assertions (observed: offsets `[7, 8, 5, 6, 3, 4, 1, 2, 0, 9, ..]`).
/// See [`test_utils::wait_for_partition_leaders`] for how the missing half is recovered.
async fn create_topic_with_admin(admin: &dyn Admin, topic: &str, num_partitions: i32, replication_factor: i16) {
    create_topic_with_admin_config(admin, topic, num_partitions, replication_factor, BTreeMap::new()).await;
}

/// [`create_topic_with_admin`] with the `topicConfig` argument of Java's
/// `TestUtils.createTopicWithAdmin`.
async fn create_topic_with_admin_config(
    admin: &dyn Admin,
    topic: &str,
    num_partitions: i32,
    replication_factor: i16,
    topic_config: BTreeMap<String, String>,
) {
    test_utils::create_topic_with_configs(admin, topic, num_partitions, replication_factor, topic_config).await;
    test_utils::wait_for_partition_leaders(admin, topic, 0..num_partitions).await;
}

/// The suite's verification consumer — `TestUtils.createConsumer(bootstrap,
/// groupProtocol)` (`BaseProducerSendTest.scala:86-90`, `TestUtils.scala:551`):
/// `auto.offset.reset=earliest`, `enable.auto.commit=true`, CONSUMER (KIP-848)
/// arm only. Always native, like the admin client.
///
/// Deviation: Java's fixed group id `"group"` becomes a per-test id, because
/// clusters are pooled across tests here and Java's are not.
fn send_test_consumer(ctx: &TestContext) -> Box<dyn Consumer<Vec<u8>, Vec<u8>>> {
    let props = HashMap::from([
        ("bootstrap.servers".to_string(), ctx.bootstrap_servers().to_string()),
        ("group.protocol".to_string(), "consumer".to_string()),
        ("group.id".to_string(), ctx.group_id("group")),
        ("auto.offset.reset".to_string(), "earliest".to_string()),
        ("enable.auto.commit".to_string(), "true".to_string()),
    ]);
    KafkaConsumer::new::<Vec<u8>, Vec<u8>>(
        ConsumerConfig::new(&props).expect("valid consumer config"),
        Box::new(ByteArrayDeserializer::new()),
        Box::new(ByteArrayDeserializer::new()),
    )
    .expect("KafkaConsumer::new should succeed")
}

/// The fields of a consumed record the suite asserts on (`ConsumerRecord` is
/// not `Clone`).
struct ConsumedRecord {
    topic: String,
    partition: i32,
    offset: i64,
    timestamp: i64,
    key: Option<Vec<u8>>,
    value: Option<Vec<u8>>,
    headers: Vec<RecordHeader>,
}

/// `TestUtils.pollUntilAtLeastNumRecords` (`TestUtils.scala:1184-1196`) over
/// `pollRecordsUntilTrue` (`:709-718`): 100 ms polls, 15 s budget
/// (`DEFAULT_MAX_WAIT_MS`), same failure message.
async fn poll_until_at_least_num_records(
    consumer: &mut dyn Consumer<Vec<u8>, Vec<u8>>,
    num_records: usize,
) -> Vec<ConsumedRecord> {
    let deadline = Instant::now() + Duration::from_millis(15_000);
    let mut records = Vec::new();
    loop {
        let polled = consumer.poll(Duration::from_millis(100)).await.expect("poll should succeed");
        for record in polled {
            records.push(ConsumedRecord {
                topic: record.topic().to_string(),
                partition: record.partition(),
                offset: record.offset(),
                timestamp: record.timestamp(),
                key: record.key().cloned(),
                value: record.value().cloned(),
                headers: record.headers().to_array().to_vec(),
            });
        }
        if records.len() >= num_records {
            return records;
        }
        assert!(
            Instant::now() < deadline,
            "Consumed {} records before timeout instead of the expected {num_records} records",
            records.len()
        );
    }
}

/// `TestUtils.consumeRecords` (`TestUtils.scala:1198-1204`).
async fn consume_records(consumer: &mut dyn Consumer<Vec<u8>, Vec<u8>>, num_records: usize) -> Vec<ConsumedRecord> {
    let records = poll_until_at_least_num_records(consumer, num_records).await;
    assert_eq!(num_records, records.len(), "Consumed more records than expected");
    records
}

/// Java's `System.currentTimeMillis()`.
fn current_time_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after epoch")
        .as_millis() as i64
}

/// State of the Scala `object callback` in `testSendOffset`
/// (`BaseProducerSendTest.scala:140-160`).
///
/// Java asserts inside `onCompletion`, which runs on the producer's I/O thread,
/// where a failed assertion is caught and logged by the producer rather than
/// failing the test. Here each would-be assertion failure is recorded and
/// checked on the test task, so the callback checks actually bind.
#[derive(Default)]
struct SendOffsetCallbackState {
    offset: i64,
    invocations: usize,
    failures: Vec<String>,
}

/// `check_serialized_sizes` gates the size assertions (and the null-value
/// check derived from them) to backends that report real serialized sizes; see
/// [`send_offset_inner`] for the binding gap.
fn send_offset_callback(
    state: Arc<Mutex<SendOffsetCallbackState>>,
    topic: String,
    partition: i32,
    check_serialized_sizes: bool,
) -> Callback {
    Box::new(move |metadata, error| {
        let mut st = state.lock().unwrap();
        st.invocations += 1;
        // Java: `if (exception == null) ... else fail(...)`.
        if let Some(e) = error {
            st.failures
                .push(format!("Send callback returns the following exception: {e:?}"));
            return;
        }
        let Some(m) = metadata else {
            st.failures
                .push("Send callback invoked with neither metadata nor error".to_string());
            return;
        };
        let offset = st.offset;
        if m.offset() != offset {
            st.failures.push(format!("expected offset {offset}, got {}", m.offset()));
        }
        if m.topic() != topic {
            st.failures.push(format!("expected topic {topic}, got {}", m.topic()));
        }
        if m.partition() != partition {
            st.failures
                .push(format!("expected partition {partition}, got {}", m.partition()));
        }
        let key_len = "key".len() as i32;
        let value_len = "value".len() as i32;
        let size_ok = match offset {
            0 => m.serialized_key_size() + m.serialized_value_size() == key_len + value_len,
            // Java checks only the key size here. The value size is also checked
            // (-1 means a null value) because it is the only observable proof that
            // record1 really was sent with a null value, not an empty one.
            1 => m.serialized_key_size() == key_len && m.serialized_value_size() == -1,
            2 => m.serialized_value_size() == value_len,
            _ => m.serialized_value_size() > 0,
        };
        if check_serialized_sizes && !size_ok {
            st.failures.push(format!(
                "offset {offset}: unexpected serialized sizes key={} value={}",
                m.serialized_key_size(),
                m.serialized_value_size()
            ));
        }
        st.offset += 1;
    })
}

/// Translated from `BaseProducerSendTest.testSendOffset`
/// (`BaseProducerSendTest.scala:127-194`).
///
/// 1. Sends with a null value, null key, or null partition are accepted.
/// 2. The last of 100 non-blocking sends reports the correct offset.
///
/// (The Scala doc also mentions a null topic being rejected; the test body has
/// no such case, and Rust's `ProducerRecord` cannot hold a null topic.)
async fn send_offset_inner<F: ProducerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let topic = ctx.topic("topic");
    let producer = factory
        .create(send_test_producer_config(
            &bootstrap_for(factory, ctx),
            &SendTestProducerOpts::default(),
        ))
        .await
        .expect("Failed to create producer");
    let partition = 0;
    let state = Arc::new(Mutex::new(SendOffsetCallbackState::default()));
    // Binding gap: both gRPC servers hardcode the serialized sizes in the
    // returned metadata to -1 (`bindings/python/grpc_translate.py`
    // `_record_metadata_to_proto`, `bindings/c/grpc_server/server.cc` ~704), and
    // the Python servers turn an absent value into `b""`
    // (`_proto_to_producer_record`), so record1 is not a null-value send there.
    // The size and null-value checks therefore run only on the native backend.
    // Everything else runs on every backend.
    let check_serialized_sizes = factory.name() == "rust";
    let callback = || {
        Some(send_offset_callback(
            state.clone(),
            topic.clone(),
            partition,
            check_serialized_sizes,
        ))
    };

    let admin = send_test_admin(ctx);
    create_topic_with_admin(admin.as_ref(), &topic, 1, 2).await;

    let record0 = || {
        ProducerRecord::with_partition_key(topic.clone(), Some(partition), Some(b("key")), Some(b("value")))
            .expect("valid record")
    };
    let send_and_get = |record: ProducerRecord<Vec<u8>, Vec<u8>>| {
        let producer = &producer;
        let callback = callback();
        async move {
            producer
                .send_with_callback(record, callback)
                .await
                .expect("send should succeed")
                .get_with_timeout(UNBOUNDED_GET_TIMEOUT)
                .await
                .expect("produce should succeed")
                .offset()
        }
    };

    // send a normal record
    assert_eq!(0, send_and_get(record0()).await, "Should have offset 0");

    // send a record with null value should be ok
    let record1 =
        ProducerRecord::with_partition_key(topic.clone(), Some(partition), Some(b("key")), None).expect("valid record");
    assert_eq!(1, send_and_get(record1).await, "Should have offset 1");

    // send a record with null key should be ok
    let record2 = ProducerRecord::with_partition_key(topic.clone(), Some(partition), None, Some(b("value")))
        .expect("valid record");
    assert_eq!(2, send_and_get(record2).await, "Should have offset 2");

    // send a record with null part id should be ok
    let record3 = ProducerRecord::with_partition_key(topic.clone(), None, Some(b("key")), Some(b("value")))
        .expect("valid record");
    assert_eq!(3, send_and_get(record3).await, "Should have offset 3");

    // non-blocking send a list of records
    for _ in 0..NUM_RECORDS {
        producer
            .send_with_callback(record0(), callback())
            .await
            .expect("send should succeed");
    }

    // check that all messages have been acked via offset
    let expected = NUM_RECORDS as i64 + 4;
    assert_eq!(expected, send_and_get(record0()).await, "Should have offset {expected}");

    producer.close().await.expect("close should succeed");

    {
        let st = state.lock().unwrap();
        assert!(st.failures.is_empty(), "callback assertions failed: {:?}", st.failures);
        assert_eq!(st.invocations, NUM_RECORDS + 5, "every send's callback must fire exactly once");
        assert_eq!(st.offset, NUM_RECORDS as i64 + 5);
    }
    admin.close_with_timeout(Duration::from_secs(5)).await;
}

/// Translated from `BaseProducerSendTest.testSendToPartition`
/// (`BaseProducerSendTest.scala:329-371`): the specified partition id is
/// respected, and the consumed records keep the partition, ordering, null key,
/// value, and explicit timestamp.
async fn send_to_partition_inner<F: ProducerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let topic = ctx.topic("topic");
    let producer = factory
        .create(send_test_producer_config(
            &bootstrap_for(factory, ctx),
            &SendTestProducerOpts::default(),
        ))
        .await
        .expect("Failed to create producer");
    let admin = send_test_admin(ctx);
    create_topic_with_admin(admin.as_ref(), &topic, 2, 2).await;
    let partition = 1;

    let now = current_time_ms();
    let mut futures = Vec::with_capacity(NUM_RECORDS);
    for i in 1..=NUM_RECORDS {
        let record = ProducerRecord::with_partition_timestamp_key(
            topic.clone(),
            Some(partition),
            Some(now),
            None,
            Some(b(&format!("value{i}"))),
        )
        .expect("valid record");
        futures.push(producer.send(record).await.expect("send should succeed"));
    }
    let mut metadatas = Vec::with_capacity(NUM_RECORDS);
    for future in &futures {
        metadatas.push(
            future
                .get_with_timeout(Duration::from_secs(30))
                .await
                .expect("produce should succeed"),
        );
    }

    // make sure all of them end up in the same partition with increasing offset values
    for (offset, metadata) in metadatas.iter().enumerate() {
        assert_eq!(offset as i64, metadata.offset());
        assert_eq!(topic, metadata.topic());
        assert_eq!(partition, metadata.partition());
    }

    let mut consumer = send_test_consumer(ctx);
    consumer
        .assign(vec![TopicPartition::new(topic.clone(), partition)])
        .await
        .expect("assign should succeed");

    // make sure the fetched messages also respect the partitioning and ordering
    let records = consume_records(consumer.as_mut(), NUM_RECORDS).await;
    for (i, record) in records.iter().enumerate() {
        assert_eq!(topic, record.topic);
        assert_eq!(partition, record.partition);
        assert_eq!(i as i64, record.offset);
        assert_eq!(None, record.key);
        assert_eq!(Some(b(&format!("value{}", i + 1))), record.value);
        assert_eq!(now, record.timestamp);
    }

    producer.close().await.expect("close should succeed");
    consumer.close().await.expect("consumer close should succeed");
    admin.close_with_timeout(Duration::from_secs(5)).await;
}

/// Sends `NUM_RECORDS` records with a null key and `value{i}` to `partition`,
/// awaiting each future for 30 s, and asserts they land at consecutive offsets
/// starting at `first_offset` — the repeated block of
/// `testSendBeforeAndAfterPartitionExpansion`.
async fn send_and_verify_partition<P: Producer<Vec<u8>, Vec<u8>>>(
    producer: &P,
    topic: &str,
    partition: i32,
    first_offset: i64,
) {
    let mut futures = Vec::with_capacity(NUM_RECORDS);
    for i in 1..=NUM_RECORDS {
        let record =
            ProducerRecord::with_partition_key(topic.to_string(), Some(partition), None, Some(b(&format!("value{i}"))))
                .expect("valid record");
        futures.push(producer.send(record).await.expect("send should succeed"));
    }
    for (i, future) in futures.iter().enumerate() {
        let metadata = future
            .get_with_timeout(Duration::from_secs(30))
            .await
            .expect("produce should succeed");
        assert_eq!(first_offset + i as i64, metadata.offset());
        assert_eq!(topic, metadata.topic());
        assert_eq!(partition, metadata.partition());
    }
}

/// Translated from `BaseProducerSendTest.testSendBeforeAndAfterPartitionExpansion`
/// (`BaseProducerSendTest.scala:426-481`).
async fn send_before_and_after_partition_expansion_inner<F: ProducerBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
) {
    let topic = ctx.topic("topic");
    let producer = factory
        .create(send_test_producer_config(
            &bootstrap_for(factory, ctx),
            &SendTestProducerOpts { max_block_ms: 5 * 1000, ..SendTestProducerOpts::default() },
        ))
        .await
        .expect("Failed to create producer");

    // create topic
    let admin = send_test_admin(ctx);
    create_topic_with_admin(admin.as_ref(), &topic, 1, 2).await;

    let partition0 = 0;
    send_and_verify_partition(&producer, &topic, partition0, 0).await;

    // Trying to send a record to a partition beyond topic's partition range before adding the partition should fail.
    let partition1 = 1;
    let record = ProducerRecord::with_partition_key(topic.clone(), Some(partition1), None, Some(b("value")))
        .expect("valid record");
    let err = producer
        .send(record)
        .await
        .expect("send should return a future (Java: the failure surfaces from get())")
        .get_with_timeout(UNBOUNDED_GET_TIMEOUT)
        .await
        .expect_err("send to a not-yet-existing partition should fail");
    assert!(matches!(err, Error::Timeout(_)), "Expected Timeout, got: {err:?}");
    // Java asserts only the class; the message is `KafkaProducer.waitOnMetadata`'s
    // (`KafkaProducer.java`, "Partition %d of topic %s with partition count %d ...").
    let expected_message = format!(
        "Partition {partition1} of topic {topic} with partition count 1 is not present in metadata after 5000 ms."
    );
    assert_eq!(err.message(), expected_message);

    admin
        .create_partitions(&HashMap::from([(topic.clone(), NewPartitions::increase_to(2))]))
        .all()
        .get()
        .await
        .expect("create partitions");

    // read metadata from a broker and verify the new topic partitions exist
    test_utils::wait_for_all_partitions_metadata(admin.as_ref(), &topic, 2).await;
    test_utils::wait_for_partition_leaders(admin.as_ref(), &topic, 0..2).await;

    // send records to the newly added partition after confirming that metadata have been updated.
    send_and_verify_partition(&producer, &topic, partition1, 0).await;

    // make sure all of them end up in the same partition with increasing offset values starting where previous
    send_and_verify_partition(&producer, &topic, partition0, NUM_RECORDS as i64).await;

    // Java leaves the close to `tearDown` (`BaseProducerSendTest.scala:97`).
    producer.close().await.expect("close should succeed");
    admin.close_with_timeout(Duration::from_secs(5)).await;
}

/// Test: Verify each compression type produces successfully.
async fn produce_with_compression_inner<F: ProducerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let bootstrap = bootstrap_for(factory, ctx);

    for name in ["none", "gzip", "snappy", "lz4", "zstd"] {
        let topic = ctx.topic(&format!("compress_{name}"));

        let mut config = make_config(&bootstrap);
        config.insert("compression.type".to_string(), name.to_string());
        let producer = factory.create(config).await.expect("Failed to create producer");

        let record =
            ProducerRecord::with_key(topic.clone(), Some(b("key")), Some(b(&format!("value-compressed-with-{name}"))));
        let future = producer.send(record).await.expect("send should succeed");

        let metadata = future
            .get_with_timeout(Duration::from_secs(30))
            .await
            .unwrap_or_else(|e| panic!("produce with compression '{name}' should succeed, got: {e:?}"));

        assert!(
            metadata.offset() >= 0,
            "Offset for compression '{name}' should be non-negative, got: {}",
            metadata.offset()
        );
        assert_eq!(metadata.topic(), topic, "Topic should match for compression '{name}'");

        producer.close().await.expect("close should succeed");
    }
}

// ---------------------------------------------------------------------------
// ProducerCompressionTest
// ---------------------------------------------------------------------------

/// `ProducerCompressionTest.numRecords` (`ProducerCompressionTest.java:56`).
const COMPRESSION_NUM_RECORDS: usize = 2000;

/// Java's `org.apache.kafka.test.TestUtils.LETTERS_AND_DIGITS`.
const LETTERS_AND_DIGITS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

/// `ProducerCompressionTest.messageValue` (`ProducerCompressionTest.java:165-172`):
/// `length` random letters and digits.
fn compression_message_value(length: usize) -> String {
    use rand::Rng;
    let mut rng = rand::rng();
    (0..length)
        .map(|_| LETTERS_AND_DIGITS[rng.random_range(0..LETTERS_AND_DIGITS.len())] as char)
        .collect()
}

/// `ProducerCompressionTest.errorMessage` (`ProducerCompressionTest.java:174-176`).
fn compression_error_message(compression: &str) -> String {
    format!("Compression type: {compression} - Assertion failed")
}

/// Translated from `ProducerCompressionTest.testCompression`
/// (`ProducerCompressionTest.java:63-68`): compressed messages should be able
/// to be sent and consumed correctly, for every `CompressionType`.
///
/// Native only, also under `multilanguage-tests`: the body sends 6000 records
/// per codec with `linger.ms=200` and only then waits on the futures. The gRPC
/// `Send` RPC blocks until delivery, so every record would wait out the full
/// linger on its own (~20 minutes per codec) instead of sharing batches.
async fn compression_inner<F: ProducerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    // `CompressionType.values()`, in declaration order.
    for compression in ["none", "gzip", "snappy", "lz4", "zstd"] {
        process_compression_test(ctx, factory, compression).await;
    }
}

/// `ProducerCompressionTest.processCompressionTest`
/// (`ProducerCompressionTest.java:71-114`).
///
/// Deviations: the topic name is per-test (pooled clusters), and only the
/// CONSUMER-protocol verification consumer is translated — the `classic` one
/// is out of scope (`consumer-threading.md` §20).
async fn process_compression_test<F: ProducerBackendFactory>(ctx: &mut TestContext, factory: &F, compression: &str) {
    let compression_topic = ctx.topic(&format!("topic_{compression}"));
    let admin = send_test_admin(ctx);
    test_utils::create_topic(admin.as_ref(), &compression_topic, 1, 1).await;

    // `cluster.producer(producerProps)`: bootstrap plus the three overrides.
    let producer_props = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap_for(factory, ctx)),
        ("compression.type".to_string(), compression.to_string()),
        ("batch.size".to_string(), "66000".to_string()),
        ("linger.ms".to_string(), "200".to_string()),
    ]);
    let mut consumer = send_test_consumer(ctx);
    let producer = factory.create(producer_props).await.expect("Failed to create producer");

    let partition = 0;
    // prepare the messages
    let messages: Vec<String> = (0..COMPRESSION_NUM_RECORDS).map(compression_message_value).collect();
    let header_arr = [RecordHeader::new("key".to_string(), Some(b("value")))];
    let headers = RecordHeaders::with_header_slice(&header_arr);

    // make sure the returned messages are correct
    let now = current_time_ms();
    let mut responses = Vec::with_capacity(COMPRESSION_NUM_RECORDS * 3);
    for message in &messages {
        let key = message.len().to_string().into_bytes();
        // 1. send message without key and header
        let record = ProducerRecord::with_partition_timestamp_key(
            compression_topic.clone(),
            None,
            Some(now),
            None,
            Some(message.as_bytes().to_vec()),
        )
        .expect("valid record");
        responses.push(producer.send(record).await.expect("send should succeed"));
        // 2. send message with key, without header
        let record = ProducerRecord::with_partition_timestamp_key(
            compression_topic.clone(),
            None,
            Some(now),
            Some(key.clone()),
            Some(message.as_bytes().to_vec()),
        )
        .expect("valid record");
        responses.push(producer.send(record).await.expect("send should succeed"));
        // 3. send message with key and header
        let record = ProducerRecord::with_options(
            ProducerRecordOptionsBuilder::new()
                .set_topic(compression_topic.clone())
                .set_timestamp(Some(now))
                .set_key(Some(key))
                .set_value(Some(message.as_bytes().to_vec()))
                .set_headers(Some(headers.clone()))
                .build()
                .expect("every mandatory parameter is set"),
        )
        .expect("valid record");
        responses.push(producer.send(record).await.expect("send should succeed"));
    }
    for (offset, response) in responses.iter().enumerate() {
        let metadata = response
            .get_with_timeout(UNBOUNDED_GET_TIMEOUT)
            .await
            .unwrap_or_else(|e| panic!("{compression}: send {offset} failed: {e:?}"));
        assert_eq!(offset as i64, metadata.offset(), "{compression}");
    }
    verify_compression_consumer_records(
        consumer.as_mut(),
        &messages,
        now,
        &header_arr,
        partition,
        &compression_topic,
        compression,
    )
    .await;

    producer.close().await.expect("close should succeed");
    // "This consumer close very slowly, which may cause the entire test to time
    // out, and we can't wait for it to auto close" (`ProducerCompressionTest.java:109-111`).
    let _ = consumer
        .close_with_options(confluent_kafka::consumer::CloseOptions::new_timeout(Duration::from_secs(1)))
        .await;
}

/// `ProducerCompressionTest.verifyConsumerRecords`
/// (`ProducerCompressionTest.java:116-154`), with `flagLoop` (`:156-163`)
/// folded into the record index: record `i` is message `i / 3`, flavour `i % 3`.
async fn verify_compression_consumer_records(
    consumer: &mut dyn Consumer<Vec<u8>, Vec<u8>>,
    messages: &[String],
    now: i64,
    header_arr: &[RecordHeader],
    partition: i32,
    topic: &str,
    compression: &str,
) {
    let tp = TopicPartition::new(topic.to_string(), partition);
    consumer.assign(vec![tp.clone()]).await.expect("assign should succeed");
    consumer.seek_with_offset(tp, 0).await.expect("seek should succeed");
    let error_message = compression_error_message(compression);
    let records = consume_records(consumer, COMPRESSION_NUM_RECORDS * 3).await;
    for (i, record) in records.iter().enumerate() {
        let (num, flag) = (i / 3, i % 3);
        let message_value = &messages[num];
        let offset = (num * 3 + flag) as i64;
        let value = String::from_utf8(record.value.clone().expect("value")).expect("utf-8 value");
        let key = record.key.as_ref().map(|k| String::from_utf8(k.clone()).expect("utf-8 key"));
        match flag {
            0 => {
                // verify message without key and header
                assert_eq!(None, key, "{error_message}");
                assert_eq!(*message_value, value, "{error_message}");
                assert_eq!(0, record.headers.len(), "{error_message}");
            },
            1 => {
                // verify message with key, without header
                assert_eq!(Some(message_value.len().to_string()), key, "{error_message}");
                assert_eq!(*message_value, value, "{error_message}");
                assert_eq!(0, record.headers.len(), "{error_message}");
            },
            _ => {
                // verify message with key and header
                assert_eq!(Some(message_value.len().to_string()), key, "{error_message}");
                assert_eq!(*message_value, value, "{error_message}");
                assert_eq!(1, record.headers.len(), "{error_message}");
                assert_eq!(header_arr[0], record.headers[0], "{error_message}");
            },
        }
        assert_eq!(now, record.timestamp, "{error_message}");
        assert_eq!(offset, record.offset, "{error_message}");
    }
}

/// Test: Try to produce to a topic with invalid characters, expect an error.
async fn produce_to_invalid_topic_inner<F: ProducerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let producer = factory
        .create(make_config(&bootstrap_for(factory, ctx)))
        .await
        .expect("Failed to create producer");

    let invalid_topic = "topic with spaces!@#$".to_string();
    let record = ProducerRecord::new(invalid_topic, Some(b("value")));

    let future = producer.send(record).await.expect("send returns Ok with a failed future");

    let result = future.get_with_timeout(Duration::from_secs(30)).await;
    assert!(
        result.is_err(),
        "Producing to an invalid topic should result in an error, got: {result:?}"
    );

    producer.close().await.expect("close should succeed");
}

/// Test: Send a record larger than max.request.size, expect RecordTooLarge.
async fn produce_record_too_large_inner<F: ProducerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let mut config = make_config(&bootstrap_for(factory, ctx));
    // Set a very small max request size to trigger the error
    config.insert("max.request.size".to_string(), "100".to_string());
    let producer = factory.create(config).await.expect("Failed to create producer");

    let large_value = vec![b'x'; 200];
    let record = ProducerRecord::new("too-large-topic".to_string(), Some(large_value));

    let future = producer.send(record).await.expect("send returns Ok with a failed future");

    assert!(future.is_done(), "RecordTooLarge future should be immediately done");
    let result = future.get().await;
    assert!(result.is_err(), "RecordTooLarge should return an error");
    let err = result.unwrap_err();
    assert!(
        matches!(err, confluent_kafka::common::Error::RecordTooLarge(_)),
        "Expected RecordTooLarge error, got: {err:?}"
    );

    producer.close().await.expect("close should succeed");
}

/// Translated from `BaseProducerSendTest.testFlush`
/// (`BaseProducerSendTest.scala:483-500`): with `linger.ms=Int.MaxValue` no
/// record is sent on its own, so none of the futures is complete until
/// `flush()` forces the accumulated batches out, after which all are.
///
/// Deviation for the gRPC backends (python / c): their `Send` RPC blocks until
/// the record is delivered, so with `linger.ms=Int.MaxValue` the first send
/// would never return, and every returned future is already complete — the
/// "no request is complete" half cannot be observed through them. They run with
/// `linger.ms=0` and a single round, checking only that `flush()` succeeds and
/// every future is complete afterwards. The native backend runs Java's exact
/// shape: 50 rounds of `numRecords` sends, not-done before `flush()`, done after.
async fn flush_sends_pending_records_inner<F: ProducerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let native = factory.name() == "rust";
    let topic = ctx.topic("topic");
    let linger_ms = if native { INT_MAX_VALUE } else { 0 };
    let producer = factory
        .create(send_test_producer_config(
            &bootstrap_for(factory, ctx),
            &SendTestProducerOpts { linger_ms, delivery_timeout_ms: INT_MAX_VALUE, ..SendTestProducerOpts::default() },
        ))
        .await
        .expect("Failed to create producer");

    let admin = send_test_admin(ctx);
    create_topic_with_admin(admin.as_ref(), &topic, 2, 2).await;
    admin.close_with_timeout(Duration::from_secs(5)).await;

    let rounds = if native { 50 } else { 1 };
    for round in 0..rounds {
        let mut responses = Vec::with_capacity(NUM_RECORDS);
        for _ in 0..NUM_RECORDS {
            let record = ProducerRecord::new(topic.clone(), Some(b("value")));
            responses.push(producer.send(record).await.expect("send should succeed"));
        }
        if native {
            assert!(
                responses.iter().all(|f| !f.is_done()),
                "No request is complete. (round {round})"
            );
        }
        producer.flush().await.expect("flush should succeed");
        assert!(
            responses.iter().all(|f| f.is_done()),
            "All requests are complete. (round {round})"
        );
    }

    producer.close().await.expect("close should succeed");
}

/// Test: Send records, call close(), verify records were delivered.
async fn close_flushes_pending_inner<F: ProducerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let topic = ctx.topic("close_flush");
    let producer = factory
        .create(make_config(&bootstrap_for(factory, ctx)))
        .await
        .expect("Failed to create producer");

    let mut futures = Vec::new();
    for i in 0..3 {
        let record =
            ProducerRecord::with_key(topic.clone(), Some(b(&format!("key-{i}"))), Some(b(&format!("value-{i}"))));
        let future = producer.send(record).await.expect("send should succeed");
        futures.push(future);
    }

    producer.close().await.expect("close should succeed");

    for (i, future) in futures.iter().enumerate() {
        assert!(future.is_done(), "Future {i} should be done after close");
        let metadata = future
            .get()
            .await
            .unwrap_or_else(|e| panic!("Future {i} should succeed after close, got: {e:?}"));
        assert!(metadata.offset() >= 0, "Record {i} should have a valid offset after close");
    }
}

// ---------------------------------------------------------------------------
// Java-source-derived test bodies (translated from
// kafka/clients/clients-integration-tests/.../ProducerFailureHandlingTest.java
// and kafka/core/src/test/scala/.../{Base,Plaintext}ProducerSendTest.scala —
// see design/history/MILESTONE-6/COVERAGE-ASSESSMENT.md)
// ---------------------------------------------------------------------------

/// Cluster with topic auto-creation disabled. Used by
/// `testNonExistentTopic` to prove the producer times out when a topic
/// truly never exists.
///
/// The `BTreeMap` keys are `KAFKA_*` env vars; the Kafka Docker image
/// converts them to dotted server.properties at startup.
fn no_auto_create_cluster_config() -> ClusterConfig {
    let mut props = std::collections::BTreeMap::new();
    props.insert("KAFKA_AUTO_CREATE_TOPICS_ENABLE".to_string(), "false".to_string());
    ClusterConfig::with_properties(props)
}

/// Cluster with a small `message.max.bytes` so server-side rejection of
/// oversized records is easy to trigger. Auto-create stays on so the
/// test topic exists by the time the producer sends.
fn small_max_bytes_cluster_config() -> ClusterConfig {
    let mut props = std::collections::BTreeMap::new();
    props.insert("KAFKA_MESSAGE_MAX_BYTES".to_string(), "15000".to_string());
    ClusterConfig::with_properties(props)
}

// ---------------------------------------------------------------------------
// ProducerFailureHandlingTest fixtures
// ---------------------------------------------------------------------------

/// `ProducerFailureHandlingTest.producerBufferSize` (`ProducerFailureHandlingTest.java:79`).
const FAILURE_PRODUCER_BUFFER_SIZE: i32 = 30000;
/// `serverMessageMaxBytes` (`ProducerFailureHandlingTest.java:80`).
const FAILURE_SERVER_MESSAGE_MAX_BYTES: i32 = FAILURE_PRODUCER_BUFFER_SIZE / 2;
/// `replicaFetchMaxPartitionBytes` (`ProducerFailureHandlingTest.java:81`).
const FAILURE_REPLICA_FETCH_MAX_PARTITION_BYTES: i32 = FAILURE_SERVER_MESSAGE_MAX_BYTES + 200;
/// `replicaFetchMaxResponseBytes` (`ProducerFailureHandlingTest.java:82`).
const FAILURE_REPLICA_FETCH_MAX_RESPONSE_BYTES: i32 = FAILURE_REPLICA_FETCH_MAX_PARTITION_BYTES + 200;

/// Java's `DefaultRecordBatch.RECORD_BATCH_OVERHEAD` (the Rust constant lives
/// in the crate-private `common::record::internal`).
const RECORD_BATCH_OVERHEAD: i32 = 61;
/// Java's `DefaultRecord.MAX_RECORD_OVERHEAD` (crate-private in Rust, as above).
const MAX_RECORD_OVERHEAD: i32 = 21;
/// Java's `Records.LOG_OVERHEAD` (crate-private in Rust, as above).
const LOG_OVERHEAD: i32 = 12;
/// Java's `ServerLogConfigs.MAX_MESSAGE_BYTES_DEFAULT` (`1024 * 1024 + LOG_OVERHEAD`),
/// the broker's default `message.max.bytes`.
const MAX_MESSAGE_BYTES_DEFAULT: i32 = 1024 * 1024 + LOG_OVERHEAD;

/// Broker shape of `ProducerFailureHandlingTest`'s `@ClusterTestDefaults`
/// (`ProducerFailureHandlingTest.java:61-76`): two brokers, topic
/// auto-creation off, `message.max.bytes=15000`,
/// `replica.fetch.max.bytes=15200`, `offsets.topic.num.partitions=1`.
///
/// Java's third property uses `REPLICA_FETCH_RESPONSE_MAX_BYTES_DOC` — the
/// config's *documentation string* — as the key, so the broker receives an
/// unknown property and `replica.fetch.response.max.bytes` keeps its default.
/// Omitting it here is the equivalent broker configuration.
fn producer_failure_handling_cluster_config() -> ClusterConfig {
    let mut cfg = ClusterConfig::with_brokers(2);
    for (key, value) in [
        ("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false".to_string()),
        ("KAFKA_MESSAGE_MAX_BYTES", FAILURE_SERVER_MESSAGE_MAX_BYTES.to_string()),
        (
            "KAFKA_REPLICA_FETCH_MAX_BYTES",
            FAILURE_REPLICA_FETCH_MAX_PARTITION_BYTES.to_string(),
        ),
        ("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", "1".to_string()),
    ] {
        cfg.server_properties.insert(key.to_string(), value);
    }
    cfg
}

/// `ProducerFailureHandlingTest.producerConfig(acks)`
/// (`ProducerFailureHandlingTest.java:291-298`) plus the bootstrap servers
/// `clusterInstance.producer(..)` adds.
fn failure_producer_config(bootstrap_servers: &str, acks: i32) -> HashMap<String, String> {
    HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap_servers.to_string()),
        ("acks".to_string(), acks.to_string()),
        ("retries".to_string(), "0".to_string()),
        ("request.timeout.ms".to_string(), "30000".to_string()),
        ("max.block.ms".to_string(), "10000".to_string()),
        ("buffer.memory".to_string(), FAILURE_PRODUCER_BUFFER_SIZE.to_string()),
    ])
}

/// Translated from `ProducerFailureHandlingTest.testPartitionTooLargeForReplicationWithAckAll`
/// (`ProducerFailureHandlingTest.java:125-129`): this should succeed as the
/// replica fetcher can handle oversized messages since KIP-74.
async fn partition_too_large_for_replication_with_ack_all_inner<F: ProducerBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
) {
    check_too_large_record_for_replication_with_ack_all(ctx, factory, FAILURE_REPLICA_FETCH_MAX_PARTITION_BYTES).await;
}

/// Translated from `ProducerFailureHandlingTest.testResponseTooLargeForReplicationWithAckAll`
/// (`ProducerFailureHandlingTest.java:134-138`): this should succeed as the
/// replica fetcher can handle oversized messages since KIP-74.
async fn response_too_large_for_replication_with_ack_all_inner<F: ProducerBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
) {
    check_too_large_record_for_replication_with_ack_all(ctx, factory, FAILURE_REPLICA_FETCH_MAX_RESPONSE_BYTES).await;
}

/// `ProducerFailureHandlingTest.checkTooLargeRecordForReplicationWithAckAll`
/// (`ProducerFailureHandlingTest.java:270-289`). The topic name is per-test
/// (pooled clusters) instead of Java's fixed `topic10`.
async fn check_too_large_record_for_replication_with_ack_all<F: ProducerBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
    max_fetch_size: i32,
) {
    let max_message_size = max_fetch_size + 100;
    let broker_size = 2;
    let topic_config = BTreeMap::from([
        ("min.insync.replicas".to_string(), broker_size.to_string()),
        ("max.message.bytes".to_string(), max_message_size.to_string()),
    ]);

    // create topic
    let topic10 = ctx.topic("topic10");
    let admin = send_test_admin(ctx);
    create_topic_with_admin_config(admin.as_ref(), &topic10, broker_size, broker_size as i16, topic_config).await;

    // send a record that is too large for replication, but within the broker max message limit
    let value = vec![0u8; (max_message_size - RECORD_BATCH_OVERHEAD - MAX_RECORD_OVERHEAD) as usize];
    let producer = factory
        .create(failure_producer_config(&bootstrap_for(factory, ctx), -1))
        .await
        .expect("Failed to create producer");
    let producer_record = ProducerRecord::new(topic10.clone(), Some(value));
    let record_metadata = producer
        .send(producer_record)
        .await
        .expect("send should succeed")
        .get_with_timeout(UNBOUNDED_GET_TIMEOUT)
        .await
        .expect("an oversized-for-replication record should still be acknowledged (KIP-74)");

    assert_eq!(topic10, record_metadata.topic());
    producer.close().await.expect("close should succeed");
}

/// Translated from `ProducerFailureHandlingTest.testCannotSendToInternalTopic`
/// (`ProducerFailureHandlingTest.java:218-237`).
///
/// Java asks the admin to create `__consumer_offsets` with the group
/// coordinator's topic configs, without looking at the (rejected) result, and
/// then `waitTopicDeletion` proves through broker internals (metadata cache,
/// replica and log managers) that no such topic exists. Those internals are not
/// observable from a client, so that wait is omitted; the admin result is
/// awaited only so the request has reached the controller before the produce,
/// and is ignored exactly like Java's.
async fn cannot_send_to_internal_topic_inner<F: ProducerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    const GROUP_METADATA_TOPIC_NAME: &str = "__consumer_offsets";
    {
        let admin = send_test_admin(ctx);
        // `groupCoordinator().groupMetadataTopicConfigs()`
        // (`GroupCoordinatorService.java:2324-2330`) with the default
        // `offsets.topic.segment.bytes`.
        let topic_config = BTreeMap::from([
            ("cleanup.policy".to_string(), "compact".to_string()),
            ("compression.type".to_string(), "producer".to_string()),
            ("segment.bytes".to_string(), (100 * 1024 * 1024).to_string()),
        ]);
        let new_topic =
            NewTopic::with_num_partitions_replication_factor(GROUP_METADATA_TOPIC_NAME.to_string(), Some(1), Some(1))
                .set_configs(topic_config);
        let _ = admin.create_topics(&[new_topic]).all().get().await;
    }

    let producer = factory
        .create(failure_producer_config(&bootstrap_for(factory, ctx), 1))
        .await
        .expect("Failed to create producer");
    let record = ProducerRecord::with_key(GROUP_METADATA_TOPIC_NAME.to_string(), Some(b("test")), Some(b("test")));
    let thrown = producer
        .send(record)
        .await
        .expect("send should accept the request")
        .get_with_timeout(UNBOUNDED_GET_TIMEOUT)
        .await
        .expect_err("sending to an internal topic should fail");
    assert!(
        matches!(thrown, Error::InvalidTopic(_)),
        "Unexpected exception while sending to an invalid topic {thrown:?}"
    );
    producer.close().await.expect("close should succeed");
}

/// Translated from `ProducerFailureHandlingTest.testTooLargeRecordWithAckZero`
/// (`ProducerFailureHandlingTest.java:87-104`): with ack == 0 the future
/// metadata will have no errors, with offset -1.
///
/// Runs on [`small_max_bytes_cluster_config`], whose `message.max.bytes`
/// (15000) is Java's `serverMessageMaxBytes`, so the record really is over the
/// broker limit. That cluster has one broker, so `brokers().size()` is 1.
async fn produce_too_large_record_acks_zero_inner<F: ProducerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let topic1 = ctx.topic("topic-1");
    let admin = send_test_admin(ctx);
    create_topic_with_admin(admin.as_ref(), &topic1, 1, 1).await;

    let producer = factory
        .create(failure_producer_config(&bootstrap_for(factory, ctx), 0))
        .await
        .expect("Failed to create producer");

    // send a too-large record
    let record = ProducerRecord::with_partition_key(
        topic1,
        None,
        Some(b("key")),
        Some(vec![0u8; (FAILURE_SERVER_MESSAGE_MAX_BYTES + 1) as usize]),
    )
    .expect("valid record");
    let record_metadata = producer
        .send(record)
        .await
        .expect("send should succeed")
        .get_with_timeout(UNBOUNDED_GET_TIMEOUT)
        .await
        .expect("ack=0 produce should not error");

    assert!(!record_metadata.has_offset(), "acks=0 metadata.has_offset() should be false");
    assert_eq!(-1, record_metadata.offset(), "acks=0 metadata.offset() should be -1");

    producer.close().await.expect("close should succeed");
}

/// Translated from `ProducerFailureHandlingTest.testTooLargeRecordWithAckOne`.
/// With acks=1 the broker rejects an oversized record (server-side
/// `message.max.bytes=15000` from the restrictive cluster) and the
/// producer surfaces RecordTooLarge.
async fn produce_too_large_record_acks_one_inner<F: ProducerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let topic = ctx.topic("ack1_too_large");
    let mut config = make_config(&bootstrap_for(factory, ctx));
    config.insert("acks".to_string(), "1".to_string());
    let producer = factory.create(config).await.expect("Failed to create producer");

    let record = ProducerRecord::with_key(topic.clone(), Some(b("key")), Some(vec![0u8; 16_000]));
    let future = producer.send(record).await.expect("send should accept the request");
    let result = future.get_with_timeout(Duration::from_secs(30)).await;

    assert!(result.is_err(), "Oversized record under acks=1 should error, got: {result:?}");
    let err = result.unwrap_err();
    // Java's Producer throws RecordTooLargeException for both client-side
    // (max.request.size) and broker-side (message.max.bytes) rejections.
    // The Rust client uses the dedicated RecordTooLarge variant only for
    // client-side rejections; broker-side rejections come back through
    // the response path as Error(MessageTooLarge). Accept either.
    let too_large = match &err {
        confluent_kafka::common::Error::RecordTooLarge(_) => true,
        confluent_kafka::common::Error::KafkaError(g) => g.error() == confluent_kafka::common::Errors::MessageTooLarge,
        _ => false,
    };
    assert!(
        too_large,
        "Expected RecordTooLarge or Error(MessageTooLarge) from server, got: {err:?}"
    );

    producer.close().await.expect("close should succeed");
}

/// Translated from `ProducerFailureHandlingTest.testNonExistentTopic`.
/// With `auto.create.topics.enable=false` (restrictive cluster), sending
/// to a never-existed topic times out trying to fetch metadata.
async fn produce_to_non_existent_topic_inner<F: ProducerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let mut config = make_config(&bootstrap_for(factory, ctx));
    // Short max.block.ms so the test doesn't sit on the default 30s.
    config.insert("max.block.ms".to_string(), "5000".to_string());
    let producer = factory.create(config).await.expect("Failed to create producer");

    let topic = ctx.topic("never_existed");
    let record = ProducerRecord::with_key(topic.clone(), Some(b("key")), Some(b("value")));
    let future = producer.send(record).await.expect("send should accept the request");
    let result = future.get_with_timeout(Duration::from_secs(30)).await;

    assert!(result.is_err(), "Send to non-existent topic should error, got: {result:?}");
    let err = result.unwrap_err();
    assert!(
        matches!(err, confluent_kafka::common::Error::Timeout(_)),
        "Expected Timeout for non-existent topic, got: {err:?}"
    );
    // The message is `KafkaProducer.waitOnMetadata`'s, pinned exactly by
    // `PlaintextProducerSendTest.testSendTimeoutErrorMessageWhenTopicDoesNotExist`
    // (`PlaintextProducerSendTest.scala:152`) — here with this test's
    // `max.block.ms` of 5000.
    assert_eq!(err.message(), format!("Topic {topic} not present in metadata after 5000 ms."));

    producer.close().await.expect("close should succeed");
}

/// Translated from `ProducerFailureHandlingTest.testWrongBrokerList`.
/// Producer with bootstrap pointing at non-existent brokers; metadata
/// fetch times out within `max.block.ms`.
async fn produce_with_wrong_broker_list_inner<F: ProducerBackendFactory>(_ctx: &mut TestContext, factory: &F) {
    // Don't use ctx.bootstrap_servers — explicitly point at a dead
    // address. 127.0.0.1:1/2 are valid IP literals (so bootstrap
    // validation passes) but ports 1/2 are unused, so connection
    // attempts get RST'd and the producer hits max.block.ms. Same
    // behavior for native rust (loopback on the test process) and the
    // gRPC backends (loopback inside their container).
    let _ = factory; // silence unused warning when no needs_container_bootstrap branch
    let bootstrap = "127.0.0.1:1,127.0.0.1:2";
    let mut config = make_config(bootstrap);
    config.insert("max.block.ms".to_string(), "3000".to_string());
    let producer = factory.create(config).await.expect("Failed to create producer");

    let record = ProducerRecord::with_key("any-topic".to_string(), Some(b("key")), Some(b("value")));
    let future = producer.send(record).await.expect("send should accept the request");
    let result = future.get_with_timeout(Duration::from_secs(30)).await;

    assert!(result.is_err(), "Send with wrong broker list should error, got: {result:?}");
    let err = result.unwrap_err();
    assert!(
        matches!(err, confluent_kafka::common::Error::Timeout(_)),
        "Expected Timeout from unreachable bootstrap, got: {err:?}"
    );

    producer.close().await.expect("close should succeed");
}

/// Translated from `ProducerFailureHandlingTest.testInvalidPartition`.
/// Send with explicit partition >= partition-count of the (auto-created)
/// topic. The producer waits for metadata that never resolves the
/// requested partition and times out within `max.block.ms`.
async fn produce_invalid_partition_inner<F: ProducerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let topic = ctx.topic("invalid_partition");
    let mut config = make_config(&bootstrap_for(factory, ctx));
    config.insert("max.block.ms".to_string(), "5000".to_string());
    let producer = factory.create(config).await.expect("Failed to create producer");

    // First, create the topic with 1 partition by sending to partition 0.
    let warmup = ProducerRecord::with_partition_key(topic.clone(), Some(0), Some(b("k")), Some(b("v")))
        .expect("record creation should succeed");
    producer
        .send(warmup)
        .await
        .expect("warmup send should accept")
        .get_with_timeout(Duration::from_secs(30))
        .await
        .expect("warmup send should succeed");

    // Now send to partition 99 which doesn't exist.
    let record = ProducerRecord::with_partition_key(topic.clone(), Some(99), Some(b("k")), Some(b("v")))
        .expect("record creation should succeed");
    let future = producer.send(record).await.expect("send should accept the request");
    let result = future.get_with_timeout(Duration::from_secs(30)).await;

    assert!(result.is_err(), "Send to invalid partition should error, got: {result:?}");
    let err = result.unwrap_err();
    assert!(
        matches!(err, confluent_kafka::common::Error::Timeout(_)),
        "Expected Timeout for invalid partition, got: {err:?}"
    );
    // `KafkaProducer.waitOnMetadata`'s message, pinned exactly by
    // `PlaintextProducerSendTest.testSendTimeoutErrorWhenPartitionDoesNotExist`
    // (`PlaintextProducerSendTest.scala:171`). The topic was auto-created by
    // the warmup send, so its partition count is the cluster's
    // `num.partitions`; read it back rather than hard-coding the broker default.
    let partition_count = producer
        .partitions_for(&topic)
        .await
        .expect("partitions_for existing topic")
        .len();
    assert_eq!(
        err.message(),
        format!(
            "Partition 99 of topic {topic} with partition count {partition_count} is not present in metadata after 5000 ms."
        )
    );

    producer.close().await.expect("close should succeed");
}

/// Translated from `ProducerFailureHandlingTest.testSendAfterClosed`.
/// Calling send() after close() returns IllegalState.
async fn send_after_closed_inner<F: ProducerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let topic = ctx.topic("send_after_closed");
    let producer = factory
        .create(make_config(&bootstrap_for(factory, ctx)))
        .await
        .expect("Failed to create producer");

    // Warmup to ensure metadata is fresh, mirroring the Java test.
    let record = ProducerRecord::with_key(topic.clone(), Some(b("key")), Some(b("value")));
    producer
        .send(record.clone())
        .await
        .expect("warmup send should accept")
        .get_with_timeout(Duration::from_secs(30))
        .await
        .expect("warmup send should succeed");

    producer.close().await.expect("close should succeed");

    // Native Rust returns Err directly from send() once closed; the
    // gRPC backends return Ok(KafkaFuture) where the future resolves to
    // an IllegalState error (the gRPC server's lookup of the closed
    // producer_id fails). Accept both shapes.
    let send_result = producer.send(record).await;
    let err = match send_result {
        Err(e) => e,
        Ok(future) => future
            .get_with_timeout(Duration::from_secs(5))
            .await
            .expect_err("future after close should be Err"),
    };
    assert!(
        matches!(err, confluent_kafka::common::Error::LocalIllegalState(_)),
        "Expected IllegalState after close, got: {err:?}"
    );
}

/// Translated from `PlaintextProducerSendTest.testBatchSizeZero`
/// (`PlaintextProducerSendTest.scala:69-78`) and the `sendAndVerify` helper it
/// calls (`BaseProducerSendTest.scala:213-237`).
///
/// With `batch.size=0` every record is its own full batch, so the records are
/// sent even though `linger.ms=Int.MaxValue`; `close(20 s)` must deliver all of
/// them at consecutive offsets.
async fn batch_size_zero_inner<F: ProducerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let topic = ctx.topic("topic");
    let producer = factory
        .create(send_test_producer_config(
            &bootstrap_for(factory, ctx),
            &SendTestProducerOpts {
                linger_ms: INT_MAX_VALUE,
                delivery_timeout_ms: INT_MAX_VALUE,
                batch_size: 0,
                ..SendTestProducerOpts::default()
            },
        ))
        .await
        .expect("Failed to create producer");

    // sendAndVerify(producer, numRecords = 100, timeoutMs = 20000)
    let partition = 0;
    let admin = send_test_admin(ctx);
    create_topic_with_admin(admin.as_ref(), &topic, 1, 2).await;

    let mut futures = Vec::with_capacity(NUM_RECORDS);
    for i in 1..=NUM_RECORDS {
        let record = ProducerRecord::with_partition_key(
            topic.clone(),
            Some(partition),
            Some(b(&format!("key{i}"))),
            Some(b(&format!("value{i}"))),
        )
        .expect("valid record");
        futures.push(producer.send(record).await.expect("send should succeed"));
    }
    producer
        .close_with_timeout(Duration::from_millis(20_000))
        .await
        .expect("close should succeed");
    let mut last_offset = 0i64;
    for future in &futures {
        let metadata = future
            .get_with_timeout(UNBOUNDED_GET_TIMEOUT)
            .await
            .expect("produce should succeed");
        assert_eq!(topic, metadata.topic());
        assert_eq!(partition, metadata.partition());
        assert_eq!(last_offset, metadata.offset());
        last_offset += 1;
    }
    assert_eq!(NUM_RECORDS as i64, last_offset);
    // Java's `finally { producer.close() }` is a no-op on the already-closed
    // producer, so it is not repeated here.
    admin.close_with_timeout(Duration::from_secs(5)).await;
}

/// Translated from `PlaintextProducerSendTest.testBatchSizeZeroNoPartitionNoRecordKey`
/// (`PlaintextProducerSendTest.scala:80-101`), including its 15 s `@Timeout`.
async fn batch_size_zero_no_partition_no_record_key_inner<F: ProducerBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
) {
    tokio::time::timeout(Duration::from_secs(15), async {
        let topic = ctx.topic("topic");
        let producer = factory
            .create(send_test_producer_config(
                &bootstrap_for(factory, ctx),
                &SendTestProducerOpts { batch_size: 0, ..SendTestProducerOpts::default() },
            ))
            .await
            .expect("Failed to create producer");
        let num_records = 10;
        // `createTopicWithAdmin(admin, topic, brokers, controllerServers, 2)`:
        // two partitions, default replication factor 1.
        let admin = send_test_admin(ctx);
        create_topic_with_admin(admin.as_ref(), &topic, 2, 1).await;

        let mut futures = Vec::with_capacity(num_records);
        for i in 1..=num_records {
            let record = ProducerRecord::new(topic.clone(), Some(b(&format!("value{i}"))));
            futures.push(producer.send(record).await.expect("send should succeed"));
        }
        producer.flush().await.expect("flush should succeed");
        let mut last_offset = 0;
        for future in &futures {
            let metadata = future.get().await.expect("produce should succeed");
            assert_eq!(topic, metadata.topic());
            last_offset += 1;
        }
        assert_eq!(num_records, last_offset);

        producer.close().await.expect("close should succeed");
        admin.close_with_timeout(Duration::from_secs(5)).await;
    })
    .await
    .expect("testBatchSizeZeroNoPartitionNoRecordKey exceeded its 15 s @Timeout");
}

// ---------------------------------------------------------------------------
// Producer timestamps (BaseProducerSendTest / PlaintextProducerSendTest)
// ---------------------------------------------------------------------------

/// Java's `TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG`. `TopicConfig` is not
/// translated (topic configs are broker-side), so the key is spelled out.
const MESSAGE_TIMESTAMP_TYPE_CONFIG: &str = "message.timestamp.type";
/// Java's `TopicConfig.MESSAGE_TIMESTAMP_BEFORE_MAX_MS_CONFIG`.
const MESSAGE_TIMESTAMP_BEFORE_MAX_MS_CONFIG: &str = "message.timestamp.before.max.ms";
/// Java's `TopicConfig.MESSAGE_TIMESTAMP_AFTER_MAX_MS_CONFIG`.
const MESSAGE_TIMESTAMP_AFTER_MAX_MS_CONFIG: &str = "message.timestamp.after.max.ms";

/// Clock-skew slack for the `LogAppendTime` range checks in
/// [`send_and_verify_timestamp`].
///
/// Translation deviation: Java bounds the broker's append timestamp by
/// `[startTime, now]` with zero tolerance (`BaseProducerSendTest.scala:256,286`),
/// which holds only because its brokers run in-process on the client's clock.
/// Here they run in Docker, where a VM-backed daemon's clock can differ from
/// the host's by milliseconds. Same slack and rationale as
/// `CLOCK_SKEW_SLACK_MS` in `plaintext_consumer_test.rs`
/// (`consume_and_verify_records_with_time_type_log_append`), which records the
/// observed 2 ms overshoot that motivated it.
const CLOCK_SKEW_SLACK_MS: i64 = 50;

/// State of the Scala `object callback` in `sendAndVerifyTimestamp`
/// (`BaseProducerSendTest.scala:245-263`). As in [`SendOffsetCallbackState`],
/// would-be assertion failures inside the callback are recorded and checked on
/// the test task, so they bind (Java's assertions in `onCompletion` run on the
/// I/O thread, where the producer catches and logs them).
struct SendTimestampCallbackState {
    offset: i64,
    timestamp_diff: i64,
    failures: Vec<String>,
}

/// `BaseProducerSendTest.sendAndVerifyTimestamp`
/// (`BaseProducerSendTest.scala:239-297`): sends `NUM_RECORDS` records with
/// explicit timestamps `123456 + i` to partition 0 of a topic whose
/// `message.timestamp.type` is `timestamp_type`, and checks both the callback's
/// and the future's metadata timestamp — the record's own timestamp for
/// `CreateTime`, a broker time within `[startTime, now]` for `LogAppendTime`.
async fn send_and_verify_timestamp<F: ProducerBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
    compression_type: &'static str,
    timestamp_type: TimestampType,
) {
    let topic = ctx.topic("topic");
    let producer = factory
        .create(send_test_producer_config(
            &bootstrap_for(factory, ctx),
            &SendTestProducerOpts {
                compression_type,
                linger_ms: INT_MAX_VALUE,
                delivery_timeout_ms: INT_MAX_VALUE,
                ..SendTestProducerOpts::default()
            },
        ))
        .await
        .expect("Failed to create producer");
    let partition = 0;

    let base_timestamp = 123456i64;
    let start_time = current_time_ms();

    let state = Arc::new(Mutex::new(SendTimestampCallbackState {
        offset: 0,
        timestamp_diff: 1,
        failures: Vec::new(),
    }));
    let callback = || -> Option<Callback> {
        let state = state.clone();
        let topic = topic.clone();
        Some(Box::new(move |metadata, error| {
            let mut st = state.lock().unwrap();
            // Java: `if (exception == null) ... else fail(...)`.
            if let Some(e) = error {
                st.failures
                    .push(format!("Send callback returns the following exception: {e:?}"));
                return;
            }
            let Some(m) = metadata else {
                st.failures
                    .push("Send callback invoked with neither metadata nor error".to_string());
                return;
            };
            let (offset, timestamp_diff) = (st.offset, st.timestamp_diff);
            if m.offset() != offset {
                st.failures.push(format!("expected offset {offset}, got {}", m.offset()));
            }
            if m.topic() != topic {
                st.failures.push(format!("expected topic {topic}, got {}", m.topic()));
            }
            if timestamp_type == TimestampType::CreateTime {
                if m.timestamp() != base_timestamp + timestamp_diff {
                    st.failures.push(format!(
                        "offset {offset}: expected timestamp {}, got {}",
                        base_timestamp + timestamp_diff,
                        m.timestamp()
                    ));
                }
            } else {
                let now = current_time_ms();
                if !(m.timestamp() >= start_time - CLOCK_SKEW_SLACK_MS && m.timestamp() <= now + CLOCK_SKEW_SLACK_MS) {
                    st.failures.push(format!(
                        "offset {offset}: log-append timestamp {} not within [{start_time}, {now}] \
                         +/- {CLOCK_SKEW_SLACK_MS} ms clock-skew slack",
                        m.timestamp()
                    ));
                }
            }
            if m.partition() != partition {
                st.failures
                    .push(format!("expected partition {partition}, got {}", m.partition()));
            }
            st.offset += 1;
            st.timestamp_diff += 1;
        }))
    };

    // create topic
    let admin = send_test_admin(ctx);
    let timestamp_type_config = if timestamp_type == TimestampType::LogAppendTime {
        "LogAppendTime"
    } else {
        "CreateTime"
    };
    create_topic_with_admin_config(
        admin.as_ref(),
        &topic,
        1,
        2,
        BTreeMap::from([(MESSAGE_TIMESTAMP_TYPE_CONFIG.to_string(), timestamp_type_config.to_string())]),
    )
    .await;

    let mut record_and_futures = Vec::with_capacity(NUM_RECORDS);
    for i in 1..=NUM_RECORDS {
        let record_timestamp = base_timestamp + i as i64;
        let record = ProducerRecord::with_partition_timestamp_key(
            topic.clone(),
            Some(partition),
            Some(record_timestamp),
            Some(b(&format!("key{i}"))),
            Some(b(&format!("value{i}"))),
        )
        .expect("valid record");
        let future = producer
            .send_with_callback(record, callback())
            .await
            .expect("send should succeed");
        record_and_futures.push((record_timestamp, future));
    }
    producer
        .close_with_timeout(Duration::from_secs(20))
        .await
        .expect("close should succeed");
    for (record_timestamp, future) in &record_and_futures {
        let record_metadata = future
            .get_with_timeout(UNBOUNDED_GET_TIMEOUT)
            .await
            .expect("produce should succeed");
        if timestamp_type == TimestampType::LogAppendTime {
            let now = current_time_ms();
            assert!(
                record_metadata.timestamp() >= start_time - CLOCK_SKEW_SLACK_MS
                    && record_metadata.timestamp() <= now + CLOCK_SKEW_SLACK_MS,
                "log-append timestamp {} not within [{start_time}, {now}] +/- {CLOCK_SKEW_SLACK_MS} ms clock-skew slack",
                record_metadata.timestamp()
            );
        } else {
            assert_eq!(*record_timestamp, record_metadata.timestamp());
        }
    }
    {
        let st = state.lock().unwrap();
        assert!(st.failures.is_empty(), "callback assertions failed: {:?}", st.failures);
        assert_eq!(
            NUM_RECORDS as i64, st.offset,
            "Should have offset {NUM_RECORDS} but only successfully sent {}",
            st.offset
        );
    }
    // Java's `finally { producer.close() }` is a no-op on the already-closed
    // producer, so it is not repeated here.
    admin.close_with_timeout(Duration::from_secs(5)).await;
}

/// Translated from `BaseProducerSendTest.testSendCompressedMessageWithCreateTime`
/// (`BaseProducerSendTest.scala:196-204`).
async fn send_compressed_message_with_create_time_inner<F: ProducerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    send_and_verify_timestamp(ctx, factory, "gzip", TimestampType::CreateTime).await;
}

/// Translated from `BaseProducerSendTest.testSendNonCompressedMessageWithCreateTime`
/// (`BaseProducerSendTest.scala:206-211`).
async fn send_non_compressed_message_with_create_time_inner<F: ProducerBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
) {
    send_and_verify_timestamp(ctx, factory, "none", TimestampType::CreateTime).await;
}

/// Translated from `PlaintextProducerSendTest.testSendCompressedMessageWithLogAppendTime`
/// (`PlaintextProducerSendTest.scala:103-111`).
async fn send_compressed_message_with_log_append_time_inner<F: ProducerBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
) {
    send_and_verify_timestamp(ctx, factory, "gzip", TimestampType::LogAppendTime).await;
}

/// Translated from `PlaintextProducerSendTest.testSendNonCompressedMessageWithLogAppendTime`
/// (`PlaintextProducerSendTest.scala:113-119`).
async fn send_non_compressed_message_with_log_append_time_inner<F: ProducerBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
) {
    send_and_verify_timestamp(ctx, factory, "none", TimestampType::LogAppendTime).await;
}

/// `PlaintextProducerSendTest.timestampConfigProvider`
/// (`PlaintextProducerSendTest.scala:341-350`), CONSUMER arm only: the
/// before-max config with a record timestamp `now - 5h`, and the after-max
/// config with `now + 5h`. (Java names the constant `fiveMinutesInMs`, but its
/// value is five hours; the value is kept.)
fn timestamp_config_provider() -> [(&'static str, i64); 2] {
    let now = current_time_ms();
    let five_minutes_in_ms: i64 = 5 * 60 * 60 * 1000;
    [
        (MESSAGE_TIMESTAMP_BEFORE_MAX_MS_CONFIG, now - five_minutes_in_ms),
        (MESSAGE_TIMESTAMP_AFTER_MAX_MS_CONFIG, now + five_minutes_in_ms),
    ]
}

/// Creates a fresh single-partition, RF-2 topic per `timestampConfigProvider`
/// case, with `message_timestamp_config = threshold(record_timestamp)`.
///
/// Java's `@ParameterizedTest` runs each case on a fresh cluster with the
/// fixed topic name `"topic"`; clusters are pooled here, so each case gets its
/// own topic instead.
async fn create_timestamp_validation_topic(
    ctx: &mut TestContext,
    admin: &dyn Admin,
    case: usize,
    message_timestamp_config: &str,
    threshold_ms: i64,
) -> String {
    let topic = ctx.topic(&format!("topic_case{case}"));
    create_topic_with_admin_config(
        admin,
        &topic,
        1,
        2,
        BTreeMap::from([(message_timestamp_config.to_string(), threshold_ms.to_string())]),
    )
    .await;
    topic
}

fn timestamped_record(topic: &str, record_timestamp: i64) -> ProducerRecord<Vec<u8>, Vec<u8>> {
    ProducerRecord::with_partition_timestamp_key(
        topic.to_string(),
        Some(0),
        Some(record_timestamp),
        Some(b("key")),
        Some(b("value")),
    )
    .expect("valid record")
}

/// Translated from `PlaintextProducerSendTest.testSendWithInvalidBeforeAndAfterTimestamp`
/// (`PlaintextProducerSendTest.scala:187-214`), both `timestampConfigProvider`
/// cases: a record 5 h outside a 1 h threshold is rejected with
/// `InvalidTimestampException`, uncompressed and gzip-compressed.
async fn send_with_invalid_before_and_after_timestamp_inner<F: ProducerBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
) {
    let admin = send_test_admin(ctx);
    for (case, (message_timestamp_config, record_timestamp)) in timestamp_config_provider().into_iter().enumerate() {
        // set the TopicConfig for timestamp validation to have 1 minute threshold. Note that recordTimestamp has 5 minutes diff
        // (Java's `oneMinuteInMs` is one hour; the value is kept.)
        let one_minute_in_ms: i64 = 60 * 60 * 1000;
        let topic =
            create_timestamp_validation_topic(ctx, admin.as_ref(), case, message_timestamp_config, one_minute_in_ms)
                .await;

        // First uncompressed, then "Test compressed messages."
        for compression_type in ["none", "gzip"] {
            let producer = factory
                .create(send_test_producer_config(
                    &bootstrap_for(factory, ctx),
                    &SendTestProducerOpts { compression_type, ..SendTestProducerOpts::default() },
                ))
                .await
                .expect("Failed to create producer");
            let result = producer
                .send(timestamped_record(&topic, record_timestamp))
                .await
                .expect("send should succeed")
                .get_with_timeout(UNBOUNDED_GET_TIMEOUT)
                .await;
            assert!(
                matches!(result, Err(Error::InvalidTimestamp(_))),
                "{message_timestamp_config}, compression {compression_type}: expected InvalidTimestamp, got {result:?}"
            );
            producer.close().await.expect("close should succeed");
        }
    }
    admin.close_with_timeout(Duration::from_secs(5)).await;
}

/// Shared body of `testValidBeforeAndAfterTimestampsAtThreshold` and
/// `testValidBeforeAndAfterTimestampsWithinThreshold`: for each
/// `timestampConfigProvider` case, the threshold is `threshold(record_timestamp)`
/// and a send of that record, uncompressed then gzip-compressed, does not fail.
///
/// Java asserts only that `send` does not throw (`assertDoesNotThrow`). Its
/// future is also awaited here: `send` is asynchronous, so a broker-side
/// `InvalidTimestampException` — the failure these tests guard against —
/// surfaces only through the future, and Java's `close()` would swallow it.
async fn send_with_valid_timestamp<F: ProducerBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
    threshold: fn(i64) -> i64,
) {
    let admin = send_test_admin(ctx);
    for (case, (message_timestamp_config, record_timestamp)) in timestamp_config_provider().into_iter().enumerate() {
        let topic = create_timestamp_validation_topic(
            ctx,
            admin.as_ref(),
            case,
            message_timestamp_config,
            threshold(record_timestamp),
        )
        .await;

        // First uncompressed, then "Test compressed messages."
        for compression_type in ["none", "gzip"] {
            let producer = factory
                .create(send_test_producer_config(
                    &bootstrap_for(factory, ctx),
                    &SendTestProducerOpts { compression_type, ..SendTestProducerOpts::default() },
                ))
                .await
                .expect("Failed to create producer");
            let future = producer
                .send(timestamped_record(&topic, record_timestamp))
                .await
                .unwrap_or_else(|e| {
                    panic!("{message_timestamp_config}, compression {compression_type}: send failed: {e:?}")
                });
            let result = future.get_with_timeout(UNBOUNDED_GET_TIMEOUT).await;
            assert!(
                result.is_ok(),
                "{message_timestamp_config}, compression {compression_type}: expected a valid timestamp, got {result:?}"
            );
            producer.close().await.expect("close should succeed");
        }
    }
    admin.close_with_timeout(Duration::from_secs(5)).await;
}

/// Translated from `PlaintextProducerSendTest.testValidBeforeAndAfterTimestampsAtThreshold`
/// (`PlaintextProducerSendTest.scala:216-234`), both `timestampConfigProvider`
/// cases.
async fn valid_before_and_after_timestamps_at_threshold_inner<F: ProducerBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
) {
    // set the TopicConfig for timestamp validation to be the same as the record timestamp
    send_with_valid_timestamp(ctx, factory, |record_timestamp| record_timestamp).await;
}

/// Translated from `PlaintextProducerSendTest.testValidBeforeAndAfterTimestampsWithinThreshold`
/// (`PlaintextProducerSendTest.scala:236-254`), both `timestampConfigProvider`
/// cases.
async fn valid_before_and_after_timestamps_within_threshold_inner<F: ProducerBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
) {
    // set the TopicConfig for timestamp validation to have 10 minute threshold. Note that recordTimestamp has 5 minutes diff
    // (Java's `tenMinutesInMs` is ten hours; the value is kept.)
    send_with_valid_timestamp(ctx, factory, |_| 10 * 60 * 60 * 1000).await;
}

/// `testNonBlockingProducer`'s `send` (`PlaintextProducerSendTest.scala:264-266`).
async fn non_blocking_send<P: Producer<Vec<u8>, Vec<u8>>>(producer: &P, topic: &str) -> KafkaFuture<RecordMetadata> {
    let record = ProducerRecord::with_partition_key(topic.to_string(), Some(0), Some(b("key")), Some(vec![0u8; 1000]))
        .expect("valid record");
    producer.send(record).await.expect("send should hand back a future")
}

/// `testNonBlockingProducer`'s `sendUntilQueued`
/// (`PlaintextProducerSendTest.scala:268-281`) over `TestUtils.computeUntilTrue`
/// (15 s budget, 100 ms pause): send until a send is queued — its future is
/// still pending, or it already completed successfully. Like Java, the last
/// future is returned even when the budget runs out; the caller's
/// verification then fails.
async fn non_blocking_send_until_queued<P: Producer<Vec<u8>, Vec<u8>>>(
    producer: &P,
    topic: &str,
) -> KafkaFuture<RecordMetadata> {
    let deadline = Instant::now() + Duration::from_millis(test_utils::DEFAULT_MAX_WAIT_MS);
    loop {
        let future = non_blocking_send(producer, topic).await;
        let queued = if future.is_done() {
            // Send was queued and completed successfully
            future.get().await.is_ok()
        } else {
            // Send future not yet complete, so it has been queued to be sent
            true
        };
        if queued || Instant::now() >= deadline {
            return future;
        }
        tokio::time::sleep(Duration::from_millis(test_utils::DEFAULT_PAUSE_MS)).await;
    }
}

/// `testNonBlockingProducer`'s `verifySendSuccess`
/// (`PlaintextProducerSendTest.scala:283-288`).
async fn non_blocking_verify_send_success(future: &KafkaFuture<RecordMetadata>, topic: &str) {
    let record_metadata = future
        .get_with_timeout(Duration::from_secs(30))
        .await
        .expect("queued send should succeed");
    assert_eq!(topic, record_metadata.topic());
    assert_eq!(0, record_metadata.partition());
    assert!(record_metadata.offset() >= 0, "Invalid offset {record_metadata:?}");
}

/// Translated from `PlaintextProducerSendTest.testNonBlockingProducer`
/// (`PlaintextProducerSendTest.scala:258-313`): requests are failed immediately
/// without blocking if metadata is not available or the buffer is full.
///
/// The buffer-exhaustion half runs on the native backend only: it needs the
/// first record to sit in a lingering batch (`linger.ms=15000`) while the
/// second send finds the buffer full, but the gRPC `Send` RPC blocks until
/// delivery, so the first batch has already been sent — and its buffer freed —
/// by the time the second send is issued.
///
/// Deviation: Java's topic `topic` is auto-created by the producer's first
/// metadata request; here it is created with the admin client beforehand. The
/// producer's metadata cache is still cold, so the first send still finds no
/// metadata available.
async fn non_blocking_producer_inner<F: ProducerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let topic = ctx.topic("topic");
    let admin = send_test_admin(ctx);
    // Auto-creation's shape: `num.partitions=4`, `default.replication.factor=1`.
    create_topic_with_admin(admin.as_ref(), &topic, 4, 1).await;
    let bootstrap = bootstrap_for(factory, ctx);

    // Topic metadata not available, send should fail without blocking
    let producer = factory
        .create(send_test_producer_config(
            &bootstrap,
            &SendTestProducerOpts { max_block_ms: 0, ..SendTestProducerOpts::default() },
        ))
        .await
        .expect("Failed to create producer");
    // verifyMetadataNotAvailable
    let future = non_blocking_send(&producer, &topic).await;
    assert!(future.is_done(), "verify future was completed immediately");
    let err = future.get().await.expect_err("send without metadata should fail");
    assert!(matches!(err, Error::Timeout(_)), "Expected Timeout, got: {err:?}");

    // Test that send starts succeeding once metadata is available
    let future = non_blocking_send_until_queued(&producer, &topic).await;
    non_blocking_verify_send_success(&future, &topic).await;
    producer.close().await.expect("close should succeed");

    if factory.name() != "rust" {
        return;
    }

    // Verify that send fails immediately without blocking when there is no space left in the buffer
    let producer2 = factory
        .create(send_test_producer_config(
            &bootstrap,
            &SendTestProducerOpts {
                max_block_ms: 0,
                linger_ms: 15000,
                batch_size: 1100,
                buffer_size: 1500,
                ..SendTestProducerOpts::default()
            },
        ))
        .await
        .expect("Failed to create producer");
    // wait until metadata is available and one record is queued
    let future2 = non_blocking_send_until_queued(&producer2, &topic).await;
    // should fail send since buffer is full (verifyBufferExhausted)
    let future = non_blocking_send(&producer2, &topic).await;
    assert!(future.is_done(), "verify future was completed immediately");
    let err = future.get().await.expect_err("send with a full buffer should fail");
    match &err {
        Error::ProducerBufferExhausted(e) => assert!(
            e.message().starts_with("Failed to allocate ") && e.message().contains("Total memory: 1500 bytes."),
            "unexpected BufferExhausted message: {}",
            e.message()
        ),
        _ => panic!("Expected BufferExhausted, got: {err:?}"),
    }
    // previous batch should be completed and sent now
    non_blocking_verify_send_success(&future2, &topic).await;
    producer2.close().await.expect("close should succeed");
}

/// Translated from `PlaintextProducerSendTest.testSendRecordBatchWithMaxRequestSizeAndHigher`
/// (`PlaintextProducerSendTest.scala:316-336`): a record whose batch is exactly
/// the broker's default `max.message.bytes` is accepted, and one byte more is
/// rejected with `RecordTooLarge` (the client's `max.request.size` bound, which
/// the estimated batch then exceeds by one byte).
///
/// `serializedValueSize` and the error message are checked on the native
/// backend only: the gRPC servers report serialized sizes as -1 and surface
/// only the error code.
async fn send_record_batch_with_max_request_size_and_higher_inner<F: ProducerBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
) {
    // Java's `topic` is auto-created by the first send; it is created with the
    // admin client here, in auto-creation's shape (`num.partitions=4`,
    // `default.replication.factor=1`).
    let topic = ctx.topic("topic");
    let admin = send_test_admin(ctx);
    create_topic_with_admin(admin.as_ref(), &topic, 4, 1).await;
    let producer_props = HashMap::from([("bootstrap.servers".to_string(), bootstrap_for(factory, ctx))]);
    let producer = factory.create(producer_props).await.expect("Failed to create producer");

    let key_length_size = 1;
    let header_length_size = 1;
    let value_length_size = 3;
    let overhead = LOG_OVERHEAD
        + RECORD_BATCH_OVERHEAD
        + MAX_RECORD_OVERHEAD
        + key_length_size
        + header_length_size
        + value_length_size;
    let value_size = MAX_MESSAGE_BYTES_DEFAULT - overhead;

    let record0 = ProducerRecord::with_key(topic.clone(), Some(Vec::new()), Some(vec![0u8; value_size as usize]));
    let metadata = producer
        .send(record0)
        .await
        .expect("send should hand back a future")
        .get_with_timeout(UNBOUNDED_GET_TIMEOUT)
        .await
        .expect("a record of exactly the maximum size should be accepted");
    if factory.name() == "rust" {
        assert_eq!(value_size, metadata.serialized_value_size());
    }

    let record1 = ProducerRecord::with_key(topic, Some(Vec::new()), Some(vec![0u8; (value_size + 1) as usize]));
    let err = producer
        .send(record1)
        .await
        .expect("send should hand back a future")
        .get_with_timeout(UNBOUNDED_GET_TIMEOUT)
        .await
        .expect_err("a record one byte over the maximum size should be rejected");
    match &err {
        Error::RecordTooLarge(e) => {
            if factory.name() == "rust" {
                assert_eq!(
                    "The message is 1048577 bytes when serialized which is larger than 1048576, which is the value \
                     of the max.request.size configuration.",
                    e.message()
                );
            }
        },
        _ => panic!("Expected RecordTooLarge, got: {err:?}"),
    }
    producer.close().await.expect("close should succeed");
}

// ---------------------------------------------------------------------------
// Test instantiations
// ---------------------------------------------------------------------------
//
// Under multilanguage-tests, each scenario fans out to rust/python/c via
// the macro. Otherwise the rust_only_fallback module instantiates each
// scenario manually against RustNativeFactory.

#[cfg(feature = "multilanguage-tests")]
crate::multilanguage_test!(test_produce_single_record, produce_single_record_inner);
#[cfg(feature = "multilanguage-tests")]
crate::multilanguage_test!(test_produce_with_key, produce_with_key_inner);
#[cfg(feature = "multilanguage-tests")]
crate::multilanguage_test!(test_send_offset, send_offset_inner, producer_send_cluster_config());
#[cfg(feature = "multilanguage-tests")]
crate::multilanguage_test!(test_send_to_partition, send_to_partition_inner, producer_send_cluster_config());
#[cfg(feature = "multilanguage-tests")]
crate::multilanguage_test!(
    test_send_before_and_after_partition_expansion,
    send_before_and_after_partition_expansion_inner,
    producer_send_cluster_config()
);
#[cfg(feature = "multilanguage-tests")]
crate::multilanguage_test!(test_produce_with_compression, produce_with_compression_inner);
#[cfg(feature = "multilanguage-tests")]
crate::multilanguage_test!(test_produce_to_invalid_topic, produce_to_invalid_topic_inner);
#[cfg(feature = "multilanguage-tests")]
crate::multilanguage_test!(test_produce_record_too_large, produce_record_too_large_inner);
/// Test: partitionsFor returns metadata for an existing topic. Exercises the
/// producer PartitionsFor RPC across all backends (the C/Python servers now
/// expose partitions_for).
async fn produce_partitions_for_inner<F: ProducerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let topic = ctx.topic("partitions_for");
    let producer = factory.create(make_config(&bootstrap_for(factory, ctx))).await.expect("create");
    // Produce one record so the topic exists.
    let record = ProducerRecord::with_key(topic.clone(), Some(b("k")), Some(b("v")));
    producer
        .send(record)
        .await
        .expect("send")
        .get_with_timeout(Duration::from_secs(30))
        .await
        .expect("produce");

    let infos = producer.partitions_for(&topic).await.expect("partitions_for should succeed");
    assert!(!infos.is_empty(), "{} backend: expected >=1 partition", factory.name());
    assert!(
        infos.iter().any(|p| p.topic() == topic && p.partition() == 0),
        "{} backend: expected partition 0 of {topic}",
        factory.name()
    );
    producer.close().await.expect("close");
}

/// Translated from `PlaintextProducerSendTest.testPartitionsForTimeoutErrorWhenTopicDoesNotExist`
/// (`PlaintextProducerSendTest.scala:178-185`). Java's only parameter set is
/// `("classic", "false")` (`protocolAndAutoCreateTopicProviders`,
/// `PlaintextProducerSendTest.scala:352-356`), i.e. auto-topic-creation
/// disabled, hence the `no_auto_create_cluster_config()` cluster. The
/// group-protocol parameter is irrelevant to a producer.
///
/// `partitionsFor` on a topic that never appears waits `max.block.ms` (500)
/// in `waitOnMetadata` and throws `TimeoutException` directly (not wrapped in
/// an `ExecutionException`) with the exact message asserted below.
async fn partitions_for_timeout_error_when_topic_does_not_exist_inner<F: ProducerBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
) {
    let mut config = make_config(&bootstrap_for(factory, ctx));
    config.insert("max.block.ms".to_string(), "500".to_string());
    let producer = factory.create(config).await.expect("Failed to create producer");

    let topic = ctx.topic("unexisting-topic");
    let err = producer
        .partitions_for(&topic)
        .await
        .expect_err("partitions_for on a non-existent topic should fail");
    assert!(
        matches!(err, Error::Timeout(_)),
        "{} backend: expected Timeout, got: {err:?}",
        factory.name()
    );
    assert_eq!(err.message(), format!("Topic {topic} not present in metadata after 500 ms."));

    producer.close().await.expect("close");
}

/// End-to-end check on the Milestone-12 producer `metrics()` wiring. For the
/// Python / C backends the snapshot crosses the `Metrics` RPC, the binding, and
/// the `kafka_producer_MetricMap_t` FFI surface before being rebuilt
/// client-side. A backend that silently reported an empty map fails here.
///
/// Assertions are value-based but robust: after producing N records and
/// flushing against a real broker, the cumulative totals must be positive
/// regardless of timing.
async fn produce_and_check_metrics_inner<F: ProducerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    use confluent_kafka::common::{Metric, MetricValue};

    let topic = ctx.topic("producer_metrics");
    let producer = factory
        .create(make_config(&bootstrap_for(factory, ctx)))
        .await
        .expect("Failed to create producer");

    // Produce a handful of records so the sender/buffer-pool metrics accrue.
    let mut futures = Vec::new();
    for i in 0..5 {
        let record =
            ProducerRecord::with_key(topic.clone(), Some(b(&format!("key-{i}"))), Some(b(&format!("value-{i}"))));
        futures.push(producer.send(record).await.expect("send should succeed"));
    }
    producer.flush().await.expect("flush should succeed");
    for (i, future) in futures.iter().enumerate() {
        future
            .get_with_timeout(Duration::from_secs(30))
            .await
            .unwrap_or_else(|e| panic!("Record {i} should succeed, got: {e:?}"));
    }

    let snapshot = producer.metrics();
    assert!(
        !snapshot.is_empty(),
        "{} backend: metrics() returned an empty map — the backend registry is not wired through",
        factory.name()
    );

    // Reads the (untagged, client-level) metric named `name` as f64. Producer
    // client-level metrics carry only the `client-id` tag, so name-match on an
    // untagged lookup is unambiguous for these.
    let value_of = |name: &str| -> Option<f64> {
        snapshot
            .iter()
            .find(|(n, _)| n.name() == name && !n.tags().contains_key("topic"))
            .map(|(_, m)| match m.metric_value() {
                MetricValue::Double(d) => d,
                MetricValue::Long(l) => l as f64,
                MetricValue::Int(i) => i as f64,
                MetricValue::String(_) => f64::NAN,
            })
    };

    // The producer-metrics + producer-topic-metrics groups must be present.
    let groups: std::collections::HashSet<&str> = snapshot.keys().map(|n| n.group()).collect();
    assert!(
        groups.contains("producer-metrics"),
        "{} backend: no producer-metrics group in {:?}",
        factory.name(),
        groups
    );

    // record-send-total is cumulative: >= the 5 records we produced.
    let record_send_total = value_of("record-send-total").expect("record-send-total present");
    assert!(
        record_send_total >= 5.0,
        "{} backend: record-send-total {record_send_total} should be >= 5 after producing 5 records",
        factory.name()
    );

    // batch-size-avg is a positive average once at least one batch was sent.
    let batch_size_avg = value_of("batch-size-avg").expect("batch-size-avg present");
    assert!(
        batch_size_avg > 0.0,
        "{} backend: batch-size-avg {batch_size_avg} should be > 0 after sends",
        factory.name()
    );

    // request-latency-avg must be recorded (present) after a real round-trip.
    assert!(
        value_of("request-latency-avg").is_some(),
        "{} backend: request-latency-avg missing from metrics()",
        factory.name()
    );

    // buffer-total-bytes is buffer.memory (> 0); available never exceeds total.
    let buffer_total = value_of("buffer-total-bytes").expect("buffer-total-bytes present");
    let buffer_available = value_of("buffer-available-bytes").expect("buffer-available-bytes present");
    assert!(
        buffer_total > 0.0,
        "{} backend: buffer-total-bytes {buffer_total} should be > 0",
        factory.name()
    );
    assert!(
        buffer_available <= buffer_total,
        "{} backend: buffer-available-bytes {buffer_available} should be <= buffer-total-bytes {buffer_total}",
        factory.name()
    );

    // flush-time-ns-total is cumulative and > 0 once flush() ran.
    let flush_time = value_of("flush-time-ns-total").expect("flush-time-ns-total present");
    assert!(
        flush_time > 0.0,
        "{} backend: flush-time-ns-total {flush_time} should be > 0 after flush()",
        factory.name()
    );

    producer.close().await.expect("close should succeed");
}

#[cfg(feature = "multilanguage-tests")]
crate::multilanguage_test!(test_produce_and_check_metrics, produce_and_check_metrics_inner);
#[cfg(feature = "multilanguage-tests")]
crate::multilanguage_test!(
    test_flush_sends_pending_records,
    flush_sends_pending_records_inner,
    producer_send_cluster_config()
);
#[cfg(feature = "multilanguage-tests")]
crate::multilanguage_test!(test_close_flushes_pending, close_flushes_pending_inner);

// New tests translated from Java/Scala sources (see COVERAGE-ASSESSMENT.md).
// Tests using the restrictive cluster (no auto-create + small message.max.bytes)
// pass it as the macro's optional 3rd argument.

#[cfg(feature = "multilanguage-tests")]
crate::multilanguage_test!(
    test_produce_too_large_record_acks_zero,
    produce_too_large_record_acks_zero_inner,
    small_max_bytes_cluster_config()
);
#[cfg(feature = "multilanguage-tests")]
crate::multilanguage_test!(
    test_partition_too_large_for_replication_with_ack_all,
    partition_too_large_for_replication_with_ack_all_inner,
    producer_failure_handling_cluster_config()
);
#[cfg(feature = "multilanguage-tests")]
crate::multilanguage_test!(
    test_response_too_large_for_replication_with_ack_all,
    response_too_large_for_replication_with_ack_all_inner,
    producer_failure_handling_cluster_config()
);
#[cfg(feature = "multilanguage-tests")]
crate::multilanguage_test!(
    test_cannot_send_to_internal_topic,
    cannot_send_to_internal_topic_inner,
    producer_failure_handling_cluster_config()
);
#[cfg(feature = "multilanguage-tests")]
crate::multilanguage_test!(
    test_produce_too_large_record_acks_one,
    produce_too_large_record_acks_one_inner,
    small_max_bytes_cluster_config()
);
#[cfg(feature = "multilanguage-tests")]
crate::multilanguage_test!(
    test_produce_to_non_existent_topic,
    produce_to_non_existent_topic_inner,
    no_auto_create_cluster_config()
);
#[cfg(feature = "multilanguage-tests")]
crate::multilanguage_test!(test_produce_with_wrong_broker_list, produce_with_wrong_broker_list_inner);
#[cfg(feature = "multilanguage-tests")]
crate::multilanguage_test!(test_produce_invalid_partition, produce_invalid_partition_inner);
#[cfg(feature = "multilanguage-tests")]
crate::multilanguage_test!(test_send_after_closed, send_after_closed_inner);
#[cfg(feature = "multilanguage-tests")]
crate::multilanguage_test!(test_batch_size_zero, batch_size_zero_inner, producer_send_cluster_config());
#[cfg(feature = "multilanguage-tests")]
crate::multilanguage_test!(
    test_batch_size_zero_no_partition_no_record_key,
    batch_size_zero_no_partition_no_record_key_inner,
    producer_send_cluster_config()
);
// The four `sendAndVerifyTimestamp` tests are native-only, also under
// `multilanguage-tests`. They keep Java's producer shape (`linger.ms` and
// `delivery.timeout.ms` = Int.MaxValue, default `batch.size`), which relies on
// `send()` returning an unresolved future so `close(20s)` can drain the
// lingering batch. The gRPC `Send` RPC is synchronous per record: it blocks
// until delivery (`bindings/c/grpc_server/server.cc` calls
// `kafka_producer_FutureRecordMetadata_get`; `bindings/python/grpc_server.py`
// calls `future.result(timeout=120)`). So the first record's partial batch
// never ships and `close` is never reached on the python / c backends.
#[cfg(feature = "multilanguage-tests")]
#[allow(non_snake_case)] // `__rust` suffix matches `multilanguage_test!` naming.
#[tokio::test(flavor = "multi_thread")]
async fn test_send_compressed_message_with_create_time__rust() {
    let mut ctx = TestContext::new(producer_send_cluster_config()).await;
    send_compressed_message_with_create_time_inner(&mut ctx, &crate::common::backend_factory::RustNativeFactory).await;
}
#[cfg(feature = "multilanguage-tests")]
#[allow(non_snake_case)] // `__rust` suffix matches `multilanguage_test!` naming.
#[tokio::test(flavor = "multi_thread")]
async fn test_send_non_compressed_message_with_create_time__rust() {
    let mut ctx = TestContext::new(producer_send_cluster_config()).await;
    send_non_compressed_message_with_create_time_inner(&mut ctx, &crate::common::backend_factory::RustNativeFactory)
        .await;
}
#[cfg(feature = "multilanguage-tests")]
#[allow(non_snake_case)] // `__rust` suffix matches `multilanguage_test!` naming.
#[tokio::test(flavor = "multi_thread")]
async fn test_send_compressed_message_with_log_append_time__rust() {
    let mut ctx = TestContext::new(producer_send_cluster_config()).await;
    send_compressed_message_with_log_append_time_inner(&mut ctx, &crate::common::backend_factory::RustNativeFactory)
        .await;
}
#[cfg(feature = "multilanguage-tests")]
#[allow(non_snake_case)] // `__rust` suffix matches `multilanguage_test!` naming.
#[tokio::test(flavor = "multi_thread")]
async fn test_send_non_compressed_message_with_log_append_time__rust() {
    let mut ctx = TestContext::new(producer_send_cluster_config()).await;
    send_non_compressed_message_with_log_append_time_inner(
        &mut ctx,
        &crate::common::backend_factory::RustNativeFactory,
    )
    .await;
}
#[cfg(feature = "multilanguage-tests")]
crate::multilanguage_test!(
    test_send_with_invalid_before_and_after_timestamp,
    send_with_invalid_before_and_after_timestamp_inner,
    producer_send_cluster_config()
);
#[cfg(feature = "multilanguage-tests")]
crate::multilanguage_test!(
    test_valid_before_and_after_timestamps_at_threshold,
    valid_before_and_after_timestamps_at_threshold_inner,
    producer_send_cluster_config()
);
#[cfg(feature = "multilanguage-tests")]
crate::multilanguage_test!(
    test_valid_before_and_after_timestamps_within_threshold,
    valid_before_and_after_timestamps_within_threshold_inner,
    producer_send_cluster_config()
);
#[cfg(feature = "multilanguage-tests")]
crate::multilanguage_test!(
    test_non_blocking_producer,
    non_blocking_producer_inner,
    producer_send_cluster_config()
);
#[cfg(feature = "multilanguage-tests")]
crate::multilanguage_test!(
    test_send_record_batch_with_max_request_size_and_higher,
    send_record_batch_with_max_request_size_and_higher_inner,
    producer_send_cluster_config()
);
#[cfg(feature = "multilanguage-tests")]
crate::multilanguage_test!(test_produce_partitions_for, produce_partitions_for_inner);
#[cfg(feature = "multilanguage-tests")]
crate::multilanguage_test!(
    test_partitions_for_timeout_error_when_topic_does_not_exist,
    partitions_for_timeout_error_when_topic_does_not_exist_inner,
    no_auto_create_cluster_config()
);
// `testCompression` is native-only, also under `multilanguage-tests` — see
// `compression_inner` for why the gRPC backends cannot run it.
#[cfg(feature = "multilanguage-tests")]
#[allow(non_snake_case)] // `__rust` suffix matches `multilanguage_test!` naming.
#[tokio::test(flavor = "multi_thread")]
async fn test_compression__rust() {
    let mut ctx = TestContext::new(ClusterConfig::default()).await;
    compression_inner(&mut ctx, &crate::common::backend_factory::RustNativeFactory).await;
}

// ---------------------------------------------------------------------------
// Rust-native-only tests — the Producer trait surface they exercise
// (KafkaFuture cancellation under close_with_timeout(0); Box<dyn Serializer>
// error path) doesn't survive the gRPC bytes-on-the-wire boundary, so
// they aren't multilanguageable. See COVERAGE-ASSESSMENT.md.
// ---------------------------------------------------------------------------

/// A test-only Serializer that always returns Err — used to translate
/// the Scala `testWrongSerializer` scenario where the producer's
/// configured serializer fails for the input data.
#[cfg(feature = "integration-tests")]
struct FailingSerializer;

#[cfg(feature = "integration-tests")]
impl confluent_kafka::common::serialization::Serializer<Vec<u8>> for FailingSerializer {
    fn serialize(
        &self,
        _topic: &str,
        _data: Option<&Vec<u8>>,
    ) -> Result<Option<Vec<u8>>, confluent_kafka::common::Error> {
        Err(confluent_kafka::common::Error::serialization("FailingSerializer always fails"))
    }
}

/// Translated from `BaseProducerSendTest.testCloseWithZeroTimeoutFromCallerThread`
/// (`BaseProducerSendTest.scala:505-525`): 50 times, a producer with
/// `linger.ms=Int.MaxValue` sends `numRecords` records to partition 0, none of
/// which may be complete yet; `close(Duration.ZERO)` must then abort every one
/// of them with a bare `KafkaException` (`Error::KafkaError`, message
/// `"Producer is closed forcefully."`, `RecordAccumulator.java:1146`).
///
/// Native only: the gRPC backends' `Send` RPC blocks until delivery, which with
/// `linger.ms=Int.MaxValue` never happens.
#[cfg(feature = "integration-tests")]
#[tokio::test(flavor = "multi_thread")]
async fn test_close_with_zero_timeout_aborts_pending() {
    close_with_zero_timeout_from_caller_thread_inner(&crate::common::backend_factory::RustNativeFactory).await;
}

/// Body of [`test_close_with_zero_timeout_aborts_pending`], generic so the
/// `Producer` trait's `send` is used rather than `KafkaProducer`'s inherent one.
#[cfg(feature = "integration-tests")]
async fn close_with_zero_timeout_from_caller_thread_inner<F: ProducerBackendFactory>(factory: &F) {
    let mut ctx = TestContext::new(producer_send_cluster_config()).await;
    let topic = ctx.topic("topic");
    let admin = send_test_admin(&ctx);
    create_topic_with_admin(admin.as_ref(), &topic, 2, 2).await;
    admin.close_with_timeout(Duration::from_secs(5)).await;
    let partition = 0;

    // Test closing from caller thread.
    for round in 0..50 {
        let producer = factory
            .create(send_test_producer_config(
                ctx.bootstrap_servers(),
                &SendTestProducerOpts {
                    linger_ms: INT_MAX_VALUE,
                    delivery_timeout_ms: INT_MAX_VALUE,
                    ..SendTestProducerOpts::default()
                },
            ))
            .await
            .expect("Failed to create producer");
        let mut responses = Vec::with_capacity(NUM_RECORDS);
        for _ in 0..NUM_RECORDS {
            let record0 = ProducerRecord::with_partition_key(topic.clone(), Some(partition), None, Some(b("value")))
                .expect("valid record");
            responses.push(producer.send(record0).await.expect("send should succeed"));
        }
        assert!(
            responses.iter().all(|f| !f.is_done()),
            "No request is complete. (round {round})"
        );
        producer
            .close_with_timeout(Duration::ZERO)
            .await
            .expect("close(Duration.ZERO) should succeed");
        for future in &responses {
            let err = future
                .get_with_timeout(UNBOUNDED_GET_TIMEOUT)
                .await
                .expect_err("future should be aborted by close(Duration.ZERO)");
            assert!(
                matches!(err, confluent_kafka::common::Error::KafkaError(_)),
                "Expected a bare KafkaError, got: {err:?}"
            );
            assert_eq!("Producer is closed forcefully.", err.message());
        }
    }
}

/// Translated from `PlaintextProducerSendTest.testWrongSerializer`.
/// A serializer that always errors causes send to surface a
/// `Error::Serialization`. The Producer trait wraps the serializer
/// in `Box<dyn Serializer>` — that surface only exists in the native
/// Rust path, hence rust-only.
#[cfg(feature = "integration-tests")]
#[tokio::test(flavor = "multi_thread")]
async fn test_wrong_serializer_errors_send() {
    use confluent_kafka::common::serialization::ByteArraySerializer;
    use confluent_kafka::producer::{KafkaProducer, ProducerConfig};

    let ctx = TestContext::new(ClusterConfig::default()).await;
    let props = make_config(ctx.bootstrap_servers());
    let producer_config = ProducerConfig::new(&props).expect("Invalid test config");
    let producer = KafkaProducer::new(producer_config, Box::new(ByteArraySerializer), Box::new(FailingSerializer))
        .expect("Failed to create producer");

    let record = ProducerRecord::with_key("any-topic".to_string(), Some(b("key")), Some(b("value")));
    // UFCS to call the trait method past the inherent shadow.
    let send_result = <KafkaProducer<Vec<u8>, Vec<u8>> as Producer<Vec<u8>, Vec<u8>>>::send(&producer, record).await;

    // Either send returns Err directly, or returns Ok(future) where
    // future.get() errors — both are acceptable for serialization
    // failure depending on whether the producer fails fast or at the
    // accumulator-add boundary.
    let err = match send_result {
        Err(e) => e,
        Ok(future) => future
            .get_with_timeout(Duration::from_secs(5))
            .await
            .expect_err("future should be Err for failing serializer"),
    };
    assert!(
        matches!(err, confluent_kafka::common::Error::Serialization(_)),
        "Expected Serialization error, got: {err:?}"
    );

    Producer::close(&producer).await.expect("close should succeed");
}

/// Translated from `BaseProducerSendTest.testCloseWithZeroTimeoutFromSenderThread`
/// (`BaseProducerSendTest.scala:527-567`): closing the producer from its own
/// send callbacks — `close(0)` then `close()`, repeated by every callback —
/// neither deadlocks nor blocks, and the originally sent records are delivered.
///
/// Deviation (Rust has no synchronous `close`/`send`): the callback is a sync
/// `FnOnce` running on the producer's sender task, while `close` and `send` are
/// `async`. Awaiting them there would need `block_on` on the sender task, which
/// is the very self-join Java's `invokedFromCallback` check
/// (`KafkaProducer.java:1398-1403`) exists to avoid. Per CLAUDE.md §9.3 the
/// callback instead hands its work to a detached task. To keep Java's
/// sequential semantics — callbacks run one after another on the single I/O
/// thread, so the first callback's sends precede every `close` — each callback
/// enqueues its work on one channel drained in callback order by a single
/// worker task, rather than spawning one task per callback.
///
/// Native only: it needs `send_with_callback` closures that call back into the
/// same producer, which cannot cross the gRPC boundary.
#[cfg(feature = "integration-tests")]
#[tokio::test(flavor = "multi_thread")]
async fn test_close_with_zero_timeout_from_sender_thread() {
    use std::sync::Arc;

    use confluent_kafka::producer::KafkaProducer;

    use crate::common::backend_factory::RustNativeFactory;

    type NativeProducer = KafkaProducer<Vec<u8>, Vec<u8>>;

    let mut ctx = TestContext::new(producer_send_cluster_config()).await;
    let topic = ctx.topic("topic");
    let admin = send_test_admin(&ctx);
    create_topic_with_admin(admin.as_ref(), &topic, 1, 2).await;
    let partition = 0;
    let mut consumer = send_test_consumer(&ctx);
    consumer
        .assign(vec![TopicPartition::new(topic.clone(), partition)])
        .await
        .expect("assign should succeed");
    let record = {
        let topic = topic.clone();
        move || {
            ProducerRecord::with_partition_key(topic.clone(), Some(partition), None, Some(b("value")))
                .expect("valid record")
        }
    };

    // Test closing from sender thread.
    for _ in 0..50 {
        let producer: Arc<NativeProducer> = Arc::new(
            ProducerBackendFactory::create(
                &RustNativeFactory,
                send_test_producer_config(
                    ctx.bootstrap_servers(),
                    &SendTestProducerOpts {
                        linger_ms: INT_MAX_VALUE,
                        delivery_timeout_ms: INT_MAX_VALUE,
                        ..SendTestProducerOpts::default()
                    },
                ),
            )
            .await
            .expect("Failed to create producer"),
        );

        // The `CloseCallback` body, run in callback order by one worker task.
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<bool>();
        let worker = {
            let producer = Arc::clone(&producer);
            let record = record.clone();
            tokio::spawn(async move {
                while let Some(send_records) = rx.recv().await {
                    // Trigger another batch in accumulator before close the producer. These messages should
                    // not be sent.
                    if send_records {
                        for _ in 0..NUM_RECORDS {
                            <NativeProducer as Producer<Vec<u8>, Vec<u8>>>::send(&producer, record())
                                .await
                                .expect("send before the first close should be accepted");
                        }
                    }
                    // The close call will be called by all the message callbacks. This tests idempotence of the close call.
                    Producer::close_with_timeout(producer.as_ref(), Duration::ZERO)
                        .await
                        .expect("close(0) should succeed");
                    // Test close with non zero timeout. Should not block at all.
                    tokio::time::timeout(Duration::from_secs(30), Producer::close(producer.as_ref()))
                        .await
                        .expect("close() after close(0) should not block")
                        .expect("close() should succeed");
                }
            })
        };

        // send message to partition 0
        // Only send the records in the first callback since we close the producer in the callback and no records
        // can be sent afterwards.
        let mut responses = Vec::with_capacity(NUM_RECORDS);
        for i in 0..NUM_RECORDS {
            let tx = tx.clone();
            let callback: Callback = Box::new(move |_metadata, _error| {
                let _ = tx.send(i == 0);
            });
            responses.push(
                Producer::send_with_callback(producer.as_ref(), record(), Some(callback))
                    .await
                    .expect("send should succeed"),
            );
        }
        drop(tx);
        assert!(responses.iter().all(|f| !f.is_done()), "No request is complete.");
        // flush the messages.
        Producer::flush(producer.as_ref()).await.expect("flush should succeed");
        assert!(responses.iter().all(|f| f.is_done()), "All requests are complete.");
        // Check the messages received by broker.
        poll_until_at_least_num_records(consumer.as_mut(), NUM_RECORDS).await;

        // Every callback has fired (all responses are done), so the worker's
        // channel is closed and it finishes once its queued closes return.
        tokio::time::timeout(Duration::from_secs(60), worker)
            .await
            .expect("callback worker should finish")
            .expect("callback worker should not panic");
        Producer::close(producer.as_ref()).await.expect("close should succeed");
    }

    consumer.close().await.expect("consumer close should succeed");
    admin.close_with_timeout(Duration::from_secs(5)).await;
}

/// Test: a delivery callback registered through each backend's own binding is
/// invoked exactly once, with metadata matching the send future's.
///
/// This is the producer half of the callback-bridging coverage; the consumer
/// half (rebalance listener / commit callback) lives in
/// multilanguage_consumer_test.rs. See
/// [`crate::common::callback_log`] for why the assertion goes through a
/// server-side log rather than a closure handed across the wire.
///
/// The `exactly once` half of the claim is what
/// `wait_for_kind_settled`'s grace window buys: a plain `wait_for_kind`
/// returns the instant the first `delivery` entry is visible, so a
/// double-firing backend would usually be sampled between the two appends and
/// the `len() == 1` assertion would pass vacuously.
async fn delivery_callback_logs_metadata_inner<F: ProducerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let topic = ctx.topic("delivery_callback");
    let (producer, log) = factory
        .create_with_callback_log(make_config(&bootstrap_for(factory, ctx)))
        .await
        .expect("create producer with callback log");

    let record = ProducerRecord::with_key(topic.clone(), Some(b("dk")), Some(b("dv")));
    let future = log
        .send_with_logging_callback(&producer, record)
        .await
        .expect("send with logging callback");
    let metadata = future
        .get_with_timeout(Duration::from_secs(30))
        .await
        .expect("produce should succeed");
    // Flush so a backend that batches has certainly run its completion path.
    producer.flush().await.expect("flush should succeed");

    // Settle before counting: `assert_eq!(len, 1)` on the earliest snapshot
    // that contains one entry cannot detect a second invocation.
    let entries = log
        .wait_for_kind_settled(KIND_DELIVERY, Duration::from_secs(20), Duration::from_millis(750))
        .await;
    let deliveries: Vec<_> = entries.iter().filter(|e| e.kind == KIND_DELIVERY).collect();
    assert_eq!(
        deliveries.len(),
        1,
        "{} backend: expected exactly one {KIND_DELIVERY} entry (callbacks fire once per record); log = {entries:?}",
        factory.name()
    );
    let delivery = deliveries[0];
    assert!(
        delivery.error.is_empty(),
        "{} backend: delivery callback saw an error: {}",
        factory.name(),
        delivery.error
    );
    assert!(
        delivery.has_partition(&topic, metadata.partition()),
        "{} backend: delivery entry does not name {topic}-{}; entry = {delivery:?}",
        factory.name(),
        metadata.partition()
    );
    assert_eq!(
        delivery.offset_for(&topic, metadata.partition()),
        Some(metadata.offset()),
        "{} backend: delivery callback offset disagrees with the send future's",
        factory.name()
    );

    producer.close().await.expect("close should succeed");
}

#[cfg(feature = "multilanguage-tests")]
crate::multilanguage_test!(test_delivery_callback_logs_metadata, delivery_callback_logs_metadata_inner);

#[cfg(all(feature = "integration-tests", not(feature = "multilanguage-tests")))]
mod rust_only_fallback {
    use super::*;
    use crate::common::backend_factory::RustNativeFactory;

    async fn ctx() -> TestContext {
        TestContext::new(ClusterConfig::default()).await
    }

    async fn no_auto_create_ctx() -> TestContext {
        TestContext::new(no_auto_create_cluster_config()).await
    }

    async fn small_max_bytes_ctx() -> TestContext {
        TestContext::new(small_max_bytes_cluster_config()).await
    }

    async fn send_ctx() -> TestContext {
        TestContext::new(producer_send_cluster_config()).await
    }

    async fn failure_handling_ctx() -> TestContext {
        TestContext::new(producer_failure_handling_cluster_config()).await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_produce_single_record() {
        produce_single_record_inner(&mut ctx().await, &RustNativeFactory).await;
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn test_produce_with_key() {
        produce_with_key_inner(&mut ctx().await, &RustNativeFactory).await;
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn test_send_offset() {
        send_offset_inner(&mut send_ctx().await, &RustNativeFactory).await;
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn test_send_to_partition() {
        send_to_partition_inner(&mut send_ctx().await, &RustNativeFactory).await;
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn test_send_before_and_after_partition_expansion() {
        send_before_and_after_partition_expansion_inner(&mut send_ctx().await, &RustNativeFactory).await;
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn test_produce_with_compression() {
        produce_with_compression_inner(&mut ctx().await, &RustNativeFactory).await;
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn test_produce_to_invalid_topic() {
        produce_to_invalid_topic_inner(&mut ctx().await, &RustNativeFactory).await;
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn test_produce_record_too_large() {
        produce_record_too_large_inner(&mut ctx().await, &RustNativeFactory).await;
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn test_flush_sends_pending_records() {
        flush_sends_pending_records_inner(&mut send_ctx().await, &RustNativeFactory).await;
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn test_produce_partitions_for() {
        produce_partitions_for_inner(&mut ctx().await, &RustNativeFactory).await;
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn test_partitions_for_timeout_error_when_topic_does_not_exist() {
        partitions_for_timeout_error_when_topic_does_not_exist_inner(
            &mut no_auto_create_ctx().await,
            &RustNativeFactory,
        )
        .await;
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn test_produce_and_check_metrics() {
        produce_and_check_metrics_inner(&mut ctx().await, &RustNativeFactory).await;
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn test_close_flushes_pending() {
        close_flushes_pending_inner(&mut ctx().await, &RustNativeFactory).await;
    }

    // The 8 newly translated multi-language tests, run rust-only when
    // the multilanguage-tests feature isn't enabled.

    #[tokio::test(flavor = "multi_thread")]
    async fn test_produce_too_large_record_acks_zero() {
        produce_too_large_record_acks_zero_inner(&mut small_max_bytes_ctx().await, &RustNativeFactory).await;
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn test_partition_too_large_for_replication_with_ack_all() {
        partition_too_large_for_replication_with_ack_all_inner(&mut failure_handling_ctx().await, &RustNativeFactory)
            .await;
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn test_response_too_large_for_replication_with_ack_all() {
        response_too_large_for_replication_with_ack_all_inner(&mut failure_handling_ctx().await, &RustNativeFactory)
            .await;
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn test_cannot_send_to_internal_topic() {
        cannot_send_to_internal_topic_inner(&mut failure_handling_ctx().await, &RustNativeFactory).await;
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn test_produce_too_large_record_acks_one() {
        produce_too_large_record_acks_one_inner(&mut small_max_bytes_ctx().await, &RustNativeFactory).await;
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn test_produce_to_non_existent_topic() {
        produce_to_non_existent_topic_inner(&mut no_auto_create_ctx().await, &RustNativeFactory).await;
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn test_produce_with_wrong_broker_list() {
        produce_with_wrong_broker_list_inner(&mut ctx().await, &RustNativeFactory).await;
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn test_produce_invalid_partition() {
        produce_invalid_partition_inner(&mut ctx().await, &RustNativeFactory).await;
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn test_send_after_closed() {
        send_after_closed_inner(&mut ctx().await, &RustNativeFactory).await;
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn test_batch_size_zero() {
        batch_size_zero_inner(&mut send_ctx().await, &RustNativeFactory).await;
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn test_batch_size_zero_no_partition_no_record_key() {
        batch_size_zero_no_partition_no_record_key_inner(&mut send_ctx().await, &RustNativeFactory).await;
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn test_send_compressed_message_with_create_time() {
        send_compressed_message_with_create_time_inner(&mut send_ctx().await, &RustNativeFactory).await;
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn test_send_non_compressed_message_with_create_time() {
        send_non_compressed_message_with_create_time_inner(&mut send_ctx().await, &RustNativeFactory).await;
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn test_send_compressed_message_with_log_append_time() {
        send_compressed_message_with_log_append_time_inner(&mut send_ctx().await, &RustNativeFactory).await;
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn test_send_non_compressed_message_with_log_append_time() {
        send_non_compressed_message_with_log_append_time_inner(&mut send_ctx().await, &RustNativeFactory).await;
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn test_send_with_invalid_before_and_after_timestamp() {
        send_with_invalid_before_and_after_timestamp_inner(&mut send_ctx().await, &RustNativeFactory).await;
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn test_valid_before_and_after_timestamps_at_threshold() {
        valid_before_and_after_timestamps_at_threshold_inner(&mut send_ctx().await, &RustNativeFactory).await;
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn test_valid_before_and_after_timestamps_within_threshold() {
        valid_before_and_after_timestamps_within_threshold_inner(&mut send_ctx().await, &RustNativeFactory).await;
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn test_non_blocking_producer() {
        non_blocking_producer_inner(&mut send_ctx().await, &RustNativeFactory).await;
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn test_send_record_batch_with_max_request_size_and_higher() {
        send_record_batch_with_max_request_size_and_higher_inner(&mut send_ctx().await, &RustNativeFactory).await;
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn test_compression() {
        compression_inner(&mut ctx().await, &RustNativeFactory).await;
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn test_delivery_callback_logs_metadata() {
        delivery_callback_logs_metadata_inner(&mut ctx().await, &RustNativeFactory).await;
    }
}
