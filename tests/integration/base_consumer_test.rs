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

//! Shared body of the security-protocol-parametrized consumer suites.
//!
//! Translates the one scenario both Java homes of the "base consumer" test
//! share, so the per-protocol suites stay thin exactly as they are in Java:
//!
//! - `org.apache.kafka.clients.ClientsTestUtils.BaseConsumerTestcase.testSimpleConsumption`
//!   (`ClientsTestUtils.java:376-392`), driven by
//!   `SaslPlainPlaintextConsumerTest` → [`super::sasl_plain_plaintext_consumer_test`].
//! - `kafka.api.BaseConsumerTest.testSimpleConsumption`
//!   (`BaseConsumerTest.scala:60-76`), driven by `SslConsumerTest` →
//!   [`super::ssl_consumer_test`].
//!
//! The two Java bodies are line-for-line the same (send 10000 records, assign +
//! `seek(0)`, verify every record, async-commit); they differ only in the
//! client configuration each harness supplies, which is why the protocol-
//! specific suites pass it in as [`SimpleConsumptionConfig`].
//!
//! Only the CONSUMER (KIP-848) group-protocol arm is translated
//! (`consumer-threading.md` §20).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use async_trait::async_trait;

use confluent_kafka::admin::Admin;
use confluent_kafka::admin::AdminClientConfig;
use confluent_kafka::admin::KafkaAdminClient;
use confluent_kafka::common::Error;
use confluent_kafka::common::TopicPartition;
use confluent_kafka::common::record::TimestampType;
use confluent_kafka::common::serialization::ByteArraySerializer;
use confluent_kafka::common::serialization::Deserializer;
use confluent_kafka::consumer::Consumer;
use confluent_kafka::consumer::ConsumerConfig;
use confluent_kafka::consumer::KafkaConsumer;
use confluent_kafka::consumer::OffsetAndMetadata;
use confluent_kafka::consumer::OffsetCommitCallback;
use confluent_kafka::producer::KafkaProducer;
use confluent_kafka::producer::Producer;
use confluent_kafka::producer::ProducerConfig;
use confluent_kafka::producer::ProducerRecord;

use crate::common::cluster_config::{ClusterConfig, kip848_3_broker};
use crate::common::test_context::TestContext;
use crate::common::test_utils;

/// Bytes-typed `Consumer` trait object returned by
/// `KafkaConsumer::new::<Vec<u8>, Vec<u8>>`.
type BytesConsumer = dyn Consumer<Vec<u8>, Vec<u8>>;

/// Java's `BaseConsumerTestcase.BROKER_COUNT` / Scala's
/// `AbstractConsumerTest.brokerCount`.
const BROKER_COUNT: i16 = 3;

/// Java's `KEY_PREFIX` / `VALUE_PREFIX` (`ClientsTestUtils.java:62-63`).
const KEY_PREFIX: &str = "key ";
const VALUE_PREFIX: &str = "value ";

/// Broker shape shared by both Java suites: 3 brokers, one offsets-topic
/// partition, `group.min.session.timeout.ms=100`
/// (`SaslPlainPlaintextConsumerTest`'s `@ClusterTestDefaults`,
/// `AbstractConsumerTest.brokerPropertyOverrides`), KIP-848 enabled.
///
/// Every pooled container exposes the PLAINTEXT, SSL, SASL_PLAINTEXT and
/// SASL_SSL listeners (`tests/common/kafka_cluster.rs`), and the broker's
/// only SASL mechanism is PLAIN — which is exactly Java's `MECHANISMS`.
pub(super) fn cluster_config() -> ClusterConfig {
    kip848_3_broker(1)
}

/// The client configuration a protocol-specific suite supplies.
pub(super) struct SimpleConsumptionConfig {
    /// `bootstrap.servers` of the secured listener plus the
    /// `security.protocol` / `sasl.*` / `ssl.*` keys. Applied to the producer,
    /// the consumer and the topic-provisioning admin, as Java's harness does
    /// (`ClusterInstance.setClientSaslConfig`, Scala's per-client
    /// `securityProtocol`).
    pub(super) client_props: HashMap<String, String>,
    /// Producer overrides of the Java harness.
    pub(super) producer_props: HashMap<String, String>,
    /// Consumer overrides of the Java harness (group id, auto-commit, ...).
    pub(super) consumer_props: HashMap<String, String>,
}

/// Local byte-array deserializer (the crate exports `ByteArraySerializer` but
/// no symmetric `ByteArrayDeserializer`).
struct ByteArrayDeserializer;

impl Deserializer<Vec<u8>> for ByteArrayDeserializer {
    fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<Vec<u8>, Error> {
        Ok(data.to_vec())
    }
}

fn merged(base: &HashMap<String, String>, overrides: &HashMap<String, String>) -> HashMap<String, String> {
    let mut props = base.clone();
    props.extend(overrides.iter().map(|(k, v)| (k.clone(), v.clone())));
    props
}

/// Topic-provisioning admin over the same secured listener.
fn admin(config: &SimpleConsumptionConfig) -> Box<dyn Admin> {
    let mut props = config.client_props.clone();
    props.insert("client.id".to_string(), "base-consumer-test-admin".to_string());
    props.insert("request.timeout.ms".to_string(), "30000".to_string());
    props.insert("default.api.timeout.ms".to_string(), "30000".to_string());
    let admin_config = AdminClientConfig::new(&props).expect("valid admin config");
    Box::new(KafkaAdminClient::new(admin_config).expect("admin client"))
}

/// Java's `System.currentTimeMillis()`.
fn current_time_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after epoch")
        .as_millis() as i64
}

/// `ClientsTestUtils.sendRecords(producer, tp, numRecords, startingTimestamp)`
/// (`ClientsTestUtils.java:266-276`) / Scala `AbstractConsumerTest.sendRecords`
/// (`AbstractConsumerTest.scala:186-201`): record `i` carries timestamp
/// `startingTimestamp + i`, key `"key i"`, value `"value i"`; then `flush()`.
async fn send_records(
    config: &SimpleConsumptionConfig,
    tp: &TopicPartition,
    num_records: usize,
    starting_timestamp: i64,
) {
    let producer: KafkaProducer<Vec<u8>, Vec<u8>> = KafkaProducer::new(
        ProducerConfig::new(&merged(&config.client_props, &config.producer_props)).expect("valid producer config"),
        Box::new(ByteArraySerializer),
        Box::new(ByteArraySerializer),
    )
    .expect("Failed to build test producer");
    for i in 0..num_records {
        let record = ProducerRecord::with_partition_timestamp_key(
            tp.topic().to_string(),
            Some(tp.partition()),
            Some(starting_timestamp + i as i64),
            Some(format!("{KEY_PREFIX}{i}").into_bytes()),
            Some(format!("{VALUE_PREFIX}{i}").into_bytes()),
        )
        .expect("valid record");
        <KafkaProducer<Vec<u8>, Vec<u8>> as Producer<Vec<u8>, Vec<u8>>>::send(&producer, record)
            .await
            .expect("send should succeed");
    }
    producer.flush().await.expect("flush should succeed");
    producer.close().await.expect("producer close should succeed");
}

/// `ClientsTestUtils.consumeAndVerifyRecords(consumer, tp, numRecords,
/// startingOffset, startingKeyAndValueIndex, startingTimestamp)`
/// (`ClientsTestUtils.java:166-196`, `timestampIncrement = -1`) over
/// `consumeRecords` (`:74-87`): 100 ms polls until at least `num_records`
/// records arrive within 60 s, then the full per-record assertion set.
/// Records are verified as they arrive because `ConsumerRecord` is not `Clone`.
async fn consume_and_verify_records(
    consumer: &mut BytesConsumer,
    tp: &TopicPartition,
    num_records: usize,
    starting_offset: i64,
    starting_key_and_value_index: usize,
    starting_timestamp: i64,
) {
    let deadline = Instant::now() + Duration::from_millis(60_000);
    let mut consumed = 0usize;
    while consumed < num_records {
        assert!(
            Instant::now() < deadline,
            "Timed out before consuming expected {num_records} records."
        );
        let records = consumer.poll(Duration::from_millis(100)).await.expect("poll should succeed");
        for record in records {
            // Java collects every polled record but verifies only the first
            // `numRecords` of them.
            if consumed >= num_records {
                break;
            }
            let i = consumed;
            let offset = starting_offset + i as i64;
            assert_eq!(tp.topic(), record.topic());
            assert_eq!(tp.partition(), record.partition());
            assert_eq!(TimestampType::CreateTime, record.timestamp_type());
            assert_eq!(starting_timestamp + i as i64, record.timestamp(), "timestamp of record {i}");
            assert_eq!(offset, record.offset(), "offset of record {i}");
            let key_and_value_index = starting_key_and_value_index + i;
            let expected_key = format!("{KEY_PREFIX}{key_and_value_index}");
            let expected_value = format!("{VALUE_PREFIX}{key_and_value_index}");
            assert_eq!(Some(&expected_key.clone().into_bytes()), record.key(), "key of record {i}");
            assert_eq!(
                Some(&expected_value.clone().into_bytes()),
                record.value(),
                "value of record {i}"
            );
            // this is true only because K and V are byte arrays
            assert_eq!(expected_key.len() as i32, record.serialized_key_size());
            assert_eq!(expected_value.len() as i32, record.serialized_value_size());
            consumed += 1;
        }
    }
}

/// `ClientsTestUtils.RetryCommitCallback` (`ClientsTestUtils.java:500`):
/// resend on `RetriableCommitFailedException`, otherwise record completion and
/// the error. Java's callback resends itself through the consumer it holds; a
/// Rust callback cannot own the `&mut` consumer, so
/// [`send_and_await_async_commit`] performs the resend when it observes the
/// recorded retriable failure.
#[derive(Clone, Default)]
struct RetryCommitCallback {
    is_complete: Arc<AtomicUsize>,
    error: Arc<Mutex<Option<Error>>>,
    retriable: Arc<AtomicUsize>,
}

#[async_trait]
impl OffsetCommitCallback for RetryCommitCallback {
    async fn on_complete(&self, _offsets: &HashMap<TopicPartition, OffsetAndMetadata>, error: Option<&Error>) {
        match error {
            Some(Error::ConsumerRetriableCommitFailed(_)) => {
                self.retriable.fetch_add(1, Ordering::SeqCst);
            },
            other => {
                *self.error.lock().expect("commit error mutex poisoned") = other.cloned();
                self.is_complete.fetch_add(1, Ordering::SeqCst);
            },
        }
    }
}

/// `ClientsTestUtils.sendAndAwaitAsyncCommit(consumer, Optional.empty())`
/// (`ClientsTestUtils.java:309-322`): `commitAsync(callback)`, poll until the
/// callback fires (`waitForCondition`'s 15 s default), then
/// `assertEquals(Optional.empty(), commitCallback.error)`.
async fn send_and_await_async_commit(consumer: &mut BytesConsumer) {
    let callback = RetryCommitCallback::default();
    let handle: Arc<dyn OffsetCommitCallback> = Arc::new(callback.clone());
    consumer
        .commit_async_with_callback(Arc::clone(&handle))
        .await
        .expect("commit_async_with_callback should enqueue");
    let deadline = Instant::now() + Duration::from_millis(15_000);
    while Instant::now() < deadline {
        if callback.is_complete.load(Ordering::SeqCst) > 0 {
            let error = callback.error.lock().expect("commit error mutex poisoned").clone();
            assert!(error.is_none(), "expected Optional.empty() commit error, got {error:?}");
            return;
        }
        if callback.retriable.swap(0, Ordering::SeqCst) > 0 {
            consumer
                .commit_async_with_callback(Arc::clone(&handle))
                .await
                .expect("retriable resend should enqueue");
        }
        let _ = consumer.poll(Duration::from_millis(100)).await.expect("poll should succeed");
    }
    panic!("Failed to observe commit callback before timeout");
}

/// `BaseConsumerTestcase.testSimpleConsumption` (`ClientsTestUtils.java:376-392`)
/// / `BaseConsumerTest.testSimpleConsumption` (`BaseConsumerTest.scala:60-76`),
/// including the suites' `@BeforeEach` topic creation
/// (`cluster.createTopic(TOPIC, 2, BROKER_COUNT)`,
/// `AbstractConsumerTest.scala:84`).
///
/// Deviation: Java's fixed topic name `"topic"` becomes a per-test name because
/// clusters are pooled across tests here.
pub(super) async fn test_simple_consumption(ctx: &mut TestContext, config: SimpleConsumptionConfig) {
    let topic = ctx.topic("topic");
    let tp = TopicPartition::new(topic.clone(), 0);
    let admin = admin(&config);
    test_utils::create_topic(admin.as_ref(), &topic, 2, BROKER_COUNT).await;
    // Java's `createTopic` waits for every broker's metadata; the leader-ready
    // wait is the producer-relevant half (see `wait_for_partition_leaders`).
    test_utils::wait_for_partition_leaders(admin.as_ref(), &topic, 0..2).await;
    admin.close_with_timeout(Duration::from_secs(5)).await;

    let num_records = 10000;
    let starting_timestamp = current_time_ms();
    send_records(&config, &tp, num_records, starting_timestamp).await;

    let mut consumer = KafkaConsumer::new::<Vec<u8>, Vec<u8>>(
        ConsumerConfig::new(&merged(&config.client_props, &config.consumer_props)).expect("valid consumer config"),
        Box::new(ByteArrayDeserializer),
        Box::new(ByteArrayDeserializer),
    )
    .expect("KafkaConsumer::new should succeed");
    assert_eq!(0, consumer.assignment().len());
    consumer.assign(vec![tp.clone()]).await.expect("assign should succeed");
    assert_eq!(1, consumer.assignment().len());
    consumer.seek_with_offset(tp.clone(), 0).await.expect("seek should succeed");
    consume_and_verify_records(consumer.as_mut(), &tp, num_records, 0, 0, starting_timestamp).await;
    // check async commit callbacks
    send_and_await_async_commit(consumer.as_mut()).await;
    consumer.close().await.expect("consumer close should succeed");
}
