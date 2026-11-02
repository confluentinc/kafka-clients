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
//! `kafka/clients/clients-integration-tests/src/test/java/org/apache/kafka/clients/consumer/PlaintextConsumerCloseTest.java`
//! (Apache Kafka 4.3.1).
//!
//! The translated tests run as multilanguage scenarios: each one runs against
//! the native Rust consumer and, through the gRPC backends, the Python (sync
//! and asyncio) and C consumers ([`multilanguage_consumer_test!`]); each name
//! below gains the macro's backend suffix (`__rust`, `__grpc_python`,
//! `__grpc_python_async`, `__grpc_c`). The setup producer and admin client
//! are native fixtures; only the consumer, and the duration of its close, are
//! under test. On a gRPC arm the measured close includes one gRPC round trip,
//! which is far below the margins the assertions use.
//!
//! # Methods classification
//!
//! ## Translated (CONSUMER / KIP-848 arm)
//!
//! - `testAsyncConsumerCloseWithDefaultTakesAtLeastFetchMaxWaitMs` →
//!   `test_async_consumer_close_with_default_takes_at_least_fetch_max_wait_ms`
//! - `testAsyncConsumerCloseWithTimeoutIgnoresFetchMaxWaitMs` →
//!   `test_async_consumer_close_with_timeout_ignores_fetch_max_wait_ms`
//!
//! ## SKIPped (classic group protocol — `consumer-threading.md` §20)
//!
//! - `testClassicConsumerCloseWithDefaultTakesAtLeastFetchMaxWaitMs`
//! - `testClassicConsumerCloseWithTimeoutIgnoresFetchMaxWaitMs`
//!
//! # Cluster
//!
//! Java's `@ClusterTestDefaults` runs `BROKER_COUNT = 3` brokers with
//! `offsets.topic.num.partitions=1`, `group.min.session.timeout.ms=100`,
//! `group.max.session.timeout.ms=60000` and
//! `group.initial.rebalance.delay.ms=10`. [`kip848_3_broker`] is a documented
//! superset of exactly those properties (its extra knobs only enable KIP-848,
//! shape the offsets topic and speed up heartbeats), so these tests share its
//! pooled container instead of starting a new one.

use std::collections::HashMap;
use std::time::Duration;
use std::time::Instant;

use confluent_kafka::admin::AdminClient;
use confluent_kafka::admin::AdminClientConfig;
use confluent_kafka::common::TopicPartition;
use confluent_kafka::common::serialization::ByteArraySerializer;
use confluent_kafka::consumer::CloseOptions;
use confluent_kafka::consumer::ConsumerConfig;
use confluent_kafka::producer::KafkaProducer;
use confluent_kafka::producer::Producer;
use confluent_kafka::producer::ProducerConfig;
use confluent_kafka::producer::ProducerRecord;

use crate::common::backend_factory::ConsumerBackendFactory;
use crate::common::cluster_config::kip848_3_broker;
use crate::common::consumer_assignment_poller::BytesConsumer;
use crate::common::test_context::TestContext;
use crate::common::test_utils;
use crate::multilanguage_consumer_test;

/// `ConsumerConfig.DEFAULT_FETCH_MAX_WAIT_MS`.
const DEFAULT_FETCH_MAX_WAIT_MS: u128 = ConsumerConfig::DEFAULT_FETCH_MAX_WAIT_MS as u128;

/// `ClientsTestUtils.consumeRecords`'s `waitForCondition` timeout.
const CONSUME_RECORDS_TIMEOUT: Duration = Duration::from_millis(60_000);

/// Which close overload `calculateConsumerCloseDelay` drives — Java passes a
/// `java.util.function.Consumer<Consumer<byte[], byte[]>>` lambda; an async
/// closure over `&mut Box<dyn DynConsumer>` is awkward in Rust, so the two
/// lambdas the Java tests use are enumerated instead.
enum CloseOperation {
    /// `Consumer::close`.
    Default,
    /// `c -> c.close(CloseOptions.timeout(Duration.ofMillis(timeoutMs)))`.
    WithTimeout(Duration),
}

/// `cluster.producer()` as used by `ClientsTestUtils.sendRecords`.
fn create_producer(ctx: &TestContext) -> KafkaProducer<Vec<u8>, Vec<u8>> {
    let props = HashMap::from([("bootstrap.servers".to_string(), ctx.bootstrap_servers().to_string())]);
    KafkaProducer::new(
        ProducerConfig::new(&props).expect("valid producer config"),
        ByteArraySerializer::default(),
        ByteArraySerializer::default(),
    )
    .expect("KafkaProducer::new should succeed")
}

fn current_time_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before Unix epoch")
        .as_millis() as i64
}

/// `ClientsTestUtils.sendRecords(cluster, tp, numRecords)`: a fresh
/// producer sends `numRecords` records with 1 ms timestamp increments, then
/// flushes and closes.
async fn send_records(ctx: &TestContext, tp: &TopicPartition, num_records: usize) {
    let producer = create_producer(ctx);
    let starting_timestamp = current_time_ms();
    for i in 0..num_records {
        let record = ProducerRecord::with_partition_timestamp_key(
            tp.topic().to_string(),
            Some(tp.partition()),
            Some(starting_timestamp + i as i64),
            Some(format!("key {i}").into_bytes()),
            Some(format!("value {i}").into_bytes()),
        )
        .expect("valid producer record");
        <KafkaProducer<Vec<u8>, Vec<u8>> as Producer<Vec<u8>, Vec<u8>>>::send(&producer, record)
            .await
            .expect("send should not fail");
    }
    producer.flush().await.expect("producer flush");
    <KafkaProducer<Vec<u8>, Vec<u8>> as Producer<Vec<u8>, Vec<u8>>>::close(&producer)
        .await
        .expect("producer close");
}

/// `ClientsTestUtils.consumeRecords(consumer, numRecords)`:
/// `poll(100ms)` until at least `numRecords` records are consumed, failing
/// after 60 s.
async fn consume_records(consumer: &mut BytesConsumer, num_records: usize, backend: &str) {
    let deadline = Instant::now() + CONSUME_RECORDS_TIMEOUT;
    let mut consumed = 0;
    while consumed < num_records {
        assert!(
            Instant::now() < deadline,
            "Timed out before consuming expected {num_records} records. ({backend} backend)"
        );
        let records = consumer
            .poll(Duration::from_millis(100))
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: poll should succeed: {e}"));
        consumed += records.count();
    }
}

/// `PlaintextConsumerCloseTest#calculateConsumerCloseDelay`, for
/// the CONSUMER group protocol, with the consumer built by `factory`. Returns
/// the wall-clock duration of the close call alone, in milliseconds.
async fn calculate_consumer_close_delay<F: ConsumerBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
    close_operation: CloseOperation,
) -> u128 {
    let backend = factory.name();
    let topic_name = ctx.topic("calculate-consumer-close-delay");
    let group_id = ctx.group_id("group_test");
    let num_records = 100;
    let admin = AdminClient::create(
        AdminClientConfig::new(&HashMap::from([(
            "bootstrap.servers".to_string(),
            ctx.bootstrap_servers().to_string(),
        )]))
        .expect("valid admin config"),
    )
    .expect("admin client");
    test_utils::create_topic(admin.as_ref(), &topic_name, 2, 2).await;
    test_utils::wait_for_partition_leaders(admin.as_ref(), &topic_name, 0..2).await;

    send_records(ctx, &TopicPartition::new(topic_name.clone(), 0), num_records).await;

    let mut props = HashMap::from([
        ("group.protocol".to_string(), "consumer".to_string()),
        ("auto.offset.reset".to_string(), "earliest".to_string()),
        ("group.id".to_string(), group_id),
        (
            "bootstrap.servers".to_string(),
            ctx.protocol_bootstrap_servers_for(factory.needs_container_bootstrap())
                .to_string(),
        ),
    ]);
    ctx.apply_security(&mut props);
    let mut consumer: BytesConsumer = factory
        .create(props)
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: create consumer: {e}"));

    consumer
        .subscribe_with_topics(vec![topic_name])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: subscribe should succeed: {e}"));
    consume_records(&mut consumer, num_records, backend).await;

    let start = Instant::now();
    // Java's lambda (`Consumer::close` / `close(CloseOptions)`) cannot fail the
    // measurement: any close error would escape as an unchecked exception and
    // fail the test, so the Rust result is asserted Ok as well.
    match close_operation {
        CloseOperation::Default => consumer.close().await,
        CloseOperation::WithTimeout(timeout) => {
            consumer.close_with_options(CloseOptions::new_with_timeout(timeout)).await
        },
    }
    .unwrap_or_else(|e| panic!("{backend} backend: close should succeed: {e}"));
    let close_ms = start.elapsed().as_millis();

    admin.close().await;
    close_ms
}

/// Translates `PlaintextConsumerCloseTest#testAsyncConsumerCloseWithDefaultTakesAtLeastFetchMaxWaitMs`,
/// via `testCloseWithDefaultTakesAtLeastFetchMaxWaitMs`.
async fn async_consumer_close_with_default_takes_at_least_fetch_max_wait_ms<F: ConsumerBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
) {
    let close_ms = calculate_consumer_close_delay(ctx, factory, CloseOperation::Default).await;
    assert!(
        close_ms >= DEFAULT_FETCH_MAX_WAIT_MS,
        "Closing a consumer with the default close() should take longer than {DEFAULT_FETCH_MAX_WAIT_MS} ms, but actually took {close_ms} ms ({} backend)",
        factory.name()
    );
    ctx.cleanup().await;
}

multilanguage_consumer_test!(
    test_async_consumer_close_with_default_takes_at_least_fetch_max_wait_ms,
    async_consumer_close_with_default_takes_at_least_fetch_max_wait_ms,
    kip848_3_broker(2)
);

/// Translates `PlaintextConsumerCloseTest#testAsyncConsumerCloseWithTimeoutIgnoresFetchMaxWaitMs`,
/// via `testCloseWithTimeoutIgnoresFetchMaxWaitMs`.
async fn async_consumer_close_with_timeout_ignores_fetch_max_wait_ms<F: ConsumerBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
) {
    let timeout_ms = 100;

    // Close the Consumer with a specified timeout that's much shorter than the default fetch timeout
    // to ensure the fetch.max.wait.ms is effectively ignored.
    let close_ms =
        calculate_consumer_close_delay(ctx, factory, CloseOperation::WithTimeout(Duration::from_millis(timeout_ms)))
            .await;
    assert!(
        close_ms <= DEFAULT_FETCH_MAX_WAIT_MS,
        "Closing a consumer with a timeout of {timeout_ms} ms should take less than {DEFAULT_FETCH_MAX_WAIT_MS} ms, but actually took {close_ms} ms ({} backend)",
        factory.name()
    );
    ctx.cleanup().await;
}

multilanguage_consumer_test!(
    test_async_consumer_close_with_timeout_ignores_fetch_max_wait_ms,
    async_consumer_close_with_timeout_ignores_fetch_max_wait_ms,
    kip848_3_broker(2)
);
