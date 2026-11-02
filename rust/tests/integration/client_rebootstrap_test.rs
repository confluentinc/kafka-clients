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
//! `kafka/clients/clients-integration-tests/src/test/java/org/apache/kafka/clients/ClientRebootstrapTest.java`
//! (Apache Kafka 4.3.1).
//!
//! Every Java test is a `@ClusterTest(brokers = 2, types = {Type.KRAFT})`: two
//! broker-only nodes plus one isolated controller, which here is
//! [`ClusterConfig::kraft_dedicated`]`(2, 1)`. The clients bootstrap against
//! both brokers; each test stops broker 0 before the client bootstraps (so the
//! client only learns about broker 1), then stops broker 1 and restarts
//! broker 0. Only `metadata.recovery.strategy=rebootstrap` (the default) lets
//! the client find broker 0 again through the bootstrap list.
//!
//! # Methods classification
//!
//! ## Translated
//!
//! - `testAdminRebootstrap` → `test_admin_rebootstrap`
//! - `testAdminRebootstrapDisabled` → `test_admin_rebootstrap_disabled`
//! - `testProducerRebootstrap` → `test_producer_rebootstrap`
//! - `testProducerRebootstrapDisabled` → `test_producer_rebootstrap_disabled`
//! - `testConsumerRebootstrap` → `test_consumer_rebootstrap`
//! - `testConsumerRebootstrapDisabled` → `test_consumer_rebootstrap_disabled`
//!
//! ## SKIPped
//!
//! - `testClassicConsumerRebootstrap` / `testClassicConsumerRebootstrapDisabled`
//!   — classic-protocol arms (`consumer-threading.md` §20).
//!
//! # Backends
//!
//! The test bodies are generic over the backend factory, so that the client
//! under test (the admin client, the producer or the consumer) could also run
//! on the Python (sync, asyncio) and C backends; the broker stops and starts
//! and the admin, producer and consumer the test only uses for setup are native
//! fixtures. Today every test runs on the native client only
//! (`MULTILANGUAGE: harness need`): a backend container bootstraps by broker
//! container hostname, and Docker's DNS has no entry for a stopped container.
//! Each test stops broker 0 before its client bootstraps, so a containerized
//! client drops broker 0 from its bootstrap list as unresolvable (as Java's
//! `ClientUtils.parseAndValidateAddresses` does) and cannot rebootstrap to it.
//! On the first runs every positive test failed on the Python backends that
//! way, and every `...Disabled` test passed without telling the two strategies
//! apart. The native client bootstraps through host proxies that keep their
//! address while the broker is down.
//!
//! The two `...Disabled` tests assert Java's `Future.get(5, SECONDS)`
//! throwing `TimeoutException`: natively, the record future's or the admin
//! future's `get_with_timeout` fails with `LocalTimeout`. A containerized
//! backend's call returns only once it completes, so on that branch the
//! producer test bounds the whole `send` instead. The admin test keeps its
//! native-only body, since an `AdminBackend` call awaits its result whole.

use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

use confluent_kafka::admin::{Admin, AdminClient, AdminClientConfig, CreateTopicsOptions, ListTopicsOptions, NewTopic};
use confluent_kafka::common::Error;
use confluent_kafka::common::TopicPartition;
use confluent_kafka::common::serialization::ByteArraySerializer;
use confluent_kafka::consumer::DynConsumer;
use confluent_kafka::producer::{KafkaProducer, Producer, ProducerConfig, ProducerRecord};

use crate::common::admin_backend::AdminBackend;
use crate::common::backend_factory::{
    AdminBackendFactory, ConsumerBackendFactory, ProducerBackendFactory, RustNativeFactory,
};
use crate::common::cluster_config::ClusterConfig;
use crate::common::test_context::TestContext;
use crate::common::test_utils::create_topic;

/// Java `TOPIC`.
const TOPIC: &str = "topic";
/// Java `PARTITIONS`.
const PARTITIONS: i32 = 1;
/// Java `REPLICAS` (also the broker count).
const REPLICAS: i16 = 2;
const BROKER0: i32 = 0;
const BROKER1: i32 = 1;

/// The `@ClusterTest` of the producer and consumer tests: two brokers, one
/// isolated controller, unclean leader election enabled (broker 0 comes back
/// with a stale or empty log while broker 1, the only ISR member, is down) and
/// `offsets.topic.replication.factor=2`.
fn rebootstrap_cluster_config() -> ClusterConfig {
    ClusterConfig::kraft_dedicated(REPLICAS as u16, 1).set_server_properties(BTreeMap::from([
        ("KAFKA_UNCLEAN_LEADER_ELECTION_ENABLE".to_string(), "true".to_string()),
        ("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR".to_string(), "2".to_string()),
    ]))
}

/// The `@ClusterTest` of the admin tests: two brokers, one isolated
/// controller and `offsets.topic.replication.factor=2`, without the unclean
/// leader election the producer and consumer tests add.
fn admin_rebootstrap_cluster_config() -> ClusterConfig {
    ClusterConfig::kraft_dedicated(REPLICAS as u16, 1).set_server_properties(BTreeMap::from([(
        "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR".to_string(),
        "2".to_string(),
    )]))
}

/// The PLAINTEXT bootstrap list (both brokers) the backend under test reaches:
/// the CONTAINER listener for a containerized backend, host loopback otherwise.
fn backend_bootstrap(ctx: &TestContext, needs_container_bootstrap: bool) -> String {
    if needs_container_bootstrap {
        ctx.container_bootstrap_servers().to_string()
    } else {
        ctx.bootstrap_servers().to_string()
    }
}

/// `bootstrap.servers` set to `bootstrap`, plus `overrides`.
fn client_props(bootstrap: &str, overrides: &[(&str, &str)]) -> HashMap<String, String> {
    let mut props = HashMap::from([("bootstrap.servers".to_string(), bootstrap.to_string())]);
    props.extend(overrides.iter().map(|(k, v)| (k.to_string(), v.to_string())));
    props
}

/// Java `clusterInstance.admin()`, the native fixture that creates the topic
/// of the producer and consumer tests.
fn admin(bootstrap: &str) -> Box<dyn Admin> {
    AdminClient::create(AdminClientConfig::new(&client_props(bootstrap, &[])).expect("valid admin config"))
        .expect("admin client")
}

/// Java `clusterInstance.admin(overrides)` for the admin under test,
/// bootstrapping against every broker.
async fn admin_under_test<F: AdminBackendFactory>(
    ctx: &TestContext,
    factory: &F,
    overrides: &[(&str, &str)],
) -> F::Admin {
    factory
        .create(client_props(
            &backend_bootstrap(ctx, factory.needs_container_bootstrap()),
            overrides,
        ))
        .await
        .unwrap_or_else(|e| panic!("{} backend: admin client: {e}", factory.name()))
}

/// Java `admin.createTopics(List.of(new NewTopic(TOPIC, PARTITIONS, (short) REPLICAS)))`,
/// whose result Java never reads: with broker 0 down the request may fail for
/// want of a second replica, and neither test depends on its outcome. Run
/// beside the first `listTopics` ([`create_topic_and_list_topics`]): Java sends
/// it and goes on to `listTopics`; the join also waits for its (quick) reply.
async fn create_topic_unawaited<A: AdminBackend>(admin: &A, topic: &str) {
    let _ = admin
        .create_topics(
            &[NewTopic::with_num_partitions_replication_factor(
                topic.to_string(),
                Some(PARTITIONS),
                Some(REPLICAS),
            )],
            CreateTopicsOptions::new(),
        )
        .await;
}

/// Java `assertDoesNotThrow(() -> admin.listTopics().names().get(timeout, TimeUnit.SECONDS).contains(TOPIC))`:
/// the call must succeed within `timeout`; Java discards the `contains` result.
async fn assert_list_topics_succeeds<A: AdminBackend>(admin: &A, timeout: Duration) {
    let backend = admin.name();
    match tokio::time::timeout(timeout, admin.list_topics(ListTopicsOptions::new())).await {
        Ok(Ok(_)) => {},
        Ok(Err(err)) => panic!("{backend} backend: listTopics failed: {err:?}"),
        Err(_) => panic!("{backend} backend: listTopics did not complete within {timeout:?}"),
    }
}

/// The unread `createTopics` and the first `listTopics` of the admin tests.
async fn create_topic_and_list_topics<A: AdminBackend>(admin: &A, topic: &str, timeout: Duration) {
    tokio::join!(
        create_topic_unawaited(admin, topic),
        assert_list_topics_succeeds(admin, timeout)
    );
}

/// Translated from `ClientRebootstrapTest#testAdminRebootstrap`.
async fn admin_rebootstrap<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let backend = factory.name();
    let topic = ctx.topic(TOPIC);
    let timeout = Duration::from_secs(60);

    ctx.cluster().shutdown_broker(BROKER0).await;

    let admin = admin_under_test(ctx, factory, &[]).await;
    // Only the broker 1 is available for the admin client during the bootstrap.
    create_topic_and_list_topics(&admin, &topic, timeout).await;

    ctx.cluster().shutdown_broker(BROKER1).await;
    ctx.cluster().start_broker(BROKER0).await;

    // The broker 1, originally cached during the bootstrap, is offline.
    // However, the broker 0 from the bootstrap list is online.
    // Should be able to list topics again.
    assert_list_topics_succeeds(&admin, timeout).await;
    admin
        .close(None)
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: admin close: {e}"));
}

// MULTILANGUAGE: harness need, see the module docs.
#[tokio::test(flavor = "multi_thread")]
async fn test_admin_rebootstrap() {
    let mut ctx = TestContext::new(admin_rebootstrap_cluster_config()).await;
    admin_rebootstrap(&mut ctx, &RustNativeFactory).await;
}

/// Translated from `ClientRebootstrapTest#testAdminRebootstrapDisabled`.
///
/// Unlike its siblings this body is written for the native admin client only:
/// Java's `Future.get(5, SECONDS)` is the native `KafkaFuture`'s
/// `get_with_timeout`, whose `LocalTimeout` error is the class Java's
/// `assertThrows` checks, and an `AdminBackend` call awaits its result whole.
/// It becomes a scenario with its siblings once the harness need is met.
// MULTILANGUAGE: harness need, see the module docs.
#[tokio::test(flavor = "multi_thread")]
async fn test_admin_rebootstrap_disabled() {
    let mut ctx = TestContext::new(admin_rebootstrap_cluster_config()).await;
    let topic = ctx.topic(TOPIC);
    let bootstrap = ctx.bootstrap_servers().to_string();

    ctx.cluster().shutdown_broker(BROKER0).await;

    let props = client_props(&bootstrap, &[("metadata.recovery.strategy", "none")]);
    let admin = AdminClient::create(AdminClientConfig::new(&props).expect("valid admin config")).expect("admin client");
    // Java's unread `createTopics` (see `create_topic_unawaited`).
    let _ = admin.create_topics(&[NewTopic::with_num_partitions_replication_factor(
        topic.clone(),
        Some(PARTITIONS),
        Some(REPLICAS),
    )]);

    // Only the broker 1 is available for the admin client during the bootstrap.
    if let Err(err) = admin.list_topics().names().get_with_timeout(Duration::from_secs(60)).await {
        panic!("listTopics failed: {err:?}");
    }

    ctx.cluster().shutdown_broker(BROKER1).await;
    ctx.cluster().start_broker(BROKER0).await;

    // The broker 1, originally cached during the bootstrap, is offline.
    // As a result, the admin client will throw a TimeoutException when trying to get list of the topics.
    //
    // Java's `Future.get(5, SECONDS)` throws `java.util.concurrent.TimeoutException`,
    // whose Rust counterpart is `Error::LocalTimeout` (`KafkaFutureImpl::get_with_timeout`).
    match admin.list_topics().names().get_with_timeout(Duration::from_secs(5)).await {
        Err(err) if err.is_local_timeout_error() => {},
        other => panic!("expected a local TimeoutException, got {other:?}"),
    }
    // Since the brokers cached during the bootstrap are offline, the admin client needs to wait the default timeout for other threads.
    admin.close_with_timeout(Duration::ZERO).await;
}

/// Java `clusterInstance.producer(overrides)` — byte-array serializers,
/// bootstrapping against every broker: the native fixture of the consumer
/// tests.
fn producer(bootstrap: &str, overrides: &[(&str, &str)]) -> KafkaProducer<Vec<u8>, Vec<u8>> {
    let config = ProducerConfig::new(&client_props(bootstrap, overrides)).expect("valid producer config");
    KafkaProducer::new(config, ByteArraySerializer::default(), ByteArraySerializer::default()).expect("build producer")
}

/// Java `clusterInstance.producer(overrides)` for the producer under test.
async fn producer_under_test<F: ProducerBackendFactory>(
    ctx: &TestContext,
    factory: &F,
    overrides: &[(&str, &str)],
) -> F::Producer {
    factory
        .create(client_props(
            &backend_bootstrap(ctx, factory.needs_container_bootstrap()),
            overrides,
        ))
        .await
        .unwrap_or_else(|e| panic!("{} backend: build producer: {e}", factory.name()))
}

/// Java `clusterInstance.consumer(overrides)` for the consumer under test —
/// byte-array deserializers, bootstrapping against every broker. The Java arm
/// under test sets `group.protocol=consumer`.
async fn consumer_under_test<F: ConsumerBackendFactory>(
    ctx: &TestContext,
    factory: &F,
    overrides: &[(&str, &str)],
) -> Box<dyn DynConsumer<Vec<u8>, Vec<u8>>> {
    let mut props = client_props(&backend_bootstrap(ctx, factory.needs_container_bootstrap()), overrides);
    props.insert("group.protocol".to_string(), "consumer".to_string());
    factory
        .create(props)
        .await
        .unwrap_or_else(|e| panic!("{} backend: build consumer: {e}", factory.name()))
}

fn record(topic: &str, value: &str) -> ProducerRecord<Vec<u8>, Vec<u8>> {
    ProducerRecord::new(topic.to_string(), Some(value.as_bytes().to_vec()))
}

/// `producer.send(record).get()`, returning the record's offset.
async fn send_and_get_offset<P: Producer<Vec<u8>, Vec<u8>>>(
    producer: &P,
    backend: &str,
    topic: &str,
    value: &str,
) -> i64 {
    producer
        .send(record(topic, value))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: send: {e}"))
        .get()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: record acknowledged: {e}"))
        .offset()
}

/// Java `TestUtils.waitForCondition(() -> consumer.poll(Duration.ofMillis(100)).count() == 1,
/// 10 * 1000, "Failed to poll data.")`.
async fn wait_for_one_polled_record(consumer: &mut dyn DynConsumer<Vec<u8>, Vec<u8>>, backend: &str) {
    let deadline = Instant::now() + Duration::from_millis(10 * 1000);
    loop {
        let count = consumer
            .poll(Duration::from_millis(100))
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: poll: {e}"))
            .count();
        if count == 1 {
            return;
        }
        assert!(Instant::now() < deadline, "{backend} backend: Failed to poll data.");
    }
}

/// Translated from `ClientRebootstrapTest#testProducerRebootstrap`.
async fn producer_rebootstrap<F: ProducerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let backend = factory.name();
    let topic = ctx.topic(TOPIC);
    {
        let admin = admin(ctx.bootstrap_servers());
        create_topic(admin.as_ref(), &topic, PARTITIONS, REPLICAS).await;
        admin.close().await;
    }

    // It's ok to shut the leader down, cause the reelection is small enough to the producer timeout.
    ctx.cluster().shutdown_broker(BROKER0).await;

    let producer = producer_under_test(ctx, factory, &[]).await;
    // Only the broker 1 is available for the producer during the bootstrap.
    assert_eq!(
        0,
        send_and_get_offset(&producer, backend, &topic, "value 0").await,
        "{backend} backend"
    );

    ctx.cluster().shutdown_broker(BROKER1).await;
    ctx.cluster().start_broker(BROKER0).await;

    // Current broker 1 is offline.
    // However, the broker 0 from the bootstrap list is online.
    // Should be able to produce records.
    assert_eq!(
        0,
        send_and_get_offset(&producer, backend, &topic, "value 1").await,
        "{backend} backend"
    );
    producer
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: producer close: {e}"));
}

// MULTILANGUAGE: harness need, see the module docs.
#[tokio::test(flavor = "multi_thread")]
async fn test_producer_rebootstrap() {
    let mut ctx = TestContext::new(rebootstrap_cluster_config()).await;
    producer_rebootstrap(&mut ctx, &RustNativeFactory).await;
}

/// Translated from `ClientRebootstrapTest#testProducerRebootstrapDisabled`.
async fn producer_rebootstrap_disabled<F: ProducerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let backend = factory.name();
    let topic = ctx.topic(TOPIC);
    {
        let admin = admin(ctx.bootstrap_servers());
        create_topic(admin.as_ref(), &topic, PARTITIONS, REPLICAS).await;
        admin.close().await;
    }

    // It's ok to shut the leader down, cause the reelection is small enough to the producer timeout.
    ctx.cluster().shutdown_broker(BROKER0).await;

    let producer = producer_under_test(ctx, factory, &[("metadata.recovery.strategy", "none")]).await;

    // Only the broker 1 is available for the producer during the bootstrap.
    assert_eq!(
        0,
        send_and_get_offset(&producer, backend, &topic, "value 0").await,
        "{backend} backend"
    );

    ctx.cluster().shutdown_broker(BROKER1).await;
    ctx.cluster().start_broker(BROKER0).await;

    // The broker 1, originally cached during the bootstrap, is offline.
    // As a result, the producer will throw a TimeoutException when trying to send a message.
    //
    // Java's `Future.get(5, SECONDS)` throws `java.util.concurrent.TimeoutException`,
    // whose Rust counterpart is `Error::LocalTimeout` (see
    // `FutureRecordMetadata::get_with_timeout`); Java's lambda includes the
    // `send`, so the native `send` has no bound of its own. A containerized
    // backend's `send` returns only once the record completes, so there the
    // record must not complete within the 5 s (see the module docs).
    let five_seconds = Duration::from_secs(5);
    if factory.needs_container_bootstrap() {
        if let Ok(result) = tokio::time::timeout(five_seconds, producer.send(record(&topic, "value 1"))).await {
            panic!("{backend} backend: expected the record not to complete within 5 s, got {result:?}");
        }
    } else {
        let future = producer
            .send(record(&topic, "value 1"))
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: send: {e:?}"));
        match future.get_with_timeout(five_seconds).await {
            Err(err @ Error::LocalTimeout(_)) => assert_eq!(
                err.to_string(),
                Error::local_timeout("Timeout after waiting for 5000 ms.").to_string(),
                "{backend} backend"
            ),
            other => panic!("{backend} backend: expected a local TimeoutException, got {other:?}"),
        }
    }
    // Since the brokers cached during the bootstrap are offline, the producer needs to wait the default timeout for other threads.
    producer
        .close_with_timeout(Duration::ZERO)
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: producer close: {e}"));
}

// MULTILANGUAGE: harness need, see the module docs.
#[tokio::test(flavor = "multi_thread")]
async fn test_producer_rebootstrap_disabled() {
    let mut ctx = TestContext::new(rebootstrap_cluster_config()).await;
    producer_rebootstrap_disabled(&mut ctx, &RustNativeFactory).await;
}

/// Java `ClientRebootstrapTest#consumerRebootstrap`, called as
/// `consumerRebootstrap(clusterInstance, GroupProtocol.CONSUMER)`.
async fn consumer_rebootstrap<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let backend = factory.name();
    let topic = ctx.topic(TOPIC);
    let bootstrap = ctx.bootstrap_servers().to_string();
    {
        let admin = admin(&bootstrap);
        create_topic(admin.as_ref(), &topic, PARTITIONS, REPLICAS).await;
        admin.close().await;
    }
    let partitions = vec![TopicPartition::new(topic.clone(), 0)];

    {
        let producer = producer(&bootstrap, &[("acks", "-1")]);
        assert_eq!(0, send_and_get_offset(&producer, "rust", &topic, "value 0").await);
        producer.close().await.expect("producer close");
    }

    ctx.cluster().shutdown_broker(BROKER0).await;

    let mut consumer = consumer_under_test(ctx, factory, &[]).await;
    // Only the server 1 is available for the consumer during the bootstrap.
    consumer
        .assign(partitions.clone())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: assign: {e}"));
    consumer
        .seek_to_beginning(&partitions)
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: seek to beginning: {e}"));
    wait_for_one_polled_record(consumer.as_mut(), backend).await;

    // Bring back the server 0 and shut down 1.
    ctx.cluster().shutdown_broker(BROKER1).await;
    ctx.cluster().start_broker(BROKER0).await;

    {
        let producer = producer(&bootstrap, &[("acks", "-1")]);
        assert_eq!(1, send_and_get_offset(&producer, "rust", &topic, "value 1").await);
        producer.close().await.expect("producer close");
    }

    // The server 1 originally cached during the bootstrap, is offline.
    // However, the server 0 from the bootstrap list is online.
    wait_for_one_polled_record(consumer.as_mut(), backend).await;
    consumer
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer close: {e}"));
}

/// Translated from `ClientRebootstrapTest#testConsumerRebootstrap`, the `GroupProtocol.CONSUMER` arm.
// MULTILANGUAGE: harness need, see the module docs.
#[tokio::test(flavor = "multi_thread")]
async fn test_consumer_rebootstrap() {
    let mut ctx = TestContext::new(rebootstrap_cluster_config()).await;
    consumer_rebootstrap(&mut ctx, &RustNativeFactory).await;
}

/// Java `ClientRebootstrapTest#consumerRebootstrapDisabled`, called as
/// `consumerRebootstrapDisabled(clusterInstance, GroupProtocol.CONSUMER)`.
async fn consumer_rebootstrap_disabled<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let backend = factory.name();
    let topic = ctx.topic(TOPIC);
    let bootstrap = ctx.bootstrap_servers().to_string();
    {
        let admin = admin(&bootstrap);
        create_topic(admin.as_ref(), &topic, PARTITIONS, REPLICAS).await;
        admin.close().await;
    }
    let tp = TopicPartition::new(topic.clone(), 0);

    {
        let producer = producer(&bootstrap, &[("acks", "-1")]);
        assert_eq!(0, send_and_get_offset(&producer, "rust", &topic, "value 0").await);
        producer.close().await.expect("producer close");
    }

    ctx.cluster().shutdown_broker(BROKER0).await;

    let mut consumer = consumer_under_test(ctx, factory, &[("metadata.recovery.strategy", "none")]).await;
    // Only the server 1 is available for the consumer during the bootstrap.
    consumer
        .assign(vec![tp.clone()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: assign: {e}"));
    consumer
        .seek_to_beginning(&[tp])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: seek to beginning: {e}"));
    wait_for_one_polled_record(consumer.as_mut(), backend).await;

    // Bring back the server 0 and shut down 1.
    ctx.cluster().shutdown_broker(BROKER1).await;
    ctx.cluster().start_broker(BROKER0).await;

    {
        let producer = producer(&bootstrap, &[("acks", "-1")]);
        assert_eq!(1, send_and_get_offset(&producer, "rust", &topic, "value 1").await);
        producer.close().await.expect("producer close");
    }

    // The server 1 originally cached during the bootstrap, is offline.
    // However, the server 0 from the bootstrap list is online.
    assert_eq!(
        0,
        consumer
            .poll(Duration::from_millis(100))
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: poll: {e}"))
            .count(),
        "{backend} backend"
    );
    consumer
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer close: {e}"));
}

/// Translated from `ClientRebootstrapTest#testConsumerRebootstrapDisabled`, the `GroupProtocol.CONSUMER` arm.
// MULTILANGUAGE: harness need, see the module docs.
#[tokio::test(flavor = "multi_thread")]
async fn test_consumer_rebootstrap_disabled() {
    let mut ctx = TestContext::new(rebootstrap_cluster_config()).await;
    consumer_rebootstrap_disabled(&mut ctx, &RustNativeFactory).await;
}
