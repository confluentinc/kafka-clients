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

//! KIP-909 (`bootstrap.resolve.timeout.ms`) through the public API of all three
//! clients.
//!
//! Java has no integration test for KIP-909: its coverage is the unit tests in
//! `KafkaProducerTest`, `KafkaConsumerTest` and `KafkaAdminClientTest`, which
//! the Rust unit tests translate. This file is Rust-only (Milestone 17 Phase 2)
//! and covers what those cannot:
//!
//! - **Unresolvable bootstrap host, positive timeout:** every client is created,
//!   then fails with Java's `BootstrapResolutionException` message once the
//!   budget runs out, and keeps failing. These need no broker, but they run the
//!   production clients through the published crate surface.
//! - **Resolvable bootstrap host, positive timeout:** against a real broker, the
//!   asynchronous path bootstraps the client, which then works normally (a
//!   produce, a consume and an admin call).
//!
//! Every test is a multilanguage scenario: the client under test also runs on
//! the Python (sync, asyncio) and C backends ([`multilanguage_test!`],
//! [`multilanguage_consumer_test!`], [`multilanguage_admin_test!`]), and its
//! name gains the backend suffix (`__rust`, `__grpc_python`,
//! `__grpc_python_async`, `__grpc_c`). The `.invalid` host does not resolve
//! inside a backend container either. The resolvable-host test, which had all
//! three clients under test in one body, is one scenario per client
//! (`test_{admin,producer,consumer}_bootstraps_asynchronously_with_positive_timeout`):
//! the client under test comes from the backend, the other two are native
//! fixtures, and every client still bootstraps asynchronously.

use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

use confluent_kafka::admin::{Admin, AdminClient, AdminClientConfig, ListTopicsOptions};
use confluent_kafka::common::Error;
use confluent_kafka::common::serialization::{ByteArrayDeserializer, ByteArraySerializer};
use confluent_kafka::consumer::{ConsumerConfig, DynConsumer, KafkaConsumer};
use confluent_kafka::producer::{KafkaProducer, Producer, ProducerConfig, ProducerRecord};

use crate::common::admin_backend::{self, AdminBackend};
use crate::common::backend_factory::{AdminBackendFactory, ConsumerBackendFactory, ProducerBackendFactory};
use crate::common::cluster_config::ClusterConfig;
use crate::common::test_context::TestContext;
use crate::common::test_utils::create_topic;
use crate::{multilanguage_admin_test, multilanguage_consumer_test, multilanguage_test};

/// An RFC 6761 reserved `.invalid` name, which never resolves.
const UNRESOLVABLE: &str = "unresolvable.invalid:9092";
/// The budget the unresolvable-host tests give the resolution.
const TIMEOUT_MS: &str = "3000";
/// Java's message for that budget (`NetworkClient.checkBootstrapTimeout`).
const EXPECTED: &str = "Failed to resolve bootstrap servers after 3000ms. \
                        Please check your bootstrap.servers configuration and DNS settings.";
/// How long a test waits for the failure before giving up (Java's 15 s).
const MAX_WAIT: Duration = Duration::from_secs(15);

fn assert_bootstrap_resolution_error<T: std::fmt::Debug>(backend: &str, result: Result<T, Error>) {
    match result {
        Err(Error::BootstrapResolution(e)) => assert_eq!(e.message(), EXPECTED, "{backend} backend"),
        other => panic!("{backend} backend: expected the bootstrap resolution failure, got {other:?}"),
    }
}

/// The producer's `partitions_for` waits on metadata, which the failure
/// wakes; it and every later call return `BootstrapResolutionError`.
async fn producer_unresolvable_bootstrap_with_positive_timeout<F: ProducerBackendFactory>(
    _ctx: &mut TestContext,
    factory: &F,
) {
    let backend = factory.name();
    let props = HashMap::from([
        (ProducerConfig::BOOTSTRAP_SERVERS_CONFIG.to_string(), UNRESOLVABLE.to_string()),
        (
            ProducerConfig::BOOTSTRAP_RESOLVE_TIMEOUT_MS_CONFIG.to_string(),
            TIMEOUT_MS.to_string(),
        ),
    ]);
    let producer = factory.create(props).await.unwrap_or_else(|e| {
        panic!("{backend} backend: a positive timeout defers resolution, so construction succeeds: {e}")
    });

    let first = tokio::time::timeout(MAX_WAIT, producer.partitions_for("topic"))
        .await
        .unwrap_or_else(|_| panic!("{backend} backend: the bootstrap failure must end the metadata wait"));
    assert_bootstrap_resolution_error(backend, first);
    assert_bootstrap_resolution_error(backend, producer.partitions_for("topic").await);
    producer
        .close_with_timeout(Duration::ZERO)
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: close: {e}"));
}

multilanguage_test!(
    test_producer_unresolvable_bootstrap_with_positive_timeout,
    producer_unresolvable_bootstrap_with_positive_timeout
);

/// The consumer's `poll` and every later call return `BootstrapResolutionError`;
/// `close` still succeeds.
async fn consumer_unresolvable_bootstrap_with_positive_timeout<F: ConsumerBackendFactory>(
    _ctx: &mut TestContext,
    factory: &F,
) {
    let backend = factory.name();
    let props = HashMap::from([
        (ConsumerConfig::BOOTSTRAP_SERVERS_CONFIG.to_string(), UNRESOLVABLE.to_string()),
        (
            ConsumerConfig::BOOTSTRAP_RESOLVE_TIMEOUT_MS_CONFIG.to_string(),
            TIMEOUT_MS.to_string(),
        ),
        (ConsumerConfig::GROUP_PROTOCOL_CONFIG.to_string(), "consumer".to_string()),
        (
            ConsumerConfig::GROUP_ID_CONFIG.to_string(),
            "bootstrap-resolution-test".to_string(),
        ),
    ]);
    let mut consumer = factory.create(props).await.unwrap_or_else(|e| {
        panic!("{backend} backend: a positive timeout defers resolution, so construction succeeds: {e}")
    });
    consumer
        .subscribe_with_topics(vec!["topic".to_string()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: subscribe: {e}"));

    let start = Instant::now();
    let first = loop {
        assert!(
            start.elapsed() < MAX_WAIT,
            "{backend} backend: no bootstrap failure within {MAX_WAIT:?}"
        );
        match consumer.poll(Duration::from_millis(100)).await {
            Ok(_) => {},
            Err(e) => break Err::<(), _>(e),
        }
    };
    assert_bootstrap_resolution_error(backend, first);
    assert_bootstrap_resolution_error(backend, consumer.poll(Duration::from_millis(100)).await);
    assert_bootstrap_resolution_error(backend, consumer.list_topics().await);
    consumer
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: close: {e}"));
}

multilanguage_consumer_test!(
    test_consumer_unresolvable_bootstrap_with_positive_timeout,
    consumer_unresolvable_bootstrap_with_positive_timeout
);

/// The admin client fails the call in flight when the budget runs out and
/// every later call with the same error.
async fn admin_unresolvable_bootstrap_with_positive_timeout<F: AdminBackendFactory>(
    _ctx: &mut TestContext,
    factory: &F,
) {
    let backend = factory.name();
    let props = HashMap::from([
        (
            AdminClientConfig::BOOTSTRAP_SERVERS_CONFIG.to_string(),
            UNRESOLVABLE.to_string(),
        ),
        (
            AdminClientConfig::BOOTSTRAP_RESOLVE_TIMEOUT_MS_CONFIG.to_string(),
            TIMEOUT_MS.to_string(),
        ),
    ]);
    let admin = factory.create(props).await.unwrap_or_else(|e| {
        panic!("{backend} backend: a positive timeout defers resolution, so creation succeeds: {e}")
    });

    let first = tokio::time::timeout(MAX_WAIT, admin.list_topics(ListTopicsOptions::new()))
        .await
        .unwrap_or_else(|_| panic!("{backend} backend: the bootstrap failure must fail the call in flight"));
    assert_bootstrap_resolution_error(backend, first);
    assert_bootstrap_resolution_error(backend, admin.list_topics(ListTopicsOptions::new()).await);
    admin
        .close(None)
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: close: {e}"));
}

multilanguage_admin_test!(
    test_admin_unresolvable_bootstrap_with_positive_timeout,
    admin_unresolvable_bootstrap_with_positive_timeout
);

/// KIP-848 consumer groups need the `consumer` rebalance protocol on the broker
/// (see `consumer_test.rs`).
fn cluster_config_with_kip848() -> ClusterConfig {
    ClusterConfig::with_properties(BTreeMap::from([(
        "KAFKA_GROUP_COORDINATOR_REBALANCE_PROTOCOLS".to_string(),
        "classic,consumer".to_string(),
    )]))
}

/// The budget the resolvable-host scenarios give the resolution: positive, so
/// every client takes the asynchronous path.
const ASYNC_TIMEOUT_MS: &str = "30000";

/// `props` with `bootstrap.resolve.timeout.ms` set to [`ASYNC_TIMEOUT_MS`].
fn async_bootstrap(mut props: HashMap<String, String>) -> HashMap<String, String> {
    props.insert("bootstrap.resolve.timeout.ms".to_string(), ASYNC_TIMEOUT_MS.to_string());
    props
}

/// The properties of the client under test: the bootstrap its backend reaches
/// over the run's protocol, with that protocol's security keys, bootstrapping
/// asynchronously.
fn backend_props(ctx: &TestContext, needs_container_bootstrap: bool) -> HashMap<String, String> {
    let mut props = HashMap::from([(
        "bootstrap.servers".to_string(),
        ctx.protocol_bootstrap_servers_for(needs_container_bootstrap).to_string(),
    )]);
    ctx.apply_security(&mut props);
    async_bootstrap(props)
}

/// The properties of a native fixture, bootstrapping asynchronously.
fn native_props(ctx: &TestContext) -> HashMap<String, String> {
    let mut props = HashMap::new();
    ctx.configure(&mut props);
    async_bootstrap(props)
}

/// `props` with the consumer settings of the resolvable-host scenarios.
fn with_consumer_props(ctx: &TestContext, mut props: HashMap<String, String>) -> HashMap<String, String> {
    props.insert("group.protocol".to_string(), "consumer".to_string());
    props.insert("group.id".to_string(), ctx.group_id("bootstrap_resolve_async"));
    props.insert("auto.offset.reset".to_string(), "earliest".to_string());
    props
}

/// The native admin fixture: creates `topic` and checks `listTopics` has it.
async fn native_admin_creates_topic(ctx: &TestContext, topic: &str) -> Box<dyn Admin> {
    let admin = AdminClient::create(AdminClientConfig::new(&native_props(ctx)).expect("valid config")).expect("admin");
    create_topic(admin.as_ref(), topic, 1, 1).await;
    let names = admin.list_topics().names().get().await.expect("list_topics");
    assert!(names.contains(topic), "{names:?}");
    admin
}

/// The produce half of the resolvable-host scenarios: one record, delivered,
/// then the producer closed.
async fn produce_one_record<P: Producer<Vec<u8>, Vec<u8>>>(producer: &P, backend: &str, topic: &str) {
    let record = ProducerRecord::with_key(topic.to_string(), Some(b"key".to_vec()), Some(b"value".to_vec()));
    producer
        .send(record)
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: send: {e}"))
        .get_with_timeout(Duration::from_secs(30))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: delivery: {e}"));
    producer
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: producer close: {e}"));
}

/// The native producer fixture: writes the one record.
async fn native_produce_one_record(ctx: &TestContext, topic: &str) {
    let producer = KafkaProducer::<Vec<u8>, Vec<u8>>::new(
        ProducerConfig::new(&native_props(ctx)).expect("valid config"),
        ByteArraySerializer::default(),
        ByteArraySerializer::default(),
    )
    .expect("producer");
    produce_one_record(&producer, "rust", topic).await;
}

/// The consume half of the resolvable-host scenarios: the record
/// [`produce_one_record`] wrote comes back, then the consumer is closed.
async fn consume_one_record(consumer: &mut Box<dyn DynConsumer<Vec<u8>, Vec<u8>>>, backend: &str, topic: &str) {
    consumer
        .subscribe_with_topics(vec![topic.to_string()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: subscribe: {e}"));
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut values = Vec::new();
    while values.is_empty() {
        assert!(Instant::now() < deadline, "{backend} backend: no record consumed within 60 s");
        let records = consumer
            .poll(Duration::from_millis(500))
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: poll: {e}"));
        values.extend((&records).into_iter().map(|r| r.value().cloned()));
    }
    assert_eq!(values, vec![Some(b"value".to_vec())], "{backend} backend");
    consumer
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer close: {e}"));
}

/// The native consumer fixture: reads the one record back.
async fn native_consume_one_record(ctx: &TestContext, topic: &str) {
    let mut consumer: Box<dyn DynConsumer<Vec<u8>, Vec<u8>>> = Box::new(
        KafkaConsumer::<Vec<u8>, Vec<u8>>::new(
            ConsumerConfig::new(&with_consumer_props(ctx, native_props(ctx))).expect("valid config"),
            ByteArrayDeserializer::default(),
            ByteArrayDeserializer::default(),
        )
        .expect("consumer"),
    );
    consume_one_record(&mut consumer, "rust", topic).await;
}

/// With a positive timeout and a resolvable bootstrap list, the asynchronous
/// path bootstraps the admin client under test, which creates a topic and
/// lists it; a native producer then writes to it and a native consumer reads it
/// back, both bootstrapping asynchronously too.
async fn admin_bootstraps_asynchronously_with_positive_timeout<F: AdminBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
) {
    let backend = factory.name();
    let topic = ctx.topic("bootstrap_resolve_async");
    let admin = factory
        .create(backend_props(ctx, factory.needs_container_bootstrap()))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: admin: {e}"));
    admin_backend::create_topic(&admin, &topic, 1, 1).await;
    let names = admin
        .list_topics(ListTopicsOptions::new())
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: list_topics: {e}"));
    assert!(names.contains_key(&topic), "{backend} backend: {:?}", names.keys());

    native_produce_one_record(ctx, &topic).await;
    native_consume_one_record(ctx, &topic).await;
    admin
        .close(None)
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: admin close: {e}"));
    ctx.cleanup().await;
}

multilanguage_admin_test!(
    test_admin_bootstraps_asynchronously_with_positive_timeout,
    admin_bootstraps_asynchronously_with_positive_timeout,
    cluster_config_with_kip848()
);

/// As [`admin_bootstraps_asynchronously_with_positive_timeout`], with the
/// producer under test and a native admin and consumer.
async fn producer_bootstraps_asynchronously_with_positive_timeout<F: ProducerBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
) {
    let backend = factory.name();
    let topic = ctx.topic("bootstrap_resolve_async");
    let admin = native_admin_creates_topic(ctx, &topic).await;

    let producer = factory
        .create(backend_props(ctx, factory.needs_container_bootstrap()))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: producer: {e}"));
    produce_one_record(&producer, backend, &topic).await;

    native_consume_one_record(ctx, &topic).await;
    admin.close().await;
    ctx.cleanup().await;
}

multilanguage_test!(
    test_producer_bootstraps_asynchronously_with_positive_timeout,
    producer_bootstraps_asynchronously_with_positive_timeout,
    cluster_config_with_kip848()
);

/// As [`admin_bootstraps_asynchronously_with_positive_timeout`], with the
/// consumer under test and a native admin and producer.
async fn consumer_bootstraps_asynchronously_with_positive_timeout<F: ConsumerBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
) {
    let backend = factory.name();
    let topic = ctx.topic("bootstrap_resolve_async");
    let admin = native_admin_creates_topic(ctx, &topic).await;
    native_produce_one_record(ctx, &topic).await;

    let mut consumer = factory
        .create(with_consumer_props(
            ctx,
            backend_props(ctx, factory.needs_container_bootstrap()),
        ))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer: {e}"));
    consume_one_record(&mut consumer, backend, &topic).await;
    admin.close().await;
    ctx.cleanup().await;
}

multilanguage_consumer_test!(
    test_consumer_bootstraps_asynchronously_with_positive_timeout,
    consumer_bootstraps_asynchronously_with_positive_timeout,
    cluster_config_with_kip848()
);
