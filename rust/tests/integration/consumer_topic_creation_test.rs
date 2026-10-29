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
//! `kafka/clients/clients-integration-tests/src/test/java/org/apache/kafka/clients/consumer/ConsumerTopicCreationTest.java`
//! (Apache Kafka 4.2).
//!
//! Exercises the interaction of the consumer-side
//! `allow.auto.create.topics` config with the broker-side
//! `auto.create.topics.enable`: a topic is auto-created on
//! `subscribe + poll` only when BOTH are `true`.
//!
//! KIP-848 (`GroupProtocol.CONSUMER`) arm only; the two `testClassic*`
//! twins are OUT_OF_SCOPE per `consumer-threading.md` §20.
//!
//! The translated tests run as multilanguage scenarios: each one runs against
//! the native Rust consumer and, through the gRPC backends, the Python (sync
//! and asyncio) and C consumers ([`multilanguage_consumer_test!`]); each name
//! below gains the macro's backend suffix (`__rust`, `__grpc_python`,
//! `__grpc_python_async`, `__grpc_c`). The arms share one pooled cluster, so
//! each arm subscribes to its own topic (`ctx.topic`) where Java uses the
//! fixed names `topic`: with a shared name, the first arm's auto-creation
//! would make the topic exist for every later one.
//!
//! ## Translated (CONSUMER arm)
//!
//! Java's `@ClusterTemplate("autoCreateTopicsConfigs")` runs each test on a
//! broker with `auto.create.topics.enable` true and one with it false; each
//! Java test is therefore two macro lines, the broker-disallows one with the
//! `_broker_disallow` suffix:
//!
//! - `testAsyncConsumerTopicCreationIfConsumerAllowToCreateTopic`
//!   → `test_async_consumer_topic_creation_if_consumer_allow_to_create_topic`,
//!   `test_async_consumer_topic_creation_if_consumer_allow_to_create_topic_broker_disallow`
//! - `testAsyncConsumerTopicCreationIfConsumerDisallowToCreateTopic`
//!   → `test_async_consumer_topic_creation_if_consumer_disallow_to_create_topic`,
//!   `test_async_consumer_topic_creation_if_consumer_disallow_to_create_topic_broker_disallow`
//!
//! Topic existence is checked as Java's `getAllTopics` does, through a native
//! admin client's `list_topics()`, never through the consumer under test. The
//! check retries over a short window (Java checks once) to absorb metadata
//! propagation after the poll.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::time::Duration;
use std::time::Instant;

use confluent_kafka::admin::{Admin, AdminClient, AdminClientConfig};
use confluent_kafka::consumer::DynConsumer;

use crate::common::backend_factory::ConsumerBackendFactory;
use crate::common::cluster_config::ClusterConfig;
use crate::common::test_context::TestContext;
use crate::multilanguage_consumer_test;

type BytesConsumer = dyn DynConsumer<Vec<u8>, Vec<u8>>;

/// Cluster config with broker-side `auto.create.topics.enable` toggled.
/// The cluster pool keys on `ClusterConfig`, so the `true` and `false`
/// variants get distinct containers — mirroring Java's two
/// `ClusterConfig.defaultBuilder()` entries from `autoCreateTopicsConfigs`.
fn cluster_config_auto_create(broker_allows: bool) -> ClusterConfig {
    let mut props = BTreeMap::new();
    props.insert(
        "KAFKA_GROUP_COORDINATOR_REBALANCE_PROTOCOLS".to_string(),
        "classic,consumer".to_string(),
    );
    props.insert("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR".to_string(), "1".to_string());
    props.insert(
        "KAFKA_AUTO_CREATE_TOPICS_ENABLE".to_string(),
        if broker_allows { "true" } else { "false" }.to_string(),
    );
    ClusterConfig::with_properties(props)
}

async fn make_consumer<F: ConsumerBackendFactory>(
    ctx: &TestContext,
    factory: &F,
    group_id: &str,
    allow_auto_create: bool,
) -> Box<BytesConsumer> {
    let mut props = HashMap::from([
        (
            "bootstrap.servers".to_string(),
            ctx.protocol_bootstrap_servers_for(factory.needs_container_bootstrap())
                .to_string(),
        ),
        ("group.protocol".to_string(), "consumer".to_string()),
        ("group.id".to_string(), group_id.to_string()),
        ("client.id".to_string(), "integration-test-consumer".to_string()),
        ("enable.auto.commit".to_string(), "false".to_string()),
        (
            "allow.auto.create.topics".to_string(),
            if allow_auto_create { "true" } else { "false" }.to_string(),
        ),
    ]);
    ctx.apply_security(&mut props);
    factory
        .create(props)
        .await
        .unwrap_or_else(|e| panic!("{} backend: create consumer: {e}", factory.name()))
}

/// Java's `subscribeAndPoll`: subscribe to the topic, poll once (1000ms). A
/// poll error fails the test, as Java's `poll` throws through.
async fn subscribe_and_poll(consumer: &mut BytesConsumer, topic: &str, backend: &str) {
    consumer
        .subscribe_with_topics(vec![topic.to_string()])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: subscribe should succeed: {e}"));
    consumer
        .poll(Duration::from_millis(1000))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: poll should succeed: {e}"));
}

/// Java's `getAllTopics`: the topic names a native admin client lists
/// (`cluster.admin()`, `admin.listTopics()`), not the consumer under test.
/// Returns whether `topic` is among them, retrying over `settle` to absorb
/// the metadata-propagation delay after a `subscribe + poll`.
async fn topic_exists(ctx: &TestContext, topic: &str, settle: Duration, backend: &str) -> bool {
    let mut props = HashMap::from([("client.id".to_string(), "topic-creation-test-admin".to_string())]);
    ctx.configure(&mut props);
    let admin: Box<dyn Admin> =
        AdminClient::create(AdminClientConfig::new(&props).expect("valid admin config")).expect("admin client");
    let deadline = Instant::now() + settle;
    let exists = loop {
        let names = admin
            .list_topics()
            .names()
            .get()
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: admin list_topics should succeed: {e}"));
        if names.contains(topic) {
            break true;
        }
        if Instant::now() >= deadline {
            break false;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    admin.close_with_timeout(Duration::from_secs(5)).await;
    exists
}

/// Translates Java's
/// `testAsyncConsumerTopicCreationIfConsumerAllowToCreateTopic`.
/// With consumer `allow.auto.create.topics=true`, the topic is created on
/// `subscribe + poll` iff the broker also allows auto-create
/// (`assertTopicCreateBasedOnPermission`). Java's `ClusterTemplate` runs it
/// on both broker settings; so does this file, one macro line each.
async fn consumer_allow_to_create_topic<F: ConsumerBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
    broker_allows: bool,
) {
    let backend = factory.name();
    // Java's fixed `TOPIC = "topic"`, made unique per arm (see the module doc).
    let topic = ctx.topic("topic");
    let group_id = ctx.group_id("g_topic_create_allow");

    let mut consumer = make_consumer(ctx, factory, &group_id, true).await;
    subscribe_and_poll(consumer.as_mut(), &topic, backend).await;

    if broker_allows {
        // Both consumer-allow AND broker-allow are true → topic IS created.
        assert!(
            topic_exists(ctx, &topic, Duration::from_secs(15), backend).await,
            "topic should be auto-created when both broker and consumer allow it ({backend} backend)"
        );
    } else {
        assert!(
            !topic_exists(ctx, &topic, Duration::from_secs(8), backend).await,
            "Both auto.create.topics.enable and allow.auto.create.topics need to be true to create topic \
             automatically ({backend} backend)"
        );
    }

    consumer
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer close should succeed: {e}"));
}

/// [`consumer_allow_to_create_topic`] on the broker that allows auto-create.
async fn async_consumer_topic_creation_if_consumer_allow_to_create_topic<F: ConsumerBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
) {
    consumer_allow_to_create_topic(ctx, factory, true).await;
}

multilanguage_consumer_test!(
    test_async_consumer_topic_creation_if_consumer_allow_to_create_topic,
    async_consumer_topic_creation_if_consumer_allow_to_create_topic,
    cluster_config_auto_create(true)
);

/// [`consumer_allow_to_create_topic`] on the broker that disallows
/// auto-create.
async fn async_consumer_topic_creation_if_consumer_allow_to_create_topic_broker_disallow<F: ConsumerBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
) {
    consumer_allow_to_create_topic(ctx, factory, false).await;
}

multilanguage_consumer_test!(
    test_async_consumer_topic_creation_if_consumer_allow_to_create_topic_broker_disallow,
    async_consumer_topic_creation_if_consumer_allow_to_create_topic_broker_disallow,
    cluster_config_auto_create(false)
);

/// Translates Java's
/// `testAsyncConsumerTopicCreationIfConsumerDisallowToCreateTopic`.
/// With consumer `allow.auto.create.topics=false`, the topic is NOT
/// created regardless of the broker setting (`assertTopicNotCreate`); Java's
/// `ClusterTemplate` runs it on both broker settings, as this file does.
async fn consumer_disallow_to_create_topic<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let backend = factory.name();
    let topic = ctx.topic("topic-disallow");
    let group_id = ctx.group_id("g_topic_create_disallow");

    let mut consumer = make_consumer(ctx, factory, &group_id, false).await;
    subscribe_and_poll(consumer.as_mut(), &topic, backend).await;

    // Java: "Both ... need to be true to create topic automatically".
    assert!(
        !topic_exists(ctx, &topic, Duration::from_secs(8), backend).await,
        "Both auto.create.topics.enable and allow.auto.create.topics need to be true to create topic \
         automatically ({backend} backend)"
    );

    consumer
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer close should succeed: {e}"));
}

/// [`consumer_disallow_to_create_topic`] on the broker that allows
/// auto-create: the consumer-side disallow alone suppresses creation.
async fn async_consumer_topic_creation_if_consumer_disallow_to_create_topic<F: ConsumerBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
) {
    consumer_disallow_to_create_topic(ctx, factory).await;
}

multilanguage_consumer_test!(
    test_async_consumer_topic_creation_if_consumer_disallow_to_create_topic,
    async_consumer_topic_creation_if_consumer_disallow_to_create_topic,
    cluster_config_auto_create(true)
);

/// [`consumer_disallow_to_create_topic`] on the broker that disallows
/// auto-create.
async fn async_consumer_topic_creation_if_consumer_disallow_to_create_topic_broker_disallow<
    F: ConsumerBackendFactory,
>(
    ctx: &mut TestContext,
    factory: &F,
) {
    consumer_disallow_to_create_topic(ctx, factory).await;
}

multilanguage_consumer_test!(
    test_async_consumer_topic_creation_if_consumer_disallow_to_create_topic_broker_disallow,
    async_consumer_topic_creation_if_consumer_disallow_to_create_topic_broker_disallow,
    cluster_config_auto_create(false)
);
