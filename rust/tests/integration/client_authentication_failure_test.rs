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

//! Translated from
//! `org.apache.kafka.common.security.authenticator.ClientAuthenticationFailureTest`
//! (`clients/src/test`, AK 4.4.0): each client, given a password the broker
//! rejects, fails with `SaslAuthenticationException`, the class of the
//! broker's `SASL_AUTHENTICATION_FAILED`, not a generic authentication error.
//!
//! - `testConsumerWithInvalidCredentials` → `test_consumer_with_invalid_credentials`
//! - `testProducerWithInvalidCredentials` → `test_producer_with_invalid_credentials`
//! - `testAdminClientWithInvalidCredentials` → `test_admin_client_with_invalid_credentials`
//! - `testTransactionalProducerWithInvalidCredentials` →
//!   `test_transactional_producer_with_invalid_credentials`
//!
//! Java runs them against an in-process `NioEchoServer` with a SASL/PLAIN
//! server authenticator. This client has no server side, so they run against
//! the harness broker's SASL_PLAINTEXT listener, with Java's wrong password.
//!
//! Every test is a multilanguage scenario ([`multilanguage_consumer_test!`],
//! [`multilanguage_test!`], [`multilanguage_admin_test!`]): the client under
//! test also runs on the Python (sync, asyncio) and C backends, over the
//! SASL_PLAINTEXT listener its backend reaches (the CONTAINER SASL_PLAINTEXT
//! listener for a containerized backend). Each name gains the backend suffix
//! (`__rust`, `__grpc_python`, `__grpc_python_async`, `__grpc_c`). The
//! clients take bytes, not Java's strings: no record reaches the broker.

use std::collections::HashMap;
use std::time::Duration;
use std::time::Instant;

use confluent_kafka::admin::DescribeTopicsOptions;
use confluent_kafka::common::Error;
use confluent_kafka::common::TopicPartition;
use confluent_kafka::producer::Producer;
use confluent_kafka::producer::ProducerRecord;

use crate::common::admin_backend::AdminBackend;
use crate::common::backend_factory::AdminBackendFactory;
use crate::common::backend_factory::ConsumerBackendFactory;
use crate::common::backend_factory::ProducerBackendFactory;
use crate::common::kafka_cluster::SASL_USERNAME;
use crate::common::test_context::TestContext;
use crate::{multilanguage_admin_test, multilanguage_consumer_test, multilanguage_test};

use super::base_consumer_test;

/// How long a client may take to report the rejection.
const MAX_WAIT: Duration = Duration::from_secs(60);

/// Java's `saslClientConfigs` with `TestJaasConfig.setClientOptions("PLAIN",
/// USERNAME, "anotherpassword")`: the right user, a password the broker
/// rejects, over the SASL_PLAINTEXT listener the backend reaches.
fn invalid_credentials_props(ctx: &TestContext, needs_container_bootstrap: bool) -> HashMap<String, String> {
    let bootstrap = if needs_container_bootstrap {
        ctx.container_sasl_plaintext_bootstrap_servers()
    } else {
        ctx.sasl_plaintext_bootstrap_servers()
    };
    HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap.to_string()),
        ("security.protocol".to_string(), "SASL_PLAINTEXT".to_string()),
        ("sasl.mechanism".to_string(), "PLAIN".to_string()),
        (
            "sasl.jaas.config".to_string(),
            format!(
                "org.apache.kafka.common.security.plain.PlainLoginModule required \
                 username=\"{SASL_USERNAME}\" password=\"anotherpassword\";"
            ),
        ),
    ])
}

/// Asserts `err` is the broker's SASL rejection.
fn assert_sasl_authentication_error(backend: &str, err: &Error) {
    assert!(err.is_sasl_authentication_error(), "{backend} backend: {err:?}");
}

/// Translated from `ClientAuthenticationFailureTest.testConsumerWithInvalidCredentials`:
/// a consumer that assigns itself a partition fails its poll.
async fn consumer_with_invalid_credentials<F: ConsumerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let backend = factory.name();
    let topic = ctx.topic("test");
    let mut props = invalid_credentials_props(ctx, factory.needs_container_bootstrap());
    props.insert("group.protocol".to_string(), "consumer".to_string());
    let mut consumer = factory
        .create(props)
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: consumer constructs: {e}"));

    consumer
        .assign(vec![TopicPartition::new(topic, 0)])
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: assign: {e}"));
    // Java's single `poll(10 s)` throws; polls here are shorter and repeated
    // until the rejection surfaces, so a slow broker does not fail the test.
    let deadline = Instant::now() + MAX_WAIT;
    let err = loop {
        assert!(
            Instant::now() < deadline,
            "{backend} backend: no authentication failure within {MAX_WAIT:?}"
        );
        if let Err(e) = consumer.poll(Duration::from_millis(500)).await {
            break e;
        }
    };
    assert_sasl_authentication_error(backend, &err);

    let _ = consumer.close().await;
    ctx.cleanup().await;
}

multilanguage_consumer_test!(
    test_consumer_with_invalid_credentials,
    consumer_with_invalid_credentials,
    base_consumer_test::cluster_config()
);

/// Translated from `ClientAuthenticationFailureTest.testProducerWithInvalidCredentials`:
/// the record's future fails.
async fn producer_with_invalid_credentials<F: ProducerBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let backend = factory.name();
    let topic = ctx.topic("test");
    let producer = factory
        .create(invalid_credentials_props(ctx, factory.needs_container_bootstrap()))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: producer constructs: {e}"));

    // Java's `send` returns a failed future when the metadata wait fails:
    // `KafkaProducer#doSend` turns an `ApiException` into a `FutureFailure`.
    let err = producer
        .send(ProducerRecord::new(topic, Some(b"message".to_vec())))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: send must return the record's future: {e}"))
        .get_with_timeout(MAX_WAIT)
        .await
        .expect_err("the record must fail the authentication");
    assert_sasl_authentication_error(backend, &err);

    let _ = producer.close().await;
    ctx.cleanup().await;
}

multilanguage_test!(
    test_producer_with_invalid_credentials,
    producer_with_invalid_credentials,
    base_consumer_test::cluster_config()
);

/// Translated from `ClientAuthenticationFailureTest.testAdminClientWithInvalidCredentials`:
/// `describeTopics(...).allTopicNames()` fails.
async fn admin_client_with_invalid_credentials<F: AdminBackendFactory>(ctx: &mut TestContext, factory: &F) {
    let backend = factory.name();
    let topic = ctx.topic("test");
    let admin = factory
        .create(invalid_credentials_props(ctx, factory.needs_container_bootstrap()))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: admin: {e}"));

    // `allTopicNames()` is `KafkaFuture.allOf` over the per-topic futures, so
    // with one topic it fails with that topic's error.
    let mut outcomes = tokio::time::timeout(
        MAX_WAIT,
        admin.describe_topics_with_topics(std::slice::from_ref(&topic), DescribeTopicsOptions::new()),
    )
    .await
    .unwrap_or_else(|_| panic!("{backend} backend: the describe did not fail within {MAX_WAIT:?}"))
    .unwrap_or_else(|e| panic!("{backend} backend: describeTopics must return its result: {e}"));
    let err = match outcomes.remove(&topic) {
        Some(Err(e)) => e,
        other => panic!("{backend} backend: the describe must fail the authentication, got {other:?}"),
    };
    assert_sasl_authentication_error(backend, &err);

    admin
        .close(None)
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: admin close: {e}"));
    ctx.cleanup().await;
}

multilanguage_admin_test!(
    test_admin_client_with_invalid_credentials,
    admin_client_with_invalid_credentials,
    base_consumer_test::cluster_config()
);

/// Translated from `ClientAuthenticationFailureTest.testTransactionalProducerWithInvalidCredentials`:
/// `initTransactions()` fails. The rejection reaches it through the
/// `Sender`'s wait for the coordinator's connection (`NetworkClientUtils.awaitReady`,
/// which rethrows the node's `AuthenticationException` as it is).
async fn transactional_producer_with_invalid_credentials<F: ProducerBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
) {
    let backend = factory.name();
    let mut props = invalid_credentials_props(ctx, factory.needs_container_bootstrap());
    props.insert("transactional.id".to_string(), ctx.group_id("txclient-1"));
    props.insert("enable.idempotence".to_string(), "true".to_string());
    let producer = factory
        .create(props)
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: producer constructs: {e}"));

    let err = tokio::time::timeout(MAX_WAIT, producer.init_transactions())
        .await
        .unwrap_or_else(|_| panic!("{backend} backend: initTransactions must not hang"))
        .expect_err("initTransactions must fail the authentication");
    assert_sasl_authentication_error(backend, &err);

    let _ = producer.close().await;
    ctx.cleanup().await;
}

multilanguage_test!(
    test_transactional_producer_with_invalid_credentials,
    transactional_producer_with_invalid_credentials,
    base_consumer_test::cluster_config()
);
