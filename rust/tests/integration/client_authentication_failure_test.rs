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
//! - `testConsumerWithInvalidCredentials` → [`test_consumer_with_invalid_credentials`]
//! - `testProducerWithInvalidCredentials` → [`test_producer_with_invalid_credentials`]
//! - `testAdminClientWithInvalidCredentials` → [`test_admin_client_with_invalid_credentials`]
//! - `testTransactionalProducerWithInvalidCredentials` →
//!   [`test_transactional_producer_with_invalid_credentials`]
//!
//! Java runs them against an in-process `NioEchoServer` with a SASL/PLAIN
//! server authenticator. This client has no server side, so they run against
//! the harness broker's SASL_PLAINTEXT listener, with Java's wrong password.

use std::collections::HashMap;
use std::time::Duration;
use std::time::Instant;

use confluent_kafka::admin::AdminClient;
use confluent_kafka::admin::AdminClientConfig;
use confluent_kafka::common::TopicPartition;
use confluent_kafka::common::serialization::StringDeserializer;
use confluent_kafka::common::serialization::StringSerializer;
use confluent_kafka::consumer::Consumer;
use confluent_kafka::consumer::ConsumerConfig;
use confluent_kafka::consumer::KafkaConsumer;
use confluent_kafka::producer::KafkaProducer;
use confluent_kafka::producer::Producer;
use confluent_kafka::producer::ProducerConfig;
use confluent_kafka::producer::ProducerRecord;

use crate::common::kafka_cluster::SASL_USERNAME;
use crate::common::test_context::TestContext;

use super::base_consumer_test;

/// How long a client may take to report the rejection.
const MAX_WAIT: Duration = Duration::from_secs(60);

/// Java's `saslClientConfigs` with `TestJaasConfig.setClientOptions("PLAIN",
/// USERNAME, "anotherpassword")`: the right user, a password the broker
/// rejects.
fn invalid_credentials_props(ctx: &TestContext) -> HashMap<String, String> {
    HashMap::from([
        (
            "bootstrap.servers".to_string(),
            ctx.sasl_plaintext_bootstrap_servers().to_string(),
        ),
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

/// Translated from `ClientAuthenticationFailureTest.testConsumerWithInvalidCredentials`:
/// a consumer that assigns itself a partition fails its poll.
#[tokio::test(flavor = "multi_thread")]
async fn test_consumer_with_invalid_credentials() {
    let mut ctx = TestContext::new(base_consumer_test::cluster_config()).await;
    let topic = ctx.topic("test");
    let mut props = invalid_credentials_props(&ctx);
    props.insert("group.protocol".to_string(), "consumer".to_string());
    let mut consumer = KafkaConsumer::<String, String>::new(
        ConsumerConfig::new(&props).expect("valid consumer config"),
        StringDeserializer::new(),
        StringDeserializer::new(),
    )
    .expect("consumer constructs");

    consumer.assign(vec![TopicPartition::new(topic, 0)]).await.expect("assign");
    // Java's single `poll(10 s)` throws; polls here are shorter and repeated
    // until the rejection surfaces, so a slow broker does not fail the test.
    let deadline = Instant::now() + MAX_WAIT;
    let err = loop {
        assert!(Instant::now() < deadline, "no authentication failure within {MAX_WAIT:?}");
        if let Err(e) = consumer.poll(Duration::from_millis(500)).await {
            break e;
        }
    };
    assert!(err.is_sasl_authentication_error(), "{err:?}");

    let _ = consumer.close().await;
    ctx.cleanup().await;
}

/// Translated from `ClientAuthenticationFailureTest.testProducerWithInvalidCredentials`:
/// the record's future fails.
#[tokio::test(flavor = "multi_thread")]
async fn test_producer_with_invalid_credentials() {
    let mut ctx = TestContext::new(base_consumer_test::cluster_config()).await;
    let topic = ctx.topic("test");
    let props = invalid_credentials_props(&ctx);
    let producer = KafkaProducer::<String, String>::new(
        ProducerConfig::new(&props).expect("valid producer config"),
        StringSerializer::new(),
        StringSerializer::new(),
    )
    .expect("producer constructs");

    // Java's `send` returns a failed future when the metadata wait fails; this
    // `send` may also return the error itself. Either way it is the same class.
    let err = match producer.send(ProducerRecord::new(topic, Some("message".to_string()))).await {
        Err(e) => e,
        Ok(future) => future
            .get_with_timeout(MAX_WAIT)
            .await
            .expect_err("the record must fail the authentication"),
    };
    assert!(err.is_sasl_authentication_error(), "{err:?}");

    let _ = producer.close().await;
    ctx.cleanup().await;
}

/// Translated from `ClientAuthenticationFailureTest.testAdminClientWithInvalidCredentials`:
/// `describeTopics(...).allTopicNames()` fails.
#[tokio::test(flavor = "multi_thread")]
async fn test_admin_client_with_invalid_credentials() {
    let mut ctx = TestContext::new(base_consumer_test::cluster_config()).await;
    let topic = ctx.topic("test");
    let props = invalid_credentials_props(&ctx);
    let admin = AdminClient::create(AdminClientConfig::new(&props).expect("valid admin config")).expect("admin");

    let err = admin
        .describe_topics_with_topic_names(&[topic])
        .all_topic_names()
        .expect("a describe by name has allTopicNames()")
        .get_with_timeout(MAX_WAIT)
        .await
        .expect_err("the describe must fail the authentication");
    assert!(err.is_sasl_authentication_error(), "{err:?}");

    admin.close().await;
    ctx.cleanup().await;
}

/// Translated from `ClientAuthenticationFailureTest.testTransactionalProducerWithInvalidCredentials`:
/// `initTransactions()` fails. The rejection reaches it through the
/// `Sender`'s wait for the coordinator's connection (`NetworkClientUtils.awaitReady`,
/// which rethrows the node's `AuthenticationException` as it is).
#[tokio::test(flavor = "multi_thread")]
async fn test_transactional_producer_with_invalid_credentials() {
    let mut ctx = TestContext::new(base_consumer_test::cluster_config()).await;
    let mut props = invalid_credentials_props(&ctx);
    props.insert("transactional.id".to_string(), ctx.group_id("txclient-1"));
    props.insert("enable.idempotence".to_string(), "true".to_string());
    let producer = KafkaProducer::<String, String>::new(
        ProducerConfig::new(&props).expect("valid producer config"),
        StringSerializer::new(),
        StringSerializer::new(),
    )
    .expect("producer constructs");

    let err = tokio::time::timeout(MAX_WAIT, producer.init_transactions())
        .await
        .expect("initTransactions must not hang")
        .expect_err("initTransactions must fail the authentication");
    assert!(err.is_sasl_authentication_error(), "{err:?}");

    let _ = producer.close().await;
    ctx.cleanup().await;
}
