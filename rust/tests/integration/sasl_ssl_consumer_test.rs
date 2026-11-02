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

//! End-to-end SASL_SSL consumer integration test (Phase 17).
//!
//! Exercises the channel-builder selection wired into `AsyncKafkaConsumer`:
//! the consumer builds an `SaslChannelBuilder` over TLS purely from
//! `security.protocol=SASL_SSL` + `sasl.*` + `ssl.*` configuration, mirroring
//! the producer wiring proven by master PR #10.
//!
//! Test: `test_sasl_ssl_wrong_credentials` — wrong SASL password surfaces as
//! an error (it must not hang), mirroring `ssl_sasl_test::test_sasl_wrong_credentials`.
//! No shared scenario makes a wrong-credentials check over SASL_SSL
//! (`client_authentication_failure_test` uses the SASL_PLAINTEXT listener).
//!
//! The successful-consumption test that was here (subscribe, produce five
//! records `key i` / `value i` to partition 0, poll them back) is covered by
//! the shared scenarios the SASL_SSL CI jobs run over this listener, for
//! example `plaintext_consumer_test::test_async_consumer_pause_state_not_preserved_by_rebalance`,
//! which subscribes, produces the same five records and verifies each one in
//! order.
//!
//! Gated identically to the other `plaintext_consumer_*` / `ssl_sasl_test`
//! suites via `tests/integration/main.rs`'s `#![cfg(feature = "integration-tests")]`.
//! Requires a Docker cluster: `cargo test --features integration-tests --test integration`.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::time::Duration;

use confluent_kafka::common::Error;
use confluent_kafka::common::serialization::Deserializer;
use confluent_kafka::consumer::Consumer;
use confluent_kafka::consumer::ConsumerConfig;
use confluent_kafka::consumer::KafkaConsumer;

use crate::common::cluster_config::ClusterConfig;
use crate::common::kafka_cluster::SASL_USERNAME;
use crate::common::test_context::TestContext;

/// Local byte-array deserializer (the crate exports `ByteArraySerializer` but
/// no symmetric `ByteArrayDeserializer`); identical in behavior.
struct ByteArrayDeserializer;

impl Deserializer<Vec<u8>> for ByteArrayDeserializer {
    async fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<Vec<u8>, Error> {
        Ok(data.to_vec())
    }
}

/// SASL_SSL JAAS config for the PLAIN mechanism with the cluster's admin
/// credentials. Mirrors Java's `sasl.jaas.config` for `PlainLoginModule`.
fn plain_jaas_config(username: &str, password: &str) -> String {
    format!(
        "org.apache.kafka.common.security.plain.PlainLoginModule required \
         username=\"{username}\" password=\"{password}\";"
    )
}

/// Single-broker cluster with KIP-848 (`group.protocol=consumer`) enabled.
/// The cluster always exposes the SASL_SSL listener on port 9097
/// (see `tests/common/kafka_cluster.rs`); replication factor is 1 for a
/// single broker.
fn cluster_config_sasl_ssl_kip848() -> ClusterConfig {
    let mut props = BTreeMap::new();
    props.insert(
        "KAFKA_GROUP_COORDINATOR_REBALANCE_PROTOCOLS".to_string(),
        "classic,consumer".to_string(),
    );
    props.insert("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR".to_string(), "1".to_string());
    props.insert("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS".to_string(), "1".to_string());
    props.insert("KAFKA_GROUP_MIN_SESSION_TIMEOUT_MS".to_string(), "100".to_string());
    props.insert("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS".to_string(), "10".to_string());
    props.insert("KAFKA_NUM_PARTITIONS".to_string(), "1".to_string());
    let mut cfg = ClusterConfig::with_brokers(1);
    cfg.server_properties = props;
    cfg
}

/// Build a SASL_SSL `ConsumerConfig` (PLAIN, KIP-848) against port 9097.
fn make_sasl_ssl_consumer_config(
    bootstrap: &str,
    group_id: &str,
    ca_cert_pem: &str,
    username: &str,
    password: &str,
) -> ConsumerConfig {
    let props = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap.to_string()),
        ("group.protocol".to_string(), "consumer".to_string()),
        ("auto.offset.reset".to_string(), "earliest".to_string()),
        ("client.id".to_string(), "sasl-ssl-test-consumer".to_string()),
        ("enable.auto.commit".to_string(), "false".to_string()),
        ("group.id".to_string(), group_id.to_string()),
        ("security.protocol".to_string(), "SASL_SSL".to_string()),
        ("sasl.mechanism".to_string(), "PLAIN".to_string()),
        ("sasl.jaas.config".to_string(), plain_jaas_config(username, password)),
        ("ssl.truststore.certificates".to_string(), ca_cert_pem.to_string()),
        // Tests connect via 127.0.0.1; disable hostname verification, matching
        // `ssl_sasl_test`'s SASL_SSL selector helper.
        ("ssl.endpoint.identification.algorithm".to_string(), String::new()),
    ]);
    ConsumerConfig::new(&props).expect("invalid SASL_SSL consumer config")
}

/// Test: SASL_SSL consumer with wrong credentials surfaces an error (not a hang).
///
/// Mirrors `ssl_sasl_test::test_sasl_wrong_credentials`. A `poll` with bad
/// credentials must terminate with an authentication error within the timeout
/// rather than hanging forever.
#[tokio::test(flavor = "multi_thread")]
async fn test_sasl_ssl_wrong_credentials() {
    let mut ctx = TestContext::new(cluster_config_sasl_ssl_kip848()).await;
    let bootstrap = ctx.sasl_ssl_bootstrap_servers().to_string();
    let ca_cert_pem = ctx.ca_cert_pem().to_string();
    let topic = ctx.topic("sasl_ssl_bad_creds_topic");
    let group_id = ctx.group_id("sasl_ssl_bad_creds_group");

    let mut consumer = KafkaConsumer::<Vec<u8>, Vec<u8>>::new(
        make_sasl_ssl_consumer_config(&bootstrap, &group_id, &ca_cert_pem, SASL_USERNAME, "wrong-password"),
        ByteArrayDeserializer,
        ByteArrayDeserializer,
    )
    .expect("KafkaConsumer::new should succeed (config is structurally valid)");

    consumer
        .subscribe_with_topics(vec![topic.clone()])
        .await
        .expect("subscribe should succeed");

    // Drive poll for a bounded period; authentication must fail and surface as
    // an error rather than hanging. We bound the whole sequence with an outer
    // timeout so a hang fails the test instead of stalling the suite.
    let outcome = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            match consumer.poll(Duration::from_millis(500)).await {
                // A poll that returns Ok with no records is expected while the
                // connection retries; keep polling until the auth error lands.
                Ok(_records) => continue,
                Err(e) => return e,
            }
        }
    })
    .await;

    let err = outcome.expect("poll must surface an auth error, not hang");
    let msg = format!("{err}");
    assert!(
        msg.to_lowercase().contains("auth")
            || msg.to_lowercase().contains("credential")
            || msg.to_lowercase().contains("sasl"),
        "error should indicate an authentication failure, got: {msg}"
    );
    // The broker rejects the password with SASL_AUTHENTICATION_FAILED, which Java
    // raises as `SaslAuthenticationException`, not the base class.
    assert!(
        err.is_sasl_authentication_error(),
        "a rejected password is a SaslAuthenticationError, got: {err:?}"
    );

    // Best-effort close (it may itself error on the unauthenticated connection).
    let _ = consumer.close().await;
}
