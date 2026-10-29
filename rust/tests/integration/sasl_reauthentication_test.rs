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

//! SASL re-authentication (KIP-368) against a broker that sets
//! `connections.max.reauth.ms`.
//!
//! Rust-only: Java covers client re-authentication in `SaslAuthenticatorTest`
//! (`testCannotReauthenticateWithDifferentPrincipal`,
//! `testCannotReauthenticateAgainFasterThanOneSecond`, ...), against an echo
//! server built on the server-side `SaslServerAuthenticator` and with
//! mechanisms (SCRAM, OAUTHBEARER) this client does not have. This test checks
//! the same contract against a real broker with the PLAIN mechanism: the
//! broker closes a connection whose session has expired when it next receives
//! anything but a re-authentication, so a client that does not re-authenticate
//! loses the requests it has in flight once per session lifetime.
//!
//! Both tests are multilanguage scenarios ([`multilanguage_test!`]): the
//! producer also runs on the Python (sync, asyncio) and C backends, over the
//! SASL_PLAINTEXT or SASL_SSL listener its backend reaches (the CONTAINER
//! listener of that protocol for a containerized backend), and each name gains
//! the backend suffix (`__rust`, `__grpc_python`, `__grpc_python_async`,
//! `__grpc_c`).

use std::collections::HashMap;
use std::time::Duration;
use std::time::Instant;

use confluent_kafka::producer::Producer;
use confluent_kafka::producer::ProducerRecord;

use crate::common::backend_factory::ProducerBackendFactory;
use crate::common::cluster_config::ClusterConfig;
use crate::common::cluster_config::kip848_3_broker;
use crate::common::kafka_cluster::{SASL_PASSWORD, SASL_USERNAME};
use crate::common::test_context::TestContext;
use crate::multilanguage_test;

/// The session lifetime the broker gives each SASL connection.
const SESSION_LIFETIME_MS: u64 = 2_000;

/// A single broker whose SASL sessions last [`SESSION_LIFETIME_MS`].
fn cluster_config() -> ClusterConfig {
    let mut config = kip848_3_broker(1);
    config
        .server_properties
        .insert("KAFKA_CONNECTIONS_MAX_REAUTH_MS".to_string(), SESSION_LIFETIME_MS.to_string());
    config
}

/// The client settings for the PLAIN mechanism with the cluster's credentials.
fn sasl_plain_props(bootstrap: &str, security_protocol: &str) -> HashMap<String, String> {
    HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap.to_string()),
        ("security.protocol".to_string(), security_protocol.to_string()),
        ("sasl.mechanism".to_string(), "PLAIN".to_string()),
        (
            "sasl.jaas.config".to_string(),
            format!(
                "org.apache.kafka.common.security.plain.PlainLoginModule required \
                 username=\"{SASL_USERNAME}\" password=\"{SASL_PASSWORD}\";"
            ),
        ),
    ])
}

/// `props` with the producer settings of [`produce_across_session_lifetimes`].
fn with_producer_props(mut props: HashMap<String, String>) -> HashMap<String, String> {
    props.extend([
        ("client.id".to_string(), "sasl-reauthentication-producer".to_string()),
        ("acks".to_string(), "all".to_string()),
        ("enable.idempotence".to_string(), "false".to_string()),
        ("retries".to_string(), "0".to_string()),
    ]);
    props
}

/// Sends one record every 100 ms for three and a half session lifetimes, with
/// retries off, so a record whose connection the broker closes under it fails
/// rather than being resent. Before the client re-authenticated, a send
/// shortly after the first session expired failed with "Disconnected from
/// node".
async fn produce_across_session_lifetimes<P: Producer<Vec<u8>, Vec<u8>>>(
    ctx: &mut TestContext,
    producer: P,
    backend: &str,
) {
    let topic = ctx.topic("sasl_reauthentication_topic");

    // The first send creates the topic.
    let deadline = Instant::now() + Duration::from_millis(7 * SESSION_LIFETIME_MS / 2);
    let mut sent = 0;
    while Instant::now() < deadline {
        producer
            .send(ProducerRecord::new(topic.clone(), Some(format!("value-{sent}").into_bytes())))
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: record {sent} must be accepted: {e:?}"))
            .get_with_timeout(Duration::from_secs(30))
            .await
            .unwrap_or_else(|e| panic!("{backend} backend: record {sent} must be acknowledged: {e:?}"));
        sent += 1;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(sent > 20, "{backend} backend: only {sent} records sent");

    producer
        .close()
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: close: {e}"));
}

/// A producer over SASL_PLAINTEXT keeps its connection to the partition leader
/// across several session lifetimes by re-authenticating it, as Java's does.
async fn sasl_plain_producer_reauthenticates_before_its_session_expires<F: ProducerBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
) {
    let backend = factory.name();
    let bootstrap = if factory.needs_container_bootstrap() {
        ctx.container_sasl_plaintext_bootstrap_servers()
    } else {
        ctx.sasl_plaintext_bootstrap_servers()
    };
    let props = with_producer_props(sasl_plain_props(bootstrap, "SASL_PLAINTEXT"));
    let producer = factory
        .create(props)
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: producer constructs: {e}"));
    produce_across_session_lifetimes(ctx, producer, backend).await;
    ctx.cleanup().await;
}

multilanguage_test!(
    test_sasl_plain_producer_reauthenticates_before_its_session_expires,
    sasl_plain_producer_reauthenticates_before_its_session_expires,
    cluster_config()
);

/// As [`sasl_plain_producer_reauthenticates_before_its_session_expires`],
/// over SASL_SSL: the re-authentication's handshake and token, and the
/// responses that arrive during it, go through TLS, which can hold several
/// decrypted frames at once.
async fn sasl_ssl_producer_reauthenticates_before_its_session_expires<F: ProducerBackendFactory>(
    ctx: &mut TestContext,
    factory: &F,
) {
    let backend = factory.name();
    let bootstrap = if factory.needs_container_bootstrap() {
        ctx.container_sasl_ssl_bootstrap_servers()
    } else {
        ctx.sasl_ssl_bootstrap_servers()
    };
    let mut props = sasl_plain_props(bootstrap, "SASL_SSL");
    props.extend([
        ("ssl.truststore.certificates".to_string(), ctx.ca_cert_pem().to_string()),
        // The native producer connects via 127.0.0.1; disable hostname
        // verification, as the other SASL_SSL suites do.
        ("ssl.endpoint.identification.algorithm".to_string(), String::new()),
    ]);
    let producer = factory
        .create(with_producer_props(props))
        .await
        .unwrap_or_else(|e| panic!("{backend} backend: producer constructs: {e}"));
    produce_across_session_lifetimes(ctx, producer, backend).await;
    ctx.cleanup().await;
}

multilanguage_test!(
    test_sasl_ssl_producer_reauthenticates_before_its_session_expires,
    sasl_ssl_producer_reauthenticates_before_its_session_expires,
    cluster_config()
);
