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

//! Integration tests for SSL and SASL connections to a real Kafka broker.
//!
//! Tests:
//! 1. SSL connection with TLS-encrypted transport
//! 2. SASL_PLAINTEXT connection with PLAIN authentication
//! 3. SASL_SSL connection with TLS + SASL combined
//! 4. SASL authentication failure with wrong credentials
//! 5. SASL unsupported mechanism rejection

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;

use crate::admin::AdminClientConfig;
use crate::common::config::{SaslConfigs, SslConfigs};
use crate::common::network::NetworkSend;
use crate::common::network::SaslChannelBuilder;
use crate::common::network::Selectable;
use crate::common::network::Selector;
use crate::common::network::SslChannelBuilder;
use crate::common::network::channel_state::State;
use crate::common::network::selectable::USE_DEFAULT_BUFFER_SIZE;
use crate::common::protocol::{ApiKeys, ByteBufferAccessor, Errors};
use crate::common::requests::ConcreteResponse;
use crate::common::requests::{RequestBuilder, RequestHeader, RequestHeaderOptionsBuilder, api_versions_request};
use crate::common::security::auth::SecurityProtocol;
use crate::common::security::ssl::SslFactory;
use crate::common::utils::internals::LogContext;

use crate::integration_tests::common::cluster_config::ClusterConfig;
use crate::integration_tests::common::kafka_cluster::{SASL_PASSWORD, SASL_USERNAME};
use crate::integration_tests::common::test_context::TestContext;

/// Maximum time to wait for a poll to make progress, in milliseconds.
const POLL_TIMEOUT_MS: i64 = 5000;

/// Maximum number of poll iterations before giving up.
const MAX_POLL_ITERATIONS: usize = 100;

/// Node ID used for the connection to the broker.
const NODE_ID: &str = "0";

/// Helper: parse `host:port` from a bootstrap servers string.
fn parse_bootstrap_addr(bootstrap_servers: &str) -> SocketAddr {
    bootstrap_servers
        .parse::<SocketAddr>()
        .unwrap_or_else(|_| panic!("Failed to parse bootstrap servers address: {bootstrap_servers}"))
}

/// Helper: parses `ssl.*` / `sasl.*` properties the way a client does.
///
/// `SslConfigs` / `SaslConfigs` fields are crate-private, so — as in Java, where
/// they travel as a `Map<String, ?>` — they are populated from config properties.
/// `bootstrap.servers` is required by the parser but unused by these helpers.
fn client_config(props: &[(&str, &str)]) -> AdminClientConfig {
    let mut map: HashMap<String, String> = props.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
    map.insert(
        AdminClientConfig::BOOTSTRAP_SERVERS_CONFIG.to_string(),
        "unused:9092".to_string(),
    );
    AdminClientConfig::new(&map).unwrap()
}

/// Helper: SSL config trusting `ca_cert_pem`, hostname verification disabled
/// (tests connect via 127.0.0.1).
fn ssl_configs(ca_cert_pem: &str) -> SslConfigs {
    ssl_configs_with(ca_cert_pem, &[])
}

/// Helper: [`ssl_configs`] plus the `extra` properties.
fn ssl_configs_with(ca_cert_pem: &str, extra: &[(&str, &str)]) -> SslConfigs {
    let mut props = vec![
        (SslConfigs::SSL_TRUSTSTORE_CERTIFICATES_CONFIG, ca_cert_pem),
        (SslConfigs::SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, ""),
    ];
    props.extend_from_slice(extra);
    client_config(&props).ssl_config().clone()
}

/// Helper: SASL config for `mechanism` with credentials in `sasl.jaas.config`.
fn sasl_configs(mechanism: &str, login_module: &str, username: &str, password: &str) -> SaslConfigs {
    let jaas = format!("{login_module} required username=\"{username}\" password=\"{password}\";");
    client_config(&[
        (SaslConfigs::SASL_MECHANISM, mechanism),
        (SaslConfigs::SASL_JAAS_CONFIG, &jaas),
    ])
    .sasl_config()
    .clone()
}

const PLAIN_LOGIN_MODULE: &str = "org.apache.kafka.common.security.plain.PlainLoginModule";

/// Helper: create a Selector with SslChannelBuilder.
fn create_ssl_selector(ca_cert_pem: &str) -> Selector {
    create_ssl_selector_with(ca_cert_pem, &[])
}

/// Helper: [`create_ssl_selector`] with the `extra` `ssl.*` properties.
fn create_ssl_selector_with(ca_cert_pem: &str, extra: &[(&str, &str)]) -> Selector {
    let ssl_factory = SslFactory::new(&ssl_configs_with(ca_cert_pem, extra)).unwrap();
    let channel_builder = Box::new(SslChannelBuilder::new(ssl_factory, None));
    Selector::with_defaults(Selector::NO_IDLE_TIMEOUT_MS, channel_builder)
}

/// Helper: create a Selector with SaslChannelBuilder for SASL_PLAINTEXT.
fn create_sasl_plaintext_selector(username: &str, password: &str) -> Selector {
    let sasl_config = sasl_configs("PLAIN", PLAIN_LOGIN_MODULE, username, password);
    let channel_builder = SaslChannelBuilder::new(
        SecurityProtocol::SaslPlaintext,
        sasl_config,
        None,
        None,
        "integration-test",
        LogContext::empty(),
    )
    .unwrap();
    Selector::with_defaults(Selector::NO_IDLE_TIMEOUT_MS, Box::new(channel_builder))
}

/// Helper: create a Selector with SaslChannelBuilder for SASL_SSL.
fn create_sasl_ssl_selector(username: &str, password: &str, ca_cert_pem: &str) -> Selector {
    let ssl_factory = SslFactory::new(&ssl_configs(ca_cert_pem)).unwrap();
    let sasl_config = sasl_configs("PLAIN", PLAIN_LOGIN_MODULE, username, password);
    let channel_builder = SaslChannelBuilder::new(
        SecurityProtocol::SaslSsl,
        sasl_config,
        Some(ssl_factory),
        None,
        "integration-test",
        LogContext::empty(),
    )
    .unwrap();
    Selector::with_defaults(Selector::NO_IDLE_TIMEOUT_MS, Box::new(channel_builder))
}

/// Helper: poll until the channel is fully ready (transport + auth complete).
///
/// For SASL connections, the SASL authentication handshake completes in
/// poll cycles after the initial TCP connect.
async fn poll_until_ready(selector: &mut Selector, node_id: &str) {
    for _ in 0..MAX_POLL_ITERATIONS {
        selector.poll(POLL_TIMEOUT_MS).await.expect("poll failed");
        if selector.is_channel_ready(node_id) {
            return;
        }
        if !selector.disconnected().is_empty() {
            panic!("Broker disconnected during authentication: {:?}", selector.disconnected());
        }
    }
    panic!("Timed out waiting for channel to become ready (authentication may have stalled)");
}

/// Helper: poll until a completed receive appears or timeout.
async fn poll_until_receive(selector: &mut Selector) {
    for _ in 0..MAX_POLL_ITERATIONS {
        selector.poll(POLL_TIMEOUT_MS).await.expect("poll failed");
        if !selector.completed_receives().is_empty() {
            return;
        }
        if !selector.disconnected().is_empty() {
            panic!("Broker disconnected during poll: {:?}", selector.disconnected());
        }
    }
    panic!("Timed out waiting for a response from the broker");
}

/// Helper: poll until disconnection (for error tests).
async fn poll_until_disconnected(selector: &mut Selector) {
    for _ in 0..MAX_POLL_ITERATIONS {
        selector.poll(POLL_TIMEOUT_MS).await.expect("poll failed");
        if !selector.disconnected().is_empty() {
            return;
        }
    }
    panic!("Timed out waiting for disconnection from the broker");
}

/// Helper: build a NetworkSend for a request.
fn build_request_send(
    builder: &mut dyn RequestBuilder,
    client_id: &str,
    correlation_id: i32,
    destination: &str,
) -> (NetworkSend, RequestHeader) {
    let api_key = builder.api_key();
    let version = builder.oldest_allowed_version();
    let mut request = builder.build_version(version).expect("Failed to build request");

    let header = RequestHeader::with_options(
        RequestHeaderOptionsBuilder::new()
            .set_request_api_key(api_key)
            .set_request_version(version)
            .set_client_id(client_id)
            .set_correlation_id(correlation_id)
            .build()
            .unwrap(),
    )
    .expect("Failed to create request header");

    let send = request.to_send(&header).expect("Failed to serialize request");
    let network_send = NetworkSend::new(destination, Box::new(send));

    (network_send, header)
}

/// Helper: parse a response from a completed receive.
fn parse_response(payload: &[u8], request_header: &RequestHeader) -> ConcreteResponse {
    let mut buffer = ByteBufferAccessor::new(payload.to_vec());
    ConcreteResponse::parse_response(&mut buffer, request_header).expect("Failed to parse response")
}

/// Test: SSL connection — TLS handshake and ApiVersions over TLS.
#[tokio::test]
async fn test_ssl_connection() {
    let ctx = TestContext::new(ClusterConfig::default()).await;
    let ca_cert_pem = ctx.ca_cert_pem();
    let mut selector = create_ssl_selector(ca_cert_pem);
    let addr = parse_bootstrap_addr(ctx.ssl_bootstrap_servers());

    selector
        .connect(NODE_ID, addr, "localhost", USE_DEFAULT_BUFFER_SIZE, USE_DEFAULT_BUFFER_SIZE)
        .await
        .expect("Failed to connect");
    // TLS handshake completes over multiple poll cycles after TCP connect
    poll_until_ready(&mut selector, NODE_ID).await;

    // Send ApiVersions to verify data flows over TLS
    let mut builder = api_versions_request::Builder::new();
    let (send, header) = build_request_send(&mut builder, "ssl-test", 1, NODE_ID);
    selector.send(send).expect("Failed to queue send");
    poll_until_receive(&mut selector).await;

    let receives = selector.completed_receives();
    assert_eq!(1, receives.len());
    let response = parse_response(receives[0].payload().unwrap(), &header);
    let ConcreteResponse::ApiVersions(ref avr) = response else {
        panic!("Expected ApiVersions response, got: {response}");
    };
    assert_eq!(avr.data().error_code(), Errors::None.code());
    assert!(avr.api_version(ApiKeys::METADATA.id()).is_some(), "Should contain METADATA API");

    selector.close().await;
}

/// Helper: connects `selector` to `addr` and checks that the channel serves an
/// `ApiVersions` request.
async fn assert_serves_api_versions(selector: &mut Selector, addr: SocketAddr) {
    selector
        .connect(NODE_ID, addr, "localhost", USE_DEFAULT_BUFFER_SIZE, USE_DEFAULT_BUFFER_SIZE)
        .await
        .expect("Failed to connect");
    poll_until_ready(selector, NODE_ID).await;
    let mut builder = api_versions_request::Builder::new();
    let (send, header) = build_request_send(&mut builder, "ssl-test", 1, NODE_ID);
    selector.send(send).expect("Failed to queue send");
    poll_until_receive(selector).await;
    let receives = selector.completed_receives();
    assert_eq!(1, receives.len());
    let response = parse_response(receives[0].payload().unwrap(), &header);
    let ConcreteResponse::ApiVersions(ref avr) = response else {
        panic!("Expected ApiVersions response, got: {response}");
    };
    assert_eq!(avr.data().error_code(), Errors::None.code());
}

/// Helper: connects `selector` to `addr` and checks that the TLS handshake
/// fails: the channel is disconnected in the `AUTHENTICATION_FAILED` state,
/// never ready, within [`MAX_POLL_ITERATIONS`] polls.
async fn assert_handshake_fails(selector: &mut Selector, addr: SocketAddr) {
    selector
        .connect(NODE_ID, addr, "localhost", USE_DEFAULT_BUFFER_SIZE, USE_DEFAULT_BUFFER_SIZE)
        .await
        .expect("Failed to connect");
    poll_until_disconnected(selector).await;
    assert!(!selector.is_channel_ready(NODE_ID));
    let state = selector.disconnected().get(NODE_ID).expect("the channel is disconnected");
    assert_eq!(state.state(), State::AuthenticationFailed, "{state:?}");
}

/// Test: `ssl.cipher.suites` reaches the handshake.
///
/// The broker certificate is ECDSA (`rcgen`'s default key), so a TLS 1.2
/// `ECDHE_ECDSA` suite completes the handshake and serves a request, while a
/// list holding only an `ECDHE_RSA` suite has no suite the broker can use with
/// that certificate and the handshake fails.
#[tokio::test]
async fn test_ssl_connection_with_protocol_and_cipher_suites() {
    let ctx = TestContext::new(ClusterConfig::default()).await;
    let ca_cert_pem = ctx.ca_cert_pem();
    let addr = parse_bootstrap_addr(ctx.ssl_bootstrap_servers());

    let mut selector = create_ssl_selector_with(
        ca_cert_pem,
        &[
            (SslConfigs::SSL_PROTOCOL_CONFIG, "TLSv1.2"),
            (SslConfigs::SSL_CIPHER_SUITES_CONFIG, "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256"),
        ],
    );
    assert_serves_api_versions(&mut selector, addr).await;
    selector.close().await;

    let mut selector = create_ssl_selector_with(
        ca_cert_pem,
        &[
            (SslConfigs::SSL_PROTOCOL_CONFIG, "TLSv1.2"),
            (SslConfigs::SSL_CIPHER_SUITES_CONFIG, "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"),
        ],
    );
    assert_handshake_fails(&mut selector, addr).await;
    selector.close().await;
}

/// Test: `ssl.protocol` selects the default protocols and cipher suites, as
/// JSSE's `SSLContext.getInstance(ssl.protocol)` does, against a broker whose
/// SSL listener allows TLS 1.3 only
/// (`listener.name.tlsonly.ssl.enabled.protocols=TLSv1.3`):
///
/// - `TLSv1.2` with the default suites cannot use TLS 1.3, so the handshake
///   fails (Java: `No appropriate protocol`);
/// - `TLSv1.2` with a TLS 1.3 suite in `ssl.cipher.suites` uses TLS 1.3 and
///   connects, as Java does;
/// - `TLSv1.3` (the default) connects.
#[tokio::test]
async fn test_ssl_protocol_against_tls13_only_listener() {
    let props = BTreeMap::from([(
        "KAFKA_LISTENER_NAME_TLSONLY_SSL_ENABLED_PROTOCOLS".to_string(),
        "TLSv1.3".to_string(),
    )]);
    let ctx = TestContext::new(ClusterConfig::default().set_server_properties(props)).await;
    let ca_cert_pem = ctx.ca_cert_pem();
    let addr = parse_bootstrap_addr(ctx.ssl_bootstrap_servers());

    let mut selector = create_ssl_selector_with(ca_cert_pem, &[(SslConfigs::SSL_PROTOCOL_CONFIG, "TLSv1.2")]);
    assert_handshake_fails(&mut selector, addr).await;
    selector.close().await;

    let mut selector = create_ssl_selector_with(
        ca_cert_pem,
        &[
            (SslConfigs::SSL_PROTOCOL_CONFIG, "TLSv1.2"),
            (SslConfigs::SSL_CIPHER_SUITES_CONFIG, "TLS_AES_128_GCM_SHA256"),
        ],
    );
    assert_serves_api_versions(&mut selector, addr).await;
    selector.close().await;

    let mut selector = create_ssl_selector_with(ca_cert_pem, &[(SslConfigs::SSL_PROTOCOL_CONFIG, "TLSv1.3")]);
    assert_serves_api_versions(&mut selector, addr).await;
    selector.close().await;
}

/// Test: SASL_PLAINTEXT connection — SASL handshake, auth, metadata fetch.
#[tokio::test]
async fn test_sasl_plaintext_connection() {
    let ctx = TestContext::new(ClusterConfig::default()).await;
    let mut selector = create_sasl_plaintext_selector(SASL_USERNAME, SASL_PASSWORD);
    let addr = parse_bootstrap_addr(ctx.sasl_plaintext_bootstrap_servers());

    selector
        .connect(NODE_ID, addr, "localhost", USE_DEFAULT_BUFFER_SIZE, USE_DEFAULT_BUFFER_SIZE)
        .await
        .expect("Failed to connect");
    // SASL authentication completes over multiple poll cycles after TCP connect
    poll_until_ready(&mut selector, NODE_ID).await;

    // Verify with an ApiVersionsRequest
    let mut builder = api_versions_request::Builder::new();
    let (send, header) = build_request_send(&mut builder, "sasl-test", 1, NODE_ID);
    selector.send(send).expect("Failed to queue send");
    poll_until_receive(&mut selector).await;

    let receives = selector.completed_receives();
    assert_eq!(1, receives.len());
    let response = parse_response(receives[0].payload().unwrap(), &header);
    let ConcreteResponse::ApiVersions(ref avr) = response else {
        panic!("Expected ApiVersions response, got: {response}");
    };
    assert_eq!(avr.data().error_code(), Errors::None.code());

    selector.close().await;
}

/// Test: SASL_SSL connection — TLS + SASL combined.
#[tokio::test]
async fn test_sasl_ssl_connection() {
    let ctx = TestContext::new(ClusterConfig::default()).await;
    let ca_cert_pem = ctx.ca_cert_pem();
    let mut selector = create_sasl_ssl_selector(SASL_USERNAME, SASL_PASSWORD, ca_cert_pem);
    let addr = parse_bootstrap_addr(ctx.sasl_ssl_bootstrap_servers());

    selector
        .connect(NODE_ID, addr, "localhost", USE_DEFAULT_BUFFER_SIZE, USE_DEFAULT_BUFFER_SIZE)
        .await
        .expect("Failed to connect");
    // TLS handshake + SASL authentication completes over multiple poll cycles
    poll_until_ready(&mut selector, NODE_ID).await;

    // Verify with an ApiVersionsRequest
    let mut builder = api_versions_request::Builder::new();
    let (send, header) = build_request_send(&mut builder, "sasl-ssl-test", 1, NODE_ID);
    selector.send(send).expect("Failed to queue send");
    poll_until_receive(&mut selector).await;

    let receives = selector.completed_receives();
    assert_eq!(1, receives.len());
    let response = parse_response(receives[0].payload().unwrap(), &header);
    let ConcreteResponse::ApiVersions(ref avr) = response else {
        panic!("Expected ApiVersions response, got: {response}");
    };
    assert_eq!(avr.data().error_code(), Errors::None.code());

    selector.close().await;
}

/// Test: SASL authentication failure with wrong credentials.
#[tokio::test]
async fn test_sasl_wrong_credentials() {
    let ctx = TestContext::new(ClusterConfig::default()).await;
    // Wrong password
    let mut selector = create_sasl_plaintext_selector(SASL_USERNAME, "wrong-password");
    let addr = parse_bootstrap_addr(ctx.sasl_plaintext_bootstrap_servers());

    selector
        .connect(NODE_ID, addr, "localhost", USE_DEFAULT_BUFFER_SIZE, USE_DEFAULT_BUFFER_SIZE)
        .await
        .expect("Connect should succeed (TCP level)");

    // Poll until disconnect — broker rejects auth
    poll_until_disconnected(&mut selector).await;
    assert!(
        !selector.is_channel_ready(NODE_ID),
        "Channel should not be ready after auth failure"
    );

    selector.close().await;
}

/// Test: SASL unsupported mechanism rejection.
#[tokio::test]
async fn test_sasl_unsupported_mechanism() {
    let ctx = TestContext::new(ClusterConfig::default()).await;
    // Use SCRAM-SHA-256 mechanism which the broker doesn't have enabled
    let sasl_config = sasl_configs(
        "SCRAM-SHA-256",
        "org.apache.kafka.common.security.scram.ScramLoginModule",
        SASL_USERNAME,
        SASL_PASSWORD,
    );
    let channel_builder = SaslChannelBuilder::new(
        SecurityProtocol::SaslPlaintext,
        sasl_config,
        None,
        None,
        "integration-test",
        LogContext::empty(),
    )
    .unwrap();
    let mut selector = Selector::with_defaults(Selector::NO_IDLE_TIMEOUT_MS, Box::new(channel_builder));
    let addr = parse_bootstrap_addr(ctx.sasl_plaintext_bootstrap_servers());

    selector
        .connect(NODE_ID, addr, "localhost", USE_DEFAULT_BUFFER_SIZE, USE_DEFAULT_BUFFER_SIZE)
        .await
        .expect("Connect should succeed (TCP level)");

    // Poll until disconnect — broker rejects unsupported mechanism
    poll_until_disconnected(&mut selector).await;
    assert!(
        !selector.is_channel_ready(NODE_ID),
        "Channel should not be ready after unsupported mechanism"
    );

    selector.close().await;
}
