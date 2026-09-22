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

//! Integration tests for metadata discovery from a real Kafka broker.
//!
//! Verifies that the client can fetch and parse cluster metadata correctly.
//!
//! Maps to Java's `MetadataVersionIntegrationTest`.

use std::net::SocketAddr;

use confluent_kafka::common::config::{SaslConfig, SslConfig};
use confluent_kafka::common::network::NetworkSend;
use confluent_kafka::common::network::PlaintextChannelBuilder;
use confluent_kafka::common::network::SaslChannelBuilder;
use confluent_kafka::common::network::Selectable;
use confluent_kafka::common::network::Selector;
use confluent_kafka::common::network::SslChannelBuilder;
use confluent_kafka::common::security::SecurityProtocol;
use confluent_kafka::common::security::SslFactory;
use confluent_kafka::common::utils::LogContext;

/// Java writes `Selectable.USE_DEFAULT_BUFFER_SIZE`; Rust cannot name a trait
/// constant without a `Self` type (E0790), so bind it once per file.
const USE_DEFAULT_BUFFER_SIZE: i32 = <Selector as Selectable>::USE_DEFAULT_BUFFER_SIZE;
use confluent_kafka::common::protocol::{ApiKeys, ByteBufferAccessor, Errors};
use confluent_kafka::common::requests::ConcreteResponse;
use confluent_kafka::common::requests::{
    ApiVersionsRequestBuilder, MetadataRequestBuilder, MetadataResponse, RequestBuilder, RequestHeader,
    RequestHeaderOptionsBuilder,
};

use crate::common::cluster_config::ClusterConfig;
use crate::common::kafka_cluster::{SASL_PASSWORD, SASL_USERNAME};
use crate::common::test_context::{TestContext, TestProtocol};

/// Maximum time to wait for a poll to make progress, in milliseconds.
const POLL_TIMEOUT_MS: i64 = 5000;

/// Maximum number of poll iterations before giving up.
const MAX_POLL_ITERATIONS: usize = 100;

/// Node ID used for the connection to the broker.
const NODE_ID: &str = "0";

/// Helper: create a Selector whose channel builder matches the protocol this run
/// targets (`INTEGRATION_TEST_PROTOCOL`). PLAINTEXT is unchanged from before the
/// parameterization; SSL and SASL_SSL mirror the dedicated `ssl_sasl_test`
/// helpers (server-cert trust only, hostname verification off because tests
/// connect via `127.0.0.1`, SASL/PLAIN with `admin` / `admin-secret`).
fn create_selector(ctx: &TestContext) -> Selector {
    match ctx.protocol() {
        TestProtocol::Plaintext => {
            let channel_builder = Box::new(PlaintextChannelBuilder::new(None));
            Selector::with_defaults(Selector::NO_IDLE_TIMEOUT_MS, channel_builder)
        },
        TestProtocol::Ssl => {
            let ssl_config = SslConfig {
                truststore_certificates: Some(ctx.ca_cert_pem().to_string()),
                endpoint_identification_algorithm: String::new(),
                ..SslConfig::default()
            };
            let ssl_factory = SslFactory::new(&ssl_config).unwrap();
            let channel_builder = Box::new(SslChannelBuilder::new(ssl_factory, None));
            Selector::with_defaults(Selector::NO_IDLE_TIMEOUT_MS, channel_builder)
        },
        TestProtocol::SaslSsl => {
            let ssl_config = SslConfig {
                truststore_certificates: Some(ctx.ca_cert_pem().to_string()),
                endpoint_identification_algorithm: String::new(),
                ..SslConfig::default()
            };
            let ssl_factory = SslFactory::new(&ssl_config).unwrap();
            let sasl_config = SaslConfig {
                mechanism: "PLAIN".to_string(),
                username: Some(SASL_USERNAME.to_string()),
                password: Some(SASL_PASSWORD.to_string()),
                ..SaslConfig::default()
            };
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
        },
    }
}

/// Helper: parse address from bootstrap servers string.
fn parse_bootstrap_addr(bootstrap_servers: &str) -> SocketAddr {
    bootstrap_servers
        .parse::<SocketAddr>()
        .unwrap_or_else(|_| panic!("Failed to parse: {bootstrap_servers}"))
}

/// Helper: connect and wait until the channel is ready.
///
/// For PLAINTEXT the readiness signal is `connected()`, exactly as before the
/// protocol parameterization. For SSL / SASL_SSL the transport (and, for SASL,
/// the authentication handshake) completes over subsequent poll cycles, so
/// readiness is `is_channel_ready(NODE_ID)` — the same signal the dedicated
/// `ssl_sasl_test` waits on.
async fn connect_and_wait(selector: &mut Selector, ctx: &TestContext, addr: SocketAddr) {
    selector
        .connect(NODE_ID, addr, "localhost", USE_DEFAULT_BUFFER_SIZE, USE_DEFAULT_BUFFER_SIZE)
        .await
        .expect("Failed to connect");

    let plaintext = ctx.protocol() == TestProtocol::Plaintext;
    for _ in 0..MAX_POLL_ITERATIONS {
        selector.poll(POLL_TIMEOUT_MS).await.expect("poll failed");
        let ready = if plaintext {
            !selector.connected().is_empty()
        } else {
            selector.is_channel_ready(NODE_ID)
        };
        if ready {
            return;
        }
        if !selector.disconnected().is_empty() {
            panic!("Broker disconnected during connect: {:?}", selector.disconnected());
        }
    }
    panic!("Timed out waiting for connection");
}

/// Helper: send a request and wait for a response, returning the payload.
async fn send_and_receive(
    selector: &mut Selector,
    builder: &mut dyn RequestBuilder,
    client_id: &str,
    correlation_id: i32,
) -> (Vec<u8>, RequestHeader) {
    let api_key = builder.api_key();
    // Use oldest allowed version for maximum broker compatibility.
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
    let network_send = NetworkSend::new(NODE_ID, Box::new(send));

    selector.send(network_send).expect("Failed to queue send");

    for _ in 0..MAX_POLL_ITERATIONS {
        selector.poll(POLL_TIMEOUT_MS).await.expect("poll failed");
        if !selector.completed_receives().is_empty() {
            break;
        }
        if !selector.disconnected().is_empty() {
            panic!("Broker disconnected during poll: {:?}", selector.disconnected());
        }
    }

    let receives: Vec<_> = selector.completed_receives();
    assert!(!receives.is_empty(), "No response received from broker");

    let payload = receives[0].payload().expect("Response has no payload").to_vec();

    (payload, header)
}

/// Helper: perform the initial ApiVersions handshake and return the max
/// supported Metadata version from the broker.
async fn handshake_and_get_metadata_version(selector: &mut Selector) -> i16 {
    let mut builder = ApiVersionsRequestBuilder::new();
    let (payload, header) = send_and_receive(selector, &mut builder, "metadata-test", 1).await;

    let mut buffer = ByteBufferAccessor::new(payload);
    let response = ConcreteResponse::parse_response(&mut buffer, &header).expect("Failed to parse response");

    let ConcreteResponse::ApiVersions(avr) = response else {
        panic!("Expected ApiVersions response");
    };
    assert_eq!(avr.data().error_code, Errors::None.code());

    avr.api_version(ApiKeys::METADATA.id())
        .expect("Broker should support METADATA")
        .max_version
}

/// Helper: send a MetadataRequest and return the parsed MetadataResponse.
async fn send_metadata_request(
    selector: &mut Selector,
    metadata_version: i16,
    topics: Option<&[&str]>,
    correlation_id: i32,
) -> MetadataResponse {
    let mut builder =
        MetadataRequestBuilder::with_topics_allow_auto_topic_creation_version(topics, true, metadata_version);

    let (payload, header) = send_and_receive(selector, &mut builder, "metadata-test", correlation_id).await;

    let mut buffer = ByteBufferAccessor::new(payload);
    let response = ConcreteResponse::parse_response(&mut buffer, &header).expect("Failed to parse response");

    let ConcreteResponse::Metadata(mr) = response else {
        panic!("Expected Metadata response");
    };

    mr
}

/// Test: Fetch metadata for the cluster and verify broker information.
#[tokio::test]
async fn test_cluster_metadata_brokers() {
    let ctx = TestContext::new(ClusterConfig::default()).await;

    let mut selector = create_selector(&ctx);
    let addr = parse_bootstrap_addr(ctx.protocol_bootstrap_servers());
    connect_and_wait(&mut selector, &ctx, addr).await;

    let metadata_version = handshake_and_get_metadata_version(&mut selector).await;
    let metadata = send_metadata_request(&mut selector, metadata_version, None, 2).await;

    // Verify at least one broker
    assert!(!metadata.data().brokers.is_empty(), "Cluster should have at least one broker");

    // Verify broker details
    let broker = &metadata.data().brokers[0];
    assert!(broker.node_id >= 0, "Broker node_id should be non-negative");
    assert!(!broker.host.is_empty(), "Broker host should not be empty");
    assert!(broker.port > 0, "Broker port should be positive");

    selector.close().await;
}

/// Test: Verify controller information is present in metadata.
#[tokio::test]
async fn test_cluster_metadata_controller() {
    let ctx = TestContext::new(ClusterConfig::default()).await;

    let mut selector = create_selector(&ctx);
    let addr = parse_bootstrap_addr(ctx.protocol_bootstrap_servers());
    connect_and_wait(&mut selector, &ctx, addr).await;

    let metadata_version = handshake_and_get_metadata_version(&mut selector).await;
    let metadata = send_metadata_request(&mut selector, metadata_version, None, 2).await;

    // In a single-broker cluster, the controller should be the only broker
    let controller_id = metadata.data().controller_id;
    assert!(controller_id >= 0, "Controller ID should be non-negative, got {controller_id}");

    // The controller ID should match one of the reported brokers
    let controller_is_broker = metadata.data().brokers.iter().any(|b| b.node_id == controller_id);
    assert!(
        controller_is_broker,
        "Controller ID {controller_id} should match a broker in the cluster"
    );

    selector.close().await;
}

/// Test: Fetch metadata for a specific non-existent topic.
///
/// When requesting metadata for a topic that does not exist:
/// - If auto-create is enabled (default), the topic may be auto-created
/// - If auto-create is disabled, the topic should have an error (UNKNOWN_TOPIC_OR_PARTITION)
///
/// We test with the default configuration (auto-create enabled).
#[tokio::test]
async fn test_metadata_for_specific_topic() {
    let mut ctx = TestContext::new(ClusterConfig::default()).await;

    let mut selector = create_selector(&ctx);
    let addr = parse_bootstrap_addr(ctx.protocol_bootstrap_servers());
    connect_and_wait(&mut selector, &ctx, addr).await;

    let metadata_version = handshake_and_get_metadata_version(&mut selector).await;

    // Request metadata for a specific topic (unique to this test)
    let topic_name = ctx.topic("metadata_test_topic");
    let metadata = send_metadata_request(&mut selector, metadata_version, Some(&[&topic_name]), 2).await;

    // Should have exactly one topic in the response
    assert_eq!(metadata.data().topics.len(), 1, "Should have exactly one topic in response");

    let topic = &metadata.data().topics[0];
    assert_eq!(
        topic.name.as_deref(),
        Some(topic_name.as_str()),
        "Topic name should match the requested name"
    );

    // With auto-create enabled (default), the topic may be auto-created with no error,
    // or it may report UNKNOWN_TOPIC_OR_PARTITION if auto-create was not triggered yet.
    // Both are valid outcomes.
    let error = Errors::for_code(topic.error_code);
    assert!(
        error == Errors::None || error == Errors::UnknownTopicOrPartition || error == Errors::LeaderNotAvailable,
        "Topic error should be NONE, UNKNOWN_TOPIC_OR_PARTITION, or LEADER_NOT_AVAILABLE, got: {error:?}"
    );

    ctx.cleanup().await;
    selector.close().await;
}

/// Test: Fetch metadata for all topics (null topics list).
#[tokio::test]
async fn test_metadata_all_topics() {
    let ctx = TestContext::new(ClusterConfig::default()).await;

    let mut selector = create_selector(&ctx);
    let addr = parse_bootstrap_addr(ctx.protocol_bootstrap_servers());
    connect_and_wait(&mut selector, &ctx, addr).await;

    let metadata_version = handshake_and_get_metadata_version(&mut selector).await;

    // Request all-topics metadata
    let metadata = send_metadata_request(&mut selector, metadata_version, None, 2).await;

    // Should have broker info regardless of topics
    assert!(
        !metadata.data().brokers.is_empty(),
        "All-topics metadata should contain brokers"
    );

    // The topics list may be empty (no topics created yet) or may contain
    // internal topics. Either is valid for a fresh cluster.

    selector.close().await;
}
