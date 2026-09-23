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

//! Integration tests for basic connection flow to a real Kafka broker.
//!
//! Tests:
//! 1. Connect to the broker using our Selector, over the protocol selected by
//!    `INTEGRATION_TEST_PROTOCOL` (PLAINTEXT, SSL or SASL_SSL)
//! 2. Send ApiVersionsRequest, receive ApiVersionsResponse
//! 3. Send MetadataRequest, receive MetadataResponse
//! 4. Verify response data
//!
//! Maps to Java's `MetadataVersionIntegrationTest` and `BootstrapControllersIntegrationTest`.

use std::net::SocketAddr;

use confluent_kafka::common::network::NetworkSend;
use confluent_kafka::common::network::Selectable;
use confluent_kafka::common::network::Selector;
use confluent_kafka::common::protocol::{ApiKeys, ByteBufferAccessor, Errors};
use confluent_kafka::common::requests::ConcreteResponse;
use confluent_kafka::common::requests::{
    ApiVersionsRequestBuilder, MetadataRequestBuilder, RequestBuilder, RequestHeader, RequestHeaderOptionsBuilder,
};

use crate::common::cluster_config::ClusterConfig;
use crate::common::test_context::TestContext;
use crate::common::test_utils::{connect_until_ready, protocol_selector};

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

/// Helper: poll the selector until a completed receive appears or timeout.
async fn poll_until_receive(selector: &mut Selector) {
    for _ in 0..MAX_POLL_ITERATIONS {
        selector.poll(POLL_TIMEOUT_MS).await.expect("poll failed");
        if !selector.completed_receives().is_empty() {
            return;
        }
        // Check for disconnection
        if !selector.disconnected().is_empty() {
            panic!("Broker disconnected during poll: {:?}", selector.disconnected());
        }
    }
    panic!("Timed out waiting for a response from the broker");
}

/// Helper: build a NetworkSend for a request.
///
/// Uses version 0 for maximum broker compatibility. Real clients would negotiate
/// versions via ApiVersions first, then use the broker-supported version.
fn build_request_send(
    builder: &mut dyn RequestBuilder,
    client_id: &str,
    correlation_id: i32,
    destination: &str,
) -> (NetworkSend, RequestHeader) {
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
    let network_send = NetworkSend::new(destination, Box::new(send));

    (network_send, header)
}

/// Helper: parse a response from a completed receive.
fn parse_response(payload: &[u8], request_header: &RequestHeader) -> ConcreteResponse {
    let mut buffer = ByteBufferAccessor::new(payload.to_vec());
    ConcreteResponse::parse_response(&mut buffer, request_header).expect("Failed to parse response")
}

/// Test: Connect to the broker and verify the connection is established.
#[tokio::test]
async fn test_tcp_connection() {
    let ctx = TestContext::new(ClusterConfig::default()).await;

    let mut selector = protocol_selector(&ctx);
    let addr = parse_bootstrap_addr(ctx.protocol_bootstrap_servers());
    connect_until_ready(&mut selector, &ctx, NODE_ID, addr).await;

    assert!(selector.is_channel_ready(NODE_ID), "Channel should be ready after connection");

    selector.close().await;
}

/// Test: Send ApiVersionsRequest and receive a valid ApiVersionsResponse.
#[tokio::test]
async fn test_api_versions_request_response() {
    let ctx = TestContext::new(ClusterConfig::default()).await;

    let mut selector = protocol_selector(&ctx);
    let addr = parse_bootstrap_addr(ctx.protocol_bootstrap_servers());

    // Connect
    connect_until_ready(&mut selector, &ctx, NODE_ID, addr).await;

    // Build and send ApiVersionsRequest
    let mut builder = ApiVersionsRequestBuilder::new();
    let (send, req_header) = build_request_send(&mut builder, "integration-test", 1, NODE_ID);

    selector.send(send).expect("Failed to queue send");
    poll_until_receive(&mut selector).await;

    // Parse response
    let receives: Vec<_> = selector.completed_receives();
    assert_eq!(1, receives.len(), "Expected exactly one completed receive");

    let payload = receives[0].payload().expect("Response has no payload");
    let response = parse_response(payload, &req_header);

    // Verify it's an ApiVersions response
    let ConcreteResponse::ApiVersions(api_versions_response) = &response else {
        panic!("Expected ApiVersions response, got: {response}");
    };

    // Verify no error
    assert_eq!(
        api_versions_response.data().error_code,
        Errors::None.code(),
        "ApiVersionsResponse should have no error"
    );

    // Verify API keys are present
    assert!(
        !api_versions_response.data().api_keys.is_empty(),
        "ApiVersionsResponse should contain API keys"
    );

    // Verify some well-known API keys are present
    let has_metadata = api_versions_response.api_version(ApiKeys::METADATA.id()).is_some();
    assert!(has_metadata, "ApiVersionsResponse should contain METADATA API");

    let has_api_versions = api_versions_response.api_version(ApiKeys::API_VERSIONS.id()).is_some();
    assert!(has_api_versions, "ApiVersionsResponse should contain API_VERSIONS API");

    selector.close().await;
}

/// Test: Full connection flow — ApiVersions followed by MetadataRequest.
#[tokio::test]
async fn test_full_connection_flow() {
    let ctx = TestContext::new(ClusterConfig::default()).await;

    let mut selector = protocol_selector(&ctx);
    let addr = parse_bootstrap_addr(ctx.protocol_bootstrap_servers());

    // Step 1: Connect
    connect_until_ready(&mut selector, &ctx, NODE_ID, addr).await;

    // Step 2: Send ApiVersionsRequest
    let mut api_versions_builder = ApiVersionsRequestBuilder::new();
    let (send, api_versions_header) = build_request_send(&mut api_versions_builder, "integration-test", 1, NODE_ID);

    selector.send(send).expect("Failed to queue ApiVersions send");
    poll_until_receive(&mut selector).await;

    let receives: Vec<_> = selector.completed_receives();
    assert_eq!(1, receives.len());
    let payload = receives[0].payload().expect("No payload");
    let api_versions_response = parse_response(payload, &api_versions_header);
    let ConcreteResponse::ApiVersions(ref avr) = api_versions_response else {
        panic!("Expected ApiVersions response");
    };
    assert_eq!(avr.data().error_code, Errors::None.code());

    // Step 3: Determine the Metadata version to use from the broker's supported range
    let metadata_version_info = avr
        .api_version(ApiKeys::METADATA.id())
        .expect("Broker should support METADATA API");
    let metadata_version = metadata_version_info.max_version;

    // Step 4: Send MetadataRequest (for all topics)
    let mut metadata_builder =
        MetadataRequestBuilder::with_topics_allow_auto_topic_creation_version(None, true, metadata_version);
    let (send, metadata_header) = build_request_send(&mut metadata_builder, "integration-test", 2, NODE_ID);

    selector.send(send).expect("Failed to queue Metadata send");
    poll_until_receive(&mut selector).await;

    let receives: Vec<_> = selector.completed_receives();
    assert_eq!(1, receives.len());
    let payload = receives[0].payload().expect("No payload");
    let metadata_response = parse_response(payload, &metadata_header);

    // Verify MetadataResponse
    let ConcreteResponse::Metadata(ref mr) = metadata_response else {
        panic!("Expected Metadata response, got: {metadata_response}");
    };

    // The cluster should have at least one broker
    assert!(
        !mr.data().brokers.is_empty(),
        "MetadataResponse should contain at least one broker"
    );

    // Verify broker information
    let broker = &mr.data().brokers[0];
    assert!(broker.node_id >= 0, "Broker node ID should be non-negative");
    assert!(!broker.host.is_empty(), "Broker host should not be empty");
    assert!(broker.port > 0, "Broker port should be positive");

    selector.close().await;
}
