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

//! Integration tests for API version negotiation with a real Kafka broker.
//!
//! Verifies that the broker reports expected API keys and version ranges.
//!
//! Maps to Java's `NodeApiVersions` integration coverage.

use std::net::SocketAddr;

use confluent_kafka_rust::common::network::NetworkSend;
use confluent_kafka_rust::common::network::plaintext_channel_builder::PlaintextChannelBuilder;
use confluent_kafka_rust::common::network::selectable::{SELECTABLE_USE_DEFAULT_BUFFER_SIZE, Selectable};
use confluent_kafka_rust::common::network::selector::{SELECTOR_NO_IDLE_TIMEOUT_MS, Selector};
use confluent_kafka_rust::common::protocol::{ApiKeys, ByteBufferAccessor, Errors};
use confluent_kafka_rust::common::requests::abstract_response::ConcreteResponse;
use confluent_kafka_rust::common::requests::{
    ApiVersionsRequestBuilder, ApiVersionsResponse, RequestBuilder, RequestHeader,
};

use crate::common::cluster_config::ClusterConfig;
use crate::common::test_context::TestContext;

/// Maximum time to wait for a poll to make progress, in milliseconds.
const POLL_TIMEOUT_MS: i64 = 5000;

/// Maximum number of poll iterations before giving up.
const MAX_POLL_ITERATIONS: usize = 100;

/// Node ID used for the connection to the broker.
const NODE_ID: &str = "0";

/// Helper: create a Selector with PlaintextChannelBuilder.
fn create_selector() -> Selector {
    let channel_builder = Box::new(PlaintextChannelBuilder::new(None));
    Selector::with_defaults(SELECTOR_NO_IDLE_TIMEOUT_MS, channel_builder)
}

/// Helper: parse address from bootstrap servers string.
fn parse_bootstrap_addr(bootstrap_servers: &str) -> SocketAddr {
    bootstrap_servers
        .parse::<SocketAddr>()
        .unwrap_or_else(|_| panic!("Failed to parse bootstrap servers address: {bootstrap_servers}"))
}

/// Helper: send an ApiVersionsRequest and return the parsed response.
///
/// Uses version 0 for the initial handshake — the standard Kafka client behavior.
/// The broker always supports ApiVersions v0; higher versions may be unsupported
/// if our client's message spec is newer than the broker.
async fn send_api_versions_request(selector: &mut Selector) -> ApiVersionsResponse {
    let builder = ApiVersionsRequestBuilder::new();
    let api_key = builder.api_key();
    // Use oldest allowed version for the initial handshake — maximum broker compatibility.
    let version = builder.oldest_allowed_version();
    let request = builder.build_version(version).expect("Failed to build request");

    let header = RequestHeader::new(api_key, version, "api-versions-test", 1).expect("Failed to create request header");

    let send = request.to_send(&header).expect("Failed to serialize request");
    let network_send = NetworkSend::new(NODE_ID, Box::new(send));

    selector.send(network_send).expect("Failed to queue send");

    // Poll until we get a response
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

    let payload = receives[0].payload().expect("Response has no payload");
    let mut buffer = ByteBufferAccessor::from_bytes(payload.to_vec());
    let response = ConcreteResponse::parse_response(&mut buffer, &header).expect("Failed to parse response");

    let ConcreteResponse::ApiVersions(api_versions_response) = response else {
        panic!("Expected ApiVersions response");
    };

    api_versions_response
}

/// Test: Query all supported API versions and verify no error.
#[tokio::test]
async fn test_api_versions_no_error() {
    let ctx = TestContext::new(ClusterConfig::default()).await;

    let mut selector = create_selector();
    let addr = parse_bootstrap_addr(ctx.bootstrap_servers());

    selector
        .connect(
            NODE_ID,
            addr,
            "localhost",
            SELECTABLE_USE_DEFAULT_BUFFER_SIZE,
            SELECTABLE_USE_DEFAULT_BUFFER_SIZE,
        )
        .await
        .expect("Failed to connect");

    // Wait for connection
    for _ in 0..MAX_POLL_ITERATIONS {
        selector.poll(POLL_TIMEOUT_MS).await.expect("poll failed");
        if !selector.connected().is_empty() {
            break;
        }
    }

    let response = send_api_versions_request(&mut selector).await;

    assert_eq!(
        response.data().error_code,
        Errors::None.code(),
        "ApiVersionsResponse should have no error"
    );

    selector.close().await;
}

/// Test: Verify well-known APIs are present in the broker's response.
#[tokio::test]
async fn test_expected_apis_present() {
    let ctx = TestContext::new(ClusterConfig::default()).await;

    let mut selector = create_selector();
    let addr = parse_bootstrap_addr(ctx.bootstrap_servers());

    selector
        .connect(
            NODE_ID,
            addr,
            "localhost",
            SELECTABLE_USE_DEFAULT_BUFFER_SIZE,
            SELECTABLE_USE_DEFAULT_BUFFER_SIZE,
        )
        .await
        .expect("Failed to connect");

    for _ in 0..MAX_POLL_ITERATIONS {
        selector.poll(POLL_TIMEOUT_MS).await.expect("poll failed");
        if !selector.connected().is_empty() {
            break;
        }
    }

    let response = send_api_versions_request(&mut selector).await;

    // These core APIs must be supported by any Kafka broker
    let expected_apis = [
        ApiKeys::PRODUCE,
        ApiKeys::FETCH,
        ApiKeys::LIST_OFFSETS,
        ApiKeys::METADATA,
        ApiKeys::OFFSET_COMMIT,
        ApiKeys::OFFSET_FETCH,
        ApiKeys::FIND_COORDINATOR,
        ApiKeys::JOIN_GROUP,
        ApiKeys::HEARTBEAT,
        ApiKeys::LEAVE_GROUP,
        ApiKeys::SYNC_GROUP,
        ApiKeys::DESCRIBE_GROUPS,
        ApiKeys::LIST_GROUPS,
        ApiKeys::API_VERSIONS,
        ApiKeys::CREATE_TOPICS,
        ApiKeys::DELETE_TOPICS,
    ];

    for api in &expected_apis {
        let version_info = response.api_version(api.id());
        assert!(
            version_info.is_some(),
            "Expected API {} (id={}) to be present in broker response",
            api.name(),
            api.id()
        );
    }

    selector.close().await;
}

/// Test: Verify version ranges are reasonable (min <= max, both non-negative).
#[tokio::test]
async fn test_version_ranges_valid() {
    let ctx = TestContext::new(ClusterConfig::default()).await;

    let mut selector = create_selector();
    let addr = parse_bootstrap_addr(ctx.bootstrap_servers());

    selector
        .connect(
            NODE_ID,
            addr,
            "localhost",
            SELECTABLE_USE_DEFAULT_BUFFER_SIZE,
            SELECTABLE_USE_DEFAULT_BUFFER_SIZE,
        )
        .await
        .expect("Failed to connect");

    for _ in 0..MAX_POLL_ITERATIONS {
        selector.poll(POLL_TIMEOUT_MS).await.expect("poll failed");
        if !selector.connected().is_empty() {
            break;
        }
    }

    let response = send_api_versions_request(&mut selector).await;

    for api_version in &response.data().api_keys {
        assert!(
            api_version.min_version >= 0,
            "API {} min_version should be non-negative, got {}",
            api_version.api_key,
            api_version.min_version
        );
        assert!(
            api_version.max_version >= api_version.min_version,
            "API {} max_version ({}) should be >= min_version ({})",
            api_version.api_key,
            api_version.max_version,
            api_version.min_version
        );
    }

    selector.close().await;
}

/// Test: Verify the Metadata API supports at least version 0.
#[tokio::test]
async fn test_metadata_api_version_range() {
    let ctx = TestContext::new(ClusterConfig::default()).await;

    let mut selector = create_selector();
    let addr = parse_bootstrap_addr(ctx.bootstrap_servers());

    selector
        .connect(
            NODE_ID,
            addr,
            "localhost",
            SELECTABLE_USE_DEFAULT_BUFFER_SIZE,
            SELECTABLE_USE_DEFAULT_BUFFER_SIZE,
        )
        .await
        .expect("Failed to connect");

    for _ in 0..MAX_POLL_ITERATIONS {
        selector.poll(POLL_TIMEOUT_MS).await.expect("poll failed");
        if !selector.connected().is_empty() {
            break;
        }
    }

    let response = send_api_versions_request(&mut selector).await;

    let metadata_version = response
        .api_version(ApiKeys::METADATA.id())
        .expect("METADATA API should be in response");

    assert_eq!(metadata_version.min_version, 0, "METADATA API should support version 0");
    assert!(
        metadata_version.max_version >= 1,
        "METADATA API should support at least version 1, got max={}",
        metadata_version.max_version
    );

    selector.close().await;
}
