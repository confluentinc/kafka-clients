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

use confluent_kafka::common::network::NetworkSend;
use confluent_kafka::common::network::PlaintextChannelBuilder;
use confluent_kafka::common::network::selectable::{Selectable, USE_DEFAULT_BUFFER_SIZE};
use confluent_kafka::common::network::selector::{NO_IDLE_TIMEOUT_MS, Selector};
use confluent_kafka::common::protocol::{ApiKeys, ByteBufferAccessor, Errors};
use confluent_kafka::common::requests::ConcreteResponse;
use confluent_kafka::common::requests::{
    ApiVersionsRequestBuilder, MetadataRequestBuilder, MetadataResponse, RequestBuilder, RequestHeader,
    RequestHeaderOptions,
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
    Selector::with_defaults(NO_IDLE_TIMEOUT_MS, channel_builder)
}

/// Helper: parse address from bootstrap servers string.
fn parse_bootstrap_addr(bootstrap_servers: &str) -> SocketAddr {
    bootstrap_servers
        .parse::<SocketAddr>()
        .unwrap_or_else(|_| panic!("Failed to parse: {bootstrap_servers}"))
}

/// Helper: connect and wait until connected.
async fn connect_and_wait(selector: &mut Selector, addr: SocketAddr) {
    selector
        .connect(NODE_ID, addr, "localhost", USE_DEFAULT_BUFFER_SIZE, USE_DEFAULT_BUFFER_SIZE)
        .await
        .expect("Failed to connect");

    for _ in 0..MAX_POLL_ITERATIONS {
        selector.poll(POLL_TIMEOUT_MS).await.expect("poll failed");
        if !selector.connected().is_empty() {
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

    let header = RequestHeader::new_request_api_key_request_version_client_id_options(
        api_key,
        version,
        client_id,
        RequestHeaderOptions::new(correlation_id),
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

    let mut buffer = ByteBufferAccessor::from_bytes(payload);
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
        MetadataRequestBuilder::new_topics_allow_auto_topic_creation_version(topics, true, metadata_version);

    let (payload, header) = send_and_receive(selector, &mut builder, "metadata-test", correlation_id).await;

    let mut buffer = ByteBufferAccessor::from_bytes(payload);
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

    let mut selector = create_selector();
    let addr = parse_bootstrap_addr(ctx.bootstrap_servers());
    connect_and_wait(&mut selector, addr).await;

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

    let mut selector = create_selector();
    let addr = parse_bootstrap_addr(ctx.bootstrap_servers());
    connect_and_wait(&mut selector, addr).await;

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

    let mut selector = create_selector();
    let addr = parse_bootstrap_addr(ctx.bootstrap_servers());
    connect_and_wait(&mut selector, addr).await;

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

    let mut selector = create_selector();
    let addr = parse_bootstrap_addr(ctx.bootstrap_servers());
    connect_and_wait(&mut selector, addr).await;

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
