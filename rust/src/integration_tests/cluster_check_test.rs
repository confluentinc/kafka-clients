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

//! KIP-1242 misrouted-connection detection against a real broker.
//!
//! The `NetworkClient` is driven directly, over metadata seeded from the
//! broker's own `Metadata` response with one field falsified, so the client
//! believes it is connecting to a different cluster or a different broker:
//! the situation Java's `ClientRebootstrapTest.testRebootstrapOnMetadataClusterCheckFail`
//! (0ef4a4c80e) produces by restarting two brokers with their client ports
//! swapped. That harness feature (`restartBrokersWithSwappedClientListenerPorts`)
//! has no counterpart in the Docker harness, and the falsified metadata
//! exercises the same client path: the ApiVersions v5 request carries the
//! expected cluster id and node id, the broker answers `REBOOTSTRAP_REQUIRED`
//! (`KafkaApis.handleApiVersionsRequest`), and the client disconnects and
//! rebootstraps.
//!
//! A broker older than 4.4 has no ApiVersions v5, so no check happens there:
//! the client falls back to v4 and the connection becomes ready whatever the
//! metadata says. Each test asserts whichever outcome the broker under test
//! supports, decided by the ApiVersions version range it reports.

use std::net::SocketAddr;
use std::sync::Arc;

use crate::common::Node;
use crate::common::internals::ClusterResourceListeners;
use crate::common::network::NetworkSend;
use crate::common::network::Selectable;
use crate::common::network::Selector;
use crate::common::protocol::{ApiKeys, ByteBufferAccessor, Errors};
use crate::common::requests::ConcreteResponse;
use crate::common::requests::{
    MetadataResponse, RequestBuilder, RequestHeader, RequestHeaderOptionsBuilder, api_versions_request,
    metadata_request,
};
use crate::common::utils::internals::LogContext;
use crate::common::utils::{SystemTime, Time};
use crate::{ApiVersions, DefaultHostResolver, KafkaClient, Metadata, MetadataRecoveryStrategy, NetworkClient};

use crate::integration_tests::common::cluster_config::ClusterConfig;
use crate::integration_tests::common::selector_utils::{connect_until_ready, protocol_selector};
use crate::integration_tests::common::test_context::TestContext;

const POLL_TIMEOUT_MS: i64 = 5000;
const MAX_POLL_ITERATIONS: usize = 100;
const NODE_ID: &str = "0";

/// What the broker reports, read over a raw selector connection.
struct BrokerInfo {
    /// The broker's own `Metadata` response (cluster id, brokers).
    metadata: MetadataResponse,
    /// Whether the broker accepts ApiVersions v5, the version with the
    /// KIP-1242 fields (Apache Kafka 4.4+).
    checks_cluster: bool,
}

async fn send_and_receive(
    selector: &mut Selector,
    builder: &mut dyn RequestBuilder,
    version: i16,
    correlation_id: i32,
) -> ConcreteResponse {
    let mut request = builder.build_version(version).expect("Failed to build request");
    let header = RequestHeader::with_options(
        RequestHeaderOptionsBuilder::new()
            .set_request_api_key(builder.api_key())
            .set_request_version(version)
            .set_client_id("cluster-check-test")
            .set_correlation_id(correlation_id)
            .build()
            .unwrap(),
    )
    .expect("Failed to create request header");
    let send = request.to_send(&header).expect("Failed to serialize request");
    selector
        .send(NetworkSend::new(NODE_ID, Box::new(send)))
        .expect("Failed to queue send");
    for _ in 0..MAX_POLL_ITERATIONS {
        selector.poll(POLL_TIMEOUT_MS).await.expect("poll failed");
        if !selector.completed_receives().is_empty() {
            break;
        }
        assert!(
            selector.disconnected().is_empty(),
            "Broker disconnected during poll: {:?}",
            selector.disconnected()
        );
    }
    let receives = selector.completed_receives();
    let payload = receives.first().expect("No response received from broker").payload().unwrap().to_vec();
    ConcreteResponse::parse_response(&mut ByteBufferAccessor::new(payload), &header).expect("Failed to parse response")
}

async fn broker_info(ctx: &TestContext) -> BrokerInfo {
    let mut selector = protocol_selector(ctx);
    let addr: SocketAddr = ctx.protocol_bootstrap_servers().parse().expect("bootstrap address");
    connect_until_ready(&mut selector, ctx, NODE_ID, addr).await;

    let mut builder = api_versions_request::Builder::new();
    let ConcreteResponse::ApiVersions(api_versions) = send_and_receive(&mut selector, &mut builder, 0, 1).await else {
        panic!("Expected ApiVersions response");
    };
    assert_eq!(api_versions.data().error_code(), Errors::None.code());
    let checks_cluster = api_versions
        .api_version(ApiKeys::API_VERSIONS.id())
        .expect("ApiVersions supported")
        .max_version()
        >= 5;
    let metadata_version = api_versions
        .api_version(ApiKeys::METADATA.id())
        .expect("Metadata supported")
        .max_version()
        .min(ApiKeys::METADATA.latest_version());

    let mut builder = metadata_request::Builder::with_topics_allow_auto_topic_creation_version(
        Some(&[]),
        false,
        metadata_version,
    );
    let ConcreteResponse::Metadata(metadata) = send_and_receive(&mut selector, &mut builder, metadata_version, 2).await
    else {
        panic!("Expected Metadata response");
    };
    selector.close().await;
    // Guard the version-dependent branches below against a harness that ran
    // another broker than the one asked for: 4.4 is the first release with
    // ApiVersions v5.
    let tag = std::env::var("INTEGRATION_TEST_BROKER_TAG")
        .ok()
        .filter(|tag| !tag.trim().is_empty())
        .unwrap_or_else(|| "4.2.0".to_string());
    let mut parts = tag.trim().split(['.', '-']).map(|p| p.parse::<u32>().unwrap_or(0));
    let release = (parts.next().unwrap_or(0), parts.next().unwrap_or(0));
    assert_eq!(
        release >= (4, 4),
        checks_cluster,
        "broker tag {tag}: ApiVersions v5 support should follow the release"
    );
    BrokerInfo { metadata, checks_cluster }
}

/// A client with the KIP-1242 check on (`rebootstrap` strategy, check
/// enabled), whose metadata holds `metadata` and bootstraps from the
/// broker's address.
fn client_over(
    ctx: &TestContext,
    metadata: &MetadataResponse,
) -> (NetworkClient<Selector, DefaultHostResolver>, Arc<Metadata>) {
    let state = Arc::new(Metadata::new(100, 1000, 300_000, ClusterResourceListeners::new()));
    let addr: SocketAddr = ctx.protocol_bootstrap_servers().parse().expect("bootstrap address");
    state.bootstrap(vec![(addr.ip().to_string(), addr)]);
    state.update_with_current_request_version(metadata, false, SystemTime.milliseconds());
    let mut client = NetworkClient::with_metadata_rebootstrap_trigger_ms(
        protocol_selector(ctx),
        Arc::clone(&state),
        "cluster-check-test",
        5,
        50,
        1000,
        -1,
        -1,
        30_000,
        10_000,
        30_000,
        true,
        Arc::new(ApiVersions::new()),
        DefaultHostResolver::new(),
        300_000,
        MetadataRecoveryStrategy::Rebootstrap,
        LogContext::empty(),
    );
    client.set_metadata_cluster_check_enable(true);
    (client, state)
}

/// Drives `client` until `node` is ready (`true`) or its connection failed
/// (`false`).
async fn connect(client: &mut NetworkClient<Selector, DefaultHostResolver>, node: &Node) -> bool {
    for _ in 0..200 {
        let now = SystemTime.milliseconds();
        if client.ready(node, now).await {
            return true;
        }
        if client.connection_failed(node) {
            return false;
        }
        client.poll(100, now).await;
    }
    panic!("node {node} neither became ready nor failed");
}

fn only_broker(metadata: &MetadataResponse) -> Node {
    let broker = metadata.data().brokers().first().expect("a broker").clone();
    Node::new(broker.node_id(), broker.host().to_string(), broker.port())
}

/// With the metadata the broker itself returned, the check passes (or, before
/// 4.4, is not made) and the connection becomes ready.
#[tokio::test]
async fn test_matching_cluster_id_and_node_id_connect() {
    let ctx = TestContext::new(ClusterConfig::default()).await;
    let info = broker_info(&ctx).await;
    let (mut client, metadata) = client_over(&ctx, &info.metadata);
    let node = only_broker(&info.metadata);

    assert!(connect(&mut client, &node).await, "the connection to {node} should be ready");
    assert!(!metadata.fetch().is_bootstrap_configured());
    client.close().await;
}

/// The client believes the broker belongs to another cluster. A 4.4 broker
/// answers `REBOOTSTRAP_REQUIRED`, so the client fails the connection and
/// rebootstraps; an older broker cannot check and the connection is ready.
#[tokio::test]
async fn test_wrong_cluster_id_rebootstraps() {
    let ctx = TestContext::new(ClusterConfig::default()).await;
    let info = broker_info(&ctx).await;
    let mut falsified = info.metadata.clone();
    falsified.data_mut().set_cluster_id(Some("not-this-cluster".to_string()));
    let (mut client, metadata) = client_over(&ctx, &falsified);
    assert_eq!(metadata.fetch().cluster_resource().cluster_id(), Some("not-this-cluster"));
    let node = only_broker(&falsified);

    let ready = connect(&mut client, &node).await;
    if info.checks_cluster {
        assert!(!ready, "a 4.4 broker must reject the misrouted connection");
        // `metadataUpdater.rebootstrap(now)`: the cache is back to the bootstrap
        // addresses, which carry no cluster id.
        assert!(metadata.fetch().is_bootstrap_configured());
        assert_eq!(metadata.fetch().cluster_resource().cluster_id(), None);
    } else {
        assert!(ready, "a broker without ApiVersions v5 performs no check");
        assert!(!metadata.fetch().is_bootstrap_configured());
    }
    client.close().await;
}

/// The client believes the broker at this address has another node id: the
/// swapped-ports case of Java's `testRebootstrapOnMetadataClusterCheckFail`.
#[tokio::test]
async fn test_wrong_node_id_rebootstraps() {
    let ctx = TestContext::new(ClusterConfig::default()).await;
    let info = broker_info(&ctx).await;
    let mut falsified = info.metadata.clone();
    let real_id = only_broker(&info.metadata).id();
    let wrong_id = real_id + 100;
    for broker in falsified.data_mut().brokers_mut().iter_mut() {
        broker.set_node_id(wrong_id);
    }
    falsified.data_mut().set_controller_id(wrong_id);
    let (mut client, metadata) = client_over(&ctx, &falsified);
    let node = only_broker(&falsified);
    assert_eq!(node.id(), wrong_id);

    let ready = connect(&mut client, &node).await;
    if info.checks_cluster {
        assert!(!ready, "a 4.4 broker must reject a connection naming another node id");
        assert!(metadata.fetch().is_bootstrap_configured());
    } else {
        assert!(ready, "a broker without ApiVersions v5 performs no check");
    }
    client.close().await;
}
