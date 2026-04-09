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

//! A network client for asynchronous request/response network I/O.
//!
//! This is an internal class used to implement the user-facing producer and
//! consumer clients.
//!
//! Translated from `org.apache.kafka.clients.NetworkClient`.
//!
//! This class is not thread-safe!

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use log::{debug, error, info, trace, warn};
use rand::Rng;

use crate::common::network::channel_state::{self, ChannelState};
use crate::common::network::network_send::NetworkSend;
use crate::common::network::receive::Receive;
use crate::common::network::selectable::Selectable;
use crate::common::protocol::{ApiKeys, Errors};
use crate::common::requests::abstract_response::ConcreteResponse;
use crate::common::requests::api_versions_request::ApiVersionsRequestBuilder;
use crate::common::requests::api_versions_response::ApiVersionsResponse;
use crate::common::requests::metadata_request::MetadataRequestBuilder;
use crate::common::requests::metadata_response::MetadataResponse;
use crate::common::requests::{ConcreteRequest, RequestBuilder, RequestHeader};

use super::client_request::ClientRequest;
use super::client_response::ClientResponse;
use super::cluster_connection_states::ClusterConnectionStates;
use super::host_resolver::HostResolver;
use super::in_flight_requests::{InFlightRequest, InFlightRequests};
use super::kafka_client::KafkaClient;
use super::least_loaded_node::LeastLoadedNode;
use super::metadata::Metadata;
use super::metadata_recovery_strategy::MetadataRecoveryStrategy;
use super::metadata_updater::MetadataUpdater;
use super::{ApiVersions, NodeApiVersions, RequestCompletionHandler};

/// Polls a future to completion synchronously.
///
/// This utility is used by `NetworkClient` (which has a synchronous API) to call
/// `Selectable` methods that return `impl Future`. For the `MockSelector` used in
/// tests, these futures are always immediately ready, so no async runtime is needed.
///
/// # Panics
///
/// Panics if the future does not resolve immediately (i.e. returns `Pending`).
fn block_on<F: std::future::Future>(fut: F) -> F::Output {
    let mut fut = std::pin::pin!(fut);
    let waker = noop_waker();
    let mut cx = std::task::Context::from_waker(&waker);
    match fut.as_mut().poll(&mut cx) {
        std::task::Poll::Ready(val) => val,
        std::task::Poll::Pending => panic!("block_on called on a future that is not immediately ready"),
    }
}

/// Creates a no-op waker that does nothing when woken.
fn noop_waker() -> std::task::Waker {
    use std::task::{RawWaker, RawWakerVTable, Waker};
    fn no_op(_: *const ()) {}
    fn clone(data: *const ()) -> RawWaker {
        RawWaker::new(data, &VTABLE)
    }
    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, no_op, no_op, no_op);
    unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) }
}

/// Internal state enum for the client lifecycle.
const STATE_ACTIVE: u8 = 0;
const STATE_CLOSING: u8 = 1;
const STATE_CLOSED: u8 = 2;

/// Data for an in-progress metadata request.
struct InProgressData {
    request_version: i32,
    is_partial_update: bool,
}

/// A network client for asynchronous request/response network I/O.
///
/// This is an internal class used to implement the user-facing producer and
/// consumer clients.
///
/// This class is not thread-safe!
///
/// Translated from `org.apache.kafka.clients.NetworkClient`.
#[allow(dead_code)]
pub struct NetworkClient<S: Selectable, H: HostResolver> {
    /// The selector used to perform network I/O.
    selector: S,
    /// The state of each node's connection.
    connection_states: ClusterConnectionStates<H>,
    /// The set of requests currently being sent or awaiting a response.
    in_flight_requests: InFlightRequests,
    /// The socket send buffer size in bytes.
    socket_send_buffer: i32,
    /// The socket receive buffer size in bytes.
    socket_receive_buffer: i32,
    /// The client id used to identify this client in requests to the server.
    client_id: String,
    /// The current correlation id to use when sending requests to servers.
    correlation: i32,
    /// Default timeout for individual requests to await acknowledgement from servers.
    default_request_timeout_ms: i32,
    /// Time in ms to wait before retrying to create connection to a server.
    reconnect_backoff_ms: i64,
    /// Timeout starting from an attempt to fetch metadata after which client rebootstraps.
    rebootstrap_trigger_ms: i64,
    /// Metadata recovery strategy.
    metadata_recovery_strategy: MetadataRecoveryStrategy,
    /// True if we should send an ApiVersionRequest when first connecting to a broker.
    discover_broker_versions: bool,
    /// API versions for each node.
    api_versions: Arc<ApiVersions>,
    /// Nodes that need an ApiVersions fetch.
    nodes_needing_api_versions_fetch: HashMap<String, ApiVersionsRequestBuilder>,
    /// Aborted sends due to unsupported versions or disconnects.
    aborted_sends: Vec<ClientResponse>,
    /// The client state (ACTIVE, CLOSING, CLOSED).
    state: Arc<AtomicU8>,
    /// Random offset for round-robin node selection.
    rand_offset: rand::rngs::ThreadRng,

    // --- DefaultMetadataUpdater state (inlined from inner class) ---
    /// The metadata instance, or `None` if using an external MetadataUpdater.
    metadata: Option<Arc<Metadata>>,
    /// External metadata updater, if not using DefaultMetadataUpdater.
    external_metadata_updater: Option<Box<dyn MetadataUpdater>>,
    /// In-progress metadata fetch data.
    in_progress: Option<InProgressData>,
    /// The time in wall-clock milliseconds when we started attempts to fetch metadata.
    metadata_attempt_start_ms: Option<i64>,
}

impl<S: Selectable, H: HostResolver> NetworkClient<S, H> {
    /// Creates a new `NetworkClient` with a `Metadata` instance (uses DefaultMetadataUpdater internally).
    ///
    /// # Arguments
    ///
    /// * `selector` - The selector for network I/O
    /// * `metadata` - The metadata instance
    /// * `client_id` - The client ID
    /// * `max_in_flight_requests_per_connection` - Maximum in-flight requests per connection
    /// * `reconnect_backoff_ms` - Reconnect backoff in milliseconds
    /// * `reconnect_backoff_max_ms` - Maximum reconnect backoff in milliseconds
    /// * `socket_send_buffer` - Socket send buffer size
    /// * `socket_receive_buffer` - Socket receive buffer size
    /// * `default_request_timeout_ms` - Default request timeout in milliseconds
    /// * `connection_setup_timeout_ms` - Connection setup timeout in milliseconds
    /// * `connection_setup_timeout_max_ms` - Maximum connection setup timeout in milliseconds
    /// * `discover_broker_versions` - Whether to discover broker versions
    /// * `api_versions` - API versions instance
    /// * `host_resolver` - Host resolver implementation
    /// * `rebootstrap_trigger_ms` - Rebootstrap trigger timeout in milliseconds
    /// * `metadata_recovery_strategy` - Metadata recovery strategy
    #[allow(clippy::too_many_arguments)]
    pub fn with_metadata(
        selector: S,
        metadata: Arc<Metadata>,
        client_id: &str,
        max_in_flight_requests_per_connection: usize,
        reconnect_backoff_ms: i64,
        reconnect_backoff_max_ms: i64,
        socket_send_buffer: i32,
        socket_receive_buffer: i32,
        default_request_timeout_ms: i32,
        connection_setup_timeout_ms: i64,
        connection_setup_timeout_max_ms: i64,
        discover_broker_versions: bool,
        api_versions: Arc<ApiVersions>,
        host_resolver: H,
        rebootstrap_trigger_ms: i64,
        metadata_recovery_strategy: MetadataRecoveryStrategy,
    ) -> Self {
        Self {
            selector,
            connection_states: ClusterConnectionStates::new(
                reconnect_backoff_ms,
                reconnect_backoff_max_ms,
                connection_setup_timeout_ms,
                connection_setup_timeout_max_ms,
                host_resolver,
            ),
            in_flight_requests: InFlightRequests::new(max_in_flight_requests_per_connection),
            socket_send_buffer,
            socket_receive_buffer,
            client_id: client_id.to_string(),
            correlation: 0,
            default_request_timeout_ms,
            reconnect_backoff_ms,
            rebootstrap_trigger_ms,
            metadata_recovery_strategy,
            discover_broker_versions,
            api_versions,
            nodes_needing_api_versions_fetch: HashMap::new(),
            aborted_sends: Vec::new(),
            state: Arc::new(AtomicU8::new(STATE_ACTIVE)),
            rand_offset: rand::rng(),
            metadata: Some(metadata),
            external_metadata_updater: None,
            in_progress: None,
            metadata_attempt_start_ms: None,
        }
    }

    /// Creates a new `NetworkClient` with an external `MetadataUpdater`.
    ///
    /// # Arguments
    ///
    /// * `selector` - The selector for network I/O
    /// * `metadata_updater` - The external metadata updater
    /// * `client_id` - The client ID
    /// * `max_in_flight_requests_per_connection` - Maximum in-flight requests per connection
    /// * `reconnect_backoff_ms` - Reconnect backoff in milliseconds
    /// * `reconnect_backoff_max_ms` - Maximum reconnect backoff in milliseconds
    /// * `socket_send_buffer` - Socket send buffer size
    /// * `socket_receive_buffer` - Socket receive buffer size
    /// * `default_request_timeout_ms` - Default request timeout in milliseconds
    /// * `connection_setup_timeout_ms` - Connection setup timeout in milliseconds
    /// * `connection_setup_timeout_max_ms` - Maximum connection setup timeout in milliseconds
    /// * `discover_broker_versions` - Whether to discover broker versions
    /// * `api_versions` - API versions instance
    /// * `host_resolver` - Host resolver implementation
    /// * `metadata_recovery_strategy` - Metadata recovery strategy
    #[allow(clippy::too_many_arguments)]
    pub fn with_metadata_updater(
        selector: S,
        metadata_updater: Box<dyn MetadataUpdater>,
        client_id: &str,
        max_in_flight_requests_per_connection: usize,
        reconnect_backoff_ms: i64,
        reconnect_backoff_max_ms: i64,
        socket_send_buffer: i32,
        socket_receive_buffer: i32,
        default_request_timeout_ms: i32,
        connection_setup_timeout_ms: i64,
        connection_setup_timeout_max_ms: i64,
        discover_broker_versions: bool,
        api_versions: Arc<ApiVersions>,
        host_resolver: H,
        metadata_recovery_strategy: MetadataRecoveryStrategy,
    ) -> Self {
        Self {
            selector,
            connection_states: ClusterConnectionStates::new(
                reconnect_backoff_ms,
                reconnect_backoff_max_ms,
                connection_setup_timeout_ms,
                connection_setup_timeout_max_ms,
                host_resolver,
            ),
            in_flight_requests: InFlightRequests::new(max_in_flight_requests_per_connection),
            socket_send_buffer,
            socket_receive_buffer,
            client_id: client_id.to_string(),
            correlation: 0,
            default_request_timeout_ms,
            reconnect_backoff_ms,
            rebootstrap_trigger_ms: i64::MAX,
            metadata_recovery_strategy,
            discover_broker_versions,
            api_versions,
            nodes_needing_api_versions_fetch: HashMap::new(),
            aborted_sends: Vec::new(),
            state: Arc::new(AtomicU8::new(STATE_ACTIVE)),
            rand_offset: rand::rng(),
            metadata: None,
            external_metadata_updater: Some(metadata_updater),
            in_progress: None,
            metadata_attempt_start_ms: None,
        }
    }

    /// Returns whether broker version discovery is enabled.
    pub fn discover_broker_versions(&self) -> bool {
        self.discover_broker_versions
    }

    /// Returns a mutable reference to the underlying selector.
    ///
    /// Visible for testing.
    pub fn selector_mut(&mut self) -> &mut S {
        &mut self.selector
    }

    /// Returns a reference to the underlying selector.
    pub fn selector(&self) -> &S {
        &self.selector
    }

    /// Returns the next correlation ID to use.
    ///
    /// Visible for testing.
    fn next_correlation_id(&mut self) -> i32 {
        let id = self.correlation;
        self.correlation += 1;
        id
    }

    /// Checks if we are connected and able to send more requests to the given node.
    fn can_send_request(&self, node: &str, now: i64) -> bool {
        self.connection_states.is_ready(node, now)
            && self.selector.is_channel_ready(node)
            && self.in_flight_requests.can_send_more(node)
    }

    /// Visible for testing.
    pub fn can_connect(&self, node_id: &str, now: i64) -> bool {
        self.connection_states.can_connect(node_id, now)
    }

    /// Returns the remaining throttling delay in milliseconds if throttling is in progress.
    /// Returns 0 otherwise. This is for testing.
    pub fn throttle_delay_ms(&self, node: &crate::common::Node, now: i64) -> i64 {
        self.connection_states.throttle_delay_ms(node.id_string(), now)
    }

    /// Ensure the client is active.
    fn ensure_active(&self) {
        if !self.active() {
            panic!(
                "NetworkClient is no longer active, state is {}",
                self.state.load(Ordering::SeqCst)
            );
        }
    }

    /// Initiate a connection to the given node.
    #[allow(dead_code)]
    async fn initiate_connect(&mut self, node: &crate::common::Node, now: i64) {
        let node_connection_id = node.id_string();
        self.connection_states.connecting(node_connection_id, now, node.host());
        match self.connection_states.current_address(node_connection_id).await {
            Ok(address) => {
                debug!("Initiating connection to node {} using address {}", node, address);
                let addr = SocketAddr::new(address, node.port() as u16);
                if let Err(e) = self
                    .selector
                    .connect(node_connection_id, addr, self.socket_send_buffer, self.socket_receive_buffer)
                    .await
                {
                    warn!("Error connecting to node {}: {}", node, e);
                    self.connection_states.disconnected(node_connection_id, now);
                    self.handle_server_disconnect(now, node_connection_id, None);
                }
            },
            Err(e) => {
                warn!("Error connecting to node {}: {}", node, e);
                self.connection_states.disconnected(node_connection_id, now);
                self.handle_server_disconnect(now, node_connection_id, None);
            },
        }
    }

    /// Send an internal metadata request.
    ///
    /// Visible for testing.
    pub fn send_internal_metadata_request(
        &mut self,
        builder: MetadataRequestBuilder,
        node_connection_id: &str,
        now: i64,
    ) {
        let client_request = self.new_client_request(node_connection_id, Box::new(builder), now, true);
        self.do_send(client_request, true, now);
    }

    /// Queue up the given request for sending.
    fn do_send(&mut self, mut client_request: ClientRequest, is_internal_request: bool, now: i64) {
        self.ensure_active();
        let node_id = client_request.destination().to_string();
        if !is_internal_request && !self.can_send_request(&node_id, now) {
            panic!("Attempt to send a request to node {} which is not ready.", node_id);
        }

        let version_info = self.api_versions.get(&node_id);
        let version = if let Some(ref vi) = version_info {
            match vi.latest_usable_version_in_range(
                client_request.api_key(),
                client_request.request_builder().oldest_allowed_version(),
                client_request.request_builder().latest_allowed_version(),
            ) {
                Ok(v) => v,
                Err(_e) => {
                    debug!(
                        "Version mismatch when attempting to send {} with correlation id {} to {}",
                        client_request.request_builder().api_key().name(),
                        client_request.correlation_id(),
                        client_request.destination()
                    );
                    let header = client_request
                        .make_header(client_request.request_builder().latest_allowed_version())
                        .expect("Failed to create header");
                    let client_response = ClientResponse::new(
                        header,
                        client_request.take_callback(),
                        client_request.destination(),
                        now,
                        now,
                        false,
                        Some("UnsupportedVersionException".to_string()),
                        None,
                        None,
                    );
                    if !is_internal_request {
                        self.aborted_sends.push(client_response);
                    } else if *client_request.api_key() == ApiKeys::METADATA {
                        self.handle_failed_request(
                            now,
                            Some(super::metadata::MetadataError::Fatal("UnsupportedVersionException".to_string())),
                        );
                    }
                    return;
                },
            }
        } else {
            let latest = client_request.request_builder().latest_allowed_version();
            if self.discover_broker_versions {
                trace!(
                    "No version information found when sending {} with correlation id {} to node {}. Assuming version {}.",
                    client_request.api_key().name(),
                    client_request.correlation_id(),
                    node_id,
                    latest
                );
            }
            latest
        };

        // Build the request at the determined version
        match client_request.request_builder().build_version(version) {
            Ok(request) => {
                self.do_send_with_request(&mut client_request, is_internal_request, now, request);
            },
            Err(_e) => {
                debug!(
                    "Version mismatch when attempting to send {} with correlation id {} to {}",
                    client_request.request_builder().api_key().name(),
                    client_request.correlation_id(),
                    client_request.destination()
                );
                let header = client_request
                    .make_header(client_request.request_builder().latest_allowed_version())
                    .expect("Failed to create header");
                let client_response = ClientResponse::new(
                    header,
                    client_request.take_callback(),
                    client_request.destination(),
                    now,
                    now,
                    false,
                    Some("UnsupportedVersionException".to_string()),
                    None,
                    None,
                );
                if !is_internal_request {
                    self.aborted_sends.push(client_response);
                } else if *client_request.api_key() == ApiKeys::METADATA {
                    self.handle_failed_request(
                        now,
                        Some(super::metadata::MetadataError::Fatal("UnsupportedVersionException".to_string())),
                    );
                }
            },
        }
    }

    fn do_send_with_request(
        &mut self,
        client_request: &mut ClientRequest,
        is_internal_request: bool,
        now: i64,
        request: ConcreteRequest,
    ) {
        let destination = client_request.destination().to_string();
        let header = client_request
            .make_header(request.version())
            .expect("Failed to create header for send");

        debug!(
            "Sending {} request with header {} and timeout {} to node {}: {}",
            client_request.api_key().name(),
            header,
            client_request.request_timeout_ms(),
            destination,
            request,
        );

        let send = request.to_send(&header).expect("Failed to serialize request");

        // Create the NetworkSend from the serialized request.
        let network_send = NetworkSend::new(&destination, Box::new(send));

        let in_flight_request = InFlightRequest::from_client_request(
            client_request,
            header,
            is_internal_request,
            Some(request),
            network_send,
            now,
        );
        self.in_flight_requests.add(in_flight_request);

        // The InFlightRequest now owns the original NetworkSend. The selector
        // needs a separate send object for the actual I/O. We re-serialize
        // to create the send for the selector.
        let last_sent = self.in_flight_requests.last_sent(&destination);
        let selector_send = if let Some(ref req) = last_sent.request {
            let send_buf = req
                .to_send(&last_sent.header)
                .expect("Failed to serialize request for selector");
            NetworkSend::new(&destination, Box::new(send_buf))
        } else {
            // Fallback: empty send (should not happen in practice)
            NetworkSend::new(&destination, Box::new(crate::common::network::ByteBufferSend::new(Vec::new())))
        };
        let _ = self.selector.send(selector_send);
    }

    /// Handle any completed request sends. If no response is expected, consider the request complete.
    fn handle_completed_sends(&mut self, responses: &mut Vec<ClientResponse>, now: i64) {
        for send in self.selector.completed_sends() {
            let destination = send.destination_id().to_string();
            let request = self.in_flight_requests.last_sent(&destination);
            if !request.expect_response {
                let mut req = self.in_flight_requests.complete_last_sent(&destination);
                responses.push(req.completed(None, now));
            }
        }
    }

    /// Handle any completed receives and update the response list.
    fn handle_completed_receives(&mut self, responses: &mut Vec<ClientResponse>, now: i64) {
        // Collect owned copies so we release the borrow on self.selector
        // before calling &mut self methods.
        let receives: Vec<(String, Option<Vec<u8>>)> = self
            .selector
            .completed_receives()
            .iter()
            .map(|r| (Receive::source(*r).to_string(), r.payload().map(|p| p.to_vec())))
            .collect();

        for (source, payload) in receives {
            let mut req = self.in_flight_requests.complete_next(&source);

            if let Some(payload_bytes) = payload {
                // Parse the response
                let mut buf = crate::common::protocol::ByteBufferAccessor::from_bytes(payload_bytes);
                match ConcreteResponse::parse_response(&mut buf, &req.header) {
                    Ok(response) => {
                        // Handle throttle
                        self.maybe_throttle(&response, req.header.api_version(), &source, now);

                        if req.is_internal_request {
                            match &response {
                                ConcreteResponse::Metadata(metadata_response) => {
                                    self.handle_successful_metadata_response(&req.header, now, metadata_response);
                                },
                                ConcreteResponse::ApiVersions(api_versions_response) => {
                                    self.handle_api_versions_response(responses, &mut req, now, api_versions_response);
                                },
                            }
                        } else {
                            responses.push(req.completed(Some(response), now));
                        }
                    },
                    Err(e) => {
                        error!("Error parsing response from node {}: {}", source, e);
                        // Treat as disconnection
                        responses.push(req.disconnected(now));
                    },
                }
            } else {
                // No payload — treat as response with no body
                responses.push(req.completed(None, now));
            }
        }
    }

    /// Handle an ApiVersions response.
    fn handle_api_versions_response(
        &mut self,
        responses: &mut Vec<ClientResponse>,
        req: &mut InFlightRequest,
        now: i64,
        api_versions_response: &ApiVersionsResponse,
    ) {
        let node = req.destination.clone();
        if api_versions_response.data().error_code != Errors::None.code() {
            let request_version = req.request.as_ref().map(|r| r.version()).unwrap_or(0);
            if request_version == 0 || api_versions_response.data().error_code != Errors::UnsupportedVersion.code() {
                warn!(
                    "Received error {:?} from node {} when making an ApiVersionsRequest with correlation id {}. Disconnecting.",
                    Errors::for_code(api_versions_response.data().error_code),
                    node,
                    req.header.correlation_id()
                );
                block_on(self.selector.close_channel(&node));
                self.process_disconnection(
                    responses,
                    &node,
                    now,
                    ChannelState::new(channel_state::State::LocalClose),
                    false,
                );
            } else {
                // Starting from Apache Kafka 2.4, ApiKeys field is populated with the supported
                // versions of the ApiVersionsRequest when an UNSUPPORTED_VERSION error is returned.
                let mut max_api_version: i16 = 0;
                if !api_versions_response.data().api_keys.is_empty()
                    && let Some(api_version) = api_versions_response
                        .data()
                        .api_keys
                        .iter()
                        .find(|k| k.api_key == ApiKeys::API_VERSIONS.id())
                {
                    max_api_version = api_version.max_version;
                }
                self.nodes_needing_api_versions_fetch
                    .insert(node, ApiVersionsRequestBuilder::for_version(max_api_version));
            }
            return;
        }

        let node_version_info = NodeApiVersions::new(
            &api_versions_response.data().api_keys.to_vec(),
            &api_versions_response.data().supported_features.to_vec(),
            &api_versions_response.data().finalized_features.to_vec(),
            api_versions_response.data().finalized_features_epoch,
        );
        self.api_versions.update(&node, node_version_info);
        self.connection_states.ready(&node);
        debug!(
            "Node {} has finalized features epoch: {}, API versions updated.",
            node,
            api_versions_response.data().finalized_features_epoch,
        );
    }

    /// Handle disconnections.
    fn handle_disconnections(&mut self, responses: &mut Vec<ClientResponse>, now: i64) {
        // Collect disconnections to avoid borrow issues
        let disconnected: Vec<(String, ChannelState)> = self
            .selector
            .disconnected()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        for (node, channel_state) in disconnected {
            if channel_state == crate::common::network::channel_state::EXPIRED {
                debug!("Idle connection to node {} disconnected.", node);
            } else {
                info!("Node {} disconnected.", node);
            }
            self.process_disconnection(responses, &node, now, channel_state, false);
        }
    }

    /// Handle new connections.
    fn handle_connections(&mut self) {
        let connected: Vec<String> = self.selector.connected().to_vec();
        for node in connected {
            if self.discover_broker_versions {
                self.nodes_needing_api_versions_fetch
                    .insert(node.clone(), ApiVersionsRequestBuilder::new());
                debug!("Completed connection to node {}. Fetching API versions.", node);
            } else {
                self.connection_states.ready(&node);
                debug!("Completed connection to node {}. Ready.", node);
            }
        }
    }

    /// Initiate ApiVersion requests for nodes that need them.
    fn handle_initiate_api_version_requests(&mut self, now: i64) {
        let ready_nodes: Vec<(String, ApiVersionsRequestBuilder)> = self
            .nodes_needing_api_versions_fetch
            .iter()
            .filter(|(node, _)| self.selector.is_channel_ready(node) && self.in_flight_requests.can_send_more(node))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        for (node, builder) in ready_nodes {
            debug!("Initiating API versions fetch from node {}.", node);
            self.connection_states.checking_api_versions(&node);
            let client_request = self.new_client_request(&node, Box::new(builder), now, true);
            self.do_send(client_request, true, now);
            self.nodes_needing_api_versions_fetch.remove(&node);
        }
    }

    /// Handle connections that timed out.
    fn handle_timed_out_connections(&mut self, responses: &mut Vec<ClientResponse>, now: i64) {
        let nodes = self.connection_states.nodes_with_connection_setup_timeout(now);
        for node_id in nodes {
            block_on(self.selector.close_channel(&node_id));
            info!(
                "Disconnecting from node {} due to socket connection setup timeout. The timeout value is {} ms.",
                node_id,
                self.connection_states.connection_setup_timeout_ms(&node_id)
            );
            self.process_timeout_disconnection(responses, &node_id, now);
        }
    }

    /// Handle requests that timed out.
    fn handle_timed_out_requests(&mut self, responses: &mut Vec<ClientResponse>, now: i64) {
        let node_ids = self.in_flight_requests.nodes_with_timed_out_requests(now);
        for node_id in node_ids {
            block_on(self.selector.close_channel(&node_id));
            info!("Disconnecting from node {} due to request timeout.", node_id);
            self.process_timeout_disconnection(responses, &node_id, now);
        }
    }

    /// Handle aborted sends.
    fn handle_aborted_sends(&mut self, responses: &mut Vec<ClientResponse>) {
        responses.append(&mut self.aborted_sends);
    }

    /// Handle rebootstrap if needed.
    fn handle_rebootstrap(&mut self, responses: &mut Vec<ClientResponse>, now: i64) {
        if self.metadata_recovery_strategy == MetadataRecoveryStrategy::Rebootstrap && self.needs_rebootstrap(now) {
            let nodes = self.fetch_nodes();
            let node_ids: Vec<String> = nodes.iter().map(|n| n.id_string().to_string()).collect();
            for node_id in node_ids {
                block_on(self.selector.close_channel(&node_id));
                if self.connection_states.is_connecting(&node_id) || self.connection_states.is_connected(&node_id) {
                    info!("Disconnecting from node {} due to client rebootstrap.", node_id);
                    self.process_disconnection(
                        responses,
                        &node_id,
                        now,
                        ChannelState::new(channel_state::State::LocalClose),
                        false,
                    );
                }
            }
            self.rebootstrap(now);
        }
    }

    /// Complete all responses by invoking their callbacks.
    fn complete_responses(responses: &mut [ClientResponse]) {
        for response in responses.iter_mut() {
            if let Err(e) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                response.on_complete();
            })) {
                error!("Uncaught error in request completion: {:?}", e);
            }
        }
    }

    /// If a response includes a non-zero throttle delay and client-side throttling
    /// has been enabled, throttle the connection.
    fn maybe_throttle(&mut self, response: &ConcreteResponse, api_version: i16, node_id: &str, now: i64) {
        let throttle_time_ms = response.throttle_time_ms();
        if throttle_time_ms > 0 && response.should_client_throttle(api_version) {
            self.in_flight_requests
                .increment_throttle_time(node_id, throttle_time_ms as i64);
            self.connection_states.throttle(node_id, now + throttle_time_ms as i64);
            trace!(
                "Connection to node {} is throttled for {} ms until timestamp {}",
                node_id,
                throttle_time_ms,
                now + throttle_time_ms as i64
            );
        }
    }

    /// Post-process disconnection of a node.
    fn process_disconnection(
        &mut self,
        responses: &mut Vec<ClientResponse>,
        node_id: &str,
        now: i64,
        disconnect_state: ChannelState,
        timed_out: bool,
    ) {
        self.connection_states.disconnected(node_id, now);
        self.api_versions.remove(node_id);
        self.nodes_needing_api_versions_fetch.remove(node_id);

        match disconnect_state.state() {
            channel_state::State::AuthenticationFailed => {
                let exception = disconnect_state.exception().unwrap_or("unknown").to_string();
                self.connection_states.authentication_failed(node_id, now, exception.clone());
                error!(
                    "Connection to node {} ({}) failed authentication due to: {}",
                    node_id,
                    disconnect_state.remote_address().unwrap_or("unknown"),
                    exception
                );
            },
            channel_state::State::Authenticate => {
                warn!(
                    "Connection to node {} ({}) terminated during authentication. This may happen \
                     due to any of the following reasons: (1) Firewall blocking Kafka TLS \
                     traffic (eg it may only allow HTTPS traffic), (2) Transient network issue.",
                    node_id,
                    disconnect_state.remote_address().unwrap_or("unknown"),
                );
            },
            channel_state::State::NotConnected => {
                warn!(
                    "Connection to node {} ({}) could not be established. Node may not be available.",
                    node_id,
                    disconnect_state.remote_address().unwrap_or("unknown"),
                );
            },
            _ => {}, // Disconnections in other states are logged at debug level in Selector
        }

        self.cancel_in_flight_requests(node_id, now, Some(responses), timed_out);
        self.handle_server_disconnect(
            now,
            node_id,
            disconnect_state
                .exception()
                .map(|e| super::metadata::MetadataError::Fatal(e.to_string())),
        );
    }

    /// Post-process timeout disconnection.
    fn process_timeout_disconnection(&mut self, responses: &mut Vec<ClientResponse>, node_id: &str, now: i64) {
        self.process_disconnection(
            responses,
            node_id,
            now,
            ChannelState::new(channel_state::State::LocalClose),
            true,
        );
    }

    /// Cancel in-flight requests for a given node.
    fn cancel_in_flight_requests(
        &mut self,
        node_id: &str,
        now: i64,
        mut responses: Option<&mut Vec<ClientResponse>>,
        timed_out: bool,
    ) {
        let mut in_flight_requests = self.in_flight_requests.clear_all(node_id);
        for request in &mut in_flight_requests {
            debug!(
                "Cancelled in-flight {} request with correlation id {} due to node {} being disconnected \
                 (elapsed time since creation: {}ms, elapsed time since send: {}ms, \
                 throttle time: {}ms, request timeout: {}ms)",
                request.header.api_key().name(),
                request.header.correlation_id(),
                node_id,
                request.time_elapsed_since_create_ms(now),
                request.time_elapsed_since_send_ms(now),
                request.throttle_time_ms(),
                request.request_timeout_ms,
            );

            if !request.is_internal_request {
                if let Some(ref mut resp) = responses {
                    let client_response = if timed_out {
                        request.timed_out(now)
                    } else {
                        request.disconnected(now)
                    };
                    resp.push(client_response);
                }
            } else if *request.header.api_key() == ApiKeys::METADATA {
                self.handle_failed_request(now, None);
            }
        }
    }

    /// Check if any node connection is currently underway.
    fn is_any_node_connecting(&self) -> bool {
        let nodes = self.fetch_nodes();
        for node in nodes {
            if self.connection_states.is_connecting(node.id_string()) {
                return true;
            }
        }
        false
    }

    // --- DefaultMetadataUpdater delegation ---

    /// Gets the current cluster nodes from metadata.
    fn fetch_nodes(&self) -> Vec<crate::common::Node> {
        if let Some(ref metadata) = self.metadata {
            metadata.fetch().nodes().to_vec()
        } else if let Some(ref updater) = self.external_metadata_updater {
            updater.fetch_nodes()
        } else {
            Vec::new()
        }
    }

    /// Returns `true` if a metadata update is due.
    fn is_update_due(&self, now: i64) -> bool {
        if let Some(ref metadata) = self.metadata {
            !self.has_fetch_in_progress() && metadata.time_to_next_update(now) == 0
        } else if let Some(ref updater) = self.external_metadata_updater {
            updater.is_update_due(now)
        } else {
            false
        }
    }

    /// Returns `true` if there's a metadata fetch in progress.
    fn has_fetch_in_progress(&self) -> bool {
        self.in_progress.is_some()
    }

    /// Perform a metadata update if needed.
    fn maybe_update(&mut self, now: i64) -> i64 {
        if let Some(ref metadata) = self.metadata {
            self.default_maybe_update(now, metadata.clone())
        } else if let Some(ref mut updater) = self.external_metadata_updater {
            updater.maybe_update(now)
        } else {
            i64::MAX
        }
    }

    /// DefaultMetadataUpdater implementation of maybe_update.
    fn default_maybe_update(&mut self, now: i64, metadata: Arc<Metadata>) -> i64 {
        let time_to_next_metadata_update = metadata.time_to_next_update(now);
        let wait_for_metadata_fetch = if self.has_fetch_in_progress() {
            self.default_request_timeout_ms as i64
        } else {
            0
        };

        let metadata_timeout = time_to_next_metadata_update.max(wait_for_metadata_fetch);
        if metadata_timeout > 0 {
            return metadata_timeout;
        }

        if self.metadata_attempt_start_ms.is_none() {
            self.metadata_attempt_start_ms = Some(now);
        }

        let least_loaded = self.least_loaded_node(now);

        // Rebootstrap if needed and configured.
        if self.metadata_recovery_strategy == MetadataRecoveryStrategy::Rebootstrap
            && !least_loaded.has_node_available_or_connection_ready()
        {
            self.rebootstrap(now);
            // Re-evaluate after rebootstrap
            let least_loaded = self.least_loaded_node(now);
            if least_loaded.node().is_none() {
                debug!("Give up sending metadata request since no node is available");
                return self.reconnect_backoff_ms;
            }
            let node = least_loaded.node().unwrap().clone();
            return self.default_maybe_update_with_node(now, &node);
        }

        if least_loaded.node().is_none() {
            debug!("Give up sending metadata request since no node is available");
            return self.reconnect_backoff_ms;
        }

        let node = least_loaded.node().unwrap().clone();
        self.default_maybe_update_with_node(now, &node)
    }

    /// DefaultMetadataUpdater: try to send a metadata request to a specific node.
    fn default_maybe_update_with_node(&mut self, now: i64, node: &crate::common::Node) -> i64 {
        let node_connection_id = node.id_string();

        if self.can_send_request(node_connection_id, now) {
            let metadata = self.metadata.as_ref().unwrap().clone();
            let request_and_version = metadata.new_metadata_request_and_version(now);
            let metadata_request = request_and_version.request_builder;
            debug!("Sending metadata request {:?} to node {}", metadata_request, node);
            self.send_internal_metadata_request(metadata_request, node_connection_id, now);
            self.in_progress = Some(InProgressData {
                request_version: request_and_version.request_version,
                is_partial_update: request_and_version.is_partial_update,
            });
            return self.default_request_timeout_ms as i64;
        }

        if self.is_any_node_connecting() {
            return self.reconnect_backoff_ms;
        }

        if self.connection_states.can_connect(node_connection_id, now) {
            debug!("Initialize connection to node {} for sending metadata request", node);
            // We can't call async initiate_connect here, so we inline the sync parts
            self.connection_states.connecting(node_connection_id, now, node.host());
            // The actual TCP connection will be made in the next poll cycle
            // For the mock selector, connect() is synchronous so this works
            return self.reconnect_backoff_ms;
        }

        i64::MAX
    }

    /// Handle a successful metadata response (DefaultMetadataUpdater).
    fn handle_successful_metadata_response(
        &mut self,
        request_header: &RequestHeader,
        now: i64,
        response: &MetadataResponse,
    ) {
        if self.metadata.is_some() {
            let metadata = self.metadata.as_ref().unwrap().clone();

            if self.metadata_recovery_strategy == MetadataRecoveryStrategy::Rebootstrap
                && response.top_level_error() == Errors::RebootstrapRequired
            {
                info!("Rebootstrap requested by server.");
                self.metadata_attempt_start_ms = Some(0); // Force rebootstrap
            } else if response.brokers_by_id().is_empty() {
                trace!(
                    "Ignoring empty metadata response with correlation id {}.",
                    request_header.correlation_id()
                );
                metadata.failed_update(now);
            } else if let Some(ref in_progress) = self.in_progress {
                metadata.update(in_progress.request_version, response, in_progress.is_partial_update, now);
                self.metadata_attempt_start_ms = None;
            }

            self.in_progress = None;
        } else if let Some(ref mut updater) = self.external_metadata_updater {
            updater.handle_successful_response(request_header, now, response);
        }
    }

    /// Handle a failed metadata request (DefaultMetadataUpdater).
    fn handle_failed_request(&mut self, now: i64, maybe_fatal_exception: Option<super::metadata::MetadataError>) {
        if let Some(ref metadata) = self.metadata {
            if let Some(exception) = maybe_fatal_exception {
                metadata.fatal_error(exception);
            }
            metadata.failed_update(now);
            self.in_progress = None;
        } else if let Some(ref mut updater) = self.external_metadata_updater {
            updater.handle_failed_request(now, maybe_fatal_exception);
        }
    }

    /// Handle server disconnect (DefaultMetadataUpdater).
    fn handle_server_disconnect(
        &mut self,
        now: i64,
        node_id: &str,
        maybe_auth_exception: Option<super::metadata::MetadataError>,
    ) {
        if let Some(metadata) = self.metadata.clone() {
            let cluster = metadata.fetch();
            if cluster.is_bootstrap_configured()
                && let Ok(node_id_int) = node_id.parse::<i32>()
                && let Some(node) = cluster.node_by_id(node_id_int)
            {
                warn!("Bootstrap broker {} disconnected", node);
            }

            if self.is_update_due(now) {
                self.handle_failed_request(now, None);
            }

            if let Some(auth_exception) = maybe_auth_exception {
                metadata.fatal_error(auth_exception);
            }

            metadata.request_update(false);
        } else if let Some(ref mut updater) = self.external_metadata_updater {
            updater.handle_server_disconnect(now, node_id, maybe_auth_exception);
        }
    }

    /// Returns `true` if metadata couldn't be fetched for rebootstrap_trigger_ms.
    fn needs_rebootstrap(&self, now: i64) -> bool {
        if self.metadata.is_some() {
            if let Some(start_ms) = self.metadata_attempt_start_ms {
                return now - start_ms > self.rebootstrap_trigger_ms;
            }
            false
        } else if let Some(ref updater) = self.external_metadata_updater {
            updater.needs_rebootstrap(now, self.rebootstrap_trigger_ms)
        } else {
            false
        }
    }

    /// Performs rebootstrap.
    fn rebootstrap(&mut self, now: i64) {
        if let Some(ref metadata) = self.metadata {
            metadata.rebootstrap();
            self.metadata_attempt_start_ms = Some(now);
        } else if let Some(ref mut updater) = self.external_metadata_updater {
            updater.rebootstrap(now);
        }
    }
}

impl<S: Selectable, H: HostResolver> KafkaClient for NetworkClient<S, H> {
    fn is_ready(&self, node: &crate::common::Node, now: i64) -> bool {
        !self.is_update_due(now) && self.can_send_request(node.id_string(), now)
    }

    fn ready(&mut self, node: &crate::common::Node, now: i64) -> bool {
        if node.is_empty() {
            panic!("Cannot connect to empty node {}", node);
        }

        if self.is_ready(node, now) {
            return true;
        }

        if self.connection_states.can_connect(node.id_string(), now) {
            // Initiate connection synchronously for the mock selector case.
            // For real async, this would need to be called from an async context.
            self.connection_states.connecting(node.id_string(), now, node.host());
        }

        false
    }

    fn connection_delay(&self, node: &crate::common::Node, now: i64) -> i64 {
        self.connection_states.connection_delay(node.id_string(), now)
    }

    fn poll_delay_ms(&self, node: &crate::common::Node, now: i64) -> i64 {
        self.connection_states.poll_delay_ms(node.id_string(), now)
    }

    fn connection_failed(&self, node: &crate::common::Node) -> bool {
        self.connection_states.is_disconnected(node.id_string())
    }

    fn authentication_exception(&self, node: &crate::common::Node) -> Option<String> {
        self.connection_states
            .authentication_exception(node.id_string())
            .map(|s| s.to_string())
    }

    fn send(&mut self, request: ClientRequest, now: i64) {
        self.do_send(request, false, now);
    }

    fn poll(&mut self, timeout: i64, now: i64) -> Vec<ClientResponse> {
        self.ensure_active();

        if !self.aborted_sends.is_empty() {
            let mut responses = Vec::new();
            self.handle_aborted_sends(&mut responses);
            Self::complete_responses(&mut responses);
            return responses;
        }

        let metadata_timeout = self.maybe_update(now);
        let effective_timeout = timeout.min(metadata_timeout).min(self.default_request_timeout_ms as i64);

        let _poll_result = block_on(self.selector.poll(effective_timeout));

        // Process completed actions
        let mut responses = Vec::new();
        self.handle_completed_sends(&mut responses, now);
        self.handle_completed_receives(&mut responses, now);
        self.handle_disconnections(&mut responses, now);
        self.handle_connections();
        self.handle_initiate_api_version_requests(now);
        self.handle_timed_out_connections(&mut responses, now);
        self.handle_timed_out_requests(&mut responses, now);
        self.handle_rebootstrap(&mut responses, now);
        Self::complete_responses(&mut responses);

        responses
    }

    fn disconnect(&mut self, node_id: &str) {
        if self.connection_states.is_disconnected(node_id) {
            debug!(
                "Client requested disconnect from node {}, which is already disconnected",
                node_id
            );
            return;
        }

        info!("Client requested disconnect from node {}", node_id);
        block_on(self.selector.close_channel(node_id));
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let mut aborted = Vec::new();
        self.cancel_in_flight_requests(node_id, now, Some(&mut aborted), false);
        self.aborted_sends.extend(aborted);
        self.connection_states.disconnected(node_id, now);
    }

    fn close_connection(&mut self, node_id: &str) {
        info!("Client requested connection close from node {}", node_id);
        block_on(self.selector.close_channel(node_id));
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        self.cancel_in_flight_requests(node_id, now, None, false);
        self.connection_states.remove(node_id);
        self.api_versions.remove(node_id);
        self.nodes_needing_api_versions_fetch.remove(node_id);
    }

    fn least_loaded_node(&self, now: i64) -> LeastLoadedNode {
        let nodes = self.fetch_nodes();
        if nodes.is_empty() {
            return LeastLoadedNode::new(None, false);
        }

        let mut inflight = usize::MAX;
        let mut found_connecting = None;
        let mut found_can_connect: Option<crate::common::Node> = None;
        let mut found_ready = None;
        let mut at_least_one_connection_ready = false;

        let offset = self.rand_offset.clone().random_range(0..nodes.len());
        for i in 0..nodes.len() {
            let idx = (offset + i) % nodes.len();
            let node = &nodes[idx];

            if !at_least_one_connection_ready
                && self.connection_states.is_ready(node.id_string(), now)
                && self.selector.is_channel_ready(node.id_string())
            {
                at_least_one_connection_ready = true;
            }

            if self.can_send_request(node.id_string(), now) {
                let curr_inflight = self.in_flight_requests.count_for_node(node.id_string());
                if curr_inflight == 0 {
                    trace!("Found least loaded node {} connected with no in-flight requests", node);
                    return LeastLoadedNode::new(Some(node.clone()), true);
                } else if curr_inflight < inflight {
                    inflight = curr_inflight;
                    found_ready = Some(node.clone());
                }
            } else if self.connection_states.is_preparing_connection(node.id_string()) {
                found_connecting = Some(node.clone());
            } else if self.connection_states.can_connect(node.id_string(), now) {
                if found_can_connect.is_none()
                    || self
                        .connection_states
                        .last_connect_attempt_ms(found_can_connect.as_ref().unwrap().id_string())
                        > self.connection_states.last_connect_attempt_ms(node.id_string())
                {
                    found_can_connect = Some(node.clone());
                }
            } else {
                trace!(
                    "Removing node {} from least loaded node selection since it is neither ready for sending or connecting",
                    node
                );
            }
        }

        if let Some(ready) = found_ready {
            trace!("Found least loaded node {} with {} inflight requests", ready, inflight);
            LeastLoadedNode::new(Some(ready), at_least_one_connection_ready)
        } else if let Some(connecting) = found_connecting {
            trace!("Found least loaded connecting node {}", connecting);
            LeastLoadedNode::new(Some(connecting), at_least_one_connection_ready)
        } else if let Some(can_connect) = found_can_connect {
            trace!("Found least loaded node {} with no active connection", can_connect);
            LeastLoadedNode::new(Some(can_connect), at_least_one_connection_ready)
        } else {
            trace!("Least loaded node selection failed to find an available node");
            LeastLoadedNode::new(None, at_least_one_connection_ready)
        }
    }

    fn in_flight_request_count(&self) -> i32 {
        self.in_flight_requests.count()
    }

    fn has_in_flight_requests(&self) -> bool {
        !self.in_flight_requests.is_empty()
    }

    fn in_flight_request_count_for_node(&self, node_id: &str) -> usize {
        self.in_flight_requests.count_for_node(node_id)
    }

    fn has_in_flight_requests_for_node(&self, node_id: &str) -> bool {
        !self.in_flight_requests.is_empty_for_node(node_id)
    }

    fn has_ready_nodes(&self, now: i64) -> bool {
        self.connection_states.has_ready_nodes(now)
    }

    fn wakeup(&self) {
        self.selector.wakeup();
    }

    fn new_client_request(
        &mut self,
        node_id: &str,
        request_builder: Box<dyn RequestBuilder>,
        created_time_ms: i64,
        expect_response: bool,
    ) -> ClientRequest {
        self.new_client_request_with_timeout(
            node_id,
            request_builder,
            created_time_ms,
            expect_response,
            self.default_request_timeout_ms,
            None,
        )
    }

    fn new_client_request_with_timeout(
        &mut self,
        node_id: &str,
        request_builder: Box<dyn RequestBuilder>,
        created_time_ms: i64,
        expect_response: bool,
        request_timeout_ms: i32,
        callback: Option<RequestCompletionHandler>,
    ) -> ClientRequest {
        let correlation_id = self.next_correlation_id();
        ClientRequest::new(
            node_id,
            request_builder,
            correlation_id,
            &self.client_id,
            created_time_ms,
            expect_response,
            request_timeout_ms,
            callback,
        )
    }

    fn initiate_close(&self) {
        let _ = self
            .state
            .compare_exchange(STATE_ACTIVE, STATE_CLOSING, Ordering::SeqCst, Ordering::SeqCst);
        self.wakeup();
    }

    fn active(&self) -> bool {
        self.state.load(Ordering::SeqCst) == STATE_ACTIVE
    }

    fn close(&mut self) {
        let _ = self
            .state
            .compare_exchange(STATE_ACTIVE, STATE_CLOSING, Ordering::SeqCst, Ordering::SeqCst);
        if self
            .state
            .compare_exchange(STATE_CLOSING, STATE_CLOSED, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            block_on(self.selector.close());
            if let Some(ref metadata) = self.metadata {
                metadata.close();
            } else if let Some(ref mut updater) = self.external_metadata_updater {
                updater.close();
            }
        } else {
            warn!("Attempting to close NetworkClient that has already been closed.");
        }
    }
}
