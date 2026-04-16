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

//! A mock implementation of [`KafkaClient`] for testing.
//!
//! Translated from `org.apache.kafka.clients.MockClient`.
//!
//! This mock allows tests to queue up future responses that will be matched
//! and delivered when `send()` is called, or to `respond()` to queued requests
//! during `poll()`.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};

use crate::common::node::Node;
use crate::common::requests::RequestBuilder;
use crate::common::requests::abstract_response::ConcreteResponse;

use super::RequestCompletionHandler;
use super::client_request::ClientRequest;
use super::client_response::ClientResponse;
use super::kafka_client::KafkaClient;
use super::least_loaded_node::LeastLoadedNode;

/// Connection state for a mock node.
#[derive(Debug, Clone)]
struct MockConnectionState {
    state: ConnState,
    throttled_until_ms: i64,
    ready_delayed_until_ms: i64,
    backing_off_until_ms: i64,
    unreachable_until_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConnState {
    Connecting,
    Connected,
    Disconnected,
}

impl MockConnectionState {
    fn new() -> Self {
        Self {
            state: ConnState::Disconnected,
            throttled_until_ms: 0,
            ready_delayed_until_ms: 0,
            backing_off_until_ms: 0,
            unreachable_until_ms: 0,
        }
    }

    fn is_ready(&self, now: i64) -> bool {
        self.state == ConnState::Connected && self.not_throttled(now)
    }

    fn not_throttled(&self, now: i64) -> bool {
        now > self.throttled_until_ms
    }

    fn is_backing_off(&self, now: i64) -> bool {
        now < self.backing_off_until_ms
    }

    fn is_unreachable(&self, now: i64) -> bool {
        now < self.unreachable_until_ms
    }

    fn is_ready_delayed(&self, now: i64) -> bool {
        now < self.ready_delayed_until_ms
    }

    fn disconnect(&mut self) {
        self.state = ConnState::Disconnected;
    }

    fn connection_delay(&self, now: i64) -> i64 {
        if self.state != ConnState::Disconnected {
            return i64::MAX;
        }
        if self.backing_off_until_ms > now {
            return self.backing_off_until_ms - now;
        }
        0
    }

    fn poll_delay_ms(&self, now: i64) -> i64 {
        if self.not_throttled(now) {
            return self.connection_delay(now);
        }
        self.throttled_until_ms - now
    }

    fn ready(&mut self, now: i64) -> bool {
        match self.state {
            ConnState::Connected => self.not_throttled(now),
            ConnState::Connecting => {
                if self.is_ready_delayed(now) {
                    return false;
                }
                self.state = ConnState::Connected;
                self.ready(now)
            },
            ConnState::Disconnected => {
                if self.is_backing_off(now) {
                    return false;
                } else if self.is_unreachable(now) {
                    self.backing_off_until_ms = now + 100;
                    return false;
                }
                self.state = ConnState::Connecting;
                self.ready(now)
            },
        }
    }
}

/// A queued future response to be delivered when a matching request is sent.
struct FutureResponse {
    /// If set, only match requests to this specific node.
    node: Option<Node>,
    /// The response body to deliver.
    response_body: Option<ConcreteResponse>,
    /// Whether to simulate a disconnection.
    disconnected: bool,
    /// Whether to simulate an unsupported version error.
    is_unsupported_request: bool,
}

/// A mock network client for use in testing code.
///
/// Translated from `org.apache.kafka.clients.MockClient`.
pub struct MockClient {
    /// Correlation ID counter.
    correlation: AtomicI32,
    /// Current time provider.
    time_provider: Arc<dyn Fn() -> i64 + Send + Sync>,
    /// Connection states per node.
    connections: HashMap<String, MockConnectionState>,
    /// Queued requests (sent but not yet responded to).
    requests: VecDeque<ClientRequest>,
    /// Queued responses (ready to be returned by poll).
    responses: VecDeque<ClientResponse>,
    /// Future responses (will be matched when send() is called).
    future_responses: VecDeque<FutureResponse>,
    /// Nodes served by this mock client.
    nodes: Vec<Node>,
    /// Whether the client is active.
    active: AtomicBool,
}

impl MockClient {
    /// Creates a new `MockClient` with the given nodes and time provider.
    pub fn new(nodes: Vec<Node>, time_provider: Arc<dyn Fn() -> i64 + Send + Sync>) -> Self {
        Self {
            correlation: AtomicI32::new(0),
            time_provider,
            connections: HashMap::new(),
            requests: VecDeque::new(),
            responses: VecDeque::new(),
            future_responses: VecDeque::new(),
            nodes,
            active: AtomicBool::new(true),
        }
    }

    fn connection_state(&mut self, id_string: &str) -> &mut MockConnectionState {
        self.connections
            .entry(id_string.to_string())
            .or_insert_with(MockConnectionState::new)
    }

    /// Returns whether the specified node is connected.
    pub fn is_connected(&mut self, id_string: &str) -> bool {
        self.connection_state(id_string).state == ConnState::Connected
    }

    /// Back off the connection to the specified node.
    pub fn backoff(&mut self, node: &Node, duration_ms: i64) {
        let now = (self.time_provider)();
        self.connection_state(node.id_string()).backing_off_until_ms = now + duration_ms;
    }

    /// Mark a node as unreachable for the specified duration.
    pub fn set_unreachable(&mut self, node: &Node, duration_ms: i64) {
        let now = (self.time_provider)();
        self.disconnect_node(node.id_string());
        self.connection_state(node.id_string()).unreachable_until_ms = now + duration_ms;
    }

    /// Throttle a node connection.
    pub fn throttle(&mut self, node: &Node, duration_ms: i64) {
        let now = (self.time_provider)();
        self.connection_state(node.id_string()).throttled_until_ms = now + duration_ms;
    }

    /// Delay the ready state for a node.
    pub fn delay_ready(&mut self, node: &Node, duration_ms: i64) {
        let now = (self.time_provider)();
        self.connection_state(node.id_string()).ready_delayed_until_ms = now + duration_ms;
    }

    /// Disconnect a node by ID string, creating disconnect responses for all
    /// pending requests to that node.
    ///
    /// Translated from `MockClient.disconnect(String)`.
    pub fn disconnect_by_id(&mut self, node_id: &str) {
        self.disconnect_node(node_id);
    }

    fn disconnect_node(&mut self, node_id: &str) {
        let now = (self.time_provider)();
        // Create disconnect responses for all pending requests to this node
        let mut remaining = VecDeque::new();
        while let Some(mut request) = self.requests.pop_front() {
            if request.destination() == node_id {
                let version = request.request_builder().latest_allowed_version();
                let header = request.make_header(version).expect("Failed to create header");
                let callback = request.take_callback();
                let response = ClientResponse::new(
                    header,
                    callback,
                    request.destination(),
                    request.created_time_ms(),
                    now,
                    true,
                    None,
                    None,
                    None,
                );
                self.responses.push_back(response);
            } else {
                remaining.push_back(request);
            }
        }
        self.requests = remaining;
        self.connection_state(node_id).disconnect();
    }

    /// Queue up a response that will be delivered when `poll()` is called.
    /// The next queued request will be matched.
    pub fn respond(&mut self, response: ConcreteResponse) {
        self.respond_with_disconnect(response, false);
    }

    /// Queue up a response with a possible disconnect flag.
    pub fn respond_with_disconnect(&mut self, response: ConcreteResponse, disconnected: bool) {
        let mut request = self.requests.pop_front().expect("No requests pending for inbound response");
        let now = (self.time_provider)();
        let version = request.request_builder().latest_allowed_version();
        let header = request.make_header(version).expect("Failed to create header");
        let callback = request.take_callback();
        let client_response = ClientResponse::new(
            header,
            callback,
            request.destination(),
            request.created_time_ms(),
            now,
            disconnected,
            None,
            None,
            Some(response),
        );
        self.responses.push_back(client_response);
    }

    /// Respond to the first pending request to the given node.
    pub fn respond_from(&mut self, response: ConcreteResponse, node: &Node) {
        self.respond_from_with_disconnect(response, node, false);
    }

    /// Respond to the first pending request to the given node, with a disconnect flag.
    pub fn respond_from_with_disconnect(&mut self, response: ConcreteResponse, node: &Node, disconnected: bool) {
        let now = (self.time_provider)();
        let node_id = node.id_string().to_string();
        let idx = self
            .requests
            .iter()
            .position(|r| r.destination() == node_id)
            .unwrap_or_else(|| panic!("No requests available to node {}", node));

        let mut request = self.requests.remove(idx).unwrap();
        let version = request.request_builder().latest_allowed_version();
        let header = request.make_header(version).expect("Failed to create header");
        let callback = request.take_callback();
        let client_response = ClientResponse::new(
            header,
            callback,
            request.destination(),
            request.created_time_ms(),
            now,
            disconnected,
            None,
            None,
            Some(response),
        );
        self.responses.push_back(client_response);
    }

    /// Prepare a future response that will be delivered when a matching request is sent.
    pub fn prepare_response(&mut self, response: ConcreteResponse) {
        self.prepare_response_from(None, response, false, false);
    }

    /// Prepare a future response from a specific node.
    pub fn prepare_response_for_node(&mut self, response: ConcreteResponse, node: &Node) {
        self.prepare_response_from(Some(node.clone()), response, false, false);
    }

    /// Prepare a disconnect response for a specific node.
    pub fn prepare_response_disconnected(&mut self, response: ConcreteResponse, disconnected: bool) {
        self.prepare_response_from(None, response, disconnected, false);
    }

    /// Prepare an unsupported version response.
    pub fn prepare_unsupported_version_response(&mut self) {
        self.future_responses.push_back(FutureResponse {
            node: None,
            response_body: None,
            disconnected: false,
            is_unsupported_request: true,
        });
    }

    fn prepare_response_from(
        &mut self,
        node: Option<Node>,
        response: ConcreteResponse,
        disconnected: bool,
        is_unsupported_version: bool,
    ) {
        self.future_responses.push_back(FutureResponse {
            node,
            response_body: Some(response),
            disconnected,
            is_unsupported_request: is_unsupported_version,
        });
    }

    /// Returns the number of pending requests.
    pub fn request_count(&self) -> usize {
        self.requests.len()
    }

    /// Returns the queued requests.
    pub fn requests(&self) -> &VecDeque<ClientRequest> {
        &self.requests
    }

    /// Returns a mutable reference to the queued requests.
    pub fn requests_mut(&mut self) -> &mut VecDeque<ClientRequest> {
        &mut self.requests
    }

    /// Returns `true` if there are pending responses or future responses.
    pub fn has_pending_responses(&self) -> bool {
        !self.responses.is_empty() || !self.future_responses.is_empty()
    }

    /// Returns the number of future responses.
    pub fn num_awaiting_responses(&self) -> usize {
        self.future_responses.len()
    }

    /// Reset all state.
    pub fn reset(&mut self) {
        self.connections.clear();
        self.requests.clear();
        self.responses.clear();
        self.future_responses.clear();
    }

    /// Set the nodes for this mock client.
    pub fn set_nodes(&mut self, nodes: Vec<Node>) {
        self.nodes = nodes;
    }
}

impl KafkaClient for MockClient {
    fn is_ready(&self, node: &Node, now: i64) -> bool {
        if let Some(state) = self.connections.get(node.id_string()) {
            state.is_ready(now)
        } else {
            false
        }
    }

    async fn ready(&mut self, node: &Node, now: i64) -> bool {
        self.connection_state(node.id_string()).ready(now)
    }

    fn connection_delay(&self, node: &Node, now: i64) -> i64 {
        if let Some(state) = self.connections.get(node.id_string()) {
            state.connection_delay(now)
        } else {
            0
        }
    }

    fn poll_delay_ms(&self, node: &Node, now: i64) -> i64 {
        if let Some(state) = self.connections.get(node.id_string()) {
            state.poll_delay_ms(now)
        } else {
            0
        }
    }

    fn connection_failed(&self, node: &Node) -> bool {
        if let Some(state) = self.connections.get(node.id_string()) {
            let now = (self.time_provider)();
            state.is_backing_off(now)
        } else {
            false
        }
    }

    fn authentication_error(&self, _node: &Node) -> Option<String> {
        None
    }

    fn send(&mut self, mut request: ClientRequest, now: i64) {
        let dest = request.destination().to_string();
        if !self.connections.get(&dest).map(|s| s.is_ready(now)).unwrap_or(false) {
            panic!("Cannot send {} since the destination is not ready", request);
        }

        // Check future responses
        let mut matched_idx = None;
        for (i, future_resp) in self.future_responses.iter().enumerate() {
            if let Some(ref node) = future_resp.node
                && dest != node.id_string()
            {
                continue;
            }
            matched_idx = Some(i);
            break;
        }

        if let Some(idx) = matched_idx {
            let future_resp = self.future_responses.remove(idx).unwrap();
            let version = request.request_builder().latest_allowed_version();
            let header = request.make_header(version).expect("Failed to create header");
            let callback = request.take_callback();

            let version_mismatch = if future_resp.is_unsupported_request {
                Some(format!("Api {} with version {}", request.api_key().name(), version))
            } else {
                None
            };

            let client_response = ClientResponse::new(
                header,
                callback,
                &dest,
                request.created_time_ms(),
                now,
                future_resp.disconnected,
                version_mismatch,
                None,
                future_resp.response_body,
            );
            self.responses.push_back(client_response);
            return;
        }

        self.requests.push_back(request);
    }

    async fn poll(&mut self, _timeout: i64, now: i64) -> Vec<ClientResponse> {
        // Check timeout of pending requests
        while let Some(request) = self.requests.front() {
            let elapsed = now.saturating_sub(request.created_time_ms()).max(0);
            if elapsed >= request.request_timeout_ms() as i64 {
                let dest = request.destination().to_string();
                self.disconnect_node(&dest);
            } else {
                break;
            }
        }

        let mut result = Vec::new();
        while let Some(mut response) = self.responses.pop_front() {
            response.on_complete();
            result.push(response);
        }
        result
    }

    async fn disconnect(&mut self, node_id: &str) {
        self.disconnect_node(node_id);
    }

    async fn close_connection(&mut self, node_id: &str) {
        self.connections.remove(node_id);
    }

    fn least_loaded_node(&self, now: i64) -> LeastLoadedNode {
        for node in &self.nodes {
            let backing_off = self
                .connections
                .get(node.id_string())
                .map(|s| s.is_backing_off(now))
                .unwrap_or(false);
            if !backing_off {
                return LeastLoadedNode::new(Some(node.clone()), true);
            }
        }
        LeastLoadedNode::new(None, false)
    }

    fn in_flight_request_count(&self) -> i32 {
        self.requests.len() as i32
    }

    fn has_in_flight_requests(&self) -> bool {
        !self.requests.is_empty()
    }

    fn in_flight_request_count_for_node(&self, node_id: &str) -> usize {
        self.requests.iter().filter(|r| r.destination() == node_id).count()
    }

    fn has_in_flight_requests_for_node(&self, node_id: &str) -> bool {
        self.in_flight_request_count_for_node(node_id) > 0
    }

    fn has_ready_nodes(&self, now: i64) -> bool {
        self.connections.values().any(|s| s.is_ready(now))
    }

    fn wakeup(&self) {
        // No-op for mock
    }

    fn new_client_request(
        &mut self,
        node_id: &str,
        request_builder: Box<dyn RequestBuilder>,
        created_time_ms: i64,
        expect_response: bool,
    ) -> ClientRequest {
        self.new_client_request_with_timeout(node_id, request_builder, created_time_ms, expect_response, 5000, None)
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
        let correlation_id = self.correlation.fetch_add(1, Ordering::SeqCst);
        ClientRequest::new(
            node_id,
            request_builder,
            correlation_id,
            "mockClientId",
            created_time_ms,
            expect_response,
            request_timeout_ms,
            callback,
        )
    }

    fn initiate_close(&self) {
        self.active.store(false, Ordering::SeqCst);
    }

    fn active(&self) -> bool {
        self.active.load(Ordering::SeqCst)
    }

    async fn close(&mut self) {
        self.active.store(false, Ordering::SeqCst);
    }
}
