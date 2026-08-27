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

#![allow(dead_code)]
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

use tokio::sync::Notify;

use crate::common::Node;
use crate::common::requests::ConcreteRequest;
use crate::common::requests::ConcreteResponse;
use crate::common::requests::RequestBuilder;

use super::ClientRequest;
use super::ClientResponse;
use super::KafkaClient;
use super::LeastLoadedNode;
use super::RequestCompletionHandler;

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

/// A predicate asserted against the request a prepared response is about to answer.
///
/// Translated from `MockClient.RequestMatcher` (`MockClient.java:623-625`), a
/// functional interface whose `matches(AbstractRequest)` Java evaluates inside
/// `send` and `respond`. Every `TransactionManagerTest` `prepare*`/`send*` helper
/// supplies one, and the assertions live *inside* it — so a port without matchers
/// silently drops them.
pub type RequestMatcher = Box<dyn Fn(&ConcreteRequest) -> bool + Send>;

/// A queued future response to be delivered when a matching request is sent.
struct FutureResponse {
    /// If set, only match requests to this specific node.
    node: Option<Node>,
    /// If set, asserted against the built request before the response is handed back.
    request_matcher: Option<RequestMatcher>,
    /// The response body to deliver.
    response_body: Option<ConcreteResponse>,
    /// Whether to simulate a disconnection.
    disconnected: bool,
    /// Whether to simulate an unsupported version error.
    is_unsupported_request: bool,
    /// A custom version-mismatch message to deliver (mirrors the message a real
    /// `NetworkClient` attaches when a request builder throws at build time,
    /// e.g. the `NoBatched*` exceptions). When set, the delivered
    /// `ClientResponse` carries this exact `version_mismatch` string.
    version_mismatch_message: Option<String>,
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
    /// When set, [`Self::least_loaded_node`] reports no node while a request is in
    /// flight, simulating `max.in.flight.requests.per.connection = 1`.
    ///
    /// The analogue of the anonymous `MockClient` subclass in
    /// `SenderTest.createMockClientWithMaxFlightOneMetadataPending`
    /// (`SenderTest.java:3956-3973`), which overrides `leastLoadedNode` to return
    /// `null` unless a `canSendMore` flag is set. Rust cannot subclass a concrete
    /// struct, so the override becomes a flag.
    max_in_flight_one: bool,
    /// Java's `canSendMore`: recomputed as `inFlightRequestCount() < 1` on every
    /// [`Self::poll`], and read by [`Self::least_loaded_node`].
    ///
    /// The snapshot semantics are load-bearing, not incidental: Java's helper sends a
    /// request and *then* polls until `leastLoadedNode` turns null, which only
    /// terminates because the flag lags a poll behind the in-flight count.
    can_send_more: bool,
    /// The `timeout_ms` of every [`KafkaClient::poll`] call, in order — see
    /// [`Self::poll_timeouts`].
    poll_timeouts: Vec<i64>,
    /// Whether the client is active.
    active: AtomicBool,
    /// Wakeup handle shared with callers of [`wakeup_notify`](Self::wakeup_notify),
    /// mirroring the real client's selector. `wakeup()` signals this same
    /// handle so a producer that cached it is actually woken.
    wakeup: Arc<Notify>,
    /// When set, [`KafkaClient::poll`] advances the caller's clock by its
    /// `timeout_ms` before returning, mirroring Java's
    /// `MockClient.advanceTimeDuringPoll` flag (`MockClient.java:74`, `:145-147`,
    /// applied at `:346-348`).
    ///
    /// Java's `MockClient` holds a whole `Time`, so its flag is a plain `boolean` and
    /// the sleep goes to `time.sleep(timeoutMs)`. This port holds only `Time`'s read
    /// half ([`Self::time_provider`]), so the write half is injected here instead.
    /// Threading Java's full `Time` interface through the producer is the larger change
    /// PLAN §9.19 tracks on its own account; this keeps the addition inside the mock.
    advance_time_during_poll: Option<Arc<dyn Fn(i64) + Send + Sync>>,
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
            max_in_flight_one: false,
            can_send_more: true,
            poll_timeouts: Vec::new(),
            active: AtomicBool::new(true),
            wakeup: Arc::new(Notify::new()),
            advance_time_during_poll: None,
        }
    }

    /// Makes [`KafkaClient::poll`] advance the caller's clock by its `timeout_ms`
    /// before returning, so a backoff or throttle a test installs actually expires.
    ///
    /// Translated from `MockClient.advanceTimeDuringPoll(boolean)`
    /// (`MockClient.java:145-147`). `Some(sleep)` is Java's `true` and `None` is its
    /// `false`; the closure supplies what Java reads off its `Time` field — see the
    /// [`Self::advance_time_during_poll`] field docs.
    pub fn advance_time_during_poll(&mut self, sleep: Option<Arc<dyn Fn(i64) + Send + Sync>>) {
        self.advance_time_during_poll = sleep;
    }

    /// Makes [`KafkaClient::least_loaded_node`] report no node while a request is in
    /// flight, simulating `max.in.flight.requests.per.connection = 1`.
    ///
    /// See [`Self::max_in_flight_one`].
    pub fn set_max_in_flight_one(&mut self, max_in_flight_one: bool) {
        self.max_in_flight_one = max_in_flight_one;
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

    /// Disconnects `node_id`, creating disconnect responses for all pending requests to
    /// that node.
    ///
    /// Translated from `MockClient.disconnect(String)` (`MockClient.java:196-198`), which
    /// delegates to the two-argument overload with `allowLateResponses = false`.
    ///
    /// # Why the `allowLateResponses` overload is not translated
    ///
    /// `MockClient.disconnect(String, boolean allowLateResponses)`
    /// (`MockClient.java:200-218`) retains the disconnected request so a later `respond*`
    /// can answer it a *second* time. In Java that second answer reaches the `Sender`
    /// because the routing lives on the request: `ClientRequest.callback()`
    /// (`ClientRequest.java:104-105`) is a plain **getter**, so the disconnect
    /// `ClientResponse` and the late one are handed the *same*
    /// `RequestCompletionHandler`, and `ClientResponse.onComplete`
    /// (`ClientResponse.java:152-154`) fires it both times.
    ///
    /// This port cannot reproduce that, because it deliberately does not route produce
    /// responses through callbacks: `Sender::send_produce_request` passes `None`
    /// (`sender.rs`, "No callback -- we process responses after poll() returns") and
    /// routes by correlation id through `Sender::pending_produce_responses`, which the
    /// **first** delivery `remove`s. A retained request's second answer therefore finds no
    /// entry and is discarded, so the overload would be inert here — it would only stop
    /// `respond*` from panicking on an empty queue.
    ///
    /// An earlier revision of this file did translate it, with a comment claiming Java's
    /// retained request "carries no callback". That is false, and it is why the overload is
    /// documented as absent rather than shipped inert. `SenderTest.testReceiveFailedBatchTwiceWithTransactions`
    /// is translated without it — see that test's rustdoc — and the divergence is tracked
    /// as `design/history/Milestone-11/PLAN.md` §9.28.
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
                // Java's `if (!allowLateResponses) iter.remove()` with
                // `allowLateResponses = false`: the request is dropped, not retained. See
                // [`Self::disconnect_by_id`] for why the retaining overload is not
                // translated.
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

    /// Queue up a response for the next pending request, first asserting `matcher`
    /// against it.
    ///
    /// Translated from `MockClient.respond(RequestMatcher, AbstractResponse)`
    /// (`MockClient.java:382-392`), which `TransactionManagerTest`'s `sendProduceResponse`
    /// / `sendAddPartitionsToTxnResponse` / `sendEndTxnResponse` use to answer a request
    /// that is *already* in flight.
    ///
    /// # Panics
    ///
    /// If no request is pending (Java's `:384-385`), or if `matcher` rejects it — Java
    /// throws `IllegalStateException` in the same place (`:388-389`).
    pub fn respond_with_matcher(&mut self, matcher: RequestMatcher, response: ConcreteResponse) {
        let built = self
            .requests
            .front_mut()
            .expect("No requests pending for inbound response")
            .request_builder_mut()
            .build()
            .expect("the pending request builds");
        assert!(matcher(&built), "Request matcher did not match next-in-line request {built}");
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

    /// Respond to the pending request at `index` in arrival order.
    ///
    /// The Rust analogue of Java's `MockClient.respondToRequest(ClientRequest,
    /// AbstractResponse)`, which `SenderTest` uses to answer in-flight requests out of
    /// order (e.g. `testCorrectHandlingOfDuplicateSequenceError`). Java identifies the
    /// request by identity; `ClientRequest` is not `Clone` here, so it is identified by
    /// position instead.
    ///
    /// # Panics
    ///
    /// If `index` is out of range.
    pub fn respond_to_request_at(&mut self, index: usize, response: ConcreteResponse) {
        let now = (self.time_provider)();
        let mut request = self
            .requests
            .remove(index)
            .unwrap_or_else(|| panic!("No pending request at index {index}"));
        let version = request.request_builder().latest_allowed_version();
        let header = request.make_header(version).expect("Failed to create header");
        let callback = request.take_callback();
        let client_response = ClientResponse::new(
            header,
            callback,
            request.destination(),
            request.created_time_ms(),
            now,
            false,
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
        self.prepare_response_from(None, None, response, false, false);
    }

    /// Prepare a future response, asserting `matcher` against the request it answers.
    ///
    /// Translated from `MockClient.prepareResponse(RequestMatcher, AbstractResponse)`
    /// (`MockClient.java:445-447`), which delegates to the disconnect overload with
    /// `disconnected = false`.
    pub fn prepare_response_with_matcher(&mut self, matcher: RequestMatcher, response: ConcreteResponse) {
        self.prepare_response_from(None, Some(matcher), response, false, false);
    }

    /// Prepare a future response with both a matcher and a disconnect flag.
    ///
    /// Translated from
    /// `MockClient.prepareResponse(RequestMatcher, AbstractResponse, boolean)`
    /// (`MockClient.java:472-474`).
    pub fn prepare_response_with_matcher_disconnected(
        &mut self,
        matcher: RequestMatcher,
        response: ConcreteResponse,
        disconnected: bool,
    ) {
        self.prepare_response_from(None, Some(matcher), response, disconnected, false);
    }

    /// Prepare a future response from a specific node.
    pub fn prepare_response_for_node(&mut self, response: ConcreteResponse, node: &Node) {
        self.prepare_response_from(Some(node.clone()), None, response, false, false);
    }

    /// Prepare a disconnect response for a specific node.
    pub fn prepare_response_disconnected(&mut self, response: ConcreteResponse, disconnected: bool) {
        self.prepare_response_from(None, None, response, disconnected, false);
    }

    /// Prepare an unsupported version response.
    pub fn prepare_unsupported_version_response(&mut self) {
        self.future_responses.push_back(FutureResponse {
            node: None,
            request_matcher: None,
            response_body: None,
            disconnected: false,
            is_unsupported_request: true,
            version_mismatch_message: None,
        });
    }

    /// Prepare a version-mismatch response carrying a custom message, mirroring
    /// the `ClientResponse` a real `NetworkClient` synthesizes when a request
    /// builder throws an `UnsupportedVersionException` (including the
    /// `NoBatched*` subclasses) at build time.
    pub fn prepare_version_mismatch_response(&mut self, message: impl Into<String>) {
        self.future_responses.push_back(FutureResponse {
            node: None,
            response_body: None,
            disconnected: false,
            is_unsupported_request: true,
            version_mismatch_message: Some(message.into()),
            request_matcher: None,
        });
    }

    fn prepare_response_from(
        &mut self,
        node: Option<Node>,
        request_matcher: Option<RequestMatcher>,
        response: ConcreteResponse,
        disconnected: bool,
        is_unsupported_version: bool,
    ) {
        self.future_responses.push_back(FutureResponse {
            node,
            request_matcher,
            response_body: Some(response),
            disconnected,
            is_unsupported_request: is_unsupported_version,
            version_mismatch_message: None,
        });
    }

    /// The `timeout_ms` of every [`KafkaClient::poll`] call, in order.
    ///
    /// Stands in for Mockito's `verify(client, times(n)).poll(eq(timeout), anyLong())`,
    /// which `SenderTest.testDoNotPollWhenNoRequestSent` (Java 3001) uses to assert
    /// that a `runOnce` which sends nothing also polls nothing. Recording the timeout
    /// rather than a bare count is what makes the `eq(RETRY_BACKOFF_MS)` matcher
    /// expressible: `Sender` polls with two different timeouts and only the
    /// transactional one is being counted.
    pub fn poll_timeouts(&self) -> &[i64] {
        &self.poll_timeouts
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
        self.poll_timeouts.clear();
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
            // Java builds the request unconditionally here (`MockClient.java:259`, inside
            // `send`) and then evaluates the matcher — unconditionally because its
            // matcher-less overloads default to `ALWAYS_TRUE` (`:49`, used at `:432` /
            // `:458`), so there is always a matcher to run. This port builds it only when a
            // matcher is
            // present, because `build()` on a `ProduceRequestBuilder` *moves* the
            // serialized records out of the batch (PLAN §9.18) — a side effect Java's
            // `build()` does not have. Nothing downstream of a matched future response
            // reads the request body, so the narrower build is equivalent; doing it
            // unconditionally would make every existing matcher-less test pay it.
            if let Some(matcher) = future_resp.request_matcher {
                let built = request.request_builder_mut().build().expect("the request builds");
                assert!(matcher(&built), "Request matcher did not match next-in-line request {built}");
            }
            let version = request.request_builder().latest_allowed_version();
            let header = request.make_header(version).expect("Failed to create header");
            let callback = request.take_callback();

            let version_mismatch = if let Some(message) = future_resp.version_mismatch_message {
                Some(message)
            } else if future_resp.is_unsupported_request {
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
        self.poll_timeouts.push(_timeout);

        // Java 3970: `canSendMore = inFlightRequestCount() < 1`, recomputed before the
        // superclass poll so the flag a later `leastLoadedNode` reads is this poll's
        // snapshot.
        self.can_send_more = self.in_flight_request_count() < 1;

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

        // Java 344-348: "In real life, if poll() is called and we get to the end with no
        // responses, time equal to timeoutMs would have passed." Applied
        // unconditionally at the end of the poll, as Java does — not only when `result`
        // is empty.
        if let Some(sleep) = &self.advance_time_during_poll {
            sleep(_timeout);
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
        if self.max_in_flight_one && !self.can_send_more {
            return LeastLoadedNode::new(None, false);
        }
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
        self.wakeup.notify_one();
    }

    fn wakeup_handle(&self) -> Arc<tokio::sync::Notify> {
        // The mock client never blocks on I/O; return an unused handle.
        Arc::new(tokio::sync::Notify::new())
    }

    fn wakeup_notify(&self) -> Arc<Notify> {
        self.wakeup.clone()
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
