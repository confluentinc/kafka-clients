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

//! Translation of `org.apache.kafka.clients.KafkaClient`.
//!
//! Note: the Java type lives in `org.apache.kafka.clients` (not `common`),
//! so it sits at the crate root rather than under `common::`.

use std::sync::Arc;

use crate::common::Node;
use crate::common::errors::KafkaError;
use crate::common::requests::AbstractRequestBuilder;
use crate::{ClientRequest, ClientResponse, LeastLoadedNode, RequestCompletionHandler};

/// The interface for `NetworkClient`.
///
/// Mirrors Java's `KafkaClient`. Implementations are not required to be
/// thread-safe — Java's `NetworkClient` doc states the same.
///
/// **Async-`poll` divergence**: Java's `poll(long, long)` blocks the
/// thread on `Selector.select(timeout)`. The Rust translation is
/// `async fn` (CLAUDE.md rule 9.1). Callers must `.await` the future.
///
/// **Numeric `nodeId` divergence**: Java keys disconnect / close / count
/// methods by `String`. The producer code path always derives that
/// string from `Node.id() (int)`. The Rust translation accepts the
/// integer directly to avoid per-message `String` clones (CLAUDE.md
/// rule 11; see `design/history/Milestone-1/Phase-5/NOTES.md`
/// "Hot-path identifier interning"). Methods that take a [`Node`]
/// keep the Node reference and let the implementation extract the id.
pub trait KafkaClient: std::marker::Send {
    /// Check if we are currently ready to send another request to the
    /// given node but don't attempt to connect if we aren't. Mirrors
    /// `KafkaClient.isReady(Node, long)`.
    fn is_ready(&self, node: &Node, now: i64) -> bool;

    /// Initiate a connection to the given node (if necessary), and
    /// return true if already connected. The readiness of a node will
    /// change only when `poll` is invoked.
    ///
    /// Mirrors `KafkaClient.ready(Node, long)`.
    fn ready(&mut self, node: &Node, now: i64) -> bool;

    /// Return the number of milliseconds to wait, based on the
    /// connection state, before attempting to send data. When
    /// disconnected, this respects the reconnect backoff time. When
    /// connecting or connected, this handles slow/stalled connections.
    ///
    /// Mirrors `KafkaClient.connectionDelay(Node, long)`.
    fn connection_delay(&self, node: &Node, now: i64) -> i64;

    /// Return the number of milliseconds to wait, based on the
    /// connection state and the throttle time, before attempting to
    /// send data. If the connection has been established but is being
    /// throttled, return throttle delay. Otherwise, return connection
    /// delay.
    ///
    /// Mirrors `KafkaClient.pollDelayMs(Node, long)`.
    fn poll_delay_ms(&self, node: &Node, now: i64) -> i64;

    /// Check if the connection of the node has failed. Mirrors
    /// `KafkaClient.connectionFailed(Node)`.
    fn connection_failed(&self, node: &Node) -> bool;

    /// Check if authentication to this node has failed. Returns
    /// `Some(KafkaError::Authentication(_))` if it has, `None`
    /// otherwise. Mirrors
    /// `KafkaClient.authenticationException(Node)`.
    fn authentication_error(&self, node: &Node) -> Option<KafkaError>;

    /// Queue up the given request for sending. Requests can only be
    /// sent on ready connections. Mirrors
    /// `KafkaClient.send(ClientRequest, long)`.
    fn send(&mut self, request: ClientRequest, now: i64);

    /// Do actual reads and writes from sockets.
    ///
    /// `timeout_ms` is the maximum amount of time to wait for responses
    /// in ms (must be non-negative). The implementation is free to use
    /// a lower value if appropriate.
    ///
    /// Mirrors `KafkaClient.poll(long, long)`. Java's signature throws
    /// `IllegalStateException` when a request was sent to an unready
    /// node — Rust translations should panic in that case (CLAUDE.md
    /// rule 10.1).
    ///
    /// Returns a `Send` future so [`Sender::run_loop`] (which awaits
    /// `poll`) can be moved into a `tokio::spawn` task that is itself
    /// `Send`. The async-fn-in-trait desugar would otherwise give a
    /// non-`Send` future and break [`KafkaProducer`]'s constructor.
    ///
    /// [`Sender::run_loop`]: crate::producer::internals::sender::Sender::run_loop
    /// [`KafkaProducer`]: crate::producer::KafkaProducer
    fn poll(&mut self, timeout_ms: i64, now: i64) -> impl std::future::Future<Output = Vec<ClientResponse>> + Send;

    /// Disconnects the connection to a particular node, if there is one.
    /// Any pending `ClientRequest`s for this connection will receive
    /// disconnections.
    ///
    /// Mirrors `KafkaClient.disconnect(String)`. Note the i32 / String
    /// divergence — see trait-level rustdoc.
    fn disconnect(&mut self, node_id: i32);

    /// Closes the connection to a particular node (if there is one).
    /// All requests on the connection will be cleared. `ClientRequest`
    /// callbacks will not be invoked for the cleared requests, nor will
    /// they be returned from `poll()`.
    ///
    /// Mirrors `KafkaClient.close(String)`.
    fn close_connection(&mut self, node_id: i32);

    /// Choose the node with the fewest outstanding requests. Mirrors
    /// `KafkaClient.leastLoadedNode(long)`.
    fn least_loaded_node(&mut self, now: i64) -> LeastLoadedNode;

    /// The number of currently in-flight requests for which we have not
    /// yet returned a response. Mirrors `KafkaClient.inFlightRequestCount()`.
    fn in_flight_request_count(&self) -> i32;

    /// Return true if there is at least one in-flight request and false
    /// otherwise. Mirrors `KafkaClient.hasInFlightRequests()`.
    fn has_in_flight_requests(&self) -> bool;

    /// Get the total in-flight requests for a particular node. Mirrors
    /// `KafkaClient.inFlightRequestCount(String)`.
    fn in_flight_request_count_for(&self, node_id: i32) -> i32;

    /// Return true if there is at least one in-flight request for a
    /// particular node and false otherwise. Mirrors
    /// `KafkaClient.hasInFlightRequests(String)`.
    fn has_in_flight_requests_for(&self, node_id: i32) -> bool;

    /// Return true if there is at least one node with connection in the
    /// READY state and not throttled. Mirrors
    /// `KafkaClient.hasReadyNodes(long)`.
    fn has_ready_nodes(&self, now: i64) -> bool;

    /// Wake up the client if it is currently blocked waiting for I/O.
    /// Mirrors `KafkaClient.wakeup()`.
    fn wakeup(&self);

    /// Create a new `ClientRequest` (no callback / default timeout).
    /// Mirrors `KafkaClient.newClientRequest(String, AbstractRequest.Builder<?>, long, boolean)`.
    ///
    /// **Mutability divergence from Java**: Java declares the method
    /// without an explicit synchronized/mutability qualifier, but the
    /// implementation bumps the per-`NetworkClient` `int correlation`
    /// counter — i.e. it *does* mutate (the class as a whole is
    /// documented as not thread-safe). The Rust trait makes that
    /// explicit by taking `&mut self` so the correlation counter need
    /// not be wrapped in an `AtomicI32` for this call site.
    fn new_client_request(
        &mut self,
        node_id: Arc<str>,
        request_builder: Arc<dyn AbstractRequestBuilder>,
        created_time_ms: i64,
        expect_response: bool,
    ) -> ClientRequest;

    /// Create a new `ClientRequest` with explicit timeout and callback.
    /// Mirrors `KafkaClient.newClientRequest(String, AbstractRequest.Builder<?>,
    /// long, boolean, int, RequestCompletionHandler)`. See
    /// [`Self::new_client_request`] for the mutability divergence.
    fn new_client_request_with_callback(
        &mut self,
        node_id: Arc<str>,
        request_builder: Arc<dyn AbstractRequestBuilder>,
        created_time_ms: i64,
        expect_response: bool,
        request_timeout_ms: i32,
        callback: Option<Arc<dyn RequestCompletionHandler>>,
    ) -> ClientRequest;

    /// Initiates shutdown of this client. Mirrors
    /// `KafkaClient.initiateClose()`.
    fn initiate_close(&mut self);

    /// Returns true if the client is still active. Mirrors
    /// `KafkaClient.active()`.
    fn active(&self) -> bool;

    /// Closes the client. Java extends `Closeable`; we model the same
    /// surface explicitly.
    fn close(&mut self);
}
