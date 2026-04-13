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

//! The interface for `NetworkClient`.
//!
//! Translated from `org.apache.kafka.clients.KafkaClient`.

use crate::common::node::Node;
use crate::common::requests::RequestBuilder;

use super::RequestCompletionHandler;
use super::client_request::ClientRequest;
use super::client_response::ClientResponse;
use super::least_loaded_node::LeastLoadedNode;

/// The interface for [`NetworkClient`](super::network_client::NetworkClient).
///
/// Provides methods for managing connections to Kafka brokers and sending
/// requests/receiving responses.
///
/// Translated from `org.apache.kafka.clients.KafkaClient`.
pub trait KafkaClient {
    /// Check if we are currently ready to send another request to the given node
    /// but don't attempt to connect if we aren't.
    ///
    /// # Arguments
    ///
    /// * `node` - The node to check
    /// * `now` - The current timestamp in milliseconds
    fn is_ready(&self, node: &Node, now: i64) -> bool;

    /// Initiate a connection to the given node (if necessary), and return true if
    /// already connected. The readiness of a node will change only when poll is invoked.
    ///
    /// # Arguments
    ///
    /// * `node` - The node to connect to
    /// * `now` - The current time in milliseconds
    ///
    /// Returns `true` iff we are ready to immediately initiate the sending of another
    /// request to the given node.
    fn ready(&mut self, node: &Node, now: i64) -> bool;

    /// Return the number of milliseconds to wait, based on the connection state,
    /// before attempting to send data. When disconnected, this respects the reconnect
    /// backoff time. When connecting or connected, this handles slow/stalled connections.
    ///
    /// # Arguments
    ///
    /// * `node` - The node to check
    /// * `now` - The current timestamp in milliseconds
    ///
    /// Returns the number of milliseconds to wait.
    fn connection_delay(&self, node: &Node, now: i64) -> i64;

    /// Return the number of milliseconds to wait, based on the connection state and
    /// the throttle time, before attempting to send data. If the connection has been
    /// established but being throttled, return throttle delay. Otherwise, return
    /// connection delay.
    ///
    /// # Arguments
    ///
    /// * `node` - The connection to check
    /// * `now` - The current time in ms
    fn poll_delay_ms(&self, node: &Node, now: i64) -> i64;

    /// Check if the connection of the node has failed, based on the connection state.
    /// Such connection failures are usually transient and can be resumed in the next
    /// `ready` call, but there are cases where transient failures needs to be caught
    /// and re-acted upon.
    ///
    /// # Arguments
    ///
    /// * `node` - The node to check
    ///
    /// Returns `true` iff the connection has failed and the node is disconnected.
    fn connection_failed(&self, node: &Node) -> bool;

    /// Check if authentication to this node has failed, based on the connection state.
    /// Authentication failures are propagated without any retries.
    ///
    /// # Arguments
    ///
    /// * `node` - The node to check
    ///
    /// Returns an authentication error message if authentication has failed, `None` otherwise.
    fn authentication_error(&self, node: &Node) -> Option<String>;

    /// Queue up the given request for sending. Requests can only be sent on ready connections.
    ///
    /// # Arguments
    ///
    /// * `request` - The request
    /// * `now` - The current timestamp in milliseconds
    fn send(&mut self, request: ClientRequest, now: i64);

    /// Do actual reads and writes from sockets.
    ///
    /// # Arguments
    ///
    /// * `timeout` - The maximum amount of time to wait for responses in ms, must be
    ///   non-negative. The implementation is free to use a lower value if appropriate
    ///   (common reasons for this are a lower request or metadata update timeout).
    /// * `now` - The current time in ms
    ///
    /// Returns the list of responses received.
    fn poll(&mut self, timeout: i64, now: i64) -> Vec<ClientResponse>;

    /// Disconnects the connection to a particular node, if there is one.
    /// Any pending ClientRequests for this connection will receive disconnections.
    ///
    /// # Arguments
    ///
    /// * `node_id` - The id of the node
    fn disconnect(&mut self, node_id: &str);

    /// Closes the connection to a particular node (if there is one).
    /// All requests on the connection will be cleared. ClientRequest callbacks will
    /// not be invoked for the cleared requests, nor will they be returned from poll().
    ///
    /// # Arguments
    ///
    /// * `node_id` - The id of the node
    fn close_connection(&mut self, node_id: &str);

    /// Choose the node with the fewest outstanding requests. This method will prefer
    /// a node with an existing connection, but will potentially choose a node for which
    /// we don't yet have a connection if all existing connections are in use.
    ///
    /// # Arguments
    ///
    /// * `now` - The current time in ms
    ///
    /// Returns the node with the fewest in-flight requests.
    fn least_loaded_node(&self, now: i64) -> LeastLoadedNode;

    /// The number of currently in-flight requests for which we have not yet returned
    /// a response.
    fn in_flight_request_count(&self) -> i32;

    /// Return `true` if there is at least one in-flight request and `false` otherwise.
    fn has_in_flight_requests(&self) -> bool;

    /// Get the total in-flight requests for a particular node.
    ///
    /// # Arguments
    ///
    /// * `node_id` - The id of the node
    fn in_flight_request_count_for_node(&self, node_id: &str) -> usize;

    /// Return `true` if there is at least one in-flight request for a particular node
    /// and `false` otherwise.
    fn has_in_flight_requests_for_node(&self, node_id: &str) -> bool;

    /// Return `true` if there is at least one node with connection in the READY state
    /// and not throttled. Returns `false` otherwise.
    ///
    /// # Arguments
    ///
    /// * `now` - The current time
    fn has_ready_nodes(&self, now: i64) -> bool;

    /// Wake up the client if it is currently blocked waiting for I/O.
    fn wakeup(&self);

    /// Create a new `ClientRequest`.
    ///
    /// # Arguments
    ///
    /// * `node_id` - The node to send to
    /// * `request_builder` - The request builder to use
    /// * `created_time_ms` - The time in milliseconds to use as the creation time of the request
    /// * `expect_response` - `true` iff we expect a response
    fn new_client_request(
        &mut self,
        node_id: &str,
        request_builder: Box<dyn RequestBuilder>,
        created_time_ms: i64,
        expect_response: bool,
    ) -> ClientRequest;

    /// Create a new `ClientRequest` with a request timeout and callback.
    ///
    /// # Arguments
    ///
    /// * `node_id` - The node to send to
    /// * `request_builder` - The request builder to use
    /// * `created_time_ms` - The time in milliseconds to use as the creation time of the request
    /// * `expect_response` - `true` iff we expect a response
    /// * `request_timeout_ms` - Upper bound time in milliseconds to await a response before
    ///   disconnecting the socket and cancelling the request
    /// * `callback` - The callback to invoke when we get a response
    fn new_client_request_with_timeout(
        &mut self,
        node_id: &str,
        request_builder: Box<dyn RequestBuilder>,
        created_time_ms: i64,
        expect_response: bool,
        request_timeout_ms: i32,
        callback: Option<RequestCompletionHandler>,
    ) -> ClientRequest;

    /// Initiates shutdown of this client. This method may be invoked from another
    /// thread while this client is being polled. No further requests may be sent
    /// using the client. The current poll() will be terminated using wakeup().
    /// The client should be explicitly shutdown using `close()` after poll returns.
    fn initiate_close(&self);

    /// Returns `true` if the client is still active. Returns `false` if
    /// `initiate_close()` or `close()` was invoked for this client.
    fn active(&self) -> bool;

    /// Close the network client.
    fn close(&mut self);
}
