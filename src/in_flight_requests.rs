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

//! In-flight request tracking for the Kafka network client.
//!
//! Translated from `org.apache.kafka.clients.NetworkClient.InFlightRequest`
//! (inner class) and `org.apache.kafka.clients.InFlightRequests`.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicI32, Ordering};

use crate::common::network::NetworkSend;
use crate::common::requests::{ConcreteRequest, ConcreteResponse, RequestHeader};

use super::ClientResponse;
use super::RequestCompletionHandler;

/// A single in-flight request that has been sent (or is being sent) to a broker
/// but has not yet received a response.
///
/// Translated from `NetworkClient.InFlightRequest` inner class in Java.
pub struct InFlightRequest {
    /// The request header.
    pub header: RequestHeader,
    /// The request timeout in milliseconds.
    pub request_timeout_ms: i64,
    /// The unix timestamp when the request was created.
    pub created_time_ms: i64,
    /// The destination node id.
    pub destination: String,
    /// The completion callback, if any.
    callback: Option<RequestCompletionHandler>,
    /// Whether we expect a response message or this request is complete once sent.
    pub expect_response: bool,
    /// Whether this request is initiated internally by the `NetworkClient`.
    pub is_internal_request: bool,
    /// The built request.
    pub request: Option<ConcreteRequest>,
    /// The network send associated with this request.
    pub send: NetworkSend,
    /// Whether the network send has been completed (confirmed by the selector).
    ///
    /// In Java, the same `Send` object is shared between `InFlightRequest` and
    /// the selector, so `send.completed()` reflects the actual I/O state. In
    /// Rust we use separate copies, so this flag is set by `NetworkClient` when
    /// the selector reports a completed send for this destination.
    send_completed: bool,
    /// The unix timestamp when this request was sent.
    pub send_time_ms: i64,
    /// Accumulated throttle time in milliseconds.
    throttle_time_ms: i64,
}

impl InFlightRequest {
    /// Creates a new `InFlightRequest` from a `ClientRequest` and additional send-time metadata.
    ///
    /// This corresponds to the Java constructor that takes a `ClientRequest`.
    pub fn from_client_request(
        client_request: &mut super::client_request::ClientRequest,
        header: RequestHeader,
        is_internal_request: bool,
        request: Option<ConcreteRequest>,
        send: NetworkSend,
        send_time_ms: i64,
    ) -> Self {
        Self {
            header,
            request_timeout_ms: client_request.request_timeout_ms() as i64,
            created_time_ms: client_request.created_time_ms(),
            destination: client_request.destination().to_string(),
            callback: client_request.take_callback(),
            expect_response: client_request.expect_response(),
            is_internal_request,
            request,
            send,
            send_completed: false,
            send_time_ms,
            throttle_time_ms: 0,
        }
    }

    /// Creates a new `InFlightRequest` with all fields specified directly.
    ///
    /// This corresponds to the Java constructor with explicit parameters.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        header: RequestHeader,
        request_timeout_ms: i64,
        created_time_ms: i64,
        destination: &str,
        callback: Option<RequestCompletionHandler>,
        expect_response: bool,
        is_internal_request: bool,
        request: Option<ConcreteRequest>,
        send: NetworkSend,
        send_time_ms: i64,
    ) -> Self {
        Self {
            header,
            request_timeout_ms,
            created_time_ms,
            destination: destination.to_string(),
            callback,
            expect_response,
            is_internal_request,
            request,
            send,
            send_completed: false,
            send_time_ms,
            throttle_time_ms: 0,
        }
    }

    /// Returns the elapsed time since this request was sent, in milliseconds.
    ///
    /// Returns 0 if the current time is before the send time (clock skew).
    pub fn time_elapsed_since_send_ms(&self, current_time_ms: i64) -> i64 {
        (current_time_ms - self.send_time_ms).max(0)
    }

    /// Returns the accumulated throttle time in milliseconds.
    pub fn throttle_time_ms(&self) -> i64 {
        self.throttle_time_ms
    }

    /// Returns the elapsed time since this request was created, in milliseconds.
    ///
    /// Returns 0 if the current time is before the creation time (clock skew).
    pub fn time_elapsed_since_create_ms(&self, current_time_ms: i64) -> i64 {
        (current_time_ms - self.created_time_ms).max(0)
    }

    /// Creates a [`ClientResponse`] for a successfully completed request.
    pub fn completed(&mut self, response: Option<ConcreteResponse>, time_ms: i64) -> ClientResponse {
        ClientResponse::new(
            self.header.clone(),
            self.callback.take(),
            &self.destination,
            self.created_time_ms,
            time_ms,
            false,
            None,
            None,
            response,
        )
    }

    /// Creates a [`ClientResponse`] for a timed-out request.
    ///
    /// A timed-out request is also considered disconnected.
    pub fn timed_out(&mut self, time_ms: i64) -> ClientResponse {
        ClientResponse::with_timeout(
            self.header.clone(),
            self.callback.take(),
            &self.destination,
            self.created_time_ms,
            time_ms,
            true,
            true,
            None,
            None,
            None,
        )
    }

    /// Creates a [`ClientResponse`] for a disconnected request.
    pub fn disconnected(&mut self, time_ms: i64) -> ClientResponse {
        ClientResponse::new(
            self.header.clone(),
            self.callback.take(),
            &self.destination,
            self.created_time_ms,
            time_ms,
            true,
            None,
            None,
            None,
        )
    }

    /// Increments the accumulated throttle time by the given amount.
    pub fn increment_throttle_time(&mut self, throttle_time_ms: i64) {
        self.throttle_time_ms += throttle_time_ms;
    }
}

impl fmt::Display for InFlightRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "InFlightRequest(header={}, destination={}, expectResponse={}, \
             createdTimeMs={}, sendTimeMs={}, isInternalRequest={})",
            self.header,
            self.destination,
            self.expect_response,
            self.created_time_ms,
            self.send_time_ms,
            self.is_internal_request,
        )
    }
}

// ---------------------------------------------------------------------------
// InFlightRequests — the collection
// ---------------------------------------------------------------------------

/// The set of requests which have been sent or are being sent but have not yet
/// received a response.
///
/// Translated from `org.apache.kafka.clients.InFlightRequests`.
///
/// # Thread safety
///
/// In Java, `inFlightRequestCount` is an `AtomicInteger` for thread-safe reads.
/// We preserve this via [`AtomicI32`] so that `count()` (total) can be called
/// from other threads without taking a lock.
pub struct InFlightRequests {
    max_in_flight_requests_per_connection: usize,
    requests: HashMap<String, VecDeque<InFlightRequest>>,
    /// Thread-safe total number of in-flight requests.
    in_flight_request_count: AtomicI32,
}

impl InFlightRequests {
    /// Creates a new `InFlightRequests` with the given per-connection limit.
    pub fn new(max_in_flight_requests_per_connection: usize) -> Self {
        Self {
            max_in_flight_requests_per_connection,
            requests: HashMap::new(),
            in_flight_request_count: AtomicI32::new(0),
        }
    }

    /// Adds the given request to the queue for the connection it was directed to.
    ///
    /// New requests are added to the *front* of the deque (most recently sent first).
    pub fn add(&mut self, request: InFlightRequest) {
        let destination = request.destination.clone();
        let reqs = self.requests.entry(destination).or_default();
        reqs.push_front(request);
        self.in_flight_request_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Returns a reference to the request queue for the given node.
    ///
    /// # Errors
    ///
    /// Returns `Err` if there are no in-flight requests for the node.
    fn request_queue(&self, node: &str) -> Result<&VecDeque<InFlightRequest>, String> {
        match self.requests.get(node) {
            Some(reqs) if !reqs.is_empty() => Ok(reqs),
            _ => Err(format!("There are no in-flight requests for node {node}")),
        }
    }

    /// Returns a mutable reference to the request queue for the given node.
    ///
    /// # Errors
    ///
    /// Returns `Err` if there are no in-flight requests for the node.
    fn request_queue_mut(&mut self, node: &str) -> Result<&mut VecDeque<InFlightRequest>, String> {
        match self.requests.get_mut(node) {
            Some(reqs) if !reqs.is_empty() => Ok(reqs),
            _ => Err(format!("There are no in-flight requests for node {node}")),
        }
    }

    /// Gets the oldest request (the one that will be completed next) for the given
    /// node and removes it from the queue.
    ///
    /// # Panics
    ///
    /// Panics if there are no in-flight requests for the given node.
    pub fn complete_next(&mut self, node: &str) -> InFlightRequest {
        let reqs = self.request_queue_mut(node).unwrap_or_else(|e| panic!("{e}"));
        let request = reqs.pop_back().expect("Queue should not be empty");
        self.in_flight_request_count.fetch_sub(1, Ordering::Relaxed);
        request
    }

    /// Gets the last request sent to the given node (but does not remove it from
    /// the queue).
    ///
    /// # Panics
    ///
    /// Panics if there are no in-flight requests for the given node.
    pub fn last_sent(&self, node: &str) -> &InFlightRequest {
        let reqs = self.request_queue(node).unwrap_or_else(|e| panic!("{e}"));
        reqs.front().expect("Queue should not be empty")
    }

    pub fn last_sent_mut(&mut self, node: &str) -> &mut InFlightRequest {
        let reqs = self.request_queue_mut(node).unwrap_or_else(|e| panic!("{e}"));
        reqs.front_mut().expect("Queue should not be empty")
    }

    /// Marks the last request sent to the given node as send-completed.
    ///
    /// In Java, the same `Send` object is shared between `InFlightRequest` and
    /// the selector, so `send.completed()` reflects the actual I/O state
    /// automatically. In Rust we use separate copies, so this method must be
    /// called when the selector reports a completed send.
    pub fn mark_last_sent_completed(&mut self, node: &str) {
        if let Some(queue) = self.requests.get_mut(node)
            && let Some(req) = queue.front_mut()
        {
            req.send_completed = true;
        }
    }

    /// Removes and returns the last request that was sent to a particular node.
    ///
    /// # Panics
    ///
    /// Panics if there are no in-flight requests for the given node.
    pub fn complete_last_sent(&mut self, node: &str) -> InFlightRequest {
        let reqs = self.request_queue_mut(node).unwrap_or_else(|e| panic!("{e}"));
        let request = reqs.pop_front().expect("Queue should not be empty");
        self.in_flight_request_count.fetch_sub(1, Ordering::Relaxed);
        request
    }

    /// Returns whether more requests can be sent to this node.
    ///
    /// More requests can be sent if:
    /// - There are no requests in the queue, or
    /// - The most recently sent request's send is completed AND the queue size is
    ///   below the per-connection limit.
    pub fn can_send_more(&self, node: &str) -> bool {
        match self.requests.get(node) {
            None => true,
            Some(queue) => {
                queue.is_empty()
                    || (queue.front().is_some_and(|r| r.send_completed)
                        && queue.len() < self.max_in_flight_requests_per_connection)
            },
        }
    }

    /// Returns the number of in-flight requests directed at the given node.
    pub fn count_for_node(&self, node: &str) -> usize {
        self.requests.get(node).map_or(0, VecDeque::len)
    }

    /// Returns `true` if there are no in-flight requests directed at the given node.
    pub fn is_empty_for_node(&self, node: &str) -> bool {
        self.requests.get(node).is_none_or(VecDeque::is_empty)
    }

    /// Returns the total count of in-flight requests across all nodes.
    ///
    /// This method is thread-safe but may lag the actual count.
    pub fn count(&self) -> i32 {
        self.in_flight_request_count.load(Ordering::Relaxed)
    }

    /// Returns `true` if there are no in-flight requests for any node.
    pub fn is_empty(&self) -> bool {
        self.requests.values().all(VecDeque::is_empty)
    }

    /// Clears all in-flight requests for the given node and returns them.
    ///
    /// The returned requests are in oldest-first order (the order they were
    /// originally sent).
    pub fn clear_all(&mut self, node: &str) -> Vec<InFlightRequest> {
        match self.requests.remove(node) {
            None => Vec::new(),
            Some(mut cleared) => {
                let count = cleared.len() as i32;
                self.in_flight_request_count.fetch_sub(count, Ordering::Relaxed);
                // Java's clearAll returns descendingIterator which iterates from
                // tail to head (oldest to newest), since addFirst puts newest at head.
                // Our VecDeque with push_front has newest at front, oldest at back.
                // Reversing gives oldest-first order, matching Java's behavior.
                let result: Vec<InFlightRequest> = cleared.drain(..).rev().collect();
                result
            },
        }
    }

    /// Returns a list of nodes with pending in-flight requests that have timed out.
    ///
    /// A request is considered timed out if the elapsed time since send (minus
    /// any throttle time) exceeds its request timeout.
    pub fn nodes_with_timed_out_requests(&self, now: i64) -> Vec<String> {
        let mut node_ids = Vec::new();
        for (node_id, deque) in &self.requests {
            if Self::has_expired_request(now, deque) {
                node_ids.push(node_id.clone());
            }
        }
        node_ids
    }

    /// Increments the throttle time for all in-flight requests to the given node.
    pub fn increment_throttle_time(&mut self, node_id: &str, throttle_time_ms: i64) {
        if let Some(deque) = self.requests.get_mut(node_id) {
            for request in deque.iter_mut() {
                request.increment_throttle_time(throttle_time_ms);
            }
        }
    }

    /// Checks if any request in the deque has expired.
    fn has_expired_request(now: i64, deque: &VecDeque<InFlightRequest>) -> bool {
        for request in deque {
            // Exclude throttle time because we want to ensure that we don't expire
            // requests while they are throttled. The request timeout should take
            // effect only after the throttle time has elapsed.
            if request.time_elapsed_since_send_ms(now) - request.throttle_time_ms() > request.request_timeout_ms {
                return true;
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::network::{ByteBufferSend, NetworkSend};
    use crate::common::protocol::ApiKeys;

    fn add_request(
        in_flight_requests: &mut InFlightRequests,
        destination: &str,
        correlation_id: &mut i32,
        send_time_ms: i64,
        request_timeout_ms: i64,
    ) -> i32 {
        let id = *correlation_id;
        *correlation_id += 1;

        let header =
            RequestHeader::new(&ApiKeys::METADATA, 0, "clientId", id).expect("header creation should not fail");

        // Create a minimal completed NetworkSend for testing.
        // An empty ByteBufferSend is immediately "completed" (remaining == 0).
        let inner_send = ByteBufferSend::new(Vec::new());
        let send = NetworkSend::new(destination, Box::new(inner_send));

        let ifr = InFlightRequest::new(
            header,
            request_timeout_ms,
            0,
            destination,
            None,
            false,
            false,
            None,
            send,
            send_time_ms,
        );
        in_flight_requests.add(ifr);
        id
    }

    fn add_request_default(
        in_flight_requests: &mut InFlightRequests,
        destination: &str,
        correlation_id: &mut i32,
    ) -> i32 {
        add_request(in_flight_requests, destination, correlation_id, 0, 10000)
    }

    /// Translated from Java `InFlightRequestsTest.testCompleteLastSent`.
    #[test]
    fn test_complete_last_sent() {
        let mut in_flight = InFlightRequests::new(12);
        let mut correlation_id = 0;
        let dest = "dest";

        let correlation_id1 = add_request_default(&mut in_flight, dest, &mut correlation_id);
        let correlation_id2 = add_request_default(&mut in_flight, dest, &mut correlation_id);
        assert_eq!(2, in_flight.count());

        assert_eq!(correlation_id2, in_flight.complete_last_sent(dest).header.correlation_id());
        assert_eq!(1, in_flight.count());

        assert_eq!(correlation_id1, in_flight.complete_last_sent(dest).header.correlation_id());
        assert_eq!(0, in_flight.count());
    }

    /// Translated from Java `InFlightRequestsTest.testClearAll`.
    #[test]
    fn test_clear_all() {
        let mut in_flight = InFlightRequests::new(12);
        let mut correlation_id = 0;
        let dest = "dest";

        let correlation_id1 = add_request_default(&mut in_flight, dest, &mut correlation_id);
        let correlation_id2 = add_request_default(&mut in_flight, dest, &mut correlation_id);

        let cleared_requests = in_flight.clear_all(dest);
        assert_eq!(0, in_flight.count());
        assert_eq!(2, cleared_requests.len());
        assert_eq!(correlation_id1, cleared_requests[0].header.correlation_id());
        assert_eq!(correlation_id2, cleared_requests[1].header.correlation_id());
    }

    /// Translated from Java `InFlightRequestsTest.testTimedOutNodes`.
    #[test]
    fn test_timed_out_nodes() {
        let mut in_flight = InFlightRequests::new(12);
        let mut correlation_id = 0;
        let mut time_ms: i64 = 0;

        add_request(&mut in_flight, "A", &mut correlation_id, time_ms, 50);
        add_request(&mut in_flight, "B", &mut correlation_id, time_ms, 200);
        add_request(&mut in_flight, "B", &mut correlation_id, time_ms, 100);

        time_ms += 50;
        assert!(in_flight.nodes_with_timed_out_requests(time_ms).is_empty());

        time_ms += 25;
        let timed_out = in_flight.nodes_with_timed_out_requests(time_ms);
        assert_eq!(1, timed_out.len());
        assert!(timed_out.contains(&"A".to_string()));

        time_ms += 50;
        let timed_out = in_flight.nodes_with_timed_out_requests(time_ms);
        assert_eq!(2, timed_out.len());
        assert!(timed_out.contains(&"A".to_string()));
        assert!(timed_out.contains(&"B".to_string()));
    }

    /// Translated from Java `InFlightRequestsTest.testCompleteNext`.
    #[test]
    fn test_complete_next() {
        let mut in_flight = InFlightRequests::new(12);
        let mut correlation_id = 0;
        let dest = "dest";

        let correlation_id1 = add_request_default(&mut in_flight, dest, &mut correlation_id);
        let correlation_id2 = add_request_default(&mut in_flight, dest, &mut correlation_id);
        assert_eq!(2, in_flight.count());

        assert_eq!(correlation_id1, in_flight.complete_next(dest).header.correlation_id());
        assert_eq!(1, in_flight.count());

        assert_eq!(correlation_id2, in_flight.complete_next(dest).header.correlation_id());
        assert_eq!(0, in_flight.count());
    }

    /// Translated from Java `InFlightRequestsTest.testCompleteNextThrowsIfNoInFlights`.
    #[test]
    #[should_panic(expected = "There are no in-flight requests for node dest")]
    fn test_complete_next_panics_if_no_in_flights() {
        let mut in_flight = InFlightRequests::new(12);
        in_flight.complete_next("dest");
    }

    /// Translated from Java `InFlightRequestsTest.testCompleteLastSentThrowsIfNoInFlights`.
    #[test]
    #[should_panic(expected = "There are no in-flight requests for node dest")]
    fn test_complete_last_sent_panics_if_no_in_flights() {
        let mut in_flight = InFlightRequests::new(12);
        in_flight.complete_last_sent("dest");
    }
}
