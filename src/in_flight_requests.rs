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

//! Translation of `org.apache.kafka.clients.InFlightRequests` and the
//! companion `NetworkClient.InFlightRequest` inner class.
//!
//! Java's `InFlightRequest` is a static inner class of `NetworkClient`.
//! Since `NetworkClient` itself does not land until Phase 5d, the inner
//! class is defined here (next to the collection that owns it) and is
//! `pub(crate)` — Java's package-private visibility (the class name is
//! `static class`, not `public static class`).
//!
//! ## Hot-path key type
//!
//! Java keys per-node deques by `String` (the broker connection id).
//! The Rust translation uses `i32` everywhere (CLAUDE.md rule 11; see
//! `design/history/Milestone-1/Phase-5/NOTES.md` "Hot-path identifier
//! interning"). The Java string is `Integer.toString(node.id())` —
//! switching to the integer avoids a per-request `String` clone. The
//! `Selectable` trait is updated in lock-step (Phase 5c-2 Selector takes
//! `i32`).
//!
//! ## Order
//!
//! Java's `Deque<InFlightRequest>` orders newest-at-front, oldest-at-back
//! (`addFirst` on add; `pollLast` on `completeNext`). The Rust
//! `VecDeque<InFlightRequest>` preserves the same convention so the
//! `lastSent` / `completeLastSent` / `completeNext` semantics match
//! exactly.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicI32, Ordering};

use crate::ClientResponse;
use crate::RequestCompletionHandler;
use crate::common::network::SendCompletion;
use crate::common::requests::{AbstractRequest, RequestHeader};

/// An in-flight request awaiting a response from the broker.
///
/// Mirrors the package-private `NetworkClient.InFlightRequest` inner
/// class. Phase 5d (`NetworkClient`) is its only producer; tests in the
/// same crate construct it directly via [`InFlightRequest::new`].
///
/// All fields use `pub(crate)` visibility — the equivalent of Java's
/// package-private access on the inner class fields (`final RequestHeader
/// header;` without an explicit modifier).
///
/// `#[allow(dead_code)]` on the struct silences `dead_code` warnings for
/// fields and methods that the in-crate `NetworkClient` (Phase 5d) will
/// be the first non-test caller of.
#[allow(dead_code)]
pub(crate) struct InFlightRequest {
    pub(crate) header: RequestHeader,
    pub(crate) destination: i32,
    pub(crate) callback: Option<Arc<dyn RequestCompletionHandler>>,
    pub(crate) expect_response: bool,
    /// `None` for synthetic / fault-injected request entries (Java's
    /// `request` parameter accepts `null`).
    pub(crate) request: Option<Box<dyn AbstractRequest>>,
    pub(crate) is_internal_request: bool,
    /// Cheap, observe-only handle to the paired `NetworkSend`'s
    /// completion bit. Populated by `NetworkClient::do_send` before the
    /// `NetworkSend` is handed to the selector, so
    /// [`InFlightRequests::can_send_more`] can mirror Java's
    /// `peekFirst().send.completed()` check (InFlightRequests.java:99).
    ///
    /// `None` for synthetic / fault-injected request entries (Java's
    /// `send` parameter accepts `null`).
    pub(crate) send: Option<SendCompletion>,
    pub(crate) send_time_ms: i64,
    pub(crate) created_time_ms: i64,
    pub(crate) request_timeout_ms: i32,
    /// Tracks accumulated throttle time. Atomic so callers can fire
    /// `incrementThrottleTime` from multiple lifecycle points without
    /// retaking the outer `&mut`.
    throttle_time_ms: AtomicI32,
}

#[allow(dead_code)] // Phase 5d NetworkClient is the first non-test caller
impl InFlightRequest {
    /// Mirrors the 10-arg Java constructor.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        header: RequestHeader,
        request_timeout_ms: i32,
        created_time_ms: i64,
        destination: i32,
        callback: Option<Arc<dyn RequestCompletionHandler>>,
        expect_response: bool,
        is_internal_request: bool,
        request: Option<Box<dyn AbstractRequest>>,
        send: Option<SendCompletion>,
        send_time_ms: i64,
    ) -> Self {
        InFlightRequest {
            header,
            destination,
            callback,
            expect_response,
            request,
            is_internal_request,
            send,
            send_time_ms,
            created_time_ms,
            request_timeout_ms,
            throttle_time_ms: AtomicI32::new(0),
        }
    }

    /// Mirrors `InFlightRequest.timeElapsedSinceSendMs(long)`.
    pub(crate) fn time_elapsed_since_send_ms(&self, current_time_ms: i64) -> i64 {
        (current_time_ms - self.send_time_ms).max(0)
    }

    /// Mirrors `InFlightRequest.throttleTimeMs()`.
    pub(crate) fn throttle_time_ms(&self) -> i32 {
        self.throttle_time_ms.load(Ordering::Relaxed)
    }

    /// Mirrors `InFlightRequest.incrementThrottleTime(long)`. The Java
    /// signature takes a `long` but the value is bounded by request
    /// timeout — `i32` is sufficient and matches the wire field.
    pub(crate) fn increment_throttle_time(&self, throttle_time_ms: i32) {
        self.throttle_time_ms.fetch_add(throttle_time_ms, Ordering::Relaxed);
    }

    /// Mirrors `InFlightRequest.timeElapsedSinceCreateMs(long)`.
    #[allow(dead_code)] // Used by Phase 5d NetworkClient
    pub(crate) fn time_elapsed_since_create_ms(&self, current_time_ms: i64) -> i64 {
        (current_time_ms - self.created_time_ms).max(0)
    }

    /// Mirrors `InFlightRequest.completed(AbstractResponse response, long timeMs)`.
    ///
    /// Java's helper passes the connection's `String destination`; the
    /// Rust translation accepts the broker's `Arc<str>` label that
    /// [`crate::ClientResponse`] expects (carried alongside the i32
    /// connection id).
    #[allow(dead_code)] // Used by Phase 5d NetworkClient
    pub(crate) fn completed(
        &self,
        response: Option<Box<dyn crate::common::requests::AbstractResponse>>,
        time_ms: i64,
        destination: Arc<str>,
    ) -> ClientResponse {
        let header = self.header.clone();
        let callback = self.callback.clone();
        ClientResponse::new(
            header,
            callback,
            destination,
            self.created_time_ms,
            time_ms,
            false,
            None,
            None,
            response,
        )
    }

    /// Mirrors `InFlightRequest.timedOut(long timeMs)`.
    #[allow(dead_code)] // Used by Phase 5d NetworkClient
    pub(crate) fn timed_out(&self, time_ms: i64, destination: Arc<str>) -> ClientResponse {
        let header = self.header.clone();
        let callback = self.callback.clone();
        ClientResponse::with_timed_out(
            header,
            callback,
            destination,
            self.created_time_ms,
            time_ms,
            true,
            true,
            None,
            None,
            None,
        )
    }

    /// Mirrors `InFlightRequest.disconnected(long timeMs)`.
    #[allow(dead_code)] // Used by Phase 5d NetworkClient
    pub(crate) fn disconnected(&self, time_ms: i64, destination: Arc<str>) -> ClientResponse {
        let header = self.header.clone();
        let callback = self.callback.clone();
        ClientResponse::new(
            header,
            callback,
            destination,
            self.created_time_ms,
            time_ms,
            true,
            None,
            None,
            None,
        )
    }
}

impl std::fmt::Debug for InFlightRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InFlightRequest")
            .field("destination", &self.destination)
            .field("expect_response", &self.expect_response)
            .field("is_internal_request", &self.is_internal_request)
            .field("created_time_ms", &self.created_time_ms)
            .field("send_time_ms", &self.send_time_ms)
            .field("correlation_id", &self.header.correlation_id())
            .field("throttle_time_ms", &self.throttle_time_ms.load(Ordering::Relaxed))
            .finish()
    }
}

/// The set of requests which have been sent or are being sent but
/// haven't yet received a response.
///
/// Mirrors the package-private Java class
/// `org.apache.kafka.clients.InFlightRequests`.
///
/// Per-node deques are ordered newest-at-front (back-loaded by `add` via
/// [`VecDeque::push_front`]; oldest popped from the back by
/// [`Self::complete_next`]; newest popped from the front by
/// [`Self::complete_last_sent`]).
#[allow(dead_code)] // Phase 5d NetworkClient is the first non-test caller
pub(crate) struct InFlightRequests {
    max_in_flight_requests_per_connection: i32,
    requests: HashMap<i32, VecDeque<InFlightRequest>>,
    /// Total number of in-flight requests across all nodes. Atomic in
    /// Java; kept atomic here so [`Self::count`] is `&self`. Mirrors
    /// Java's `AtomicInteger inFlightRequestCount`.
    in_flight_request_count: AtomicI32,
}

#[allow(dead_code)] // Phase 5d NetworkClient is the first non-test caller
impl InFlightRequests {
    /// Mirrors `new InFlightRequests(int)`.
    pub(crate) fn new(max_in_flight_requests_per_connection: i32) -> Self {
        InFlightRequests {
            max_in_flight_requests_per_connection,
            requests: HashMap::new(),
            in_flight_request_count: AtomicI32::new(0),
        }
    }

    /// Add the given request to the queue for the connection it was
    /// directed to. Mirrors `InFlightRequests.add(InFlightRequest)`.
    pub(crate) fn add(&mut self, request: InFlightRequest) {
        let dest = request.destination;
        self.requests.entry(dest).or_default().push_front(request);
        self.in_flight_request_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Get the request queue for the given node, panicking with the
    /// Java error message if missing. Mirrors the private
    /// `requestQueue` method.
    fn request_queue(&mut self, node: i32) -> &mut VecDeque<InFlightRequest> {
        match self.requests.get_mut(&node) {
            Some(queue) if !queue.is_empty() => queue,
            _ => panic!("There are no in-flight requests for node {node}"),
        }
    }

    fn request_queue_ref(&self, node: i32) -> &VecDeque<InFlightRequest> {
        match self.requests.get(&node) {
            Some(queue) if !queue.is_empty() => queue,
            _ => panic!("There are no in-flight requests for node {node}"),
        }
    }

    /// Get the oldest request (the one that will be completed next) for
    /// the given node. Mirrors `InFlightRequests.completeNext(String)`.
    pub(crate) fn complete_next(&mut self, node: i32) -> InFlightRequest {
        let queue = self.request_queue(node);
        let request = queue.pop_back().expect("non-empty queue checked by request_queue");
        self.in_flight_request_count.fetch_sub(1, Ordering::Relaxed);
        request
    }

    /// Get the last request we sent to the given node (but don't remove
    /// it from the queue). Mirrors `InFlightRequests.lastSent(String)`.
    #[allow(dead_code)] // Used by Phase 5d NetworkClient
    pub(crate) fn last_sent(&self, node: i32) -> &InFlightRequest {
        self.request_queue_ref(node)
            .front()
            .expect("non-empty queue checked by request_queue_ref")
    }

    /// Complete the last request that was sent to a particular node.
    /// Mirrors `InFlightRequests.completeLastSent(String)`.
    pub(crate) fn complete_last_sent(&mut self, node: i32) -> InFlightRequest {
        let queue = self.request_queue(node);
        let request = queue.pop_front().expect("non-empty queue checked by request_queue");
        self.in_flight_request_count.fetch_sub(1, Ordering::Relaxed);
        request
    }

    /// Can we send more requests to this node?
    ///
    /// Returns true iff we have no requests still being sent to the
    /// given node. Mirrors `InFlightRequests.canSendMore(String)`.
    #[allow(dead_code)] // Used by Phase 5d NetworkClient
    pub(crate) fn can_send_more(&self, node: i32) -> bool {
        match self.requests.get(&node) {
            None => true,
            Some(queue) if queue.is_empty() => true,
            Some(queue) => {
                // Java: queue.peekFirst().send.completed() && queue.size() < max
                let first = queue.front().expect("non-empty");
                let first_send_completed = match &first.send {
                    Some(send) => send.completed(),
                    // Java's `peekFirst().send.completed()` would throw on
                    // a null `send` — the only way an `InFlightRequest`
                    // gets created with no send in production is internal
                    // book-keeping or tests. We treat the absence as
                    // "completed" (the entry isn't blocking the wire).
                    None => true,
                };
                first_send_completed && (queue.len() as i32) < self.max_in_flight_requests_per_connection
            },
        }
    }

    /// Return the number of in-flight requests directed at the given
    /// node. Mirrors `InFlightRequests.count(String)`.
    pub(crate) fn count_for(&self, node: i32) -> i32 {
        self.requests.get(&node).map(|q| q.len() as i32).unwrap_or(0)
    }

    /// Return true if there is no in-flight request directed at the
    /// given node and false otherwise. Mirrors
    /// `InFlightRequests.isEmpty(String)`.
    #[allow(dead_code)] // Used by Phase 5d NetworkClient
    pub(crate) fn is_empty_for(&self, node: i32) -> bool {
        self.requests.get(&node).map(|q| q.is_empty()).unwrap_or(true)
    }

    /// Count all in-flight requests for all nodes. Mirrors
    /// `InFlightRequests.count()`.
    pub(crate) fn count(&self) -> i32 {
        self.in_flight_request_count.load(Ordering::Relaxed)
    }

    /// Return true if there is no in-flight request and false otherwise.
    /// Mirrors `InFlightRequests.isEmpty()`.
    #[allow(dead_code)] // Used by Phase 5d NetworkClient
    pub(crate) fn is_empty(&self) -> bool {
        self.requests.values().all(|q| q.is_empty())
    }

    /// Clear out all the in-flight requests for the given node and
    /// return them in oldest-to-newest order (Java's
    /// `descendingIterator()` semantics — the deque is newest-at-front
    /// so descending == oldest-to-newest).
    ///
    /// Mirrors `InFlightRequests.clearAll(String)`.
    pub(crate) fn clear_all(&mut self, node: i32) -> Vec<InFlightRequest> {
        let removed = self.requests.remove(&node).unwrap_or_default();
        if removed.is_empty() {
            return Vec::new();
        }
        self.in_flight_request_count.fetch_sub(removed.len() as i32, Ordering::Relaxed);
        // Java's `clearedRequests::descendingIterator` walks the deque
        // newest-first → oldest-first as a *reverse* — so the first
        // element of the resulting `Iterable` is the *oldest*. Match
        // that by reversing the front-loaded deque.
        let mut out: Vec<InFlightRequest> = removed.into_iter().collect();
        out.reverse();
        out
    }

    fn has_expired_request(now: i64, deque: &VecDeque<InFlightRequest>) -> bool {
        deque.iter().any(|request| {
            // We exclude throttle time here because we want to ensure that
            // we don't expire requests while they are throttled. The
            // request timeout should take effect only after the throttle
            // time has elapsed.
            request.time_elapsed_since_send_ms(now) - (request.throttle_time_ms() as i64)
                > request.request_timeout_ms as i64
        })
    }

    /// Returns a list of nodes with pending in-flight request, that need
    /// to be timed out. Mirrors
    /// `InFlightRequests.nodesWithTimedOutRequests(long)`.
    ///
    /// The order of the result follows the Java implementation: it walks
    /// `requests.entrySet()` (HashMap iteration order) — the test
    /// `testTimedOutNodes` constructs nodes in an order whose iteration
    /// happens to match the assertion. Rust's `HashMap` does not
    /// preserve insertion order either, so the test sorts the result
    /// before comparison (see `nodes_with_timed_out_requests` test).
    pub(crate) fn nodes_with_timed_out_requests(&self, now: i64) -> Vec<i32> {
        let mut node_ids = Vec::new();
        for (node, deque) in &self.requests {
            if Self::has_expired_request(now, deque) {
                node_ids.push(*node);
            }
        }
        node_ids
    }

    /// Mirrors `InFlightRequests.incrementThrottleTime(String, long)`.
    #[allow(dead_code)] // Used by Phase 5d NetworkClient
    pub(crate) fn increment_throttle_time(&self, node_id: i32, throttle_time_ms: i32) {
        if let Some(deque) = self.requests.get(&node_id) {
            for request in deque {
                request.increment_throttle_time(throttle_time_ms);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    //! Translation of `InFlightRequestsTest`. The test fixtures here use
    //! synthetic `InFlightRequest` values constructed directly (Java's
    //! test does the same via the package-private constructor).

    use std::sync::Arc;

    use super::*;
    use crate::common::protocol::ApiKeys;
    use crate::common::utils::{MockTime, Time};

    const DEST: i32 = 17;

    fn make_request(
        destination: i32,
        send_time_ms: i64,
        request_timeout_ms: i32,
        correlation_id: i32,
    ) -> InFlightRequest {
        let api_key = ApiKeys::for_id(3).expect("METADATA"); // Match Java fixture (METADATA / v0)
        let header = RequestHeader::new(api_key, 0, "clientId", correlation_id);
        InFlightRequest::new(
            header,
            request_timeout_ms,
            0,
            destination,
            None,
            // Match Java fixture (`expectResponse = false`,
            // `isInternalRequest = false`).
            false,
            false,
            None,
            None,
            send_time_ms,
        )
    }

    /// Java: `testCompleteLastSent`.
    #[test]
    fn complete_last_sent() {
        let mut in_flight = InFlightRequests::new(12);
        in_flight.add(make_request(DEST, 0, 10000, 0));
        in_flight.add(make_request(DEST, 0, 10000, 1));
        assert_eq!(in_flight.count(), 2);

        // Java asserts correlation IDs in LIFO order — most recently
        // added comes back first via `completeLastSent`.
        assert_eq!(in_flight.complete_last_sent(DEST).header.correlation_id(), 1);
        assert_eq!(in_flight.count(), 1);

        assert_eq!(in_flight.complete_last_sent(DEST).header.correlation_id(), 0);
        assert_eq!(in_flight.count(), 0);
    }

    /// Java: `testClearAll`.
    #[test]
    fn clear_all() {
        let mut in_flight = InFlightRequests::new(12);
        in_flight.add(make_request(DEST, 0, 10000, 0));
        in_flight.add(make_request(DEST, 0, 10000, 1));

        let cleared = in_flight.clear_all(DEST);
        assert_eq!(in_flight.count(), 0);
        assert_eq!(cleared.len(), 2);
        // Java asserts correlation_id order [first added, second added]
        // because `descendingIterator()` walks newest→oldest in a
        // newest-front deque, which is oldest→newest after reversing.
        assert_eq!(cleared[0].header.correlation_id(), 0);
        assert_eq!(cleared[1].header.correlation_id(), 1);
    }

    /// Java: `testTimedOutNodes`.
    #[test]
    fn nodes_with_timed_out_requests() {
        let mut in_flight = InFlightRequests::new(12);
        let time = Arc::new(MockTime::default());

        in_flight.add(make_request(101, time.milliseconds(), 50, 0));
        in_flight.add(make_request(202, time.milliseconds(), 200, 1));
        in_flight.add(make_request(202, time.milliseconds(), 100, 2));

        time.sleep(50);
        let mut timed_out = in_flight.nodes_with_timed_out_requests(time.milliseconds());
        timed_out.sort();
        assert_eq!(timed_out, Vec::<i32>::new());

        time.sleep(25);
        let mut timed_out = in_flight.nodes_with_timed_out_requests(time.milliseconds());
        timed_out.sort();
        assert_eq!(timed_out, vec![101]);

        time.sleep(50);
        let mut timed_out = in_flight.nodes_with_timed_out_requests(time.milliseconds());
        timed_out.sort();
        assert_eq!(timed_out, vec![101, 202]);
    }

    /// Java: `testCompleteNext`.
    #[test]
    fn complete_next() {
        let mut in_flight = InFlightRequests::new(12);
        in_flight.add(make_request(DEST, 0, 10000, 0));
        in_flight.add(make_request(DEST, 0, 10000, 1));
        assert_eq!(in_flight.count(), 2);

        // Oldest first
        assert_eq!(in_flight.complete_next(DEST).header.correlation_id(), 0);
        assert_eq!(in_flight.count(), 1);

        assert_eq!(in_flight.complete_next(DEST).header.correlation_id(), 1);
        assert_eq!(in_flight.count(), 0);
    }

    /// Java: `testCompleteNextThrowsIfNoInFlights`.
    #[test]
    #[should_panic(expected = "no in-flight requests")]
    fn complete_next_panics_if_no_in_flights() {
        let mut in_flight = InFlightRequests::new(12);
        let _ = in_flight.complete_next(DEST);
    }

    /// Java: `testCompleteLastSentThrowsIfNoInFlights`.
    #[test]
    #[should_panic(expected = "no in-flight requests")]
    fn complete_last_sent_panics_if_no_in_flights() {
        let mut in_flight = InFlightRequests::new(12);
        let _ = in_flight.complete_last_sent(DEST);
    }
}
