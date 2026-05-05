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

//! Translation of `org.apache.kafka.clients.NetworkClientUtils`.
//!
//! Provides "blocking" helpers on top of the non-blocking `NetworkClient`.
//! In Rust the non-blocking client is `async`, so the helpers are also
//! `async fn` (CLAUDE.md rule 9.1) — the "block" semantic is preserved
//! by `.await`-ing the underlying `poll`.
//!
//! ## Skipped vs. Java
//!
//! Java's `sendAndReceive` catches `DisconnectException` to disambiguate
//! "shutdown initiated mid-call" from "broker hung up". The Rust trait
//! does not throw — `NetworkClient::poll` returns an empty list when
//! the client is no longer active, so we surface the same outcome via
//! a [`KafkaError::Network`] when the loop terminates without a match.

use crate::common::Node;
use crate::common::errors::KafkaError;
use crate::common::utils::Time;
use crate::{ClientRequest, ClientResponse, KafkaClient};

/// Mirrors `NetworkClientUtils.isReady(KafkaClient, Node, long)`.
///
/// Drives a single `poll(0)` to flush any pending disconnects, then
/// returns the current readiness state.
pub async fn is_ready<C: KafkaClient>(client: &mut C, node: &Node, current_time: i64) -> bool {
    client.poll(0, current_time).await;
    client.is_ready(node, current_time)
}

/// Mirrors `NetworkClientUtils.awaitReady(KafkaClient, Node, Time, long)`.
///
/// Drives `client.poll` until either the connection is ready, the timeout
/// expires, or the connection fails. Returns `Ok(true)` if ready, `Ok(false)`
/// on timeout, or [`KafkaError::Network`] on connection / authentication
/// failure.
pub async fn await_ready<C: KafkaClient>(
    client: &mut C,
    node: &Node,
    time: &dyn Time,
    timeout_ms: i64,
) -> Result<bool, KafkaError> {
    if timeout_ms < 0 {
        return Err(KafkaError::IllegalArgument("Timeout needs to be greater than 0".to_owned()));
    }
    let start_time = time.milliseconds();
    if is_ready(client, node, start_time).await || client.ready(node, start_time) {
        return Ok(true);
    }
    let mut attempt_start_time = time.milliseconds();
    while !client.is_ready(node, attempt_start_time) && attempt_start_time - start_time < timeout_ms {
        if client.connection_failed(node) {
            return Err(KafkaError::Network(format!("Connection to {} failed.", node)));
        }
        let mut poll_timeout = timeout_ms - (attempt_start_time - start_time);
        let waiting_time = client.poll_delay_ms(node, start_time);
        if waiting_time > 0 && poll_timeout > waiting_time {
            poll_timeout = waiting_time;
        }
        client.poll(poll_timeout, attempt_start_time).await;
        if let Some(auth_err) = client.authentication_error(node) {
            return Err(auth_err);
        }
        attempt_start_time = time.milliseconds();
    }
    Ok(client.is_ready(node, attempt_start_time))
}

/// Mirrors `NetworkClientUtils.sendAndReceive(KafkaClient, ClientRequest, Time)`.
///
/// Sends `request`, then drives `poll` until a matching response arrives
/// or the connection closes. Returns the matched [`ClientResponse`].
pub async fn send_and_receive<C: KafkaClient>(
    client: &mut C,
    request: ClientRequest,
    time: &dyn Time,
) -> Result<ClientResponse, KafkaError> {
    let correlation_id = request.correlation_id();
    client.send(request, time.milliseconds());
    while client.active() {
        let responses = client.poll(i64::MAX, time.milliseconds()).await;
        for response in responses {
            if response.request_header().correlation_id() == correlation_id {
                if response.was_disconnected() {
                    return Err(KafkaError::Network(format!(
                        "Connection to {} was disconnected before the response was read",
                        response.destination()
                    )));
                }
                if let Some(version_mismatch) = response.version_mismatch() {
                    return Err(version_mismatch.clone());
                }
                return Ok(response);
            }
        }
    }
    Err(KafkaError::Network("Client was shutdown before response was read".to_owned()))
}

/// Mirrors `NetworkClientUtils.isUnavailable(KafkaClient, Node, Time)`.
pub fn is_unavailable<C: KafkaClient>(client: &C, node: &Node, time: &dyn Time) -> bool {
    client.connection_failed(node) && client.connection_delay(node, time.milliseconds()) > 0
}

/// Mirrors `NetworkClientUtils.maybeThrowAuthFailure(KafkaClient, Node)`.
/// Returns `Ok(())` when there is no error, otherwise the auth error.
pub fn maybe_return_auth_failure<C: KafkaClient>(client: &C, node: &Node) -> Result<(), KafkaError> {
    match client.authentication_error(node) {
        None => Ok(()),
        Some(e) => Err(e),
    }
}

/// Mirrors `NetworkClientUtils.tryConnect(KafkaClient, Node, Time)`.
pub fn try_connect<C: KafkaClient>(client: &mut C, node: &Node, time: &dyn Time) {
    let _ = client.ready(node, time.milliseconds());
}

#[cfg(test)]
mod tests {
    //! Translation of `NetworkClientUtilsTest` (Java has no dedicated
    //! test file — coverage lives in `NetworkClientTest`'s flow tests).
    //! We validate the small surface that doesn't drive a `poll`:
    //! `is_unavailable`, `maybe_return_auth_failure`, and the
    //! `await_ready` `timeout_ms < 0` precondition.

    use std::sync::Arc;

    use super::*;
    use crate::common::Node;
    use crate::common::utils::MockTime;

    /// Trivial implementation that always reports ready / never failing —
    /// just enough to verify the helpers' synchronous paths.
    struct AlwaysReadyClient {
        connection_failed: bool,
        delay: i64,
    }

    impl KafkaClient for AlwaysReadyClient {
        fn is_ready(&self, _: &Node, _: i64) -> bool {
            true
        }
        fn ready(&mut self, _: &Node, _: i64) -> bool {
            true
        }
        fn connection_delay(&self, _: &Node, _: i64) -> i64 {
            self.delay
        }
        fn poll_delay_ms(&self, _: &Node, _: i64) -> i64 {
            0
        }
        fn connection_failed(&self, _: &Node) -> bool {
            self.connection_failed
        }
        fn authentication_error(&self, _: &Node) -> Option<KafkaError> {
            None
        }
        fn send(&mut self, _: ClientRequest, _: i64) {}
        async fn poll(&mut self, _: i64, _: i64) -> Vec<ClientResponse> {
            Vec::new()
        }
        fn disconnect(&mut self, _: i32) {}
        fn close_connection(&mut self, _: i32) {}
        fn least_loaded_node(&mut self, _: i64) -> crate::LeastLoadedNode {
            crate::LeastLoadedNode::new(None, false)
        }
        fn in_flight_request_count(&self) -> i32 {
            0
        }
        fn has_in_flight_requests(&self) -> bool {
            false
        }
        fn in_flight_request_count_for(&self, _: i32) -> i32 {
            0
        }
        fn has_in_flight_requests_for(&self, _: i32) -> bool {
            false
        }
        fn has_ready_nodes(&self, _: i64) -> bool {
            true
        }
        fn wakeup(&self) {}
        fn new_client_request(
            &mut self,
            _: Arc<str>,
            _: Arc<dyn crate::common::requests::AbstractRequestBuilder>,
            _: i64,
            _: bool,
        ) -> ClientRequest {
            unreachable!("not exercised in these tests")
        }
        fn new_client_request_with_callback(
            &mut self,
            _: Arc<str>,
            _: Arc<dyn crate::common::requests::AbstractRequestBuilder>,
            _: i64,
            _: bool,
            _: i32,
            _: Option<Arc<dyn crate::RequestCompletionHandler>>,
        ) -> ClientRequest {
            unreachable!("not exercised in these tests")
        }
        fn initiate_close(&mut self) {}
        fn active(&self) -> bool {
            true
        }
        fn close(&mut self) {}
    }

    #[test]
    fn is_unavailable_combines_failed_and_delay() {
        let time = MockTime::default();
        let node = Node::new(0, "localhost".into(), 9092);
        let client = AlwaysReadyClient { connection_failed: false, delay: 500 };
        assert!(!is_unavailable(&client, &node, &time));
        let client = AlwaysReadyClient { connection_failed: true, delay: 0 };
        assert!(!is_unavailable(&client, &node, &time));
        let client = AlwaysReadyClient { connection_failed: true, delay: 500 };
        assert!(is_unavailable(&client, &node, &time));
    }

    #[test]
    fn maybe_return_auth_failure_passes_through_none() {
        let node = Node::new(0, "localhost".into(), 9092);
        let client = AlwaysReadyClient { connection_failed: false, delay: 0 };
        assert!(maybe_return_auth_failure(&client, &node).is_ok());
    }

    #[tokio::test]
    async fn await_ready_rejects_negative_timeout() {
        let time = MockTime::default();
        let node = Node::new(0, "localhost".into(), 9092);
        let mut client = AlwaysReadyClient { connection_failed: false, delay: 0 };
        let err = await_ready(&mut client, &node, &time, -1).await.unwrap_err();
        assert!(matches!(err, KafkaError::IllegalArgument(_)));
    }

    /// `await_ready` returns immediately when the client is already ready.
    #[tokio::test]
    async fn await_ready_short_circuits_when_ready() {
        let time = MockTime::default();
        let node = Node::new(0, "localhost".into(), 9092);
        let mut client = AlwaysReadyClient { connection_failed: false, delay: 0 };
        assert!(await_ready(&mut client, &node, &time, 100).await.unwrap());
    }

    // ---- send_and_receive integration tests --------------------------------
    //
    // Java has no dedicated `NetworkClientUtilsTest` (the helpers are
    // covered as a side effect of `NetworkClientTest`'s flow tests). We
    // pin each of the four exit paths of `sendAndReceive` here with a
    // `Vec<ClientResponse>`-queue mock client: the mock is enough to
    // exercise the four error-translation arms without spinning up a
    // real `Selector`.

    use std::collections::VecDeque;

    use super::send_and_receive;
    use crate::ClientResponse;
    use crate::common::protocol::ApiKeys;
    use crate::common::requests::{
        AbstractRequestBuilder, ApiVersionsRequestBuilder, MetadataResponse, RequestHeader, ResponseHeader,
    };

    /// A `KafkaClient` whose `poll` drains a pre-loaded queue of
    /// `Vec<ClientResponse>` slices. Tracks `active` so we can simulate
    /// `initiate_close()` mid-call.
    struct MockKafkaClient {
        responses: VecDeque<Vec<ClientResponse>>,
        active: bool,
        sent: Vec<i32>, // recorded correlation ids
    }

    impl MockKafkaClient {
        fn new(responses: Vec<Vec<ClientResponse>>) -> Self {
            MockKafkaClient { responses: VecDeque::from(responses), active: true, sent: Vec::new() }
        }
    }

    impl KafkaClient for MockKafkaClient {
        fn is_ready(&self, _: &Node, _: i64) -> bool {
            true
        }
        fn ready(&mut self, _: &Node, _: i64) -> bool {
            true
        }
        fn connection_delay(&self, _: &Node, _: i64) -> i64 {
            0
        }
        fn poll_delay_ms(&self, _: &Node, _: i64) -> i64 {
            0
        }
        fn connection_failed(&self, _: &Node) -> bool {
            false
        }
        fn authentication_error(&self, _: &Node) -> Option<KafkaError> {
            None
        }
        fn send(&mut self, request: ClientRequest, _: i64) {
            self.sent.push(request.correlation_id());
        }
        async fn poll(&mut self, _: i64, _: i64) -> Vec<ClientResponse> {
            self.responses.pop_front().unwrap_or_default()
        }
        fn disconnect(&mut self, _: i32) {}
        fn close_connection(&mut self, _: i32) {}
        fn least_loaded_node(&mut self, _: i64) -> crate::LeastLoadedNode {
            crate::LeastLoadedNode::new(None, false)
        }
        fn in_flight_request_count(&self) -> i32 {
            0
        }
        fn has_in_flight_requests(&self) -> bool {
            false
        }
        fn in_flight_request_count_for(&self, _: i32) -> i32 {
            0
        }
        fn has_in_flight_requests_for(&self, _: i32) -> bool {
            false
        }
        fn has_ready_nodes(&self, _: i64) -> bool {
            true
        }
        fn wakeup(&self) {}
        fn new_client_request(
            &mut self,
            _: Arc<str>,
            _: Arc<dyn AbstractRequestBuilder>,
            _: i64,
            _: bool,
        ) -> ClientRequest {
            unreachable!("not exercised in these tests")
        }
        fn new_client_request_with_callback(
            &mut self,
            _: Arc<str>,
            _: Arc<dyn AbstractRequestBuilder>,
            _: i64,
            _: bool,
            _: i32,
            _: Option<Arc<dyn crate::RequestCompletionHandler>>,
        ) -> ClientRequest {
            unreachable!("not exercised in these tests")
        }
        fn initiate_close(&mut self) {
            self.active = false;
        }
        fn active(&self) -> bool {
            self.active
        }
        fn close(&mut self) {
            self.active = false;
        }
    }

    /// Construct an empty `MetadataResponseData` (needed because the
    /// generated struct has no `Default` impl).
    fn empty_metadata_response_data() -> crate::common::message::metadata_response_data::MetadataResponseData {
        crate::common::message::metadata_response_data::MetadataResponseData {
            throttle_time_ms: 0,
            brokers: Vec::new(),
            cluster_id: Some(String::new()),
            controller_id: -1,
            topics: Vec::new(),
            cluster_authorized_operations: 0,
            error_code: 0,
            unknown_tagged_fields: Vec::new(),
        }
    }

    fn make_client_request(correlation_id: i32) -> ClientRequest {
        let builder: Arc<dyn AbstractRequestBuilder> = Arc::new(ApiVersionsRequestBuilder::new());
        ClientRequest::new(
            Arc::from("0"),
            builder,
            correlation_id,
            Arc::from("test-client"),
            0,
            true,
            10_000,
            None,
        )
    }

    fn make_client_response(
        correlation_id: i32,
        disconnected: bool,
        version_mismatch: Option<KafkaError>,
        body: Option<Box<dyn crate::common::requests::AbstractResponse>>,
    ) -> ClientResponse {
        let api_key = ApiKeys::for_id(18).expect("API_VERSIONS");
        let header = RequestHeader::new(api_key, api_key.latest_version(), "test-client", correlation_id);
        ClientResponse::new(header, None, Arc::from("0"), 0, 0, disconnected, version_mismatch, None, body)
    }

    /// Java: `sendAndReceive` happy path — matching response is returned.
    #[tokio::test]
    async fn send_and_receive_returns_matching_response() {
        let time = MockTime::default();
        let correlation_id = 42;
        let response_body: Box<dyn crate::common::requests::AbstractResponse> =
            Box::new(MetadataResponse::new(empty_metadata_response_data(), true));
        let response = make_client_response(correlation_id, false, None, Some(response_body));
        let mut client = MockKafkaClient::new(vec![vec![response]]);
        let request = make_client_request(correlation_id);

        let resp = send_and_receive(&mut client, request, &time).await.expect("send_and_receive");
        assert_eq!(resp.request_header().correlation_id(), correlation_id);
        assert!(resp.has_response());
        assert_eq!(client.sent, vec![correlation_id]);
    }

    /// Java: `sendAndReceive` disconnect arm — response with
    /// `wasDisconnected=true` translates to
    /// `IOException("Connection ... was disconnected ...")`. The Rust
    /// translation surfaces `KafkaError::Network`.
    #[tokio::test]
    async fn send_and_receive_disconnected_response_returns_network_error() {
        let time = MockTime::default();
        let correlation_id = 7;
        let response = make_client_response(correlation_id, /* disconnected= */ true, None, None);
        let mut client = MockKafkaClient::new(vec![vec![response]]);
        let request = make_client_request(correlation_id);

        let err = send_and_receive(&mut client, request, &time)
            .await
            .expect_err("disconnected response must surface as KafkaError::Network");
        match err {
            KafkaError::Network(msg) => {
                assert!(
                    msg.contains("disconnected"),
                    "error message must reference the disconnect: {msg}"
                );
            },
            other => panic!("expected KafkaError::Network, got {other:?}"),
        }
    }

    /// Java: `sendAndReceive` version-mismatch arm — when the
    /// `ClientResponse.versionMismatch()` is set, that exception is
    /// rethrown verbatim. The Rust translation returns the stored
    /// `KafkaError::UnsupportedVersion`.
    #[tokio::test]
    async fn send_and_receive_version_mismatch_returns_stored_error() {
        let time = MockTime::default();
        let correlation_id = 11;
        let mismatch = KafkaError::UnsupportedVersion("test ver mismatch".to_owned());
        let response = make_client_response(correlation_id, false, Some(mismatch), None);
        let mut client = MockKafkaClient::new(vec![vec![response]]);
        let request = make_client_request(correlation_id);

        let err = send_and_receive(&mut client, request, &time)
            .await
            .expect_err("version-mismatch response must surface as KafkaError::UnsupportedVersion");
        match err {
            KafkaError::UnsupportedVersion(msg) => {
                assert_eq!(msg, "test ver mismatch", "error message must round-trip the stored cause");
            },
            other => panic!("expected KafkaError::UnsupportedVersion, got {other:?}"),
        }
    }

    /// Java: `sendAndReceive` shutdown arm — when `client.active()`
    /// becomes `false` mid-loop, the helper exits with
    /// `IOException("Client was shutdown ...")`. The Rust translation
    /// surfaces `KafkaError::Network`.
    #[tokio::test]
    async fn send_and_receive_returns_error_when_client_shut_down() {
        let time = MockTime::default();
        let correlation_id = 99;

        // First poll yields nothing AND flips `active` to false (the mock
        // simulates this by feeding a pre-pop hook via the response
        // queue: an empty Vec on the first pop, then `active=false`).
        // We model it by pre-loading an empty response Vec and explicitly
        // setting `active=false` after sending.
        let mut client = MockKafkaClient::new(vec![Vec::new(), Vec::new()]);
        let request = make_client_request(correlation_id);
        client.active = false;

        let err = send_and_receive(&mut client, request, &time)
            .await
            .expect_err("inactive client must surface as KafkaError::Network");
        match err {
            KafkaError::Network(msg) => {
                assert!(
                    msg.contains("shutdown") || msg.contains("Client"),
                    "error message must reference the shutdown: {msg}"
                );
            },
            other => panic!("expected KafkaError::Network, got {other:?}"),
        }
    }

    /// Java: a non-matching correlation id must NOT be returned, and the
    /// loop must continue polling until either a match or shutdown.
    /// Verifies the inner `for response in responses` filter semantics.
    #[tokio::test]
    async fn send_and_receive_skips_non_matching_responses() {
        let time = MockTime::default();
        let target_correlation_id = 50;
        let body: Box<dyn crate::common::requests::AbstractResponse> =
            Box::new(MetadataResponse::new(empty_metadata_response_data(), true));
        // First poll: response for a *different* correlation id (e.g.
        // an earlier in-flight request from another caller). Second poll:
        // the matching response.
        let bystander = make_client_response(target_correlation_id - 1, false, None, None);
        let target = make_client_response(target_correlation_id, false, None, Some(body));
        let mut client = MockKafkaClient::new(vec![vec![bystander], vec![target]]);
        let request = make_client_request(target_correlation_id);

        let resp = send_and_receive(&mut client, request, &time)
            .await
            .expect("send_and_receive should match on second poll");
        assert_eq!(resp.request_header().correlation_id(), target_correlation_id);
        assert!(resp.has_response());
    }

    /// Helper trait surface — `ResponseHeader` exists and `RequestHeader`
    /// is constructible. Pull these into the namespace so the rustdoc
    /// links resolve.
    #[allow(dead_code)]
    fn _resolve_doc_links() -> ResponseHeader {
        ResponseHeader::new(0, 1)
    }
}
