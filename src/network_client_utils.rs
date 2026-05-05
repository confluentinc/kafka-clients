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
}
