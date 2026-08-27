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
//! Provides additional utilities for [`NetworkClient`](super::network_client::NetworkClient)
//! (e.g. to implement blocking behaviour).
//!
//! Translated from `org.apache.kafka.clients.NetworkClientUtils`.

use std::io;

use crate::common::Node;

use super::ClientRequest;
use super::ClientResponse;
use super::KafkaClient;

/// Checks whether the node is currently connected, first calling `client.poll` to ensure
/// that any pending disconnects have been processed.
///
/// This method can be used to check the status of a connection prior to calling the blocking
/// version to be able to tell whether the latter completed a new connection.
///
/// Returns the readiness together with **every** [`ClientResponse`] the internal
/// `client.poll` collected. Unlike Java's `NetworkClient.poll` — which dispatches
/// each response to its `RequestCompletionHandler` as a side effect — this crate's
/// `poll` only *collects* responses; the caller routes them by correlation id (see
/// PLAN §9.28). Discarding the returned responses here would silently lose a
/// produce/transactional response for a different in-flight request that lands on
/// the shared selector during this poll.
pub async fn is_ready<C: KafkaClient>(client: &mut C, node: &Node, current_time: i64) -> (bool, Vec<ClientResponse>) {
    let responses = client.poll(0, current_time).await;
    (client.is_ready(node, current_time), responses)
}

/// Invokes `client.poll` to discard pending disconnects, followed by `client.ready` and
/// 0 or more `client.poll` invocations until the connection to `node` is ready, the
/// `timeout_ms` expires or the connection fails.
///
/// It returns `true` if the call completes normally or `false` if the `timeout_ms` expires.
/// If the connection fails, an `io::Error` is returned instead. Note that if the
/// `NetworkClient` has been configured with a positive connection timeout, it is possible
/// for this method to return an error for a previous connection which has recently
/// disconnected. If authentication to the node fails, an authentication error is returned.
///
/// This method is useful for implementing blocking behaviour on top of the non-blocking
/// `NetworkClient`, use it with care.
///
/// The return shape is `(Vec<ClientResponse>, io::Result<bool>)`: the collected
/// responses are **always** surfaced, *outside* the `Result`, so they reach the
/// caller even when the readiness attempt ends in a connection-failed or
/// authentication error. Every internal `client.poll` call contributes to the Vec
/// (the initial [`is_ready`] poll and every loop poll), so the caller can route
/// them by correlation id (see [`is_ready`] and PLAN §9.28). The caller MUST
/// dispatch the returned responses **before** propagating any error; dropping them
/// silently loses a produce/transactional response for a different in-flight
/// request that arrived on the shared selector while awaiting readiness. Java's
/// `client.poll()` self-dispatches before it throws
/// (`NetworkClientUtils.java:43,70-71,85-87`), so it loses nothing on these error
/// paths — this shape reproduces that. (An earlier shape,
/// `io::Result<(bool, Vec)>`, dropped the Vec on the two `Err` early-returns.)
///
/// # Arguments
///
/// * `client` - The Kafka client to use
/// * `node` - The node to await readiness for
/// * `now_ms_fn` - A function that returns the current time in milliseconds. `Send`
///   and `Sync` because this future is awaited inside the producer's spawned
///   `Sender` task, and `&dyn Fn()` is only `Send` when the trait object is `Sync`
/// * `timeout_ms` - The maximum time to wait in milliseconds
pub async fn await_ready<C: KafkaClient>(
    client: &mut C,
    node: &Node,
    now_ms_fn: &(dyn Fn() -> i64 + Send + Sync),
    timeout_ms: i64,
) -> (Vec<ClientResponse>, io::Result<bool>) {
    if timeout_ms < 0 {
        return (
            Vec::new(),
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Timeout needs to be greater than 0",
            )),
        );
    }

    let start_time = now_ms_fn();

    // Accumulate the responses from every internal poll so the caller can route
    // them, rather than discarding them as a bare `client.poll(..)` would. The Vec
    // rides *alongside* the result on every exit — success, timeout, and both error
    // returns — so a response for an unrelated in-flight request that landed during
    // the readiness poll is never lost.
    let (ready, mut responses) = is_ready(client, node, start_time).await;
    if ready || client.ready(node, start_time).await {
        return (responses, Ok(true));
    }

    let mut attempt_start_time = now_ms_fn();
    while !client.is_ready(node, attempt_start_time) && attempt_start_time - start_time < timeout_ms {
        if client.connection_failed(node) {
            return (
                responses,
                Err(io::Error::new(
                    io::ErrorKind::ConnectionRefused,
                    format!("Connection to {} failed.", node),
                )),
            );
        }
        let mut poll_timeout = timeout_ms - (attempt_start_time - start_time);

        // If the network client is waiting to send data for some reason (eg. throttling
        // or retry backoff), polling longer than that is potentially dangerous as the
        // producer will not attempt to send any pending requests.
        let waiting_time = client.poll_delay_ms(node, start_time);
        if waiting_time > 0 && poll_timeout > waiting_time {
            poll_timeout = waiting_time;
        }

        responses.extend(client.poll(poll_timeout, attempt_start_time).await);
        if let Some(auth_error) = client.authentication_error(node) {
            return (responses, Err(io::Error::new(io::ErrorKind::PermissionDenied, auth_error)));
        }
        attempt_start_time = now_ms_fn();
    }

    (responses, Ok(client.is_ready(node, attempt_start_time)))
}

/// Invokes `client.send` followed by 1 or more `client.poll` invocations until a response
/// is received or a disconnection happens (which can happen for a number of reasons
/// including a request timeout).
///
/// In case of a disconnection, an `io::Error` is returned.
/// If shutdown is initiated on the client during this method, an `io::Error` is returned.
///
/// This method is useful for implementing blocking behaviour on top of the non-blocking
/// `NetworkClient`, use it with care.
///
/// # Arguments
///
/// * `client` - The Kafka client to use
/// * `request` - The request to send
/// * `now_ms_fn` - A function that returns the current time in milliseconds
pub async fn send_and_receive<C: KafkaClient>(
    client: &mut C,
    request: ClientRequest,
    now_ms_fn: &dyn Fn() -> i64,
) -> io::Result<ClientResponse> {
    let correlation_id = request.correlation_id();
    client.send(request, now_ms_fn());

    while client.active() {
        let responses = client.poll(i64::MAX, now_ms_fn()).await;
        for response in responses {
            if response.request_header().correlation_id() == correlation_id {
                if response.was_disconnected() {
                    return Err(io::Error::new(
                        io::ErrorKind::ConnectionReset,
                        format!(
                            "Connection to {} was disconnected before the response was read",
                            response.destination()
                        ),
                    ));
                }
                if response.version_mismatch().is_some() {
                    return Err(io::Error::new(io::ErrorKind::Unsupported, "UnsupportedVersionError"));
                }
                return Ok(response);
            }
        }
    }

    Err(io::Error::new(
        io::ErrorKind::ConnectionAborted,
        "Client was shutdown before response was read",
    ))
}

/// Check if the node is disconnected and unavailable for immediate reconnection
/// (i.e. if it is in reconnect backoff window following the disconnect).
pub fn is_unavailable<C: KafkaClient>(client: &C, node: &Node, now: i64) -> bool {
    client.connection_failed(node) && client.connection_delay(node, now) > 0
}

/// Check for an authentication error on a given node and return the error if there
/// is one.
pub fn maybe_return_auth_failure<C: KafkaClient>(client: &C, node: &Node) -> io::Result<()> {
    if let Some(err) = client.authentication_error(node) {
        Err(io::Error::new(io::ErrorKind::PermissionDenied, err))
    } else {
        Ok(())
    }
}

/// Initiate a connection if currently possible. This is only really useful for resetting
/// the failed status of a socket.
pub async fn try_connect<C: KafkaClient>(client: &mut C, node: &Node, now: i64) {
    client.ready(node, now).await;
}
