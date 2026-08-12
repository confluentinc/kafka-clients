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

//! A response from the server, containing both the response body and correlated
//! request metadata.
//!
//! Translated from `org.apache.kafka.clients.ClientResponse`.

use std::fmt;

use crate::common::requests::{ConcreteResponse, RequestHeader};

use super::RequestCompletionHandler;

/// A response from the server.
///
/// Contains both the body of the response as well as the correlated request
/// metadata that was originally sent.
pub struct ClientResponse {
    /// The header of the corresponding request.
    request_header: RequestHeader,
    /// The callback to be invoked when the response is complete.
    callback: Option<RequestCompletionHandler>,
    /// The node the corresponding request was sent to.
    destination: String,
    /// The unix timestamp when this response was received.
    received_time_ms: i64,
    /// The latency in milliseconds (received_time_ms - created_time_ms).
    latency_ms: i64,
    /// Whether the client disconnected before fully reading a response.
    disconnected: bool,
    /// Whether the client was disconnected because of a timeout.
    timed_out: bool,
    /// Error message if there was a version mismatch that prevented sending the request.
    ///
    /// In Java this is an `UnsupportedVersionException`. We represent it as an
    /// optional error string since `KafkaError` is the primary error type.
    version_mismatch: Option<String>,
    /// Error message if there was an authentication error.
    ///
    /// In Java this is an `AuthenticationException`. We represent it as an
    /// optional error string since `KafkaError` is the primary error type.
    authentication_error: Option<String>,
    /// The response contents, or `None` if we disconnected, no response was expected,
    /// or if there was a version mismatch.
    response_body: Option<ConcreteResponse>,
}

impl ClientResponse {
    /// Creates a new `ClientResponse` without a timeout flag.
    ///
    /// # Arguments
    ///
    /// * `request_header` - The header of the corresponding request
    /// * `callback` - The callback to be invoked
    /// * `destination` - The node the corresponding request was sent to
    /// * `created_time_ms` - The unix timestamp when the corresponding request was created
    /// * `received_time_ms` - The unix timestamp when this response was received
    /// * `disconnected` - Whether the client disconnected before fully reading a response
    /// * `version_mismatch` - Error message if there was a version mismatch
    /// * `authentication_error` - Error message if there was an authentication error
    /// * `response_body` - The response contents (or `None`)
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        request_header: RequestHeader,
        callback: Option<RequestCompletionHandler>,
        destination: &str,
        created_time_ms: i64,
        received_time_ms: i64,
        disconnected: bool,
        version_mismatch: Option<String>,
        authentication_error: Option<String>,
        response_body: Option<ConcreteResponse>,
    ) -> Self {
        Self::with_timeout(
            request_header,
            callback,
            destination,
            created_time_ms,
            received_time_ms,
            disconnected,
            false,
            version_mismatch,
            authentication_error,
            response_body,
        )
    }

    /// Creates a new `ClientResponse` with all fields including the timeout flag.
    ///
    /// # Panics
    ///
    /// Panics if `timed_out` is `true` but `disconnected` is `false`, since
    /// a timed-out response is always considered disconnected.
    ///
    /// # Arguments
    ///
    /// * `request_header` - The header of the corresponding request
    /// * `callback` - The callback to be invoked
    /// * `destination` - The node the corresponding request was sent to
    /// * `created_time_ms` - The unix timestamp when the corresponding request was created
    /// * `received_time_ms` - The unix timestamp when this response was received
    /// * `disconnected` - Whether the client disconnected before fully reading a response
    /// * `timed_out` - Whether the client was disconnected because of a timeout;
    ///   when `true`, `disconnected` must also be `true`
    /// * `version_mismatch` - Error message if there was a version mismatch
    /// * `authentication_error` - Error message if there was an authentication error
    /// * `response_body` - The response contents (or `None`)
    #[allow(clippy::too_many_arguments)]
    pub fn with_timeout(
        request_header: RequestHeader,
        callback: Option<RequestCompletionHandler>,
        destination: &str,
        created_time_ms: i64,
        received_time_ms: i64,
        disconnected: bool,
        timed_out: bool,
        version_mismatch: Option<String>,
        authentication_error: Option<String>,
        response_body: Option<ConcreteResponse>,
    ) -> Self {
        assert!(
            disconnected || !timed_out,
            "The client response can't be in the state of connected, yet timed out"
        );

        Self {
            request_header,
            callback,
            destination: destination.to_string(),
            received_time_ms,
            latency_ms: received_time_ms - created_time_ms,
            disconnected,
            timed_out,
            version_mismatch,
            authentication_error,
            response_body,
        }
    }

    /// Returns the unix timestamp when this response was received.
    pub fn received_time_ms(&self) -> i64 {
        self.received_time_ms
    }

    /// Returns whether the client disconnected before fully reading a response.
    pub fn was_disconnected(&self) -> bool {
        self.disconnected
    }

    /// Returns whether the client was disconnected because of a timeout.
    pub fn was_timed_out(&self) -> bool {
        self.timed_out
    }

    /// Returns the version mismatch error message, if any.
    pub fn version_mismatch(&self) -> Option<&str> {
        self.version_mismatch.as_deref()
    }

    /// Returns the authentication error error message, if any.
    pub fn authentication_error(&self) -> Option<&str> {
        self.authentication_error.as_deref()
    }

    /// Returns a reference to the request header.
    pub fn request_header(&self) -> &RequestHeader {
        &self.request_header
    }

    /// Returns the destination node id.
    pub fn destination(&self) -> &str {
        &self.destination
    }

    /// Returns a reference to the response body, if present.
    pub fn response_body(&self) -> Option<&ConcreteResponse> {
        self.response_body.as_ref()
    }

    /// Takes the response body out of this response, leaving `None` in its
    /// place. Used by callbacks that need owned access to the body when
    /// the [`ClientResponse`] itself is only available by `&mut`.
    pub fn take_response_body(&mut self) -> Option<ConcreteResponse> {
        self.response_body.take()
    }

    /// Returns whether this response has a body.
    pub fn has_response(&self) -> bool {
        self.response_body.is_some()
    }

    /// Returns the request latency in milliseconds
    /// (`received_time_ms - created_time_ms`). Translates Java's
    /// `ClientResponse.requestLatencyMs()` (`ClientResponse.java:148`), which is
    /// the only latency accessor Java exposes.
    pub fn request_latency_ms(&self) -> i64 {
        self.latency_ms
    }

    /// Invokes the completion callback with this response, if a callback was set.
    ///
    /// The callback is taken out (consumed) by this call. Subsequent calls will
    /// be no-ops.
    pub fn on_complete(&mut self) {
        if let Some(callback) = self.callback.take() {
            callback(self);
        }
    }
}

impl fmt::Display for ClientResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ClientResponse(receivedTimeMs={}, latencyMs={}, disconnected={}, \
             timedOut={}, requestHeader={}, responseBody={})",
            self.received_time_ms,
            self.latency_ms,
            self.disconnected,
            self.timed_out,
            self.request_header,
            match &self.response_body {
                Some(body) => format!("{body}"),
                None => "None".to_string(),
            },
        )
    }
}
