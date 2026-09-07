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

//! A request being sent to the server, holding both the network send and client-level metadata.
//!
//! Translated from `org.apache.kafka.clients.ClientRequest`.

use std::fmt;
use std::io;

use crate::common::protocol::ApiKeys;
use crate::common::requests::{RequestBuilder, RequestHeader, RequestHeaderOptionsBuilder};

use super::RequestCompletionHandler;

/// A request being sent to the server.
///
/// This holds both the network send as well as the client-level metadata.
pub struct ClientRequest {
    /// The broker id to send the request to.
    destination: String,
    /// The builder for the request to make.
    request_builder: Box<dyn RequestBuilder>,
    /// The correlation id for this client request.
    correlation_id: i32,
    /// The client ID to use for the header.
    client_id: String,
    /// The unix timestamp in milliseconds for the time at which this request was created.
    created_time_ms: i64,
    /// Whether we expect a response message or this request is complete once sent.
    expect_response: bool,
    /// The request timeout in milliseconds.
    request_timeout_ms: i32,
    /// A callback to execute when the response has been received (or `None` if no
    /// callback is necessary).
    callback: Option<RequestCompletionHandler>,
}

impl ClientRequest {
    /// Creates a new `ClientRequest`.
    ///
    /// # Arguments
    ///
    /// * `destination` - The broker id to send the request to
    /// * `request_builder` - The builder for the request to make
    /// * `correlation_id` - The correlation id for this client request
    /// * `client_id` - The client ID to use for the header
    /// * `created_time_ms` - The unix timestamp in milliseconds for the time at which this
    ///   request was created
    /// * `expect_response` - Whether we expect a response message or this request is complete
    ///   once sent
    /// * `request_timeout_ms` - The request timeout in milliseconds
    /// * `callback` - A callback to execute when the response has been received
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        destination: &str,
        request_builder: Box<dyn RequestBuilder>,
        correlation_id: i32,
        client_id: &str,
        created_time_ms: i64,
        expect_response: bool,
        request_timeout_ms: i32,
        callback: Option<RequestCompletionHandler>,
    ) -> Self {
        Self {
            destination: destination.to_string(),
            request_builder,
            correlation_id,
            client_id: client_id.to_string(),
            created_time_ms,
            expect_response,
            request_timeout_ms,
            callback,
        }
    }

    /// Returns whether a response is expected for this request.
    pub fn expect_response(&self) -> bool {
        self.expect_response
    }

    /// Returns the API key for this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        self.request_builder.api_key()
    }

    /// Creates a [`RequestHeader`] for this request at the given version.
    ///
    /// # Errors
    ///
    /// Returns an error if the API key is not recognized.
    pub fn make_header(&self, version: i16) -> io::Result<RequestHeader> {
        RequestHeader::new_options(
            RequestHeaderOptionsBuilder::new_request_api_key_request_version_client_id_correlation_id(
                self.api_key(),
                version,
                &self.client_id,
                self.correlation_id,
            )
            .build(),
        )
    }

    /// Returns a reference to the request builder.
    pub fn request_builder(&self) -> &dyn RequestBuilder {
        self.request_builder.as_ref()
    }

    /// Returns a mutable reference to the request builder.
    pub fn request_builder_mut(&mut self) -> &mut dyn RequestBuilder {
        self.request_builder.as_mut()
    }

    /// Returns the destination broker id.
    pub fn destination(&self) -> &str {
        &self.destination
    }

    /// Returns the callback, if any.
    ///
    /// Note: Since `RequestCompletionHandler` uses `FnOnce` which cannot be cloned,
    /// this method takes ownership of the callback by removing it from the request.
    /// Subsequent calls will return `None`.
    pub fn take_callback(&mut self) -> Option<RequestCompletionHandler> {
        self.callback.take()
    }

    /// Returns the creation time in milliseconds.
    pub fn created_time_ms(&self) -> i64 {
        self.created_time_ms
    }

    /// Returns the correlation id.
    pub fn correlation_id(&self) -> i32 {
        self.correlation_id
    }

    /// Returns the request timeout in milliseconds.
    pub fn request_timeout_ms(&self) -> i32 {
        self.request_timeout_ms
    }
}

impl fmt::Display for ClientRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ClientRequest(expectResponse={}, callback={}, destination={}, \
             correlationId={}, clientId={}, createdTimeMs={})",
            self.expect_response,
            if self.callback.is_some() { "Some" } else { "None" },
            self.destination,
            self.correlation_id,
            self.client_id,
            self.created_time_ms,
        )
    }
}
