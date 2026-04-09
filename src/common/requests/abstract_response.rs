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

//! Abstract response framework for Kafka protocol responses.
//!
//! Corresponds to `org.apache.kafka.common.requests.AbstractResponse`.
//!
//! Java uses an abstract class with per-type subclasses. In Rust we use an enum
//! (`ConcreteResponse`) with a variant for each supported response type. Currently
//! only ApiVersions and Metadata are supported; other variants will be added as
//! their request/response types are translated.

use std::collections::HashMap;
use std::io;

use crate::common::network::ByteBufferSend;
use crate::common::protocol::{ApiKeys, ByteBufferAccessor, Errors, Readable};

use super::request_header::RequestHeader;
use super::response_header::ResponseHeader;

/// Default throttle time in milliseconds.
pub const DEFAULT_THROTTLE_TIME: i32 = 0;

/// Enum dispatch for all supported Kafka response types.
///
/// Each variant wraps a concrete response struct. Common methods are dispatched
/// via `match` on the variant.
///
/// Currently only ApiVersions and Metadata are supported — variants will be
/// added in Steps 3 and 4.
#[derive(Debug, Clone)]
pub enum ConcreteResponse {
    // Variants will be added in Step 3 and Step 4 (ApiVersions, Metadata).
}

impl ConcreteResponse {
    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        match *self {
            // Will be populated when concrete response types are added.
        }
    }

    /// Builds a size-prefixed [`ByteBufferSend`] for network transmission.
    ///
    /// Corresponds to `AbstractResponse.toSend` in Java.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails.
    pub fn to_send(&self, _header: &ResponseHeader, _version: i16) -> io::Result<ByteBufferSend> {
        match *self {
            // Will be populated when concrete response types are added.
        }
    }

    /// Serializes header and body without a size prefix.
    ///
    /// Corresponds to `AbstractResponse.serializeWithHeader` in Java.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails.
    pub fn serialize_with_header(&self, _header: &ResponseHeader, _version: i16) -> io::Result<ByteBufferAccessor> {
        match *self {
            // Will be populated when concrete response types are added.
        }
    }

    /// Serializes just the response body (no header, no size prefix).
    ///
    /// Corresponds to `AbstractResponse.serialize` in Java (visible for testing).
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails.
    pub fn serialize(&self, _version: i16) -> io::Result<ByteBufferAccessor> {
        match *self {
            // Will be populated when concrete response types are added.
        }
    }

    /// Returns the error counts for this response.
    pub fn error_counts(&self) -> HashMap<Errors, i32> {
        match *self {
            // Will be populated when concrete response types are added.
        }
    }

    /// Returns the throttle time in milliseconds.
    ///
    /// Returns 0 if the response schema does not support this field.
    pub fn throttle_time_ms(&self) -> i32 {
        match *self {
            // Will be populated when concrete response types are added.
        }
    }

    /// Sets the throttle time in the response if the schema supports it.
    /// Otherwise, this is a no-op.
    pub fn maybe_set_throttle_time_ms(&mut self, _throttle_time_ms: i32) {
        match *self {
            // Will be populated when concrete response types are added.
        }
    }

    /// Returns whether the client should throttle upon receiving this response.
    pub fn should_client_throttle(&self, _version: i16) -> bool {
        match *self {
            // Will be populated when concrete response types are added.
        }
    }

    /// Parses a response from a buffer that contains both the response header and the
    /// response body.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The response header cannot be parsed
    /// - The correlation id in the response does not match the request
    /// - The response body cannot be parsed
    pub fn parse_response(
        buffer: &mut crate::common::protocol::ByteBufferAccessor,
        request_header: &RequestHeader,
    ) -> io::Result<Self> {
        let api_key = request_header.api_key();
        let api_version = request_header.api_version();

        let response_header = ResponseHeader::parse(buffer, api_key.response_header_version(api_version))?;

        if request_header.correlation_id() != response_header.correlation_id() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Correlation id for response ({}) does not match request ({}), request header: {}",
                    response_header.correlation_id(),
                    request_header.correlation_id(),
                    request_header
                ),
            ));
        }

        Self::parse(api_key, buffer, api_version)
    }

    /// Parses a response body from the buffer for the given API key and version.
    ///
    /// # Errors
    ///
    /// Returns an error if the API key is not supported or parsing fails.
    pub fn parse(api_key: &ApiKeys, _readable: &mut dyn Readable, _version: i16) -> io::Result<Self> {
        // Only ApiVersions and Metadata will be supported — added in Steps 3-4.
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!("ApiKey {} is not currently handled in parse_response", api_key.name()),
        ))
    }
}

impl std::fmt::Display for ConcreteResponse {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            // Will be populated when concrete response types are added.
        }
    }
}

/// Helper: counts a single error, returning a map with one entry.
pub fn single_error_count(error: Errors) -> HashMap<Errors, i32> {
    let mut map = HashMap::new();
    map.insert(error, 1);
    map
}

/// Helper: increments the count for the given error in the map.
pub fn update_error_counts(error_counts: &mut HashMap<Errors, i32>, error: Errors) {
    *error_counts.entry(error).or_insert(0) += 1;
}
