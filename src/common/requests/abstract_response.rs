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

//! Translation of `org.apache.kafka.common.requests.AbstractResponse`.

use std::collections::HashMap;

use crate::common::errors::KafkaError;
use crate::common::protocol::ApiKey;
use crate::common::protocol::Errors;
use crate::common::protocol::byte_buffer_accessor::ByteBufferAccessor;
use crate::common::protocol::message_util::to_byte_buffer_accessor;
use crate::common::requests::AbstractRequestResponse;
use crate::common::requests::CorrelationIdMismatchError;
use crate::common::requests::RequestHeader;
use crate::common::requests::ResponseHeader;
use crate::common::requests::request_utils;

/// Mirrors `AbstractResponse.DEFAULT_THROTTLE_TIME = 0`.
pub const DEFAULT_THROTTLE_TIME: i32 = 0;

/// Translation of `org.apache.kafka.common.requests.AbstractResponse`.
///
/// In Java this is an abstract class with state (`apiKey`) and abstract
/// methods (`data()`, `errorCounts()`, `throttleTimeMs()`,
/// `maybeSetThrottleTimeMs(int)`). In Rust we model it as a trait whose
/// state-bearing methods are required and helpers (`serialize`,
/// `serialize_with_header`) are provided.
pub trait AbstractResponse: AbstractRequestResponse {
    /// The API key of this response. Mirrors `AbstractResponse.apiKey()`.
    fn api_key(&self) -> &'static ApiKey;

    /// The number of each type of error in the response, including
    /// [`Errors::None`]. Mirrors `AbstractResponse.errorCounts()`.
    fn error_counts(&self) -> HashMap<Errors, i32>;

    /// Returns the throttle time in milliseconds. Mirrors
    /// `AbstractResponse.throttleTimeMs()`.
    fn throttle_time_ms(&self) -> i32;

    /// Set the throttle time on this response if the schema supports it.
    /// Otherwise a no-op. Mirrors `AbstractResponse.maybeSetThrottleTimeMs(int)`.
    fn maybe_set_throttle_time_ms(&mut self, throttle_time_ms: i32);

    /// Mirrors `AbstractResponse.shouldClientThrottle(short version)`. The
    /// default returns `false`; per-API responses override.
    fn should_client_throttle(&self, _version: i16) -> bool {
        false
    }

    /// Test-visible: serialize the response body. Mirrors
    /// `AbstractResponse.serialize(short)`.
    fn serialize(&self, version: i16) -> Result<ByteBufferAccessor, KafkaError> {
        to_byte_buffer_accessor(self.data(), version)
    }

    /// Mirrors `AbstractResponse.serializeWithHeader(ResponseHeader, short)`.
    /// Concatenates header+body bytes, no length prefix.
    fn serialize_with_header(&self, header: &ResponseHeader, version: i16) -> Result<Vec<u8>, KafkaError> {
        request_utils::serialize(header.header_data(), header.header_version(), self.data(), version)
    }
}

/// Free function mirror of Java's static `AbstractResponse.parseResponse(
/// ByteBuffer, RequestHeader)`.
///
/// Reads the [`ResponseHeader`] off the front of `accessor`, validates the
/// correlation id matches, then dispatches to the per-API parser.
///
/// Phase 2e wires only the producer-relevant APIs (Produce, Metadata,
/// ApiVersions). Other API keys return [`KafkaError::UnsupportedVersion`]
/// — Phase 5+ will fill the rest in if needed.
pub fn parse_response(
    accessor: &mut ByteBufferAccessor,
    request_header: &RequestHeader,
) -> Result<Box<dyn AbstractResponse>, KafkaError> {
    let api_key = request_header.api_key()?;
    let api_version = request_header.api_version();
    let response_header_version = api_key.response_header_version(api_version);
    let response_header = ResponseHeader::parse(accessor, response_header_version)?;

    if request_header.correlation_id() != response_header.correlation_id() {
        return Err(KafkaError::Generic(
            CorrelationIdMismatchError::new(
                format!(
                    "Correlation id for response ({}) does not match request ({}), request header: {request_header}",
                    response_header.correlation_id(),
                    request_header.correlation_id(),
                ),
                request_header.correlation_id(),
                response_header.correlation_id(),
            )
            .to_string(),
        ));
    }

    parse_response_body(api_key, accessor, api_version)
}

/// Mirror of Java's `AbstractResponse.parseResponse(ApiKeys, Readable, short)`.
/// Translates only producer-relevant API keys; others return
/// [`KafkaError::UnsupportedVersion`].
pub fn parse_response_body(
    api_key: &'static ApiKey,
    accessor: &mut ByteBufferAccessor,
    api_version: i16,
) -> Result<Box<dyn AbstractResponse>, KafkaError> {
    use crate::common::requests::{ApiVersionsResponse, MetadataResponse, ProduceResponse};
    match api_key.id {
        // PRODUCE = 0
        0 => Ok(Box::new(ProduceResponse::parse(accessor, api_version)?)),
        // METADATA = 3
        3 => Ok(Box::new(MetadataResponse::parse(accessor, api_version)?)),
        // API_VERSIONS = 18
        18 => Ok(Box::new(ApiVersionsResponse::parse(accessor, api_version)?)),
        _ => Err(KafkaError::UnsupportedVersion(format!(
            "ApiKey {} ({}) is not currently handled in `parse_response`. Phase 2e wires only Produce, Metadata, and ApiVersions.",
            api_key.id, api_key.name,
        ))),
    }
}

/// Helper: produce an `errorCounts` map containing a single
/// (error, 1) entry. Mirrors `AbstractResponse.errorCounts(Errors)`.
pub fn error_counts_one(error: Errors) -> HashMap<Errors, i32> {
    let mut map = HashMap::new();
    map.insert(error, 1);
    map
}

/// Helper: produce an `errorCounts` map by counting occurrences of each
/// error in `errors`. Mirrors `AbstractResponse.errorCounts(Collection<Errors>)`.
pub fn error_counts_from_iter(errors: impl IntoIterator<Item = Errors>) -> HashMap<Errors, i32> {
    let mut map = HashMap::new();
    for e in errors {
        *map.entry(e).or_insert(0) += 1;
    }
    map
}

/// Helper: increment `error` in `error_counts` by 1. Mirrors
/// `AbstractResponse.updateErrorCounts(Map<Errors, Integer>, Errors)`.
pub fn update_error_counts(error_counts: &mut HashMap<Errors, i32>, error: Errors) {
    *error_counts.entry(error).or_insert(0) += 1;
}
