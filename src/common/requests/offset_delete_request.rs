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

//! `OffsetDelete` request handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.OffsetDeleteRequest`.
//!
//! Wraps the auto-generated [`OffsetDeleteRequestData`] and exposes an
//! [`OffsetDeleteRequestBuilder`] used by the admin client's
//! `deleteConsumerGroupOffsets` path. `OffsetDelete` is a dedicated RPC
//! (`ApiKeys.OFFSET_DELETE`) — deletion is NOT expressed as an `OffsetCommit`
//! with a sentinel offset.

use std::io;

use crate::OffsetDeleteRequestData;
use crate::OffsetDeleteResponseData;
use crate::common::protocol::{ApiKeys, Errors, Readable};

use super::ConcreteResponse;
use super::OffsetDeleteResponse;
use super::abstract_request::{ConcreteRequest, RequestBuilder};

/// An `OffsetDelete` request.
///
/// Corresponds to `org.apache.kafka.common.requests.OffsetDeleteRequest`.
#[derive(Debug, Clone)]
pub struct OffsetDeleteRequest {
    data: OffsetDeleteRequestData,
    version: i16,
}

impl OffsetDeleteRequest {
    /// Creates a new `OffsetDeleteRequest` from data and version.
    ///
    /// Mirrors Java's constructor `OffsetDeleteRequest(OffsetDeleteRequestData, short)`.
    pub fn new(data: OffsetDeleteRequestData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &OffsetDeleteRequestData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut OffsetDeleteRequestData {
        &mut self.data
    }

    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Returns the API key for this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::OFFSET_DELETE
    }

    /// Builds the canonical error response for this request, matching Java's
    /// `OffsetDeleteRequest.getErrorResponse(int, Throwable)`: a top-level
    /// error code covering the whole request.
    pub fn get_error_response(&self, throttle_time_ms: i32, error: &Errors) -> ConcreteResponse {
        let mut data = OffsetDeleteResponseData::new();
        data.set_throttle_time_ms(throttle_time_ms);
        data.set_error_code(error.code());
        ConcreteResponse::OffsetDelete(OffsetDeleteResponse::new(data))
    }

    /// Parses an `OffsetDeleteRequest` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = OffsetDeleteRequestData::read(readable, version)?;
        Ok(Self::new(data, version))
    }
}

impl std::fmt::Display for OffsetDeleteRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "OffsetDeleteRequest(version={}, data={})", self.version, self.data)
    }
}

/// Builder for [`OffsetDeleteRequest`].
///
/// Corresponds to `OffsetDeleteRequest.Builder` in Java.
#[derive(Debug, Clone)]
pub struct OffsetDeleteRequestBuilder {
    data: OffsetDeleteRequestData,
}

impl OffsetDeleteRequestBuilder {
    /// Creates a builder over the given request data.
    ///
    /// Mirrors Java's `OffsetDeleteRequest.Builder(OffsetDeleteRequestData)`.
    pub fn new(data: OffsetDeleteRequestData) -> Self {
        Self { data }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &OffsetDeleteRequestData {
        &self.data
    }
}

impl RequestBuilder for OffsetDeleteRequestBuilder {
    fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::OFFSET_DELETE
    }

    fn oldest_allowed_version(&self) -> i16 {
        ApiKeys::OFFSET_DELETE.oldest_version()
    }

    fn latest_allowed_version(&self) -> i16 {
        ApiKeys::OFFSET_DELETE.latest_version()
    }

    fn build_version(&mut self, version: i16) -> io::Result<ConcreteRequest> {
        if version < self.oldest_allowed_version() || version > self.latest_allowed_version() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!(
                    "Cannot build OffsetDelete request with version {version} (allowed range: {}..={})",
                    self.oldest_allowed_version(),
                    self.latest_allowed_version(),
                ),
            ));
        }
        Ok(ConcreteRequest::OffsetDelete(OffsetDeleteRequest::new(
            self.data.clone(),
            version,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::offset_delete_request_data::{OffsetDeleteRequestPartition, OffsetDeleteRequestTopic};

    fn topic(name: &str, partition_index: i32) -> OffsetDeleteRequestTopic {
        let mut topic = OffsetDeleteRequestTopic::new();
        topic.set_name(name.to_string());
        let mut partition = OffsetDeleteRequestPartition::new();
        partition.set_partition_index(partition_index);
        topic.set_partitions(vec![partition]);
        topic
    }

    /// `build_version` yields an `OffsetDelete` request within the version
    /// range and carries the request data through unchanged.
    #[test]
    fn build_version_carries_data() {
        let mut data = OffsetDeleteRequestData::new();
        data.set_group_id("g".to_string());
        data.set_topics(vec![topic("t", 0)]);
        let mut builder = OffsetDeleteRequestBuilder::new(data);
        let version = ApiKeys::OFFSET_DELETE.latest_version();
        match builder.build_version(version).unwrap() {
            ConcreteRequest::OffsetDelete(r) => {
                assert_eq!(r.version(), version);
                assert_eq!(r.data().group_id, "g");
                assert_eq!(r.data().topics.len(), 1);
            },
            other => panic!("expected OffsetDelete, got {}", other.api_key().name()),
        }
    }

    /// `build_version` rejects a version outside the supported range.
    #[test]
    fn build_version_out_of_range_returns_err() {
        let mut data = OffsetDeleteRequestData::new();
        data.set_group_id("g".to_string());
        let mut builder = OffsetDeleteRequestBuilder::new(data);
        let too_new = ApiKeys::OFFSET_DELETE.latest_version() + 1;
        assert!(builder.build_version(too_new).is_err());
    }

    /// Byte-level wire-encoding check for the v0 (non-flexible) request.
    /// Field-by-field (big-endian, non-flexible framing):
    ///   group_id "g": string len 1 -> 0x00 0x01, then 0x67
    ///   topics: array len 1 -> 0x00 0x00 0x00 0x01
    ///     name "t": string len 1 -> 0x00 0x01, then 0x74
    ///     partitions: array len 1 -> 0x00 0x00 0x00 0x01
    ///       partition_index 3 -> 0x00 0x00 0x00 0x03
    #[test]
    fn serialize_known_byte_vector_v0() {
        let mut data = OffsetDeleteRequestData::new();
        data.set_group_id("g".to_string());
        data.set_topics(vec![topic("t", 3)]);
        let mut builder = OffsetDeleteRequestBuilder::new(data);
        let mut req = builder.build_version(0).unwrap();
        let expected: &[u8] = &[
            0x00, 0x01, 0x67, // group_id "g"
            0x00, 0x00, 0x00, 0x01, // topics len 1
            0x00, 0x01, 0x74, // name "t"
            0x00, 0x00, 0x00, 0x01, // partitions len 1
            0x00, 0x00, 0x00, 0x03, // partition_index 3
        ];
        assert_eq!(req.serialize().unwrap().into_buffer().as_slice(), expected);
    }

    /// `get_error_response` sets a top-level error code and the throttle time.
    #[test]
    fn get_error_response_sets_top_level_error() {
        let mut data = OffsetDeleteRequestData::new();
        data.set_group_id("g".to_string());
        let request = OffsetDeleteRequest::new(data, ApiKeys::OFFSET_DELETE.latest_version());
        let response = request.get_error_response(42, &Errors::GroupAuthorizationFailed);
        match response {
            ConcreteResponse::OffsetDelete(r) => {
                assert_eq!(r.data().error_code, Errors::GroupAuthorizationFailed.code());
                assert_eq!(r.throttle_time_ms(), 42);
            },
            other => panic!("expected OffsetDelete response, got {}", other.api_key().name()),
        }
    }
}
