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

//! `DeleteGroups` request handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.DeleteGroupsRequest`.
//!
//! Wraps the auto-generated [`DeleteGroupsRequestData`] and exposes a
//! [`DeleteGroupsRequestBuilder`] used by the admin client's
//! `deleteConsumerGroups` path.

use std::io;

use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::delete_groups_request_data::DeleteGroupsRequestData;
use crate::delete_groups_response_data::{DeletableGroupResult, DeleteGroupsResponseData};

use super::ConcreteResponse;
use super::DeleteGroupsResponse;
use super::abstract_request::{ConcreteRequest, RequestBuilder};

/// A `DeleteGroups` request.
///
/// Corresponds to `org.apache.kafka.common.requests.DeleteGroupsRequest`.
#[derive(Debug, Clone)]
pub struct DeleteGroupsRequest {
    data: DeleteGroupsRequestData,
    version: i16,
}

impl DeleteGroupsRequest {
    /// Creates a new `DeleteGroupsRequest` from data and version.
    ///
    /// Mirrors Java's constructor `DeleteGroupsRequest(DeleteGroupsRequestData, short)`.
    pub fn new(data: DeleteGroupsRequestData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &DeleteGroupsRequestData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut DeleteGroupsRequestData {
        &mut self.data
    }

    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Returns the API key for this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::DELETE_GROUPS
    }

    /// Builds the canonical error response for this request, matching Java's
    /// `DeleteGroupsRequest.getErrorResponse(int, Throwable)`: one
    /// `DeletableGroupResult` per requested group all carrying the same error
    /// code.
    pub fn get_error_response(&self, throttle_time_ms: i32, error: &Errors) -> ConcreteResponse {
        let results: Vec<DeletableGroupResult> = self
            .data
            .groups_names
            .iter()
            .map(|group_id| {
                let mut result = DeletableGroupResult::new();
                result.set_group_id(group_id.clone()).set_error_code(error.code());
                result
            })
            .collect();
        let mut data = DeleteGroupsResponseData::new();
        data.set_throttle_time_ms(throttle_time_ms);
        data.set_results(results);
        ConcreteResponse::DeleteGroups(DeleteGroupsResponse::new(data))
    }

    /// Parses a `DeleteGroupsRequest` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = DeleteGroupsRequestData::read(readable, version)?;
        Ok(Self::new(data, version))
    }
}

impl std::fmt::Display for DeleteGroupsRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DeleteGroupsRequest(version={}, data={})", self.version, self.data)
    }
}

/// Builder for [`DeleteGroupsRequest`].
///
/// Corresponds to `DeleteGroupsRequest.Builder` in Java.
#[derive(Debug, Clone)]
pub struct DeleteGroupsRequestBuilder {
    data: DeleteGroupsRequestData,
}

impl DeleteGroupsRequestBuilder {
    /// Creates a builder over the given request data.
    ///
    /// Mirrors Java's `DeleteGroupsRequest.Builder(DeleteGroupsRequestData)`.
    pub fn new(data: DeleteGroupsRequestData) -> Self {
        Self { data }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &DeleteGroupsRequestData {
        &self.data
    }
}

impl RequestBuilder for DeleteGroupsRequestBuilder {
    fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::DELETE_GROUPS
    }

    fn oldest_allowed_version(&self) -> i16 {
        ApiKeys::DELETE_GROUPS.oldest_version()
    }

    fn latest_allowed_version(&self) -> i16 {
        ApiKeys::DELETE_GROUPS.latest_version()
    }

    fn build_version(&mut self, version: i16) -> io::Result<ConcreteRequest> {
        if version < self.oldest_allowed_version() || version > self.latest_allowed_version() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!(
                    "Cannot build DeleteGroups request with version {version} (allowed range: {}..={})",
                    self.oldest_allowed_version(),
                    self.latest_allowed_version(),
                ),
            ));
        }
        Ok(ConcreteRequest::DeleteGroups(DeleteGroupsRequest::new(
            self.data.clone(),
            version,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `build_version` yields a `DeleteGroups` request within the version range
    /// and carries the request data through unchanged.
    #[test]
    fn build_version_carries_data() {
        let mut data = DeleteGroupsRequestData::new();
        data.set_groups_names(vec!["g".to_string()]);
        let mut builder = DeleteGroupsRequestBuilder::new(data);
        let version = ApiKeys::DELETE_GROUPS.latest_version();
        match builder.build_version(version).unwrap() {
            ConcreteRequest::DeleteGroups(r) => {
                assert_eq!(r.version(), version);
                assert_eq!(r.data().groups_names, vec!["g".to_string()]);
            },
            other => panic!("expected DeleteGroups, got {}", other.api_key().name()),
        }
    }

    /// `build_version` rejects a version outside the supported range.
    #[test]
    fn build_version_out_of_range_returns_err() {
        let mut data = DeleteGroupsRequestData::new();
        data.set_groups_names(vec!["g".to_string()]);
        let mut builder = DeleteGroupsRequestBuilder::new(data);
        let too_new = ApiKeys::DELETE_GROUPS.latest_version() + 1;
        assert!(builder.build_version(too_new).is_err());
    }

    /// Byte-level wire-encoding check for the v0 (non-flexible) request.
    /// Field-by-field (big-endian, non-flexible framing):
    ///   groups_names: array len 1 -> 0x00 0x00 0x00 0x01
    ///     "g": string len 1 -> 0x00 0x01, then 0x67
    #[test]
    fn serialize_known_byte_vector_v0() {
        let mut data = DeleteGroupsRequestData::new();
        data.set_groups_names(vec!["g".to_string()]);
        let mut builder = DeleteGroupsRequestBuilder::new(data);
        let mut req = builder.build_version(0).unwrap();
        let expected: &[u8] = &[
            0x00, 0x00, 0x00, 0x01, // groups_names len 1
            0x00, 0x01, 0x67, // "g"
        ];
        assert_eq!(req.serialize().unwrap().into_buffer().as_slice(), expected);
    }

    /// Byte-level wire-encoding check for the v2 (flexible) request.
    /// Flexible framing uses unsigned-varint length prefixes (n+1):
    ///   groups_names: compact array len 1 -> 0x02
    ///     "g": compact string len 1 -> 0x02, then 0x67
    ///   top-level tagged fields: 0x00
    #[test]
    fn serialize_known_byte_vector_v2_flexible() {
        let mut data = DeleteGroupsRequestData::new();
        data.set_groups_names(vec!["g".to_string()]);
        let mut builder = DeleteGroupsRequestBuilder::new(data);
        let mut req = builder.build_version(2).unwrap();
        let expected: &[u8] = &[
            0x02, // groups_names compact array len 1 (=n+1)
            0x02, 0x67, // "g" compact string len 1 (=n+1), then 'g'
            0x00, // top-level tagged fields
        ];
        assert_eq!(req.serialize().unwrap().into_buffer().as_slice(), expected);
    }

    /// `get_error_response` builds one result per group carrying the error code.
    #[test]
    fn get_error_response_sets_per_group_error() {
        let mut data = DeleteGroupsRequestData::new();
        data.set_groups_names(vec!["g1".to_string(), "g2".to_string()]);
        let request = DeleteGroupsRequest::new(data, ApiKeys::DELETE_GROUPS.latest_version());
        let response = request.get_error_response(42, &Errors::GroupAuthorizationFailed);
        match response {
            ConcreteResponse::DeleteGroups(r) => {
                assert_eq!(r.data().throttle_time_ms, 42);
                assert_eq!(r.data().results.len(), 2);
                for result in &r.data().results {
                    assert_eq!(result.error_code, Errors::GroupAuthorizationFailed.code());
                }
            },
            other => panic!("expected DeleteGroups response, got {}", other.api_key().name()),
        }
    }
}
