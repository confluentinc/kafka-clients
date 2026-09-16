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

//! `DescribeGroups` request handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.DescribeGroupsRequest`.

use std::io;

use crate::DescribeGroupsRequestData;
use crate::DescribeGroupsResponseData;
use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::describe_groups_response_data::DescribedGroup;

use super::ConcreteRequest;
use super::ConcreteResponse;
use super::DescribeGroupsResponse;
use super::RequestBuilder;

/// A `DescribeGroups` request.
///
/// Corresponds to `org.apache.kafka.common.requests.DescribeGroupsRequest`.
#[derive(Debug, Clone)]
pub struct DescribeGroupsRequest {
    data: DescribeGroupsRequestData,
    version: i16,
}

impl DescribeGroupsRequest {
    /// Creates a new `DescribeGroupsRequest` from data and version.
    pub fn new(data: DescribeGroupsRequestData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &DescribeGroupsRequestData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut DescribeGroupsRequestData {
        &mut self.data
    }

    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Returns the API key for this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::DESCRIBE_GROUPS
    }

    /// Builds the canonical error response for this request, matching Java's
    /// `DescribeGroupsRequest.getErrorResponse(throttleTimeMs, Throwable)`.
    pub fn get_error_response(&self, throttle_time_ms: i32, error: &Errors) -> ConcreteResponse {
        let mut data = DescribeGroupsResponseData::new();
        let groups: Vec<DescribedGroup> = self
            .data
            .groups
            .iter()
            .map(|group_id| DescribeGroupsResponse::group_error(group_id.clone(), *error))
            .collect();
        data.set_groups(groups);
        if self.version >= 1 {
            data.set_throttle_time_ms(throttle_time_ms);
        }
        ConcreteResponse::DescribeGroups(DescribeGroupsResponse::new(data))
    }

    /// Parses a `DescribeGroupsRequest` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = DescribeGroupsRequestData::read(readable, version)?;
        Ok(Self::new(data, version))
    }
}

impl std::fmt::Display for DescribeGroupsRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DescribeGroupsRequest(version={}, data={})", self.version, self.data)
    }
}

/// Builder for [`DescribeGroupsRequest`].
///
/// Corresponds to `DescribeGroupsRequest.Builder` in Java.
#[derive(Debug, Clone)]
pub struct DescribeGroupsRequestBuilder {
    data: DescribeGroupsRequestData,
    oldest_allowed_version: i16,
    latest_allowed_version: i16,
}

impl DescribeGroupsRequestBuilder {
    /// Creates a builder wrapping the given data with the full supported
    /// version range.
    pub fn new(data: DescribeGroupsRequestData) -> Self {
        Self {
            data,
            oldest_allowed_version: ApiKeys::DESCRIBE_GROUPS.oldest_version(),
            latest_allowed_version: ApiKeys::DESCRIBE_GROUPS.latest_version(),
        }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &DescribeGroupsRequestData {
        &self.data
    }
}

impl RequestBuilder for DescribeGroupsRequestBuilder {
    fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::DESCRIBE_GROUPS
    }

    fn oldest_allowed_version(&self) -> i16 {
        self.oldest_allowed_version
    }

    fn latest_allowed_version(&self) -> i16 {
        self.latest_allowed_version
    }

    fn build_version(&mut self, version: i16) -> io::Result<ConcreteRequest> {
        Ok(ConcreteRequest::DescribeGroups(DescribeGroupsRequest::new(
            self.data.clone(),
            version,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Byte-level wire-encoding check for a v0 (non-flexible) request with a
    /// single group. Field-by-field:
    ///   groups: int32 array len 1 -> 00 00 00 01
    ///     "g1": int16 string len 2 -> 00 02, then bytes 67 31
    ///   (include_authorized_operations is v3+, absent at v0)
    #[test]
    fn test_serialize_known_byte_vector_v0() {
        let mut data = DescribeGroupsRequestData::new();
        data.set_groups(vec!["g1".to_string()]);
        let mut builder = DescribeGroupsRequestBuilder::new(data);
        let mut req = builder.build_version(0).unwrap();
        let expected: &[u8] = &[0x00, 0x00, 0x00, 0x01, 0x00, 0x02, 0x67, 0x31];
        assert_eq!(req.serialize().unwrap().into_buffer().as_slice(), expected);
    }

    #[test]
    fn test_api_key() {
        let builder = DescribeGroupsRequestBuilder::new(DescribeGroupsRequestData::new());
        assert_eq!(builder.api_key(), &ApiKeys::DESCRIBE_GROUPS);
    }

    /// The error response carries one error group per requested group id.
    #[test]
    fn test_get_error_response_one_per_group() {
        let mut data = DescribeGroupsRequestData::new();
        data.set_groups(vec!["g1".to_string(), "g2".to_string()]);
        let request = DescribeGroupsRequest::new(data, 5);
        let resp = request.get_error_response(50, &Errors::GroupAuthorizationFailed);
        match resp {
            ConcreteResponse::DescribeGroups(r) => {
                assert_eq!(r.data().groups.len(), 2);
                for group in &r.data().groups {
                    assert_eq!(group.error_code, Errors::GroupAuthorizationFailed.code());
                }
                assert_eq!(r.data().throttle_time_ms, 50);
            },
            other => panic!("expected DescribeGroups response, got {other:?}"),
        }
    }
}
