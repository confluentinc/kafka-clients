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

//! `ConsumerGroupDescribe` request handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.ConsumerGroupDescribeRequest`.

use std::io;

use crate::ConsumerGroupDescribeRequestData;
use crate::ConsumerGroupDescribeResponseData;
use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::consumer_group_describe_response_data::DescribedGroup;

use super::ConcreteRequest;
use super::ConcreteResponse;
use super::ConsumerGroupDescribeResponse;
use super::RequestBuilder;

/// A `ConsumerGroupDescribe` request.
///
/// Corresponds to `org.apache.kafka.common.requests.ConsumerGroupDescribeRequest`.
#[derive(Debug, Clone)]
pub struct ConsumerGroupDescribeRequest {
    data: ConsumerGroupDescribeRequestData,
    version: i16,
}

impl ConsumerGroupDescribeRequest {
    /// Creates a new `ConsumerGroupDescribeRequest` from data and version.
    pub fn new(data: ConsumerGroupDescribeRequestData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &ConsumerGroupDescribeRequestData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut ConsumerGroupDescribeRequestData {
        &mut self.data
    }

    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Returns the API key for this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::CONSUMER_GROUP_DESCRIBE
    }

    /// Builds the canonical error response for this request, matching Java's
    /// `ConsumerGroupDescribeRequest.getErrorResponse(throttleTimeMs, Throwable)`.
    pub fn get_error_response(&self, throttle_time_ms: i32, error: &Errors) -> ConcreteResponse {
        let mut data = ConsumerGroupDescribeResponseData::new();
        data.set_throttle_time_ms(throttle_time_ms);
        let groups: Vec<DescribedGroup> = self
            .data
            .group_ids
            .iter()
            .map(|group_id| {
                let mut group = DescribedGroup::new();
                group.set_group_id(group_id.clone()).set_error_code(error.code());
                group
            })
            .collect();
        data.set_groups(groups);
        ConcreteResponse::ConsumerGroupDescribe(ConsumerGroupDescribeResponse::new(data))
    }

    /// Parses a `ConsumerGroupDescribeRequest` from a readable buffer at the
    /// given version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = ConsumerGroupDescribeRequestData::read(readable, version)?;
        Ok(Self::new(data, version))
    }
}

impl std::fmt::Display for ConsumerGroupDescribeRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ConsumerGroupDescribeRequest(version={}, data={})", self.version, self.data)
    }
}

/// Builder for [`ConsumerGroupDescribeRequest`].
///
/// Corresponds to `ConsumerGroupDescribeRequest.Builder` in Java.
#[derive(Debug, Clone)]
pub struct ConsumerGroupDescribeRequestBuilder {
    data: ConsumerGroupDescribeRequestData,
    oldest_allowed_version: i16,
    latest_allowed_version: i16,
}

impl ConsumerGroupDescribeRequestBuilder {
    /// Creates a builder wrapping the given data with the full supported
    /// version range.
    pub fn new(data: ConsumerGroupDescribeRequestData) -> Self {
        Self {
            data,
            oldest_allowed_version: ApiKeys::CONSUMER_GROUP_DESCRIBE.oldest_version(),
            latest_allowed_version: ApiKeys::CONSUMER_GROUP_DESCRIBE.latest_version(),
        }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &ConsumerGroupDescribeRequestData {
        &self.data
    }
}

impl RequestBuilder for ConsumerGroupDescribeRequestBuilder {
    fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::CONSUMER_GROUP_DESCRIBE
    }

    fn oldest_allowed_version(&self) -> i16 {
        self.oldest_allowed_version
    }

    fn latest_allowed_version(&self) -> i16 {
        self.latest_allowed_version
    }

    fn build_version(&mut self, version: i16) -> io::Result<ConcreteRequest> {
        Ok(ConcreteRequest::ConsumerGroupDescribe(ConsumerGroupDescribeRequest::new(
            self.data.clone(),
            version,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Byte-level wire-encoding check for a v0 (flexible) request with a single
    /// group id. Field-by-field:
    ///   group_ids: compact array len 1 -> 0x02 (N+1)
    ///     "g1": compact string len 2 -> 0x03 (N+1), then bytes 67 31
    ///   include_authorized_operations: bool false -> 0x00
    ///   _tagged_fields: 0x00
    #[test]
    fn test_serialize_known_byte_vector_v0() {
        let mut data = ConsumerGroupDescribeRequestData::new();
        data.set_group_ids(vec!["g1".to_string()]);
        let mut builder = ConsumerGroupDescribeRequestBuilder::new(data);
        let mut req = builder.build_version(0).unwrap();
        let expected: &[u8] = &[0x02, 0x03, 0x67, 0x31, 0x00, 0x00];
        assert_eq!(req.serialize().unwrap().into_buffer().as_slice(), expected);
    }

    #[test]
    fn test_api_key() {
        let builder = ConsumerGroupDescribeRequestBuilder::new(ConsumerGroupDescribeRequestData::new());
        assert_eq!(builder.api_key(), &ApiKeys::CONSUMER_GROUP_DESCRIBE);
    }

    /// The error response carries one error group per requested group id and
    /// always sets the throttle time (Java has no version gate here).
    #[test]
    fn test_get_error_response_one_per_group() {
        let mut data = ConsumerGroupDescribeRequestData::new();
        data.set_group_ids(vec!["g1".to_string(), "g2".to_string()]);
        let request = ConsumerGroupDescribeRequest::new(data, 0);
        let resp = request.get_error_response(75, &Errors::GroupIdNotFound);
        match resp {
            ConcreteResponse::ConsumerGroupDescribe(r) => {
                assert_eq!(r.data().groups.len(), 2);
                for group in &r.data().groups {
                    assert_eq!(group.error_code, Errors::GroupIdNotFound.code());
                }
                assert_eq!(r.data().throttle_time_ms, 75);
            },
            other => panic!("expected ConsumerGroupDescribe response, got {other:?}"),
        }
    }
}
