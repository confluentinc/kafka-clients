// Copyright 2026 Confluent Inc.
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

//! `ShareGroupDescribe` request handling (KIP-932).
//!
//! Corresponds to `org.apache.kafka.common.requests.ShareGroupDescribeRequest`.

use std::fmt;
use std::io;

use crate::ShareGroupDescribeRequestData;
use crate::ShareGroupDescribeResponseData;
use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::share_group_describe_response_data::DescribedGroup;

use super::AbstractRequest;
use super::ConcreteResponse;
use super::RequestBuilder;
use super::ShareGroupDescribeResponse;

/// A `ShareGroupDescribe` request.
///
/// Corresponds to `org.apache.kafka.common.requests.ShareGroupDescribeRequest`.
#[derive(Debug, Clone)]
#[doc(alias = "org.apache.kafka.common.requests.ShareGroupDescribeRequest")]
pub struct ShareGroupDescribeRequest {
    data: ShareGroupDescribeRequestData,
    version: i16,
}

impl ShareGroupDescribeRequest {
    /// Creates a new `ShareGroupDescribeRequest` from data and version.
    #[doc(alias = "org.apache.kafka.common.requests.ShareGroupDescribeRequest#ShareGroupDescribeRequest")]
    pub fn new(data: ShareGroupDescribeRequestData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns a reference to the underlying data.
    #[doc(alias = "org.apache.kafka.common.requests.ShareGroupDescribeRequest#data")]
    pub fn data(&self) -> &ShareGroupDescribeRequestData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut ShareGroupDescribeRequestData {
        &mut self.data
    }

    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Returns the API key for this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::SHARE_GROUP_DESCRIBE
    }

    /// Builds the error response for this request: one described group per
    /// requested group id, each carrying the error code (and no message).
    ///
    /// Translates `ShareGroupDescribeRequest.getErrorResponse(int, Throwable)`;
    /// the caller supplies `Errors.forException(e)`.
    #[doc(alias = "org.apache.kafka.common.requests.ShareGroupDescribeRequest#getErrorResponse")]
    pub fn get_error_response(&self, throttle_time_ms: i32, error: &Errors) -> ConcreteResponse {
        let error_code = error.code();
        let groups = self
            .data
            .group_ids()
            .iter()
            .map(|group_id| {
                let mut group = DescribedGroup::new();
                group.set_group_id(group_id.clone()).set_error_code(error_code);
                group
            })
            .collect();
        let mut data = ShareGroupDescribeResponseData::new();
        data.set_throttle_time_ms(throttle_time_ms).set_groups(groups);
        ConcreteResponse::ShareGroupDescribe(ShareGroupDescribeResponse::new(data))
    }

    /// Parses a `ShareGroupDescribeRequest` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    #[doc(alias = "org.apache.kafka.common.requests.ShareGroupDescribeRequest#parse")]
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = ShareGroupDescribeRequestData::read(readable, version)?;
        Ok(Self::new(data, version))
    }

    /// Returns one described group per group id, each carrying `error`'s code
    /// and its default message.
    #[doc(alias = "org.apache.kafka.common.requests.ShareGroupDescribeRequest#getErrorDescribedGroupList")]
    pub fn get_error_described_group_list(group_ids: &[String], error: Errors) -> Vec<DescribedGroup> {
        group_ids
            .iter()
            .map(|group_id| {
                let mut group = DescribedGroup::new();
                group
                    .set_group_id(group_id.clone())
                    .set_error_code(error.code())
                    .set_error_message(Some(error.message().to_string()));
                group
            })
            .collect()
    }
}

impl fmt::Display for ShareGroupDescribeRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.data)
    }
}

/// Builder for [`ShareGroupDescribeRequest`].
///
/// Corresponds to `ShareGroupDescribeRequest.Builder` in Java.
#[derive(Debug, Clone)]
#[doc(alias = "org.apache.kafka.common.requests.ShareGroupDescribeRequest$Builder")]
pub struct Builder {
    data: ShareGroupDescribeRequestData,
}

impl Builder {
    /// Creates a builder wrapping the given data.
    #[doc(alias = "org.apache.kafka.common.requests.ShareGroupDescribeRequest$Builder#Builder")]
    pub fn new(data: ShareGroupDescribeRequestData) -> Self {
        Self { data }
    }
}

impl fmt::Display for Builder {
    /// Matches Java's `Builder.toString()`: the data.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.data)
    }
}

impl RequestBuilder for Builder {
    fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::SHARE_GROUP_DESCRIBE
    }

    fn oldest_allowed_version(&self) -> i16 {
        ApiKeys::SHARE_GROUP_DESCRIBE.oldest_version()
    }

    fn latest_allowed_version(&self) -> i16 {
        // Mirrors Java's `super(ApiKeys)` → `latestVersion(false)`
        // (producer-transactions.md §12).
        ApiKeys::SHARE_GROUP_DESCRIBE.latest_version_enable_unstable_last_version(false)
    }

    fn build_version(&mut self, version: i16) -> io::Result<AbstractRequest> {
        // Java's `Builder.build(short)` performs no validation for this request.
        Ok(AbstractRequest::ShareGroupDescribe(ShareGroupDescribeRequest::new(
            self.data.clone(),
            version,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::requests::request_response_test::{check_error_response, check_request};

    /// `RequestResponseTest.createShareGroupDescribeRequest(short)`.
    fn create_share_group_describe_request(version: i16) -> ShareGroupDescribeRequest {
        let mut data = ShareGroupDescribeRequestData::new();
        data.set_group_ids(vec!["group".to_string()])
            .set_include_authorized_operations(false);
        match Builder::new(data).build_version(version).expect("build") {
            AbstractRequest::ShareGroupDescribe(request) => request,
            other => panic!("expected ShareGroupDescribe, got {other:?}"),
        }
    }

    /// `RequestResponseTest.testSerialization`'s `SHARE_GROUP_DESCRIBE` arm, at
    /// every version: `checkRequest` and `checkErrorResponse` (the
    /// `checkResponse` half lives in `share_group_describe_response.rs`).
    #[test]
    fn test_serialization_all_versions() {
        for version in ApiKeys::SHARE_GROUP_DESCRIBE.all_versions() {
            let request = AbstractRequest::ShareGroupDescribe(create_share_group_describe_request(version));
            check_request(&request);
            check_error_response(&request);
        }
    }

    #[test]
    fn test_builder_version_range() {
        let builder = Builder::new(ShareGroupDescribeRequestData::new());
        assert_eq!(&ApiKeys::SHARE_GROUP_DESCRIBE, builder.api_key());
        assert_eq!(1, builder.oldest_allowed_version());
        assert_eq!(1, builder.latest_allowed_version());
    }

    /// The error response has one group per requested id, with the code and no
    /// message (Java sets only `groupId` and `errorCode`).
    #[test]
    fn test_get_error_response() {
        let mut data = ShareGroupDescribeRequestData::new();
        data.set_group_ids(vec!["b".to_string(), "a".to_string()]);
        let request = ShareGroupDescribeRequest::new(data, 1);
        match request.get_error_response(9, &Errors::GroupAuthorizationFailed) {
            ConcreteResponse::ShareGroupDescribe(response) => {
                assert_eq!(9, response.throttle_time_ms());
                let groups = response.data().groups();
                assert_eq!(vec!["b", "a"], groups.iter().map(|g| g.group_id()).collect::<Vec<_>>());
                for group in groups {
                    assert_eq!(Errors::GroupAuthorizationFailed.code(), group.error_code());
                    assert_eq!(None, group.error_message().as_deref());
                }
            },
            other => panic!("expected ShareGroupDescribe, got {other:?}"),
        }
    }

    #[test]
    fn test_get_error_described_group_list() {
        let groups = ShareGroupDescribeRequest::get_error_described_group_list(
            &["g1".to_string(), "g2".to_string()],
            Errors::GroupIdNotFound,
        );
        assert_eq!(2, groups.len());
        assert_eq!("g1", groups[0].group_id());
        assert_eq!("g2", groups[1].group_id());
        for group in &groups {
            assert_eq!(Errors::GroupIdNotFound.code(), group.error_code());
            assert_eq!(Some("The group id does not exist."), group.error_message().as_deref());
        }
    }

    /// Byte-level v1 encoding, derived by hand from `ShareGroupDescribeRequest.json`.
    #[test]
    fn test_v1_encoding() {
        let mut data = ShareGroupDescribeRequestData::new();
        data.set_group_ids(vec!["g1".to_string(), "g2".to_string()])
            .set_include_authorized_operations(true);
        let mut request = Builder::new(data).build_version(1).unwrap();
        let expected = vec![
            0x03, // GroupIds: compact array of 2
            0x03, b'g', b'1', // "g1"
            0x03, b'g', b'2', // "g2"
            0x01, // IncludeAuthorizedOperations: true
            0x00, // tagged fields
        ];
        assert_eq!(expected, request.serialize().unwrap().into_buffer());
    }
}
