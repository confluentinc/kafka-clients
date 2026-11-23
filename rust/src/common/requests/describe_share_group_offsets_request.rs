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

//! `DescribeShareGroupOffsets` request handling (KIP-932).
//!
//! Corresponds to `org.apache.kafka.common.requests.DescribeShareGroupOffsetsRequest`.

use std::fmt;
use std::io;

use crate::DescribeShareGroupOffsetsRequestData;
use crate::common::Error;
use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::describe_share_group_offsets_request_data::DescribeShareGroupOffsetsRequestGroup;
use crate::describe_share_group_offsets_response_data::DescribeShareGroupOffsetsResponseGroup;

use super::AbstractRequest;
use super::ConcreteResponse;
use super::DescribeShareGroupOffsetsResponse;
use super::RequestBuilder;

/// A `DescribeShareGroupOffsets` request.
///
/// Corresponds to `org.apache.kafka.common.requests.DescribeShareGroupOffsetsRequest`.
#[derive(Debug, Clone)]
#[doc(alias = "org.apache.kafka.common.requests.DescribeShareGroupOffsetsRequest")]
pub struct DescribeShareGroupOffsetsRequest {
    data: DescribeShareGroupOffsetsRequestData,
    version: i16,
}

impl DescribeShareGroupOffsetsRequest {
    /// Creates a new `DescribeShareGroupOffsetsRequest` from data and version.
    #[doc(alias = "org.apache.kafka.common.requests.DescribeShareGroupOffsetsRequest#DescribeShareGroupOffsetsRequest")]
    pub fn new(data: DescribeShareGroupOffsetsRequestData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns a reference to the underlying data.
    #[doc(alias = "org.apache.kafka.common.requests.DescribeShareGroupOffsetsRequest#data")]
    pub fn data(&self) -> &DescribeShareGroupOffsetsRequestData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut DescribeShareGroupOffsetsRequestData {
        &mut self.data
    }

    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Returns the API key for this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::DESCRIBE_SHARE_GROUP_OFFSETS
    }

    /// Returns the ids of the requested groups, in request order.
    #[doc(alias = "org.apache.kafka.common.requests.DescribeShareGroupOffsetsRequest#groupIds")]
    pub fn group_ids(&self) -> Vec<String> {
        self.data.groups.iter().map(|group| group.group_id.clone()).collect()
    }

    /// Returns the requested groups.
    #[doc(alias = "org.apache.kafka.common.requests.DescribeShareGroupOffsetsRequest#groups")]
    pub fn groups(&self) -> &[DescribeShareGroupOffsetsRequestGroup] {
        &self.data.groups
    }

    /// Builds the error response for this request: `error` as the group-level
    /// error of every requested group, with no topics.
    ///
    /// Translates `DescribeShareGroupOffsetsRequest.getErrorResponse(int, Throwable)`.
    /// Java's `Throwable` is the crate's [`Error`] here, rather than the bare
    /// [`Errors`] code most wrappers take, because the response copies the
    /// throwable's message (except for an unknown server error).
    #[doc(alias = "org.apache.kafka.common.requests.DescribeShareGroupOffsetsRequest#getErrorResponse")]
    pub fn get_error_response(&self, throttle_time_ms: i32, error: &Error) -> ConcreteResponse {
        // Sets the error as the group-level error code for all groups, with empty
        // partitions data for all groups.
        ConcreteResponse::DescribeShareGroupOffsets(
            DescribeShareGroupOffsetsResponse::with_throttle_time_ms_group_ids_all_groups_error(
                throttle_time_ms,
                &self.group_ids(),
                error,
            ),
        )
    }

    /// Parses a `DescribeShareGroupOffsetsRequest` from a readable buffer at the
    /// given version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    #[doc(alias = "org.apache.kafka.common.requests.DescribeShareGroupOffsetsRequest#parse")]
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = DescribeShareGroupOffsetsRequestData::read(readable, version)?;
        Ok(Self::new(data, version))
    }

    /// Returns a response group for `group_id` carrying `error`'s code and its
    /// default message.
    #[doc(alias = "org.apache.kafka.common.requests.DescribeShareGroupOffsetsRequest#getErrorDescribedGroup")]
    pub fn get_error_described_group(group_id: &str, error: Errors) -> DescribeShareGroupOffsetsResponseGroup {
        let mut group = DescribeShareGroupOffsetsResponseGroup::new();
        group
            .set_group_id(group_id.to_string())
            .set_error_code(error.code())
            .set_error_message(Some(error.message().to_string()));
        group
    }
}

impl fmt::Display for DescribeShareGroupOffsetsRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.data)
    }
}

/// Builder for [`DescribeShareGroupOffsetsRequest`].
///
/// Corresponds to `DescribeShareGroupOffsetsRequest.Builder` in Java.
#[derive(Debug, Clone)]
#[doc(alias = "org.apache.kafka.common.requests.DescribeShareGroupOffsetsRequest$Builder")]
pub struct Builder {
    data: DescribeShareGroupOffsetsRequestData,
}

impl Builder {
    /// Creates a builder wrapping the given data.
    #[doc(alias = "org.apache.kafka.common.requests.DescribeShareGroupOffsetsRequest$Builder#Builder")]
    pub fn new(data: DescribeShareGroupOffsetsRequestData) -> Self {
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
        &ApiKeys::DESCRIBE_SHARE_GROUP_OFFSETS
    }

    fn oldest_allowed_version(&self) -> i16 {
        ApiKeys::DESCRIBE_SHARE_GROUP_OFFSETS.oldest_version()
    }

    fn latest_allowed_version(&self) -> i16 {
        // Mirrors Java's `super(ApiKeys)` → `latestVersion(false)`
        // (producer-transactions.md §12).
        ApiKeys::DESCRIBE_SHARE_GROUP_OFFSETS.latest_version_enable_unstable_last_version(false)
    }

    fn build_version(&mut self, version: i16) -> io::Result<AbstractRequest> {
        // Java's `Builder.build(short)` performs no validation for this request.
        Ok(AbstractRequest::DescribeShareGroupOffsets(
            DescribeShareGroupOffsetsRequest::new(self.data.clone(), version),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::requests::request_response_test::{check_error_response, check_request, check_response};
    use crate::describe_share_group_offsets_request_data::DescribeShareGroupOffsetsRequestTopic;

    fn group(
        group_id: &str,
        topics: Option<Vec<DescribeShareGroupOffsetsRequestTopic>>,
    ) -> DescribeShareGroupOffsetsRequestGroup {
        let mut group = DescribeShareGroupOffsetsRequestGroup::new();
        group.set_group_id(group_id.to_string()).set_topics(topics);
        group
    }

    /// `RequestResponseTest.createDescribeShareGroupOffsetsRequest(short)`.
    fn create_describe_share_group_offsets_request(version: i16) -> DescribeShareGroupOffsetsRequest {
        let mut topic = DescribeShareGroupOffsetsRequestTopic::new();
        topic.set_topic_name("topic-1".to_string()).set_partitions(vec![0]);
        let mut data = DescribeShareGroupOffsetsRequestData::new();
        data.set_groups(vec![group("group", Some(vec![topic]))]);
        match Builder::new(data).build_version(version).expect("build") {
            AbstractRequest::DescribeShareGroupOffsets(request) => request,
            other => panic!("expected DescribeShareGroupOffsets, got {other:?}"),
        }
    }

    /// `RequestResponseTest.testSerialization`'s `DESCRIBE_SHARE_GROUP_OFFSETS`
    /// arm, at every version: `checkRequest` and `checkErrorResponse` (the
    /// `checkResponse` half lives in `describe_share_group_offsets_response.rs`).
    #[test]
    fn test_serialization_all_versions() {
        for version in ApiKeys::DESCRIBE_SHARE_GROUP_OFFSETS.all_versions() {
            let request =
                AbstractRequest::DescribeShareGroupOffsets(create_describe_share_group_offsets_request(version));
            check_request(&request);
            check_error_response(&request);
        }
    }

    /// `checkErrorResponse` with Java's actual throwable,
    /// `new UnknownServerException("secret")`: the response carries the code
    /// and the error's *default* message, never `"secret"`.
    #[test]
    fn test_error_response_does_not_leak_unknown_server_error_message() {
        let secret = Error::with_message(Errors::UnknownServerError, "secret");
        for version in ApiKeys::DESCRIBE_SHARE_GROUP_OFFSETS.all_versions() {
            let request = create_describe_share_group_offsets_request(version);
            let response = request.get_error_response(0, &secret);
            check_response(&response, version);
            assert!(!response.to_string().contains("secret"), "v{version}: {response}");
            match &response {
                ConcreteResponse::DescribeShareGroupOffsets(r) => {
                    let group = &r.data().groups()[0];
                    assert_eq!(Errors::UnknownServerError.code(), group.error_code());
                    assert_eq!(Some(Errors::UnknownServerError.message()), group.error_message().as_deref());
                },
                other => panic!("expected DescribeShareGroupOffsets, got {other:?}"),
            }
        }
    }

    /// For any other error, the response copies the error's message.
    #[test]
    fn test_error_response_copies_the_message_of_other_errors() {
        let mut data = DescribeShareGroupOffsetsRequestData::new();
        data.set_groups(vec![group("g1", None), group("g2", None)]);
        let request = DescribeShareGroupOffsetsRequest::new(data, 1);
        let error = Error::with_message(Errors::GroupAuthorizationFailed, "denied");
        match request.get_error_response(5, &error) {
            ConcreteResponse::DescribeShareGroupOffsets(response) => {
                assert_eq!(5, response.throttle_time_ms());
                let groups = response.data().groups();
                assert_eq!(
                    vec!["g1", "g2"],
                    groups.iter().map(|g| g.group_id().as_str()).collect::<Vec<_>>()
                );
                for group in groups {
                    assert_eq!(Errors::GroupAuthorizationFailed.code(), group.error_code());
                    assert_eq!(Some("denied"), group.error_message().as_deref());
                    assert!(group.topics().is_empty());
                }
                assert!(response.has_group_error("g1"));
                assert_eq!("denied", response.group_error("g2").expect("group error").message());
            },
            other => panic!("expected DescribeShareGroupOffsets, got {other:?}"),
        }
    }

    #[test]
    fn test_group_ids_and_groups() {
        let mut data = DescribeShareGroupOffsetsRequestData::new();
        data.set_groups(vec![group("b", None), group("a", None)]);
        let request = DescribeShareGroupOffsetsRequest::new(data, 0);
        assert_eq!(vec!["b".to_string(), "a".to_string()], request.group_ids());
        assert_eq!(2, request.groups().len());
        assert_eq!(&ApiKeys::DESCRIBE_SHARE_GROUP_OFFSETS, request.api_key());
    }

    #[test]
    fn test_get_error_described_group() {
        let group = DescribeShareGroupOffsetsRequest::get_error_described_group("g", Errors::GroupIdNotFound);
        assert_eq!("g", group.group_id());
        assert_eq!(Errors::GroupIdNotFound.code(), group.error_code());
        assert_eq!(Some("The group id does not exist."), group.error_message().as_deref());
    }

    #[test]
    fn test_builder_version_range() {
        let builder = Builder::new(DescribeShareGroupOffsetsRequestData::new());
        assert_eq!(&ApiKeys::DESCRIBE_SHARE_GROUP_OFFSETS, builder.api_key());
        assert_eq!(0, builder.oldest_allowed_version());
        assert_eq!(1, builder.latest_allowed_version());
    }

    /// Byte-level encoding (v0 and v1 share one request schema), derived by hand
    /// from `DescribeShareGroupOffsetsRequest.json`: the nullable `Topics`
    /// array is `count + 1`, or a lone `0` for null (all topics).
    #[test]
    fn test_encoding() {
        let mut topic = DescribeShareGroupOffsetsRequestTopic::new();
        topic.set_topic_name("t".to_string()).set_partitions(vec![0, 2]);
        let mut data = DescribeShareGroupOffsetsRequestData::new();
        data.set_groups(vec![group("g", Some(vec![topic])), group("h", None)]);
        let mut expected = vec![
            0x03, // Groups: compact array of 2
            0x02, b'g', // GroupId
            0x02, // Topics: compact array of 1
            0x02, b't', // TopicName
            0x03, // Partitions: compact array of 2
        ];
        expected.extend_from_slice(&0i32.to_be_bytes());
        expected.extend_from_slice(&2i32.to_be_bytes());
        expected.push(0x00); // topic tagged fields
        expected.push(0x00); // group tagged fields
        expected.extend_from_slice(&[0x02, b'h']); // GroupId
        expected.push(0x00); // Topics: null
        expected.push(0x00); // group tagged fields
        expected.push(0x00); // request tagged fields
        for version in ApiKeys::DESCRIBE_SHARE_GROUP_OFFSETS.all_versions() {
            let mut request = Builder::new(data.clone()).build_version(version).unwrap();
            assert_eq!(expected, request.serialize().unwrap().into_buffer(), "v{version}");
        }
    }
}
