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

//! `AlterShareGroupOffsets` request handling (KIP-932).
//!
//! Corresponds to `org.apache.kafka.common.requests.AlterShareGroupOffsetsRequest`.

use std::fmt;
use std::io;

use crate::AlterShareGroupOffsetsRequestData;
use crate::AlterShareGroupOffsetsResponseData;
use crate::common::protocol::{ApiKeys, Errors, Readable};

use super::AbstractRequest;
use super::AlterShareGroupOffsetsResponse;
use super::ConcreteResponse;
use super::RequestBuilder;

/// An `AlterShareGroupOffsets` request.
///
/// Corresponds to `org.apache.kafka.common.requests.AlterShareGroupOffsetsRequest`.
#[derive(Debug, Clone)]
#[doc(alias = "org.apache.kafka.common.requests.AlterShareGroupOffsetsRequest")]
pub struct AlterShareGroupOffsetsRequest {
    data: AlterShareGroupOffsetsRequestData,
    version: i16,
}

impl AlterShareGroupOffsetsRequest {
    /// Wraps the given data. Private in Java: requests come from the
    /// [`Builder`] or [`parse`](Self::parse).
    fn new(data: AlterShareGroupOffsetsRequestData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns a reference to the underlying data.
    #[doc(alias = "org.apache.kafka.common.requests.AlterShareGroupOffsetsRequest#data")]
    pub fn data(&self) -> &AlterShareGroupOffsetsRequestData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut AlterShareGroupOffsetsRequestData {
        &mut self.data
    }

    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Returns the API key for this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::ALTER_SHARE_GROUP_OFFSETS
    }

    /// Builds the error response for this request: the top-level error code
    /// and its default message.
    ///
    /// Translates both `getErrorResponse(int, Throwable)` (the caller supplies
    /// `Errors.forException(e)`) and `getErrorResponse(int, Errors)`, which the
    /// former delegates to.
    #[doc(alias = "org.apache.kafka.common.requests.AlterShareGroupOffsetsRequest#getErrorResponse")]
    pub fn get_error_response(&self, throttle_time_ms: i32, error: &Errors) -> ConcreteResponse {
        ConcreteResponse::AlterShareGroupOffsets(self.get_error_response_with_error_code_message(
            throttle_time_ms,
            error.code(),
            Some(error.message()),
        ))
    }

    /// Builds the error response for this request from a raw error code and
    /// message.
    #[doc(alias = "org.apache.kafka.common.requests.AlterShareGroupOffsetsRequest#getErrorResponse")]
    pub fn get_error_response_with_error_code_message(
        &self,
        throttle_time_ms: i32,
        error_code: i16,
        message: Option<&str>,
    ) -> AlterShareGroupOffsetsResponse {
        let mut data = AlterShareGroupOffsetsResponseData::new();
        data.set_throttle_time_ms(throttle_time_ms)
            .set_error_code(error_code)
            .set_error_message(message.map(str::to_string));
        AlterShareGroupOffsetsResponse::new(data)
    }

    /// Returns response data carrying `error`'s code and `error_message`, or
    /// `error`'s default message when `error_message` is `None`.
    ///
    /// Translates `getErrorResponseData(Errors)` and
    /// `getErrorResponseData(Errors, String)`, which differ only in the
    /// optional message (CLAUDE.md §2).
    #[doc(alias = "org.apache.kafka.common.requests.AlterShareGroupOffsetsRequest#getErrorResponseData")]
    pub fn get_error_response_data(error: Errors, error_message: Option<&str>) -> AlterShareGroupOffsetsResponseData {
        let mut data = AlterShareGroupOffsetsResponseData::new();
        data.set_error_code(error.code())
            .set_error_message(Some(error_message.unwrap_or(error.message()).to_string()));
        data
    }

    /// Parses an `AlterShareGroupOffsetsRequest` from a readable buffer at the
    /// given version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    #[doc(alias = "org.apache.kafka.common.requests.AlterShareGroupOffsetsRequest#parse")]
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = AlterShareGroupOffsetsRequestData::read(readable, version)?;
        Ok(Self::new(data, version))
    }
}

impl fmt::Display for AlterShareGroupOffsetsRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.data)
    }
}

/// Builder for [`AlterShareGroupOffsetsRequest`].
///
/// Corresponds to `AlterShareGroupOffsetsRequest.Builder` in Java.
#[derive(Debug, Clone)]
#[doc(alias = "org.apache.kafka.common.requests.AlterShareGroupOffsetsRequest$Builder")]
pub struct Builder {
    data: AlterShareGroupOffsetsRequestData,
}

impl Builder {
    /// Creates a builder wrapping the given data.
    #[doc(alias = "org.apache.kafka.common.requests.AlterShareGroupOffsetsRequest$Builder#Builder")]
    pub fn new(data: AlterShareGroupOffsetsRequestData) -> Self {
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
        &ApiKeys::ALTER_SHARE_GROUP_OFFSETS
    }

    fn oldest_allowed_version(&self) -> i16 {
        ApiKeys::ALTER_SHARE_GROUP_OFFSETS.oldest_version()
    }

    fn latest_allowed_version(&self) -> i16 {
        // Mirrors Java's `super(ApiKeys)` → `latestVersion(false)`
        // (producer-transactions.md §12).
        ApiKeys::ALTER_SHARE_GROUP_OFFSETS.latest_version_enable_unstable_last_version(false)
    }

    fn build_version(&mut self, version: i16) -> io::Result<AbstractRequest> {
        // Java's `Builder.build(short)` performs no validation for this request.
        Ok(AbstractRequest::AlterShareGroupOffsets(AlterShareGroupOffsetsRequest::new(
            self.data.clone(),
            version,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::alter_share_group_offsets_request_data::{
        AlterShareGroupOffsetsRequestPartition, AlterShareGroupOffsetsRequestTopic,
    };
    use crate::common::requests::request_response_test::{check_error_response, check_request};

    /// `RequestResponseTest.createAlterShareGroupOffsetsRequest(short)`.
    fn create_alter_share_group_offsets_request(version: i16) -> AlterShareGroupOffsetsRequest {
        let mut partition = AlterShareGroupOffsetsRequestPartition::new();
        partition.set_partition_index(0).set_start_offset(0);
        let mut topic = AlterShareGroupOffsetsRequestTopic::new();
        topic.set_topic_name("topic".to_string()).set_partitions(vec![partition]);
        let mut data = AlterShareGroupOffsetsRequestData::new();
        data.set_group_id("group".to_string()).set_topics(vec![topic]);
        match Builder::new(data).build_version(version).expect("build") {
            AbstractRequest::AlterShareGroupOffsets(request) => request,
            other => panic!("expected AlterShareGroupOffsets, got {other:?}"),
        }
    }

    /// `RequestResponseTest.testSerialization`'s `ALTER_SHARE_GROUP_OFFSETS`
    /// arm, at every version: `checkRequest` and `checkErrorResponse` (the
    /// `checkResponse` half lives in `alter_share_group_offsets_response.rs`).
    #[test]
    fn test_serialization_all_versions() {
        for version in ApiKeys::ALTER_SHARE_GROUP_OFFSETS.all_versions() {
            let request = AbstractRequest::AlterShareGroupOffsets(create_alter_share_group_offsets_request(version));
            check_request(&request);
            check_error_response(&request);
        }
    }

    #[test]
    fn test_builder_version_range() {
        let builder = Builder::new(AlterShareGroupOffsetsRequestData::new());
        assert_eq!(&ApiKeys::ALTER_SHARE_GROUP_OFFSETS, builder.api_key());
        assert_eq!(0, builder.oldest_allowed_version());
        assert_eq!(0, builder.latest_allowed_version());
    }

    /// `getErrorResponse(int, Errors)` is the code plus its default message.
    #[test]
    fn test_get_error_response() {
        let request = create_alter_share_group_offsets_request(0);
        match request.get_error_response(4, &Errors::GroupIdNotFound) {
            ConcreteResponse::AlterShareGroupOffsets(response) => {
                assert_eq!(4, response.throttle_time_ms());
                assert_eq!(Errors::GroupIdNotFound.code(), response.data().error_code());
                assert_eq!(Some("The group id does not exist."), response.data().error_message().as_deref());
                assert!(response.data().responses().is_empty());
            },
            other => panic!("expected AlterShareGroupOffsets, got {other:?}"),
        }
        let response = request.get_error_response_with_error_code_message(1, Errors::InvalidRequest.code(), None);
        assert_eq!(Errors::InvalidRequest.code(), response.data().error_code());
        assert_eq!(&None, response.data().error_message());
    }

    /// `getErrorResponseData` falls back to the error's default message.
    #[test]
    fn test_get_error_response_data() {
        let data = AlterShareGroupOffsetsRequest::get_error_response_data(Errors::GroupIdNotFound, None);
        assert_eq!(Errors::GroupIdNotFound.code(), data.error_code());
        assert_eq!(Some("The group id does not exist."), data.error_message().as_deref());
        let data = AlterShareGroupOffsetsRequest::get_error_response_data(Errors::GroupIdNotFound, Some("custom"));
        assert_eq!(Some("custom"), data.error_message().as_deref());
        assert_eq!(0, data.throttle_time_ms());
    }

    /// Byte-level v0 encoding, derived by hand from `AlterShareGroupOffsetsRequest.json`.
    #[test]
    fn test_v0_encoding() {
        let mut partition = AlterShareGroupOffsetsRequestPartition::new();
        partition.set_partition_index(1).set_start_offset(-1);
        let mut topic = AlterShareGroupOffsetsRequestTopic::new();
        topic.set_topic_name("t".to_string()).set_partitions(vec![partition]);
        let mut data = AlterShareGroupOffsetsRequestData::new();
        data.set_group_id("g".to_string()).set_topics(vec![topic]);
        let mut request = Builder::new(data).build_version(0).unwrap();

        let mut expected = vec![
            0x02, b'g', // GroupId
            0x02, // Topics: compact array of 1
            0x02, b't', // TopicName
            0x02, // Partitions: compact array of 1
        ];
        expected.extend_from_slice(&1i32.to_be_bytes()); // PartitionIndex
        expected.extend_from_slice(&(-1i64).to_be_bytes()); // StartOffset
        expected.push(0x00); // partition tagged fields
        expected.push(0x00); // topic tagged fields
        expected.push(0x00); // request tagged fields
        assert_eq!(expected, request.serialize().unwrap().into_buffer());
    }
}
