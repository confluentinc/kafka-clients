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

//! `DeleteShareGroupOffsets` request handling (KIP-932).
//!
//! Corresponds to `org.apache.kafka.common.requests.DeleteShareGroupOffsetsRequest`.

use std::fmt;
use std::io;

use crate::DeleteShareGroupOffsetsRequestData;
use crate::DeleteShareGroupOffsetsResponseData;
use crate::common::protocol::{ApiKeys, Errors, Readable};

use super::AbstractRequest;
use super::ConcreteResponse;
use super::DeleteShareGroupOffsetsResponse;
use super::RequestBuilder;

/// A `DeleteShareGroupOffsets` request.
///
/// Corresponds to `org.apache.kafka.common.requests.DeleteShareGroupOffsetsRequest`.
#[derive(Debug, Clone)]
#[doc(alias = "org.apache.kafka.common.requests.DeleteShareGroupOffsetsRequest")]
pub struct DeleteShareGroupOffsetsRequest {
    data: DeleteShareGroupOffsetsRequestData,
    version: i16,
}

impl DeleteShareGroupOffsetsRequest {
    /// Creates a new `DeleteShareGroupOffsetsRequest` from data and version.
    #[doc(alias = "org.apache.kafka.common.requests.DeleteShareGroupOffsetsRequest#DeleteShareGroupOffsetsRequest")]
    pub fn new(data: DeleteShareGroupOffsetsRequestData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns a reference to the underlying data.
    #[doc(alias = "org.apache.kafka.common.requests.DeleteShareGroupOffsetsRequest#data")]
    pub fn data(&self) -> &DeleteShareGroupOffsetsRequestData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut DeleteShareGroupOffsetsRequestData {
        &mut self.data
    }

    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Returns the API key for this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::DELETE_SHARE_GROUP_OFFSETS
    }

    /// Builds the error response for this request: the top-level error code
    /// and its default message.
    ///
    /// Translates both `getErrorResponse(int, Throwable)` (the caller supplies
    /// `Errors.forException(e)`) and the package-private
    /// `getErrorResponse(int, Errors)`, which the former delegates to.
    #[doc(alias = "org.apache.kafka.common.requests.DeleteShareGroupOffsetsRequest#getErrorResponse")]
    pub fn get_error_response(&self, throttle_time_ms: i32, error: &Errors) -> ConcreteResponse {
        ConcreteResponse::DeleteShareGroupOffsets(self.get_error_response_with_error_code_error_message(
            throttle_time_ms,
            error.code(),
            Some(error.message()),
        ))
    }

    /// Builds the error response for this request from a raw error code and
    /// message.
    #[doc(alias = "org.apache.kafka.common.requests.DeleteShareGroupOffsetsRequest#getErrorResponse")]
    pub fn get_error_response_with_error_code_error_message(
        &self,
        throttle_time_ms: i32,
        error_code: i16,
        error_message: Option<&str>,
    ) -> DeleteShareGroupOffsetsResponse {
        let mut data = DeleteShareGroupOffsetsResponseData::new();
        data.set_throttle_time_ms(throttle_time_ms)
            .set_error_message(error_message.map(str::to_string))
            .set_error_code(error_code);
        DeleteShareGroupOffsetsResponse::new(data)
    }

    /// Parses a `DeleteShareGroupOffsetsRequest` from a readable buffer at the
    /// given version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    #[doc(alias = "org.apache.kafka.common.requests.DeleteShareGroupOffsetsRequest#parse")]
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = DeleteShareGroupOffsetsRequestData::read(readable, version)?;
        Ok(Self::new(data, version))
    }

    /// Returns response data carrying `error`'s code and `error_message`, or
    /// `error`'s default message when `error_message` is `None`.
    ///
    /// Translates `getErrorDeleteResponseData(Errors)` and
    /// `getErrorDeleteResponseData(Errors, String)`, which differ only in the
    /// optional message (CLAUDE.md §2).
    #[doc(alias = "org.apache.kafka.common.requests.DeleteShareGroupOffsetsRequest#getErrorDeleteResponseData")]
    pub fn get_error_delete_response_data(
        error: Errors,
        error_message: Option<&str>,
    ) -> DeleteShareGroupOffsetsResponseData {
        let mut data = DeleteShareGroupOffsetsResponseData::new();
        data.set_error_code(error.code())
            .set_error_message(Some(error_message.unwrap_or(error.message()).to_string()));
        data
    }

    /// Returns response data carrying `error_code` and `error_message`, or the
    /// code's default message when `error_message` is `None`.
    ///
    /// Translates `getErrorDeleteResponseData(short, String)`.
    #[doc(alias = "org.apache.kafka.common.requests.DeleteShareGroupOffsetsRequest#getErrorDeleteResponseData")]
    pub fn get_error_delete_response_data_with_error_code_error_message(
        error_code: i16,
        error_message: Option<&str>,
    ) -> DeleteShareGroupOffsetsResponseData {
        let mut data = DeleteShareGroupOffsetsResponseData::new();
        data.set_error_code(error_code).set_error_message(Some(
            error_message.unwrap_or(Errors::for_code(error_code).message()).to_string(),
        ));
        data
    }
}

impl fmt::Display for DeleteShareGroupOffsetsRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.data)
    }
}

/// Builder for [`DeleteShareGroupOffsetsRequest`].
///
/// Corresponds to `DeleteShareGroupOffsetsRequest.Builder` in Java.
#[derive(Debug, Clone)]
#[doc(alias = "org.apache.kafka.common.requests.DeleteShareGroupOffsetsRequest$Builder")]
pub struct Builder {
    data: DeleteShareGroupOffsetsRequestData,
}

impl Builder {
    /// Creates a builder wrapping the given data.
    #[doc(alias = "org.apache.kafka.common.requests.DeleteShareGroupOffsetsRequest$Builder#Builder")]
    pub fn new(data: DeleteShareGroupOffsetsRequestData) -> Self {
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
        &ApiKeys::DELETE_SHARE_GROUP_OFFSETS
    }

    fn oldest_allowed_version(&self) -> i16 {
        ApiKeys::DELETE_SHARE_GROUP_OFFSETS.oldest_version()
    }

    fn latest_allowed_version(&self) -> i16 {
        // Mirrors Java's `super(ApiKeys)` → `latestVersion(false)`
        // (producer-transactions.md §12).
        ApiKeys::DELETE_SHARE_GROUP_OFFSETS.latest_version_enable_unstable_last_version(false)
    }

    fn build_version(&mut self, version: i16) -> io::Result<AbstractRequest> {
        // Java's `Builder.build(short)` performs no validation for this request.
        Ok(AbstractRequest::DeleteShareGroupOffsets(DeleteShareGroupOffsetsRequest::new(
            self.data.clone(),
            version,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::requests::request_response_test::{check_error_response, check_request};
    use crate::delete_share_group_offsets_request_data::DeleteShareGroupOffsetsRequestTopic;

    /// `RequestResponseTest.createDeleteShareGroupOffsetsRequest(short)`.
    fn create_delete_share_group_offsets_request(version: i16) -> DeleteShareGroupOffsetsRequest {
        let mut topic = DeleteShareGroupOffsetsRequestTopic::new();
        topic.set_topic_name("topic-1".to_string());
        let mut data = DeleteShareGroupOffsetsRequestData::new();
        data.set_group_id("group".to_string()).set_topics(vec![topic]);
        match Builder::new(data).build_version(version).expect("build") {
            AbstractRequest::DeleteShareGroupOffsets(request) => request,
            other => panic!("expected DeleteShareGroupOffsets, got {other:?}"),
        }
    }

    /// `RequestResponseTest.testSerialization`'s `DELETE_SHARE_GROUP_OFFSETS`
    /// arm, at every version: `checkRequest` and `checkErrorResponse` (the
    /// `checkResponse` half lives in `delete_share_group_offsets_response.rs`).
    #[test]
    fn test_serialization_all_versions() {
        for version in ApiKeys::DELETE_SHARE_GROUP_OFFSETS.all_versions() {
            let request = AbstractRequest::DeleteShareGroupOffsets(create_delete_share_group_offsets_request(version));
            check_request(&request);
            check_error_response(&request);
        }
    }

    #[test]
    fn test_builder_version_range() {
        let builder = Builder::new(DeleteShareGroupOffsetsRequestData::new());
        assert_eq!(&ApiKeys::DELETE_SHARE_GROUP_OFFSETS, builder.api_key());
        assert_eq!(0, builder.oldest_allowed_version());
        assert_eq!(0, builder.latest_allowed_version());
    }

    #[test]
    fn test_get_error_response() {
        let request = create_delete_share_group_offsets_request(0);
        match request.get_error_response(6, &Errors::GroupIdNotFound) {
            ConcreteResponse::DeleteShareGroupOffsets(response) => {
                assert_eq!(6, response.throttle_time_ms());
                assert_eq!(Errors::GroupIdNotFound.code(), response.data().error_code());
                assert_eq!(Some("The group id does not exist."), response.data().error_message().as_deref());
            },
            other => panic!("expected DeleteShareGroupOffsets, got {other:?}"),
        }
        let response = request.get_error_response_with_error_code_error_message(0, 42, Some("raw"));
        assert_eq!(42, response.data().error_code());
        assert_eq!(Some("raw"), response.data().error_message().as_deref());
    }

    /// Every `getErrorDeleteResponseData` overload falls back to the code's
    /// default message.
    #[test]
    fn test_get_error_delete_response_data() {
        let data = DeleteShareGroupOffsetsRequest::get_error_delete_response_data(Errors::GroupIdNotFound, None);
        assert_eq!(Errors::GroupIdNotFound.code(), data.error_code());
        assert_eq!(Some("The group id does not exist."), data.error_message().as_deref());
        let data = DeleteShareGroupOffsetsRequest::get_error_delete_response_data(Errors::GroupIdNotFound, Some("x"));
        assert_eq!(Some("x"), data.error_message().as_deref());

        let data = DeleteShareGroupOffsetsRequest::get_error_delete_response_data_with_error_code_error_message(
            Errors::GroupIdNotFound.code(),
            None,
        );
        assert_eq!(Errors::GroupIdNotFound.code(), data.error_code());
        assert_eq!(Some("The group id does not exist."), data.error_message().as_deref());
        let data = DeleteShareGroupOffsetsRequest::get_error_delete_response_data_with_error_code_error_message(
            Errors::GroupIdNotFound.code(),
            Some("y"),
        );
        assert_eq!(Some("y"), data.error_message().as_deref());
    }

    /// Byte-level v0 encoding, derived by hand from `DeleteShareGroupOffsetsRequest.json`.
    #[test]
    fn test_v0_encoding() {
        let mut first = DeleteShareGroupOffsetsRequestTopic::new();
        first.set_topic_name("a".to_string());
        let mut second = DeleteShareGroupOffsetsRequestTopic::new();
        second.set_topic_name("bc".to_string());
        let mut data = DeleteShareGroupOffsetsRequestData::new();
        data.set_group_id("g".to_string()).set_topics(vec![first, second]);
        let mut request = Builder::new(data).build_version(0).unwrap();
        let expected = vec![
            0x02, b'g', // GroupId
            0x03, // Topics: compact array of 2
            0x02, b'a', 0x00, // TopicName, tagged fields
            0x03, b'b', b'c', 0x00, // TopicName, tagged fields
            0x00, // request tagged fields
        ];
        assert_eq!(expected, request.serialize().unwrap().into_buffer());
    }
}
