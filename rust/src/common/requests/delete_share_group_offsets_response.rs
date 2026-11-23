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

//! `DeleteShareGroupOffsets` response handling (KIP-932).
//!
//! Corresponds to `org.apache.kafka.common.requests.DeleteShareGroupOffsetsResponse`.

use std::collections::HashMap;
use std::fmt;
use std::io;

use crate::DeleteShareGroupOffsetsResponseData;
use crate::common::protocol::{ApiKeys, Errors, Readable};

use super::AbstractResponse;

/// A `DeleteShareGroupOffsets` response.
///
/// Corresponds to `org.apache.kafka.common.requests.DeleteShareGroupOffsetsResponse`.
#[derive(Debug, Clone)]
#[doc(alias = "org.apache.kafka.common.requests.DeleteShareGroupOffsetsResponse")]
pub struct DeleteShareGroupOffsetsResponse {
    data: DeleteShareGroupOffsetsResponseData,
}

impl DeleteShareGroupOffsetsResponse {
    /// Creates a new `DeleteShareGroupOffsetsResponse` from the underlying data.
    #[doc(alias = "org.apache.kafka.common.requests.DeleteShareGroupOffsetsResponse#DeleteShareGroupOffsetsResponse")]
    pub fn new(data: DeleteShareGroupOffsetsResponseData) -> Self {
        Self { data }
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::DELETE_SHARE_GROUP_OFFSETS
    }

    /// Returns a reference to the underlying data.
    #[doc(alias = "org.apache.kafka.common.requests.DeleteShareGroupOffsetsResponse#data")]
    pub fn data(&self) -> &DeleteShareGroupOffsetsResponseData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut DeleteShareGroupOffsetsResponseData {
        &mut self.data
    }

    /// Returns the error counts: the top-level error once, plus one per topic.
    #[doc(alias = "org.apache.kafka.common.requests.DeleteShareGroupOffsetsResponse#errorCounts")]
    pub fn error_counts(&self) -> HashMap<Errors, i32> {
        let mut counts = HashMap::new();
        AbstractResponse::update_error_counts(&mut counts, Errors::for_code(self.data.error_code));
        for topic_result in &self.data.responses {
            AbstractResponse::update_error_counts(&mut counts, Errors::for_code(topic_result.error_code));
        }
        counts
    }

    /// Returns the throttle time in milliseconds.
    #[doc(alias = "org.apache.kafka.common.requests.DeleteShareGroupOffsetsResponse#throttleTimeMs")]
    pub fn throttle_time_ms(&self) -> i32 {
        self.data.throttle_time_ms
    }

    /// Sets the throttle time in the response.
    #[doc(alias = "org.apache.kafka.common.requests.DeleteShareGroupOffsetsResponse#maybeSetThrottleTimeMs")]
    pub fn maybe_set_throttle_time_ms(&mut self, throttle_time_ms: i32) {
        self.data.set_throttle_time_ms(throttle_time_ms);
    }

    /// Parses a `DeleteShareGroupOffsetsResponse` from a readable buffer at the
    /// given version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    #[doc(alias = "org.apache.kafka.common.requests.DeleteShareGroupOffsetsResponse#parse")]
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = DeleteShareGroupOffsetsResponseData::read(readable, version)?;
        Ok(Self::new(data))
    }
}

impl fmt::Display for DeleteShareGroupOffsetsResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Uuid;
    use crate::common::requests::ConcreteResponse;
    use crate::common::requests::request_response_test::check_response;
    use crate::delete_share_group_offsets_response_data::DeleteShareGroupOffsetsResponseTopic;

    fn topic(name: &str, topic_id: Uuid, error: Errors) -> DeleteShareGroupOffsetsResponseTopic {
        let mut topic = DeleteShareGroupOffsetsResponseTopic::new();
        topic
            .set_topic_name(name.to_string())
            .set_topic_id(topic_id)
            .set_error_code(error.code());
        topic
    }

    /// `RequestResponseTest.createDeleteShareGroupOffsetsResponse()`.
    fn create_delete_share_group_offsets_response() -> DeleteShareGroupOffsetsResponse {
        let mut data = DeleteShareGroupOffsetsResponseData::new();
        data.set_responses(vec![topic("topic-1", Uuid::random_uuid(), Errors::None)]);
        DeleteShareGroupOffsetsResponse::new(data)
    }

    /// `RequestResponseTest.testSerialization`'s `checkResponse` half for
    /// `DELETE_SHARE_GROUP_OFFSETS`, at every version.
    #[test]
    fn test_serialization_all_versions() {
        let response = ConcreteResponse::DeleteShareGroupOffsets(create_delete_share_group_offsets_response());
        for version in ApiKeys::DELETE_SHARE_GROUP_OFFSETS.all_versions() {
            check_response(&response, version);
        }
    }

    #[test]
    fn test_error_counts() {
        let mut data = DeleteShareGroupOffsetsResponseData::new();
        data.set_responses(vec![
            topic("a", Uuid::zero(), Errors::None),
            topic("b", Uuid::zero(), Errors::UnknownTopicOrPartition),
            topic("c", Uuid::zero(), Errors::UnknownTopicOrPartition),
        ]);
        assert_eq!(
            HashMap::from([(Errors::None, 2), (Errors::UnknownTopicOrPartition, 2)]),
            DeleteShareGroupOffsetsResponse::new(data).error_counts()
        );
    }

    #[test]
    fn test_throttle_time_accessors() {
        let mut response = create_delete_share_group_offsets_response();
        response.maybe_set_throttle_time_ms(2);
        assert_eq!(2, response.throttle_time_ms());
        assert_eq!(&ApiKeys::DELETE_SHARE_GROUP_OFFSETS, response.api_key());
    }

    /// Byte-level v0 encoding, derived by hand from `DeleteShareGroupOffsetsResponse.json`.
    #[test]
    fn test_v0_encoding() {
        let topic_id = Uuid::new(0x0102_0304_0506_0708, 0x090a_0b0c_0d0e_0f10);
        let mut failed = topic("t", topic_id, Errors::GroupIdNotFound);
        failed.set_error_message(Some("e".to_string()));
        let mut data = DeleteShareGroupOffsetsResponseData::new();
        data.set_throttle_time_ms(3)
            .set_error_code(0)
            .set_error_message(None)
            .set_responses(vec![failed]);
        let mut response = ConcreteResponse::DeleteShareGroupOffsets(DeleteShareGroupOffsetsResponse::new(data));

        let mut expected = Vec::new();
        expected.extend_from_slice(&3i32.to_be_bytes()); // ThrottleTimeMs
        expected.extend_from_slice(&0i16.to_be_bytes()); // ErrorCode
        expected.push(0x00); // ErrorMessage: null
        expected.push(0x02); // Responses: compact array of 1
        expected.extend_from_slice(&[0x02, b't']); // TopicName
        expected.extend_from_slice(&[
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
        ]); // TopicId
        expected.extend_from_slice(&Errors::GroupIdNotFound.code().to_be_bytes()); // ErrorCode
        expected.extend_from_slice(&[0x02, b'e']); // ErrorMessage
        expected.push(0x00); // topic tagged fields
        expected.push(0x00); // response tagged fields
        assert_eq!(expected, response.serialize(0).unwrap().into_buffer());
    }
}
