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

//! `AlterShareGroupOffsets` response handling (KIP-932).
//!
//! Corresponds to `org.apache.kafka.common.requests.AlterShareGroupOffsetsResponse`.

use std::collections::HashMap;
use std::fmt;
use std::io;

use crate::AlterShareGroupOffsetsResponseData;
use crate::alter_share_group_offsets_response_data::{
    AlterShareGroupOffsetsResponsePartition, AlterShareGroupOffsetsResponseTopic,
};
use crate::common::Uuid;
use crate::common::protocol::{ApiKeys, Errors, Readable};

use super::AbstractResponse;

/// An `AlterShareGroupOffsets` response.
///
/// Corresponds to `org.apache.kafka.common.requests.AlterShareGroupOffsetsResponse`.
#[derive(Debug, Clone)]
#[doc(alias = "org.apache.kafka.common.requests.AlterShareGroupOffsetsResponse")]
pub struct AlterShareGroupOffsetsResponse {
    data: AlterShareGroupOffsetsResponseData,
}

impl AlterShareGroupOffsetsResponse {
    /// Creates a new `AlterShareGroupOffsetsResponse` from the underlying data.
    #[doc(alias = "org.apache.kafka.common.requests.AlterShareGroupOffsetsResponse#AlterShareGroupOffsetsResponse")]
    pub fn new(data: AlterShareGroupOffsetsResponseData) -> Self {
        Self { data }
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::ALTER_SHARE_GROUP_OFFSETS
    }

    /// Returns the error counts: the top-level error once, plus one per
    /// partition.
    #[doc(alias = "org.apache.kafka.common.requests.AlterShareGroupOffsetsResponse#errorCounts")]
    pub fn error_counts(&self) -> HashMap<Errors, i32> {
        let mut counts = HashMap::new();
        AbstractResponse::update_error_counts(&mut counts, Errors::for_code(self.data.error_code));
        for topic in &self.data.responses {
            for partition_response in &topic.partitions {
                AbstractResponse::update_error_counts(&mut counts, Errors::for_code(partition_response.error_code));
            }
        }
        counts
    }

    /// Returns the throttle time in milliseconds.
    #[doc(alias = "org.apache.kafka.common.requests.AlterShareGroupOffsetsResponse#throttleTimeMs")]
    pub fn throttle_time_ms(&self) -> i32 {
        self.data.throttle_time_ms
    }

    /// Sets the throttle time in the response.
    #[doc(alias = "org.apache.kafka.common.requests.AlterShareGroupOffsetsResponse#maybeSetThrottleTimeMs")]
    pub fn maybe_set_throttle_time_ms(&mut self, throttle_time_ms: i32) {
        self.data.set_throttle_time_ms(throttle_time_ms);
    }

    /// Returns a reference to the underlying data.
    #[doc(alias = "org.apache.kafka.common.requests.AlterShareGroupOffsetsResponse#data")]
    pub fn data(&self) -> &AlterShareGroupOffsetsResponseData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut AlterShareGroupOffsetsResponseData {
        &mut self.data
    }

    /// Parses an `AlterShareGroupOffsetsResponse` from a readable buffer at the
    /// given version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    #[doc(alias = "org.apache.kafka.common.requests.AlterShareGroupOffsetsResponse#parse")]
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = AlterShareGroupOffsetsResponseData::read(readable, version)?;
        Ok(Self::new(data))
    }
}

impl fmt::Display for AlterShareGroupOffsetsResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.data)
    }
}

/// Assembles an [`AlterShareGroupOffsetsResponse`] partition by partition,
/// grouping the partitions by topic name.
///
/// Corresponds to `AlterShareGroupOffsetsResponse.Builder` in Java.
#[derive(Debug, Clone)]
#[doc(alias = "org.apache.kafka.common.requests.AlterShareGroupOffsetsResponse$Builder")]
pub struct Builder {
    data: AlterShareGroupOffsetsResponseData,
    /// Java's `AlterShareGroupOffsetsResponseTopicCollection`, keyed by topic
    /// name; insertion order is kept.
    topics: Vec<AlterShareGroupOffsetsResponseTopic>,
}

impl Builder {
    /// Creates an empty builder.
    #[doc(alias = "org.apache.kafka.common.requests.AlterShareGroupOffsetsResponse$Builder#Builder")]
    pub fn new() -> Self {
        Self { data: AlterShareGroupOffsetsResponseData::new(), topics: Vec::new() }
    }

    /// Returns the topic named `topic`, adding it with `topic_id` (or the zero
    /// id) if the builder has none.
    #[doc(alias = "org.apache.kafka.common.requests.AlterShareGroupOffsetsResponse$Builder#getOrCreateTopic")]
    fn get_or_create_topic(&mut self, topic: &str, topic_id: Option<Uuid>) -> &mut AlterShareGroupOffsetsResponseTopic {
        let index = match self.topics.iter().position(|t| t.topic_name == topic) {
            Some(index) => index,
            None => {
                let mut topic_data = AlterShareGroupOffsetsResponseTopic::new();
                topic_data
                    .set_topic_name(topic.to_string())
                    .set_topic_id(topic_id.unwrap_or_else(Uuid::zero));
                self.topics.push(topic_data);
                self.topics.len() - 1
            },
        };
        &mut self.topics[index]
    }

    /// Adds the result of one partition.
    ///
    /// Java takes an `ApiError`; the crate has no `ApiError` type, so its
    /// `(error, message)` pair is passed apart (the `AlterClientQuotasResponse`
    /// precedent). `error_message` is `ApiError.message()`, which may be null.
    /// `topic_ids_to_names` maps topic **names** to ids, whatever its Java name
    /// says.
    #[doc(alias = "org.apache.kafka.common.requests.AlterShareGroupOffsetsResponse$Builder#addPartition")]
    pub fn add_partition(
        &mut self,
        topic: &str,
        partition: i32,
        topic_ids_to_names: &HashMap<String, Uuid>,
        error: Errors,
        error_message: Option<&str>,
    ) -> &mut Self {
        let topic_data = self.get_or_create_topic(topic, topic_ids_to_names.get(topic).copied());
        let mut partition_data = AlterShareGroupOffsetsResponsePartition::new();
        partition_data
            .set_partition_index(partition)
            .set_error_code(error.code())
            .set_error_message(error_message.map(str::to_string));
        topic_data.partitions.push(partition_data);
        self
    }

    /// Builds the response from the partitions added so far.
    #[doc(alias = "org.apache.kafka.common.requests.AlterShareGroupOffsetsResponse$Builder#build")]
    pub fn build(self) -> AlterShareGroupOffsetsResponse {
        let Self { mut data, topics } = self;
        data.set_responses(topics);
        AlterShareGroupOffsetsResponse::new(data)
    }

    /// Adds every partition result of `data`, grouping them under the topics
    /// already in the builder.
    #[doc(alias = "org.apache.kafka.common.requests.AlterShareGroupOffsetsResponse$Builder#merge")]
    pub fn merge(
        &mut self,
        data: &AlterShareGroupOffsetsResponseData,
        topic_ids_to_names: &HashMap<String, Uuid>,
    ) -> &mut Self {
        for topic in &data.responses {
            let new_topic =
                self.get_or_create_topic(&topic.topic_name, topic_ids_to_names.get(&topic.topic_name).copied());
            for partition in &topic.partitions {
                let mut partition_data = AlterShareGroupOffsetsResponsePartition::new();
                partition_data
                    .set_partition_index(partition.partition_index)
                    .set_error_code(partition.error_code)
                    .set_error_message(partition.error_message.clone());
                new_topic.partitions.push(partition_data);
            }
        }
        self
    }
}

impl Default for Builder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::requests::ConcreteResponse;
    use crate::common::requests::request_response_test::check_response;

    /// `RequestResponseTest.createAlterShareGroupOffsetsResponse()`.
    fn create_alter_share_group_offsets_response() -> AlterShareGroupOffsetsResponse {
        let mut partition = AlterShareGroupOffsetsResponsePartition::new();
        partition.set_partition_index(0).set_error_code(Errors::None.code());
        let mut topic = AlterShareGroupOffsetsResponseTopic::new();
        topic
            .set_partitions(vec![partition])
            .set_topic_name("topic".to_string())
            .set_topic_id(Uuid::random_uuid());
        let mut data = AlterShareGroupOffsetsResponseData::new();
        data.set_responses(vec![topic]);
        AlterShareGroupOffsetsResponse::new(data)
    }

    /// `RequestResponseTest.testSerialization`'s `checkResponse` half for
    /// `ALTER_SHARE_GROUP_OFFSETS`, at every version.
    #[test]
    fn test_serialization_all_versions() {
        let response = ConcreteResponse::AlterShareGroupOffsets(create_alter_share_group_offsets_response());
        for version in ApiKeys::ALTER_SHARE_GROUP_OFFSETS.all_versions() {
            check_response(&response, version);
        }
    }

    #[test]
    fn test_error_counts() {
        assert_eq!(
            HashMap::from([(Errors::None, 2)]),
            create_alter_share_group_offsets_response().error_counts()
        );
    }

    #[test]
    fn test_throttle_time_accessors() {
        let mut response = create_alter_share_group_offsets_response();
        response.maybe_set_throttle_time_ms(8);
        assert_eq!(8, response.throttle_time_ms());
        assert_eq!(&ApiKeys::ALTER_SHARE_GROUP_OFFSETS, response.api_key());
    }

    /// `Builder.addPartition` groups by topic name in first-seen order, takes
    /// the id from the map (zero when missing), and keeps a null message null.
    #[test]
    fn test_builder_add_partition() {
        let id_b = Uuid::new(0, 2);
        let ids = HashMap::from([("b".to_string(), id_b)]);
        let mut builder = Builder::new();
        builder
            .add_partition("b", 1, &ids, Errors::None, None)
            .add_partition("a", 0, &ids, Errors::UnknownTopicOrPartition, Some("missing"))
            .add_partition("b", 0, &ids, Errors::None, None);
        let response = builder.build();
        let topics = response.data().responses();
        assert_eq!(2, topics.len());
        assert_eq!(("b", id_b), (topics[0].topic_name().as_str(), topics[0].topic_id()));
        assert_eq!(("a", Uuid::zero()), (topics[1].topic_name().as_str(), topics[1].topic_id()));
        let b: Vec<i32> = topics[0].partitions().iter().map(|p| p.partition_index()).collect();
        assert_eq!(vec![1, 0], b);
        assert_eq!(&None, topics[0].partitions()[0].error_message());
        assert_eq!(Errors::UnknownTopicOrPartition.code(), topics[1].partitions()[0].error_code());
        assert_eq!(Some("missing"), topics[1].partitions()[0].error_message().as_deref());
        assert_eq!(
            HashMap::from([(Errors::None, 3), (Errors::UnknownTopicOrPartition, 1)]),
            response.error_counts()
        );
    }

    /// `Builder.merge` appends the partitions of other response data under the
    /// matching topics, ignoring their ids in favour of the map.
    #[test]
    fn test_builder_merge() {
        let id_a = Uuid::new(0, 1);
        let ids = HashMap::from([("a".to_string(), id_a)]);
        let mut other = Builder::new();
        other
            .add_partition("a", 3, &HashMap::new(), Errors::InvalidRequest, Some("bad"))
            .add_partition("c", 0, &HashMap::new(), Errors::None, None);
        let other = other.build();

        let mut builder = Builder::new();
        builder
            .add_partition("a", 1, &ids, Errors::None, None)
            .merge(other.data(), &ids);
        let response = builder.build();
        let topics = response.data().responses();
        assert_eq!(
            vec!["a", "c"],
            topics.iter().map(|t| t.topic_name().as_str()).collect::<Vec<_>>()
        );
        assert_eq!(id_a, topics[0].topic_id());
        assert_eq!(Uuid::zero(), topics[1].topic_id());
        let a: Vec<(i32, i16, Option<&str>)> = topics[0]
            .partitions()
            .iter()
            .map(|p| (p.partition_index(), p.error_code(), p.error_message().as_deref()))
            .collect();
        assert_eq!(vec![(1, 0, None), (3, Errors::InvalidRequest.code(), Some("bad"))], a);
    }

    /// Byte-level v0 encoding, derived by hand from `AlterShareGroupOffsetsResponse.json`.
    #[test]
    fn test_v0_encoding() {
        let topic_id = Uuid::new(0x0102_0304_0506_0708, 0x090a_0b0c_0d0e_0f10);
        let ids = HashMap::from([("t".to_string(), topic_id)]);
        let mut builder = Builder::new();
        builder.add_partition("t", 2, &ids, Errors::None, None);
        let mut response = ConcreteResponse::AlterShareGroupOffsets(builder.build());

        let mut expected = Vec::new();
        expected.extend_from_slice(&0i32.to_be_bytes()); // ThrottleTimeMs
        expected.extend_from_slice(&0i16.to_be_bytes()); // ErrorCode
        expected.push(0x00); // ErrorMessage: null
        expected.push(0x02); // Responses: compact array of 1
        expected.extend_from_slice(&[0x02, b't']); // TopicName
        expected.extend_from_slice(&[
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
        ]); // TopicId
        expected.push(0x02); // Partitions: compact array of 1
        expected.extend_from_slice(&2i32.to_be_bytes()); // PartitionIndex
        expected.extend_from_slice(&0i16.to_be_bytes()); // ErrorCode
        expected.push(0x00); // ErrorMessage: null
        expected.push(0x00); // partition tagged fields
        expected.push(0x00); // topic tagged fields
        expected.push(0x00); // response tagged fields
        assert_eq!(expected, response.serialize(0).unwrap().into_buffer());
    }
}
