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

//! `DescribeShareGroupOffsets` response handling (KIP-932).
//!
//! Corresponds to `org.apache.kafka.common.requests.DescribeShareGroupOffsetsResponse`.

use std::collections::HashMap;
use std::fmt;
use std::io;

use crate::DescribeShareGroupOffsetsResponseData;
use crate::common::Error;
use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::describe_share_group_offsets_response_data::DescribeShareGroupOffsetsResponseGroup;

use super::AbstractResponse;

/// A `DescribeShareGroupOffsets` response.
///
/// Corresponds to `org.apache.kafka.common.requests.DescribeShareGroupOffsetsResponse`.
#[derive(Debug, Clone)]
#[doc(alias = "org.apache.kafka.common.requests.DescribeShareGroupOffsetsResponse")]
pub struct DescribeShareGroupOffsetsResponse {
    data: DescribeShareGroupOffsetsResponseData,
    /// The group-level error of each group that has one, by group id.
    group_level_errors: HashMap<String, Error>,
}

impl DescribeShareGroupOffsetsResponse {
    /// Wraps the given data, recording the group-level error of every group
    /// whose error code is not `NONE` (built from its code and message).
    #[doc(
        alias = "org.apache.kafka.common.requests.DescribeShareGroupOffsetsResponse#DescribeShareGroupOffsetsResponse"
    )]
    pub fn new(data: DescribeShareGroupOffsetsResponseData) -> Self {
        let mut group_level_errors = HashMap::new();
        for group in &data.groups {
            if group.error_code != Errors::None.code() {
                group_level_errors.insert(
                    group.group_id.clone(),
                    Errors::for_code(group.error_code).error_with_optional_message(group.error_message.as_deref()),
                );
            }
        }
        Self { data, group_level_errors }
    }

    /// Builds a response with the same group-level error for all groups and
    /// empty topics lists for all groups.
    ///
    /// Each group carries the code of `all_groups_error` and its message, or,
    /// for an unknown server error, that code's default message, so that an
    /// internal message never reaches the client.
    #[doc(
        alias = "org.apache.kafka.common.requests.DescribeShareGroupOffsetsResponse#DescribeShareGroupOffsetsResponse"
    )]
    pub fn with_throttle_time_ms_group_ids_all_groups_error(
        throttle_time_ms: i32,
        group_ids: &[String],
        all_groups_error: &Error,
    ) -> Self {
        let error_code = all_groups_error.error().code();
        let error_message = if error_code == Errors::UnknownServerError.code() {
            Errors::for_code(error_code).message()
        } else {
            all_groups_error.message()
        };
        let mut group_list = Vec::with_capacity(group_ids.len());
        let mut group_level_errors = HashMap::new();
        for group_id in group_ids {
            let mut group = DescribeShareGroupOffsetsResponseGroup::new();
            group
                .set_group_id(group_id.clone())
                .set_error_code(error_code)
                .set_error_message(Some(error_message.to_string()));
            group_list.push(group);
            group_level_errors.insert(group_id.clone(), all_groups_error.clone());
        }
        let mut data = DescribeShareGroupOffsetsResponseData::new();
        data.set_throttle_time_ms(throttle_time_ms).set_groups(group_list);
        Self { data, group_level_errors }
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::DESCRIBE_SHARE_GROUP_OFFSETS
    }

    /// Whether `group_id` has a group-level error.
    #[doc(alias = "org.apache.kafka.common.requests.DescribeShareGroupOffsetsResponse#hasGroupError")]
    pub fn has_group_error(&self, group_id: &str) -> bool {
        self.group_level_errors.contains_key(group_id)
    }

    /// Returns the group-level error of `group_id`, if it has one.
    #[doc(alias = "org.apache.kafka.common.requests.DescribeShareGroupOffsetsResponse#groupError")]
    pub fn group_error(&self, group_id: &str) -> Option<&Error> {
        self.group_level_errors.get(group_id)
    }

    /// Returns a reference to the underlying data.
    #[doc(alias = "org.apache.kafka.common.requests.DescribeShareGroupOffsetsResponse#data")]
    pub fn data(&self) -> &DescribeShareGroupOffsetsResponseData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut DescribeShareGroupOffsetsResponseData {
        &mut self.data
    }

    /// Returns the error counts: one per group-level error, plus one per
    /// partition of every group.
    #[doc(alias = "org.apache.kafka.common.requests.DescribeShareGroupOffsetsResponse#errorCounts")]
    pub fn error_counts(&self) -> HashMap<Errors, i32> {
        let mut counts = HashMap::new();
        for error in self.group_level_errors.values() {
            AbstractResponse::update_error_counts(&mut counts, error.error());
        }
        for group in &self.data.groups {
            for topic in &group.topics {
                for partition in &topic.partitions {
                    AbstractResponse::update_error_counts(&mut counts, Errors::for_code(partition.error_code));
                }
            }
        }
        counts
    }

    /// Returns the throttle time in milliseconds.
    #[doc(alias = "org.apache.kafka.common.requests.DescribeShareGroupOffsetsResponse#throttleTimeMs")]
    pub fn throttle_time_ms(&self) -> i32 {
        self.data.throttle_time_ms
    }

    /// Sets the throttle time in the response.
    #[doc(alias = "org.apache.kafka.common.requests.DescribeShareGroupOffsetsResponse#maybeSetThrottleTimeMs")]
    pub fn maybe_set_throttle_time_ms(&mut self, throttle_time_ms: i32) {
        self.data.set_throttle_time_ms(throttle_time_ms);
    }

    /// Parses a `DescribeShareGroupOffsetsResponse` from a readable buffer at
    /// the given version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    #[doc(alias = "org.apache.kafka.common.requests.DescribeShareGroupOffsetsResponse#parse")]
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = DescribeShareGroupOffsetsResponseData::read(readable, version)?;
        Ok(Self::new(data))
    }
}

impl fmt::Display for DescribeShareGroupOffsetsResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Uuid;
    use crate::common::protocol::ByteBufferAccessor;
    use crate::common::requests::ConcreteResponse;
    use crate::common::requests::request_response_test::check_response;
    use crate::describe_share_group_offsets_response_data::{
        DescribeShareGroupOffsetsResponsePartition, DescribeShareGroupOffsetsResponseTopic,
    };

    fn partition(index: i32, error: Errors) -> DescribeShareGroupOffsetsResponsePartition {
        let mut partition = DescribeShareGroupOffsetsResponsePartition::new();
        partition.set_partition_index(index).set_error_code(error.code());
        partition
    }

    /// `RequestResponseTest.createDescribeShareGroupOffsetsResponse()`.
    fn create_describe_share_group_offsets_response() -> DescribeShareGroupOffsetsResponse {
        let mut partition = partition(0, Errors::None);
        partition.set_start_offset(0).set_lag(0).set_leader_epoch(0);
        let mut topic = DescribeShareGroupOffsetsResponseTopic::new();
        topic
            .set_topic_name("topic-1".to_string())
            .set_topic_id(Uuid::random_uuid())
            .set_partitions(vec![partition]);
        let mut group = DescribeShareGroupOffsetsResponseGroup::new();
        group.set_group_id("group".to_string()).set_topics(vec![topic]);
        let mut data = DescribeShareGroupOffsetsResponseData::new();
        data.set_groups(vec![group]);
        DescribeShareGroupOffsetsResponse::new(data)
    }

    /// `RequestResponseTest.testSerialization`'s `checkResponse` half for
    /// `DESCRIBE_SHARE_GROUP_OFFSETS`, at every version. `Lag` is a v1+
    /// ignorable field, so v0 drops it (and the round trip still matches).
    #[test]
    fn test_serialization_all_versions() {
        let response = ConcreteResponse::DescribeShareGroupOffsets(create_describe_share_group_offsets_response());
        for version in ApiKeys::DESCRIBE_SHARE_GROUP_OFFSETS.all_versions() {
            check_response(&response, version);
        }
    }

    /// The data constructor records the error of each failed group, with its
    /// message, or the code's default message when the message is null.
    #[test]
    fn test_new_records_group_level_errors() {
        let mut ok = DescribeShareGroupOffsetsResponseGroup::new();
        ok.set_group_id("ok".to_string());
        let mut with_message = DescribeShareGroupOffsetsResponseGroup::new();
        with_message
            .set_group_id("m".to_string())
            .set_error_code(Errors::GroupAuthorizationFailed.code())
            .set_error_message(Some("nope".to_string()));
        let mut without_message = DescribeShareGroupOffsetsResponseGroup::new();
        without_message
            .set_group_id("n".to_string())
            .set_error_code(Errors::GroupIdNotFound.code());
        let mut data = DescribeShareGroupOffsetsResponseData::new();
        data.set_groups(vec![ok, with_message, without_message]);
        let response = DescribeShareGroupOffsetsResponse::new(data);

        assert!(!response.has_group_error("ok"));
        assert!(response.group_error("ok").is_none());
        let error = response.group_error("m").expect("m failed");
        assert_eq!(Errors::GroupAuthorizationFailed, error.error());
        assert_eq!("nope", error.message());
        let error = response.group_error("n").expect("n failed");
        assert_eq!(Errors::GroupIdNotFound, error.error());
        assert_eq!("The group id does not exist.", error.message());
        assert!(!response.has_group_error("missing"));
    }

    /// `errorCounts` counts each group-level error and each partition error;
    /// a group without an error is not counted.
    #[test]
    fn test_error_counts() {
        let mut topic = DescribeShareGroupOffsetsResponseTopic::new();
        topic.set_partitions(vec![
            partition(0, Errors::None),
            partition(1, Errors::UnknownTopicOrPartition),
        ]);
        let mut ok = DescribeShareGroupOffsetsResponseGroup::new();
        ok.set_group_id("ok".to_string()).set_topics(vec![topic]);
        let mut failed = DescribeShareGroupOffsetsResponseGroup::new();
        failed
            .set_group_id("f".to_string())
            .set_error_code(Errors::GroupIdNotFound.code());
        let mut data = DescribeShareGroupOffsetsResponseData::new();
        data.set_groups(vec![ok, failed]);
        assert_eq!(
            HashMap::from([
                (Errors::None, 1),
                (Errors::UnknownTopicOrPartition, 1),
                (Errors::GroupIdNotFound, 1),
            ]),
            DescribeShareGroupOffsetsResponse::new(data).error_counts()
        );
        assert_eq!(
            HashMap::from([(Errors::None, 1)]),
            create_describe_share_group_offsets_response().error_counts()
        );
    }

    /// The all-groups constructor counts one error per group.
    #[test]
    fn test_all_groups_error_counts() {
        let response = DescribeShareGroupOffsetsResponse::with_throttle_time_ms_group_ids_all_groups_error(
            3,
            &["a".to_string(), "b".to_string()],
            &Error::new(Errors::CoordinatorNotAvailable),
        );
        assert_eq!(3, response.throttle_time_ms());
        assert_eq!(HashMap::from([(Errors::CoordinatorNotAvailable, 2)]), response.error_counts());
        assert_eq!(
            Some(Errors::CoordinatorNotAvailable.message()),
            response.data().groups()[1].error_message().as_deref()
        );
    }

    #[test]
    fn test_throttle_time_accessors() {
        let mut response = create_describe_share_group_offsets_response();
        response.maybe_set_throttle_time_ms(9);
        assert_eq!(9, response.throttle_time_ms());
        assert_eq!(&ApiKeys::DESCRIBE_SHARE_GROUP_OFFSETS, response.api_key());
    }

    /// Byte-level v0 and v1 encodings, derived by hand from
    /// `DescribeShareGroupOffsetsResponse.json` in field order (all versions
    /// flexible). They differ only in `Lag`, a v1+ ignorable field that v0
    /// drops.
    #[test]
    fn test_v0_and_v1_encoding() {
        let mut partition = partition(2, Errors::None);
        partition
            .set_start_offset(40)
            .set_leader_epoch(1)
            .set_lag(5)
            .set_error_message(None);
        let mut topic = DescribeShareGroupOffsetsResponseTopic::new();
        topic
            .set_topic_name("t".to_string())
            .set_topic_id(Uuid::new(0x0102_0304_0506_0708, 0x090a_0b0c_0d0e_0f10))
            .set_partitions(vec![partition]);
        let mut group = DescribeShareGroupOffsetsResponseGroup::new();
        group
            .set_group_id("g".to_string())
            .set_topics(vec![topic])
            .set_error_code(0)
            .set_error_message(None);
        let mut data = DescribeShareGroupOffsetsResponseData::new();
        data.set_throttle_time_ms(1).set_groups(vec![group]);
        let response = DescribeShareGroupOffsetsResponse::new(data);

        let encode = |version: i16| {
            let mut bytes = Vec::new();
            bytes.extend_from_slice(&1i32.to_be_bytes()); // ThrottleTimeMs
            bytes.push(0x02); // Groups: compact array of 1
            bytes.extend_from_slice(&[0x02, b'g']); // GroupId
            bytes.push(0x02); // Topics: compact array of 1
            bytes.extend_from_slice(&[0x02, b't']); // TopicName
            bytes.extend_from_slice(&[
                0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
            ]); // TopicId
            bytes.push(0x02); // Partitions: compact array of 1
            bytes.extend_from_slice(&2i32.to_be_bytes()); // PartitionIndex
            bytes.extend_from_slice(&40i64.to_be_bytes()); // StartOffset
            bytes.extend_from_slice(&1i32.to_be_bytes()); // LeaderEpoch
            if version >= 1 {
                bytes.extend_from_slice(&5i64.to_be_bytes()); // Lag
            }
            bytes.extend_from_slice(&0i16.to_be_bytes()); // ErrorCode
            bytes.push(0x00); // ErrorMessage: null
            bytes.push(0x00); // partition tagged fields
            bytes.push(0x00); // topic tagged fields
            bytes.extend_from_slice(&0i16.to_be_bytes()); // group ErrorCode
            bytes.push(0x00); // group ErrorMessage: null
            bytes.push(0x00); // group tagged fields
            bytes.push(0x00); // response tagged fields
            bytes
        };
        for version in ApiKeys::DESCRIBE_SHARE_GROUP_OFFSETS.all_versions() {
            let mut concrete = ConcreteResponse::DescribeShareGroupOffsets(response.clone());
            assert_eq!(
                encode(version),
                concrete.serialize(version).unwrap().into_buffer(),
                "v{version}"
            );
        }
        // Lag decodes back to its -1 default at v0.
        let parsed = DescribeShareGroupOffsetsResponse::parse(&mut ByteBufferAccessor::new(encode(0)), 0).unwrap();
        assert_eq!(-1, parsed.data().groups()[0].topics()[0].partitions()[0].lag());
    }
}
