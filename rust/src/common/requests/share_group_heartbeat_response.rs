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

//! `ShareGroupHeartbeat` response handling (KIP-932).
//!
//! Corresponds to `org.apache.kafka.common.requests.ShareGroupHeartbeatResponse`.
//!
//! Possible error codes:
//!  - `GroupAuthorizationFailed`
//!  - `NotCoordinator`
//!  - `CoordinatorNotAvailable`
//!  - `CoordinatorLoadInProgress`
//!  - `InvalidRequest`
//!  - `UnknownMemberId`
//!  - `GroupMaxSizeReached`
//!  - `TopicAuthorizationFailed`

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::io;

use crate::ShareGroupHeartbeatResponseData;
use crate::common::Uuid;
use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::share_group_heartbeat_response_data::{Assignment, TopicPartitions};

/// A `ShareGroupHeartbeat` response.
///
/// Corresponds to `org.apache.kafka.common.requests.ShareGroupHeartbeatResponse`.
#[derive(Debug, Clone)]
#[doc(alias = "org.apache.kafka.common.requests.ShareGroupHeartbeatResponse")]
pub struct ShareGroupHeartbeatResponse {
    data: ShareGroupHeartbeatResponseData,
}

impl ShareGroupHeartbeatResponse {
    /// Creates a new `ShareGroupHeartbeatResponse` from the underlying data.
    #[doc(alias = "org.apache.kafka.common.requests.ShareGroupHeartbeatResponse#ShareGroupHeartbeatResponse")]
    pub fn new(data: ShareGroupHeartbeatResponseData) -> Self {
        Self { data }
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::SHARE_GROUP_HEARTBEAT
    }

    /// Returns a reference to the underlying data.
    #[doc(alias = "org.apache.kafka.common.requests.ShareGroupHeartbeatResponse#data")]
    pub fn data(&self) -> &ShareGroupHeartbeatResponseData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut ShareGroupHeartbeatResponseData {
        &mut self.data
    }

    /// Returns the error counts: the top-level error, counted once.
    #[doc(alias = "org.apache.kafka.common.requests.ShareGroupHeartbeatResponse#errorCounts")]
    pub fn error_counts(&self) -> HashMap<Errors, i32> {
        HashMap::from([(Errors::for_code(self.data.error_code), 1)])
    }

    /// Returns the throttle time in milliseconds.
    #[doc(alias = "org.apache.kafka.common.requests.ShareGroupHeartbeatResponse#throttleTimeMs")]
    pub fn throttle_time_ms(&self) -> i32 {
        self.data.throttle_time_ms
    }

    /// Sets the throttle time in the response.
    #[doc(alias = "org.apache.kafka.common.requests.ShareGroupHeartbeatResponse#maybeSetThrottleTimeMs")]
    pub fn maybe_set_throttle_time_ms(&mut self, throttle_time_ms: i32) {
        self.data.set_throttle_time_ms(throttle_time_ms);
    }

    /// Parses a `ShareGroupHeartbeatResponse` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    #[doc(alias = "org.apache.kafka.common.requests.ShareGroupHeartbeatResponse#parse")]
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = ShareGroupHeartbeatResponseData::read(readable, version)?;
        Ok(Self::new(data))
    }

    /// Builds an [`Assignment`] from a map of topic id to assigned partitions.
    ///
    /// Translates `ShareGroupHeartbeatResponse.createAssignment(Map<Uuid, Set<Integer>>)`;
    /// as in Java, the topic and partition order is the iteration order of the
    /// map and the sets.
    #[doc(alias = "org.apache.kafka.common.requests.ShareGroupHeartbeatResponse#createAssignment")]
    pub fn create_assignment(assignment: &HashMap<Uuid, HashSet<i32>>) -> Assignment {
        let topic_partitions = assignment
            .iter()
            .map(|(topic_id, partitions)| {
                let mut topic_partitions = TopicPartitions::new();
                topic_partitions
                    .set_topic_id(*topic_id)
                    .set_partitions(partitions.iter().copied().collect());
                topic_partitions
            })
            .collect();
        let mut result = Assignment::new();
        result.set_topic_partitions(topic_partitions);
        result
    }
}

impl fmt::Display for ShareGroupHeartbeatResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::protocol::ByteBufferAccessor;
    use crate::common::requests::ConcreteResponse;
    use crate::common::requests::request_response_test::check_response;

    /// `RequestResponseTest.createShareGroupHeartbeatResponse()`.
    fn create_share_group_heartbeat_response() -> ShareGroupHeartbeatResponse {
        let mut first = TopicPartitions::new();
        first.set_topic_id(Uuid::random_uuid()).set_partitions(vec![0, 1, 2]);
        let mut second = TopicPartitions::new();
        second.set_topic_id(Uuid::random_uuid()).set_partitions(vec![3, 4, 5]);
        let mut assignment = Assignment::new();
        assignment.set_topic_partitions(vec![first, second]);
        let mut data = ShareGroupHeartbeatResponseData::new();
        data.set_error_code(Errors::None.code())
            .set_throttle_time_ms(1000)
            .set_member_id(Some("memberid".to_string()))
            .set_member_epoch(11)
            .set_assignment(Some(assignment));
        ShareGroupHeartbeatResponse::new(data)
    }

    /// `RequestResponseTest.testSerialization`'s `checkResponse` half for
    /// `SHARE_GROUP_HEARTBEAT`, at every version.
    #[test]
    fn test_serialization_all_versions() {
        let response = ConcreteResponse::ShareGroupHeartbeat(create_share_group_heartbeat_response());
        for version in ApiKeys::SHARE_GROUP_HEARTBEAT.all_versions() {
            check_response(&response, version);
        }
    }

    /// `RequestResponseTest#testErrorCountsIncludesNone`:
    /// `assertEquals(1, createShareGroupHeartbeatResponse().errorCounts().get(Errors.NONE))`.
    #[test]
    fn test_error_counts_includes_none() {
        assert_eq!(
            Some(&1),
            create_share_group_heartbeat_response().error_counts().get(&Errors::None)
        );
    }

    #[test]
    fn test_throttle_time_accessors() {
        let mut response = create_share_group_heartbeat_response();
        assert_eq!(1000, response.throttle_time_ms());
        response.maybe_set_throttle_time_ms(7);
        assert_eq!(7, response.throttle_time_ms());
        assert_eq!(&ApiKeys::SHARE_GROUP_HEARTBEAT, response.api_key());
    }

    #[test]
    fn test_create_assignment() {
        let topic_a = Uuid::new(1, 1);
        let topic_b = Uuid::new(2, 2);
        let assignment = ShareGroupHeartbeatResponse::create_assignment(&HashMap::from([
            (topic_a, HashSet::from([2, 0])),
            (topic_b, HashSet::from([5])),
        ]));
        let mut actual: Vec<(Uuid, Vec<i32>)> = assignment
            .topic_partitions()
            .iter()
            .map(|tp| {
                let mut partitions = tp.partitions().clone();
                partitions.sort_unstable();
                (tp.topic_id(), partitions)
            })
            .collect();
        actual.sort_by_key(|(id, _)| *id);
        assert_eq!(vec![(topic_a, vec![0, 2]), (topic_b, vec![5])], actual);
    }

    /// Byte-level v1 encoding, derived by hand from `ShareGroupHeartbeatResponse.json`
    /// in field order (all versions flexible). The nullable `Assignment` struct
    /// is a presence byte (`1`) followed by the struct; the topic id is its two
    /// big-endian `i64` halves.
    #[test]
    fn test_v1_encoding() {
        let topic_id = Uuid::new(0x0102_0304_0506_0708, 0x090a_0b0c_0d0e_0f10);
        let mut topic = TopicPartitions::new();
        topic.set_topic_id(topic_id).set_partitions(vec![7]);
        let mut assignment = Assignment::new();
        assignment.set_topic_partitions(vec![topic]);
        let mut data = ShareGroupHeartbeatResponseData::new();
        data.set_throttle_time_ms(5)
            .set_error_code(0)
            .set_error_message(None)
            .set_member_id(Some("m".to_string()))
            .set_member_epoch(2)
            .set_heartbeat_interval_ms(3000)
            .set_assignment(Some(assignment));
        let mut response = ConcreteResponse::ShareGroupHeartbeat(ShareGroupHeartbeatResponse::new(data));

        let mut expected = Vec::new();
        expected.extend_from_slice(&5i32.to_be_bytes()); // ThrottleTimeMs
        expected.extend_from_slice(&0i16.to_be_bytes()); // ErrorCode
        expected.push(0x00); // ErrorMessage: null
        expected.extend_from_slice(&[0x02, b'm']); // MemberId
        expected.extend_from_slice(&2i32.to_be_bytes()); // MemberEpoch
        expected.extend_from_slice(&3000i32.to_be_bytes()); // HeartbeatIntervalMs
        expected.push(0x01); // Assignment: present
        expected.push(0x02); // TopicPartitions: compact array of 1
        expected.extend_from_slice(&[
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
        ]); // TopicId
        expected.push(0x02); // Partitions: compact array of 1
        expected.extend_from_slice(&7i32.to_be_bytes());
        expected.push(0x00); // TopicPartitions tagged fields
        expected.push(0x00); // Assignment tagged fields
        expected.push(0x00); // response tagged fields
        assert_eq!(expected, response.serialize(1).unwrap().into_buffer());

        let parsed = ShareGroupHeartbeatResponse::parse(&mut ByteBufferAccessor::new(expected), 1).unwrap();
        assert_eq!(Some("m"), parsed.data().member_id.as_deref());
        assert_eq!(
            Some(&vec![7]),
            parsed.data().assignment.as_ref().map(|a| a.topic_partitions()[0].partitions())
        );
    }

    /// A null assignment (unchanged since the last heartbeat) is the `-1`
    /// presence byte.
    #[test]
    fn test_v1_encoding_null_assignment() {
        let mut data = ShareGroupHeartbeatResponseData::new();
        data.set_member_id(None).set_assignment(None);
        let mut response = ConcreteResponse::ShareGroupHeartbeat(ShareGroupHeartbeatResponse::new(data));

        let mut expected = Vec::new();
        expected.extend_from_slice(&0i32.to_be_bytes()); // ThrottleTimeMs
        expected.extend_from_slice(&0i16.to_be_bytes()); // ErrorCode
        expected.push(0x00); // ErrorMessage: null
        expected.push(0x00); // MemberId: null
        expected.extend_from_slice(&0i32.to_be_bytes()); // MemberEpoch
        expected.extend_from_slice(&0i32.to_be_bytes()); // HeartbeatIntervalMs
        expected.push(0xff); // Assignment: null
        expected.push(0x00); // response tagged fields
        assert_eq!(expected, response.serialize(1).unwrap().into_buffer());
    }
}
