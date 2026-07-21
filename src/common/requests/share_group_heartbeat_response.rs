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
use std::io;

use crate::common::Uuid;
use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::share_group_heartbeat_response_data::{Assignment, ShareGroupHeartbeatResponseData, TopicPartitions};

/// A `ShareGroupHeartbeat` response.
///
/// Corresponds to `org.apache.kafka.common.requests.ShareGroupHeartbeatResponse`.
#[derive(Debug, Clone)]
pub struct ShareGroupHeartbeatResponse {
    data: ShareGroupHeartbeatResponseData,
}

impl ShareGroupHeartbeatResponse {
    /// Creates a new `ShareGroupHeartbeatResponse` from the underlying data.
    pub fn new(data: ShareGroupHeartbeatResponseData) -> Self {
        Self { data }
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::SHARE_GROUP_HEARTBEAT
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &ShareGroupHeartbeatResponseData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut ShareGroupHeartbeatResponseData {
        &mut self.data
    }

    /// Returns the top-level error code wrapped as an [`Errors`].
    pub fn error(&self) -> Errors {
        Errors::for_code(self.data.error_code)
    }

    /// Returns the throttle time in milliseconds.
    pub fn throttle_time_ms(&self) -> i32 {
        self.data.throttle_time_ms
    }

    /// Sets the throttle time in the response.
    pub fn maybe_set_throttle_time_ms(&mut self, throttle_time_ms: i32) {
        self.data.set_throttle_time_ms(throttle_time_ms);
    }

    /// Returns whether the client should throttle upon receiving this response.
    /// Java's `ShareGroupHeartbeatResponse` does NOT override
    /// `shouldClientThrottle`; it inherits the `AbstractResponse` default which
    /// returns `false`.
    pub fn should_client_throttle(&self, _version: i16) -> bool {
        false
    }

    /// Returns error counts by [`Errors`] (single top-level entry).
    ///
    /// Translates `ShareGroupHeartbeatResponse.errorCounts()`.
    pub fn error_counts(&self) -> HashMap<Errors, i32> {
        let mut counts = HashMap::new();
        counts.insert(Errors::for_code(self.data.error_code), 1);
        counts
    }

    /// Parses a `ShareGroupHeartbeatResponse` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = ShareGroupHeartbeatResponseData::read(readable, version)?;
        Ok(Self::new(data))
    }

    /// Builds an [`Assignment`] from a map of topic id to partition set.
    ///
    /// Corresponds to Java's
    /// `ShareGroupHeartbeatResponse.createAssignment(Map<Uuid, Set<Integer>>)`.
    pub fn create_assignment(assignment: HashMap<Uuid, HashSet<i32>>) -> Assignment {
        let topic_partitions: Vec<TopicPartitions> = assignment
            .into_iter()
            .map(|(topic_id, partitions)| {
                let mut tp = TopicPartitions::new();
                tp.set_topic_id(topic_id).set_partitions(partitions.into_iter().collect());
                tp
            })
            .collect();

        let mut result = Assignment::new();
        result.set_topic_partitions(topic_partitions);
        result
    }
}

impl std::fmt::Display for ShareGroupHeartbeatResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_and_error_counts() {
        let mut data = ShareGroupHeartbeatResponseData::new();
        data.set_error_code(Errors::NotCoordinator.code());
        let resp = ShareGroupHeartbeatResponse::new(data);
        assert_eq!(resp.error(), Errors::NotCoordinator);
        let counts = resp.error_counts();
        assert_eq!(*counts.get(&Errors::NotCoordinator).unwrap(), 1);
        assert_eq!(counts.len(), 1);
    }

    #[test]
    fn test_throttle() {
        let data = ShareGroupHeartbeatResponseData::new();
        let mut resp = ShareGroupHeartbeatResponse::new(data);
        assert_eq!(resp.throttle_time_ms(), 0);
        resp.maybe_set_throttle_time_ms(250);
        assert_eq!(resp.throttle_time_ms(), 250);
        assert!(!resp.should_client_throttle(0));
    }

    /// Byte-level wire-encoding test against a known vector (DoD §3). Verifies
    /// the flexible v1 `ShareGroupHeartbeatResponse` body encodes exactly as the
    /// Kafka wire spec requires, including the `-1` (0xFF) null-struct presence
    /// byte for a null `Assignment`.
    #[test]
    fn test_wire_encoding_known_vector() {
        use crate::common::requests::ConcreteResponse;
        let mut data = ShareGroupHeartbeatResponseData::new();
        data.set_error_code(Errors::NotCoordinator.code());
        let mut resp = ConcreteResponse::ShareGroupHeartbeat(ShareGroupHeartbeatResponse::new(data));
        let serialized = resp.serialize(1).expect("serialize body");
        let expected: Vec<u8> = vec![
            0x00, 0x00, 0x00, 0x00, // ThrottleTimeMs: int32 = 0
            0x00, 0x10, // ErrorCode: int16 = 16 (NOT_COORDINATOR)
            0x00, // ErrorMessage: compact nullable string, null
            0x00, // MemberId: compact nullable string, null
            0x00, 0x00, 0x00, 0x00, // MemberEpoch: int32 = 0
            0x00, 0x00, 0x00, 0x00, // HeartbeatIntervalMs: int32 = 0
            0xFF, // Assignment: null nullable struct -> -1 presence byte
            0x00, // empty tagged-field buffer
        ];
        assert_eq!(serialized.buffer(), expected.as_slice());
    }

    #[test]
    fn test_create_assignment() {
        let topic_id = Uuid::new(1, 2);
        let mut parts = HashSet::new();
        parts.insert(0);
        parts.insert(1);
        let mut input = HashMap::new();
        input.insert(topic_id, parts);

        let assignment = ShareGroupHeartbeatResponse::create_assignment(input);
        assert_eq!(assignment.topic_partitions.len(), 1);
        assert_eq!(assignment.topic_partitions[0].topic_id, topic_id);
        let mut got: Vec<i32> = assignment.topic_partitions[0].partitions.clone();
        got.sort_unstable();
        assert_eq!(got, vec![0, 1]);
    }
}
