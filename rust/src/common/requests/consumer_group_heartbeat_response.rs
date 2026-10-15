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

//! `ConsumerGroupHeartbeat` response handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.ConsumerGroupHeartbeatResponse`.
//!
//! Possible error codes:
//!  - `GroupAuthorizationFailed`
//!  - `NotCoordinator`
//!  - `CoordinatorNotAvailable`
//!  - `CoordinatorLoadInProgress`
//!  - `InvalidRequest`
//!  - `UnknownMemberId`
//!  - `FencedMemberEpoch`
//!  - `UnsupportedAssignor`
//!  - `UnreleasedInstanceId`
//!  - `GroupMaxSizeReached`
//!  - `InvalidRegularExpression`
//!  - `TopicAuthorizationFailed`

use std::collections::HashMap;
use std::io;

use crate::ConsumerGroupHeartbeatResponseData;
use crate::common::Uuid;
use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::consumer_group_heartbeat_response_data::{Assignment, TopicPartitions};

/// A `ConsumerGroupHeartbeat` response.
///
/// Corresponds to `org.apache.kafka.common.requests.ConsumerGroupHeartbeatResponse`.
#[derive(Debug, Clone)]
#[doc(alias = "org.apache.kafka.common.requests.ConsumerGroupHeartbeatResponse")]
pub struct ConsumerGroupHeartbeatResponse {
    data: ConsumerGroupHeartbeatResponseData,
}

impl ConsumerGroupHeartbeatResponse {
    /// Creates a new `ConsumerGroupHeartbeatResponse` from the underlying data.
    #[doc(alias = "org.apache.kafka.common.requests.ConsumerGroupHeartbeatResponse#ConsumerGroupHeartbeatResponse")]
    pub fn new(data: ConsumerGroupHeartbeatResponseData) -> Self {
        Self { data }
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::CONSUMER_GROUP_HEARTBEAT
    }

    /// Returns a reference to the underlying data.
    #[doc(alias = "org.apache.kafka.common.requests.ConsumerGroupHeartbeatResponse#data")]
    pub fn data(&self) -> &ConsumerGroupHeartbeatResponseData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut ConsumerGroupHeartbeatResponseData {
        &mut self.data
    }

    /// Returns the throttle time in milliseconds.
    #[doc(alias = "org.apache.kafka.common.requests.ConsumerGroupHeartbeatResponse#throttleTimeMs")]
    pub fn throttle_time_ms(&self) -> i32 {
        self.data.throttle_time_ms
    }

    /// Sets the throttle time in the response.
    #[doc(alias = "org.apache.kafka.common.requests.ConsumerGroupHeartbeatResponse#maybeSetThrottleTimeMs")]
    pub fn maybe_set_throttle_time_ms(&mut self, throttle_time_ms: i32) {
        self.data.set_throttle_time_ms(throttle_time_ms);
    }

    /// Returns the top-level error code wrapped as an [`Errors`].
    pub fn error(&self) -> Errors {
        Errors::for_code(self.data.error_code)
    }

    /// Returns error counts by [`Errors`].
    #[doc(alias = "org.apache.kafka.common.requests.ConsumerGroupHeartbeatResponse#errorCounts")]
    pub fn error_counts(&self) -> HashMap<Errors, i32> {
        let mut counts = HashMap::new();
        counts.insert(Errors::for_code(self.data.error_code), 1);
        counts
    }

    /// Parses a `ConsumerGroupHeartbeatResponse` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    #[doc(alias = "org.apache.kafka.common.requests.ConsumerGroupHeartbeatResponse#parse")]
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = ConsumerGroupHeartbeatResponseData::read(readable, version)?;
        Ok(Self::new(data))
    }

    /// Builds an [`Assignment`] from a map of topic id to a per-partition map
    /// (partition index → partition metadata); only the partition indices (map
    /// keys) are placed on the wire.
    ///
    /// Corresponds to Java's
    /// `ConsumerGroupHeartbeatResponse.createAssignment(Map<Uuid, Map<Integer, Integer>>)`
    /// (the value type widened from `Set<Integer>` to `Map<Integer, Integer>`
    /// in AK 4.3.1 for KIP-1251; `createAssignment` uses `.keySet()`).
    #[doc(alias = "org.apache.kafka.common.requests.ConsumerGroupHeartbeatResponse#createAssignment")]
    pub fn create_assignment(assignment: HashMap<Uuid, HashMap<i32, i32>>) -> Assignment {
        let topic_partitions: Vec<TopicPartitions> = assignment
            .into_iter()
            .map(|(topic_id, partitions)| {
                let mut tp = TopicPartitions::new();
                tp.set_topic_id(topic_id).set_partitions(partitions.into_keys().collect());
                tp
            })
            .collect();

        let mut result = Assignment::new();
        result.set_topic_partitions(topic_partitions);
        result
    }
}

impl std::fmt::Display for ConsumerGroupHeartbeatResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verifies `error()` accessor reflects the top-level error code.
    #[test]
    fn test_error_reflects_top_level_code() {
        let mut data = ConsumerGroupHeartbeatResponseData::new();
        data.set_error_code(Errors::CoordinatorNotAvailable.code());
        let resp = ConsumerGroupHeartbeatResponse::new(data);
        assert_eq!(resp.error(), Errors::CoordinatorNotAvailable);
    }

    /// Verifies `error_counts()` returns a single entry for the top-level code.
    #[test]
    fn test_error_counts_single_entry() {
        let mut data = ConsumerGroupHeartbeatResponseData::new();
        data.set_error_code(Errors::FencedMemberEpoch.code());
        let resp = ConsumerGroupHeartbeatResponse::new(data);
        let counts = resp.error_counts();
        assert_eq!(counts.get(&Errors::FencedMemberEpoch).copied().unwrap_or(0), 1);
    }

    /// Verifies `throttle_time_ms()` and `maybe_set_throttle_time_ms()`.
    #[test]
    fn test_throttle_time_ms() {
        let data = ConsumerGroupHeartbeatResponseData::new();
        let mut resp = ConsumerGroupHeartbeatResponse::new(data);
        assert_eq!(resp.throttle_time_ms(), 0);
        resp.maybe_set_throttle_time_ms(500);
        assert_eq!(resp.throttle_time_ms(), 500);
    }

    /// Verifies `create_assignment()` builds an Assignment with sorted partitions.
    #[test]
    fn test_create_assignment() {
        let mut input = HashMap::new();
        let topic_id = Uuid::new(1, 2);
        // Value type is Map<partition, metadata> (KIP-1251); only the keys
        // (partition indices) reach the wire.
        let mut parts = HashMap::new();
        parts.insert(0, 0);
        parts.insert(1, 0);
        parts.insert(2, 0);
        input.insert(topic_id, parts);

        let assignment = ConsumerGroupHeartbeatResponse::create_assignment(input);
        assert_eq!(assignment.topic_partitions.len(), 1);
        assert_eq!(assignment.topic_partitions[0].topic_id, topic_id);
        let mut got: Vec<i32> = assignment.topic_partitions[0].partitions.clone();
        got.sort();
        assert_eq!(got, vec![0, 1, 2]);
    }
}
