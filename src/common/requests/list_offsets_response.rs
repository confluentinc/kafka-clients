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

//! `ListOffsets` response handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.ListOffsetsResponse`.
//!
//! Possible error codes:
//! - `UNSUPPORTED_FOR_MESSAGE_FORMAT` — message format does not support
//!   lookup by timestamp.
//! - `TOPIC_AUTHORIZATION_FAILED` — caller lacks DESCRIBE access.
//! - `REPLICA_NOT_AVAILABLE` — request received by a non-replica broker.
//! - `NOT_LEADER_OR_FOLLOWER` / `FENCED_LEADER_EPOCH` / `UNKNOWN_LEADER_EPOCH`
//!   — leader epoch mismatch.
//! - `UNKNOWN_TOPIC_OR_PARTITION` / `KAFKA_STORAGE_ERROR` — metadata or log
//!   directory issues.
//! - `LEADER_NOT_AVAILABLE` (v4) / `OFFSET_NOT_AVAILABLE` (v5+) — leader's
//!   high-watermark has not caught up after recent election.

use std::collections::HashMap;
use std::io;

use crate::common::TopicPartition;
use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::list_offsets_response_data::{
    ListOffsetsPartitionResponse, ListOffsetsResponseData, ListOffsetsTopicResponse,
};

use super::RECORD_BATCH_NO_PARTITION_LEADER_EPOCH;
use super::abstract_response::update_error_counts;

/// Sentinel timestamp used when the broker has no usable timestamp.
pub const UNKNOWN_TIMESTAMP: i64 = -1;
/// Sentinel offset used when the broker has no usable offset.
pub const UNKNOWN_OFFSET: i64 = -1;
/// Sentinel leader-epoch used when the broker has no usable leader epoch.
pub const UNKNOWN_EPOCH: i32 = RECORD_BATCH_NO_PARTITION_LEADER_EPOCH;

/// A `ListOffsets` response.
///
/// Corresponds to `org.apache.kafka.common.requests.ListOffsetsResponse`.
#[derive(Debug, Clone)]
pub struct ListOffsetsResponse {
    data: ListOffsetsResponseData,
}

impl ListOffsetsResponse {
    /// Creates a new `ListOffsetsResponse` from the underlying data.
    pub fn new(data: ListOffsetsResponseData) -> Self {
        Self { data }
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::LIST_OFFSETS
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &ListOffsetsResponseData {
        &self.data
    }

    /// Returns the throttle time in milliseconds.
    pub fn throttle_time_ms(&self) -> i32 {
        self.data.throttle_time_ms
    }

    /// Sets the throttle time in the response.
    pub fn maybe_set_throttle_time_ms(&mut self, throttle_time_ms: i32) {
        self.data.set_throttle_time_ms(throttle_time_ms);
    }

    /// Returns the per-topic response list.
    pub fn topics(&self) -> &[ListOffsetsTopicResponse] {
        &self.data.topics
    }

    /// Returns the error counts aggregated across all partition responses.
    pub fn error_counts(&self) -> HashMap<Errors, i32> {
        let mut counts = HashMap::new();
        for topic in &self.data.topics {
            for partition in &topic.partitions {
                update_error_counts(&mut counts, Errors::for_code(partition.error_code));
            }
        }
        counts
    }

    /// Parses a `ListOffsetsResponse` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = ListOffsetsResponseData::read(readable, version)?;
        Ok(Self::new(data))
    }

    /// Whether the client should throttle on this response (v3+).
    pub fn should_client_throttle(&self, version: i16) -> bool {
        version >= 3
    }

    /// Builds a single-topic / single-partition `ListOffsetsTopicResponse`
    /// for tests and synthetic responses.
    ///
    /// Mirrors `ListOffsetsResponse.singletonListOffsetsTopicResponse(...)`.
    pub fn singleton_list_offsets_topic_response(
        tp: &TopicPartition,
        error: Errors,
        timestamp: i64,
        offset: i64,
        epoch: i32,
    ) -> ListOffsetsTopicResponse {
        let mut partition = ListOffsetsPartitionResponse::new();
        partition.set_partition_index(tp.partition());
        partition.set_error_code(error.code());
        partition.set_timestamp(timestamp);
        partition.set_offset(offset);
        partition.set_leader_epoch(epoch);
        let mut topic_response = ListOffsetsTopicResponse::new();
        topic_response.set_name(tp.topic().to_string());
        topic_response.set_partitions(vec![partition]);
        topic_response
    }
}

impl std::fmt::Display for ListOffsetsResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verifies that `singleton_list_offsets_topic_response` builds a topic
    /// response with the expected field values.
    #[test]
    fn singleton_list_offsets_topic_response_builds_expected_fields() {
        let tp = TopicPartition::new("topic-a".to_string(), 3);
        let topic = ListOffsetsResponse::singleton_list_offsets_topic_response(&tp, Errors::None, 100, 50, 4);
        assert_eq!(topic.name, "topic-a");
        assert_eq!(topic.partitions.len(), 1);
        let p = &topic.partitions[0];
        assert_eq!(p.partition_index, 3);
        assert_eq!(p.error_code, Errors::None.code());
        assert_eq!(p.timestamp, 100);
        assert_eq!(p.offset, 50);
        assert_eq!(p.leader_epoch, 4);
    }

    /// Verifies that `error_counts` aggregates across topics/partitions.
    #[test]
    fn error_counts_aggregates_across_partitions() {
        let mut data = ListOffsetsResponseData::new();
        let mut topic = ListOffsetsTopicResponse::new();
        topic.set_name("t".to_string());
        let mut p0 = ListOffsetsPartitionResponse::new();
        p0.set_partition_index(0);
        p0.set_error_code(Errors::NotLeaderOrFollower.code());
        let mut p1 = ListOffsetsPartitionResponse::new();
        p1.set_partition_index(1);
        p1.set_error_code(Errors::NotLeaderOrFollower.code());
        let mut p2 = ListOffsetsPartitionResponse::new();
        p2.set_partition_index(2);
        p2.set_error_code(Errors::None.code());
        topic.partitions = vec![p0, p1, p2];
        data.set_topics(vec![topic]);

        let response = ListOffsetsResponse::new(data);
        let counts = response.error_counts();
        assert_eq!(counts.get(&Errors::NotLeaderOrFollower).copied().unwrap_or(0), 2);
        assert_eq!(counts.get(&Errors::None).copied().unwrap_or(0), 1);
    }

    /// Verifies `should_client_throttle` returns true only for v3+.
    #[test]
    fn should_client_throttle_v3_threshold() {
        let response = ListOffsetsResponse::new(ListOffsetsResponseData::new());
        assert!(!response.should_client_throttle(2));
        assert!(response.should_client_throttle(3));
        assert!(response.should_client_throttle(10));
    }
}
