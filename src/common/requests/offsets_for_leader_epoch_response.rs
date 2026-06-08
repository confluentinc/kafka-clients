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

//! `OffsetsForLeaderEpoch` response handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse`.
//!
//! Possible error codes:
//! - `TOPIC_AUTHORIZATION_FAILED` — caller lacks DESCRIBE access.
//! - `REPLICA_NOT_AVAILABLE` — request received by a non-replica broker.
//! - `NOT_LEADER_OR_FOLLOWER` / `FENCED_LEADER_EPOCH` / `UNKNOWN_LEADER_EPOCH`
//!   — leader epoch mismatch.
//! - `UNKNOWN_TOPIC_OR_PARTITION` / `KAFKA_STORAGE_ERROR` — metadata or log
//!   directory issues.

use std::collections::HashMap;
use std::io;

use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::offset_for_leader_epoch_response_data::OffsetForLeaderEpochResponseData;

use super::RECORD_BATCH_NO_PARTITION_LEADER_EPOCH;
use super::abstract_response::update_error_counts;

/// Sentinel epoch-offset used when the broker has no usable epoch.
pub const UNDEFINED_EPOCH_OFFSET: i64 = RECORD_BATCH_NO_PARTITION_LEADER_EPOCH as i64;
/// Sentinel leader-epoch used when the broker has no usable epoch.
pub const UNDEFINED_EPOCH: i32 = RECORD_BATCH_NO_PARTITION_LEADER_EPOCH;

/// An `OffsetsForLeaderEpoch` response.
///
/// Corresponds to `org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse`.
#[derive(Debug, Clone)]
pub struct OffsetsForLeaderEpochResponse {
    data: OffsetForLeaderEpochResponseData,
}

impl OffsetsForLeaderEpochResponse {
    /// Creates a new `OffsetsForLeaderEpochResponse` from the underlying data.
    pub fn new(data: OffsetForLeaderEpochResponseData) -> Self {
        Self { data }
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::OFFSET_FOR_LEADER_EPOCH
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &OffsetForLeaderEpochResponseData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut OffsetForLeaderEpochResponseData {
        &mut self.data
    }

    /// Returns the throttle time in milliseconds.
    pub fn throttle_time_ms(&self) -> i32 {
        self.data.throttle_time_ms
    }

    /// Sets the throttle time in the response.
    pub fn maybe_set_throttle_time_ms(&mut self, throttle_time_ms: i32) {
        self.data.set_throttle_time_ms(throttle_time_ms);
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

    /// Returns `false` — `OffsetsForLeaderEpoch` does not signal
    /// client-side throttling (Java `AbstractResponse` default).
    pub fn should_client_throttle(&self, _version: i16) -> bool {
        false
    }

    /// Parses an `OffsetsForLeaderEpochResponse` from a readable buffer at
    /// the given version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = OffsetForLeaderEpochResponseData::read(readable, version)?;
        Ok(Self::new(data))
    }
}

impl std::fmt::Display for OffsetsForLeaderEpochResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::offset_for_leader_epoch_response_data::{EpochEndOffset, OffsetForLeaderTopicResult};

    /// Verifies that `error_counts` aggregates across topics/partitions.
    #[test]
    fn error_counts_aggregates_across_partitions() {
        let mut data = OffsetForLeaderEpochResponseData::new();
        let mut topic = OffsetForLeaderTopicResult::new();
        topic.set_topic("t".to_string());
        let mut p0 = EpochEndOffset::new();
        p0.set_partition(0);
        p0.set_error_code(Errors::NotLeaderOrFollower.code());
        let mut p1 = EpochEndOffset::new();
        p1.set_partition(1);
        p1.set_error_code(Errors::FencedLeaderEpoch.code());
        topic.partitions = vec![p0, p1];
        data.set_topics(vec![topic]);

        let response = OffsetsForLeaderEpochResponse::new(data);
        let counts = response.error_counts();
        assert_eq!(counts.get(&Errors::NotLeaderOrFollower).copied().unwrap_or(0), 1);
        assert_eq!(counts.get(&Errors::FencedLeaderEpoch).copied().unwrap_or(0), 1);
    }

    /// Verifies the constants match Java's `OffsetsForLeaderEpochResponse`.
    #[test]
    fn sentinel_constants() {
        assert_eq!(UNDEFINED_EPOCH, RECORD_BATCH_NO_PARTITION_LEADER_EPOCH);
        assert_eq!(UNDEFINED_EPOCH_OFFSET, RECORD_BATCH_NO_PARTITION_LEADER_EPOCH as i64);
    }
}
