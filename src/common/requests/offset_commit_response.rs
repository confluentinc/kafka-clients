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

//! `OffsetCommit` response handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.OffsetCommitResponse`.
//!
//! Possible error codes:
//! - `UNKNOWN_TOPIC_OR_PARTITION`
//! - `REQUEST_TIMED_OUT`
//! - `OFFSET_METADATA_TOO_LARGE`
//! - `COORDINATOR_LOAD_IN_PROGRESS`
//! - `COORDINATOR_NOT_AVAILABLE`
//! - `NOT_COORDINATOR`
//! - `ILLEGAL_GENERATION`
//! - `UNKNOWN_MEMBER_ID`
//! - `REBALANCE_IN_PROGRESS`
//! - `INVALID_COMMIT_OFFSET_SIZE`
//! - `TOPIC_AUTHORIZATION_FAILED`
//! - `GROUP_AUTHORIZATION_FAILED`
//! - `STALE_MEMBER_EPOCH`

use std::collections::HashMap;
use std::io;

use crate::OffsetCommitResponseData;
use crate::common::TopicPartition;
use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::offset_commit_response_data::{OffsetCommitResponsePartition, OffsetCommitResponseTopic};

use super::AbstractResponse;

/// An `OffsetCommit` response.
///
/// Corresponds to `org.apache.kafka.common.requests.OffsetCommitResponse`.
#[derive(Debug, Clone)]
pub struct OffsetCommitResponse {
    data: OffsetCommitResponseData,
}

impl OffsetCommitResponse {
    /// Creates a new `OffsetCommitResponse` from the underlying data.
    ///
    /// Mirrors Java's constructor `OffsetCommitResponse(OffsetCommitResponseData)`.
    pub fn with_data(data: OffsetCommitResponseData) -> Self {
        Self { data }
    }

    /// Constructs a response from a map of `TopicPartition -> Errors`, with a
    /// caller-provided throttle time.
    ///
    /// Mirrors Java's `OffsetCommitResponse(int, Map<TopicPartition, Errors>)`.
    pub fn with_throttle_time_ms_response_data(
        throttle_time_ms: i32,
        response_data: &HashMap<TopicPartition, Errors>,
    ) -> Self {
        let mut by_topic: HashMap<String, OffsetCommitResponseTopic> = HashMap::new();
        for (tp, error) in response_data {
            let topic_name = tp.topic().to_string();
            let topic = by_topic.entry(topic_name.clone()).or_insert_with(|| {
                let mut t = OffsetCommitResponseTopic::new();
                t.set_name(topic_name);
                t
            });
            let mut partition = OffsetCommitResponsePartition::new();
            partition.set_partition_index(tp.partition());
            partition.set_error_code(error.code());
            topic.partitions.push(partition);
        }
        let mut data = OffsetCommitResponseData::new();
        data.set_topics(by_topic.into_values().collect());
        data.set_throttle_time_ms(throttle_time_ms);
        Self { data }
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::OFFSET_COMMIT
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &OffsetCommitResponseData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut OffsetCommitResponseData {
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

    /// Returns the per-topic response list.
    pub fn topics(&self) -> &[OffsetCommitResponseTopic] {
        &self.data.topics
    }

    /// Returns the error counts aggregated across all partition responses.
    pub fn error_counts(&self) -> HashMap<Errors, i32> {
        let mut counts = HashMap::new();
        for topic in &self.data.topics {
            for partition in &topic.partitions {
                AbstractResponse::update_error_counts(&mut counts, Errors::for_code(partition.error_code));
            }
        }
        counts
    }

    /// Parses an `OffsetCommitResponse` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = OffsetCommitResponseData::read(readable, version)?;
        Ok(Self::with_data(data))
    }

    /// Whether the client should throttle on this response (v4+).
    pub fn should_client_throttle(&self, version: i16) -> bool {
        version >= 4
    }

    /// Whether the wire protocol uses topic ids at the given version (v10+).
    pub fn use_topic_ids(version: i16) -> bool {
        version >= 10
    }
}

impl std::fmt::Display for OffsetCommitResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `error_counts` aggregates the partition error codes.
    #[test]
    fn error_counts_aggregates_across_partitions() {
        let mut data = OffsetCommitResponseData::new();
        let mut topic = OffsetCommitResponseTopic::new();
        topic.set_name("t".to_string());
        let mut p0 = OffsetCommitResponsePartition::new();
        p0.set_partition_index(0);
        p0.set_error_code(Errors::NotCoordinator.code());
        let mut p1 = OffsetCommitResponsePartition::new();
        p1.set_partition_index(1);
        p1.set_error_code(Errors::NotCoordinator.code());
        let mut p2 = OffsetCommitResponsePartition::new();
        p2.set_partition_index(2);
        p2.set_error_code(Errors::None.code());
        topic.partitions = vec![p0, p1, p2];
        data.set_topics(vec![topic]);

        let response = OffsetCommitResponse::with_data(data);
        let counts = response.error_counts();
        assert_eq!(counts.get(&Errors::NotCoordinator).copied().unwrap_or(0), 2);
        assert_eq!(counts.get(&Errors::None).copied().unwrap_or(0), 1);
    }

    /// `should_client_throttle` returns true only for v4+.
    #[test]
    fn should_client_throttle_v4_threshold() {
        let response = OffsetCommitResponse::with_data(OffsetCommitResponseData::new());
        assert!(!response.should_client_throttle(3));
        assert!(response.should_client_throttle(4));
        assert!(response.should_client_throttle(10));
    }

    /// `use_topic_ids` returns true only for v10+.
    #[test]
    fn use_topic_ids_v10_threshold() {
        assert!(!OffsetCommitResponse::use_topic_ids(9));
        assert!(OffsetCommitResponse::use_topic_ids(10));
    }

    /// `with_throttle_time_ms_response_data` builds the per-topic / per-partition structure
    /// with the caller-provided throttle time.
    #[test]
    fn from_response_data_populates_topics() {
        let mut input: HashMap<TopicPartition, Errors> = HashMap::new();
        input.insert(TopicPartition::new("t".to_string(), 0), Errors::None);
        input.insert(TopicPartition::new("t".to_string(), 1), Errors::IllegalGeneration);
        let response = OffsetCommitResponse::with_throttle_time_ms_response_data(42, &input);
        assert_eq!(response.throttle_time_ms(), 42);
        // Both partitions are under one "t" topic.
        let topic = response.topics().iter().find(|t| t.name == "t").expect("topic present");
        assert_eq!(topic.partitions.len(), 2);
    }

    /// `maybe_set_throttle_time_ms` propagates the new value.
    #[test]
    fn maybe_set_throttle_time_ms_updates_field() {
        let mut response = OffsetCommitResponse::with_data(OffsetCommitResponseData::new());
        response.maybe_set_throttle_time_ms(123);
        assert_eq!(response.throttle_time_ms(), 123);
    }
}
