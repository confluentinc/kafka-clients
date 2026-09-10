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

//! `TxnOffsetCommit` response handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.TxnOffsetCommitResponse`.
//!
//! Errors are reported per partition; there is no top-level error code at any
//! version.
//!
//! Possible error codes:
//!  - `CoordinatorLoadInProgress` (14)
//!  - `CoordinatorNotAvailable` (15)
//!  - `NotCoordinator` (16)
//!  - `OffsetMetadataTooLarge` (12)
//!  - `InvalidCommitOffsetSize` (28)
//!  - `GroupAuthorizationFailed` (30)
//!  - `InvalidProducerEpoch` (47)
//!  - `TransactionalIdAuthorizationFailed` (53)
//!  - `UnsupportedForMessageFormat` (43)
//!  - `RequestTimedOut` (7)
//!  - `UnknownMemberId` (25)
//!  - `FencedInstanceId` (82)
//!  - `IllegalGeneration` (22)
//!
//! # Scope
//!
//! Java's nested `TxnOffsetCommitResponse.Builder` (`addPartition`,
//! `addPartitions`, `merge`, `build`) is not translated: its only non-test caller
//! is `core/.../KafkaApis.scala`, i.e. broker-side response assembly. Same
//! reasoning as the broker-side members omitted from `AddPartitionsToTxnRequest`
//! (see `design/history/Milestone-11/PLAN.md` §10.2).

use std::collections::HashMap;
use std::io;

use crate::common::TopicPartition;
use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::txn_offset_commit_response_data::{
    TxnOffsetCommitResponseData, TxnOffsetCommitResponsePartition, TxnOffsetCommitResponseTopic,
};

use super::abstract_response::update_error_counts;

/// A `TxnOffsetCommit` response.
///
/// Corresponds to `org.apache.kafka.common.requests.TxnOffsetCommitResponse`.
#[derive(Debug, Clone)]
pub struct TxnOffsetCommitResponse {
    data: TxnOffsetCommitResponseData,
}

impl TxnOffsetCommitResponse {
    /// Creates a new `TxnOffsetCommitResponse` from the underlying data.
    pub fn new_data(data: TxnOffsetCommitResponseData) -> Self {
        Self { data }
    }

    /// Builds a response from a per-partition error map.
    ///
    /// Corresponds to Java's second constructor,
    /// `TxnOffsetCommitResponse(int requestThrottleMs, Map<TopicPartition, Errors>)`.
    /// Java groups through a `HashMap` and so has unspecified order; this sorts by
    /// topic name and then partition index for a deterministic encoding — see
    /// `.claude/rules/producer-transactions.md` §10.
    pub fn new_request_throttle_ms_response_data(
        request_throttle_ms: i32,
        response_data: &HashMap<TopicPartition, Errors>,
    ) -> Self {
        let mut by_topic: HashMap<&str, Vec<(i32, Errors)>> = HashMap::new();
        for (topic_partition, error) in response_data {
            by_topic
                .entry(topic_partition.topic())
                .or_default()
                .push((topic_partition.partition(), *error));
        }

        let mut names: Vec<&str> = by_topic.keys().copied().collect();
        names.sort_unstable();

        let topics = names
            .into_iter()
            .map(|name| {
                let mut entries = by_topic[name].clone();
                entries.sort_unstable_by_key(|(index, _)| *index);

                let partitions = entries
                    .into_iter()
                    .map(|(index, error)| {
                        let mut partition = TxnOffsetCommitResponsePartition::new();
                        partition.set_partition_index(index).set_error_code(error.code());
                        partition
                    })
                    .collect();

                let mut topic = TxnOffsetCommitResponseTopic::new();
                topic.set_name(name.to_string()).set_partitions(partitions);
                topic
            })
            .collect();

        let mut data = TxnOffsetCommitResponseData::new();
        data.set_topics(topics).set_throttle_time_ms(request_throttle_ms);
        Self::new_data(data)
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::TXN_OFFSET_COMMIT
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &TxnOffsetCommitResponseData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut TxnOffsetCommitResponseData {
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

    /// Whether the client should throttle upon receiving this response.
    ///
    /// Returns `true` for v1+.
    pub fn should_client_throttle(&self, version: i16) -> bool {
        version >= 1
    }

    /// Per-partition errors.
    ///
    /// Corresponds to Java's `errors()`.
    pub fn errors(&self) -> HashMap<TopicPartition, Errors> {
        let mut error_map = HashMap::new();
        for topic in &self.data.topics {
            for partition in &topic.partitions {
                error_map.insert(
                    TopicPartition::new(topic.name.clone(), partition.partition_index),
                    Errors::for_code(partition.error_code),
                );
            }
        }
        error_map
    }

    /// Returns error counts by [`Errors`].
    ///
    /// Counts every partition's error. Unlike most responses there is no
    /// top-level code to fold in.
    pub fn error_counts(&self) -> HashMap<Errors, i32> {
        let mut counts = HashMap::new();
        for topic in &self.data.topics {
            for partition in &topic.partitions {
                update_error_counts(&mut counts, Errors::for_code(partition.error_code));
            }
        }
        counts
    }

    /// Parses a `TxnOffsetCommitResponse` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = TxnOffsetCommitResponseData::read(readable, version)?;
        Ok(Self::new_data(data))
    }
}

impl std::fmt::Display for TxnOffsetCommitResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tp(topic: &str, partition: i32) -> TopicPartition {
        TopicPartition::new(topic.to_string(), partition)
    }

    #[test]
    fn test_from_error_map_and_errors_round_trip() {
        let expected = HashMap::from([
            (tp("topic-a", 0), Errors::None),
            (tp("topic-a", 3), Errors::OffsetMetadataTooLarge),
            (tp("topic-b", 1), Errors::NotCoordinator),
        ]);

        let response = TxnOffsetCommitResponse::new_request_throttle_ms_response_data(19, &expected);
        assert_eq!(response.throttle_time_ms(), 19);
        assert_eq!(response.errors(), expected);
    }

    /// Grouping is deterministic (rules §10): topics by name, partitions by index.
    #[test]
    fn test_from_error_map_is_deterministic() {
        let mut map = HashMap::new();
        for (topic, partition) in [("topic-b", 5), ("topic-a", 9), ("topic-b", 1), ("topic-a", 0)] {
            map.insert(tp(topic, partition), Errors::None);
        }

        let response = TxnOffsetCommitResponse::new_request_throttle_ms_response_data(0, &map);
        let topics = &response.data().topics;
        assert_eq!(topics[0].name, "topic-a");
        assert_eq!(
            topics[0].partitions.iter().map(|p| p.partition_index).collect::<Vec<_>>(),
            vec![0, 9]
        );
        assert_eq!(topics[1].name, "topic-b");
        assert_eq!(
            topics[1].partitions.iter().map(|p| p.partition_index).collect::<Vec<_>>(),
            vec![1, 5]
        );
    }

    #[test]
    fn test_error_counts_aggregates_across_topics() {
        let map = HashMap::from([
            (tp("topic-a", 0), Errors::NotCoordinator),
            (tp("topic-a", 1), Errors::NotCoordinator),
            (tp("topic-b", 0), Errors::None),
        ]);
        let response = TxnOffsetCommitResponse::new_request_throttle_ms_response_data(0, &map);

        let counts = response.error_counts();
        assert_eq!(counts.get(&Errors::NotCoordinator), Some(&2));
        assert_eq!(counts.get(&Errors::None), Some(&1));
        assert_eq!(counts.len(), 2);
    }

    /// This response has no top-level error code, so an empty topic list yields
    /// no counts at all — unlike most responses, which would still count `None`.
    #[test]
    fn test_error_counts_is_empty_when_no_partitions() {
        let response = TxnOffsetCommitResponse::new_data(TxnOffsetCommitResponseData::new());
        assert!(response.error_counts().is_empty());
        assert!(response.errors().is_empty());
    }

    #[test]
    fn test_throttle_time_round_trip() {
        let mut response = TxnOffsetCommitResponse::new_data(TxnOffsetCommitResponseData::new());
        assert_eq!(response.throttle_time_ms(), 0);
        response.maybe_set_throttle_time_ms(88);
        assert_eq!(response.throttle_time_ms(), 88);
    }

    #[test]
    fn test_should_client_throttle() {
        let response = TxnOffsetCommitResponse::new_data(TxnOffsetCommitResponseData::new());
        assert!(!response.should_client_throttle(0));
        assert!(response.should_client_throttle(1));
        assert!(response.should_client_throttle(ApiKeys::TXN_OFFSET_COMMIT.latest_version()));
    }

    #[test]
    fn test_api_key() {
        let response = TxnOffsetCommitResponse::new_data(TxnOffsetCommitResponseData::new());
        assert_eq!(response.api_key(), &ApiKeys::TXN_OFFSET_COMMIT);
    }

    /// Translated from
    /// `TxnOffsetCommitResponseTest.testConstructorWithErrorResponse`.
    #[test]
    fn test_constructor_with_error_response() {
        const THROTTLE_TIME_MS: i32 = 10;
        let errors_map = HashMap::from([
            (tp("topic-a", 1), Errors::CoordinatorNotAvailable),
            (tp("topic-b", 2), Errors::NotCoordinator),
        ]);

        let response = TxnOffsetCommitResponse::new_request_throttle_ms_response_data(THROTTLE_TIME_MS, &errors_map);

        assert_eq!(response.errors(), errors_map);
        let counts = response.error_counts();
        assert_eq!(counts.get(&Errors::CoordinatorNotAvailable), Some(&1));
        assert_eq!(counts.get(&Errors::NotCoordinator), Some(&1));
        assert_eq!(counts.len(), 2);
        assert_eq!(response.throttle_time_ms(), THROTTLE_TIME_MS);
    }

    /// Translated from `TxnOffsetCommitResponseTest.testParse`.
    ///
    /// Java builds the data directly (two topics, one partition each, different
    /// errors), encodes it via `MessageUtil.toByteBufferAccessor`, then parses and
    /// asserts at every version. `@ApiKeyVersionsSource`-style loops become Rust
    /// loops per DoD §3.
    #[test]
    fn test_parse() {
        use super::super::ConcreteResponse;

        const THROTTLE_TIME_MS: i32 = 10;
        let error_one = Errors::CoordinatorNotAvailable;
        let error_two = Errors::NotCoordinator;

        for version in ApiKeys::TXN_OFFSET_COMMIT.oldest_version()..=ApiKeys::TXN_OFFSET_COMMIT.latest_version() {
            let topics = [("topic-a", 1, error_one), ("topic-b", 2, error_two)]
                .into_iter()
                .map(|(name, index, error)| {
                    let mut partition = TxnOffsetCommitResponsePartition::new();
                    partition.set_partition_index(index).set_error_code(error.code());
                    let mut topic = TxnOffsetCommitResponseTopic::new();
                    topic.set_name(name.to_string()).set_partitions(vec![partition]);
                    topic
                })
                .collect();

            let mut data = TxnOffsetCommitResponseData::new();
            data.set_throttle_time_ms(THROTTLE_TIME_MS).set_topics(topics);

            let mut concrete = ConcreteResponse::TxnOffsetCommit(TxnOffsetCommitResponse::new_data(data));
            let mut buffer = concrete.serialize(version).expect("serialize");
            buffer.flip();
            let response = TxnOffsetCommitResponse::parse(&mut buffer, version).expect("parse");

            let counts = response.error_counts();
            assert_eq!(counts.get(&error_one), Some(&1), "v{version}");
            assert_eq!(counts.get(&error_two), Some(&1), "v{version}");
            assert_eq!(counts.len(), 2, "v{version}");
            assert_eq!(response.throttle_time_ms(), THROTTLE_TIME_MS, "v{version}");
            assert_eq!(response.should_client_throttle(version), version >= 1, "v{version}");
        }
    }

    #[test]
    fn test_serialization_round_trip_all_versions() {
        use super::super::ConcreteResponse;

        let map = HashMap::from([
            (tp("topic-a", 0), Errors::None),
            (tp("topic-b", 2), Errors::IllegalGeneration),
        ]);

        for version in ApiKeys::TXN_OFFSET_COMMIT.oldest_version()..=ApiKeys::TXN_OFFSET_COMMIT.latest_version() {
            let response = TxnOffsetCommitResponse::new_request_throttle_ms_response_data(11, &map);
            let mut concrete = ConcreteResponse::TxnOffsetCommit(response);
            let mut buffer = concrete.serialize(version).expect("serialize");
            buffer.flip();
            let parsed = TxnOffsetCommitResponse::parse(&mut buffer, version).expect("parse");

            assert_eq!(parsed.throttle_time_ms(), 11, "v{version}");
            assert_eq!(parsed.errors(), map, "v{version}");
        }
    }
}
