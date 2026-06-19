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

//! `OffsetsForLeaderEpoch` client helpers.
//!
//! Corresponds to `org.apache.kafka.clients.consumer.internals.OffsetsForLeaderEpochClient`
//! and the static helpers in `OffsetsForLeaderEpochUtils`.
//!
//! The Java `OffsetsForLeaderEpochClient` extends `AsyncClient` and is used
//! by the classic consumer's `ConsumerNetworkClient`. For the KIP-848
//! consumer the equivalent helpers — `prepareRequest` and
//! `handleResponse` — live in `OffsetsForLeaderEpochUtils`. The Rust
//! translation collapses them onto a single zero-sized type because the
//! `OffsetsRequestManager` calls them directly without an
//! `AsyncClient`-style dispatch layer.

#![allow(dead_code)]

use std::collections::{HashMap, HashSet};

use crate::common::TopicPartition;
use crate::common::kafka_error::TopicAuthorizationError;
use crate::common::protocol::Errors;
use crate::common::record::RecordBatch;
use crate::common::requests::OffsetsForLeaderEpochResponse;
use crate::common::requests::offsets_for_leader_epoch_request::OffsetsForLeaderEpochRequestBuilder;
use crate::offset_for_leader_epoch_request_data::{OffsetForLeaderPartition, OffsetForLeaderTopic};
use crate::offset_for_leader_epoch_response_data::EpochEndOffset;

use super::subscription_state::FetchPosition;

/// Result of handling an `OffsetsForLeaderEpoch` response.
///
/// Mirrors Java's `OffsetsForLeaderEpochUtils.OffsetForEpochResult` —
/// successful partitions appear in `end_offsets`; partitions whose error
/// should be retried via metadata refresh appear in `partitions_to_retry`.
#[derive(Debug, Default)]
pub(crate) struct OffsetForEpochResult {
    /// Successful epoch-end-offset entries, keyed by partition.
    pub end_offsets: HashMap<TopicPartition, EpochEndOffset>,
    /// Partitions whose response indicated a retriable error.
    pub partitions_to_retry: HashSet<TopicPartition>,
}

impl OffsetForEpochResult {
    pub(crate) fn new(
        end_offsets: HashMap<TopicPartition, EpochEndOffset>,
        partitions_to_retry: HashSet<TopicPartition>,
    ) -> Self {
        Self { end_offsets, partitions_to_retry }
    }

    pub(crate) fn end_offsets(&self) -> &HashMap<TopicPartition, EpochEndOffset> {
        &self.end_offsets
    }

    pub(crate) fn partitions_to_retry(&self) -> &HashSet<TopicPartition> {
        &self.partitions_to_retry
    }
}

/// `OffsetsForLeaderEpoch` client — namespace for the per-request helpers.
///
/// Zero-sized; the Java equivalent's `client` and `logContext` fields
/// don't carry per-request state and aren't needed in the KIP-848 wiring.
pub(crate) struct OffsetsForLeaderEpochClient;

impl OffsetsForLeaderEpochClient {
    /// Builds a consumer-side `OffsetsForLeaderEpoch` request builder from
    /// `(TopicPartition, FetchPosition)` entries that carry an offset epoch.
    ///
    /// Mirrors `OffsetsForLeaderEpochUtils.prepareRequest(Map<TopicPartition,FetchPosition>)`.
    ///
    /// Entries without an `offset_epoch` are skipped, matching Java's
    /// `fetchPosition.offsetEpoch.ifPresent(...)`.
    pub(crate) fn prepare_request(
        request_data: &HashMap<TopicPartition, FetchPosition>,
    ) -> OffsetsForLeaderEpochRequestBuilder {
        let mut topics: HashMap<String, OffsetForLeaderTopic> = HashMap::new();
        for (topic_partition, fetch_position) in request_data {
            let Some(fetch_epoch) = fetch_position.offset_epoch else {
                continue;
            };
            let topic_entry = topics.entry(topic_partition.topic().to_string()).or_insert_with(|| {
                let mut t = OffsetForLeaderTopic::new();
                t.set_topic(topic_partition.topic().to_string());
                t
            });
            let mut partition = OffsetForLeaderPartition::new();
            partition.set_partition(topic_partition.partition());
            partition.set_leader_epoch(fetch_epoch);
            partition.set_current_leader_epoch(
                fetch_position
                    .current_leader
                    .epoch
                    .unwrap_or(RecordBatch::NO_PARTITION_LEADER_EPOCH),
            );
            topic_entry.partitions.push(partition);
        }
        OffsetsForLeaderEpochRequestBuilder::for_consumer(topics.into_values().collect())
    }

    /// Processes an `OffsetsForLeaderEpoch` response.
    ///
    /// Returns an [`OffsetForEpochResult`] with successful epoch-end-offset
    /// entries and the set of partitions whose response indicated a
    /// retriable error.
    ///
    /// # Errors
    ///
    /// Returns [`TopicAuthorizationError`] if any partition response
    /// carried `TOPIC_AUTHORIZATION_FAILED`. Mirrors Java's `throw`.
    ///
    /// Mirrors `OffsetsForLeaderEpochUtils.handleResponse(Map<TopicPartition,FetchPosition>, OffsetsForLeaderEpochResponse)`.
    pub(crate) fn handle_response(
        request_data: &HashMap<TopicPartition, FetchPosition>,
        response: &OffsetsForLeaderEpochResponse,
    ) -> Result<OffsetForEpochResult, TopicAuthorizationError> {
        let mut partitions_to_retry: HashSet<TopicPartition> = request_data.keys().cloned().collect();
        let mut unauthorized_topics: HashSet<String> = HashSet::new();
        let mut end_offsets: HashMap<TopicPartition, EpochEndOffset> = HashMap::new();

        for topic in &response.data().topics {
            for partition in &topic.partitions {
                let topic_partition = TopicPartition::new(topic.topic.clone(), partition.partition);
                if !request_data.contains_key(&topic_partition) {
                    log::warn!(
                        "Received unrequested topic or partition {} from response, ignoring.",
                        topic_partition,
                    );
                    continue;
                }
                let error = Errors::for_code(partition.error_code);
                match error {
                    Errors::None => {
                        log::debug!(
                            "Handling OffsetsForLeaderEpoch response for {}. Got offset {} for epoch {}.",
                            topic_partition,
                            partition.end_offset,
                            partition.leader_epoch,
                        );
                        end_offsets.insert(topic_partition.clone(), partition.clone());
                        partitions_to_retry.remove(&topic_partition);
                    },
                    Errors::NotLeaderOrFollower
                    | Errors::ReplicaNotAvailable
                    | Errors::KafkaStorageError
                    | Errors::OffsetNotAvailable
                    | Errors::LeaderNotAvailable
                    | Errors::FencedLeaderEpoch
                    | Errors::UnknownLeaderEpoch => {
                        log::debug!(
                            "Attempt to fetch offsets for partition {} failed due to {}, retrying.",
                            topic_partition,
                            error,
                        );
                    },
                    Errors::UnknownTopicOrPartition => {
                        log::warn!(
                            "Received unknown topic or partition error in OffsetsForLeaderEpoch request for partition {}.",
                            topic_partition,
                        );
                    },
                    Errors::TopicAuthorizationFailed => {
                        unauthorized_topics.insert(topic_partition.topic().to_string());
                        partitions_to_retry.remove(&topic_partition);
                    },
                    _ => {
                        log::warn!(
                            "Attempt to fetch offsets for partition {} failed due to: {}, retrying.",
                            topic_partition,
                            error.message(),
                        );
                    },
                }
            }
        }

        if !unauthorized_topics.is_empty() {
            return Err(TopicAuthorizationError::new(unauthorized_topics));
        }
        Ok(OffsetForEpochResult::new(end_offsets, partitions_to_retry))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Node;
    use crate::common::requests::abstract_request::RequestBuilder;
    use crate::metadata::LeaderAndEpoch;
    use crate::offset_for_leader_epoch_response_data::{OffsetForLeaderEpochResponseData, OffsetForLeaderTopicResult};

    fn fetch_position_with_epoch(offset: i64, offset_epoch: i32, current_epoch: i32) -> FetchPosition {
        let leader = Node::new(1, "host".to_string(), 9092);
        FetchPosition::with_leader(
            offset,
            Some(offset_epoch),
            LeaderAndEpoch::new(Some(leader), Some(current_epoch)),
        )
    }

    /// Verifies that `prepare_request` groups by topic, populates partition
    /// data, and skips entries without an offset epoch.
    #[test]
    fn prepare_request_groups_by_topic_and_skips_no_epoch() {
        let tp_a0 = TopicPartition::new("topic-a".to_string(), 0);
        let tp_a1 = TopicPartition::new("topic-a".to_string(), 1);
        let tp_b0 = TopicPartition::new("topic-b".to_string(), 0);
        let tp_no_epoch = TopicPartition::new("topic-c".to_string(), 0);

        let mut data = HashMap::new();
        data.insert(tp_a0, fetch_position_with_epoch(10, 3, 5));
        data.insert(tp_a1, fetch_position_with_epoch(20, 4, 5));
        data.insert(tp_b0, fetch_position_with_epoch(30, 2, 7));
        // No offset epoch — must be skipped.
        let no_epoch = FetchPosition::with_leader(0, None, LeaderAndEpoch::no_leader_or_epoch());
        data.insert(tp_no_epoch, no_epoch);

        let builder = OffsetsForLeaderEpochClient::prepare_request(&data);
        let topics = &builder.data().topics;
        assert_eq!(topics.len(), 2);
        let topic_a = topics.iter().find(|t| t.topic == "topic-a").expect("topic-a present");
        assert_eq!(topic_a.partitions.len(), 2);
        let topic_b = topics.iter().find(|t| t.topic == "topic-b").expect("topic-b present");
        assert_eq!(topic_b.partitions.len(), 1);
        assert_eq!(topic_b.partitions[0].leader_epoch, 2);
        assert_eq!(topic_b.partitions[0].current_leader_epoch, 7);
        assert_eq!(builder.api_key(), &crate::common::protocol::ApiKeys::OFFSET_FOR_LEADER_EPOCH);
    }

    /// Verifies `handle_response` returns end offsets for successful entries
    /// and removes them from the retry set.
    #[test]
    fn handle_response_extracts_end_offsets() {
        let tp = TopicPartition::new("t".to_string(), 0);
        let mut request_data = HashMap::new();
        request_data.insert(tp.clone(), fetch_position_with_epoch(10, 3, 5));

        let mut response_data = OffsetForLeaderEpochResponseData::new();
        let mut topic_result = OffsetForLeaderTopicResult::new();
        topic_result.set_topic("t".to_string());
        let mut eeo = EpochEndOffset::new();
        eeo.set_partition(0);
        eeo.set_error_code(Errors::None.code());
        eeo.set_leader_epoch(4);
        eeo.set_end_offset(100);
        topic_result.set_partitions(vec![eeo]);
        response_data.set_topics(vec![topic_result]);

        let response = OffsetsForLeaderEpochResponse::new(response_data);
        let result = OffsetsForLeaderEpochClient::handle_response(&request_data, &response).expect("ok");
        assert_eq!(result.end_offsets().get(&tp).map(|e| e.end_offset), Some(100));
        assert!(result.partitions_to_retry().is_empty());
    }

    /// Verifies `handle_response` keeps retriable-error partitions in
    /// `partitions_to_retry`.
    #[test]
    fn handle_response_marks_retriable_errors() {
        let tp = TopicPartition::new("t".to_string(), 0);
        let mut request_data = HashMap::new();
        request_data.insert(tp.clone(), fetch_position_with_epoch(10, 3, 5));

        let mut response_data = OffsetForLeaderEpochResponseData::new();
        let mut topic_result = OffsetForLeaderTopicResult::new();
        topic_result.set_topic("t".to_string());
        let mut eeo = EpochEndOffset::new();
        eeo.set_partition(0);
        eeo.set_error_code(Errors::NotLeaderOrFollower.code());
        topic_result.set_partitions(vec![eeo]);
        response_data.set_topics(vec![topic_result]);

        let response = OffsetsForLeaderEpochResponse::new(response_data);
        let result = OffsetsForLeaderEpochClient::handle_response(&request_data, &response).expect("ok");
        assert!(result.end_offsets().is_empty());
        assert!(result.partitions_to_retry().contains(&tp));
    }

    /// Verifies `handle_response` raises a `TopicAuthorizationError` when
    /// any partition response carries `TOPIC_AUTHORIZATION_FAILED`.
    #[test]
    fn handle_response_raises_topic_auth_exception() {
        let tp = TopicPartition::new("t".to_string(), 0);
        let mut request_data = HashMap::new();
        request_data.insert(tp.clone(), fetch_position_with_epoch(10, 3, 5));

        let mut response_data = OffsetForLeaderEpochResponseData::new();
        let mut topic_result = OffsetForLeaderTopicResult::new();
        topic_result.set_topic("t".to_string());
        let mut eeo = EpochEndOffset::new();
        eeo.set_partition(0);
        eeo.set_error_code(Errors::TopicAuthorizationFailed.code());
        topic_result.set_partitions(vec![eeo]);
        response_data.set_topics(vec![topic_result]);

        let response = OffsetsForLeaderEpochResponse::new(response_data);
        let err = OffsetsForLeaderEpochClient::handle_response(&request_data, &response).expect_err("auth error");
        assert!(err.unauthorized_topics.contains("t"));
    }

    /// Verifies that responses for partitions not in the request are ignored.
    #[test]
    fn handle_response_ignores_unrequested_partitions() {
        let tp = TopicPartition::new("t".to_string(), 0);
        let mut request_data = HashMap::new();
        request_data.insert(tp.clone(), fetch_position_with_epoch(10, 3, 5));

        let mut response_data = OffsetForLeaderEpochResponseData::new();
        let mut topic_result = OffsetForLeaderTopicResult::new();
        topic_result.set_topic("t".to_string());
        let mut requested_partition_result = EpochEndOffset::new();
        requested_partition_result.set_partition(0);
        requested_partition_result.set_error_code(Errors::None.code());
        requested_partition_result.set_end_offset(100);
        let mut unrequested_partition_result = EpochEndOffset::new();
        unrequested_partition_result.set_partition(99);
        unrequested_partition_result.set_error_code(Errors::None.code());
        unrequested_partition_result.set_end_offset(200);
        topic_result.set_partitions(vec![requested_partition_result, unrequested_partition_result]);
        response_data.set_topics(vec![topic_result]);

        let response = OffsetsForLeaderEpochResponse::new(response_data);
        let result = OffsetsForLeaderEpochClient::handle_response(&request_data, &response).expect("ok");
        assert_eq!(result.end_offsets().len(), 1);
        assert!(result.end_offsets().contains_key(&tp));
    }

    /// Java parity: `testEmptyResponse`. An empty request (no partitions)
    /// paired with an empty response yields empty `end_offsets` and empty
    /// `partitions_to_retry` — `partitions_to_retry` seeds from the request
    /// keys, so with no requested partitions there is nothing to retry.
    /// Distinct from `testUnexpectedEmptyResponse`, which requests a
    /// partition that is then absent from the response.
    #[test]
    fn handle_response_empty_request_and_response_are_both_empty() {
        let request_data: HashMap<TopicPartition, FetchPosition> = HashMap::new();
        let response = OffsetsForLeaderEpochResponse::new(OffsetForLeaderEpochResponseData::new());
        let result = OffsetsForLeaderEpochClient::handle_response(&request_data, &response).expect("ok");
        assert!(
            result.partitions_to_retry().is_empty(),
            "no requested partitions ⇒ nothing to retry"
        );
        assert!(result.end_offsets().is_empty(), "empty response ⇒ no end offsets");
    }

    /// Java parity: `testUnexpectedEmptyResponse`. A partition that was
    /// requested but is ABSENT from the response must remain in
    /// `partitions_to_retry` (distinct code path from "unrequested partition
    /// ignored": here the requested key is never removed because no response
    /// entry references it).
    #[test]
    fn handle_response_requested_partition_absent_stays_in_retry() {
        let tp = TopicPartition::new("topic".to_string(), 0);
        let mut request_data = HashMap::new();
        request_data.insert(tp.clone(), fetch_position_with_epoch(0, 1, 1));

        // Empty response — the requested partition is not present.
        let response = OffsetsForLeaderEpochResponse::new(OffsetForLeaderEpochResponseData::new());
        let result = OffsetsForLeaderEpochClient::handle_response(&request_data, &response).expect("ok");
        assert!(result.end_offsets().is_empty(), "no end offsets in an empty response");
        assert!(
            result.partitions_to_retry().contains(&tp),
            "requested-but-absent partition must stay in partitions_to_retry"
        );
    }
}
