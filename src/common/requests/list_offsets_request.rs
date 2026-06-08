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

//! `ListOffsets` request handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.ListOffsetsRequest`.
//!
//! Wraps the auto-generated [`ListOffsetsRequestData`] and exposes a
//! [`ListOffsetsRequestBuilder`] that picks the right version based on the
//! consumer's options (require timestamp, isolation level, etc.).

use std::collections::{HashMap, HashSet};
use std::io;

use crate::common::IsolationLevel;
use crate::common::TopicPartition;
use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::list_offsets_request_data::{ListOffsetsPartition, ListOffsetsRequestData, ListOffsetsTopic};
use crate::list_offsets_response_data::{
    ListOffsetsPartitionResponse, ListOffsetsResponseData, ListOffsetsTopicResponse,
};

use super::ConcreteResponse;
use super::ListOffsetsResponse;
use super::abstract_request::{ConcreteRequest, RequestBuilder};
use super::list_offsets_response::{UNKNOWN_OFFSET, UNKNOWN_TIMESTAMP};

/// Sentinel timestamp for the earliest available message in the log.
pub const EARLIEST_TIMESTAMP: i64 = -2;
/// Sentinel timestamp for the latest available message in the log.
pub const LATEST_TIMESTAMP: i64 = -1;
/// Sentinel timestamp for the maximum-timestamp message in the log.
pub const MAX_TIMESTAMP: i64 = -3;
/// Sentinel timestamp for the earliest local (non-tiered) message.
pub const EARLIEST_LOCAL_TIMESTAMP: i64 = -4;
/// Sentinel timestamp for the latest tiered-storage message.
pub const LATEST_TIERED_TIMESTAMP: i64 = -5;
/// Sentinel timestamp for the earliest message pending upload to tiered storage.
pub const EARLIEST_PENDING_UPLOAD_TIMESTAMP: i64 = -6;

/// Wire `replica_id` value sent by ordinary consumers.
pub const CONSUMER_REPLICA_ID: i32 = -1;
/// Wire `replica_id` value sent by debugging tools.
pub const DEBUGGING_REPLICA_ID: i32 = -2;

/// A `ListOffsets` request.
///
/// Corresponds to `org.apache.kafka.common.requests.ListOffsetsRequest`.
#[derive(Debug, Clone)]
pub struct ListOffsetsRequest {
    data: ListOffsetsRequestData,
    version: i16,
    duplicate_partitions: HashSet<TopicPartition>,
}

impl ListOffsetsRequest {
    /// Creates a new `ListOffsetsRequest` from data and version.
    ///
    /// Mirrors Java's private constructor that scans the data to build the
    /// duplicate-partitions set.
    pub fn new(data: ListOffsetsRequestData, version: i16) -> Self {
        let mut duplicate_partitions = HashSet::new();
        let mut seen: HashSet<TopicPartition> = HashSet::new();
        for topic in &data.topics {
            for partition in &topic.partitions {
                let tp = TopicPartition::new(topic.name.clone(), partition.partition_index);
                if !seen.insert(tp.clone()) {
                    duplicate_partitions.insert(tp);
                }
            }
        }
        Self { data, version, duplicate_partitions }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &ListOffsetsRequestData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut ListOffsetsRequestData {
        &mut self.data
    }

    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Returns the API key for this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::LIST_OFFSETS
    }

    /// Returns the wire replica id.
    pub fn replica_id(&self) -> i32 {
        self.data.replica_id
    }

    /// Returns the isolation level decoded from the wire field.
    ///
    /// # Errors
    ///
    /// Returns an error if the wire byte does not correspond to a known
    /// isolation level.
    pub fn isolation_level(&self) -> Result<IsolationLevel, crate::common::KafkaError> {
        IsolationLevel::for_id(self.data.isolation_level as u8)
    }

    /// Returns the list of topic-level requests.
    pub fn topics(&self) -> &[ListOffsetsTopic] {
        &self.data.topics
    }

    /// Returns the set of partitions that appear more than once in the data.
    pub fn duplicate_partitions(&self) -> &HashSet<TopicPartition> {
        &self.duplicate_partitions
    }

    /// Returns the request timeout in milliseconds (v10+).
    pub fn timeout_ms(&self) -> i32 {
        self.data.timeout_ms
    }

    /// Builds the canonical error response for this request, matching Java's
    /// `ListOffsetsRequest.getErrorResponse(int, Throwable)`.
    pub fn get_error_response(&self, throttle_time_ms: i32, error: &Errors) -> ConcreteResponse {
        let error_code = error.code();
        let mut response_topics = Vec::with_capacity(self.data.topics.len());
        for topic in &self.data.topics {
            let mut topic_response = ListOffsetsTopicResponse::new();
            topic_response.set_name(topic.name.clone());
            let mut partitions = Vec::with_capacity(topic.partitions.len());
            for partition in &topic.partitions {
                let mut partition_response = ListOffsetsPartitionResponse::new();
                partition_response.set_error_code(error_code);
                partition_response.set_partition_index(partition.partition_index);
                partition_response.set_offset(UNKNOWN_OFFSET);
                partition_response.set_timestamp(UNKNOWN_TIMESTAMP);
                partitions.push(partition_response);
            }
            topic_response.set_partitions(partitions);
            response_topics.push(topic_response);
        }
        let mut data = ListOffsetsResponseData::new();
        data.set_throttle_time_ms(throttle_time_ms);
        data.set_topics(response_topics);
        ConcreteResponse::ListOffsets(ListOffsetsResponse::new(data))
    }

    /// Parses a `ListOffsetsRequest` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = ListOffsetsRequestData::read(readable, version)?;
        Ok(Self::new(data, version))
    }

    /// Groups a `(TopicPartition, ListOffsetsPartition)` map into per-topic
    /// `ListOffsetsTopic` entries, preserving insertion order of new topics
    /// the way Java's `computeIfAbsent` does.
    ///
    /// Mirrors Java's `toListOffsetsTopics(Map<TopicPartition, ListOffsetsPartition>)`.
    pub fn to_list_offsets_topics(
        timestamps_to_search: &HashMap<TopicPartition, ListOffsetsPartition>,
    ) -> Vec<ListOffsetsTopic> {
        let mut topics: HashMap<String, ListOffsetsTopic> = HashMap::new();
        for (tp, partition) in timestamps_to_search {
            let topic = topics.entry(tp.topic().to_string()).or_insert_with(|| {
                let mut t = ListOffsetsTopic::new();
                t.set_name(tp.topic().to_string());
                t
            });
            topic.partitions.push(partition.clone());
        }
        topics.into_values().collect()
    }
}

impl std::fmt::Display for ListOffsetsRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ListOffsetsRequest(version={}, data={})", self.version, self.data)
    }
}

/// Builder for [`ListOffsetsRequest`].
///
/// Corresponds to `ListOffsetsRequest.Builder` in Java.
#[derive(Debug, Clone)]
pub struct ListOffsetsRequestBuilder {
    data: ListOffsetsRequestData,
    oldest_allowed_version: i16,
    latest_allowed_version: i16,
}

impl ListOffsetsRequestBuilder {
    /// Constructs a consumer-side builder.
    ///
    /// Mirrors `ListOffsetsRequest.Builder.forConsumer(boolean, IsolationLevel)`.
    pub fn for_consumer(require_timestamp: bool, isolation_level: IsolationLevel) -> Self {
        Self::for_consumer_with_features(require_timestamp, isolation_level, false, false, false, false)
    }

    /// Constructs a consumer-side builder, picking the minimum API version
    /// required to satisfy the requested feature flags.
    ///
    /// Mirrors the six-arg `Builder.forConsumer(...)`.
    pub fn for_consumer_with_features(
        require_timestamp: bool,
        isolation_level: IsolationLevel,
        require_max_timestamp: bool,
        require_earliest_local_timestamp: bool,
        require_tiered_storage_timestamp: bool,
        require_earliest_pending_upload_timestamp: bool,
    ) -> Self {
        let mut min_version = ApiKeys::LIST_OFFSETS.oldest_version();
        if require_earliest_pending_upload_timestamp {
            min_version = 11;
        } else if require_tiered_storage_timestamp {
            min_version = 9;
        } else if require_earliest_local_timestamp {
            min_version = 8;
        } else if require_max_timestamp {
            min_version = 7;
        } else if matches!(isolation_level, IsolationLevel::ReadCommitted) {
            min_version = 2;
        } else if require_timestamp {
            min_version = 1;
        }
        Self::new(
            min_version,
            ApiKeys::LIST_OFFSETS.latest_version(),
            CONSUMER_REPLICA_ID,
            isolation_level,
        )
    }

    /// Constructs a replica-side builder targeting the supplied broker id.
    ///
    /// Mirrors `ListOffsetsRequest.Builder.forReplica(short, int)`.
    pub fn for_replica(allowed_version: i16, replica_id: i32) -> Self {
        Self::new(
            ApiKeys::LIST_OFFSETS.oldest_version(),
            allowed_version,
            replica_id,
            IsolationLevel::ReadUncommitted,
        )
    }

    fn new(
        oldest_allowed_version: i16,
        latest_allowed_version: i16,
        replica_id: i32,
        isolation_level: IsolationLevel,
    ) -> Self {
        let mut data = ListOffsetsRequestData::new();
        data.set_isolation_level(isolation_level.id() as i8);
        data.set_replica_id(replica_id);
        Self { data, oldest_allowed_version, latest_allowed_version }
    }

    /// Sets the topic-partition timestamps to search.
    ///
    /// Mirrors `Builder.setTargetTimes(List<ListOffsetsTopic>)`.
    pub fn set_target_times(&mut self, topics: Vec<ListOffsetsTopic>) -> &mut Self {
        self.data.set_topics(topics);
        self
    }

    /// Sets the broker-side timeout for tiered-storage reads (v10+).
    ///
    /// Mirrors `Builder.setTimeoutMs(int)`.
    pub fn set_timeout_ms(&mut self, timeout_ms: i32) -> &mut Self {
        self.data.set_timeout_ms(timeout_ms);
        self
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &ListOffsetsRequestData {
        &self.data
    }
}

impl RequestBuilder for ListOffsetsRequestBuilder {
    fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::LIST_OFFSETS
    }

    fn oldest_allowed_version(&self) -> i16 {
        self.oldest_allowed_version
    }

    fn latest_allowed_version(&self) -> i16 {
        self.latest_allowed_version
    }

    fn build_version(&mut self, version: i16) -> io::Result<ConcreteRequest> {
        if version < self.oldest_allowed_version || version > self.latest_allowed_version {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!(
                    "Cannot build ListOffsets request with version {version} (allowed range: {}..={})",
                    self.oldest_allowed_version, self.latest_allowed_version,
                ),
            ));
        }
        Ok(ConcreteRequest::ListOffsets(ListOffsetsRequest::new(
            self.data.clone(),
            version,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::list_offsets_request_data::ListOffsetsPartition;

    /// Verifies the version selection logic for a vanilla consumer ListOffsets
    /// request (translated from `Builder.forConsumer(false, READ_UNCOMMITTED)`).
    #[test]
    fn for_consumer_default_uses_oldest_version() {
        let builder = ListOffsetsRequestBuilder::for_consumer(false, IsolationLevel::ReadUncommitted);
        assert_eq!(builder.oldest_allowed_version(), ApiKeys::LIST_OFFSETS.oldest_version());
        assert_eq!(builder.latest_allowed_version(), ApiKeys::LIST_OFFSETS.latest_version());
    }

    /// Verifies that `READ_COMMITTED` forces minimum v2.
    #[test]
    fn for_consumer_read_committed_forces_v2() {
        let builder = ListOffsetsRequestBuilder::for_consumer(false, IsolationLevel::ReadCommitted);
        assert_eq!(builder.oldest_allowed_version(), 2);
    }

    /// Verifies that `requireTimestamp` forces minimum v1.
    #[test]
    fn for_consumer_require_timestamp_forces_v1() {
        let builder = ListOffsetsRequestBuilder::for_consumer(true, IsolationLevel::ReadUncommitted);
        assert_eq!(builder.oldest_allowed_version(), 1);
    }

    /// Verifies that `requireMaxTimestamp` forces minimum v7.
    #[test]
    fn for_consumer_require_max_timestamp_forces_v7() {
        let builder = ListOffsetsRequestBuilder::for_consumer_with_features(
            true,
            IsolationLevel::ReadCommitted,
            true,
            false,
            false,
            false,
        );
        assert_eq!(builder.oldest_allowed_version(), 7);
    }

    /// Verifies that `requireEarliestPendingUploadTimestamp` forces minimum v11.
    #[test]
    fn for_consumer_require_earliest_pending_upload_forces_v11() {
        let builder = ListOffsetsRequestBuilder::for_consumer_with_features(
            true,
            IsolationLevel::ReadCommitted,
            false,
            false,
            false,
            true,
        );
        assert_eq!(builder.oldest_allowed_version(), 11);
    }

    /// Verifies that `to_list_offsets_topics` groups by topic name, matching
    /// Java's `toListOffsetsTopics(Map)` behavior.
    #[test]
    fn to_list_offsets_topics_groups_by_topic() {
        let mut map: HashMap<TopicPartition, ListOffsetsPartition> = HashMap::new();
        let mut p0 = ListOffsetsPartition::new();
        p0.set_partition_index(0);
        p0.set_timestamp(EARLIEST_TIMESTAMP);
        let mut p1 = ListOffsetsPartition::new();
        p1.set_partition_index(1);
        p1.set_timestamp(LATEST_TIMESTAMP);
        let mut p_other = ListOffsetsPartition::new();
        p_other.set_partition_index(0);
        p_other.set_timestamp(EARLIEST_TIMESTAMP);
        map.insert(TopicPartition::new("topic-a".to_string(), 0), p0);
        map.insert(TopicPartition::new("topic-a".to_string(), 1), p1);
        map.insert(TopicPartition::new("topic-b".to_string(), 0), p_other);

        let result = ListOffsetsRequest::to_list_offsets_topics(&map);
        assert_eq!(result.len(), 2);
        let topic_a = result.iter().find(|t| t.name == "topic-a").expect("topic-a present");
        assert_eq!(topic_a.partitions.len(), 2);
        let topic_b = result.iter().find(|t| t.name == "topic-b").expect("topic-b present");
        assert_eq!(topic_b.partitions.len(), 1);
    }

    /// Verifies that duplicate partitions in the request are collected into
    /// `duplicate_partitions()`, matching Java's constructor behavior.
    #[test]
    fn duplicate_partitions_detected() {
        let mut data = ListOffsetsRequestData::new();
        let mut topic = ListOffsetsTopic::new();
        topic.set_name("t".to_string());
        let mut p1 = ListOffsetsPartition::new();
        p1.set_partition_index(0);
        let mut p2 = ListOffsetsPartition::new();
        p2.set_partition_index(0);
        topic.partitions = vec![p1, p2];
        data.set_topics(vec![topic]);

        let req = ListOffsetsRequest::new(data, ApiKeys::LIST_OFFSETS.latest_version());
        assert_eq!(req.duplicate_partitions().len(), 1);
        assert!(req.duplicate_partitions().contains(&TopicPartition::new("t".to_string(), 0)));
    }
}
