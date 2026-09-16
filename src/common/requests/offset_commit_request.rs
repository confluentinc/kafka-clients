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

//! `OffsetCommit` request handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.OffsetCommitRequest`.
//!
//! Wraps the auto-generated [`OffsetCommitRequestData`] and exposes an
//! [`OffsetCommitRequestBuilder`] for the consumer's commit path.
//!
//! # Version selection
//!
//! KIP-848 (the new consumer group protocol) requires `OffsetCommit` v8+:
//! v8 introduced flexible (compact) framing and tagged fields; v9 carries
//! `groupInstanceId` and a non-classic `generationIdOrMemberEpoch`. v10
//! switches from topic names to topic ids.

use std::collections::HashMap;
use std::io;

use crate::OffsetCommitRequestData;
use crate::OffsetCommitResponseData;
use crate::common::TopicPartition;
use crate::common::Uuid;
use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::offset_commit_response_data::{OffsetCommitResponsePartition, OffsetCommitResponseTopic};

use super::ConcreteResponse;
use super::OffsetCommitResponse;
use super::abstract_request::{ConcreteRequest, RequestBuilder};

/// An `OffsetCommit` request.
///
/// Corresponds to `org.apache.kafka.common.requests.OffsetCommitRequest`.
#[derive(Debug, Clone)]
pub struct OffsetCommitRequest {
    data: OffsetCommitRequestData,
    version: i16,
}

impl OffsetCommitRequest {
    /// Default value for the `generation_id_or_member_epoch` wire field when no
    /// generation is known. Mirrors Java's `OffsetCommitRequest.DEFAULT_GENERATION_ID`.
    pub const DEFAULT_GENERATION_ID: i32 = -1;

    /// Default value for the `member_id` wire field when no member id is known.
    /// Mirrors Java's `OffsetCommitRequest.DEFAULT_MEMBER_ID`.
    pub const DEFAULT_MEMBER_ID: &str = "";

    /// Default value for the `retention_time_ms` wire field (v2..v4 only).
    /// Mirrors Java's `OffsetCommitRequest.DEFAULT_RETENTION_TIME`.
    pub const DEFAULT_RETENTION_TIME: i64 = -1;

    /// Default value for the `committed_timestamp` field (v0..v1 only).
    /// Mirrors Java's `OffsetCommitRequest.DEFAULT_TIMESTAMP`.
    pub const DEFAULT_TIMESTAMP: i64 = -1;

    /// Creates a new `OffsetCommitRequest` from data and version.
    ///
    /// Mirrors Java's constructor `OffsetCommitRequest(OffsetCommitRequestData, short)`.
    pub fn new(data: OffsetCommitRequestData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &OffsetCommitRequestData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut OffsetCommitRequestData {
        &mut self.data
    }

    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Returns the API key for this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::OFFSET_COMMIT
    }

    /// Returns a map of `TopicPartition -> committedOffset`, mirroring
    /// Java's `offsets()` accessor.
    pub fn offsets(&self) -> HashMap<TopicPartition, i64> {
        let mut offsets = HashMap::new();
        for topic in &self.data.topics {
            for partition in &topic.partitions {
                offsets.insert(
                    TopicPartition::new(topic.name.clone(), partition.partition_index),
                    partition.committed_offset,
                );
            }
        }
        offsets
    }

    /// Builds the canonical error response for this request, matching Java's
    /// `OffsetCommitRequest.getErrorResponse(int, Throwable)`.
    pub fn get_error_response(&self, throttle_time_ms: i32, error: &Errors) -> ConcreteResponse {
        let mut data = Self::error_response_data(&self.data, *error);
        data.set_throttle_time_ms(throttle_time_ms);
        ConcreteResponse::OffsetCommit(OffsetCommitResponse::new_data(data))
    }

    /// Builds an `OffsetCommitResponseData` carrying the given error code for
    /// every partition in the request. Equivalent to Java's static
    /// `getErrorResponse(OffsetCommitRequestData, Errors)`.
    pub fn error_response_data(request: &OffsetCommitRequestData, error: Errors) -> OffsetCommitResponseData {
        let mut response = OffsetCommitResponseData::new();
        let mut topics = Vec::with_capacity(request.topics.len());
        for topic in &request.topics {
            let mut response_topic = OffsetCommitResponseTopic::new();
            response_topic.set_topic_id(topic.topic_id);
            response_topic.set_name(topic.name.clone());
            let mut partitions = Vec::with_capacity(topic.partitions.len());
            for partition in &topic.partitions {
                let mut response_partition = OffsetCommitResponsePartition::new();
                response_partition.set_partition_index(partition.partition_index);
                response_partition.set_error_code(error.code());
                partitions.push(response_partition);
            }
            response_topic.set_partitions(partitions);
            topics.push(response_topic);
        }
        response.set_topics(topics);
        response
    }

    /// Parses an `OffsetCommitRequest` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = OffsetCommitRequestData::read(readable, version)?;
        Ok(Self::new(data, version))
    }
}

impl std::fmt::Display for OffsetCommitRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "OffsetCommitRequest(version={}, data={})", self.version, self.data)
    }
}

/// Builder for [`OffsetCommitRequest`].
///
/// Corresponds to `OffsetCommitRequest.Builder` in Java. Two factory
/// constructors mirror Java's two static factories:
///
/// - [`Self::for_topic_ids_or_names`] — allow up to the latest supported
///   version (v10+ uses topic ids).
/// - [`Self::for_topic_names`] — cap the version at v9 so the request is
///   guaranteed to use topic names.
#[derive(Debug, Clone)]
pub struct OffsetCommitRequestBuilder {
    data: OffsetCommitRequestData,
    oldest_allowed_version: i16,
    latest_allowed_version: i16,
}

impl OffsetCommitRequestBuilder {
    /// Build a request that can use either topic ids or topic names.
    ///
    /// Mirrors Java's `Builder.forTopicIdsOrNames(OffsetCommitRequestData)`.
    pub fn for_topic_ids_or_names(data: OffsetCommitRequestData) -> Self {
        Self {
            data,
            oldest_allowed_version: ApiKeys::OFFSET_COMMIT.oldest_version(),
            latest_allowed_version: ApiKeys::OFFSET_COMMIT.latest_version(),
        }
    }

    /// Build a request that uses topic names — capped at v9.
    ///
    /// Mirrors Java's `Builder.forTopicNames(OffsetCommitRequestData)`.
    pub fn for_topic_names(data: OffsetCommitRequestData) -> Self {
        Self {
            data,
            oldest_allowed_version: ApiKeys::OFFSET_COMMIT.oldest_version(),
            latest_allowed_version: 9,
        }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &OffsetCommitRequestData {
        &self.data
    }
}

impl RequestBuilder for OffsetCommitRequestBuilder {
    fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::OFFSET_COMMIT
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
                    "Cannot build OffsetCommit request with version {version} (allowed range: {}..={})",
                    self.oldest_allowed_version, self.latest_allowed_version,
                ),
            ));
        }
        // Java validates the version-vs-topic-id / version-vs-name invariants here.
        if self.data.group_instance_id.is_some() && version < 7 {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!(
                    "The broker offset commit api version {version} does not support usage of config group.instance.id."
                ),
            ));
        }
        if version >= 10 {
            for topic in &self.data.topics {
                if topic.topic_id == Uuid::zero() {
                    return Err(io::Error::new(
                        io::ErrorKind::Unsupported,
                        format!("The broker offset commit api version {version} does require usage of topic ids."),
                    ));
                }
            }
        } else {
            for topic in &self.data.topics {
                if topic.name.is_empty() {
                    return Err(io::Error::new(
                        io::ErrorKind::Unsupported,
                        format!("The broker offset commit api version {version} does require usage of topic names."),
                    ));
                }
            }
        }
        Ok(ConcreteRequest::OffsetCommit(OffsetCommitRequest::new(
            self.data.clone(),
            version,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::offset_commit_request_data::{OffsetCommitRequestPartition, OffsetCommitRequestTopic};

    fn topic_named(name: &str, partition_index: i32) -> OffsetCommitRequestTopic {
        let mut topic = OffsetCommitRequestTopic::new();
        topic.set_name(name.to_string());
        let mut partition = OffsetCommitRequestPartition::new();
        partition.partition_index = partition_index;
        partition.committed_offset = 100;
        topic.set_partitions(vec![partition]);
        topic
    }

    /// `for_topic_ids_or_names` opens the full version range supported by
    /// the API, matching Java's static factory.
    #[test]
    fn for_topic_ids_or_names_uses_full_range() {
        let mut data = OffsetCommitRequestData::new();
        data.set_group_id("g".to_string());
        let builder = OffsetCommitRequestBuilder::for_topic_ids_or_names(data);
        assert_eq!(builder.oldest_allowed_version(), ApiKeys::OFFSET_COMMIT.oldest_version());
        assert_eq!(builder.latest_allowed_version(), ApiKeys::OFFSET_COMMIT.latest_version());
    }

    /// `for_topic_names` caps the latest allowed version at v9.
    #[test]
    fn for_topic_names_caps_at_v9() {
        let mut data = OffsetCommitRequestData::new();
        data.set_group_id("g".to_string());
        let builder = OffsetCommitRequestBuilder::for_topic_names(data);
        assert_eq!(builder.latest_allowed_version(), 9);
    }

    /// `offsets()` returns the TopicPartition -> committedOffset map.
    #[test]
    fn offsets_returns_partition_map() {
        let mut data = OffsetCommitRequestData::new();
        data.set_group_id("g".to_string());
        data.set_topics(vec![topic_named("t", 0)]);
        let req = OffsetCommitRequest::new(data, 9);
        let offsets = req.offsets();
        assert_eq!(offsets.len(), 1);
        let tp = TopicPartition::new("t".to_string(), 0);
        assert_eq!(offsets.get(&tp), Some(&100_i64));
    }

    /// `build_version` rejects versions outside the builder's range.
    #[test]
    fn build_version_out_of_range_returns_err() {
        let mut data = OffsetCommitRequestData::new();
        data.set_group_id("g".to_string());
        let mut builder = OffsetCommitRequestBuilder::for_topic_names(data);
        let result = builder.build_version(10);
        assert!(result.is_err());
    }

    /// `build_version` rejects v10 requests that don't carry topic ids.
    #[test]
    fn build_version_v10_requires_topic_ids() {
        let mut data = OffsetCommitRequestData::new();
        data.set_group_id("g".to_string());
        let mut topic = OffsetCommitRequestTopic::new();
        topic.set_name("t".to_string()); // no topic_id set
        data.set_topics(vec![topic]);
        let mut builder = OffsetCommitRequestBuilder::for_topic_ids_or_names(data);
        let err = builder.build_version(10).unwrap_err();
        assert!(err.to_string().contains("topic ids"));
    }

    /// `build_version` rejects v<10 requests that don't carry topic names.
    #[test]
    fn build_version_v9_requires_topic_names() {
        let mut data = OffsetCommitRequestData::new();
        data.set_group_id("g".to_string());
        let mut topic = OffsetCommitRequestTopic::new();
        topic.set_topic_id(Uuid::new(1, 2));
        // name left empty
        data.set_topics(vec![topic]);
        let mut builder = OffsetCommitRequestBuilder::for_topic_names(data);
        let err = builder.build_version(9).unwrap_err();
        assert!(err.to_string().contains("topic names"));
    }

    /// `build_version` rejects `groupInstanceId` on versions < 7.
    #[test]
    fn build_version_group_instance_id_requires_v7() {
        let mut data = OffsetCommitRequestData::new();
        data.set_group_id("g".to_string());
        data.set_group_instance_id(Some("instance-a".to_string()));
        let mut builder = OffsetCommitRequestBuilder::for_topic_ids_or_names(data);
        let err = builder.build_version(6).unwrap_err();
        assert!(err.to_string().contains("group.instance.id"));
    }

    /// `error_response_data` propagates the supplied error code to every
    /// partition in the request.
    #[test]
    fn error_response_data_propagates_error() {
        let mut data = OffsetCommitRequestData::new();
        data.set_group_id("g".to_string());
        data.set_topics(vec![topic_named("t", 0), topic_named("t", 1)]);
        let response = OffsetCommitRequest::error_response_data(&data, Errors::NotCoordinator);
        assert_eq!(response.topics.len(), 2);
        for topic in &response.topics {
            for partition in &topic.partitions {
                assert_eq!(partition.error_code, Errors::NotCoordinator.code());
            }
        }
    }
}
