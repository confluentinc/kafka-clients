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

//! `OffsetsForLeaderEpoch` request handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.OffsetsForLeaderEpochRequest`.
//!
//! Wraps the auto-generated [`OffsetForLeaderEpochRequestData`] and exposes
//! a [`OffsetsForLeaderEpochRequestBuilder`] that emits v3+ for consumer
//! callers (the version range required for topic-level permission instead
//! of cluster permission).

use std::io;

use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::offset_for_leader_epoch_request_data::{OffsetForLeaderEpochRequestData, OffsetForLeaderTopic};
use crate::offset_for_leader_epoch_response_data::{
    EpochEndOffset, OffsetForLeaderEpochResponseData, OffsetForLeaderTopicResult,
};

use super::ConcreteResponse;
use super::OffsetsForLeaderEpochResponse;
use super::abstract_request::{ConcreteRequest, RequestBuilder};
use super::offsets_for_leader_epoch_response::{UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET};

/// Sentinel replica id used by ordinary consumers.
///
/// Mirrors `OffsetsForLeaderEpochRequest.CONSUMER_REPLICA_ID` in Java.
pub const CONSUMER_REPLICA_ID: i32 = -1;

/// Minimum API version that allows topic-level permission instead of
/// cluster-level permission. Consumer-side callers always target this
/// version or higher.
pub const MIN_CONSUMER_VERSION: i16 = 3;

/// Returns `true` if `latest_usable_version` allows topic-level permission.
///
/// Mirrors `OffsetsForLeaderEpochRequest.supportsTopicPermission(short)`.
pub fn supports_topic_permission(latest_usable_version: i16) -> bool {
    latest_usable_version >= MIN_CONSUMER_VERSION
}

/// An `OffsetsForLeaderEpoch` request.
///
/// Corresponds to `org.apache.kafka.common.requests.OffsetsForLeaderEpochRequest`.
#[derive(Debug, Clone)]
pub struct OffsetsForLeaderEpochRequest {
    data: OffsetForLeaderEpochRequestData,
    version: i16,
}

impl OffsetsForLeaderEpochRequest {
    /// Creates a new `OffsetsForLeaderEpochRequest` from data and version.
    pub fn new(data: OffsetForLeaderEpochRequestData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &OffsetForLeaderEpochRequestData {
        &self.data
    }

    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Returns the API key for this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::OFFSET_FOR_LEADER_EPOCH
    }

    /// Returns the wire replica id.
    pub fn replica_id(&self) -> i32 {
        self.data.replica_id
    }

    /// Builds the canonical error response for this request, matching Java's
    /// `OffsetsForLeaderEpochRequest.getErrorResponse(int, Throwable)`.
    pub fn get_error_response(&self, throttle_time_ms: i32, error: &Errors) -> ConcreteResponse {
        let error_code = error.code();
        let mut response_data = OffsetForLeaderEpochResponseData::new();
        response_data.set_throttle_time_ms(throttle_time_ms);
        let mut topics = Vec::with_capacity(self.data.topics.len());
        for topic in &self.data.topics {
            let mut topic_result = OffsetForLeaderTopicResult::new();
            topic_result.set_topic(topic.topic.clone());
            let mut partitions = Vec::with_capacity(topic.partitions.len());
            for partition in &topic.partitions {
                let mut end_offset = EpochEndOffset::new();
                end_offset.set_partition(partition.partition);
                end_offset.set_error_code(error_code);
                end_offset.set_leader_epoch(UNDEFINED_EPOCH);
                end_offset.set_end_offset(UNDEFINED_EPOCH_OFFSET);
                partitions.push(end_offset);
            }
            topic_result.set_partitions(partitions);
            topics.push(topic_result);
        }
        response_data.set_topics(topics);
        ConcreteResponse::OffsetsForLeaderEpoch(OffsetsForLeaderEpochResponse::new(response_data))
    }

    /// Parses an `OffsetsForLeaderEpochRequest` from a readable buffer at
    /// the given version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = OffsetForLeaderEpochRequestData::read(readable, version)?;
        Ok(Self::new(data, version))
    }
}

impl std::fmt::Display for OffsetsForLeaderEpochRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "OffsetsForLeaderEpochRequest(version={}, data={})", self.version, self.data)
    }
}

/// Builder for [`OffsetsForLeaderEpochRequest`].
///
/// Corresponds to `OffsetsForLeaderEpochRequest.Builder` in Java.
#[derive(Debug, Clone)]
pub struct OffsetsForLeaderEpochRequestBuilder {
    data: OffsetForLeaderEpochRequestData,
    oldest_allowed_version: i16,
    latest_allowed_version: i16,
}

impl OffsetsForLeaderEpochRequestBuilder {
    /// Constructs a consumer-side builder targeting v3+ (the version that
    /// allows topic-level permission).
    ///
    /// Mirrors `Builder.forConsumer(OffsetForLeaderTopicCollection)`.
    pub fn for_consumer(epochs_by_partition: Vec<OffsetForLeaderTopic>) -> Self {
        let mut data = OffsetForLeaderEpochRequestData::new();
        data.set_replica_id(CONSUMER_REPLICA_ID);
        data.set_topics(epochs_by_partition);
        Self {
            data,
            oldest_allowed_version: MIN_CONSUMER_VERSION,
            latest_allowed_version: ApiKeys::OFFSET_FOR_LEADER_EPOCH.latest_version(),
        }
    }

    /// Constructs a follower-side builder pinned to v4.
    ///
    /// Mirrors `Builder.forFollower(OffsetForLeaderTopicCollection, int)`.
    pub fn for_follower(epochs_by_partition: Vec<OffsetForLeaderTopic>, replica_id: i32) -> Self {
        let mut data = OffsetForLeaderEpochRequestData::new();
        data.set_replica_id(replica_id);
        data.set_topics(epochs_by_partition);
        Self {
            data,
            // Java pins follower requests to v4 — newer versions are gated
            // behind metadata-version checks not modelled here.
            oldest_allowed_version: 4,
            latest_allowed_version: 4,
        }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &OffsetForLeaderEpochRequestData {
        &self.data
    }
}

impl RequestBuilder for OffsetsForLeaderEpochRequestBuilder {
    fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::OFFSET_FOR_LEADER_EPOCH
    }

    fn oldest_allowed_version(&self) -> i16 {
        self.oldest_allowed_version
    }

    fn latest_allowed_version(&self) -> i16 {
        self.latest_allowed_version
    }

    fn build_version(&self, version: i16) -> io::Result<ConcreteRequest> {
        if version < self.oldest_allowed_version || version > self.latest_allowed_version {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!(
                    "Cannot build OffsetsForLeaderEpoch request with version {version} (allowed range: {}..={})",
                    self.oldest_allowed_version, self.latest_allowed_version,
                ),
            ));
        }
        Ok(ConcreteRequest::OffsetsForLeaderEpoch(OffsetsForLeaderEpochRequest::new(
            self.data.clone(),
            version,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verifies that `for_consumer` targets v3+ to honour the topic-permission
    /// requirement.
    #[test]
    fn for_consumer_targets_v3_plus() {
        let builder = OffsetsForLeaderEpochRequestBuilder::for_consumer(Vec::new());
        assert_eq!(builder.oldest_allowed_version(), MIN_CONSUMER_VERSION);
        assert_eq!(
            builder.latest_allowed_version(),
            ApiKeys::OFFSET_FOR_LEADER_EPOCH.latest_version()
        );
        assert_eq!(builder.data().replica_id, CONSUMER_REPLICA_ID);
    }

    /// Verifies that `for_follower` pins the version to 4 and uses the
    /// supplied replica id.
    #[test]
    fn for_follower_pins_to_v4() {
        let builder = OffsetsForLeaderEpochRequestBuilder::for_follower(Vec::new(), 7);
        assert_eq!(builder.oldest_allowed_version(), 4);
        assert_eq!(builder.latest_allowed_version(), 4);
        assert_eq!(builder.data().replica_id, 7);
    }

    /// Verifies `supports_topic_permission` returns true iff version >= 3.
    #[test]
    fn supports_topic_permission_threshold() {
        assert!(!supports_topic_permission(0));
        assert!(!supports_topic_permission(2));
        assert!(supports_topic_permission(3));
        assert!(supports_topic_permission(4));
    }
}
