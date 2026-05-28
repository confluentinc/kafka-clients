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

//! `OffsetFetch` request handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.OffsetFetchRequest`.
//!
//! Wraps the auto-generated [`OffsetFetchRequestData`] and exposes the
//! `OffsetFetchRequestBuilder` used by the consumer's fetch-committed-offsets
//! path.
//!
//! # Version selection
//!
//! - v2+ supports the top-level `error_code` field and `null` `topics`
//!   ("fetch all topic-partitions").
//! - v7+ supports `requireStable`.
//! - v8+ supports batched groups (single request asking offsets for
//!   multiple group ids).
//! - v10+ uses topic ids on the wire instead of names.

use std::collections::HashMap;
use std::io;

use crate::common::TopicPartition;
use crate::common::Uuid;
use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::offset_fetch_request_data::{
    OffsetFetchRequestData, OffsetFetchRequestGroup, OffsetFetchRequestTopic, OffsetFetchRequestTopics,
};
use crate::offset_fetch_response_data::{
    OffsetFetchResponseData, OffsetFetchResponseGroup, OffsetFetchResponsePartition, OffsetFetchResponseTopic,
};

use super::ConcreteResponse;
use super::OffsetFetchResponse;
use super::RECORD_BATCH_NO_PARTITION_LEADER_EPOCH;
use super::abstract_request::{ConcreteRequest, RequestBuilder};

/// Wire version at which the top-level `error_code` and nullable `topics`
/// fields appear.
pub const TOP_LEVEL_ERROR_AND_NULL_TOPICS_MIN_VERSION: i16 = 2;
/// Wire version at which `requireStable` becomes available.
pub const REQUIRE_STABLE_OFFSET_MIN_VERSION: i16 = 7;
/// Wire version at which multiple groups can be batched in a single request.
pub const BATCH_MIN_VERSION: i16 = 8;
/// Wire version at which topic ids replace topic names.
pub const TOPIC_ID_MIN_VERSION: i16 = 10;

/// Sentinel offset value indicating "no committed offset".
pub const INVALID_OFFSET: i64 = -1;
/// Sentinel metadata string used when no metadata is associated.
pub const NO_METADATA: &str = "";

/// An `OffsetFetch` request.
///
/// Corresponds to `org.apache.kafka.common.requests.OffsetFetchRequest`.
#[derive(Debug, Clone)]
pub struct OffsetFetchRequest {
    data: OffsetFetchRequestData,
    version: i16,
}

impl OffsetFetchRequest {
    /// Creates a new `OffsetFetchRequest` from data and version.
    ///
    /// Mirrors Java's private constructor.
    pub fn new(data: OffsetFetchRequestData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &OffsetFetchRequestData {
        &self.data
    }

    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Returns the API key for this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::OFFSET_FETCH
    }

    /// Returns the group id for v<8 requests (single-group form).
    ///
    /// Mirrors Java's `groupId()`.
    pub fn group_id(&self) -> &str {
        &self.data.group_id
    }

    /// Returns `true` if the `requireStable` flag is set.
    ///
    /// Mirrors Java's `requireStable()`.
    pub fn require_stable(&self) -> bool {
        self.data.require_stable
    }

    /// Returns the list of groups carried by this request. For v<8 requests
    /// (single-group form), constructs a single-element list from the
    /// top-level `group_id` and `topics` so the caller can treat both
    /// versions uniformly.
    ///
    /// Mirrors Java's `groups()`.
    pub fn groups(&self) -> Vec<OffsetFetchRequestGroup> {
        if self.version >= BATCH_MIN_VERSION {
            return self.data.groups.clone();
        }
        let mut group = OffsetFetchRequestGroup::new();
        group.set_group_id(self.data.group_id.clone());
        match &self.data.topics {
            None => {
                group.set_topics(None);
            },
            Some(topics) => {
                let translated: Vec<OffsetFetchRequestTopics> = topics
                    .iter()
                    .map(|t| {
                        let mut new_topic = OffsetFetchRequestTopics::new();
                        new_topic.set_name(t.name.clone());
                        new_topic.set_partition_indexes(t.partition_indexes.clone());
                        new_topic
                    })
                    .collect();
                group.set_topics(Some(translated));
            },
        }
        vec![group]
    }

    /// Returns a map of group id -> list of partitions requested by that
    /// group, mirroring Java's `groupIdsToPartitions()`.
    pub fn group_ids_to_partitions(&self) -> HashMap<String, Option<Vec<TopicPartition>>> {
        let mut result = HashMap::new();
        for group in &self.data.groups {
            let tp_list = group.topics.as_ref().map(|topics| {
                let mut tps = Vec::new();
                for topic in topics {
                    for partition_index in &topic.partition_indexes {
                        tps.push(TopicPartition::new(topic.name.clone(), *partition_index));
                    }
                }
                tps
            });
            result.insert(group.group_id.clone(), tp_list);
        }
        result
    }

    /// Returns a map of group id -> requested topics, mirroring Java's
    /// `groupIdsToTopics()`.
    pub fn group_ids_to_topics(&self) -> HashMap<String, Option<Vec<OffsetFetchRequestTopics>>> {
        let mut result = HashMap::with_capacity(self.data.groups.len());
        for group in &self.data.groups {
            result.insert(group.group_id.clone(), group.topics.clone());
        }
        result
    }

    /// Returns the list of group ids carried by this batched request.
    ///
    /// Mirrors Java's `groupIds()`.
    pub fn group_ids(&self) -> Vec<String> {
        self.data.groups.iter().map(|g| g.group_id.clone()).collect()
    }

    /// Builds the canonical error response for this request, matching Java's
    /// `OffsetFetchRequest.getErrorResponse(int, Throwable)` over the
    /// three supported version layouts.
    pub fn get_error_response(&self, throttle_time_ms: i32, error: &Errors) -> ConcreteResponse {
        let mut data = OffsetFetchResponseData::new();
        data.set_throttle_time_ms(throttle_time_ms);

        if self.version < TOP_LEVEL_ERROR_AND_NULL_TOPICS_MIN_VERSION {
            // No top-level error; propagate the error to every partition.
            if let Some(topics) = &self.data.topics {
                let response_topics = topics
                    .iter()
                    .map(|topic| {
                        let mut response_topic = OffsetFetchResponseTopic::new();
                        response_topic.set_name(topic.name.clone());
                        let partitions = topic
                            .partition_indexes
                            .iter()
                            .map(|&partition_index| {
                                let mut p = OffsetFetchResponsePartition::new();
                                p.set_partition_index(partition_index);
                                p.set_error_code(error.code());
                                p.set_committed_offset(INVALID_OFFSET);
                                p.set_metadata(Some(NO_METADATA.to_string()));
                                p.set_committed_leader_epoch(RECORD_BATCH_NO_PARTITION_LEADER_EPOCH);
                                p
                            })
                            .collect();
                        response_topic.set_partitions(partitions);
                        response_topic
                    })
                    .collect();
                data.set_topics(response_topics);
            }
        } else if self.version < BATCH_MIN_VERSION {
            // Top-level error code; single-group form.
            data.set_error_code(error.code());
        } else {
            // Multi-group form with per-group top-level error.
            let groups = self
                .data
                .groups
                .iter()
                .map(|group| {
                    let mut g = OffsetFetchResponseGroup::new();
                    g.set_group_id(group.group_id.clone());
                    g.set_error_code(error.code());
                    g
                })
                .collect();
            data.set_groups(groups);
        }
        ConcreteResponse::OffsetFetch(OffsetFetchResponse::new(data, self.version))
    }

    /// Returns `true` if the wire protocol uses topic ids at the given
    /// version (v10+).
    pub fn use_topic_ids(version: i16) -> bool {
        version >= TOPIC_ID_MIN_VERSION
    }

    /// Parses an `OffsetFetchRequest` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = OffsetFetchRequestData::read(readable, version)?;
        Ok(Self::new(data, version))
    }
}

impl std::fmt::Display for OffsetFetchRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "OffsetFetchRequest(version={}, data={})", self.version, self.data)
    }
}

/// Builder for [`OffsetFetchRequest`].
///
/// Corresponds to `OffsetFetchRequest.Builder` in Java.
#[derive(Debug, Clone)]
pub struct OffsetFetchRequestBuilder {
    data: OffsetFetchRequestData,
    throw_on_fetch_stable_offsets_unsupported: bool,
    oldest_allowed_version: i16,
    latest_allowed_version: i16,
}

impl OffsetFetchRequestBuilder {
    /// Build a request that can use either topic ids or topic names.
    ///
    /// Mirrors Java's `Builder.forTopicIdsOrNames(OffsetFetchRequestData, boolean)`.
    pub fn for_topic_ids_or_names(
        data: OffsetFetchRequestData,
        throw_on_fetch_stable_offsets_unsupported: bool,
    ) -> Self {
        Self {
            data,
            throw_on_fetch_stable_offsets_unsupported,
            oldest_allowed_version: ApiKeys::OFFSET_FETCH.oldest_version(),
            latest_allowed_version: ApiKeys::OFFSET_FETCH.latest_version(),
        }
    }

    /// Build a request that uses topic names — capped at v9.
    ///
    /// Mirrors Java's `Builder.forTopicNames(OffsetFetchRequestData, boolean)`.
    pub fn for_topic_names(data: OffsetFetchRequestData, throw_on_fetch_stable_offsets_unsupported: bool) -> Self {
        Self {
            data,
            throw_on_fetch_stable_offsets_unsupported,
            oldest_allowed_version: ApiKeys::OFFSET_FETCH.oldest_version(),
            latest_allowed_version: TOPIC_ID_MIN_VERSION - 1,
        }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &OffsetFetchRequestData {
        &self.data
    }

    /// Java: `maybeDowngrade(short)`. Converts batched (v8+) data to the
    /// single-group v<8 wire layout. Returns the (possibly rebuilt) data.
    fn maybe_downgrade(&self, version: i16) -> OffsetFetchRequestData {
        if version >= BATCH_MIN_VERSION || self.data.groups.is_empty() {
            return self.data.clone();
        }
        let group = &self.data.groups[0];
        let mut downgraded = OffsetFetchRequestData::new();
        downgraded.set_group_id(group.group_id.clone());
        if let Some(topics) = &group.topics {
            let old_topics: Vec<OffsetFetchRequestTopic> = topics
                .iter()
                .map(|t| {
                    let mut new_topic = OffsetFetchRequestTopic::new();
                    new_topic.set_name(t.name.clone());
                    new_topic.set_partition_indexes(t.partition_indexes.clone());
                    new_topic
                })
                .collect();
            downgraded.set_topics(Some(old_topics));
        } else {
            downgraded.set_topics(None);
        }
        downgraded.set_require_stable(self.data.require_stable);
        downgraded
    }
}

impl RequestBuilder for OffsetFetchRequestBuilder {
    fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::OFFSET_FETCH
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
                    "Cannot build OffsetFetch request with version {version} (allowed range: {}..={})",
                    self.oldest_allowed_version, self.latest_allowed_version,
                ),
            ));
        }
        // throwIfBatchingIsUnsupported
        if self.data.groups.len() > 1 && version < BATCH_MIN_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!("Broker does not support batching groups for fetch offset request on version {version}"),
            ));
        }
        // throwIfStableOffsetsUnsupported
        if self.data.require_stable
            && version < REQUIRE_STABLE_OFFSET_MIN_VERSION
            && self.throw_on_fetch_stable_offsets_unsupported
        {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!("Broker unexpectedly doesn't support requireStable flag on version {version}"),
            ));
        }
        // Java logs `trace` and clears the flag in the non-strict case;
        // the clearing happens further below by mutating a downgraded copy
        // of the data so the request as built reflects `requireStable=false`.
        // throwIfMissingRequiredTopicIdentifiers
        if version < TOPIC_ID_MIN_VERSION {
            for group in &self.data.groups {
                if let Some(topics) = &group.topics {
                    for topic in topics {
                        if topic.name.is_empty() {
                            return Err(io::Error::new(
                                io::ErrorKind::Unsupported,
                                format!(
                                    "The broker offset fetch api version {version} does require usage of topic names."
                                ),
                            ));
                        }
                    }
                }
            }
        } else {
            for group in &self.data.groups {
                if let Some(topics) = &group.topics {
                    for topic in topics {
                        if topic.topic_id == Uuid::zero() {
                            return Err(io::Error::new(
                                io::ErrorKind::Unsupported,
                                format!(
                                    "The broker offset fetch api version {version} does require usage of topic ids."
                                ),
                            ));
                        }
                    }
                }
            }
        }
        // throwIfRequestingAllTopicsIsUnsupported
        if version < TOP_LEVEL_ERROR_AND_NULL_TOPICS_MIN_VERSION {
            for group in &self.data.groups {
                if group.topics.is_none() {
                    return Err(io::Error::new(
                        io::ErrorKind::Unsupported,
                        format!(
                            "The broker only supports OffsetFetchRequest v{version}, but we need v2 or newer to \
                             request all topic partitions."
                        ),
                    ));
                }
            }
        }
        // Build with maybeDowngrade applied for older versions.
        let mut data = self.maybe_downgrade(version);
        if !self.throw_on_fetch_stable_offsets_unsupported
            && self.data.require_stable
            && version < REQUIRE_STABLE_OFFSET_MIN_VERSION
        {
            data.set_require_stable(false);
        }
        Ok(ConcreteRequest::OffsetFetch(OffsetFetchRequest::new(data, version)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn topics_for_test() -> Vec<OffsetFetchRequestTopics> {
        let mut topic = OffsetFetchRequestTopics::new();
        topic.set_name("t".to_string());
        topic.set_partition_indexes(vec![0, 1]);
        vec![topic]
    }

    fn group_with_topics() -> OffsetFetchRequestGroup {
        let mut g = OffsetFetchRequestGroup::new();
        g.set_group_id("g".to_string());
        g.set_topics(Some(topics_for_test()));
        g
    }

    /// `for_topic_ids_or_names` opens the full version range.
    #[test]
    fn for_topic_ids_or_names_uses_full_range() {
        let builder = OffsetFetchRequestBuilder::for_topic_ids_or_names(OffsetFetchRequestData::new(), false);
        assert_eq!(builder.oldest_allowed_version(), ApiKeys::OFFSET_FETCH.oldest_version());
        assert_eq!(builder.latest_allowed_version(), ApiKeys::OFFSET_FETCH.latest_version());
    }

    /// `for_topic_names` caps the latest allowed version at v9.
    #[test]
    fn for_topic_names_caps_at_v9() {
        let builder = OffsetFetchRequestBuilder::for_topic_names(OffsetFetchRequestData::new(), false);
        assert_eq!(builder.latest_allowed_version(), TOPIC_ID_MIN_VERSION - 1);
    }

    /// `build_version` rejects multi-group requests at v<8.
    #[test]
    fn build_version_rejects_multi_group_below_v8() {
        let mut data = OffsetFetchRequestData::new();
        let mut g1 = OffsetFetchRequestGroup::new();
        g1.set_group_id("g1".to_string());
        let mut g2 = OffsetFetchRequestGroup::new();
        g2.set_group_id("g2".to_string());
        data.set_groups(vec![g1, g2]);
        let builder = OffsetFetchRequestBuilder::for_topic_ids_or_names(data, false);
        let err = builder.build_version(7).unwrap_err();
        assert!(err.to_string().contains("batching groups"));
    }

    /// `build_version` rejects `requireStable` at v<7 with the strict flag.
    #[test]
    fn build_version_rejects_require_stable_below_v7_when_strict() {
        let mut data = OffsetFetchRequestData::new();
        data.set_groups(vec![group_with_topics()]);
        data.set_require_stable(true);
        let builder = OffsetFetchRequestBuilder::for_topic_ids_or_names(data, true);
        let err = builder.build_version(6).unwrap_err();
        assert!(err.to_string().contains("requireStable"));
    }

    /// `build_version` silently falls back when `requireStable` is set on
    /// v<7 and the strict flag is `false`.
    #[test]
    fn build_version_falls_back_require_stable_below_v7() {
        let mut data = OffsetFetchRequestData::new();
        data.set_groups(vec![group_with_topics()]);
        data.set_require_stable(true);
        let builder = OffsetFetchRequestBuilder::for_topic_ids_or_names(data, false);
        let req = builder.build_version(6).expect("falls back silently");
        match req {
            ConcreteRequest::OffsetFetch(r) => assert!(!r.require_stable()),
            other => panic!("expected OffsetFetch, got {}", other.api_key().name()),
        }
    }

    /// `build_version` requires topic names below v10.
    #[test]
    fn build_version_below_v10_requires_topic_names() {
        let mut data = OffsetFetchRequestData::new();
        let mut g = OffsetFetchRequestGroup::new();
        g.set_group_id("g".to_string());
        let mut t = OffsetFetchRequestTopics::new();
        t.set_topic_id(Uuid::new(1, 2));
        // name left empty
        g.set_topics(Some(vec![t]));
        data.set_groups(vec![g]);
        let builder = OffsetFetchRequestBuilder::for_topic_names(data, false);
        let err = builder.build_version(8).unwrap_err();
        assert!(err.to_string().contains("topic names"));
    }

    /// `build_version` requires topic ids at v10.
    #[test]
    fn build_version_v10_requires_topic_ids() {
        let mut data = OffsetFetchRequestData::new();
        let mut g = OffsetFetchRequestGroup::new();
        g.set_group_id("g".to_string());
        let mut t = OffsetFetchRequestTopics::new();
        t.set_name("t".to_string()); // no topic_id
        g.set_topics(Some(vec![t]));
        data.set_groups(vec![g]);
        let builder = OffsetFetchRequestBuilder::for_topic_ids_or_names(data, false);
        let err = builder.build_version(10).unwrap_err();
        assert!(err.to_string().contains("topic ids"));
    }

    /// `groups()` on a v<8 request synthesises a single-group list from
    /// `group_id` + `topics`.
    #[test]
    fn groups_synthesises_for_pre_batch_versions() {
        let mut data = OffsetFetchRequestData::new();
        data.set_group_id("g".to_string());
        let mut topic = OffsetFetchRequestTopic::new();
        topic.set_name("t".to_string());
        topic.set_partition_indexes(vec![0]);
        data.set_topics(Some(vec![topic]));
        let req = OffsetFetchRequest::new(data, 7);
        let groups = req.groups();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].group_id, "g");
        let topics = groups[0].topics.as_ref().expect("topics present");
        assert_eq!(topics.len(), 1);
        assert_eq!(topics[0].name, "t");
        assert_eq!(topics[0].partition_indexes, vec![0]);
    }

    /// `group_ids_to_partitions` returns the list of TopicPartitions per group.
    #[test]
    fn group_ids_to_partitions_returns_list() {
        let mut data = OffsetFetchRequestData::new();
        data.set_groups(vec![group_with_topics()]);
        let req = OffsetFetchRequest::new(data, 8);
        let map = req.group_ids_to_partitions();
        assert_eq!(map.len(), 1);
        let partitions = map.get("g").unwrap().as_ref().unwrap();
        assert_eq!(partitions.len(), 2);
    }

    /// `use_topic_ids` returns true only for v10+.
    #[test]
    fn use_topic_ids_v10_threshold() {
        assert!(!OffsetFetchRequest::use_topic_ids(9));
        assert!(OffsetFetchRequest::use_topic_ids(10));
    }
}
