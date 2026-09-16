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

//! `OffsetFetch` response handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.OffsetFetchResponse`.
//!
//! Possible error codes:
//!
//! - Partition errors:
//!   - `UNKNOWN_TOPIC_OR_PARTITION`
//!   - `TOPIC_AUTHORIZATION_FAILED`
//!   - `UNSTABLE_OFFSET_COMMIT`
//!
//! - Group / coordinator errors:
//!   - `COORDINATOR_LOAD_IN_PROGRESS`
//!   - `COORDINATOR_NOT_AVAILABLE`
//!   - `NOT_COORDINATOR`
//!   - `GROUP_AUTHORIZATION_FAILED`
//!   - `UNKNOWN_MEMBER_ID`
//!   - `STALE_MEMBER_EPOCH`

use crate::common::requests::OffsetFetchRequest;
use std::collections::HashMap;
use std::io;
use std::sync::Mutex;

use crate::OffsetFetchResponseData;
use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::offset_fetch_request_data::OffsetFetchRequestGroup;
use crate::offset_fetch_response_data::OffsetFetchResponseTopics;
use crate::offset_fetch_response_data::{
    OffsetFetchResponseGroup, OffsetFetchResponsePartition, OffsetFetchResponsePartitions, OffsetFetchResponseTopic,
};

use super::AbstractResponse;
use super::RECORD_BATCH_NO_PARTITION_LEADER_EPOCH;

/// Per-partition errors that should not be promoted to a group-level error
/// when normalising the v<2 response. Mirrors Java's `PARTITION_ERRORS`.
const PARTITION_ERRORS: &[Errors] = &[Errors::UnknownTopicOrPartition, Errors::TopicAuthorizationFailed];

/// An `OffsetFetch` response.
///
/// Corresponds to `org.apache.kafka.common.requests.OffsetFetchResponse`.
#[derive(Debug)]
pub struct OffsetFetchResponse {
    version: i16,
    data: OffsetFetchResponseData,
    /// Lazily initialised when `group(groupId)` is called for v8+ responses.
    /// `Mutex` because `group(...)` takes `&self` per Java but mutates this
    /// cache.
    groups_cache: Mutex<Option<HashMap<String, OffsetFetchResponseGroup>>>,
}

impl Clone for OffsetFetchResponse {
    fn clone(&self) -> Self {
        // Do not propagate the cache; rebuild on demand.
        Self { version: self.version, data: self.data.clone(), groups_cache: Mutex::new(None) }
    }
}

impl OffsetFetchResponse {
    /// Creates a new `OffsetFetchResponse` from the underlying data and
    /// version.
    pub fn new(data: OffsetFetchResponseData, version: i16) -> Self {
        Self { version, data, groups_cache: Mutex::new(None) }
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::OFFSET_FETCH
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &OffsetFetchResponseData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut OffsetFetchResponseData {
        &mut self.data
    }

    /// Returns the version of this response.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Returns the throttle time in milliseconds.
    pub fn throttle_time_ms(&self) -> i32 {
        self.data.throttle_time_ms
    }

    /// Sets the throttle time in the response.
    pub fn maybe_set_throttle_time_ms(&mut self, throttle_time_ms: i32) {
        self.data.set_throttle_time_ms(throttle_time_ms);
    }

    /// Returns the response for the requested group id. For v<8 responses,
    /// synthesises a single-group view from the top-level fields; for v8+
    /// responses, looks up the group inside `data.groups`.
    ///
    /// Mirrors Java's `group(String)`.
    ///
    /// # Errors
    ///
    /// Returns `Err` when the requested group id is not present in a v8+
    /// response.
    pub fn group(&self, group_id: &str) -> Result<OffsetFetchResponseGroup, io::Error> {
        if self.version < OffsetFetchRequest::BATCH_MIN_VERSION {
            // For v<2 there's no top-level error code; derive it from the
            // partition errors. For v2..7 use `data.error_code`.
            let top_level_error_code = if self.version < OffsetFetchRequest::TOP_LEVEL_ERROR_AND_NULL_TOPICS_MIN_VERSION
            {
                Self::top_level_error(&self.data).code()
            } else {
                self.data.error_code
            };
            let mut g = OffsetFetchResponseGroup::new();
            g.set_group_id(group_id.to_string());
            if top_level_error_code != Errors::None.code() {
                g.set_error_code(top_level_error_code);
                return Ok(g);
            }
            let topics: Vec<OffsetFetchResponseTopics> = self
                .data
                .topics
                .iter()
                .map(|topic| {
                    let mut t = OffsetFetchResponseTopics::new();
                    t.set_name(topic.name.clone());
                    let partitions = topic
                        .partitions
                        .iter()
                        .map(|partition| {
                            let mut p = OffsetFetchResponsePartitions::new();
                            p.set_partition_index(partition.partition_index);
                            p.set_error_code(partition.error_code);
                            p.set_committed_offset(partition.committed_offset);
                            p.set_metadata(partition.metadata.clone());
                            p.set_committed_leader_epoch(partition.committed_leader_epoch);
                            p
                        })
                        .collect();
                    t.set_partitions(partitions);
                    t
                })
                .collect();
            g.set_topics(topics);
            Ok(g)
        } else {
            let mut guard = match self.groups_cache.lock() {
                Ok(g) => g,
                Err(poisoned) => poisoned.into_inner(),
            };
            if guard.is_none() {
                let map: HashMap<String, OffsetFetchResponseGroup> =
                    self.data.groups.iter().map(|g| (g.group_id.clone(), g.clone())).collect();
                *guard = Some(map);
            }
            let group = guard
                .as_ref()
                .expect("just initialised")
                .get(group_id)
                .cloned()
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::NotFound, format!("Group {group_id} not found in the response"))
                })?;
            Ok(group)
        }
    }

    /// Walks the v<2 partition error codes and returns the first
    /// non-partition-level error (i.e. group / coordinator error). Mirrors
    /// Java's static `topLevelError(OffsetFetchResponseData)`.
    fn top_level_error(data: &OffsetFetchResponseData) -> Errors {
        for topic in &data.topics {
            for partition in &topic.partitions {
                let partition_error = Errors::for_code(partition.error_code);
                if partition_error != Errors::None && !PARTITION_ERRORS.contains(&partition_error) {
                    return partition_error;
                }
            }
        }
        Errors::None
    }

    /// Returns the error counts aggregated across all partition responses.
    pub fn error_counts(&self) -> HashMap<Errors, i32> {
        let mut counts = HashMap::new();
        if self.version < OffsetFetchRequest::BATCH_MIN_VERSION {
            if self.version >= OffsetFetchRequest::TOP_LEVEL_ERROR_AND_NULL_TOPICS_MIN_VERSION {
                AbstractResponse::update_error_counts(&mut counts, Errors::for_code(self.data.error_code));
            }
            for topic in &self.data.topics {
                for partition in &topic.partitions {
                    AbstractResponse::update_error_counts(&mut counts, Errors::for_code(partition.error_code));
                }
            }
        } else {
            for group in &self.data.groups {
                AbstractResponse::update_error_counts(&mut counts, Errors::for_code(group.error_code));
                for topic in &group.topics {
                    for partition in &topic.partitions {
                        AbstractResponse::update_error_counts(&mut counts, Errors::for_code(partition.error_code));
                    }
                }
            }
        }
        counts
    }

    /// Parses an `OffsetFetchResponse` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = OffsetFetchResponseData::read(readable, version)?;
        Ok(Self::new(data, version))
    }

    /// Whether the client should throttle on this response (v4+).
    pub fn should_client_throttle(&self, version: i16) -> bool {
        version >= 4
    }

    /// Constructs an `OffsetFetchResponseGroup` carrying the given error for
    /// every topic-partition in the request group. Mirrors Java's static
    /// `groupError(OffsetFetchRequestGroup, Errors, int)`.
    pub fn group_error(group: &OffsetFetchRequestGroup, error: Errors, version: i16) -> OffsetFetchResponseGroup {
        let mut response = OffsetFetchResponseGroup::new();
        response.set_group_id(group.group_id.clone());
        if version >= OffsetFetchRequest::TOP_LEVEL_ERROR_AND_NULL_TOPICS_MIN_VERSION as i32 as i16 {
            response.set_error_code(error.code());
        } else {
            let topics: Vec<OffsetFetchResponseTopics> = group
                .topics
                .as_ref()
                .map(|topics| {
                    topics
                        .iter()
                        .map(|topic| {
                            let mut t = OffsetFetchResponseTopics::new();
                            t.set_name(topic.name.clone());
                            let partitions: Vec<OffsetFetchResponsePartitions> = topic
                                .partition_indexes
                                .iter()
                                .map(|&partition_index| {
                                    let mut p = OffsetFetchResponsePartitions::new();
                                    p.set_partition_index(partition_index);
                                    p.set_error_code(error.code());
                                    p.set_committed_offset(OffsetFetchRequest::INVALID_OFFSET);
                                    p.set_metadata(Some(OffsetFetchRequest::NO_METADATA.to_string()));
                                    p.set_committed_leader_epoch(RECORD_BATCH_NO_PARTITION_LEADER_EPOCH);
                                    p
                                })
                                .collect();
                            t.set_partitions(partitions);
                            t
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            response.set_topics(topics);
        }
        response
    }
}

/// Builder for [`OffsetFetchResponse`] when constructing from a list of
/// per-group response payloads. Mirrors Java's `OffsetFetchResponse.Builder`.
pub struct OffsetFetchResponseBuilder {
    groups: Vec<OffsetFetchResponseGroup>,
}

impl OffsetFetchResponseBuilder {
    /// Construct a builder over a single group's response.
    ///
    /// Mirrors Java's `Builder(OffsetFetchResponseGroup)`.
    pub fn new_group(group: OffsetFetchResponseGroup) -> Self {
        Self { groups: vec![group] }
    }

    /// Construct a builder over multiple groups (v8+ batched).
    ///
    /// Mirrors Java's `Builder(List<OffsetFetchResponseGroup>)`.
    pub fn new_groups(groups: Vec<OffsetFetchResponseGroup>) -> Self {
        Self { groups }
    }

    /// Build an [`OffsetFetchResponse`] for the given version.
    ///
    /// # Errors
    ///
    /// Returns `Err` if a v<8 build receives more than one group.
    pub fn build(self, version: i16) -> io::Result<OffsetFetchResponse> {
        let mut data = OffsetFetchResponseData::new();
        if version >= OffsetFetchRequest::BATCH_MIN_VERSION {
            data.set_groups(self.groups);
        } else {
            if self.groups.len() != 1 {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    format!("Version {version} of OffsetFetchResponse only supports one group."),
                ));
            }
            let group = self.groups.into_iter().next().expect("len == 1");
            data.set_error_code(group.error_code);
            let topics: Vec<OffsetFetchResponseTopic> = group
                .topics
                .into_iter()
                .map(|topic| {
                    let mut new_topic = OffsetFetchResponseTopic::new();
                    new_topic.set_name(topic.name);
                    let partitions: Vec<OffsetFetchResponsePartition> = topic
                        .partitions
                        .into_iter()
                        .map(|partition| {
                            let mut p = OffsetFetchResponsePartition::new();
                            p.set_partition_index(partition.partition_index);
                            p.set_error_code(partition.error_code);
                            p.set_committed_offset(partition.committed_offset);
                            p.set_metadata(partition.metadata);
                            p.set_committed_leader_epoch(partition.committed_leader_epoch);
                            p
                        })
                        .collect();
                    new_topic.set_partitions(partitions);
                    new_topic
                })
                .collect();
            data.set_topics(topics);
        }
        Ok(OffsetFetchResponse::new(data, version))
    }
}

impl std::fmt::Display for OffsetFetchResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_topic_response(name: &str, partition_index: i32, error_code: i16, offset: i64) -> OffsetFetchResponseTopic {
        let mut topic = OffsetFetchResponseTopic::new();
        topic.set_name(name.to_string());
        let mut partition = OffsetFetchResponsePartition::new();
        partition.set_partition_index(partition_index);
        partition.set_error_code(error_code);
        partition.set_committed_offset(offset);
        partition.set_metadata(Some(String::new()));
        partition.set_committed_leader_epoch(-1);
        topic.set_partitions(vec![partition]);
        topic
    }

    /// `group()` returns the requested group for v8+ responses.
    #[test]
    fn group_returns_requested_group_for_v8() {
        let mut data = OffsetFetchResponseData::new();
        let mut g1 = OffsetFetchResponseGroup::new();
        g1.set_group_id("g1".to_string());
        g1.set_error_code(Errors::None.code());
        let mut g2 = OffsetFetchResponseGroup::new();
        g2.set_group_id("g2".to_string());
        g2.set_error_code(Errors::None.code());
        data.set_groups(vec![g1, g2]);
        let response = OffsetFetchResponse::new(data, 8);
        let group = response.group("g2").expect("group present");
        assert_eq!(group.group_id, "g2");
    }

    /// `group()` returns the group id with the top-level error for v2..7.
    #[test]
    fn group_returns_error_for_v2_v7() {
        let mut data = OffsetFetchResponseData::new();
        data.set_error_code(Errors::NotCoordinator.code());
        let response = OffsetFetchResponse::new(data, 5);
        let group = response.group("g").expect("group present");
        assert_eq!(group.error_code, Errors::NotCoordinator.code());
    }

    /// `group()` synthesises the group view for v<2 responses.
    #[test]
    fn group_synthesises_for_v0_v1() {
        let mut data = OffsetFetchResponseData::new();
        data.set_topics(vec![make_topic_response("t", 0, Errors::None.code(), 100)]);
        let response = OffsetFetchResponse::new(data, 1);
        let group = response.group("g").expect("group present");
        assert_eq!(group.group_id, "g");
        // No error → returns topics translated to the new layout.
        assert_eq!(group.topics.len(), 1);
    }

    /// `top_level_error` returns the first non-partition-level error.
    #[test]
    fn top_level_error_returns_non_partition_error() {
        let mut data = OffsetFetchResponseData::new();
        data.set_topics(vec![
            // First topic has a partition-level error (UTOPP) — ignored.
            make_topic_response("t1", 0, Errors::UnknownTopicOrPartition.code(), -1),
            // Second topic has NotCoordinator — promotes to top level.
            make_topic_response("t2", 0, Errors::NotCoordinator.code(), -1),
        ]);
        assert_eq!(OffsetFetchResponse::top_level_error(&data), Errors::NotCoordinator);
    }

    /// `top_level_error` returns `None` when only partition-level errors are
    /// present.
    #[test]
    fn top_level_error_returns_none_for_partition_errors_only() {
        let mut data = OffsetFetchResponseData::new();
        data.set_topics(vec![make_topic_response("t", 0, Errors::UnknownTopicOrPartition.code(), -1)]);
        assert_eq!(OffsetFetchResponse::top_level_error(&data), Errors::None);
    }

    /// `error_counts` aggregates across groups for v8+ responses.
    #[test]
    fn error_counts_aggregates_groups_for_v8() {
        let mut data = OffsetFetchResponseData::new();
        let mut g = OffsetFetchResponseGroup::new();
        g.set_group_id("g".to_string());
        g.set_error_code(Errors::NotCoordinator.code());
        data.set_groups(vec![g]);
        let response = OffsetFetchResponse::new(data, 8);
        let counts = response.error_counts();
        assert_eq!(counts.get(&Errors::NotCoordinator).copied().unwrap_or(0), 1);
    }

    /// `should_client_throttle` returns true only for v4+.
    #[test]
    fn should_client_throttle_v4_threshold() {
        let response = OffsetFetchResponse::new(OffsetFetchResponseData::new(), 4);
        assert!(!response.should_client_throttle(3));
        assert!(response.should_client_throttle(4));
    }

    /// `Builder::build` rejects multi-group on v<8.
    #[test]
    fn builder_rejects_multi_group_below_v8() {
        let g1 = {
            let mut g = OffsetFetchResponseGroup::new();
            g.set_group_id("a".to_string());
            g
        };
        let g2 = {
            let mut g = OffsetFetchResponseGroup::new();
            g.set_group_id("b".to_string());
            g
        };
        let builder = OffsetFetchResponseBuilder::new_groups(vec![g1, g2]);
        let err = builder.build(7).unwrap_err();
        assert!(err.to_string().contains("only supports one group"));
    }

    /// `Builder::build` populates topics from the single group on v<8.
    #[test]
    fn builder_populates_topics_for_pre_batch() {
        let mut group = OffsetFetchResponseGroup::new();
        group.set_group_id("g".to_string());
        let mut topic = OffsetFetchResponseTopics::new();
        topic.set_name("t".to_string());
        let mut partition = OffsetFetchResponsePartitions::new();
        partition.set_partition_index(0);
        partition.set_committed_offset(123);
        partition.set_metadata(Some("m".to_string()));
        partition.set_error_code(Errors::None.code());
        partition.set_committed_leader_epoch(-1);
        topic.set_partitions(vec![partition]);
        group.set_topics(vec![topic]);

        let response = OffsetFetchResponseBuilder::new_group(group).build(5).expect("build ok");
        assert_eq!(response.data().topics.len(), 1);
        assert_eq!(response.data().topics[0].partitions[0].committed_offset, 123);
    }
}
