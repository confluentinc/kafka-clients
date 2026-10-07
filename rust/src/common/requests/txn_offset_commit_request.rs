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

//! `TxnOffsetCommit` request handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.TxnOffsetCommitRequest`.
//!
//! Sent by a transactional producer to commit consumer offsets as part of its
//! transaction (the read-process-write pattern), after
//! [`AddOffsetsToTxn`](super::AddOffsetsToTxnRequest) has registered the group.
//!
//! Version 6 (KIP-1319) identifies topics by **id** instead of by name. The
//! builder has two factories, as in Java: [`Builder::for_topic_names`] caps the
//! request at v5, and [`Builder::for_topic_ids_or_names`] allows v6; `build`
//! rejects a v6 request with a topic that has no id and a v0-5 request with a
//! topic that has no name. The producer picks `for_topic_ids_or_names` only when
//! every topic resolved to a known id (`TransactionManager.txnOffsetCommitHandler`),
//! so v6 is never negotiated for a request that cannot fill it.
//!
//! # Scope
//!
//! Java's single-argument `getErrorResponse(Throwable)` is an `AbstractRequest`
//! convenience that fills in a default throttle time; this codebase's dispatch
//! always supplies one, and no other translated wrapper has that form either.

use std::collections::HashMap;
use std::fmt;
use std::io;

use crate::TxnOffsetCommitRequestData;
use crate::TxnOffsetCommitResponseData;
use crate::common::TopicPartition;
use crate::common::Uuid;
use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::common::record::internal::RecordBatch;
use crate::txn_offset_commit_request_data::{TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic};
use crate::txn_offset_commit_response_data::{TxnOffsetCommitResponsePartition, TxnOffsetCommitResponseTopic};

use super::AbstractRequest;
use super::ConcreteResponse;
use super::RequestBuilder;
use super::RequestUtils;
use super::TxnOffsetCommitResponse;

/// An offset being committed inside a transaction.
///
/// Corresponds to the nested `TxnOffsetCommitRequest.CommittedOffset`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
#[doc(alias = "org.apache.kafka.common.requests.TxnOffsetCommitRequest$CommittedOffset")]
pub struct CommittedOffset {
    /// The offset to commit.
    pub(crate) offset: i64,
    /// Optional metadata to store alongside the offset.
    pub(crate) metadata: Option<String>,
    /// The leader epoch of the partition when the offset was read, if known.
    pub(crate) leader_epoch: Option<i32>,
}

impl CommittedOffset {
    /// Creates a committed offset.
    #[doc(alias = "org.apache.kafka.common.requests.TxnOffsetCommitRequest$CommittedOffset#CommittedOffset")]
    pub fn new(offset: i64, metadata: Option<String>, leader_epoch: Option<i32>) -> Self {
        Self { offset, metadata, leader_epoch }
    }

    /// The offset to commit.
    ///
    /// Java's public `CommittedOffset.offset`.
    pub fn offset(&self) -> i64 {
        self.offset
    }

    /// Optional metadata to store alongside the offset.
    ///
    /// Java's public `CommittedOffset.metadata`.
    pub fn metadata(&self) -> Option<&str> {
        self.metadata.as_deref()
    }

    /// The leader epoch of the partition when the offset was read, if known.
    ///
    /// Java's public `CommittedOffset.leaderEpoch`.
    pub fn leader_epoch(&self) -> Option<i32> {
        self.leader_epoch
    }
}

impl fmt::Display for CommittedOffset {
    /// Matches Java's `toString()` form character-for-character.
    ///
    /// Java interpolates the `Optional<Integer>` directly, which renders as
    /// `Optional[2]` / `Optional.empty` — **not** Rust's `Debug` form
    /// `Some(2)` / `None`. The epoch is formatted explicitly to reproduce Java's
    /// text, since this string is user-visible in logs.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CommittedOffset(offset={}, leaderEpoch=", self.offset)?;
        match self.leader_epoch {
            Some(epoch) => write!(f, "Optional[{epoch}]")?,
            None => write!(f, "Optional.empty")?,
        }
        write!(f, ", metadata='{}')", self.metadata.as_deref().unwrap_or("null"))
    }
}

/// A `TxnOffsetCommit` request.
///
/// Corresponds to `org.apache.kafka.common.requests.TxnOffsetCommitRequest`.
#[derive(Debug, Clone)]
#[doc(alias = "org.apache.kafka.common.requests.TxnOffsetCommitRequest")]
pub struct TxnOffsetCommitRequest {
    data: TxnOffsetCommitRequestData,
    version: i16,
}

impl TxnOffsetCommitRequest {
    /// Highest version predating KIP-890 Transaction V2.
    ///
    /// Corresponds to `TxnOffsetCommitRequest.LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2`.
    pub const LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2: i16 = 4;

    /// Sentinel for "no member id".
    ///
    /// Java reads this from `JoinGroupRequest.UNKNOWN_MEMBER_ID`. `JoinGroupRequest`
    /// belongs to the classic group protocol, which is out of scope for this port
    /// (`consumer-threading.md` §20), so the value is restated here rather than
    /// pulling in that class. Java's value is `""`.
    pub const UNKNOWN_MEMBER_ID: &str = "";

    /// Sentinel for "no generation id".
    ///
    /// Java reads this from `JoinGroupRequest.UNKNOWN_GENERATION_ID`; see
    /// [`Self::UNKNOWN_MEMBER_ID`] for why it is restated. Java's value is `-1`.
    pub const UNKNOWN_GENERATION_ID: i32 = -1;

    /// Returns `true` if the given version returns `GROUP_ID_NOT_FOUND` directly
    /// when the group is not found; `false` if the legacy mapping to
    /// `ILLEGAL_GENERATION` is used (KIP-1319).
    #[doc(alias = "org.apache.kafka.common.requests.TxnOffsetCommitRequest#supportsGroupIdNotFoundError")]
    pub fn supports_group_id_not_found_error(version: i16) -> bool {
        version >= 6
    }

    /// Returns `true` if the given version returns `STALE_MEMBER_EPOCH` directly
    /// when the member epoch is stale; `false` if the legacy mapping to
    /// `ILLEGAL_GENERATION` is used (KIP-1319).
    #[doc(alias = "org.apache.kafka.common.requests.TxnOffsetCommitRequest#supportsStaleMemberEpochError")]
    pub fn supports_stale_member_epoch_error(version: i16) -> bool {
        version >= 6
    }

    /// Creates a new `TxnOffsetCommitRequest` from data and version.
    #[doc(alias = "org.apache.kafka.common.requests.TxnOffsetCommitRequest#TxnOffsetCommitRequest")]
    pub fn new(data: TxnOffsetCommitRequestData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns a reference to the underlying data.
    #[doc(alias = "org.apache.kafka.common.requests.TxnOffsetCommitRequest#data")]
    pub fn data(&self) -> &TxnOffsetCommitRequestData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut TxnOffsetCommitRequestData {
        &mut self.data
    }

    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Returns the API key for this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::TXN_OFFSET_COMMIT
    }

    /// Flattens the wire topics back into a per-partition offset map.
    ///
    /// Corresponds to Java's `offsets()`. The leader epoch goes through
    /// [`RequestUtils::get_leader_epoch`], which maps the
    /// `NO_PARTITION_LEADER_EPOCH` sentinel to `None`. As in Java the map is keyed
    /// by topic **name**, so a v6 request built from topic ids alone yields
    /// empty-named partitions.
    #[doc(alias = "org.apache.kafka.common.requests.TxnOffsetCommitRequest#offsets")]
    pub fn offsets(&self) -> HashMap<TopicPartition, CommittedOffset> {
        let mut offset_map = HashMap::new();
        for topic in &self.data.topics {
            for partition in &topic.partitions {
                offset_map.insert(
                    TopicPartition::new(topic.name.clone(), partition.partition_index),
                    CommittedOffset::new(
                        partition.committed_offset,
                        partition.committed_metadata.clone(),
                        RequestUtils::get_leader_epoch(partition.committed_leader_epoch),
                    ),
                );
            }
        }
        offset_map
    }

    /// Groups an offset map into the wire topic list, without topic ids.
    ///
    /// Corresponds to Java's one-argument static `getTopics`, which forwards to the
    /// two-argument form with an empty id map: every topic gets the zero id.
    #[doc(alias = "org.apache.kafka.common.requests.TxnOffsetCommitRequest#getTopics")]
    pub fn get_topics(
        pending_txn_offset_commits: &HashMap<TopicPartition, CommittedOffset>,
    ) -> Vec<TxnOffsetCommitRequestTopic> {
        Self::get_topics_with_topic_ids(pending_txn_offset_commits, &HashMap::new())
    }

    /// Groups an offset map into the wire topic list, setting each topic's id
    /// from `topic_ids` (the zero id where it has none).
    ///
    /// Corresponds to Java's two-argument static `getTopics` (KIP-1319). Java
    /// groups through a `HashMap` and so has unspecified order; this sorts by
    /// topic name and then partition index for a deterministic encoding — see
    /// `.claude/rules/producer-transactions.md` §10.
    #[doc(alias = "org.apache.kafka.common.requests.TxnOffsetCommitRequest#getTopics")]
    pub fn get_topics_with_topic_ids(
        pending_txn_offset_commits: &HashMap<TopicPartition, CommittedOffset>,
        topic_ids: &HashMap<String, Uuid>,
    ) -> Vec<TxnOffsetCommitRequestTopic> {
        let mut by_topic: HashMap<&str, Vec<(i32, &CommittedOffset)>> = HashMap::new();
        for (topic_partition, offset) in pending_txn_offset_commits {
            by_topic
                .entry(topic_partition.topic())
                .or_default()
                .push((topic_partition.partition(), offset));
        }

        let mut names: Vec<&str> = by_topic.keys().copied().collect();
        names.sort_unstable();

        names
            .into_iter()
            .map(|name| {
                let mut entries = by_topic[name].clone();
                entries.sort_unstable_by_key(|(index, _)| *index);

                let partitions = entries
                    .into_iter()
                    .map(|(index, offset)| {
                        let mut partition = TxnOffsetCommitRequestPartition::new();
                        partition
                            .set_partition_index(index)
                            .set_committed_offset(offset.offset)
                            .set_committed_leader_epoch(
                                offset.leader_epoch.unwrap_or(RecordBatch::NO_PARTITION_LEADER_EPOCH),
                            )
                            .set_committed_metadata(offset.metadata.clone());
                        partition
                    })
                    .collect();

                let mut topic = TxnOffsetCommitRequestTopic::new();
                topic
                    .set_name(name.to_string())
                    .set_topic_id(topic_ids.get(name).copied().unwrap_or_else(Uuid::zero))
                    .set_partitions(partitions);
                topic
            })
            .collect()
    }

    /// Builds the canonical error response for this request, matching Java's
    /// `TxnOffsetCommitRequest.getErrorResponse(throttleTimeMs, Throwable)`.
    ///
    /// The error is reported per partition — this response has no top-level error
    /// code at any version. Java delegates to the static form
    /// ([`Self::get_error_response_with_request`]) and then sets the throttle time.
    #[doc(alias = "org.apache.kafka.common.requests.TxnOffsetCommitRequest#getErrorResponse")]
    pub fn get_error_response(&self, throttle_time_ms: i32, error: &Errors) -> ConcreteResponse {
        let mut response = Self::get_error_response_with_request(&self.data, error);
        response.set_throttle_time_ms(throttle_time_ms);
        ConcreteResponse::TxnOffsetCommit(TxnOffsetCommitResponse::with_data(response))
    }

    /// Mirrors `request`'s topic/partition shape with `error` on every partition,
    /// carrying each topic's id **and** name over (KIP-1319).
    ///
    /// Corresponds to Java's static
    /// `getErrorResponse(TxnOffsetCommitRequestData, Errors)`. It shares its Java
    /// name with the instance method above, so it takes the `_with_request` suffix
    /// (CLAUDE.md §2: the parameter that discriminates it is `request`).
    #[doc(alias = "org.apache.kafka.common.requests.TxnOffsetCommitRequest#getErrorResponse")]
    pub fn get_error_response_with_request(
        request: &TxnOffsetCommitRequestData,
        error: &Errors,
    ) -> TxnOffsetCommitResponseData {
        let topics = request
            .topics
            .iter()
            .map(|request_topic| {
                let partitions = request_topic
                    .partitions
                    .iter()
                    .map(|request_partition| {
                        let mut partition = TxnOffsetCommitResponsePartition::new();
                        partition
                            .set_partition_index(request_partition.partition_index)
                            .set_error_code(error.code());
                        partition
                    })
                    .collect();
                let mut topic = TxnOffsetCommitResponseTopic::new();
                topic
                    .set_topic_id(request_topic.topic_id)
                    .set_name(request_topic.name.clone())
                    .set_partitions(partitions);
                topic
            })
            .collect();
        let mut response = TxnOffsetCommitResponseData::new();
        response.set_topics(topics);
        response
    }

    /// Parses a `TxnOffsetCommitRequest` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    #[doc(alias = "org.apache.kafka.common.requests.TxnOffsetCommitRequest#parse")]
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = TxnOffsetCommitRequestData::read(readable, version)?;
        Ok(Self::new(data, version))
    }
}

impl fmt::Display for TxnOffsetCommitRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TxnOffsetCommitRequest(version={}, data={})", self.version, self.data)
    }
}

/// Builder for [`TxnOffsetCommitRequest`].
///
/// Corresponds to `TxnOffsetCommitRequest.Builder` in Java. Java's constructor is
/// private; the two static factories mirror Java's:
///
/// - [`Self::for_topic_names`] — capped at v5 (v4 without Transaction V2), so the
///   request is guaranteed to use topic names.
/// - [`Self::for_topic_ids_or_names`] — allows v6, which uses topic ids.
#[derive(Debug, Clone)]
#[doc(alias = "org.apache.kafka.common.requests.TxnOffsetCommitRequest$Builder")]
pub struct Builder {
    data: TxnOffsetCommitRequestData,
    oldest_allowed_version: i16,
    latest_allowed_version: i16,
}

impl Builder {
    /// Java's private `Builder(data, oldestAllowedVersion, latestAllowedVersion)`,
    /// which calls `super(ApiKeys.TXN_OFFSET_COMMIT, oldest, latest)`.
    fn new(data: TxnOffsetCommitRequestData, oldest_allowed_version: i16, latest_allowed_version: i16) -> Self {
        Self { data, oldest_allowed_version, latest_allowed_version }
    }

    /// Builds a request that uses topic names: capped at v5, or at
    /// [`TxnOffsetCommitRequest::LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2`]
    /// without Transaction V2.
    ///
    /// Mirrors Java's `Builder.forTopicNames(TxnOffsetCommitRequestData, boolean)`.
    /// Java passes the two bounds as **constants** (`(short) 5` and
    /// `LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2`), so they are constants here too
    /// (`producer-transactions.md` §12).
    #[doc(alias = "org.apache.kafka.common.requests.TxnOffsetCommitRequest$Builder#forTopicNames")]
    pub fn for_topic_names(data: TxnOffsetCommitRequestData, is_transaction_v2_enabled: bool) -> Self {
        Self::new(
            data,
            ApiKeys::TXN_OFFSET_COMMIT.oldest_version(),
            if is_transaction_v2_enabled {
                5
            } else {
                TxnOffsetCommitRequest::LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2
            },
        )
    }

    /// Builds a request that may use topic ids (v6+) or topic names: up to the
    /// latest version, or
    /// [`TxnOffsetCommitRequest::LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2`]
    /// without Transaction V2.
    ///
    /// Mirrors Java's `Builder.forTopicIdsOrNames(TxnOffsetCommitRequestData, boolean)`.
    /// Java deliberately passes the unstable-inclusive
    /// `ApiKeys.TXN_OFFSET_COMMIT.latestVersion()` (`TxnOffsetCommitRequest.java:94`),
    /// so this is [`ApiKeys::latest_version`], not the released-only accessor
    /// (`producer-transactions.md` §12). v6 is stable in 4.4 (b9945c8e84), so the
    /// two accessors agree today.
    #[doc(alias = "org.apache.kafka.common.requests.TxnOffsetCommitRequest$Builder#forTopicIdsOrNames")]
    pub fn for_topic_ids_or_names(data: TxnOffsetCommitRequestData, is_transaction_v2_enabled: bool) -> Self {
        Self::new(
            data,
            ApiKeys::TXN_OFFSET_COMMIT.oldest_version(),
            if is_transaction_v2_enabled {
                ApiKeys::TXN_OFFSET_COMMIT.latest_version()
            } else {
                TxnOffsetCommitRequest::LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2
            },
        )
    }

    /// Returns a reference to the underlying data.
    ///
    /// Java's public `Builder.data` field.
    pub fn data(&self) -> &TxnOffsetCommitRequestData {
        &self.data
    }

    /// Whether any consumer-group metadata field is set.
    ///
    /// Corresponds to Java's private `groupMetadataSet()`. Note it is an **OR**
    /// across all three fields, so setting any one of them requires v3+.
    #[doc(alias = "org.apache.kafka.common.requests.TxnOffsetCommitRequest$Builder#groupMetadataSet")]
    fn group_metadata_set(&self) -> bool {
        self.data.member_id != TxnOffsetCommitRequest::UNKNOWN_MEMBER_ID
            || self.data.generation_id_or_member_epoch != TxnOffsetCommitRequest::UNKNOWN_GENERATION_ID
            || self.data.group_instance_id.is_some()
    }
}

impl RequestBuilder for Builder {
    fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::TXN_OFFSET_COMMIT
    }

    fn oldest_allowed_version(&self) -> i16 {
        self.oldest_allowed_version
    }

    fn latest_allowed_version(&self) -> i16 {
        self.latest_allowed_version
    }

    /// Builds at `version`, rejecting group metadata below v3, a topic without an
    /// id at v6+, and a topic without a name below v6.
    ///
    /// Mirrors Java's `Builder.build(short)`. Java throws
    /// `UnsupportedVersionException`; per CLAUDE.md §12.2 this is a `Result`, with
    /// Java's message text. The topic checks are where Java enforces the
    /// `ignorable` `Name` / `TopicId` fields (`producer-transactions.md` §11): the
    /// generated writer drops either silently.
    fn build_version(&mut self, version: i16) -> io::Result<AbstractRequest> {
        if version < 3 && self.group_metadata_set() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!(
                    "Broker doesn't support group metadata commit API on version {version}, \
                     minimum supported request version is 3 which requires brokers to be on \
                     version 2.5 or above."
                ),
            ));
        }
        if version >= 6 {
            // Java also tests `topicId() == null`; a Rust `Uuid` cannot be null.
            if self.data.topics.iter().any(|topic| topic.topic_id == Uuid::zero()) {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    format!("The broker TxnOffsetCommit api version {version} does require usage of topic ids."),
                ));
            }
        } else if self.data.topics.iter().any(|topic| topic.name.is_empty()) {
            // Java also tests `name() == null`; the field is a non-nullable string.
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!("The broker TxnOffsetCommit api version {version} does require usage of topic names."),
            ));
        }

        Ok(AbstractRequest::TxnOffsetCommit(TxnOffsetCommitRequest::new(
            self.data.clone(),
            version,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Fixture values of `OffsetCommitRequestTest` (Java 49-62), which
    // `TxnOffsetCommitRequestTest` extends.
    const GROUP_ID: &str = "groupId";
    const MEMBER_ID: &str = "consumerId";
    const GROUP_INSTANCE_ID: &str = "groupInstanceId";
    const TOPIC_ONE: &str = "topicOne";
    const TOPIC_TWO: &str = "topicTwo";
    const PARTITION_ONE: i32 = 1;
    const PARTITION_TWO: i32 = 2;
    const OFFSET: i64 = 100;
    const LEADER_EPOCH: i32 = 20;
    const METADATA: &str = "metadata";
    const THROTTLE_TIME_MS: i32 = 10;

    fn tp(topic: &str, partition: i32) -> TopicPartition {
        TopicPartition::new(topic.to_string(), partition)
    }

    fn unwrap_request(request: AbstractRequest) -> TxnOffsetCommitRequest {
        match request {
            AbstractRequest::TxnOffsetCommit(request) => request,
            other => panic!("expected a TxnOffsetCommit request, got {other:?}"),
        }
    }

    /// Java's `OFFSETS` (`TxnOffsetCommitRequestTest.setUp`, Java 59-68).
    fn java_offsets() -> HashMap<TopicPartition, CommittedOffset> {
        HashMap::from([
            (
                tp(TOPIC_ONE, PARTITION_ONE),
                CommittedOffset::new(OFFSET, Some(METADATA.to_string()), Some(LEADER_EPOCH)),
            ),
            (
                tp(TOPIC_TWO, PARTITION_TWO),
                CommittedOffset::new(OFFSET, Some(METADATA.to_string()), Some(LEADER_EPOCH)),
            ),
        ])
    }

    /// Java's `builder` (`setUp`, Java 70-75): no group metadata.
    fn java_builder() -> Builder {
        let mut data = TxnOffsetCommitRequestData::new();
        data.set_transactional_id("transactionalId".to_string())
            .set_group_id(GROUP_ID.to_string())
            .set_producer_id(10)
            .set_producer_epoch(1)
            .set_topics(TxnOffsetCommitRequest::get_topics(&java_offsets()));
        Builder::for_topic_names(data, true)
    }

    /// Java's `builderWithGroupMetadata` (`setUp`, Java 77-87).
    fn java_builder_with_group_metadata() -> Builder {
        let mut data = TxnOffsetCommitRequestData::new();
        data.set_transactional_id("transactionalId".to_string())
            .set_group_id(GROUP_ID.to_string())
            .set_producer_id(10)
            .set_producer_epoch(1)
            .set_member_id(MEMBER_ID.to_string())
            .set_generation_id_or_member_epoch(5)
            .set_group_instance_id(Some(GROUP_INSTANCE_ID.to_string()))
            .set_topics(TxnOffsetCommitRequest::get_topics(&java_offsets()));
        Builder::for_topic_names(data, true)
    }

    fn request_partition(index: i32, offset: i64) -> TxnOffsetCommitRequestPartition {
        let mut partition = TxnOffsetCommitRequestPartition::new();
        partition.set_partition_index(index).set_committed_offset(offset);
        partition
    }

    /// The data of `testForTopicIdsOrNamesWithTopicNameOnly` /
    /// `...WithTopicIdOnly` (Java 179-226): one topic with one partition.
    fn single_topic_data(name: &str, topic_id: Uuid) -> TxnOffsetCommitRequestData {
        let mut topic = TxnOffsetCommitRequestTopic::new();
        topic
            .set_name(name.to_string())
            .set_topic_id(topic_id)
            .set_partitions(vec![request_partition(0, 0)]);
        let mut data = TxnOffsetCommitRequestData::new();
        data.set_transactional_id("tx".to_string())
            .set_group_id(GROUP_ID.to_string())
            .set_producer_id(1)
            .set_producer_epoch(0)
            .set_topics(vec![topic]);
        data
    }

    /// Translated from `TxnOffsetCommitRequestTest.testConstructor`
    /// (`@ApiKeyVersionsSource(toVersion = 5)`, so v0-5).
    ///
    /// The expected topic list is in Java's `List.of` order, which the sorted
    /// grouping (rules §10) reproduces: `topicOne` < `topicTwo`.
    #[test]
    #[doc(alias = "org.apache.kafka.common.requests.TxnOffsetCommitRequestTest#testConstructor")]
    fn test_constructor() {
        let expected_topics: Vec<TxnOffsetCommitRequestTopic> =
            [(TOPIC_ONE, PARTITION_ONE), (TOPIC_TWO, PARTITION_TWO)]
                .into_iter()
                .map(|(name, index)| {
                    let mut partition = request_partition(index, OFFSET);
                    partition
                        .set_committed_leader_epoch(LEADER_EPOCH)
                        .set_committed_metadata(Some(METADATA.to_string()));
                    let mut topic = TxnOffsetCommitRequestTopic::new();
                    topic.set_name(name.to_string()).set_partitions(vec![partition]);
                    topic
                })
                .collect();

        for version in ApiKeys::TXN_OFFSET_COMMIT.oldest_version()..=5 {
            let request = unwrap_request(if version < 3 {
                java_builder().build_version(version).expect("build")
            } else {
                java_builder_with_group_metadata().build_version(version).expect("build")
            });
            assert_eq!(request.offsets(), java_offsets(), "v{version}");
            assert_eq!(
                TxnOffsetCommitRequest::get_topics(&request.offsets()),
                expected_topics,
                "v{version}"
            );

            let ConcreteResponse::TxnOffsetCommit(response) =
                request.get_error_response(THROTTLE_TIME_MS, &Errors::NotCoordinator)
            else {
                panic!("expected a TxnOffsetCommit response");
            };
            assert_eq!(
                response.error_counts(),
                HashMap::from([(Errors::NotCoordinator, 2)]),
                "v{version}"
            );
            assert_eq!(response.throttle_time_ms(), THROTTLE_TIME_MS, "v{version}");
        }
    }

    /// Translated from `TxnOffsetCommitRequestTest.testGetErrorResponse`.
    ///
    /// Both the static form and the instance form carry each topic's id **and**
    /// name into the response (KIP-1319).
    #[test]
    #[doc(alias = "org.apache.kafka.common.requests.TxnOffsetCommitRequestTest#testGetErrorResponse")]
    fn test_get_error_response() {
        let topic_one_id = Uuid::random_uuid();
        let topic_two_id = Uuid::random_uuid();

        let request_topics = [
            (topic_one_id, TOPIC_ONE, PARTITION_ONE),
            (topic_two_id, TOPIC_TWO, PARTITION_TWO),
        ]
        .into_iter()
        .map(|(id, name, index)| {
            let mut topic = TxnOffsetCommitRequestTopic::new();
            topic
                .set_topic_id(id)
                .set_name(name.to_string())
                .set_partitions(vec![request_partition(index, OFFSET)]);
            topic
        })
        .collect();
        let mut data = TxnOffsetCommitRequestData::new();
        data.set_transactional_id("transactionalId".to_string())
            .set_group_id(GROUP_ID.to_string())
            .set_producer_id(10)
            .set_producer_epoch(1)
            .set_topics(request_topics);

        let response_topics = [
            (topic_one_id, TOPIC_ONE, PARTITION_ONE),
            (topic_two_id, TOPIC_TWO, PARTITION_TWO),
        ]
        .into_iter()
        .map(|(id, name, index)| {
            let mut partition = TxnOffsetCommitResponsePartition::new();
            partition
                .set_partition_index(index)
                .set_error_code(Errors::UnknownMemberId.code());
            let mut topic = TxnOffsetCommitResponseTopic::new();
            topic
                .set_topic_id(id)
                .set_name(name.to_string())
                .set_partitions(vec![partition]);
            topic
        })
        .collect();
        let mut expected_response_data = TxnOffsetCommitResponseData::new();
        expected_response_data.set_topics(response_topics);

        assert_eq!(
            TxnOffsetCommitRequest::get_error_response_with_request(&data, &Errors::UnknownMemberId),
            expected_response_data
        );

        let request = unwrap_request(Builder::for_topic_ids_or_names(data, true).build().expect("build"));
        let ConcreteResponse::TxnOffsetCommit(response) =
            request.get_error_response(THROTTLE_TIME_MS, &Errors::UnknownMemberId)
        else {
            panic!("expected a TxnOffsetCommit response");
        };
        expected_response_data.set_throttle_time_ms(THROTTLE_TIME_MS);
        assert_eq!(response.data(), &expected_response_data);
    }

    /// Translated from
    /// `TxnOffsetCommitRequestTest.testVersionSupportForGroupMetadata`
    /// (`toVersion = 5`).
    #[test]
    #[doc(alias = "org.apache.kafka.common.requests.TxnOffsetCommitRequestTest#testVersionSupportForGroupMetadata")]
    fn test_version_support_for_group_metadata() {
        for version in ApiKeys::TXN_OFFSET_COMMIT.oldest_version()..=5 {
            java_builder()
                .build_version(version)
                .unwrap_or_else(|error| panic!("v{version} without metadata must build: {error}"));
            if version >= 3 {
                java_builder_with_group_metadata()
                    .build_version(version)
                    .unwrap_or_else(|error| panic!("v{version} with metadata must build: {error}"));
            } else {
                let error = java_builder_with_group_metadata()
                    .build_version(version)
                    .expect_err("must reject below v3");
                assert_eq!(error.kind(), io::ErrorKind::Unsupported);
                assert_eq!(
                    error.to_string(),
                    format!(
                        "Broker doesn't support group metadata commit API on version {version}, \
                         minimum supported request version is 3 which requires brokers to be on \
                         version 2.5 or above."
                    )
                );
            }
        }
    }

    /// Translated from
    /// `TxnOffsetCommitRequestTest.testForTopicIdsOrNamesWithTopicNameOnly`, over
    /// every version. Java asserts only the class at v6; the message is asserted
    /// here too (`definition-of-done.md` §3).
    #[test]
    #[doc(
        alias = "org.apache.kafka.common.requests.TxnOffsetCommitRequestTest#testForTopicIdsOrNamesWithTopicNameOnly"
    )]
    fn test_for_topic_ids_or_names_with_topic_name_only() {
        let data = single_topic_data("foo", Uuid::zero());
        for version in ApiKeys::TXN_OFFSET_COMMIT.oldest_version()..=ApiKeys::TXN_OFFSET_COMMIT.latest_version() {
            let result = Builder::for_topic_ids_or_names(data.clone(), true).build_version(version);
            if version >= 6 {
                let error = result.expect_err("a name-only request cannot be built at v6+");
                assert_eq!(error.kind(), io::ErrorKind::Unsupported);
                assert_eq!(
                    error.to_string(),
                    format!("The broker TxnOffsetCommit api version {version} does require usage of topic ids.")
                );
            } else {
                result.unwrap_or_else(|error| panic!("v{version} with a topic name must build: {error}"));
            }
        }
    }

    /// Translated from
    /// `TxnOffsetCommitRequestTest.testForTopicIdsOrNamesWithTopicIdOnly`, over
    /// every version, with the message asserted below v6.
    #[test]
    #[doc(alias = "org.apache.kafka.common.requests.TxnOffsetCommitRequestTest#testForTopicIdsOrNamesWithTopicIdOnly")]
    fn test_for_topic_ids_or_names_with_topic_id_only() {
        let data = single_topic_data("", Uuid::random_uuid());
        for version in ApiKeys::TXN_OFFSET_COMMIT.oldest_version()..=ApiKeys::TXN_OFFSET_COMMIT.latest_version() {
            let result = Builder::for_topic_ids_or_names(data.clone(), true).build_version(version);
            if version >= 6 {
                let request = unwrap_request(result.expect("an id-only request builds at v6+"));
                assert_eq!(request.data(), &data);
            } else {
                let error = result.expect_err("an id-only request cannot be built below v6");
                assert_eq!(error.kind(), io::ErrorKind::Unsupported);
                assert_eq!(
                    error.to_string(),
                    format!("The broker TxnOffsetCommit api version {version} does require usage of topic names.")
                );
            }
        }
    }

    #[test]
    #[doc(
        alias = "org.apache.kafka.common.requests.TxnOffsetCommitRequestTest#testForTopicNamesCapsAtTransactionV1WhenTransactionV2IsDisabled"
    )]
    fn test_for_topic_names_caps_at_transaction_v1_when_transaction_v2_is_disabled() {
        let builder = Builder::for_topic_names(TxnOffsetCommitRequestData::new(), false);
        assert_eq!(
            builder.latest_allowed_version(),
            TxnOffsetCommitRequest::LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2
        );
    }

    #[test]
    #[doc(
        alias = "org.apache.kafka.common.requests.TxnOffsetCommitRequestTest#testForTopicNamesCapsAtV5WhenTransactionV2IsEnabled"
    )]
    fn test_for_topic_names_caps_at_v5_when_transaction_v2_is_enabled() {
        let builder = Builder::for_topic_names(TxnOffsetCommitRequestData::new(), true);
        assert_eq!(builder.latest_allowed_version(), 5);
    }

    #[test]
    #[doc(
        alias = "org.apache.kafka.common.requests.TxnOffsetCommitRequestTest#testForTopicIdsOrNamesCapsAtTransactionV1WhenTransactionV2IsDisabled"
    )]
    fn test_for_topic_ids_or_names_caps_at_transaction_v1_when_transaction_v2_is_disabled() {
        let builder = Builder::for_topic_ids_or_names(TxnOffsetCommitRequestData::new(), false);
        assert_eq!(
            builder.latest_allowed_version(),
            TxnOffsetCommitRequest::LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2
        );
    }

    #[test]
    #[doc(
        alias = "org.apache.kafka.common.requests.TxnOffsetCommitRequestTest#testForTopicIdsOrNamesUsesLatestVersionWhenTransactionV2IsEnabled"
    )]
    fn test_for_topic_ids_or_names_uses_latest_version_when_transaction_v2_is_enabled() {
        let builder = Builder::for_topic_ids_or_names(TxnOffsetCommitRequestData::new(), true);
        assert_eq!(builder.latest_allowed_version(), ApiKeys::TXN_OFFSET_COMMIT.latest_version());
        // v6 is stable in 4.4 (b9945c8e84): the released-only accessor reaches it
        // too, so the builder's bound is not an unreleased version.
        assert_eq!(builder.latest_allowed_version(), 6);
        assert_eq!(ApiKeys::TXN_OFFSET_COMMIT.latest_version_enable_unstable_last_version(false), 6);
        assert_eq!(builder.oldest_allowed_version(), ApiKeys::TXN_OFFSET_COMMIT.oldest_version());
    }

    /// `supportsGroupIdNotFoundError` / `supportsStaleMemberEpochError`
    /// (723847904b) both switch at v6. Java tests them only through the broker's
    /// `OffsetMetadataManagerTest`, which has no client counterpart.
    #[test]
    fn test_supports_kip_1319_errors_from_v6() {
        for version in ApiKeys::TXN_OFFSET_COMMIT.oldest_version()..=ApiKeys::TXN_OFFSET_COMMIT.latest_version() {
            assert_eq!(
                TxnOffsetCommitRequest::supports_group_id_not_found_error(version),
                version >= 6,
                "v{version}"
            );
            assert_eq!(
                TxnOffsetCommitRequest::supports_stale_member_epoch_error(version),
                version >= 6,
                "v{version}"
            );
        }
    }

    /// `group_metadata_set` is an OR: **any** of the three fields triggers it.
    #[test]
    fn test_group_metadata_set_is_an_or_across_all_three_fields() {
        let builder = |setup: &dyn Fn(&mut TxnOffsetCommitRequestData)| {
            let mut data = TxnOffsetCommitRequestData::new();
            setup(&mut data);
            Builder::for_topic_names(data, true)
        };
        assert!(!builder(&|_| {}).group_metadata_set());
        assert!(
            builder(&|data| {
                data.set_member_id("m".to_string());
            })
            .group_metadata_set(),
            "member id alone must count"
        );
        assert!(
            builder(&|data| {
                data.set_generation_id_or_member_epoch(0);
            })
            .group_metadata_set(),
            "generation id alone must count"
        );
        assert!(
            builder(&|data| {
                data.set_group_instance_id(Some("i".to_string()));
            })
            .group_metadata_set(),
            "group instance id alone must count"
        );
    }

    /// The group-metadata check runs first, so a request that also lacks topic
    /// names reports the group-metadata problem, as Java's order does.
    #[test]
    fn test_group_metadata_check_precedes_the_topic_checks() {
        let mut data = single_topic_data("", Uuid::random_uuid());
        data.set_member_id("member".to_string());
        let error = Builder::for_topic_ids_or_names(data, true)
            .build_version(2)
            .expect_err("both checks fail at v2");
        assert!(
            error
                .to_string()
                .starts_with("Broker doesn't support group metadata commit API on version 2"),
            "{error}"
        );
    }

    /// 4.4 drops the `build()` clamp: `forTopicNames(data, false)` bounds the
    /// negotiated version through `latest_allowed_version` instead, and a version
    /// passed to `build_version` is used as given.
    #[test]
    fn test_build_version_uses_the_requested_version() {
        for version in ApiKeys::TXN_OFFSET_COMMIT.oldest_version()..=5 {
            let request = unwrap_request(
                Builder::for_topic_names(single_topic_data("foo", Uuid::zero()), false)
                    .build_version(version)
                    .expect("build"),
            );
            assert_eq!(request.version(), version);
        }
    }

    /// Grouping is deterministic: topics by name, partitions by index
    /// (rules §10). Java's `HashMap` gives no such guarantee.
    #[test]
    fn test_get_topics_is_deterministic() {
        let mut map = HashMap::new();
        for (topic, partition) in [("topic-b", 5), ("topic-a", 9), ("topic-b", 1), ("topic-a", 0)] {
            map.insert(tp(topic, partition), CommittedOffset::new(1, None, None));
        }

        let topics = TxnOffsetCommitRequest::get_topics(&map);
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
        assert!(topics.iter().all(|topic| topic.topic_id == Uuid::zero()));
    }

    /// The two-argument `getTopics` sets each topic's id from the map and the zero
    /// id for a topic it does not know — exactly Java's `getOrDefault(.., ZERO_UUID)`.
    #[test]
    fn test_get_topics_with_topic_ids_sets_known_ids_and_zero_otherwise() {
        let known = Uuid::random_uuid();
        let map = HashMap::from([
            (tp("known", 0), CommittedOffset::new(1, None, None)),
            (tp("unknown", 0), CommittedOffset::new(2, None, None)),
        ]);
        let topics =
            TxnOffsetCommitRequest::get_topics_with_topic_ids(&map, &HashMap::from([("known".to_string(), known)]));
        assert_eq!(topics.len(), 2);
        assert_eq!((topics[0].name.as_str(), topics[0].topic_id), ("known", known));
        assert_eq!((topics[1].name.as_str(), topics[1].topic_id), ("unknown", Uuid::zero()));
    }

    /// A `None` leader epoch is encoded as the sentinel and decoded back to
    /// `None` — not to `Some(-1)`.
    #[test]
    fn test_absent_leader_epoch_survives_the_sentinel_round_trip() {
        let mut map = HashMap::new();
        map.insert(tp("topic-a", 0), CommittedOffset::new(7, None, None));

        let topics = TxnOffsetCommitRequest::get_topics(&map);
        assert_eq!(
            topics[0].partitions[0].committed_leader_epoch,
            RecordBatch::NO_PARTITION_LEADER_EPOCH,
            "absent epoch encodes as the sentinel"
        );

        let mut data = TxnOffsetCommitRequestData::new();
        data.set_topics(topics);
        let request = TxnOffsetCommitRequest::new(data, 0);
        assert_eq!(request.offsets()[&tp("topic-a", 0)].leader_epoch, None);
    }

    #[test]
    fn test_committed_offset_display_and_equality() {
        let offset = CommittedOffset::new(5, Some("m".to_string()), Some(2));
        // Java interpolates the Optional directly, giving `Optional[2]` /
        // `Optional.empty` — not Rust's Debug form `Some(2)` / `None`.
        assert_eq!(
            offset.to_string(),
            "CommittedOffset(offset=5, leaderEpoch=Optional[2], metadata='m')"
        );
        assert_eq!(offset, CommittedOffset::new(5, Some("m".to_string()), Some(2)));
        assert_ne!(offset, CommittedOffset::new(6, Some("m".to_string()), Some(2)));
        assert_ne!(offset, CommittedOffset::new(5, None, Some(2)));
        assert_ne!(offset, CommittedOffset::new(5, Some("m".to_string()), None));

        let absent = CommittedOffset::new(5, None, None);
        assert_eq!(
            absent.to_string(),
            "CommittedOffset(offset=5, leaderEpoch=Optional.empty, metadata='null')"
        );
    }

    #[test]
    fn test_api_key_and_version() {
        let builder = java_builder();
        assert_eq!(builder.api_key(), &ApiKeys::TXN_OFFSET_COMMIT);
        let request = TxnOffsetCommitRequest::new(TxnOffsetCommitRequestData::new(), 3);
        assert_eq!(request.api_key(), &ApiKeys::TXN_OFFSET_COMMIT);
        assert_eq!(request.version(), 3);
    }

    /// `RequestResponseTest.createTxnOffsetCommitRequest(short)`: names below v6
    /// through `forTopicNames(data, version >= 5)`, ids from v6 through
    /// `forTopicIdsOrNames(data, true)`, with group metadata from v3.
    fn create_txn_offset_commit_request(version: i16) -> TxnOffsetCommitRequest {
        let offsets = HashMap::from([
            (tp("topic", 73), CommittedOffset::new(100, None, None)),
            (tp("topic", 74), CommittedOffset::new(100, Some("blah".to_string()), Some(27))),
        ]);
        let topic_ids = HashMap::from([("topic".to_string(), Uuid::random_uuid())]);
        let mut data = TxnOffsetCommitRequestData::new();
        data.set_transactional_id("transactionalId".to_string())
            .set_group_id("groupId".to_string())
            .set_producer_id(21)
            .set_producer_epoch(42)
            .set_topics(TxnOffsetCommitRequest::get_topics_with_topic_ids(&offsets, &topic_ids));
        if version >= 3 {
            data.set_member_id("member".to_string())
                .set_generation_id_or_member_epoch(2)
                .set_group_instance_id(Some("instance".to_string()));
        }
        let mut builder = if version >= 6 {
            Builder::for_topic_ids_or_names(data, true)
        } else {
            Builder::for_topic_names(data, version >= 5)
        };
        unwrap_request(builder.build_version(version).expect("build"))
    }

    /// `RequestResponseTest`'s `TXN_OFFSET_COMMIT` arm of the per-version
    /// serialization check (`checkRequest` over `createTxnOffsetCommitRequest`),
    /// plus its error response at the same version (`checkErrorResponse`).
    ///
    /// `CommittedLeaderEpoch` is a v2+ field marked `"ignorable": true`, so below
    /// v2 it is absent from the wire and decodes back to the sentinel; Java drops
    /// it identically (`producer-transactions.md` §11). Likewise `Name` (v0-5)
    /// and `TopicId` (v6+) are both ignorable, so each is dropped at the versions
    /// that lack it.
    #[test]
    #[doc(alias = "org.apache.kafka.common.requests.RequestResponseTest#testSerialization")]
    fn test_request_response_serialization_all_versions() {
        for version in ApiKeys::TXN_OFFSET_COMMIT.oldest_version()..=ApiKeys::TXN_OFFSET_COMMIT.latest_version() {
            let request = create_txn_offset_commit_request(version);
            let mut expected = request.data().clone();
            for topic in expected.topics_mut() {
                if version >= 6 {
                    topic.set_name(String::new());
                } else {
                    topic.set_topic_id(Uuid::zero());
                }
                if version < 2 {
                    for partition in topic.partitions_mut() {
                        partition.set_committed_leader_epoch(RecordBatch::NO_PARTITION_LEADER_EPOCH);
                    }
                }
            }

            let mut serialized = AbstractRequest::TxnOffsetCommit(request.clone())
                .serialize()
                .expect("serialize");
            serialized.flip();
            let parsed = TxnOffsetCommitRequest::parse(&mut serialized, version).expect("parse");
            assert_eq!(parsed.version(), version);
            assert_eq!(parsed.data(), &expected, "v{version}");

            let mut error_response = request.get_error_response(0, &Errors::UnknownServerError);
            let mut buffer = error_response.serialize(version).expect("serialize error response");
            buffer.flip();
            let parsed_response = TxnOffsetCommitResponse::parse(&mut buffer, version).expect("parse");
            assert_eq!(
                parsed_response.error_counts(),
                HashMap::from([(Errors::UnknownServerError, 2)]),
                "v{version}"
            );
        }
    }

    /// `RequestResponseTest.createTxnOffsetCommitRequestWithAutoDowngrade`:
    /// `forTopicNames(data, false).build()` builds at the builder's latest allowed
    /// version, which without Transaction V2 is v4 — group metadata included.
    #[test]
    #[doc(alias = "org.apache.kafka.common.requests.RequestResponseTest#testSerialization")]
    fn test_create_txn_offset_commit_request_with_auto_downgrade() {
        let offsets = HashMap::from([
            (tp("topic", 73), CommittedOffset::new(100, None, None)),
            (tp("topic", 74), CommittedOffset::new(100, Some("blah".to_string()), Some(27))),
        ]);
        let mut data = TxnOffsetCommitRequestData::new();
        data.set_transactional_id("transactionalId".to_string())
            .set_group_id("groupId".to_string())
            .set_producer_id(21)
            .set_producer_epoch(42)
            .set_member_id("member".to_string())
            .set_generation_id_or_member_epoch(2)
            .set_group_instance_id(Some("instance".to_string()))
            .set_topics(TxnOffsetCommitRequest::get_topics(&offsets));
        let request = unwrap_request(Builder::for_topic_names(data, false).build().expect("build"));
        assert_eq!(
            request.version(),
            TxnOffsetCommitRequest::LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2
        );

        let mut serialized = AbstractRequest::TxnOffsetCommit(request.clone())
            .serialize()
            .expect("serialize");
        serialized.flip();
        let parsed = TxnOffsetCommitRequest::parse(&mut serialized, request.version()).expect("parse");
        assert_eq!(parsed.data(), request.data());
        assert_eq!(parsed.offsets(), offsets);
    }

    /// The data of the byte-level tests: one topic, one partition, every
    /// top-level field set, group instance id and metadata null.
    fn encoding_data(name: &str, topic_id: Uuid) -> TxnOffsetCommitRequestData {
        let mut partition = request_partition(0, 5);
        partition.set_committed_leader_epoch(-1).set_committed_metadata(None);
        let mut topic = TxnOffsetCommitRequestTopic::new();
        topic
            .set_name(name.to_string())
            .set_topic_id(topic_id)
            .set_partitions(vec![partition]);
        let mut data = TxnOffsetCommitRequestData::new();
        data.set_transactional_id("t".to_string())
            .set_group_id("g".to_string())
            .set_producer_id(1)
            .set_producer_epoch(2)
            .set_generation_id_or_member_epoch(3)
            .set_member_id("m".to_string())
            .set_group_instance_id(None)
            .set_topics(vec![topic]);
        data
    }

    fn encode(builder: &mut Builder, version: i16) -> Vec<u8> {
        let mut request = builder.build_version(version).expect("build");
        request.serialize().expect("serialize").into_buffer()
    }

    /// The fields both versions share, in spec order, as the flexible (v3+)
    /// encoding writes them: compact strings are `length + 1` then bytes, the
    /// nullable group instance id is a lone `0` for null.
    fn shared_header() -> Vec<u8> {
        let mut bytes = vec![
            0x02, b't', // TransactionalId
            0x02, b'g', // GroupId
        ];
        bytes.extend_from_slice(&1i64.to_be_bytes()); // ProducerId
        bytes.extend_from_slice(&2i16.to_be_bytes()); // ProducerEpoch
        bytes.extend_from_slice(&3i32.to_be_bytes()); // GenerationIdOrMemberEpoch
        bytes.extend_from_slice(&[0x02, b'm']); // MemberId
        bytes.push(0x00); // GroupInstanceId: null
        bytes.push(0x02); // Topics: compact array of 1
        bytes
    }

    /// One partition and the closing tagged-field sections, as both versions end.
    fn shared_trailer() -> Vec<u8> {
        let mut bytes = vec![0x02]; // Partitions: compact array of 1
        bytes.extend_from_slice(&0i32.to_be_bytes()); // PartitionIndex
        bytes.extend_from_slice(&5i64.to_be_bytes()); // CommittedOffset
        bytes.extend_from_slice(&(-1i32).to_be_bytes()); // CommittedLeaderEpoch
        bytes.push(0x00); // CommittedMetadata: null
        bytes.push(0x00); // partition tagged fields
        bytes.push(0x00); // topic tagged fields
        bytes.push(0x00); // request tagged fields
        bytes
    }

    /// Byte-level v6 encoding (DoD #3, PLAN §5), derived from
    /// `TxnOffsetCommitRequest.json` field by field in Java's declaration order:
    /// at v6 the topic carries its 16-byte `TopicId` (most-significant 8 bytes
    /// first, as `Uuid` is written) and **no** `Name`.
    #[test]
    fn test_v6_encoding_carries_the_topic_id_and_no_name() {
        let topic_id = Uuid::new(0x0102_0304_0506_0708, 0x090a_0b0c_0d0e_0f10);
        let mut builder = Builder::for_topic_ids_or_names(encoding_data("ignored-at-v6", topic_id), true);

        let mut expected = shared_header();
        expected.extend_from_slice(&[
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
        ]); // TopicId
        expected.extend(shared_trailer());

        assert_eq!(encode(&mut builder, 6), expected);
    }

    /// The same request at v5 encodes the topic by **name** and drops the id: the
    /// two versions differ only in the topic key.
    #[test]
    fn test_v5_encoding_carries_the_topic_name_and_no_id() {
        let topic_id = Uuid::new(0x0102_0304_0506_0708, 0x090a_0b0c_0d0e_0f10);
        let mut builder = Builder::for_topic_ids_or_names(encoding_data("foo", topic_id), true);

        let mut expected = shared_header();
        expected.extend_from_slice(&[0x04, b'f', b'o', b'o']); // Name
        expected.extend(shared_trailer());

        assert_eq!(encode(&mut builder, 5), expected);
    }
}
