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
//! # Scope
//!
//! Java's static `getErrorResponse(TxnOffsetCommitRequestData, Errors)` overload
//! is not translated: its only callers are in `group-coordinator`
//! (`GroupCoordinatorService.java:2169`, `:2176`, `:2195`), which is broker-side.
//! The instance `getErrorResponse(throttleTimeMs, Throwable)` used by the client
//! dispatch path *is* translated. Java's single-argument
//! `getErrorResponse(Throwable)` is an `AbstractRequest` convenience that fills in
//! a default throttle time; this codebase's dispatch always supplies one, and no
//! other translated wrapper has that form either.

use crate::common::Error;
use std::collections::HashMap;
use std::fmt;
use std::io;

use crate::TxnOffsetCommitRequestData;
use crate::TxnOffsetCommitResponseData;
use crate::common::TopicPartition;
use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::common::record::internal::RecordBatch;
use crate::txn_offset_commit_request_data::{TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic};
use crate::txn_offset_commit_response_data::{TxnOffsetCommitResponsePartition, TxnOffsetCommitResponseTopic};

use super::ConcreteRequest;
use super::ConcreteResponse;
use super::RequestBuilder;
use super::RequestUtils;
use super::TxnOffsetCommitResponse;

/// An offset being committed inside a transaction.
///
/// Corresponds to the nested `TxnOffsetCommitRequest.CommittedOffset`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CommittedOffset {
    /// The offset to commit.
    pub offset: i64,
    /// Optional metadata to store alongside the offset.
    pub metadata: Option<String>,
    /// The leader epoch of the partition when the offset was read, if known.
    pub leader_epoch: Option<i32>,
}

impl CommittedOffset {
    /// Creates a committed offset.
    pub fn new(offset: i64, metadata: Option<String>, leader_epoch: Option<i32>) -> Self {
        Self { offset, metadata, leader_epoch }
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

    /// Creates a new `TxnOffsetCommitRequest` from data and version.
    pub fn new(data: TxnOffsetCommitRequestData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns a reference to the underlying data.
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
    /// `NO_PARTITION_LEADER_EPOCH` sentinel to `None`.
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

    /// Groups an offset map into the wire topic list.
    ///
    /// Corresponds to Java's static `getTopics`. Java groups through a `HashMap`
    /// and so has unspecified order; this sorts by topic name and then partition
    /// index for a deterministic encoding — see
    /// `.claude/rules/producer-transactions.md` §10.
    pub fn get_topics(
        pending_txn_offset_commits: &HashMap<TopicPartition, CommittedOffset>,
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
                topic.set_name(name.to_string()).set_partitions(partitions);
                topic
            })
            .collect()
    }

    /// Mirrors the request's topic/partition shape with `error` on every
    /// partition.
    ///
    /// Corresponds to Java's static `getErrorResponseTopics`.
    pub fn get_error_response_topics(
        request_topics: &[TxnOffsetCommitRequestTopic],
        error: &Errors,
    ) -> Vec<TxnOffsetCommitResponseTopic> {
        request_topics
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
                topic.set_name(request_topic.name.clone()).set_partitions(partitions);
                topic
            })
            .collect()
    }

    /// Builds the canonical error response for this request, matching Java's
    /// `TxnOffsetCommitRequest.getErrorResponse(throttleTimeMs, Throwable)`.
    ///
    /// The error is reported per partition — this response has no top-level error
    /// code at any version.
    pub fn get_error_response(&self, throttle_time_ms: i32, error: &Errors) -> ConcreteResponse {
        let mut response = TxnOffsetCommitResponseData::new();
        response
            .set_throttle_time_ms(throttle_time_ms)
            .set_topics(Self::get_error_response_topics(&self.data.topics, error));
        ConcreteResponse::TxnOffsetCommit(TxnOffsetCommitResponse::new_data(response))
    }

    /// Parses a `TxnOffsetCommitRequest` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
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
/// Corresponds to `TxnOffsetCommitRequest.Builder` in Java.
#[derive(Debug, Clone)]
pub struct TxnOffsetCommitRequestBuilder {
    data: TxnOffsetCommitRequestData,
    is_transaction_v2_enabled: bool,
    oldest_allowed_version: i16,
    latest_allowed_version: i16,
}

/// The parameters of Java's nine-argument `TxnOffsetCommitRequest.Builder`
/// constructor (`TxnOffsetCommitRequest.java:67`) that do not fit in the derived
/// method name.
///
/// Java's three `Builder` constructors (`:50`, `:67`, `:89`) share no parameter
/// name, so `:67`'s nine parameters all reach its derived name. CLAUDE.md §2 caps
/// that at three parameters and moves the remainder here. This struct has no Java
/// counterpart: it exists solely to satisfy that naming rule (DoD #7).
///
/// Because the cap applies to the *whole* group, Java's `:50` and `:67` forms
/// derive the same name — `new_options` — so they collapse into the single
/// constructor below, whose only parameter is this struct. `:50`'s three
/// literals (`JoinGroupRequest.UNKNOWN_MEMBER_ID`, `UNKNOWN_GENERATION_ID`,
/// `Optional.empty()`) become this struct's initial `member_id`,
/// `generation_id` and `group_instance_id`, which is what its own body passes;
/// overriding them gives `:67`'s behaviour.
///
/// It deliberately has **no** `Default`. Only those three of its nine fields
/// are supplied by a narrower Java overload; the other six are what even `:50`
/// takes from its caller, so they have no Java-derived default — and a
/// synthesised `producer_epoch` of `0` is a *valid* epoch, so it would silently
/// commit the transaction offsets under the wrong one. Construct it with
/// [`TxnOffsetCommitRequestBuilderOptionsBuilder::new`] and set those six:
/// [`TxnOffsetCommitRequestBuilderOptionsBuilder::build`] returns an error if any of `transactional_id`, `consumer_group_id`, `producer_id`, `producer_epoch`, `pending_txn_offset_commits`, `is_transaction_v2_enabled` was not set.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct TxnOffsetCommitRequestBuilderOptions<'a> {
    /// Java's `transactionalId`.
    pub transactional_id: String,
    /// Java's `consumerGroupId`.
    pub consumer_group_id: String,
    /// Java's `producerId`.
    pub producer_id: i64,
    /// Java's `producerEpoch`.
    pub producer_epoch: i16,
    /// Java's `pendingTxnOffsetCommits`.
    pub pending_txn_offset_commits: &'a HashMap<TopicPartition, CommittedOffset>,
    /// Java's `memberId`. Starts as [`TxnOffsetCommitRequest::UNKNOWN_MEMBER_ID`], as in `:50`.
    pub member_id: String,
    /// Java's `generationId`. Starts as [`TxnOffsetCommitRequest::UNKNOWN_GENERATION_ID`], as in `:50`.
    pub generation_id: i32,
    /// Java's `groupInstanceId`. Starts as `None`, as in `:50`'s
    /// `Optional.empty()`.
    pub group_instance_id: Option<String>,
    /// Java's `isTransactionV2Enabled`.
    pub is_transaction_v2_enabled: bool,
}

/// Fluent builder for [`TxnOffsetCommitRequestBuilderOptions`].
///
/// Per CLAUDE.md §2 [`Self::new`] takes no parameters, every parameter has a
/// fluent setter, and [`Self::build`] validates the mandatory ones — returning
/// [`Error::LocalIllegalArgument`] if they were not set. Like [`TxnOffsetCommitRequestBuilderOptions`] it has no Java counterpart and
/// exists solely to satisfy that naming rule (DoD #7).
pub struct TxnOffsetCommitRequestBuilderOptionsBuilder<'a> {
    transactional_id: Option<String>,
    consumer_group_id: Option<String>,
    producer_id: Option<i64>,
    producer_epoch: Option<i16>,
    pending_txn_offset_commits: Option<&'a HashMap<TopicPartition, CommittedOffset>>,
    member_id: String,
    generation_id: i32,
    group_instance_id: Option<String>,
    is_transaction_v2_enabled: Option<bool>,
}

impl<'a> Default for TxnOffsetCommitRequestBuilderOptionsBuilder<'a> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> TxnOffsetCommitRequestBuilderOptionsBuilder<'a> {
    /// Creates a builder with every mandatory parameter unset and every other
    /// parameter at the value Java passes on the caller's behalf.
    pub fn new() -> Self {
        Self {
            transactional_id: None,
            consumer_group_id: None,
            producer_id: None,
            producer_epoch: None,
            pending_txn_offset_commits: None,
            member_id: TxnOffsetCommitRequest::UNKNOWN_MEMBER_ID.to_string(),
            generation_id: TxnOffsetCommitRequest::UNKNOWN_GENERATION_ID,
            group_instance_id: None,
            is_transaction_v2_enabled: None,
        }
    }

    /// Sets [`TxnOffsetCommitRequestBuilderOptions::transactional_id`], a mandatory parameter: [`Self::build`]
    /// panics if it was not set.
    pub fn set_transactional_id(mut self, transactional_id: impl Into<String>) -> Self {
        self.transactional_id = Some(transactional_id.into());
        self
    }
    /// Sets [`TxnOffsetCommitRequestBuilderOptions::consumer_group_id`], a mandatory parameter: [`Self::build`]
    /// panics if it was not set.
    pub fn set_consumer_group_id(mut self, consumer_group_id: impl Into<String>) -> Self {
        self.consumer_group_id = Some(consumer_group_id.into());
        self
    }
    /// Sets [`TxnOffsetCommitRequestBuilderOptions::producer_id`], a mandatory parameter: [`Self::build`]
    /// panics if it was not set.
    pub fn set_producer_id(mut self, producer_id: i64) -> Self {
        self.producer_id = Some(producer_id);
        self
    }
    /// Sets [`TxnOffsetCommitRequestBuilderOptions::producer_epoch`], a mandatory parameter: [`Self::build`]
    /// panics if it was not set.
    pub fn set_producer_epoch(mut self, producer_epoch: i16) -> Self {
        self.producer_epoch = Some(producer_epoch);
        self
    }
    /// Sets [`TxnOffsetCommitRequestBuilderOptions::pending_txn_offset_commits`], a mandatory parameter: [`Self::build`]
    /// panics if it was not set.
    pub fn set_pending_txn_offset_commits(
        mut self,
        pending_txn_offset_commits: &'a HashMap<TopicPartition, CommittedOffset>,
    ) -> Self {
        self.pending_txn_offset_commits = Some(pending_txn_offset_commits);
        self
    }
    /// Sets [`TxnOffsetCommitRequestBuilderOptions::member_id`].
    pub fn set_member_id(mut self, member_id: String) -> Self {
        self.member_id = member_id;
        self
    }
    /// Sets [`TxnOffsetCommitRequestBuilderOptions::generation_id`].
    pub fn set_generation_id(mut self, generation_id: i32) -> Self {
        self.generation_id = generation_id;
        self
    }
    /// Sets [`TxnOffsetCommitRequestBuilderOptions::group_instance_id`].
    pub fn set_group_instance_id(mut self, group_instance_id: Option<String>) -> Self {
        self.group_instance_id = group_instance_id;
        self
    }
    /// Sets [`TxnOffsetCommitRequestBuilderOptions::is_transaction_v2_enabled`], a mandatory parameter: [`Self::build`]
    /// panics if it was not set.
    pub fn set_is_transaction_v2_enabled(mut self, is_transaction_v2_enabled: bool) -> Self {
        self.is_transaction_v2_enabled = Some(is_transaction_v2_enabled);
        self
    }

    /// Returns the built options.
    ///
    /// Per CLAUDE.md §2 the mandatory parameters are validated here rather than
    /// being named in the constructor, so a later Java version that makes one of
    /// them optional changes the set this accepts instead of adding a second
    /// constructor. Today there is one mandatory set: `transactional_id`, `consumer_group_id`, `producer_id`, `producer_epoch`, `pending_txn_offset_commits`, `is_transaction_v2_enabled`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::LocalIllegalArgument`] naming the first parameter of that
    /// set which was not given a setter call. Only presence is checked here;
    /// semantic validation belongs to the method the options are passed to
    /// (CLAUDE.md §2).
    pub fn build(self) -> Result<TxnOffsetCommitRequestBuilderOptions<'a>, Error> {
        Ok(TxnOffsetCommitRequestBuilderOptions {
            transactional_id: self.transactional_id.ok_or_else(|| Self::missing("transactional_id"))?,
            consumer_group_id: self.consumer_group_id.ok_or_else(|| Self::missing("consumer_group_id"))?,
            producer_id: self.producer_id.ok_or_else(|| Self::missing("producer_id"))?,
            producer_epoch: self.producer_epoch.ok_or_else(|| Self::missing("producer_epoch"))?,
            pending_txn_offset_commits: self
                .pending_txn_offset_commits
                .ok_or_else(|| Self::missing("pending_txn_offset_commits"))?,
            member_id: self.member_id,
            generation_id: self.generation_id,
            group_instance_id: self.group_instance_id,
            is_transaction_v2_enabled: self
                .is_transaction_v2_enabled
                .ok_or_else(|| Self::missing("is_transaction_v2_enabled"))?,
        })
    }

    /// Builds the [`Error::LocalIllegalArgument`] naming a mandatory parameter
    /// [`Self::build`] found unset.
    fn missing(parameter: &str) -> Error {
        Error::local_illegal_argument(format!(
            "TxnOffsetCommitRequestBuilderOptionsBuilder::build: mandatory parameter `{parameter}` was not set"
        ))
    }
}

impl TxnOffsetCommitRequestBuilder {
    /// Creates a builder carrying consumer-group metadata.
    ///
    /// This is the form the producer uses (`TransactionManager.java:1232`).
    /// Corresponds to Java's nine-argument `Builder`
    /// (`TxnOffsetCommitRequest.java:67`) **and** its six-argument form (`:50`),
    /// which derive the same name under CLAUDE.md §2 and so collapse here. For
    /// `:50`'s behaviour leave the options' `member_id`, `generation_id` and
    /// `group_instance_id` at their initial values — exactly what its Java body
    /// forwards.
    pub fn new_options(options: TxnOffsetCommitRequestBuilderOptions<'_>) -> Self {
        let TxnOffsetCommitRequestBuilderOptions {
            transactional_id,
            consumer_group_id,
            producer_id,
            producer_epoch,
            pending_txn_offset_commits,
            member_id,
            generation_id,
            group_instance_id,
            is_transaction_v2_enabled,
        } = options;
        let mut data = TxnOffsetCommitRequestData::new();
        data.set_transactional_id(transactional_id)
            .set_group_id(consumer_group_id)
            .set_producer_id(producer_id)
            .set_producer_epoch(producer_epoch)
            .set_topics(TxnOffsetCommitRequest::get_topics(pending_txn_offset_commits))
            .set_member_id(member_id)
            .set_generation_id(generation_id)
            .set_group_instance_id(group_instance_id);

        Self {
            data,
            is_transaction_v2_enabled,
            oldest_allowed_version: ApiKeys::TXN_OFFSET_COMMIT.oldest_version(),
            // Mirrors Java's `super(ApiKeys)` → `Builder(apiKey, false)` →
            // `latestVersion(false)`: released versions only. Rules §12.
            latest_allowed_version: ApiKeys::TXN_OFFSET_COMMIT.latest_version_enable_unstable_last_version(false),
        }
    }

    /// Creates a builder from pre-built wire data.
    ///
    /// Corresponds to Java's single-argument `Builder(TxnOffsetCommitRequestData)`
    /// (`TxnOffsetCommitRequest.java:89`), which hardcodes
    /// `isTransactionV2Enabled = true`.
    pub fn new_data(data: TxnOffsetCommitRequestData) -> Self {
        Self {
            data,
            is_transaction_v2_enabled: true,
            oldest_allowed_version: ApiKeys::TXN_OFFSET_COMMIT.oldest_version(),
            // Mirrors Java's `super(ApiKeys)` → `Builder(apiKey, false)` →
            // `latestVersion(false)`: released versions only. Rules §12.
            latest_allowed_version: ApiKeys::TXN_OFFSET_COMMIT.latest_version_enable_unstable_last_version(false),
        }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &TxnOffsetCommitRequestData {
        &self.data
    }

    /// Whether Transaction V2 was negotiated.
    pub fn is_transaction_v2_enabled(&self) -> bool {
        self.is_transaction_v2_enabled
    }

    /// Whether any consumer-group metadata field is set.
    ///
    /// Corresponds to Java's private `groupMetadataSet()`. Note it is an **OR**
    /// across all three fields, so setting any one of them requires v3+.
    fn group_metadata_set(&self) -> bool {
        self.data.member_id != TxnOffsetCommitRequest::UNKNOWN_MEMBER_ID
            || self.data.generation_id != TxnOffsetCommitRequest::UNKNOWN_GENERATION_ID
            || self.data.group_instance_id.is_some()
    }
}

impl RequestBuilder for TxnOffsetCommitRequestBuilder {
    fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::TXN_OFFSET_COMMIT
    }

    fn oldest_allowed_version(&self) -> i16 {
        self.oldest_allowed_version
    }

    fn latest_allowed_version(&self) -> i16 {
        self.latest_allowed_version
    }

    /// Builds at `version`, rejecting group metadata below v3 and clamping to
    /// [`TxnOffsetCommitRequest::LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2`] without Transaction V2.
    ///
    /// Mirrors Java's `Builder.build(short)`. Order matters: the group-metadata
    /// check runs against the **requested** version, before any clamping, so a
    /// clamp cannot mask an unsupported-version error.
    fn build_version(&mut self, version: i16) -> io::Result<ConcreteRequest> {
        if version < 3 && self.group_metadata_set() {
            // Java throws UnsupportedVersionException; per CLAUDE.md §10.2 this
            // is a Result. Message text preserved.
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!(
                    "Broker doesn't support group metadata commit API on version {version}, \
                     minimum supported request version is 3 which requires brokers to be on \
                     version 2.5 or above."
                ),
            ));
        }

        let version = if self.is_transaction_v2_enabled {
            version
        } else {
            version.min(TxnOffsetCommitRequest::LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2)
        };

        Ok(ConcreteRequest::TxnOffsetCommit(TxnOffsetCommitRequest::new(
            self.data.clone(),
            version,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tp(topic: &str, partition: i32) -> TopicPartition {
        TopicPartition::new(topic.to_string(), partition)
    }

    fn offsets() -> HashMap<TopicPartition, CommittedOffset> {
        HashMap::from([
            (tp("topic-a", 0), CommittedOffset::new(100, Some("meta-a".to_string()), Some(5))),
            (tp("topic-b", 1), CommittedOffset::new(200, None, None)),
        ])
    }

    fn builder_with_metadata() -> TxnOffsetCommitRequestBuilder {
        TxnOffsetCommitRequestBuilder::new_options(
            TxnOffsetCommitRequestBuilderOptionsBuilder::new()
                .set_transactional_id("txn-1")
                .set_consumer_group_id("group-1")
                .set_producer_id(42)
                .set_producer_epoch(7)
                .set_pending_txn_offset_commits(&offsets())
                .set_is_transaction_v2_enabled(true)
                .set_member_id(("member-1").to_string())
                .set_generation_id(3)
                .set_group_instance_id(Some("instance-1".to_string()))
                .build()
                .unwrap(),
        )
    }

    #[test]
    fn test_builder_sets_all_fields() {
        let builder = builder_with_metadata();
        let data = builder.data();
        assert_eq!(data.transactional_id, "txn-1");
        assert_eq!(data.group_id, "group-1");
        assert_eq!(data.producer_id, 42);
        assert_eq!(data.producer_epoch, 7);
        assert_eq!(data.member_id, "member-1");
        assert_eq!(data.generation_id, 3);
        assert_eq!(data.group_instance_id.as_deref(), Some("instance-1"));
    }

    #[test]
    fn test_without_group_metadata_uses_sentinels() {
        let builder = TxnOffsetCommitRequestBuilder::new_options(
            TxnOffsetCommitRequestBuilderOptionsBuilder::new()
                .set_transactional_id("txn-1")
                .set_consumer_group_id("group-1")
                .set_producer_id(42)
                .set_producer_epoch(7)
                .set_pending_txn_offset_commits(&offsets())
                .set_is_transaction_v2_enabled(true)
                .build()
                .unwrap(),
        );
        assert_eq!(builder.data().member_id, TxnOffsetCommitRequest::UNKNOWN_MEMBER_ID);
        assert_eq!(builder.data().generation_id, TxnOffsetCommitRequest::UNKNOWN_GENERATION_ID);
        assert_eq!(builder.data().group_instance_id, None);
        assert!(!builder.group_metadata_set());
    }

    /// `group_metadata_set` is an OR: **any** of the three fields triggers it.
    #[test]
    fn test_group_metadata_set_is_an_or_across_all_three_fields() {
        let none = TxnOffsetCommitRequestBuilder::new_options(
            TxnOffsetCommitRequestBuilderOptionsBuilder::new()
                .set_transactional_id("t")
                .set_consumer_group_id("g")
                .set_producer_id(1)
                .set_producer_epoch(0)
                .set_pending_txn_offset_commits(&HashMap::new())
                .set_is_transaction_v2_enabled(true)
                .build()
                .unwrap(),
        );
        assert!(!none.group_metadata_set());

        let member_only = TxnOffsetCommitRequestBuilder::new_options(
            TxnOffsetCommitRequestBuilderOptionsBuilder::new()
                .set_transactional_id("t")
                .set_consumer_group_id("g")
                .set_producer_id(1)
                .set_producer_epoch(0)
                .set_pending_txn_offset_commits(&HashMap::new())
                .set_is_transaction_v2_enabled(true)
                .set_member_id(("m").to_string())
                .build()
                .unwrap(),
        );
        assert!(member_only.group_metadata_set(), "member id alone must count");

        let generation_only = TxnOffsetCommitRequestBuilder::new_options(
            TxnOffsetCommitRequestBuilderOptionsBuilder::new()
                .set_transactional_id("t")
                .set_consumer_group_id("g")
                .set_producer_id(1)
                .set_producer_epoch(0)
                .set_pending_txn_offset_commits(&HashMap::new())
                .set_is_transaction_v2_enabled(true)
                .set_generation_id(0)
                .build()
                .unwrap(),
        );
        assert!(generation_only.group_metadata_set(), "generation id alone must count");

        let instance_only = TxnOffsetCommitRequestBuilder::new_options(
            TxnOffsetCommitRequestBuilderOptionsBuilder::new()
                .set_transactional_id("t")
                .set_consumer_group_id("g")
                .set_producer_id(1)
                .set_producer_epoch(0)
                .set_pending_txn_offset_commits(&HashMap::new())
                .set_is_transaction_v2_enabled(true)
                .set_group_instance_id(Some("i".to_string()))
                .build()
                .unwrap(),
        );
        assert!(instance_only.group_metadata_set(), "group instance id alone must count");
    }

    #[test]
    fn test_build_rejects_group_metadata_below_v3() {
        for version in 0..3 {
            let mut builder = builder_with_metadata();
            let error = builder.build_version(version).expect_err("group metadata needs v3+");
            assert_eq!(
                error.to_string(),
                format!(
                    "Broker doesn't support group metadata commit API on version {version}, \
                     minimum supported request version is 3 which requires brokers to be on \
                     version 2.5 or above."
                )
            );
        }
        // v3 is where it becomes legal.
        builder_with_metadata().build_version(3).expect("v3 accepts group metadata");
    }

    /// Without group metadata, versions below 3 are fine.
    #[test]
    fn test_build_allows_low_versions_without_group_metadata() {
        for version in 0..3 {
            let mut builder = TxnOffsetCommitRequestBuilder::new_options(
                TxnOffsetCommitRequestBuilderOptionsBuilder::new()
                    .set_transactional_id("txn-1")
                    .set_consumer_group_id("group-1")
                    .set_producer_id(42)
                    .set_producer_epoch(7)
                    .set_pending_txn_offset_commits(&offsets())
                    .set_is_transaction_v2_enabled(true)
                    .build()
                    .unwrap(),
            );
            builder
                .build_version(version)
                .unwrap_or_else(|error| panic!("v{version} without group metadata must build, got {error}"));
        }
    }

    /// The group-metadata check runs against the *requested* version, before the
    /// Transaction V2 clamp — so a clamp cannot mask the error.
    #[test]
    fn test_group_metadata_check_precedes_the_version_clamp() {
        // `is_transaction_v2_enabled` is false, so clamping stays active.
        let mut builder = TxnOffsetCommitRequestBuilder::new_options(
            TxnOffsetCommitRequestBuilderOptionsBuilder::new()
                .set_transactional_id("txn-1")
                .set_consumer_group_id("group-1")
                .set_producer_id(42)
                .set_producer_epoch(7)
                .set_pending_txn_offset_commits(&offsets())
                .set_is_transaction_v2_enabled(false)
                .set_member_id(("member-1").to_string())
                .set_generation_id(3)
                .build()
                .unwrap(),
        );
        // v2 is below 3 and also below the clamp ceiling, so the error must win.
        assert!(builder.build_version(2).is_err());
    }

    #[test]
    fn test_build_clamps_version_without_transaction_v2() {
        let latest = ApiKeys::TXN_OFFSET_COMMIT.latest_version();
        assert!(
            latest > TxnOffsetCommitRequest::LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2,
            "the clamp is only meaningful if the API supports higher versions"
        );

        let mut builder = TxnOffsetCommitRequestBuilder::new_options(
            TxnOffsetCommitRequestBuilderOptionsBuilder::new()
                .set_transactional_id("txn-1")
                .set_consumer_group_id("group-1")
                .set_producer_id(42)
                .set_producer_epoch(7)
                .set_pending_txn_offset_commits(&offsets())
                .set_is_transaction_v2_enabled(false)
                .set_member_id(("member-1").to_string())
                .set_generation_id(3)
                .build()
                .unwrap(),
        );
        match builder.build_version(latest).expect("build") {
            ConcreteRequest::TxnOffsetCommit(request) => {
                assert_eq!(
                    request.version(),
                    TxnOffsetCommitRequest::LAST_STABLE_VERSION_BEFORE_TRANSACTION_V2
                )
            },
            other => panic!("unexpected {other:?}"),
        }

        // With TV2 the requested version passes through.
        let mut builder = builder_with_metadata();
        match builder.build_version(latest).expect("build") {
            ConcreteRequest::TxnOffsetCommit(request) => assert_eq!(request.version(), latest),
            other => panic!("unexpected {other:?}"),
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
    }

    /// `offsets()` round-trips whatever `get_topics` produced, including the
    /// absent-leader-epoch and absent-metadata cases.
    #[test]
    fn test_offsets_round_trips_get_topics() {
        let expected = offsets();
        let mut data = TxnOffsetCommitRequestData::new();
        data.set_topics(TxnOffsetCommitRequest::get_topics(&expected));
        let request = TxnOffsetCommitRequest::new(data, ApiKeys::TXN_OFFSET_COMMIT.latest_version());

        assert_eq!(request.offsets(), expected);
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
    fn test_get_error_response_is_per_partition() {
        let mut builder = builder_with_metadata();
        let request = match builder.build_version(3).expect("build") {
            ConcreteRequest::TxnOffsetCommit(request) => request,
            other => panic!("unexpected {other:?}"),
        };

        match request.get_error_response(17, &Errors::InvalidTxnState) {
            ConcreteResponse::TxnOffsetCommit(response) => {
                assert_eq!(response.data().throttle_time_ms, 17);
                let topics = &response.data().topics;
                assert_eq!(topics.len(), 2);
                assert_eq!(topics[0].name, "topic-a");
                assert_eq!(topics[1].name, "topic-b");
                for topic in topics {
                    for partition in &topic.partitions {
                        assert_eq!(partition.error_code, Errors::InvalidTxnState.code());
                    }
                }
            },
            other => panic!("expected TxnOffsetCommit response, got {other:?}"),
        }
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
    fn test_from_data_defaults_transaction_v2_to_enabled() {
        let builder = TxnOffsetCommitRequestBuilder::new_data(TxnOffsetCommitRequestData::new());
        assert!(builder.is_transaction_v2_enabled(), "Java hardcodes true in this constructor");
    }

    #[test]
    fn test_serialization_round_trip_all_versions() {
        for version in ApiKeys::TXN_OFFSET_COMMIT.oldest_version()..=ApiKeys::TXN_OFFSET_COMMIT.latest_version() {
            // No group metadata, so every version including 0-2 is legal.
            let mut builder = TxnOffsetCommitRequestBuilder::new_options(
                TxnOffsetCommitRequestBuilderOptionsBuilder::new()
                    .set_transactional_id("txn-1")
                    .set_consumer_group_id("group-1")
                    .set_producer_id(42)
                    .set_producer_epoch(7)
                    .set_pending_txn_offset_commits(&offsets())
                    .set_is_transaction_v2_enabled(true)
                    .build()
                    .unwrap(),
            );
            let mut built = builder.build_version(version).expect("build");
            let mut buffer = built.serialize().expect("serialize");
            buffer.flip();
            let parsed = TxnOffsetCommitRequest::parse(&mut buffer, version).expect("parse");

            assert_eq!(parsed.version(), version);
            assert_eq!(parsed.data().transactional_id, "txn-1", "v{version}");
            assert_eq!(parsed.data().group_id, "group-1", "v{version}");

            // `CommittedLeaderEpoch` is a v2+ field marked `"ignorable": true`
            // in the spec, so below v2 it is absent from the wire and decodes
            // back to `None`. Java behaves identically: its generator emits the
            // "non-default at unsupported version" check only for
            // NON-ignorable fields (`MessageDataGenerator.java:792`), so an
            // ignorable field is silently dropped rather than rejected. Every
            // other field survives at every version.
            let mut expected = offsets();
            if version < 2 {
                for offset in expected.values_mut() {
                    offset.leader_epoch = None;
                }
            }
            assert_eq!(parsed.offsets(), expected, "v{version}");
        }
    }

    #[test]
    fn test_api_key_and_version() {
        let builder = builder_with_metadata();
        assert_eq!(builder.api_key(), &ApiKeys::TXN_OFFSET_COMMIT);
        let request = TxnOffsetCommitRequest::new(TxnOffsetCommitRequestData::new(), 3);
        assert_eq!(request.api_key(), &ApiKeys::TXN_OFFSET_COMMIT);
        assert_eq!(request.version(), 3);
    }

    /// Translated from `TxnOffsetCommitRequestTest.testConstructor`.
    ///
    /// Java loops every version, using the metadata-free builder below v3 and the
    /// metadata-carrying one from v3, then asserts the offsets, the regrouped
    /// topics, and the error response.
    ///
    /// Note this asserts the *in-memory* offsets, which carry the leader epoch at
    /// every version — the epoch is only dropped on serialization at v0/v1 (see
    /// `test_serialization_round_trip_all_versions`). `build_version` wraps the
    /// data with a version, it does not encode it.
    #[test]
    fn test_constructor() {
        const THROTTLE_TIME_MS: i32 = 10;
        let offsets_map = HashMap::from([
            (tp("topic-a", 0), CommittedOffset::new(100, Some("meta".to_string()), Some(5))),
            (tp("topic-b", 1), CommittedOffset::new(100, Some("meta".to_string()), Some(5))),
        ]);
        let expected_errors: HashMap<TopicPartition, Errors> = offsets_map
            .keys()
            .cloned()
            .map(|partition| (partition, Errors::NotCoordinator))
            .collect();

        for version in ApiKeys::TXN_OFFSET_COMMIT.oldest_version()..=ApiKeys::TXN_OFFSET_COMMIT.latest_version() {
            let mut builder = if version < 3 {
                TxnOffsetCommitRequestBuilder::new_options(
                    TxnOffsetCommitRequestBuilderOptionsBuilder::new()
                        .set_transactional_id("transactionalId")
                        .set_consumer_group_id("group-1")
                        .set_producer_id(10)
                        .set_producer_epoch(1)
                        .set_pending_txn_offset_commits(&offsets_map)
                        .set_is_transaction_v2_enabled(true)
                        .build()
                        .unwrap(),
                )
            } else {
                TxnOffsetCommitRequestBuilder::new_options(
                    TxnOffsetCommitRequestBuilderOptionsBuilder::new()
                        .set_transactional_id("transactionalId")
                        .set_consumer_group_id("group-1")
                        .set_producer_id(10)
                        .set_producer_epoch(1)
                        .set_pending_txn_offset_commits(&offsets_map)
                        .set_is_transaction_v2_enabled(true)
                        .set_member_id(("member-1").to_string())
                        .set_generation_id(5)
                        .set_group_instance_id(Some("instance-1".to_string()))
                        .build()
                        .unwrap(),
                )
            };
            let request = match builder.build_version(version).expect("build") {
                ConcreteRequest::TxnOffsetCommit(request) => request,
                other => panic!("unexpected {other:?}"),
            };

            assert_eq!(request.offsets(), offsets_map, "v{version}");
            // Regrouping the flattened offsets reproduces the wire topics.
            assert_eq!(
                TxnOffsetCommitRequest::get_topics(&request.offsets()),
                request.data().topics,
                "v{version}"
            );

            match request.get_error_response(THROTTLE_TIME_MS, &Errors::NotCoordinator) {
                ConcreteResponse::TxnOffsetCommit(response) => {
                    assert_eq!(response.errors(), expected_errors, "v{version}");
                    let counts = response.error_counts();
                    assert_eq!(counts.len(), 1, "v{version}");
                    assert_eq!(counts.get(&Errors::NotCoordinator), Some(&2), "v{version}");
                    assert_eq!(response.throttle_time_ms(), THROTTLE_TIME_MS, "v{version}");
                },
                other => panic!("expected TxnOffsetCommit response, got {other:?}"),
            }
        }
    }

    /// Translated from
    /// `TxnOffsetCommitRequestTest.testVersionSupportForGroupMetadata`.
    ///
    /// The metadata-free builder works at every version; the metadata-carrying one
    /// only from v3, with the exact message asserted below that.
    #[test]
    fn test_version_support_for_group_metadata() {
        for version in ApiKeys::TXN_OFFSET_COMMIT.oldest_version()..=ApiKeys::TXN_OFFSET_COMMIT.latest_version() {
            TxnOffsetCommitRequestBuilder::new_options(
                TxnOffsetCommitRequestBuilderOptionsBuilder::new()
                    .set_transactional_id("txn-1")
                    .set_consumer_group_id("group-1")
                    .set_producer_id(10)
                    .set_producer_epoch(1)
                    .set_pending_txn_offset_commits(&offsets())
                    .set_is_transaction_v2_enabled(true)
                    .build()
                    .unwrap(),
            )
            .build_version(version)
            .unwrap_or_else(|error| panic!("v{version} without metadata must build: {error}"));

            let mut with_metadata = builder_with_metadata();
            if version >= 3 {
                with_metadata
                    .build_version(version)
                    .unwrap_or_else(|error| panic!("v{version} with metadata must build: {error}"));
            } else {
                let error = with_metadata.build_version(version).expect_err("must reject below v3");
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

    // -- Java test deliberately not translated (DoD §3) ----------------------
    //
    // `TxnOffsetCommitRequestTest.testGetErrorResponse` exercises the **static**
    // `getErrorResponse(TxnOffsetCommitRequestData, Errors)` overload, which is
    // not translated — its only callers are in `group-coordinator`
    // (`GroupCoordinatorService.java:2169`, `:2176`, `:2195`), i.e. broker-side.
    //
    // The response *shape* it asserts (one partition entry per request partition,
    // each carrying the same error code) is identical to what the translated
    // instance `getErrorResponse(throttle_time_ms, error)` produces, and is
    // asserted by `test_get_error_response_is_per_partition` above and by
    // `test_constructor`. The only difference in the static overload is that it
    // omits the throttle time.

    /// CLAUDE.md §2: the mandatory parameters are validated in
    /// [`TxnOffsetCommitRequestBuilderOptionsBuilder::build`], not named in the constructor, so a
    /// builder left untouched panics naming the first one it finds unset.
    #[test]
    fn txn_offset_commit_request_builder_options_builder_build_errors_when_no_mandatory_parameter_is_set() {
        let Err(error) = TxnOffsetCommitRequestBuilderOptionsBuilder::new().build() else {
            panic!("build must reject the unset mandatory parameter");
        };
        assert!(matches!(error, Error::LocalIllegalArgument(_)), "{error:?}");
        assert_eq!(
            error.message(),
            "TxnOffsetCommitRequestBuilderOptionsBuilder::build: mandatory parameter `transactional_id` was not set"
        );
    }
}
