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

//! Shared helpers for the offset-fetch / reset / validate code paths.
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.OffsetFetcherUtils` (static
//! helpers + per-request state) and
//! `org.apache.kafka.clients.consumer.internals.PositionsValidator`
//! (validation-on-metadata-change driver). The classic-consumer
//! `OffsetFetcher` is out of scope per `consumer-threading.md` §20.

#![allow(dead_code)]

use crate::consumer::ConsumerNoOffsetForPartitionError;
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::sync::atomic::{AtomicI32, Ordering};

use crate::api_versions::ApiVersions;
use crate::common::IsolationLevel;
use crate::common::errors::TopicAuthorizationError;
use crate::common::protocol::{ApiKeys, Errors};
use crate::common::requests::ListOffsetsResponse;
use crate::common::requests::list_offsets_response::{UNKNOWN_EPOCH, UNKNOWN_OFFSET};
use crate::common::requests::offsets_for_leader_epoch_request::supports_topic_permission;
use crate::common::{Error, Node, TopicPartition};
use crate::list_offsets_request_data::ListOffsetsPartition;
use crate::node_api_versions::NodeApiVersions;

use super::auto_offset_reset_strategy::AutoOffsetResetStrategy;
use super::consumer_metadata::ConsumerMetadata;
use super::offset_and_timestamp_internal::OffsetAndTimestampInternal;
use super::offsets_for_leader_epoch_client::OffsetForEpochResult;
use super::subscription_state::{FetchPosition, LogTruncation, SubscriptionState};

/// Data about an offset returned by a broker for a single partition.
///
/// Translated from `OffsetFetcherUtils.ListOffsetData`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ListOffsetData {
    /// The fetched offset.
    pub offset: i64,
    /// The timestamp associated with the returned offset, if the broker
    /// supports returning timestamps (else `None`).
    pub timestamp: Option<i64>,
    /// The leader epoch associated with the returned offset, if known
    /// (else `None`).
    pub leader_epoch: Option<i32>,
}

impl ListOffsetData {
    pub(crate) fn new(offset: i64, timestamp: Option<i64>, leader_epoch: Option<i32>) -> Self {
        Self { offset, timestamp, leader_epoch }
    }
}

/// Result of processing a `ListOffsets` response.
///
/// Translated from `OffsetFetcherUtils.ListOffsetResult`.
#[derive(Default, Debug)]
pub(crate) struct ListOffsetResult {
    /// Per-partition fetched offsets.
    pub fetched_offsets: HashMap<TopicPartition, ListOffsetData>,
    /// Partitions whose response indicated a retriable error.
    pub partitions_to_retry: HashSet<TopicPartition>,
}

impl ListOffsetResult {
    pub(crate) fn new(
        fetched_offsets: HashMap<TopicPartition, ListOffsetData>,
        partitions_to_retry: HashSet<TopicPartition>,
    ) -> Self {
        Self { fetched_offsets, partitions_to_retry }
    }
}

/// Returns `true` if the broker's `OffsetForLeaderEpoch` API supports
/// topic-level permission (v3+).
///
/// Translated from `OffsetFetcherUtils.hasUsableOffsetForLeaderEpochVersion`.
pub(crate) fn has_usable_offset_for_leader_epoch_version(node_api_versions: &NodeApiVersions) -> bool {
    match node_api_versions.api_version(&ApiKeys::OFFSET_FOR_LEADER_EPOCH) {
        Some(version) => supports_topic_permission(version.max_version),
        None => false,
    }
}

/// Groups partition entries by the current leader from
/// [`FetchPosition::current_leader`], dropping entries without a leader.
///
/// Translated from `OffsetFetcherUtils.regroupFetchPositionsByLeader`.
pub(crate) fn regroup_fetch_positions_by_leader(
    partition_map: &HashMap<TopicPartition, FetchPosition>,
) -> HashMap<Node, HashMap<TopicPartition, FetchPosition>> {
    let mut result: HashMap<Node, HashMap<TopicPartition, FetchPosition>> = HashMap::new();
    for (tp, position) in partition_map {
        if let Some(leader) = &position.current_leader.leader {
            result.entry(leader.clone()).or_default().insert(tp.clone(), position.clone());
        }
    }
    result
}

/// Returns the set of topics referenced by the given partition iterator.
///
/// Translated from `OffsetFetcherUtils.topicsForPartitions`.
pub(crate) fn topics_for_partitions<'a, I>(partitions: I) -> HashSet<String>
where
    I: IntoIterator<Item = &'a TopicPartition>,
{
    partitions.into_iter().map(|tp| tp.topic().to_string()).collect()
}

/// Aggregates a `(TopicPartition, T)` map by leader node.
///
/// Returns a map from `Node` to the sub-map of entries whose current leader
/// is that node. Partitions for which the metadata does not yet have a
/// leader are dropped (matching Java's `groupingBy` over `leaderFor` which
/// would NPE on null).
///
/// Translated from `OffsetFetcherUtils.regroupPartitionMapByNode`.
pub(crate) fn regroup_partition_map_by_node<T: Clone>(
    metadata: &ConsumerMetadata,
    partition_map: &HashMap<TopicPartition, T>,
) -> HashMap<Node, HashMap<TopicPartition, T>> {
    let cluster = metadata.metadata_arc().fetch();
    let mut result: HashMap<Node, HashMap<TopicPartition, T>> = HashMap::new();
    for (tp, value) in partition_map {
        if let Some(node) = cluster.leader_for(tp) {
            result.entry(node.clone()).or_default().insert(tp.clone(), value.clone());
        }
    }
    result
}

/// Builds an `OffsetAndTimestampInternal` result map from a
/// `timestamps_to_search` input plus per-partition fetched offsets.
///
/// Each input partition appears in the output (as `None` when no offset
/// was returned for it; `Some(_)` otherwise — including the
/// `endOffsets`/`beginningOffsets` case where the broker returns
/// `timestamp == -1` as the "no timestamp" sentinel).
///
/// Translates Java's `OffsetFetcherUtils.buildOffsetsForTimeInternalResult`
/// (NOT the public-class `buildOffsetsForTimesResult`): the result is
/// the internal `OffsetAndTimestampInternal` so it can carry the broker's
/// negative-timestamp sentinel without failing validation. The
/// `offsetsForTimes` public API converts to [`OffsetAndTimestamp`] at
/// its boundary; `endOffsets`/`beginningOffsets` reads `.offset()`
/// directly.
///
/// Mirrors COMMENTS.DONE.1.md Issue 6: a previous translation used the
/// public-class constructor here and silently produced `None` for every
/// `endOffsets(tp)` because `OffsetAndTimestamp::with_leader_epoch`
/// rejected `timestamp == -1`.
pub(crate) fn build_offsets_for_times_result(
    timestamps_to_search: &HashMap<TopicPartition, i64>,
    fetched_offsets: &HashMap<TopicPartition, ListOffsetData>,
) -> HashMap<TopicPartition, Option<OffsetAndTimestampInternal>> {
    let mut result: HashMap<TopicPartition, Option<OffsetAndTimestampInternal>> =
        HashMap::with_capacity(timestamps_to_search.len());
    for tp in timestamps_to_search.keys() {
        result.insert(tp.clone(), None);
    }
    for (tp, offset_data) in fetched_offsets {
        let oat = OffsetAndTimestampInternal::new(
            offset_data.offset,
            offset_data.timestamp.unwrap_or(-1),
            offset_data.leader_epoch,
        );
        result.insert(tp.clone(), Some(oat));
    }
    result
}

/// Per-instance state for the `OffsetFetcherUtils` translation.
///
/// The Rust translation collapses Java's `OffsetFetcherUtils` and
/// `PositionsValidator` instance state — neither holds enough independent
/// behaviour to warrant a separate type at the Rust call sites — and
/// exposes the union of their methods.
pub(crate) struct OffsetFetcherUtilsState {
    /// `Arc` clone shared with the `OffsetsRequestManager`. Held so that
    /// Java's instance methods that read/write `metadata` and
    /// `subscriptionState` translate 1:1 without callers having to pass
    /// them in.
    pub(crate) metadata: std::sync::Arc<ConsumerMetadata>,
    pub(crate) subscriptions: std::sync::Arc<Mutex<SubscriptionState>>,
    pub(crate) api_versions: std::sync::Arc<ApiVersions>,
    pub(crate) retry_backoff_ms: i64,
    cached_reset_positions_error: Mutex<Option<Error>>,
    cached_validate_positions_error: Mutex<Option<Error>>,
    metadata_update_version: AtomicI32,
}

impl OffsetFetcherUtilsState {
    pub(crate) fn new(
        metadata: std::sync::Arc<ConsumerMetadata>,
        subscriptions: std::sync::Arc<Mutex<SubscriptionState>>,
        api_versions: std::sync::Arc<ApiVersions>,
        retry_backoff_ms: i64,
    ) -> Self {
        Self {
            metadata,
            subscriptions,
            api_versions,
            retry_backoff_ms,
            cached_reset_positions_error: Mutex::new(None),
            cached_validate_positions_error: Mutex::new(None),
            metadata_update_version: AtomicI32::new(-1),
        }
    }

    /// Processes a successful `ListOffsets` response and returns the
    /// extracted offsets + retry set.
    ///
    /// Translated from `OffsetFetcherUtils.handleListOffsetResponse(...)`.
    ///
    /// # Errors
    ///
    /// Returns [`TopicAuthorizationError`] if any partition response
    /// carried `TOPIC_AUTHORIZATION_FAILED`.
    pub(crate) fn handle_list_offset_response(
        &self,
        response: &ListOffsetsResponse,
    ) -> Result<ListOffsetResult, TopicAuthorizationError> {
        let mut fetched_offsets: HashMap<TopicPartition, ListOffsetData> = HashMap::new();
        let mut partitions_to_retry: HashSet<TopicPartition> = HashSet::new();
        let mut unauthorized_topics: HashSet<String> = HashSet::new();

        for topic in response.topics() {
            for partition in &topic.partitions {
                let tp = TopicPartition::new(topic.name.clone(), partition.partition_index);
                let error = Errors::for_code(partition.error_code);
                match error {
                    Errors::None => {
                        if partition.offset != UNKNOWN_OFFSET {
                            let leader_epoch = if partition.leader_epoch == UNKNOWN_EPOCH {
                                None
                            } else {
                                Some(partition.leader_epoch)
                            };
                            fetched_offsets.insert(
                                tp.clone(),
                                ListOffsetData::new(partition.offset, Some(partition.timestamp), leader_epoch),
                            );
                        }
                    },
                    Errors::UnsupportedForMessageFormat => {
                        // Message format pre-0.10.0 has no timestamps: drop entry.
                    },
                    // Java splits these into three arms that differ ONLY in their
                    // log statement — all three call `partitionsToRetry.add(tp)`
                    // (`OffsetFetcherUtils.java:136-150`): a `debug` for the
                    // leader/replica group, a distinct `warn` for
                    // UNKNOWN_TOPIC_OR_PARTITION (`:147`), and a `warn` for the
                    // default (`:154`). `UnsupportedForMessageFormat` above has a
                    // fourth (`:132`).
                    //
                    // NOT TRANSLATED: this type has no logger. Java's
                    // `OffsetFetcherUtils` takes a `LogContext`
                    // (`OffsetFetcherUtils.java:73`, `:84`); adding one here means
                    // threading it through `OffsetFetcherUtilsState::new` AND
                    // `OffsetsRequestManager::new` (11 call sites, none of which
                    // has one in scope). Deferred deliberately rather than
                    // bundled here: it is log-only — the four statements change no
                    // behaviour, so folding the arms is behaviour-preserving —
                    // and the constructor cascade would make any regression in
                    // this round ambiguous. Note Java's default-arm text says
                    // "unexpected exception", which §2 would render as
                    // "unexpected error" when it is translated.
                    Errors::NotLeaderOrFollower
                    | Errors::ReplicaNotAvailable
                    | Errors::KafkaStorageError
                    | Errors::OffsetNotAvailable
                    | Errors::LeaderNotAvailable
                    | Errors::FencedLeaderEpoch
                    | Errors::UnknownLeaderEpoch
                    | Errors::UnknownTopicOrPartition => {
                        partitions_to_retry.insert(tp);
                    },
                    Errors::TopicAuthorizationFailed => {
                        unauthorized_topics.insert(tp.topic().to_string());
                    },
                    _ => {
                        partitions_to_retry.insert(tp);
                    },
                }
            }
        }

        if !unauthorized_topics.is_empty() {
            return Err(TopicAuthorizationError::new(unauthorized_topics));
        }
        Ok(ListOffsetResult::new(fetched_offsets, partitions_to_retry))
    }

    /// Updates the `SubscriptionState` HW / LSO for each fetched partition,
    /// matching Java's `updateSubscriptionState`.
    pub(crate) fn update_subscription_state(
        &self,
        fetched_offsets: &HashMap<TopicPartition, ListOffsetData>,
        isolation_level: IsolationLevel,
    ) -> Result<(), Error> {
        let mut subs = self.subscriptions.lock().expect("SubscriptionState mutex poisoned");
        for (partition, data) in fetched_offsets {
            if subs.is_assigned(partition) {
                let offset = data.offset;
                if isolation_level == IsolationLevel::ReadCommitted {
                    subs.update_last_stable_offset(partition, offset)?;
                } else {
                    subs.update_high_watermark(partition, offset)?;
                }
            }
        }
        Ok(())
    }

    /// Stores `error` for later propagation on the next call to
    /// `get_offset_reset_strategy_for_partitions`. Idempotent: a second
    /// call while a previous error is pending logs a warning and is
    /// dropped (matches Java's `compareAndSet(null, error)`).
    pub(crate) fn maybe_set_reset_error(&self, error: Error) {
        let mut guard = self.cached_reset_positions_error.lock().expect("reset cache poisoned");
        if guard.is_none() {
            *guard = Some(error);
        } else {
            log::error!("Discarding error resetting positions because another error is pending");
        }
    }

    /// Stores `error` for later propagation on the next call to
    /// `refresh_and_get_partitions_to_validate`. Idempotent.
    pub(crate) fn maybe_set_validate_error(&self, error: Error) {
        let mut guard = self.cached_validate_positions_error.lock().expect("validate cache poisoned");
        if guard.is_none() {
            *guard = Some(error);
        } else {
            log::error!("Discarding error validating positions because another error is pending");
        }
    }

    /// Returns the next reset-strategy-per-partition map for partitions
    /// that need a reset. Mirrors Java's
    /// `getOffsetResetStrategyForPartitions()`.
    ///
    /// # Errors
    ///
    /// Returns the cached reset error (if any) or
    /// `Error` (NoOffsetForPartition) when a partition needs reset
    /// but its strategy carries no timestamp.
    pub(crate) fn get_offset_reset_strategy_for_partitions(
        &self,
        now_ms: i64,
    ) -> Result<HashMap<TopicPartition, AutoOffsetResetStrategy>, Error> {
        // Propagate any pending exception, clearing the slot atomically.
        if let Some(err) = self.cached_reset_positions_error.lock().expect("reset cache poisoned").take() {
            return Err(err);
        }

        let subs = self.subscriptions.lock().expect("SubscriptionState mutex poisoned");
        let partitions = subs.partitions_needing_reset(now_ms);
        let mut result: HashMap<TopicPartition, AutoOffsetResetStrategy> = HashMap::new();
        for partition in &partitions {
            let strategy = subs.reset_strategy(partition)?.ok_or_else(|| {
                Error::ConsumerNoOffsetForPartition(ConsumerNoOffsetForPartitionError::new(partition.clone()))
            })?;
            if strategy.timestamp().is_some() {
                result.insert(partition.clone(), strategy);
            } else {
                return Err(Error::ConsumerNoOffsetForPartition(ConsumerNoOffsetForPartitionError::new(
                    partition.clone(),
                )));
            }
        }
        Ok(result)
    }

    /// Runs the `PositionsValidator` reset-positions path: returns the
    /// partition map needing validation against the current metadata.
    ///
    /// Mirrors `PositionsValidator.refreshAndGetPartitionsToValidate`.
    ///
    /// # Errors
    ///
    /// Propagates any cached validate-positions error from
    /// [`Self::maybe_set_validate_error`].
    pub(crate) fn refresh_and_get_partitions_to_validate(
        &self,
        now_ms: i64,
    ) -> Result<HashMap<TopicPartition, FetchPosition>, Error> {
        if let Some(err) = self
            .cached_validate_positions_error
            .lock()
            .expect("validate cache poisoned")
            .take()
        {
            return Err(err);
        }
        self.validate_positions_on_metadata_change();
        let subs = self.subscriptions.lock().expect("SubscriptionState mutex poisoned");
        Ok(subs.partitions_needing_validation(now_ms))
    }

    /// Validates positions whenever the metadata snapshot has advanced.
    ///
    /// Mirrors `PositionsValidator.validatePositionsOnMetadataChange`.
    pub(crate) fn validate_positions_on_metadata_change(&self) {
        let metadata_arc = self.metadata.metadata_arc();
        let new_version = metadata_arc.update_version();
        if self.metadata_update_version.swap(new_version, Ordering::Relaxed) == new_version {
            return;
        }
        // Snapshot assigned partitions to avoid double-locking the mutex
        // while invoking maybe_validate_position_for_current_leader (which
        // also locks).
        let assigned: Vec<TopicPartition> = {
            let subs = self.subscriptions.lock().expect("SubscriptionState mutex poisoned");
            subs.assigned_partitions().into_iter().collect()
        };
        for tp in &assigned {
            let leader_and_epoch = metadata_arc.current_leader(tp);
            let mut subs = self.subscriptions.lock().expect("SubscriptionState mutex poisoned");
            subs.maybe_validate_position_for_current_leader(&self.api_versions, tp, &leader_and_epoch);
        }
    }

    /// Resets a partition's position to the offset returned by a
    /// `ListOffsets` response, mirroring Java's `resetPositionIfNeeded`.
    pub(crate) fn reset_position_if_needed(
        &self,
        partition: &TopicPartition,
        requested_reset_strategy: AutoOffsetResetStrategy,
        offset_data: &ListOffsetData,
    ) -> Result<(), Error> {
        let metadata_arc = self.metadata.metadata_arc();
        let position = FetchPosition::with_leader(
            offset_data.offset,
            // Empty offset epoch ensures we skip validation.
            None,
            metadata_arc.current_leader(partition),
        );
        if let Some(epoch) = offset_data.leader_epoch {
            // Best-effort metadata bump — discard the returned bool.
            let _ = metadata_arc.update_last_seen_epoch_if_newer(partition, epoch);
        }
        let mut subs = self.subscriptions.lock().expect("SubscriptionState mutex poisoned");
        subs.maybe_seek_unvalidated(partition, position, Some(&requested_reset_strategy));
        Ok(())
    }

    /// Mirrors Java's `onSuccessfulResponseForResettingPositions`. Updates
    /// the retry timer for partitions that need retry and resets the
    /// position for partitions that returned an offset.
    pub(crate) fn on_successful_response_for_resetting_positions(
        &self,
        result: &ListOffsetResult,
        partition_strategy: &HashMap<TopicPartition, AutoOffsetResetStrategy>,
        now_ms: i64,
    ) -> Result<(), Error> {
        if !result.partitions_to_retry.is_empty() {
            let mut subs = self.subscriptions.lock().expect("SubscriptionState mutex poisoned");
            subs.request_failed(&result.partitions_to_retry, now_ms + self.retry_backoff_ms);
            drop(subs);
            self.metadata.metadata_arc().request_update(false);
        }
        for (partition, data) in &result.fetched_offsets {
            if let Some(strategy) = partition_strategy.get(partition).cloned() {
                self.reset_position_if_needed(partition, strategy, data)?;
            }
        }
        Ok(())
    }

    /// Mirrors Java's `onFailedResponseForResettingPositions`. Bumps the
    /// retry timer for all `reset_timestamps` partitions and caches the
    /// error for the next call to `get_offset_reset_strategy_for_partitions`
    /// (unless retriable).
    pub(crate) fn on_failed_response_for_resetting_positions(
        &self,
        reset_timestamps: &HashMap<TopicPartition, ListOffsetsPartition>,
        error: Error,
        now_ms: i64,
    ) {
        let partitions: HashSet<TopicPartition> = reset_timestamps.keys().cloned().collect();
        {
            let mut subs = self.subscriptions.lock().expect("SubscriptionState mutex poisoned");
            subs.request_failed(&partitions, now_ms + self.retry_backoff_ms);
        }
        self.metadata.metadata_arc().request_update(false);
        if !error.is_retriable_error() {
            self.maybe_set_reset_error(error);
        }
    }

    /// Mirrors Java's `onSuccessfulResponseForValidatingPositions`. Runs
    /// `SubscriptionState::maybe_complete_validation` per partition and
    /// returns any detected log truncations.
    pub(crate) fn on_successful_response_for_validating_positions(
        &self,
        fetch_positions: &HashMap<TopicPartition, FetchPosition>,
        offsets_result: &OffsetForEpochResult,
        now_ms: i64,
    ) -> Vec<LogTruncation> {
        let mut truncations: Vec<LogTruncation> = Vec::new();
        if !offsets_result.partitions_to_retry.is_empty() {
            let mut subs = self.subscriptions.lock().expect("SubscriptionState mutex poisoned");
            subs.set_next_allowed_retry(&offsets_result.partitions_to_retry, now_ms + self.retry_backoff_ms);
            drop(subs);
            self.metadata.metadata_arc().request_update(false);
        }
        for (tp, epoch_end_offset) in &offsets_result.end_offsets {
            if let Some(request_position) = fetch_positions.get(tp) {
                let mut subs = self.subscriptions.lock().expect("SubscriptionState mutex poisoned");
                if let Some(t) = subs.maybe_complete_validation(tp, request_position, epoch_end_offset) {
                    truncations.push(t);
                }
            }
        }
        truncations
    }

    /// Mirrors Java's `onFailedResponseForValidatingPositions`. Bumps the
    /// retry timer for all `fetch_positions` keys and caches the error
    /// (unless retriable).
    pub(crate) fn on_failed_response_for_validating_positions(
        &self,
        fetch_positions: &HashMap<TopicPartition, FetchPosition>,
        error: Error,
        now_ms: i64,
    ) {
        let partitions: HashSet<TopicPartition> = fetch_positions.keys().cloned().collect();
        {
            let mut subs = self.subscriptions.lock().expect("SubscriptionState mutex poisoned");
            subs.request_failed(&partitions, now_ms + self.retry_backoff_ms);
        }
        self.metadata.metadata_arc().request_update(false);
        if !error.is_retriable_error() {
            self.maybe_set_validate_error(error);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Node;
    use crate::metadata::LeaderAndEpoch;

    /// Verifies that `topics_for_partitions` collects topic names.
    #[test]
    fn topics_for_partitions_collects_unique_names() {
        let parts = [
            TopicPartition::new("a".to_string(), 0),
            TopicPartition::new("a".to_string(), 1),
            TopicPartition::new("b".to_string(), 0),
        ];
        let topics = topics_for_partitions(parts.iter());
        assert_eq!(topics.len(), 2);
        assert!(topics.contains("a"));
        assert!(topics.contains("b"));
    }

    /// Verifies that `regroup_fetch_positions_by_leader` partitions by node
    /// and drops entries without a leader.
    #[test]
    fn regroup_fetch_positions_by_leader_groups_and_drops_no_leader() {
        let node1 = Node::new(1, "host1".to_string(), 9092);
        let node2 = Node::new(2, "host2".to_string(), 9092);
        let mut map = HashMap::new();
        map.insert(
            TopicPartition::new("a".to_string(), 0),
            FetchPosition::with_leader(0, None, LeaderAndEpoch::new(Some(node1.clone()), Some(1))),
        );
        map.insert(
            TopicPartition::new("a".to_string(), 1),
            FetchPosition::with_leader(0, None, LeaderAndEpoch::new(Some(node1.clone()), Some(1))),
        );
        map.insert(
            TopicPartition::new("b".to_string(), 0),
            FetchPosition::with_leader(0, None, LeaderAndEpoch::new(Some(node2.clone()), Some(1))),
        );
        // Dropped entry (no leader).
        map.insert(
            TopicPartition::new("c".to_string(), 0),
            FetchPosition::with_leader(0, None, LeaderAndEpoch::no_leader_or_epoch()),
        );

        let grouped = regroup_fetch_positions_by_leader(&map);
        assert_eq!(grouped.len(), 2);
        assert_eq!(grouped.get(&node1).unwrap().len(), 2);
        assert_eq!(grouped.get(&node2).unwrap().len(), 1);
    }

    /// Verifies that `build_offsets_for_times_result` includes every
    /// requested partition (with `None` for those not in `fetched_offsets`)
    /// and decorates the fetched ones with leader epoch.
    #[test]
    fn build_offsets_for_times_result_preserves_input_keys() {
        let mut search = HashMap::new();
        let tp_a = TopicPartition::new("a".to_string(), 0);
        let tp_b = TopicPartition::new("b".to_string(), 0);
        search.insert(tp_a.clone(), -1);
        search.insert(tp_b.clone(), -1);

        let mut fetched = HashMap::new();
        fetched.insert(tp_a.clone(), ListOffsetData::new(100, Some(1234), Some(5)));

        let result = build_offsets_for_times_result(&search, &fetched);
        assert_eq!(result.len(), 2);
        let entry_a = result.get(&tp_a).unwrap().as_ref().expect("fetched entry has offset");
        assert_eq!(entry_a.offset(), 100);
        assert_eq!(entry_a.timestamp(), 1234);
        assert_eq!(entry_a.leader_epoch(), Some(5));
        assert!(result.get(&tp_b).unwrap().is_none());
    }

    // =================================================================
    //   Phase 31: OffsetValidation → LogTruncation structured payload
    //
    //   Translated from `OffsetFetcherTest.testOffsetValidationWithGivenEpochOffset`
    //   (the @MethodSource matrix). These assert the structured
    //   `LogTruncation` payload (offsetOutOfRangePartitions, divergentOffsets)
    //   returned by `on_successful_response_for_validating_positions`. The
    //   end-to-end `Error::from(Error::ConsumerLogTruncation(ConsumerLogTruncationError::new(..)))`
    //   conversion flattens to `Error::IllegalState` and loses the
    //   structured fields (documented design choice in
    //   `src/consumer/errors.rs:237`), so the structured payload MUST be
    //   asserted here, against the `LogTruncation` struct directly.
    // =================================================================

    use crate::common::internals::ClusterResourceListeners;
    use crate::consumer::ConsumerConfig;
    use crate::offset_for_leader_epoch_response_data::EpochEndOffset;

    /// Build an `OffsetFetcherUtilsState` with `tp` assigned and seeked
    /// (unvalidated) to `offset`/`epoch` — i.e. AWAITING_VALIDATION with a
    /// known leader. `reset_strategy` controls the subscription's default
    /// reset policy (EARLIEST → reset on truncation; NONE → LogTruncation).
    fn fetcher_utils_awaiting_validation(
        tp: &TopicPartition,
        offset: i64,
        epoch: i32,
        reset_strategy: AutoOffsetResetStrategy,
    ) -> (OffsetFetcherUtilsState, std::sync::Arc<Mutex<SubscriptionState>>) {
        let config = ConsumerConfig::from_properties(&std::collections::HashMap::from([
            ("bootstrap.servers".to_string(), "localhost:9092".to_string()),
            ("group.id".to_string(), "g".to_string()),
        ]))
        .expect("config");
        let subscriptions = std::sync::Arc::new(Mutex::new(SubscriptionState::new(reset_strategy)));
        let metadata = std::sync::Arc::new(ConsumerMetadata::from_config(
            &config,
            subscriptions.clone(),
            ClusterResourceListeners::new(),
        ));
        let leader = Node::new(0, "localhost".to_string(), 1969);
        let leader_and_epoch = LeaderAndEpoch::new(Some(leader), Some(epoch));
        let position = FetchPosition::with_leader(offset, Some(epoch), leader_and_epoch);
        {
            let mut subs = subscriptions.lock().expect("subs");
            subs.assign_from_user(HashSet::from([tp.clone()])).expect("assign");
            subs.seek_unvalidated(tp, position).expect("seek");
        }
        let state =
            OffsetFetcherUtilsState::new(metadata, subscriptions.clone(), std::sync::Arc::new(ApiVersions::new()), 500);
        (state, subscriptions)
    }

    /// Helper: build an `OffsetForEpochResult` carrying a single `tp` end
    /// offset / leader epoch (mirrors Java's
    /// `prepareOffsetsForLeaderEpochResponse`).
    fn epoch_result(tp: &TopicPartition, leader_epoch: i32, end_offset: i64) -> OffsetForEpochResult {
        let mut eeo = EpochEndOffset::new();
        eeo.set_partition(tp.partition());
        eeo.set_error_code(Errors::None.code());
        eeo.set_leader_epoch(leader_epoch);
        eeo.set_end_offset(end_offset);
        let mut end_offsets = HashMap::new();
        end_offsets.insert(tp.clone(), eeo);
        OffsetForEpochResult::new(end_offsets, HashSet::new())
    }

    /// Java parity: `testOffsetValidationresetPositionForUndefined*WithDefinedResetPolicy`
    /// — undefined epoch/offset + EARLIEST reset policy → the partition is
    /// reset (request_offset_reset_default), no LogTruncation is returned,
    /// and the partition leaves AWAITING_VALIDATION.
    #[test]
    fn validation_undefined_with_defined_reset_policy_resets() {
        use crate::common::requests::offsets_for_leader_epoch_response::{UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET};
        let initial_offset = 5i64;
        let initial_epoch = 1i32;
        // (leader_epoch, end_offset) cases mirroring Java:
        //   testOffsetValidationresetPositionForUndefinedEpochWithDefinedResetPolicy:  (UNDEFINED_EPOCH, 0)
        //   testOffsetValidationresetPositionForUndefinedOffsetWithDefinedResetPolicy: (2, UNDEFINED_EPOCH_OFFSET)
        for (leader_epoch, end_offset) in [(UNDEFINED_EPOCH, 0i64), (2, UNDEFINED_EPOCH_OFFSET)] {
            let tp = TopicPartition::new("t1".to_string(), 0);
            let (state, subscriptions) = fetcher_utils_awaiting_validation(
                &tp,
                initial_offset,
                initial_epoch,
                AutoOffsetResetStrategy::EARLIEST,
            );
            let position = subscriptions.lock().unwrap().position(&tp).unwrap().unwrap().clone();
            let mut fetch_positions = HashMap::new();
            fetch_positions.insert(tp.clone(), position);

            let truncations = state.on_successful_response_for_validating_positions(
                &fetch_positions,
                &epoch_result(&tp, leader_epoch, end_offset),
                0,
            );
            assert!(
                truncations.is_empty(),
                "EARLIEST reset policy must NOT surface LogTruncation (case {leader_epoch}/{end_offset})"
            );
            let subs = subscriptions.lock().unwrap();
            // Reset requested → partition is now AWAITING_RESET, no longer
            // awaiting validation.
            assert!(
                !subs.awaiting_validation(&tp).expect("assigned"),
                "partition must leave AWAITING_VALIDATION after reset"
            );
            assert!(
                subs.is_offset_reset_needed(&tp).expect("assigned"),
                "EARLIEST reset must mark the partition AWAITING_RESET"
            );
        }
    }

    /// Java parity:
    /// `testOffsetValidationresetPositionForUndefined{Epoch,Offset}WithUndefinedResetPolicy`
    /// — undefined epoch/offset + NONE reset policy → LogTruncation with
    /// `offsetOutOfRangePartitions == {tp: initialOffset}` and EMPTY
    /// `divergentOffsets`. The partition stays AWAITING_VALIDATION.
    #[test]
    fn validation_undefined_with_undefined_reset_policy_log_truncation() {
        use crate::common::requests::offsets_for_leader_epoch_response::{UNDEFINED_EPOCH, UNDEFINED_EPOCH_OFFSET};
        let initial_offset = 5i64;
        let initial_epoch = 1i32;
        for (leader_epoch, end_offset) in [(UNDEFINED_EPOCH, 0i64), (2, UNDEFINED_EPOCH_OFFSET)] {
            let tp = TopicPartition::new("t1".to_string(), 0);
            let (state, subscriptions) =
                fetcher_utils_awaiting_validation(&tp, initial_offset, initial_epoch, AutoOffsetResetStrategy::NONE);
            let position = subscriptions.lock().unwrap().position(&tp).unwrap().unwrap().clone();
            let mut fetch_positions = HashMap::new();
            fetch_positions.insert(tp.clone(), position);

            let truncations = state.on_successful_response_for_validating_positions(
                &fetch_positions,
                &epoch_result(&tp, leader_epoch, end_offset),
                0,
            );
            assert_eq!(truncations.len(), 1, "NONE reset policy must surface one LogTruncation");
            let t = &truncations[0];
            assert_eq!(t.topic_partition, tp);
            // Java: assertEquals(singletonMap(tp, initialOffset),
            //                     thrown.offsetOutOfRangePartitions())
            assert_eq!(t.fetch_position.offset, initial_offset);
            // Java: assertEquals(Collections.emptyMap(), thrown.divergentOffsets())
            assert!(
                t.divergent_offset_opt.is_none(),
                "undefined epoch/offset must produce EMPTY divergent offsets (case {leader_epoch}/{end_offset})"
            );
            assert!(
                subscriptions.lock().unwrap().awaiting_validation(&tp).expect("assigned"),
                "partition must STAY AWAITING_VALIDATION on LogTruncation"
            );
        }
    }

    /// Java parity:
    /// `testOffsetValidationTriggerLogTruncationForBadOffsetWithUndefinedResetPolicy`
    /// — a bad end offset (1 < initialOffset 5) with a defined epoch +
    /// NONE reset policy → LogTruncation with
    /// `offsetOutOfRangePartitions == {tp: 5}` and
    /// `divergentOffsets == {tp: OffsetAndMetadata(endOffset=1, epoch=1, "")}`.
    #[test]
    fn validation_bad_offset_with_undefined_reset_policy_log_truncation() {
        let initial_offset = 5i64;
        let initial_epoch = 1i32;
        let bad_leader_epoch = 1i32;
        let bad_end_offset = 1i64;
        let tp = TopicPartition::new("t1".to_string(), 0);
        let (state, subscriptions) =
            fetcher_utils_awaiting_validation(&tp, initial_offset, initial_epoch, AutoOffsetResetStrategy::NONE);
        let position = subscriptions.lock().unwrap().position(&tp).unwrap().unwrap().clone();
        let mut fetch_positions = HashMap::new();
        fetch_positions.insert(tp.clone(), position);

        let truncations = state.on_successful_response_for_validating_positions(
            &fetch_positions,
            &epoch_result(&tp, bad_leader_epoch, bad_end_offset),
            0,
        );
        assert_eq!(truncations.len(), 1);
        let t = &truncations[0];
        assert_eq!(t.topic_partition, tp);
        assert_eq!(
            t.fetch_position.offset, initial_offset,
            "offsetOutOfRangePartitions key offset == 5"
        );
        // Java: OffsetAndMetadata(endOffset, Optional.of(leaderEpoch), "")
        let divergent = t
            .divergent_offset_opt
            .as_ref()
            .expect("divergent offset present for bad offset");
        assert_eq!(divergent.offset(), bad_end_offset, "divergent offset == broker end offset (1)");
        assert_eq!(divergent.leader_epoch(), Some(bad_leader_epoch), "divergent leader epoch == 1");
        assert!(
            subscriptions.lock().unwrap().awaiting_validation(&tp).expect("assigned"),
            "partition must STAY AWAITING_VALIDATION on LogTruncation"
        );
    }
}
