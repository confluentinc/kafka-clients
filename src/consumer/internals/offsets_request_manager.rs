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

//! `OffsetsRequestManager` — drives `ListOffsets` and `OffsetsForLeaderEpoch`
//! requests required to reset and validate partition positions.
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.OffsetsRequestManager`.
//!
//! ## Scope
//!
//! This translation covers the **reset** (`reset_positions_if_needed`),
//! **validate** (`validate_positions_if_needed`) and
//! **init-with-committed-offsets**
//! ([`Self::init_with_committed_offsets_if_needed`]) code paths that Java's
//! `updateFetchPositions` orchestrates. The remaining Java methods that
//! depend on `CommitRequestManager` (`fetch_offsets` for `list_offsets`,
//! `update_fetch_positions`, `prepare_fetch_offsets_requests`) are
//! deferred to Phase 10 commits 3b/3c.
//!
//! ## Concurrency
//!
//! All mutable state lives on `OffsetsRequestManager` itself, which the
//! consumer's bg task owns exclusively (consumer-threading.md §10). The
//! `Arc<Mutex<SubscriptionState>>` is borrowed for the brief critical
//! sections; no lock is held across an `.await` (CLAUDE.md §9.6).
//!
//! ## Pending-completion handling
//!
//! Java composes `CompletableFuture<...>` chains to attach response
//! handlers. The Rust translation enqueues an `UnsentRequest`, then —
//! when the bg task dispatches it — takes its
//! [`oneshot::Receiver<Result<ClientResponse, KafkaError>>`] and
//! `spawn`s a small task that awaits the response and forwards the
//! result back into the manager via a `mpsc` channel. The manager
//! drains the channel on its next `poll`, applying the success/failure
//! handler from `OffsetFetcherUtilsState`.

#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use tokio::sync::{mpsc, oneshot};

use crate::api_versions::ApiVersions;
use crate::client_response::ClientResponse;
use crate::common::cluster_resource::ClusterResource;
use crate::common::cluster_resource_listener::ClusterResourceListener;
use crate::common::requests::{
    ConcreteResponse, ListOffsetsRequestBuilder, OffsetsForLeaderEpochResponse,
    list_offsets_request::CONSUMER_REPLICA_ID,
};
use crate::common::{IsolationLevel, KafkaError, Node, TopicPartition};
use crate::consumer::OffsetAndMetadata;
use crate::list_offsets_request_data::ListOffsetsPartition;

use super::auto_offset_reset_strategy::AutoOffsetResetStrategy;
use super::commit_request_manager::CommitRequestManager;
use super::consumer_metadata::ConsumerMetadata;
use super::network_client_delegate::{PollResult, UnsentRequest};
use super::offset_and_timestamp_internal::OffsetAndTimestampInternal;
use super::offset_fetcher_utils::{
    ListOffsetData, ListOffsetResult, OffsetFetcherUtilsState, build_offsets_for_times_result,
    has_usable_offset_for_leader_epoch_version, regroup_fetch_positions_by_leader, regroup_partition_map_by_node,
    topics_for_partitions,
};
use super::offsets_for_leader_epoch_client::OffsetsForLeaderEpochClient;
use super::request_manager::RequestManager;
use super::subscription_state::{FetchPosition, SubscriptionState};

/// Tracks pending request completions that need to be processed on the
/// next `poll()` call.
pub(crate) enum PendingCompletion {
    ListOffsetsForReset {
        reset_timestamps: HashMap<TopicPartition, ListOffsetsPartition>,
        partition_strategies: HashMap<TopicPartition, AutoOffsetResetStrategy>,
        result: Result<ClientResponse, KafkaError>,
    },
    OffsetsForLeaderEpoch {
        fetch_positions: HashMap<TopicPartition, FetchPosition>,
        result: Result<ClientResponse, KafkaError>,
    },
    /// Per-node ListOffsets response for the `fetch_offsets` flow. The
    /// outer `state` accumulates partial results across all nodes (Java:
    /// `MultiNodeRequest.addPartialResult`); when every node has reported,
    /// the global future resolves.
    ListOffsetsForFetchOffsets {
        state: Arc<Mutex<ListOffsetsRequestState>>,
        node_partitions: HashMap<TopicPartition, ListOffsetsPartition>,
        result: Result<ClientResponse, KafkaError>,
    },
}

/// Sender used to deliver the global outcome of a `fetch_offsets` call
/// to one waiter. Aliased to keep the `Vec<...>` declaration tractable
/// for clippy's `type_complexity` lint.
type FetchOffsetsWaiter =
    oneshot::Sender<Result<HashMap<TopicPartition, Option<OffsetAndTimestampInternal>>, KafkaError>>;

/// Per-`fetch_offsets` request state. Mirrors Java's
/// `OffsetsRequestManager.ListOffsetsRequestState`.
///
/// Accumulates per-node partial results until every node responds (or
/// fails), then routes the global outcome to all registered waiters and
/// — on full success — updates the subscription state HW/LSO via
/// [`OffsetFetcherUtilsState::update_subscription_state`].
///
/// Java tracks one `CompletableFuture<ListOffsetResult>` (the
/// "global result") whose downstream `whenComplete` handlers form a chain.
/// Rust uses a `Vec<oneshot::Sender>` for waiters because `oneshot` is
/// single-consumer. The reuse pattern (multiple `update_fetch_positions`
/// calls piggy-backing on the same fetch) does not apply here — each
/// `fetch_offsets` call is independent — but the second `fetchOffsets`
/// call from the same set of timestamps can fail with `StaleMetadataException`
/// and be parked on `requests_to_retry`, where it's replayed when
/// `OffsetsClusterListener::on_update` fires.
pub(crate) struct ListOffsetsRequestState {
    /// The input timestamps the caller asked us to resolve. Java:
    /// `ListOffsetsRequestState.timestampsToSearch`.
    timestamps_to_search: HashMap<TopicPartition, i64>,
    /// Whether the broker must support precise timestamps (Java:
    /// `requireTimestamps`).
    require_timestamps: bool,
    /// Partitions whose `groupListOffsetRequests` failed (no leader) and
    /// must be retried after the next metadata update. Java:
    /// `ListOffsetsRequestState.remainingToSearch`.
    remaining_to_search: HashMap<TopicPartition, i64>,
    /// Offsets collected across all per-node responses. Java:
    /// `ListOffsetsRequestState.fetchedOffsets`.
    fetched_offsets: HashMap<TopicPartition, ListOffsetData>,
    /// Number of per-node responses still expected before the global
    /// future resolves. Java tracks this through a `MultiNodeRequest`
    /// inside `buildListOffsetsRequests`; we collapse it onto the request
    /// state to avoid maintaining a parallel struct.
    expected_responses: usize,
    /// Outer-future waiters. The driver completes each by `send`.
    waiters: Vec<FetchOffsetsWaiter>,
    /// `true` once the global result has been routed (either by success
    /// or failure). Once set, further per-node completions are ignored —
    /// mirrors Java's `CompletableFuture::complete` idempotency.
    completed: bool,
}

impl ListOffsetsRequestState {
    fn new(timestamps_to_search: HashMap<TopicPartition, i64>, require_timestamps: bool) -> Self {
        Self {
            timestamps_to_search,
            require_timestamps,
            remaining_to_search: HashMap::new(),
            fetched_offsets: HashMap::new(),
            expected_responses: 0,
            waiters: Vec::new(),
            completed: false,
        }
    }

    /// Java: `addPartitionsToRetry`. Records the input timestamp for each
    /// partition that the broker couldn't serve (retriable error / unknown
    /// leader) so the metadata-update replay can rebuild requests.
    fn add_partitions_to_retry(&mut self, partitions: &HashSet<TopicPartition>) {
        for tp in partitions {
            if let Some(ts) = self.timestamps_to_search.get(tp) {
                self.remaining_to_search.insert(tp.clone(), *ts);
            }
        }
    }
}

/// A deferred call to `init_with_partition_offsets_if_needed` scheduled by
/// the spawned [`Self::update_fetch_positions`] continuation. Carries the
/// originally-captured `initializing_partitions` set so the
/// `reset_initializing_positions` filter still excludes partitions added
/// to the assignment after the OffsetFetch was issued (Java parity:
/// `OffsetsRequestManager.testUpdatePositionsDoesNotResetPositionBeforeRetrievingOffsetsForNewlyAddedPartition`).
///
/// Decoupling this from `PendingCompletion` keeps the post-fetch reset
/// chain self-contained — the followup is owned by `update_fetch_positions`
/// and never interleaved with response-handling work.
struct PendingFollowupReset {
    initial_partitions: HashSet<TopicPartition>,
}

/// State shared between the [`OffsetsRequestManager`] and the
/// [`OffsetsClusterListener`].
///
/// The cluster listener is registered on
/// [`crate::metadata::Metadata::add_cluster_update_listener`] which gives it
/// only a `&self` borrow — so any state the listener needs to mutate
/// (`requests_to_send`, `requests_to_retry`) must live inside an
/// `Arc<Mutex<...>>`. Read-only fields (metadata, subscription state,
/// isolation level, timeout, fetcher-utils, completion sender) are wrapped
/// in `Arc` so the listener can call into [`Self::prepare_fetch_offsets_requests`]
/// without going through the manager.
///
/// Java carries every one of these as instance fields on
/// `OffsetsRequestManager` itself; the Rust split is a pure mechanical
/// consequence of `&self` listener access vs. `&mut self` manager-poll
/// access.
pub(crate) struct OffsetsManagerShared {
    pub(crate) subscription_state: Arc<Mutex<SubscriptionState>>,
    pub(crate) metadata: Arc<ConsumerMetadata>,
    pub(crate) isolation_level: IsolationLevel,
    pub(crate) request_timeout_ms: i64,
    pub(crate) offset_fetcher_utils: Arc<OffsetFetcherUtilsState>,
    pub(crate) pending_completions_tx: mpsc::UnboundedSender<PendingCompletion>,
    /// Requests built but not yet drained by `poll`. Java:
    /// `requestsToSend`.
    pub(crate) requests_to_send: Mutex<Vec<UnsentRequest>>,
    /// Per-`fetch_offsets` request state queued for replay when the next
    /// metadata update arrives. Java: `requestsToRetry` (a `HashSet`).
    /// Rust uses a `Vec` because deduplication is by-pointer (`Arc::ptr_eq`)
    /// not value equality — Java relies on Java identity hash for the
    /// equivalent semantic.
    pub(crate) requests_to_retry: Mutex<Vec<Arc<Mutex<ListOffsetsRequestState>>>>,
    /// Set to `true` by [`OffsetsClusterListener::on_update`] when the
    /// metadata cache advances; consumed by [`OffsetsRequestManager::poll`]
    /// to replay any requests parked on [`Self::requests_to_retry`].
    ///
    /// **Why defer?** Java fires `ClusterResourceListener.onUpdate` from
    /// inside `Metadata::update`, which holds `Metadata`'s internal lock.
    /// Java's `synchronized` is reentrant, so the listener can call back
    /// into `metadata.currentLeader(tp)` and other accessors without
    /// deadlock. Rust's `std::sync::Mutex` is NOT reentrant — calling
    /// `metadata.current_leader(tp)` from inside `on_update` would
    /// deadlock. We avoid the deadlock by flagging the update here and
    /// draining the retry queue on the next `poll()` (which holds no
    /// metadata locks). The observable behaviour matches Java: the
    /// retried requests appear on `requests_to_send` before the next
    /// network poll.
    pub(crate) metadata_updated: std::sync::atomic::AtomicBool,
    /// Nodes for which the manager wants the bg task to invoke
    /// `NetworkClientDelegate::try_connect` on its next iteration. Mirrors
    /// Java's `networkClientDelegate.tryConnect(node)` side effect inside
    /// `sendOffsetsForLeaderEpochRequestsAndValidatePositions`.
    ///
    /// Drained by [`OffsetsRequestManager::poll`] into
    /// [`PollResult::try_connect`].
    pub(crate) try_connect_queue: Mutex<Vec<Node>>,
}

impl OffsetsManagerShared {
    /// Mirrors Java's `prepareFetchOffsetsRequests(timestampsToSearch,
    /// requireTimestamps, listOffsetsRequestState)`.
    ///
    /// Builds per-leader `ListOffsets` requests for `timestamps_to_search`
    /// and enqueues them on [`Self::requests_to_send`]. Partitions whose
    /// leader is unknown to the metadata snapshot are added to the
    /// request state's `remaining_to_search`, and — if no requests could
    /// be built at all — the entire state is parked on
    /// [`Self::requests_to_retry`] so the next
    /// [`OffsetsClusterListener::on_update`] replays it.
    ///
    /// `&Arc<Self>` rather than `&self` because the spawned per-node
    /// response forwarder needs a strong reference. The listener path
    /// also passes `Arc<Self>` (it owns one).
    fn prepare_fetch_offsets_requests(
        self_arc: &Arc<Self>,
        timestamps_to_search: &HashMap<TopicPartition, i64>,
        require_timestamps: bool,
        state: &Arc<Mutex<ListOffsetsRequestState>>,
    ) {
        match Self::build_list_offsets_requests(self_arc, timestamps_to_search, require_timestamps, state) {
            Ok(unsent_requests) => {
                let mut guard = self_arc.requests_to_send.lock().expect("requests_to_send mutex poisoned");
                guard.extend(unsent_requests);
            },
            Err(StaleMetadata) => {
                // Java: `requestsToRetry.add(listOffsetsRequestState)`.
                // The state's `remaining_to_search` is already populated
                // with every input partition (since none had a leader).
                let mut guard = self_arc.requests_to_retry.lock().expect("requests_to_retry mutex poisoned");
                guard.push(state.clone());
            },
        }
    }

    /// Mirrors Java's
    /// `buildListOffsetsRequests(timestampsToSearch, requireTimestamps,
    /// listOffsetsRequestState)`.
    ///
    /// Returns the list of `UnsentRequest`s built for partitions with
    /// known leaders, or `Err(StaleMetadata)` when not a single partition
    /// has a known leader — the caller parks the state on
    /// `requests_to_retry`.
    fn build_list_offsets_requests(
        self_arc: &Arc<Self>,
        timestamps_to_search: &HashMap<TopicPartition, i64>,
        require_timestamps: bool,
        state: &Arc<Mutex<ListOffsetsRequestState>>,
    ) -> Result<Vec<UnsentRequest>, StaleMetadata> {
        log::debug!("Building ListOffsets request for partitions {:?}", timestamps_to_search.keys());
        let by_node = Self::group_list_offset_requests(self_arc, timestamps_to_search, Some(state));
        if by_node.is_empty() {
            return Err(StaleMetadata);
        }

        let mut unsent_requests: Vec<UnsentRequest> = Vec::new();
        let node_count = by_node.len();
        {
            let mut guard = state.lock().expect("ListOffsetsRequestState mutex poisoned");
            // Java initialises `expectedResponses` to `nodeCount`. We add
            // here so an interleaved second `fetch_offsets` call (parked
            // and replayed) accumulates on top of in-flight expectations.
            guard.expected_responses = guard.expected_responses.saturating_add(node_count);
        }

        for (node, target_times) in by_node {
            let mut builder = ListOffsetsRequestBuilder::for_consumer(require_timestamps, self_arc.isolation_level);
            let topics = crate::common::requests::ListOffsetsRequest::to_list_offsets_topics(&target_times);
            builder.set_target_times(topics);
            builder.set_timeout_ms(self_arc.request_timeout_ms as i32);

            log::debug!("Creating ListOffset request for broker {} to fetch offsets", node);

            let mut unsent = UnsentRequest::new(Box::new(builder), Some(node));
            let response_rx = unsent.take_response_receiver().expect("receiver fresh");
            let tx = self_arc.pending_completions_tx.clone();
            let state_for_task = Arc::clone(state);
            let target_times_for_task = target_times.clone();
            tokio::spawn(async move {
                let result = match response_rx.await {
                    Ok(r) => r,
                    Err(_) => Err(KafkaError::new(crate::common::protocol::Errors::NetworkException)),
                };
                let _ = tx.send(PendingCompletion::ListOffsetsForFetchOffsets {
                    state: state_for_task,
                    node_partitions: target_times_for_task,
                    result,
                });
            });
            unsent_requests.push(unsent);
        }
        Ok(unsent_requests)
    }

    /// Mirrors Java's
    /// `groupListOffsetRequests(timestampsToSearch, listOffsetsRequestState)`.
    ///
    /// Looks up the current leader for each partition in the metadata
    /// cache; partitions without a known leader are recorded on the
    /// state's `remaining_to_search` (when `state` is `Some`) and a
    /// metadata-update is scheduled. Returns the grouped per-node map of
    /// `ListOffsetsPartition` payloads ready to be packed into a single
    /// per-broker request.
    fn group_list_offset_requests(
        self_arc: &Arc<Self>,
        timestamps_to_search: &HashMap<TopicPartition, i64>,
        state: Option<&Arc<Mutex<ListOffsetsRequestState>>>,
    ) -> HashMap<crate::common::Node, HashMap<TopicPartition, ListOffsetsPartition>> {
        let mut partition_data_map: HashMap<TopicPartition, ListOffsetsPartition> = HashMap::new();

        for (tp, &offset) in timestamps_to_search {
            let leader_and_epoch = self_arc.metadata.current_leader(tp);
            if leader_and_epoch.leader.is_none() {
                log::debug!("Leader for partition {} is unknown for fetching offset {}", tp, offset);
                self_arc.metadata.metadata_arc().request_update(true);
                if let Some(state_arc) = state {
                    let mut guard = state_arc.lock().expect("ListOffsetsRequestState mutex poisoned");
                    guard.remaining_to_search.insert(tp.clone(), offset);
                }
            } else {
                let current_leader_epoch = leader_and_epoch
                    .epoch
                    .unwrap_or(crate::common::requests::list_offsets_response::UNKNOWN_EPOCH);
                let mut part = ListOffsetsPartition::new();
                part.set_partition_index(tp.partition());
                part.set_timestamp(offset);
                part.set_current_leader_epoch(current_leader_epoch);
                partition_data_map.insert(tp.clone(), part);
            }
        }
        regroup_partition_map_by_node(&self_arc.metadata, &partition_data_map)
    }

    /// Mirrors the success branch of Java's per-`buildListOffsetsRequests`
    /// `whenComplete` plus the `MultiNodeRequest.addPartialResult` /
    /// `onComplete` chain.
    ///
    /// Applies the per-node `ListOffsetResult` to the shared state.
    /// When all expected responses have arrived, the global outcome is
    /// routed to every waiter on the state, the subscription state's
    /// HW/LSO is updated, and — if any partitions still need to be
    /// searched (retriable errors) — the state is re-parked on
    /// `requests_to_retry` and a metadata update is scheduled.
    fn apply_partial_result(
        self_arc: &Arc<Self>,
        state: &Arc<Mutex<ListOffsetsRequestState>>,
        node_partitions: &HashMap<TopicPartition, ListOffsetsPartition>,
        partial: ListOffsetResult,
    ) {
        let mut guard = state.lock().expect("ListOffsetsRequestState mutex poisoned");
        if guard.completed {
            return;
        }
        // Java: `fetchedOffsets.putAll(multiNodeResult.fetchedOffsets)` +
        // `addPartitionsToRetry(multiNodeResult.partitionsToRetry)`.
        // `expectedResponses` is decremented once *per node*, not per
        // partition.
        for (tp, data) in partial.fetched_offsets {
            guard.fetched_offsets.insert(tp, data);
        }
        guard.add_partitions_to_retry(&partial.partitions_to_retry);
        // Apply HW/LSO updates from the partial result (Java:
        // `offsetFetcherUtils.updateSubscriptionState(multiNodeResult.fetchedOffsets, isolationLevel)`).
        // Borrow the inner fetched_offsets map (the
        // `OffsetFetcherUtilsState::update_subscription_state` API takes
        // `&HashMap<TopicPartition, ListOffsetData>`).
        let just_fetched: HashMap<TopicPartition, ListOffsetData> =
            partial_fetched_for_node(&guard.fetched_offsets, node_partitions.keys());
        // Drop the guard before calling into the utils (which locks the
        // subscription state mutex) — CLAUDE.md §16.
        let isolation_level = self_arc.isolation_level;
        drop(guard);
        if let Err(err) = self_arc
            .offset_fetcher_utils
            .update_subscription_state(&just_fetched, isolation_level)
        {
            log::debug!(
                "Skipped HW/LSO subscription-state update for {} partitions: {}",
                just_fetched.len(),
                err
            );
        }

        // Reacquire the guard to update the expected-responses counter
        // and possibly route the global result.
        let mut guard = state.lock().expect("ListOffsetsRequestState mutex poisoned");
        if guard.expected_responses == 0 {
            return;
        }
        guard.expected_responses -= 1;
        if guard.expected_responses > 0 {
            return;
        }

        // Last response — finalise.
        if guard.remaining_to_search.is_empty() {
            guard.completed = true;
            let fetched = guard.fetched_offsets.clone();
            let timestamps_to_search = guard.timestamps_to_search.clone();
            let waiters = std::mem::take(&mut guard.waiters);
            drop(guard);
            let result = build_offsets_for_times_result(&timestamps_to_search, &fetched);
            for waiter in waiters {
                let _ = waiter.send(Ok(result.clone()));
            }
            // Java: `listOffsetsRequestState.globalResult.whenComplete(...
            // metadata.clearTransientTopics(); ...)`. The hook fires on
            // BOTH success and failure paths
            // (`OffsetsRequestManager.java:200-209`). Mirror it here on
            // the success branch; `fail_request_state` handles the
            // failure branch.
            self_arc.metadata.clear_transient_topics();
        } else {
            // Java: `requestsToRetry.add(listOffsetsRequestState);
            // metadata.requestUpdate(false);`. NOTE: the transient-topic
            // hook does NOT fire here — the global result hasn't
            // completed (the state is parked for retry). Java's
            // `whenComplete` will fire on the eventual completion when
            // the retry resolves it.
            self_arc.metadata.metadata_arc().request_update(false);
            drop(guard);
            self_arc
                .requests_to_retry
                .lock()
                .expect("requests_to_retry mutex poisoned")
                .push(state.clone());
        }
    }

    /// Routes a hard failure (network / authentication / topic-auth) to
    /// every waiter on the request state and marks the state completed.
    ///
    /// Mirrors Java's `globalResult.completeExceptionally(error)` /
    /// `listOffsetsRequestState.globalResult.completeExceptionally(error)`.
    /// Also fires the `clearTransientTopics` hook because Java's
    /// `whenComplete` runs on the failure branch too.
    fn fail_request_state(self_arc: &Arc<Self>, state: &Arc<Mutex<ListOffsetsRequestState>>, err: KafkaError) {
        let waiters = {
            let mut guard = state.lock().expect("ListOffsetsRequestState mutex poisoned");
            if guard.completed {
                return;
            }
            guard.completed = true;
            std::mem::take(&mut guard.waiters)
        };
        for waiter in waiters {
            let _ = waiter.send(Err(err.clone()));
        }
        // Java parity (`OffsetsRequestManager.java:200-209` —
        // `whenComplete` fires on success AND failure).
        self_arc.metadata.clear_transient_topics();
    }
}

/// Marker error returned by [`OffsetsManagerShared::build_list_offsets_requests`]
/// when not a single partition has a known leader. Java throws
/// `StaleMetadataException` to signal the same condition. We use a Rust
/// unit struct to keep the failure path zero-allocation and to make the
/// signature self-documenting.
struct StaleMetadata;

/// Returns a sub-map of `fetched_offsets` restricted to the keys of
/// `node_partitions`. Used to subset the cumulative `fetched_offsets`
/// down to "only the entries from this per-node response" before passing
/// into `update_subscription_state`.
fn partial_fetched_for_node<'a>(
    fetched_offsets: &HashMap<TopicPartition, ListOffsetData>,
    node_partitions: impl IntoIterator<Item = &'a TopicPartition>,
) -> HashMap<TopicPartition, ListOffsetData> {
    let mut out: HashMap<TopicPartition, ListOffsetData> = HashMap::new();
    for tp in node_partitions {
        if let Some(data) = fetched_offsets.get(tp) {
            out.insert(tp.clone(), data.clone());
        }
    }
    out
}

/// The KIP-848 `OffsetsRequestManager`. Drives `ListOffsets` (for offset
/// reset, position validation and `fetch_offsets`) and
/// `OffsetsForLeaderEpoch` (for position validation) requests.
///
/// Java: `org.apache.kafka.clients.consumer.internals.OffsetsRequestManager`
/// (which extends `RequestManager` and `ClusterResourceListener`). The
/// Rust translation factors the `ClusterResourceListener` callback into a
/// separate handle struct ([`OffsetsClusterListener`]) so we can register
/// the callback with `Metadata::add_cluster_update_listener` without
/// binding the listener's lifetime to the manager's `&mut`. The
/// callback-shared state lives in [`OffsetsManagerShared`].
pub(crate) struct OffsetsRequestManager {
    /// Shared state accessible to the [`OffsetsClusterListener`] and to
    /// per-response spawned tasks.
    shared: Arc<OffsetsManagerShared>,
    /// Java: `defaultApiTimeoutMs`. Used by
    /// [`Self::init_with_committed_offsets_if_needed`] to compute the
    /// internal `fetchCommittedDeadlineMs` (Java
    /// `OffsetsRequestManager.java:379`).
    default_api_timeout_ms: i64,
    api_versions: Arc<ApiVersions>,
    /// Optional handle to the commit manager — `None` when the consumer
    /// has no group. Java's `OffsetsRequestManager` accepts the commit
    /// manager as a non-null parameter but the
    /// `initWithCommittedOffsetsIfNeeded` path explicitly returns early
    /// when no group is configured; this Rust translation collapses both
    /// cases into `None`.
    commit_request_manager: Option<Arc<CommitRequestManager>>,
    /// The in-flight `OffsetFetch` triggered by the most-recent
    /// [`Self::init_with_committed_offsets_if_needed`] call. Java:
    /// `pendingOffsetFetchEvent`. Reused by subsequent calls when the
    /// initializing-partition set matches, so a poll that times out
    /// while an `OffsetFetch` is still in flight does not waste the
    /// outstanding request. Cleared when the underlying fetch resolves.
    pending_offset_fetch_event: Arc<Mutex<Option<PendingFetchCommittedRequest>>>,

    /// Completions waiting to be applied on the next `poll` call.
    /// `Mutex` because the receiver-side `tokio::spawn` writes into it
    /// from another task.
    pending_completions_rx: mpsc::UnboundedReceiver<PendingCompletion>,
    /// Pending `init_with_partition_offsets_if_needed` follow-ups
    /// scheduled by the spawned task driving
    /// [`Self::update_fetch_positions`]. Drained on the next `poll` —
    /// Java performs this in-line inside `whenComplete`; the Rust bg
    /// task achieves the same effect by deferring to the request-manager
    /// `poll` call, which owns `&mut self` and can mutate
    /// `requests_to_send`.
    pending_followup_rx: mpsc::UnboundedReceiver<PendingFollowupReset>,
    pending_followup_tx: mpsc::UnboundedSender<PendingFollowupReset>,
    /// Java: `cachedUpdatePositionsException`. Stores an error that
    /// occurred during a previous `updateFetchPositions` call whose
    /// triggering event already expired by the time the inner OffsetFetch
    /// chain resolved. Surfaced on the next call via
    /// [`Self::maybe_complete_with_previous_exception`] (Java parity:
    /// `OffsetsRequestManager.maybeCompleteWithPreviousException`).
    cached_update_positions_exception: Arc<Mutex<Option<KafkaError>>>,
    closing: bool,
}

/// In-flight `OffsetFetch` carrying the set of initializing partitions
/// that triggered it plus any additional `oneshot::Sender`s that have
/// piggy-backed on the same request (the "reuse" path).
///
/// Mirrors Java's `OffsetsRequestManager.PendingFetchCommittedRequest`.
/// Java holds a `CompletableFuture` whose downstream `whenComplete`
/// handlers form a chain; Rust uses a `Vec<oneshot::Sender>` because
/// `oneshot::Receiver` is single-consumer.
struct PendingFetchCommittedRequest {
    requested_partitions: HashSet<TopicPartition>,
    /// Senders waiting for the in-flight fetch to resolve. The first
    /// entry is for the caller that initially issued the fetch; each
    /// subsequent reuse adds another sender. When the fetch completes,
    /// the driver task drains the vec and forwards the result (or its
    /// `()` ack on success — offsets are written into the
    /// `SubscriptionState` as side effects) to every sender.
    waiters: Vec<oneshot::Sender<Result<(), KafkaError>>>,
}

/// The error used when a committed-offset fetch's waiter is orphaned because
/// the pending-fetch slot was replaced by a request for a different partition
/// set (see `init_with_committed_offsets_if_needed`).
///
/// This state has no Java counterpart. In Java
/// (`OffsetsRequestManager.initWithCommittedOffsetsIfNeeded`) each caller's
/// `result` future is completed by a `whenComplete` handler hung off
/// `commitRequestManager.fetchOffsets(...)`; that chain is independent of the
/// `pendingOffsetFetchEvent` field, so reassigning the field only swaps the
/// reuse-lookup entry and never abandons an earlier caller. The earlier caller
/// still resolves with whatever the fetch yields: success, or — once
/// `fetchCommittedDeadlineMs` passes — the `TimeoutException` that
/// `CommitRequestManager.fetchOffsetsWithRetries` wraps its retriable failures
/// in. Java therefore cannot surface a `DisconnectException` here.
///
/// Rust's `oneshot` is single-consumer, so the waiter list lives *inside* the
/// replaceable slot and its senders are dropped on replacement. We must still
/// complete the caller (CLAUDE.md §5 — a silently hung future is worse than an
/// explicit error), so we complete it with the outcome Java produces for an
/// abandoned fetch: a timeout. That matters beyond cosmetics —
/// `is_ignorable_async_poll_error` swallows only `KafkaError::Timeout`, exactly
/// as Java's `maybeCompleteAsyncPollEventExceptionally` swallows only
/// `TimeoutException`, so `poll()` returns empty records and retries instead of
/// failing the caller. Returning a retriable *wire* error here (as this code
/// previously did, with `Errors::NetworkException`) escaped that predicate and
/// surfaced from `poll()` as a spurious `NetworkException` during rebalances,
/// which Java never does.
fn superseded_committed_fetch_error() -> KafkaError {
    KafkaError::timeout("Committed-offset fetch was superseded before it completed")
}

impl OffsetsRequestManager {
    /// Constructs a new `OffsetsRequestManager`.
    ///
    /// Mirrors Java's constructor minus the `LogContext` parameter
    /// (implicit in Rust through `log`). `commit_request_manager` is
    /// `Option<Arc<CommitRequestManager>>` so test fixtures and the
    /// (out-of-scope) group-less assignor path can pass `None`; Java
    /// requires the commit manager non-null and short-circuits the
    /// `initWithCommittedOffsetsIfNeeded` path internally.
    #[allow(clippy::too_many_arguments)] // Mirrors Java constructor argument list.
    pub(crate) fn new(
        subscription_state: Arc<Mutex<SubscriptionState>>,
        metadata: Arc<ConsumerMetadata>,
        isolation_level: IsolationLevel,
        retry_backoff_ms: i64,
        request_timeout_ms: i64,
        default_api_timeout_ms: i64,
        api_versions: Arc<ApiVersions>,
        commit_request_manager: Option<Arc<CommitRequestManager>>,
    ) -> Self {
        let offset_fetcher_utils = Arc::new(OffsetFetcherUtilsState::new(
            metadata.clone(),
            subscription_state.clone(),
            api_versions.clone(),
            retry_backoff_ms,
        ));
        let (pending_completions_tx, pending_completions_rx) = mpsc::unbounded_channel();
        let (pending_followup_tx, pending_followup_rx) = mpsc::unbounded_channel();
        let shared = Arc::new(OffsetsManagerShared {
            subscription_state,
            metadata: metadata.clone(),
            isolation_level,
            request_timeout_ms,
            offset_fetcher_utils,
            pending_completions_tx,
            requests_to_send: Mutex::new(Vec::new()),
            requests_to_retry: Mutex::new(Vec::new()),
            metadata_updated: std::sync::atomic::AtomicBool::new(false),
            try_connect_queue: Mutex::new(Vec::new()),
        });
        let manager = Self {
            shared: shared.clone(),
            default_api_timeout_ms,
            api_versions,
            commit_request_manager,
            pending_offset_fetch_event: Arc::new(Mutex::new(None)),
            pending_completions_rx,
            pending_followup_rx,
            pending_followup_tx,
            cached_update_positions_exception: Arc::new(Mutex::new(None)),
            closing: false,
        };
        // Register the cluster metadata update callback. The listener
        // re-issues any deferred `fetch_offsets` requests on metadata
        // change (Java: `OffsetsRequestManager.onUpdate`).
        metadata
            .metadata_arc()
            .add_cluster_update_listener(Box::new(OffsetsClusterListener { shared }));
        manager
    }

    /// Reset offsets for all assigned partitions that require it. Offsets
    /// are reset with timestamps according to the configured reset
    /// strategy. This generates `ListOffsets` requests, enqueued for the
    /// next `poll` call.
    ///
    /// Mirrors Java's `resetPositionsIfNeeded()`.
    ///
    /// # Errors
    ///
    /// Propagates the cached reset-positions exception from a previous
    /// call (e.g. `TopicAuthorizationException`), or any
    /// `NoOffsetForPartitionException` raised when a partition needs
    /// reset but no strategy is configured.
    pub(crate) fn reset_positions_if_needed(&mut self, current_time_ms: i64) -> Result<(), KafkaError> {
        let partition_strategies = self
            .shared
            .offset_fetcher_utils
            .get_offset_reset_strategy_for_partitions(current_time_ms)?;
        if partition_strategies.is_empty() {
            return Ok(());
        }
        self.send_list_offsets_requests_and_reset_positions(partition_strategies, current_time_ms);
        Ok(())
    }

    /// Validate positions for all assigned partitions that have a leader
    /// change pending validation. Generates `OffsetsForLeaderEpoch`
    /// requests grouped by leader, enqueued for the next `poll` call.
    ///
    /// Mirrors Java's `validatePositionsIfNeeded()`.
    ///
    /// # Errors
    ///
    /// Propagates the cached validate-positions exception from a previous
    /// call (e.g. a saved `LogTruncationException`).
    pub(crate) fn validate_positions_if_needed(&mut self, current_time_ms: i64) -> Result<(), KafkaError> {
        let partitions_to_validate = self
            .shared
            .offset_fetcher_utils
            .refresh_and_get_partitions_to_validate(current_time_ms)?;
        if partitions_to_validate.is_empty() {
            return Ok(());
        }
        self.send_offsets_for_leader_epoch_requests_and_validate_positions(partitions_to_validate, current_time_ms);
        Ok(())
    }

    /// Retrieve offsets for the given partitions and timestamps.
    ///
    /// Mirrors Java's
    /// `OffsetsRequestManager.fetchOffsets(Map<TopicPartition, Long>, boolean)`.
    ///
    /// For each partition, returns the offset of the first message whose
    /// timestamp is `≥` the target timestamp. The returned future resolves
    /// when all `ListOffsets` responses have been received and processed —
    /// each input partition appears in the result map, with `None` for
    /// partitions whose offset could not be determined.
    ///
    /// Partitions whose leader is unknown at request-build time are parked
    /// on the manager's [`OffsetsManagerShared::requests_to_retry`] queue
    /// and replayed when the next metadata update arrives (see
    /// [`OffsetsClusterListener::on_update`]).
    ///
    /// Used by `ListOffsetsEvent` and `CurrentLagEvent`.
    pub(crate) fn fetch_offsets(
        &mut self,
        timestamps_to_search: HashMap<TopicPartition, i64>,
        require_timestamps: bool,
    ) -> oneshot::Receiver<Result<HashMap<TopicPartition, Option<OffsetAndTimestampInternal>>, KafkaError>> {
        let (tx, rx) = oneshot::channel();
        if timestamps_to_search.is_empty() {
            let _ = tx.send(Ok(HashMap::new()));
            return rx;
        }
        // Java: `metadata.addTransientTopics(topicsForPartitions(...))` so
        // the next metadata refresh covers any topics not already in the
        // cache.
        self.shared
            .metadata
            .add_transient_topics(topics_for_partitions(timestamps_to_search.keys()));

        let state = Arc::new(Mutex::new(ListOffsetsRequestState::new(
            timestamps_to_search.clone(),
            require_timestamps,
        )));
        {
            let mut guard = state.lock().expect("ListOffsetsRequestState mutex poisoned");
            guard.waiters.push(tx);
        }

        OffsetsManagerShared::prepare_fetch_offsets_requests(
            &self.shared,
            &timestamps_to_search,
            require_timestamps,
            &state,
        );

        rx
    }

    /// Test/debug accessor — number of `fetch_offsets` requests currently
    /// queued for retry (Java: `requestsToRetry()`).
    ///
    /// Drains any pending metadata-update replay first so a recent
    /// `metadata.update()` call is observable through this accessor —
    /// matches Java's synchronous `onUpdate` semantics (see the
    /// `metadata_updated` flag docs for why the replay itself is
    /// deferred).
    pub(crate) fn requests_to_retry_count(&self) -> usize {
        if self.shared.metadata_updated.swap(false, std::sync::atomic::Ordering::AcqRel) {
            OffsetsManagerShared::replay_retries_after_metadata_update(&self.shared);
        }
        self.shared
            .requests_to_retry
            .lock()
            .expect("requests_to_retry mutex poisoned")
            .len()
    }

    /// Test/debug accessor — number of pending unsent requests waiting on
    /// the next `poll` (Java: `requestsToSend()`).
    ///
    /// Drains any pending metadata-update replay first so a recent
    /// `metadata.update()` call is observable through this accessor.
    pub(crate) fn requests_to_send_count(&self) -> usize {
        if self.shared.metadata_updated.swap(false, std::sync::atomic::Ordering::AcqRel) {
            OffsetsManagerShared::replay_retries_after_metadata_update(&self.shared);
        }
        self.shared
            .requests_to_send
            .lock()
            .expect("requests_to_send mutex poisoned")
            .len()
    }

    /// Mirrors Java's `sendListOffsetsRequestsAndResetPositions`.
    ///
    /// Groups partitions by current leader and builds one `ListOffsets`
    /// request per leader. Each request's response receiver is wired
    /// into a tokio task that forwards the result into the manager's
    /// `pending_completions` channel; `poll` drains the channel and
    /// applies success/failure handlers via `OffsetFetcherUtilsState`.
    fn send_list_offsets_requests_and_reset_positions(
        &mut self,
        partition_strategies: HashMap<TopicPartition, AutoOffsetResetStrategy>,
        current_time_ms: i64,
    ) {
        let now_ms = current_time_ms;
        // Build per-partition `ListOffsetsPartition` carrying the strategy's
        // wire timestamp and the current leader epoch. Mirrors Java's
        // `groupListOffsetRequests` (OffsetsRequestManager.java:892-914):
        // for each partition look up the current leader — if it is unknown,
        // request a metadata update and skip the partition; otherwise stamp
        // the request with the partition's `currentLeaderEpoch`.
        let metadata_arc = self.shared.metadata.metadata_arc();
        let mut timestamps_to_search: HashMap<TopicPartition, ListOffsetsPartition> = HashMap::new();
        for (tp, strategy) in &partition_strategies {
            if let Some(ts) = strategy.timestamp() {
                let leader_and_epoch = metadata_arc.current_leader(tp);
                if leader_and_epoch.leader.is_none() {
                    log::debug!("Leader for partition {tp} is unknown for fetching offset {ts}");
                    metadata_arc.request_update(true);
                    continue;
                }
                let current_leader_epoch = leader_and_epoch
                    .epoch
                    .unwrap_or(crate::common::requests::list_offsets_response::UNKNOWN_EPOCH);
                let mut part = ListOffsetsPartition::new();
                part.set_partition_index(tp.partition());
                part.set_timestamp(ts);
                part.set_current_leader_epoch(current_leader_epoch);
                timestamps_to_search.insert(tp.clone(), part);
            }
        }

        // Group by current leader (the `leader.is_none()` entries were
        // already dropped above with a metadata-update request).
        let by_node = regroup_partition_map_by_node(&self.shared.metadata, &timestamps_to_search);

        for (node, reset_timestamps) in by_node {
            // Java: subscriptionState.setNextAllowedRetry(...) to back off
            // any duplicate sends while this is in flight.
            {
                let partitions: std::collections::HashSet<TopicPartition> = reset_timestamps.keys().cloned().collect();
                let mut subs = self.shared.subscription_state.lock().expect("SubscriptionState mutex poisoned");
                subs.set_next_allowed_retry(&partitions, now_ms + self.shared.request_timeout_ms);
            }

            let mut builder = ListOffsetsRequestBuilder::for_consumer(false, self.shared.isolation_level);
            let topics = crate::common::requests::ListOffsetsRequest::to_list_offsets_topics(&reset_timestamps);
            builder.set_target_times(topics);
            builder.set_timeout_ms(self.shared.request_timeout_ms as i32);
            // Override the wire replica id (Builder::new used
            // CONSUMER_REPLICA_ID above; this is a no-op assert but
            // documents intent).
            debug_assert_eq!(CONSUMER_REPLICA_ID, -1);

            let mut unsent = UnsentRequest::new(Box::new(builder), Some(node));
            // Take the receiver and spawn a forwarder.
            let response_rx = unsent.take_response_receiver().expect("receiver fresh");
            let tx = self.shared.pending_completions_tx.clone();
            let strategies = partition_strategies.clone();
            let timestamps = reset_timestamps.clone();
            tokio::spawn(async move {
                let result = match response_rx.await {
                    Ok(r) => r,
                    Err(_) => Err(KafkaError::new(crate::common::protocol::Errors::NetworkException)),
                };
                let _ = tx.send(PendingCompletion::ListOffsetsForReset {
                    reset_timestamps: timestamps,
                    partition_strategies: strategies,
                    result,
                });
            });
            self.shared
                .requests_to_send
                .lock()
                .expect("requests_to_send mutex poisoned")
                .push(unsent);
        }
    }

    /// Mirrors Java's `sendOffsetsForLeaderEpochRequestsAndValidatePositions`.
    fn send_offsets_for_leader_epoch_requests_and_validate_positions(
        &mut self,
        partitions_to_validate: HashMap<TopicPartition, FetchPosition>,
        current_time_ms: i64,
    ) {
        let now_ms = current_time_ms;
        let regrouped = regroup_fetch_positions_by_leader(&partitions_to_validate);

        for (node, fetch_positions) in regrouped {
            if node.is_empty() {
                self.shared.metadata.metadata_arc().request_update(true);
                continue;
            }
            let node_versions = self.api_versions.get(node.id_string());
            let Some(versions) = node_versions else {
                // Java: `networkClientDelegate.tryConnect(node)`. The
                // Rust bg task (Phase 10 commit 7) owns the delegate;
                // queue the node so the next bg-task iteration drains
                // `PollResult::try_connect` into
                // `NetworkClientDelegate::try_connect`.
                self.shared
                    .try_connect_queue
                    .lock()
                    .expect("try_connect_queue mutex poisoned")
                    .push(node);
                continue;
            };
            if !has_usable_offset_for_leader_epoch_version(&versions) {
                log::debug!(
                    "Skipping validation of fetch offsets for partitions {:?} since the broker does not support the \
                     required protocol version (introduced in Kafka 2.3)",
                    fetch_positions.keys()
                );
                let mut subs = self.shared.subscription_state.lock().expect("SubscriptionState mutex poisoned");
                for partition in fetch_positions.keys() {
                    let _ = subs.complete_validation(partition);
                }
                continue;
            }
            {
                let partitions: std::collections::HashSet<TopicPartition> = fetch_positions.keys().cloned().collect();
                let mut subs = self.shared.subscription_state.lock().expect("SubscriptionState mutex poisoned");
                subs.set_next_allowed_retry(&partitions, now_ms + self.shared.request_timeout_ms);
            }

            let builder = OffsetsForLeaderEpochClient::prepare_request(&fetch_positions);
            let mut unsent = UnsentRequest::new(Box::new(builder), Some(node));
            let response_rx = unsent.take_response_receiver().expect("receiver fresh");
            let tx = self.shared.pending_completions_tx.clone();
            let positions = fetch_positions.clone();
            tokio::spawn(async move {
                let result = match response_rx.await {
                    Ok(r) => r,
                    Err(_) => Err(KafkaError::new(crate::common::protocol::Errors::NetworkException)),
                };
                let _ = tx.send(PendingCompletion::OffsetsForLeaderEpoch { fetch_positions: positions, result });
            });
            self.shared
                .requests_to_send
                .lock()
                .expect("requests_to_send mutex poisoned")
                .push(unsent);
        }
    }

    /// Refresh fetch positions for any partition that still requires
    /// one, using committed offsets retrieved from the group
    /// coordinator. Mirrors Java's
    /// `OffsetsRequestManager.initWithCommittedOffsetsIfNeeded`.
    ///
    /// The Java contract:
    ///
    /// 1. Empty `initializing_partitions` ⇒ completed `Ok(())`.
    /// 2. Otherwise, if there is an in-flight `OffsetFetch` whose
    ///    requested-partition set matches the new request, **reuse it**:
    ///    register the caller's `oneshot::Sender` on the existing
    ///    pending event. Java:
    ///    `OffsetsRequestManager.canReusePendingOffsetFetchEvent`.
    /// 3. Otherwise, build a new `OffsetFetch` via the commit manager,
    ///    using `max(deadline_ms, current_time_ms + default_api_timeout_ms)`
    ///    as the request deadline so the OffsetFetch outlives the
    ///    triggering `updateFetchPositions` deadline if needed.
    /// 4. On fetch completion: apply offsets via [`Self::refresh_offsets`]
    ///    (mirrors Java's `refreshOffsets` + `ConsumerUtils.refreshCommittedOffsets`)
    ///    and fan the result out to every waiter on the pending event.
    pub(crate) fn init_with_committed_offsets_if_needed(
        &self,
        initializing_partitions: HashSet<TopicPartition>,
        deadline_ms: i64,
        current_time_ms: i64,
    ) -> oneshot::Receiver<Result<(), KafkaError>> {
        let (tx, rx) = oneshot::channel();
        if initializing_partitions.is_empty() {
            let _ = tx.send(Ok(()));
            return rx;
        }
        log::debug!("Refreshing committed offsets for partitions {:?}", initializing_partitions);

        // Reuse path — same partition set still in flight.
        {
            let mut guard = self
                .pending_offset_fetch_event
                .lock()
                .expect("pending_offset_fetch_event mutex poisoned");
            if let Some(pending) = guard.as_mut()
                && pending.requested_partitions == initializing_partitions
            {
                pending.waiters.push(tx);
                return rx;
            }
        }

        // No commit manager (e.g. consumer has no group): nothing to do,
        // resolve immediately. Java reaches the same outcome by
        // short-circuiting before `commitRequestManager.fetchOffsets` is
        // called.
        let Some(commit_rm) = self.commit_request_manager.as_ref() else {
            let _ = tx.send(Ok(()));
            return rx;
        };

        // New fetch path. The deadline is the later of the caller's
        // `deadlineMs` and the internal default-api-timeout slack so the
        // underlying OffsetFetch is not abandoned solely because the
        // triggering event's deadline is short.
        let fetch_committed_deadline_ms = deadline_ms.max(current_time_ms.saturating_add(self.default_api_timeout_ms));
        let inner_rx =
            commit_rm.fetch_offsets(initializing_partitions.clone(), fetch_committed_deadline_ms, current_time_ms);

        {
            let mut guard = self
                .pending_offset_fetch_event
                .lock()
                .expect("pending_offset_fetch_event mutex poisoned");
            *guard =
                Some(PendingFetchCommittedRequest { requested_partitions: initializing_partitions, waiters: vec![tx] });
        }

        // Drive the in-flight fetch: when it resolves, apply the offsets
        // to the subscription state and fan the result out to every
        // waiter that registered on this pending event.
        let pending_slot = Arc::clone(&self.pending_offset_fetch_event);
        let subscription_state = Arc::clone(&self.shared.subscription_state);
        let metadata = Arc::clone(&self.shared.metadata);
        tokio::spawn(async move {
            let fetch_result = match inner_rx.await {
                Ok(r) => r,
                Err(_) => Err(superseded_committed_fetch_error()),
            };

            // Take the waiters out of the pending slot and clear it
            // (Java: `pendingOffsetFetchEvent = null` inside the
            // `whenComplete` callback).
            let waiters = {
                let mut guard = pending_slot.lock().expect("pending_offset_fetch_event mutex poisoned");
                guard.take().map(|p| p.waiters).unwrap_or_default()
            };

            // Apply the committed offsets to the subscription state.
            let result_for_waiters = match fetch_result {
                Ok(result) => {
                    // Java: `.thenApply(OffsetFetchResult::toOffsetMapWithNulls)`
                    // — partitions with retriable errors become `None` (Java's
                    // null) and are skipped by `refresh_offsets`.
                    let offsets = result.to_offset_map_with_nulls();
                    refresh_offsets(&offsets, subscription_state.as_ref(), metadata.as_ref());
                    Ok(())
                },
                Err(err) => {
                    log::error!("Error fetching committed offsets to update positions: {}", err);
                    Err(err)
                },
            };

            // Fan the result out to every waiter. Use `clone` because
            // `KafkaError` is cloneable but the `Result` we send is
            // by-value per sender.
            for waiter in waiters {
                let payload = match &result_for_waiters {
                    Ok(()) => Ok(()),
                    Err(err) => Err(err.clone()),
                };
                let _ = waiter.send(payload);
            }
        });

        rx
    }

    /// Drive a position update for the consumer's assigned partitions.
    ///
    /// Mirrors Java's `OffsetsRequestManager.updateFetchPositions(long)`.
    /// Returns a `oneshot::Receiver` that resolves with `Ok(())` once
    /// every initializing partition has either a fetched committed
    /// offset applied to its position, or has been marked for reset via
    /// [`SubscriptionState::reset_initializing_positions`].
    ///
    /// High-level flow (Java parity):
    ///
    /// 1. If a previous call cached an exception via
    ///    [`Self::cache_exception_if_event_expired`], surface it now
    ///    (clearing the slot).
    /// 2. Run `validate_positions_if_needed` synchronously — log
    ///    truncation detection is part of "update positions".
    /// 3. If `subscription_state.has_all_fetch_positions()`, resolve
    ///    immediately with `Ok(())`.
    /// 4. Otherwise capture the current `initializing_partitions` set
    ///    and either:
    ///     - call [`Self::init_with_committed_offsets_if_needed`], chained
    ///       with a spawned followup that, on success, marks the
    ///       captured set for reset via `reset_initializing_positions`
    ///       and enqueues the ListOffsets requests on the next `poll`;
    ///     - or, when no group is configured (no commit manager),
    ///       run [`Self::init_with_partition_offsets_if_needed`] inline.
    ///
    /// **Deviation from Java (acceptable):** Java's
    /// `resetPositionsIfNeeded()` returns a `CompletableFuture<Void>`
    /// whose completion is chained into the outer result; the Rust
    /// `reset_positions_if_needed` is fire-and-forget (no completion
    /// future yet — the underlying chain is a Phase-7d carry-over). The
    /// returned receiver therefore resolves once `reset_initializing_positions`
    /// has marked the captured partitions, NOT when the resulting
    /// ListOffsets requests have completed. Callers that need
    /// "positions actually retrieved" must continue calling
    /// `update_fetch_positions` until `has_all_fetch_positions()`
    /// returns true. This matches Java's caller pattern in
    /// `AsyncKafkaConsumer.poll`.
    pub(crate) fn update_fetch_positions(
        &mut self,
        deadline_ms: i64,
        current_time_ms: i64,
    ) -> oneshot::Receiver<Result<(), KafkaError>> {
        let (tx, rx) = oneshot::channel();

        // Java's outer try wraps the whole body in `maybeWrapAsKafkaException`.
        // The Rust translation already returns `KafkaError` from every fallible
        // call below, so the explicit wrap is a no-op (`KafkaError` is the
        // Rust equivalent of `KafkaException`).
        match self.update_fetch_positions_inner(deadline_ms, current_time_ms, tx) {
            Ok(consumed_tx) => consumed_tx,
            Err((tx, err)) => {
                // Java's outer `catch (Exception e)` in
                // `updateFetchPositions` (`OffsetsRequestManager.java:260-262`)
                // calls `result.completeExceptionally(maybeWrapAsKafkaException(e))`
                // ONLY — it does NOT register a `whenComplete` cache hook.
                // The `cacheExceptionIfEventExpired` hook is registered
                // exclusively inside `updatePositionsWithOffsets` (the
                // committed-offset path's spawned continuation in Rust).
                // Synchronously-thrown errors (e.g. a cached
                // `LogTruncationException` flowing back from
                // `validatePositionsIfNeeded`) must NOT be cached here —
                // doing so causes double-delivery when the previous call
                // already surfaced the same error.
                let _ = tx.send(Err(err));
            },
        }
        rx
    }

    /// Inner driver for [`Self::update_fetch_positions`]. Returns the
    /// `oneshot::Sender` un-fired when work has been scheduled
    /// asynchronously (the spawned chain owns the sender), or returns
    /// it back with an error when a synchronous fault occurred and the
    /// caller should fail the result.
    #[allow(clippy::type_complexity)] // Java has the same fan-out via try/catch.
    fn update_fetch_positions_inner(
        &mut self,
        deadline_ms: i64,
        current_time_ms: i64,
        tx: oneshot::Sender<Result<(), KafkaError>>,
    ) -> Result<(), (oneshot::Sender<Result<(), KafkaError>>, KafkaError)> {
        // (1) Propagate a previously-cached error from an expired event.
        if let Some(cached) = self.take_cached_update_positions_exception() {
            let _ = tx.send(Err(cached));
            return Ok(());
        }

        // (2) Validate positions. Java's `validatePositionsIfNeeded()` is
        // void; the cached LogTruncationException flows back here via the
        // Rust `Result` return.
        if let Err(err) = self.validate_positions_if_needed(current_time_ms) {
            return Err((tx, err));
        }

        // (3) Fast path — every partition already has a fetch position.
        let has_all = {
            let subs = self.shared.subscription_state.lock().expect("SubscriptionState mutex poisoned");
            subs.has_all_fetch_positions()
        };
        if has_all {
            let _ = tx.send(Ok(()));
            return Ok(());
        }

        // (4) Capture the initializing set at this moment, as Java does
        // inside `updatePositionsWithOffsets`. The captured set is used
        // BOTH as the input to the OffsetFetch AND as the filter passed
        // to `resetInitializingPositions` after the response arrives —
        // this is what prevents the reset from including partitions
        // added to the assignment mid-flight.
        let initializing_partitions = {
            let subs = self.shared.subscription_state.lock().expect("SubscriptionState mutex poisoned");
            subs.initializing_partitions()
        };

        if self.commit_request_manager.is_some() {
            // The committed-offset path: issue (or reuse) an OffsetFetch,
            // then on resolution mark the captured initializing set for
            // reset and schedule ListOffsets enqueueing on the next poll.
            let inner_rx = self.init_with_committed_offsets_if_needed(
                initializing_partitions.clone(),
                deadline_ms,
                current_time_ms,
            );
            self.spawn_committed_offsets_followup(inner_rx, initializing_partitions, deadline_ms, tx);
            Ok(())
        } else {
            // No group → no committed offsets → just mark partitions for
            // reset inline. Java reaches the same result via
            // `updatePositions = initWithPartitionOffsetsIfNeeded(...)`
            // when `commitRequestManager == null`.
            if let Err(err) = self.init_with_partition_offsets_if_needed(&initializing_partitions, current_time_ms) {
                return Err((tx, err));
            }
            let _ = tx.send(Ok(()));
            Ok(())
        }
    }

    /// Java parity: `initWithPartitionOffsetsIfNeeded(initializingPartitions)`.
    ///
    /// Marks every captured initializing partition for reset (filtered by
    /// the captured set so partitions added mid-flight are NOT reset) and
    /// then enqueues `ListOffsets` requests via
    /// [`Self::reset_positions_if_needed`].
    fn init_with_partition_offsets_if_needed(
        &mut self,
        initializing_partitions: &HashSet<TopicPartition>,
        current_time_ms: i64,
    ) -> Result<(), KafkaError> {
        {
            // Java captures `initializingPartitions::contains` as a predicate;
            // clone the set so we don't hold the subscription-state lock
            // across the predicate's borrow of `initializing_partitions`.
            let captured = initializing_partitions.clone();
            let mut subs = self.shared.subscription_state.lock().expect("SubscriptionState mutex poisoned");
            subs.reset_initializing_positions(|tp| captured.contains(tp))?;
        }
        self.reset_positions_if_needed(current_time_ms)
    }

    /// Spawn the followup task that awaits the committed-offset fetch
    /// `oneshot`, then either:
    ///
    /// - on success, locks the subscription state to mark the captured
    ///   partitions for reset and queues a `PendingFollowupReset` so the
    ///   next `poll()` enqueues ListOffsets requests, then completes
    ///   the outer `tx` with `Ok(())`;
    /// - on failure, maybe-caches the error and completes the outer
    ///   `tx` with `Err`.
    fn spawn_committed_offsets_followup(
        &self,
        inner_rx: oneshot::Receiver<Result<(), KafkaError>>,
        initial_partitions: HashSet<TopicPartition>,
        deadline_ms: i64,
        outer_tx: oneshot::Sender<Result<(), KafkaError>>,
    ) {
        let subscription_state = Arc::clone(&self.shared.subscription_state);
        let pending_followup_tx = self.pending_followup_tx.clone();
        let cached = Arc::clone(&self.cached_update_positions_exception);
        tokio::spawn(async move {
            // Await the committed-offset fetch. The sender can be dropped
            // when the pending-fetch slot is replaced by a later request for
            // a different partition set — see
            // `superseded_committed_fetch_error` for why that completes as a
            // timeout rather than a wire error.
            let fetch_result = match inner_rx.await {
                Ok(r) => r,
                Err(_) => Err(superseded_committed_fetch_error()),
            };

            let result_for_outer: Result<(), KafkaError> = match fetch_result {
                Ok(()) => {
                    // Java's `initWithPartitionOffsetsIfNeeded` runs inside
                    // the `whenComplete` chain. The synchronous bit
                    // (`reset_initializing_positions`) is done inline; the
                    // `reset_positions_if_needed` (ListOffsets enqueue) is
                    // deferred to the next `poll` via the followup channel
                    // because it requires `&mut self`.
                    let reset_outcome = {
                        let captured = initial_partitions.clone();
                        let mut subs = subscription_state.lock().expect("SubscriptionState mutex poisoned");
                        subs.reset_initializing_positions(|tp| captured.contains(tp))
                    };
                    match reset_outcome {
                        Ok(()) => {
                            // Schedule the ListOffsets-enqueue follow-up on
                            // the next poll. If the receiver is gone (manager
                            // dropped) the send is a no-op — the partitions
                            // are still marked AWAIT_RESET in the
                            // subscription state, so a subsequent
                            // `reset_positions_if_needed` call will pick them
                            // up.
                            let _ = pending_followup_tx.send(PendingFollowupReset { initial_partitions });
                            Ok(())
                        },
                        Err(err) => Err(err),
                    }
                },
                Err(err) => {
                    log::debug!("OffsetFetch chain failed during update_fetch_positions: {}", err);
                    Err(err)
                },
            };

            // Java parity: `cacheExceptionIfEventExpired` runs on every
            // result completion. We invoke the same logic here. The
            // "current time" is captured at this moment — Java reads
            // `time.milliseconds()` inside the `whenComplete` callback.
            let now_ms = current_time_ms_for_followup();
            if let Err(ref err) = result_for_outer
                && now_ms >= deadline_ms
            {
                let mut guard = cached.lock().expect("cached_update_positions_exception mutex poisoned");
                if guard.is_none() {
                    *guard = Some(err.clone());
                } else {
                    log::debug!(
                        "Discarding expired update_fetch_positions error because another error is already cached: {}",
                        err
                    );
                }
            }

            let _ = outer_tx.send(result_for_outer);
        });
    }

    /// Take and clear the cached `update_fetch_positions` error (Java:
    /// `cachedUpdatePositionsException.getAndSet(null)`).
    fn take_cached_update_positions_exception(&self) -> Option<KafkaError> {
        let mut guard = self
            .cached_update_positions_exception
            .lock()
            .expect("cached_update_positions_exception mutex poisoned");
        guard.take()
    }

    /// Test-only helper: pre-seed `cached_update_positions_exception` so
    /// the NEXT [`Self::update_fetch_positions`] call surfaces the given
    /// error. Used by sibling-module tests (e.g.
    /// `ApplicationEventProcessorTest::refresh_committed_offsets_*`) to
    /// drive the AsyncPoll failure path without standing up a full
    /// network client — Java's equivalent stubs
    /// `OffsetsRequestManager.updateFetchPositions` via Mockito.
    #[cfg(test)]
    pub(crate) fn set_cached_update_positions_exception_for_test(&self, err: KafkaError) {
        let mut guard = self
            .cached_update_positions_exception
            .lock()
            .expect("cached_update_positions_exception mutex poisoned");
        *guard = Some(err);
    }

    // Note: there is no shared `maybe_cache_update_positions_exception`
    // helper. Java's `cacheExceptionIfEventExpired` hook (registered as a
    // `whenComplete` inside `updatePositionsWithOffsets` —
    // `OffsetsRequestManager.java:283`) is inlined into the
    // committed-offset spawned followup (see
    // [`Self::spawn_committed_offsets_followup`]). Synchronous errors
    // from the outer `updateFetchPositions` body are NOT cached
    // (Java's outer `catch` block does not register the hook), so
    // there is no caller from the sync path.

    /// Drain any pending `update_fetch_positions` followups scheduled by
    /// the spawned task. Each followup triggers a
    /// [`Self::reset_positions_if_needed`] call so the captured
    /// partitions' ListOffsets requests are enqueued on the next
    /// network poll. Java does this inline inside the `whenComplete`
    /// chain; the Rust translation defers to `poll()` because
    /// `reset_positions_if_needed` requires `&mut self`.
    fn drain_pending_followups(&mut self, current_time_ms: i64) {
        while let Ok(_followup) = self.pending_followup_rx.try_recv() {
            // The captured partitions are already marked AWAIT_RESET on
            // the subscription state by the spawned task; we just need to
            // enqueue ListOffsets requests for everything in that state.
            if let Err(err) = self.reset_positions_if_needed(current_time_ms) {
                log::error!("Error enqueueing ListOffsets after committed-offset fetch: {}", err);
            }
        }
    }

    /// Drains pending completions and forwards them to the
    /// `OffsetFetcherUtilsState` handlers.
    fn drain_pending_completions(&mut self, current_time_ms: i64) -> Result<(), KafkaError> {
        let now_ms = current_time_ms;
        while let Ok(completion) = self.pending_completions_rx.try_recv() {
            match completion {
                PendingCompletion::ListOffsetsForReset { reset_timestamps, partition_strategies, result } => {
                    match result {
                        Ok(client_response) => {
                            if let Some(list_offsets_response) = downcast_list_offsets(&client_response) {
                                match self
                                    .shared
                                    .offset_fetcher_utils
                                    .handle_list_offset_response(list_offsets_response)
                                {
                                    Ok(result) => {
                                        // Reset-path: do NOT call update_subscription_state.
                                        // The fetched offsets are EARLIEST/LATEST per the reset
                                        // strategy, NOT HW/LSO — using them to update HW/LSO
                                        // would corrupt the lag metric. Java's reset path
                                        // (OffsetsRequestManager.java:613-650) does not call
                                        // updateSubscriptionState either; that's only for the
                                        // multi-node fetchOffsets flow (Java:570-573), which
                                        // is currently deferred.
                                        self.shared
                                            .offset_fetcher_utils
                                            .on_successful_response_for_resetting_positions(
                                                &result,
                                                &partition_strategies,
                                                now_ms,
                                            )?;
                                    },
                                    Err(err) => {
                                        self.shared.offset_fetcher_utils.on_failed_response_for_resetting_positions(
                                            &reset_timestamps,
                                            KafkaError::topic_authorization(err.unauthorized_topics.clone()),
                                            now_ms,
                                        );
                                    },
                                }
                            } else {
                                log::debug!("ListOffsets response had unexpected body type, ignoring");
                            }
                        },
                        Err(err) => {
                            self.shared.offset_fetcher_utils.on_failed_response_for_resetting_positions(
                                &reset_timestamps,
                                err,
                                now_ms,
                            );
                        },
                    }
                },
                PendingCompletion::OffsetsForLeaderEpoch { fetch_positions, result } => match result {
                    Ok(client_response) => {
                        if let Some(response) = downcast_offsets_for_leader_epoch(&client_response) {
                            match OffsetsForLeaderEpochClient::handle_response(&fetch_positions, response) {
                                Ok(epoch_result) => {
                                    let truncations = self
                                        .shared
                                        .offset_fetcher_utils
                                        .on_successful_response_for_validating_positions(
                                            &fetch_positions,
                                            &epoch_result,
                                            now_ms,
                                        );
                                    if !truncations.is_empty() {
                                        let fetch_offsets: HashMap<TopicPartition, i64> = truncations
                                            .iter()
                                            .map(|t| (t.topic_partition.clone(), t.fetch_position.offset))
                                            .collect();
                                        let divergent_offsets: HashMap<
                                            TopicPartition,
                                            crate::consumer::OffsetAndMetadata,
                                        > = truncations
                                            .iter()
                                            .filter_map(|t| {
                                                t.divergent_offset_opt
                                                    .as_ref()
                                                    .map(|d| (t.topic_partition.clone(), d.clone()))
                                            })
                                            .collect();
                                        let log_truncation =
                                            KafkaError::from(crate::consumer::errors::ConsumerError::log_truncation(
                                                fetch_offsets,
                                                divergent_offsets,
                                            ));
                                        self.shared.offset_fetcher_utils.maybe_set_validate_error(log_truncation);
                                    }
                                },
                                Err(err) => {
                                    self.shared.offset_fetcher_utils.on_failed_response_for_validating_positions(
                                        &fetch_positions,
                                        KafkaError::topic_authorization(err.unauthorized_topics.clone()),
                                        now_ms,
                                    );
                                },
                            }
                        } else {
                            log::debug!("OffsetsForLeaderEpoch response had unexpected body type, ignoring");
                        }
                    },
                    Err(err) => {
                        self.shared.offset_fetcher_utils.on_failed_response_for_validating_positions(
                            &fetch_positions,
                            err,
                            now_ms,
                        );
                    },
                },
                PendingCompletion::ListOffsetsForFetchOffsets { state, node_partitions, result } => {
                    self.handle_fetch_offsets_response(state, node_partitions, result);
                },
            }
        }
        // `metadata.clear_transient_topics()` runs inside the global-result
        // completion paths (`OffsetsManagerShared::apply_partial_result`
        // final-response branch + `fail_request_state`), mirroring Java's
        // `listOffsetsRequestState.globalResult.whenComplete(...)` hook at
        // `OffsetsRequestManager.java:200-209`.
        Ok(())
    }

    /// Apply a `ListOffsets` response received for the `fetch_offsets`
    /// flow. Mirrors the per-node `partialResult.whenComplete` handler
    /// inside Java's `buildListOffsetsRequests` plus
    /// `MultiNodeRequest.addPartialResult`.
    fn handle_fetch_offsets_response(
        &mut self,
        state: Arc<Mutex<ListOffsetsRequestState>>,
        node_partitions: HashMap<TopicPartition, ListOffsetsPartition>,
        result: Result<ClientResponse, KafkaError>,
    ) {
        match result {
            Ok(client_response) => match downcast_list_offsets(&client_response) {
                Some(list_offsets_response) => {
                    match self
                        .shared
                        .offset_fetcher_utils
                        .handle_list_offset_response(list_offsets_response)
                    {
                        Ok(partial) => {
                            OffsetsManagerShared::apply_partial_result(&self.shared, &state, &node_partitions, partial);
                        },
                        Err(err) => {
                            // `TopicAuthorizationException` — the global
                            // result fails immediately (Java mirrors via
                            // `globalResult.completeExceptionally`).
                            OffsetsManagerShared::fail_request_state(
                                &self.shared,
                                &state,
                                KafkaError::topic_authorization(err.unauthorized_topics.clone()),
                            );
                        },
                    }
                },
                None => {
                    log::debug!("ListOffsets response had unexpected body type, ignoring");
                },
            },
            Err(err) => {
                // Java: `result.completeExceptionally(error)` immediately.
                // For `fetch_offsets`, ALL waiters are failed even if
                // other per-node responses are still outstanding —
                // mirrors Java's `multiNodeRequest.resultFuture.completeExceptionally`.
                OffsetsManagerShared::fail_request_state(&self.shared, &state, err);
            },
        }
    }
}

/// Read the wall-clock time in milliseconds since the unix epoch — used
/// by the spawned `update_fetch_positions` followup to decide whether
/// the triggering event has already expired (Java parity:
/// `time.milliseconds()` inside `cacheExceptionIfEventExpired`).
///
/// Java's `Time` abstraction is mock-friendly; the Rust translation uses
/// `std::time::SystemTime` directly inside the spawned task. Tests that
/// need to control time can instead pass `current_time_ms` directly via
/// the synchronous fail path of [`OffsetsRequestManager::update_fetch_positions`]
/// (where the spawned task is not used).
fn current_time_ms_for_followup() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(i64::MAX)
}

/// Helper to downcast a `ClientResponse` to a `ListOffsetsResponse`.
fn downcast_list_offsets(response: &ClientResponse) -> Option<&crate::common::requests::ListOffsetsResponse> {
    match response.response_body() {
        Some(ConcreteResponse::ListOffsets(r)) => Some(r),
        _ => None,
    }
}

/// Helper to downcast a `ClientResponse` to an `OffsetsForLeaderEpochResponse`.
fn downcast_offsets_for_leader_epoch(response: &ClientResponse) -> Option<&OffsetsForLeaderEpochResponse> {
    match response.response_body() {
        Some(ConcreteResponse::OffsetsForLeaderEpoch(r)) => Some(r),
        _ => None,
    }
}

/// Apply committed offsets to the subscription state for partitions
/// that are still initializing. Mirrors Java's
/// `ConsumerUtils.refreshCommittedOffsets` composed with
/// `OffsetsRequestManager.offsetsForInitializingPartitions` (filter to
/// initializing) and `OffsetsRequestManager.refreshOffsets` (success
/// branch).
///
/// Filtering ensures we do not overwrite a position that the user set
/// manually via `seek` between the time the OffsetFetch was issued and
/// the response arrived; Java does the same in
/// `OffsetsRequestManager.offsetsForInitializingPartitions`.
fn refresh_offsets(
    offsets: &HashMap<TopicPartition, Option<OffsetAndMetadata>>,
    subscription_state: &Mutex<SubscriptionState>,
    metadata: &ConsumerMetadata,
) {
    // Snapshot the currently initializing partitions before acquiring
    // the write lock (Java reads via subscriptionState.initializingPartitions()).
    let currently_initializing = {
        let guard = subscription_state.lock().expect("SubscriptionState mutex poisoned");
        guard.initializing_partitions()
    };

    let metadata_arc = metadata.metadata_arc();
    let mut subs = subscription_state.lock().expect("SubscriptionState mutex poisoned");
    for (tp, oam) in offsets {
        let Some(oam) = oam.as_ref() else {
            // No committed offset for this partition — Java's
            // `refreshCommittedOffsets` skips `null` values.
            continue;
        };
        if !currently_initializing.contains(tp) {
            continue;
        }
        // Update the last-seen epoch first, then seek if still assigned.
        if let Some(epoch) = oam.leader_epoch() {
            let _ = metadata_arc.update_last_seen_epoch_if_newer(tp, epoch);
        }
        if !subs.is_assigned(tp) {
            // Partition was unassigned between request and response —
            // skip the seek (Java: same isAssigned guard).
            continue;
        }
        let leader_and_epoch = metadata_arc.current_leader(tp);
        let position = FetchPosition::with_leader(oam.offset(), oam.leader_epoch(), leader_and_epoch);
        let _ = subs.seek_unvalidated(tp, position);
    }
}

impl RequestManager for OffsetsRequestManager {
    /// Drains completed responses then returns the queued
    /// requests. Java: `poll(long currentTimeMs)`.
    fn poll(&mut self, current_time_ms: i64) -> PollResult {
        // If the cluster listener flagged a metadata update since the
        // last poll, replay any deferred `fetch_offsets` requests now
        // (the listener can't do this synchronously due to the
        // reentrant-lock issue — see
        // `OffsetsManagerShared::metadata_updated`).
        if self.shared.metadata_updated.swap(false, std::sync::atomic::Ordering::AcqRel) {
            OffsetsManagerShared::replay_retries_after_metadata_update(&self.shared);
        }
        // Drain any completions first, applying success/failure handlers.
        // Errors here are stored on the OffsetFetcherUtilsState for the
        // next call to surface; we don't propagate them through `poll`
        // because Java's poll signature returns only a `PollResult`.
        if let Err(err) = self.drain_pending_completions(current_time_ms) {
            log::error!("Error draining pending offset completions: {}", err);
        }
        // Run any pending update-fetch-positions follow-ups before
        // `requests_to_send` is taken — the follow-up calls
        // `reset_positions_if_needed` which pushes into the queue.
        self.drain_pending_followups(current_time_ms);
        let unsent = {
            let mut guard = self.shared.requests_to_send.lock().expect("requests_to_send mutex poisoned");
            std::mem::take(&mut *guard)
        };
        let try_connect = {
            let mut guard = self.shared.try_connect_queue.lock().expect("try_connect_queue mutex poisoned");
            std::mem::take(&mut *guard)
        };
        let mut result = if unsent.is_empty() {
            PollResult::empty()
        } else {
            PollResult::with_requests(unsent)
        };
        result.try_connect = try_connect;
        result
    }

    fn signal_close(&mut self) {
        self.closing = true;
    }
}

/// `ClusterResourceListener` registered on the consumer's `Metadata`.
///
/// When the metadata snapshot advances we replay every deferred
/// `fetch_offsets` request whose first build attempt failed with stale
/// metadata or whose responses returned retriable errors. Mirrors Java's
/// `OffsetsRequestManager.onUpdate(ClusterResource)`.
struct OffsetsClusterListener {
    shared: Arc<OffsetsManagerShared>,
}

impl ClusterResourceListener for OffsetsClusterListener {
    fn on_update(&self, _cluster_resource: &ClusterResource) {
        // The replay itself is deferred to the next `poll()` call —
        // calling `metadata.current_leader(...)` from here would attempt
        // to re-acquire `Metadata`'s inner lock, which is held by the
        // `update()` caller invoking this listener (see
        // `OffsetsManagerShared::metadata_updated` docs).
        self.shared.metadata_updated.store(true, std::sync::atomic::Ordering::Release);
    }
}

impl OffsetsManagerShared {
    /// Drain the parked retry queue, rebuilding each request against
    /// the now-current metadata snapshot. Called from
    /// [`OffsetsRequestManager::poll`] when the
    /// [`Self::metadata_updated`] flag is set. Matches Java's
    /// `OffsetsRequestManager.onUpdate(ClusterResource)` body
    /// (whose work the Rust listener defers to avoid the reentrancy
    /// deadlock — see field docs).
    fn replay_retries_after_metadata_update(self_arc: &Arc<Self>) {
        let to_process: Vec<Arc<Mutex<ListOffsetsRequestState>>> = {
            let mut guard = self_arc.requests_to_retry.lock().expect("requests_to_retry mutex poisoned");
            std::mem::take(&mut *guard)
        };
        for state in to_process {
            let (timestamps_to_search, require_timestamps, completed) = {
                let mut guard = state.lock().expect("ListOffsetsRequestState mutex poisoned");
                if guard.completed {
                    (HashMap::new(), false, true)
                } else {
                    let map = std::mem::take(&mut guard.remaining_to_search);
                    (map, guard.require_timestamps, false)
                }
            };
            if completed || timestamps_to_search.is_empty() {
                continue;
            }
            Self::prepare_fetch_offsets_requests(self_arc, &timestamps_to_search, require_timestamps, &state);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api_versions::ApiVersions;
    use crate::common::internals::ClusterResourceListeners;
    use crate::consumer::ConsumerConfig;
    use crate::consumer::internals::subscription_state::SubscriptionState;

    /// Returns a fresh manager wired against a no-op metadata / subscription state.
    fn new_manager() -> OffsetsRequestManager {
        let config = ConsumerConfig::from_properties(&std::collections::HashMap::from([
            ("bootstrap.servers".to_string(), "localhost:9092".to_string()),
            ("group.id".to_string(), "g".to_string()),
        ]))
        .expect("config");
        let subscription_state = Arc::new(Mutex::new(SubscriptionState::new(
            crate::consumer::internals::auto_offset_reset_strategy::AutoOffsetResetStrategy::EARLIEST,
        )));
        let metadata =
            ConsumerMetadata::from_config(&config, subscription_state.clone(), ClusterResourceListeners::new());
        OffsetsRequestManager::new(
            subscription_state,
            Arc::new(metadata),
            IsolationLevel::ReadUncommitted,
            100,
            30_000,
            60_000,
            Arc::new(ApiVersions::new()),
            None,
        )
    }

    /// Verifies that a freshly-constructed manager has nothing to poll.
    #[tokio::test]
    async fn empty_manager_poll_returns_empty() {
        let mut mgr = new_manager();
        let result = RequestManager::poll(&mut mgr, 0);
        assert_eq!(result.time_until_next_poll_ms, PollResult::WAIT_FOREVER);
        assert!(result.unsent_requests.is_empty());
    }

    /// Verifies that `reset_positions_if_needed` is a no-op when no
    /// partition is awaiting reset.
    #[tokio::test]
    async fn reset_positions_if_needed_with_no_resets_is_noop() {
        let mut mgr = new_manager();
        let result = mgr.reset_positions_if_needed(0);
        assert!(result.is_ok());
        assert_eq!(RequestManager::poll(&mut mgr, 0).unsent_requests.len(), 0);
    }

    /// Verifies that `validate_positions_if_needed` is a no-op when no
    /// partition needs validation.
    #[tokio::test]
    async fn validate_positions_if_needed_with_nothing_to_validate_is_noop() {
        let mut mgr = new_manager();
        let result = mgr.validate_positions_if_needed(0);
        assert!(result.is_ok());
        assert_eq!(RequestManager::poll(&mut mgr, 0).unsent_requests.len(), 0);
    }

    /// Translated from
    /// `OffsetsRequestManagerTest.testValidatePositionsAbortIfNoApiVersionsToCheckAgainstThenRecovers`.
    ///
    /// When `NodeApiVersions` for the leader are missing, the manager
    /// MUST NOT enqueue an `OffsetsForLeaderEpoch` request — instead it
    /// queues the node onto `PollResult::try_connect` (Java:
    /// `networkClientDelegate.tryConnect(node)`). Once the API versions
    /// land, the next `validate_positions_if_needed` produces the
    /// expected request and the `try_connect` queue is empty.
    #[tokio::test]
    async fn test_validate_positions_abort_if_no_api_versions_to_check_against_then_recovers() {
        // Build the manager around a controllable `ApiVersions` so the
        // test can withhold/install entries; the rest of the wiring
        // matches `new_manager`.
        let config = ConsumerConfig::from_properties(&std::collections::HashMap::from([
            ("bootstrap.servers".to_string(), "localhost:9092".to_string()),
            ("group.id".to_string(), "g".to_string()),
        ]))
        .expect("config");
        let subscription_state = Arc::new(Mutex::new(SubscriptionState::new(
            crate::consumer::internals::auto_offset_reset_strategy::AutoOffsetResetStrategy::EARLIEST,
        )));
        let metadata = Arc::new(ConsumerMetadata::from_config(
            &config,
            subscription_state.clone(),
            ClusterResourceListeners::new(),
        ));
        let api_versions = Arc::new(ApiVersions::new());
        let mut mgr = OffsetsRequestManager::new(
            subscription_state.clone(),
            metadata,
            IsolationLevel::ReadUncommitted,
            100,
            30_000,
            60_000,
            api_versions.clone(),
            None,
        );

        // Set up a partition assigned to leader-1 with a position that
        // is awaiting validation. Mirrors the Java fixture
        // `subscriptionState.partitionsNeedingValidation(...)`.
        let leader_1 = crate::common::Node::new(1, "localhost".to_string(), 9092);
        let tp = TopicPartition::new("topic".to_string(), 0);
        let leader_and_epoch = crate::metadata::LeaderAndEpoch::new(Some(leader_1.clone()), Some(3));
        let position =
            crate::consumer::internals::subscription_state::FetchPosition::with_leader(5, Some(10), leader_and_epoch);
        {
            let mut subs = subscription_state.lock().expect("subs");
            subs.assign_from_user(std::collections::HashSet::from([tp.clone()]))
                .expect("assign");
            subs.seek_unvalidated(&tp, position).expect("seek_unvalidated");
        }

        // No api version info initially available — validation aborts.
        // Java: `verify(subscriptionState, never()).setNextAllowedRetry(...)`
        // and `assertEquals(0, requestManager.requestsToSend())`.
        mgr.validate_positions_if_needed(0).expect("ok");
        let result = RequestManager::poll(&mut mgr, 0);
        assert_eq!(
            result.unsent_requests.len(),
            0,
            "no OffsetsForLeaderEpoch request must be enqueued"
        );
        assert_eq!(result.try_connect.len(), 1, "leader node must be queued for try_connect");
        assert_eq!(result.try_connect[0].id(), leader_1.id());

        // Install API versions for the leader. The next call to
        // `validate_positions_if_needed` should now build the request.
        api_versions.update(leader_1.id_string(), crate::NodeApiVersions::create());
        mgr.validate_positions_if_needed(0).expect("ok");
        let result = RequestManager::poll(&mut mgr, 0);
        assert_eq!(
            result.unsent_requests.len(),
            1,
            "OffsetsForLeaderEpoch request must be enqueued"
        );
        assert!(result.try_connect.is_empty(), "try_connect queue must be empty after recovery");
    }

    /// Behavior-equivalent of Java's
    /// `testResetPositionsSendNoRequestIfNoPartitionsNeedingReset` — no
    /// partition needs reset, so no requests are enqueued.
    ///
    /// Java uses a Mockito stub on `subscriptionState.partitionsNeedingReset`
    /// to return an empty set; the Rust translation uses the real
    /// `SubscriptionState` and gets the same outcome by not assigning
    /// any partitions awaiting reset.
    #[tokio::test]
    async fn reset_positions_send_no_request_if_no_partitions_needing_reset() {
        let mut mgr = new_manager();
        // No partitions are assigned, so partitions_needing_reset returns an empty set.
        mgr.reset_positions_if_needed(0).expect("ok");
        assert_eq!(RequestManager::poll(&mut mgr, 0).unsent_requests.len(), 0);
    }

    /// Verifies `signal_close` flips the `closing` flag so subsequent
    /// `poll` calls can choose to short-circuit. Java does not have a
    /// direct test for this — it's exercised implicitly through the bg
    /// task shutdown path — but pinning the behaviour here protects the
    /// translation contract.
    #[tokio::test]
    async fn signal_close_sets_closing_flag() {
        let mut mgr = new_manager();
        assert!(!mgr.closing);
        RequestManager::signal_close(&mut mgr);
        assert!(mgr.closing);
    }

    // -----------------------------------------------------------------
    //   init_with_committed_offsets_if_needed (relocated from
    //   CommitRequestManager — Phase 10 commit 3a/N)
    // -----------------------------------------------------------------

    use crate::consumer::OffsetAndMetadata;
    use crate::consumer::internals::commit_request_manager::CommitRequestManager;
    use crate::metadata::LeaderAndEpoch;

    /// Build an `OffsetsRequestManager` plus a backing
    /// `CommitRequestManager` so the init-with-committed path can be
    /// exercised end-to-end (the spawned driver task expects a real
    /// commit manager to fulfil the fetch oneshot).
    fn new_manager_with_commit() -> (OffsetsRequestManager, Arc<CommitRequestManager>, Arc<Mutex<SubscriptionState>>) {
        let config = ConsumerConfig::from_properties(&std::collections::HashMap::from([
            ("bootstrap.servers".to_string(), "localhost:9092".to_string()),
            ("group.id".to_string(), "g".to_string()),
        ]))
        .expect("config");
        let subscription_state = Arc::new(Mutex::new(SubscriptionState::new(
            crate::consumer::internals::auto_offset_reset_strategy::AutoOffsetResetStrategy::EARLIEST,
        )));
        let metadata = Arc::new(ConsumerMetadata::from_config(
            &config,
            subscription_state.clone(),
            ClusterResourceListeners::new(),
        ));
        let commit_rm = Arc::new(CommitRequestManager::new(
            &config,
            metadata.clone(),
            subscription_state.clone(),
            "g",
            None,
            Arc::new(crate::common::metrics::time::SystemTime),
            0,
        ));
        let mgr = OffsetsRequestManager::new(
            subscription_state.clone(),
            metadata,
            IsolationLevel::ReadUncommitted,
            100,
            30_000,
            60_000,
            Arc::new(ApiVersions::new()),
            Some(commit_rm.clone()),
        );
        (mgr, commit_rm, subscription_state)
    }

    /// Java parity:
    /// `OffsetsRequestManager.initWithCommittedOffsetsIfNeeded` returns
    /// `CompletableFuture.completedFuture(null)` when the
    /// initializing-partition set is empty.
    #[tokio::test(flavor = "current_thread")]
    async fn init_with_committed_offsets_empty_partitions_resolves_immediately() {
        let mgr = new_manager();
        let rx = mgr.init_with_committed_offsets_if_needed(HashSet::new(), i64::MAX, 0);
        let result = rx.await.expect("oneshot");
        assert!(result.is_ok());
    }

    /// Java models the commit manager as a constructor-required
    /// argument, but the
    /// `initWithCommittedOffsetsIfNeeded` chain effectively short-circuits
    /// when there is nothing to fetch from. The Rust translation collapses
    /// "no group" into `commit_request_manager = None`, and the method
    /// resolves immediately with `Ok(())`.
    #[tokio::test(flavor = "current_thread")]
    async fn init_with_committed_offsets_no_commit_manager_resolves_immediately() {
        let mgr = new_manager(); // no commit manager
        let mut partitions = HashSet::new();
        partitions.insert(TopicPartition::new("t".to_string(), 0));
        let rx = mgr.init_with_committed_offsets_if_needed(partitions, i64::MAX, 0);
        let result = rx.await.expect("oneshot");
        assert!(result.is_ok());
    }

    /// First call with non-empty initializing partitions issues an
    /// `OffsetFetch` request on the commit manager (visible as an entry
    /// on the commit manager's `unsent_offset_fetches` queue).
    #[tokio::test(flavor = "current_thread")]
    async fn init_with_committed_offsets_enqueues_fetch_offsets_request() {
        let (mgr, commit_rm, _) = new_manager_with_commit();
        let mut partitions = HashSet::new();
        partitions.insert(TopicPartition::new("t".to_string(), 0));
        let _rx = mgr.init_with_committed_offsets_if_needed(partitions.clone(), i64::MAX, 0);
        // The commit manager's pending queue should have one fetch
        // request enqueued by the spawned chain.
        let guard = commit_rm.inner_state_for_test();
        assert_eq!(guard, 1, "fetch_offsets should have enqueued one OffsetFetch request");
    }

    /// Java parity:
    /// `OffsetsRequestManager.canReusePendingOffsetFetchEvent` returns
    /// true when the partition set matches an in-flight fetch. A second
    /// call with the same partitions must NOT issue another
    /// `OffsetFetch` — both callers wait on the same underlying request.
    #[tokio::test(flavor = "current_thread")]
    async fn init_with_committed_offsets_reuses_pending_fetch() {
        let (mgr, commit_rm, _) = new_manager_with_commit();
        let mut partitions = HashSet::new();
        partitions.insert(TopicPartition::new("t".to_string(), 0));

        let _rx1 = mgr.init_with_committed_offsets_if_needed(partitions.clone(), i64::MAX, 0);
        let _rx2 = mgr.init_with_committed_offsets_if_needed(partitions.clone(), i64::MAX, 0);

        // Only one OffsetFetch should have been enqueued — the second
        // call piggy-backs on the pending event.
        let guard = commit_rm.inner_state_for_test();
        assert_eq!(
            guard, 1,
            "second call should reuse the pending OffsetFetch, not issue a new one"
        );
    }

    /// Java parity: a second call with a DIFFERENT partition set bypasses
    /// the reuse path (`canReusePendingOffsetFetchEvent` returns false)
    /// and issues a new `OffsetFetch`. Note that Java's exact behavior
    /// here is to overwrite the pending event slot; the new request
    /// replaces the old one. We accept the same: the new fetch is
    /// enqueued.
    #[tokio::test(flavor = "current_thread")]
    async fn init_with_committed_offsets_does_not_reuse_for_different_partitions() {
        let (mgr, commit_rm, _) = new_manager_with_commit();
        let mut partitions1 = HashSet::new();
        partitions1.insert(TopicPartition::new("t".to_string(), 0));
        let mut partitions2 = HashSet::new();
        partitions2.insert(TopicPartition::new("t".to_string(), 1));

        let _rx1 = mgr.init_with_committed_offsets_if_needed(partitions1, i64::MAX, 0);
        let _rx2 = mgr.init_with_committed_offsets_if_needed(partitions2, i64::MAX, 0);

        let guard = commit_rm.inner_state_for_test();
        assert_eq!(guard, 2, "different partition sets should each issue their own OffsetFetch");
    }

    /// Java parity:
    /// `ConsumerUtils.refreshCommittedOffsets` — for each entry in the
    /// offsets map that maps to a currently-assigned, currently-initializing
    /// partition, set the position via `seek_unvalidated`. The Rust
    /// helper is a free function so we can drive it independently of the
    /// in-flight fetch.
    #[tokio::test(flavor = "current_thread")]
    async fn refresh_offsets_seeks_unvalidated_for_initializing_assigned_partitions() {
        let (_mgr, _commit_rm, subscription_state) = new_manager_with_commit();
        let tp = TopicPartition::new("t".to_string(), 0);

        // Assign tp without a position so it is "initializing".
        {
            let mut subs = subscription_state.lock().unwrap();
            let mut set = HashSet::new();
            set.insert(tp.clone());
            subs.assign_from_user(set).expect("assign");
            assert!(subs.initializing_partitions().contains(&tp));
            assert!(subs.is_assigned(&tp));
        }

        let metadata = Arc::new(ConsumerMetadata::from_config(
            &ConsumerConfig::from_properties(&std::collections::HashMap::from([
                ("bootstrap.servers".to_string(), "localhost:9092".to_string()),
                ("group.id".to_string(), "g".to_string()),
            ]))
            .expect("config"),
            subscription_state.clone(),
            ClusterResourceListeners::new(),
        ));

        let mut offsets: HashMap<TopicPartition, Option<OffsetAndMetadata>> = HashMap::new();
        offsets.insert(tp.clone(), Some(OffsetAndMetadata::new(10).expect("offset metadata")));

        super::refresh_offsets(&offsets, subscription_state.as_ref(), metadata.as_ref());

        // The position should now be set to offset 10.
        let subs = subscription_state.lock().unwrap();
        let position = subs.position(&tp).expect("position lookup").expect("position present");
        assert_eq!(position.offset, 10);
    }

    /// Java parity:
    /// `OffsetsRequestManager.offsetsForInitializingPartitions` — filter
    /// out offsets for partitions that are no longer initializing (e.g.
    /// the user called `seek` manually between request and response).
    #[tokio::test(flavor = "current_thread")]
    async fn refresh_offsets_skips_partitions_no_longer_initializing() {
        let (_mgr, _commit_rm, subscription_state) = new_manager_with_commit();
        let tp = TopicPartition::new("t".to_string(), 0);

        // Assign tp AND give it a manual position — it is no longer
        // initializing.
        {
            let mut subs = subscription_state.lock().unwrap();
            let mut set = HashSet::new();
            set.insert(tp.clone());
            subs.assign_from_user(set).expect("assign");
            let pos = FetchPosition::with_leader(99, None, LeaderAndEpoch::no_leader_or_epoch());
            subs.seek_unvalidated(&tp, pos).expect("seek");
            assert!(!subs.initializing_partitions().contains(&tp));
        }

        let metadata = Arc::new(ConsumerMetadata::from_config(
            &ConsumerConfig::from_properties(&std::collections::HashMap::from([
                ("bootstrap.servers".to_string(), "localhost:9092".to_string()),
                ("group.id".to_string(), "g".to_string()),
            ]))
            .expect("config"),
            subscription_state.clone(),
            ClusterResourceListeners::new(),
        ));

        let mut offsets: HashMap<TopicPartition, Option<OffsetAndMetadata>> = HashMap::new();
        offsets.insert(tp.clone(), Some(OffsetAndMetadata::new(10).expect("offset metadata")));
        super::refresh_offsets(&offsets, subscription_state.as_ref(), metadata.as_ref());

        // The position should NOT have been overwritten to 10.
        let subs = subscription_state.lock().unwrap();
        let position = subs.position(&tp).expect("position lookup").expect("position present");
        assert_eq!(position.offset, 99);
    }

    // -----------------------------------------------------------------
    //   update_fetch_positions
    //   (Java: OffsetsRequestManager.updateFetchPositions)
    // -----------------------------------------------------------------

    /// Helper: yield several times to give spawned futures a chance to
    /// run. The followup driver awaits the inner `oneshot::Receiver`,
    /// then performs synchronous state-mutation work and completes the
    /// outer sender; on a current-thread runtime a single yield is not
    /// always enough.
    async fn yield_until<F: Fn() -> bool>(predicate: F) {
        for _ in 0..16 {
            if predicate() {
                return;
            }
            tokio::task::yield_now().await;
        }
    }

    /// Java parity: with no initializing partitions, the result resolves
    /// immediately to `Ok(())` because `has_all_fetch_positions()` is
    /// true at entry.
    #[tokio::test(flavor = "current_thread")]
    async fn update_fetch_positions_with_no_initializing_partitions_resolves_immediately() {
        let mut mgr = new_manager();
        let rx = mgr.update_fetch_positions(i64::MAX, 0);
        let result = rx.await.expect("oneshot");
        assert!(result.is_ok());
    }

    /// Java parity: `OffsetsRequestManager.updateFetchPositions` with the
    /// fast-path branch `hasAllFetchPositions == true` does not call
    /// `commitRequestManager.fetchOffsets`.
    #[tokio::test(flavor = "current_thread")]
    async fn update_fetch_positions_with_all_positions_does_not_issue_fetch() {
        let (mut mgr, commit_rm, subscription_state) = new_manager_with_commit();
        let tp = TopicPartition::new("topic1".to_string(), 1);

        // Assign and seek manually so the partition has a position.
        {
            let mut subs = subscription_state.lock().unwrap();
            let mut set = HashSet::new();
            set.insert(tp.clone());
            subs.assign_from_user(set).expect("assign");
            let pos = FetchPosition::with_leader(42, None, LeaderAndEpoch::no_leader_or_epoch());
            subs.seek_unvalidated(&tp, pos).expect("seek");
            assert!(subs.has_all_fetch_positions());
        }

        let rx = mgr.update_fetch_positions(i64::MAX, 0);
        let result = rx.await.expect("oneshot");
        assert!(result.is_ok());

        // No OffsetFetch should have been issued.
        assert_eq!(commit_rm.inner_state_for_test(), 0);
    }

    /// Java parity: `testUpdatePositionsWithCommittedOffsets` (request
    /// issuance half).
    ///
    /// `update_fetch_positions` triggered with a single initializing
    /// partition must enqueue exactly one `OffsetFetch` request through
    /// the commit manager.
    #[tokio::test(flavor = "current_thread")]
    async fn update_fetch_positions_with_committed_offsets_enqueues_offset_fetch() {
        let (mut mgr, commit_rm, subscription_state) = new_manager_with_commit();
        let tp = TopicPartition::new("topic1".to_string(), 1);

        {
            let mut subs = subscription_state.lock().unwrap();
            let mut set = HashSet::new();
            set.insert(tp.clone());
            subs.assign_from_user(set).expect("assign");
            assert!(!subs.has_all_fetch_positions());
        }

        let _rx = mgr.update_fetch_positions(i64::MAX, 0);
        assert_eq!(
            commit_rm.inner_state_for_test(),
            1,
            "update_fetch_positions should enqueue exactly one OffsetFetch"
        );
    }

    /// Java parity: `testUpdatePositionsWithCommittedOffsets` (response
    /// half). After the OffsetFetch resolves with committed offsets, the
    /// outer `update_fetch_positions` result is `Ok(())` and the
    /// position has been seeked-unvalidated to the committed offset.
    #[tokio::test(flavor = "current_thread")]
    async fn update_fetch_positions_with_committed_offsets_applies_position_on_response() {
        let (mut mgr, commit_rm, subscription_state) = new_manager_with_commit();
        let tp = TopicPartition::new("topic1".to_string(), 1);

        {
            let mut subs = subscription_state.lock().unwrap();
            let mut set = HashSet::new();
            set.insert(tp.clone());
            subs.assign_from_user(set).expect("assign");
        }

        let rx = mgr.update_fetch_positions(i64::MAX, 0);

        // Drive the inner OffsetFetch to completion with committed offset = 10.
        let mut offsets: HashMap<TopicPartition, Option<OffsetAndMetadata>> = HashMap::new();
        offsets.insert(tp.clone(), Some(OffsetAndMetadata::new(10).expect("offset metadata")));
        assert!(
            commit_rm.complete_first_unsent_fetch_for_test(offsets),
            "expected an unsent OffsetFetch to complete"
        );

        let result = rx.await.expect("oneshot");
        assert!(result.is_ok());

        let subs = subscription_state.lock().unwrap();
        let position = subs.position(&tp).expect("position lookup").expect("position present");
        assert_eq!(position.offset, 10);
    }

    /// Java parity: `testUpdatePositionsWithCommittedOffsetsReusesRequest`.
    ///
    /// Two `update_fetch_positions` calls with the same initializing
    /// partition set must reuse the in-flight `OffsetFetch` — only one
    /// fetch request appears on the commit manager's pending queue, and
    /// both outer receivers resolve when the underlying fetch
    /// completes.
    #[tokio::test(flavor = "current_thread")]
    async fn update_fetch_positions_reuses_pending_request_for_same_partitions() {
        let (mut mgr, commit_rm, subscription_state) = new_manager_with_commit();
        let tp = TopicPartition::new("topic1".to_string(), 1);

        {
            let mut subs = subscription_state.lock().unwrap();
            let mut set = HashSet::new();
            set.insert(tp.clone());
            subs.assign_from_user(set).expect("assign");
        }

        let rx1 = mgr.update_fetch_positions(i64::MAX, 0);
        let rx2 = mgr.update_fetch_positions(i64::MAX, 0);

        // Only one OffsetFetch should have been enqueued — the second
        // call piggy-backs on the pending event (mirrors
        // `canReusePendingOffsetFetchEvent`).
        assert_eq!(
            commit_rm.inner_state_for_test(),
            1,
            "second update_fetch_positions call must reuse the pending OffsetFetch"
        );

        // Complete the single in-flight fetch and verify both outer
        // receivers resolve.
        let mut offsets: HashMap<TopicPartition, Option<OffsetAndMetadata>> = HashMap::new();
        offsets.insert(tp.clone(), Some(OffsetAndMetadata::new(10).expect("offset metadata")));
        assert!(commit_rm.complete_first_unsent_fetch_for_test(offsets));

        assert!(rx1.await.expect("rx1").is_ok());
        assert!(rx2.await.expect("rx2").is_ok());

        let subs = subscription_state.lock().unwrap();
        let position = subs.position(&tp).expect("position lookup").expect("position present");
        assert_eq!(position.offset, 10);
    }

    /// Regression test: a committed-offset fetch superseded by a later request
    /// for a *different* partition set must complete its orphaned waiter with a
    /// `Timeout`, never a fabricated `NetworkException`.
    ///
    /// Java never reaches this state — each caller's `result` future is
    /// completed by a `whenComplete` chain hung off
    /// `commitRequestManager.fetchOffsets(...)`, independent of the
    /// `pendingOffsetFetchEvent` field, so replacing that field cannot abandon
    /// an earlier caller; it resolves with success or `TimeoutException`. Rust's
    /// single-consumer `oneshot` keeps the waiters inside the replaceable slot,
    /// so we must synthesise a completion — and it has to be a `Timeout`,
    /// because `is_ignorable_async_poll_error` swallows only `Timeout` (exactly
    /// as Java swallows only `TimeoutException`). Using a retriable wire error
    /// here previously escaped that predicate and surfaced from `poll()` as a
    /// spurious `NetworkException` during rebalances.
    #[tokio::test(flavor = "current_thread")]
    async fn superseded_committed_offset_fetch_completes_with_timeout_not_network_error() {
        let (mut mgr, commit_rm, subscription_state) = new_manager_with_commit();
        let tp1 = TopicPartition::new("topic1".to_string(), 1);
        let tp2 = TopicPartition::new("topic2".to_string(), 2);

        {
            let mut subs = subscription_state.lock().unwrap();
            subs.assign_from_user(HashSet::from([tp1.clone()])).expect("assign");
        }

        let rx1 = mgr.update_fetch_positions(i64::MAX, 0);
        assert_eq!(commit_rm.inner_state_for_test(), 1);

        // Expand the assignment so the initializing set differs. The next call
        // cannot reuse the pending event, so it replaces the slot — dropping
        // the first call's waiter.
        {
            let mut subs = subscription_state.lock().unwrap();
            subs.assign_from_user(HashSet::from([tp1.clone(), tp2.clone()]))
                .expect("reassign");
        }

        let _rx2 = mgr.update_fetch_positions(i64::MAX, 0);
        assert_eq!(
            commit_rm.inner_state_for_test(),
            2,
            "a differing initializing set must issue a new OffsetFetch, superseding the first"
        );

        // The orphaned caller must still be completed (CLAUDE.md §5 — never
        // leave the future hanging) and with an ignorable timeout.
        let err = rx1
            .await
            .expect("superseded caller must be completed, not left hanging")
            .expect_err("a superseded committed-offset fetch cannot report success");
        assert!(
            matches!(err, KafkaError::Timeout(_)),
            "superseded fetch must yield an ignorable Timeout (Java's TimeoutException), got {err:?}"
        );
        assert_eq!(
            err.to_string(),
            "TimeoutError: Committed-offset fetch was superseded before it completed"
        );
    }

    /// Java parity:
    /// `testUpdatePositionsDoesNotApplyOffsetsIfPartitionNotInitializingAnymore`.
    ///
    /// If a partition was initializing when the OffsetFetch was issued
    /// but gets a manual position via `seek` before the response
    /// arrives, the committed offset MUST NOT overwrite that position.
    #[tokio::test(flavor = "current_thread")]
    async fn update_fetch_positions_does_not_apply_offsets_if_partition_no_longer_initializing() {
        let (mut mgr, commit_rm, subscription_state) = new_manager_with_commit();
        let tp = TopicPartition::new("topic1".to_string(), 1);

        {
            let mut subs = subscription_state.lock().unwrap();
            let mut set = HashSet::new();
            set.insert(tp.clone());
            subs.assign_from_user(set).expect("assign");
        }

        let _rx = mgr.update_fetch_positions(i64::MAX, 0);
        assert_eq!(commit_rm.inner_state_for_test(), 1);

        // Between request and response, seek manually — the partition is
        // no longer initializing.
        {
            let mut subs = subscription_state.lock().unwrap();
            let pos = FetchPosition::with_leader(99, None, LeaderAndEpoch::no_leader_or_epoch());
            subs.seek_unvalidated(&tp, pos).expect("seek");
            assert!(!subs.initializing_partitions().contains(&tp));
        }

        // Now complete the fetch with a different offset.
        let mut offsets: HashMap<TopicPartition, Option<OffsetAndMetadata>> = HashMap::new();
        offsets.insert(tp.clone(), Some(OffsetAndMetadata::new(5).expect("offset metadata")));
        assert!(commit_rm.complete_first_unsent_fetch_for_test(offsets));

        // Give the spawned followup a chance to run.
        let subs_for_check = subscription_state.clone();
        let tp_for_check = tp.clone();
        yield_until(|| {
            let subs = subs_for_check.lock().unwrap();
            subs.position(&tp_for_check)
                .ok()
                .flatten()
                .map(|p| p.offset == 99)
                .unwrap_or(false)
        })
        .await;

        let subs = subscription_state.lock().unwrap();
        let position = subs.position(&tp).expect("position lookup").expect("position present");
        // The manual seek must NOT have been overwritten by the committed offset.
        assert_eq!(position.offset, 99);
    }

    /// Java parity:
    /// `testUpdatePositionsDoesNotResetPositionBeforeRetrievingOffsetsForNewlyAddedPartition`.
    ///
    /// `update_fetch_positions` captures the initializing-partition set
    /// at call time. If a NEW partition is added to the assignment
    /// between the OffsetFetch dispatch and its response, the
    /// `reset_initializing_positions` step in the followup MUST NOT
    /// include that newly added partition — its filter is restricted to
    /// the captured set.
    #[tokio::test(flavor = "current_thread")]
    async fn update_fetch_positions_does_not_reset_partitions_added_mid_flight() {
        let (mut mgr, commit_rm, subscription_state) = new_manager_with_commit();
        let tp1 = TopicPartition::new("topic1".to_string(), 1);
        let tp2 = TopicPartition::new("topic2".to_string(), 2);

        // Assign tp1 initially.
        {
            let mut subs = subscription_state.lock().unwrap();
            let mut set = HashSet::new();
            set.insert(tp1.clone());
            subs.assign_from_user(set).expect("assign");
        }

        let _rx = mgr.update_fetch_positions(i64::MAX, 0);
        assert_eq!(commit_rm.inner_state_for_test(), 1);

        // Now add tp2 to the assignment while the OffsetFetch is still
        // in flight.
        {
            let mut subs = subscription_state.lock().unwrap();
            let mut set = HashSet::new();
            set.insert(tp1.clone());
            set.insert(tp2.clone());
            subs.assign_from_user(set).expect("assign");
        }

        // Complete the fetch returning a committed offset for tp1 (Java
        // mock returns `Map.of(tp1, ...)`).
        let mut offsets: HashMap<TopicPartition, Option<OffsetAndMetadata>> = HashMap::new();
        offsets.insert(tp1.clone(), Some(OffsetAndMetadata::new(10).expect("offset metadata")));
        assert!(commit_rm.complete_first_unsent_fetch_for_test(offsets));

        // Wait for the followup to apply the committed offset to tp1.
        let subs_for_check = subscription_state.clone();
        let tp1_for_check = tp1.clone();
        yield_until(|| {
            let subs = subs_for_check.lock().unwrap();
            subs.position(&tp1_for_check)
                .ok()
                .flatten()
                .map(|p| p.offset == 10)
                .unwrap_or(false)
        })
        .await;

        let subs = subscription_state.lock().unwrap();
        // tp1: position applied via seek_unvalidated to 10.
        let position1 = subs.position(&tp1).expect("position lookup").expect("position present");
        assert_eq!(position1.offset, 10);
        // tp2: still initializing — was NOT marked AWAIT_RESET (filter
        // excluded it). It would not have a position yet AND it would
        // still be in `initializing_partitions`.
        assert!(
            subs.initializing_partitions().contains(&tp2),
            "tp2 added mid-flight must NOT have been marked AWAIT_RESET"
        );
    }

    /// Java parity (no-group branch): when `commit_request_manager` is
    /// `None`, `update_fetch_positions` runs
    /// `initWithPartitionOffsetsIfNeeded` inline. With
    /// `AutoOffsetResetStrategy::EARLIEST`, the initializing partition
    /// is marked for AWAIT_RESET — verifiable on the subscription state.
    #[tokio::test(flavor = "current_thread")]
    async fn update_fetch_positions_without_commit_manager_marks_partitions_for_reset() {
        let mut mgr = new_manager(); // No commit manager
        let tp = TopicPartition::new("topic1".to_string(), 1);

        // Note: `new_manager`'s SubscriptionState is initialized with
        // AutoOffsetResetStrategy::EARLIEST, so reset_initializing_positions
        // marks for AWAIT_RESET (does not raise NoOffsetForPartition).
        {
            let mut subs = mgr.shared.subscription_state.lock().unwrap();
            let mut set = HashSet::new();
            set.insert(tp.clone());
            subs.assign_from_user(set).expect("assign");
            assert!(subs.initializing_partitions().contains(&tp));
        }

        let rx = mgr.update_fetch_positions(i64::MAX, 0);
        let result = rx.await.expect("oneshot");
        assert!(result.is_ok());

        // After the call, the partition is no longer "initializing" —
        // it's now in AWAIT_RESET state (marked by
        // `request_offset_reset_default`).
        let subs = mgr.shared.subscription_state.lock().unwrap();
        assert!(
            !subs.initializing_partitions().contains(&tp),
            "tp must have been moved out of initializing into AWAIT_RESET"
        );
    }

    /// Regression for COMMENTS R2-3: `update_fetch_positions` MUST NOT
    /// cache synchronous errors thrown from `validate_positions_if_needed`.
    /// Java's outer `catch (Exception e)` in `updateFetchPositions`
    /// (`OffsetsRequestManager.java:260-262`) only calls
    /// `result.completeExceptionally(...)`; the
    /// `cacheExceptionIfEventExpired` hook is registered ONLY inside the
    /// committed-offset path's `whenComplete`. Caching here would
    /// produce double-delivery: the caller sees the error THIS call,
    /// then the next `update_fetch_positions` call surfaces the same
    /// cached error.
    ///
    /// Setup: pre-seed a `LogTruncationError` in
    /// `cached_validate_positions_exception` (Java path:
    /// `OffsetsForLeaderEpoch` response set it). Call
    /// `update_fetch_positions` with `current_time_ms >= deadline_ms`
    /// (the Java "event expired" condition that would trigger caching
    /// IF the bug were present). The Err must propagate to the caller,
    /// and `cached_update_positions_exception` MUST be empty afterwards.
    #[tokio::test(flavor = "current_thread")]
    async fn update_fetch_positions_does_not_cache_synchronous_validate_errors() {
        let mut mgr = new_manager();

        // Pre-seed a validate error (Java's path:
        // `OffsetsForLeaderEpoch` response set it via
        // `cachedValidatePositionsException.set(error)`).
        let seeded_err = KafkaError::new(crate::common::protocol::Errors::UnknownServerError);
        mgr.shared.offset_fetcher_utils.maybe_set_validate_error(seeded_err.clone());

        // current_time_ms == deadline_ms triggers the would-be cache
        // condition (`now_ms >= deadline_ms`). Java's outer catch does
        // NOT cache; Rust must match.
        let deadline_ms = 100;
        let current_time_ms = 100;
        let rx = mgr.update_fetch_positions(deadline_ms, current_time_ms);
        let result = rx.await.expect("oneshot");

        match result {
            Err(err) => {
                assert_eq!(
                    err.error().to_string(),
                    seeded_err.error().to_string(),
                    "validate error must propagate to caller this call",
                );
            },
            Ok(()) => panic!("expected validate error to be surfaced"),
        }

        // The cache MUST be empty — Java does not cache from the outer
        // catch. The bug fix removes the
        // `maybe_cache_update_positions_exception` call from the sync
        // error path.
        let guard = mgr.cached_update_positions_exception.lock().unwrap();
        assert!(
            guard.is_none(),
            "synchronous validate error must NOT be cached (Java's outer catch does not cache)",
        );
    }

    /// Java parity (`maybeCompleteWithPreviousException`): a cached
    /// `update_fetch_positions` error from a previous expired event is
    /// surfaced on the next call (and cleared atomically).
    #[tokio::test(flavor = "current_thread")]
    async fn update_fetch_positions_surfaces_cached_previous_exception() {
        let mut mgr = new_manager();

        // Seed a cached error directly (this is what
        // `cacheExceptionIfEventExpired` does in Java when an expired event
        // surfaces an error).
        let cached_err = KafkaError::new(crate::common::protocol::Errors::TopicAuthorizationFailed);
        {
            let mut guard = mgr.cached_update_positions_exception.lock().unwrap();
            *guard = Some(cached_err.clone());
        }

        let rx = mgr.update_fetch_positions(i64::MAX, 0);
        let result = rx.await.expect("oneshot");

        match result {
            Err(err) => {
                // Java propagates the exact cached exception; Rust does
                // the same with the cloned `KafkaError`. Confirm the
                // error type matches.
                assert_eq!(
                    err.error().to_string(),
                    cached_err.error().to_string(),
                    "cached error should be surfaced on the next call"
                );
            },
            Ok(()) => panic!("expected cached error to be surfaced"),
        }

        // The cache must have been cleared.
        let guard = mgr.cached_update_positions_exception.lock().unwrap();
        assert!(guard.is_none(), "cache should be cleared after consumption");
    }

    // -----------------------------------------------------------------
    //   fetch_offsets + OffsetsClusterListener::on_update
    //   (Java: OffsetsRequestManager.fetchOffsets + onUpdate)
    // -----------------------------------------------------------------

    use crate::client_response::ClientResponse;
    use crate::common::protocol::Errors;
    use crate::common::protocol::api_keys::ApiKeys;
    use crate::common::requests::list_offsets_request::EARLIEST_TIMESTAMP;
    use crate::common::requests::list_offsets_response::{
        ListOffsetsResponse, UNKNOWN_EPOCH, UNKNOWN_OFFSET, UNKNOWN_TIMESTAMP,
    };
    use crate::common::requests::request_test_utils;
    use crate::common::requests::{ConcreteResponse, MetadataResponse, RequestHeader};
    use crate::list_offsets_response_data::{
        ListOffsetsPartitionResponse, ListOffsetsResponseData, ListOffsetsTopicResponse,
    };
    use std::time::Duration;

    /// Bootstrap `metadata` with a single topic / one-partition layout
    /// using the named broker as the leader. Mirrors the per-test setup
    /// done by Java's `mockSuccessfulRequest` via mocks.
    ///
    /// `topic` is added to the transient-topics set first so the retain
    /// filter keeps the topic in the cluster snapshot — without this, the
    /// `ConsumerMetadata::retain_topic_fn` would drop unsubscribed topics
    /// from the cluster snapshot (mirroring Java's filtering of `Cluster`
    /// to the consumer's subscribed set).
    fn bootstrap_metadata_with_topic(
        metadata: &ConsumerMetadata,
        topic: &str,
        num_partitions: i32,
    ) -> MetadataResponse {
        metadata.add_transient_topics(HashSet::from([topic.to_string()]));
        let mut counts = HashMap::new();
        counts.insert(topic.to_string(), num_partitions);
        let response = request_test_utils::metadata_update_with(1, &counts);
        metadata.metadata_arc().update_with_current_request_version(&response, false, 0);
        response
    }

    /// Build a synthesised `ClientResponse` carrying the given
    /// `ListOffsetsResponse` so the test can drive `unsent.handler().on_complete(...)`
    /// to resolve the request's response receiver — mirrors the Java test
    /// helper `buildClientResponse`.
    fn build_list_offsets_client_response(response: ListOffsetsResponse) -> ClientResponse {
        let header =
            RequestHeader::new(&ApiKeys::LIST_OFFSETS, ApiKeys::LIST_OFFSETS.latest_version(), "", 1).expect("header");
        ClientResponse::with_timeout(
            header,
            None,
            "0",
            0,
            0,
            false,
            false,
            None,
            None,
            Some(ConcreteResponse::ListOffsets(response)),
        )
    }

    /// Build a synthesised disconnect-style `ClientResponse` so the test
    /// can drive a transport-level failure into the request handler.
    fn build_disconnected_client_response() -> ClientResponse {
        let header =
            RequestHeader::new(&ApiKeys::LIST_OFFSETS, ApiKeys::LIST_OFFSETS.latest_version(), "", 1).expect("header");
        ClientResponse::with_timeout(
            header,
            None,
            "0",
            0,
            0,
            true,
            false,
            None,
            Some("auth failed".to_string()),
            None,
        )
    }

    /// Build a synthesised pure-disconnect `ClientResponse` (no auth /
    /// version-mismatch annotation), so the handler maps it to a
    /// `NetworkException` (Java's transport-level disconnect) rather than
    /// the SASL-authentication failure path.
    fn build_network_disconnect_client_response() -> ClientResponse {
        let header =
            RequestHeader::new(&ApiKeys::LIST_OFFSETS, ApiKeys::LIST_OFFSETS.latest_version(), "", 1).expect("header");
        ClientResponse::with_timeout(header, None, "0", 0, 0, true, false, None, None, None)
    }

    /// Build a single-topic, multi-partition `ListOffsetsResponse` from a
    /// map of `partition -> (error, timestamp, offset, leader_epoch)`.
    fn build_list_offsets_response(topic: &str, partitions: Vec<(i32, Errors, i64, i64, i32)>) -> ListOffsetsResponse {
        let mut topic_response = ListOffsetsTopicResponse::new();
        topic_response.set_name(topic.to_string());
        let mut parts = Vec::new();
        for (idx, error, timestamp, offset, leader_epoch) in partitions {
            let mut p = ListOffsetsPartitionResponse::new();
            p.set_partition_index(idx);
            p.set_error_code(error.code());
            p.set_timestamp(timestamp);
            p.set_offset(offset);
            p.set_leader_epoch(leader_epoch);
            parts.push(p);
        }
        topic_response.set_partitions(parts);
        let mut data = ListOffsetsResponseData::new();
        data.set_topics(vec![topic_response]);
        ListOffsetsResponse::new(data)
    }

    /// Bootstrap `metadata` with a single topic spread across `num_nodes`
    /// brokers. `metadata_update_with` assigns each partition's leader as
    /// `nodes[partition_index % num_nodes]`, so with `num_nodes == 2`
    /// partition 1 → node 1 and partition 2 → node 0 — distinct leaders,
    /// mirroring the Java fixture's `LEADER_1` / `LEADER_2` two-broker
    /// layout used by the multi-partition / partial-failure tests.
    fn bootstrap_metadata_with_nodes(
        metadata: &ConsumerMetadata,
        topic: &str,
        num_partitions: i32,
        num_nodes: i32,
    ) -> MetadataResponse {
        metadata.add_transient_topics(HashSet::from([topic.to_string()]));
        let mut counts = HashMap::new();
        counts.insert(topic.to_string(), num_partitions);
        let response = request_test_utils::metadata_update_with(num_nodes, &counts);
        metadata.metadata_arc().update_with_current_request_version(&response, false, 0);
        response
    }

    /// Bootstrap `metadata` with several topics in one update, each with its
    /// own partition count, spread across `num_nodes` brokers. Used by the
    /// build-time-partial-park test (`testGetOffsetsForTimesWhenSomeTopic`
    /// `PartitionLeadersNotKnownInitially`) where the first refresh knows
    /// only a subset of the requested topics and a later refresh adds the
    /// rest. Every named topic is registered as transient so the consumer's
    /// `retain_topic_fn` keeps it in the cluster snapshot.
    fn bootstrap_metadata_multi_topic(
        metadata: &ConsumerMetadata,
        topic_partition_counts: &[(&str, i32)],
        num_nodes: i32,
    ) -> MetadataResponse {
        let topics: HashSet<String> = topic_partition_counts.iter().map(|(t, _)| (*t).to_string()).collect();
        metadata.add_transient_topics(topics);
        let mut counts = HashMap::new();
        for (topic, num_partitions) in topic_partition_counts {
            counts.insert((*topic).to_string(), *num_partitions);
        }
        let response = request_test_utils::metadata_update_with(num_nodes, &counts);
        metadata.metadata_arc().update_with_current_request_version(&response, false, 0);
        response
    }

    /// Drain EVERY unsent `ListOffsets` request from one `poll()` and
    /// complete each with a response built from `per_partition`
    /// (`partition_index -> (error, timestamp, offset, leader_epoch)`),
    /// restricted to the partitions that request actually carried.
    ///
    /// The fetch-path response handler
    /// (`PendingCompletion::ListOffsetsForFetchOffsets`) routes on the
    /// request's `node_partitions`, not on response keys, so each per-node
    /// request can be answered with only its own partitions. Returns the
    /// number of requests drained — mirrors the Java multi-broker pattern
    /// of completing `res.unsentRequests.get(0)`, `get(1)`, ... in turn.
    async fn complete_all_unsent_with_per_partition_response(
        mgr: &mut OffsetsRequestManager,
        topic: &str,
        per_partition: &HashMap<i32, (Errors, i64, i64, i32)>,
        now_ms: i64,
    ) -> usize {
        let poll_result = RequestManager::poll(mgr, now_ms);
        let mut count = 0;
        for unsent in poll_result.unsent_requests {
            // Determine which partitions this request carried by building
            // it and reading back its target topics. Match Java's
            // per-broker response assembly.
            let built = {
                let mut unsent = unsent;
                let request = unsent.request_builder_mut().expect("builder present").build().expect("build");
                let crate::common::requests::ConcreteRequest::ListOffsets(r) = request else {
                    panic!("expected ListOffsetsRequest");
                };
                // Re-take the handler from the original unsent: rebuild is
                // destructive, so capture the partition indices then drive
                // completion through the handler we still hold.
                let indices: Vec<i32> = r
                    .topics()
                    .iter()
                    .flat_map(|t| t.partitions.iter().map(|p| p.partition_index))
                    .collect();
                (unsent, indices)
            };
            let (unsent, indices) = built;
            let mut parts: Vec<(i32, Errors, i64, i64, i32)> = Vec::new();
            for idx in indices {
                let (error, ts, offset, epoch) = per_partition.get(&idx).copied().unwrap_or((
                    Errors::None,
                    UNKNOWN_TIMESTAMP,
                    UNKNOWN_OFFSET,
                    UNKNOWN_EPOCH,
                ));
                parts.push((idx, error, ts, offset, epoch));
            }
            let response = build_list_offsets_response(topic, parts);
            unsent.handler().on_complete(build_list_offsets_client_response(response));
            count += 1;
        }
        // Let the spawned forwarder tasks enqueue their `PendingCompletion`s.
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        count
    }

    /// Drive the first request on the manager's `requests_to_send` to
    /// completion with the given `ListOffsetsResponse`. Returns whether
    /// a request was actually drained — mirrors the Java pattern of
    /// `poll() → unsentRequest.future().whenComplete(...)`.
    async fn complete_first_unsent_with_response(
        mgr: &mut OffsetsRequestManager,
        response: ListOffsetsResponse,
        now_ms: i64,
    ) -> bool {
        let poll_result = RequestManager::poll(mgr, now_ms);
        let Some(unsent) = poll_result.unsent_requests.into_iter().next() else {
            return false;
        };
        let client_response = build_list_offsets_client_response(response);
        unsent.handler().on_complete(client_response);
        // Allow the spawned forwarder task to enqueue the
        // `PendingCompletion` on the manager's channel.
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        true
    }

    /// Helper: wait until the outer `fetch_offsets` receiver resolves.
    /// Polls the manager between yields so the
    /// `PendingCompletion::ListOffsetsForFetchOffsets` queued by the
    /// spawned forwarder is drained and applied to the request state.
    async fn await_fetch_result(
        mgr: &mut OffsetsRequestManager,
        rx: oneshot::Receiver<Result<HashMap<TopicPartition, Option<OffsetAndTimestampInternal>>, KafkaError>>,
        now_ms: i64,
    ) -> Result<HashMap<TopicPartition, Option<OffsetAndTimestampInternal>>, KafkaError> {
        // Drive the runtime forward to give the forwarder task room to
        // run, then drain pending completions.
        let mut rx = rx;
        for _ in 0..16 {
            tokio::task::yield_now().await;
            // `poll` invokes `drain_pending_completions` which routes
            // `ListOffsetsForFetchOffsets` to the request state.
            let _ = RequestManager::poll(mgr, now_ms);
            if let Ok(result) = tokio::time::timeout(Duration::from_millis(1), &mut rx).await {
                return result.expect("oneshot dropped");
            }
        }
        // Last-chance: await the receiver directly (will hang if the
        // chain didn't complete — making the test failure observable).
        rx.await.expect("oneshot dropped")
    }

    /// Java parity: `testListOffsetsRequestEmpty`. Empty input → empty
    /// result, no requests enqueued.
    #[tokio::test(flavor = "current_thread")]
    async fn fetch_offsets_empty_resolves_immediately() {
        let mut mgr = new_manager();
        let rx = mgr.fetch_offsets(HashMap::new(), false);
        let result = rx.await.expect("oneshot").expect("ok result");
        assert!(result.is_empty(), "empty input should give empty result");
        assert_eq!(mgr.requests_to_send_count(), 0);
        assert_eq!(mgr.requests_to_retry_count(), 0);
    }

    /// Java parity: `testListOffsetsRequest_Success`. Single partition
    /// with a known leader → request enqueued → success response →
    /// outer future resolves with the offset.
    ///
    /// The event payload carries
    /// [`OffsetAndTimestampInternal`](super::offset_and_timestamp_internal::OffsetAndTimestampInternal),
    /// which (matching Java) allows the broker-returned
    /// `timestamp == -1` sentinel for `EARLIEST` / `LATEST` queries.
    /// COMMENTS.DONE.1.md Issue 6 closed a regression where the
    /// translation used the public-class constructor and silently
    /// surfaced these entries as `None`.
    #[tokio::test(flavor = "current_thread")]
    async fn fetch_offsets_success_single_partition() {
        let (mut mgr, _commit_rm, _subs) = new_manager_with_commit();
        bootstrap_metadata_with_topic(&mgr.shared.metadata, "t1", 2);
        let tp = TopicPartition::new("t1".to_string(), 1);
        let mut timestamps = HashMap::new();
        timestamps.insert(tp.clone(), EARLIEST_TIMESTAMP);

        let rx = mgr.fetch_offsets(timestamps, false);
        assert_eq!(mgr.requests_to_send_count(), 1, "exactly one ListOffsets request expected");
        assert_eq!(mgr.requests_to_retry_count(), 0);

        let response = build_list_offsets_response("t1", vec![(1, Errors::None, 100, 5, UNKNOWN_EPOCH)]);
        assert!(complete_first_unsent_with_response(&mut mgr, response, 0).await);

        let result = await_fetch_result(&mut mgr, rx, 0).await.expect("ok");
        let oat = result.get(&tp).expect("entry present").as_ref().expect("non-null offset");
        assert_eq!(oat.offset(), 5);
    }

    /// Regression for COMMENTS R2-4: `fetch_offsets` clears the
    /// transient-topic registration on the global-result completion
    /// path — Java does the same via
    /// `listOffsetsRequestState.globalResult.whenComplete(...
    ///   metadata.clearTransientTopics(); ...)` at
    /// `OffsetsRequestManager.java:200-209`. Success branch.
    #[tokio::test(flavor = "current_thread")]
    async fn fetch_offsets_clears_transient_topics_on_success() {
        let (mut mgr, _commit_rm, _subs) = new_manager_with_commit();
        bootstrap_metadata_with_topic(&mgr.shared.metadata, "t1", 2);
        let tp = TopicPartition::new("t1".to_string(), 1);
        let mut timestamps = HashMap::new();
        timestamps.insert(tp.clone(), EARLIEST_TIMESTAMP);

        let rx = mgr.fetch_offsets(timestamps, false);
        // `fetch_offsets` adds "t1" to the transient set before issuing
        // the request — `add_transient_topics` is called inside the
        // method.
        assert!(
            mgr.shared.metadata.transient_topics_snapshot_for_test().contains("t1"),
            "fetch_offsets must add the topic to the transient set before issuing the request",
        );

        let response = build_list_offsets_response("t1", vec![(1, Errors::None, 100, 5, UNKNOWN_EPOCH)]);
        assert!(complete_first_unsent_with_response(&mut mgr, response, 0).await);
        let _result = await_fetch_result(&mut mgr, rx, 0).await.expect("ok");

        // Java's `whenComplete` fires on success; the transient set
        // must be cleared.
        assert!(
            !mgr.shared.metadata.transient_topics_snapshot_for_test().contains("t1"),
            "transient_topics must be cleared on fetch_offsets success completion",
        );
    }

    /// Regression for COMMENTS R2-4: failure branch — Java's
    /// `whenComplete` fires on both success AND error. Use the
    /// topic-authorization failure path which exercises
    /// `fail_request_state`.
    #[tokio::test(flavor = "current_thread")]
    async fn fetch_offsets_clears_transient_topics_on_failure() {
        let (mut mgr, _commit_rm, _subs) = new_manager_with_commit();
        bootstrap_metadata_with_topic(&mgr.shared.metadata, "t1", 2);
        let tp = TopicPartition::new("t1".to_string(), 1);
        let mut timestamps = HashMap::new();
        timestamps.insert(tp, EARLIEST_TIMESTAMP);

        let rx = mgr.fetch_offsets(timestamps, false);
        assert!(
            mgr.shared.metadata.transient_topics_snapshot_for_test().contains("t1"),
            "fetch_offsets must add the topic to the transient set",
        );

        let response =
            build_list_offsets_response("t1", vec![(1, Errors::TopicAuthorizationFailed, -1, -1, UNKNOWN_EPOCH)]);
        assert!(complete_first_unsent_with_response(&mut mgr, response, 0).await);
        let outcome = await_fetch_result(&mut mgr, rx, 0).await;
        outcome.expect_err("expected topic-authorization error");

        // Java's `whenComplete` fires on the error branch too.
        assert!(
            !mgr.shared.metadata.transient_topics_snapshot_for_test().contains("t1"),
            "transient_topics must be cleared on fetch_offsets error completion",
        );
    }

    /// Java parity: `testListOffsetsWaitingForMetadataUpdate_Timeout`.
    /// Building the request fails because the leader is unknown; the
    /// request is parked on `requests_to_retry`, `metadata.requestUpdate(true)`
    /// is requested, `poll()` returns no unsent requests, and the fetch
    /// future never resolves (Java asserts `TimeoutException` on
    /// `future.get(5ms)`; the Rust analogue is "receiver still pending").
    #[tokio::test(flavor = "current_thread")]
    async fn fetch_offsets_unknown_leader_parks_on_retry() {
        let mut mgr = new_manager();
        // No metadata bootstrap: leader is unknown.
        // Seed the backoff counter to a non-zero value so the
        // `request_update(true)` reset is observable and distinguishable
        // from a `request_update(false)` (which leaves it untouched).
        mgr.shared.metadata.metadata_arc().set_equivalent_response_count_for_test(3);
        let tp = TopicPartition::new("t1".to_string(), 1);
        let mut timestamps = HashMap::new();
        timestamps.insert(tp, EARLIEST_TIMESTAMP);

        let mut rx = mgr.fetch_offsets(timestamps, false);
        assert_eq!(mgr.requests_to_send_count(), 0, "no request built when leader unknown");
        assert_eq!(mgr.requests_to_retry_count(), 1, "state parked for retry");
        // Java: `verify(metadata).requestUpdate(true)`. The unknown-leader
        // build path calls `request_update(true)`, which sets the
        // `need_full_update` flag (distinct from the transient-topic
        // partial-update set by `fetch_offsets` itself) AND resets the
        // `equivalent_response_count` backoff counter to 0 — the latter is
        // the side effect that pins the `true` argument specifically (a
        // `request_update(false)` would have left it at 3).
        assert!(
            mgr.shared.metadata.metadata_arc().need_full_update_for_test(),
            "unknown leader must trigger metadata.requestUpdate(true)"
        );
        assert_eq!(
            mgr.shared.metadata.metadata_arc().equivalent_response_count_for_test(),
            0,
            "requestUpdate(true) must reset the equivalent-response backoff counter (distinguishes true from false)"
        );

        // Subsequent poll yields no unsent requests.
        let res = RequestManager::poll(&mut mgr, 0);
        assert!(res.unsent_requests.is_empty());

        // Java: metadata update never arrives within the future's time
        // boundary, so `future.get(5ms)` throws `TimeoutException`. The
        // Rust receiver must still be pending (un-resolved).
        assert!(
            matches!(rx.try_recv(), Err(oneshot::error::TryRecvError::Empty)),
            "fetch future must stay pending while the request is parked for retry"
        );
    }

    /// Java parity: `testListOffsetsWaitingForMetadataUpdate_RetrySucceeds`.
    /// First attempt parks the state (no leader); subsequent metadata
    /// update fires `on_update`, which replays the request — this time
    /// with a known leader.
    #[tokio::test(flavor = "current_thread")]
    async fn fetch_offsets_metadata_update_retries_successfully() {
        let mut mgr = new_manager();
        // Seed the backoff counter so the `request_update(true)` reset is
        // observable (distinguishes the `true` argument from `false`).
        mgr.shared.metadata.metadata_arc().set_equivalent_response_count_for_test(3);
        let tp = TopicPartition::new("t1".to_string(), 1);
        let mut timestamps = HashMap::new();
        timestamps.insert(tp.clone(), EARLIEST_TIMESTAMP);

        let rx = mgr.fetch_offsets(timestamps, false);
        assert_eq!(mgr.requests_to_send_count(), 0);
        assert_eq!(mgr.requests_to_retry_count(), 1);
        // Java: `verify(metadata).requestUpdate(true)` — same unknown-leader
        // path as the timeout test. The `true` argument is pinned by the
        // backoff-counter reset to 0 (a `false` would leave it at 3).
        assert!(
            mgr.shared.metadata.metadata_arc().need_full_update_for_test(),
            "unknown leader must trigger metadata.requestUpdate(true)"
        );
        assert_eq!(
            mgr.shared.metadata.metadata_arc().equivalent_response_count_for_test(),
            0,
            "requestUpdate(true) must reset the equivalent-response backoff counter"
        );

        // Trigger metadata update — fires the cluster listener which
        // replays the parked request.
        bootstrap_metadata_with_topic(&mgr.shared.metadata, "t1", 2);
        assert_eq!(
            mgr.requests_to_send_count(),
            1,
            "cluster listener should have re-issued the request after metadata update"
        );
        assert_eq!(mgr.requests_to_retry_count(), 0);

        // Complete the now-issued request.
        let response = build_list_offsets_response("t1", vec![(1, Errors::None, 100, 5, UNKNOWN_EPOCH)]);
        assert!(complete_first_unsent_with_response(&mut mgr, response, 0).await);

        let result = await_fetch_result(&mut mgr, rx, 0).await.expect("ok");
        let oat = result.get(&tp).expect("entry present").as_ref().expect("non-null offset");
        assert_eq!(oat.offset(), 5);
    }

    /// The full retriable-error matrix used by Java's
    /// `OffsetsRequestManagerTest.retriableErrors()` `@MethodSource`. Every
    /// one of these, when received in a `ListOffsets` response, must be
    /// treated as retriable: the partition is re-parked and a
    /// `metadata.requestUpdate(false)` is requested.
    const RETRIABLE_LIST_OFFSETS_ERRORS: &[Errors] = &[
        Errors::NotLeaderOrFollower,
        Errors::ReplicaNotAvailable,
        Errors::KafkaStorageError,
        Errors::OffsetNotAvailable,
        Errors::LeaderNotAvailable,
        Errors::FencedLeaderEpoch,
        Errors::BrokerNotAvailable,
        Errors::InvalidRequest,
        Errors::UnknownLeaderEpoch,
        Errors::UnknownTopicOrPartition,
    ];

    /// Mirrors the response-handler classification in
    /// [`OffsetFetcherUtilsState::handle_list_offset_response`]: a
    /// `ListOffsets` partition error is retriable (lands in
    /// `partitions_to_retry`) unless it is `NONE`,
    /// `UNSUPPORTED_FOR_MESSAGE_FORMAT` (dropped, null offset), or
    /// `TOPIC_AUTHORIZATION_FAILED` (fatal). Used by the mixed-error
    /// matrix test to decide whether a retry round is expected.
    fn is_retriable_list_offsets_error(error: Errors) -> bool {
        !matches!(
            error,
            Errors::None | Errors::UnsupportedForMessageFormat | Errors::TopicAuthorizationFailed
        )
    }

    /// Java parity: `testRequestFailsWithRetriableError_RetrySucceeds`
    /// (`@ParameterizedTest @MethodSource("retriableErrors")`). Translated
    /// as a loop over EVERY one of the 10 retriable error codes (DoD §3 —
    /// no collapsing a parameterized matrix to a single representative).
    ///
    /// For each error: first attempt's response carries the retriable
    /// error → partition added to `remaining_to_search`, state re-parked,
    /// `metadata.requestUpdate(false)` requested. Metadata update → replay
    /// → success.
    #[tokio::test(flavor = "current_thread")]
    async fn fetch_offsets_retriable_error_retries_after_metadata_update() {
        for &error in RETRIABLE_LIST_OFFSETS_ERRORS {
            // Fresh manager per error so the metadata `need_full_update`
            // flag (reset to false by `bootstrap_metadata_with_topic`'s
            // `update_with_current_request_version`) is a clean baseline.
            let (mut mgr, _commit_rm, _subs) = new_manager_with_commit();
            bootstrap_metadata_with_topic(&mgr.shared.metadata, "t1", 2);
            // Seed the backoff counter so the `request_update(false)` on the
            // retriable branch (which must NOT reset it) is distinguishable
            // from a `request_update(true)`.
            mgr.shared.metadata.metadata_arc().set_equivalent_response_count_for_test(3);
            let tp = TopicPartition::new("t1".to_string(), 1);
            let mut timestamps = HashMap::new();
            timestamps.insert(tp.clone(), EARLIEST_TIMESTAMP);

            let rx = mgr.fetch_offsets(timestamps, false);
            assert_eq!(mgr.requests_to_send_count(), 1, "{error:?}: one request built");
            // After the leader is known, `need_full_update` is false (only
            // the transient-topic partial-update was set by fetch_offsets).
            assert!(
                !mgr.shared.metadata.metadata_arc().need_full_update_for_test(),
                "{error:?}: no full-update requested before the error response"
            );

            // Respond with the retriable error.
            let response = build_list_offsets_response("t1", vec![(1, error, -1, -1, UNKNOWN_EPOCH)]);
            assert!(complete_first_unsent_with_response(&mut mgr, response, 0).await);

            // After the response is drained, the partition should be re-parked.
            for _ in 0..16 {
                tokio::task::yield_now().await;
                let _ = RequestManager::poll(&mut mgr, 0);
                if mgr.requests_to_retry_count() == 1 {
                    break;
                }
            }
            assert_eq!(
                mgr.requests_to_retry_count(),
                1,
                "{error:?}: retriable error should re-park the state on requests_to_retry"
            );
            // Java: `verify(metadata).requestUpdate(false)`. The retriable
            // branch in `apply_partial_result` calls `request_update(false)`,
            // setting `need_full_update` and (crucially) NOT resetting the
            // backoff counter — the latter pins the `false` argument.
            assert!(
                mgr.shared.metadata.metadata_arc().need_full_update_for_test(),
                "{error:?}: retriable error must trigger metadata.requestUpdate(false)"
            );
            assert_eq!(
                mgr.shared.metadata.metadata_arc().equivalent_response_count_for_test(),
                3,
                "{error:?}: requestUpdate(false) must NOT reset the backoff counter (distinguishes false from true)"
            );

            // Metadata update fires the listener → replay.
            bootstrap_metadata_with_topic(&mgr.shared.metadata, "t1", 2);
            assert_eq!(mgr.requests_to_send_count(), 1, "{error:?}: replayed request");
            assert_eq!(mgr.requests_to_retry_count(), 0, "{error:?}");

            let response = build_list_offsets_response("t1", vec![(1, Errors::None, 100, 5, UNKNOWN_EPOCH)]);
            assert!(complete_first_unsent_with_response(&mut mgr, response, 0).await);

            let result = await_fetch_result(&mut mgr, rx, 0).await.expect("ok");
            let oat = result.get(&tp).expect("entry present").as_ref().expect("non-null offset");
            assert_eq!(oat.offset(), 5, "{error:?}: retried offset");
        }
    }

    // =================================================================
    //   Phase 32: ORM fetch-path multi-partition / multi-node tests.
    //   Translated from `OffsetsRequestManagerTest` (fetch group) and
    //   `OffsetFetcherTest` (offsetsForTimes / beginning / end — KIP-848
    //   logic now lives in `OffsetsRequestManager::fetch_offsets`). See
    //   `design/history/Milestone-8/Phase-32-test-parity-offset-queries/PLAN.md`.
    // =================================================================

    /// Java parity: `testListOffsetsRequestMultiplePartitions`. Two
    /// partitions sharing one leader → a single `ListOffsets` request,
    /// both offsets returned.
    #[tokio::test(flavor = "current_thread")]
    async fn fetch_offsets_multiple_partitions_same_leader() {
        let (mut mgr, _commit_rm, _subs) = new_manager_with_commit();
        // One node → both partitions share leader node 0.
        bootstrap_metadata_with_nodes(&mgr.shared.metadata, "t1", 3, 1);
        let tp1 = TopicPartition::new("t1".to_string(), 1);
        let tp2 = TopicPartition::new("t1".to_string(), 2);
        let mut timestamps = HashMap::new();
        timestamps.insert(tp1.clone(), EARLIEST_TIMESTAMP);
        timestamps.insert(tp2.clone(), EARLIEST_TIMESTAMP);

        let rx = mgr.fetch_offsets(timestamps, false);
        assert_eq!(mgr.requests_to_send_count(), 1, "two partitions on one leader → one request");
        assert_eq!(mgr.requests_to_retry_count(), 0);

        let response = build_list_offsets_response(
            "t1",
            vec![
                (1, Errors::None, 100, 5, UNKNOWN_EPOCH),
                (2, Errors::None, 100, 5, UNKNOWN_EPOCH),
            ],
        );
        assert!(complete_first_unsent_with_response(&mut mgr, response, 0).await);

        let result = await_fetch_result(&mut mgr, rx, 0).await.expect("ok");
        assert_eq!(result.get(&tp1).expect("tp1").as_ref().expect("non-null").offset(), 5);
        assert_eq!(result.get(&tp2).expect("tp2").as_ref().expect("non-null").offset(), 5);
    }

    /// Java parity: `testRequestPartiallyFailsWithRetriableError_RetrySucceeds`.
    /// Two partitions on two distinct leaders → two requests. One node
    /// succeeds, the other returns a retriable `UNKNOWN_LEADER_EPOCH`.
    /// The partial result merges (`apply_partial_result`): the failed
    /// partition is re-parked, `metadata.requestUpdate(false)` is
    /// requested, and the retry (after a metadata update) succeeds. The
    /// global result carries BOTH offsets.
    #[tokio::test(flavor = "current_thread")]
    async fn fetch_offsets_partial_retriable_error_merges_after_retry() {
        let (mut mgr, _commit_rm, _subs) = new_manager_with_commit();
        // Two nodes → partition 1 → node 1, partition 2 → node 0 (distinct
        // leaders, mirroring Java's LEADER_1 / LEADER_2).
        bootstrap_metadata_with_nodes(&mgr.shared.metadata, "t1", 3, 2);
        // Seed the backoff counter to a known non-zero value so that the
        // subsequent `request_update(false)` (which must NOT reset it) is
        // distinguishable from a `request_update(true)` (which would).
        mgr.shared.metadata.metadata_arc().set_equivalent_response_count_for_test(3);
        let tp1 = TopicPartition::new("t1".to_string(), 1);
        let tp2 = TopicPartition::new("t1".to_string(), 2);
        let mut timestamps = HashMap::new();
        timestamps.insert(tp1.clone(), EARLIEST_TIMESTAMP);
        timestamps.insert(tp2.clone(), EARLIEST_TIMESTAMP);

        let rx = mgr.fetch_offsets(timestamps, false);
        assert_eq!(mgr.requests_to_send_count(), 2, "two leaders → two requests");
        assert_eq!(mgr.requests_to_retry_count(), 0);
        assert!(
            !mgr.shared.metadata.metadata_arc().need_full_update_for_test(),
            "no full-update requested before the partial error response"
        );

        // Complete both per-node requests: partition 1 succeeds (offset 5),
        // partition 2 returns a retriable error.
        let mut per_partition: HashMap<i32, (Errors, i64, i64, i32)> = HashMap::new();
        per_partition.insert(1, (Errors::None, 100, 5, UNKNOWN_EPOCH));
        per_partition.insert(2, (Errors::UnknownLeaderEpoch, -1, -1, UNKNOWN_EPOCH));
        let drained = complete_all_unsent_with_per_partition_response(&mut mgr, "t1", &per_partition, 0).await;
        assert_eq!(drained, 2, "both per-node requests completed");

        // After both partial results merge, the failed partition is
        // re-parked and a metadata update is requested.
        for _ in 0..16 {
            tokio::task::yield_now().await;
            let _ = RequestManager::poll(&mut mgr, 0);
            if mgr.requests_to_retry_count() == 1 {
                break;
            }
        }
        assert_eq!(mgr.requests_to_retry_count(), 1, "failed partition re-parked");
        assert_eq!(mgr.requests_to_send_count(), 0);
        assert!(
            mgr.shared.metadata.metadata_arc().need_full_update_for_test(),
            "partial retriable error must trigger metadata.requestUpdate(false)"
        );
        // Java: `verify(metadata).requestUpdate(false)`. The `false` argument
        // is pinned by the backoff counter NOT being reset (it stays at the
        // seeded 3); a `request_update(true)` would have reset it to 0.
        assert_eq!(
            mgr.shared.metadata.metadata_arc().equivalent_response_count_for_test(),
            3,
            "requestUpdate(false) must NOT reset the equivalent-response backoff counter (distinguishes false from true)"
        );

        // Metadata update → replay the failed partition's request.
        bootstrap_metadata_with_nodes(&mgr.shared.metadata, "t1", 3, 2);
        assert_eq!(mgr.requests_to_send_count(), 1, "replayed request for the failed partition");
        assert_eq!(mgr.requests_to_retry_count(), 0);

        // The replayed request now succeeds for partition 2 (offset 5).
        let response = build_list_offsets_response("t1", vec![(2, Errors::None, 100, 5, UNKNOWN_EPOCH)]);
        assert!(complete_first_unsent_with_response(&mut mgr, response, 0).await);

        // The global result carries BOTH offsets.
        let result = await_fetch_result(&mut mgr, rx, 0).await.expect("ok");
        assert_eq!(result.get(&tp1).expect("tp1").as_ref().expect("non-null").offset(), 5);
        assert_eq!(result.get(&tp2).expect("tp2").as_ref().expect("non-null").offset(), 5);
    }

    /// Java parity: `OffsetFetcherTest.testGetOffsetsForTimesWhenSomeTopicPartitionLeadersNotKnownInitially`.
    ///
    /// Exercises the **build-time partial park** branch of
    /// `build_list_offsets_requests`: some requested partitions have known
    /// leaders at build time (their request is built and `expected_responses`
    /// counts only those nodes), while another requested partition's topic is
    /// NOT yet in the metadata cache, so it goes to `remaining_to_search` via
    /// `group_list_offset_requests` and triggers `request_update(true)`.
    ///
    /// `build_list_offsets_requests` returns `Ok(unsent_requests)` for the
    /// resolvable subset (Java `OffsetsRequestManager.java:575-583`), so the
    /// known partitions' requests fly immediately. When their responses
    /// arrive, `apply_partial_result` sees `remaining_to_search` non-empty,
    /// re-parks the state, and requests a metadata update (`requestUpdate(false)`).
    /// A second metadata refresh brings in the missing topic; the parked
    /// request replays, the now-resolvable partition completes, and the global
    /// result MERGES the build-time-resolved partitions with the
    /// build-time-parked-then-resolved partition into one map.
    ///
    /// This is the only test that drives the `Ok`-with-non-empty-
    /// `remaining_to_search` branch: the all-leaderless park tests hit
    /// `Err(StaleMetadata)` (no request built), and the partial-response-error
    /// tests build every partition successfully on round 1. Distinct path,
    /// distinct coverage.
    #[tokio::test(flavor = "current_thread")]
    async fn fetch_offsets_build_time_partial_park_merges_after_metadata_update() {
        let (mut mgr, _commit_rm, _subs) = new_manager_with_commit();
        // Initial metadata knows ONLY t1 (3 nodes). The second topic t2 is
        // unknown, so t2-p0's leader is unresolvable at build time. Mirrors
        // Java's "metadata initially has one topic".
        bootstrap_metadata_multi_topic(&mgr.shared.metadata, &[("t1", 2)], 3);
        // Seed the backoff counter so the unknown-leader `request_update(true)`
        // reset is observable.
        mgr.shared.metadata.metadata_arc().set_equivalent_response_count_for_test(3);

        // tp0 (t1, p0) → node 0, tp1 (t1, p1) → node 1 — both known leaders.
        let tp0 = TopicPartition::new("t1".to_string(), 0);
        let tp1 = TopicPartition::new("t1".to_string(), 1);
        // t2p0 (t2, p0) — leader unknown until the second metadata refresh.
        let t2p0 = TopicPartition::new("t2".to_string(), 0);
        let mut timestamps = HashMap::new();
        timestamps.insert(tp0.clone(), EARLIEST_TIMESTAMP);
        timestamps.insert(tp1.clone(), EARLIEST_TIMESTAMP);
        timestamps.insert(t2p0.clone(), EARLIEST_TIMESTAMP);

        let rx = mgr.fetch_offsets(timestamps, false);

        // Build-time partial park: the two known partitions build into
        // requests (two distinct leaders → two requests); t2p0 parks in
        // `remaining_to_search`. The state is NOT on `requests_to_retry`
        // yet (a request WAS built), unlike the all-leaderless case.
        assert_eq!(
            mgr.requests_to_send_count(),
            2,
            "two known-leader partitions build into requests while the unknown-leader partition parks at build time"
        );
        assert_eq!(
            mgr.requests_to_retry_count(),
            0,
            "build-time partial park does NOT park the whole state on requests_to_retry (a request was built)"
        );
        // The unknown leader triggered `request_update(true)` inside
        // `group_list_offset_requests` (reset the backoff counter to 0).
        assert!(
            mgr.shared.metadata.metadata_arc().need_full_update_for_test(),
            "unknown leader at build time must trigger metadata.requestUpdate(true)"
        );
        assert_eq!(
            mgr.shared.metadata.metadata_arc().equivalent_response_count_for_test(),
            0,
            "build-time requestUpdate(true) must reset the backoff counter"
        );

        // Round 1: complete the two known-leader requests (tp0 → 11, tp1 → 32).
        let mut per_partition: HashMap<i32, (Errors, i64, i64, i32)> = HashMap::new();
        per_partition.insert(0, (Errors::None, 1000, 11, UNKNOWN_EPOCH));
        per_partition.insert(1, (Errors::None, 1000, 32, UNKNOWN_EPOCH));
        let drained = complete_all_unsent_with_per_partition_response(&mut mgr, "t1", &per_partition, 0).await;
        assert_eq!(drained, 2, "both known-leader requests completed");

        // After both responses merge, `remaining_to_search` (still holding
        // t2p0) is non-empty, so the state is re-parked and
        // `requestUpdate(false)` is issued. Seed the counter again so the
        // (false) non-reset is observable across the re-park.
        mgr.shared.metadata.metadata_arc().set_equivalent_response_count_for_test(7);
        for _ in 0..16 {
            tokio::task::yield_now().await;
            let _ = RequestManager::poll(&mut mgr, 0);
            if mgr.requests_to_retry_count() == 1 {
                break;
            }
        }
        assert_eq!(
            mgr.requests_to_retry_count(),
            1,
            "state re-parked because t2p0 remains in remaining_to_search"
        );
        assert_eq!(mgr.requests_to_send_count(), 0);

        // Second metadata refresh adds t2 (3 nodes → t2-p0 → node 0). This
        // fires the cluster listener → replays the parked request, now
        // resolving t2p0's leader.
        bootstrap_metadata_multi_topic(&mgr.shared.metadata, &[("t1", 2), ("t2", 1)], 3);
        assert_eq!(
            mgr.requests_to_send_count(),
            1,
            "metadata refresh resolves t2's leader → parked request replays"
        );
        assert_eq!(mgr.requests_to_retry_count(), 0);

        // The replayed request now resolves t2p0 (offset 54).
        let response = build_list_offsets_response("t2", vec![(0, Errors::None, 1000, 54, UNKNOWN_EPOCH)]);
        assert!(complete_first_unsent_with_response(&mut mgr, response, 0).await);

        // The merged global result carries ALL THREE offsets: the two
        // build-time-resolved partitions PLUS the build-time-parked-then-
        // resolved partition.
        let result = await_fetch_result(&mut mgr, rx, 0).await.expect("ok");
        assert_eq!(
            result.get(&tp0).expect("tp0").as_ref().expect("non-null").offset(),
            11,
            "build-time-resolved tp0"
        );
        assert_eq!(
            result.get(&tp1).expect("tp1").as_ref().expect("non-null").offset(),
            32,
            "build-time-resolved tp1"
        );
        assert_eq!(
            result.get(&t2p0).expect("t2p0").as_ref().expect("non-null").offset(),
            54,
            "build-time-parked-then-resolved t2p0 merged into the same result"
        );
    }

    /// Java parity: `testRequestFailedResponse_NonRetriableErrorTimeout`.
    /// The response carries an error keyed on a partition that was NOT in
    /// the request (`TEST_PARTITION_2` while only `TEST_PARTITION_1` was
    /// requested).
    ///
    /// **Documented divergence from Java's observable behavior.** Java's
    /// `MultiNodeRequest.onComplete` callback calls
    /// `listOffsetsRequestState.addPartitionsToRetry(multiNodeResult.partitionsToRetry)`,
    /// which does `partitionsToRetry.stream().collect(toMap(tp -> tp,
    /// timestampsToSearch::get))`. For an *unrequested* partition,
    /// `timestampsToSearch.get(tp2)` is `null`, so `Collectors.toMap`
    /// throws a `NullPointerException` inside the `whenComplete`
    /// callback — *before* `globalResult.complete(...)` is reached. The
    /// NPE is swallowed by the `CompletableFuture` machinery, leaving
    /// `globalResult` un-completed, so Java's `future.get(5ms)` throws
    /// `TimeoutException` (the test's assertion).
    ///
    /// The Rust `add_partitions_to_retry`
    /// (offsets_request_manager.rs:172-178) faithfully mirrors the *intent*
    /// — re-add only originally-requested partitions — by filtering on
    /// `timestamps_to_search.get(tp)` instead of replicating the NPE. So
    /// the unrequested partition is silently skipped, `remaining_to_search`
    /// stays empty, and the global result completes with `{tp1: None}`
    /// (requested partition present, no offset). This is the faithful
    /// translation of the documented intent (DoD §7 / §28 — a deviation
    /// from an accidental Java NPE artifact, with rationale). The
    /// behavioral contract that matters — "nothing pending to send or
    /// retry; the requested partition surfaces no offset" — is asserted.
    #[tokio::test(flavor = "current_thread")]
    async fn fetch_offsets_non_retriable_error_for_unrequested_partition() {
        let (mut mgr, _commit_rm, _subs) = new_manager_with_commit();
        bootstrap_metadata_with_topic(&mgr.shared.metadata, "t1", 3);
        let tp1 = TopicPartition::new("t1".to_string(), 1);
        let mut timestamps = HashMap::new();
        timestamps.insert(tp1.clone(), EARLIEST_TIMESTAMP);

        let rx = mgr.fetch_offsets(timestamps, false);
        assert_eq!(mgr.requests_to_send_count(), 1);
        assert_eq!(mgr.requests_to_retry_count(), 0);

        // Respond with an error keyed on partition 2 — which was never
        // requested (only partition 1 was). The handler matches responses
        // by the request's `node_partitions`, so partition 2's retry entry
        // is filtered out (not in `timestamps_to_search`).
        let response = build_list_offsets_response("t1", vec![(2, Errors::BrokerNotAvailable, -1, -1, UNKNOWN_EPOCH)]);
        assert!(complete_first_unsent_with_response(&mut mgr, response, 0).await);

        let result = await_fetch_result(&mut mgr, rx, 0).await.expect("global result resolves");

        // Java: nothing pending to send or retry (the unrequested-partition
        // retry entry is filtered out).
        assert_eq!(
            mgr.requests_to_retry_count(),
            0,
            "no retry for an unrequested-partition response"
        );
        assert_eq!(mgr.requests_to_send_count(), 0);
        // The requested partition surfaces no offset (Java: would hang on
        // the NPE; Rust resolves it cleanly with a null entry).
        assert!(result.contains_key(&tp1), "requested partition present in result");
        assert!(
            result.get(&tp1).expect("tp1 entry").is_none(),
            "requested partition has no offset"
        );
    }

    /// Java parity: `OffsetFetcherTest.testGetOffsetsUnknownLeaderEpoch`
    /// at the fetch path. A `ListOffsets` response carrying
    /// `UNKNOWN_LEADER_EPOCH` is retriable: the partition is re-parked and
    /// `metadata.requestUpdate(false)` is requested. (The Java test drives
    /// the reset path and asserts SubscriptionState reset flags; that
    /// path is covered by Phase 31's `reset_*` tests. Here we pin the
    /// fetch-path classification.)
    #[tokio::test(flavor = "current_thread")]
    async fn fetch_offsets_unknown_leader_epoch_is_retriable() {
        let (mut mgr, _commit_rm, _subs) = new_manager_with_commit();
        bootstrap_metadata_with_topic(&mgr.shared.metadata, "t1", 2);
        // Seed the backoff counter so the `request_update(false)` on the
        // retriable branch (which must NOT reset it) is distinguishable.
        mgr.shared.metadata.metadata_arc().set_equivalent_response_count_for_test(3);
        let tp = TopicPartition::new("t1".to_string(), 1);
        let mut timestamps = HashMap::new();
        timestamps.insert(tp, EARLIEST_TIMESTAMP);

        let rx = mgr.fetch_offsets(timestamps, false);
        assert_eq!(mgr.requests_to_send_count(), 1);

        let response = build_list_offsets_response("t1", vec![(1, Errors::UnknownLeaderEpoch, -1, -1, UNKNOWN_EPOCH)]);
        assert!(complete_first_unsent_with_response(&mut mgr, response, 0).await);

        for _ in 0..16 {
            tokio::task::yield_now().await;
            let _ = RequestManager::poll(&mut mgr, 0);
            if mgr.requests_to_retry_count() == 1 {
                break;
            }
        }
        assert_eq!(mgr.requests_to_retry_count(), 1, "UNKNOWN_LEADER_EPOCH is retriable");
        assert!(
            mgr.shared.metadata.metadata_arc().need_full_update_for_test(),
            "retriable error must trigger metadata.requestUpdate(false)"
        );
        // `false` argument pinned by the backoff counter NOT being reset.
        assert_eq!(
            mgr.shared.metadata.metadata_arc().equivalent_response_count_for_test(),
            3,
            "requestUpdate(false) must NOT reset the backoff counter"
        );
        drop(rx);
    }

    /// Java parity: `OffsetFetcherTest.testBatchedListOffsetsMetadataErrors`.
    /// Two partitions on one leader → one batched request. The response
    /// carries `NOT_LEADER_OR_FOLLOWER` for one and
    /// `UNKNOWN_TOPIC_OR_PARTITION` for the other — both retriable. The
    /// state re-parks and the future never resolves (Java's
    /// `TimeoutException` with `time.timer(1)`).
    #[tokio::test(flavor = "current_thread")]
    async fn batched_list_offsets_metadata_errors_future_pending() {
        let (mut mgr, _commit_rm, _subs) = new_manager_with_commit();
        // One node → both partitions batched into one request.
        bootstrap_metadata_with_nodes(&mgr.shared.metadata, "t1", 3, 1);
        let tp1 = TopicPartition::new("t1".to_string(), 1);
        let tp2 = TopicPartition::new("t1".to_string(), 2);
        let mut timestamps = HashMap::new();
        timestamps.insert(tp1.clone(), EARLIEST_TIMESTAMP);
        timestamps.insert(tp2.clone(), EARLIEST_TIMESTAMP);

        let mut rx = mgr.fetch_offsets(timestamps, false);
        assert_eq!(mgr.requests_to_send_count(), 1, "batched into one request");

        let response = build_list_offsets_response(
            "t1",
            vec![
                (1, Errors::NotLeaderOrFollower, UNKNOWN_TIMESTAMP, UNKNOWN_OFFSET, UNKNOWN_EPOCH),
                (
                    2,
                    Errors::UnknownTopicOrPartition,
                    UNKNOWN_TIMESTAMP,
                    UNKNOWN_OFFSET,
                    UNKNOWN_EPOCH,
                ),
            ],
        );
        assert!(complete_first_unsent_with_response(&mut mgr, response, 0).await);

        for _ in 0..16 {
            tokio::task::yield_now().await;
            let _ = RequestManager::poll(&mut mgr, 0);
            if mgr.requests_to_retry_count() == 1 {
                break;
            }
        }
        // Both partitions retriable → re-parked, future stays pending.
        assert_eq!(
            mgr.requests_to_retry_count(),
            1,
            "both retriable errors re-park the batched state"
        );
        assert!(
            matches!(rx.try_recv(), Err(oneshot::error::TryRecvError::Empty)),
            "future must stay pending (Java TimeoutException)"
        );
    }

    /// Java parity: `OffsetFetcherTest.testGetOffsetsForTimes`. Drives the
    /// multi-partition mixed-error matrix from the Java test as a loop:
    /// for each `(error_p0, error_p1, offset_p0, expected_p0)` row, two
    /// partitions are searched, retriable errors are retried after a
    /// metadata update, and the final offsets/timestamps match.
    ///
    /// `offsetsForTimes` uses `require_timestamps = true`; a `None` result
    /// for a partition (UNSUPPORTED_FOR_MESSAGE_FORMAT, or NONE + unknown
    /// offset) maps to a missing/null entry.
    #[tokio::test(flavor = "current_thread")]
    async fn offsets_for_times_multi_partition_mixed_errors() {
        // (error_p0, error_p1, offset_p0, expected_p0_present)
        // Mirrors the Java rows. Both partitions are searched with a real
        // timestamp; on the FIRST attempt one or both may carry a
        // retriable error, after which a metadata update + retry resolves
        // them to NONE.
        struct Row {
            error_p0: Errors,
            error_p1: Errors,
            offset_p0: i64,
            expected_p0: Option<i64>,
        }
        let rows = [
            // Error code NONE with unknown offset → null p0.
            Row { error_p0: Errors::None, error_p1: Errors::None, offset_p0: -1, expected_p0: None },
            // Error code NONE with known offset.
            Row {
                error_p0: Errors::None,
                error_p1: Errors::None,
                offset_p0: 10,
                expected_p0: Some(10),
            },
            // Both partitions have a (retriable) error → retried.
            Row {
                error_p0: Errors::NotLeaderOrFollower,
                error_p1: Errors::InvalidRequest,
                offset_p0: 10,
                expected_p0: Some(10),
            },
            // Second partition has error.
            Row {
                error_p0: Errors::None,
                error_p1: Errors::NotLeaderOrFollower,
                offset_p0: 10,
                expected_p0: Some(10),
            },
            // First partition has error.
            Row {
                error_p0: Errors::NotLeaderOrFollower,
                error_p1: Errors::None,
                offset_p0: 10,
                expected_p0: Some(10),
            },
            Row {
                error_p0: Errors::UnknownTopicOrPartition,
                error_p1: Errors::None,
                offset_p0: 10,
                expected_p0: Some(10),
            },
            // UNSUPPORTED_FOR_MESSAGE_FORMAT → null p0 (non-retriable, dropped).
            Row {
                error_p0: Errors::UnsupportedForMessageFormat,
                error_p1: Errors::None,
                offset_p0: 10,
                expected_p0: None,
            },
            Row {
                error_p0: Errors::BrokerNotAvailable,
                error_p1: Errors::None,
                offset_p0: 10,
                expected_p0: Some(10),
            },
        ];
        const OFFSET_P1: i64 = 100;

        for (i, row) in rows.iter().enumerate() {
            let (mut mgr, _commit_rm, _subs) = new_manager_with_commit();
            // Two partitions on one leader so both batch into one request.
            bootstrap_metadata_with_nodes(&mgr.shared.metadata, "t1", 3, 1);
            let tp1 = TopicPartition::new("t1".to_string(), 1);
            let tp2 = TopicPartition::new("t1".to_string(), 2);
            let mut timestamps = HashMap::new();
            timestamps.insert(tp1.clone(), 0);
            timestamps.insert(tp2.clone(), 0);

            // require_timestamps = true matches offsetsForTimes.
            let rx = mgr.fetch_offsets(timestamps, true);
            assert_eq!(mgr.requests_to_send_count(), 1, "row {i}: one batched request");

            // First response: apply the row's errors. A retriable error
            // re-parks the partition; a non-retriable error (UNSUPPORTED)
            // or NONE finalises it.
            let response = build_list_offsets_response(
                "t1",
                vec![
                    (1, row.error_p0, row.offset_p0, row.offset_p0, UNKNOWN_EPOCH),
                    (2, row.error_p1, OFFSET_P1, OFFSET_P1, UNKNOWN_EPOCH),
                ],
            );
            assert!(complete_first_unsent_with_response(&mut mgr, response, 0).await);

            // Determine whether a retry is needed (any retriable error).
            let p0_retriable = is_retriable_list_offsets_error(row.error_p0);
            let p1_retriable = is_retriable_list_offsets_error(row.error_p1);

            if p0_retriable || p1_retriable {
                for _ in 0..16 {
                    tokio::task::yield_now().await;
                    let _ = RequestManager::poll(&mut mgr, 0);
                    if mgr.requests_to_retry_count() == 1 {
                        break;
                    }
                }
                assert_eq!(mgr.requests_to_retry_count(), 1, "row {i}: retriable error re-parked");
                // Metadata update → replay; the retried partition(s) now
                // resolve to NONE with the correct offset.
                bootstrap_metadata_with_nodes(&mgr.shared.metadata, "t1", 3, 1);
                assert_eq!(mgr.requests_to_send_count(), 1, "row {i}: replay request");

                let mut parts = Vec::new();
                if p0_retriable {
                    parts.push((1, Errors::None, row.offset_p0, row.offset_p0, UNKNOWN_EPOCH));
                }
                if p1_retriable {
                    parts.push((2, Errors::None, OFFSET_P1, OFFSET_P1, UNKNOWN_EPOCH));
                }
                let response = build_list_offsets_response("t1", parts);
                assert!(complete_first_unsent_with_response(&mut mgr, response, 0).await);
            }

            let result = await_fetch_result(&mut mgr, rx, 0).await.expect("ok");

            // p0 expected per the row.
            match row.expected_p0 {
                Some(off) => {
                    let oat = result.get(&tp1).expect("tp1 entry").as_ref().expect("non-null p0");
                    assert_eq!(oat.offset(), off, "row {i}: p0 offset");
                    assert_eq!(oat.timestamp(), off, "row {i}: p0 timestamp");
                },
                None => {
                    // UNSUPPORTED / unknown-offset → null entry.
                    assert!(
                        result.get(&tp1).map(|o| o.is_none()).unwrap_or(true),
                        "row {i}: p0 expected null"
                    );
                },
            }
            // p1 always present with offset 100.
            let oat1 = result.get(&tp2).expect("tp2 entry").as_ref().expect("non-null p1");
            assert_eq!(oat1.offset(), OFFSET_P1, "row {i}: p1 offset");
            assert_eq!(oat1.timestamp(), OFFSET_P1, "row {i}: p1 timestamp");
        }
    }

    /// Java parity: `OffsetFetcherTest.testGetOffsetByTimeWithPartitionsRetryCouldTriggerMetadataUpdate`.
    /// Loop over the 7-error retriable list. Two partitions on distinct
    /// leaders; tp0 succeeds first try (offset 4), tp1 carries the
    /// retriable error → metadata update → tp1 succeeds against the new
    /// leader (offset 5). Both offsets present.
    #[tokio::test(flavor = "current_thread")]
    async fn offsets_for_times_retriable_retry_triggers_metadata_update() {
        // Java's 7-error retriableErrors list for this test.
        let retriable = [
            Errors::NotLeaderOrFollower,
            Errors::ReplicaNotAvailable,
            Errors::KafkaStorageError,
            Errors::OffsetNotAvailable,
            Errors::LeaderNotAvailable,
            Errors::FencedLeaderEpoch,
            Errors::UnknownLeaderEpoch,
        ];
        const FETCH_TIMESTAMP: i64 = 10;

        for &error in &retriable {
            let (mut mgr, _commit_rm, _subs) = new_manager_with_commit();
            // Two nodes → tp0 (partition 0) → node 0, tp1 (partition 1) → node 1.
            bootstrap_metadata_with_nodes(&mgr.shared.metadata, "t1", 2, 2);
            // Seed the backoff counter so the retriable `request_update(false)`
            // (which must NOT reset it) is distinguishable from `(true)`.
            mgr.shared.metadata.metadata_arc().set_equivalent_response_count_for_test(3);
            let tp0 = TopicPartition::new("t1".to_string(), 0);
            let tp1 = TopicPartition::new("t1".to_string(), 1);
            let mut timestamps = HashMap::new();
            timestamps.insert(tp0.clone(), FETCH_TIMESTAMP);
            timestamps.insert(tp1.clone(), FETCH_TIMESTAMP);

            let rx = mgr.fetch_offsets(timestamps, true);
            assert_eq!(mgr.requests_to_send_count(), 2, "{error:?}: two leaders → two requests");

            // First responses: tp0 succeeds (offset 4), tp1 retriable.
            let mut per_partition: HashMap<i32, (Errors, i64, i64, i32)> = HashMap::new();
            per_partition.insert(0, (Errors::None, FETCH_TIMESTAMP, 4, UNKNOWN_EPOCH));
            per_partition.insert(1, (error, FETCH_TIMESTAMP, -1, UNKNOWN_EPOCH));
            let drained = complete_all_unsent_with_per_partition_response(&mut mgr, "t1", &per_partition, 0).await;
            assert_eq!(drained, 2, "{error:?}: both per-node requests completed");

            for _ in 0..16 {
                tokio::task::yield_now().await;
                let _ = RequestManager::poll(&mut mgr, 0);
                if mgr.requests_to_retry_count() == 1 {
                    break;
                }
            }
            assert_eq!(mgr.requests_to_retry_count(), 1, "{error:?}: tp1 re-parked");
            assert!(
                mgr.shared.metadata.metadata_arc().need_full_update_for_test(),
                "{error:?}: retriable error must trigger metadata update"
            );
            // Java: `requestUpdate(false)` on the retriable branch — `false`
            // pinned by the backoff counter NOT being reset.
            assert_eq!(
                mgr.shared.metadata.metadata_arc().equivalent_response_count_for_test(),
                3,
                "{error:?}: requestUpdate(false) must NOT reset the backoff counter"
            );

            // Metadata update → replay tp1's request against the (now
            // up-to-date) leader.
            bootstrap_metadata_with_nodes(&mgr.shared.metadata, "t1", 2, 2);
            assert_eq!(mgr.requests_to_send_count(), 1, "{error:?}: replay for tp1");

            let response =
                build_list_offsets_response("t1", vec![(1, Errors::None, FETCH_TIMESTAMP, 5, UNKNOWN_EPOCH)]);
            assert!(complete_first_unsent_with_response(&mut mgr, response, 0).await);

            let result = await_fetch_result(&mut mgr, rx, 0).await.expect("ok");
            assert_eq!(
                result.get(&tp0).expect("tp0").as_ref().expect("non-null").offset(),
                4,
                "{error:?}"
            );
            assert_eq!(
                result.get(&tp1).expect("tp1").as_ref().expect("non-null").offset(),
                5,
                "{error:?}"
            );
        }
    }

    /// Java parity: `OffsetFetcherTest.testBeginningOffsetsMultipleTopicPartitions`.
    /// Three partitions, `EARLIEST_TIMESTAMP` on the wire, distinct
    /// offsets 2 / 4 / 6.
    #[tokio::test(flavor = "current_thread")]
    async fn beginning_offsets_multiple_partitions() {
        let (mut mgr, _commit_rm, _subs) = new_manager_with_commit();
        bootstrap_metadata_with_nodes(&mgr.shared.metadata, "t1", 3, 1);
        let tp0 = TopicPartition::new("t1".to_string(), 0);
        let tp1 = TopicPartition::new("t1".to_string(), 1);
        let tp2 = TopicPartition::new("t1".to_string(), 2);
        let mut timestamps = HashMap::new();
        timestamps.insert(tp0.clone(), EARLIEST_TIMESTAMP);
        timestamps.insert(tp1.clone(), EARLIEST_TIMESTAMP);
        timestamps.insert(tp2.clone(), EARLIEST_TIMESTAMP);

        let rx = mgr.fetch_offsets(timestamps, false);
        assert_eq!(mgr.requests_to_send_count(), 1, "three partitions one leader → one request");

        // The request must carry EARLIEST_TIMESTAMP for each partition.
        let poll_result = RequestManager::poll(&mut mgr, 0);
        let mut unsent = poll_result.unsent_requests.into_iter().next().expect("one request");
        let built = unsent.request_builder_mut().expect("builder").build().expect("build");
        let crate::common::requests::ConcreteRequest::ListOffsets(req) = built else {
            panic!("expected ListOffsetsRequest");
        };
        for topic in req.topics() {
            for p in &topic.partitions {
                assert_eq!(p.timestamp, EARLIEST_TIMESTAMP, "beginning_offsets sends EARLIEST_TIMESTAMP");
            }
        }

        let response = build_list_offsets_response(
            "t1",
            vec![
                (0, Errors::None, EARLIEST_TIMESTAMP, 2, UNKNOWN_EPOCH),
                (1, Errors::None, EARLIEST_TIMESTAMP, 4, UNKNOWN_EPOCH),
                (2, Errors::None, EARLIEST_TIMESTAMP, 6, UNKNOWN_EPOCH),
            ],
        );
        unsent.handler().on_complete(build_list_offsets_client_response(response));

        let result = await_fetch_result(&mut mgr, rx, 0).await.expect("ok");
        assert_eq!(result.get(&tp0).expect("tp0").as_ref().expect("non-null").offset(), 2);
        assert_eq!(result.get(&tp1).expect("tp1").as_ref().expect("non-null").offset(), 4);
        assert_eq!(result.get(&tp2).expect("tp2").as_ref().expect("non-null").offset(), 6);
    }

    /// Java parity: `OffsetFetcherTest.testEndOffsetsMultipleTopicPartitions`.
    /// Three partitions, `LATEST_TIMESTAMP` on the wire, distinct offsets
    /// 5 / 7 / 9.
    #[tokio::test(flavor = "current_thread")]
    async fn end_offsets_multiple_partitions() {
        use crate::common::requests::list_offsets_request::LATEST_TIMESTAMP;
        let (mut mgr, _commit_rm, _subs) = new_manager_with_commit();
        bootstrap_metadata_with_nodes(&mgr.shared.metadata, "t1", 3, 1);
        let tp0 = TopicPartition::new("t1".to_string(), 0);
        let tp1 = TopicPartition::new("t1".to_string(), 1);
        let tp2 = TopicPartition::new("t1".to_string(), 2);
        let mut timestamps = HashMap::new();
        timestamps.insert(tp0.clone(), LATEST_TIMESTAMP);
        timestamps.insert(tp1.clone(), LATEST_TIMESTAMP);
        timestamps.insert(tp2.clone(), LATEST_TIMESTAMP);

        let rx = mgr.fetch_offsets(timestamps, false);
        assert_eq!(mgr.requests_to_send_count(), 1);

        let poll_result = RequestManager::poll(&mut mgr, 0);
        let mut unsent = poll_result.unsent_requests.into_iter().next().expect("one request");
        let built = unsent.request_builder_mut().expect("builder").build().expect("build");
        let crate::common::requests::ConcreteRequest::ListOffsets(req) = built else {
            panic!("expected ListOffsetsRequest");
        };
        for topic in req.topics() {
            for p in &topic.partitions {
                assert_eq!(p.timestamp, LATEST_TIMESTAMP, "end_offsets sends LATEST_TIMESTAMP");
            }
        }

        let response = build_list_offsets_response(
            "t1",
            vec![
                (0, Errors::None, LATEST_TIMESTAMP, 5, UNKNOWN_EPOCH),
                (1, Errors::None, LATEST_TIMESTAMP, 7, UNKNOWN_EPOCH),
                (2, Errors::None, LATEST_TIMESTAMP, 9, UNKNOWN_EPOCH),
            ],
        );
        unsent.handler().on_complete(build_list_offsets_client_response(response));

        let result = await_fetch_result(&mut mgr, rx, 0).await.expect("ok");
        assert_eq!(result.get(&tp0).expect("tp0").as_ref().expect("non-null").offset(), 5);
        assert_eq!(result.get(&tp1).expect("tp1").as_ref().expect("non-null").offset(), 7);
        assert_eq!(result.get(&tp2).expect("tp2").as_ref().expect("non-null").offset(), 9);
    }

    /// The built `ListOffsets` request carries the manager's configured
    /// isolation level on the wire. Java parity:
    /// `OffsetFetcherTest.testListOffsetSendsReadUncommitted` /
    /// `testListOffsetSendsReadCommitted` (beginning/end offsets path).
    #[tokio::test(flavor = "current_thread")]
    async fn fetch_offsets_request_carries_isolation_level_read_uncommitted() {
        let (mut mgr, _commit_rm, _subs) = new_manager_with_commit();
        bootstrap_metadata_with_topic(&mgr.shared.metadata, "t1", 2);
        let tp = TopicPartition::new("t1".to_string(), 1);
        let mut timestamps = HashMap::new();
        timestamps.insert(tp, EARLIEST_TIMESTAMP);
        let _rx = mgr.fetch_offsets(timestamps, false);

        let res = RequestManager::poll(&mut mgr, 0);
        let mut unsent = res.unsent_requests.into_iter().next().expect("one unsent");
        let built = unsent.request_builder_mut().expect("builder").build().expect("build");
        let crate::common::requests::ConcreteRequest::ListOffsets(req) = built else {
            panic!("expected ListOffsetsRequest");
        };
        assert_eq!(
            req.isolation_level().expect("isolation level"),
            IsolationLevel::ReadUncommitted,
            "default manager sends READ_UNCOMMITTED"
        );
    }

    /// READ_COMMITTED variant of the isolation-level-on-wire test.
    #[tokio::test(flavor = "current_thread")]
    async fn fetch_offsets_request_carries_isolation_level_read_committed() {
        let config = ConsumerConfig::from_properties(&std::collections::HashMap::from([
            ("bootstrap.servers".to_string(), "localhost:9092".to_string()),
            ("group.id".to_string(), "g".to_string()),
        ]))
        .expect("config");
        let subscription_state = Arc::new(Mutex::new(SubscriptionState::new(
            crate::consumer::internals::auto_offset_reset_strategy::AutoOffsetResetStrategy::EARLIEST,
        )));
        let metadata = Arc::new(ConsumerMetadata::from_config(
            &config,
            subscription_state.clone(),
            ClusterResourceListeners::new(),
        ));
        let mut mgr = OffsetsRequestManager::new(
            subscription_state,
            metadata.clone(),
            IsolationLevel::ReadCommitted,
            100,
            30_000,
            60_000,
            Arc::new(ApiVersions::new()),
            None,
        );
        bootstrap_metadata_with_topic(&metadata, "t1", 2);
        let tp = TopicPartition::new("t1".to_string(), 1);
        let mut timestamps = HashMap::new();
        timestamps.insert(tp, EARLIEST_TIMESTAMP);
        let _rx = mgr.fetch_offsets(timestamps, false);

        let res = RequestManager::poll(&mut mgr, 0);
        let mut unsent = res.unsent_requests.into_iter().next().expect("one unsent");
        let built = unsent.request_builder_mut().expect("builder").build().expect("build");
        let crate::common::requests::ConcreteRequest::ListOffsets(req) = built else {
            panic!("expected ListOffsetsRequest");
        };
        assert_eq!(
            req.isolation_level().expect("isolation level"),
            IsolationLevel::ReadCommitted,
            "READ_COMMITTED manager sends READ_COMMITTED"
        );
    }

    /// Java parity: `testRequestWithUnknownOffsetInResponseReturnsNullOffset`.
    /// Response with `Errors::None` but `UNKNOWN_OFFSET` → result entry
    /// is `None`.
    #[tokio::test(flavor = "current_thread")]
    async fn fetch_offsets_unknown_offset_in_response_returns_none() {
        let (mut mgr, _commit_rm, _subs) = new_manager_with_commit();
        bootstrap_metadata_with_topic(&mgr.shared.metadata, "t1", 2);
        let tp = TopicPartition::new("t1".to_string(), 1);
        let mut timestamps = HashMap::new();
        timestamps.insert(tp.clone(), EARLIEST_TIMESTAMP);

        let rx = mgr.fetch_offsets(timestamps, false);
        let response = build_list_offsets_response(
            "t1",
            vec![(1, Errors::None, UNKNOWN_TIMESTAMP, UNKNOWN_OFFSET, UNKNOWN_EPOCH)],
        );
        assert!(complete_first_unsent_with_response(&mut mgr, response, 0).await);

        let result = await_fetch_result(&mut mgr, rx, 0).await.expect("ok");
        // The input partition is present with `None`.
        assert!(result.contains_key(&tp));
        assert!(
            result.get(&tp).expect("entry present").is_none(),
            "expected None for unknown offset"
        );
    }

    /// Java parity: `testRequestNotSupportedErrorReturnsNullOffset`.
    /// `UNSUPPORTED_FOR_MESSAGE_FORMAT` → result entry is `None`
    /// (the partition is silently dropped from `fetched_offsets`).
    #[tokio::test(flavor = "current_thread")]
    async fn fetch_offsets_unsupported_for_message_format_returns_none() {
        let (mut mgr, _commit_rm, _subs) = new_manager_with_commit();
        bootstrap_metadata_with_topic(&mgr.shared.metadata, "t1", 2);
        let tp = TopicPartition::new("t1".to_string(), 1);
        let mut timestamps = HashMap::new();
        timestamps.insert(tp.clone(), EARLIEST_TIMESTAMP);

        let rx = mgr.fetch_offsets(timestamps, false);
        let response =
            build_list_offsets_response("t1", vec![(1, Errors::UnsupportedForMessageFormat, -1, -1, UNKNOWN_EPOCH)]);
        assert!(complete_first_unsent_with_response(&mut mgr, response, 0).await);

        let result = await_fetch_result(&mut mgr, rx, 0).await.expect("ok");
        assert!(result.contains_key(&tp));
        assert!(
            result.get(&tp).expect("entry present").is_none(),
            "unsupported format error should result in None"
        );
    }

    /// Java parity: `testRequestFailedResponse_NonRetriableAuthError`.
    /// Response carries `TopicAuthorizationFailed` → outer future
    /// completes exceptionally with the topic-authorization error.
    #[tokio::test(flavor = "current_thread")]
    async fn fetch_offsets_topic_authorization_failed_surfaces_error() {
        let (mut mgr, _commit_rm, _subs) = new_manager_with_commit();
        bootstrap_metadata_with_topic(&mgr.shared.metadata, "t1", 2);
        let tp = TopicPartition::new("t1".to_string(), 1);
        let mut timestamps = HashMap::new();
        timestamps.insert(tp, EARLIEST_TIMESTAMP);

        let rx = mgr.fetch_offsets(timestamps, false);
        let response =
            build_list_offsets_response("t1", vec![(1, Errors::TopicAuthorizationFailed, -1, -1, UNKNOWN_EPOCH)]);
        assert!(complete_first_unsent_with_response(&mut mgr, response, 0).await);

        let outcome = await_fetch_result(&mut mgr, rx, 0).await;
        let err = outcome.expect_err("expected topic-authorization error");
        // Assert error type & message content per DoD §3. Rust's
        // `KafkaError::TopicAuthorization` (Display: "Topic authorization
        // failed.") corresponds to Java's `TopicAuthorizationException`.
        // Pin both the typed variant and the human-readable message so a
        // future rename of either is caught.
        assert!(
            matches!(err, KafkaError::TopicAuthorization(_)),
            "expected KafkaError::TopicAuthorization, got {:?}",
            err
        );
        assert!(
            err.error().to_string().to_lowercase().contains("topic authorization"),
            "expected topic-authorization error message, got {:?}",
            err.error().to_string()
        );
        // After the error, nothing should remain queued.
        assert_eq!(mgr.requests_to_retry_count(), 0);
    }

    /// Java parity: `testRequestFails_AuthenticationException`. Transport-
    /// level authentication failure surfaces through the response receiver
    /// as `SaslAuthenticationFailed`. The outer future completes
    /// exceptionally; no retry entry is left behind.
    #[tokio::test(flavor = "current_thread")]
    async fn fetch_offsets_authentication_exception_completes_exceptionally() {
        let (mut mgr, _commit_rm, _subs) = new_manager_with_commit();
        bootstrap_metadata_with_topic(&mgr.shared.metadata, "t1", 2);
        let tp = TopicPartition::new("t1".to_string(), 1);
        let mut timestamps = HashMap::new();
        timestamps.insert(tp, EARLIEST_TIMESTAMP);

        let rx = mgr.fetch_offsets(timestamps, false);

        // Drain the unsent request and complete with an authentication
        // error (Java: `buildClientResponse(... new AuthenticationException(...))`).
        let poll_result = RequestManager::poll(&mut mgr, 0);
        let unsent = poll_result.unsent_requests.into_iter().next().expect("one unsent");
        unsent.handler().on_complete(build_disconnected_client_response());
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }

        let outcome = await_fetch_result(&mut mgr, rx, 0).await;
        let err = outcome.expect_err("expected authentication error");
        // Java surfaces `AuthenticationException`; the Rust translation maps it to
        // `SaslAuthenticationFailed` (see `FutureCompletionHandler::on_complete`).
        let msg = err.error().to_string();
        assert!(
            msg.contains("SaslAuthenticationFailed") || msg.contains("Authentication"),
            "expected authentication-related error, got {msg}"
        );
        assert_eq!(mgr.requests_to_retry_count(), 0);
    }

    /// Fetch-path behavior referenced by Java's
    /// `OffsetFetcherTest.testGetOffsetsForTimesWhenSomeTopicPartitionLeadersDisconnectException`.
    ///
    /// **Why this is the faithful ORM translation (not the Java test's
    /// observable retry-and-succeed).** The Java `OffsetFetcher` test asserts
    /// that a disconnect on one node is silently retried and the offset is
    /// eventually returned — but that retry-on-disconnect is a property of the
    /// *classic* `OffsetFetcher`'s `ConsumerNetworkClient` layer, which is
    /// out of scope (consumer-threading.md §20). The KIP-848
    /// `OffsetsRequestManager` fetch path does NOT re-park on a transport
    /// disconnect: `handle_fetch_offsets_response` routes a network error to
    /// `fail_request_state`, completing the global result exceptionally for
    /// ALL waiters (Java `OffsetsRequestManager.java:586`/`:600`
    /// `globalResult.completeExceptionally(error)`). So the in-scope ORM
    /// behavior is: a per-node disconnect FAILS the whole `fetch_offsets`
    /// future with `NetworkException`, leaving nothing parked for retry. This
    /// is the branch that, prior to this test, was only covered for the reset
    /// path (Phase 31), never the fetch path.
    #[tokio::test(flavor = "current_thread")]
    async fn fetch_offsets_disconnect_fails_global_result_without_reparking() {
        let (mut mgr, _commit_rm, _subs) = new_manager_with_commit();
        // Two nodes → two requests (tp1 → node 1, tp2 → node 0), so we can
        // disconnect one node while the other could still be outstanding —
        // Java's `DisconnectException` on one leader of a multi-leader fetch.
        bootstrap_metadata_with_nodes(&mgr.shared.metadata, "t1", 3, 2);
        let tp1 = TopicPartition::new("t1".to_string(), 1);
        let tp2 = TopicPartition::new("t1".to_string(), 2);
        let mut timestamps = HashMap::new();
        timestamps.insert(tp1.clone(), EARLIEST_TIMESTAMP);
        timestamps.insert(tp2.clone(), EARLIEST_TIMESTAMP);

        let rx = mgr.fetch_offsets(timestamps, false);
        let poll_result = RequestManager::poll(&mut mgr, 0);
        let mut unsent = poll_result.unsent_requests.into_iter();
        let first = unsent.next().expect("first per-node request");

        // Disconnect the FIRST per-node request. The fetch path fails the
        // entire global result immediately (it does NOT wait for the second
        // node, and does NOT re-park).
        first.handler().on_complete(build_network_disconnect_client_response());
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }

        let outcome = await_fetch_result(&mut mgr, rx, 0).await;
        let err = outcome.expect_err("disconnect must fail the global fetch_offsets future");
        // Assert error type AND message content (DoD §3). The disconnect
        // maps to `NetworkException` in `FutureCompletionHandler::on_complete`.
        assert_eq!(
            err.error(),
            crate::common::protocol::Errors::NetworkException,
            "per-node disconnect must surface as a NetworkException"
        );
        let msg = err.error().to_string();
        assert!(
            msg.contains("disconnect") || msg.contains("network") || msg.contains("Network"),
            "expected a network/disconnect-related message, got {msg}"
        );

        // Java `OffsetsRequestManager` fail path: NOTHING is parked for retry
        // on a disconnect (unlike a retriable ListOffsets error code).
        assert_eq!(
            mgr.requests_to_retry_count(),
            0,
            "fetch path must NOT re-park on a disconnect — it fails the global result"
        );
    }

    /// Java parity: `testRemoteListOffsetsRequestTimeoutMs`. The built
    /// `ListOffsets` request carries the configured `requestTimeoutMs`
    /// on the wire (Java reads it back from
    /// `unsentRequest.requestBuilder().build().timeoutMs()`).
    #[tokio::test(flavor = "current_thread")]
    async fn fetch_offsets_uses_configured_request_timeout_ms() {
        // Build a manager with custom `request_timeout_ms` so the test
        // can assert the value flows through to the wire request.
        let config = ConsumerConfig::from_properties(&std::collections::HashMap::from([
            ("bootstrap.servers".to_string(), "localhost:9092".to_string()),
            ("group.id".to_string(), "g".to_string()),
        ]))
        .expect("config");
        let subscription_state = Arc::new(Mutex::new(SubscriptionState::new(
            crate::consumer::internals::auto_offset_reset_strategy::AutoOffsetResetStrategy::EARLIEST,
        )));
        let metadata = Arc::new(ConsumerMetadata::from_config(
            &config,
            subscription_state.clone(),
            ClusterResourceListeners::new(),
        ));
        const TEST_REQUEST_TIMEOUT_MS: i64 = 100;
        let mut mgr = OffsetsRequestManager::new(
            subscription_state,
            metadata.clone(),
            IsolationLevel::ReadUncommitted,
            500, // retry_backoff
            TEST_REQUEST_TIMEOUT_MS,
            500, // default_api_timeout
            Arc::new(ApiVersions::new()),
            None,
        );
        bootstrap_metadata_with_topic(&metadata, "t1", 2);

        let tp = TopicPartition::new("t1".to_string(), 1);
        let mut timestamps = HashMap::new();
        timestamps.insert(tp, EARLIEST_TIMESTAMP);
        let _rx = mgr.fetch_offsets(timestamps, false);

        let res = RequestManager::poll(&mut mgr, 0);
        let mut unsent = res.unsent_requests.into_iter().next().expect("one unsent");
        assert_eq!(
            *unsent.request_builder().expect("builder present").api_key(),
            ApiKeys::LIST_OFFSETS
        );
        // Java's `unsentRequest.requestBuilder().build()` returns an
        // `AbstractRequest` that is downcast to `ListOffsetsRequest`. The
        // Rust equivalent is `builder.build()` → `ConcreteRequest::ListOffsets`.
        let built = unsent.request_builder_mut().expect("builder present").build().expect("build");
        let request = match built {
            crate::common::requests::ConcreteRequest::ListOffsets(r) => r,
            other => panic!("expected ListOffsetsRequest, got {other:?}"),
        };
        assert_eq!(request.timeout_ms(), TEST_REQUEST_TIMEOUT_MS as i32);
    }

    // =================================================================
    //   Phase 31: reset-positions / validate-positions / LogTruncation
    //   response-path tests.
    //
    //   Translated from `OffsetsRequestManagerTest` (reset/validate group)
    //   and `OffsetFetcherTest` (KIP-848 reset/validate logic, now living
    //   in `OffsetsRequestManager`). See
    //   `design/history/Milestone-8/Phase-31-test-parity-reset-validate/PLAN.md`
    //   for the full Java→Rust mapping and documented skips.
    // =================================================================

    use crate::consumer::internals::auto_offset_reset_strategy::AutoOffsetResetStrategy;
    use crate::offset_for_leader_epoch_response_data::{
        EpochEndOffset, OffsetForLeaderEpochResponseData, OffsetForLeaderTopicResult,
    };

    /// Bootstrap `metadata` with a single topic / one-partition-per-index
    /// layout, assigning the given `leader_epoch` to every partition via the
    /// epoch supplier. Mirrors Java's
    /// `RequestTestUtils.metadataUpdateWithIds(..., tp -> epoch, ...)` used
    /// by the validate/reset tests that care about leader-epoch progression.
    fn bootstrap_metadata_with_epoch(metadata: &ConsumerMetadata, topic: &str, num_partitions: i32, leader_epoch: i32) {
        metadata.add_transient_topics(HashSet::from([topic.to_string()]));
        let mut counts = HashMap::new();
        counts.insert(topic.to_string(), num_partitions);
        let response = request_test_utils::metadata_update_with_cluster_id(
            "kafka-cluster",
            1,
            &HashMap::new(),
            &counts,
            &|_tp: &TopicPartition| Some(leader_epoch),
        );
        metadata.metadata_arc().update_with_current_request_version(&response, false, 0);
    }

    /// Build a single-topic `OffsetForLeaderEpochResponse` from a map of
    /// `partition -> (error, leader_epoch, end_offset)`. Mirrors Java's
    /// `buildOffsetsForLeaderEpochResponse` /
    /// `buildOffsetsForLeaderEpochResponseWithErrors` helpers.
    ///
    /// Only added because the Phase-31 validate tests call it (clippy runs
    /// with `-D warnings`).
    fn build_offsets_for_leader_epoch_response(
        topic: &str,
        partitions: Vec<(i32, Errors, i32, i64)>,
    ) -> OffsetsForLeaderEpochResponse {
        let mut topic_result = OffsetForLeaderTopicResult::new();
        topic_result.set_topic(topic.to_string());
        let mut parts = Vec::new();
        for (partition_index, error, leader_epoch, end_offset) in partitions {
            let mut eeo = EpochEndOffset::new();
            eeo.set_partition(partition_index);
            eeo.set_error_code(error.code());
            eeo.set_leader_epoch(leader_epoch);
            eeo.set_end_offset(end_offset);
            parts.push(eeo);
        }
        topic_result.set_partitions(parts);
        let mut data = OffsetForLeaderEpochResponseData::new();
        data.set_topics(vec![topic_result]);
        OffsetsForLeaderEpochResponse::new(data)
    }

    /// Wrap an `OffsetsForLeaderEpochResponse` in a `ClientResponse` so the
    /// test can drive `unsent.handler().on_complete(...)`. Mirrors the
    /// ListOffsets helper `build_list_offsets_client_response`.
    fn build_oitle_client_response(response: OffsetsForLeaderEpochResponse) -> ClientResponse {
        let header = RequestHeader::new(
            &ApiKeys::OFFSET_FOR_LEADER_EPOCH,
            ApiKeys::OFFSET_FOR_LEADER_EPOCH.latest_version(),
            "",
            1,
        )
        .expect("header");
        ClientResponse::with_timeout(
            header,
            None,
            "0",
            0,
            0,
            false,
            false,
            None,
            None,
            Some(ConcreteResponse::OffsetsForLeaderEpoch(response)),
        )
    }

    /// Drive the first pending `OffsetsForLeaderEpoch` request on the
    /// manager to completion with the given response, then yield so the
    /// spawned forwarder enqueues the `PendingCompletion`. Mirrors
    /// `complete_first_unsent_with_response` (the ListOffsets analogue).
    async fn complete_first_oitle_with_response(
        mgr: &mut OffsetsRequestManager,
        response: OffsetsForLeaderEpochResponse,
        now_ms: i64,
    ) -> bool {
        let poll_result = RequestManager::poll(mgr, now_ms);
        let Some(unsent) = poll_result.unsent_requests.into_iter().next() else {
            return false;
        };
        unsent.handler().on_complete(build_oitle_client_response(response));
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        true
    }

    /// Assign `tp` to the manager's subscription state and request a reset
    /// with the given strategy (Java: `assignFromUser` +
    /// `subscriptions.requestOffsetReset(tp, strategy)`).
    fn assign_and_request_reset(
        subscription_state: &Arc<Mutex<SubscriptionState>>,
        tp: &TopicPartition,
        strategy: AutoOffsetResetStrategy,
    ) {
        let mut subs = subscription_state.lock().expect("subs");
        subs.assign_from_user(HashSet::from([tp.clone()])).expect("assign");
        subs.request_offset_reset(tp, strategy).expect("request reset");
    }

    // -----------------------------------------------------------------
    //   reset-positions: missing-leader / success / auth-failure
    //   (OffsetsRequestManagerTest)
    // -----------------------------------------------------------------

    /// Java parity: `testResetPositionsMissingLeader`. A partition needs
    /// reset but its leader is unknown — the manager requests a metadata
    /// update (`metadata.requestUpdate(true)`) and enqueues no request.
    #[tokio::test(flavor = "current_thread")]
    async fn reset_positions_missing_leader() {
        let (mut mgr, _commit_rm, subscription_state) = new_manager_with_commit();
        let tp = TopicPartition::new("t1".to_string(), 1);
        // No metadata bootstrap: leader is unknown.
        assign_and_request_reset(&subscription_state, &tp, AutoOffsetResetStrategy::EARLIEST);

        let before = mgr.shared.metadata.metadata_arc().update_requested();
        mgr.reset_positions_if_needed(0).expect("ok");
        assert_eq!(mgr.requests_to_send_count(), 0, "no request when leader unknown");
        assert!(
            !before && mgr.shared.metadata.metadata_arc().update_requested(),
            "missing leader must trigger metadata.requestUpdate(true)"
        );
    }

    /// Java parity: `testResetPositionsSuccess_NoLeaderEpochInResponse` and
    /// `testUpdateFetchPositionResetToEarliestOffset` /
    /// `testListOffsetNoUpdateMissingEpoch`. Reset to EARLIEST with a
    /// response that carries no leader epoch (`UNKNOWN_EPOCH`) — the
    /// position is set, reset is no longer needed, the partition becomes
    /// fetchable, and `updateLastSeenEpochIfNewer` is NOT called (the
    /// metadata last-seen epoch stays absent).
    #[tokio::test(flavor = "current_thread")]
    async fn reset_positions_success_no_leader_epoch_in_response() {
        let (mut mgr, _commit_rm, subscription_state) = new_manager_with_commit();
        bootstrap_metadata_with_topic(&mgr.shared.metadata, "t1", 2);
        let tp = TopicPartition::new("t1".to_string(), 1);
        assign_and_request_reset(&subscription_state, &tp, AutoOffsetResetStrategy::EARLIEST);

        mgr.reset_positions_if_needed(0).expect("ok");
        assert_eq!(mgr.requests_to_send_count(), 1, "one ListOffsets request expected");

        // Response with offset 5 and no leader epoch.
        let response = build_list_offsets_response("t1", vec![(1, Errors::None, 1, 5, UNKNOWN_EPOCH)]);
        assert!(complete_first_unsent_with_response(&mut mgr, response, 0).await);
        // Drain the completion.
        let _ = RequestManager::poll(&mut mgr, 0);

        let subs = subscription_state.lock().expect("subs");
        assert!(!subs.is_offset_reset_needed(&tp).expect("assigned"), "reset no longer needed");
        assert!(subs.has_valid_position(&tp), "position must be valid after reset");
        assert!(subs.is_fetchable(&tp), "partition must be fetchable after reset");
        assert_eq!(subs.position(&tp).expect("lookup").expect("present").offset, 5);
        drop(subs);

        // No leader epoch in the response ⇒ metadata last-seen epoch absent.
        assert_eq!(
            mgr.shared.metadata.metadata_arc().last_seen_leader_epoch(&tp),
            None,
            "updateLastSeenEpochIfNewer must NOT have been called",
        );
    }

    /// Java parity: `testResetPositionsSuccess_LeaderEpochInResponse` and
    /// `testListOffsetUpdateEpoch`. Reset response carries a higher leader
    /// epoch — `updateLastSeenEpochIfNewer(tp, epoch)` is called, bumping
    /// the metadata last-seen epoch.
    #[tokio::test(flavor = "current_thread")]
    async fn reset_positions_success_leader_epoch_in_response() {
        let (mut mgr, _commit_rm, subscription_state) = new_manager_with_commit();
        // Bootstrap with leader epoch 1 so the response's epoch 5 is newer.
        bootstrap_metadata_with_epoch(&mgr.shared.metadata, "t1", 2, 1);
        let tp = TopicPartition::new("t1".to_string(), 1);
        assign_and_request_reset(&subscription_state, &tp, AutoOffsetResetStrategy::EARLIEST);

        mgr.reset_positions_if_needed(0).expect("ok");
        assert_eq!(mgr.requests_to_send_count(), 1);

        // Response with offset 5 and leader epoch 5.
        let response = build_list_offsets_response("t1", vec![(1, Errors::None, 1, 5, 5)]);
        assert!(complete_first_unsent_with_response(&mut mgr, response, 0).await);
        let _ = RequestManager::poll(&mut mgr, 0);

        let subs = subscription_state.lock().expect("subs");
        assert!(!subs.is_offset_reset_needed(&tp).expect("assigned"));
        assert_eq!(subs.position(&tp).expect("lookup").expect("present").offset, 5);
        drop(subs);

        assert_eq!(
            mgr.shared.metadata.metadata_arc().last_seen_leader_epoch(&tp),
            Some(5),
            "updateLastSeenEpochIfNewer(tp, 5) must have bumped the metadata epoch",
        );
    }

    /// Java parity: `testResetOffsetsAuthorizationFailure`. A reset response
    /// carrying `TOPIC_AUTHORIZATION_FAILED` is cached (non-retriable) and
    /// re-raised on the next `reset_positions_if_needed` call without
    /// issuing any request.
    #[tokio::test(flavor = "current_thread")]
    async fn reset_offsets_authorization_failure() {
        let (mut mgr, _commit_rm, subscription_state) = new_manager_with_commit();
        bootstrap_metadata_with_topic(&mgr.shared.metadata, "t1", 2);
        let tp = TopicPartition::new("t1".to_string(), 1);
        assign_and_request_reset(&subscription_state, &tp, AutoOffsetResetStrategy::EARLIEST);

        mgr.reset_positions_if_needed(0).expect("ok");
        assert_eq!(mgr.requests_to_send_count(), 1);

        let before_update_requested = mgr.shared.metadata.metadata_arc().update_requested();
        let response =
            build_list_offsets_response("t1", vec![(1, Errors::TopicAuthorizationFailed, -1, -1, UNKNOWN_EPOCH)]);
        assert!(complete_first_unsent_with_response(&mut mgr, response, 0).await);
        let _ = RequestManager::poll(&mut mgr, 0);

        // Java error-path side effects (verify(...) in the ORM test):
        //   verify(subscriptionState).requestFailed(any(), anyLong());
        //   verify(metadata).requestUpdate(false);
        // `requestFailed` advances the partition's retry backoff: at the same
        // instant the response was handled (now_ms = 0) the partition is no
        // longer reset-ready, even though it is still awaiting reset.
        {
            let subs = subscription_state.lock().expect("subs");
            assert!(
                subs.is_offset_reset_needed(&tp).expect("assigned"),
                "partition still awaiting reset after auth failure"
            );
            assert!(
                !subs.partitions_needing_reset(0).contains(&tp),
                "requestFailed must have advanced the retry backoff (partition not reset-ready now)"
            );
        }
        // `metadata.requestUpdate(false)` requests a full metadata update.
        assert!(
            !before_update_requested && mgr.shared.metadata.metadata_arc().update_requested(),
            "auth-failure reset path must request a metadata update (requestUpdate(false))"
        );

        // Following resetPositions should re-raise the cached exception
        // and issue no request.
        let err = mgr.reset_positions_if_needed(0).expect_err("cached auth error re-raised");
        assert_eq!(mgr.requests_to_send_count(), 0, "no request issued on cached-error path");
        // The cached error is the topic-authorization failure (DoD §3:
        // assert message content, not just is_err()).
        assert!(
            matches!(err, KafkaError::TopicAuthorization(_)),
            "expected TopicAuthorization, got {err:?}",
        );
        assert_eq!(err.error(), Errors::TopicAuthorizationFailed);
    }

    // -----------------------------------------------------------------
    //   validate-positions: success / missing-leader / auth-failure
    //   (OffsetsRequestManagerTest)
    // -----------------------------------------------------------------

    /// Seek `tp` (assigned, leader = node 0 from the metadata bootstrap)
    /// into AWAITING_VALIDATION at the given offset/epoch, and install a
    /// modern `NodeApiVersions` for node 0 so the OffsetsForLeaderEpoch
    /// request is buildable. Mirrors the Java validate-test fixture
    /// (`seekUnvalidated` + `apiVersions.update(node, NodeApiVersions.create())`).
    fn seek_unvalidated_and_install_api_versions(
        mgr: &OffsetsRequestManager,
        subscription_state: &Arc<Mutex<SubscriptionState>>,
        tp: &TopicPartition,
        offset: i64,
        epoch: i32,
    ) {
        let leader = crate::common::Node::new(0, "localhost".to_string(), 1969);
        let leader_and_epoch = LeaderAndEpoch::new(Some(leader.clone()), Some(epoch));
        let position = FetchPosition::with_leader(offset, Some(epoch), leader_and_epoch);
        {
            let mut subs = subscription_state.lock().expect("subs");
            subs.assign_from_user(HashSet::from([tp.clone()])).expect("assign");
            subs.seek_unvalidated(tp, position).expect("seek_unvalidated");
        }
        mgr.api_versions.update(leader.id_string(), crate::NodeApiVersions::create());
    }

    /// Java parity: `testValidatePositionsSuccess`. A partition awaiting
    /// validation produces one OffsetsForLeaderEpoch request; a successful
    /// response (end offset ≥ position, defined epoch) completes the
    /// validation — the partition is no longer awaiting validation and its
    /// position becomes valid/fetchable.
    #[tokio::test(flavor = "current_thread")]
    async fn validate_positions_success() {
        let (mut mgr, _commit_rm, subscription_state) = new_manager_with_commit();
        bootstrap_metadata_with_epoch(&mgr.shared.metadata, "t1", 2, 3);
        let tp = TopicPartition::new("t1".to_string(), 1);
        seek_unvalidated_and_install_api_versions(&mgr, &subscription_state, &tp, 5, 3);

        mgr.validate_positions_if_needed(0).expect("ok");
        assert_eq!(mgr.requests_to_send_count(), 1, "one OffsetsForLeaderEpoch request expected");
        {
            let subs = subscription_state.lock().expect("subs");
            assert!(
                subs.awaiting_validation(&tp).expect("assigned"),
                "awaiting validation before response"
            );
        }

        // Validate response with a non-divergent end offset (100 > 5) and a
        // defined leader epoch (3).
        let response = build_offsets_for_leader_epoch_response("t1", vec![(1, Errors::None, 3, 100)]);
        assert!(complete_first_oitle_with_response(&mut mgr, response, 0).await);
        let _ = RequestManager::poll(&mut mgr, 0);

        let subs = subscription_state.lock().expect("subs");
        assert!(
            !subs.awaiting_validation(&tp).expect("assigned"),
            "maybe_complete_validation must clear the AWAITING_VALIDATION state"
        );
        assert!(
            subs.has_valid_position(&tp),
            "position must be valid after successful validation"
        );
        assert!(subs.is_fetchable(&tp), "partition must be fetchable after validation");
        assert_eq!(subs.position(&tp).expect("lookup").expect("present").offset, 5);
    }

    /// Java parity: `testValidatePositionsMissingLeader`. The partition
    /// awaiting validation has a no-node leader — the manager requests a
    /// metadata update (`metadata.requestUpdate(true)`) and enqueues no
    /// request.
    #[tokio::test(flavor = "current_thread")]
    async fn validate_positions_missing_leader() {
        let (mut mgr, _commit_rm, subscription_state) = new_manager_with_commit();
        let tp = TopicPartition::new("t1".to_string(), 1);
        // Seek with a NO-NODE leader (Node::no_node) but a defined epoch,
        // mirroring Java's `new LeaderAndEpoch(Optional.of(Node.noNode()),
        // Optional.of(5))`.
        let no_node = crate::common::Node::no_node().clone();
        let leader_and_epoch = LeaderAndEpoch::new(Some(no_node.clone()), Some(5));
        let position = FetchPosition::with_leader(5, Some(10), leader_and_epoch);
        {
            let mut subs = subscription_state.lock().expect("subs");
            subs.assign_from_user(HashSet::from([tp.clone()])).expect("assign");
            subs.seek_unvalidated(&tp, position).expect("seek_unvalidated");
        }
        // Install api versions for the no-node id so the only reason the
        // request is not built is the missing (no-node) leader.
        mgr.api_versions.update(no_node.id_string(), crate::NodeApiVersions::create());

        let before = mgr.shared.metadata.metadata_arc().update_requested();
        mgr.validate_positions_if_needed(0).expect("ok");
        assert_eq!(mgr.requests_to_send_count(), 0, "no request when leader is no-node");
        assert!(
            !before && mgr.shared.metadata.metadata_arc().update_requested(),
            "no-node leader must trigger metadata.requestUpdate(true)"
        );
    }

    /// Java parity: `testValidatePositionsFailureWithUnrecoverableAuthException`.
    /// A validate response carrying `TOPIC_AUTHORIZATION_FAILED` is cached
    /// (non-retriable) and re-raised on the next `validate_positions_if_needed`
    /// call without issuing any request.
    #[tokio::test(flavor = "current_thread")]
    async fn validate_positions_failure_with_unrecoverable_auth_exception() {
        let (mut mgr, _commit_rm, subscription_state) = new_manager_with_commit();
        bootstrap_metadata_with_epoch(&mgr.shared.metadata, "t1", 2, 5);
        let tp = TopicPartition::new("t1".to_string(), 1);
        seek_unvalidated_and_install_api_versions(&mgr, &subscription_state, &tp, 5, 5);

        mgr.validate_positions_if_needed(0).expect("ok");
        assert_eq!(mgr.requests_to_send_count(), 1);

        let response = build_offsets_for_leader_epoch_response("t1", vec![(1, Errors::TopicAuthorizationFailed, 0, 0)]);
        assert!(complete_first_oitle_with_response(&mut mgr, response, 0).await);
        let _ = RequestManager::poll(&mut mgr, 0);

        // Following validatePositions should re-raise the cached exception.
        let err = mgr.validate_positions_if_needed(0).expect_err("cached auth error re-raised");
        assert_eq!(mgr.requests_to_send_count(), 0, "no request issued on cached-error path");
        assert!(
            matches!(err, KafkaError::TopicAuthorization(_)),
            "expected TopicAuthorization, got {err:?}",
        );
        assert_eq!(err.error(), Errors::TopicAuthorizationFailed);
    }

    // -----------------------------------------------------------------
    //   OffsetValidation → LogTruncation end-to-end re-raise
    //   (OffsetFetcherTest.testOffsetValidationTriggerLogTruncation...)
    // -----------------------------------------------------------------

    /// Build a manager whose subscription state uses the NONE reset policy,
    /// so a validate truncation surfaces as a LogTruncation instead of a
    /// reset. Mirrors Java's `buildFetcher(AutoOffsetResetStrategy.NONE)`.
    fn new_manager_none_reset() -> (OffsetsRequestManager, Arc<Mutex<SubscriptionState>>) {
        let config = ConsumerConfig::from_properties(&std::collections::HashMap::from([
            ("bootstrap.servers".to_string(), "localhost:9092".to_string()),
            ("group.id".to_string(), "g".to_string()),
        ]))
        .expect("config");
        let subscription_state = Arc::new(Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::NONE)));
        let metadata = Arc::new(ConsumerMetadata::from_config(
            &config,
            subscription_state.clone(),
            ClusterResourceListeners::new(),
        ));
        let mgr = OffsetsRequestManager::new(
            subscription_state.clone(),
            metadata,
            IsolationLevel::ReadUncommitted,
            500,
            30_000,
            60_000,
            Arc::new(ApiVersions::new()),
            None,
        );
        (mgr, subscription_state)
    }

    /// Java parity:
    /// `testOffsetValidationTriggerLogTruncationForBadOffsetWithUndefinedResetPolicy`
    /// (end-to-end re-raise half). With the NONE reset policy, a validate
    /// response carrying a bad end offset (1 < position 5) caches a
    /// LogTruncation; the next `validate_positions_if_needed` re-raises it.
    /// The structured payload is asserted at the OFU level
    /// (`validation_bad_offset_with_undefined_reset_policy_log_truncation`);
    /// here we assert the end-to-end re-raise carries the truncation message
    /// (DoD §3).
    #[tokio::test(flavor = "current_thread")]
    async fn validation_bad_offset_triggers_log_truncation_reraise() {
        let (mut mgr, subscription_state) = new_manager_none_reset();
        bootstrap_metadata_with_epoch(&mgr.shared.metadata, "t1", 2, 1);
        let tp = TopicPartition::new("t1".to_string(), 1);
        seek_unvalidated_and_install_api_versions(&mgr, &subscription_state, &tp, 5, 1);

        mgr.validate_positions_if_needed(0).expect("ok");
        assert_eq!(mgr.requests_to_send_count(), 1);

        // Bad end offset (1) with defined leader epoch (1) → truncation.
        let response = build_offsets_for_leader_epoch_response("t1", vec![(1, Errors::None, 1, 1)]);
        assert!(complete_first_oitle_with_response(&mut mgr, response, 0).await);
        let _ = RequestManager::poll(&mut mgr, 0);

        // The partition stays awaiting validation; the error is cached.
        {
            let subs = subscription_state.lock().expect("subs");
            assert!(
                subs.awaiting_validation(&tp).expect("assigned"),
                "partition must stay AWAITING_VALIDATION on truncation"
            );
        }

        // Next validate call re-raises the LogTruncation. The conversion
        // flattens to KafkaError::IllegalState carrying the truncation
        // Display string (the structured payload is verified at OFU level).
        let err = mgr.validate_positions_if_needed(0).expect_err("LogTruncation re-raised");
        assert_eq!(mgr.requests_to_send_count(), 0, "no request on cached-error path");
        assert!(
            err.message().contains("Truncated partitions detected with divergent offsets"),
            "re-raised error must carry the LogTruncation message, got: {}",
            err.message(),
        );
    }

    // -----------------------------------------------------------------
    //   reset behavioral family (OffsetFetcherTest — KIP-848 logic now
    //   in OffsetsRequestManager). assign + requestOffsetReset →
    //   resetPositionsIfNeeded → response → SubscriptionState asserts.
    // -----------------------------------------------------------------

    /// Drive a reset for `tp` with `strategy` against a single-partition
    /// metadata bootstrap, complete the ListOffsets request with `response`,
    /// drain, and return the manager + subscription state for assertions.
    async fn drive_reset(
        strategy: AutoOffsetResetStrategy,
        response: ListOffsetsResponse,
    ) -> (OffsetsRequestManager, Arc<Mutex<SubscriptionState>>, TopicPartition) {
        let (mut mgr, _commit_rm, subscription_state) = new_manager_with_commit();
        bootstrap_metadata_with_topic(&mgr.shared.metadata, "t1", 2);
        let tp = TopicPartition::new("t1".to_string(), 1);
        assign_and_request_reset(&subscription_state, &tp, strategy);
        mgr.reset_positions_if_needed(0).expect("ok");
        assert_eq!(mgr.requests_to_send_count(), 1);
        assert!(complete_first_unsent_with_response(&mut mgr, response, 0).await);
        let _ = RequestManager::poll(&mut mgr, 0);
        (mgr, subscription_state, tp)
    }

    /// Assert the canonical "reset succeeded to offset 5" post-state.
    fn assert_reset_to_5(subscription_state: &Arc<Mutex<SubscriptionState>>, tp: &TopicPartition) {
        let subs = subscription_state.lock().expect("subs");
        assert!(!subs.is_offset_reset_needed(tp).expect("assigned"), "reset no longer needed");
        assert!(subs.is_fetchable(tp), "fetchable after reset");
        assert!(subs.has_valid_position(tp), "valid position after reset");
        assert_eq!(subs.position(tp).expect("lookup").expect("present").offset, 5);
    }

    /// Java parity: `testUpdateFetchPositionResetToEarliestOffset`.
    #[tokio::test(flavor = "current_thread")]
    async fn reset_to_earliest_offset() {
        let response = build_list_offsets_response("t1", vec![(1, Errors::None, 1, 5, UNKNOWN_EPOCH)]);
        let (mgr, subscription_state, tp) = drive_reset(AutoOffsetResetStrategy::EARLIEST, response).await;
        assert_reset_to_5(&subscription_state, &tp);
        // EARLIEST timestamp on the wire is exercised by
        // reset_list_offset_sends_read_uncommitted; here assert the request
        // was the only one and consumed.
        assert_eq!(mgr.requests_to_send_count(), 0);
    }

    /// Java parity: `testUpdateFetchPositionResetToLatestOffset`.
    #[tokio::test(flavor = "current_thread")]
    async fn reset_to_latest_offset() {
        let response = build_list_offsets_response("t1", vec![(1, Errors::None, 1, 5, UNKNOWN_EPOCH)]);
        let (_mgr, subscription_state, tp) = drive_reset(AutoOffsetResetStrategy::LATEST, response).await;
        assert_reset_to_5(&subscription_state, &tp);
    }

    /// Java parity: `testUpdateFetchPositionResetToDefaultOffset`
    /// (`requestOffsetReset(tp)` with no explicit strategy → the
    /// subscription's default, EARLIEST here).
    #[tokio::test(flavor = "current_thread")]
    async fn reset_to_default_offset() {
        let (mut mgr, _commit_rm, subscription_state) = new_manager_with_commit();
        bootstrap_metadata_with_topic(&mgr.shared.metadata, "t1", 2);
        let tp = TopicPartition::new("t1".to_string(), 1);
        {
            let mut subs = subscription_state.lock().expect("subs");
            subs.assign_from_user(HashSet::from([tp.clone()])).expect("assign");
            // Default reset (Java: subscriptions.requestOffsetReset(tp0)).
            subs.request_offset_reset_default(&tp).expect("reset default");
        }
        mgr.reset_positions_if_needed(0).expect("ok");
        assert_eq!(mgr.requests_to_send_count(), 1);
        let response = build_list_offsets_response("t1", vec![(1, Errors::None, 1, 5, UNKNOWN_EPOCH)]);
        assert!(complete_first_unsent_with_response(&mut mgr, response, 0).await);
        let _ = RequestManager::poll(&mut mgr, 0);
        assert_reset_to_5(&subscription_state, &tp);
    }

    /// Java parity: `testUpdateFetchPositionResetToDurationOffset`. A
    /// by-timestamp (duration) reset strategy resolves to a positive wire
    /// timestamp; the response resets the position the same way.
    #[tokio::test(flavor = "current_thread")]
    async fn reset_to_duration_offset() {
        let (mut mgr, _commit_rm, subscription_state) = new_manager_with_commit();
        bootstrap_metadata_with_topic(&mgr.shared.metadata, "t1", 2);
        let tp = TopicPartition::new("t1".to_string(), 1);
        // A by-duration strategy (Java's mocked durationStrategy) resolves
        // to a concrete epoch-millis timestamp (Some), so it produces a
        // request — unlike EARLIEST/LATEST which use sentinel timestamps.
        let duration_strategy = AutoOffsetResetStrategy::from_string("by_duration:PT1H").expect("duration strategy");
        {
            let mut subs = subscription_state.lock().expect("subs");
            subs.assign_from_user(HashSet::from([tp.clone()])).expect("assign");
            subs.request_offset_reset(&tp, duration_strategy.clone()).expect("reset");
        }
        // Sanity: the strategy yields a concrete (Some) timestamp.
        assert!(duration_strategy.timestamp().is_some());
        mgr.reset_positions_if_needed(0).expect("ok");
        assert_eq!(mgr.requests_to_send_count(), 1);
        let response = build_list_offsets_response("t1", vec![(1, Errors::None, 1, 5, UNKNOWN_EPOCH)]);
        assert!(complete_first_unsent_with_response(&mut mgr, response, 0).await);
        let _ = RequestManager::poll(&mut mgr, 0);
        assert_reset_to_5(&subscription_state, &tp);
    }

    /// Java parity: `testListOffsetSendsReadUncommitted` /
    /// `testListOffsetSendsReadCommitted`. The isolation level configured on
    /// the manager flows through to the ListOffsets request built for a
    /// reset, and the request timeout matches `request_timeout_ms`.
    async fn reset_list_offset_sends_isolation_level(isolation_level: IsolationLevel) {
        let config = ConsumerConfig::from_properties(&std::collections::HashMap::from([
            ("bootstrap.servers".to_string(), "localhost:9092".to_string()),
            ("group.id".to_string(), "g".to_string()),
        ]))
        .expect("config");
        let subscription_state = Arc::new(Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::EARLIEST)));
        let metadata = Arc::new(ConsumerMetadata::from_config(
            &config,
            subscription_state.clone(),
            ClusterResourceListeners::new(),
        ));
        const TEST_REQUEST_TIMEOUT_MS: i64 = 100;
        let mut mgr = OffsetsRequestManager::new(
            subscription_state.clone(),
            metadata.clone(),
            isolation_level,
            500,
            TEST_REQUEST_TIMEOUT_MS,
            60_000,
            Arc::new(ApiVersions::new()),
            None,
        );
        bootstrap_metadata_with_topic(&metadata, "t1", 2);
        let tp = TopicPartition::new("t1".to_string(), 1);
        assign_and_request_reset(&subscription_state, &tp, AutoOffsetResetStrategy::LATEST);

        mgr.reset_positions_if_needed(0).expect("ok");
        let res = RequestManager::poll(&mut mgr, 0);
        let mut unsent = res.unsent_requests.into_iter().next().expect("one unsent");
        let built = unsent.request_builder_mut().expect("builder").build().expect("build");
        let request = match built {
            crate::common::requests::ConcreteRequest::ListOffsets(r) => r,
            other => panic!("expected ListOffsetsRequest, got {other:?}"),
        };
        // Java: assertEquals(requestTimeoutMs, request.timeoutMs()).
        assert_eq!(request.timeout_ms(), TEST_REQUEST_TIMEOUT_MS as i32);
        // Java: request.isolationLevel() == isolationLevel.
        assert_eq!(request.isolation_level().expect("isolation level"), isolation_level);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn reset_list_offset_sends_read_uncommitted() {
        reset_list_offset_sends_isolation_level(IsolationLevel::ReadUncommitted).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn reset_list_offset_sends_read_committed() {
        reset_list_offset_sends_isolation_level(IsolationLevel::ReadCommitted).await;
    }

    /// Java parity: `testGetOffsetsIncludesLeaderEpoch`. The ListOffsets
    /// request built for a reset carries `currentLeaderEpoch` taken from the
    /// metadata (99 here), not the `UNKNOWN_EPOCH` sentinel. This is the
    /// behavior the Phase-31 production fix restored.
    #[tokio::test(flavor = "current_thread")]
    async fn reset_request_includes_current_leader_epoch() {
        let (mut mgr, _commit_rm, subscription_state) = new_manager_with_commit();
        // Metadata with leader epoch 99.
        bootstrap_metadata_with_epoch(&mgr.shared.metadata, "t1", 2, 99);
        let tp = TopicPartition::new("t1".to_string(), 1);
        {
            let mut subs = subscription_state.lock().expect("subs");
            subs.assign_from_user(HashSet::from([tp.clone()])).expect("assign");
            subs.request_offset_reset_default(&tp).expect("reset default");
        }
        mgr.reset_positions_if_needed(0).expect("ok");
        let res = RequestManager::poll(&mut mgr, 0);
        let mut unsent = res.unsent_requests.into_iter().next().expect("one unsent");
        let built = unsent.request_builder_mut().expect("builder").build().expect("build");
        let request = match built {
            crate::common::requests::ConcreteRequest::ListOffsets(r) => r,
            other => panic!("expected ListOffsetsRequest, got {other:?}"),
        };
        let epoch = request.topics()[0].partitions[0].current_leader_epoch;
        assert_ne!(epoch, UNKNOWN_EPOCH, "expected leader epoch set in request");
        assert_eq!(epoch, 99, "expected leader epoch to match metadata epoch");
    }

    /// Java parity: `testGetOffsetsFencedLeaderEpoch`. A reset response with
    /// `FENCED_LEADER_EPOCH` (retriable) leaves the partition still needing
    /// reset, not fetchable, without a valid position, and triggers a
    /// metadata update (Java: `timeToNextUpdate == 0`).
    #[tokio::test(flavor = "current_thread")]
    async fn reset_fenced_leader_epoch_still_needs_reset() {
        let response = build_list_offsets_response("t1", vec![(1, Errors::FencedLeaderEpoch, -1, -1, UNKNOWN_EPOCH)]);
        let (mgr, subscription_state, tp) = drive_reset(AutoOffsetResetStrategy::LATEST, response).await;
        let subs = subscription_state.lock().expect("subs");
        assert!(
            subs.is_offset_reset_needed(&tp).expect("assigned"),
            "reset still needed after fenced epoch"
        );
        assert!(!subs.is_fetchable(&tp), "not fetchable");
        assert!(!subs.has_valid_position(&tp), "no valid position");
        drop(subs);
        // Java asserts metadata.timeToNextUpdate == 0 (update requested).
        assert!(
            mgr.shared.metadata.metadata_arc().update_requested(),
            "retriable reset error must request a metadata update"
        );
    }

    /// Java parity: `testFetchOffsetErrors`. Retriable errors
    /// (OFFSET_NOT_AVAILABLE, then LEADER_NOT_AVAILABLE) leave the partition
    /// needing reset / not fetchable; the third attempt (NONE) succeeds.
    /// The retry-backoff sleep between attempts is modeled by advancing
    /// `now_ms` past `set_next_allowed_retry`.
    #[tokio::test(flavor = "current_thread")]
    async fn reset_fetch_offset_errors_then_recovers() {
        let (mut mgr, _commit_rm, subscription_state) = new_manager_with_commit();
        bootstrap_metadata_with_topic(&mgr.shared.metadata, "t1", 2);
        let tp = TopicPartition::new("t1".to_string(), 1);
        assign_and_request_reset(&subscription_state, &tp, AutoOffsetResetStrategy::LATEST);

        // retry_backoff_ms == 500 (new_manager_with_commit), request_timeout
        // 30_000 → set_next_allowed_retry pushes the partition's allowed
        // retry to now + 30_000 on each send.
        let mut now = 0i64;

        // Attempt 1: OFFSET_NOT_AVAILABLE.
        mgr.reset_positions_if_needed(now).expect("ok");
        assert_eq!(mgr.requests_to_send_count(), 1);
        let r1 = build_list_offsets_response("t1", vec![(1, Errors::OffsetNotAvailable, -1, -1, UNKNOWN_EPOCH)]);
        assert!(complete_first_unsent_with_response(&mut mgr, r1, now).await);
        let _ = RequestManager::poll(&mut mgr, now);
        {
            let subs = subscription_state.lock().expect("subs");
            assert!(!subs.has_valid_position(&tp));
            assert!(subs.is_offset_reset_needed(&tp).expect("assigned"));
            assert!(!subs.is_fetchable(&tp));
        }

        // Attempt 2: LEADER_NOT_AVAILABLE (after backoff window passes).
        now += 60_000;
        mgr.reset_positions_if_needed(now).expect("ok");
        assert_eq!(mgr.requests_to_send_count(), 1, "second attempt issued after backoff");
        let r2 = build_list_offsets_response("t1", vec![(1, Errors::LeaderNotAvailable, -1, -1, UNKNOWN_EPOCH)]);
        assert!(complete_first_unsent_with_response(&mut mgr, r2, now).await);
        let _ = RequestManager::poll(&mut mgr, now);
        {
            let subs = subscription_state.lock().expect("subs");
            assert!(!subs.has_valid_position(&tp));
            assert!(subs.is_offset_reset_needed(&tp).expect("assigned"));
        }

        // Attempt 3: success.
        now += 60_000;
        mgr.reset_positions_if_needed(now).expect("ok");
        assert_eq!(mgr.requests_to_send_count(), 1);
        let r3 = build_list_offsets_response("t1", vec![(1, Errors::None, 1, 5, UNKNOWN_EPOCH)]);
        assert!(complete_first_unsent_with_response(&mut mgr, r3, now).await);
        let _ = RequestManager::poll(&mut mgr, now);
        let subs = subscription_state.lock().expect("subs");
        assert!(subs.has_valid_position(&tp));
        assert!(!subs.is_offset_reset_needed(&tp).expect("assigned"));
        assert!(subs.is_fetchable(&tp));
        assert_eq!(subs.position(&tp).expect("lookup").expect("present").offset, 5);
    }

    /// Java parity: `testUpdateFetchPositionDisconnect`. A disconnected
    /// reset response (`build_disconnected_client_response`) re-parks the
    /// partition (no valid position); a later attempt after backoff
    /// succeeds.
    #[tokio::test(flavor = "current_thread")]
    async fn reset_disconnect_reparks_and_retries() {
        let (mut mgr, _commit_rm, subscription_state) = new_manager_with_commit();
        bootstrap_metadata_with_topic(&mgr.shared.metadata, "t1", 2);
        let tp = TopicPartition::new("t1".to_string(), 1);
        assign_and_request_reset(&subscription_state, &tp, AutoOffsetResetStrategy::LATEST);

        let mut now = 0i64;
        mgr.reset_positions_if_needed(now).expect("ok");
        let poll_result = RequestManager::poll(&mut mgr, now);
        let unsent = poll_result.unsent_requests.into_iter().next().expect("one unsent");
        // Plain disconnect (disconnected=true, no authentication exception)
        // ⇒ retriable NetworkException (Java's DisconnectException). The
        // shared `build_disconnected_client_response` helper carries an auth
        // exception, which would map to a non-retriable
        // SaslAuthenticationFailed; build a clean disconnect inline instead.
        let disconnect_header =
            RequestHeader::new(&ApiKeys::LIST_OFFSETS, ApiKeys::LIST_OFFSETS.latest_version(), "", 1).expect("header");
        let disconnect_response = ClientResponse::with_timeout(
            disconnect_header,
            None,
            "0",
            0,
            0,
            true, // disconnected
            false,
            None,
            None, // no authentication exception
            None,
        );
        unsent.handler().on_complete(disconnect_response);
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        let _ = RequestManager::poll(&mut mgr, now);
        {
            let subs = subscription_state.lock().expect("subs");
            assert!(!subs.has_valid_position(&tp), "disconnect leaves no valid position");
        }

        // After the backoff window, the next attempt succeeds.
        now += 60_000;
        mgr.reset_positions_if_needed(now).expect("ok");
        assert_eq!(mgr.requests_to_send_count(), 1, "retry issued after backoff");
        let r = build_list_offsets_response("t1", vec![(1, Errors::None, 1, 5, UNKNOWN_EPOCH)]);
        assert!(complete_first_unsent_with_response(&mut mgr, r, now).await);
        let _ = RequestManager::poll(&mut mgr, now);
        assert_reset_to_5(&subscription_state, &tp);
    }

    /// Java parity: `testUpdateFetchPositionOfPausedPartitionsRequiringOffsetReset`.
    /// A reset completes for a paused partition: it gets a valid position
    /// and no longer needs reset, but is NOT fetchable because it is paused.
    #[tokio::test(flavor = "current_thread")]
    async fn reset_on_paused_partition_completes_but_not_fetchable() {
        let (mut mgr, _commit_rm, subscription_state) = new_manager_with_commit();
        bootstrap_metadata_with_topic(&mgr.shared.metadata, "t1", 2);
        let tp = TopicPartition::new("t1".to_string(), 1);
        {
            let mut subs = subscription_state.lock().expect("subs");
            subs.assign_from_user(HashSet::from([tp.clone()])).expect("assign");
            subs.pause(&tp).expect("pause");
            subs.request_offset_reset(&tp, AutoOffsetResetStrategy::LATEST).expect("reset");
        }
        mgr.reset_positions_if_needed(0).expect("ok");
        assert_eq!(mgr.requests_to_send_count(), 1);
        let response = build_list_offsets_response("t1", vec![(1, Errors::None, 1, 10, UNKNOWN_EPOCH)]);
        assert!(complete_first_unsent_with_response(&mut mgr, response, 0).await);
        let _ = RequestManager::poll(&mut mgr, 0);

        let subs = subscription_state.lock().expect("subs");
        assert!(!subs.is_offset_reset_needed(&tp).expect("assigned"), "reset satisfied");
        assert!(!subs.is_fetchable(&tp), "paused partition is not fetchable");
        assert!(subs.has_valid_position(&tp), "paused partition still has a valid position");
        assert_eq!(subs.position(&tp).expect("lookup").expect("present").offset, 10);
    }

    /// Java parity: `testSeekWithInFlightReset`. A user `seek` arrives while
    /// a reset response is in flight; the response is discarded
    /// (`maybe_seek_unvalidated` skips because the partition is no longer
    /// AWAITING_RESET) and the seeked position (237) wins.
    #[tokio::test(flavor = "current_thread")]
    async fn reset_seek_with_in_flight_reset_discards_stale_response() {
        let (mut mgr, _commit_rm, subscription_state) = new_manager_with_commit();
        bootstrap_metadata_with_topic(&mgr.shared.metadata, "t1", 2);
        let tp = TopicPartition::new("t1".to_string(), 1);
        assign_and_request_reset(&subscription_state, &tp, AutoOffsetResetStrategy::LATEST);

        mgr.reset_positions_if_needed(0).expect("ok");
        let poll_result = RequestManager::poll(&mut mgr, 0);
        let unsent = poll_result.unsent_requests.into_iter().next().expect("one unsent");

        // User seek arrives while the reset is in flight.
        {
            let mut subs = subscription_state.lock().expect("subs");
            subs.seek(&tp, 237).expect("seek");
        }

        // The reset response returns and is discarded.
        let response = build_list_offsets_response("t1", vec![(1, Errors::None, 1, 5, UNKNOWN_EPOCH)]);
        unsent.handler().on_complete(build_list_offsets_client_response(response));
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        let _ = RequestManager::poll(&mut mgr, 0);

        let subs = subscription_state.lock().expect("subs");
        assert_eq!(
            subs.position(&tp).expect("lookup").expect("present").offset,
            237,
            "seeked position must win; stale reset response discarded"
        );
    }

    /// Java parity: `testIdempotentResetWithInFlightReset`. A second reset
    /// request for the SAME strategy arrives while the first is in flight;
    /// the response applies (the requested strategy still matches), and the
    /// position is reset to 5.
    #[tokio::test(flavor = "current_thread")]
    async fn reset_idempotent_with_in_flight_reset_applies() {
        let (mut mgr, _commit_rm, subscription_state) = new_manager_with_commit();
        bootstrap_metadata_with_topic(&mgr.shared.metadata, "t1", 2);
        let tp = TopicPartition::new("t1".to_string(), 1);
        assign_and_request_reset(&subscription_state, &tp, AutoOffsetResetStrategy::LATEST);

        mgr.reset_positions_if_needed(0).expect("ok");
        let poll_result = RequestManager::poll(&mut mgr, 0);
        let unsent = poll_result.unsent_requests.into_iter().next().expect("one unsent");

        // Idempotent re-request: SAME strategy.
        {
            let mut subs = subscription_state.lock().expect("subs");
            subs.request_offset_reset(&tp, AutoOffsetResetStrategy::LATEST).expect("reset");
        }

        let response = build_list_offsets_response("t1", vec![(1, Errors::None, 1, 5, UNKNOWN_EPOCH)]);
        unsent.handler().on_complete(build_list_offsets_client_response(response));
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        let _ = RequestManager::poll(&mut mgr, 0);

        let subs = subscription_state.lock().expect("subs");
        assert!(!subs.is_offset_reset_needed(&tp).expect("assigned"), "idempotent reset applies");
        assert_eq!(subs.position(&tp).expect("lookup").expect("present").offset, 5);
    }

    /// Java parity: `OffsetFetcherTest.testChangeResetWithInFlightReset`. A
    /// reset to a DIFFERENT strategy arrives while the first reset response
    /// is in flight. When the original (LATEST) response returns it is
    /// discarded by the third `maybe_seek_unvalidated` guard
    /// (`subscription_state.rs:978-983` — requested strategy mismatches the
    /// current `reset_strategy`). The new EARLIEST strategy survives: the
    /// partition is still awaiting reset, no position is applied, and
    /// `reset_strategy` is EARLIEST.
    #[tokio::test(flavor = "current_thread")]
    async fn reset_change_strategy_with_in_flight_reset_discards_stale_response() {
        let (mut mgr, _commit_rm, subscription_state) = new_manager_with_commit();
        bootstrap_metadata_with_topic(&mgr.shared.metadata, "t1", 2);
        let tp = TopicPartition::new("t1".to_string(), 1);
        // Initial reset request is LATEST.
        assign_and_request_reset(&subscription_state, &tp, AutoOffsetResetStrategy::LATEST);

        mgr.reset_positions_if_needed(0).expect("ok");
        let poll_result = RequestManager::poll(&mut mgr, 0);
        let unsent = poll_result.unsent_requests.into_iter().next().expect("one unsent");
        {
            let subs = subscription_state.lock().expect("subs");
            assert!(!subs.has_valid_position(&tp), "no valid position while reset in flight");
        }

        // Before the in-flight (LATEST) response is handled, the user
        // re-requests a reset to a DIFFERENT strategy (EARLIEST).
        {
            let mut subs = subscription_state.lock().expect("subs");
            subs.request_offset_reset(&tp, AutoOffsetResetStrategy::EARLIEST)
                .expect("reset");
        }

        // The original (LATEST) response returns and must be discarded
        // because the requested strategy no longer matches.
        let response = build_list_offsets_response("t1", vec![(1, Errors::None, 1, 5, UNKNOWN_EPOCH)]);
        unsent.handler().on_complete(build_list_offsets_client_response(response));
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        let _ = RequestManager::poll(&mut mgr, 0);

        let subs = subscription_state.lock().expect("subs");
        assert!(
            subs.is_offset_reset_needed(&tp).expect("assigned"),
            "reset still needed after discarding the stale LATEST response"
        );
        assert_eq!(
            subs.reset_strategy(&tp).expect("assigned"),
            Some(AutoOffsetResetStrategy::EARLIEST),
            "the newly-requested EARLIEST strategy must survive the discard",
        );
        assert!(
            subs.position(&tp).expect("lookup").is_none(),
            "no position applied from the discarded LATEST response"
        );
    }

    /// Java parity: `OffsetFetcherTest.testEarlierOffsetResetArrivesLate`. A
    /// two-phase sequence: (1) an in-flight EARLIEST reset response is
    /// discarded because a LATEST reset was requested before the response
    /// was handled (the strategy-mismatch guard,
    /// `subscription_state.rs:978-983`); the partition is still awaiting
    /// reset under the LATEST strategy. (2) A second reset issued under the
    /// new LATEST strategy succeeds, applying `position == 10`.
    #[tokio::test(flavor = "current_thread")]
    async fn reset_earlier_offset_reset_arrives_late() {
        let (mut mgr, _commit_rm, subscription_state) = new_manager_with_commit();
        bootstrap_metadata_with_topic(&mgr.shared.metadata, "t1", 2);
        let tp = TopicPartition::new("t1".to_string(), 1);
        // Initial reset request is EARLIEST.
        assign_and_request_reset(&subscription_state, &tp, AutoOffsetResetStrategy::EARLIEST);

        mgr.reset_positions_if_needed(0).expect("ok");
        let poll_result = RequestManager::poll(&mut mgr, 0);
        let unsent = poll_result.unsent_requests.into_iter().next().expect("one unsent");

        // Before the EARLIEST response is handled, request a reset to LATEST.
        {
            let mut subs = subscription_state.lock().expect("subs");
            subs.request_offset_reset(&tp, AutoOffsetResetStrategy::LATEST).expect("reset");
        }

        // The stale EARLIEST response (offset 0) returns and is ignored.
        let earlier = build_list_offsets_response("t1", vec![(1, Errors::None, 1, 0, UNKNOWN_EPOCH)]);
        unsent.handler().on_complete(build_list_offsets_client_response(earlier));
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        let _ = RequestManager::poll(&mut mgr, 0);

        {
            let subs = subscription_state.lock().expect("subs");
            assert!(
                subs.is_offset_reset_needed(&tp).expect("assigned"),
                "stale EARLIEST result ignored; reset still needed"
            );
            assert_eq!(
                subs.reset_strategy(&tp).expect("assigned"),
                Some(AutoOffsetResetStrategy::LATEST),
                "reset strategy is now LATEST",
            );
        }

        // Phase 2: issue a second reset under the LATEST strategy.
        // `request_offset_reset` cleared the retry backoff, so the partition
        // needs reset again immediately.
        mgr.reset_positions_if_needed(0).expect("ok");
        let later = build_list_offsets_response("t1", vec![(1, Errors::None, 1, 10, UNKNOWN_EPOCH)]);
        assert!(complete_first_unsent_with_response(&mut mgr, later, 0).await);
        let _ = RequestManager::poll(&mut mgr, 0);

        let subs = subscription_state.lock().expect("subs");
        assert!(
            !subs.is_offset_reset_needed(&tp).expect("assigned"),
            "LATEST reset applies on the second pass"
        );
        assert_eq!(subs.position(&tp).expect("lookup").expect("present").offset, 10);
    }

    /// Java parity: `OffsetFetcherTest.testAssignmentChangeWithInFlightReset`.
    /// The consumer is reassigned to a different partition while a reset
    /// response for the original partition is in flight. When the original
    /// response returns it is discarded by the first `maybe_seek_unvalidated`
    /// guard (`subscription_state.rs:965-971` — the partition is no longer
    /// assigned). Observable in Rust: `tp0` is not assigned, `tp1` is, and
    /// no position is applied.
    #[tokio::test(flavor = "current_thread")]
    async fn reset_assignment_change_with_in_flight_reset_discards_stale_response() {
        let (mut mgr, _commit_rm, subscription_state) = new_manager_with_commit();
        bootstrap_metadata_with_topic(&mgr.shared.metadata, "t1", 2);
        let tp0 = TopicPartition::new("t1".to_string(), 0);
        let tp1 = TopicPartition::new("t1".to_string(), 1);
        assign_and_request_reset(&subscription_state, &tp0, AutoOffsetResetStrategy::LATEST);

        mgr.reset_positions_if_needed(0).expect("ok");
        let poll_result = RequestManager::poll(&mut mgr, 0);
        let unsent = poll_result.unsent_requests.into_iter().next().expect("one unsent");
        {
            let subs = subscription_state.lock().expect("subs");
            assert!(!subs.has_valid_position(&tp0), "no valid position while reset in flight");
        }

        // Assignment change: reassign to tp1, dropping tp0.
        {
            let mut subs = subscription_state.lock().expect("subs");
            subs.assign_from_user(HashSet::from([tp1.clone()])).expect("assign");
        }

        // The in-flight tp0 response returns and is discarded (tp0 is no
        // longer assigned).
        let response = build_list_offsets_response("t1", vec![(0, Errors::None, 1, 5, UNKNOWN_EPOCH)]);
        unsent.handler().on_complete(build_list_offsets_client_response(response));
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        let _ = RequestManager::poll(&mut mgr, 0);

        let subs = subscription_state.lock().expect("subs");
        assert!(!subs.is_assigned(&tp0), "tp0 no longer assigned after reassignment");
        assert!(subs.is_assigned(&tp1), "tp1 is now assigned");
        assert!(
            subs.position(&tp1).expect("lookup").is_none(),
            "no position applied to tp1 from the discarded tp0 response"
        );
    }

    // -----------------------------------------------------------------
    //   validate skip / stale-response (OffsetFetcherTest)
    // -----------------------------------------------------------------

    /// Java parity: `testOffsetValidationSkippedForOldBroker`. A broker that
    /// only supports OffsetForLeaderEpoch v0-v2 (pre-2.3) cannot be used for
    /// offset validation; the manager completes validation immediately
    /// (without sending a request), so the partition leaves
    /// AWAITING_VALIDATION and no request is enqueued.
    #[tokio::test(flavor = "current_thread")]
    async fn validation_skipped_for_old_broker() {
        let (mut mgr, _commit_rm, subscription_state) = new_manager_with_commit();
        bootstrap_metadata_with_epoch(&mgr.shared.metadata, "t1", 2, 1);
        let tp = TopicPartition::new("t1".to_string(), 1);

        // Seek into AWAITING_VALIDATION with leader = node 0.
        let leader = crate::common::Node::new(0, "localhost".to_string(), 1969);
        let leader_and_epoch = LeaderAndEpoch::new(Some(leader.clone()), Some(1));
        let position = FetchPosition::with_leader(0, Some(1), leader_and_epoch);
        {
            let mut subs = subscription_state.lock().expect("subs");
            subs.assign_from_user(HashSet::from([tp.clone()])).expect("assign");
            subs.seek_unvalidated(&tp, position).expect("seek");
            assert!(subs.awaiting_validation(&tp).expect("assigned"));
        }

        // Old broker: OFFSET_FOR_LEADER_EPOCH supported only at v0-v2.
        let mut old_oitle = crate::api_versions_response_data::ApiVersion::new();
        old_oitle.set_api_key(ApiKeys::OFFSET_FOR_LEADER_EPOCH.id());
        old_oitle.set_min_version(0);
        old_oitle.set_max_version(2);
        mgr.api_versions
            .update(leader.id_string(), crate::NodeApiVersions::create_with_overrides(&[old_oitle]));

        mgr.validate_positions_if_needed(0).expect("ok");
        assert_eq!(
            mgr.requests_to_send_count(),
            0,
            "no OffsetsForLeaderEpoch request to an old broker"
        );
        let subs = subscription_state.lock().expect("subs");
        assert!(
            !subs.awaiting_validation(&tp).expect("assigned"),
            "validation must be skipped (completed) for an old broker"
        );
    }

    /// Java parity:
    /// `testOffsetValidationHandlesSeekWithInflightOffsetForLeaderRequest`.
    /// A `seek_unvalidated` to a DIFFERENT position arrives while the
    /// OffsetsForLeaderEpoch request is in flight; the response is ignored
    /// (`maybe_complete_validation` sees the current position no longer
    /// matches the request position), and the partition stays
    /// AWAITING_VALIDATION.
    #[tokio::test(flavor = "current_thread")]
    async fn validation_handles_seek_with_inflight_request() {
        let (mut mgr, _commit_rm, subscription_state) = new_manager_with_commit();
        bootstrap_metadata_with_epoch(&mgr.shared.metadata, "t1", 2, 1);
        let tp = TopicPartition::new("t1".to_string(), 1);
        // Initial position at offset 0, epoch 1.
        seek_unvalidated_and_install_api_versions(&mgr, &subscription_state, &tp, 0, 1);

        mgr.validate_positions_if_needed(0).expect("ok");
        let poll_result = RequestManager::poll(&mut mgr, 0);
        let unsent = poll_result.unsent_requests.into_iter().next().expect("one unsent");
        {
            let subs = subscription_state.lock().expect("subs");
            assert!(subs.awaiting_validation(&tp).expect("assigned"));
        }

        // Seek to a DIFFERENT position while the request is in flight.
        {
            let leader = crate::common::Node::new(0, "localhost".to_string(), 1969);
            let leader_and_epoch = LeaderAndEpoch::new(Some(leader), Some(1));
            let new_position = FetchPosition::with_leader(5, Some(1), leader_and_epoch);
            let mut subs = subscription_state.lock().expect("subs");
            subs.seek_unvalidated(&tp, new_position).expect("seek");
            assert!(subs.awaiting_validation(&tp).expect("assigned"));
        }

        // The response returns and is ignored (position changed).
        let response = build_offsets_for_leader_epoch_response("t1", vec![(1, Errors::None, 0, 0)]);
        unsent.handler().on_complete(build_oitle_client_response(response));
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        let _ = RequestManager::poll(&mut mgr, 0);

        let subs = subscription_state.lock().expect("subs");
        assert!(
            subs.awaiting_validation(&tp).expect("assigned"),
            "stale OffsetsForLeaderEpoch response for a changed position must be ignored"
        );
    }
}
