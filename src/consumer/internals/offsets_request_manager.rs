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
use crate::consumer::OffsetAndTimestamp;
use crate::list_offsets_request_data::ListOffsetsPartition;

use super::auto_offset_reset_strategy::AutoOffsetResetStrategy;
use super::commit_request_manager::CommitRequestManager;
use super::consumer_metadata::ConsumerMetadata;
use super::network_client_delegate::{PollResult, UnsentRequest};
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
type FetchOffsetsWaiter = oneshot::Sender<Result<HashMap<TopicPartition, Option<OffsetAndTimestamp>>, KafkaError>>;

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
        } else {
            // Java: `requestsToRetry.add(listOffsetsRequestState);
            // metadata.requestUpdate(false);`
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
    fn fail_request_state(state: &Arc<Mutex<ListOffsetsRequestState>>, err: KafkaError) {
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
    ) -> oneshot::Receiver<Result<HashMap<TopicPartition, Option<OffsetAndTimestamp>>, KafkaError>> {
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
        // wire timestamp.
        let mut timestamps_to_search: HashMap<TopicPartition, ListOffsetsPartition> = HashMap::new();
        for (tp, strategy) in &partition_strategies {
            if let Some(ts) = strategy.timestamp() {
                let mut part = ListOffsetsPartition::new();
                part.set_partition_index(tp.partition());
                part.set_timestamp(ts);
                timestamps_to_search.insert(tp.clone(), part);
            }
        }

        // Group by current leader, dropping entries without one — those
        // will be retried on the next metadata update.
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
                Err(_) => Err(KafkaError::new(crate::common::protocol::Errors::NetworkException)),
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
                Ok(offsets) => {
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
                // Match Java: synchronous error path also goes through
                // `cacheExceptionIfEventExpired`. The `whenComplete` runs
                // immediately because the result is already failed.
                self.maybe_cache_update_positions_exception(&err, deadline_ms, current_time_ms);
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
            // Await the committed-offset fetch. If the inner sender was
            // dropped (request cancelled / manager torn down) Java would
            // never complete the future; we surface a network error
            // instead so the outer caller doesn't hang silently
            // (CLAUDE.md §5: silently completing or hanging futures is
            // worse than an explicit error).
            let fetch_result = match inner_rx.await {
                Ok(r) => r,
                Err(_) => Err(KafkaError::new(crate::common::protocol::Errors::NetworkException)),
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

    /// Cache the given error if `current_time_ms >= deadline_ms`. Java:
    /// `cacheExceptionIfEventExpired`. The cache is idempotent — only
    /// the first error in a contiguous run is stored.
    fn maybe_cache_update_positions_exception(&self, err: &KafkaError, deadline_ms: i64, current_time_ms: i64) {
        if current_time_ms < deadline_ms {
            return;
        }
        let mut guard = self
            .cached_update_positions_exception
            .lock()
            .expect("cached_update_positions_exception mutex poisoned");
        if guard.is_none() {
            *guard = Some(err.clone());
        }
    }

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
        // Clear transient topics once every in-flight `fetch_offsets`
        // operation has completed. Java does this inside the global
        // `whenComplete` chain on `fetchOffsets`; we approximate it here
        // by checking whether `requests_to_retry` is empty and no
        // in-flight `fetch_offsets` requests remain to drain. Since
        // `requests_to_send` may still hold reset / validate requests,
        // we conservatively only clear when both retry and unsent vectors
        // are devoid of `ListOffsetsRequestState` references — but the
        // simpler and behaviour-equivalent approximation is to defer the
        // clear to the listener's path. Java's eventual-consistency on
        // transient topics is already loose; not clearing here just keeps
        // the topic in the cache one refresh longer, which is benign.
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
                OffsetsManagerShared::fail_request_state(&state, err);
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
        rx: oneshot::Receiver<Result<HashMap<TopicPartition, Option<OffsetAndTimestamp>>, KafkaError>>,
        now_ms: i64,
    ) -> Result<HashMap<TopicPartition, Option<OffsetAndTimestamp>>, KafkaError> {
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
    /// **Rust deviation note:** Java's `OffsetAndTimestampInternal`
    /// permits negative timestamps (the broker may omit timestamps for
    /// earliest/latest queries), but the Rust public `OffsetAndTimestamp`
    /// enforces non-negative. The `ListOffsetsEvent` API uses
    /// `Option<OffsetAndTimestamp>` to preserve the sentinel semantics —
    /// negative timestamps surface as `None`. This test therefore uses
    /// timestamp=100 so the assertion can pin the offset value.
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

    /// Java parity: `testListOffsetsWaitingForMetadataUpdate_Timeout`.
    /// Building the request fails because the leader is unknown; the
    /// request is parked on `requests_to_retry`, and `poll()` returns no
    /// unsent requests.
    #[tokio::test(flavor = "current_thread")]
    async fn fetch_offsets_unknown_leader_parks_on_retry() {
        let mut mgr = new_manager();
        // No metadata bootstrap: leader is unknown.
        let tp = TopicPartition::new("t1".to_string(), 1);
        let mut timestamps = HashMap::new();
        timestamps.insert(tp, EARLIEST_TIMESTAMP);

        let _rx = mgr.fetch_offsets(timestamps, false);
        assert_eq!(mgr.requests_to_send_count(), 0, "no request built when leader unknown");
        assert_eq!(mgr.requests_to_retry_count(), 1, "state parked for retry");

        // Subsequent poll yields no unsent requests.
        let res = RequestManager::poll(&mut mgr, 0);
        assert!(res.unsent_requests.is_empty());
    }

    /// Java parity: `testListOffsetsWaitingForMetadataUpdate_RetrySucceeds`.
    /// First attempt parks the state (no leader); subsequent metadata
    /// update fires `on_update`, which replays the request — this time
    /// with a known leader.
    #[tokio::test(flavor = "current_thread")]
    async fn fetch_offsets_metadata_update_retries_successfully() {
        let mut mgr = new_manager();
        let tp = TopicPartition::new("t1".to_string(), 1);
        let mut timestamps = HashMap::new();
        timestamps.insert(tp.clone(), EARLIEST_TIMESTAMP);

        let rx = mgr.fetch_offsets(timestamps, false);
        assert_eq!(mgr.requests_to_send_count(), 0);
        assert_eq!(mgr.requests_to_retry_count(), 1);

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

    /// Java parity: `testRequestFailsWithRetriableError_RetrySucceeds`.
    /// First attempt's response carries a retriable error → parition
    /// added to `remaining_to_search`, state re-parked. Metadata update
    /// → replay → success.
    #[tokio::test(flavor = "current_thread")]
    async fn fetch_offsets_retriable_error_retries_after_metadata_update() {
        let (mut mgr, _commit_rm, _subs) = new_manager_with_commit();
        bootstrap_metadata_with_topic(&mgr.shared.metadata, "t1", 2);
        let tp = TopicPartition::new("t1".to_string(), 1);
        let mut timestamps = HashMap::new();
        timestamps.insert(tp.clone(), EARLIEST_TIMESTAMP);

        let rx = mgr.fetch_offsets(timestamps, false);
        assert_eq!(mgr.requests_to_send_count(), 1);

        // Respond with a retriable error.
        let response = build_list_offsets_response("t1", vec![(1, Errors::UnknownLeaderEpoch, -1, -1, UNKNOWN_EPOCH)]);
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
            "retriable error should re-park the state on requests_to_retry"
        );

        // Metadata update fires the listener → replay.
        bootstrap_metadata_with_topic(&mgr.shared.metadata, "t1", 2);
        assert_eq!(mgr.requests_to_send_count(), 1);
        assert_eq!(mgr.requests_to_retry_count(), 0);

        let response = build_list_offsets_response("t1", vec![(1, Errors::None, 100, 5, UNKNOWN_EPOCH)]);
        assert!(complete_first_unsent_with_response(&mut mgr, response, 0).await);

        let result = await_fetch_result(&mut mgr, rx, 0).await.expect("ok");
        let oat = result.get(&tp).expect("entry present").as_ref().expect("non-null offset");
        assert_eq!(oat.offset(), 5);
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
        let unsent = res.unsent_requests.into_iter().next().expect("one unsent");
        let builder = unsent.request_builder().expect("builder present");
        // Java's `unsentRequest.requestBuilder().build()` returns an
        // `AbstractRequest` that is downcast to `ListOffsetsRequest`. The
        // Rust equivalent is `builder.build()` → `ConcreteRequest::ListOffsets`.
        assert_eq!(*builder.api_key(), ApiKeys::LIST_OFFSETS);
        let built = builder.build().expect("build");
        let request = match built {
            crate::common::requests::ConcreteRequest::ListOffsets(r) => r,
            other => panic!("expected ListOffsetsRequest, got {other:?}"),
        };
        assert_eq!(request.timeout_ms(), TEST_REQUEST_TIMEOUT_MS as i32);
    }
}
