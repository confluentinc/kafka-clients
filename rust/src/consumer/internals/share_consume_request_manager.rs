// Copyright 2026 Confluent Inc.
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

//! `ShareConsumeRequestManager` — builds the `ShareFetch` and
//! `ShareAcknowledge` requests of a share consumer and handles their
//! responses.
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager`,
//! with its nested classes `AcknowledgeRequestState`, `ResultHandler`,
//! `Tuple`, `IdAndPartition`, `LeaderIdAndEpoch` and `AcknowledgeRequestType`.
//!
//! # Translation notes
//!
//! - **Task.** Every method runs on the consumer's background task, as Java's
//!   run on the network thread, so the manager's maps are plain fields
//!   (`share-consumer.md` §6). Java's `AtomicInteger fetchRecordsNodeId` is a
//!   plain `i32`: it is only read and written by `#poll` and the two
//!   `ShareFetch` response handlers, all on that thread.
//! - **Responses.** Java registers `whenComplete` callbacks on each
//!   `UnsentRequest`; they run inside the network poll. The manager keeps the
//!   response receiver of each request it builds and runs the same handlers in
//!   [`ShareConsumeRequestManager::handle_completed_requests`], at the start
//!   of [`RequestManager::poll`] and of every entry point an application event
//!   reaches (`fetch`, `commit_sync`, `commit_async`, `acknowledge_on_close`,
//!   `set_acknowledgement_commit_callback_registered`), so an event sees the
//!   state the responses that arrived before it left, as in Java. No task is
//!   spawned per request: the network poll returns when a response arrives,
//!   and the background loop polls the managers again right after. A
//!   request that fails because its node disconnected completes with
//!   `DisconnectError`, Java's `DisconnectException`, although the Rust
//!   `NetworkClientDelegate` reports a `NetworkError`. Responses
//!   that arrive in the same network poll are handled in the order their
//!   requests were built, where Java handles them in the order the network
//!   client completes them; the two orders only differ across nodes.
//! - **Shared objects.** Java's `AcknowledgeRequestState` holds a reference to
//!   a `ShareSessionHandler` that `sessionHandlers` also holds, and several
//!   states of one `commitSync` share one `ResultHandler`. Rust shares them as
//!   `Arc<Mutex<_>>` / `Arc<ResultHandler>`. The mutexes are only ever taken
//!   on the background task, so they are never contended; the order is
//!   request state, then session handler, and no lock is held across an
//!   `.await` (there is none in this module).
//! - **Ordering.** Java keys `sessionHandlers`, `acknowledgeRequestStates` and
//!   the set of missing nodes by `Integer` node id in a `HashMap` / `HashSet`,
//!   which iterate small ids in ascending order; the tests rely on it (a
//!   response prepared first goes to the lowest node). Rust uses `BTreeMap` /
//!   `BTreeSet` to keep that order, and builds the `ShareFetch` requests in
//!   node-id order (Java's `Map<Node, ShareSessionHandler>` order is the
//!   `Node` hash order, which no test depends on).
//! - **`IdempotentCloser`** is a `closed` flag, as in `ShareFetchBuffer`
//!   (ledger G14).

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicI32, AtomicI64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use log::{debug, error, trace};
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::TryRecvError;

use crate::ClientResponse;
use crate::common::errors::{DisconnectError, InvalidRecordStateError};
use crate::common::internals::KafkaFutureImpl;
use crate::common::protocol::Errors;
use crate::common::requests::ConcreteResponse;
use crate::common::requests::ShareRequestMetadata;
use crate::common::utils::Time;
use crate::common::utils::internals::BufferSupplier;
use crate::common::{Error, Node, TopicIdPartition, TopicPartition, Uuid};
use crate::consumer::internals::events::{ShareAcknowledgementEvent, ShareAcknowledgementEventHandler};
use crate::consumer::internals::{
    Acknowledgements, MemberStateListener, NodeAcknowledgements, PollResult, RequestManager, ShareAcquireMode,
    ShareCompletedFetch, ShareConsumerMetadata, ShareFetchBuffer, ShareFetchConfig, ShareFetchMetricsAggregator,
    ShareFetchMetricsManager, ShareSessionHandler, SubscriptionState, TimedRequestState, UnsentRequest,
};
use crate::share_acknowledge_response_data;
use crate::share_fetch_response_data;
use crate::{ShareAcknowledgeRequestData, ShareFetchRequestData};

/// A share session handler, shared between the manager's `session_handlers`
/// and the acknowledge request states built for its node.
type SharedSessionHandler = Arc<Mutex<ShareSessionHandler>>;

/// An acknowledge request state, shared between its node's [`Tuple`] and the
/// in-flight request that completes it.
type SharedAcknowledgeRequestState = Arc<Mutex<AcknowledgeRequestState>>;

/// The response future of a request (the receiver of its `UnsentRequest`).
type ResponseReceiver = oneshot::Receiver<Result<ClientResponse, Error>>;

/// The result of a `commitSync`: the completed acknowledgements by partition.
type CommitSyncResult = HashMap<TopicIdPartition, Acknowledgements>;

/// Locks `mutex`, recovering the guard if a panic poisoned it. The guarded
/// objects are only touched by the background task, and a panic there ends
/// the task, so poisoning carries no information here.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// An `Acknowledgements` count as Java's `int`.
fn count(acknowledgements: &Acknowledgements) -> i32 {
    i32::try_from(acknowledgements.size()).unwrap_or(i32::MAX)
}

const INVALID_RESPONSE: &str = "Acknowledgement not successful due to invalid response from broker";

/// The error an acknowledgement completes with for an error code: `None` for
/// [`Errors::None`], Java's `Errors.exception()`.
fn error_for(code: Errors) -> Option<Error> {
    code.error()
}

/// A `ShareFetch` or `ShareAcknowledge` request waiting for its response.
///
/// Rust-only (DoD §7): Java's `UnsentRequest#whenComplete` callback captures
/// these values; the manager keeps them with the request's response receiver
/// and runs the callback body in
/// [`ShareConsumeRequestManager::handle_completed_requests`].
enum PendingRequest {
    ShareFetch {
        target: Node,
        request_data: ShareFetchRequestData,
        response_rx: ResponseReceiver,
    },
    ShareAcknowledge {
        target: Node,
        request_data: ShareAcknowledgeRequestData,
        state: SharedAcknowledgeRequestState,
        /// Java's `unsentRequest.handler().completionTimeMs()`.
        completion_time_ms: Arc<AtomicI64>,
        response_rx: ResponseReceiver,
    },
}

impl PendingRequest {
    fn response_rx(&mut self) -> &mut ResponseReceiver {
        match self {
            Self::ShareFetch { response_rx, .. } | Self::ShareAcknowledge { response_rx, .. } => response_rx,
        }
    }
}

/// `ShareConsumeRequestManager` is responsible for generating
/// `ShareFetchRequest` and `ShareAcknowledgeRequest` to fetch and acknowledge
/// records being delivered for a consumer in a share group.
///
/// The request manager keeps track of which share sessions contain which
/// topic-partitions. This information will generally match the leaders in the
/// metadata, but when the fetchable partitions or leadership change, there can
/// be short-lived differences. This is intentional to ensure that
/// topic-partitions which are no longer being fetched and topic-partitions
/// which have changed leaders are cleanly removed from their former share
/// sessions. It is all slightly complicated by the fact that the fetchable
/// partitions are represented by TopicPartition (without topic ID) and, when a
/// topic is recreated, its topic ID changes. All state in share sessions is
/// tracked by topic ID so the current mapping from topic name to topic ID as
/// represented in the share sessions is also maintained in the request
/// manager, and again there can be short-lived differences between this and
/// the cluster metadata.
#[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager")]
pub(crate) struct ShareConsumeRequestManager {
    time: Arc<dyn Time>,
    group_id: String,
    metadata: Arc<ShareConsumerMetadata>,
    subscriptions: Arc<Mutex<SubscriptionState>>,
    share_fetch_config: ShareFetchConfig,
    share_fetch_buffer: Arc<ShareFetchBuffer>,
    acknowledge_event_handler: Arc<ShareAcknowledgementEventHandler>,
    /// Java's `HashMap<Integer, ShareSessionHandler>`; ascending node order
    /// (module docs, "Ordering").
    session_handlers: BTreeMap<i32, SharedSessionHandler>,
    nodes_with_pending_requests: HashSet<i32>,
    metrics_manager: Arc<ShareFetchMetricsManager>,
    /// Java's `IdempotentCloser`.
    closed: bool,
    member_id: Option<Uuid>,
    fetch_more_records: bool,
    fetch_records_node_id: i32,
    share_session_topic_id_map: HashMap<TopicPartition, TopicIdPartition>,
    share_session_leader_map: HashMap<TopicIdPartition, LeaderIdAndEpoch>,
    fetch_acknowledgements_to_send: HashMap<i32, HashMap<TopicIdPartition, Acknowledgements>>,
    fetch_acknowledgements_in_flight: HashMap<i32, HashMap<TopicIdPartition, Acknowledgements>>,
    /// Java's `HashMap<Integer, Tuple<AcknowledgeRequestState>>`; ascending
    /// node order (module docs, "Ordering").
    acknowledge_request_states: BTreeMap<i32, Tuple<SharedAcknowledgeRequestState>>,
    retry_backoff_ms: i64,
    retry_backoff_max_ms: i64,
    closing: bool,
    close_future: KafkaFutureImpl<()>,
    is_acknowledgement_commit_callback_registered: bool,
    topic_names_map: HashMap<IdAndPartition, String>,
    /// The requests built and not yet handled, in the order they were built.
    pending_requests: Vec<PendingRequest>,
    /// Test-only stand-in for Mockito's `verify(manager, times(n)).closeInternal()`
    /// (`ShareConsumeRequestManagerTest#testCloseShouldBeIdempotent`).
    #[cfg(test)]
    close_internal_calls: usize,
}

impl ShareConsumeRequestManager {
    /// Creates the manager.
    ///
    /// Java's `LogContext` parameter is dropped (the `log` crate is used).
    #[expect(clippy::too_many_arguments)]
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager#ShareConsumeRequestManager")]
    pub(crate) fn new(
        time: Arc<dyn Time>,
        group_id: impl Into<String>,
        metadata: Arc<ShareConsumerMetadata>,
        subscriptions: Arc<Mutex<SubscriptionState>>,
        share_fetch_config: ShareFetchConfig,
        share_fetch_buffer: Arc<ShareFetchBuffer>,
        acknowledge_event_handler: Arc<ShareAcknowledgementEventHandler>,
        metrics_manager: Arc<ShareFetchMetricsManager>,
        retry_backoff_ms: i64,
        retry_backoff_max_ms: i64,
    ) -> Self {
        Self {
            time,
            group_id: group_id.into(),
            metadata,
            subscriptions,
            share_fetch_config,
            share_fetch_buffer,
            acknowledge_event_handler,
            session_handlers: BTreeMap::new(),
            nodes_with_pending_requests: HashSet::new(),
            metrics_manager,
            closed: false,
            member_id: None,
            fetch_more_records: false,
            fetch_records_node_id: -1,
            share_session_topic_id_map: HashMap::new(),
            share_session_leader_map: HashMap::new(),
            fetch_acknowledgements_to_send: HashMap::new(),
            fetch_acknowledgements_in_flight: HashMap::new(),
            acknowledge_request_states: BTreeMap::new(),
            retry_backoff_ms,
            retry_backoff_max_ms,
            closing: false,
            close_future: KafkaFutureImpl::new(),
            is_acknowledgement_commit_callback_registered: false,
            topic_names_map: HashMap::new(),
            pending_requests: Vec::new(),
            #[cfg(test)]
            close_internal_calls: 0,
        }
    }

    /// Runs the response handling of every request whose response (or
    /// failure) has arrived, in the order the requests were built.
    ///
    /// Rust-only: Java runs these handlers in the `whenComplete` callbacks of
    /// its `UnsentRequest`s, inside the network poll (module docs,
    /// "Responses"). A request dropped by the network client without a
    /// response is handled as a failure with a `DisconnectError`.
    pub(crate) fn handle_completed_requests(&mut self) {
        if self.pending_requests.is_empty() {
            return;
        }
        let mut completed = Vec::new();
        let pending = std::mem::take(&mut self.pending_requests);
        for mut request in pending {
            match request.response_rx().try_recv() {
                Ok(result) => completed.push((request, result.map_err(Self::disconnect_if_network_error))),
                Err(TryRecvError::Empty) => self.pending_requests.push(request),
                Err(TryRecvError::Closed) => completed.push((request, Err(Self::disconnect()))),
            }
        }
        for (request, result) in completed {
            match request {
                PendingRequest::ShareFetch { target, request_data, .. } => match result {
                    Ok(response) => self.handle_share_fetch_success(&target, &request_data, response),
                    Err(error) => self.handle_share_fetch_failure(&target, &request_data, &error),
                },
                PendingRequest::ShareAcknowledge { target, request_data, state, completion_time_ms, .. } => {
                    let completion_time_ms = completion_time_ms.load(Ordering::Acquire);
                    match result {
                        Ok(response) => self.handle_share_acknowledge_success(
                            &target,
                            &request_data,
                            &state,
                            response,
                            completion_time_ms,
                        ),
                        Err(error) => self.handle_share_acknowledge_failure(
                            &target,
                            &request_data,
                            &state,
                            &error,
                            completion_time_ms,
                        ),
                    }
                },
            }
        }
    }

    /// Java's `DisconnectException.INSTANCE`, built with the no-argument
    /// `DisconnectException#DisconnectException()` constructor: no message.
    fn disconnect() -> Error {
        Error::Disconnect(DisconnectError::new(""))
    }

    /// The error a request to a node failed with, as Java's
    /// `NetworkClientDelegate.FutureCompletionHandler#onComplete` / `#onFailure`
    /// report it: `DisconnectException` for a disconnected response or a
    /// failed connection with no authentication error, where the Rust
    /// `NetworkClientDelegate` reports a `NetworkError` (PLAN §8, a consumer
    /// follow-up). Every share request names its node, so a `NetworkError`
    /// from the delegate is one of those two cases; Java's own `NetworkException`
    /// there is the node-less close path, which no share request takes.
    fn disconnect_if_network_error(error: Error) -> Error {
        if error.error() == Errors::NetworkError {
            Self::disconnect()
        } else {
            error
        }
    }

    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager#isShareAcquireModeRecordLimit"
    )]
    fn is_share_acquire_mode_record_limit(&self) -> bool {
        self.share_fetch_config.share_acquire_mode == ShareAcquireMode::RecordLimit
    }

    /// Add acknowledgements for a topic-partition to the node's in-flight
    /// acknowledgements. If we cannot add acknowledgements, they are completed
    /// with `NETWORK_EXCEPTION`. This probably indicates the connection to the
    /// leader broker was lost, but then re-established without a leadership
    /// change, in which case the acknowledgements fail.
    ///
    /// # Returns
    ///
    /// The acknowledgements to add to the request if we can add
    /// acknowledgements to the share session.
    ///
    /// # Translation notes
    ///
    /// Java returns a `boolean` and the caller keeps its reference to the
    /// acknowledgements; Rust returns the copy the caller hands to the session
    /// handler, the in-flight map keeping the original.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager#maybeAddAcknowledgements")]
    fn maybe_add_acknowledgements(
        &mut self,
        handler: &ShareSessionHandler,
        node: &Node,
        tip: &TopicIdPartition,
        mut acknowledgements: Acknowledgements,
    ) -> Option<Acknowledgements> {
        if handler.is_new_session() {
            // Failing the acknowledgements as we cannot have piggybacked acknowledgements in the initial ShareFetchRequest.
            debug!("Cannot send acknowledgements on initial epoch for ShareSession for partition {tip}");
            acknowledgements.complete(error_for(Errors::NetworkError));
            self.maybe_send_share_acknowledgement_event(HashMap::from([(tip.clone(), acknowledgements)]), true, None);
            None
        } else {
            self.metrics_manager.record_acknowledgement_sent(count(&acknowledgements));
            let to_send = acknowledgements.clone();
            self.fetch_acknowledgements_in_flight
                .entry(node.id())
                .or_default()
                .insert(tip.clone(), acknowledgements);
            Some(to_send)
        }
    }

    /// Iterate over the session handlers to see if there are acknowledgements
    /// to be sent for partitions which are no longer part of the current
    /// subscription or which have disappeared from the metadata. Also include
    /// session handlers which have no fetchable partitions so the share
    /// sessions can be brought up to date.
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager#prepareRemainingSessionHandlerForRequest"
    )]
    fn prepare_remaining_session_handler_for_request(
        &mut self,
        node: &Node,
        session_handler: &SharedSessionHandler,
        handler_map: &mut BTreeMap<i32, (Node, SharedSessionHandler)>,
    ) {
        let node_id = node.id();
        match self.fetch_acknowledgements_to_send.get_mut(&node_id) {
            Some(node_acks_from_fetch_map) => {
                let node_acks = std::mem::take(node_acks_from_fetch_map);
                for (tip, mut acks) in node_acks {
                    let leader = self.share_session_leader_map.get(&tip);
                    if leader.is_some_and(|leader| leader.leader_id == node_id) {
                        // Check if the share session epoch is valid for sending acknowledgements.
                        let mut handler = lock(session_handler);
                        let Some(acks) = self.maybe_add_acknowledgements(&handler, node, &tip, acks) else {
                            continue;
                        };

                        handler.add_partition_to_acknowledge_only(tip.clone(), acks);
                        drop(handler);

                        self.topic_names_map
                            .entry(IdAndPartition::new(tip.topic_id(), tip.partition()))
                            .or_insert_with(|| tip.topic().to_string());
                        debug!("Added fetch request for previously subscribed partition {tip} to node {node_id}");
                    } else {
                        debug!(
                            "Leader for the partition is down or has changed, failing acknowledgements for partition {tip}"
                        );
                        acks.complete(error_for(self.acknowledgements_cannot_be_sent_error(node_id, &tip)));
                        self.maybe_send_share_acknowledgement_event(HashMap::from([(tip, acks)]), true, None);
                    }
                }
                handler_map.insert(node_id, (node.clone(), Arc::clone(session_handler)));
            },
            None => {
                handler_map
                    .entry(node_id)
                    .or_insert_with(|| (node.clone(), Arc::clone(session_handler)));
            },
        }
    }

    /// A share session can be closed if it's empty and has no buffered data.
    ///
    /// # Returns
    ///
    /// True if the node's share session can be closed.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager#isSessionCloseCandidate")]
    fn is_session_close_candidate(
        &self,
        node: &Node,
        session_handler: &SharedSessionHandler,
        handler_map: &BTreeMap<i32, (Node, SharedSessionHandler)>,
        buffered_nodes: &HashSet<i32>,
    ) -> bool {
        let node_id = node.id();
        // Read under one short lock, released before the request states are
        // locked below (lock order: request state, then session handler).
        let (is_session_empty, is_new_session) = {
            let session_handler = lock(session_handler);
            (session_handler.is_session_empty(), session_handler.is_new_session())
        };
        !self.closing                                       // the close path already managed closing share sessions
            && is_session_empty                             // no share partitions remain in the share session
            && !is_new_session                              // the session is established on the broker
            && !handler_map.contains_key(&node_id)          // the handler is already being used in this poll
            && !buffered_nodes.contains(&node_id)           // no records for this node remain in the fetch buffer
            && !self.node_has_pending_acknowledgements(node_id) // the node has pending acknowledgements
    }

    /// Returns whether there are acknowledgements still being delivered for
    /// the node, whether waiting to be sent, in flight, or being processed as
    /// part of an acknowledge request state.
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager#nodeHasPendingAcknowledgements"
    )]
    fn node_has_pending_acknowledgements(&self, node_id: i32) -> bool {
        if self
            .fetch_acknowledgements_to_send
            .get(&node_id)
            .is_some_and(|acks_to_send| !acks_to_send.is_empty())
        {
            return true;
        }

        if self
            .fetch_acknowledgements_in_flight
            .get(&node_id)
            .is_some_and(|acks_in_flight| !acks_in_flight.is_empty())
        {
            return true;
        }

        if let Some(request_states) = self.acknowledge_request_states.get(&node_id) {
            return Self::are_request_states_in_progress(request_states.sync_request_queue())
                || Self::is_request_state_in_progress(request_states.async_request())
                || Self::is_request_state_in_progress(request_states.close_request());
        }

        false
    }

    /// Remove session handlers for nodes which are now missing from the
    /// cluster metadata. Only nodes with no in-flight requests are removed,
    /// since those requests will complete. Piggyback acknowledgements for the
    /// missing nodes are completed with `NETWORK_EXCEPTION`.
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager#removeSessionHandlersForMissingNodes"
    )]
    fn remove_session_handlers_for_missing_nodes(&mut self, missing_nodes: &BTreeSet<i32>) {
        let cluster = self.metadata.fetch();
        for &node_id in missing_nodes {
            if cluster.node_by_id(node_id).is_none() && !self.nodes_with_pending_requests.contains(&node_id) {
                if let Some(node_acks_from_fetch_map) = self.fetch_acknowledgements_to_send.remove(&node_id) {
                    for (tip, mut acks) in node_acks_from_fetch_map {
                        debug!(
                            "Node {node_id} is no longer in the cluster metadata, failing acknowledgements for partition {tip}"
                        );
                        acks.complete(error_for(Errors::NetworkError));
                        self.maybe_send_share_acknowledgement_event(HashMap::from([(tip, acks)]), true, None);
                    }
                }

                debug!("Removing session handler for node {node_id} which is no longer in the cluster metadata");
                self.session_handlers.remove(&node_id);
            }
        }
    }

    /// Signals that the consumer wants more records, and stores the
    /// acknowledgements to send in the next `ShareFetch`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager#fetch")]
    pub(crate) fn fetch(
        &mut self,
        acknowledgements_map: impl IntoIterator<Item = (TopicIdPartition, NodeAcknowledgements)>,
    ) {
        self.handle_completed_requests();
        if !self.fetch_more_records {
            debug!("Fetch more data");
            self.fetch_more_records = true;
        }

        // Store the acknowledgements and send them in the next ShareFetch.
        self.process_acknowledgements_map(acknowledgements_map);
    }

    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager#processAcknowledgementsMap")]
    fn process_acknowledgements_map(
        &mut self,
        acknowledgements_map: impl IntoIterator<Item = (TopicIdPartition, NodeAcknowledgements)>,
    ) {
        for (tip, node_acks) in acknowledgements_map {
            let node_id = node_acks.node_id();
            let current_node_acknowledgements_map = self.fetch_acknowledgements_to_send.entry(node_id).or_default();
            match current_node_acknowledgements_map.get_mut(&tip) {
                Some(current_acknowledgements_for_node) => {
                    current_acknowledgements_for_node.merge(node_acks.acknowledgements());
                },
                None => {
                    current_node_acknowledgements_map.insert(tip, node_acks.into_acknowledgements());
                },
            }
        }
    }

    /// Process acknowledgeRequestStates and prepares a list of
    /// acknowledgements to be sent in the poll().
    ///
    /// # Arguments
    ///
    /// - `current_time_ms` - the current time in ms.
    ///
    /// # Returns
    ///
    /// the PollResult containing zero or more acknowledgements.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager#processAcknowledgements")]
    fn process_acknowledgements(&mut self, current_time_ms: i64) -> Option<PollResult> {
        let mut unsent_requests = Vec::new();
        let node_ids: Vec<i32> = self.acknowledge_request_states.keys().copied().collect();
        for node_id in node_ids {
            if !self.is_node_free(node_id) {
                trace!(
                    "Skipping acknowledge request because previous request to {node_id} has not been processed, so acks are not sent"
                );
            } else {
                // Java's `isAsyncSent`, one `AtomicBoolean` reset to false for each node.
                let mut is_async_sent = false;

                // First, the acknowledgements from commitAsync are sent.
                let async_request = self.request_states(node_id).and_then(|states| states.async_request().cloned());
                if let Some(request) =
                    self.maybe_build_request(async_request.as_ref(), current_time_ms, true, &mut is_async_sent)
                {
                    unsent_requests.push(request);
                }

                // Check to ensure we start processing commitSync/close only if there are no commitAsync requests left to process.
                if is_async_sent {
                    if !self.is_node_free(node_id) {
                        trace!(
                            "Skipping acknowledge request because previous request to {node_id} has not been processed, so acks are not sent"
                        );
                        continue;
                    }

                    let Some(request_states) = self.request_states(node_id) else {
                        continue;
                    };
                    // We try to process the close request only if we have processed the async and the sync requests for the node.
                    match request_states.sync_request_queue() {
                        None => {
                            let close_request_state = request_states.close_request().cloned();

                            if let Some(request) = self.maybe_build_request(
                                close_request_state.as_ref(),
                                current_time_ms,
                                false,
                                &mut is_async_sent,
                            ) {
                                unsent_requests.push(request);
                            }
                        },
                        Some(sync_request_queue) => {
                            // Processing the acknowledgements from commitSync
                            let sync_request_queue: Vec<SharedAcknowledgeRequestState> =
                                sync_request_queue.iter().cloned().collect();
                            for acknowledge_request_state in sync_request_queue {
                                if !self.is_node_free(node_id) {
                                    trace!(
                                        "Skipping acknowledge request because previous request to {node_id} has not been processed, so acks are not sent"
                                    );
                                    break;
                                }
                                if let Some(request) = self.maybe_build_request(
                                    Some(&acknowledge_request_state),
                                    current_time_ms,
                                    false,
                                    &mut is_async_sent,
                                ) {
                                    unsent_requests.push(request);
                                }
                            }
                        },
                    }
                }
            }
        }

        if !unsent_requests.is_empty() {
            Some(PollResult::with_requests(unsent_requests))
        } else if self.check_and_remove_completed_acknowledgements() {
            // Return empty result until all the acknowledgement request states are processed
            Some(PollResult::empty())
        } else if self.closing {
            if !self.close_future.is_done() {
                self.close_future.complete(());
            }
            Some(PollResult::empty())
        } else {
            None
        }
    }

    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager#isNodeFree")]
    fn is_node_free(&self, node_id: i32) -> bool {
        !self.nodes_with_pending_requests.contains(&node_id)
    }

    /// Records whether an acknowledgement commit callback is registered, which
    /// decides whether completed acknowledgements are sent to the application
    /// task.
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager#setAcknowledgementCommitCallbackRegistered"
    )]
    pub(crate) fn set_acknowledgement_commit_callback_registered(
        &mut self,
        is_acknowledgement_commit_callback_registered: bool,
    ) {
        self.handle_completed_requests();
        self.is_acknowledgement_commit_callback_registered = is_acknowledgement_commit_callback_registered;
    }

    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager#maybeSendShareAcknowledgementEvent"
    )]
    fn maybe_send_share_acknowledgement_event(
        &self,
        acknowledgements_map: HashMap<TopicIdPartition, Acknowledgements>,
        check_for_renew_acknowledgements: bool,
        acquisition_lock_timeout_ms: Option<i32>,
    ) {
        if self.is_acknowledgement_commit_callback_registered || check_for_renew_acknowledgements {
            let event = ShareAcknowledgementEvent::new(
                acknowledgements_map,
                check_for_renew_acknowledgements,
                acquisition_lock_timeout_ms,
            );
            self.acknowledge_event_handler.add(event);
        }
    }

    /// Builds the request of `acknowledge_request_state` if it has
    /// acknowledgements to send and its backoff has elapsed, and fails them if
    /// it has expired.
    ///
    /// # Arguments
    ///
    /// - `acknowledge_request_state` - Contains the acknowledgements to be sent.
    /// - `current_time_ms` - The current time in ms.
    /// - `on_commit_async` - Boolean to denote if the acknowledgements came from a commitAsync or not.
    /// - `is_async_sent` - Boolean to indicate if the async request has been sent.
    ///
    /// # Returns
    ///
    /// Returns the request if it was built.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager#maybeBuildRequest")]
    fn maybe_build_request(
        &mut self,
        acknowledge_request_state: Option<&SharedAcknowledgeRequestState>,
        current_time_ms: i64,
        on_commit_async: bool,
        is_async_sent: &mut bool,
    ) -> Option<UnsentRequest> {
        // Java's `try { ... } finally { if (onCommitAsync) isAsyncSent.set(asyncSent); }`.
        let (request, async_sent) = self.do_maybe_build_request(acknowledge_request_state, current_time_ms);
        if on_commit_async {
            *is_async_sent = async_sent;
        }
        request
    }

    /// The body of [`Self::maybe_build_request`] without its `finally`:
    /// returns the request, if built, and Java's `asyncSent`.
    fn do_maybe_build_request(
        &mut self,
        acknowledge_request_state: Option<&SharedAcknowledgeRequestState>,
        current_time_ms: i64,
    ) -> (Option<UnsentRequest>, bool) {
        let Some(state_arc) = acknowledge_request_state else {
            return (None, true);
        };
        let mut state = lock(state_arc);
        if (!state.is_close_request() && state.is_empty()) || (state.is_close_request() && state.is_processed) {
            return (None, true);
        }

        if state.maybe_expire(self.time.as_ref()) {
            // Fill in TimeoutException
            let tips: Vec<TopicIdPartition> = state.incomplete_acknowledgements.keys().cloned().collect();
            for tip in tips {
                self.metrics_manager
                    .record_failed_acknowledgements(state.incomplete_acknowledgements_count(&tip));
                state.handle_acknowledge_timed_out(self, &tip);
            }
            state.incomplete_acknowledgements.clear();
            // Reset timer for any future processing on the same request state.
            state.maybe_reset_timer_and_request_state(self.time.as_ref());
            return (None, true);
        }

        if !state.can_send_request(current_time_ms) {
            // We wait for the backoff before we can send this request.
            return (None, false);
        }

        let Some(request) = state.build_request(self, state_arc) else {
            return (None, false);
        };

        state.on_send_attempt(current_time_ms);
        (Some(request), true)
    }

    /// Prunes the empty acknowledgementRequestStates in
    /// `acknowledge_request_states`.
    ///
    /// # Returns
    ///
    /// Returns true if there are still any acknowledgements left to be
    /// processed.
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager#checkAndRemoveCompletedAcknowledgements"
    )]
    fn check_and_remove_completed_acknowledgements(&mut self) -> bool {
        let mut are_any_acks_left = false;

        self.acknowledge_request_states.retain(|_, request_states| {
            let mut are_async_acks_left = true;
            let mut are_sync_acks_left = true;
            if !Self::is_request_state_in_progress(request_states.async_request()) {
                request_states.set_async_request(None);
                are_async_acks_left = false;
            }

            if !Self::are_request_states_in_progress(request_states.sync_request_queue()) {
                request_states.nullify_sync_request_queue();
                are_sync_acks_left = false;
            }

            if !Self::is_request_state_in_progress(request_states.close_request()) {
                request_states.set_close_request(None);
            }

            if are_async_acks_left || are_sync_acks_left {
                are_any_acks_left = true;
                true
            } else {
                request_states.close_request().is_some()
            }
        });

        if !self.acknowledge_request_states.is_empty() {
            are_any_acks_left = true;
        }
        are_any_acks_left
    }

    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager#isRequestStateInProgress")]
    fn is_request_state_in_progress(acknowledge_request_state: Option<&SharedAcknowledgeRequestState>) -> bool {
        match acknowledge_request_state {
            None => false,
            Some(state) => {
                let state = lock(state);
                if state.is_close_request() {
                    !state.is_processed
                } else {
                    !state.is_empty()
                }
            },
        }
    }

    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager#areRequestStatesInProgress")]
    fn are_request_states_in_progress(
        acknowledge_request_states: Option<&VecDeque<SharedAcknowledgeRequestState>>,
    ) -> bool {
        acknowledge_request_states
            .is_some_and(|states| states.iter().any(|state| Self::is_request_state_in_progress(Some(state))))
    }

    /// Groups `acknowledgements_map` by node, merging the acknowledgements of
    /// a partition given twice for one node. The shared first step of
    /// Java's `commitSync`, `commitAsync` and `acknowledgeOnClose`.
    fn acknowledgements_by_node(
        acknowledgements_map: impl IntoIterator<Item = (TopicIdPartition, NodeAcknowledgements)>,
    ) -> BTreeMap<i32, HashMap<TopicIdPartition, Acknowledgements>> {
        let mut acknowledgements_map_all_nodes: BTreeMap<i32, HashMap<TopicIdPartition, Acknowledgements>> =
            BTreeMap::new();
        for (tip, node_acks) in acknowledgements_map {
            let acks_map = acknowledgements_map_all_nodes.entry(node_acks.node_id()).or_default();
            match acks_map.get_mut(&tip) {
                Some(prev_acks) => {
                    prev_acks.merge(node_acks.acknowledgements());
                },
                None => {
                    acks_map.insert(tip, node_acks.into_acknowledgements());
                },
            }
        }
        acknowledgements_map_all_nodes
    }

    /// Enqueue an AcknowledgeRequestState to be picked up on the next poll
    ///
    /// # Arguments
    ///
    /// - `acknowledgements_map` - The acknowledgements to commit
    /// - `deadline_ms` - Time until which the request will be retried if it fails with
    ///   an expected retriable error.
    ///
    /// # Returns
    ///
    /// The future which completes when the acknowledgements finished
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager#commitSync")]
    pub(crate) fn commit_sync(
        &mut self,
        acknowledgements_map: impl IntoIterator<Item = (TopicIdPartition, NodeAcknowledgements)>,
        deadline_ms: i64,
    ) -> KafkaFutureImpl<CommitSyncResult> {
        self.handle_completed_requests();
        let result_count = Arc::new(AtomicI32::new(0));
        let future = KafkaFutureImpl::new();
        let result_handler = Arc::new(ResultHandler::with_remaining_results_future(
            Some(Arc::clone(&result_count)),
            Some(future.clone()),
        ));

        let mut acknowledgements_map_all_nodes = Self::acknowledgements_by_node(acknowledgements_map);

        let session_handlers: Vec<(i32, SharedSessionHandler)> = self
            .session_handlers
            .iter()
            .map(|(node_id, handler)| (*node_id, Arc::clone(handler)))
            .collect();
        for (node_id, session_handler) in session_handlers {
            let Some(node_acknowledgements) = acknowledgements_map_all_nodes.remove(&node_id) else {
                continue;
            };

            let mut acknowledgements_map_to_send = HashMap::new();

            self.acknowledge_request_states
                .entry(node_id)
                .or_insert_with(|| Tuple::new(None, None, None));

            // Add the incoming commitSync() request to the queue.
            for (tip, acks) in node_acknowledgements {
                result_count.fetch_add(1, Ordering::AcqRel);

                self.metrics_manager.record_acknowledgement_sent(count(&acks));
                debug!(
                    "Added sync acknowledge request for partition {} to node {node_id}",
                    tip.topic_partition()
                );
                acknowledgements_map_to_send.insert(tip, acks);
            }

            if !acknowledgements_map_to_send.is_empty() {
                let state = AcknowledgeRequestState::new(
                    self.time.as_ref(),
                    "ShareConsumeRequestManager:1",
                    deadline_ms,
                    self.retry_backoff_ms,
                    self.retry_backoff_max_ms,
                    session_handler,
                    node_id,
                    acknowledgements_map_to_send,
                    Arc::clone(&result_handler),
                    AcknowledgeRequestType::CommitSync,
                );
                if let Some(request_states) = self.acknowledge_request_states.get_mut(&node_id) {
                    request_states.add_sync_request(Arc::new(Mutex::new(state)));
                }
            }
        }

        if !acknowledgements_map_all_nodes.is_empty() {
            for node_acknowledgements in acknowledgements_map_all_nodes.values() {
                result_count
                    .fetch_add(i32::try_from(node_acknowledgements.len()).unwrap_or(i32::MAX), Ordering::AcqRel);
            }

            for (node_id, node_acknowledgements) in acknowledgements_map_all_nodes {
                for (tip, mut acks) in node_acknowledgements {
                    debug!("No share session handler for node {node_id}, failing acknowledgements for partition {tip}");
                    acks.complete(error_for(self.acknowledgements_cannot_be_sent_error(node_id, &tip)));
                    result_handler.complete(self, tip, Some(acks), AcknowledgeRequestType::CommitSync, true, None);
                }
            }
        }

        result_handler.complete_if_empty();
        future
    }

    /// Enqueue an AcknowledgeRequestState to be picked up on the next poll.
    ///
    /// # Arguments
    ///
    /// - `acknowledgements_map` - The acknowledgements to commit
    /// - `deadline_ms` - Time until which the request will be retried if it fails with
    ///   an expected retriable error.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager#commitAsync")]
    pub(crate) fn commit_async(
        &mut self,
        acknowledgements_map: impl IntoIterator<Item = (TopicIdPartition, NodeAcknowledgements)>,
        deadline_ms: i64,
    ) {
        self.handle_completed_requests();
        let result_handler = Arc::new(ResultHandler::new(None));

        let mut acknowledgements_map_all_nodes = Self::acknowledgements_by_node(acknowledgements_map);

        let session_handlers: Vec<(i32, SharedSessionHandler)> = self
            .session_handlers
            .iter()
            .map(|(node_id, handler)| (*node_id, Arc::clone(handler)))
            .collect();
        for (node_id, session_handler) in session_handlers {
            let Some(node_acknowledgements) = acknowledgements_map_all_nodes.remove(&node_id) else {
                continue;
            };

            let request_states = self
                .acknowledge_request_states
                .entry(node_id)
                .or_insert_with(|| Tuple::new(None, None, None));

            // Java builds the state from the first partition with an empty map
            // (`acknowledgementsMapForNode`) that the later partitions keep
            // filling; the state is created up front here and filled the same
            // way.
            let mut acknowledgements_map_for_node = HashMap::new();
            let async_request_state = request_states.async_request().cloned();
            for (tip, acks) in node_acknowledgements {
                self.metrics_manager.record_acknowledgement_sent(count(&acks));
                debug!(
                    "Added async acknowledge request for partition {} to node {node_id}",
                    tip.topic_partition()
                );

                match &async_request_state {
                    None => {
                        acknowledgements_map_for_node.insert(tip, acks);
                    },
                    Some(async_request_state) => {
                        let mut async_request_state = lock(async_request_state);
                        match async_request_state.acknowledgements_to_send.get_mut(&tip) {
                            Some(prev_acks) => {
                                prev_acks.merge(&acks);
                            },
                            None => {
                                async_request_state.acknowledgements_to_send.insert(tip, acks);
                            },
                        }
                    },
                }
            }

            if async_request_state.is_none() && !acknowledgements_map_for_node.is_empty() {
                let state = AcknowledgeRequestState::new(
                    self.time.as_ref(),
                    "ShareConsumeRequestManager:2",
                    deadline_ms,
                    self.retry_backoff_ms,
                    self.retry_backoff_max_ms,
                    session_handler,
                    node_id,
                    acknowledgements_map_for_node,
                    Arc::clone(&result_handler),
                    AcknowledgeRequestType::CommitAsync,
                );
                if let Some(request_states) = self.acknowledge_request_states.get_mut(&node_id) {
                    request_states.set_async_request(Some(Arc::new(Mutex::new(state))));
                }
            }
        }

        for (node_id, node_acknowledgements) in acknowledgements_map_all_nodes {
            for (tip, mut acks) in node_acknowledgements {
                debug!("No share session handler for node {node_id}, failing acknowledgements for partition {tip}");
                acks.complete(error_for(self.acknowledgements_cannot_be_sent_error(node_id, &tip)));
                self.maybe_send_share_acknowledgement_event(HashMap::from([(tip, acks)]), true, None);
            }
        }

        result_handler.complete_if_empty();
    }

    /// Enqueue the final AcknowledgeRequestState used to commit the final
    /// acknowledgements and close the share sessions.
    ///
    /// # Arguments
    ///
    /// - `acknowledgements_map` - The acknowledgements to commit
    /// - `deadline_ms` - Time until which the request will be retried if it fails with
    ///   an expected retriable error.
    ///
    /// # Returns
    ///
    /// The future which completes when the acknowledgements finished
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager#acknowledgeOnClose")]
    pub(crate) fn acknowledge_on_close(
        &mut self,
        acknowledgements_map: impl IntoIterator<Item = (TopicIdPartition, NodeAcknowledgements)>,
        deadline_ms: i64,
    ) -> KafkaFutureImpl<()> {
        self.handle_completed_requests();
        let result_count = Arc::new(AtomicI32::new(0));
        let result_handler = Arc::new(ResultHandler::with_remaining_results_future(
            Some(Arc::clone(&result_count)),
            None,
        ));

        self.closing = true;

        let mut acknowledgements_map_all_nodes = Self::acknowledgements_by_node(acknowledgements_map);

        // Add any waiting piggyback acknowledgements. Java shares these objects
        // with `fetchAcknowledgementsToSend`, which it leaves as it is; the copy
        // keeps that map unchanged too.
        for (node_id, node_acks) in &self.fetch_acknowledgements_to_send {
            let acks_map = acknowledgements_map_all_nodes.entry(*node_id).or_default();
            for (tip, acks) in node_acks {
                match acks_map.get_mut(tip) {
                    Some(prev_acks) => {
                        prev_acks.merge(acks);
                    },
                    None => {
                        acks_map.insert(tip.clone(), acks.clone());
                    },
                }
            }
        }

        let session_handlers: Vec<(i32, SharedSessionHandler)> = self
            .session_handlers
            .iter()
            .map(|(node_id, handler)| (*node_id, Arc::clone(handler)))
            .collect();
        for (node_id, session_handler) in session_handlers {
            let node_acknowledgements = match acknowledgements_map_all_nodes.remove(&node_id) {
                Some(node_acknowledgements) => {
                    for (tip, acknowledgements) in &node_acknowledgements {
                        result_count.fetch_add(1, Ordering::AcqRel);

                        self.metrics_manager.record_acknowledgement_sent(count(acknowledgements));
                        debug!(
                            "Added closing acknowledge request for partition {} to node {node_id}",
                            tip.topic_partition()
                        );
                    }
                    node_acknowledgements
                },
                None => HashMap::new(),
            };

            let request_states = self
                .acknowledge_request_states
                .entry(node_id)
                .or_insert_with(|| Tuple::new(None, None, None));

            // Ensure there is no close() request already present as they are blocking calls
            // and only one request can be active at a time.
            let existing_close_request = request_states.close_request().is_some()
                && Self::is_request_state_in_progress(request_states.close_request());
            if existing_close_request {
                error!(
                    "Attempt to call close() when there is an existing close request for node {node_id}-{} sync request(s)",
                    request_states.sync_request_queue().map_or(0, VecDeque::len)
                );
                for (tip, mut acks) in node_acknowledgements {
                    acks.complete(error_for(Errors::NotLeaderOrFollower));
                    self.maybe_send_share_acknowledgement_event(HashMap::from([(tip, acks)]), true, None);
                }
                self.close_future.complete_with_error(Error::local_illegal_state(format!(
                    "Attempt to call close() when there is an existing close request for node {node_id}"
                )));
            } else {
                // There can only be one close() happening at a time. So per node, there will be one acknowledge request state.
                let state = AcknowledgeRequestState::new(
                    self.time.as_ref(),
                    "ShareConsumeRequestManager:3",
                    deadline_ms,
                    self.retry_backoff_ms,
                    self.retry_backoff_max_ms,
                    session_handler,
                    node_id,
                    node_acknowledgements,
                    Arc::clone(&result_handler),
                    AcknowledgeRequestType::Close,
                );
                if let Some(request_states) = self.acknowledge_request_states.get_mut(&node_id) {
                    request_states.set_close_request(Some(Arc::new(Mutex::new(state))));
                }
            }
        }

        for (node_id, node_acknowledgements) in acknowledgements_map_all_nodes {
            for (tip, mut acks) in node_acknowledgements {
                debug!("No share session handler for node {node_id}, failing acknowledgements for partition {tip}");
                acks.complete(error_for(self.acknowledgements_cannot_be_sent_error(node_id, &tip)));
                self.maybe_send_share_acknowledgement_event(HashMap::from([(tip, acks)]), true, None);
            }
        }

        result_handler.complete_if_empty();
        self.close_future.clone()
    }

    /// Handles a `ShareFetch` response: completes the acknowledgements it
    /// carried, updates the leader and topic-id caches and adds the fetched
    /// data to the share fetch buffer.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager#handleShareFetchSuccess")]
    fn handle_share_fetch_success(
        &mut self,
        fetch_target: &Node,
        request_data: &ShareFetchRequestData,
        mut resp: ClientResponse,
    ) {
        // Java's `try { ... } finally { ... }`: the body never returns an error,
        // so the `finally` part runs after it.
        self.do_handle_share_fetch_success(fetch_target, request_data, &mut resp);

        debug!("Removing pending request for node {} - success", fetch_target.id());
        if self.is_share_acquire_mode_record_limit() && self.fetch_records_node_id == fetch_target.id() {
            self.fetch_records_node_id = -1;
        }
        self.nodes_with_pending_requests.remove(&fetch_target.id());

        self.maybe_remove_session_handler_on_close(fetch_target.id(), request_data.share_session_epoch);
    }

    /// The `try` block of [`Self::handle_share_fetch_success`].
    fn do_handle_share_fetch_success(
        &mut self,
        fetch_target: &Node,
        request_data: &ShareFetchRequestData,
        resp: &mut ClientResponse,
    ) {
        debug!("Completed ShareFetch request from node {} successfully", fetch_target.id());
        let Some(ConcreteResponse::ShareFetch(mut response)) = resp.take_response_body() else {
            // Java's cast to `ShareFetchResponse` would fail here; the callback
            // ends and only its `finally` runs.
            error!(
                "Unexpected response to a ShareFetch request from node {}, ignoring it",
                fetch_target.id()
            );
            return;
        };
        let handler = self.session_handler(fetch_target.id());

        self.metrics_manager
            .record_latency(resp.destination(), resp.request_latency_ms());

        let Some(handler) = handler else {
            error!(
                "Unable to find ShareSessionHandler for node {}. Ignoring ShareFetch response.",
                fetch_target.id()
            );
            return;
        };

        let request_version = resp.request_header().api_version();

        if !lock(&handler).handle_response_with_share_fetch_response(&response, request_version) {
            if response.error() == Errors::UnknownTopicId {
                self.metadata.request_update(false);
            }
            // Complete any in-flight acknowledgements with the error code from the response, unless the share session was lost,
            // in which case they are failed with NETWORK_EXCEPTION reflecting a loss of connectivity.
            let ack_error = if matches!(
                response.error(),
                Errors::ShareSessionNotFound | Errors::InvalidShareSessionEpoch
            ) {
                Errors::NetworkError
            } else {
                response.error()
            };
            if let Some(mut node_acknowledgements_in_flight) =
                self.fetch_acknowledgements_in_flight.remove(&fetch_target.id())
                && !node_acknowledgements_in_flight.is_empty()
            {
                for acks in node_acknowledgements_in_flight.values_mut() {
                    acks.complete(error_for(ack_error));
                    self.metrics_manager.record_failed_acknowledgements(count(acks));
                }
                self.maybe_send_share_acknowledgement_event(
                    node_acknowledgements_in_flight,
                    request_data.is_renew_ack,
                    None,
                );
            }
            return;
        }

        let response_acquisition_lock_timeout_ms = if response.data().acquisition_lock_timeout_ms > 0 {
            Some(response.data().acquisition_lock_timeout_ms)
        } else {
            None
        };
        let node_endpoints = std::mem::take(&mut response.data_mut().node_endpoints);

        // Java's `LinkedHashMap`: a partition given twice keeps its first
        // position and takes the later data (translation-checklist R5).
        let topic_names = self.metadata.topic_names();
        let mut response_data: indexmap::IndexMap<TopicIdPartition, share_fetch_response_data::PartitionData> =
            indexmap::IndexMap::new();
        for topic_response in std::mem::take(&mut response.data_mut().responses) {
            for partition in topic_response.partitions {
                if let Some(tip) =
                    self.lookup_topic_id(&topic_names, topic_response.topic_id, partition.partition_index)
                {
                    response_data.insert(tip, partition);
                }
            }
        }

        let partitions: HashSet<TopicPartition> =
            response_data.keys().map(|tip| tip.topic_partition().clone()).collect();
        let share_fetch_metrics_aggregator =
            Arc::new(ShareFetchMetricsAggregator::new(Arc::clone(&self.metrics_manager), partitions));

        let mut completed_fetches = Vec::with_capacity(response_data.len());
        let mut partitions_with_updated_leader_info: HashMap<TopicPartition, crate::metadata::LeaderIdAndEpoch> =
            HashMap::new();
        for (tip, partition_data) in response_data {
            debug!("ShareFetch for partition {tip} returned fetch data {partition_data:?}");

            if let Some(node_acknowledgements_in_flight) =
                self.fetch_acknowledgements_in_flight.get_mut(&fetch_target.id())
                && let Some(mut acks) = node_acknowledgements_in_flight.remove(&tip)
            {
                if partition_data.acknowledge_error_code != Errors::None.code() {
                    self.metrics_manager.record_failed_acknowledgements(count(&acks));
                }
                let acknowledge_error = Errors::for_code(partition_data.acknowledge_error_code);
                acks.complete(match partition_data.acknowledge_error_message.as_deref() {
                    Some(message) => acknowledge_error.error_with_message(message),
                    None => error_for(acknowledge_error),
                });
                let acks_map = HashMap::from([(tip.clone(), acks)]);
                self.maybe_send_share_acknowledgement_event(
                    acks_map,
                    request_data.is_renew_ack,
                    response_acquisition_lock_timeout_ms,
                );
            }

            let partition_error = Errors::for_code(partition_data.error_code);
            if matches!(partition_error, Errors::NotLeaderOrFollower | Errors::FencedLeaderEpoch) {
                debug!(
                    "For {tip}, received error {partition_error}, with leaderIdAndEpoch {:?} in ShareFetch",
                    partition_data.current_leader
                );
                if partition_data.current_leader.leader_id != -1 && partition_data.current_leader.leader_epoch != -1 {
                    partitions_with_updated_leader_info.insert(
                        tip.topic_partition().clone(),
                        crate::metadata::LeaderIdAndEpoch::new(
                            Some(partition_data.current_leader.leader_id),
                            Some(partition_data.current_leader.leader_epoch),
                        ),
                    );

                    self.maybe_update_leader_cache(
                        &tip,
                        partition_data.current_leader.leader_id,
                        partition_data.current_leader.leader_epoch,
                    );
                } else {
                    self.share_session_leader_map.remove(&tip);
                }
            } else if matches!(partition_error, Errors::UnknownTopicOrPartition | Errors::UnknownTopicId) {
                self.share_session_leader_map.remove(&tip);
                self.share_session_topic_id_map.remove(tip.topic_partition());
            }

            let acquired_records = !partition_data.acquired_records.is_empty();

            completed_fetches.push(ShareCompletedFetch::new(
                Arc::new(BufferSupplier::create()),
                fetch_target.id(),
                tip,
                partition_data,
                response_acquisition_lock_timeout_ms,
                Arc::clone(&share_fetch_metrics_aggregator),
                request_version,
            ));

            if acquired_records {
                self.fetch_more_records = false;
            }
        }

        if !completed_fetches.is_empty() {
            self.share_fetch_buffer.add(completed_fetches);
        }

        // Handle any acknowledgements which were not received in the response for this node.
        if let Some(node_acknowledgements_in_flight) = self.fetch_acknowledgements_in_flight.remove(&fetch_target.id())
        {
            for (partition, mut acknowledgements) in node_acknowledgements_in_flight {
                acknowledgements
                    .complete(Some(Error::InvalidRecordState(InvalidRecordStateError::new(INVALID_RESPONSE))));
                self.maybe_send_share_acknowledgement_event(HashMap::from([(partition, acknowledgements)]), true, None);
            }
        }

        if !partitions_with_updated_leader_info.is_empty() {
            let leader_nodes: Vec<Node> = node_endpoints
                .into_iter()
                .map(|e| Node::with_rack(e.node_id, e.host, e.port, e.rack))
                .filter(|e| e != Node::no_node())
                .collect();
            self.metadata
                .update_partition_leadership(&partitions_with_updated_leader_info, &leader_nodes);
        }
    }

    /// Handles a `ShareFetch` request that failed without a response: fails
    /// the acknowledgements it carried.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager#handleShareFetchFailure")]
    fn handle_share_fetch_failure(&mut self, fetch_target: &Node, request_data: &ShareFetchRequestData, error: &Error) {
        debug!(
            "Completed ShareFetch request from node {} unsuccessfully {}",
            fetch_target.id(),
            error.error()
        );
        if let Some(handler) = self.session_handler(fetch_target.id()) {
            lock(&handler).handle_error(error);
        }

        let topic_names = self.metadata.topic_names();
        for topic in &request_data.topics {
            for partition in &topic.partitions {
                let Some(tip) = self.lookup_topic_id(&topic_names, topic.topic_id, partition.partition_index) else {
                    continue;
                };

                let Some(mut acks) = self
                    .fetch_acknowledgements_in_flight
                    .get_mut(&fetch_target.id())
                    .and_then(|node_acknowledgements_in_flight| node_acknowledgements_in_flight.remove(&tip))
                else {
                    continue;
                };
                self.metrics_manager.record_failed_acknowledgements(count(&acks));
                if error.is_kafka_error() {
                    acks.complete(Some(error.clone()));
                } else {
                    acks.complete(error_for(Errors::UnknownServerError));
                }
                let acks_map = HashMap::from([(tip, acks)]);
                self.maybe_send_share_acknowledgement_event(acks_map, request_data.is_renew_ack, None);
            }
        }

        // Java's `finally`.
        debug!("Removing pending request for node {} - failed", fetch_target.id());
        if self.is_share_acquire_mode_record_limit() && self.fetch_records_node_id == fetch_target.id() {
            self.fetch_records_node_id = -1;
        }
        self.nodes_with_pending_requests.remove(&fetch_target.id());

        self.maybe_remove_session_handler_on_close(fetch_target.id(), request_data.share_session_epoch);
    }

    /// If the completed ShareFetch was closing the share session with a final
    /// epoch, the session handler is no longer required. A subsequent poll will
    /// establish a new session if partitions return to the node.
    ///
    /// # Translation notes
    ///
    /// Java passes the request data; only its share session epoch is read.
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager#maybeRemoveSessionHandlerOnClose"
    )]
    fn maybe_remove_session_handler_on_close(&mut self, node_id: i32, share_session_epoch: i32) {
        if share_session_epoch == ShareRequestMetadata::FINAL_EPOCH {
            debug!("Removing session handler for node {node_id} after closing empty share session");
            self.session_handlers.remove(&node_id);
        }
    }

    /// Handles a `ShareAcknowledge` response: completes, retries or fails the
    /// acknowledgements of the request state that sent it.
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager#handleShareAcknowledgeSuccess"
    )]
    fn handle_share_acknowledge_success(
        &mut self,
        fetch_target: &Node,
        request_data: &ShareAcknowledgeRequestData,
        acknowledge_request_state: &SharedAcknowledgeRequestState,
        mut resp: ClientResponse,
        response_completion_time_ms: i64,
    ) {
        let is_close_request = lock(acknowledge_request_state).is_close_request();
        // Java's `try { ... } finally { ... }`.
        self.do_handle_share_acknowledge_success(
            fetch_target,
            request_data,
            acknowledge_request_state,
            &mut resp,
            response_completion_time_ms,
        );

        debug!("Removing pending request for node {} - success", fetch_target.id());
        self.nodes_with_pending_requests.remove(&fetch_target.id());

        if is_close_request {
            debug!("Removing node from ShareSession {}", fetch_target.id());
            self.session_handlers.remove(&fetch_target.id());
        }
    }

    /// The `try` block of [`Self::handle_share_acknowledge_success`].
    fn do_handle_share_acknowledge_success(
        &mut self,
        fetch_target: &Node,
        request_data: &ShareAcknowledgeRequestData,
        acknowledge_request_state: &SharedAcknowledgeRequestState,
        resp: &mut ClientResponse,
        response_completion_time_ms: i64,
    ) {
        debug!(
            "Completed ShareAcknowledge request from node {} successfully",
            fetch_target.id()
        );
        let Some(ConcreteResponse::ShareAcknowledge(mut response)) = resp.take_response_body() else {
            // Java's cast to `ShareAcknowledgeResponse` would fail here; the
            // callback ends and only its `finally` runs.
            error!(
                "Unexpected response to a ShareAcknowledge request from node {}, ignoring it",
                fetch_target.id()
            );
            return;
        };
        let response_acquisition_lock_timeout_ms = if response.data().acquisition_lock_timeout_ms > 0 {
            Some(response.data().acquisition_lock_timeout_ms)
        } else {
            None
        };

        let mut partitions_with_updated_leader_info: HashMap<TopicPartition, crate::metadata::LeaderIdAndEpoch> =
            HashMap::new();
        let topic_names = self.metadata.topic_names();
        let mut state = lock(acknowledge_request_state);

        if state.is_close_request() {
            for topic_response in &response.data().responses {
                for partition_data in &topic_response.partitions {
                    let Some(tip) =
                        self.lookup_topic_id(&topic_names, topic_response.topic_id, partition_data.partition_index)
                    else {
                        continue;
                    };

                    if partition_data.error_code != Errors::None.code() {
                        self.metrics_manager
                            .record_failed_acknowledgements(state.in_flight_acknowledgements_count(&tip));
                    }

                    state.handle_acknowledge_error_code(
                        self,
                        &tip,
                        Errors::for_code(partition_data.error_code),
                        request_data.is_renew_ack,
                        response_acquisition_lock_timeout_ms,
                    );
                }
            }

            state.on_successful_attempt(response_completion_time_ms);
            state.processing_complete(self);
        } else {
            let well_formed = lock(&state.session_handler)
                .handle_response_with_share_acknowledge_response(&response, resp.request_header().api_version());
            if !well_formed {
                // Received a response-level error code.
                state.on_failed_attempt(response_completion_time_ms);

                let response_error = response.error().error();
                if response_error.as_ref().is_some_and(Error::is_retriable_error) {
                    // We retry the request until the timer expires, unless we are closing.
                    state.move_all_to_incomplete_acks();
                } else {
                    state.process_pending_in_flight_acknowledgements(
                        self,
                        response_error.unwrap_or_else(|| Error::new(response.error())),
                    );
                    state.processing_complete(self);
                }
            } else {
                let mut should_retry = false;

                // Check all partition-level error codes.
                for topic_response in &response.data().responses {
                    for partition_data in &topic_response.partitions {
                        let partition_error = Errors::for_code(partition_data.error_code);
                        let Some(tip) =
                            self.lookup_topic_id(&topic_names, topic_response.topic_id, partition_data.partition_index)
                        else {
                            continue;
                        };

                        self.handle_partition_error(
                            partition_data,
                            &mut partitions_with_updated_leader_info,
                            &mut state,
                            partition_error,
                            &tip,
                            &mut should_retry,
                            request_data.is_renew_ack,
                            response_acquisition_lock_timeout_ms,
                        );
                    }
                }

                self.process_retry_logic(&mut state, should_retry, response_completion_time_ms);
            }
        }
        drop(state);

        if !partitions_with_updated_leader_info.is_empty() {
            let leader_nodes: Vec<Node> = std::mem::take(&mut response.data_mut().node_endpoints)
                .into_iter()
                .map(|e| Node::with_rack(e.node_id, e.host, e.port, e.rack))
                .filter(|e| e != Node::no_node())
                .collect();
            self.metadata
                .update_partition_leadership(&partitions_with_updated_leader_info, &leader_nodes);
        }
    }

    /// Handles a `ShareAcknowledge` request that failed without a response:
    /// fails the acknowledgements it carried.
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager#handleShareAcknowledgeFailure"
    )]
    fn handle_share_acknowledge_failure(
        &mut self,
        fetch_target: &Node,
        request_data: &ShareAcknowledgeRequestData,
        acknowledge_request_state: &SharedAcknowledgeRequestState,
        error: &Error,
        response_completion_time_ms: i64,
    ) {
        let mut state = lock(acknowledge_request_state);
        debug!(
            "Completed ShareAcknowledge request from node {} unsuccessfully {}",
            fetch_target.id(),
            error.error()
        );
        lock(state.session_handler()).handle_error(error);
        state.on_failed_attempt(response_completion_time_ms);

        let topic_names = self.metadata.topic_names();
        for topic in &request_data.topics {
            for partition in &topic.partitions {
                let Some(tip) = self.lookup_topic_id(&topic_names, topic.topic_id, partition.partition_index) else {
                    continue;
                };

                self.metrics_manager
                    .record_failed_acknowledgements(state.in_flight_acknowledgements_count(&tip));
                state.handle_acknowledge_error_code(self, &tip, error.error(), request_data.is_renew_ack, None);
            }
        }

        state.processing_complete(self);
        let is_close_request = state.is_close_request();
        drop(state);

        // Java's `finally`.
        debug!("Removing pending request for node {} - failed", fetch_target.id());
        self.nodes_with_pending_requests.remove(&fetch_target.id());

        if is_close_request {
            debug!("Removing node from ShareSession {}", fetch_target.id());
            self.session_handlers.remove(&fetch_target.id());
        }
    }

    #[expect(clippy::too_many_arguments)]
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager#handlePartitionError")]
    fn handle_partition_error(
        &mut self,
        partition_data: &share_acknowledge_response_data::PartitionData,
        partitions_with_updated_leader_info: &mut HashMap<TopicPartition, crate::metadata::LeaderIdAndEpoch>,
        acknowledge_request_state: &mut AcknowledgeRequestState,
        partition_error: Errors,
        tip: &TopicIdPartition,
        should_retry: &mut bool,
        is_renew_ack: bool,
        acquisition_lock_timeout_ms: Option<i32>,
    ) {
        if let Some(partition_error_value) = partition_error.error() {
            let mut retry = false;
            if matches!(partition_error, Errors::NotLeaderOrFollower | Errors::FencedLeaderEpoch) {
                // If the leader has changed, there's no point in retrying the operation because the acquisition locks
                // will have been released. Instead, these records will be re-delivered once they get timed out on the broker.
                // If the topic or partition has been deleted, we do not retry the failed acknowledgements.
                self.update_leader_info_map(partition_data, partitions_with_updated_leader_info, partition_error, tip);
            } else if matches!(partition_error, Errors::UnknownTopicOrPartition | Errors::UnknownTopicId) {
                self.share_session_leader_map.remove(tip);
                self.share_session_topic_id_map.remove(tip.topic_partition());
            } else if partition_error_value.is_retriable_error() {
                retry = true;
            }

            if retry {
                if acknowledge_request_state.move_to_incomplete_acks(tip) {
                    *should_retry = true;
                }
            } else {
                self.metrics_manager
                    .record_failed_acknowledgements(acknowledge_request_state.in_flight_acknowledgements_count(tip));
                acknowledge_request_state.handle_acknowledge_error_code(self, tip, partition_error, is_renew_ack, None);
            }
        } else {
            acknowledge_request_state.handle_acknowledge_error_code(
                self,
                tip,
                partition_error,
                is_renew_ack,
                acquisition_lock_timeout_ms,
            );
        }
    }

    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager#processRetryLogic")]
    fn process_retry_logic(
        &self,
        acknowledge_request_state: &mut AcknowledgeRequestState,
        should_retry: bool,
        response_completion_time_ms: i64,
    ) {
        if should_retry {
            acknowledge_request_state.on_failed_attempt(response_completion_time_ms);

            // Check for any acknowledgements that did not receive a response.
            // These acknowledgements are failed with InvalidRecordStateException.
            acknowledge_request_state.process_pending_in_flight_acknowledgements(
                self,
                Error::InvalidRecordState(InvalidRecordStateError::new(INVALID_RESPONSE)),
            );
        } else {
            acknowledge_request_state.on_successful_attempt(response_completion_time_ms);
            acknowledge_request_state.processing_complete(self);
        }
    }

    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager#updateLeaderInfoMap")]
    fn update_leader_info_map(
        &mut self,
        partition_data: &share_acknowledge_response_data::PartitionData,
        partitions_with_updated_leader_info: &mut HashMap<TopicPartition, crate::metadata::LeaderIdAndEpoch>,
        partition_error: Errors,
        tip: &TopicIdPartition,
    ) {
        debug!(
            "For {tip}, received error {partition_error}, with leaderIdAndEpoch {:?} in ShareAcknowledge",
            partition_data.current_leader
        );
        if partition_data.current_leader.leader_id != -1 && partition_data.current_leader.leader_epoch != -1 {
            partitions_with_updated_leader_info.insert(
                tip.topic_partition().clone(),
                crate::metadata::LeaderIdAndEpoch::new(
                    Some(partition_data.current_leader.leader_id),
                    Some(partition_data.current_leader.leader_epoch),
                ),
            );

            self.maybe_update_leader_cache(
                tip,
                partition_data.current_leader.leader_id,
                partition_data.current_leader.leader_epoch,
            );
        } else {
            self.share_session_leader_map.remove(tip);
        }
    }

    /// Update the cache leader for a partition in a share session, only if the
    /// new leader epoch is newer than the currently cached epoch. This mirrors
    /// the rules applied by `Metadata::update_last_seen_epoch_if_newer`. A
    /// stale entry is never overwritten by an older epoch.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager#maybeUpdateLeaderCache")]
    fn maybe_update_leader_cache(&mut self, tip: &TopicIdPartition, leader_id: i32, leader_epoch: i32) {
        let old_leader = self.share_session_leader_map.get(tip);
        if old_leader.is_none_or(|old_leader| leader_epoch > old_leader.epoch) {
            self.share_session_leader_map
                .insert(tip.clone(), LeaderIdAndEpoch::new(leader_id, leader_epoch));
        }
    }

    /// Chooses the error used to fail acknowledgements which could not be
    /// sent to the node hosting the share session. If leadership has moved to
    /// a different live broker, the error is `NOT_LEADER_OR_FOLLOWER`.
    /// Otherwise, the error is `NETWORK_EXCEPTION`.
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager#acknowledgementsCannotBeSentError"
    )]
    fn acknowledgements_cannot_be_sent_error(&self, node_id: i32, tip: &TopicIdPartition) -> Errors {
        if let Some(leader_and_epoch) = self.share_session_leader_map.get(tip)
            && self.metadata.fetch().node_by_id(leader_and_epoch.leader_id).is_some()
        {
            if leader_and_epoch.leader_id != node_id {
                debug!("Node {node_id} is no longer the leader for partition {tip}, failing acknowledgements");
                return Errors::NotLeaderOrFollower;
            }
            return Errors::NetworkError;
        }

        match self.metadata.current_leader(tip.topic_partition()).leader() {
            Some(leader_node) => {
                if leader_node.id() != node_id {
                    debug!("Node {node_id} is no longer the leader for partition {tip}, failing acknowledgements");
                    return Errors::NotLeaderOrFollower;
                }
            },
            None => {
                debug!("No leader found for partition {tip}");
                self.metadata.request_update(false);
            },
        }

        Errors::NetworkError
    }

    /// The partition of `topic_id` and `partition_index`, named from the
    /// metadata or, failing that, from the names this manager recorded when it
    /// added the partition to a request.
    ///
    /// # Translation notes
    ///
    /// Java reads `metadata.topicNames()` on each call; the caller passes one
    /// snapshot for all the partitions of a response, which no step of the
    /// response handling changes before the lookups end.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager#lookupTopicId")]
    fn lookup_topic_id(
        &mut self,
        topic_names: &HashMap<Uuid, String>,
        topic_id: Uuid,
        partition_index: i32,
    ) -> Option<TopicIdPartition> {
        let topic_name = match topic_names.get(&topic_id) {
            Some(topic_name) => Some(topic_name.clone()),
            None => self.topic_names_map.remove(&IdAndPartition::new(topic_id, partition_index)),
        };
        match topic_name {
            Some(topic_name) => Some(TopicIdPartition::with_partition_topic(topic_id, partition_index, topic_name)),
            None => {
                error!("Topic name not found in metadata for topicId {topic_id} and partitionIndex {partition_index}");
                None
            },
        }
    }

    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager#partitionsToFetch")]
    fn partitions_to_fetch(&self) -> Vec<TopicPartition> {
        lock(&self.subscriptions).fetchable_partitions(|_| true)
    }

    /// The share session handler of `node`, if there is one.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager#sessionHandler")]
    pub(crate) fn session_handler(&self, node: i32) -> Option<SharedSessionHandler> {
        self.session_handlers.get(&node).cloned()
    }

    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager#hasCompletedFetches")]
    pub(crate) fn has_completed_fetches(&self) -> bool {
        !self.share_fetch_buffer.is_empty()
    }

    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager#closeInternal")]
    fn close_internal(&mut self) {
        #[cfg(test)]
        {
            self.close_internal_calls += 1;
        }
        // Java's `Utils.closeQuietly`: neither close returns an error.
        self.share_fetch_buffer.close();
        self.metrics_manager.close();
    }

    /// Closes the share fetch buffer and removes the share fetch metrics. A
    /// second call does nothing.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager#close")]
    pub(crate) fn close(&mut self) {
        if self.closed {
            return;
        }
        self.closed = true;
        self.close_internal();
    }

    /// Records the member id the share sessions are created for.
    ///
    /// # Translation notes
    ///
    /// Java's `Uuid.fromString` throws on a malformed id; the share membership
    /// manager generates the id with `Uuid::random_uuid`, so a malformed one
    /// cannot occur, and it is logged and ignored rather than panicking.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager#onMemberEpochUpdated")]
    pub(crate) fn on_member_epoch_updated(&mut self, _member_epoch_opt: Option<i32>, member_id: &str) {
        match Uuid::from_string(member_id) {
            Ok(member_id) => self.member_id = Some(member_id),
            Err(e) => error!("Ignoring member id {member_id} which is not a valid UUID: {e}"),
        }
    }

    /// The acknowledge request states of `node_id`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager#requestStates")]
    fn request_states(&self, node_id: i32) -> Option<&Tuple<SharedAcknowledgeRequestState>> {
        self.acknowledge_request_states.get(&node_id)
    }

    /// The node of the share session `tip` is fetched from, or -1.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "Java uses it only in ShareConsumeRequestManagerTest")
    )]
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager#shareSessionNodeId")]
    fn share_session_node_id(&self, tip: &TopicIdPartition) -> i32 {
        self.share_session_leader_map.get(tip).map_or(-1, |leader| leader.leader_id)
    }
}

impl RequestManager for ShareConsumeRequestManager {
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager#poll")]
    fn poll(&mut self, current_time_ms: i64) -> PollResult {
        self.handle_completed_requests();
        let Some(member_id) = self.member_id else {
            if self.closing && !self.close_future.is_done() {
                self.close_future.complete(());
            }
            return PollResult::empty();
        };

        // Send any pending acknowledgements before fetching more records.
        if let Some(poll_result) = self.process_acknowledgements(current_time_ms) {
            return poll_result;
        }

        if !self.fetch_more_records {
            return PollResult::empty();
        }

        // Iterate over the partitions to fetch, building a map from partition to leader node ID
        let mut handler_map: BTreeMap<i32, (Node, SharedSessionHandler)> = BTreeMap::new();
        let cluster = self.metadata.fetch();
        let topic_ids = self.metadata.topic_ids();
        for partition in self.partitions_to_fetch() {
            let tip = match self.share_session_topic_id_map.get(&partition) {
                Some(tip) => tip.clone(),
                None => {
                    let Some(topic_id) = topic_ids.get(partition.topic()) else {
                        debug!("Requesting metadata update for partition {partition} since topic ID is missing");
                        self.metadata.request_update(false);
                        continue;
                    };

                    let tip = TopicIdPartition::new(*topic_id, partition.clone());
                    self.share_session_topic_id_map.insert(partition.clone(), tip.clone());
                    tip
                },
            };

            let leader = self.share_session_leader_map.get(&tip);
            if leader.is_none_or(|leader| cluster.node_by_id(leader.leader_id).is_none()) {
                let leader_opt = self.metadata.current_leader(&partition);
                match leader_opt.leader() {
                    Some(leader) if cluster.node_by_id(leader.id()).is_some() => {
                        self.share_session_leader_map
                            .insert(tip, LeaderIdAndEpoch::new(leader.id(), leader_opt.epoch().unwrap_or(-1)));
                    },
                    _ => {
                        debug!(
                            "Requesting metadata update for partition {partition} since current leader node is missing"
                        );
                        self.metadata.request_update(false);
                        self.share_session_leader_map.remove(&tip);
                        continue;
                    },
                }
            }
        }

        let mut missing_nodes = BTreeSet::new();
        for partition in self.partitions_to_fetch() {
            let Some(tip) = self.share_session_topic_id_map.get(&partition).cloned() else {
                continue;
            };
            let Some(leader) = self.share_session_leader_map.get(&tip) else {
                continue;
            };
            let node_id = leader.leader_id;
            let Some(node) = cluster.node_by_id(node_id) else {
                self.share_session_leader_map.remove(&tip);
                missing_nodes.insert(node_id);
                continue;
            };
            if self.nodes_with_pending_requests.contains(&node_id) {
                trace!(
                    "Skipping fetch for partition {partition} because previous request to {node_id} has not been processed"
                );
            } else {
                let session_handlers = &mut self.session_handlers;
                let handler = Arc::clone(
                    &handler_map
                        .entry(node_id)
                        .or_insert_with(|| {
                            let handler = session_handlers
                                .entry(node_id)
                                .or_insert_with(|| Arc::new(Mutex::new(ShareSessionHandler::new(node_id, member_id))));
                            (node.clone(), Arc::clone(handler))
                        })
                        .1,
                );

                let mut acknowledgements_to_send = None;

                if let Some(node_acks_from_fetch_map) = self.fetch_acknowledgements_to_send.get_mut(&node_id)
                    && let Some(acks) = node_acks_from_fetch_map.remove(&tip)
                {
                    // Check if the share session epoch is valid for sending acknowledgements.
                    let guard = lock(&handler);
                    acknowledgements_to_send = self.maybe_add_acknowledgements(&guard, node, &tip, acks);
                }

                lock(&handler).add_partition_to_fetch(tip.clone(), acknowledgements_to_send);
                self.topic_names_map
                    .entry(IdAndPartition::new(tip.topic_id(), tip.partition()))
                    .or_insert_with(|| tip.topic().to_string());

                // If we have not chosen a node for fetching records yet, choose now, and rotate the assigned partitions
                // so the next poll starts on a different partition. This is only applicable for record_limit mode.
                if self.is_share_acquire_mode_record_limit() && self.fetch_records_node_id == -1 {
                    self.fetch_records_node_id = node_id;
                    lock(&self.subscriptions).move_partition_to_end(&partition);
                }

                debug!("Added fetch request for partition {tip} to node {node_id}");
            }
        }

        // Calculate the set of nodes which have records buffered for the consumer to consume and acknowledge.
        // Even when assignments change, we do not close the share sessions which have buffered records.
        // This also does bookkeeping of the nodes which had pending acknowledgements in case they have become
        // eligible for cleaning up.
        let buffered_nodes = self.share_fetch_buffer.buffered_nodes();

        // Iterate over the session handlers to see if there are acknowledgements to be sent for partitions
        // which are no longer part of the current subscription or which have disappeared from the metadata.
        // Also include session handlers which have no fetchable partitions so the share sessions can be
        // brought up to date. Finally, close share sessions for nodes which no longer have any share partitions
        // and which do not have buffered data.
        let session_handlers: Vec<(i32, SharedSessionHandler)> = self
            .session_handlers
            .iter()
            .map(|(node_id, handler)| (*node_id, Arc::clone(handler)))
            .collect();
        for (node_id, session_handler) in session_handlers {
            let Some(node) = cluster.node_by_id(node_id) else {
                missing_nodes.insert(node_id);
                continue;
            };
            if self.nodes_with_pending_requests.contains(&node_id) {
                trace!("Skipping fetch because previous request to {node_id} has not been processed");
            } else if self.is_session_close_candidate(node, &session_handler, &handler_map, &buffered_nodes) {
                debug!("Closing share session for node {node_id} which is now empty");
                lock(&session_handler).notify_close();
                handler_map.insert(node_id, (node.clone(), session_handler));
            } else {
                self.prepare_remaining_session_handler_for_request(node, &session_handler, &mut handler_map);
            }
        }

        // Iterate over the share session handlers and build a list of UnsentRequests.
        let mut requests = Vec::with_capacity(handler_map.len());
        for (_, (target, handler)) in handler_map {
            // For record_limit mode, we only send a full ShareFetch to a single node at a time. We prepare to build ShareFetch
            // requests for all nodes with session handlers to permit piggybacking of acknowledgements, and also to adjust the
            // topic-partitions in the share session, but if the request would contain neither of those, it can be skipped.
            let can_skip_if_request_empty =
                self.is_share_acquire_mode_record_limit() && target.id() != self.fetch_records_node_id;

            let request_builder = lock(&handler).new_share_fetch_builder(
                &self.group_id,
                &self.share_fetch_config,
                can_skip_if_request_empty,
            );
            let Some(request_builder) = request_builder else {
                trace!("Skipping ShareFetch request to send to node {}", target.id());
                continue;
            };

            trace!("Building ShareFetch request to send to node {}", target.id());

            self.nodes_with_pending_requests.insert(target.id());

            let request_data = request_builder.data().clone();
            let mut unsent_request = UnsentRequest::new(Box::new(request_builder), Some(target.clone()));
            if let Some(response_rx) = unsent_request.take_response_receiver() {
                self.pending_requests
                    .push(PendingRequest::ShareFetch { target, request_data, response_rx });
            }
            requests.push(unsent_request);
        }

        // Remove session handlers for nodes which are now missing from the cluster metadata.
        if !missing_nodes.is_empty() {
            self.remove_session_handlers_for_missing_nodes(&missing_nodes);
        }

        PollResult::with_requests(requests)
    }
}

/// Java's `ShareConsumeRequestManager implements MemberStateListener`.
/// Listeners are registered as `Arc<dyn MemberStateListener>` and called
/// through `&self`, while the manager is polled through `&mut self`, so the
/// listener is the manager behind the lock that also serializes its polls.
impl MemberStateListener for Mutex<ShareConsumeRequestManager> {
    fn on_member_epoch_updated(&self, member_epoch: Option<i32>, member_id: &str) {
        lock(self).on_member_epoch_updated(member_epoch, member_id);
    }
}

impl fmt::Debug for ShareConsumeRequestManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShareConsumeRequestManager")
            .field("group_id", &self.group_id)
            .field("member_id", &self.member_id)
            .field("session_handlers", &self.session_handlers.keys().collect::<Vec<_>>())
            .field("nodes_with_pending_requests", &self.nodes_with_pending_requests)
            .field("closing", &self.closing)
            .finish_non_exhaustive()
    }
}

/// Represents a request to acknowledge delivery that can be retried or
/// aborted.
#[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager$AcknowledgeRequestState")]
pub(crate) struct AcknowledgeRequestState {
    /// Java's `extends TimedRequestState`.
    base: TimedRequestState,
    /// The share session handler.
    session_handler: SharedSessionHandler,
    /// The node to send the request to.
    node_id: i32,
    /// The map of acknowledgements to send
    acknowledgements_to_send: HashMap<TopicIdPartition, Acknowledgements>,
    /// The map of acknowledgements to be retried in the next attempt.
    incomplete_acknowledgements: HashMap<TopicIdPartition, Acknowledgements>,
    /// The in-flight acknowledgements
    in_flight_acknowledgements: HashMap<TopicIdPartition, Acknowledgements>,
    /// This handles completing a future when all results are known.
    result_handler: Arc<ResultHandler>,
    /// Indicates whether this was part of commitAsync, commitSync or close operation.
    request_type: AcknowledgeRequestType,
    /// Boolean to indicate if the request has been processed.
    ///
    /// Set to true once we process the response and do not retry the request.
    ///
    /// Initialized to false every time we build a request.
    is_processed: bool,
    /// Timeout in milliseconds indicating how long the request would be retried if it fails with a retriable exception.
    timeout_ms: i64,
}

impl AcknowledgeRequestState {
    /// Creates the state of an acknowledge request to `node_id`.
    ///
    /// # Translation notes
    ///
    /// Java's inner class reads the manager's `time`; it is passed here, and
    /// the `LogContext` is dropped.
    #[expect(clippy::too_many_arguments)]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager$AcknowledgeRequestState#AcknowledgeRequestState"
    )]
    fn new(
        time: &dyn Time,
        owner: &str,
        deadline_ms: i64,
        retry_backoff_ms: i64,
        retry_backoff_max_ms: i64,
        session_handler: SharedSessionHandler,
        node_id: i32,
        acknowledgements_map: HashMap<TopicIdPartition, Acknowledgements>,
        result_handler: Arc<ResultHandler>,
        acknowledge_request_type: AcknowledgeRequestType,
    ) -> Self {
        // Java's `deadlineTimer(time, deadlineMs)`.
        let deadline_ms = TimedRequestState::deadline_for(time.milliseconds(), deadline_ms);
        let base = TimedRequestState::new(owner, retry_backoff_ms, retry_backoff_max_ms, deadline_ms);
        let timeout_ms = base.remaining_ms(time.milliseconds());
        Self {
            base,
            session_handler,
            node_id,
            acknowledgements_to_send: acknowledgements_map,
            incomplete_acknowledgements: HashMap::new(),
            in_flight_acknowledgements: HashMap::new(),
            result_handler,
            request_type: acknowledge_request_type,
            is_processed: false,
            timeout_ms,
        }
    }

    /// Builds the request carrying the acknowledgements to send (or those to
    /// retry), and records them as in flight.
    ///
    /// # Translation notes
    ///
    /// Java's inner class reaches the manager's fields; the manager and the
    /// shared handle of this state, which the response handling needs, are
    /// passed here.
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager$AcknowledgeRequestState#buildRequest"
    )]
    fn build_request(
        &mut self,
        manager: &mut ShareConsumeRequestManager,
        this: &SharedAcknowledgeRequestState,
    ) -> Option<UnsentRequest> {
        // If the node is no longer in the cluster metadata, we can never send to it
        let Some(node_to_send) = manager.metadata.fetch().node_by_id(self.node_id).cloned() else {
            self.fail_pending_acknowledgements_cannot_be_sent(manager);
            return None;
        };

        let mut session_handler = lock(&self.session_handler);
        // If this is the closing request, close the share session by setting the final epoch
        if self.is_close_request() {
            session_handler.notify_close();
        }

        let final_acknowledgements_to_send = if self.incomplete_acknowledgements.is_empty() {
            &self.acknowledgements_to_send
        } else {
            &self.incomplete_acknowledgements
        };

        for (tip, acknowledgements) in final_acknowledgements_to_send {
            session_handler.add_partition_to_acknowledge_only(tip.clone(), acknowledgements.clone());
        }

        let request_builder = session_handler.new_share_acknowledge_builder(&manager.group_id);
        drop(session_handler);

        self.is_processed = false;

        let Some(request_builder) = request_builder else {
            self.fail_pending_acknowledgements_cannot_be_sent(manager);
            return None;
        };

        manager.nodes_with_pending_requests.insert(self.node_id);

        trace!("Building acknowledgements to send : {final_acknowledgements_to_send:?}");

        // Java copies the map into `inFlightAcknowledgements` and then clears
        // its source; the entries are moved instead.
        let final_acknowledgements_to_send = if self.incomplete_acknowledgements.is_empty() {
            std::mem::take(&mut self.acknowledgements_to_send)
        } else {
            std::mem::take(&mut self.incomplete_acknowledgements)
        };
        self.in_flight_acknowledgements.extend(final_acknowledgements_to_send);

        let request_data = request_builder.data().clone();
        let mut unsent_request = UnsentRequest::new(Box::new(request_builder), Some(node_to_send.clone()));
        let completion_time_ms = unsent_request.handler().completion_time_ms_cell();
        if let Some(response_rx) = unsent_request.take_response_receiver() {
            manager.pending_requests.push(PendingRequest::ShareAcknowledge {
                target: node_to_send,
                request_data,
                state: Arc::clone(this),
                completion_time_ms,
                response_rx,
            });
        }
        Some(unsent_request)
    }

    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager$AcknowledgeRequestState#getInFlightAcknowledgementsCount"
    )]
    fn in_flight_acknowledgements_count(&self, tip: &TopicIdPartition) -> i32 {
        self.in_flight_acknowledgements.get(tip).map_or(0, count)
    }

    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager$AcknowledgeRequestState#getIncompleteAcknowledgementsCount"
    )]
    fn incomplete_acknowledgements_count(&self, tip: &TopicIdPartition) -> i32 {
        self.incomplete_acknowledgements.get(tip).map_or(0, count)
    }

    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "Java uses it only in ShareConsumeRequestManagerTest")
    )]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager$AcknowledgeRequestState#getAcknowledgementsToSendCount"
    )]
    fn acknowledgements_to_send_count(&self, tip: &TopicIdPartition) -> i32 {
        self.acknowledgements_to_send.get(tip).map_or(0, count)
    }

    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager$AcknowledgeRequestState#isEmpty"
    )]
    fn is_empty(&self) -> bool {
        self.acknowledgements_to_send.is_empty()
            && self.incomplete_acknowledgements.is_empty()
            && self.in_flight_acknowledgements.is_empty()
    }

    /// Resets the timer with the configured timeout and resets the
    /// RequestState. This is only applicable for commitAsync() requests as
    /// these states could be re-used.
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager$AcknowledgeRequestState#maybeResetTimerAndRequestState"
    )]
    fn maybe_reset_timer_and_request_state(&mut self, time: &dyn Time) {
        if self.request_type == AcknowledgeRequestType::CommitAsync {
            // Java's `resetTimeout(timeoutMs)`.
            self.base.reset_deadline(time.milliseconds().saturating_add(self.timeout_ms));
            self.base.reset();
        }
    }

    /// Sets the error code in the acknowledgements and sends the response
    /// through a background event.
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager$AcknowledgeRequestState#handleAcknowledgeErrorCode"
    )]
    fn handle_acknowledge_error_code(
        &mut self,
        manager: &ShareConsumeRequestManager,
        tip: &TopicIdPartition,
        acknowledge_error_code: Errors,
        check_for_renew_acknowledgements: bool,
        acquisition_lock_timeout_ms: Option<i32>,
    ) {
        match self.in_flight_acknowledgements.remove(tip) {
            Some(mut acks) => {
                acks.complete(error_for(acknowledge_error_code));
                self.result_handler.complete(
                    manager,
                    tip.clone(),
                    Some(acks),
                    self.request_type,
                    check_for_renew_acknowledgements,
                    acquisition_lock_timeout_ms,
                );
            },
            None => error!("Invalid partition {tip} received in ShareAcknowledge response"),
        }
    }

    /// Sets the error code for the acknowledgements which were timed out
    /// after some retries.
    ///
    /// # Translation notes
    ///
    /// Java leaves the entry in `incompleteAcknowledgements`, which its only
    /// caller clears right after; Rust takes it out, handing it to the result
    /// handler.
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager$AcknowledgeRequestState#handleAcknowledgeTimedOut"
    )]
    fn handle_acknowledge_timed_out(&mut self, manager: &ShareConsumeRequestManager, tip: &TopicIdPartition) {
        if let Some(mut acks) = self.incomplete_acknowledgements.remove(tip) {
            acks.complete(error_for(Errors::RequestTimedOut));
            // We do not know whether this is a renew ack, but handling the error as if it were, will ensure
            // that we do not leave dangling acknowledgements
            self.result_handler
                .complete(manager, tip.clone(), Some(acks), self.request_type, true, None);
        }
    }

    /// Fail all remaining acknowledgements when they cannot be sent. This can
    /// happen when network connectivity is lost with the leader and the share
    /// session is lost, they are failed with `NETWORK_EXCEPTION`. If the leader
    /// has moved to a different live broker, they are failed with
    /// `NOT_LEADER_OR_FOLLOWER`.
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager$AcknowledgeRequestState#failPendingAcknowledgementsCannotBeSent"
    )]
    fn fail_pending_acknowledgements_cannot_be_sent(&mut self, manager: &ShareConsumeRequestManager) {
        let acknowledgements_map_to_clear = if self.incomplete_acknowledgements.is_empty() {
            std::mem::take(&mut self.acknowledgements_to_send)
        } else {
            std::mem::take(&mut self.incomplete_acknowledgements)
        };

        for (tip, mut acks) in acknowledgements_map_to_clear {
            acks.complete(error_for(manager.acknowledgements_cannot_be_sent_error(self.node_id, &tip)));
            // We do not know whether this is a renew ack, but handling the error as if it were, will ensure
            // that we do not leave dangling acknowledgements
            self.result_handler
                .complete(manager, tip, Some(acks), self.request_type, true, None);
        }
        self.processing_complete(manager);
    }

    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager$AcknowledgeRequestState#sessionHandler"
    )]
    fn session_handler(&self) -> &SharedSessionHandler {
        &self.session_handler
    }

    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager$AcknowledgeRequestState#processingComplete"
    )]
    fn processing_complete(&mut self, manager: &ShareConsumeRequestManager) {
        // If there are any pending inFlightAcknowledgements after processing the response, we fail them with an InvalidRecordStateException.
        self.process_pending_in_flight_acknowledgements(
            manager,
            Error::InvalidRecordState(InvalidRecordStateError::new(INVALID_RESPONSE)),
        );
        self.result_handler.complete_if_empty();
        self.is_processed = true;
        self.maybe_reset_timer_and_request_state(manager.time.as_ref());
    }

    /// Fail any existing in-flight acknowledgements with the given error and
    /// clear the map. We also send a background event to update the
    /// `AcknowledgementCommitCallback`.
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager$AcknowledgeRequestState#processPendingInFlightAcknowledgements"
    )]
    fn process_pending_in_flight_acknowledgements(&mut self, manager: &ShareConsumeRequestManager, error: Error) {
        if !self.in_flight_acknowledgements.is_empty() {
            for (partition, mut acknowledgements) in std::mem::take(&mut self.in_flight_acknowledgements) {
                acknowledgements.complete(Some(error.clone()));
                // We do not know whether this is a renew ack, but handling the error as if it were, will ensure
                // that we do not leave dangling acknowledgements
                self.result_handler
                    .complete(manager, partition, Some(acknowledgements), self.request_type, true, None);
            }
        }
    }

    /// Moves all the in-flight acknowledgements to incomplete acknowledgements
    /// to retry in the next request.
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager$AcknowledgeRequestState#moveAllToIncompleteAcks"
    )]
    fn move_all_to_incomplete_acks(&mut self) {
        self.incomplete_acknowledgements
            .extend(std::mem::take(&mut self.in_flight_acknowledgements));
    }

    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager$AcknowledgeRequestState#maybeExpire"
    )]
    fn maybe_expire(&self, time: &dyn Time) -> bool {
        self.base.num_attempts() > 0 && self.base.is_expired(time.milliseconds())
    }

    /// Moves the in-flight acknowledgements for a given partition to
    /// incomplete acknowledgements to retry in the next request.
    ///
    /// # Arguments
    ///
    /// - `tip` - The TopicIdPartition for which we move the acknowledgements.
    ///
    /// # Returns
    ///
    /// True if the partition was sent in the request.
    ///
    /// False if the partition was not part of the request, we log an error and
    /// ignore such partitions.
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager$AcknowledgeRequestState#moveToIncompleteAcks"
    )]
    fn move_to_incomplete_acks(&mut self, tip: &TopicIdPartition) -> bool {
        match self.in_flight_acknowledgements.remove(tip) {
            Some(acks) => {
                self.incomplete_acknowledgements.insert(tip.clone(), acks);
                true
            },
            None => {
                error!("Invalid partition {tip} received in ShareAcknowledge response");
                false
            },
        }
    }

    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager$AcknowledgeRequestState#isCloseRequest"
    )]
    fn is_close_request(&self) -> bool {
        self.request_type == AcknowledgeRequestType::Close
    }

    // Java's inherited `RequestState` / `TimedRequestState` methods.

    fn can_send_request(&self, current_time_ms: i64) -> bool {
        self.base.can_send_request(current_time_ms)
    }

    fn on_send_attempt(&mut self, current_time_ms: i64) {
        self.base.on_send_attempt(current_time_ms);
    }

    fn on_successful_attempt(&mut self, current_time_ms: i64) {
        self.base.on_successful_attempt(current_time_ms);
    }

    fn on_failed_attempt(&mut self, current_time_ms: i64) {
        self.base.on_failed_attempt(current_time_ms);
    }
}

/// Sends a ShareAcknowledgementEvent event to the application when it is
/// done processing all the remaining acknowledgement request states. Also
/// manages completing the future for synchronous acknowledgement commit by
/// counting down the results as they are known and completing the future at
/// the end.
#[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager$ResultHandler")]
pub(crate) struct ResultHandler {
    result: Mutex<CommitSyncResult>,
    remaining_results: Option<Arc<AtomicI32>>,
    future: Option<KafkaFutureImpl<CommitSyncResult>>,
}

impl ResultHandler {
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager$ResultHandler#ResultHandler")]
    fn new(future: Option<KafkaFutureImpl<CommitSyncResult>>) -> Self {
        Self::with_remaining_results_future(None, future)
    }

    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager$ResultHandler#ResultHandler")]
    fn with_remaining_results_future(
        remaining_results: Option<Arc<AtomicI32>>,
        future: Option<KafkaFutureImpl<CommitSyncResult>>,
    ) -> Self {
        Self { result: Mutex::new(HashMap::new()), remaining_results, future }
    }

    /// Handle the result of a ShareAcknowledge request sent to one or more
    /// nodes and signal the completion when all results are known.
    ///
    /// # Translation notes
    ///
    /// Java's non-static inner class reaches the manager to send the event;
    /// the manager is passed here.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager$ResultHandler#complete")]
    fn complete(
        &self,
        manager: &ShareConsumeRequestManager,
        partition: TopicIdPartition,
        acknowledgements: Option<Acknowledgements>,
        r#type: AcknowledgeRequestType,
        check_for_renew_acknowledgements: bool,
        acquisition_lock_timeout_ms: Option<i32>,
    ) {
        if r#type == AcknowledgeRequestType::CommitAsync {
            if let Some(acknowledgements) = acknowledgements {
                manager.maybe_send_share_acknowledgement_event(
                    HashMap::from([(partition, acknowledgements)]),
                    check_for_renew_acknowledgements,
                    acquisition_lock_timeout_ms,
                );
            }
        } else {
            let mut result = lock(&self.result);
            if let Some(acknowledgements) = acknowledgements {
                result.insert(partition, acknowledgements);
            }
            if let Some(remaining_results) = &self.remaining_results
                && remaining_results.fetch_sub(1, Ordering::AcqRel) == 1
            {
                // Java hands the same map to the event and to the future.
                manager.maybe_send_share_acknowledgement_event(
                    result.clone(),
                    check_for_renew_acknowledgements,
                    acquisition_lock_timeout_ms,
                );
                if let Some(future) = &self.future {
                    future.complete(result.clone());
                }
            }
        }
    }

    /// Handles the case where there are no results pending after
    /// initialization.
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager$ResultHandler#completeIfEmpty"
    )]
    fn complete_if_empty(&self) {
        if let Some(remaining_results) = &self.remaining_results
            && remaining_results.load(Ordering::Acquire) == 0
            && let Some(future) = &self.future
        {
            future.complete(lock(&self.result).clone());
        }
    }
}

/// The acknowledge request states of one node: the `commitAsync` one, the
/// queue of `commitSync` ones and the close one.
#[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager$Tuple")]
pub(crate) struct Tuple<V> {
    async_request: Option<V>,
    sync_request_queue: Option<VecDeque<V>>,
    close_request: Option<V>,
}

impl<V> Tuple<V> {
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager$Tuple#Tuple")]
    fn new(async_request: Option<V>, sync_request_queue: Option<VecDeque<V>>, close_request: Option<V>) -> Self {
        Self { async_request, sync_request_queue, close_request }
    }

    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager$Tuple#setAsyncRequest")]
    fn set_async_request(&mut self, async_request: Option<V>) {
        self.async_request = async_request;
    }

    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager$Tuple#nullifySyncRequestQueue"
    )]
    fn nullify_sync_request_queue(&mut self) {
        self.sync_request_queue = None;
    }

    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager$Tuple#addSyncRequest")]
    fn add_sync_request(&mut self, sync_request: V) {
        self.sync_request_queue
            .get_or_insert_with(VecDeque::new)
            .push_back(sync_request);
    }

    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager$Tuple#setCloseRequest")]
    fn set_close_request(&mut self, close_request: Option<V>) {
        self.close_request = close_request;
    }

    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager$Tuple#getAsyncRequest")]
    fn async_request(&self) -> Option<&V> {
        self.async_request.as_ref()
    }

    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager$Tuple#getSyncRequestQueue")]
    fn sync_request_queue(&self) -> Option<&VecDeque<V>> {
        self.sync_request_queue.as_ref()
    }

    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager$Tuple#getCloseRequest")]
    fn close_request(&self) -> Option<&V> {
        self.close_request.as_ref()
    }
}

/// A topic id and partition index, the key of the topic names the manager
/// records when it adds a partition to a request.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager$IdAndPartition")]
struct IdAndPartition {
    topic_id: Uuid,
    partition_index: i32,
}

impl IdAndPartition {
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager$IdAndPartition#IdAndPartition"
    )]
    fn new(topic_id: Uuid, partition_index: i32) -> Self {
        Self { topic_id, partition_index }
    }
}

/// The leader of a partition in a share session, and its epoch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager$LeaderIdAndEpoch")]
struct LeaderIdAndEpoch {
    leader_id: i32,
    epoch: i32,
}

impl LeaderIdAndEpoch {
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager$LeaderIdAndEpoch#LeaderIdAndEpoch"
    )]
    fn new(leader_id: i32, epoch: i32) -> Self {
        Self { leader_id, epoch }
    }
}

/// The operation an acknowledge request belongs to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager$AcknowledgeRequestType")]
pub(crate) enum AcknowledgeRequestType {
    CommitAsync,
    CommitSync,
    Close,
}

impl AcknowledgeRequestType {
    /// The id of the type.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager$AcknowledgeRequestType#id")]
    #[cfg_attr(not(test), expect(dead_code, reason = "Java declares the id but reads it nowhere"))]
    pub(crate) fn id(self) -> i8 {
        match self {
            Self::CommitAsync => 0,
            Self::CommitSync => 1,
            Self::Close => 2,
        }
    }
}

impl fmt::Display for AcknowledgeRequestType {
    /// The constant's name in lower case (Java's `toString`).
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::CommitAsync => "commit_async",
            Self::CommitSync => "commit_sync",
            Self::Close => "close",
        })
    }
}

#[cfg(test)]
mod tests {
    //! Translated from `ShareConsumeRequestManagerTest` (70 `@Test`, 6
    //! `@ParameterizedTest`, each looped over its exact Java source).
    //!
    //! # Harness
    //!
    //! Java drives a `TestableShareConsumeRequestManager` over a real
    //! `NetworkClientDelegate` and a `MockClient`. The fixture below does the
    //! same; `network_poll` is Java's `networkClientDelegate.poll(time.timer(0))`
    //! followed by [`ShareConsumeRequestManager::handle_completed_requests`],
    //! which runs the `whenComplete` callbacks Java runs inside that poll.
    //! Java's `TestableShareAcknowledgementEventHandler` records each event as it
    //! is added; the fixture drains the real handler into the same two
    //! collections (`completed_acknowledgements`, `renewed_records`) before
    //! each read.
    //!
    //! Java compares `Acknowledgements` by identity (`assertEquals` /
    //! `assertSame` on the object it passed in). The Rust manager owns copies,
    //! so the tests compare the acknowledged offsets and types instead
    //! (`assert_same_acknowledgements`), and the error separately where Java
    //! checks it. Java's `assertEquals(Errors.X.exception(), e)` compares the
    //! cached instance; the tests compare the error code and message.
    //!
    //! `ShareInFlightBatch` hands each record out once (D5), so a test that
    //! reads `fetch.records()` several times in Java binds them once.

    use std::collections::{HashMap, HashSet};

    use bytes::Bytes;
    use indexmap::IndexSet;
    use tokio::sync::mpsc;

    use super::*;
    use crate::MockClient;
    use crate::common::compress::Compression;
    use crate::common::header::{Header, Headers, RecordHeader};
    use crate::common::internals::ClusterResourceListeners;
    use crate::common::metrics::Metrics;
    use crate::common::record::TimestampType;
    use crate::common::record::internal::{
        MemoryRecords, MemoryRecordsBuilderOptionsBuilder, RecordBatch, SimpleRecord,
    };
    use crate::common::requests::metadata_response::{PartitionMetadata, TopicMetadata};
    use crate::common::requests::{
        AbstractRequest, MetadataResponse, RequestTestUtils, ShareAcknowledgeResponse, ShareFetchResponse,
    };
    use crate::common::serialization::ByteArrayDeserializer;
    use crate::common::utils::MockTime;
    use crate::common::{Metric, MetricNameTemplate};
    use crate::consumer::internals::events::BackgroundEventHandler;
    use crate::consumer::internals::share_completed_fetch::tests::acquired_records;
    use crate::consumer::internals::{
        AutoOffsetResetStrategy, ConsumerUtils, Deserializers, NetworkClientDelegate, ShareFetch, ShareFetchCollector,
        ShareFetchMetricsRegistry,
    };
    use crate::consumer::{AcknowledgeType, ConsumerConfig, ConsumerRecord};
    use crate::share_acknowledge_response_data::{
        LeaderIdAndEpoch as AckLeaderIdAndEpoch, PartitionData as AckPartitionData,
    };
    use crate::share_fetch_response_data::{
        AcquiredRecords, LeaderIdAndEpoch as FetchLeaderIdAndEpoch, PartitionData as FetchPartitionData,
    };
    use crate::{ShareAcknowledgeRequestData, ShareFetchRequestData};

    const TOPIC_NAME: &str = "test";
    const TOPIC_NAME2: &str = "test-2";
    const GROUP_ID: &str = "test-group";
    const VALID_LEADER_EPOCH: i32 = 0;
    const RETRY_BACKOFF_MS: i64 = 100;
    const REQUEST_TIMEOUT_MS: i32 = 30000;
    const DEFAULT_API_TIMEOUT_MS: i64 = 60000;

    type Record = ConsumerRecord<Vec<u8>, Vec<u8>>;
    type Fetch = ShareFetch<Vec<u8>, Vec<u8>>;

    /// A request the manager built, as `pollResult.unsentRequests` exposes it
    /// to Java (`node()`, and the builder's `data()` through a cast).
    struct SentRequest {
        node: Node,
        request: AbstractRequest,
    }

    impl SentRequest {
        fn of(unsent: &mut UnsentRequest) -> Self {
            let node = unsent.node().cloned().expect("the manager addresses every request to a node");
            let request = unsent
                .request_builder_mut()
                .expect("not sent yet")
                .build()
                .expect("the request builds");
            Self { node, request }
        }

        /// `(ShareFetchRequest.Builder) unsentRequest.requestBuilder()).data()`.
        fn share_fetch(&self) -> &ShareFetchRequestData {
            match &self.request {
                AbstractRequest::ShareFetch(request) => request.data(),
                other => panic!("expected a ShareFetch request, got {other:?}"),
            }
        }

        /// `(ShareAcknowledgeRequest.Builder) unsentRequest.requestBuilder()).data()`.
        fn share_acknowledge(&self) -> &ShareAcknowledgeRequestData {
            match &self.request {
                AbstractRequest::ShareAcknowledge(request) => request.data(),
                other => panic!("expected a ShareAcknowledge request, got {other:?}"),
            }
        }
    }

    /// Java's test fields, `setup()` and `buildRequestManager(...)` /
    /// `buildDependencies(...)`.
    struct Fixture {
        topic_id: Uuid,
        tp0: TopicPartition,
        tip0: TopicIdPartition,
        tp1: TopicPartition,
        tip1: TopicIdPartition,
        t2p0: TopicPartition,
        t2ip0: TopicIdPartition,
        topic_ids: HashMap<String, Uuid>,
        /// Java's `MockTime(1, 0, 0)`: every read advances the clock by 1 ms.
        time: Arc<MockTime>,
        subscriptions: Arc<Mutex<SubscriptionState>>,
        metadata: Arc<ShareConsumerMetadata>,
        metrics: Arc<Metrics>,
        share_fetch_metrics_registry: ShareFetchMetricsRegistry,
        network_client_delegate: NetworkClientDelegate<MockClient>,
        manager: ShareConsumeRequestManager,
        deserializers: Arc<Deserializers<Vec<u8>, Vec<u8>>>,
        share_fetch_collector: ShareFetchCollector<Vec<u8>, Vec<u8>>,
        acknowledgement_event_handler: Arc<ShareAcknowledgementEventHandler>,
        completed_acknowledgements: Vec<HashMap<TopicIdPartition, Acknowledgements>>,
        renewed_records: HashSet<i64>,
        records: Bytes,
        acquired_records: Vec<AcquiredRecords>,
        empty_acquired_records: Vec<AcquiredRecords>,
    }

    impl Fixture {
        /// `buildRequestManager()`.
        fn new() -> Self {
            Self::with_acquire_mode(ShareAcquireMode::BatchOptimized)
        }

        /// `buildRequestManager(ShareAcquireMode)`.
        fn with_acquire_mode(share_acquire_mode: ShareAcquireMode) -> Self {
            Self::with_member_id(Some(&Uuid::random_uuid().to_string()), share_acquire_mode)
        }

        /// `buildRequestManager(MetricConfig, Deserializer, Deserializer, String memberId, ShareAcquireMode)`.
        fn with_member_id(member_id: Option<&str>, share_acquire_mode: ShareAcquireMode) -> Self {
            let topic_id = Uuid::random_uuid();
            let topic_id2 = Uuid::random_uuid();
            let tp0 = TopicPartition::new(TOPIC_NAME.to_string(), 0);
            let tp1 = TopicPartition::new(TOPIC_NAME.to_string(), 1);
            let t2p0 = TopicPartition::new(TOPIC_NAME2.to_string(), 0);
            let topic_ids = HashMap::from([(TOPIC_NAME.to_string(), topic_id), (TOPIC_NAME2.to_string(), topic_id2)]);

            // buildDependencies
            let time = Arc::new(MockTime::with_auto_tick_ms_current_time_ms_current_high_res_time_ns(1, 0, 0));
            let subscriptions = Arc::new(Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::EARLIEST)));
            let metadata = Arc::new(ShareConsumerMetadata::new(
                0,
                0,
                i64::MAX,
                false,
                Arc::clone(&subscriptions),
                ClusterResourceListeners::new(),
            ));
            let client = MockClient::new(Arc::clone(&time) as Arc<dyn Time>);
            let metrics = Arc::new(Metrics::with_time(Arc::clone(&time) as Arc<dyn Time>));
            let share_fetch_metrics_registry = ShareFetchMetricsRegistry::with_tags_metric_grp_prefix(
                IndexSet::new(),
                &format!("{}{GROUP_ID}", ConsumerUtils::CONSUMER_SHARE_METRIC_GROUP_PREFIX),
            );
            let metrics_manager = Arc::new(ShareFetchMetricsManager::new(
                Arc::clone(&metrics),
                &share_fetch_metrics_registry,
            ));
            let config = ConsumerConfig {
                bootstrap_servers: vec!["localhost:9092".to_string()],
                request_timeout_ms: REQUEST_TIMEOUT_MS,
                retry_backoff_ms: RETRY_BACKOFF_MS,
                ..ConsumerConfig::defaults()
            };
            let (background_event_tx, _background_event_rx) = mpsc::unbounded_channel();
            let network_client_delegate = NetworkClientDelegate::new(
                &config,
                client,
                metadata.metadata_arc(),
                Arc::new(BackgroundEventHandler::new(background_event_tx)),
                false,
            );

            // buildRequestManager
            let deserializers = Arc::new(Deserializers::new(
                Box::new(ByteArrayDeserializer),
                Box::new(ByteArrayDeserializer),
            ));
            let share_fetch_config = ShareFetchConfig::new(
                1,
                i32::MAX,
                0,
                1000,
                i32::MAX,
                true,
                "",
                crate::common::IsolationLevel::ReadUncommitted,
                share_acquire_mode,
            );
            let share_fetch_collector = ShareFetchCollector::new(
                Arc::clone(&metadata),
                Arc::clone(&subscriptions),
                share_fetch_config.clone(),
                Arc::clone(&deserializers),
            );
            let acknowledgement_event_handler = Arc::new(ShareAcknowledgementEventHandler::new());
            let mut manager = ShareConsumeRequestManager::new(
                Arc::clone(&time) as Arc<dyn Time>,
                GROUP_ID,
                Arc::clone(&metadata),
                Arc::clone(&subscriptions),
                share_fetch_config,
                Arc::new(ShareFetchBuffer::new()),
                Arc::clone(&acknowledgement_event_handler),
                metrics_manager,
                RETRY_BACKOFF_MS,
                1000,
            );
            if let Some(member_id) = member_id {
                manager.on_member_epoch_updated(None, member_id);
            }

            Self {
                topic_id,
                tip0: TopicIdPartition::new(topic_id, tp0.clone()),
                tp0,
                tip1: TopicIdPartition::new(topic_id, tp1.clone()),
                tp1,
                t2ip0: TopicIdPartition::new(topic_id2, t2p0.clone()),
                t2p0,
                topic_ids,
                time,
                subscriptions,
                metadata,
                metrics,
                share_fetch_metrics_registry,
                network_client_delegate,
                manager,
                deserializers,
                share_fetch_collector,
                acknowledgement_event_handler,
                completed_acknowledgements: Vec::new(),
                renewed_records: HashSet::new(),
                // setup()
                records: build_records(1, 3, 1),
                acquired_records: acquired_records(1, 3),
                empty_acquired_records: Vec::new(),
            }
        }

        fn now(&self) -> i64 {
            self.time.milliseconds()
        }

        /// `calculateDeadlineMs(time.timer(timeoutMs))` and
        /// `calculateDeadlineMs(time, timeoutMs)`.
        fn deadline(&self, timeout_ms: i64) -> i64 {
            self.now().saturating_add(timeout_ms)
        }

        /// `client.updateMetadata(response)`.
        fn update_metadata(&self, response: &MetadataResponse) {
            let now = self.now();
            self.metadata.update_with_current_request_version(response, false, now);
        }

        /// `RequestTestUtils.metadataUpdateWithIds(numNodes, counts, tp -> epoch, topicIds, false)`.
        fn metadata_update_with_ids(
            num_nodes: i32,
            topic_partition_counts: &[(&str, i32)],
            epoch: i32,
            topic_ids: &HashMap<String, Uuid>,
        ) -> MetadataResponse {
            let counts: HashMap<String, i32> = topic_partition_counts
                .iter()
                .map(|(topic, count)| ((*topic).to_string(), *count))
                .collect();
            RequestTestUtils::metadata_update_with_ids(
                "kafka-cluster",
                num_nodes,
                &HashMap::new(),
                &counts,
                &|_| Some(epoch),
                topic_ids,
            )
        }

        /// `assignFromSubscribed(Set<TopicPartition>)`.
        fn assign_from_subscribed(&self, partitions: &[TopicPartition]) {
            let topics: HashSet<String> = partitions.iter().map(|tp| tp.topic().to_string()).collect();
            {
                let mut subscriptions = lock(&self.subscriptions);
                subscriptions.subscribe_to_share_group(topics).expect("subscribe");
                subscriptions.assign_from_subscribed(partitions).expect("assign");
            }

            // initialUpdateResponse
            let initial_update_response = RequestTestUtils::metadata_update_with_ids(
                "kafka-cluster",
                1,
                &HashMap::new(),
                &HashMap::from([(TOPIC_NAME.to_string(), 2)]),
                &|_| None,
                &self.topic_ids,
            );
            self.update_metadata(&initial_update_response);

            // A dummy metadata update to ensure valid leader epoch.
            let response = Self::metadata_update_with_ids(
                1,
                &[(TOPIC_NAME, 2), (TOPIC_NAME2, 1)],
                VALID_LEADER_EPOCH,
                &self.topic_ids,
            );
            self.metadata.update_with_current_request_version(&response, false, 0);
        }

        fn subscribe_to_share_group(&self, topics: &[&str]) {
            lock(&self.subscriptions)
                .subscribe_to_share_group(topics.iter().map(|topic| (*topic).to_string()).collect())
                .expect("subscribe");
        }

        fn assign(&self, partitions: &[TopicPartition]) {
            lock(&self.subscriptions).assign_from_subscribed(partitions).expect("assign");
        }

        fn node(&self, id: i32) -> Node {
            self.metadata.fetch().node_by_id(id).cloned().expect("node in metadata")
        }

        fn leader_for(&self, tp: &TopicPartition) -> Node {
            self.metadata.fetch().leader_for(tp).cloned().expect("leader in metadata")
        }

        /// `metadata.updatePartitionLeadership(Map.of(tp, new LeaderIdAndEpoch(..)), nodes)`.
        fn move_leadership(&self, tp: &TopicPartition, leader_id: i32, epoch: i32, leader_nodes: &[Node]) {
            self.metadata.update_partition_leadership(
                &HashMap::from([(tp.clone(), crate::metadata::LeaderIdAndEpoch::new(Some(leader_id), Some(epoch)))]),
                leader_nodes,
            );
        }

        /// `TestableShareConsumeRequestManager#sendFetches`.
        fn send_fetches(&mut self) -> usize {
            self.send_fetches_return_poll_result().len()
        }

        /// `TestableShareConsumeRequestManager#sendFetchesReturnPollResult`.
        fn send_fetches_return_poll_result(&mut self) -> Vec<SentRequest> {
            self.manager.fetch(HashMap::new());
            self.send_acknowledgements_return_poll_result()
        }

        /// `TestableShareConsumeRequestManager#sendAcknowledgements`.
        fn send_acknowledgements(&mut self) -> usize {
            self.send_acknowledgements_return_poll_result().len()
        }

        fn send_acknowledgements_return_poll_result(&mut self) -> Vec<SentRequest> {
            let now = self.now();
            let poll_result = self.manager.poll(now);
            self.add_all(poll_result)
        }

        /// `networkClientDelegate.addAll(pollResult.unsentRequests)`.
        fn add_all(&mut self, poll_result: PollResult) -> Vec<SentRequest> {
            let mut unsent_requests = poll_result.unsent_requests;
            let sent = unsent_requests.iter_mut().map(SentRequest::of).collect();
            let now = self.now();
            self.network_client_delegate.add_all(unsent_requests, now);
            sent
        }

        /// `networkClientDelegate.poll(time.timer(0))`, and the response
        /// callbacks Java runs inside it.
        async fn network_poll(&mut self) {
            let now = self.now();
            self.network_client_delegate.poll(0, now, false).await;
            self.manager.handle_completed_requests();
        }

        fn client(&mut self) -> &mut MockClient {
            self.network_client_delegate.client_for_test_mut()
        }

        fn prepare_fetch_response(&mut self, response: ShareFetchResponse) {
            self.client().prepare_response(ConcreteResponse::ShareFetch(response));
        }

        fn prepare_fetch_response_from(&mut self, response: ShareFetchResponse, node: &Node) {
            self.client()
                .prepare_response_from(ConcreteResponse::ShareFetch(response), node);
        }

        fn prepare_acknowledge_response(&mut self, response: ShareAcknowledgeResponse) {
            self.client().prepare_response(ConcreteResponse::ShareAcknowledge(response));
        }

        fn prepare_acknowledge_response_from(&mut self, response: ShareAcknowledgeResponse, node: &Node) {
            self.client()
                .prepare_response_from(ConcreteResponse::ShareAcknowledge(response), node);
        }

        /// `ShareConsumeRequestManagerTest#sendFetchAndVerifyResponse`.
        async fn send_fetch_and_verify_response(
            &mut self,
            records: Bytes,
            acquired_records: Vec<AcquiredRecords>,
            error: Errors,
            acknowledge_error: Errors,
        ) {
            // normal fetch
            assert_eq!(1, self.send_fetches());
            assert!(!self.manager.has_completed_fetches());

            let tip0 = self.tip0.clone();
            self.prepare_fetch_response(full_fetch_response(
                &tip0,
                Some(records),
                acquired_records,
                error,
                acknowledge_error,
            ));
            self.network_poll().await;
            assert!(self.manager.has_completed_fetches());
        }

        /// `shareConsumeRequestManager.collectFetch()`.
        async fn collect_fetch(
            &self,
        ) -> crate::consumer::internals::share_fetch_collector::ShareFetchCollectResult<Vec<u8>, Vec<u8>> {
            self.share_fetch_collector.collect(&self.manager.share_fetch_buffer).await
        }

        /// `collectFetch()` where the test expects no error.
        async fn collect_fetch_ok(&self) -> Fetch {
            match self.collect_fetch().await {
                Ok(Ok(fetch)) => fetch,
                Ok(Err(share_fetch_error)) => panic!("unexpected collect error {:?}", share_fetch_error.cause()),
                Err(error) => panic!("unexpected collect error {error:?}"),
            }
        }

        /// `ShareConsumeRequestManagerTest#fetchRecords`.
        async fn fetch_records(&self) -> HashMap<TopicPartition, Vec<Record>> {
            let mut fetch = self.collect_fetch_ok().await;
            if fetch.is_empty() {
                return HashMap::new();
            }
            fetch.records()
        }

        /// `ShareConsumeRequestManagerTest#assertEmptyFetch`.
        async fn assert_empty_fetch(&self, reason: &str) {
            let mut fetch = self.collect_fetch_ok().await;
            assert!(fetch.records().is_empty(), "{reason}");
            assert!(fetch.is_empty(), "{reason}");
        }

        /// The events Java's `TestableShareAcknowledgementEventHandler#add`
        /// records, drained from the real handler.
        fn completed_acknowledgements(&mut self) -> &Vec<HashMap<TopicIdPartition, Acknowledgements>> {
            for event in self.acknowledgement_event_handler.drain_events() {
                if event.check_for_renew_acknowledgements() {
                    for acks in event.acknowledgements_map().values() {
                        self.renewed_records.extend(acks.acknowledgements_type_map().keys().copied());
                    }
                }
                self.completed_acknowledgements.push(event.acknowledgements_map().clone());
            }
            &self.completed_acknowledgements
        }

        fn clear_completed_acknowledgements(&mut self) {
            self.completed_acknowledgements();
            self.completed_acknowledgements.clear();
        }

        fn renewed_records(&mut self) -> &HashSet<i64> {
            self.completed_acknowledgements();
            &self.renewed_records
        }

        /// `metrics.metrics().get(metrics.metricInstance(template)).metricValue()`.
        fn metric_value(&self, template: &MetricNameTemplate) -> f64 {
            let name = self
                .metrics
                .metric_instance_with_key_value(template, &[])
                .expect("metric instance");
            self.metrics
                .metric(&name)
                .expect("metric registered")
                .metric_value()
                .as_double()
                .expect("a numeric metric")
        }

        fn request_states(&self, node_id: i32) -> Option<&Tuple<SharedAcknowledgeRequestState>> {
            self.manager.request_states(node_id)
        }

        /// `requestStates(node).getAsyncRequest()`.
        fn async_request(&self, node_id: i32) -> MutexGuard<'_, AcknowledgeRequestState> {
            lock(
                self.request_states(node_id)
                    .and_then(Tuple::async_request)
                    .expect("an async request state"),
            )
        }

        /// `requestStates(node).getSyncRequestQueue().peek()`.
        fn first_sync_request(&self, node_id: i32) -> MutexGuard<'_, AcknowledgeRequestState> {
            lock(
                self.request_states(node_id)
                    .and_then(Tuple::sync_request_queue)
                    .and_then(VecDeque::front)
                    .expect("a sync request state"),
            )
        }

        fn sync_request_queue_size(&self, node_id: i32) -> usize {
            self.request_states(node_id)
                .and_then(Tuple::sync_request_queue)
                .map_or(0, VecDeque::len)
        }
    }

    impl Drop for Fixture {
        /// Java's `@AfterEach teardown`.
        fn drop(&mut self) {
            self.manager.close();
        }
    }

    /// `ShareConsumeRequestManagerTest#getAcknowledgements`.
    fn get_acknowledgements(start_index: i64, acknowledge_types: &[AcknowledgeType]) -> Acknowledgements {
        let mut acknowledgements = Acknowledgements::empty();
        for (index, r#type) in (start_index..).zip(acknowledge_types) {
            acknowledgements.add(index, Some(*r#type));
        }
        acknowledgements
    }

    fn single_acknowledgement(offset: i64, r#type: AcknowledgeType) -> Acknowledgements {
        get_acknowledgements(offset, &[r#type])
    }

    fn node_acks(
        tip: &TopicIdPartition,
        node_id: i32,
        acknowledgements: &Acknowledgements,
    ) -> HashMap<TopicIdPartition, NodeAcknowledgements> {
        HashMap::from([(tip.clone(), NodeAcknowledgements::new(node_id, acknowledgements.clone()))])
    }

    /// Java's `assertEquals(Map.of(tip, acks), completed)` (identity there).
    fn assert_same_acknowledgements(
        expected: &[(&TopicIdPartition, &Acknowledgements)],
        actual: &HashMap<TopicIdPartition, Acknowledgements>,
    ) {
        assert_eq!(expected.len(), actual.len(), "{actual:?}");
        for (tip, acks) in expected {
            assert_eq!(
                acks.acknowledgements_type_map(),
                actual.get(*tip).expect("partition completed").acknowledgements_type_map()
            );
        }
    }

    /// `assertEquals(Errors.X.exception(), acks.getAcknowledgeException())`.
    fn assert_acknowledge_error(acks: &Acknowledgements, expected: Errors) {
        let error = acks.acknowledge_error().expect("completed with an error");
        assert_eq!(expected, error.error(), "{error:?}");
        assert_eq!(expected.error().expect("an error code").message(), error.message());
    }

    /// `ShareConsumeRequestManagerTest#buildRecords`.
    fn build_records(base_offset: i64, count: i32, first_message_id: i64) -> Bytes {
        let mut builder = MemoryRecords::builder_with_initial_capacity(
            1024,
            Compression::none().build(),
            TimestampType::CreateTime,
            base_offset,
        );
        for i in 0..i64::from(count) {
            builder.append_kv(0, Some(b"key"), Some(format!("value-{}", first_message_id + i).as_bytes()));
        }
        builder.build().into_buffer()
    }

    /// `MemoryRecords.EMPTY`.
    fn empty_records() -> Option<Bytes> {
        Some(Bytes::new())
    }

    /// `ShareConsumeRequestManagerTest#partitionDataForFetch`.
    fn partition_data_for_fetch(
        tip: &TopicIdPartition,
        records: Option<Bytes>,
        acquired_records: Vec<AcquiredRecords>,
        error: Errors,
        acknowledge_error: Errors,
    ) -> FetchPartitionData {
        let mut partition_data = FetchPartitionData::new();
        partition_data
            .set_partition_index(tip.partition())
            .set_error_code(error.code())
            .set_acknowledge_error_code(acknowledge_error.code())
            .set_records(records)
            .set_acquired_records(acquired_records);
        partition_data
    }

    /// `new ShareFetchResponseData.PartitionData().setPartitionIndex(..).setErrorCode(..)`.
    fn fetch_partition_with_error(tip: &TopicIdPartition, error: Errors) -> FetchPartitionData {
        let mut partition_data = FetchPartitionData::new();
        partition_data.set_partition_index(tip.partition()).set_error_code(error.code());
        partition_data
    }

    /// `ShareFetchResponse.of(Errors.NONE, 0, partitions, nodeEndpoints, 0)`.
    fn fetch_response(
        partitions: Vec<(TopicIdPartition, FetchPartitionData)>,
        node_endpoints: &[Node],
    ) -> ShareFetchResponse {
        ShareFetchResponse::of(Errors::None, 0, partitions, node_endpoints, 0)
    }

    /// `ShareFetchResponse.of(Errors.NONE, 0, new LinkedHashMap<>(), List.of(), 0)`.
    fn empty_fetch_response() -> ShareFetchResponse {
        fetch_response(Vec::new(), &[])
    }

    /// `ShareConsumeRequestManagerTest#fetchResponseWithTopLevelError`.
    fn fetch_response_with_top_level_error(tip: &TopicIdPartition, error: Errors) -> ShareFetchResponse {
        ShareFetchResponse::of(error, 0, [(tip.clone(), fetch_partition_with_error(tip, error))], &[], 0)
    }

    /// `ShareConsumeRequestManagerTest#fullFetchResponse`.
    fn full_fetch_response(
        tip: &TopicIdPartition,
        records: Option<Bytes>,
        acquired_records: Vec<AcquiredRecords>,
        error: Errors,
        acknowledge_error: Errors,
    ) -> ShareFetchResponse {
        fetch_response(
            vec![(
                tip.clone(),
                partition_data_for_fetch(tip, records, acquired_records, error, acknowledge_error),
            )],
            &[],
        )
    }

    /// `ShareConsumeRequestManagerTest#partitionDataForAcknowledge`.
    fn partition_data_for_acknowledge(
        tip: &TopicIdPartition,
        error: Errors,
        current_leader: Option<AckLeaderIdAndEpoch>,
    ) -> AckPartitionData {
        let mut partition_data = AckPartitionData::new();
        partition_data.set_partition_index(tip.partition()).set_error_code(error.code());
        if let Some(current_leader) = current_leader {
            partition_data.set_current_leader(current_leader);
        }
        partition_data
    }

    /// `ShareConsumeRequestManagerTest#emptyAcknowledgeResponse`.
    fn empty_acknowledge_response() -> ShareAcknowledgeResponse {
        ShareAcknowledgeResponse::of(Errors::None, 0, Vec::new(), &[], 0)
    }

    /// `ShareConsumeRequestManagerTest#acknowledgeResponseWithTopLevelError`.
    fn acknowledge_response_with_top_level_error(tip: &TopicIdPartition, error: Errors) -> ShareAcknowledgeResponse {
        ShareAcknowledgeResponse::of(
            error,
            0,
            [(tip.clone(), partition_data_for_acknowledge(tip, Errors::None, None))],
            &[],
            0,
        )
    }

    /// `ShareConsumeRequestManagerTest#fullAcknowledgeResponse(TopicIdPartition, Errors)`.
    fn full_acknowledge_response(tip: &TopicIdPartition, error: Errors) -> ShareAcknowledgeResponse {
        full_acknowledge_response_with_errors(&[(tip, error)])
    }

    /// `ShareConsumeRequestManagerTest#fullAcknowledgeResponse(Map<TopicIdPartition, Errors>)`.
    fn full_acknowledge_response_with_errors(errors: &[(&TopicIdPartition, Errors)]) -> ShareAcknowledgeResponse {
        ShareAcknowledgeResponse::of(
            Errors::None,
            0,
            errors
                .iter()
                .map(|(tip, error)| ((*tip).clone(), partition_data_for_acknowledge(tip, *error, None))),
            &[],
            0,
        )
    }

    /// `ShareConsumeRequestManagerTest#fullAcknowledgeResponse(TopicIdPartition, Errors, LeaderIdAndEpoch, List<Node>)`.
    fn full_acknowledge_response_with_leader(
        tip: &TopicIdPartition,
        error: Errors,
        leader_id: i32,
        leader_epoch: i32,
        node_endpoints: &[Node],
    ) -> ShareAcknowledgeResponse {
        let mut current_leader = AckLeaderIdAndEpoch::new();
        current_leader.set_leader_id(leader_id).set_leader_epoch(leader_epoch);
        ShareAcknowledgeResponse::of(
            Errors::None,
            0,
            [(tip.clone(), partition_data_for_acknowledge(tip, error, Some(current_leader)))],
            node_endpoints,
            0,
        )
    }

    /// The partition `index` of `topic_id` in a ShareFetch request's data.
    fn fetch_partition(
        data: &ShareFetchRequestData,
        topic_id: Uuid,
        index: i32,
    ) -> &crate::share_fetch_request_data::FetchPartition {
        data.topics
            .iter()
            .find(|topic| topic.topic_id == topic_id)
            .and_then(|topic| topic.partitions.iter().find(|partition| partition.partition_index == index))
            .expect("partition in the request")
    }

    #[tokio::test(flavor = "current_thread")]
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testFetchNormal")]
    async fn test_fetch_normal() {
        let mut f = Fixture::new();

        f.assign_from_subscribed(&[f.tp0.clone()]);
        f.send_fetch_and_verify_response(f.records.clone(), f.acquired_records.clone(), Errors::None, Errors::None)
            .await;

        let partition_records = f.fetch_records().await;
        assert!(partition_records.contains_key(&f.tp0));

        let records = &partition_records[&f.tp0];
        assert_eq!(3, records.len());
    }

    #[tokio::test(flavor = "current_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testFetchWithAcquiredRecords"
    )]
    async fn test_fetch_with_acquired_records() {
        let mut f = Fixture::new();

        f.assign_from_subscribed(&[f.tp0.clone()]);
        f.send_fetch_and_verify_response(f.records.clone(), acquired_records(1, 1), Errors::None, Errors::None)
            .await;

        let partition_records = f.fetch_records().await;
        assert!(partition_records.contains_key(&f.tp0));

        // As only 1 record was acquired, we must fetch only 1 record.
        let fetched_records = &partition_records[&f.tp0];
        assert_eq!(1, fetched_records.len());
    }

    #[tokio::test(flavor = "current_thread")]
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testMultipleFetches")]
    async fn test_multiple_fetches() {
        let mut f = Fixture::new();
        // Enabling the config so that background event is sent when the acknowledgement response is received.
        f.manager.set_acknowledgement_commit_callback_registered(true);
        f.assign_from_subscribed(&[f.tp0.clone()]);

        f.send_fetch_and_verify_response(f.records.clone(), acquired_records(1, 1), Errors::None, Errors::None)
            .await;

        let partition_records = f.fetch_records().await;
        assert!(partition_records.contains_key(&f.tp0));

        // As only 1 record was acquired, we must fetch only 1 record.
        let fetched_records = &partition_records[&f.tp0];
        assert_eq!(1, fetched_records.len());

        let acknowledgements = single_acknowledgement(1, AcknowledgeType::Accept);
        f.manager.fetch(node_acks(&f.tip0, 0, &acknowledgements));

        f.send_fetch_and_verify_response(f.records.clone(), acquired_records(2, 1), Errors::None, Errors::None)
            .await;
        assert_eq!(1.0, f.metric_value(&f.share_fetch_metrics_registry.acknowledgement_send_total));

        let partition_records = f.fetch_records().await;
        assert!(partition_records.contains_key(&f.tp0));
        let tip0 = f.tip0.clone();
        assert_same_acknowledgements(&[(&tip0, &acknowledgements)], &f.completed_acknowledgements()[0]);
        f.clear_completed_acknowledgements();

        let acknowledgements2 = single_acknowledgement(2, AcknowledgeType::Reject);
        f.manager.fetch(node_acks(&f.tip0, 0, &acknowledgements2));

        // Preparing a response with an acknowledgement error.
        f.send_fetch_and_verify_response(f.records.clone(), Vec::new(), Errors::None, Errors::InvalidRecordState)
            .await;

        assert_eq!(2.0, f.metric_value(&f.share_fetch_metrics_registry.acknowledgement_send_total));
        assert_eq!(1.0, f.metric_value(&f.share_fetch_metrics_registry.acknowledgement_error_total));

        let partition_records = f.fetch_records().await;
        assert!(partition_records.is_empty());
        assert_same_acknowledgements(&[(&tip0, &acknowledgements2)], &f.completed_acknowledgements()[0]);
    }

    #[tokio::test(flavor = "current_thread")]
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testCommitSync")]
    async fn test_commit_sync() {
        let mut f = Fixture::new();
        // Enabling the config so that background event is sent when the acknowledgement response is received.
        f.manager.set_acknowledgement_commit_callback_registered(true);

        f.assign_from_subscribed(&[f.tp0.clone()]);
        f.send_fetch_and_verify_response(f.records.clone(), f.acquired_records.clone(), Errors::None, Errors::None)
            .await;

        let acknowledgements = get_acknowledgements(
            1,
            &[
                AcknowledgeType::Accept,
                AcknowledgeType::Accept,
                AcknowledgeType::Reject,
            ],
        );

        let deadline = f.deadline(2000);
        f.manager.commit_sync(node_acks(&f.tip0, 0, &acknowledgements), deadline);

        assert_eq!(1, f.send_acknowledgements());

        let tip0 = f.tip0.clone();
        f.prepare_acknowledge_response(full_acknowledge_response(&tip0, Errors::None));
        f.network_poll().await;
        assert!(f.manager.has_completed_fetches());

        assert_same_acknowledgements(&[(&tip0, &acknowledgements)], &f.completed_acknowledgements()[0]);
    }

    #[tokio::test(flavor = "current_thread")]
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testCommitAsync")]
    async fn test_commit_async() {
        let mut f = Fixture::new();
        // Enabling the config so that background event is sent when the acknowledgement response is received.
        f.manager.set_acknowledgement_commit_callback_registered(true);

        f.assign_from_subscribed(&[f.tp0.clone()]);
        f.send_fetch_and_verify_response(f.records.clone(), f.acquired_records.clone(), Errors::None, Errors::None)
            .await;

        let acknowledgements = get_acknowledgements(
            1,
            &[
                AcknowledgeType::Accept,
                AcknowledgeType::Accept,
                AcknowledgeType::Reject,
            ],
        );

        let deadline = f.deadline(DEFAULT_API_TIMEOUT_MS);
        f.manager.commit_async(node_acks(&f.tip0, 0, &acknowledgements), deadline);

        assert_eq!(1, f.send_acknowledgements());

        let tip0 = f.tip0.clone();
        f.prepare_acknowledge_response(full_acknowledge_response(&tip0, Errors::None));
        f.network_poll().await;
        assert!(f.manager.has_completed_fetches());

        assert_same_acknowledgements(&[(&tip0, &acknowledgements)], &f.completed_acknowledgements()[0]);
    }

    #[tokio::test(flavor = "current_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testServerDisconnectedOnShareAcknowledge"
    )]
    async fn test_server_disconnected_on_share_acknowledge() {
        let mut f = Fixture::new();
        // Enabling the config so that background event is sent when the acknowledgement response is received.
        f.manager.set_acknowledgement_commit_callback_registered(true);

        f.assign_from_subscribed(&[f.tp0.clone()]);
        f.send_fetch_and_verify_response(f.records.clone(), f.acquired_records.clone(), Errors::None, Errors::None)
            .await;

        f.fetch_records().await;

        let acknowledgements = get_acknowledgements(1, &[AcknowledgeType::Accept, AcknowledgeType::Accept]);

        let deadline = f.deadline(DEFAULT_API_TIMEOUT_MS);
        f.manager.commit_async(node_acks(&f.tip0, 0, &acknowledgements), deadline);

        assert_eq!(1, f.send_acknowledgements());

        let acknowledgements2 = single_acknowledgement(3, AcknowledgeType::Reject);

        let deadline = f.deadline(DEFAULT_API_TIMEOUT_MS);
        f.manager.commit_async(node_acks(&f.tip0, 0, &acknowledgements2), deadline);

        f.client()
            .prepare_response_disconnected(ConcreteResponse::ShareAcknowledge(empty_acknowledge_response()), true);
        f.network_poll().await;

        let tip0 = f.tip0.clone();
        assert_same_acknowledgements(&[(&tip0, &acknowledgements)], &f.completed_acknowledgements()[0]);
        assert_acknowledge_error(&f.completed_acknowledgements()[0][&tip0], Errors::UnknownServerError);
        f.clear_completed_acknowledgements();

        assert_eq!(1, f.async_request(0).acknowledgements_to_send_count(&tip0));

        // Wait for backoff time before sending the next request.
        f.time.sleep(RETRY_BACKOFF_MS);

        assert_eq!(0, f.send_acknowledgements());
        // We expect the remaining acknowledgements to be cleared due to share session epoch being set to 0.
        assert!(f.request_states(0).is_none());
        // The callback for these unsent acknowledgements will be invoked with an error code.
        assert_same_acknowledgements(&[(&tip0, &acknowledgements2)], &f.completed_acknowledgements()[0]);
        assert_acknowledge_error(&f.completed_acknowledgements()[0][&tip0], Errors::NetworkError);

        // Attempt a normal fetch to check if nodesWithPendingRequests is empty.
        assert_eq!(1, f.send_fetches());
        assert!(!f.manager.has_completed_fetches());

        f.prepare_fetch_response(full_fetch_response(
            &tip0,
            Some(f.records.clone()),
            f.acquired_records.clone(),
            Errors::None,
            Errors::None,
        ));
        f.network_poll().await;
        assert!(f.manager.has_completed_fetches());
    }

    #[tokio::test(flavor = "current_thread")]
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testAcknowledgeOnClose")]
    async fn test_acknowledge_on_close() {
        let mut f = Fixture::new();
        // Enabling the config so that background event is sent when the acknowledgement response is received.
        f.manager.set_acknowledgement_commit_callback_registered(true);

        f.assign_from_subscribed(&[f.tp0.clone()]);
        f.send_fetch_and_verify_response(f.records.clone(), f.acquired_records.clone(), Errors::None, Errors::None)
            .await;

        let acknowledgements = single_acknowledgement(1, AcknowledgeType::Accept);

        // Piggyback acknowledgements
        f.manager.fetch(node_acks(&f.tip0, 0, &acknowledgements));

        // Remaining acknowledgements sent with close().
        let acknowledgements2 = get_acknowledgements(2, &[AcknowledgeType::Accept, AcknowledgeType::Reject]);

        let deadline = f.deadline(100);
        let close_future = f
            .manager
            .acknowledge_on_close(node_acks(&f.tip0, 0, &acknowledgements2), deadline);

        assert_eq!(1, f.send_acknowledgements());

        let tip0 = f.tip0.clone();
        f.prepare_acknowledge_response(full_acknowledge_response(&tip0, Errors::None));
        f.network_poll().await;
        assert_eq!(1, f.completed_acknowledgements().len());

        let mut merged_acks = acknowledgements.clone();
        merged_acks.merge(&acknowledgements2);
        merged_acks.complete(None);
        // Verifying that all 3 offsets were acknowledged as part of the final ShareAcknowledge on close.
        assert_eq!(
            merged_acks.acknowledgements_type_map(),
            f.completed_acknowledgements()[0][&tip0].acknowledgements_type_map()
        );
        assert!(f.manager.has_completed_fetches());

        // Polling once more to complete the closeFuture.
        f.send_fetches();
        assert!(close_future.is_done());
    }

    #[tokio::test(flavor = "current_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testCloseFutureCompletedWhenMemberIdIsNull"
    )]
    async fn test_close_future_completed_when_member_id_is_null() {
        let mut f = Fixture::with_member_id(None, ShareAcquireMode::BatchOptimized);
        f.assign_from_subscribed(&[f.tp0.clone()]);

        let deadline = f.deadline(100);
        let close_future = f.manager.acknowledge_on_close(HashMap::new(), deadline);

        assert!(!close_future.is_done());

        // The subsequent poll should complete the closeFuture as the memberId is null.
        f.send_fetches();
        assert!(close_future.is_done());
    }

    #[tokio::test(flavor = "current_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testAcknowledgeOnCloseWithPendingCommitAsync"
    )]
    async fn test_acknowledge_on_close_with_pending_commit_async() {
        let mut f = Fixture::new();
        // Enabling the config so that background event is sent when the acknowledgement response is received.
        f.manager.set_acknowledgement_commit_callback_registered(true);

        f.assign_from_subscribed(&[f.tp0.clone()]);
        f.send_fetch_and_verify_response(f.records.clone(), f.acquired_records.clone(), Errors::None, Errors::None)
            .await;

        let acknowledgements = get_acknowledgements(
            1,
            &[
                AcknowledgeType::Accept,
                AcknowledgeType::Accept,
                AcknowledgeType::Reject,
            ],
        );

        let deadline = f.deadline(DEFAULT_API_TIMEOUT_MS);
        f.manager.commit_async(node_acks(&f.tip0, 0, &acknowledgements), deadline);
        let deadline = f.deadline(100);
        f.manager.acknowledge_on_close(HashMap::new(), deadline);

        assert_eq!(1, f.send_acknowledgements());

        let tip0 = f.tip0.clone();
        f.prepare_acknowledge_response(full_acknowledge_response(&tip0, Errors::None));
        f.network_poll().await;

        f.prepare_acknowledge_response(empty_acknowledge_response());
        f.network_poll().await;
        assert!(f.manager.has_completed_fetches());

        assert_same_acknowledgements(&[(&tip0, &acknowledgements)], &f.completed_acknowledgements()[0]);
    }

    #[tokio::test(flavor = "current_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testAcknowledgeOnCloseWithPendingCommitSync"
    )]
    async fn test_acknowledge_on_close_with_pending_commit_sync() {
        let mut f = Fixture::new();
        // Enabling the config so that background event is sent when the acknowledgement response is received.
        f.manager.set_acknowledgement_commit_callback_registered(true);

        f.assign_from_subscribed(&[f.tp0.clone()]);
        f.send_fetch_and_verify_response(f.records.clone(), f.acquired_records.clone(), Errors::None, Errors::None)
            .await;

        let acknowledgements = get_acknowledgements(
            1,
            &[
                AcknowledgeType::Accept,
                AcknowledgeType::Accept,
                AcknowledgeType::Reject,
            ],
        );

        let deadline = f.deadline(100);
        f.manager.commit_sync(node_acks(&f.tip0, 0, &acknowledgements), deadline);
        let deadline = f.deadline(100);
        f.manager.acknowledge_on_close(HashMap::new(), deadline);

        assert_eq!(1, f.send_acknowledgements());

        let tip0 = f.tip0.clone();
        f.prepare_acknowledge_response(full_acknowledge_response(&tip0, Errors::None));
        f.network_poll().await;

        f.prepare_acknowledge_response(empty_acknowledge_response());
        f.network_poll().await;
        assert!(f.manager.has_completed_fetches());

        assert_same_acknowledgements(&[(&tip0, &acknowledgements)], &f.completed_acknowledgements()[0]);
    }

    #[tokio::test(flavor = "current_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testResultHandlerOnCommitAsync"
    )]
    async fn test_result_handler_on_commit_async() {
        let mut f = Fixture::new();
        // Enabling the config so that background event is sent when the acknowledgement response is received.
        f.manager.set_acknowledgement_commit_callback_registered(true);

        let acknowledgements = get_acknowledgements(
            1,
            &[
                AcknowledgeType::Accept,
                AcknowledgeType::Accept,
                AcknowledgeType::Reject,
            ],
        );

        let result_handler = ResultHandler::with_remaining_results_future(None, None);

        // Passing null acknowledgements should mean we do not send the background event at all.
        result_handler.complete(
            &f.manager,
            f.tip0.clone(),
            None,
            AcknowledgeRequestType::CommitAsync,
            false,
            None,
        );
        assert_eq!(0, f.completed_acknowledgements().len());

        // Setting the request type to COMMIT_SYNC should still not send any background event
        // as we have initialized remainingResults to null.
        result_handler.complete(
            &f.manager,
            f.tip0.clone(),
            Some(acknowledgements.clone()),
            AcknowledgeRequestType::CommitSync,
            false,
            None,
        );
        assert_eq!(0, f.completed_acknowledgements().len());

        // Sending non-null acknowledgements means we do send the background event
        result_handler.complete(
            &f.manager,
            f.tip0.clone(),
            Some(acknowledgements),
            AcknowledgeRequestType::CommitAsync,
            false,
            None,
        );
        let tip0 = f.tip0.clone();
        assert_eq!(3, f.completed_acknowledgements()[0][&tip0].size());
    }

    #[tokio::test(flavor = "current_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testResultHandlerOnCommitSync"
    )]
    async fn test_result_handler_on_commit_sync() {
        let mut f = Fixture::new();
        // Enabling the config so that background event is sent when the acknowledgement response is received.
        f.manager.set_acknowledgement_commit_callback_registered(true);

        let acknowledgements = get_acknowledgements(
            1,
            &[
                AcknowledgeType::Accept,
                AcknowledgeType::Accept,
                AcknowledgeType::Reject,
            ],
        );

        let future = KafkaFutureImpl::new();

        // Initializing resultCount to 3.
        let result_count = Arc::new(AtomicI32::new(3));

        let result_handler = ResultHandler::with_remaining_results_future(Some(result_count), Some(future.clone()));

        // We only send the background event after all results have been completed.
        result_handler.complete(
            &f.manager,
            f.tip0.clone(),
            Some(acknowledgements.clone()),
            AcknowledgeRequestType::CommitSync,
            false,
            None,
        );
        assert_eq!(0, f.completed_acknowledgements().len());
        assert!(!future.is_done());

        result_handler.complete(
            &f.manager,
            f.t2ip0.clone(),
            None,
            AcknowledgeRequestType::CommitSync,
            false,
            None,
        );
        assert_eq!(0, f.completed_acknowledgements().len());
        assert!(!future.is_done());

        // After third response is received, we send the background event.
        result_handler.complete(
            &f.manager,
            f.tip1.clone(),
            Some(acknowledgements),
            AcknowledgeRequestType::CommitSync,
            false,
            None,
        );
        let (tip0, tip1) = (f.tip0.clone(), f.tip1.clone());
        assert_eq!(1, f.completed_acknowledgements().len());
        assert_eq!(2, f.completed_acknowledgements()[0].len());
        assert_eq!(3, f.completed_acknowledgements()[0][&tip0].size());
        assert_eq!(3, f.completed_acknowledgements()[0][&tip1].size());
        assert!(future.is_done());
    }

    #[tokio::test(flavor = "current_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testResultHandlerCompleteIfEmpty"
    )]
    async fn test_result_handler_complete_if_empty() {
        let _f = Fixture::new();

        let future: KafkaFutureImpl<CommitSyncResult> = KafkaFutureImpl::new();

        // Initializing resultCount to 1.
        let result_count = Arc::new(AtomicI32::new(1));

        let result_handler =
            ResultHandler::with_remaining_results_future(Some(Arc::clone(&result_count)), Some(future.clone()));

        result_handler.complete_if_empty();
        assert!(!future.is_done());

        result_count.fetch_sub(1, Ordering::AcqRel);

        result_handler.complete_if_empty();
        assert!(future.is_done());
    }

    #[tokio::test(flavor = "current_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testBatchingAcknowledgeRequestStates"
    )]
    async fn test_batching_acknowledge_request_states() {
        let mut f = Fixture::new();

        f.assign_from_subscribed(&[f.tp0.clone()]);
        f.send_fetch_and_verify_response(build_records(1, 6, 1), acquired_records(1, 6), Errors::None, Errors::None)
            .await;

        let acknowledgements = get_acknowledgements(
            1,
            &[
                AcknowledgeType::Accept,
                AcknowledgeType::Accept,
                AcknowledgeType::Reject,
            ],
        );

        let deadline = f.deadline(DEFAULT_API_TIMEOUT_MS);
        f.manager.commit_async(node_acks(&f.tip0, 0, &acknowledgements), deadline);

        let acknowledgements2 = get_acknowledgements(
            4,
            &[
                AcknowledgeType::Accept,
                AcknowledgeType::Accept,
                AcknowledgeType::Reject,
            ],
        );

        let deadline = f.deadline(DEFAULT_API_TIMEOUT_MS);
        f.manager.commit_async(node_acks(&f.tip0, 0, &acknowledgements2), deadline);

        let tip0 = f.tip0.clone();
        assert_eq!(6, f.async_request(0).acknowledgements_to_send_count(&tip0));

        assert_eq!(1, f.send_acknowledgements());

        assert_eq!(0, f.async_request(0).acknowledgements_to_send_count(&tip0));
        assert_eq!(6, f.async_request(0).in_flight_acknowledgements_count(&tip0));

        f.prepare_acknowledge_response(full_acknowledge_response(&tip0, Errors::None));
        f.network_poll().await;

        assert_eq!(0, f.async_request(0).acknowledgements_to_send_count(&tip0));
        assert_eq!(0, f.async_request(0).in_flight_acknowledgements_count(&tip0));
    }

    #[tokio::test(flavor = "current_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testPendingCommitAsyncBeforeCommitSync"
    )]
    async fn test_pending_commit_async_before_commit_sync() {
        let mut f = Fixture::new();

        f.assign_from_subscribed(&[f.tp0.clone()]);
        f.send_fetch_and_verify_response(build_records(1, 6, 1), acquired_records(1, 6), Errors::None, Errors::None)
            .await;

        let acknowledgements = get_acknowledgements(
            1,
            &[
                AcknowledgeType::Accept,
                AcknowledgeType::Accept,
                AcknowledgeType::Reject,
            ],
        );

        let deadline = f.deadline(DEFAULT_API_TIMEOUT_MS);
        f.manager.commit_async(node_acks(&f.tip0, 0, &acknowledgements), deadline);

        let acknowledgements2 = get_acknowledgements(
            4,
            &[
                AcknowledgeType::Accept,
                AcknowledgeType::Accept,
                AcknowledgeType::Reject,
            ],
        );

        let deadline = f.deadline(60000);
        f.manager.commit_sync(node_acks(&f.tip0, 0, &acknowledgements2), deadline);

        let tip0 = f.tip0.clone();
        assert_eq!(3, f.async_request(0).acknowledgements_to_send_count(&tip0));
        assert_eq!(1, f.sync_request_queue_size(0));
        assert_eq!(3, f.first_sync_request(0).acknowledgements_to_send_count(&tip0));

        assert_eq!(1, f.send_acknowledgements());

        assert_eq!(3, f.async_request(0).in_flight_acknowledgements_count(&tip0));

        f.prepare_acknowledge_response(full_acknowledge_response(&tip0, Errors::None));
        f.network_poll().await;

        assert_eq!(0, f.async_request(0).in_flight_acknowledgements_count(&tip0));
        assert_eq!(1, f.sync_request_queue_size(0));
        assert_eq!(3, f.first_sync_request(0).acknowledgements_to_send_count(&tip0));

        assert_eq!(1, f.send_acknowledgements());

        assert_eq!(1, f.sync_request_queue_size(0));
        assert_eq!(3, f.first_sync_request(0).in_flight_acknowledgements_count(&tip0));

        f.prepare_acknowledge_response(full_acknowledge_response(&tip0, Errors::None));
        f.network_poll().await;

        assert_eq!(0, f.first_sync_request(0).in_flight_acknowledgements_count(&tip0));
    }

    #[tokio::test(flavor = "current_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testRetryAcknowledgements"
    )]
    async fn test_retry_acknowledgements() {
        let mut f = Fixture::new();

        f.assign_from_subscribed(&[f.tp0.clone()]);
        f.send_fetch_and_verify_response(build_records(1, 6, 1), acquired_records(1, 6), Errors::None, Errors::None)
            .await;

        let acknowledgements = get_acknowledgements(
            1,
            &[
                AcknowledgeType::Accept,
                AcknowledgeType::Accept,
                AcknowledgeType::Reject,
                AcknowledgeType::Accept,
                AcknowledgeType::Release,
                AcknowledgeType::Accept,
            ],
        );

        // Java passes the literal 60000L as the deadline.
        f.manager.commit_sync(node_acks(&f.tip0, 0, &acknowledgements), 60000);
        assert!(f.request_states(0).and_then(Tuple::async_request).is_none());

        let tip0 = f.tip0.clone();
        assert_eq!(1, f.sync_request_queue_size(0));
        assert_eq!(6, f.first_sync_request(0).acknowledgements_to_send_count(&tip0));

        assert_eq!(1, f.send_acknowledgements());
        assert_eq!(6, f.first_sync_request(0).in_flight_acknowledgements_count(&tip0));

        f.prepare_acknowledge_response(full_acknowledge_response(&tip0, Errors::RequestTimedOut));
        f.network_poll().await;

        assert_eq!(6, f.first_sync_request(0).incomplete_acknowledgements_count(&tip0));
        assert_eq!(0, f.first_sync_request(0).in_flight_acknowledgements_count(&tip0));

        // Wait for backoff time before sending the next request.
        // After the first attempt, it can maximum be 1.2x of the configured backoff when acknowledge fails. (jitter = 0.2)
        f.time.sleep(RETRY_BACKOFF_MS * 3 / 2);
        assert_eq!(1, f.send_acknowledgements());

        assert_eq!(6, f.first_sync_request(0).in_flight_acknowledgements_count(&tip0));

        f.prepare_acknowledge_response(full_acknowledge_response(&tip0, Errors::None));
        f.network_poll().await;

        assert_eq!(0, f.first_sync_request(0).in_flight_acknowledgements_count(&tip0));
        assert_eq!(0, f.first_sync_request(0).incomplete_acknowledgements_count(&tip0));
    }

    /// `@EnumSource(names = {"FENCED_LEADER_EPOCH", "NOT_LEADER_OR_FOLLOWER", "UNKNOWN_TOPIC_OR_PARTITION"})`.
    #[tokio::test(flavor = "current_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testFatalErrorsAcknowledgementResponse"
    )]
    async fn test_fatal_errors_acknowledgement_response() {
        for error in [
            Errors::FencedLeaderEpoch,
            Errors::NotLeaderOrFollower,
            Errors::UnknownTopicOrPartition,
        ] {
            let mut f = Fixture::new();
            f.manager.set_acknowledgement_commit_callback_registered(true);

            f.assign_from_subscribed(&[f.tp0.clone()]);
            f.send_fetch_and_verify_response(f.records.clone(), f.acquired_records.clone(), Errors::None, Errors::None)
                .await;

            let acknowledgements = get_acknowledgements(
                1,
                &[
                    AcknowledgeType::Accept,
                    AcknowledgeType::Accept,
                    AcknowledgeType::Reject,
                ],
            );

            let deadline = f.deadline(DEFAULT_API_TIMEOUT_MS);
            f.manager.commit_async(node_acks(&f.tip0, 0, &acknowledgements), deadline);

            assert_eq!(1, f.send_acknowledgements(), "{error}");
            let tip0 = f.tip0.clone();
            f.prepare_acknowledge_response(full_acknowledge_response(&tip0, error));
            f.network_poll().await;

            // Assert these errors are not retried even if they are retriable. They are treated as fatal and a metadata update is triggered.
            assert_eq!(0, f.async_request(0).in_flight_acknowledgements_count(&tip0), "{error}");
            assert_eq!(0, f.async_request(0).incomplete_acknowledgements_count(&tip0), "{error}");
            assert_eq!(1, f.completed_acknowledgements().len(), "{error}");
            assert_eq!(3, f.completed_acknowledgements()[0][&tip0].size(), "{error}");
        }
    }

    #[tokio::test(flavor = "current_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testRetryAcknowledgementsMultipleCommitAsync"
    )]
    async fn test_retry_acknowledgements_multiple_commit_async() {
        let mut f = Fixture::new();
        f.manager.set_acknowledgement_commit_callback_registered(true);

        f.assign_from_subscribed(&[f.tp0.clone()]);
        f.send_fetch_and_verify_response(build_records(1, 6, 1), acquired_records(1, 6), Errors::None, Errors::None)
            .await;

        let acknowledgements = get_acknowledgements(1, &[AcknowledgeType::Accept, AcknowledgeType::Accept]);

        // commitAsync() acknowledges the first 2 records.
        let deadline = f.deadline(1000);
        f.manager.commit_async(node_acks(&f.tip0, 0, &acknowledgements), deadline);

        let tip0 = f.tip0.clone();
        assert_eq!(2, f.async_request(0).acknowledgements_to_send_count(&tip0));

        assert_eq!(1, f.send_acknowledgements());
        assert_eq!(2, f.async_request(0).in_flight_acknowledgements_count(&tip0));

        // Response contains a retriable exception, so we retry.
        f.prepare_acknowledge_response(full_acknowledge_response(&tip0, Errors::RequestTimedOut));
        f.network_poll().await;

        let acknowledgements1 = get_acknowledgements(3, &[AcknowledgeType::Accept, AcknowledgeType::Reject]);

        // 2nd commitAsync() acknowledges the next 2 records.
        let deadline = f.deadline(1000);
        f.manager.commit_async(node_acks(&f.tip0, 0, &acknowledgements1), deadline);
        assert_eq!(2, f.async_request(0).incomplete_acknowledgements_count(&tip0));

        let acknowledgements2 = get_acknowledgements(5, &[AcknowledgeType::Release, AcknowledgeType::Accept]);

        // 3rd commitAsync() acknowledges the next 2 records.
        let deadline = f.deadline(1000);
        f.manager.commit_async(node_acks(&f.tip0, 0, &acknowledgements2), deadline);

        f.time.sleep(2000);

        // As the timer for the initial commitAsync() was 1000ms, the request times out, and we fill the callback with a timeout exception.
        assert_eq!(0, f.send_acknowledgements());
        assert_eq!(1, f.completed_acknowledgements().len());
        assert_eq!(2, f.completed_acknowledgements()[0][&tip0].size());
        assert_acknowledge_error(&f.completed_acknowledgements()[0][&tip0], Errors::RequestTimedOut);
        f.clear_completed_acknowledgements();

        // Further requests which came before the timeout are processed as expected.
        assert_eq!(1, f.send_acknowledgements());
        assert_eq!(4, f.async_request(0).in_flight_acknowledgements_count(&tip0));

        f.prepare_acknowledge_response(full_acknowledge_response(&tip0, Errors::None));
        f.network_poll().await;

        assert_eq!(0, f.async_request(0).in_flight_acknowledgements_count(&tip0));
        assert_eq!(1, f.completed_acknowledgements().len());
        assert_eq!(4, f.completed_acknowledgements()[0][&tip0].size());
        assert!(f.completed_acknowledgements()[0][&tip0].acknowledge_error().is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testRetryAcknowledgementsMultipleCommitSync"
    )]
    async fn test_retry_acknowledgements_multiple_commit_sync() {
        let mut f = Fixture::new();
        f.manager.set_acknowledgement_commit_callback_registered(true);

        f.assign_from_subscribed(&[f.tp0.clone()]);
        f.send_fetch_and_verify_response(build_records(1, 6, 1), acquired_records(1, 6), Errors::None, Errors::None)
            .await;

        let acknowledgements = get_acknowledgements(1, &[AcknowledgeType::Accept, AcknowledgeType::Accept]);

        // commitSync() for the first 2 acknowledgements.
        let deadline = f.deadline(1000);
        f.manager.commit_sync(node_acks(&f.tip0, 0, &acknowledgements), deadline);

        assert_eq!(1, f.send_acknowledgements());

        // Response contains a retriable exception.
        let tip0 = f.tip0.clone();
        f.prepare_acknowledge_response(full_acknowledge_response(&tip0, Errors::RequestTimedOut));
        f.network_poll().await;
        assert_eq!(2, f.first_sync_request(0).incomplete_acknowledgements_count(&tip0));

        // We expire the commitSync request as it had a timer of 1000ms.
        f.time.sleep(2000);

        let acknowledgements1 = get_acknowledgements(
            3,
            &[
                AcknowledgeType::Accept,
                AcknowledgeType::Reject,
                AcknowledgeType::Release,
                AcknowledgeType::Accept,
            ],
        );

        // commitSync() for the next 4 acknowledgements.
        let deadline = f.deadline(1000);
        f.manager.commit_sync(node_acks(&f.tip0, 0, &acknowledgements1), deadline);

        // We send the 2nd commitSync request, and fail the first one as timer has expired.
        assert_eq!(1, f.send_acknowledgements());
        assert_eq!(2, f.completed_acknowledgements()[0][&tip0].size());
        assert_acknowledge_error(&f.completed_acknowledgements()[0][&tip0], Errors::RequestTimedOut);
        f.clear_completed_acknowledgements();

        // We get a successful response for the 2nd commitSync request.
        f.prepare_acknowledge_response(full_acknowledge_response(&tip0, Errors::None));
        f.network_poll().await;

        assert_eq!(1, f.completed_acknowledgements().len());
        assert_eq!(4, f.completed_acknowledgements()[0][&tip0].size());
        assert!(f.completed_acknowledgements()[0][&tip0].acknowledge_error().is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testPiggybackAcknowledgementsInFlight"
    )]
    async fn test_piggyback_acknowledgements_in_flight() {
        let mut f = Fixture::new();

        f.assign_from_subscribed(&[f.tp0.clone()]);
        f.send_fetch_and_verify_response(f.records.clone(), f.acquired_records.clone(), Errors::None, Errors::None)
            .await;

        let acknowledgements = get_acknowledgements(1, &[AcknowledgeType::Accept, AcknowledgeType::Accept]);

        // Reading records from the share fetch buffer.
        f.fetch_records().await;

        // Piggyback acknowledgements
        f.manager.fetch(node_acks(&f.tip0, 0, &acknowledgements));

        assert_eq!(1, f.send_fetches());
        assert!(!f.manager.has_completed_fetches());

        assert_eq!(2.0, f.metric_value(&f.share_fetch_metrics_registry.acknowledgement_send_total));

        let acknowledgements2 = single_acknowledgement(3, AcknowledgeType::Accept);
        f.manager.fetch(node_acks(&f.tip0, 0, &acknowledgements2));

        let tip0 = f.tip0.clone();
        f.prepare_fetch_response(full_fetch_response(
            &tip0,
            Some(f.records.clone()),
            f.acquired_records.clone(),
            Errors::None,
            Errors::None,
        ));
        f.network_poll().await;
        assert!(f.manager.has_completed_fetches());

        f.fetch_records().await;

        assert_eq!(1, f.send_fetches());
        assert!(!f.manager.has_completed_fetches());
        assert_eq!(3.0, f.metric_value(&f.share_fetch_metrics_registry.acknowledgement_send_total));
    }

    #[tokio::test(flavor = "current_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testAcknowledgeErrorMessagePropagatedFromFetchResponse"
    )]
    async fn test_acknowledge_error_message_propagated_from_fetch_response() {
        let mut f = Fixture::new();
        f.manager.set_acknowledgement_commit_callback_registered(true);

        f.assign_from_subscribed(&[f.tp0.clone()]);
        f.send_fetch_and_verify_response(f.records.clone(), f.acquired_records.clone(), Errors::None, Errors::None)
            .await;

        f.fetch_records().await;

        let acknowledgements = single_acknowledgement(1, AcknowledgeType::Accept);
        f.manager.fetch(node_acks(&f.tip0, 0, &acknowledgements));

        assert_eq!(1, f.send_fetches());
        assert!(!f.manager.has_completed_fetches());

        let acknowledge_error_message = "ack failure with broker context";
        let tip0 = f.tip0.clone();
        let mut response = full_fetch_response(
            &tip0,
            Some(f.records.clone()),
            f.acquired_records.clone(),
            Errors::None,
            Errors::UnknownServerError,
        );
        for topic_response in &mut response.data_mut().responses {
            for partition in &mut topic_response.partitions {
                partition.set_acknowledge_error_message(Some(acknowledge_error_message.to_string()));
            }
        }

        f.prepare_fetch_response(response);
        f.network_poll().await;

        assert!(f.manager.has_completed_fetches());
        f.fetch_records().await;

        assert_eq!(1, f.completed_acknowledgements().len());
        let acknowledgements_map = f.completed_acknowledgements()[0].clone();
        assert_same_acknowledgements(&[(&tip0, &acknowledgements)], &acknowledgements_map);

        let acknowledge_error = acknowledgements_map[&tip0]
            .acknowledge_error()
            .expect("completed with an error");
        assert!(acknowledge_error.is_api_error(), "{acknowledge_error:?}");
        assert_eq!(acknowledge_error_message, acknowledge_error.message());
    }

    /// `testCommitAsyncWithSubscriptionChange` and `testCommitSyncWithSubscriptionChange`.
    async fn commit_with_subscription_change(commit_sync: bool) {
        let mut f = Fixture::new();
        f.manager.set_acknowledgement_commit_callback_registered(true);

        f.assign_from_subscribed(&[f.tp0.clone()]);
        f.send_fetch_and_verify_response(f.records.clone(), f.acquired_records.clone(), Errors::None, Errors::None)
            .await;

        let acknowledgements = get_acknowledgements(
            1,
            &[
                AcknowledgeType::Accept,
                AcknowledgeType::Accept,
                AcknowledgeType::Reject,
            ],
        );

        f.subscribe_to_share_group(&[TOPIC_NAME2]);
        f.assign(&[f.t2p0.clone()]);

        f.update_metadata(&Fixture::metadata_update_with_ids(
            1,
            &[(TOPIC_NAME2, 1)],
            VALID_LEADER_EPOCH,
            &f.topic_ids,
        ));

        if commit_sync {
            let deadline = f.deadline(100);
            f.manager.commit_sync(node_acks(&f.tip0, 0, &acknowledgements), deadline);
        } else {
            let deadline = f.deadline(DEFAULT_API_TIMEOUT_MS);
            f.manager.commit_async(node_acks(&f.tip0, 0, &acknowledgements), deadline);
        }

        assert_eq!(1, f.send_acknowledgements());

        let tip0 = f.tip0.clone();
        f.prepare_acknowledge_response(full_acknowledge_response(&tip0, Errors::None));
        f.network_poll().await;

        assert_eq!(3, f.completed_acknowledgements()[0][&tip0].size());
        assert!(f.completed_acknowledgements()[0][&tip0].acknowledge_error().is_none());

        // We should send a fetch to the newly subscribed partition.
        assert_eq!(1, f.send_fetches());

        let t2ip0 = f.t2ip0.clone();
        f.prepare_fetch_response(full_fetch_response(
            &t2ip0,
            Some(f.records.clone()),
            f.acquired_records.clone(),
            Errors::None,
            Errors::None,
        ));
        f.network_poll().await;
        assert!(f.manager.has_completed_fetches());
    }

    #[tokio::test(flavor = "current_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testCommitAsyncWithSubscriptionChange"
    )]
    async fn test_commit_async_with_subscription_change() {
        commit_with_subscription_change(false).await;
    }

    #[tokio::test(flavor = "current_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testCommitSyncWithSubscriptionChange"
    )]
    async fn test_commit_sync_with_subscription_change() {
        commit_with_subscription_change(true).await;
    }

    /// `@ValueSource(booleans = {true, false})`.
    ///
    /// Java renews with the `Acknowledgements` object it handed to the manager,
    /// which the manager completed; the Rust manager completes its own copy, so
    /// the test renews with the copy the completion event carried.
    #[tokio::test(flavor = "current_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testCommitWithRenewAcknowledgements"
    )]
    async fn test_commit_with_renew_acknowledgements() {
        for commit_sync in [true, false] {
            let mut f = Fixture::new();
            f.manager.set_acknowledgement_commit_callback_registered(true);

            f.assign_from_subscribed(&[f.tp0.clone()]);
            f.send_fetch_and_verify_response(f.records.clone(), f.acquired_records.clone(), Errors::None, Errors::None)
                .await;
            let mut fetch = f.collect_fetch_ok().await;
            assert_eq!(3, fetch.num_records());

            let node_id0 = f.leader_for(&f.tp0).id();

            // The application renews the delivered records. They are still logically held by the consumer.
            fetch.acknowledge_all(AcknowledgeType::Renew);
            let renew_acknowledgements = fetch.take_acknowledged_records();
            let tip0 = f.tip0.clone();
            let acknowledgements = renew_acknowledgements[&tip0].acknowledgements().clone();

            // Renew acknowledgements are committed through a ShareAcknowledge request (not piggybacked on a ShareFetch).
            let future = if commit_sync {
                let deadline = f.deadline(2000);
                Some(f.manager.commit_sync(renew_acknowledgements, deadline))
            } else {
                let deadline = f.deadline(DEFAULT_API_TIMEOUT_MS);
                f.manager.commit_async(renew_acknowledgements, deadline);
                None
            };

            // The partition is no longer assigned, so the share session would normally be tidied up and eventually closed.
            // However, the delivery of the records with renew acknowledgements must be completed before that can happen.
            f.assign(&[]);

            let now = f.now();
            let poll_result = f.manager.poll(now);
            assert_eq!(1, poll_result.unsent_requests.len(), "commit_sync={commit_sync}");
            let sent = f.add_all(poll_result);
            assert!(sent[0].share_acknowledge().is_renew_ack);
            assert_eq!(0, f.renewed_records().len());

            // While the renew acknowledgements are in flight, the share session is not closed.
            assert_eq!(0, f.send_fetches());
            assert!(f.manager.session_handler(node_id0).is_some());

            // The broker responds to the renew acknowledgements and the records are reported to the application.
            f.prepare_acknowledge_response(full_acknowledge_response(&tip0, Errors::None));
            f.network_poll().await;
            assert_same_acknowledgements(&[(&tip0, &acknowledgements)], &f.completed_acknowledgements()[0]);
            let completed_renew_acknowledgements = f.completed_acknowledgements()[0][&tip0].clone();
            assert_eq!(&HashSet::from([1, 2, 3]), f.renewed_records());
            if let Some(future) = future {
                assert!(future.is_done());
            }

            // The revoked partition is removed from the share session, but the session cannot be closed while the records are still held.
            let remove_result = f.send_fetches_return_poll_result();
            assert_eq!(1, remove_result.len());
            let remove_data = remove_result[0].share_fetch();
            assert_ne!(ShareRequestMetadata::FINAL_EPOCH, remove_data.share_session_epoch);
            assert_eq!(1, remove_data.forgotten_topics_data.len());
            assert_eq!(tip0.topic_id(), remove_data.forgotten_topics_data[0].topic_id);
            f.prepare_fetch_response(empty_fetch_response());
            f.network_poll().await;

            assert_eq!(0, f.send_fetches_return_poll_result().len());
            assert!(f.manager.session_handler(node_id0).is_some());

            fetch
                .renew(&HashMap::from([(tip0.clone(), completed_renew_acknowledgements)]), None)
                .expect("the acknowledgements are completed");
            fetch
                .take_renewed_records(&f.deserializers)
                .await
                .expect("the renewed records decode");
            assert_eq!(3, fetch.num_records());
            assert_eq!(0, f.send_fetches_return_poll_result().len());
            assert!(f.manager.session_handler(node_id0).is_some());

            // The application finally accepts the records and the acknowledgements are sent.
            fetch.acknowledge_all(AcknowledgeType::Accept);
            f.manager.fetch(fetch.take_acknowledged_records());
            let ack_result = f.send_fetches_return_poll_result();
            assert_eq!(1, ack_result.len());

            assert_ne!(
                ShareRequestMetadata::FINAL_EPOCH,
                ack_result[0].share_fetch().share_session_epoch
            );
            f.prepare_fetch_response(full_fetch_response(
                &tip0,
                empty_records(),
                f.empty_acquired_records.clone(),
                Errors::None,
                Errors::None,
            ));
            f.network_poll().await;

            // The application consumes the fetch response corresponding to the piggybacked acknowledgements.
            f.fetch_records().await;

            // With the record finally acknowledged, the empty share session is closed.
            let close_result = f.send_fetches_return_poll_result();
            assert_eq!(1, close_result.len());
            assert_eq!(
                ShareRequestMetadata::FINAL_EPOCH,
                close_result[0].share_fetch().share_session_epoch
            );
            f.prepare_fetch_response(empty_fetch_response());
            f.network_poll().await;

            assert!(f.manager.session_handler(node_id0).is_none());
        }
    }

    #[tokio::test(flavor = "current_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testCloseWithSubscriptionChange"
    )]
    async fn test_close_with_subscription_change() {
        let mut f = Fixture::new();
        f.manager.set_acknowledgement_commit_callback_registered(true);

        f.assign_from_subscribed(&[f.tp0.clone()]);
        f.send_fetch_and_verify_response(f.records.clone(), f.acquired_records.clone(), Errors::None, Errors::None)
            .await;

        let acknowledgements = get_acknowledgements(
            1,
            &[
                AcknowledgeType::Accept,
                AcknowledgeType::Accept,
                AcknowledgeType::Reject,
            ],
        );

        f.subscribe_to_share_group(&[TOPIC_NAME2]);
        f.assign(&[f.t2p0.clone()]);

        f.update_metadata(&Fixture::metadata_update_with_ids(
            1,
            &[(TOPIC_NAME2, 1)],
            VALID_LEADER_EPOCH,
            &f.topic_ids,
        ));

        let deadline = f.deadline(100);
        f.manager
            .acknowledge_on_close(node_acks(&f.tip0, 0, &acknowledgements), deadline);

        assert_eq!(1, f.send_acknowledgements());

        let tip0 = f.tip0.clone();
        f.prepare_acknowledge_response(full_acknowledge_response(&tip0, Errors::None));
        f.network_poll().await;

        assert_eq!(3, f.completed_acknowledgements()[0][&tip0].size());
        assert!(f.completed_acknowledgements()[0][&tip0].acknowledge_error().is_none());

        // As we are closing, we would not send any more fetches.
        assert_eq!(0, f.send_fetches());
    }

    #[tokio::test(flavor = "current_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testShareFetchWithSubscriptionChange"
    )]
    async fn test_share_fetch_with_subscription_change() {
        let mut f = Fixture::new();

        f.assign_from_subscribed(&[f.tp0.clone()]);
        f.send_fetch_and_verify_response(f.records.clone(), f.acquired_records.clone(), Errors::None, Errors::None)
            .await;

        let acknowledgements = get_acknowledgements(
            1,
            &[
                AcknowledgeType::Accept,
                AcknowledgeType::Release,
                AcknowledgeType::Accept,
            ],
        );

        // Send acknowledgements via ShareFetch
        f.manager.fetch(node_acks(&f.tip0, 0, &acknowledgements));
        f.fetch_records().await;

        // Subscription changes.
        f.subscribe_to_share_group(&[TOPIC_NAME2]);
        f.assign(&[f.t2p0.clone()]);

        f.update_metadata(&Fixture::metadata_update_with_ids(
            1,
            &[(TOPIC_NAME2, 1)],
            VALID_LEADER_EPOCH,
            &f.topic_ids,
        ));

        assert_eq!(1, f.send_fetches());
        assert!(!f.manager.has_completed_fetches());
        assert_eq!(3.0, f.metric_value(&f.share_fetch_metrics_registry.acknowledgement_send_total));
    }

    /// The request of `requests` addressed to `node`, and the other one.
    fn split_by_node<'a>(requests: &'a [SentRequest], node: &Node) -> (&'a SentRequest, &'a SentRequest) {
        assert_eq!(2, requests.len());
        if &requests[0].node == node {
            (&requests[0], &requests[1])
        } else {
            (&requests[1], &requests[0])
        }
    }

    #[tokio::test(flavor = "current_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testShareFetchWithSubscriptionChangeMultipleNodes"
    )]
    async fn test_share_fetch_with_subscription_change_multiple_nodes() {
        let mut f = Fixture::new();

        f.subscribe_to_share_group(&[TOPIC_NAME]);
        f.assign(&[f.tp0.clone()]);

        f.update_metadata(&Fixture::metadata_update_with_ids(
            2,
            &[(TOPIC_NAME, 2)],
            VALID_LEADER_EPOCH,
            &f.topic_ids,
        ));
        let node_id0 = f.node(0);
        let node_id1 = f.node(1);
        let tp0_leader = f.leader_for(&f.tp0);
        let tp1_leader = f.leader_for(&f.tp1);

        assert_eq!(node_id0, tp0_leader);
        assert_eq!(node_id1, tp1_leader);

        f.send_fetch_and_verify_response(
            f.records.clone(),
            f.empty_acquired_records.clone(),
            Errors::None,
            Errors::None,
        )
        .await;

        let acknowledgements = get_acknowledgements(
            0,
            &[
                AcknowledgeType::Accept,
                AcknowledgeType::Release,
                AcknowledgeType::Accept,
            ],
        );

        // Send acknowledgements via ShareFetch
        f.manager.fetch(node_acks(&f.tip0, 0, &acknowledgements));
        f.fetch_records().await;

        // Subscription changes.
        f.assign(&[f.tp1.clone()]);

        let poll_result = f.send_fetches_return_poll_result();
        let (request1, request2) = split_by_node(&poll_result, &node_id0);
        assert_eq!(node_id1, request2.node);
        let (builder1, builder2) = (request1.share_fetch(), request2.share_fetch());

        // Verify the builder data for node0.
        assert_eq!(1, builder1.topics.len());
        let fetch_topic = &builder1.topics[0];
        assert_eq!(f.tip0.topic_id(), fetch_topic.topic_id);
        assert_eq!(1, fetch_topic.partitions.len());
        let fetch_partition = &fetch_topic.partitions[0];
        assert_eq!(0, fetch_partition.partition_index);
        assert_eq!(1, fetch_partition.acknowledgement_batches.len());
        assert_eq!(0, fetch_partition.acknowledgement_batches[0].first_offset);
        assert_eq!(2, fetch_partition.acknowledgement_batches[0].last_offset);

        assert_eq!(1, builder1.forgotten_topics_data.len());
        assert_eq!(f.tip0.topic_id(), builder1.forgotten_topics_data[0].topic_id);
        assert_eq!(1, builder1.forgotten_topics_data[0].partitions.len());
        assert_eq!(0, builder1.forgotten_topics_data[0].partitions[0]);

        // Verify the builder data for node1.
        assert_eq!(1, builder2.topics.len());
        let fetch_topic = &builder2.topics[0];
        assert_eq!(f.tip1.topic_id(), fetch_topic.topic_id);
        assert_eq!(1, fetch_topic.partitions.len());
        assert_eq!(1, fetch_topic.partitions[0].partition_index);
    }

    #[tokio::test(flavor = "current_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testShareFetchWithSubscriptionChangeMultipleNodesEmptyAcknowledgements"
    )]
    async fn test_share_fetch_with_subscription_change_multiple_nodes_empty_acknowledgements() {
        let mut f = Fixture::new();

        f.subscribe_to_share_group(&[TOPIC_NAME]);
        f.assign(&[f.tp0.clone()]);

        f.update_metadata(&Fixture::metadata_update_with_ids(
            2,
            &[(TOPIC_NAME, 2)],
            VALID_LEADER_EPOCH,
            &f.topic_ids,
        ));
        let node_id0 = f.node(0);
        let node_id1 = f.node(1);
        let tp0_leader = f.leader_for(&f.tp0);
        let tp1_leader = f.leader_for(&f.tp1);

        assert_eq!(node_id0, tp0_leader);
        assert_eq!(node_id1, tp1_leader);

        // Send the first ShareFetch with an empty response
        f.send_fetch_and_verify_response(
            f.records.clone(),
            f.empty_acquired_records.clone(),
            Errors::None,
            Errors::None,
        )
        .await;

        f.fetch_records().await;

        // Change the subscription.
        f.assign(&[f.tp1.clone()]);

        // We build a request to node 1 to fetch tip1, and a request to node 0 to remove tip0
        // from the share session even though there are no acknowledgements to send.
        let poll_result = f.send_fetches_return_poll_result();
        let (node0_request, node1_request) = split_by_node(&poll_result, &node_id0);
        assert_eq!(node_id1, node1_request.node);
        let (node0_builder, node1_builder) = (node0_request.share_fetch(), node1_request.share_fetch());

        // node1 fetches the newly assigned partition tip1.
        assert_eq!(1, node1_builder.topics.len());
        let fetch_topic = &node1_builder.topics[0];
        assert_eq!(f.tip1.topic_id(), fetch_topic.topic_id);
        assert_eq!(1, fetch_topic.partitions.len());
        assert_eq!(1, fetch_topic.partitions[0].partition_index);
        assert_eq!(0, node1_builder.forgotten_topics_data.len());

        // node0 removes tip0 from the share session and fetches nothing.
        assert_eq!(0, node0_builder.topics.len());
        assert_eq!(1, node0_builder.forgotten_topics_data.len());
        assert_eq!(f.tip0.topic_id(), node0_builder.forgotten_topics_data[0].topic_id);
        assert_eq!(1, node0_builder.forgotten_topics_data[0].partitions.len());
        assert_eq!(0, node0_builder.forgotten_topics_data[0].partitions[0]);
    }

    #[tokio::test(flavor = "current_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testShareFetchRemovesUnassignedPartitionFromSession"
    )]
    async fn test_share_fetch_removes_unassigned_partition_from_session() {
        let mut f = Fixture::new();

        f.assign_from_subscribed(&[f.tp0.clone()]);

        // Establish the share session by fetching from tp0.
        f.send_fetch_and_verify_response(
            f.records.clone(),
            f.empty_acquired_records.clone(),
            Errors::None,
            Errors::None,
        )
        .await;
        f.fetch_records().await;

        // The partition is no longer assigned and there are no acknowledgements to send.
        f.assign(&[]);

        // We still build a ShareFetch to remove tip0 from the share session on the broker.
        let poll_result = f.send_fetches_return_poll_result();
        assert_eq!(1, poll_result.len());

        let builder = poll_result[0].share_fetch();
        assert_eq!(0, builder.topics.len());
        assert_eq!(1, builder.forgotten_topics_data.len());
        assert_eq!(f.tip0.topic_id(), builder.forgotten_topics_data[0].topic_id);
        assert_eq!(1, builder.forgotten_topics_data[0].partitions.len());
        assert_eq!(0, builder.forgotten_topics_data[0].partitions[0]);

        // The partition has already been removed from the session, so no further ShareFetch is built.
        assert_eq!(0, f.send_fetches());
    }

    #[tokio::test(flavor = "current_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testCloseSessionWhenNoSharePartitions"
    )]
    async fn test_close_session_when_no_share_partitions() {
        let mut f = Fixture::new();

        f.assign_from_subscribed(&[f.tp0.clone()]);

        // Establish the share session by fetching from tp0 and consume the (empty) buffered fetch.
        f.send_fetch_and_verify_response(
            f.records.clone(),
            f.empty_acquired_records.clone(),
            Errors::None,
            Errors::None,
        )
        .await;
        f.fetch_records().await;

        let node0 = f.leader_for(&f.tp0);
        let node_id0 = node0.id();
        assert!(f.manager.session_handler(node_id0).is_some());

        // The partition is no longer assigned, so a ShareFetch is built to remove tip0 from the share session,
        // leaving it empty.
        f.assign(&[]);
        assert_eq!(1, f.send_fetches());
        f.prepare_fetch_response(empty_fetch_response());
        f.network_poll().await;

        // The next poll closes the now-empty share session using a final-epoch ShareFetch.
        let poll_result = f.send_fetches_return_poll_result();
        assert_eq!(1, poll_result.len());
        assert_eq!(node0, poll_result[0].node);
        let builder = poll_result[0].share_fetch();
        assert_eq!(ShareRequestMetadata::FINAL_EPOCH, builder.share_session_epoch);
        assert!(builder.topics.is_empty());
        assert!(builder.forgotten_topics_data.is_empty());

        // The session handle is only removed once the close response is received.
        assert!(f.manager.session_handler(node_id0).is_some());
        f.prepare_fetch_response(empty_fetch_response());
        f.network_poll().await;
        assert!(f.manager.session_handler(node_id0).is_none());

        // With the session closed, there is nothing left to send.
        assert_eq!(0, f.send_fetches());
    }

    #[tokio::test(flavor = "current_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testSecondFetchBeforeCollectClearsInflightRecords"
    )]
    async fn test_second_fetch_before_collect_clears_inflight_records() {
        let mut f = Fixture::new();
        f.assign_from_subscribed(&[f.tp0.clone()]);

        // Send and receive a successful response for tip0.
        f.send_fetch_and_verify_response(f.records.clone(), f.acquired_records.clone(), Errors::None, Errors::None)
            .await;

        // Call fetch() again, what the next poll() call does, calls fetchMoreRecords and lets
        // a second fetch for the same partition go out and complete before the first has been collected.
        let second_records = build_records(4, 2, 4);
        let second_acquired_records = acquired_records(4, 2);
        assert_eq!(1, f.send_fetches());
        let tip0 = f.tip0.clone();
        f.prepare_fetch_response(full_fetch_response(
            &tip0,
            Some(second_records),
            second_acquired_records,
            Errors::None,
            Errors::None,
        ));
        f.network_poll().await;

        // Drain both completed fetches for tip0 in one collect() call.
        let mut fetch = f.collect_fetch_ok().await;
        let fetched_records = fetch.records().remove(&f.tp0).expect("records for tp0");
        assert_eq!(
            5,
            fetched_records.len(),
            "records from both fetches should be visible after the merge"
        );

        // Acknowledge every record the application actually received.
        fetch.acknowledge_all(AcknowledgeType::Accept);
        f.manager.fetch(fetch.take_acknowledged_records());

        // Nothing should still look buffered/pending i.e. every record the application received has been
        // acknowledged. If the merged-away second batch's records were never cleared, tip0 leaks.
        assert!(
            f.manager.share_fetch_buffer.buffered_partitions().is_empty(),
            "No partition should still be considered buffered after all delivered records are acknowledged"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testEmptyResponseThenDataResponseBeforeCollectClearsInflightRecords"
    )]
    async fn test_empty_response_then_data_response_before_collect_clears_inflight_records() {
        let mut f = Fixture::new();
        f.assign_from_subscribed(&[f.tp0.clone()]);

        // The first request returns no acquired records. The empty result is still placed in the fetch buffer.
        assert_eq!(1, f.send_fetches());
        let tip0 = f.tip0.clone();
        f.prepare_fetch_response(full_fetch_response(
            &tip0,
            empty_records(),
            f.empty_acquired_records.clone(),
            Errors::None,
            Errors::None,
        ));
        f.network_poll().await;
        assert!(f.manager.has_completed_fetches());

        // Because no records were acquired, the request manager polls the same node straight away, without the
        // application having collected anything. The second response carries records and lands behind the empty
        // completed fetch for the same partition.
        assert_eq!(1, f.send_acknowledgements());
        f.prepare_fetch_response(full_fetch_response(
            &tip0,
            Some(f.records.clone()),
            f.acquired_records.clone(),
            Errors::None,
            Errors::None,
        ));
        f.network_poll().await;

        // Drive the application side the way poll() does in implicit mode: collect, acknowledge everything,
        // take the acknowledgements, and repeat until the buffer has been drained.
        let mut records_delivered = 0;
        let mut acks_taken = 0;
        for _ in 0..10 {
            let mut fetch = f.collect_fetch_ok().await;
            records_delivered += fetch.num_records();
            fetch.acknowledge_all(AcknowledgeType::Accept);
            let acks = fetch.take_acknowledged_records();
            if let Some(node_acknowledgements) = acks.get(&tip0) {
                acks_taken += node_acknowledgements.acknowledgements().size();
            }
            assert!(fetch.is_empty());
            if !f.manager.has_completed_fetches() {
                break;
            }
        }
        assert_eq!(3, records_delivered);
        assert_eq!(3, acks_taken);

        // Every delivered record has been acknowledged, so no partition should still be considered buffered.
        assert!(
            f.manager.share_fetch_buffer.buffered_partitions().is_empty(),
            "No partition should still be considered buffered after all delivered records are acknowledged"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testDoesNotCloseSessionWhileRecordsBuffered"
    )]
    async fn test_does_not_close_session_while_records_buffered() {
        let mut f = Fixture::new();

        f.assign_from_subscribed(&[f.tp0.clone()]);

        // Establish the share session, leaving the fetched records unconsumed in the buffer.
        f.send_fetch_and_verify_response(f.records.clone(), f.acquired_records.clone(), Errors::None, Errors::None)
            .await;
        let node0 = f.leader_for(&f.tp0);
        let node_id0 = node0.id();

        // Remove the partition from the session so the session becomes empty.
        f.assign(&[]);
        assert_eq!(1, f.send_fetches());
        f.prepare_fetch_response(empty_fetch_response());
        f.network_poll().await;

        // The session is empty, but records for the node remain in the buffer, so the session is not closed.
        assert_eq!(0, f.send_fetches_return_poll_result().len());
        assert!(f.manager.session_handler(node_id0).is_some());

        // Consuming the records hands them to the application, but leaves the acknowledgements outstanding and the session
        // is not closed.
        let mut fetch = f.collect_fetch_ok().await;
        assert_eq!(0, f.send_fetches_return_poll_result().len());
        assert!(f.manager.session_handler(node_id0).is_some());

        // Acknowledge the records and send the acknowledgements. This is not a close request.
        fetch.acknowledge_all(AcknowledgeType::Accept);
        f.manager.fetch(fetch.take_acknowledged_records());
        let ack_result = f.send_fetches_return_poll_result();
        assert_eq!(1, ack_result.len());
        assert_ne!(
            ShareRequestMetadata::FINAL_EPOCH,
            ack_result[0].share_fetch().share_session_epoch
        );
        f.prepare_fetch_response(empty_fetch_response());
        f.network_poll().await;

        // Once the acknowledgements have been sent, the empty session is closed.
        let close_result = f.send_fetches_return_poll_result();
        assert_eq!(1, close_result.len());
        assert_eq!(node0, close_result[0].node);
        assert_eq!(
            ShareRequestMetadata::FINAL_EPOCH,
            close_result[0].share_fetch().share_session_epoch
        );
    }

    #[tokio::test(flavor = "current_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testDoesNotCloseSessionWhileAcknowledgementsPending"
    )]
    async fn test_does_not_close_session_while_acknowledgements_pending() {
        let mut f = Fixture::new();

        f.assign_from_subscribed(&[f.tp0.clone()]);

        // Establish the share session by fetching from tp0 and consume the (empty) buffered fetch.
        f.send_fetch_and_verify_response(
            f.records.clone(),
            f.empty_acquired_records.clone(),
            Errors::None,
            Errors::None,
        )
        .await;
        f.fetch_records().await;

        let node0 = f.leader_for(&f.tp0);
        let node_id0 = node0.id();
        assert!(f.manager.session_handler(node_id0).is_some());

        // Remove the partition from the session so the session becomes empty.
        f.assign(&[]);
        assert_eq!(1, f.send_fetches());
        f.prepare_fetch_response(empty_fetch_response());
        f.network_poll().await;

        // There are acknowledgements still to be delivered for the node, so the session is not closed.
        let acknowledgements = get_acknowledgements(
            0,
            &[
                AcknowledgeType::Accept,
                AcknowledgeType::Accept,
                AcknowledgeType::Accept,
            ],
        );
        f.manager.fetch(node_acks(&f.tip0, node_id0, &acknowledgements));

        let now = f.now();
        let mut poll_result = f.manager.poll(now);
        assert_eq!(1, poll_result.unsent_requests.len());
        let request = SentRequest::of(&mut poll_result.unsent_requests[0]);
        assert_eq!(node0, request.node);
        assert_ne!(ShareRequestMetadata::FINAL_EPOCH, request.share_fetch().share_session_epoch);
        assert!(f.manager.session_handler(node_id0).is_some());
    }

    #[tokio::test(flavor = "current_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testShareFetchAndCloseMultipleNodes"
    )]
    async fn test_share_fetch_and_close_multiple_nodes() {
        let mut f = Fixture::new();
        f.manager.set_acknowledgement_commit_callback_registered(true);

        f.subscribe_to_share_group(&[TOPIC_NAME]);
        f.assign(&[f.tp0.clone(), f.tp1.clone()]);

        f.update_metadata(&Fixture::metadata_update_with_ids(
            2,
            &[(TOPIC_NAME, 2)],
            VALID_LEADER_EPOCH,
            &f.topic_ids,
        ));

        assert_eq!(2, f.send_fetches());
        assert!(!f.manager.has_completed_fetches());

        let (tip0, tip1) = (f.tip0.clone(), f.tip1.clone());
        f.prepare_fetch_response(full_fetch_response(
            &tip0,
            Some(f.records.clone()),
            f.acquired_records.clone(),
            Errors::None,
            Errors::None,
        ));
        f.prepare_fetch_response(full_fetch_response(
            &tip1,
            Some(f.records.clone()),
            f.acquired_records.clone(),
            Errors::None,
            Errors::None,
        ));
        f.network_poll().await;
        assert!(f.manager.has_completed_fetches());

        let acknowledgements = get_acknowledgements(
            1,
            &[
                AcknowledgeType::Accept,
                AcknowledgeType::Accept,
                AcknowledgeType::Reject,
            ],
        );
        let acknowledgements1 = get_acknowledgements(
            1,
            &[
                AcknowledgeType::Accept,
                AcknowledgeType::Accept,
                AcknowledgeType::Reject,
            ],
        );

        let acknowledgements_map = HashMap::from([
            (tip0.clone(), NodeAcknowledgements::new(0, acknowledgements)),
            (tip1.clone(), NodeAcknowledgements::new(1, acknowledgements1)),
        ]);
        let deadline = f.deadline(1000);
        f.manager.acknowledge_on_close(acknowledgements_map, deadline);

        assert_eq!(2, f.send_acknowledgements());

        f.prepare_acknowledge_response(full_acknowledge_response(&tip0, Errors::None));
        f.prepare_acknowledge_response(full_acknowledge_response(&tip1, Errors::None));
        f.network_poll().await;

        assert_eq!(3, f.completed_acknowledgements()[0][&tip0].size());
        assert_eq!(3, f.completed_acknowledgements()[0][&tip1].size());

        assert_eq!(0, f.send_acknowledgements());
        assert!(f.request_states(0).is_none());
        assert!(f.request_states(1).is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testRetryAcknowledgementsWithLeaderChange"
    )]
    async fn test_retry_acknowledgements_with_leader_change() {
        let mut f = Fixture::new();

        f.subscribe_to_share_group(&[TOPIC_NAME]);
        f.assign(&[f.tp0.clone()]);

        f.update_metadata(&Fixture::metadata_update_with_ids(
            2,
            &[(TOPIC_NAME, 1)],
            VALID_LEADER_EPOCH,
            &f.topic_ids,
        ));
        let node_id0 = f.node(0);
        let node_id1 = f.node(1);
        let node_list = vec![node_id0, node_id1.clone()];

        f.send_fetch_and_verify_response(build_records(1, 6, 1), acquired_records(1, 6), Errors::None, Errors::None)
            .await;

        let acknowledgements = get_acknowledgements(
            1,
            &[
                AcknowledgeType::Accept,
                AcknowledgeType::Accept,
                AcknowledgeType::Reject,
                AcknowledgeType::Accept,
                AcknowledgeType::Release,
                AcknowledgeType::Accept,
            ],
        );

        let deadline = f.deadline(60000);
        f.manager.commit_sync(node_acks(&f.tip0, 0, &acknowledgements), deadline);
        assert!(f.request_states(0).and_then(Tuple::async_request).is_none());

        let tip0 = f.tip0.clone();
        assert_eq!(1, f.sync_request_queue_size(0));
        assert_eq!(6, f.first_sync_request(0).acknowledgements_to_send_count(&tip0));

        assert_eq!(1, f.send_acknowledgements());
        assert_eq!(6, f.first_sync_request(0).in_flight_acknowledgements_count(&tip0));

        // Fail the acknowledgement and provide the new current leader information - this should stop the retry
        f.prepare_acknowledge_response(full_acknowledge_response_with_leader(
            &tip0,
            Errors::NotLeaderOrFollower,
            node_id1.id(),
            VALID_LEADER_EPOCH + 1,
            &node_list,
        ));
        f.network_poll().await;

        assert_eq!(0, f.first_sync_request(0).in_flight_acknowledgements_count(&tip0));
        assert_eq!(0, f.first_sync_request(0).incomplete_acknowledgements_count(&tip0));
    }

    #[tokio::test(flavor = "current_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testCallbackHandlerConfig"
    )]
    async fn test_callback_handler_config() {
        let mut f = Fixture::new();
        f.manager.set_acknowledgement_commit_callback_registered(true);

        f.assign_from_subscribed(&[f.tp0.clone()]);

        f.send_fetch_and_verify_response(f.records.clone(), f.acquired_records.clone(), Errors::None, Errors::None)
            .await;

        let acknowledgements = get_acknowledgements(1, &[AcknowledgeType::Accept, AcknowledgeType::Accept]);

        let deadline = f.deadline(DEFAULT_API_TIMEOUT_MS);
        f.manager.commit_async(node_acks(&f.tip0, 0, &acknowledgements), deadline);

        assert_eq!(1, f.send_acknowledgements());

        let tip0 = f.tip0.clone();
        f.prepare_acknowledge_response(full_acknowledge_response(&tip0, Errors::None));
        f.network_poll().await;
        assert!(f.manager.has_completed_fetches());

        assert_same_acknowledgements(&[(&tip0, &acknowledgements)], &f.completed_acknowledgements()[0]);

        f.clear_completed_acknowledgements();

        // Setting the boolean to false, indicating there is no callback handler registered.
        f.manager.set_acknowledgement_commit_callback_registered(false);

        let acknowledgements2 = single_acknowledgement(3, AcknowledgeType::Accept);

        let deadline = f.deadline(DEFAULT_API_TIMEOUT_MS);
        f.manager.commit_async(node_acks(&f.tip0, 0, &acknowledgements2), deadline);

        // Wait for backoff time before sending the next request.
        f.time.sleep(RETRY_BACKOFF_MS);
        assert_eq!(1, f.send_acknowledgements());

        f.prepare_acknowledge_response(full_acknowledge_response(&tip0, Errors::None));
        f.network_poll().await;
        assert!(f.manager.has_completed_fetches());

        // We expect no acknowledgements to be added as the callback handler is not configured.
        assert_eq!(0, f.completed_acknowledgements().len());
    }

    #[tokio::test(flavor = "current_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testAcknowledgementCommitCallbackMultiplePartitionCommitAsync"
    )]
    async fn test_acknowledgement_commit_callback_multiple_partition_commit_async() {
        let mut f = Fixture::new();
        f.manager.set_acknowledgement_commit_callback_registered(true);

        f.assign_from_subscribed(&[f.tp0.clone(), f.t2p0.clone()]);

        assert_eq!(1, f.send_fetches());
        assert!(!f.manager.has_completed_fetches());

        let (tip0, t2ip0) = (f.tip0.clone(), f.t2ip0.clone());
        f.prepare_fetch_response(fetch_response(
            vec![
                (
                    tip0.clone(),
                    partition_data_for_fetch(
                        &tip0,
                        Some(f.records.clone()),
                        f.acquired_records.clone(),
                        Errors::None,
                        Errors::None,
                    ),
                ),
                (
                    t2ip0.clone(),
                    partition_data_for_fetch(
                        &t2ip0,
                        Some(f.records.clone()),
                        f.acquired_records.clone(),
                        Errors::None,
                        Errors::None,
                    ),
                ),
            ],
            &[],
        ));

        f.network_poll().await;
        assert!(f.manager.has_completed_fetches());

        let acknowledgements = get_acknowledgements(
            1,
            &[
                AcknowledgeType::Accept,
                AcknowledgeType::Accept,
                AcknowledgeType::Reject,
            ],
        );

        let acknowledgements2 = get_acknowledgements(
            1,
            &[
                AcknowledgeType::Accept,
                AcknowledgeType::Accept,
                AcknowledgeType::Reject,
            ],
        );

        let acks = HashMap::from([
            (tip0.clone(), NodeAcknowledgements::new(0, acknowledgements)),
            (t2ip0.clone(), NodeAcknowledgements::new(0, acknowledgements2)),
        ]);

        let deadline = f.deadline(DEFAULT_API_TIMEOUT_MS);
        f.manager.commit_async(acks, deadline);

        assert_eq!(1, f.send_acknowledgements());

        f.prepare_acknowledge_response(full_acknowledge_response_with_errors(&[
            (&tip0, Errors::None),
            (&t2ip0, Errors::None),
        ]));

        f.network_poll().await;
        assert!(f.manager.has_completed_fetches());

        // Verifying that the acknowledgement commit callback is invoked for both the partitions.
        assert_eq!(2, f.completed_acknowledgements().len());
        assert_eq!(1, f.completed_acknowledgements()[0].len());
        assert_eq!(1, f.completed_acknowledgements()[1].len());
    }

    #[tokio::test(flavor = "current_thread")]
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testMultipleTopicsFetch")]
    async fn test_multiple_topics_fetch() {
        let mut f = Fixture::new();

        f.assign_from_subscribed(&[f.tp0.clone(), f.t2p0.clone()]);

        assert_eq!(1, f.send_fetches());
        assert!(!f.manager.has_completed_fetches());

        let (tip0, t2ip0) = (f.tip0.clone(), f.t2ip0.clone());
        f.prepare_fetch_response(fetch_response(
            vec![
                (
                    tip0.clone(),
                    partition_data_for_fetch(
                        &tip0,
                        Some(f.records.clone()),
                        f.acquired_records.clone(),
                        Errors::None,
                        Errors::None,
                    ),
                ),
                (
                    t2ip0.clone(),
                    partition_data_for_fetch(
                        &t2ip0,
                        Some(f.records.clone()),
                        f.empty_acquired_records.clone(),
                        Errors::TopicAuthorizationFailed,
                        Errors::None,
                    ),
                ),
            ],
            &[],
        ));

        f.network_poll().await;
        assert!(f.manager.has_completed_fetches());

        let mut share_fetch = f.collect_fetch_ok().await;
        let records = share_fetch.records();
        assert_eq!(1, records.len());
        // The first topic-partition is fetched successfully and returns all the records.
        assert_eq!(3, records[&f.tp0].len());
        // As the second topic failed authorization, we do not get the records in the ShareFetch.
        assert!(!records.contains_key(&f.t2p0));
        let error = f.collect_fetch().await.expect_err("a TopicAuthorizationError");
        assert!(matches!(error, Error::TopicAuthorization(_)), "{error:?}");
    }

    #[tokio::test(flavor = "current_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testMultipleTopicsFetchError"
    )]
    async fn test_multiple_topics_fetch_error() {
        let mut f = Fixture::new();

        f.assign_from_subscribed(&[f.tp0.clone(), f.t2p0.clone()]);

        assert_eq!(1, f.send_fetches());
        assert!(!f.manager.has_completed_fetches());

        let (tip0, t2ip0) = (f.tip0.clone(), f.t2ip0.clone());
        f.prepare_fetch_response(fetch_response(
            vec![
                (
                    t2ip0.clone(),
                    partition_data_for_fetch(
                        &t2ip0,
                        Some(f.records.clone()),
                        f.empty_acquired_records.clone(),
                        Errors::TopicAuthorizationFailed,
                        Errors::None,
                    ),
                ),
                (
                    tip0.clone(),
                    partition_data_for_fetch(
                        &tip0,
                        Some(f.records.clone()),
                        f.acquired_records.clone(),
                        Errors::None,
                        Errors::None,
                    ),
                ),
            ],
            &[],
        ));

        f.network_poll().await;
        assert!(f.manager.has_completed_fetches());

        // The first call throws TopicAuthorizationException because there are no records ready to return when the
        // exception is noticed.
        let error = f.collect_fetch().await.expect_err("a TopicAuthorizationError");
        assert!(matches!(error, Error::TopicAuthorization(_)), "{error:?}");
        // And then a second iteration returns the records.
        let mut share_fetch = f.collect_fetch_ok().await;
        let records = share_fetch.records();
        assert_eq!(1, records.len());
        // The first topic-partition is fetched successfully and returns all the records.
        assert_eq!(3, records[&f.tp0].len());
        // As the second topic failed authorization, we do not get the records in the ShareFetch.
        assert!(!records.contains_key(&f.t2p0));
    }

    #[tokio::test(flavor = "current_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testShareFetchInvalidResponse"
    )]
    async fn test_share_fetch_invalid_response() {
        let mut f = Fixture::new();
        f.manager.set_acknowledgement_commit_callback_registered(true);

        f.subscribe_to_share_group(&[TOPIC_NAME]);
        f.assign(&[f.tp0.clone()]);

        f.update_metadata(&Fixture::metadata_update_with_ids(
            1,
            &[(TOPIC_NAME, 1)],
            VALID_LEADER_EPOCH,
            &f.topic_ids,
        ));

        assert_eq!(1, f.send_fetches());
        assert!(!f.manager.has_completed_fetches());

        let t2ip0 = f.t2ip0.clone();
        f.prepare_fetch_response(full_fetch_response(
            &t2ip0,
            Some(f.records.clone()),
            f.acquired_records.clone(),
            Errors::None,
            Errors::None,
        ));
        f.network_poll().await;
        assert!(!f.manager.has_completed_fetches());
    }

    #[tokio::test(flavor = "current_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testShareAcknowledgeInvalidResponse"
    )]
    async fn test_share_acknowledge_invalid_response() {
        let mut f = Fixture::new();
        f.manager.set_acknowledgement_commit_callback_registered(true);

        f.subscribe_to_share_group(&[TOPIC_NAME]);
        f.assign(&[f.tp0.clone()]);

        f.update_metadata(&Fixture::metadata_update_with_ids(
            1,
            &[(TOPIC_NAME, 1)],
            VALID_LEADER_EPOCH,
            &f.topic_ids,
        ));

        f.send_fetch_and_verify_response(f.records.clone(), f.acquired_records.clone(), Errors::None, Errors::None)
            .await;

        f.fetch_records().await;

        let acknowledgements = single_acknowledgement(1, AcknowledgeType::Accept);

        let deadline = f.deadline(DEFAULT_API_TIMEOUT_MS);
        f.manager.commit_async(node_acks(&f.tip0, 0, &acknowledgements), deadline);

        assert_eq!(1, f.send_acknowledgements());

        // If a top-level error is received, we still retry the acknowledgements independent of the topic-partitions received in the response.
        let (tip0, t2ip0) = (f.tip0.clone(), f.t2ip0.clone());
        f.prepare_acknowledge_response(acknowledge_response_with_top_level_error(&t2ip0, Errors::LeaderNotAvailable));
        f.network_poll().await;

        assert_eq!(1, f.async_request(0).incomplete_acknowledgements_count(&tip0));

        // Wait for backoff time before sending the next request. (it can maximum be 1.2x of the configured backoff when acknowledge fails.)
        f.time.sleep(RETRY_BACKOFF_MS * 3 / 2);
        assert_eq!(1, f.send_acknowledgements());

        f.prepare_acknowledge_response(full_acknowledge_response(&t2ip0, Errors::None));
        f.network_poll().await;

        // If we do not get the expected partitions in the response, we fail these acknowledgements with InvalidRecordStateException.
        let error = f.completed_acknowledgements()[0][&tip0]
            .acknowledge_error()
            .expect("an error")
            .clone();
        assert!(matches!(error, Error::InvalidRecordState(_)), "{error:?}");
        assert_eq!(INVALID_RESPONSE, error.message());
        f.clear_completed_acknowledgements();

        // Send remaining acknowledgements through piggybacking on the next fetch.
        let acknowledgements1 = get_acknowledgements(2, &[AcknowledgeType::Accept, AcknowledgeType::Reject]);

        f.manager.fetch(node_acks(&f.tip0, 0, &acknowledgements1));

        assert_eq!(1, f.send_fetches());
        assert!(!f.manager.has_completed_fetches());

        f.prepare_fetch_response(full_fetch_response(
            &t2ip0,
            Some(f.records.clone()),
            f.acquired_records.clone(),
            Errors::None,
            Errors::None,
        ));
        f.network_poll().await;
        assert!(!f.manager.has_completed_fetches());

        // If we do not get the expected partitions in the response, we fail these acknowledgements with InvalidRecordStateException.
        let error = f.completed_acknowledgements()[0][&tip0]
            .acknowledge_error()
            .expect("an error")
            .clone();
        assert!(matches!(error, Error::InvalidRecordState(_)), "{error:?}");
        assert_eq!(INVALID_RESPONSE, error.message());
    }

    #[tokio::test(flavor = "current_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testCloseShouldBeIdempotent"
    )]
    async fn test_close_should_be_idempotent() {
        let mut f = Fixture::new();

        f.manager.close();
        f.manager.close();
        f.manager.close();

        assert_eq!(1, f.manager.close_internal_calls);
    }

    #[tokio::test(flavor = "current_thread")]
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testFetchError")]
    async fn test_fetch_error() {
        let mut f = Fixture::new();

        f.assign_from_subscribed(&[f.tp0.clone()]);
        f.send_fetch_and_verify_response(
            f.records.clone(),
            f.empty_acquired_records.clone(),
            Errors::NotLeaderOrFollower,
            Errors::None,
        )
        .await;

        let partition_records = f.fetch_records().await;
        assert!(!partition_records.contains_key(&f.tp0));
    }

    #[tokio::test(flavor = "current_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testPiggybackAcknowledgementsOnInitialShareSessionError"
    )]
    async fn test_piggyback_acknowledgements_on_initial_share_session_error() {
        let mut f = Fixture::new();
        f.manager.set_acknowledgement_commit_callback_registered(true);

        f.assign_from_subscribed(&[f.tp0.clone()]);

        let acknowledgements = get_acknowledgements(
            1,
            &[
                AcknowledgeType::Accept,
                AcknowledgeType::Accept,
                AcknowledgeType::Reject,
            ],
        );

        f.manager.fetch(node_acks(&f.tip0, 0, &acknowledgements));

        let poll_result = f.send_fetches_return_poll_result();
        assert_eq!(1, poll_result.len());
        let builder = poll_result[0].share_fetch();
        assert_eq!(1, builder.topics.len());
        // We should not add the acknowledgements as part of the request.
        assert_eq!(0, fetch_partition(builder, f.tip0.topic_id(), 0).acknowledgement_batches.len());

        let tip0 = f.tip0.clone();
        assert_eq!(3, f.completed_acknowledgements()[0][&tip0].size());
        assert_acknowledge_error(&f.completed_acknowledgements()[0][&tip0], Errors::NetworkError);
    }

    #[tokio::test(flavor = "current_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testPiggybackAcknowledgementsOnInitialShareSessionErrorTopicRemovedFromMetadata"
    )]
    async fn test_piggyback_acknowledgements_on_initial_share_session_error_topic_removed_from_metadata() {
        let mut f = Fixture::new();
        f.manager.set_acknowledgement_commit_callback_registered(true);

        f.assign_from_subscribed(&[f.tp0.clone()]);
        f.send_fetch_and_verify_response(f.records.clone(), f.acquired_records.clone(), Errors::None, Errors::None)
            .await;

        f.fetch_records().await;

        // Simulate a broker restart, but no leader change, this resets share session epoch to 0.
        assert_eq!(1, f.send_fetches());
        assert!(!f.manager.has_completed_fetches());
        let tip0 = f.tip0.clone();
        f.prepare_fetch_response(fetch_response_with_top_level_error(&tip0, Errors::ShareSessionNotFound));
        f.network_poll().await;

        // Simulate a metadata update with no topics in the response.
        f.update_metadata(&Fixture::metadata_update_with_ids(1, &[], VALID_LEADER_EPOCH, &HashMap::new()));

        // The acknowledgements for the initial fetch from tip0 are processed now and sent to the background thread.
        let acknowledgements = get_acknowledgements(
            1,
            &[
                AcknowledgeType::Accept,
                AcknowledgeType::Accept,
                AcknowledgeType::Reject,
            ],
        );
        f.manager.fetch(node_acks(&f.tip0, 0, &acknowledgements));

        assert_eq!(0, f.completed_acknowledgements().len());

        // Next fetch would not include any acknowledgements, but it will include tip-0 because it's a new share session.
        let poll_result = f.send_fetches_return_poll_result();
        assert_eq!(1, poll_result.len());
        assert_eq!(1, poll_result[0].share_fetch().topics.len());

        // We should fail any waiting acknowledgements for tip-0 as it would have a share session epoch equal to 0.
        assert_eq!(3, f.completed_acknowledgements()[0][&tip0].size());
        assert_acknowledge_error(&f.completed_acknowledgements()[0][&tip0], Errors::NetworkError);
    }

    #[tokio::test(flavor = "current_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testPiggybackAcknowledgementsOnInitialShareSession_ShareSessionNotFound"
    )]
    async fn test_piggyback_acknowledgements_on_initial_share_session_share_session_not_found() {
        let mut f = Fixture::new();
        f.manager.set_acknowledgement_commit_callback_registered(true);

        f.assign_from_subscribed(&[f.tp0.clone()]);
        f.send_fetch_and_verify_response(f.records.clone(), f.acquired_records.clone(), Errors::None, Errors::None)
            .await;

        f.fetch_records().await;

        // The acknowledgements for the initial fetch from tip0 are processed now and sent to the background thread.
        let acknowledgements = get_acknowledgements(
            1,
            &[
                AcknowledgeType::Accept,
                AcknowledgeType::Accept,
                AcknowledgeType::Reject,
            ],
        );
        f.manager.fetch(node_acks(&f.tip0, 0, &acknowledgements));

        // We attempt to send the acknowledgements piggybacking on the fetch.
        assert_eq!(1, f.send_fetches());
        assert!(!f.manager.has_completed_fetches());

        // Simulate a broker restart, but no leader change, this resets share session epoch to 0.
        let tip0 = f.tip0.clone();
        f.prepare_fetch_response(fetch_response_with_top_level_error(&tip0, Errors::ShareSessionNotFound));
        f.network_poll().await;

        // We would complete these acknowledgements with the error code from the response.
        assert_eq!(3, f.completed_acknowledgements()[0][&tip0].size());
        assert_acknowledge_error(&f.completed_acknowledgements()[0][&tip0], Errors::NetworkError);

        // Next fetch would proceed as expected and would not include any acknowledgements.
        let poll_result = f.send_fetches_return_poll_result();
        assert_eq!(1, poll_result.len());
        let builder = poll_result[0].share_fetch();
        assert_eq!(0, fetch_partition(builder, f.topic_id, 0).acknowledgement_batches.len());
    }

    /// Java's `verify(metricsManager, times(n)).recordLatency(anyString(), anyLong())`:
    /// `recordLatency` records the `fetch-latency` sensor, whose cumulative
    /// count is `fetch-total`.
    #[tokio::test(flavor = "current_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testRecordLatencyOnFetchResponseLevelError"
    )]
    async fn test_record_latency_on_fetch_response_level_error() {
        // Latency is recorded on response-level errors (ex: SHARE_SESSION_NOT_FOUND) since the round-trip completed.
        let mut f = Fixture::new();

        f.assign_from_subscribed(&[f.tp0.clone()]);
        f.send_fetch_and_verify_response(f.records.clone(), f.acquired_records.clone(), Errors::None, Errors::None)
            .await;
        assert_eq!(1.0, f.metric_value(&f.share_fetch_metrics_registry.fetch_request_total));

        assert_eq!(1, f.send_fetches());
        let tip0 = f.tip0.clone();
        f.prepare_fetch_response(fetch_response_with_top_level_error(&tip0, Errors::ShareSessionNotFound));
        f.network_poll().await;

        assert_eq!(2.0, f.metric_value(&f.share_fetch_metrics_registry.fetch_request_total));
    }

    /// The error a collect returns for a batch that fails to decode.
    async fn assert_collect_fails_to_decode(f: &Fixture) -> Error {
        match f.collect_fetch().await {
            Ok(Err(share_fetch_error)) => share_fetch_error.into_parts().1,
            Ok(Ok(fetch)) => panic!("expected a decoding error, got {fetch:?}"),
            Err(error) => error,
        }
    }

    #[tokio::test(flavor = "current_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testInvalidDefaultRecordBatch"
    )]
    async fn test_invalid_default_record_batch() {
        let mut f = Fixture::new();

        let mut builder = MemoryRecords::builder_with_options(
            MemoryRecordsBuilderOptionsBuilder::new()
                .set_initial_capacity(1024)
                .set_magic(RecordBatch::CURRENT_MAGIC_VALUE)
                .set_compression(Compression::none().build())
                .set_timestamp_type(TimestampType::CreateTime)
                .set_base_offset(0)
                .set_log_append_time(10)
                .set_producer_id(0)
                .set_producer_epoch(0)
                .set_base_sequence(0)
                .set_is_transactional(false)
                .set_is_control_batch(false)
                .set_partition_leader_epoch(0)
                .set_write_limit(1024)
                .build()
                .expect("every mandatory parameter is set"),
        );
        builder.append_kv(10, Some(b"key"), Some(b"value"));
        let mut buffer = builder.build().buffer().to_vec();

        // Garble the CRC
        buffer[17..21].copy_from_slice(b"beef");

        f.assign_from_subscribed(&[f.tp0.clone()]);

        // normal fetch
        assert_eq!(1, f.send_fetches());
        let tip0 = f.tip0.clone();
        f.prepare_fetch_response(full_fetch_response(
            &tip0,
            Some(Bytes::from(buffer)),
            acquired_records(0, 1),
            Errors::None,
            Errors::None,
        ));
        f.network_poll().await;

        // The first call to collectFetch, throws an exception
        let error = assert_collect_fails_to_decode(&f).await;
        assert!(error.is_kafka_error(), "{error:?}");
        assert_eq!(
            "Record batch for partition test-0 at offset 0 is invalid, cause: Record is corrupt (stored crc = 1650812262, computed crc = 603621382)",
            error.message()
        );

        // The exception is cleared once thrown
        let mut fetch = f.collect_fetch_ok().await;
        assert!(fetch.is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testParseInvalidRecordBatch"
    )]
    async fn test_parse_invalid_record_batch() {
        let mut f = Fixture::new();
        let records = MemoryRecords::with_records_with_magic_initial_offset_timestamp_type(
            RecordBatch::MAGIC_VALUE_V2,
            0,
            Compression::none().build(),
            TimestampType::CreateTime,
            &[
                SimpleRecord::with_timestamp_key_value(1, Some(b"a".to_vec()), Some(b"1".to_vec())),
                SimpleRecord::with_timestamp_key_value(2, Some(b"b".to_vec()), Some(b"2".to_vec())),
                SimpleRecord::with_timestamp_key_value(3, Some(b"c".to_vec()), Some(b"3".to_vec())),
            ],
        );
        let mut buffer = records.buffer().to_vec();

        // flip some bits to fail the crc
        // Java's `buffer.putInt(32, buffer.get(32) ^ 87238423)`: the byte is sign-extended to an int.
        let flipped = i32::from(buffer[32] as i8) ^ 87238423;
        buffer[32..36].copy_from_slice(&flipped.to_be_bytes());

        f.assign_from_subscribed(&[f.tp0.clone()]);

        // normal fetch
        assert_eq!(1, f.send_fetches());
        let tip0 = f.tip0.clone();
        f.prepare_fetch_response(full_fetch_response(
            &tip0,
            Some(Bytes::from(buffer)),
            acquired_records(0, 3),
            Errors::None,
            Errors::None,
        ));
        f.network_poll().await;

        let error = assert_collect_fails_to_decode(&f).await;
        assert!(error.is_kafka_error(), "{error:?}");
        assert_eq!(
            "Record batch for partition test-0 at offset 0 is invalid, cause: Record is corrupt (stored crc = 4023521662, computed crc = 1622589528)",
            error.message()
        );
    }

    #[tokio::test(flavor = "current_thread")]
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testHeaders")]
    async fn test_headers() {
        let mut f = Fixture::new();

        let mut builder = MemoryRecords::builder_with_initial_capacity(
            1024,
            Compression::none().build(),
            TimestampType::CreateTime,
            1,
        );
        builder.append_kv(0, Some(b"key"), Some(b"value-1"));

        let headers_array = [RecordHeader::new(
            "headerKey".to_string(),
            Some(b"headerValue".to_vec()),
        )];
        builder.append(0, Some(b"key"), Some(b"value-2"), &headers_array);

        let headers_array2 = [
            RecordHeader::new("headerKey".to_string(), Some(b"headerValue".to_vec())),
            RecordHeader::new("headerKey".to_string(), Some(b"headerValue2".to_vec())),
        ];
        builder.append(0, Some(b"key"), Some(b"value-3"), &headers_array2);

        let memory_records = builder.build().into_buffer();

        f.assign_from_subscribed(&[f.tp0.clone()]);

        let tip0 = f.tip0.clone();
        f.prepare_fetch_response(full_fetch_response(
            &tip0,
            Some(memory_records),
            acquired_records(1, 3),
            Errors::None,
            Errors::None,
        ));

        assert_eq!(1, f.send_fetches());
        f.network_poll().await;
        let mut records_by_partition = f.fetch_records().await;
        let records = records_by_partition.remove(&f.tp0).expect("records for tp0");

        assert_eq!(3, records.len());

        let mut record_iterator = records.iter();

        let record = record_iterator.next().expect("a first record");
        assert!(record.headers().last_header("headerKey").is_none());

        let record = record_iterator.next().expect("a second record");
        let header = record.headers().last_header("headerKey").expect("a header");
        assert_eq!(Some(&b"headerValue"[..]), header.value());
        assert_eq!("headerKey", header.key());

        let record = record_iterator.next().expect("a third record");
        let header = record.headers().last_header("headerKey").expect("a header");
        assert_eq!(Some(&b"headerValue2"[..]), header.value());
        assert_eq!("headerKey", header.key());
    }

    #[tokio::test(flavor = "current_thread")]
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testUnauthorizedTopic")]
    async fn test_unauthorized_topic() {
        let mut f = Fixture::new();

        f.assign_from_subscribed(&[f.tp0.clone()]);

        assert_eq!(1, f.send_fetches());
        let tip0 = f.tip0.clone();
        f.prepare_fetch_response(full_fetch_response(
            &tip0,
            Some(f.records.clone()),
            f.empty_acquired_records.clone(),
            Errors::TopicAuthorizationFailed,
            Errors::None,
        ));
        f.network_poll().await;
        match f.collect_fetch().await {
            Err(Error::TopicAuthorization(error)) => {
                assert_eq!(&HashSet::from([TOPIC_NAME.to_string()]), error.unauthorized_topics());
            },
            other => panic!("collectFetch should have returned a TopicAuthorizationError, got {other:?}"),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testUnknownTopicIdError")]
    async fn test_unknown_topic_id_error() {
        let mut f = Fixture::new();
        f.assign_from_subscribed(&[f.tp0.clone()]);

        assert_eq!(1, f.send_fetches());
        let tip0 = f.tip0.clone();
        f.prepare_fetch_response(fetch_response_with_top_level_error(&tip0, Errors::UnknownTopicId));
        f.network_poll().await;
        f.assert_empty_fetch("Should not return records on fetch error").await;
        let now = f.now();
        assert_eq!(0, f.metadata.time_to_next_update(now));
    }

    /// `@MethodSource("handleFetchResponseErrorSupplier")`.
    #[tokio::test(flavor = "current_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testHandleFetchResponseError"
    )]
    async fn test_handle_fetch_response_error() {
        // handleFetchResponseErrorSupplier
        let handle_fetch_response_error_supplier = [
            (Errors::NotLeaderOrFollower, false, true),
            (Errors::UnknownTopicOrPartition, false, true),
            (Errors::UnknownTopicId, true, true),
            (Errors::InconsistentTopicId, false, true),
            (Errors::FencedLeaderEpoch, false, true),
            (Errors::UnknownLeaderEpoch, false, false),
        ];
        for (error, has_top_level_error, should_request_metadata_update) in handle_fetch_response_error_supplier {
            let mut f = Fixture::new();
            f.assign_from_subscribed(&[f.tp0.clone()]);

            assert_eq!(1, f.send_fetches(), "{error}");

            let tip0 = f.tip0.clone();
            let fetch_response = if has_top_level_error {
                fetch_response_with_top_level_error(&tip0, error)
            } else {
                full_fetch_response(
                    &tip0,
                    Some(f.records.clone()),
                    f.empty_acquired_records.clone(),
                    error,
                    Errors::None,
                )
            };

            f.prepare_fetch_response(fetch_response);
            f.network_poll().await;

            f.assert_empty_fetch("Should not return records on fetch error").await;

            let now = f.now();
            let time_to_next_update = f.metadata.time_to_next_update(now);

            if should_request_metadata_update {
                assert_eq!(0, time_to_next_update, "Should have requested metadata update for {error}");
            } else {
                assert_ne!(0, time_to_next_update, "Should not have requested metadata update for {error}");
            }
        }
    }

    #[tokio::test(flavor = "current_thread")]
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testFetchDisconnected")]
    async fn test_fetch_disconnected() {
        let mut f = Fixture::new();

        f.assign_from_subscribed(&[f.tp0.clone()]);

        assert_eq!(1, f.send_fetches());
        let tip0 = f.tip0.clone();
        let response = full_fetch_response(
            &tip0,
            Some(f.records.clone()),
            f.acquired_records.clone(),
            Errors::None,
            Errors::None,
        );
        f.client()
            .prepare_response_disconnected(ConcreteResponse::ShareFetch(response), true);
        f.network_poll().await;
        f.assert_empty_fetch("Should not return records on disconnect").await;
    }

    /// Java removes the last record with `MemoryRecords#filterTo`, which keeps
    /// the batch's last offset; Rust has no `filterTo`, so the batch is built
    /// with the three keyed records and its last offset delta set to cover the
    /// removed fourth record, the CRC recomputed (the
    /// `FetchRequestManagerTest#testUpdatePositionWithLastRecordMissingFromBatch`
    /// precedent in `fetch_request_manager.rs`).
    #[tokio::test(flavor = "current_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testFetchWithLastRecordMissingFromBatch"
    )]
    async fn test_fetch_with_last_record_missing_from_batch() {
        let mut f = Fixture::new();

        let mut builder = MemoryRecords::builder_with_initial_capacity(
            1024,
            Compression::none().build(),
            TimestampType::CreateTime,
            0,
        );
        for i in 0..3 {
            builder.append_kv(0, Some(i.to_string().as_bytes()), Some(b"v"));
        }
        let mut compacted_records = builder.build().buffer().to_vec();
        // Remove the last record to simulate compaction
        compacted_records[RecordBatch::LAST_OFFSET_DELTA_OFFSET..RecordBatch::LAST_OFFSET_DELTA_OFFSET + 4]
            .copy_from_slice(&3_i32.to_be_bytes());
        let crc = crc32c::crc32c(&compacted_records[RecordBatch::ATTRIBUTES_OFFSET..]);
        compacted_records[RecordBatch::CRC_OFFSET..RecordBatch::CRC_OFFSET + 4].copy_from_slice(&crc.to_be_bytes());

        f.assign_from_subscribed(&[f.tp0.clone()]);
        assert_eq!(1, f.send_fetches());
        let tip0 = f.tip0.clone();
        f.prepare_fetch_response(full_fetch_response(
            &tip0,
            Some(Bytes::from(compacted_records)),
            acquired_records(0, 3),
            Errors::None,
            Errors::None,
        ));
        f.network_poll().await;
        assert!(f.manager.has_completed_fetches());

        let all_fetched_records = f.fetch_records().await;
        assert!(all_fetched_records.contains_key(&f.tp0));
        let fetched_records = &all_fetched_records[&f.tp0];
        assert_eq!(3, fetched_records.len());

        for (i, record) in fetched_records.iter().enumerate() {
            assert_eq!(Some(&i.to_string().into_bytes()), record.key());
        }
    }

    #[tokio::test(flavor = "current_thread")]
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testCorruptMessageError")]
    async fn test_corrupt_message_error() {
        let mut f = Fixture::new();
        f.assign_from_subscribed(&[f.tp0.clone()]);

        assert_eq!(1, f.send_fetches());
        assert!(!f.manager.has_completed_fetches());

        // Prepare a response with the CORRUPT_MESSAGE error.
        let tip0 = f.tip0.clone();
        f.prepare_fetch_response(full_fetch_response(
            &tip0,
            Some(build_records(1, 1, 1)),
            acquired_records(1, 1),
            Errors::CorruptMessage,
            Errors::None,
        ));
        f.network_poll().await;
        assert!(f.manager.has_completed_fetches());

        // Trigger the exception.
        let error = f.collect_fetch().await.expect_err("a KafkaError");
        assert!(error.is_kafka_error(), "{error:?}");
        assert_eq!(
            "Encountered corrupt message when fetching topic-partition test-0",
            error.message()
        );
    }

    /// The two-node setup shared by the leadership tests: `topic` with tp0 on
    /// node 0 and tp1 on node 1, both assigned.
    fn two_node_fixture(callback_registered: bool) -> (Fixture, Node, Node) {
        let mut f = Fixture::new();
        if callback_registered {
            f.manager.set_acknowledgement_commit_callback_registered(true);
        }

        f.subscribe_to_share_group(&[TOPIC_NAME]);
        f.assign(&[f.tp0.clone(), f.tp1.clone()]);

        f.update_metadata(&Fixture::metadata_update_with_ids(
            2,
            &[(TOPIC_NAME, 2)],
            VALID_LEADER_EPOCH,
            &f.topic_ids,
        ));
        let node_id0 = f.node(0);
        let node_id1 = f.node(1);
        (f, node_id0, node_id1)
    }

    fn fetch_partition_with_leader(
        tip: &TopicIdPartition,
        error: Errors,
        leader_id: i32,
        leader_epoch: i32,
    ) -> FetchPartitionData {
        let mut current_leader = FetchLeaderIdAndEpoch::new();
        current_leader.set_leader_id(leader_id).set_leader_epoch(leader_epoch);
        let mut partition_data = fetch_partition_with_error(tip, error);
        partition_data.set_current_leader(current_leader);
        partition_data
    }

    /// `new PartitionData().setPartitionIndex(..).setRecords(records).setAcquiredRecords(..).setAcknowledgeErrorCode(NONE)`.
    fn fetch_partition_with_records(
        tip: &TopicIdPartition,
        records: &Bytes,
        acquired_records: Vec<AcquiredRecords>,
    ) -> FetchPartitionData {
        let mut partition_data = FetchPartitionData::new();
        partition_data
            .set_partition_index(tip.partition())
            .set_records(Some(records.clone()))
            .set_acquired_records(acquired_records)
            .set_acknowledge_error_code(Errors::None.code());
        partition_data
    }

    /// `new PartitionData().setPartitionIndex(..)`.
    fn fetch_partition_empty(tip: &TopicIdPartition) -> FetchPartitionData {
        let mut partition_data = FetchPartitionData::new();
        partition_data.set_partition_index(tip.partition());
        partition_data
    }

    /// `testWhenShareFetchResponseReturnsALeadershipChangeErrorButNoNewLeaderInformation` and
    /// `testWhenShareFetchResponseReturnsWithALeadershipChangeErrorAndNewLeaderInformation`.
    async fn share_fetch_response_returns_a_leadership_change_error(error: Errors, with_new_leader_information: bool) {
        let (mut f, node_id0, node_id1) = two_node_fixture(false);
        let tp0_leader = f.leader_for(&f.tp0);
        let tp1_leader = f.leader_for(&f.tp1);

        let starting_cluster_metadata = f.metadata.fetch();
        assert!(!f.metadata.update_requested());

        assert_eq!(2, f.send_fetches());
        assert!(!f.manager.has_completed_fetches());

        let (tip0, tip1) = (f.tip0.clone(), f.tip1.clone());
        f.prepare_fetch_response_from(
            fetch_response(
                vec![(
                    tip0.clone(),
                    partition_data_for_fetch(
                        &tip0,
                        Some(f.records.clone()),
                        acquired_records(1, 1),
                        Errors::None,
                        Errors::None,
                    ),
                )],
                &[],
            ),
            &node_id0,
        );
        if with_new_leader_information {
            f.prepare_fetch_response_from(
                fetch_response(
                    vec![(
                        tip1.clone(),
                        fetch_partition_with_leader(&tip1, error, tp0_leader.id(), VALID_LEADER_EPOCH + 1),
                    )],
                    std::slice::from_ref(&tp0_leader),
                ),
                &node_id1,
            );
        } else {
            f.prepare_fetch_response_from(
                fetch_response(vec![(tip1.clone(), fetch_partition_with_error(&tip1, error))], &[]),
                &node_id1,
            );
        }
        f.network_poll().await;
        assert!(f.manager.has_completed_fetches());

        let partition_records = f.fetch_records().await;
        assert!(partition_records.contains_key(&f.tp0));
        assert!(!partition_records.contains_key(&f.tp1));

        let fetched_records = &partition_records[&f.tp0];
        assert_eq!(1, fetched_records.len());

        let acknowledgements = single_acknowledgement(1, AcknowledgeType::Accept);
        f.manager.fetch(node_acks(&f.tip0, 0, &acknowledgements));

        if with_new_leader_information {
            // The metadata snapshot will have been updated with the new leader information
            assert_ne!(*starting_cluster_metadata, *f.metadata.fetch());

            // Validate metadata update is still requested even though the current leader was returned
            assert!(f.metadata.update_requested());
        } else {
            assert!(Arc::ptr_eq(&starting_cluster_metadata, &f.metadata.fetch()));

            // Validate metadata update is requested due to the leadership error
            assert!(f.metadata.update_requested());

            // Move the leadership of tp1 onto node 1
            let leader_nodes = vec![tp0_leader.clone(), tp1_leader];
            f.move_leadership(&f.tp1, node_id0.id(), VALID_LEADER_EPOCH + 1, &leader_nodes);

            assert_ne!(*starting_cluster_metadata, *f.metadata.fetch());
        }

        // And now the partitions are on the same leader but a fetch is still sent to the former leader to remove the partition from the share session
        assert_eq!(2, f.send_fetches());
        assert!(!f.manager.has_completed_fetches());

        f.prepare_fetch_response_from(
            fetch_response(
                vec![
                    (
                        tip0.clone(),
                        partition_data_for_fetch(
                            &tip0,
                            Some(f.records.clone()),
                            acquired_records(2, 1),
                            Errors::None,
                            Errors::None,
                        ),
                    ),
                    (
                        tip1.clone(),
                        fetch_partition_with_records(&tip1, &f.records, acquired_records(1, 1)),
                    ),
                ],
                &[],
            ),
            &node_id0,
        );
        f.prepare_fetch_response_from(
            fetch_response(vec![(tip1.clone(), fetch_partition_empty(&tip1))], &[]),
            &node_id1,
        );
        f.network_poll().await;
        assert!(f.manager.has_completed_fetches());

        let partition_records = f.fetch_records().await;
        assert!(partition_records.contains_key(&f.tp0));
        assert!(partition_records.contains_key(&f.tp1));

        assert_eq!(1, partition_records[&f.tp0].len());
        assert_eq!(1, partition_records[&f.tp1].len());
    }

    /// Test the scenario that ShareFetchResponse returns with an error indicating leadership change for the partition,
    /// but it does not contain new leader info (defined in KIP-951).
    ///
    /// `@EnumSource(names = {"FENCED_LEADER_EPOCH", "NOT_LEADER_OR_FOLLOWER"})`.
    #[tokio::test(flavor = "current_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testWhenShareFetchResponseReturnsALeadershipChangeErrorButNoNewLeaderInformation"
    )]
    async fn test_when_share_fetch_response_returns_a_leadership_change_error_but_no_new_leader_information() {
        for error in [Errors::FencedLeaderEpoch, Errors::NotLeaderOrFollower] {
            share_fetch_response_returns_a_leadership_change_error(error, false).await;
        }
    }

    /// Test the scenario that ShareFetchResponse returns with an error indicating leadership change for the partition,
    /// along with new leader info (defined in KIP-951).
    ///
    /// `@EnumSource(names = {"FENCED_LEADER_EPOCH", "NOT_LEADER_OR_FOLLOWER"})`.
    #[tokio::test(flavor = "current_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testWhenShareFetchResponseReturnsWithALeadershipChangeErrorAndNewLeaderInformation"
    )]
    async fn test_when_share_fetch_response_returns_with_a_leadership_change_error_and_new_leader_information() {
        for error in [Errors::FencedLeaderEpoch, Errors::NotLeaderOrFollower] {
            share_fetch_response_returns_a_leadership_change_error(error, true).await;
        }
    }

    /// Test the scenario that the metadata indicated a change in leadership between ShareFetch requests such
    /// as could occur when metadata is periodically updated.
    ///
    /// `@EnumSource(names = {"FENCED_LEADER_EPOCH", "NOT_LEADER_OR_FOLLOWER"})`.
    #[tokio::test(flavor = "current_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testWhenLeadershipChangeBetweenShareFetchRequests"
    )]
    async fn test_when_leadership_change_between_share_fetch_requests() {
        for error in [Errors::FencedLeaderEpoch, Errors::NotLeaderOrFollower] {
            let (mut f, node_id0, node_id1) = two_node_fixture(true);

            let starting_cluster_metadata = f.metadata.fetch();
            assert!(!f.metadata.update_requested());

            assert_eq!(2, f.send_fetches());
            assert!(!f.manager.has_completed_fetches());

            let (tip0, tip1) = (f.tip0.clone(), f.tip1.clone());
            f.prepare_fetch_response_from(
                fetch_response(
                    vec![(
                        tip0.clone(),
                        partition_data_for_fetch(
                            &tip0,
                            Some(f.records.clone()),
                            acquired_records(1, 1),
                            Errors::None,
                            Errors::None,
                        ),
                    )],
                    &[],
                ),
                &node_id0,
            );
            f.prepare_fetch_response_from(
                fetch_response(vec![(tip1.clone(), fetch_partition_with_error(&tip1, Errors::None))], &[]),
                &node_id1,
            );
            f.network_poll().await;
            assert!(f.manager.has_completed_fetches());

            let partition_records = f.fetch_records().await;
            assert!(partition_records.contains_key(&f.tp0));
            assert!(!partition_records.contains_key(&f.tp1));

            assert_eq!(1, partition_records[&f.tp0].len());

            let acknowledgements = single_acknowledgement(1, AcknowledgeType::Accept);
            f.manager.fetch(node_acks(&f.tip0, 0, &acknowledgements));

            assert!(Arc::ptr_eq(&starting_cluster_metadata, &f.metadata.fetch()));

            // Move the leadership of tp0 onto node 1
            f.move_leadership(&f.tp0, node_id1.id(), VALID_LEADER_EPOCH + 1, &[]);

            assert_ne!(*starting_cluster_metadata, *f.metadata.fetch());

            // Even though the partitions are on the same leader, records were fetched on the previous leader.
            // Since we need to remove the partition from the previous leader, we still send those acknowledgement to the previous leader
            // with the partition in the list of partitions to forget.
            assert_eq!(2, f.send_fetches());
            assert!(!f.manager.has_completed_fetches());
            assert!(f.completed_acknowledgements().is_empty());

            let mut ack_partition = fetch_partition_with_error(&tip0, Errors::None);
            ack_partition.set_acknowledge_error_code(error.code());
            f.prepare_fetch_response_from(fetch_response(vec![(tip0.clone(), ack_partition)], &[]), &node_id0);
            f.prepare_fetch_response_from(
                fetch_response(
                    vec![
                        (
                            tip0.clone(),
                            partition_data_for_fetch(
                                &tip0,
                                Some(f.records.clone()),
                                acquired_records(1, 1),
                                Errors::None,
                                Errors::None,
                            ),
                        ),
                        (
                            tip1.clone(),
                            fetch_partition_with_records(&tip1, &f.records, acquired_records(1, 1)),
                        ),
                    ],
                    &[],
                ),
                &node_id1,
            );
            f.network_poll().await;
            assert!(f.manager.has_completed_fetches());
            assert_same_acknowledgements(&[(&tip0, &acknowledgements)], &f.completed_acknowledgements()[0]);
            assert_acknowledge_error(&f.completed_acknowledgements()[0][&tip0], error);

            let partition_records = f.fetch_records().await;
            assert!(partition_records.contains_key(&f.tp0));
            assert!(partition_records.contains_key(&f.tp1));

            assert_eq!(1, partition_records[&f.tp0].len());
            assert_eq!(1, partition_records[&f.tp1].len());
        }
    }

    /// The fetch shared by the leadership-change-after-fetch tests: tp0 (1
    /// record) from node 0 and tp1 (2 records) from node 1.
    async fn fetch_from_both_nodes(f: &mut Fixture, node_id0: &Node, node_id1: &Node) {
        assert_eq!(2, f.send_fetches());
        assert!(!f.manager.has_completed_fetches());

        let (tip0, tip1) = (f.tip0.clone(), f.tip1.clone());
        f.prepare_fetch_response_from(
            fetch_response(
                vec![(
                    tip0.clone(),
                    partition_data_for_fetch(
                        &tip0,
                        Some(f.records.clone()),
                        acquired_records(1, 1),
                        Errors::None,
                        Errors::None,
                    ),
                )],
                &[],
            ),
            node_id0,
        );
        f.prepare_fetch_response_from(
            fetch_response(
                vec![(
                    tip1.clone(),
                    partition_data_for_fetch(
                        &tip1,
                        Some(f.records.clone()),
                        acquired_records(1, 2),
                        Errors::None,
                        Errors::None,
                    ),
                )],
                &[],
            ),
            node_id1,
        );
        f.network_poll().await;
        assert!(f.manager.has_completed_fetches());

        let partition_records = f.fetch_records().await;
        assert!(partition_records.contains_key(&f.tp0));
        assert!(partition_records.contains_key(&f.tp1));

        assert_eq!(1, partition_records[&f.tp0].len());
        assert_eq!(2, partition_records[&f.tp1].len());
    }

    /// Java's `completedAcknowledgements.forEach(completed::putAll)`.
    fn all_completed(f: &mut Fixture) -> HashMap<TopicIdPartition, Acknowledgements> {
        let mut completed = HashMap::new();
        for acknowledgements in f.completed_acknowledgements() {
            completed.extend(acknowledgements.clone());
        }
        completed
    }

    /// `testLeadershipChangeAfterFetchBeforeCommitAsync` and `...BeforeCommitSync`.
    async fn leadership_change_after_fetch_before_commit(commit_sync: bool) {
        let (mut f, node_id0, node_id1) = two_node_fixture(true);

        let starting_cluster_metadata = f.metadata.fetch();
        assert!(!f.metadata.update_requested());

        fetch_from_both_nodes(&mut f, &node_id0, &node_id1).await;

        let acknowledgements_tp0 = single_acknowledgement(1, AcknowledgeType::Accept);
        let acknowledgements_tp1 = get_acknowledgements(1, &[AcknowledgeType::Accept, AcknowledgeType::Accept]);

        let (tip0, tip1) = (f.tip0.clone(), f.tip1.clone());
        let commit_acks = HashMap::from([
            (tip0.clone(), NodeAcknowledgements::new(0, acknowledgements_tp0.clone())),
            (tip1.clone(), NodeAcknowledgements::new(1, acknowledgements_tp1.clone())),
        ]);

        // Move the leadership of tp0 onto node 1.
        f.move_leadership(&f.tp0, node_id1.id(), VALID_LEADER_EPOCH + 1, &[]);

        assert_ne!(*starting_cluster_metadata, *f.metadata.fetch());

        // The acknowledgements are sent to the nodes the records were fetched from.
        if commit_sync {
            let deadline = f.deadline(100);
            f.manager.commit_sync(commit_acks, deadline);
        } else {
            let deadline = f.deadline(DEFAULT_API_TIMEOUT_MS);
            f.manager.commit_async(commit_acks, deadline);
            assert!(f.completed_acknowledgements().is_empty());
        }
        assert_eq!(2, f.send_acknowledgements());

        // The former leader for tp0 rejects the acknowledgements with NOT_LEADER_OR_FOLLOWER and points to the new leader.
        f.prepare_acknowledge_response_from(
            full_acknowledge_response_with_leader(
                &tip0,
                Errors::NotLeaderOrFollower,
                node_id1.id(),
                VALID_LEADER_EPOCH + 1,
                &[node_id0.clone(), node_id1.clone()],
            ),
            &node_id0,
        );
        f.prepare_acknowledge_response_from(full_acknowledge_response(&tip1, Errors::None), &node_id1);
        f.network_poll().await;

        let completed = all_completed(&mut f);
        assert_eq!(2, completed.len());
        assert_same_acknowledgements(
            &[(&tip0, &acknowledgements_tp0)],
            &HashMap::from([(tip0.clone(), completed[&tip0].clone())]),
        );
        assert_eq!(
            Errors::NotLeaderOrFollower,
            completed[&tip0].acknowledge_error().expect("an error").error()
        );
        assert_eq!(
            acknowledgements_tp1.acknowledgements_type_map(),
            completed[&tip1].acknowledgements_type_map()
        );
        assert!(completed[&tip1].acknowledge_error().is_none());

        assert_eq!(node_id1.id(), f.manager.share_session_node_id(&tip0));
    }

    #[tokio::test(flavor = "current_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testLeadershipChangeAfterFetchBeforeCommitAsync"
    )]
    async fn test_leadership_change_after_fetch_before_commit_async() {
        leadership_change_after_fetch_before_commit(false).await;
    }

    #[tokio::test(flavor = "current_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testLeadershipChangeAfterFetchBeforeCommitSync"
    )]
    async fn test_leadership_change_after_fetch_before_commit_sync() {
        leadership_change_after_fetch_before_commit(true).await;
    }

    #[tokio::test(flavor = "current_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testLeadershipChangeAfterFetchBeforeCloseMove"
    )]
    async fn test_leadership_change_after_fetch_before_close_move() {
        let (mut f, node_id0, node_id1) = two_node_fixture(true);

        let starting_cluster_metadata = f.metadata.fetch();
        assert!(!f.metadata.update_requested());

        fetch_from_both_nodes(&mut f, &node_id0, &node_id1).await;

        let acknowledgements_tp0 = single_acknowledgement(1, AcknowledgeType::Accept);
        let acknowledgements_tp1 = get_acknowledgements(1, &[AcknowledgeType::Accept, AcknowledgeType::Accept]);

        f.manager.fetch(node_acks(&f.tip1, 1, &acknowledgements_tp1));

        // Move the leadership of tp0 onto node 1.
        f.move_leadership(&f.tp0, node_id1.id(), VALID_LEADER_EPOCH + 1, &[]);

        assert_ne!(*starting_cluster_metadata, *f.metadata.fetch());

        // The acknowledgements are sent to the nodes the records were fetched from.
        let deadline = f.deadline(100);
        f.manager
            .acknowledge_on_close(node_acks(&f.tip0, 0, &acknowledgements_tp0), deadline);
        assert!(f.completed_acknowledgements().is_empty());
        assert_eq!(2, f.send_acknowledgements());

        // The former leader for tp0 rejects the acknowledgements with NOT_LEADER_OR_FOLLOWER and points to the new leader.
        let (tip0, tip1) = (f.tip0.clone(), f.tip1.clone());
        f.prepare_acknowledge_response_from(
            full_acknowledge_response_with_leader(
                &tip0,
                Errors::NotLeaderOrFollower,
                node_id1.id(),
                VALID_LEADER_EPOCH + 1,
                &[node_id0.clone(), node_id1.clone()],
            ),
            &node_id0,
        );
        f.prepare_acknowledge_response_from(full_acknowledge_response(&tip1, Errors::None), &node_id1);
        f.network_poll().await;

        let completed = all_completed(&mut f);
        assert_eq!(2, completed.len());
        assert_eq!(
            acknowledgements_tp0.acknowledgements_type_map(),
            completed[&tip0].acknowledgements_type_map()
        );
        assert_eq!(
            Errors::NotLeaderOrFollower,
            completed[&tip0].acknowledge_error().expect("an error").error()
        );
        assert_eq!(
            acknowledgements_tp1.acknowledgements_type_map(),
            completed[&tip1].acknowledgements_type_map()
        );
        assert!(completed[&tip1].acknowledge_error().is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testLeadershipChangeAfterFetchMoveBeforeClose"
    )]
    async fn test_leadership_change_after_fetch_move_before_close() {
        let (mut f, node_id0, node_id1) = two_node_fixture(true);

        let starting_cluster_metadata = f.metadata.fetch();
        assert!(!f.metadata.update_requested());

        fetch_from_both_nodes(&mut f, &node_id0, &node_id1).await;

        let acknowledgements_tp0 = single_acknowledgement(1, AcknowledgeType::Accept);
        let acknowledgements_tp1 = get_acknowledgements(1, &[AcknowledgeType::Accept, AcknowledgeType::Accept]);

        f.manager.fetch(node_acks(&f.tip1, 1, &acknowledgements_tp1));

        // Move the leadership of tp1 onto node 0.
        f.move_leadership(&f.tp1, node_id0.id(), VALID_LEADER_EPOCH + 1, &[]);

        assert_ne!(*starting_cluster_metadata, *f.metadata.fetch());

        // The acknowledgements are sent to the nodes the records were fetched from.
        let deadline = f.deadline(100);
        f.manager
            .acknowledge_on_close(node_acks(&f.tip0, 0, &acknowledgements_tp0), deadline);
        assert!(f.completed_acknowledgements().is_empty());
        assert_eq!(2, f.send_acknowledgements());

        // The former leader for tp0 rejects the acknowledgements with NOT_LEADER_OR_FOLLOWER and points to the new leader.
        let (tip0, tip1) = (f.tip0.clone(), f.tip1.clone());
        f.prepare_acknowledge_response_from(full_acknowledge_response(&tip0, Errors::None), &node_id0);
        f.prepare_acknowledge_response_from(
            full_acknowledge_response_with_leader(
                &tip1,
                Errors::NotLeaderOrFollower,
                node_id0.id(),
                VALID_LEADER_EPOCH + 1,
                &[node_id0.clone(), node_id1.clone()],
            ),
            &node_id1,
        );
        f.network_poll().await;

        let completed = all_completed(&mut f);
        assert_eq!(2, completed.len());
        assert_eq!(
            acknowledgements_tp0.acknowledgements_type_map(),
            completed[&tip0].acknowledgements_type_map()
        );
        assert!(completed[&tip0].acknowledge_error().is_none());
        assert_eq!(
            acknowledgements_tp1.acknowledgements_type_map(),
            completed[&tip1].acknowledgements_type_map()
        );
        assert_eq!(
            Errors::NotLeaderOrFollower,
            completed[&tip1].acknowledge_error().expect("an error").error()
        );
    }

    #[tokio::test(flavor = "current_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testLeadershipChangeAfterFetchMoveBeforeCloseMove"
    )]
    async fn test_leadership_change_after_fetch_move_before_close_move() {
        let (mut f, node_id0, node_id1) = two_node_fixture(true);

        let starting_cluster_metadata = f.metadata.fetch();
        assert!(!f.metadata.update_requested());

        fetch_from_both_nodes(&mut f, &node_id0, &node_id1).await;

        let acknowledgements_tp0 = single_acknowledgement(1, AcknowledgeType::Accept);
        let acknowledgements_tp1 = get_acknowledgements(1, &[AcknowledgeType::Accept, AcknowledgeType::Accept]);

        f.manager.fetch(node_acks(&f.tip1, 1, &acknowledgements_tp1));

        // Move the leadership of tp1 onto node 0, and tp0 onto node 1.
        f.move_leadership(&f.tp1, node_id0.id(), VALID_LEADER_EPOCH + 1, &[]);
        f.move_leadership(&f.tp0, node_id1.id(), VALID_LEADER_EPOCH + 1, &[]);

        assert_ne!(*starting_cluster_metadata, *f.metadata.fetch());

        // The acknowledgements are sent to the nodes the records were fetched from.
        let deadline = f.deadline(100);
        f.manager
            .acknowledge_on_close(node_acks(&f.tip0, 0, &acknowledgements_tp0), deadline);
        assert_eq!(2, f.send_acknowledgements());

        // The former leaders reject the acknowledgements with NOT_LEADER_OR_FOLLOWER and point to the new leaders.
        let (tip0, tip1) = (f.tip0.clone(), f.tip1.clone());
        f.prepare_acknowledge_response_from(
            full_acknowledge_response_with_leader(
                &tip0,
                Errors::NotLeaderOrFollower,
                node_id1.id(),
                VALID_LEADER_EPOCH + 1,
                &[node_id0.clone(), node_id1.clone()],
            ),
            &node_id0,
        );
        f.prepare_acknowledge_response_from(
            full_acknowledge_response_with_leader(
                &tip1,
                Errors::NotLeaderOrFollower,
                node_id0.id(),
                VALID_LEADER_EPOCH + 1,
                &[node_id0.clone(), node_id1.clone()],
            ),
            &node_id1,
        );
        f.network_poll().await;

        let completed = all_completed(&mut f);
        assert_eq!(2, completed.len());
        assert_eq!(
            acknowledgements_tp0.acknowledgements_type_map(),
            completed[&tip0].acknowledgements_type_map()
        );
        assert_eq!(
            Errors::NotLeaderOrFollower,
            completed[&tip0].acknowledge_error().expect("an error").error()
        );
        assert_eq!(
            acknowledgements_tp1.acknowledgements_type_map(),
            completed[&tip1].acknowledgements_type_map()
        );
        assert_eq!(
            Errors::NotLeaderOrFollower,
            completed[&tip1].acknowledge_error().expect("an error").error()
        );
    }

    #[tokio::test(flavor = "current_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testStaleMetadataLeadershipChangeDoesNotFailAcknowledgements"
    )]
    async fn test_stale_metadata_leadership_change_does_not_fail_acknowledgements() {
        let (mut f, node_id0, node_id1) = two_node_fixture(true);

        let starting_cluster_metadata = f.metadata.fetch();
        assert!(!f.metadata.update_requested());

        fetch_from_both_nodes(&mut f, &node_id0, &node_id1).await;

        let acknowledgements_tp0 = single_acknowledgement(1, AcknowledgeType::Accept);
        let acknowledgements_tp1 = get_acknowledgements(1, &[AcknowledgeType::Accept, AcknowledgeType::Accept]);

        let (tip0, tip1) = (f.tip0.clone(), f.tip1.clone());
        let commit_acks = HashMap::from([
            (tip0.clone(), NodeAcknowledgements::new(0, acknowledgements_tp0.clone())),
            (tip1.clone(), NodeAcknowledgements::new(1, acknowledgements_tp1.clone())),
        ]);

        // The metadata reports tp0 has moved to node 1, but the cached leader used for fetching (node 0) is unchanged.
        f.move_leadership(&f.tp0, node_id1.id(), VALID_LEADER_EPOCH + 1, &[]);

        assert_ne!(*starting_cluster_metadata, *f.metadata.fetch());

        // The acknowledgements are sent to the nodes the records were fetched from, so nothing fails synchronously.
        let deadline = f.deadline(DEFAULT_API_TIMEOUT_MS);
        f.manager.commit_async(commit_acks, deadline);
        assert!(f.completed_acknowledgements().is_empty());

        // We send acknowledgements for tip0 to node0 and tip1 to node1.
        assert_eq!(2, f.send_acknowledgements());

        // Both cached leaders accept the acknowledgements despite the stale metadata for tp0.
        f.prepare_acknowledge_response_from(full_acknowledge_response(&tip0, Errors::None), &node_id0);
        f.prepare_acknowledge_response_from(full_acknowledge_response(&tip1, Errors::None), &node_id1);
        f.network_poll().await;

        let completed = all_completed(&mut f);
        assert_eq!(2, completed.len());
        assert_eq!(
            acknowledgements_tp0.acknowledgements_type_map(),
            completed[&tip0].acknowledgements_type_map()
        );
        assert!(completed[&tip0].acknowledge_error().is_none());
        assert_eq!(
            acknowledgements_tp1.acknowledgements_type_map(),
            completed[&tip1].acknowledgements_type_map()
        );
        assert!(completed[&tip1].acknowledge_error().is_none());

        // The cached leader for tp0 is unchanged - the broker success did not repoint it to the stale metadata leader.
        assert_eq!(node_id0.id(), f.manager.share_session_node_id(&tip0));
    }

    #[tokio::test(flavor = "current_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testWhenLeadershipChangedAfterDisconnected"
    )]
    async fn test_when_leadership_changed_after_disconnected() {
        let (mut f, node_id0, node_id1) = two_node_fixture(true);

        let starting_cluster_metadata = f.metadata.fetch();
        assert!(!f.metadata.update_requested());

        assert_eq!(2, f.send_fetches());
        assert!(!f.manager.has_completed_fetches());

        let (tip0, tip1) = (f.tip0.clone(), f.tip1.clone());
        let mut partition = fetch_partition_with_records(&tip0, &f.records, acquired_records(1, 1));
        partition.set_error_code(Errors::None.code());
        f.prepare_fetch_response_from(fetch_response(vec![(tip0.clone(), partition)], &[]), &node_id0);
        f.prepare_fetch_response_from(
            fetch_response(vec![(tip1.clone(), fetch_partition_with_error(&tip1, Errors::None))], &[]),
            &node_id1,
        );
        f.network_poll().await;
        assert!(f.manager.has_completed_fetches());

        let partition_records = f.fetch_records().await;
        assert!(partition_records.contains_key(&f.tp0));
        assert!(!partition_records.contains_key(&f.tp1));

        assert_eq!(1, partition_records[&f.tp0].len());

        assert!(Arc::ptr_eq(&starting_cluster_metadata, &f.metadata.fetch()));

        let acknowledgements1 = single_acknowledgement(1, AcknowledgeType::Accept);
        f.manager.fetch(node_acks(&f.tip0, 0, &acknowledgements1));

        assert_eq!(2, f.send_fetches());
        assert!(!f.manager.has_completed_fetches());

        let mut ack_partition = fetch_partition_with_error(&tip0, Errors::None);
        ack_partition.set_acknowledge_error_code(Errors::None.code());
        f.client().prepare_response_from_disconnected(
            ConcreteResponse::ShareFetch(fetch_response(vec![(tip0.clone(), ack_partition)], &[])),
            &node_id0,
            true,
        );
        f.prepare_fetch_response_from(
            fetch_response(
                vec![(
                    tip1.clone(),
                    fetch_partition_with_records(&tip1, &f.records, acquired_records(1, 1)),
                )],
                &[],
            ),
            &node_id1,
        );
        f.network_poll().await;
        assert!(f.manager.has_completed_fetches());

        // The node was disconnected, so the acknowledgements for tp0 failed
        let error = f.completed_acknowledgements()[0][&tip0]
            .acknowledge_error()
            .expect("an error")
            .clone();
        assert!(error.is_disconnect_error(), "{error:?}");
        f.clear_completed_acknowledgements();

        let partition_records = f.fetch_records().await;
        assert!(!partition_records.contains_key(&f.tp0));
        assert!(partition_records.contains_key(&f.tp1));

        assert_eq!(1, partition_records[&f.tp1].len());

        // Move the leadership of tp0 onto node 1
        f.move_leadership(&f.tp0, node_id1.id(), VALID_LEADER_EPOCH + 1, &[]);

        assert_ne!(*starting_cluster_metadata, *f.metadata.fetch());

        let acknowledgements2 = single_acknowledgement(1, AcknowledgeType::Accept);
        f.manager.fetch(node_acks(&f.tip1, 1, &acknowledgements2));

        assert_eq!(2, f.send_fetches());
        assert!(!f.manager.has_completed_fetches());

        let mut partition = fetch_partition_with_records(&tip0, &f.records, acquired_records(1, 1));
        partition.set_error_code(Errors::None.code());
        f.prepare_fetch_response_from(
            fetch_response(
                vec![(tip0.clone(), partition), (tip1.clone(), fetch_partition_empty(&tip1))],
                &[],
            ),
            &node_id1,
        );
        f.prepare_fetch_response_from(
            ShareFetchResponse::of(Errors::ShareSessionNotFound, 0, Vec::new(), &[], 0),
            &node_id0,
        );
        f.network_poll().await;
        assert!(f.manager.has_completed_fetches());

        assert!(f.completed_acknowledgements()[0][&tip1].acknowledge_error().is_none());

        let partition_records = f.fetch_records().await;
        assert!(partition_records.contains_key(&f.tp0));
        assert!(!partition_records.contains_key(&f.tp1));

        assert_eq!(1, partition_records[&f.tp0].len());
    }

    /// A metadata response from which node 0 has disappeared, tp0 led by node 1
    /// at a higher epoch.
    fn metadata_without_node0(f: &Fixture, node_id1: &Node) -> MetadataResponse {
        let tp0_metadata = PartitionMetadata {
            error: Errors::None,
            topic_partition: f.tp0.clone(),
            leader_id: Some(node_id1.id()),
            leader_epoch: Some(VALID_LEADER_EPOCH + 1),
            replica_ids: vec![node_id1.id()],
            in_sync_replica_ids: vec![node_id1.id()],
            offline_replica_ids: Vec::new(),
        };
        let topic_metadata = TopicMetadata {
            error: Errors::None,
            topic: TOPIC_NAME.to_string(),
            topic_id: f.topic_id,
            is_internal: false,
            partition_metadata: vec![tp0_metadata],
            authorized_operations: MetadataResponse::AUTHORIZED_OPERATIONS_OMITTED,
        };
        RequestTestUtils::metadata_response(
            std::slice::from_ref(node_id1),
            Some("kafka-cluster"),
            1,
            vec![topic_metadata],
        )
    }

    /// The single-partition, two-node setup of the node-disappears tests:
    /// tp0 assigned and led by node 0, the session established on node 0.
    async fn session_on_node0(callback_registered: bool) -> (Fixture, Node, Node) {
        let mut f = Fixture::new();
        if callback_registered {
            f.manager.set_acknowledgement_commit_callback_registered(true);
        }

        f.subscribe_to_share_group(&[TOPIC_NAME]);
        f.assign(&[f.tp0.clone()]);

        // Two-node cluster; tp0 is led by node 0, so the share session and the cached leader are on node 0.
        f.update_metadata(&Fixture::metadata_update_with_ids(
            2,
            &[(TOPIC_NAME, 2)],
            VALID_LEADER_EPOCH,
            &f.topic_ids,
        ));
        let node_id0 = f.node(0);
        let node_id1 = f.node(1);
        assert_eq!(node_id0, f.leader_for(&f.tp0));

        // Establish the share session and cache the leader (node 0) for tp0.
        assert_eq!(1, f.send_fetches());
        let tip0 = f.tip0.clone();
        f.prepare_fetch_response_from(
            fetch_response(
                vec![(
                    tip0.clone(),
                    partition_data_for_fetch(
                        &tip0,
                        Some(f.records.clone()),
                        f.acquired_records.clone(),
                        Errors::None,
                        Errors::None,
                    ),
                )],
                &[],
            ),
            &node_id0,
        );
        f.network_poll().await;
        (f, node_id0, node_id1)
    }

    /// When a broker is fenced, it is dropped from the `brokers` list of subsequent MetadataResponses,
    /// so `Cluster::node_by_id()` returns `None` for it, while leadership for its partitions fails over to
    /// another live broker. This test checks that once the cached leader node disappears from the metadata
    /// and leadership moves to a still-present node, the next fetch is routed to the new leader.
    #[tokio::test(flavor = "current_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testFetchRecoversWhenCachedLeaderNodeDisappearsFromMetadata"
    )]
    async fn test_fetch_recovers_when_cached_leader_node_disappears_from_metadata() {
        let (mut f, _node_id0, node_id1) = session_on_node0(false).await;
        assert!(f.manager.has_completed_fetches());
        f.fetch_records().await;

        // Node 0 is fenced: it disappears from the brokers list of the next MetadataResponse and leadership
        // for tp0 fails over to node 1 with a higher leader epoch.
        let metadata_without_node0 = metadata_without_node0(&f, &node_id1);
        f.update_metadata(&metadata_without_node0);

        // Node 0 is gone from the cluster; node 1 is now the leader for tp0.
        assert!(f.metadata.fetch().node_by_id(0).is_none());
        assert_eq!(node_id1, f.leader_for(&f.tp0));

        // The next fetch should be routed to the new leader (node 1), not silently skipped.
        let poll_result = f.send_fetches_return_poll_result();
        assert_eq!(
            1,
            poll_result.len(),
            "Expected a fetch to the new leader after the cached leader node disappeared from metadata"
        );
        assert_eq!(node_id1, poll_result[0].node);
        let builder = poll_result[0].share_fetch();
        assert_eq!(1, builder.topics.len());
        assert_eq!(f.tip0.topic_id(), builder.topics[0].topic_id);
    }

    /// When the node backing a share session disappears from the cluster metadata, the stale session handler
    /// must be removed and any acknowledgements that were queued to be piggybacked on a fetch to that node
    /// must be failed with NETWORK_EXCEPTION.
    #[tokio::test(flavor = "current_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testSessionHandlerRemovedAndPiggybackAcksFailedWhenNodeDisappears"
    )]
    async fn test_session_handler_removed_and_piggyback_acks_failed_when_node_disappears() {
        let (mut f, node_id0, node_id1) = session_on_node0(true).await;
        assert!(f.manager.has_completed_fetches());
        f.fetch_records().await;
        assert!(f.manager.session_handler(node_id0.id()).is_some());

        // Queue acknowledgements to be piggybacked on the next fetch to node 0.
        let acknowledgements = get_acknowledgements(
            1,
            &[
                AcknowledgeType::Accept,
                AcknowledgeType::Accept,
                AcknowledgeType::Reject,
            ],
        );
        f.manager.fetch(node_acks(&f.tip0, 0, &acknowledgements));

        // Node 0 is fenced: it disappears from the metadata and leadership for tp0 fails over to node 1.
        let metadata_without_node0 = metadata_without_node0(&f, &node_id1);
        f.update_metadata(&metadata_without_node0);
        assert!(f.metadata.fetch().node_by_id(0).is_none());

        // The next poll re-routes the fetch to node 1, removes the stale session handler for node 0, and fails
        // the piggyback acknowledgements that could no longer be sent to node 0.
        assert_eq!(1, f.send_fetches());
        assert!(
            f.manager.session_handler(node_id0.id()).is_none(),
            "Session handler for the disappeared node should have been removed"
        );
        assert!(f.manager.session_handler(node_id1.id()).is_some());

        let tip0 = f.tip0.clone();
        assert_eq!(1, f.completed_acknowledgements().len());
        assert_same_acknowledgements(&[(&tip0, &acknowledgements)], &f.completed_acknowledgements()[0]);
        assert_acknowledge_error(&f.completed_acknowledgements()[0][&tip0], Errors::NetworkError);
    }

    /// An acknowledge request state is created for the node that led the partition at the time of the commit.
    /// If that node then disappears from the cluster metadata before the request is sent, the acknowledgements must be failed with
    /// NOT_LEADER_OR_FOLLOWER.
    #[tokio::test(flavor = "current_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testAcknowledgeRequestFailedWhenNodeDisappearsBeforeSend"
    )]
    async fn test_acknowledge_request_failed_when_node_disappears_before_send() {
        let (mut f, _node_id0, node_id1) = session_on_node0(true).await;
        f.fetch_records().await;

        // commitSync enqueues an acknowledge request state for node 0, which is still the leader at this point.
        let acknowledgements = get_acknowledgements(
            1,
            &[
                AcknowledgeType::Accept,
                AcknowledgeType::Accept,
                AcknowledgeType::Reject,
            ],
        );
        let deadline = f.deadline(DEFAULT_API_TIMEOUT_MS);
        let future = f.manager.commit_sync(node_acks(&f.tip0, 0, &acknowledgements), deadline);
        assert!(!future.is_done());

        // Node 0 is fenced: it disappears from the metadata and leadership for tp0 fails over to node 1.
        let metadata_without_node0 = metadata_without_node0(&f, &node_id1);
        f.update_metadata(&metadata_without_node0);
        assert!(f.metadata.fetch().node_by_id(0).is_none());

        // No request is sent to the vanished node, and the commit completes with NOT_LEADER_OR_FOLLOWER
        // rather than hanging.
        assert_eq!(0, f.send_acknowledgements());
        assert!(future.is_done());
        let tip0 = f.tip0.clone();
        assert_eq!(1, f.completed_acknowledgements().len());
        assert_same_acknowledgements(&[(&tip0, &acknowledgements)], &f.completed_acknowledgements()[0]);
        assert_acknowledge_error(&f.completed_acknowledgements()[0][&tip0], Errors::NotLeaderOrFollower);
    }

    /// The topic recreated with `recreated_topic_id`: `client.updateMetadata(metadataUpdateWithIds(1,
    /// Map.of(topicName, 2), tp -> validLeaderEpoch, Map.of(topicName, recreatedTopicId), false))`.
    fn recreate_topic(f: &Fixture, recreated_topic_id: Uuid) {
        f.update_metadata(&Fixture::metadata_update_with_ids(
            1,
            &[(TOPIC_NAME, 2)],
            VALID_LEADER_EPOCH,
            &HashMap::from([(TOPIC_NAME.to_string(), recreated_topic_id)]),
        ));
    }

    /// The next ShareFetch fetches the recreated topic ID and forgets the stale one from the share session.
    fn assert_fetches_recreated_topic(f: &mut Fixture, recreated_topic_id: Uuid) {
        let poll_result = f.send_fetches_return_poll_result();
        assert_eq!(1, poll_result.len());
        let builder = poll_result[0].share_fetch();

        // The recreated topic ID is fetched.
        assert_eq!(1, builder.topics.len());
        assert_eq!(recreated_topic_id, builder.topics[0].topic_id);

        // The stale topic ID is forgotten.
        assert_eq!(1, builder.forgotten_topics_data.len());
        assert_eq!(f.topic_id, builder.forgotten_topics_data[0].topic_id);
        assert_eq!(1, builder.forgotten_topics_data[0].partitions.len());
        assert_eq!(0, builder.forgotten_topics_data[0].partitions[0]);
    }

    /// When a topic is deleted and recreated, its topic ID changes. The request manager caches the mapping
    /// from topic-partition to topic ID (and leader). A partition-level UNKNOWN_TOPIC_ID error in a ShareFetch
    /// response must clear that cached mapping so that, once the metadata reflects the recreated topic, the stale
    /// topic ID is forgotten from the share session and the new one is fetched.
    #[tokio::test(flavor = "current_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testFetchResponseWithUnknownTopicIdRefreshesTopicIdOnRecreation"
    )]
    async fn test_fetch_response_with_unknown_topic_id_refreshes_topic_id_on_recreation() {
        let mut f = Fixture::new();

        f.assign_from_subscribed(&[f.tp0.clone()]);

        // Establish the share session and cache the topic ID and leader for tp0.
        f.send_fetch_and_verify_response(f.records.clone(), f.acquired_records.clone(), Errors::None, Errors::None)
            .await;
        f.fetch_records().await;

        // A subsequent fetch returns a partition-level UNKNOWN_TOPIC_ID error, which causes the request
        // manager to forget the cached topic ID and leader for the partition.
        assert_eq!(1, f.send_fetches());
        let tip0 = f.tip0.clone();
        f.prepare_fetch_response(full_fetch_response(
            &tip0,
            Some(f.records.clone()),
            f.empty_acquired_records.clone(),
            Errors::UnknownTopicId,
            Errors::None,
        ));
        f.network_poll().await;
        assert!(f.manager.has_completed_fetches());
        f.fetch_records().await;

        // The topic is recreated with a new topic ID.
        let recreated_topic_id = Uuid::random_uuid();
        recreate_topic(&f, recreated_topic_id);

        assert_fetches_recreated_topic(&mut f, recreated_topic_id);
    }

    /// As `test_fetch_response_with_unknown_topic_id_refreshes_topic_id_on_recreation` but the UNKNOWN_TOPIC_ID
    /// error arrives in a ShareAcknowledge response rather than a ShareFetch response.
    #[tokio::test(flavor = "current_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testAcknowledgeResponseWithUnknownTopicIdRefreshesTopicIdOnRecreation"
    )]
    async fn test_acknowledge_response_with_unknown_topic_id_refreshes_topic_id_on_recreation() {
        let mut f = Fixture::new();
        f.manager.set_acknowledgement_commit_callback_registered(true);

        f.assign_from_subscribed(&[f.tp0.clone()]);

        // Establish the share session and cache the topic ID and leader for tp0.
        f.send_fetch_and_verify_response(f.records.clone(), f.acquired_records.clone(), Errors::None, Errors::None)
            .await;
        f.fetch_records().await;

        // Acknowledge some records and receive a partition-level UNKNOWN_TOPIC_ID error, which causes the request
        // manager to forget the cached topic ID and leader for the partition.
        let acknowledgements = get_acknowledgements(
            1,
            &[
                AcknowledgeType::Accept,
                AcknowledgeType::Accept,
                AcknowledgeType::Reject,
            ],
        );
        let deadline = f.deadline(DEFAULT_API_TIMEOUT_MS);
        f.manager.commit_async(node_acks(&f.tip0, 0, &acknowledgements), deadline);

        assert_eq!(1, f.send_acknowledgements());
        let tip0 = f.tip0.clone();
        f.prepare_acknowledge_response(full_acknowledge_response(&tip0, Errors::UnknownTopicId));
        f.network_poll().await;

        // The acknowledgements are completed with the error rather than retried.
        assert_eq!(3, f.completed_acknowledgements()[0][&tip0].size());

        // The topic is recreated with a new topic ID.
        let recreated_topic_id = Uuid::random_uuid();
        recreate_topic(&f, recreated_topic_id);

        assert_fetches_recreated_topic(&mut f, recreated_topic_id);
    }

    /// The cached leader for a partition in the share session should only be replaced when the ShareFetch response
    /// carries a newer leader epoch, following the same rules as `Metadata::update_last_seen_epoch_if_newer`. A stale
    /// leader with an older epoch must not overwrite the cached leader, so subsequent fetches continue to go to the
    /// node with the newest known leader epoch.
    #[tokio::test(flavor = "current_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testStaleLeaderEpochDoesNotDowngradeShareSessionLeader"
    )]
    async fn test_stale_leader_epoch_does_not_downgrade_share_session_leader() {
        let mut f = Fixture::new();

        f.subscribe_to_share_group(&[TOPIC_NAME]);
        f.assign(&[f.tp0.clone()]);

        // tp0's leader is node0 with a high leader epoch.
        f.update_metadata(&Fixture::metadata_update_with_ids(
            2,
            &[(TOPIC_NAME, 1)],
            VALID_LEADER_EPOCH + 5,
            &f.topic_ids,
        ));
        let node_id0 = f.node(0);
        let node_id1 = f.node(1);
        assert_eq!(node_id0, f.leader_for(&f.tp0));

        // The first fetch goes to node0 and caches the leader (node0, epoch + 5).
        assert_eq!(1, f.send_fetches());
        assert_eq!(node_id0, f.leader_for(&f.tp0));

        // node0 responds with a leadership error naming node1 as the new leader, but with an older leader epoch.
        let tip0 = f.tip0.clone();
        f.prepare_fetch_response_from(
            fetch_response(
                vec![(
                    tip0.clone(),
                    fetch_partition_with_leader(
                        &tip0,
                        Errors::NotLeaderOrFollower,
                        node_id1.id(),
                        VALID_LEADER_EPOCH + 2,
                    ),
                )],
                std::slice::from_ref(&node_id1),
            ),
            &node_id0,
        );
        f.network_poll().await;
        assert!(f.manager.has_completed_fetches());
        f.fetch_records().await;

        // The stale leader must not have replaced the cached leader, so the next fetch still goes only to node0.
        let poll_result = f.send_fetches_return_poll_result();
        assert_eq!(1, poll_result.len());
        assert_eq!(node_id0, poll_result[0].node);
    }

    #[tokio::test(flavor = "current_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testFetchOneNodeAtATimeForRecordLimitMode"
    )]
    async fn test_fetch_one_node_at_a_time_for_record_limit_mode() {
        // We will simulate two nodes, each with one partition. The first node will have more records
        let mut f = Fixture::with_acquire_mode(ShareAcquireMode::RecordLimit);

        f.subscribe_to_share_group(&[TOPIC_NAME]);
        f.assign(&[f.tp0.clone(), f.tp1.clone()]);
        // Java's `SubscriptionState#assignFromSubscribed` keeps the assignment
        // order (a `LinkedHashMap`), so tp0 is fetched first; the Rust one
        // collects into a `HashMap` (PLAN §8), so the order is pinned here.
        lock(&f.subscriptions).move_partition_to_end(&f.tp1);

        f.update_metadata(&Fixture::metadata_update_with_ids(
            2,
            &[(TOPIC_NAME, 2)],
            VALID_LEADER_EPOCH,
            &f.topic_ids,
        ));
        let node_id0 = f.node(0);
        let node_id1 = f.node(1);
        let tp0_leader = f.leader_for(&f.tp0);
        let tp1_leader = f.leader_for(&f.tp1);

        assert_eq!(node_id0, tp0_leader);
        assert_eq!(node_id1, tp1_leader);

        // The first poll sends ShareFetch to both nodes
        // - node 0 - fetching records from tp0
        // - node 1 - establishing the share session, but not fetching records
        assert_eq!(2, f.send_fetches());
        assert!(!f.manager.has_completed_fetches());

        // Prepare responses from both nodes.
        let (tip0, tip1) = (f.tip0.clone(), f.tip1.clone());
        f.prepare_fetch_response_from(
            fetch_response(vec![(tip1.clone(), fetch_partition_with_error(&tip1, Errors::None))], &[]),
            &node_id1,
        );
        f.prepare_fetch_response_from(
            fetch_response(
                vec![(
                    tip0.clone(),
                    partition_data_for_fetch(
                        &tip0,
                        Some(f.records.clone()),
                        acquired_records(1, 1),
                        Errors::None,
                        Errors::None,
                    ),
                )],
                &[],
            ),
            &node_id0,
        );
        f.network_poll().await;
        assert!(f.manager.has_completed_fetches());

        let partition_records = f.fetch_records().await;
        assert!(partition_records.contains_key(&f.tp0) && !partition_records.contains_key(&f.tp1));

        assert_eq!(1, partition_records[&f.tp0].len());

        let acknowledgements = single_acknowledgement(1, AcknowledgeType::Accept);
        f.manager.fetch(node_acks(&f.tip0, 0, &acknowledgements));

        // The second poll sends ShareFetch to both nodes
        // - node 0 - acknowledges records from tp0, but not fetching records
        // - node 1 - fetching records from tp1
        assert_eq!(2, f.send_fetches());
        assert!(!f.manager.has_completed_fetches());

        // Let's assume there are no records for tp1 to fetch.
        f.prepare_fetch_response_from(
            fetch_response(vec![(tip1.clone(), fetch_partition_with_error(&tip1, Errors::None))], &[]),
            &node_id1,
        );

        let mut ack_partition = fetch_partition_with_error(&tip0, Errors::None);
        ack_partition.set_acknowledge_error_code(Errors::None.code());
        f.prepare_fetch_response_from(fetch_response(vec![(tip0.clone(), ack_partition)], &[]), &node_id0);
        f.network_poll().await;
        assert!(f.manager.has_completed_fetches());

        f.fetch_records().await;

        // The third poll sends ShareFetch to only 1 node.
        // - node 0 - sends a share fetch to fetch records from tp0.
        // - node 1 - does not send, as we do not have any acks to send, it will wait until node0 has returned a response.
        assert_eq!(1, f.send_fetches());
        assert!(!f.manager.has_completed_fetches());
    }

    #[tokio::test(flavor = "current_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testCloseInternalClosesShareFetchMetricsManager"
    )]
    async fn test_close_internal_closes_share_fetch_metrics_manager() {
        let mut f = Fixture::new();

        // Define all sensor names that should be created and removed
        let sensor_names = [
            "fetch-throttle-time",
            "bytes-fetched",
            "records-fetched",
            "fetch-latency",
            "sent-acknowledgements",
            "failed-acknowledgements",
        ];

        // Verify that sensors exist before closing
        for sensor_name in sensor_names {
            assert!(
                f.metrics.get_sensor(sensor_name).is_some(),
                "Sensor {sensor_name} should exist before closing"
            );
        }

        // Close the request manager
        f.manager.close();

        // Verify that all sensors are removed after closing
        for sensor_name in sensor_names {
            assert!(
                f.metrics.get_sensor(sensor_name).is_none(),
                "Sensor {sensor_name} should be removed after closing"
            );
        }
    }

    #[tokio::test(flavor = "current_thread")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManagerTest#testShareFetchWithRenewAcknowledgement"
    )]
    async fn test_share_fetch_with_renew_acknowledgement() {
        let mut f = Fixture::new();

        f.assign_from_subscribed(&[f.tp0.clone()]);
        f.send_fetch_and_verify_response(f.records.clone(), f.acquired_records.clone(), Errors::None, Errors::None)
            .await;

        let acknowledgements =
            get_acknowledgements(1, &[AcknowledgeType::Renew, AcknowledgeType::Renew, AcknowledgeType::Renew]);

        // Reading records from the share fetch buffer.
        f.fetch_records().await;

        // Piggyback acknowledgements
        f.manager.fetch(node_acks(&f.tip0, 0, &acknowledgements));

        let poll_result = f.send_fetches_return_poll_result();
        assert_eq!(1, poll_result.len());
        assert!(poll_result[0].share_fetch().is_renew_ack);

        assert!(!f.manager.has_completed_fetches());

        assert_eq!(3.0, f.metric_value(&f.share_fetch_metrics_registry.acknowledgement_send_total));

        assert_eq!(0, f.renewed_records().len());

        let tip0 = f.tip0.clone();
        f.prepare_fetch_response(full_fetch_response(
            &tip0,
            empty_records(),
            Vec::new(),
            Errors::None,
            Errors::None,
        ));
        f.network_poll().await;
        assert!(f.manager.has_completed_fetches());

        assert_eq!(3, f.renewed_records().len());

        let partition_records = f.fetch_records().await;
        assert!(partition_records.is_empty());

        let acknowledgements2 = get_acknowledgements(
            1,
            &[
                AcknowledgeType::Accept,
                AcknowledgeType::Accept,
                AcknowledgeType::Accept,
            ],
        );
        f.manager.fetch(node_acks(&f.tip0, 0, &acknowledgements2));

        assert_eq!(1, f.send_fetches());
        assert!(!f.manager.has_completed_fetches());

        f.prepare_fetch_response(full_fetch_response(
            &tip0,
            Some(f.records.clone()),
            f.acquired_records.clone(),
            Errors::None,
            Errors::None,
        ));
        f.network_poll().await;
        assert!(f.manager.has_completed_fetches());

        let partition_records = f.fetch_records().await;
        assert!(partition_records.contains_key(&f.tp0));

        assert_eq!(1, f.send_fetches());
        assert!(!f.manager.has_completed_fetches());
        assert_eq!(6.0, f.metric_value(&f.share_fetch_metrics_registry.acknowledgement_send_total));
    }

    /// Rust-only: a request whose completion handler is dropped without
    /// completing it is handled as a failure with a `DisconnectError`: the
    /// piggy-backed acknowledgements fail with it and the node takes requests
    /// again. The test drops the unsent request; a network client dropping an
    /// in-flight one (`NetworkClient::close`) closes the receiver the same way.
    #[tokio::test(flavor = "current_thread")]
    async fn test_dropped_request_is_handled_as_disconnect() {
        let mut f = Fixture::new();
        f.manager.set_acknowledgement_commit_callback_registered(true);

        f.assign_from_subscribed(&[f.tp0.clone()]);
        f.send_fetch_and_verify_response(f.records.clone(), f.acquired_records.clone(), Errors::None, Errors::None)
            .await;
        f.fetch_records().await;

        let acknowledgements = single_acknowledgement(1, AcknowledgeType::Accept);
        f.manager.fetch(node_acks(&f.tip0, 0, &acknowledgements));
        let now = f.now();
        let poll_result = f.manager.poll(now);
        assert_eq!(1, poll_result.unsent_requests.len());
        // The request is dropped unsent: its completion handler goes with it.
        drop(poll_result);
        assert!(!f.manager.is_node_free(0));

        f.manager.handle_completed_requests();

        let tip0 = f.tip0.clone();
        assert_same_acknowledgements(&[(&tip0, &acknowledgements)], &f.completed_acknowledgements()[0]);
        let error = f.completed_acknowledgements()[0][&tip0]
            .acknowledge_error()
            .expect("an error")
            .clone();
        assert!(error.is_disconnect_error(), "{error:?}");
        assert!(error.message().is_empty(), "{error:?}");
        assert!(f.manager.is_node_free(0));
        assert_eq!(1, f.send_fetches());
    }

    /// The network poll completes the requests sent so far, but nothing has
    /// handled their responses yet: Java's `networkClientDelegate.poll` with the
    /// `whenComplete` callbacks not yet run.
    async fn network_poll_without_handling(f: &mut Fixture) {
        let now = f.now();
        f.network_client_delegate.poll(0, now, false).await;
    }

    /// Rust-only: `fetch` handles the responses that arrived before it, as Java
    /// does inside the network poll that precedes the `ShareFetchEvent`. A
    /// `ShareFetch` response with acquired records clears "fetch more records"
    /// and the `fetch` that follows sets it again, so the next poll fetches.
    /// Handled after `fetch`, the response would clear it and no fetch would
    /// be sent until the next `ShareFetchEvent`.
    #[tokio::test(flavor = "current_thread")]
    async fn test_fetch_handles_completed_responses_first() {
        let mut f = Fixture::new();
        f.assign_from_subscribed(&[f.tp0.clone()]);

        assert_eq!(1, f.send_fetches());
        let tip0 = f.tip0.clone();
        f.prepare_fetch_response(full_fetch_response(
            &tip0,
            Some(f.records.clone()),
            f.acquired_records.clone(),
            Errors::None,
            Errors::None,
        ));
        network_poll_without_handling(&mut f).await;
        assert!(!f.manager.has_completed_fetches());
        assert!(!f.manager.is_node_free(0));

        f.manager.fetch(HashMap::new());

        assert!(f.manager.has_completed_fetches());
        // A poll alone (no further `fetch`) sends the next ShareFetch.
        assert_eq!(1, f.send_acknowledgements());
    }

    /// Rust-only: `commit_async` handles the responses that arrived before it.
    /// The FINAL_EPOCH `ShareFetch` that closes the empty share session removes
    /// the node's session handler when its response is handled, so the commit
    /// that follows finds no session handler and fails its acknowledgements at
    /// once, as in Java. Handled after `commit_async`, the response would leave
    /// an acknowledge request state built on the closed session.
    #[tokio::test(flavor = "current_thread")]
    async fn test_commit_async_handles_completed_responses_first() {
        let mut f = Fixture::new();
        f.assign_from_subscribed(&[f.tp0.clone()]);

        // Establish the share session, then remove tip0 from it.
        f.send_fetch_and_verify_response(
            f.records.clone(),
            f.empty_acquired_records.clone(),
            Errors::None,
            Errors::None,
        )
        .await;
        f.fetch_records().await;
        f.assign(&[]);
        assert_eq!(1, f.send_fetches());
        f.prepare_fetch_response(empty_fetch_response());
        f.network_poll().await;

        // The next poll closes the now-empty session; its response arrives but is not handled yet.
        let close_result = f.send_fetches_return_poll_result();
        assert_eq!(
            ShareRequestMetadata::FINAL_EPOCH,
            close_result[0].share_fetch().share_session_epoch
        );
        f.prepare_fetch_response(empty_fetch_response());
        network_poll_without_handling(&mut f).await;
        assert!(f.manager.session_handler(0).is_some());

        let acknowledgements = single_acknowledgement(1, AcknowledgeType::Accept);
        let deadline = f.deadline(DEFAULT_API_TIMEOUT_MS);
        f.manager.commit_async(node_acks(&f.tip0, 0, &acknowledgements), deadline);

        assert!(f.manager.session_handler(0).is_none());
        assert!(f.request_states(0).is_none());
        let tip0 = f.tip0.clone();
        assert_same_acknowledgements(&[(&tip0, &acknowledgements)], &f.completed_acknowledgements()[0]);
        assert_acknowledge_error(&f.completed_acknowledgements()[0][&tip0], Errors::NetworkError);
    }

    /// Rust-only: `set_acknowledgement_commit_callback_registered` handles the
    /// responses that arrived before it under the old setting, as Java does in
    /// the network poll that precedes the registration event: piggy-backed
    /// acknowledgements completed while no callback was registered emit no
    /// event.
    #[tokio::test(flavor = "current_thread")]
    async fn test_callback_registration_handles_completed_responses_first() {
        let mut f = Fixture::new();
        f.assign_from_subscribed(&[f.tp0.clone()]);
        f.send_fetch_and_verify_response(f.records.clone(), f.acquired_records.clone(), Errors::None, Errors::None)
            .await;
        f.fetch_records().await;

        let acknowledgements = single_acknowledgement(1, AcknowledgeType::Accept);
        f.manager.fetch(node_acks(&f.tip0, 0, &acknowledgements));
        assert_eq!(1, f.send_fetches());
        let tip0 = f.tip0.clone();
        f.prepare_fetch_response(full_fetch_response(
            &tip0,
            Some(f.records.clone()),
            f.acquired_records.clone(),
            Errors::None,
            Errors::None,
        ));
        network_poll_without_handling(&mut f).await;

        f.manager.set_acknowledgement_commit_callback_registered(true);

        assert!(f.manager.has_completed_fetches());
        assert!(f.completed_acknowledgements().is_empty());
    }

    /// Rust-only: Java has no test of a second `acknowledgeOnClose` while the
    /// first close request is in flight. Its acknowledgements fail with
    /// `NOT_LEADER_OR_FOLLOWER` and the close future fails with Java's message.
    #[tokio::test(flavor = "current_thread")]
    async fn test_acknowledge_on_close_with_existing_close_request() {
        let mut f = Fixture::new();
        f.manager.set_acknowledgement_commit_callback_registered(true);

        f.assign_from_subscribed(&[f.tp0.clone()]);
        f.send_fetch_and_verify_response(f.records.clone(), f.acquired_records.clone(), Errors::None, Errors::None)
            .await;

        let acknowledgements = single_acknowledgement(1, AcknowledgeType::Accept);
        let deadline = f.deadline(100);
        let close_future = f
            .manager
            .acknowledge_on_close(node_acks(&f.tip0, 0, &acknowledgements), deadline);
        assert_eq!(1, f.send_acknowledgements());

        let acknowledgements2 = single_acknowledgement(2, AcknowledgeType::Accept);
        let deadline = f.deadline(100);
        f.manager
            .acknowledge_on_close(node_acks(&f.tip0, 0, &acknowledgements2), deadline);

        let tip0 = f.tip0.clone();
        assert_same_acknowledgements(&[(&tip0, &acknowledgements2)], &f.completed_acknowledgements()[0]);
        assert_acknowledge_error(&f.completed_acknowledgements()[0][&tip0], Errors::NotLeaderOrFollower);
        let error = close_future
            .future()
            .result_if_done()
            .expect("the close future is done")
            .expect_err("the close future failed");
        assert!(error.is_local_illegal_state_error(), "{error:?}");
        assert_eq!(
            "Attempt to call close() when there is an existing close request for node 0",
            error.message()
        );
    }

    /// Rust-only: the share membership manager reaches the manager through
    /// `Arc<dyn MemberStateListener>`; the member id it reports is the one the
    /// share sessions are created for, and a malformed one that follows it is
    /// ignored rather than clearing it.
    #[tokio::test(flavor = "current_thread")]
    async fn test_member_state_listener_sets_the_member_id() {
        let f = Fixture::with_member_id(None, ShareAcquireMode::BatchOptimized);
        f.assign_from_subscribed(std::slice::from_ref(&f.tp0));
        let metrics_manager = Arc::new(ShareFetchMetricsManager::new(
            Arc::new(Metrics::new()),
            &ShareFetchMetricsRegistry::new(),
        ));
        let manager = Arc::new(Mutex::new(ShareConsumeRequestManager::new(
            Arc::clone(&f.time) as Arc<dyn Time>,
            GROUP_ID,
            Arc::clone(&f.metadata),
            Arc::clone(&f.subscriptions),
            f.manager.share_fetch_config.clone(),
            Arc::new(ShareFetchBuffer::new()),
            Arc::new(ShareAcknowledgementEventHandler::new()),
            metrics_manager,
            RETRY_BACKOFF_MS,
            1000,
        )));
        let listener: Arc<dyn MemberStateListener> = Arc::clone(&manager) as Arc<dyn MemberStateListener>;

        let member_id = Uuid::random_uuid();
        listener.on_member_epoch_updated(Some(1), &member_id.to_string());
        // A malformed member id is ignored: the valid one stays.
        listener.on_member_epoch_updated(Some(2), "not-a-uuid");
        lock(&manager).fetch(HashMap::new());
        let now = f.now();
        let mut poll_result = RequestManager::poll(&mut *lock(&manager), now);
        assert_eq!(1, poll_result.unsent_requests.len());
        let request = SentRequest::of(&mut poll_result.unsent_requests[0]);
        assert_eq!(Some(member_id.to_string()), request.share_fetch().member_id);
    }

    /// Rust-only: Java's `AcknowledgeRequestType` ids and lower-case names.
    #[test]
    fn test_acknowledge_request_type_id_and_display() {
        assert_eq!(0, AcknowledgeRequestType::CommitAsync.id());
        assert_eq!(1, AcknowledgeRequestType::CommitSync.id());
        assert_eq!(2, AcknowledgeRequestType::Close.id());
        assert_eq!("commit_async", AcknowledgeRequestType::CommitAsync.to_string());
        assert_eq!("commit_sync", AcknowledgeRequestType::CommitSync.to_string());
        assert_eq!("close", AcknowledgeRequestType::Close.to_string());
    }
}
