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

//! `ShareConsumeRequestManager` — generates `ShareFetch` and
//! `ShareAcknowledge` requests to fetch and acknowledge records being
//! delivered for a consumer in a share group (KIP-932).
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager`.
//!
//! # Response-handler wiring
//!
//! Java registers a `whenComplete((clientResponse, error) -> ...)` lambda on
//! each [`UnsentRequest`] that mutates the manager (and the per-node
//! `AcknowledgeRequestState`) directly on the network thread. The Rust
//! `RequestManager::poll` takes `&mut self`, so the completion callback cannot
//! hold a second mutable borrow of the manager. Instead the manager exposes
//! the completion handlers ([`Self::handle_share_fetch_success`],
//! [`Self::handle_share_acknowledge_success`], …) as `&mut self` methods; the
//! bg task (Phase 6/7) awaits each request's response receiver and dispatches
//! into them serially, exactly as `FetchRequestManager` routes
//! `PendingFetchCompletion`s back through its next `poll`. The per-node
//! in-flight [`AcknowledgeRequestState`] a response belongs to is identified
//! by node id plus the [`InFlightAckSlot`] recorded when the request was
//! built (only one acknowledge request per node is in flight at a time).
//!
//! # Metrics
//!
//! `ShareFetchMetricsManager` / `ShareFetchMetricsAggregator` recording is
//! deferred to KIP-714 (`// metrics: deferred to KIP-714`); the surrounding
//! logic is preserved.

#![allow(dead_code)]

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use indexmap::IndexMap;
use log::{debug, error, trace};
use tokio::sync::oneshot;

use crate::common::protocol::Errors;
use crate::common::requests::{ShareAcknowledgeResponse, ShareFetchResponse};
use crate::common::utils::LogContext;
use crate::common::{KafkaError, Node, TopicIdPartition, TopicPartition, Uuid};
use crate::consumer::internals::acknowledgements::Acknowledgements;
use crate::consumer::internals::events::completable_event::CompletableEventHandle;
use crate::consumer::internals::events::share_acknowledgement_event::ShareAcknowledgementEvent;
use crate::consumer::internals::events::share_acknowledgement_event_handler::ShareAcknowledgementEventHandler;
use crate::consumer::internals::network_client_delegate::{PollResult, UnsentRequest};
use crate::consumer::internals::node_acknowledgements::NodeAcknowledgements;
use crate::consumer::internals::request_manager::RequestManager;
use crate::consumer::internals::share_acquire_mode::ShareAcquireMode;
use crate::consumer::internals::share_completed_fetch::ShareCompletedFetch;
use crate::consumer::internals::share_consumer_metadata::ShareConsumerMetadata;
use crate::consumer::internals::share_fetch_buffer::ShareFetchBuffer;
use crate::consumer::internals::share_fetch_config::ShareFetchConfig;
use crate::consumer::internals::share_session_handler::ShareSessionHandler;
use crate::consumer::internals::subscription_state::SubscriptionState;
use crate::consumer::internals::timed_request_state::TimedRequestState;
use crate::metadata::LeaderIdAndEpoch;
use crate::share_acknowledge_request_data::ShareAcknowledgeRequestData;
use crate::share_fetch_request_data::ShareFetchRequestData;

const INVALID_RESPONSE: &str = "Acknowledgement not successful due to invalid response from broker";

/// Time source used by the manager for building retry timers. Mirrors Java's
/// `Time` interface (only `milliseconds()` is used here). Same shape as the
/// existing `FetchCollectorTime` / `ThreadTime` traits.
pub(crate) trait ShareConsumeTime: Send + Sync + 'static {
    fn milliseconds(&self) -> i64;
}

/// Default time source — wraps `std::time::SystemTime::now()`.
#[derive(Debug, Default)]
pub(crate) struct SystemShareConsumeTime;

impl ShareConsumeTime for SystemShareConsumeTime {
    fn milliseconds(&self) -> i64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0)
    }
}

/// The result payload the `commitSync` future completes with.
pub(crate) type AcknowledgeResult = IndexMap<TopicIdPartition, Acknowledgements>;

/// Shared, poll-able completion future (`CompletableFuture<T>` equivalent).
/// Cloneable (`Arc`) so the manager and a [`ResultHandler`] can share it.
pub(crate) type ShareFuture<T> = Arc<CompletableEventHandle<T>>;

/// Indicates whether the acknowledgements came from a `commitAsync`,
/// `commitSync`, or close operation.
///
/// Corresponds to Java's `ShareConsumeRequestManager.AcknowledgeRequestType`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AcknowledgeRequestType {
    CommitAsync,
    CommitSync,
    Close,
}

impl AcknowledgeRequestType {
    /// The wire id byte for this request type.
    pub(crate) fn id(&self) -> u8 {
        match self {
            Self::CommitAsync => 0,
            Self::CommitSync => 1,
            Self::Close => 2,
        }
    }
}

impl std::fmt::Display for AcknowledgeRequestType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::CommitAsync => "commit_async",
            Self::CommitSync => "commit_sync",
            Self::Close => "close",
        };
        f.write_str(s)
    }
}

/// Hash key mirroring Java's `ShareConsumeRequestManager.IdAndPartition`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct IdAndPartition {
    topic_id: Uuid,
    partition_index: i32,
}

impl IdAndPartition {
    fn new(topic_id: Uuid, partition_index: i32) -> Self {
        Self { topic_id, partition_index }
    }
}

/// Sends a [`ShareAcknowledgementEvent`] to the application when the network
/// task is done processing acknowledgements. Free function so it can be
/// invoked from both the manager and a [`ResultHandler`] without a back
/// reference.
///
/// Corresponds to Java's `maybeSendShareAcknowledgementEvent`.
fn maybe_send_share_acknowledgement_event(
    is_ack_callback_registered: &AtomicBool,
    event_handler: &ShareAcknowledgementEventHandler,
    acknowledgements_map: IndexMap<TopicIdPartition, Acknowledgements>,
    check_for_renew_acknowledgements: bool,
    acquisition_lock_timeout_ms: Option<i32>,
) {
    if is_ack_callback_registered.load(Ordering::SeqCst) || check_for_renew_acknowledgements {
        let event = ShareAcknowledgementEvent::new(
            acknowledgements_map,
            check_for_renew_acknowledgements,
            acquisition_lock_timeout_ms,
        );
        event_handler.add(event);
    }
}

/// Convert an [`Errors`] code to the optional exception used to complete an
/// [`Acknowledgements`]. `Errors::None` maps to `None` (Java's
/// `Errors.NONE.exception()` returns `null`).
fn errors_to_exception(error: Errors) -> Option<KafkaError> {
    if error == Errors::None {
        None
    } else {
        Some(KafkaError::new(error))
    }
}

/// Like [`errors_to_exception`] but carries a broker-supplied message when
/// present (Java's `Errors.forCode(code).exception(message)`).
fn errors_to_exception_with_message(code: i16, message: Option<String>) -> Option<KafkaError> {
    let error = Errors::for_code(code);
    if error == Errors::None {
        None
    } else if let Some(msg) = message {
        Some(KafkaError::with_message(error, msg))
    } else {
        Some(KafkaError::new(error))
    }
}

/// Handles completing a future when all results are known. Also manages
/// completing the `commitSync` future by counting down results.
///
/// Corresponds to Java's `ShareConsumeRequestManager.ResultHandler`.
pub(crate) struct ResultHandler {
    result: std::sync::Mutex<IndexMap<TopicIdPartition, Acknowledgements>>,
    remaining_results: Option<std::sync::atomic::AtomicI32>,
    future: Option<ShareFuture<AcknowledgeResult>>,
    is_ack_callback_registered: Arc<AtomicBool>,
    event_handler: ShareAcknowledgementEventHandler,
}

impl ResultHandler {
    fn new(
        remaining_results: Option<i32>,
        future: Option<ShareFuture<AcknowledgeResult>>,
        is_ack_callback_registered: Arc<AtomicBool>,
        event_handler: ShareAcknowledgementEventHandler,
    ) -> Self {
        Self {
            result: std::sync::Mutex::new(IndexMap::new()),
            remaining_results: remaining_results.map(std::sync::atomic::AtomicI32::new),
            future,
            is_ack_callback_registered,
            event_handler,
        }
    }

    /// Increments the pending-result counter. Java increments the shared
    /// `AtomicInteger resultCount` directly from the manager; here the counter
    /// lives inside the (shared) `ResultHandler`.
    fn increment_remaining(&self) {
        if let Some(remaining) = &self.remaining_results {
            remaining.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// Handle the result of a `ShareAcknowledge` request sent to one or more
    /// nodes and signal the completion when all results are known.
    ///
    /// Corresponds to Java's `ResultHandler.complete`.
    pub(crate) fn complete(
        &self,
        partition: TopicIdPartition,
        acknowledgements: Option<Acknowledgements>,
        request_type: AcknowledgeRequestType,
        check_for_renew_acknowledgements: bool,
        acquisition_lock_timeout_ms: Option<i32>,
    ) {
        if request_type == AcknowledgeRequestType::CommitAsync {
            if let Some(acks) = acknowledgements {
                let mut map = IndexMap::new();
                map.insert(partition, acks);
                maybe_send_share_acknowledgement_event(
                    &self.is_ack_callback_registered,
                    &self.event_handler,
                    map,
                    check_for_renew_acknowledgements,
                    acquisition_lock_timeout_ms,
                );
            }
        } else {
            if let Some(acks) = acknowledgements {
                self.result.lock().unwrap_or_else(|e| e.into_inner()).insert(partition, acks);
            }
            if let Some(remaining) = &self.remaining_results
                && remaining.fetch_sub(1, Ordering::SeqCst) - 1 == 0
            {
                let result = std::mem::take(&mut *self.result.lock().unwrap_or_else(|e| e.into_inner()));
                maybe_send_share_acknowledgement_event(
                    &self.is_ack_callback_registered,
                    &self.event_handler,
                    result.clone(),
                    check_for_renew_acknowledgements,
                    acquisition_lock_timeout_ms,
                );
                if let Some(future) = &self.future {
                    future.complete(result);
                }
            }
        }
    }

    /// Handles the case where there are no results pending after
    /// initialization.
    ///
    /// Corresponds to Java's `ResultHandler.completeIfEmpty`.
    pub(crate) fn complete_if_empty(&self) {
        if let Some(remaining) = &self.remaining_results
            && remaining.load(Ordering::SeqCst) == 0
            && let Some(future) = &self.future
        {
            let result = std::mem::take(&mut *self.result.lock().unwrap_or_else(|e| e.into_inner()));
            future.complete(result);
        }
    }

    /// Test-only: whether the associated future has completed.
    #[cfg(test)]
    pub(crate) fn is_future_done(&self) -> bool {
        self.future.as_ref().is_some_and(|f| f.is_done())
    }
}

/// Represents a request to acknowledge delivery that can be retried or
/// aborted.
///
/// Corresponds to Java's inner class
/// `ShareConsumeRequestManager.AcknowledgeRequestState`.
pub(crate) struct AcknowledgeRequestState {
    request_state: TimedRequestState,
    /// The node to send the request to.
    node_id: i32,
    /// The map of acknowledgements to send.
    acknowledgements_to_send: IndexMap<TopicIdPartition, Acknowledgements>,
    /// The map of acknowledgements to be retried in the next attempt.
    incomplete_acknowledgements: IndexMap<TopicIdPartition, Acknowledgements>,
    /// The in-flight acknowledgements.
    in_flight_acknowledgements: IndexMap<TopicIdPartition, Acknowledgements>,
    /// Handles completing a future when all results are known.
    result_handler: Arc<ResultHandler>,
    /// Indicates whether this was part of commitAsync, commitSync or close.
    request_type: AcknowledgeRequestType,
    /// Whether the request has been processed (response received, no retry).
    is_processed: bool,
    /// Timeout in ms indicating how long the request would be retried.
    timeout_ms: i64,
}

impl AcknowledgeRequestState {
    #[allow(clippy::too_many_arguments)]
    fn new(
        owner: &str,
        now_ms: i64,
        deadline_ms: i64,
        retry_backoff_ms: i64,
        retry_backoff_max_ms: i64,
        node_id: i32,
        acknowledgements_map: IndexMap<TopicIdPartition, Acknowledgements>,
        result_handler: Arc<ResultHandler>,
        request_type: AcknowledgeRequestType,
    ) -> Self {
        let request_state = TimedRequestState::new(
            owner,
            retry_backoff_ms,
            retry_backoff_max_ms,
            TimedRequestState::deadline_for(now_ms, deadline_ms),
        );
        let timeout_ms = request_state.remaining_ms(now_ms);
        Self {
            request_state,
            node_id,
            acknowledgements_to_send: acknowledgements_map,
            incomplete_acknowledgements: IndexMap::new(),
            in_flight_acknowledgements: IndexMap::new(),
            result_handler,
            request_type,
            is_processed: false,
            timeout_ms,
        }
    }

    pub(crate) fn get_in_flight_acknowledgements_count(&self, tip: &TopicIdPartition) -> usize {
        self.in_flight_acknowledgements.get(tip).map_or(0, Acknowledgements::size)
    }

    pub(crate) fn get_incomplete_acknowledgements_count(&self, tip: &TopicIdPartition) -> usize {
        self.incomplete_acknowledgements.get(tip).map_or(0, Acknowledgements::size)
    }

    pub(crate) fn get_acknowledgements_to_send_count(&self, tip: &TopicIdPartition) -> usize {
        self.acknowledgements_to_send.get(tip).map_or(0, Acknowledgements::size)
    }

    /// Corresponds to Java's `isEmpty()`.
    pub(crate) fn is_empty(&self) -> bool {
        self.acknowledgements_to_send.is_empty()
            && self.incomplete_acknowledgements.is_empty()
            && self.in_flight_acknowledgements.is_empty()
    }

    pub(crate) fn is_close_request(&self) -> bool {
        self.request_type == AcknowledgeRequestType::Close
    }

    pub(crate) fn is_processed(&self) -> bool {
        self.is_processed
    }

    /// Corresponds to Java's `maybeExpire()`.
    fn maybe_expire(&self, now_ms: i64) -> bool {
        self.request_state.num_attempts() > 0 && self.request_state.is_expired(now_ms)
    }

    /// Corresponds to Java's `canSendRequest()`.
    fn can_send_request(&self, now_ms: i64) -> bool {
        self.request_state.can_send_request(now_ms)
    }

    fn on_send_attempt(&mut self, now_ms: i64) {
        self.request_state.on_send_attempt(now_ms);
    }

    fn on_failed_attempt(&mut self, now_ms: i64) {
        self.request_state.on_failed_attempt(now_ms);
    }

    fn on_successful_attempt(&mut self, now_ms: i64) {
        self.request_state.on_successful_attempt(now_ms);
    }

    /// Resets the timer with the configured timeout and resets the request
    /// state. Only applicable to `commitAsync` requests, which can be reused.
    ///
    /// Corresponds to Java's `maybeResetTimerAndRequestState()`.
    fn maybe_reset_timer_and_request_state(&mut self, now_ms: i64) {
        if self.request_type == AcknowledgeRequestType::CommitAsync {
            self.request_state.reset_deadline(now_ms.saturating_add(self.timeout_ms));
            self.request_state.reset();
        }
    }

    /// Sets the error code in the acknowledgements and sends the response
    /// through a background event.
    ///
    /// Corresponds to Java's `handleAcknowledgeErrorCode()`.
    fn handle_acknowledge_error_code(
        &mut self,
        tip: TopicIdPartition,
        acknowledge_error_code: Errors,
        check_for_renew_acknowledgements: bool,
        acquisition_lock_timeout_ms: Option<i32>,
    ) {
        if let Some(mut acks) = self.in_flight_acknowledgements.shift_remove(&tip) {
            acks.complete(errors_to_exception(acknowledge_error_code));
            self.result_handler.complete(
                tip,
                Some(acks),
                self.request_type,
                check_for_renew_acknowledgements,
                acquisition_lock_timeout_ms,
            );
        } else {
            error!("Invalid partition {tip} received in ShareAcknowledge response");
        }
    }

    /// Sets the error code for acknowledgements which timed out after retries.
    ///
    /// Corresponds to Java's `handleAcknowledgeTimedOut()`.
    fn handle_acknowledge_timed_out(&mut self, tip: &TopicIdPartition) {
        if let Some(acks) = self.incomplete_acknowledgements.get(tip) {
            let mut acks = acks.clone();
            acks.complete(errors_to_exception(Errors::RequestTimedOut));
            self.result_handler
                .complete(tip.clone(), Some(acks), self.request_type, true, None);
        }
    }

    /// Set the error code for all remaining acknowledgements when a share
    /// session-not-found error prevents them from being sent.
    ///
    /// Corresponds to Java's `handleAcknowledgeShareSessionNotFound()`.
    fn handle_acknowledge_share_session_not_found(&mut self) {
        let use_incomplete = !self.incomplete_acknowledgements.is_empty();
        let map = if use_incomplete {
            std::mem::take(&mut self.incomplete_acknowledgements)
        } else {
            std::mem::take(&mut self.acknowledgements_to_send)
        };
        for (tip, mut acks) in map {
            acks.complete(errors_to_exception(Errors::ShareSessionNotFound));
            self.result_handler.complete(tip, Some(acks), self.request_type, true, None);
        }
        self.processing_complete();
    }

    /// Corresponds to Java's `processingComplete()`.
    fn processing_complete(&mut self) {
        self.process_pending_in_flight_acknowledgements(KafkaError::with_message(
            Errors::InvalidRecordState,
            INVALID_RESPONSE,
        ));
        self.result_handler.complete_if_empty();
        self.is_processed = true;
        // Only reset the timer for reusable commitAsync states.
        self.maybe_reset_timer_and_request_state(0);
    }

    /// Fail any existing in-flight acknowledgements with the given exception
    /// and clear the map, also sending a background event.
    ///
    /// Corresponds to Java's `processPendingInFlightAcknowledgements()`.
    fn process_pending_in_flight_acknowledgements(&mut self, exception: KafkaError) {
        if !self.in_flight_acknowledgements.is_empty() {
            let in_flight = std::mem::take(&mut self.in_flight_acknowledgements);
            for (partition, mut acks) in in_flight {
                acks.complete(Some(exception.clone()));
                self.result_handler
                    .complete(partition, Some(acks), self.request_type, true, None);
            }
        }
    }

    /// Moves all in-flight acknowledgements to incomplete acknowledgements to
    /// retry in the next request.
    ///
    /// Corresponds to Java's `moveAllToIncompleteAcks()`.
    fn move_all_to_incomplete_acks(&mut self) {
        for (tip, acks) in std::mem::take(&mut self.in_flight_acknowledgements) {
            self.incomplete_acknowledgements.insert(tip, acks);
        }
    }

    /// Moves the in-flight acknowledgements for a partition to incomplete
    /// acknowledgements to retry. Returns `true` if the partition was sent.
    ///
    /// Corresponds to Java's `moveToIncompleteAcks()`.
    fn move_to_incomplete_acks(&mut self, tip: &TopicIdPartition) -> bool {
        if let Some(acks) = self.in_flight_acknowledgements.shift_remove(tip) {
            self.incomplete_acknowledgements.insert(tip.clone(), acks);
            true
        } else {
            error!("Invalid partition {tip} received in ShareAcknowledge response");
            false
        }
    }
}

/// Holds the async, sync-queue, and close acknowledge request states for a
/// node.
///
/// Corresponds to Java's static inner class
/// `ShareConsumeRequestManager.Tuple`.
pub(crate) struct Tuple {
    async_request: Option<AcknowledgeRequestState>,
    sync_request_queue: Option<VecDeque<AcknowledgeRequestState>>,
    close_request: Option<AcknowledgeRequestState>,
}

impl Tuple {
    fn new(
        async_request: Option<AcknowledgeRequestState>,
        sync_request_queue: Option<VecDeque<AcknowledgeRequestState>>,
        close_request: Option<AcknowledgeRequestState>,
    ) -> Self {
        Self { async_request, sync_request_queue, close_request }
    }

    fn set_async_request(&mut self, async_request: Option<AcknowledgeRequestState>) {
        self.async_request = async_request;
    }

    fn nullify_sync_request_queue(&mut self) {
        self.sync_request_queue = None;
    }

    fn add_sync_request(&mut self, sync_request: AcknowledgeRequestState) {
        self.sync_request_queue
            .get_or_insert_with(VecDeque::new)
            .push_back(sync_request);
    }

    fn set_close_request(&mut self, close_request: Option<AcknowledgeRequestState>) {
        self.close_request = close_request;
    }

    pub(crate) fn get_async_request(&self) -> Option<&AcknowledgeRequestState> {
        self.async_request.as_ref()
    }

    pub(crate) fn get_sync_request_queue(&self) -> Option<&VecDeque<AcknowledgeRequestState>> {
        self.sync_request_queue.as_ref()
    }

    pub(crate) fn get_close_request(&self) -> Option<&AcknowledgeRequestState> {
        self.close_request.as_ref()
    }
}

/// `ShareConsumeRequestManager` — responsible for generating `ShareFetch` and
/// `ShareAcknowledge` requests.
pub(crate) struct ShareConsumeRequestManager {
    time: Arc<dyn ShareConsumeTime>,
    log_context: LogContext,
    group_id: String,
    metadata: Arc<ShareConsumerMetadata>,
    subscriptions: Arc<std::sync::Mutex<SubscriptionState>>,
    share_fetch_config: ShareFetchConfig,
    share_fetch_buffer: Arc<ShareFetchBuffer>,
    acknowledge_event_handler: ShareAcknowledgementEventHandler,
    // metrics: deferred to KIP-714 (ShareFetchMetricsManager omitted)
    session_handlers: HashMap<i32, ShareSessionHandler>,
    nodes_with_pending_requests: HashSet<i32>,
    member_id: Option<Uuid>,
    fetch_more_records: bool,
    fetch_records_node_id: i32,
    fetch_acknowledgements_to_send: HashMap<i32, IndexMap<TopicIdPartition, Acknowledgements>>,
    fetch_acknowledgements_in_flight: HashMap<i32, IndexMap<TopicIdPartition, Acknowledgements>>,
    acknowledge_request_states: HashMap<i32, Tuple>,
    retry_backoff_ms: i64,
    retry_backoff_max_ms: i64,
    closing: bool,
    close_future: ShareFuture<()>,
    close_future_rx: Option<oneshot::Receiver<Result<(), KafkaError>>>,
    is_acknowledgement_commit_callback_registered: Arc<AtomicBool>,
    topic_names_map: HashMap<IdAndPartition, String>,
    closed: bool,
}

impl ShareConsumeRequestManager {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        time: Arc<dyn ShareConsumeTime>,
        log_context: LogContext,
        group_id: String,
        metadata: Arc<ShareConsumerMetadata>,
        subscriptions: Arc<std::sync::Mutex<SubscriptionState>>,
        share_fetch_config: ShareFetchConfig,
        share_fetch_buffer: Arc<ShareFetchBuffer>,
        acknowledge_event_handler: ShareAcknowledgementEventHandler,
        retry_backoff_ms: i64,
        retry_backoff_max_ms: i64,
    ) -> Self {
        let (close_handle, close_rx) = CompletableEventHandle::new(i64::MAX);
        Self {
            time,
            log_context,
            group_id,
            metadata,
            subscriptions,
            share_fetch_config,
            share_fetch_buffer,
            acknowledge_event_handler,
            session_handlers: HashMap::new(),
            nodes_with_pending_requests: HashSet::new(),
            member_id: None,
            fetch_more_records: false,
            fetch_records_node_id: -1,
            fetch_acknowledgements_to_send: HashMap::new(),
            fetch_acknowledgements_in_flight: HashMap::new(),
            acknowledge_request_states: HashMap::new(),
            retry_backoff_ms,
            retry_backoff_max_ms,
            closing: false,
            close_future: Arc::new(close_handle),
            close_future_rx: Some(close_rx),
            is_acknowledgement_commit_callback_registered: Arc::new(AtomicBool::new(false)),
            topic_names_map: HashMap::new(),
            closed: false,
        }
    }

    fn is_share_acquire_mode_record_limit(&self) -> bool {
        self.share_fetch_config.share_acquire_mode == ShareAcquireMode::RecordLimit
    }

    fn is_node_free(&self, node_id: i32) -> bool {
        !self.nodes_with_pending_requests.contains(&node_id)
    }

    /// Corresponds to Java's `setAcknowledgementCommitCallbackRegistered`.
    pub(crate) fn set_acknowledgement_commit_callback_registered(&self, registered: bool) {
        self.is_acknowledgement_commit_callback_registered
            .store(registered, Ordering::SeqCst);
    }

    /// Corresponds to Java's `onMemberEpochUpdated` (MemberStateListener). The
    /// listener registration wiring lands with the share consumer bg loop
    /// (Phase 6); this inherent method lets the manager be seeded directly.
    pub(crate) fn on_member_epoch_updated(&mut self, _member_epoch: Option<i32>, member_id: &str) {
        self.member_id = Uuid::from_string(member_id).ok();
    }

    /// Corresponds to Java's `sessionHandler(int)`.
    pub(crate) fn session_handler(&self, node: i32) -> Option<&ShareSessionHandler> {
        self.session_handlers.get(&node)
    }

    /// Corresponds to Java's `hasCompletedFetches()`.
    pub(crate) fn has_completed_fetches(&self) -> bool {
        !self.share_fetch_buffer.is_empty()
    }

    /// Corresponds to Java's `requestStates(int)`.
    pub(crate) fn request_states(&self, node_id: i32) -> Option<&Tuple> {
        self.acknowledge_request_states.get(&node_id)
    }

    /// Test/Phase-6 accessor: the shared `close` future.
    pub(crate) fn close_future(&self) -> ShareFuture<()> {
        Arc::clone(&self.close_future)
    }

    /// Corresponds to Java's `partitionsToFetch()`.
    fn partitions_to_fetch(&self) -> Vec<TopicPartition> {
        let subs = self.subscriptions.lock().unwrap_or_else(|e| e.into_inner());
        subs.fetchable_partitions(|_| true)
    }

    /// Corresponds to Java's `closeInternal()`.
    pub(crate) fn close_internal(&mut self) {
        self.share_fetch_buffer.close();
        // metrics: deferred to KIP-714 (metricsManager close omitted)
    }

    /// Corresponds to Java's `close()` (idempotent via `IdempotentCloser`).
    pub(crate) fn close(&mut self) {
        if self.closed {
            return;
        }
        self.closed = true;
        self.close_internal();
    }

    /// Corresponds to Java's `fetch(Map<TopicIdPartition, NodeAcknowledgements>)`.
    pub(crate) fn fetch(&mut self, acknowledgements_map: IndexMap<TopicIdPartition, NodeAcknowledgements>) {
        if !self.fetch_more_records {
            debug!("Fetch more data");
            self.fetch_more_records = true;
        }
        // Store the acknowledgements and send them in the next ShareFetch.
        self.process_acknowledgements_map(acknowledgements_map);
    }

    /// Corresponds to Java's `processAcknowledgementsMap`.
    fn process_acknowledgements_map(&mut self, acknowledgements_map: IndexMap<TopicIdPartition, NodeAcknowledgements>) {
        for (tip, node_acks) in acknowledgements_map {
            let node_id = node_acks.node_id();
            let acks = node_acks.into_acknowledgements();
            let node_map = self.fetch_acknowledgements_to_send.entry(node_id).or_default();
            if let Some(current) = node_map.get_mut(&tip) {
                current.merge(&acks);
            } else {
                node_map.insert(tip, acks);
            }
        }
    }

    /// Corresponds to Java's `maybeAddAcknowledgements`. Returns `true` if the
    /// acknowledgements can be added to the share session; otherwise completes
    /// them with `INVALID_SHARE_SESSION_EPOCH` and enqueues an event.
    #[allow(clippy::too_many_arguments)]
    fn maybe_add_acknowledgements(
        is_new_session: bool,
        node_id: i32,
        tip: &TopicIdPartition,
        acknowledgements: Acknowledgements,
        fetch_acknowledgements_in_flight: &mut HashMap<i32, IndexMap<TopicIdPartition, Acknowledgements>>,
        is_ack_callback_registered: &AtomicBool,
        event_handler: &ShareAcknowledgementEventHandler,
    ) -> bool {
        if is_new_session {
            let mut acks = acknowledgements;
            debug!("Cannot send acknowledgements on initial epoch for ShareSession for partition {tip}");
            acks.complete(errors_to_exception(Errors::InvalidShareSessionEpoch));
            let mut map = IndexMap::new();
            map.insert(tip.clone(), acks);
            maybe_send_share_acknowledgement_event(is_ack_callback_registered, event_handler, map, true, None);
            false
        } else {
            // metrics: deferred to KIP-714 (recordAcknowledgementSent)
            fetch_acknowledgements_in_flight
                .entry(node_id)
                .or_default()
                .insert(tip.clone(), acknowledgements);
            true
        }
    }

    /// Corresponds to Java's `isLeaderKnownToHaveChanged`.
    fn is_leader_known_to_have_changed(
        metadata: &ShareConsumerMetadata,
        node_id: i32,
        topic_id_partition: &TopicIdPartition,
    ) -> bool {
        match metadata.current_leader(topic_id_partition.topic_partition()).leader {
            Some(leader) => {
                if leader.id() != node_id {
                    debug!(
                        "Node {node_id} is no longer the leader for partition {topic_id_partition}, failing acknowledgements"
                    );
                    true
                } else {
                    false
                }
            },
            None => {
                debug!("No leader found for partition {topic_id_partition}");
                metadata.request_update(false);
                false
            },
        }
    }

    /// Corresponds to Java's `lookupTopicId`.
    fn lookup_topic_id(
        metadata: &ShareConsumerMetadata,
        topic_names_map: &mut HashMap<IdAndPartition, String>,
        topic_id: Uuid,
        partition_index: i32,
    ) -> Option<TopicIdPartition> {
        let names = metadata.topic_names();
        let mut topic_name = names.get(&topic_id).cloned();
        if topic_name.is_none() {
            topic_name = topic_names_map.remove(&IdAndPartition::new(topic_id, partition_index));
        }
        match topic_name {
            Some(name) => Some(TopicIdPartition::from_parts(topic_id, partition_index, name)),
            None => {
                error!("Topic name not found in metadata for topicId {topic_id} and partitionIndex {partition_index}");
                None
            },
        }
    }
}

/// Identifies the in-flight acknowledge request slot for a node. Only one
/// acknowledge request per node is in flight at a time (guarded by
/// `nodes_with_pending_requests`), so a single slot per node is unambiguous.
#[derive(Clone, Copy)]
enum AckSlot {
    Async,
    Sync(usize),
    Close,
}

impl ShareConsumeRequestManager {
    /// Finds the in-flight acknowledge request slot for the node's [`Tuple`].
    /// An async / sync request in flight always has non-empty in-flight
    /// acknowledgements; a close request may be in flight with none, so it is
    /// matched by `!is_processed`.
    fn find_in_flight_ack_slot(tuple: &Tuple) -> Option<AckSlot> {
        if tuple
            .async_request
            .as_ref()
            .is_some_and(|s| !s.in_flight_acknowledgements.is_empty())
        {
            return Some(AckSlot::Async);
        }
        if let Some(queue) = tuple.sync_request_queue.as_ref() {
            for (i, s) in queue.iter().enumerate() {
                if !s.in_flight_acknowledgements.is_empty() {
                    return Some(AckSlot::Sync(i));
                }
            }
        }
        if tuple.close_request.as_ref().is_some_and(|s| !s.is_processed) {
            return Some(AckSlot::Close);
        }
        None
    }

    fn ack_slot_state_mut(tuple: &mut Tuple, slot: AckSlot) -> Option<&mut AcknowledgeRequestState> {
        match slot {
            AckSlot::Async => tuple.async_request.as_mut(),
            AckSlot::Sync(i) => tuple.sync_request_queue.as_mut().and_then(|q| q.get_mut(i)),
            AckSlot::Close => tuple.close_request.as_mut(),
        }
    }

    /// Corresponds to Java's `poll(long currentTimeMs)`.
    pub(crate) fn poll(&mut self, current_time_ms: i64) -> PollResult {
        if self.member_id.is_none() {
            if self.closing && !self.close_future.is_done() {
                self.close_future.complete(());
            }
            return PollResult::empty();
        }

        // Send any pending acknowledgements before fetching more records.
        if let Some(poll_result) = self.process_acknowledgements(current_time_ms) {
            return poll_result;
        }

        if !self.fetch_more_records {
            return PollResult::empty();
        }

        self.poll_fetch()
    }

    /// The fetch-building half of `poll`, mirroring the second part of Java's
    /// `poll`.
    fn poll_fetch(&mut self) -> PollResult {
        let member_id = self.member_id.expect("member id set");
        let topic_ids = self.metadata.topic_ids();
        let partitions = self.partitions_to_fetch();
        let cluster = self.metadata.fetch();

        let mut handler_nodes: IndexMap<i32, Node> = IndexMap::new();

        let Self {
            metadata,
            session_handlers,
            nodes_with_pending_requests,
            fetch_acknowledgements_to_send,
            fetch_acknowledgements_in_flight,
            is_acknowledgement_commit_callback_registered,
            acknowledge_event_handler,
            topic_names_map,
            log_context,
            fetch_records_node_id,
            share_fetch_config,
            subscriptions,
            group_id,
            ..
        } = self;

        for partition in &partitions {
            let leader = metadata.current_leader(partition).leader;
            let Some(node) = leader else {
                debug!("Requesting metadata update for partition {partition} since current leader node is missing");
                metadata.request_update(false);
                continue;
            };
            let Some(&topic_id) = topic_ids.get(partition.topic()) else {
                debug!("Requesting metadata update for partition {partition} since topic ID is missing");
                metadata.request_update(false);
                continue;
            };
            if nodes_with_pending_requests.contains(&node.id()) {
                trace!(
                    "Skipping fetch for partition {partition} because previous fetch request to {} has not been processed",
                    node.id()
                );
                continue;
            }

            session_handlers
                .entry(node.id())
                .or_insert_with(|| ShareSessionHandler::new(log_context, node.id(), member_id));
            handler_nodes.insert(node.id(), node.clone());

            let tip = TopicIdPartition::new(topic_id, partition.clone());
            let mut acks_to_send = fetch_acknowledgements_to_send
                .get_mut(&node.id())
                .and_then(|m| m.shift_remove(&tip));
            let mut can_send_acknowledgements = true;

            if let Some(acks) = &acks_to_send {
                let is_new = session_handlers
                    .get(&node.id())
                    .is_some_and(ShareSessionHandler::is_new_session);
                if !Self::maybe_add_acknowledgements(
                    is_new,
                    node.id(),
                    &tip,
                    acks.clone(),
                    fetch_acknowledgements_in_flight,
                    is_acknowledgement_commit_callback_registered,
                    acknowledge_event_handler,
                ) {
                    can_send_acknowledgements = false;
                }
            }

            let handler = session_handlers.get_mut(&node.id()).expect("session handler present");
            if can_send_acknowledgements {
                handler.add_partition_to_fetch(tip.clone(), acks_to_send.take());
            } else {
                handler.add_partition_to_fetch(tip.clone(), None);
            }
            topic_names_map
                .entry(IdAndPartition::new(topic_id, partition.partition()))
                .or_insert_with(|| partition.topic().to_string());

            if share_fetch_config.share_acquire_mode == ShareAcquireMode::RecordLimit && *fetch_records_node_id == -1 {
                *fetch_records_node_id = node.id();
                subscriptions
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .move_partition_to_end(partition);
            }

            debug!("Added fetch request for partition {tip} to node {}", node.id());
        }

        // Iterate over the session handlers to send acknowledgements for
        // partitions which are no longer part of the current subscription. We
        // fail acknowledgements for records fetched from a previous leader.
        let session_node_ids: Vec<i32> = session_handlers.keys().copied().collect();
        for node_id in session_node_ids {
            let Some(node) = cluster.node_by_id(node_id).cloned() else {
                continue;
            };
            if nodes_with_pending_requests.contains(&node_id) {
                trace!("Skipping fetch because previous fetch request to {node_id} has not been processed");
                continue;
            }
            let Some(node_acks_map) = fetch_acknowledgements_to_send.remove(&node_id) else {
                continue;
            };
            for (tip, acks) in node_acks_map {
                if !Self::is_leader_known_to_have_changed(metadata, node_id, &tip) {
                    let is_new = session_handlers.get(&node_id).is_some_and(ShareSessionHandler::is_new_session);
                    if !Self::maybe_add_acknowledgements(
                        is_new,
                        node_id,
                        &tip,
                        acks.clone(),
                        fetch_acknowledgements_in_flight,
                        is_acknowledgement_commit_callback_registered,
                        acknowledge_event_handler,
                    ) {
                        continue;
                    }
                    session_handlers
                        .get_mut(&node_id)
                        .expect("session handler present")
                        .add_partition_to_acknowledge_only(tip.clone(), acks);
                    handler_nodes.insert(node_id, node.clone());
                    topic_names_map
                        .entry(IdAndPartition::new(tip.topic_id(), tip.partition()))
                        .or_insert_with(|| tip.topic().to_string());
                    debug!("Added fetch request for previously subscribed partition {tip} to node {node_id}");
                } else {
                    let mut acks = acks;
                    debug!(
                        "Leader for the partition is down or has changed, failing acknowledgements for partition {tip}"
                    );
                    acks.complete(errors_to_exception(Errors::NotLeaderOrFollower));
                    let mut map = IndexMap::new();
                    map.insert(tip.clone(), acks);
                    maybe_send_share_acknowledgement_event(
                        is_acknowledgement_commit_callback_registered,
                        acknowledge_event_handler,
                        map,
                        true,
                        None,
                    );
                }
            }
        }

        // Build the list of UnsentRequests from the touched session handlers.
        let mut requests: Vec<UnsentRequest> = Vec::new();
        for (node_id, node) in handler_nodes {
            // For record_limit mode, only send a full ShareFetch to a single
            // node at a time; other nodes can skip an empty request.
            let can_skip_if_request_empty = share_fetch_config.share_acquire_mode == ShareAcquireMode::RecordLimit
                && node_id != *fetch_records_node_id;
            let Some(handler) = session_handlers.get_mut(&node_id) else {
                continue;
            };
            let builder =
                handler.new_share_fetch_builder(group_id.as_str(), share_fetch_config, can_skip_if_request_empty);
            let Some(builder) = builder else {
                trace!("Skipping ShareFetch request to send to node {node_id}");
                continue;
            };
            trace!("Building ShareFetch request to send to node {node_id}");
            nodes_with_pending_requests.insert(node_id);
            requests.push(UnsentRequest::new(Box::new(builder), Some(node)));
        }

        PollResult::with_requests(requests)
    }

    /// Corresponds to Java's `processAcknowledgements`.
    fn process_acknowledgements(&mut self, current_time_ms: i64) -> Option<PollResult> {
        let mut unsent_requests: Vec<UnsentRequest> = Vec::new();

        let Self {
            acknowledge_request_states,
            session_handlers,
            nodes_with_pending_requests,
            metadata,
            group_id,
            share_fetch_config,
            closing,
            close_future,
            ..
        } = self;

        let node_ids: Vec<i32> = acknowledge_request_states.keys().copied().collect();
        for node_id in node_ids {
            if nodes_with_pending_requests.contains(&node_id) {
                trace!(
                    "Skipping acknowledge request because previous request to {node_id} has not been processed, so acks are not sent"
                );
                continue;
            }

            let mut ctx = AckBuildCtx {
                session_handlers: &mut *session_handlers,
                nodes_with_pending_requests: &mut *nodes_with_pending_requests,
                metadata: &*metadata,
                group_id: group_id.as_str(),
                share_fetch_config: &*share_fetch_config,
            };

            // First, the acknowledgements from commitAsync are sent.
            let is_async_sent;
            {
                let tuple = acknowledge_request_states.get_mut(&node_id).expect("tuple present");
                let (req, sent) =
                    Self::maybe_build_request(tuple.async_request.as_mut(), current_time_ms, true, node_id, &mut ctx);
                is_async_sent = sent;
                if let Some(r) = req {
                    unsent_requests.push(r);
                }
            }

            if is_async_sent {
                if ctx.nodes_with_pending_requests.contains(&node_id) {
                    trace!(
                        "Skipping acknowledge request because previous request to {node_id} has not been processed, so acks are not sent"
                    );
                    continue;
                }
                let has_sync_queue = acknowledge_request_states
                    .get(&node_id)
                    .and_then(Tuple::get_sync_request_queue)
                    .is_some();
                if !has_sync_queue {
                    let tuple = acknowledge_request_states.get_mut(&node_id).expect("tuple present");
                    let (req, _) = Self::maybe_build_request(
                        tuple.close_request.as_mut(),
                        current_time_ms,
                        false,
                        node_id,
                        &mut ctx,
                    );
                    if let Some(r) = req {
                        unsent_requests.push(r);
                    }
                } else {
                    let queue_len = acknowledge_request_states
                        .get(&node_id)
                        .and_then(Tuple::get_sync_request_queue)
                        .map_or(0, VecDeque::len);
                    for i in 0..queue_len {
                        if ctx.nodes_with_pending_requests.contains(&node_id) {
                            trace!(
                                "Skipping acknowledge request because previous request to {node_id} has not been processed, so acks are not sent"
                            );
                            break;
                        }
                        let tuple = acknowledge_request_states.get_mut(&node_id).expect("tuple present");
                        let state = tuple.sync_request_queue.as_mut().and_then(|q| q.get_mut(i));
                        let (req, _) = Self::maybe_build_request(state, current_time_ms, false, node_id, &mut ctx);
                        if let Some(r) = req {
                            unsent_requests.push(r);
                        }
                    }
                }
            }
        }

        if !unsent_requests.is_empty() {
            Some(PollResult::with_requests(unsent_requests))
        } else if Self::check_and_remove_completed_acknowledgements(acknowledge_request_states) {
            // Return empty result until all the acknowledgement request states are processed.
            Some(PollResult::empty())
        } else if *closing {
            if !close_future.is_done() {
                close_future.complete(());
            }
            Some(PollResult::empty())
        } else {
            None
        }
    }

    /// Corresponds to Java's `maybeBuildRequest`. Returns the built request (if
    /// any) and whether the async request was considered "sent" (only
    /// meaningful when `on_commit_async` is `true`).
    fn maybe_build_request(
        state: Option<&mut AcknowledgeRequestState>,
        current_time_ms: i64,
        _on_commit_async: bool,
        node_id: i32,
        ctx: &mut AckBuildCtx<'_>,
    ) -> (Option<UnsentRequest>, bool) {
        let Some(state) = state else { return (None, true) };
        if (!state.is_close_request() && state.is_empty()) || (state.is_close_request() && state.is_processed) {
            return (None, true);
        }

        if state.maybe_expire(current_time_ms) {
            let tips: Vec<TopicIdPartition> = state.incomplete_acknowledgements.keys().cloned().collect();
            for tip in &tips {
                // metrics: deferred to KIP-714 (recordFailedAcknowledgements)
                state.handle_acknowledge_timed_out(tip);
            }
            state.incomplete_acknowledgements.clear();
            // Reset timer for any future processing on the same request state.
            state.maybe_reset_timer_and_request_state(current_time_ms);
            return (None, true);
        }

        if !state.can_send_request(current_time_ms) {
            // We wait for the backoff before we can send this request.
            return (None, false);
        }

        let request = Self::build_ack_request(state, node_id, ctx);
        let Some(request) = request else {
            return (None, false);
        };

        state.on_send_attempt(current_time_ms);
        (Some(request), true)
    }

    /// Corresponds to Java's `AcknowledgeRequestState.buildRequest`.
    fn build_ack_request(
        state: &mut AcknowledgeRequestState,
        node_id: i32,
        ctx: &mut AckBuildCtx<'_>,
    ) -> Option<UnsentRequest> {
        let session_handler = ctx.session_handlers.get_mut(&node_id)?;

        // If this is the closing request, close the share session by setting
        // the final epoch.
        if state.is_close_request() {
            session_handler.notify_close();
        }

        let use_incomplete = !state.incomplete_acknowledgements.is_empty();
        let final_acknowledgements: Vec<(TopicIdPartition, Acknowledgements)> = if use_incomplete {
            state
                .incomplete_acknowledgements
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        } else {
            state
                .acknowledgements_to_send
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        };

        for (tip, acks) in &final_acknowledgements {
            session_handler.add_partition_to_fetch(tip.clone(), Some(acks.clone()));
        }

        let request_builder = session_handler.new_share_acknowledge_builder(ctx.group_id, ctx.share_fetch_config);

        state.is_processed = false;
        let node_to_send = ctx.metadata.fetch().node_by_id(node_id).cloned();

        let Some(request_builder) = request_builder else {
            state.handle_acknowledge_share_session_not_found();
            return None;
        };

        let node = node_to_send?;

        ctx.nodes_with_pending_requests.insert(node_id);
        trace!("Building acknowledgements to send : {final_acknowledgements:?}");

        for (tip, acks) in final_acknowledgements {
            state.in_flight_acknowledgements.insert(tip, acks);
        }
        if use_incomplete {
            state.incomplete_acknowledgements.clear();
        } else {
            state.acknowledgements_to_send.clear();
        }

        Some(UnsentRequest::new(Box::new(request_builder), Some(node)))
    }

    /// Corresponds to Java's `checkAndRemoveCompletedAcknowledgements`.
    fn check_and_remove_completed_acknowledgements(acknowledge_request_states: &mut HashMap<i32, Tuple>) -> bool {
        let mut are_any_acks_left = false;
        acknowledge_request_states.retain(|_node_id, tuple| {
            let mut are_async_acks_left = true;
            let mut are_sync_acks_left = true;

            if !Self::is_request_state_in_progress(tuple.async_request.as_ref()) {
                tuple.set_async_request(None);
                are_async_acks_left = false;
            }

            if !Self::are_request_states_in_progress(tuple.sync_request_queue.as_ref()) {
                tuple.nullify_sync_request_queue();
                are_sync_acks_left = false;
            }

            if !Self::is_request_state_in_progress(tuple.close_request.as_ref()) {
                tuple.set_close_request(None);
            }

            if are_async_acks_left || are_sync_acks_left {
                are_any_acks_left = true;
                true
            } else {
                // Keep the entry only if a close request is still present.
                tuple.close_request.is_some()
            }
        });

        if !acknowledge_request_states.is_empty() {
            are_any_acks_left = true;
        }
        are_any_acks_left
    }

    fn is_request_state_in_progress(state: Option<&AcknowledgeRequestState>) -> bool {
        match state {
            None => false,
            Some(s) if s.is_close_request() => !s.is_processed,
            Some(s) => !s.is_empty(),
        }
    }

    fn are_request_states_in_progress(queue: Option<&VecDeque<AcknowledgeRequestState>>) -> bool {
        match queue {
            None => false,
            Some(q) => q.iter().any(|s| Self::is_request_state_in_progress(Some(s))),
        }
    }
}

/// Bundles the disjoint manager fields needed to build acknowledge requests,
/// so [`ShareConsumeRequestManager::maybe_build_request`] can borrow them
/// alongside a `&mut AcknowledgeRequestState` from a different field.
struct AckBuildCtx<'a> {
    session_handlers: &'a mut HashMap<i32, ShareSessionHandler>,
    nodes_with_pending_requests: &'a mut HashSet<i32>,
    metadata: &'a Arc<ShareConsumerMetadata>,
    group_id: &'a str,
    share_fetch_config: &'a ShareFetchConfig,
}

impl ShareConsumeRequestManager {
    /// Enqueue an [`AcknowledgeRequestState`] to be picked up on the next poll.
    /// Returns the future which completes when the acknowledgements finish.
    ///
    /// Corresponds to Java's `commitSync`.
    pub(crate) fn commit_sync(
        &mut self,
        acknowledgements_map: IndexMap<TopicIdPartition, NodeAcknowledgements>,
        deadline_ms: i64,
    ) -> oneshot::Receiver<Result<AcknowledgeResult, KafkaError>> {
        let now = self.time.milliseconds();
        let (future_handle, rx) = CompletableEventHandle::new(i64::MAX);
        let future = Arc::new(future_handle);
        let result_handler = Arc::new(ResultHandler::new(
            Some(0),
            Some(Arc::clone(&future)),
            Arc::clone(&self.is_acknowledgement_commit_callback_registered),
            self.acknowledge_event_handler.clone(),
        ));

        let cluster = self.metadata.fetch();
        let mut acks_map = acknowledgements_map;
        let session_node_ids: Vec<i32> = self.session_handlers.keys().copied().collect();

        for node_id in session_node_ids {
            let Some(node) = cluster.node_by_id(node_id).cloned() else {
                continue;
            };
            self.acknowledge_request_states
                .entry(node_id)
                .or_insert_with(|| Tuple::new(None, None, None));

            let mut acks_for_node: IndexMap<TopicIdPartition, Acknowledgements> = IndexMap::new();
            let session_partitions = self
                .session_handlers
                .get(&node_id)
                .map(ShareSessionHandler::session_partitions)
                .unwrap_or_default();

            for tip in session_partitions {
                let matches = acks_map.get(&tip).is_some_and(|na| na.node_id() == node.id());
                if !matches {
                    continue;
                }
                if !Self::is_leader_known_to_have_changed(&self.metadata, node.id(), &tip) {
                    let acks = acks_map.shift_remove(&tip).expect("present").into_acknowledgements();
                    acks_for_node.insert(tip.clone(), acks);
                    // metrics: deferred to KIP-714 (recordAcknowledgementSent)
                    debug!(
                        "Added sync acknowledge request for partition {} to node {}",
                        tip.topic_partition(),
                        node.id()
                    );
                    result_handler.increment_remaining();
                } else {
                    let mut acks = acks_map.shift_remove(&tip).expect("present").into_acknowledgements();
                    acks.complete(errors_to_exception(Errors::NotLeaderOrFollower));
                    let mut map = IndexMap::new();
                    map.insert(tip.clone(), acks);
                    maybe_send_share_acknowledgement_event(
                        &self.is_acknowledgement_commit_callback_registered,
                        &self.acknowledge_event_handler,
                        map,
                        true,
                        None,
                    );
                }
            }

            if !acks_for_node.is_empty() {
                let state = AcknowledgeRequestState::new(
                    "ShareConsumeRequestManager:1",
                    now,
                    deadline_ms,
                    self.retry_backoff_ms,
                    self.retry_backoff_max_ms,
                    node_id,
                    acks_for_node,
                    Arc::clone(&result_handler),
                    AcknowledgeRequestType::CommitSync,
                );
                self.acknowledge_request_states
                    .get_mut(&node_id)
                    .expect("present")
                    .add_sync_request(state);
            }
        }

        result_handler.complete_if_empty();
        rx
    }

    /// Enqueue an [`AcknowledgeRequestState`] to be picked up on the next poll.
    ///
    /// Corresponds to Java's `commitAsync`.
    pub(crate) fn commit_async(
        &mut self,
        acknowledgements_map: IndexMap<TopicIdPartition, NodeAcknowledgements>,
        deadline_ms: i64,
    ) {
        let now = self.time.milliseconds();
        let cluster = self.metadata.fetch();
        let result_handler = Arc::new(ResultHandler::new(
            None,
            None,
            Arc::clone(&self.is_acknowledgement_commit_callback_registered),
            self.acknowledge_event_handler.clone(),
        ));

        let mut acks_map = acknowledgements_map;
        let session_node_ids: Vec<i32> = self.session_handlers.keys().copied().collect();

        for node_id in session_node_ids {
            let Some(node) = cluster.node_by_id(node_id).cloned() else {
                continue;
            };
            self.acknowledge_request_states
                .entry(node_id)
                .or_insert_with(|| Tuple::new(None, None, None));

            let session_partitions = self
                .session_handlers
                .get(&node_id)
                .map(ShareSessionHandler::session_partitions)
                .unwrap_or_default();

            for tip in session_partitions {
                let matches = acks_map.get(&tip).is_some_and(|na| na.node_id() == node.id());
                if !matches {
                    continue;
                }
                if !Self::is_leader_known_to_have_changed(&self.metadata, node.id(), &tip) {
                    let acks = acks_map.shift_remove(&tip).expect("present").into_acknowledgements();
                    // metrics: deferred to KIP-714 (recordAcknowledgementSent)
                    debug!(
                        "Added async acknowledge request for partition {} to node {}",
                        tip.topic_partition(),
                        node.id()
                    );
                    let retry_backoff_ms = self.retry_backoff_ms;
                    let retry_backoff_max_ms = self.retry_backoff_max_ms;
                    let tuple = self.acknowledge_request_states.get_mut(&node_id).expect("present");
                    if let Some(state) = tuple.async_request.as_mut() {
                        if let Some(existing) = state.acknowledgements_to_send.get_mut(&tip) {
                            existing.merge(&acks);
                        } else {
                            state.acknowledgements_to_send.insert(tip.clone(), acks);
                        }
                    } else {
                        let mut map = IndexMap::new();
                        map.insert(tip.clone(), acks);
                        let state = AcknowledgeRequestState::new(
                            "ShareConsumeRequestManager:2",
                            now,
                            deadline_ms,
                            retry_backoff_ms,
                            retry_backoff_max_ms,
                            node_id,
                            map,
                            Arc::clone(&result_handler),
                            AcknowledgeRequestType::CommitAsync,
                        );
                        tuple.set_async_request(Some(state));
                    }
                } else {
                    let mut acks = acks_map.shift_remove(&tip).expect("present").into_acknowledgements();
                    acks.complete(errors_to_exception(Errors::NotLeaderOrFollower));
                    let mut map = IndexMap::new();
                    map.insert(tip.clone(), acks);
                    maybe_send_share_acknowledgement_event(
                        &self.is_acknowledgement_commit_callback_registered,
                        &self.acknowledge_event_handler,
                        map,
                        true,
                        None,
                    );
                }
            }
        }

        result_handler.complete_if_empty();
    }

    /// Enqueue the final [`AcknowledgeRequestState`] used to commit the final
    /// acknowledgements and close the share sessions.
    ///
    /// Corresponds to Java's `acknowledgeOnClose`.
    pub(crate) fn acknowledge_on_close(
        &mut self,
        acknowledgements_map: IndexMap<TopicIdPartition, NodeAcknowledgements>,
        deadline_ms: i64,
    ) -> ShareFuture<()> {
        let now = self.time.milliseconds();
        let cluster = self.metadata.fetch();
        let result_handler = Arc::new(ResultHandler::new(
            Some(0),
            None,
            Arc::clone(&self.is_acknowledgement_commit_callback_registered),
            self.acknowledge_event_handler.clone(),
        ));

        self.closing = true;
        let mut all_nodes: HashMap<i32, IndexMap<TopicIdPartition, Acknowledgements>> = HashMap::new();

        for (tip, node_acks) in acknowledgements_map {
            let nid = node_acks.node_id();
            if !Self::is_leader_known_to_have_changed(&self.metadata, nid, &tip) {
                let acks = node_acks.into_acknowledgements();
                let node_map = all_nodes.entry(nid).or_default();
                if let Some(existing) = node_map.get_mut(&tip) {
                    existing.merge(&acks);
                } else {
                    node_map.insert(tip, acks);
                }
            } else {
                let mut acks = node_acks.into_acknowledgements();
                acks.complete(errors_to_exception(Errors::NotLeaderOrFollower));
                let mut map = IndexMap::new();
                map.insert(tip.clone(), acks);
                maybe_send_share_acknowledgement_event(
                    &self.is_acknowledgement_commit_callback_registered,
                    &self.acknowledge_event_handler,
                    map,
                    true,
                    None,
                );
            }
        }

        let session_node_ids: Vec<i32> = self.session_handlers.keys().copied().collect();
        for node_id in session_node_ids {
            if cluster.node_by_id(node_id).is_none() {
                continue;
            }
            // Add any waiting piggyback acknowledgements for the node.
            if let Some(fetch_acks) = self.fetch_acknowledgements_to_send.remove(&node_id) {
                for (tip, acks) in fetch_acks {
                    if !Self::is_leader_known_to_have_changed(&self.metadata, node_id, &tip) {
                        let node_map = all_nodes.entry(node_id).or_default();
                        if let Some(existing) = node_map.get_mut(&tip) {
                            existing.merge(&acks);
                        } else {
                            node_map.insert(tip, acks);
                        }
                    } else {
                        let mut acks = acks;
                        acks.complete(errors_to_exception(Errors::NotLeaderOrFollower));
                        let mut map = IndexMap::new();
                        map.insert(tip.clone(), acks);
                        maybe_send_share_acknowledgement_event(
                            &self.is_acknowledgement_commit_callback_registered,
                            &self.acknowledge_event_handler,
                            map,
                            true,
                            None,
                        );
                    }
                }
            }

            let acks_for_node = all_nodes.remove(&node_id).unwrap_or_default();
            for (tip, acks) in &acks_for_node {
                let _ = acks;
                // metrics: deferred to KIP-714 (recordAcknowledgementSent)
                debug!(
                    "Added closing acknowledge request for partition {} to node {node_id}",
                    tip.topic_partition()
                );
                result_handler.increment_remaining();
            }

            self.acknowledge_request_states
                .entry(node_id)
                .or_insert_with(|| Tuple::new(None, None, None));

            let existing_close_in_progress = self
                .acknowledge_request_states
                .get(&node_id)
                .and_then(Tuple::get_close_request)
                .is_some_and(|s| Self::is_request_state_in_progress(Some(s)));

            if existing_close_in_progress {
                error!("Attempt to call close() when there is an existing close request for node {node_id}");
                self.close_future.complete_exceptionally(KafkaError::illegal_state(format!(
                    "Attempt to call close() when there is an existing close request for node : {node_id}"
                )));
            } else {
                let state = AcknowledgeRequestState::new(
                    "ShareConsumeRequestManager:3",
                    now,
                    deadline_ms,
                    self.retry_backoff_ms,
                    self.retry_backoff_max_ms,
                    node_id,
                    acks_for_node,
                    Arc::clone(&result_handler),
                    AcknowledgeRequestType::Close,
                );
                self.acknowledge_request_states
                    .get_mut(&node_id)
                    .expect("present")
                    .set_close_request(Some(state));
            }
        }

        result_handler.complete_if_empty();
        Arc::clone(&self.close_future)
    }

    /// Test/Phase-6 helper mirroring Java's `TestableShareConsumeRequestManager.buildResultHandler`.
    #[cfg(test)]
    pub(crate) fn build_result_handler(
        &self,
        remaining_results: Option<i32>,
        future: Option<ShareFuture<AcknowledgeResult>>,
    ) -> Arc<ResultHandler> {
        Arc::new(ResultHandler::new(
            remaining_results,
            future,
            Arc::clone(&self.is_acknowledgement_commit_callback_registered),
            self.acknowledge_event_handler.clone(),
        ))
    }
}

impl ShareConsumeRequestManager {
    /// Corresponds to Java's `handleShareFetchSuccess`.
    pub(crate) fn handle_share_fetch_success(
        &mut self,
        fetch_target: &Node,
        request_data: &ShareFetchRequestData,
        response: ShareFetchResponse,
        request_version: i16,
    ) {
        debug!("Completed ShareFetch request from node {} successfully", fetch_target.id());
        self.handle_share_fetch_success_body(fetch_target, request_data, response, request_version);
        // finally
        debug!("Removing pending request for node {} - success", fetch_target.id());
        if self.is_share_acquire_mode_record_limit() && self.fetch_records_node_id == fetch_target.id() {
            self.fetch_records_node_id = -1;
        }
        self.nodes_with_pending_requests.remove(&fetch_target.id());
    }

    fn handle_share_fetch_success_body(
        &mut self,
        fetch_target: &Node,
        request_data: &ShareFetchRequestData,
        mut response: ShareFetchResponse,
        request_version: i16,
    ) {
        let node_id = fetch_target.id();
        let Self {
            session_handlers,
            metadata,
            fetch_acknowledgements_in_flight,
            is_acknowledgement_commit_callback_registered,
            acknowledge_event_handler,
            share_fetch_buffer,
            fetch_more_records,
            topic_names_map,
            ..
        } = self;
        let metadata: &ShareConsumerMetadata = metadata;
        let is_ack_cb: &AtomicBool = is_acknowledgement_commit_callback_registered;

        let Some(handler) = session_handlers.get_mut(&node_id) else {
            error!("Unable to find ShareSessionHandler for node {node_id}. Ignoring ShareFetch response.");
            return;
        };

        let response_error = response.error();
        if !handler.handle_fetch_response(&response, request_version) {
            if response_error == Errors::UnknownTopicId {
                metadata.request_update(false);
            }
            // Complete any in-flight acknowledgements with the error code.
            if let Some(node_acks) = fetch_acknowledgements_in_flight.remove(&node_id) {
                let mut ev_map = IndexMap::new();
                for (tip, mut acks) in node_acks {
                    acks.complete(errors_to_exception(Errors::for_code(response_error.code())));
                    // metrics: deferred to KIP-714 (recordFailedAcknowledgements)
                    ev_map.insert(tip, acks);
                }
                maybe_send_share_acknowledgement_event(
                    is_ack_cb,
                    acknowledge_event_handler,
                    ev_map,
                    request_data.is_renew_ack,
                    None,
                );
            }
            return;
        }

        let response_ack_timeout = if response.data().acquisition_lock_timeout_ms > 0 {
            Some(response.data().acquisition_lock_timeout_ms)
        } else {
            None
        };

        let node_endpoints = std::mem::take(&mut response.data_mut().node_endpoints);
        let topic_responses = std::mem::take(&mut response.data_mut().responses);

        let mut response_data: IndexMap<TopicIdPartition, crate::share_fetch_response_data::PartitionData> =
            IndexMap::new();
        for topic_response in topic_responses {
            let topic_id = topic_response.topic_id;
            for partition in topic_response.partitions {
                let partition_index = partition.partition_index;
                if let Some(tip) = Self::lookup_topic_id(metadata, topic_names_map, topic_id, partition_index) {
                    response_data.insert(tip, partition);
                }
            }
        }

        // metrics: deferred to KIP-714 (ShareFetchMetricsAggregator)
        let mut completed_fetches: Vec<ShareCompletedFetch> = Vec::with_capacity(response_data.len());
        let mut partitions_with_updated_leader_info: HashMap<TopicPartition, LeaderIdAndEpoch> = HashMap::new();

        for (tip, partition_data) in response_data {
            debug!("ShareFetch for partition {tip} returned fetch data");

            if let Some(node_acks) = fetch_acknowledgements_in_flight.get_mut(&node_id)
                && let Some(mut acks) = node_acks.shift_remove(&tip)
            {
                // metrics: deferred to KIP-714 (recordFailedAcknowledgements)
                acks.complete(errors_to_exception_with_message(
                    partition_data.acknowledge_error_code,
                    partition_data.acknowledge_error_message.clone(),
                ));
                let mut map = IndexMap::new();
                map.insert(tip.clone(), acks);
                maybe_send_share_acknowledgement_event(
                    is_ack_cb,
                    acknowledge_event_handler,
                    map,
                    request_data.is_renew_ack,
                    response_ack_timeout,
                );
            }

            let partition_error = Errors::for_code(partition_data.error_code);
            if partition_error == Errors::NotLeaderOrFollower || partition_error == Errors::FencedLeaderEpoch {
                debug!(
                    "For {tip}, received error {partition_error:?}, with leaderIdAndEpoch {:?} in ShareFetch",
                    partition_data.current_leader
                );
                if partition_data.current_leader.leader_id != -1 && partition_data.current_leader.leader_epoch != -1 {
                    partitions_with_updated_leader_info.insert(
                        tip.topic_partition().clone(),
                        LeaderIdAndEpoch {
                            leader_id: Some(partition_data.current_leader.leader_id),
                            epoch: Some(partition_data.current_leader.leader_epoch),
                        },
                    );
                }
            }

            let has_acquired = !partition_data.acquired_records.is_empty();
            completed_fetches.push(ShareCompletedFetch::new_with_version(
                node_id,
                tip,
                partition_data,
                response_ack_timeout,
                request_version,
            ));

            if has_acquired {
                *fetch_more_records = false;
            }
        }

        if !completed_fetches.is_empty() {
            share_fetch_buffer.add(completed_fetches);
        }

        // Handle any acknowledgements which were not received in the response.
        if let Some(node_acks) = fetch_acknowledgements_in_flight.remove(&node_id) {
            for (partition, mut acknowledgements) in node_acks {
                acknowledgements.complete(Some(KafkaError::with_message(Errors::InvalidRecordState, INVALID_RESPONSE)));
                let mut map = IndexMap::new();
                map.insert(partition, acknowledgements);
                maybe_send_share_acknowledgement_event(is_ack_cb, acknowledge_event_handler, map, true, None);
            }
        }

        if !partitions_with_updated_leader_info.is_empty() {
            let leader_nodes: Vec<Node> = node_endpoints
                .iter()
                .map(|e| Node::with_rack(e.node_id, e.host.clone(), e.port, e.rack.clone()))
                .filter(|n| n.id() != Node::no_node().id())
                .collect();
            metadata.update_partition_leadership(&partitions_with_updated_leader_info, &leader_nodes);
        }
        // metrics: deferred to KIP-714 (recordLatency)
    }

    /// Corresponds to Java's `handleShareFetchFailure`.
    pub(crate) fn handle_share_fetch_failure(
        &mut self,
        fetch_target: &Node,
        request_data: &ShareFetchRequestData,
        error: &KafkaError,
    ) {
        let node_id = fetch_target.id();
        debug!(
            "Completed ShareFetch request from node {node_id} unsuccessfully {:?}",
            error.error()
        );
        {
            let Self {
                session_handlers,
                metadata,
                fetch_acknowledgements_in_flight,
                is_acknowledgement_commit_callback_registered,
                acknowledge_event_handler,
                topic_names_map,
                ..
            } = self;
            let metadata: &ShareConsumerMetadata = metadata;
            let is_ack_cb: &AtomicBool = is_acknowledgement_commit_callback_registered;

            if let Some(handler) = session_handlers.get_mut(&node_id) {
                handler.handle_error(error);
            }

            for topic in &request_data.topics {
                for partition in &topic.partitions {
                    let Some(tip) =
                        Self::lookup_topic_id(metadata, topic_names_map, topic.topic_id, partition.partition_index)
                    else {
                        continue;
                    };
                    if let Some(node_acks) = fetch_acknowledgements_in_flight.get_mut(&node_id)
                        && let Some(mut acks) = node_acks.shift_remove(&tip)
                    {
                        // metrics: deferred to KIP-714 (recordFailedAcknowledgements)
                        acks.complete(Some(error.clone()));
                        let mut map = IndexMap::new();
                        map.insert(tip.clone(), acks);
                        maybe_send_share_acknowledgement_event(
                            is_ack_cb,
                            acknowledge_event_handler,
                            map,
                            request_data.is_renew_ack,
                            None,
                        );
                    }
                }
            }
        }
        // finally
        debug!("Removing pending request for node {node_id} - failed");
        if self.is_share_acquire_mode_record_limit() && self.fetch_records_node_id == node_id {
            self.fetch_records_node_id = -1;
        }
        self.nodes_with_pending_requests.remove(&node_id);
    }

    /// Corresponds to Java's `handleShareAcknowledgeSuccess`.
    pub(crate) fn handle_share_acknowledge_success(
        &mut self,
        fetch_target: &Node,
        request_data: &ShareAcknowledgeRequestData,
        response: ShareAcknowledgeResponse,
        request_version: i16,
        response_completion_time_ms: i64,
    ) {
        let node_id = fetch_target.id();
        debug!("Completed ShareAcknowledge request from node {node_id} successfully");
        let Self {
            acknowledge_request_states,
            session_handlers,
            metadata,
            topic_names_map,
            nodes_with_pending_requests,
            ..
        } = self;
        let metadata: &ShareConsumerMetadata = metadata;

        let response_ack_timeout = if response.data().acquisition_lock_timeout_ms > 0 {
            Some(response.data().acquisition_lock_timeout_ms)
        } else {
            None
        };
        let is_renew_ack = request_data.is_renew_ack;
        let mut partitions_with_updated_leader_info: HashMap<TopicPartition, LeaderIdAndEpoch> = HashMap::new();
        let mut is_close = false;

        let slot = acknowledge_request_states.get(&node_id).and_then(Self::find_in_flight_ack_slot);

        if let Some(slot) = slot {
            let state =
                Self::ack_slot_state_mut(acknowledge_request_states.get_mut(&node_id).expect("tuple present"), slot)
                    .expect("slot resolves");
            is_close = state.is_close_request();

            if is_close {
                for topic in &response.data().responses {
                    for pd in &topic.partitions {
                        let Some(tip) =
                            Self::lookup_topic_id(metadata, topic_names_map, topic.topic_id, pd.partition_index)
                        else {
                            continue;
                        };
                        // metrics: deferred to KIP-714 (recordFailedAcknowledgements)
                        state.handle_acknowledge_error_code(
                            tip,
                            Errors::for_code(pd.error_code),
                            is_renew_ack,
                            response_ack_timeout,
                        );
                    }
                }
                state.on_successful_attempt(response_completion_time_ms);
                state.processing_complete();
            } else {
                let handled = match session_handlers.get_mut(&node_id) {
                    Some(handler) => handler.handle_acknowledge_response(&response, request_version),
                    None => false,
                };
                if !handled {
                    // Received a response-level error code.
                    state.on_failed_attempt(response_completion_time_ms);
                    let resp_error = response.error();
                    if resp_error.is_retriable() {
                        // Retry until the timer expires, unless we are closing.
                        state.move_all_to_incomplete_acks();
                    } else {
                        state.process_pending_in_flight_acknowledgements(KafkaError::new(resp_error));
                        state.processing_complete();
                    }
                } else {
                    let mut should_retry = false;
                    for topic in &response.data().responses {
                        for pd in &topic.partitions {
                            let partition_error = Errors::for_code(pd.error_code);
                            let Some(tip) =
                                Self::lookup_topic_id(metadata, topic_names_map, topic.topic_id, pd.partition_index)
                            else {
                                continue;
                            };
                            Self::handle_partition_error(
                                pd,
                                &mut partitions_with_updated_leader_info,
                                state,
                                partition_error,
                                tip,
                                &mut should_retry,
                                is_renew_ack,
                                response_ack_timeout,
                            );
                        }
                    }
                    Self::process_retry_logic(state, should_retry, response_completion_time_ms);
                }
            }

            if !partitions_with_updated_leader_info.is_empty() {
                let leader_nodes: Vec<Node> = response
                    .data()
                    .node_endpoints
                    .iter()
                    .map(|e| Node::with_rack(e.node_id, e.host.clone(), e.port, e.rack.clone()))
                    .filter(|n| n.id() != Node::no_node().id())
                    .collect();
                metadata.update_partition_leadership(&partitions_with_updated_leader_info, &leader_nodes);
            }
            // metrics: deferred to KIP-714 (recordLatency when state.is_processed)
        }

        // finally
        debug!("Removing pending request for node {node_id} - success");
        nodes_with_pending_requests.remove(&node_id);
        if is_close {
            debug!("Removing node from ShareSession {node_id}");
            session_handlers.remove(&node_id);
        }
    }

    /// Corresponds to Java's `handleShareAcknowledgeFailure`.
    pub(crate) fn handle_share_acknowledge_failure(
        &mut self,
        fetch_target: &Node,
        request_data: &ShareAcknowledgeRequestData,
        error: &KafkaError,
        response_completion_time_ms: i64,
    ) {
        let node_id = fetch_target.id();
        debug!(
            "Completed ShareAcknowledge request from node {node_id} unsuccessfully {:?}",
            error.error()
        );
        let Self {
            acknowledge_request_states,
            session_handlers,
            metadata,
            topic_names_map,
            nodes_with_pending_requests,
            ..
        } = self;
        let metadata: &ShareConsumerMetadata = metadata;
        let mut is_close = false;

        if let Some(handler) = session_handlers.get_mut(&node_id) {
            handler.handle_error(error);
        }

        let slot = acknowledge_request_states.get(&node_id).and_then(Self::find_in_flight_ack_slot);
        if let Some(slot) = slot {
            let state =
                Self::ack_slot_state_mut(acknowledge_request_states.get_mut(&node_id).expect("tuple present"), slot)
                    .expect("slot resolves");
            is_close = state.is_close_request();
            state.on_failed_attempt(response_completion_time_ms);
            for topic in &request_data.topics {
                for partition in &topic.partitions {
                    let Some(tip) =
                        Self::lookup_topic_id(metadata, topic_names_map, topic.topic_id, partition.partition_index)
                    else {
                        continue;
                    };
                    // metrics: deferred to KIP-714 (recordFailedAcknowledgements)
                    state.handle_acknowledge_error_code(tip, error.error(), request_data.is_renew_ack, None);
                }
            }
            state.processing_complete();
        }

        // finally
        debug!("Removing pending request for node {node_id} - failed");
        nodes_with_pending_requests.remove(&node_id);
        if is_close {
            debug!("Removing node from ShareSession {node_id}");
            session_handlers.remove(&node_id);
        }
    }

    /// Corresponds to Java's `handlePartitionError`.
    #[allow(clippy::too_many_arguments)]
    fn handle_partition_error(
        partition_data: &crate::share_acknowledge_response_data::PartitionData,
        partitions_with_updated_leader_info: &mut HashMap<TopicPartition, LeaderIdAndEpoch>,
        state: &mut AcknowledgeRequestState,
        partition_error: Errors,
        tip: TopicIdPartition,
        should_retry: &mut bool,
        is_renew_ack: bool,
        acquisition_lock_timeout_ms: Option<i32>,
    ) {
        if partition_error != Errors::None {
            let mut retry = false;
            if matches!(
                partition_error,
                Errors::NotLeaderOrFollower
                    | Errors::FencedLeaderEpoch
                    | Errors::UnknownTopicOrPartition
                    | Errors::UnknownTopicId
            ) {
                // If the leader has changed, there's no point retrying — the
                // acquisition locks will have been released. If the topic or
                // partition was deleted, we do not retry; those records will
                // be re-delivered once they time out on the broker.
                Self::update_leader_info_map(
                    partition_data,
                    partitions_with_updated_leader_info,
                    partition_error,
                    tip.topic_partition().clone(),
                );
            } else if partition_error.is_retriable() {
                retry = true;
            }

            if retry {
                if state.move_to_incomplete_acks(&tip) {
                    *should_retry = true;
                }
            } else {
                // metrics: deferred to KIP-714 (recordFailedAcknowledgements)
                state.handle_acknowledge_error_code(tip, partition_error, is_renew_ack, None);
            }
        } else {
            state.handle_acknowledge_error_code(tip, partition_error, is_renew_ack, acquisition_lock_timeout_ms);
        }
    }

    /// Corresponds to Java's `processRetryLogic`.
    fn process_retry_logic(state: &mut AcknowledgeRequestState, should_retry: bool, response_completion_time_ms: i64) {
        if should_retry {
            state.on_failed_attempt(response_completion_time_ms);
            // Acknowledgements that did not receive a response are failed with
            // InvalidRecordStateException.
            state.process_pending_in_flight_acknowledgements(KafkaError::with_message(
                Errors::InvalidRecordState,
                INVALID_RESPONSE,
            ));
        } else {
            state.on_successful_attempt(response_completion_time_ms);
            state.processing_complete();
        }
    }

    /// Corresponds to Java's `updateLeaderInfoMap`.
    fn update_leader_info_map(
        partition_data: &crate::share_acknowledge_response_data::PartitionData,
        partitions_with_updated_leader_info: &mut HashMap<TopicPartition, LeaderIdAndEpoch>,
        partition_error: Errors,
        tp: TopicPartition,
    ) {
        debug!(
            "For {tp}, received error {partition_error:?}, with leaderIdAndEpoch {:?} in ShareAcknowledge",
            partition_data.current_leader
        );
        if partition_data.current_leader.leader_id != -1 && partition_data.current_leader.leader_epoch != -1 {
            partitions_with_updated_leader_info.insert(
                tp,
                LeaderIdAndEpoch {
                    leader_id: Some(partition_data.current_leader.leader_id),
                    epoch: Some(partition_data.current_leader.leader_epoch),
                },
            );
        }
    }
}

impl RequestManager for ShareConsumeRequestManager {
    fn poll(&mut self, current_time_ms: i64) -> PollResult {
        ShareConsumeRequestManager::poll(self, current_time_ms)
    }
}
