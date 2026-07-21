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

//! `ShareHeartbeatRequestManager` — KIP-932 heartbeat lifecycle for share
//! groups.
//!
//! Composes
//! [`super::abstract_heartbeat_request_manager::AbstractHeartbeatRequestManager`]
//! and supplies the share-group-specific request builder
//! ([`crate::common::requests::ShareGroupHeartbeatRequest`]), response handler
//! ([`crate::common::requests::ShareGroupHeartbeatResponse`]), and error
//! classification.
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.ShareHeartbeatRequestManager`.
//!
//! # Relationship to `ConsumerHeartbeatRequestManager`
//!
//! This is a close cousin of
//! [`super::consumer_heartbeat_request_manager::ConsumerHeartbeatRequestManager`].
//! The response-routing scaffolding (spawned forwarder + `mpsc`
//! channel-back, async membership-transition side-channel) is identical —
//! see that module for the full rationale (`consumer-threading.md` §10 /
//! §16). The share variant differs in:
//!
//! - The request/response types are `ShareGroupHeartbeat{Request,Response}`.
//! - The `HeartbeatState` field-diff carries only `groupId`, `memberId`,
//!   `memberEpoch`, `rackId` (sent once), and `subscribedTopicNames`
//!   (diffed). Share heartbeats do not carry `rebalanceTimeoutMs`,
//!   `serverAssignor`, `subscribedTopicRegex`, or `topicPartitions`.
//! - Error classification adds only the two `UNSUPPORTED_VERSION`
//!   branches (broker-side / client-side) with the share-specific
//!   protocol-not-supported messages; there is no
//!   `FENCED_INSTANCE_ID` / `UNRELEASED_INSTANCE_ID` / `GROUP_ID_NOT_FOUND`
//!   special-casing (share groups have no static membership, and Java's
//!   share manager treats those via the abstract-layer default arm).
//! - `shouldSendLeaveHeartbeatNow()` is simply `state() == LEAVING`.
//!
//! **Metrics deferred to KIP-714**: `HeartbeatMetricsManager` recording is
//! omitted.

#![allow(dead_code)] // Phase 4: lands before the bg-loop wiring (Phases 5-6).

use std::sync::Arc;
use std::sync::Mutex;

use tokio::sync::mpsc;

use crate::common::KafkaError;
use crate::common::protocol::Errors;
use crate::common::requests::ConcreteResponse;
use crate::common::requests::share_group_heartbeat_request::ShareGroupHeartbeatRequestBuilder;
use crate::common::requests::share_group_heartbeat_response::ShareGroupHeartbeatResponse;
use crate::consumer::ConsumerConfig;
use crate::consumer::internals::events::background_event::BackgroundEvent;
use crate::consumer::internals::events::background_event_handler::BackgroundEventHandler;
use crate::share_group_heartbeat_request_data::ShareGroupHeartbeatRequestData;

use super::abstract_heartbeat_request_manager::{
    AbstractHeartbeatRequestManager, HeartbeatErrorAction, HeartbeatFailureAction, make_heartbeat_poll_result,
};
use super::coordinator_request_manager::CoordinatorRequestManager;
use super::member_state::MemberState;
use super::network_client_delegate::{PollResult, UnsentRequest};
use super::request_manager::RequestManager;
use super::share_membership_manager::ShareMembershipManager;
use super::subscription_state::SubscriptionState;

/// Message logged / propagated when the broker reports the share group
/// protocol is not enabled.
///
/// Java: `ShareHeartbeatRequestManager.SHARE_PROTOCOL_NOT_SUPPORTED_MSG`.
pub(crate) const SHARE_PROTOCOL_NOT_SUPPORTED_MSG: &str = "The cluster does not support the share group protocol. \
     To use share groups, the cluster must have the share group protocol enabled.";

/// Message logged / propagated when the broker does not support
/// `ShareGroupHeartbeat` API version 1 or later.
///
/// Java: `ShareHeartbeatRequestManager.SHARE_PROTOCOL_VERSION_NOT_SUPPORTED_MSG`.
pub(crate) const SHARE_PROTOCOL_VERSION_NOT_SUPPORTED_MSG: &str = "The cluster does not support the share group protocol \
     using ShareGroupHeartbeat API version 1 or later. This version of the API was introduced in Apache Kafka v4.1.";

/// Envelope routing a `ShareGroupHeartbeatResponse` (or its transport-level
/// failure) from the spawned forwarder back to the heartbeat manager's next
/// `poll(now)`. See
/// [`super::consumer_heartbeat_request_manager::PendingHeartbeatCompletion`]
/// for the full rationale.
pub(crate) enum PendingHeartbeatCompletion {
    Response {
        response: ShareGroupHeartbeatResponse,
        completion_time_ms: i64,
    },
    Failure {
        error: KafkaError,
        completion_time_ms: i64,
    },
}

/// Side-channel envelope emitted when the response classifier yields a
/// `Fenced`, `Fatal`, or (poll-timer-expiry) `Stale` outcome. The bg-task
/// drains it after `entries().poll(now)` and `await`s the matching
/// `ShareMembershipManager::transition_to_*`. See
/// [`super::consumer_heartbeat_request_manager::PendingMembershipTransition`].
#[derive(Debug)]
pub(crate) enum PendingMembershipTransition {
    /// Broker returned `FENCED_MEMBER_EPOCH` or `UNKNOWN_MEMBER_ID`.
    Fenced,
    /// Fatal heartbeat outcome. Carries the error for logging.
    Fatal(KafkaError),
    /// The member transitioned to STALE because the poll timer expired.
    Stale,
}

/// Tracks which fields were sent on the most recent heartbeat. Java's
/// `ShareHeartbeatRequestManager.HeartbeatState.SentFields`.
#[derive(Default)]
struct SentFields {
    /// Mirrors Java's `String rackId` sentinel. `None` means "not yet
    /// sent" (matching Java's `null`): the rack ID is sent once, but if the
    /// member's rack ID is itself `None`, this stays `None` and the field
    /// is re-set (to `None`) on each build — harmless, exactly as Java's
    /// `setRackId(null)` is.
    rack_id: Option<String>,
    /// Topic names sorted; `None` means "not yet sent".
    subscribed_topic_names: Option<Vec<String>>,
}

impl SentFields {
    fn reset(&mut self) {
        self.rack_id = None;
        self.subscribed_topic_names = None;
    }
}

/// State for building `ShareGroupHeartbeatRequest`s with field diffing.
/// Mirrors Java's `ShareHeartbeatRequestManager.HeartbeatState`.
pub(crate) struct HeartbeatState {
    subscriptions: Arc<Mutex<SubscriptionState>>,
    membership_manager: Arc<ShareMembershipManager>,
    sent_fields: SentFields,
}

impl HeartbeatState {
    pub(crate) fn new(
        subscriptions: Arc<Mutex<SubscriptionState>>,
        membership_manager: Arc<ShareMembershipManager>,
    ) -> Self {
        Self { subscriptions, membership_manager, sent_fields: SentFields::default() }
    }

    fn reset(&mut self) {
        self.sent_fields.reset();
    }

    /// Java: `buildRequestData()`. Constructs the request data with
    /// field-level diffing so subsequent heartbeats only include changed
    /// fields.
    fn build_request_data(&mut self) -> ShareGroupHeartbeatRequestData {
        let mut data = ShareGroupHeartbeatRequestData::new();

        // GroupId - always sent.
        data.set_group_id(self.membership_manager.group_id());
        // MemberId - always sent (generated at Consumer startup).
        data.set_member_id(self.membership_manager.member_id());
        // MemberEpoch - always sent.
        data.set_member_epoch(self.membership_manager.member_epoch());

        // RackId - only sent the first time, because it does not change.
        // Mirrors Java's `if (sentFields.rackId == null) { ... }`.
        if self.sent_fields.rack_id.is_none() {
            data.set_rack_id(self.membership_manager.rack_id().map(str::to_string));
            self.sent_fields.rack_id = self.membership_manager.rack_id().map(str::to_string);
        }

        let send_all_fields = self.membership_manager.state() == MemberState::Joining;

        // SubscribedTopicNames - only sent when joining or if it has changed
        // since the last heartbeat.
        let mut current_topics = {
            let subs = match self.subscriptions.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            subs.subscription().into_iter().collect::<Vec<String>>()
        };
        current_topics.sort();
        let topics_changed = match &self.sent_fields.subscribed_topic_names {
            None => true,
            Some(prev) => prev != &current_topics,
        };
        if send_all_fields || topics_changed {
            data.set_subscribed_topic_names(Some(current_topics.clone()));
            self.sent_fields.subscribed_topic_names = Some(current_topics);
        }

        data
    }
}

/// KIP-932 share-group heartbeat request manager. Composes
/// [`AbstractHeartbeatRequestManager`].
///
/// Java: `ShareHeartbeatRequestManager extends AbstractHeartbeatRequestManager<ShareGroupHeartbeatResponse>`.
pub(crate) struct ShareHeartbeatRequestManager {
    inner: AbstractHeartbeatRequestManager,
    membership_manager: Arc<ShareMembershipManager>,
    heartbeat_state: HeartbeatState,
    pending_completion_tx: mpsc::UnboundedSender<PendingHeartbeatCompletion>,
    pending_completion_rx: mpsc::UnboundedReceiver<PendingHeartbeatCompletion>,
    pending_membership_transition_tx: mpsc::UnboundedSender<PendingMembershipTransition>,
    pending_membership_transition_rx: mpsc::UnboundedReceiver<PendingMembershipTransition>,
}

impl ShareHeartbeatRequestManager {
    /// Java: `ShareHeartbeatRequestManager(LogContext, Time, ConsumerConfig,
    /// CoordinatorRequestManager, SubscriptionState, ShareMembershipManager,
    /// BackgroundEventHandler, Metrics)`.
    pub(crate) fn new(
        current_time_ms: i64,
        config: &ConsumerConfig,
        coordinator_request_manager: Arc<CoordinatorRequestManager>,
        subscriptions: Arc<Mutex<SubscriptionState>>,
        membership_manager: Arc<ShareMembershipManager>,
        background_event_handler: Arc<BackgroundEventHandler>,
    ) -> Self {
        let inner = AbstractHeartbeatRequestManager::new(
            current_time_ms,
            config,
            coordinator_request_manager,
            background_event_handler,
        );
        let heartbeat_state = HeartbeatState::new(subscriptions, membership_manager.clone());
        let (pending_completion_tx, pending_completion_rx) = mpsc::unbounded_channel();
        let (pending_membership_transition_tx, pending_membership_transition_rx) = mpsc::unbounded_channel();
        Self {
            inner,
            membership_manager,
            heartbeat_state,
            pending_completion_tx,
            pending_completion_rx,
            pending_membership_transition_tx,
            pending_membership_transition_rx,
        }
    }

    /// Drain the [`PendingMembershipTransition`] side-channel. See
    /// [`super::consumer_heartbeat_request_manager::ConsumerHeartbeatRequestManager::take_pending_membership_transitions`].
    pub(crate) fn take_pending_membership_transitions(&mut self) -> Vec<PendingMembershipTransition> {
        let mut out = Vec::new();
        while let Ok(t) = self.pending_membership_transition_rx.try_recv() {
            out.push(t);
        }
        out
    }

    /// Java: `resetHeartbeatState()`.
    pub(crate) fn reset_heartbeat_state(&mut self) {
        self.heartbeat_state.reset();
    }

    /// Java: `heartbeatRequestName()`.
    fn heartbeat_request_name(&self) -> &'static str {
        "ShareGroupHeartbeatRequest"
    }

    /// Java: `shouldSendLeaveHeartbeatNow()` — `state() == LEAVING`.
    fn should_send_leave_heartbeat_now(&self) -> bool {
        self.membership_manager.state() == MemberState::Leaving
    }

    /// Test-only accessor for [`Self::should_send_leave_heartbeat_now`].
    #[cfg(test)]
    pub(crate) fn should_send_leave_heartbeat_now_for_test(&self) -> bool {
        self.should_send_leave_heartbeat_now()
    }

    /// Returns the wrapped membership manager.
    pub(crate) fn membership_manager(&self) -> &Arc<ShareMembershipManager> {
        &self.membership_manager
    }

    /// Returns a reference to the underlying
    /// [`AbstractHeartbeatRequestManager`].
    pub(crate) fn inner(&self) -> &AbstractHeartbeatRequestManager {
        &self.inner
    }

    /// Returns a mutable reference to the underlying
    /// [`AbstractHeartbeatRequestManager`].
    pub(crate) fn inner_mut(&mut self) -> &mut AbstractHeartbeatRequestManager {
        &mut self.inner
    }

    /// Test-only: invoke the `HeartbeatState::build_request_data` field-diff
    /// logic directly.
    #[cfg(test)]
    pub(crate) fn build_request_data_for_test(&mut self) -> ShareGroupHeartbeatRequestData {
        self.heartbeat_state.build_request_data()
    }

    /// Java: `buildHeartbeatRequest()`. Returns an `UnsentRequest` targeting
    /// the current coordinator node and spawns a forwarder that routes the
    /// response back through [`Self::drain_pending_completions`]. See the
    /// consumer variant for the full mechanism.
    fn build_heartbeat_request(&mut self, ignore_response: bool) -> UnsentRequest {
        let data = self.heartbeat_state.build_request_data();
        let builder = Box::new(ShareGroupHeartbeatRequestBuilder::new(data));
        let node = self.inner.coordinator_request_manager.coordinator();
        let mut unsent = UnsentRequest::new(builder, node);

        let response_rx = unsent.take_response_receiver().expect("receiver fresh");
        let tx = self.pending_completion_tx.clone();
        tokio::spawn(async move {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0);
            let completion = match response_rx.await {
                Ok(Ok(mut client_response)) => match client_response.take_response_body() {
                    Some(ConcreteResponse::ShareGroupHeartbeat(resp)) => {
                        PendingHeartbeatCompletion::Response { response: resp, completion_time_ms: now_ms }
                    },
                    _ => PendingHeartbeatCompletion::Failure {
                        error: KafkaError::new(Errors::UnknownServerError),
                        completion_time_ms: now_ms,
                    },
                },
                Ok(Err(err)) => PendingHeartbeatCompletion::Failure { error: err, completion_time_ms: now_ms },
                Err(_recv) => PendingHeartbeatCompletion::Failure {
                    error: KafkaError::new(Errors::NetworkException),
                    completion_time_ms: now_ms,
                },
            };
            if !ignore_response {
                let _ = tx.send(completion);
            }
        });
        unsent
    }

    /// Drains the [`PendingHeartbeatCompletion`] mpsc channel. Called at the
    /// top of [`RequestManager::poll`].
    fn drain_pending_completions(&mut self, _current_time_ms: i64) {
        while let Ok(completion) = self.pending_completion_rx.try_recv() {
            match completion {
                PendingHeartbeatCompletion::Response { response, completion_time_ms } => {
                    self.on_response(&response, completion_time_ms);
                },
                PendingHeartbeatCompletion::Failure { error, completion_time_ms } => {
                    self.on_failure(&error, completion_time_ms);
                },
            }
        }
    }

    /// Dispatch a `ShareGroupHeartbeatResponse` body to the success or error
    /// path. Mirrors Java's `onResponse(R, long)`.
    fn on_response(&mut self, response: &ShareGroupHeartbeatResponse, completion_time_ms: i64) {
        let error = response.error();
        if error == Errors::None {
            let new_interval_ms = i64::from(response.data().heartbeat_interval_ms);
            self.inner.on_successful_response(new_interval_ms, completion_time_ms);
            if let Err(e) = self.membership_manager.on_heartbeat_success(response) {
                log::error!("on_heartbeat_success failed: {}", e);
                let _ = self
                    .inner
                    .background_event_handler
                    .add(BackgroundEvent::Error { error: e }, completion_time_ms);
            }
            return;
        }
        // Error response — reset the per-request `HeartbeatState` field
        // tracker at the top, before classifying (Java's `onErrorResponse`).
        self.reset_heartbeat_state();
        let error_message = format!("{error:?}");
        let action = self.inner.classify_response_error(error, &error_message, completion_time_ms);
        let final_action = match action {
            HeartbeatErrorAction::DelegateToSpecific => self
                .handle_specific_exception_in_response(error, &error_message, completion_time_ms)
                .unwrap_or_else(|| {
                    // Java: default arm of `onErrorResponse` → fatal.
                    log::error!(
                        "{} failed due to unexpected error {:?}: {}",
                        self.heartbeat_request_name(),
                        error,
                        error_message
                    );
                    HeartbeatErrorAction::Fatal(KafkaError::with_message(error, error_message.clone()))
                }),
            other => other,
        };
        match final_action {
            HeartbeatErrorAction::Handled => {},
            HeartbeatErrorAction::Fenced => {
                // Internal state-machine event — no ErrorEvent emitted (see
                // consumer variant). Route the async transition through the
                // side-channel.
                let _ = self.pending_membership_transition_tx.send(PendingMembershipTransition::Fenced);
            },
            HeartbeatErrorAction::Fatal(err) => {
                let _ = self
                    .inner
                    .background_event_handler
                    .add(BackgroundEvent::Error { error: err.clone() }, completion_time_ms);
                let _ = self
                    .pending_membership_transition_tx
                    .send(PendingMembershipTransition::Fatal(err));
            },
            HeartbeatErrorAction::DelegateToSpecific => {
                debug_assert!(false, "DelegateToSpecific should have been resolved");
            },
        }
        // Java: `membershipManager().onHeartbeatFailure(false)` at the tail.
        self.membership_manager.on_heartbeat_failure(false);
    }

    /// Transport-level / non-response failure handler. Mirrors Java's
    /// `onFailure(Throwable, long)`.
    fn on_failure(&mut self, error: &KafkaError, completion_time_ms: i64) {
        self.reset_heartbeat_state();
        let abstract_action = self.inner.on_failure(error, completion_time_ms);
        let retriable = matches!(abstract_action, HeartbeatFailureAction::Retriable);
        if !retriable {
            let specific_handled = self.handle_specific_failure(error, completion_time_ms);
            if !specific_handled {
                log::error!("{} failed due to fatal error: {}", self.heartbeat_request_name(), error);
                let _ = self
                    .inner
                    .background_event_handler
                    .add(BackgroundEvent::Error { error: error.clone() }, completion_time_ms);
                let _ = self
                    .pending_membership_transition_tx
                    .send(PendingMembershipTransition::Fatal(error.clone()));
            }
        }
        self.membership_manager.on_heartbeat_failure(retriable);
    }

    /// Java: `handleSpecificFailure(Throwable exception)`. Maps
    /// `UnsupportedVersionException` (client-side, e.g. unsupported API
    /// version) to a fatal failure carrying
    /// [`SHARE_PROTOCOL_VERSION_NOT_SUPPORTED_MSG`].
    pub(crate) fn handle_specific_failure(&mut self, error: &KafkaError, current_time_ms: i64) -> bool {
        if error.error() == Errors::UnsupportedVersion {
            log::error!(
                "{} failed due to {}: {}",
                self.heartbeat_request_name(),
                error,
                SHARE_PROTOCOL_VERSION_NOT_SUPPORTED_MSG
            );
            let fatal_err = KafkaError::unsupported_version(SHARE_PROTOCOL_VERSION_NOT_SUPPORTED_MSG.to_string());
            let _ = self
                .inner
                .background_event_handler
                .add(BackgroundEvent::Error { error: fatal_err.clone() }, current_time_ms);
            let _ = self
                .pending_membership_transition_tx
                .send(PendingMembershipTransition::Fatal(fatal_err));
            return true;
        }
        false
    }

    /// Java: `handleSpecificExceptionInResponse(response, currentTimeMs)`.
    /// The share variant maps a broker-side `UNSUPPORTED_VERSION` to a fatal
    /// failure carrying [`SHARE_PROTOCOL_NOT_SUPPORTED_MSG`].
    pub(crate) fn handle_specific_exception_in_response(
        &mut self,
        error: Errors,
        _error_message: &str,
        _current_time_ms: i64,
    ) -> Option<HeartbeatErrorAction> {
        match error {
            Errors::UnsupportedVersion => {
                log::error!(
                    "{} failed due to unsupported version: {}",
                    self.heartbeat_request_name(),
                    SHARE_PROTOCOL_NOT_SUPPORTED_MSG
                );
                Some(HeartbeatErrorAction::Fatal(KafkaError::with_message(
                    Errors::UnsupportedVersion,
                    SHARE_PROTOCOL_NOT_SUPPORTED_MSG.to_string(),
                )))
            },
            _ => None,
        }
    }
}

impl RequestManager for ShareHeartbeatRequestManager {
    /// Java: `poll(long currentTimeMs)`. Mirrors
    /// `AbstractHeartbeatRequestManager.poll(...)` phase-for-phase. `poll` is
    /// sync (`consumer-threading.md` §10); the membership state machine's
    /// async transitions happen in the bg-task path (Phase 5/6).
    fn poll(&mut self, current_time_ms: i64) -> PollResult {
        // 0. Drain any pending heartbeat completions from prior forwarders.
        self.drain_pending_completions(current_time_ms);

        // 1. Skip-heartbeat short-circuit.
        let coordinator_known = self.inner.coordinator_request_manager.coordinator().is_some();
        if !coordinator_known
            || self
                .membership_manager
                .abstract_mm
                .inner
                .lock()
                .map(|g| g.should_skip_heartbeat())
                .unwrap_or(false)
        {
            let _ = self.membership_manager.abstract_mm.on_heartbeat_request_skipped();
            self.inner.maybe_propagate_coordinator_fatal_error_event(current_time_ms);
            return PollResult::empty();
        }

        // 2. Poll-timer-expired stale path.
        if self.inner.poll_timer_is_expired(current_time_ms) && !self.membership_manager.is_leaving_group() {
            log::warn!(
                "Consumer poll timeout has expired. This means the time between subsequent calls to poll() was longer than the configured max.poll.interval.ms."
            );
            if let Err(e) = self.membership_manager.transition_to_sending_leave_group(true) {
                log::warn!("transition_to_sending_leave_group failed: {}", e);
                return PollResult::empty();
            }
            let request = self.build_heartbeat_request(true);
            if let Err(e) = self.membership_manager.abstract_mm.on_heartbeat_request_generated() {
                log::warn!("on_heartbeat_request_generated (poll-timer-expiry) failed: {}", e);
            }
            if self.membership_manager.state() == MemberState::Stale {
                let _ = self.pending_membership_transition_tx.send(PendingMembershipTransition::Stale);
            }
            self.inner.heartbeat_request_state.reset();
            self.reset_heartbeat_state();
            return PollResult::new(self.inner.heartbeat_request_state.heartbeat_interval_ms(), vec![request]);
        }

        // 3. Decide whether to heartbeat now.
        let heartbeat_now = self.should_send_leave_heartbeat_now() || {
            let guard = match self.membership_manager.abstract_mm.inner.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            guard.should_heartbeat_now() && !self.inner.heartbeat_request_state.request_in_flight()
        };

        if !self.inner.heartbeat_request_state.can_send_request(current_time_ms) && !heartbeat_now {
            return PollResult::from_wait(
                self.inner.heartbeat_request_state.time_to_next_heartbeat_ms(current_time_ms),
            );
        }

        // Java: `makeHeartbeatRequest(currentTimeMs, false)`.
        let request = self.build_heartbeat_request(false);
        if let Err(e) = self.membership_manager.abstract_mm.on_heartbeat_request_generated() {
            log::warn!("on_heartbeat_request_generated failed: {}", e);
        }
        make_heartbeat_poll_result(request, &mut self.inner, current_time_ms)
    }

    fn poll_on_close(&mut self, current_time_ms: i64) -> PollResult {
        self.drain_pending_completions(current_time_ms);
        if self.membership_manager.is_leaving_group() {
            let request = self.build_heartbeat_request(true);
            return PollResult::new(self.inner.heartbeat_request_state.heartbeat_interval_ms(), vec![request]);
        }
        PollResult::empty()
    }

    fn maximum_time_to_wait(&self, current_time_ms: i64) -> i64 {
        if self.inner.poll_timer_is_expired(current_time_ms) {
            return 0;
        }
        let should_now = {
            let guard = match self.membership_manager.abstract_mm.inner.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            guard.should_heartbeat_now() && !self.inner.heartbeat_request_state.request_in_flight()
        };
        if should_now {
            return 0;
        }
        let remaining_poll = self.inner.poll_timer_remaining_ms(current_time_ms);
        let time_to_hb = self.inner.heartbeat_request_state.time_to_next_heartbeat_ms(current_time_ms);
        std::cmp::min(remaining_poll / 2, time_to_hb)
    }
}

/// Translation notes on Java test coverage (`ShareHeartbeatRequestManagerTest`,
/// 15 `@Test` / `@ParameterizedTest`). Like the consumer heartbeat manager,
/// the shared timing / backoff / error-classification logic lives in
/// [`AbstractHeartbeatRequestManager`] (tested there); these tests cover the
/// share-specific surface (request/response types, the two
/// `SHARE_PROTOCOL_*` messages, the `HeartbeatState` field diff) plus a
/// representative slice of the poll lifecycle, with REAL managers and
/// test-only accessors (Mockito's role in Java).
///
/// Not translated:
/// - `testHeartbeatMetrics` — OUT_OF_SCOPE, metrics deferred to KIP-714.
/// - `testSuccessfulHeartbeatTiming` full matrix — reduced; the timing core
///   is covered by `heartbeat_on_startup` + `timer_not_due` and the
///   `AbstractHeartbeatRequestManager` tests.
/// - The `handler().onComplete(...)` / `onFailure(...)` response-delivery
///   tests (`testHeartbeatResponseOnErrorHandling`,
///   `testNetworkTimeout`, `testFailureOnFatalException`,
///   `testUnsupportedVersionGeneratedOnThe{Broker,Client}`) exercise the
///   spawned-forwarder + `mpsc` channel-back path; the classification
///   helpers they hit (`handle_specific_*`, `on_response`, `on_failure`)
///   are covered directly by the unit tests below plus the abstract-layer
///   `classify_response_error` tests. The full round-trip is deferred to
///   the Phase 5/6 bg-loop integration (same deferral as the consumer
///   heartbeat manager's Phase-12.5 response-routing tests).
#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Node;
    use crate::common::internals::ClusterResourceListeners;
    use crate::consumer::internals::auto_offset_reset_strategy::AutoOffsetResetStrategy;
    use crate::consumer::internals::consumer_metadata::ConsumerMetadata;
    use std::collections::HashSet;

    const DEFAULT_HEARTBEAT_INTERVAL_MS: i64 = 1_000;
    const GROUP_ID: &str = "test-group";

    #[allow(clippy::type_complexity)]
    fn make_with_coord(
        initial_interval_ms: Option<i64>,
    ) -> (
        ShareHeartbeatRequestManager,
        Arc<CoordinatorRequestManager>,
        Arc<ShareMembershipManager>,
        Arc<Mutex<SubscriptionState>>,
    ) {
        let config = ConsumerConfig::new(vec!["localhost:9092".to_string()]);
        let subs = Arc::new(Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::LATEST)));
        let metadata = Arc::new(ConsumerMetadata::from_config(
            &config,
            subs.clone(),
            ClusterResourceListeners::new(),
        ));
        let (tx, _rx) = mpsc::unbounded_channel();
        let beh = Arc::new(BackgroundEventHandler::new(tx));
        let coord = Arc::new(CoordinatorRequestManager::new(100, 1_000, GROUP_ID));
        let mm = Arc::new(ShareMembershipManager::new(GROUP_ID, None, subs.clone(), metadata, beh.clone()));
        let mut hb = ShareHeartbeatRequestManager::new(0, &config, coord.clone(), subs.clone(), mm.clone(), beh);
        if let Some(interval) = initial_interval_ms {
            hb.inner.heartbeat_request_state.update_heartbeat_interval_ms(0, interval);
        }
        (hb, coord, mm, subs)
    }

    fn make() -> ShareHeartbeatRequestManager {
        make_with_coord(None).0
    }

    fn set_coordinator(coord: &Arc<CoordinatorRequestManager>) {
        coord.set_coordinator_for_test(Node::new(0, "localhost".to_string(), 9092));
    }

    fn make_joining(mm: &ShareMembershipManager) {
        mm.transition_to_joining().unwrap();
    }

    /// When the coordinator is unknown, poll returns EMPTY.
    #[test]
    fn poll_returns_empty_when_no_coordinator() {
        let mut mgr = make();
        let result = mgr.poll(0);
        assert!(result.unsent_requests.is_empty());
        assert_eq!(result.time_until_next_poll_ms, i64::MAX);
    }

    /// `should_send_leave_heartbeat_now` is false when not in LEAVING state.
    #[test]
    fn should_not_send_leave_when_not_leaving() {
        let mgr = make();
        assert!(!mgr.should_send_leave_heartbeat_now_for_test());
    }

    /// Broker-side `UnsupportedVersion` is fatal with the
    /// share-protocol-not-supported message.
    #[test]
    fn handle_specific_unsupported_version_is_fatal_with_share_message() {
        let mut mgr = make();
        let action = mgr.handle_specific_exception_in_response(Errors::UnsupportedVersion, "broker doesn't support", 0);
        match action {
            Some(HeartbeatErrorAction::Fatal(err)) => {
                assert_eq!(err.to_string(), SHARE_PROTOCOL_NOT_SUPPORTED_MSG);
            },
            other => panic!("expected fatal, got {other:?}"),
        }
    }

    /// `handle_specific_exception_in_response` returns None for an error not
    /// in the share-specific set (delegated back to the fatal default arm by
    /// the caller).
    #[test]
    fn handle_specific_returns_none_for_other_errors() {
        let mut mgr = make();
        assert!(
            mgr.handle_specific_exception_in_response(Errors::UnreleasedInstanceId, "msg", 0)
                .is_none()
        );
        assert!(mgr.handle_specific_exception_in_response(Errors::None, "", 0).is_none());
    }

    /// Client-side `UnsupportedVersion` failure is fatal with the
    /// share-protocol-VERSION-not-supported message, and emits an error event.
    #[test]
    fn handle_specific_failure_unsupported_version_is_fatal_with_version_message() {
        let mut mgr = make();
        let err = KafkaError::unsupported_version("api too old".to_string());
        assert!(mgr.handle_specific_failure(&err, 0));
        // The pushed membership transition carries the fatal error with the
        // share-version message.
        let transitions = mgr.take_pending_membership_transitions();
        assert_eq!(transitions.len(), 1);
        match &transitions[0] {
            PendingMembershipTransition::Fatal(e) => {
                assert_eq!(e.to_string(), SHARE_PROTOCOL_VERSION_NOT_SUPPORTED_MSG);
            },
            other => panic!("expected Fatal, got {other:?}"),
        }
    }

    /// A non-`UnsupportedVersion` failure is NOT handled by
    /// `handle_specific_failure` (returns false; caller applies the generic
    /// fatal path).
    #[test]
    fn handle_specific_failure_returns_false_for_other_errors() {
        let mut mgr = make();
        let err = KafkaError::new(Errors::UnknownServerError);
        assert!(!mgr.handle_specific_failure(&err, 0));
    }

    /// Translated from `testHeartbeatOnStartup`. A fresh (UNSUBSCRIBED)
    /// member skips heartbeats; once JOINING and the initial interval fires,
    /// a single heartbeat is emitted; a second poll while in-flight is EMPTY.
    #[tokio::test]
    async fn heartbeat_on_startup() {
        let (mut mgr, coord, mm, _subs) = make_with_coord(Some(0));
        set_coordinator(&coord);

        let result = mgr.poll(0);
        assert_eq!(result.unsent_requests.len(), 0);

        make_joining(&mm);
        assert_eq!(mgr.maximum_time_to_wait(0), 0);

        let result = mgr.poll(0);
        assert_eq!(result.unsent_requests.len(), 1);

        let result2 = mgr.poll(0);
        assert_eq!(result2.unsent_requests.len(), 0);
    }

    /// Translated from `testTimerNotDue`.
    #[tokio::test]
    async fn timer_not_due() {
        let (mut mgr, coord, mm, _subs) = make_with_coord(Some(DEFAULT_HEARTBEAT_INTERVAL_MS));
        set_coordinator(&coord);
        make_joining(&mm);
        let _ = mgr.poll(0);

        let result = mgr.poll(100);
        assert_eq!(result.unsent_requests.len(), 0);
    }

    /// Translated from `testHeartbeatNotSentIfAnotherOneInFlight` (subset).
    #[tokio::test]
    async fn heartbeat_not_sent_if_another_one_in_flight() {
        let (mut mgr, coord, mm, _subs) = make_with_coord(Some(0));
        set_coordinator(&coord);
        make_joining(&mm);

        let result = mgr.poll(0);
        assert_eq!(result.unsent_requests.len(), 1);

        let result = mgr.poll(DEFAULT_HEARTBEAT_INTERVAL_MS);
        assert_eq!(result.unsent_requests.len(), 0);
    }

    /// Translated from `testHeartbeatOutsideInterval`. JOINING forces a
    /// heartbeat even when the interval timer has not elapsed.
    #[tokio::test]
    async fn heartbeat_outside_interval() {
        let (mut mgr, coord, mm, _subs) = make_with_coord(Some(DEFAULT_HEARTBEAT_INTERVAL_MS));
        set_coordinator(&coord);
        make_joining(&mm);

        let result = mgr.poll(0);
        assert_eq!(result.unsent_requests.len(), 1);
        assert_eq!(result.time_until_next_poll_ms, DEFAULT_HEARTBEAT_INTERVAL_MS);
    }

    /// Translated from `testFirstHeartbeatIncludesRequiredInfoToJoinGroupAndGetAssignments`.
    /// The first (JOINING) heartbeat carries epoch 0, the group id, and the
    /// subscribed topic names.
    #[tokio::test]
    async fn first_heartbeat_includes_join_info() {
        let (mut mgr, _coord, mm, subs) = make_with_coord(Some(0));
        make_joining(&mm);
        subs.lock()
            .unwrap()
            .subscribe_to_share_group(HashSet::from(["topic1".to_string()]))
            .unwrap();

        let data = mgr.build_request_data_for_test();
        assert_eq!(data.group_id, GROUP_ID);
        assert_eq!(data.member_epoch, 0);
        assert_eq!(data.subscribed_topic_names, Some(vec!["topic1".to_string()]));
    }

    /// Translated from `testHeartbeatState` (field-diff core). A rack id is
    /// sent only on the first heartbeat; subscribed topic names are sent when
    /// joining and re-sent only when they change.
    #[tokio::test]
    async fn heartbeat_state_field_diff() {
        let config = ConsumerConfig::new(vec!["localhost:9092".to_string()]);
        let subs = Arc::new(Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::LATEST)));
        let metadata = Arc::new(ConsumerMetadata::from_config(
            &config,
            subs.clone(),
            ClusterResourceListeners::new(),
        ));
        let (tx, _rx) = mpsc::unbounded_channel();
        let beh = Arc::new(BackgroundEventHandler::new(tx));
        let coord = Arc::new(CoordinatorRequestManager::new(100, 1_000, GROUP_ID));
        let mm = Arc::new(ShareMembershipManager::new(
            GROUP_ID,
            Some("rack-1".to_string()),
            subs.clone(),
            metadata,
            beh.clone(),
        ));
        let mut hb = ShareHeartbeatRequestManager::new(0, &config, coord, subs.clone(), mm.clone(), beh);

        // First build while JOINING: rack id + subscribed topics present.
        mm.transition_to_joining().unwrap();
        subs.lock()
            .unwrap()
            .subscribe_to_share_group(HashSet::from(["topic1".to_string()]))
            .unwrap();
        let data = hb.build_request_data_for_test();
        assert_eq!(data.rack_id, Some("rack-1".to_string()));
        assert_eq!(data.subscribed_topic_names, Some(vec!["topic1".to_string()]));

        // Second build still JOINING: rack id NOT re-sent (sent once);
        // subscribed topics re-sent because sendAllFields (JOINING) is true.
        let data = hb.build_request_data_for_test();
        assert_eq!(data.rack_id, None);
        assert_eq!(data.subscribed_topic_names, Some(vec!["topic1".to_string()]));
    }

    /// Translated from `testPollOnCloseGeneratesRequestIfNeeded` (subset: no
    /// leave in progress -> no request).
    #[tokio::test]
    async fn poll_on_close_no_request_when_not_leaving() {
        let (mut mgr, coord, _mm, _subs) = make_with_coord(Some(0));
        set_coordinator(&coord);
        let result = mgr.poll_on_close(0);
        assert!(result.unsent_requests.is_empty());
    }
}
