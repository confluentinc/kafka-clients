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

//! `ConsumerHeartbeatRequestManager` — KIP-848 specialization of the
//! heartbeat lifecycle. Composes
//! [`super::abstract_heartbeat_request_manager::AbstractHeartbeatRequestManager`]
//! and supplies the consumer-group-specific request builder + error
//! classification.
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.ConsumerHeartbeatRequestManager`.

#![allow(dead_code)]

use std::sync::Arc;
use std::sync::Mutex;

use tokio::sync::mpsc;

use crate::common::KafkaError;
use crate::common::Uuid;
use crate::common::protocol::Errors;
use crate::common::requests::ConcreteResponse;
use crate::common::requests::consumer_group_heartbeat_request::{
    ConsumerGroupHeartbeatRequestBuilder, REGEX_RESOLUTION_NOT_SUPPORTED_MSG,
};
use crate::common::requests::consumer_group_heartbeat_response::ConsumerGroupHeartbeatResponse;
use crate::consumer::ConsumerConfig;
use crate::consumer::internals::events::background_event::BackgroundEvent;
use crate::consumer::internals::events::background_event_handler::BackgroundEventHandler;
use crate::consumer_group_heartbeat_request_data::{
    ConsumerGroupHeartbeatRequestData, TopicPartitions as RequestTopicPartitions,
};

use super::abstract_heartbeat_request_manager::{
    AbstractHeartbeatRequestManager, CONSUMER_PROTOCOL_NOT_SUPPORTED_MSG, HeartbeatErrorAction, HeartbeatFailureAction,
    make_heartbeat_poll_result,
};
use super::abstract_membership_manager::LocalAssignment;
use super::consumer_membership_manager::ConsumerMembershipManager;
use super::coordinator_request_manager::CoordinatorRequestManager;
use super::member_state::MemberState;
use super::network_client_delegate::{PollResult, UnsentRequest};
use super::request_manager::RequestManager;
use super::subscription_state::SubscriptionState;

/// Envelope for routing a `ConsumerGroupHeartbeatResponse` (or its
/// transport-level failure) from the spawned response forwarder back
/// to the heartbeat manager's next `poll(now)` call.
///
/// Mirrors the Java `AbstractHeartbeatRequestManager.makeHeartbeatRequest`
/// `whenComplete((response, exception) -> { onResponse / onFailure })`
/// dispatch (`AbstractHeartbeatRequestManager.java:292-310`), but
/// defers the state-update to the next bg-task `poll()` cycle. The
/// rationale (per Phase 12.5 PLAN §"2. consumer_heartbeat_request_manager"
/// "Structural decision"): the success path calls
/// `membership_manager.on_heartbeat_success(...)`, and the membership
/// manager owns its own `Arc<Mutex<...>>`. Channel-back ensures no
/// heartbeat-side guard is held when the cross-RM call fires
/// (consumer-threading.md §16).
pub(crate) enum PendingHeartbeatCompletion {
    /// Broker returned a `ConsumerGroupHeartbeatResponse`. The forwarder
    /// captures the response body and the completion time; the drain
    /// applies success/error classification on `&mut self` inside the
    /// next `poll(now)`.
    Response {
        response: ConsumerGroupHeartbeatResponse,
        completion_time_ms: i64,
    },
    /// Transport-level failure (network error, in-flight cancellation,
    /// type mismatch on the response body). The drain calls
    /// `inner.on_failure(...)` and `membership_manager.on_heartbeat_failure(retriable)`.
    Failure { error: KafkaError, completion_time_ms: i64 },
}

/// Side-channel envelope emitted by [`ConsumerHeartbeatRequestManager`]
/// when the response classifier yields a `Fenced` or `Fatal` outcome.
/// The heartbeat manager's `poll(now)` is sync, but
/// `ConsumerMembershipManager::transition_to_fenced` /
/// `transition_to_fatal` are `async` (they await §31
/// `onPartitionsLost` listener callbacks). The bg-task drains this
/// channel from the heartbeat handle after `entries().poll(now)` and
/// `await`s the appropriate transition on `self.membership`.
///
/// Mirrors Java's `AbstractHeartbeatRequestManager.java:415,424`
/// (`membershipManager().transitionToFenced();` synchronous inside the
/// `whenComplete` lambda) and `AbstractHeartbeatRequestManager.java:457`
/// (`handleFatalFailure` → `membershipManager().transitionToFatal();`).
/// Rust splits the "advise the membership manager" half off because the
/// membership transition is `async` and `poll` is sync.
#[derive(Debug)]
pub(crate) enum PendingMembershipTransition {
    /// Broker returned `FENCED_MEMBER_EPOCH` or `UNKNOWN_MEMBER_ID`.
    /// The bg-task drives
    /// `ConsumerMembershipManager::transition_to_fenced(now).await`.
    Fenced,
    /// Fatal heartbeat outcome. Carries the error for logging /
    /// debugging; the membership-side `transition_to_fatal(now)` does
    /// not consume the error itself but the surrounding bg-task may
    /// log it. The fatal `BackgroundEvent::Error` envelope is emitted
    /// to the user separately at the call site that pushes this
    /// transition.
    Fatal(KafkaError),
}

/// Tracks which fields were sent on the most recent heartbeat. Java's
/// `HeartbeatState.SentFields` private inner class. We omit fields only
/// changed in unscoped paths (rebalance timeout, regex pattern) on a
/// per-request basis to keep the protocol compact.
#[derive(Default)]
struct SentFields {
    rebalance_timeout_ms: i32,
    /// Topic names sorted; `None` means "not yet sent".
    subscribed_topic_names: Option<Vec<String>>,
    pattern: Option<String>,
    server_assignor: Option<String>,
    local_assignment: Option<LocalAssignment>,
}

impl SentFields {
    fn new() -> Self {
        Self { rebalance_timeout_ms: -1, ..Default::default() }
    }

    fn reset(&mut self) {
        self.subscribed_topic_names = None;
        self.rebalance_timeout_ms = -1;
        self.server_assignor = None;
        self.local_assignment = None;
        self.pattern = None;
    }
}

/// State for building `ConsumerGroupHeartbeatRequest`s with field
/// diffing. Mirrors Java's `ConsumerHeartbeatRequestManager.HeartbeatState`.
struct HeartbeatState {
    subscriptions: Arc<Mutex<SubscriptionState>>,
    membership_manager: Arc<ConsumerMembershipManager>,
    rebalance_timeout_ms: i32,
    sent_fields: SentFields,
}

impl HeartbeatState {
    fn new(
        subscriptions: Arc<Mutex<SubscriptionState>>,
        membership_manager: Arc<ConsumerMembershipManager>,
        rebalance_timeout_ms: i32,
    ) -> Self {
        Self {
            subscriptions,
            membership_manager,
            rebalance_timeout_ms,
            sent_fields: SentFields::new(),
        }
    }

    fn reset(&mut self) {
        self.sent_fields.reset();
    }

    /// Java: `buildRequestData()`. Constructs the request data with
    /// field-level diffing so subsequent heartbeats only include
    /// changed fields.
    fn build_request_data(&mut self) -> ConsumerGroupHeartbeatRequestData {
        let mut data = ConsumerGroupHeartbeatRequestData::new();

        // GroupId - always sent.
        let group_id = self.membership_manager.group_id();
        data.set_group_id(group_id);

        // MemberId - always sent.
        data.set_member_id(self.membership_manager.member_id());

        // MemberEpoch - always sent.
        data.set_member_epoch(self.membership_manager.member_epoch());

        // InstanceId - set if present.
        if let Some(instance_id) = self.membership_manager.group_instance_id() {
            data.set_instance_id(Some(instance_id.to_string()));
        }

        let state = self.membership_manager.state();
        let send_all_fields = state == MemberState::Joining;

        // RebalanceTimeoutMs.
        if send_all_fields || self.sent_fields.rebalance_timeout_ms != self.rebalance_timeout_ms {
            data.set_rebalance_timeout_ms(self.rebalance_timeout_ms);
            self.sent_fields.rebalance_timeout_ms = self.rebalance_timeout_ms;
        }

        // SubscribedTopicNames - sorted set for stable comparison.
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

        // SubscribedTopicRegex.
        let current_pattern = {
            let subs = match self.subscriptions.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            subs.subscription_pattern().map(|p| p.pattern().to_string())
        };
        let pattern_changed = current_pattern != self.sent_fields.pattern;
        if (send_all_fields && current_pattern.is_some()) || pattern_changed {
            data.set_subscribed_topic_regex(current_pattern.clone().or(Some(String::new())));
            self.sent_fields.pattern = current_pattern;
        }

        // ServerAssignor.
        if let Some(server_assignor) = self.membership_manager.server_assignor() {
            let changed = match &self.sent_fields.server_assignor {
                None => true,
                Some(prev) => prev != server_assignor,
            };
            if send_all_fields || changed {
                data.set_server_assignor(Some(server_assignor.to_string()));
                self.sent_fields.server_assignor = Some(server_assignor.to_string());
            }
        }

        // TopicPartitions - sent on join, or when local assignment
        // changed.
        let current_local = self.membership_manager.current_assignment();
        let assignment_changed = match &self.sent_fields.local_assignment {
            None => true,
            Some(prev) => prev != &current_local,
        };
        if send_all_fields || assignment_changed {
            data.set_topic_partitions(Some(build_topic_partitions_list(&current_local.partitions)));
            self.sent_fields.local_assignment = Some(current_local);
        }

        // RackId - on join.
        if send_all_fields {
            data.set_rack_id(self.membership_manager.rack_id().map(|s| s.to_string()));
        }
        data
    }
}

fn build_topic_partitions_list(partitions: &std::collections::HashMap<Uuid, Vec<i32>>) -> Vec<RequestTopicPartitions> {
    partitions
        .iter()
        .map(|(topic_id, ps)| {
            let mut tp = RequestTopicPartitions::new();
            tp.set_topic_id(*topic_id);
            let mut sorted = ps.clone();
            sorted.sort();
            tp.set_partitions(sorted);
            tp
        })
        .collect()
}

/// KIP-848 consumer-group heartbeat request manager. Composes
/// [`AbstractHeartbeatRequestManager`].
///
/// Java: `ConsumerHeartbeatRequestManager extends AbstractHeartbeatRequestManager<ConsumerGroupHeartbeatResponse>`.
pub(crate) struct ConsumerHeartbeatRequestManager {
    inner: AbstractHeartbeatRequestManager,
    membership_manager: Arc<ConsumerMembershipManager>,
    heartbeat_state: HeartbeatState,
    /// Cloned into each spawned response forwarder so the forwarder
    /// can route the heartbeat response back through `poll(now)`'s
    /// drain step. See [`PendingHeartbeatCompletion`] for the
    /// rationale (channel-back avoids §16 violations on the
    /// `membership_manager` cross-call).
    pending_completion_tx: mpsc::UnboundedSender<PendingHeartbeatCompletion>,
    /// Drained by [`Self::drain_pending_completions`] at the top of
    /// every `poll(now)` call. Held directly (no outer `Mutex`)
    /// because the heartbeat manager is single-owner — only the
    /// bg-task `poll(now)` cycle touches it. (`mpsc::UnboundedReceiver`
    /// is `Send` but not `Sync`; single-ownership keeps it sound.)
    pending_completion_rx: mpsc::UnboundedReceiver<PendingHeartbeatCompletion>,
    /// Sender for [`PendingMembershipTransition`]. The heartbeat-side
    /// `on_response` / `on_failure` pushes an envelope here whenever
    /// the classifier yields `Fenced` or `Fatal`. The bg-task drains
    /// via [`Self::take_pending_membership_transitions`] after
    /// `entries().poll(now)` and `await`s the matching
    /// `ConsumerMembershipManager::transition_to_*` call on
    /// `self.membership`.
    pending_membership_transition_tx: mpsc::UnboundedSender<PendingMembershipTransition>,
    /// Drained by [`Self::take_pending_membership_transitions`].
    /// Same single-owner constraint as `pending_completion_rx`.
    pending_membership_transition_rx: mpsc::UnboundedReceiver<PendingMembershipTransition>,
}

impl ConsumerHeartbeatRequestManager {
    /// Java: `ConsumerHeartbeatRequestManager(LogContext, Time, ConsumerConfig,
    /// CoordinatorRequestManager, SubscriptionState, ConsumerMembershipManager,
    /// BackgroundEventHandler, Metrics)`.
    pub(crate) fn new(
        current_time_ms: i64,
        config: &ConsumerConfig,
        coordinator_request_manager: Arc<CoordinatorRequestManager>,
        subscriptions: Arc<Mutex<SubscriptionState>>,
        membership_manager: Arc<ConsumerMembershipManager>,
        background_event_handler: Arc<BackgroundEventHandler>,
    ) -> Self {
        let inner = AbstractHeartbeatRequestManager::new(
            current_time_ms,
            config,
            coordinator_request_manager,
            background_event_handler,
        );
        let rebalance_timeout_ms = membership_manager.rebalance_timeout_ms;
        let heartbeat_state = HeartbeatState::new(subscriptions, membership_manager.clone(), rebalance_timeout_ms);
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

    /// Drain the [`PendingMembershipTransition`] side-channel. Called
    /// by the bg-task immediately after `entries().poll(now)` has run
    /// (so the heartbeat's own drain has had a chance to classify any
    /// pending responses) and BEFORE `membership.reconcile(now).await`
    /// (so the membership state machine observes the fence / fatal
    /// before reconciliation runs).
    ///
    /// The bg-task then `await`s each transition via
    /// `ConsumerMembershipManager::transition_to_fenced(now)` /
    /// `transition_to_fatal(now)`.
    ///
    /// Mirrors Java's `AbstractHeartbeatRequestManager.java:415,424`
    /// (`membershipManager().transitionToFenced();`) and `:457`
    /// (`handleFatalFailure` → `membershipManager().transitionToFatal();`).
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

    /// Java: `shouldSendLeaveHeartbeatNow()`.
    fn should_send_leave_heartbeat_now(&self) -> bool {
        use crate::consumer::close_options::GroupMembershipOperation;
        if self.membership_manager.group_instance_id().is_none()
            && matches!(
                self.membership_manager.leave_group_operation(),
                GroupMembershipOperation::RemainInGroup
            )
        {
            return false;
        }
        self.membership_manager.state() == MemberState::Leaving
    }

    /// Java: `buildHeartbeatRequest()`. Returns an `UnsentRequest`
    /// targeting the current coordinator node. Spawns a forwarder
    /// that awaits the response receiver and routes the result back
    /// to [`Self::drain_pending_completions`] via the
    /// [`PendingHeartbeatCompletion`] channel — mirroring Java's
    /// `AbstractHeartbeatRequestManager.makeHeartbeatRequest`'s
    /// `whenComplete((response, exception) -> ...)` lambda
    /// (`AbstractHeartbeatRequestManager.java:295-304`).
    ///
    /// If `ignore_response` is true (Java parity at line 309 — the
    /// LEAVING / poll-timer-expired path), the forwarder drops the
    /// completion silently instead of enqueueing it. Java's
    /// `logResponse(request)` path also drops the response side-effect
    /// without driving state machinery.
    fn build_heartbeat_request(&mut self, ignore_response: bool) -> UnsentRequest {
        let data = self.heartbeat_state.build_request_data();
        let builder = Box::new(ConsumerGroupHeartbeatRequestBuilder::new(data));
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
                    Some(ConcreteResponse::ConsumerGroupHeartbeat(resp)) => {
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
            // `ignore_response` mirrors Java's `logResponse(request)`
            // path (`AbstractHeartbeatRequestManager.java:307-319`):
            // log the outcome but do NOT drive the consumer state
            // machinery. We drop the envelope outright; the response
            // future itself was already resolved (the forwarder
            // observed the result), so any callers waiting on it have
            // already been unblocked.
            if !ignore_response {
                // Receiver lives as long as the heartbeat manager;
                // ignore the send error in case the manager has been
                // dropped during a shutdown race.
                let _ = tx.send(completion);
            }
        });
        unsent
    }

    /// Drains the [`PendingHeartbeatCompletion`] mpsc channel into the
    /// abstract base + the membership manager. Called at the top of
    /// [`RequestManager::poll`].
    ///
    /// Java reference: the `whenComplete` lambda body in
    /// `AbstractHeartbeatRequestManager.makeHeartbeatRequest` —
    /// `onResponse(...)` (success) / `onFailure(...)` (failure). The
    /// Java code runs the lambda on the network-IO thread; Rust runs
    /// the equivalent work here on the bg-task to keep all `&mut
    /// self` access serialized through `poll(now)` and to keep the
    /// cross-RM `membership_manager.on_heartbeat_*` call out of any
    /// heartbeat-side guard (`consumer-threading.md` §16).
    ///
    /// **§16 audit**: between draining the channel and the cross-RM
    /// `membership_manager.on_heartbeat_*` call, NO `Mutex::lock()`
    /// invocation is made. The membership manager's own `Mutex` is
    /// acquired only inside its `on_heartbeat_success` /
    /// `on_heartbeat_failure` methods.
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

    /// Dispatch a `ConsumerGroupHeartbeatResponse` body to the
    /// success or error path. Mirrors Java's `onResponse(R response,
    /// long currentTimeMs)` (`AbstractHeartbeatRequestManager.java:340-348`).
    fn on_response(&mut self, response: &ConsumerGroupHeartbeatResponse, completion_time_ms: i64) {
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
        // Error response — Java's `onErrorResponse` resets the
        // per-request `HeartbeatState` (the `SentFields` field tracker)
        // at the TOP, before classifying. Without this, the next
        // heartbeat's `build_request_data()` would diff against stale
        // "sent" tracking and SKIP fields (subscribed topic names,
        // rebalance timeout, server assignor, local assignment, pattern)
        // that Java would re-send. The broker would then assume the
        // consumer is still using stale subscription state.
        //
        // Java reference: `AbstractHeartbeatRequestManager.java:356`
        // (`resetHeartbeatState();` runs at the top of `onErrorResponse`,
        // before `heartbeatRequestState.onFailedAttempt(currentTimeMs)`
        // and the per-error switch).
        self.reset_heartbeat_state();
        // Classify via the abstract dispatch first, then delegate to
        // the consumer-specific extras.
        let error_message = format!("{error:?}");
        let action = self.inner.classify_response_error(error, &error_message, completion_time_ms);
        let final_action = match action {
            HeartbeatErrorAction::DelegateToSpecific => self
                .handle_specific_exception_in_response(error, &error_message, completion_time_ms)
                .unwrap_or_else(|| {
                    // Java: `AbstractHeartbeatRequestManager.java:435-441` —
                    // the `default:` arm of `onErrorResponse`'s switch
                    // calls `handleSpecificExceptionInResponse(...)`; if
                    // that returns false (no consumer-specific handler
                    // matched), Java falls back to
                    // `handleFatalFailure(error.exception(errorMessage))`.
                    // Rust's mapping: `None` from the specific handler
                    // means "no match" — fall through to the same fatal
                    // path so unknown / future error codes don't get
                    // silently swallowed.
                    log::error!(
                        "ConsumerGroupHeartbeatRequest failed due to unexpected error {:?}: {}",
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
                // Java: `membershipManager().transitionToFenced()`
                // (`AbstractHeartbeatRequestManager.java:411-427` —
                // FENCED_MEMBER_EPOCH and UNKNOWN_MEMBER_ID arms).
                // The fence is treated as an INTERNAL state-machine
                // event: Java does NOT call
                // `backgroundEventHandler.add(new ErrorEvent(...))`
                // here (compare with `handleFatalFailure` at
                // `:455-458` which does emit an ErrorEvent). The
                // member transitions through FENCED → JOINING and
                // re-joins silently; the user never sees the fence
                // from `poll()`. Emitting a `BackgroundEvent::Error`
                // would surface `KafkaError::FencedMemberEpoch` to
                // the user, diverging from Java which returns
                // `ConsumerRecords::empty()` and rejoins
                // transparently.
                //
                // The Rust `transition_to_fenced` is `async` (it
                // awaits the §31 onPartitionsLost listener); we
                // can't `.await` from sync `poll(now)`. Instead we
                // emit a `PendingMembershipTransition::Fenced`
                // envelope onto the side-channel — the bg-task
                // drains it via
                // `take_pending_membership_transitions()` after
                // `entries().poll(now)` and BEFORE
                // `membership.reconcile(now).await`. See
                // [`PendingMembershipTransition`] for the rationale.
                //
                // Receiver lives as long as the heartbeat manager —
                // ignore the send error during shutdown races.
                let _ = self.pending_membership_transition_tx.send(PendingMembershipTransition::Fenced);
            },
            HeartbeatErrorAction::Fatal(err) => {
                // Java: `handleFatalFailure(error.exception(...))`
                // (`AbstractHeartbeatRequestManager.java:455-458`) —
                // emits an `ErrorEvent` AND calls
                // `membershipManager().transitionToFatal()`. The
                // Rust `transition_to_fatal` is `async` (it awaits
                // the §31 onPartitionsLost listener); we can't
                // `.await` from sync `poll(now)`. Push a
                // `PendingMembershipTransition::Fatal(...)`
                // envelope; the bg-task drains and `await`s the
                // transition after `entries().poll(now)`.
                let _ = self
                    .inner
                    .background_event_handler
                    .add(BackgroundEvent::Error { error: err.clone() }, completion_time_ms);
                let _ = self
                    .pending_membership_transition_tx
                    .send(PendingMembershipTransition::Fatal(err));
            },
            HeartbeatErrorAction::DelegateToSpecific => {
                // Already handled above; this arm is unreachable
                // because we unwrapped to `Handled` if the specific
                // handler returned `None`.
                debug_assert!(false, "DelegateToSpecific should have been resolved");
            },
        }
        // Java: `membershipManager().onHeartbeatFailure(false)` at the
        // tail of `onErrorResponse`. The Rust `on_heartbeat_failure`
        // mutates membership state internally — no further async
        // transition is required at this point.
        self.membership_manager.on_heartbeat_failure(false);
    }

    /// Transport-level / non-response failure handler. Mirrors Java's
    /// `onFailure(Throwable, long)`
    /// (`AbstractHeartbeatRequestManager.java:321-338`).
    fn on_failure(&mut self, error: &KafkaError, completion_time_ms: i64) {
        // Java: `resetHeartbeatState()` at the top of `onFailure`.
        self.reset_heartbeat_state();
        let abstract_action = self.inner.on_failure(error, completion_time_ms);
        let retriable = matches!(abstract_action, HeartbeatFailureAction::Retriable);
        if !retriable {
            // Java: `handleSpecificFailure(exception)` runs only on
            // the non-retriable path. If it returns false, fall back
            // to `handleFatalFailure(exception)`.
            let specific_handled = self.handle_specific_failure(error, completion_time_ms);
            if !specific_handled {
                log::error!("ConsumerGroupHeartbeatRequest failed due to fatal error: {}", error);
                // Java: `handleFatalFailure(exception)`
                // (`AbstractHeartbeatRequestManager.java:455-458`) —
                // emits an `ErrorEvent` AND calls
                // `membershipManager().transitionToFatal()`. Both
                // are mirrored in Rust:
                //   1. emit `BackgroundEvent::Error` for the user.
                //   2. push `PendingMembershipTransition::Fatal(...)`
                //      onto the side-channel; the bg-task drains and
                //      `await`s `transition_to_fatal(now)` after
                //      `entries().poll(now)`. The membership
                //      transition is `async` (it awaits §31's
                //      onPartitionsLost listener) so we can't
                //      `.await` from sync `poll`.
                let _ = self
                    .inner
                    .background_event_handler
                    .add(BackgroundEvent::Error { error: error.clone() }, completion_time_ms);
                let _ = self
                    .pending_membership_transition_tx
                    .send(PendingMembershipTransition::Fatal(error.clone()));
            }
        }
        // Java: `membershipManager().onHeartbeatFailure(retriable)`
        // at the tail of `onFailure`.
        self.membership_manager.on_heartbeat_failure(retriable);
    }

    /// Java: `handleSpecificFailure(Throwable exception)`. The Consumer
    /// variant maps `UnsupportedVersionException` carrying the regex
    /// resolution message to a fatal failure with the special-cased
    /// message.
    ///
    /// `current_time_ms` is threaded through to
    /// [`BackgroundEventHandler::add`] so the resulting `ErrorEvent` is
    /// attributed to the actual failure time rather than epoch zero.
    pub(crate) fn handle_specific_failure(&mut self, error: &crate::common::KafkaError, current_time_ms: i64) -> bool {
        use crate::common::KafkaError;
        use crate::common::protocol::Errors;
        if error.error() == Errors::UnsupportedVersion {
            let msg = error.to_string();
            let message = if msg.contains(REGEX_RESOLUTION_NOT_SUPPORTED_MSG) {
                REGEX_RESOLUTION_NOT_SUPPORTED_MSG
            } else {
                CONSUMER_PROTOCOL_NOT_SUPPORTED_MSG
            };
            log::error!("ConsumerGroupHeartbeatRequest failed due to unsupported version: {message}");
            let fatal_err = KafkaError::unsupported_version(message.to_string());
            // Java (`ConsumerHeartbeatRequestManager.java:109`):
            // `handleFatalFailure(new UnsupportedVersionException(message, exception));`
            // i.e. emits ErrorEvent AND calls
            // `membershipManager().transitionToFatal()`. Mirror both.
            let _ = self.inner.background_event_handler.add(
                crate::consumer::internals::events::background_event::BackgroundEvent::Error {
                    error: fatal_err.clone(),
                },
                current_time_ms,
            );
            let _ = self
                .pending_membership_transition_tx
                .send(PendingMembershipTransition::Fatal(fatal_err));
            return true;
        }
        false
    }

    /// Wrap the shared `classify_response_error` dispatch with the
    /// Consumer-specific extras (UNSUPPORTED_VERSION, UNRELEASED_INSTANCE_ID,
    /// FENCED_INSTANCE_ID).
    pub(crate) fn handle_specific_exception_in_response(
        &mut self,
        error: crate::common::protocol::Errors,
        error_message: &str,
        _current_time_ms: i64,
    ) -> Option<HeartbeatErrorAction> {
        use crate::common::KafkaError;
        use crate::common::protocol::Errors;
        match error {
            Errors::UnsupportedVersion => {
                log::error!(
                    "ConsumerGroupHeartbeatRequest failed due to unsupported version response on broker side: {}",
                    CONSUMER_PROTOCOL_NOT_SUPPORTED_MSG
                );
                Some(HeartbeatErrorAction::Fatal(KafkaError::unsupported_version(
                    CONSUMER_PROTOCOL_NOT_SUPPORTED_MSG.to_string(),
                )))
            },
            Errors::UnreleasedInstanceId => {
                log::error!(
                    "ConsumerGroupHeartbeatRequest failed due to unreleased instance id: {}",
                    error_message
                );
                Some(HeartbeatErrorAction::Fatal(KafkaError::with_message(
                    error,
                    error_message.to_string(),
                )))
            },
            Errors::FencedInstanceId => {
                log::error!(
                    "ConsumerGroupHeartbeatRequest failed due to fenced instance id: {}",
                    error_message
                );
                Some(HeartbeatErrorAction::Fatal(KafkaError::with_message(
                    error,
                    error_message.to_string(),
                )))
            },
            _ => None,
        }
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

    /// Returns the wrapped membership manager.
    pub(crate) fn membership_manager(&self) -> &Arc<ConsumerMembershipManager> {
        &self.membership_manager
    }

    /// Test-only accessor: returns `true` when the per-request
    /// `SentFields` tracker has `subscribed_topic_names` populated (i.e.
    /// the last build did NOT reset it). Used by the Issue-3 regression
    /// to verify that `reset_heartbeat_state()` runs at the top of the
    /// error branch of `on_response`.
    #[cfg(test)]
    pub(crate) fn sent_fields_topics_populated(&self) -> bool {
        self.heartbeat_state.sent_fields.subscribed_topic_names.is_some()
    }
}

impl RequestManager for ConsumerHeartbeatRequestManager {
    /// Java: `poll(long currentTimeMs)`. Mirrors
    /// `AbstractHeartbeatRequestManager.poll(...)` phase-for-phase.
    ///
    /// Per `consumer-threading.md` §10, `poll` is sync. The membership
    /// state machine's async transitions (fenced, fatal, reconcile)
    /// happen in the bg-task path (Phase 10) which can `.await`;
    /// `poll` only computes whether a heartbeat needs to be sent.
    fn poll(&mut self, current_time_ms: i64) -> PollResult {
        // 0. Drain any pending heartbeat completions from prior
        // spawned forwarders. State-update happens before the next
        // emission decision so `should_heartbeat_now` /
        // `request_in_flight` observe the post-completion state.
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
            // Java: membershipManager().onHeartbeatRequestSkipped().
            // We ignore the error: this only mutates state on LEAVING.
            let _ = self.membership_manager.abstract_mm.on_heartbeat_request_skipped();
            self.inner.maybe_propagate_coordinator_fatal_error_event(current_time_ms);
            return PollResult::empty();
        }

        // 2. Poll-timer-expired stale path. Java calls
        // `pollTimer.update(currentTimeMs)` here; the Rust deadline-based
        // timer doesn't need the update — `poll_timer_is_expired` reads
        // `current_time_ms` directly.
        if self.inner.poll_timer_is_expired(current_time_ms) && !self.membership_manager.is_leaving_group() {
            log::warn!(
                "Consumer poll timeout has expired. This means the time between subsequent calls to poll() was longer than the configured max.poll.interval.ms."
            );
            // Java: membershipManager().transitionToSendingLeaveGroup(true).
            if let Err(e) = self.membership_manager.transition_to_sending_leave_group(true) {
                log::warn!("transition_to_sending_leave_group failed: {}", e);
                return PollResult::empty();
            }
            // Build leave heartbeat (ignoreResponse=true) per Java's
            // `AbstractHeartbeatRequestManager.java:309`.
            let request = self.build_heartbeat_request(true);
            self.inner.heartbeat_request_state.reset();
            self.reset_heartbeat_state();
            return PollResult::new(self.inner.heartbeat_request_state.heartbeat_interval_ms(), vec![request]);
        }

        // 3. Decide whether to heartbeat now.
        let heartbeat_now = self.should_send_leave_heartbeat_now() || {
            let inner = self.membership_manager.abstract_mm.inner.lock();
            let guard = match inner {
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
        // Java: record send attempt, reset timer, increment metrics.
        // membershipManager().onHeartbeatRequestGenerated() advances
        // ACKNOWLEDGING / LEAVING / etc.
        if let Err(e) = self.membership_manager.abstract_mm.on_heartbeat_request_generated() {
            log::warn!("on_heartbeat_request_generated failed: {}", e);
        }
        make_heartbeat_poll_result(request, &mut self.inner, current_time_ms)
    }

    fn poll_on_close(&mut self, _current_time_ms: i64) -> PollResult {
        // Drain any pending completions one last time so close paths
        // observe the post-completion state.
        self.drain_pending_completions(_current_time_ms);
        // Java: if (membershipManager().isLeavingGroup()) send the
        // leave heartbeat (ignoreResponse=true — pollOnClose drops
        // the response by Java's `logResponse(...)` semantics).
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
            let inner = self.membership_manager.abstract_mm.inner.lock();
            let guard = match inner {
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

/// Translation notes on Java test coverage (`ConsumerHeartbeatRequestManagerTest`):
///
/// Translated (12 / 31):
/// - `poll_returns_empty_when_no_coordinator` — Java: `testSkippingHeartbeat`
/// - `should_not_send_leave_when_not_leaving` — Java: internal `shouldSendLeaveHeartbeatNow` shape
/// - `handle_specific_unsupported_version_is_fatal` — Java: `testHeartbeatResponseOnErrorHandling` (UnsupportedVersion)
/// - `handle_specific_fenced_instance_id_is_fatal` — Java: `testHeartbeatResponseOnErrorHandling` (FencedInstanceId)
/// - `handle_specific_returns_none_for_other_errors` (no Java analog; behavior pin)
/// - `maximum_time_to_wait_returns_zero_when_poll_timer_expired`
/// - `heartbeat_on_startup` — Java: `testHeartbeatOnStartup`
/// - `timer_not_due` — Java: `testTimerNotDue`
/// - `heartbeat_not_sent_if_another_one_in_flight` — Java: `testHeartbeatNotSentIfAnotherOneInFlight` (subset)
/// - `heartbeat_outside_interval` — Java: `testHeartbeatOutsideInterval`
/// - `handle_specific_unreleased_instance_id_is_fatal` — Java: error-matrix `UnreleasedInstanceId` row
/// - `handle_specific_failure_unsupported_version_emits_error_event` — Java: `testHeartbeatHandleSpecificFailureOnUnsupportedVersion`
///
/// Not translated (19 / 31) — rationale per case:
///
/// - `testHeartbeatRequestFields*`, `testFirstHeartbeatIncludesRequiredInfoToJoinGroupAndGetAssignments`,
///   `testHeartbeatState*` family: depend on Mockito-mocking individual
///   getters on `ConsumerMembershipManager` (member_id, group_instance_id,
///   server_assignor, rack_id) AND comparing wire-protocol field-by-field
///   diffs across multiple heartbeats. The diff-tracking logic in our
///   `HeartbeatState::build_request_data` does the right thing but
///   pinning it requires either Mockito-style mocks (not present in
///   Rust) OR an integration test that drives the full membership
///   lifecycle, which is Phase 10's responsibility. Deferred.
/// - `testNetworkTimeout`, `testDisconnect`: Java exercises
///   `request.handler().onFailure(...)` to simulate transport
///   failures. `UnsentRequest`'s response handler is not yet wired in
///   Phase 8b (Phase 10 owns the response-loop driver). Deferred.
/// - `testFailureOnFatalException`, `testHeartbeatResponseErrorNotifiedToGroupManagerAfterErrorPropagated`,
///   and the remaining `testHeartbeatResponseOnErrorHandling*` matrix
///   rows (`GROUP_AUTHORIZATION_FAILED`, `NOT_COORDINATOR`,
///   `CoordinatorLoadInProgress`, …): all exercise the
///   `BackgroundEventHandler` add ordering + the
///   `membershipManager.onHeartbeatFailure(retriable)` ordering. The
///   ordering check requires Mockito's `InOrder` verifier; the Rust
///   equivalent is observable but tedious and is most-naturally
///   asserted as part of Phase 10's end-to-end response-handling
///   harness. Deferred.
/// - `testHeartbeatStartupOnSuccess`, `testHeartbeatRequestFailedAndOnHeartbeatFailureCalled`:
///   require simulating async response delivery via `ClientResponse`
///   construction; the helper to build a `ClientResponse` for
///   ConsumerGroupHeartbeat from a stubbed Errors is not yet in the
///   Rust test toolkit. Deferred to Phase 10.
/// - `testPollTimerExpiration`, `testPollTimerNotReachedRebalanceTimeoutBudget`:
///   exercise `reset_poll_timer` + `maybe_rejoin_stale_member`
///   semantics, both of which live in the Phase 10 epilogue (per
///   docstring on `reset_poll_timer`). Deferred.
/// - `testRegexResolutionNotSupported*`: depend on the regex
///   subscription path that lands fully in Phase 9 (RE2/J wiring).
///   Deferred.
/// - `testRebalanceTimeoutOnPollTimerExpiration`: relies on the
///   full bg-task driver to time out the rebalance and re-emit the
///   leave heartbeat. Deferred to Phase 10.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Node;
    use crate::common::internals::ClusterResourceListeners;
    use crate::consumer::ConsumerConfig;
    use crate::consumer::internals::auto_offset_reset_strategy::AutoOffsetResetStrategy;
    use crate::consumer::internals::consumer_metadata::ConsumerMetadata;
    use tokio::sync::mpsc;

    /// Default heartbeat interval used by Java's
    /// `ConsumerHeartbeatRequestManagerTest`.
    const DEFAULT_HEARTBEAT_INTERVAL_MS: i64 = 1_000;
    /// Default retry-backoff (matches `retry.backoff.ms` default).
    const DEFAULT_RETRY_BACKOFF_MS: i64 = 100;

    fn make() -> ConsumerHeartbeatRequestManager {
        make_with_coord(None).0
    }

    /// Like `make`, but also returns the underlying coordinator
    /// manager so tests can inject a coordinator node. Optionally
    /// installs an immediately-firing heartbeat interval (matches
    /// Java's `createHeartbeatRequestStateWithZeroHeartbeatInterval`).
    fn make_with_coord(
        initial_interval_ms: Option<i64>,
    ) -> (
        ConsumerHeartbeatRequestManager,
        Arc<CoordinatorRequestManager>,
        Arc<ConsumerMembershipManager>,
    ) {
        let (hb, coord, mm, _rx) = make_with_coord_capturing_events(initial_interval_ms);
        (hb, coord, mm)
    }

    /// Same as [`make_with_coord`] but also returns the background-event
    /// receiver so a test can observe `BackgroundEvent::Error` envelopes
    /// emitted by the heartbeat manager.
    fn make_with_coord_capturing_events(
        initial_interval_ms: Option<i64>,
    ) -> (
        ConsumerHeartbeatRequestManager,
        Arc<CoordinatorRequestManager>,
        Arc<ConsumerMembershipManager>,
        mpsc::UnboundedReceiver<crate::consumer::internals::events::background_event::BackgroundEventEnvelope>,
    ) {
        let config = ConsumerConfig::new(vec!["localhost:9092".to_string()]);
        let subs = Arc::new(Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::LATEST)));
        let metadata = Arc::new(ConsumerMetadata::from_config(
            &config,
            subs.clone(),
            ClusterResourceListeners::new(),
        ));
        let (tx, rx) = mpsc::unbounded_channel();
        let beh = Arc::new(BackgroundEventHandler::new(tx));
        let coord = Arc::new(CoordinatorRequestManager::new(100, 1_000, "g"));
        let mm = Arc::new(ConsumerMembershipManager::new(
            "g",
            None,
            None,
            100,
            None,
            subs.clone(),
            None,
            metadata,
            beh.clone(),
            true,
        ));
        let mut hb = ConsumerHeartbeatRequestManager::new(0, &config, coord.clone(), subs, mm.clone(), beh);
        if let Some(interval) = initial_interval_ms {
            hb.inner.heartbeat_request_state.update_heartbeat_interval_ms(0, interval);
        }
        (hb, coord, mm, rx)
    }

    /// Test helper: inject a coordinator so `poll` doesn't short-circuit
    /// on "coordinator unknown". Mirrors Mockito
    /// `when(coordinatorRequestManager.coordinator()).thenReturn(...)`.
    fn set_coordinator(coord: &Arc<CoordinatorRequestManager>) {
        coord.set_coordinator_for_test(Node::new(0, "localhost".to_string(), 9092));
    }

    /// Test helper: drive the membership manager through
    /// UNSUBSCRIBED -> JOINING so `should_skip_heartbeat()` returns
    /// false and a heartbeat is eligible to be sent.
    fn make_joining(mm: &ConsumerMembershipManager) {
        mm.transition_to_joining().unwrap();
    }

    /// When the coordinator is unknown, poll returns EMPTY.
    #[test]
    fn poll_returns_empty_when_no_coordinator() {
        let mut mgr = make();
        let result = mgr.poll(0);
        assert!(result.unsent_requests.is_empty());
    }

    /// `should_send_leave_heartbeat_now` returns false when not in
    /// LEAVING state.
    #[test]
    fn should_not_send_leave_when_not_leaving() {
        let mgr = make();
        assert!(!mgr.should_send_leave_heartbeat_now());
    }

    /// `handle_specific_exception_in_response` for `UnsupportedVersion`
    /// is fatal with the consumer-protocol-not-supported message.
    #[test]
    fn handle_specific_unsupported_version_is_fatal() {
        let mut mgr = make();
        let action = mgr.handle_specific_exception_in_response(
            crate::common::protocol::Errors::UnsupportedVersion,
            "broker doesn't support",
            0,
        );
        match action {
            Some(HeartbeatErrorAction::Fatal(err)) => {
                assert!(err.to_string().contains("CONSUMER group protocol"));
            },
            other => panic!("expected fatal, got {other:?}"),
        }
    }

    /// `handle_specific_exception_in_response` for `FencedInstanceId`
    /// is fatal.
    #[test]
    fn handle_specific_fenced_instance_id_is_fatal() {
        let mut mgr = make();
        let action =
            mgr.handle_specific_exception_in_response(crate::common::protocol::Errors::FencedInstanceId, "msg", 0);
        assert!(matches!(action, Some(HeartbeatErrorAction::Fatal(_))));
    }

    /// `handle_specific_exception_in_response` returns None for an
    /// error not in the consumer-specific set.
    #[test]
    fn handle_specific_returns_none_for_other_errors() {
        let mut mgr = make();
        let action = mgr.handle_specific_exception_in_response(crate::common::protocol::Errors::None, "", 0);
        assert!(action.is_none());
    }

    /// `maximum_time_to_wait` returns 0 when the poll timer has
    /// expired.
    #[test]
    fn maximum_time_to_wait_returns_zero_when_poll_timer_expired() {
        let mgr = make();
        // Default max.poll.interval.ms is 300_000; advance past it.
        assert_eq!(mgr.maximum_time_to_wait(300_001), 0);
    }

    /// Translated from
    /// `ConsumerHeartbeatRequestManagerTest#testHeartbeatOnStartup`.
    /// First poll on a fresh (UNSUBSCRIBED) member returns EMPTY
    /// because heartbeats are skipped. Once the member transitions to
    /// JOINING and the initial interval fires, a single heartbeat is
    /// emitted; a second poll while the previous request is in-flight
    /// returns EMPTY again.
    #[tokio::test]
    async fn heartbeat_on_startup() {
        let (mut mgr, coord, mm) = make_with_coord(Some(0));
        set_coordinator(&coord);

        // UNSUBSCRIBED -> skip_heartbeat is true -> EMPTY.
        let result = mgr.poll(0);
        assert_eq!(result.unsent_requests.len(), 0);

        // Drive to JOINING — should_heartbeat_now() returns true.
        make_joining(&mm);
        assert_eq!(mgr.maximum_time_to_wait(0), 0);

        let result = mgr.poll(0);
        assert_eq!(result.unsent_requests.len(), 1);

        // Second poll without completing the inflight: no new
        // request (request_in_flight() short-circuits should_heartbeat_now).
        let result2 = mgr.poll(0);
        assert_eq!(result2.unsent_requests.len(), 0);
    }

    /// Translated from
    /// `ConsumerHeartbeatRequestManagerTest#testTimerNotDue`.
    /// When the heartbeat interval has not yet elapsed, no heartbeat
    /// is sent and the result's `time_until_next_poll_ms` carries the
    /// remaining time.
    #[tokio::test]
    async fn timer_not_due() {
        let (mut mgr, coord, mm) = make_with_coord(Some(DEFAULT_HEARTBEAT_INTERVAL_MS));
        set_coordinator(&coord);
        make_joining(&mm);
        // First poll fires immediately (initial interval, request_in_flight=false).
        let _ = mgr.poll(0);

        // 100ms after: interval not yet due (request still in-flight).
        let result = mgr.poll(100);
        assert_eq!(result.unsent_requests.len(), 0);
    }

    /// Translated from
    /// `ConsumerHeartbeatRequestManagerTest#testHeartbeatNotSentIfAnotherOneInFlight`
    /// (subset — we omit the inflight-completion + retry-backoff
    /// segment which depends on the response delivery harness that
    /// lands in Phase 10).
    #[tokio::test]
    async fn heartbeat_not_sent_if_another_one_in_flight() {
        let (mut mgr, coord, mm) = make_with_coord(Some(0));
        set_coordinator(&coord);
        make_joining(&mm);

        // Initial heartbeat fires.
        let result = mgr.poll(0);
        assert_eq!(result.unsent_requests.len(), 1);

        // Interval elapses but request still in flight -> EMPTY.
        let result = mgr.poll(DEFAULT_HEARTBEAT_INTERVAL_MS);
        assert_eq!(
            result.unsent_requests.len(),
            0,
            "no heartbeat should be sent while a previous one is in-flight"
        );

        // Another interval elapses; still in-flight -> EMPTY.
        let result = mgr.poll(2 * DEFAULT_HEARTBEAT_INTERVAL_MS);
        assert_eq!(result.unsent_requests.len(), 0);
    }

    /// Translated from
    /// `ConsumerHeartbeatRequestManagerTest#testHeartbeatOutsideInterval`.
    /// Even when the interval timer has not elapsed,
    /// `should_heartbeat_now()` (driven by membership state
    /// JOINING / ACKNOWLEDGING / LEAVING) forces a heartbeat.
    #[tokio::test]
    async fn heartbeat_outside_interval() {
        let (mut mgr, coord, mm) = make_with_coord(Some(DEFAULT_HEARTBEAT_INTERVAL_MS));
        set_coordinator(&coord);
        // JOINING is a should_heartbeat_now() state per
        // MembershipInner::should_heartbeat_now().
        make_joining(&mm);

        let result = mgr.poll(0);
        // Heartbeat should be sent because JOINING forces it even
        // outside the interval window.
        assert_eq!(result.unsent_requests.len(), 1);
        // Interval timer was reset (request now in-flight).
        assert_eq!(result.time_until_next_poll_ms, DEFAULT_HEARTBEAT_INTERVAL_MS);
    }

    /// Translated from
    /// `ConsumerHeartbeatRequestManagerTest#testHeartbeatResponseOnErrorHandling`
    /// (UnreleasedInstanceId row).
    #[test]
    fn handle_specific_unreleased_instance_id_is_fatal() {
        let mut mgr = make();
        let action = mgr.handle_specific_exception_in_response(
            crate::common::protocol::Errors::UnreleasedInstanceId,
            "instance id still in use",
            0,
        );
        assert!(matches!(action, Some(HeartbeatErrorAction::Fatal(_))));
    }

    /// Regression for COMMENTS.1.md #12: `handle_specific_failure`
    /// receives `current_time_ms` and emits an `ErrorEvent` carrying
    /// the consumer-protocol-not-supported message.
    #[test]
    fn handle_specific_failure_unsupported_version_emits_error_event() {
        let (mut mgr, _coord, _mm) = make_with_coord(None);
        // Build an UnsupportedVersion error WITHOUT the regex-not-supported
        // tag so we hit the CONSUMER_PROTOCOL_NOT_SUPPORTED_MSG branch.
        let err = crate::common::KafkaError::unsupported_version("broker too old".to_string());
        let fatal = mgr.handle_specific_failure(&err, 12_345);
        assert!(fatal, "UnsupportedVersion must be classified as fatal");
    }

    /// Phase 12.5 (3/N) regression — response routing via the
    /// `PendingHeartbeatCompletion` mpsc channel. Drive `poll(now)` so
    /// a heartbeat `UnsentRequest` is emitted, synthesise a
    /// `ConsumerGroupHeartbeatResponse` and resolve the request's
    /// completion handler. The spawned forwarder enqueues a
    /// `PendingHeartbeatCompletion::Response` envelope. On the next
    /// `poll(now)`, the drain at the top invokes
    /// `on_response` → `inner.on_successful_response` +
    /// `membership_manager.on_heartbeat_success(response)`. The
    /// membership state advances past JOINING.
    ///
    /// This is the test that would have caught the Phase-12 audit
    /// response-routing gap on the heartbeat path (audit verdict:
    /// BROKEN, no production callsite of `take_response_receiver`).
    #[tokio::test]
    async fn test_response_routing_through_spawned_forwarder() {
        use crate::client_response::ClientResponse;
        use crate::common::protocol::ApiKeys;
        use crate::common::requests::request_header::RequestHeader;
        use crate::consumer_group_heartbeat_response_data::{Assignment, ConsumerGroupHeartbeatResponseData};

        let (mut mgr, coord, mm) = make_with_coord(Some(0));
        set_coordinator(&coord);
        make_joining(&mm);

        // First poll emits a single heartbeat request — JOINING is a
        // should_heartbeat_now() state and the initial interval is 0.
        let result = mgr.poll(0);
        assert_eq!(result.unsent_requests.len(), 1);
        let unsent = result.unsent_requests.into_iter().next().unwrap();

        // Build a successful response with `member_epoch=1` and an
        // empty assignment so the membership manager transitions
        // JOINING → RECONCILING (mirrors
        // `on_heartbeat_success_empty_assignment_transitions_to_reconciling`).
        let mut data = ConsumerGroupHeartbeatResponseData::new();
        data.error_code = Errors::None.code();
        data.member_id = Some(mm.member_id());
        data.member_epoch = 1;
        data.heartbeat_interval_ms = 5_000;
        data.assignment = Some(Assignment { topic_partitions: vec![], unknown_tagged_fields: vec![] });
        let resp = ConsumerGroupHeartbeatResponse::new(data);

        let header = RequestHeader::new(
            &ApiKeys::CONSUMER_GROUP_HEARTBEAT,
            ApiKeys::CONSUMER_GROUP_HEARTBEAT.latest_version(),
            "",
            1,
        )
        .expect("header ok");
        let client_response = ClientResponse::with_timeout(
            header,
            None,
            "0",
            0,
            0,
            false,
            false,
            None,
            None,
            Some(ConcreteResponse::ConsumerGroupHeartbeat(resp)),
        );
        unsent.handler().on_complete(client_response);

        // Wait deterministically for the spawned forwarder to enqueue
        // the completion AND for the next `poll(now)`'s drain to
        // dispatch it onto the membership manager.
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(200);
        loop {
            // Each poll() call drains the pending-completion channel
            // at its top. We `poll` repeatedly because the spawned
            // forwarder runs asynchronously — the first `poll()` may
            // see an empty channel.
            let _ = mgr.poll(0);
            if mm.state() != MemberState::Joining {
                break;
            }
            if std::time::Instant::now() >= deadline {
                panic!(
                    "membership state did not advance past JOINING within 200ms; \
                     drain did not observe the spawned forwarder's response (state={:?})",
                    mm.state()
                );
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        // JOINING + empty assignment → RECONCILING (see
        // `on_heartbeat_success_empty_assignment_transitions_to_reconciling`).
        assert_eq!(mm.state(), MemberState::Reconciling);
        // The heartbeat interval was updated from the response (Java:
        // `heartbeatRequestState.updateHeartbeatIntervalMs(...)`).
        assert_eq!(mgr.inner.heartbeat_request_state.heartbeat_interval_ms(), 5_000);
    }

    /// Phase 12.5 round-2 regression for Issue 3: on the **error**
    /// branch of `on_response` (broker returns a non-NONE error code in
    /// the response body), the per-request `SentFields` tracker MUST be
    /// reset, mirroring Java's `AbstractHeartbeatRequestManager.java:356`
    /// (`resetHeartbeatState();` at the top of `onErrorResponse`).
    ///
    /// Without this, the next heartbeat's `build_request_data()` would
    /// diff against stale `SentFields` and SKIP fields the broker needs
    /// to re-receive (subscribed topic names, rebalance timeout, server
    /// assignor, local assignment, pattern).
    ///
    /// Test shape:
    /// 1. Subscribe to a topic so `SubscriptionState` has a non-empty
    ///    subscription. Drive `poll(now)` once — the heartbeat builder
    ///    populates `SentFields.subscribed_topic_names`.
    /// 2. Synthesise a `ConsumerGroupHeartbeatResponse` with
    ///    `error_code = CoordinatorLoadInProgress` (a benign
    ///    retriable-via-backoff error that exercises the error branch
    ///    of `on_response`).
    /// 3. Drive the heartbeat handler with `on_complete(response)`.
    /// 4. Drive `poll(now)` again to drain the
    ///    `PendingHeartbeatCompletion::Response` envelope. Assert that
    ///    `sent_fields_topics_populated()` is now `false` — the reset
    ///    fired at the top of the error branch.
    #[tokio::test]
    async fn issue3_error_response_resets_sent_fields() {
        use crate::client_response::ClientResponse;
        use crate::common::protocol::ApiKeys;
        use crate::common::requests::request_header::RequestHeader;
        use crate::consumer_group_heartbeat_response_data::ConsumerGroupHeartbeatResponseData;
        use std::collections::HashSet;

        let (mut mgr, coord, mm) = make_with_coord(Some(0));
        set_coordinator(&coord);
        // Subscribe to a topic BEFORE driving the heartbeat so the
        // request body includes `SubscribedTopicNames` and the diff
        // tracker populates `SentFields.subscribed_topic_names`.
        {
            let subs_arc = mgr.heartbeat_state.subscriptions.clone();
            let mut guard = subs_arc.lock().unwrap();
            let mut topics = HashSet::new();
            topics.insert("t".to_string());
            guard.subscribe_topics(topics, None).unwrap();
        }
        make_joining(&mm);

        // First poll emits a single heartbeat — the diff tracker now
        // has `subscribed_topic_names = Some(...)`.
        let result = mgr.poll(0);
        assert_eq!(result.unsent_requests.len(), 1);
        assert!(
            mgr.sent_fields_topics_populated(),
            "sanity: SentFields should be populated after the first heartbeat build"
        );
        let unsent = result.unsent_requests.into_iter().next().unwrap();

        // Synthesise an ERROR response (CoordinatorLoadInProgress) so
        // the `on_response` error branch runs. The drain runs at the
        // top of the next `poll(now)`, where `reset_heartbeat_state()`
        // must fire BEFORE `classify_response_error`.
        let mut data = ConsumerGroupHeartbeatResponseData::new();
        data.error_code = Errors::CoordinatorLoadInProgress.code();
        data.member_id = Some(mm.member_id());
        data.member_epoch = 0;
        data.heartbeat_interval_ms = 1_000;
        let resp = ConsumerGroupHeartbeatResponse::new(data);

        let header = RequestHeader::new(
            &ApiKeys::CONSUMER_GROUP_HEARTBEAT,
            ApiKeys::CONSUMER_GROUP_HEARTBEAT.latest_version(),
            "",
            1,
        )
        .expect("header ok");
        let client_response = ClientResponse::with_timeout(
            header,
            None,
            "0",
            0,
            0,
            false,
            false,
            None,
            None,
            Some(ConcreteResponse::ConsumerGroupHeartbeat(resp)),
        );
        unsent.handler().on_complete(client_response);

        // Drive poll() until the spawned forwarder has enqueued the
        // completion and the drain has run.
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(200);
        loop {
            let _ = mgr.poll(0);
            if !mgr.sent_fields_topics_populated() {
                break;
            }
            if std::time::Instant::now() >= deadline {
                panic!(
                    "SentFields.subscribed_topic_names was NOT reset by the error branch \
                     of on_response — Java's `resetHeartbeatState()` at the top of \
                     `onErrorResponse` is missing in Rust"
                );
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        assert!(
            !mgr.sent_fields_topics_populated(),
            "after the error-response drain, SentFields must be reset so the next \
             heartbeat re-sends the subscription state"
        );
    }

    /// Phase 12.5 (3/N) regression — failure path. When the response
    /// receiver resolves with `Err(transport_err)`, the forwarder
    /// enqueues `PendingHeartbeatCompletion::Failure`. On the next
    /// `poll(now)`, the drain calls `inner.on_failure(...)` and
    /// `membership_manager.on_heartbeat_failure(retriable)`. The
    /// `heartbeat_request_state` is reset (Java's
    /// `resetHeartbeatState()` at the top of `onFailure`).
    #[tokio::test]
    async fn test_response_routing_failure_path() {
        let (mut mgr, coord, mm) = make_with_coord(Some(0));
        set_coordinator(&coord);
        make_joining(&mm);

        let result = mgr.poll(0);
        assert_eq!(result.unsent_requests.len(), 1);
        let unsent = result.unsent_requests.into_iter().next().unwrap();

        // Fire a transport-layer retriable failure through the handler.
        unsent.handler().on_failure(0, KafkaError::new(Errors::NetworkException));

        // Wait deterministically for the drain on the next `poll(now)`
        // to observe the failure and advance heartbeat-request state.
        // The observable: `request_in_flight()` flips false after
        // `on_failure → on_failed_attempt`.
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(200);
        loop {
            let _ = mgr.poll(100);
            if !mgr.inner.heartbeat_request_state.request_in_flight() {
                break;
            }
            if std::time::Instant::now() >= deadline {
                panic!("heartbeat_request_state remained in-flight; drain did not observe the transport failure");
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        assert!(
            !mgr.inner.heartbeat_request_state.request_in_flight(),
            "request_in_flight must be cleared by the failure path drain"
        );
    }

    /// Helper for Issue-4 regression tests: drives a heartbeat, routes
    /// the supplied error-code response through the spawned forwarder,
    /// drives `poll(now)` until the drain has classified the response,
    /// then drains the side-channel and returns the pending transitions
    /// + the background-event envelopes that arrived during the loop.
    ///
    /// Encapsulates the response-routing scaffolding so the per-error
    /// tests focus on the classification mapping.
    async fn drive_error_response_and_collect(
        mgr: &mut ConsumerHeartbeatRequestManager,
        mm: &Arc<ConsumerMembershipManager>,
        coord: &Arc<CoordinatorRequestManager>,
        beh_rx: &mut mpsc::UnboundedReceiver<
            crate::consumer::internals::events::background_event::BackgroundEventEnvelope,
        >,
        error_code: i16,
    ) -> (
        Vec<PendingMembershipTransition>,
        Vec<crate::consumer::internals::events::background_event::BackgroundEventEnvelope>,
    ) {
        use crate::client_response::ClientResponse;
        use crate::common::protocol::ApiKeys;
        use crate::common::requests::request_header::RequestHeader;
        use crate::consumer_group_heartbeat_response_data::ConsumerGroupHeartbeatResponseData;

        set_coordinator(coord);
        make_joining(mm);
        // First poll emits the heartbeat. JOINING is a
        // should_heartbeat_now() state and the initial interval is 0.
        let result = mgr.poll(0);
        assert_eq!(result.unsent_requests.len(), 1);
        let unsent = result.unsent_requests.into_iter().next().unwrap();

        let mut data = ConsumerGroupHeartbeatResponseData::new();
        data.error_code = error_code;
        data.member_id = Some(mm.member_id());
        data.member_epoch = 0;
        data.heartbeat_interval_ms = 1_000;
        let resp = ConsumerGroupHeartbeatResponse::new(data);

        let header = RequestHeader::new(
            &ApiKeys::CONSUMER_GROUP_HEARTBEAT,
            ApiKeys::CONSUMER_GROUP_HEARTBEAT.latest_version(),
            "",
            1,
        )
        .expect("header ok");
        let client_response = ClientResponse::with_timeout(
            header,
            None,
            "0",
            0,
            0,
            false,
            false,
            None,
            None,
            Some(ConcreteResponse::ConsumerGroupHeartbeat(resp)),
        );
        unsent.handler().on_complete(client_response);

        // Drive poll() until the spawned forwarder has enqueued the
        // completion and the drain has fired the classification.
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(200);
        loop {
            let _ = mgr.poll(0);
            let transitions = mgr.take_pending_membership_transitions();
            if !transitions.is_empty() {
                // Collect any pending background events from the drain.
                let mut events = Vec::new();
                while let Ok(env) = beh_rx.try_recv() {
                    events.push(env);
                }
                return (transitions, events);
            }
            if std::time::Instant::now() >= deadline {
                panic!(
                    "no PendingMembershipTransition was emitted by the drain after 200ms — \
                     classification of error_code={} did not route to Fenced/Fatal",
                    error_code
                );
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    }

    /// Phase 12.5 round-2 regression for Issue 4 (Fenced path).
    ///
    /// When the broker returns a `FENCED_MEMBER_EPOCH` error in the
    /// heartbeat response body, Java
    /// (`AbstractHeartbeatRequestManager.java:411-418`) calls
    /// `membershipManager().transitionToFenced()` synchronously inside
    /// the `whenComplete` lambda. Rust's transition is `async`, so the
    /// heartbeat manager emits a
    /// `PendingMembershipTransition::Fenced` envelope onto its
    /// side-channel; the bg-task drains and `await`s the transition.
    ///
    /// Test shape:
    /// 1. Drive a heartbeat, route a `FENCED_MEMBER_EPOCH` response
    ///    through the forwarder, drive `poll(now)` until the drain
    ///    classifies (helper).
    /// 2. Assert exactly one `PendingMembershipTransition::Fenced`
    ///    envelope was emitted on the side-channel.
    /// 3. Drive `mm.transition_to_fenced(now).await` — what the
    ///    bg-task does after draining.
    /// 4. Assert the membership state reflects the post-fence flow:
    ///    with no assignment, `transition_to_fenced` runs FENCED →
    ///    (no listener) → JOINING (Java's
    ///    `transitionToFenced(callbackHandlerSupplier)` rejoins
    ///    immediately when there's nothing to release).
    /// 5. Assert NO `BackgroundEvent::Error` envelope was emitted —
    ///    Java treats the fence as an INTERNAL state-machine event
    ///    and does NOT call `backgroundEventHandler.add(...)` on the
    ///    fence path (`AbstractHeartbeatRequestManager.java:411-427`).
    #[tokio::test]
    async fn issue4_fenced_member_epoch_drives_transition_to_fenced() {
        let (mut mgr, coord, mm, mut beh_rx) = make_with_coord_capturing_events(Some(0));

        let (transitions, events) =
            drive_error_response_and_collect(&mut mgr, &mm, &coord, &mut beh_rx, Errors::FencedMemberEpoch.code())
                .await;

        assert_eq!(transitions.len(), 1, "exactly one PendingMembershipTransition expected");
        assert!(
            matches!(transitions[0], PendingMembershipTransition::Fenced),
            "FENCED_MEMBER_EPOCH must classify to PendingMembershipTransition::Fenced, got: {:?}",
            transitions[0]
        );

        // Drive the transition as the bg-task would. No assignment is
        // held, so transition_to_fenced runs Joining → Fenced →
        // (no listener, no partitions to release) → Joining.
        mm.transition_to_fenced(0).await.expect("transition_to_fenced ok");

        // Final state: Joining (post-fence rejoin). The intermediate
        // Fenced state is exercised by `state == Fenced` checks inside
        // transition_to_fenced; what we observe externally after the
        // await completes is the post-rejoin state. See
        // `consumer_membership_manager.rs::transition_to_fenced` for
        // the FENCED → JOINING tail.
        assert_eq!(
            mm.state(),
            MemberState::Joining,
            "after transition_to_fenced with empty assignment, state should rejoin to JOINING"
        );

        // Java reference: `AbstractHeartbeatRequestManager.java:411-427`
        // — the FENCED_MEMBER_EPOCH / UNKNOWN_MEMBER_ID arms call
        // ONLY `membershipManager().transitionToFenced()` +
        // `heartbeatRequestState.reset()`. They do NOT invoke
        // `backgroundEventHandler.add(new ErrorEvent(...))` — that is
        // reserved for `handleFatalFailure` (`:455-458`). The fence
        // is internal: Java's consumer rejoins transparently and the
        // user observes `ConsumerRecords::empty()` from `poll()`, not
        // an error. Rust must match: NO `BackgroundEvent::Error`
        // envelope on the fence path.
        assert!(
            !events.iter().any(|env| matches!(
                &env.event,
                crate::consumer::internals::events::background_event::BackgroundEvent::Error { .. }
            )),
            "BackgroundEvent::Error must NOT be emitted on the fence path — Java treats the \
             fence as an internal state transition and does not surface an ErrorEvent to the user. \
             Got events: {:?}",
            events.iter().map(|env| std::mem::discriminant(&env.event)).collect::<Vec<_>>()
        );
    }

    /// Phase 12.5 round-2 regression for Issue 4 (Fatal path).
    ///
    /// When the broker returns a fatal-class error in the heartbeat
    /// response body (here `GROUP_AUTHORIZATION_FAILED`), Java's
    /// `AbstractHeartbeatRequestManager.java:388-394` calls
    /// `handleFatalFailure(error.exception(...))`, which (`:455-458`)
    /// emits an `ErrorEvent` AND calls
    /// `membershipManager().transitionToFatal()`. Rust's transition is
    /// `async`, so the heartbeat manager emits a
    /// `PendingMembershipTransition::Fatal(err)` envelope and the
    /// bg-task drains + `await`s the transition.
    ///
    /// Test shape:
    /// 1. Drive a heartbeat, route a `GROUP_AUTHORIZATION_FAILED`
    ///    response through the forwarder, drive `poll(now)` until
    ///    the drain classifies (helper).
    /// 2. Assert exactly one `PendingMembershipTransition::Fatal(...)`
    ///    envelope was emitted on the side-channel, carrying the
    ///    appropriate error.
    /// 3. Drive `mm.transition_to_fatal(now).await` — what the
    ///    bg-task does after draining.
    /// 4. Assert the membership state is `Fatal`.
    /// 5. Assert at least one `BackgroundEvent::Error` envelope was
    ///    emitted (matches Java's `handleFatalFailure` ErrorEvent).
    #[tokio::test]
    async fn issue4_group_authorization_failed_drives_transition_to_fatal() {
        let (mut mgr, coord, mm, mut beh_rx) = make_with_coord_capturing_events(Some(0));

        let (transitions, events) = drive_error_response_and_collect(
            &mut mgr,
            &mm,
            &coord,
            &mut beh_rx,
            Errors::GroupAuthorizationFailed.code(),
        )
        .await;

        assert_eq!(transitions.len(), 1, "exactly one PendingMembershipTransition expected");
        match &transitions[0] {
            PendingMembershipTransition::Fatal(err) => {
                assert_eq!(
                    err.error(),
                    Errors::GroupAuthorizationFailed,
                    "Fatal envelope must carry the original error code"
                );
            },
            other => panic!("GROUP_AUTHORIZATION_FAILED must classify to Fatal, got: {:?}", other),
        }

        // Drive the transition as the bg-task would.
        mm.transition_to_fatal(0).await.expect("transition_to_fatal ok");

        assert_eq!(mm.state(), MemberState::Fatal, "after transition_to_fatal, state must be FATAL");

        // The error is also surfaced to the user via the background
        // event handler (matches Java's `handleFatalFailure` —
        // `backgroundEventHandler.add(new ErrorEvent(error))`).
        assert!(
            events.iter().any(|env| matches!(
                &env.event,
                crate::consumer::internals::events::background_event::BackgroundEvent::Error { .. }
            )),
            "BackgroundEvent::Error must be emitted on the fatal path"
        );
    }

    /// Phase 12.5 round-3 regression for Issue 5 — unknown error
    /// codes must fall through to the `Fatal` arm, not `Handled`.
    ///
    /// Java reference: `AbstractHeartbeatRequestManager.java:435-441`
    /// — the `default:` arm of `onErrorResponse`'s switch calls
    /// `handleSpecificExceptionInResponse(...)`; if that returns
    /// false (no consumer-specific handler matched), Java falls back
    /// to `handleFatalFailure(error.exception(errorMessage))`. The
    /// effect: an unknown error code (or one the abstract switch
    /// does not enumerate and the consumer-specific handler does
    /// not recognise) puts the member into FATAL state and surfaces
    /// an `ErrorEvent` so `poll()` returns the failure to the user.
    ///
    /// Rust used to `.unwrap_or(HeartbeatErrorAction::Handled)` when
    /// the specific handler returned `None`, which silently swallowed
    /// the unknown error and left the member in its current state —
    /// the heartbeat would then keep retrying indefinitely. This
    /// test pins the corrected behaviour.
    ///
    /// `RebalanceInProgress` is used as the unknown-code probe: it
    /// is not enumerated in the abstract `classify_response_error`
    /// match (which only handles
    /// `NotCoordinator|CoordinatorNotAvailable|CoordinatorLoadInProgress|GroupAuthorizationFailed|TopicAuthorizationFailed|InvalidRequest|GroupMaxSizeReached|UnsupportedAssignor|FencedMemberEpoch|UnknownMemberId|InvalidRegularExpression`),
    /// so it falls through to `DelegateToSpecific`. The Consumer
    /// variant's `handle_specific_exception_in_response` only
    /// recognises `UnsupportedVersion|UnreleasedInstanceId|FencedInstanceId`,
    /// so it returns `None` and the fallback `Fatal` arm must fire.
    ///
    /// Test shape:
    /// 1. Drive a heartbeat, route a `RebalanceInProgress` response
    ///    through the forwarder, drive `poll(now)` until the drain
    ///    classifies (helper).
    /// 2. Assert exactly one `PendingMembershipTransition::Fatal(...)`
    ///    envelope was emitted on the side-channel, carrying the
    ///    original error code.
    /// 3. Drive `mm.transition_to_fatal(now).await` and assert state
    ///    is `Fatal`.
    /// 4. Assert at least one `BackgroundEvent::Error` envelope was
    ///    emitted (matches Java's `handleFatalFailure` ErrorEvent
    ///    propagation).
    #[tokio::test]
    async fn issue5_unknown_error_code_falls_through_to_fatal() {
        let (mut mgr, coord, mm, mut beh_rx) = make_with_coord_capturing_events(Some(0));

        let (transitions, events) =
            drive_error_response_and_collect(&mut mgr, &mm, &coord, &mut beh_rx, Errors::RebalanceInProgress.code())
                .await;

        assert_eq!(
            transitions.len(),
            1,
            "exactly one PendingMembershipTransition expected for the unknown-code → Fatal fallback"
        );
        match &transitions[0] {
            PendingMembershipTransition::Fatal(err) => {
                assert_eq!(
                    err.error(),
                    Errors::RebalanceInProgress,
                    "Fatal envelope must carry the original (unknown) error code, not a substituted one"
                );
            },
            other => panic!(
                "unknown error code REBALANCE_IN_PROGRESS must classify to Fatal (Java \
                 `AbstractHeartbeatRequestManager.java:435-441` `default:` arm), got: {:?}",
                other
            ),
        }

        // Drive the transition as the bg-task would.
        mm.transition_to_fatal(0).await.expect("transition_to_fatal ok");

        assert_eq!(mm.state(), MemberState::Fatal, "after transition_to_fatal, state must be FATAL");

        // Java `handleFatalFailure` (`:455-458`) emits an ErrorEvent
        // alongside the fatal transition so the user observes the
        // failure from `poll()`. The Rust Fatal arm must do the same.
        assert!(
            events.iter().any(|env| matches!(
                &env.event,
                crate::consumer::internals::events::background_event::BackgroundEvent::Error { .. }
            )),
            "BackgroundEvent::Error must be emitted on the unknown-error-code fatal fallback path \
             so poll() surfaces the failure to the user"
        );
    }
}
