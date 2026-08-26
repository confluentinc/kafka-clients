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
        /// `ClientResponse.requestLatencyMs()` — recorded into the heartbeat
        /// metrics sensor (`recordRequestLatency`) when the drain applies it.
        /// Java records this in the `whenComplete` lambda whenever a response
        /// arrives, regardless of the response's error code
        /// (`AbstractHeartbeatRequestManager.java:299`).
        request_latency_ms: i64,
    },
    /// Transport-level failure (network error, in-flight cancellation,
    /// type mismatch on the response body). The drain calls
    /// `inner.on_failure(...)` and `membership_manager.on_heartbeat_failure(retriable)`.
    Failure { error: KafkaError, completion_time_ms: i64 },
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
        Self {
            inner,
            membership_manager,
            heartbeat_state,
            pending_completion_tx,
            pending_completion_rx,
        }
    }

    /// Wire up the [`HeartbeatMetricsManager`] so the heartbeat send/response
    /// paths record `last-heartbeat-seconds-ago` and `heartbeat-latency`.
    /// Java passes the metrics manager into the constructor; in Rust it shares
    /// the consumer's `Arc<Metrics>` registry and is wired post-construction.
    pub(crate) fn set_metrics_manager(
        &mut self,
        metrics_manager: Arc<crate::consumer::internals::heartbeat_metrics_manager::HeartbeatMetricsManager>,
    ) {
        self.inner.metrics_manager = Some(metrics_manager);
    }

    /// Java: `resetHeartbeatState()`.
    pub(crate) fn reset_heartbeat_state(&mut self) {
        self.heartbeat_state.reset();
    }

    /// Test-only accessor for [`Self::should_send_leave_heartbeat_now`].
    /// Mirrors what Java's `testPollOnLeaving` isolates (it stubs
    /// `shouldHeartbeatNow()` to its `false` Mockito default so only the
    /// `shouldSendLeaveHeartbeatNow()` predicate decides). With a REAL
    /// membership manager, a LEAVING member's `should_heartbeat_now()` is
    /// also `true`, so the full `poll()` cannot isolate this predicate — we
    /// test it directly instead.
    #[cfg(test)]
    pub(crate) fn should_send_leave_heartbeat_now_for_test(&self) -> bool {
        self.should_send_leave_heartbeat_now()
    }

    /// Test-only: has the spawned response forwarder delivered a completion
    /// that the next `poll(now)`'s step 0 will drain? Lets a test wait for
    /// delivery and then poll exactly once, so the classification a poll
    /// performs is deterministic instead of raced against the forwarder.
    #[cfg(test)]
    pub(crate) fn pending_completions_empty_for_test(&self) -> bool {
        self.pending_completion_rx.is_empty()
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
        // For the `logResponse`/ignore path, latency must still be recorded
        // (Java `AbstractHeartbeatRequestManager.java:311`). The normal path
        // records it via the drained envelope, so the metrics manager is only
        // cloned for the ignore path.
        let ignore_path_metrics = if ignore_response {
            self.inner.metrics_manager.clone()
        } else {
            None
        };
        tokio::spawn(async move {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0);
            let completion = match response_rx.await {
                Ok(Ok(mut client_response)) => {
                    // Java: `response.requestLatencyMs()` — captured before
                    // `take_response_body()`. Recorded in the drain (normal
                    // path) or inline below (`logResponse`/ignore path).
                    let request_latency_ms = client_response.request_latency_ms();
                    match client_response.take_response_body() {
                        Some(ConcreteResponse::ConsumerGroupHeartbeat(resp)) => PendingHeartbeatCompletion::Response {
                            response: resp,
                            completion_time_ms: now_ms,
                            request_latency_ms,
                        },
                        _ => PendingHeartbeatCompletion::Failure {
                            error: KafkaError::new(Errors::UnknownServerError),
                            completion_time_ms: now_ms,
                        },
                    }
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
            if ignore_response {
                // Java `logResponse`: record latency for the arrived response
                // (`AbstractHeartbeatRequestManager.java:311`), then drop the
                // state-driving side-effect.
                if let (Some(metrics_manager), PendingHeartbeatCompletion::Response { request_latency_ms, .. }) =
                    (ignore_path_metrics.as_ref(), &completion)
                {
                    metrics_manager.record_request_latency(*request_latency_ms);
                }
            }
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
                PendingHeartbeatCompletion::Response { response, completion_time_ms, request_latency_ms } => {
                    // Java: `metricsManager.recordRequestLatency(response.requestLatencyMs())`
                    // before `onResponse` (`AbstractHeartbeatRequestManager.java:299-300`).
                    if let Some(metrics_manager) = self.inner.metrics_manager.as_ref() {
                        metrics_manager.record_request_latency(request_latency_ms);
                    }
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
                // Applied inline, where Java applies it. The
                // transition only *enqueues* the §31 onPartitionsLost
                // callback (storing the ack receiver for the bg task
                // to drive), so it needs no `.await` and runs from
                // this sync path. No lock guard is live here — see
                // [`Self::drain_pending_completions`].
                if let Err(e) = self.membership_manager.transition_to_fenced(completion_time_ms) {
                    // Java's `whenComplete` lambda logs what
                    // `transitionToFenced` throws and does not rethrow.
                    log::warn!("transition_to_fenced failed: {}", e);
                }
            },
            HeartbeatErrorAction::Fatal(err) => {
                // Java: `handleFatalFailure(error.exception(...))`
                // (`AbstractHeartbeatRequestManager.java:455-458`) —
                // emits an `ErrorEvent` AND calls
                // `membershipManager().transitionToFatal()`, in that
                // order.
                let _ = self
                    .inner
                    .background_event_handler
                    .add(BackgroundEvent::Error { error: err.clone() }, completion_time_ms);
                if let Err(e) = self.membership_manager.transition_to_fatal(completion_time_ms) {
                    log::warn!("transition_to_fatal failed: {}", e);
                }
            },
            HeartbeatErrorAction::DelegateToSpecific => {
                // Already handled above; this arm is unreachable
                // because the `match action` block resolves
                // `DelegateToSpecific` to a concrete action — either
                // the specific handler's return value, or `Fatal`
                // when the specific handler returns `None` (the
                // `unwrap_or_else` fallback above). No path leaves
                // `DelegateToSpecific` as the `final_action`.
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
                // `membershipManager().transitionToFatal()`, in that
                // order. Both are mirrored here.
                let _ = self
                    .inner
                    .background_event_handler
                    .add(BackgroundEvent::Error { error: error.clone() }, completion_time_ms);
                if let Err(e) = self.membership_manager.transition_to_fatal(completion_time_ms) {
                    log::warn!("transition_to_fatal failed: {}", e);
                }
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
            if let Err(e) = self.membership_manager.transition_to_fatal(current_time_ms) {
                log::warn!("transition_to_fatal failed: {}", e);
            }
            return true;
        }
        false
    }

    /// Wrap the shared `classify_response_error` dispatch with the
    /// Consumer-specific extras (UNSUPPORTED_VERSION, UNRELEASED_INSTANCE_ID,
    /// FENCED_INSTANCE_ID, GROUP_ID_NOT_FOUND).
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
            Errors::GroupIdNotFound => {
                // AK 4.3.1: if the group doesn't exist (e.g., the member never
                // joined due to InvalidTopicException) and the member is
                // UNSUBSCRIBED, GROUP_ID_NOT_FOUND is ignored — the leave is
                // effectively complete. When a leave heartbeat (epoch=-1) is
                // sent, the state transitions synchronously from LEAVING to
                // UNSUBSCRIBED in on_heartbeat_request_generated() before the
                // request is sent. Java:
                //   `if (state() == UNSUBSCRIBED) { onHeartbeatRequestSkipped(); }`
                if self.membership_manager.state() == MemberState::Unsubscribed {
                    log::info!(
                        "ConsumerGroupHeartbeatRequest received GROUP_ID_NOT_FOUND for group {} while \
                         unsubscribed.",
                        self.membership_manager.group_id()
                    );
                    let _ = self.membership_manager.abstract_mm.on_heartbeat_request_skipped();
                    return Some(HeartbeatErrorAction::Handled);
                }

                // KIP-848 fence-and-rejoin transient. See Issue 9 in
                // `design/history/Milestone-8/Phase-13/COMMENTS.DONE.1.md`.
                //
                // NOTE deviation vs AK 4.3.1: Java's non-unsubscribed arm is
                // FATAL (handleFatalFailure). Rust keeps the Issue-9
                // epoch-conditional recovery (retry when epoch==0, fenced-rejoin
                // when epoch>0) for consumer recoverability — a pre-existing
                // deviation, so testGroupIdNotFoundWhileStableIsFatal is a
                // recorded skip.
                //
                // The broker returns `GROUP_ID_NOT_FOUND` from
                // `getOrMaybeCreateConsumerGroup(...,
                // createIfNotExists = memberEpoch == 0, ...)`
                // (`GroupMetadataManager.java:2326-2327`) only when
                // both:
                //
                //   1. The group does not exist on the broker
                //      (typically because it was just reaped after the
                //      last member left), AND
                //   2. `createIfNotExists` is `false`, which happens
                //      when `memberEpoch != 0`.
                //
                // Java treats this as fatal
                // (`AbstractHeartbeatRequestManager.java:435-441`'s
                // default arm); we deviate here to keep the consumer
                // recoverable. Behavior depends on the membership
                // manager's current epoch:
                //
                // - **memberEpoch > 0** (member previously in-group;
                //   group has been reaped): treat as `Fenced`. The
                //   `Fenced` flow transitions through FENCED → JOINING,
                //   which sets `memberEpoch = 0` (Java's `resetEpoch`).
                //   The next heartbeat carries the fresh epoch and
                //   re-creates the group on the broker.
                // - **memberEpoch == 0** (first heartbeat after a
                //   fresh subscribe; or a previously-fenced consumer
                //   that hasn't yet been able to send the rejoin):
                //   the broker should have created the group on this
                //   heartbeat — receiving `GROUP_ID_NOT_FOUND` here
                //   means the broker hit a transient bad state.
                //   Backoff + retry by treating as `Handled`. The
                //   `classify_response_error` caller in the abstract
                //   layer already calls `on_failed_attempt(...)`
                //   before we reach this method, so the next
                //   heartbeat is naturally backed off.
                let member_epoch = self.membership_manager.member_epoch();
                if member_epoch == 0 {
                    log::warn!(
                        "ConsumerGroupHeartbeatRequest failed with GROUP_ID_NOT_FOUND on first heartbeat \
                         (memberEpoch=0): {}. Will retry with backoff.",
                        error_message
                    );
                    Some(HeartbeatErrorAction::Handled)
                } else {
                    log::warn!(
                        "ConsumerGroupHeartbeatRequest failed with GROUP_ID_NOT_FOUND while rejoining \
                         (memberEpoch={member_epoch}): {error_message}. Member will rejoin from scratch."
                    );
                    self.inner.heartbeat_request_state.reset();
                    Some(HeartbeatErrorAction::Fenced)
                }
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

    /// Test-only: invoke the `HeartbeatState::build_request_data` field-diff
    /// logic directly. Java tests construct a `HeartbeatState` and call
    /// `buildRequestData()` repeatedly to assert which fields are present /
    /// omitted across heartbeats. The Rust `HeartbeatState` is a private
    /// inner type; this accessor exposes the same observable on the manager.
    #[cfg(test)]
    pub(crate) fn build_request_data_for_test(&mut self) -> ConsumerGroupHeartbeatRequestData {
        self.heartbeat_state.build_request_data()
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
            // Java parity: `makeHeartbeatRequest(currentTimeMs, true)` records
            // the heartbeat-sent time (`AbstractHeartbeatRequestManager.java:285`)
            // for every send, including this poll-timer-expired leave path.
            // The normal path records it inside `make_heartbeat_poll_result`
            // below; this branch builds its own `PollResult`, so record here.
            if let Some(metrics_manager) = self.inner.metrics_manager.as_ref() {
                metrics_manager.record_heartbeat_sent_ms(current_time_ms);
            }
            // Java parity: `makeHeartbeatRequest(currentTimeMs, true)` always
            // calls `membershipManager().onHeartbeatRequestGenerated()`
            // (`AbstractHeartbeatRequestManager.makeHeartbeatRequest`). For a
            // member whose poll timer expired, that transitions LEAVING →
            // STALE (`AbstractMembershipManager.onHeartbeatRequestGenerated`
            // → `transitionToStale()`). The earlier Rust translation omitted
            // this call here, so the member stayed in LEAVING forever and the
            // STALE path was unreachable via poll().
            if let Err(e) = self.membership_manager.abstract_mm.on_heartbeat_request_generated() {
                log::warn!("on_heartbeat_request_generated (poll-timer-expiry) failed: {}", e);
            }
            // If the member is now STALE, the assignment must be released via
            // the §31 onPartitionsLost listener — the tail of Java's
            // `transitionToStale`. Enqueuing that callback needs no `.await`
            // (the ack is stored for the bg task to drive), so it runs
            // inline here, as it does in Java.
            if self.membership_manager.state() == MemberState::Stale
                && let Err(e) = self.membership_manager.transition_to_stale(current_time_ms)
            {
                log::warn!("transition_to_stale failed: {}", e);
            }
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

    fn poll_on_close(&mut self, current_time_ms: i64) -> PollResult {
        // Drain any pending completions one last time so close paths
        // observe the post-completion state.
        self.drain_pending_completions(current_time_ms);
        // Java: if (membershipManager().isLeavingGroup()) send the
        // leave heartbeat (ignoreResponse=true — pollOnClose drops
        // the response by Java's `logResponse(...)` semantics).
        if self.membership_manager.is_leaving_group() {
            let request = self.build_heartbeat_request(true);
            // Java parity: `pollOnClose` routes its leave heartbeat through
            // `makeHeartbeatRequest(currentTimeMs, true)`
            // (`AbstractHeartbeatRequestManager.java:233`), which records the
            // heartbeat-sent time (`:285`). This is the third of Java's three
            // heartbeat send sites; record here so `last-heartbeat-seconds-ago`
            // reflects the close-path leave heartbeat, matching the poll-timer
            // leave path above and the normal path in `make_heartbeat_poll_result`.
            if let Some(metrics_manager) = self.inner.metrics_manager.as_ref() {
                metrics_manager.record_heartbeat_sent_ms(current_time_ms);
            }
            return PollResult::new(self.inner.heartbeat_request_state.heartbeat_interval_ms(), vec![request]);
        }
        PollResult::empty()
    }

    fn maximum_time_to_wait(&self, current_time_ms: i64) -> i64 {
        // AK 4.3.1 (KAFKA-20426): when the member is UNSUBSCRIBED (for example,
        // with manual assignment and no group), return i64::MAX to indicate
        // there is no next heartbeat to wait for — allowing the application
        // thread to block for the full user-specified poll timeout rather than
        // spinning in a busy loop. Java:
        //   `if (membershipManager().state() == MemberState.UNSUBSCRIBED) return Long.MAX_VALUE;`
        {
            let inner = self.membership_manager.abstract_mm.inner.lock();
            let guard = match inner {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            if guard.state == MemberState::Unsubscribed {
                return i64::MAX;
            }
        }
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

/// Translation notes on Java test coverage (`ConsumerHeartbeatRequestManagerTest`,
/// 31 `@Test`). Coverage after Phases 8b / 12.5 / 35:
///
/// Translated / behaviorally covered (~26 / 31):
/// - `testSkippingHeartbeat` — `poll_returns_empty_when_no_coordinator`
/// - `testHeartbeatOnStartup` — `heartbeat_on_startup`
/// - `testTimerNotDue` — `timer_not_due`
/// - `testHeartbeatNotSentIfAnotherOneInFlight` — `heartbeat_not_sent_if_another_one_in_flight` (subset)
/// - `testHeartbeatOutsideInterval` — `heartbeat_outside_interval`
/// - `testNoCoordinator` — `poll_returns_empty_when_no_coordinator` (coordinator-unknown subset)
/// - `testHeartbeatResponseOnErrorHandling` matrix — abstract `classify_response_error` table
///   + `handle_specific_{unsupported_version,fenced_instance_id,unreleased_instance_id}_*`
/// - `testUnsupportedVersionFromBroker` / `FromClient` — `handle_specific_*` + emit-error-event
/// - `testNetworkTimeout` / `testDisconnect` / `testFailureOnFatalException` /
///   `testHeartbeatResponseErrorNotifiedToGroupManager*` / `testHeartbeatRequestFailureNotified*` —
///   Phase-12.5 response-routing tests (`test_response_routing_*`, `issue3/4/5_*`)
/// - `testFencedMemberStopHeartbeatUntilItReleasesAssignmentToRejoin` — `issue4_fenced_member_*` (subset)
/// - `testHeartBeatRequestStateToStringBase` — `heartbeat_request_state_to_string_base`
///   (in `heartbeat_request_state.rs`; Phase 35)
/// - `testFirstHeartbeatIncludesRequiredInfoToJoinGroupAndGetAssignments` —
///   `first_heartbeat_includes_required_info_to_join_group` (Phase 35)
/// - `testValidateConsumerGroupHeartbeatRequest` — `validate_consumer_group_heartbeat_request` (Phase 35)
/// - `testValidateConsumerGroupHeartbeatRequestAssignmentSentWhenLocalEpochChanges` —
///   `validate_heartbeat_request_assignment_sent_when_local_epoch_changes` (Phase 35)
/// - `testHeartbeatState` — `heartbeat_state_field_diff_lifecycle` (Phase 35)
/// - `testRackIdInHeartbeatLifecycle` — `rack_id_in_heartbeat_lifecycle` (Phase 35)
/// - `testRegexInHeartbeatLifecycle` — `regex_in_heartbeat_lifecycle` (Phase 35)
/// - `testRegexInJoiningHeartbeat` — `regex_in_joining_heartbeat` (Phase 35)
/// - `testPollTimerExpiration` — `poll_timer_expiration` (Phase 35)
/// - `testPollTimerExpirationShouldNotMarkMemberStaleIfMemberAlreadyLeaving` —
///   `poll_timer_expiration_should_not_mark_member_stale_if_member_already_leaving` (Phase 35)
/// - `testPollOnLeaving` — `poll_on_leaving` (Phase 35; asserts the
///   `should_send_leave_heartbeat_now` predicate directly — see note in the test)
/// - `testPollOnCloseGeneratesRequestIfNeeded` — `poll_on_close_generates_request_if_needed` (Phase 35)
/// - `testSendingLeaveGroupHeartbeatWhenPreviousOneInFlight` —
///   `sending_leave_group_heartbeat_when_previous_one_in_flight` (Phase 35)
/// - `testisExpiredByUsedForLogging` — `is_expired_by_used_for_logging` (Phase 35;
///   the `isExpiredBy` value drives only the warn log — no metric is recorded)
/// - `testConsumerAcksReconciledAssignmentAfterAckLost` —
///   `consumer_acks_reconciled_assignment_after_ack_lost` (Phase 35)
///
/// Genuinely not translated:
/// - `testSuccessfulHeartbeatTiming` (REDUCED — full timing matrix not reproduced;
///   `successful_response_updates_interval` + `timer_not_due` cover the timing core).
/// - Metrics tests (HeartbeatMetrics): OUT_OF_SCOPE — no Rust metrics framework.
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
            None,
            Arc::new(crate::common::metrics::time::SystemTime),
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

    // ===============================================================
    // Phase 35 — heartbeat request-field diff helpers + tests.
    // ===============================================================

    /// Default group id used by Java's `ConsumerHeartbeatRequestManagerTest`.
    const DEFAULT_GROUP_ID: &str = "groupId";
    /// Default server assignor.
    const DEFAULT_REMOTE_ASSIGNOR: &str = "uniform";
    /// Default group instance id (static membership).
    const DEFAULT_GROUP_INSTANCE_ID: &str = "group-instance-id";
    /// Default member epoch returned by the broker.
    const DEFAULT_MEMBER_EPOCH: i32 = 1;

    /// Builder for the request-field-diff tests. Lets a test control the
    /// group instance id, server assignor, rack id and rebalance timeout
    /// (Java mocks these getters on the membership manager). Returns the
    /// manager plus the shared `SubscriptionState` so the test can drive
    /// `subscribe` / regex changes (Java mocks `subscriptions.subscription()`
    /// / `subscriptionPattern()`).
    fn make_field_diff(
        group_instance_id: Option<String>,
        server_assignor: Option<String>,
        rack_id: Option<String>,
        rebalance_timeout_ms: i32,
        initial_interval_ms: Option<i64>,
    ) -> (
        ConsumerHeartbeatRequestManager,
        Arc<CoordinatorRequestManager>,
        Arc<ConsumerMembershipManager>,
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
        let coord = Arc::new(CoordinatorRequestManager::new(100, 1_000, "g"));
        let mm = Arc::new(ConsumerMembershipManager::new(
            DEFAULT_GROUP_ID,
            group_instance_id,
            rack_id,
            rebalance_timeout_ms,
            server_assignor,
            subs.clone(),
            None,
            metadata,
            beh.clone(),
            true,
            None,
            Arc::new(crate::common::metrics::time::SystemTime),
        ));
        let mut hb = ConsumerHeartbeatRequestManager::new(0, &config, coord.clone(), subs.clone(), mm.clone(), beh);
        if let Some(interval) = initial_interval_ms {
            hb.inner.heartbeat_request_state.update_heartbeat_interval_ms(0, interval);
        }
        (hb, coord, mm, subs)
    }

    /// Force the membership manager's state (Java mocks
    /// `when(membershipManager.state()).thenReturn(...)`). Bypasses
    /// transition validity — the field-diff tests only need the member
    /// parked in a given state to observe `build_request_data`'s
    /// `send_all_fields` (JOINING) gate.
    fn force_state(mm: &ConsumerMembershipManager, state: MemberState) {
        mm.abstract_mm.inner.lock().unwrap().state = state;
    }

    /// Force member id + epoch (Java mocks `memberId()` / `memberEpoch()`).
    fn force_member(mm: &ConsumerMembershipManager, member_id: &str, epoch: i32) {
        let mut g = mm.abstract_mm.inner.lock().unwrap();
        g.member_id = member_id.to_string();
        g.member_epoch = epoch;
    }

    /// Force the current local assignment (Java mocks `currentAssignment()`).
    fn force_current_assignment(mm: &ConsumerMembershipManager, assignment: LocalAssignment) {
        mm.abstract_mm.inner.lock().unwrap().current_assignment = assignment;
    }

    /// Set the subscription topic set on the shared `SubscriptionState`
    /// (Java mocks `subscriptions.subscription()`).
    fn set_subscription(subs: &Arc<Mutex<SubscriptionState>>, topics: &[&str]) {
        let set: std::collections::HashSet<String> = topics.iter().map(|s| s.to_string()).collect();
        subs.lock().unwrap().subscribe_topics(set, None).unwrap();
    }

    /// Set (or clear) the RE2J subscription pattern on the shared
    /// `SubscriptionState` (Java mocks `subscriptions.subscriptionPattern()`).
    fn set_pattern(subs: &Arc<Mutex<SubscriptionState>>, pattern: Option<&str>) {
        use crate::consumer::SubscriptionPattern;
        subs.lock()
            .unwrap()
            .set_subscription_pattern_for_test(pattern.map(SubscriptionPattern::new));
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
    /// expired. (AK 4.3.1: the member must NOT be UNSUBSCRIBED, else the
    /// KAFKA-20426 short-circuit returns i64::MAX first — see
    /// `maximum_time_to_wait_returns_max_when_unsubscribed`.)
    #[test]
    fn maximum_time_to_wait_returns_zero_when_poll_timer_expired() {
        let (mgr, _coord, mm) = make_with_coord(None);
        mm.transition_to_joining().unwrap();
        // Default max.poll.interval.ms is 300_000; advance past it.
        assert_eq!(mgr.maximum_time_to_wait(300_001), 0);
    }

    /// AK 4.3.1 (KAFKA-20426): `maximum_time_to_wait` returns `i64::MAX` when
    /// the member is UNSUBSCRIBED (e.g. manual assignment with no group), so
    /// the app thread can block for the full poll timeout instead of spinning.
    /// Translated from
    /// `ConsumerHeartbeatRequestManagerTest#testMaximumTimeToWaitWhenHeartbeatShouldBeSkipped`
    /// (both `isUnsubscribed` parameter values).
    #[test]
    fn maximum_time_to_wait_returns_max_when_unsubscribed() {
        // isUnsubscribed = true: UNSUBSCRIBED (default) -> i64::MAX.
        let (mgr, _c, mm) = make_with_coord(Some(0));
        assert_eq!(mm.state(), MemberState::Unsubscribed);
        assert_eq!(
            mgr.maximum_time_to_wait(0),
            i64::MAX,
            "maximumTimeToWait must return i64::MAX when UNSUBSCRIBED to prevent a busy loop",
        );

        // isUnsubscribed = false (JOINING): the zero heartbeat interval timer
        // has already expired, so it returns 0.
        let (mgr2, _c2, mm2) = make_with_coord(Some(0));
        mm2.transition_to_joining().unwrap();
        assert_eq!(
            mgr2.maximum_time_to_wait(0),
            0,
            "maximumTimeToWait must return 0 when the heartbeat interval timer has expired",
        );
    }

    /// AK 4.3.1: `GROUP_ID_NOT_FOUND` while the member is UNSUBSCRIBED is a
    /// benign skip (the leave is effectively complete) — NOT a fatal error.
    /// Translated from
    /// `ConsumerHeartbeatRequestManagerTest#testGroupIdNotFoundExceptionWhileUnsubscribed`.
    /// (`testGroupIdNotFoundWhileStableIsFatal` is a recorded skip: the Rust
    /// GROUP_ID_NOT_FOUND handling keeps the Issue-9 epoch-conditional recovery
    /// for non-unsubscribed members instead of Java's fatal treatment — a
    /// pre-existing deviation.)
    #[test]
    fn group_id_not_found_while_unsubscribed_is_skipped() {
        let (mut mgr, _coord, mm) = make_with_coord(None);
        assert_eq!(mm.state(), MemberState::Unsubscribed);
        let action = mgr.handle_specific_exception_in_response(
            crate::common::protocol::Errors::GroupIdNotFound,
            "group not found",
            0,
        );
        assert!(
            matches!(action, Some(HeartbeatErrorAction::Handled)),
            "GROUP_ID_NOT_FOUND while UNSUBSCRIBED must be skipped (Handled), not fatal/fenced",
        );
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
    #[tokio::test]
    async fn handle_specific_failure_unsupported_version_emits_error_event() {
        let (mut mgr, _coord, mm, mut beh_rx) = make_with_coord_capturing_events(None);
        make_joining(&mm);
        // Build an UnsupportedVersion error WITHOUT the regex-not-supported
        // tag so we hit the CONSUMER_PROTOCOL_NOT_SUPPORTED_MSG branch.
        let err = crate::common::KafkaError::unsupported_version("broker too old".to_string());
        let fatal = mgr.handle_specific_failure(&err, 12_345);
        assert!(fatal, "UnsupportedVersion must be classified as fatal");

        // Java (`ConsumerHeartbeatRequestManager.java:109`) routes this
        // through `handleFatalFailure`, so BOTH halves must happen: the
        // ErrorEvent and the fatal transition.
        let mut events = Vec::new();
        while let Ok(env) = beh_rx.try_recv() {
            events.push(env);
        }
        assert_eq!(
            error_events(&events),
            vec![Errors::UnsupportedVersion],
            "exactly one ErrorEvent carrying UNSUPPORTED_VERSION"
        );
        assert!(
            matches!(&events[0].event,
                crate::consumer::internals::events::background_event::BackgroundEvent::Error { error }
                    if error.to_string().contains(CONSUMER_PROTOCOL_NOT_SUPPORTED_MSG)),
            "the ErrorEvent must carry the consumer-protocol-not-supported message, got: {:?}",
            events[0].event
        );
        assert_eq!(
            mm.state(),
            MemberState::Fatal,
            "handle_specific_failure must apply the fatal transition, as Java's \
             handleFatalFailure does"
        );
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

    /// Drives a heartbeat, routes the supplied error-code response through
    /// the spawned forwarder, and returns the `PollResult` of the ONE
    /// `poll(now)` that drains and classifies it, plus the background-event
    /// envelopes that classification emitted.
    ///
    /// The membership transition is applied inline by that same poll (Java
    /// applies it inside the response callback), so the caller asserts on
    /// `mm.state()` and on the returned events — there is no side-channel
    /// to drain.
    ///
    /// Classification is deterministic rather than raced: we wait for the
    /// forwarder to deliver the completion, then poll exactly once. So the
    /// returned `PollResult` is precisely "what the manager did in response
    /// to this error", which is what the no-extra-heartbeat assertions need.
    async fn drive_error_response_and_collect(
        mgr: &mut ConsumerHeartbeatRequestManager,
        mm: &Arc<ConsumerMembershipManager>,
        coord: &Arc<CoordinatorRequestManager>,
        beh_rx: &mut mpsc::UnboundedReceiver<
            crate::consumer::internals::events::background_event::BackgroundEventEnvelope,
        >,
        error_code: i16,
    ) -> (
        PollResult,
        Vec<crate::consumer::internals::events::background_event::BackgroundEventEnvelope>,
    ) {
        inject_error_response(mgr, mm, coord, error_code);
        classify_injected_response(mgr, beh_rx).await
    }

    /// Second half of [`drive_error_response_and_collect`]: waits for the
    /// forwarder to deliver the injected completion, then polls exactly once
    /// to drain, classify and apply.
    ///
    /// Split out because [`inject_error_response`] runs `make_joining`, which
    /// resets the member epoch — a test that needs to observe the epoch
    /// changing has to seed it BETWEEN injection and classification.
    async fn classify_injected_response(
        mgr: &mut ConsumerHeartbeatRequestManager,
        beh_rx: &mut mpsc::UnboundedReceiver<
            crate::consumer::internals::events::background_event::BackgroundEventEnvelope,
        >,
    ) -> (
        PollResult,
        Vec<crate::consumer::internals::events::background_event::BackgroundEventEnvelope>,
    ) {
        // Wait for the spawned forwarder to hand the completion to the
        // manager's channel. On the current-thread test runtime the
        // forwarder runs while we await.
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(200);
        while mgr.pending_completions_empty_for_test() {
            assert!(
                std::time::Instant::now() < deadline,
                "the spawned forwarder did not deliver the heartbeat completion within 200ms"
            );
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }

        // Exactly one poll: step 0 drains the completion, classifies it and
        // applies the membership transition inline.
        let result = mgr.poll(0);
        let mut events = Vec::new();
        while let Ok(env) = beh_rx.try_recv() {
            events.push(env);
        }
        (result, events)
    }

    /// Emits the first heartbeat and completes it with `error_code`.
    ///
    /// The completion is routed through the spawned forwarder, so the
    /// response is classified by a later `poll(now)` (whose step 0 drains
    /// the completion), not by this call. Extracted from
    /// [`drive_error_response_and_collect`] so a test can also observe the
    /// polls BETWEEN classification and the side-channel drain.
    fn inject_error_response(
        mgr: &mut ConsumerHeartbeatRequestManager,
        mm: &Arc<ConsumerMembershipManager>,
        coord: &Arc<CoordinatorRequestManager>,
        error_code: i16,
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
    }

    /// Phase 12.5 round-2 regression for Issue 4 (Fenced path).
    ///
    /// When the broker returns a `FENCED_MEMBER_EPOCH` error in the
    /// heartbeat response body, Java
    /// (`AbstractHeartbeatRequestManager.java:411-418`) calls
    /// `membershipManager().transitionToFenced()` synchronously inside
    /// the `whenComplete` lambda; Rust applies it inline in the same
    /// response handler.
    ///
    /// Test shape:
    /// 1. Drive a heartbeat, route a `FENCED_MEMBER_EPOCH` response
    ///    through the forwarder, then poll once so the drain
    ///    classifies (helper).
    /// 2. Assert the membership state reflects the post-fence flow:
    ///    with no assignment, `transition_to_fenced` runs FENCED →
    ///    (no listener) → JOINING (Java's
    ///    `transitionToFenced(callbackHandlerSupplier)` rejoins
    ///    immediately when there's nothing to release).
    /// 3. Assert NO `BackgroundEvent::Error` envelope was emitted —
    ///    Java treats the fence as an INTERNAL state-machine event
    ///    and does NOT call `backgroundEventHandler.add(...)` on the
    ///    fence path (`AbstractHeartbeatRequestManager.java:411-427`).
    #[tokio::test]
    async fn issue4_fenced_member_epoch_drives_transition_to_fenced() {
        let (mut mgr, coord, mm, mut beh_rx) = make_with_coord_capturing_events(Some(0));

        inject_error_response(&mut mgr, &mm, &coord, Errors::FencedMemberEpoch.code());

        // Give the member a non-zero epoch before the classifying poll: the
        // post-fence state is JOINING, which is also where it starts, so
        // state alone cannot witness that the fence happened.
        // `transition_to_fenced` resets the epoch (Java's `resetEpoch()`
        // right after `transitionTo(FENCED)`,
        // `AbstractMembershipManager.java:416-417`), so the epoch can. This
        // has to happen after the injection, whose `make_joining` resets it.
        force_member(&mm, "member-1", 42);
        assert_ne!(mm.member_epoch(), mm.join_group_epoch(), "epoch seeded for the fence to reset");

        let (_result, events) = classify_injected_response(&mut mgr, &mut beh_rx).await;

        // The classifying poll applied the transition inline. No assignment
        // is held, so it ran Joining → Fenced → (no listener, no partitions
        // to release) → Joining. The intermediate Fenced state is exercised
        // by `state == Fenced` checks inside transition_to_fenced; what we
        // observe externally is the post-rejoin state. See
        // `consumer_membership_manager.rs::transition_to_fenced` for the
        // FENCED → JOINING tail.
        assert_eq!(
            mm.state(),
            MemberState::Joining,
            "after the fence with an empty assignment, state should rejoin to JOINING"
        );
        assert_eq!(
            mm.member_epoch(),
            mm.join_group_epoch(),
            "the fence must reset the member epoch (Java `resetEpoch()`)"
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
    /// `membershipManager().transitionToFatal()`.
    ///
    /// Test shape:
    /// 1. Drive a heartbeat, route a `GROUP_AUTHORIZATION_FAILED`
    ///    response through the forwarder, then poll once so the drain
    ///    classifies (helper).
    /// 2. Assert the membership state is `Fatal` — the transition is
    ///    applied inline by that poll, as in Java.
    /// 3. Assert a `BackgroundEvent::Error` carrying the original error
    ///    code reached the application (Java's `handleFatalFailure`
    ///    ErrorEvent).
    #[tokio::test]
    async fn issue4_group_authorization_failed_drives_transition_to_fatal() {
        let (mut mgr, coord, mm, mut beh_rx) = make_with_coord_capturing_events(Some(0));

        let (_result, events) = drive_error_response_and_collect(
            &mut mgr,
            &mm,
            &coord,
            &mut beh_rx,
            Errors::GroupAuthorizationFailed.code(),
        )
        .await;

        assert_eq!(
            mm.state(),
            MemberState::Fatal,
            "GROUP_AUTHORIZATION_FAILED must classify to Fatal and apply the transition inline"
        );

        // The error is also surfaced to the user via the background
        // event handler (matches Java's `handleFatalFailure` —
        // `backgroundEventHandler.add(new ErrorEvent(error))`).
        let fatal_errors = error_events(&events);
        assert_eq!(fatal_errors.len(), 1, "exactly one BackgroundEvent::Error on the fatal path");
        assert_eq!(
            fatal_errors[0],
            Errors::GroupAuthorizationFailed,
            "the ErrorEvent must carry the original error code"
        );
    }

    /// Extracts the `Errors` code of every `BackgroundEvent::Error` in
    /// `events`, so the fatal-path tests can assert on what the
    /// application actually sees.
    fn error_events(
        events: &[crate::consumer::internals::events::background_event::BackgroundEventEnvelope],
    ) -> Vec<Errors> {
        use crate::consumer::internals::events::background_event::BackgroundEvent;
        events
            .iter()
            .filter_map(|env| match &env.event {
                BackgroundEvent::Error { error } => Some(error.error()),
                _ => None,
            })
            .collect()
    }

    /// A `Fatal` classification must stop the heartbeat stream at once,
    /// not one bg-task iteration later.
    ///
    /// Java applies `membershipManager().transitionToFatal()`
    /// synchronously inside `handleFatalFailure`
    /// (`AbstractHeartbeatRequestManager.java:455-458`), so the classifying
    /// `poll()` leaves the member FATAL and every later one takes the
    /// skip-heartbeat short-circuit
    /// (`AbstractMembershipManager.java:754-760`). Exactly one `ErrorEvent`
    /// reaches the application.
    ///
    /// This pins that end-to-end, because deferring the transition breaks
    /// it: the member stays in its pre-transition state, the classifying
    /// `poll()` emits another heartbeat, and since the condition that
    /// produced the classification still holds, that heartbeat's response
    /// re-runs it. The application then sees the first error through the
    /// call that observed the failure and a duplicate through the NEXT
    /// consumer API call — failing a call Java guarantees succeeds. That is
    /// the intermittent failure of
    /// `test_re2j_pattern_subscription_invalid_regex`, whose
    /// `unsubscribe()` follows the `poll()` that surfaced the error.
    ///
    /// `InvalidRegularExpression` is the probe because it is that
    /// integration test's error, and because the broker returns it for
    /// every heartbeat carrying the bad pattern — so the duplicate is a
    /// certainty once a second heartbeat goes out, not a race.
    #[tokio::test]
    async fn fatal_classification_stops_heartbeats_at_once() {
        let (mut mgr, coord, mm, mut beh_rx) = make_with_coord_capturing_events(Some(0));

        let (result, events) = drive_error_response_and_collect(
            &mut mgr,
            &mm,
            &coord,
            &mut beh_rx,
            Errors::InvalidRegularExpression.code(),
        )
        .await;

        // The classifying poll must not emit another heartbeat: Java is
        // already FATAL at this instant.
        assert!(
            result.unsent_requests.is_empty(),
            "the poll that classified the fatal error must not emit another heartbeat — its \
             response would enqueue a duplicate ErrorEvent that the next consumer API call returns"
        );
        assert_eq!(
            error_events(&events),
            vec![Errors::InvalidRegularExpression],
            "handle_fatal_failure must emit exactly one ErrorEvent, carrying the broker's code"
        );
        assert_eq!(mm.state(), MemberState::Fatal, "the transition is applied inline, as in Java");

        // And it stays stopped, on Java's own predicate, with no further
        // error reaching the application.
        for _ in 0..3 {
            assert!(
                mgr.poll(0).unsent_requests.is_empty(),
                "should_skip_heartbeat() must keep the stream stopped in FATAL"
            );
            // The bg loop must also still get a real wait out of us: a 0 here
            // would spin it, since `poll(now)` has nothing to send.
            assert!(
                mgr.maximum_time_to_wait(0) > 0,
                "maximum_time_to_wait must not return 0 once the member is FATAL — the bg loop \
                 would poll with a 0 timeout and spin"
            );
        }
        assert!(beh_rx.try_recv().is_err(), "no duplicate ErrorEvent may reach the application");
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
    ///    through the forwarder, then poll once so the drain
    ///    classifies (helper).
    /// 2. Assert the member is `Fatal` — the fallback arm fired and
    ///    applied the transition inline.
    /// 3. Assert a `BackgroundEvent::Error` carrying the ORIGINAL
    ///    (unknown) error code reached the application, matching Java's
    ///    `handleFatalFailure` ErrorEvent propagation.
    #[tokio::test]
    async fn issue5_unknown_error_code_falls_through_to_fatal() {
        let (mut mgr, coord, mm, mut beh_rx) = make_with_coord_capturing_events(Some(0));

        let (_result, events) =
            drive_error_response_and_collect(&mut mgr, &mm, &coord, &mut beh_rx, Errors::RebalanceInProgress.code())
                .await;

        assert_eq!(
            mm.state(),
            MemberState::Fatal,
            "unknown error code REBALANCE_IN_PROGRESS must classify to Fatal (Java \
             `AbstractHeartbeatRequestManager.java:435-441` `default:` arm)"
        );

        // Java `handleFatalFailure` (`:455-458`) emits an ErrorEvent
        // alongside the fatal transition so the user observes the
        // failure from `poll()`. The Rust Fatal arm must do the same.
        assert_eq!(
            error_events(&events),
            vec![Errors::RebalanceInProgress],
            "the unknown-code fatal fallback must surface exactly one ErrorEvent, carrying the \
             original error code rather than a substituted one"
        );
    }

    // ===============================================================
    // Phase 35 — poll-timer / leave-group poll lifecycle.
    // ===============================================================

    /// Default `max.poll.interval.ms` (matches `ConsumerConfig::new`).
    const DEFAULT_MAX_POLL_INTERVAL_MS: i64 = 300_000;

    /// Is the STALE-path assignment release still outstanding?
    /// `on_heartbeat_request_generated` sets this when it moves the member to
    /// STALE, and only the release tail of `transition_to_stale` clears it —
    /// so it distinguishes "the release ran" from "nothing happened".
    fn stale_release_pending(mm: &Arc<ConsumerMembershipManager>) -> bool {
        let guard = match mm.abstract_mm.inner.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        guard.stale_assignment_release_pending
    }

    /// Translated from
    /// `ConsumerHeartbeatRequestManagerTest#testPollTimerExpiration`.
    /// On poll-timer expiration the member sends a last (leave) heartbeat,
    /// is transitioned to STALE (no further heartbeats), and resumes
    /// heartbeating after `reset_poll_timer` + `maybe_rejoin_stale_member`
    /// bring it back to JOINING.
    #[tokio::test]
    async fn poll_timer_expiration() {
        let (mut mgr, coord, mm) = make_with_coord(Some(0));
        set_coordinator(&coord);
        make_joining(&mm);
        // Arm the poll timer (Java arms it at construction; Rust arms on the
        // first reset_poll_timer — Issue 9).
        mgr.inner.reset_poll_timer(0);

        // Poll past max.poll.interval.ms: a leave heartbeat is generated and
        // the member transitions to STALE.
        let result = mgr.poll(DEFAULT_MAX_POLL_INTERVAL_MS);
        assert_eq!(
            result.unsent_requests.len(),
            1,
            "a leave heartbeat must be sent on poll-timer expiry"
        );
        assert_eq!(
            mm.state(),
            MemberState::Stale,
            "poll-timer expiry must transition the member to STALE"
        );
        // The assignment release ran inline (mirrors Java's transitionToStale
        // tail). With no partitions owned the §31 callback is a completed
        // no-op, so the release tail has already cleared the pending flag
        // that `on_heartbeat_request_generated` set — nothing is left for the
        // bg task to drive.
        assert!(
            !mm.has_pending_release(),
            "with no partitions owned the release completes inline, leaving nothing pending"
        );
        assert!(
            !stale_release_pending(&mm),
            "the stale release tail must have run, clearing stale_assignment_release_pending"
        );

        // STALE member skips heartbeats.
        let result = mgr.poll(DEFAULT_MAX_POLL_INTERVAL_MS);
        assert_eq!(result.unsent_requests.len(), 0, "a STALE member must not send heartbeats");

        // Reset the poll timer (application polled again) and rejoin.
        mgr.inner.reset_poll_timer(DEFAULT_MAX_POLL_INTERVAL_MS);
        mm.abstract_mm.maybe_rejoin_stale_member(mm.join_group_epoch());
        assert_eq!(mm.state(), MemberState::Joining, "after timer reset the member rejoins");
        assert!(!mgr.inner.poll_timer_is_expired(DEFAULT_MAX_POLL_INTERVAL_MS));

        // JOINING member resumes heartbeating.
        let result = mgr.poll(DEFAULT_MAX_POLL_INTERVAL_MS);
        assert_eq!(result.unsent_requests.len(), 1, "the rejoined member resumes heartbeating");
    }

    /// Translated from
    /// `ConsumerHeartbeatRequestManagerTest#testPollTimerExpirationShouldNotMarkMemberStaleIfMemberAlreadyLeaving`.
    /// A member already leaving the group when the poll timer expires must
    /// NOT be transitioned to STALE — it continues sending heartbeats to
    /// complete the ongoing leave.
    #[tokio::test]
    async fn poll_timer_expiration_should_not_mark_member_stale_if_member_already_leaving() {
        let (mut mgr, coord, mm) = make_with_coord(Some(0));
        set_coordinator(&coord);
        // Drive the member to LEAVING (a user-initiated leave, not poll-timer).
        mgr.inner.reset_poll_timer(0);
        make_joining(&mm);
        mm.leave_group(0).await.unwrap();
        assert_eq!(mm.state(), MemberState::Leaving);
        assert!(mm.is_leaving_group());

        // Poll past max.poll.interval.ms.
        let result = mgr.poll(DEFAULT_MAX_POLL_INTERVAL_MS);

        // No poll-timer-driven leave transition (member was already leaving);
        // the member is NOT STALE.
        assert_ne!(
            mm.state(),
            MemberState::Stale,
            "an already-leaving member must not be marked STALE"
        );
        assert!(
            !stale_release_pending(&mm) && !mm.has_pending_release(),
            "no stale assignment release should be triggered for an already-leaving member"
        );
        // A heartbeat is still generated to complete the ongoing leave.
        assert_eq!(
            result.unsent_requests.len(),
            1,
            "a heartbeat request should be generated to complete the ongoing leaving operation"
        );
    }

    /// Translated from
    /// `ConsumerHeartbeatRequestManagerTest#testFirstHeartbeatIncludesRequiredInfoToJoinGroupAndGetAssignments`.
    /// The FIRST heartbeat (JOINING) carries member id, epoch 0, the
    /// subscribed topics, the rebalance timeout, group id, instance id,
    /// server assignor and rack id.
    #[tokio::test]
    async fn first_heartbeat_includes_required_info_to_join_group() {
        let (mut mgr, _coord, mm, subs) = make_field_diff(
            Some(DEFAULT_GROUP_INSTANCE_ID.to_string()),
            Some(DEFAULT_REMOTE_ASSIGNOR.to_string()),
            Some("rack-1".to_string()),
            DEFAULT_MAX_POLL_INTERVAL_MS as i32,
            Some(0),
        );
        set_subscription(&subs, &["topic1"]);
        // Joining member, epoch 0 (real transition).
        mm.transition_to_joining().unwrap();
        assert_eq!(mm.state(), MemberState::Joining);

        let data = mgr.build_request_data_for_test();

        // Member id present and non-empty (Java's assertNotNull / assertFalse
        // isEmpty); the real Rust member id is a random UUID. The request must
        // carry the member's own id.
        assert!(!data.member_id.is_empty());
        assert_eq!(data.member_id, mm.member_id());
        assert_eq!(data.member_epoch, 0);
        assert_eq!(data.subscribed_topic_names, Some(vec!["topic1".to_string()]));
        assert_eq!(data.rebalance_timeout_ms, DEFAULT_MAX_POLL_INTERVAL_MS as i32);
        assert_eq!(data.group_id, DEFAULT_GROUP_ID);
        assert_eq!(data.instance_id, Some(DEFAULT_GROUP_INSTANCE_ID.to_string()));
        assert_eq!(data.server_assignor, Some(DEFAULT_REMOTE_ASSIGNOR.to_string()));
        assert_eq!(data.rack_id, Some("rack-1".to_string()));
    }

    /// Translated from
    /// `ConsumerHeartbeatRequestManagerTest#testValidateConsumerGroupHeartbeatRequest`.
    /// A STABLE member's heartbeat carries the required group/member fields
    /// with their correct values plus the subscription, rebalance timeout,
    /// instance id and server assignor.
    #[tokio::test]
    async fn validate_consumer_group_heartbeat_request() {
        let (mut mgr, _coord, mm, subs) = make_field_diff(
            Some(DEFAULT_GROUP_INSTANCE_ID.to_string()),
            Some(DEFAULT_REMOTE_ASSIGNOR.to_string()),
            None,
            10_000,
            Some(0),
        );
        set_subscription(&subs, &["topic"]);
        // Stable member with broker-supplied member id + epoch.
        force_state(&mm, MemberState::Stable);
        force_member(&mm, "member-id", DEFAULT_MEMBER_EPOCH);
        force_current_assignment(&mm, LocalAssignment::new(0, std::collections::HashMap::new()).unwrap());

        let data = mgr.build_request_data_for_test();
        assert_eq!(data.group_id, DEFAULT_GROUP_ID);
        assert_eq!(data.member_id, "member-id");
        assert_eq!(data.member_epoch, DEFAULT_MEMBER_EPOCH);
        assert_eq!(data.rebalance_timeout_ms, 10_000);
        assert_eq!(data.subscribed_topic_names, Some(vec!["topic".to_string()]));
        assert_eq!(data.instance_id, Some(DEFAULT_GROUP_INSTANCE_ID.to_string()));
        assert_eq!(data.server_assignor, Some(DEFAULT_REMOTE_ASSIGNOR.to_string()));
    }

    /// Translated from
    /// `ConsumerHeartbeatRequestManagerTest#testValidateConsumerGroupHeartbeatRequestAssignmentSentWhenLocalEpochChanges`.
    /// The assignment (topicPartitions) is sent on the first heartbeat,
    /// OMITTED on the next when unchanged, and RE-SENT when the local epoch
    /// of the current assignment changes.
    #[tokio::test]
    async fn validate_heartbeat_request_assignment_sent_when_local_epoch_changes() {
        let (mut mgr, _coord, mm, _subs) =
            make_field_diff(None, Some(DEFAULT_REMOTE_ASSIGNOR.to_string()), None, 10_000, Some(0));
        // Force a should-heartbeat-now state so build is exercised; we call
        // build_request_data directly so only the diff matters.
        force_state(&mm, MemberState::Stable);

        let topic_id = Uuid::random_uuid();
        let mut partitions = std::collections::HashMap::new();
        partitions.insert(topic_id, vec![0]);

        // First heartbeat: include assignment (local epoch 0).
        force_current_assignment(&mm, LocalAssignment::new(0, partitions.clone()).unwrap());
        let data1 = mgr.build_request_data_for_test();
        let tps1 = data1.topic_partitions.expect("first HB must include topic partitions");
        assert_eq!(tps1.len(), 1);
        assert_eq!(tps1[0].topic_id, topic_id);
        assert_eq!(tps1[0].partitions, vec![0]);

        // Assignment unchanged (same local epoch): omitted.
        let data2 = mgr.build_request_data_for_test();
        assert_eq!(data2.topic_partitions, None, "unchanged assignment must be omitted");

        // Local epoch bumped: re-sent.
        force_current_assignment(&mm, LocalAssignment::new(1, partitions.clone()).unwrap());
        let data3 = mgr.build_request_data_for_test();
        let tps3 = data3
            .topic_partitions
            .expect("assignment must be re-sent after local epoch change");
        assert_eq!(tps3.len(), 1);
        assert_eq!(tps3[0].topic_id, topic_id);
        assert_eq!(tps3[0].partitions, vec![0]);
    }

    /// Translated from
    /// `ConsumerHeartbeatRequestManagerTest#testHeartbeatState`.
    /// Exercises the full `build_request_data` field-diff lifecycle:
    /// join (all fields) → stable (unchanged fields omitted, epoch updated)
    /// → rejoin (JOINING re-sends all fields) → steady (subscription resent
    /// only on change).
    #[tokio::test]
    async fn heartbeat_state_field_diff_lifecycle() {
        let (mut mgr, _coord, mm, subs) = make_field_diff(
            None,
            Some(DEFAULT_REMOTE_ASSIGNOR.to_string()),
            None,
            DEFAULT_MAX_POLL_INTERVAL_MS as i32,
            Some(0),
        );
        // JOINING member, epoch 0, no instance id, no subscription yet.
        force_state(&mm, MemberState::Joining);
        force_member(&mm, "member-id", 0);
        force_current_assignment(&mm, LocalAssignment::none());

        // Initial HB sets most fields to their initial empty values.
        let data = mgr.build_request_data_for_test();
        assert_eq!(data.group_id, DEFAULT_GROUP_ID);
        assert_eq!(data.member_id, "member-id");
        assert_eq!(data.member_epoch, 0);
        assert_eq!(data.instance_id, None);
        assert_eq!(data.rebalance_timeout_ms, DEFAULT_MAX_POLL_INTERVAL_MS as i32);
        assert_eq!(data.subscribed_topic_names, Some(vec![]));
        assert_eq!(data.server_assignor, Some(DEFAULT_REMOTE_ASSIGNOR.to_string()));
        assert_eq!(data.topic_partitions, Some(vec![]));

        // Broker supplies a new epoch; move to STABLE. Mirrors Java's
        // `mockStableMemberData`, which sets the current assignment to
        // `LocalAssignment(0, emptyMap)` — a CHANGE from the JOINING build's
        // `LocalAssignment.NONE` (epoch -1), so topicPartitions is re-sent as
        // an empty list. The rebalance timeout, subscribed names and assignor
        // are unchanged, so they are omitted (-1 / null / null).
        force_state(&mm, MemberState::Stable);
        force_member(&mm, "member-id", 1);
        force_current_assignment(&mm, LocalAssignment::new(0, std::collections::HashMap::new()).unwrap());
        let data = mgr.build_request_data_for_test();
        assert_eq!(data.group_id, DEFAULT_GROUP_ID);
        assert_eq!(data.member_id, "member-id");
        assert_eq!(data.member_epoch, 1);
        assert_eq!(data.instance_id, None);
        assert_eq!(
            data.rebalance_timeout_ms, -1,
            "unchanged rebalance timeout must be omitted (-1)"
        );
        assert_eq!(
            data.subscribed_topic_names, None,
            "unchanged subscription must be omitted (null)"
        );
        assert_eq!(data.server_assignor, None, "unchanged assignor must be omitted (null)");
        assert_eq!(
            data.topic_partitions,
            Some(vec![]),
            "assignment changed (NONE -> epoch-0 empty), re-sent as empty list"
        );

        // Rejoin (JOINING) + subscribe a topic: all fields re-sent.
        set_subscription(&subs, &["topic1"]);
        force_state(&mm, MemberState::Joining);
        force_member(&mm, "member-id", 0);
        let data = mgr.build_request_data_for_test();
        assert_eq!(data.member_epoch, 0);
        assert_eq!(data.rebalance_timeout_ms, DEFAULT_MAX_POLL_INTERVAL_MS as i32);
        assert_eq!(data.subscribed_topic_names, Some(vec!["topic1".to_string()]));
        assert_eq!(data.server_assignor, Some(DEFAULT_REMOTE_ASSIGNOR.to_string()));
        assert_eq!(data.topic_partitions, Some(vec![]));

        // Another JOINING build: still re-sends (send_all_fields on JOINING).
        let data = mgr.build_request_data_for_test();
        assert_eq!(data.subscribed_topic_names, Some(vec!["topic1".to_string()]));
        assert_eq!(data.server_assignor, Some(DEFAULT_REMOTE_ASSIGNOR.to_string()));
    }

    /// Translated from
    /// `ConsumerHeartbeatRequestManagerTest#testRackIdInHeartbeatLifecycle`.
    /// rackId is included only on JOINING; omitted otherwise; an absent rack
    /// id is never sent.
    #[tokio::test]
    async fn rack_id_in_heartbeat_lifecycle() {
        let (mut mgr, _coord, mm, _subs) = make_field_diff(
            None,
            Some(DEFAULT_REMOTE_ASSIGNOR.to_string()),
            Some("rack1".to_string()),
            DEFAULT_MAX_POLL_INTERVAL_MS as i32,
            Some(0),
        );
        // Initial heartbeat with rackId (JOINING).
        force_state(&mm, MemberState::Joining);
        let data = mgr.build_request_data_for_test();
        assert_eq!(data.rack_id, Some("rack1".to_string()));

        // RackId omitted when not JOINING.
        force_state(&mm, MemberState::Stable);
        let data = mgr.build_request_data_for_test();
        assert_eq!(data.rack_id, None);

        // RackId included again when JOINING again.
        force_state(&mm, MemberState::Joining);
        let data = mgr.build_request_data_for_test();
        assert_eq!(data.rack_id, Some("rack1".to_string()));

        // Absent rack id is never sent (new manager with no rack id).
        let (mut mgr2, _c, mm2, _s) = make_field_diff(
            None,
            Some(DEFAULT_REMOTE_ASSIGNOR.to_string()),
            None,
            DEFAULT_MAX_POLL_INTERVAL_MS as i32,
            Some(0),
        );
        force_state(&mm2, MemberState::Joining);
        let data = mgr2.build_request_data_for_test();
        assert_eq!(data.rack_id, None);
    }

    /// Translated from
    /// `ConsumerHeartbeatRequestManagerTest#testRegexInHeartbeatLifecycle`.
    /// The regex is sent on change, "" is sent to clear the pattern, and the
    /// field is omitted when the pattern is unchanged.
    #[tokio::test]
    async fn regex_in_heartbeat_lifecycle() {
        let (mut mgr, _coord, mm, subs) = make_field_diff(
            None,
            Some(DEFAULT_REMOTE_ASSIGNOR.to_string()),
            None,
            DEFAULT_MAX_POLL_INTERVAL_MS as i32,
            Some(0),
        );
        // Initial heartbeat with regex (JOINING).
        force_state(&mm, MemberState::Joining);
        set_pattern(&subs, Some("t1.*"));
        let data = mgr.build_request_data_for_test();
        assert_eq!(data.subscribed_topic_regex, Some("t1.*".to_string()));

        // Regex omitted if unchanged (STABLE, same pattern).
        force_state(&mm, MemberState::Stable);
        let data = mgr.build_request_data_for_test();
        assert_eq!(data.subscribed_topic_regex, None);

        // Regex included if changed.
        set_pattern(&subs, Some("t2.*"));
        let data = mgr.build_request_data_for_test();
        assert_eq!(data.subscribed_topic_regex, Some("t2.*".to_string()));

        // Empty regex sent to remove the pattern subscription.
        set_pattern(&subs, None);
        let data = mgr.build_request_data_for_test();
        assert_eq!(data.subscribed_topic_regex, Some(String::new()));

        // Regex omitted after the pattern was already removed.
        set_pattern(&subs, None);
        let data = mgr.build_request_data_for_test();
        assert_eq!(data.subscribed_topic_regex, None);
    }

    /// Translated from
    /// `ConsumerHeartbeatRequestManagerTest#testRegexInJoiningHeartbeat`.
    /// "" is sent to unsubscribe from the regex; a JOINING rejoin with no
    /// pattern omits the regex field.
    #[tokio::test]
    async fn regex_in_joining_heartbeat() {
        let (mut mgr, _coord, mm, subs) = make_field_diff(
            None,
            Some(DEFAULT_REMOTE_ASSIGNOR.to_string()),
            None,
            DEFAULT_MAX_POLL_INTERVAL_MS as i32,
            Some(0),
        );
        // Initial heartbeat with regex (JOINING).
        force_state(&mm, MemberState::Joining);
        set_pattern(&subs, Some("t1.*"));
        let data = mgr.build_request_data_for_test();
        assert_eq!(data.subscribed_topic_regex, Some("t1.*".to_string()));

        // Member unsubscribes from regex: "" is sent.
        set_pattern(&subs, None);
        let data = mgr.build_request_data_for_test();
        assert_eq!(data.subscribed_topic_regex, Some(String::new()));

        // Member rejoins (JOINING) with no pattern: regex field omitted.
        force_state(&mm, MemberState::Joining);
        set_pattern(&subs, None);
        let data = mgr.build_request_data_for_test();
        assert_eq!(data.subscribed_topic_regex, None);
    }

    // ===============================================================
    // Phase 35 — leave-group / poll-on-close lifecycle.
    // ===============================================================

    /// Translated from
    /// `ConsumerHeartbeatRequestManagerTest#testPollOnLeaving` (parameterized
    /// over the `pollOnLeavingMatrix`). A LEAVING member sends a leave
    /// heartbeat EXCEPT when it is dynamic (no group-instance-id) AND the
    /// leave operation is `RemainInGroup`.
    ///
    /// Java isolates the `shouldSendLeaveHeartbeatNow()` predicate by leaving
    /// `shouldHeartbeatNow()` at its Mockito `false` default. With a REAL
    /// membership manager a LEAVING member's `should_heartbeat_now()` is also
    /// `true`, so `poll()` cannot isolate the predicate — we assert
    /// `should_send_leave_heartbeat_now()` directly (the unit Java tests),
    /// and additionally assert that the full `poll()` DOES send a leave HB
    /// for the cases where the predicate is true.
    #[tokio::test]
    async fn poll_on_leaving() {
        use crate::consumer::close_options::GroupMembershipOperation;
        let matrix = [
            (None, GroupMembershipOperation::Default, true),
            (None, GroupMembershipOperation::LeaveGroup, true),
            (None, GroupMembershipOperation::RemainInGroup, false),
            (Some("gii".to_string()), GroupMembershipOperation::Default, true),
            (Some("gii".to_string()), GroupMembershipOperation::LeaveGroup, true),
            (Some("gii".to_string()), GroupMembershipOperation::RemainInGroup, true),
        ];
        for (instance_id, op, expect_leave_hb) in matrix {
            let (mut mgr, coord, mm, _subs) = make_field_diff(
                instance_id.clone(),
                Some(DEFAULT_REMOTE_ASSIGNOR.to_string()),
                None,
                10_000,
                Some(0),
            );
            set_coordinator(&coord);
            force_state(&mm, MemberState::Leaving);
            mm.set_leave_group_operation(op);

            assert_eq!(
                mgr.should_send_leave_heartbeat_now_for_test(),
                expect_leave_hb,
                "should_send_leave_heartbeat_now (instance_id={instance_id:?}, op={op:?})"
            );

            // For the cases where the leave HB must be sent, the full poll()
            // generates it.
            if expect_leave_hb {
                let result = mgr.poll(0);
                assert_eq!(
                    result.unsent_requests.len(),
                    1,
                    "LEAVING member (instance_id={instance_id:?}, op={op:?}) must send a leave heartbeat"
                );
            }
        }
    }

    /// Translated from
    /// `ConsumerHeartbeatRequestManagerTest#testPollOnCloseGeneratesRequestIfNeeded`
    /// (parameterized over the `pollOnLeavingMatrix`). `poll_on_close`
    /// generates a leave heartbeat iff the member is still leaving — and a
    /// dynamic member with `RemainInGroup` is treated as NOT leaving.
    #[tokio::test]
    async fn poll_on_close_generates_request_if_needed() {
        use crate::consumer::close_options::GroupMembershipOperation;
        let matrix = [
            (None, GroupMembershipOperation::Default, true),
            (None, GroupMembershipOperation::LeaveGroup, true),
            (None, GroupMembershipOperation::RemainInGroup, false),
            (Some("gii".to_string()), GroupMembershipOperation::Default, true),
            (Some("gii".to_string()), GroupMembershipOperation::LeaveGroup, true),
            (Some("gii".to_string()), GroupMembershipOperation::RemainInGroup, true),
        ];
        for (instance_id, op, expect_hb) in matrix {
            let (mut mgr, _coord, mm, _subs) = make_field_diff(
                instance_id.clone(),
                Some(DEFAULT_REMOTE_ASSIGNOR.to_string()),
                None,
                10_000,
                Some(0),
            );
            // A member still leaving when the manager closes is in LEAVING.
            force_state(&mm, MemberState::Leaving);
            mm.set_leave_group_operation(op);

            let result = mgr.poll_on_close(0);
            if expect_hb {
                assert_eq!(
                    result.unsent_requests.len(),
                    1,
                    "poll_on_close must generate a leave request while still leaving (instance_id={instance_id:?}, op={op:?})"
                );
            } else {
                assert!(
                    result.unsent_requests.is_empty(),
                    "poll_on_close must NOT generate a leave request for a dynamic RemainInGroup member"
                );
            }
        }
    }

    /// Java parity: `pollOnClose` routes its leave heartbeat through
    /// `makeHeartbeatRequest(currentTimeMs, true)`
    /// (`AbstractHeartbeatRequestManager.java:233`), which records the
    /// heartbeat-sent time (`:285`). The close-path leave heartbeat must update
    /// `last-heartbeat-seconds-ago` like the other two send sites do.
    #[tokio::test]
    async fn poll_on_close_records_heartbeat_sent_ms() {
        use crate::common::metrics::Metrics;
        use crate::consumer::internals::heartbeat_metrics_manager::HeartbeatMetricsManager;

        let (mut mgr, _coord, mm, _subs) =
            make_field_diff(None, Some(DEFAULT_REMOTE_ASSIGNOR.to_string()), None, 10_000, Some(0));
        let metrics = Arc::new(Metrics::new());
        let metrics_manager = Arc::new(HeartbeatMetricsManager::new(&metrics));
        mgr.set_metrics_manager(Arc::clone(&metrics_manager));

        // No heartbeat recorded yet → sentinel.
        assert_eq!(metrics_manager.last_heartbeat_ms_for_test(), -1);

        // A member still leaving when the manager closes is in LEAVING.
        force_state(&mm, MemberState::Leaving);

        let current_time_ms = 12_345;
        let result = mgr.poll_on_close(current_time_ms);
        assert_eq!(
            result.unsent_requests.len(),
            1,
            "poll_on_close must generate a leave request while leaving"
        );

        // The close-path leave heartbeat recorded the send time, so
        // `last-heartbeat-seconds-ago` is no longer stale.
        assert_eq!(
            metrics_manager.last_heartbeat_ms_for_test(),
            current_time_ms,
            "poll_on_close leave heartbeat must record record_heartbeat_sent_ms"
        );
    }

    /// Translated from
    /// `ConsumerHeartbeatRequestManagerTest#testSendingLeaveGroupHeartbeatWhenPreviousOneInFlight`.
    /// A regular heartbeat is in flight (so a normal HB is suppressed), but a
    /// transition to LEAVING forces a leave heartbeat to be sent regardless of
    /// the in-flight request; once the member is skip-heartbeat, no further HB.
    #[tokio::test]
    async fn sending_leave_group_heartbeat_when_previous_one_in_flight() {
        let (mut mgr, coord, mm, _subs) =
            make_field_diff(None, Some(DEFAULT_REMOTE_ASSIGNOR.to_string()), None, 10_000, Some(0));
        set_coordinator(&coord);
        // JOINING so the first HB fires.
        mm.transition_to_joining().unwrap();
        let result = mgr.poll(0);
        assert_eq!(result.unsent_requests.len(), 1);

        // Second poll: previous HB still in flight ⇒ no new HB.
        let result = mgr.poll(0);
        assert_eq!(
            result.unsent_requests.len(),
            0,
            "no heartbeat while a previous one is in-flight"
        );

        // Transition to LEAVING: a leave heartbeat is forced even with the
        // previous request in flight (`should_send_leave_heartbeat_now`).
        force_state(&mm, MemberState::Leaving);
        let result = mgr.poll(0);
        assert_eq!(
            result.unsent_requests.len(),
            1,
            "leave heartbeat must be sent even with a previous HB in-flight"
        );

        // Member becomes skip-heartbeat (e.g. UNSUBSCRIBED after the leave):
        // no further heartbeat.
        force_state(&mm, MemberState::Unsubscribed);
        let result = mgr.poll(0);
        assert_eq!(result.unsent_requests.len(), 0);
    }

    /// Translated from
    /// `ConsumerHeartbeatRequestManagerTest#testisExpiredByUsedForLogging`.
    /// On poll-timer expiry the member sends a leave heartbeat and
    /// `poll_timer_is_expired_by` reports a positive overdue value (used only
    /// for the warn log). After `reset_poll_timer` the timer is no longer
    /// expired.
    #[tokio::test]
    async fn is_expired_by_used_for_logging() {
        let (mut mgr, coord, mm, _subs) =
            make_field_diff(None, Some(DEFAULT_REMOTE_ASSIGNOR.to_string()), None, 10_000, Some(0));
        set_coordinator(&coord);
        mgr.inner.reset_poll_timer(0);
        mm.transition_to_joining().unwrap();

        // The poll timer uses the config's max.poll.interval.ms (300_000),
        // not the membership rebalance timeout (10_000) — distinct fields.
        let exceeded = 5i64;
        let now = DEFAULT_MAX_POLL_INTERVAL_MS + exceeded;
        // Overdue value is positive (logging helper).
        assert!(mgr.inner.poll_timer_is_expired(now));
        assert_eq!(mgr.inner.poll_timer_is_expired_by(now), exceeded);

        let result = mgr.poll(now);
        assert_eq!(result.unsent_requests.len(), 1, "leave heartbeat on poll-timer expiry");
        assert_eq!(mm.state(), MemberState::Stale);

        // After reset, the poll timer is not expired.
        mgr.inner.reset_poll_timer(now);
        assert!(!mgr.inner.poll_timer_is_expired(now));
        assert!(mgr.inner.poll_timer_is_expired_by(now) < 0, "not overdue after reset");
    }

    /// Translated from
    /// `ConsumerHeartbeatRequestManagerTest#testConsumerAcksReconciledAssignmentAfterAckLost`.
    /// After a heartbeat that acked an assignment is lost (the manager resets
    /// its `SentFields` via `reset_heartbeat_state`), the next heartbeat
    /// re-includes the subscription and the assignment (acting as the ack
    /// again).
    #[tokio::test]
    async fn consumer_acks_reconciled_assignment_after_ack_lost() {
        let (mut mgr, _coord, mm, subs) =
            make_field_diff(None, Some(DEFAULT_REMOTE_ASSIGNOR.to_string()), None, 10_000, Some(0));
        set_subscription(&subs, &["topic1"]);
        let topic_id = Uuid::random_uuid();
        let mut partitions = std::collections::HashMap::new();
        partitions.insert(topic_id, vec![0]);
        force_state(&mm, MemberState::Reconciling);
        force_current_assignment(&mm, LocalAssignment::new(0, partitions.clone()).unwrap());

        // First HB acks the assignment (topic + partitions present).
        let data1 = mgr.build_request_data_for_test();
        assert_eq!(data1.subscribed_topic_names, Some(vec!["topic1".to_string()]));
        let tps1 = data1.topic_partitions.expect("first HB includes topic partitions");
        assert_eq!(tps1[0].topic_id, topic_id);
        assert_eq!(tps1[0].partitions, vec![0]);

        // HB lost ⇒ the manager resets its SentFields tracker.
        mgr.reset_heartbeat_state();

        // The following HB re-includes the subscription AND the assignment
        // (acting as the ack again), because the reset cleared the diff state.
        let data2 = mgr.build_request_data_for_test();
        assert_eq!(data2.subscribed_topic_names, Some(vec!["topic1".to_string()]));
        let tps2 = data2.topic_partitions.expect("post-reset HB re-includes topic partitions");
        assert_eq!(tps2.len(), 1);
        assert_eq!(tps2[0].topic_id, topic_id);
        assert_eq!(tps2[0].partitions, vec![0]);
    }
}
