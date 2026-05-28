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

use crate::common::Uuid;
use crate::common::requests::consumer_group_heartbeat_request::{
    ConsumerGroupHeartbeatRequestBuilder, REGEX_RESOLUTION_NOT_SUPPORTED_MSG,
};
use crate::consumer::ConsumerConfig;
use crate::consumer::internals::events::background_event_handler::BackgroundEventHandler;
use crate::consumer_group_heartbeat_request_data::{
    ConsumerGroupHeartbeatRequestData, TopicPartitions as RequestTopicPartitions,
};

use super::abstract_heartbeat_request_manager::{
    AbstractHeartbeatRequestManager, CONSUMER_PROTOCOL_NOT_SUPPORTED_MSG, HeartbeatErrorAction,
    make_heartbeat_poll_result,
};
use super::abstract_membership_manager::LocalAssignment;
use super::consumer_membership_manager::ConsumerMembershipManager;
use super::coordinator_request_manager::CoordinatorRequestManager;
use super::member_state::MemberState;
use super::network_client_delegate::{PollResult, UnsentRequest};
use super::request_manager::RequestManager;
use super::subscription_state::SubscriptionState;

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
}

impl ConsumerHeartbeatRequestManager {
    /// Java: `ConsumerHeartbeatRequestManager(LogContext, Time, ConsumerConfig,
    /// CoordinatorRequestManager, SubscriptionState, ConsumerMembershipManager,
    /// BackgroundEventHandler, Metrics)`.
    pub(crate) fn new(
        current_time_ms: i64,
        config: &ConsumerConfig,
        coordinator_request_manager: Arc<Mutex<CoordinatorRequestManager>>,
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
        Self { inner, membership_manager, heartbeat_state }
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
    /// targeting the current coordinator node.
    fn build_heartbeat_request(&mut self) -> UnsentRequest {
        let data = self.heartbeat_state.build_request_data();
        let builder = Box::new(ConsumerGroupHeartbeatRequestBuilder::new(data));
        let node = {
            let coord = match self.inner.coordinator_request_manager.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            coord.coordinator().cloned()
        };
        UnsentRequest::new(builder, node)
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
            let _ = self.inner.background_event_handler.add(
                crate::consumer::internals::events::background_event::BackgroundEvent::Error {
                    error: KafkaError::unsupported_version(message.to_string()),
                },
                current_time_ms,
            );
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
        // 1. Skip-heartbeat short-circuit.
        let coordinator_known = {
            let coord = match self.inner.coordinator_request_manager.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            coord.coordinator().is_some()
        };
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
            // Build leave heartbeat (ignoreResponse=true).
            let request = self.build_heartbeat_request();
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

        let request = self.build_heartbeat_request();
        // Java: this is `makeHeartbeatRequest(currentTimeMs, false)` —
        // record send attempt, reset timer, increment metrics.
        // membershipManager().onHeartbeatRequestGenerated() advances
        // ACKNOWLEDGING / LEAVING / etc.
        if let Err(e) = self.membership_manager.abstract_mm.on_heartbeat_request_generated() {
            log::warn!("on_heartbeat_request_generated failed: {}", e);
        }
        make_heartbeat_poll_result(request, &mut self.inner, current_time_ms)
    }

    fn poll_on_close(&mut self, _current_time_ms: i64) -> PollResult {
        // Java: if (membershipManager().isLeavingGroup()) send the
        // leave heartbeat.
        if self.membership_manager.is_leaving_group() {
            let request = self.build_heartbeat_request();
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
        Arc<Mutex<CoordinatorRequestManager>>,
        Arc<ConsumerMembershipManager>,
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
        let coord = Arc::new(Mutex::new(CoordinatorRequestManager::new(100, 1_000, "g")));
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
        (hb, coord, mm)
    }

    /// Test helper: inject a coordinator so `poll` doesn't short-circuit
    /// on "coordinator unknown". Mirrors Mockito
    /// `when(coordinatorRequestManager.coordinator()).thenReturn(...)`.
    fn set_coordinator(coord: &Arc<Mutex<CoordinatorRequestManager>>) {
        coord
            .lock()
            .unwrap()
            .set_coordinator_for_test(Node::new(0, "localhost".to_string(), 9092));
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
    #[test]
    fn heartbeat_on_startup() {
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
    #[test]
    fn timer_not_due() {
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
    #[test]
    fn heartbeat_not_sent_if_another_one_in_flight() {
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
    #[test]
    fn heartbeat_outside_interval() {
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
}
