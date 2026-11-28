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

//! `ShareHeartbeatRequestManager` — the heartbeat request manager for share
//! groups (KIP-932). Composes
//! [`super::AbstractHeartbeatRequestManager`]`<ShareGroupHeartbeatResponse>`
//! and implements its [`super::HeartbeatRequestManagerHooks`]; see the base
//! for the heartbeat lifecycle.
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.ShareHeartbeatRequestManager`.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use crate::ShareGroupHeartbeatRequestData;
use crate::common::Error;
use crate::common::errors::UnsupportedVersionError;
use crate::common::metrics::Metrics;
use crate::common::protocol::Errors;
use crate::common::requests::share_group_heartbeat_request;
use crate::common::requests::{ConcreteResponse, ShareGroupHeartbeatResponse};
use crate::consumer::ConsumerConfig;
use crate::consumer::internals::ConsumerUtils;
use crate::consumer::internals::events::{BackgroundEvent, BackgroundEventHandler};
use crate::consumer::internals::metrics::HeartbeatMetricsManager;

use super::CoordinatorRequestManager;
use super::HeartbeatRequestState;
use super::MemberState;
use super::RequestManager;
use super::ShareMembershipManager;
use super::SubscriptionState;
use super::{AbstractHeartbeatRequestManager, HeartbeatErrorAction, HeartbeatRequestManagerHooks};
use super::{PollResult, UnsentRequest};

/// Fields of `ShareGroupHeartbeatRequest` sent in the most recent request.
///
/// Java: `ShareHeartbeatRequestManager.HeartbeatState.SentFields`.
#[derive(Default)]
#[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareHeartbeatRequestManager$HeartbeatState$SentFields")]
pub(crate) struct SentFields {
    /// Java: `private String rackId = null`.
    rack_id: Option<String>,
    /// Java: `private TreeSet<String> subscribedTopicNames = null`.
    subscribed_topic_names: Option<BTreeSet<String>>,
}

impl SentFields {
    /// Java: `SentFields()`.
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareHeartbeatRequestManager$HeartbeatState$SentFields#SentFields"
    )]
    fn new() -> Self {
        Self::default()
    }

    /// Java: `void reset()`.
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareHeartbeatRequestManager$HeartbeatState$SentFields#reset"
    )]
    fn reset(&mut self) {
        self.rack_id = None;
        self.subscribed_topic_names = None;
    }
}

/// Builds the heartbeat requests correctly, ensuring that all information is
/// sent according to the protocol, but subsequent requests do not send
/// information which has not changed. This is important to ensure that
/// reconciliation completes successfully.
///
/// Java: `static class ShareHeartbeatRequestManager.HeartbeatState`.
#[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareHeartbeatRequestManager$HeartbeatState")]
pub(crate) struct HeartbeatState {
    subscriptions: Arc<Mutex<SubscriptionState>>,
    share_membership_manager: Arc<ShareMembershipManager>,
    sent_fields: SentFields,
}

impl HeartbeatState {
    /// Java: `public HeartbeatState(SubscriptionState, ShareMembershipManager)`.
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareHeartbeatRequestManager$HeartbeatState#HeartbeatState"
    )]
    pub(crate) fn new(
        subscriptions: Arc<Mutex<SubscriptionState>>,
        share_membership_manager: Arc<ShareMembershipManager>,
    ) -> Self {
        Self { subscriptions, share_membership_manager, sent_fields: SentFields::new() }
    }

    /// Java: `public void reset()`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareHeartbeatRequestManager$HeartbeatState#reset")]
    pub(crate) fn reset(&mut self) {
        self.sent_fields.reset();
    }

    /// Java: `public ShareGroupHeartbeatRequestData buildRequestData()`.
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareHeartbeatRequestManager$HeartbeatState#buildRequestData"
    )]
    pub(crate) fn build_request_data(&mut self) -> ShareGroupHeartbeatRequestData {
        let mut data = ShareGroupHeartbeatRequestData::new();

        // GroupId - always sent.
        data.set_group_id(self.share_membership_manager.group_id());

        // MemberId - always sent, it will be generated at Consumer startup.
        data.set_member_id(self.share_membership_manager.member_id());

        // MemberEpoch - always sent.
        data.set_member_epoch(self.share_membership_manager.member_epoch());

        // RackId - only sent the first time, because it does not change.
        if self.sent_fields.rack_id.is_none() {
            let rack_id = self.share_membership_manager.rack_id().map(str::to_string);
            data.set_rack_id(rack_id.clone());
            self.sent_fields.rack_id = rack_id;
        }

        let send_all_fields = self.share_membership_manager.state() == MemberState::Joining;

        // SubscribedTopicNames - only sent when joining or if it has changed
        // since the last heartbeat.
        let subscribed_topic_names: BTreeSet<String> = {
            let subs = match self.subscriptions.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            subs.subscription().into_iter().collect()
        };
        if send_all_fields || self.sent_fields.subscribed_topic_names.as_ref() != Some(&subscribed_topic_names) {
            // Java sends `new ArrayList<>(subscriptions.subscription())`, a
            // `TreeSet`, so the names go out sorted.
            data.set_subscribed_topic_names(Some(subscribed_topic_names.iter().cloned().collect()));
            self.sent_fields.subscribed_topic_names = Some(subscribed_topic_names);
        }

        data
    }
}

/// This is the heartbeat request manager for share groups. See
/// [`AbstractHeartbeatRequestManager`] for more details.
///
/// Java: `public class ShareHeartbeatRequestManager extends
/// AbstractHeartbeatRequestManager<ShareGroupHeartbeatResponse>`.
#[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareHeartbeatRequestManager")]
pub(crate) struct ShareHeartbeatRequestManager {
    inner: AbstractHeartbeatRequestManager<ShareGroupHeartbeatResponse>,
    /// Membership manager for share groups.
    membership_manager: Arc<ShareMembershipManager>,
    /// HeartbeatState manages building the heartbeat requests correctly.
    heartbeat_state: HeartbeatState,
}

impl ShareHeartbeatRequestManager {
    /// Java: `SHARE_PROTOCOL_NOT_SUPPORTED_MSG`.
    pub(crate) const SHARE_PROTOCOL_NOT_SUPPORTED_MSG: &str = "The cluster does not support the share group protocol. \
         To use share groups, the cluster must have the share group protocol enabled.";

    /// Java: `SHARE_PROTOCOL_VERSION_NOT_SUPPORTED_MSG`.
    pub(crate) const SHARE_PROTOCOL_VERSION_NOT_SUPPORTED_MSG: &str = "The cluster does not support the share group \
         protocol using ShareGroupHeartbeat API version 1 or later. This version of the API was introduced in Apache \
         Kafka v4.1.";

    /// Java: `ShareHeartbeatRequestManager(LogContext, Time, ConsumerConfig,
    /// CoordinatorRequestManager, SubscriptionState, ShareMembershipManager,
    /// BackgroundEventHandler, Metrics)`. The heartbeat metrics go in the
    /// `consumer-share` group (`CONSUMER_SHARE_METRIC_GROUP_PREFIX`).
    /// `current_time_ms` replaces Java's `Time`.
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareHeartbeatRequestManager#ShareHeartbeatRequestManager"
    )]
    pub(crate) fn new(
        current_time_ms: i64,
        config: &ConsumerConfig,
        coordinator_request_manager: Arc<CoordinatorRequestManager>,
        subscriptions: Arc<Mutex<SubscriptionState>>,
        membership_manager: Arc<ShareMembershipManager>,
        background_event_handler: Arc<BackgroundEventHandler>,
        metrics: &Arc<Metrics>,
    ) -> Self {
        let mut inner = AbstractHeartbeatRequestManager::new(
            current_time_ms,
            config,
            coordinator_request_manager,
            background_event_handler,
        );
        inner.metrics_manager = Some(Arc::new(HeartbeatMetricsManager::with_prefix(
            metrics,
            ConsumerUtils::CONSUMER_SHARE_METRIC_GROUP_PREFIX,
        )));
        let heartbeat_state = HeartbeatState::new(subscriptions, Arc::clone(&membership_manager));
        Self { inner, membership_manager, heartbeat_state }
    }

    /// Java's visible-for-testing `ShareHeartbeatRequestManager(LogContext,
    /// Timer, ConsumerConfig, CoordinatorRequestManager, ShareMembershipManager,
    /// HeartbeatState, HeartbeatRequestState, BackgroundEventHandler, Metrics)`.
    /// Java's `Timer` is the poll timer, which the Rust base arms on the first
    /// `reset_poll_timer` instead (see
    /// `AbstractHeartbeatRequestManager::poll_timer_expires_at_ms`).
    #[expect(clippy::too_many_arguments, reason = "the Java constructor's parameters")]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareHeartbeatRequestManager#ShareHeartbeatRequestManager"
    )]
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "Java's visible-for-testing constructor: only the tests call it")
    )]
    pub(crate) fn with_state(
        current_time_ms: i64,
        config: &ConsumerConfig,
        coordinator_request_manager: Arc<CoordinatorRequestManager>,
        membership_manager: Arc<ShareMembershipManager>,
        heartbeat_state: HeartbeatState,
        heartbeat_request_state: HeartbeatRequestState,
        background_event_handler: Arc<BackgroundEventHandler>,
        metrics: &Arc<Metrics>,
    ) -> Self {
        let mut inner = AbstractHeartbeatRequestManager::with_state(
            current_time_ms,
            config,
            coordinator_request_manager,
            heartbeat_request_state,
            background_event_handler,
        );
        inner.metrics_manager = Some(Arc::new(HeartbeatMetricsManager::with_prefix(
            metrics,
            ConsumerUtils::CONSUMER_SHARE_METRIC_GROUP_PREFIX,
        )));
        Self { inner, membership_manager, heartbeat_state }
    }

    /// Installs the bg task's wakeup `Notify`, so the spawned response
    /// forwarder can wake the network poll once a heartbeat completion is
    /// queued (see `AbstractHeartbeatRequestManager::completion_notify`).
    /// Rust-only, as `ConsumerHeartbeatRequestManager::set_completion_notify`.
    pub(crate) fn set_completion_notify(&mut self, notify: Arc<tokio::sync::Notify>) {
        self.inner.completion_notify = notify;
    }

    /// The composed base state.
    #[cfg_attr(not(test), expect(dead_code, reason = "only the tests read the base directly"))]
    pub(crate) fn inner(&self) -> &AbstractHeartbeatRequestManager<ShareGroupHeartbeatResponse> {
        &self.inner
    }

    /// Mutable access to the composed base state.
    #[cfg_attr(not(test), expect(dead_code, reason = "only the tests read the base directly"))]
    pub(crate) fn inner_mut(&mut self) -> &mut AbstractHeartbeatRequestManager<ShareGroupHeartbeatResponse> {
        &mut self.inner
    }

    /// Java: `resetPollTimer(long pollMs)` (inherited): if the poll timer has
    /// expired, the member rejoins (`membershipManager().maybeRejoinStaleMember()`),
    /// then the timer restarts. The Rust base cannot reach the membership
    /// manager, so the subclass performs the expiry check, as the consumer's
    /// `ApplicationEventProcessor` does around its `reset_poll_timer`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.AbstractHeartbeatRequestManager#resetPollTimer")]
    pub(crate) fn reset_poll_timer(&mut self, current_time_ms: i64) {
        if self.inner.poll_timer_is_expired(current_time_ms) {
            log::warn!(
                "Time between subsequent calls to poll() was longer than the configured max.poll.interval.ms, \
                 exceeded approximately by {} ms. Member {} will rejoin the group now.",
                self.inner.poll_timer_is_expired_by(current_time_ms),
                self.membership_manager.member_id()
            );
            self.membership_manager.maybe_rejoin_stale_member();
        }
        self.inner.reset_poll_timer(current_time_ms);
    }
}

/// The `ShareHeartbeatRequestManager` implementations of Java's abstract
/// `AbstractHeartbeatRequestManager` methods.
impl HeartbeatRequestManagerHooks for ShareHeartbeatRequestManager {
    type Response = ShareGroupHeartbeatResponse;
    type MembershipManager = ShareMembershipManager;

    fn base(&self) -> &AbstractHeartbeatRequestManager<ShareGroupHeartbeatResponse> {
        &self.inner
    }

    fn base_mut(&mut self) -> &mut AbstractHeartbeatRequestManager<ShareGroupHeartbeatResponse> {
        &mut self.inner
    }

    /// Java: `public ShareMembershipManager membershipManager()`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareHeartbeatRequestManager#membershipManager")]
    fn membership_manager(&self) -> &Arc<ShareMembershipManager> {
        &self.membership_manager
    }

    /// Java: `public String heartbeatRequestName()`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareHeartbeatRequestManager#heartbeatRequestName")]
    fn heartbeat_request_name(&self) -> &'static str {
        "ShareGroupHeartbeatRequest"
    }

    /// Java: `public NetworkClientDelegate.UnsentRequest buildHeartbeatRequest()`:
    /// a `ShareGroupHeartbeatRequest` built from [`HeartbeatState`], to the
    /// current coordinator.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareHeartbeatRequestManager#buildHeartbeatRequest")]
    fn build_heartbeat_request(&mut self) -> UnsentRequest {
        let data = self.heartbeat_state.build_request_data();
        let builder = Box::new(share_group_heartbeat_request::Builder::new(data));
        let node = self.inner.coordinator_request_manager.coordinator();
        UnsentRequest::new(builder, node)
    }

    fn response_body(body: ConcreteResponse) -> Option<ShareGroupHeartbeatResponse> {
        match body {
            ConcreteResponse::ShareGroupHeartbeat(response) => Some(response),
            _ => None,
        }
    }

    /// Java: `public Errors errorForResponse(ShareGroupHeartbeatResponse)` →
    /// `Errors.forCode(response.data().errorCode())`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareHeartbeatRequestManager#errorForResponse")]
    fn error_for_response(response: &ShareGroupHeartbeatResponse) -> Errors {
        Errors::for_code(response.data().error_code)
    }

    /// Java: `public String errorMessageForResponse(ShareGroupHeartbeatResponse)`
    /// → `response.data().errorMessage()`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareHeartbeatRequestManager#errorMessageForResponse")]
    fn error_message_for_response(response: &ShareGroupHeartbeatResponse) -> Option<&str> {
        response.data().error_message.as_deref()
    }

    /// Java: `public long heartbeatIntervalForResponse(ShareGroupHeartbeatResponse)`
    /// → `response.data().heartbeatIntervalMs()`.
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareHeartbeatRequestManager#heartbeatIntervalForResponse"
    )]
    fn heartbeat_interval_for_response(response: &ShareGroupHeartbeatResponse) -> i64 {
        i64::from(response.data().heartbeat_interval_ms)
    }

    /// An `UnsupportedVersionException` the client raised (the negotiated
    /// `ShareGroupHeartbeat` version is below 1) is fatal, reported with
    /// [`Self::SHARE_PROTOCOL_VERSION_NOT_SUPPORTED_MSG`] and the original
    /// error as its cause. Java's `handleFatalFailure` emits the error event,
    /// then transitions the member to FATAL.
    ///
    /// Java: `public boolean handleSpecificFailure(Throwable exception)`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareHeartbeatRequestManager#handleSpecificFailure")]
    fn handle_specific_failure(&mut self, error: &Error, current_time_ms: i64) -> bool {
        if !error.is_unsupported_version_error() {
            return false;
        }
        log::error!(
            "{} failed due to {}: {}",
            self.heartbeat_request_name(),
            error.message(),
            Self::SHARE_PROTOCOL_VERSION_NOT_SUPPORTED_MSG
        );
        let fatal_err = Error::UnsupportedVersion(UnsupportedVersionError::with_source(
            Self::SHARE_PROTOCOL_VERSION_NOT_SUPPORTED_MSG,
            error.clone(),
        ));
        // Java: `handleFatalFailure(new UnsupportedVersionException(...))`.
        let _ = self
            .inner
            .background_event_handler
            .add(BackgroundEvent::Error { error: fatal_err }, current_time_ms);
        if let Err(e) = self.membership_manager.transition_to_fatal(current_time_ms) {
            log::warn!("transition_to_fatal failed: {e}");
        }
        true
    }

    /// Broker responded with HB not supported, meaning the new protocol is
    /// not enabled, so propagate a custom message for it. Note that the case
    /// where the protocol is not supported at all should fail on the client
    /// side when building the request and checking supporting APIs (handled
    /// in [`Self::handle_specific_failure`]).
    ///
    /// Java: `public boolean handleSpecificExceptionInResponse(ShareGroupHeartbeatResponse,
    /// long)`: `UNSUPPORTED_VERSION` → `handleFatalFailure(error.exception(SHARE_PROTOCOL_NOT_SUPPORTED_MSG))`;
    /// any other error is not handled here, except `GROUP_ID_NOT_FOUND`, which
    /// Java handles in the base `onErrorResponse` (see the arm below).
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareHeartbeatRequestManager#handleSpecificExceptionInResponse"
    )]
    fn handle_specific_error_in_response(
        &mut self,
        error: Errors,
        error_message: Option<&str>,
        _current_time_ms: i64,
    ) -> Option<HeartbeatErrorAction> {
        match error {
            // Java handles GROUP_ID_NOT_FOUND in the base `onErrorResponse`
            // switch, for every subclass. The Rust base defers it to the
            // subclass so the consumer can keep its recorded epoch-conditional
            // recovery (see `ConsumerHeartbeatRequestManager`); the share
            // manager translates the Java arm as is.
            //
            // If the group doesn't exist (e.g., member never joined due to
            // InvalidTopicException), GROUP_ID_NOT_FOUND should be ignored -
            // the leave is effectively complete. When a leave heartbeat
            // (epoch=-1) is sent, the state transitions synchronously from
            // LEAVING to UNSUBSCRIBED in onHeartbeatRequestGenerated() before
            // the request is sent.
            Errors::GroupIdNotFound => {
                if self.membership_manager.state() == MemberState::Unsubscribed {
                    log::info!(
                        "{} received GROUP_ID_NOT_FOUND for group {} while unsubscribed. ",
                        self.heartbeat_request_name(),
                        self.membership_manager.group_id()
                    );
                    Some(HeartbeatErrorAction::Handled)
                } else {
                    // Else, this is a fatal error, we should throw it and
                    // transition to fatal state.
                    log::error!(
                        "{} failed due to unexpected error {:?}: {}",
                        self.heartbeat_request_name(),
                        error,
                        error_message.unwrap_or_default()
                    );
                    // Java: `error.exception(errorMessage)`, which falls back to
                    // the code's default message when `errorMessage` is null.
                    Some(HeartbeatErrorAction::Fatal(match error_message {
                        Some(message) => Error::with_message(error, message),
                        None => Error::new(error),
                    }))
                }
            },
            Errors::UnsupportedVersion => {
                log::error!(
                    "{} failed due to unsupported version: {}",
                    self.heartbeat_request_name(),
                    Self::SHARE_PROTOCOL_NOT_SUPPORTED_MSG
                );
                Some(HeartbeatErrorAction::Fatal(Error::with_message(
                    error,
                    Self::SHARE_PROTOCOL_NOT_SUPPORTED_MSG,
                )))
            },
            _ => None,
        }
    }

    /// Java: `public void resetHeartbeatState()`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareHeartbeatRequestManager#resetHeartbeatState")]
    fn reset_heartbeat_state(&mut self) {
        self.heartbeat_state.reset();
    }

    /// Java: `protected boolean shouldSendLeaveHeartbeatNow()` →
    /// `membershipManager().state() == MemberState.LEAVING`.
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.ShareHeartbeatRequestManager#shouldSendLeaveHeartbeatNow"
    )]
    fn should_send_leave_heartbeat_now(&self) -> bool {
        self.membership_manager.state() == MemberState::Leaving
    }
}

/// The base `poll` / `pollOnClose` / `maximumTimeToWait` of Java's
/// `AbstractHeartbeatRequestManager`, which this subclass inherits.
impl RequestManager for ShareHeartbeatRequestManager {
    fn poll(&mut self, current_time_ms: i64) -> PollResult {
        AbstractHeartbeatRequestManager::poll(self, current_time_ms)
    }

    fn poll_on_close(&mut self, current_time_ms: i64) -> PollResult {
        AbstractHeartbeatRequestManager::poll_on_close(self, current_time_ms)
    }

    fn maximum_time_to_wait(&self, current_time_ms: i64) -> i64 {
        AbstractHeartbeatRequestManager::maximum_time_to_wait(self, current_time_ms)
    }
}

/// Translation map of `ShareHeartbeatRequestManagerTest`, which extends
/// `AbstractHeartbeatRequestManagerTest<ShareGroupHeartbeatResponse>`, so JUnit
/// runs the base class's tests against the share manager too. They run here,
/// against `ShareHeartbeatRequestManager`, as the consumer file runs them
/// against `ConsumerHeartbeatRequestManager`.
///
/// Java mocks the membership manager, the subscription state, the coordinator
/// manager and the `HeartbeatState`. Rust uses the real ones and forces the
/// member's state (`force_state` / `force_member`); a Mockito `verify` becomes
/// the observable state change the call makes on the real object.
///
/// `ShareHeartbeatRequestManagerTest` (15 tests):
/// - `testMaximumTimeToWaitWhenHeartbeatShouldBeSkippedDoesNotSpin` — same name, snake case
/// - `testMaximumTimeToWaitWhenFatalReturnsMaxValue` — same name, snake case
/// - `testMaximumTimeToWaitWhenJoiningAndCoordinatorUnknownDoesNotSpin` — same name, snake case
/// - `testHeartbeatOnStartup` — `test_heartbeat_on_startup`
/// - `testFirstHeartbeatIncludesRequiredInfoToJoinGroupAndGetAssignments` (every
///   `SHARE_GROUP_HEARTBEAT` version) — same name, snake case
/// - `testSkippingHeartbeat` (`true`, `false`) — `test_skipping_heartbeat`
/// - `testHeartbeatNotSentIfAnotherOneInFlight` — same name, snake case
/// - `testNetworkTimeout` — `test_network_timeout`
/// - `testFailureOnFatalException` — `test_failure_on_fatal_exception`
/// - `testUnsupportedVersionGeneratedOnTheBroker` / `testUnsupportedVersionGeneratedOnTheClient` —
///   same names, snake case
/// - `testHeartbeatState` — `test_heartbeat_state`
/// - `testPollTimerExpiration` — `test_poll_timer_expiration`
/// - `testHeartbeatMetrics` (every `nextInt(11)` value) — `test_heartbeat_metrics`
///
/// `AbstractHeartbeatRequestManagerTest` (9 tests, inherited):
/// - `testTimerNotDue`, `testHeartbeatOutsideInterval`, `testNoCoordinator`,
///   `testPollTimerExpirationShouldNotMarkMemberStaleIfMemberAlreadyLeaving`,
///   `testSuccessfulHeartbeatTiming`,
///   `testLogsHeartbeatIntervalReceivedFromCoordinatorOnlyWhenChanged`,
///   `testGroupIdNotFoundExceptionWhileUnsubscribed`,
///   `testGroupIdNotFoundWhileStableIsFatal`,
///   `testHeartbeatResponseOnErrorHandling` (all 14 `errorProvider` rows) —
///   the same names in snake case. The consumer file skips
///   `testGroupIdNotFoundWhileStableIsFatal` because the consumer keeps a
///   recorded GROUP_ID_NOT_FOUND recovery; the share manager translates Java's
///   arm as is, so it runs here.
///
/// Rust-only: `test_new_registers_share_heartbeat_metrics` (the production
/// constructor, which Java's tests never call) and
/// `test_forwarder_wakes_bg_task_after_queueing_the_response`.
#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use tokio::sync::mpsc;

    use super::*;
    use crate::Metadata;
    use crate::ShareGroupHeartbeatResponseData;
    use crate::common::internals::ClusterResourceListeners;
    use crate::common::protocol::ApiKeys;
    use crate::common::requests::{
        AbstractRequest, RequestHeader, RequestHeaderOptionsBuilder, ShareGroupHeartbeatRequest,
    };
    use crate::common::utils::{MockTime, Time};
    use crate::common::{Metric, Node, Uuid};
    use crate::consumer::internals::AutoOffsetResetStrategy;
    use crate::consumer::internals::LocalAssignment;
    use crate::consumer::internals::MembershipManagerHooks;
    use crate::consumer::internals::events::BackgroundEventEnvelope;
    use crate::consumer::internals::metrics::ShareRebalanceMetricsManager;
    use crate::share_group_heartbeat_response_data::{Assignment, TopicPartitions};

    const DEFAULT_GROUP_ID: &str = "groupId";
    const DEFAULT_MEMBER_ID: &str = "member-id";
    const DEFAULT_MEMBER_EPOCH: i32 = 1;
    const DEFAULT_HEARTBEAT_INTERVAL_MS: i64 = 1_000;
    const DEFAULT_MAX_POLL_INTERVAL_MS: i64 = 10_000;
    const DEFAULT_RETRY_BACKOFF_MS: i64 = 80;
    const DEFAULT_RETRY_BACKOFF_MAX_MS: i64 = 1_000;
    const DEFAULT_HEARTBEAT_JITTER_MS: f64 = 0.0;
    const SHARE_CONSUMER_COORDINATOR_METRICS: &str = "consumer-share-coordinator-metrics";

    /// Java's `config()`: `max.poll.interval.ms`, `retry.backoff.ms` and
    /// `retry.backoff.max.ms` set to the test defaults.
    fn config() -> ConsumerConfig {
        ConsumerConfig {
            bootstrap_servers: vec!["localhost:9999".to_string()],
            max_poll_interval_ms: DEFAULT_MAX_POLL_INTERVAL_MS as i32,
            retry_backoff_ms: DEFAULT_RETRY_BACKOFF_MS,
            retry_backoff_max_ms: DEFAULT_RETRY_BACKOFF_MAX_MS,
            ..ConsumerConfig::defaults()
        }
    }

    /// Force the member's state (Java: `when(membershipManager.state()).thenReturn(..)`).
    fn force_state(mm: &ShareMembershipManager, state: MemberState) {
        mm.abstract_mm.inner.lock().unwrap().state = state;
    }

    /// Force member id and epoch (Java: `memberId()` / `memberEpoch()` stubs).
    fn force_member(mm: &ShareMembershipManager, member_id: &str, epoch: i32) {
        let mut g = mm.abstract_mm.inner.lock().unwrap();
        g.member_id = member_id.to_string();
        g.member_epoch = epoch;
    }

    /// Java's `@BeforeEach setUp()`, with the real collaborators. The member
    /// is STABLE (Java's mock answers `false` to both `shouldSkipHeartbeat()`
    /// and `shouldHeartbeatNow()`); the poll timer is armed at construction,
    /// as Java's `time.timer(DEFAULT_MAX_POLL_INTERVAL_MS)` is. Java's
    /// `HeartbeatState` is a mock; a real one is used.
    struct Fixture {
        /// Java's `MockTime`, shared with `metrics`.
        time: Arc<MockTime>,
        metrics: Arc<Metrics>,
        coord: Arc<CoordinatorRequestManager>,
        subs: Arc<Mutex<SubscriptionState>>,
        mm: Arc<ShareMembershipManager>,
        beh: Arc<BackgroundEventHandler>,
        events: mpsc::UnboundedReceiver<BackgroundEventEnvelope>,
        mgr: ShareHeartbeatRequestManager,
    }

    impl Fixture {
        fn new() -> Self {
            let time = Arc::new(MockTime::new());
            let metrics = Arc::new(Metrics::with_time(Arc::clone(&time) as Arc<dyn Time>));
            let subs = Arc::new(Mutex::new(SubscriptionState::new(AutoOffsetResetStrategy::LATEST)));
            let metadata = Arc::new(Metadata::new(100, 1_000, 300_000, ClusterResourceListeners::new()));
            let rebalance_metrics = Arc::new(Metrics::with_time(Arc::clone(&time) as Arc<dyn Time>));
            let mm = Arc::new(ShareMembershipManager::with_metrics_manager(
                DEFAULT_GROUP_ID,
                None,
                Arc::clone(&subs),
                metadata,
                Arc::clone(&time) as Arc<dyn Time>,
                Arc::new(ShareRebalanceMetricsManager::new(&rebalance_metrics)),
            ));
            let (tx, events) = mpsc::unbounded_channel();
            let beh = Arc::new(BackgroundEventHandler::new(tx));
            let coord = Arc::new(CoordinatorRequestManager::new(
                DEFAULT_RETRY_BACKOFF_MS,
                DEFAULT_RETRY_BACKOFF_MAX_MS,
                DEFAULT_GROUP_ID,
            ));
            // Java: `when(coordinatorRequestManager.coordinator()).thenReturn(Optional.of(new Node(1, "localhost", 9999)))`.
            coord.set_coordinator_for_test(Node::new(1, "localhost".to_string(), 9999));
            force_state(&mm, MemberState::Stable);
            force_member(&mm, DEFAULT_MEMBER_ID, DEFAULT_MEMBER_EPOCH);
            let mgr = Self::build(&time, &metrics, &coord, &subs, &mm, &beh, DEFAULT_HEARTBEAT_INTERVAL_MS);
            Self { time, metrics, coord, subs, mm, beh, events, mgr }
        }

        /// Java's visible-for-testing constructor with a fresh `HeartbeatState`
        /// and a `HeartbeatRequestState` of the given interval, its poll timer
        /// armed now.
        fn build(
            time: &Arc<MockTime>,
            metrics: &Arc<Metrics>,
            coord: &Arc<CoordinatorRequestManager>,
            subs: &Arc<Mutex<SubscriptionState>>,
            mm: &Arc<ShareMembershipManager>,
            beh: &Arc<BackgroundEventHandler>,
            heartbeat_interval_ms: i64,
        ) -> ShareHeartbeatRequestManager {
            let now = time.milliseconds();
            let heartbeat_request_state = HeartbeatRequestState::new(
                now,
                heartbeat_interval_ms,
                DEFAULT_RETRY_BACKOFF_MS,
                DEFAULT_RETRY_BACKOFF_MAX_MS,
                DEFAULT_HEARTBEAT_JITTER_MS,
            );
            let mut mgr = ShareHeartbeatRequestManager::with_state(
                now,
                &config(),
                Arc::clone(coord),
                Arc::clone(mm),
                HeartbeatState::new(Arc::clone(subs), Arc::clone(mm)),
                heartbeat_request_state,
                Arc::clone(beh),
                metrics,
            );
            mgr.inner.reset_poll_timer(now);
            mgr
        }

        /// Java's `createHeartbeatRequestManager(..)` with the current
        /// heartbeat interval (`testPollTimerExpiration`), or
        /// `createHeartbeatRequestStateWithZeroHeartbeatInterval()` with 0.
        /// Java passes `new Metrics()`.
        fn rebuild(&mut self, heartbeat_interval_ms: i64) {
            let metrics = Arc::new(Metrics::with_time(Arc::clone(&self.time) as Arc<dyn Time>));
            self.mgr = Self::build(
                &self.time,
                &metrics,
                &self.coord,
                &self.subs,
                &self.mm,
                &self.beh,
                heartbeat_interval_ms,
            );
        }

        fn now(&self) -> i64 {
            self.time.milliseconds()
        }

        /// Java's `time.sleep(ms)`.
        fn sleep(&self, ms: i64) {
            self.time.sleep(ms);
        }

        fn poll(&mut self) -> PollResult {
            let now = self.now();
            self.mgr.poll(now)
        }

        fn maximum_time_to_wait(&self) -> i64 {
            self.mgr.maximum_time_to_wait(self.now())
        }

        /// `AbstractHeartbeatRequestManagerTest#assertNextHeartbeatTiming`.
        fn assert_next_heartbeat_timing(&mut self, expected_time_to_next_heartbeat_ms: i64) {
            let now = self.now();
            let state = &mut self.mgr.inner.heartbeat_request_state;
            assert_eq!(expected_time_to_next_heartbeat_ms, state.time_to_next_heartbeat_ms(now));
            if expected_time_to_next_heartbeat_ms != 0 {
                assert!(!state.can_send_request(now));
                self.time.sleep(expected_time_to_next_heartbeat_ms);
            }
            let now = self.now();
            assert!(self.mgr.inner.heartbeat_request_state.can_send_request(now));
        }

        /// Every background event emitted so far.
        fn drain_events(&mut self) -> Vec<BackgroundEventEnvelope> {
            let mut events = Vec::new();
            while let Ok(envelope) = self.events.try_recv() {
                events.push(envelope);
            }
            events
        }

        /// The errors of every `ErrorEvent` emitted so far; any other event
        /// fails the test.
        fn drain_error_events(&mut self) -> Vec<Error> {
            self.drain_events()
                .into_iter()
                .map(|envelope| match envelope.event {
                    BackgroundEvent::Error { error } => error,
                    other => panic!("expected only Error events, got {other:?}"),
                })
                .collect()
        }

        /// Java runs the `whenComplete` lambda inside `onComplete` /
        /// `onFailure`; the Rust forwarder is a spawned task, so this waits for
        /// it to queue the completion and drains it at the mock time.
        async fn await_and_drain_completion(&mut self) {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            while self.mgr.inner.pending_completion_rx.is_empty() {
                assert!(
                    std::time::Instant::now() < deadline,
                    "the spawned forwarder did not deliver the heartbeat completion"
                );
                tokio::task::yield_now().await;
            }
            let now = self.now();
            AbstractHeartbeatRequestManager::drain_pending_completions(&mut self.mgr, now);
        }

        /// `request.handler().onComplete(createHeartbeatResponse(request, error,
        /// heartbeatIntervalMs))`.
        async fn complete(&mut self, request: &UnsentRequest, error: Errors, heartbeat_interval_ms: i64) {
            request
                .handler()
                .on_complete(create_heartbeat_response(error, heartbeat_interval_ms, self.now()));
            self.await_and_drain_completion().await;
        }

        /// `request.handler().onFailure(time.milliseconds(), error)`.
        async fn fail(&mut self, request: &UnsentRequest, error: Error) {
            request.handler().on_failure(self.now(), error);
            self.await_and_drain_completion().await;
        }

        /// `ShareHeartbeatRequestManagerTest#assertHeartbeat`. A heartbeat sent
        /// with `ignoreResponse` (the poll-timer leave) is completed but drives
        /// no state, so `drives_state` says whether to wait for its completion.
        async fn assert_heartbeat(&mut self, next_poll_ms: i64, drives_state: bool) {
            let poll_result = self.poll();
            assert_eq!(1, poll_result.unsent_requests.len());
            assert_eq!(next_poll_ms, poll_result.time_until_next_poll_ms);
            let request = &poll_result.unsent_requests[0];
            if drives_state {
                self.complete(request, Errors::None, DEFAULT_HEARTBEAT_INTERVAL_MS).await;
            } else {
                request.handler().on_complete(create_heartbeat_response(
                    Errors::None,
                    DEFAULT_HEARTBEAT_INTERVAL_MS,
                    self.now(),
                ));
            }
        }

        /// `ShareHeartbeatRequestManagerTest#assertNoHeartbeat`.
        fn assert_no_heartbeat(&mut self) {
            let poll_result = self.poll();
            assert_eq!(0, poll_result.unsent_requests.len());
        }

        /// `getMetric(name).metricValue()` in the `consumer-share-coordinator-metrics` group.
        fn metric(&self, name: &str) -> Option<f64> {
            let metric_name = self.metrics.metric_name(name, SHARE_CONSUMER_COORDINATOR_METRICS);
            self.metrics
                .metric(&metric_name)
                .map(|metric| metric.metric_value().as_double().expect("a numeric metric"))
        }
    }

    fn client_response(
        now_ms: i64,
        version_mismatch: Option<UnsupportedVersionError>,
        response: Option<ShareGroupHeartbeatResponse>,
    ) -> crate::ClientResponse {
        let header = RequestHeader::with_options(
            RequestHeaderOptionsBuilder::new()
                .set_request_api_key(&ApiKeys::SHARE_GROUP_HEARTBEAT)
                .set_request_version(ApiKeys::SHARE_GROUP_HEARTBEAT.latest_version())
                .set_client_id("client-id")
                .set_correlation_id(1)
                .build()
                .unwrap(),
        )
        .expect("header ok");
        crate::ClientResponse::with_timed_out(
            header,
            None,
            "0",
            now_ms,
            now_ms,
            false,
            false,
            version_mismatch,
            None,
            response.map(ConcreteResponse::ShareGroupHeartbeat),
        )
    }

    /// `ShareHeartbeatRequestManagerTest#createHeartbeatResponse(request, error,
    /// heartbeatIntervalMs)`: `DEFAULT_MEMBER_ID`, `DEFAULT_MEMBER_EPOCH`, and the
    /// stubbed error message only on an error.
    fn create_heartbeat_response(error: Errors, heartbeat_interval_ms: i64, now_ms: i64) -> crate::ClientResponse {
        let mut data = ShareGroupHeartbeatResponseData::new();
        data.set_error_code(error.code())
            .set_heartbeat_interval_ms(i32::try_from(heartbeat_interval_ms).expect("interval fits an i32"))
            .set_member_id(Some(DEFAULT_MEMBER_ID.to_string()))
            .set_member_epoch(DEFAULT_MEMBER_EPOCH);
        if error != Errors::None {
            data.set_error_message(Some("stubbed error message".to_string()));
        }
        client_response(now_ms, None, Some(ShareGroupHeartbeatResponse::new(data)))
    }

    /// `ShareHeartbeatRequestManagerTest#createHeartbeatResponseWithException`.
    fn create_heartbeat_response_with_exception(
        error: UnsupportedVersionError,
        is_from_broker: bool,
        now_ms: i64,
    ) -> crate::ClientResponse {
        if is_from_broker {
            let mut data = ShareGroupHeartbeatResponseData::new();
            data.set_error_code(Errors::UnsupportedVersion.code());
            client_response(now_ms, None, Some(ShareGroupHeartbeatResponse::new(data)))
        } else {
            client_response(now_ms, Some(error), None)
        }
    }

    /// Translated from `ShareHeartbeatRequestManagerTest#testMaximumTimeToWaitWhenHeartbeatShouldBeSkippedDoesNotSpin`
    /// (KAFKA-20253). FENCED skips heartbeats. Java stubs
    /// `timeToNextHeartbeatMs` to 0; Rust lets the interval elapse instead.
    #[tokio::test]
    async fn test_maximum_time_to_wait_when_heartbeat_should_be_skipped_does_not_spin() {
        let f = Fixture::new();
        force_state(&f.mm, MemberState::Fenced);
        f.sleep(DEFAULT_HEARTBEAT_INTERVAL_MS);
        let now = f.now();
        assert_eq!(0, f.mgr.inner.heartbeat_request_state.time_to_next_heartbeat_ms(now));

        let result = f.maximum_time_to_wait();

        assert!(
            result > 0,
            "maximumTimeToWait must be > 0 while heartbeats are skipped to avoid a busy-spin; got {result}"
        );
        assert_eq!(DEFAULT_RETRY_BACKOFF_MS, result);
    }

    /// Translated from `ShareHeartbeatRequestManagerTest#testMaximumTimeToWaitWhenFatalReturnsMaxValue`.
    #[tokio::test]
    async fn test_maximum_time_to_wait_when_fatal_returns_max_value() {
        let f = Fixture::new();
        force_state(&f.mm, MemberState::Fatal);

        assert_eq!(
            i64::MAX,
            f.maximum_time_to_wait(),
            "maximumTimeToWait should return Long.MAX_VALUE in the terminal FATAL state"
        );
    }

    /// Translated from `ShareHeartbeatRequestManagerTest#testMaximumTimeToWaitWhenJoiningAndCoordinatorUnknownDoesNotSpin`.
    /// JOINING is the real state where `shouldHeartbeatNow()` is true.
    #[tokio::test]
    async fn test_maximum_time_to_wait_when_joining_and_coordinator_unknown_does_not_spin() {
        let mut f = Fixture::new();
        f.rebuild(0);
        f.coord.mark_coordinator_unknown("test", f.now());
        force_state(&f.mm, MemberState::Joining);

        let result = f.maximum_time_to_wait();

        assert!(
            result > 0,
            "maximumTimeToWait must be > 0 while the member is joining and the coordinator is unknown to avoid a \
             busy-spin; got {result}"
        );
        assert_eq!(DEFAULT_RETRY_BACKOFF_MS, result);
    }

    /// Translated from `ShareHeartbeatRequestManagerTest#testHeartbeatOnStartup`.
    #[tokio::test]
    async fn test_heartbeat_on_startup() {
        let mut f = Fixture::new();
        let result = f.poll();
        assert_eq!(0, result.unsent_requests.len());

        f.rebuild(0);
        assert_eq!(0, f.maximum_time_to_wait());
        let result = f.poll();
        assert_eq!(1, result.unsent_requests.len());

        // Ensure we do not resend the request without the first request being completed
        let result2 = f.poll();
        assert_eq!(0, result2.unsent_requests.len());
    }

    /// Translated from `ShareHeartbeatRequestManagerTest#testFirstHeartbeatIncludesRequiredInfoToJoinGroupAndGetAssignments`
    /// (`@ApiKeyVersionsSource(apiKey = SHARE_GROUP_HEARTBEAT)`: every version).
    #[tokio::test]
    async fn test_first_heartbeat_includes_required_info_to_join_group_and_get_assignments() {
        for version in ApiKeys::SHARE_GROUP_HEARTBEAT.all_versions() {
            let mut f = Fixture::new();
            f.rebuild(0);
            f.sleep(DEFAULT_HEARTBEAT_INTERVAL_MS);
            let topic = "topic1";
            f.subs
                .lock()
                .unwrap()
                .subscribe_to_share_group(HashSet::from([topic.to_string()]))
                .unwrap();

            // Create a ShareGroupHeartbeatRequest and verify the payload
            // (`mockJoiningMemberData()`).
            force_state(&f.mm, MemberState::Joining);
            force_member(&f.mm, "", 0);
            assert_eq!(0, f.maximum_time_to_wait(), "version {version}");
            let mut poll_result = f.poll();
            assert_eq!(1, poll_result.unsent_requests.len(), "version {version}");
            let builder = poll_result.unsent_requests[0].request_builder_mut().expect("a request builder");
            assert_eq!(&ApiKeys::SHARE_GROUP_HEARTBEAT, builder.api_key(), "version {version}");

            let heartbeat_request = match builder.build_version(version).expect("build") {
                AbstractRequest::ShareGroupHeartbeat(request) => request,
                other => panic!("expected a ShareGroupHeartbeatRequest, got {other:?}"),
            };

            // Should include epoch 0 to join and no member ID.
            assert!(heartbeat_request.data().member_id.is_empty(), "version {version}");
            assert_eq!(0, heartbeat_request.data().member_epoch, "version {version}");

            // Should include subscription and group basic info to start getting assignments.
            assert_eq!(
                Some(vec![topic.to_string()]),
                heartbeat_request.data().subscribed_topic_names,
                "version {version}"
            );
            assert_eq!(DEFAULT_GROUP_ID, heartbeat_request.data().group_id, "version {version}");
        }
    }

    /// Translated from `ShareHeartbeatRequestManagerTest#testSkippingHeartbeat`
    /// (`@ValueSource(booleans = {true, false})`). UNSUBSCRIBED is a real state
    /// that skips heartbeats, STABLE one that does not.
    #[tokio::test]
    async fn test_skipping_heartbeat() {
        for should_skip_heartbeat in [true, false] {
            let mut f = Fixture::new();
            // The initial heartbeatInterval is set to 0
            f.rebuild(0);

            // Mocking notInGroup
            if should_skip_heartbeat {
                force_state(&f.mm, MemberState::Unsubscribed);
            }

            let result = f.poll();

            if !should_skip_heartbeat {
                assert_eq!(1, result.unsent_requests.len());
                assert_eq!(0, result.time_until_next_poll_ms);
            } else {
                assert_eq!(0, result.unsent_requests.len());
                assert_eq!(i64::MAX, result.time_until_next_poll_ms);
            }
        }
    }

    /// Translated from `ShareHeartbeatRequestManagerTest#testHeartbeatNotSentIfAnotherOneInFlight`.
    #[tokio::test]
    async fn test_heartbeat_not_sent_if_another_one_in_flight() {
        let mut f = Fixture::new();
        f.sleep(DEFAULT_HEARTBEAT_INTERVAL_MS);

        // Heartbeat sent (no response received)
        let result = f.poll();
        assert_eq!(1, result.unsent_requests.len());
        let inflight_req = result.unsent_requests.into_iter().next().unwrap();

        f.sleep(DEFAULT_HEARTBEAT_INTERVAL_MS);
        let result = f.poll();
        assert_eq!(
            0,
            result.unsent_requests.len(),
            "No heartbeat should be sent while a previous one in-flight"
        );

        f.sleep(DEFAULT_HEARTBEAT_INTERVAL_MS);
        let result = f.poll();
        assert_eq!(
            0,
            result.unsent_requests.len(),
            "No heartbeat should be sent when the interval expires if there is a previous heartbeat request in-flight"
        );

        // Receive response for the inflight after the interval expired. The next HB should be sent
        // on the next poll waiting only for the minimal backoff.
        f.complete(&inflight_req, Errors::None, DEFAULT_HEARTBEAT_INTERVAL_MS).await;
        f.sleep(DEFAULT_RETRY_BACKOFF_MS);
        let result = f.poll();
        assert_eq!(
            1,
            result.unsent_requests.len(),
            "A next heartbeat should be sent on the first poll after receiving a response that took longer than the \
             interval, waiting only for the minimal backoff."
        );
    }

    /// Translated from `ShareHeartbeatRequestManagerTest#testNetworkTimeout`.
    /// `verify(membershipManager).onHeartbeatFailure(true)`: a retriable failure
    /// records nothing on the share rebalance metrics, so its call is not
    /// observable; the member staying STABLE (not FATAL) is asserted instead.
    #[tokio::test]
    async fn test_network_timeout() {
        let mut f = Fixture::new();
        // The initial heartbeatInterval is set to 0
        f.rebuild(0);
        let result = f.poll();
        assert_eq!(1, result.unsent_requests.len());

        // Mimic network timeout
        f.fail(&result.unsent_requests[0], Error::timeout("timeout")).await;
        assert_eq!(MemberState::Stable, f.mm.state());
        assert!(f.drain_events().is_empty(), "backgroundEventHandler.add never called");

        // Assure the manager will backoff on timeout
        f.sleep(DEFAULT_RETRY_BACKOFF_MS - 1);
        let result = f.poll();
        assert_eq!(0, result.unsent_requests.len());

        f.sleep(1);
        let result = f.poll();
        assert_eq!(1, result.unsent_requests.len());
    }

    /// Translated from `ShareHeartbeatRequestManagerTest#testFailureOnFatalException`.
    #[tokio::test]
    async fn test_failure_on_fatal_exception() {
        let mut f = Fixture::new();
        // The initial heartbeatInterval is set to 0
        f.sleep(DEFAULT_HEARTBEAT_INTERVAL_MS);

        let result = f.poll();
        assert_eq!(1, result.unsent_requests.len());
        f.fail(&result.unsent_requests[0], Error::kafka_message("fatal")).await;
        assert_eq!(MemberState::Fatal, f.mm.state(), "transitionToFatal()");
        assert_eq!(1, f.drain_events().len(), "backgroundEventHandler.add(any())");
    }

    /// `ShareHeartbeatRequestManagerTest#mockResponseWithException` followed by
    /// the `ArgumentCaptor<ErrorEvent>` checks shared by both
    /// `testUnsupportedVersionGenerated*` tests.
    async fn assert_unsupported_version_error_event(error_msg: &str, is_from_broker: bool) {
        let mut f = Fixture::new();
        f.sleep(DEFAULT_HEARTBEAT_INTERVAL_MS);
        let result = f.poll();
        assert_eq!(1, result.unsent_requests.len());

        // Manually completing the response to test error handling
        let response =
            create_heartbeat_response_with_exception(UnsupportedVersionError::new(error_msg), is_from_broker, f.now());
        result.unsent_requests[0].handler().on_complete(response);
        f.await_and_drain_completion().await;

        let errors = f.drain_error_events();
        assert_eq!(1, errors.len(), "verify(backgroundEventHandler).add(..)");
        assert!(matches!(errors[0], Error::UnsupportedVersion(_)), "{:?}", errors[0]);
        assert_eq!(error_msg, errors[0].message());
    }

    /// Translated from `ShareHeartbeatRequestManagerTest#testUnsupportedVersionGeneratedOnTheBroker`
    /// (`@ValueSource(strings = {SHARE_PROTOCOL_NOT_SUPPORTED_MSG})`).
    #[tokio::test]
    async fn test_unsupported_version_generated_on_the_broker() {
        for error_msg in [ShareHeartbeatRequestManager::SHARE_PROTOCOL_NOT_SUPPORTED_MSG] {
            assert_unsupported_version_error_event(error_msg, true).await;
        }
    }

    /// Translated from `ShareHeartbeatRequestManagerTest#testUnsupportedVersionGeneratedOnTheClient`
    /// (`@ValueSource(strings = {SHARE_PROTOCOL_VERSION_NOT_SUPPORTED_MSG})`).
    /// The response's `versionMismatch` reaches `onFailure`, as in Java.
    #[tokio::test]
    async fn test_unsupported_version_generated_on_the_client() {
        for error_msg in [ShareHeartbeatRequestManager::SHARE_PROTOCOL_VERSION_NOT_SUPPORTED_MSG] {
            assert_unsupported_version_error_event(error_msg, false).await;
        }
    }

    /// Translated from `ShareHeartbeatRequestManagerTest#testHeartbeatState`.
    /// Java's `membershipManager.onHeartbeatRequestGenerated()` /
    /// `onHeartbeatSuccess(rs1)` calls go to a mock; they run on the real
    /// manager here, and the final `onHeartbeatSuccess` must succeed.
    #[tokio::test]
    async fn test_heartbeat_state() {
        let mut f = Fixture::new();
        // `mockJoiningMemberData()`.
        force_state(&f.mm, MemberState::Joining);
        force_member(&f.mm, "", 0);

        let mut heartbeat_state = HeartbeatState::new(Arc::clone(&f.subs), Arc::clone(&f.mm));

        f.rebuild(0);

        // The initial ShareGroupHeartbeatRequest sets most fields to their initial empty values
        let data = heartbeat_state.build_request_data();
        assert_eq!(DEFAULT_GROUP_ID, data.group_id);
        assert_eq!("", data.member_id);
        assert_eq!(0, data.member_epoch);
        assert_eq!(Some(Vec::<String>::new()), data.subscribed_topic_names);
        f.mm.on_heartbeat_request_generated().unwrap();

        // Mock a response from the group coordinator, that supplies the member ID and a new epoch
        force_state(&f.mm, MemberState::Stable);
        // `mockStableMemberData()`.
        f.mm.abstract_mm.inner.lock().unwrap().current_assignment =
            LocalAssignment::new(0, std::collections::HashMap::new()).unwrap();
        force_member(&f.mm, DEFAULT_MEMBER_ID, DEFAULT_MEMBER_EPOCH);
        let data = heartbeat_state.build_request_data();
        assert_eq!(DEFAULT_GROUP_ID, data.group_id);
        assert_eq!(DEFAULT_MEMBER_ID, data.member_id);
        assert_eq!(DEFAULT_MEMBER_EPOCH, data.member_epoch);
        assert_eq!(None, data.subscribed_topic_names);
        f.mm.on_heartbeat_request_generated().unwrap();

        // Join the group and subscribe to a topic, but the response has not yet been received
        let topic = "topic1";
        f.subs
            .lock()
            .unwrap()
            .subscribe_with_topics(HashSet::from([topic.to_string()]), None)
            .unwrap();
        // `mockRejoiningMemberData()`.
        force_state(&f.mm, MemberState::Joining);
        force_member(&f.mm, DEFAULT_MEMBER_ID, 0);
        let data = heartbeat_state.build_request_data();
        assert_eq!(DEFAULT_GROUP_ID, data.group_id);
        assert_eq!(DEFAULT_MEMBER_ID, data.member_id);
        assert_eq!(0, data.member_epoch);
        assert_eq!(Some(vec![topic.to_string()]), data.subscribed_topic_names);
        f.mm.on_heartbeat_request_generated().unwrap();

        let data = heartbeat_state.build_request_data();
        assert_eq!(DEFAULT_GROUP_ID, data.group_id);
        assert_eq!(DEFAULT_MEMBER_ID, data.member_id);
        assert_eq!(0, data.member_epoch);
        assert_eq!(Some(vec![topic.to_string()]), data.subscribed_topic_names);

        // Mock the response from the group coordinator which returns an assignment
        let mut tp_topic1 = TopicPartitions::new();
        let topic_id = Uuid::random_uuid();
        tp_topic1.set_topic_id(topic_id).set_partitions(vec![0]);
        let mut assignment_topic1 = Assignment::new();
        assignment_topic1.set_topic_partitions(vec![tp_topic1]);
        let mut rs1_data = ShareGroupHeartbeatResponseData::new();
        rs1_data
            .set_heartbeat_interval_ms(DEFAULT_HEARTBEAT_INTERVAL_MS as i32)
            .set_member_id(Some(DEFAULT_MEMBER_ID.to_string()))
            .set_member_epoch(DEFAULT_MEMBER_EPOCH)
            .set_assignment(Some(assignment_topic1));
        let rs1 = ShareGroupHeartbeatResponse::new(rs1_data);
        // Deviation: Java stubs `metadata.topicNames()` with the topic id; the
        // real metadata does not know it, so the assignment stays unresolved.
        f.mm.on_heartbeat_success(&rs1).unwrap();
    }

    /// Translated from `ShareHeartbeatRequestManagerTest#testPollTimerExpiration`.
    /// Mockito verifications become the state they stand for:
    ///   - `transitionToSendingLeaveGroup(true)` + `onHeartbeatRequestGenerated()`:
    ///     the leave epoch is applied and LEAVING moves to STALE (the poll
    ///     timer expired);
    ///   - `heartbeatState.reset()`: the sent fields are cleared;
    ///   - `heartbeatRequestState.reset()`: Deviation: no state distinguishes a
    ///     reset here (the interval had already elapsed), so the post-reset
    ///     "can send now" state is asserted;
    ///   - `maybeRejoinStaleMember()`: STALE moves to JOINING.
    ///
    /// Java stubs `shouldSkipHeartbeat()`; STALE and JOINING are the real
    /// states with those answers.
    #[tokio::test]
    async fn test_poll_timer_expiration() {
        let mut f = Fixture::new();
        f.rebuild(DEFAULT_HEARTBEAT_INTERVAL_MS);

        // On poll timer expiration, the member should send a last heartbeat to leave the group
        // and notify the membership manager
        f.sleep(DEFAULT_MAX_POLL_INTERVAL_MS);
        f.assert_heartbeat(DEFAULT_HEARTBEAT_INTERVAL_MS, false).await;
        assert_eq!(ShareGroupHeartbeatRequest::LEAVE_GROUP_MEMBER_EPOCH, f.mm.member_epoch());
        assert_eq!(MemberState::Stale, f.mm.state());
        assert!(f.mgr.heartbeat_state.sent_fields.rack_id.is_none());
        assert!(f.mgr.heartbeat_state.sent_fields.subscribed_topic_names.is_none());
        let now = f.now();
        assert_eq!(0, f.mgr.inner.heartbeat_request_state.time_to_next_heartbeat_ms(now));
        assert!(f.mgr.inner.heartbeat_request_state.can_send_request(now));

        f.assert_no_heartbeat();
        let now = f.now();
        f.mgr.reset_poll_timer(now);
        assert!(!f.mgr.inner().poll_timer_is_expired(f.now()));
        assert_eq!(MemberState::Joining, f.mm.state(), "maybeRejoinStaleMember()");
        f.assert_heartbeat(DEFAULT_HEARTBEAT_INTERVAL_MS, true).await;
    }

    /// Translated from `ShareHeartbeatRequestManagerTest#testHeartbeatMetrics`.
    /// Java sleeps a random `nextInt(11)` seconds at the end; every value runs.
    #[tokio::test]
    async fn test_heartbeat_metrics() {
        for random_sleep_s in 0..=10i64 {
            let mut f = Fixture::new();
            assert!(f.metric("heartbeat-response-time-max").is_some());
            assert!(f.metric("heartbeat-rate").is_some());
            assert!(f.metric("heartbeat-total").is_some());
            assert!(f.metric("last-heartbeat-seconds-ago").is_some());

            // test poll
            f.sleep(DEFAULT_HEARTBEAT_INTERVAL_MS);
            f.assert_heartbeat(DEFAULT_HEARTBEAT_INTERVAL_MS, true).await;
            f.sleep(1_000);
            assert_eq!(Some(1.0), f.metric("heartbeat-total"));
            assert_eq!(
                Some((DEFAULT_HEARTBEAT_INTERVAL_MS / 1_000) as f64),
                f.metric("last-heartbeat-seconds-ago")
            );

            f.assert_heartbeat(DEFAULT_HEARTBEAT_INTERVAL_MS, true).await;
            let rate = f.metric("heartbeat-rate").unwrap();
            assert!((rate - 0.06).abs() <= 0.005, "heartbeat-rate {rate} not within 0.005 of 0.06");
            assert_eq!(Some(2.0), f.metric("heartbeat-total"));

            // Randomly sleep for some time
            f.sleep(random_sleep_s * 1_000);
            assert_eq!(Some(random_sleep_s as f64), f.metric("last-heartbeat-seconds-ago"));
        }
    }

    /// Rust-only: the production constructor (Java's tests use only the
    /// visible-for-testing one) registers the heartbeat metrics in the
    /// `consumer-share-coordinator-metrics` group and starts from a zero
    /// heartbeat interval, so the first heartbeat goes out on the first poll.
    #[tokio::test]
    async fn test_new_registers_share_heartbeat_metrics() {
        let f = Fixture::new();
        let metrics = Arc::new(Metrics::with_time(Arc::clone(&f.time) as Arc<dyn Time>));
        let mut mgr = ShareHeartbeatRequestManager::new(
            f.now(),
            &config(),
            Arc::clone(&f.coord),
            Arc::clone(&f.subs),
            Arc::clone(&f.mm),
            Arc::clone(&f.beh),
            &metrics,
        );
        for name in [
            "heartbeat-response-time-max",
            "heartbeat-rate",
            "heartbeat-total",
            "last-heartbeat-seconds-ago",
        ] {
            let metric_name = metrics.metric_name(name, SHARE_CONSUMER_COORDINATOR_METRICS);
            assert!(metrics.metric(&metric_name).is_some(), "{name}");
        }
        assert_eq!(0, mgr.inner().heartbeat_request_state.heartbeat_interval_ms());
        assert_eq!(1, mgr.poll(f.now()).unsent_requests.len());
        assert!(mgr.inner().metrics_manager.is_some());
        assert_eq!(
            f.now(),
            mgr.inner_mut().metrics_manager.as_ref().unwrap().last_heartbeat_ms_for_test()
        );
    }

    /// Rust-only (R3): with a rack configured on the production-built
    /// managers, `HeartbeatState#buildRequestData` sends the RackId on the
    /// first heartbeat only ("only sent the first time, because it does not
    /// change"), and again after an error response resets the sent fields
    /// (`resetHeartbeatState` at the top of Java's `onErrorResponse`).
    #[tokio::test]
    async fn test_rack_id_sent_once_and_again_after_reset() {
        let f = Fixture::new();
        let metrics = Arc::new(Metrics::with_time(Arc::clone(&f.time) as Arc<dyn Time>));
        let metadata = Arc::new(Metadata::new(100, 1_000, 300_000, ClusterResourceListeners::new()));
        let mm = Arc::new(ShareMembershipManager::new(
            DEFAULT_GROUP_ID,
            Some("rack-1".to_string()),
            Arc::clone(&f.subs),
            metadata,
            Arc::clone(&f.time) as Arc<dyn Time>,
            &metrics,
        ));
        force_state(&mm, MemberState::Stable);
        force_member(&mm, "member-1", 1);
        let mut mgr = ShareHeartbeatRequestManager::new(
            f.now(),
            &config(),
            Arc::clone(&f.coord),
            Arc::clone(&f.subs),
            Arc::clone(&mm),
            Arc::clone(&f.beh),
            &metrics,
        );
        mgr.reset_poll_timer(f.now());

        fn rack_id_sent(result: &mut PollResult) -> Option<String> {
            assert_eq!(1, result.unsent_requests.len());
            let builder = result.unsent_requests[0].request_builder_mut().expect("a request builder");
            let version = builder.latest_allowed_version();
            match builder.build_version(version).expect("build") {
                AbstractRequest::ShareGroupHeartbeat(request) => request.data().rack_id.clone(),
                other => panic!("expected a ShareGroupHeartbeatRequest, got {other:?}"),
            }
        }

        async fn complete(mgr: &ShareHeartbeatRequestManager, result: &PollResult, error: Errors, now: i64) {
            result.unsent_requests[0].handler().on_complete(create_heartbeat_response(
                error,
                DEFAULT_HEARTBEAT_INTERVAL_MS,
                now,
            ));
            for _ in 0..10_000 {
                if !mgr.inner.pending_completion_rx.is_empty() {
                    return;
                }
                tokio::task::yield_now().await;
            }
            panic!("the forwarder did not queue the response");
        }

        // First heartbeat: the rack is sent.
        let mut first = mgr.poll(f.now());
        assert_eq!(Some("rack-1".to_string()), rack_id_sent(&mut first));
        complete(&mgr, &first, Errors::None, f.now()).await;

        // Next heartbeat: unchanged, so not sent.
        f.sleep(DEFAULT_HEARTBEAT_INTERVAL_MS);
        let mut second = mgr.poll(f.now());
        assert_eq!(None, rack_id_sent(&mut second));
        complete(&mgr, &second, Errors::CoordinatorLoadInProgress, f.now()).await;

        // The error response resets the sent fields: the rack goes out again.
        let _ = mgr.poll(f.now());
        f.sleep(DEFAULT_RETRY_BACKOFF_MAX_MS);
        let mut third = mgr.poll(f.now());
        assert_eq!(Some("rack-1".to_string()), rack_id_sent(&mut third));
    }

    /// Rust-only, as the consumer's test of the same name: the response
    /// forwarder wakes the bg task once it has queued the completion.
    #[tokio::test]
    async fn test_forwarder_wakes_bg_task_after_queueing_the_response() {
        let mut f = Fixture::new();
        let notify = Arc::new(tokio::sync::Notify::new());
        f.mgr.set_completion_notify(Arc::clone(&notify));

        f.sleep(DEFAULT_HEARTBEAT_INTERVAL_MS);
        let result = f.poll();
        assert_eq!(1, result.unsent_requests.len());
        result.unsent_requests[0].handler().on_complete(create_heartbeat_response(
            Errors::None,
            DEFAULT_HEARTBEAT_INTERVAL_MS + 500,
            f.now(),
        ));

        tokio::time::timeout(std::time::Duration::from_secs(5), notify.notified())
            .await
            .expect("the forwarder must wake the bg task after queueing the response");
        assert!(
            !f.mgr.inner.pending_completion_rx.is_empty(),
            "the completion is queued before the wake"
        );
        let _ = f.poll();
        assert_eq!(
            DEFAULT_HEARTBEAT_INTERVAL_MS + 500,
            f.mgr.inner.heartbeat_request_state.heartbeat_interval_ms(),
            "the woken poll drains and applies the response"
        );
    }

    /// Translated from `AbstractHeartbeatRequestManagerTest#testTimerNotDue` (inherited).
    /// UNSUBSCRIBED is a real state where `shouldSkipHeartbeat()` is true.
    #[tokio::test]
    async fn test_timer_not_due() {
        let mut f = Fixture::new();
        f.sleep(100); // before heartbeatInterval, no heartbeat should be sent
        let result = f.poll();

        assert_eq!(0, result.unsent_requests.len());
        assert_eq!(DEFAULT_HEARTBEAT_INTERVAL_MS - 100, result.time_until_next_poll_ms);
        assert_eq!(DEFAULT_HEARTBEAT_INTERVAL_MS - 100, f.maximum_time_to_wait());

        // Member in state where it should not send Heartbeat anymore
        force_state(&f.mm, MemberState::Unsubscribed);
        let result = f.poll();
        assert_eq!(i64::MAX, result.time_until_next_poll_ms);
    }

    /// Translated from `AbstractHeartbeatRequestManagerTest#testHeartbeatOutsideInterval` (inherited).
    /// ACKNOWLEDGING heartbeats now; `verify(membershipManager).onHeartbeatRequestGenerated()`
    /// becomes the transition it makes (ACKNOWLEDGING with the target
    /// reconciled moves to STABLE).
    #[tokio::test]
    async fn test_heartbeat_outside_interval() {
        let mut f = Fixture::new();
        force_state(&f.mm, MemberState::Acknowledging);
        let result = f.poll();

        assert_eq!(1, result.unsent_requests.len());
        assert_eq!(DEFAULT_HEARTBEAT_INTERVAL_MS, result.time_until_next_poll_ms);
        assert_eq!(DEFAULT_HEARTBEAT_INTERVAL_MS, f.maximum_time_to_wait());
        assert_eq!(MemberState::Stable, f.mm.state(), "onHeartbeatRequestGenerated() ran");
    }

    /// Translated from `AbstractHeartbeatRequestManagerTest#testNoCoordinator` (inherited).
    #[tokio::test]
    async fn test_no_coordinator() {
        let mut f = Fixture::new();
        f.coord.mark_coordinator_unknown("test", f.now());
        let result = f.poll();

        assert_eq!(i64::MAX, result.time_until_next_poll_ms);
        assert_eq!(DEFAULT_RETRY_BACKOFF_MS, f.maximum_time_to_wait());
        assert_eq!(0, result.unsent_requests.len());
    }

    /// Translated from
    /// `AbstractHeartbeatRequestManagerTest#testPollTimerExpirationShouldNotMarkMemberStaleIfMemberAlreadyLeaving`
    /// (inherited). PREPARE_LEAVING is the real state with Java's stubs
    /// (`isLeavingGroup()` true, `shouldSkipHeartbeat()` and
    /// `shouldHeartbeatNow()` false, `state()` not LEAVING).
    /// `verify(membershipManager, never()).transitionToSendingLeaveGroup(anyBoolean())`
    /// becomes: the member stays PREPARE_LEAVING with its epoch.
    #[tokio::test]
    async fn test_poll_timer_expiration_should_not_mark_member_stale_if_member_already_leaving() {
        let mut f = Fixture::new();
        force_state(&f.mm, MemberState::PrepareLeaving);
        assert!(MembershipManagerHooks::is_leaving_group(&*f.mm));

        f.sleep(DEFAULT_MAX_POLL_INTERVAL_MS);
        let result = f.poll();

        // No transition to leave due to stale member should be triggered, because the member
        // is already leaving the group.
        assert_eq!(MemberState::PrepareLeaving, f.mm.state());
        assert_eq!(DEFAULT_MEMBER_EPOCH, f.mm.member_epoch());

        assert_eq!(
            1,
            result.unsent_requests.len(),
            "A heartbeat request should be generated to complete the ongoing leaving operation that was triggered \
             before the poll timer expired."
        );
    }

    /// Translated from `AbstractHeartbeatRequestManagerTest#testSuccessfulHeartbeatTiming` (inherited).
    #[tokio::test]
    async fn test_successful_heartbeat_timing() {
        let mut f = Fixture::new();
        let result = f.poll();
        assert_eq!(
            0,
            result.unsent_requests.len(),
            "No heartbeat should be sent while interval has not expired"
        );
        let now = f.now();
        assert_eq!(
            f.mgr.inner.heartbeat_request_state.time_to_next_heartbeat_ms(now),
            result.time_until_next_poll_ms
        );
        f.assert_next_heartbeat_timing(DEFAULT_HEARTBEAT_INTERVAL_MS);

        let result = f.poll();
        assert_eq!(
            1,
            result.unsent_requests.len(),
            "A heartbeat should be sent when interval expires"
        );
        let inflight_req = result.unsent_requests.into_iter().next().unwrap();
        let now = f.now();
        assert_eq!(
            DEFAULT_HEARTBEAT_INTERVAL_MS,
            f.mgr.inner.heartbeat_request_state.time_to_next_heartbeat_ms(now),
            "Heartbeat timer was not reset to the interval when the heartbeat request was sent."
        );

        let part_of_interval = DEFAULT_HEARTBEAT_INTERVAL_MS / 3;
        f.sleep(part_of_interval);
        let result = f.poll();
        assert_eq!(
            0,
            result.unsent_requests.len(),
            "No heartbeat should be sent while only part of the interval has passed"
        );
        let now = f.now();
        assert_eq!(
            DEFAULT_HEARTBEAT_INTERVAL_MS - part_of_interval,
            f.mgr.inner.heartbeat_request_state.time_to_next_heartbeat_ms(now),
            "Time to next interval was not properly updated."
        );

        f.complete(&inflight_req, Errors::None, DEFAULT_HEARTBEAT_INTERVAL_MS).await;
        f.assert_next_heartbeat_timing(DEFAULT_HEARTBEAT_INTERVAL_MS - part_of_interval);
    }

    /// Translated from
    /// `AbstractHeartbeatRequestManagerTest#testLogsHeartbeatIntervalReceivedFromCoordinatorOnlyWhenChanged`
    /// (inherited). Java's `LogCaptureAppender` becomes the messages the base
    /// recorded at the log site (`logged_heartbeat_interval_messages`).
    #[tokio::test]
    async fn test_logs_heartbeat_interval_received_from_coordinator_only_when_changed() {
        let mut f = Fixture::new();
        let changed_interval_ms = DEFAULT_HEARTBEAT_INTERVAL_MS + 500;
        let count_heartbeat_interval_logs = |f: &Fixture| {
            f.mgr
                .inner
                .logged_heartbeat_interval_messages
                .iter()
                .filter(|(_, message)| message.contains("received heartbeat interval"))
                .count()
        };

        // A successful heartbeat whose interval differs from the current one is applied and logged.
        f.sleep(DEFAULT_HEARTBEAT_INTERVAL_MS);
        let result = f.poll();
        assert_eq!(1, result.unsent_requests.len());
        f.complete(&result.unsent_requests[0], Errors::None, changed_interval_ms).await;

        assert_eq!(changed_interval_ms, f.mgr.inner.heartbeat_request_state.heartbeat_interval_ms());
        assert_eq!(
            1,
            count_heartbeat_interval_logs(&f),
            "The heartbeat interval received from the coordinator should be logged when it changes."
        );
        assert!(
            f.mgr
                .inner
                .logged_heartbeat_interval_messages
                .iter()
                .any(|(level, message)| *level == log::Level::Info
                    && message.contains(&format!(
                        "Member {DEFAULT_MEMBER_ID} received heartbeat interval {changed_interval_ms}ms"
                    ))),
            "The logged message should contain the member id and the received interval."
        );

        // A subsequent heartbeat carrying the same interval must not be logged again.
        f.sleep(changed_interval_ms);
        let result = f.poll();
        assert_eq!(1, result.unsent_requests.len());
        f.complete(&result.unsent_requests[0], Errors::None, changed_interval_ms).await;

        assert_eq!(
            1,
            count_heartbeat_interval_logs(&f),
            "An unchanged heartbeat interval must not be logged again."
        );
    }

    /// Translated from `AbstractHeartbeatRequestManagerTest#testGroupIdNotFoundExceptionWhileUnsubscribed`
    /// (inherited). Java mocks an UNSUBSCRIBED member with epoch -1 that still
    /// heartbeats; the real state machine gets there through the leave
    /// heartbeat, which `onHeartbeatRequestGenerated()` moves from LEAVING to
    /// UNSUBSCRIBED before the request goes out. `never().transitionToFatal()` /
    /// `never().add(any())` become: still UNSUBSCRIBED, no background event.
    /// Deviation: `onHeartbeatFailure(false)` records nothing on the share
    /// rebalance metrics, so its call is not observable.
    #[tokio::test]
    async fn test_group_id_not_found_exception_while_unsubscribed() {
        let mut f = Fixture::new();
        force_state(&f.mm, MemberState::PrepareLeaving);
        f.mm.transition_to_sending_leave_group(false).unwrap();
        assert_eq!(MemberState::Leaving, f.mm.state());

        f.sleep(DEFAULT_HEARTBEAT_INTERVAL_MS);
        let result = f.poll();
        assert_eq!(1, result.unsent_requests.len());
        assert_eq!(MemberState::Unsubscribed, f.mm.state());
        assert_eq!(-1, f.mm.member_epoch());

        f.complete(
            &result.unsent_requests[0],
            Errors::GroupIdNotFound,
            DEFAULT_HEARTBEAT_INTERVAL_MS,
        )
        .await;

        assert_eq!(MemberState::Unsubscribed, f.mm.state(), "never transitionToFatal()");
        assert!(f.drain_events().is_empty(), "never backgroundEventHandler.add(any())");
    }

    /// Translated from `AbstractHeartbeatRequestManagerTest#testGroupIdNotFoundWhileStableIsFatal` (inherited).
    #[tokio::test]
    async fn test_group_id_not_found_while_stable_is_fatal() {
        let mut f = Fixture::new();
        assert_eq!(MemberState::Stable, f.mm.state());
        assert_eq!(DEFAULT_MEMBER_EPOCH, f.mm.member_epoch());

        f.sleep(DEFAULT_HEARTBEAT_INTERVAL_MS);
        let result = f.poll();
        assert_eq!(1, result.unsent_requests.len());

        f.coord.mark_coordinator_unknown("test", f.now());
        f.complete(
            &result.unsent_requests[0],
            Errors::GroupIdNotFound,
            DEFAULT_HEARTBEAT_INTERVAL_MS,
        )
        .await;

        assert_eq!(MemberState::Fatal, f.mm.state(), "transitionToFatal()");
        assert_eq!(1, f.drain_events().len(), "backgroundEventHandler.add(any())");
    }

    /// Translated from `AbstractHeartbeatRequestManagerTest#testHeartbeatResponseOnErrorHandling`
    /// (inherited; `@MethodSource("errorProvider")`, all 14 rows, a fresh
    /// fixture each), with `assertHeartbeatErrorHandling` and
    /// `ensureFatalError`. Mockito verifications become the state they stand
    /// for:
    ///   - `onHeartbeatSuccess(response)` (NONE): the response's member epoch
    ///     is applied (seeded to 0 first);
    ///   - `markCoordinatorUnknown`: the coordinator is unknown afterwards;
    ///   - `backgroundEventHandler.add` / `never()`: the Error events emitted;
    ///   - `transitionToFatal()` / `never()`: the member is / is not FATAL.
    ///
    /// Deviation: `onHeartbeatFailure(false)` records nothing on the share
    /// rebalance metrics, so its call is not asserted. Java's mocked
    /// coordinator manager keeps the node after `markCoordinatorUnknown`; the
    /// real one forgets it, so the follow-up heartbeat check re-seeds it.
    #[tokio::test]
    async fn test_heartbeat_response_on_error_handling() {
        // error, isFatal
        let error_provider = [
            (Errors::None, false),
            (Errors::CoordinatorNotAvailable, false),
            (Errors::CoordinatorLoadInProgress, false),
            (Errors::NotCoordinator, false),
            (Errors::GroupAuthorizationFailed, true),
            (Errors::InvalidRequest, true),
            (Errors::UnknownMemberId, false),
            (Errors::FencedMemberEpoch, false),
            (Errors::UnsupportedAssignor, true),
            (Errors::UnsupportedVersion, true),
            (Errors::UnreleasedInstanceId, true),
            (Errors::FencedInstanceId, true),
            (Errors::GroupMaxSizeReached, true),
            (Errors::TopicAuthorizationFailed, false),
        ];
        for (error, is_fatal) in error_provider {
            let mut f = Fixture::new();
            // Handling errors on the second heartbeat
            f.sleep(DEFAULT_HEARTBEAT_INTERVAL_MS);
            let result = f.poll();
            assert_eq!(1, result.unsent_requests.len(), "{error:?}");

            // Manually completing the response to test error handling. A member
            // epoch the response does not carry, so NONE's
            // `onHeartbeatSuccess` is visible.
            force_member(&f.mm, DEFAULT_MEMBER_ID, 0);
            f.complete(&result.unsent_requests[0], error, DEFAULT_HEARTBEAT_INTERVAL_MS)
                .await;

            match error {
                Errors::None => {
                    assert_eq!(DEFAULT_MEMBER_EPOCH, f.mm.member_epoch(), "onHeartbeatSuccess(response) ran");
                    f.assert_next_heartbeat_timing(DEFAULT_HEARTBEAT_INTERVAL_MS);
                },
                Errors::CoordinatorLoadInProgress => {
                    assert!(f.drain_error_events().is_empty(), "{error:?}");
                    f.assert_next_heartbeat_timing(DEFAULT_RETRY_BACKOFF_MS);
                },
                Errors::CoordinatorNotAvailable | Errors::NotCoordinator => {
                    assert!(f.drain_error_events().is_empty(), "{error:?}");
                    assert!(f.coord.coordinator().is_none(), "{error:?}: markCoordinatorUnknown");
                    f.assert_next_heartbeat_timing(0);
                },
                Errors::UnknownMemberId | Errors::FencedMemberEpoch => {
                    assert!(f.drain_error_events().is_empty(), "{error:?}");
                    f.assert_next_heartbeat_timing(0);
                },
                Errors::TopicAuthorizationFailed => {
                    let errors = f.drain_error_events();
                    assert_eq!(1, errors.len(), "{error:?}");
                    assert_eq!(error, errors[0].error());
                    f.assert_next_heartbeat_timing(DEFAULT_RETRY_BACKOFF_MS);
                    assert_ne!(MemberState::Fatal, f.mm.state(), "{error:?}");
                },
                _ => {
                    assert!(is_fatal, "{error:?} is listed as fatal");
                    // Drop the coordinator so the follow-up poll inside ensureFatalError() does
                    // not produce another heartbeat request.
                    f.coord.mark_coordinator_unknown("test", f.now());
                    // `ensureFatalError(error)`.
                    assert_eq!(MemberState::Fatal, f.mm.state(), "{error:?}: transitionToFatal()");
                    let errors = f.drain_error_events();
                    assert_eq!(1, errors.len(), "{error:?}");
                    assert_eq!(
                        error,
                        errors[0].error(),
                        "The fatal error propagated to the app thread does not match the error received in the \
                         heartbeat response."
                    );
                    // Ensure no further heartbeat is generated after the fatal error.
                    f.sleep(DEFAULT_HEARTBEAT_INTERVAL_MS);
                    let result = f.poll();
                    assert_eq!(
                        0,
                        result.unsent_requests.len(),
                        "No further heartbeat should be sent after a fatal {error:?} error."
                    );
                },
            }

            if !is_fatal {
                // Make sure a next heartbeat is sent for all non-fatal errors (to retry or rejoin)
                f.coord.set_coordinator_for_test(Node::new(1, "localhost".to_string(), 9999));
                f.sleep(DEFAULT_HEARTBEAT_INTERVAL_MS);
                let result = f.poll();
                assert_eq!(
                    1,
                    result.unsent_requests.len(),
                    "A follow-up heartbeat should be sent after a non-fatal error {error:?}"
                );
            }
        }
    }
}
