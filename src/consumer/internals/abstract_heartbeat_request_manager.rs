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

//! `AbstractHeartbeatRequestManager` — shared heartbeat lifecycle and
//! response routing for KIP-848 consumer groups.
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.AbstractHeartbeatRequestManager`.
//!
//! # Translation notes
//!
//! Java models this as `abstract class AbstractHeartbeatRequestManager<R extends
//! AbstractResponse>`. Rust composes it as a concrete `pub(crate) struct`
//! (Phase 7a `AbstractFetch` precedent). Because Share / Streams subclasses
//! are out of scope per `consumer-threading.md` §20, the response type is
//! hard-wired to [`ConsumerGroupHeartbeatResponse`]; the composing
//! [`crate::consumer::internals::consumer_heartbeat_request_manager::ConsumerHeartbeatRequestManager`]
//! supplies request building and error classification through plain method
//! calls on the wrapping struct rather than generic dispatch.
//!
//! Metrics (`HeartbeatMetricsManager`) are dropped — no Rust metrics
//! framework. `LogContext` is dropped — we use the `log` crate.

#![allow(dead_code)]

use std::sync::Arc;

use crate::common::KafkaError;
use crate::common::protocol::Errors;
use crate::consumer::ConsumerConfig;
use crate::consumer::internals::events::background_event::BackgroundEvent;
use crate::consumer::internals::events::background_event_handler::BackgroundEventHandler;

use super::coordinator_request_manager::CoordinatorRequestManager;
use super::heartbeat_request_state::HeartbeatRequestState;
use super::network_client_delegate::{PollResult, UnsentRequest};

/// Exponent base used by `RequestState`'s exponential-backoff machinery.
/// Mirrors Java's `RequestState.RETRY_BACKOFF_JITTER` (0.2 jitter; the
/// exp-base is fixed at 2 inside Java).
pub(crate) const RETRY_BACKOFF_JITTER: f64 = 0.2;

/// Message logged when the broker reports `UnsupportedVersion` on the
/// `ConsumerGroupHeartbeat` API.
///
/// Java: `AbstractHeartbeatRequestManager.CONSUMER_PROTOCOL_NOT_SUPPORTED_MSG`.
pub(crate) const CONSUMER_PROTOCOL_NOT_SUPPORTED_MSG: &str = "The cluster does not support the new CONSUMER group protocol. \
     Set group.protocol=classic on the consumer configs to revert to the CLASSIC protocol \
     until the cluster is upgraded.";

/// Shared heartbeat lifecycle state composed by
/// [`super::consumer_heartbeat_request_manager::ConsumerHeartbeatRequestManager`].
///
/// All fields are `pub(crate)` so the composing manager (single subclass
/// in scope) can mutate them directly. This matches Java's `protected`
/// semantics.
pub(crate) struct AbstractHeartbeatRequestManager {
    /// Max time allowed between invocations of `poll`, from
    /// `max.poll.interval.ms`. Sent on the first join heartbeat as the
    /// broker-side rebalance timeout.
    pub(crate) max_poll_interval_ms: i32,
    /// Coordinator-discovery manager — heartbeats target whatever node
    /// the coordinator manager has resolved. Held as `Arc<...>` (no
    /// outer Mutex); the manager itself uses interior mutability.
    pub(crate) coordinator_request_manager: Arc<CoordinatorRequestManager>,
    /// State for heartbeat-request timing and retry backoff.
    pub(crate) heartbeat_request_state: HeartbeatRequestState,
    /// Channel for surfacing errors and rebalance-listener callback
    /// events back to the application thread.
    pub(crate) background_event_handler: Arc<BackgroundEventHandler>,
    /// Absolute wall-clock millisecond expiration for the poll timer.
    /// Java models this with a `Timer`; Rust stores the deadline
    /// directly (mirroring [`HeartbeatRequestState`]).
    poll_timer_expires_at_ms: i64,
}

impl AbstractHeartbeatRequestManager {
    /// Constructs a new instance starting from the supplied wall-clock
    /// time. The poll timer starts running immediately with a duration
    /// of `max.poll.interval.ms`.
    ///
    /// Java: `AbstractHeartbeatRequestManager(LogContext, Time, ConsumerConfig,
    /// CoordinatorRequestManager, BackgroundEventHandler, HeartbeatMetricsManager)`.
    pub(crate) fn new(
        current_time_ms: i64,
        config: &ConsumerConfig,
        coordinator_request_manager: Arc<CoordinatorRequestManager>,
        background_event_handler: Arc<BackgroundEventHandler>,
    ) -> Self {
        let max_poll_interval_ms = config.max_poll_interval_ms();
        let retry_backoff_ms = config.retry_backoff_ms();
        let retry_backoff_max_ms = config.retry_backoff_max_ms();
        // Java: `new HeartbeatRequestState(logContext, time, 0,
        //   retryBackoffMs, retryBackoffMaxMs, RETRY_BACKOFF_JITTER)`.
        // The initial interval of 0 means the first call to
        // `time_to_next_heartbeat_ms` reports "ready now"; the broker's
        // first response then sets the real interval via
        // `update_heartbeat_interval_ms`.
        let heartbeat_request_state = HeartbeatRequestState::new(
            current_time_ms,
            0,
            retry_backoff_ms,
            retry_backoff_max_ms,
            RETRY_BACKOFF_JITTER,
        );
        Self {
            max_poll_interval_ms,
            coordinator_request_manager,
            heartbeat_request_state,
            background_event_handler,
            poll_timer_expires_at_ms: current_time_ms + i64::from(max_poll_interval_ms),
        }
    }

    /// Visible-for-testing constructor: lets callers supply a custom
    /// [`HeartbeatRequestState`] (mirrors Java's package-private second
    /// constructor).
    pub(crate) fn with_state(
        current_time_ms: i64,
        config: &ConsumerConfig,
        coordinator_request_manager: Arc<CoordinatorRequestManager>,
        heartbeat_request_state: HeartbeatRequestState,
        background_event_handler: Arc<BackgroundEventHandler>,
    ) -> Self {
        let max_poll_interval_ms = config.max_poll_interval_ms();
        Self {
            max_poll_interval_ms,
            coordinator_request_manager,
            heartbeat_request_state,
            background_event_handler,
            poll_timer_expires_at_ms: current_time_ms + i64::from(max_poll_interval_ms),
        }
    }

    /// Returns `true` if the poll timer has expired at `current_time_ms`.
    pub(crate) fn poll_timer_is_expired(&self, current_time_ms: i64) -> bool {
        current_time_ms >= self.poll_timer_expires_at_ms
    }

    /// Returns remaining ms on the poll timer, clamped at 0.
    pub(crate) fn poll_timer_remaining_ms(&self, current_time_ms: i64) -> i64 {
        (self.poll_timer_expires_at_ms - current_time_ms).max(0)
    }

    /// Returns ms by which the poll timer is overdue (negative if not
    /// expired). Mirrors Java's `Timer.isExpiredBy()`.
    pub(crate) fn poll_timer_is_expired_by(&self, current_time_ms: i64) -> i64 {
        current_time_ms - self.poll_timer_expires_at_ms
    }

    /// Resets the poll timer so it expires `max_poll_interval_ms` from
    /// `current_time_ms`. Java's `Timer.reset(maxPollIntervalMs)` (which
    /// internally snaps `now` from `Time.milliseconds()`).
    ///
    /// **Phase 10 carry-over**: Java's `resetPollTimer(pollMs)` also
    /// checks `pollTimer.isExpired()` and calls
    /// `membershipManager().maybeRejoinStaleMember()` when expired (see
    /// `AbstractHeartbeatRequestManager.java:265-274`). The abstract
    /// layer here lacks a back-reference to the membership manager, so
    /// the expiry-then-rejoin step must be performed by Phase 10's
    /// `consumer.poll()` epilogue after invoking `reset_poll_timer`:
    ///
    /// ```ignore
    /// hb.reset_poll_timer(now);
    /// if hb.poll_timer_is_expired(now) {
    ///     membership_manager.maybe_rejoin_stale_member();
    /// }
    /// ```
    pub(crate) fn reset_poll_timer(&mut self, current_time_ms: i64) {
        self.poll_timer_expires_at_ms = current_time_ms + i64::from(self.max_poll_interval_ms);
    }

    /// Surface a coordinator fatal error to the application thread via
    /// the background-event channel, if one has been recorded.
    ///
    /// Java: `maybePropagateCoordinatorFatalErrorEvent()`.
    pub(crate) fn maybe_propagate_coordinator_fatal_error_event(&self, current_time_ms: i64) {
        let fatal = self.coordinator_request_manager.get_and_clear_fatal_error();
        if let Some(err) = fatal {
            // Ignored if the receiver is dropped — same as Java's silent
            // success when the queue is closed during shutdown.
            let _ = self
                .background_event_handler
                .add(BackgroundEvent::Error { error: err }, current_time_ms);
        }
    }

    /// Translates Java's error → action dispatch for the
    /// `ConsumerGroupHeartbeat` response. Returns:
    ///
    /// - `HeartbeatErrorAction::Handled` — error already classified and
    ///   the manager has updated its own backoff / state listener.
    /// - `HeartbeatErrorAction::Fenced` — caller must mark the member
    ///   fenced.
    /// - `HeartbeatErrorAction::Fatal(error)` — caller must mark the
    ///   member fatal with the supplied error event.
    /// - `HeartbeatErrorAction::DelegateToSpecific` — error is not in
    ///   the abstract dispatch; the caller's
    ///   `handle_specific_exception_in_response` runs.
    ///
    /// Java: `onErrorResponse(R response, long currentTimeMs)`. Rust
    /// splits the "advise the membership manager" half off because the
    /// abstract layer cannot mutate the membership state directly
    /// (the membership manager is held by the composing consumer).
    pub(crate) fn classify_response_error(
        &mut self,
        error: Errors,
        error_message: &str,
        current_time_ms: i64,
    ) -> HeartbeatErrorAction {
        self.heartbeat_request_state.on_failed_attempt(current_time_ms);
        match error {
            Errors::NotCoordinator => {
                self.coordinator_request_manager
                    .mark_coordinator_unknown(error_message, current_time_ms);
                // Skip backoff so the next HB targets the new coordinator
                self.heartbeat_request_state.reset();
                HeartbeatErrorAction::Handled
            },
            Errors::CoordinatorNotAvailable => {
                self.coordinator_request_manager
                    .mark_coordinator_unknown(error_message, current_time_ms);
                self.heartbeat_request_state.reset();
                HeartbeatErrorAction::Handled
            },
            Errors::CoordinatorLoadInProgress => {
                // Backoff and retry.
                HeartbeatErrorAction::Handled
            },
            Errors::GroupAuthorizationFailed => HeartbeatErrorAction::Fatal(KafkaError::with_message(
                Errors::GroupAuthorizationFailed,
                error_message.to_string(),
            )),
            Errors::TopicAuthorizationFailed => {
                // Surface the auth error via the background-event
                // channel so it's returned on the next poll. The member
                // stays in its current state to allow recovery if ACLs
                // are added.
                let _ = self.background_event_handler.add(
                    BackgroundEvent::Error { error: KafkaError::with_message(error, error_message.to_string()) },
                    current_time_ms,
                );
                HeartbeatErrorAction::Handled
            },
            Errors::InvalidRequest | Errors::GroupMaxSizeReached | Errors::UnsupportedAssignor => {
                HeartbeatErrorAction::Fatal(KafkaError::with_message(error, error_message.to_string()))
            },
            Errors::FencedMemberEpoch | Errors::UnknownMemberId => {
                // Skip backoff so the next rejoin heartbeat is sent ASAP.
                self.heartbeat_request_state.reset();
                HeartbeatErrorAction::Fenced
            },
            Errors::InvalidRegularExpression => HeartbeatErrorAction::Fatal(KafkaError::with_message(
                Errors::InvalidRegularExpression,
                format!("Invalid RE2J SubscriptionPattern provided in the call to subscribe. {error_message}"),
            )),
            _ => HeartbeatErrorAction::DelegateToSpecific,
        }
    }

    /// Marks the manager as having received a successful heartbeat
    /// response. Updates the backoff state and the heartbeat interval.
    ///
    /// Java: `onResponse(R response, long currentTimeMs)` (success
    /// branch only — error responses go through
    /// [`Self::classify_response_error`]).
    pub(crate) fn on_successful_response(&mut self, new_heartbeat_interval_ms: i64, current_time_ms: i64) {
        self.heartbeat_request_state
            .update_heartbeat_interval_ms(current_time_ms, new_heartbeat_interval_ms);
        self.heartbeat_request_state.on_successful_attempt(current_time_ms);
    }

    /// Failure path mirroring Java's `onFailure(Throwable, long)` for
    /// retriable / generic-fatal errors. The caller still has to invoke
    /// `membership_manager().on_heartbeat_failure(retriable)` to mirror
    /// Java's `membershipManager().onHeartbeatFailure(...)` at the tail
    /// of `onFailure`.
    pub(crate) fn on_failure(&mut self, error: &KafkaError, current_time_ms: i64) -> HeartbeatFailureAction {
        self.heartbeat_request_state.on_failed_attempt(current_time_ms);
        if error.is_retriable() {
            self.coordinator_request_manager
                .handle_coordinator_disconnect(error, current_time_ms);
            log::debug!(
                "ConsumerGroupHeartbeatRequest failed because of the retriable exception. \
                 Will retry in {} ms: {}",
                self.heartbeat_request_state.remaining_backoff_ms(current_time_ms),
                error
            );
            HeartbeatFailureAction::Retriable
        } else {
            HeartbeatFailureAction::NonRetriable
        }
    }
}

/// Outcome of dispatching a heartbeat-response error through
/// [`AbstractHeartbeatRequestManager::classify_response_error`].
#[derive(Debug)]
pub(crate) enum HeartbeatErrorAction {
    /// Error fully handled at the abstract layer; caller should
    /// continue.
    Handled,
    /// Caller must mark the member fenced (FENCED_MEMBER_EPOCH /
    /// UNKNOWN_MEMBER_ID).
    Fenced,
    /// Caller must mark the member fatal and propagate the supplied
    /// error to the background-event channel.
    Fatal(KafkaError),
    /// Error was not in the abstract layer's dispatch table — caller's
    /// subclass-specific handler must run.
    DelegateToSpecific,
}

/// Outcome of dispatching a non-response failure through
/// [`AbstractHeartbeatRequestManager::on_failure`].
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum HeartbeatFailureAction {
    /// Retriable error — already logged, backoff applied. Caller passes
    /// `true` to `membership_manager.on_heartbeat_failure(...)`.
    Retriable,
    /// Non-retriable error — caller should run its subclass-specific
    /// handler. If that returns false, caller should mark the member
    /// fatal with the error.
    NonRetriable,
}

/// Helper used by the composing consumer manager to assemble a
/// `PollResult` carrying a heartbeat request. Mirrors the
/// `makeHeartbeatRequest(currentTimeMs, ignoreResponse)` side-effects:
///
/// - Bookkeeping: record send attempt, reset timer.
/// - Caller still constructs the `UnsentRequest` (because the request
///   payload depends on the membership manager's current state, which
///   is owned by the composing consumer manager).
///
/// Java's `makeHeartbeatRequest(currentTimeMs, ignoreResponse)` returns
/// a `PollResult` with the heartbeat interval as the wait hint and the
/// supplied request inside.
pub(crate) fn make_heartbeat_poll_result(
    request: UnsentRequest,
    state: &mut AbstractHeartbeatRequestManager,
    current_time_ms: i64,
) -> PollResult {
    state.heartbeat_request_state.on_send_attempt(current_time_ms);
    state.heartbeat_request_state.reset_timer();
    PollResult::new(state.heartbeat_request_state.heartbeat_interval_ms(), vec![request])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumer::ConsumerConfig;
    use tokio::sync::mpsc;

    fn make_state(now: i64) -> AbstractHeartbeatRequestManager {
        let config = ConsumerConfig::new(vec!["localhost:9092".to_string()]);
        let coord = Arc::new(CoordinatorRequestManager::new(100, 1_000, "g"));
        let (tx, _rx) = mpsc::unbounded_channel();
        let beh = Arc::new(BackgroundEventHandler::new(tx));
        AbstractHeartbeatRequestManager::new(now, &config, coord, beh)
    }

    /// Poll timer starts running on construction.
    #[test]
    fn poll_timer_starts_running() {
        let mgr = make_state(0);
        assert!(!mgr.poll_timer_is_expired(0));
        // Default max.poll.interval.ms is 300_000.
        assert!(mgr.poll_timer_is_expired(300_000));
    }

    /// Reset re-arms the timer from the new "now".
    #[test]
    fn reset_poll_timer_rearms() {
        let mut mgr = make_state(0);
        assert!(mgr.poll_timer_is_expired(300_000));
        mgr.reset_poll_timer(300_000);
        assert!(!mgr.poll_timer_is_expired(300_000));
        assert!(mgr.poll_timer_is_expired(600_000));
    }

    /// `classify_response_error` for `NOT_COORDINATOR` resets heartbeat
    /// state so the next heartbeat is sent without backoff.
    #[test]
    fn not_coordinator_resets_heartbeat_state() {
        let mut mgr = make_state(0);
        let action = mgr.classify_response_error(Errors::NotCoordinator, "x", 100);
        assert!(matches!(action, HeartbeatErrorAction::Handled));
        // After reset, no in-flight request and zero backoff.
        assert!(!mgr.heartbeat_request_state.request_in_flight());
    }

    /// `classify_response_error` for `GROUP_AUTHORIZATION_FAILED` is
    /// fatal.
    #[test]
    fn group_authorization_failed_is_fatal() {
        let mut mgr = make_state(0);
        let action = mgr.classify_response_error(Errors::GroupAuthorizationFailed, "msg", 0);
        match action {
            HeartbeatErrorAction::Fatal(err) => {
                assert_eq!(err.error(), Errors::GroupAuthorizationFailed);
            },
            other => panic!("expected fatal, got {other:?}"),
        }
    }

    /// `FencedMemberEpoch` and `UnknownMemberId` both request a Fenced
    /// transition.
    #[test]
    fn fenced_member_epoch_is_fenced() {
        let mut mgr = make_state(0);
        let action = mgr.classify_response_error(Errors::FencedMemberEpoch, "fenced", 0);
        assert!(matches!(action, HeartbeatErrorAction::Fenced));
    }

    #[test]
    fn unknown_member_id_is_fenced() {
        let mut mgr = make_state(0);
        let action = mgr.classify_response_error(Errors::UnknownMemberId, "unknown", 0);
        assert!(matches!(action, HeartbeatErrorAction::Fenced));
    }

    /// Other errors (e.g., `UnsupportedVersion`) defer to the specific
    /// subclass handler.
    #[test]
    fn unknown_error_defers_to_specific() {
        let mut mgr = make_state(0);
        let action = mgr.classify_response_error(Errors::UnsupportedVersion, "msg", 0);
        assert!(matches!(action, HeartbeatErrorAction::DelegateToSpecific));
    }

    /// Successful response updates the heartbeat interval.
    #[test]
    fn successful_response_updates_interval() {
        let mut mgr = make_state(0);
        mgr.on_successful_response(5_000, 0);
        assert_eq!(mgr.heartbeat_request_state.heartbeat_interval_ms(), 5_000);
    }

    /// `on_failure` for a retriable error tells the caller to retry.
    #[test]
    fn on_failure_retriable() {
        let mut mgr = make_state(0);
        let err = KafkaError::new(Errors::NetworkException);
        assert!(err.is_retriable());
        let action = mgr.on_failure(&err, 0);
        assert_eq!(action, HeartbeatFailureAction::Retriable);
    }
}
