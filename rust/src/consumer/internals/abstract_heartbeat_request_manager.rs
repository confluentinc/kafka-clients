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
//! AbstractResponse>`. Rust composes it as a `pub(crate) struct`, generic over
//! the same response type `R` (Phase 7a `AbstractFetch` precedent), held by the
//! subclass ([`crate::consumer::internals::ConsumerHeartbeatRequestManager`])
//! as its `inner` field.
//!
//! Java's `abstract` methods are the [`HeartbeatRequestManagerHooks`] trait,
//! implemented by the subclass. The base methods that call them (Java's
//! `poll`, `pollOnClose`, `maximumTimeToWait`, `makeHeartbeatRequest`,
//! `onResponse` / `onErrorResponse`, `onFailure`, `handleFatalFailure`) are
//! associated functions taking the subclass as `this: &mut H`, the Rust
//! stand-in for Java's virtual dispatch on `this`; they reach the base state
//! through [`HeartbeatRequestManagerHooks::base`] /
//! [`HeartbeatRequestManagerHooks::base_mut`]. The subclass's `RequestManager`
//! impl forwards to them.
//!
//! Metrics (`HeartbeatMetricsManager`) are dropped — no Rust metrics
//! framework. `LogContext` is dropped — we use the `log` crate.

#![expect(dead_code)]

use std::sync::Arc;
use std::sync::atomic::Ordering;

use tokio::sync::mpsc;

use crate::common::Error;
use crate::common::error::ErrorMessage;
use crate::common::errors::GroupAuthorizationError;
use crate::common::protocol::Errors;
use crate::common::requests::ConcreteResponse;
use crate::consumer::ConsumerConfig;
use crate::consumer::internals::events::BackgroundEvent;
use crate::consumer::internals::events::BackgroundEventHandler;

use super::CoordinatorRequestManager;
use super::HeartbeatRequestState;
use super::MemberState;
use super::MembershipManagerHooks;
use super::metrics::HeartbeatMetricsManager;
use super::{PollResult, UnsentRequest};

/// Java's `Errors#exception(String)`: a null
/// broker-supplied message yields the code's own default message, a non-null
/// one replaces it.
///
/// ```java
/// public ApiException exception(String message) {
///     if (message == null) {
///         // If no error message was specified, return an exception with the default error message.
///         return exception;
///     }
///     // Return an exception with the given error message.
///     return builder.apply(message);
/// }
/// ```
fn error_or_default_message(error: Errors, message: Option<&str>) -> Error {
    match message {
        Some(message) => Error::with_message(error, message),
        None => Error::new(error),
    }
}

/// Envelope for routing a heartbeat response `R` (or its
/// transport-level failure) from the spawned response forwarder back
/// to the heartbeat manager's next `poll(now)` call.
///
/// Mirrors the Java `AbstractHeartbeatRequestManager#makeHeartbeatRequest(boolean)`
/// `whenComplete((response, exception) -> { onResponse / onFailure })`
/// dispatch, but
/// defers the state-update to the next bg-task `poll()` cycle. The
/// rationale (per Phase 12.5 PLAN §"2. consumer_heartbeat_request_manager"
/// "Structural decision"): the success path calls
/// `membership_manager.on_heartbeat_success(...)`, and the membership
/// manager owns its own `Arc<Mutex<...>>`. Channel-back ensures no
/// heartbeat-side guard is held when the cross-RM call fires
/// (consumer-threading.md §16).
pub(crate) enum PendingHeartbeatCompletion<R> {
    /// Broker returned a heartbeat response. The forwarder
    /// captures the response body and the completion time; the drain
    /// applies success/error classification on `&mut self` inside the
    /// next `poll(now)`.
    Response {
        response: R,
        completion_time_ms: i64,
        /// `ClientResponse.requestLatencyMs()` — recorded into the heartbeat
        /// metrics sensor (`recordRequestLatency`) when the drain applies it.
        /// Java records this in the `whenComplete` lambda whenever a response
        /// arrives, regardless of the response's error code
        /// (`AbstractHeartbeatRequestManager#makeHeartbeatRequest(boolean)`).
        request_latency_ms: i64,
    },
    /// Transport-level failure (network error, in-flight cancellation,
    /// type mismatch on the response body). The drain calls
    /// `inner.on_failure(...)` and `membership_manager.on_heartbeat_failure(retriable)`.
    Failure { error: Error, completion_time_ms: i64 },
}

/// The group-type-specific half of Java's `AbstractHeartbeatRequestManager<R>`:
/// its `abstract` methods, implemented by each heartbeat request manager (the
/// consumer's [`super::ConsumerHeartbeatRequestManager`], and the share
/// consumer's from P4b). The base methods that call them take the implementor
/// as `this: &mut H` (see the module docs).
pub(crate) trait HeartbeatRequestManagerHooks: Send + 'static {
    /// Java's type parameter `R extends AbstractResponse`.
    type Response: Send + 'static;

    /// The membership manager type `membershipManager()` returns (Java:
    /// `AbstractMembershipManager<R>`).
    type MembershipManager: MembershipManagerHooks<Response = Self::Response>;

    /// The composed base state. Rust-only accessor: Java's subclass *is* the
    /// base.
    fn base(&self) -> &AbstractHeartbeatRequestManager<Self::Response>;

    /// Mutable access to the composed base state; see [`Self::base`].
    fn base_mut(&mut self) -> &mut AbstractHeartbeatRequestManager<Self::Response>;

    /// Java: `public abstract AbstractMembershipManager<R> membershipManager()`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.AbstractHeartbeatRequestManager#membershipManager")]
    fn membership_manager(&self) -> &Arc<Self::MembershipManager>;

    /// The request name used in log lines.
    ///
    /// Java: `abstract String heartbeatRequestName()`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.AbstractHeartbeatRequestManager#heartbeatRequestName")]
    fn heartbeat_request_name(&self) -> &'static str;

    /// Builds the heartbeat request to the current coordinator.
    /// [`AbstractHeartbeatRequestManager::make_heartbeat_request`] attaches the
    /// response handling.
    ///
    /// Java: `abstract NetworkClientDelegate.UnsentRequest buildHeartbeatRequest()`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.AbstractHeartbeatRequestManager#buildHeartbeatRequest")]
    fn build_heartbeat_request(&mut self) -> UnsentRequest;

    /// Takes the group type's response out of a response body, `None` if the
    /// body is of another type. Rust-only: Java's `whenComplete` lambda casts
    /// `(R) response.responseBody()`.
    fn response_body(body: ConcreteResponse) -> Option<Self::Response>;

    /// Java: `abstract Errors errorForResponse(R response)`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.AbstractHeartbeatRequestManager#errorForResponse")]
    fn error_for_response(response: &Self::Response) -> Errors;

    /// The broker-supplied error message, `None` when null.
    ///
    /// Java: `abstract String errorMessageForResponse(R response)`.
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.AbstractHeartbeatRequestManager#errorMessageForResponse"
    )]
    fn error_message_for_response(response: &Self::Response) -> Option<&str>;

    /// Java: `abstract long heartbeatIntervalForResponse(R response)`.
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.AbstractHeartbeatRequestManager#heartbeatIntervalForResponse"
    )]
    fn heartbeat_interval_for_response(response: &Self::Response) -> i64;

    /// Handles a non-retriable request failure specific to the group type;
    /// returns `false` to fall back to the fatal path. The base handles
    /// nothing.
    ///
    /// Java: `public boolean handleSpecificFailure(Throwable exception)`
    /// (base body `return false;`).
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.AbstractHeartbeatRequestManager#handleSpecificFailure")]
    fn handle_specific_failure(&mut self, _error: &Error, _current_time_ms: i64) -> bool {
        false
    }

    /// Handles an error response the base does not classify. `None` falls
    /// back to the fatal path; the base handles nothing.
    ///
    /// Java: `public boolean handleSpecificExceptionInResponse(R response,
    /// long currentTimeMs)` (base body `return false;`), which handles the
    /// error itself and returns whether it did; Rust returns the action for
    /// the base to apply, and passes the error and message the base already
    /// read from the response.
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.AbstractHeartbeatRequestManager#handleSpecificExceptionInResponse"
    )]
    fn handle_specific_error_in_response(
        &mut self,
        _error: Errors,
        _error_message: Option<&str>,
        _current_time_ms: i64,
    ) -> Option<HeartbeatErrorAction> {
        None
    }

    /// Java: `abstract void resetHeartbeatState()`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.AbstractHeartbeatRequestManager#resetHeartbeatState")]
    fn reset_heartbeat_state(&mut self);

    /// Java: `abstract boolean shouldSendLeaveHeartbeatNow()`.
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.AbstractHeartbeatRequestManager#shouldSendLeaveHeartbeatNow"
    )]
    fn should_send_leave_heartbeat_now(&self) -> bool;
}

/// Shared heartbeat lifecycle state composed by
/// [`super::ConsumerHeartbeatRequestManager`].
///
/// All fields are `pub(crate)` so the composing manager (single subclass
/// in scope) can mutate them directly. This matches Java's `protected`
/// semantics.
#[doc(alias = "org.apache.kafka.clients.consumer.internals.AbstractHeartbeatRequestManager")]
pub(crate) struct AbstractHeartbeatRequestManager<R> {
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
    /// `HeartbeatMetricsManager` recording per-heartbeat-send time
    /// (`recordHeartbeatSentMs`) and per-heartbeat-response latency
    /// (`recordRequestLatency`). Java passes it into the constructor of
    /// `AbstractHeartbeatRequestManager` and records in
    /// `AbstractHeartbeatRequestManager#makeHeartbeatRequest(long,boolean)` and the
    /// `whenComplete` lambdas of `AbstractHeartbeatRequestManager#makeHeartbeatRequest(boolean)` and
    /// `AbstractHeartbeatRequestManager#logResponse`. In Rust it shares the consumer's
    /// `Arc<Metrics>` registry and is wired post-construction (like the
    /// commit manager's metrics manager); `None` for tests that don't
    /// exercise metrics (recording is then a no-op, value-neutral).
    pub(crate) metrics_manager: Option<Arc<HeartbeatMetricsManager>>,
    /// Absolute wall-clock millisecond expiration for the poll timer, or
    /// [`i64::MAX`] as a sentinel meaning "not armed yet".
    ///
    /// Java models this with a `Timer` armed at construction
    /// (`AbstractHeartbeatRequestManager`'s constructor:
    /// `this.pollTimer = time.timer(maxPollIntervalMs);`). In the Rust
    /// translation the timer is **not** armed at construction — it is
    /// armed by the first [`Self::reset_poll_timer`] call from the
    /// `ApplicationEventProcessor::process(AsyncPollEvent)` arm
    /// (mirroring Java's `hrm.resetPollTimer(event.pollTimeMs())`).
    ///
    /// **Why the deviation from Java**: Java's bg-thread starts running
    /// the heartbeat manager's `poll()` more or less immediately on
    /// consumer construction, and the `pollTimer.update(now)` call
    /// inside `poll()` keeps the timer tracking real time. The Rust bg
    /// task has higher latency to spin up (one `tokio::spawn` + the
    /// first response round-trip) — with `max.poll.interval.ms=1000`
    /// the consumer can already be near-expired by the time the user's
    /// first `poll()` call lands, and the consumer fences itself during
    /// the initial join sequence. By deferring the timer arm to the
    /// first `reset_poll_timer` (Java's `resetPollTimer` is called on
    /// every `AsyncPollEvent`, so semantics are preserved for the
    /// steady-state case), we ensure the initial join + assignment
    /// window has a full `max.poll.interval.ms` budget. See Issue 9 in
    /// `design/history/Milestone-8/Phase-13/COMMENTS.DONE.1.md`.
    poll_timer_expires_at_ms: i64,
    /// Every "received heartbeat interval" message [`Self::on_successful_response`]
    /// logged, in order. The Rust stand-in for the `LogCaptureAppender` in
    /// `AbstractHeartbeatRequestManagerTest#testLogsHeartbeatIntervalReceivedFromCoordinatorOnlyWhenChanged`:
    /// it is appended at the log site, with the level and the string that
    /// were logged (Java's appender is set to `Level.INFO`, so a regression
    /// to a lower level fails its test too).
    #[cfg(test)]
    pub(crate) logged_heartbeat_interval_messages: Vec<(log::Level, String)>,
    /// Cloned into each spawned response forwarder so the forwarder
    /// can route the heartbeat response back through `poll(now)`'s
    /// drain step. See [`PendingHeartbeatCompletion`] for the
    /// rationale (channel-back avoids §16 violations on the
    /// membership manager cross-call).
    pending_completion_tx: mpsc::UnboundedSender<PendingHeartbeatCompletion<R>>,
    /// Drained by `drain_pending_completions` at the top of every `poll(now)`
    /// call. Held directly (no outer `Mutex`) because the heartbeat manager is
    /// single-owner — only the bg-task `poll(now)` cycle touches it.
    /// (`mpsc::UnboundedReceiver` is `Send` but not `Sync`; single-ownership
    /// keeps it sound.)
    pub(crate) pending_completion_rx: mpsc::UnboundedReceiver<PendingHeartbeatCompletion<R>>,
    /// The bg task's wakeup `Notify` (a clone of `event_notify`), poked by the
    /// spawned response forwarder once it has queued a completion for
    /// `drain_pending_completions`, so the next `run_once` drains it
    /// at once. Java needs no equivalent: its `whenComplete` lambda runs
    /// inside the network poll, so `onResponse` / `onFailure` have run before
    /// the next `runOnce`. Here the forwarder runs after the poll returned;
    /// without the poke the response waits out the poll timeout, which while
    /// the request is in flight is up to the heartbeat interval
    /// (`maximum_time_to_wait`) — delaying a new assignment,
    /// a fence or a fatal error by that much. The FindCoordinator, commit and
    /// fetch forwarders wake the bg task the same way. A default no-listener
    /// `Notify` stands in until the subclass installs the real one (unit tests
    /// that drive no bg task keep the default).
    pub(crate) completion_notify: Arc<tokio::sync::Notify>,
}

impl<R: Send + 'static> AbstractHeartbeatRequestManager<R> {
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
        state: &mut AbstractHeartbeatRequestManager<R>,
        current_time_ms: i64,
    ) -> PollResult {
        state.heartbeat_request_state.on_send_attempt(current_time_ms);
        // Java: `metricsManager.recordHeartbeatSentMs(currentTimeMs)` in
        // `AbstractHeartbeatRequestManager#makeHeartbeatRequest(long,boolean)`.
        if let Some(metrics_manager) = state.metrics_manager.as_ref() {
            metrics_manager.record_heartbeat_sent_ms(current_time_ms);
        }
        state.heartbeat_request_state.reset_timer();
        PollResult::new(state.heartbeat_request_state.heartbeat_interval_ms(), vec![request])
    }

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

    /// Constructs a new instance starting from the supplied wall-clock
    /// time. The poll timer starts running immediately with a duration
    /// of `max.poll.interval.ms`.
    ///
    /// Java: `AbstractHeartbeatRequestManager(LogContext, Time, ConsumerConfig,
    /// CoordinatorRequestManager, BackgroundEventHandler, HeartbeatMetricsManager)`.
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.AbstractHeartbeatRequestManager#AbstractHeartbeatRequestManager"
    )]
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
        let (pending_completion_tx, pending_completion_rx) = mpsc::unbounded_channel();
        let heartbeat_request_state = HeartbeatRequestState::new(
            current_time_ms,
            0,
            retry_backoff_ms,
            retry_backoff_max_ms,
            Self::RETRY_BACKOFF_JITTER,
        );
        Self {
            max_poll_interval_ms,
            coordinator_request_manager,
            heartbeat_request_state,
            background_event_handler,
            metrics_manager: None,
            // Deviation from Java: poll timer is NOT armed at
            // construction. It is armed by the first
            // `reset_poll_timer` call from the AsyncPoll event
            // arm. See [`Self::poll_timer_expires_at_ms`] doc-comment.
            poll_timer_expires_at_ms: i64::MAX,
            #[cfg(test)]
            logged_heartbeat_interval_messages: Vec::new(),
            pending_completion_tx,
            pending_completion_rx,
            completion_notify: Arc::new(tokio::sync::Notify::new()),
        }
    }

    /// Visible-for-testing constructor: lets callers supply a custom
    /// [`HeartbeatRequestState`] (mirrors Java's package-private second
    /// constructor).
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.AbstractHeartbeatRequestManager#AbstractHeartbeatRequestManager"
    )]
    pub(crate) fn with_state(
        _current_time_ms: i64,
        config: &ConsumerConfig,
        coordinator_request_manager: Arc<CoordinatorRequestManager>,
        heartbeat_request_state: HeartbeatRequestState,
        background_event_handler: Arc<BackgroundEventHandler>,
    ) -> Self {
        let max_poll_interval_ms = config.max_poll_interval_ms();
        let (pending_completion_tx, pending_completion_rx) = mpsc::unbounded_channel();
        Self {
            max_poll_interval_ms,
            coordinator_request_manager,
            heartbeat_request_state,
            background_event_handler,
            metrics_manager: None,
            // See [`Self::poll_timer_expires_at_ms`] doc-comment — the
            // timer is armed by the first `reset_poll_timer` call, not
            // at construction.
            poll_timer_expires_at_ms: i64::MAX,
            #[cfg(test)]
            logged_heartbeat_interval_messages: Vec::new(),
            pending_completion_tx,
            pending_completion_rx,
            completion_notify: Arc::new(tokio::sync::Notify::new()),
        }
    }

    /// Returns `true` if the poll timer has expired at `current_time_ms`.
    pub(crate) fn poll_timer_is_expired(&self, current_time_ms: i64) -> bool {
        current_time_ms >= self.poll_timer_expires_at_ms
    }

    /// Returns remaining ms on the poll timer, clamped at 0. When the
    /// timer is not armed yet (Issue 9), [`Self::poll_timer_expires_at_ms`]
    /// is [`i64::MAX`] and `saturating_sub` produces [`i64::MAX`] — i.e.
    /// "infinite time remaining".
    pub(crate) fn poll_timer_remaining_ms(&self, current_time_ms: i64) -> i64 {
        self.poll_timer_expires_at_ms.saturating_sub(current_time_ms).max(0)
    }

    /// Returns ms by which the poll timer is overdue (negative if not
    /// expired). Mirrors Java's `Timer.isExpiredBy()`. When the timer is
    /// not armed yet (Issue 9), [`Self::poll_timer_expires_at_ms`] is
    /// [`i64::MAX`] and `saturating_sub` produces [`i64::MIN`] — i.e.
    /// "very not overdue".
    pub(crate) fn poll_timer_is_expired_by(&self, current_time_ms: i64) -> i64 {
        current_time_ms.saturating_sub(self.poll_timer_expires_at_ms)
    }

    /// Resets the poll timer so it expires `max_poll_interval_ms` from
    /// `current_time_ms`. Java's `Timer.reset(maxPollIntervalMs)` (which
    /// internally snaps `now` from `Time.milliseconds()`).
    ///
    /// **Phase 10 carry-over**: Java's `resetPollTimer(pollMs)` also
    /// checks `pollTimer.isExpired()` and calls
    /// `membershipManager().maybeRejoinStaleMember()` when expired (see
    /// `AbstractHeartbeatRequestManager#resetPollTimer`). The abstract
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
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.AbstractHeartbeatRequestManager#resetPollTimer")]
    pub(crate) fn reset_poll_timer(&mut self, current_time_ms: i64) {
        self.poll_timer_expires_at_ms = current_time_ms + i64::from(self.max_poll_interval_ms);
    }

    /// Surface a coordinator fatal error to the application thread via
    /// the background-event channel, if one has been recorded.
    ///
    /// Java: `maybePropagateCoordinatorFatalErrorEvent()`.
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.AbstractHeartbeatRequestManager#maybePropagateCoordinatorFatalErrorEvent"
    )]
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
    /// heartbeat response. Returns:
    ///
    /// - `HeartbeatErrorAction::Handled` — error already classified and
    ///   the manager has updated its own backoff / state listener.
    /// - `HeartbeatErrorAction::Fenced` — caller must mark the member
    ///   fenced.
    /// - `HeartbeatErrorAction::Fatal(error)` — caller must mark the
    ///   member fatal with the supplied error event.
    /// - `HeartbeatErrorAction::DelegateToSpecific` — error is not in
    ///   the abstract dispatch; the caller's
    ///   `handle_specific_error_in_response` runs.
    ///
    /// Java: `onErrorResponse(R response, long currentTimeMs)`. Rust
    /// splits the "advise the membership manager" half off because the
    /// abstract layer cannot mutate the membership state directly
    /// (the membership manager is held by the composing consumer).
    /// `error_message` is the broker-supplied `ErrorMessage` field of the
    /// heartbeat response (Java's `errorMessageForResponse(response)` →
    /// `response.data().errorMessage()`), and is `None` when the broker left it
    /// null. Java threads that nullable string through the switch and passes it
    /// to `Errors.exception(String)`, which falls back to the code's own
    /// default message when it is null (`Errors#exception(String)`) —
    /// [`Error::with_message`] / [`Error::new`] are the two halves of that.
    ///
    /// `heartbeat_request_name` is the subclass's `heartbeatRequestName()`
    /// ([`HeartbeatRequestManagerHooks::heartbeat_request_name`]), used in the
    /// log lines.
    ///
    /// `group_id` is Java's `membershipManager().groupId()`, needed by the
    /// `GROUP_AUTHORIZATION_FAILED` arm. The Rust split keeps the membership
    /// manager out of this layer, so it is passed in.
    pub(crate) fn classify_response_error(
        &mut self,
        heartbeat_request_name: &str,
        error: Errors,
        error_message: Option<&str>,
        group_id: &str,
        current_time_ms: i64,
    ) -> HeartbeatErrorAction {
        self.heartbeat_request_state.on_failed_attempt(current_time_ms);
        match error {
            Errors::NotCoordinator => {
                self.coordinator_request_manager
                    .mark_coordinator_unknown(error_message.unwrap_or_default(), current_time_ms);
                // Skip backoff so the next HB targets the new coordinator
                self.heartbeat_request_state.reset();
                HeartbeatErrorAction::Handled
            },
            Errors::CoordinatorNotAvailable => {
                self.coordinator_request_manager
                    .mark_coordinator_unknown(error_message.unwrap_or_default(), current_time_ms);
                self.heartbeat_request_state.reset();
                HeartbeatErrorAction::Handled
            },
            Errors::CoordinatorLoadInProgress => {
                // Backoff and retry.
                HeartbeatErrorAction::Handled
            },
            // Note: `Errors::GroupIdNotFound` is intentionally NOT
            // handled at the abstract layer — it falls through to
            // `DelegateToSpecific` so the consumer-specific layer
            // (`ConsumerHeartbeatRequestManager::handle_specific_error_in_response`)
            // can branch on the current `memberEpoch`. See that
            // method's `GROUP_ID_NOT_FOUND` arm for the rationale and
            // Issue 9 in
            // `design/history/Milestone-8/Phase-13/COMMENTS.DONE.1.md`.
            Errors::GroupAuthorizationFailed => {
                // Java does NOT pass the broker's `errorMessage` here: it
                // builds `GroupAuthorizationException.forGroupId(groupId)` and
                // hands *that* message to `error.exception(...)` in
                // `org.apache.kafka.clients.consumer.internals.AbstractHeartbeatRequestManager#onErrorResponse`, so the
                // application sees "Not authorized to access group: <groupId>".
                // The resulting class carries no group id of its own, because
                // Java rebuilds it through `Errors.GROUP_AUTHORIZATION_FAILED
                // .exception(message)` — `Error::with_message` matches that.
                let auth_error = GroupAuthorizationError::for_group_id(group_id);
                log::error!(
                    "{} failed due to group authorization failure: {}",
                    heartbeat_request_name,
                    auth_error.message()
                );
                HeartbeatErrorAction::Fatal(Error::with_message(
                    Errors::GroupAuthorizationFailed,
                    auth_error.message().to_string(),
                ))
            },
            Errors::TopicAuthorizationFailed => {
                // Surface the auth error via the background-event
                // channel so it's returned on the next poll. The member
                // stays in its current state to allow recovery if ACLs
                // are added.
                //
                // Java calls `error.exception()` with NO argument here, in
                // `org.apache.kafka.clients.consumer.internals.AbstractHeartbeatRequestManager#onErrorResponse`:
                // the code's default "Topic authorization failed." — even though
                // `errorMessage` is in scope and is used in the log line
                // immediately above. That asymmetry with the arms below is
                // deliberate in Java, so it is reproduced rather than tidied.
                log::error!(
                    "{} failed due to {:?}: {}",
                    heartbeat_request_name,
                    error,
                    error_message.unwrap_or_default()
                );
                let _ = self
                    .background_event_handler
                    .add(BackgroundEvent::Error { error: Error::new(error) }, current_time_ms);
                HeartbeatErrorAction::Handled
            },
            Errors::InvalidRequest | Errors::GroupMaxSizeReached | Errors::UnsupportedAssignor => {
                log::error!(
                    "{} failed due to {:?}: {}",
                    heartbeat_request_name,
                    error,
                    error_message.unwrap_or_default()
                );
                HeartbeatErrorAction::Fatal(error_or_default_message(error, error_message))
            },
            Errors::FencedMemberEpoch | Errors::UnknownMemberId => {
                // Skip backoff so the next rejoin heartbeat is sent ASAP.
                self.heartbeat_request_state.reset();
                HeartbeatErrorAction::Fenced
            },
            Errors::InvalidRegularExpression => {
                log::error!(
                    "{} failed due to {:?}: {}",
                    heartbeat_request_name,
                    error,
                    error_message.unwrap_or_default()
                );
                // Java concatenates the broker text onto its own prefix in
                // `org.apache.kafka.clients.consumer.internals.AbstractHeartbeatRequestManager#onErrorResponse`, so the
                // message is never null here and `exception(String)` never
                // falls back. A null broker message concatenates as "null" in
                // Java; Rust renders the empty string, which is the same
                // information without the misleading literal.
                HeartbeatErrorAction::Fatal(Error::with_message(
                    Errors::InvalidRegularExpression,
                    format!(
                        "Invalid RE2J SubscriptionPattern provided in the call to subscribe. {}",
                        error_message.unwrap_or_default()
                    ),
                ))
            },
            _ => HeartbeatErrorAction::DelegateToSpecific,
        }
    }

    /// Marks the manager as having received a successful heartbeat
    /// response. Updates the backoff state and the heartbeat interval,
    /// logging the interval when it differs from the current one
    /// (KAFKA-20761).
    ///
    /// Java: `onResponse(R response, long currentTimeMs)` (success
    /// branch only — error responses go through
    /// [`Self::classify_response_error`];
    /// `AbstractHeartbeatRequestManager#onResponse`). `member_id` is Java's
    /// `membershipManager().memberId()`: the Rust split keeps the membership
    /// manager out of this layer, so the caller passes it in (as for
    /// `classify_response_error`'s `group_id`). The caller then runs
    /// `membershipManager().onHeartbeatSuccess(response)`, Java's next line.
    pub(crate) fn on_successful_response(
        &mut self,
        member_id: &str,
        new_heartbeat_interval_ms: i64,
        current_time_ms: i64,
    ) {
        let previous_heartbeat_interval_ms = self.heartbeat_request_state.heartbeat_interval_ms();
        // The heartbeat interval is a group config owned by the broker, so log it when it changes to give
        // visibility into the value the coordinator is applying (it is not derivable from client config).
        if new_heartbeat_interval_ms != previous_heartbeat_interval_ms {
            const LEVEL: log::Level = log::Level::Info;
            let message = Self::heartbeat_interval_received_message(member_id, new_heartbeat_interval_ms);
            log::log!(LEVEL, "{message}");
            #[cfg(test)]
            self.logged_heartbeat_interval_messages.push((LEVEL, message));
        }
        self.heartbeat_request_state
            .update_heartbeat_interval_ms(current_time_ms, new_heartbeat_interval_ms);
        self.heartbeat_request_state.on_successful_attempt(current_time_ms);
    }

    /// The text of Java's `logger.info("Member {} received heartbeat interval {}ms from the group
    /// coordinator", ...)` (`AbstractHeartbeatRequestManager#onResponse`).
    fn heartbeat_interval_received_message(member_id: &str, heartbeat_interval_ms: i64) -> String {
        format!("Member {member_id} received heartbeat interval {heartbeat_interval_ms}ms from the group coordinator")
    }

    /// The classification half of Java's `onFailure(Throwable, long)`:
    /// records the failed attempt and, for a retriable error, marks the
    /// coordinator disconnected. [`Self::on_failure`] runs the rest (the
    /// subclass handler, the fatal path and
    /// `membershipManager().onHeartbeatFailure(...)`).
    pub(crate) fn classify_failure(
        &mut self,
        heartbeat_request_name: &str,
        error: &Error,
        current_time_ms: i64,
    ) -> HeartbeatFailureAction {
        self.heartbeat_request_state.on_failed_attempt(current_time_ms);
        if error.is_retriable_error() {
            self.coordinator_request_manager
                .handle_coordinator_disconnect(error, current_time_ms);
            log::debug!(
                "{} failed because of the retriable error. Will retry in {} ms: {}",
                heartbeat_request_name,
                self.heartbeat_request_state.remaining_backoff_ms(current_time_ms),
                error
            );
            HeartbeatFailureAction::Retriable
        } else {
            HeartbeatFailureAction::NonRetriable
        }
    }

    /// Java: `makeHeartbeatRequest(boolean ignoreResponse)`: builds the
    /// request through [`HeartbeatRequestManagerHooks::build_heartbeat_request`]
    /// (Java's `buildHeartbeatRequest()`) and spawns a forwarder
    /// that awaits the response receiver and routes the result back
    /// to [`Self::drain_pending_completions`] via the
    /// [`PendingHeartbeatCompletion`] channel — mirroring Java's
    /// `AbstractHeartbeatRequestManager#makeHeartbeatRequest(boolean)`'s
    /// `whenComplete((response, exception) -> ...)` lambda.
    ///
    /// If `ignore_response` is true (Java parity with its `ignoreResponse` branch — the
    /// LEAVING / poll-timer-expired path), the forwarder drops the
    /// completion silently instead of enqueueing it. Java's
    /// `logResponse(request)` path also drops the response side-effect
    /// without driving state machinery.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.AbstractHeartbeatRequestManager#makeHeartbeatRequest")]
    pub(crate) fn make_heartbeat_request<H: HeartbeatRequestManagerHooks<Response = R>>(
        this: &mut H,
        ignore_response: bool,
    ) -> UnsentRequest {
        let mut unsent = this.build_heartbeat_request();
        let response_body = H::response_body;
        let base = this.base();

        let response_rx = unsent.take_response_receiver().expect("receiver fresh");
        let tx = base.pending_completion_tx.clone();
        // For the `logResponse`/ignore path, latency must still be recorded
        // (Java `org.apache.kafka.clients.consumer.internals.AbstractHeartbeatRequestManager#logResponse`). The normal
        // path records it via the drained envelope, so the metrics manager is only
        // cloned for the ignore path.
        let ignore_path_metrics = if ignore_response {
            base.metrics_manager.clone()
        } else {
            None
        };
        // The completion-time cell, not a handler clone: a clone would keep the
        // sender alive, so `response_rx` could never see the request dropped
        // uncompleted (`NetworkClient::close` with it in flight) and this
        // task would never exit.
        let completion_time_ms = unsent.handler().completion_time_ms_cell();
        let completion_notify = Arc::clone(&base.completion_notify);
        tokio::spawn(async move {
            let result = response_rx.await;
            // Java: `long completionTimeMs = request.handler().completionTimeMs()`
            // — read once the request has completed, so it is the time the
            // response (or failure) arrived, not the time the request was sent.
            let now_ms = completion_time_ms.load(Ordering::Acquire);
            let completion = match result {
                Ok(Ok(mut client_response)) => {
                    // Java: `response.requestLatencyMs()` — captured before
                    // `take_response_body()`. Recorded in the drain (normal
                    // path) or inline below (`logResponse`/ignore path).
                    let request_latency_ms = client_response.request_latency_ms();
                    match client_response.take_response_body().and_then(response_body) {
                        Some(resp) => PendingHeartbeatCompletion::Response {
                            response: resp,
                            completion_time_ms: now_ms,
                            request_latency_ms,
                        },
                        None => PendingHeartbeatCompletion::Failure {
                            error: Error::new(Errors::UnknownServerError),
                            completion_time_ms: now_ms,
                        },
                    }
                },
                Ok(Err(err)) => PendingHeartbeatCompletion::Failure { error: err, completion_time_ms: now_ms },
                Err(_recv) => PendingHeartbeatCompletion::Failure {
                    error: Error::new(Errors::NetworkError),
                    completion_time_ms: now_ms,
                },
            };
            // `ignore_response` mirrors Java's
            // `org.apache.kafka.clients.consumer.internals.AbstractHeartbeatRequestManager#logResponse` path:
            // log the outcome but do NOT drive the consumer state
            // machinery. We drop the envelope outright; the response
            // future itself was already resolved (the forwarder
            // observed the result), so any callers waiting on it have
            // already been unblocked.
            if ignore_response {
                // Java `org.apache.kafka.clients.consumer.internals.AbstractHeartbeatRequestManager#logResponse`:
                // record latency for the arrived response, then drop the
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
                // Wake the bg task so the next `poll(now)` drains the
                // completion now rather than after the poll timeout. Ordered
                // after the send: once the permit is stored, the completion
                // is queued. The ignore path drives no state, so it does not
                // wake.
                completion_notify.notify_one();
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
    /// acquired only inside its `abstract_mm.on_heartbeat_success` /
    /// `on_heartbeat_failure` methods.
    pub(crate) fn drain_pending_completions<H: HeartbeatRequestManagerHooks<Response = R>>(
        this: &mut H,
        _current_time_ms: i64,
    ) {
        while let Ok(completion) = this.base_mut().pending_completion_rx.try_recv() {
            match completion {
                PendingHeartbeatCompletion::Response { response, completion_time_ms, request_latency_ms } => {
                    // Java: `metricsManager.recordRequestLatency(response.requestLatencyMs())`
                    // before `onResponse`
                    // (`AbstractHeartbeatRequestManager#makeHeartbeatRequest(boolean)`).
                    if let Some(metrics_manager) = this.base().metrics_manager.as_ref() {
                        metrics_manager.record_request_latency(request_latency_ms);
                    }
                    Self::on_response(this, &response, completion_time_ms);
                },
                PendingHeartbeatCompletion::Failure { error, completion_time_ms } => {
                    Self::on_failure(this, &error, completion_time_ms);
                },
            }
        }
    }

    /// Dispatch a heartbeat response body to the
    /// success or error path. Mirrors Java's `onResponse(R response,
    /// long currentTimeMs)` (`AbstractHeartbeatRequestManager#onResponse`).
    fn on_response<H: HeartbeatRequestManagerHooks<Response = R>>(this: &mut H, response: &R, completion_time_ms: i64) {
        let mm = Arc::clone(this.membership_manager());
        let error = H::error_for_response(response);
        if error == Errors::None {
            let new_interval_ms = H::heartbeat_interval_for_response(response);
            let member_id = mm.abstract_mm().member_id();
            this.base_mut()
                .on_successful_response(&member_id, new_interval_ms, completion_time_ms);
            if let Err(e) = mm.abstract_mm().on_heartbeat_success::<H::MembershipManager>(response) {
                log::error!("on_heartbeat_success failed: {}", e);
                let _ = this
                    .base()
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
        // Java reference: `org.apache.kafka.clients.consumer.internals.AbstractHeartbeatRequestManager#onErrorResponse`
        // (`resetHeartbeatState();` runs at its top,
        // before `heartbeatRequestState.onFailedAttempt(currentTimeMs)`
        // and the per-error switch).
        this.reset_heartbeat_state();
        // Classify via the abstract dispatch first, then delegate to
        // the consumer-specific extras.
        //
        // Java: `String errorMessage = errorMessageForResponse(response);`
        // (in `org.apache.kafka.clients.consumer.internals.AbstractHeartbeatRequestManager#onErrorResponse`), whose
        // consumer implementation is `return response.data().errorMessage();`
        // (`org.apache.kafka.clients.consumer.internals.ConsumerHeartbeatRequestManager#errorMessageForResponse`). It
        // is the BROKER-supplied diagnostic and is nullable — for several codes
        // (`UNSUPPORTED_ASSIGNOR`, `INVALID_REGULAR_EXPRESSION`, and every
        // unknown code) it is the only information available about what the
        // broker rejected, so it must not be substituted with the error
        // enum's own name.
        let error_message = H::error_message_for_response(response);
        let group_id = mm.abstract_mm().group_id();
        let heartbeat_request_name = this.heartbeat_request_name();
        let action = this.base_mut().classify_response_error(
            heartbeat_request_name,
            error,
            error_message,
            &group_id,
            completion_time_ms,
        );
        let final_action = match action {
            HeartbeatErrorAction::DelegateToSpecific => this
                .handle_specific_error_in_response(error, error_message, completion_time_ms)
                .unwrap_or_else(|| {
                    // Java: the `default:` arm of the switch in
                    // `org.apache.kafka.clients.consumer.internals.AbstractHeartbeatRequestManager#onErrorResponse`
                    // calls `handleSpecificExceptionInResponse(...)`; if
                    // that returns false (no consumer-specific handler
                    // matched), Java falls back to
                    // `handleFatalFailure(error.exception(errorMessage))`.
                    // Rust's mapping: `None` from the specific handler
                    // means "no match" — fall through to the same fatal
                    // path so unknown / future error codes don't get
                    // silently swallowed.
                    log::error!(
                        "{} failed due to unexpected error {:?}: {}",
                        heartbeat_request_name,
                        error,
                        error_message.unwrap_or_default()
                    );
                    // Java: `handleFatalFailure(error.exception(errorMessage))`
                    // — `Errors.exception(String)` falls back to the code's own
                    // default message when the broker sent none
                    // (`org.apache.kafka.common.protocol.Errors#exception(String)`).
                    HeartbeatErrorAction::Fatal(match error_message {
                        Some(message) => Error::with_message(error, message),
                        None => Error::new(error),
                    })
                }),
            other => other,
        };
        match final_action {
            HeartbeatErrorAction::Handled => {},
            HeartbeatErrorAction::Fenced => {
                // Java: `membershipManager().transitionToFenced()`
                // (the FENCED_MEMBER_EPOCH and UNKNOWN_MEMBER_ID arms of
                // `org.apache.kafka.clients.consumer.internals.AbstractHeartbeatRequestManager#onErrorResponse`).
                // The fence is treated as an INTERNAL state-machine
                // event: Java does NOT call
                // `backgroundEventHandler.add(new ErrorEvent(...))`
                // here (compare with `AbstractHeartbeatRequestManager#handleFatalFailure`,
                // which does emit an ErrorEvent). The
                // member transitions through FENCED → JOINING and
                // re-joins silently; the user never sees the fence
                // from `poll()`. Emitting a `BackgroundEvent::Error`
                // would surface `Error::FencedMemberEpoch` to
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
                if let Err(e) = mm.abstract_mm().transition_to_fenced(&*mm, completion_time_ms) {
                    // Java's `whenComplete` lambda logs what
                    // `transitionToFenced` throws and does not rethrow.
                    log::warn!("transition_to_fenced failed: {}", e);
                }
            },
            HeartbeatErrorAction::Fatal(err) => {
                // Java: `handleFatalFailure(error.exception(...))`
                // (`org.apache.kafka.clients.consumer.internals.AbstractHeartbeatRequestManager#handleFatalFailure`) —
                // emits an `ErrorEvent` AND calls
                // `membershipManager().transitionToFatal()`, in that
                // order.
                let _ = this
                    .base()
                    .background_event_handler
                    .add(BackgroundEvent::Error { error: err.clone() }, completion_time_ms);
                if let Err(e) = mm.abstract_mm().transition_to_fatal(&*mm, completion_time_ms) {
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
        mm.abstract_mm().on_heartbeat_failure(false);
    }

    /// Transport-level / non-response failure handler. Mirrors Java's
    /// `onFailure(Throwable, long)`
    /// (`AbstractHeartbeatRequestManager#onFailure`).
    pub(crate) fn on_failure<H: HeartbeatRequestManagerHooks<Response = R>>(
        this: &mut H,
        error: &Error,
        completion_time_ms: i64,
    ) {
        let mm = Arc::clone(this.membership_manager());
        // Java: `resetHeartbeatState()` at the top of `onFailure`.
        this.reset_heartbeat_state();
        let heartbeat_request_name = this.heartbeat_request_name();
        let abstract_action = this
            .base_mut()
            .classify_failure(heartbeat_request_name, error, completion_time_ms);
        let retriable = matches!(abstract_action, HeartbeatFailureAction::Retriable);
        if !retriable {
            // Java: `handleSpecificFailure(exception)` runs only on
            // the non-retriable path. If it returns false, fall back
            // to `handleFatalFailure(exception)`.
            let specific_handled = this.handle_specific_failure(error, completion_time_ms);
            if !specific_handled {
                log::error!("{} failed due to fatal error: {}", heartbeat_request_name, error);
                // Java: `handleFatalFailure(exception)`
                // (`org.apache.kafka.clients.consumer.internals.AbstractHeartbeatRequestManager#handleFatalFailure`) —
                // emits an `ErrorEvent` AND calls
                // `membershipManager().transitionToFatal()`, in that
                // order. Both are mirrored here.
                let _ = this
                    .base()
                    .background_event_handler
                    .add(BackgroundEvent::Error { error: error.clone() }, completion_time_ms);
                if let Err(e) = mm.abstract_mm().transition_to_fatal(&*mm, completion_time_ms) {
                    log::warn!("transition_to_fatal failed: {}", e);
                }
            }
        }
        // Java: `membershipManager().onHeartbeatFailure(retriable)`
        // at the tail of `onFailure`.
        mm.abstract_mm().on_heartbeat_failure(retriable);
    }

    /// Java: `poll(long currentTimeMs)`. Mirrors
    /// `AbstractHeartbeatRequestManager.poll(...)` phase-for-phase.
    ///
    /// Per `consumer-threading.md` §10, `poll` is sync. The membership
    /// state machine's async transitions (fenced, fatal, reconcile)
    /// happen in the bg-task path (Phase 10) which can `.await`;
    /// `poll` only computes whether a heartbeat needs to be sent.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.AbstractHeartbeatRequestManager#poll")]
    pub(crate) fn poll<H: HeartbeatRequestManagerHooks<Response = R>>(
        this: &mut H,
        current_time_ms: i64,
    ) -> PollResult {
        let mm = Arc::clone(this.membership_manager());
        // 0. Drain any pending heartbeat completions from prior
        // spawned forwarders. State-update happens before the next
        // emission decision so `should_heartbeat_now` /
        // `request_in_flight` observe the post-completion state.
        Self::drain_pending_completions(this, current_time_ms);

        // 1. Skip-heartbeat short-circuit.
        let coordinator_known = this.base().coordinator_request_manager.coordinator().is_some();
        if !coordinator_known
            || mm
                .abstract_mm()
                .inner
                .lock()
                .map(|g| g.should_skip_heartbeat())
                .unwrap_or(false)
        {
            // Java: membershipManager().onHeartbeatRequestSkipped().
            // We ignore the error: this only mutates state on LEAVING.
            let _ = mm.abstract_mm().on_heartbeat_request_skipped();
            this.base().maybe_propagate_coordinator_fatal_error_event(current_time_ms);
            return PollResult::empty();
        }

        // 2. Poll-timer-expired stale path. Java calls
        // `pollTimer.update(currentTimeMs)` here; the Rust deadline-based
        // timer doesn't need the update — `poll_timer_is_expired` reads
        // `current_time_ms` directly.
        // Java parity, including its limitation: a dynamic member closed with
        // REMAIN_IN_GROUP is LEAVING but `is_leaving_group()` is false for
        // it, so if its poll timer has already expired this branch is taken
        // on every poll, `transition_to_sending_leave_group(true)` fails
        // (LEAVING -> PREPARE_LEAVING is not a valid transition) and no
        // heartbeat is built. The close-time leave then never completes and
        // `close()` waits out its close timeout, as Java's does (where the
        // `IllegalStateException` escapes `runOnce` on every iteration).
        if this.base().poll_timer_is_expired(current_time_ms) && !mm.is_leaving_group() {
            log::warn!(
                "Consumer poll timeout has expired. This means the time between subsequent calls to poll() was longer than the configured max.poll.interval.ms."
            );
            // Java: membershipManager().transitionToSendingLeaveGroup(true).
            if let Err(e) = mm.abstract_mm().transition_to_sending_leave_group(mm.leave_group_epoch(), true) {
                log::warn!("transition_to_sending_leave_group failed: {}", e);
                return PollResult::empty();
            }
            // Build leave heartbeat (ignoreResponse=true) per Java's
            // `org.apache.kafka.clients.consumer.internals.AbstractHeartbeatRequestManager#poll`.
            let request = Self::make_heartbeat_request(this, true);
            // Java parity: `makeHeartbeatRequest(currentTimeMs, true)` records
            // the heartbeat-sent time (`AbstractHeartbeatRequestManager#makeHeartbeatRequest(long,boolean)`)
            // for every send, including this poll-timer-expired leave path.
            // The normal path records it inside `make_heartbeat_poll_result`
            // below; this branch builds its own `PollResult`, so record here.
            if let Some(metrics_manager) = this.base().metrics_manager.as_ref() {
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
            if let Err(e) = mm.abstract_mm().on_heartbeat_request_generated() {
                log::warn!("on_heartbeat_request_generated (poll-timer-expiry) failed: {}", e);
            }
            // If the member is now STALE, the assignment must be released via
            // the §31 onPartitionsLost listener — the tail of Java's
            // `transitionToStale`. Enqueuing that callback needs no `.await`
            // (the ack is stored for the bg task to drive), so it runs
            // inline here, as it does in Java.
            if mm.abstract_mm().state() == MemberState::Stale
                && let Err(e) = mm.abstract_mm().transition_to_stale(&*mm, current_time_ms)
            {
                log::warn!("transition_to_stale failed: {}", e);
            }
            this.base_mut().heartbeat_request_state.reset();
            this.reset_heartbeat_state();
            return PollResult::new(this.base().heartbeat_request_state.heartbeat_interval_ms(), vec![request]);
        }

        // 3. Decide whether to heartbeat now.
        let heartbeat_now = this.should_send_leave_heartbeat_now() || {
            let inner = mm.abstract_mm().inner.lock();
            let guard = match inner {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            guard.should_heartbeat_now() && !this.base().heartbeat_request_state.request_in_flight()
        };

        if !this.base_mut().heartbeat_request_state.can_send_request(current_time_ms) && !heartbeat_now {
            return PollResult::with_time_until_next_poll_ms(
                this.base().heartbeat_request_state.time_to_next_heartbeat_ms(current_time_ms),
            );
        }

        // Java: `makeHeartbeatRequest(currentTimeMs, false)`.
        let request = Self::make_heartbeat_request(this, false);
        // Java: record send attempt, reset timer, increment metrics.
        // membershipManager().onHeartbeatRequestGenerated() advances
        // ACKNOWLEDGING / LEAVING / etc.
        if let Err(e) = mm.abstract_mm().on_heartbeat_request_generated() {
            log::warn!("on_heartbeat_request_generated failed: {}", e);
        }
        Self::make_heartbeat_poll_result(request, this.base_mut(), current_time_ms)
    }

    #[doc(alias = "org.apache.kafka.clients.consumer.internals.AbstractHeartbeatRequestManager#pollOnClose")]
    pub(crate) fn poll_on_close<H: HeartbeatRequestManagerHooks<Response = R>>(
        this: &mut H,
        current_time_ms: i64,
    ) -> PollResult {
        // Drain any pending completions one last time so close paths
        // observe the post-completion state.
        Self::drain_pending_completions(this, current_time_ms);
        // Java: if (membershipManager().isLeavingGroup()) send the
        // leave heartbeat (ignoreResponse=true — pollOnClose drops
        // the response by Java's `logResponse(...)` semantics).
        if this.membership_manager().is_leaving_group() {
            let request = Self::make_heartbeat_request(this, true);
            // Java parity: `pollOnClose` routes its leave heartbeat through
            // `makeHeartbeatRequest(currentTimeMs, true)`
            // (in `org.apache.kafka.clients.consumer.internals.AbstractHeartbeatRequestManager#pollOnClose`), which
            // records the heartbeat-sent time (`AbstractHeartbeatRequestManager#makeHeartbeatRequest(long,boolean)`).
            // This is the third of Java's three heartbeat send sites; record here so `last-heartbeat-seconds-ago`
            // reflects the close-path leave heartbeat, matching the poll-timer
            // leave path above and the normal path in `make_heartbeat_poll_result`.
            if let Some(metrics_manager) = this.base().metrics_manager.as_ref() {
                metrics_manager.record_heartbeat_sent_ms(current_time_ms);
            }
            return PollResult::new(this.base().heartbeat_request_state.heartbeat_interval_ms(), vec![request]);
        }
        PollResult::empty()
    }

    #[doc(alias = "org.apache.kafka.clients.consumer.internals.AbstractHeartbeatRequestManager#maximumTimeToWait")]
    pub(crate) fn maximum_time_to_wait<H: HeartbeatRequestManagerHooks<Response = R>>(
        this: &H,
        current_time_ms: i64,
    ) -> i64 {
        // When the member is UNSUBSCRIBED (for example, with manual assignment;
        // KAFKA-20426) or in the terminal FATAL state (KAFKA-21010), return
        // i64::MAX to indicate there is no next heartbeat to wait for —
        // allowing the application thread to block for the full user-specified
        // poll timeout rather than spinning in a busy loop.
        //
        // Deviation note (Critic 64, Observation 1): Java
        // `AbstractHeartbeatRequestManager.maximumTimeToWait` (:254 at 4.4)
        // calls `pollTimer.update(currentTimeMs)` FIRST, before the
        // UNSUBSCRIBED || FATAL short-circuit at :259. That `update` advances the Java `Timer`'s
        // *internal* clock so its no-arg `isExpired()` / `remainingMs()`
        // queries later in the method observe `currentTimeMs`. The Rust poll
        // timer holds only an absolute `poll_timer_expires_at_ms` and every
        // query (`poll_timer_is_expired` / `poll_timer_remaining_ms`) takes
        // `current_time_ms` as a parameter, so there is no mutable internal
        // clock to update — the update is folded into each query call. This
        // method also takes `&self`, so it cannot mutate timer state. Thus
        // the `pollTimer.update` step has no Rust counterpart on this path and
        // its omission is behavior-faithful.
        //
        // The membership state is read once, under one lock, for all three
        // predicates below: Java's mocked/`synchronized` reads are separate
        // calls, but nothing else runs on the background task between them.
        let (state, should_skip_heartbeat, should_heartbeat_now) = {
            let inner = this.membership_manager().abstract_mm().inner.lock();
            let guard = match inner {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            (guard.state, guard.should_skip_heartbeat(), guard.should_heartbeat_now())
        };
        // No heartbeat can be sent in these states: UNSUBSCRIBED has nothing to heartbeat for,
        // and FATAL is terminal. The fatal error has already been propagated to the
        // application thread, so there is no need to wake it before its poll timeout expires.
        if state == MemberState::Unsubscribed || state == MemberState::Fatal {
            return i64::MAX;
        }
        // Unblock the application thread so STALE/FENCED members can run
        // assignment-release callbacks and rejoin during the next poll.
        if this.base().poll_timer_is_expired(current_time_ms) {
            return 0;
        }
        // Mirror the guard in poll(). A heartbeat is only sent when the coordinator is known and the
        // member is in a state that can send heartbeats. This covers cases such as:
        // - The coordinator is unavailable (for example, during bootstrap DNS resolution or after a
        //   re-authentication failure).
        // - The member is FENCED (or STALE with the poll timer already reset) and waiting for the
        //   application thread to run assignment-release callbacks before rejoining.
        // Return retryBackoffMs rather than the heartbeat interval, since the interval remains 0 until
        // the first heartbeat response is received, which would also lead to busy-spinning.
        // (KAFKA-20253 added this branch returning the heartbeat interval; KAFKA-21010 changed it.)
        if this.base().coordinator_request_manager.coordinator().is_none() || should_skip_heartbeat {
            return this.base().heartbeat_request_state.retry_backoff_ms();
        }
        if should_heartbeat_now && !this.base().heartbeat_request_state.request_in_flight() {
            return 0;
        }
        let remaining_poll = this.base().poll_timer_remaining_ms(current_time_ms);
        let time_to_hb = this.base().heartbeat_request_state.time_to_next_heartbeat_ms(current_time_ms);
        std::cmp::min(remaining_poll / 2, time_to_hb)
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
    Fatal(Error),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::requests::ConsumerGroupHeartbeatResponse;
    use crate::consumer::ConsumerConfig;

    fn make_state(now: i64) -> AbstractHeartbeatRequestManager<ConsumerGroupHeartbeatResponse> {
        let config = ConsumerConfig {
            bootstrap_servers: vec!["localhost:9092".to_string()],
            ..ConsumerConfig::defaults()
        };
        let coord = Arc::new(CoordinatorRequestManager::new(100, 1_000, "g"));
        let (tx, _rx) = mpsc::unbounded_channel();
        let beh = Arc::new(BackgroundEventHandler::new(tx));
        AbstractHeartbeatRequestManager::new(now, &config, coord, beh)
    }

    /// Poll timer is NOT armed at construction (Issue 9 fix in
    /// `design/history/Milestone-8/Phase-13/COMMENTS.DONE.1.md`).
    /// `poll_timer_is_expired` returns `false` for any `current_time_ms`
    /// until the first [`AbstractHeartbeatRequestManager::reset_poll_timer`]
    /// call arms it.
    #[test]
    fn poll_timer_not_armed_at_construction() {
        let mgr = make_state(0);
        assert!(!mgr.poll_timer_is_expired(0));
        // Even with `current_time_ms` far beyond the default
        // `max.poll.interval.ms=300_000`, the timer is not expired
        // because it has not been armed.
        assert!(!mgr.poll_timer_is_expired(300_000));
        assert!(!mgr.poll_timer_is_expired(i64::MAX - 1));
    }

    /// First call to `reset_poll_timer` arms the timer at
    /// `current_time_ms + max_poll_interval_ms`.
    #[test]
    fn reset_poll_timer_arms_then_rearms() {
        let mut mgr = make_state(0);
        // Initially not expired (not armed).
        assert!(!mgr.poll_timer_is_expired(300_000));
        // First call arms it: expires at `100 + 300_000`.
        mgr.reset_poll_timer(100);
        assert!(!mgr.poll_timer_is_expired(100));
        assert!(!mgr.poll_timer_is_expired(300_099));
        assert!(mgr.poll_timer_is_expired(300_100));
        // Second call re-arms relative to the new "now".
        mgr.reset_poll_timer(300_000);
        assert!(!mgr.poll_timer_is_expired(300_000));
        assert!(mgr.poll_timer_is_expired(600_000));
    }

    /// `classify_response_error` for `NOT_COORDINATOR` resets heartbeat
    /// state so the next heartbeat is sent without backoff.
    #[test]
    fn not_coordinator_resets_heartbeat_state() {
        let mut mgr = make_state(0);
        let action = mgr.classify_response_error(
            "ConsumerGroupHeartbeatRequest",
            Errors::NotCoordinator,
            Some("x"),
            "test-group",
            100,
        );
        assert!(matches!(action, HeartbeatErrorAction::Handled));
        // After reset, no in-flight request and zero backoff.
        assert!(!mgr.heartbeat_request_state.request_in_flight());
    }

    /// `classify_response_error` for `GROUP_AUTHORIZATION_FAILED` is
    /// fatal.
    #[test]
    fn group_authorization_failed_is_fatal() {
        let mut mgr = make_state(0);
        let action = mgr.classify_response_error(
            "ConsumerGroupHeartbeatRequest",
            Errors::GroupAuthorizationFailed,
            Some("msg"),
            "test-group",
            0,
        );
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
        let action = mgr.classify_response_error(
            "ConsumerGroupHeartbeatRequest",
            Errors::FencedMemberEpoch,
            Some("fenced"),
            "test-group",
            0,
        );
        assert!(matches!(action, HeartbeatErrorAction::Fenced));
    }

    #[test]
    fn unknown_member_id_is_fenced() {
        let mut mgr = make_state(0);
        let action = mgr.classify_response_error(
            "ConsumerGroupHeartbeatRequest",
            Errors::UnknownMemberId,
            Some("unknown"),
            "test-group",
            0,
        );
        assert!(matches!(action, HeartbeatErrorAction::Fenced));
    }

    /// Other errors (e.g., `UnsupportedVersion`) defer to the specific
    /// subclass handler.
    #[test]
    fn unknown_error_defers_to_specific() {
        let mut mgr = make_state(0);
        let action = mgr.classify_response_error(
            "ConsumerGroupHeartbeatRequest",
            Errors::UnsupportedVersion,
            Some("msg"),
            "test-group",
            0,
        );
        assert!(matches!(action, HeartbeatErrorAction::DelegateToSpecific));
    }

    /// Successful response updates the heartbeat interval.
    #[test]
    fn successful_response_updates_interval() {
        let mut mgr = make_state(0);
        mgr.on_successful_response("member-id", 5_000, 0);
        assert_eq!(mgr.heartbeat_request_state.heartbeat_interval_ms(), 5_000);
    }

    /// `on_failure` for a retriable error tells the caller to retry.
    #[test]
    fn on_failure_retriable() {
        let mut mgr = make_state(0);
        let err = Error::new(Errors::NetworkError);
        assert!(err.is_retriable_error());
        let action = mgr.classify_failure("ConsumerGroupHeartbeatRequest", &err, 0);
        assert_eq!(action, HeartbeatFailureAction::Retriable);
    }

    /// Like [`make_state`] but keeps the background-event receiver so a test
    /// can inspect the `ErrorEvent` an arm emits.
    fn make_state_capturing_events(
        now: i64,
    ) -> (
        AbstractHeartbeatRequestManager<ConsumerGroupHeartbeatResponse>,
        mpsc::UnboundedReceiver<crate::consumer::internals::events::BackgroundEventEnvelope>,
    ) {
        let config = ConsumerConfig {
            bootstrap_servers: vec!["localhost:9092".to_string()],
            ..ConsumerConfig::defaults()
        };
        let coord = Arc::new(CoordinatorRequestManager::new(100, 1_000, "g"));
        let (tx, rx) = mpsc::unbounded_channel();
        let beh = Arc::new(BackgroundEventHandler::new(tx));
        (AbstractHeartbeatRequestManager::new(now, &config, coord, beh), rx)
    }

    /// `GROUP_AUTHORIZATION_FAILED` ignores the broker's `ErrorMessage` and
    /// uses `GroupAuthorizationException.forGroupId(groupId)`'s text
    /// (`AbstractHeartbeatRequestManager#onErrorResponse`), so the application
    /// learns WHICH group it lacks access to.
    #[test]
    fn group_authorization_failed_uses_for_group_id_message() {
        let mut mgr = make_state(0);
        let action = mgr.classify_response_error(
            "ConsumerGroupHeartbeatRequest",
            Errors::GroupAuthorizationFailed,
            Some("broker text that Java does not use here"),
            "my-group",
            0,
        );
        match action {
            HeartbeatErrorAction::Fatal(err) => {
                assert_eq!(err.error(), Errors::GroupAuthorizationFailed);
                assert_eq!("Not authorized to access group: my-group", err.message());
            },
            other => panic!("expected fatal, got {other:?}"),
        }
    }

    /// `TOPIC_AUTHORIZATION_FAILED` calls `error.exception()` with NO argument
    /// (`AbstractHeartbeatRequestManager#onErrorResponse`), i.e. the code's default
    /// message — even though `errorMessage` is in scope. The asymmetry with the
    /// `INVALID_REQUEST` family below is deliberate in Java.
    #[test]
    fn topic_authorization_failed_uses_the_default_message() {
        let (mut mgr, mut rx) = make_state_capturing_events(0);
        let action = mgr.classify_response_error(
            "ConsumerGroupHeartbeatRequest",
            Errors::TopicAuthorizationFailed,
            Some("broker text that Java does not use here"),
            "my-group",
            0,
        );
        assert!(matches!(action, HeartbeatErrorAction::Handled));
        let envelope = rx.try_recv().expect("an ErrorEvent is enqueued");
        match envelope.event {
            BackgroundEvent::Error { error } => {
                assert_eq!(error.error(), Errors::TopicAuthorizationFailed);
                assert_eq!("Topic authorization failed.", error.message());
            },
            other => panic!("expected an Error event, got {other:?}"),
        }
    }

    /// `INVALID_REQUEST` / `GROUP_MAX_SIZE_REACHED` / `UNSUPPORTED_ASSIGNOR`
    /// DO carry the broker's `ErrorMessage`
    /// (`AbstractHeartbeatRequestManager#onErrorResponse`,
    /// `error.exception(errorMessage)`). For `UNSUPPORTED_ASSIGNOR` it is the
    /// only way to learn which assignor the broker rejected.
    #[test]
    fn invalid_request_family_carries_the_broker_error_message() {
        for error in [
            Errors::InvalidRequest,
            Errors::GroupMaxSizeReached,
            Errors::UnsupportedAssignor,
        ] {
            let mut mgr = make_state(0);
            let action = mgr.classify_response_error(
                "ConsumerGroupHeartbeatRequest",
                error,
                Some("server-side detail"),
                "my-group",
                0,
            );
            match action {
                HeartbeatErrorAction::Fatal(err) => {
                    assert_eq!(err.error(), error);
                    assert_eq!("server-side detail", err.message(), "for {error:?}");
                },
                other => panic!("expected fatal for {error:?}, got {other:?}"),
            }
        }
    }

    /// A NULL broker `ErrorMessage` falls back to the code's own default
    /// message, mirroring `Errors.exception(String)`
    /// — not to the literal text `"None"`.
    #[test]
    fn invalid_request_family_falls_back_to_the_default_message() {
        let mut mgr = make_state(0);
        let action = mgr.classify_response_error(
            "ConsumerGroupHeartbeatRequest",
            Errors::UnsupportedAssignor,
            None,
            "my-group",
            0,
        );
        match action {
            HeartbeatErrorAction::Fatal(err) => {
                assert_eq!(err.error(), Errors::UnsupportedAssignor);
                assert_eq!(Errors::UnsupportedAssignor.message(), err.message());
            },
            other => panic!("expected fatal, got {other:?}"),
        }
    }

    /// `INVALID_REGULAR_EXPRESSION` concatenates the broker's text onto Java's
    /// own prefix (`AbstractHeartbeatRequestManager#onErrorResponse`). The broker
    /// text is the only actionable part — it says why the regex did not
    /// compile.
    #[test]
    fn invalid_regular_expression_appends_the_broker_error_message() {
        let mut mgr = make_state(0);
        let action = mgr.classify_response_error(
            "ConsumerGroupHeartbeatRequest",
            Errors::InvalidRegularExpression,
            Some("regex 'foo[' failed to compile: missing ]"),
            "my-group",
            0,
        );
        match action {
            HeartbeatErrorAction::Fatal(err) => {
                assert_eq!(
                    "Invalid RE2J SubscriptionPattern provided in the call to subscribe. \
                     regex 'foo[' failed to compile: missing ]",
                    err.message()
                );
            },
            other => panic!("expected fatal, got {other:?}"),
        }
    }

    /// `NOT_COORDINATOR` / `COORDINATOR_NOT_AVAILABLE` pass the broker's text
    /// to `markCoordinatorUnknown(errorMessage, ...)`
    /// (`AbstractHeartbeatRequestManager#onErrorResponse`), which logs it as
    /// the rediscovery cause. A null message must not become the literal
    /// `"None"`.
    #[test]
    fn not_coordinator_accepts_a_null_error_message() {
        let mut mgr = make_state(0);
        let action =
            mgr.classify_response_error("ConsumerGroupHeartbeatRequest", Errors::NotCoordinator, None, "my-group", 100);
        assert!(matches!(action, HeartbeatErrorAction::Handled));
    }
}
