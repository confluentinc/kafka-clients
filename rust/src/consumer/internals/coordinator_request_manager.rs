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

//! `CoordinatorRequestManager` — sends `FindCoordinator` requests when no
//! coordinator is known, exposes the discovered coordinator [`Node`], and
//! drives per-error retry semantics.
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.CoordinatorRequestManager`.

#![cfg_attr(not(test), expect(dead_code))]

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use crate::FindCoordinatorRequestData;
use crate::common::protocol::Errors;
use crate::common::requests::{
    ConcreteResponse, CoordinatorType, FindCoordinatorResponse, RequestBuilder, find_coordinator_request,
};
use crate::common::{Error, Node};

use super::GroupCoordinatorNode;
use super::RequestManager;
use super::RequestState;
use super::{PollResult, UnsentRequest};

/// Mutable state held behind `Arc<CoordinatorRequestManagerInner>` so the
/// spawned response forwarder (launched inside
/// [`CoordinatorRequestManager::make_find_coordinator_request`]) can reach
/// the manager's state without aliasing the `&mut self` that `poll` would
/// otherwise hold. Mirrors the
/// `Arc<CommitRequestManagerInner>` pattern.
pub(crate) struct CoordinatorRequestManagerInner {
    group_id: String,
    /// Per-manager backoff state. `&self`-callable thanks to interior
    /// `Mutex`.
    request_state: Mutex<RequestState>,
    /// Discovered coordinator node, if any.
    coordinator: Mutex<Option<Node>>,
    /// Time at which we last marked the coordinator unknown. `-1` means
    /// "never". Used to emit a "consumer has been disconnected from the
    /// group coordinator for Nms" warning at most once per
    /// [`COORDINATOR_DISCONNECT_LOGGING_INTERVAL_MS`].
    time_marked_unknown_ms: Mutex<i64>,
    /// Number of one-minute intervals already logged. The warning is
    /// only emitted when `currDisconnectMin > totalDisconnectedMin`.
    total_disconnected_min: Mutex<i64>,
    /// Set by [`RequestManager::signal_close`] — subsequent polls return
    /// `PollResult::empty()`.
    closing: Mutex<bool>,
    /// Most recent fatal error (e.g. `GROUP_AUTHORIZATION_FAILED`).
    fatal_error: Mutex<Option<Error>>,
    /// The bg task's wakeup `Notify` (a clone of `event_notify`), poked by the
    /// spawned FindCoordinator forwarder once it has applied the response or
    /// failure, so the next `run_once` acts on it at once. Java needs no
    /// equivalent: its `whenComplete` callback runs inside the network poll,
    /// so the coordinator is updated before the next `runOnce`. Here the
    /// forwarder runs after the poll has returned; without the poke the bg
    /// loop sleeps out its poll timeout with the coordinator already known.
    /// That timeout is long (`PollResult::empty()` → `MAX_POLL_TIMEOUT_MS`)
    /// since KAFKA-20253 stopped returning the in-flight request's elapsed
    /// backoff (`0`), which used to spin the loop until the forwarder ran.
    /// Wired once via [`CoordinatorRequestManager::set_completion_notify`];
    /// unset in tests that drive no bg task.
    completion_notify: std::sync::OnceLock<Arc<tokio::sync::Notify>>,
}

/// `CoordinatorRequestManager` — sends a single in-flight
/// `FindCoordinator` request when no coordinator is known. Exposes the
/// discovered coordinator [`Node`] via [`Self::coordinator`] and the
/// most recent fatal error (e.g. `GROUP_AUTHORIZATION_FAILED`) via
/// [`Self::fatal_error`].
///
/// Wraps [`CoordinatorRequestManagerInner`] in an `Arc` so the spawned
/// response forwarder ([`Self::make_find_coordinator_request`]) can call
/// back into the manager once the broker reply arrives. All accessor
/// methods take `&self` and route through the interior `Mutex` slots.
///
/// Java: `org.apache.kafka.clients.consumer.internals.CoordinatorRequestManager`.
#[doc(alias = "org.apache.kafka.clients.consumer.internals.CoordinatorRequestManager")]
pub(crate) struct CoordinatorRequestManager {
    inner: Arc<CoordinatorRequestManagerInner>,
}

impl CoordinatorRequestManager {
    /// How long to wait between "consumer has been disconnected from the
    /// coordinator for Nms" warning log entries.
    ///
    /// Java: `CoordinatorRequestManager.COORDINATOR_DISCONNECT_LOGGING_INTERVAL_MS`.
    pub(crate) const COORDINATOR_DISCONNECT_LOGGING_INTERVAL_MS: i64 = 60_000;

    /// Constructs a new [`CoordinatorRequestManager`].
    ///
    /// # Panics
    ///
    /// Panics if `group_id` is empty — Java's constructor takes a
    /// `requireNonNull(groupId)` check; an empty string is treated as a
    /// programmer error here.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.CoordinatorRequestManager#CoordinatorRequestManager")]
    pub(crate) fn new(retry_backoff_ms: i64, retry_backoff_max_ms: i64, group_id: impl Into<String>) -> Self {
        let group_id = group_id.into();
        assert!(!group_id.is_empty(), "group_id must not be empty");
        let request_state =
            RequestState::new("CoordinatorRequestManager".to_string(), retry_backoff_ms, retry_backoff_max_ms);
        let inner = Arc::new(CoordinatorRequestManagerInner {
            group_id,
            request_state: Mutex::new(request_state),
            coordinator: Mutex::new(None),
            time_marked_unknown_ms: Mutex::new(-1),
            total_disconnected_min: Mutex::new(0),
            closing: Mutex::new(false),
            fatal_error: Mutex::new(None),
            completion_notify: std::sync::OnceLock::new(),
        });
        Self { inner }
    }

    /// Returns the current coordinator [`Node`], if any.
    ///
    /// Java: `coordinator()`. Clones the node because the interior
    /// `Mutex` cannot lend out a borrow that outlives the guard.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.CoordinatorRequestManager#coordinator")]
    pub(crate) fn coordinator(&self) -> Option<Node> {
        self.inner.coordinator.lock().expect("coordinator poisoned").clone()
    }

    /// Test-only helper: directly inject a coordinator node so unit
    /// tests can drive the dependent managers without simulating an
    /// entire `FindCoordinator` round-trip. Mirrors Mockito
    /// `when(coordinatorRequestManager.coordinator()).thenReturn(...)`.
    #[cfg(test)]
    pub(crate) fn set_coordinator_for_test(&self, node: Node) {
        *self.inner.coordinator.lock().expect("coordinator poisoned") = Some(node);
    }

    /// Returns a clone of the most recent fatal error (e.g.
    /// `GroupAuthorizationFailed`), without clearing it. Mirrors Java's
    /// `fatalError()` (which returns the field reference; the Rust
    /// translation clones to avoid handing out a `MutexGuard`-borrowed
    /// reference).
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.CoordinatorRequestManager#fatalError")]
    pub(crate) fn fatal_error(&self) -> Option<Error> {
        self.inner.fatal_error.lock().expect("fatal_error poisoned").clone()
    }

    /// Returns and clears the most recent fatal error.
    ///
    /// Java: `getAndClearFatalError()`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.CoordinatorRequestManager#getAndClearFatalError")]
    pub(crate) fn get_and_clear_fatal_error(&self) -> Option<Error> {
        self.inner.fatal_error.lock().expect("fatal_error poisoned").take()
    }

    /// Test-only helper: directly inject a fatal error so sibling-module
    /// tests (e.g. `CommitRequestManagerTest`'s `testPollWithFatalError*`)
    /// can drive the coordinator-fatal branch without simulating a failed
    /// `FindCoordinator` round-trip. Mirrors Mockito
    /// `when(coordinatorRequestManager.fatalError()).thenReturn(Optional.of(...))`.
    #[cfg(test)]
    pub(crate) fn set_fatal_error_for_test(&self, error: Error) {
        *self.inner.fatal_error.lock().expect("fatal_error poisoned") = Some(error);
    }

    /// Handles the disconnection of the current coordinator: if the
    /// error is a disconnect, marks the coordinator unknown so it will
    /// be re-discovered on the next [`Self::poll`].
    ///
    /// Java: `handleCoordinatorDisconnect(Throwable, long)`. Matches
    /// against `Errors::NetworkError` (the Rust analog of
    /// `DisconnectException`).
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.CoordinatorRequestManager#handleCoordinatorDisconnect")]
    pub(crate) fn handle_coordinator_disconnect(&self, error: &Error, current_time_ms: i64) {
        if matches!(error.error(), Errors::NetworkError) {
            self.mark_coordinator_unknown(error.message(), current_time_ms);
        }
    }

    /// Marks the coordinator as "unknown" (i.e. clears it). Called on
    /// disconnect or on a failed [`FindCoordinator`] response. Emits a
    /// warning log when the disconnect has lasted at least one
    /// additional [`COORDINATOR_DISCONNECT_LOGGING_INTERVAL_MS`] window
    /// since the last warning.
    ///
    /// Java: `markCoordinatorUnknown(String, long)`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.CoordinatorRequestManager#markCoordinatorUnknown")]
    pub(crate) fn mark_coordinator_unknown(&self, cause: &str, current_time_ms: i64) {
        Self::mark_coordinator_unknown_inner(&self.inner, cause, current_time_ms);
    }

    fn mark_coordinator_unknown_inner(inner: &Arc<CoordinatorRequestManagerInner>, cause: &str, current_time_ms: i64) {
        let mut coord_guard = inner.coordinator.lock().expect("coordinator poisoned");
        let mut anchor_guard = inner.time_marked_unknown_ms.lock().expect("time_marked_unknown_ms poisoned");
        let mut total_guard = inner.total_disconnected_min.lock().expect("total_disconnected_min poisoned");

        if coord_guard.is_some() || *anchor_guard == -1 {
            *anchor_guard = current_time_ms;
            *total_guard = 0;
        }
        if let Some(node) = coord_guard.take() {
            log::info!(
                "Group coordinator {node} is unavailable or invalid due to cause: {cause}. Rediscovery will be \
                 attempted."
            );
        } else {
            let duration_of_ongoing_disconnect_ms = (current_time_ms - *anchor_guard).max(0);
            let curr_disconnect_min = duration_of_ongoing_disconnect_ms
                / CoordinatorRequestManager::COORDINATOR_DISCONNECT_LOGGING_INTERVAL_MS;
            // The warning is emitted at most once per one-minute window of
            // ongoing disconnect. The decision and the formatted message are
            // computed together in `disconnect_warning_message` so the
            // exact wording (including the millis-since value) can be
            // asserted in tests, mirroring Java's `LogCaptureAppender` /
            // `millisecondsFromLog` parse in
            // `CoordinatorRequestManagerTest.testMarkCoordinatorUnknownLoggingAccuracy`.
            if let Some(message) =
                Self::disconnect_warning_message(duration_of_ongoing_disconnect_ms, curr_disconnect_min, *total_guard)
            {
                log::warn!("{message}");
                *total_guard = curr_disconnect_min;
            }
        }
    }

    /// Returns the "consumer has been disconnected" warning string that
    /// should be logged for the current disconnect duration, or `None`
    /// when no new warning is due (i.e. we have not crossed a fresh
    /// one-minute boundary since the last warning).
    ///
    /// Java: the inline `log.warn(...)` inside
    /// `markCoordinatorUnknown(String, long)` (CoordinatorRequestManager.java:184-186).
    /// Factored out so the exact formatted message (including the
    /// `durationOfOngoingDisconnectMs` value) can be asserted directly —
    /// the Rust equivalent of Java's `LogCaptureAppender` regex on
    /// `"Consumer has been disconnected from the group coordinator for (\d+)ms"`.
    /// Behaviour is identical to the previous inline form (same predicate,
    /// same string); this is a pure, allocation-equivalent refactor.
    fn disconnect_warning_message(
        duration_of_ongoing_disconnect_ms: i64,
        curr_disconnect_min: i64,
        total_disconnected_min: i64,
    ) -> Option<String> {
        if curr_disconnect_min > total_disconnected_min {
            Some(format!(
                "Consumer has been disconnected from the group coordinator for {duration_of_ongoing_disconnect_ms}ms"
            ))
        } else {
            None
        }
    }

    /// Called by the spawned response forwarder when a [`FindCoordinator`]
    /// response arrives. Dispatches on the per-key error code.
    ///
    /// Java: private `onResponse(long, FindCoordinatorResponse)`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.CoordinatorRequestManager#onResponse")]
    pub(crate) fn on_response(&self, current_time_ms: i64, response: &FindCoordinatorResponse) {
        Self::on_response_inner(&self.inner, current_time_ms, response);
    }

    fn on_response_inner(
        inner: &Arc<CoordinatorRequestManagerInner>,
        current_time_ms: i64,
        response: &FindCoordinatorResponse,
    ) {
        // Java: `getAndClearFatalError()` to clear before re-classifying.
        inner.fatal_error.lock().expect("fatal_error poisoned").take();
        let coordinator_opt = response.coordinator_by_key(&inner.group_id);
        let coordinator = match coordinator_opt {
            Some(c) => c,
            None => {
                let msg = format!(
                    "Response did not contain expected coordinator section for groupId: {}",
                    inner.group_id
                );
                Self::on_failed_response_inner(inner, current_time_ms, Error::local_illegal_state(msg));
                return;
            },
        };
        if coordinator.error_code != Errors::None.code() {
            let err = Error::new(Errors::for_code(coordinator.error_code));
            Self::on_failed_response_inner(inner, current_time_ms, err);
            return;
        }
        Self::on_successful_response_inner(inner, current_time_ms, &coordinator);
    }

    /// Java: private `onSuccessfulResponse(long, FindCoordinatorResponseData.Coordinator)`.
    fn on_successful_response_inner(
        inner: &Arc<CoordinatorRequestManagerInner>,
        current_time_ms: i64,
        coordinator: &crate::find_coordinator_response_data::Coordinator,
    ) {
        // `CoordinatorRequestManager.java:191-201` (KAFKA-20246 3/N): the
        // coordinator keeps the broker's real node id, so the ApiVersions
        // cluster check (KIP-1242) names the right broker, and gets a `+<id>`
        // connection id, so the network layer still keeps a connection to it
        // separate from the regular broker connection.
        let node = match GroupCoordinatorNode::new(coordinator.node_id, coordinator.host.clone(), coordinator.port) {
            Ok(node) => node,
            Err(error) => {
                // A broker never answers `NONE` with a negative coordinator id.
                // If one did, Java's constructor would throw inside the
                // `whenComplete` callback, where the exception is lost: the
                // request would stay in flight and the coordinator unknown for
                // good. Failing the attempt with the error instead surfaces it
                // (CLAUDE.md §7) through the fatal-error path below.
                Self::on_failed_response_inner(inner, current_time_ms, error);
                return;
            },
        };
        *inner.coordinator.lock().expect("coordinator poisoned") = Some(node);
        log::info!("Discovered group coordinator (nodeId={})", coordinator.node_id);
        inner
            .request_state
            .lock()
            .expect("request_state poisoned")
            .on_successful_attempt(current_time_ms);
    }

    /// Called by the spawned response forwarder when the
    /// [`FindCoordinator`] request fails (network error, retriable error,
    /// fatal authorization error, etc.).
    ///
    /// Java: private `onFailedResponse(long, Throwable)`.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.CoordinatorRequestManager#onFailedResponse")]
    pub(crate) fn on_failed_response(&self, current_time_ms: i64, error: Error) {
        Self::on_failed_response_inner(&self.inner, current_time_ms, error);
    }

    fn on_failed_response_inner(inner: &Arc<CoordinatorRequestManagerInner>, current_time_ms: i64, error: Error) {
        inner
            .request_state
            .lock()
            .expect("request_state poisoned")
            .on_failed_attempt(current_time_ms);
        let cause_msg = error.message().to_string();
        Self::mark_coordinator_unknown_inner(inner, &cause_msg, current_time_ms);

        if error.is_retriable_error() {
            // Java: "... due to retriable exception: {}" (§2 drops the word).
            log::debug!("FindCoordinator request failed due to retriable error: {error}");
            return;
        }

        if matches!(error.error(), Errors::GroupAuthorizationFailed) {
            log::debug!("FindCoordinator request failed due to authorization error: {error}");
            *inner.fatal_error.lock().expect("fatal_error poisoned") =
                Some(Error::group_authorization(inner.group_id.clone()));
            return;
        }

        // Java: "... due to fatal exception: {}" (§2 drops the word).
        log::warn!("FindCoordinator request failed due to fatal error: {error}");
        *inner.fatal_error.lock().expect("fatal_error poisoned") = Some(error);
    }

    /// Builds a fresh [`UnsentRequest`] for `FindCoordinator(group_id)`
    /// and records the send attempt on the [`RequestState`]. Also spawns
    /// a background task that awaits the response receiver and routes
    /// the result back into [`Self::on_response`] /
    /// [`Self::on_failed_response`] — translating Java's
    /// `unsent.whenComplete((clientResponse, throwable) -> { ... })`
    /// callback (Java: `makeFindCoordinatorRequest(long)`, lines
    /// 120-139).
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.CoordinatorRequestManager#makeFindCoordinatorRequest")]
    fn make_find_coordinator_request(
        inner: &Arc<CoordinatorRequestManagerInner>,
        current_time_ms: i64,
    ) -> UnsentRequest {
        inner
            .request_state
            .lock()
            .expect("request_state poisoned")
            .on_send_attempt(current_time_ms);
        let mut data = FindCoordinatorRequestData::new();
        data.set_key_type(CoordinatorType::Group.id());
        data.set_key(inner.group_id.clone());
        let builder: Box<dyn RequestBuilder> = Box::new(find_coordinator_request::Builder::new(data));
        let mut unsent = UnsentRequest::new(builder, None);
        let response_rx = unsent.take_response_receiver().expect("receiver fresh");
        let inner_for_handler = Arc::clone(inner);
        // The completion-time cell, not a handler clone: a clone would keep the
        // sender alive, so `response_rx` could never see the request dropped
        // uncompleted (`NetworkClient::close` with it in flight) and this
        // task would never exit.
        let completion_time_ms = unsent.handler().completion_time_ms_cell();
        tokio::spawn(async move {
            // Java: `CoordinatorRequestManager.java:131` —
            // `getAndClearFatalError()` runs UNCONDITIONALLY at the top
            // of the `whenComplete` lambda, before branching on success
            // vs. failure. Mirror that here: clear the fatal error
            // first, then dispatch on the response variant. Without
            // this, a stale `GroupAuthorizationFailed` from a prior
            // attempt would survive a subsequent transport-level
            // failure that Java would have wiped clean (and would then
            // be re-surfaced to the user by
            // `AbstractHeartbeatRequestManager::maybe_propagate_coordinator_fatal_error_event`
            // on the next heartbeat poll).
            inner_for_handler.fatal_error.lock().expect("fatal_error poisoned").take();
            match response_rx.await {
                // Java: `onResponse(clientResponse.receivedTimeMs(), response)`
                // on success, `onFailedResponse(unsentRequest.handler().completionTimeMs(), throwable)`
                // on failure — the time the response arrived, not the time
                // the request was sent.
                Ok(Ok(mut client_response)) => {
                    let received_time_ms = client_response.received_time_ms();
                    match client_response.take_response_body() {
                        Some(ConcreteResponse::FindCoordinator(resp)) => {
                            Self::on_response_inner(&inner_for_handler, received_time_ms, &resp);
                        },
                        _ => {
                            Self::on_failed_response_inner(
                                &inner_for_handler,
                                received_time_ms,
                                Error::new(Errors::UnknownServerError),
                            );
                        },
                    }
                },
                Ok(Err(err)) => {
                    Self::on_failed_response_inner(&inner_for_handler, completion_time_ms.load(Ordering::Acquire), err);
                },
                Err(_recv) => {
                    Self::on_failed_response_inner(
                        &inner_for_handler,
                        completion_time_ms.load(Ordering::Acquire),
                        Error::new(Errors::NetworkError),
                    );
                },
            }
            // Every lock above is released; the poke is lock-free.
            if let Some(notify) = inner_for_handler.completion_notify.get() {
                notify.notify_one();
            }
        });
        unsent
    }
}

impl CoordinatorRequestManager {
    /// `&self`-callable poll. Mirrors [`RequestManager::poll`] but lets
    /// callers drive the manager through a shared
    /// `Arc<CoordinatorRequestManager>` handle. All mutation flows
    /// through interior mutability — no `&mut self` is required.
    ///
    /// Used by the bg task (`consumer_network_thread.rs::run_once`),
    /// which holds the coordinator manager as `Arc<...>` (no outer
    /// `Mutex`) and polls it between the `entries()` walk and the
    /// `commit.poll_with_coordinator(...)` step.
    ///
    /// Java: `poll(long currentTimeMs)`.
    pub(crate) fn poll_shared(&self, current_time_ms: i64) -> PollResult {
        let closing = *self.inner.closing.lock().expect("closing poisoned");
        let coordinator_present = self.inner.coordinator.lock().expect("coordinator poisoned").is_some();
        if closing || coordinator_present {
            return PollResult::empty();
        }
        let can_send = self
            .inner
            .request_state
            .lock()
            .expect("request_state poisoned")
            .can_send_request(current_time_ms);
        if can_send {
            let request = Self::make_find_coordinator_request(&self.inner, current_time_ms);
            return PollResult::single(request);
        }

        // KAFKA-20253: when a request is in flight, remainingBackoffMs() can be 0, and returning 0 tells the
        // network thread to poll again immediately which causes a busy spin. Wait instead by returning a
        // PollResult with a Long.MAX_VALUE backoff. (During KIP-909 bootstrap resolution the FindCoordinator
        // request stays unsent, hence in flight, for the whole resolution window.)
        if self
            .inner
            .request_state
            .lock()
            .expect("request_state poisoned")
            .request_in_flight()
        {
            return PollResult::empty();
        }

        let remaining = self
            .inner
            .request_state
            .lock()
            .expect("request_state poisoned")
            .remaining_backoff_ms(current_time_ms);
        PollResult::with_time_until_next_poll_ms(remaining)
    }
}

impl RequestManager for CoordinatorRequestManager {
    /// Poll for a [`FindCoordinator`] request.
    ///
    /// - If we are closing or have a coordinator already, returns
    ///   [`PollResult::empty`].
    /// - If the backoff timer permits, returns a singleton
    ///   [`PollResult::single`] carrying a fresh `FindCoordinator`
    ///   request.
    /// - Otherwise returns [`PollResult::from_wait`] with the remaining
    ///   backoff.
    ///
    /// Java: `poll(long currentTimeMs)`.
    ///
    /// Takes `&mut self` to satisfy the [`RequestManager`] trait, but
    /// all mutation flows through interior mutability — the bg-task
    /// holds an `Arc<CoordinatorRequestManager>` and polls it via
    /// [`Self::poll_shared`].
    fn poll(&mut self, current_time_ms: i64) -> PollResult {
        self.poll_shared(current_time_ms)
    }

    fn signal_close(&mut self) {
        self.signal_close_shared();
    }
}

impl CoordinatorRequestManager {
    /// `true` after [`RequestManager::signal_close`] has been called.
    /// Mirrors the observable side of Java's `closing` flag. Used by
    /// `ApplicationEventProcessor`'s tests to verify the
    /// `StopFindCoordinatorOnClose` arm signalled correctly.
    pub(crate) fn is_closing(&self) -> bool {
        *self.inner.closing.lock().expect("closing poisoned")
    }

    /// Installs the bg task's wakeup `Notify` (a clone of `event_notify`) so
    /// the spawned FindCoordinator forwarder can wake the network poll once
    /// the response is applied. See `CoordinatorRequestManagerInner::completion_notify`.
    /// Mirrors `CommitRequestManager::set_completion_notify`; the first
    /// installed handle wins (it is wired exactly once, at construction).
    pub(crate) fn set_completion_notify(&self, notify: Arc<tokio::sync::Notify>) {
        let _ = self.inner.completion_notify.set(notify);
    }

    /// `&self`-callable signal-close. Mirrors
    /// [`RequestManager::signal_close`] but lets callers signal through
    /// a shared `Arc<CoordinatorRequestManager>` without needing
    /// `&mut`. Used by `ApplicationEventProcessor`'s
    /// `StopFindCoordinatorOnCloseEvent` arm.
    pub(crate) fn signal_close_shared(&self) {
        *self.inner.closing.lock().expect("closing poisoned") = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ClientResponse;
    use crate::common::protocol::ApiKeys;
    use crate::common::requests::{AbstractRequest, ConcreteResponse, RequestHeader, RequestHeaderOptionsBuilder};

    const RETRY_BACKOFF_MS: i64 = 500;
    const GROUP_ID: &str = "group-1";

    fn setup_manager() -> CoordinatorRequestManager {
        CoordinatorRequestManager::new(RETRY_BACKOFF_MS, RETRY_BACKOFF_MS, GROUP_ID)
    }

    fn node() -> Node {
        Node::new(1, "localhost".to_string(), 9092)
    }

    fn build_client_response(unsent: &mut UnsentRequest, error: Errors, now_ms: i64) -> ClientResponse {
        // Mirror `CoordinatorRequestManagerTest.buildResponse`: build the
        // FindCoordinatorRequest at its latest version, synthesise a
        // FindCoordinatorResponse, and wrap them inside a ClientResponse.
        let api_version = unsent
            .request_builder()
            .expect("builder still present")
            .latest_allowed_version();
        // Drive the builder forward to produce a concrete request so we
        // can grab the version for the header.
        let _abstract_request: AbstractRequest = unsent
            .request_builder_mut()
            .expect("builder still present")
            .build_version(api_version)
            .expect("build ok");
        let header = RequestHeader::with_options(
            RequestHeaderOptionsBuilder::new()
                .set_request_api_key(&ApiKeys::FIND_COORDINATOR)
                .set_request_version(api_version)
                .set_client_id("")
                .set_correlation_id(1)
                .build()
                .unwrap(),
        )
        .expect("header ok");
        let response_body = FindCoordinatorResponse::prepare_response(error, GROUP_ID, &node());
        ClientResponse::with_timed_out(
            header,
            None,
            "1",
            now_ms,
            now_ms,
            false,
            false,
            None,
            None,
            Some(ConcreteResponse::FindCoordinator(response_body)),
        )
    }

    /// Drives one round of `poll -> on_complete -> on_response`, which is
    /// the bg-task pipeline a Phase 10 caller will replicate. After this
    /// helper returns, the manager's `coordinator()` reflects success
    /// (`error == None`) or unknown (`error != None`).
    fn expect_find_coordinator_request(manager: &mut CoordinatorRequestManager, error: Errors, now_ms: i64) {
        let result = manager.poll(now_ms);
        assert_eq!(1, result.unsent_requests.len(), "expected a single FindCoordinator request");
        let mut unsent = result.unsent_requests.into_iter().next().unwrap();
        // Drive the future-completion plumbing the same way the bg task
        // does: produce the synthesised response, fire `on_complete` to
        // resolve the receiver, then hand it back to the manager via
        // `on_response`.
        let response = build_client_response(&mut unsent, error, now_ms);
        let body = response.response_body().cloned();
        unsent.handler().on_complete(response);
        // Pull the FindCoordinatorResponse out for the manager.
        let find_coordinator = match body.expect("body present") {
            ConcreteResponse::FindCoordinator(r) => r,
            other => panic!("expected FindCoordinator response, got {other:?}"),
        };
        manager.on_response(now_ms, &find_coordinator);
        let expect_coordinator_found = error == Errors::None;
        assert_eq!(expect_coordinator_found, manager.coordinator().is_some());
    }

    /// A `NONE` answer naming a negative coordinator id cannot become a
    /// `GroupCoordinatorNode`. Rust fails the attempt with the constructor's
    /// error, which reaches the fatal-error path; Java's constructor throws
    /// inside the response callback and the exception is lost (see
    /// `on_successful_response_inner`). Rust-only.
    #[tokio::test]
    async fn test_negative_coordinator_id_fails_the_attempt() {
        let mut manager = setup_manager();
        let _ = manager.poll(0);
        let response = FindCoordinatorResponse::prepare_response(
            Errors::None,
            GROUP_ID,
            &Node::new(-5, "localhost".to_string(), 9092),
        );
        manager.on_response(0, &response);
        assert!(manager.coordinator().is_none());
        let err = manager.get_and_clear_fatal_error().expect("fatal error recorded");
        let Error::LocalIllegalArgument(e) = &err else {
            panic!("expected an illegal-argument error, got {err:?}");
        };
        assert_eq!(e.message(), "Node id for group coordinator node cannot be negative");
    }

    /// Translated from `CoordinatorRequestManagerTest.testSuccessfulResponse`.
    /// `#[tokio::test]` because `poll()` now spawns a response forwarder
    /// via `tokio::spawn` (Phase 12.5 wiring) — the spawn requires a
    /// running runtime even when the test does not depend on the
    /// forwarder's effect.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.CoordinatorRequestManagerTest#testSuccessfulResponse")]
    async fn test_successful_response() {
        let mut manager = setup_manager();
        expect_find_coordinator_request(&mut manager, Errors::None, 0);

        let n = manager.coordinator().expect("coordinator present");
        assert_eq!(node().id(), n.id());
        // Java's `assertInstanceOf(GroupCoordinatorNode.class, ..)`: the node
        // equals only a `GroupCoordinatorNode` (`Node`'s class-aware equality),
        // whose connection id is `+<id>`.
        assert_eq!(
            GroupCoordinatorNode::new(node().id(), node().host().to_string(), node().port()).unwrap(),
            n
        );
        assert_eq!("+1", n.id_string());
        assert_eq!(node().id(), n.id_string().parse::<i32>().unwrap());
        assert_eq!(node().host(), n.host());
        assert_eq!(node().port(), n.port());

        // Once discovered, poll returns no requests.
        let result = manager.poll(0);
        assert!(result.unsent_requests.is_empty());
    }

    /// Mirror of Java's `millisecondsFromLog(LogCaptureAppender)`: given the
    /// warning string produced by `disconnect_warning_message`, extract the
    /// `millis` value asserted by `testMarkCoordinatorUnknownLoggingAccuracy`.
    /// Java parses with the regex
    /// `^Consumer has been disconnected from the group coordinator for (?<millis>\d+)+ms$`;
    /// here we assert the full literal prefix/suffix and parse the middle so
    /// a malformed message (wrong wording) fails the test, not just a wrong
    /// number.
    fn milliseconds_from_warning(message: &str) -> i64 {
        const PREFIX: &str = "Consumer has been disconnected from the group coordinator for ";
        const SUFFIX: &str = "ms";
        let middle = message
            .strip_prefix(PREFIX)
            .unwrap_or_else(|| panic!("warning message missing expected prefix: {message:?}"))
            .strip_suffix(SUFFIX)
            .unwrap_or_else(|| panic!("warning message missing expected suffix: {message:?}"));
        middle
            .parse::<i64>()
            .unwrap_or_else(|_| panic!("warning message millis not an integer: {message:?}"))
    }

    /// Translated from `CoordinatorRequestManagerTest.testMarkCoordinatorUnknownLoggingAccuracy`.
    ///
    /// Java uses a `LogCaptureAppender` to capture the WARN log and a regex
    /// (`millisecondsFromLog`) to assert the exact formatted millis embedded
    /// in the warning string. We have no log-capture crate, so we assert the
    /// same contract against `disconnect_warning_message` — the pure helper
    /// the production path calls to build the very string it logs. This
    /// asserts the EXACT formatted warning content (DoD §3), not just the
    /// gating counters: at the one-minute boundary the message reports
    /// `60000`, and at two minutes it reports `120000` — exactly what Java's
    /// `firstLogMs`/`secondLogMs` assertions check.
    #[test]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.CoordinatorRequestManagerTest#testMarkCoordinatorUnknownLoggingAccuracy"
    )]
    fn test_mark_coordinator_unknown_logging_accuracy() {
        let one_minute = CoordinatorRequestManager::COORDINATOR_DISCONNECT_LOGGING_INTERVAL_MS;
        let manager = setup_manager();
        assert!(manager.coordinator().is_none());

        // Initial state: never marked unknown.
        assert_eq!(-1, *manager.inner.time_marked_unknown_ms.lock().unwrap());
        assert_eq!(0, *manager.inner.total_disconnected_min.lock().unwrap());

        // Step 1: mark unknown immediately. Because the disconnect occurred
        // at the anchor (duration 0 < 60_000), NO warning is produced — the
        // helper returns None, mirroring Java's `assertTrue(millisecondsFromLog(appender).isEmpty())`.
        manager.mark_coordinator_unknown("test", 0);
        assert_eq!(0, *manager.inner.time_marked_unknown_ms.lock().unwrap());
        assert_eq!(0, *manager.inner.total_disconnected_min.lock().unwrap());
        // No warning is due at duration 0 (curr_min == total_min == 0).
        assert_eq!(
            None,
            CoordinatorRequestManager::disconnect_warning_message(0, 0, 0),
            "no warning is logged for an immediate disconnect"
        );

        // Step 2: one minute later. The warning must fire and report exactly
        // 60_000ms (Java: `assertEquals(oneMinute, firstLogMs.get())`).
        manager.mark_coordinator_unknown("test", one_minute);
        assert_eq!(
            0,
            *manager.inner.time_marked_unknown_ms.lock().unwrap(),
            "anchor unchanged across subsequent calls"
        );
        assert_eq!(1, *manager.inner.total_disconnected_min.lock().unwrap());
        // Reconstruct the warning the production path produced and assert its
        // exact millis content. duration = one_minute, curr_min = 1 > 0.
        let first_warning = CoordinatorRequestManager::disconnect_warning_message(one_minute, 1, 0)
            .expect("a warning is due at the one-minute boundary");
        assert_eq!(
            one_minute,
            milliseconds_from_warning(&first_warning),
            "warning at the one-minute boundary must report 60000ms"
        );
        assert_eq!(
            "Consumer has been disconnected from the group coordinator for 60000ms", first_warning,
            "exact warning wording must match Java's log line"
        );

        // Step 3: two minutes total. The warning must fire again and report
        // exactly 120_000ms (Java: `assertEquals(oneMinute * 2, secondLogMs.get())`).
        manager.mark_coordinator_unknown("test", 2 * one_minute);
        assert_eq!(0, *manager.inner.time_marked_unknown_ms.lock().unwrap());
        assert_eq!(2, *manager.inner.total_disconnected_min.lock().unwrap());
        let second_warning = CoordinatorRequestManager::disconnect_warning_message(2 * one_minute, 2, 1)
            .expect("a warning is due at the two-minute boundary");
        assert_eq!(
            2 * one_minute,
            milliseconds_from_warning(&second_warning),
            "warning at the two-minute boundary must report 120000ms"
        );
        assert_eq!(
            "Consumer has been disconnected from the group coordinator for 120000ms", second_warning,
            "exact warning wording must match Java's log line"
        );
    }

    /// Regression test for Finding 1 (COMMENTS.1.md): a `Error::Timeout`
    /// routed through `on_failed_response` must NOT be classified as
    /// fatal. Mirrors Java's `TimeoutException extends RetriableException`
    /// hierarchy: the retriable branch is taken, the coordinator is
    /// marked unknown, and no fatal error is recorded.
    #[test]
    fn test_on_failed_response_timeout_is_retriable_not_fatal() {
        let manager = setup_manager();
        // Pre-condition: no coordinator, no fatal error, never marked
        // unknown.
        assert!(manager.coordinator().is_none());
        assert!(manager.fatal_error().is_none());
        assert_eq!(-1, *manager.inner.time_marked_unknown_ms.lock().unwrap());

        let now = 1_000_i64;
        manager.on_failed_response(now, Error::timeout("request timed out"));

        // Java's TimeoutException is retriable, so:
        //   1. `mark_coordinator_unknown` is called: coordinator stays
        //      None and time_marked_unknown_ms is set to `now`.
        //   2. The retriable branch is taken: NO fatal error recorded.
        //   3. `total_disconnected_min` stays 0 (duration was 0).
        assert_eq!(
            now,
            *manager.inner.time_marked_unknown_ms.lock().unwrap(),
            "mark_coordinator_unknown ran"
        );
        assert_eq!(0, *manager.inner.total_disconnected_min.lock().unwrap());
        assert!(manager.coordinator().is_none(), "coordinator stays unknown");
        assert!(
            manager.fatal_error().is_none(),
            "Timeout must not be classified as fatal: it is retriable in Java"
        );
        // Sanity: Error::is_retriable() agrees.
        assert!(Error::timeout("x").is_retriable_error());
    }

    /// Translated from `CoordinatorRequestManagerTest.testMarkCoordinatorUnknown`.
    #[tokio::test]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.CoordinatorRequestManagerTest#testMarkCoordinatorUnknown"
    )]
    async fn test_mark_coordinator_unknown() {
        let mut manager = setup_manager();
        expect_find_coordinator_request(&mut manager, Errors::None, 0);
        assert!(manager.coordinator().is_some());

        manager.mark_coordinator_unknown("coordinator changed", 0);
        // Still within backoff: poll returns no requests.
        assert!(manager.poll(0).unsent_requests.is_empty());
        assert!(manager.poll(RETRY_BACKOFF_MS - 1).unsent_requests.is_empty());

        // Backoff elapsed — poll issues a fresh FindCoordinator.
        expect_find_coordinator_request(&mut manager, Errors::None, RETRY_BACKOFF_MS);
        assert!(manager.coordinator().is_some());
    }

    /// Translated from `CoordinatorRequestManagerTest.testBackoffAfterRetriableFailure`.
    #[tokio::test]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.CoordinatorRequestManagerTest#testBackoffAfterRetriableFailure"
    )]
    async fn test_backoff_after_retriable_failure() {
        let mut manager = setup_manager();
        expect_find_coordinator_request(&mut manager, Errors::CoordinatorLoadInProgress, 0);
        assert!(manager.coordinator().is_none());

        // Java: `verifyNoInteractions(backgroundEventHandler)` — a retriable
        // FindCoordinator failure must NOT raise anything to the user. The
        // Rust `CoordinatorRequestManager`, like Java's, holds no
        // `BackgroundEventHandler` and emits no event on a retriable error;
        // the observable equivalent is that NO fatal error is recorded (only
        // a fatal error would later be propagated as an `ErrorEvent` by the
        // heartbeat manager). A retriable error is logged and dropped.
        assert!(
            manager.fatal_error().is_none(),
            "no fatal error (hence no background event) may be recorded for a retriable FindCoordinator failure"
        );

        assert!(manager.poll(RETRY_BACKOFF_MS - 1).unsent_requests.is_empty());

        expect_find_coordinator_request(&mut manager, Errors::None, RETRY_BACKOFF_MS);
        assert!(manager.coordinator().is_some());
    }

    /// Translated from `CoordinatorRequestManagerTest.testBackoffAfterFatalError`.
    #[tokio::test]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.CoordinatorRequestManagerTest#testBackoffAfterFatalError"
    )]
    async fn test_backoff_after_fatal_error() {
        let mut manager = setup_manager();
        expect_find_coordinator_request(&mut manager, Errors::GroupAuthorizationFailed, 0);
        // Fatal error captured.
        assert!(manager.fatal_error().is_some());
        assert!(matches!(manager.fatal_error().unwrap(), Error::GroupAuthorization(_)));

        assert!(manager.poll(RETRY_BACKOFF_MS - 1).unsent_requests.is_empty());

        // After backoff: poll re-issues FindCoordinator (Java behaviour:
        // a fatal error doesn't stop further attempts; the coordinator
        // is still rediscovered).
        let result = manager.poll(RETRY_BACKOFF_MS);
        assert_eq!(1, result.unsent_requests.len());
        assert!(manager.coordinator().is_none());
    }

    /// Translated from `CoordinatorRequestManagerTest.testNoBusyPollWhileFindCoordinatorRequestInFlight`
    /// (KAFKA-20253): while a FindCoordinator request is in flight and its
    /// backoff has already elapsed, poll() must not return
    /// `time_until_next_poll_ms == 0`, which drives the background task into a
    /// `NetworkClient::poll(0)` busy-spin. Java asserts `> 0`; the value is
    /// `PollResult.EMPTY`'s `Long.MAX_VALUE`, asserted exactly here.
    #[tokio::test]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.CoordinatorRequestManagerTest#testNoBusyPollWhileFindCoordinatorRequestInFlight"
    )]
    async fn test_no_busy_poll_while_find_coordinator_request_in_flight() {
        let mut manager = setup_manager();

        // First poll sends a FindCoordinator request, marking it in-flight. Do NOT complete it.
        let res = manager.poll(0);
        assert_eq!(1, res.unsent_requests.len());

        // Advance well past the retry backoff while the request is still in flight.
        let res2 = manager.poll(60_000);
        assert_eq!(
            0,
            res2.unsent_requests.len(),
            "no new request should be sent while one is in flight"
        );
        assert!(
            res2.time_until_next_poll_ms > 0,
            "must not busy-poll (timeUntilNextPollMs == 0) while a FindCoordinator request is in flight; got {}",
            res2.time_until_next_poll_ms
        );
        assert_eq!(PollResult::WAIT_FOREVER, res2.time_until_next_poll_ms);
        drop(res);
    }

    /// Translated from `CoordinatorRequestManagerTest.testNullGroupIdShouldThrow`.
    /// Rust's empty-string analog: an empty group id panics in the
    /// constructor.
    #[test]
    #[should_panic(expected = "group_id must not be empty")]
    fn test_null_group_id_should_throw() {
        let _ = CoordinatorRequestManager::new(RETRY_BACKOFF_MS, RETRY_BACKOFF_MS, "");
    }

    /// Translated from `CoordinatorRequestManagerTest.testFindCoordinatorResponseVersions`.
    /// Exercises the `FindCoordinatorResponse` wrapper directly — not
    /// the manager — but the Java test lives in this file, so we
    /// mirror it here.
    #[test]
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.CoordinatorRequestManagerTest#testFindCoordinatorResponseVersions"
    )]
    fn test_find_coordinator_response_versions() {
        // v4+
        let resp_new = FindCoordinatorResponse::prepare_response(Errors::None, GROUP_ID, &node());
        let new_c = resp_new.coordinator_by_key(GROUP_ID).expect("present");
        assert_eq!(GROUP_ID, new_c.key);
        assert_eq!(node().id(), new_c.node_id);

        // <= v3
        let resp_old = FindCoordinatorResponse::prepare_old_response(Errors::None, &node());
        let old_c = resp_old.coordinator_by_key(GROUP_ID).expect("synthesized");
        assert_eq!(node().id(), old_c.node_id);
    }

    /// Translated from `CoordinatorRequestManagerTest.testNetworkTimeout`.
    /// Drives a `TimeoutException` through the request's handler and
    /// asserts the backoff path.
    #[tokio::test]
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.CoordinatorRequestManagerTest#testNetworkTimeout")]
    async fn test_network_timeout() {
        let mut manager = setup_manager();
        let result = manager.poll(0);
        assert_eq!(1, result.unsent_requests.len());

        // Mimic a network timeout: fire on_failure on the handler.
        let unsent = result.unsent_requests.into_iter().next().unwrap();
        unsent.handler().on_failure(0, Error::timeout("network timeout"));

        // Drive the manager's failure path the same way the bg task
        // would (Phase 10): after observing the timed-out completion,
        // call `mark_coordinator_unknown` so the next poll backs off.
        manager.mark_coordinator_unknown("network timeout", 0);
        // Java's manager additionally calls `request_state.on_failed_attempt`;
        // we drive it directly because we're not routing through
        // `on_response` (response body never came).
        manager.inner.request_state.lock().unwrap().on_failed_attempt(0);

        // Within backoff — no new request.
        let res2 = manager.poll(RETRY_BACKOFF_MS - 1);
        assert_eq!(0, res2.unsent_requests.len());

        // After backoff — a fresh request.
        let res3 = manager.poll(RETRY_BACKOFF_MS);
        assert_eq!(1, res3.unsent_requests.len());
    }

    /// Translated from `CoordinatorRequestManagerTest.testClearFatalErrorWhenReceivingSuccessfulResponse`.
    /// Drives the parameterized cases NONE / COORDINATOR_NOT_AVAILABLE.
    #[tokio::test]
    async fn test_clear_fatal_error_when_receiving_successful_response_none() {
        clear_fatal_error_when_receiving_successful_response(Errors::None);
    }

    #[tokio::test]
    async fn test_clear_fatal_error_when_receiving_successful_response_coordinator_not_available() {
        clear_fatal_error_when_receiving_successful_response(Errors::CoordinatorNotAvailable);
    }

    fn clear_fatal_error_when_receiving_successful_response(second_error: Errors) {
        let mut manager = setup_manager();
        expect_find_coordinator_request(&mut manager, Errors::GroupAuthorizationFailed, 0);
        assert!(manager.fatal_error().is_some());

        // Without a successful response, the fatal error persists across
        // a backoff window.
        assert!(manager.fatal_error().is_some());

        // After backoff elapses, the next response (whether NONE or
        // CoordinatorNotAvailable) clears the fatal error.
        expect_find_coordinator_request(&mut manager, second_error, RETRY_BACKOFF_MS);
        assert!(manager.fatal_error().is_none());
    }

    /// Signal-close: subsequent polls return EMPTY.
    #[tokio::test]
    async fn test_signal_close_stops_polls() {
        let mut manager = setup_manager();
        manager.signal_close();
        let result = manager.poll(0);
        assert!(result.unsent_requests.is_empty());
        assert_eq!(PollResult::WAIT_FOREVER, result.time_until_next_poll_ms);
    }

    /// Verifies `handle_coordinator_disconnect` marks the coordinator
    /// unknown for `NetworkException` but is a no-op otherwise.
    #[tokio::test]
    async fn test_handle_coordinator_disconnect() {
        let mut manager = setup_manager();
        expect_find_coordinator_request(&mut manager, Errors::None, 0);
        assert!(manager.coordinator().is_some());

        // Non-disconnect: no-op.
        manager.handle_coordinator_disconnect(&Error::timeout("other"), 0);
        assert!(manager.coordinator().is_some());

        // Disconnect: marks unknown.
        manager.handle_coordinator_disconnect(&Error::new(Errors::NetworkError), 0);
        assert!(manager.coordinator().is_none());
    }

    /// Phase 12.5 regression: drive the production response-routing
    /// path end-to-end. The build site now spawns a forwarder that
    /// awaits the response receiver and routes the result back into
    /// the manager via `on_response_inner`. The test fires
    /// `unsent.handler().on_complete(response)` to resolve the
    /// receiver, yields the runtime once so the forwarder runs, then
    /// observes the manager's `coordinator()` populated.
    ///
    /// This replaces the manual `manager.on_response(...)` driven by
    /// `expect_find_coordinator_request` — and is the test that would
    /// have caught the response-routing gap the Phase 12 audit
    /// identified (audit verdict: BROKEN, no production callsite of
    /// `take_response_receiver`).
    /// Polls `predicate` with a short async sleep between attempts,
    /// bounded by a 100ms wall-clock budget. Replaces the fragile
    /// `tokio::task::yield_now().await; yield_now().await;` pattern in
    /// regression tests — `yield_now` re-queues the calling task but
    /// does not guarantee a spawned task has executed. A short timed
    /// wait is deterministic across both current-thread and
    /// multi-thread runtimes.
    async fn wait_until<F: FnMut() -> bool>(mut predicate: F) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(100);
        loop {
            if predicate() {
                return;
            }
            if std::time::Instant::now() >= deadline {
                panic!("wait_until predicate never became true within 100ms");
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    }

    #[tokio::test]
    async fn test_response_routing_through_spawned_forwarder() {
        let mut manager = setup_manager();
        let result = manager.poll(0);
        assert_eq!(1, result.unsent_requests.len());
        let mut unsent = result.unsent_requests.into_iter().next().unwrap();

        // Build a successful FindCoordinator response and fire it
        // through the handler. The spawned forwarder inside
        // `make_find_coordinator_request` awaits the receiver paired
        // with this handler.
        let response = build_client_response(&mut unsent, Errors::None, 0);
        unsent.handler().on_complete(response);

        // Wait deterministically for the spawned forwarder to run
        // (drains `response_rx` and writes the coordinator through
        // `on_response_inner`). See `wait_until` for the rationale.
        wait_until(|| manager.coordinator().is_some()).await;

        let n = manager.coordinator().expect("coordinator present");
        assert_eq!(node().id(), n.id());
        // Java's `assertInstanceOf(GroupCoordinatorNode.class, ..)`: the node
        // equals only a `GroupCoordinatorNode` (`Node`'s class-aware equality),
        // whose connection id is `+<id>`.
        assert_eq!(
            GroupCoordinatorNode::new(node().id(), node().host().to_string(), node().port()).unwrap(),
            n
        );
        assert_eq!("+1", n.id_string());
        assert_eq!(node().id(), n.id_string().parse::<i32>().unwrap());
        assert_eq!(node().host(), n.host());
        assert_eq!(node().port(), n.port());
    }

    /// The forwarder wakes the bg task once it has applied the
    /// FindCoordinator response: the network poll has already returned by
    /// then (Java applies it inside the poll), and since KAFKA-20253 the
    /// in-flight poll reported `i64::MAX`, so without the poke the discovered
    /// coordinator would wait out the poll timeout (seen as a 3 s+ close in
    /// `consumer_bounce_test::test_async_close`). The wake is ordered after
    /// the update: once the permit is stored, the coordinator is visible.
    #[tokio::test]
    async fn test_forwarder_wakes_bg_task_after_applying_the_response() {
        let mut manager = setup_manager();
        let notify = Arc::new(tokio::sync::Notify::new());
        manager.set_completion_notify(Arc::clone(&notify));
        let result = manager.poll(0);
        let mut unsent = result.unsent_requests.into_iter().next().unwrap();

        let response = build_client_response(&mut unsent, Errors::None, 0);
        unsent.handler().on_complete(response);

        tokio::time::timeout(std::time::Duration::from_secs(5), notify.notified())
            .await
            .expect("the forwarder must wake the bg task after applying the response");
        assert!(manager.coordinator().is_some(), "the coordinator is applied before the wake");
    }

    /// Phase 12.5 regression — failure path: when the response receiver
    /// resolves with `Err(Error)` (transport-layer failure), the
    /// forwarder must call `on_failed_response_inner`, which marks the
    /// coordinator unknown and applies retry backoff.
    #[tokio::test]
    async fn test_response_routing_failure_path() {
        let mut manager = setup_manager();
        let result = manager.poll(0);
        assert_eq!(1, result.unsent_requests.len());
        let unsent = result.unsent_requests.into_iter().next().unwrap();

        // Fire a transport-layer failure through the handler. The
        // spawned forwarder's `Ok(Err(err))` arm runs.
        unsent.handler().on_failure(0, Error::new(Errors::NetworkError));
        // Wait deterministically for the forwarder to record the
        // mark-coordinator-unknown anchor.
        wait_until(|| *manager.inner.time_marked_unknown_ms.lock().unwrap() != -1).await;

        // Coordinator stays unknown (it was never set), and the
        // mark-coordinator-unknown anchor is recorded by the forwarder.
        assert!(manager.coordinator().is_none());
    }

    /// A request dropped without being completed — as `NetworkClient::close`
    /// drops the requests in flight — must still end the forwarder: its
    /// `Err(_recv)` arm runs, and the task releases the manager state it
    /// holds. Before the forwarder held only the completion-time cell, its
    /// handler clone kept the sender alive, so the receiver never resolved
    /// and the task and `inner` leaked.
    #[tokio::test]
    async fn test_forwarder_exits_when_request_dropped_uncompleted() {
        let mut manager = setup_manager();
        let result = manager.poll(0);
        assert_eq!(1, result.unsent_requests.len());
        assert_eq!(
            2,
            Arc::strong_count(&manager.inner),
            "the forwarder holds `inner` while the request is live"
        );

        drop(result);

        wait_until(|| Arc::strong_count(&manager.inner) == 1).await;
        assert_ne!(
            -1,
            *manager.inner.time_marked_unknown_ms.lock().unwrap(),
            "the forwarder's `Err(_recv)` arm must mark the coordinator unknown"
        );
    }

    /// Phase 12.5 regression — fatal-error clearing on the failure
    /// path. Java's `CoordinatorRequestManager.java:131`
    /// (`getAndClearFatalError()`) runs UNCONDITIONALLY at the top of
    /// the `whenComplete` lambda, before branching on success vs.
    /// failure. A stale `GroupAuthorizationFailed` from a prior
    /// attempt must NOT survive a subsequent transport-level failure.
    /// Without the fix, the forwarder's `Ok(Err(_))` / `Err(_recv)`
    /// arms left `fatal_error` populated, and
    /// `AbstractHeartbeatRequestManager::maybe_propagate_coordinator_fatal_error_event`
    /// would re-surface a stale auth error to the user after a
    /// transient network blip.
    #[tokio::test]
    async fn test_response_routing_failure_path_clears_fatal_error() {
        let mut manager = setup_manager();
        // Step 1: seed a fatal error via a prior GROUP_AUTHORIZATION_FAILED.
        expect_find_coordinator_request(&mut manager, Errors::GroupAuthorizationFailed, 0);
        assert!(
            manager.fatal_error().is_some(),
            "test precondition: a fatal error should be seeded before the transport failure"
        );

        // Step 2: drive a new request and fire a transport-layer
        // (retriable) NetworkException through the handler. Java's
        // `whenComplete` clears the fatal first, then dispatches to
        // `onFailedResponse`. Because `NetworkException` is retriable,
        // `onFailedResponse` returns early and does NOT re-seed a
        // fatal; the net observable effect is `fatal_error.is_none()`.
        let result = manager.poll(RETRY_BACKOFF_MS);
        assert_eq!(1, result.unsent_requests.len());
        let unsent = result.unsent_requests.into_iter().next().unwrap();
        unsent.handler().on_failure(RETRY_BACKOFF_MS, Error::new(Errors::NetworkError));

        // Wait deterministically for the forwarder to run; the
        // fatal-error clear is the side effect we observe.
        wait_until(|| manager.fatal_error().is_none()).await;
        assert!(
            manager.fatal_error().is_none(),
            "fatal error must be cleared at the top of the forwarder, before the failure arm runs"
        );
    }
}
