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

//! `RequestState` — per-manager exponential-backoff bookkeeping.
//!
//! Translated from `org.apache.kafka.clients.consumer.internals.RequestState`.

// Items here are consumed by Phase 6's `CoordinatorRequestManager` and by
// later phases (`CommitRequestManager`, `*HeartbeatRequestManager`,
// `OffsetsRequestManager`, ...). Suppress dead-code warnings for items
// that are not yet referenced inside the workspace.
#![allow(dead_code)]

use std::fmt;

use crate::common::utils::ExponentialBackoff;

/// Per-manager exponential-backoff bookkeeping shared across all
/// `RequestManager` implementations.
///
/// Mirrors Java's `RequestState` (package-private). Tracks the last send /
/// receive timestamps, the number of consecutive failed attempts, the
/// computed backoff for the next attempt, and whether an in-flight request
/// has been issued.
///
/// Java translates as follows:
///
/// - `protected final ExponentialBackoff exponentialBackoff` →
///   [`Self::exponential_backoff`]
/// - `protected long lastSentMs` → [`Self::last_sent_ms`]
/// - `protected long lastReceivedMs` → [`Self::last_received_ms`]
/// - `protected int numAttempts` → [`Self::num_attempts`]
/// - `protected long backoffMs` → [`Self::backoff_ms`]
/// - `private boolean requestInFlight` → [`Self::request_in_flight`]
/// - `protected final String owner` → [`Self::owner`] (used only for
///   diagnostic `Display`)
///
/// Java's `Logger` is omitted — the Rust translation uses `log::*` macros
/// directly inside [`RequestState::can_send_request`] when a backoff
/// remains.
pub(crate) struct RequestState {
    owner: String,
    exponential_backoff: ExponentialBackoff,
    last_sent_ms: i64,
    last_received_ms: i64,
    num_attempts: i32,
    backoff_ms: i64,
    request_in_flight: bool,
}

impl RequestState {
    /// Default exponent base for retry backoff.
    ///
    /// Java: `RequestState.RETRY_BACKOFF_EXP_BASE`.
    pub(crate) const RETRY_BACKOFF_EXP_BASE: i32 = 2;

    /// Default jitter factor for retry backoff.
    ///
    /// Java: `RequestState.RETRY_BACKOFF_JITTER`.
    pub(crate) const RETRY_BACKOFF_JITTER: f64 = 0.2;

    /// Creates a new [`RequestState`] using the default exponent base
    /// ([`RETRY_BACKOFF_EXP_BASE`]) and jitter ([`RETRY_BACKOFF_JITTER`]).
    ///
    /// Mirrors Java's public constructor
    /// `RequestState(LogContext, String, long, long)`.
    pub(crate) fn new(owner: impl Into<String>, retry_backoff_ms: i64, retry_backoff_max_ms: i64) -> Self {
        Self::with_backoff_params(
            owner,
            retry_backoff_ms,
            RequestState::RETRY_BACKOFF_EXP_BASE,
            retry_backoff_max_ms,
            RequestState::RETRY_BACKOFF_JITTER,
        )
    }

    /// Creates a new [`RequestState`] with explicit exponential-backoff
    /// parameters. Mirrors Java's package-private (visible-for-testing)
    /// constructor.
    pub(crate) fn with_backoff_params(
        owner: impl Into<String>,
        retry_backoff_ms: i64,
        retry_backoff_exp_base: i32,
        retry_backoff_max_ms: i64,
        jitter: f64,
    ) -> Self {
        let exponential_backoff =
            ExponentialBackoff::new(retry_backoff_ms, retry_backoff_exp_base, retry_backoff_max_ms, jitter)
                .expect("ExponentialBackoff::new only fails on invalid jitter");
        Self {
            owner: owner.into(),
            exponential_backoff,
            last_sent_ms: -1,
            last_received_ms: -1,
            num_attempts: 0,
            backoff_ms: 0,
            request_in_flight: false,
        }
    }

    /// Reset request state so that new requests can be sent immediately and
    /// the backoff is restored to its minimal configuration.
    ///
    /// Java: `reset()`.
    pub(crate) fn reset(&mut self) {
        self.request_in_flight = false;
        self.last_sent_ms = -1;
        self.last_received_ms = -1;
        self.num_attempts = 0;
        self.backoff_ms = self.exponential_backoff.backoff(0);
    }

    /// Returns `true` if it is permissible to send a new request now: no
    /// request is in flight and the backoff timer has elapsed.
    ///
    /// Java: `canSendRequest(long currentTimeMs)`.
    pub(crate) fn can_send_request(&self, current_time_ms: i64) -> bool {
        if self.request_in_flight() {
            log::trace!("An inflight request already exists for {self}");
            return false;
        }
        let remaining = self.remaining_backoff_ms(current_time_ms);
        if remaining <= 0 {
            true
        } else {
            log::trace!("{remaining} ms remain before another request should be sent for {self}");
            false
        }
    }

    /// Returns `true` if a request has been sent but its response has not
    /// yet been observed.
    ///
    /// Java: `requestInFlight()`.
    pub(crate) fn request_in_flight(&self) -> bool {
        self.request_in_flight
    }

    /// Updates state to reflect that a request was sent at the given time.
    ///
    /// Java: `onSendAttempt(long currentTimeMs)`.
    pub(crate) fn on_send_attempt(&mut self, current_time_ms: i64) {
        self.request_in_flight = true;
        // The timer is updated every send attempt.
        self.last_sent_ms = current_time_ms;
    }

    /// Callback invoked after a successful send. Resets the number of
    /// attempts to 0, but the minimal backoff is still enforced before a
    /// new send is allowed. To send immediately, call [`Self::reset`].
    ///
    /// Java: `onSuccessfulAttempt(long currentTimeMs)`.
    pub(crate) fn on_successful_attempt(&mut self, current_time_ms: i64) {
        self.request_in_flight = false;
        self.last_received_ms = current_time_ms;
        self.backoff_ms = self.exponential_backoff.backoff(0);
        self.num_attempts = 0;
    }

    /// Callback invoked after a failed send. Increments the number of
    /// attempts, increasing the backoff before the next send attempt.
    ///
    /// Java: `onFailedAttempt(long currentTimeMs)`.
    pub(crate) fn on_failed_attempt(&mut self, current_time_ms: i64) {
        self.request_in_flight = false;
        self.last_received_ms = current_time_ms;
        self.backoff_ms = self.exponential_backoff.backoff(self.num_attempts as i64);
        self.num_attempts += 1;
    }

    /// Returns the number of consecutive failed attempts since the last
    /// successful send (or construction). Mirrors Java's
    /// `protected int numAttempts` — exposed as a read-only accessor so
    /// retry-driver code can decide between retry and surface-to-caller
    /// based on accumulated attempts.
    pub(crate) fn num_attempts(&self) -> i32 {
        self.num_attempts
    }

    /// Returns the number of milliseconds remaining before the next send
    /// is allowed, given the current time.
    ///
    /// Java: package-private `remainingBackoffMs(long currentTimeMs)`.
    pub(crate) fn remaining_backoff_ms(&self, current_time_ms: i64) -> i64 {
        let time_since_last_receive = current_time_ms - self.last_received_ms;
        (self.backoff_ms - time_since_last_receive).max(0)
    }

    /// Returns the comma-separated key=value pairs that Java's
    /// `toStringBase()` produces. Visible to subclasses (e.g.
    /// [`super::TimedRequestState`]) so they can append
    /// their own state without duplicating each field.
    pub(crate) fn to_string_base(&self) -> String {
        format!(
            "owner='{}', exponentialBackoff={}, lastSentMs={}, lastReceivedMs={}, numAttempts={}, backoffMs={}, \
             requestInFlight={}",
            self.owner,
            self.exponential_backoff,
            self.last_sent_ms,
            self.last_received_ms,
            self.num_attempts,
            self.backoff_ms,
            self.request_in_flight
        )
    }
}

impl fmt::Display for RequestState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RequestState{{{}}}", self.to_string_base())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Translated from `RequestStateTest.testRequestStateSimple`. The Java
    /// test uses jitter = 0 so backoffs are deterministic.
    #[test]
    fn test_request_state_simple() {
        let mut state = RequestState::with_backoff_params("RequestStateTest", 100, 2, 1000, 0.0);

        // ensure not permitting consecutive requests
        assert!(state.can_send_request(0));
        state.on_send_attempt(0);
        assert!(!state.can_send_request(0));
        state.on_failed_attempt(35);
        assert!(state.can_send_request(135));
        state.on_failed_attempt(140);
        assert!(!state.can_send_request(200));
        // exponential backoff (after 2 failures with base 2 -> 100 * 2^1 = 200; 140 + 200 = 340)
        assert!(state.can_send_request(340));

        // test reset
        state.reset();
        assert!(state.can_send_request(200));
    }

    /// Translated from `RequestStateTest.testTrackInflightOnSuccessfulAttempt`.
    #[test]
    fn test_track_inflight_on_successful_attempt() {
        test_track_inflight(|state, ms| state.on_successful_attempt(ms));
    }

    /// Translated from `RequestStateTest.testTrackInflightOnFailedAttempt`.
    #[test]
    fn test_track_inflight_on_failed_attempt() {
        test_track_inflight(|state, ms| state.on_failed_attempt(ms));
    }

    fn test_track_inflight<F>(on_completed_attempt: F)
    where
        F: Fn(&mut RequestState, i64),
    {
        let mut state = RequestState::with_backoff_params("RequestStateTest", 100, 2, 1000, 0.0);

        // A fresh RequestState must not think a request is in flight.
        assert!(!state.request_in_flight());

        // After send, the inflight flag flips to true.
        state.on_send_attempt(202);
        assert!(state.request_in_flight());

        // Response received — the flag flips back to false.
        on_completed_attempt(&mut state, 236);
        assert!(!state.request_in_flight());

        // Same-timestamp send after response must still flip the flag.
        state.on_send_attempt(236);
        assert!(state.request_in_flight());
    }
}
