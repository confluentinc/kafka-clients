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

//! `HeartbeatRequestState` — composes [`RequestState`] with a heartbeat
//! timer for KIP-848 heartbeat scheduling.
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.HeartbeatRequestState`.

#![allow(dead_code)]

use std::fmt;

use super::request_state::RequestState;

/// State for a heartbeat request, including a heartbeat-interval timer
/// composed alongside [`RequestState`]'s exponential-backoff machinery.
///
/// Java extends `RequestState`; Rust composes it as a field.
///
/// # Why composition over inheritance
///
/// Java models the relationship via `extends`. Rust's translation rule
/// (Phase 7a precedent for `AbstractFetch`) is to compose the base type as
/// a `pub(crate)` field and forward only the methods we use.
pub(crate) struct HeartbeatRequestState {
    /// Composed [`RequestState`] for exponential-backoff bookkeeping.
    request_state: RequestState,
    /// Absolute expiration time (ms). Java models this via `Timer`; Rust
    /// stores the deadline directly. When `current_time_ms >=
    /// timer_expires_at_ms` the timer is "expired".
    timer_expires_at_ms: i64,
    /// Last observed time (ms). Mirrors Java's `Timer::update(currentMs)`
    /// which stores the time the timer was last refreshed against.
    timer_last_update_ms: i64,
    /// The heartbeat interval which is acquired/updated through the
    /// heartbeat request.
    heartbeat_interval_ms: i64,
}

impl HeartbeatRequestState {
    /// Creates a new [`HeartbeatRequestState`] with the given interval and
    /// retry-backoff configuration. The internal timer starts running from
    /// the time `time` was first observed by the caller — Java reads
    /// `Time::milliseconds()` inside `time.timer(...)`. Rust takes the
    /// starting time explicitly as `current_time_ms`.
    ///
    /// Java: `HeartbeatRequestState(LogContext, Time, long, long, long, double)`.
    pub(crate) fn new(
        current_time_ms: i64,
        heartbeat_interval_ms: i64,
        retry_backoff_ms: i64,
        retry_backoff_max_ms: i64,
        jitter: f64,
    ) -> Self {
        // Java: super(logContext, HeartbeatRequestState.class.getName(),
        //   retryBackoffMs, 2, retryBackoffMaxMs, jitter);
        let request_state = RequestState::with_backoff_params(
            "HeartbeatRequestState",
            retry_backoff_ms,
            2, // exp base — Java passes the literal 2
            retry_backoff_max_ms,
            jitter,
        );
        Self {
            request_state,
            timer_expires_at_ms: current_time_ms + heartbeat_interval_ms,
            timer_last_update_ms: current_time_ms,
            heartbeat_interval_ms,
        }
    }

    /// Returns the current heartbeat interval (ms).
    ///
    /// Java: `heartbeatIntervalMs()`.
    pub(crate) fn heartbeat_interval_ms(&self) -> i64 {
        self.heartbeat_interval_ms
    }

    /// Resets the heartbeat timer to a full `heartbeat_interval_ms` from
    /// the last observed time.
    ///
    /// Java: `resetTimer()`.
    pub(crate) fn reset_timer(&mut self) {
        self.timer_expires_at_ms = self.timer_last_update_ms + self.heartbeat_interval_ms;
    }

    /// Updates the timer's internal "now" reference to the supplied
    /// `current_time_ms`. Mirrors Java's `Timer::update(now)`.
    fn update(&mut self, current_time_ms: i64) {
        self.timer_last_update_ms = current_time_ms;
    }

    /// Returns `true` if the timer has expired at `current_time_ms`.
    fn timer_is_expired(&self, current_time_ms: i64) -> bool {
        current_time_ms >= self.timer_expires_at_ms
    }

    /// Returns the remaining time on the timer (ms), clamped at 0.
    /// Mirrors Java's `Timer::remainingMs()`.
    fn timer_remaining_ms(&self, current_time_ms: i64) -> i64 {
        (self.timer_expires_at_ms - current_time_ms).max(0)
    }

    /// Returns the time remaining until the next heartbeat is due (ms).
    /// If the timer is already expired, returns the remaining backoff.
    ///
    /// Java: `timeToNextHeartbeatMs(long currentTimeMs)`.
    pub(crate) fn time_to_next_heartbeat_ms(&self, current_time_ms: i64) -> i64 {
        if self.timer_is_expired(current_time_ms) {
            return self.request_state.remaining_backoff_ms(current_time_ms);
        }
        self.timer_remaining_ms(current_time_ms)
    }

    /// On failure, reset the heartbeat timer to a zero interval so that a
    /// retry can be sent without waiting for the full interval, then defer
    /// to the inner [`RequestState`] for backoff bookkeeping.
    ///
    /// Java: overridden `onFailedAttempt(long currentTimeMs)`.
    pub(crate) fn on_failed_attempt(&mut self, current_time_ms: i64) {
        // heartbeatTimer.reset(0) → timer expires immediately at last update.
        self.timer_expires_at_ms = self.timer_last_update_ms;
        self.request_state.on_failed_attempt(current_time_ms);
    }

    /// Returns `true` when (a) the heartbeat timer has expired and (b) no
    /// request is already in flight and the backoff timer has elapsed.
    ///
    /// Java: overridden `canSendRequest(long currentTimeMs)`.
    pub(crate) fn can_send_request(&mut self, current_time_ms: i64) -> bool {
        self.update(current_time_ms);
        self.timer_is_expired(current_time_ms) && self.request_state.can_send_request(current_time_ms)
    }

    /// Update the heartbeat interval. If the interval hasn't changed, this
    /// is a no-op; otherwise the timer is refreshed to `current_time_ms`
    /// and reset to fire after `heartbeat_interval_ms` from that moment.
    /// Mirrors Java's `Timer.updateAndReset(intervalMs)` which calls
    /// `update()` internally to snap `currentTimeMs` to `time.milliseconds()`
    /// before computing the new deadline.
    ///
    /// **Signature deviation from Java**: Java's `Timer` owns a `Time`
    /// reference and self-updates inside `updateAndReset`. Rust does not
    /// thread a `Time` into `HeartbeatRequestState`; callers (notably
    /// `AbstractHeartbeatRequestManager.onResponse`) pass `current_time_ms`
    /// explicitly so the timer baseline matches Java's self-update.
    ///
    /// Java: `updateHeartbeatIntervalMs(long heartbeatIntervalMs)`.
    pub(crate) fn update_heartbeat_interval_ms(&mut self, current_time_ms: i64, heartbeat_interval_ms: i64) {
        if self.heartbeat_interval_ms == heartbeat_interval_ms {
            return;
        }
        self.heartbeat_interval_ms = heartbeat_interval_ms;
        // Java's Timer.updateAndReset: snap currentTimeMs first, then
        // compute the new deadline from there.
        self.timer_last_update_ms = current_time_ms;
        self.timer_expires_at_ms = current_time_ms + heartbeat_interval_ms;
    }

    /// Forwards to the inner [`RequestState`].
    pub(crate) fn on_send_attempt(&mut self, current_time_ms: i64) {
        self.request_state.on_send_attempt(current_time_ms);
    }

    /// Forwards to the inner [`RequestState`].
    pub(crate) fn on_successful_attempt(&mut self, current_time_ms: i64) {
        self.request_state.on_successful_attempt(current_time_ms);
    }

    /// Forwards to the inner [`RequestState`].
    pub(crate) fn reset(&mut self) {
        self.request_state.reset();
    }

    /// Forwards to the inner [`RequestState`].
    pub(crate) fn request_in_flight(&self) -> bool {
        self.request_state.request_in_flight()
    }

    /// Forwards to the inner [`RequestState`].
    pub(crate) fn remaining_backoff_ms(&self, current_time_ms: i64) -> i64 {
        self.request_state.remaining_backoff_ms(current_time_ms)
    }
}

impl fmt::Display for HeartbeatRequestState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Java: super.toStringBase() + ", remainingMs=" + remainingMs + ", heartbeatIntervalMs="
        // Java's Timer.remainingMs() clamps at 0; mirror that.
        write!(
            f,
            "HeartbeatRequestState{{{}, remainingMs={}, heartbeatIntervalMs={}}}",
            self.request_state.to_string_base(),
            (self.timer_expires_at_ms - self.timer_last_update_ms).max(0),
            self.heartbeat_interval_ms
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HEARTBEAT_INTERVAL_MS: i64 = 1000;
    const RETRY_BACKOFF_MS: i64 = 100;
    const RETRY_BACKOFF_MAX_MS: i64 = 500;
    const JITTER: f64 = 0.2;

    fn make_state(now: i64) -> HeartbeatRequestState {
        HeartbeatRequestState::new(now, HEARTBEAT_INTERVAL_MS, RETRY_BACKOFF_MS, RETRY_BACKOFF_MAX_MS, JITTER)
    }

    /// Translated from
    /// `HeartbeatRequestStateTest#testCanSendRequestAndTimeToNextHeartbeatMs`.
    #[test]
    fn test_can_send_request_and_time_to_next_heartbeat_ms() {
        let mut now = 0i64;
        let mut state = make_state(now);

        assert!(!state.can_send_request(now));
        assert_eq!(HEARTBEAT_INTERVAL_MS, state.time_to_next_heartbeat_ms(now));

        now += HEARTBEAT_INTERVAL_MS - 1;
        assert!(!state.can_send_request(now));
        assert_eq!(1, state.time_to_next_heartbeat_ms(now));

        now += 1;
        assert!(state.can_send_request(now));
        assert_eq!(0, state.time_to_next_heartbeat_ms(now));

        now += 100;
        assert!(state.can_send_request(now));
        assert_eq!(0, state.time_to_next_heartbeat_ms(now));
    }

    /// Translated from `HeartbeatRequestStateTest#testResetTimer`.
    #[test]
    fn test_reset_timer() {
        let mut now = 0i64;
        let mut state = make_state(now);

        now += HEARTBEAT_INTERVAL_MS + 100;
        assert!(state.can_send_request(now));
        assert_eq!(0, state.time_to_next_heartbeat_ms(now));

        state.reset_timer();
        assert!(!state.can_send_request(now));
        assert_eq!(HEARTBEAT_INTERVAL_MS, state.time_to_next_heartbeat_ms(now));
    }

    /// Translated from `HeartbeatRequestStateTest#testUpdateHeartbeatIntervalMs`.
    /// Java: at t=1100, calls `updateHeartbeatIntervalMs(2 * HEARTBEAT_INTERVAL_MS)`
    /// directly. Java's `Timer.updateAndReset` self-updates from `time.milliseconds()`
    /// so the deadline becomes `1100 + 2000 = 3100`, and
    /// `timeToNextHeartbeatMs(1100) == 2000`.
    #[test]
    fn test_update_heartbeat_interval_ms() {
        let mut now = 0i64;
        let mut state = make_state(now);
        let updated_interval = 2 * HEARTBEAT_INTERVAL_MS;

        now += HEARTBEAT_INTERVAL_MS + 100;
        // No prior refresh — pass current_time_ms explicitly to mirror Java's
        // Timer.updateAndReset self-update.
        state.update_heartbeat_interval_ms(now, updated_interval);

        assert!(!state.can_send_request(now));
        assert_eq!(2 * HEARTBEAT_INTERVAL_MS, state.time_to_next_heartbeat_ms(now));
    }

    /// Translated from
    /// `HeartbeatRequestStateTest#testUpdateHeartbeatIntervalMsWithSameInterval`.
    #[test]
    fn test_update_heartbeat_interval_ms_with_same_interval() {
        let mut now = 0i64;
        let mut state = make_state(now);

        now += HEARTBEAT_INTERVAL_MS + 100;
        state.update_heartbeat_interval_ms(now, HEARTBEAT_INTERVAL_MS);

        assert_eq!(HEARTBEAT_INTERVAL_MS, state.heartbeat_interval_ms());
        assert!(state.can_send_request(now));
    }

    /// Translated from `HeartbeatRequestStateTest#testOnFailedAttempt`.
    #[test]
    fn test_on_failed_attempt() {
        let mut now = 0i64;
        let mut state = make_state(now);

        now += HEARTBEAT_INTERVAL_MS + 100;
        // Java's `onFailedAttempt` overrides reset the timer to 0 (immediate)
        // but the backoff inside RequestState still applies — must wait
        // RETRY_BACKOFF_MS (with jitter) before next attempt is allowed.
        state.on_failed_attempt(now);

        assert!(!state.can_send_request(now));
        assert!(state.time_to_next_heartbeat_ms(now) > 0);
    }
}
