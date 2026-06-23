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

//! `TimedRequestState` — [`RequestState`] augmented with a deadline timer.
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.TimedRequestState`.

// Items here are consumed by Phase 7-9 managers (`CommitRequestManager`,
// `OffsetsRequestManager`, etc.). Suppress dead-code warnings for items
// that aren't yet referenced inside the workspace.
#![allow(dead_code)]

use std::fmt;
use std::ops::{Deref, DerefMut};

use super::request_state::{RETRY_BACKOFF_EXP_BASE, RETRY_BACKOFF_JITTER, RequestState};

/// Wraps a [`RequestState`] with an absolute deadline (wall-clock
/// millisecond timestamp) so callers can ask "is this request past its
/// deadline?".
///
/// Java's [`TimedRequestState`] *extends* [`RequestState`]; the Rust
/// translation uses composition and exposes the inner state via
/// [`Deref`]/[`DerefMut`] so callers can call `RequestState` methods
/// transparently:
///
/// ```ignore
/// timed_state.on_send_attempt(now); // delegates to RequestState
/// timed_state.is_expired(now);      // TimedRequestState-specific
/// ```
///
/// # Decision: `deadline_ms` instead of Java's `Timer`
///
/// Java's `Timer` is updated via `timer.update(currentTimeMs)` and queried
/// via `timer.isExpired()`. The Rust translation collapses this to a plain
/// `i64 deadline_ms` because every callsite already threads
/// `current_time_ms` through `RequestManager::poll`. We do **not** add a
/// Rust analog of Java's `Timer` class at this surface — the
/// `current_time_ms` parameter on every query is the contract.
pub(crate) struct TimedRequestState {
    state: RequestState,
    deadline_ms: i64,
}

impl TimedRequestState {
    /// Creates a [`TimedRequestState`] with the default exponent base and
    /// jitter.
    ///
    /// `deadline_ms` is the absolute wall-clock millisecond timestamp at
    /// which the request expires (i.e. `now_ms + timeout_ms`).
    pub(crate) fn new(
        owner: impl Into<String>,
        retry_backoff_ms: i64,
        retry_backoff_max_ms: i64,
        deadline_ms: i64,
    ) -> Self {
        Self::with_backoff_params(
            owner,
            retry_backoff_ms,
            RETRY_BACKOFF_EXP_BASE,
            retry_backoff_max_ms,
            RETRY_BACKOFF_JITTER,
            deadline_ms,
        )
    }

    /// Creates a [`TimedRequestState`] with explicit exponential-backoff
    /// parameters and an absolute deadline.
    pub(crate) fn with_backoff_params(
        owner: impl Into<String>,
        retry_backoff_ms: i64,
        retry_backoff_exp_base: i32,
        retry_backoff_max_ms: i64,
        jitter: f64,
        deadline_ms: i64,
    ) -> Self {
        Self {
            state: RequestState::with_backoff_params(
                owner,
                retry_backoff_ms,
                retry_backoff_exp_base,
                retry_backoff_max_ms,
                jitter,
            ),
            deadline_ms,
        }
    }

    /// Returns `true` if the deadline has passed at the given current time.
    ///
    /// Java: `isExpired()` (the Java version internally calls
    /// `timer.update()` then `timer.isExpired()`).
    pub(crate) fn is_expired(&self, current_time_ms: i64) -> bool {
        current_time_ms >= self.deadline_ms
    }

    /// Returns the number of milliseconds remaining until the deadline at
    /// the given current time, clamped to `[0, i64::MAX]`.
    ///
    /// Java: `remainingMs()` (Java version `update`s then returns the
    /// timer's `remainingMs`).
    pub(crate) fn remaining_ms(&self, current_time_ms: i64) -> i64 {
        (self.deadline_ms - current_time_ms).max(0)
    }

    /// Replaces the deadline with `deadline_ms`. This is the Rust analog
    /// of Java's `resetTimeout(long timeoutMs)`, which calls
    /// `timer.updateAndReset(timeoutMs)` (relative). Here the absolute
    /// deadline is passed directly so the caller does not need a `Time`
    /// reference.
    ///
    /// Java: `resetTimeout(long timeoutMs)` — but with absolute
    /// `deadline_ms` instead of a relative `timeoutMs`.
    pub(crate) fn reset_deadline(&mut self, deadline_ms: i64) {
        self.deadline_ms = deadline_ms;
    }

    /// Computes an absolute deadline from `now_ms + max(0, deadline_ms - now_ms)`.
    /// Mirrors Java's `deadlineTimer(time, deadlineMs)` semantics for the
    /// "allow overdue deadline" case: an in-the-past deadline collapses to
    /// `now_ms` so `remaining_ms()` returns 0 (never a negative value).
    pub(crate) fn deadline_for(now_ms: i64, deadline_ms: i64) -> i64 {
        if deadline_ms < now_ms { now_ms } else { deadline_ms }
    }

    /// Renders the inner state followed by `, remainingMs=...`, matching
    /// Java's `toStringBase()` override.
    fn to_string_with(&self, current_time_ms: i64) -> String {
        format!(
            "{}, remainingMs={}",
            self.state.to_string_base(),
            self.remaining_ms(current_time_ms)
        )
    }
}

impl Deref for TimedRequestState {
    type Target = RequestState;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

impl DerefMut for TimedRequestState {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.state
    }
}

impl fmt::Display for TimedRequestState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Java's toString calls toStringBase which internally calls
        // timer.update() — using `i64::MIN` as current_time_ms would give
        // a garbage remainingMs. We rely on the test helpers to call
        // `to_string_with(current_time_ms)` instead. For general Display
        // we emit a stable rendering that does not consult wall-clock
        // time. Tests covering `toString()` behaviour go through
        // [`Self::to_string_with`].
        write!(
            f,
            "TimedRequestState{{{}, deadlineMs={}}}",
            self.state.to_string_base(),
            self.deadline_ms
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DEFAULT_TIMEOUT_MS: i64 = 30_000;

    /// Translated from `TimedRequestStateTest.testIsExpired`. The Java
    /// version constructs the state with `time.timer(DEFAULT_TIMEOUT_MS)`
    /// — the Rust translation passes `now + DEFAULT_TIMEOUT_MS` as the
    /// absolute deadline.
    #[test]
    fn test_is_expired() {
        let now = 0_i64;
        let state = TimedRequestState::new(
            "TimedRequestStateTest",
            100,
            1000,
            TimedRequestState::deadline_for(now, now + DEFAULT_TIMEOUT_MS),
        );
        assert!(!state.is_expired(now));
        assert!(state.is_expired(now + DEFAULT_TIMEOUT_MS));
    }

    /// Translated from `TimedRequestStateTest.testRemainingMs`.
    #[test]
    fn test_remaining_ms() {
        let now = 0_i64;
        let state = TimedRequestState::new(
            "TimedRequestStateTest",
            100,
            1000,
            TimedRequestState::deadline_for(now, now + DEFAULT_TIMEOUT_MS),
        );
        assert_eq!(DEFAULT_TIMEOUT_MS, state.remaining_ms(now));
        assert_eq!(0, state.remaining_ms(now + DEFAULT_TIMEOUT_MS));
    }

    /// Translated from `TimedRequestStateTest.testDeadlineTimer`. The
    /// Java test wraps `time.timer(...)`; we test the Rust analog
    /// `deadline_for(now, deadline_ms)` directly.
    #[test]
    fn test_deadline_timer() {
        let now = 0_i64;
        let deadline_ms = now + DEFAULT_TIMEOUT_MS;
        let resolved = TimedRequestState::deadline_for(now, deadline_ms);
        // remaining is DEFAULT_TIMEOUT_MS at now, 0 at deadline.
        assert_eq!(DEFAULT_TIMEOUT_MS, (resolved - now).max(0));
        assert_eq!(0, (resolved - (now + DEFAULT_TIMEOUT_MS)).max(0));
    }

    /// Translated from `TimedRequestStateTest.testAllowOverdueDeadlineTimer`.
    /// An in-the-past deadline collapses to "now" so `remaining_ms` is 0.
    #[test]
    fn test_allow_overdue_deadline_timer() {
        let now = DEFAULT_TIMEOUT_MS;
        let deadline_ms = now - DEFAULT_TIMEOUT_MS; // in the past
        let resolved = TimedRequestState::deadline_for(now, deadline_ms);
        assert_eq!(0, (resolved - now).max(0));
    }

    /// Translated from `TimedRequestStateTest.testToStringUpdatesTimer`.
    /// Java's `toString()` reads from `Timer` which is updated by
    /// `update()`; the Rust translation uses `to_string_with(now)` because
    /// `Display::fmt` cannot consult wall-clock time deterministically.
    #[test]
    fn test_to_string_updates_timer() {
        let now = 0_i64;
        let state = TimedRequestState::new(
            "TimedRequestStateTest",
            100,
            1000,
            TimedRequestState::deadline_for(now, now + DEFAULT_TIMEOUT_MS),
        );

        assert!(state.to_string_with(now).contains(&format!("remainingMs={DEFAULT_TIMEOUT_MS}")));
        assert!(state.to_string_with(now + DEFAULT_TIMEOUT_MS).contains("remainingMs=0"));
    }

    /// Verifies that the `Deref` impl exposes `RequestState` methods
    /// transparently (composition-over-inheritance contract).
    #[test]
    fn test_deref_to_request_state() {
        let now = 0_i64;
        let mut state = TimedRequestState::new(
            "TimedRequestStateTest",
            100,
            1000,
            TimedRequestState::deadline_for(now, now + DEFAULT_TIMEOUT_MS),
        );

        assert!(state.can_send_request(now)); // RequestState method
        state.on_send_attempt(now); // RequestState method via DerefMut
        assert!(state.request_in_flight()); // RequestState method
    }
}
