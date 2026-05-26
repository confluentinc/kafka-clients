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

//! Translation of `org.apache.kafka.common.utils.Timer`.

use std::sync::Arc;

use crate::common::utils::Time;

/// A helper that simplifies blocking calls with composite timeouts.
///
/// Mirrors `org.apache.kafka.common.utils.Timer`. Caches the cached current
/// time in milliseconds so callers can advance the timer explicitly via
/// [`Timer::update`] (or [`Timer::sleep`]) and the underlying clock is hit
/// as rarely as possible. The cached current time updates monotonically
/// even if the underlying clock goes backwards.
pub struct Timer {
    time: Arc<dyn Time>,
    start_ms: i64,
    current_time_ms: i64,
    deadline_ms: i64,
    timeout_ms: i64,
}

impl Timer {
    /// Construct a [`Timer`] for the given clock with an initial timeout.
    /// Equivalent to Java's package-private `Timer(Time, long)` constructor;
    /// production callers go through `Time.timer(timeout)` (see
    /// [`crate::common::utils::time::timer`]).
    pub fn new(time: Arc<dyn Time>, timeout_ms: i64) -> Self {
        let mut t = Timer { time, start_ms: 0, current_time_ms: 0, deadline_ms: 0, timeout_ms: 0 };
        t.update();
        t.reset(timeout_ms);
        t
    }

    /// Returns true iff the timer has expired. Like [`Timer::remaining_ms`],
    /// this depends on the cached current time.
    pub fn is_expired(&self) -> bool {
        self.current_time_ms >= self.deadline_ms
    }

    /// Get the time in milliseconds that the timer has been expired. Returns
    /// `0` if the timer has not yet expired.
    pub fn is_expired_by(&self) -> i64 {
        std::cmp::max(0, self.current_time_ms - self.deadline_ms)
    }

    /// Convenience: the inverse of [`Timer::is_expired`].
    pub fn not_expired(&self) -> bool {
        !self.is_expired()
    }

    /// Update the cached current time and reset the deadline to
    /// `current_time + timeout_ms`. Equivalent to Java's `updateAndReset`.
    pub fn update_and_reset(&mut self, timeout_ms: i64) {
        self.update();
        self.reset(timeout_ms);
    }

    /// Reset the timer using a new timeout. Does NOT advance the cached time
    /// — typically follow with [`Timer::update`] if you want a fresh
    /// observation. Equivalent to Java's `reset`.
    ///
    /// # Errors
    ///
    /// Java raises `IllegalArgumentException` for negative timeouts. We mirror
    /// that with a panic since the Java caller treats it as a programming
    /// bug; the public API equivalents that translate Java methods returning
    /// `void` retain that contract.
    pub fn reset(&mut self, timeout_ms: i64) {
        assert!(timeout_ms >= 0, "Invalid negative timeout {timeout_ms}");
        self.timeout_ms = timeout_ms;
        self.start_ms = self.current_time_ms;
        // Saturating add to mirror Java's `if (currentTimeMs > Long.MAX_VALUE - timeoutMs)` clamp.
        self.deadline_ms = self.current_time_ms.saturating_add(timeout_ms);
    }

    /// Reset the deadline directly to `deadline_ms`. Equivalent to Java's
    /// `resetDeadline`.
    pub fn reset_deadline(&mut self, deadline_ms: i64) {
        assert!(deadline_ms >= 0, "Invalid negative deadline {deadline_ms}");
        self.timeout_ms = std::cmp::max(0, deadline_ms - self.current_time_ms);
        self.start_ms = self.current_time_ms;
        self.deadline_ms = deadline_ms;
    }

    /// Refresh the cached current time from the underlying [`Time`]. Updates
    /// are monotonic — if the new observation is older than the cached value
    /// it is ignored.
    pub fn update(&mut self) {
        let now = self.time.milliseconds();
        self.update_to(now);
    }

    /// Update the cached current time to a specific value. If the new value
    /// is smaller than the cached one, the update is ignored.
    pub fn update_to(&mut self, current_time_ms: i64) {
        self.current_time_ms = std::cmp::max(current_time_ms, self.current_time_ms);
    }

    /// Get the remaining time in milliseconds until the timer expires.
    pub fn remaining_ms(&self) -> i64 {
        std::cmp::max(0, self.deadline_ms - self.current_time_ms)
    }

    /// Get the cached current time in milliseconds.
    pub fn current_time_ms(&self) -> i64 {
        self.current_time_ms
    }

    /// Time elapsed since the timer was constructed or last reset.
    pub fn elapsed_ms(&self) -> i64 {
        self.current_time_ms - self.start_ms
    }

    /// The configured timeout for the current period.
    pub fn timeout_ms(&self) -> i64 {
        self.timeout_ms
    }

    /// Sleep for `duration_ms`, or until the timer expires (whichever is
    /// shorter), then refresh the cached time.
    pub fn sleep(&mut self, duration_ms: i64) {
        let sleep_duration_ms = std::cmp::min(duration_ms, self.remaining_ms());
        self.time.sleep(sleep_duration_ms);
        self.update();
    }
}

#[cfg(test)]
mod tests {
    // Translation of `org.apache.kafka.common.utils.TimerTest`.

    use super::*;
    use crate::common::utils::MockTime;

    fn mock_time() -> Arc<MockTime> {
        Arc::new(MockTime::with_initial(0, 0, 0))
    }

    fn timer_with(time: Arc<MockTime>, timeout_ms: i64) -> Timer {
        let dyn_time: Arc<dyn Time> = time.clone();
        Timer::new(dyn_time, timeout_ms)
    }

    /// Java: `testTimerUpdate`.
    #[test]
    fn timer_update() {
        let time = mock_time();
        let mut timer = timer_with(time.clone(), 500);
        assert_eq!(timer.timeout_ms(), 500);
        assert_eq!(timer.remaining_ms(), 500);
        assert_eq!(timer.elapsed_ms(), 0);

        time.sleep(100);
        timer.update();

        assert_eq!(timer.timeout_ms(), 500);
        assert_eq!(timer.remaining_ms(), 400);
        assert_eq!(timer.elapsed_ms(), 100);

        time.sleep(400);
        let now = time.milliseconds();
        timer.update_to(now);

        assert_eq!(timer.timeout_ms(), 500);
        assert_eq!(timer.remaining_ms(), 0);
        assert_eq!(timer.elapsed_ms(), 500);
        assert!(timer.is_expired());

        time.sleep(200);
        let now = time.milliseconds();
        timer.update_to(now);
        assert!(timer.is_expired());
        assert_eq!(timer.timeout_ms(), 500);
        assert_eq!(timer.remaining_ms(), 0);
        assert_eq!(timer.elapsed_ms(), 700);
    }

    /// Java: `testTimerUpdateAndReset`.
    #[test]
    fn timer_update_and_reset() {
        let time = mock_time();
        let mut timer = timer_with(time.clone(), 500);
        timer.sleep(200);
        assert_eq!(timer.timeout_ms(), 500);
        assert_eq!(timer.remaining_ms(), 300);
        assert_eq!(timer.elapsed_ms(), 200);

        timer.update_and_reset(400);
        assert_eq!(timer.timeout_ms(), 400);
        assert_eq!(timer.remaining_ms(), 400);
        assert_eq!(timer.elapsed_ms(), 0);

        timer.sleep(400);
        assert!(timer.is_expired());

        timer.update_and_reset(200);
        assert_eq!(timer.timeout_ms(), 200);
        assert_eq!(timer.remaining_ms(), 200);
        assert_eq!(timer.elapsed_ms(), 0);
        assert!(!timer.is_expired());
    }

    /// Java: `testTimerResetUsesCurrentTime`.
    #[test]
    fn timer_reset_uses_current_time() {
        let time = mock_time();
        let mut timer = timer_with(time.clone(), 500);
        timer.sleep(200);
        assert_eq!(timer.remaining_ms(), 300);
        assert_eq!(timer.elapsed_ms(), 200);

        time.sleep(300);
        timer.reset(500);
        assert_eq!(timer.remaining_ms(), 500);

        timer.update();
        assert_eq!(timer.remaining_ms(), 200);
    }

    /// Java: `testTimerResetDeadlineUsesCurrentTime`.
    #[test]
    fn timer_reset_deadline_uses_current_time() {
        let time = mock_time();
        let mut timer = timer_with(time.clone(), 500);
        timer.sleep(200);
        assert_eq!(timer.remaining_ms(), 300);
        assert_eq!(timer.elapsed_ms(), 200);

        timer.sleep(100);
        let deadline = time.milliseconds() + 200;
        timer.reset_deadline(deadline);
        assert_eq!(timer.timeout_ms(), 200);
        assert_eq!(timer.remaining_ms(), 200);

        timer.sleep(100);
        assert_eq!(timer.timeout_ms(), 200);
        assert_eq!(timer.remaining_ms(), 100);
    }

    /// Java: `testTimeoutOverflow`.
    #[test]
    fn timeout_overflow() {
        let time = mock_time();
        let timer = timer_with(time, i64::MAX);
        assert_eq!(timer.remaining_ms(), i64::MAX - timer.current_time_ms());
        assert_eq!(timer.elapsed_ms(), 0);
    }

    /// Java: `testNonMonotonicUpdate`.
    #[test]
    fn non_monotonic_update() {
        let time = mock_time();
        let mut timer = timer_with(time, 100);
        let current_time_ms = timer.current_time_ms();
        timer.update_to(current_time_ms - 1);
        assert_eq!(timer.current_time_ms(), current_time_ms);

        assert_eq!(timer.remaining_ms(), 100);
        assert_eq!(timer.elapsed_ms(), 0);
    }

    /// Java: `testTimerSleep`.
    #[test]
    fn timer_sleep() {
        let time = mock_time();
        let mut timer = timer_with(time.clone(), 500);
        let current_time_ms = timer.current_time_ms();

        timer.sleep(200);
        assert_eq!(timer.current_time_ms(), time.milliseconds());
        assert_eq!(timer.current_time_ms(), current_time_ms + 200);

        timer.sleep(1000);
        assert_eq!(timer.current_time_ms(), time.milliseconds());
        assert_eq!(timer.current_time_ms(), current_time_ms + 500);
        assert!(timer.is_expired());
    }
}
