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

//! Translation of `org.apache.kafka.common.utils.ExponentialBackoffManager`.

use crate::common::utils::exponential_backoff::ExponentialBackoff;

/// Tracks attempt counts and computes exponential backoff for retries.
///
/// Mirrors Java's `ExponentialBackoffManager`. Not thread-safe in the Java
/// reference implementation either; callers serialize access externally.
pub struct ExponentialBackoffManager {
    max_attempts: i32,
    attempts: i32,
    backoff: ExponentialBackoff,
}

impl ExponentialBackoffManager {
    /// Construct a new manager. See [`ExponentialBackoff::new`] for jitter
    /// validation rules; we propagate any error here.
    pub fn new(
        max_attempts: i32,
        initial_interval: i64,
        multiplier: i32,
        max_interval: i64,
        jitter: f64,
    ) -> Result<Self, String> {
        Ok(ExponentialBackoffManager {
            max_attempts,
            attempts: 0,
            backoff: ExponentialBackoff::new(initial_interval, multiplier, max_interval, jitter)?,
        })
    }

    /// Increment the attempt counter.
    pub fn increment_attempt(&mut self) {
        self.attempts += 1;
    }

    /// Reset the attempt counter back to zero.
    pub fn reset_attempts(&mut self) {
        self.attempts = 0;
    }

    /// True iff the caller has not yet exhausted `max_attempts` retries.
    pub fn can_attempt(&self) -> bool {
        self.attempts < self.max_attempts
    }

    /// Compute the backoff for the current attempt count.
    pub fn back_off(&self) -> i64 {
        self.backoff.backoff(self.attempts as i64)
    }

    /// The current attempt count.
    pub fn attempts(&self) -> i32 {
        self.attempts
    }
}

#[cfg(test)]
mod tests {
    // Translation of `ExponentialBackoffManagerTest`.

    use super::*;

    fn no_jitter_manager(max_attempts: i32) -> ExponentialBackoffManager {
        ExponentialBackoffManager::new(max_attempts, 100, 2, 1000, 0.0).unwrap()
    }

    /// Java: `testInitialState`.
    #[test]
    fn initial_state() {
        let mgr = no_jitter_manager(5);
        assert_eq!(mgr.attempts(), 0);
        assert!(mgr.can_attempt());
    }

    /// Java: `testIncrementAttempt`.
    #[test]
    fn increment_attempt() {
        let mut mgr = no_jitter_manager(5);
        assert_eq!(mgr.attempts(), 0);
        mgr.increment_attempt();
        assert_eq!(mgr.attempts(), 1);
    }

    /// Java: `testResetAttempts`.
    #[test]
    fn reset_attempts() {
        let mut mgr = no_jitter_manager(5);
        mgr.increment_attempt();
        mgr.increment_attempt();
        mgr.increment_attempt();
        assert_eq!(mgr.attempts(), 3);

        mgr.reset_attempts();
        assert_eq!(mgr.attempts(), 0);
        assert!(mgr.can_attempt());
    }

    /// Java: `testCanAttempt`.
    #[test]
    fn can_attempt() {
        let mut mgr = no_jitter_manager(3);
        assert!(mgr.can_attempt());
        assert_eq!(mgr.attempts(), 0);

        mgr.increment_attempt();
        mgr.increment_attempt();
        mgr.increment_attempt();
        assert!(!mgr.can_attempt());
        assert_eq!(mgr.attempts(), 3);
    }

    /// Java: `testBackOffWithoutJitter`.
    #[test]
    fn back_off_without_jitter() {
        let backoffs: [i64; 5] = [100, 200, 400, 800, 1600];
        let mut mgr = no_jitter_manager(5);
        for expected in backoffs {
            let observed = mgr.back_off();
            assert_eq!(observed, std::cmp::min(1000, expected));
            mgr.increment_attempt();
        }
    }

    /// Java: `testBackOffWithJitter`.
    #[test]
    fn back_off_with_jitter() {
        let backoffs: [i64; 5] = [100, 200, 400, 800, 1600];
        let mut mgr = ExponentialBackoffManager::new(5, 100, 2, 1000, 0.2).unwrap();
        for expected in backoffs {
            let cap = std::cmp::min(1000, expected) as f64;
            let observed = mgr.back_off();
            assert!(observed as f64 >= 0.8 * cap, "observed={observed}, cap={cap}");
            assert!(observed as f64 <= 1.2 * cap, "observed={observed}, cap={cap}");
            mgr.increment_attempt();
        }
    }
}
