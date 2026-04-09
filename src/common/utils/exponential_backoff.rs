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

//! A utility class for keeping the parameters and providing the value of exponential
//! retry backoff, exponential reconnect backoff, exponential timeout, etc.
//!
//! The formula is:
//! ```text
//! Backoff(attempts) = random(1 - jitter, 1 + jitter) * initial_interval * multiplier ^ attempts
//! ```
//! If `max_interval` is less than `initial_interval`, a constant backoff of
//! `max_interval` will be provided. The jitter will never cause the backoff to exceed
//! `max_interval`.
//!
//! This struct is thread-safe (all fields are immutable after construction).

use std::fmt;

use rand::Rng;

/// Provides exponential backoff values with optional jitter.
///
/// Used for retry backoff, reconnect backoff, exponential timeout, etc.
/// The formula is:
/// ```text
/// Backoff(attempts) = random(1 - jitter, 1 + jitter) * initial_interval * multiplier ^ attempts
/// ```
///
/// This struct is `Send + Sync` since all fields are immutable after construction.
#[derive(Debug)]
pub struct ExponentialBackoff {
    initial_interval: i64,
    multiplier: i32,
    max_interval: i64,
    jitter: f64,
    exp_max: f64,
}

impl ExponentialBackoff {
    /// Creates a new `ExponentialBackoff` with the given parameters.
    ///
    /// # Arguments
    /// * `initial_interval` - The initial backoff interval in milliseconds.
    /// * `multiplier` - The multiplier applied to the interval for each attempt.
    /// * `max_interval` - The maximum backoff interval in milliseconds.
    /// * `jitter` - The jitter factor, must be between 0.0 and 1.0 (inclusive).
    ///
    /// # Errors
    /// Returns an error if `jitter` is not between 0.0 and 1.0.
    pub fn new(initial_interval: i64, multiplier: i32, max_interval: i64, jitter: f64) -> Result<Self, String> {
        if !(0.0..=1.0).contains(&jitter) {
            return Err(format!("jitter must be between 0 and 1, but got {}", jitter));
        }
        let clamped_initial = initial_interval.min(max_interval);
        let exp_max = if max_interval > clamped_initial {
            (max_interval as f64 / (clamped_initial.max(1) as f64)).ln() / (multiplier as f64).ln()
        } else {
            0.0
        };
        Ok(Self { initial_interval: clamped_initial, multiplier, max_interval, jitter, exp_max })
    }

    /// Returns the initial interval.
    pub fn initial_interval(&self) -> i64 {
        self.initial_interval
    }

    /// Computes the backoff value for the given number of attempts.
    ///
    /// The returned value is clamped to `max_interval` and includes jitter if configured.
    pub fn backoff(&self, attempts: i64) -> i64 {
        if self.exp_max == 0.0 {
            return self.initial_interval;
        }
        let exp = (attempts as f64).min(self.exp_max);
        let term = self.initial_interval as f64 * (self.multiplier as f64).powf(exp);
        let random_factor = if self.jitter < f64::MIN_POSITIVE {
            1.0
        } else {
            let mut rng = rand::rng();
            rng.random_range((1.0 - self.jitter)..(1.0 + self.jitter))
        };
        let backoff_value = (random_factor * term) as i64;
        backoff_value.min(self.max_interval)
    }
}

impl fmt::Display for ExponentialBackoff {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ExponentialBackoff{{multiplier={}, expMax={}, initialInterval={}, jitter={}}}",
            self.multiplier, self.exp_max, self.initial_interval, self.jitter
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Translated from `ExponentialBackoffTest.testExponentialBackoff`
    #[test]
    fn test_exponential_backoff() {
        let scale_factor: i64 = 100;
        let ratio: i32 = 2;
        let backoff_max: i64 = 2000;
        let jitter: f64 = 0.2;
        let exponential_backoff = ExponentialBackoff::new(scale_factor, ratio, backoff_max, jitter).unwrap();

        for _i in 0..=100 {
            for attempts in 0..=10 {
                if attempts <= 4 {
                    let expected = scale_factor as f64 * (ratio as f64).powi(attempts);
                    let tolerance = expected * jitter;
                    let actual = exponential_backoff.backoff(attempts as i64);
                    assert!(
                        (actual as f64 - expected).abs() <= tolerance,
                        "Expected backoff({}) ~= {} +/- {}, got {}",
                        attempts,
                        expected,
                        tolerance,
                        actual
                    );
                } else {
                    let actual = exponential_backoff.backoff(attempts as i64);
                    assert!(
                        actual as f64 <= backoff_max as f64 * (1.0 + jitter),
                        "Expected backoff({}) <= {}, got {}",
                        attempts,
                        backoff_max as f64 * (1.0 + jitter),
                        actual
                    );
                }
            }
        }
    }

    /// Translated from `ExponentialBackoffTest.testExponentialBackoffWithoutJitter`
    #[test]
    fn test_exponential_backoff_without_jitter() {
        let exponential_backoff = ExponentialBackoff::new(100, 2, 400, 0.0).unwrap();
        assert_eq!(100, exponential_backoff.backoff(0));
        assert_eq!(200, exponential_backoff.backoff(1));
        assert_eq!(400, exponential_backoff.backoff(2));
        assert_eq!(400, exponential_backoff.backoff(3));
    }

    /// Translated from `ExponentialBackoffTest.testExponentialBackoffWithInvalidJitter`
    #[test]
    fn test_exponential_backoff_with_invalid_jitter() {
        let err = ExponentialBackoff::new(100, 2, 400, -1.0).unwrap_err();
        assert_eq!("jitter must be between 0 and 1, but got -1", err);

        let err = ExponentialBackoff::new(100, 2, 400, 3000.0).unwrap_err();
        assert_eq!("jitter must be between 0 and 1, but got 3000", err);
    }
}
