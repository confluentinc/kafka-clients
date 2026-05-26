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

//! Translation of `org.apache.kafka.common.utils.ExponentialBackoff`.

use rand::Rng;

/// Computes exponential retry / reconnect backoff with optional jitter.
///
/// `Backoff(attempts) = random(1 - jitter, 1 + jitter) * initial * multiplier ^ attempts`.
/// If `max_interval` is less than `initial_interval`, a constant backoff of
/// `max_interval` is returned. The jitter never causes the backoff to exceed
/// `max_interval`.
///
/// This struct is thread-safe (immutable after construction; the random
/// source is per-thread via `rand::thread_rng()`).
#[derive(Clone, Debug)]
pub struct ExponentialBackoff {
    initial_interval: i64,
    multiplier: i32,
    max_interval: i64,
    jitter: f64,
    exp_max: f64,
}

impl ExponentialBackoff {
    /// Construct a new backoff strategy. Mirrors Java's
    /// `new ExponentialBackoff(long, int, long, double)`.
    ///
    /// # Errors
    ///
    /// Returns [`String`] (matching the Java `IllegalArgumentException`
    /// message) when `jitter` is outside `[0, 1]`. Java raises an unchecked
    /// exception; we surface it as a `Result` so the producer config
    /// validator can compose this with other config errors instead of
    /// panicking on user input.
    pub fn new(initial_interval: i64, multiplier: i32, max_interval: i64, jitter: f64) -> Result<Self, String> {
        if !(0.0..=1.0).contains(&jitter) {
            return Err(format!("jitter must be between 0 and 1, but got {jitter:?}"));
        }
        let initial_interval = initial_interval.min(max_interval);
        let exp_max = if max_interval > initial_interval {
            (max_interval as f64 / initial_interval.max(1) as f64).ln() / (multiplier as f64).ln()
        } else {
            0.0
        };
        Ok(ExponentialBackoff { initial_interval, multiplier, max_interval, jitter, exp_max })
    }

    /// The initial (capped) interval, mirroring Java's `initialInterval()`.
    pub fn initial_interval(&self) -> i64 {
        self.initial_interval
    }

    /// Compute the backoff for the given number of attempts.
    pub fn backoff(&self, attempts: i64) -> i64 {
        if self.exp_max == 0.0 {
            return self.initial_interval;
        }
        let exp = (attempts as f64).min(self.exp_max);
        let term = (self.initial_interval as f64) * (self.multiplier as f64).powf(exp);
        let random_factor = if self.jitter < f64::MIN_POSITIVE {
            1.0
        } else {
            rand::rng().random_range((1.0 - self.jitter)..(1.0 + self.jitter))
        };
        let backoff_value = (random_factor * term) as i64;
        backoff_value.min(self.max_interval)
    }
}

#[cfg(test)]
mod tests {
    // Translation of `ExponentialBackoffTest`.

    use super::*;

    /// Java: `testExponentialBackoff`.
    #[test]
    fn exponential_backoff_with_jitter() {
        let scale_factor: i64 = 100;
        let ratio = 2;
        let backoff_max: i64 = 2000;
        let jitter = 0.2;
        let exp = ExponentialBackoff::new(scale_factor, ratio, backoff_max, jitter).unwrap();

        for _ in 0..=100 {
            for attempts in 0..=10 {
                let backoff = exp.backoff(attempts);
                if attempts <= 4 {
                    let expected = scale_factor as f64 * (ratio as f64).powi(attempts as i32);
                    let tolerance = expected * jitter;
                    let diff = (backoff as f64 - expected).abs();
                    assert!(
                        diff <= tolerance + 1.0,
                        "attempts={attempts}, backoff={backoff}, expected={expected}, tol={tolerance}"
                    );
                } else {
                    assert!(
                        (backoff as f64) <= (backoff_max as f64) * (1.0 + jitter),
                        "attempts={attempts}, backoff={backoff} exceeds cap"
                    );
                }
            }
        }
    }

    /// Java: `testExponentialBackoffWithoutJitter`.
    #[test]
    fn exponential_backoff_without_jitter() {
        let exp = ExponentialBackoff::new(100, 2, 400, 0.0).unwrap();
        assert_eq!(exp.backoff(0), 100);
        assert_eq!(exp.backoff(1), 200);
        assert_eq!(exp.backoff(2), 400);
        assert_eq!(exp.backoff(3), 400);
    }

    /// Java: `testExponentialBackoffWithInvalidJitter`.
    #[test]
    fn exponential_backoff_with_invalid_jitter() {
        let err = ExponentialBackoff::new(100, 2, 400, -1.0).unwrap_err();
        assert_eq!(err, "jitter must be between 0 and 1, but got -1.0");
        let err = ExponentialBackoff::new(100, 2, 400, 3000.0).unwrap_err();
        assert_eq!(err, "jitter must be between 0 and 1, but got 3000.0");
    }
}
