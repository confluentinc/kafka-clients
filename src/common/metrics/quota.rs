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

//! An upper or lower bound for a metric.
//!
//! Translated from `org.apache.kafka.common.metrics.Quota`.

use std::fmt;

use crate::common::utils::double_to_string;

/// An upper or lower bound for a metric.
#[derive(Clone, Copy, Debug)]
pub struct Quota {
    upper: bool,
    bound: f64,
}

impl Quota {
    /// Creates a new quota with the given bound and direction.
    pub fn new(bound: f64, upper: bool) -> Self {
        Self { upper, bound }
    }

    /// Creates an upper-bound quota.
    pub fn upper_bound(upper_bound: f64) -> Self {
        Self::new(upper_bound, true)
    }

    /// Creates a lower-bound quota.
    pub fn lower_bound(lower_bound: f64) -> Self {
        Self::new(lower_bound, false)
    }

    /// Whether this quota is an upper bound.
    pub fn is_upper_bound(&self) -> bool {
        self.upper
    }

    /// The bound value.
    pub fn bound(&self) -> f64 {
        self.bound
    }

    /// Whether the given value satisfies this quota.
    pub fn acceptable(&self, value: f64) -> bool {
        (self.upper && value <= self.bound) || (!self.upper && value >= self.bound)
    }
}

impl PartialEq for Quota {
    fn eq(&self, other: &Self) -> bool {
        self.bound == other.bound && self.upper == other.upper
    }
}

impl fmt::Display for Quota {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}{}",
            if self.upper { "upper=" } else { "lower=" },
            double_to_string(self.bound)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_upper_bound_acceptable() {
        let quota = Quota::upper_bound(5.0);
        assert!(quota.is_upper_bound());
        assert_eq!(quota.bound(), 5.0);
        assert!(quota.acceptable(4.0));
        assert!(quota.acceptable(5.0));
        assert!(!quota.acceptable(6.0));
    }

    #[test]
    fn test_lower_bound_acceptable() {
        let quota = Quota::lower_bound(5.0);
        assert!(!quota.is_upper_bound());
        assert!(quota.acceptable(6.0));
        assert!(quota.acceptable(5.0));
        assert!(!quota.acceptable(4.0));
    }

    #[test]
    fn test_equality_and_display() {
        assert_eq!(Quota::upper_bound(5.0), Quota::new(5.0, true));
        assert_ne!(Quota::upper_bound(5.0), Quota::lower_bound(5.0));
        assert_eq!(Quota::upper_bound(5.0).to_string(), "upper=5.0");
        assert_eq!(Quota::lower_bound(2.5).to_string(), "lower=2.5");
    }
}
