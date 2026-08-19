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

//! An upper or lower bound for metrics (`org.apache.kafka.common.metrics.Quota`).

/// An upper or lower bound for metrics.
#[derive(Clone, Copy, Debug)]
pub struct Quota {
    upper: bool,
    bound: f64,
}

impl Quota {
    /// Create a quota with the given bound and whether it is an upper bound.
    pub fn new(bound: f64, upper: bool) -> Self {
        Self { bound, upper }
    }

    /// Create an upper-bound quota.
    pub fn upper_bound(upper_bound: f64) -> Self {
        Self::new(upper_bound, true)
    }

    /// Create a lower-bound quota.
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

    /// Whether the given value is acceptable under this quota.
    pub fn acceptable(&self, value: f64) -> bool {
        (self.upper && value <= self.bound) || (!self.upper && value >= self.bound)
    }
}

impl PartialEq for Quota {
    fn eq(&self, other: &Self) -> bool {
        // Mirrors Java: equal if bound and upper match.
        self.bound == other.bound && self.upper == other.upper
    }
}

impl Eq for Quota {}

impl std::fmt::Display for Quota {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.upper {
            write!(f, "upper={}", self.bound)
        } else {
            write!(f, "lower={}", self.bound)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quotas_equality() {
        let quota1 = Quota::upper_bound(10.5);
        let quota2 = Quota::upper_bound(10.5);
        assert_eq!(quota1, quota2, "Quota with same upper bound should be equal");

        let quota3 = Quota::lower_bound(10.5);
        let quota4 = Quota::lower_bound(10.5);
        assert_eq!(quota3, quota4, "Quota with same lower bound should be equal");

        assert_ne!(
            quota1, quota3,
            "Quota with same bound but different upper/lower should not be equal"
        );
    }

    #[test]
    fn test_acceptable() {
        let upper = Quota::upper_bound(5.0);
        assert!(upper.acceptable(5.0));
        assert!(upper.acceptable(4.0));
        assert!(!upper.acceptable(6.0));

        let lower = Quota::lower_bound(5.0);
        assert!(lower.acceptable(5.0));
        assert!(lower.acceptable(6.0));
        assert!(!lower.acceptable(4.0));
    }
}
