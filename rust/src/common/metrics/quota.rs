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
#[derive(Clone, Debug)]
#[doc(alias = "org.apache.kafka.common.metrics.Quota")]
pub struct Quota {
    upper: bool,
    bound: f64,
}

impl Quota {
    /// Creates a quota with the given bound and whether it is an upper bound.
    ///
    /// # Arguments
    ///
    /// - `bound` - the bound value
    /// - `upper` - `true` for an upper bound, `false` for a lower bound
    #[doc(alias = "org.apache.kafka.common.metrics.Quota#Quota")]
    pub fn new(bound: f64, upper: bool) -> Self {
        Self { bound, upper }
    }

    /// Creates an upper-bound quota.
    ///
    /// # Arguments
    ///
    /// - `upper_bound` - the highest acceptable value
    ///
    /// # Returns
    ///
    /// The upper-bound quota.
    #[doc(alias = "org.apache.kafka.common.metrics.Quota#upperBound")]
    pub fn upper_bound(upper_bound: f64) -> Self {
        Self::new(upper_bound, true)
    }

    /// Creates a lower-bound quota.
    ///
    /// # Arguments
    ///
    /// - `lower_bound` - the lowest acceptable value
    ///
    /// # Returns
    ///
    /// The lower-bound quota.
    #[doc(alias = "org.apache.kafka.common.metrics.Quota#lowerBound")]
    pub fn lower_bound(lower_bound: f64) -> Self {
        Self::new(lower_bound, false)
    }

    /// Returns whether this quota is an upper bound.
    #[doc(alias = "org.apache.kafka.common.metrics.Quota#isUpperBound")]
    pub fn is_upper_bound(&self) -> bool {
        self.upper
    }

    /// Returns the bound value.
    #[doc(alias = "org.apache.kafka.common.metrics.Quota#bound")]
    pub fn bound(&self) -> f64 {
        self.bound
    }

    /// Returns whether the given value is acceptable under this quota.
    ///
    /// # Arguments
    ///
    /// - `value` - the value to check against the bound
    #[doc(alias = "org.apache.kafka.common.metrics.Quota#acceptable")]
    pub fn acceptable(&self, value: f64) -> bool {
        (self.upper && value <= self.bound) || (!self.upper && value >= self.bound)
    }
}

impl Quota {
    /// The bound as Java's `Double.doubleToLongBits` reads it: its bits, with
    /// every `NaN` folded into the canonical one.
    fn bound_bits(&self) -> u64 {
        if self.bound.is_nan() {
            f64::NAN.to_bits()
        } else {
            self.bound.to_bits()
        }
    }
}

/// Java's `Quota.equals` compares `bound` with the primitive `==`, under which
/// `NaN` differs from itself and `0.0` equals `-0.0`. A lawful `Eq` must be
/// reflexive, so the bound is compared as Java's `Double.equals` compares it
/// instead: every `NaN` is equal, and the two zeros differ. Any other bound
/// compares as in Java.
impl PartialEq for Quota {
    fn eq(&self, other: &Self) -> bool {
        self.bound_bits() == other.bound_bits() && self.upper == other.upper
    }
}

impl Eq for Quota {}

/// Hashes what [`PartialEq`] compares. Java hashes `(int) bound`, which would
/// put `0.0` and `-0.0`, now unequal, under one hash; the Rust hash only has
/// to agree with Rust's equality, so it takes the bits.
impl std::hash::Hash for Quota {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.bound_bits().hash(state);
        self.upper.hash(state);
    }
}

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

    // New test, no Java original: the NaN and signed-zero cases where a lawful
    // `Eq` cannot follow Java's primitive `==` (see `PartialEq for Quota`).
    #[test]
    fn test_quota_equality_reads_the_bound_as_double_equals() {
        use std::hash::BuildHasher;
        let state = std::collections::hash_map::RandomState::new();
        let nan = Quota::upper_bound(f64::NAN);
        let other_nan = Quota::upper_bound(f64::from_bits(f64::NAN.to_bits() ^ 1));
        assert_eq!(nan, Quota::upper_bound(f64::NAN));
        assert_eq!(nan, other_nan);
        assert_eq!(state.hash_one(nan), state.hash_one(other_nan));
        assert_ne!(Quota::upper_bound(0.0), Quota::upper_bound(-0.0));
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
