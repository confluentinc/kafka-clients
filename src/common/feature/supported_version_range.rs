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

//! Translation of `org.apache.kafka.common.feature.SupportedVersionRange`
//! (and its abstract parent `BaseVersionRange`).
//!
//! The Java class hierarchy splits the validation logic into a
//! package-private `BaseVersionRange` parent and a public
//! `SupportedVersionRange` subclass. Because we only need the supported
//! variant (`FinalizedVersionRange` is not used on the producer path),
//! the Rust translation collapses the two onto a single struct.

use crate::common::errors::KafkaError;

/// Represents a range of versions that a particular broker supports for
/// some feature. Mirrors the Java `SupportedVersionRange`.
///
/// Construction enforces `0 <= min_version <= max_version`. Out-of-range
/// inputs surface as [`KafkaError::IllegalArgument`] (Java throws
/// `IllegalArgumentException`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupportedVersionRange {
    min_version: i16,
    max_version: i16,
}

impl SupportedVersionRange {
    /// Mirrors `new SupportedVersionRange(short, short)`.
    pub fn new(min_version: i16, max_version: i16) -> Result<Self, KafkaError> {
        if min_version < 0 || max_version < 0 || max_version < min_version {
            return Err(KafkaError::IllegalArgument(format!(
                "Expected minValue >= 0, maxValue >= 0 and maxValue >= minValue, but received minValue: {min_version}, maxValue: {max_version}"
            )));
        }
        Ok(SupportedVersionRange { min_version, max_version })
    }

    /// Mirrors `new SupportedVersionRange(short maxVersion)`.
    pub fn with_max(max_version: i16) -> Result<Self, KafkaError> {
        Self::new(0, max_version)
    }

    /// Mirrors `min()`.
    pub fn min(&self) -> i16 {
        self.min_version
    }

    /// Mirrors `max()`.
    pub fn max(&self) -> i16 {
        self.max_version
    }

    /// Checks if the version level does *NOT* fall within the
    /// `[min, max]` range of this `SupportedVersionRange`. Mirrors
    /// `isIncompatibleWith(short)`.
    pub fn is_incompatible_with(&self, version: i16) -> bool {
        self.min_version > version || self.max_version < version
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let range = SupportedVersionRange::new(0, 2).expect("valid");
        assert_eq!(range.min(), 0);
        assert_eq!(range.max(), 2);
        assert!(!range.is_incompatible_with(0));
        assert!(!range.is_incompatible_with(1));
        assert!(!range.is_incompatible_with(2));
        assert!(range.is_incompatible_with(3));
    }

    #[test]
    fn negative_min_rejected() {
        let err = SupportedVersionRange::new(-1, 1).unwrap_err();
        assert!(matches!(err, KafkaError::IllegalArgument(_)));
    }

    #[test]
    fn max_less_than_min_rejected() {
        let err = SupportedVersionRange::new(2, 1).unwrap_err();
        assert!(matches!(err, KafkaError::IllegalArgument(_)));
    }

    #[test]
    fn with_max_defaults_min_to_zero() {
        let range = SupportedVersionRange::with_max(5).expect("valid");
        assert_eq!(range.min(), 0);
        assert_eq!(range.max(), 5);
    }
}
