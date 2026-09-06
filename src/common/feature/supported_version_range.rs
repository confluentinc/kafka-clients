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

//! An immutable version range representing the min/max versions for a supported feature.
//!
//! Translated from `org.apache.kafka.common.feature.SupportedVersionRange` and its
//! base class `org.apache.kafka.common.feature.BaseVersionRange`.

use std::collections::HashMap;
use std::fmt;

/// Label for the min version key, used only for map conversion.
const MIN_VERSION_KEY_LABEL: &str = "min_version";

/// Label for the max version key, used only for map conversion.
const MAX_VERSION_KEY_LABEL: &str = "max_version";

/// An immutable version range representing the min/max versions for a supported feature.
///
/// The min and max values must satisfy:
/// - Both must be >= 0 (only non-negative version values are valid).
/// - max must be >= min.
#[derive(Debug, Clone, Copy)]
pub struct SupportedVersionRange {
    min_value: i16,
    max_value: i16,
}

impl SupportedVersionRange {
    /// Creates a new `SupportedVersionRange` with the given min and max versions.
    ///
    /// # Errors
    /// Returns an error if `min_version < 0`, `max_version < 0`, or `max_version < min_version`.
    pub fn new_min_version(min_version: i16, max_version: i16) -> Result<Self, String> {
        if min_version < 0 || max_version < 0 || max_version < min_version {
            return Err(format!(
                "Expected minValue >= 0, maxValue >= 0 and maxValue >= minValue, \
                 but received minValue: {}, maxValue: {}",
                min_version, max_version
            ));
        }
        Ok(Self { min_value: min_version, max_value: max_version })
    }

    /// Creates a new `SupportedVersionRange` with min_version = 0 and the given max version.
    ///
    /// # Errors
    /// Returns an error if `max_version < 0`.
    pub fn new(max_version: i16) -> Result<Self, String> {
        Self::new_min_version(0, max_version)
    }

    /// Creates a `SupportedVersionRange` from a map with `min_version` and `max_version` keys.
    ///
    /// # Errors
    /// Returns an error if required keys are missing or the values are invalid.
    pub fn from_map(version_range_map: &HashMap<&str, i16>) -> Result<Self, String> {
        let min = value_or_err(MIN_VERSION_KEY_LABEL, version_range_map)?;
        let max = value_or_err(MAX_VERSION_KEY_LABEL, version_range_map)?;
        Self::new_min_version(min, max)
    }

    /// Returns the minimum version.
    pub fn min(&self) -> i16 {
        self.min_value
    }

    /// Returns the maximum version.
    pub fn max(&self) -> i16 {
        self.max_value
    }

    /// Converts this version range to a map with `min_version` and `max_version` keys.
    pub fn to_map(&self) -> HashMap<&'static str, i16> {
        let mut map = HashMap::new();
        map.insert(MIN_VERSION_KEY_LABEL, self.min_value);
        map.insert(MAX_VERSION_KEY_LABEL, self.max_value);
        map
    }

    /// Checks if the version level does *NOT* fall within the [min, max] range of this
    /// `SupportedVersionRange`.
    ///
    /// Returns `true` if the version is incompatible (outside the range), `false` otherwise.
    pub fn is_incompatible_with(&self, version: i16) -> bool {
        self.min_value > version || self.max_value < version
    }
}

impl PartialEq for SupportedVersionRange {
    fn eq(&self, other: &Self) -> bool {
        self.min_value == other.min_value && self.max_value == other.max_value
    }
}

impl Eq for SupportedVersionRange {}

impl std::hash::Hash for SupportedVersionRange {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.min_value.hash(state);
        self.max_value.hash(state);
    }
}

impl fmt::Display for SupportedVersionRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "SupportedVersionRange[{}:{}, {}:{}]",
            MIN_VERSION_KEY_LABEL, self.min_value, MAX_VERSION_KEY_LABEL, self.max_value
        )
    }
}

/// Looks up a key in the map, returning an error if absent.
fn value_or_err(key: &str, map: &HashMap<&str, i16>) -> Result<i16, String> {
    map.get(key).copied().ok_or_else(|| {
        let map_str: String = map.iter().map(|(k, v)| format!("{}:{}", k, v)).collect::<Vec<_>>().join(", ");
        format!("{} absent in [{}]", key, map_str)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Translated from `SupportedVersionRangeTest.testFailDueToInvalidParams`
    #[test]
    fn test_fail_due_to_invalid_params() {
        // min and max can't be < 0.
        assert!(SupportedVersionRange::new_min_version(-1, -1).is_err());
        // min can't be < 0.
        assert!(SupportedVersionRange::new_min_version(-1, 0).is_err());
        // max can't be < 0.
        assert!(SupportedVersionRange::new_min_version(0, -1).is_err());
        // min can't be > max.
        assert!(SupportedVersionRange::new_min_version(2, 1).is_err());
    }

    /// Translated from `SupportedVersionRangeTest.testFromToMap`
    #[test]
    fn test_from_to_map() {
        let version_range = SupportedVersionRange::new_min_version(1, 2).unwrap();
        assert_eq!(1, version_range.min());
        assert_eq!(2, version_range.max());

        let version_range_map = version_range.to_map();
        let mut expected_map = HashMap::new();
        expected_map.insert("min_version", version_range.min());
        expected_map.insert("max_version", version_range.max());
        assert_eq!(expected_map, version_range_map);

        let new_version_range = SupportedVersionRange::from_map(&version_range_map).unwrap();
        assert_eq!(1, new_version_range.min());
        assert_eq!(2, new_version_range.max());
        assert_eq!(version_range, new_version_range);
    }

    /// Translated from `SupportedVersionRangeTest.testFromMapFailure`
    #[test]
    fn test_from_map_failure() {
        // min_version can't be < 0.
        let mut invalid = HashMap::new();
        invalid.insert("min_version", -1_i16);
        invalid.insert("max_version", 0_i16);
        assert!(SupportedVersionRange::from_map(&invalid).is_err());

        // max_version can't be < 0.
        let mut invalid = HashMap::new();
        invalid.insert("min_version", 0_i16);
        invalid.insert("max_version", -1_i16);
        assert!(SupportedVersionRange::from_map(&invalid).is_err());

        // min_version and max_version can't be < 0.
        let mut invalid = HashMap::new();
        invalid.insert("min_version", -1_i16);
        invalid.insert("max_version", -1_i16);
        assert!(SupportedVersionRange::from_map(&invalid).is_err());

        // min_version can't be > max_version.
        let mut invalid = HashMap::new();
        invalid.insert("min_version", 2_i16);
        invalid.insert("max_version", 1_i16);
        assert!(SupportedVersionRange::from_map(&invalid).is_err());

        // min_version key missing.
        let mut invalid = HashMap::new();
        invalid.insert("max_version", 1_i16);
        assert!(SupportedVersionRange::from_map(&invalid).is_err());

        // max_version key missing.
        let mut invalid = HashMap::new();
        invalid.insert("min_version", 1_i16);
        assert!(SupportedVersionRange::from_map(&invalid).is_err());
    }

    /// Translated from `SupportedVersionRangeTest.testToString`
    #[test]
    fn test_to_string() {
        assert_eq!(
            "SupportedVersionRange[min_version:1, max_version:1]",
            SupportedVersionRange::new_min_version(1, 1).unwrap().to_string()
        );
        assert_eq!(
            "SupportedVersionRange[min_version:1, max_version:2]",
            SupportedVersionRange::new_min_version(1, 2).unwrap().to_string()
        );
    }

    /// Translated from `SupportedVersionRangeTest.testEquals`
    #[test]
    fn test_equals() {
        let tested = SupportedVersionRange::new_min_version(1, 1).unwrap();
        assert_eq!(tested, tested);
        assert_ne!(SupportedVersionRange::new_min_version(1, 2).unwrap(), tested);
    }

    /// Translated from `SupportedVersionRangeTest.testMinMax`
    #[test]
    fn test_min_max() {
        let version_range = SupportedVersionRange::new_min_version(1, 2).unwrap();
        assert_eq!(1, version_range.min());
        assert_eq!(2, version_range.max());
    }

    /// Translated from `SupportedVersionRangeTest.testIsIncompatibleWith`
    #[test]
    fn test_is_incompatible_with() {
        assert!(!SupportedVersionRange::new_min_version(1, 1).unwrap().is_incompatible_with(1));
        assert!(!SupportedVersionRange::new_min_version(1, 4).unwrap().is_incompatible_with(2));
        assert!(!SupportedVersionRange::new_min_version(1, 4).unwrap().is_incompatible_with(1));
        assert!(!SupportedVersionRange::new_min_version(1, 4).unwrap().is_incompatible_with(4));

        assert!(SupportedVersionRange::new_min_version(2, 3).unwrap().is_incompatible_with(1));
        assert!(SupportedVersionRange::new_min_version(2, 3).unwrap().is_incompatible_with(4));
    }
}
