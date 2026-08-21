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

//! A range of version levels supported by every broker for a feature.
//!
//! Corresponds to `org.apache.kafka.clients.admin.FinalizedVersionRange`.

use crate::common::Error;

/// Represents a range of version levels supported by every broker in a cluster
/// for some feature.
///
/// Corresponds to `org.apache.kafka.clients.admin.FinalizedVersionRange`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct FinalizedVersionRange {
    min_version_level: i16,
    max_version_level: i16,
}

impl FinalizedVersionRange {
    /// Creates a new `FinalizedVersionRange`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::local_illegal_argument`] (mirroring Java's
    /// `IllegalArgumentException`) unless `min_version_level >= 0`,
    /// `max_version_level >= 0`, and `max_version_level >= min_version_level`.
    pub fn new(min_version_level: i16, max_version_level: i16) -> Result<Self, Error> {
        if min_version_level < 0 || max_version_level < 0 || max_version_level < min_version_level {
            return Err(Error::local_illegal_argument(format!(
                "Expected minVersionLevel >= 0, maxVersionLevel >= 0 and maxVersionLevel >= minVersionLevel, but \
                 received minVersionLevel: {min_version_level}, maxVersionLevel: {max_version_level}"
            )));
        }
        Ok(Self { min_version_level, max_version_level })
    }

    /// Returns the minimum version level value.
    pub fn min_version_level(&self) -> i16 {
        self.min_version_level
    }

    /// Returns the maximum version level value.
    pub fn max_version_level(&self) -> i16 {
        self.max_version_level
    }
}

impl std::fmt::Display for FinalizedVersionRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "FinalizedVersionRange[min_version_level:{}, max_version_level:{}]",
            self.min_version_level, self.max_version_level
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_valid_range() {
        let range = FinalizedVersionRange::new(1, 3).unwrap();
        assert_eq!(range.min_version_level(), 1);
        assert_eq!(range.max_version_level(), 3);
    }

    #[test]
    fn new_equal_bounds_is_allowed() {
        let range = FinalizedVersionRange::new(2, 2).unwrap();
        assert_eq!(range.min_version_level(), 2);
        assert_eq!(range.max_version_level(), 2);
    }

    #[test]
    fn new_rejects_negative_and_inverted_ranges() {
        let expected = "Expected minVersionLevel >= 0, maxVersionLevel >= 0 and maxVersionLevel >= minVersionLevel, \
                        but received minVersionLevel: -1, maxVersionLevel: 2";
        assert_eq!(FinalizedVersionRange::new(-1, 2).unwrap_err().message(), expected);
        assert!(FinalizedVersionRange::new(3, 2).is_err());
        assert!(FinalizedVersionRange::new(0, -1).is_err());
    }

    #[test]
    fn to_string_matches_java() {
        let range = FinalizedVersionRange::new(2, 5).unwrap();
        assert_eq!(
            range.to_string(),
            "FinalizedVersionRange[min_version_level:2, max_version_level:5]"
        );
    }
}
