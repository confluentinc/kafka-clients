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

//! A range of versions that a particular broker supports for a feature.

use crate::common::Error;

/// Represents a range of versions that a particular broker supports for some
/// feature.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[doc(alias = "org.apache.kafka.clients.admin.SupportedVersionRange")]
pub struct SupportedVersionRange {
    min_version: i16,
    max_version: i16,
}

impl SupportedVersionRange {
    /// Raises an error unless the following conditions are met: `0 <= min_version <=
    /// max_version`.
    ///
    /// # Arguments
    ///
    /// - `min_version` - The minimum version value.
    /// - `max_version` - The maximum version value.
    ///
    /// # Errors
    ///
    /// - `Error::is_local_illegal_argument_error` Raised when the condition described above is not
    ///   met.
    #[doc(alias = "org.apache.kafka.clients.admin.SupportedVersionRange#SupportedVersionRange")]
    pub fn new(min_version: i16, max_version: i16) -> Result<Self, Error> {
        if min_version < 0 || max_version < 0 || max_version < min_version {
            return Err(Error::local_illegal_argument(format!(
                "Expected 0 <= minVersion <= maxVersion but received minVersion:{min_version}, \
                 maxVersion:{max_version}."
            )));
        }
        Ok(Self { min_version, max_version })
    }

    /// Returns the minimum version value.
    #[doc(alias = "org.apache.kafka.clients.admin.SupportedVersionRange#minVersion")]
    pub fn min_version(&self) -> i16 {
        self.min_version
    }

    /// Returns the maximum version value.
    #[doc(alias = "org.apache.kafka.clients.admin.SupportedVersionRange#maxVersion")]
    pub fn max_version(&self) -> i16 {
        self.max_version
    }
}

impl std::fmt::Display for SupportedVersionRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SupportedVersionRange[min_version:{}, max_version:{}]",
            self.min_version, self.max_version
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_valid_range() {
        let range = SupportedVersionRange::new(1, 5).unwrap();
        assert_eq!(range.min_version(), 1);
        assert_eq!(range.max_version(), 5);
    }

    #[test]
    fn new_rejects_negative_and_inverted_ranges() {
        let expected = "Expected 0 <= minVersion <= maxVersion but received minVersion:-1, maxVersion:2.";
        assert_eq!(SupportedVersionRange::new(-1, 2).unwrap_err().message(), expected);
        assert!(SupportedVersionRange::new(5, 2).is_err());
    }

    #[test]
    fn to_string_matches_java() {
        let range = SupportedVersionRange::new(1, 5).unwrap();
        assert_eq!(range.to_string(), "SupportedVersionRange[min_version:1, max_version:5]");
    }
}
