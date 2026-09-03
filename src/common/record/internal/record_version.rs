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

//! Defines the record format versions supported by Kafka.
//!
//! For historical reasons, the record format version is also known as `magic`
//! and `message format version`. Note that the version actually applies to the
//! record batch (instead of the individual record).
//!
//! Corresponds to Java's `org.apache.kafka.common.record.RecordVersion`.

/// Record format versions supported by Kafka.
///
/// Corresponds to Java's `org.apache.kafka.common.record.RecordVersion`.
// `V0`/`V1` are translated for API completeness (DoD #2); only `V2` has a
// crate-internal constructor now that the module is `internal`.
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RecordVersion {
    /// Record version 0 (oldest format).
    V0 = 0,
    /// Record version 1 (added timestamps).
    V1 = 1,
    /// Record version 2 (current format, added headers and idempotent producer support).
    V2 = 2,
}

impl RecordVersion {
    /// The byte value of this record version.
    pub fn value(self) -> i8 {
        self as i8
    }

    /// Look up a `RecordVersion` by byte value.
    ///
    /// Returns `None` if the value is not recognized.
    #[allow(dead_code)]
    pub fn lookup(value: i8) -> Option<Self> {
        match value {
            0 => Some(Self::V0),
            1 => Some(Self::V1),
            2 => Some(Self::V2),
            _ => None,
        }
    }

    /// Returns the current (latest) record version.
    #[allow(dead_code)]
    pub fn current() -> Self {
        Self::V2
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_values() {
        assert_eq!(RecordVersion::V0.value(), 0);
        assert_eq!(RecordVersion::V1.value(), 1);
        assert_eq!(RecordVersion::V2.value(), 2);
    }

    #[test]
    fn test_lookup() {
        assert_eq!(RecordVersion::lookup(0), Some(RecordVersion::V0));
        assert_eq!(RecordVersion::lookup(1), Some(RecordVersion::V1));
        assert_eq!(RecordVersion::lookup(2), Some(RecordVersion::V2));
        assert_eq!(RecordVersion::lookup(-1), None);
        assert_eq!(RecordVersion::lookup(3), None);
    }

    #[test]
    fn test_current() {
        assert_eq!(RecordVersion::current(), RecordVersion::V2);
    }
}
