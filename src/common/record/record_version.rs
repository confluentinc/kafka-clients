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

//! Translation of `org.apache.kafka.common.record.RecordVersion`.

use crate::common::errors::KafkaError;

/// Defines the record format versions supported by Kafka.
///
/// For historical reasons, the record format version is also known as `magic`
/// and `message format version`. Note that the version actually applies to the
/// `RecordBatch` (instead of the `Record`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RecordVersion {
    V0,
    V1,
    V2,
}

impl RecordVersion {
    /// The numeric value (`magic` byte) for this version.
    pub fn value(&self) -> i8 {
        match self {
            RecordVersion::V0 => 0,
            RecordVersion::V1 => 1,
            RecordVersion::V2 => 2,
        }
    }

    /// Look up a `RecordVersion` from its raw `magic` byte value.
    ///
    /// Mirrors Java's `RecordVersion.lookup(byte)` — Java throws
    /// `IllegalArgumentException` for unknown values; we surface that as a
    /// [`KafkaError::InvalidRecord`].
    pub fn lookup(value: i8) -> Result<RecordVersion, KafkaError> {
        match value {
            0 => Ok(RecordVersion::V0),
            1 => Ok(RecordVersion::V1),
            2 => Ok(RecordVersion::V2),
            _ => Err(KafkaError::InvalidRecord(format!("Unknown record version: {value}"))),
        }
    }

    /// The current record format version. Mirrors Java's `RecordVersion.current()`.
    pub fn current() -> RecordVersion {
        RecordVersion::V2
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lookup_known_versions() {
        assert_eq!(RecordVersion::lookup(0).unwrap(), RecordVersion::V0);
        assert_eq!(RecordVersion::lookup(1).unwrap(), RecordVersion::V1);
        assert_eq!(RecordVersion::lookup(2).unwrap(), RecordVersion::V2);
    }

    #[test]
    fn lookup_unknown_returns_error() {
        let err = RecordVersion::lookup(3).unwrap_err();
        assert!(matches!(err, KafkaError::InvalidRecord(_)));
        assert!(err.to_string().contains("Unknown record version: 3"));

        let err = RecordVersion::lookup(-1).unwrap_err();
        assert!(matches!(err, KafkaError::InvalidRecord(_)));
    }

    #[test]
    fn value_returns_magic_byte() {
        assert_eq!(RecordVersion::V0.value(), 0);
        assert_eq!(RecordVersion::V1.value(), 1);
        assert_eq!(RecordVersion::V2.value(), 2);
    }

    #[test]
    fn current_is_v2() {
        assert_eq!(RecordVersion::current(), RecordVersion::V2);
    }
}
