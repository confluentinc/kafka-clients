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

//! Share fetch acquire mode (KIP-1206).
//!
//! Corresponds to `org.apache.kafka.clients.consumer.internals.ShareAcquireMode`.

// Phase 1 (M9) translates the share wire/session layer; the config wiring and
// share fetch path that consume the remaining accessors land in a later phase.
#![allow(dead_code)]

use crate::common::KafkaError;

/// The acquire mode controls the fetch behavior of a share consumer.
///
/// Corresponds to `org.apache.kafka.clients.consumer.internals.ShareAcquireMode`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum ShareAcquireMode {
    /// Batch-optimized acquisition.
    BatchOptimized,
    /// Record-limit acquisition.
    RecordLimit,
}

impl ShareAcquireMode {
    /// Returns the configuration string name of this acquire mode
    /// (the Java `public final String name` field).
    pub(crate) fn name(&self) -> &'static str {
        match self {
            Self::BatchOptimized => "batch_optimized",
            Self::RecordLimit => "record_limit",
        }
    }

    /// Returns the wire id for this acquire mode (the Java `byte id()`).
    pub(crate) fn id(&self) -> i8 {
        match self {
            Self::BatchOptimized => 0,
            Self::RecordLimit => 1,
        }
    }

    /// Case-insensitive acquire mode lookup by string name.
    ///
    /// Corresponds to Java's `ShareAcquireMode.of(String name)`.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::IllegalArgument`] for a null-equivalent empty
    /// value or an unrecognized name, matching Java's `IllegalArgumentException`.
    pub(crate) fn of(name: &str) -> Result<Self, KafkaError> {
        match name.to_ascii_uppercase().as_str() {
            "BATCH_OPTIMIZED" => Ok(Self::BatchOptimized),
            "RECORD_LIMIT" => Ok(Self::RecordLimit),
            _ => Err(KafkaError::illegal_argument(format!(
                "Invalid value `{name}` for configuration {name}. The value must either be \
                 'batch_optimized' or 'record_limit'."
            ))),
        }
    }

    /// Returns the acquire mode for the given wire id.
    ///
    /// Corresponds to Java's `ShareAcquireMode.forId(byte id)`.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::IllegalArgument`] for an unknown id, matching
    /// Java's `IllegalArgumentException`.
    pub(crate) fn for_id(id: i8) -> Result<Self, KafkaError> {
        match id {
            0 => Ok(Self::BatchOptimized),
            1 => Ok(Self::RecordLimit),
            other => Err(KafkaError::illegal_argument(format!("Unknown share acquire mode id: {other}"))),
        }
    }
}

impl std::fmt::Display for ShareAcquireMode {
    /// Matches Java's `toString()`, which returns the configuration name.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_id_and_name() {
        assert_eq!(ShareAcquireMode::BatchOptimized.id(), 0);
        assert_eq!(ShareAcquireMode::RecordLimit.id(), 1);
        assert_eq!(ShareAcquireMode::BatchOptimized.name(), "batch_optimized");
        assert_eq!(ShareAcquireMode::RecordLimit.name(), "record_limit");
    }

    #[test]
    fn test_of_case_insensitive() {
        assert_eq!(
            ShareAcquireMode::of("batch_optimized").unwrap(),
            ShareAcquireMode::BatchOptimized
        );
        assert_eq!(ShareAcquireMode::of("RECORD_LIMIT").unwrap(), ShareAcquireMode::RecordLimit);
    }

    #[test]
    fn test_of_invalid() {
        let err = ShareAcquireMode::of("nope").expect_err("invalid mode must be rejected");
        assert!(
            err.to_string().contains("must either be 'batch_optimized' or 'record_limit'"),
            "got: {err}"
        );
    }

    #[test]
    fn test_for_id_round_trip() {
        for m in [ShareAcquireMode::BatchOptimized, ShareAcquireMode::RecordLimit] {
            assert_eq!(ShareAcquireMode::for_id(m.id()).unwrap(), m);
        }
    }

    #[test]
    fn test_for_id_unknown() {
        let err = ShareAcquireMode::for_id(9).expect_err("unknown id must be rejected");
        assert!(err.to_string().contains("Unknown share acquire mode id: 9"), "got: {err}");
    }
}
