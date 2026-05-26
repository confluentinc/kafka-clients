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

//! Translation of `org.apache.kafka.common.utils.ProducerIdAndEpoch`.

use std::fmt;

/// Sentinel: no producer id assigned.
/// Mirrors `RecordBatch.NO_PRODUCER_ID` (defined in `DefaultRecordBatch.java`,
/// translated in Phase 3).
pub const NO_PRODUCER_ID: i64 = -1;

/// Sentinel: no producer epoch assigned.
/// Mirrors `RecordBatch.NO_PRODUCER_EPOCH`.
pub const NO_PRODUCER_EPOCH: i16 = -1;

/// A pair of producer-id + epoch, used to identify and fence transactional
/// or idempotent producers.
///
/// Per CLAUDE.md naming rule: `producer_id` is `i64` (signed) because Java
/// compares it as `long`.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct ProducerIdAndEpoch {
    pub producer_id: i64,
    pub epoch: i16,
}

/// The "unset" sentinel pair, equivalent to Java's `ProducerIdAndEpoch.NONE`.
pub const NONE: ProducerIdAndEpoch = ProducerIdAndEpoch { producer_id: NO_PRODUCER_ID, epoch: NO_PRODUCER_EPOCH };

impl ProducerIdAndEpoch {
    /// Construct a new pair.
    pub const fn new(producer_id: i64, epoch: i16) -> Self {
        ProducerIdAndEpoch { producer_id, epoch }
    }

    /// True iff this pair represents an actually-assigned producer id.
    /// Mirrors Java's `isValid()` (`NO_PRODUCER_ID < producerId`).
    pub fn is_valid(&self) -> bool {
        NO_PRODUCER_ID < self.producer_id
    }
}

impl fmt::Display for ProducerIdAndEpoch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(producerId={}, epoch={})", self.producer_id, self.epoch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn none_is_invalid() {
        assert!(!NONE.is_valid());
        assert_eq!(NONE.producer_id, NO_PRODUCER_ID);
        assert_eq!(NONE.epoch, NO_PRODUCER_EPOCH);
    }

    #[test]
    fn assigned_pid_is_valid() {
        let pid = ProducerIdAndEpoch::new(42, 0);
        assert!(pid.is_valid());
    }

    #[test]
    fn display_matches_java_format() {
        let pid = ProducerIdAndEpoch::new(42, 7);
        assert_eq!(pid.to_string(), "(producerId=42, epoch=7)");
    }
}
