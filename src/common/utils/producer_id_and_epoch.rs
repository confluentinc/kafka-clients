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

//! A producer id paired with its epoch.
//!
//! Corresponds to `org.apache.kafka.common.utils.ProducerIdAndEpoch`.

use crate::common::record::RecordBatch;

/// A producer id and its epoch.
///
/// Corresponds to `org.apache.kafka.common.utils.ProducerIdAndEpoch`. The
/// `producer_id` is `i64` and `epoch` is `i16` (Java `long` / `short`), matching
/// the wire representation and the signed comparison semantics required by
/// CLAUDE.md.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ProducerIdAndEpoch {
    /// The producer id.
    pub producer_id: i64,
    /// The producer epoch.
    pub epoch: i16,
}

impl ProducerIdAndEpoch {
    /// The sentinel "no producer id / epoch" value.
    ///
    /// Mirrors `ProducerIdAndEpoch.NONE`.
    pub const NONE: ProducerIdAndEpoch =
        ProducerIdAndEpoch { producer_id: RecordBatch::NO_PRODUCER_ID, epoch: RecordBatch::NO_PRODUCER_EPOCH };

    /// Creates a new `ProducerIdAndEpoch`.
    pub fn new(producer_id: i64, epoch: i16) -> Self {
        Self { producer_id, epoch }
    }

    /// Whether this represents a valid producer id.
    ///
    /// Mirrors `ProducerIdAndEpoch.isValid`.
    pub fn is_valid(&self) -> bool {
        RecordBatch::NO_PRODUCER_ID < self.producer_id
    }
}

impl std::fmt::Display for ProducerIdAndEpoch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "(producerId={}, epoch={})", self.producer_id, self.epoch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn none_is_invalid() {
        assert!(!ProducerIdAndEpoch::NONE.is_valid());
        assert_eq!(ProducerIdAndEpoch::NONE.producer_id, RecordBatch::NO_PRODUCER_ID);
        assert_eq!(ProducerIdAndEpoch::NONE.epoch, RecordBatch::NO_PRODUCER_EPOCH);
    }

    #[test]
    fn valid_when_producer_id_positive() {
        assert!(ProducerIdAndEpoch::new(0, 0).is_valid());
        assert!(ProducerIdAndEpoch::new(1, 5).is_valid());
        assert!(!ProducerIdAndEpoch::new(-1, 5).is_valid());
    }

    #[test]
    fn equality_uses_both_fields() {
        assert_eq!(ProducerIdAndEpoch::new(1, 2), ProducerIdAndEpoch::new(1, 2));
        assert_ne!(ProducerIdAndEpoch::new(1, 2), ProducerIdAndEpoch::new(1, 3));
        assert_ne!(ProducerIdAndEpoch::new(1, 2), ProducerIdAndEpoch::new(2, 2));
    }

    #[test]
    fn display_matches_java() {
        assert_eq!(ProducerIdAndEpoch::new(42, 7).to_string(), "(producerId=42, epoch=7)");
    }
}
