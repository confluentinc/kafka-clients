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

//! Producer ID and epoch pair.

use std::fmt;

use crate::common::record::RecordBatch;

/// A producer ID and epoch pair, identifying a producer session to the broker.
///
/// Translated from `org.apache.kafka.common.utils.ProducerIdAndEpoch`.
///
/// `producer_id` is `i64` (Java `long`) rather than `u64`: it is compared
/// against the [`RecordBatch::NO_PRODUCER_ID`] sentinel (`-1`), and signed
/// versus unsigned comparison would order high-bit values differently.
///
/// This type is two scalars, so it is `Copy` and lives on the stack — Java
/// allocates an object here, but there is nothing to own.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ProducerIdAndEpoch {
    /// The producer ID assigned by the broker, or [`RecordBatch::NO_PRODUCER_ID`].
    pub producer_id: i64,
    /// The producer epoch, or [`RecordBatch::NO_PRODUCER_EPOCH`].
    pub epoch: i16,
}

impl ProducerIdAndEpoch {
    /// The sentinel pair for a producer with no assigned ID.
    ///
    /// Corresponds to Java's `ProducerIdAndEpoch.NONE`.
    pub const NONE: Self = Self { producer_id: RecordBatch::NO_PRODUCER_ID, epoch: RecordBatch::NO_PRODUCER_EPOCH };

    /// Creates a new producer ID and epoch pair.
    pub const fn new(producer_id: i64, epoch: i16) -> Self {
        Self { producer_id, epoch }
    }

    /// Whether this pair identifies a real producer session.
    ///
    /// Note this compares `<` against the sentinel rather than testing
    /// inequality with [`Self::NONE`], matching Java's `isValid()`: any
    /// producer ID above `NO_PRODUCER_ID` is valid regardless of epoch.
    pub fn is_valid(&self) -> bool {
        RecordBatch::NO_PRODUCER_ID < self.producer_id
    }
}

impl fmt::Display for ProducerIdAndEpoch {
    /// Formats as `(producerId=<id>, epoch=<epoch>)`.
    ///
    /// The exact form matters: it appears in log output that some
    /// `TransactionManagerTest` assertions compare against.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(producerId={}, epoch={})", self.producer_id, self.epoch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_none_uses_record_batch_sentinels() {
        assert_eq!(ProducerIdAndEpoch::NONE.producer_id, RecordBatch::NO_PRODUCER_ID);
        assert_eq!(ProducerIdAndEpoch::NONE.epoch, RecordBatch::NO_PRODUCER_EPOCH);
    }

    #[test]
    fn test_none_is_not_valid() {
        assert!(!ProducerIdAndEpoch::NONE.is_valid());
    }

    #[test]
    fn test_is_valid_compares_against_sentinel_not_none() {
        // Java compares `NO_PRODUCER_ID < producerId`, so a valid producer id
        // with the sentinel epoch is still valid — it is not an equality test
        // against NONE.
        let pid = ProducerIdAndEpoch::new(0, RecordBatch::NO_PRODUCER_EPOCH);
        assert_ne!(pid, ProducerIdAndEpoch::NONE);
        assert!(pid.is_valid());
    }

    #[test]
    fn test_is_valid_boundary() {
        // -1 is the sentinel: not valid. 0 and above are valid.
        assert!(!ProducerIdAndEpoch::new(-1, 0).is_valid());
        assert!(ProducerIdAndEpoch::new(0, 0).is_valid());
        assert!(ProducerIdAndEpoch::new(1, 0).is_valid());
        assert!(ProducerIdAndEpoch::new(i64::MAX, 0).is_valid());
    }

    #[test]
    fn test_is_valid_negative_below_sentinel() {
        // Anything below the sentinel is also invalid, since the comparison is
        // `NO_PRODUCER_ID < producerId`.
        assert!(!ProducerIdAndEpoch::new(-2, 0).is_valid());
        assert!(!ProducerIdAndEpoch::new(i64::MIN, 0).is_valid());
    }

    #[test]
    fn test_equality_covers_both_fields() {
        let a = ProducerIdAndEpoch::new(1, 5);
        assert_eq!(a, ProducerIdAndEpoch::new(1, 5));
        assert_ne!(a, ProducerIdAndEpoch::new(1, 6));
        assert_ne!(a, ProducerIdAndEpoch::new(2, 5));
    }

    #[test]
    fn test_display_format() {
        assert_eq!(ProducerIdAndEpoch::new(42, 7).to_string(), "(producerId=42, epoch=7)");
        assert_eq!(ProducerIdAndEpoch::NONE.to_string(), "(producerId=-1, epoch=-1)");
    }

    #[test]
    fn test_is_copy() {
        // Guards CLAUDE.md §11: the type must stay stack-allocated and cheap to
        // pass by value. If `Copy` is ever dropped this stops compiling.
        let a = ProducerIdAndEpoch::new(1, 2);
        let b = a;
        assert_eq!(a, b);
    }
}
