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

//! Container holding an offset and its timestamp.
//!
//! Translated from `org.apache.kafka.clients.consumer.OffsetAndTimestamp`.

use std::fmt;

use crate::common::Error;

/// A container class for offset and timestamp.
///
/// Corresponds to Java's `org.apache.kafka.clients.consumer.OffsetAndTimestamp`.
///
/// **`offsets_for_times` mapping note:** this value is non-nullable, so when
/// it is the value type of an `offsets_for_times` result map, an unresolved
/// partition (queried but with no offset at/after the target time) is
/// **omitted from the map** (key absent) rather than present with a `null`
/// value as in Java. See
/// [`Consumer::offsets_for_times`](crate::consumer::Consumer::offsets_for_times).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct OffsetAndTimestamp {
    timestamp: i64,
    offset: i64,
    leader_epoch: Option<i32>,
}

impl OffsetAndTimestamp {
    /// Create a new `OffsetAndTimestamp` with no leader epoch.
    ///
    /// # Errors
    ///
    /// Returns [`Error::LocalIllegalArgument`] if `offset` or `timestamp` is
    /// negative (matching Java's `IllegalArgumentException`).
    pub fn new(offset: i64, timestamp: i64) -> Result<Self, Error> {
        Self::new_leader_epoch(offset, timestamp, None)
    }

    /// Create a new `OffsetAndTimestamp` with an optional leader epoch.
    ///
    /// # Errors
    ///
    /// Returns [`Error::LocalIllegalArgument`] if `offset` or `timestamp` is
    /// negative (matching Java's `IllegalArgumentException`).
    pub fn new_leader_epoch(offset: i64, timestamp: i64, leader_epoch: Option<i32>) -> Result<Self, Error> {
        if offset < 0 {
            return Err(Error::local_illegal_argument("Invalid negative offset"));
        }
        if timestamp < 0 {
            return Err(Error::local_illegal_argument("Invalid negative timestamp"));
        }
        Ok(Self { offset, timestamp, leader_epoch })
    }

    /// The timestamp.
    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }

    /// The offset.
    pub fn offset(&self) -> i64 {
        self.offset
    }

    /// Get the leader epoch corresponding to the offset that was found (if
    /// one exists). This can be provided to `seek()` to ensure the log hasn't
    /// been truncated prior to fetching.
    pub fn leader_epoch(&self) -> Option<i32> {
        self.leader_epoch
    }
}

impl fmt::Display for OffsetAndTimestamp {
    /// Matches Java's `toString()`:
    /// `(timestamp=N, leaderEpoch=X, offset=M)`
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "(timestamp={}, leaderEpoch={}, offset={})",
            self.timestamp,
            match self.leader_epoch {
                Some(e) => e.to_string(),
                None => "null".to_string(),
            },
            self.offset
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_accessors() {
        let oat = OffsetAndTimestamp::new(10, 100).unwrap();
        assert_eq!(oat.offset(), 10);
        assert_eq!(oat.timestamp(), 100);
        assert_eq!(oat.leader_epoch(), None);
    }

    #[test]
    fn test_with_leader_epoch() {
        let oat = OffsetAndTimestamp::new_leader_epoch(10, 100, Some(5)).unwrap();
        assert_eq!(oat.leader_epoch(), Some(5));
    }

    #[test]
    fn test_invalid_negative_offset() {
        let err = OffsetAndTimestamp::new(-1, 100).unwrap_err();
        assert!(err.message().contains("Invalid negative offset"));
    }

    #[test]
    fn test_invalid_negative_timestamp() {
        let err = OffsetAndTimestamp::new(10, -1).unwrap_err();
        assert!(err.message().contains("Invalid negative timestamp"));
    }

    #[test]
    fn test_display() {
        let oat = OffsetAndTimestamp::new_leader_epoch(10, 100, Some(5)).unwrap();
        assert_eq!(oat.to_string(), "(timestamp=100, leaderEpoch=5, offset=10)");

        let oat = OffsetAndTimestamp::new(10, 100).unwrap();
        assert_eq!(oat.to_string(), "(timestamp=100, leaderEpoch=null, offset=10)");
    }

    #[test]
    fn test_equality() {
        let a = OffsetAndTimestamp::new_leader_epoch(10, 100, Some(5)).unwrap();
        let b = OffsetAndTimestamp::new_leader_epoch(10, 100, Some(5)).unwrap();
        let c = OffsetAndTimestamp::new_leader_epoch(10, 100, Some(6)).unwrap();
        assert_eq!(a, b);
        assert_ne!(a, c);
    }
}
