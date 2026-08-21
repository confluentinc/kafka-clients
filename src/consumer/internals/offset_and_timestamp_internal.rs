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

//! Internal representation of [`OffsetAndTimestamp`] that allows
//! negative timestamps and offset values.
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.OffsetAndTimestampInternal`.
//!
//! # Why this exists
//!
//! Public [`OffsetAndTimestamp`] validates `offset >= 0` and
//! `timestamp >= 0` (matching Java's public constructor). The internal
//! flow that backs `endOffsets` / `beginningOffsets` carries a
//! `ListOffsets` response with `timestamp == -1` (the broker's "no
//! timestamp" sentinel for the LATEST / EARLIEST sentinels). Java
//! sidesteps the public-class validation by routing the internal event
//! payload through this loosely-validated companion type, then converts
//! to the public class only at the `offsetsForTimes` boundary (where
//! the user supplied a real timestamp to search for, so the response
//! timestamp is necessarily non-negative).
//!
//! In Rust, the same separation is needed for the same reason: a
//! `Result<OffsetAndTimestamp, Error>` that fails on `timestamp ==
//! -1` makes the receive path silently produce `None` for every
//! `endOffsets(tp)` and the caller gets a value-omitted-from-map
//! result. See COMMENTS.DONE.1.md Issue 6.

use crate::consumer::OffsetAndTimestamp;

/// Internal counterpart to [`OffsetAndTimestamp`]; allows the
/// broker-returned negative-timestamp sentinels.
///
/// Translates `OffsetAndTimestampInternal` (Apache Kafka
/// `clients/consumer/internals/OffsetAndTimestampInternal.java`).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct OffsetAndTimestampInternal {
    timestamp: i64,
    offset: i64,
    leader_epoch: Option<i32>,
}

impl OffsetAndTimestampInternal {
    /// Constructs a new `OffsetAndTimestampInternal`. Negative values
    /// for `offset` and `timestamp` are accepted (matching Java; the
    /// broker uses `-1` to indicate "no timestamp available" for
    /// `LATEST` / `EARLIEST` lookups).
    pub(crate) fn new(offset: i64, timestamp: i64, leader_epoch: Option<i32>) -> Self {
        Self { offset, timestamp, leader_epoch }
    }

    /// The offset value carried by this entry.
    pub(crate) fn offset(&self) -> i64 {
        self.offset
    }

    /// The timestamp value carried by this entry. May be negative —
    /// `-1` is the broker's sentinel for "no timestamp" on
    /// `LATEST`/`EARLIEST` queries.
    #[allow(dead_code)] // Used by tests and future internal accessors.
    pub(crate) fn timestamp(&self) -> i64 {
        self.timestamp
    }

    /// The leader epoch associated with this offset, if any.
    #[allow(dead_code)] // Used by tests and future internal accessors.
    pub(crate) fn leader_epoch(&self) -> Option<i32> {
        self.leader_epoch
    }

    /// Convert this internal value to the public [`OffsetAndTimestamp`].
    ///
    /// Translates Java's `OffsetAndTimestampInternal::buildOffsetAndTimestamp`.
    ///
    /// # Errors
    ///
    /// Returns the underlying `OffsetAndTimestamp::with_leader_epoch`
    /// error (`Error::LocalIllegalArgument`) if either `offset` or
    /// `timestamp` is negative. Callers that route values from a
    /// `LATEST` / `EARLIEST` ListOffsets must NOT call this — those
    /// values carry `timestamp == -1`. Use [`Self::offset`] directly
    /// for those flows.
    pub(crate) fn build_offset_and_timestamp(&self) -> Result<OffsetAndTimestamp, crate::common::Error> {
        OffsetAndTimestamp::with_leader_epoch(self.offset, self.timestamp, self.leader_epoch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allows_negative_timestamp() {
        let v = OffsetAndTimestampInternal::new(0, -1, Some(0));
        assert_eq!(v.offset(), 0);
        assert_eq!(v.timestamp(), -1);
        assert_eq!(v.leader_epoch(), Some(0));
    }

    #[test]
    fn allows_negative_offset() {
        // Java's OffsetAndTimestampInternal accepts negative offsets too.
        let v = OffsetAndTimestampInternal::new(-1, 0, None);
        assert_eq!(v.offset(), -1);
    }

    #[test]
    fn build_offset_and_timestamp_succeeds_for_non_negative() {
        let v = OffsetAndTimestampInternal::new(10, 100, Some(5));
        let public_value = v.build_offset_and_timestamp().expect("non-negative -> Ok");
        assert_eq!(public_value.offset(), 10);
        assert_eq!(public_value.timestamp(), 100);
        assert_eq!(public_value.leader_epoch(), Some(5));
    }

    #[test]
    fn build_offset_and_timestamp_fails_for_negative_timestamp() {
        // Mirrors the silent-None receive-path bug COMMENTS.DONE.1.md
        // Issue 6 closed — callers must NOT invoke build_offset_and_timestamp
        // for `endOffsets` / `beginningOffsets` results because those
        // legitimately carry timestamp=-1.
        let v = OffsetAndTimestampInternal::new(10, -1, None);
        assert!(v.build_offset_and_timestamp().is_err());
    }
}
