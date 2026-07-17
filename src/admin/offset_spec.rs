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

//! Specification of the desired offsets when listing offsets.
//!
//! Corresponds to `org.apache.kafka.clients.admin.OffsetSpec`.

/// Specifies the desired offsets when using `Admin::list_offsets`.
///
/// Corresponds to `org.apache.kafka.clients.admin.OffsetSpec`. Java models the
/// variants as nested subclasses of `OffsetSpec`; the Rust translation uses a
/// single closed enum since the variants carry no behavior beyond their
/// timestamp mapping (`getOffsetFromSpec`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OffsetSpec {
    /// Retrieve the earliest offset of a partition.
    ///
    /// Mirrors `OffsetSpec.EarliestSpec`.
    Earliest,
    /// Retrieve the latest offset of a partition.
    ///
    /// Mirrors `OffsetSpec.LatestSpec`.
    Latest,
    /// Retrieve the offset with the largest timestamp of a partition.
    ///
    /// Mirrors `OffsetSpec.MaxTimestampSpec`.
    MaxTimestamp,
    /// Retrieve the local log start offset.
    ///
    /// Mirrors `OffsetSpec.EarliestLocalSpec`.
    EarliestLocal,
    /// Retrieve the highest offset of data stored in remote storage.
    ///
    /// Mirrors `OffsetSpec.LatestTieredSpec`.
    LatestTiered,
    /// Retrieve the earliest offset of records pending upload to remote storage.
    ///
    /// Mirrors `OffsetSpec.EarliestPendingUploadSpec`.
    EarliestPendingUpload,
    /// Retrieve the earliest offset whose timestamp is greater than or equal to
    /// the given timestamp (in milliseconds).
    ///
    /// Mirrors `OffsetSpec.TimestampSpec`.
    Timestamp(i64),
}

impl OffsetSpec {
    /// Used to retrieve the latest offset of a partition.
    ///
    /// Mirrors `OffsetSpec.latest()`.
    pub fn latest() -> Self {
        Self::Latest
    }

    /// Used to retrieve the earliest offset of a partition.
    ///
    /// Mirrors `OffsetSpec.earliest()`.
    pub fn earliest() -> Self {
        Self::Earliest
    }

    /// Used to retrieve the earliest offset whose timestamp is greater than or
    /// equal to the given timestamp (in milliseconds).
    ///
    /// Mirrors `OffsetSpec.forTimestamp(long)`.
    pub fn for_timestamp(timestamp: i64) -> Self {
        Self::Timestamp(timestamp)
    }

    /// Used to retrieve the offset with the largest timestamp of a partition.
    ///
    /// Mirrors `OffsetSpec.maxTimestamp()`.
    pub fn max_timestamp() -> Self {
        Self::MaxTimestamp
    }

    /// Used to retrieve the local log start offset.
    ///
    /// Mirrors `OffsetSpec.earliestLocal()`.
    pub fn earliest_local() -> Self {
        Self::EarliestLocal
    }

    /// Used to retrieve the highest offset of data stored in remote storage.
    ///
    /// Mirrors `OffsetSpec.latestTiered()`.
    pub fn latest_tiered() -> Self {
        Self::LatestTiered
    }

    /// Used to retrieve the earliest offset of records pending upload to remote
    /// storage.
    ///
    /// Mirrors `OffsetSpec.earliestPendingUpload()`.
    pub fn earliest_pending_upload() -> Self {
        Self::EarliestPendingUpload
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn factories_produce_expected_variants() {
        assert_eq!(OffsetSpec::latest(), OffsetSpec::Latest);
        assert_eq!(OffsetSpec::earliest(), OffsetSpec::Earliest);
        assert_eq!(OffsetSpec::max_timestamp(), OffsetSpec::MaxTimestamp);
        assert_eq!(OffsetSpec::earliest_local(), OffsetSpec::EarliestLocal);
        assert_eq!(OffsetSpec::latest_tiered(), OffsetSpec::LatestTiered);
        assert_eq!(OffsetSpec::earliest_pending_upload(), OffsetSpec::EarliestPendingUpload);
        assert_eq!(OffsetSpec::for_timestamp(42), OffsetSpec::Timestamp(42));
    }
}
