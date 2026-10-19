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
//! Corresponds to `org.apache.kafka.clients.admin.OffsetSpec`. Its nested
//! subclass with data, [`TimestampSpec`], is reached through this module, as
//! CLAUDE.md §2 provides for nested classes.

/// Specifies the desired offsets when using `Admin::list_offsets`.
///
/// Corresponds to `org.apache.kafka.clients.admin.OffsetSpec`. Java models the
/// variants as nested subclasses of `OffsetSpec`; the Rust translation uses a
/// single closed enum since the variants carry no behavior beyond their
/// timestamp mapping (`getOffsetFromSpec`). The one subclass with data,
/// `TimestampSpec`, stays behind its variant as a struct, so a field Java adds
/// to it arrives without changing the variant's shape.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
#[doc(alias = "org.apache.kafka.clients.admin.OffsetSpec")]
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
    Timestamp(TimestampSpec),
}

/// The offset whose timestamp is greater than or equal to a requested
/// timestamp (in milliseconds).
///
/// Corresponds to `org.apache.kafka.clients.admin.OffsetSpec.TimestampSpec`.
/// Java keeps its constructor and `timestamp()` package-private: the value is
/// built with [`OffsetSpec::for_timestamp`] and only `KafkaAdminClient`'s
/// `getOffsetFromSpec` reads it, so outside the crate it is opaque.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[doc(alias = "org.apache.kafka.clients.admin.OffsetSpec$TimestampSpec")]
pub struct TimestampSpec {
    timestamp: i64,
}

impl TimestampSpec {
    /// Translated from the package-private constructor `TimestampSpec(long)`.
    #[doc(alias = "org.apache.kafka.clients.admin.OffsetSpec$TimestampSpec#TimestampSpec")]
    pub(crate) fn new(timestamp: i64) -> Self {
        Self { timestamp }
    }

    /// The requested timestamp, in milliseconds.
    ///
    /// Translated from the package-private `TimestampSpec.timestamp()`.
    #[doc(alias = "org.apache.kafka.clients.admin.OffsetSpec$TimestampSpec#timestamp")]
    pub(crate) fn timestamp(&self) -> i64 {
        self.timestamp
    }
}

impl OffsetSpec {
    /// Used to retrieve the latest offset of a partition.
    ///
    /// Mirrors `OffsetSpec.latest()`.
    #[doc(alias = "org.apache.kafka.clients.admin.OffsetSpec#latest")]
    pub fn latest() -> Self {
        Self::Latest
    }

    /// Used to retrieve the earliest offset of a partition.
    ///
    /// Mirrors `OffsetSpec.earliest()`.
    #[doc(alias = "org.apache.kafka.clients.admin.OffsetSpec#earliest")]
    pub fn earliest() -> Self {
        Self::Earliest
    }

    /// Used to retrieve the earliest offset whose timestamp is greater than or
    /// equal to the given timestamp (in milliseconds).
    ///
    /// Mirrors `OffsetSpec.forTimestamp(long)`.
    #[doc(alias = "org.apache.kafka.clients.admin.OffsetSpec#forTimestamp")]
    pub fn for_timestamp(timestamp: i64) -> Self {
        Self::Timestamp(TimestampSpec::new(timestamp))
    }

    /// Used to retrieve the offset with the largest timestamp of a partition.
    ///
    /// Mirrors `OffsetSpec.maxTimestamp()`.
    #[doc(alias = "org.apache.kafka.clients.admin.OffsetSpec#maxTimestamp")]
    pub fn max_timestamp() -> Self {
        Self::MaxTimestamp
    }

    /// Used to retrieve the local log start offset.
    ///
    /// Mirrors `OffsetSpec.earliestLocal()`.
    #[doc(alias = "org.apache.kafka.clients.admin.OffsetSpec#earliestLocal")]
    pub fn earliest_local() -> Self {
        Self::EarliestLocal
    }

    /// Used to retrieve the highest offset of data stored in remote storage.
    ///
    /// Mirrors `OffsetSpec.latestTiered()`.
    #[doc(alias = "org.apache.kafka.clients.admin.OffsetSpec#latestTiered")]
    pub fn latest_tiered() -> Self {
        Self::LatestTiered
    }

    /// Used to retrieve the earliest offset of records pending upload to remote
    /// storage.
    ///
    /// Mirrors `OffsetSpec.earliestPendingUpload()`.
    #[doc(alias = "org.apache.kafka.clients.admin.OffsetSpec#earliestPendingUpload")]
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
        assert_eq!(OffsetSpec::for_timestamp(42), OffsetSpec::Timestamp(TimestampSpec::new(42)));
    }

    #[test]
    fn timestamp_spec_keeps_its_timestamp() {
        let OffsetSpec::Timestamp(spec) = OffsetSpec::for_timestamp(42) else {
            panic!("forTimestamp builds a TimestampSpec");
        };
        assert_eq!(spec.timestamp(), 42);
        assert_ne!(OffsetSpec::for_timestamp(42), OffsetSpec::for_timestamp(43));
    }
}
