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
//! Its nested subclass with data, [`TimestampSpec`], is reached through this
//! module.

/// Specifies the desired offsets when using `Admin::list_offsets`.
// Java models the variants as nested subclasses of `OffsetSpec`; the Rust translation uses a
// single closed enum since the variants carry no behavior beyond their timestamp mapping
// (`getOffsetFromSpec`). The one subclass with data, `TimestampSpec`, stays behind its variant as
// a struct, so a field Java adds to it arrives without changing the variant's shape.
#[derive(Clone, Debug)]
#[non_exhaustive]
#[doc(alias = "org.apache.kafka.clients.admin.OffsetSpec")]
pub enum OffsetSpec {
    /// Retrieve the earliest offset of a partition.
    Earliest,
    /// Retrieve the latest offset of a partition.
    Latest,
    /// Retrieve the offset with the largest timestamp of a partition.
    MaxTimestamp,
    /// Retrieve the local log start offset.
    EarliestLocal,
    /// Retrieve the highest offset of data stored in remote storage.
    LatestTiered,
    /// Retrieve the earliest offset of records pending upload to remote storage.
    EarliestPendingUpload,
    /// Retrieve the earliest offset whose timestamp is greater than or equal to
    /// the given timestamp (in milliseconds).
    Timestamp(TimestampSpec),
}

/// The offset whose timestamp is greater than or equal to a requested
/// timestamp (in milliseconds).
///
/// Java keeps its constructor and `timestamp()` package-private: the value is
/// built with [`OffsetSpec::for_timestamp`] and only `KafkaAdminClient`'s
/// `getOffsetFromSpec` reads it, so outside the crate it is opaque.
#[derive(Clone, Debug)]
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
    #[doc(alias = "org.apache.kafka.clients.admin.OffsetSpec#latest")]
    pub fn latest() -> Self {
        Self::Latest
    }

    /// Used to retrieve the earliest offset of a partition.
    #[doc(alias = "org.apache.kafka.clients.admin.OffsetSpec#earliest")]
    pub fn earliest() -> Self {
        Self::Earliest
    }

    /// Used to retrieve the earliest offset whose timestamp is greater than or
    /// equal to the given timestamp (in milliseconds).
    ///
    /// # Arguments
    ///
    /// - `timestamp` - in milliseconds
    #[doc(alias = "org.apache.kafka.clients.admin.OffsetSpec#forTimestamp")]
    pub fn for_timestamp(timestamp: i64) -> Self {
        Self::Timestamp(TimestampSpec::new(timestamp))
    }

    /// Used to retrieve the offset with the largest timestamp of a partition as message
    /// timestamps can be specified client side this may not match the log end offset returned by
    /// [`OffsetSpec::latest`].
    #[doc(alias = "org.apache.kafka.clients.admin.OffsetSpec#maxTimestamp")]
    pub fn max_timestamp() -> Self {
        Self::MaxTimestamp
    }

    /// Used to retrieve the local log start offset. Local log start offset is the offset of a log
    /// above which reads are guaranteed to be served from the disk of the leader broker.
    ///
    /// Note: When tiered Storage is not enabled, it behaves the same as retrieving the earliest
    /// timestamp offset.
    #[doc(alias = "org.apache.kafka.clients.admin.OffsetSpec#earliestLocal")]
    pub fn earliest_local() -> Self {
        Self::EarliestLocal
    }

    /// Used to retrieve the highest offset of data stored in remote storage.
    ///
    /// Note: When tiered storage is not enabled, we will return unknown offset.
    #[doc(alias = "org.apache.kafka.clients.admin.OffsetSpec#latestTiered")]
    pub fn latest_tiered() -> Self {
        Self::LatestTiered
    }

    /// Used to retrieve the earliest offset of records that are pending upload to remote storage.
    ///
    /// Note: When tiered storage is not enabled, we will return unknown offset.
    #[doc(alias = "org.apache.kafka.clients.admin.OffsetSpec#earliestPendingUpload")]
    pub fn earliest_pending_upload() -> Self {
        Self::EarliestPendingUpload
    }
}

/// What tells two specs apart, `OffsetSpec` having no `PartialEq` (Java's
/// `OffsetSpec` does not override `equals`): the kind and, for a
/// `TimestampSpec`, its timestamp. The tests here and in the C binding
/// compare specs through it.
#[cfg(test)]
pub(crate) fn spec_key(spec: &OffsetSpec) -> (std::mem::Discriminant<OffsetSpec>, Option<i64>) {
    let timestamp = match spec {
        OffsetSpec::Timestamp(timestamp_spec) => Some(timestamp_spec.timestamp()),
        _ => None,
    };
    (std::mem::discriminant(spec), timestamp)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn factories_produce_expected_variants() {
        assert_eq!(spec_key(&OffsetSpec::latest()), spec_key(&OffsetSpec::Latest));
        assert_eq!(spec_key(&OffsetSpec::earliest()), spec_key(&OffsetSpec::Earliest));
        assert_eq!(spec_key(&OffsetSpec::max_timestamp()), spec_key(&OffsetSpec::MaxTimestamp));
        assert_eq!(spec_key(&OffsetSpec::earliest_local()), spec_key(&OffsetSpec::EarliestLocal));
        assert_eq!(spec_key(&OffsetSpec::latest_tiered()), spec_key(&OffsetSpec::LatestTiered));
        assert_eq!(
            spec_key(&OffsetSpec::earliest_pending_upload()),
            spec_key(&OffsetSpec::EarliestPendingUpload)
        );
        assert_eq!(
            spec_key(&OffsetSpec::for_timestamp(42)),
            spec_key(&OffsetSpec::Timestamp(TimestampSpec::new(42)))
        );
    }

    #[test]
    fn timestamp_spec_keeps_its_timestamp() {
        let OffsetSpec::Timestamp(spec) = OffsetSpec::for_timestamp(42) else {
            panic!("forTimestamp builds a TimestampSpec");
        };
        assert_eq!(spec.timestamp(), 42);
        assert_ne!(
            spec_key(&OffsetSpec::for_timestamp(42)),
            spec_key(&OffsetSpec::for_timestamp(43))
        );
    }
}
