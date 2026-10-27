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

//! Offset and metadata committed by a consumer.
//!
//! Translated from `org.apache.kafka.clients.consumer.OffsetAndMetadata`.

use std::fmt;
use std::hash::{Hash, Hasher};

use crate::common::Error;

/// The Kafka offset commit API allows users to provide additional metadata
/// (in the form of a string) when an offset is committed. This can be useful
/// (for example) to store information about which node made the commit, what
/// time the commit was made, etc.
#[derive(Clone, Debug)]
#[doc(alias = "org.apache.kafka.clients.consumer.OffsetAndMetadata")]
pub struct OffsetAndMetadata {
    offset: i64,
    // Internal leader epoch (raw); the public getter normalizes negative
    // values to `None` to match Java's `leaderEpoch()`.
    leader_epoch: Option<i32>,
    metadata: String,
}

impl OffsetAndMetadata {
    /// Construct a new OffsetAndMetadata object for committing through
    /// [`KafkaConsumer`](crate::consumer::KafkaConsumer). The metadata
    /// associated with the commit will be empty.
    ///
    /// # Arguments
    ///
    /// - `offset` - The offset to be committed
    ///
    /// # Returns
    ///
    /// The new `OffsetAndMetadata`.
    ///
    /// # Errors
    ///
    /// - `Error::is_local_illegal_argument_error` if `offset` is negative.
    #[doc(alias = "org.apache.kafka.clients.consumer.OffsetAndMetadata#OffsetAndMetadata")]
    pub fn new(offset: i64) -> Result<Self, Error> {
        Self::with_leader_epoch_metadata(offset, None, String::new())
    }

    /// Construct a new OffsetAndMetadata object for committing through
    /// [`KafkaConsumer`](crate::consumer::KafkaConsumer).
    ///
    /// # Arguments
    ///
    /// - `offset` - The offset to be committed
    /// - `metadata` - Non-null metadata
    ///
    /// # Returns
    ///
    /// The new `OffsetAndMetadata`.
    ///
    /// # Errors
    ///
    /// - `Error::is_local_illegal_argument_error` if `offset` is negative.
    #[doc(alias = "org.apache.kafka.clients.consumer.OffsetAndMetadata#OffsetAndMetadata")]
    pub fn with_metadata(offset: i64, metadata: impl Into<String>) -> Result<Self, Error> {
        Self::with_leader_epoch_metadata(offset, None, metadata)
    }

    /// Construct a new OffsetAndMetadata object for committing through
    /// [`KafkaConsumer`](crate::consumer::KafkaConsumer).
    ///
    /// # Arguments
    ///
    /// - `offset` - The offset to be committed
    /// - `leader_epoch` - Optional leader epoch of the last consumed record
    /// - `metadata` - Non-null metadata
    ///
    /// # Returns
    ///
    /// The new `OffsetAndMetadata`.
    ///
    /// # Errors
    ///
    /// - `Error::is_local_illegal_argument_error` if `offset` is negative.
    #[doc(alias = "org.apache.kafka.clients.consumer.OffsetAndMetadata#OffsetAndMetadata")]
    pub fn with_leader_epoch_metadata(
        offset: i64,
        leader_epoch: Option<i32>,
        metadata: impl Into<String>,
    ) -> Result<Self, Error> {
        if offset < 0 {
            return Err(Error::local_illegal_argument("Invalid negative offset"));
        }
        // Java's constructor normalizes a null metadata string to the empty
        // string ("NO_METADATA"). In Rust, callers can pass `""` directly;
        // `impl Into<String>` accepts both `&str` and `String`.
        Ok(Self { offset, leader_epoch, metadata: metadata.into() })
    }

    /// Returns the committed offset.
    #[doc(alias = "org.apache.kafka.clients.consumer.OffsetAndMetadata#offset")]
    pub fn offset(&self) -> i64 {
        self.offset
    }

    /// Get the metadata of the previously consumed record.
    ///
    /// # Returns
    ///
    /// The metadata or empty string if no metadata
    #[doc(alias = "org.apache.kafka.clients.consumer.OffsetAndMetadata#metadata")]
    pub fn metadata(&self) -> &str {
        &self.metadata
    }

    /// Get the leader epoch of the previously consumed record (if one is
    /// known). Log truncation is detected if there exists a leader epoch which
    /// is larger than this epoch and begins at an offset earlier than the
    /// committed offset.
    ///
    /// # Returns
    ///
    /// The leader epoch or empty if not known
    #[doc(alias = "org.apache.kafka.clients.consumer.OffsetAndMetadata#leaderEpoch")]
    pub fn leader_epoch(&self) -> Option<i32> {
        match self.leader_epoch {
            Some(e) if e >= 0 => Some(e),
            _ => None,
        }
    }
}

impl PartialEq for OffsetAndMetadata {
    fn eq(&self, other: &Self) -> bool {
        // Java equality uses the filtered leaderEpoch() value, not the raw
        // field — see OffsetAndMetadata.equals in the Java source.
        self.offset == other.offset && self.metadata == other.metadata && self.leader_epoch() == other.leader_epoch()
    }
}

impl Eq for OffsetAndMetadata {}

impl Hash for OffsetAndMetadata {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.offset.hash(state);
        self.metadata.hash(state);
        self.leader_epoch().hash(state);
    }
}

impl fmt::Display for OffsetAndMetadata {
    // Matches Java's `toString()`:
    // `OffsetAndMetadata{offset=N, leaderEpoch=X, metadata='Y'}`
    // where `X` is `null` if no leader epoch is present.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let leader_epoch_str = match self.leader_epoch() {
            Some(e) => e.to_string(),
            None => "null".to_string(),
        };
        write!(
            f,
            "OffsetAndMetadata{{offset={}, leaderEpoch={}, metadata='{}'}}",
            self.offset, leader_epoch_str, self.metadata
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::hash_map::DefaultHasher;

    fn hash_of(o: &OffsetAndMetadata) -> u64 {
        let mut h = DefaultHasher::new();
        o.hash(&mut h);
        h.finish()
    }

    #[test]
    fn test_simple_constructor() {
        let o = OffsetAndMetadata::new(10).unwrap();
        assert_eq!(o.offset(), 10);
        assert_eq!(o.metadata(), "");
        assert_eq!(o.leader_epoch(), None);
    }

    #[test]
    fn test_new_metadata() {
        let o = OffsetAndMetadata::with_metadata(10, "m").unwrap();
        assert_eq!(o.metadata(), "m");
        assert_eq!(o.leader_epoch(), None);
    }

    #[test]
    fn test_new_leader_epoch_metadata() {
        let o = OffsetAndMetadata::with_leader_epoch_metadata(10, Some(2), "m").unwrap();
        assert_eq!(o.leader_epoch(), Some(2));
    }

    #[test]
    #[doc(alias = "org.apache.kafka.clients.consumer.OffsetAndMetadataTest#testInvalidNegativeOffset")]
    fn test_invalid_negative_offset() {
        let err = OffsetAndMetadata::new(-1).unwrap_err();
        assert!(err.message().contains("Invalid negative offset"));
    }

    #[test]
    fn test_negative_leader_epoch_treated_as_none() {
        let o = OffsetAndMetadata::with_leader_epoch_metadata(10, Some(-1), "m").unwrap();
        assert_eq!(o.leader_epoch(), None);
    }

    #[test]
    fn test_display() {
        let o = OffsetAndMetadata::with_leader_epoch_metadata(10, Some(2), "m").unwrap();
        assert_eq!(o.to_string(), "OffsetAndMetadata{offset=10, leaderEpoch=2, metadata='m'}");

        let o = OffsetAndMetadata::new(10).unwrap();
        assert_eq!(o.to_string(), "OffsetAndMetadata{offset=10, leaderEpoch=null, metadata=''}");
    }

    #[test]
    #[doc(alias = "org.apache.kafka.clients.consumer.OffsetAndMetadataTest#testEqualsWithNullAndNegativeLeaderEpoch")]
    fn test_equals_with_null_and_negative_leader_epoch() {
        let with_none = OffsetAndMetadata::with_leader_epoch_metadata(100, None, "metadata").unwrap();
        let with_neg = OffsetAndMetadata::with_leader_epoch_metadata(100, Some(-1), "metadata").unwrap();
        assert_eq!(with_none, with_neg);
        assert_eq!(hash_of(&with_none), hash_of(&with_neg));
    }
}
