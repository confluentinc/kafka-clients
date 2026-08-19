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

//! Control-record key parsing.
//!
//! Translated from `org.apache.kafka.common.record.ControlRecordType`.
//!
//! # Method accounting (`definition-of-done.md` §2)
//!
//! Java declares five members. Four are translated: `type()`
//! ([`ControlRecordType::type_id`]), `parseTypeId`, `fromTypeId` and `parse`.
//!
//! The fifth, `recordKey()` (Java 78-87), is **not** translated, and it is a write-path
//! method with no reachable client caller. It builds the `Struct` that *serialises* a
//! control-record key, and its only call site in the whole Java tree is
//! `MemoryRecordsBuilder.appendControlRecord` (`MemoryRecordsBuilder.java:614`):
//!
//! ```text
//! $ grep -rn 'recordKey()' kafka/clients/src/main/java/
//! .../record/MemoryRecordsBuilder.java:614:        Struct keyStruct = type.recordKey();
//! .../record/ControlRecordType.java:78:    public Struct recordKey() {
//! ```
//!
//! `appendControlRecord` is itself only reached from `appendEndTxnMarker` and the KRaft
//! leader-change / snapshot writers — broker and controller paths. None is translated,
//! `appendControlRecord` included, and `EndTransactionMarker` is explicitly out of scope
//! (`design/history/Milestone-11/PLAN.md` §1.1). A Kafka *client* only ever reads control
//! records, which is why the parsing half is what this port needs.

use crate::common::record::InvalidRecordError;

/// Control records specify a schema for the record key which includes a version
/// and type:
///
/// ```text
/// Key => Version Type
///   Version => Int16
///   Type => Int16
/// ```
///
/// In the future, the version can be bumped to indicate a new schema, but it must
/// be backwards compatible with the current schema. In general, this means we can
/// add new fields, but we cannot remove old ones.
///
/// Note that control records are not considered for compaction by the log cleaner.
///
/// The schema for the value field is left to the control record type to specify.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlRecordType {
    /// A transaction abort marker.
    Abort,
    /// A transaction commit marker.
    Commit,

    // KRaft quorum related control messages
    /// KRaft leader change.
    LeaderChange,
    /// KRaft snapshot header.
    SnapshotHeader,
    /// KRaft snapshot footer.
    SnapshotFooter,

    // KRaft membership changes messages
    /// KRaft version.
    KRaftVersion,
    /// KRaft voters.
    KRaftVoters,

    /// Indicates a control type which the client is not aware of and should be
    /// ignored.
    Unknown,
}

/// `CURRENT_CONTROL_RECORD_KEY_VERSION` (Java 61).
pub(crate) const CURRENT_CONTROL_RECORD_KEY_VERSION: i16 = 0;
/// `CURRENT_CONTROL_RECORD_KEY_SIZE` (Java 62).
pub(crate) const CURRENT_CONTROL_RECORD_KEY_SIZE: usize = 4;

impl ControlRecordType {
    /// The wire type id.
    ///
    /// Corresponds to Java's `type()` accessor over the enum's `type` field.
    pub fn type_id(self) -> i16 {
        match self {
            Self::Abort => 0,
            Self::Commit => 1,
            Self::LeaderChange => 2,
            Self::SnapshotHeader => 3,
            Self::SnapshotFooter => 4,
            Self::KRaftVersion => 5,
            Self::KRaftVoters => 6,
            Self::Unknown => -1,
        }
    }

    /// Reads the type id out of a control record's key.
    ///
    /// Translated from `parseTypeId(ByteBuffer key)` (Java 88-101). Java reads
    /// absolutely (`key.getShort(0)` / `key.getShort(2)`) rather than through the
    /// buffer's position, so a `&[u8]` is the faithful parameter.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidRecordError`] — Java's `InvalidRecordException` — when the
    /// key is too short or carries a negative version. Java throws in both cases;
    /// CLAUDE.md §10.2 makes that a `Result` here.
    pub fn parse_type_id(key: &[u8]) -> Result<i16, InvalidRecordError> {
        if key.len() < CURRENT_CONTROL_RECORD_KEY_SIZE {
            return Err(InvalidRecordError::new(format!(
                "Invalid value size found for end control record key. Must have at least {} bytes, but found only {}",
                CURRENT_CONTROL_RECORD_KEY_SIZE,
                key.len()
            )));
        }

        let version = i16::from_be_bytes([key[0], key[1]]);
        if version < 0 {
            return Err(InvalidRecordError::new(format!(
                "Invalid version found for control record: {version}. May indicate data corruption"
            )));
        }

        if version != CURRENT_CONTROL_RECORD_KEY_VERSION {
            log::debug!(
                "Received unknown control record key version {version}. Parsing as version {CURRENT_CONTROL_RECORD_KEY_VERSION}"
            );
        }
        Ok(i16::from_be_bytes([key[2], key[3]]))
    }

    /// Maps a wire type id onto a variant, with unknown ids becoming
    /// [`Self::Unknown`].
    ///
    /// Translated from `fromTypeId(short typeId)` (Java 103-123).
    pub fn from_type_id(type_id: i16) -> Self {
        match type_id {
            0 => Self::Abort,
            1 => Self::Commit,
            2 => Self::LeaderChange,
            3 => Self::SnapshotHeader,
            4 => Self::SnapshotFooter,
            5 => Self::KRaftVersion,
            6 => Self::KRaftVoters,
            _ => Self::Unknown,
        }
    }

    /// Parses a control record's key into its type.
    ///
    /// Translated from `parse(ByteBuffer key)` (Java 125-127).
    ///
    /// # Errors
    ///
    /// Propagates [`Self::parse_type_id`]'s errors.
    pub fn parse(key: &[u8]) -> Result<Self, InvalidRecordError> {
        Ok(Self::from_type_id(Self::parse_type_id(key)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A control-record key: version 0 then `type_id`, both big-endian i16.
    fn key(version: i16, type_id: i16) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(4);
        bytes.extend_from_slice(&version.to_be_bytes());
        bytes.extend_from_slice(&type_id.to_be_bytes());
        bytes
    }

    /// Round-trips every id Java's `fromTypeId` switch names, plus an unknown one.
    ///
    /// Java has no `ControlRecordTypeTest`; the class is covered indirectly through
    /// `MemoryRecordsTest` / `FileRecordsTest`. This pins the mapping directly,
    /// which is what `CompletedFetch`'s abort-marker branch depends on.
    #[test]
    fn test_from_type_id_covers_every_named_type() {
        let expected = [
            (0i16, ControlRecordType::Abort),
            (1, ControlRecordType::Commit),
            (2, ControlRecordType::LeaderChange),
            (3, ControlRecordType::SnapshotHeader),
            (4, ControlRecordType::SnapshotFooter),
            (5, ControlRecordType::KRaftVersion),
            (6, ControlRecordType::KRaftVoters),
        ];
        for (type_id, variant) in expected {
            assert_eq!(ControlRecordType::from_type_id(type_id), variant);
            assert_eq!(variant.type_id(), type_id);
        }
        // Anything else is UNKNOWN, "a control type which the client is not aware
        // of and should be ignored".
        assert_eq!(ControlRecordType::from_type_id(7), ControlRecordType::Unknown);
        assert_eq!(ControlRecordType::from_type_id(-1), ControlRecordType::Unknown);
        assert_eq!(ControlRecordType::Unknown.type_id(), -1);
    }

    /// The two markers a transactional consumer discriminates.
    #[test]
    fn test_parse_reads_the_type_from_the_key() {
        assert_eq!(
            ControlRecordType::parse(&key(0, 0)).expect("a well-formed key"),
            ControlRecordType::Abort
        );
        assert_eq!(
            ControlRecordType::parse(&key(0, 1)).expect("a well-formed key"),
            ControlRecordType::Commit
        );
    }

    /// A future key version is parsed as the current one rather than rejected —
    /// Java logs at debug and reads the type anyway (Java 97-100).
    #[test]
    fn test_parse_accepts_a_newer_key_version() {
        assert_eq!(
            ControlRecordType::parse(&key(1, 0)).expect("a newer version is still parsed"),
            ControlRecordType::Abort
        );
    }

    /// A key shorter than 4 bytes is `InvalidRecordException` in Java.
    #[test]
    fn test_parse_rejects_a_short_key() {
        let error = ControlRecordType::parse(&[0, 0, 0]).expect_err("3 bytes is too short");
        assert_eq!(
            error.message(),
            "Invalid value size found for end control record key. Must have at least 4 bytes, but found only 3"
        );
        let error = ControlRecordType::parse(&[]).expect_err("an empty key is too short");
        assert_eq!(
            error.message(),
            "Invalid value size found for end control record key. Must have at least 4 bytes, but found only 0"
        );
    }

    /// A negative version "may indicate data corruption" and is rejected.
    #[test]
    fn test_parse_rejects_a_negative_version() {
        let error = ControlRecordType::parse(&key(-1, 0)).expect_err("a negative version is rejected");
        assert_eq!(
            error.message(),
            "Invalid version found for control record: -1. May indicate data corruption"
        );
    }
}
