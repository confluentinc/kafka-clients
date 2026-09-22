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

//! Control-record key parsing and serialization.
//!
//! Translated from `org.apache.kafka.common.record.internal.ControlRecordType`.
//!
//! # KAFKA-10863
//!
//! In Kafka 4.3.1 the control-record key schema is derived from the generated
//! `ControlRecordTypeSchema` message (spec `ControlRecordTypeSchema.json`,
//! generated type [`ControlRecordTypeSchemaData`]) rather than a hand-rolled
//! `protocol.types.Schema`. `recordKey()` now returns a version-prefixed
//! `ByteBuffer` and `parseTypeId` reads the type back through the same schema.
//!
//! # Method accounting (`definition-of-done.md` §2)
//!
//! Java declares `type()` ([`ControlRecordType::type_id`]), `recordKey()`
//! ([`ControlRecordType::record_key`]), `controlRecordKeySize()`
//! ([`ControlRecordType::control_record_key_size`]), `parseTypeId`, `fromTypeId`
//! and `parse` — all translated.
//!
//! Java caches the serialized key in a per-enum-constant field built in the enum
//! constructor. Rust enums have no such per-variant field, so `record_key`
//! recomputes the 4-byte buffer on demand, while `control_record_key_size`
//! returns the constant Java's cached `buffer.remaining()` amounts to. Both are
//! write-path / cold-path methods (control records are read, not written, by a
//! Kafka *client*; the only in-tree writer is `MemoryRecordsBuilder`'s
//! control-record path, which is not translated — see `memory_records_builder.rs`),
//! so `record_key`'s recomputation is a negligible, faithful divergence.

use crate::ControlRecordTypeSchemaData;
use crate::common::Error;
use crate::common::InvalidRecordError;
use crate::common::protocol::MessageUtil;
use crate::common::protocol::{ByteBufferAccessor, Readable};

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

/// `CONTROL_RECORD_KEY_SIZE` (Java 60): the minimum size of a control-record key
/// (`version` int16 + `type` int16).
const CONTROL_RECORD_KEY_SIZE: usize = 4;

impl ControlRecordType {
    /// The wire type id.
    ///
    /// Corresponds to Java's `type()` accessor over the enum's `type` field.
    #[allow(dead_code)]
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

    /// Serializes this type into a control-record key buffer.
    ///
    /// Mirrors the buffer Java precomputes in the enum constructor:
    /// `MessageUtil.toVersionPrefixedByteBuffer(HIGHEST_SUPPORTED_VERSION, schema)`
    /// where `schema` is a [`ControlRecordTypeSchemaData`] with this type set.
    /// The result is the 2-byte version prefix followed by the schema body.
    #[allow(dead_code)]
    fn key_buffer(self) -> Vec<u8> {
        let mut schema = ControlRecordTypeSchemaData::new();
        schema.set_type(self.type_id());
        // Java builds this in the enum constructor, where a serialization failure
        // is an `ExceptionInInitializerError` (fatal). Writing a single `int16`
        // field into a correctly pre-sized buffer cannot fail, so an error here is
        // unrecoverable (CLAUDE.md §10.1).
        MessageUtil::to_version_prefixed_byte_buffer(
            ControlRecordTypeSchemaData::HIGHEST_SUPPORTED_VERSION,
            &mut schema,
        )
        .expect("control record key serialization is infallible")
        .buffer()
        .to_vec()
    }

    /// The serialized control-record key for this type.
    ///
    /// Translated from `recordKey()` (Java 79-84). Java returns a
    /// `ByteBuffer.duplicate()`; the Rust equivalent hands back the owned bytes.
    ///
    /// # Errors
    ///
    /// Returns a [`Error`] for [`Self::Unknown`] — Java throws
    /// `IllegalArgumentException("Cannot serialize UNKNOWN control record type")`
    /// (a recoverable unchecked exception, so a `Result` here per CLAUDE.md §10.2).
    #[allow(dead_code)]
    pub fn record_key(self) -> Result<Vec<u8>, Error> {
        if self == Self::Unknown {
            return Err(Error::local_illegal_argument("Cannot serialize UNKNOWN control record type"));
        }
        Ok(self.key_buffer())
    }

    /// The size in bytes of a control-record key for this type (always 4).
    ///
    /// Translated from `controlRecordKeySize()` (Java 86-88), which returns
    /// `buffer.remaining()`. Unlike [`Self::record_key`] there is no `UNKNOWN`
    /// guard: Java builds the buffer for every constant, `UNKNOWN` included.
    /// Java's `buffer` is precomputed once in the enum constructor, so its
    /// `remaining()` is a constant read; returning the constant here mirrors
    /// that amortization instead of re-serializing the key per call.
    #[allow(dead_code)]
    pub fn control_record_key_size(self) -> usize {
        CONTROL_RECORD_KEY_SIZE
    }

    /// Reads the type id out of a control record's key.
    ///
    /// Translated from `parseTypeId(ByteBuffer key)` (Java 90-108). Java reads the
    /// version through the buffer's position and then decodes the remaining bytes
    /// with [`ControlRecordTypeSchemaData`], clamping an unknown (higher) version
    /// down to the highest supported one. A `&[u8]` is the faithful parameter — the
    /// Java `duplicate()` exists only to leave the caller's buffer position
    /// untouched, which has no analogue for a slice.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidRecordError`] — Java's `InvalidRecordException` — when the
    /// key is too short or carries a version below the lowest supported one. Java
    /// throws in both cases; CLAUDE.md §10.2 makes that a `Result` here.
    pub fn parse_type_id(key: &[u8]) -> Result<i16, InvalidRecordError> {
        // We should duplicate the original buffer since it will be read again in
        // some cases, for example, read by KafkaRaftClient and RaftClient.Listener.
        if key.len() < CONTROL_RECORD_KEY_SIZE {
            return Err(InvalidRecordError::new(format!(
                "Invalid value size found for control record key. Must have at least {} bytes, but found only {}",
                CONTROL_RECORD_KEY_SIZE,
                key.len()
            )));
        }

        let mut buffer = ByteBufferAccessor::new(key.to_vec());
        let mut version = buffer
            .read_short()
            .map_err(|e| InvalidRecordError::new(format!("Failed to read control record key version: {e}")))?;
        if version < ControlRecordTypeSchemaData::LOWEST_SUPPORTED_VERSION {
            return Err(InvalidRecordError::new(format!(
                "Invalid version found for control record: {version}. May indicate data corruption"
            )));
        }

        if version > ControlRecordTypeSchemaData::HIGHEST_SUPPORTED_VERSION {
            log::debug!(
                "Received unknown control record key version {version}. Parsing as version {}",
                ControlRecordTypeSchemaData::HIGHEST_SUPPORTED_VERSION
            );
            version = ControlRecordTypeSchemaData::HIGHEST_SUPPORTED_VERSION;
        }
        let schema = ControlRecordTypeSchemaData::read(&mut buffer, version)
            .map_err(|e| InvalidRecordError::new(format!("Failed to parse control record key: {e}")))?;
        Ok(schema.r#type)
    }

    /// Maps a wire type id onto a variant, with unknown ids becoming
    /// [`Self::Unknown`].
    ///
    /// Translated from `fromTypeId(short typeId)` (Java 110-130).
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
    /// Translated from `parse(ByteBuffer key)` (Java 132-134).
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

    /// Every named control-record type, matching Java's `@EnumSource`.
    const ALL_TYPES: [ControlRecordType; 8] = [
        ControlRecordType::Abort,
        ControlRecordType::Commit,
        ControlRecordType::LeaderChange,
        ControlRecordType::SnapshotHeader,
        ControlRecordType::SnapshotFooter,
        ControlRecordType::KRaftVersion,
        ControlRecordType::KRaftVoters,
        ControlRecordType::Unknown,
    ];

    /// An old hard-coded v0 control-record key: `version` int16 then `type` int16,
    /// both big-endian. Mirrors the Java test's `v0Schema` built from
    /// `protocol.types` (the untranslated `Schema` runtime), which serialises to
    /// exactly these bytes.
    fn v0_key(version: i16, type_id: i16) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(4);
        bytes.extend_from_slice(&version.to_be_bytes());
        bytes.extend_from_slice(&type_id.to_be_bytes());
        bytes
    }

    /// Java: `testParseUnknownType`.
    #[test]
    fn test_parse_unknown_type() {
        let key = v0_key(ControlRecordTypeSchemaData::HIGHEST_SUPPORTED_VERSION, 337);
        assert_eq!(
            ControlRecordType::parse(&key).expect("a well-formed key"),
            ControlRecordType::Unknown
        );
    }

    /// Java: `testParseUnknownVersion`. A newer key version carrying an extra
    /// trailing field is parsed as the highest supported version, reading only the
    /// known `type` field.
    #[test]
    fn test_parse_unknown_version() {
        let mut key = v0_key(5, ControlRecordType::Abort.type_id());
        key.extend_from_slice(&23432i32.to_be_bytes()); // some field added in version 5
        assert_eq!(
            ControlRecordType::parse(&key).expect("a newer version is still parsed"),
            ControlRecordType::Abort
        );
    }

    /// Java: `testRoundTrip`. `UNKNOWN` is excluded (it cannot be serialized).
    #[test]
    fn test_round_trip() {
        for expected in ALL_TYPES {
            if expected == ControlRecordType::Unknown {
                continue;
            }
            for _version in ControlRecordTypeSchemaData::LOWEST_SUPPORTED_VERSION
                ..=ControlRecordTypeSchemaData::HIGHEST_SUPPORTED_VERSION
            {
                let buffer = expected.record_key().expect("a known type serializes");
                assert_eq!(ControlRecordType::parse(&buffer).expect("its own key round-trips"), expected);
            }
        }
    }

    /// Java: `testValueControlRecordKeySize`. Every type — `UNKNOWN` included —
    /// has a 4-byte key.
    #[test]
    fn test_value_control_record_key_size() {
        for control_type in ALL_TYPES {
            for _version in ControlRecordTypeSchemaData::LOWEST_SUPPORTED_VERSION
                ..=ControlRecordTypeSchemaData::HIGHEST_SUPPORTED_VERSION
            {
                assert_eq!(control_type.control_record_key_size(), 4);
            }
        }
    }

    /// Java: `testBackwardDeserializeCompatibility`. A key written in the old
    /// hard-coded v0 format still parses to the right type.
    #[test]
    fn test_backward_deserialize_compatibility() {
        for control_type in ALL_TYPES {
            for version in ControlRecordTypeSchemaData::LOWEST_SUPPORTED_VERSION
                ..=ControlRecordTypeSchemaData::HIGHEST_SUPPORTED_VERSION
            {
                let old_version_buffer = v0_key(version, control_type.type_id());
                let deserialized = ControlRecordType::parse(&old_version_buffer).expect("old-format key parses");
                assert_eq!(deserialized, control_type);
            }
        }
    }

    /// Java: `testForwardDeserializeCompatibility`. A key written with the new
    /// schema is readable by the old v0 layout (`version` int16, `type` int16).
    /// `UNKNOWN` is excluded (it cannot be serialized).
    #[test]
    fn test_forward_deserialize_compatibility() {
        for control_type in ALL_TYPES {
            if control_type == ControlRecordType::Unknown {
                continue;
            }
            for _version in ControlRecordTypeSchemaData::LOWEST_SUPPORTED_VERSION
                ..=ControlRecordTypeSchemaData::HIGHEST_SUPPORTED_VERSION
            {
                let new_version_buffer = control_type.record_key().expect("a known type serializes");
                // Read the type field the way the old v0 schema would: skip the
                // 2-byte version prefix and read the type int16.
                let type_id = i16::from_be_bytes([new_version_buffer[2], new_version_buffer[3]]);
                assert_eq!(ControlRecordType::from_type_id(type_id), control_type);
            }
        }
    }

    /// The mapping `from_type_id` / `type_id` agree for every named type, and any
    /// other id is `UNKNOWN`. Not in the Java test; pins the mapping that
    /// `CompletedFetch`'s abort-marker branch depends on.
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
        assert_eq!(ControlRecordType::from_type_id(7), ControlRecordType::Unknown);
        assert_eq!(ControlRecordType::from_type_id(-1), ControlRecordType::Unknown);
        assert_eq!(ControlRecordType::Unknown.type_id(), -1);
    }

    /// `record_key` refuses to serialize `UNKNOWN`, matching Java's
    /// `IllegalArgumentException`. Not in the Java test, but the contract
    /// `recordKey()` documents.
    #[test]
    fn test_record_key_rejects_unknown() {
        let error = ControlRecordType::Unknown
            .record_key()
            .expect_err("UNKNOWN cannot be serialized");
        assert_eq!(error.message(), "Cannot serialize UNKNOWN control record type");
    }

    /// A key shorter than 4 bytes is `InvalidRecordException` in Java. The 4.3.1
    /// message dropped the word "end" from the 4.2 text.
    #[test]
    fn test_parse_rejects_a_short_key() {
        let error = ControlRecordType::parse(&[0, 0, 0]).expect_err("3 bytes is too short");
        assert_eq!(
            error.message(),
            "Invalid value size found for control record key. Must have at least 4 bytes, but found only 3"
        );
        let error = ControlRecordType::parse(&[]).expect_err("an empty key is too short");
        assert_eq!(
            error.message(),
            "Invalid value size found for control record key. Must have at least 4 bytes, but found only 0"
        );
    }

    /// A negative version is below `LOWEST_SUPPORTED_VERSION` and "may indicate
    /// data corruption".
    #[test]
    fn test_parse_rejects_a_negative_version() {
        let error = ControlRecordType::parse(&v0_key(-1, 0)).expect_err("a negative version is rejected");
        assert_eq!(
            error.message(),
            "Invalid version found for control record: -1. May indicate data corruption"
        );
    }
}
