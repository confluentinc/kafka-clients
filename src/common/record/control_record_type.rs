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

//! Translation of `org.apache.kafka.common.record.ControlRecordType`.
//!
//! Control records specify a schema for the record key which includes a
//! version and type:
//!
//! ```text
//! Key => Version Type
//!   Version => Int16
//!   Type    => Int16
//! ```
//!
//! In the future, the version can be bumped to indicate a new schema, but it
//! must be backwards compatible with the current schema. In general, this
//! means we can add new fields, but we cannot remove old ones.
//!
//! Note that control records are not considered for compaction by the log
//! cleaner.

use log::debug;

use crate::common::errors::KafkaError;

/// Wire-encoded type ids for control records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ControlRecordType {
    Abort,
    Commit,

    // KRaft quorum related control messages
    LeaderChange,
    SnapshotHeader,
    SnapshotFooter,

    // KRaft membership changes messages
    KraftVersion,
    KraftVoters,

    /// Used to indicate a control type which the client is not aware of and
    /// should be ignored.
    Unknown,
}

/// Current version of the control record key schema.
pub const CURRENT_CONTROL_RECORD_KEY_VERSION: i16 = 0;
/// Number of bytes occupied by the control record key (Int16 version + Int16 type).
pub const CURRENT_CONTROL_RECORD_KEY_SIZE: usize = 4;

impl ControlRecordType {
    /// Wire-encoded type id (matches Java's `type` field). Returns `-1` for
    /// `Unknown`.
    pub fn type_id(&self) -> i16 {
        match self {
            ControlRecordType::Abort => 0,
            ControlRecordType::Commit => 1,
            ControlRecordType::LeaderChange => 2,
            ControlRecordType::SnapshotHeader => 3,
            ControlRecordType::SnapshotFooter => 4,
            ControlRecordType::KraftVersion => 5,
            ControlRecordType::KraftVoters => 6,
            ControlRecordType::Unknown => -1,
        }
    }

    /// Look up a `ControlRecordType` from a wire-encoded type id.
    /// Mirrors Java's `ControlRecordType.fromTypeId(short)` — unknown values
    /// map to [`ControlRecordType::Unknown`] (matching Java).
    pub fn from_type_id(type_id: i16) -> ControlRecordType {
        match type_id {
            0 => ControlRecordType::Abort,
            1 => ControlRecordType::Commit,
            2 => ControlRecordType::LeaderChange,
            3 => ControlRecordType::SnapshotHeader,
            4 => ControlRecordType::SnapshotFooter,
            5 => ControlRecordType::KraftVersion,
            6 => ControlRecordType::KraftVoters,
            _ => ControlRecordType::Unknown,
        }
    }

    /// Read the type id from a control-record key buffer at absolute offset
    /// 2 (after the 2-byte version prefix), without consuming any bytes.
    ///
    /// Mirrors Java's `ControlRecordType.parseTypeId(ByteBuffer)`. The Java
    /// implementation uses absolute `getShort(0)` / `getShort(2)`, leaving
    /// the buffer position untouched. Our `&[u8]` argument is the read-mode
    /// "remaining" view of the Java buffer; we therefore validate against
    /// `key.len()` rather than `remaining()`.
    ///
    /// # Errors
    ///
    /// * [`KafkaError::InvalidRecord`] if the key is shorter than
    ///   [`CURRENT_CONTROL_RECORD_KEY_SIZE`] bytes, or if the encoded version
    ///   is negative (suggesting data corruption).
    pub fn parse_type_id(key: &[u8]) -> Result<i16, KafkaError> {
        if key.len() < CURRENT_CONTROL_RECORD_KEY_SIZE {
            return Err(KafkaError::InvalidRecord(format!(
                "Invalid value size found for end control record key. Must have at least {CURRENT_CONTROL_RECORD_KEY_SIZE} bytes, but found only {}",
                key.len()
            )));
        }

        // Big-endian Int16 at offset 0.
        let version = i16::from_be_bytes([key[0], key[1]]);
        if version < 0 {
            return Err(KafkaError::InvalidRecord(format!(
                "Invalid version found for control record: {version}. May indicate data corruption"
            )));
        }

        if version != CURRENT_CONTROL_RECORD_KEY_VERSION {
            debug!(
                "Received unknown control record key version {version}. Parsing as version {CURRENT_CONTROL_RECORD_KEY_VERSION}"
            );
        }

        // Big-endian Int16 at offset 2.
        Ok(i16::from_be_bytes([key[2], key[3]]))
    }

    /// Parse a control-record key into its [`ControlRecordType`]. Unknown
    /// type ids resolve to [`ControlRecordType::Unknown`] (matching Java's
    /// `ControlRecordType.parse(ByteBuffer)`).
    pub fn parse(key: &[u8]) -> Result<ControlRecordType, KafkaError> {
        Ok(ControlRecordType::from_type_id(ControlRecordType::parse_type_id(key)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Java: `ControlRecordTypeTest#testParseUnknownType`.
    #[test]
    fn parse_unknown_type() {
        let mut buf = [0u8; 32];
        // version (Int16) at offset 0
        buf[0..2].copy_from_slice(&CURRENT_CONTROL_RECORD_KEY_VERSION.to_be_bytes());
        // type 337 at offset 2
        buf[2..4].copy_from_slice(&337i16.to_be_bytes());
        // After the Java `flip()`, only the written prefix is "remaining".
        let key = &buf[..4];
        assert_eq!(ControlRecordType::parse(key).unwrap(), ControlRecordType::Unknown);
    }

    /// Java: `ControlRecordTypeTest#testParseUnknownVersion`.
    #[test]
    fn parse_unknown_version() {
        let mut buf = [0u8; 32];
        // version 5 at offset 0
        buf[0..2].copy_from_slice(&5i16.to_be_bytes());
        // type ABORT at offset 2
        buf[2..4].copy_from_slice(&ControlRecordType::Abort.type_id().to_be_bytes());
        // 4-byte field added in version 5 at offset 4
        buf[4..8].copy_from_slice(&23432i32.to_be_bytes());
        let key = &buf[..8];
        assert_eq!(ControlRecordType::parse(key).unwrap(), ControlRecordType::Abort);
    }

    /// Java: `ControlRecordTypeTest#testRoundTrip` (parameterized over every
    /// enum variant).
    #[test]
    fn round_trip_every_variant() {
        for expected in [
            ControlRecordType::Abort,
            ControlRecordType::Commit,
            ControlRecordType::LeaderChange,
            ControlRecordType::SnapshotHeader,
            ControlRecordType::SnapshotFooter,
            ControlRecordType::KraftVersion,
            ControlRecordType::KraftVoters,
            ControlRecordType::Unknown,
        ] {
            let mut buf = [0u8; 32];
            buf[0..2].copy_from_slice(&CURRENT_CONTROL_RECORD_KEY_VERSION.to_be_bytes());
            buf[2..4].copy_from_slice(&expected.type_id().to_be_bytes());
            let key = &buf[..4];
            assert_eq!(ControlRecordType::parse(key).unwrap(), expected);
        }
    }

    #[test]
    fn parse_buffer_too_short() {
        let key = &[0u8; 3][..];
        let err = ControlRecordType::parse(key).unwrap_err();
        assert!(matches!(err, KafkaError::InvalidRecord(_)));
        assert!(err.to_string().contains("Invalid value size"));
    }

    #[test]
    fn parse_negative_version_rejected() {
        let mut buf = [0u8; 4];
        buf[0..2].copy_from_slice(&(-1i16).to_be_bytes());
        let err = ControlRecordType::parse(&buf).unwrap_err();
        assert!(matches!(err, KafkaError::InvalidRecord(_)));
        assert!(err.to_string().contains("Invalid version"));
    }

    #[test]
    fn from_type_id_unknown_maps_to_unknown() {
        assert_eq!(ControlRecordType::from_type_id(7), ControlRecordType::Unknown);
        assert_eq!(ControlRecordType::from_type_id(-2), ControlRecordType::Unknown);
        assert_eq!(ControlRecordType::from_type_id(0), ControlRecordType::Abort);
    }
}
