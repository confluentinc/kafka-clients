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

//! The transaction-completion control record.
//!
//! Translated from `org.apache.kafka.common.record.internal.EndTransactionMarker`.

use crate::common::Error;
use crate::common::InvalidRecordError;
use crate::common::protocol::message_util::to_version_prefixed_byte_buffer;
use crate::common::protocol::{ByteBufferAccessor, Readable};
use crate::common::record::internal::{ControlRecordType, DefaultRecord, Record, RecordBatch};
use crate::end_txn_marker_data::EndTxnMarkerData;

/// This struct represents the control record which is written to the log to
/// indicate the completion of a transaction.
///
/// The record key specifies the [`ControlRecordType`] (control type) and the
/// value embeds information useful for write validation (for now, just the
/// coordinator epoch).
///
// Declared `pub` (not `pub(crate)`) to match every sibling record type in this
// module — `SimpleRecord`, `DefaultRecord`, `MemoryRecordsBuilder`,
// `RecordBatch`, `MemoryRecords` — so the `pub fn` end-txn-marker factory
// methods on `MemoryRecords` can name it in their signatures without tripping
// the private-interfaces lint. External visibility is still `pub(crate)`: the
// enclosing `mod` is `pub(crate)` and `mod.rs` re-exports it with `pub(crate)
// use` (CLAUDE.md §2). The constructor / `deserialize` / getter chain is still
// reachable only from `#[cfg(test)]` code today — no non-test call site
// constructs a marker (the producer's transaction path lands in a later
// milestone), and `--all-targets` builds the plain lib without the test cfg, so
// those items read as dead there; `#[allow(dead_code)]` mirrors the same
// treatment `ControlRecordType`'s serialization helpers carry for the identical
// reason.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct EndTransactionMarker {
    /// The control type — either [`ControlRecordType::Commit`] or
    /// [`ControlRecordType::Abort`].
    control_type: ControlRecordType,
    /// The coordinator epoch when the marker was written.
    coordinator_epoch: i32,
    /// The version-prefixed serialized `EndTxnMarker` value.
    buffer: Vec<u8>,
}

#[allow(dead_code)]
impl EndTransactionMarker {
    /// Creates a new marker for the given control type and coordinator epoch.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] when `control_type` is neither
    /// [`ControlRecordType::Commit`] nor [`ControlRecordType::Abort`] — Java
    /// throws `IllegalArgumentException` (a recoverable unchecked exception, so a
    /// `Result` here per CLAUDE.md §10.2).
    pub(crate) fn new(control_type: ControlRecordType, coordinator_epoch: i32) -> Result<Self, Error> {
        Self::ensure_transaction_marker_control_type(control_type)?;
        let mut marker = EndTxnMarkerData::new();
        marker.set_coordinator_epoch(coordinator_epoch);
        // Writing a single `int32` field into a correctly pre-sized buffer cannot
        // fail, so an error here is unrecoverable (CLAUDE.md §10.1) — mirroring
        // that Java's `MessageUtil.toVersionPrefixedByteBuffer` does not declare a
        // checked exception here.
        let buffer = to_version_prefixed_byte_buffer(EndTxnMarkerData::HIGHEST_SUPPORTED_VERSION, &mut marker)
            .expect("end transaction marker value serialization is infallible")
            .buffer()
            .to_vec();
        Ok(Self { control_type, coordinator_epoch, buffer })
    }

    /// The coordinator epoch when the marker was written.
    ///
    /// Translated from `coordinatorEpoch()` (Java 49-51).
    pub(crate) fn coordinator_epoch(&self) -> i32 {
        self.coordinator_epoch
    }

    /// The control type of this marker.
    ///
    /// Translated from `controlType()` (Java 53-55).
    pub(crate) fn control_type(&self) -> ControlRecordType {
        self.control_type
    }

    /// The serialized marker value.
    ///
    /// Translated from `serializeValue()` (Java 57-59). Java returns a
    /// `ByteBuffer.duplicate()` so the caller's reads do not disturb the marker's
    /// own buffer position; a `&[u8]` is inherently a read-only view with no
    /// position state, so borrowing the owned bytes is the faithful zero-copy
    /// equivalent.
    pub(crate) fn serialize_value(&self) -> &[u8] {
        &self.buffer
    }

    /// Rejects a control type that is not a transaction marker.
    ///
    /// Translated from `ensureTransactionMarkerControlType` (Java 77-80).
    fn ensure_transaction_marker_control_type(control_type: ControlRecordType) -> Result<(), Error> {
        if control_type != ControlRecordType::Commit && control_type != ControlRecordType::Abort {
            // Java renders the enum via `toString()` (upper-case `COMMIT` / `ABORT`
            // / `UNKNOWN`); Rust's `Debug` yields the PascalCase variant name. The
            // control type is otherwise faithful.
            return Err(Error::local_illegal_argument(format!(
                "Invalid control record type for end transaction marker {control_type:?}"
            )));
        }
        Ok(())
    }

    /// Reads a marker out of a control record.
    ///
    /// Translated from `deserialize(Record record)` (Java 82-85).
    ///
    /// # Errors
    ///
    /// Propagates the key-parse error from [`ControlRecordType::parse`] and the
    /// value-parse errors from [`Self::deserialize_value`].
    pub(crate) fn deserialize(record: &dyn Record) -> Result<Self, Error> {
        let control_type = ControlRecordType::parse(record.key().unwrap_or_default()).map_err(Error::InvalidRecord)?;
        Self::deserialize_value(control_type, record.value().unwrap_or_default())
    }

    /// Reads a marker out of a control record's already-parsed type and value.
    ///
    /// Translated from `deserializeValue(ControlRecordType, ByteBuffer)`
    /// (Java 87-103, package-private "visible for testing").
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] when `control_type` is not a transaction marker
    /// (`IllegalArgumentException`) or when the value carries a version below the
    /// lowest supported one (`InvalidRecordException`). A version above the highest
    /// supported one is clamped down, matching Java.
    pub(crate) fn deserialize_value(control_type: ControlRecordType, value: &[u8]) -> Result<Self, Error> {
        Self::ensure_transaction_marker_control_type(control_type)?;

        let mut accessor = ByteBufferAccessor::from_bytes(value.to_vec());
        let mut version = accessor.read_short().map_err(|e| {
            Error::InvalidRecord(InvalidRecordError::new(format!(
                "Failed to read end transaction marker version: {e}"
            )))
        })?;
        if version < EndTxnMarkerData::LOWEST_SUPPORTED_VERSION {
            return Err(Error::InvalidRecord(InvalidRecordError::new(format!(
                "Invalid version found for end transaction marker: {version}. May indicate data corruption"
            ))));
        }

        if version > EndTxnMarkerData::HIGHEST_SUPPORTED_VERSION {
            log::debug!(
                "Received end transaction marker value version {version}. Parsing as version {}",
                EndTxnMarkerData::HIGHEST_SUPPORTED_VERSION
            );
            version = EndTxnMarkerData::HIGHEST_SUPPORTED_VERSION;
        }
        let marker = EndTxnMarkerData::read(&mut accessor, version).map_err(|e| {
            Error::InvalidRecord(InvalidRecordError::new(format!("Failed to parse end transaction marker: {e}")))
        })?;
        Self::new(control_type, marker.coordinator_epoch)
    }

    /// The size in bytes a control record carrying this marker would occupy.
    ///
    /// Translated from `endTxnMarkerValueSize()` (Java 105-110).
    pub(crate) fn end_txn_marker_value_size(&self) -> i32 {
        DefaultRecord::size_in_bytes_for(
            0,
            0,
            self.control_type.control_record_key_size() as i32,
            self.buffer.len() as i32,
            RecordBatch::EMPTY_HEADERS,
        )
    }
}

/// Equality mirrors Java's `equals` (Java 61-68): two markers are equal when they
/// share a coordinator epoch and control type. The serialized `buffer` is
/// excluded — it is derived from the coordinator epoch, so it carries no
/// independent identity.
impl PartialEq for EndTransactionMarker {
    fn eq(&self, other: &Self) -> bool {
        self.coordinator_epoch == other.coordinator_epoch && self.control_type == other.control_type
    }
}

impl Eq for EndTransactionMarker {}

/// Hashing mirrors Java's `hashCode` (Java 70-75): `31 * type.hashCode() +
/// coordinatorEpoch`. It reads the same two fields `eq` does, keeping the
/// `Eq`/`Hash` contract, and excludes the derived `buffer`.
impl std::hash::Hash for EndTransactionMarker {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.control_type.type_id().hash(state);
        self.coordinator_epoch.hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::protocol::varint::{size_of_varint, size_of_varlong};

    /// Java's `VALID_CONTROLLER_RECORD_TYPE`: `[COMMIT, ABORT]`.
    const VALID_CONTROLLER_RECORD_TYPE: [ControlRecordType; 2] = [ControlRecordType::Commit, ControlRecordType::Abort];

    /// Java: `testUnknownControlTypeNotAllowed`. Java asserts only the exception
    /// type; DoD #3 additionally pins the message text.
    #[test]
    fn test_unknown_control_type_not_allowed() {
        let error = EndTransactionMarker::new(ControlRecordType::Unknown, 24)
            .expect_err("UNKNOWN is not a valid end transaction marker type");
        assert_eq!(
            error.message(),
            "Invalid control record type for end transaction marker Unknown"
        );
    }

    /// Java: `testCannotDeserializeUnknownControlType`. The type check fires
    /// before the value's version short is read, so an empty value is fine.
    #[test]
    fn test_cannot_deserialize_unknown_control_type() {
        let error = EndTransactionMarker::deserialize_value(ControlRecordType::Unknown, &[])
            .expect_err("UNKNOWN cannot be deserialized as an end transaction marker");
        assert_eq!(
            error.message(),
            "Invalid control record type for end transaction marker Unknown"
        );
    }

    /// Java: `testIllegalVersion`. A value whose version short is -1 is below the
    /// lowest supported version.
    #[test]
    fn test_illegal_version() {
        // ByteBuffer.allocate(2); putShort((short) -1); flip() -> big-endian -1.
        let value = (-1i16).to_be_bytes();
        let error = EndTransactionMarker::deserialize_value(ControlRecordType::Abort, &value)
            .expect_err("version -1 is below the lowest supported version");
        assert_eq!(
            error.message(),
            "Invalid version found for end transaction marker: -1. May indicate data corruption"
        );
    }

    /// Java: `testSerde`.
    #[test]
    fn test_serde() {
        let coordinator_epoch = 79;
        let marker = EndTransactionMarker::new(ControlRecordType::Commit, coordinator_epoch).expect("COMMIT is valid");
        let value = marker.serialize_value();
        let deserialized =
            EndTransactionMarker::deserialize_value(ControlRecordType::Commit, value).expect("round-trips");
        assert_eq!(coordinator_epoch, deserialized.coordinator_epoch());
    }

    /// Java: `testDeserializeNewerVersion`. Version 5 (> HIGHEST) is clamped to the
    /// highest supported version, which reads only the known `coordinator_epoch`
    /// field and ignores the unexpected trailing data.
    #[test]
    fn test_deserialize_newer_version() {
        let coordinator_epoch = 79i32;
        // ByteBuffer.allocate(8): putShort(5), putInt(coordinatorEpoch), putShort(0).
        let mut value = Vec::with_capacity(8);
        value.extend_from_slice(&5i16.to_be_bytes());
        value.extend_from_slice(&coordinator_epoch.to_be_bytes());
        value.extend_from_slice(&0i16.to_be_bytes()); // unexpected data
        let deserialized = EndTransactionMarker::deserialize_value(ControlRecordType::Commit, &value)
            .expect("a newer version is clamped and parsed");
        assert_eq!(coordinator_epoch, deserialized.coordinator_epoch());
    }

    /// Java: `testSerializeAndDeserialize`.
    #[test]
    fn test_serialize_and_deserialize() {
        for control_type in VALID_CONTROLLER_RECORD_TYPE {
            for _version in EndTxnMarkerData::LOWEST_SUPPORTED_VERSION..=EndTxnMarkerData::HIGHEST_SUPPORTED_VERSION {
                let marker = EndTransactionMarker::new(control_type, 1).expect("a valid type");
                let value = marker.serialize_value();
                let deserialized_marker =
                    EndTransactionMarker::deserialize_value(control_type, value).expect("round-trips");
                assert_eq!(marker, deserialized_marker);
            }
        }
    }

    /// Java: `testEndTxnMarkerValueSize`.
    #[test]
    fn test_end_txn_marker_value_size() {
        for control_type in VALID_CONTROLLER_RECORD_TYPE {
            let marker = EndTransactionMarker::new(control_type, 1).expect("a valid type");
            let offset_size = size_of_varint(0);
            let timestamp_size = size_of_varlong(0);
            let key_size = control_type.control_record_key_size() as i32;
            let value_size = marker.serialize_value().len() as i32;
            let header_size = size_of_varint(RecordBatch::EMPTY_HEADERS.len() as i32);
            let total_size = 1
                + offset_size
                + timestamp_size
                + size_of_varint(key_size)
                + key_size
                + size_of_varint(value_size)
                + value_size
                + header_size;
            assert_eq!(size_of_varint(total_size) + total_size, marker.end_txn_marker_value_size());
        }
    }

    /// Java: `testBackwardDeserializeCompatibility`. The old hard-coded v0 schema
    /// (`version` int16, `coordinator_epoch` int32, both big-endian) serializes to
    /// exactly these bytes, and the new deserializer still reads them.
    #[test]
    fn test_backward_deserialize_compatibility() {
        let coordinator_epoch = 10i32;
        for control_type in VALID_CONTROLLER_RECORD_TYPE {
            for version in EndTxnMarkerData::LOWEST_SUPPORTED_VERSION..=EndTxnMarkerData::HIGHEST_SUPPORTED_VERSION {
                let mut old_version_buffer = Vec::with_capacity(6);
                old_version_buffer.extend_from_slice(&version.to_be_bytes());
                old_version_buffer.extend_from_slice(&coordinator_epoch.to_be_bytes());

                let deserialized_marker = EndTransactionMarker::deserialize_value(control_type, &old_version_buffer)
                    .expect("an old-format value parses");
                assert_eq!(coordinator_epoch, deserialized_marker.coordinator_epoch());
                assert_eq!(control_type, deserialized_marker.control_type());
            }
        }
    }

    /// Java: `testForwardDeserializeCompatibility`. A value written with the new
    /// schema is readable by the old v0 layout (`version` int16, `coordinator_epoch`
    /// int32).
    #[test]
    fn test_forward_deserialize_compatibility() {
        let coordinator_epoch = 10i32;
        for control_type in VALID_CONTROLLER_RECORD_TYPE {
            for _version in EndTxnMarkerData::LOWEST_SUPPORTED_VERSION..=EndTxnMarkerData::HIGHEST_SUPPORTED_VERSION {
                let marker = EndTransactionMarker::new(control_type, coordinator_epoch).expect("a valid type");
                let new_version_buffer = marker.serialize_value();
                // Read the value the way the old v0 schema would: skip the 2-byte
                // version prefix and read the coordinator_epoch int32.
                let epoch = i32::from_be_bytes([
                    new_version_buffer[2],
                    new_version_buffer[3],
                    new_version_buffer[4],
                    new_version_buffer[5],
                ]);
                let deserialized_marker = EndTransactionMarker::new(control_type, epoch).expect("a valid type");
                assert_eq!(marker, deserialized_marker);
            }
        }
    }
}
