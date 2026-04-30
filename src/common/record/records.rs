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

//! Translation of `org.apache.kafka.common.record.Records`.

use crate::common::errors::KafkaError;
use crate::common::record::{Record, RecordBatch, TransferableRecords};

/// Offset at the start of a v0/v1 batch frame.
pub const OFFSET_OFFSET: usize = 0;
/// Length in bytes of the offset field at the start of a batch frame.
pub const OFFSET_LENGTH: usize = 8;
/// Offset of the size field, immediately following the offset.
pub const SIZE_OFFSET: usize = OFFSET_OFFSET + OFFSET_LENGTH;
/// Length in bytes of the size field.
pub const SIZE_LENGTH: usize = 4;
/// Total log overhead (offset + size).
pub const LOG_OVERHEAD: usize = SIZE_OFFSET + SIZE_LENGTH;

/// Offset of the magic byte. The magic byte is at the same offset across all
/// current message formats, but the four bytes between size and magic are
/// version-dependent.
pub const MAGIC_OFFSET: usize = LOG_OVERHEAD + 4;
/// Length in bytes of the magic byte.
pub const MAGIC_LENGTH: usize = 1;
/// Header size up to and including the magic byte.
pub const HEADER_SIZE_UP_TO_MAGIC: usize = MAGIC_OFFSET + MAGIC_LENGTH;

/// Interface for accessing the records contained in a log. The log itself is
/// represented as a sequence of record batches (see [`RecordBatch`]).
///
/// Mirrors Java's `Records` interface. Notes on the translation:
///
/// * Java's `batchIterator()` returns `AbstractIterator<? extends RecordBatch>`
///   for callers that want `peek()`. In Rust the standard `Iterator::peekable`
///   adapter provides `peek` on any iterator, so we omit the second method —
///   callers wrap `batches()` in `.peekable()` themselves.
/// * The default impls of [`Records::last_batch`] and
///   [`Records::has_matching_magic`] mirror Java's `AbstractRecords`
///   overrides; concrete Phase 3d types automatically inherit them.
/// * [`Records::slice`] and [`Records::records`] need a backing
///   `LogInputStream` (Phase 3d) to implement; concrete impls land in
///   Phase 3d.
pub trait Records: TransferableRecords {
    /// Iterate over the record batches.
    fn batches<'a>(&'a self) -> Box<dyn Iterator<Item = Box<dyn RecordBatch + 'a>> + 'a>;

    /// Return the last record batch, if any. Default impl mirrors Java's
    /// `AbstractRecords#lastBatch` (walks every batch — expensive).
    fn last_batch<'a>(&'a self) -> Option<Box<dyn RecordBatch + 'a>> {
        let mut last = None;
        for batch in self.batches() {
            last = Some(batch);
        }
        last
    }

    /// Whether every batch in this set has the supplied magic value.
    /// Default impl mirrors Java's `AbstractRecords#hasMatchingMagic`.
    fn has_matching_magic(&self, magic: i8) -> bool {
        for batch in self.batches() {
            if batch.magic() != magic {
                return false;
            }
        }
        true
    }

    /// Iterate over the (deeply-decompressed) records in this log. Java
    /// provides a default in `AbstractRecords`; the Rust default lives on
    /// concrete Phase 3d implementations because it requires retaining a
    /// per-batch iterator that borrows from the trait object — a self-
    /// referential pattern that needs the concrete batch type to express.
    fn records<'a>(&'a self) -> Box<dyn Iterator<Item = Box<dyn Record + 'a>> + 'a>;

    /// Return a slice of records from this instance, which is a view into the
    /// set starting from `position` and limited to `size` bytes. The position
    /// is expected to be aligned to a batch boundary, else the resulting slice
    /// cannot be iterated.
    ///
    /// Java's `Records.slice(int position, int size)` declares no `throws`
    /// but raises `IllegalArgumentException` (unchecked) on invalid
    /// arguments — see `MemoryRecords#slice` and `FileRecords#slice`. Per
    /// CLAUDE.md rule 10, the Rust translation returns
    /// `Err(KafkaError)` (typically `KafkaError::IllegalArgument`) so
    /// argument validation is explicit at every call site, rather than
    /// matching Java's unchecked-exception model with a panic.
    fn slice(&self, position: i32, size: i32) -> Result<Box<dyn Records + '_>, KafkaError>;
}
