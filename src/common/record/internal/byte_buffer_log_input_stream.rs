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

//! A byte buffer backed log input stream.
//!
//! Translated from
//! `org.apache.kafka.common.record.internal.ByteBufferLogInputStream`.

use crate::common::Error;
use crate::common::protocol::Errors;
use crate::common::record::internal::AbstractRecords;
use crate::common::record::internal::DefaultRecordBatchRef;
use crate::common::record::internal::LegacyRecord;
use crate::common::record::internal::RecordBatch;

/// A byte buffer backed log input stream. This class avoids the need to copy
/// records by returning slices from the underlying byte buffer.
///
/// Corresponds to Java's `ByteBufferLogInputStream`
/// (`ByteBufferLogInputStream.java:32-88`). Java's `ByteBuffer` carries its own
/// read position; here the stream borrows the buffer and keeps the position
/// itself, so `remaining()` is `buffer.len() - position`.
///
/// Every batch goes through [`next_batch_size`](Self::next_batch_size) — its
/// sign, its minimum size and its magic — before [`next_batch`](Self::next_batch)
/// hands it out, and the handed-out view is limited to exactly the declared
/// size. Both properties are what make the walk safe over bytes a broker sent:
/// a batch's header is never trusted to say where the *next* batch starts until
/// it has been checked, and no view reaches past the batch it describes.
///
/// Java's class implements `LogInputStream<MutableRecordBatch>`, whose only
/// other implementation reads from a file; this client has no file-backed
/// records, so there is no Rust trait for it.
pub(crate) struct ByteBufferLogInputStream<'a> {
    /// The records being walked; never modified.
    buffer: &'a [u8],
    /// Java's `buffer.position()`: the offset of the next batch header.
    position: usize,
    /// Batches whose record size exceeds this are rejected as corrupt.
    max_message_size: i32,
}

impl<'a> ByteBufferLogInputStream<'a> {
    /// Creates a stream positioned at the start of `buffer`.
    ///
    /// Corresponds to Java's `ByteBufferLogInputStream(ByteBuffer, int)`.
    pub(crate) fn new(buffer: &'a [u8], max_message_size: i32) -> Self {
        Self { buffer, position: 0, max_message_size }
    }

    /// Returns the next batch, or `None` when the buffer holds no further
    /// complete batch.
    ///
    /// Corresponds to Java's `nextBatch()` (`ByteBufferLogInputStream.java:41-58`):
    /// the header is validated by [`next_batch_size`](Self::next_batch_size), a
    /// batch that is not wholly present is no batch (`:44-46`), and the returned
    /// view is limited to exactly the declared size (`batchSlice.limit(batchSize)`,
    /// `:51`) before the position moves past it.
    ///
    /// # Errors
    ///
    /// - [`Errors::CorruptMessage`] from [`next_batch_size`](Self::next_batch_size).
    /// - An unsupported-version error ([`unsupported_magic_error`](Self::unsupported_magic_error))
    ///   for magic v0
    ///   or v1. Java returns an `AbstractLegacyRecordBatch.ByteBufferLegacyRecordBatch`
    ///   here (`:57`); this client does not implement the legacy formats, which
    ///   Kafka 4.0 removed (KIP-724), and fails the batch instead of misreading
    ///   its header as a v2 one.
    /// - An invalid-record error for a v2 batch shorter than
    ///   [`RecordBatch::RECORD_BATCH_OVERHEAD`] ([`DefaultRecordBatchRef::new`]).
    ///   Java hands out a `DefaultRecordBatch` whose header reads past the end
    ///   would throw `IndexOutOfBoundsException`; the Rust view refuses to exist,
    ///   because those reads would be slice panics.
    ///
    /// In both of the last two cases the position has already moved past the
    /// batch, as in Java, whose `:52` runs before the batch class is chosen.
    pub(crate) fn next_batch(&mut self) -> Result<Option<DefaultRecordBatchRef<'a>>, Error> {
        let remaining = self.remaining();

        let Some(batch_size) = self.next_batch_size()? else {
            return Ok(None);
        };
        if remaining < batch_size {
            return Ok(None);
        }

        // In bounds: `next_batch_size` returns a size only once
        // `HEADER_SIZE_UP_TO_MAGIC` bytes are present, and `remaining >= batch_size`.
        let magic = self.buffer[self.position + RecordBatch::MAGIC_OFFSET] as i8;
        let batch_slice = &self.buffer[self.position..self.position + batch_size];
        self.position += batch_size;

        if magic > RecordBatch::MAGIC_VALUE_V1 {
            DefaultRecordBatchRef::new(batch_slice).map(Some).map_err(Error::InvalidRecord)
        } else {
            Err(Self::unsupported_magic_error(magic))
        }
    }

    /// Validates the header of the next batch and returns batch size.
    ///
    /// Returns the next batch size including `LOG_OVERHEAD` if the buffer
    /// contains the header up to the magic byte, `None` otherwise.
    ///
    /// Corresponds to Java's `nextBatchSize()` (`ByteBufferLogInputStream.java:66-87`),
    /// check for check and message for message. The record size is read as a
    /// signed `i32` and compared signed, so a negative length is rejected as too
    /// small rather than wrapping to an enormous `usize`, and the returned size is
    /// computed only after the checks, so it cannot overflow.
    ///
    /// # Errors
    ///
    /// Returns [`Errors::CorruptMessage`] (Java's `CorruptRecordException`) if
    /// the record size or the magic is invalid.
    pub(crate) fn next_batch_size(&self) -> Result<Option<usize>, Error> {
        let remaining = self.remaining();
        if remaining < AbstractRecords::LOG_OVERHEAD {
            return Ok(None);
        }
        // Java's `Records.SIZE_OFFSET` is `DefaultRecordBatch.LENGTH_OFFSET` (8).
        let Some(size_bytes) = self
            .buffer
            .get(self.position + RecordBatch::LENGTH_OFFSET..)
            .and_then(|tail| tail.first_chunk::<{ RecordBatch::LENGTH_LENGTH }>())
        else {
            return Ok(None);
        };
        let record_size = i32::from_be_bytes(*size_bytes);
        // V0 has the smallest overhead, stricter checking is done later
        if record_size < LegacyRecord::RECORD_OVERHEAD_V0 {
            return Err(Error::with_message(
                Errors::CorruptMessage,
                format!(
                    "Record size {record_size} is less than the minimum record overhead ({})",
                    LegacyRecord::RECORD_OVERHEAD_V0
                ),
            ));
        }
        if record_size > self.max_message_size {
            return Err(Error::with_message(
                Errors::CorruptMessage,
                format!(
                    "Record size {record_size} exceeds the largest allowable message size ({}).",
                    self.max_message_size
                ),
            ));
        }

        if remaining < AbstractRecords::HEADER_SIZE_UP_TO_MAGIC {
            return Ok(None);
        }

        // Java's `Records.MAGIC_OFFSET` is `DefaultRecordBatch.MAGIC_OFFSET` (16).
        let magic = self.buffer[self.position + RecordBatch::MAGIC_OFFSET] as i8;
        if !(0..=RecordBatch::CURRENT_MAGIC_VALUE).contains(&magic) {
            return Err(Error::with_message(
                Errors::CorruptMessage,
                format!("Invalid magic found in record: {magic}"),
            ));
        }

        // `record_size >= RECORD_OVERHEAD_V0 > 0` here, so the widening is lossless.
        Ok(Some(AbstractRecords::LOG_OVERHEAD + record_size as usize))
    }

    /// Java's `buffer.remaining()`.
    fn remaining(&self) -> usize {
        self.buffer.len().saturating_sub(self.position)
    }

    /// The error for a batch in message format v0 or v1, which this client does not
    /// read (APPSEC-7665 D7).
    ///
    /// Java reads such a batch as an `AbstractLegacyRecordBatch`. This client
    /// implements only message format v2 — Kafka 4.0 removed v0 and v1 (KIP-724), so
    /// a 4.x broker never sends them — and per CLAUDE.md §5 an unimplemented Java
    /// path fails loudly: without the check, a legacy header (as small as 26 bytes)
    /// would either trip the v2 minimum-size check with a misleading "corrupt"
    /// message or, past it, be read as a v2 header. The error class is the one this
    /// crate uses for a Java path it does not implement. Shared with the consumer's
    /// receive path, which reads the magic off the header itself.
    pub(crate) fn unsupported_magic_error(magic: i8) -> Error {
        Error::unsupported_version(format!(
            "Record batch magic v{magic} is not supported: this client reads only magic v2 record batches \
             (message formats v0 and v1 were removed in Kafka 4.0 by KIP-724)"
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::compress::Compression;
    use crate::common::record::TimestampType;
    use crate::common::record::internal::MemoryRecords;

    /// One uncompressed v2 batch at `base_offset` holding `records` as
    /// `(timestamp, key, value)` — Java's `MemoryRecords.builder(buffer,
    /// Compression.NONE, TimestampType.CREATE_TIME, baseOffset)` fixtures.
    fn batch(base_offset: i64, records: &[(i64, &[u8], &[u8])]) -> Vec<u8> {
        let mut builder = MemoryRecords::builder_with_initial_capacity(
            1024,
            Compression::none(),
            TimestampType::CreateTime,
            base_offset,
        );
        for &(timestamp, key, value) in records {
            builder.append_kv(timestamp, Some(key), Some(value));
        }
        builder.build().buffer().to_vec()
    }

    /// The fixture of the first three Java tests: offsets 0-1 in one batch and
    /// 2-3 in the next. Returns the buffer and the position of the second batch.
    fn two_batches() -> (Vec<u8>, usize) {
        let mut buffer = batch(0, &[(15, b"a", b"1"), (20, b"b", b"2")]);
        let position = buffer.len();
        buffer.extend_from_slice(&batch(2, &[(30, b"c", b"3"), (40, b"d", b"4")]));
        (buffer, position)
    }

    /// Translated from `ByteBufferLogInputStreamTest.iteratorIgnoresIncompleteEntries`.
    #[test]
    fn test_iterator_ignores_incomplete_entries() {
        let (mut buffer, _) = two_batches();
        buffer.truncate(buffer.len() - 5);

        // Java drives this one through `MemoryRecords.readableRecords(buffer).batches()`.
        let records = MemoryRecords::readable_records(&buffer);
        let mut batches = records.batches();
        let first = batches.next().expect("the first batch is complete");
        assert_eq!(1, first.last_offset());
        assert!(batches.next().is_none());

        // The stream underneath agrees, and a truncated batch is not an error.
        let mut stream = ByteBufferLogInputStream::new(&buffer, i32::MAX);
        assert_eq!(
            1,
            stream.next_batch().unwrap().expect("the first batch is complete").last_offset()
        );
        assert!(stream.next_batch().unwrap().is_none());
    }

    /// Translated from `ByteBufferLogInputStreamTest.iteratorRaisesOnTooSmallRecords`.
    #[test]
    fn test_iterator_raises_on_too_small_records() {
        let (mut buffer, position) = two_batches();
        let length = position + RecordBatch::LENGTH_OFFSET;
        buffer[length..length + 4].copy_from_slice(&9_i32.to_be_bytes());

        let mut stream = ByteBufferLogInputStream::new(&buffer, i32::MAX);
        assert!(stream.next_batch().unwrap().is_some());
        let err = stream
            .next_batch()
            .expect_err("a 9-byte record size is below every format's overhead");
        // Java throws `CorruptRecordException`, the class of `CORRUPT_MESSAGE`.
        assert!(matches!(err, Error::CorruptRecord(_)), "{err:?}");
        assert_eq!(Errors::CorruptMessage, err.error());
        assert_eq!("Record size 9 is less than the minimum record overhead (14)", err.message());
    }

    /// Translated from `ByteBufferLogInputStreamTest.iteratorRaisesOnInvalidMagic`.
    #[test]
    fn test_iterator_raises_on_invalid_magic() {
        let (mut buffer, position) = two_batches();
        buffer[position + RecordBatch::MAGIC_OFFSET] = 37;

        let mut stream = ByteBufferLogInputStream::new(&buffer, i32::MAX);
        assert!(stream.next_batch().unwrap().is_some());
        let err = stream.next_batch().expect_err("magic 37 is not a message format");
        assert!(matches!(err, Error::CorruptRecord(_)), "{err:?}");
        assert_eq!(Errors::CorruptMessage, err.error());
        assert_eq!("Invalid magic found in record: 37", err.message());
    }

    /// Translated from `ByteBufferLogInputStreamTest.iteratorRaisesOnTooLargeRecords`.
    #[test]
    fn test_iterator_raises_on_too_large_records() {
        let mut buffer = batch(0, &[(15, b"a", b"1")]);
        let position = buffer.len();
        buffer.extend_from_slice(&batch(2, &[(30, b"c", b"3"), (40, b"d", b"4")]));
        // Fixture precondition: the first batch fits the limit and the second
        // does not. Each record is 9 bytes (a 1-byte length varint and an 8-byte
        // body), so the record sizes are 61 + 9 - 12 = 58 and 61 + 18 - 12 = 67.
        let length = position + RecordBatch::LENGTH_OFFSET;
        assert_eq!(
            58,
            i32::from_be_bytes(
                buffer[RecordBatch::LENGTH_OFFSET..RecordBatch::LENGTH_OFFSET + 4]
                    .try_into()
                    .unwrap()
            )
        );
        assert_eq!(67, i32::from_be_bytes(buffer[length..length + 4].try_into().unwrap()));

        let mut stream = ByteBufferLogInputStream::new(&buffer, 60);
        assert!(stream.next_batch().unwrap().is_some());
        let err = stream.next_batch().expect_err("the second batch exceeds the 60-byte limit");
        assert!(matches!(err, Error::CorruptRecord(_)), "{err:?}");
        assert_eq!(Errors::CorruptMessage, err.error());
        assert_eq!("Record size 67 exceeds the largest allowable message size (60).", err.message());
    }

    /// A negative length is compared signed — the finding this type exists to
    /// close was an unchecked `i32 as usize` that read -5 as a size near
    /// `usize::MAX`.
    #[test]
    fn test_next_batch_size_rejects_a_negative_record_size() {
        let (mut buffer, position) = two_batches();
        let length = position + RecordBatch::LENGTH_OFFSET;
        buffer[length..length + 4].copy_from_slice(&(-5_i32).to_be_bytes());

        let mut stream = ByteBufferLogInputStream::new(&buffer, i32::MAX);
        assert!(stream.next_batch().unwrap().is_some());
        let err = stream.next_batch_size().expect_err("a negative size is corrupt");
        assert_eq!(Errors::CorruptMessage, err.error());
        assert_eq!("Record size -5 is less than the minimum record overhead (14)", err.message());
    }

    /// Java's two "not yet" answers (`ByteBufferLogInputStream.java:68-69`,
    /// `:79-80`): fewer than `LOG_OVERHEAD` bytes, and a valid size without the
    /// magic byte.
    #[test]
    fn test_next_batch_size_returns_none_until_the_magic_byte_is_present() {
        let (buffer, first_batch_size) = two_batches();
        for len in 0..AbstractRecords::HEADER_SIZE_UP_TO_MAGIC {
            assert_eq!(
                None,
                ByteBufferLogInputStream::new(&buffer[..len], i32::MAX)
                    .next_batch_size()
                    .unwrap()
            );
        }
        // The size is known from the header alone; the body need not be present.
        assert_eq!(
            Some(first_batch_size),
            ByteBufferLogInputStream::new(&buffer[..AbstractRecords::HEADER_SIZE_UP_TO_MAGIC], i32::MAX)
                .next_batch_size()
                .unwrap()
        );
        // A size is checked before the magic byte is needed (`:70-77` precede `:79`).
        let mut short = buffer[..AbstractRecords::LOG_OVERHEAD].to_vec();
        short[RecordBatch::LENGTH_OFFSET..RecordBatch::LENGTH_OFFSET + 4].copy_from_slice(&3_i32.to_be_bytes());
        let err = ByteBufferLogInputStream::new(&short, i32::MAX).next_batch_size().unwrap_err();
        assert_eq!("Record size 3 is less than the minimum record overhead (14)", err.message());
    }

    /// `next_batch` hands out a view limited to exactly the declared size
    /// (`batchSlice.limit(batchSize)`, `:51`) and moves past it.
    #[test]
    fn test_next_batch_view_is_limited_to_the_declared_size() {
        let (buffer, position) = two_batches();
        let mut stream = ByteBufferLogInputStream::new(&buffer, i32::MAX);

        let first = stream.next_batch().unwrap().expect("first batch");
        assert_eq!(position, first.buffer().len());
        assert_eq!(position, first.size_in_bytes());
        assert_eq!(0, first.base_offset());

        let second = stream.next_batch().unwrap().expect("second batch");
        assert_eq!(buffer.len() - position, second.buffer().len());
        assert_eq!(2, second.base_offset());
        assert_eq!(3, second.last_offset());

        assert!(stream.next_batch().unwrap().is_none());
    }

    /// D7: a batch in message format v0 or v1 is refused by its magic, not read
    /// as a v2 header.
    #[test]
    fn test_next_batch_rejects_legacy_magic() {
        for magic in [RecordBatch::MAGIC_VALUE_V0, RecordBatch::MAGIC_VALUE_V1] {
            let (mut buffer, position) = two_batches();
            buffer[position + RecordBatch::MAGIC_OFFSET] = magic as u8;

            let mut stream = ByteBufferLogInputStream::new(&buffer, i32::MAX);
            assert!(stream.next_batch().unwrap().is_some());
            // `next_batch_size` accepts the legacy magic, as Java's does...
            assert_eq!(Some(buffer.len() - position), stream.next_batch_size().unwrap());
            // ...and `next_batch` refuses to hand out a v2 view of it.
            let err = stream.next_batch().expect_err("a legacy batch has no v2 view");
            assert_eq!(Errors::UnsupportedVersion, err.error(), "{err:?}");
            assert_eq!(
                format!(
                    "Record batch magic v{magic} is not supported: this client reads only magic v2 record batches \
                     (message formats v0 and v1 were removed in Kafka 4.0 by KIP-724)"
                ),
                err.message()
            );
            // The legacy batch's own size was valid, so the walk moved past it.
            assert!(stream.next_batch().unwrap().is_none());
        }
    }

    /// D2: a v2 batch whose declared size passes `next_batch_size` (at least
    /// `LOG_OVERHEAD + 14` bytes) but is below the 61-byte v2 header has no view:
    /// every header accessor past the end would be a slice panic.
    #[test]
    fn test_next_batch_rejects_a_v2_batch_below_the_header_size() {
        for batch_size in AbstractRecords::LOG_OVERHEAD + LegacyRecord::RECORD_OVERHEAD_V0 as usize
            ..RecordBatch::RECORD_BATCH_OVERHEAD
        {
            let mut buffer = vec![0_u8; batch_size];
            let length = (batch_size - AbstractRecords::LOG_OVERHEAD) as i32;
            buffer[RecordBatch::LENGTH_OFFSET..RecordBatch::LENGTH_OFFSET + 4].copy_from_slice(&length.to_be_bytes());
            buffer[RecordBatch::MAGIC_OFFSET] = RecordBatch::MAGIC_VALUE_V2 as u8;

            let mut stream = ByteBufferLogInputStream::new(&buffer, i32::MAX);
            assert_eq!(Some(batch_size), stream.next_batch_size().unwrap());
            let err = stream.next_batch().expect_err("no v2 view below the header size");
            assert!(matches!(err, Error::InvalidRecord(_)), "{err:?}");
            assert_eq!(
                format!(
                    "Record batch is corrupt (the size {batch_size} is smaller than the minimum allowed overhead 61)"
                ),
                err.message()
            );
        }
    }
}
