// Copyright 2026 Confluent Inc.
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

//! The incremental-allocation half of [`ProducerBatch`].
//!
//! Translated from `org.apache.kafka.clients.producer.internals.ChunkedProducerBatch`
//! (Apache Kafka 4.4, KIP-1332 / KAFKA-20578).
//!
//! # Deviation (DoD #7): the subclass is folded into `ProducerBatch`
//!
//! Java's `ChunkedProducerBatch extends ProducerBatch`. Rust has no inheritance, and the two
//! kinds share one partition deque (a split batch stays a plain `ProducerBatch` even in
//! incremental mode) while every batch moves **by value** between the accumulator, the `Sender`
//! and the `TransactionManager` (`producer-transactions.md` §7) — so a second type would need a
//! trait object or an enum in every one of those owners. Instead (PLAN §2.3):
//!
//! - one `ProducerBatch` type carries a single-or-chunked buffer, through its
//!   [`MemoryRecordsBuilder`]'s `ByteBufferOutputStream`;
//! - `instanceof ChunkedProducerBatch` becomes [`ProducerBatch::is_chunked`];
//! - the methods Java adds live in this file as an `impl` block on [`ChunkedProducerBatch`], a
//!   type alias of `ProducerBatch` that carries the Java class name;
//! - the three methods Java overrides (`tryAppend`, `deallocateBuffer`, `deallocateInflightBuffer`)
//!   branch on [`ProducerBatch::is_chunked`] inside the base methods in `producer_batch.rs`,
//!   each citing the override it folds in.
//!
//! This class is not thread safe and external synchronization must be used when modifying it.

use crate::common::Error;
use crate::common::TopicPartition;
use crate::common::header::RecordHeader;
use crate::common::record::internal::MemoryRecordsBuilder;
use crate::producer::internals::ChunkedByteBufferOutputStream;
use crate::producer::internals::ProducerBatch;

/// A [`ProducerBatch`] for the incremental buffer.memory allocation strategy, backed by a
/// [`MemoryRecordsBuilder`] whose stream is a [`ChunkedByteBufferOutputStream`]. It adds mid-batch
/// chunk extension support ([`extension_bytes_needed`](ProducerBatch::extension_bytes_needed) /
/// [`add_buffers`](ProducerBatch::add_buffers)) and overrides the pool deallocation hooks so all
/// chunks are returned to the pool rather than a single buffer.
///
/// An alias rather than a type: see the module docs for why the subclass is folded into
/// `ProducerBatch`.
#[doc(alias = "org.apache.kafka.clients.producer.internals.ChunkedProducerBatch")]
pub(crate) type ChunkedProducerBatch = ProducerBatch;

impl ChunkedProducerBatch {
    /// The `IllegalStateException` message of `ChunkedProducerBatch.tryAppend`: the first record
    /// of a chunked batch arrived without the chunk capacity its stream should have been pre-sized
    /// with.
    pub(crate) const UNSIZED_FIRST_APPEND_MESSAGE: &'static str = "Unexpected append to a chunked batch whose chunks lack capacity for the record; \
         the stream should have been pre-sized for the batch's first record";

    /// Java's `ChunkedProducerBatch(TopicPartition tp, MemoryRecordsBuilder recordsBuilder, long
    /// createdMs)`. Named `new_chunked` because the folded type already has Java's
    /// `ProducerBatch(..)` constructor of the same parameters as [`ProducerBatch::new`]; it
    /// carries no Java marker for that reason.
    ///
    /// # Errors
    ///
    /// [`Error::LocalIllegalArgument`] if `records_builder`'s stream is not a
    /// [`ChunkedByteBufferOutputStream`].
    pub(crate) fn new_chunked(
        tp: TopicPartition,
        records_builder: MemoryRecordsBuilder,
        created_ms: i64,
    ) -> Result<Self, Error> {
        if !records_builder.buffer_stream().is_chunked() {
            return Err(Error::local_illegal_argument(
                "recordsBuilder must be an instance of ChunkedByteBufferOutputStream, but found \
                 org.apache.kafka.common.utils.internals.ByteBufferOutputStream",
            ));
        }
        Ok(ProducerBatch::new(tp, records_builder, created_ms))
    }

    /// Whether this batch is backed by a chunked stream: Java's `instanceof ChunkedProducerBatch`.
    pub(crate) fn is_chunked(&self) -> bool {
        self.records_builder.buffer_stream().is_chunked()
    }

    /// Bytes of chunk capacity this batch needs before `try_append` could accept the given
    /// record. Returns 0 when no extension is needed: the batch is at its batch-size limit, or the
    /// attached chunk capacity already has room (always the case for an empty batch, whose stream
    /// is pre-sized for the first record). Positive when the record is within the batch-size limit
    /// but the attached chunks lack capacity — the accumulator then allocates exactly the missing
    /// bytes (rounded up to whole chunks) and attaches them via [`add_buffers`](Self::add_buffers)
    /// before retrying.
    ///
    /// Java's `TODO (KAFKA-20859): improve by reusing size calculation` is an optimisation note,
    /// not missing behaviour.
    ///
    /// Also 0 for a plain batch or a chunked one whose stream was deallocated: neither can take
    /// chunks. Java can only be asked about a live `ChunkedProducerBatch`.
    #[doc(alias = "org.apache.kafka.clients.producer.internals.ChunkedProducerBatch#extensionBytesNeeded")]
    pub(crate) fn extension_bytes_needed(
        &self,
        timestamp: i64,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: &[RecordHeader],
    ) -> i32 {
        if !self.records_builder.has_room_for(timestamp, key, value, headers) {
            return 0;
        }
        let Some(attached_capacity) = self.attached_capacity() else {
            return 0;
        };
        // Size against the batch's projected total output after this record (header counted once,
        // ratio-adjusted when compressed), not per-record. Per-record sizing would over-count the
        // header and miss the compressor's flush-accumulation behavior.
        let target = self.records_builder.estimated_bytes_written_after(key, value, headers);
        target.saturating_sub(attached_capacity).min(i32::MAX as usize) as i32
    }

    /// The chunk capacity attached to this batch's stream, or `None` for a plain batch or a
    /// deallocated stream.
    fn attached_capacity(&self) -> Option<usize> {
        match self.records_builder.buffer_stream() {
            crate::common::utils::internals::ByteBufferOutputStream::Chunked(stream) => stream.attached_capacity().ok(),
            crate::common::utils::internals::ByteBufferOutputStream::Single(_) => None,
        }
    }

    /// Attach pre-allocated chunks to this batch's stream so the next `try_append` can spill
    /// into them. Ownership of the chunks transfers to the stream.
    ///
    /// The accumulator calls this only while [`extension_bytes_needed`](Self::extension_bytes_needed)
    /// is positive, under the same deque lock, so the chunks are still needed when attached. Any
    /// never written to are returned to the pool when the batch closes for appends.
    ///
    /// `chunks` is drained only on success (see `ChunkedByteBufferOutputStream::add_buffers`), so
    /// on error the caller still holds them and returns them to the pool.
    ///
    /// # Errors
    ///
    /// [`Error::LocalIllegalState`] for a plain batch (Java's failing `ChunkedProducerBatch` cast),
    /// and the stream's errors: closed for appends, deallocated, or a chunk of the wrong size.
    #[doc(alias = "org.apache.kafka.clients.producer.internals.ChunkedProducerBatch#addBuffers")]
    pub(crate) fn add_buffers(&mut self, chunks: &mut Vec<Vec<u8>>) -> Result<(), Error> {
        self.stream()?.add_buffers(chunks)
    }

    /// The batch's chunked stream (Java's `(ChunkedByteBufferOutputStream)
    /// recordsBuilder.bufferStream()`).
    #[doc(alias = "org.apache.kafka.clients.producer.internals.ChunkedProducerBatch#stream")]
    fn stream(&mut self) -> Result<&mut ChunkedByteBufferOutputStream, Error> {
        self.records_builder
            .buffer_stream_mut()
            .as_chunked_mut()
            .ok_or_else(|| Error::local_illegal_state("not a chunked batch"))
    }
}
