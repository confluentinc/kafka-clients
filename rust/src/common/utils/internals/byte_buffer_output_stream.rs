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

//! The buffer a `MemoryRecordsBuilder` writes into: one growable buffer, or fixed chunks.
//!
//! Corresponds to Java's `org.apache.kafka.common.utils.internals.ByteBufferOutputStream`.

use bytes::{Bytes, BytesMut};

use crate::common::Error;
use crate::producer::internals::ChunkedByteBufferOutputStream;

/// The output stream behind a `MemoryRecordsBuilder`.
///
/// Java 4.4 has a concrete `ByteBufferOutputStream` (a single buffer that is reallocated to grow)
/// and `ChunkedByteBufferOutputStream extends ByteBufferOutputStream` (KIP-1332's fixed chunks).
/// Java 4.5 makes the base abstract and moves the single-buffer behaviour into
/// `SingleByteBufferOutputStream` (KAFKA-20807). Rust has no inheritance, so the two concrete
/// streams are the two variants of this enum, which already has 4.5's shape.
///
/// **Why a new type (DoD #7):** the builder must hold either stream, and the per-record write path
/// must not pay for dynamic dispatch (CLAUDE.md §13), so this is an enum rather than a trait
/// object. The `Single` variant is the one buffer the builder held before KIP-1332; the builder
/// matches on the variant at each use, so the full allocation strategy runs the same code as
/// before.
///
/// **Why `BytesMut` and a `Frozen` state:** Java's `close()` leaves the batch in the stream's
/// `ByteBuffer` and hands out `builtRecords` as a `duplicate().slice()` of it — two views of one
/// allocation. Rust spells the same thing with `bytes`: the single buffer is a [`BytesMut`] while
/// it is written, and `close()` [freezes](BytesMut::freeze) it into the `Frozen` state, whose
/// [`Bytes`] the `MemoryRecords` is an O(1) slice of (CLAUDE.md §14: batch finalisation must not
/// copy already-serialized bytes). The frozen buffer is turned back into a `BytesMut`, in place,
/// when the batch is reopened (`reopenAndRewriteProducerState`) and when it is returned to the
/// `BufferPool`, so pooling works as before.
#[doc(alias = "org.apache.kafka.common.utils.internals.ByteBufferOutputStream")]
#[derive(Debug)]
pub(crate) enum ByteBufferOutputStream {
    /// A single buffer that grows on demand: Java 4.4's `ByteBufferOutputStream` (4.5's
    /// `SingleByteBufferOutputStream`). The buffer's length is the stream position.
    Single(BytesMut),
    /// The single buffer once the builder has closed it: Java's `ByteBuffer` after `close()`,
    /// which only the batch header is written into. Its length is the stream position.
    Frozen(Bytes),
    /// KIP-1332's chunk-backed stream, used by the incremental buffer.memory allocation strategy.
    Chunked(ChunkedByteBufferOutputStream),
}

impl ByteBufferOutputStream {
    /// The current write position.
    ///
    /// # Errors
    ///
    /// `IllegalState` for a chunked stream that has been deallocated.
    #[doc(alias = "org.apache.kafka.common.utils.internals.ByteBufferOutputStream#position")]
    pub(crate) fn position(&self) -> Result<usize, Error> {
        match self {
            ByteBufferOutputStream::Single(buffer) => Ok(buffer.len()),
            ByteBufferOutputStream::Frozen(buffer) => Ok(buffer.len()),
            ByteBufferOutputStream::Chunked(stream) => stream.position(),
        }
    }

    /// Whether this is the chunked stream (Java's `instanceof ChunkedByteBufferOutputStream`).
    pub(crate) fn is_chunked(&self) -> bool {
        matches!(self, ByteBufferOutputStream::Chunked(_))
    }

    /// The chunked stream, if this is one (Java's cast after `instanceof`).
    pub(crate) fn as_chunked_mut(&mut self) -> Option<&mut ChunkedByteBufferOutputStream> {
        match self {
            ByteBufferOutputStream::Chunked(stream) => Some(stream),
            ByteBufferOutputStream::Single(_) | ByteBufferOutputStream::Frozen(_) => None,
        }
    }

    /// Makes a `Frozen` single buffer writable again, in place when nothing else references it
    /// (every `MemoryRecords` view of it has been dropped) and by copying it otherwise, which is
    /// what keeps an outstanding view unchanged. `Single` and `Chunked` streams are left as they
    /// are.
    pub(crate) fn thaw(&mut self) {
        if let ByteBufferOutputStream::Frozen(frozen) = self {
            let buffer = std::mem::take(frozen)
                .try_into_mut()
                .unwrap_or_else(|shared| BytesMut::from(&shared[..]));
            *self = ByteBufferOutputStream::Single(buffer);
        }
    }
}
