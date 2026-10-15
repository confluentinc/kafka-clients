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
/// object. The `Single` variant is exactly the `Vec<u8>` the builder held before KIP-1332; the
/// builder matches on the variant at each use, so the full allocation strategy runs the same code
/// as before.
#[doc(alias = "org.apache.kafka.common.utils.internals.ByteBufferOutputStream")]
#[derive(Debug)]
pub(crate) enum ByteBufferOutputStream {
    /// A single buffer that grows on demand: Java 4.4's `ByteBufferOutputStream` (4.5's
    /// `SingleByteBufferOutputStream`). The `Vec`'s length is the stream position.
    Single(Vec<u8>),
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
            ByteBufferOutputStream::Single(_) => None,
        }
    }
}
