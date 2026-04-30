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

//! Slim translations of `org.apache.kafka.common.protocol.{Readable, Writable}`.
//!
//! Phase 2b only needs the operations used by [`super::RawTaggedFieldWriter`]
//! (and a handful of helpers reused in tests). Phase 2c will move these
//! traits to `crate::common::protocol::{readable, writable}` and expand them
//! to the full Java contract (record sets, primitive reads, varint helpers).
//! Until then, both traits live here so callers do not depend on a module
//! that does not yet exist.

use crate::common::errors::KafkaError;

/// Translation of the relevant subset of
/// `org.apache.kafka.common.protocol.Writable`.
///
/// Phase 2b only declares the methods that [`super::RawTaggedFieldWriter`]
/// invokes; the full Writable contract (record sets, primitives, varlongs)
/// is added in Phase 2c.
pub trait Writable {
    /// Append a single byte. Mirrors `Writable#writeByte`.
    fn write_byte(&mut self, byte: i8);

    /// Append `data` verbatim. Mirrors `Writable#writeByteArray`.
    fn write_byte_array(&mut self, data: &[u8]);

    /// Append `value` as an unsigned varint. Mirrors
    /// `Writable#writeUnsignedVarint`.
    fn write_unsigned_varint(&mut self, value: u32);
}

/// Translation of the relevant subset of
/// `org.apache.kafka.common.protocol.Readable`. Currently unused by any
/// Phase 2b source class but defined here so Phase 2c can extend it without
/// a breaking move.
pub trait Readable {
    /// Read a single byte. Mirrors `Readable#readByte`.
    fn read_byte(&mut self) -> Result<i8, KafkaError>;

    /// Read `n` bytes into a freshly allocated `Vec`. Mirrors
    /// `Readable#readByteArray`.
    fn read_byte_array(&mut self, n: usize) -> Result<Vec<u8>, KafkaError>;

    /// Read an unsigned varint. Mirrors `Readable#readUnsignedVarint`.
    fn read_unsigned_varint(&mut self) -> Result<u32, KafkaError>;
}

/// A simple `Writable` over a `&mut Vec<u8>`. Mirrors how
/// `ByteBufferAccessor` wraps a `ByteBuffer` in the Java client. Phase 2c
/// will provide a richer accessor that tracks position and limit.
pub struct VecWritable<'a> {
    buffer: &'a mut Vec<u8>,
}

impl<'a> VecWritable<'a> {
    /// Wrap an existing `Vec<u8>` so writes append to its tail.
    pub fn new(buffer: &'a mut Vec<u8>) -> Self {
        VecWritable { buffer }
    }
}

impl Writable for VecWritable<'_> {
    fn write_byte(&mut self, byte: i8) {
        self.buffer.push(byte as u8);
    }

    fn write_byte_array(&mut self, data: &[u8]) {
        self.buffer.extend_from_slice(data);
    }

    fn write_unsigned_varint(&mut self, value: u32) {
        crate::common::utils::byte_utils::write_unsigned_varint(value, self.buffer);
    }
}

/// A `Writable` that writes to a fixed-size byte slice at a tracked
/// position. Mirrors how `ByteBufferAccessor` wraps a pre-allocated
/// `ByteBuffer`. The `RawTaggedFieldWriterTest` requires this shape so it
/// can pre-zero the destination buffer and assert on its bytes.
pub struct SliceWritable<'a> {
    buffer: &'a mut [u8],
    position: usize,
}

impl<'a> SliceWritable<'a> {
    /// Wrap a slice; writes start at index `0`.
    pub fn new(buffer: &'a mut [u8]) -> Self {
        SliceWritable { buffer, position: 0 }
    }

    /// Returns the current write index.
    pub fn position(&self) -> usize {
        self.position
    }
}

impl Writable for SliceWritable<'_> {
    fn write_byte(&mut self, byte: i8) {
        self.buffer[self.position] = byte as u8;
        self.position += 1;
    }

    fn write_byte_array(&mut self, data: &[u8]) {
        self.buffer[self.position..self.position + data.len()].copy_from_slice(data);
        self.position += data.len();
    }

    fn write_unsigned_varint(&mut self, value: u32) {
        // Write to a temporary 5-byte stage buffer to reuse the canonical
        // implementation, then copy into the slice.
        let mut tmp: Vec<u8> = Vec::with_capacity(5);
        crate::common::utils::byte_utils::write_unsigned_varint(value, &mut tmp);
        self.write_byte_array(&tmp);
    }
}
