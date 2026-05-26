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

//! Translation of `org.apache.kafka.common.utils.ByteBufferOutputStream`.

use std::io::{self, Write};

/// Reallocation factor when the buffer needs to expand. Matches Java
/// `REALLOCATION_FACTOR = 1.1f`.
const REALLOCATION_FACTOR: f64 = 1.1;

/// A growing-buffer [`Write`] adapter. Mirrors Java's `ByteBufferOutputStream`.
///
/// Rust's idiomatic equivalent is just `Vec<u8>` (which implements [`Write`]),
/// but the producer code keeps the explicit `position`/`limit`/`buffer()`
/// accessors the Java caller relies on. We back the storage with a `Vec<u8>`
/// pre-grown to `initial_capacity` so the position counter and the underlying
/// buffer length stay in sync.
pub struct ByteBufferOutputStream {
    buffer: Vec<u8>,
    position: usize,
    initial_position: usize,
    initial_capacity: usize,
    /// The "limit" — bytes beyond this are reserved capacity but not yet
    /// considered written. Mirrors `ByteBuffer.limit()`.
    limit: usize,
}

impl ByteBufferOutputStream {
    /// Construct a stream wrapping an existing buffer. The initial position
    /// is the first byte of the buffer, and the initial limit is the
    /// buffer's length.
    pub fn from_buffer(buffer: Vec<u8>) -> Self {
        let initial_capacity = buffer.capacity();
        let initial_position = 0;
        let limit = buffer.len();
        ByteBufferOutputStream { buffer, position: initial_position, initial_position, initial_capacity, limit }
    }

    /// Construct a stream with the requested initial capacity. Mirrors
    /// `new ByteBufferOutputStream(int)`. Match Java's
    /// `ByteBuffer.allocate(initial_capacity)` semantics: the buffer is
    /// "filled" with zeros up to its capacity (limit == capacity,
    /// position == 0).
    pub fn with_capacity(initial_capacity: usize) -> Self {
        let buffer = vec![0u8; initial_capacity];
        ByteBufferOutputStream {
            buffer,
            position: 0,
            initial_position: 0,
            initial_capacity,
            limit: initial_capacity,
        }
    }

    /// Current write position (bytes written so far). Mirrors `position()`.
    pub fn position(&self) -> usize {
        self.position
    }

    /// Bytes remaining in the current buffer between the position and the
    /// limit. Mirrors `remaining()`.
    pub fn remaining(&self) -> usize {
        self.limit.saturating_sub(self.position)
    }

    /// Limit (effective capacity). Mirrors `limit()`.
    pub fn limit(&self) -> usize {
        self.limit
    }

    /// Capacity at construction time, before any expansion. Mirrors
    /// `initialCapacity()`.
    pub fn initial_capacity(&self) -> usize {
        self.initial_capacity
    }

    /// Set the write cursor to `new_position`, expanding the buffer if
    /// `new_position` is beyond the current limit. Mirrors `position(int)`.
    pub fn set_position(&mut self, new_position: usize) {
        if new_position > self.position {
            self.ensure_remaining(new_position - self.position);
        }
        self.position = new_position;
    }

    /// Borrow the underlying buffer up to the current limit. Mirrors `buffer()`.
    pub fn buffer(&self) -> &[u8] {
        &self.buffer[..self.limit]
    }

    /// Mutably borrow the underlying buffer up to the current limit.
    pub fn buffer_mut(&mut self) -> &mut [u8] {
        &mut self.buffer[..self.limit]
    }

    /// Take the underlying buffer (consuming the stream). The returned
    /// `Vec<u8>` has length equal to the current position; bytes beyond the
    /// position were reserved capacity that isn't part of the written data.
    pub fn into_buffer(mut self) -> Vec<u8> {
        self.buffer.truncate(self.position);
        self.buffer
    }

    /// Ensure there's enough room to write `remaining_bytes_required` more
    /// bytes, expanding the buffer if necessary. Mirrors `ensureRemaining`.
    pub fn ensure_remaining(&mut self, remaining_bytes_required: usize) {
        if remaining_bytes_required > self.remaining() {
            self.expand_buffer(remaining_bytes_required);
        }
    }

    fn expand_buffer(&mut self, remaining_required: usize) {
        let scaled = ((self.limit as f64) * REALLOCATION_FACTOR) as usize;
        let needed = self.position + remaining_required;
        let new_size = std::cmp::max(scaled, needed);
        // Resize fills the newly added bytes with zero, matching
        // `ByteBuffer.allocate(newSize)` followed by `put(oldBuffer)`.
        self.buffer.resize(new_size, 0);
        self.limit = new_size;
    }

    /// The initial write position at construction. Mirrors Java's private
    /// field `initialPosition`. Exposed for parity with the Java reference
    /// even though no Phase 1 caller currently reads it.
    pub fn initial_position(&self) -> usize {
        self.initial_position
    }

    /// Append the contents of `source` to the buffer at the current position.
    pub fn write_buffer(&mut self, source: &[u8]) -> io::Result<()> {
        self.ensure_remaining(source.len());
        self.buffer[self.position..self.position + source.len()].copy_from_slice(source);
        self.position += source.len();
        Ok(())
    }
}

impl Write for ByteBufferOutputStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.write_buffer(buf)?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    // Translation of `ByteBufferOutputStreamTest` (the regular non-direct
    // ByteBuffer cases — Java's `allocateDirect` distinction is irrelevant
    // in Rust where we have one `Vec<u8>` representation).

    use super::*;

    /// Java: `testExpandByteBufferOnPositionIncrease`.
    #[test]
    fn expand_on_position_increase() {
        let mut output = ByteBufferOutputStream::with_capacity(16);
        output.write_all(b"hello").unwrap();
        output.set_position(32);
        assert_eq!(output.position(), 32);

        let buf = output.buffer();
        assert_eq!(buf.len(), 32);
        assert_eq!(&buf[..5], b"hello");
    }

    /// Java: `testExpandByteBufferOnWrite`.
    #[test]
    fn expand_on_write() {
        let mut output = ByteBufferOutputStream::with_capacity(16);
        output.write_all(b"hello").unwrap();
        output.write_all(&[0u8; 27]).unwrap();
        assert_eq!(output.position(), 32);

        let buf = output.buffer();
        assert!(buf.len() >= 32);
        assert_eq!(&buf[..5], b"hello");
    }

    /// Java: `testWriteByteBuffer`.
    #[test]
    fn write_existing_buffer() {
        let value: i64 = 234_239_230;
        let input = value.to_be_bytes();
        let mut output = ByteBufferOutputStream::with_capacity(32);
        output.write_buffer(&input).unwrap();
        assert_eq!(output.position(), 8);
        let observed = i64::from_be_bytes(output.buffer()[..8].try_into().unwrap());
        assert_eq!(observed, value);
    }

    #[test]
    fn into_buffer_truncates_to_position() {
        let mut output = ByteBufferOutputStream::with_capacity(16);
        output.write_all(b"hi").unwrap();
        let v = output.into_buffer();
        assert_eq!(v, b"hi");
    }

    #[test]
    fn from_buffer_round_trip() {
        let backing = vec![0u8; 8];
        let mut output = ByteBufferOutputStream::from_buffer(backing);
        output.write_all(b"abc").unwrap();
        assert_eq!(output.position(), 3);
        assert_eq!(&output.buffer()[..3], b"abc");
    }
}
