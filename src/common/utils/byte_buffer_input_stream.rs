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

//! Translation of `org.apache.kafka.common.utils.ByteBufferInputStream`.

use std::io::{self, Read};

/// A byte-buffer-backed [`Read`] adapter. Mirrors Java's `ByteBufferInputStream`.
///
/// The Rust idiom is `std::io::Cursor<&[u8]>`, which already implements
/// [`Read`]; the Java class is a thin shim over `ByteBuffer` that we keep
/// alongside the `Cursor` for translation parity.
///
/// `available()` returns the number of unread bytes (Java
/// `InputStream#available`).
pub struct ByteBufferInputStream<'a> {
    cursor: io::Cursor<&'a [u8]>,
}

impl<'a> ByteBufferInputStream<'a> {
    /// Construct a stream over the borrowed slice. Mirrors
    /// `new ByteBufferInputStream(ByteBuffer)`.
    pub fn new(buffer: &'a [u8]) -> Self {
        ByteBufferInputStream { cursor: io::Cursor::new(buffer) }
    }

    /// Number of bytes remaining to be read. Mirrors `available()`.
    pub fn available(&self) -> usize {
        let total = self.cursor.get_ref().len() as u64;
        let pos = self.cursor.position();
        (total.saturating_sub(pos)) as usize
    }

    /// Reset the read cursor to the beginning of the underlying buffer.
    /// Equivalent to Java's `ByteBuffer#rewind()`. Used by the
    /// `ByteBufferInputStreamTest` which rewinds and reads again.
    pub fn rewind(&mut self) {
        self.cursor.set_position(0);
    }
}

impl Read for ByteBufferInputStream<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.cursor.read(buf)
    }
}

#[cfg(test)]
mod tests {
    // Translation of `ByteBufferInputStreamTest`.

    use super::*;

    /// Java: `testReadUnsignedIntFromInputStream`.
    /// The Java test uses ByteBuffer's mutable `position`/`flip`/`rewind`;
    /// we reimplement the same observable behaviour using `Read::read` plus
    /// the explicit `rewind` helper.
    #[test]
    fn read_unsigned_int_from_input_stream() {
        // Pre-populate a 3-byte payload (the Java test only puts 3 bytes
        // before calling `flip()`).
        let payload = [10u8, 20, 30];
        let mut input = ByteBufferInputStream::new(&payload);

        assert_eq!(input.available(), 3);

        // Read two single bytes.
        let mut single = [0u8];
        assert_eq!(input.read(&mut single).unwrap(), 1);
        assert_eq!(single[0], 10);
        assert_eq!(input.read(&mut single).unwrap(), 1);
        assert_eq!(single[0], 20);

        // Try to read 3 bytes but only 1 remains. The Java test reads into
        // `b[3..]` (length 3); Rust's `Read::read` returns `usize` of the
        // bytes actually read.
        let mut buf = [0u8; 6];
        let n = input.read(&mut buf[3..6]).unwrap();
        assert_eq!(n, 1);

        // Subsequent reads at end of stream return 0 (Rust convention)
        // — Java returns -1 from `read()`, and the parallel construct here
        // is `read(&mut single) == 0`.
        let n = input.read(&mut single).unwrap();
        assert_eq!(n, 0);

        // Rewind and read again into a 6-byte buffer.
        input.rewind();
        let n = input.read(&mut buf).unwrap();
        assert_eq!(n, 3);

        // EOF again.
        let n = input.read(&mut buf).unwrap();
        assert_eq!(n, 0);

        // Read 0 bytes returns 0; subsequent read still EOF.
        let n = input.read(&mut buf[0..0]).unwrap();
        assert_eq!(n, 0);
        let n = input.read(&mut single).unwrap();
        assert_eq!(n, 0);
    }
}
