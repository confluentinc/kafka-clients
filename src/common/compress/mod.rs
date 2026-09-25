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

//! Compression codecs for Kafka record batches (org.apache.kafka.common.compress).
//!
//! Uses an enum (not trait) since the set of compression types is fixed by the
//! protocol. Each variant wraps configuration specific to that codec.
//!
//! Corresponds to Java's `org.apache.kafka.common.compress.Compression` and its
//! subclasses (`NoCompression`, `GzipCompression`, `SnappyCompression`,
//! `Lz4Compression`, `ZstdCompression`).
//!
//! # Dead-code lint
//!
//! Java marks this package "not a supported API", so it is crate-private. It
//! is translated in full (DoD #2), but the client uses only part of it; the
//! rest has no caller yet, or only the translated tests. Nothing outside the
//! crate can reach it, so the module allows dead code rather than dropping
//! Java methods.

#![allow(dead_code, unused_imports)]

mod compression;
pub mod gzip_compression;
pub mod lz4_compression;
pub mod zstd_compression;

pub use compression::{Compression, StatelessCompressionBuilder};
pub use gzip_compression::GzipCompression;
pub use lz4_compression::Lz4Compression;
pub use zstd_compression::ZstdCompression;

use std::io::{self, Cursor, Read, Write};

// --- Xerial/snappy-java framing format ---
//
// Kafka uses the xerial/snappy-java block format, NOT the standard Snappy
// framing format (RFC 7849). The formats are incompatible.
//
// Xerial format:
//   - 16-byte magic header
//   - Sequence of blocks, each: 4-byte big-endian compressed length + compressed data
//
// See: https://github.com/xerial/snappy-java

/// Magic header for the xerial/snappy-java format.
const XERIAL_HEADER: [u8; 16] = [
    0x82, b'S', b'N', b'A', b'P', b'P', b'Y', 0, // magic
    0, 0, 0, 1, // min compatible version
    0, 0, 0, 1, // version
];

/// Default block size for xerial snappy (matches Java's default of 32KB).
const XERIAL_BLOCK_SIZE: usize = 32 * 1024;

/// A writer that compresses data using the xerial/snappy-java block format.
///
/// Buffers input data and compresses in blocks when the buffer reaches
/// `XERIAL_BLOCK_SIZE`. Call `finish()` to flush the final block.
pub struct XerialSnappyWriter<W: Write> {
    inner: W,
    buffer: Vec<u8>,
    header_written: bool,
}

impl<W: Write> XerialSnappyWriter<W> {
    fn new(inner: W) -> Self {
        Self { inner, buffer: Vec::with_capacity(XERIAL_BLOCK_SIZE), header_written: false }
    }

    fn ensure_header(&mut self) -> io::Result<()> {
        if !self.header_written {
            self.inner.write_all(&XERIAL_HEADER)?;
            self.header_written = true;
        }
        Ok(())
    }

    fn flush_block(&mut self) -> io::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        self.ensure_header()?;
        let mut encoder = snap::raw::Encoder::new();
        let compressed = encoder
            .compress_vec(&self.buffer)
            .map_err(|e| io::Error::other(e.to_string()))?;
        let len = compressed.len() as u32;
        self.inner.write_all(&len.to_be_bytes())?;
        self.inner.write_all(&compressed)?;
        self.buffer.clear();
        Ok(())
    }

    fn finish(mut self) -> io::Result<W> {
        self.ensure_header()?;
        self.flush_block()?;
        Ok(self.inner)
    }
}

impl<W: Write> Write for XerialSnappyWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.buffer.extend_from_slice(buf);
        while self.buffer.len() >= XERIAL_BLOCK_SIZE {
            let block: Vec<u8> = self.buffer.drain(..XERIAL_BLOCK_SIZE).collect();
            self.ensure_header()?;
            let mut encoder = snap::raw::Encoder::new();
            let compressed = encoder.compress_vec(&block).map_err(|e| io::Error::other(e.to_string()))?;
            let len = compressed.len() as u32;
            self.inner.write_all(&len.to_be_bytes())?;
            self.inner.write_all(&compressed)?;
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

/// A reader that decompresses data in the xerial/snappy-java block format.
///
/// Reads the magic header, then decompresses blocks one at a time.
pub struct XerialSnappyReader<R: Read> {
    inner: R,
    decompressed: Cursor<Vec<u8>>,
    header_read: bool,
}

impl<R: Read> XerialSnappyReader<R> {
    fn new(inner: R) -> Self {
        Self { inner, decompressed: Cursor::new(Vec::new()), header_read: false }
    }

    fn ensure_header(&mut self) -> io::Result<()> {
        if !self.header_read {
            let mut header = [0u8; 16];
            self.inner.read_exact(&mut header)?;
            if header[..8] != XERIAL_HEADER[..8] {
                return Err(io::Error::other("Invalid xerial snappy header"));
            }
            self.header_read = true;
        }
        Ok(())
    }

    fn read_next_block(&mut self) -> io::Result<bool> {
        let mut len_buf = [0u8; 4];
        match self.inner.read_exact(&mut len_buf) {
            Ok(()) => {},
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(false),
            Err(e) => return Err(e),
        }
        let compressed_len = u32::from_be_bytes(len_buf) as usize;
        let mut compressed = vec![0u8; compressed_len];
        self.inner.read_exact(&mut compressed)?;
        let mut decoder = snap::raw::Decoder::new();
        let decompressed = decoder
            .decompress_vec(&compressed)
            .map_err(|e| io::Error::other(e.to_string()))?;
        self.decompressed = Cursor::new(decompressed);
        Ok(true)
    }
}

impl<R: Read> Read for XerialSnappyReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.ensure_header()?;
        loop {
            let n = self.decompressed.read(buf)?;
            if n > 0 {
                return Ok(n);
            }
            if !self.read_next_block()? {
                return Ok(0);
            }
        }
    }
}

/// A writer that compresses data.
///
/// This enum wraps the different compression encoder types. Use
/// [`finish()`](CompressingWriter::finish) to finalize compression and
/// recover the inner writer.
#[non_exhaustive]
pub enum CompressingWriter<W: Write> {
    /// No compression — data is passed through.
    None(W),
    /// Gzip compression.
    Gzip(flate2::write::GzEncoder<W>),
    /// Snappy compression using xerial/snappy-java block format (boxed to reduce enum size).
    Snappy(Box<XerialSnappyWriter<W>>),
    /// LZ4 compression.
    Lz4(lz4_flex::frame::FrameEncoder<W>),
    /// Zstd compression.
    Zstd(zstd::Encoder<'static, W>),
}

impl<W: Write> CompressingWriter<W> {
    /// Finalize the compression and return the inner writer.
    pub fn finish(self) -> io::Result<W> {
        match self {
            Self::None(w) => Ok(w),
            Self::Gzip(encoder) => encoder.finish(),
            Self::Snappy(encoder) => encoder.finish(),
            Self::Lz4(encoder) => encoder.finish().map_err(|e| io::Error::other(e.to_string())),
            Self::Zstd(encoder) => encoder.finish(),
        }
    }
}

impl<W: Write> Write for CompressingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Self::None(w) => w.write(buf),
            Self::Gzip(w) => w.write(buf),
            Self::Snappy(w) => w.write(buf),
            Self::Lz4(w) => w.write(buf),
            Self::Zstd(w) => w.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::None(w) => w.flush(),
            Self::Gzip(w) => w.flush(),
            Self::Snappy(w) => w.flush(),
            Self::Lz4(w) => w.flush(),
            Self::Zstd(w) => w.flush(),
        }
    }
}

/// A reader that decompresses data.
#[non_exhaustive]
pub enum DecompressingReader<R: Read> {
    /// No compression — data is passed through.
    None(R),
    /// Gzip decompression.
    Gzip(flate2::read::GzDecoder<R>),
    /// Snappy decompression using xerial/snappy-java block format.
    Snappy(XerialSnappyReader<R>),
    /// LZ4 decompression.
    Lz4(lz4_flex::frame::FrameDecoder<R>),
    /// Zstd decompression.
    Zstd(zstd::Decoder<'static, std::io::BufReader<R>>),
}

impl<R: Read> Read for DecompressingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::None(r) => r.read(buf),
            Self::Gzip(r) => r.read(buf),
            Self::Snappy(r) => r.read(buf),
            Self::Lz4(r) => r.read(buf),
            Self::Zstd(r) => r.read(buf),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::record::internal::CompressionType;
    use std::io::{Read, Write};

    /// Use record batch magic v2 for tests — this is the only version the
    /// producer creates.
    const TEST_MESSAGE_VERSION: i8 = 2;

    fn round_trip(compression: &Compression, data: &[u8]) -> Vec<u8> {
        // Compress
        let mut compressed = Vec::new();
        let mut writer = compression.wrap_for_output(&mut compressed, TEST_MESSAGE_VERSION).unwrap();
        writer.write_all(data).unwrap();
        let compressed = writer.finish().unwrap().clone();

        // Decompress
        let mut decompressed = Vec::new();
        let mut reader = compression.wrap_for_input(compressed.as_slice(), TEST_MESSAGE_VERSION).unwrap();
        reader.read_to_end(&mut decompressed).unwrap();

        decompressed
    }

    #[test]
    fn test_none_round_trip() {
        let data = b"Hello, Kafka!";
        let result = round_trip(&Compression::none().build(), data);
        assert_eq!(result, data);
    }

    #[test]
    fn test_gzip_round_trip() {
        let data = b"Hello, Kafka! This is a test of gzip compression.";
        let result = round_trip(&Compression::gzip().build(), data);
        assert_eq!(result, data);
    }

    #[test]
    fn test_gzip_with_level() {
        let data = b"Hello, Kafka! Testing gzip with specific level.";
        let compression = Compression::gzip().level(6).unwrap().build();
        let result = round_trip(&compression, data);
        assert_eq!(result, data);
    }

    #[test]
    fn test_gzip_invalid_level() {
        // Java throws IllegalArgumentException with this exact text; the message
        // is part of the contract, so assert it and not merely `is_err`.
        let err = Compression::gzip().level(10).unwrap_err();
        assert!(
            err.to_string().contains("gzip doesn't support given compression level: 10"),
            "unexpected message: {err}"
        );
        assert!(Compression::gzip().level(0).is_err());
    }

    #[test]
    fn test_gzip_default_level_valid() {
        assert!(Compression::gzip().level(-1).is_ok());
    }

    #[test]
    fn test_snappy_round_trip() {
        let data = b"Hello, Kafka! This is a test of snappy compression.";
        let result = round_trip(&Compression::snappy().build(), data);
        assert_eq!(result, data);
    }

    #[test]
    fn test_lz4_round_trip() {
        let data = b"Hello, Kafka! This is a test of lz4 compression.";
        let result = round_trip(&Compression::lz4().build(), data);
        assert_eq!(result, data);
    }

    #[test]
    fn test_lz4_invalid_level() {
        let err = Compression::lz4().level(18).unwrap_err();
        assert!(
            err.to_string().contains("lz4 doesn't support given compression level: 18"),
            "unexpected message: {err}"
        );
        assert!(Compression::lz4().level(0).is_err());
    }

    #[test]
    fn test_zstd_round_trip() {
        let data = b"Hello, Kafka! This is a test of zstd compression.";
        let result = round_trip(&Compression::zstd().build(), data);
        assert_eq!(result, data);
    }

    #[test]
    fn test_zstd_with_level() {
        let data = b"Hello, Kafka! Testing zstd with specific level.";
        let compression = Compression::zstd().level(1).unwrap().build();
        let result = round_trip(&compression, data);
        assert_eq!(result, data);
    }

    #[test]
    fn test_zstd_invalid_level() {
        let err = Compression::zstd().level(23).unwrap_err();
        assert!(
            err.to_string().contains("zstd doesn't support given compression level: 23"),
            "unexpected message: {err}"
        );
    }

    #[test]
    fn test_compression_type() {
        assert_eq!(Compression::NONE.compression_type(), CompressionType::None);
        assert_eq!(Compression::gzip().build().compression_type(), CompressionType::Gzip);
        assert_eq!(Compression::snappy().build().compression_type(), CompressionType::Snappy);
        assert_eq!(Compression::lz4().build().compression_type(), CompressionType::Lz4);
        assert_eq!(Compression::zstd().build().compression_type(), CompressionType::Zstd);
    }

    #[test]
    fn test_of() {
        assert_eq!(Compression::of(CompressionType::None).build(), Compression::NONE);
        assert_eq!(Compression::of(CompressionType::Gzip).build(), Compression::gzip().build());
        assert_eq!(Compression::of(CompressionType::Snappy).build(), Compression::snappy().build());
        assert_eq!(Compression::of(CompressionType::Lz4).build(), Compression::lz4().build());
        assert_eq!(Compression::of(CompressionType::Zstd).build(), Compression::zstd().build());
    }

    /// Translates Java's `Compression.of(String)` overload.
    #[test]
    fn test_of_name() {
        assert_eq!(Compression::of_name("none").unwrap().build(), Compression::NONE);
        assert_eq!(Compression::of_name("gzip").unwrap().build(), Compression::gzip().build());
        assert_eq!(Compression::of_name("snappy").unwrap().build(), Compression::snappy().build());
        assert_eq!(Compression::of_name("lz4").unwrap().build(), Compression::lz4().build());
        assert_eq!(Compression::of_name("zstd").unwrap().build(), Compression::zstd().build());

        // Java's CompressionType.forName throws for an unknown name.
        assert!(Compression::of_name("bogus").is_err());
    }

    #[test]
    fn test_empty_data_round_trip() {
        let data = b"";
        for compression_type in CompressionType::values() {
            let compression = Compression::of(*compression_type).build();
            let result = round_trip(&compression, data);
            assert_eq!(result, data, "Empty data round-trip failed for {:?}", compression_type);
        }
    }

    #[test]
    fn test_large_data_round_trip() {
        let data: Vec<u8> = (0..10000).map(|i| (i % 256) as u8).collect();
        for compression_type in CompressionType::values() {
            let compression = Compression::of(*compression_type).build();
            let result = round_trip(&compression, &data);
            assert_eq!(result, data, "Large data round-trip failed for {:?}", compression_type);
        }
    }
}
