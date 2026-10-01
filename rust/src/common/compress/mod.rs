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

#![expect(unused_imports)]
#![cfg_attr(not(test), expect(dead_code))]

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

/// Snappy's densest element is a 3-byte copy of up to 64 bytes (a copy with a
/// 2-byte offset); a 2-byte copy yields at most 11 bytes, a 5-byte copy at most
/// 64 and a literal at most one byte per input byte. So a block's output is at
/// most `64 / 3` times its size, and a header declaring more cannot be valid.
const MAX_SNAPPY_EXPANSION_NUMERATOR: u64 = 64;
const MAX_SNAPPY_EXPANSION_DENOMINATOR: u64 = 3;

/// The error a decompressing read returns once its output would pass
/// `max_bytes` (D4).
///
/// Shared by [`XerialSnappyReader`], which knows from a block's header that the
/// block would pass the limit, and the record batch reader
/// (`DefaultRecordBatchRef::decompress_records`), which counts every codec's
/// output as it reads, so the limit reads the same whichever notices first.
pub(crate) fn decompressed_size_limit_error(max_bytes: usize) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("decompressed size exceeds the limit of {max_bytes} bytes per record batch"),
    )
}

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
///
/// Each block carries two lengths read off the wire — its compressed length in
/// the framing and its decompressed length in the snappy header — and neither
/// sizes an allocation on trust (D4). The compressed block grows
/// with the bytes actually present, and the decompressed length is checked
/// against both what the block's bytes can encode and the stream's remaining
/// budget before its buffer is allocated, fallibly. snappy-java, which Java's
/// client uses (1.1.10.7 in Kafka 4.3.1), bounds a chunk's compressed length
/// likewise since CVE-2023-34455 (`SnappyInputStream.MAX_CHUNK_SIZE`).
pub struct XerialSnappyReader<R: Read> {
    inner: R,
    decompressed: Cursor<Vec<u8>>,
    header_read: bool,
    /// The most bytes the whole stream may decompress to; `usize::MAX` for no
    /// limit beyond the per-block encoding bound.
    max_decompressed_bytes: usize,
    /// Bytes decompressed so far, over every block.
    decompressed_bytes: usize,
}

impl<R: Read> XerialSnappyReader<R> {
    fn new(inner: R, max_decompressed_bytes: usize) -> Self {
        Self {
            inner,
            decompressed: Cursor::new(Vec::new()),
            header_read: false,
            max_decompressed_bytes,
            decompressed_bytes: 0,
        }
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
        let compressed_len = u32::from_be_bytes(len_buf);
        // The declared length sizes nothing: the block grows with the bytes
        // actually present, and a length past them is an early end of stream.
        let mut compressed = Vec::new();
        (&mut self.inner).take(u64::from(compressed_len)).read_to_end(&mut compressed)?;
        if compressed.len() as u64 != u64::from(compressed_len) {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "Snappy block declares {compressed_len} compressed bytes, but the stream ends after {}",
                    compressed.len()
                ),
            ));
        }

        let decompressed_len = snap::raw::decompress_len(&compressed).map_err(|e| io::Error::other(e.to_string()))?;
        let max_encodable = compressed.len() as u64 * MAX_SNAPPY_EXPANSION_NUMERATOR / MAX_SNAPPY_EXPANSION_DENOMINATOR;
        if decompressed_len as u64 > max_encodable {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Snappy block declares {decompressed_len} decompressed bytes, more than its {} compressed bytes \
                     can encode",
                    compressed.len()
                ),
            ));
        }
        if decompressed_len > self.max_decompressed_bytes.saturating_sub(self.decompressed_bytes) {
            return Err(decompressed_size_limit_error(self.max_decompressed_bytes));
        }

        let mut block = Vec::new();
        block.try_reserve_exact(decompressed_len).map_err(io::Error::other)?;
        block.resize(decompressed_len, 0);
        let written = snap::raw::Decoder::new()
            .decompress(&compressed, &mut block)
            .map_err(|e| io::Error::other(e.to_string()))?;
        block.truncate(written);
        self.decompressed_bytes += written;
        self.decompressed = Cursor::new(block);
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
    use crate::AllocTrackingGuard;
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

    // ── D4: xerial block lengths do not size allocations ────────────────────

    /// The xerial framing of one snappy block: the header, then `block` behind a
    /// declared compressed length of `compressed_len`.
    fn xerial_stream(compressed_len: u32, block: &[u8]) -> Vec<u8> {
        let mut stream = XERIAL_HEADER.to_vec();
        stream.extend_from_slice(&compressed_len.to_be_bytes());
        stream.extend_from_slice(block);
        stream
    }

    fn read_all(reader: &mut impl Read) -> io::Result<Vec<u8>> {
        let mut out = Vec::new();
        reader.read_to_end(&mut out)?;
        Ok(out)
    }

    /// A block length the stream does not hold is an early end of stream, and
    /// the read allocates for the bytes present — it used to allocate the
    /// declared 4 GiB up front.
    #[test]
    fn test_xerial_block_length_past_the_stream_is_an_early_end() {
        let stream = xerial_stream(0xFFFF_FFFF, b"abc");
        let (err, max_allocation) = {
            let _guard = AllocTrackingGuard::new();
            let mut reader = Compression::snappy()
                .build()
                .wrap_for_input(stream.as_slice(), TEST_MESSAGE_VERSION)
                .unwrap();
            let err = read_all(&mut reader).expect_err("the block is cut short");
            (err, AllocTrackingGuard::max_allocation())
        };
        assert_eq!(io::ErrorKind::UnexpectedEof, err.kind());
        assert_eq!(
            "Snappy block declares 4294967295 compressed bytes, but the stream ends after 3",
            err.to_string()
        );
        assert!(max_allocation < 1024, "allocated {max_allocation} bytes for a 3-byte block");
    }

    /// A snappy header declaring more than the block's bytes can encode is
    /// refused before the output buffer exists, even with no stream limit —
    /// `decompress_vec` used to allocate the declared length first.
    #[test]
    fn test_xerial_block_declaring_more_than_it_can_encode_is_refused() {
        // A varint header declaring u32::MAX bytes (the most snap accepts), then
        // a one-byte literal: 7 bytes that can encode at most 7 * 64 / 3 = 149.
        let block = [0xFF, 0xFF, 0xFF, 0xFF, 0x0F, 0x00, b'x'];
        let stream = xerial_stream(block.len() as u32, &block);
        let (err, max_allocation) = {
            let _guard = AllocTrackingGuard::new();
            let mut reader = Compression::snappy()
                .build()
                .wrap_for_input(stream.as_slice(), TEST_MESSAGE_VERSION)
                .unwrap();
            let err = read_all(&mut reader).expect_err("the header is impossible");
            (err, AllocTrackingGuard::max_allocation())
        };
        assert_eq!(io::ErrorKind::InvalidData, err.kind());
        assert_eq!(
            "Snappy block declares 4294967295 decompressed bytes, more than its 7 compressed bytes can encode",
            err.to_string()
        );
        assert!(
            max_allocation < 1024,
            "allocated {max_allocation} bytes for an impossible block"
        );
    }

    /// A block the encoding allows but the stream's limit does not is refused
    /// with the shared limit error before its buffer is allocated, and the limit
    /// counts every block of the stream.
    #[test]
    fn test_xerial_block_past_the_limit_is_refused() {
        let data = vec![b'a'; 10_000];
        let compressed = snap::raw::Encoder::new().compress_vec(&data).unwrap();
        let stream = xerial_stream(compressed.len() as u32, &compressed);

        let mut reader = Compression::snappy()
            .build()
            .wrap_for_input_with_limit(stream.as_slice(), TEST_MESSAGE_VERSION, data.len())
            .unwrap();
        assert_eq!(data, read_all(&mut reader).expect("exactly at the limit"));

        let (err, max_allocation) = {
            let _guard = AllocTrackingGuard::new();
            let mut reader = Compression::snappy()
                .build()
                .wrap_for_input_with_limit(stream.as_slice(), TEST_MESSAGE_VERSION, data.len() - 1)
                .unwrap();
            let err = read_all(&mut reader).expect_err("one byte over the limit");
            (err, AllocTrackingGuard::max_allocation())
        };
        assert_eq!(io::ErrorKind::InvalidData, err.kind());
        assert_eq!(
            "decompressed size exceeds the limit of 9999 bytes per record batch",
            err.to_string()
        );
        assert!(
            max_allocation < data.len(),
            "allocated {max_allocation} bytes for a refused block"
        );

        // Two blocks of 100 under a limit of 150: the second one crosses it.
        let block = snap::raw::Encoder::new().compress_vec(&[b'b'; 100]).unwrap();
        let mut two_blocks = xerial_stream(block.len() as u32, &block);
        two_blocks.extend_from_slice(&(block.len() as u32).to_be_bytes());
        two_blocks.extend_from_slice(&block);
        let mut reader = Compression::snappy()
            .build()
            .wrap_for_input_with_limit(two_blocks.as_slice(), TEST_MESSAGE_VERSION, 150)
            .unwrap();
        let err = read_all(&mut reader).expect_err("the second block crosses the limit");
        assert_eq!(
            "decompressed size exceeds the limit of 150 bytes per record batch",
            err.to_string()
        );
    }
}
