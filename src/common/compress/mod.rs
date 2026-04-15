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

use std::io::{self, Read, Write};

use crate::common::record::CompressionType;

/// Compression codec for Kafka record batches.
///
/// Wraps the configuration for a particular compression algorithm and provides
/// methods to wrap streams for compression (output) and decompression (input).
///
/// # Examples
///
/// ```
/// use confluent_kafka_rust::common::compress::Compression;
///
/// let compression = Compression::none();
/// assert_eq!(compression.compression_type().name(), "none");
///
/// let gzip = Compression::gzip_with_level(6).unwrap();
/// assert_eq!(gzip.compression_type().name(), "gzip");
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Compression {
    /// No compression.
    None,
    /// Gzip compression with a configurable level.
    Gzip {
        /// Compression level. -1 means default.
        level: i32,
    },
    /// Snappy compression.
    Snappy,
    /// LZ4 compression with a configurable level.
    Lz4 {
        /// Compression level.
        level: i32,
    },
    /// Zstandard compression with a configurable level.
    Zstd {
        /// Compression level.
        level: i32,
    },
}

impl Compression {
    /// Create a no-compression instance.
    pub fn none() -> Self {
        Self::None
    }

    /// Create a gzip compression instance with default level.
    pub fn gzip() -> Self {
        Self::Gzip { level: CompressionType::Gzip.default_level().unwrap() }
    }

    /// Create a gzip compression instance with a specific level.
    ///
    /// Returns `None` if the level is out of range.
    pub fn gzip_with_level(level: i32) -> Option<Self> {
        let min = CompressionType::Gzip.min_level().unwrap();
        let max = CompressionType::Gzip.max_level().unwrap();
        let default = CompressionType::Gzip.default_level().unwrap();
        if (level < min || level > max) && level != default {
            return Option::None;
        }
        Some(Self::Gzip { level })
    }

    /// Create a snappy compression instance.
    pub fn snappy() -> Self {
        Self::Snappy
    }

    /// Create an LZ4 compression instance with default level.
    pub fn lz4() -> Self {
        Self::Lz4 { level: CompressionType::Lz4.default_level().unwrap() }
    }

    /// Create an LZ4 compression instance with a specific level.
    ///
    /// Returns `None` if the level is out of range.
    pub fn lz4_with_level(level: i32) -> Option<Self> {
        let min = CompressionType::Lz4.min_level().unwrap();
        let max = CompressionType::Lz4.max_level().unwrap();
        if level < min || level > max {
            return Option::None;
        }
        Some(Self::Lz4 { level })
    }

    /// Create a zstd compression instance with default level.
    pub fn zstd() -> Self {
        Self::Zstd { level: CompressionType::Zstd.default_level().unwrap() }
    }

    /// Create a zstd compression instance with a specific level.
    ///
    /// Returns `None` if the level is out of range.
    pub fn zstd_with_level(level: i32) -> Option<Self> {
        let min = CompressionType::Zstd.min_level().unwrap();
        let max = CompressionType::Zstd.max_level().unwrap();
        if level < min || level > max {
            return Option::None;
        }
        Some(Self::Zstd { level })
    }

    /// Create a `Compression` from a `CompressionType` with default settings.
    pub fn of(compression_type: CompressionType) -> Self {
        match compression_type {
            CompressionType::None => Self::none(),
            CompressionType::Gzip => Self::gzip(),
            CompressionType::Snappy => Self::snappy(),
            CompressionType::Lz4 => Self::lz4(),
            CompressionType::Zstd => Self::zstd(),
        }
    }

    /// The compression type for this compression codec.
    pub fn compression_type(&self) -> CompressionType {
        match self {
            Self::None => CompressionType::None,
            Self::Gzip { .. } => CompressionType::Gzip,
            Self::Snappy => CompressionType::Snappy,
            Self::Lz4 { .. } => CompressionType::Lz4,
            Self::Zstd { .. } => CompressionType::Zstd,
        }
    }

    /// Wrap a writer with a compressing output stream.
    ///
    /// The returned writer compresses data written to it. Call `finish()` on the
    /// inner writer (via the returned `CompressingWriter`) when done.
    ///
    /// # Arguments
    ///
    /// * `writer` - The underlying writer to compress data into.
    /// * `message_version` - The record batch magic version. For LZ4 with
    ///   `message_version == RecordBatch::MAGIC_VALUE_V0`, Java uses a broken
    ///   flag-descriptor checksum for compatibility. Currently only v2 behavior
    ///   is implemented; the parameter is accepted for forward compatibility so
    ///   that later phases (consumer reading v0/v1 records) do not need to
    ///   change the public API.
    pub fn wrap_for_output<W: Write>(&self, writer: W, _message_version: i8) -> io::Result<CompressingWriter<W>> {
        match self {
            Self::None => Ok(CompressingWriter::None(writer)),
            Self::Gzip { level } => {
                let flate2_level = if *level == -1 {
                    flate2::Compression::default()
                } else {
                    flate2::Compression::new(*level as u32)
                };
                Ok(CompressingWriter::Gzip(flate2::write::GzEncoder::new(writer, flate2_level)))
            },
            Self::Snappy => {
                let encoder = snap::write::FrameEncoder::new(writer);
                Ok(CompressingWriter::Snappy(Box::new(encoder)))
            },
            Self::Lz4 { .. } => {
                // Note: lz4_flex is a pure-Rust LZ4 implementation that does not support
                // compression levels. The Java client uses net.jpountz.lz4.LZ4Compressor
                // which supports levels 1-17 via Lz4BlockOutputStream. We keep lz4_flex
                // to avoid a C dependency; the configured level (stored in the Lz4 { level }
                // field) is accepted and validated but does not affect compression output.
                // All data is compressed at lz4_flex's single default level, which is
                // equivalent to Java's default LZ4 fast compressor.
                let encoder = lz4_flex::frame::FrameEncoder::new(writer);
                Ok(CompressingWriter::Lz4(encoder))
            },
            Self::Zstd { level } => {
                let encoder = zstd::Encoder::new(writer, *level)?;
                Ok(CompressingWriter::Zstd(encoder))
            },
        }
    }

    /// Wrap a reader with a decompressing input stream.
    ///
    /// # Arguments
    ///
    /// * `reader` - The underlying reader containing compressed data.
    /// * `message_version` - The record batch magic version. For LZ4 with
    ///   `message_version == RecordBatch::MAGIC_VALUE_V0`, Java uses a broken
    ///   flag-descriptor checksum for compatibility. Currently only v2 behavior
    ///   is implemented; the parameter is accepted for forward compatibility.
    pub fn wrap_for_input<R: Read>(&self, reader: R, _message_version: i8) -> io::Result<DecompressingReader<R>> {
        match self {
            Self::None => Ok(DecompressingReader::None(reader)),
            Self::Gzip { .. } => {
                let decoder = flate2::read::GzDecoder::new(reader);
                Ok(DecompressingReader::Gzip(decoder))
            },
            Self::Snappy => {
                let decoder = snap::read::FrameDecoder::new(reader);
                Ok(DecompressingReader::Snappy(decoder))
            },
            Self::Lz4 { .. } => {
                let decoder = lz4_flex::frame::FrameDecoder::new(reader);
                Ok(DecompressingReader::Lz4(decoder))
            },
            Self::Zstd { .. } => {
                let decoder = zstd::Decoder::new(reader)?;
                Ok(DecompressingReader::Zstd(decoder))
            },
        }
    }
}

/// A writer that compresses data.
///
/// This enum wraps the different compression encoder types. Use
/// [`finish()`](CompressingWriter::finish) to finalize compression and
/// recover the inner writer.
pub enum CompressingWriter<W: Write> {
    /// No compression — data is passed through.
    None(W),
    /// Gzip compression.
    Gzip(flate2::write::GzEncoder<W>),
    /// Snappy compression (boxed to reduce enum size).
    Snappy(Box<snap::write::FrameEncoder<W>>),
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
            Self::Snappy(encoder) => encoder.into_inner().map_err(|e| io::Error::other(e.error().to_string())),
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
pub enum DecompressingReader<R: Read> {
    /// No compression — data is passed through.
    None(R),
    /// Gzip decompression.
    Gzip(flate2::read::GzDecoder<R>),
    /// Snappy decompression.
    Snappy(snap::read::FrameDecoder<R>),
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
        let result = round_trip(&Compression::none(), data);
        assert_eq!(result, data);
    }

    #[test]
    fn test_gzip_round_trip() {
        let data = b"Hello, Kafka! This is a test of gzip compression.";
        let result = round_trip(&Compression::gzip(), data);
        assert_eq!(result, data);
    }

    #[test]
    fn test_gzip_with_level() {
        let data = b"Hello, Kafka! Testing gzip with specific level.";
        let compression = Compression::gzip_with_level(6).unwrap();
        let result = round_trip(&compression, data);
        assert_eq!(result, data);
    }

    #[test]
    fn test_gzip_invalid_level() {
        assert!(Compression::gzip_with_level(10).is_none());
        assert!(Compression::gzip_with_level(0).is_none());
    }

    #[test]
    fn test_gzip_default_level_valid() {
        assert!(Compression::gzip_with_level(-1).is_some());
    }

    #[test]
    fn test_snappy_round_trip() {
        let data = b"Hello, Kafka! This is a test of snappy compression.";
        let result = round_trip(&Compression::snappy(), data);
        assert_eq!(result, data);
    }

    #[test]
    fn test_lz4_round_trip() {
        let data = b"Hello, Kafka! This is a test of lz4 compression.";
        let result = round_trip(&Compression::lz4(), data);
        assert_eq!(result, data);
    }

    #[test]
    fn test_lz4_invalid_level() {
        assert!(Compression::lz4_with_level(0).is_none());
        assert!(Compression::lz4_with_level(18).is_none());
    }

    #[test]
    fn test_zstd_round_trip() {
        let data = b"Hello, Kafka! This is a test of zstd compression.";
        let result = round_trip(&Compression::zstd(), data);
        assert_eq!(result, data);
    }

    #[test]
    fn test_zstd_with_level() {
        let data = b"Hello, Kafka! Testing zstd with specific level.";
        let compression = Compression::zstd_with_level(1).unwrap();
        let result = round_trip(&compression, data);
        assert_eq!(result, data);
    }

    #[test]
    fn test_zstd_invalid_level() {
        assert!(Compression::zstd_with_level(23).is_none());
    }

    #[test]
    fn test_compression_type() {
        assert_eq!(Compression::none().compression_type(), CompressionType::None);
        assert_eq!(Compression::gzip().compression_type(), CompressionType::Gzip);
        assert_eq!(Compression::snappy().compression_type(), CompressionType::Snappy);
        assert_eq!(Compression::lz4().compression_type(), CompressionType::Lz4);
        assert_eq!(Compression::zstd().compression_type(), CompressionType::Zstd);
    }

    #[test]
    fn test_of() {
        assert_eq!(Compression::of(CompressionType::None), Compression::none());
        assert_eq!(Compression::of(CompressionType::Gzip), Compression::gzip());
        assert_eq!(Compression::of(CompressionType::Snappy), Compression::snappy());
        assert_eq!(Compression::of(CompressionType::Lz4), Compression::lz4());
        assert_eq!(Compression::of(CompressionType::Zstd), Compression::zstd());
    }

    #[test]
    fn test_empty_data_round_trip() {
        let data = b"";
        for compression_type in CompressionType::values() {
            let compression = Compression::of(*compression_type);
            let result = round_trip(&compression, data);
            assert_eq!(result, data, "Empty data round-trip failed for {:?}", compression_type);
        }
    }

    #[test]
    fn test_large_data_round_trip() {
        let data: Vec<u8> = (0..10000).map(|i| (i % 256) as u8).collect();
        for compression_type in CompressionType::values() {
            let compression = Compression::of(*compression_type);
            let result = round_trip(&compression, &data);
            assert_eq!(result, data, "Large data round-trip failed for {:?}", compression_type);
        }
    }
}
