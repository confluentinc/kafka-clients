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

//! Translates `org.apache.kafka.common.compress.Compression`.

use std::io::{self, Read, Write};

use super::{CompressingWriter, DecompressingReader, XerialSnappyReader, XerialSnappyWriter};
use super::{GzipCompression, Lz4Compression, ZstdCompression};
use super::{gzip_compression, lz4_compression, zstd_compression};
use crate::common::Error;
use crate::common::record::internal::CompressionType;

/// Compression codec for Kafka record batches.
///
/// Wraps the configuration for a particular compression algorithm and provides
/// methods to wrap streams for compression (output) and decompression (input).
///
/// # Examples
///
/// ```
/// use confluent_kafka::common::compress::Compression;
///
/// let compression = Compression::none().build();
/// assert_eq!(compression.compression_type().name(), "none");
///
/// let gzip = Compression::gzip().level(6).unwrap().build();
/// assert_eq!(gzip.compression_type().name(), "gzip");
/// ```
///
/// # Deviations from Java
///
/// Java models this as `interface Compression` with one implementing class per
/// codec; Rust models it as an enum whose variants wrap those classes. Three
/// consequences are worth naming, because the shapes are not identical:
///
/// 1. `Builder::build` returns `Compression`, not the concrete per-codec struct.
///    Java's `Builder<T>.build()` returns `GzipCompression`, implicitly upcast to
///    the `Compression` interface at the call site. A Rust enum has no subtyping,
///    so `build` performs that upcast itself. Call-site shape is unchanged.
/// 2. [`Compression::of`] returns a [`Builder`] enum rather than
///    Java's `Builder<? extends Compression>` wildcard — Rust cannot return an
///    existential without boxing, and an enum is this crate's established
///    pattern for a closed set of wire/config types.
/// 3. Java's overloaded `of(String)` becomes [`Compression::of_name`], since Rust
///    has no overloading.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
#[doc(alias = "org.apache.kafka.common.compress.Compression")]
pub enum Compression {
    /// No compression. Translates Java's `NoCompression`, which holds no state.
    None,
    /// Gzip compression. Translates Java's `GzipCompression`.
    Gzip(GzipCompression),
    /// Snappy compression. Translates Java's `SnappyCompression`, which holds no
    /// state.
    Snappy,
    /// LZ4 compression. Translates Java's `Lz4Compression`.
    Lz4(Lz4Compression),
    /// Zstandard compression. Translates Java's `ZstdCompression`.
    Zstd(ZstdCompression),
}

/// Builder for a codec that carries no settings — Java's `NoCompression.Builder`
/// and `SnappyCompression.Builder`, which have no `level`.
#[derive(Clone, Debug)]
pub struct StatelessCompressionBuilder {
    compression: Compression,
}

impl StatelessCompressionBuilder {
    /// Build the compression codec.
    ///
    /// `const` so that [`Compression::NONE`] can be defined as
    /// `Compression::none().build()`, exactly as Java defines its `NONE` field
    /// (`Compression.java:91`). Java's is a static initializer rather than a
    /// compile-time constant, but a stateless builder has nothing to evaluate,
    /// so Rust can do it at compile time and keep the one definition.
    pub const fn build(self) -> Compression {
        self.compression
    }
}

/// The builder returned by [`Compression::of`] / [`Compression::of_name`],
/// standing in for Java's `Builder<? extends Compression>` wildcard.
#[derive(Clone, Debug)]
#[non_exhaustive]
#[doc(alias = "org.apache.kafka.common.compress.Compression$Builder")]
pub enum Builder {
    /// A codec with no settings (none / snappy).
    Stateless(StatelessCompressionBuilder),
    /// Gzip, with a configurable level.
    Gzip(gzip_compression::Builder),
    /// LZ4, with a configurable level.
    Lz4(lz4_compression::Builder),
    /// Zstandard, with a configurable level.
    Zstd(zstd_compression::Builder),
}

impl Builder {
    /// Build the compression codec.
    #[doc(alias = "org.apache.kafka.common.compress.Compression$Builder#build")]
    pub fn build(self) -> Compression {
        match self {
            Self::Stateless(b) => b.build(),
            Self::Gzip(b) => b.build(),
            Self::Lz4(b) => b.build(),
            Self::Zstd(b) => b.build(),
        }
    }
}

impl Compression {
    /// No compression. Translates Java's `Compression.NONE` constant, which is
    /// defined as `none().build()` (`Compression.java:91`) — as it is here.
    ///
    /// Prefer `Compression::none().build()` at call sites: every codec is then
    /// constructed the same way, through its builder.
    pub const NONE: Compression = Compression::none().build();

    /// Create a builder for no compression.
    #[doc(alias = "org.apache.kafka.common.compress.Compression#none")]
    pub const fn none() -> StatelessCompressionBuilder {
        StatelessCompressionBuilder { compression: Self::None }
    }

    /// Create a builder for gzip compression, defaulted to gzip's default level.
    #[doc(alias = "org.apache.kafka.common.compress.Compression#gzip")]
    pub fn gzip() -> gzip_compression::Builder {
        gzip_compression::Builder::new()
    }

    /// Create a builder for snappy compression.
    #[doc(alias = "org.apache.kafka.common.compress.Compression#snappy")]
    pub const fn snappy() -> StatelessCompressionBuilder {
        StatelessCompressionBuilder { compression: Self::Snappy }
    }

    /// Create a builder for LZ4 compression, defaulted to LZ4's default level.
    #[doc(alias = "org.apache.kafka.common.compress.Compression#lz4")]
    pub fn lz4() -> lz4_compression::Builder {
        lz4_compression::Builder::new()
    }

    /// Create a builder for zstd compression, defaulted to zstd's default level.
    #[doc(alias = "org.apache.kafka.common.compress.Compression#zstd")]
    pub fn zstd() -> zstd_compression::Builder {
        zstd_compression::Builder::new()
    }

    /// Create a builder for the given compression type, with default settings.
    #[doc(alias = "org.apache.kafka.common.compress.Compression#of")]
    pub fn of(compression_type: CompressionType) -> Builder {
        match compression_type {
            CompressionType::None => Builder::Stateless(Self::none()),
            CompressionType::Gzip => Builder::Gzip(Self::gzip()),
            CompressionType::Snappy => Builder::Stateless(Self::snappy()),
            CompressionType::Lz4 => Builder::Lz4(Self::lz4()),
            CompressionType::Zstd => Builder::Zstd(Self::zstd()),
        }
    }

    /// Create a builder for the named compression type, with default settings.
    ///
    /// Translates Java's overloaded `Compression.of(String)`; Rust has no
    /// overloading, so the name-taking form gets its own name.
    pub fn of_name(compression_name: &str) -> Result<Builder, Error> {
        Ok(Self::of(CompressionType::for_name(compression_name)?))
    }

    /// The compression type for this compression codec.
    pub fn compression_type(&self) -> CompressionType {
        match self {
            Self::None => CompressionType::None,
            Self::Gzip(_) => CompressionType::Gzip,
            Self::Snappy => CompressionType::Snappy,
            Self::Lz4(_) => CompressionType::Lz4,
            Self::Zstd(_) => CompressionType::Zstd,
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
    #[doc(alias = "org.apache.kafka.common.compress.Compression#wrapForOutput")]
    pub fn wrap_for_output<W: Write>(&self, writer: W, _message_version: i8) -> io::Result<CompressingWriter<W>> {
        match self {
            Self::None => Ok(CompressingWriter::None(writer)),
            Self::Gzip(gzip) => {
                let flate2_level = if gzip.level() == -1 {
                    flate2::Compression::default()
                } else {
                    flate2::Compression::new(gzip.level() as u32)
                };
                Ok(CompressingWriter::Gzip(flate2::write::GzEncoder::new(writer, flate2_level)))
            },
            Self::Snappy => {
                let encoder = XerialSnappyWriter::new(writer);
                Ok(CompressingWriter::Snappy(Box::new(encoder)))
            },
            Self::Lz4(_) => {
                // Note: lz4_flex is a pure-Rust LZ4 implementation that does not support
                // compression levels. The Java client uses net.jpountz.lz4.LZ4Compressor
                // which supports levels 1-17 via Lz4BlockOutputStream. We keep lz4_flex
                // to avoid a C dependency; the configured level (stored in `Lz4Compression`)
                // is accepted and validated but does not affect compression output.
                // All data is compressed at lz4_flex's single default level, which is
                // equivalent to Java's default LZ4 fast compressor.
                let encoder = lz4_flex::frame::FrameEncoder::new(writer);
                Ok(CompressingWriter::Lz4(encoder))
            },
            Self::Zstd(zstd_cfg) => {
                let encoder = zstd::Encoder::new(writer, zstd_cfg.level())?;
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
    #[doc(alias = "org.apache.kafka.common.compress.Compression#wrapForInput")]
    pub fn wrap_for_input<R: Read>(&self, reader: R, _message_version: i8) -> io::Result<DecompressingReader<R>> {
        match self {
            Self::None => Ok(DecompressingReader::None(reader)),
            Self::Gzip(_) => {
                let decoder = flate2::read::GzDecoder::new(reader);
                Ok(DecompressingReader::Gzip(decoder))
            },
            Self::Snappy => {
                let decoder = XerialSnappyReader::new(reader);
                Ok(DecompressingReader::Snappy(decoder))
            },
            Self::Lz4(_) => {
                let decoder = lz4_flex::frame::FrameDecoder::new(reader);
                Ok(DecompressingReader::Lz4(decoder))
            },
            Self::Zstd(_) => {
                let decoder = zstd::Decoder::new(reader)?;
                Ok(DecompressingReader::Zstd(decoder))
            },
        }
    }
}
