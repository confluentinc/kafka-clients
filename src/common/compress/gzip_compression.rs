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

//! Translation of `org.apache.kafka.common.compress.GzipCompression`.

use std::io::{BufWriter, Read, Write};

use flate2::read::GzDecoder;

use crate::common::compress::compression::Compression;
use crate::common::compress::gzip_output_stream::GzipOutputStream;
use crate::common::errors::KafkaError;
use crate::common::record::CompressionType;
use crate::common::utils::buffer_supplier::BufferSupplier;
use crate::common::utils::byte_buffer_input_stream::ByteBufferInputStream;
use crate::common::utils::byte_buffer_output_stream::ByteBufferOutputStream;
use crate::common::utils::chunked_bytes_stream::ChunkedBytesStream;

/// 8 KB output (compressed) buffer used by `wrapForOutput`. Java sets this
/// explicitly to avoid the JDK default of 0.5 KB.
const GZIP_OUTPUT_BUFFER_SIZE: usize = 8 * 1024;

/// 16 KB input (uncompressed) buffer used by `wrapForOutput`'s
/// `BufferedOutputStream` wrapper.
const GZIP_INPUT_BUFFER_SIZE: usize = 16 * 1024;

/// Gzip codec. Mirrors Java's `GzipCompression`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GzipCompression {
    level: i32,
}

impl GzipCompression {
    /// Returns the configured compression level.
    pub fn level(&self) -> i32 {
        self.level
    }

    /// Construct a codec at the supplied level. Mirrors the package-private
    /// constructor; callers typically go through [`Builder`].
    fn new(level: i32) -> Self {
        GzipCompression { level }
    }
}

impl Default for GzipCompression {
    fn default() -> Self {
        // Mirrors `Builder { level = GZIP.defaultLevel() }`.
        GzipCompression {
            level: CompressionType::Gzip.default_level().expect("Gzip always has a default level"),
        }
    }
}

impl Compression for GzipCompression {
    fn compression_type(&self) -> CompressionType {
        CompressionType::Gzip
    }

    fn wrap_for_output<'a>(
        &self,
        buffer_stream: &'a mut ByteBufferOutputStream,
        _message_version: i8,
    ) -> Box<dyn Write + 'a> {
        // Java wraps `new GzipOutputStream(buffer, 8 * 1024, level)` in a
        // `BufferedOutputStream` of 16 KB. The Rust translation mirrors that
        // sandwich exactly: BufWriter for input buffering, then our
        // `GzipOutputStream` (which sets compression level).
        let inner = GzipOutputStream::new(buffer_stream, GZIP_OUTPUT_BUFFER_SIZE, self.level);
        Box::new(BufWriter::with_capacity(GZIP_INPUT_BUFFER_SIZE, inner))
    }

    fn wrap_for_input<'a>(
        &self,
        buffer: &'a [u8],
        _message_version: i8,
        decompression_buffer_supplier: BufferSupplier,
    ) -> Box<dyn Read + 'a> {
        // Java wraps `new GZIPInputStream(new ByteBufferInputStream(buffer), 8 * 1024)`
        // in a `ChunkedBytesStream` (decompression-output sized). The Rust
        // `flate2::read::GzDecoder` already buffers internally, but we keep
        // the `ChunkedBytesStream` layer for parity (it pools the decompression
        // output buffer through the supplier).
        let bbis = ByteBufferInputStream::new(buffer);
        let gz = GzDecoder::new(bbis);
        Box::new(ChunkedBytesStream::new(
            gz,
            decompression_buffer_supplier,
            self.decompression_output_size(),
            false,
        ))
    }

    fn decompression_output_size(&self) -> usize {
        // 16 KB legacy (https://github.com/apache/kafka/pull/6785).
        16 * 1024
    }
}

/// Builder for [`GzipCompression`]. Mirrors Java's `GzipCompression.Builder`.
#[derive(Debug, Clone, Copy)]
pub struct Builder {
    level: i32,
}

impl Default for Builder {
    fn default() -> Self {
        Builder {
            level: CompressionType::Gzip.default_level().expect("Gzip always has a default level"),
        }
    }
}

impl Builder {
    pub fn new() -> Self {
        Builder::default()
    }

    /// Set the compression level. Mirrors Java's `Builder#level(int)`.
    /// Returns `self` for chaining.
    ///
    /// Java throws `IllegalArgumentException` when the level is outside
    /// the codec's `[minLevel, maxLevel]` range *and* is not the default.
    /// We surface the same condition as [`KafkaError::Config`].
    pub fn level(mut self, level: i32) -> Result<Self, KafkaError> {
        let min = CompressionType::Gzip.min_level()?;
        let max = CompressionType::Gzip.max_level()?;
        let default = CompressionType::Gzip.default_level()?;
        if (level < min || max < level) && level != default {
            return Err(KafkaError::Config(format!(
                "gzip doesn't support given compression level: {level}"
            )));
        }
        self.level = level;
        Ok(self)
    }

    pub fn build(self) -> GzipCompression {
        GzipCompression::new(self.level)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::record::record_batch::{MAGIC_VALUE_V0, MAGIC_VALUE_V1, MAGIC_VALUE_V2};

    /// Translation of `GzipCompressionTest.testCompressionDecompression`.
    #[test]
    fn compression_decompression() {
        let data = "data".repeat(256).into_bytes();

        for magic in [MAGIC_VALUE_V0, MAGIC_VALUE_V1, MAGIC_VALUE_V2] {
            for level in [
                CompressionType::Gzip.min_level().unwrap(),
                CompressionType::Gzip.default_level().unwrap(),
                CompressionType::Gzip.max_level().unwrap(),
            ] {
                let compression = Builder::new().level(level).unwrap().build();
                let mut buffer_stream = ByteBufferOutputStream::with_capacity(4);
                {
                    let mut out = compression.wrap_for_output(&mut buffer_stream, magic);
                    out.write_all(&data).unwrap();
                    out.flush().unwrap();
                }
                let written = &buffer_stream.buffer()[..buffer_stream.position()];

                let mut input = compression.wrap_for_input(written, magic, BufferSupplier::create());
                let mut result = vec![0u8; data.len()];
                let mut read_total = 0;
                while read_total < data.len() {
                    let n = input.read(&mut result[read_total..]).unwrap();
                    if n == 0 {
                        break;
                    }
                    read_total += n;
                }
                assert_eq!(read_total, data.len(), "magic {magic}, level {level}");
                assert_eq!(result, data);
            }
        }
    }

    /// Translation of `GzipCompressionTest.testCompressionLevels`.
    #[test]
    fn compression_levels() {
        let min = CompressionType::Gzip.min_level().unwrap();
        let max = CompressionType::Gzip.max_level().unwrap();
        let default = CompressionType::Gzip.default_level().unwrap();

        assert!(matches!(Builder::new().level(min - 1), Err(KafkaError::Config(_))));
        assert!(matches!(Builder::new().level(max + 1), Err(KafkaError::Config(_))));

        Builder::new().level(min).unwrap();
        Builder::new().level(max).unwrap();
        // Default is allowed even though it's outside [min, max].
        Builder::new().level(default).unwrap();
    }

    /// Translation of `GzipCompressionTest.testLevelValidator`.
    #[test]
    fn level_validator() {
        let validator = CompressionType::Gzip.level_validator();
        for level in CompressionType::Gzip.min_level().unwrap()..=CompressionType::Gzip.max_level().unwrap() {
            validator(level).unwrap();
        }
        validator(CompressionType::Gzip.default_level().unwrap()).unwrap();
        let min = CompressionType::Gzip.min_level().unwrap();
        let max = CompressionType::Gzip.max_level().unwrap();
        assert!(matches!(validator(min - 1), Err(KafkaError::Config(_))));
        assert!(matches!(validator(max + 1), Err(KafkaError::Config(_))));
    }
}
