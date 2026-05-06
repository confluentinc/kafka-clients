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

//! Translation of `org.apache.kafka.common.record.CompressionType`.
//!
//! Codec dispatch wiring (Phase 3c, RESOLVED): [`CompressionType::wrap_for_output`]
//! and [`CompressionType::wrap_for_input`] dispatch into the matching
//! `crate::common::compress::*Compression` implementations using each
//! codec's default compression level. [`CompressionType::level_validator`]
//! returns the `ConfigDef.Validator`-equivalent closure used by
//! `ProducerConfig` to validate `compression.gzip.level` etc. (the level
//! constants `MIN_LEVEL`, `MAX_LEVEL`, `DEFAULT_LEVEL` are stable wire
//! metadata and remained in this enum from Phase 3a).

use std::fmt;
use std::io::{Read, Write};

use crate::common::compress::{
    Compression, GzipCompression, Lz4Compression, NoCompression, SnappyCompression, ZstdCompression,
};
use crate::common::errors::KafkaError;
use crate::common::utils::buffer_supplier::BufferSupplier;
use crate::common::utils::byte_buffer_output_stream::ByteBufferOutputStream;

/// The compression type to use.
///
/// Compression type is represented by two bits in the attributes field of the
/// record batch header, so a single byte is large enough.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CompressionType {
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}

// Per-codec level ranges. Mirrors the constants nested inside the Java
// `CompressionType.GZIP`, `LZ4`, and `ZSTD` instance bodies.

// GZIP comes from `java.util.zip.Deflater`. The JDK stable values are:
// BEST_SPEED = 1, BEST_COMPRESSION = 9, DEFAULT_COMPRESSION = -1.
const GZIP_MIN_LEVEL: i32 = 1;
const GZIP_MAX_LEVEL: i32 = 9;
const GZIP_DEFAULT_LEVEL: i32 = -1;

// LZ4 values come from `net.jpountz.lz4.LZ4Constants`.
const LZ4_MIN_LEVEL: i32 = 1;
const LZ4_MAX_LEVEL: i32 = 17;
const LZ4_DEFAULT_LEVEL: i32 = 9;

// ZSTD values come from the zstd library upstream:
//   ZSTD_minCLevel: -131072
//   ZSTD_MAX_CLEVEL: 22
//   ZSTD_CLEVEL_DEFAULT: 3
const ZSTD_MIN_LEVEL: i32 = -131072;
const ZSTD_MAX_LEVEL: i32 = 22;
const ZSTD_DEFAULT_LEVEL: i32 = 3;

impl CompressionType {
    /// Wire-encoded id (matches Java's `id` field).
    pub fn id(&self) -> i8 {
        match self {
            CompressionType::None => 0,
            CompressionType::Gzip => 1,
            CompressionType::Snappy => 2,
            CompressionType::Lz4 => 3,
            CompressionType::Zstd => 4,
        }
    }

    /// Lower-case codec name (matches Java's `name` field).
    pub fn name(&self) -> &'static str {
        match self {
            CompressionType::None => "none",
            CompressionType::Gzip => "gzip",
            CompressionType::Snappy => "snappy",
            CompressionType::Lz4 => "lz4",
            CompressionType::Zstd => "zstd",
        }
    }

    /// Estimated decompression rate (Java keeps a `rate` field for legacy
    /// reasons; all variants ship `1.0f` today).
    pub fn rate(&self) -> f32 {
        1.0
    }

    /// Look up a `CompressionType` by its wire id.
    ///
    /// Mirrors Java's `CompressionType.forId(int)`; Java throws
    /// `IllegalArgumentException` for unknown ids, we surface that as
    /// [`KafkaError::InvalidRecord`] (the wire-protocol-level error class).
    pub fn for_id(id: i32) -> Result<CompressionType, KafkaError> {
        match id {
            0 => Ok(CompressionType::None),
            1 => Ok(CompressionType::Gzip),
            2 => Ok(CompressionType::Snappy),
            3 => Ok(CompressionType::Lz4),
            4 => Ok(CompressionType::Zstd),
            _ => Err(KafkaError::InvalidRecord(format!("Unknown compression type id: {id}"))),
        }
    }

    /// Look up a `CompressionType` by its lower-case codec name.
    ///
    /// Mirrors Java's `CompressionType.forName(String)`.
    pub fn for_name(name: &str) -> Result<CompressionType, KafkaError> {
        match name {
            "none" => Ok(CompressionType::None),
            "gzip" => Ok(CompressionType::Gzip),
            "snappy" => Ok(CompressionType::Snappy),
            "lz4" => Ok(CompressionType::Lz4),
            "zstd" => Ok(CompressionType::Zstd),
            other => Err(KafkaError::InvalidRequest(format!("Unknown compression name: {other}"))),
        }
    }

    /// Default compression level for codecs that accept one. Mirrors Java's
    /// `defaultLevel()` — `NONE` and `SNAPPY` raise `UnsupportedOperationException`
    /// in Java; we return [`KafkaError::InvalidRequest`].
    pub fn default_level(&self) -> Result<i32, KafkaError> {
        match self {
            CompressionType::Gzip => Ok(GZIP_DEFAULT_LEVEL),
            CompressionType::Lz4 => Ok(LZ4_DEFAULT_LEVEL),
            CompressionType::Zstd => Ok(ZSTD_DEFAULT_LEVEL),
            CompressionType::None | CompressionType::Snappy => Err(KafkaError::InvalidRequest(format!(
                "Compression levels are not defined for this compression type: {}",
                self.name()
            ))),
        }
    }

    /// Minimum compression level for codecs that accept one. Mirrors Java's
    /// `minLevel()`.
    pub fn min_level(&self) -> Result<i32, KafkaError> {
        match self {
            CompressionType::Gzip => Ok(GZIP_MIN_LEVEL),
            CompressionType::Lz4 => Ok(LZ4_MIN_LEVEL),
            CompressionType::Zstd => Ok(ZSTD_MIN_LEVEL),
            CompressionType::None | CompressionType::Snappy => Err(KafkaError::InvalidRequest(format!(
                "Compression levels are not defined for this compression type: {}",
                self.name()
            ))),
        }
    }

    /// Maximum compression level for codecs that accept one. Mirrors Java's
    /// `maxLevel()`.
    pub fn max_level(&self) -> Result<i32, KafkaError> {
        match self {
            CompressionType::Gzip => Ok(GZIP_MAX_LEVEL),
            CompressionType::Lz4 => Ok(LZ4_MAX_LEVEL),
            CompressionType::Zstd => Ok(ZSTD_MAX_LEVEL),
            CompressionType::None | CompressionType::Snappy => Err(KafkaError::InvalidRequest(format!(
                "Compression levels are not defined for this compression type: {}",
                self.name()
            ))),
        }
    }

    /// Wrap `buffer_stream` with a `Write` adapter that compresses data
    /// with this compression type at its default level. Mirrors Java's
    /// pattern `Compression.gzip().build().wrapForOutput(...)` etc., where
    /// the per-message-version dispatch is `CompressionType.wrap_for_output`.
    ///
    /// `message_version` is the record-format magic byte and steers LZ4
    /// to emit the broken FD checksum for V0 (legacy compatibility).
    pub fn wrap_for_output<'a>(
        &self,
        buffer_stream: &'a mut ByteBufferOutputStream,
        message_version: i8,
    ) -> Box<dyn Write + 'a> {
        match self {
            CompressionType::None => NoCompression::new().wrap_for_output(buffer_stream, message_version),
            CompressionType::Gzip => GzipCompression::default().wrap_for_output(buffer_stream, message_version),
            CompressionType::Snappy => SnappyCompression::new().wrap_for_output(buffer_stream, message_version),
            CompressionType::Lz4 => Lz4Compression::default().wrap_for_output(buffer_stream, message_version),
            CompressionType::Zstd => ZstdCompression::default().wrap_for_output(buffer_stream, message_version),
        }
    }

    /// Wrap `buffer` with a `Read` adapter that decompresses data with
    /// this compression type. Mirrors Java's `CompressionType.wrap_for_input`.
    pub fn wrap_for_input<'a>(
        &self,
        buffer: &'a [u8],
        message_version: i8,
        decompression_buffer_supplier: BufferSupplier,
    ) -> Box<dyn Read + 'a> {
        match self {
            CompressionType::None => {
                NoCompression::new().wrap_for_input(buffer, message_version, decompression_buffer_supplier)
            },
            CompressionType::Gzip => {
                GzipCompression::default().wrap_for_input(buffer, message_version, decompression_buffer_supplier)
            },
            CompressionType::Snappy => {
                SnappyCompression::new().wrap_for_input(buffer, message_version, decompression_buffer_supplier)
            },
            CompressionType::Lz4 => {
                Lz4Compression::default().wrap_for_input(buffer, message_version, decompression_buffer_supplier)
            },
            CompressionType::Zstd => {
                ZstdCompression::default().wrap_for_input(buffer, message_version, decompression_buffer_supplier)
            },
        }
    }

    /// Returns a closure that validates a candidate compression level
    /// against this codec's `[min_level, max_level]` bounds. Mirrors Java's
    /// `levelValidator()` which returns a `ConfigDef.Validator`.
    ///
    /// The validator's contract:
    /// - For Gzip, the codec's `defaultLevel()` (`-1`) is allowed even
    ///   though it is outside `[1, 9]`.
    /// - For Lz4 and Zstd, the level must lie strictly within
    ///   `[min_level, max_level]`.
    /// - For None and Snappy, any level invocation is an error: those
    ///   codecs do not accept a level.
    ///
    /// We expose the validator as a `Box<dyn Fn>` instead of a custom
    /// trait because `ConfigDef.Validator` in Java has a single
    /// `ensureValid(name, value)` method that returns `void` or throws.
    pub fn level_validator(&self) -> Box<dyn Fn(i32) -> Result<(), KafkaError> + Send + Sync + 'static> {
        match self {
            CompressionType::Gzip => {
                let min = GZIP_MIN_LEVEL;
                let max = GZIP_MAX_LEVEL;
                let default = GZIP_DEFAULT_LEVEL;
                Box::new(move |level| {
                    if (level < min || max < level) && level != default {
                        Err(KafkaError::Config(format!(
                            "gzip doesn't support given compression level: {level}"
                        )))
                    } else {
                        Ok(())
                    }
                })
            },
            CompressionType::Lz4 => {
                let min = LZ4_MIN_LEVEL;
                let max = LZ4_MAX_LEVEL;
                Box::new(move |level| {
                    if level < min || max < level {
                        Err(KafkaError::Config(format!(
                            "lz4 doesn't support given compression level: {level}"
                        )))
                    } else {
                        Ok(())
                    }
                })
            },
            CompressionType::Zstd => {
                let min = ZSTD_MIN_LEVEL;
                let max = ZSTD_MAX_LEVEL;
                Box::new(move |level| {
                    if level < min || max < level {
                        Err(KafkaError::Config(format!(
                            "zstd doesn't support given compression level: {level}"
                        )))
                    } else {
                        Ok(())
                    }
                })
            },
            CompressionType::None | CompressionType::Snappy => {
                let name = self.name();
                Box::new(move |_| {
                    Err(KafkaError::InvalidRequest(format!(
                        "Compression levels are not defined for this compression type: {name}"
                    )))
                })
            },
        }
    }
}

impl fmt::Display for CompressionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_match_wire_values() {
        assert_eq!(CompressionType::None.id(), 0);
        assert_eq!(CompressionType::Gzip.id(), 1);
        assert_eq!(CompressionType::Snappy.id(), 2);
        assert_eq!(CompressionType::Lz4.id(), 3);
        assert_eq!(CompressionType::Zstd.id(), 4);
    }

    #[test]
    fn names_match_java() {
        assert_eq!(CompressionType::None.name(), "none");
        assert_eq!(CompressionType::Gzip.name(), "gzip");
        assert_eq!(CompressionType::Snappy.name(), "snappy");
        assert_eq!(CompressionType::Lz4.name(), "lz4");
        assert_eq!(CompressionType::Zstd.name(), "zstd");
    }

    #[test]
    fn for_id_round_trips() {
        for t in [
            CompressionType::None,
            CompressionType::Gzip,
            CompressionType::Snappy,
            CompressionType::Lz4,
            CompressionType::Zstd,
        ] {
            assert_eq!(CompressionType::for_id(t.id() as i32).unwrap(), t);
        }
    }

    #[test]
    fn for_id_unknown() {
        let err = CompressionType::for_id(7).unwrap_err();
        assert!(matches!(err, KafkaError::InvalidRecord(_)));
        assert!(err.to_string().contains("Unknown compression type id: 7"));
    }

    #[test]
    fn for_name_round_trips() {
        for t in [
            CompressionType::None,
            CompressionType::Gzip,
            CompressionType::Snappy,
            CompressionType::Lz4,
            CompressionType::Zstd,
        ] {
            assert_eq!(CompressionType::for_name(t.name()).unwrap(), t);
        }
    }

    #[test]
    fn for_name_unknown() {
        let err = CompressionType::for_name("brotli").unwrap_err();
        assert!(matches!(err, KafkaError::InvalidRequest(_)));
        assert!(err.to_string().contains("Unknown compression name: brotli"));
    }

    #[test]
    fn level_metadata_defined_for_level_codecs() {
        assert_eq!(CompressionType::Gzip.default_level().unwrap(), GZIP_DEFAULT_LEVEL);
        assert_eq!(CompressionType::Gzip.min_level().unwrap(), GZIP_MIN_LEVEL);
        assert_eq!(CompressionType::Gzip.max_level().unwrap(), GZIP_MAX_LEVEL);

        assert_eq!(CompressionType::Lz4.default_level().unwrap(), LZ4_DEFAULT_LEVEL);
        assert_eq!(CompressionType::Lz4.min_level().unwrap(), LZ4_MIN_LEVEL);
        assert_eq!(CompressionType::Lz4.max_level().unwrap(), LZ4_MAX_LEVEL);

        assert_eq!(CompressionType::Zstd.default_level().unwrap(), ZSTD_DEFAULT_LEVEL);
        assert_eq!(CompressionType::Zstd.min_level().unwrap(), ZSTD_MIN_LEVEL);
        assert_eq!(CompressionType::Zstd.max_level().unwrap(), ZSTD_MAX_LEVEL);
    }

    #[test]
    fn level_metadata_undefined_for_levelless_codecs() {
        for t in [CompressionType::None, CompressionType::Snappy] {
            for r in [t.default_level(), t.min_level(), t.max_level()] {
                let err = r.unwrap_err();
                assert!(matches!(err, KafkaError::InvalidRequest(_)));
                assert!(err.to_string().contains("Compression levels are not defined"));
            }
        }
    }

    #[test]
    fn display_uses_name() {
        assert_eq!(CompressionType::Gzip.to_string(), "gzip");
    }
}
