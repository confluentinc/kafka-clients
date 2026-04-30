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
//! Compression codec dispatch (the Java overrides on `GZIP`, `LZ4`, `ZSTD`
//! that build a `Compression` instance) is deferred to Phase 3c. The
//! per-codec level constants (`MIN_LEVEL`, `MAX_LEVEL`, `DEFAULT_LEVEL`) are
//! ported here because they are stable, non-codec-dispatch metadata used
//! by configuration validation.

use std::fmt;

use crate::common::errors::KafkaError;

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
