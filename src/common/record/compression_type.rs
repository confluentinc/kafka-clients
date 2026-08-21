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

//! The compression type to use for record batches.
//!
//! Corresponds to Java's `org.apache.kafka.common.record.CompressionType`.

use crate::common::Error;

/// The compression type to use.
///
/// Compression type is represented by two bits in the attributes field of the
/// record batch header, so a byte is large enough.
///
/// Corresponds to Java's `org.apache.kafka.common.record.CompressionType`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum CompressionType {
    /// No compression.
    None = 0,
    /// Gzip compression.
    Gzip = 1,
    /// Snappy compression.
    Snappy = 2,
    /// LZ4 compression.
    Lz4 = 3,
    /// Zstandard compression.
    Zstd = 4,
}

impl CompressionType {
    /// The number of compression types.
    pub const COUNT: usize = 5;

    /// Returns the numeric ID for this compression type.
    pub fn id(self) -> u8 {
        self as u8
    }

    /// Returns the name of this compression type.
    pub fn name(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Gzip => "gzip",
            Self::Snappy => "snappy",
            Self::Lz4 => "lz4",
            Self::Zstd => "zstd",
        }
    }

    /// The names of every compression type, in declaration order.
    ///
    /// Stands for Java's `Utils.enumOptions(CompressionType.class)`, which maps
    /// each enum constant through `toString()` — and `CompressionType.toString()`
    /// returns `name` (`CompressionType.java:192-195`). Used by the
    /// `compression.type` config validator to build `ConfigDef`'s
    /// "String must be one of: …" message. Mirrors the existing
    /// `SecurityProtocol::names()` precedent.
    pub fn names() -> [&'static str; Self::COUNT] {
        [
            Self::None.name(),
            Self::Gzip.name(),
            Self::Snappy.name(),
            Self::Lz4.name(),
            Self::Zstd.name(),
        ]
    }

    /// Returns the initial compression ratio for this type.
    pub fn rate(self) -> f32 {
        1.0
    }

    /// Look up a `CompressionType` by numeric ID.
    ///
    /// # Errors
    ///
    /// Returns a `Error` if the ID is not recognized, matching Java's
    /// `IllegalArgumentException` thrown by `CompressionType.forId()`.
    pub fn for_id(id: u8) -> Result<Self, Error> {
        match id {
            0 => Ok(Self::None),
            1 => Ok(Self::Gzip),
            2 => Ok(Self::Snappy),
            3 => Ok(Self::Lz4),
            4 => Ok(Self::Zstd),
            // Java throws `IllegalArgumentException` (`CompressionType.java:157`),
            // a plain `RuntimeException` OUTSIDE the `KafkaException` hierarchy.
            // `Error::with_message(Errors::UnknownServerError, ..)` would resolve
            // the code to `UnknownServerException`, flipping both
            // `is_kafka_error()` and `is_api_error()` to `true` — which lets the
            // error be swallowed by the consumer's `catch (KafkaException e)`
            // guards that Java lets it escape.
            _ => Err(Error::illegal_argument(format!("Unknown compression type id: {id}"))),
        }
    }

    /// Look up a `CompressionType` by name.
    ///
    /// # Errors
    ///
    /// Returns a `Error` if the name is not recognized, matching Java's
    /// `IllegalArgumentException` thrown by `CompressionType.forName()`.
    pub fn for_name(name: &str) -> Result<Self, Error> {
        match name {
            "none" => Ok(Self::None),
            "gzip" => Ok(Self::Gzip),
            "snappy" => Ok(Self::Snappy),
            "lz4" => Ok(Self::Lz4),
            "zstd" => Ok(Self::Zstd),
            // Java: `throw new IllegalArgumentException("Unknown compression name: " + name)`
            // (`CompressionType.java:173`). See `for_id` for why this must not be
            // an `Errors::UnknownServerError`-coded error.
            _ => Err(Error::illegal_argument(format!("Unknown compression name: {name}"))),
        }
    }

    /// Returns the default compression level for this type.
    ///
    /// Returns `None` for types that do not support compression levels
    /// (`None`, `Snappy`).
    pub fn default_level(self) -> Option<i32> {
        match self {
            Self::Gzip => Some(-1), // Deflater.DEFAULT_COMPRESSION
            Self::Lz4 => Some(9),
            Self::Zstd => Some(3),
            Self::None | Self::Snappy => Option::None,
        }
    }

    /// Returns the minimum compression level for this type.
    ///
    /// Returns `None` for types that do not support compression levels.
    pub fn min_level(self) -> Option<i32> {
        match self {
            Self::Gzip => Some(1), // Deflater.BEST_SPEED
            Self::Lz4 => Some(1),
            Self::Zstd => Some(-131072),
            Self::None | Self::Snappy => Option::None,
        }
    }

    /// Returns the maximum compression level for this type.
    ///
    /// Returns `None` for types that do not support compression levels.
    pub fn max_level(self) -> Option<i32> {
        match self {
            Self::Gzip => Some(9), // Deflater.BEST_COMPRESSION
            Self::Lz4 => Some(17),
            Self::Zstd => Some(22),
            Self::None | Self::Snappy => Option::None,
        }
    }

    /// Returns all compression type values.
    pub fn values() -> &'static [CompressionType] {
        &[Self::None, Self::Gzip, Self::Snappy, Self::Lz4, Self::Zstd]
    }
}

impl std::fmt::Display for CompressionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ids() {
        assert_eq!(CompressionType::None.id(), 0);
        assert_eq!(CompressionType::Gzip.id(), 1);
        assert_eq!(CompressionType::Snappy.id(), 2);
        assert_eq!(CompressionType::Lz4.id(), 3);
        assert_eq!(CompressionType::Zstd.id(), 4);
    }

    #[test]
    fn test_names() {
        assert_eq!(CompressionType::None.name(), "none");
        assert_eq!(CompressionType::Gzip.name(), "gzip");
        assert_eq!(CompressionType::Snappy.name(), "snappy");
        assert_eq!(CompressionType::Lz4.name(), "lz4");
        assert_eq!(CompressionType::Zstd.name(), "zstd");
    }

    #[test]
    fn test_for_id() {
        assert_eq!(CompressionType::for_id(0).unwrap(), CompressionType::None);
        assert_eq!(CompressionType::for_id(1).unwrap(), CompressionType::Gzip);
        assert_eq!(CompressionType::for_id(2).unwrap(), CompressionType::Snappy);
        assert_eq!(CompressionType::for_id(3).unwrap(), CompressionType::Lz4);
        assert_eq!(CompressionType::for_id(4).unwrap(), CompressionType::Zstd);
    }

    #[test]
    fn test_for_id_unknown() {
        let err = CompressionType::for_id(5).unwrap_err();
        assert_eq!(err.message(), "Unknown compression type id: 5");
        // Java throws `IllegalArgumentException` (`CompressionType.java:157`), which
        // is neither a `KafkaException` nor an `ApiException`. A code-resolved
        // `UnknownServerError` would answer `true` to both.
        assert!(
            matches!(err, Error::IllegalArgument(_)),
            "expected IllegalArgument, got {err:?}"
        );
        assert!(!err.is_kafka_error(), "Java's IllegalArgumentException is not a KafkaException");
        assert!(!err.is_api_error(), "Java's IllegalArgumentException is not an ApiException");
    }

    #[test]
    fn test_for_name() {
        assert_eq!(CompressionType::for_name("none").unwrap(), CompressionType::None);
        assert_eq!(CompressionType::for_name("gzip").unwrap(), CompressionType::Gzip);
        assert_eq!(CompressionType::for_name("snappy").unwrap(), CompressionType::Snappy);
        assert_eq!(CompressionType::for_name("lz4").unwrap(), CompressionType::Lz4);
        assert_eq!(CompressionType::for_name("zstd").unwrap(), CompressionType::Zstd);
    }

    #[test]
    fn test_for_name_unknown() {
        let err = CompressionType::for_name("unknown").unwrap_err();
        // Java's literal text is "Unknown compression name: " (`CompressionType.java:173`) —
        // no "type" word, unlike `forId`'s message.
        assert_eq!(err.message(), "Unknown compression name: unknown");
        assert!(
            matches!(err, Error::IllegalArgument(_)),
            "expected IllegalArgument, got {err:?}"
        );
        assert!(!err.is_kafka_error(), "Java's IllegalArgumentException is not a KafkaException");
        assert!(!err.is_api_error(), "Java's IllegalArgumentException is not an ApiException");
    }

    #[test]
    fn test_display() {
        assert_eq!(format!("{}", CompressionType::None), "none");
        assert_eq!(format!("{}", CompressionType::Gzip), "gzip");
        assert_eq!(format!("{}", CompressionType::Snappy), "snappy");
        assert_eq!(format!("{}", CompressionType::Lz4), "lz4");
        assert_eq!(format!("{}", CompressionType::Zstd), "zstd");
    }

    #[test]
    fn test_rate() {
        for ct in CompressionType::values() {
            assert_eq!(ct.rate(), 1.0);
        }
    }

    #[test]
    fn test_compression_levels() {
        // None doesn't support levels
        assert!(CompressionType::None.default_level().is_none());
        assert!(CompressionType::None.min_level().is_none());
        assert!(CompressionType::None.max_level().is_none());

        // Snappy doesn't support levels
        assert!(CompressionType::Snappy.default_level().is_none());
        assert!(CompressionType::Snappy.min_level().is_none());
        assert!(CompressionType::Snappy.max_level().is_none());

        // Gzip: min=1, max=9, default=-1
        assert_eq!(CompressionType::Gzip.default_level(), Some(-1));
        assert_eq!(CompressionType::Gzip.min_level(), Some(1));
        assert_eq!(CompressionType::Gzip.max_level(), Some(9));

        // LZ4: min=1, max=17, default=9
        assert_eq!(CompressionType::Lz4.default_level(), Some(9));
        assert_eq!(CompressionType::Lz4.min_level(), Some(1));
        assert_eq!(CompressionType::Lz4.max_level(), Some(17));

        // Zstd: min=-131072, max=22, default=3
        assert_eq!(CompressionType::Zstd.default_level(), Some(3));
        assert_eq!(CompressionType::Zstd.min_level(), Some(-131072));
        assert_eq!(CompressionType::Zstd.max_level(), Some(22));
    }

    #[test]
    fn test_values() {
        let values = CompressionType::values();
        assert_eq!(values.len(), 5);
        assert_eq!(values[0], CompressionType::None);
        assert_eq!(values[1], CompressionType::Gzip);
        assert_eq!(values[2], CompressionType::Snappy);
        assert_eq!(values[3], CompressionType::Lz4);
        assert_eq!(values[4], CompressionType::Zstd);
    }
}
