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

    /// Returns the initial compression ratio for this type.
    pub fn rate(self) -> f32 {
        1.0
    }

    /// Look up a `CompressionType` by numeric ID.
    ///
    /// Returns `None` if the ID is not recognized.
    pub fn for_id(id: u8) -> Option<Self> {
        match id {
            0 => Some(Self::None),
            1 => Some(Self::Gzip),
            2 => Some(Self::Snappy),
            3 => Some(Self::Lz4),
            4 => Some(Self::Zstd),
            _ => Option::None,
        }
    }

    /// Look up a `CompressionType` by name.
    ///
    /// Returns `None` if the name is not recognized.
    pub fn for_name(name: &str) -> Option<Self> {
        match name {
            "none" => Some(Self::None),
            "gzip" => Some(Self::Gzip),
            "snappy" => Some(Self::Snappy),
            "lz4" => Some(Self::Lz4),
            "zstd" => Some(Self::Zstd),
            _ => Option::None,
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
        assert_eq!(CompressionType::for_id(0), Some(CompressionType::None));
        assert_eq!(CompressionType::for_id(1), Some(CompressionType::Gzip));
        assert_eq!(CompressionType::for_id(2), Some(CompressionType::Snappy));
        assert_eq!(CompressionType::for_id(3), Some(CompressionType::Lz4));
        assert_eq!(CompressionType::for_id(4), Some(CompressionType::Zstd));
        assert_eq!(CompressionType::for_id(5), Option::None);
    }

    #[test]
    fn test_for_name() {
        assert_eq!(CompressionType::for_name("none"), Some(CompressionType::None));
        assert_eq!(CompressionType::for_name("gzip"), Some(CompressionType::Gzip));
        assert_eq!(CompressionType::for_name("snappy"), Some(CompressionType::Snappy));
        assert_eq!(CompressionType::for_name("lz4"), Some(CompressionType::Lz4));
        assert_eq!(CompressionType::for_name("zstd"), Some(CompressionType::Zstd));
        assert_eq!(CompressionType::for_name("unknown"), Option::None);
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
