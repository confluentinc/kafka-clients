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
//! Translated from `org.apache.kafka.common.record.CompressionType`.
//! Only NONE is fully implemented for MVP; other variants are stubbed.

use std::fmt;

/// The compression type to use.
///
/// Compression type is represented by two bits in the attributes field of the record batch header.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CompressionType {
    /// No compression.
    None,
    /// GZIP compression (not implemented for MVP).
    Gzip,
    /// Snappy compression (not implemented for MVP).
    Snappy,
    /// LZ4 compression (not implemented for MVP).
    Lz4,
    /// ZStandard compression (not implemented for MVP).
    Zstd,
}

impl CompressionType {
    /// Returns the numeric id of this compression type.
    pub fn id(self) -> u8 {
        match self {
            CompressionType::None => 0,
            CompressionType::Gzip => 1,
            CompressionType::Snappy => 2,
            CompressionType::Lz4 => 3,
            CompressionType::Zstd => 4,
        }
    }

    /// Returns the name of this compression type.
    pub fn name(self) -> &'static str {
        match self {
            CompressionType::None => "none",
            CompressionType::Gzip => "gzip",
            CompressionType::Snappy => "snappy",
            CompressionType::Lz4 => "lz4",
            CompressionType::Zstd => "zstd",
        }
    }

    /// Look up a compression type by its numeric id.
    ///
    /// # Errors
    /// Returns an error if the id doesn't match any known compression type.
    pub fn for_id(id: i32) -> Result<CompressionType, String> {
        match id {
            0 => Ok(CompressionType::None),
            1 => Ok(CompressionType::Gzip),
            2 => Ok(CompressionType::Snappy),
            3 => Ok(CompressionType::Lz4),
            4 => Ok(CompressionType::Zstd),
            _ => Err(format!("Unknown compression type id: {}", id)),
        }
    }

    /// Look up a compression type by its name.
    ///
    /// # Errors
    /// Returns an error if the name doesn't match any known compression type.
    pub fn for_name(name: &str) -> Result<CompressionType, String> {
        match name {
            "none" => Ok(CompressionType::None),
            "gzip" => Ok(CompressionType::Gzip),
            "snappy" => Ok(CompressionType::Snappy),
            "lz4" => Ok(CompressionType::Lz4),
            "zstd" => Ok(CompressionType::Zstd),
            _ => Err(format!("Unknown compression name: {}", name)),
        }
    }
}

impl fmt::Display for CompressionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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
    fn test_for_id() {
        assert_eq!(CompressionType::for_id(0).unwrap(), CompressionType::None);
        assert_eq!(CompressionType::for_id(1).unwrap(), CompressionType::Gzip);
        assert_eq!(CompressionType::for_id(4).unwrap(), CompressionType::Zstd);
        assert!(CompressionType::for_id(5).is_err());
    }

    #[test]
    fn test_for_name() {
        assert_eq!(CompressionType::for_name("none").unwrap(), CompressionType::None);
        assert_eq!(CompressionType::for_name("zstd").unwrap(), CompressionType::Zstd);
        assert!(CompressionType::for_name("invalid").is_err());
    }

    #[test]
    fn test_display() {
        assert_eq!(format!("{}", CompressionType::None), "none");
        assert_eq!(format!("{}", CompressionType::Gzip), "gzip");
    }
}
