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

//! Translates `org.apache.kafka.common.compress.Lz4Compression`.

use super::Compression;
use crate::common::Error;
use crate::common::record::internal::CompressionType;

/// LZ4 compression settings.
///
/// Translates Java's `Lz4Compression`. See [`GzipCompression`] for why `level`
/// is private. Build one with [`Compression::lz4`].
#[derive(Clone, Debug, PartialEq, Eq)]
#[doc(alias = "org.apache.kafka.common.compress.Lz4Compression")]
pub struct Lz4Compression {
    level: i32,
}

/// Builder for [`Lz4Compression`]. Translates Java's `Lz4Compression.Builder`.
#[derive(Clone, Debug)]
#[doc(alias = "org.apache.kafka.common.compress.Lz4Compression$Builder")]
pub struct Builder {
    level: i32,
}

impl Builder {
    /// A builder defaulted to lz4's default level, as Java's `Builder`
    /// initializes its `level` field.
    pub(super) fn new() -> Self {
        Self { level: CompressionType::Lz4.default_level().unwrap() }
    }

    /// Set the compression level. See [`gzip_compression::Builder::level`](super::gzip_compression::Builder::level) for the
    /// throw-to-`Err` translation.
    #[doc(alias = "org.apache.kafka.common.compress.Lz4Compression$Builder#level")]
    pub fn level(mut self, level: i32) -> Result<Self, Error> {
        let min = CompressionType::Lz4.min_level().unwrap();
        let max = CompressionType::Lz4.max_level().unwrap();
        if level < min || max < level {
            return Err(Error::local_illegal_argument(format!(
                "lz4 doesn't support given compression level: {level}"
            )));
        }
        self.level = level;
        Ok(self)
    }

    /// Build the compression codec.
    #[doc(alias = "org.apache.kafka.common.compress.Lz4Compression$Builder#build")]
    pub fn build(self) -> Compression {
        Compression::Lz4(Lz4Compression { level: self.level })
    }
}
