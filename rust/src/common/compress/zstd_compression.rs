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

//! Translates `org.apache.kafka.common.compress.ZstdCompression`.

use super::Compression;
use crate::common::Error;
use crate::common::record::internal::CompressionType;

/// Zstandard compression settings.
///
/// Translates Java's `ZstdCompression`. See [`GzipCompression`] for why `level`
/// is private. Build one with [`Compression::zstd`].
#[derive(Clone, Debug, PartialEq, Eq)]
#[doc(alias = "org.apache.kafka.common.compress.ZstdCompression")]
pub struct ZstdCompression {
    level: i32,
}

impl ZstdCompression {
    /// The configured compression level, read when wrapping an output stream.
    pub(super) fn level(&self) -> i32 {
        self.level
    }
}

/// Builder for [`ZstdCompression`]. Translates Java's `ZstdCompression.Builder`.
#[derive(Clone, Debug)]
#[doc(alias = "org.apache.kafka.common.compress.ZstdCompression$Builder")]
pub struct Builder {
    level: i32,
}

impl Builder {
    /// A builder defaulted to zstd's default level, as Java's `Builder`
    /// initializes its `level` field.
    pub(super) fn new() -> Self {
        Self { level: CompressionType::Zstd.default_level().unwrap() }
    }

    /// Set the compression level. See [`gzip_compression::Builder::level`](super::gzip_compression::Builder::level) for the
    /// throw-to-`Err` translation.
    #[doc(alias = "org.apache.kafka.common.compress.ZstdCompression$Builder#level")]
    pub fn level(mut self, level: i32) -> Result<Self, Error> {
        let min = CompressionType::Zstd.min_level().unwrap();
        let max = CompressionType::Zstd.max_level().unwrap();
        if level < min || max < level {
            return Err(Error::local_illegal_argument(format!(
                "zstd doesn't support given compression level: {level}"
            )));
        }
        self.level = level;
        Ok(self)
    }

    /// Build the compression codec.
    #[doc(alias = "org.apache.kafka.common.compress.ZstdCompression$Builder#build")]
    pub fn build(self) -> Compression {
        Compression::Zstd(ZstdCompression { level: self.level })
    }
}
