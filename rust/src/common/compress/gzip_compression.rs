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

//! Translates `org.apache.kafka.common.compress.GzipCompression`.

use super::Compression;
use crate::common::Error;
use crate::common::record::internal::CompressionType;

/// Gzip compression settings.
///
/// Translates Java's `GzipCompression`. `level` is private exactly as in Java
/// (`private final int level`), which exposes no getter for it — the level is
/// read only when wrapping an output stream. Build one with
/// [`Compression::gzip`].
#[derive(Clone, Debug, PartialEq, Eq)]
#[doc(alias = "org.apache.kafka.common.compress.GzipCompression")]
pub struct GzipCompression {
    level: i32,
}

impl GzipCompression {
    /// The configured compression level, read when wrapping an output stream.
    pub(super) fn level(&self) -> i32 {
        self.level
    }
}

/// Builder for [`GzipCompression`]. Translates Java's `GzipCompression.Builder`.
#[derive(Clone, Debug)]
#[doc(alias = "org.apache.kafka.common.compress.GzipCompression$Builder")]
pub struct Builder {
    level: i32,
}

impl Builder {
    /// A builder defaulted to gzip's default level, as Java's `Builder`
    /// initializes its `level` field.
    pub(super) fn new() -> Self {
        Self { level: CompressionType::Gzip.default_level().unwrap() }
    }

    /// Set the compression level.
    ///
    /// Java throws `IllegalArgumentException` for an out-of-range level; per
    /// CLAUDE.md §12.2 that becomes an `Err` here, carrying Java's message.
    #[doc(alias = "org.apache.kafka.common.compress.GzipCompression$Builder#level")]
    pub fn level(mut self, level: i32) -> Result<Self, Error> {
        let min = CompressionType::Gzip.min_level().unwrap();
        let max = CompressionType::Gzip.max_level().unwrap();
        let default = CompressionType::Gzip.default_level().unwrap();
        if (level < min || max < level) && level != default {
            return Err(Error::local_illegal_argument(format!(
                "gzip doesn't support given compression level: {level}"
            )));
        }
        self.level = level;
        Ok(self)
    }

    /// Build the compression codec.
    #[doc(alias = "org.apache.kafka.common.compress.GzipCompression$Builder#build")]
    pub fn build(self) -> Compression {
        Compression::Gzip(GzipCompression { level: self.level })
    }
}
