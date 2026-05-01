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

//! Translation of `org.apache.kafka.common.compress`.
//!
//! Phase 3c covers the [`Compression`] trait plus the five concrete codec
//! types ([`NoCompression`], [`GzipCompression`], [`SnappyCompression`],
//! [`Lz4Compression`], [`ZstdCompression`]) and the LZ4 frame
//! input/output streams. The `CompressionType` dispatch in
//! `crate::common::record::compression_type` is wired to these codecs.

#[allow(clippy::module_inception)]
pub mod compression;
pub mod gzip_compression;
pub mod gzip_output_stream;
pub mod lz4_block_input_stream;
pub mod lz4_block_output_stream;
pub mod lz4_compression;
pub mod no_compression;
pub mod snappy_compression;
pub mod zstd_compression;

pub use compression::Compression;
pub use gzip_compression::GzipCompression;
pub use lz4_compression::Lz4Compression;
pub use no_compression::NoCompression;
pub use snappy_compression::SnappyCompression;
pub use zstd_compression::ZstdCompression;
