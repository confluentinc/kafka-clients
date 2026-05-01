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

//! Translation of `org.apache.kafka.common.compress.Compression`.
//!
//! Java's interface is the polymorphic dispatch layer between
//! `MemoryRecordsBuilder` / `DefaultRecordBatch` and the per-codec
//! Output/Input streams. We translate it as a Rust trait so the consumer
//! and producer record paths can hold a `Box<dyn Compression>`.
//!
//! The static `Compression.of(name)` and per-codec builders live in this
//! module as free functions.

use std::io::{Read, Write};

use crate::common::record::CompressionType;
use crate::common::utils::buffer_supplier::BufferSupplier;
use crate::common::utils::byte_buffer_output_stream::ByteBufferOutputStream;

/// Polymorphic compression-codec interface. Mirrors Java's
/// `org.apache.kafka.common.compress.Compression` interface.
pub trait Compression: Send + Sync {
    /// The compression type for this compression codec.
    fn compression_type(&self) -> CompressionType;

    /// Wrap `buffer_stream` with a [`Write`] adapter that compresses data
    /// with this codec.
    ///
    /// `message_version` is the record format magic byte (`MAGIC_VALUE_V0`,
    /// `MAGIC_VALUE_V1`, `MAGIC_VALUE_V2`). LZ4 inspects this to select the
    /// "broken" frame-descriptor checksum (Java emits the broken checksum
    /// for V0 only — see `Lz4BlockOutputStream.useBrokenFlagDescriptorChecksum`).
    ///
    /// Returned `Box<dyn Write + 'a>` borrows `buffer_stream` for its
    /// lifetime, mirroring Java's pattern where `wrapForOutput` reuses
    /// the underlying buffer (which may grow during writes).
    fn wrap_for_output<'a>(
        &self,
        buffer_stream: &'a mut ByteBufferOutputStream,
        message_version: i8,
    ) -> Box<dyn Write + 'a>;

    /// Wrap `buffer` with a [`Read`] adapter that decompresses data with
    /// this codec.
    ///
    /// The `decompression_buffer_supplier` provides reusable buffers — for
    /// small batches the cost of allocating a 64KB LZ4 buffer dominates
    /// the cost of decompressing a few records, so a pooling supplier is
    /// expected on hot paths.
    fn wrap_for_input<'a>(
        &self,
        buffer: &'a [u8],
        message_version: i8,
        decompression_buffer_supplier: BufferSupplier,
    ) -> Box<dyn Read + 'a>;

    /// Recommended size of buffer for storing decompressed output.
    ///
    /// Java's default throws `UnsupportedOperationException`; Rust returns
    /// 0 by default so callers explicitly opt into a sized buffer.
    /// Codecs that ship a real value (Gzip / Snappy / LZ4 / ZSTD) override
    /// this method.
    fn decompression_output_size(&self) -> usize {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::compress::{
        gzip_compression::GzipCompression, lz4_compression::Lz4Compression, no_compression::NoCompression,
        snappy_compression::SnappyCompression, zstd_compression::ZstdCompression,
    };

    fn _assert_compression_object_safe() {
        // Compile-time check: the trait can be used as a trait object. If
        // any method became non-object-safe (e.g. by taking `Self`) this
        // would fail to compile.
        let _: Box<dyn Compression> = Box::new(NoCompression::new());
        let _: Box<dyn Compression> = Box::new(GzipCompression::default());
        let _: Box<dyn Compression> = Box::new(SnappyCompression::new());
        let _: Box<dyn Compression> = Box::new(Lz4Compression::default());
        let _: Box<dyn Compression> = Box::new(ZstdCompression::default());
    }
}
