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

//! Translation of `org.apache.kafka.common.compress.NoCompression`.

use std::io::{Read, Write};

use crate::common::compress::compression::Compression;
use crate::common::record::CompressionType;
use crate::common::utils::buffer_supplier::BufferSupplier;
use crate::common::utils::byte_buffer_input_stream::ByteBufferInputStream;
use crate::common::utils::byte_buffer_output_stream::ByteBufferOutputStream;

/// Pass-through codec. Mirrors Java's `NoCompression`.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct NoCompression;

impl NoCompression {
    /// Mirrors Java's private constructor (`Compression.NONE` is the
    /// canonical instance). In Rust we keep `new` public for symmetry with
    /// the other codec types.
    pub fn new() -> Self {
        NoCompression
    }
}

impl Compression for NoCompression {
    fn compression_type(&self) -> CompressionType {
        CompressionType::None
    }

    fn wrap_for_output<'a>(
        &self,
        buffer_stream: &'a mut ByteBufferOutputStream,
        _message_version: i8,
    ) -> Box<dyn Write + 'a> {
        // Java returns `bufferStream` itself; we mirror the pass-through by
        // boxing a mutable reference. The trait object's `Write` impl
        // delegates straight to the underlying buffer.
        Box::new(buffer_stream)
    }

    fn wrap_for_input<'a>(
        &self,
        buffer: &'a [u8],
        _message_version: i8,
        _decompression_buffer_supplier: BufferSupplier,
    ) -> Box<dyn Read + 'a> {
        // Java returns `new ByteBufferInputStream(buffer)`. The supplier is
        // unused — pass-through requires no decompression buffer.
        Box::new(ByteBufferInputStream::new(buffer))
    }
}

/// Builder for [`NoCompression`]. Mirrors Java's `NoCompression.Builder`.
/// Returned by [`crate::common::compress::compression_factory::none`].
#[derive(Debug, Default, Clone, Copy)]
pub struct Builder;

impl Builder {
    pub fn new() -> Self {
        Builder
    }

    pub fn build(self) -> NoCompression {
        NoCompression::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::record::record_batch::{MAGIC_VALUE_V0, MAGIC_VALUE_V1, MAGIC_VALUE_V2};

    /// Translation of `NoCompressionTest.testCompressionDecompression`.
    #[test]
    fn compression_decompression() {
        let compression = NoCompression::new();
        let data = "data".repeat(256).into_bytes();

        for magic in [MAGIC_VALUE_V0, MAGIC_VALUE_V1, MAGIC_VALUE_V2] {
            let mut buffer_stream = ByteBufferOutputStream::with_capacity(4);
            {
                let mut out = compression.wrap_for_output(&mut buffer_stream, magic);
                out.write_all(&data).unwrap();
                out.flush().unwrap();
            }

            // Java: `bufferStream.buffer().array()` after flip gives the raw
            // bytes; for pass-through they equal the input.
            let written = &buffer_stream.buffer()[..buffer_stream.position()];
            assert_eq!(written, data.as_slice(), "magic {magic}");

            let mut input = compression.wrap_for_input(written, magic, BufferSupplier::create());
            let mut result = vec![0u8; data.len()];
            let mut read_total = 0;
            while read_total < data.len() {
                let n = input.read(&mut result[read_total..]).unwrap();
                if n == 0 {
                    break;
                }
                read_total += n;
            }
            assert_eq!(read_total, data.len());
            assert_eq!(result, data);
        }
    }

    #[test]
    fn type_is_none() {
        assert_eq!(NoCompression::new().compression_type(), CompressionType::None);
    }
}
