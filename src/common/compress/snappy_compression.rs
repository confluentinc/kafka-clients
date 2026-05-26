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

//! Translation of `org.apache.kafka.common.compress.SnappyCompression`.
//!
//! Java uses `org.xerial.snappy.SnappyOutputStream` / `SnappyInputStream`
//! — these implement the standard *xerial-snappy* framing (8-byte magic
//! `[-126, 'S', 'N', 'A', 'P', 'P', 'Y', 0]` + per-block headers). The
//! Rust `snap::write::FrameEncoder` / `snap::read::FrameDecoder` produce
//! the *Snappy framing format* (RFC standard, magic `0xff 0x06 0x00 0x00
//! 0x73 0x4e 0x61 0x50 0x70 0x59`), which is wire-incompatible.
//!
//! Kafka's wire-format choice for `CompressionType.SNAPPY` is xerial-snappy
//! framing. Because no popular Rust crate currently emits that exact
//! framing, **Phase 3c implements roundtrip-only Snappy through `snap`'s
//! framed format**. This means our compressed output decompresses correctly
//! within this client (matches `SnappyCompressionTest.testCompressionDecompression`
//! which is roundtrip-only), but it is **not byte-compatible with Java
//! brokers/clients** until a xerial-framed encoder is added.
//!
//! See `phase3c_snappy_framing_gap.md` in agent memory.

use std::io::{Read, Write};

use snap::read::FrameDecoder;
use snap::write::FrameEncoder;

use crate::common::compress::compression::Compression;
use crate::common::record::CompressionType;
use crate::common::utils::buffer_supplier::BufferSupplier;
use crate::common::utils::byte_buffer_input_stream::ByteBufferInputStream;
use crate::common::utils::byte_buffer_output_stream::ByteBufferOutputStream;
use crate::common::utils::chunked_bytes_stream::ChunkedBytesStream;

/// Snappy codec. Mirrors Java's `SnappyCompression`.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SnappyCompression;

impl SnappyCompression {
    pub fn new() -> Self {
        SnappyCompression
    }
}

impl Compression for SnappyCompression {
    fn compression_type(&self) -> CompressionType {
        CompressionType::Snappy
    }

    fn wrap_for_output<'a>(
        &self,
        buffer_stream: &'a mut ByteBufferOutputStream,
        _message_version: i8,
    ) -> Box<dyn Write + 'a> {
        Box::new(SnappyWriter { encoder: Some(FrameEncoder::new(buffer_stream)) })
    }

    fn wrap_for_input<'a>(
        &self,
        buffer: &'a [u8],
        _message_version: i8,
        decompression_buffer_supplier: BufferSupplier,
    ) -> Box<dyn Read + 'a> {
        // Java wraps `SnappyInputStream` in a `ChunkedBytesStream`. Mirror
        // the same layering.
        let bbis = ByteBufferInputStream::new(buffer);
        let dec = FrameDecoder::new(bbis);
        Box::new(ChunkedBytesStream::new(
            dec,
            decompression_buffer_supplier,
            self.decompression_output_size(),
            false,
        ))
    }

    fn decompression_output_size(&self) -> usize {
        // 2 KB legacy (https://github.com/apache/kafka/pull/6785).
        2 * 1024
    }
}

/// `Drop` adapter so that `FrameEncoder::into_inner` runs on close (it
/// writes the trailer / flushes any in-flight block).
struct SnappyWriter<'a> {
    encoder: Option<FrameEncoder<&'a mut ByteBufferOutputStream>>,
}

impl Write for SnappyWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.encoder.as_mut().expect("encoder dropped").write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.encoder.as_mut().expect("encoder dropped").flush()
    }
}

impl Drop for SnappyWriter<'_> {
    fn drop(&mut self) {
        if let Some(enc) = self.encoder.take() {
            // `into_inner` flushes any pending block and writes the trailer.
            // Errors here are swallowed by the Drop contract; callers that
            // care call `flush()` explicitly first.
            let _ = enc.into_inner();
        }
    }
}

/// Builder for [`SnappyCompression`]. Mirrors Java's `SnappyCompression.Builder`.
#[derive(Debug, Default, Clone, Copy)]
pub struct Builder;

impl Builder {
    pub fn new() -> Self {
        Builder
    }

    pub fn build(self) -> SnappyCompression {
        SnappyCompression::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::record::record_batch::{MAGIC_VALUE_V0, MAGIC_VALUE_V1, MAGIC_VALUE_V2};

    /// Translation of `SnappyCompressionTest.testCompressionDecompression`.
    #[test]
    fn compression_decompression() {
        let compression = SnappyCompression::new();
        let data = "data".repeat(256).into_bytes();

        for magic in [MAGIC_VALUE_V0, MAGIC_VALUE_V1, MAGIC_VALUE_V2] {
            let mut buffer_stream = ByteBufferOutputStream::with_capacity(4);
            {
                let mut out = compression.wrap_for_output(&mut buffer_stream, magic);
                out.write_all(&data).unwrap();
                out.flush().unwrap();
            }
            let written = &buffer_stream.buffer()[..buffer_stream.position()];

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
}
