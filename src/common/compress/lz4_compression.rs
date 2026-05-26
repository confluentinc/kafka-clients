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

//! Translation of `org.apache.kafka.common.compress.Lz4Compression`.

use std::io::{Read, Write};

use crate::common::compress::compression::Compression;
use crate::common::compress::lz4_block_input_stream::Lz4BlockInputStream;
use crate::common::compress::lz4_block_output_stream::Lz4BlockOutputStream;
use crate::common::errors::KafkaError;
use crate::common::record::CompressionType;
use crate::common::record::record_batch::MAGIC_VALUE_V0;
use crate::common::utils::buffer_supplier::BufferSupplier;
use crate::common::utils::byte_buffer_output_stream::ByteBufferOutputStream;
use crate::common::utils::chunked_bytes_stream::ChunkedBytesStream;

/// LZ4 codec. Mirrors Java's `Lz4Compression`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Lz4Compression {
    level: i32,
}

impl Lz4Compression {
    /// Returns the configured compression level.
    pub fn level(&self) -> i32 {
        self.level
    }

    fn new(level: i32) -> Self {
        Lz4Compression { level }
    }
}

impl Default for Lz4Compression {
    fn default() -> Self {
        Lz4Compression {
            level: CompressionType::Lz4.default_level().expect("LZ4 always has a default level"),
        }
    }
}

impl Compression for Lz4Compression {
    fn compression_type(&self) -> CompressionType {
        CompressionType::Lz4
    }

    fn wrap_for_output<'a>(
        &self,
        buffer_stream: &'a mut ByteBufferOutputStream,
        message_version: i8,
    ) -> Box<dyn Write + 'a> {
        // Java emits the broken FD checksum only for V0.
        let use_broken = message_version == MAGIC_VALUE_V0;
        let stream = Lz4BlockOutputStream::with_default_block_size(buffer_stream, self.level, use_broken)
            .expect("LZ4 header construction never fails for valid level");
        Box::new(stream)
    }

    fn wrap_for_input<'a>(
        &self,
        buffer: &'a [u8],
        message_version: i8,
        decompression_buffer_supplier: BufferSupplier,
    ) -> Box<dyn Read + 'a> {
        let ignore = message_version == MAGIC_VALUE_V0;
        // We split the supplier into two: one is consumed by the
        // `Lz4BlockInputStream` to allocate its decompression buffer, the
        // other is given to the outer `ChunkedBytesStream` for its
        // intermediate buffer. Mirroring Java is impossible here without
        // sharing a `&mut` (Java's `BufferSupplier` is not thread-safe and
        // is shared as `final` within the iterator's scope). For
        // correctness in Rust we own one supplier per layer and return them
        // to the caller's pool on drop.
        //
        // Behavioural difference: cached buffers are not shared between
        // `Lz4BlockInputStream` and `ChunkedBytesStream`. The supplier
        // contract of "buffers come back when reads complete" is preserved
        // because each supplier independently caches.
        let outer_supplier = match &decompression_buffer_supplier {
            BufferSupplier::NoCaching => BufferSupplier::no_caching(),
            BufferSupplier::Default(_) => BufferSupplier::create(),
            BufferSupplier::Growable(_) => BufferSupplier::growable(),
        };

        let inner = Lz4BlockInputStream::new(buffer, decompression_buffer_supplier, ignore)
            .map_err(|e| io::Error::other(format!("LZ4 input init failed: {e}")));

        // We can't return a Result from a trait fn-with-default-error
        // contract; Java throws KafkaException on bad header and the
        // ChunkedBytesStream below would propagate the same. Mirror by
        // wrapping into an error reader if construction failed.
        let inner = match inner {
            Ok(s) => s,
            Err(e) => {
                return Box::new(ErrorReader { error: Some(e) });
            },
        };

        Box::new(ChunkedBytesStream::new(
            inner,
            outer_supplier,
            self.decompression_output_size(),
            true,
        ))
    }

    fn decompression_output_size(&self) -> usize {
        2 * 1024
    }
}

use std::io;

/// `Read` adapter that returns its stored error on the first read call.
/// Used to surface init errors from `wrap_for_input` since the trait
/// signature returns `Box<dyn Read>` rather than `Result<Box<dyn Read>>`.
struct ErrorReader {
    error: Option<io::Error>,
}

impl Read for ErrorReader {
    fn read(&mut self, _buf: &mut [u8]) -> io::Result<usize> {
        match self.error.take() {
            Some(e) => Err(e),
            None => Ok(0),
        }
    }
}

/// Builder for [`Lz4Compression`]. Mirrors Java's `Lz4Compression.Builder`.
#[derive(Debug, Clone, Copy)]
pub struct Builder {
    level: i32,
}

impl Default for Builder {
    fn default() -> Self {
        Builder {
            level: CompressionType::Lz4.default_level().expect("LZ4 always has a default level"),
        }
    }
}

impl Builder {
    pub fn new() -> Self {
        Builder::default()
    }

    /// Set the compression level. Mirrors Java's `Builder#level(int)`.
    /// Java throws `IllegalArgumentException` outside `[minLevel, maxLevel]`.
    /// Note: Java's LZ4 builder does NOT make an exception for the default
    /// level (unlike Gzip), so the bounds are strict.
    pub fn level(mut self, level: i32) -> Result<Self, KafkaError> {
        let min = CompressionType::Lz4.min_level()?;
        let max = CompressionType::Lz4.max_level()?;
        if level < min || max < level {
            return Err(KafkaError::Config(format!(
                "lz4 doesn't support given compression level: {level}"
            )));
        }
        self.level = level;
        Ok(self)
    }

    pub fn build(self) -> Lz4Compression {
        Lz4Compression::new(self.level)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::record::record_batch::{MAGIC_VALUE_V0, MAGIC_VALUE_V1, MAGIC_VALUE_V2};

    /// Translation of `Lz4CompressionTest.testLz4FramingMagicV0`.
    #[test]
    fn lz4_framing_magic_v0() {
        let compression = Builder::new().build();
        let mut buffer_stream = ByteBufferOutputStream::with_capacity(256);
        // Wrap and immediately drop; we just want to inspect
        // `useBrokenFlagDescriptorChecksum` via the V0/V1 boundary.
        // For V0, the codec must use the broken checksum.
        // Java's test downcasts the OutputStream — we can't downcast a
        // `Box<dyn Write>`, so we observe behaviour: writing then re-reading
        // with `ignore_flag_descriptor_checksum=false` should *fail* with
        // DESCRIPTOR_HASH_MISMATCH for V0 (broken checksum), and succeed
        // for V1.
        {
            let mut out = compression.wrap_for_output(&mut buffer_stream, MAGIC_VALUE_V0);
            out.write_all(b"hi").unwrap();
            out.flush().unwrap();
            // drop closes
        }
        let written = buffer_stream.buffer()[..buffer_stream.position()].to_vec();
        // V0 produces broken checksum -> reading with ignore=false fails.
        let r = Lz4BlockInputStream::new(&written, BufferSupplier::create(), false);
        assert!(r.is_err(), "V0 broken checksum should fail strict parse");
        // ignoring the FD checksum should succeed.
        let _ok = Lz4BlockInputStream::new(&written, BufferSupplier::create(), true).unwrap();
    }

    /// Translation of `Lz4CompressionTest.testLz4FramingMagicV1`.
    #[test]
    fn lz4_framing_magic_v1() {
        let compression = Builder::new().build();
        let mut buffer_stream = ByteBufferOutputStream::with_capacity(256);
        {
            let mut out = compression.wrap_for_output(&mut buffer_stream, MAGIC_VALUE_V1);
            out.write_all(b"hi").unwrap();
            out.flush().unwrap();
        }
        let written = buffer_stream.buffer()[..buffer_stream.position()].to_vec();
        // V1 produces the correct checksum -> reads with ignore=false should succeed.
        let _ok = Lz4BlockInputStream::new(&written, BufferSupplier::create(), false).unwrap();
    }

    /// Translation of `Lz4CompressionTest.testCompressionDecompression`.
    #[test]
    fn compression_decompression() {
        let data = "data".repeat(256).into_bytes();

        for magic in [MAGIC_VALUE_V0, MAGIC_VALUE_V1, MAGIC_VALUE_V2] {
            for level in [
                CompressionType::Lz4.min_level().unwrap(),
                CompressionType::Lz4.default_level().unwrap(),
                CompressionType::Lz4.max_level().unwrap(),
            ] {
                let compression = Builder::new().level(level).unwrap().build();
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
                assert_eq!(read_total, data.len(), "magic {magic}, level {level}");
                assert_eq!(result, data);
            }
        }
    }

    /// Translation of `Lz4CompressionTest.testCompressionLevels`.
    #[test]
    fn compression_levels() {
        let min = CompressionType::Lz4.min_level().unwrap();
        let max = CompressionType::Lz4.max_level().unwrap();

        assert!(matches!(Builder::new().level(min - 1), Err(KafkaError::Config(_))));
        assert!(matches!(Builder::new().level(max + 1), Err(KafkaError::Config(_))));
        Builder::new().level(min).unwrap();
        Builder::new().level(max).unwrap();
    }

    // -------------------------------------------------------------------
    // Parameterized tests translated from `Lz4CompressionTest`. Java uses
    // JUnit 5's `@ParameterizedTest`/`@ArgumentsSource(Lz4ArgumentsProvider)`
    // with 6 nested loops. Rust's unit-test framework has no equivalent;
    // we materialise each combination directly inside the test body.
    // -------------------------------------------------------------------

    use crate::common::compress::lz4_block_input_stream::{DESCRIPTOR_HASH_MISMATCH, NOT_SUPPORTED, PREMATURE_EOS};
    use crate::common::compress::lz4_block_output_stream::{
        BLOCKSIZE_64KB, LZ4_FRAME_INCOMPRESSIBLE_MASK, Lz4BlockOutputStream,
    };

    fn args_payloads() -> Vec<(&'static str, Vec<u8>)> {
        // Mirrors Java's `Lz4ArgumentsProvider#payloads` — empty, onebyte,
        // and three sizes × {random, ones}. We seed `random` with a fixed
        // bytestring so the test is deterministic.
        let mut payloads: Vec<(&'static str, Vec<u8>)> = Vec::new();
        payloads.push(("empty", Vec::new()));
        payloads.push(("onebyte", vec![1u8]));
        for size in [1000usize, 1 << 16, 1024 * 96] {
            // Pseudo-random fill (deterministic XOR sequence).
            let mut random = vec![0u8; size];
            for (i, b) in random.iter_mut().enumerate() {
                *b = (i as u32).wrapping_mul(2654435761).to_le_bytes()[i & 3];
            }
            payloads.push(("random", random));
            payloads.push(("ones", vec![1u8; size]));
        }
        payloads
    }

    #[derive(Clone, Debug)]
    struct ArgsCase {
        use_broken: bool,
        ignore: bool,
        level: i32,
        block_checksum: bool,
        close: bool,
        payload: Vec<u8>,
    }

    fn args_cases() -> impl Iterator<Item = ArgsCase> {
        let payloads = args_payloads();
        let mut out: Vec<ArgsCase> = Vec::new();
        for (_, payload) in &payloads {
            for &broken in &[false, true] {
                for &ignore in &[false, true] {
                    for &block_checksum in &[false, true] {
                        for &close in &[false, true] {
                            for &level in &[
                                CompressionType::Lz4.min_level().unwrap(),
                                CompressionType::Lz4.default_level().unwrap(),
                                CompressionType::Lz4.max_level().unwrap(),
                            ] {
                                out.push(ArgsCase {
                                    use_broken: broken,
                                    ignore,
                                    level,
                                    block_checksum,
                                    close,
                                    payload: payload.clone(),
                                });
                            }
                        }
                    }
                }
            }
        }
        out.into_iter()
    }

    fn compressed_bytes(args: &ArgsCase) -> Vec<u8> {
        let mut sink: Vec<u8> = Vec::new();
        if args.close {
            let mut lz4 =
                Lz4BlockOutputStream::new(&mut sink, BLOCKSIZE_64KB, args.level, args.block_checksum, args.use_broken)
                    .unwrap();
            lz4.write_all(&args.payload).unwrap();
            lz4.close().unwrap();
        } else {
            // Java's `flush()` does NOT write an end-mark — only `close()` does.
            // Capture the bytes after flush and *before* the writer's `Drop`
            // runs `close()` (which would write the end-mark).
            {
                let mut lz4 = Lz4BlockOutputStream::new(
                    &mut sink,
                    BLOCKSIZE_64KB,
                    args.level,
                    args.block_checksum,
                    args.use_broken,
                )
                .unwrap();
                lz4.write_all(&args.payload).unwrap();
                lz4.flush().unwrap();
                // We need to stop Drop from writing the end-mark. The
                // simplest mechanism is `std::mem::forget`, which deliberately
                // leaks the writer's owned scratch buffers. The test runs
                // O(thousands) cases each with bounded buffers — a one-off
                // leak in a test binary is acceptable.
                std::mem::forget(lz4);
            }
        }
        sink
    }

    /// Translation of `Lz4CompressionTest.testHeaderPrematureEnd`.
    #[test]
    fn header_premature_end() {
        // Same behaviour for every parameterization: a 2-byte buffer is
        // shorter than the 6-byte header, so construction fails with PREMATURE_EOS.
        let buffer = vec![0u8; 2];
        for case in args_cases() {
            match Lz4BlockInputStream::new(&buffer, BufferSupplier::create(), case.ignore) {
                Ok(_) => panic!("expected PREMATURE_EOS for {case:?}"),
                Err(err) => assert_eq!(err.to_string(), PREMATURE_EOS, "case={case:?}"),
            }
        }
    }

    /// Translation of `Lz4CompressionTest.testNotSupported`.
    #[test]
    fn not_supported() {
        for case in args_cases() {
            let mut compressed = compressed_bytes(&case);
            // Corrupt the magic.
            compressed[0] = 0x00;
            match Lz4BlockInputStream::new(&compressed, BufferSupplier::create(), case.ignore) {
                Ok(_) => panic!("expected NOT_SUPPORTED for {case:?}"),
                Err(err) => assert_eq!(err.to_string(), NOT_SUPPORTED, "case={case:?}"),
            }
        }
    }

    /// Translation of `Lz4CompressionTest.testBadFrameChecksum`.
    #[test]
    fn bad_frame_checksum() {
        for case in args_cases() {
            let mut compressed = compressed_bytes(&case);
            // Corrupt the HC byte (offset 6 = 4 magic + 1 flg + 1 bd).
            compressed[6] = 0xFF;
            let r = Lz4BlockInputStream::new(&compressed, BufferSupplier::create(), case.ignore);
            if case.ignore {
                if r.is_err() {
                    panic!("expected ok for {case:?}");
                }
            } else {
                match r {
                    Ok(_) => panic!("expected DESCRIPTOR_HASH_MISMATCH for {case:?}"),
                    Err(err) => {
                        assert_eq!(err.to_string(), DESCRIPTOR_HASH_MISMATCH, "case={case:?}")
                    },
                }
            }
        }
    }

    /// Translation of `Lz4CompressionTest.testBadBlockSize`.
    #[test]
    fn bad_block_size() {
        for case in args_cases() {
            // Java skips when !close || (use_broken && !ignore).
            if !case.close || (case.use_broken && !case.ignore) {
                continue;
            }
            let mut compressed = compressed_bytes(&case);
            // Read & rewrite the first block-size header (offset 7 = end of FD)
            // with a value > maxBlockSize.
            let off = 7;
            let block_size = u32::from_le_bytes(compressed[off..off + 4].try_into().unwrap());
            let new_size = (block_size & LZ4_FRAME_INCOMPRESSIBLE_MASK) | (1 << 24 & !LZ4_FRAME_INCOMPRESSIBLE_MASK);
            compressed[off..off + 4].copy_from_slice(&new_size.to_le_bytes());
            let result = decompress_to_vec(&compressed, &case);
            let err = result.unwrap_err();
            assert!(err.to_string().contains("exceeded max"), "case={case:?}, err={err}");
        }
    }

    fn decompress_to_vec(buffer: &[u8], case: &ArgsCase) -> std::io::Result<Vec<u8>> {
        let mut s = Lz4BlockInputStream::new(buffer, BufferSupplier::create(), case.ignore)?;
        let mut out = Vec::new();
        let mut tmp = [0u8; 1024];
        loop {
            let n = s.read(&mut tmp)?;
            if n == 0 {
                break;
            }
            out.extend_from_slice(&tmp[..n]);
        }
        Ok(out)
    }

    /// Translation of `Lz4CompressionTest.testCompression` — verifies the
    /// frame-descriptor structure byte-by-byte, including version bits,
    /// reserved bits, block descriptor bounds, the HC checksum, and the
    /// 4-byte zero end-mark on close.
    #[test]
    fn compression_frame_structure() {
        for case in args_cases() {
            let compressed = compressed_bytes(&case);

            // Magic bytes are little-endian 0x184D2204.
            assert_eq!(compressed[0], 0x04);
            assert_eq!(compressed[1], 0x22);
            assert_eq!(compressed[2], 0x4D);
            assert_eq!(compressed[3], 0x18);

            // FLG.
            let flg = compressed[4];
            assert_eq!((flg >> 6) & 3, 1, "version must be 1");
            assert_eq!(flg & 3, 0, "reserved bits must be 0");

            // BD.
            let bd = compressed[5];
            let block_max_size = (bd >> 4) & 7;
            assert!((4..=7).contains(&block_max_size));
            assert_eq!(bd & 15, 0);
            assert_eq!((bd >> 7) & 1, 0);

            // Optional content-size: 8 bytes between BD and HC if FLG bit 3 set.
            // Our encoder never sets it, but mirror Java's path for parity.
            let mut offset = 6usize;
            let content_size_bit = (flg >> 3) & 1 != 0;
            if content_size_bit {
                offset += 8;
            }

            let (off, len) = if case.use_broken {
                (0usize, offset)
            } else {
                (4usize, offset - 4)
            };
            let h = {
                use std::hash::Hasher;
                let mut hh = twox_hash::XxHash32::with_seed(0);
                hh.write(&compressed[off..off + len]);
                hh.finish() as u32
            };
            let hc = compressed[offset];
            assert_eq!(((h >> 8) & 0xFF) as u8, hc, "case={case:?}");

            if case.close {
                let n = compressed.len();
                assert_eq!(compressed[n - 4..n], [0u8; 4], "case={case:?}");
            }
        }
    }

    /// Translation of `Lz4CompressionTest.testArrayBackedBuffer`.
    #[test]
    fn array_backed_buffer() {
        for case in args_cases() {
            let compressed = compressed_bytes(&case);
            test_decompression(&compressed, &case);
        }
    }

    /// Translation of `Lz4CompressionTest.testArrayBackedBufferSlice` —
    /// the Java test exercises non-zero ByteBuffer offsets. In Rust we
    /// pass a `&[u8]` slice that already starts at the right offset, so
    /// the test reduces to the same as `testArrayBackedBuffer`.
    #[test]
    fn array_backed_buffer_slice() {
        for case in args_cases() {
            let compressed = compressed_bytes(&case);
            // Pre/post-pad to simulate slice offsets.
            let mut padded = vec![0u8; 12];
            padded.extend_from_slice(&compressed);
            padded.extend_from_slice(&[0u8; 123]);
            // The "slice" is the meaningful sub-range.
            let slice = &padded[12..12 + compressed.len()];
            test_decompression(slice, &case);
        }
    }

    fn test_decompression(buffer: &[u8], case: &ArgsCase) {
        let result = decompress_to_vec(buffer, case);
        // Mirror Java's expected-error matrix.
        if !case.ignore && case.use_broken {
            let err = result.unwrap_err();
            assert_eq!(err.to_string(), DESCRIPTOR_HASH_MISMATCH, "case={case:?}");
        } else if !case.close {
            let err = result.unwrap_err();
            assert_eq!(err.to_string(), PREMATURE_EOS, "case={case:?}");
        } else {
            let decoded = result.unwrap();
            assert_eq!(decoded, case.payload, "case={case:?}");
        }
    }

    /// Translation of `Lz4CompressionTest.testSkip`.
    #[test]
    fn skip() {
        for case in args_cases() {
            if !case.close || (case.use_broken && !case.ignore) {
                continue;
            }
            let compressed = compressed_bytes(&case);
            let mut input = Lz4BlockInputStream::new(&compressed, BufferSupplier::create(), case.ignore).unwrap();

            let n = 100usize;
            let mut remaining = case.payload.len();
            let skipped = input.skip(n).unwrap();
            assert_eq!(skipped, std::cmp::min(n, remaining));

            let n = 10000usize;
            remaining -= skipped;
            let skipped2 = input.skip(n).unwrap();
            assert_eq!(skipped2, std::cmp::min(n, remaining));
        }
    }
}
