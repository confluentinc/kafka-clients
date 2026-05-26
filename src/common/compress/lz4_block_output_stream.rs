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

//! Translation of `org.apache.kafka.common.compress.Lz4BlockOutputStream`.
//!
//! Reproduces Kafka's framed-LZ4 wire format byte-for-byte:
//!
//! 1. 4-byte magic `0x184D2204` (little-endian).
//! 2. 1-byte FLG descriptor (version=1, blockIndependence=1, optional
//!    blockChecksum bit).
//! 3. 1-byte BD descriptor (blockMaxSize, low 4 bits + bit 7 reserved).
//! 4. 1-byte HC checksum (XXH32 of FLG..end-of-FD, byte `(hash>>8) & 0xFF`).
//!    Note: when `useBrokenFlagDescriptorChecksum=true`, the checksum
//!    covers the magic bytes too — this preserves V0 compatibility.
//! 5. A sequence of blocks, each: `[ blockSize:4 LE ][ data ][ optional
//!    blockChecksum:4 LE ]`. The high bit of `blockSize` is the
//!    *incompressible* flag (`0x80000000`).
//! 6. End-mark: `[ 0:4 LE ]` (no checksum).

use std::io::{self, Write};

use std::hash::Hasher;
use twox_hash::XxHash32;

use crate::common::utils::byte_utils::{write_unsigned_int_le_at, write_unsigned_int_le_to_stream};

/// LZ4 frame magic — 4 bytes little-endian.
pub const MAGIC: u32 = 0x184D2204;

/// High bit indicates the block was stored uncompressed (incompressible).
pub const LZ4_FRAME_INCOMPRESSIBLE_MASK: u32 = 0x80000000;

/// Block size index for 64 KB blocks. Mirrors the Java constant.
pub const BLOCKSIZE_64KB: u8 = 4;

/// Java `Lz4BlockOutputStream.CLOSED_STREAM`.
pub const CLOSED_STREAM: &str = "The stream is already closed";

/// LZ4 frame descriptor FLG byte. Mirrors Java's `FLG` inner class.
#[derive(Debug, Clone, Copy)]
pub struct Flg {
    reserved: u8,
    content_checksum: u8,
    content_size: u8,
    block_checksum: u8,
    block_independence: u8,
    version: u8,
}

impl Flg {
    const VERSION: u8 = 1;

    /// Mirrors Java's `FLG(boolean blockChecksum)`.
    pub fn new(block_checksum: bool) -> io::Result<Self> {
        Self::with_fields(0, 0, 0, if block_checksum { 1 } else { 0 }, 1, Self::VERSION)
    }

    fn with_fields(
        reserved: u8,
        content_checksum: u8,
        content_size: u8,
        block_checksum: u8,
        block_independence: u8,
        version: u8,
    ) -> io::Result<Self> {
        let f = Flg {
            reserved,
            content_checksum,
            content_size,
            block_checksum,
            block_independence,
            version,
        };
        f.validate()?;
        Ok(f)
    }

    pub fn from_byte(b: u8) -> io::Result<Self> {
        Self::with_fields(b & 3, (b >> 2) & 1, (b >> 3) & 1, (b >> 4) & 1, (b >> 5) & 1, (b >> 6) & 3)
    }

    fn validate(&self) -> io::Result<()> {
        if self.reserved != 0 {
            return Err(io::Error::other("Reserved bits must be 0"));
        }
        if self.block_independence != 1 {
            return Err(io::Error::other("Dependent block stream is unsupported"));
        }
        if self.version != Self::VERSION {
            return Err(io::Error::other(format!("Version {} is unsupported", self.version)));
        }
        Ok(())
    }

    pub fn to_byte(self) -> u8 {
        (self.reserved & 3)
            | ((self.content_checksum & 1) << 2)
            | ((self.content_size & 1) << 3)
            | ((self.block_checksum & 1) << 4)
            | ((self.block_independence & 1) << 5)
            | ((self.version & 3) << 6)
    }

    pub fn is_content_checksum_set(&self) -> bool {
        self.content_checksum == 1
    }

    pub fn is_content_size_set(&self) -> bool {
        self.content_size == 1
    }

    pub fn is_block_checksum_set(&self) -> bool {
        self.block_checksum == 1
    }

    pub fn version(&self) -> u8 {
        self.version
    }
}

/// LZ4 frame descriptor BD byte. Mirrors Java's `BD` inner class.
#[derive(Debug, Clone, Copy)]
pub struct Bd {
    reserved2: u8,
    block_size_value: u8,
    reserved3: u8,
}

impl Bd {
    pub fn new(block_size_value: u8) -> io::Result<Self> {
        Self::with_fields(0, block_size_value, 0)
    }

    fn with_fields(reserved2: u8, block_size_value: u8, reserved3: u8) -> io::Result<Self> {
        let b = Bd { reserved2, block_size_value, reserved3 };
        b.validate()?;
        Ok(b)
    }

    pub fn from_byte(b: u8) -> io::Result<Self> {
        Self::with_fields(b & 15, (b >> 4) & 7, (b >> 7) & 1)
    }

    fn validate(&self) -> io::Result<()> {
        if self.reserved2 != 0 {
            return Err(io::Error::other("Reserved2 field must be 0"));
        }
        if self.block_size_value < 4 || self.block_size_value > 7 {
            return Err(io::Error::other("Block size value must be between 4 and 7"));
        }
        if self.reserved3 != 0 {
            return Err(io::Error::other("Reserved3 field must be 0"));
        }
        Ok(())
    }

    /// 2^(2n+8). Mirrors Java's `getBlockMaximumSize`.
    pub fn block_maximum_size(&self) -> usize {
        1usize << ((2 * self.block_size_value as usize) + 8)
    }

    pub fn to_byte(self) -> u8 {
        (self.reserved2 & 15) | ((self.block_size_value & 7) << 4) | ((self.reserved3 & 1) << 7)
    }
}

/// Compute XXH32 of `data` with seed 0. Mirrors `XXHashFactory.fastestInstance().hash32().hash(data, 0, len, 0)`.
fn xxh32(data: &[u8]) -> u32 {
    let mut h = XxHash32::with_seed(0);
    h.write(data);
    h.finish() as u32
}

/// Streaming LZ4 framed-block writer. Mirrors Java's `Lz4BlockOutputStream`.
pub struct Lz4BlockOutputStream<W: Write> {
    out: Option<W>,
    use_broken_flag_descriptor_checksum: bool,
    flg: Flg,
    bd: Bd,
    max_block_size: usize,
    /// Uncompressed input buffer, sized to `max_block_size`.
    buffer: Vec<u8>,
    /// Compressed output scratch, sized to `lz4_flex::block::get_maximum_output_size(max_block_size)`.
    compressed_buffer: Vec<u8>,
    buffer_offset: usize,
    finished: bool,
    /// Compression level. `lz4_flex` does not expose a level knob (it always
    /// uses the LZ4_compress_default algorithm) — we record the value for
    /// API parity but don't apply it.
    #[allow(dead_code)]
    level: i32,
}

impl<W: Write> Lz4BlockOutputStream<W> {
    /// Mirrors Java's `Lz4BlockOutputStream(OutputStream, int blockSize, int level, boolean blockChecksum, boolean useBrokenFlagDescriptorChecksum)`.
    pub fn new(
        out: W,
        block_size: u8,
        level: i32,
        block_checksum: bool,
        use_broken_flag_descriptor_checksum: bool,
    ) -> io::Result<Self> {
        let bd = Bd::new(block_size)?;
        let flg = Flg::new(block_checksum)?;
        let max_block_size = bd.block_maximum_size();
        let mut buffer = vec![0u8; max_block_size];
        let compressed_capacity = lz4_flex::block::get_maximum_output_size(max_block_size);
        let compressed_buffer = vec![0u8; compressed_capacity];

        let mut s = Lz4BlockOutputStream {
            out: Some(out),
            use_broken_flag_descriptor_checksum,
            flg,
            bd,
            max_block_size,
            buffer: std::mem::take(&mut buffer),
            compressed_buffer,
            buffer_offset: 0,
            finished: false,
            level,
        };
        s.write_header()?;
        Ok(s)
    }

    /// Convenience constructor matching Java's
    /// `Lz4BlockOutputStream(OutputStream, int level, boolean useBrokenFlagDescriptorChecksum)`.
    pub fn with_default_block_size(out: W, level: i32, use_broken_flag_descriptor_checksum: bool) -> io::Result<Self> {
        Self::new(out, BLOCKSIZE_64KB, level, false, use_broken_flag_descriptor_checksum)
    }

    pub fn use_broken_flag_descriptor_checksum(&self) -> bool {
        self.use_broken_flag_descriptor_checksum
    }

    /// Mirrors Java's `writeHeader`. Writes magic + FD + HC checksum to `out`.
    fn write_header(&mut self) -> io::Result<()> {
        // Stage the header in `buffer` (re-using the uncompressed input
        // buffer is what Java does; the buffer is reset to offset 0 after
        // the header is flushed).
        write_unsigned_int_le_at(&mut self.buffer[..], 0, MAGIC);
        self.buffer_offset = 4;
        self.buffer[self.buffer_offset] = self.flg.to_byte();
        self.buffer_offset += 1;
        self.buffer[self.buffer_offset] = self.bd.to_byte();
        self.buffer_offset += 1;
        // Compute HC checksum.
        let (offset, len) = if self.use_broken_flag_descriptor_checksum {
            (0, self.buffer_offset)
        } else {
            (4, self.buffer_offset - 4)
        };
        let hash = xxh32(&self.buffer[offset..offset + len]);
        let hc = ((hash >> 8) & 0xFF) as u8;
        self.buffer[self.buffer_offset] = hc;
        self.buffer_offset += 1;

        let out = self.out.as_mut().ok_or_else(|| io::Error::other(CLOSED_STREAM))?;
        out.write_all(&self.buffer[..self.buffer_offset])?;
        self.buffer_offset = 0;
        Ok(())
    }

    fn ensure_not_finished(&self) -> io::Result<()> {
        if self.finished {
            return Err(io::Error::other(CLOSED_STREAM));
        }
        Ok(())
    }

    /// Mirrors Java's `writeBlock`.
    fn write_block(&mut self) -> io::Result<()> {
        if self.buffer_offset == 0 {
            return Ok(());
        }

        let compressed_len =
            lz4_flex::block::compress_into(&self.buffer[..self.buffer_offset], &mut self.compressed_buffer)
                .map_err(io::Error::other)?;

        // Decide which buffer to write and whether to mark as incompressible.
        let (compressed_len, compress_method, source_is_compressed) = if compressed_len >= self.buffer_offset {
            (self.buffer_offset, LZ4_FRAME_INCOMPRESSIBLE_MASK, false)
        } else {
            (compressed_len, 0, true)
        };

        let out = self.out.as_mut().ok_or_else(|| io::Error::other(CLOSED_STREAM))?;
        write_unsigned_int_le_to_stream(out, (compressed_len as u32) | compress_method)?;
        let bytes_to_write = if source_is_compressed {
            &self.compressed_buffer[..compressed_len]
        } else {
            &self.buffer[..compressed_len]
        };
        out.write_all(bytes_to_write)?;

        if self.flg.is_block_checksum_set() {
            let hash = xxh32(bytes_to_write);
            write_unsigned_int_le_to_stream(out, hash)?;
        }
        self.buffer_offset = 0;
        Ok(())
    }

    /// Mirrors Java's `writeEndMark`.
    fn write_end_mark(&mut self) -> io::Result<()> {
        let out = self.out.as_mut().ok_or_else(|| io::Error::other(CLOSED_STREAM))?;
        write_unsigned_int_le_to_stream(out, 0)?;
        Ok(())
    }

    /// Explicitly close the stream. Java's `close()` writes the final block
    /// and the end-mark, then flushes/closes the underlying writer. Since
    /// Rust's `Drop` cannot return `io::Result`, callers that care about
    /// errors should call `close()` explicitly.
    pub fn close(&mut self) -> io::Result<()> {
        if !self.finished {
            self.write_block()?;
            self.write_end_mark()?;
        }
        self.finished = true;
        if let Some(mut out) = self.out.take() {
            out.flush()?;
        }
        Ok(())
    }
}

impl<W: Write> Write for Lz4BlockOutputStream<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.ensure_not_finished()?;
        let mut off = 0usize;
        let mut len = buf.len();

        let mut buffer_remaining = self.max_block_size - self.buffer_offset;
        while len > buffer_remaining {
            self.buffer[self.buffer_offset..self.buffer_offset + buffer_remaining]
                .copy_from_slice(&buf[off..off + buffer_remaining]);
            self.buffer_offset = self.max_block_size;
            self.write_block()?;
            off += buffer_remaining;
            len -= buffer_remaining;
            buffer_remaining = self.max_block_size;
        }
        self.buffer[self.buffer_offset..self.buffer_offset + len].copy_from_slice(&buf[off..off + len]);
        self.buffer_offset += len;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        if !self.finished {
            self.write_block()?;
        }
        if let Some(out) = self.out.as_mut() {
            out.flush()?;
        }
        Ok(())
    }
}

impl<W: Write> Drop for Lz4BlockOutputStream<W> {
    fn drop(&mut self) {
        // Best-effort close. Java's `close()` is idempotent and writes the
        // end-mark; we do the same.
        let _ = self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// FLG/BD round-trip via byte conversion, mirroring Java's `fromByte`/`toByte`.
    #[test]
    fn flg_round_trip() {
        let flg = Flg::new(false).unwrap();
        let parsed = Flg::from_byte(flg.to_byte()).unwrap();
        assert_eq!(parsed.to_byte(), flg.to_byte());
        assert!(!parsed.is_block_checksum_set());

        let flg = Flg::new(true).unwrap();
        let parsed = Flg::from_byte(flg.to_byte()).unwrap();
        assert!(parsed.is_block_checksum_set());
    }

    #[test]
    fn bd_round_trip() {
        for v in 4..=7u8 {
            let bd = Bd::new(v).unwrap();
            assert_eq!(bd.block_maximum_size(), 1 << ((2 * v as usize) + 8));
            let parsed = Bd::from_byte(bd.to_byte()).unwrap();
            assert_eq!(parsed.to_byte(), bd.to_byte());
        }
    }

    #[test]
    fn bd_rejects_invalid_block_size() {
        assert!(Bd::new(3).is_err());
        assert!(Bd::new(8).is_err());
    }

    #[test]
    fn xxh32_known_vectors() {
        // canonical XXH32 of empty string with seed 0 = 0x02CC5D05
        assert_eq!(xxh32(&[]), 0x02CC5D05);
    }

    #[test]
    fn writes_header_with_correct_magic() {
        // We capture the header bytes immediately after construction.
        // The Drop on the stream writes the end-mark (4 zero bytes), so we
        // assert on the prefix.
        let mut sink: Vec<u8> = Vec::new();
        {
            let _ = Lz4BlockOutputStream::with_default_block_size(&mut sink, 9, false).unwrap();
            // drop here writes end-mark (4 LE zero bytes for empty content).
        }
        // Header: magic (4 LE) + flg + bd + hc = 7 bytes;
        // then 4-byte end-mark (zeroes) from close.
        // 0x184D2204 LE = [0x04, 0x22, 0x4D, 0x18]
        assert_eq!(sink[0..4], [0x04, 0x22, 0x4D, 0x18]);
        assert!(sink.len() >= 7);
        // Last 4 bytes are the end-mark.
        assert_eq!(sink[sink.len() - 4..], [0u8, 0, 0, 0]);
    }
}
