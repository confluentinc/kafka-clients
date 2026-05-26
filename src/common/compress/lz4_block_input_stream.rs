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

//! Translation of `org.apache.kafka.common.compress.Lz4BlockInputStream`.
//!
//! Reads the framed-LZ4 wire format produced by `Lz4BlockOutputStream`.

use std::hash::Hasher;
use std::io::{self, Read};

use twox_hash::XxHash32;

use crate::common::compress::lz4_block_output_stream::{Bd, Flg, LZ4_FRAME_INCOMPRESSIBLE_MASK, MAGIC};
use crate::common::utils::buffer_supplier::BufferSupplier;

pub const PREMATURE_EOS: &str = "Stream ended prematurely";
pub const NOT_SUPPORTED: &str = "Stream unsupported (invalid magic bytes)";
pub const BLOCK_HASH_MISMATCH: &str = "Block checksum mismatch";
pub const DESCRIPTOR_HASH_MISMATCH: &str = "Stream frame descriptor corrupted";

fn xxh32(data: &[u8]) -> u32 {
    let mut h = XxHash32::with_seed(0);
    h.write(data);
    h.finish() as u32
}

/// Streaming framed-LZ4 reader. Mirrors Java's `Lz4BlockInputStream`.
///
/// Owns its own decompression buffer (sourced from the [`BufferSupplier`]
/// at construction time, returned to the supplier on drop). The `'a`
/// lifetime ties the reader to the borrowed input slice. Java's `slice()`
/// optimization for uncompressed blocks (avoiding a copy) is sacrificed in
/// the Rust translation: we always copy block bytes into the owned buffer
/// to keep lifetimes straightforward and to match `BufferSupplier`'s
/// "the buffer is held while data is consumed" contract.
pub struct Lz4BlockInputStream<'a> {
    /// Slice of the input buffer that has not yet been read.
    input: &'a [u8],
    flg: Flg,
    max_block_size: usize,
    ignore_flag_descriptor_checksum: bool,
    /// Decompression buffer — re-used per block.
    decompression_buffer: Vec<u8>,
    /// Offset into `decompression_buffer` of the next byte to be returned.
    decompressed_offset: usize,
    /// One past the last valid byte in `decompression_buffer`.
    decompressed_len: usize,
    finished: bool,
    /// Borrowed back to the [`BufferSupplier`] on `close()`.
    supplier: BufferSupplier,
}

impl<'a> Lz4BlockInputStream<'a> {
    /// Mirrors Java's `Lz4BlockInputStream(ByteBuffer in, BufferSupplier bufferSupplier, boolean ignoreFlagDescriptorChecksum)`.
    pub fn new(
        buffer: &'a [u8],
        mut supplier: BufferSupplier,
        ignore_flag_descriptor_checksum: bool,
    ) -> io::Result<Self> {
        let mut input = buffer;
        let (flg, max_block_size) = read_header(&mut input, ignore_flag_descriptor_checksum)?;
        let decompression_buffer = supplier.get(max_block_size);

        Ok(Lz4BlockInputStream {
            input,
            flg,
            max_block_size,
            ignore_flag_descriptor_checksum,
            decompression_buffer,
            decompressed_offset: 0,
            decompressed_len: 0,
            finished: false,
            supplier,
        })
    }

    pub fn ignore_flag_descriptor_checksum(&self) -> bool {
        self.ignore_flag_descriptor_checksum
    }

    /// Number of bytes ready in the current decompressed window.
    pub fn available(&self) -> usize {
        self.decompressed_len.saturating_sub(self.decompressed_offset)
    }

    /// Skip up to `n` bytes from the decompressed stream. Mirrors Java's
    /// `skip(long)` (returning bytes actually skipped).
    pub fn skip(&mut self, n: usize) -> io::Result<usize> {
        if self.finished {
            return Ok(0);
        }
        if self.available() == 0 {
            self.read_block()?;
        }
        if self.finished {
            return Ok(0);
        }
        let skipped = std::cmp::min(n, self.available());
        self.decompressed_offset += skipped;
        Ok(skipped)
    }

    /// Mirrors Java's `readBlock`.
    fn read_block(&mut self) -> io::Result<()> {
        if self.input.len() < 4 {
            return Err(io::Error::other(PREMATURE_EOS));
        }
        let block_header = u32::from_le_bytes(self.input[..4].try_into().unwrap());
        self.input = &self.input[4..];

        let compressed = (block_header & LZ4_FRAME_INCOMPRESSIBLE_MASK) == 0;
        let block_size = (block_header & !LZ4_FRAME_INCOMPRESSIBLE_MASK) as usize;

        if block_size == 0 {
            self.finished = true;
            if self.flg.is_content_checksum_set() {
                if self.input.len() < 4 {
                    return Err(io::Error::other(PREMATURE_EOS));
                }
                self.input = &self.input[4..];
            }
            return Ok(());
        }
        if block_size > self.max_block_size {
            return Err(io::Error::other(format!(
                "Block size {block_size} exceeded max: {}",
                self.max_block_size
            )));
        }
        if self.input.len() < block_size {
            return Err(io::Error::other(PREMATURE_EOS));
        }

        let block_bytes = &self.input[..block_size];

        if compressed {
            let n = lz4_flex::block::decompress_into(block_bytes, &mut self.decompression_buffer)
                .map_err(io::Error::other)?;
            self.decompressed_offset = 0;
            self.decompressed_len = n;
        } else {
            // Copy the uncompressed block into our owned buffer. Java
            // slices the input directly; we copy to keep lifetime-safe
            // borrowing simple.
            self.decompression_buffer[..block_size].copy_from_slice(block_bytes);
            self.decompressed_offset = 0;
            self.decompressed_len = block_size;
        }

        // Verify block checksum.
        if self.flg.is_block_checksum_set() {
            let hash = xxh32(block_bytes);
            self.input = &self.input[block_size..];
            if self.input.len() < 4 {
                return Err(io::Error::other(PREMATURE_EOS));
            }
            let written = u32::from_le_bytes(self.input[..4].try_into().unwrap());
            self.input = &self.input[4..];
            if hash != written {
                return Err(io::Error::other(BLOCK_HASH_MISMATCH));
            }
        } else {
            self.input = &self.input[block_size..];
        }
        Ok(())
    }
}

/// Read the FD header from `input`. Returns `(flg, max_block_size)` and
/// advances `*input` past the header. Mirrors Java's `readHeader`.
fn read_header(input: &mut &[u8], ignore_flag_descriptor_checksum: bool) -> io::Result<(Flg, usize)> {
    if input.len() < 6 {
        return Err(io::Error::other(PREMATURE_EOS));
    }
    let magic = u32::from_le_bytes(input[..4].try_into().unwrap());
    if magic != MAGIC {
        return Err(io::Error::other(NOT_SUPPORTED));
    }
    let post_magic = &input[4..];

    let flg = Flg::from_byte(post_magic[0])?;
    let bd = Bd::from_byte(post_magic[1])?;
    let max_block_size = bd.block_maximum_size();

    let mut consumed = 2usize;
    if flg.is_content_size_set() {
        if post_magic.len() < consumed + 8 {
            return Err(io::Error::other(PREMATURE_EOS));
        }
        consumed += 8;
    }

    if post_magic.len() < consumed + 1 {
        return Err(io::Error::other(PREMATURE_EOS));
    }

    let hc_byte = post_magic[consumed];

    if !ignore_flag_descriptor_checksum {
        let hash = xxh32(&post_magic[..consumed]);
        let expected = ((hash >> 8) & 0xFF) as u8;
        if hc_byte != expected {
            return Err(io::Error::other(DESCRIPTOR_HASH_MISMATCH));
        }
    }
    consumed += 1; // skip HC

    *input = &post_magic[consumed..];
    Ok((flg, max_block_size))
}

impl Read for Lz4BlockInputStream<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.finished {
            return Ok(0);
        }
        if self.available() == 0 {
            self.read_block()?;
        }
        if self.finished {
            return Ok(0);
        }
        let n = std::cmp::min(buf.len(), self.available());
        buf[..n].copy_from_slice(&self.decompression_buffer[self.decompressed_offset..self.decompressed_offset + n]);
        self.decompressed_offset += n;
        Ok(n)
    }
}

impl Drop for Lz4BlockInputStream<'_> {
    fn drop(&mut self) {
        // Return the decompression buffer to the supplier.
        let buf = std::mem::take(&mut self.decompression_buffer);
        self.supplier.release(buf);
    }
}
