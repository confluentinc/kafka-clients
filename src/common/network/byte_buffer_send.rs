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

//! A send backed by an array of byte buffers.
//!
//! Translated from `org.apache.kafka.common.network.ByteBufferSend`.

use super::KafkaSend;
use super::TransportLayer;

use std::fmt;
use std::future::Future;
use std::io;
use std::pin::Pin;

/// A send backed by an array of byte buffers.
///
/// Each buffer is written in sequence to the destination channel.
/// Uses vectored writes (`IoSlice`) for efficient scatter-gather I/O.
pub struct ByteBufferSend {
    /// The byte buffers to send. Each buffer tracks its own position
    /// as a `(data, offset)` pair where offset marks the next byte to write.
    buffers: Vec<(Vec<u8>, usize)>,
    /// The total size of this send (sum of all buffer lengths).
    size: usize,
    /// The remaining number of bytes to write.
    remaining: usize,
    /// Whether the underlying channel has pending writes.
    pending: bool,
}

impl ByteBufferSend {
    /// Creates a new `ByteBufferSend` from the given byte buffers.
    ///
    /// The size is computed as the sum of all buffer lengths.
    pub fn new(buffers: Vec<Vec<u8>>) -> Self {
        let remaining: usize = buffers.iter().map(|b| b.len()).sum();
        let size = remaining;
        let buffers = buffers.into_iter().map(|b| (b, 0)).collect();
        Self { buffers, size, remaining, pending: false }
    }

    /// Creates a new `ByteBufferSend` from the given byte buffers with a pre-computed size.
    ///
    /// This constructor allows specifying the size explicitly, which may differ from the
    /// sum of buffer lengths if buffers have already been partially consumed.
    pub fn with_size(buffers: Vec<Vec<u8>>, size: usize) -> Self {
        let buffers = buffers.into_iter().map(|b| (b, 0)).collect();
        Self { buffers, size, remaining: size, pending: false }
    }

    /// Creates a size-prefixed send: prepends a 4-byte big-endian size header
    /// followed by the given buffer's content.
    pub fn size_prefixed(buffer: Vec<u8>) -> Self {
        let size_buffer = (buffer.len() as i32).to_be_bytes().to_vec();
        Self::new(vec![size_buffer, buffer])
    }

    /// Returns the number of bytes remaining to be written.
    pub fn remaining(&self) -> usize {
        self.remaining
    }
}

impl KafkaSend for ByteBufferSend {
    fn completed(&self) -> bool {
        self.remaining == 0 && !self.pending
    }

    fn write_to<'a>(
        &'a mut self,
        channel: &'a mut dyn TransportLayer,
    ) -> Pin<Box<dyn Future<Output = io::Result<usize>> + Send + 'a>> {
        Box::pin(async {
            // Build IoSlice views of unwritten portions of each buffer
            let slices: Vec<io::IoSlice<'_>> = self
                .buffers
                .iter()
                .filter(|(data, offset)| *offset < data.len())
                .map(|(data, offset)| io::IoSlice::new(&data[*offset..]))
                .collect();

            let written = if slices.is_empty() {
                0
            } else {
                match channel.write_vectored(&slices).await {
                    Ok(n) => n,
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        // No space in the socket buffer right now. Matches Java NIO
                        // non-blocking write returning 0: try again later.
                        0
                    },
                    Err(e) => return Err(e),
                }
            };

            if written == 0 {
                self.pending = channel.has_pending_writes();
                return Ok(0);
            }

            // Advance buffer offsets based on bytes written
            let mut to_consume = written;
            for (data, offset) in &mut self.buffers {
                if to_consume == 0 {
                    break;
                }
                let available = data.len() - *offset;
                let consumed = to_consume.min(available);
                *offset += consumed;
                to_consume -= consumed;
            }

            self.remaining -= written;
            self.pending = channel.has_pending_writes();
            Ok(written)
        })
    }

    fn try_write_to(&mut self, channel: &mut dyn TransportLayer) -> io::Result<usize> {
        let mut slices_buf = [io::IoSlice::new(&[]); 8];
        let mut count = 0;
        for (data, offset) in &self.buffers {
            if *offset < data.len() && count < slices_buf.len() {
                slices_buf[count] = io::IoSlice::new(&data[*offset..]);
                count += 1;
            }
        }

        if count == 0 {
            if self.pending {
                let _ = channel.try_write_vectored(&[]);
                self.pending = channel.has_pending_writes();
            }
            return Ok(0);
        }

        // Propagate WouldBlock so the caller can fall back to async.
        let written = channel.try_write_vectored(&slices_buf[..count])?;

        if written == 0 {
            self.pending = channel.has_pending_writes();
            return Ok(0);
        }

        let mut to_consume = written;
        for (data, offset) in &mut self.buffers {
            if to_consume == 0 {
                break;
            }
            let available = data.len() - *offset;
            let consumed = to_consume.min(available);
            *offset += consumed;
            to_consume -= consumed;
        }

        self.remaining -= written;
        self.pending = channel.has_pending_writes();
        Ok(written)
    }

    fn size(&self) -> usize {
        self.size
    }
}

impl fmt::Display for ByteBufferSend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ByteBufferSend(size={}, remaining={}, pending={})",
            self.size, self.remaining, self.pending
        )
    }
}
