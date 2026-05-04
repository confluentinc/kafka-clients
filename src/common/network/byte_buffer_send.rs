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

//! Translation of `org.apache.kafka.common.network.ByteBufferSend`.

use std::io::{self, IoSlice};

use bytes::Bytes;

use super::{Send, TransferableChannel};

/// A send backed by an array of byte buffers.
///
/// Mirrors the Java `ByteBufferSend`. The Java implementation holds a
/// `ByteBuffer[]` and tracks a `remaining` byte counter plus a `pending`
/// flag set from `TransferableChannel.hasPendingWrites()`. We mirror that
/// shape with `Vec<Bytes>` (each `bytes::Bytes` is cheap-cloneable shared
/// ownership — never deep-copied on the send path) and per-buffer cursors.
///
/// The wire-write path uses `write_vectored` (Java's
/// `GatheringByteChannel.write(ByteBuffer[])`) so the concatenated framing
/// header + payload is never assembled into a single contiguous buffer
/// (CLAUDE.md rule 12).
pub struct ByteBufferSend {
    buffers: Vec<Bytes>,
    /// Per-buffer offset of how many bytes have already been written.
    /// `offsets[i] <= buffers[i].len()`.
    offsets: Vec<usize>,
    size: u64,
    remaining: u64,
    pending: bool,
}

impl ByteBufferSend {
    /// Construct a `ByteBufferSend` from a sequence of `Bytes` chunks. The
    /// total size is the sum of the chunk lengths. Mirrors the Java
    /// varargs constructor `ByteBufferSend(ByteBuffer... buffers)`.
    pub fn from_buffers(buffers: Vec<Bytes>) -> Self {
        let size: u64 = buffers.iter().map(|b| b.len() as u64).sum();
        let offsets = vec![0; buffers.len()];
        ByteBufferSend { buffers, offsets, size, remaining: size, pending: false }
    }

    /// Construct a `ByteBufferSend` with an explicit size. Mirrors
    /// `ByteBufferSend(ByteBuffer[] buffers, long size)`. The Java
    /// constructor is used when only part of the buffer's contents will be
    /// sent — the size is taken as authoritative.
    pub fn from_buffers_with_size(buffers: Vec<Bytes>, size: u64) -> Self {
        let offsets = vec![0; buffers.len()];
        ByteBufferSend { buffers, offsets, size, remaining: size, pending: false }
    }

    /// Number of bytes remaining to be written. Mirrors
    /// `ByteBufferSend.remaining()`.
    pub fn remaining(&self) -> u64 {
        self.remaining
    }

    /// Mirrors `ByteBufferSend.sizePrefixed(ByteBuffer)`. Prepends a
    /// 4-byte big-endian length header in front of `payload`. Header and
    /// payload are kept as separate buffers so the eventual
    /// `write_vectored` send path can hand them to the kernel in a single
    /// `writev` call without an intermediate copy (CLAUDE.md rule 12).
    pub fn size_prefixed(payload: Bytes) -> Self {
        let len = payload.len() as i32;
        let mut header = [0u8; 4];
        header.copy_from_slice(&len.to_be_bytes());
        let header_buf = Bytes::copy_from_slice(&header);
        Self::from_buffers(vec![header_buf, payload])
    }
}

impl Send for ByteBufferSend {
    fn completed(&self) -> bool {
        self.remaining == 0 && !self.pending
    }

    fn size(&self) -> u64 {
        self.size
    }

    fn write_to(&mut self, channel: &mut dyn TransferableChannel) -> io::Result<u64> {
        // Build the IoSlice list from the unsent tail of each buffer.
        let mut slices: Vec<IoSlice<'_>> = Vec::with_capacity(self.buffers.len());
        for (buf, off) in self.buffers.iter().zip(self.offsets.iter()) {
            if *off < buf.len() {
                slices.push(IoSlice::new(&buf[*off..]));
            }
        }

        if slices.is_empty() {
            self.pending = channel.has_pending_writes();
            return Ok(0);
        }

        let written = channel.write_vectored(&slices)?;
        // Mirror Java's EOF check: `if (written < 0) throw new EOFException`.
        // Rust's `io::Write::write_vectored` cannot return negative — but we
        // forward `Ok(0)` straight through (a closed channel returns it).

        // Advance offsets/remaining to match `written` bytes.
        let mut to_consume = written;
        for (buf, off) in self.buffers.iter().zip(self.offsets.iter_mut()) {
            if to_consume == 0 {
                break;
            }
            let avail = buf.len() - *off;
            let step = avail.min(to_consume);
            *off += step;
            to_consume -= step;
        }
        self.remaining = self.remaining.saturating_sub(written as u64);
        self.pending = channel.has_pending_writes();
        Ok(written as u64)
    }
}

impl std::fmt::Debug for ByteBufferSend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ByteBufferSend")
            .field("size", &self.size)
            .field("remaining", &self.remaining)
            .field("pending", &self.pending)
            .finish()
    }
}

impl std::fmt::Display for ByteBufferSend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ByteBufferSend(, size={}, remaining={}, pending={})",
            self.size, self.remaining, self.pending
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Mock channel that records what was written and reports
    /// `hasPendingWrites = false`. The `write_vectored` impl writes at most
    /// `max_per_call` bytes per invocation so we can exercise partial
    /// writes.
    struct MockChannel {
        sink: Vec<u8>,
        max_per_call: Option<usize>,
        pending: bool,
    }

    impl MockChannel {
        fn new() -> Self {
            MockChannel { sink: Vec::new(), max_per_call: None, pending: false }
        }

        fn with_cap(max_per_call: usize) -> Self {
            MockChannel { sink: Vec::new(), max_per_call: Some(max_per_call), pending: false }
        }
    }

    impl TransferableChannel for MockChannel {
        fn write_vectored(&mut self, bufs: &[IoSlice<'_>]) -> io::Result<usize> {
            let mut total = 0usize;
            let cap = self.max_per_call;
            for slice in bufs {
                let remaining_cap = cap.map(|c| c.saturating_sub(total));
                let step = match remaining_cap {
                    Some(0) => break,
                    Some(c) => slice.len().min(c),
                    None => slice.len(),
                };
                self.sink.extend_from_slice(&slice[..step]);
                total += step;
                if remaining_cap == Some(step) && step < slice.len() {
                    // We hit the cap mid-buffer.
                    break;
                }
            }
            Ok(total)
        }

        fn has_pending_writes(&self) -> bool {
            self.pending
        }
    }

    #[test]
    fn empty_send_is_completed() {
        let send = ByteBufferSend::from_buffers(Vec::new());
        assert!(send.completed());
        assert_eq!(send.size(), 0);
        assert_eq!(send.remaining(), 0);
    }

    #[test]
    fn single_buffer_full_write() {
        let payload = Bytes::from_static(b"abcdef");
        let mut send = ByteBufferSend::from_buffers(vec![payload]);
        assert_eq!(send.size(), 6);
        assert!(!send.completed());

        let mut channel = MockChannel::new();
        let written = send.write_to(&mut channel).expect("write");
        assert_eq!(written, 6);
        assert!(send.completed());
        assert_eq!(channel.sink, b"abcdef");
    }

    #[test]
    fn multi_buffer_concatenation() {
        let mut send = ByteBufferSend::from_buffers(vec![Bytes::from_static(b"head"), Bytes::from_static(b"-tail")]);
        assert_eq!(send.size(), 9);
        let mut channel = MockChannel::new();
        let written = send.write_to(&mut channel).expect("write");
        assert_eq!(written, 9);
        assert!(send.completed());
        assert_eq!(channel.sink, b"head-tail");
    }

    #[test]
    fn partial_write_then_resume() {
        let mut send = ByteBufferSend::from_buffers(vec![Bytes::from_static(b"hello"), Bytes::from_static(b"world")]);
        let mut channel = MockChannel::with_cap(3);
        // First call: 3 bytes from buffer 0.
        let n1 = send.write_to(&mut channel).expect("w1");
        assert_eq!(n1, 3);
        assert_eq!(send.remaining(), 7);
        assert!(!send.completed());
        // Second call: 3 more (2 from buffer 0, 1 from buffer 1).
        let n2 = send.write_to(&mut channel).expect("w2");
        assert_eq!(n2, 3);
        assert_eq!(send.remaining(), 4);
        // Drain the rest.
        channel.max_per_call = None;
        let n3 = send.write_to(&mut channel).expect("w3");
        assert_eq!(n3, 4);
        assert!(send.completed());
        assert_eq!(channel.sink, b"helloworld");
    }

    #[test]
    fn size_prefixed_emits_header_then_payload() {
        let payload = Bytes::from_static(b"hello");
        let mut send = ByteBufferSend::size_prefixed(payload);
        assert_eq!(send.size(), 4 + 5);
        let mut channel = MockChannel::new();
        send.write_to(&mut channel).expect("write");
        assert!(send.completed());
        // Header is the big-endian i32 length (5).
        assert_eq!(channel.sink, vec![0, 0, 0, 5, b'h', b'e', b'l', b'l', b'o']);
    }

    #[test]
    fn pending_writes_keep_send_incomplete() {
        let mut send = ByteBufferSend::from_buffers(vec![Bytes::from_static(b"x")]);
        let mut channel = MockChannel::new();
        channel.pending = true;
        let written = send.write_to(&mut channel).expect("write");
        assert_eq!(written, 1);
        assert!(!send.completed());
        // Once pending clears, the send is complete.
        channel.pending = false;
        let _ = send.write_to(&mut channel).expect("flush pending");
        assert!(send.completed());
    }
}
