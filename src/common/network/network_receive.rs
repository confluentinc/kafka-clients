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

//! Translation of `org.apache.kafka.common.network.NetworkReceive`.

use std::io;

use bytes::BytesMut;

use super::{InvalidReceiveError, Receive};

/// Convention for an unset source (mirrors `NetworkReceive.UNKNOWN_SOURCE`).
pub const UNKNOWN_SOURCE: &str = "";

/// Sentinel for "no maximum size". Mirrors `NetworkReceive.UNLIMITED = -1`.
pub const UNLIMITED: i32 = -1;

/// A size-delimited [`Receive`] consisting of a 4-byte network-ordered
/// size `N` followed by `N` bytes of payload.
///
/// Mirrors the Java `NetworkReceive`. The Java class accepts an optional
/// `MemoryPool` for pooled payload allocation; we leave the memory-pool
/// hook for a future phase and always allocate a fresh `BytesMut` for the
/// payload. `BytesMut` enables zero-copy hand-off (`freeze` to `Bytes`)
/// downstream of the receive.
pub struct NetworkReceive {
    source: String,
    /// Size header — always 4 bytes once filled. We track `size_pos` ranging
    /// `0..=4` to mirror Java's `ByteBuffer.position`.
    size_buf: [u8; 4],
    size_pos: usize,
    max_size: i32,
    /// Once the size header is parsed, set to the requested payload size
    /// (may be 0).  Mirrors Java's `requestedBufferSize` initialised to -1.
    requested_buffer_size: i32,
    /// Payload buffer once allocated. Mirrors Java's `buffer`.
    buffer: Option<BytesMut>,
}

impl NetworkReceive {
    /// Mirrors `new NetworkReceive(int maxSize, String source)`.
    pub fn with_max_size(max_size: i32, source: impl Into<String>) -> Self {
        NetworkReceive {
            source: source.into(),
            size_buf: [0; 4],
            size_pos: 0,
            max_size,
            requested_buffer_size: -1,
            buffer: None,
        }
    }

    /// Mirrors `new NetworkReceive(String source)`.
    pub fn with_source(source: impl Into<String>) -> Self {
        Self::with_max_size(UNLIMITED, source)
    }

    /// Mirrors the no-arg `new NetworkReceive()`.
    pub fn new() -> Self {
        Self::with_source(UNKNOWN_SOURCE)
    }

    /// Mirrors `new NetworkReceive(String source, ByteBuffer buffer)` — a
    /// receive pre-populated with a payload, used by tests. The size header
    /// is treated as fully written (`size_pos = 4`).
    pub fn with_buffer(source: impl Into<String>, buffer: BytesMut) -> Self {
        let len = buffer.len() as i32;
        let mut size_buf = [0u8; 4];
        size_buf.copy_from_slice(&len.to_be_bytes());
        NetworkReceive {
            source: source.into(),
            size_buf,
            size_pos: 4,
            max_size: UNLIMITED,
            requested_buffer_size: len,
            buffer: Some(buffer),
        }
    }

    /// Mirrors `NetworkReceive.bytesRead()`.
    pub fn bytes_read(&self) -> i32 {
        self.size_pos as i32 + self.buffer.as_ref().map(|b| b.len() as i32).unwrap_or(0)
    }

    /// Mirrors `NetworkReceive.size()` — total receive size including the
    /// 4-byte length header. Java requires the payload buffer to be set
    /// before it's safe to call (`payload().limit()` would NPE otherwise).
    /// We mirror that contract by panicking only if the size header has
    /// not been parsed; once it's parsed, `requested_buffer_size` is the
    /// authoritative payload size even before the payload bytes have been
    /// fully read (matching Java's `payload().limit()` after `flip`).
    pub fn size(&self) -> i32 {
        // 4 byte size header + payload size.
        if self.requested_buffer_size < 0 {
            // Mirrors Java's NPE on `payload().limit()` when buffer == null.
            panic!("NetworkReceive.size() called before the size header was parsed");
        }
        4 + self.requested_buffer_size
    }

    /// The parsed payload (the `N` bytes after the size header). Mirrors
    /// `NetworkReceive.payload()`. Returns `None` until the size header is
    /// fully read.
    pub fn payload(&self) -> Option<&BytesMut> {
        self.buffer.as_ref()
    }

    /// Detach the payload and return it as a [`bytes::Bytes`] for zero-copy
    /// hand-off to downstream parsers. Mirrors taking ownership of
    /// `payload()`. Once detached, the receive is no longer usable for
    /// reads.
    pub fn take_payload(&mut self) -> Option<bytes::Bytes> {
        self.buffer.take().map(BytesMut::freeze)
    }
}

impl Default for NetworkReceive {
    fn default() -> Self {
        Self::new()
    }
}

impl Receive for NetworkReceive {
    fn source(&self) -> &str {
        &self.source
    }

    fn complete(&self) -> bool {
        self.size_pos == 4
            && self
                .buffer
                .as_ref()
                .is_some_and(|b| b.len() as i32 == self.requested_buffer_size)
    }

    fn read_from(&mut self, src: &mut dyn io::Read) -> io::Result<u64> {
        let mut read: u64 = 0;

        // First fill the 4-byte size header.
        //
        // Java's NIO `channel.read(buffer)` returns:
        //   * a positive count when bytes were copied,
        //   * 0 when the channel has no bytes available right now (the
        //     non-blocking "would block" signal — the receive simply
        //     returns and the caller retries on the next select wake),
        //   * -1 on end-of-stream (close), at which point Java throws
        //     `EOFException`.
        //
        // In Rust, `io::Read::read` returns `Ok(0)` for either case
        // (well-behaved non-blocking adapters like Tokio surface "would
        // block" through `Err(WouldBlock)` instead). The translation
        // therefore treats `Ok(0)` as "no more progress on this call";
        // upper-layer EOF detection lives in the Phase 5b/5c transport,
        // which can distinguish a closed socket from a quiet one.
        if self.size_pos < 4 {
            let n = src.read(&mut self.size_buf[self.size_pos..])?;
            self.size_pos += n;
            read += n as u64;

            if self.size_pos == 4 {
                let receive_size = i32::from_be_bytes(self.size_buf);
                if receive_size < 0 {
                    return Err(io::Error::from(InvalidReceiveError::new(format!(
                        "Invalid receive (size = {receive_size})"
                    ))));
                }
                if self.max_size != UNLIMITED && receive_size > self.max_size {
                    return Err(io::Error::from(InvalidReceiveError::new(format!(
                        "Invalid receive (size = {} larger than {})",
                        receive_size, self.max_size
                    ))));
                }
                self.requested_buffer_size = receive_size;
                if receive_size == 0 {
                    self.buffer = Some(BytesMut::new());
                }
            }
        }

        // Allocate the payload buffer once we know the size.
        if self.buffer.is_none() && self.requested_buffer_size > 0 {
            let cap = self.requested_buffer_size as usize;
            let mut buf = BytesMut::with_capacity(cap);
            buf.resize(cap, 0);
            // Reset to empty so we can fill with successive reads.
            buf.clear();
            self.buffer = Some(buf);
        }

        // Fill the payload buffer.
        if let Some(buf) = self.buffer.as_mut() {
            let needed = self.requested_buffer_size as usize - buf.len();
            if needed > 0 {
                let mut scratch = vec![0u8; needed];
                let n = src.read(&mut scratch)?;
                buf.extend_from_slice(&scratch[..n]);
                read += n as u64;
            }
        }

        Ok(read)
    }

    fn required_memory_amount_known(&self) -> bool {
        self.requested_buffer_size != -1
    }

    fn memory_allocated(&self) -> bool {
        self.buffer.is_some()
    }

    fn close(&mut self) -> io::Result<()> {
        // Drop the payload buffer. Equivalent to Java's
        // `memoryPool.release(buffer); buffer = null;`.
        self.buffer = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Read};

    use super::*;

    /// Mock `io::Read` that delivers a sequence of canned chunks, one per
    /// `read` call. Mirrors the Mockito `ScatteringByteChannel.read(buf)`
    /// answers used in the Java tests.
    struct ChunkedReader {
        chunks: Vec<Vec<u8>>,
    }

    impl ChunkedReader {
        fn new(chunks: Vec<Vec<u8>>) -> Self {
            ChunkedReader { chunks }
        }
    }

    impl Read for ChunkedReader {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            if self.chunks.is_empty() {
                return Ok(0);
            }
            let next = self.chunks.remove(0);
            let n = next.len().min(buf.len());
            buf[..n].copy_from_slice(&next[..n]);
            Ok(n)
        }
    }

    fn random_bytes(n: usize) -> Vec<u8> {
        (0..n).map(|i| (i % 251) as u8).collect()
    }

    /// Translation of `NetworkReceiveTest.testBytesRead`.
    #[test]
    fn bytes_read() {
        let mut receive = NetworkReceive::with_max_size(128, "0");
        assert_eq!(receive.bytes_read(), 0);

        // First read: 4 bytes of size = 128 (big-endian).
        let mut size_chunk = ChunkedReader::new(vec![128i32.to_be_bytes().to_vec()]);
        assert_eq!(receive.read_from(&mut size_chunk).expect("read"), 4);
        assert_eq!(receive.bytes_read(), 4);
        assert!(!receive.complete());

        // Second read: 64 random payload bytes.
        let chunk = random_bytes(64);
        let mut r2 = ChunkedReader::new(vec![chunk]);
        assert_eq!(receive.read_from(&mut r2).expect("read"), 64);
        assert_eq!(receive.bytes_read(), 68);
        assert!(!receive.complete());

        // Third read: 64 more bytes — completes.
        let chunk = random_bytes(64);
        let mut r3 = ChunkedReader::new(vec![chunk]);
        assert_eq!(receive.read_from(&mut r3).expect("read"), 64);
        assert_eq!(receive.bytes_read(), 132);
        assert!(receive.complete());
    }

    /// Translation of
    /// `NetworkReceiveTest.testRequiredMemoryAmountKnownWhenNotSet`.
    #[test]
    fn required_memory_amount_known_when_not_set() {
        let receive = NetworkReceive::with_source("0");
        assert!(
            !receive.required_memory_amount_known(),
            "Memory amount should not be known before read."
        );
    }

    /// Translation of
    /// `NetworkReceiveTest.testRequiredMemoryAmountKnownWhenSet`.
    #[test]
    fn required_memory_amount_known_when_set() {
        let mut receive = NetworkReceive::with_max_size(128, "0");
        let mut r = ChunkedReader::new(vec![64i32.to_be_bytes().to_vec()]);
        receive.read_from(&mut r).expect("read");
        assert!(
            receive.required_memory_amount_known(),
            "Memory amount should be known after read."
        );
    }

    /// Translation of `NetworkReceiveTest.testSizeWithPredefineBuffer`.
    #[test]
    fn size_with_predefined_buffer() {
        let payload_size = 8usize;
        let payload = (0..payload_size as u8).collect::<Vec<u8>>();
        let mut buf = BytesMut::with_capacity(payload_size);
        buf.extend_from_slice(&payload);

        let receive = NetworkReceive::with_buffer("0", buf);
        assert_eq!(
            receive.size() as usize,
            4 + payload_size,
            "The total size should be the sum of the size buffer and payload."
        );
    }

    /// Translation of `NetworkReceiveTest.testSizeAfterRead`.
    #[test]
    fn size_after_read() {
        let payload_size = 32i32;
        let mut receive = NetworkReceive::with_max_size(128, "0");
        let mut r = ChunkedReader::new(vec![payload_size.to_be_bytes().to_vec()]);
        receive.read_from(&mut r).expect("read");
        assert_eq!(
            receive.size(),
            4 + payload_size,
            "The total size should be the sum of the size buffer and receive size."
        );
    }

    #[test]
    fn invalid_negative_size_is_rejected() {
        let mut receive = NetworkReceive::with_max_size(128, "0");
        let mut bytes = (-5i32).to_be_bytes().to_vec();
        // Append some payload so the cursor returns the size in one call.
        bytes.extend_from_slice(&[0u8; 4]);
        let mut cursor = Cursor::new(bytes);
        let err = receive.read_from(&mut cursor).expect_err("expected invalid size");
        assert!(err.to_string().contains("Invalid receive"));
    }

    #[test]
    fn size_larger_than_max_is_rejected() {
        let mut receive = NetworkReceive::with_max_size(8, "0");
        let mut bytes = 100i32.to_be_bytes().to_vec();
        bytes.extend_from_slice(&[0u8; 4]);
        let mut cursor = Cursor::new(bytes);
        let err = receive.read_from(&mut cursor).expect_err("expected oversize");
        assert!(err.to_string().contains("larger than 8"));
    }

    #[test]
    fn close_releases_buffer() {
        let mut buf = BytesMut::with_capacity(4);
        buf.extend_from_slice(b"abcd");
        let mut receive = NetworkReceive::with_buffer("0", buf);
        assert!(receive.memory_allocated());
        receive.close().expect("close");
        assert!(!receive.memory_allocated());
    }
}
