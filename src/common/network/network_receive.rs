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
    /// Payload buffer once allocated. Mirrors Java's `buffer`. Allocated to
    /// the full `requested_buffer_size` capacity once known and read into
    /// directly via `&mut buf[payload_pos..]` — no per-call scratch buffer.
    buffer: Option<BytesMut>,
    /// Number of payload bytes filled so far (mirrors Java's
    /// `buffer.position()` for the payload buffer). Used to determine the
    /// next write window into [`Self::buffer`] without relying on
    /// `BytesMut::len`, which we keep equal to `requested_buffer_size`
    /// once allocated so the receive owns a single contiguous, zeroed
    /// region (matching Java's `ByteBuffer` allocation pattern).
    payload_pos: usize,
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
            payload_pos: 0,
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
    /// receive pre-populated with a payload, used by tests. Mirrors the
    /// Java semantics exactly: the `size` ByteBuffer is left fresh
    /// (`position=0`, `limit=4`), so `complete()` returns `false`,
    /// `bytes_read()` returns `buffer.position()` (== 0 in Java since
    /// the caller hasn't moved the buffer's position), and `size()`
    /// returns `buffer.limit() + 4`. The size header is **not**
    /// synthesised from the payload length.
    pub fn with_buffer(source: impl Into<String>, buffer: BytesMut) -> Self {
        NetworkReceive {
            source: source.into(),
            size_buf: [0; 4],
            size_pos: 0,
            max_size: UNLIMITED,
            requested_buffer_size: -1,
            buffer: Some(buffer),
            payload_pos: 0,
        }
    }

    /// Mirrors `NetworkReceive.bytesRead()`. Java returns
    /// `size.position()` when buffer is null, else
    /// `buffer.position() + size.position()`.
    pub fn bytes_read(&self) -> i32 {
        if self.buffer.is_none() {
            self.size_pos as i32
        } else {
            self.payload_pos as i32 + self.size_pos as i32
        }
    }

    /// Mirrors `NetworkReceive.size()` — total receive size including the
    /// 4-byte length header. Java implements this as
    /// `payload().limit() + size.limit()` and NPEs if `payload() == null`.
    ///
    /// **Precondition (mirrors Java):** the payload buffer must be set
    /// before calling — either by completing the size-header parse or via
    /// the `with_buffer` constructor. Callers that compute metrics during
    /// the lifecycle of a receive should gate on
    /// [`Receive::complete`] or [`Receive::memory_allocated`] first
    /// (see CLAUDE.md rule 10.1: panic mirrors the Java unchecked
    /// `NullPointerException`, since the only legitimate caller is
    /// metrics emission which already knows how to gate).
    pub fn size(&self) -> i32 {
        // Mirrors Java's `payload().limit() + size.limit()`. When the size
        // header has been parsed, `requested_buffer_size` is the authoritative
        // payload limit. When the receive was constructed with a pre-populated
        // buffer (Java sets `buffer = ByteBuffer.allocate(N)` directly, leaving
        // `size` fresh), the payload's `limit()` equals the buffer's length.
        let payload_limit = if self.requested_buffer_size >= 0 {
            self.requested_buffer_size
        } else if let Some(buf) = self.buffer.as_ref() {
            buf.len() as i32
        } else {
            // Mirrors Java's NPE on `payload().limit()` when buffer == null.
            panic!("NetworkReceive.size() called before the size header was parsed");
        };
        4 + payload_limit
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
            && self.buffer.is_some()
            && self.requested_buffer_size >= 0
            && self.payload_pos as i32 == self.requested_buffer_size
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

        // Allocate the payload buffer once we know the size. Allocate once
        // at the full requested size (zero-initialised so the spare slice
        // is a valid `&mut [u8]`); subsequent reads fill `&mut buf[pos..]`
        // in place — no per-call scratch buffer, mirroring Java's
        // `channel.read(buffer)` directly into the backing `ByteBuffer`.
        if self.buffer.is_none() && self.requested_buffer_size > 0 {
            let cap = self.requested_buffer_size as usize;
            let mut buf = BytesMut::with_capacity(cap);
            buf.resize(cap, 0);
            self.buffer = Some(buf);
        }

        // Fill the payload buffer in place. Skip when the size header
        // hasn't been parsed yet (e.g. a fixture constructed via
        // `with_buffer`) — Java behaves the same: `requestedBufferSize`
        // remains -1 and the channel.read call still happens, but no
        // size is known.
        if self.requested_buffer_size > 0
            && let Some(buf) = self.buffer.as_mut()
        {
            let total = self.requested_buffer_size as usize;
            if self.payload_pos < total {
                let n = src.read(&mut buf[self.payload_pos..total])?;
                self.payload_pos += n;
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
        self.payload_pos = 0;
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

    /// Java parity check for the `with_buffer` constructor: the size header
    /// is left fresh (not synthesised), so `complete()` is `false` and
    /// `bytes_read()` excludes the unread size header. Mirrors Java's
    /// `new NetworkReceive(source, ByteBuffer.allocate(8).put(...))`
    /// behaviour where `size.position() == 0` and `buffer.position() == 0`
    /// (caller hasn't moved the position).
    #[test]
    fn with_buffer_does_not_synthesise_size_header() {
        let payload_size = 8usize;
        let mut buf = BytesMut::with_capacity(payload_size);
        buf.extend_from_slice(&(0..payload_size as u8).collect::<Vec<u8>>());

        let receive = NetworkReceive::with_buffer("0", buf);
        assert!(
            !receive.complete(),
            "complete() must be false: size.hasRemaining() in Java is true"
        );
        // bytes_read in Java = buffer.position() + size.position() = 0 + 0 = 0
        // for a freshly-wrapped ByteBuffer whose position the caller did not
        // advance. Our BytesMut doesn't track a separate position; we mirror
        // the Java `bytesRead` == 0 when nothing has been read off the wire.
        assert_eq!(
            receive.bytes_read(),
            0,
            "bytes_read should be 0 (size_pos=0, payload_pos=0) before any read_from call"
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
