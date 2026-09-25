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

//! A size-delimited receive that consists of a 4-byte network-ordered size N followed by
//! N bytes of content.
//!
//! Translated from `org.apache.kafka.common.network.NetworkReceive`.

use super::InvalidReceiveError;
use super::Receive;
use super::TransportLayer;

use log::trace;

use std::future::Future;
use std::io;
use std::pin::Pin;

/// A size-delimited receive that consists of a 4-byte network-ordered size N followed by
/// N bytes of content.
///
/// # Wire Format
///
/// ```text
/// [4 bytes: size (big-endian i32)] [N bytes: payload]
/// ```
///
/// The receive proceeds in two phases:
/// 1. Read the 4-byte size header
/// 2. Allocate and read N bytes of payload
///
/// EOF detection: when the remote end closes the connection, `read()` returns
/// `Ok(0)`. This is translated to an `UnexpectedEof` error to match Java's
/// `EOFException` behavior in `NetworkReceive.readFrom()`.
pub struct NetworkReceive {
    /// The source identifier for this receive.
    source: String,
    /// Buffer for reading the 4-byte size header.
    size_buf: [u8; NetworkReceive::SIZE_LENGTH],
    /// Number of bytes read into the size buffer so far.
    size_bytes_read: usize,
    /// Maximum allowed receive size. `UNLIMITED` (-1) means no limit.
    max_size: i32,
    /// The requested buffer size, or -1 if not yet known.
    requested_buffer_size: i32,
    /// The payload buffer, allocated once the size is known.
    /// `None` if not yet allocated.
    buffer: Option<Vec<u8>>,
    /// Number of bytes read into the payload buffer so far.
    buffer_bytes_read: usize,
}

impl NetworkReceive {
    /// Source identifier used when the source is unknown.
    pub const UNKNOWN_SOURCE: &str = "";

    /// Value indicating no maximum size limit for receives.
    pub const UNLIMITED: i32 = -1;

    /// Size of the header that precedes each message (4 bytes for the i32 size).
    const SIZE_LENGTH: usize = 4;

    /// Creates a new `NetworkReceive` with the given source and a pre-existing payload buffer.
    ///
    /// The size header is considered already read and the payload buffer is provided directly.
    /// This constructor is used when the buffer contents are already known (e.g., in tests).
    pub fn with_source_buffer(source: &str, buffer: Vec<u8>) -> Self {
        // When a buffer is provided, we treat the size header as fully read
        // and set the payload position to the buffer's capacity (matching Java behavior
        // where buffer.remaining() == 0 for a fully-positioned buffer).
        let buffer_len = buffer.len();
        Self {
            source: source.to_string(),
            size_buf: [0; NetworkReceive::SIZE_LENGTH],
            size_bytes_read: NetworkReceive::SIZE_LENGTH,
            max_size: NetworkReceive::UNLIMITED,
            requested_buffer_size: buffer_len as i32,
            buffer: Some(buffer),
            buffer_bytes_read: buffer_len,
        }
    }

    /// Creates a new `NetworkReceive` with the given source and no size limit.
    pub fn with_source(source: &str) -> Self {
        Self::with_max_size_source(NetworkReceive::UNLIMITED, source)
    }

    /// Creates a new `NetworkReceive` with the given maximum size and source.
    pub fn with_max_size_source(max_size: i32, source: &str) -> Self {
        Self {
            source: source.to_string(),
            size_buf: [0; NetworkReceive::SIZE_LENGTH],
            size_bytes_read: 0,
            max_size,
            requested_buffer_size: -1,
            buffer: None,
            buffer_bytes_read: 0,
        }
    }

    /// Creates a new `NetworkReceive` with unknown source and no size limit.
    pub fn new() -> Self {
        Self::with_source(NetworkReceive::UNKNOWN_SOURCE)
    }

    /// Returns the payload buffer, or `None` if it has not been allocated yet.
    pub fn payload(&self) -> Option<&[u8]> {
        self.buffer.as_deref()
    }

    /// Consumes the receive, returning its source id and payload buffer by
    /// **move** — the payload `Vec<u8>` is taken out without copying (§27
    /// receive-path zero-copy, Phase 20 Fix #3). Used by
    /// `Selectable::drain_completed_receives` so the network client can parse
    /// the response straight from the moved buffer instead of `to_vec()`-ing
    /// it out of the selector.
    pub fn into_source_and_payload(self) -> (String, Option<Vec<u8>>) {
        (self.source, self.buffer)
    }

    /// Returns the number of bytes read so far (both size header and payload).
    pub fn bytes_read(&self) -> usize {
        if self.buffer.is_none() {
            self.size_bytes_read
        } else {
            self.buffer_bytes_read + self.size_bytes_read
        }
    }

    /// Returns the total size of the receive including payload and size buffer,
    /// for use in metrics. This is consistent with `NetworkSend::size()`.
    ///
    /// # Panics
    ///
    /// Panics if the payload buffer has not been allocated yet.
    pub fn size(&self) -> usize {
        self.buffer.as_ref().expect("payload buffer not yet allocated").len() + NetworkReceive::SIZE_LENGTH
    }

    /// Synchronous, non-blocking mirror of [`Receive::read_from`](Receive::read_from).
    ///
    /// Calls [`TransportLayer::try_read`](TransportLayer::try_read) instead of
    /// awaiting [`TransportLayer::read`](TransportLayer::read), avoiding the
    /// `Box::pin(async {…})` allocation on every call and (more importantly)
    /// the `tokio::time::timeout(Duration::ZERO, …)` wrapper that the selector
    /// previously used in the hot read loop. The size-header / allocate /
    /// payload state machine is identical to the async version.
    ///
    /// `WouldBlock` from the transport is converted to "no progress this call"
    /// (returns the bytes accumulated so far), matching how the async version
    /// behaves and matching Java NIO's non-blocking `channel.read()` returning 0.
    ///
    /// # Errors
    ///
    /// Returns `UnexpectedEof` if the transport returns `Ok(0)` mid-receive
    /// (remote closed). Other I/O errors are propagated.
    pub fn try_read_from(&mut self, channel: &mut dyn TransportLayer) -> io::Result<usize> {
        let mut total_read = 0;

        // Phase 1: Read the 4-byte size header
        if self.size_bytes_read < NetworkReceive::SIZE_LENGTH {
            match channel.try_read(&mut self.size_buf[self.size_bytes_read..NetworkReceive::SIZE_LENGTH]) {
                Ok(0) => {
                    return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "EOF during size header read"));
                },
                Ok(bytes_read) => {
                    total_read += bytes_read;
                    self.size_bytes_read += bytes_read;
                },
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    return Ok(total_read);
                },
                Err(e) => {
                    return Err(e);
                },
            }

            if self.size_bytes_read == NetworkReceive::SIZE_LENGTH {
                let receive_size = i32::from_be_bytes(self.size_buf);
                if receive_size < 0 {
                    return Err(InvalidReceiveError::new(format!("Invalid receive (size = {receive_size})")).into());
                }
                if self.max_size != NetworkReceive::UNLIMITED && receive_size > self.max_size {
                    return Err(InvalidReceiveError::new(format!(
                        "Invalid receive (size = {receive_size} larger than {max_size})",
                        max_size = self.max_size,
                    ))
                    .into());
                }
                self.requested_buffer_size = receive_size;
                if receive_size == 0 {
                    self.buffer = Some(Vec::new());
                }
            }
        }

        // Phase 2: Allocate buffer if size is known but not yet allocated.
        //
        // `with_capacity`, NOT `vec![0u8; n]` (Phase 28): the payload is
        // appended via `try_read_append`, which fills the spare capacity
        // without requiring it to be pre-initialized — zeroing every
        // received byte (the full fetch throughput) was pure waste because
        // the socket bytes immediately overwrite it. (Java zeroes its
        // `ByteBuffer.allocate` too, but in TLAB; glibc memset was the Rust
        // translation artifact.) `buffer_bytes_read` stays in sync with
        // `buf.len()` in this append model.
        if self.buffer.is_none() && self.requested_buffer_size != -1 {
            self.buffer = Some(Vec::with_capacity(self.requested_buffer_size as usize));
            self.buffer_bytes_read = 0;
            trace!(
                "Allocated buffer of size {} for source {}",
                self.requested_buffer_size, self.source
            );
        }

        // Phase 3: Read payload data.
        //
        // Drain ALL currently-available socket bytes in a tight loop rather
        // than one chunk per call — Java-NIO read pattern. Avoids the
        // per-chunk readiness/timer overhead that throttled large fetch reads
        // (design/current/consumer-throughput-bottleneck.md, UPDATE 4).
        if let Some(ref mut buf) = self.buffer {
            let requested = self.requested_buffer_size as usize;
            while buf.len() < requested {
                match channel.try_read_append(buf, requested - buf.len()) {
                    Ok(0) => {
                        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "EOF during payload read"));
                    },
                    Ok(bytes_read) => {
                        total_read += bytes_read;
                        self.buffer_bytes_read = buf.len();
                    },
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        return Ok(total_read); // socket drained for now
                    },
                    Err(e) => {
                        return Err(e);
                    },
                }
            }
        }

        Ok(total_read)
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
        // Compare against the requested size, not `buffer.len()`: the
        // append model (Phase 28, `try_read_append`) grows `buffer.len()`
        // as bytes arrive (so len == bytes_read at all times), while the
        // zero-initialized async-fallback model keeps len == requested
        // throughout. `buffer_bytes_read == requested` is the completion
        // condition in both.
        self.size_bytes_read == NetworkReceive::SIZE_LENGTH
            && self.buffer.is_some()
            && self.requested_buffer_size >= 0
            && self.buffer_bytes_read as i64 == self.requested_buffer_size as i64
    }

    fn read_from<'a>(
        &'a mut self,
        channel: &'a mut dyn TransportLayer,
    ) -> Pin<Box<dyn Future<Output = io::Result<usize>> + Send + 'a>> {
        Box::pin(async {
            let mut total_read = 0;

            // Phase 1: Read the 4-byte size header (non-blocking on transports
            // that support `try_read`, so the whole receive drains without
            // per-chunk async overhead).
            if self.size_bytes_read < NetworkReceive::SIZE_LENGTH {
                let header_result = if channel.supports_try_read() {
                    channel.try_read(&mut self.size_buf[self.size_bytes_read..NetworkReceive::SIZE_LENGTH])
                } else {
                    channel
                        .read(&mut self.size_buf[self.size_bytes_read..NetworkReceive::SIZE_LENGTH])
                        .await
                };
                match header_result {
                    Ok(0) => {
                        // Ok(0) means EOF (remote closed connection).
                        // Matches Java: bytesRead < 0 → EOFException.
                        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "EOF during size header read"));
                    },
                    Ok(bytes_read) => {
                        total_read += bytes_read;
                        self.size_bytes_read += bytes_read;
                    },
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        // No data available right now. Matches Java NIO
                        // non-blocking returning 0: try again later.
                        return Ok(total_read);
                    },
                    Err(e) => {
                        return Err(e);
                    },
                }

                if self.size_bytes_read == NetworkReceive::SIZE_LENGTH {
                    let receive_size = i32::from_be_bytes(self.size_buf);
                    if receive_size < 0 {
                        return Err(InvalidReceiveError::new(format!("Invalid receive (size = {receive_size})")).into());
                    }
                    if self.max_size != NetworkReceive::UNLIMITED && receive_size > self.max_size {
                        return Err(InvalidReceiveError::new(format!(
                            "Invalid receive (size = {receive_size} larger than {max_size})",
                            max_size = self.max_size,
                        ))
                        .into());
                    }
                    self.requested_buffer_size = receive_size;
                    if receive_size == 0 {
                        self.buffer = Some(Vec::new());
                    }
                }
            }

            // Phase 2: Allocate buffer if size is known but not yet allocated.
            //
            // Transports with `try_read` fill via the appending
            // `try_read_append` (Phase 28), so the buffer is allocated with
            // `with_capacity` and NOT zeroed (the memset of every received
            // byte was pure waste — see `try_read_from`). The async fallback
            // (mock transports) reads into `&mut [u8]` and keeps the
            // zero-initialized model.
            if self.buffer.is_none() && self.requested_buffer_size != -1 {
                let requested = self.requested_buffer_size as usize;
                self.buffer = Some(if channel.supports_try_read() {
                    Vec::with_capacity(requested)
                } else {
                    vec![0u8; requested]
                });
                self.buffer_bytes_read = 0;
                trace!(
                    "Allocated buffer of size {} for source {}",
                    self.requested_buffer_size, self.source
                );
            }

            // Phase 3: Read payload data.
            //
            // On transports with non-blocking `try_read` (plaintext / SSL),
            // drain ALL currently-available socket bytes in a tight loop
            // rather than one chunk per call — Java-NIO read pattern. Avoids
            // the per-chunk readiness/timer overhead that throttled large
            // fetch reads (design/current/consumer-throughput-bottleneck.md,
            // UPDATE 4).
            if let Some(ref mut buf) = self.buffer {
                if channel.supports_try_read() {
                    let requested = self.requested_buffer_size as usize;
                    while buf.len() < requested {
                        match channel.try_read_append(buf, requested - buf.len()) {
                            Ok(0) => {
                                // Ok(0) means EOF (remote closed). In Java,
                                // `bytesRead < 0` during the payload phase
                                // always throws `EOFException`, regardless of
                                // what was read earlier in the same call.
                                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "EOF during payload read"));
                            },
                            Ok(bytes_read) => {
                                total_read += bytes_read;
                                self.buffer_bytes_read = buf.len();
                            },
                            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                                return Ok(total_read); // socket drained for now
                            },
                            Err(e) => return Err(e),
                        }
                    }
                } else if self.buffer_bytes_read < buf.len() {
                    match channel.read(&mut buf[self.buffer_bytes_read..]).await {
                        Ok(0) => {
                            // Ok(0) means EOF (remote closed). In Java,
                            // `bytesRead < 0` during the payload phase always
                            // throws `EOFException`, regardless of what was
                            // read earlier in the same call.
                            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "EOF during payload read"));
                        },
                        Ok(bytes_read) => {
                            total_read += bytes_read;
                            self.buffer_bytes_read += bytes_read;
                        },
                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                            // No data available right now. Matches Java NIO
                            // non-blocking returning 0: return bytes read so far.
                            return Ok(total_read);
                        },
                        Err(e) => return Err(e),
                    }
                }
            }

            Ok(total_read)
        })
    }

    fn required_memory_amount_known(&self) -> bool {
        self.requested_buffer_size != -1
    }

    fn memory_allocated(&self) -> bool {
        self.buffer.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::network::InterestOps;

    use std::io;
    use std::net::SocketAddr;

    /// A mock transport layer backed by a byte buffer for testing.
    ///
    /// When `eof_on_exhaustion` is `true` (default), reads return `Ok(0)` once
    /// the buffer is exhausted — matching a closed TCP connection.
    ///
    /// When `eof_on_exhaustion` is `false`, reads return `WouldBlock` once
    /// the buffer is exhausted — matching a still-open non-blocking channel
    /// that has no more data right now. This models the Java NIO behavior
    /// where `ScatteringByteChannel.read()` returns 0 (not -1) on a
    /// non-blocking channel with no available data.
    struct MockTransportLayer {
        data: Vec<u8>,
        pos: usize,
        eof_on_exhaustion: bool,
    }

    impl MockTransportLayer {
        /// Creates a mock that returns `Ok(0)` (EOF) when the buffer is exhausted.
        fn new(data: Vec<u8>) -> Self {
            Self { data, pos: 0, eof_on_exhaustion: true }
        }

        /// Creates a mock that returns `WouldBlock` when the buffer is exhausted,
        /// simulating a still-open channel with no data available. This matches
        /// Java NIO non-blocking behavior where `channel.read()` returns 0.
        fn new_open(data: Vec<u8>) -> Self {
            Self { data, pos: 0, eof_on_exhaustion: false }
        }
    }

    impl TransportLayer for MockTransportLayer {
        fn peer_addr(&self) -> io::Result<SocketAddr> {
            Ok("127.0.0.1:9092".parse().unwrap())
        }

        fn ready(&self) -> bool {
            true
        }

        fn finish_connect(&mut self) -> Pin<Box<dyn Future<Output = io::Result<bool>> + Send + '_>> {
            Box::pin(async { Ok(true) })
        }

        fn disconnect(&mut self) {}

        fn is_connected(&self) -> bool {
            true
        }

        fn handshake(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
            Box::pin(async { Ok(()) })
        }

        fn add_interest_ops(&mut self, _ops: InterestOps) {}
        fn remove_interest_ops(&mut self, _ops: InterestOps) {}

        fn is_mute(&self) -> bool {
            false
        }

        fn has_bytes_buffered(&self) -> bool {
            false
        }

        fn has_pending_writes(&self) -> bool {
            false
        }

        fn is_open(&self) -> bool {
            true
        }

        fn close(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
            Box::pin(async { Ok(()) })
        }

        fn readable(&self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
            Box::pin(async { Ok(()) })
        }

        fn writable(&self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
            Box::pin(async { Ok(()) })
        }

        fn poll_readable(&self, _cx: &mut std::task::Context<'_>) -> std::task::Poll<io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn poll_writable(&self, _cx: &mut std::task::Context<'_>) -> std::task::Poll<io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn read<'a>(&'a mut self, dst: &'a mut [u8]) -> Pin<Box<dyn Future<Output = io::Result<usize>> + Send + 'a>> {
            let remaining = self.data.len() - self.pos;
            let to_read = remaining.min(dst.len());
            if to_read == 0 && !self.eof_on_exhaustion {
                // Simulate a still-open non-blocking channel with no data
                // available. In Java NIO, this returns 0 (not -1). In Rust
                // async, we return WouldBlock so that the caller knows the
                // connection is still alive but has no data right now.
                return Box::pin(async { Err(io::Error::from(io::ErrorKind::WouldBlock)) });
            }
            dst[..to_read].copy_from_slice(&self.data[self.pos..self.pos + to_read]);
            self.pos += to_read;
            Box::pin(async move { Ok(to_read) })
        }

        fn write<'a>(&'a mut self, _src: &'a [u8]) -> Pin<Box<dyn Future<Output = io::Result<usize>> + Send + 'a>> {
            Box::pin(async { Ok(0) })
        }

        fn write_vectored<'a>(
            &'a mut self,
            _srcs: &'a [io::IoSlice<'a>],
        ) -> Pin<Box<dyn Future<Output = io::Result<usize>> + Send + 'a>> {
            Box::pin(async { Ok(0) })
        }
    }

    /// A mock transport that supports the non-blocking [`try_read`](TransportLayer::try_read)
    /// path, returning at most `chunk` bytes per call so a single `read_from`
    /// exercises the tight drain loop across several `try_read` calls.
    ///
    /// When the buffer is exhausted it returns `Ok(0)` (EOF) if `eof_on_exhaustion`
    /// is `true`, else `WouldBlock` — matching a closed connection vs. a still-open
    /// non-blocking channel with no data right now.
    struct ChunkedTryReadMock {
        data: Vec<u8>,
        pos: usize,
        chunk: usize,
        eof_on_exhaustion: bool,
    }

    impl ChunkedTryReadMock {
        fn new(data: Vec<u8>, chunk: usize, eof_on_exhaustion: bool) -> Self {
            Self { data, pos: 0, chunk, eof_on_exhaustion }
        }

        fn read_chunk(&mut self, dst: &mut [u8]) -> io::Result<usize> {
            let remaining = self.data.len() - self.pos;
            if remaining == 0 {
                return if self.eof_on_exhaustion {
                    Ok(0) // EOF: remote closed the connection
                } else {
                    Err(io::Error::from(io::ErrorKind::WouldBlock)) // still open, no data now
                };
            }
            let to_read = remaining.min(self.chunk).min(dst.len());
            dst[..to_read].copy_from_slice(&self.data[self.pos..self.pos + to_read]);
            self.pos += to_read;
            Ok(to_read)
        }
    }

    impl TransportLayer for ChunkedTryReadMock {
        fn peer_addr(&self) -> io::Result<SocketAddr> {
            Ok("127.0.0.1:9092".parse().unwrap())
        }
        fn ready(&self) -> bool {
            true
        }
        fn finish_connect(&mut self) -> Pin<Box<dyn Future<Output = io::Result<bool>> + Send + '_>> {
            Box::pin(async { Ok(true) })
        }
        fn disconnect(&mut self) {}
        fn is_connected(&self) -> bool {
            true
        }
        fn handshake(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
            Box::pin(async { Ok(()) })
        }
        fn add_interest_ops(&mut self, _ops: InterestOps) {}
        fn remove_interest_ops(&mut self, _ops: InterestOps) {}
        fn is_mute(&self) -> bool {
            false
        }
        fn has_bytes_buffered(&self) -> bool {
            false
        }
        fn has_pending_writes(&self) -> bool {
            false
        }
        fn is_open(&self) -> bool {
            true
        }
        fn close(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
            Box::pin(async { Ok(()) })
        }
        fn readable(&self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
            Box::pin(async { Ok(()) })
        }
        fn writable(&self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
            Box::pin(async { Ok(()) })
        }
        fn poll_readable(&self, _cx: &mut std::task::Context<'_>) -> std::task::Poll<io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
        fn poll_writable(&self, _cx: &mut std::task::Context<'_>) -> std::task::Poll<io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
        // Required by the trait; the `supports_try_read` path means `read_from`
        // uses `try_read` instead, but provide a consistent single-chunk impl.
        fn read<'a>(&'a mut self, dst: &'a mut [u8]) -> Pin<Box<dyn Future<Output = io::Result<usize>> + Send + 'a>> {
            let r = self.read_chunk(dst);
            Box::pin(async move { r })
        }
        fn try_read(&mut self, dst: &mut [u8]) -> io::Result<usize> {
            self.read_chunk(dst)
        }
        fn supports_try_read(&self) -> bool {
            true
        }
        fn write<'a>(&'a mut self, _src: &'a [u8]) -> Pin<Box<dyn Future<Output = io::Result<usize>> + Send + 'a>> {
            Box::pin(async { Ok(0) })
        }
        fn write_vectored<'a>(
            &'a mut self,
            _srcs: &'a [io::IoSlice<'a>],
        ) -> Pin<Box<dyn Future<Output = io::Result<usize>> + Send + 'a>> {
            Box::pin(async { Ok(0) })
        }
    }

    /// The `try_read` tight-drain path reads a header + multi-chunk payload to
    /// completion in a single `read_from` call (Java-NIO `pollSelectionKeys`
    /// drain pattern), without fragmenting across calls.
    #[tokio::test]
    async fn test_try_read_drains_multi_chunk_payload_in_one_call() {
        let payload: Vec<u8> = (0..64u8).collect();
        let mut wire = 64_i32.to_be_bytes().to_vec();
        wire.extend_from_slice(&payload);

        // chunk=16 forces the 64-byte payload to drain over 4 `try_read` calls
        // inside one `read_from`. Header (4B) fits in the first chunk.
        let mut channel = ChunkedTryReadMock::new(wire, 16, false);
        let mut receive = NetworkReceive::with_max_size_source(128, "0");

        let read = receive.read_from(&mut channel).await.unwrap();
        assert_eq!(4 + 64, read, "header + full payload drained in one read_from");
        assert!(receive.complete(), "receive must complete in a single drain");
        assert_eq!(Some(&payload[..]), receive.payload());
    }

    /// `WouldBlock` mid-payload returns the partial bytes read so far (receive not
    /// complete) and is resumed by a subsequent `read_from` — no data lost, no EOF.
    #[tokio::test]
    async fn test_try_read_wouldblock_mid_payload_returns_partial() {
        // Only header + 40 of 64 payload bytes available now; still-open channel.
        let payload_part: Vec<u8> = (0..40u8).collect();
        let mut wire = 64_i32.to_be_bytes().to_vec();
        wire.extend_from_slice(&payload_part);

        let mut channel = ChunkedTryReadMock::new(wire, 16, false);
        let mut receive = NetworkReceive::with_max_size_source(128, "0");

        let read = receive.read_from(&mut channel).await.unwrap();
        assert_eq!(4 + 40, read, "header + available payload");
        assert!(!receive.complete(), "receive not complete on WouldBlock mid-payload");

        // Remaining 24 bytes arrive; receive completes on the next call.
        let rest: Vec<u8> = (40..64u8).collect();
        let mut channel2 = ChunkedTryReadMock::new(rest, 16, false);
        let read2 = receive.read_from(&mut channel2).await.unwrap();
        assert_eq!(24, read2);
        assert!(receive.complete(), "receive completes once the payload tail arrives");
    }

    /// `Ok(0)` mid-payload means EOF (remote closed) → `UnexpectedEof`, matching
    /// Java's `bytesRead < 0` → `EOFException` during a payload read.
    #[tokio::test]
    async fn test_try_read_eof_mid_payload_errors() {
        // Header says 64 bytes but only 40 are sent, then the connection closes.
        let payload_part: Vec<u8> = (0..40u8).collect();
        let mut wire = 64_i32.to_be_bytes().to_vec();
        wire.extend_from_slice(&payload_part);

        let mut channel = ChunkedTryReadMock::new(wire, 16, true);
        let mut receive = NetworkReceive::with_max_size_source(128, "0");

        let result = receive.read_from(&mut channel).await;
        let err = result.expect_err("EOF mid-payload must error");
        assert_eq!(io::ErrorKind::UnexpectedEof, err.kind());
    }

    /// Translated from `NetworkReceiveTest.testBytesRead` in
    /// `org.apache.kafka.common.network.NetworkReceiveTest`.
    #[tokio::test]
    async fn test_bytes_read() {
        let mut receive = NetworkReceive::with_max_size_source(128, "0");
        assert_eq!(0, receive.bytes_read());

        // Simulate channel that returns a 4-byte size header indicating 128 bytes of payload.
        // Uses new_open because the connection is still alive — the channel just has no
        // more data for this call. Matches Java NIO mock returning 0 (not -1) on the
        // subsequent payload read.
        let mut channel = MockTransportLayer::new_open(128_i32.to_be_bytes().to_vec());

        let read = receive.read_from(&mut channel).await.unwrap();
        assert_eq!(4, read);
        assert_eq!(4, receive.bytes_read());
        assert!(!receive.complete());

        // Simulate reading 64 bytes of payload
        let mut channel1 = MockTransportLayer::new(vec![0xABu8; 64]);

        let read = receive.read_from(&mut channel1).await.unwrap();
        assert_eq!(64, read);
        assert_eq!(68, receive.bytes_read());
        assert!(!receive.complete());

        // Simulate reading the remaining 64 bytes of payload
        let mut channel2 = MockTransportLayer::new(vec![0xCDu8; 64]);

        let read = receive.read_from(&mut channel2).await.unwrap();
        assert_eq!(64, read);
        assert_eq!(132, receive.bytes_read());
        assert!(receive.complete());
    }

    /// Translated from `NetworkReceiveTest.testRequiredMemoryAmountKnownWhenNotSet` in
    /// `org.apache.kafka.common.network.NetworkReceiveTest`.
    #[test]
    fn test_required_memory_amount_known_when_not_set() {
        let receive = NetworkReceive::with_source("0");
        assert!(
            !receive.required_memory_amount_known(),
            "Memory amount should not be known before read."
        );
    }

    /// Translated from `NetworkReceiveTest.testRequiredMemoryAmountKnownWhenSet` in
    /// `org.apache.kafka.common.network.NetworkReceiveTest`.
    #[tokio::test]
    async fn test_required_memory_amount_known_when_set() {
        let mut receive = NetworkReceive::with_max_size_source(128, "0");

        // Channel provides size header indicating 64 bytes. Uses new_open because the
        // connection is still alive — the Java mock returns 0 (not -1) on the
        // subsequent payload read.
        let mut channel = MockTransportLayer::new_open(64_i32.to_be_bytes().to_vec());

        receive.read_from(&mut channel).await.unwrap();
        assert!(
            receive.required_memory_amount_known(),
            "Memory amount should be known after read."
        );
    }

    /// Translated from `NetworkReceiveTest.testSizeWithPredefineBuffer` in
    /// `org.apache.kafka.common.network.NetworkReceiveTest`.
    #[test]
    fn test_size_with_predefined_buffer() {
        let payload_size = 8;
        let expected_total_size = 4 + payload_size; // 4 bytes for size buffer + payload size

        // Create a payload buffer with sequential byte values
        let payload_buffer: Vec<u8> = (0..payload_size as u8).collect();

        let network_receive = NetworkReceive::with_source_buffer("0", payload_buffer);
        assert_eq!(
            expected_total_size,
            network_receive.size(),
            "The total size should be the sum of the size buffer and payload."
        );
    }

    /// Translated from `NetworkReceiveTest.testSizeAfterRead` in
    /// `org.apache.kafka.common.network.NetworkReceiveTest`.
    #[tokio::test]
    async fn test_size_after_read() {
        let payload_size: i32 = 32;
        let expected_total_size = 4 + payload_size as usize; // 4 bytes for size buffer + payload size
        let mut receive = NetworkReceive::with_max_size_source(128, "0");

        // Channel provides size header. Uses new_open because the connection is still
        // alive — the Java mock returns 0 (not -1) on the subsequent payload read.
        let mut channel = MockTransportLayer::new_open(payload_size.to_be_bytes().to_vec());

        receive.read_from(&mut channel).await.unwrap();
        assert_eq!(
            expected_total_size,
            receive.size(),
            "The total size should be the sum of the size buffer and receive size."
        );
    }

    /// Test that negative size in header is rejected.
    #[tokio::test]
    async fn test_invalid_negative_size() {
        let mut receive = NetworkReceive::with_max_size_source(128, "0");
        let mut channel = MockTransportLayer::new((-1_i32).to_be_bytes().to_vec());

        let result = receive.read_from(&mut channel).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(io::ErrorKind::InvalidData, err.kind());
    }

    /// Test that size exceeding max is rejected.
    #[tokio::test]
    async fn test_invalid_size_exceeding_max() {
        let mut receive = NetworkReceive::with_max_size_source(64, "0");
        let mut channel = MockTransportLayer::new(128_i32.to_be_bytes().to_vec());

        let result = receive.read_from(&mut channel).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(io::ErrorKind::InvalidData, err.kind());
    }

    /// Test zero-size payload (used by SASL).
    #[tokio::test]
    async fn test_zero_size_payload() {
        let mut receive = NetworkReceive::with_max_size_source(128, "0");
        let mut channel = MockTransportLayer::new(0_i32.to_be_bytes().to_vec());

        let read = receive.read_from(&mut channel).await.unwrap();
        assert_eq!(4, read);
        assert!(receive.complete());
        assert_eq!(Some(&[][..]), receive.payload());
    }

    /// Test default constructor.
    #[test]
    fn test_default() {
        let receive = NetworkReceive::new();
        assert_eq!(NetworkReceive::UNKNOWN_SOURCE, receive.source());
        assert!(!receive.complete());
        assert!(!receive.required_memory_amount_known());
        assert!(!receive.memory_allocated());
    }

    /// Test that EOF during size header read is detected.
    ///
    /// Matches Java behavior: `NetworkReceive.readFrom()` throws `EOFException`
    /// when `channel.read(size)` returns -1 (which in Rust is `Ok(0)`).
    #[tokio::test]
    async fn test_eof_during_size_read() {
        let mut receive = NetworkReceive::with_max_size_source(128, "0");
        // Empty channel simulates a closed connection
        let mut channel = MockTransportLayer::new(Vec::new());

        let result = receive.read_from(&mut channel).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(io::ErrorKind::UnexpectedEof, err.kind());
    }

    /// Test that EOF during payload read is detected (separate calls).
    ///
    /// Matches Java behavior: `NetworkReceive.readFrom()` throws `EOFException`
    /// when `channel.read(buffer)` returns -1 during the payload phase.
    #[tokio::test]
    async fn test_eof_during_payload_read() {
        let mut receive = NetworkReceive::with_max_size_source(128, "0");

        // First, provide the size header indicating 64 bytes of payload.
        // Uses new_open because the connection is still alive during header read.
        let mut size_channel = MockTransportLayer::new_open(64_i32.to_be_bytes().to_vec());
        receive.read_from(&mut size_channel).await.unwrap();

        // Then, provide an empty channel (EOF) when payload is expected
        let mut eof_channel = MockTransportLayer::new(Vec::new());
        let result = receive.read_from(&mut eof_channel).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(io::ErrorKind::UnexpectedEof, err.kind());
    }

    /// Test that EOF during payload read is detected even when the size header
    /// was read in the same call.
    ///
    /// Verifies that `read_from` returns `Err(UnexpectedEof)` when the channel
    /// provides exactly the 4-byte size header but then returns EOF for the
    /// payload — matching Java's `NetworkReceive.readFrom()` which always throws
    /// `EOFException` on `bytesRead < 0` during the payload phase, regardless
    /// of whether the size header was read in the same invocation.
    #[tokio::test]
    async fn test_eof_during_payload_read_same_call_as_header() {
        let mut receive = NetworkReceive::with_max_size_source(128, "0");

        // Channel provides only the 4-byte size header (indicating 64 bytes of
        // payload), then EOF. Both phases happen in the same read_from call.
        let mut channel = MockTransportLayer::new(64_i32.to_be_bytes().to_vec());

        let result = receive.read_from(&mut channel).await;
        assert!(
            result.is_err(),
            "EOF during payload phase must always be an error, even when size header was read in the same call"
        );
        let err = result.unwrap_err();
        assert_eq!(io::ErrorKind::UnexpectedEof, err.kind());
    }

    /// Phase 20 Fix #3: `into_source_and_payload` moves the source id and
    /// payload buffer out of the receive without copying the bytes.
    #[test]
    fn test_into_source_and_payload_moves_buffer() {
        let payload: Vec<u8> = (0..16u8).collect();
        let receive = NetworkReceive::with_source_buffer("node-3", payload.clone());
        let (source, buffer) = receive.into_source_and_payload();
        assert_eq!("node-3", source);
        assert_eq!(Some(payload), buffer);
    }

    /// An unallocated receive yields its source and `None` payload.
    #[test]
    fn test_into_source_and_payload_no_buffer() {
        let receive = NetworkReceive::with_source("node-9");
        let (source, buffer) = receive.into_source_and_payload();
        assert_eq!("node-9", source);
        assert!(buffer.is_none());
    }
}
