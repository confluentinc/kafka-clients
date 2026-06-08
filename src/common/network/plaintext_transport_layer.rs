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

//! Transport layer for PLAINTEXT communication.
//!
//! Translated from `org.apache.kafka.common.network.PlaintextTransportLayer`.
//!
//! In Java, this wraps a `SocketChannel` obtained from a `SelectionKey`.
//! In Rust, this wraps a `tokio::net::TcpStream` for async non-blocking I/O,
//! per CLAUDE.md rule 8.

use super::{InterestOps, TransportLayer};

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;

use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

/// Transport layer for PLAINTEXT (unencrypted) communication.
///
/// This is a wrapper around a Tokio [`TcpStream`] with interest ops tracking.
/// It provides no encryption — data is sent and received as plaintext.
///
/// The stream is wrapped in `Option` so that `close()` can drop it (via `take()`),
/// releasing the OS socket resource. This matches Java's `socketChannel.close()`
/// semantics where the channel is actually closed and `isOpen()` returns `false`.
///
/// All I/O operations are async and driven by the Tokio runtime.
pub struct PlaintextTransportLayer {
    /// The underlying async TCP stream, or `None` if the transport has been closed.
    stream: Option<TcpStream>,
    /// Whether the connection has been established.
    connected: bool,
    /// Current interest operations for selector registration.
    interest_ops: InterestOps,
}

impl PlaintextTransportLayer {
    /// Creates a new `PlaintextTransportLayer` wrapping the given Tokio TCP stream.
    ///
    /// The initial interest ops are set to `OP_CONNECT` to indicate that the
    /// connection is being established.
    pub fn new(stream: TcpStream) -> Self {
        Self { stream: Some(stream), connected: false, interest_ops: InterestOps::OP_CONNECT }
    }

    /// Creates a new `PlaintextTransportLayer` from an already-connected Tokio TCP stream.
    ///
    /// The initial interest ops are set to `OP_READ` since the connection is
    /// already established.
    pub fn connected(stream: TcpStream) -> Self {
        Self { stream: Some(stream), connected: true, interest_ops: InterestOps::OP_READ }
    }

    /// Returns a mutable reference to the underlying stream, or an error if closed.
    ///
    /// Returns `ErrorKind::NotConnected` after `close()` has been called,
    /// matching Java's `ClosedChannelException` on I/O after `socketChannel.close()`.
    fn stream_mut(&mut self) -> io::Result<&mut TcpStream> {
        self.stream
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "transport layer is closed"))
    }
}

impl TransportLayer for PlaintextTransportLayer {
    /// Returns the remote address of the connected peer.
    fn peer_addr(&self) -> io::Result<SocketAddr> {
        match &self.stream {
            Some(stream) => stream.peer_addr(),
            None => Err(io::Error::new(io::ErrorKind::NotConnected, "transport layer is closed")),
        }
    }

    /// Always returns `true` for plaintext — no handshake/authentication required.
    fn ready(&self) -> bool {
        true
    }

    /// Finishes the process of connecting a socket channel.
    ///
    /// For a Tokio `TcpStream`, the connection is established when the stream
    /// becomes writable. On success, updates the interest ops from `OP_CONNECT`
    /// to `OP_READ`.
    fn finish_connect(&mut self) -> Pin<Box<dyn Future<Output = io::Result<bool>> + Send + '_>> {
        Box::pin(async {
            let stream = self.stream_mut()?;

            // For tokio TcpStream, wait for the stream to be writable which
            // indicates the connection has completed.
            stream.writable().await?;

            // Check if the connection succeeded by checking for socket errors
            match stream.peer_addr() {
                Ok(_) => {
                    self.connected = true;
                    self.interest_ops = self.interest_ops.remove(InterestOps::OP_CONNECT) | InterestOps::OP_READ;
                    Ok(true)
                },
                Err(e) if e.kind() == io::ErrorKind::NotConnected => Ok(false),
                Err(e) => Err(e),
            }
        })
    }

    /// Disconnects the underlying socket channel.
    fn disconnect(&mut self) {
        self.connected = false;
    }

    /// Returns `true` if this channel's network socket is connected.
    fn is_connected(&self) -> bool {
        self.connected
    }

    /// Performs SSL handshake — this is a no-op for the non-secure PLAINTEXT implementation.
    fn handshake(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }

    /// Adds the given interest operations.
    fn add_interest_ops(&mut self, ops: InterestOps) {
        self.interest_ops |= ops;
    }

    /// Removes the given interest operations.
    fn remove_interest_ops(&mut self, ops: InterestOps) {
        self.interest_ops = self.interest_ops.remove(ops);
    }

    /// Returns `true` if this channel is muted (not interested in read operations).
    fn is_mute(&self) -> bool {
        !self.interest_ops.contains(InterestOps::OP_READ)
    }

    /// Always returns `false` — there are no intermediate buffers for plaintext.
    fn has_bytes_buffered(&self) -> bool {
        false
    }

    /// Always returns `false` — plaintext writes go directly to the socket channel.
    fn has_pending_writes(&self) -> bool {
        false
    }

    /// Returns `true` if the underlying stream is open.
    ///
    /// After `close()` has been called, the stream is dropped and this returns `false`.
    /// This matches Java's `socketChannel.isOpen()` which returns `false` after
    /// `socketChannel.close()`.
    fn is_open(&self) -> bool {
        self.stream.is_some()
    }

    /// Closes the transport layer by dropping the underlying TCP stream.
    ///
    /// This releases the OS socket resource, matching Java's `socketChannel.close()`
    /// semantics. After this call, `is_open()` returns `false` and any subsequent
    /// I/O operations return an error with `ErrorKind::NotConnected`.
    fn close(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async {
            if let Some(mut stream) = self.stream.take() {
                // Shut down the write half gracefully before dropping.
                // Ignore shutdown errors — the important thing is that the stream
                // is dropped and the socket resource is released.
                let _ = stream.shutdown().await;
            }
            self.connected = false;
            Ok(())
        })
    }

    fn readable(&self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async {
            match &self.stream {
                Some(stream) => stream.readable().await,
                None => Err(io::Error::new(io::ErrorKind::NotConnected, "transport layer is closed")),
            }
        })
    }

    fn writable(&self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async {
            match &self.stream {
                Some(stream) => stream.writable().await,
                None => Err(io::Error::new(io::ErrorKind::NotConnected, "transport layer is closed")),
            }
        })
    }

    /// Reads data from this channel into the given buffer.
    ///
    /// Waits for the socket to become readable, then reads available data.
    /// Matches Java NIO's `SocketChannel.read()` on a non-blocking channel:
    /// the caller is expected to register read interest and wait for readiness
    /// before calling read. Returns `WouldBlock` if data is not yet available
    /// after a spurious readiness notification.
    fn read<'a>(&'a mut self, dst: &'a mut [u8]) -> Pin<Box<dyn Future<Output = io::Result<usize>> + Send + 'a>> {
        Box::pin(async {
            let stream = self.stream_mut()?;
            stream.readable().await?;
            stream.try_read(dst)
        })
    }

    fn try_read(&mut self, dst: &mut [u8]) -> io::Result<usize> {
        self.stream_mut()?.try_read(dst)
    }

    fn supports_try_read(&self) -> bool {
        true
    }

    /// Writes data to this channel from the given buffer.
    ///
    /// Waits for the socket to become writable, then writes data.
    fn write<'a>(&'a mut self, src: &'a [u8]) -> Pin<Box<dyn Future<Output = io::Result<usize>> + Send + 'a>> {
        Box::pin(async {
            let stream = self.stream_mut()?;
            stream.writable().await?;
            stream.try_write(src)
        })
    }

    /// Writes data from multiple buffers to this channel (scatter-gather write).
    fn write_vectored<'a>(
        &'a mut self,
        srcs: &'a [io::IoSlice<'a>],
    ) -> Pin<Box<dyn Future<Output = io::Result<usize>> + Send + 'a>> {
        Box::pin(async {
            let stream = self.stream_mut()?;
            stream.writable().await?;
            stream.try_write_vectored(srcs)
        })
    }

    fn try_write_vectored(&mut self, srcs: &[io::IoSlice<'_>]) -> io::Result<usize> {
        let stream = self.stream_mut()?;
        stream.try_write_vectored(srcs)
    }
}
