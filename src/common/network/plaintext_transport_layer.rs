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
//! In Rust, this wraps a `std::net::TcpStream` set to non-blocking mode.
//! For async usage, the caller is expected to integrate with a reactor/selector
//! (e.g., `mio` or `tokio`) that drives readiness notifications.

use super::transport_layer::{InterestOps, TransportLayer};

use std::io;
use std::io::{Read, Write};
use std::net::TcpStream;

/// Transport layer for PLAINTEXT (unencrypted) communication.
///
/// This is a wrapper around a [`TcpStream`] with interest ops tracking.
/// It provides no encryption — data is sent and received as plaintext.
///
/// The stream should be set to non-blocking mode. The caller is responsible
/// for driving I/O readiness via a selector/reactor pattern.
pub struct PlaintextTransportLayer {
    /// The underlying TCP stream.
    stream: TcpStream,
    /// Whether the connection has been established.
    connected: bool,
    /// Current interest operations for selector registration.
    interest_ops: InterestOps,
}

impl PlaintextTransportLayer {
    /// Creates a new `PlaintextTransportLayer` wrapping the given TCP stream.
    ///
    /// The stream is set to non-blocking mode. The initial interest ops are set to
    /// `OP_CONNECT` to indicate that the connection is being established.
    ///
    /// # Errors
    ///
    /// Returns an error if the stream cannot be set to non-blocking mode.
    pub fn new(stream: TcpStream) -> io::Result<Self> {
        stream.set_nonblocking(true)?;
        Ok(Self { stream, connected: false, interest_ops: InterestOps::OP_CONNECT })
    }

    /// Creates a new `PlaintextTransportLayer` from an already-connected TCP stream.
    ///
    /// The stream is set to non-blocking mode. The initial interest ops are set to
    /// `OP_READ` since the connection is already established.
    ///
    /// # Errors
    ///
    /// Returns an error if the stream cannot be set to non-blocking mode.
    pub fn connected(stream: TcpStream) -> io::Result<Self> {
        stream.set_nonblocking(true)?;
        Ok(Self { stream, connected: true, interest_ops: InterestOps::OP_READ })
    }
}

impl TransportLayer for PlaintextTransportLayer {
    /// Always returns `true` for plaintext — no handshake/authentication required.
    fn ready(&self) -> bool {
        true
    }

    /// Finishes the process of connecting a socket channel.
    ///
    /// On success, updates the interest ops from `OP_CONNECT` to `OP_READ`.
    fn finish_connect(&mut self) -> io::Result<bool> {
        // For non-blocking TCP, we check if the connection is established
        // by attempting to get the peer address
        match self.stream.peer_addr() {
            Ok(_) => {
                self.connected = true;
                self.interest_ops = self.interest_ops.remove(InterestOps::OP_CONNECT) | InterestOps::OP_READ;
                Ok(true)
            },
            Err(e) if e.kind() == io::ErrorKind::NotConnected => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Disconnects the underlying socket channel.
    fn disconnect(&mut self) {
        self.connected = false;
        // Shutdown the stream to signal disconnect; ignore errors
        let _ = self.stream.shutdown(std::net::Shutdown::Both);
    }

    /// Returns `true` if this channel's network socket is connected.
    fn is_connected(&self) -> bool {
        self.connected
    }

    /// Performs SSL handshake — this is a no-op for the non-secure PLAINTEXT implementation.
    fn handshake(&mut self) -> io::Result<()> {
        Ok(())
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
    /// We check by attempting to get the local address; if the socket is closed
    /// this will fail.
    fn is_open(&self) -> bool {
        self.stream.local_addr().is_ok()
    }

    /// Closes the transport layer.
    fn close(&mut self) -> io::Result<()> {
        self.stream.shutdown(std::net::Shutdown::Both)?;
        self.connected = false;
        Ok(())
    }

    /// Reads data from this channel into the given buffer.
    fn read(&mut self, dst: &mut [u8]) -> io::Result<usize> {
        self.stream.read(dst)
    }

    /// Writes data to this channel from the given buffer.
    fn write(&mut self, src: &[u8]) -> io::Result<usize> {
        self.stream.write(src)
    }

    /// Writes data from multiple buffers to this channel (scatter-gather write).
    fn write_vectored(&mut self, srcs: &[io::IoSlice<'_>]) -> io::Result<usize> {
        self.stream.write_vectored(srcs)
    }
}
