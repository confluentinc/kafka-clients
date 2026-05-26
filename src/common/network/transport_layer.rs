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

//! Translation of `org.apache.kafka.common.network.TransportLayer`.

use std::io;
use std::net::SocketAddr;
use std::task::{Context, Poll};

use crate::common::network::TransferableChannel;
use crate::common::security::auth::KafkaPrincipal;

/// Selection-key interest-op flags. Mirror the same bit values as Java's
/// `java.nio.channels.SelectionKey` so existing code paths that bitwise-OR
/// these constants behave identically.
///
/// Phase 5b/5c uses these to track which I/O readiness events the
/// `KafkaChannel`/`Selector` layer is interested in for a given
/// transport. Tokio drives the readiness on its own, but the Java
/// `Selector` and `KafkaChannel` toggle these to mute/unmute the channel,
/// so we keep them as first-class state on the transport.
pub const OP_READ: i32 = 1 << 0;
pub const OP_WRITE: i32 = 1 << 2;
pub const OP_CONNECT: i32 = 1 << 3;

/// Transport layer for underlying communication.
///
/// At a very basic level, this is a wrapper around the underlying socket
/// (a `tokio::net::TcpStream` for the Rust translation) that can be used
/// as a substitute for the socket and other network channel
/// implementations. As `NetworkClient` replaces `BlockingChannel` and
/// other implementations, `KafkaChannel` is the network I/O channel
/// layered on top of a `TransportLayer`.
///
/// The Java interface extends `ScatteringByteChannel` and
/// `TransferableChannel` (which itself extends `GatheringByteChannel`).
/// The Rust trait keeps the same shape:
///
/// * `TransferableChannel::write_vectored` mirrors the gathering write
///   (`GatheringByteChannel.write(ByteBuffer[])`).
/// * [`TransportLayer::read`] mirrors the scattering read into a single
///   buffer (`ReadableByteChannel.read(ByteBuffer)`).
///
/// `read` is sync and treats `WouldBlock` as `Ok(0)` (Java NIO's "would
/// block" signal) so that it composes with [`io::Read`] adapters and
/// with [`crate::common::network::Receive::read_from`], which expects
/// `Ok(0)` to mean "no progress on this call". End-of-stream (peer
/// closed the socket) is surfaced as `Err(io::ErrorKind::UnexpectedEof)`,
/// mirroring Java's `EOFException` thrown by `NetworkReceive.readFrom`
/// when `channel.read()` returns `-1`. The upper layer (`KafkaChannel`,
/// `Selector`) translates that error into a channel-disconnected event.
///
/// **Java SocketChannel/SelectionKey accessors are intentionally absent
/// from this trait.** Java's `socketChannel()` / `selectionKey()` getters
/// are tied to the NIO event loop; in the Tokio-based translation the
/// readiness mechanism is the runtime itself, and the `Selector` reaches
/// state via [`Self::add_interest_ops`] / [`Self::remove_interest_ops`]
/// / [`Self::is_mute`] rather than fishing it out of a `SelectionKey`.
pub trait TransportLayer: TransferableChannel {
    /// Returns true if the channel has handshake and authentication done.
    /// Mirrors `TransportLayer.ready()`. Plaintext is always `true`; SSL
    /// returns `true` only after the handshake completes.
    fn ready(&self) -> bool;

    /// Finishes the process of connecting a socket channel. Mirrors
    /// `TransportLayer.finishConnect()`. Returns `true` when the underlying
    /// connection is fully established; flips the interest-op set
    /// (`OP_CONNECT` cleared, `OP_READ` set) to mirror the Java
    /// behaviour.
    fn finish_connect(&mut self) -> io::Result<bool>;

    /// Disconnect the underlying socket. Mirrors
    /// `TransportLayer.disconnect()`. In Java this calls
    /// `selectionKey.cancel()`; in Rust this drops/closes the socket on
    /// the next `close()` call but is otherwise a hint-only operation.
    fn disconnect(&mut self);

    /// Tells whether this channel's network socket is connected.
    /// Mirrors `TransportLayer.isConnected()`.
    fn is_connected(&self) -> bool;

    /// Tells whether the channel is open. Mirrors `Channel.isOpen()`.
    fn is_open(&self) -> bool;

    /// Close the underlying socket. Mirrors `Closeable.close()`.
    fn close(&mut self) -> io::Result<()>;

    /// Read a sequence of bytes from this channel into the given buffer.
    /// Mirrors `ReadableByteChannel.read(ByteBuffer)`. Returns the number
    /// of bytes read; `Ok(0)` means the underlying socket has no bytes
    /// ready (Java NIO's "would block" signal). End-of-stream (peer
    /// closed) is surfaced as `Err(io::ErrorKind::UnexpectedEof)` to
    /// mirror Java's `channel.read() == -1 → throw EOFException`.
    fn read(&mut self, dst: &mut [u8]) -> io::Result<usize>;

    /// Performs SSL handshake. No-op for the PLAINTEXT implementation.
    /// Mirrors `TransportLayer.handshake()`. Returns
    /// `Err(KafkaError::Authentication)` when the SSL handshake fails.
    fn handshake(&mut self) -> io::Result<()>;

    /// Returns the peer principal — `KafkaPrincipal::ANONYMOUS` for
    /// PLAINTEXT, the SSL session's `getPeerPrincipal()` (or ANONYMOUS
    /// if peer auth was not requested) for SSL. Mirrors
    /// `TransportLayer.peerPrincipal()`.
    fn peer_principal(&self) -> io::Result<KafkaPrincipal>;

    /// Add the given interest-op flags. Bits should be a bitwise-OR of the
    /// `OP_*` constants in this module.
    fn add_interest_ops(&mut self, ops: i32);

    /// Remove the given interest-op flags.
    fn remove_interest_ops(&mut self, ops: i32);

    /// Read the current interest-op flag set. Returns 0 if the channel
    /// has been closed/cancelled.
    fn interest_ops(&self) -> i32;

    /// `true` iff `OP_READ` is *not* in the interest-op set (and the
    /// channel is still open). Mirrors Java's
    /// `selectionKey.isValid() && (interestOps() & OP_READ) == 0`.
    ///
    /// **Connect-pending caveat**: a freshly-`pending_connect`-constructed
    /// channel reports `is_mute() == true` because its initial interest-op
    /// set is `OP_CONNECT` only — `OP_READ` is added by `finish_connect`.
    /// Callers that need to distinguish "actively muted by the upper
    /// layer" from "not yet eligible to read because the connect has not
    /// completed" should pair this with [`Self::is_connected`] /
    /// [`Self::ready`] rather than treating `is_mute() == true` as a
    /// monolithic signal.
    fn is_mute(&self) -> bool;

    /// `true` iff this transport has bytes buffered internally that may be
    /// processed without reading additional data from the network. SSL
    /// implementations override this to signal that the
    /// decrypted-but-not-yet-consumed data is available; the plaintext
    /// implementation always returns `false`.
    fn has_bytes_buffered(&self) -> bool;

    /// Local address of the underlying socket. Mirrors Java's
    /// `transportLayer.socketChannel().socket().getLocalSocketAddress()`,
    /// which `KafkaChannel` reads for connection-introspection metadata
    /// (selector logging, idle-expiry, channel id computation).
    fn local_addr(&self) -> io::Result<SocketAddr>;

    /// Peer (remote) address of the underlying socket. Mirrors Java's
    /// `transportLayer.socketChannel().socket().getRemoteSocketAddress()`.
    fn peer_addr(&self) -> io::Result<SocketAddr>;

    /// Poll the underlying socket for readability. Used by
    /// [`crate::common::network::Selector::poll`] to wake from its
    /// timeout sleep when bytes arrive on any open channel — the Tokio
    /// equivalent of Java's `nio.Selector.select(timeout)` returning on
    /// OS-level read readiness.
    ///
    /// Returns `Poll::Ready(())` when the underlying socket has bytes
    /// available (or is in a state that should be checked, such as
    /// EOF); `Poll::Pending` registers the context's waker so Tokio
    /// will re-poll when readability changes.
    ///
    /// Default implementation returns `Poll::Ready(())` so a transport
    /// that doesn't model OS-level readiness still works (the upper
    /// poll loop falls back to the timeout-driven retry path).
    fn poll_read_ready(&self, _cx: &mut Context<'_>) -> Poll<()> {
        Poll::Ready(())
    }
}
