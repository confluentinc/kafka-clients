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

//! Translation of `org.apache.kafka.common.network.PlaintextTransportLayer`.

use std::io::{self, IoSlice};
use std::net::SocketAddr;

use tokio::net::TcpStream;

use crate::common::network::TransferableChannel;
use crate::common::network::transport_layer::{OP_CONNECT, OP_READ, TransportLayer};
use crate::common::security::auth::KafkaPrincipal;

/// Transport layer for PLAINTEXT communication. Mirrors the Java
/// `PlaintextTransportLayer`, wrapping a [`tokio::net::TcpStream`] in
/// place of Java's `SocketChannel`.
///
/// The Tokio runtime drives readiness; the per-channel interest-op set is
/// kept here so that the upper [`crate::common::network`] (`KafkaChannel`,
/// `Selector`) can mute/unmute reads without owning the underlying socket
/// directly. This mirrors the Java pattern where `selectionKey.interestOps()`
/// is the source of truth for "is this channel currently mute?".
///
/// All I/O operations are non-blocking: they call into Tokio's `try_read`
/// / `try_write` / `try_write_vectored` and translate `WouldBlock` to
/// `Ok(0)`, matching Java NIO semantics on a non-blocking
/// `SocketChannel`. This is the same convention used by
/// [`crate::common::network::NetworkReceive::read_from`], which treats
/// `Ok(0)` as "no progress on this call".
pub struct PlaintextTransportLayer {
    stream: Option<TcpStream>,
    interest_ops: i32,
    is_open: bool,
    /// Cached connect state. Mirrors Java's `SocketChannel.isConnected()`,
    /// which returns a cached field — calling `getpeername()` on every
    /// `is_connected()` invocation would be a syscall on every poll
    /// iteration of the upper Selector.
    connected: bool,
}

impl PlaintextTransportLayer {
    /// Construct a transport over an already-connected `TcpStream`. The
    /// initial interest-ops set is `OP_READ`, mirroring Java's
    /// `finishConnect` flip after the connect completes.
    pub fn new(stream: TcpStream) -> Self {
        PlaintextTransportLayer { stream: Some(stream), interest_ops: OP_READ, is_open: true, connected: true }
    }

    /// Construct a transport in the *connect-pending* state. The initial
    /// interest-ops set is `OP_CONNECT`, mirroring the Java constructor
    /// path that registers a freshly-created `SocketChannel` for connect
    /// completion. [`finish_connect`] should be called when the runtime
    /// signals connect readiness.
    pub fn pending_connect(stream: TcpStream) -> Self {
        PlaintextTransportLayer { stream: Some(stream), interest_ops: OP_CONNECT, is_open: true, connected: false }
    }

    /// Local address of the underlying socket, when the socket is open.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.stream
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "transport closed"))?
            .local_addr()
    }

    /// Peer (remote) address of the underlying socket, when the socket is
    /// open.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.stream
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "transport closed"))?
            .peer_addr()
    }

    fn stream_mut(&mut self) -> io::Result<&mut TcpStream> {
        self.stream
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "transport closed"))
    }
}

impl TransferableChannel for PlaintextTransportLayer {
    fn write_vectored(&mut self, bufs: &[IoSlice<'_>]) -> io::Result<usize> {
        // Empty input: mirror Java's GatheringByteChannel which writes 0
        // bytes for empty buffer arrays.
        if bufs.is_empty() {
            return Ok(0);
        }
        let stream = self.stream_mut()?;
        match stream.try_write_vectored(bufs) {
            Ok(n) => Ok(n),
            // WouldBlock is Java NIO's `read/write returns 0` semantics —
            // map to Ok(0) so callers (NetworkReceive / ByteBufferSend)
            // treat it as "no progress on this call" rather than as an
            // error.
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => Ok(0),
            Err(e) => Err(e),
        }
    }

    fn has_pending_writes(&self) -> bool {
        // PLAINTEXT writes directly to the socket; nothing is buffered
        // inside the transport.
        false
    }
}

impl TransportLayer for PlaintextTransportLayer {
    fn ready(&self) -> bool {
        // PLAINTEXT has no handshake — ready as soon as the socket is open
        // and the connect has finished.
        self.is_open && self.is_connected()
    }

    fn finish_connect(&mut self) -> io::Result<bool> {
        // For Tokio, `TcpStream::connect` returns once the connect is
        // established (or errors). `Selector` constructs the transport via
        // `pending_connect` after issuing the non-blocking `connect`, then
        // calls `finish_connect` when the runtime signals writability —
        // we use `peer_addr()` as the connect-completion probe (matches
        // POSIX `getpeername()` returning ENOTCONN until the connect
        // finishes). Mirrors Java's `socketChannel.finishConnect()`
        // semantics.
        let connected = match self.stream.as_ref() {
            Some(s) => s.peer_addr().is_ok(),
            None => false,
        };
        if connected {
            self.connected = true;
            // Mirror Java: clear OP_CONNECT, set OP_READ.
            self.interest_ops = (self.interest_ops & !OP_CONNECT) | OP_READ;
        }
        Ok(connected)
    }

    fn disconnect(&mut self) {
        // Java cancels the SelectionKey here; in Rust we mark the channel
        // closed so subsequent operations short-circuit. The actual socket
        // close is deferred to `close()` to mirror Java's pattern of
        // `disconnect()` + later `close()`.
        self.is_open = false;
        self.connected = false;
        self.interest_ops = 0;
    }

    fn is_connected(&self) -> bool {
        // Mirror Java's `socketChannel.isConnected()`: a cached flag,
        // not a syscall, so the upper Selector can call this on every
        // poll iteration cheaply. `connected` is set on construction
        // (`new`) or by `finish_connect` returning true; it's cleared
        // by `disconnect`/`close`.
        self.is_open && self.connected
    }

    fn is_open(&self) -> bool {
        self.is_open && self.stream.is_some()
    }

    fn close(&mut self) -> io::Result<()> {
        // Drop the TcpStream — Tokio closes the socket on drop. Mirrors
        // Java's `socketChannel.close()`.
        self.is_open = false;
        self.connected = false;
        self.interest_ops = 0;
        self.stream = None;
        Ok(())
    }

    fn read(&mut self, dst: &mut [u8]) -> io::Result<usize> {
        // Empty buffer: Java NIO `channel.read(ByteBuffer)` returns 0 for
        // a buffer with no remaining capacity.
        if dst.is_empty() {
            return Ok(0);
        }
        let stream = self.stream_mut()?;
        match stream.try_read(dst) {
            Ok(n) => Ok(n),
            // WouldBlock → Ok(0): same Java NIO semantic as on the write
            // path. NetworkReceive::read_from already handles `Ok(0)` as
            // "no progress this call".
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => Ok(0),
            Err(e) => Err(e),
        }
    }

    fn handshake(&mut self) -> io::Result<()> {
        // Mirror Java's empty `handshake()` for the PLAINTEXT
        // implementation.
        Ok(())
    }

    fn peer_principal(&self) -> io::Result<KafkaPrincipal> {
        // Mirror Java: PLAINTEXT always returns `KafkaPrincipal.ANONYMOUS`.
        Ok(KafkaPrincipal::anonymous())
    }

    fn add_interest_ops(&mut self, ops: i32) {
        if !self.is_open {
            return;
        }
        self.interest_ops |= ops;
    }

    fn remove_interest_ops(&mut self, ops: i32) {
        if !self.is_open {
            return;
        }
        self.interest_ops &= !ops;
    }

    fn interest_ops(&self) -> i32 {
        if self.is_open { self.interest_ops } else { 0 }
    }

    fn is_mute(&self) -> bool {
        // Mirror Java: `key.isValid() && (key.interestOps() & OP_READ) == 0`.
        self.is_open && (self.interest_ops & OP_READ) == 0
    }

    fn has_bytes_buffered(&self) -> bool {
        // PLAINTEXT has no internal buffering — always `false`.
        false
    }
}

/// Bridge `PlaintextTransportLayer` to `io::Read` so it can be passed as
/// `&mut dyn io::Read` to [`crate::common::network::Receive::read_from`]
/// without an extra adapter. The implementation simply forwards to
/// [`TransportLayer::read`] which already handles the
/// `WouldBlock` → `Ok(0)` translation.
impl io::Read for PlaintextTransportLayer {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        TransportLayer::read(self, buf)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read as IoRead;
    use std::net::SocketAddr;

    use tokio::io::AsyncWriteExt;
    use tokio::net::{TcpListener, TcpStream};

    use super::*;
    use crate::common::network::Receive;
    use crate::common::network::network_receive::NetworkReceive;
    use crate::common::network::transport_layer::OP_WRITE;

    /// Convenience: spin up a localhost listener, connect a `TcpStream`
    /// to it, and return the matching peer pair.
    async fn connected_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr: SocketAddr = listener.local_addr().expect("local_addr");
        let connect = TcpStream::connect(addr);
        let accept = listener.accept();
        let (client, accepted) = tokio::join!(connect, accept);
        let (server, _) = accepted.expect("accept");
        (client.expect("connect"), server)
    }

    #[tokio::test]
    async fn open_after_construction() {
        let (client, _server) = connected_pair().await;
        let layer = PlaintextTransportLayer::new(client);
        assert!(layer.is_open());
        assert!(layer.is_connected());
        assert!(layer.ready(), "PLAINTEXT is ready as soon as connected");
        assert!(!layer.has_pending_writes());
        assert!(!layer.has_bytes_buffered());
    }

    #[tokio::test]
    async fn local_and_peer_addr_round_trip() {
        let (client, server) = connected_pair().await;
        let server_addr = server.local_addr().expect("server local_addr");
        let layer = PlaintextTransportLayer::new(client);
        let peer = layer.peer_addr().expect("peer_addr");
        assert_eq!(peer, server_addr);
        let local = layer.local_addr().expect("local_addr");
        assert_eq!(local.ip().to_string(), "127.0.0.1");
    }

    #[tokio::test]
    async fn handshake_is_noop_for_plaintext() {
        let (client, _server) = connected_pair().await;
        let mut layer = PlaintextTransportLayer::new(client);
        layer.handshake().expect("handshake");
        // Must remain ready and unchanged.
        assert!(layer.ready());
    }

    #[tokio::test]
    async fn peer_principal_is_anonymous() {
        let (client, _server) = connected_pair().await;
        let layer = PlaintextTransportLayer::new(client);
        let principal = layer.peer_principal().expect("peer_principal");
        assert_eq!(principal, KafkaPrincipal::anonymous());
        assert_eq!(principal.to_string(), "User:ANONYMOUS");
    }

    #[tokio::test]
    async fn interest_ops_default_to_op_read() {
        let (client, _server) = connected_pair().await;
        let layer = PlaintextTransportLayer::new(client);
        assert_eq!(layer.interest_ops(), OP_READ);
        assert!(!layer.is_mute(), "OP_READ set → not muted");
    }

    #[tokio::test]
    async fn pending_connect_uses_op_connect() {
        let (client, _server) = connected_pair().await;
        let layer = PlaintextTransportLayer::pending_connect(client);
        assert_eq!(layer.interest_ops(), OP_CONNECT);
        // Mute = OP_READ not set; pending-connect transport is therefore
        // muted from the perspective of read interest.
        assert!(layer.is_mute());
    }

    #[tokio::test]
    async fn finish_connect_flips_op_connect_to_op_read() {
        let (client, _server) = connected_pair().await;
        let mut layer = PlaintextTransportLayer::pending_connect(client);
        let connected = layer.finish_connect().expect("finish_connect");
        assert!(connected);
        let ops = layer.interest_ops();
        assert_eq!(ops & OP_CONNECT, 0, "OP_CONNECT cleared");
        assert_eq!(ops & OP_READ, OP_READ, "OP_READ set");
    }

    #[tokio::test]
    async fn add_remove_interest_ops_toggles_mute() {
        let (client, _server) = connected_pair().await;
        let mut layer = PlaintextTransportLayer::new(client);
        layer.remove_interest_ops(OP_READ);
        assert!(layer.is_mute(), "removed OP_READ → mute");
        layer.add_interest_ops(OP_READ | OP_WRITE);
        assert!(!layer.is_mute());
        assert_eq!(layer.interest_ops() & OP_WRITE, OP_WRITE);
    }

    #[tokio::test]
    async fn close_drops_socket_and_zeroes_interest_ops() {
        let (client, _server) = connected_pair().await;
        let mut layer = PlaintextTransportLayer::new(client);
        layer.close().expect("close");
        assert!(!layer.is_open());
        assert!(!layer.is_connected());
        assert!(!layer.ready());
        assert_eq!(layer.interest_ops(), 0);
        // local/peer addr now error out because the socket is gone.
        assert!(layer.local_addr().is_err());
    }

    #[tokio::test]
    async fn disconnect_marks_closed_without_dropping_stream() {
        let (client, _server) = connected_pair().await;
        let mut layer = PlaintextTransportLayer::new(client);
        layer.disconnect();
        assert!(!layer.is_open());
        assert_eq!(layer.interest_ops(), 0);
    }

    /// Round-trip a payload over the connected pair: the writer side
    /// sends `4-byte length-prefix + payload`, and the reader side
    /// drains it through `NetworkReceive::read_from`, treating the
    /// transport as `&mut dyn io::Read`.
    #[tokio::test]
    async fn round_trip_via_network_receive() {
        let (client, mut server) = connected_pair().await;
        let mut layer = PlaintextTransportLayer::new(client);

        // Server writes: 4-byte big-endian size + payload bytes.
        let payload: Vec<u8> = (0u8..32u8).collect();
        let mut framed = Vec::with_capacity(4 + payload.len());
        framed.extend_from_slice(&(payload.len() as i32).to_be_bytes());
        framed.extend_from_slice(&payload);
        server.write_all(&framed).await.expect("write_all");
        server.flush().await.expect("flush");

        // Wait for the data to land in the kernel buffer of `client`.
        // We use `readable()` as a readiness probe (analogous to Java's
        // selector signalling OP_READ).
        let stream = layer.stream_mut().expect("stream");
        stream.readable().await.expect("readable");

        let mut receive = NetworkReceive::with_max_size(1024, "0");
        // Multiple read_from passes are expected: header first, payload
        // next. We loop until completion or until the readable signal
        // dries up.
        for _ in 0..16 {
            let _ = receive.read_from(&mut layer).expect("read_from");
            if receive.complete() {
                break;
            }
            // Ensure further data has arrived if we need it.
            let stream = layer.stream_mut().expect("stream");
            // `readable()` resolves immediately if data is ready; otherwise
            // a short timeout to avoid hanging the test.
            tokio::time::timeout(std::time::Duration::from_millis(100), stream.readable())
                .await
                .expect("readable timeout")
                .expect("readable");
        }
        assert!(receive.complete(), "receive should complete after server writes");
        let parsed: Vec<u8> = receive.payload().expect("payload").to_vec();
        assert_eq!(parsed, payload);
    }

    /// Verify the Read adapter returns `Ok(0)` (not an error) when the
    /// kernel buffer is empty — Java NIO "would block" semantics.
    #[tokio::test]
    async fn read_returns_ok_zero_when_no_data_ready() {
        let (client, _server) = connected_pair().await;
        let mut layer = PlaintextTransportLayer::new(client);
        let mut buf = [0u8; 64];
        let n = IoRead::read(&mut layer, &mut buf).expect("read should not error on quiet socket");
        assert_eq!(n, 0, "no data ready → Ok(0), not WouldBlock");
    }

    /// Verify that `write_vectored` honours `Ok(0)` for empty input.
    #[tokio::test]
    async fn write_vectored_with_empty_input_writes_zero() {
        let (client, _server) = connected_pair().await;
        let mut layer = PlaintextTransportLayer::new(client);
        let n = layer.write_vectored(&[]).expect("write_vectored");
        assert_eq!(n, 0);
    }

    /// Operations on a closed transport must surface a `NotConnected`
    /// error, not panic.
    #[tokio::test]
    async fn ops_on_closed_transport_error() {
        let (client, _server) = connected_pair().await;
        let mut layer = PlaintextTransportLayer::new(client);
        layer.close().expect("close");

        let mut buf = [0u8; 16];
        let err = TransportLayer::read(&mut layer, &mut buf).expect_err("read on closed transport must error");
        assert_eq!(err.kind(), io::ErrorKind::NotConnected);

        let err = layer
            .write_vectored(&[IoSlice::new(b"x")])
            .expect_err("write on closed must error");
        assert_eq!(err.kind(), io::ErrorKind::NotConnected);
    }
}
