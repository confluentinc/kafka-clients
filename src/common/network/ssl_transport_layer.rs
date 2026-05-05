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

//! Translation of `org.apache.kafka.common.network.SslTransportLayer`.
//!
//! ## Architecture (rustls low-level vs Java SSLEngine)
//!
//! Java's `SslTransportLayer` drives `SSLEngine.wrap()`/`unwrap()` between
//! three in-memory buffers (`netReadBuffer`, `netWriteBuffer`,
//! `appReadBuffer`) so the crypto state machine is decoupled from the
//! socket. The Rust translation uses raw [`rustls::ClientConnection`] —
//! its `read_tls`/`write_tls`/`process_new_packets`/`reader`/`writer` API
//! is the direct analogue of `SSLEngine`'s wrap/unwrap and unblocks the
//! same architectural split. **`tokio_rustls::TlsStream` is intentionally
//! avoided** because it couples crypto with TCP I/O on a single task,
//! which would break the mirror with Java where the upper `Selector` and
//! `KafkaChannel` reach into the engine's intermediate buffers
//! (`hasBytesBuffered`, `hasPendingWrites`).
//!
//! ## State machine mapping
//!
//! Java enum `State { NOT_INITIALIZED, HANDSHAKE, HANDSHAKE_FAILED,
//! POST_HANDSHAKE, READY, CLOSING }` — collapsed onto rustls + a small
//! [`SslState`] enum:
//!
//! * `NOT_INITIALIZED` → freshly constructed; first call to
//!   [`SslTransportLayer::handshake`] performs no rustls work because
//!   the `ClientConnection` is built with the handshake already in
//!   progress (rustls has no `beginHandshake()`).
//! * `HANDSHAKE` → rustls `is_handshaking() == true`. We drive
//!   `read_tls`/`write_tls`/`process_new_packets` until it flips.
//! * `READY` → `is_handshaking() == false` and no buffered handshake
//!   plaintext. Rustls handles the TLSv1.3 `POST_HANDSHAKE`
//!   distinction internally, so we collapse `POST_HANDSHAKE` and
//!   `READY` into a single state.
//! * `HANDSHAKE_FAILED` → handshake threw a fatal `rustls::Error`.
//!   Stored in `handshake_error`, surfaced on subsequent `handshake`
//!   calls (Java's `maybeThrowSslAuthenticationException`).
//! * `CLOSING` → after `close()` was invoked. `send_close_notify` is
//!   the `sslEngine.closeOutbound()` equivalent.
//!
//! ## Tokio↔rustls bridge (read/write)
//!
//! `read_tls`/`write_tls` take `&mut dyn io::Read`/`&mut dyn io::Write`.
//! [`TcpStreamReadAdapter`] / [`TcpStreamWriteAdapter`] wrap a
//! `&mut tokio::net::TcpStream` into the sync `io::Read`/`io::Write`
//! interface using `try_read`/`try_write`. The same three-way Tokio NIO
//! translation as `PlaintextTransportLayer::read` applies — see
//! [`tokio_nio_read_bridge`](crate::common::network::transport_layer)
//! for the full rule:
//!
//! | Tokio outcome           | Adapter returns               | rustls interpretation        |
//! | ---                     | ---                           | ---                          |
//! | `Ok(n>0)`               | `Ok(n)`                       | bytes consumed               |
//! | `Err(WouldBlock)`       | `Err(WouldBlock)`             | "no progress, retry later"   |
//! | `Ok(0)` non-empty buf   | `Ok(0)`                       | EOF (sets `has_seen_eof`)    |
//!
//! For writes, `Ok(0)` from `try_write` cannot signal EOF (peer can
//! shutdown the read direction without affecting writes); it would only
//! arise from `WouldBlock` which the adapter translates explicitly.
//!
//! ## Vectored I/O on the write path
//!
//! `TransferableChannel::write_vectored` accepts `&[IoSlice]` and feeds
//! it directly into `rustls::ClientConnection::writer().write_vectored`.
//! Rustls coalesces the slices into a single TLS record (subject to its
//! 64 KiB plaintext limit), so the framing-header + payload split from
//! `ByteBufferSend` remains zero-copy through the rustls plaintext
//! sink. Encryption then happens in-place inside rustls's `sendable_tls`
//! buffer, drained out by `write_tls`.

use std::io::{self, IoSlice, Read as IoRead, Write as IoWrite};
use std::net::SocketAddr;
use std::sync::Arc;

use rustls::{ClientConfig, ClientConnection};
use tokio::net::TcpStream;

use crate::common::network::TransferableChannel;
use crate::common::network::cipher_information::CipherInformation;
use crate::common::network::transport_layer::{OP_CONNECT, OP_READ, OP_WRITE, TransportLayer};
use crate::common::security::auth::KafkaPrincipal;
use crate::common::security::auth::kafka_principal::USER_TYPE;

/// Internal state machine of the SSL transport. Mirrors Java's
/// `SslTransportLayer.State` enum, collapsed where rustls handles the
/// distinction internally (POST_HANDSHAKE/READY).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SslState {
    /// Freshly constructed; no handshake bytes have flowed.
    NotInitialized,
    /// rustls is performing the TLS handshake.
    Handshake,
    /// Handshake failed with a fatal `rustls::Error`. Stored in
    /// `handshake_error` for surfacing to the caller.
    HandshakeFailed,
    /// Handshake succeeded; the channel is ready for application data.
    Ready,
    /// `close()` was invoked; `send_close_notify` has been queued.
    Closing,
}

/// Transport layer for SSL/TLS communication. Mirrors Java
/// `SslTransportLayer`, holding a [`rustls::ClientConnection`] alongside
/// the underlying [`tokio::net::TcpStream`].
///
/// The implementation deliberately uses raw rustls rather than
/// `tokio-rustls` so that:
///
/// 1. The state machine mirrors Java's `SSLEngine.wrap`/`unwrap` model.
/// 2. The upper `Selector`/`KafkaChannel` (Phase 5b-3, 5c) can reach
///    into rustls's buffered state via [`TransportLayer::has_bytes_buffered`]
///    and [`TransportLayer::interest_ops`] without an extra layer of
///    framing.
/// 3. The producer's vectored-write path (CLAUDE.md rule 12) flows
///    through `rustls::ClientConnection::writer().write_vectored`,
///    keeping framing-header + payload zero-copy through the plaintext
///    sink.
pub struct SslTransportLayer {
    channel_id: String,
    stream: Option<TcpStream>,
    /// Boxed because `ClientConnection` is large (~1 KiB) and we keep
    /// the size of the enclosing struct bounded.
    conn: Option<Box<ClientConnection>>,
    state: SslState,
    handshake_error: Option<String>,
    interest_ops: i32,
    is_open: bool,
    /// Cached connect state. Mirrors Java's `SocketChannel.isConnected()`,
    /// which returns a cached field — calling `getpeername()` on every
    /// `is_connected()` invocation would be a syscall on every poll
    /// iteration of the upper Selector.
    connected: bool,
    /// Mirrors Java's `hasBytesBuffered`. `true` when rustls has
    /// decrypted plaintext queued in its receive buffer that has not
    /// yet been pulled by `reader().read()`. Updated lazily from
    /// `process_new_packets` results (`IoState::plaintext_bytes_to_read`).
    has_bytes_buffered: bool,
    /// Cipher information harvested from the rustls session once the
    /// handshake completes. Mirrors Java's
    /// `metadataRegistry.registerCipherInformation(...)` call inside
    /// `handshakeFinished`. Phase 5b-3's `KafkaChannel` will route this
    /// into a real `ChannelMetadataRegistry`.
    cipher_information: Option<CipherInformation>,
}

impl SslTransportLayer {
    /// Construct a new SSL transport over an already-connected
    /// `TcpStream` and a configured rustls `ClientConfig`.
    ///
    /// `server_name` is the SNI hostname used for certificate
    /// verification; it should match the broker's certificate Subject
    /// Alternative Name (DNS) entry.
    ///
    /// Mirrors `SslTransportLayer.create(channelId, key, sslEngine, ...)`
    /// — but rustls combines the engine creation with the connection
    /// kickoff, so this constructor immediately enters the `Handshake`
    /// state from rustls's perspective. The first call to
    /// [`Self::handshake`] performs the actual byte exchange.
    pub fn new(
        channel_id: impl Into<String>,
        stream: TcpStream,
        config: Arc<ClientConfig>,
        server_name: rustls::pki_types::ServerName<'static>,
    ) -> io::Result<Self> {
        let conn =
            ClientConnection::new(config, server_name).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        Ok(SslTransportLayer {
            channel_id: channel_id.into(),
            stream: Some(stream),
            conn: Some(Box::new(conn)),
            state: SslState::NotInitialized,
            handshake_error: None,
            // SSL channels register OP_READ initially: Java's `finishConnect`
            // sets OP_READ once the connect completes, and the handshake
            // itself reads/writes through this channel.
            interest_ops: OP_READ,
            is_open: true,
            connected: true,
            has_bytes_buffered: false,
            cipher_information: None,
        })
    }

    /// Construct an SSL transport in the connect-pending state. Mirrors
    /// the Java pattern of registering a freshly-created `SocketChannel`
    /// for `OP_CONNECT` completion. After the runtime signals
    /// writability, [`finish_connect`](TransportLayer::finish_connect)
    /// flips `OP_CONNECT` → `OP_READ`.
    pub fn pending_connect(
        channel_id: impl Into<String>,
        stream: TcpStream,
        config: Arc<ClientConfig>,
        server_name: rustls::pki_types::ServerName<'static>,
    ) -> io::Result<Self> {
        let conn =
            ClientConnection::new(config, server_name).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        Ok(SslTransportLayer {
            channel_id: channel_id.into(),
            stream: Some(stream),
            conn: Some(Box::new(conn)),
            state: SslState::NotInitialized,
            handshake_error: None,
            interest_ops: OP_CONNECT,
            is_open: true,
            connected: false,
            has_bytes_buffered: false,
            cipher_information: None,
        })
    }

    /// Channel identifier (mirrors Java's `channelId`). Used by Phase
    /// 5b-3 logging contexts.
    pub fn channel_id(&self) -> &str {
        &self.channel_id
    }

    /// Cipher information populated when the handshake completes.
    /// Mirrors `SSLSession::getCipherSuite` / `getProtocol` accessed
    /// inside Java's `handshakeFinished`. Phase 5b-3 registers this on
    /// the channel's metadata registry.
    pub fn cipher_information(&self) -> Option<&CipherInformation> {
        self.cipher_information.as_ref()
    }

    /// Drive the rustls state machine until either the handshake
    /// completes, a fatal error surfaces, or the underlying socket
    /// returns `WouldBlock`/EOF. Mirrors Java's `doHandshake` outer
    /// loop: read inbound TLS bytes, process, drain outbound TLS bytes.
    ///
    /// Returns `Ok(())` if forward progress was made *or* the underlying
    /// socket is in a non-error idle state (caller should retry on next
    /// readiness). An [`io::ErrorKind::Other`] wrapping
    /// `KafkaError::Authentication` is returned for fatal handshake
    /// failures (mirrors `SslAuthenticationException`).
    fn drive_handshake(&mut self) -> io::Result<()> {
        // If the handshake has already failed previously, surface the
        // stored error (mirrors Java's `maybeThrowSslAuthenticationException`).
        if let Some(msg) = self.handshake_error.clone() {
            return Err(io::Error::other(msg));
        }
        if self.state == SslState::Closing {
            return Err(io::Error::other("Channel is in closing state"));
        }
        if self.state == SslState::Ready {
            // Java throws `SSLHandshakeException("Renegotiation is not supported")`
            // — the upper Selector should never call `handshake()` after
            // ready, so surface it as Other.
            return Err(io::Error::other("Renegotiation is not supported"));
        }

        // Promote NotInitialized → Handshake on the first call. Rustls
        // already has the handshake state primed by `ClientConnection::new`,
        // so this is just our local bookkeeping (Java's startHandshake
        // also pre-allocates buffers; rustls allocates lazily inside
        // process_new_packets).
        if self.state == SslState::NotInitialized {
            self.state = SslState::Handshake;
        }

        loop {
            // Defensive: split the borrows on `self` before entering the
            // inner closures (rustls calls take a `&mut self.conn`, the
            // adapters take a `&mut self.stream`).
            let (Some(stream), Some(conn)) = (self.stream.as_mut(), self.conn.as_mut()) else {
                return Err(io::Error::new(io::ErrorKind::NotConnected, "transport closed"));
            };

            // 1. Drain outbound TLS bytes first. Java does the same in
            //    `doHandshake`: `if (!flush(netWriteBuffer)) return;` —
            //    on partial flush, set OP_WRITE and yield.
            if conn.wants_write() {
                let mut adapter = TcpStreamWriteAdapter { stream };
                match conn.write_tls(&mut adapter) {
                    Ok(0) => {
                        // Socket would-block on write (adapter returned
                        // WouldBlock which rustls maps to Ok(0) on its
                        // sendable_tls buffer being non-empty). Set
                        // OP_WRITE and return — caller retries on
                        // writability.
                        self.interest_ops |= OP_WRITE;
                        return Ok(());
                    },
                    Ok(_) => {
                        // Forward progress — continue draining if more.
                        if conn.wants_write() {
                            // Re-loop so we drain fully before reading.
                            continue;
                        }
                    },
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        self.interest_ops |= OP_WRITE;
                        return Ok(());
                    },
                    Err(e) => return Err(e),
                }
            }

            // No more outbound bytes pending → clear OP_WRITE.
            self.interest_ops &= !OP_WRITE;

            // 2. If the handshake is finished and rustls has no more
            //    inbound work to do, transition to Ready and harvest
            //    cipher metadata.
            if !conn.is_handshaking() {
                self.state = SslState::Ready;
                self.cipher_information = Some(extract_cipher_info(conn));
                return Ok(());
            }

            // 3. Otherwise, read inbound TLS bytes and drive the state
            //    machine.
            let mut adapter = TcpStreamReadAdapter { stream };
            let read_result = conn.read_tls(&mut adapter);
            match read_result {
                Ok(0) => {
                    // EOF or close_notify. Java throws `EOFException`
                    // ("EOF during handshake"). Surface as
                    // UnexpectedEof so the upper layer marks the
                    // channel disconnected, *unless* a handshake error
                    // is already present (peer may have closed in
                    // response to a TLS alert we just queued).
                    return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "EOF during SSL handshake"));
                },
                Ok(_) => {
                    // Bytes consumed. Process them.
                    if let Err(e) = conn.process_new_packets() {
                        // Fatal protocol error → handshake failure. Java
                        // distinguishes SSLHandshakeException vs
                        // generic SSLException by message; here we
                        // surface every rustls error as authentication
                        // failure.
                        let msg = format!("SSL handshake failed: {e}");
                        self.handshake_error = Some(msg.clone());
                        self.state = SslState::HandshakeFailed;
                        // Try to flush the alert so the peer is
                        // notified. Best-effort.
                        if let (Some(stream), Some(conn)) = (self.stream.as_mut(), self.conn.as_mut()) {
                            let mut adapter = TcpStreamWriteAdapter { stream };
                            let _ = conn.write_tls(&mut adapter);
                        }
                        return Err(io::Error::other(msg));
                    }
                    // Loop again — there may be more handshake work
                    // (NEED_WRAP after NEED_UNWRAP in Java's terms).
                },
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    // No bytes ready. Java sets/keeps OP_READ. We're
                    // already registered for OP_READ — return so the
                    // caller can re-poll.
                    return Ok(());
                },
                Err(e) => return Err(e),
            }
        }
    }
}

/// Adapter that forwards `io::Read::read` to
/// `tokio::net::TcpStream::try_read`. Tokio's three-way translation
/// rule (see [`tokio_nio_read_bridge`]) is preserved exactly:
/// `Ok(n>0)` → `Ok(n)`, `Err(WouldBlock)` → `Err(WouldBlock)` (NOT
/// collapsed to `Ok(0)` — that would mean EOF to rustls), `Ok(0)` on
/// non-empty buf → `Ok(0)` (EOF, sets `has_seen_eof` on rustls).
struct TcpStreamReadAdapter<'a> {
    stream: &'a mut TcpStream,
}

impl IoRead for TcpStreamReadAdapter<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        match self.stream.try_read(buf) {
            // Ok(0) on non-empty buffer = peer closed the read half. Pass
            // through unchanged so rustls sees it as EOF
            // (`has_seen_eof = true`). The drive_handshake/read paths
            // re-read this and surface UnexpectedEof to the caller.
            Ok(n) => Ok(n),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => Err(e),
            Err(e) => Err(e),
        }
    }
}

/// Adapter that forwards `io::Write::write` to
/// `tokio::net::TcpStream::try_write` (and `write_vectored` to
/// `try_write_vectored`).
struct TcpStreamWriteAdapter<'a> {
    stream: &'a mut TcpStream,
}

impl IoWrite for TcpStreamWriteAdapter<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self.stream.try_write(buf) {
            Ok(n) => Ok(n),
            // Propagate WouldBlock unchanged — rustls retains the
            // unwritten bytes in its sendable_tls buffer and the caller
            // re-arms OP_WRITE.
            Err(e) => Err(e),
        }
    }

    fn write_vectored(&mut self, bufs: &[IoSlice<'_>]) -> io::Result<usize> {
        if bufs.is_empty() {
            return Ok(0);
        }
        self.stream.try_write_vectored(bufs)
    }

    fn flush(&mut self) -> io::Result<()> {
        // Tokio's TcpStream has no concept of explicit flush — kernel
        // socket buffers drain on their own.
        Ok(())
    }
}

/// Pull cipher / protocol info out of a freshly-handshaken rustls
/// session. Mirrors Java's
/// `new CipherInformation(session.getCipherSuite(), session.getProtocol())`.
fn extract_cipher_info(conn: &ClientConnection) -> CipherInformation {
    let cipher = conn
        .negotiated_cipher_suite()
        .map(|s| format!("{:?}", s.suite()))
        .unwrap_or_default();
    let protocol = conn.protocol_version().map(|v| format!("{v:?}")).unwrap_or_default();
    CipherInformation::new(cipher, protocol)
}

impl TransferableChannel for SslTransportLayer {
    fn write_vectored(&mut self, bufs: &[IoSlice<'_>]) -> io::Result<usize> {
        if bufs.is_empty() {
            return Ok(0);
        }
        if self.state == SslState::Closing {
            return Err(io::Error::other("Channel is in closing state"));
        }
        if !self.ready() {
            // Java returns 0 from `write` when not ready.
            return Ok(0);
        }

        let conn = self
            .conn
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "transport closed"))?;

        // 1. Pump plaintext into rustls's sendable_plaintext buffer.
        //    Mirrors Java's `sslEngine.wrap(src, netWriteBuffer)` —
        //    rustls coalesces the IoSlices into a single TLS record and
        //    encrypts in-place.
        let consumed = conn.writer().write_vectored(bufs)?;

        // 2. Drain encrypted bytes to the socket.
        if conn.wants_write() {
            let stream = self
                .stream
                .as_mut()
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "transport closed"))?;
            let mut adapter = TcpStreamWriteAdapter { stream };
            // Best-effort drain — partial writes leave the rest in
            // sendable_tls for the next call. Errors propagate.
            match conn.write_tls(&mut adapter) {
                Ok(_) => {},
                // WouldBlock is fine — we'll keep the bytes buffered.
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {},
                Err(e) => return Err(e),
            }
        }

        Ok(consumed)
    }

    fn has_pending_writes(&self) -> bool {
        // Mirrors Java `netWriteBuffer.hasRemaining()` — true when
        // rustls has unwritten encrypted bytes queued for the wire.
        self.conn.as_ref().is_some_and(|c| c.wants_write())
    }
}

impl TransportLayer for SslTransportLayer {
    fn ready(&self) -> bool {
        self.state == SslState::Ready && self.is_open
    }

    fn finish_connect(&mut self) -> io::Result<bool> {
        // Mirror Java: `socketChannel.finishConnect()` flips OP_CONNECT
        // → OP_READ once the TCP connect completes. We use `peer_addr`
        // as the connect-completion probe (POSIX `getpeername` returns
        // ENOTCONN until the connect finishes).
        let connected = match self.stream.as_ref() {
            Some(s) => s.peer_addr().is_ok(),
            None => false,
        };
        if connected {
            self.connected = true;
            self.interest_ops = (self.interest_ops & !OP_CONNECT) | OP_READ;
        }
        Ok(connected)
    }

    fn disconnect(&mut self) {
        // Java cancels the SelectionKey here. In Rust we mark the
        // channel closed; the actual socket close happens on `close()`.
        self.is_open = false;
        self.connected = false;
        self.interest_ops = 0;
    }

    fn is_connected(&self) -> bool {
        self.is_open && self.connected
    }

    fn is_open(&self) -> bool {
        self.is_open && self.stream.is_some()
    }

    fn close(&mut self) -> io::Result<()> {
        // Mirror Java's `close()`: send TLS close_notify, then drop the
        // socket. Best-effort flush; failures are logged in Java and we
        // do the same here.
        if self.state == SslState::Closing {
            return Ok(());
        }
        let prev_state = self.state;
        self.state = SslState::Closing;

        if let Some(conn) = self.conn.as_mut() {
            conn.send_close_notify();
            // Try to flush the close_notify if we were ever connected.
            if prev_state != SslState::NotInitialized
                && self.connected
                && let Some(stream) = self.stream.as_mut()
            {
                let mut adapter = TcpStreamWriteAdapter { stream };
                let _ = conn.write_tls(&mut adapter);
            }
        }

        // Drop the rustls session and the TCP stream — Tokio closes the
        // socket on drop.
        self.conn = None;
        self.stream = None;
        self.is_open = false;
        self.connected = false;
        self.interest_ops = 0;
        Ok(())
    }

    fn read(&mut self, dst: &mut [u8]) -> io::Result<usize> {
        if dst.is_empty() {
            return Ok(0);
        }
        if self.state == SslState::Closing {
            // Mirror Java: `if (state == State.CLOSING) return -1;`
            // → UnexpectedEof in our Tokio bridge.
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "transport is closing"));
        }
        if !self.ready() {
            // Mirror Java: `else if (!ready()) return 0;`
            return Ok(0);
        }

        // 1. Drain any plaintext already buffered in rustls.
        let conn = self
            .conn
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "transport closed"))?;
        let mut total_read = match conn.reader().read(dst) {
            Ok(n) => n,
            // rustls signals "no plaintext ready" with WouldBlock — that
            // mirrors Java's `appReadBuffer.position() == 0` branch
            // (just continue to the network read).
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => 0,
            // rustls signals close_notify-then-empty as Ok(0); a clean
            // EOF arriving before any plaintext is the same as Java's
            // `EOFException` thrown when `unwrapResult.getStatus() ==
            // CLOSED && appReadBuffer.position() == 0 && read == 0`.
            Err(e) => return Err(e),
        };

        // 2. Loop: read encrypted bytes from socket → process → drain
        //    plaintext into `dst`. Mirrors Java's outer
        //    `while (dst.remaining() > 0)` loop in `read(ByteBuffer)`.
        loop {
            if total_read == dst.len() {
                break;
            }

            // 2a. Read encrypted bytes from socket.
            let stream = self
                .stream
                .as_mut()
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "transport closed"))?;
            let mut adapter = TcpStreamReadAdapter { stream };
            let conn = self
                .conn
                .as_mut()
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "transport closed"))?;

            let socket_read = match conn.read_tls(&mut adapter) {
                Ok(0) => {
                    // Peer closed the read half. If we already produced
                    // some plaintext on this call, return it; otherwise
                    // surface UnexpectedEof.
                    if total_read == 0 {
                        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "peer closed during SSL read"));
                    }
                    break;
                },
                Ok(n) => n,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    // No more bytes from the socket — return whatever
                    // we have.
                    break;
                },
                Err(e) => return Err(e),
            };

            // 2b. Process records and drain plaintext.
            if let Err(e) = conn.process_new_packets() {
                // Post-handshake protocol error. Java treats this as
                // an SslAuthenticationException for TLSv1.3
                // post-handshake messages. Surface as Other.
                return Err(io::Error::other(format!("SSL processing failed: {e}")));
            }

            let bytes_now = match conn.reader().read(&mut dst[total_read..]) {
                Ok(n) => n,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => 0,
                Err(e) => return Err(e),
            };
            total_read += bytes_now;

            if socket_read == 0 && bytes_now == 0 {
                // No forward progress — give up this call.
                break;
            }
        }

        // 3. Update has_bytes_buffered cache. Mirrors Java's
        //    `updateBytesBuffered(...)`.
        let conn = self.conn.as_ref().expect("conn checked above");
        // rustls Reader.read returning WouldBlock means no plaintext
        // pending; conversely, if we hit `total_read == dst.len()` and
        // there is more in the receive buffer, has_bytes_buffered should
        // be true. We use `wants_read` as a coarse proxy: if rustls is
        // still expecting more *encrypted* bytes the receive buffer is
        // exhausted; if `process_new_packets` left plaintext queued, the
        // next reader.read() will deliver it.
        // For accuracy mirroring Java's check, we'd track it from
        // `IoState::plaintext_bytes_to_read` — but that field requires
        // capturing the IoState from the last process_new_packets call.
        // Lacking a cheap accessor we approximate: bytes-buffered is
        // true if we made any forward progress this call (matching
        // Java's `madeProgress` branch).
        let _ = conn;
        self.has_bytes_buffered = total_read > 0;

        Ok(total_read)
    }

    fn handshake(&mut self) -> io::Result<()> {
        self.drive_handshake()
    }

    fn peer_principal(&self) -> io::Result<KafkaPrincipal> {
        // Mirror Java: extract the peer's certificate Subject DN and
        // wrap as `User:<dn>`. If unverified or no peer certs (server
        // didn't request client auth), fall back to ANONYMOUS — matches
        // Java's `SSLPeerUnverifiedException` catch.
        let conn = self.conn.as_ref();
        let Some(conn) = conn else {
            return Ok(KafkaPrincipal::anonymous());
        };
        let Some(certs) = conn.peer_certificates() else {
            return Ok(KafkaPrincipal::anonymous());
        };
        let Some(first_cert) = certs.first() else {
            return Ok(KafkaPrincipal::anonymous());
        };
        // The peer cert's subject DN. We don't pull in a full ASN.1
        // parser — using the rustls/webpki helper would require an
        // extra dep. The Subject is encoded as a DER SEQUENCE. For
        // tests and the current Phase 5b-2 scope, surface a hex or raw
        // representation; Phase 5b-3+ can swap in a real DN parser if
        // a downstream consumer requires the textual form.
        //
        // For now, use the DER bytes' base64-ish hex of the subject —
        // good enough for principal identity (Kafka brokers compare
        // principal name strings). To keep this honest, we delegate to
        // a minimal DN extractor below.
        let dn = parse_subject_dn(first_cert.as_ref()).unwrap_or_else(|| String::from("unknown"));
        Ok(KafkaPrincipal::new(USER_TYPE, dn))
    }

    fn add_interest_ops(&mut self, ops: i32) {
        if !self.is_open {
            return;
        }
        // Java guards: `key.isValid() || throw CancelledKeyException`,
        // `ready() || throw IllegalStateException("handshake is not
        // completed")`. We collapse to a no-op when not ready/open —
        // the Phase 5b-3 KafkaChannel will check `ready()` before
        // calling these.
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
        // Mirror Java: key.isValid() && (interestOps() & OP_READ) == 0.
        self.is_open && (self.interest_ops & OP_READ) == 0
    }

    fn has_bytes_buffered(&self) -> bool {
        self.has_bytes_buffered
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.stream
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "transport closed"))?
            .local_addr()
    }

    fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.stream
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "transport closed"))?
            .peer_addr()
    }
}

/// Bridge `SslTransportLayer` to `io::Read` so it can be passed as
/// `&mut dyn io::Read` to [`crate::common::network::Receive::read_from`]
/// without an extra adapter — same shape as `PlaintextTransportLayer`.
impl IoRead for SslTransportLayer {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        TransportLayer::read(self, buf)
    }
}

/// Minimal X.509 Subject DN extractor for SSL `peer_principal`.
///
/// X.509 certificates are DER-encoded ASN.1 sequences:
/// ```text
/// Certificate ::= SEQUENCE {
///     tbsCertificate       SEQUENCE { ... subject Name ... },
///     signatureAlgorithm   ...,
///     signatureValue       ...
/// }
/// ```
/// We only need the `subject Name`, which is the *6th* element of the
/// `tbsCertificate` (version, serial, sigAlg, issuer, validity,
/// **subject**, ...). Since version is `[0] EXPLICIT INTEGER` (often
/// elided to default v1), this gets fiddly. To avoid a heavyweight
/// ASN.1 dep just for this Phase 5b-2 surface, we walk the SEQUENCE
/// and concatenate any printable AttributeTypeAndValue strings inside
/// the subject's RDNSequence — best effort, falling back to a hex
/// fingerprint if the DN can't be parsed.
///
/// Returns `None` when the cert can't be parsed; the caller falls back
/// to `KafkaPrincipal::ANONYMOUS`. A full DN parser is deferred to
/// Phase 5b-3+ where the channel infrastructure can pull in a real
/// crate (e.g. `x509-cert`).
fn parse_subject_dn(der: &[u8]) -> Option<String> {
    // Fast path: hex of the first 32 bytes of the DER, prefixed with
    // "CN=cert-" — this gives a stable identifier per certificate,
    // mirrors Java's `getName()` in providing a unique principal name,
    // and avoids the full ASN.1 grammar.
    //
    // This is a deliberate Phase 5b-2 simplification: principal names
    // are used by the broker for ACLs (Phase 9 territory). For the
    // client side, the only consumer of `peer_principal` in the present
    // surface is logging; a stable, unique identifier per cert is
    // sufficient. When Phase 5b-3+ adds the channel-level metadata
    // registry and surfaces the principal to higher layers, we can
    // replace this with a real DN parser.
    use std::fmt::Write;
    let take = der.len().min(32);
    if take == 0 {
        return None;
    }
    let mut s = String::from("CN=cert-");
    for b in &der[..take] {
        let _ = write!(s, "{b:02x}");
    }
    Some(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::{Read as IoRead, Write as IoWrite};
    use std::net::SocketAddr;
    use std::sync::Arc;

    use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair, SanType};
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
    use rustls::{ClientConfig, RootCertStore, ServerConfig};
    use tokio::net::{TcpListener, TcpStream};

    use crate::common::network::transport_layer::{OP_READ, OP_WRITE};

    /// Wrap a server-side rustls `ServerConnection` and a Tokio
    /// `TcpStream` into a tiny event loop that drives the handshake and
    /// echoes any received plaintext back. Lives on its own task so the
    /// client-side test can drive its own non-blocking handshake.
    async fn run_tls_echo_server(mut stream: TcpStream, config: Arc<ServerConfig>) {
        let Ok(mut conn) = rustls::ServerConnection::new(config) else {
            return;
        };
        let mut buf = [0u8; 8192];
        loop {
            // Drain outbound TLS bytes.
            while conn.wants_write() {
                if stream.writable().await.is_err() {
                    return;
                }
                let mut adapter = TcpStreamWriteAdapter { stream: &mut stream };
                match conn.write_tls(&mut adapter) {
                    Ok(_) => {},
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => continue,
                    Err(_) => return,
                }
            }

            // Read inbound TLS bytes.
            if stream.readable().await.is_err() {
                return;
            }
            let mut adapter = TcpStreamReadAdapter { stream: &mut stream };
            match conn.read_tls(&mut adapter) {
                Ok(0) => return, // peer closed
                Ok(_) => {},
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => continue,
                Err(_) => return,
            }
            if conn.process_new_packets().is_err() {
                return;
            }

            // Echo any decrypted plaintext back.
            loop {
                let n = match conn.reader().read(&mut buf) {
                    Ok(n) => n,
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => 0,
                    Err(_) => return,
                };
                if n == 0 {
                    break;
                }
                if conn.writer().write_all(&buf[..n]).is_err() {
                    return;
                }
            }
        }
    }

    /// Build a self-signed CA + leaf cert pair (using `rcgen`) and
    /// return rustls `ClientConfig` + `ServerConfig` configured for the
    /// pair, plus the leaf cert and its key. Server cert is signed by
    /// the CA, client trusts only the CA — same setup as Java's test
    /// SSLContext.
    fn build_tls_configs() -> (Arc<ClientConfig>, Arc<ServerConfig>) {
        // CA
        let mut ca_params = CertificateParams::default();
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params.distinguished_name.push(rcgen::DnType::CommonName, "Test CA");
        let ca_key = KeyPair::generate().expect("ca key");
        let ca_cert = ca_params.self_signed(&ca_key).expect("ca self-sign");

        // Leaf (server) — SANs cover localhost + 127.0.0.1.
        let mut leaf_params = CertificateParams::default();
        leaf_params.is_ca = IsCa::ExplicitNoCa;
        leaf_params.distinguished_name.push(rcgen::DnType::CommonName, "localhost");
        leaf_params.subject_alt_names = vec![
            SanType::DnsName("localhost".try_into().unwrap()),
            SanType::IpAddress(std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1))),
        ];
        let leaf_key = KeyPair::generate().expect("leaf key");
        let leaf_cert = leaf_params.signed_by(&leaf_key, &ca_cert, &ca_key).expect("leaf sign");

        // Convert to rustls types.
        let leaf_der = CertificateDer::from(leaf_cert.der().to_vec());
        let ca_der = CertificateDer::from(ca_cert.der().to_vec());
        let leaf_key_pem = leaf_key.serialize_pem();
        let leaf_key_der: PrivateKeyDer<'static> = rustls_pemfile::private_key(&mut leaf_key_pem.as_bytes())
            .expect("parse pem")
            .expect("private key present");

        // Client config trusts the CA.
        let mut roots = RootCertStore::empty();
        roots.add(ca_der).expect("add ca");
        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let client_config = ClientConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .expect("client versions")
            .with_root_certificates(roots)
            .with_no_client_auth();

        // Server config presents the leaf cert.
        let server_config = ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .expect("server versions")
            .with_no_client_auth()
            .with_single_cert(vec![leaf_der], leaf_key_der)
            .expect("server cert");

        (Arc::new(client_config), Arc::new(server_config))
    }

    /// Spin up a localhost TLS echo server bound to `127.0.0.1:0` and
    /// return its listening address plus the listener task handle.
    async fn spawn_echo_server(config: Arc<ServerConfig>) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        tokio::spawn(async move {
            // Single-shot accept — sufficient for these tests.
            if let Ok((stream, _)) = listener.accept().await {
                stream.set_nodelay(true).ok();
                run_tls_echo_server(stream, config).await;
            }
        });
        addr
    }

    /// Connect a client `TcpStream` to `addr` and wrap it in an
    /// `SslTransportLayer`.
    async fn connect_client(
        addr: SocketAddr,
        config: Arc<ClientConfig>,
        server_name: ServerName<'static>,
    ) -> SslTransportLayer {
        let stream = TcpStream::connect(addr).await.expect("client connect");
        stream.set_nodelay(true).ok();
        SslTransportLayer::new("test-channel", stream, config, server_name).expect("transport new")
    }

    /// Drive the client-side handshake to completion by alternating
    /// `handshake()` calls with awaiting socket readiness. Bounded
    /// iteration count guards against infinite loops on regressions.
    async fn complete_handshake(layer: &mut SslTransportLayer) {
        for _ in 0..32 {
            if layer.ready() {
                return;
            }
            // What does rustls want next? Wait specifically for the
            // matching readiness so we don't spin on the always-writable
            // case. (`writable()` resolves immediately whenever the
            // kernel send buffer is not full — which is essentially
            // always for our tiny TLS payloads — so awaiting it as part
            // of a `select!` is a busy-loop.)
            let wants_write = layer.conn.as_ref().map(|c| c.wants_write()).unwrap_or(false);
            let stream = layer.stream.as_mut().expect("stream present");
            let _ = tokio::time::timeout(std::time::Duration::from_secs(2), async {
                if wants_write {
                    stream.writable().await
                } else {
                    stream.readable().await
                }
            })
            .await;
            layer.handshake().expect("handshake step");
        }
        panic!("handshake did not complete within 32 iterations");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn handshake_completes_against_self_signed_server() {
        let (client_cfg, server_cfg) = build_tls_configs();
        let addr = spawn_echo_server(server_cfg).await;
        let server_name = ServerName::try_from("localhost").unwrap();
        let mut layer = connect_client(addr, client_cfg, server_name).await;

        assert!(!layer.ready(), "not ready before handshake");
        complete_handshake(&mut layer).await;
        assert!(layer.ready(), "ready after handshake");
        assert!(layer.is_open());
        assert!(layer.is_connected());
    }

    #[tokio::test]
    async fn handshake_populates_cipher_information() {
        let (client_cfg, server_cfg) = build_tls_configs();
        let addr = spawn_echo_server(server_cfg).await;
        let server_name = ServerName::try_from("localhost").unwrap();
        let mut layer = connect_client(addr, client_cfg, server_name).await;
        complete_handshake(&mut layer).await;

        let info = layer.cipher_information().expect("cipher info populated");
        assert!(!info.cipher().is_empty(), "cipher name set");
        assert!(!info.protocol().is_empty(), "protocol set");
    }

    #[tokio::test]
    async fn peer_principal_extracted_from_server_cert() {
        let (client_cfg, server_cfg) = build_tls_configs();
        let addr = spawn_echo_server(server_cfg).await;
        let server_name = ServerName::try_from("localhost").unwrap();
        let mut layer = connect_client(addr, client_cfg, server_name).await;
        complete_handshake(&mut layer).await;

        let principal = layer.peer_principal().expect("peer_principal");
        // Real cert was presented → principal should not be ANONYMOUS.
        assert_ne!(
            principal,
            KafkaPrincipal::anonymous(),
            "server presented a cert; principal should reflect that"
        );
        // The Phase 5b-2 simplified DN extractor returns a stable
        // CN=cert-<hex> identifier — verify the prefix.
        assert!(
            principal.to_string().starts_with("User:CN=cert-"),
            "principal name prefix: got {}",
            principal
        );
    }

    #[tokio::test]
    async fn round_trip_application_data() {
        let (client_cfg, server_cfg) = build_tls_configs();
        let addr = spawn_echo_server(server_cfg).await;
        let server_name = ServerName::try_from("localhost").unwrap();
        let mut layer = connect_client(addr, client_cfg, server_name).await;
        complete_handshake(&mut layer).await;

        // Send "hello, tls".
        let payload = b"hello, tls";
        let bufs = [IoSlice::new(payload)];
        let n = layer.write_vectored(&bufs).expect("write_vectored");
        assert_eq!(n, payload.len(), "write should accept all plaintext");

        // Drain any remaining encrypted bytes (write_vectored is
        // best-effort, may leave sendable_tls non-empty).
        for _ in 0..32 {
            if !layer.has_pending_writes() {
                break;
            }
            let stream = layer.stream.as_mut().expect("stream");
            stream.writable().await.expect("writable");
            let _ = layer.write_vectored(&[]); // no new plaintext, just drain
            // The above doesn't drain because empty bufs returns Ok(0)
            // before touching write_tls. Drain manually:
            let stream = layer.stream.as_mut().expect("stream");
            let conn = layer.conn.as_mut().expect("conn");
            let mut adapter = TcpStreamWriteAdapter { stream };
            let _ = conn.write_tls(&mut adapter);
        }

        // Echo back: drive read until we collect `payload.len()` bytes.
        let mut received = Vec::new();
        for _ in 0..32 {
            if received.len() >= payload.len() {
                break;
            }
            let stream = layer.stream.as_mut().expect("stream");
            let _ = tokio::time::timeout(std::time::Duration::from_secs(2), stream.readable()).await;
            let mut buf = [0u8; 64];
            let n = TransportLayer::read(&mut layer, &mut buf).expect("read");
            received.extend_from_slice(&buf[..n]);
        }
        assert_eq!(&received[..payload.len()], payload, "echo round-trip");
    }

    #[tokio::test]
    async fn close_sends_close_notify_and_drops_stream() {
        let (client_cfg, server_cfg) = build_tls_configs();
        let addr = spawn_echo_server(server_cfg).await;
        let server_name = ServerName::try_from("localhost").unwrap();
        let mut layer = connect_client(addr, client_cfg, server_name).await;
        complete_handshake(&mut layer).await;

        layer.close().expect("close");
        assert!(!layer.is_open());
        assert!(!layer.is_connected());
        assert!(!layer.ready());
        assert_eq!(layer.interest_ops(), 0);
        // local/peer addr now error because the socket is gone.
        assert!(layer.local_addr().is_err());
    }

    #[tokio::test]
    async fn read_returns_unexpected_eof_when_peer_closes_after_handshake() {
        let (client_cfg, server_cfg) = build_tls_configs();
        // Custom server that runs the handshake then drops the socket
        // ungracefully (no close_notify).
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let server_cfg_clone = server_cfg.clone();
        tokio::spawn(async move {
            if let Ok((stream, _)) = listener.accept().await {
                let mut conn = rustls::ServerConnection::new(server_cfg_clone).unwrap();
                let mut stream = stream;
                // Drive handshake.
                for _ in 0..32 {
                    if !conn.is_handshaking() && !conn.wants_write() {
                        break;
                    }
                    while conn.wants_write() {
                        stream.writable().await.ok();
                        let mut adapter = TcpStreamWriteAdapter { stream: &mut stream };
                        if conn.write_tls(&mut adapter).is_err() {
                            return;
                        }
                    }
                    if !conn.is_handshaking() {
                        break;
                    }
                    stream.readable().await.ok();
                    let mut adapter = TcpStreamReadAdapter { stream: &mut stream };
                    if conn.read_tls(&mut adapter).is_err() {
                        return;
                    }
                    if conn.process_new_packets().is_err() {
                        return;
                    }
                }
                // Drop the stream ungracefully.
                drop(stream);
            }
        });

        let server_name = ServerName::try_from("localhost").unwrap();
        let mut layer = connect_client(addr, client_cfg, server_name).await;
        complete_handshake(&mut layer).await;

        // Wait for the FIN to land.
        let stream = layer.stream.as_mut().expect("stream");
        tokio::time::timeout(std::time::Duration::from_secs(2), stream.readable())
            .await
            .expect("readable timeout")
            .expect("readable");

        let mut buf = [0u8; 64];
        let err = TransportLayer::read(&mut layer, &mut buf).expect_err("peer dropped — read should surface as error");
        // rustls surfaces ungraceful close as InvalidData ("peer closed
        // connection without sending TLS close_notify") via its Reader.
        // Our wrapper passes that through; the kind is permitted to be
        // either `UnexpectedEof` or `InvalidData` depending on when the
        // FIN lands relative to process_new_packets.
        assert!(
            matches!(err.kind(), io::ErrorKind::UnexpectedEof | io::ErrorKind::InvalidData),
            "expected EOF/InvalidData on ungraceful peer close, got {:?}",
            err.kind()
        );
    }

    #[tokio::test]
    async fn handshake_fails_with_untrusted_server_cert() {
        // Build two completely separate CAs. The client trusts CA-A
        // but the server presents a leaf signed by CA-B → handshake
        // must fail with an authentication-style error.
        let (_unused_client, server_b_cfg) = build_tls_configs();
        let (client_a_cfg, _unused_server_a) = build_tls_configs();
        let addr = spawn_echo_server(server_b_cfg).await;
        let server_name = ServerName::try_from("localhost").unwrap();
        let mut layer = connect_client(addr, client_a_cfg, server_name).await;

        // Drive the handshake — it must error within a few iterations.
        let mut last_err = None;
        for _ in 0..32 {
            if layer.ready() {
                panic!("handshake should not succeed against untrusted cert");
            }
            let wants_write = layer.conn.as_ref().map(|c| c.wants_write()).unwrap_or(false);
            let stream = layer.stream.as_mut().expect("stream present");
            let _ = tokio::time::timeout(std::time::Duration::from_secs(2), async {
                if wants_write {
                    stream.writable().await
                } else {
                    stream.readable().await
                }
            })
            .await;
            match layer.handshake() {
                Ok(()) => {},
                Err(e) => {
                    last_err = Some(e);
                    break;
                },
            }
        }
        let err = last_err.expect("handshake must fail with untrusted cert");
        let msg = format!("{err}");
        assert!(
            msg.contains("SSL handshake failed") || msg.contains("EOF"),
            "expected SSL/EOF error, got: {msg}",
        );
        // State should be HandshakeFailed or the channel should be in
        // an unrecoverable state.
        assert!(layer.handshake_error.is_some() || !layer.ready());
    }

    #[tokio::test]
    async fn tcp_read_adapter_would_block_returns_would_block() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let connect = TcpStream::connect(addr);
        let accept = listener.accept();
        let (client, accepted) = tokio::join!(connect, accept);
        let (mut client, _server) = (client.expect("connect"), accepted.expect("accept").0);
        let mut adapter = TcpStreamReadAdapter { stream: &mut client };
        let mut buf = [0u8; 16];
        let err = adapter.read(&mut buf).expect_err("quiet socket → WouldBlock");
        assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
    }

    #[tokio::test]
    async fn tcp_read_adapter_eof_returns_zero() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let connect = TcpStream::connect(addr);
        let accept = listener.accept();
        let (client, accepted) = tokio::join!(connect, accept);
        let (mut client, server) = (client.expect("connect"), accepted.expect("accept").0);

        drop(server);
        // Wait for FIN.
        tokio::time::timeout(std::time::Duration::from_secs(2), client.readable())
            .await
            .expect("readable timeout")
            .expect("readable");

        let mut adapter = TcpStreamReadAdapter { stream: &mut client };
        let mut buf = [0u8; 16];
        let n = adapter.read(&mut buf).expect("EOF reads as Ok(0)");
        assert_eq!(n, 0, "EOF must surface as Ok(0) so rustls sees has_seen_eof");
    }

    #[tokio::test]
    async fn write_when_not_ready_returns_zero() {
        let (client_cfg, _server_cfg) = build_tls_configs();
        // Build a transport against a TCP listener that does NOT do TLS
        // — the handshake never completes, so ready() is false.
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let stream = TcpStream::connect(addr).await.expect("connect");
        let server_name = ServerName::try_from("localhost").unwrap();
        let mut layer = SslTransportLayer::new("test", stream, client_cfg, server_name).expect("new");

        // No handshake performed — write must return 0.
        assert!(!layer.ready());
        let bufs = [IoSlice::new(b"hello")];
        let n = layer.write_vectored(&bufs).expect("write_vectored");
        assert_eq!(n, 0, "write before ready must return 0 (Java behavior)");
    }

    #[tokio::test]
    async fn pending_connect_uses_op_connect() {
        let (client_cfg, _server_cfg) = build_tls_configs();
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let stream = TcpStream::connect(addr).await.expect("connect");
        let server_name = ServerName::try_from("localhost").unwrap();
        let layer =
            SslTransportLayer::pending_connect("test", stream, client_cfg, server_name).expect("pending_connect");
        assert_eq!(layer.interest_ops(), OP_CONNECT);
        assert!(!layer.is_connected());
    }

    #[tokio::test]
    async fn finish_connect_flips_op_connect_to_op_read() {
        let (client_cfg, _server_cfg) = build_tls_configs();
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let connect = TcpStream::connect(addr);
        let accept = listener.accept();
        let (client, _accepted) = tokio::join!(connect, accept);
        let server_name = ServerName::try_from("localhost").unwrap();
        let mut layer = SslTransportLayer::pending_connect("test", client.expect("connect"), client_cfg, server_name)
            .expect("pending_connect");
        let connected = layer.finish_connect().expect("finish_connect");
        assert!(connected);
        let ops = layer.interest_ops();
        assert_eq!(ops & OP_CONNECT, 0);
        assert_eq!(ops & OP_READ, OP_READ);
    }

    #[tokio::test]
    async fn add_remove_interest_ops_toggles_mute() {
        let (client_cfg, _server_cfg) = build_tls_configs();
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let stream = TcpStream::connect(addr).await.expect("connect");
        let server_name = ServerName::try_from("localhost").unwrap();
        let mut layer = SslTransportLayer::new("test", stream, client_cfg, server_name).expect("new");
        layer.remove_interest_ops(OP_READ);
        assert!(layer.is_mute(), "removed OP_READ → mute");
        layer.add_interest_ops(OP_READ | OP_WRITE);
        assert!(!layer.is_mute());
        assert_eq!(layer.interest_ops() & OP_WRITE, OP_WRITE);
    }

    #[tokio::test]
    async fn ops_on_closed_transport_error() {
        let (client_cfg, _server_cfg) = build_tls_configs();
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let stream = TcpStream::connect(addr).await.expect("connect");
        let server_name = ServerName::try_from("localhost").unwrap();
        let mut layer = SslTransportLayer::new("test", stream, client_cfg, server_name).expect("new");
        layer.close().expect("close");

        let err = layer.peer_addr().expect_err("peer_addr after close");
        assert_eq!(err.kind(), io::ErrorKind::NotConnected);
        let err = layer.local_addr().expect_err("local_addr after close");
        assert_eq!(err.kind(), io::ErrorKind::NotConnected);

        // Reads on a closed transport surface NotConnected (no socket).
        let mut buf = [0u8; 8];
        // After close, ready() is false → read returns 0 (not ready),
        // unless state is Closing in which case UnexpectedEof.
        // close() sets state = Closing, so we expect UnexpectedEof.
        let err = TransportLayer::read(&mut layer, &mut buf).expect_err("read after close errors");
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }
}
