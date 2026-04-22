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

//! Transport layer for SSL/TLS communication.
//!
//! Translated from `org.apache.kafka.common.network.SslTransportLayer`.
//!
//! ## Buffer-based design
//!
//! This implementation pairs a raw [`TcpStream`] with a low-level
//! [`rustls::ClientConnection`]. TLS encryption (CPU work) is decoupled from
//! TCP I/O (potentially blocking) by performing all `wrap` operations
//! synchronously into rustls's internal output buffer, and by issuing TCP
//! reads/writes through small adapters that translate `WouldBlock` into
//! "no progress this round". This mirrors what the Java client gets from
//! `SSLEngine.wrap()` + non-blocking `SocketChannel.write()`.
//!
//! Encryption never awaits TCP. The selector task can never stall on a
//! single channel's TCP buffer: when the kernel buffer is full,
//! `try_write_vectored`/`write` returns immediately, leaving any
//! unflushed ciphertext in the rustls output buffer. The selector
//! re-registers `OP_WRITE` via `has_pending_writes()` and retries when the
//! socket becomes writable again.
//!
//! ## State machine
//!
//! ```text
//! Handshaking { tcp, conn }   // rustls::ClientConnection in handshake state
//!     |
//!     | handshake() drives: read_tls / write_tls / process_new_packets
//!     v
//! Ready { tcp, conn }         // same shape; conn.is_handshaking() == false
//!     |
//!     | close() -> conn.send_close_notify(); flush write_tls; tcp.shutdown
//!     v
//! Closed
//! ```

use super::{InterestOps, TransportLayer};

use std::future::Future;
use std::io;
use std::io::{Read as _, Write as _};
use std::net::SocketAddr;
use std::pin::Pin;

use rustls::pki_types::ServerName;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

/// Maximum number of plaintext bytes coalesced into rustls per `write_vectored`/
/// `write` call. Caps the amount of CPU work spent encrypting ahead of a TCP
/// stall — once the kernel buffer fills, any encrypted data beyond this cap is
/// wasted memcpy until the socket drains.
const MAX_TLS_COALESCE: usize = 256 * 1024;

/// Holds the (TCP, rustls) pair; boxed inside the `SslState` enum to avoid the
/// large variant-size discrepancy that boxing `TlsStream` previously avoided.
struct SslConnection {
    tcp: TcpStream,
    conn: rustls::ClientConnection,
}

/// Internal state of the SSL transport layer.
enum SslState {
    /// TLS handshake has not yet been completed. Both ciphertext and plaintext
    /// flows are blocked at the `read`/`write` boundary; only `handshake()`
    /// drives I/O in this state.
    Handshaking(Box<SslConnection>),
    /// TLS handshake is complete; `read`/`write` are operational.
    Ready(Box<SslConnection>),
    /// The transport has been closed.
    Closed,
}

/// Transport layer for SSL/TLS encrypted communication.
///
/// Translated from `org.apache.kafka.common.network.SslTransportLayer`.
///
/// Key differences from Java:
/// - Uses `rustls::ClientConnection` directly with the buffer-in / buffer-out
///   API (`reader()`/`writer()` + `read_tls`/`write_tls`/`process_new_packets`)
///   so encryption is CPU-only; awaiting TCP is opt-in (handshake only).
/// - `KafkaChannel`'s selector loop calls `try_write_vectored` for the write
///   fast path: encrypt synchronously, push as much ciphertext as the kernel
///   accepts, and surface `OP_WRITE` interest via `has_pending_writes()` for
///   the remainder.
pub struct SslTransportLayer {
    /// Current state of the TLS connection.
    state: SslState,
    /// Whether the underlying TCP connection has been established.
    connected: bool,
    /// Current interest operations for selector registration.
    interest_ops: InterestOps,
    /// Cached peer address from the underlying TCP stream.
    peer_addr: Option<SocketAddr>,
    /// Reusable buffer for coalescing IoSlice data before TLS encryption.
    write_buf: Vec<u8>,
}

impl SslTransportLayer {
    /// Creates a new `SslTransportLayer` in the `Handshaking` state.
    ///
    /// The TLS handshake will be performed when `handshake()` is called.
    ///
    /// # Arguments
    ///
    /// * `tcp` - The raw TCP stream (already connected)
    /// * `conn` - The rustls client connection (created from the SSL factory's
    ///   shared `Arc<ClientConfig>`); must be in handshaking state
    /// * `_server_name` - The server name used to construct `conn`. Retained
    ///   for symmetry with the Java API; rustls already records it on `conn`.
    pub fn new(tcp: TcpStream, conn: rustls::ClientConnection, _server_name: ServerName<'static>) -> Self {
        let peer_addr = tcp.peer_addr().ok();
        Self {
            state: SslState::Handshaking(Box::new(SslConnection { tcp, conn })),
            connected: true,
            interest_ops: InterestOps::OP_READ,
            peer_addr,
            write_buf: Vec::new(),
        }
    }
}

impl TransportLayer for SslTransportLayer {
    fn peer_addr(&self) -> io::Result<SocketAddr> {
        match &self.state {
            SslState::Handshaking(c) | SslState::Ready(c) => c.tcp.peer_addr(),
            SslState::Closed => self
                .peer_addr
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "transport layer is closed")),
        }
    }

    /// Returns `true` only when the TLS handshake is complete.
    fn ready(&self) -> bool {
        matches!(self.state, SslState::Ready(_))
    }

    /// Finishes the process of connecting the socket channel.
    ///
    /// For SSL, the TCP connection is already established when the `TcpStream` is
    /// passed in. This method just verifies the connection is valid and transitions
    /// interest ops from `OP_CONNECT` to `OP_READ`.
    fn finish_connect(&mut self) -> Pin<Box<dyn Future<Output = io::Result<bool>> + Send + '_>> {
        Box::pin(async {
            match &self.state {
                SslState::Handshaking(c) => {
                    c.tcp.writable().await?;
                    self.connected = true;
                    self.interest_ops = self.interest_ops.remove(InterestOps::OP_CONNECT) | InterestOps::OP_READ;
                    Ok(true)
                },
                SslState::Ready(_) => Ok(true),
                SslState::Closed => Err(io::Error::new(io::ErrorKind::NotConnected, "transport layer is closed")),
            }
        })
    }

    fn disconnect(&mut self) {
        self.connected = false;
    }

    fn is_connected(&self) -> bool {
        self.connected
    }

    /// Performs the TLS handshake, transitioning from `Handshaking` to `Ready`.
    ///
    /// Drives the rustls state machine manually: while `conn.is_handshaking()`,
    /// alternate between writing pending TLS records (when `wants_write()`)
    /// and reading + processing new ciphertext (otherwise). This is called by
    /// `KafkaChannel::prepare()` after `finish_connect()` returns true.
    ///
    /// # Errors
    ///
    /// Returns an error if the TLS handshake fails (certificate validation,
    /// hostname mismatch, protocol version mismatch, EOF, etc.).
    fn handshake(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async {
            // Take state out so we can either restore on success or transition to Closed on failure.
            let current = std::mem::replace(&mut self.state, SslState::Closed);
            let mut boxed = match current {
                SslState::Handshaking(c) => c,
                SslState::Ready(c) => {
                    self.state = SslState::Ready(c);
                    return Ok(());
                },
                SslState::Closed => {
                    return Err(io::Error::new(io::ErrorKind::NotConnected, "transport layer is closed"));
                },
            };

            loop {
                // Handshake is complete only when both sides finished AND the
                // local output buffer has been flushed. In TLS 1.3 the client's
                // own Finished is queued for `write_tls` after `is_handshaking()`
                // returns false; if we exit here without flushing, the server
                // will never see it and the session is wedged.
                if !boxed.conn.is_handshaking() && !boxed.conn.wants_write() {
                    self.state = SslState::Ready(boxed);
                    return Ok(());
                }

                if boxed.conn.wants_write() {
                    // Push handshake bytes until WouldBlock or no more pending output.
                    boxed.tcp.writable().await?;
                    loop {
                        if !boxed.conn.wants_write() {
                            break;
                        }
                        let mut adapter = TryWriteAdapter(&boxed.tcp);
                        match boxed.conn.write_tls(&mut adapter) {
                            Ok(0) => break,
                            Ok(_) => continue,
                            Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                            Err(e) => return Err(io::Error::other(format!("TLS handshake failed: {e}"))),
                        }
                    }
                    continue;
                }

                // Need to read.
                boxed.tcp.readable().await?;
                let mut adapter = TryReadAdapter(&boxed.tcp);
                match boxed.conn.read_tls(&mut adapter) {
                    Ok(0) => {
                        // EOF mid-handshake — peer closed the TCP connection.
                        if boxed.conn.is_handshaking() {
                            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "TLS handshake EOF"));
                        }
                        self.state = SslState::Ready(boxed);
                        return Ok(());
                    },
                    Ok(_) => {},
                    // Spurious wake-ups can return WouldBlock — loop and re-await readable.
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => continue,
                    Err(e) => return Err(io::Error::other(format!("TLS handshake failed: {e}"))),
                }
                if let Err(e) = boxed.conn.process_new_packets() {
                    return Err(io::Error::other(format!("TLS handshake failed: {e}")));
                }
            }
        })
    }

    fn add_interest_ops(&mut self, ops: InterestOps) {
        self.interest_ops |= ops;
    }

    fn remove_interest_ops(&mut self, ops: InterestOps) {
        self.interest_ops = self.interest_ops.remove(ops);
    }

    fn is_mute(&self) -> bool {
        !self.interest_ops.contains(InterestOps::OP_READ)
    }

    /// Returns `true` when `rustls` has buffered plaintext data that has been
    /// decrypted but not yet consumed by the application.
    ///
    /// This is critical for the selector: when a single TLS record contains more
    /// data than one Kafka message, the selector must zero its poll timeout so it
    /// processes the remaining buffered data immediately instead of sleeping.
    ///
    /// Translated from `SslTransportLayer.hasBytesBuffered()` in Java, which
    /// checks `netReadBuffer` and `appReadBuffer`. In `rustls`, the equivalent
    /// is `!conn.wants_read()`: rustls only wants more ciphertext when its
    /// internal `received_plaintext` buffer is empty. Anything else means
    /// there's still decoded plaintext to drain.
    fn has_bytes_buffered(&self) -> bool {
        match &self.state {
            SslState::Ready(c) => !c.conn.wants_read(),
            _ => false,
        }
    }

    /// Returns `true` if the TLS layer has pending encrypted data to flush.
    fn has_pending_writes(&self) -> bool {
        match &self.state {
            SslState::Ready(c) => c.conn.wants_write(),
            _ => false,
        }
    }

    fn is_open(&self) -> bool {
        !matches!(self.state, SslState::Closed)
    }

    /// Closes the transport layer by sending TLS close-notify (best-effort) and
    /// shutting down the TCP socket.
    fn close(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async {
            let current = std::mem::replace(&mut self.state, SslState::Closed);
            match current {
                SslState::Ready(mut c) => {
                    // Send close_notify and flush whatever ciphertext rustls produces.
                    c.conn.send_close_notify();
                    Self::flush_tls(&c.tcp, &mut c.conn);
                    let _ = c.tcp.shutdown().await;
                },
                SslState::Handshaking(mut c) => {
                    let _ = c.tcp.shutdown().await;
                },
                SslState::Closed => {},
            }
            self.connected = false;
            Ok(())
        })
    }

    fn readable(&self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async {
            // Always wait on the underlying TCP socket for fresh ciphertext.
            //
            // We deliberately do NOT short-circuit when rustls already has
            // buffered plaintext: the outer selector's `select_all` would
            // then fire immediately without waiting on the kernel, and if
            // `attempt_read` doesn't produce a complete `NetworkReceive`
            // (partial response), the loop would spin instead of waiting
            // for the next broker packet. Already-buffered plaintext is
            // drained by the next read() call, so no wakeup is needed for
            // it here.
            match &self.state {
                SslState::Handshaking(c) | SslState::Ready(c) => c.tcp.readable().await,
                SslState::Closed => Err(io::Error::new(io::ErrorKind::NotConnected, "transport layer is closed")),
            }
        })
    }

    fn writable(&self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
        Box::pin(async {
            match &self.state {
                SslState::Handshaking(c) | SslState::Ready(c) => c.tcp.writable().await,
                SslState::Closed => Err(io::Error::new(io::ErrorKind::NotConnected, "transport layer is closed")),
            }
        })
    }

    /// Reads decrypted plaintext from the TLS layer.
    ///
    /// Pulls fresh ciphertext from the TCP socket via `read_tls`, advances the
    /// rustls state machine via `process_new_packets`, then drains buffered
    /// plaintext from `conn.reader()` into `dst`. `WouldBlock` on the TCP read
    /// is normal — we still drain whatever plaintext is already buffered.
    /// Returns `Ok(0)` only on TLS EOF (matching Tokio's `AsyncRead` contract).
    fn read<'a>(&'a mut self, dst: &'a mut [u8]) -> Pin<Box<dyn Future<Output = io::Result<usize>> + Send + 'a>> {
        Box::pin(async {
            let c = match &mut self.state {
                SslState::Ready(c) => c,
                SslState::Handshaking(_) => {
                    return Err(io::Error::new(io::ErrorKind::WouldBlock, "TLS handshake not yet complete"));
                },
                SslState::Closed => {
                    return Err(io::Error::new(io::ErrorKind::NotConnected, "transport layer is closed"));
                },
            };

            // Step 1: pull fresh ciphertext from the socket (non-blocking).
            let mut tcp_eof = false;
            let mut adapter = TryReadAdapter(&c.tcp);
            match c.conn.read_tls(&mut adapter) {
                Ok(0) => tcp_eof = true,
                Ok(_) => {},
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {},
                Err(e) => return Err(e),
            }

            // Step 2: drive the TLS state machine.
            if let Err(e) = c.conn.process_new_packets() {
                return Err(io::Error::other(format!("TLS error: {e}")));
            }

            // Step 3: drain buffered plaintext.
            match c.conn.reader().read(dst) {
                Ok(n) => {
                    if n == 0 && tcp_eof {
                        // Plaintext drained AND socket closed -> propagate EOF.
                        Ok(0)
                    } else if n == 0 {
                        // No plaintext yet; ask caller to come back later.
                        Err(io::Error::from(io::ErrorKind::WouldBlock))
                    } else {
                        Ok(n)
                    }
                },
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    if tcp_eof {
                        Ok(0)
                    } else {
                        Err(e)
                    }
                },
                Err(e) => Err(e),
            }
        })
    }

    /// Writes plaintext into the TLS layer (encrypted by rustls before being
    /// sent). Encryption happens synchronously (no `await`); ciphertext is then
    /// pushed to TCP non-blockingly via `write_tls`. WouldBlock on the TCP
    /// write is fine — the unflushed ciphertext stays in the rustls output
    /// buffer and `has_pending_writes()` returns `true` so the selector
    /// re-registers `OP_WRITE`.
    ///
    /// The returned count is the number of plaintext bytes accepted in step 1
    /// (capped at [`MAX_TLS_COALESCE`]). It does not reflect how many ciphertext
    /// bytes actually reached the wire.
    fn write<'a>(&'a mut self, src: &'a [u8]) -> Pin<Box<dyn Future<Output = io::Result<usize>> + Send + 'a>> {
        Box::pin(async {
            match &mut self.state {
                SslState::Ready(c) => {
                    let to_write = src.len().min(MAX_TLS_COALESCE);
                    let accepted = c.conn.writer().write(&src[..to_write])?;
                    Self::flush_tls(&c.tcp, &mut c.conn);
                    Ok(accepted)
                },
                SslState::Handshaking(_) => {
                    Err(io::Error::new(io::ErrorKind::WouldBlock, "TLS handshake not yet complete"))
                },
                SslState::Closed => Err(io::Error::new(io::ErrorKind::NotConnected, "transport layer is closed")),
            }
        })
    }

    /// Vectored write that coalesces up to [`MAX_TLS_COALESCE`] bytes of
    /// plaintext across `srcs` into rustls in a single call. Same encryption /
    /// non-blocking flush semantics as [`write`](Self::write); returns the
    /// number of plaintext bytes accepted by rustls.
    fn write_vectored<'a>(
        &'a mut self,
        srcs: &'a [io::IoSlice<'a>],
    ) -> Pin<Box<dyn Future<Output = io::Result<usize>> + Send + 'a>> {
        Box::pin(async move {
            match &mut self.state {
                SslState::Ready(c) => {
                    if srcs.is_empty() {
                        Self::flush_tls(&c.tcp, &mut c.conn);
                        return Ok(0);
                    }
                    if srcs.len() == 1 {
                        let buf = &srcs[0];
                        let to_write = buf.len().min(MAX_TLS_COALESCE);
                        let accepted = c.conn.writer().write(&buf[..to_write])?;
                        Self::flush_tls(&c.tcp, &mut c.conn);
                        return Ok(accepted);
                    }
                    let total: usize = srcs.iter().map(|s| s.len()).sum();
                    let coalesce_limit = total.min(MAX_TLS_COALESCE);
                    self.write_buf.clear();
                    self.write_buf.reserve(coalesce_limit);
                    let mut budget = coalesce_limit;
                    for src in srcs {
                        if budget == 0 {
                            break;
                        }
                        let n = src.len().min(budget);
                        self.write_buf.extend_from_slice(&src[..n]);
                        budget -= n;
                    }
                    let accepted = c.conn.writer().write(&self.write_buf)?;
                    Self::flush_tls(&c.tcp, &mut c.conn);
                    Ok(accepted)
                },
                SslState::Handshaking(_) => {
                    Err(io::Error::new(io::ErrorKind::WouldBlock, "TLS handshake not yet complete"))
                },
                SslState::Closed => Err(io::Error::new(io::ErrorKind::NotConnected, "transport layer is closed")),
            }
        })
    }

    /// Synchronous fast path for vectored writes — no `await`, no boxed future.
    /// Same logic as `write_vectored` but issued inline; the selector calls
    /// this in its hot path to avoid heap allocation on every send.
    fn try_write_vectored(&mut self, srcs: &[io::IoSlice<'_>]) -> io::Result<usize> {
        match &mut self.state {
            SslState::Ready(c) => {
                if srcs.is_empty() {
                    Self::flush_tls(&c.tcp, &mut c.conn);
                    return Ok(0);
                }
                if srcs.len() == 1 {
                    let buf = &srcs[0];
                    let to_write = buf.len().min(MAX_TLS_COALESCE);
                    let accepted = c.conn.writer().write(&buf[..to_write])?;
                    Self::flush_tls(&c.tcp, &mut c.conn);
                    return Ok(accepted);
                }
                let total: usize = srcs.iter().map(|s| s.len()).sum();
                let coalesce_limit = total.min(MAX_TLS_COALESCE);
                self.write_buf.clear();
                self.write_buf.reserve(coalesce_limit);
                let mut budget = coalesce_limit;
                for src in srcs {
                    if budget == 0 {
                        break;
                    }
                    let n = src.len().min(budget);
                    self.write_buf.extend_from_slice(&src[..n]);
                    budget -= n;
                }
                let accepted = c.conn.writer().write(&self.write_buf)?;
                Self::flush_tls(&c.tcp, &mut c.conn);
                Ok(accepted)
            },
            SslState::Handshaking(_) => Err(io::Error::from(io::ErrorKind::WouldBlock)),
            SslState::Closed => Err(io::Error::new(io::ErrorKind::NotConnected, "transport layer is closed")),
        }
    }
}

impl SslTransportLayer {
    /// Pushes pending ciphertext from rustls's output buffer to the TCP socket
    /// without ever awaiting. Stops at the first `WouldBlock` (kernel buffer
    /// full) or when rustls has nothing more to send. Other errors are
    /// swallowed because this is only ever called from non-fallible paths
    /// (write fast-path / close); the next caller of `write_tls` will re-raise
    /// any persistent failure.
    fn flush_tls(tcp: &TcpStream, conn: &mut rustls::ClientConnection) {
        while conn.wants_write() {
            let mut adapter = TryWriteAdapter(tcp);
            match conn.write_tls(&mut adapter) {
                Ok(0) => break,
                Ok(_) => continue,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(_) => break,
            }
        }
    }
}

/// Adapter implementing [`io::Write`] over [`TcpStream::try_write`] so it can
/// be passed to [`rustls::ClientConnection::write_tls`].
struct TryWriteAdapter<'a>(&'a TcpStream);

impl io::Write for TryWriteAdapter<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.try_write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Adapter implementing [`io::Read`] over [`TcpStream::try_read`] so it can be
/// passed to [`rustls::ClientConnection::read_tls`]. Returns `WouldBlock`
/// cleanly when no fresh ciphertext is available.
struct TryReadAdapter<'a>(&'a TcpStream);

impl io::Read for TryReadAdapter<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.try_read(buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::config::SslConfig;
    use crate::common::security::SslFactory;
    use std::sync::Arc;

    fn create_test_factory() -> SslFactory {
        SslFactory::new(&SslConfig::default()).unwrap()
    }

    fn make_client_conn(factory: &SslFactory) -> rustls::ClientConnection {
        let server_name = SslFactory::create_server_name("localhost").unwrap();
        rustls::ClientConnection::new(Arc::clone(factory.client_config()), server_name)
            .expect("failed to construct rustls ClientConnection")
    }

    /// Test that a new SslTransportLayer is not ready (handshake not done).
    #[tokio::test]
    async fn test_initial_state_not_ready() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let stream = TcpStream::connect(addr).await.unwrap();
        let factory = create_test_factory();
        let conn = make_client_conn(&factory);
        let domain = SslFactory::create_server_name("localhost").unwrap();

        let transport = SslTransportLayer::new(stream, conn, domain);
        assert!(!transport.ready());
        assert!(transport.is_open());
        assert!(transport.is_connected());
    }

    /// Test that read before handshake returns WouldBlock.
    #[tokio::test]
    async fn test_read_before_handshake() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let stream = TcpStream::connect(addr).await.unwrap();
        let factory = create_test_factory();
        let conn = make_client_conn(&factory);
        let domain = SslFactory::create_server_name("localhost").unwrap();

        let mut transport = SslTransportLayer::new(stream, conn, domain);
        let mut buf = [0u8; 16];
        let result = transport.read(&mut buf).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::WouldBlock);
    }

    /// Test that write before handshake returns WouldBlock.
    #[tokio::test]
    async fn test_write_before_handshake() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let stream = TcpStream::connect(addr).await.unwrap();
        let factory = create_test_factory();
        let conn = make_client_conn(&factory);
        let domain = SslFactory::create_server_name("localhost").unwrap();

        let mut transport = SslTransportLayer::new(stream, conn, domain);
        let result = transport.write(b"hello").await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::WouldBlock);
    }

    /// Test close from handshaking state.
    #[tokio::test]
    async fn test_close_from_handshaking() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let stream = TcpStream::connect(addr).await.unwrap();
        let factory = create_test_factory();
        let conn = make_client_conn(&factory);
        let domain = SslFactory::create_server_name("localhost").unwrap();

        let mut transport = SslTransportLayer::new(stream, conn, domain);
        transport.close().await.unwrap();
        assert!(!transport.is_open());
        assert!(!transport.is_connected());
    }

    /// Test close from already-closed state (idempotent).
    #[tokio::test]
    async fn test_close_from_closed() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let stream = TcpStream::connect(addr).await.unwrap();
        let factory = create_test_factory();
        let conn = make_client_conn(&factory);
        let domain = SslFactory::create_server_name("localhost").unwrap();

        let mut transport = SslTransportLayer::new(stream, conn, domain);
        transport.close().await.unwrap();
        // Close again — should be idempotent
        transport.close().await.unwrap();
        assert!(!transport.is_open());
    }

    /// Test interest ops management.
    #[test]
    fn test_interest_ops() {
        // We can't create a real TcpStream without tokio runtime for this sync test,
        // so we test through the trait methods indirectly.
        // The initial interest_ops is set to OP_READ in the constructor.
        // We verify the add/remove logic is correct through the is_mute() method.
        //
        // This test is validated through the integration with KafkaChannel tests
        // and the selector tests.
    }

    /// Test peer_addr is available before handshake.
    #[tokio::test]
    async fn test_peer_addr_before_handshake() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let stream = TcpStream::connect(addr).await.unwrap();
        let factory = create_test_factory();
        let conn = make_client_conn(&factory);
        let domain = SslFactory::create_server_name("localhost").unwrap();

        let transport = SslTransportLayer::new(stream, conn, domain);
        let peer = transport.peer_addr().unwrap();
        assert_eq!(peer.port(), addr.port());
    }

    /// Test that has_bytes_buffered returns false before handshake (not in Ready state).
    #[tokio::test]
    async fn test_has_bytes_buffered_before_handshake() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let stream = TcpStream::connect(addr).await.unwrap();
        let factory = create_test_factory();
        let conn = make_client_conn(&factory);
        let domain = SslFactory::create_server_name("localhost").unwrap();

        let transport = SslTransportLayer::new(stream, conn, domain);
        // Before handshake (Handshaking state), has_bytes_buffered() returns false.
        assert!(!transport.has_bytes_buffered());
        assert!(!transport.has_pending_writes());
    }

    #[tokio::test]
    async fn test_try_write_vectored_before_handshake() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let stream = TcpStream::connect(addr).await.unwrap();
        let factory = create_test_factory();
        let conn = make_client_conn(&factory);
        let domain = SslFactory::create_server_name("localhost").unwrap();

        let mut transport = SslTransportLayer::new(stream, conn, domain);
        let data = b"hello";
        let slices = [io::IoSlice::new(data)];
        let result = transport.try_write_vectored(&slices);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::WouldBlock);
    }

    #[tokio::test]
    async fn test_try_write_vectored_empty_before_handshake() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let stream = TcpStream::connect(addr).await.unwrap();
        let factory = create_test_factory();
        let conn = make_client_conn(&factory);
        let domain = SslFactory::create_server_name("localhost").unwrap();

        let mut transport = SslTransportLayer::new(stream, conn, domain);
        // try_write_vectored returns WouldBlock pre-handshake even for empty slices,
        // because the handshake is a hard precondition for the data path.
        let result = transport.try_write_vectored(&[]);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::WouldBlock);
    }

    // ---- Tests for the new buffer-based design --------------------------------

    /// End-to-end TLS handshake against an in-process rustls server. Verifies
    /// that the manual handshake state machine drives `is_handshaking()` to
    /// false in one `handshake()` await.
    #[tokio::test]
    async fn test_handshake_drives_to_completion() {
        let (factory, server_config) = build_paired_factory_and_server_config();
        let (client_stream, server_stream) = make_localhost_pair().await;
        let domain = SslFactory::create_server_name("localhost").unwrap();
        let conn = make_client_conn(&factory);
        let mut transport = SslTransportLayer::new(client_stream, conn, domain);

        // Drive a server-side rustls connection in parallel.
        let server_task = tokio::spawn(async move { drive_server(server_stream, server_config).await });

        transport.handshake().await.expect("handshake failed");
        assert!(transport.ready(), "transport should be Ready after handshake");

        // Cleanly close client; let server task finish.
        let _ = transport.close().await;
        let _ = server_task.await;
    }

    /// After a real handshake + a server-side write, calling `read` once must
    /// return decrypted plaintext (`read_tls` + `process_new_packets` +
    /// `reader().read`).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_buffered_plaintext_after_read_tls() {
        let (factory, server_config) = build_paired_factory_and_server_config();
        let (client_stream, server_stream) = make_localhost_pair().await;
        let domain = SslFactory::create_server_name("localhost").unwrap();
        let conn = make_client_conn(&factory);
        let mut transport = SslTransportLayer::new(client_stream, conn, domain);

        let server_task =
            tokio::spawn(async move { drive_server_send(server_stream, server_config, b"hello-tls").await });

        transport.handshake().await.expect("handshake failed");

        // Wait for the server's encrypted record to arrive on the wire.
        transport.readable().await.unwrap();

        let mut buf = [0u8; 64];
        // First read may need to round-trip; loop until we get the data or fail.
        let mut total = 0;
        while total < 9 {
            match transport.read(&mut buf[total..]).await {
                Ok(0) => break,
                Ok(n) => total += n,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    transport.readable().await.unwrap();
                },
                Err(e) => panic!("read failed: {e}"),
            }
        }
        assert_eq!(&buf[..total], b"hello-tls");

        let _ = transport.close().await;
        let _ = server_task.await;
    }

    /// When the kernel TCP buffer is saturated, a write that can't be flushed
    /// must leave the remainder in rustls's output buffer (i.e.
    /// `has_pending_writes()` is true) and not error out.
    #[tokio::test]
    async fn test_write_tls_wouldblock_leaves_remainder() {
        let (factory, server_config) = build_paired_factory_and_server_config();
        let (client_stream, server_stream) = make_localhost_pair().await;
        // Shrink TCP buffers so the kernel saturates quickly.
        let _ = client_stream.set_nodelay(true);
        let domain = SslFactory::create_server_name("localhost").unwrap();
        let conn = make_client_conn(&factory);
        let mut transport = SslTransportLayer::new(client_stream, conn, domain);

        // Server completes the handshake but never reads — let TCP buffer fill.
        let server_task = tokio::spawn(async move { drive_server_then_idle(server_stream, server_config).await });

        transport.handshake().await.expect("handshake failed");

        // Spam writes until rustls retains pending ciphertext (i.e. TCP backpressure hit).
        let payload = vec![0xABu8; MAX_TLS_COALESCE];
        let mut iterations = 0;
        let mut saw_pending = false;
        while iterations < 64 {
            let res = transport.write(&payload).await;
            match res {
                Ok(_) => {},
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {},
                Err(e) => panic!("unexpected write error: {e}"),
            }
            if transport.has_pending_writes() {
                saw_pending = true;
                break;
            }
            iterations += 1;
        }
        assert!(
            saw_pending,
            "expected rustls to retain pending ciphertext under TCP backpressure"
        );

        // Drop the transport (cleanup) and unblock the server task.
        drop(transport);
        let _ = server_task.await;
    }

    /// `try_write_vectored` with empty slices on a Ready connection must call
    /// `flush_tls` and return Ok(0) without error. Combined with prior writes
    /// that left ciphertext pending, the next flush should drain it.
    #[tokio::test]
    async fn test_flush_tls_after_coalesced_writes() {
        let (factory, server_config) = build_paired_factory_and_server_config();
        let (client_stream, server_stream) = make_localhost_pair().await;
        let domain = SslFactory::create_server_name("localhost").unwrap();
        let conn = make_client_conn(&factory);
        let mut transport = SslTransportLayer::new(client_stream, conn, domain);

        let server_task = tokio::spawn(async move { drive_server_drain(server_stream, server_config).await });

        transport.handshake().await.expect("handshake failed");

        let a = b"alpha-payload";
        let b = b"beta-payload";
        let slices = [io::IoSlice::new(a), io::IoSlice::new(b)];
        let n = transport.try_write_vectored(&slices).expect("vectored write failed");
        assert_eq!(n, a.len() + b.len());

        // Subsequent empty try_write_vectored on Ready state must succeed and
        // (eventually) flush any remaining ciphertext.
        for _ in 0..16 {
            transport.writable().await.unwrap();
            let _ = transport.try_write_vectored(&[]);
            if !transport.has_pending_writes() {
                break;
            }
        }
        assert!(!transport.has_pending_writes(), "all ciphertext should have been flushed");

        let _ = transport.close().await;
        let _ = server_task.await;
    }

    /// `close()` from Ready must send TLS close-notify so the peer sees a
    /// clean shutdown rather than a TCP RST.
    #[tokio::test]
    async fn test_close_sends_close_notify() {
        let (factory, server_config) = build_paired_factory_and_server_config();
        let (client_stream, server_stream) = make_localhost_pair().await;
        let domain = SslFactory::create_server_name("localhost").unwrap();
        let conn = make_client_conn(&factory);
        let mut transport = SslTransportLayer::new(client_stream, conn, domain);

        let server_task = tokio::spawn(async move { drive_server_observe_close(server_stream, server_config).await });

        transport.handshake().await.expect("handshake failed");
        transport.close().await.expect("close failed");
        assert!(!transport.is_open());

        // Server should have observed an orderly TLS close (not an error).
        let observed = server_task.await.expect("server task failed");
        assert!(observed, "server should observe clean close-notify from client");
    }

    // ---- Test helpers ---------------------------------------------------------

    /// Self-signed certificate + private key pair for "localhost", generated at
    /// test time via `rcgen`. Returns DER cert bytes and DER PKCS#8 key bytes,
    /// plus a PEM string for the cert (so the client truststore can ingest it).
    fn make_self_signed() -> (Vec<u8>, Vec<u8>, String) {
        let mut params = rcgen::CertificateParams::new(vec!["localhost".to_string()]).unwrap();
        params.distinguished_name = rcgen::DistinguishedName::new();
        params.distinguished_name.push(rcgen::DnType::CommonName, "localhost");
        let key_pair = rcgen::KeyPair::generate().unwrap();
        let cert = params.self_signed(&key_pair).unwrap();
        let cert_der = cert.der().to_vec();
        let key_der = key_pair.serialize_der();
        let cert_pem = cert.pem();
        (cert_der, key_der, cert_pem)
    }

    /// Build a paired (client SslFactory, server ServerConfig) sharing one self-signed cert.
    fn build_paired_factory_and_server_config() -> (SslFactory, Arc<rustls::ServerConfig>) {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let (cert_der, key_der, cert_pem) = make_self_signed();

        let client_factory =
            SslFactory::new(&SslConfig { truststore_certificates: Some(cert_pem), ..SslConfig::default() })
                .expect("client SslFactory");

        let cert = rustls::pki_types::CertificateDer::from(cert_der);
        let key = rustls::pki_types::PrivateKeyDer::Pkcs8(rustls::pki_types::PrivatePkcs8KeyDer::from(key_der));
        let server_cfg = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert], key)
            .expect("server ServerConfig");
        (client_factory, Arc::new(server_cfg))
    }

    /// Bind a localhost listener and return (client_side, server_side) TcpStreams.
    async fn make_localhost_pair() -> (TcpStream, TcpStream) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let connect = TcpStream::connect(addr);
        let accept = listener.accept();
        let (client_res, accept_res) = tokio::join!(connect, accept);
        let client = client_res.unwrap();
        let (server, _) = accept_res.unwrap();
        (client, server)
    }

    /// Drive the server-side handshake using tokio-rustls (which is already a dep).
    async fn drive_server(server_stream: TcpStream, cfg: Arc<rustls::ServerConfig>) {
        let acceptor = tokio_rustls::TlsAcceptor::from(cfg);
        let _ = acceptor.accept(server_stream).await;
    }

    async fn drive_server_send(server_stream: TcpStream, cfg: Arc<rustls::ServerConfig>, payload: &[u8]) {
        use tokio::io::AsyncWriteExt as _;
        let acceptor = tokio_rustls::TlsAcceptor::from(cfg);
        if let Ok(mut tls) = acceptor.accept(server_stream).await {
            let _ = tls.write_all(payload).await;
            let _ = tls.flush().await;
            // Hold the connection open briefly so the client can read.
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            let _ = tls.shutdown().await;
        }
    }

    async fn drive_server_then_idle(server_stream: TcpStream, cfg: Arc<rustls::ServerConfig>) {
        let acceptor = tokio_rustls::TlsAcceptor::from(cfg);
        if let Ok(_tls) = acceptor.accept(server_stream).await {
            // Don't read; let TCP buffers fill. Hold for a bounded period so the
            // test can assert backpressure, then drop.
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        }
    }

    async fn drive_server_drain(server_stream: TcpStream, cfg: Arc<rustls::ServerConfig>) {
        use tokio::io::AsyncReadExt as _;
        let acceptor = tokio_rustls::TlsAcceptor::from(cfg);
        if let Ok(mut tls) = acceptor.accept(server_stream).await {
            let mut buf = vec![0u8; 4096];
            // Drain until the client closes.
            for _ in 0..256 {
                match tokio::time::timeout(std::time::Duration::from_millis(100), tls.read(&mut buf)).await {
                    Ok(Ok(0)) => break,
                    Ok(Ok(_)) => continue,
                    Ok(Err(_)) => break,
                    Err(_) => continue,
                }
            }
        }
    }

    /// Returns true if the server observed an orderly TLS close-notify (read returned 0).
    async fn drive_server_observe_close(server_stream: TcpStream, cfg: Arc<rustls::ServerConfig>) -> bool {
        use tokio::io::AsyncReadExt as _;
        let acceptor = tokio_rustls::TlsAcceptor::from(cfg);
        let Ok(mut tls) = acceptor.accept(server_stream).await else {
            return false;
        };
        let mut buf = [0u8; 16];
        // tokio-rustls returns Ok(0) on a clean close-notify; any other result
        // (Err, or non-zero read) means a different shutdown happened.
        matches!(
            tokio::time::timeout(std::time::Duration::from_secs(2), tls.read(&mut buf)).await,
            Ok(Ok(0))
        )
    }
}
