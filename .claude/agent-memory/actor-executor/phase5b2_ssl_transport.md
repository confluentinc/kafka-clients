---
name: Phase 5b-2 SslTransportLayer (rustls low-level)
description: Architectural decisions and gotchas for the rustls-based SSL transport translation
type: project
---

Phase 5b-2 landed `SslTransportLayer` in `src/common/network/ssl_transport_layer.rs` using raw `rustls::ClientConnection` (no `tokio_rustls`), implementing the same `TransportLayer` trait shape as `PlaintextTransportLayer`.

**Why:** The Java `SslTransportLayer` is the largest single file in the network module (~1063 LOC) and the only place where the Tokio↔Java NIO bridge gets thorny. Locking the architectural pattern here unblocks Phase 5b-3 (KafkaChannel) which needs a finished transport surface.

**How to apply (esp. for Phase 5b-3 / 5c / future SSL consumers):**

1. **State-machine collapse**: Java's `State { NOT_INITIALIZED, HANDSHAKE, HANDSHAKE_FAILED, POST_HANDSHAKE, READY, CLOSING }` collapses to 5 Rust variants. POST_HANDSHAKE is folded into Ready because rustls handles the TLSv1.3 post-handshake-message distinction internally — once `is_handshaking()` returns false, rustls has either drained or queued post-handshake state on its own.

2. **rustls API shape mapping**:
   - `SSLEngine.wrap()` → `ClientConnection::writer().write[_vectored]()` + `write_tls(&mut io::Write)`
   - `SSLEngine.unwrap()` → `read_tls(&mut io::Read)` + `process_new_packets()` + `reader().read()`
   - `SSLEngine.beginHandshake()` → no-op; rustls primes the handshake at `ClientConnection::new`
   - `SSLEngine.closeOutbound()` → `send_close_notify()`
   - `SSLSession::getCipherSuite/getProtocol` → `negotiated_cipher_suite()` + `protocol_version()`
   - `SSLSession::getPeerPrincipal` → `peer_certificates()` + custom DN extractor
   - `getDelegatedTask()` → not needed; rustls runs delegated work inside `process_new_packets()`

3. **Tokio↔rustls adapter (`TcpStreamReadAdapter`/`TcpStreamWriteAdapter`)**: rustls's `read_tls`/`write_tls` take `&mut dyn io::Read/Write`. The adapter wraps `tokio::net::TcpStream` via `try_read`/`try_write`. **Critical translation differs from `PlaintextTransportLayer`**:
   - For the rustls-facing adapter, `WouldBlock` is propagated as `Err(WouldBlock)` (NOT collapsed to `Ok(0)`) because rustls treats `Ok(0)` as EOF.
   - `Ok(0)` on non-empty buf is passed through (rustls reads it as `has_seen_eof = true`).
   - For the public `TransportLayer::read` surface, the same three-way Tokio rule applies as plaintext.

4. **Handshake driver loop pattern**: The `drive_handshake` loop has three phases per iteration: drain `wants_write` → bail to `Ready` if `!is_handshaking()` → `read_tls` + `process_new_packets`. Re-loop on inbound progress because TLS 1.3 frequently emits ClientFinished immediately after ServerFinished is unwrapped. Set `OP_WRITE` only when an outbound drain returns WouldBlock; clear it once drained.

5. **Test harness gotcha (BIG)**: When driving the handshake from a test that uses `tokio::select! { readable() | writable() }`, **`writable()` resolves immediately whenever the kernel send buffer is non-full** — i.e. essentially always for tiny TLS payloads. The select then returns immediately and you spin without ever blocking for inbound data. Fix: pick the readiness based on `conn.wants_write()` — wait for `writable()` only when there's pending TLS output, otherwise wait for `readable()`. Saved me 30 minutes of "why is the server not responding" debugging when the server *was* responding but the client's spin loop blew through 32 iterations in microseconds.

6. **Multi-thread runtime for handshake tests**: One handshake test uses `#[tokio::test(flavor = "multi_thread")]` for the in-process echo server. Single-threaded current_thread did not deadlock in this case once the readiness wait was fixed (test passes on default flavor), but the multi_thread flavor mirrors the Phase 5d production setup more closely — the spawned echo server runs concurrently with the client driver.

7. **DN extractor is intentionally simplified**: The current `parse_subject_dn` returns `CN=cert-<hex of first 32 bytes of DER>`. A real ASN.1 DN parser is deferred — for the Phase 5b-2 surface, the only consumer of `peer_principal` is logging; a stable, unique identifier per cert is sufficient. When Phase 5b-3+ surfaces the principal to ACL/auth code, swap in `x509-cert` or `x509-parser` (neither is currently a dep). The principal *prefix* (`User:CN=cert-`) is asserted in `peer_principal_extracted_from_server_cert` — change-detector if anyone tries to silently widen the surface.

8. **Cipher info harvesting (RESOLVED in Round 2 fixup `5b9420b`-style)**: rustls's `Debug` impl on `CipherSuite` is **non-IANA** for TLS 1.3 (`"TLS13_AES_128_GCM_SHA256"` instead of IANA's `"TLS_AES_128_GCM_SHA256"`); same for `ProtocolVersion` (`"TLSv1_3"` vs Java's `"TLSv1.3"`). Java's `SSLSession::getCipherSuite()` / `getProtocol()` always return the IANA names. We now route through `iana_cipher_name(CipherSuite) -> String` and `iana_protocol_name(ProtocolVersion) -> String` mapping helpers in `ssl_transport_layer.rs`. They cover the modern suites rustls negotiates by default with `aws_lc_rs` (5 TLS 1.3 + the ECDHE-RSA / ECDHE-ECDSA TLS 1.2 suites). Unknown values fall back to rustls Debug so a regression is grep-able in logs.

9. **`tokio_rustls` is in Cargo.toml but NOT used by the SSL transport**. It's there transitively (via the default features pulling `aws_lc_rs` which the SSL transport needs as the rustls crypto provider). When Phase 5b-3 lands, audit: if no other code path actually uses `tokio_rustls::TlsStream`, consider downgrading to `dep:rustls` + explicit aws-lc-rs feature on rustls itself, removing tokio_rustls entirely.

10. **PLAN.md was updated** to reflect the rustls-low-level decision (line 260): `tokio_rustls::client::TlsStream<TcpStream>` reference replaced with the rustls-low-level architecture statement. Future phases reading the plan will see the correct architectural directive without needing to consult agent memory.
