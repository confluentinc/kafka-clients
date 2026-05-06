---
name: Phase-5b-2 review patterns — rustls↔SSLEngine bridge gotchas
description: TLS-specific review traps discovered translating Java SslTransportLayer to raw rustls::ClientConnection
type: project
---

Phase 5b-2 landed `SslTransportLayer` (1340 LOC Rust ← 1063 LOC Java) on
top of raw `rustls::ClientConnection` (deliberately *not* `tokio_rustls`).
The architectural mirror is good but several semantic edges between
Java's bounded-buffer SSLEngine and rustls's unbounded-by-default plaintext
queue surface as defects.

**Why:** all of these will recur in Phase 5b-3 (SslChannelBuilder),
Phase 5c (TLS Selector tests), and Phase 9 (SASL_SSL). Recording them
once means I don't re-discover them per phase.

**How to apply:**

1. **rustls's plaintext queue is unbounded by default.** Java's
   `sslEngine.wrap(src, netWriteBuffer)` is bounded by `netWriteBuffer`
   capacity (~16 KiB), so `bytesConsumed()` returns less than `src.remaining()`
   under network backpressure → the producer's `Sender` stops feeding
   plaintext. rustls's `ClientConnection::writer().write_vectored(bufs)`
   returns `Ok(sum_of_lens)` always (no bound). On a slow/blocked broker
   connection, plaintext queue can balloon to many MB before the producer's
   BufferPool quota kicks in. **Fix**: `conn.set_buffer_limit(Some(<n>))`
   at construction. **Always check** for this when reviewing TLS code:
   grep for `set_buffer_limit` and ensure it's set, not left at the unbounded
   default.

2. **`has_bytes_buffered()` semantic.** Java's `updateBytesBuffered(madeProgress)`
   sets `hasBytesBuffered = (netReadBuffer.position() != 0 || appReadBuffer.position() != 0)`
   when progress was made — i.e. "is there still queued data after this read?".
   The Rust simplification `has_bytes_buffered = total_read > 0` is
   asymmetrically wrong:
   - false-positive: delivered some plaintext, buffer now empty → spurious
     wakeup (perf hit).
   - **false-negative**: delivered 0 plaintext but rustls has another TLS
     record buffered → **missed wakeup**, channel stalls.
   The Selector in 5c uses `hasBytesBuffered()` to schedule same-poll re-tick
   of the channel — the false-negative is the load-bearing case. **The fix**
   is to capture `IoState` from `process_new_packets()` —
   `IoState::plaintext_bytes_to_read()` is the rustls-equivalent of
   `appReadBuffer.position() != 0`. Don't accept `total_read > 0` as
   sufficient.

3. **rustls API → SSLEngine API mapping (verified-good):**
   - `SSLEngine.wrap()` → `writer().write[_vectored]()` + `write_tls(io)`
   - `SSLEngine.unwrap()` → `read_tls(io)` + `process_new_packets()` + `reader().read()`
   - `SSLEngine.beginHandshake()` → no-op (rustls primes at `ClientConnection::new`)
   - `SSLEngine.closeOutbound()` → `send_close_notify()`
   - `SSLEngine.closeInbound()` → drop the connection (no direct equivalent)
   - `SSLSession.getCipherSuite/getProtocol` → `negotiated_cipher_suite()` / `protocol_version()`
   - `getDelegatedTask()` → no equivalent; rustls runs delegated work inside `process_new_packets()`
   The `closeInbound` test (`testSSLEngineCloseInboundInvokedOnClose`) is a
   JVM-mock-specific verification — defensible skip in Rust.

4. **Tokio↔rustls adapter has DIFFERENT WouldBlock translation than the
   public TransportLayer surface.** This is non-obvious:
   - `TransportLayer::read` (public): `WouldBlock` → `Ok(0)`, `Ok(0)` → `Err(UnexpectedEof)`
     (Java NIO contract).
   - `TcpStreamReadAdapter` (rustls-facing): `WouldBlock` → `Err(WouldBlock)`,
     `Ok(0)` → `Ok(0)` (rustls expects EOF as `Ok(0)` to set
     `has_seen_eof = true`).
   The two adapters look identical on the read path but the WouldBlock
   handling diverges. **Always verify both** when reviewing TLS read paths.

5. **`add_interest_ops`/`remove_interest_ops` Java behavior is
   exception-throwing, not silent no-op.** Java throws `IllegalStateException`
   if not ready, `CancelledKeyException` if key invalid. Silent no-op in Rust
   makes the public API tolerant of caller bugs — flag if the caller's gate
   isn't documented somewhere external.

6. **Cipher/protocol Debug format ≠ IANA name.** rustls Debug:
   `"TLS13_AES_256_GCM_SHA384"`, `"TLSv1_3"`. Java IANA:
   `"TLS_AES_256_GCM_SHA384"`, `"TLSv1.3"`. Cosmetic for logging but breaks
   string-equality assertions in tests / metadata-registry consumers.
   `format!("{:?}", suite.suite())` is convenient but not byte-faithful.

7. **Skipped Java tests audit.** For TLS-specific Mockito tests, walk through
   each:
   - `testGatheringWrite` / `testScatteringRead` → exercise multi-buffer
     dispatch (`read(ByteBuffer[], offset, length)`). Defensible skip if the
     Rust `TransportLayer` trait deliberately has only single-buffer `read`
     and single-call `write_vectored`.
   - `testHandshakeUnwrapContinuesUnwrappingOnNeedUnwrapAfterAllBytesRead`
     → tests SSLEngine's NEED_UNWRAP loop after socket EOF. rustls handles
     this internally inside `process_new_packets`. Defensible skip.
   - `testSSLEngineCloseInboundInvokedOnClose` → verifies the engine method
     was called. rustls has no equivalent method. Defensible skip.
   The integration tests (`testValidEndpointIdentification*`,
   `testClientAuthentication*`, `testServerKeystoreDynamicUpdate*`) genuinely
   require Selector / ChannelBuilder / NioEchoServer — defer to 5b-3/5c.

8. **Test-driver gotcha: `TcpStream::writable()` resolves immediately** when
   the kernel send buffer is non-full, which is *always* for tiny TLS
   payloads. A `tokio::select! { readable() | writable() }` becomes a busy
   loop. The fix: pick the await target based on `conn.wants_write()` —
   only await `writable()` when there's pending TLS output. The actor
   captured this in their memory; record it on my side too because future
   SSL test reviews will encounter the same trap.

9. **Self-signed cert test fixture (rcgen).** `build_tls_configs()` shape:
   - CA + leaf signed-by-CA pattern (mirrors Java's CertStores).
   - `aws_lc_rs` is the rustls crypto provider used; explicitly built via
     `Arc::new(rustls::crypto::aws_lc_rs::default_provider())`.
   - `with_safe_default_protocol_versions()` includes hostname verification
     by default (no `with_custom_certificate_verifier(NoopVerifier)`).
   - SANs cover `localhost` + `127.0.0.1` so the test can connect via either.
   Reusable for any future SSL test in 5b-3+.

10. **`peer_principal` for client-side.** When client config has
    `with_no_client_auth`, the peer cert returned via `peer_certificates()`
    is the *server's* leaf cert. The DN there is the server's cert subject
    (e.g. `CN=localhost` for the test fixture). For a client-only connection,
    the server's cert subject IS the principal. Don't confuse with
    "client principal" — that only exists when client auth is requested.
