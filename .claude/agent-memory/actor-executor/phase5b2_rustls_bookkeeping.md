---
name: rustls vs Java SSLEngine bookkeeping bridges
description: Three patterns where the rustls API needs explicit bookkeeping calls to mirror Java SSLEngine behaviour the producer relies on
type: project
---

Java's `SSLEngine` and `SslTransportLayer` have implicit behaviours that rustls does not provide by default. These came up during Phase 5b-2 Round 2 review (Critic flagged 6 Suggestions; 4 fixed, 2 deferred). Three are reusable patterns when bridging rustls to a Java-style transport abstraction.

**Why:** When you accept a "default rustls API call" without bridging, the resulting transport quietly diverges from Java semantics in ways the producer/Selector relies on — bounded backpressure, accurate same-poll re-tick scheduling, IANA cipher reporting.

**How to apply (esp. for Phase 5c Selector and any future rustls-based SASL/IAM transport):**

1. **Plaintext sink must be bounded** (`ClientConnection::set_buffer_limit(Some(N))`):
   - Java's `SslTransportLayer.write(ByteBuffer src)` is bounded via the fixed-size `netWriteBuffer` (typically 16 KiB / one TLS record). When the wire is backed up Java's `write` returns 0 and the producer's `Sender` stops accumulating.
   - Rustls's `sendable_plaintext` queue is **unbounded by default**. Without `set_buffer_limit`, a slow broker can balloon many MB of buffered plaintext per channel before producer-side BufferPool admission control kicks in.
   - Apply `conn.set_buffer_limit(Some(64 * 1024))` immediately after `ClientConnection::new` in BOTH constructors (synchronous `new` and `pending_connect`). Once the queue is full, `writer().write_vectored` returns `Ok(n < total)` and the Selector applies backpressure analogous to Java's `netWriteBuffer.hasRemaining()` gate.
   - Sized at 64 KiB to comfortably hold a couple of TLS records' worth of pending plaintext. Same order of magnitude as rustls's internal `DEFAULT_BUFFER_LIMIT`.

2. **`has_bytes_buffered` must come from `IoState::plaintext_bytes_to_read`**, not `total_read > 0`:
   - Java's `updateBytesBuffered(madeProgress)` reflects whether buffered plaintext or unprocessed TLS bytes remain *after* this call so the next poll can deliver them without going to the network.
   - rustls API: `process_new_packets()` returns `Result<IoState, Error>`. `IoState::plaintext_bytes_to_read()` is the rustls-equivalent of Java's `appReadBuffer.position() != 0`.
   - The Phase 5c Selector reads `has_bytes_buffered()` exactly to schedule a same-poll re-tick of the channel. Returning `total_read > 0` produces:
     - false positives (we delivered some plaintext but the receive buffer is now empty — wakeup spam),
     - false negatives (we delivered 0 plaintext but rustls has another record queued — **stalls the channel**).
   - Bridge: in the read loop, capture `IoState::plaintext_bytes_to_read()` from each `process_new_packets` call, subtract whatever was just drained into `dst`, then OR with `madeProgress` to mirror Java's predicate exactly:
     ```rust
     let made_progress = read_from_network || total_read > 0;
     self.has_bytes_buffered = made_progress && last_plaintext_pending > 0;
     ```
   - **Pre-loop snapshot pitfall (Round 2 Issue 7)**: Mirror Java's pre-drain snapshot exactly — call `process_new_packets()` BEFORE the step-1 drain (`conn.reader().read(dst)`) so `last_plaintext_pending` is initialised from rustls's queue length, then subtract the step-1 drain count. **Why:** if step 1 fully fills `dst` from already-queued plaintext, the loop's top guard `if total_read == dst.len() { break; }` exits before any in-loop `process_new_packets` call — bookkeeping initialised inside the loop stays at 0 even though rustls has remaining plaintext. Java mirrors `appReadBuffer.position()` snapshotted at the top of `read(ByteBuffer)`. This is the exact early-exit-bypasses-bookkeeping pattern: when control flow has multiple drain sites and bookkeeping lives only on one, an early-exit path skips the bookkeeping. Either initialise the counter from a single pre-flight call, OR collapse drain sites into one path. **How to apply:** any time the Selector or the SSL layer maintains a "pending after this call" counter, audit *every* path that can return without falling through to the bookkeeping update — early-exit guards, error paths, the "no progress this iteration" break — and ensure the counter is either snapshot-correct on entry or unchanged-as-0 is the right answer. `process_new_packets` is documented as cheap when idle, so pre-flight calls have bounded steady-state cost.

3. **Cipher / protocol names are IANA in Java, rustls Debug in Rust**:
   - rustls `CipherSuite::TLS13_AES_128_GCM_SHA256` Debug-formats as `"TLS13_AES_128_GCM_SHA256"` (with `13_` infix). IANA name is `"TLS_AES_128_GCM_SHA256"` (no infix). Same idea for `ProtocolVersion::TLSv1_3` → `"TLSv1_3"` Debug vs `"TLSv1.3"` IANA.
   - Java's `SSLSession::getCipherSuite()` / `getProtocol()` always return IANA strings.
   - Bridge: pure helper functions `iana_cipher_name(CipherSuite) -> String` / `iana_protocol_name(ProtocolVersion) -> String`. Match-on-discriminant for the modern suites rustls negotiates by default (5 TLS 1.3 + ECDHE-RSA/ECDHE-ECDSA TLS 1.2). Fall back to rustls `Debug` for unknown values so a regression is grep-able in logs (better than empty string).

**Deferred to Phase 5c (carryover, not new):**
- `add_interest_ops`/`remove_interest_ops` semantics when `!ready` — Java throws `IllegalStateException`, Rust silently no-ops. Whether the trait should expose `Result<()>` is a Selector-time decision once the Selector has a concrete caller. The current silent gate on `is_open` prevents the most dangerous misuse (call after disconnect).
- `disconnect()` flips `is_open=false` (same divergence as PlaintextTransportLayer 5b-1 Comment 2). Filed in lockstep so when the underlying semantic is finally split (`key_valid: bool` + `socket_open: bool`, or by documenting on the trait), both transports are addressed together.

**Test-flake gotcha (macOS under load)**: The `read_returns_unexpected_eof_when_peer_closes_after_handshake` test asserts an EOF-style error after the test server drops its socket. Under heavy parallel test load on macOS, the kernel can deliver the peer's drop as a `ConnectionReset` (RST) rather than a clean `UnexpectedEof` (FIN). The test now accepts `UnexpectedEof | InvalidData | ConnectionReset` — all three are legitimate "peer is gone" signals, and the upper layer translates all of them to a channel-disconnected event. Apply the same matcher generosity in any future TLS test that depends on a specific error-kind for ungraceful peer close.
