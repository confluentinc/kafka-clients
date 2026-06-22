# Phase 29 — rustls `UnbufferedConnection` receive/send path (INVALIDATED)

**Milestone-8 / Phase-29** · Agent number **N = 29** · Status: **INVALIDATED
before implementation** (2026-06-11). Kept for the record.

## Why it was dropped

Source inspection of the vendored rustls 0.23.38
(`src/conn/unbuffered.rs:330-372`): the unbuffered API does **NOT** decrypt
in place. `ReadTraffic::next_record()` pops an **owned `Vec<u8>` chunk** from
the same internal `received_plaintext` `ChunkVecBuffer` that the buffered
`reader().read()` path drains; the `incoming_tls` parameter is held only
"for forwards compatibility; to support in-place decryption in the future"
(upstream TODO). The receive-path copy chain is therefore **identical** in
both APIs:

    kernel → TLS-framing buffer → per-record owned plaintext chunk
    (alloc + copy, rustls-internal, both modes) → our receive Vec

The intermediate per-record chunk is rustls's current floor in every API it
offers — that, not buffered-mode bookkeeping, is the structural delta vs
Java's `SSLEngine.unwrap` (which decrypts directly into the app's buffer).
The rewrite would buy only discard-policy/outgoing-buffer control (~0–1 pp)
for a ~1.4k-line state-machine rewrite of the historically fragile TLS
read/handshake path. Not worth it.

**Re-open only if** upstream rustls ships in-place decryption for the
unbuffered API (track the TODO in `unbuffered.rs`), at which point the
original analysis below applies and the win becomes the full intermediate
copy + per-record alloc (~2-3 pp).

---

*Original plan (premise invalidated) follows:*

## Why

Post-Phase-27 cloud profile (76.4% CPU @200k SASL_SSL latency-tuned):
`__GI___memcpy_sve` 3.7% (bg) + rustls `process_new_packets` 1.2% +
`ChunkVecBuffer` bookkeeping. The buffered rustls API hides a copy chain we
don't control:

1. kernel → rustls deframer buffer (`read_tls`, the unavoidable syscall copy)
2. in-place decrypt (cheap, hardware AES)
3. **deframer discard: `copy_within` shift of remaining buffered bytes —
   rustls's policy, invisible to us**
4. `reader().read(dst)` → memcpy plaintext → `NetworkReceive` Vec

With `UnbufferedClientConnection` the app owns both TLS buffers:

1. kernel → **our** `incoming_tls` buffer (same syscall)
2. `process_tls_records` decrypts in place; `AppDataRecord { payload: &[u8] }`
   borrows the buffer
3. payload appended straight into the `NetworkReceive` Vec (Phase-28's
   `try_read_append` already established the appending receive contract —
   this slots in as a new implementation of the same method)
4. discard under **our** policy: consume every record in the buffer, then one
   bulk `discard` — the shift moves only the (typically tiny) unconsumed tail

Net: step-3' equals step-4 (one inherent plaintext copy, same as Java
`SSLEngine.unwrap` into an app buffer — this phase makes the Rust shape *more*
faithful to Java's SSLEngine model, where the app owns `netReadBuffer` /
`appReadBuffer`); steps 3 (hidden shift) and rustls's internal buffer
growth/management go away. Honest estimate: **−2 to −3 pp**, plus RSS
predictability (we size the TLS buffers).

## Scope

Rewrite `src/common/network/ssl_transport_layer.rs` (~1.4k lines) from the
buffered `ClientConnection` API (`reader()`/`writer()`/`read_tls`/`write_tls`/
`process_new_packets`) to `UnbufferedClientConnection`
(`process_tls_records` state machine: `ReadTraffic` / `EncodeTlsData` /
`TransmitTlsData` / `BlockedHandshake` / `WriteTraffic` / `Closed`):

- handshake driving (today: read_tls/write_tls loop; after: encode/transmit/
  blocked states),
- `try_read` / `try_read_append` / async `read` (ReadTraffic record iteration),
- write path + vectored writes + `has_pending_writes` (WriteTraffic encrypt
  into our outgoing buffer),
- close (queue_close_notify + flush),
- `log_negotiated_params`, `has_bytes_buffered` equivalents (unconsumed
  records in `incoming_tls`).

Out of scope: SASL (sits above the transport), certificate/config plumbing
(`SslFactory` unchanged), the `TransportLayer` trait surface (unchanged —
Phase 28's `try_read_append` is the receive entry point).

## Risks / review focus

- The buffered API's `try_read` WouldBlock/EOF mapping took several phases to
  get right (Phase 19, the `tcp_eof`+`close_notify` matrix) — port the exact
  mapping and keep `test_try_read_buffered_plaintext_then_eof` +
  `test_try_read_append_limit_then_eof` green unchanged.
- Handshake interleaving with the selector's interest predicate
  (`channel_interest`: `in_handshake`, `has_pending_writes`) — the unbuffered
  state machine must surface the same signals or the Phase-23/24 invariants
  (no busy-spin, no stall) break.
- TLS record spanning the `incoming_tls` buffer end (partial record): keep a
  correctly-sized buffer (max record 16KB + header) and the
  `InsufficientSizeError` resize arm.
- KIP-848 join over TLS is the historically fragile path (join-stall
  root-cause doc) — integration-validate join + steady state on the rig
  before/after.

## Validation

Same matrix as Phase 28 (200k latency-tuned / 64KB / big-batch 200p / 5k
low-rate) + the SSL unit tests + a full join/consume integration pass.
