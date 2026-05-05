# Critic 0 — Phase 5a (Network primitives & framing) — Done

## Issue: `NetworkReceive::with_buffer` pre-fills size header, diverging from Java

- **File**: `src/common/network/network_receive.rs:77-89`
- **Severity**: Suggestion (Behavior Mismatch)
- **Java Reference**: `kafka/clients/src/main/java/org/apache/kafka/common/network/NetworkReceive.java:47-50`
- **Originating commit**: `2a31a08`

The Java constructor `NetworkReceive(String source, ByteBuffer buffer)` only sets
the payload buffer; the `size: ByteBuffer` is left as a fresh `allocate(4)` (i.e.
`size.position() == 0`, `size.remaining() == 4`). The Rust equivalent
`with_buffer` synthesised the size header eagerly, setting `size_pos = 4` and
`requested_buffer_size = buffer.len()`. This caused two observable divergences:

1. **`complete()` returned true in Rust, false in Java** for the same constructor.
2. **`bytes_read()` returned `4 + buf.len()` in Rust, `0 + buf.len()` in Java**.

**Resolution:** Fixed. `with_buffer` now leaves `size_pos = 0` and
`requested_buffer_size = -1` (mirroring Java exactly). `size()` was extended to
fall back to `payload.len() + 4` when `requested_buffer_size == -1` but
`buffer.is_some()`, matching Java's `payload().limit() + size.limit()`. Also
introduced a `payload_pos` field to track payload progress separately from
`buffer.len()`, so `bytes_read()` correctly returns `payload_pos + size_pos`.
Added a Java-parity test `with_buffer_does_not_synthesise_size_header`.

## Issue: Per-call scratch allocation on the receive hot path

- **File**: `src/common/network/network_receive.rs:204-209`
- **Severity**: Suggestion (Performance — CLAUDE.md rule 11)
- **Java Reference**: `kafka/clients/src/main/java/org/apache/kafka/common/network/NetworkReceive.java:107-112`
- **Originating commit**: `2a31a08`

Each call to `read_from` that had remaining payload to read allocated a fresh
`vec![0u8; needed]` scratch buffer, copied bytes from the source into it, then
copied again into `buf` via `extend_from_slice`. Java reads directly into the
backing `ByteBuffer` via `channel.read(buffer)` — no temporary buffer, no
second copy.

**Resolution:** Fixed. The payload buffer is now allocated once at the full
`requested_buffer_size` (zero-initialised so the spare slice is a valid
`&mut [u8]`), and subsequent reads fill `&mut buf[payload_pos..total]` in
place — no per-call scratch buffer, mirroring Java's `channel.read(buffer)`
directly into the backing `ByteBuffer`. Tracked the write cursor via the new
`payload_pos: usize` field instead of relying on `BytesMut::len`.

## Issue: `ClientResponse::try_with_timed_out` is unjustified API surface (CLAUDE.md rule 7)

- **File**: `src/client_response.rs:122-151`
- **Severity**: Suggestion (Definition of Done #7)
- **Java Reference**: `kafka/clients/src/main/java/org/apache/kafka/clients/ClientResponse.java:87-110`
- **Originating commit**: `49c19cb`

Java has a single 10-arg constructor that throws `IllegalStateException` when
the `disconnected`/`timedOut` invariant is violated. Rust shipped two:
`with_timed_out` (panicking) and `try_with_timed_out` (returns
`Result<_, KafkaError::IllegalState>`). `try_with_timed_out` was only used
in tests.

**Resolution:** Fixed. Removed `try_with_timed_out` and its dedicated test.
Kept only the panicking `with_timed_out` (which mirrors Java's
`IllegalStateException`) and the convenience `new` (delegates to
`with_timed_out` with `timed_out = false`). CLAUDE.md rule 10.1 explicitly
endorses panic for unrecoverable invariant violations.

## Issue: `NetworkReceive::size()` panic-on-misuse outlives the contract it mirrors

- **File**: `src/common/network/network_receive.rs:103-110`
- **Severity**: Suggestion (API Design)
- **Java Reference**: `kafka/clients/src/main/java/org/apache/kafka/common/network/NetworkReceive.java:150-152`
- **Originating commit**: `2a31a08`

Java's `size()` NPEs through `payload().limit()` when `buffer == null`.
Critic suggested returning `Option<i32>` instead of panicking.

**Resolution:** Rejected — CLAUDE.md rule 10.1 endorses panic for unrecoverable
invariants and the Java contract is the authoritative reference; Java NPEs in
this case. The only legitimate caller is metrics emission, which already gates
on `complete()` or `memory_allocated()` first. Rustdoc on `size()` was tightened
to make the precondition explicit (commit reference: see network_receive.rs
`size()` rustdoc).

## Issue: `ListenerName::normalised`/`config_prefix` use ASCII-only case folding

- **File**: `src/common/network/listener_name.rs:51, 61, 71`
- **Severity**: Suggestion (Behavior Mismatch — minor)
- **Java Reference**: `kafka/clients/src/main/java/org/apache/kafka/common/network/ListenerName.java:44, 77, 85`
- **Originating commit**: `b675e2f`

Java uses `value.toUpperCase(Locale.ROOT)` (Unicode case folding). Rust uses
`to_ascii_uppercase` (ASCII-only).

**Resolution:** Rejected — Listener names are configured via Kafka client
properties and are ASCII by spec (`PLAINTEXT`, `SSL`, `INTERNAL`, etc.).
CLAUDE.md rule 11 prefers ASCII case folding on hot paths to avoid the cost
of full Unicode tables. While `normalised` is called once at startup, the
operator-supplied listener name space is conventionally ASCII; the
divergence on Turkish dotted-I etc. is theoretical and unlikely in
practice for a Kafka deployment.

---

# Critic 0 — Phase 5b-1 (TransportLayer + PlaintextTransportLayer) — Done

## Issue: `PlaintextTransportLayer::read` collapses peer-EOF into "no data ready", losing the `EOFException` signal Java relies on

- **File**: `src/common/network/plaintext_transport_layer.rs:181-196` (also the `io::Read` forwarder)
- **Severity**: Blocking (Behavior Mismatch — DoD #1, contract divergence from Java)
- **Java Reference**: `kafka/clients/src/main/java/org/apache/kafka/common/network/NetworkReceive.java:85-87, 109-111`
- **Originating commit**: `63fac0c`

Tokio's `TcpStream::try_read(non_empty_buf)` returns `Ok(0)` to mean
peer-closed/EOF; the original implementation collapsed both `WouldBlock`
and `Ok(0)` into `Ok(0)` for the caller, so a half-closed socket looked
identical to a quiet socket. Java's `SocketChannel.read()` returns `-1` on
EOF, which `NetworkReceive.readFrom` translates into `EOFException`.

**Resolution:** Fixed. `PlaintextTransportLayer::read` now distinguishes
the three Tokio outcomes:

| Tokio outcome | Adapter result | Java NIO equivalent |
| --- | --- | --- |
| `Ok(n)` with `n > 0` | `Ok(n)` | `read() == n` |
| `Err(WouldBlock)` | `Ok(0)` | `read() == 0` |
| `Ok(0)` (peer closed) | `Err(io::ErrorKind::UnexpectedEof)` | `read() == -1 → EOFException` |

The trait rustdoc on `TransportLayer::read` and the `NetworkReceive::read_from`
comment were updated to document the EOF semantic. Two regression tests were
added: `read_returns_unexpected_eof_on_peer_close` (drops the server side and
asserts the next `TransportLayer::read` returns `UnexpectedEof`) and
`io_read_adapter_propagates_eof` (same path through the `io::Read`
forwarder used by `NetworkReceive`).

## Issue: `is_open()` and `interest_ops()` desync after `disconnect()`

- **File**: `src/common/network/plaintext_transport_layer.rs:148-156, 167-169, 209-225`
- **Severity**: Suggestion (Java Behavior Divergence — minor, may matter in 5c)
- **Java Reference**: `kafka/clients/src/main/java/org/apache/kafka/common/network/PlaintextTransportLayer.java:56-58, 71-73, 188-205`
- **Originating commit**: `63fac0c`

Java's `disconnect()` cancels the SelectionKey but leaves the
`socketChannel` open until `close()`. Rust's `disconnect()` flips
`is_open = false` immediately, so `is_open()` after `disconnect()` returns
`false` in Rust but `true` in Java.

**Resolution:** Rejected (deferred to Phase 5b-3/5c per Critic's own
recommendation). The Critic explicitly tagged this comment as "Filed as
Suggestion — defer the fix until 5b-3/5c demonstrates a real caller
diverging." There is no current caller of `is_open()` between `disconnect()`
and `close()`, so the divergence is unobservable at the present surface.
Once `KafkaChannel` lands and exercises the disconnect→close window we
will revisit either by splitting `is_open` into a `selection_key_cancelled`
+ `socket_open` pair, or by documenting the combined Rust semantic on
the trait. Preferring to defer rather than speculatively reshape the
state machine without a concrete caller.

## Issue: Trait surface lacks `peer_addr`/`local_addr` accessors that `KafkaChannel` will need in 5b-3

- **File**: `src/common/network/transport_layer.rs:67-135`
- **Severity**: Suggestion (Missing Requirement — DoD #2 forecast)
- **Java Reference**: `kafka/clients/src/main/java/org/apache/kafka/common/network/KafkaChannel.java:368-388`
- **Originating commit**: `63fac0c`

Java's `KafkaChannel` reads `transportLayer.socketChannel().socket()`
peer/local addresses for connection-introspection metadata. The
Rust trait omitted them, leaving Phase 5b-3 with the choice of
downcasting the trait object (CLAUDE.md rule 7 violation) or making
`KafkaChannel` generic over the concrete transport. Critic recommended
adding the accessors now to avoid a forced refactor.

**Resolution:** Fixed. `TransportLayer` gained
`fn local_addr(&self) -> io::Result<SocketAddr>` and
`fn peer_addr(&self) -> io::Result<SocketAddr>`. The two methods
`PlaintextTransportLayer::local_addr` / `::peer_addr` previously declared
as inherent moved into the trait `impl`, with no behaviour change
(forwarding to `TcpStream::local_addr` / `::peer_addr`). The future
`SslTransportLayer` will trivially forward to its inner `TcpStream` the
same way. Existing tests (`local_and_peer_addr_round_trip`,
`ops_on_closed_transport_error`) continue to pass.

## Issue: `KafkaPrincipal::new` rejects empty strings; Java accepts them

- **File**: `src/common/security/auth/kafka_principal.rs:65-79`
- **Severity**: Suggestion (Behavior Mismatch — public API contract)
- **Java Reference**: `kafka/clients/src/main/java/org/apache/kafka/common/security/auth/KafkaPrincipal.java:55-59`
- **Originating commit**: `63fac0c`

Java's constructor uses `requireNonNull` only — empty strings are
accepted. Rust panicked on empty strings, which CLAUDE.md rule 4
forbids ("Never change the contract of public API"). `String` is
already non-nullable in Rust, so no further validation is needed.

**Resolution:** Fixed. The `assert!(!principal_type.is_empty(), ...)`
and `assert!(!name.is_empty(), ...)` were removed. `with_token_authenticated`
now constructs the `KafkaPrincipal` directly from the converted strings.
Added a `empty_strings_are_accepted` regression test that exercises both
`("", "")` and `("User", "")` — both must succeed and produce the
expected `to_string()` output. Java's constructor `requireNonNull`
behavior is now mirrored exactly.

## Issue: `KafkaPrincipal::anonymous()` allocates two `String`s per call

- **File**: `src/common/security/auth/kafka_principal.rs:81-84`
- **Severity**: Suggestion (Performance — minor, off the per-message hot path)
- **Java Reference**: `kafka/clients/src/main/java/org/apache/kafka/common/security/auth/KafkaPrincipal.java:45`
- **Originating commit**: `63fac0c`

Java caches `public static final KafkaPrincipal ANONYMOUS = new KafkaPrincipal(...)`.
Rust allocated fresh `String`s on every `anonymous()` call.

**Resolution:** Fixed. `anonymous()` now uses a `static OnceLock<KafkaPrincipal>`
to cache the singleton and returns `.clone()` of the cached value (one cheap
`KafkaPrincipal::clone` per call instead of two `String::from` allocations).
Added a `pub const ANONYMOUS_NAME: &str = "ANONYMOUS"` constant so the
literal is reused. Added a `anonymous_is_idempotent` test to lock in the
singleton-equivalent contract.

---

# Critic 0 — Phase 5b-2 (SslTransportLayer) — Done

## Issue: rustls plaintext sink is unbounded — `write_vectored` has no backpressure

- **File**: `src/common/network/ssl_transport_layer.rs:441-490` (pre-fix)
- **Severity**: Suggestion (Performance / Behavior)
- **Java Reference**: `kafka/clients/src/main/java/org/apache/kafka/common/network/SslTransportLayer.java:709-742`
- **Originating commit**: `faa61cc`

Java's `write(ByteBuffer src)` is bounded: the `netWriteBuffer` is a fixed-size
(typically 16 KiB) buffer, so when the network is backed up Java's `write` returns
0 and the producer's `Sender` stops accumulating. Rust's `writer().write_vectored`
appended *all* IoSlice content into rustls's internal `sendable_plaintext` queue,
which is unbounded by default. A slow/blocked broker connection could balloon to
many MB of buffered plaintext per channel before producer-side BufferPool
admission control kicks in.

**Resolution:** Fixed. Both constructors call
`conn.set_buffer_limit(Some(PLAINTEXT_BUFFER_LIMIT))` immediately after
`ClientConnection::new`, where `PLAINTEXT_BUFFER_LIMIT = 64 * 1024`. Once the
queue is full `writer().write_vectored` returns `Ok(n < total)`, providing the
same backpressure as Java's `netWriteBuffer.hasRemaining()` gate. Added the
constant with a doc-comment explaining the rationale and the 64 KiB sizing
(comfortably holds a couple of full TLS records' worth of pending plaintext).
Added regression test `plaintext_buffer_limit_applied_on_construction` that
pumps 8 KiB chunks into the writer without draining the socket and asserts the
queue refuses to grow past the cap.

## Issue: `has_bytes_buffered` set to `total_read > 0` ignores remaining buffered plaintext

- **File**: `src/common/network/ssl_transport_layer.rs:561-672` (pre-fix)
- **Severity**: Suggestion (Behavior Mismatch)
- **Java Reference**: `kafka/clients/src/main/java/org/apache/kafka/common/network/SslTransportLayer.java:991-1000`
- **Originating commit**: `faa61cc`

Java's `updateBytesBuffered(madeProgress)` reflects whether buffered plaintext or
unprocessed TLS bytes remain *after* this call so the next poll can deliver them
without going to the network. The Rust translation set
`has_bytes_buffered = total_read > 0`, which:
1. Returned `true` when we delivered some plaintext but the receive buffer was
   now empty — Java would return `false`. Result: spurious wakeup.
2. Returned `false` when we delivered 0 plaintext but rustls still had plaintext
   pending in `IoState` — Java would return `true`. Result: missed wakeup, the
   channel stalls until external readiness fires again.

The (2) case is the load-bearing one because Phase 5c's Selector uses
`hasBytesBuffered()` exactly to schedule a same-poll re-tick of the channel.

**Resolution:** Fixed. The `read` method now captures
`IoState::plaintext_bytes_to_read()` from each `process_new_packets` call and
subtracts the bytes it just drained into the caller's `dst`. The final
`has_bytes_buffered` is computed as
`(read_from_network || total_read > 0) && last_plaintext_pending > 0`, mirroring
Java's `updateBytesBuffered(madeProgress)` semantics — `madeProgress` AND there
is queued plaintext post-drain. Updated the `has_bytes_buffered` field rustdoc
to reflect the new accuracy. Added regression test
`has_bytes_buffered_false_after_full_drain` that drains a full echo round-trip
into a generously-sized buffer and asserts `has_bytes_buffered() == false`
afterwards (the previous `total_read > 0` heuristic would have returned true).

## Issue: cipher information uses Debug format ("TLS13_AES_256_GCM_SHA384") not IANA name

- **File**: `src/common/network/ssl_transport_layer.rs:432-439` (pre-fix)
- **Severity**: Suggestion (Behavior Mismatch — cosmetic)
- **Java Reference**: `kafka/clients/src/main/java/org/apache/kafka/common/network/SslTransportLayer.java:469`
- **Originating commit**: `faa61cc`

Java's `session.getCipherSuite()` returns the IANA name
(`"TLS_AES_256_GCM_SHA384"`); rustls's `Debug` impl returns
`"TLS13_AES_256_GCM_SHA384"` (with `13_` instead of `_`). Same for
`ProtocolVersion`: rustls Debug gives `"TLSv1_3"` vs Java's `"TLSv1.3"`. This
surfaces in the channel metadata registry (Phase 5b-3+).

**Resolution:** Fixed. Added two pure helper functions `iana_cipher_name`
(maps `CipherSuite` → IANA-canonical `String`) and `iana_protocol_name` (maps
`ProtocolVersion` → dotted form like `"TLSv1.3"`). They cover the cipher suites
rustls negotiates by default with `aws_lc_rs` (the 5 TLS 1.3 suites + the modern
TLS 1.2 ECDHE-RSA / ECDHE-ECDSA ones — AES-GCM and CHACHA20-POLY1305).
`extract_cipher_info` now routes through them. Unmapped cipher / protocol
values fall back to the rustls `Debug` form so a regression is obvious in logs
(rather than an empty string). Added two unit tests
(`iana_cipher_name_strips_tls13_infix`, `iana_protocol_name_uses_dotted_form`)
plus three new assertions in the existing `handshake_populates_cipher_information`
test verifying the negotiated session reports IANA-canonical strings.

## Issue: `is_mute()` returns `true` for a not-yet-ready SSL channel

- **File**: `src/common/network/ssl_transport_layer.rs:731-734`
- **Severity**: Suggestion (Behavior Mismatch — clarification)
- **Java Reference**: `kafka/clients/src/main/java/org/apache/kafka/common/network/SslTransportLayer.java:981-984`
- **Originating commit**: `faa61cc`

Critic verified the behaviour matches Java exactly (a freshly-`pending_connect`-
constructed channel has `interest_ops == OP_CONNECT`, no `OP_READ`, so `is_mute()`
returns `true`). The concern was the rustdoc didn't disambiguate "actively muted
by upper layer" from "not yet eligible to read because connect hasn't completed".

**Resolution:** Fixed. Added rustdoc on `TransportLayer::is_mute` documenting
the connect-pending caveat and recommending callers pair the predicate with
`is_connected` / `ready` rather than treating it as a monolithic signal. The
implementation itself is unchanged because the behaviour matches Java.

## Issue: `add_interest_ops`/`remove_interest_ops` no-op when not ready (Java throws)

- **File**: `src/common/network/ssl_transport_layer.rs:708-725`
- **Severity**: Suggestion (Behavior Mismatch — Selector-time concern)
- **Java Reference**: `kafka/clients/src/main/java/org/apache/kafka/common/network/SslTransportLayer.java:815-837`
- **Originating commit**: `faa61cc`

Java throws `IllegalStateException("handshake is not completed")` when
`addInterestOps`/`removeInterestOps` is called pre-handshake-complete, and
`CancelledKeyException` if the key is invalid. The Rust translation silently
no-ops with only a doc-comment ("the Phase 5b-3 KafkaChannel will check
`ready()` before calling these"). Critic flagged this as fragile.

**Resolution:** Deferred to Phase 5c. Tokio doesn't have the
`SelectionKey`/`CancelledKeyException` model that Java's interest-op
manipulation predicates on — the runtime drives readiness directly. Whether
the trait surface should expose `Result<()>` (with explicit error variants for
not-ready / cancelled) versus retaining the no-op shape is a question the
Selector implementation will answer once it has a concrete caller. Until then,
adding `Result<()>` returns to all three impls (Plaintext + SSL + future
mocks) without a consumer is design speculation.

The current SSL impl already gates on `is_open` (silent no-op when closed),
which prevents the most dangerous misuse — call after `disconnect()`. The
rustdoc on the SSL impl already documents the "Phase 5b-3 KafkaChannel will
check `ready()` before calling these" contract. Filed as a tracking note for
Phase 5c review: when the Selector lands, decide whether to (a) keep the
silent gate, (b) split into `try_add_interest_ops`/`add_interest_ops`, or
(c) propagate `Result<()>` through the trait.

## Issue: `disconnect()` flips `is_open=false` (carryover from 5b-1 deferral)

- **File**: `src/common/network/ssl_transport_layer.rs:513-519`
- **Severity**: Suggestion (Behavior Mismatch — same defer rationale as 5b-1 Comment 2)
- **Java Reference**: `kafka/clients/src/main/java/org/apache/kafka/common/network/SslTransportLayer.java:149-152`
- **Originating commit**: `faa61cc`

Same divergence as `PlaintextTransportLayer` — Java's `disconnect()` calls
`key.cancel()` only; the underlying socket remains open until `close()`. The
Rust SSL layer flips `is_open = false`, `connected = false`, `interest_ops = 0`,
but does NOT drop `stream`/`conn`. So `is_open()` returns `false` even though
`stream.is_some()` is still true. Critic correctly noted the additional SSL
wrinkle: between `disconnect()` and `close()`, `peer_principal()` walks
`self.conn` (still `Some`) and would happily return a principal for a socket
the upper layer thinks is gone.

**Resolution:** Deferred — same rationale as 5b-1 Comment 2. No caller in
5b-2 observes the divergence. The defer is filed in lockstep with the 5b-1
plaintext-layer defer so when the underlying semantic is finally split (e.g.
into `key_valid: bool` + `socket_open: bool`, or by documenting the joint
semantic on the trait), both transports are addressed in the same change.
Filed as a tracking note in Phase 5c review so the Selector implementation
doesn't accidentally call `peer_principal` after `disconnect`.

## Issue 7 (Round 2): `has_bytes_buffered` returns `false` when step-1 drain fills `dst` while plaintext remains queued

- **File**: `src/common/network/ssl_transport_layer.rs:662-757` (post-fix `5fa1bed`)
- **Severity**: Bug (Behavior Mismatch — same load-bearing case the actor cited)
- **Java Reference**: `kafka/clients/src/main/java/org/apache/kafka/common/network/SslTransportLayer.java:566-651,995-1000`
- **Originating commit**: `faa61cc` (issue persisted in fixup `5fa1bed`)

The Round-1 fix correctly captured `IoState::plaintext_bytes_to_read` from
`process_new_packets` calls inside the network-read loop, but a path bypassed
that capture entirely:

1. Step 1 (line 667) drains *already-buffered plaintext* via
   `conn.reader().read(dst)` — the case where the previous read left rustls's
   queue with N bytes still pending and the caller's `dst` is M < N bytes.
2. After step 1, `total_read = M` (dst is full). The loop's first guard
   `if total_read == dst.len() { break; }` fired immediately.
3. `process_new_packets` was never called this iteration.
   `last_plaintext_pending` retained its initial value of 0.
   `read_from_network` retained `false`.
4. Final computation: `made_progress = (false || M > 0) = true`, but
   `has_bytes_buffered = true && (0 > 0) = false`.

Java's behaviour for the same scenario:
- `appReadBuffer.position() > 0` ⇒ `read = readFromAppBuffer(dst)` (M bytes
  drained, dst full, appReadBuffer still holds N − M bytes).
- The `while (dst.remaining() > 0)` loop body skipped because dst has no room.
- `updateBytesBuffered(readFromNetwork || read > 0)` ⇒ `madeProgress = true`.
- `hasBytesBuffered = (netReadBuffer.position() != 0 || appReadBuffer.position() != 0) = (N − M) != 0 = true`.

So Java set `hasBytesBuffered = true`, Rust set it to `false` — the exact
"missed wakeup, channel stalls until external readiness fires" failure mode
the Phase 5c Selector depends on getting right. The existing test
`has_bytes_buffered_false_after_full_drain` did not catch this because it
used an oversized 4 KiB buffer for a 10-byte payload, so step-1 never
overflowed.

**Resolution:** Fixed. Adopted suggestion (a) from the Critic — initialise
`last_plaintext_pending` from a `process_new_packets()` call performed
*before* the step-1 drain, then subtract step-1's `total_read` from it.
This mirrors Java's `appReadBuffer.position()` snapshot taken at the top
of `read(ByteBuffer)`. `process_new_packets` is documented as cheap when
idle (no new TLS records to surface), so the steady-state cost is bounded.
Errors from the pre-drain `process_new_packets` are surfaced as
`io::Error::other` (same shape as the loop-internal call).

Added regression test `has_bytes_buffered_true_when_step1_drain_fills_dst`:

1. Sends 2 KiB through the echo loop.
2. Stage 1: reads back 1 KiB into a 1 KiB buffer (populates rustls's
   plaintext queue with the remaining 1 KiB after decryption + drain).
3. Asserts `has_bytes_buffered() == true` (already-queued case).
4. Stage 2: reads with a 256-byte tiny buffer — step 1 fills `dst`,
   loop top guard fires, `process_new_packets` is never called this
   iteration. This is the exact path the bug regressed on.
5. Asserts `has_bytes_buffered() == true` (the load-bearing assertion;
   would have FAILED with the buggy code).
6. Drains the rest of the payload, asserts `has_bytes_buffered() == false`
   only after the queue is fully empty (confirms the snapshot
   bookkeeping correctly subtracts step-1 drains over multiple reads).

All 4 gates green. Fixup chain: `5fa1bed` → fixup of `faa61cc`, this fixup
chains on top.
