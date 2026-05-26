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

# Critic 0 — Phase 5b-3 (KafkaChannel + ChannelBuilders) — Done

Round 1 of `253c383` filed 5 Suggestions; this fixup addresses all 5.

## Issue 1: `SslAuthenticator` caches `peer_principal()` at pre-handshake construction time

- **File**: `src/common/network/ssl_channel_builder.rs:119`,
  `src/common/network/authenticator.rs:138-140`
- **Severity**: Suggestion
- **Originating commit**: `253c383`
- **Disposition**: **Fixed**

`SslAuthenticator` was holding a `KafkaPrincipal` field captured at
construction time, before the TLS handshake had run. Every subsequent
`KafkaChannel::principal()` call returned the frozen anonymous value
even after the handshake completed and `peer_certificates()` was
populated.

**Fix**: Made `SslAuthenticator` stateless — removed the cached
`principal: KafkaPrincipal` field. Changed `Authenticator::principal`
to take a `&dyn TransportLayer` argument so the SSL impl can re-query
`transport.peer_principal()` lazily on every call (mirrors Java's
`SslAuthenticator.principal()` reading `transportLayer.sslSession()`
on demand). The owning `KafkaChannel::principal()` now forwards
`self.transport_layer.as_ref()` into the authenticator. The
plaintext impl ignores the argument and returns
`KafkaPrincipal::anonymous()` as before.

New regression test
`authenticator::tests::ssl_authenticator_principal_is_lazy` flips a
stub transport's handshake state mid-test and asserts the second
`principal()` call returns the new identity — locking in lazy
semantics.

## Issue 2: `channel_builder_configs` test silently elides two Java assertions

- **File**: `src/common/network/channel_builders.rs:184-297`
- **Severity**: Suggestion
- **Originating commit**: `253c383`
- **Disposition**: **Fixed (test cleanup + explicit divergence assertions)**

The test docstring/comment block was a 90-line stream-of-consciousness
re-derivation of Java's filter logic that ended up not asserting two
Java facts (lines 74 and 77 of the Java test). The diverged behaviour
is real (Java's `valuesWithPrefixOverride` consults a `ConfigDef`
schema that we have not translated; the helper here is schema-less),
but the prior comment buried the divergence rather than locking it in.

**Fix**: Replaced the long comment with a concise docstring listing
the two divergences with line references (Java lines 74 and 77) and
the underlying cause (`ConfigDef` schema not translated until SASL).
Added explicit assertions for the actual Rust behaviour for both
keys, so the divergence is now part of the test contract — Phase 9
SASL will need to update both assertions when the typed-config helper
lands.

## Issue 3: `sending_lifecycle` test does not exercise multi-tick partial-write progression

- **File**: `src/common/network/kafka_channel.rs:845-868`
- **Severity**: Suggestion
- **Originating commit**: `253c383`
- **Disposition**: **Fixed**

Java's `KafkaChannelTest.testSending` configures the mock to return
partial byte counts (4, 64, 64) across three writes and asserts the
in-progress send remains incomplete after each partial. The Rust
`MockTransport::write_vectored` always wrote everything in one call,
collapsing the progression to a single tick. A regression where
`maybe_complete_send` always returned `Some(...)` would not have
been caught.

**Fix**: Extended `MockState` with an optional `max_bytes_per_write`
cap and a helper `set_max_bytes_per_write`. `write_vectored` now
truncates the call to at most the cap (mirroring a kernel-buffer-
exhausted `SocketChannel.write` returning a partial count). Added a
new test `sending_partial_writes_progress_across_multiple_ticks`
that drives 3 ticks with caps `4 / 64 / 64` and asserts
`maybe_complete_send() == None` after the first two ticks and
`Some(send)` only on the third — locking in the
`bytes_remaining > 0 → maybe_complete_send() == None` invariant the
Selector relies on.

## Issue 4: `mute()`, `maybe_unmute()`, `complete_close_on_authentication_failure()` are `pub` instead of `pub(crate)`

- **File**: `src/common/network/kafka_channel.rs:368, 383, 460`
- **Severity**: Suggestion
- **Originating commit**: `253c383`
- **Disposition**: **Fixed**

Java's `KafkaChannel.mute`, `maybeUnmute`, and
`completeCloseOnAuthenticationFailure` are package-private. The Rust
translation exposed them as `pub`, widening the trust boundary
beyond Java's. The Phase 5c Selector (sibling module in the same
crate) is the only legitimate caller — `pub(crate)` is the closer
mirror.

**Fix**: Tightened all three to `pub(crate)`. Added
`#[allow(dead_code)]` annotations because the Selector that exercises
them lands in Phase 5c; the existing tests cover the methods through
private-test access. Updated rustdoc to reference `pub(crate)`.

## Issue 5: `socket_address()` returns full `SocketAddr` instead of host-only equivalent of `InetAddress`

- **File**: `src/common/network/kafka_channel.rs:508-520`
- **Severity**: Suggestion
- **Originating commit**: `253c383`
- **Disposition**: **Fixed**

Java's `KafkaChannel.socketAddress()` returns `InetAddress` (host
only); the Rust translation returned `SocketAddr` (host + port),
collapsing two distinct Java methods (`socketAddress` and
`socketPort`) into one with a different return type.
`socketDescription()` was missing entirely — the Phase 5c Selector
will need it for disconnect log lines.

**Fix**:
- `socket_address()` now returns `io::Result<IpAddr>` (the host-only
  equivalent of Java's `InetAddress`).
- Added `socket_port() -> u16` mirroring Java's `socketPort()` —
  returns `0` if never connected, falls back to the captured
  `remote_address.port()` after disconnect (matches Java's "continue
  to return the connected port number after the socket is closed").
- Added `socket_description() -> String` mirroring Java's
  `socketDescription()` — peer address if available, captured
  remote, then local address fallback (Java's `getLocalAddress`
  fallback when `getInetAddress()` is null).

## Issue 6: `socket_description()` local-fallback includes port — Java does not

- **File**: `src/common/network/kafka_channel.rs:551-563`
- **Severity**: Suggestion (Behavior Mismatch — log format only)
- **Java Reference**:
  `kafka/clients/src/main/java/org/apache/kafka/common/network/KafkaChannel.java:382-387`
- **Originating commit**: `253c383`
- **Disposition**: **Fixed**

Java's `socketDescription()` returns either
`socket.getInetAddress().toString()` or
`socket.getLocalAddress().toString()`. Both call
`InetAddress.toString()`, which is host-only — `InetAddress` has no
port. The Rust translation was asymmetric: the peer and
captured-remote branches used `addr.ip().to_string()` (host only),
but the `local_addr()` fallback used `local.to_string()` on a
`SocketAddr`, which renders as `host:port`. That made the local
fallback the only branch with a port, diverging from Java and from
the function's other branches.

**Fix**: Changed the local-fallback arm to `local.ip().to_string()`
so all three branches emit host-only strings, matching Java's
`InetAddress.toString()` shape. Log format only — no protocol-level
behavior change. Verified via the existing 842-test suite (all
green) and the four gates (build / test / format-check / lint).

---

# Critic 0 — Phase 5c-1 (Round 1) — Done

All 5 actionable Suggestion comments on commit `c66c3b3` resolved. Issue 6
was self-withdrawn by the Critic in Round 1 (Java has the same triple
HashMap lookup) and is archived here for completeness.

## Issue 1: `ClusterConnectionStates` is `pub`, but Java is package-private

- **File**: `src/cluster_connection_states.rs:65`
- **Severity**: Suggestion
- **Java Reference**: `kafka/clients/src/main/java/org/apache/kafka/clients/ClusterConnectionStates.java:39`
- **Originating commit**: `c66c3b3`
- **Disposition**: Fixed.

`ClusterConnectionStates`, the four `RECONNECT_*` / `CONNECTION_SETUP_TIMEOUT_*`
constants, and the inner `NodeConnectionState` are now `pub(crate)` (Java is
package-private). The `pub use cluster_connection_states::ClusterConnectionStates`
re-export from `lib.rs` was dropped — sister `InFlightRequests` is also
`pub(crate)` with no re-export, so the conventions now match.
`#[allow(dead_code)]` lint annotations track the same "Phase 5d NetworkClient
is the first non-test caller" rationale that `InFlightRequests` already uses.

## Issue 2: Hot-path identifier interning inconsistency between sibling classes

- **File**: `src/api_versions.rs:83`, `:93`, `:100`, `src/metadata_updater.rs:57`,
  `src/manual_metadata_updater.rs:74`
- **Severity**: Suggestion (hot-path performance + API consistency)
- **Java Reference**: `NetworkClient.java:1052`
- **Originating commit**: `c66c3b3`
- **Disposition**: Fixed.

`ApiVersions::update / remove / get` now take `i32` instead of `&str`.
`MetadataUpdater::handle_server_disconnect` and the `ManualMetadataUpdater`
impl now take `i32`. The `ApiVersionsInner.node_api_versions` HashMap key
flipped from `String` → `i32`. Test fixture in `api_versions_test` (Java
`testFinalizedFeaturesUpdate`) updated from `"2"` / `"1"` literals to
`2` / `1`. The `i32` flow is now unbroken end-to-end across all sibling
classes — the producer hot path, when Phase 5d wires it up, will not pay
a per-request `String` clone for connection-id keying. Documented inline
on each method's rustdoc + on the `ApiVersionsInner.node_api_versions`
field.

## Issue 3: `testUsableVersionLatestVersions` silently skipped invalid api keys

- **File**: `src/node_api_versions.rs:451-457`
- **Severity**: Suggestion (test-fidelity)
- **Java Reference**: `NodeApiVersionsTest.java:147-161`
- **Originating commit**: `c66c3b3`
- **Disposition**: Fixed.

Removed the `if !api_key.has_valid_version() { continue; }` guard. To
match Java's contract, the *fixture* now filters on `has_valid_version()`
when synthesising the `version_list` (mirroring Java's
`filterApis` / `toApiVersionForApiResponse` pipeline that
`defaultApiVersionsResponse` runs through). The loop assertion is
unconditional, identical to Java. If a future spec change introduces a
listener-scoped key without a valid version, the assertion fires loudly
— same signal Java's test gives.

The Critic's hint "drop the `continue`" was the right diagnosis but the
direct fix would have failed locally because `StreamsGroupHeartbeat`
(id=88) is currently flagged unstable (`max=-1` with
`enable_unstable_last_version=false`). Filtering the fixture instead
preserves the no-skip loop contract while keeping the test green for
the current spec — the equivalent change Java's `filterApis` makes.

## Issue 4: `make_request` test fixture used `expect_response = true` while Java uses `false`

- **File**: `src/in_flight_requests.rs:441-453`
- **Severity**: Suggestion (test-fidelity)
- **Java Reference**: `InFlightRequestsTest.java:122-125`
- **Originating commit**: `c66c3b3`
- **Disposition**: Fixed.

Trivial flip: `true` → `false`. Inline comment now references the Java
fixture (`expectResponse = false`, `isInternalRequest = false`) so a
future Phase 5d test that copies this fixture as a starting point cannot
silently inherit the wrong default. None of the existing translated
tests assert on `expect_response`, so the change is observation-neutral
today.

## Issue 5: Dead `host_changed` variable in `ClusterConnectionStates::connecting`

- **File**: `src/cluster_connection_states.rs:177-202`
- **Severity**: Suggestion (code clarity)
- **Java Reference**: `ClusterConnectionStates.java:147-165`
- **Originating commit**: `c66c3b3`
- **Disposition**: Fixed.

Restructured `connecting()` to mirror Java's shape: an `if let Some(state)`
guard with an inner same-host early-return (with `state.last_connect_attempt_ms`
update + `move_to_next_address` + `connecting_nodes.insert`); on hostname
change, log via `info!` and fall through to the unconditional
"create new state" block. The previous `let host_changed = match { … } true;
… let _ = host_changed;` dance is gone. Behaviour identical (verified by
the existing 15 `cluster_connection_states` tests, all still green).

## Issue 6: `is_connection_setup_timeout` HashMap lookups (self-withdrawn)

- **File**: `src/cluster_connection_states.rs:435-441`
- **Severity**: Suggestion (Performance) — Critic withdrew in Round 1
- **Originating commit**: `c66c3b3`
- **Disposition**: No action — Java has the same triple HashMap lookup
  shape (`nodeState(id)` + `lastConnectAttemptMs(id)` +
  `connectionSetupTimeoutMs(id)`, all of which dispatch through
  `nodeState.get(id)`). Fidelity to Java's structure trumps the
  micro-optimisation.

## DoD sign-off

- 886 lib tests passing (no delta — same fixture count, in_flight_requests
  test still passes with the new `expect_response = false`, the other
  fixes are visibility/typing/restructuring only).
- `cargo build` clean.
- `cargo xtask format-check` clean.
- `cargo xtask lint` clean.
- Fixup chain: a single `fixup! c66c3b3` commit covers all 5 issues —
  they are tightly cohesive (visibility tightening + type alignment +
  fixture fidelity + code clarity, all in the same connection-state
  plumbing surface area).

---

# Phase 5c-2 (Selector — Tokio rewrite) — Round 1 disposition

Critic-0 filed 1 Blocking + 4 Suggestion comments on commit `c02ca85`.
All five resolved by a single `fixup! c02ca85`.

## Issue 1: `idle.update` runs every poll for every channel — defeats `connections.max.idle.ms` in production

- **File**: `src/common/network/selector.rs:921-926` (pre-fix)
- **Severity**: Blocking — Behavior Mismatch
- **Java Reference**: `Selector.java:525-526` (`idleExpiryManager.update(nodeId, currentTimeNanos)` per ready key inside `pollSelectionKeys`)
- **Originating commit**: `c02ca85`
- **Disposition**: Fixed.

Java only touches the LRU for keys returned ready by `nioSelector.select(...)` —
i.e. channels with actual I/O activity this poll. The pre-fix code
unconditionally refreshed `last_active_ns` for every open channel at
the bottom of `poll`, so a connection idle for the entire
`connections.max.idle.ms` window still appeared "fresh" and
`pollExpiredConnection` never returned it.

Fix: `drive_channel_io` already returns the per-tick `made_progress`
flag (now also tracking write-bytes-progress, not just read). `poll`
collects io-active ids inline as it drives each channel and updates
the LRU only for those ids before the expiry sweep. Rule: the
[`IdleExpiryManager`] LRU is touched iff the channel had read or
write progress on this tick. Since the sweep runs AFTER io-active
LRU updates, busy channels are protected exactly as in Java.

Two regression tests:
- `busy_poll_does_not_reset_idle_clock` — drives `poll(0)` repeatedly
  for 300ms with no I/O against a 100ms idle budget; asserts the
  channel surfaces in `disconnected` with state EXPIRED. Under the
  bug, every busy poll refreshed `last_active_ns`, making expiry
  impossible.
- `idle_expiry_manager_only_updated_channel_is_refreshed` — pins
  the LRU bookkeeping rule in isolation: only the touched channel's
  timestamp moves; the untouched channel expires first regardless
  of how many times the busy channel is updated.

## Issue 5 (resolved together with Issue 1): `IdleExpiryManager.update` O(n) per call

- **File**: `src/common/network/selector.rs:189-199` (pre-fix)
- **Severity**: Suggestion — Performance
- **Originating commit**: `c02ca85`
- **Disposition**: Fixed (rewritten as part of the Issue-1 fix).

Replaced the `HashMap + VecDeque` LRU with a `BTreeSet<(timestamp, id)>`
+ `HashMap<id, timestamp>` pair: O(log n) `update` / `remove` /
`poll_expired_connection`, no linear scans. The `(timestamp, id)`
key avoids collision on simultaneous touches (deterministic-test
edge case). After Issue 1's fix limits LRU updates to io-active
channels, the per-poll cost is O(io-active × log(open)) instead of
the old O(open²) — sufficient for thousands of connections per
the brief.

The existing `idle_expiry_manager_polls_oldest_first` test and the
new `idle_expiry_manager_only_updated_channel_is_refreshed` test
both pass against the new data structure.

## Issue 2: `process_closing_channels` runs after `failed_sends` was already drained — closing-channel `sendFailed` short-circuit dead

- **File**: `src/common/network/selector.rs:639-657, 873-876, 691-693` (pre-fix)
- **Severity**: Suggestion — Behavior Mismatch
- **Java Reference**: `Selector.java:842-863` (private `clear()`)
- **Originating commit**: `c02ca85`
- **Disposition**: Fixed.

Java's order in `clear()`:
1. Clear vec outputs (`completedSends`, `completedReceives`, `connected`, `disconnected`).
2. Process closing channels — for each, `failedSends.remove(channel.id())` returns true if a send was queued and consumes the entry; on true, skip `maybeReadFromClosingChannel`.
3. Drain remaining `failedSends` into `disconnected`.

Pre-fix Rust ran step 1 + step 3 together (`clear_per_poll_outputs`
drained `failed_sends` immediately), then step 2 — at which point
`failed_sends` was always empty and the short-circuit fired
`false` regardless. Closing channels with a queued failed-send got
an extra wasted read.

Fix: split `clear_per_poll_outputs` (vec clears) from
`drain_failed_sends` (`failed_sends` → `disconnected`), and call
`process_closing_channels` between them. Inside
`process_closing_channels` we now `swap_remove` the matching id
from `failed_sends` (Java's `failedSends.remove` consumes the
entry), so the post-step-2 drain doesn't double-insert into
`disconnected`. New regression test `closing_channel_failed_send_short_circuits_read`
exercises the closing-channel + failed-send path and asserts the
channel surfaces in `disconnected` exactly once.

## Issue 3: `set_send_buffer_size` / `set_receive_buffer_size` config silently ignored

- **File**: `src/common/network/selector.rs:714-758` (pre-fix)
- **Severity**: Suggestion — Behavior Mismatch
- **Java Reference**: `Selector.java:284-294` (`configureSocketChannel`)
- **Originating commit**: `c02ca85`
- **Disposition**: Fixed.

`Selectable::connect` accepted `send_buffer_size` and
`receive_buffer_size` parameters but the implementation prefixed
them with `_` (unused). Phase 5d producer config keys
`send.buffer.bytes` / `receive.buffer.bytes` would have silently
no-op'd. Java applies these via `Socket.setSendBufferSize` /
`setReceiveBufferSize` on the unconnected socket when not
[`USE_DEFAULT_BUFFER_SIZE`].

Fix: replaced [`TcpStream::connect`] with [`TcpSocket`] +
`set_send_buffer_size` / `set_recv_buffer_size` + `connect`.
Tokio's `TcpSocket` (stable since 1.18) is the equivalent of
Java's pre-connect `SocketChannel.socket()`. The new
`connect_socket` helper centralises the configuration so any
future option (e.g. `SO_LINGER`) lands in one place.

Regression test `connect_applies_keepalive_and_buffer_sizes`
connects to the echo server with non-default 32 KiB send/recv
buffers and round-trips a payload — exercising the wired path
without asserting OS-specific clamped values from `getsockopt`
(Linux doubles SNDBUF/RCVBUF internally; macOS clamps to
`net.inet.tcp.sendspace`).

## Issue 4: TCP keepalive is not enabled

- **File**: `src/common/network/selector.rs:736-756` (pre-fix)
- **Severity**: Suggestion — Behavior Mismatch
- **Java Reference**: `Selector.java:288` (`socket.setKeepAlive(true)`)
- **Originating commit**: `c02ca85`
- **Disposition**: Fixed.

Java unconditionally sets `SO_KEEPALIVE` on every client connection.
Pre-fix Rust only set `set_nodelay(true)`. The default kernel
keepalive timer is measured in hours so the practical impact is
muted, but it is a documented Java client behaviour.

Fix: same path as Issue 3 — `TcpSocket::set_keepalive(true)` is
called on the freshly-created socket BEFORE connect, mirroring
Java's `configureSocketChannel` order. Errors propagate (Java's
`setKeepAlive` throws too).

The Issue-3 regression test (`connect_applies_keepalive_and_buffer_sizes`)
exercises the keepalive path implicitly — connect succeeds and
data flows after the option is set. We do not assert the kernel
keepalive timer values via `getsockopt` because they are
OS-defaulted and not part of Java's client surface either.

## DoD sign-off

- 910 lib tests passing (was 906 — `+4` new regression tests:
  `busy_poll_does_not_reset_idle_clock`,
  `idle_expiry_manager_only_updated_channel_is_refreshed`,
  `closing_channel_failed_send_short_circuits_read`,
  `connect_applies_keepalive_and_buffer_sizes`).
- `cargo build` clean.
- `cargo xtask format-check` clean.
- `cargo xtask lint` clean.
- Fixup chain: a single `fixup! c02ca85` covers all 5 issues —
  they are tightly cohesive (idle-expiry semantics + closing-channel
  ordering + connect-path option wiring, all in the same Selector
  poll/connect surface).

# Critic 0 — Phase 5d (NetworkClient + NetworkClientUtils) — Done

## Round 1 dispositions (Actor 0 fixup)

| # | Severity | Issue | Disposition | Resolution |
|---|---|---|---|---|
| 1 | Bug | `do_send` UnsupportedVersion drops internal METADATA failure callback | **Accept and fix** | Mirrored Java's `else if (apiKey == ApiKeys.METADATA)` arm in both `do_send` UnsupportedVersion sites (`latest_usable_version_in_range` failure path AND `builder.build(version)` failure path). Latent until Phase 6 wires `DefaultMetadataUpdater`, but the contract is now correct. |
| 2 | Missing Req. | KIP-511 fallback path untested | **Accept and fix** | Added `unsupported_api_versions_request_with_broker_version_falls_back_and_resends` — pre-queues a delayed receive with `error_code=UNSUPPORTED_VERSION` + KIP-511 `api_keys=[{api_key=18, max_version=2}]`, then verifies the connection stays open and a v2 fallback request is dispatched in the same poll (the same poll runs `handle_initiate_api_version_requests` after `handle_completed_receives`, so the in-flight is replaced rather than left in `nodes_needing_api_versions_fetch`). Asserts via `InFlightRequests::last_sent` that the new in-flight is at version 2. |
| 3 | Test name overclaim | `disconnect_marks_node_failed_AND_RESPECTS_BACKOFF` | **Accept and fix** | Test kept its name; body extended with the three Java assertions: `can_connect=false` immediately after disconnect, `can_connect=true` after `time.sleep(reconnect_backoff_max_ms_test=100_000)`, then re-disconnect on already-disconnected node MUST NOT reset the backoff (`can_connect` stays true). Used 100_000 ms (matches `create_client` fixture's `reconnect_backoff_max=100_000`) — initial 5_000 was too low because the first disconnect's backoff is `~10_000 ± 20%`. |
| 4 | Missing Req. | Multi-in-flight disconnect fan-out untested | **Accept and fix** | Added `disconnect_with_multiple_in_flights_fans_out_in_order` — sends 3 requests on the same ready node, asserts distinct correlation ids, then `disconnect()` followed by `poll(0)` must surface ALL THREE responses in FIFO order with `was_disconnected=true`. Mirrors Java's `testDisconnectWithMultipleInFlights`. |
| 5 | Missing Req. | `send_and_receive` 4 error arms untested | **Accept and fix** | Added 5 tests in `network_client_utils.rs`: happy-path matching response; `was_disconnected=true → KafkaError::Network` with "disconnected" in the message; `version_mismatch=Some(KafkaError::UnsupportedVersion(_)) →` returns the stored error verbatim; `client.active=false → KafkaError::Network` with "shutdown"/"Client" in the message; non-matching correlation id is filtered (loop continues). Uses a `MockKafkaClient` driven by a `VecDeque<Vec<ClientResponse>>` queue — no real `Selector`. |
| 6 | Missing Req. | `least_loaded_node` 60-line tie-break untested | **Accept and fix** | Added two tests: `least_loaded_node_prefers_zero_in_flight` — two READY nodes, one with 1 in-flight, one with 0 in-flight; selection runs 16× and the 0-in-flight node MUST always win (defeats the random offset by exercising the `curr_inflight == 0` fast-path return). `least_loaded_node_returns_none_when_all_in_backoff` — single node, disconnect, then `least_loaded_node` returns `None` and `has_node_available_or_connection_ready=false`. The third Java tie-break (oldest `last_connect_attempt_ms` among `can_connect` nodes) is stable-keyed but exercised obliquely by these two tests; a third dedicated test was deferred as it would re-cover ground that the existing `cluster_connection_states::node_with_oldest_last_connect_attempt` test already pins at the layer below. |
| 7 | Suggestion | `MockSelector::reset()` rustdoc claims fidelity it doesn't have | **Accept and fix** | Updated the rustdoc to "Diverges from Java's `MockSelector.reset()` — Java does NOT touch the `ready` set; the Rust translation also clears `ready` for symmetry with the explicit `clear` semantic." |

### Test count delta

- Phase 5d Round 1: 17 tests
- Phase 5d Round 2: 27 tests (`+5` in `network_client.rs` + `+5` in `network_client_utils.rs`)
- Total lib tests: 937 (was 927)

### DoD sign-off

- `cargo build` — clean
- `cargo test` — 937 unit tests pass (was 927)
- `cargo xtask format-check` — clean
- `cargo xtask lint` — clean

### Fixup chain

A single `fixup! a0fa3f1` covers all 7 issues — they are tightly cohesive
(all in the `NetworkClient` / `NetworkClientUtils` test surface plus one
4-line behavioural fix to `do_send`).

# Critic 0 — Phase 5d (NetworkClient + NetworkClientUtils) review

Reviewing commit `a0fa3f1` — Phase 5d adds `src/network_client.rs`
(2202 LOC including tests + DoD integration) and
`src/network_client_utils.rs` (260 LOC). Java sources translated:
`NetworkClient.java` (1607) and `NetworkClientUtils.java` (154).
17 new tests; total 927 (was 910). All green locally:

```
cargo test --lib network_client
... 17 passed; 0 failed; 0 ignored
```

## Actor's flagged design choices — verified

1. **`KafkaClient::new_client_request*` takes `&mut self`**: defensible.
   Java's `nextCorrelationId` mutates `this.correlation` in-place; the
   class doc explicitly says "not thread-safe". Making the trait
   signature `&mut self` matches the actual mutation contract and avoids
   bolting an `AtomicI32` onto a counter that doesn't need atomicity.
   Trait-surface impact for Phase 6+ producers: the producer must hold
   the client behind exclusive access (single-task-per-client pattern,
   already implied by `Selector`'s design).

2. **`AbstractRequest`/`AbstractResponse` gain `Send + Sync`** but
   parent `AbstractRequestResponse` doesn't: defensible. The parent
   trait must remain `!Sync` because `RequestHeader`/`ResponseHeader`
   carry `Cell<Option<i32>>` size caches. Concrete request/response
   types are constructed without those Cells, so they CAN be `Send +
   Sync` — and `NetworkClient<S, M>: Send` requires it via the
   `aborted_sends: Vec<ClientResponse>` field (the `Box<dyn
   AbstractResponse>` inside).

3. **Wrapping correlation counter via `Wrapping<i32>`**: matches Java
   exactly. Verified `i32::MAX + 1 == i32::MIN` (-2147483648), and
   `is_reserved_correlation_id(i32::MIN)` returns `false` (since
   `i32::MIN < MIN_RESERVED_CORRELATION_ID == i32::MAX - 7`). Wrap
   semantics preserved.

4. **`node_labels: HashMap<i32, Arc<str>>` cache**: correctly invalidated.
   `initiate_connect` pre-populates the label so
   `cancel_in_flight_requests` and `do_send` get a cache hit. No
   per-message `Arc::from(format!(...))` allocation observed in
   `do_send`. Hot-path audit clean for this allocation.

5. **Internal METADATA / API_VERSIONS responses re-parsed at the
   call site**: defensible. Avoids `Any`-style downcast on a
   `Box<dyn AbstractResponse>`. The cost is one extra parse for the
   metadata/api-versions paths only; the response payload is already
   a `Bytes` clone, so no extra allocation. Phase 6 may revisit.

## Defects found

### Issue: `do_send` UnsupportedVersion path drops internal METADATA failures silently

- **File**: `src/network_client.rs:799-849`
- **Severity**: Bug (Behavior Mismatch)
- **Java Reference**: `NetworkClient.java:583-598`
- **Description**: When `builder.build(version)` returns
  `KafkaError::UnsupportedVersion`, Java's
  `doSend` distinguishes three cases for the synthetic ClientResponse:

  ```java
  if (!isInternalRequest)
      abortedSends.add(clientResponse);
  else if (clientRequest.apiKey() == ApiKeys.METADATA)
      metadataUpdater.handleFailedRequest(now, Optional.of(unsupportedVersionException));
  else if (isTelemetryApi(...) && telemetrySender != null)
      telemetrySender.handleFailedRequest(...);
  ```

  Rust's `do_send` (`network_client.rs:821-823` and `:845-847`) only
  handles the `!is_internal_request` arm:

  ```rust
  if !is_internal_request {
      self.aborted_sends.push(response);
  }
  return Ok(());
  ```

  When the request IS internal AND is a METADATA request,
  `metadata_updater.handle_failed_request(now, Some(version_err))` is
  never invoked. The `DefaultMetadataUpdater` (Phase 6+) tracks
  in-progress fetches via this callback and uses it to drive backoff
  and rebootstrap timing. Phase 5d uses `ManualMetadataUpdater` which
  ignores `handle_failed_request`, so the bug is latent in the current
  scope, but it WILL cause stuck metadata-fetch state when Phase 6
  wires `DefaultMetadataUpdater`.

  Telemetry is correctly skipped per Phase 5d scope; the METADATA arm
  is the actionable gap.

- **Expected**: When the version-mismatch path triggers an internal
  request that is METADATA (api_key.id == 3), call
  `self.metadata_updater.handle_failed_request(now, Some(e.clone()))`
  before returning `Ok(())`. Mirrors the corresponding path in
  `cancel_in_flight_requests` (which DOES fire the metadata-failure
  callback for internal METADATA on disconnect — see line 326).
- **Actual**: Internal METADATA requests with version mismatch are
  silently dropped — no aborted-send, no metadata-failed callback.
  No regression test for this path.

### Issue: `handle_api_versions_response` KIP-511 fallback path has zero test coverage

- **File**: `src/network_client.rs:614-625`
- **Severity**: Missing Requirement (test gap on a real code path)
- **Java Reference**: `NetworkClientTest.java:449-518`
  (`testUnsupportedApiVersionsRequestWithVersionProvidedByTheBroker`)
- **Description**: The non-trivial else branch in
  `handle_api_versions_response` extracts the broker-advertised
  `max_version` from the response's `api_keys` and re-queues a fresh
  `ApiVersionsRequestBuilder::with_version(max_api_version)`:

  ```rust
  let mut max_api_version: i16 = 0;
  if !data.api_keys.is_empty()
      && let Some(api_version_entry) = data.api_keys.iter().find(|a| a.api_key == 18)
  {
      max_api_version = api_version_entry.max_version;
  }
  self.nodes_needing_api_versions_fetch
      .insert(node, ApiVersionsRequestBuilder::with_version(max_api_version));
  ```

  This is the KIP-511 version-fallback handshake: when the broker
  doesn't speak the latest client `ApiVersions` version, it returns
  `UNSUPPORTED_VERSION` along with its own supported version range,
  and the client must downgrade and retry.

  Java has dedicated tests
  (`testUnsupportedApiVersionsRequestWithVersionProvidedByTheBroker`,
  `testUnsupportedApiVersionsRequestWithoutVersionProvidedByTheBroker`)
  exercising this exact branch. Rust has neither — only the
  "happy-path API_VERSIONS handshake" test
  (`api_versions_handoff_marks_node_ready`) and the "invalid response
  closes connection" test (`invalid_api_versions_response_closes_connection`).
  The fallback / re-queue path is untested.

  A bug in `find(|a| a.api_key == 18)` (e.g. if the api_key id were
  ever changed, or the field name renamed) would not be caught.
- **Expected**: A test that:
  1. Sends an initial ApiVersionsRequest at the latest version.
  2. Surfaces a delayed receive whose error_code is
     `UNSUPPORTED_VERSION` and whose `api_keys` contains an entry
     for `api_key=18, min_version=0, max_version=2`.
  3. Verifies `nodes_needing_api_versions_fetch` now holds an entry
     for the node (not closed) and that on the next
     `handle_initiate_api_version_requests` pass, a v2 ApiVersions
     request is dispatched.
- **Actual**: The KIP-511 fallback branch executes only in the
  `invalid_api_versions_response_closes_connection` test's negative
  setup (where `error_code != UNSUPPORTED_VERSION`, so the branch
  is NOT taken). No positive-path coverage.

### Issue: `disconnect_marks_node_failed_and_respects_backoff` test does not test backoff

- **File**: `src/network_client.rs:1804-1821`
- **Severity**: Bug (Test name overstates coverage)
- **Java Reference**: `NetworkClientTest.java:1101-1119` (`testCallDisconnect`)
- **Description**: The Rust test name explicitly claims "respects
  backoff" but the test body only checks `is_ready=false` and
  `connection_failed=true` after a single `disconnect()` call. Java's
  `testCallDisconnect` additionally exercises:

  ```java
  assertFalse(client.canConnect(node, time.milliseconds()));   // backoff in effect
  time.sleep(reconnectBackoffMaxMsTest);
  assertTrue(client.canConnect(node, time.milliseconds()));    // backoff expired
  client.disconnect(node.idString());
  assertTrue(client.canConnect(node, time.milliseconds()));    // re-disconnect doesn't reset backoff
  ```

  None of these three assertions are translated. The "respects backoff"
  claim in the function name is unsupported by the test body. A bug
  that allowed `disconnect()` on an already-disconnected node to
  reset the reconnect-backoff window (defeating the exponential
  backoff invariant) would not be caught.
- **Expected**: Either translate the three additional assertions
  (the `can_connect` accessor and the time-sleep manipulation are
  available on the existing test fixture), or rename the test to
  `disconnect_marks_node_failed` to drop the unsupported claim.
- **Actual**: Test passes trivially without exercising backoff
  semantics.

### Issue: Multiple-in-flight disconnect fan-out is untested

- **File**: `src/network_client.rs` (test module — no test exists)
- **Severity**: Missing Requirement
- **Java Reference**: `NetworkClientTest.java:1056-1098`
  (`testDisconnectWithMultipleInFlights`)
- **Description**: `cancel_in_flight_requests` (`network_client.rs:298-331`)
  is the spine of disconnect / close / timeout handling. It iterates
  the in-flight deque and emits one ClientResponse per request. Java's
  test asserts:

  1. Two in-flight requests on the same connection have distinct
     correlation ids (`assertNotEquals(request1.correlationId(),
     request2.correlationId())`).
  2. After `disconnect(node)`, the next `poll()` returns BOTH
     responses (`assertEquals(2, responses.size())`).
  3. The responses are returned IN ORDER (first sent, first
     surfaced — the deque ordering invariant).
  4. Each response is flagged `wasDisconnected=true`.
  5. Both callbacks fire in the same order.

  Rust's `close_clears_in_flight_requests` (line 1593) only sends
  ONE request and uses `close_connection` (which doesn't surface
  responses). The disconnect fan-out path is untested. A bug in
  `clear_all` or in `cancel_in_flight_requests`'s response-pushing
  loop ordering would be silently masked.
- **Expected**: A regression test that mirrors `testDisconnectWithMultipleInFlights`:
  send two requests on a ready node (both expecting responses, both
  internal=false), `disconnect(node)`, `poll(0)`, assert exactly two
  ClientResponses come back, in the order sent, both with
  `was_disconnected=true`.
- **Actual**: Disconnect with multiple in-flight requests is not
  exercised at all.

### Issue: `network_client_utils::send_and_receive` integration paths untested

- **File**: `src/network_client_utils.rs:86-111`
- **Severity**: Missing Requirement
- **Java Reference**: `NetworkClientUtils.java:103-128`
- **Description**: `send_and_receive` is the synchronous helper most
  Producer-internals code paths (Phase 6+) will call. Java's
  `sendAndReceive` has FOUR distinct exit paths:
  1. Matching response received (success).
  2. Response received but `wasDisconnected=true` →
     `IOException("Connection to ... was disconnected ...")`.
  3. Response received but `versionMismatch != null` → throws the
     stored exception.
  4. Loop exits because `client.active() == false` →
     `IOException("Client was shutdown ...")`.

  The Rust translation has corresponding logic at lines 96-110 that
  mirrors these exits as `KafkaError::Network` returns. The unit-test
  module (`network_client_utils.rs:132-260`) covers only:
  - `is_unavailable_combines_failed_and_delay`
  - `maybe_return_auth_failure_passes_through_none`
  - `await_ready_rejects_negative_timeout`
  - `await_ready_short_circuits_when_ready`

  None of these exercise the actual `send_and_receive` round-trip,
  including the THREE error-translation arms that map Java's
  `IOException` / `versionMismatch()` onto `KafkaError::Network` /
  `KafkaError::UnsupportedVersion`. A bug in the disconnect-detection
  arm — e.g. swallowing the `was_disconnected` flag and returning
  `Ok(response)` — would not be caught.
- **Expected**: At least one async test that drives `send_and_receive`
  through:
  1. Happy-path round-trip (already covered indirectly by the DoD
     `loopback_metadata_request_response` at the `NetworkClient`
     level — could be lifted into a `send_and_receive` test).
  2. The disconnect-during-flight arm (`response.was_disconnected()`
     → `KafkaError::Network`).
  3. The shutdown arm (`client.initiate_close()` mid-call →
     `KafkaError::Network("Client was shutdown ...")`).

  A `MockKafkaClient` over a `Vec<ClientResponse>` queue would
  suffice; no real `Selector` needed.
- **Actual**: 0 of 4 `send_and_receive` exit paths have direct test
  coverage.

### Issue: `least_loaded_node` 60-line tie-break logic is untested

- **File**: `src/network_client.rs:1024-1090`
- **Severity**: Missing Requirement
- **Java Reference**: `NetworkClientTest.java:777-892`
  (`testLeastLoadedNode`,
  `testLeastLoadedNodeProvideDisconnectedNodesPrioritizedByLastConnectionTimestamp`,
  `testLeastLoadedNodeConsidersThrottledConnections`,
  `testHasNodeAvailableOrConnectionReady`)
- **Description**: `least_loaded_node` implements a non-trivial
  preference order:
  1. Among `can_send_request` nodes, the one with fewest in-flight
     wins; tie broken by the random offset.
  2. Else, any `is_preparing_connection` node.
  3. Else, the `can_connect` node with the OLDEST
     `last_connect_attempt_ms` (verified at line 1064: `>` not `<`,
     mirroring Java line 798).
  4. Else, `LeastLoadedNode::new(None, at_least_one_connection_ready)`.

  The `at_least_one_connection_ready` flag (line 1035, 1044-1049)
  determines whether the producer should wait or proceed without
  metadata. This matters for the `DefaultMetadataUpdater` rebootstrap
  trigger (which Phase 5d doesn't translate, but `MetadataUpdater`
  callers DO use the flag).

  Java has four dedicated tests for this 60-line method. Rust has
  zero. Bugs in the tie-break ordering (e.g. flipping `<` and `>` on
  line 1064) or in the flag computation would silently slip through.
- **Expected**: At minimum, a smoke test that verifies:
  1. Three nodes — one ready with 0 in-flight, one ready with 5
     in-flight, one connecting → expect node 1 (0 in-flight).
  2. All nodes failed but two `can_connect`, one with older last-
     attempt → expect the older one.
  3. `at_least_one_connection_ready` reflects whether ANY node
     passes both `connection_states.is_ready` AND
     `selector.is_channel_ready`.
- **Actual**: No test coverage; only the `least_loaded_node` panic
  on empty-cluster is exercised at all (and only via the Rust unit
  test `wakeup_does_not_panic` which doesn't actually call it).

### Issue: `MockSelector::reset()` rustdoc claims fidelity it doesn't have

- **File**: `src/network_client.rs:1293-1302`
- **Severity**: Suggestion (test-fixture documentation drift)
- **Java Reference**: `MockSelector.java:238-242`
- **Description**: The Rust comment says

  ```rust
  /// Reset everything including `ready`. Mirrors
  /// `MockSelector.reset()` (only used by test setup).
  fn reset(&self) { ... s.ready.clear(); ... }
  ```

  Java's `reset()` does:

  ```java
  public void reset() {
      clear();
      initiatedSends.clear();
      delayedReceives.clear();
  }
  ```

  Java does NOT clear `ready`. The Rust translation does. The method
  is marked `#[allow(dead_code)]` and isn't exercised in any current
  test, so the divergence is harmless TODAY — but the rustdoc claims
  "Mirrors `MockSelector.reset()`" which is false. If a future test
  switches from `clear()` to `reset()` expecting Java semantics,
  it would lose the `ready` set unexpectedly.
- **Expected**: Either drop the `s.ready.clear();` line (matching
  Java exactly) or update the rustdoc to say
  "Diverges from Java by also clearing `ready` set; preserved here
  for symmetry with the explicit `clear` semantic."
- **Actual**: Comment claims a mirror that doesn't exist.

## Verdicts on actor's deferrals

### TLS handshake at `NetworkClient` layer — accept the deferral

The actor's rationale (Phase 5b-2 + 5b-3 + 5c-2 already cover the
rustls handshake + KafkaChannel SSL wiring + Selector SSL drain) is
defensible. Verified `ssl_transport_layer.rs:1109-1153` does exercise
a real rcgen self-signed peer + full TLSv1.3 handshake + populated
cipher information. The Phase 5d test would re-cover those layers
with no new behavioral surface — only the additional plumbing of the
NetworkClient state machine on top, and that surface is exercised by
the loopback `MetadataRequest` test (which uses `Selector` +
`PlaintextChannelBuilder` end-to-end).

PLAN.md 280's wording "TLS handshake test connects to a self-signed
broker" can be read either way; the Phase 5b-2 test does meet the
literal wording (it does connect to a self-signed broker, just not
through `NetworkClient::poll`). The actor explicitly captured the
"Flag for Critic 0" rationale in `tls_handshake_test_skip` rustdoc
(`network_client.rs:2179-2197`) with concrete instructions for what
the test would be if Critic disagreed. I do not.

### Skipped Java tests — accept all five

- `testReconnectAfterAddressChange` — needs Mockito-style
  `ClientTelemetrySender` and `AddressChangeHostResolver`; address-
  change is exercised at `cluster_connection_states` level in Phase
  4c.
- `testRebootstrap` / `testInflightRequestsDuringRebootstrap` —
  requires `DefaultMetadataUpdater`; deferred to Phase 6.
- Throttling (`testConnectionThrottling`,
  `testConnectionTimeoutAfterThrottling`) — telemetry-adjacent;
  state-machine effects covered in Phase 4c.
- Connection-setup-timeout tests
  (`testConnectionSetupTimeout`) — covered by
  `cluster_connection_states::nodes_with_connection_setup_timeout`
  in Phase 4c.
- Telemetry (`testTelemetryRequest`) — Phase 9 scope.

## Summary

- **Blocking**: 0
- **Bug**: 1 (`do_send` UnsupportedVersion drops internal METADATA
  callback)
- **Missing Requirement**: 4 (KIP-511 fallback test gap;
  multi-in-flight disconnect test gap; `send_and_receive` arm test
  gap; `least_loaded_node` test gap)
- **Test name overstates coverage**: 1
  (`disconnect_marks_node_failed_and_respects_backoff`)
- **Suggestion**: 1 (`MockSelector::reset()` rustdoc drift)

Round 1 verdict: Phase 5d is mechanically correct on the producer
hot path that Phase 6 will exercise; the `NetworkClient::poll`
state-machine ordering matches Java; the correlation-id wrap, the
API_VERSIONS handshake handoff, the timeout-disconnect path, and
the `node_labels` cache all hold up under read-through. The single
real bug (`do_send` UnsupportedVersion + internal METADATA) is
latent until Phase 6 wires `DefaultMetadataUpdater` — but it WILL
manifest then. The four test-coverage gaps each map to a code path
that already exists and is taken in production: a regression in any
of them would slip through the current 17-test suite.

Recommend: fix the `do_send` METADATA callback gap (~5 lines), add
one test for each of the four uncovered paths (KIP-511 fallback,
multi-in-flight disconnect, `send_and_receive` disconnect arm,
`least_loaded_node` 0-in-flight tiebreak). Rename the
`disconnect_marks_node_failed_and_respects_backoff` test or add the
three missing backoff assertions. The `MockSelector::reset()` doc
fix is a one-line rustdoc edit.

