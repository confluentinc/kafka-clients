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
