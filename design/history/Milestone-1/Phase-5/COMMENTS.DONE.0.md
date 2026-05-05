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
