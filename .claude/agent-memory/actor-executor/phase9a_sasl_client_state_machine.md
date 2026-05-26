---
name: phase9a-sasl-client-state-machine
description: SaslClientAuthenticator PLAIN state machine + SaslChannelBuilder — sync state-loop design, channel-wiring deferral, generator-level credential redaction
metadata:
  type: project
---

Phase 9a closed (5 commits). Key learnings.

**Generator-level credential redaction (option a) was non-invasive.**
3 helpers + 3 wirings in generator/src/lib.rs (`is_credential_field_name`,
`has_credential_field`, `generate_redacted_debug_impl`), plus a one-line
change to `generate_struct_derives_and_impls` to conditionally drop
`Debug` from the derive list. The 3 call sites that emit struct-debug
(top-level message, nested struct, common struct) each got a single
`generate_redacted_debug_impl` call inserted right before
`generate_display_impl`. `Display` continues to delegate to `Debug`
via the existing `write!(f, "{:?}", self)` pattern and inherits the
redaction automatically. **How to apply: if Critic flags more
credential-bearing fields in future phases (e.g. `client_secret`),
add the field name to `is_credential_field_name`. No structural
changes needed.**

**State machine: sync loop, not async.** Java's `authenticate()` is
called repeatedly from the Selector's main poll loop, advancing the
state machine one step per call and returning when waiting on I/O.
The Rust translation uses the same pattern: sync fn, internal `loop`
that advances state transitions while no I/O blocking is needed,
returns `Ok(())` when further progress requires bytes. The transport's
`read()` returns `Ok(0)` on `WouldBlock` so NetworkReceive's
`complete()` returns false → state stays put → outer poll loop wakes
us on the next select. **Why: the Phase 5b-3 `Authenticator` trait
already exposes a sync `authenticate(&mut self)` and converting to
async would touch every call site. Sync composes naturally with the
existing pattern.**

**Channel-side SASL wiring deferred to 9b.** The Phase 5b-3
`Authenticator` trait's `authenticate(&mut self) -> io::Result<()>`
does NOT thread a transport reference. The SASL state machine needs
the transport on every step (to flush sends, drive receives), so it
cannot fit the existing trait without:
  (a) reshaping the trait to `authenticate(&mut self, transport: &mut dyn TransportLayer)`, or
  (b) holding a transport `Rc<RefCell<dyn TransportLayer>>` on the
      authenticator side
Both are intrusive. Phase 9a kept `SaslClientAuthenticator` standalone
(fully implemented + tested) and `SaslChannelBuilder::build_channel`
returns `KafkaError::UnsupportedOperation`. **How to apply in 9b:
extend the Authenticator trait with the transport parameter; the
PLAINTEXT and SSL authenticators are no-op for `authenticate()`
so the change is backwards-compatible.**

**`SecurityProtocol` got SaslPlaintext/SaslSsl variants.** Previously
deferred per the reserved-id constants. Phase 9a landed them because
`channel_builders.rs` dispatch needed concrete variants to match
against. Added `is_sasl()` and `uses_ssl()` predicates mirroring
Java's idiom. The producer-side gate at `kafka_producer.rs:660`
still rejects non-PLAINTEXT — user-visible behaviour for
`KafkaProducer::new` is unchanged until 9b lands the config plumbing.

**`PlainCredentials` struct replaces Java's `Subject` + JAAS
indirection.** JAAS config parsing deferred to 9b. The 9a struct
accepts `username: String, password: String` directly. Hand-emitted
Debug masks the password.

**`MIN/MAX_RESERVED_CORRELATION_ID` + `is_reserved()` translated
verbatim from Java.** These are part of the public surface used
by `NetworkClient` to disambiguate SASL traffic from in-flight
Kafka requests. Range is `i32::MAX - 7 ..= i32::MAX` (8 ids); the
authenticator wraps when exhausted. Translation of Java
`SaslAuthenticatorTest.testCorrelationId` covers wrap semantics.

**Test count: 1275 → 1299 (+24).** Breakdown:
- 2 lib tests (commit 1, generator-level redaction round-trip)
- 4 generator unit tests (commit 1)
- 2 lib tests (commit 2, Suggestion 3 success-NONE assertions)
- 10 SaslClientAuthenticator unit tests (commit 3)
- 6 SecurityProtocol expansion tests (commit 4)
- 4 channel_builders dispatch tests (commit 4)
- 5 SaslChannelBuilder construction tests (commit 4)
  (Note: the 4 generator tests don't count in lib test totals.)

**RFC 4616 PLAIN token: `\0user\0pass` (one NUL before, one between,
no trailing NUL).** Inlined as `build_plain_token` because for PLAIN
we don't need a JCA-style pluggable SASL provider. Test
`plain_token_rfc_4616_byte_shape` pins the literal byte sequence.

**Java error message parity verified.** The 9a tests assert specific
error string content like "Client SASL mechanism 'PLAIN' not enabled
in the server, enabled mechanisms are [SCRAM-SHA-512]" — this is the
Java string format from
`SaslClientAuthenticator.handleSaslHandshakeResponse`. PLAN.md DoD #1
("Auth failure surfaces as KafkaError::Authentication with a message
matching Java's error string") is satisfied for the handshake-error
and authenticate-error paths.
