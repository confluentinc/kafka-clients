---
name: phase9b-sasl-channel-wiring
description: Phase 9b — SaslAuthenticator trait + ChannelAuthenticator enum + JAAS PLAIN-only parser + producer gate lift
metadata:
  type: project
---

# Phase 9b — SASL config & channel wiring (closed accept-pending-Critic)

**Why:** Phase 9a left `SaslChannelBuilder::build_channel()` returning
`UnsupportedOperation` because Phase 5b-3's `Authenticator` trait
doesn't thread a transport reference. Phase 9b unblocks it and ships
the config plumbing (`sasl.jaas.config`, `sasl.username`/`sasl.password`
shortcut, mechanism validator narrowing) that 9c integration tests need.

**How to apply:** Reference for future SASL-touching phases (especially
9c, 9d, 9e, 9f, 9g). The architectural patterns below are stable.

## Architectural decision: SaslAuthenticator + ChannelAuthenticator enum

Critic 9 (9a review) recommended either (a) a separate
`SaslAuthenticator` trait OR (b) a `ChannelAuthenticator` wrapper.
Phase 9b implemented **both** — they solve different problems:

- `SaslAuthenticator` trait: solves the transport-reference plumbing.
  Its `authenticate(&mut self, transport: &mut dyn TransportLayer)`
  is fundamentally different from non-SASL `Authenticator::authenticate
  (&mut self)`. Keeping it a sibling trait avoids touching every
  PlaintextAuthenticator/SslAuthenticator call site.
- `ChannelAuthenticator` enum: solves the downcasting at the
  `KafkaChannel` level. The enum carries either `Box<dyn Authenticator>`
  or `Box<dyn SaslAuthenticator>` and exposes a uniform
  `authenticate(&mut self, transport: &mut dyn TransportLayer)`
  that dispatches correctly (Network arm ignores transport; Sasl
  arm passes it through).

`BoxedAuthenticator` is now a `pub type` alias for
`ChannelAuthenticator`, kept to minimise churn at the channel call
sites that already use the alias.

The split-trait + enum-bridge has no runtime cost (the enum's match
compiles to a single tag check). This is the deviation-from-Java
where Rust *improves* on Java's shape — Java's `Authenticator` interface
forces all subclasses to share `authenticate()` even though only SASL
actually uses network I/O at that call.

## KafkaChannel::prepare() borrow split

```rust
let transport = self.transport_layer.as_mut();
let authenticator = &mut self.authenticator;
let result: io::Result<()> = (|| {
    if !transport.ready() { transport.handshake()?; }
    if transport.ready() && !authenticator.complete() {
        authenticator.authenticate(transport)?;
    }
    Ok(())
})();
```

Borrow checker requires manually splitting field borrows because the
closure can't simultaneously borrow `self.transport_layer` and
`self.authenticator`.

## JAAS parser: PLAIN-only

`src/common/security/jaas_config.rs` recognises only:
```
org.apache.kafka.common.security.plain.PlainLoginModule <control-flag>
    username="..." password="...";
```

~270 LOC including 15 unit tests. Full JAAS grammar (multi-module
contexts, escaped quotes, arbitrary keys) is explicitly out of scope —
non-PLAIN configs are rejected at the parser boundary with:
"this client supports PLAIN mechanism only — see sasl.username /
sasl.password as an alternative."

## sasl.username / sasl.password shortcut keys

Fresh-impl extension NOT present in Java's `ProducerConfig`. JAAS
wins precedence when both are set (canonical-Java-source semantics).

## Producer-side gate

`build_production_network_client` at `kafka_producer.rs:660` lifted
for `SASL_PLAINTEXT`. `SSL` / `SASL_SSL` still gated with new error
("requires SSL plumbing not yet wired in this milestone"). The
`resolve_plain_credentials` helper handles the JAAS-first /
shortcut-fallback / partial-credentials-error matrix.

## EOF→Failed transition (S2)

`SaslClientAuthenticator::authenticate()` wraps the existing state
machine in an outer post-processor:

```rust
let result = self.authenticate_inner(transport);
if let Err(ref e) = result
    && self.state != SaslState::Failed
    && matches!(e.kind(), io::ErrorKind::UnexpectedEof | io::ErrorKind::ConnectionReset)
{
    self.state = SaslState::Failed;
    self.failure = Some(KafkaError::Authentication("EOF during SASL handshake".to_owned()));
}
```

Without this, EOF mid-handshake left the state stuck at
`ReceiveApiVersionsResponse` with `failure = None`; a buggy retry
layer would wedge. Now re-invocation surfaces the captured
`KafkaError` deterministically.

## Partial-write resume (S3)

`MockTransport.max_write_per_call: Option<usize>` caps accepted bytes
per call. The new `partial_writes_resume_correctly_to_complete` test
caps at 4 bytes and drives the full PLAIN exchange. Result: the
existing `flush_pending_send` top-guard in `authenticate_inner`
handles resumption correctly — no refactor to defer state transition
(to match Java's `pendingSaslState`) was needed.

## Files added/touched

New:
- `src/common/security/jaas_config.rs` (~270 LOC, 15 tests)

Modified (interesting):
- `src/common/network/authenticator.rs` — new SaslAuthenticator trait
  + ChannelAuthenticator enum (lines ~73-200)
- `src/common/network/kafka_channel.rs` — BoxedAuthenticator alias,
  prepare() borrow split, two test-site updates
- `src/common/network/sasl_channel_builder.rs` — build_channel +
  build_sasl_ssl_channel landed; deferral test replaced with 3 tests
- `src/common/security/authenticator/sasl_client_authenticator.rs` —
  outer wrapper for EOF→Failed, SaslAuthenticator trait impl,
  password() pub(crate) accessor, partial-write test, tag-field N1
  fix, correlation-id N2 fix
- `src/common/config/sasl_configs.rs` — SASL_USERNAME / SASL_PASSWORD
  constants + add_client_sasl_support registrations
- `src/producer/producer_config.rs` — validator lift + milestone-1
  mechanism narrowing + 4 new tests
- `src/producer/kafka_producer.rs` — gate lift + resolve_plain_credentials
  helper + 3 new tests
