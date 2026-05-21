---
name: phase9b-review-patterns
description: SASL channel-wiring review — SaslAuthenticator-trait + ChannelAuthenticator-enum design audit, JAAS PLAIN-only parser, partial-write resume walkthrough, producer-side gate scope verification
metadata:
  type: feedback
---

# Phase 9b — SASL channel wiring + config plumbing review patterns

Carry-forward for future Critic reviews of trait-reshape work + config-plumbing translations + Java→Rust polymorphism collapse.

## High-yield audit areas

1. **Sibling-trait + enum-bridge as the right shape for polymorphism with disjoint method signatures.** Java's `Authenticator` interface forces all subclasses to share `authenticate()` even though only SASL uses network I/O. The Rust translation correctly introduced a sibling trait + enum:
   ```rust
   trait Authenticator { fn authenticate(&mut self) -> io::Result<()>; }   // no transport
   trait SaslAuthenticator { fn authenticate(&mut self, transport: &mut dyn TransportLayer) -> io::Result<()>; }
   enum ChannelAuthenticator { Network(Box<dyn Authenticator>), Sasl(Box<dyn SaslAuthenticator>) }
   ```
   **Verify the sibling-vs-collapse decision** by tracing: would collapsing to one trait force every existing call site to plumb a useless `&mut dyn TransportLayer`? If YES, the sibling is justified. If NO (e.g., the param could be made optional or carried via interior mutability), the collapse may be cleaner.

2. **Partial-write resumption requires a specific invariant: state must advance only after the network operation completes.** Java uses `pendingSaslState` to defer the next state until `netOutBuffer.completed() == true`. Rust eagerly advances state after `queue_request` returns, then relies on `flush_pending_send` at the loop top + `receive_response` early-return on `Ok(None)` to absorb the partial-write case. **Walk through the resumption path step-by-step** before accepting "no refactor needed" claims:
   - Step 1: First `authenticate()` — `queue_request` partial-writes, sets `pending_send = Some`, `current_request_header` NOT set, advances state to Receive*.
   - Step 2: Loop iterates to Receive*, `receive_response` calls `receive_raw_token` → `Ok(None)` (no bytes) → returns `Ok(())` from `authenticate_inner`.
   - Step 3: Next `authenticate()` — top-guard `flush_pending_send` completes the send, sets `current_request_header`, OP_WRITE removed.
   - Step 4: Loop iterates to Receive*, `receive_response` reads response.

   The critical invariant: `receive_raw_token` must always return `Ok(None)` first when no payload is buffered, before `current_request_header.take()` is reached. If `receive_raw_token` ever reaches `take()` before the flush completes, you'd see "received SASL response with no pending request header". Verify by tracing.

3. **JAAS parser PLAIN-only choice — option (b) over full grammar.** Acceptable for Milestone 1 if:
   - The rationale is clear in module rustdoc.
   - Non-PLAIN LoginModule names produce a clear "this client supports PLAIN mechanism only" error with the offending name.
   - The rejection paths (different module, unknown PLAIN option, missing user/pass, malformed quoted) all have unit-test coverage.
   - Full-grammar tar pit is explicitly named as out-of-scope.

   Typical sizes: ~270 LOC + 15 unit tests for PLAIN-only; full grammar would be ~700+ LOC.

4. **Producer-side SASL_SSL deferral — verify the gate scope is correct.** When the gate at `kafka_producer.rs` rejects `SecurityProtocol::Ssl | SaslSsl` with "SSL plumbing not yet wired", check:
   - Is `SslTransportLayer` / `SslChannelBuilder` actually wired through Phase 5b? (YES — they exist.)
   - Is there a producer-config-key-to-`Arc<ClientConfig>` bridge in ProducerConfig? (NO — the gate is the right scope.)
   - Will Phase 9c fold-in 8e fill this gap? (YES — per NOTES.md.)
   
   If all three answers match, the deferral is correct. If `SslTransportLayer` doesn't exist OR the config bridge IS wired but the gate is still on, flag as 9b miss.

5. **Validator error message Java parity vs. fresh-impl narrowing.** Two different concepts:
   - **Java parity**: error string matches `CommonClientConfigs.postValidateSaslMechanismConfig` exact text.
   - **Fresh-impl narrowing**: Milestone-1 layer adds a NEW rejection (e.g., "Unsupported SASL mechanism: GSSAPI. Milestone-1 supports only PLAIN") that Java doesn't have.
   
   Both are acceptable IF clearly labelled. Don't flag a narrowing layer as "diverges from Java" — that's expected. Flag it ONLY if the doc claims Java parity but the message doesn't match.

6. **Test-name-vs-assertion mismatch trap on tagged-field tests.** "round_trip_on_authenticate_v2_response" implies parser preserves the tagged fields in the parsed struct. If the test only asserts the encoder writes the bytes (without capturing the parsed response from the authenticator), the test name overclaims. Either rename or extend the assertion. Phase 9.0's hex fixtures + `Readable::read_tagged_field` may already cover the parser surface separately — that doesn't make the name accurate.

7. **`SaslClientAuthenticator::principal()` returns anonymous, Java returns username.** Acceptable Milestone-1 deferral (client-only — principal unused for ACL). But it affects log-line identity (you'd see `User:ANONYMOUS` for an authenticated session). One-line fix: `KafkaPrincipal::new("User", &self.credentials.username)`. Flag as Suggestion for 9c when integration tests start exercising log output.

## Common false-positive traps to avoid

- **Don't flag the sibling-trait + enum design as "ceremony"** if the trait signatures genuinely diverge (different `authenticate()` arg lists). It's the cleanest Rust shape for Java's "all-inherit-one-method" polymorphism when the method args don't apply uniformly.
- **Don't flag the `build_channel` vs `build_sasl_ssl_channel` split** as a "deviation from Java" — the split is forced by rustls needing an SNI hostname that the generic trait method can't carry. Java avoids this by threading `SelectionKey` (which gives access to the SocketChannel's remote address) — different I/O model, different ergonomics.
- **Don't flag JAAS PLAIN-only as missing tests** for SCRAM/OAUTHBEARER/GSSAPI configs — those are explicitly out-of-Milestone-1 scope and the rejection path's 1-test coverage is sufficient.
- **Don't flag the producer-side SASL_SSL gate** as a "9b miss" until you've confirmed both the SSL transport layer infrastructure exists AND the config-to-`Arc<ClientConfig>` plumbing is absent. The dependency direction matters.
- **Don't flag Milestone-1 narrowing error messages** as "diverges from Java" — Java's `postValidateSaslMechanismConfig` only checks null/empty; the Milestone-1 layer is a NEW rejection on top, not a Java parity claim.

## Verification recipe for trait-reshape work

1. **List existing impls of the original trait** (Plaintext, SSL, etc.). Audit `git diff` for any non-doc changes to them. If unchanged, the reshape preserved Phase 5b-3 work.
2. **List new impls of any sibling trait** (SaslClientAuthenticator here). Verify each method is wired correctly.
3. **List enum dispatch sites** (`ChannelAuthenticator::authenticate`, `principal`, etc.). Verify all match arms call the right method.
4. **Trace one call from each variant** through to the inner impl. Counting-stub tests are good for this (record `fetch_add` on each method to prove the right one was called).
5. **Verify no double-borrow or borrow-checker workarounds** at the call sites. `KafkaChannel::prepare()` splits its borrow of `self` cleanly — `let transport = self.transport_layer.as_mut(); let authenticator = &mut self.authenticator;` — that's safe and idiomatic.

## Cross-phase notes

- Phase 9a (Round 1): clean. 4 followups (S1/S2/S3/N1/N2) carried into 9b.
- Phase 9b: 8 commits, +28 tests, 1299 → 1327. All 9a followups resolved. New deferrals to 9c: SASL_SSL gate, integration testing, J-runtime hex-fixture cross-verification.
- Phase 9c will deliver: SSL config plumbing through ProducerConfig (`Arc<ClientConfig>` build), SASL_SSL gate lift, Testcontainers integration with multi-listener broker (PLAINTEXT 9092, SSL 9096, SASL_PLAINTEXT 9094, SASL_SSL 9095), J-runtime hex-fixture cross-verification.
