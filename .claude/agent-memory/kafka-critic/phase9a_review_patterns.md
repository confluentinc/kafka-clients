---
name: phase9a-review-patterns
description: SASL client authenticator state-machine review — RFC 4616 byte exactness, Java state-machine collapse safety (INTERMEDIATE → ReceiveAuthenticate), Java List<String>.toString() parity, eager-vs-deferred state transitions, generator-level credential redaction audit
metadata:
  type: feedback
---

# Phase 9a — SASL PLAIN state machine review patterns

Carry-forward for future Critic reviews of state-machine translations and credential-handling work.

## High-yield audit areas

1. **Java state collapse safety** — Java's `SaslClientAuthenticator` has 8 client-side states (INTERMEDIATE, CLIENT_COMPLETE among them). For PLAIN, Java goes through `INITIAL → INTERMEDIATE → COMPLETE` (skipping CLIENT_COMPLETE because PLAIN's `saslClient.isComplete()` is true after one round-trip + `noResponsesPending` is true). Rust collapsed this into `SendInitialToken → ReceiveAuthenticateResponse → Complete`. **For PLAIN this is faithful**; for SCRAM (deferred to future milestone) this would NOT be faithful and would need re-introduction of CLIENT_COMPLETE.

2. **RFC 4616 PLAIN token byte exactness** — `\0username\0password` with ONE leading NUL, ONE separator NUL, NO trailing NUL. Test pin: `assert_eq!(token, b"\0alice\0supersecret")`. Hand-derived tokens with a trailing NUL or `\0authzid\0username\0password` (the more general RFC form with non-empty authzid) would NOT be byte-compatible with Kafka brokers.

3. **Java `List<String>.toString()` parity in error messages** — Java's `List.toString()` produces `[elem1, elem2]` WITHOUT quotes around individual strings. Rust's `format!("{:?}", vec)` produces `["elem1", "elem2"]` WITH quotes. This is a real divergence in error-message content when the user reads logs. Fix: `format!("[{}]", vec.join(", "))` matches Java exactly. Audit any `format!("{:?}", vec_of_string)` in user-facing error paths.

4. **Generator-level credential redaction audit** — when a generated `*Data` struct has a credential field (e.g. `auth_bytes`), the generator must:
   - Drop `Debug` from the derive list
   - Emit a hand-written `impl fmt::Debug` that renders the credential as `<redacted>`
   - Display can stay as `write!(f, "{:?}", self)` and will inherit the redaction
   
   Audit checklist:
   - Verify the *generated* file (not the source), e.g. `target/debug/build/.../sasl_authenticate_request_data.rs`, NOT `generator/src/lib.rs` patterns alone
   - Test both `{:?}` and `{}` (Display) — Suggestion 1's original concern was that `Display`-via-Debug-delegate would leak even after wrapper-level Debug was fixed
   - Confirm the credential-field-name list is **small and explicit** (`auth_bytes` only, currently) — over-broad patterns (`password`, `token`, `secret`) would false-positive on non-credential fields like `session_token` or `delegation_token` if they exist in any JSON spec
   - Audit `generator/messages/*.json` for other fields with credential-shaped names

5. **Eager vs deferred state transition (Java `pendingSaslState`)** — Java defers state advancement when `netOutBuffer.completed() == false`, latching the next state into `pendingSaslState`. Rust advances eagerly after `queue_request`. For the producer use case this is invisible because the broker can't respond to a partial request (TCP ordering). But a test using a partial-write mock would catch any latent invariant break (e.g. `current_request_header == None` when entering Receive state). Phase 9a doesn't have a partial-write test — flag as suggestion for 9b/9c.

6. **EOF → Failed state transition** — Java relies on JVM exception propagation; the authenticator's state stays as-is on EOF and the Selector closes the channel. Rust's `io::Result<()>` doesn't propagate beyond the function; if the caller re-invokes `authenticate()` after EOF, the state machine wedges (no Failed transition). For Phase 9a this is acceptable because the upper layer hasn't been wired yet. But there's no pinning test that 9b's channel wiring will detect EOF. Suggest: set `state = Failed` + `failure = Some(KafkaError::Authentication("EOF during SASL handshake"))` on EOF.

7. **`build_channel() = UnsupportedOperation` deviation** — acceptable as a deliberate scope deferral IF the brief explicitly says "no integration test yet for this sub-phase" AND a pinning test locks the deferred contract AND the producer-side gate still rejects the feature. Audit the 3-leg test:
   - The state machine has comprehensive unit tests via direct `::new`
   - The producer gate (`kafka_producer.rs:660` style) still rejects the feature
   - A pinning test asserts the deferred entry point returns `UnsupportedOperation` with a Phase-number breadcrumb

8. **Reserved correlation-id range arithmetic** — `MIN = i32::MAX - 7`, `MAX = i32::MAX`. Range is 8 values (inclusive). Wrap arithmetic: `correlation_id.wrapping_add(1)` after MAX yields `i32::MIN` (negative). The reset check `if !is_reserved(self.correlation_id)` catches this. Test pattern: insert `2 * range_size = 16` calls and assert `seen.len() == 8`. The previous Phase 9a code uses `(MAX - MIN) * 2 = 14` instead of `2 * (MAX - MIN + 1) = 16` — works but has a misleading comment.

## Common false-positive traps to avoid

- **Don't flag `INITIAL` / `INTERMEDIATE` / `CLIENT_COMPLETE` as "missing"** if PLAIN-only scope. Java's `CLIENT_COMPLETE` is only reachable for mechanisms where the client sends another response after broker's auth-success — not PLAIN.
- **Don't flag tagged-field round-trip as "weak coverage"** if the message-type's hex fixtures (Phase 9.0) already cover byte-level parity at the protocol layer. The authenticator-level test just proves "state machine doesn't crash on tagged fields"; that's a different (weaker) contract and is fine to keep weaker.
- **Don't flag `ApiVersionsRequest` v0 populating v3+ fields (`client_software_name`)** — at v0 serialization, these fields are skipped per the JSON spec versions filter. The code structure is fine.

## Verification recipe for state-machine translations

When reviewing any Java state-machine translation:

1. Read the Java enum (`SaslState` here) — identify all states + count
2. Read the Java `authenticate()` (or equivalent) — identify all transitions including fall-through (Java's switch fall-through is significant in this file)
3. Identify mechanism-specific paths (PLAIN goes through different states than SCRAM)
4. Verify the Rust enum has at least all states reachable for the in-scope mechanisms
5. Verify the Rust loop is functionally equivalent to Java's switch (including `pendingSaslState` deferral semantics)
6. Check the error path — Java exceptions become Rust `Result::Err`; the captured failure should match Java's `IllegalSaslStateException` message format
7. Audit the error-message string for `format!("{:?}", ...)` vs Java's `toString()` differences (List<String> being the canonical trap)

## Cross-phase notes

- Phase 9.0 generator+wire-protocol prerequisite landed clean (Round 1)
- Phase 9b will: reshape `Authenticator` trait OR introduce SASL-specific trait + wire SaslChannelBuilder::build_channel; add partial-write test coverage; bridge typed ProducerConfig→PlainCredentials
- Phase 9c-g will validate against Testcontainers; that's where the J-runtime hex-fixture cross-verification (deferred from 9.0) finally happens
