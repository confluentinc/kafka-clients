---
name: phase9g-option-a-zero-change
description: Phase 9g closed Option A — both scopes (validator-time rejection AND broker-handshake rejection) already pinned by 9b + 9a; recipe for resolving similar "reframed by close stanza" ambiguities
metadata:
  type: project
---

# Phase 9g — scope-resolved to Option A (zero-code-change close)

Phase 9g had a documented scope ambiguity: the original sub-phase
ladder (`NOTES.md:52`) mandated a **client-side validator
rejection** unit test; the 9e and 9f close stanzas reframed it as
a **broker-side handshake rejection** integration test. The 9g
Actor brief required a defensible decision before writing code.

**Resolution: both scopes were already pinned by prior phases.
9g closes as a pure NOTES.md update — no production code, no new
tests.**

## The two pinning citations

| Scope | Pinned by | File:line | Assertion strength |
|---|---|---|---|
| Validator rejection (original ladder) | Phase 9b | `src/producer/producer_config.rs:1865-1974` (4 tests: SCRAM-SHA-512, OAUTHBEARER, GSSAPI-default, non-SASL bypass) | `matches!(KafkaError::Config(_))` + `.contains("Unsupported SASL mechanism: <mech>")` |
| Broker-handshake rejection (9e/9f reframe) | Phase 9a | `src/common/security/authenticator/sasl_client_authenticator.rs:1122-1164` (`handshake_unsupported_mechanism_fails_with_java_message`) | `matches!(KafkaError::Authentication(_))` + `is_fatal()` + `!is_retriable()` + `assert_eq!` exact Java parity error string |

The Phase 9a unit test drives the full PLAIN handshake state
machine through a `MockTransport` and pins the **exact** Java
`List<String>.toString()` error string format
(`"Client SASL mechanism 'PLAIN' not enabled in the server,
enabled mechanisms are [SCRAM-SHA-512]"`). This is a *stronger*
assertion than a live integration test could produce, because it
uses `assert_eq!` instead of substring containment.

## Why integration coverage doesn't add evidence

1. **The wire-code path is already pinned at unit level** with
   an exact-string assertion.
2. **The test broker is hard-coded to advertise only PLAIN**
   (`tests/common/kafka_cluster.rs:202`). Exercising scenario (2)
   live would require a second container image or admin-API
   broker reconfiguration not supported by the test harness.
3. **9f already pinned the propagation chain end-to-end** on a
   different SASL error code (`SaslAuthenticationFailed`). The
   `process_disconnection` → `metadata.fatal_error` → notify path
   is the same regardless of which SASL wire code triggered it;
   only the *message rendering* differs, and that piece is the
   one pinned by the Phase 9a unit test.

## The "close-stanza reframing" pattern (decision-rule)

Sub-phase 9e's close stanza wrote: *"9g — unsupported-mechanism
path. Client requests a mechanism the broker has not enabled..."*
This is what Critic-style narrative calls **"close-stanza drift"**:
when phase N's close stanza re-describes a deferred phase N+1
scope, it can drift away from the ladder's original verification
form.

Rule of thumb when resolving such ambiguity:

1. **Read the original ladder line.** Its "verification" column
   is authoritative.
2. **Check both scopes against existing code.** If both are
   pinned, the phase closes as Option A regardless of how the
   close-stanza reframed it.
3. **Document the resolution explicitly** in the close stanza —
   name the inference vs the mandate. Future Critics will check
   this trail.

## Apache 9b validator narrowing — recipe for Milestone-2 lift

`reject_milestone_1_unsupported_sasl_mechanism` at
`producer_config.rs:1252-1272` is the **only** site narrowing
`sasl.mechanism` to PLAIN. When SCRAM / OAUTHBEARER ships in
Milestone-2, that single guard moves from a hard reject to a
mechanism-specific dispatch in `SaslClientAuthenticator`'s
`init_sasl_state`. The corresponding test files
(`producer_config.rs:1916-1953`) become positive-path tests for
the newly supported mechanisms; only the GSSAPI / non-listed-
mechanism rejection stays as a negative path.

## Java parity — `SaslAuthenticatorTest.testInvalidMechanism`

`kafka/clients/src/test/java/org/apache/kafka/common/security/
authenticator/SaslAuthenticatorTest.java:1290-1310` asserts a
JDK-`Sasl.createSaslClient(...) == null` path: setting
`SASL_MECHANISM = "INVALID"` causes the JVM SASL factory lookup
to fail at producer-construction time with
`"Failed to create SaslClient with mechanism INVALID"`. The Rust
equivalent is *not* a JDK-provider lookup — it's the
`reject_milestone_1_unsupported_sasl_mechanism` validator. **Same
contract** (mechanism-outside-supported-set fails at config
time), **different implementation mechanism** (validator vs SPI
lookup), **different error string** (Milestone-1-specific vs
JDK-specific).

This is the canonical example of "Java parity ≠ string-identical;
parity is contract-identical." Documented for future SASL-related
parity decisions where the JDK provider abstraction has no Rust
equivalent.
