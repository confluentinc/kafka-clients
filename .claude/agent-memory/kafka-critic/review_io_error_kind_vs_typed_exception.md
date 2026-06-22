---
name: review-io-error-kind-vs-typed-exception
description: io::ErrorKind heuristics cannot substitute for Java's typed exception hierarchy — auth-vs-disconnect classification bug
metadata:
  type: feedback
---

When Java classifies an error by exception **type** (e.g.
`catch (AuthenticationException)` vs `catch (IOException)`), translating that to
a Rust `io::ErrorKind` heuristic is a recurring source of behavior-divergence
bugs. `io::Error::other(...)` collapses unrelated failures into
`ErrorKind::Other`, destroying the distinction the Java code depends on.

**Confirmed instance (2026-06-16, consumer SASL_SSL):** A TLS-handshake
connection-reset (os error 104) was misclassified as a fatal authentication
failure and stopped the whole consumer.
- Origin: `ssl_transport_layer.rs` handshake wraps reset, cert-failure, and TLS
  write-failure all as `io::Error::other("TLS handshake failed: ...")` → all
  become `ErrorKind::Other`.
- Linchpin: `kafka_channel.rs` `prepare()` sets `State::AuthenticationFailed` for
  ANY handshake error. Java `KafkaChannel.prepare()` (L183) only does so for
  `AuthenticationException`; the comment says "Other errors are handled as
  network exceptions in Selector."
- `selector.rs` then uses `e.kind() == Other || InvalidInput` ⇒ "Failed
  authentication" — wrong in BOTH directions: it mis-catches resets AND misses
  real SASL failures, which use `ErrorKind::InvalidData`.
- Downstream: `SaslAuthenticationFailed` is NOT in `errors.rs is_retriable()`
  (NetworkException IS) → coordinator RM stores it as fatal → surfaces out of
  `poll()`.
- Full report: `design/current/consumer-tls-reset-fatal-rootcause.md`.

**Why:** Java's exception type IS the classification; an opaque error kind is a
lossy proxy. Connection-reset = retriable disconnect; genuine TLS/SASL
negotiation failure = fatal AuthenticationException. Conflating them changes the
retriable/fatal contract.

**How to apply (review heuristic):** Whenever a Rust transport/auth path returns
`io::Result` and a caller branches on `ErrorKind` to decide
fatal-vs-retriable, flag it. Check: (1) does the Java original branch on a typed
exception? (2) does any error-producing site `io::Error::other(...)` away the
distinction? (3) are genuine auth failures and transport disconnects guaranteed
to land in different buckets? Real auth failures must be carried as a typed
error across the `io::Result` boundary (downcastable inner error, or a reserved
ErrorKind for auth), not inferred from a kind that transport errors also use.

Related: [[review-m8-phase17]] (security wiring), [[review-ssl-tls-patterns]].
