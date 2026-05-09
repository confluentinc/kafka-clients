# Translation Plan: Skip testDsaKeyPair when DSA algorithm is not supported

**AK commit:** `cdc40190fc092fdedc8e30b69123976c4a96e30d`
**AK branch:** trunk
**PR:** #101
**Rust branch:** `kafka-translate/cdc40190fc092fdedc8e30b69123976c4a96e30d`

---

## Summary of the Apache Kafka Commit

This is a **test-only fix**. No production code was changed.

The commit fixes a flaky test `SslTransportLayerTest.testDsaKeyPair` which fails
on JVMs that do not fully support the DSA algorithm (even if the `KeyPairGenerator`
is available, some JVMs lack DSA-compatible cipher suites for TLSv1.2).

**Root cause:** The test assumes DSA key pairs and DSA-compatible cipher suites are
always available. On certain JVM configurations (e.g., newer Java distributions),
DSA cipher suites (`_DSS_` pattern) may not be present, causing the SSL handshake
to fail with "Channel was not ready after 30 seconds."

**Fix applied in AK:**
- Added an `assumeTrue(isDsaSupported(), ...)` guard to skip the test when DSA is
  not fully supported.
- Added a new `isDsaSupported()` helper method that checks:
  1. Whether the `DSA` `KeyPairGenerator` algorithm is available.
  2. Whether any DSA-compatible cipher suite (containing `_DSS_`) is available in
     the `TLSv1.2` `SSLContext`.

**Changed file:**
```
clients/src/test/java/org/apache/kafka/common/network/SslTransportLayerTest.java
```

---

## Rust Translation Analysis

### Does the test exist in Rust?

No. The Rust codebase has `src/common/network/ssl_transport_layer.rs` (production
SSL transport layer) and an `ssl_sasl_test.rs` integration test, but there is no
equivalent `testDsaKeyPair` test. The Rust SSL layer uses `rustls` or native TLS
libraries, which handle algorithm availability differently from Java's
`KeyPairGenerator`/`SSLContext` APIs.

### Is there production code to translate?

No. The AK commit modifies only a Java test file. No library source files changed.

### What needs to be done?

**Nothing.** This commit is a no-op for the Rust translation because:

1. The `testDsaKeyPair` test does not exist in the Rust test suite.
2. DSA key pairs are a JVM-specific concern. Rust's TLS libraries (`rustls`,
   `native-tls`) do not expose DSA key generation or DSA cipher suite selection
   in the same way — DSA is effectively unsupported/deprecated in modern Rust TLS
   stacks.
3. If/when SSL transport layer tests are expanded in Rust, DSA testing would not
   be applicable since `rustls` does not support DSA at all.

---

## Implementation Plan

### No action required

This commit translates to a **no-op** in the Rust codebase. No files need to be
created or modified.

---

## Files to Create / Modify

| File | Action | Reason |
|------|--------|--------|
| (none) | — | Test-only JVM-specific fix with no Rust equivalent |

---

## Out of Scope

- Translating the full `SslTransportLayerTest` — the Rust SSL tests use a
  different testing approach appropriate for `rustls`/`native-tls`.
- DSA algorithm support — not applicable to Rust TLS libraries.

---

## Definition of Done

- [x] Design document written acknowledging this is a no-op translation.
- [ ] PR merged with no code changes (documentation only).
