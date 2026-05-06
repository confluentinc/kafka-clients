# Translation Plan: PR #66

## AK Commit

**Hash:** `cdc40190fc092fdedc8e30b69123976c4a96e30d`
**Title:** MINOR: Skip testDsaKeyPair when DSA algorithm is not supported (#20967)
**Branch:** trunk
**Author:** Jian <fujian1115@gmail.com>
**Date:** 2025-11-25

## Summary of AK Change

This commit is a pure test-infrastructure fix in
`clients/src/test/java/org/apache/kafka/common/network/SslTransportLayerTest.java`.

It addresses a flaky test failure where `testDsaKeyPair` failed on JVM environments
that do not support the DSA key algorithm (e.g. certain JVM builds/distributions where
DSA cipher suites are absent from TLSv1.2 or the `KeyPairGenerator` for DSA is
unavailable).

The fix adds:

1. A private `isDsaSupported()` helper method that:
   - Checks whether `java.security.KeyPairGenerator.getInstance("DSA")` succeeds.
   - Checks whether any `_DSS_`-named cipher suites exist in the JVM's default TLS 1.2
     `SSLContext`, since DSA-key certificates require DHE_DSS or DH_DSS cipher suites to
     complete a TLS handshake.

2. An `assumeTrue(isDsaSupported(), ...)` guard at the top of `testDsaKeyPair` so the test
   is skipped (rather than failing) when DSA is unavailable.

No production code is modified; no logic changes are made to the SSL transport layer
itself. The only change is the test guard.

## Files Changed (AK)

| File | Change |
|------|--------|
| `clients/src/test/java/org/apache/kafka/common/network/SslTransportLayerTest.java` | Added `isDsaSupported()` helper + `assumeTrue` guard in `testDsaKeyPair` |

## Rust Translation Analysis

### Why this commit is a no-op for the Rust client

The Rust client uses [`rustls`](https://github.com/rustls/rustls) as its TLS library.
`rustls` explicitly does **not** support the DSA key algorithm; DSA was removed from
`rustls` by design because:

- DSA was deprecated in TLS 1.3 (it is not defined as a valid signature algorithm).
- DSA is considered cryptographically weak by modern standards.
- `rustls` has a strict "no unsafe legacy algorithms" policy.

As a consequence:

- There is no `testDsaKeyPair` equivalent in the Rust client's SSL test suite.
- The Rust `SslTransportLayer` (`src/common/network/ssl_transport_layer.rs`) and
  `SslChannelBuilder` (`src/common/network/ssl_channel_builder.rs`) only support RSA and
  ECDSA certificates via `rustls`.
- The test infrastructure (`tests/integration/ssl_sasl_test.rs`) never attempts to
  generate or use DSA key pairs.

There is nothing to translate. The Java change guards against a JVM capability gap that
simply does not exist in the Rust/rustls world — rustls categorically rejects DSA,
so the question of "is DSA supported?" is always answered "no" and no DSA path
is reachable.

### Conclusion

**This commit requires no code changes to the Rust client.**

The DSA key-pair test and the surrounding guard logic are JVM-specific test
infrastructure with no Rust equivalent. No new tests, no production-code changes,
and no documentation updates are needed.

## Implementation Plan

No implementation required. This PR is a no-op translation.

Steps:
1. Verify no DSA-related code exists in the Rust client (confirmed above).
2. Verify `rustls` dependency in `Cargo.toml` continues to exclude DSA (it does by
   default — there is no DSA feature flag).
3. No commits beyond this design document commit are needed.
