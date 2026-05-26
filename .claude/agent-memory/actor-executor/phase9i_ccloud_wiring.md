---
name: phase9i-ccloud-wiring
description: Phase 9i added rustls-native-certs system-trust-store fallback for SSL/SASL_SSL when ssl.truststore.location is unset, plus a skip-gated CCloud smoke test
metadata:
  type: project
---

Phase 9i closed the NOTES.md:54 CCloud smoke test deferral as post-Milestone-1
additive work. Branch strategy: stayed on `fresh-impl`.

**Trust-store crate decision: `rustls-native-certs`.** Evaluated against
`webpki-roots`. Chose native-certs because it reads the OS keychain dynamically
(macOS Keychain Access, Linux `/etc/ssl/certs`, Windows cert store) — closest
analogue of Java's `TrustManagerFactory.init(null)` → `$JAVA_HOME/lib/security/cacerts`.
webpki-roots bundles Mozilla's static list at crate-build time. Both popular and
CCloud-tested; the Java-parity argument tipped it.

**Java parity precedent**:
`kafka/clients/src/main/java/org/apache/kafka/common/security/ssl/DefaultSslEngineFactory.java:270-275`
calls `tmf.init(null)` when `createTruststore` (line 307-328) returned null
(no path/certs configured). That `tmf.init(null)` triggers the JVM-default
trust store. Our `load_native_certs_into_root_store` mirrors it.

**Relax-truststore-required pattern**: lift the `.ok_or_else(...)` upstream
of `truststore_location` so it becomes `Option<&str>`, then match on
`Some(path)` vs `None` and branch to the fallback in the None arm. The
truststore_type validation stays out of the match (applies to both arms).

**Test pattern for skip-gated cloud tests**: use a plain `#[tokio::test]`
in the `tests/integration/` tree (default `integration-tests` feature). Skip
with `println!` + early `return` when the auth env var is unset — NO custom
feature flag, NO `#[ignore]`. Mirrors how other ecosystem clients keep
cloud-dependent tests opt-in without a separate test runner invocation.

**Cross-platform gotchas hit**: none on macOS Darwin 24.6.0.
`rustls-native-certs` 0.8 + `security-framework` 3.7 compiled cleanly.
Linux CI would pull `openssl-probe` / direct `/etc/ssl/certs` reading
instead — also documented as working by upstream. No special-casing
needed in our code.

**Tests added/changed**:
- `src/common/security/ssl/mod.rs`: new
  `build_client_config_falls_back_to_system_trust_store_when_location_unset`
  (replaces old reject test), new `load_native_certs_populates_root_store`
  direct unit test.
- `src/producer/kafka_producer.rs`: replaced
  `public_new_rejects_ssl_without_truststore_location` with
  `public_new_accepts_ssl_without_truststore_location_via_system_trust_store`.
- `tests/integration/ccloud_smoke_test.rs`: new file; skip path runs
  in 0.00 s; un-skip path unverified live (no CCloud creds at time of write).

**Lib test count**: 1343 → 1344 (+1 net).

**Live verification of un-skip path**: NOT done. Future operator with
CCloud access should run the documented `SASL_USERNAME=… cargo test`
invocation and report.

**Deferred to Milestone-2**: `webpki-roots` direct-dep cleanup (no longer
used in src/tests, only transitive via `rustls-native-certs` now), live
CCloud verification of un-skip path.

Related: [[phase9h-flakiness-gate]] [[phase9f-auth-failure-integration]]
[[phase9e-sasl-ssl-integration]]
