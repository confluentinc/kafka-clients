# Tier 3 Phase 3 — SCRAM credentials — Review record

**Status: COMPLETE, Critic-CLEAN on first pass (no fix cycle).** Agent N=1.
Date: 2026-07-30. This was the FINAL in-scope Milestone 11 phase — all 46/46
in-scope Admin RPCs are now translated.

## RPCs translated
`describeUserScramCredentials` (plain `Call`, `LeastLoadedNodeProvider`),
`alterUserScramCredentials` (plain `Call`, `ControllerNodeProvider`).

## Commits
- `e50be89` — SCRAM crypto helper, wire, RPCs.
- `dd4f79d` — SCRAM `KafkaAdminClientTest` slices.
- `2d7e7a3` — SCRAM real-broker integration test.

## Crypto dependency decision (CLAUDE.md §1.2 — approved by user 2026-07-30)
**OPTION A: `aws-lc-rs`.** Declared in `Cargo.toml` as
`aws-lc-rs = { version = "1", default-features = false, features = ["aws-lc-sys", "alloc"] }`.
Deviation from the literal `aws-lc-rs = "1"` was deliberate: plain `"1"` (default
features) pulls one extra crate (`untrusted v0.7.1`) via ring-compat features;
disabling defaults matches rustls's own feature selection so the normal-edges
dependency graph is **byte-identical before/after (zero new compiled crates)** —
the whole rationale for choosing aws-lc-rs (already in-tree via rustls, the
project's FIPS crypto backend, `ssl_factory.rs:79`). Critic independently
confirmed the normal-edges graph is identical to parent `f12093a` (80 crates,
empty diff) and that feature unification keeps rustls's crypto-provider setup
intact. This is the ONLY new dependency.

## Crypto correctness — `ScramFormatter::hi()`
RFC 5802 `Hi` = PBKDF2-HMAC first output block when dkLen = digest length.
Implemented via `aws_lc_rs::pbkdf2::derive`: SCRAM-SHA-256 → `PBKDF2_HMAC_SHA256`
out-len 32; SCRAM-SHA-512 → `PBKDF2_HMAC_SHA512` out-len 64; iterations via
`NonZeroU32` (`iterations.max(1)`, faithful to Java's `for i=2..=iterations` loop
yielding just `U1` for iterations ≤ 1). Internal `ScramMechanism` name→algorithm
mapping translated under `src/common/security/scram/internals/`. Only `hi()` and
the mapping were translated — NOT a full SASL/SCRAM client.

**Byte-vector test (the phase's most important test):** the Critic INDEPENDENTLY
recomputed both hardcoded vectors with Python `hashlib.pbkdf2_hmac` and got exact
matches:
- SHA-256 `hi("passwd","salt",1)` = `55ac046e56e3089fec1691c22544b605f94185216dde0465e68b9d57c20dacbc` (RFC 7914 §11 first block).
- SHA-512 `hi("pencil","salt",4096)` = `2cfe3a1c151662b1ea49d13f595674a1c666add70df15d3d02254e9905993878261da7407fd11c2fee4b0a30df5154b1a752f86a13380ddd4bdd9a7c958ec769`.
Together they exercise both the INT(1) big-endian block counter (c=1) and the
iteration loop (c=4096).

## MockAdminClient (finding #9 — mirror Java's unsupported)
Both `describe_user_scram_credentials` and `alter_user_scram_credentials` return
`KafkaError::unsupported_version("Not implemented yet")` (NOT panic), citing
`MockAdminClient.java` ~1251–1259 where Java throws
`UnsupportedOperationException("Not implemented yet")`. Consequently the unit
tests run against the network-mocked `KafkaAdminClient`, not `MockAdminClient`.

## DoD verification (independently confirmed by the Critic)
- `cargo build`: clean.
- `cargo test --lib`: **3029 passed, 0 failed**.
- `cargo xtask format-check`: clean.
- `cargo xtask lint` (clippy -D warnings): clean.
- Integration: `tests/integration/admin_scram_test.rs` — upsert SCRAM-SHA-256
  (it=8192) → describe (asserts mechanism+iterations round-trip; never the salted
  password) → delete → assert gone. Self-scoped to a per-test username (complies
  with the global-state-isolation hazard rule). PASSED against a real 4.2 broker.

## Tests
- 1:1: `testDescribeUserScramCredentials`, `testAlterUserScramCredentialsUnknownMechanism`,
  `testAlterUserScramCredentials` (exercises REAL PBKDF2 for all users),
  `ScramMechanismTest`, `DescribeUserScramCredentialsResultTest`. Byte-level wire
  vectors for both new request/response types. Exact error semantics
  (`UnacceptableCredential`, `UnsupportedSaslMechanism`, deletions-before-upsertions
  ordering, NOT_CONTROLLER-first, RESOURCE_NOT_FOUND) match Java.
- `UserScramCredentialAlteration` (abstract, exactly 2 subclasses) → faithful
  closed enum `{Upsertion, Deletion}`.
- No Java tests skipped. DoD #10 N/A (not a hot path).

## Scoped-out (documented, legitimate)
The plan's optional integration step (b) "SASL-authenticate a client with the
created credential" was NOT run: the admin client has no SASL client support on
this branch (`AdminClientConfig` hardcodes PLAINTEXT — the same gap that scoped
down Phase 4 delegation-token integration). The upsert→describe→delete round-trip
(the broker rejects a malformed salted password) is the correctness signal.
Deferred with an explicit note; not a vacuous test.

## Notes
- `admin_smoke_test_manual` line confirmed NOT committed; `main.rs` gained only
  `mod admin_scram_test;`.
- Full resolved-issues milestone archive is this directory's `COMMENTS.DONE.1.md`.
