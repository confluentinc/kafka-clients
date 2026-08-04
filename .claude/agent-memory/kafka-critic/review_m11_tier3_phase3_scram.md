---
name: review-m11-tier3-phase3-scram
description: M11 Tier3 Phase3 SCRAM credentials review — CLEAN; crypto/dep verification heuristics
metadata:
  type: project
---

M11 Tier 3 Phase 3 (SCRAM credentials: describeUserScramCredentials / alterUserScramCredentials) reviewed CLEAN — no findings written to COMMENTS.1.md.

**Why:** final in-scope Admin phase; risk concentrated in PBKDF2 crypto + a new crypto dependency (aws-lc-rs).

**How to apply (reusable verification heuristics for crypto/dep phases):**
- **PBKDF2 == RFC 5802 Hi** when dkLen = digest length (SHA-256→32, SHA-512→64): `ScramFormatter::hi` correctly maps to `aws_lc_rs::pbkdf2::derive` with one output block. Independently recompute vectors with `python3 -c "import hashlib,binascii; print(binascii.hexlify(hashlib.pbkdf2_hmac('sha256',b'passwd',b'salt',1,32)))"` — both hardcoded test vectors matched exactly. Don't trust "test passes"; recompute.
- **iterations.max(1) clamp** is faithful to Java's `for i=2; i<=iterations` loop (≤1 yields U1 = PBKDF2 c=1). Correct.
- **Zero-new-crate dep check**: compare `cargo tree --edges normal --prefix none | sed 's/ (.*//' | sort -u` at HEAD vs parent (via `git worktree add`). Phase 3 graph was byte-identical (80 crates, empty diff) — `untrusted v0.9.0` already present via rustls. The `default-features=false, features=["aws-lc-sys","alloc"]` deviation from literal `aws-lc-rs="1"` is SOUND: cargo feature unification is additive so rustls's own aws-lc-rs features (crypto provider at ssl_factory.rs) still apply. Agreed with Actor's call.
- **Integration test against real broker is the crypto acceptance test**: upsert→describe→delete round-trip; broker rejects malformed salted password. Ran green (Docker available). SASL-auth step deferral legitimate (admin client has no SASL client on this branch — same gap as Phase 4 delegation tokens).
- Java `MockAdminClient.describeUserScramCredentials`/`alterUserScramCredentials` genuinely throw `UnsupportedOperationException("Not implemented yet")` (~1252/1258) → Rust exceptional-future is faithful (§9), not a scope stub.
- Java `UserScramCredentialAlteration` is `abstract` (not `sealed`) with exactly 2 subclasses; closed Rust enum {Upsertion,Deletion} is faithful — client only ever casts to those two.
- Java tests `testAlterUserScramCredentials*` use `assertThrows(Exception.class,...)` (no type/message assert) → Rust `is_err()` is faithful, NOT an under-assertion.
