---
name: m11-tier3-phase3-scram
description: M11 Tier 3 Phase 3 SCRAM credentials — aws-lc-rs PBKDF2, hi() vectors, abstract-base-as-enum, SASL integration gap
metadata:
  type: project
---

Milestone 11 Tier 3 Phase 3 (SCRAM user credentials, KIP-554) — the final
in-scope Admin phase. Landed in 3 commits on
`dev/admin-client-implementation` (crypto+wire+RPCs / KafkaAdminClientTest /
integration). See [[m11-tier3-phase4-delegation-tokens]] for the SASL gap this
shares.

**Crypto dependency (aws-lc-rs):**
- Added `aws-lc-rs = { version = "1", default-features = false, features = ["aws-lc-sys", "alloc"] }`.
  Plain `"1"` (default features) pulls an extra `untrusted` crate via
  `ring-io`/`ring-sig-verify`; disabling defaults matches rustls's own feature
  selection so the normal-edges `cargo tree` is byte-identical before/after
  (zero new compiled crates — the whole rationale for choosing aws-lc-rs).
  Verify with `cargo tree --edges normal --prefix none | sed 's/ (.*//' | sort -u`.

**ScramFormatter::hi() — narrow translation:**
- `hi()` with output length pinned to one digest block == PBKDF2's first block
  `T_1`, so `aws_lc_rs::pbkdf2::derive(algo, NonZeroU32, salt, secret, out)`
  computes it directly. SHA-256 -> `PBKDF2_HMAC_SHA256`, out 32; SHA-512 ->
  `PBKDF2_HMAC_SHA512`, out 64.
- Java's loop starts at `i=2`, so `iterations<=1` yields just `U1`; clamp with
  `iterations.max(1)` (also keeps `NonZeroU32::new(..).unwrap()` panic-free).
- Known vectors: SHA-256 `hi("passwd","salt",1)` = RFC 7914 §11 PBKDF2-HMAC-SHA-256
  first block `55ac046e...c20dacbc`. SHA-512 cross-checked against Python
  `hashlib.pbkdf2_hmac` (RFC 7914 has no SHA-512 vector).
- Trim the internal `ScramMechanism` to ONLY variants + `for_mechanism_name`
  (per the narrow directive). The full Java enum (hash_algorithm/mac_algorithm/
  type/min/max_iterations/is_scram) trips `-D warnings` dead_code because
  ScramFormatter matches the enum directly for algo selection.

**Abstract base modeled as a closed enum:** `UserScramCredentialAlteration` is
an enum `{ Upsertion(..), Deletion(..) }` with `From` impls; Java's `instanceof`
dispatch in `alterUserScramCredentials` becomes a `match`. The `.user()`
accessor delegates to the active variant. Faithful because the Java hierarchy
is sealed (both subclasses in-package, no public subclassing contract).

**RPCs:** describe = plain `Call` + `NodeProvider::LeastLoaded`; alter = plain
`Call` + `NodeProvider::Controller` with client-side validation done in the RPC
method BEFORE the call (empty user -> UnacceptableCredential; UNKNOWN mech ->
UnsupportedSaslMechanism; empty password -> UnacceptableCredential), pre-building
the wire upsertions (PBKDF2 done once) / deletions, then completing illegal
users + per-result + completeUnrealizedFutures in handle_response. NOT_CONTROLLER
retry via the shared `handle_not_controller_error(mm, error_counts)` (clears
controller + request_update).

**MockAdminClient:** BOTH methods return `KafkaError::unsupported_version("Not
implemented yet")` — faithful to Java's mock (MockAdminClient.java:1251-1259
throws UnsupportedOperationException). So the unit tests run against the
network-mocked `KafkaAdminClient` (env()/prepare_response/pump), not the mock.

**Integration (`tests/integration/admin_scram_test.rs`):** upsert SCRAM-SHA-256
(it=8192) -> describe -> assert mechanism+iterations round-trip (NEVER the
salted password) -> delete -> `users()` no longer lists the user. Self-scoped
to `ctx.group_id(...)` unique username. The optional SASL-authenticate step is
deferred: admin client has no SASL client support (AdminClientConfig has no
security.protocol/sasl keys; from_config hardcodes PLAINTEXT) — same gap as
Phase 4. `cargo test --lib` 3016 -> 3029.

**main.rs staging gotcha:** `tests/integration/main.rs` has an unstaged local
`mod admin_smoke_test_manual;` line (gitignored dev file). `git add -p` can't
split adjacent added lines; stage only your own `mod` line with an explicit
`git apply --cached` patch whose context matches HEAD (which has neither line),
then confirm `git diff --cached tests/integration/main.rs`.
