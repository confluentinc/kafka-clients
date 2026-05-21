---
name: phase9c-review-patterns
description: Phase 9c (SSL + SNI plumbing) review patterns — doc-vs-test internal contradictions, security-test gaps for custom rustls verifiers, behavior-change without regression test, IP-vs-DNS-SAN trap
metadata:
  type: feedback
---

## Top recurring traps in Phase 9c

### 1. Doc claims about rustls behavior diverge from actual library behavior (3 locations in same commit)

When translating Java's `ssl.endpoint.identification.algorithm` semantics, the Actor wrote (in 3 different comment blocks): "raw IP literal causes `ServerName::try_from` to fail, returning `Err`, so we pass `None`, and SSL builders reject `None` with IllegalState."

But rustls 0.23.38 `ServerName::try_from("127.0.0.1")` returns `Ok(ServerName::IpAddress(...))`. The test in the same commit correctly observes this (`assert!(matches!(&cap, Some(Some(_))))`) but its own header doc-comment still claims "None". Worse: the integration test relies on the actual `Ok(IpAddress)` behavior (cert has IP-SAN `127.0.0.1`), so the docs are inconsistent with both the code AND with the live integration use case.

**Reviewer check:** when a translation uses a third-party library's `try_from` / `parse` / fallible constructor, verify the doc's claim against (a) the library docs of the actual version in `Cargo.lock`, (b) the test in the same commit that exercises the path. If they disagree, the doc is the easiest part to update — but file the suggestion.

### 2. Custom `ServerCertVerifier` impl has no chain-validation test

When implementing a custom `rustls::ServerCertVerifier` that translates name-mismatch errors into success (Java's `endpoint.identification.algorithm=""` escape hatch), the verifier MUST still propagate chain-of-trust failures.

The Phase 9c.1 commit added 9 tests but ONLY one for the empty-string path — and that test only checked `build_client_config_from_producer_config` succeeds. No test exercised:
- An untrusted CA cert → must reject
- A trusted CA cert with mismatched hostname → must accept (= the actual behavior we want)

Both can be constructed with `rcgen` in the same style as the existing `build_client_config_accepts_inline_client_keystore` test.

**Reviewer check:** for any custom `ServerCertVerifier` / `KeyManager` / `TrustManager` impl that intercepts specific error variants, verify the test pins (a) the rejection paths the variant `match` does NOT catch, (b) the acceptance paths it DOES catch. Otherwise a rustls version bump could silently flip the security posture.

### 3. Behavior change without regression test

The 9c.5 commit changed `SaslClientAuthenticator::principal()` from `KafkaPrincipal::anonymous()` to `KafkaPrincipal::new(USER_TYPE, &self.credentials.username)`. No test exists asserting the new behavior. A future revert would compile, pass the entire suite, and silently regress production log identity.

**Reviewer check:** when reviewing a commit that says "fix X to match Java's behavior," grep for any test asserting the NEW behavior. If absent, file a Suggestion to add one. DoD #3 (error message content asserted) extends here to principal identity and any other behavioral surface that downstream tooling reads.

### 4. "Java parity" claim that's actually a behavioral improvement over Java

The 9c.5 fix to `principal()` claimed Java parity. But Java's `SaslClientAuthenticator.java:200-206` sets `clientPrincipalName = null` for non-GSSAPI mechanisms, and `principal()` at line 487-489 does `new KafkaPrincipal(USER_TYPE, clientPrincipalName)` — which throws NPE (KafkaPrincipal ctor calls `requireNonNull(name)`).

The Rust impl returns `User:<configured-username>` — better than Java's NPE-on-call, but it's NOT parity. The "Java parity" wording in the comment misleads future maintainers debugging an actual Java-vs-Rust comparison.

**Reviewer check:** for any "Java parity" claim, trace the Java code path to the actual returned value (read all assignments of fields used in the return). If Java's actual return is `null`, broken, or throws, the Rust fix is a *deviation*, not parity — flag the wording.

### 5. Test fixture port literals vs hostname comments

Integration test comment claimed "cert SAN is `localhost` which matches the `ssl_bootstrap_servers` hostname." But `ssl_bootstrap_servers()` returns `127.0.0.1:port`, not a hostname. The IP-SAN matched, not the DNS-SAN.

**Reviewer check:** when an integration test comment claims a specific SAN match, verify the source of the bootstrap string (often `format!("127.0.0.1:{port}")`) and the cert's SAN list. A comment that says "hostname matches" when the actual mechanism is IP-SAN is misleading.

### 6. Inconsistent style within the same commit

`ee3ea59` introduced `producer_smoke_ssl_1000_records` AND applied 8c-S1's `Arc::try_unwrap → Arc::into_inner` refactor in `producer_smoke_plaintext_byte_fidelity` — but used the OLD `Arc::try_unwrap` ceremony in the brand-new SSL test. The Actor was applying the followup fix and adding the symmetric test in the same commit but didn't apply the refactor to the new code.

**Reviewer check:** for fixup commits that introduce new code alongside the fix, grep the new code for the OLD pattern that the fix replaced. Inconsistency lands silently.

### 7. Dead-code-style comment with three "considered options"

`NoHostnameVerifier::verify_server_cert` has a 14-line comment that walks through three implementation candidates ("literal `_`", "manually re-implement", "cleanest approach") before describing the actual code. A future reader has to do the same elimination.

**Reviewer check:** comment blocks that read like in-progress dev notes ("Easier path:", "Cleanest approach:", "We could X but actually Y") should describe the implemented approach + the load-bearing rationale, with alternatives moved to NOTES.md if they're worth preserving.

## What's NOT a finding

- **Apache 2.0 license header** — present.
- **No TODO/FIXME** — confirmed.
- **`#![allow(dead_code)]` lifted in 9c.3** — confirmed in `866502a` diff.
- **MockSelectorView updated for new `host: &str` parameter** — confirmed.
- **Selector `connection_hosts` lifecycle complete** — insert at connect, remove at build success, build failure, connect failure, close_connection, close. All paths verified.
- **`tempfile::NamedTempFile` outlives ProducerConfig lookup** — `_truststore_file` binding kept alive in tests via `let _ = ...`.
- **PEM-only narrowing** — documented in module rustdoc as Milestone-1 narrowing.
- **`SaslChannelBuilder` `build_channel_with_server_name` dispatch** — SaslPlaintext → `build_channel`, SaslSsl → require `Some(server_name)`. Defensive `IllegalState` for non-SASL protocols (unreachable but exhaustive match).
- **`NetworkClient::initiate_connect` threads `node.host()` through `Selectable::connect`** — confirmed at `network_client.rs:855,883`.
- **Cargo.toml dev-deps** — `rcgen` and `tempfile` correctly under `[dev-dependencies]`.
- **Test count delta** — 1339 vs 1327 = +12 matches close-stanza claim (+9 SSL + 1 SNI propagation + 3 SSL acceptance - 1 SSL rejection).
