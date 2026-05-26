---
name: phase9c-round1-patterns
description: Patterns learned during Phase 9c Round 1 fixup pass — doc-rot recovery, security-verifier testing, comment honesty over Java-parity claims
metadata:
  type: feedback
---

Phase 9c Round 1 surfaced four cross-cutting patterns worth remembering for future security/TLS work and code review.

**Doc rot in security code is high-impact.** S1 had wrong rustdoc in 4 source-tree locations + NOTES.md claiming `rustls::ServerName::try_from("127.0.0.1")` returns `Err` and SSL builders reject raw IPs. The actual code was correct; the docs were just wrong. The integration test in the same phase relied on the correct behaviour. **Lesson:** when writing comments about library behaviour (especially security-sensitive: rustls, JCE, OpenSSL), verify the behaviour empirically before writing the comment. Don't trust an earlier draft's claim — re-check.

**Why:** Future maintainers debugging an IP-bootstrap SSL handshake would read the wrong rustdoc and waste hours.

**How to apply:** For TLS/SSL/SASL code, the rustdoc on every public function and every non-trivial block comment must match the runtime behaviour. When refactoring, re-verify the comment, don't just preserve it.

---

**Security verifiers need direct unit-test exposure.** `NoHostnameVerifier::verify_server_cert` (S2) was only tested indirectly via `build_client_config_disables_hostname_verification_for_empty_string` which just asserted `Ok(_)` on builder construction — the verifier itself was never invoked. The verifier's correctness rested on two unstated rustls assumptions about chain validation ordering and the closed set of name-mismatch error variants. A rustls version bump could silently start accepting unverified certs.

**Why:** rustls doesn't guarantee `verify_server_cert`'s internal ordering as part of its public contract. Future versions can reorder checks, add new error variants, or change the failure shape. If the only test is "does the builder build", regressions are invisible.

**How to apply:** Any `ServerCertVerifier` impl (or `ClientCertVerifier`, `HandshakeSignatureValid`, etc.) needs at least two direct unit tests: one that pins the "we reject this" case (untrusted CA, expired cert, etc.) and one that pins the "we accept despite this" case (the property being disabled). Build cert chains via `rcgen` — it's already a dev-dep. The verifier struct itself can stay `pub(crate)` if the test mod is its sibling.

---

**"Java parity" is not always the right framing.** S3 had an inline comment claiming "Java parity: `principal()` returns `User:<configured-username>`". Actually, Java's `principal()` for PLAIN throws NPE because `clientPrincipalName = null` and `requireNonNull(name)` rejects it. The Rust impl returns a sensible value instead — but that's a *documented deviation*, not parity.

**Why:** Calling a deviation "parity" tricks future maintainers into thinking the Java impl is exactly the same. They'll trust the Rust translation as authoritative when it's actually an improvement. If a Java behaviour change ever ports over (e.g. Java fixes the NPE), the "parity" comment ages badly.

**How to apply:** When the Rust translation differs from Java behaviour — for any reason (Rust idiom, bug fix, milestone narrowing) — call it deviation explicitly. Cite the Java line numbers. Explain *why* the deviation is acceptable. Don't paper over with "parity" wording.

---

**Choose substring vs `assert_eq!` based on what's load-bearing.** S4 raised the choice between tightening the SSL truststore-missing test to `assert_eq!(err.message(), "<exact>")` vs leaving as substring. Critic explicitly marked both options acceptable because the symmetric SASL test was already substring. Chose the lighter option (substring + rustdoc paragraph documenting the policy + full message inline).

**Why:** Substring assertions are resilient to harmless suffix additions (e.g. remediation hints, version-specific notes). `assert_eq!` is more rigorous but locks the exact wording. The right choice depends on which part of the message is the *behavioural contract*. If it's the key name (load-bearing diagnostic), substring is sufficient. If downstream consumers parse the message programmatically, `assert_eq!`.

**How to apply:** When writing an error-message test, document inline (not just in the commit) why substring vs `assert_eq!` was chosen. Pin the full message in the rustdoc so future refactors have a baseline to compare against. Don't just write `assert!(msg.contains("X"))` without documenting the policy — a future reviewer will ask the same question.

---

**Smaller-than-S2 lesson on `[[wikilink]]` test mod visibility.** The `NoHostnameVerifier` struct is private (`struct NoHostnameVerifier`), but the `tests` module is its in-file sibling. Sibling private items are visible inside `mod tests` without any pub(crate) annotation needed. This avoided having to widen the API surface to expose the struct just for testing. Useful for security-sensitive internals where you want the test reach but not the public reach.

Related: [[phase9c_ssl_plumbing]] for the broader Phase 9c context.
