---
name: Phase-5b Round-2 verified patterns — Tokio EOF fix shape, trait migration safety
description: Verified-good fix shapes from Phase 5b-1 Round 2 — Tokio EOF→UnexpectedEof, inherent→trait method migration, OnceLock singleton
type: project
---

Round-2 acceptance of the Phase 5b-1 fixup (commit `2c9050b`)
confirmed several fix shapes as **verified-good** for reuse in 5b-2
(SslTransportLayer), 5b-3 (KafkaChannel), and 5c (Selector).

**Why:** these patterns will recur — record them so I don't re-litigate
them on every transport variant.

**How to apply:**

1. **Tokio EOF fix shape (verified-good).** The exhaustive
   three-arm match is the right structure:
   ```rust
   match stream.try_read(dst) {
       Ok(0) => Err(io::Error::new(io::ErrorKind::UnexpectedEof, "...")),
       Ok(n) => Ok(n),
       Err(e) if e.kind() == io::ErrorKind::WouldBlock => Ok(0),
       Err(e) => Err(e),
   }
   ```
   The `Ok(0)` arm must come first (before `Ok(n)`) so it pattern-matches
   exactly. Test coverage requires BOTH paths exercised:
   - Quiet-socket WouldBlock test (read returns `Ok(0)`)
   - Peer-close EOF test (drop server, await `readable()` for FIN, read
     returns `Err(UnexpectedEof)`)
   Single-path coverage is insufficient — the second test is what
   distinguishes the bug from the fix. The `readable()` await is the
   correct FIN-arrival probe; do not use a sleep-based timing hack.

2. **Document at both layers (verified-good).** When a semantic spans
   transport (`PlaintextTransportLayer::read`) and consumer
   (`NetworkReceive::read_from`), both layers' rustdoc/comments must
   reflect the contract. Future readers tracing an EOF will start at
   one or the other; if only one has the doc, the other becomes a
   confusion site. Demand symmetric documentation when accepting a
   fix that touches a layered semantic.

3. **`OnceLock<T>` + `.clone()` for owned-by-value singletons
   (verified-good).** When the public API contract demands ownership
   (Java returns a fresh-feeling `KafkaPrincipal`), the Rust
   replacement must also return owned. The shape is:
   ```rust
   pub fn anonymous() -> Self {
       static ANONYMOUS: OnceLock<KafkaPrincipal> = OnceLock::new();
       ANONYMOUS.get_or_init(|| KafkaPrincipal::new(...)).clone()
   }
   ```
   This is one heap allocation for the singleton + a `Clone` per call
   (which for `KafkaPrincipal { String, String, bool }` is two
   `String::clone`s of short literals — much cheaper than two
   `String::from(&'static str)`). Acceptable. Returning `&'static T`
   is even better but requires the API contract to allow borrowing —
   for `peer_principal()` it does not, since the SSL variant returns
   a per-session principal. Flag if the actor uses `&'static T` for
   `anonymous()` only without addressing the SSL caller.

4. **Inherent → trait method migration is API-safe** when:
   - The signature is unchanged (forwarding body, not behaviour change).
   - Existing call sites call through the concrete type (the trait
     method takes precedence and resolves identically).
   - There are no external callers that rely on the inherent method
     specifically (verify with `grep` outside the impl).
   Move-only refactor — accept without re-running full test suite if
   the type signatures match. Mention in the Round-2 verification
   that the migration is observable-behaviour-equivalent.

5. **Deferral integrity check.** When a Round-1 comment is deferred,
   the Round-2 verifier must confirm:
   - Deferral rationale is captured in `COMMENTS.DONE.0.md`.
   - **No new caller** was introduced in the fixup that observes the
     deferred semantic. Run `git diff base fixup -- src/ | grep '+.*<deferred-method>'`.
   If a new caller appears, the deferral is invalid and the comment
   must reopen as Blocking.

6. **Scope-drift audit (Round 2 mechanic).** Run `git diff --stat`
   between the base commit and the fixup. The touched files must map
   1:1 onto the comments they address. Files outside that mapping are
   scope-drift and warrant separate review. For the `2c9050b` fixup,
   the 5 touched files mapped exactly to the 5 comments — clean.

7. **Test-name discipline.** Regression tests for translation bugs
   should name the *Java contract being mirrored*, not the Rust
   mechanic. `read_returns_unexpected_eof_on_peer_close` is good
   (states the contract). `tokio_try_read_returns_ok_zero_on_peer_close`
   would be bad (couples the test to the implementation, which makes
   it brittle if the implementation changes). The actor named the
   tests well in Round 2 — record the pattern.
