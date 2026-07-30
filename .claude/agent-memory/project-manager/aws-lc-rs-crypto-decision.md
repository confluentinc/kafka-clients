---
name: aws-lc-rs-crypto-decision
description: aws-lc-rs is the approved crypto backend for this repo; how to add it without pulling new crates
metadata:
  type: project
---

`aws-lc-rs` is this project's chosen crypto backend. The user approved it
(2026-07-30, CLAUDE.md §1.2) as OPTION A for SCRAM's PBKDF2 need over `ring`
(dev-only here) and RustCrypto (`pbkdf2`/`hmac`/`sha2` — would add a parallel
crypto stack).

**Why:** it is already an in-tree production dependency (pulled by rustls; the
project deliberately installs `rustls::crypto::aws_lc_rs::default_provider()` at
`ssl_factory.rs`), it is FIPS-capable, and it exposes PBKDF2 directly
(`aws_lc_rs::pbkdf2::derive` with `PBKDF2_HMAC_SHA256`/`SHA512`).

**How to apply (for any future crypto work — e.g. a real SASL/SCRAM client):**
- Use `aws-lc-rs`; do NOT introduce `ring` or RustCrypto crates.
- Declare it with `default-features = false, features = ["aws-lc-sys", "alloc"]`,
  NOT the bare `aws-lc-rs = "1"`. The bare form pulls an extra crate
  (`untrusted v0.7.1`) via ring-compat default features. Disabling defaults
  matches rustls's own feature selection, so cargo feature-unification keeps the
  normal-edges dependency graph byte-identical (zero new compiled crates) while
  still giving you PBKDF2. Verify with:
  `cargo tree --edges normal --prefix none | sed 's/ (.*//' | sort -u` diffed
  against the parent commit — must be empty.
- Feature unification is additive, so disabling the direct dep's default features
  does NOT remove features rustls requests; the crypto provider stays intact.

Applied in Milestone 11 Tier 3 Phase 3 (SCRAM); Critic independently confirmed the
zero-new-crate property. Related: [[milestone11-cadence-and-stops]].
