---
name: review-m15-password-redaction
description: M15 Password/secret-redaction review (Critic 85): P1 CLOSED r2, P2 CLOSED r3, r4 (master merge) found 1 blocker — Password pub in an unsupported package fails check-public-audience (Public list alone is not enough); r5 CLOSED clean (pub(crate) mod fix); raw-map Debug = key set only; {:?} log sinks; Rust-only holder must mask like the Java class it feeds; +/- parity diff for merges; compiler-mutant proof for "no public signature names X"
metadata:
  type: project
---

Critic 85 reviewed M15 (Password type + Debug redaction, 2 commits). Result: 1 in-scope
Behavior Mismatch, plus 12 out-of-scope Phase-2 candidates. Round 2 (fixup `868ea49d`,
key-set rendering, predicates deleted) closed with 0 issues; the plan was left describing the
superseded by-key design, which I flagged for the Manager's handoff rather than as an Actor issue. The rest of the phase was
sound: Password matches Java, the predicates are exact, delegating Debug impls are correct,
and lint/format pass.

**Lesson 1: redacting a raw user map by key leaks unless the key set is Java's full hidden set.**
- `ProducerConfig.originals` stores every user key (`props.clone()`). Its Debug hid only the
  seven *translated* PASSWORD keys.
- Java never prints an undefined key's value. `AbstractConfig.values` = `definition.parse`
  keeps only defined keys; `logAll()` prints only those; `logUnused()` prints names only;
  `ConfigUtils.configMapToRedactedString` redacts unknown-or-sensitive keys.
- So values still leaked for untranslated PASSWORD keys (the two OAuth keys at
  `SaslConfigs.java:392` and `:402`) and for unknown keys (e.g. a serializer's
  `basic.auth.user.info`).
- **Why:** a plan premise ("secrets are exactly the PASSWORD keys") silently narrowed to
  "the translated ones".
- **How to apply:** for any hand-written Debug over a passthrough map, ask what Java
  prints for (a) untranslated defined keys and (b) undefined keys.

**Lesson 2: a plan's "latent, nothing prints it" claim must be checked against every `{:?}` log sink.**
- `network_client.rs` `cancel_in_flight_requests` logs `{:?}` of `Option<ConcreteRequest>` at
  DEBUG level. This fires on disconnect, request timeout and close_connection. Java logs the
  same value via its redacting `toString()` (`NetworkClient.java:405-409`).
- Every request variant with a derived Debug is therefore *live*. Examples:
  AlterUserScramCredentials (salt and salted password) and IncrementalAlterConfigs (values).
- The send log uses `{}`, which is safe.
- **How to apply:** grep multi-line log macros for `{:?}` of request/response/builder values,
  then trace them to derived Debug impls.

**Lesson 3, technique: executed proofs without touching the repo.**
- Create a scratch crate in the scratchpad with a path dependency on the repo, a copy of the
  repo's Cargo.lock, and its own `CARGO_TARGET_DIR`.
- Do not enable the `ffi` feature: build.rs would then write a header into the repo's
  `target/include`.
- The first build takes about 5 minutes; reruns take seconds.
- For a mutation check on a fixup, `git archive <sha> | tar -x -C <scratch>/copy` and build there with
  its own `CARGO_TARGET_DIR`. The lib test build takes a few minutes and about 3.7 GB; delete the copy after.
- zsh trap: `git show $r:src/...` is misparsed, because `:s` is a parameter modifier. Write `"${r}:src/..."`.
- Do not quote the identifier grep pattern literally in a COMMENTS file: the "must be 0" grep then matches itself.

**Lesson 4: count assertions.** For a `[hidden]` count, compare the fields that are *reachable* with
the fields the input *can set*. There were eight `Option<Password>` fields but only seven config keys
(`SaslConfig::password` is programmatic-only), so the count was right and "once per field" was loose.

**Round 3 (Phase 2, `35d399fa`, 12 items) closed clean: 0 issues.**
- 13 mutants: 12 compiled and all were caught. 11 were caught by a test, and 1 by the
  lib-target lint only, which is by design.

**Lesson 5: an asymmetry that predates the milestone is a note, not the phase's defect.**
- The SCRAM and IncrementalAlterConfigs request `Display` renders counts only, while Java
  renders `maskData(data)`. It came from `3e84b9bd` (PR #127), before M15.
- The approved plan row said "delegate Debug to the existing Display". The Display hides
  more than Java, so it leaks nothing.
- **Why:** a Behavior Mismatch filed against the phase would ask the Actor to go beyond the
  approved scope.
- **How to apply:** before you call a deviation the phase's defect, check where it came from
  (`git log -S`) and what the plan row says. Report it as "pre-existing, fidelity-only", and
  add a one-line follow-up and the tests the change would touch.

**Lesson 6: technique for pinning a log argument when the logger cannot be captured.**
- The logger is process-wide, and `env_logger` is installed by the ffi tests.
- The pattern: a private helper that returns `&dyn Display`, so `{:?}` on its result is
  E0277, and that carries `#[deny(dead_code)]`.
- To verify it, mutate the log line away from the helper. The unit test still passes, but
  `cargo check --lib` fails. `cargo xtask lint` runs clippy with `--all-targets`, which
  includes the lib target.

**Lesson 7: mutants against a hand-written Debug.**
- An exhaustive `let Self { .. } = self;` under `#![deny(warnings)]` rejects a mutant that
  renders `&self.field` next to an unused binding: the unused binding is an error.
- Mutate the binding's own rendering instead, for example drop its `.map(config_keys)`.

**Lesson 8: completeness sweep for Java masking.**
- Grep `common/requests/*.java` for
  `maskData|REDACTED|setHmac\(new byte|setSalt|setAuthBytes\(new byte`. At 4.3.1 this finds
  9 classes. `AlterConfigsRequest` has no Rust translation.
- Checking each of the 119 classes that override `toString` was not needed: the others print
  `data.toString()`.
- The plan's §11.2 rows 4–5 misstated Java: builder `toString()` is `maskData(data)`, not
  "type only". Check the Java yourself, not the plan's paraphrase of it.

**Round 4 (master merge `091f6f9d` + fixup + docs): 1 blocker, everything else clean.**

**Lesson 9: CLAUDE.md §2's three public-visibility conditions are conjunctive; the Public list answers only one.**
- `org.apache.kafka.common.config.types.Password` is on `interface-audience-public-4.4.txt` (line 278). But its
  `package-info.java` at 4.4.0-rc3 says "This package is not a supported Kafka API", so `pub` fails
  `check-public-audience`. Master runs green; the merge kept the branch's pre-rule `pub mod types;`.
- The brief asserted "stays `pub` legitimately: line 278". A brief's premise is a claim to verify, not a fact.
- **How to apply:** for every new or widened `pub` item, check all three: no `internal` segment, no package-info
  disclaimer at `AUDIENCE_REF`, and on the Public list. 14 packages carry the disclaimer at 4.4.0-rc3, including
  `common.config.types`, `common.requests`, `common.protocol`, `common.network`, `common.security.ssl` and
  `common.security.authenticator`.
- Execute the rule rather than reason about it: [[review-lint-java-rules-repro]].
- Fix side effect: privatising the module makes public docs' `[`Password`]` links private, and
  `#![deny(warnings)]` turns that into a `cargo doc` failure. Include the doc-link edits in the fix.

**Lesson 10, false negative (round 3): an element type's Java-faithful Debug does not make its Rust-only holder safe.**
- I cleared `DescribeDelegationTokenResponseOptions`' derived Debug, because `DelegationToken`'s Debug mirrors Java and
  hides only the HMAC. The PR review flagged it. The response those options build masks the token id too
  (`DescribeDelegationTokenResponse.java:131-140`).
- **How to apply:** compare a Rust-only holder with the `toString()` of the Java class whose data it carries, not with
  its element's `toString()`.

**Lesson 11, merge-review technique: per-file `+`/`-` parity.**
- Extract per file, in order, the `+`/`-` lines of the pre-merge milestone delta (`git diff <base> <premerge>`) and
  of the post-merge delta against master (`git diff <master> <merge>`). Use `-U0`, `grep -E '^[+-]'`, and drop the
  `+++`/`---` headers. Then diff the two lists.
- Every residual line must be explained by a master change (renames, `pub(crate)`, aliases). A missing `+` line is a
  dropped redaction or test.
- `cargo xtask lint` stops at lint-custom without `kafka/`, so run doc-hygiene and the three clippy passes
  separately. Module-path hygiene has no standalone task.

**Round 5 (fixup `3d11391f` = `pub(crate) mod types;` + five doc links to code spans, docs, memory): CLOSED clean, 0 findings.**

**Lesson 12: prove "no public signature names X" with a compiler mutant, not a grep.**
- In a `git archive` copy, add a `pub fn` returning `Option<&X>` to a public type and run `cargo check --lib`. Under
  `#![warn(unnameable_types)]` + `#![deny(warnings)]` it fails: "struct `X` is reachable but cannot be named".
- Subtlety: for a `pub` type inside a `pub(crate)` module, `private_interfaces` does NOT fire (it compares nominal
  visibility, which is `pub`); only `unnameable_types` catches the leak. Check the crate enables it before relying on
  the compiler.

**Lesson 13: decompose a lint-count delta by deletion.** Delete the new module (and its `mod` line) in a scratch copy and
re-run `lint-custom`; if the count returns to master's, the delta is fully attributed. `check-java-name` counts
crate-private items too, so a +N there after privatising is expected; `check-no-public-field` and
`check-no-deprecated-translation` count public items only, so they drop back to master's.

**Lesson 14: evidence hygiene.**
- A cached cargo "Finished" under the same `-D warnings` flags is valid (failures record no fingerprint), but a fresh
  archive build is cheap (doc about 40 s, `check --lib` about 20 s) and independent — do it when the claim carries the
  verdict.
- In an archive copy, an `--all-features` build is safe: build.rs writes the C header to `CARGO_MANIFEST_DIR/target/include`,
  i.e. into the copy. Lesson 3's "no `ffi`" applies only to a scratch crate path-depending on the repo.
- "Commit X committed my files unchanged": clean `git status` + mtimes predating the fixup and the commit + the commit
  stat adding exactly those paths.
- The CLAUDE.md in the system context can be stale: §2's three visibility conditions (on-disk `CLAUDE.md:29-31`) were
  missing from it. Cite rules from the on-disk file.

- Related: [[review-expectations]], [[review-fix-commit-rereview]], [[review-lint-java-rules-repro]].
