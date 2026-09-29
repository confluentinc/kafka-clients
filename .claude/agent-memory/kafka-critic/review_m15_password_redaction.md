---
name: review-m15-password-redaction
description: M15 Password/secret-redaction review (Critic 85): Phase 1 CLOSED r2, Phase 2 CLOSED r3 with 0 issues — raw-map Debug renders key set only; check live {:?} log sinks before accepting "latent"; pre-existing over-redacting Display the plan says to delegate to is a deviation note, not a phase defect; &dyn Display + deny(dead_code) pins a log argument; git-archive mutation copies
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

- Related: [[review-expectations]], [[review-fix-commit-rereview]].
