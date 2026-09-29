---
name: m15-password-redaction
description: M15 Password/Debug redaction (Actor 85, Phases 1-2) — Vec<u8> Debug teeth, raw maps render as key sets, file-level allow(dead_code) defeats dead-code teeth, log lines can't be captured in lib tests, verify without the kafka/ submodule
metadata:
  type: project
---

Milestone 15 (branch `fix/password-type-redaction`). Phase 1 (2026-09-29): `Password`
type, typed SSL/SASL secret fields, hand-written/delegating `Debug` on ProducerConfig,
DelegationToken + six wrappers, UserScramCredentialUpsertion. Phase 2 (2026-09-30)
closed the adjacent leaks Phase 1 had only reported: the NetworkClient cancel log,
the AlterUserScram/IncrementalAlterConfigs requests, five request builders,
ConfigEntry (+ Config/AlterConfigOp through it), ConfigEntryOptions,
CreateDelegationTokenResponseOptions, MockAdminClient. Out of scope by decision:
generated `*Data` types (Java's generated `toString()` prints bytes too — Java
redacts only at the wrapper, `MessageDataGenerator.generateFieldToString`).

**Redaction-test teeth (the non-obvious part):**
- A derived `Debug` of `Vec<u8>` prints *numbers* (`[115, 101, ...]`), so
  `!rendered.contains("secret")` is toothless for byte secrets. Assert absence of
  `format!("{:?}", bytes)`, and for a Debug that delegates to Display assert
  `format!("{x:?}") == x.to_string()` — that equality is what gives `{:#?}` teeth
  (the inner `write!` gets a fresh spec, so `#` does not propagate).
- `{:#?}` spreads a byte list over indented lines, so no fixed substring matches;
  check the exact field syntax instead (`hmac: [],`).
- Teeth-check by breaking each impl (backup → break → run → restore + `touch`, see
  [[workflow-teeth-check-mtime]]). The lib has `#![deny(warnings)]` (lib.rs), so a
  mutation that leaves an unused `mut` fails to COMPILE and the whole batch runs zero
  tests: mutate the `let mut` line along with the body.
- A mutation of a helper the request's Display shares also reddens that file's
  pre-existing tests — expected, not a teeth-script failure.

**Log lines cannot be captured in lib tests.** The `log` logger is process-wide and
set once; `src/ffi/common.rs` calls `env_logger::try_init()` inside the same
`--features ffi` test binary; raising the global level perturbs concurrent tests.
Fallback (Phase 38 precedent): the log line formats a small pure helper
(`loggable_request`) and the test asserts on the helper for a real in-flight request.
**Trap:** `network_client.rs` (like several legacy files) has a file-level
`#![allow(dead_code)]`, so "the helper would become dead code" gives NO teeth — found
only by running the mutation. Put `#[deny(dead_code)]` on the helper, and prove it
with `cargo check --lib` (the test target uses the helper, so `cargo test --lib`
stays green under the mutation).

**Raw maps render as key sets only.** A raw user map holds EVERY key the user passed,
so a by-key filter covers only the translated subset (the untranslated OAuth
`Type.PASSWORD` keys, `SaslConfigs.java:392`/`:402`, and unknown keys such as
`basic.auth.user.info` leaked). Java never prints a raw `originals` value. Same for
MockAdminClient's `BTreeMap<String, String>` config maps (Java's mock has no
`toString()`): `Debug` renders them as `BTreeSet<&str>`.
**How to apply:** any Debug of a raw user-supplied config map renders keys only; do
not reintroduce a password-key predicate for it (the Phase 1 fixup deleted
`is_password_config` for exactly this reason).

**Rust-only options types** (no Java `toString()`): hand-write `Debug`, destructure
exhaustively, bind secrets to `_`, and render the secret the way the Java type the
options build renders it (`"Redacted"` for a sensitive ConfigEntry value, `"REDACTED"`
+ empty hmac for CreateDelegationTokenResponse) — never its length.

**Verify without the kafka/ submodule:** `make verify` = build (its `build-c` runs
`git submodule update`) + format-check + lint + `test-rust-all-features`
(`cargo test --all-features -- --skip __grpc`) + test-c + test-python +
check-bindings. `test-rust` (`--workspace`, `--features ffi`) is NOT in `verify`;
run it anyway. When submodule update is forbidden, run the Rust arms directly and
read Java via `curl https://raw.githubusercontent.com/apache/kafka/<sha>/clients/src/main/java/...`
(the generator lives under `generator/src/main/java/...` at the same sha).
