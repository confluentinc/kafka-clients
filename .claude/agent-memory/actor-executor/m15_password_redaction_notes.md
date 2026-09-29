---
name: m15-password-redaction
description: M15 Password/Debug redaction (Actor 85) — redaction-test teeth for Vec<u8>, originals rendered as key set only (by-key filter leaks untranslated keys), leak sites classified vs Java, verify without the kafka/ submodule
metadata:
  type: project
---

Milestone 15 (branch `fix/password-type-redaction`, 2026-09-29): `Password` type,
typed SSL/SASL secret fields, hand-written/delegating `Debug` on ProducerConfig,
DelegationToken + six request/response wrappers, UserScramCredentialUpsertion.
The PLAN scope was binding, so adjacent leaks were **reported, not fixed**.

**Redaction-test teeth (the non-obvious part):**
- A derived `Debug` of `Vec<u8>` prints *numbers* (`[115, 101, ...]`), so
  `!rendered.contains("secret")` is toothless for byte secrets. Assert absence of
  `format!("{:?}", bytes)`, and for a Debug that delegates to Display assert
  `format!("{x:?}") == x.to_string()` — that equality is what gives `{:#?}` teeth.
- `{:#?}` spreads a byte list over indented lines, so no fixed substring matches;
  check for the derived field syntax instead (e.g. `!pretty.contains("hmac: [")`).
- Teeth-check by breaking each impl in one batch (backup → break → run → restore +
  `touch`, see [[workflow-teeth-check-mtime]]); every redaction test must go red.

**Found-not-fixed, classified against Java (check the Java `toString()` before
calling a derived `Debug` a leak):**
- *Divergence, same class as the milestone* (Java redacts in `toString()`, Rust has
  the redacting `Display` but a derived `Debug`): `AlterUserScramCredentialsRequest`,
  `IncrementalAlterConfigsRequest` (Java `maskData`), `ConfigEntry` (Java prints
  `Redacted` when `isSensitive`).
- *Divergence on builders* (Java `Builder.toString()` masks, Rust derives Debug):
  `SaslAuthenticateRequestBuilder` (Java: `(type=SaslAuthenticateRequest)`),
  `Renew`/`ExpireDelegationTokenRequestBuilder`,
  `AlterUserScramCredentialsRequestBuilder`, `IncrementalAlterConfigsRequestBuilder`.
- *Rust-only options holding secrets, derived Debug*:
  `CreateDelegationTokenResponseOptions` (token_id, hmac), `ConfigEntryOptions`
  (value even when `is_sensitive`).
- *Java parity, NOT a divergence*: generated `*Data` types — Java's generated
  `toString()` prints bytes with `Arrays.toString` (`MessageDataGenerator.generateFieldToString`);
  Java redacts only at the wrapper.

**`originals` renders as its key set only (Critic 85 fix, Manager's decision).**
The first cut hid `originals` values by key via `SslConfigs`/`SaslConfigs::is_password_config`.
Wrong shape: a raw user map holds EVERY key the user passed, so a by-key filter can only
cover the translated subset — the untranslated OAuth `Type.PASSWORD` keys
(`SaslConfigs.java:392`, `:402`) and unknown keys such as a serializer's
`basic.auth.user.info` still printed. Java never prints a raw `originals` value
(`AbstractConfig.logAll()` :371-385 prints parsed known values, PASSWORD as `[hidden]`;
`logUnused()` :390-395 prints names only). The predicates were deleted (no caller).
**How to apply:** any Debug of a raw user-supplied map (a future Consumer/Admin
`originals`) renders keys only; do not reintroduce a password-key predicate for it.

**Verify without the kafka/ submodule:** `make verify` = build (its `build-c` runs
`git submodule update`) + format-check + lint + `test-rust-all-features`
(`cargo test --all-features -- --skip __grpc`) + test-c + test-python +
check-bindings. `test-rust` (`--workspace`, `--features ffi`) is NOT in `verify`;
run it anyway. When submodule update is forbidden, run the Rust arms directly and
read Java via `curl https://raw.githubusercontent.com/apache/kafka/<sha>/clients/src/main/java/...`
(the generator lives under `generator/src/main/java/...` at the same sha).
