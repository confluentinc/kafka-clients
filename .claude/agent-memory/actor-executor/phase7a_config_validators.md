---
name: Phase 7a ConfigDef validators + SSL/SASL helpers
description: New validators in config_def.rs and SSL/SASL schema-extension helpers; lessons learned during Phase 7a foundation
type: project
---

Foundation work for Phase 7a (ProducerConfig translation):

* `CaseInsensitiveValidString` — for `security.protocol`, `metadata.recovery.strategy`. Java's error message format: `"String must be one of (case insensitive): A, B"` — we match exactly. Important: Java treats `null` as invalid for this validator (no isNullAllowed flag).
* `NonEmptyString` — for `transactional.id`. Allows `Null` but rejects empty `String` (matches Java's `s != null && s.isEmpty()`).
* `ValidList` — for `bootstrap.servers`, `interceptor.classes`, `metric.reporters`, `ssl.cipher.suites`, `ssl.enabled.protocols`, `sasl.oauthbearer.expected.audience`. Constructed via `any_non_duplicate_values(is_empty_allowed, is_null_allowed)` per Java's `anyNonDuplicateValues`. Validates: null per flag, empty per flag, dedup, per-entry empty rejection.

**Why:** ProducerConfig schema needs all four validators. They were missing from the Phase 1 ConfigDef slice.

**How to apply:**
- When adding a Java `ValidString`/`CaseInsensitiveValidString`/`NonEmptyString`/`ValidList` validator in any new config schema, use these helpers from `crate::common::config::config_def`.
- Rust `parse_typed` for `Type::List` strips empty entries, so the per-entry empty rejection only fires when callers manually construct a `ConfigValue::List`.
- For `Type::List` with `Vec<String>` defaults, register `ConfigValue::List(Vec::new())` (Java default `List.of()`) — never `ConfigValue::Null` unless Java explicitly defaults `null`.

**SSL/SASL schema extension** lives in `ssl_configs::add_client_ssl_support` and `sasl_configs::add_client_sasl_support` (free functions, Java parity per CLAUDE.md "static functions exported only by the file defining them"). The methods on `ConfigDef::with_client_ssl_support` / `with_client_sasl_support` are thin wrappers that delegate.

**Pitfall hit:** clippy `useless_concat` fires on a `concat!` macro with a single argument. The formatter sometimes collapses a multi-arg `concat!` to single-arg when one of the strings is removed. Always use a plain string literal when there is no concatenation.
