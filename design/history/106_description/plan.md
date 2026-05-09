# Translation Plan: MINOR — Followup KIP-1161 upgrade and test

**AK commit:** `b11b2cdd6a30ae56a0c806b4b279e3b78721a260`
**AK branch:** trunk
**PR:** #106
**Rust branch:** `kafka-translate/b11b2cdd6a30ae56a0c806b4b279e3b78721a260`

---

## Summary of the Apache Kafka Commit

This is a **minor followup** to KIP-1161 that touches only a test file and an
HTML documentation file. No production logic was changed.

**Changed files:**
1. `clients/src/test/java/org/apache/kafka/common/config/ConfigDefTest.java`
   — Fixes a test assertion: the test for duplicate detection in
   `allowAnyNonDuplicateValues` was incorrectly using `List.of("a", "", "b")`
   (which triggers the "values must not be empty" error) instead of
   `List.of("a", "a")` (which triggers the "values must not be duplicated"
   error). The expected message is updated accordingly.

2. `docs/upgrade.html` — Adds "or through the API" to the upgrade note about
   null values no longer being accepted for LIST-type configurations.

**PR reference:** https://github.com/apache/kafka/pull/20989

---

## Rust Translation Analysis

### Does the test exist in Rust?

The `ConfigDef` / `ConfigDefTest` infrastructure may or may not be translated
yet. However, since this is a test-only fix correcting assertion inputs/messages
to properly exercise duplicate-value detection, it depends on whether
`ConfigDef` with its `ListValidator` (specifically `allowAnyNonDuplicateValues`)
has been translated.

### Is there production code to translate?

No. The commit touches only:
- A Java test file (test assertion correction)
- An HTML documentation file (wording clarification)

No Rust library source files need to change.

### Does the documentation change affect Rust?

No. The `docs/upgrade.html` file is Apache Kafka server documentation and is
not part of the client library translation.

### What needs to be done?

This commit is a **no-op for the Rust translation**. The reasons:

1. The test fix corrects a pre-existing bug in the Java test assertions — it
   does not change any behavior or API contract. If and when `ConfigDef`
   validation tests are translated to Rust, they should use the *corrected*
   assertions (testing duplicate values with `vec!["a", "a"]`), which is simply
   the correct way to write the test.

2. The documentation change is server-side upgrade guidance and does not apply
   to the client library.

---

## Implementation Plan

### Phase 1 — No-op confirmation

Since this commit has no translatable production or test code that affects the
Rust codebase:

1. Confirm via `cargo build` that the existing codebase still compiles.
2. Confirm via `cargo test` that the existing test suite passes.

No files need to be created or modified.

---

## Files to Create / Modify

| File | Action | Reason |
|------|--------|--------|
| (none) | — | Commit is a test assertion fix + docs wording; no Rust translation needed |

---

## Out of Scope

- Translating `ConfigDefTest` — the full `ConfigDef` validation framework
  (KIP-1161) translation is tracked separately.
- Translating `docs/upgrade.html` — server documentation is not in scope for
  the client library.

---

## Definition of Done

- [ ] `cargo build` succeeds with no warnings.
- [ ] `cargo test` passes.
- [ ] No Rust source changes required — this PR is a submodule bump only.
