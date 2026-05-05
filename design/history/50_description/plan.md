# Translation Design: MINOR — Fix library name jacksonDatabindYaml -> jacksonDataformatYaml

**AK commit:** `8b119e5105906a66cc99264527670c88b07c7a10`
**AK branch:** trunk
**PR:** #50
**Rust branch:** `kafka-translate/8b119e5105906a66cc99264527670c88b07c7a10`

---

## Summary of the Java Commit

This is a pure build-system rename fix. The commit corrects a misnamed
Gradle dependency alias used throughout the Apache Kafka project:

- **Old alias:** `jacksonDatabindYaml`
- **New alias:** `jacksonDataformatYaml`

Both aliases resolve to the same Maven artifact:
`com.fasterxml.jackson.dataformat:jackson-dataformat-yaml`.

The old name `jacksonDatabindYaml` was misleading because:
1. It implied a relation to `jackson-databind`, which is a different
   artifact (`com.fasterxml.jackson.core:jackson-databind`).
2. The correct artifact family for data-format extensions is
   `jackson-dataformat-*`, not `jackson-databind-*`.

### Files changed

| File | Change |
|---|---|
| `build.gradle` | Replaced all 12 occurrences of `libs.jacksonDatabindYaml` with `libs.jacksonDataformatYaml` |
| `gradle/dependencies.gradle` | Renamed the alias key from `jacksonDatabindYaml` to `jacksonDataformatYaml` (artifact coordinates unchanged) |

The change has zero runtime or behavior effect — it is exclusively a
rename of a build-script identifier.

---

## Applicability to the Rust Client Library

### No translation required

This commit touches only the Gradle build system and its dependency
alias definitions. The Rust client library:

1. **Does not use Gradle.** It uses Cargo (`Cargo.toml` /
   `Cargo.lock`) as its build system and dependency manager.
2. **Does not depend on Jackson.** Jackson is a Java JSON/YAML
   serialization library. The Rust client uses `serde` and
   `serde_json` for serialization instead.
3. **Has no equivalent alias indirection layer** — Cargo dependencies
   are referenced directly by crate name and version in `Cargo.toml`,
   with no separate alias file that could harbor a misnaming.

Because there is no Rust counterpart to either the `build.gradle`
dependency listing mechanism or the `jackson-dataformat-yaml` library,
there is nothing to translate, rename, or fix.

### Completeness check

No other files were touched by this commit. The PR description confirms
the change is cosmetic/documentation-quality: fixing a confusing name
so future readers are not misled. This rationale has no analog in the
Rust codebase.

---

## Rust Implementation Plan

**No changes required.**

This commit is entirely out of scope for the Rust client library
translation. The PR for this branch should be marked as a no-op
translation with a note explaining why.

### Files NOT changed

| Java file | Reason not translated |
|---|---|
| `build.gradle` | Gradle build system, no Rust equivalent |
| `gradle/dependencies.gradle` | Gradle dependency alias file, no Rust equivalent |

---

## Test Plan

No tests are required. There are no code changes to verify.
