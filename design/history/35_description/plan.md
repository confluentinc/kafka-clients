# Translation Design: PR #35

## AK Commit

- **Commit:** `8b119e5105906a66cc99264527670c88b07c7a10`
- **Branch:** `trunk`
- **Title:** `MINOR: Fix library name jacksonDatabindYaml -> jacksonDataformatYaml (#20937)`
- **Author:** Philippus Baalman
- **Date:** 2025-11-21

## Commit Summary

This commit fixes a typo in the Gradle build system: the dependency alias
`jacksonDatabindYaml` (which did not exist) is renamed to the correct name
`jacksonDataformatYaml` (`com.fasterxml.jackson.dataformat:jackson-dataformat-yaml`).

### Changed Files

| File | Change |
|------|--------|
| `build.gradle` | Replace 11 occurrences of `libs.jacksonDatabindYaml` with `libs.jacksonDataformatYaml` |
| `gradle/dependencies.gradle` | Rename alias `jacksonDatabindYaml` → `jacksonDataformatYaml`, reorder alphabetically |

## Translation Impact

**No translation work required.**

This commit exclusively modifies the Java Gradle build system files (`build.gradle`,
`gradle/dependencies.gradle`). There are no changes to Java source files, test files,
or protocol definitions that would require a Rust equivalent.

The Rust project uses Cargo (`Cargo.toml`) for dependency management and has no
equivalent of the `jacksonDataformatYaml` library (YAML serialization for Java).
No action is needed in the Rust codebase.

## Decision

Skip — no Rust translation needed. This PR is a no-op for the Rust client.
