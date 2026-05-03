# PR #17: MINOR: Include numeric bound in group config validation errors

## AK Commit

- **Hash**: `22c1e445f17e82ac66800d8150ab46b5547e4035`
- **Title**: `MINOR: Include numeric bound in group config validation errors (#22185)`
- **Author**: David Jacot

## Summary

When a dynamic group config value is out of range, the old error message referenced
the broker-level config name that defines the bound (e.g.
`group.consumer.min.heartbeat.interval.ms`). Operators had to look up the broker
config to know which value would be accepted.

This commit updates `validateIntRange`, `validateIntMin`, and `validateIntMax` helpers
in `GroupConfig` to include the numeric bound directly. For example, setting
`consumer.heartbeat.interval.ms` below the minimum now reports:

```
consumer.heartbeat.interval.ms must be greater than or equal to 5
```

A new parameterized test `testValidationErrorMessageIncludesBound` covers both
directions for each range-bounded config and the max-only and min-only checks.

## Java Source Files Changed

| File | Change type |
|------|-------------|
| `group-coordinator/src/main/java/org/apache/kafka/coordinator/group/GroupConfig.java` | Refactor `validateIntRange` / `validateIntMin` / `validateIntMax` signatures and error strings |
| `group-coordinator/src/test/java/org/apache/kafka/coordinator/group/GroupConfigTest.java` | New parameterized test `testValidationErrorMessageIncludesBound` |
| `clients-integration-tests/…/PlaintextAdminIntegrationTest.scala` | Update expected error string |
| `clients/src/test/…/ConfigCommandIntegrationTest.java` | Update expected error strings |

## Current Rust State

`GroupConfig` does not yet exist in the Rust codebase. There is no
`src/coordinator/` module. This PR must translate `GroupConfig.java`
from scratch, with the new (post-commit) error message format as the
canonical target — there is no old Rust version to upgrade.

## Scope

**In scope:**

- `GroupConfig` struct mirroring the Java class:
  - `group_config.rs` in a new `src/coordinator/group/` module
  - All config key constants (`CONSUMER_SESSION_TIMEOUT_MS_CONFIG`, etc.)
  - `CONFIG_DEF` equivalent: a static `HashMap<&str, ConfigDef>` describing each key,
    its type, default, and docs — or a simpler direct-validation approach if
    the Rust codebase does not yet have a `ConfigDef` abstraction
  - `GroupCoordinatorConfig` bounds struct (minimal — only the bound accessors
    called by `validateValues`) to support validation
  - `validate_names`, `validate`, `validate_values` (private)
  - `validate_int_range`, `validate_int_min`, `validate_int_max` private helpers
    with the **new** error message format (numeric bound included)
  - `validate_session_exceeds_heartbeat` cross-field validation
  - `ALL_GROUP_CONFIG_SYNONYMS` map and `broker_synonym` accessor
  - `config_type` and `config_names` accessors
- Unit tests mirroring `GroupConfigTest.java`:
  - `test_validation_error_message_includes_bound` (the new test from this commit)
  - Existing tests from `GroupConfigTest` that are straightforward to port
    (`test_from_props_invalid`, `test_validate_names`, range boundary tests)

**Out of scope (deferred):**

- `evaluate` / `clampToRange` (value-capping path — used at runtime, not in validation)
- Share group DLQ configs (`errors.deadletterqueue.*`)
- `optionalInt` / `optionalBoolean` / `optionalString` accessors and the full
  `GroupConfig(props)` constructor (field hydration) — only needed when group
  coordinator runtime is translated
- Integration-test adaptations (`PlaintextAdminIntegrationTest`,
  `ConfigCommandIntegrationTest`) — no integration harness exists in Rust yet
- `ShareGroupConfig` bounds (defer until share group coordinator is translated)

## Error Message Format (canonical)

After this commit the Java helpers produce:

```
// validateIntRange:
"{key} must be in the range {min} to {max} inclusive."

// validateIntMax:
"{key} must be less than or equal to {max}"

// validateIntMin:
"{key} must be greater than or equal to {min}"

// validateSessionExceedsHeartbeat:
"{session_key} must be greater than {heartbeat_key}"
```

The Rust translation must produce identical strings so that any future
integration test that asserts on the error text passes without adaptation.

## Rust Module Structure

```
src/
└── coordinator/
    └── group/
        ├── mod.rs              # pub use GroupConfig; pub use GroupCoordinatorConfig;
        ├── group_config.rs     # GroupConfig struct + validation helpers
        └── group_coordinator_config.rs  # GroupCoordinatorConfig bounds (minimal)
```

The new module is wired in via `src/lib.rs`:
```rust
pub mod coordinator;
```

## Key Design Decisions

### No ConfigDef abstraction yet

The Rust client does not have a port of `ConfigDef` / `AbstractConfig`. Rather than
introducing a heavyweight abstraction for a single use-case, `GroupConfig` will:

1. Expose a `const` array or `HashMap<&'static str, ConfigMeta>` for
   config-name validation (`validate_names`).
2. Parse values from `HashMap<String, String>` directly (each key parsed to its
   expected type: `i32`, `bool`, or `String`).
3. Perform range validation via the three private helpers.

This matches the spirit of the Java implementation while staying consistent with
the simpler, direct-parse patterns used elsewhere in the Rust codebase.

### Error type

Validation errors return `KafkaError` (already defined in
`src/common/kafka_error.rs`) with variant `InvalidConfiguration(String)` —
mirroring Java's `InvalidConfigurationException`.

If `InvalidConfiguration` does not yet exist in `KafkaError`, add it as part of
this PR.

### GroupCoordinatorConfig bounds

Only the accessor methods called by `validateValues` are needed. Implement
`GroupCoordinatorConfig` as a plain struct with public `i32` fields and default
constants matching the Java defaults (pulled from
`GroupCoordinatorConfig.java`). Mark `ShareGroupConfig` bounds with placeholder
constants for now (share coordinator not yet translated).

## Phases

| Phase | Description | Files | Build gate |
|-------|-------------|-------|------------|
| 1 | New module skeleton + `GroupCoordinatorConfig` bounds | `src/coordinator/group/mod.rs`, `group_coordinator_config.rs`, `src/coordinator/mod.rs`, `src/lib.rs` | `cargo build` |
| 2 | `GroupConfig` constants, `config_names`, `broker_synonym`, `config_type` | `src/coordinator/group/group_config.rs` | `cargo build` |
| 3 | Validation helpers (`validate_int_range`, `validate_int_min`, `validate_int_max`, `validate_session_exceeds_heartbeat`, `validate_names`, `validate`) | same file | `cargo build` |
| 4 | Unit tests mirroring `GroupConfigTest.java` | `tests/coordinator/group/group_config_test.rs` or inline `#[cfg(test)]` | `cargo test` |

Each phase ends with `cargo build`, `cargo test`, `cargo xtask format-check`,
`cargo xtask lint` all passing.

## Definition of Done

- [ ] `cargo build` passes with no warnings
- [ ] `cargo test` passes — all new tests green
- [ ] `cargo xtask format-check` passes
- [ ] `cargo xtask lint` passes
- [ ] `validate_int_range` / `validate_int_min` / `validate_int_max` produce the
      exact error strings specified in the **Error Message Format** section above
- [ ] `test_validation_error_message_includes_bound` (parameterized) covers:
      - each range-bounded config below min → `"must be in the range X to Y inclusive."`
      - each range-bounded config above max → `"must be in the range X to Y inclusive."`
      - each max-only config above max → `"must be less than or equal to Y"`
      - each min-only config below min → `"must be greater than or equal to X"`
