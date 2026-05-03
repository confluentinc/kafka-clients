# PR #20 — Move SecurityUtils, ConfigUtils, LogContext, AppInfoParser into internal

## AK Commit

`10805c9782a2285a77fb0bf922db05590afb2704`
`KAFKA-20297 Move SecurityUtils, ConfigUtils, LogContext, AppInfoParser into internal (#22110)`

## Summary of the Java Change

The commit moves four utility classes from the public package
`org.apache.kafka.common.utils` to the internal sub-package
`org.apache.kafka.common.utils.internals`:

| Class | Old Java path | New Java path |
|---|---|---|
| `LogContext` | `org.apache.kafka.common.utils.LogContext` | `org.apache.kafka.common.utils.internals.LogContext` |
| `ConfigUtils` | `org.apache.kafka.common.utils.ConfigUtils` | `org.apache.kafka.common.utils.internals.ConfigUtils` |
| `SecurityUtils` | `org.apache.kafka.common.utils.SecurityUtils` | `org.apache.kafka.common.utils.internals.SecurityUtils` |
| `AppInfoParser` | `org.apache.kafka.common.utils.AppInfoParser` | `org.apache.kafka.common.utils.internals.AppInfoParser` |

The change is **pure package relocation** — all class bodies, method
signatures, and behaviour are identical before and after. The motivation
is that these classes were erroneously visible as public API; they are
implementation details of the Kafka client.

## Impact on the Rust Translation

### Naming Convention (CLAUDE.md rule 2)

> Classes whose package contains `internal` MUST use only `pub(crate)`

Because the new package contains `internals`, all four items become
crate-internal in Rust (`pub(crate)`).

### Module Mapping

The Java package structure `common.utils.internals` maps to the Rust
module path `common::utils::internals`. A new `internals` sub-module
must be created inside `src/common/utils/`.

## Current State in the Rust Codebase

| Item | Current Rust location | Visibility | Action needed |
|---|---|---|---|
| `LogContext` | `src/common/utils/log_context.rs` | `pub` | Move to `utils::internals`, make `pub(crate)` |
| `ConfigUtils` | — not translated — | — | Translate into `utils::internals` |
| `SecurityUtils` | — not translated — | — | Translate into `utils::internals` |
| `AppInfoParser` | — not translated — | — | Translate (reduced form) into `utils::internals` |

## Detailed Changes

### 1. Create `src/common/utils/internals/` submodule

Create `src/common/utils/internals/mod.rs` and re-export the four
items with `pub(crate)` visibility. Update `src/common/utils/mod.rs`
to declare the new sub-module and remove the now-misplaced `log_context`
declarations.

### 2. Move `LogContext`

- Move `src/common/utils/log_context.rs` to
  `src/common/utils/internals/log_context.rs`.
- Change all `pub struct LogContext` and `pub impl` items to
  `pub(crate)`.
- Update the module doc comment to reference
  `org.apache.kafka.common.utils.internals.LogContext`.
- Fix all `use crate::common::utils::LogContext` imports throughout the
  codebase to `use crate::common::utils::internals::LogContext` (or
  through the parent module re-export if one is kept for convenience).
- Remove the `pub use log_context::LogContext` re-export from
  `src/common/utils/mod.rs`.

### 3. Translate `ConfigUtils`

File: `src/common/utils/internals/config_utils.rs`

Methods to translate:

| Java method | Rust function |
|---|---|
| `configMapToRedactedString(Map<String,Object>, ConfigDef)` | `config_map_to_redacted_string(map: &HashMap<String,Object>, config_def: &ConfigDef) -> String` |
| `getBoolean(Map<String,Object>, String, boolean)` | `get_boolean(configs: &HashMap<String, Box<dyn Any>>, key: &str, default_value: bool) -> bool` |

Both functions are `pub(crate)`.

`config_map_to_redacted_string` produces a sorted `{key=value, ...}`
string where sensitive (`ConfigKey::is_sensitive()`) or unknown keys are
replaced with `(redacted)`, and string values are quoted.

`get_boolean` returns the `bool` value for `key` in `configs`, logging
an error and falling back to `default_value` when the value is not a
`bool` or parseable `&str`.

### 4. Translate `SecurityUtils`

File: `src/common/utils/internals/security_utils.rs`

The class provides case-insensitive lookups between string names and
Kafka ACL enum variants plus string serialisation. The Java static
initializer populates three `HashMap`s at class load time; Rust uses
`once_cell::sync::Lazy` (or `std::sync::LazyLock` on Rust ≥ 1.80) for
the equivalent static maps.

Functions (all `pub(crate)`):

| Java method | Rust function |
|---|---|
| `parseKafkaPrincipal(String)` | `parse_kafka_principal(s: &str) -> Result<KafkaPrincipal, KafkaError>` |
| `addConfiguredSecurityProviders(Map)` | `add_configured_security_providers(configs: &HashMap<String, String>)` |
| `resourceType(String)` | `resource_type(name: &str) -> ResourceType` |
| `operation(String)` | `operation(name: &str) -> AclOperation` |
| `permissionType(String)` | `permission_type(name: &str) -> AclPermissionType` |
| `resourceTypeName(ResourceType)` | `resource_type_name(rt: ResourceType) -> String` |
| `operationName(AclOperation)` | `operation_name(op: AclOperation) -> String` |
| `permissionTypeName(AclPermissionType)` | `permission_type_name(pt: AclPermissionType) -> String` |
| `authorizeByResourceTypeCheckArgs(AclOperation, ResourceType)` | `authorize_by_resource_type_check_args(op: AclOperation, rt: ResourceType) -> Result<(), KafkaError>` |

`toPascalCase` is a private helper; translate as `fn to_pascal_case(name: &str) -> String`.

`add_configured_security_providers` is a no-op stub in Rust — the JVM
security-provider plugin mechanism (`java.security.Security`) has no
direct equivalent. Log a warning if the config key is set.

### 5. Translate `AppInfoParser` (reduced form)

File: `src/common/utils/internals/app_info_parser.rs`

`AppInfoParser` has two concerns in Java:

1. **Version/commit info** — reads `kafka/kafka-version.properties` from
   the classpath at startup.
2. **JMX registration** — registers an MBean and metrics. JMX is
   Java-specific; this part is **not translated**.

In Rust, the version and commit-id can be embedded at compile time via
`env!("CARGO_PKG_VERSION")` for the version, and a build-script-computed
git commit hash for `COMMIT_ID`.

```rust
// pub(crate) constants
pub(crate) const VERSION: &str = env!("CARGO_PKG_VERSION");
pub(crate) const COMMIT_ID: &str = env!("KAFKA_COMMIT_ID"); // set in build.rs
```

Provide `pub(crate)` free functions:

| Java method | Rust function |
|---|---|
| `getVersion()` | `get_version() -> &'static str` |
| `getCommitId()` | `get_commit_id() -> &'static str` |
| `registerAppInfo(…)` | omitted (JMX only) |
| `unregisterAppInfo(…)` | omitted (JMX only) |

`build.rs` should set `KAFKA_COMMIT_ID` by running
`git rev-parse --short HEAD` and falling back to `"unknown"`.

## File List

```
src/common/utils/internals/
    mod.rs              — new; declares sub-modules, re-exports with pub(crate)
    log_context.rs      — moved from src/common/utils/log_context.rs
    config_utils.rs     — new translation
    security_utils.rs   — new translation
    app_info_parser.rs  — new translation (reduced)
src/common/utils/mod.rs — updated: add internals sub-module, remove old log_context
```

All existing callers of `LogContext` within the crate (e.g.
`src/network_client.rs`, `src/metadata.rs`, `src/cluster_connection_states.rs`,
etc.) keep their existing import paths if the parent `utils::mod.rs`
re-exports `LogContext` as `pub(crate) use internals::LogContext;`.

## Tests

For each new module, translate the corresponding Java unit tests:
- `ConfigUtilsTest` → `config_utils` inline `#[cfg(test)]` module
- `SecurityUtilsTest` → `security_utils` inline `#[cfg(test)]` module
- `AppInfoParserTest` → `app_info_parser` inline `#[cfg(test)]` module
  (version/commit-id tests only; skip JMX tests)
- Existing `log_context` tests remain in the moved file

## Verification

```
cargo build
cargo test
cargo xtask format-check
cargo xtask lint
```

No integration-test changes are expected because these utilities are
crate-internal and not exposed through the C FFI.
