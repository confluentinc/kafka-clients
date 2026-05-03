# PR #40: Revert timing change for creating connect config

## AK Commit

- **SHA**: `80928ac737512dafa087f6bc80f2f5c98c5e730a`
- **Title**: `MINOR: Revert timing change for creating connect config (#20891)`
- **Branch**: `trunk`

## Summary of AK Change

This commit reverts a previous change (Apache Kafka PR #20612) that had adjusted
the creation order of Connect configurations. The original change moved `DistributedConfig`
construction to after `plugins.compareAndSwapWithDelegatingLoader()` was called, ensuring
`plugin.path` validation occurred first. However, the revert was needed because the ordering
change broke loading of classes that only exist on the `plugin.path` classpath.

The commit also adds unit tests in `AbstractConnectCliTest` to prevent a regression.

### Files changed in AK commit

| File | Change |
|---|---|
| `connect/mirror/src/main/java/.../MirrorMaker.java` | Reverts config construction order in `addHerder()` |
| `connect/runtime/src/main/java/.../cli/AbstractConnectCli.java` | Reverts config construction order in `startConnect()`, adds comment |
| `connect/runtime/src/main/java/.../isolation/PluginUtils.java` | Minor addition (3 lines) |
| `connect/runtime/src/test/java/.../cli/AbstractConnectCliTest.java` | New test class (173 lines) |

## Scope Analysis

This Rust project translates the **Apache Kafka client library** (`org.apache.kafka.clients.*`
and supporting `org.apache.kafka.common.*` packages). The CLAUDE.md scope is explicitly
"client only".

All four files changed in this AK commit belong to the **Kafka Connect runtime**
(`org.apache.kafka.connect.*`), which is a separate framework for streaming data between
Kafka and other systems. Kafka Connect:

- Has its own plugin classloading infrastructure (`Plugins`, `PluginUtils`)
- Runs as a standalone distributed service (`Connect`, `Herder`, `Worker`)
- Is not part of the Kafka producer/consumer client API

None of these changed classes exist in the Rust translation target, nor are they
depended upon by any class that has been translated.

## Conclusion: No Rust Changes Required

This AK commit is entirely within Kafka Connect — a component outside the scope of
this Rust client translation. There are no Rust files to create, modify, or delete.

The PR branch exists to track this AK commit in the translation pipeline and confirm
it has been evaluated. No Actor implementation cycle is needed.

## Verification

No build or test steps are required since no code changes are made.
To confirm scope, verify that no source file under `src/` references any class from
the `org.apache.kafka.connect` package.
