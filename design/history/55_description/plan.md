# Translation Design: PR #55

## AK Commit

- **Commit:** `80928ac737512dafa087f6bc80f2f5c98c5e730a`
- **Title:** `MINOR: Revert timing change for creating connect config (#20891)`
- **Author:** majialong
- **Date:** 2025-11-22

## Summary

This commit reverts a prior initialization-order change in Kafka Connect and adds
unit tests to prevent the same regression from recurring. The original change
(#20612) moved `DistributedConfig` / `createConfig` construction to before
`plugins.compareAndSwapWithDelegatingLoader()`, which prevented plugin classes
that exist only on `plugin.path` from being loaded during configuration
instantiation. This commit restores the correct order (swap the delegating
classloader first, then create config) and adds a warning log for empty
`plugin.path` elements.

## Changed Files in AK

| File | Change |
|------|--------|
| `connect/mirror/src/main/java/org/apache/kafka/connect/mirror/MirrorMaker.java` | Move `DistributedConfig` construction to after `compareAndSwapWithDelegatingLoader()` |
| `connect/runtime/src/main/java/org/apache/kafka/connect/cli/AbstractConnectCli.java` | Move `createConfig()` call to after `compareAndSwapWithDelegatingLoader()` |
| `connect/runtime/src/main/java/org/apache/kafka/connect/runtime/isolation/PluginUtils.java` | Add `log.warn` when a plugin path element is empty |
| `connect/runtime/src/test/java/org/apache/kafka/connect/cli/AbstractConnectCliTest.java` | New test asserting correct initialization order |

## Scope Analysis

All changed files reside in the `connect/` module (Kafka Connect), which is the
server-side Connect framework. This project translates only the **Kafka client**
(`org.apache.kafka.clients.*` and `org.apache.kafka.common.*`). Kafka Connect
infrastructure is explicitly out of scope.

None of the changed classes — `MirrorMaker`, `AbstractConnectCli`,
`DistributedConfig`, `PluginUtils`, `Plugins`, `AbstractConnectCliTest` — have
equivalents in the Rust client codebase, and none are referenced by any
in-scope client code.

## Translation Decision

**No code changes required.**

This commit touches only the Kafka Connect runtime and mirror-maker, which are
not part of the client-side translation. There is no corresponding Rust code to
update, fix, or add.

The design document is recorded here to maintain a complete history of every AK
commit that has been evaluated during the translation project.

## Out-of-Scope Inventory

The following concepts from the commit are entirely outside the client boundary
and will be addressed if/when Kafka Connect translation is ever undertaken:

- `Plugins` / classloader isolation (`org.apache.kafka.connect.runtime.isolation`)
- `DistributedConfig` / `WorkerConfig` configuration hierarchy
- `AbstractConnectCli` startup lifecycle
- `MirrorMaker` distributed mode herder creation
- `ConfigProvider` plugin loading on `plugin.path`

## Action Items

| # | Action | Owner | Status |
|---|--------|-------|--------|
| 1 | Confirm no in-scope client files were silently affected | Manager | Done — diff reviewed, no client files changed |
| 2 | Record PR as no-op translation | Manager | Done — this document |
