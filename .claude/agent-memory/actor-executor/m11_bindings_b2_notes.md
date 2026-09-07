---
name: m11-bindings-b2-notes
description: M11 admin bindings B2 (cluster/configs/log dirs) — edition-2024 RPIT lifetime capture, Java enums without numeric ids, composite C keys, cbindgen callback-typedef grep
metadata:
  type: project
---

Admin C FFI + Python bindings, slice B2 (describeCluster, describeConfigs,
incrementalAlterConfigs, listConfigResources, listClientMetricsResources,
describeLogDirs, alterReplicaLogDirs, describeReplicaLogDirs), landed on
`dev/admin-bindings`. Builds on [[m11_bindings_b0_b1_notes]].

**Why:** B3–B6 repeat the same shape; these four points are not visible from the
code once it works.

**How to apply:** read alongside the B0/B1 note before the next admin slice.

## Edition-2024 RPIT captures the `&dyn Admin` lifetime

A `fn submit_x(admin: &dyn Admin, ...) -> impl Future<...> + Send + 'static`
does **not** compile: in edition 2024 an RPIT captures every in-scope lifetime,
so the closure `|a| Ok(submit_x(a, ...))` fails with "lifetime may not live long
enough". Fix is `+ Send + use<>` (capture nothing). Needed whenever a submit
helper composes several futures rather than returning one `KafkaFuture`.

`describeCluster` is the reason a general `admin_async_future_op` /
`admin_sync_future_op` pair now exists: Java's `DescribeClusterResult` holds four
independent futures (nodes, controller, clusterId, authorizedOperations) that all
have to resolve before one C handle can be built. The KafkaFuture-shaped helpers
are thin wrappers over the general ones. Await all four *then* pick the first
error in Java's field order, rather than `?`-short-circuiting, so none is
abandoned.

## Which Java enums have a numeric id, and which do not

`ConfigResource.Type.id()` and `AlterConfigOp.OpType.id()` exist in Java, so
those cross the C boundary as `int32_t` codes (TOPIC=2, BROKER=4,
BROKER_LOGGER=8, CLIENT_METRICS=16, GROUP=32; SET=0, DELETE=1, APPEND=2,
SUBTRACT=3). `ConfigEntry.ConfigSource` and `ConfigEntry.ConfigType` are plain
Java enums with **no** id — inventing one would be a fabricated contract, so they
cross as their enum constant name string (`"DYNAMIC_TOPIC_CONFIG"`, `"UNKNOWN"`,
…). Check for an `id()` in the Java source before choosing.

Composite keys follow `DeleteRecordsResult`'s precedent instead of a dedicated
handle type: `ConfigResource` → `_get_key_type(i)` + `_get_key_name(i)`,
`TopicPartitionReplica` → `_get_topic/_get_partition/_get_broker_id`. On the
Python side the matching classes need `__eq__`/`__hash__` so they can key the
result dicts as in Java.

## Verifying cbindgen output

Opaque types appear as `} name;`, but **callback typedefs appear as
`typedef void (*name)(`** — a grep for `} name;` silently reports 0 for them and
looks like a missing allowlist entry. Grep the bare name instead. Useful
invariant after `cargo build --features ffi`: the count of
`kafka_admin_AdminClient_*_async(const` declarations must equal the count of
`serialised on one thread` (the callback-thread contract sentence).

`#[deny(warnings)]` makes a deprecation an error, and an `#[allow(deprecated)]`
on the function is not enough — the `use` item importing the deprecated type
needs its own attribute, so split those imports out
(`listClientMetricsResources` is deprecated in Java 4.1).
