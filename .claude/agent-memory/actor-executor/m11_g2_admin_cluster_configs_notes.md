---
name: m11-g2-admin-cluster-configs-multilanguage
description: Milestone 11 G2 (admin multilanguage cluster/configs/log dirs) — when the harness needs a parallel value type, the nested-map verdict for describeLogDirs, the wrong reachability assumption about authorizedOperations, and the cheapest conclusive teeth check for a container backend
metadata:
  type: project
---

Slice G2 of `design/history/Milestone-11/PLAN-multilanguage-admin.md`: the eight
cluster / config / log-dir RPCs across all four backends, 44 green entries.

## The envelope needed no fourth shape

All eight fit the three shapes G1 settled, and the shape was decided by reading
each RPC's `src/admin/*_result.rs` rather than by analogy:

  - **Whole-value** for the three with no per-key futures — `describeCluster`
    (four *independent* futures over attributes of one cluster),
    `listConfigResources` and `listClientMetricsResources` (one future over a
    collection each).
  - **Value-carries-its-own-error** for `describeLogDirs` (`LogDirDescription.error`).
  - **Ordinary per-key oneof** for the other four, needing two new `ResultKey`
    arms (`ConfigResource`, `TopicPartitionReplica`). Appending a `oneof` arm is
    wire-compatible, so this is cheap; G3..G6 will keep doing it.

**`describeLogDirs`' nested map needs no flattening.** The value is
`LogDirDescriptionMap { map<string, LogDirDescription> }` inside the entry's
`oneof`, keeping broker → log dir → `ReplicaInfo` at three levels — the C
boundary already does exactly this (`DescribeLogDirsResult_get_value` yields a
`LogDirDescriptionMap_t` with its own `_get_key` / `_get_value`). A `map` field
cannot sit directly in a `oneof`, hence the wrapper message; `ReplicaInfo` pairs
are a `repeated` field rather than a map because proto3 map keys must be scalars
and the key is a `TopicPartition`.

## G1's "no parallel value types needed" does not hold for G2 — check `fn new`

G1 recorded that every value type crossed as the production public type. Three
G2 values cannot, and the blocker is always a `pub(crate)` constructor that is
*faithful to Java* and must not be widened:

  - `ConfigSynonym::new` is `pub(crate)`; Java's
    `ConfigEntry.ConfigSynonym(String, String, ConfigSource)` is package-private
    (`ConfigEntry.java:243`). `ConfigEntry::with_metadata` is public but takes
    `Vec<ConfigSynonym>`, so the whole entry needs a view type
    (`ConfigEntryView` / `ConfigSynonymView` / `ConfigView`).
  - `ReplicaLogDirInfo::new` is `pub(crate)`; Java's constructors are
    package-private (`DescribeReplicaLogDirsResult.java:71,75`).
  - `ClusterDescription` has no production counterpart at all — it is the
    convention-#4 struct-of-resolved-values, which `admin.py` already invented
    for the same reason.

`LogDirDescription` / `ReplicaInfo` / `ClientMetricsResourceListing` /
`ConfigResource` / `TopicPartitionReplica` **do** have public constructors, so
they cross as production types. **Grep `fn new` visibility for every value type
before assuming either way.**

Do NOT reach for G1's `comparable_config` projection here. That existed because
`createTopics` really only carries five `ConfigEntry` fields through every
binding; `describeConfigs` carries all nine (`kafka_admin_ConfigEntry_*`,
`admin.py`'s `_to_full_config_entry`), so projecting would retire real
differential coverage. Two encoders now coexist in `grpc_translate.py`:
`_admin_config_entry_to_proto` (5 fields, createTopics) and
`_admin_full_config_entry_to_proto` (9, describeConfigs), with the extra proto
fields `optional` so "this RPC does not report it" stays distinct from
"reported as UNKNOWN".

`ConfigSource` / `ConfigType` cross as Java **enum constant name strings** —
neither enum has a numeric id, so the name is the C contract. The mapping table
is private in `src/ffi/admin.rs` (`config_source_name` / `config_type_name`), so
the harness restates it; that file's
`config_source_name_matches_java_enum_constant_names` test pins the same table.

## A reachability assumption that was wrong

I asserted `describeCluster().authorizedOperations` is `None` on an
authorizer-less broker, reasoning from `AdminUtils.validAclOperations` mapping
`Integer.MIN_VALUE` to null. **A KRaft broker with no authorizer still computes
them** — `User:ANONYMOUS` is a super user and gets
`{Create, Alter, Describe, ClusterAction, DescribeConfigs, AlterConfigs, IdempotentWrite}`.
The *null* branch is the unreachable one on this fixture. The test caught it in
one run. General lesson: for "the broker omits field X" claims, run it before
writing the assertion — the omission path usually needs a specific broker
configuration, not merely the absence of one.

## Genuinely unreachable, with the citation

  - `LogDirDescription.error` — the broker sets it only for a directory it has
    marked offline via `LogDirFailureChannel` after an I/O failure. Needs a disk
    to fail mid-run. Recorded in the log-dirs module docs.
  - A per-broker error entry, and any multi-broker `describeLogDirs` fan-out —
    one broker means one entry, and it always answers.
  - A cross-*broker* replica move is not a state `alterReplicaLogDirs` has:
    `AlterReplicaLogDirsRequest` is per-broker. Reassignment does that. Don't
    file it as a coverage gap.
  - A cross-*directory* move IS reachable, via `ClusterConfig::with_properties`
    with two `KAFKA_LOG_DIRS`. The 3-argument `multilanguage_admin_test!` form
    takes the cluster config, and `backend_pool` keys containers by
    `(kind, broker_network)`, so a distinct cluster just starts its own — no
    extra work.

## The cheapest conclusive teeth check for a container backend

Worry: are the `__grpc_*` arms running the image I just built, or a stale one?
Two independent proofs, both free:

 1. **New RPCs are self-proving.** A stale image lacks the method entirely, so
    tonic returns `UNIMPLEMENTED` and the arm fails. Any green arm on a *new*
    RPC is proof of a fresh image. (Not true for an edit to an existing handler —
    that needs proof 2.)
 2. **Transpose one field in one server, rebuild only that image, run.** I
    swapped `source`/`config_type` in the sync Python encoder: exactly the four
    `__grpc_python` arms went red, rust / python_async / c stayed green. That is
    both a freshness proof and a proof the assertions discriminate, in ~90 s.
    Restore with a saved copy and **rebuild the image again** — the mtime trap
    applies to Docker contexts too.

## Environment

  - `cargo xtask format-check` / `lint` need the nix 1.97.1 triple. The PATH that
    works (all four dirs, in this order): `*-cargo-1.97.1/bin`,
    `*-rustfmt-1.97.1/bin`, `*-clippy-1.97.1/bin`, `*-rustc-1.97.1/bin`. Putting
    the rustc dir first silently yields `cargo 1.95.0`.
  - `cargo xtask lint`'s second pass is `--all-features --all-targets`, which
    **does** cover the integration and performance test targets. An older memory
    note claiming otherwise was stale and is corrected.
  - `cargo xtask check-generated` fails on
    `join_group_response_data.rs` (a blank-line diff in generated wire code).
    Pre-existing, unrelated to any admin work — Critic round 4 already
    adjudicated it as environment/pre-existing.
  - No new Dockerfile / CMakeLists edits were needed: G2 added no proto *file*
    and no Python module, only messages and handlers.
