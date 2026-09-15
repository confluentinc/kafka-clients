---
name: admin_per_key_callbacks_phase_c_notes
description: Phase C (Log dirs family — describeLogDirs, alterReplicaLogDirs, describeReplicaLogDirs) of the admin FFI per-key async callback rollout — the mock-omission-vs-pre-built-futures-dict hazard this phase surfaced
metadata:
  type: project
---

Branch `feat/admin-per-key-callbacks-phase-c` (worktree
`.claude/worktrees/actor4-admin-per-key-callbacks-c`), off Phase B's tip
(`feat/admin-per-key-callbacks-phase-b`). Full plan:
`/Users/pratyush/.claude/plans/spicy-splashing-kitten.md`. Read
`admin_per_key_callbacks_phase_a_notes.md` and `_phase_b_notes.md` first — this
phase reused the mechanism verbatim (`admin_async_per_key_op`,
`KafkaFuture::when_complete`, the Python `Future`-per-key closure pattern, the
C trampoline pattern).

## Scope

Three RPCs: `describeLogDirs` (key: broker id `i32`), `alterReplicaLogDirs`
and `describeReplicaLogDirs` (key: `TopicPartitionReplica`, a THREE-part
composite — `(topic, partition, broker_id)`, one field wider than Phase B's
`ConfigResource` two-part key). Delivered as three C params, not an opaque
handle — matching the existing flattened result's `_get_topic`/`_get_partition`/
`_get_broker_id` triple, generalizing Phase A/B's "check the existing
flattened key representation before inventing a new one" rule to a 3-tuple.

## The one genuinely new wrinkle: §D2's nested-handle case, reused not rebuilt

`describeLogDirs`'s per-broker value is `Map<String, LogDirDescription>` —
already had a nested handle (`kafka_admin_LogDirDescriptionMap_t`) minted for
the flattened sync-result path (§D2's "fifth rule": mint a handle when the
element itself contains a collection). Phase C changes ONLY the outer
per-broker delivery: `LogDirDescriptionMapInner::new(&map)` is called exactly
as before, just `Box::into_raw`'d individually and owned instead of stored
inline in `DescribeLogDirsResultInner`'s `Vec`. Needed a NEW standalone
destructor (`kafka_admin_LogDirDescriptionMap_destroy`) — same dual-provenance
pattern as Phase B's `kafka_admin_Config_destroy` (borrowed-from-flattened-
result vs. owned-from-per-key-callback, same type name, two destroy rules).
`kafka_admin_ReplicaLogDirInfo_t` got the identical treatment
(`kafka_admin_ReplicaLogDirInfo_destroy`). Zero new marshaling logic needed —
this is the cheapest of the three per-key value shapes seen so far (Phase A:
new standalone handles for scalar/simple types; Phase B: `Config`, same
pattern; Phase C: reuse an ALREADY-nested handle type verbatim).

## The hazard this phase surfaced, and why my first resolution was WRONG (COMMENTS.67)

`MockAdminClient::describe_replica_log_dirs` (`src/admin/mock_admin_client.rs`)
mirrors Java's own `MockAdminClient.describeReplicaLogDirs`
(`MockAdminClient.java:1112`, `if (topicMetadata != null)`): a replica of a
topic the mock does not know is skipped ENTIRELY — no `KafkaFutureImpl` is
ever created for it, so it is simply absent from `result.values()`.

This collides with the per-key architecture's core assumption: Python (and
the C caller in general) must pre-build one `Future`/`Promise` per requested
key BEFORE calling the async FFI entry point, so the native callback,
whenever it fires, has somewhere to deliver to. If a key never gets a
callback at all (as opposed to an error callback), that pre-built
`Future`/`Promise` hangs forever.

**My first pass got the fix wrong**: I documented this as an accepted,
"Java-faithful mock behavior" limitation (citing admin-client.md §9's "mirror
Java's MockAdminClient method-for-method") and added a test proving only the
*pending* state, not resolution. **A Critic review (COMMENTS.67) correctly
rejected this.** The citation was the wrong layer: nobody asked to change
what the mock resolves to (§9 is about the mock's *values*, not this
wrapper's *delivery* contract). Java's real `MockAdminClient` never promises
a `Future` per key in the first place — a missing key is simply absent from
the returned `Map`, which a Java caller detects immediately
(`results.get(key) == null`). The hang is a consequence purely of *this
port's own wrapping design* (pre-building a `Future` per input key before
the call returns), not something inherited faithfully from Java. It was also
reachable outside the unit test: `grpc_translate.py`'s
`_resolve_admin_futures`/`_resolve_admin_futures_async` call
`.result()`/`await fut` with no timeout, so the gRPC harness would hang too.

**The actual fix, generic, in `admin_async_per_key_op` itself**
(`src/ffi/admin.rs`) — since this helper is shared by every per-key RPC
across Phases A–C (and D–G to come), fixing it here protects all of them:
after `submit` succeeds, any key present in `keys` but absent from `entries`
now fires an explicit `Error::local_illegal_state(...)` synchronously,
extending the existing total-submission-failure fan-out (which already
handled "every key fails with the same error" for a null admin or `submit`
`Err`) to also cover this **partial**-success gap. Needed a new `K: PartialEq`
bound — verified against all 15 call sites' key types
(`String`/`TopicPartition`/`ConfigResource`/`i32`/`TopicPartitionReplica`,
all already `Eq + Hash` for `HashMap` usage elsewhere) — zero call-site
changes required, crate builds clean. Used a `claimed: Vec<bool>` mask over
`entries`' positions (not a plain membership check) so a **repeated** key in
`keys` (raw, undeduped input — `describe_log_dirs`'s broker-id list, say)
claims at most one real entry; a second occurrence of an already-claimed key
is correctly reported missing too, not silently treated as satisfied by the
first occurrence's entry (which would leave it with zero callbacks, since
`entries` only fires once for that key).

**Confirmed zero behavior change for Phases A/B**: none of their `entries`
builders can currently produce fewer entries than `keys` (their admin-core
implementations always create one future per requested/deduped key), so the
new fan-out path is provably never reached for them — full `cargo test --lib
--features ffi` count is identical before/after apart from the 3 new tests.

**Lesson for later phases (D–G) and for future Critic rounds on this
mechanism**: before assuming "every requested key eventually gets a
callback," check whether the Java/mock source can OMIT a key from its
per-key future map entirely (`continue`/`skip` before creating a
`KafkaFutureImpl`, not just `complete_with_error`) — grep for an early
`continue`/skip guard keyed on caller input. Thanks to this fixup,
`admin_async_per_key_op` now closes that gap generically, so a later phase
hitting the same pattern should NOT need its own fix — but verify the
generic fan-out actually reaches the affected RPC's key type (it will, since
the bound is `PartialEq` and every admin key type already satisfies it) and
add the phase-specific end-to-end test anyway (three layers: Rust unit test
against the real async entry point + a real `MockAdminClient`, Python test
asserting eventual resolution with an explicit error within a bounded
timeout, C test doing the same through the ABI) rather than assuming the
generic mechanism test alone is sufficient coverage for a new call site.
**Second lesson, on my own process**: "matches Java" is not a blanket
excuse for a design choice that Java's own architecture doesn't actually
require — check whether the divergence is forced by JAVA'S CONTRACT or is
purely an artifact of THIS PORT'S OWN wrapping/translation choices (here:
pre-building futures eagerly) before citing a "mirror Java" rule to justify
leaving something documented-but-broken.

## Dedup direction: no `Entry::Vacant` question here, but cardinality still matters

Unlike Phase A's `create_topics` (where a duplicate key's VALUE varies and
dedup direction was load-bearing), none of these three RPCs' per-key
inputs carry a distinguishable "value" beyond the key itself (broker id is
just an id; a `TopicPartitionReplica` combined with its move-target log dir
for `alter_replica_log_dirs` is a dict already, so Python dict semantics
dedupe it before this even matters). So dedup direction (first- vs
last-occurrence-wins) is a non-issue here — same as Phase B's
`_describe_configs_keys_and_spec`. What DOES matter, mirroring the Rust core:

  - `describe_log_dirs`: `admin.describe_log_dirs()` keys its per-broker
    future map with `HashMap::insert` over the (possibly-duplicated) input
    slice — a duplicate broker id in the raw C array collapses to ONE
    resolved key, not two. Python's `_describe_log_dirs_keys_and_spec` MUST
    dedupe (`dict.fromkeys`) before building `keys`/`spec`, or
    `admin_incref_n`'s count (built from the deduped `spec` length) would
    still be right, but the STALE mental model of "one Future per input
    broker" would be wrong (a duplicate would just silently share one
    Future — better to dedupe explicitly so `set(futures)` accurately
    reflects what will actually resolve).
  - `describe_replica_log_dirs`: same reasoning, `dict.fromkeys(replicas)`
    (the input is a `Collection`, not a `Map`, so raw duplicates ARE
    possible at the Python API surface, unlike `alter_replica_log_dirs`
    whose input is already a `{TopicPartitionReplica: log_dir}` dict).
  - `alter_replica_log_dirs`: input already a dict — `keys =
    list(replica_assignment.keys())` needs no dedup step, mirroring
    `_incremental_alter_configs_keys_and_spec`'s treatment of its
    already-a-dict input.

## Verification actually run (per Phase A/B's Critic-round lesson)

```
cargo build --features ffi              # debug, fast iteration
cargo build --features ffi --release    # bindings/c looks in target/release first
git submodule update --init             # bindings/c/tests/unity was uninitialized in this fresh worktree
cmake -S bindings/c -B bindings/c/build -DRUST_PROJECT_ROOT=$(pwd)
cmake --build bindings/c/build -j 4
cd bindings/c/build && ctest --output-on-failure
```
`mock_admin` ctest target: full pass. `kafka_admin` ctest target: ONE
pre-existing unrelated failure (`test_kafka_admin_b3_empty_batches_need_no_broker`,
a log-dirs/offsets/reassignments test THIS phase never touches per
`git diff --stat -- bindings/c/tests/test_kafka_admin.c` being empty) —
confirmed via `lsof -iTCP -sTCP:LISTEN` that a stray `java` process was
listening on port 9093, exactly the scenario Phase A/B's memory already
documented three times running now. Do not chase it.

Found and fixed all 6 `*_async` C test callbacks (`on_describe_log_dirs`,
`on_alter_replica_log_dirs`, `on_describe_replica_log_dirs`, and their null-
handle counterparts) plus updated the two NULL-handle tests' inputs to
non-empty arrays (Phase A's "an empty request fans out over zero keys" rule)
BEFORE they could become a Critic finding — added one extra C test
(`test_mock_admin_describe_log_dirs_async_two_brokers`) exercising the
empty-per-key-value case (broker 7, no topics) end-to-end through the async
C entry point too, not just at the Rust unit-test level.

Python: rebuilt `_confluentkafka.cpython-313-darwin.so` against
`target/debug` (fast iteration; `python_bindings_local_build_test_macos.md`'s
mise-Python-3.13 recipe, unchanged from Phase A/B's session). `pytest`/
`pytest-asyncio` were ALREADY installed (persisted across sessions).
`test/unit/test_admin.py`: 190 passed (was 189 before this phase's edits;
net +1 after removing 2 orphaned direct-converter tests and adding 2 new
structural-independence tests — see below). Full `test/unit/`: 369 passed,
2 skipped — exactly matching Phase B's baseline count (the removed/added
tests netted to +0 there once `test_alter_replica_log_dirs_two_replicas_
resolve_independently` was added to compensate for the -1 from removing the
two dead `_to_describe_log_dirs`/`_to_describe_replica_log_dirs` tests —
see "orphaned converters" below). **If a future phase's count comes in
below the prior phase's baseline, check whether dead-code removal is the
cause before assuming a real regression** — it's legitimate as long as the
removed test's coverage is provably subsumed elsewhere (here: by the
end-to-end async tests using the new `_drain_log_dir_map`/
`_drain_replica_log_dir_info` converters, which — like `_drain_config`/
`_drain_deleted_records`/`_drain_description` in Phases A/B — are NOT
directly unit-tested in isolation, only exercised end-to-end through the
real FFI call; confirmed this is the established precedent by grepping for
direct `_drain_X` test usage before relying on it).

Rust: sanity-checked the temporal-independence test
(`describe_log_dirs_async_delivers_a_resolved_broker_without_waiting_on_a_pending_one`)
actually catches a reintroduced join by inserting a `block_on(KafkaFuture::
all_of(...).get())` before the per-key loop in `admin_async_per_key_op`,
confirming `timeout 20 cargo test ... ; echo REAL_EXIT=$?` gives `124`
(hangs), then reverting and confirming `REAL_EXIT=0`. Third phase running
this exact "inject the bug, watch it hang, revert" ritual — it has caught
nothing NEW each time, which is itself useful confirmation the mechanism is
solid, not evidence the ritual is skippable.

### Post-COMMENTS.67-fixup counts (final)

After the missing-key fan-out fix (see the corrected section above): `cargo
test --lib` 3920 passed (unchanged), `cargo test --lib --features ffi` 4136
passed (was 4133 pre-fixup, +3 for the two generic-mechanism tests plus the
end-to-end `describe_replica_log_dirs_async_resolves_an_unknown_topic_replica_
with_an_explicit_error`). `bindings/python/test/unit/` 369 passed, 2 skipped
(unchanged — one test renamed/rewritten in place, no net count change).
`mock_admin` ctest target full pass including the new
`test_mock_admin_describe_replica_log_dirs_async_unknown_topic_resolves_with_error`.
`kafka_admin` ctest target: same one pre-existing unrelated failure as before
the fixup (stray broker on port 9093).

## Orphaned converters: removed, not left dead

`_to_describe_log_dirs`/`_to_alter_replica_log_dirs`/`_to_describe_replica_log_dirs`
(the OLD whole-batch-dict converters, used only by the now-deleted joined
`_describe_log_dirs_spec`/`_alter_replica_log_dirs_spec`/
`_describe_replica_log_dirs_spec` helpers) became fully unreferenced by
production code once the RPCs moved to per-key `_drain_log_dir_map`/
`_drain_replica_log_dir_info`. Removed all three from `admin.py`, plus their
two direct unit tests in `test_admin.py` (no direct test existed for
`_to_alter_replica_log_dirs`) and the now-unused import line. The
corresponding C-level `_lib.DescribeLogDirsResult_drain`/
`AlterReplicaLogDirsResult_drain`/`DescribeReplicaLogDirsResult_drain`
functions were LEFT AS-IS in `_confluentkafka.c` (matching Phase B's
precedent for `DescribeConfigsResult_drain`/`AlterConfigsResult_drain`) —
they still back the unchanged SYNCHRONOUS blocking C entry points, which
Python's `admin.py` simply doesn't happen to call directly, so they are not
truly dead from the C ABI's perspective even though no current Python code
path reaches them.

## gRPC harness fix (same shape as Phase A/B)

`grpc_server.py` (sync) / `grpc_server_async.py` (async): the three
`DescribeLogDirs`/`AlterReplicaLogDirs`/`DescribeReplicaLogDirs` RPC handlers
called `client.describe_log_dirs(...)` etc. and fed the result straight to
`_admin_describe_log_dirs_response`/`_admin_void_response`/
`_admin_describe_replica_log_dirs_response`, which still expect an
ALREADY-RESOLVED `{key: value | KafkaError}` dict. Wrapped each call site with
`_resolve_admin_futures(futures)` (sync) / `await _resolve_admin_futures_async(futures)`
(async) — the exact same helper Phase A introduced, zero changes needed to it
or to the three `_admin_*_response` translators (their expected value shape,
`{log_dir: LogDirDescription}` / `None` / `ReplicaLogDirInfo`, is exactly what
`_drain_log_dir_map`/`_drain_replica_log_dir_info` produce via
`fut.result()`).

## Files touched

- `src/ffi/admin.rs`: 3 new `submit_*_entries` fns, 3 new callback typedefs
  (signature change on the existing name, per naming rule), 2 new destroy
  fns (`kafka_admin_LogDirDescriptionMap_destroy`,
  `kafka_admin_ReplicaLogDirInfo_destroy`), 3 rewritten `_async` fn bodies,
  5 new tests.
- `bindings/python/admin.py`: removed 3 orphaned `_to_*` converters + 3
  orphaned joined `_*_spec` methods; added 2 `_drain_*` converters, 3
  `_keyed_*_cb` closures, 3 `_*_keys_and_spec` builders, rewrote 6
  sync/async `Admin`/`AsyncAdmin` methods; updated the module docstring's
  per-key RPC inventory (seven -> ten).
- `bindings/python/_confluentkafka.c`: 3 rewritten trampolines, 3 updated
  `admin_incref_n` call sites, 2 new standalone drain functions
  (`LogDirDescriptionMap_drain`, `ReplicaLogDirInfo_drain`), method-table doc
  strings updated.
- `bindings/python/grpc_server.py` / `grpc_server_async.py`: 3 call sites
  each wrapped with `_resolve_admin_futures`/`_resolve_admin_futures_async`.
- `bindings/python/test/unit/test_admin.py`: rewrote 4 existing tests to the
  Future-based contract, removed 2 orphaned direct-converter tests, added 3
  new tests (2 structural-independence, kept count at Phase B's baseline).
- `bindings/c/tests/test_mock_admin.c`: 6 rewritten `*_async` tests +
  trampolines, 1 new test (empty-value two-broker case).
- `design/history/Milestone-11/PLAN-bindings.md`: Phase C §D2 addendum.
