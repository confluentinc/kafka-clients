---
name: admin_per_key_callbacks_phase_d_notes
description: Phase D (Partitions/offsets family — alterPartitionReassignments, listOffsets) of the admin FFI per-key async callback rollout — cheapest phase so far, plus the AdminApiDriver temporal-independence caveat
metadata:
  type: project
---

Branch `feat/admin-per-key-callbacks-phase-d` (worktree
`.claude/worktrees/actor5-admin-per-key-callbacks-d`), off Phase C's tip
(`feat/admin-per-key-callbacks-phase-c` @ `020227ef`). Full plan:
`/Users/pratyush/.claude/plans/spicy-splashing-kitten.md`. Read
`admin_per_key_callbacks_phase_a_notes.md`, `_phase_b_notes.md`, and
`_phase_c_notes.md` first — this phase reused the mechanism verbatim
(`admin_async_per_key_op`, `KafkaFuture::when_complete`, the Python
`Future`-per-key closure pattern, the C trampoline pattern) and hit none of
the three prior phases' hazards (dedup direction, mock-key-omission, C-tests
verification gap) because both RPCs' inputs are already `{(topic, partition):
...}` dicts (unique by construction) and both mocks resolve every requested
key.

## Scope

Two RPCs, both keyed by `TopicPartition`: `alterPartitionReassignments`
(`KafkaFuture<Void>`, delivered as `(topic, partition, error)` — same shape
as Phase C's `alter_replica_log_dirs` minus the broker-id field) and
`listOffsets` (`KafkaFuture<ListOffsetsResultInfo>`, delivered as `(topic,
partition, value, error)`, reusing the pre-existing `ListOffsetsResultInfoInner`
backing struct verbatim with a new `kafka_admin_ListOffsetsResultInfo_destroy`
for the dual-provenance split).

## The cheapest phase so far

Unlike every prior phase, NEITHER RPC needed a dedup helper on the Python
side: `alter_partition_reassignments`'s input is `{(topic, partition):
NewPartitionReassignment | None}` and `list_offsets`'s is `{(topic,
partition): OffsetSpec}` — both already dicts, unique by construction, same
as Phase C's `_alter_replica_log_dirs_keys_and_spec`. So
`_alter_partition_reassignments_keys_and_spec`/`_list_offsets_keys_and_spec`
are just `keys = list(d.keys())` plus the existing (unchanged) spec-row
logic — no `dict.fromkeys`, no distinct-resource counting like Phase B's
`incrementalAlterConfigs`. On the C side, `admin_incref_n(cb, n)` needed no
recount helper either — `n` (the row count) already equals the per-key
callback count for both RPCs, for the same reason.

On the Rust side, `read_topic_partitions` (topic+partition pair, skip NULL
topic) already existed (used by `read_optional_partition_set`), so `keys` for
both async entry points is one line: `unsafe { read_topic_partitions(topics,
partitions, count) }`, computed BEFORE the fallible parse
(`read_reassignments`/`read_offset_specs`), mirroring Phase A's
`delete_topics_by_ids_entries` "keys independent of the fallible parse"
pattern — a marshaling failure (empty replica list, bad offset sentinel, bad
isolation level) still fans an explicit error out to every requested key
rather than leaving any without a callback.

## The one caveat worth recording: `listOffsets` and `AdminApiDriver`

`admin-client.md` §2 flags `listOffsets` as the first RPC using
`AdminApiDriver`/`PartitionLeaderStrategy` (per-partition-leader lookup)
rather than a plain `Call`. Checked `KafkaAdminClient::list_offsets`
(`src/admin/kafka_admin_client.rs:4038`) — confirmed: it builds a
`ListOffsetsHandler` + `AdminApiDriver` and drives it via `invoke_driver`.
Practically this means on a REAL broker, partitions sharing a leader are
fetched by one wire request and tend to resolve together — genuine (not
merely structural) temporal independence between two such partitions is
harder to demonstrate end-to-end than for a `Call`-based RPC. This does NOT
weaken the per-key CONTRACT (`Admin::list_offsets` still returns one
`KafkaFuture` per partition in a `HashMap`, and `admin_async_per_key_op`
still registers one independent `when_complete` per entry) — it only affects
what an end-to-end integration test could prove. Noted in the Rust doc
comment on `submit_list_offsets_entries`, the PLAN-bindings.md Phase D
addendum, and the two temporal-independence tests' doc comments.

**This did not block the required temporal-independence test.** Per Phase
A's precedent, the direct `admin_async_per_key_op`-level test (hand-built
`KafkaFutureImpl`, one resolved one pending, bypassing `&dyn Admin` entirely
since `submit`'s closure ignores its `admin` parameter) is unaffected by the
driver architecture — it never goes through a real leader lookup. Added one
for EACH RPC this phase (`alter_partition_reassignments_async_delivers_a_
resolved_partition_without_waiting_on_a_pending_one` /
`list_offsets_async_delivers_a_resolved_partition_without_waiting_on_a_
pending_one`), using the RPCs' real `TopicPartition`/`ListOffsetsResultInfo`
types (not generic `String`/`i32` stand-ins), following Phase B's precedent
of exercising the mechanism against the actual key/value types at least once
per phase. Both were sanity-checked by injecting a
`block_on(f.clone().get())`-per-entry loop before the per-key dispatch inside
`admin_async_per_key_op` (simulating a reintroduced join) and confirming
`timeout 30 cargo test ...` gives exit 124 for both, then reverting and
confirming both pass — fourth phase running this ritual, still catching
nothing new (continued confirmation the mechanism itself is solid).

MockAdminClient resolves every partition's future synchronously and
independently in a per-partition loop for BOTH RPCs (no batch join even on
the mock side), so the Python/C end-to-end tests only prove structural
independence — documented as such in every such test's docstring, per
established precedent.

## Verification actually run

```
cargo build --features ffi                      # debug, fast iteration
cargo test --lib --features ffi                  # 4141 passed (was 4136 baseline, +5 new tests)
cargo test --lib                                 # 3920 passed (unchanged, ffi-gated tests not counted)
cargo xtask format-check                         # clean after `cargo xtask format`
cargo xtask lint                                 # unavailable in this sandbox (env_clippy_unavailable_nix.md), not re-diagnosed
git submodule update --init bindings/c/tests/unity   # was uninitialized in this fresh worktree
cargo build --features ffi --release             # bindings/c looks in target/release first; ran in background, ~2min
cmake -S bindings/c -B bindings/c/build -DRUST_PROJECT_ROOT=$(pwd)
cmake --build bindings/c/build -j 4
cd bindings/c/build && ctest -R mock_admin --output-on-failure   # full pass
ctest -R kafka_admin --output-on-failure                          # 1 pre-existing unrelated failure (see below)
```
`kafka_admin`'s one failure (`test_kafka_admin_b3_empty_batches_need_no_broker`)
is the same stray-broker-on-port-9093/9092 flake Phases A/B/C already
documented three times running — confirmed `git diff --stat -- bindings/c/tests/
test_kafka_admin.c` is empty for this phase (file untouched), so it cannot be
this phase's regression. Did not chase it, per established precedent.

Python: rebuilt `_confluentkafka.cpython-313-darwin.so` against `target/debug`
(fast iteration), mise-managed Python 3.13.14, exact `cc -bundle` recipe from
`python_bindings_local_build_test_macos.md`. `pytest`/`pytest-asyncio` were
ALREADY installed (persisted across sessions, confirmed again this session —
third phase running now confirming this). `test/unit/test_admin.py`: 191
passed (was 190; net +1 after removing 1 orphaned direct-converter test
[`test_to_list_offsets_maps_both_arms`] and adding 2 new structural-
independence tests). Full `test/unit/`: 370 passed, 2 skipped (was 369/2 —
Phase C's baseline), matching the expected net +1.

## Orphaned converter removed, not left dead

`_to_alter_partition_reassignments`/`_to_list_offsets` (the OLD whole-batch
converters, used only by the now-deleted joined `_alter_partition_
reassignments_spec`/`_list_offsets_spec` helpers) became fully unreferenced
once both RPCs moved to per-key `_keyed_topic_partition_void_cb`/
`_keyed_topic_partition_value_cb` + `_drain_list_offsets_info`. Removed both
converters from `admin.py`. `_to_alter_partition_reassignments` had no direct
unit test (same as Phase C's `_to_alter_replica_log_dirs`); `_to_list_offsets`
did (`test_to_list_offsets_maps_both_arms`, covering the populated-
leader-epoch arm the mock never reaches) — removed it with a documented
reason: that arm is still covered at the Rust level by the pre-existing
`list_offsets_result_carries_value_and_error_per_partition` test, which
exercises the identical `ListOffsetsResultInfoInner` scalar-accessor logic
(`kafka_admin_ListOffsetsResultInfo_offset/timestamp/leader_epoch`) the new
per-key `ListOffsetsResultInfo_drain` path also uses — same "was already
unreachable end-to-end, coverage moves not disappears" reasoning as Phase A's
"orphaned converters" section and Phase C's.

The corresponding C-level `_lib.AlterPartitionReassignmentsResult_drain`/
`ListOffsetsResult_drain` functions were LEFT AS-IS in `_confluentkafka.c`
(same precedent as every prior phase) — they still back the unchanged
SYNCHRONOUS blocking C entry points. `ListOffsetsResult_drain`'s value-tuple
logic was refactored to share a new `list_offsets_info_value_to_py` helper
with the new standalone `ListOffsetsResultInfo_drain`, rather than
duplicating the `Py_BuildValue`/leader-epoch-optional logic — the first time
this phase pairing shared C-level logic between the flattened and per-key
drain paths (Phase C's log-dirs family reused the *handle type*, not helper
*functions*, since `LogDirDescriptionMap_drain`/`ReplicaLogDirInfo_drain`
had no natural sub-helper to extract).

## gRPC harness fix (same shape as Phases A-C) + a stale-docstring fix along the way

`grpc_server.py` (sync) / `grpc_server_async.py` (async): the
`AlterPartitionReassignments`/`ListOffsets` RPC handlers called
`client.alter_partition_reassignments(...)`/`client.list_offsets(...)` and
fed the result straight to `_admin_void_response`/`_admin_list_offsets_response`,
which still expect an ALREADY-RESOLVED dict. Wrapped each call site with
`_resolve_admin_futures(futures)` (sync) /
`await _resolve_admin_futures_async(futures)` (async) — the exact same
Phase-A helper, zero changes needed to it or to the two `_admin_*_response`
translators.

Noticed in passing: `_resolve_admin_futures`'s own docstring
(`grpc_translate.py`) still only listed Phase A/B's RPCs — Phase C's commit
touched `grpc_server.py`/`grpc_server_async.py` but never updated this shared
docstring, so it had already drifted stale by one phase before Phase D even
started. Fixed it to list Phases C and D together while touching the file
anyway. **Lesson for phase E onward**: check `grpc_translate.py`'s
`_resolve_admin_futures` docstring for staleness even if your phase's own
`grpc_server.py` edits look complete — it is a shared helper multiple phases'
commits touch tangentially and it's easy for exactly one phase's entry to
get skipped.

## Files touched (7 commits, same granularity as Phase C)

- `src/ffi/admin.rs`: 2 new `submit_*_entries` fns, 2 callback typedef
  signature changes (existing names, per naming rule), 1 new destroy fn
  (`kafka_admin_ListOffsetsResultInfo_destroy`), 2 rewritten `_async` fn
  bodies, 6 new tests (1 destroy-null, 2 direct temporal-independence, 2
  end-to-end structural, all in the "Phase D" test section at the end of the
  module).
- `bindings/python/admin.py`: removed 2 orphaned `_to_*` converters + 2
  orphaned joined `_*_spec` methods; added 1 `_drain_*` converter, 2
  `_keyed_topic_partition_*_cb` closures (generalizing
  `_keyed_delete_records_cb`'s two-param key handling to take a `convert`
  param, like Phase B's `_keyed_config_resource_*_cb`), 2
  `_*_keys_and_spec` builders (no dedup needed — see above), rewrote 4
  sync/async `Admin`/`AsyncAdmin` methods; updated the module docstring's
  per-key RPC inventory (ten -> twelve).
- `bindings/python/_confluentkafka.c`: 2 rewritten trampolines, 2 updated
  `admin_incref_n` call sites (no recount helper needed), 1 new standalone
  drain function (`ListOffsetsResultInfo_drain`) sharing a new
  `list_offsets_info_value_to_py` helper with the existing
  `ListOffsetsResult_drain`, method-table doc strings updated.
- `bindings/python/grpc_server.py` / `grpc_server_async.py`: 2 call sites
  each wrapped with `_resolve_admin_futures`/`_resolve_admin_futures_async`;
  comment blocks updated.
- `bindings/python/grpc_translate.py`: `_resolve_admin_futures`'s docstring
  fixed to include Phases C and D (see stale-docstring note above).
- `bindings/python/test/unit/test_admin.py`: rewrote 6 existing tests to the
  Future-based contract, removed 1 orphaned direct-converter test, added 2
  new tests (structural-independence, net +1 vs Phase C's baseline).
- `bindings/c/tests/test_mock_admin.c`: 2 rewritten result-struct/trampoline
  pairs, 6 rewritten tests (3 per RPC: happy-path, submission-failure,
  null-handle with non-empty input), 2 new two-partition structural tests.
- `design/history/Milestone-11/PLAN-bindings.md`: Phase D §D2 addendum.
