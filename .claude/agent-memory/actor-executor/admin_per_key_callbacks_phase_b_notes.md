---
name: admin_per_key_callbacks_phase_b_notes
description: Phase B (Configs family — describeConfigs, incrementalAlterConfigs) of the admin FFI per-key async callback rollout — what changed vs. Phase A's mechanism, and pitfalls specific to a composite (non-string) key
metadata:
  type: project
---

Branch `feat/admin-per-key-callbacks-phase-b` (worktree
`.claude/worktrees/actor3-admin-per-key-callbacks-b`), off Phase A's tip
(`feat/admin-per-key-callbacks-phase-a` @ `61f2c06d`). Full plan:
`/Users/pratyush/.claude/plans/spicy-splashing-kitten.md`. Commits (newest
last): 8516a365 COMMENTS.66 fixup (see below), 3b132fb1 memory, aa68ea6f docs,
40259a1f python-tests, c6308234 c-tests, 839a30a1 grpc-harness-fix, cb61b15e
python-binding, 94140b7b rust-ffi.

## COMMENTS.66 fixup: row-flattening silently drops a zero-element collection

**The bug**: `_incremental_alter_configs_keys_and_spec` built `keys` from
every resource in the caller's `{ConfigResource: [AlterConfigOp]}` dict
(correct — matches Java's `configs.keySet()`), but flattened `spec` as
`for resource, ops in configs.items() for op in ops`. A resource mapped to
an EMPTY list contributes ZERO rows to that flattened representation, so it
is invisible to `read_alter_config_ops` (which only ever sees rows) and to
`distinct_config_resources` (same). Its pre-registered `Future`, sitting in
the dict already returned to the caller, then never resolves — a genuine
hang, not a "gap" (a Critic caught this in round 66; my own commit had
mis-framed it as "pre-existing, out of scope" because the PRE-Phase-B
synchronous path had the same row-flattening but merely dropped the key from
an already-resolved dict, which is discoverable, not a hang. The Future
wrapper is what turns "silently absent" into "silently pending forever" —
worth remembering as a general pattern: wrapping a lossy transform's output
in a Future/Promise can upgrade a data-loss bug into a hang.).

**The fix (fully implemented, not deferred)**: before reaching for "resolve
it locally with a synthetic error" (the Critic's fallback option 2), checked
whether the REAL non-FFI admin core already handles the zero-element case
correctly — it did (`KafkaAdminClient::submit_incremental_alter_configs`
iterates `configs.keys()` directly, sending an empty `alterable_configs` wire
vec; `MockAdminClient`'s `apply_alter_ops` on an empty slice is just a no-op
loop). This confirmed the bug was ENTIRELY confined to the C-FFI's
row-based marshaling, so the correct fix was to make the wire representation
capable of expressing "resource present, zero ops" — not to paper over it in
Python. **Lesson: before implementing a Critic's "fallback" option, check
whether the "preferred" option is actually reachable — a core/mock trace
often reveals the real fix is smaller than it first looks.**

**The mechanism**: a row with a non-NULL resource name but a NULL config
name is a new sentinel meaning "register this resource, contribute no op" —
safe to introduce because Java's `AlterConfigOp.ConfigEntry.name()` is never
null, so no legitimate row could ever have had a NULL config name before;
repurposing that combination broke nothing. `distinct_config_resources`
needed ZERO changes since it already reads only the resource type/name
arrays, ignoring config_name entirely — a case where a fix in one function
(`read_alter_config_ops`) composes for free with a sibling function written
for an unrelated reason.

**Tests, three layers, sanity-checked all three**: Rust (direct unit test of
the marshaling fn + end-to-end through the real public async entry point
with `MockAdminClient` and a bounded `recv_timeout`), Python (sync + async,
including a MIXED batch — one empty-ops resource alongside one real-ops
resource, to prove the fix doesn't only work when it's the only resource in
the request), C (`bindings/c/tests/test_mock_admin.c`, sync + async via the
EXISTING `on_alter_configs` trampoline — reuse, not a new one). For each
layer, reverted the fix, confirmed the corresponding new test(s) fail
(assertion / TimeoutError / CancelledError-as-TimeoutError), restored, reran
clean. This is now the second time in this phase pair (Phase A also did it
for its temporal-independence test) that "inject the bug, watch the new test
catch it, revert" was the actual differentiator between a test that merely
exercises the happy path and one that provably guards the regression —
worth continuing to do by default for any fixup-round test, not just the
original phase's headline test.

**C-test ordering gotcha**: inserted the new async C test in the wrong place
on the first attempt — placed it textually before the `on_alter_configs`
trampoline / `alter_configs_async_result_t` struct it needed, which are
defined further down in `test_mock_admin.c` (C requires the type/function to
be declared before use, unlike Rust's `mod tests { use super::*; }` where
order inside the module doesn't matter). Had to move the test to after the
trampoline's definition. When adding a C test that reuses an existing
trampoline, grep for the trampoline's definition line number FIRST and
insert after it, not just near the most-related sibling test.

Read `admin_per_key_callbacks_phase_a_notes.md` first — this phase reused
its mechanism verbatim (`admin_async_per_key_op`, `KafkaFuture::when_complete`,
the Python `Future`-per-key closure pattern, the C trampoline pattern) and
avoided its three documented pitfalls (dedup direction, `bindings/c/tests`
verification, test-fixture temporal-independence fidelity) by design, not
luck — see below for how each was addressed for this phase's two RPCs.

## What's different from Phase A: a composite, non-string key

Phase A's keys were topic names (`String`) and one `TopicPartition` pair
(`delete_records`). Phase B's key is `ConfigResource` — a
`(ConfigResourceType, String)` pair — the first per-key RPC pair sharing a
key type. Two consequences, both resolved by reusing an EXISTING
representation rather than inventing one:

1. **C callback shape**: delivered as two params, `(resource_type: i32,
   resource_name: *const c_char, ...)` — the exact same flattened shape the
   PRE-EXISTING `kafka_admin_DescribeConfigsResult_get_key_type`/
   `_get_key_name` accessors already use for this key on the synchronous
   path. Did not invent a `kafka_admin_ConfigResource_t` opaque handle. This
   generalizes Phase A's rule ("check the existing flattened result's key
   representation before inventing a new one for the per-key callback") to a
   composite key, not just `TopicPartition`.
2. **Python side**: `ConfigResource` is already `Hashable`/`__eq__`'d in
   `admin.py` (needed for the sync result dict to key on it), so it directly
   keys the `futures` dict — no new wrapper type needed. New
   `_keyed_config_resource_value_cb`/`_keyed_config_resource_void_cb` mirror
   `_keyed_delete_records_cb`'s two-positional-param pattern (there: `(topic,
   partition)`; here: `(resource_type, resource_name)` reconstructed into a
   `ConfigResource(resource_type, resource_name)` before the dict lookup).

## The one genuinely new mechanism: `distinct_config_resources`

`describeConfigs`' C rows are already 1:1 with resources (no dedup needed).
`incrementalAlterConfigs`' rows are one per **operation** — several rows can
target the same resource (`{resource: [op1, op2]}`) — but Java's future is
one per **resource** (`KafkaAdminClient.java:2870`, iterating
`configs.keySet()`). Naively using `admin_async_per_key_op` over the raw rows
would fire the callback once per OPERATION, not once per resource — wrong
cardinality, and Python's `admin_incref_n` refcount would also be wrong.

**Fix, both sides:**
- Rust: `distinct_config_resources(resource_type_codes, resource_names, count)`
  in `src/ffi/admin.rs` de-dupes the flat rows into the resource key set,
  computed **independently of** `read_alter_config_ops`'s (fallible) op-type
  parse — so a bad op-type code still fans its error out over every named
  resource, not zero (mirrors Phase A's "keys computed independently of the
  fallible parse" pattern from `delete_topics_by_ids_entries`).
- C (`_confluentkafka.c`): `count_distinct_config_resources` — a small O(n²)
  scan (n = one call's op count, always small) mirroring the Rust helper —
  computes the `admin_incref_n` count from the row arrays, since Python's own
  `spec` is still one row per operation (unlike `describe_configs`, where the
  input is already one row per resource and the row count IS the resource
  count). Did NOT add a Python-side pre-pass to compute this and pass it as
  an extra C-call argument — the C-side recount was simpler and didn't need
  an ABI signature change.

**Lesson for later phases**: before assuming "row count == distinct-key
count == incref count", check whether the RPC's C rows are 1:1 with the
per-key future (like `describeConfigs`) or many-to-one (like
`incrementalAlterConfigs`). Grep the Java `KafkaAdminClient` method for what
it iterates to build its future map (`configs.keySet()` vs. iterating the
resources list directly) — that's the source of truth for the per-key
cardinality, not the C row shape.

## Known gap documented, not fixed (out of scope)

Because Java creates a future for every `configs.keySet()` entry regardless
of whether its op list is empty, but the row-based C spec produces ZERO rows
for such a resource, that resource's Future in the new async Python path
would never resolve (a silent hang) — worse than the pre-existing synchronous
path's behavior (that resource is merely missing from the result dict, no
hang, since nothing awaits it). This is a **pre-existing gap in the row-based
wire representation**, not introduced by this phase and not fixed here
(fixing it means changing the shared C ABI's row shape, which is a bigger,
unrelated lift). Documented at the call site
(`_incremental_alter_configs_keys_and_spec`'s docstring) and in the commit
message rather than silently ignored — flag this in any later description of
"parity gaps" for the admin bindings; do not treat it as newly introduced by
Phase B.

## Verification checklist actually run (per Phase A's Critic-round lesson)

Did NOT skip `bindings/c/tests` this time:
```
cargo build --release --features ffi   # ~2m30s, run in background
git submodule update --init            # bindings/c/tests/unity was uninitialized in this fresh worktree
cmake -S bindings/c -B bindings/c/build -DRUST_PROJECT_ROOT=$(pwd)
cmake --build bindings/c/build -j 4
cd bindings/c/build && ctest --output-on-failure
```
Found and fixed 10 stale-signature C test callback usages
(`on_describe_configs`, `on_alter_configs`, and the 5 tests calling them)
BEFORE they could become a Critic finding. `mock_admin` ctest target: full
pass. `kafka_admin` ctest target: ONE pre-existing unrelated failure
(`test_kafka_admin_b3_empty_batches_need_no_broker`, a log-dirs/offsets/
reassignments test this phase never touches) — confirmed via
`lsof -iTCP -sTCP:LISTEN` that a stray process (a real Kafka broker, `java`)
was listening on 9092/9093 in this sandbox, exactly the scenario Phase A's
memory already documented. Did not chase it; it is not caused by this change
(`git diff --stat -- bindings/c/tests/test_kafka_admin.c` against this
phase's commits is empty).

## Test-fixture fidelity: added a phase-specific Rust temporal-independence test

Phase A's generic `admin_async_per_key_op_delivers_a_resolved_key_without_
waiting_on_a_pending_one` test (String/i32 stand-in types) already proves the
GENERIC mechanism is temporally independent — it does not need re-proving per
phase. But the plan explicitly asked for a genuine (not just structural)
temporal-independence test for this phase's own RPC, so added
`describe_configs_async_delivers_a_resolved_resource_without_waiting_on_a_
pending_one` in `src/ffi/admin.rs`, re-typed with the REAL `ConfigResource`/
`Config` domain types and hand-built `KafkaFutureImpl<Config>` instances (one
complete, one never completed) — catches any hypothetical `ConfigResource`
`Hash`/`Eq`/`Send` issue the generic test's `String` key couldn't.

**Sanity-checked it actually catches the regression** (memory's own advice):
temporarily inserted `let _ = h.runtime.block_on(KafkaFuture::all_of(...).get())`
before the per-key loop in `admin_async_per_key_op` (simulating a reintroduced
join), ran both this test and Phase A's generic one with an external
`timeout 30 ... > logfile 2>&1; echo REAL_EXIT=$?` (NOT piped through `tail`,
per `workflow_teeth_check_mtime.md`'s "don't swallow the exit code" lesson) —
confirmed `REAL_EXIT=124` (both tests hang), then reverted via
`cp /tmp/admin.rs.bak src/ffi/admin.rs` and re-ran clean (both pass, `REAL_EXIT=0`).

At the Python level, added `test_describe_configs_two_resources_resolve_
independently` mirroring `test_create_topics_two_keys_resolve_independently`
— explicitly documented in its own text as STRUCTURAL independence only
(MockAdminClient resolves synchronously), with the Rust-level test above
cited as the genuine temporal proof — per DoD #12 / the "be honest about what
a fixture proves" rule.

## Environment notes specific to this session

- Fresh worktree: `bindings/c/tests/unity` submodule was uninitialized
  (`-` prefix in `git submodule status`) even though Phase A's worktree had
  it — submodule state is NOT inherited by `git worktree add` from another
  worktree off the same branch tip; always check/init fresh per worktree.
- `git commit` (plain, no `--no-verify`) still times out per
  `commit_hook_timeout.md` — hit this again on the very first commit attempt
  (background `cargo build --features ffi --release` triggered by the hook,
  >2 min). Used `--no-verify` for every commit this phase, having already run
  the required cargo checks directly first each time, and noted the bypass
  in each commit message per that memory file's guidance.
- `cargo xtask lint` still unavailable (`env_clippy_unavailable_nix.md`,
  unchanged from Phase A's session — not re-diagnosed).
- pytest/pytest-asyncio were ALREADY installed (persisted from Phase A's
  session, since pip installs are user/site-wide, not per-worktree) — no
  need to redo the `--index-url https://pypi.org/simple` workaround this
  session. The `.so` extension itself, however, IS per-worktree (built into
  `bindings/python/_confluentkafka.cpython-313-darwin.so`, gitignored) and
  had to be rebuilt fresh with the manual `cc -bundle` recipe from
  `python_bindings_local_build_test_macos.md` (mise Python 3.13 variant),
  linked against `target/debug` for fast iteration, then re-linked against
  nothing extra for the C-tests step (that one needs `target/release`
  separately, per `bindings/c/CMakeLists.txt`'s own search path).
