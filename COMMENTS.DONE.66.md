# Critic review of Phase B (admin per-key async callbacks, Configs family) — RESOLVED

Commits reviewed: `94140b7b`, `cb61b15e`, `839a30a1`, `c6308234`, `40259a1f`, `aa68ea6f`, `3b132fb1`
on branch `feat/admin-per-key-callbacks-phase-b`.

Fix commit: `fixup! 94140b7b / cb61b15e: address COMMENTS.66 (empty-op-list resource hang in incremental_alter_configs)`.

## 1. [Bug — blocking, CLAUDE.md §5] `incremental_alter_configs`: a resource with an empty `AlterConfigOp` list gets a `Future` that never resolves — RESOLVED

Implemented fix option 1 from the review (the preferred, most faithful one):
made the wire representation carry zero-op resources so they get a real
per-key callback and resolve exactly like every other resource, matching
Java's actual behavior. The real Rust admin core
(`KafkaAdminClient::incremental_alter_configs` /
`submit_incremental_alter_configs` in `src/admin/kafka_admin_client.rs`)
already iterates `configs.keys()` and handles an empty `Vec<AlterConfigOp>`
correctly (an empty `alterable_configs` wire vec, still sent and still
completed) — the bug was entirely in the C-FFI row-flattening layer, which
had no way to represent "this resource exists in the map but has zero ops."

**`src/ffi/admin.rs` (`read_alter_config_ops`)**: changed the skip condition
from "NULL resource name OR NULL config name" to "NULL resource name" alone.
A row with a non-NULL resource name but a NULL config name is now a
**sentinel**: it calls `.entry(...).or_default()` to register the resource
in the output map with no op contributed, instead of being dropped entirely.
This is unambiguous because no real op row can have a NULL config name (Java
`AlterConfigOp`'s `ConfigEntry.name()` is never null), so there is no
existing legitimate use of "NULL config name" that this repurposes. Updated
the function's doc comment and the two public rustdocs
(`kafka_admin_AdminClient_incremental_alter_configs` /
`_async`) to describe the new row semantics. `distinct_config_resources`
needed no change — it already reads only the resource type/name arrays, so a
sentinel row is counted as a distinct resource exactly like a real op row.

**`bindings/python/admin.py` (`_incremental_alter_configs_keys_and_spec`)**:
now emits one explicit sentinel row `(type, name, None, None, -1)` for any
resource whose op list is empty, instead of contributing zero rows for it.
Docstring rewritten to describe the sentinel mechanism instead of documenting
the (now-fixed) gap.

**`bindings/python/_confluentkafka.c`
(`py_Admin_incremental_alter_configs_async`)**: `PyArg_ParseTuple` format
changed from `"isszi"` to `"iszzi"` so the `key` (config_name) position also
accepts `None`, needed to pass the sentinel row through from Python.
`count_distinct_config_resources` needed no change, for the same reason as
the Rust-side `distinct_config_resources`.

**Verified the real admin core and the mock both handle a zero-op resource
correctly** before implementing: `KafkaAdminClient::submit_incremental_alter_configs`
builds an empty `alterable_configs` Vec and still sends/completes the
resource; `MockAdminClient`'s `handle_incremental_resource_alteration` +
`apply_alter_ops` treat an empty `ops` slice as a no-op loop that still
returns `Ok(())` for an existing resource (mirroring Java, which validates
resource existence but not op-list length).

### Tests added (three layers, per the review's explicit ask)

- **Rust** (`src/ffi/admin.rs`): `read_alter_config_ops_registers_a_zero_op_resource_via_the_sentinel_row`
  (direct unit test of the marshaling fix — a sentinel row registers its
  resource with an empty `Vec`, while a row with a NULL *resource* name is
  still a full skip) and `incremental_alter_configs_async_resolves_a_zero_op_resource`
  (end-to-end through the actual public `kafka_admin_AdminClient_incremental_alter_configs_async`
  entry point against a real `MockAdminClient`, with a bounded
  `recv_timeout(5s)` so a reintroduced regression fails fast instead of
  hanging the test process).
- **Python** (`bindings/python/test/unit/test_admin.py`):
  `test_incremental_alter_configs_empty_op_list_resolves_not_hangs` (sync,
  including a mixed batch with one empty-ops resource and one real-ops
  resource, both must resolve independently) and
  `test_async_incremental_alter_configs_empty_op_list_resolves_not_hangs`
  (async counterpart, via `AsyncMockAdminClient` + `asyncio.wait_for`).
- **C** (`bindings/c/tests/test_mock_admin.c`):
  `test_mock_admin_incremental_alter_configs_empty_op_list_resource` (sync,
  flattened-result path) and
  `test_mock_admin_incremental_alter_configs_async_empty_op_list_resource`
  (async, per-key-callback path via the existing `on_alter_configs`
  trampoline and `wait_for`).

**Sanity-checked every new test actually catches the regression it targets**
(per this project's own precedent for temporal-independence tests): reverted
the Rust fix in `read_alter_config_ops` and reran the two new Rust tests —
the direct unit test failed on an assertion immediately, and the end-to-end
test failed with a `Timeout` after its bounded 5s wait (confirmed via
external `timeout N ...; echo REAL_EXIT=$?`, not piped through `tail`).
Reverted the Python fix in `_incremental_alter_configs_keys_and_spec` and
reran the two new Python tests — both failed with `TimeoutError` /
`asyncio.exceptions.CancelledError → TimeoutError` as expected. Restored both
fixes and reran everything clean before committing.

### Full verification after the fix

- `cargo build` / `cargo build --features ffi`: clean.
- `cargo test --lib`: 3920 passed (unchanged). `cargo test --lib --features
  ffi`: 4130 passed (was 4128 before this fixup, +2 new Rust tests).
- `cargo xtask format-check`: clean.
- `bindings/c/tests`: rebuilt `target/release` and reran `cmake --build` +
  `ctest`; `mock_admin` target passes in full (including the 2 new tests);
  `kafka_admin` target's one pre-existing, unrelated failure
  (`test_kafka_admin_b3_empty_batches_need_no_broker`, caused by a stray
  broker listening on the sandbox's 9092/9093, per the original Phase B
  report) is unaffected by this fixup.
- `bindings/python/test/unit`: 369 passed, 2 skipped (was 367/2 before this
  fixup, +2 new Python tests).
