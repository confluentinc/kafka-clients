# Critic review of Phase C (admin per-key async callbacks, Log dirs family) — RESOLVED

Commits reviewed: `5c2b3867`, `5a062931`, `166e5a1a`, `099b659a`, `57c254a5`,
`22e3fa4c`, `5adce8cd`, `172f1046` on branch `feat/admin-per-key-callbacks-phase-c`.

Fix commit: `fixup! 5c2b3867: address COMMENTS.67 (missing-key hang in admin_async_per_key_op)`.

## 1. [Bug — blocking, CLAUDE.md §5, same class as Phase B's COMMENTS.66] `describe_replica_log_dirs` promises a `Future` for a key it can never resolve — RESOLVED

**Rejected the "Java-faithful mock behavior" excuse** per the review: Java's
real `MockAdminClient.describeReplicaLogDirs` never promises a `Future` per
key in the first place — an unknown-topic key is simply absent from the
returned `Map`, detectable immediately by a Java caller. The hang was a
consequence purely of this port's own wrapping design (pre-building one
`Future`/`Promise` per input key before the async call returns), not
something inherited faithfully from Java. Fixed in the per-key delivery
contract itself, not by changing what the mock resolves to.

**Fix, generic, in `admin_async_per_key_op` (`src/ffi/admin.rs`)** — shared
by every per-key RPC across Phases A/B/C so far (and D–G to come):

- Added `K: PartialEq` to the function's bounds. Verified every existing `K`
  type across all 15 call sites (`String`, `TopicPartition`, `ConfigResource`,
  `i32`, `TopicPartitionReplica`) already satisfies it (all are `Eq + Hash`
  for their `HashMap` usage elsewhere) — the crate builds clean with zero
  call-site changes needed.
- After `submit` succeeds, any key present in `keys` but absent from
  `entries` — matched with a `claimed: Vec<bool>` mask over `entries`'
  positions so a **repeated** key in `keys` (e.g. `describe_log_dirs`'s raw,
  undeduped broker-id list) claims at most one real entry and any further
  occurrence is correctly reported missing rather than silently treated as
  satisfied — now gets `complete(key, Err(Error::local_illegal_state(...)),
  user_data)` fired synchronously, before the real entries are registered.
  This extends the existing total-submission-failure fan-out (which already
  did exactly this for *every* key on a null-admin or `submit`-`Err` failure)
  to also cover the partial-success gap where `submit` succeeds but returns
  fewer entries than requested keys.
- Confirmed via the full test suite that this does **not** change Phase A/B's
  observable behavior: none of their `entries` builders can currently produce
  fewer entries than `keys` (their admin-core implementations always create
  one future per requested/deduped key), so the new fan-out path is simply
  never reached for those RPCs — `cargo test --lib --features ffi` count
  before and after the fix is identical apart from the newly added tests.

**Tests added, three layers** (Rust, Python, C), mirroring Phase B's
COMMENTS.66 fixup precedent:

- Rust (`src/ffi/admin.rs`):
  `admin_async_per_key_op_reports_an_explicit_error_for_a_key_missing_from_entries`
  (generic mechanism, hand-built `KafkaFutureImpl`s, bounded `recv_timeout`),
  `admin_async_per_key_op_handles_a_repeated_key_where_only_one_occurrence_has_an_entry`
  (the `claimed`-mask edge case), and
  `describe_replica_log_dirs_async_resolves_an_unknown_topic_replica_with_an_explicit_error`
  (end-to-end through the real async entry point against a real
  `MockAdminClient` with both a known and an unknown-topic replica).
- Python (`bindings/python/test/unit/test_admin.py`): rewrote
  `test_describe_replica_log_dirs_omits_unknown_topics` →
  `test_describe_replica_log_dirs_resolves_unknown_topics_with_an_explicit_error`,
  asserting the unknown-topic replica's `Future` resolves with an explicit
  `KafkaError` within a bounded `timeout=5.0` (previously it only proved the
  pending state via `pytest.raises(concurrent.futures.TimeoutError)` with a
  short 0.2s wait — replaced entirely, not merely extended, since the old
  assertion is now false).
- C (`bindings/c/tests/test_mock_admin.c`):
  `test_mock_admin_describe_replica_log_dirs_async_unknown_topic_resolves_with_error`,
  same end-to-end shape as the Rust test, via the C ABI directly.

**Docs updated** to match: `bindings/python/admin.py`'s module docstring and
both `Admin.describe_replica_log_dirs`/`AsyncAdmin.describe_replica_log_dirs`
docstrings now state the explicit-error outcome instead of "never resolves" /
"stays pending forever"; `design/history/Milestone-11/PLAN-bindings.md`'s
Phase C §D2 addendum rewritten to describe the fix instead of the
now-superseded "documented mock limitation" framing.

**Verification after the fix**: `cargo build` / `--features ffi` clean;
`cargo test --lib` 3920 passed (unchanged), `cargo test --lib --features ffi`
4136 passed (+3 over the pre-fix 4133); `cargo xtask format-check` clean;
`bindings/c/tests` `mock_admin` ctest target full pass (including the two new
tests); `bindings/python/test/unit/` 369 passed, 2 skipped (unchanged count,
one test renamed in place).
