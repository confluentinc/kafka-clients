# Critic 7 — Phase F (ACLs/quotas/features per-key callbacks) review

Commits reviewed: `0a4d998f`..`4280d7f0` (5 commits) on
`feat/admin-per-key-callbacks-phase-f`, off Phase E's tip `224271b7`.

## Verified correct (settled ground — do not re-litigate)

- **Refcount fix (item 1).** Read all 5 `py_Admin_*_async` wrappers
  (`create_acls`/`delete_acls`/`alter_client_quotas`/`alter_user_scram_credentials`/
  `update_features`). Each now calls `admin_incref_n(cb, n)` with `n` = the raw
  row count passed to the C array (not the possibly-smaller distinct-key count),
  which is exactly how many times `Py_DECREF(cb)` fires — traced through
  `admin_async_per_key_op` (`src/ffi/admin.rs:929`): every `keys` occurrence gets
  exactly one `complete()` call, whether matched (later, via `entries`) or
  unmatched (immediately, synthetic error). Ran a fresh multi-key stress test
  (5 keys, all 5 RPCs, 5 repeated iterations, via `MockAdminClient`) — no crash,
  no leak-detectable failure. Grepped every other `Py_INCREF(cb)` (singular) site
  in `_confluentkafka.c` (23 of them, all pre-Phase-F) and cross-checked each
  against its Rust doc comment ("the callback fires exactly once") — all are
  genuinely single-future RPCs (`describe_producers`, `list_transactions`, etc.),
  not per-key fan-outs. No other latent instance of this bug class found.
- **`AclBindingKey` (item 2).** `type AclBindingKey = (i32, String, i32, String,
  String, i32, i32)` at `src/ffi/admin.rs:14299` is a private (no `pub`) type
  alias local to this file's marshaling layer, never exposed across the FFI
  boundary (the real `kafka_common_AclBinding_t`/`AclBindingInner` is what
  crosses to C). Justification — `AclBinding`'s constructor validates and
  rejects malformed rows, so a rejected row still needs *an* identity for
  `admin_async_per_key_op`'s total-fan-out guarantee — holds up and doesn't
  duplicate logic that could live on `AclBinding` itself (a rejected row, by
  definition, cannot become one).
- **`kafka_admin_DeleteAclsFilterResults_t` (item 3).** Confirmed via
  `git diff feat/admin-per-key-callbacks-phase-e..feat/admin-per-key-callbacks-phase-f`
  that this handle is genuinely new to Phase F. It follows the established
  owned-handle idiom exactly (`Box::into_raw`/`Box::from_raw` pair, `_count`/
  `_get_binding`/`_get_error`/`_destroy`), and is the correct mint per
  `admin-client.md` §7 (`FilterResults` wraps `List<FilterResult>`, a genuine
  nested collection, not a flat scalar tuple).
- **Key-as-owned-handle lifecycle (item 4).** Each per-key callback firing
  allocates a *fresh* `Box::new(...)` for its key (`create_acls`'s trampoline
  closure builds a new `AclBindingInner` from the raw key tuple on every call),
  so there is no aliasing between firings. The dual borrowed/owned convention
  for the same opaque C type (`kafka_common_AclBinding_t` is borrowed from
  `*Result_get_binding` accessors but owned when delivered via this callback)
  is not new risk introduced by Phase F — it's the pre-existing idiom already
  used for `kafka_common_Error_t` throughout every RPC in this file, just
  applied to a new type, and it's explicitly documented on
  `kafka_common_AclBinding_destroy`'s doc comment ("do NOT call this on a value
  returned by `*_get_binding`").
- **`alter_client_quotas`'s undeduped async spec (item 5).** Traced the failure
  path: `read_client_quota_alterations` rejects a genuine duplicate entity with
  `Err` *before* `submit_alter_client_quotas_entries` runs, so the outer
  `submit()` closure fails outright. Per `admin_async_per_key_op`'s "whole
  submit failed" branch, **every** key in `keys` (all rows, including the
  duplicates) gets the *same* synthetic error — there is no per-key mismatch,
  no spurious "missing" misfire, and no silent accept of a duplicate Java would
  reject. This validation predates Phase F (`git log -S`, introduced in B5a);
  Phase F only wired the existing validated path into the new per-key
  mechanism, and it composes safely.
- **`update_features` zero-key doc (item 6).** The doc comment on
  `kafka_admin_AdminClient_update_features_async` explicitly states "When
  `count` is 0, there is no key at all — the callback is **never invoked**...
  Use the synchronous entry point to observe that outcome" — correctly avoiding
  Phase E's doc-accuracy mistake.
- **Java key-shape spot check (item 7).** Read `KafkaAdminClient.java` directly
  for `createAcls` (2606), `deleteAcls` (2666), `alterClientQuotas` (4314), and
  `alterUserScramCredentials` (4391). `createAcls`/`deleteAcls` are guarded
  (`if (futures.get(x) == null)` / `futures.get(filter) == null`) —
  first-occurrence-wins, matched by the Rust core's `Entry::Vacant` check at
  `src/admin/kafka_admin_client.rs:4405`. See the blocker below for
  `alterUserScramCredentials`, which is **not** first-wins.
- **370→369 test count (item 8).** Diffed `test_admin.py`'s `def test_` name
  sets between Phase E and Phase F: 2 added, 3 removed (net −1). The one
  genuinely-net-removed test, `test_to_create_acls_maps_none_to_success`,
  tested a converter function (`_to_create_acls`) that Phase F deleted
  entirely (the whole-batch joined-callback drain no longer exists for this
  RPC). `test_create_acls_reports_unsupported_per_binding` and
  `test_create_acls_round_trips_every_field_through_the_key` (both pre-existing
  or added) cover the surviving per-key path. Not a coverage regression.
- **Scope discipline (item 11).** `git diff --stat` shows only
  `bindings/c/tests/test_mock_admin.c`, `bindings/python/_confluentkafka.c`,
  `bindings/python/admin.py`, `bindings/python/grpc_server*.py`,
  `bindings/python/test/unit/test_admin.py`, `cbindgen.toml`,
  `design/history/Milestone-11/PLAN-bindings.md`, `src/ffi/admin.rs`. All
  in-family.
- **Build/test matrix (item 12).** All run directly, not taken on faith:
  - `cargo build`, `cargo build --features ffi`: clean.
  - `cargo test --lib --features ffi`: **4160 passed, 0 failed, 3 ignored.**
  - `cargo xtask format-check`: clean.
  - `cargo xtask check-bindings`: clean (68 `Py_BuildValue`, 33
    `PyObject_CallFunction`, 239 `PyArg_Parse*` sites, no arity mismatches).
  - C tests: built `test_mock_admin` via CMake against a debug `--features ffi`
    build and ran it directly: **181 Tests 0 Failures 0 Ignored, OK.**
  - Python: rebuilt the extension from source (`pip install --no-build-isolation
    -e .` against a release `--features ffi` build) and ran
    `pytest test/unit -q`: **369 passed, 2 skipped.**
  - `cargo xtask lint` (clippy) is not runnable in this sandbox (`cargo-clippy`
    not installed) — an environment limitation, not something to hold against
    the Actor; not otherwise checked.

## Blocker

### Issue: `alter_user_scram_credentials`'s duplicate-user doc claim is false — the real result is silently replaced by a synthetic error
- **File**: `src/ffi/admin.rs` (doc comment ~17771, `kafka_admin_AdminClient_alter_user_scram_credentials_async`); `bindings/python/admin.py:3254` (`_alter_user_scram_credentials_keys_and_spec`)
- **Severity**: Bug (Behavior Mismatch) — silently wrong data delivered to the caller, not merely a hang or a crash
- **Java Reference**: `KafkaAdminClient.java:4391-4397` (`alterUserScramCredentials`)
- **Description**:

  The Rust doc comment explicitly claims:

  > Two rows naming the same user (a `SCRAM_SHA_256` deletion plus a
  > `SCRAM_SHA_512` upsertion, say) are **passed through**, mirroring Java: both
  > reach the broker, and `KafkaAdminClient` keys one future per user ... so the
  > two rows collapse to one outcome row here as well.

  This is false. I reproduced it live (MockAdminClient, no mocking of the
  mechanism):

  ```python
  with MockAdminClient(1) as admin:
      futures = admin.alter_user_scram_credentials([
          UserScramCredentialUpsertion(
              "alice", ScramCredentialInfo(ScramMechanism.SCRAM_SHA_256, 4096), b"pw"),
          UserScramCredentialDeletion("alice", ScramMechanism.SCRAM_SHA_512),
      ])
      # futures == {"alice": <Future>}   (deduped, as _alter_user_scram_credentials_keys_and_spec intends)
      futures["alice"].result(timeout=5)
      # -> producer.KafkaError: the requested key was not present in the admin RPC's response
  ```

  The real per-user outcome (for MockAdminClient, `"Not implemented yet"` — see
  the passing single-user test `test_alter_user_scram_credentials_reports_
  unsupported_per_user`) never reaches the caller. Instead the caller always
  sees the *synthetic* "key not present" error that
  `admin_async_per_key_op`'s fallback manufactures for an unmatched key
  occurrence.

  **Root cause, traced through the code:**
  - `read_scram_user_keys` (line 17468) builds `keys` from the **raw, undeduped**
    row list — 2 entries, `["alice", "alice"]`, for the example above.
  - `submit_alter_user_scram_credentials_entries` (line 17482) returns `entries`
    from `admin.alter_user_scram_credentials(...).values()` — the Rust core
    mirrors Java's unconditional `futures.put(user, ...)`
    (`kafka_admin_client.rs:4548`, confirmed unconditional `HashMap::insert`,
    i.e. **last-wins**, not a `Entry::Vacant` guard) — so `entries` has exactly
    **one** "alice" row.
  - `admin_async_per_key_op` (line 929) processes `keys` **before** it registers
    any `when_complete` callback on `entries`: the first "alice" occurrence
    claims the one entry (no immediate callback); the second finds no unclaimed
    entry and calls `complete()` **synchronously, inline, right there** with
    `Error::local_illegal_state("the requested key was not present in the
    admin RPC's response")` (line 986-992). Only *after* this whole loop
    finishes does the function register `when_complete` on the real "alice"
    future (line 1000-1010), which resolves later, asynchronously.
  - So the ordering is deterministic, not racy: the synthetic error **always**
    fires first, the real result **always** arrives second.
  - `bindings/python/admin.py:3262`'s `_alter_user_scram_credentials_keys_and_spec`
    dedupes the **futures dict** key list (`dict.fromkeys(...)`) but passes the
    **raw, undeduped** row list as `rows`/`spec` — so both "alice" callback
    firings resolve `futures["alice"]`, the *same* Python `Future`.
  - `_resolve_keyed_void` (admin.py:2440) is idempotent-first-write-wins
    (`if fut.cancelled() or fut.done(): return`), so the first (synthetic,
    wrong) resolution wins and the second (real) one is silently dropped.

  Contrast with `create_acls`/`delete_acls`, where the actor got this right:
  `_create_acls_keys_and_spec`/`_delete_acls_keys_and_spec` (admin.py:3105,
  3126) dedupe **both** the futures-dict keys **and** the row list sent to C
  from the *same* `deduped` collection, so no duplicate row ever reaches
  `admin_async_per_key_op` in the first place — the commit message itself
  explains this was done "to avoid the raw per-key fan-out's generic 'not
  present in the response' fallback for a repeated key." That reasoning was
  applied correctly to 2 of the 3 RPCs whose Rust core dedupes by key
  (`alter_client_quotas`'s case is different and safe — see the verified list
  above), but not to `alter_user_scram_credentials`, where rows legitimately
  cannot be deduped (two rows for one user carry different mechanisms/data) yet
  the futures dict still is.
- **Expected**: A caller submitting 2+ rows for the same user (a legitimate,
  documented use case — the doc comment's own example) should see the real
  per-user outcome, exactly as Java delivers it through its one collapsed
  `KafkaFutureImpl`.
- **Actual**: The caller always sees a bogus
  `"the requested key was not present in the admin RPC's response"` error for
  that user, regardless of what the real operation actually did.
- **Suggested direction** (not prescriptive): either (a) have
  `admin_async_per_key_op` (or a scram-specific variant) suppress/ignore the
  synthetic "not present" firing for occurrences beyond the first *when the
  caller is known to collapse duplicates onto one target* — risky to generalize
  since other callers rely on the current "every occurrence gets exactly one
  firing" contract — or (b), simpler and more local: don't dedupe the futures
  dict in `_alter_user_scram_credentials_keys_and_spec`; instead deliver one
  Python `Future` per **row** (matching `keys` 1:1, as `describe_log_dirs`'s
  sibling `_describe_replica_log_dirs_keys_and_spec` pattern does for cases
    that can't pre-dedupe), and let the caller await all of them for a given
  user (accepting that one will carry the synthetic error and the other the
  real result, same as any other legitimately-repeated key today). Either way,
  the current combination (dedupe the sink, don't dedupe the source) is wrong.
- **Test gap**: No test anywhere (Rust unit, C, or Python) exercises 2+ rows
  naming the same user for `alter_user_scram_credentials`/
  `alter_user_scram_credentials_async` — every existing test
  (`test_mock_admin_alter_user_scram_credentials_reports_unsupported_per_user`,
  `test_alter_user_scram_credentials_reports_unsupported_per_user`, etc.) uses
  3 distinct users. This is exactly the scenario the doc comment calls out by
  name, so it should have its own test and does not.

## Note (not a blocker, but worth recording)

### Issue: Task/PLAN framing of "first-occurrence-wins" dedup direction is itself wrong for `alterUserScramCredentials`
- **Severity**: Design Flaw / documentation gap (in the review brief and,
  implicitly, in how Phase F's own commit message describes the RPC family)
- **Java Reference**: `KafkaAdminClient.java:4396`
  (`futures.put(alteration.user(), new KafkaFutureImpl<>())`, unconditional)
  vs. `KafkaAdminClient.java:2612`/`2672`
  (`if (futures.get(acl) == null)` / `if (futures.get(filter) == null)`)
- **Description**: `alterUserScramCredentials` is **not** first-occurrence-wins
  in Java — it's unconditional `Map.put`, i.e. **last-wins**, the same pattern
  as `alterClientQuotas` (`KafkaAdminClient.java:4317`), not the same pattern as
  `createAcls`/`deleteAcls` (which do guard with `futures.get(x) == null`). The
  Rust admin core already mirrors this correctly (`kafka_admin_client.rs:4548`,
  unconditional `HashMap::insert`). Grouping "createAcls/deleteAcls/
  alterUserScramCredentials/updateFeatures" together as "the 4 that should be
  normally deduped, first-occurrence-wins" conflates two different Java
  behaviors. In practice this mislabeling is what let the actual bug above go
  unnoticed: had the wins-direction been correctly identified as "last-wins,
  same as quotas," the natural next question — "then how does quotas avoid the
  problem quotas would have?" (answer: it rejects duplicates outright) — would
  have surfaced that scram has no equivalent safety net.
- **Suggested update**: `.claude/rules/admin-client.md` (or a future phase's
  PLAN) could usefully record the actual per-RPC futures-map collapse
  direction (first-wins: createAcls, deleteAcls; last-wins/unconditional-put:
  alterClientQuotas, alterUserScramCredentials; N/A — Map input, structurally
  unique: updateFeatures) so a future Critic doesn't have to re-derive it from
  `KafkaAdminClient.java` line-by-line each time.

## Deviation verdicts (Actor's self-declared deviations from Java)

| Actor's claim | Verdict |
|---|---|
| `createAcls`/`deleteAcls` dedupe key list before submission (Rust core is `Entry::Vacant`-guarded, first-wins) | **Correct.** Verified against Java and Rust core. |
| `alterClientQuotas` deliberately does NOT dedupe its native spec, relying on `read_client_quota_alterations`'s rejection | **Correct**, and safe — traced the whole-submit-fails path; no per-key misfire possible. |
| `alterUserScramCredentials` passes every row through undeduplicated, "matching Java accepting two rows for the same user" | **Half right, half wrong.** Java does accept two rows for the same user — but the *futures dict* built on top of the Rust per-key mechanism does NOT deliver that user's real outcome; see Blocker above. The claim "matching Java" is false for the actually-observable behavior. |
| Refcount fix via `admin_incref_n(cb, n)` for all 5 wrappers | **Correct and verified** — code-read plus live stress test, no other latent instance found. |
| Net one test fewer (370→369), not a coverage regression | **Correct**, verified by diffing test name sets and confirming the removed test's target function was deleted, with surviving coverage elsewhere. |

## Suggested CLAUDE.md / rule updates

- Consider adding to `admin-client.md` a short subsection (or extending §9) on
  the "futures-map collapse direction" table above, since it's Phase-spanning
  knowledge (createAcls/deleteAcls/alterClientQuotas/alterUserScramCredentials/
  updateFeatures now all touch it) and is easy to get backwards without reading
  the exact `KafkaAdminClient.java` line.
- Consider adding an explicit rule (could go in `admin-client.md` §2, next to
  the `admin_async_per_key_op` description) stating: *"If a Python/C binding
  dedupes the futures/promise-tracking keys for a per-key RPC, it MUST also
  dedupe (or otherwise pre-resolve) the row list handed to the native call, OR
  guarantee the underlying Rust core never collapses two rows into one future
  in the first place. Deduping only one side reintroduces the exact
  'not-present' fallback race this mechanism's own doc comment warns about."*
  This would have caught the scram bug mechanically rather than requiring a
  live reproduction.


---

## Resolution (Opus, 2026-09-16)

Fixed by deduping to DISTINCT users across all three layers (matching Java's
per-user result map):
- `src/ffi/admin.rs` `kafka_admin_AdminClient_alter_user_scram_credentials_async`: `keys` now deduped to distinct usernames (first-occurrence order) so it matches the per-user `entries`, eliminating the synthetic missing-key error that raced ahead and won the caller's Future.
- `bindings/python/_confluentkafka.c`: new `count_distinct_scram_users` helper; incref count is now the distinct-user count (mirrors `count_distinct_config_resources`), keeping the callback refcount balanced against the now-deduped firing count.
- `bindings/python/admin.py`: docstring corrected.
- Regression tests: `alter_user_scram_credentials_async_collapses_two_rows_for_one_user` (Rust FFI, asserts exactly one firing with the real error) and `test_alter_user_scram_credentials_collapses_two_rows_for_one_user_to_the_real_outcome` (Python).

Verified: cargo test --lib --features ffi (4161 passed), C test_mock_admin (181/0), Python admin suite (191 passed) + scram tests (8 passed), format-check clean.
