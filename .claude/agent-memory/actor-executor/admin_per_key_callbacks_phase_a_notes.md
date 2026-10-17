---
name: admin_per_key_callbacks_phase_a_notes
description: Phase A (Topics family) of reversing the admin FFI's joined per-key callback design into true per-key delivery — mechanism, gotchas, and what later phases (B–G) must replicate
metadata:
  type: project
---

Branch `feat/admin-per-key-callbacks-phase-a` (worktree, off master
6c302437). Full plan: `/Users/pratyush/.claude/plans/spicy-splashing-kitten.md`
(7 phases total, Phase A = Topics family: create_topics, delete_topics ×2,
describe_topics ×2, create_partitions, delete_records). Commits (newest last):
92930ab3 (Critic-65 fixup), 09f2e294 docs, b5412794 test, 9003db1a
grpc-harness-fix, fb7a1889 python-binding, 2e774090 rust-ffi, bba95a8b
core-kafka-future.

**Working-directory note:** this session discovered the main checkout
(`/Users/pratyush/Desktop/example-confluent-kafka-rust`) was being actively
used *concurrently* by another Actor session (their `git stash` got popped by
their own process mid-investigation, and a later untracked memory-file write
to the main tree's `.claude/agent-memory/` vanished between sessions — likely
wiped by their `git clean`/`checkout`). Moved to an isolated `git worktree`
(`.claude/worktrees/actor2-admin-per-key-callbacks`) for the rest of the
session. **Consequence for memory:** writes to the main tree's
`.claude/agent-memory/` are not safe when another session may be using that
same checkout concurrently — prefer writing into the worktree you are
actually committing from, and note that it will not propagate to the main
tree or sibling worktrees on its own.

## The mechanism (reusable verbatim for Phases B–G)

1. **Core**: `KafkaFuture<T>::when_complete` (public, new) in
   `src/common/kafka_future.rs`. Implemented via a new
   `KafkaFutureOps::register_completion(self: Arc<Self>, callback)` trait
   method — **default** impl spawns a task awaiting `get()` (works for every
   existing implementor — `FutureRecordMetadata`, `AclBindingsFuture`,
   `AllByBrokerIdFuture`, the 4 combinator futures — with zero changes to
   those files); `Completable` overrides it to delegate to the existing
   (already-tested) eager `on_complete` inherent method — **this is the path
   every admin per-key future actually takes** (KafkaFutureImpl-backed, always
   `Completable`). `CompletedFuture` overrides to fire immediately too.
   Needed `T: Send + 'static` added to the trait bound (was just `T: Send`) —
   required for the spawn's `'static` future; harmless, all impls already
   satisfied it.
2. **FFI**: `admin_async_per_key_op<K,T,S,C>` in `src/ffi/admin.rs`, sibling
   of `admin_async_value_op`. Takes `keys: Vec<K>` (for the failure fan-out)
   + `submit: FnOnce(&dyn Admin) -> Result<Vec<(K, KafkaFuture<T>), Error>>`
   (note: **unjoined** `Vec`, not the `HashMap` the old `submit_*` helpers
   built for `join_map_results`) + `complete: Fn(K, Result<T,Error>, *mut
   c_void)`. Registers `when_complete` per entry; each fires independently
   through the SAME completion-dispatch queue every other async op uses. On
   null admin or `submit` Err, fans the SAME error out to every key in
   `keys` inline — never a single aggregate error. **If `keys` is empty
   (e.g. a genuinely empty request combined with a null admin), the callback
   correctly fires ZERO times** — there is nothing to report per-key. This
   is intentional, not a bug, and no real caller can trigger it (Python
   always builds `keys` from non-empty input before calling in) — but it
   DOES mean a C-level "null handle" unit test must submit a non-empty
   key set to actually observe the fan-out (see the C-tests section below).
3. Each RPC needs a new `submit_X_entries` fn (unjoined counterpart of the
   existing `submit_X`) — **do not delete or touch `submit_X`**, the sync
   path still uses `join_map_results` unchanged.
4. Callback typedef: kept the EXISTING name (`kafka_admin_AdminClient_X_callback_t`,
   no `_async` in the name — CLAUDE.md's C-FFI naming rule names it after the
   Java method, not the C entry point) but changed its SIGNATURE to
   `(key: *const c_char, [value: *mut ValueHandle_t,] error: *mut
   kafka_common_Error_t, user_data: *mut c_void)`. Void-shaped RPCs
   (`KafkaFuture<Void>`) drop the value param.
5. **Key is always the STRING representation**, even for by-id variants —
   `Uuid.to_string()` re-stringified, matching what Python passed in. For a
   composite key (delete_records' `TopicPartition`), split into two params
   (`topic: *const c_char, partition: i32`), not a single opaque key handle.
6. **New standalone value handles**: when a per-key value type (e.g.
   `TopicMetadataAndConfig`) previously only existed *embedded* in a
   flattened `*ResultInner`'s `Vec` (borrowed, freed via the parent's
   `_destroy`), the per-key callback needs it delivered **individually and
   owned** — same backing struct (`TopicMetadataAndConfigInner`), reused
   as-is via `Box::into_raw`, plus a NEW `_destroy` fn. Document the
   dual-provenance clearly (same type name, two different ownership/destroy
   rules depending on which path delivered it) — this mirrors the
   PRE-EXISTING `kafka_common_Error_t` convention in this same file
   (borrowed+don't-destroy from a `_get_error(i)` accessor vs.
   owned+must-destroy from a callback), so it's not a new pattern, just
   applied to a second type.
7. For a Java class with only ONE scalar field (`DeletedRecords.lowWatermark()`)
   that the OLD sync path inlined directly into the flattened result (no
   handle at all), the per-key callback still needs a minimal NEW opaque
   handle (`kafka_admin_DeletedRecords_t`) — there's no containing result to
   inline into. Small: one field, one accessor, one destroy.

## The refcount problem (the one genuinely hard part) — solved, reuse the fix

The native callback fires N times (once per key) sharing ONE `user_data`/`cb`
slot (the outer `_async` C entry point's signature has exactly one
`callback`+`user_data` param pair — did not change that). Naive
`Py_INCREF(cb)` once + `Py_DECREF(cb)` per invocation only balances if the
INCREF count exactly equals the number of times Rust actually invokes the
callback.

**Fix**: `admin_incref_n(cb, count)` in `_confluentkafka.c`, where `count` is
computed from the SAME array/count the C wrapper already builds for the
request (e.g. `build_new_topics`'s returned count) — AS LONG AS the Python
layer has **already deduplicated** the input by key before calling in.

**De-duplication direction is NOT always "last wins" — check the specific
RPC's Java/Rust semantics, don't assume.** `create_partitions`/`delete_records`
were already dict-keyed inputs (a Python dict can't have duplicate keys, so no
extra work). `delete_topics`/`describe_topics` dedupe via `dict.fromkeys` on
the plain string key (no "which value wins" question — there IS no per-key
value beyond the key itself). **`create_topics` is the one case where the
value actually varies per duplicate key (a full `NewTopic` spec), and Critic
round 65 caught that the FIRST implementation got the direction backwards**:
`{t.name: t for t in new_topics}` is a dict comprehension, which is
LAST-occurrence-wins. Java's `KafkaAdminClient.createTopics`
(`KafkaAdminClient.java:1782-1796`) and this crate's own
`KafkaAdminClient::create_topics` (`src/admin/kafka_admin_client.rs`) both use
an `Entry::Vacant` check — FIRST-occurrence-wins, silently dropping later
duplicates. The fix is an explicit loop:
```python
deduped = {}
for t in new_topics:
    if t.name not in deduped:
        deduped[t.name] = t
```
**Lesson for later phases**: any RPC whose per-key value can legitimately
differ across a duplicate key needs its Python-side dedup direction checked
against the actual Rust admin-core implementation (grep for `Entry::Vacant`
vs. `Entry::Occupied`/overwrite in `src/admin/kafka_admin_client.rs`) — do not
assume a dict comprehension's natural last-wins is correct just because it's
convenient to write.

## asyncio.Future subtlety (bit me once, worth restating)

`AsyncAdmin`'s per-key callback resolves via `loop.call_soon_threadsafe(...)`
(asyncio.Future isn't thread-safe to touch directly, unlike
concurrent.futures.Future which IS — so the SYNC `Admin` class resolves
directly, no thread hop). Since `create_topics` etc. are `async def` methods
with **no internal `await`**, calling `await admin.create_topics(...)` runs
the body to completion WITHOUT ceding control to the event loop (no
suspension point == no yield), so the returned Futures are **provably still
pending** immediately after — even for MockAdminClient, which resolves
everything synchronously at the Rust layer. Every caller (tests, gRPC
harness) must `await` each individual Future, not just index into the
returned dict. This was the single most common test-fix pattern (7+ test
failures) — grep `await futures\[` in test_admin.py for the fixed shape.

## Verification gap Critic round 65 caught — ADD THIS to every later phase's checklist

**The Rust FFI callback typedef signature changes are NOT exercised by
`cargo test` / `cargo build --features ffi` alone.** `bindings/c/tests/`
(a separate CMake/CTest C project under `bindings/c/`) has its own hand-written
C callback functions matching the OLD signatures, and nothing in a normal
`cargo build`/`cargo test` run compiles or links that project — so a
callback-typedef shape change can silently break it with zero red flags from
the Rust-only verification pass. This is exactly what happened here: Phase
A's original verification never touched `bindings/c/tests`, and it had 10
hard compile errors (`-Wincompatible-function-pointer-types`) that
`bindings/c/CMakeLists.txt` (no `-Werror`) might not even fail the build on
with every compiler — silent UB at the call site on some toolchains.

**Required verification for every phase that changes an admin async callback
typedef** (this includes every later phase B–G, since they all touch
`kafka_admin_AdminClient_*_async` typedefs the same way):
```
cargo build --release --features ffi   # bindings/c looks in target/release first
cmake -S bindings/c -B bindings/c/build -DRUST_PROJECT_ROOT=$(pwd)
cmake --build bindings/c/build
cd bindings/c/build && ctest --output-on-failure
```
`bindings/c/tests/unity` is a git submodule; if it's ever empty run `git
submodule update --init` first (it was already populated in this worktree).
The `mock_admin`/`test_kafka_admin` targets are the ones that actually touch
the admin FFI; `test_kafka_admin` needs a real broker for several tests and
may show a PRE-EXISTING unrelated failure if one happens to be reachable at
`localhost:9092` in the sandbox (this happened in this session — a stray
`java` process was listening there — confirmed via `lsof -iTCP:9092
-sTCP:LISTEN`; not caused by any admin change, don't chase it).

**Updating the C test callbacks**: same signature change as the Python
trampolines in `_confluentkafka.c` — `(key, [value,] error, user_data)` or
`(topic, partition, [value,] error, user_data)` for delete_records. Each test
struct needs generic `error_count`/`value_count` (usually `atomic_int`,
mirroring the existing `atomic_int fired`) since the callback now fires once
per key, not once per batch — `wait_for(&r.fired, N)` where N is the number
of keys, not 1. **The two "NULL handle" tests per RPC need a non-empty
input array** (not the old `NULL, 0`) — see point 2 in "The mechanism" above:
an empty request fans out over zero keys regardless of admin validity, so the
old degenerate all-null test would hang forever waiting for a callback that
correctly never fires under the new design.

**Sanity-checking a "proves independence" test actually catches the
regression it targets is cheap and worth doing**: temporarily inject the bug
you're guarding against (e.g. a `block_on` before per-key dispatch,
simulating a reintroduced join), confirm the test fails (here: the whole
`cargo test` process hangs, caught by wrapping the invocation in an external
`timeout N ... ; echo REAL_EXIT=$?` — do NOT pipe through `tail` first, it
swallows the real exit code, see `workflow_teeth_check_mtime.md`-style
gotchas), then revert and confirm it passes again. Five minutes of extra
work, and it is the only way to know a test *would* have caught the bug it's
named after rather than merely exercising the happy path.

## Things checked and found NOT needed

- `KafkaFutureImpl::when_complete` (crate-internal, pre-erasure,
  `#[allow(dead_code)]`) — still unused BY THIS PHASE'S code, still dead
  code from this branch's perspective, do NOT try to wire it: it operates
  BEFORE the admin `*Result` type-erases to the public `KafkaFuture<T>`, and
  the FFI only ever sees the post-erasure type. **Correction from Critic
  round 65**: the original Phase-A commit message calling it "still unused"
  was itself checked and found WRONG independent of this branch — it already
  has a real caller at `src/admin/kafka_admin_client.rs:4321`
  (`describe_handle.when_complete(...)`) predating this branch entirely. Its
  `#[allow(dead_code)]` annotation was already stale on `master` before
  Phase A — a pre-existing doc/annotation mismatch, not something to fix here
  (flagged as informational-only, no action taken).
- No new `QueuedSends`-style machinery, no atomic-counter heap context
  struct for the C refcounting — the simpler `admin_incref_n` count-matching
  approach (see above) is sufficient and was NOT more complex to get right
  than an atomic-counter alternative would have been.
- A full `Admin` trait stub for testing temporal independence — the trait has
  47 methods. Instead, test `admin_async_per_key_op` DIRECTLY with hand-built
  `KafkaFutureImpl` instances (one complete, one never completed) and any
  concrete `&dyn Admin` just to build a valid `AdminHandle` (a
  `MockAdminClient` already at hand works fine, since `submit`'s `&dyn Admin`
  parameter is never touched by the test closure). See
  `ffi::admin::tests::admin_async_per_key_op_delivers_a_resolved_key_without_waiting_on_a_pending_one`.

## Test-writing gotchas found here (apply to B–G too)

- A "submission failure" (bad marshaling, e.g. invalid base64 topic id) NO
  LONGER raises synchronously from the call — it resolves EVERY requested
  key's Future with that error (fan-out), keyed by the RAW input string since
  parsing never got far enough to produce a typed key. Tests asserting
  `pytest.raises(...)` around the call itself must become
  `futures[key].result()`/`.exception()` assertions.
- A whole-batch pure-Python converter tested via synthetic tuples
  (`_to_delete_records(raw_dict)`-style) may become genuinely untestable in
  pure Python once its logic moves behind a C drain function requiring a live
  Rust handle (`_lib.DeletedRecords_drain`). If the mock ALSO never reaches
  that success path (check the Java `MockAdminClient` source — ours mirrors
  it), removing the test with a documented reason is correct, not a coverage
  regression — it was never reachable end-to-end either.
- Add at least one explicit "two keys resolve independently regardless of
  read order" test per phase — it's the ENTIRE point of the change. But be
  honest in the test's own docstring about what it does and does NOT prove
  if the mock resolves synchronously (structural independence, not temporal)
  — and consider adding the Rust-level `admin_async_per_key_op`-direct test
  described above for genuine temporal-independence coverage, since it's
  cheap once the pattern exists.

## Non-obvious dependents outside admin.py / test_admin.py

`grpc_server.py` / `grpc_server_async.py` (multilanguage test harness) call
these RPCs and feed the result straight to `grpc_translate.py`'s
`_admin_*_response` builders, which still expect an ALREADY-RESOLVED dict.
Added `_resolve_admin_futures` (sync, blocks) / `_resolve_admin_futures_async`
(async, awaits) in `grpc_translate.py`; call one right after each of the
7 affected RPCs, BEFORE handing off to the (unchanged) translator — do this
for every later phase's RPCs too, or the gRPC harness silently breaks
(`TypeError` trying to iterate `Future` objects as `(key, value)` pairs, or
worse, treating a `Future` object as the value itself).
`bindings/python/soak/*.py` and `test/performance/performance_common.py` use
the REAL `confluent_kafka` package (not this repo's `admin.py`) — false
positives when grepping for `create_topics` etc., not actually affected.

## Environment gotchas hit this session (see also other memory files)

- `pip install -e .` fails here (broken internal CodeArtifact index with no
  PyPI fallback configured) — see `python_bindings_local_build_test_macos.md`
  for the manual `cc -bundle` workaround; this session's exact command (for
  the mise-managed Python 3.13, not the old 3.12 homebrew one that memory
  documents):
  `PYINC=/Users/pratyush/.local/share/mise/installs/python/3.13.14/include/python3.13`,
  output `_confluentkafka.cpython-313-darwin.so`, link `target/debug` +
  matching rpath (release build not required for iteration — debug is fine
  and much faster; only `bindings/c/tests` needs a `target/release` build
  since its `CMakeLists.txt` searches `target/release` before `target/debug`).
- `pip install --index-url https://pypi.org/simple <pkg>` DOES work despite
  the 401 warnings from the broken internal index (it falls through to the
  explicit `--index-url` for the actual package fetch) — this got `pytest`,
  `pytest-asyncio`, and `psutil` installed successfully this session, so the
  earlier memory's "pip install is denied" framing is stale for THIS session
  at least; the fallback URL is what unblocks it. Try that before assuming
  pytest is unreachable.
- `cargo xtask lint` (clippy) is NOT available in this sandbox at all — see
  `env_clippy_unavailable_nix.md`.
- This session's working directory was ALSO being actively used by a
  concurrent Actor (their `git stash` got popped by their own process
  mid-command while I was investigating unrelated staged changes on
  `master`, and later an untracked memory-file write to the main tree
  vanished between sessions). Used an isolated `git worktree` (not just a
  branch) for the rest of the session to avoid any further collision —
  recommended for any future multi-actor session sharing this same repo
  checkout, and be aware memory writes to the main tree are NOT safe under
  the same conditions.
- `bindings/c/tests/unity` is a git submodule and was already initialized in
  this worktree (`git submodule status` showed a real commit hash, not a
  `-` prefix). If it ever shows uninitialized, run
  `git submodule update --init` before trying to build `bindings/c`.
