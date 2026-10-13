# Critic 71 review of `e15df4c3..03183b96` — resolved

### C71-1: Commit replaces the `kafka` submodule with a symlink to a local absolute path
- **Severity**: Bug (repository damage), High
- **File**: `kafka` (tree entry), still present at 03183b96
- **Introduced by**: `29078f40` ("ffi/admin tests: rename the features group's complete_later
  helper"), carried forward by the merges `d2fdc99a`, `b48130d0`, `604a86bc`
- **Description**: `git show --stat 29078f40` lists `kafka | 2 +-` next to the test rename.
  The tree entry changes from the gitlink `160000 commit 26b251a451ce…` (Apache Kafka 4.3.1) to
  `120000 blob c13a95bc…`, a symlink whose target is
  `/Users/pratyush/Desktop/example-confluent-kafka-rust/kafka`. `.gitmodules` still declares
  `[submodule "kafka"] path = kafka`. This is the parallel-worktree hazard (each worktree symlinks
  `kafka` to the main checkout) committed by accident.
- **Failure scenario**: the branch is already pushed to PR #201's branch. Any other clone or CI
  checkout gets a dangling symlink where the Java source of truth should be, and
  `git submodule update` no longer matches the tree. Merging #201 deletes the submodule from
  `master`. The `rust/xtask` Java-parity tooling (`xtask/src/java.rs`, `error_hierarchy.rs`,
  `config_types.rs`) reads `kafka/clients/...`, so it breaks on such a checkout too.
- **Expected**: `kafka` stays the gitlink at `26b251a4`. Restore it with a fixup of 29078f40
  (`git update-index --cacheinfo 160000,26b251a451ce941d3d7a55e6487bcb7f16b5ad48,kafka`, without
  touching the local symlink), and check `git ls-tree HEAD kafka` shows mode `160000` before
  pushing.
- **Actual**: `git ls-tree 03183b96 kafka` → `120000 blob c13a95bc… kafka`.
- **Resolution** (`883a46bb`, fixup of `29078f40`): restored the gitlink with
  `git update-index --cacheinfo 160000,26b251a451ce941d3d7a55e6487bcb7f16b5ad48,kafka`, committing
  only that index change. `git ls-tree HEAD kafka` → `160000 commit 26b251a451ce941d3d7a55e6487bcb7f16b5ad48 kafka`.
  The worktree's local `kafka` symlink stays on disk, unstaged; every later commit was staged by explicit path
  and checked with `git diff --cached --stat`.

### C71-2: `_is_done` stays false forever for a future pending when the admin client is destroyed
- **Severity**: Behavior Mismatch, Low
- **File**: `rust/src/ffi/admin.rs:1151` (`LazyOutcome::is_done`), so every `*_is_done` accessor
- **Introduced by**: `27850506` (shared machinery)
- **Java Reference**: `KafkaAdminClient.java:1477-1486` (`AdminClientRunnable.run()`) and
  `:1584-1586` (calls submitted after exit): when the admin thread
  exits it fails every pending call with `TimeoutException("The AdminClient thread has
  exited.")`, so every outstanding future's `isDone()` becomes true.
- **Description**: `FutureWaiter::get` checks the destroy token and returns
  `admin_thread_exited()` at once. `LazyOutcome::is_done` is
  `cell.get().is_some() || (p.is_done)()`. It never looks at `p.waiter.destroyed`, and nothing
  completes the core `KafkaFutureImpl` after `shutdown_background()` (there is no `Drop`
  completion).
- **Failure scenario** (reproduced in a scratch copy, with a test added next to
  `list_topics_pending_at_destroy_reports_that_the_admin_thread_exited`):
  1. `list_topics(admin, 60000, …)` returns a handle.
  2. `kafka_admin_AdminClient_destroy(admin)`, then sleep 200 ms.
  3. `kafka_admin_ListTopicsResult_is_done(result)` → **false**.
  4. `names_to_listings(result)` → "The AdminClient thread has exited." at once.
  5. `_is_done` → **true** only after that read.

  PLAN §2.4 names `_is_done` as the polling primitive. A C caller polling
  `while (!X_is_done(r, i)) sleep(...)` after destroy-without-close therefore spins forever. That
  is exactly the hang the destroy token was added to prevent, and the error getter on the same key
  would have answered at once.
- **Expected**: `is_done` returns true once the token is cancelled, e.g.
  `|| p.waiter.destroyed.is_cancelled()`, matching what the next `get()` reports. Add a test
  asserting `_is_done` is true after destroy and before any read.
- **Actual**: false until some blocking accessor has been called.
- **Resolution** (`d336db6c`, fixup of `27850506`): `LazyOutcome::is_done` is now
  `cell.get().is_some() || pending.is_some_and(|p| (p.is_done)() || p.waiter.destroyed.is_cancelled())`, so every
  `_is_done` (and the new `_all_is_done`) reports true once the destroy token is cancelled, agreeing with the
  next read, which fails at once. Every public `_is_done` doc now says so; `ListTransactionsResult_is_done(i)`
  notes that a discovery still pending at destroy fails discovery instead (so it stays false while
  `_by_broker_id_is_done` is true). Tests: `a_pending_future_reports_done_once_the_admin_client_is_destroyed`
  (shared machinery: cancel the token → `is_done` true before any read; reads return at once),
  `list_topics_pending_at_destroy_reports_that_the_admin_thread_exited` (now asserts `_is_done` true right after
  destroy), and the new per-key `create_topics_pending_at_destroy_reports_done_and_the_admin_thread_exited`.
  Mutation check: removing the `is_cancelled()` term fails all three.

### C71-3: The header does not document the two new behaviours a C caller must know
- **Severity**: Missing Requirement (docs, DoD §7 "documented deviation"), Low
- **File**: `rust/src/ffi/admin.rs:676-690` (`kafka_admin_AdminClient_destroy` rustdoc), and
  the "**Blocks**" accessor docs
- **Introduced by**: `27850506` (and each group commit that copied the accessor doc template)
- **Description**: PLAN §2.3 calls destroy-before-resolution "a documented deviation (DoD §7)",
  but it is documented only in private items (`FutureWaiter`, `admin_thread_exited`,
  `AdminHandle::destroyed`), which cbindgen does not copy. `grep -c "thread has exited"
  rust/target/include/confluent_kafka.h` → 0. The header's `kafka_admin_AdminClient_destroy` doc
  still only talks about `_async` operations in flight. It does not say:
  - that a sync result handle may outlive the admin client and is still safe to read and
    `_destroy`;
  - that a future still pending then reports `RequestTimedOut` "The AdminClient thread has
    exited.".

  The second missing rule: a blocking accessor must not be called from a tokio runtime thread.
  The documented case is the `_async` callback that "runs on a tokio worker thread" when the
  dispatcher is gone. That call panics in `Handle::block_on`, and the panic surfaces only through
  the error getters. `_count` returns 0, value getters return NULL / -1, and `_is_done` returns
  false, all indistinguishable from an empty or failed result. Today a C reader learns this only
  from the internal `FutureWaiter::get` comment and PLAN §2.3.
- **Expected**: The `kafka_admin_AdminClient_destroy` rustdoc states the result-handle lifetime
  and the error. Each blocking accessor's "**Blocks** …" sentence (or at least the result type's
  opaque-struct doc) says it must not be called from a thread driving a tokio runtime, and what
  it returns if it is.
- **Actual**: neither rule appears in `confluent_kafka.h`.
- **Resolution** (`28feec74`, fixup of `27850506`):
  - `kafka_admin_AdminClient_destroy` doc: sync result handles are not freed and may outlive the client (still
    safe to read, still released by their own `_destroy`); a resolved outcome stays readable; a future still
    pending reports `_is_done` true and every read fails at once with `REQUEST_TIMED_OUT` (Java's
    `TimeoutException`) "The AdminClient thread has exited."; Java has no `destroy`, call `close` first.
  - `kafka_admin_AdminClient_t` gains a "Result handles of the synchronous entry points" section: a blocking
    accessor whose future is pending must not be called from a thread driving a tokio runtime (an `_async`
    callback that fell back to a tokio worker, or C code called from Rust async code); there the error getters
    and `_all` return `LOCAL_ILLEGAL_STATE` "Rust panic caught at the FFI boundary...", the others their failure
    value (0 / -1 / NULL / false), nothing is cached, and a later read from an ordinary thread waits normally.
    Non-blocking accessors and already-resolved futures (every `_async`-delivered handle) are safe anywhere.
  - Each of the 44 `*Result_t` opaque types points to that section (rather than repeating it in 171
    "**Blocks**" sentences).
  - Test `a_blocking_accessor_on_a_tokio_runtime_thread_fails_and_caches_nothing` pins the documented behaviour.

### C71-4: Key accessors without a blocking statement; stale comments; duplicated submit helpers
- **Severity**: Design Flaw (docs / DoD §6), Low
- **Introduced by**: `27850506`
- **Description**:
  1. PLAN §2.2 requires *every* accessor's own rustdoc to say whether it blocks, because cbindgen
     copies only per-item docs. Nine never-blocking key accessors in the topics family say
     nothing:
     - `DeleteTopicsResult_count` / `_get_key` (`admin.rs:3973`, `:3990`)
     - `DescribeTopicsResult_count` / `_get_key` (`:4324`, `:4343`)
     - `CreatePartitionsResult_count` / `_get_key` (`:4531`, `:4549`)
     - `DeleteRecordsResult_count` / `_get_topic` / `_get_partition` (`:4792`, `:4810`, `:4829`)

     Their siblings (`CreateTopicsResult_count` / `_get_key`, and every later group) do say
     "Never blocks". In a header where most accessors now block, silence reads as "may block".
  2. `submit_delete_topics_by_names` (`:4972`) is now identical to
     `submit_delete_topics_by_names_entries` (`:5044`) apart from the name, and the same holds for
     `submit_describe_topics_by_names` (`:4998`) and `_entries` (`:5073`). Before 27850506 the
     first of each pair returned the joined future, so the split had a reason; it no longer does
     (DoD §6). The by-ids pairs differ only in key type, `Uuid` versus re-stringified `String`.
  3. Stale section comments describe the deleted joined path:
     - `:4963-4969` says the helpers return "the `KafkaFuture` whose resolution yields the
       per-key outcomes".
     - `:5023-5031` says "The synchronous/flattened path keeps using the `join_map_results`
       variants above unchanged", which is now false.
     - The `admin_async_value_op` doc (`:998`) says its future is "normally
       `KafkaFuture::join_map_results(...)`". No non-test code calls `join_map_results` any more;
       every remaining caller passes a single core future.
     - `AdminHandle.runtime` (`:242`) says it "Drives the sync entry points via `block_on`".
       Only `close` and accessors do; the entry points no longer block.
     - The SAFETY comment of `list_topics_result_ref` (`:4141`) names only `box_list_topics_result`
       as the allocator, while the sync path now allocates through `into_handle`.
- **Expected**: add "Never blocks: …" to the nine docs; delete one helper of each identical pair;
  fix the five comments.
- **Resolution** (`d35dba3f`, fixup of `27850506`):
  1. "Never blocks" added to all nine listed accessors.
  2. Deleted `submit_delete_topics_by_names` and `submit_describe_topics_by_names`; the sync entry points call
     the `_entries` twins. The by-ids pairs are kept on purpose: the sync handle sorts by `Uuid` before
     stringifying (a different order from sorting the base64 strings), while the per-key callbacks need the
     `String` key; the section comment now states this.
  3. Fixed: both submission-helper section comments (merged into one accurate header), the `admin_async_value_op`
     doc, the `admin_async_per_key_op` doc (also described `admin_async_value_op` as joining with
     `join_map_results`), `AdminHandle.runtime`, the `list_topics_result_ref` SAFETY comment (allocated by
     `into_handle`), and `FutureWaiter::get`'s "blocking entry points" wording.

### C71-5: PLAN's `_all_is_done(result)` (Java `all().isDone()`) was not added
- **Severity**: Missing Requirement (vs the approved design), Low
- **File**: PLAN §2.2 table row `isDone()`; `rust/src/ffi/admin.rs` (only
  `DescribeUserScramCredentialsResult_all_is_done`, `:28227`, exists)
- **Introduced by**: the group commits (27850506 … 18b35d77), branch-wide
- **Description**: The approved table promises `_is_done(result, i)` / `_all_is_done(result)` as
  new. Only the per-key `_is_done(i)` (and single-future `_is_done(result)`) were added; no per-key
  result has `_all_is_done`.
  - For the plain `allOf` results it can be derived in C by looping `_is_done(i)`.
  - It cannot be derived as simply for `ListTransactionsResult`. Java's `all()` / `allByBrokerId()`
    complete on the **first** failing broker (`ListTransactionsResult.java:92-120`), so
    `all().isDone()` can be true while other brokers are pending. A C caller then has to combine
    `_by_broker_id_is_done`, per-broker `_is_done(i)` and per-broker `_get_error(i)` to reproduce
    it.
- **Expected**: either add `_all_is_done` (at least where `all()` is not a plain `allOf`:
  `ListTransactionsResult` `_all` / `_all_by_broker_id`), or amend PLAN §2.2 to say
  `all().isDone()` is intentionally derived from the per-key `_is_done`.
- **Resolution** (`10a1f372`, fixup of `27850506`, branch-wide): added `keyed_all_is_done` next to `keyed_all`
  (Java `allOf`: done once every key resolved, failed or not; true for no keys) and
  `kafka_admin_<R>Result_all_is_done(result) -> bool` (never blocks) for all 24 per-key results with a Java
  `all()` (`DescribeTopics` documents `allTopicNames()`/`allTopicIds()`, `DescribeLogDirs` `allDescriptions()`,
  `DeleteAcls` follows its `allOf`-then-`getAclBindings` `all()`). `ListTransactionsResult_all_is_done` and
  `_all_by_broker_id_is_done` read the core `all()` / `allByBrokerId()` futures, which are fail-fast like Java's.
  Single-future results keep `_is_done`; SCRAM's existing `_all_is_done` is unchanged. Tests:
  `keyed_all_is_done_is_true_once_every_key_has_resolved`, `list_transactions_all_is_done_fails_fast_like_java`
  (done while broker 1 is still pending once broker 2 failed; all-succeed; failed discovery),
  `topics_family_all_is_done_tracks_the_pending_request` (DeleteTopics / DescribeTopics / CreatePartitions /
  DeleteRecords end to end), and a false-before / true-after-`all()` pair in each other per-key result's existing
  accessor test.

## Suggested rule updates (Critic 71), with the Actor's endorsement — text only, rules not edited

1. **C71-1 is the second time the parallel-worktree `kafka` symlink has threatened a commit**
   (user memory "Parallel worktree agent hazards" already lists "kafka symlink breaks git apply").
   Suggest a check in `make verify` / the pre-commit gate that fails when
   `git ls-tree HEAD kafka` is not mode `160000`, plus a line in `agent-roles.md` for Actors
   working in a worktree: "never `git add -A` / `git commit -a` at the repo root; stage paths
   explicitly". A Critic reviewing a range should always run `git diff --stat <range>` over the
   whole tree, not just the files named in the task. The task brief listed six files; the seventh
   changed path was the damaging one.
2. **`definition-of-done.md`, for FFI changes that add a cancellation / "fail instead of hang"
   path**: require the test to cover *every* observer of the future, not only `get()`. Here,
   `isDone` (C71-2) was missed while `get` was tested.

   Actor note: endorsed. Concretely, for the Manager to consider under the agent-roles process:
   - `definition-of-done.md`, new item: "Before every commit run `git diff --cached --stat` and confirm no
     submodule path (`kafka`) appears; in a worktree never use `git add -A` / `git add .` / `git commit -a`."
   - `make verify` / the pre-commit gate: fail when `git ls-tree HEAD kafka` is not mode `160000`.
   - `definition-of-done.md` §3: "a fail-instead-of-hang path (cancellation, destroy) must be tested against
     every observer of the future — `get`, `isDone`, and any aggregate such as `all().isDone()` — not only `get`."
