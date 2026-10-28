# M15/P13.3 — .NET Admin: merge PR #201's round-70 commits (`3b27d2c9`) and adapt the binding

**N = 86.** Branch `prashah_dev_dotnet_binding`. **Base = merge `16d2b4c5`**
(CP0: merge of PR #201 head `3b27d2c9` into `a26f43c6`, M15/P13.2's close; not pushed —
leave it that way). The phase's first commit is the merge itself (§3); every later
commit is **Mode A** (C# only).
**User ruling, 2026-09-30: APPROVED** with every PM recommendation, D1–D18, and one
amendment to D1 — "Let's run critic only after CP6. That way critic will review changes
for CP0–CP6 in one go." So D11/D12/D13 are in (CP5 extern target **697**), D14 is deferred,
D18's non-admin regression set is in, and the Critic runs once over CP0–CP6 (§1.1, §8 D1).

N derivation: unfiltered `find -name 'COMMENTS*.md'` over the repo (excluding `target/`,
`kafka/`) → highest used = **85** (P13.2; its record is archived at
`design/history/M15/P13.2-admin-parity-audit-fixes/COMMENTS.DONE.85.md`). Nothing ≥ 86
exists anywhere, and `project_dotnet_binding_numbering.md` (PM memory) agrees: 84 = P13.1,
85 = P13.2. The root file `COMMENTS.DONE.70.md` that the merge brings in is the Rust
side's Critic 70 record, not ours.

Source data: PR #201 (`feat/admin-per-key-python`), the **22 commits** from the merge
base `8796564b` (our last PR #201 merge, P13.1's `54ed917c`) to the head `3b27d2c9`.
28 files, +2904/−733. **No file under `bindings/dotnet/` changes.** Everything that
breaks or changes for .NET arrives through the C ABI (`target/include/confluent_kafka.h`,
generated from `src/ffi/*.rs`).

---

## 0. Why, and what is in scope

PR #201's round 70 made the C ABI more Java-exact. Two of its changes are **breaking**
for .NET: one crashes the test host, the other changes a return type. Several more
change behaviour that .NET's code relies on. This phase merges the round into our branch
and fixes the .NET side so the binding is correct against the new ABI — without writing
any Rust.

| ID | Kind | RPC / area | What the core now does | What happens to .NET today | Fix | CP |
|---|---|---|---|---|---|---|
| **F5** | **MUST FIX — crash** | `listTransactions` | Two callbacks: one "discovery" call with the broker ids, then one call per broker as it finishes. Signature changed. | The old P/Invoke passes arguments into the wrong slots → the core calls through a bad pointer → **test host aborts** | New two-stage bridge (§4.1) | CP1 |
| **F10** | behaviour | `describeReplicaLogDirs` (mock) | A replica the mock skips arrives with **both** value and error NULL ("absent key") instead of a synthetic error | .NET throws its own "carried neither…" error; **1 test fails** | Treat both-NULL as absent; fault with Python's code/message (§4.2) | CP1 |
| **F6** | **MUST FIX — ABI** | `updateFeatures` | Returns `kafka_common_Error_t*`. Non-NULL = nothing submitted, **no callback ever** | P/Invoke still says `void`: an error is leaked and every feature's `Task` **hangs** | `IntPtr` return; throw synchronously (§4.3) | CP2 |
| **F4** | behaviour | all 24 per-key RPCs | One callback per **distinct** key (first occurrence wins); was one per occurrence | Where .NET counts more keys than the core sees as distinct, the countdown never reaches zero → **hang + leak** | Audit all 26 arming sites; fix the comparer class (§4.4) | CP2 |
| **F8** | behaviour | `createAcls` (+ the older `deleteAcls` gap) | Callback keys are the **normalized** bindings/filters (an undefined enum code becomes `UNKNOWN`) | .NET keeps the raw code → lookup misses → wrong error, or a **hang** when two inputs normalize to one | Normalize undefined codes in the ACL ctors (§4.5) | CP3 |
| **F7** | behaviour | `alterClientQuotas` | A repeated entity is **sent**, like Java; one callback per distinct entity | .NET still rejects it with `ArgumentException` | Stop rejecting; key by distinct entity (§4.6) | CP3 |
| **(c)** | pre-existing, widened by F4 | per-key string keys | Keys compared as C strings | Two C# strings that differ only by an embedded NUL or a lone surrogate become **one** C key → hang | Reject such strings before submit (§4.7) | CP4 |
| **cfg** | behaviour | `describeConfigs` / IAC | A non-numeric BROKER / BROKER_LOGGER name fails every resource with `For input string: "x"` | Passes through unchanged; nothing pins it | Tests only (§4.8) | CP4 |
| D15 | optional | `describeCluster` | `cluster_id` may be NULL (old-broker fallback) | .NET turns NULL into `""` | Nullable `ClusterId()` (§4.9) | CP4 |
| D16 | optional | `NewTopic` | `put_config(key, NULL)` keeps a null value | .NET throws `ArgumentNullException("s")` on a null value | Pass NULL through (§4.10) | CP4 |
| D11 | optional | every client | New `kafka_common_Error_cause` (Java `getCause()`) | No `InnerException` | Walk the cause chain (§4.11) | CP5 |
| D12 | optional | `Node` | New `kafka_common_Node_is_fenced` | Fenced brokers look active | `Node.IsFenced` + Java's `ToString` (§4.12) | CP5 |
| D13 | optional | `createTopics` | New `TopicMetadataAndConfig_config` → full `ConfigEntry` with its real **source** | Source is guessed from `isDefault` | Read the real source (§4.13) | CP5 |
| D14 | optional | `MockAdminClient` | New `add_topic` / `mark_topic_for_deletion` / `set_broker_log_dirs` | Not exposed | **Recommend defer** (§4.14) | — |
| docs | drift | several | — | Stale remarks, stale `h:NNNN` line cites | §4.16 | CP1–CP6 |

Items with **no effect on .NET** are recorded in §4.15 so nobody re-investigates them.

---

## 1. Standing constraints (relay verbatim to Actor + Critic)

1. **Cadence — D1, as amended by the user (2026-09-30):** checkpoints run back to back
   with **no user stop** and **no Critic** between them. `dotnet-actor` 86 does CP1 → CP6,
   one checkpoint per session, and commits each one; the PM verifies each checkpoint's
   §7.1 gates itself before starting the next. After CP6, `dotnet-critic` 86 runs **one
   review over CP0–CP6 together** (the merge's conflict resolution, the plan commit and
   every checkpoint commit); then the normal fix → re-review loop runs until the Critic
   reports nothing. The PM hands back to the user only at the end (or on a blocker).
   1. `dotnet-actor` 86 implements CPk and commits; the PM verifies its gates; then CPk+1.
   2. After CP6, `dotnet-critic` 86 reviews CP0–CP6 (and the premises they rely on) in
      checkpoint order and writes each finding to `bindings/dotnet/COMMENTS.86.md` as it
      is found (exclusive lock).
   3. The Actor fixes findings with `fixup!` commits referencing the original commit, and
      moves resolved items to `COMMENTS.DONE.86.md` (exclusive lock).
   4. The Critic re-reviews; repeat 3–4 until clean.
2. One checkpoint = one commit (+ its fixups) = one resumable Actor session. With no
   Critic between checkpoints there is no step-0 nit folding: every finding lands in the
   single end-of-range review.
3. **No Rust.** `dotnet-actor` and `dotnet-critic` never author Rust, C, Python,
   `cbindgen.toml`, the generator or the Rust integration tests. Every fix is .NET-only
   (Mode A). The merge commit (§3) is the **only** commit in this phase that touches
   Rust, and it brings in Pratyush's code unchanged. If something truly needs a core
   change, stop and raise it to the PM as a user decision — **do not** write a note or
   message to Pratyush.
4. Stage by explicit path only (never `git add -A` / `.` / `commit -a`). Never stage the
   binding-root `COMMENTS.86.md` / `COMMENTS.DONE.86.md` (`bindings/dotnet/CLAUDE.md`
   §8.4); the PM archives them under `design/history/M15/P13.3-pr201-round70-merge/`
   at close. Never stage this phase's uncommitted working files or unrelated untracked
   files. **Do not push** (only when the user asks). Do not squash.
5. Do **not** edit `PendingAdminClientFindingsForDotnet.md` or anything in
   `Dotnet-AdminClient-Findings-Workflow/`. Local-only notes stay untracked through
   `.git/info/exclude` — never `.gitignore`. Never `ssh`/`scp` to the user's machines.
6. **Critic ground truth**: the **merged** C ABI header + the Kafka Java public API
   (`bindings/dotnet/CLAUDE.md` §8.2). Two header facts to rely on:
   - the P13.2 "known inaccuracy" G4-1 (deleteAcls binding/error "exactly one non-null")
     is **fixed** by `6d86635d`: the header now says a failed deletion carries both.
     .NET already reads both, so there is nothing to change.
   - the P13.2 G4-4 ambiguity ("no entity types" = a NULL pointer) is untouched and
     still recorded as known.
7. **Env traps** (they silently fake a PASS):
   - `dotnet` = `~/.dotnet/dotnet`. If `git`/`cargo` look missing, prefix
     `export PATH="/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin:$HOME/.nix-profile/bin:$HOME/.cargo/bin:$HOME/.dotnet:$PATH"`.
   - Always `cargo build --features ffi` (never a bare `cargo build`). A .NET
     **Release** build needs the native library from `cargo build --features ffi --release`.
   - `grep` may be ugrep → use `/usr/bin/grep`. There is no `sed` → use `awk` or
     `python3`. `cat` may be `bat` → use `/bin/cat`.
   - zsh does **not** word-split an unquoted `$var`, and aborts on an unmatched glob
     (quote every `--include='*.cs'`).
   - A test filter that matches nothing reports 0 tests as a pass → **always report
     `Passed:` / `Total:` counts**.
   - `Test Run Aborted` is the crash signal (exit codes vary).
   - A failed build + `--no-build` prints a stale `Passed!` → assert `0 Error(s)`.
   - Build and test in the **same** configuration, on **both** `net8.0` and `net10.0`.
   - Prove the test's native library is fresh:
     `nm -gU tests/Confluent.Kafka.UnitTests/bin/Debug/net10.0/libconfluent_kafka.dylib | /usr/bin/grep -c _kafka_common_Error_cause`
     must print `1` (the symbol does not exist before the merge).
   - Bound every tool's output (`| head`, `| tail`, `/usr/bin/grep -c`). Grep
     `src/ffi/admin.rs` and `target/include/confluent_kafka.h`; never read them whole.
8. **Exact messages** (DoD §3): every new or rewritten assertion on an exception asserts
   the message (and `ParamName` for `Argument*Exception`, and `Code` for
   `KafkaException`). Core/mock messages are copied from the core, never paraphrased.
9. Reflection tests pin every public-surface change (§6).
10. **Out of scope — do not touch, do not "fix":** the `ProducerSubmitHandleRefTests`
    heap-budget noise (if it flakes, re-run once and record it), and the `DisposeAsync`
    completion-ordering race in `OperationCompletionSource.Complete`.

---

## 2. Re-verification (PM, 2026-09-30, read-only)

All of this was checked without touching the branch: `git merge-tree`, `git archive`
into a scratch folder, and builds/tests only in that scratch copy.

| # | Claim | How checked | Result |
|---|---|---|---|
| 1 | PR head is still `3b27d2c9` | `git ls-remote origin` | ✓ `refs/pull/201/head` = `refs/heads/feat/admin-per-key-python` = `3b27d2c9eaa299654864128322b6093cc27fb393` |
| 2 | The merge is clean apart from one memory file | `git merge-tree --write-tree a26f43c6 3b27d2c9` | ✓ one conflict: `.claude/agent-memory/actor-executor/MEMORY.md` (both sides appended one index line at the same place). `src/ffi/admin.rs`, `transaction_manager.rs`, `tests/common/admin_backend.rs` auto-merge |
| 3 | PR-added files do not collide with local untracked files | `git diff --diff-filter=A` + `test -e` | ✓ neither `COMMENTS.DONE.70.md` nor the actor memory note exists locally |
| 4 | Header delta | declaration diff, HEAD header vs merged header | ✓ 916 → 923: **7 added** (`kafka_common_Error_cause`, `kafka_common_Node_is_fenced`, `kafka_admin_TopicMetadataAndConfig_config`, `kafka_admin_MockAdminClient_add_topic`, `…_mark_topic_for_deletion`, `…_set_broker_log_dirs`, typedef `kafka_admin_ListTransactionsResult_by_broker_id_callback_t`), **3 changed** (`kafka_admin_AdminClient_update_features_async` void → `kafka_common_Error_t*`; `kafka_admin_AdminClient_list_transactions_async` gains `by_broker_id_callback`; `kafka_admin_AdminClient_list_transactions_callback_t` gains a leading `int32_t broker_id`), **0 removed** |
| 5 | P/Invoke count | `/usr/bin/grep -c "internal static extern"` over `Internal/Interop/*.cs` | ✓ **700** |
| 6 | .NET still compiles after the merge | scratch build, net10.0 and net8.0 | ✓ `0 Warning(s)`, `0 Error(s)` — P/Invoke signatures are not checked at compile time, which is why the break shows up only at run time |
| 7 | The listTransactions crash | scratch, `--filter FullyQualifiedName~ListTransactions`, net10.0 | ✓ `Test Run Aborted` — "thread 'kafka-admin-callback-dispatcher' panicked at src/ffi/admin.rs:21772:14: null pointer dereference occurred" (16 passed before the abort) |
| 8 | The log-dirs failure | scratch, `AdminLogDirsLifetimeTests` | ✓ 1 failed / 14 passed / 15: `AReplicaTheMockOmits_FaultsThatKeyWithAMessageNamingIt` expected the core's "the requested key was not present in the admin RPC's response", got .NET's "The describeReplicaLogDirs result carried neither…" |
| 9 | Post-merge baseline | scratch, full suite with `--filter "FullyQualifiedName!~ListTransactions"` | ✓ **net10.0: Failed 1, Passed 2410, Total 2411. net8.0: identical.** The filter removes exactly 23 tests (pre-merge total at `a26f43c6` = 2434 on both TFMs, P13.2 close). The 1 failure is #8 |
| 10 | The mock refuses listTransactions | `mock_admin_client.rs:1076-1084` (merged) | ✓ the top-level future fails with `unsupported_version("Not implemented yet")` (Java's mock throws). So through the real native, a unit test reaches **only the discovery-error path**; the per-broker path needs the submit seam or Docker |
| 11 | The absent-key code is −4 | header `kafka_common_ErrorCode_LOCAL_ILLEGAL_STATE = -4`; `src/ffi/common.rs:474`; Python `admin.py:177-178` | ✓ code −4, not retriable, message "the requested key was not present in the admin RPC's response" |
| 12 | `kafka_common_Error_new` cannot mint −4 | header doc: a non-protocol code maps to `UnknownServerError` | ✓ so .NET builds the −4 exception directly (as Python does) |
| 13 | `updateFeatures` error sources | merged `src/ffi/admin.rs:19350-19394`, `read_feature_updates` (`:18062`) | ✓ NULL admin, a repeated name ("feature update at index {i} repeats feature \`{name}\`"), a `FeatureUpdate` ctor error, the core's validation (empty map, blank name). .NET already pre-checks the empty map, blank name and ctor with Java's messages, so through the public API the error is reachable by a repeated name (a caller dictionary with its own comparer) |
| 14 | The gRPC servicers | `grpc-server/AdminServiceImpl.cs` | ✓ ListTransactions (1109-1176) awaits `ByBrokerId()` then each broker — works unchanged with independent tasks. UpdateFeatures (2107-2141) already maps a synchronous throw to the top-level error. DescribeCluster (414, 424) sets `ClusterId = clusterId` — a null would throw inside protobuf, so D15 needs `?? ""` there |
| 15 | PR test-harness diffs do not move .NET arms | `git diff a26f43c6...3b27d2c9 -- tests/` | ✓ `admin_backend.rs` (config type now optional; cluster id `unwrap_or_default`), `multilanguage_admin.rs` (config values must be non-null), `admin_topics_test.rs` (`Some(..)`). No expectation of a `__grpc_dotnet` arm changes |
| 16 | .NET never reads `GroupAuthorizationError_group_id` | grep `.cs` sources | ✓ no reference (F11 is no-effect) |
| 17 | No .NET test pins a message the PR changed | grep "rack: None", "CreateTopicsResult::UNKNOWN", "Broker x not found", "no such broker as" | ✓ none |
| 18 | 26 countdown arming sites over 24 RPCs | `/usr/bin/grep -n "SetPendingCallbacks(" NativeAdminClient.cs` | ✓ listed in §4.4 |
| 19 | Java `MockAdminClient.updateFeatures` validates nothing | `MockAdminClient.java:1286-` | ✓ noted only; .NET's empty-map guard also applies to the mock (pre-existing, out of scope, §10) |

---

## 3. CP0 — the merge (run by the PM, before any Actor starts)

**Who:** the PM (P13.1 precedent: `54ed917c` was merged by the PM before the loop).
Proposed as **D2**. The merge is not .NET authoring, and doing it first keeps every
Actor commit Mode A.

**Steps:**
1. **Re-check the PR head** right before merging: `git fetch origin` then
   `git ls-remote origin refs/pull/201/head`. It must still be `3b27d2c9…`. **If it moved,
   stop** and come back to the user: the plan was written against `3b27d2c9` only.
2. Confirm the tree: `git status --porcelain` shows no staged changes; the only tracked
   modification allowed is the PM's own `.claude/agent-memory/project-manager/MEMORY.md`
   (the PR does not touch it). This plan file is untracked and is not touched by the PR.
3. `git merge --no-ff --no-commit 3b27d2c9`.
4. Resolve `.claude/agent-memory/actor-executor/MEMORY.md`: keep HEAD's line
   (`- [M14 verifiable-clients](…)`) **and** the PR's line
   (`- [FFI Java-exact round 70](…)`), in that order, no conflict markers.
   `git add` that one path.
5. Commit with the message:
   ```
   Merge PR #201 commit 3b27d2c9 (feat/admin-per-key-python) into prashah_dev_dotnet_binding

   Brings in round 70 of the Java-exact FFI audit: the two-stage listTransactions
   ABI, updateFeatures' synchronous error return, per-distinct-key callbacks,
   normalized ACL callback keys, repeated quota entities, the absent-replica
   outcome, Error_cause, Node_is_fenced, TopicMetadataAndConfig_config, the mock
   seeding functions, nullable cluster ids and NewTopic config values. The .NET
   Admin binding is adapted in M15/P13.3.

   Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
   ```
6. Commit this plan (`docs(dotnet): M15/P13.3 plan — …`) with **Base = the merge
   commit** filled in, as P13.1 did.

**Post-merge gates (PM, recorded in the plan commit):**
1. `cargo build --features ffi` succeeds.
2. The regenerated header matches §2 #4 exactly (7 added / 3 changed / 0 removed).
   Quick check: each of the 6 new function names greps to 1 declaration; the two changed
   prototypes read as in §2 #4.
3. `~/.dotnet/dotnet build Confluent.Kafka.sln` (Debug): `0 Warning(s)`, `0 Error(s)`.
4. The native library in the test output is fresh (§1.7 `nm` check prints `1`).
5. **Expected red, recorded as the baseline:** full suite on **net10.0 and net8.0** with
   `--filter "FullyQualifiedName!~ListTransactions"` → **Failed 1, Passed 2410,
   Total 2411** on each; the 1 failure is the log-dirs test. An unfiltered run **aborts**
   (the F5 crash). Both numbers must match §2 #9; if not, stop and investigate before CP1.
6. `git diff a26f43c6 HEAD --stat -- bindings/dotnet` is **empty** (the PR changes no
   .NET file).

**How checkpoints handle the red suite:** until CP1 lands, every full-suite run uses the
`!~ListTransactions` filter and expects exactly that one known failure. CP1 fixes both
causes, so from the end of CP1 on every run is unfiltered with `Failed: 0` and no abort.
The Critic must not report the known pre-CP1 red as a regression.

---

## 4. Per-item plan

### 4.1 F5 — `listTransactions`: the two-stage bridge (CP1, Mode A)

**What the core does now** (merged header):
- `list_transactions_async(…, timeout_ms, by_broker_id_callback, callback, user_data)`.
- `by_broker_id_callback(const int32_t* broker_ids, int32_t count, kafka_common_Error_t* error, void* user_data)`
  fires **exactly once**. On success: the broker ids, **sorted and distinct**, borrowed
  only for the call. On failure: `broker_ids` NULL, `count` 0, an **owned** error — and
  **nothing else fires**.
- `callback(int32_t broker_id, kafka_admin_ListTransactionsResult_t* value, kafka_common_Error_t* error, void* user_data)`
  fires **exactly once per broker id**, as that broker finishes, and **never before the
  discovery callback has returned**. Exactly one of `value` / `error` is non-NULL. `value`
  is owned and holds **one** broker at index 0 (free it with
  `kafka_admin_ListTransactionsResult_destroy`). The error is owned.
- Callbacks are **not** serialized on one thread. With a NULL admin, the discovery
  callback runs inline on the caller's thread. An empty pattern `""` is now dropped, the
  same as NULL (Java's `ListTransactionsHandler` does the same).

**Why .NET crashes:** the P/Invoke still has the old argument list, so .NET's single
callback lands in the `by_broker_id_callback` slot and the `user_data` pointer lands in
the `callback` slot. The core then calls through a pointer that is not a function.

**Recommended shape — Option I (D3), Java-exact:** Java's
`ListTransactionsResult.byBrokerId()` is a future of a map of futures: the map completes
when the brokers are known, and each broker's future completes on its own. The new ABI
gives exactly that, so .NET can match it.

1. **P/Invoke**: `AdminClientListTransactionsAsync` takes the extra
   `by_broker_id_callback` parameter; two delegate types
   (`ListTransactionsByBrokerIdCallback(IntPtr brokerIds, int count, IntPtr error, IntPtr userData)`
   and `ListTransactionsCallback(int brokerId, IntPtr value, IntPtr error, IntPtr userData)`),
   each with one **rooted static** instance in `AdminCallbacks`. The submit seam delegate
   (`NativeListTransactionsSubmit`) changes the same way. The P/Invoke is changed, not
   added: **700 stays 700**.
2. **Operation**: a new internal `ListTransactionsAdminOperation : AdminOperation`
   holding the outer `TaskCompletionSource<IReadOnlyDictionary<int, Task<IReadOnlyCollection<TransactionListing>>>>`
   and the per-broker `TaskCompletionSource`s (all `RunContinuationsAsynchronously`).
   DoD §7 note for the commit: Java does this with future chaining; .NET needs one object
   to own both stages, like the other `AdminOperation` subclasses.
3. **Countdown** (the existing `n + 1` scheme, where +1 is the submit token):
   - `SetPendingCallbacks(1)` before submit → discovery + submit token.
   - New internal `AdminOperation.AddPendingCallbacks(int n)` (`Interlocked.Add`). The
     discovery callback calls it **before** releasing its own count, so the count can
     never touch zero while per-broker callbacks are still due. Assert the count is > 0
     when adding.
   - `ReleaseSubmitToken()` after submit returns; `AbandonBeforeSubmit()` if anything
     throws before submit.
4. **Discovery callback**: on error → `KafkaException.FromHandle(error)` faults the outer
   task (all three result views then fail with it), then `ReleaseOne()`. On success →
   copy the ids with `Marshal.Copy` (they are borrowed), create one per-broker
   `TaskCompletionSource` per id, `AddPendingCallbacks(count)`, publish the map
   (`Volatile.Write`; the map is read-only from then on), complete the outer task with the
   map of per-broker `Task`s, then `ReleaseOne()`. `count == 0` → the outer task resolves
   to an **empty map**.
5. **Per-broker callback**: look up the broker's source. A `value` → read its listings at
   index 0 with the existing listing reader, destroy it in a `finally`, complete the
   task. An `error` → `FromHandle(error)` faults that broker's task only. Then
   `ReleaseOne()`. An id the discovery did not announce cannot happen per the header;
   if it does, destroy what arrived and `ReleaseOne()` — never throw across the boundary.
6. **At zero**: the existing `OnAllCallbacksComplete` → `FailUncompleted` sweep faults
   anything never completed, frees the `GCHandle`, and releases the handle ref.
7. **`ListTransactionsResult`**: `ByBrokerId()` returns the outer task.
   `AllByBrokerId()` follows Java's algorithm: wait for the outer map, then complete with
   the full map when every broker has succeeded, or **fail as soon as any broker fails**
   (the current `Task.WhenAll` gather waits for every broker, which is not Java's
   fail-fast). An empty map completes with an empty map (the core does this too; Java's
   own loop would never complete — recorded as a known Java quirk, not copied).
   `All()` stays `Flatten(AllByBrokerId())`. No public signature changes.
8. **Retire** the old single-callback code for this RPC (`OnListTransactions` →
   `CompleteAggregateRpc`, `ListTransactionsOptionalError`, the one-shot
   `SingleAdminOperation` use) where nothing else uses it.
9. **Docs**: fix the P/Invoke remark and the `NativeAdminClient` remark that say a NULL
   pattern and an empty pattern differ (now both mean "no filter", as in Java).

**Option J (alternative):** do what Python's C extension does — join all per-broker
callbacks back into one completion, so `ByBrokerId()` resolves only after every broker
has finished (today's .NET behaviour). Less code, but not Java's shape and not
fail-fast.

**Tests (CP1):**
- Through the **submit seam** (a fake submit that drives the two callbacks, with owned
  errors minted by `kafka_common_Error_new` using real protocol codes):
  discovery → per-broker order; `ByBrokerId()` resolves while a broker is still pending;
  one broker's task completes independently of another; `AllByBrokerId()` / `All()` fail
  fast when one broker fails while another is still pending; a discovery error faults all
  three views with the exact code/message; `count == 0` → empty map and empty `All()`;
  per-broker callbacks from two threads at once; the `GCHandle` is freed and the handle
  ref released after the last callback (existing lifetime helpers); discovery fired
  inline during submit (before the submit token is released) and after it.
- Through the **real native mock**: the discovery error — code 35
  (`UNSUPPORTED_VERSION`) and the exact core message "Not implemented yet" on
  `ByBrokerId()`, `AllByBrokerId()` and `All()`.
- The per-broker **success** path (a real value handle) can only be reached with a real
  broker → the four Docker `list_transactions` scenarios (§7.3). This is a known limit
  of the unit tests (R2).
- Update the 23 existing `ListTransactions` tests to the new shape; keep
  `PublicAdminP8ShapeParityTests` green (the public shape does not change).

### 4.2 F10 — the absent replica in `describeReplicaLogDirs` (CP1, Mode A)

**What changed:** Java's `MockAdminClient.describeReplicaLogDirs` skips a replica of a
topic it does not know, so a Java caller finds that key absent. The core's mock now
reports such a replica with **both** `value` and `error` NULL (only this RPC does this;
every other per-key RPC keeps an explicit error for a missing key).

**Recommended (D4) — Python parity:** when both are NULL, fault that replica's task with
`new KafkaException(-4, "the requested key was not present in the admin RPC's response", isRetriable: false)`
— the code and message the core used before, and exactly what Python's
`_keyed_replica_value_cb` does. The known replica still succeeds, and `All()` fails.
.NET cannot drop the key the way Java does, because `Values` is handed to the caller
before any result arrives. Completing it with an empty `ReplicaLogDirInfo` would invent
data (the reason the old test gives).

**Changes:** handle the both-NULL case in the callback path before
`LogDirMarshal.CopyOutReplicaInfo` is called; rewrite `CopyOutReplicaInfo`'s
"unreachable" doc (it is now reachable, but the new branch stops it being called); fix
the test's remarks (they describe the old core behaviour); assert `Code == -4`,
`IsRetriable == false` and the exact message.

### 4.3 F6 — `updateFeatures` returns its error synchronously (CP2, Mode A)

**What changed:** `kafka_admin_AdminClient_update_features_async` now returns
`kafka_common_Error_t*`. **Non-NULL means nothing was submitted and the callback will
never fire**; the caller owns and frees the error. NULL means submitted; the callback
then fires once per distinct feature. This is the core's version of Java's synchronous
`IllegalArgumentException`.

**Why it is a MUST FIX:** with the old `void` P/Invoke, .NET ignores the returned error
(leaked) and waits for callbacks that never come, so every feature's task **hangs** and
the operation's `GCHandle` and handle ref leak.

**Recommended (D5):**
- P/Invoke and the `NativeUpdateFeaturesSubmit` seam return `IntPtr`.
- After submit: if the result is non-zero → `var ex = KafkaException.FromHandle(err)`
  (frees it), then `operation.AbandonBeforeSubmit()` (frees the `GCHandle`, releases the
  handle ref, no task is handed out), then **throw `ex`** from `UpdateFeatures`. Java
  throws from the call itself; the gRPC servicer already maps a synchronous throw to the
  top-level error.
- Keep .NET's own pre-checks exactly as they are (empty map, null/blank name, null
  update — Java's messages, `ArgumentException`). They run first, so the only errors
  reaching the new path through the public API are the ones .NET cannot see (a repeated
  name from a caller comparer; anything the core adds later).
- Rewrite the remark "the whole-call error the ABI delivers has nowhere to go" and the
  P/Invoke doc.

**Alternative:** fault every feature's task with the error (what Python's C extension
does, to keep its old shape). Not Java's shape.

**Tests:** (a) seam: a fake submit returns an owned error → `UpdateFeatures` throws
`KafkaException` with that exact code and message, nothing is returned, the `GCHandle`
is freed and the handle ref released; (b) real native: a
`Dictionary<string, FeatureUpdate>(ReferenceEqualityComparer.Instance)` holding two
distinct `"f"` string instances → throws with the core's exact message
``feature update at index 1 repeats feature `f` `` and the core's code (read the
`LOCAL_ILLEGAL_ARGUMENT` value from the header enum); without the fix this test hangs, so
wrap it in `TestTimeout`.

### 4.4 F4 — one callback per distinct key: audit all 26 arming sites (CP2, Mode A)

**What changed:** the core now fires **one callback per distinct key** (first
occurrence wins) instead of one per occurrence (the old second, synthetic "not present"
callback is gone). .NET counts callbacks: if .NET arms the countdown with more keys than
the core sees as distinct, the count never reaches zero — the call **hangs** and leaks
the client ref.

Three ways .NET's key count can exceed the core's:
1. **A caller dictionary with its own comparer** that treats two value-equal keys as
   different (STATUS open item (a) — known at `DeleteRecords` and `AlterReplicaLogDirs`,
   which arm with `keys.Count`). Handled here.
2. **Two C# strings that become one C string** (STATUS (c)) → §4.7, CP4.
3. **The core normalizes the key** (undefined ACL codes) → §4.5, CP3.

**The audit:** for each arming site the Actor records one row in the commit message
(where the keys come from; which equality .NET de-dups with; whether it matches the
core's equality; the fix, if any). The sites (line numbers at the merge):
`CreateTopics` 1103 · `DeleteTopics` 1192, 1223 · `DescribeTopics` 1307, 1338 ·
`CreatePartitions` 1518 · `DeleteRecords` 1668 · `DescribeConfigs` 1975 ·
`IncrementalAlterConfigs` 2211 · `DescribeLogDirs` 2306 · `AlterReplicaLogDirs` 2438 ·
`DescribeReplicaLogDirs` 2576 · `AlterPartitionReassignments` 2886 ·
`DeleteConsumerGroups` 3283 · `CreateAcls` 3495 · `DeleteAcls` 3592 ·
`AlterClientQuotas` 3901 · `AlterUserScramCredentials` 4107 · `UpdateFeatures` 4586 ·
`FenceProducers` 4674 · `DescribeTransactions` 4754 · `DescribeProducers` 4853 ·
`ListOffsets` 5418 · `DescribeConsumerGroups` 5787 · `DescribeClassicGroups` 5871 ·
`ListConsumerGroupOffsets` 6082.

**The fix for way 1 (D6, recommended):** where keys come from a caller collection whose
comparer the caller chooses, de-dup them by the **core's** equality (ordinal for
strings, value equality for `TopicPartition` / `TopicPartitionReplica` / `ConfigResource`
/ `ClientQuotaEntity`), keep the **first** occurrence (as the core does), submit only
those, and arm with that count. Exception: `UpdateFeatures` — after F6 the core rejects
a repeated name synchronously, so there is no hang and nothing to change (and §4.3's
test depends on that).

**STATUS (b) is resolved by the merge:** the header said "distinct" for `listOffsets`,
`alterPartitionReassignments` and `updateFeatures`; the core now matches it.

**Tests:** for each site the audit fixes, a `ReferenceEqualityComparer` input with two
value-equal keys completes under `TestTimeout` with one outcome per distinct key; plus
one list-input regression (`DescribeTopics` with `["a", "a"]`) that completes once and
does not hang.

### 4.5 F8 — normalize undefined ACL enum codes (CP3, Mode A)

**What changed:** `createAcls` callbacks are now keyed by the **validated** binding,
where an undefined code (e.g. operation 99) has become `UNKNOWN` — Java's `fromCode`.
`deleteAcls` filters were already keyed this way (the older gap). .NET keys its tasks by
the caller's raw objects, so a lookup misses (the task later fails with the wrong error),
and two inputs that differ only by undefined codes collapse to one core key → **hang**.

**Recommended (D7) — normalize in the constructors** (the P13.2 G2-1 precedent,
`ConfigResource`): `AccessControlEntry`, `ResourcePattern`, `AccessControlEntryFilter`
and `ResourcePatternFilter` turn an undefined `AclOperation` / `AclPermissionType` /
`ResourceType` / `PatternType` value into `Unknown`. In Java an undefined value cannot
exist at all, so this makes .NET match. `DistinctBindings` / `DistinctFilters` then use
the same equality as the core, and both create and delete are fixed. The existing `Any`
checks stay.

**Alternative:** Python's approach — normalize only the lookup key and keep the caller's
objects unchanged. More code, and the public objects still hold values Java cannot hold.

**Tests:** `new AccessControlEntry(…, (AclOperation)99, …).Operation == AclOperation.Unknown`
(one per enum per ctor); `CreateAcls` with an undefined code resolves with the mock's
exact outcome; two bindings differing only by codes 98/99 collapse to one task and
complete (no hang); the same two for `DeleteAcls`.

### 4.6 F7 — send a repeated quota entity, like Java (CP3, Mode A)

**What changed:** Java sends every alteration (a repeated entity included) and its
per-entity future map simply collapses; the core now does the same, with one callback per
distinct entity.

**Recommended (D8):** remove .NET's rejection ("The client quota alterations must not
alter the entity {0} more than once."), send every alteration, key the tasks by distinct
entity, and arm with the distinct count. Keep the null-element guard. Rewrite the two
tests that pin the rejection (`PublicAdminAlterClientQuotasTests.cs` lines ~110 and
~313): a repeated entity is sent and its one task resolves with the mock's exact text
(copy it from the core's mock — Java's mock says "Not implement yet").

### 4.7 (c) — reject strings that cannot cross the ABI (CP4, Mode A)

**The problem:** C strings end at a NUL, and `Encoding.UTF8` turns a lone surrogate into
U+FFFD. So `"a\0b"` and `"a\0c"` both reach the core as `"a"`, and two different lone
surrogates both become `"�"`. Two C# keys become one C key; after F4 that is a
**hang** at every per-key RPC. It also silently turns `"a\0b"` into topic `"a"`.

**Recommended (D9):** before any pin / `GCHandle` / AddRef (ffi §B5 order), reject any
string that is part of a per-key key at the 24 per-key RPCs — topic names (also inside
`TopicPartition` / `TopicPartitionReplica`), config resource names, group ids,
transactional ids, feature names, SCRAM user names, quota entity names, and ACL
principal / host / resource names — when it contains `'\0'` or an unpaired surrogate,
with an `ArgumentException` (the existing null-topic guard precedent). One shared helper;
the exact message is chosen once and asserted (with `ParamName`). The commit lists every
guarded string.

**Alternatives:** strict encoding for **every** string crossing the ABI (also changes
producer/consumer behaviour — broader than this phase); de-dup on the encoded bytes
(silently merges two caller keys into one result); defer (the hang stays).

### 4.8 Non-numeric BROKER / BROKER_LOGGER config names (CP4, tests only)

**What changed:** Java's `nodeFor` is `Integer.valueOf(name)`, so a non-numeric name
throws `NumberFormatException("For input string: \"x\"")`. The core now fails **every**
requested resource of that `describeConfigs` / `incrementalAlterConfigs` call with that
message (it cannot throw from the call), and sends nothing. The core's mock does the same
for its BROKER arms (was "Broker x not found." / "no such broker as x").

**Recommended (D10):** no .NET code change (the binding forwards; adding its own parse
would be logic in the binding). Add tests on the mock: BROKER `"x"` and BROKER_LOGGER
`"x"`, each alongside a TOPIC resource, for both RPCs → every resource faults with the
exact code and message. Fix any xmldoc that says a bad name goes to "any broker".

### 4.9 D15 — nullable cluster id (CP4, Mode A)

`kafka_admin_DescribeClusterResult_cluster_id` can now return NULL (Java's `clusterId()`
is null on the old-broker Metadata fallback). **Recommended:** `ClusterId()` becomes
`Task<string?>` (annotation only), `DescribeClusterMarshal` stops turning NULL into `""`,
and the gRPC servicer sets `ClusterId = clusterId ?? ""` (the proto string cannot be
null; the C server and the Rust test view do the same). A unit test cannot produce a
NULL id (the mock always has one); the reflection test pins the annotation and the mock
test pins the unchanged value.

### 4.10 D16 — `NewTopic` null config values (CP4, Mode A)

Java's `NewTopic.configs(Map)` accepts a null value and sends it; the core now keeps a
NULL from `kafka_admin_NewTopic_put_config`. Today .NET throws
`ArgumentNullException` (ParamName `"s"`, from inside `Encoding.UTF8`).
**Recommended:** `NewTopicMarshal` passes `IntPtr.Zero` for a null value; make
`NewTopic`'s `Equals` / `GetHashCode` / `ToString` null-safe for values; **keep** the
`IReadOnlyDictionary<string, string>?` annotation (changing it to `string?` would warn
every caller passing a `Dictionary<string, string>`), and document that a null value is
sent as Java's null. Test on the mock: `createTopics` with `{"retention.ms": null}`
succeeds, the createTopics config result and `describeConfigs` echo a null value, and
equality/hash do not throw.

### 4.11 D11 — `Error_cause` → `InnerException` (CP5, Mode A, all clients)

`kafka_common_Error_cause` returns the cause as an **owned** copy (NULL when there is
none); walk it repeatedly for a chain. **Recommended:** add
`internal KafkaException(int code, string? message, bool isRetriable, Exception? innerException)`;
in `FromBorrowedHandle`, read the cause and, when non-zero, build the inner exception with
`FromHandle(cause)` (which frees it — recursion walks the chain). +1 P/Invoke.
This touches **every** exception built from a native error (producer and consumer too) —
Java-faithful, but the widest change in the phase, hence its own checkpoint.

Knock-on: P13.1's recorded residual "RA … no `InnerException` (D3)" is **superseded** —
`PublicAdminRemoveMembersFromConsumerGroupTests.cs:440` asserts `Assert.Null(thrown.InnerException)`
and must be updated if that error now carries a cause. The PM records the supersession in
STATUS at close. Tests: a real-native path where the core attaches a cause (e.g. the
admin-client create failure "Failed to create new KafkaAdminClient", or the remove-all
wrap), asserting the inner exception's code and message; the no-cause case stays `null`.

### 4.12 D12 — `Node.IsFenced` (CP5, Mode A)

`describeCluster` with `IncludeFencedBrokers` can return fenced brokers, and today they
look active. **Recommended:** `public bool IsFenced { get; }`, an internal ctor
parameter, `NodeMarshal` reads `kafka_common_Node_is_fenced` (+1 P/Invoke), and
`ToString` becomes Java's `"{host}:{port} (id: {id} rack: {rack} isFenced: {false|true})"`
(lowercase, as Java prints it). `Node` has no value equality in .NET today (pre-existing,
out of scope). Grep for tests that pin `Node.ToString()` and update them.

### 4.13 D13 — `createTopics` config entries with their real source (CP5, Mode A)

`kafka_admin_TopicMetadataAndConfig_config` returns a borrowed `kafka_admin_Config_t*`
(NULL when the metadata is unavailable) whose entries are full `ConfigEntry` handles.
**Recommended:** read it with the existing `ConfigMarshal` reader, so `Source`,
`IsSensitive` and `IsReadOnly` come from the core and `Source` is no longer guessed from
`isDefault`. `Type` stays `ConfigType.Unknown` (Java's `type()` is null here; .NET's
`ConfigType` is not nullable, and the core already returns NULL, which `ConfigMarshal`
maps to `Unknown`). Delete the six flat `TopicMetadataAndConfig_config_*` P/Invokes and
the internal 5-arg `ConfigEntry` ctor if nothing else uses them (it has no Java
counterpart — DoD §7). Count: +1 −6.

### 4.14 D14 — mock seeding functions (recommend **defer**)

`kafka_admin_MockAdminClient_add_topic` / `mark_topic_for_deletion` /
`set_broker_log_dirs` would give .NET's `MockAdminClient` Java's `addTopic` /
`markTopicForDeletion` / builder log dirs. It is new public surface with parallel-array
marshalling, Python has none of it, and nothing in this phase needs it. Leave STATUS (k)
open and do it in a later phase if wanted.

### 4.15 No effect on .NET (recorded so nobody re-checks)

| Commit | Change | Why .NET is unaffected |
|---|---|---|
| `2af7ddae` F11 | `GroupAuthorizationError_group_id` may return NULL | .NET never calls it (§2 #16) |
| `c2d00dad` F3 | `ConfigEntry_type` may return NULL (createTopics entries) | `ConfigMarshal` already maps NULL → `Unknown`; .NET reads createTopics entries through the flat getters until D13 |
| `f8621c48` | Node `Display` prints `rack: null` / `rack: r1` | Only inside core-built messages; no .NET test pins the old text (§2 #17) |
| `72bdcf4e` | describeAcls unknown-filter message text | .NET passes the core's text through; nothing pins it |
| `6d86635d` | deleteAcls FilterResult, SCRAM `users()`, empty txn pattern docs | Header text now matches what .NET already does (P13.2 G4-1, G4-2); the pattern doc is §4.1 step 9 |
| `e72135db` / `7780f0b8` / `d6c188d5` | new symbols | optional, D11–D14 |
| `transaction_manager.rs`, `coordinator_strategy.rs` | `group_id` becomes `Option` | internal to the core |

### 4.16 Doc drift

| Where | What | CP |
|---|---|---|
| `NativeMethods.Admin.cs` list_transactions P/Invoke remark; `NativeAdminClient` ListTransactions remark | "NULL and empty pattern differ" → both mean no filter | CP1 |
| `LogDirMarshal.CopyOutReplicaInfo` | "unreachable" | CP1 |
| `NativeAdminClient.UpdateFeatures` remark + P/Invoke doc | "the whole-call error … has nowhere to go" | CP2 |
| `h:NNNN` header line cites (115 in 34 `.cs` files; 15 in `design/current`; 10 in `.claude/` + `CLAUDE.md`) | every line number shifts with the merged header (STATUS (d)) | CP6 + close, per D17 |
| `design/current/STATUS.md` | (a), (b), (c), (d); the P13.2 "recorded as known" G4-1 line; the P13.1 RA "no InnerException" residual (if D11) | close (PM) |

**D17 (recommended):** in a doc-only **CP6**, the Actor replaces every `h:NNNN` cite in
the `.cs` files with the **symbol name** it points at (e.g. "header,
`kafka_admin_AdminClient_update_features_async`"), which survives future merges. The PM
does the same in `design/current` at close. The 10 cites in `.claude/` rules and
`bindings/dotnet/CLAUDE.md` are **not** edited by agents (rule files) — they are listed
for the user.

---

## 5. Checkpoints

| CP | Items | Why grouped | Blast radius |
|---|---|---|---|
| **CP0 (PM)** | the merge (§3) | must come first | the whole branch |
| **CP1** | F5, F10 | both make the suite red after the merge; CP1 ends green, unfiltered, on both TFMs | listTransactions, describeReplicaLogDirs |
| **CP2** | F6, F4 audit (+ STATUS (a)), docs | same machinery (the countdown and submit); F6's real-native test relies on the audit leaving updateFeatures alone | updateFeatures; up to 26 arming sites |
| **CP3** | F8, F7 | ACL / quota family, each contained to its ctors or one RPC | createAcls, deleteAcls, alterClientQuotas, the ACL value types |
| **CP4** | (c), non-numeric config tests, D15, D16 | small, independent input/value fixes | per-key inputs; describeCluster; NewTopic |
| **CP5** | D11, D12, D13 (whichever are approved) | new symbols; D11 touches every client's errors, so it goes last | every `KafkaException` from native; `Node`; createTopics results |
| **CP6** | D17 doc-only cite refresh | pure doc; last so line moves in CP1–CP5 are already in | comments only |
| **Close (PM)** | Docker gate, STATUS/design docs, archive `COMMENTS.DONE.86.md`, memory | — | docs |

If CP1 grows too big for one session (it is the largest), the Actor may split it into
CP1a (P/Invoke + bridge + operation) and CP1b (result views + tests), each its own
commit; the full suite is green only after CP1b.

---

## 6. Public API surface changes (each pinned by reflection)

| Item | Change | Reflection test |
|---|---|---|
| F5 | none (`ListTransactionsResult` keeps `ByBrokerId` / `AllByBrokerId` / `All`) | the existing shape-parity test stays green |
| F6 | none public (`UpdateFeatures` may now throw `KafkaException` — xmldoc) | behavioural |
| F7 | contract: a repeated entity is accepted (xmldoc) | behavioural |
| F8 | the four ACL ctors normalize undefined codes (contract/xmldoc) | behavioural |
| (c) | per-key string inputs reject NUL / lone surrogate (xmldoc `<exception>`) | behavioural |
| D11 | `KafkaException.InnerException` populated from native causes | behavioural |
| D12 | `Node.IsFenced` (new public property); `Node.ToString` format | property type/accessors; `ToString` behavioural |
| D13 | `ConfigEntry` 5-arg internal ctor removed (internal only) | the public ctor count stays 2 |
| D15 | `DescribeClusterResult.ClusterId()` → `Task<string?>` | `NullabilityInfoContext` reports the generic argument nullable (net8.0+) |
| D16 | contract: a null config value is sent as null (xmldoc; annotation unchanged) | behavioural |

---

## 7. Gates

### 7.1 Per checkpoint (Actor; verified by the PM per checkpoint, reviewed by the Critic at the end)
1. `cargo build --features ffi` (Debug) first. The header is **byte-identical** to the
   post-merge header.
2. Mode A: `git diff <merge>..HEAD -- src/ cbindgen.toml generator/ build.rs Cargo.toml Cargo.lock tests/ bindings/python bindings/c`
   is **empty**.
3. `internal static extern` count: **700** at CP1–CP4 and CP6; CP5 = 700 + 1 (D11)
   + 1 (D12) + 1 − 6 (D13) = **697** if all three are approved (report the actual
   arithmetic otherwise).
4. `~/.dotnet/dotnet build Confluent.Kafka.sln` (Debug): `0 Warning(s)`, `0 Error(s)`,
   six TFM outputs; the `nm` freshness check prints `1`.
5. Filtered `dotnet test` on the touched classes, net10.0: explicit `Passed:` counts,
   `Test Run Aborted` = 0.
6. **Full suite on net10.0 and net8.0**, Debug, every checkpoint: totals reported,
   `Failed: 0`, no abort. (Before CP1 is done: the `!~ListTransactions` filter with
   exactly the 1 known failure — §3.)
7. `dotnet format Confluent.Kafka.sln --verify-no-changes` (and `grpc-server` when
   touched); `grpc-server` builds.
8. The commit message carries the checkpoint's evidence (counts, the F4 audit table in
   CP2, the guarded-string list in CP4).

### 7.2 Final (after the last checkpoint)
9. Full suite net10.0 **and** net8.0: `Failed: 0`, no abort, totals reported against the
   pre-merge **2434** plus the new tests.
10. Critic 86 clean on the full CP0–CP6 range.

### 7.3 Docker (PM, at close — proposed as D18)
- `docker info` first. If Docker is down: **CI-pending**, flagged loudly (R2).
- Build a **fresh** linux/amd64 `.so` and **prove it is fresh**:
  `nm -D <staged .so> | /usr/bin/grep -c kafka_common_Error_cause` = 1 (absent before the
  merge). Rebuild the **sync** .NET gRPC image. Follow the P13.2 close recipe
  (PM memory `project_m8p1_dotnet_grpc_local_gate_recipe.md`).
- **Count scenarios with `cargo test … -- --list` before running** (P13.2 lesson: its
  estimate was off by one per family). Counts from the macro registrations at the merge,
  to be confirmed by `--list`:
  - **Touched families, all three arms** (`__rust` and `__grpc_python` as the oracle
    for the merged core, `__grpc_dotnet` for this phase): `admin_transactions_test` 11
    (the **4** `list_transactions_*` scenarios are the only proof of F5's per-broker
    success path), `admin_acls_test` 5, `admin_quotas_test` 3, `admin_features_test` 4,
    `admin_log_dirs_test` 4, `admin_cluster_configs_test` 7, `admin_topics_test` 9 —
    **43** per arm.
  - **Other admin families, `__grpc_dotnet` regression:** `admin_scram_test` 2,
    `admin_partitions_records_test` 7, `admin_elections_reassignments_offsets_test` 8,
    `admin_delegation_tokens_test` 2, `admin_group_offsets_test` 5, `admin_groups_test`
    12 — **36**. (P13.2: 41 + 38 = 79 admin arms; same total.)
  - **If D11 or D12 is taken:** the non-admin sync `__grpc_dotnet` arms (P13.2: 34 — 14
    consumer, 19 producer, 1 multilanguage-admin) as regression, **excluding** the three
    `producer_transactions_test` arms (known, pre-existing `Unimplemented`).
- Caveats recorded with the result: the 36 `_async` arms are not run (the async image is
  not rebuilt); the three `producer_transactions_test` arms are excluded; no scenario
  exercises the new edge cases (duplicate keys, NUL, undefined ACL codes, null cluster
  id) — adding scenarios is cross-binding harness work and out of scope.
- Report `N passed; 0 failed` per filter, store the logs in the phase folder's untracked
  area (excluded via `.git/info/exclude`), and record the numbers in STATUS.

> **Close-out record (2026-09-30).** The scenario counts above were right: `--list` shows
> 43 per arm for the touched families, 36 for the other admin families, and 34 for the
> non-admin sync set. The gate ran on `cfb71be6` and every filter passed:
> - touched families, 129 passed; 0 failed (43 × `__rust` / `__grpc_python` / `__grpc_dotnet`,
>   the 4 `list_transactions_*` `__grpc_dotnet` arms included);
> - other admin `__grpc_dotnet`, 36 passed; 0 failed;
> - non-admin sync `__grpc_dotnet`, 34 passed; 0 failed.
>
> The `.so` was proven fresh: `kafka_common_Error_cause` is exported, and all 697 of the
> binding's entry points resolve. The sync .NET image was rebuilt from it. So was the sync
> Python image, because PR #201 changed `bindings/python`, and an image built before the
> merge would not have been a valid oracle.
>
> **Premise errors in this plan, found during the phase:**
> - **F4 (row above, §4.4).** The symptom is a **leak, not a hang**. `KeyedAdminOperation`'s
>   Tasks are keyed by the core's comparer, so every Task still completes. Only the countdown
>   is stranded, together with the `GCHandle` and the client reference (CP2 commit).
> - **§4.8.** The core **mock** does not fail every resource. Only the BROKER resource gets
>   (-3, `For input string: "x"`). BROKER_LOGGER is refused with (35, "Not implemented yet"),
>   and TOPIC is answered normally. Only the real client fails every resource (CP4 commit).
> - **D16.** .NET did not throw `ArgumentNullException("s")` from `Encoding.UTF8`. It threw its
>   own `ArgumentException` from the `CreateTopics` precheck, which CP4 removed.
> - **D17.** The "10 cites in `.claude/` + `CLAUDE.md`" are **3** rule-file cites. The other 7
>   are in agent-memory notes. The 15 in `design/current` are right, plus 2 bare `:NNNN`
>   continuations, and the Manager replaced all of them at close.

---

## 8. Decisions for the user

| # | Decision | Recommendation | Why |
|---|---|---|---|
| **D1** | Cadence | ~~P13.1/P13.2 style: CPs back to back, Critic after each CP until clean, no stop between CPs, PM hands back at the end~~ **Amended by the user (2026-09-30):** CPs back to back with no Critic between them; the PM verifies each CP's gates; one Critic review over CP0–CP6 together, then fix → re-review until clean; PM hands back at the end | Worked twice; the user still gates the start ("don't trigger until I tell"). Amendment: "Let's run critic only after CP6. That way critic will review changes for CP0–CP6 in one go." |
| **D2** | Who merges | The PM, as CP0, after re-checking the PR head | P13.1 precedent; keeps every Actor commit Mode A |
| **D3** | F5 shape | **Option I** — Java-exact independent per-broker tasks, fail-fast `AllByBrokerId` | The new ABI exists to allow exactly this; Option J (Python's join) keeps .NET off Java's shape |
| **D4** | F10 absent replica | **Python parity** — fault with −4 and the old message | Java's "drop the key" is impossible once `Values` is handed out; a default value would invent data |
| **D5** | F6 error | **Throw `KafkaException` synchronously**; keep .NET's own pre-checks | Java throws from the call; the servicer already handles it. Faulting every task is Python's workaround, not Java's shape |
| **D6** | F4 audit scope | **Fix every caller-comparer site** (closes STATUS (a)) | After F4 these are real hangs, and the fix is the same small change everywhere |
| **D7** | F8 | **Normalize in the four ACL ctors** (fixes create + delete) | G2-1 precedent; Java cannot hold an undefined code at all |
| **D8** | F7 | **Send a repeated entity**, like Java | The core and Java both accept it; .NET's rejection is now the only divergence |
| **D9** | (c) | **Reject NUL / lone surrogate in per-key keys** with `ArgumentException` | Stops the hang and the silent `"a\0b"` → `"a"`; limited to admin keys so producer/consumer do not change |
| **D10** | Non-numeric broker names | **Accept the core's behaviour, add tests** | A .NET-side parse would be logic in the binding |
| **D11** | `Error_cause` → `InnerException` | **Include** (CP5); supersedes P13.1's "no InnerException" residual | Java-faithful and cheap; its own CP because it touches every client |
| **D12** | `Node.IsFenced` | **Include** (CP5) | Cheap; `IncludeFencedBrokers` already exists, so fenced brokers are currently indistinguishable |
| **D13** | createTopics config source | **Include; keep `Type` = `Unknown`; delete the six flat getters** | Real `Source` instead of a guess; removes dead code |
| **D14** | Mock seeding functions | **Defer** | New public surface, Python lacks it, nothing needs it now |
| **D15** | Null cluster id | **Include** — `Task<string?>`, servicer maps null → `""` | Java's `clusterId()` is nullable; two-line change |
| **D16** | NewTopic null config values | **Include at runtime, keep the annotation** | Java sends null; today .NET throws an odd `ArgumentNullException("s")`; changing the annotation would warn callers |
| **D17** | `h:NNNN` cites | **Replace with symbol names** (CP6 for `.cs`, PM for `design/current`); list the 10 rule-file cites for the user | Line numbers break on every merge; symbol names do not |
| **D18** | Docker scope | 43 touched scenarios × 3 arms + 36 other admin `__grpc_dotnet`; non-admin 34 (−3) only if D11/D12 | The 4 listTransactions scenarios are F5's only real per-broker proof |

---

## 9. Risks

- **R1 Red suite after the merge.** Mitigated by §3's filtered baseline and CP1 going
  first. The Critic is told the pre-CP1 red is known.
- **R2 F5's per-broker success path is not unit-testable** (the mock refuses
  listTransactions and a value handle cannot be fabricated). The seam covers ordering,
  errors, fail-fast and lifetime; only Docker proves success. If Docker is down, F5 closes
  as CI-pending and the STATUS entry says so.
- **R3 Count added after zero.** If `AddPendingCallbacks` ran after the discovery
  callback released its own count, the operation could be freed while per-broker
  callbacks are still due (use-after-free of the `GCHandle`). Mitigated by the order in
  §4.1 step 3 and tests with inline and deferred discovery.
- **R4 Unserialized callbacks.** The per-broker map must be read-only after publish and
  every completion must be `TrySet*`; tested with two concurrent per-broker callbacks.
- **R5 Missed arming site.** The CP2 audit table covers all 26; the Critic checks each
  row against the code.
- **R6 D11 blast radius** (every native error, all clients). Full suite on both TFMs
  plus the non-admin Docker arms.
- **R7 Stale native library** giving a false pass (P13.1 lesson). The `nm` freshness
  check is a gate at every CP and at Docker staging.
- **R8 The PR head moves before approval.** CP0 step 1 re-checks; if moved, stop and
  re-plan.
- **R9 Actor context budget on CP1.** Allowed split into CP1a/CP1b (§5).
- **R10 Header vs behaviour.** Where the merged header and the core disagree, the
  Critic raises it to the PM as a user decision (no Rust change, no note to Pratyush).

---

## 10. Out of scope (sibling observations, not added)

- The `ProducerSubmitHandleRefTests` heap-budget noise and the `DisposeAsync`
  completion-ordering race in `OperationCompletionSource.Complete`.
- .NET's empty-map `updateFeatures` guard also fires on `MockAdminClient`, where Java's
  mock accepts an empty map (pre-existing; §2 #19).
- `Node` has no value equality in .NET, while Java's `equals`/`hashCode` include
  `isFenced` (pre-existing).
- STATUS open items (e)–(m), except where this plan says otherwise.
- D14 (mock seeding) if deferred; the async gRPC image; new Docker scenarios; any Rust,
  C or Python change; C FFI/Python binding work; Tier 4.
