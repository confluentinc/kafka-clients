# M15/P13.1 — .NET Admin: adopt PR #201's single-callback ABI for three group RPCs

**N = 84.** Branch `prashah_dev_dotnet_binding`. **Base = merge `54ed917c`**
(merge of PR #201 head `8796564b` into `76629aea`; not pushed).
**Mode A** (C#-only). **APPROVED 2026-09-29** (D1–D7 as recommended; D8 cadence
modified by the user — see §1). Three checkpoints run back to back; `dotnet-critic`
84 reviews **after each checkpoint** until clean before the next starts.

N derivation: unfiltered `find -name 'COMMENTS*.md'` over the repo (excluding
`target/`, `kafka/`): highest used = 83 (`bindings/dotnet/COMMENTS.83.md`),
nothing ≥ 84 anywhere. The root file `COMMENTS.DONE.69.md` added by the merge is
the root repo's own sequence and does not collide.

---

## 0. Why this phase exists

`8796564b` changed exactly **3 callback typedefs** (verified by diffing every
`extern "C" fn` / `pub type` in `src/ffi/admin.rs` + `src/ffi/common.rs`
pre/post: 3 typedefs changed, 7 functions added, 0 removed, 0 fn signatures
changed):

| Typedef | Old | New |
|---|---|---|
| `…_alter_consumer_group_offsets_callback_t` | `(topic, partition, error, ud)` × N | `(AlterConsumerGroupOffsetsResult_t*, error, ud)` × **1** |
| `…_delete_consumer_group_offsets_callback_t` | same as alter | `(DeleteConsumerGroupOffsetsResult_t*, error, ud)` × 1 |
| `…_remove_members_from_consumer_group_callback_t` | `(group_instance_id \| NULL, error, ud)` | `(RemoveMembersFromConsumerGroupResult_t*, error, ud)` × 1 |

New contract (header ~1157-1257): fires **exactly once** (empty input and
removeAll included); exactly one of `result`/`error` non-null, callback owns it;
submit failure / NULL admin fire **inline on the calling thread** before
`_async` returns (`admin_async_future_op`, `src/ffi/admin.rs:769`).

Added: `{Alter,Delete}ConsumerGroupOffsetsResult_{partition_result,all}`,
`RemoveMembersFromConsumerGroupResult_{member_result,all}`, `kafka_common_Error_clone`.

**P/Invoke names are unchanged, so .NET compiles (0W/0E) but is broken at
runtime** — confirmed after the merge, filtered net10.0 runs:

| Class | Result |
|---|---|
| `PublicAdminAlterConsumerGroupOffsetsTests` | **Test Run Aborted** — host crash, Rust UB-check panic in `alloc::ffi::c_str` on `kafka-admin-callback-dispatcher` |
| `PublicAdminDeleteConsumerGroupOffsetsTests` | **Test Run Aborted** |
| `AdminP9PerKeyStage6Tests` | **Test Run Aborted** |
| `PublicAdminRemoveMembersFromConsumerGroupTests` | 16/16 pass — *by accident* (mock always fails the whole future → NULL result looks like the old NULL-key path); every N>1 call leaks its GCHandle + client `DangerousAddRef` (countdown armed at N+1, one callback) |
| `AdminP4ReaderWiringTests` / `AdminP9CountdownTests` | 31/31, 7/7 (no ABI callback driven) |

The other **43 RPCs are untouched** by the ABI change and by this phase.

## 1. Standing constraints (relay verbatim to Actor + Critic)

Ruled 2026-09-29: P12's "token-constrained / terse code" relaxation and its
"hard stop after every checkpoint" gate do **not** carry over. Normal repo
conventions (CLAUDE.md, `ffi-marshalling.md`, doc-comment habits) apply to Actor
and Critic alike.

1. **Per-checkpoint Critic cycle** (no user gate between steps or checkpoints):
   1. `dotnet-actor` 84 implements CPk and commits.
   2. `dotnet-critic` 84 reviews **that checkpoint's commits** (and the premises
      they rely on) and writes `bindings/dotnet/COMMENTS.84.md`.
   3. Findings → the Actor fixes them with `fixup!` commits referencing the
      original commit, and moves resolved items to `COMMENTS.DONE.84.md`.
   4. The Critic re-reviews; repeat until it reports nothing.
   5. Then CPk+1 starts. CP1 → CP2 → CP3 run back to back.
2. One checkpoint = one commit (+ its fixups) = one resumable Actor session
   (context budget, per P5's crashes — not a user gate).
3. Stage by explicit path only (never `git add -A` / `.` / `commit -a`); never
   stage the binding-root `COMMENTS.DONE.84.md` (§8.4) or unrelated untracked
   files. Do not push. Do not edit `PendingAdminClientFindingsForDotnet.md` or
   `Dotnet-AdminClient-Findings-Workflow/`.
4. Critic ground truth: the C ABI header + the Kafka Java public API
   (`bindings/dotnet/CLAUDE.md` §8.2), not Rust internals.
5. Env traps: `dotnet` = `~/.dotnet/dotnet`; clobbered PATH can hide cargo (prefix
   the PATH export); `grep` may be ugrep → use `/usr/bin/grep`; no `sed`; `cat`
   may be `bat` → `/bin/cat`; zsh doesn't word-split; a zero-match libtest filter
   exits 0 → always report counts; an **aborted** `dotnet test` prints no
   `Passed!` (it exited 1 here, but do not rely on the exit code) → grep
   `Test Run Aborted`; failed build + `--no-build` prints stale `Passed!` → assert
   `0 Error(s)`; build and test in the **same** configuration (Debug ↔
   `target/debug`); never a bare `cargo build` (use `--features ffi`); bound every
   tool's output (`| tail -n`, `head`, filtered test runs, logs to the scratchpad).

## 2. Approach (D1 (b′), ruled 2026-09-29)

Move the three RPCs from mechanism **1c** (`FanInAdminOperation`, N-callback
countdown) to mechanism **2** (`SingleAdminOperation` + one callback), the exact
shape `ElectLeaders` already uses (`OnElectLeaders → CompleteAggregateRpc`,
`AdminCallbacks.cs:2738-2795`).

In the one callback:
- `(NULL, error)` → `SetException(KafkaException.FromHandle(error))` (owned).
- `(result, NULL)` → walk `_count` / `_get_topic|_get_partition` (or
  `_get_group_instance_id`) / `_get_error(i)` into the per-key map, **plus**
  read `_all(result)` once (owned → `FromHandle`), resolve the Task with
  `(IReadOnlyDictionary<K, KafkaException?> PerKey, KafkaException? All)`
  (tuple — precedent `SingleAdminOperation<(Valid, Errors)>`, so no new type),
  then `*_destroy(result)` exactly once.

Why the walker is not "re-implementing Java": `_get_error(i)` is populated by the
**core's** `resolved_partition_result` / `resolved_member_result`
(`src/ffi/admin.rs:12010`, `:12518`) — the same derivation `_partition_result` /
`_member_result` return — and the header states they carry the same error. So
per-key outcomes are already core-owned; only `All()` is still derived in C#
today, and `_all` replaces that.

Result classes (`Admin/{Alter,Delete}ConsumerGroupOffsetsResult.cs`,
`RemoveMembersFromConsumerGroupResult.cs`) keep their public surface; internal
ctor takes the tuple Task. `All()` = await, throw `All` if non-null.
`PartitionResult`/`MemberResult` = keep the C# guards (D2), else await, throw the
stored per-key error if non-null. Delete: the C# `All()` derivations, the
"not included in the response" branches (dead: the walker covers every
requested key), the removeAll "Encounter exception" loop (G5-7).

## 3. Checkpoints

Run back to back. Each closes only when its §1.1 Critic cycle is clean; the next
starts immediately after.

### CP1 — Alter + Delete group offsets
- `AdminCallbacks.cs`: both delegates → `(IntPtr result, IntPtr error, IntPtr userData)`;
  new trampolines via a sibling of `CompleteAggregateRpc` that also reads `_all`
  (do not fork the ownership/`finally` logic: destroy result, `FailUncompleted`,
  `FreeGcHandle`); key readers + count/destroy statics, static-rooted delegates.
- `NativeMethods.Admin.cs`: `+2` DllImports (`…Result_all` × 2); fix the two
  `_async` doc comments (STATUS Critic-83 note (e): they describe the joined sync
  sibling).
- `NativeAdminClient.cs` alter (~3011-3065) / delete (~3171-3213): switch to
  `SingleAdminOperation`; **remove `SetPendingCallbacks` and `ReleaseSubmitToken`**
  (R1); keep `AbandonBeforeSubmit` in `catch`.
- Tests (§5 rows 1-6).
- Gate: §6 per-checkpoint.

### CP2 — RemoveMembersFromConsumerGroup
- Delegate + trampoline as CP1 (`+1` DllImport `…Result_all`); removeAll: `_count`
  is 0, **`_all` is the only carrier of a partial failure** (R4).
- Submit path (~3372-3422): `SingleAdminOperation`, drop the mode-dependent
  countdown. Removes the N>1 leak.
- Tests (§5 rows 7-10).
- Gate: §6 per-checkpoint; **full suite green from here** (no remaining crash).

### CP3 — delete dead machinery, docs, final gates
- Delete `FanInAdminOperation`, `CompletePerKeyFanIn`,
  `CompleteFanInWholeOperation` (grep: their only users are these three RPCs +
  `AdminP9CountdownTests`' two `FanIn_*` tests).
- Doc sweep, scoped to the three RPCs only (other 43 still per-key — do not touch
  their "per-key" wording): shape-4c remarks in `AdminCallbacks.cs` (~292, 304,
  455, 902-928, 2512-2580), `NativeMethods.Admin.cs` doc comments, the three
  Result classes' remarks, `IAdmin.cs` remarks for these RPCs,
  `grpc-server/AdminServiceImpl.cs` ~1515-1519/1556 comment (D6).
- `.claude/rules/ffi-marshalling.md:2008` — retire sub-shape 4c (D5).
- `design/current/STATUS.md` entry. At close the Manager archives a copy of
  `COMMENTS.DONE.84.md` (if any fix cycle ran) under this phase folder; the
  binding-root copy is never staged (§8.4).
- Final gate: §6 final.

## 4. User-visible behaviour changes (all verified against Java + header)

| # | RPC | Before (pre-merge .NET) | After |
|---|---|---|---|
| F4 | alter, delete | empty input: `All()` succeeds at the submit boundary | waits for the real outcome; against the mock faults with `UNSUPPORTED_VERSION` "Not implement yet" / "Not implemented yet" (Java-faithful) |
| F5 | alter | whole-call failure: `All()` throws "Failed altering group offsets for the following partitions: [..]" | rethrows the original error unchanged (Java `thenApply`) |
| G5-4 | alter | `PartitionResult(unrequested)` after whole failure → `ArgumentException` "was not attempted" | the whole-call error (Java checks `throwable` first) |
| RA | remove, removeAll partial failure | `Code` = member's code (e.g. 25), `IsRetriable` = member's | `Code` = **-1**, `IsRetriable` = false, same message "Encounter error when trying to remove: MemberIdentity(..)"; no `InnerException` (ABI cannot read `source`) — D3 |
| ORD | alter `All()`, remove `All()` | ≥2 failing keys: alter's first failure + partition list in callback-arrival order; remove's first failure in `Members` order | core order (topic→partition; group-instance-id). Delete already sorted — unchanged |
| G5-7 | remove | dead "Encounter exception" branch | removed; core text says "error" (CLAUDE.md §2) |

Unchanged: public API surface; per-key messages/codes; C# guards' types and text
(D2); requested-but-absent-from-response keys still surface as `KafkaException`
from the core (pre-existing; P9's fan-in delivered the same core error).
`kafka_common_Error_clone` is **not needed** by .NET (one Task per call).

## 5. Tests (inventory → disposition)

| # | File (count today) | Disposition |
|---|---|---|
| 1 | `PublicAdminAlterConsumerGroupOffsetsTests` (6) | invert `…SurfacesTheMocksDocumentedRefusal` to the mock's text unchanged (F5); rewrite derivation tests (`All_ListsEveryFailedPartition…`, `AFaultedFuture…`) against the tuple ctor: `All()` rethrows stored `All`, per-key rethrow, unrequested guard after success vs after whole failure (G5-4) |
| 2 | `PublicAdminDeleteConsumerGroupOffsetsTests` (7) | same; `PartitionResult_PartitionNotInResponse…` and `All_ReportsOnlyTheFirstFailing…` become "stored outcome rethrown" |
| 3 | `AdminP9PerKeyStage6Tests` (11) | alter/delete rows: delete old-shape trampoline tests; `EmptyConsumerGroupOffsetRequests_ResolveAtTheSubmitBoundary` **inverted** (F4) |
| 4 | new: empty input | mock: `All()` faults with the refusal, Task not completed at submit return |
| 5 | new: inline submit failure (real native) | via the internal `submit` seam, call the real P/Invoke with `IntPtr.Zero` admin (and, for alter, a negative offset): Task already faulted on return with the core's message, GCHandle freed exactly once, client disposes normally (precedent `UnknownOpTypeCode_DrivesTheRealInlineCallbackPath`, `InlineCallback_FreesTheGcHandleExactlyOnce`) |
| 6 | new: lifetime | seam that does not fire: Task pending + GCHandle alive after submit returns (catches a leftover `ReleaseSubmitToken`); then fire once → freed. (`MockAdminClient_timeout_next_request` does not reach these three — the Rust mock fails them unconditionally, `src/admin/mock_admin_client.rs:1634/1650/1683`.) |
| 7 | `PublicAdminRemoveMembersFromConsumerGroupTests` (16) | rewrite `MemberResult_MemberMissingFromResponse…`, `All_NonRemoveAllMode…`, `All_RemoveAllMode_CompletesOnEmptyResolvedMap`; keep the synchronous-guard tests |
| 8 | `AdminP9PerKeyStage6Tests` remove rows | delete NULL-key tests; replace `…ArmsTheDistinctCount` with "N>1 members: one callback, GCHandle freed, client dispose not blocked" |
| 9 | new: removeAll | `All()` carries `_all`'s outcome; wiring test asserts the trampoline captures `…_all` (reflection pattern of `AdminP4ReaderWiringTests`) |
| 10 | `AdminP4ReaderWiringTests` (3 `*OptionalError_CapturesItsOwnErrorAccessor`) | **keep** (readers reused); add 3 `_all` wiring tests |
| 11 | `AdminP9CountdownTests` (7) | delete the 2 `FanIn_*` tests at CP3; keep 5 base-countdown tests (still used by 1a/1b) |
| 12 | optional (D7) | test-only DllImport of the 3 **sync** entry points to get a real handle (mock → whole-error content), fed straight into the new trampolines: exercises walker + borrowed `_get_error` + owned `_all` + single destroy on a real native handle |

Every exact-message assertion re-derived from the header doc text (§0), never
paraphrased.

**Unreachable in-process:** the `(result, NULL)` branch with real per-key
successes/errors (the Rust mock always fails the whole future; no ABI constructor
for a populated result). Covered only by the Docker multilanguage `__grpc_dotnet`
arm: `alter_consumer_group_offsets_and_resume`,
`delete_consumer_group_offsets_on_inactive_group`,
`delete_consumer_group_offsets_on_active_group_errors`,
`remove_one_member_from_consumer_group`, `remove_all_members_from_consumer_group`,
`remove_members_rejects_an_explicitly_empty_selection`.

## 6. Gates

Per checkpoint:
1. `cargo build --features ffi` (Debug) first; header has the 3 new typedefs.
2. Mode A: `git diff 54ed917c..HEAD -- src/ cbindgen.toml generator/ bindings/python bindings/c` **empty**.
3. `internal static extern` count: 697 → 699 (CP1) → 700 (CP2) → 700 (CP3).
4. `~/.dotnet/dotnet build Confluent.Kafka.sln` (Debug): `0 Warning(s)`, `0 Error(s)`, six TFM outputs.
5. Filtered `dotnet test -f net10.0 --no-build` on the touched classes: explicit `Passed:` counts, `Test Run Aborted` = 0. CP1: the alter/delete classes and Stage6's alter/delete rows must not crash; RemoveMembers (and Stage6's remove rows) stay on the old ABI until CP2, so their post-merge misbehaviour is expected at CP1 and is **not** a CP1 finding.
6. `dotnet format Confluent.Kafka.sln --verify-no-changes`.

Final (CP3), in addition:
7. Full `dotnet test -f net10.0` **and** `-f net8.0`, Debug, `Failed: 0`, `Test Run Aborted` = 0, totals reported (baseline pre-merge 2317/2317 net10.0).
8. grpc-server builds + `dotnet format` if touched.
9. `docker info` first; if up, run the six §5 scenarios on `__grpc_dotnet` (and `__grpc_python` as oracle) per the M8/P1 local recipe. If down, **CI-pending** — flagged loudly, since it is the only real-broker coverage of the success path.
10. Critic 84 clean on CP3 (every checkpoint closes only when its Critic cycle is clean — §1.1).

## 7. Decisions — **RULED 2026-09-29**

D1–D7 approved exactly as recommended below. **D8 was modified by the user**
(see D8). Recommendations are kept verbatim as the record of what was ruled.

- **D1 — result derivation.** Rec: **(b′)** walker for per-key (existing
  `_count/_get_*` P/Invokes + existing borrowed readers) + `_all` (+3 DllImports).
  Alternatives: (a) walker + keep C# `All()` — **not viable as-is**: in removeAll
  mode `_count` = 0 and a partial failure is visible only through `_all`, so (a)
  silently reports success unless it also calls `_all`; and it keeps C# logic §2.6
  forbids. (b) per-key `_partition_result`/`_member_result` + `_all` (+6
  DllImports): same outcomes as (b′), marshals every key back into native, and
  retires the tested walker readers.
- **D2 — keep the C# guards.** Rec: **keep all four** (Delete/RemoveMembers
  "not in the original request", RemoveMembers removeAll-mode, Alter "was not
  attempted"). Delete/RemoveMembers are *mandatory*: Java throws them
  synchronously before the future exists
  (`DeleteConsumerGroupOffsetsResult.java` / `RemoveMembersFromConsumerGroupResult.java`
  first lines of `partitionResult`/`memberResult`), so no handle exists yet.
  Alter's is post-await in Java; routing it through `_partition_result` would
  need a retained native handle (P10 Option L machinery) and would turn
  `ArgumentException` into `KafkaException` (`FromHandle` always builds
  `KafkaException`).
- **D3 — removeAll `Code = -1`, no inner cause.** Rec: **accept** for P13.1
  (Python has the same limit; Java's outer is a plain `KafkaException` with no
  code of its own, so -1 is shape-faithful; only `InnerException` is missing).
  Separately propose to Pratyush a general borrowed
  `kafka_common_Error_source(const Error*)` accessor (Mode B, benefits every
  binding's `getCause()`), **not** in this phase.
- **D4 — accept the §4 behaviour changes.** Rec: **accept all** (each is
  Java-faithful or core-owned).
- **D5 — `ffi-marshalling.md` 4c.** Rec: **authorize a direct edit** (P9
  precedent) retiring sub-shape 4c and listing these three with the hookless
  one-shot family. Otherwise the rule stays stale and STATUS records it.
- **D6 — Critic 83 Finding 1** (servicer drops a whole-call error at zero keys,
  `AdminServiceImpl.cs` ~1516-1564). Now fixable (F4 is fixed underneath), but
  Python's servicer has the identical gap (returns `{}`), and the servicer is not
  testable in-process. Rec: **fix only the now-false comment in CP3; defer the
  behaviour change** to a cross-binding harness item (Python + .NET together).
- **D7 — test-only sync-entry DllImports** (§5 row 12). Rec: **yes** — new but
  narrow precedent (test project only); otherwise no in-process test runs the new
  trampolines' `(result, NULL)` branch on a real handle, and the phase exists to
  fix memory corruption there.
- **D8 — cadence.** Rec was: 3 user-gated checkpoints, Critic once at the end.
  **RULED (modified):** 3 checkpoints run back to back with **no** user gate; the
  Critic runs after **each** checkpoint and the fix/re-review cycle repeats until
  clean before the next checkpoint starts (§1.1). The P12 token-constrained
  relaxation is dropped.

## 8. Risks

- **R1 exactly-once / free ownership.** 1c frees via countdown + submit token;
  mechanism 2 frees in the callback's `finally`. A leftover `SetPendingCallbacks(0)`
  + `ReleaseSubmitToken()` frees the GCHandle and releases the client ref
  **before** the callback → UAF (exactly today's empty-input bug). Test row 6.
- **R2 inline callback vs synchronous return.** On submit failure the callback
  runs inside the P/Invoke on the caller's thread: faults the Task, frees the
  GCHandle, releases the ref. After the P/Invoke returns nothing may touch the
  GCHandle; `AbandonBeforeSubmit` runs only if the P/Invoke *threw* and is
  idempotent (`Interlocked`). `RunContinuationsAsynchronously` keeps user
  continuations out of the native frame. Test row 5.
- **R3 borrowed vs owned.** `_get_error(i)` borrowed (`FromBorrowedHandle`, never
  destroy); `_all` and the callback `error` owned (`FromHandle`); result destroyed
  once, after all reads, null-safe.
- **R4 removeAll partial failure** lives only in `_all` (count 0) and is
  unreachable locally — wiring test (row 9) + Docker.
- **R5 broken tree between checkpoints** (merge already aborts 3 classes): gates
  are filtered until CP2; full suite from CP2/CP3.
- **R6 Docker** currently down (`docker info` failed 2026-09-29) → success path
  may close CI-pending.
- **R7 messages** now core-owned; any paraphrase in tests is a DoD §3 defect.

## 9. Out of scope

The other 43 RPCs; C/Python bindings; any Rust change (Mode B items only
proposed: D3's `Error_source`); `Error_clone` binding; the gRPC servicer's
behaviour (D6); updating `PendingAdminClientFindingsForDotnet.md` or the
audit baseline (user-owned).
