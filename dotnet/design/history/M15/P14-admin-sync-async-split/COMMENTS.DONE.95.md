# COMMENTS.DONE.95 — Critic 95, M15/P14 pass 2 (range `6fba2aa6..98167351`)

## 1. Sync ListTransactions never runs the countdown-zero sweep; the sync-only `FailBroker → BrokerGroup.Complete` path has no test (Medium, test coverage)

**Where:**
- `dotnet/src/Confluent.Kafka/Internal/ListTransactionsAdminOperation.cs:331-376`: `OnAllCallbacksComplete`, its per-broker sweep at 350-364, and `FailBroker` at 369-376.
- `dotnet/tests/Confluent.Kafka.UnitTests/Interop/AdminListTransactionsTwoStageTests.cs:473`: `CountdownZero_FaultsEverythingNothingCompleted`, plus its siblings at :235, :288, :350.

**What is wrong:**

- **The async-only suite.** All 17 tests in `AdminListTransactionsTwoStageTests` build the operation with `AdminFlavour.Async` (lines 408, 451, 475, 484, 506, 582-584). This suite is the only one that drives the operation through:
  - `SetPendingCallbacks` / `AddPendingCallbacks` / `ReleaseSubmitToken`, which is how `OnAllCallbacksComplete` gets reached;
  - the real `AdminCallbacks.OnListTransactionsByBrokerId` / `OnListTransactions` trampolines;
  - the "silent broker faults at zero" and two-thread rows.
- **The sync rows skip the countdown.** The new sync rows in `AdminP14SyncCompositionTests` (2668-2994, via `ListTransactionsHarness` at 4525) call only `PublishBrokers` / `SetBrokerResult` / `SetBrokerException` / `FailDiscovery` by hand. None of them calls `SetPendingCallbacks`, `ReleaseSubmitToken`, `AddPendingCallbacks` or `AdminCallbacks`. A repo-wide grep for `no entry for broker` hits only the two async assertions (:368, :496).
- **The mock row stops early.** `PublicAdminP14DifferentialTests.ListTransactions_OnBothMocks_FailsAlike` (:2193) goes through the mock. The mock fails discovery, so it never reaches a per-broker completion.

**The sync-only logic nobody pins:** on the sync flavour, every per-broker completion must also count down the `BrokerGroup` monitor (`_group?.Complete`). Otherwise `FailFastGroupSyncFuture.Get()` waits on `WaitForBrokerGroup` with no deadline. S5f added that counting to the sweep through `FailBroker`; at `6fba2aa6` the sweep called `entry.Value.TrySetException(...)` directly.

**The mutation that survives:** revert the sweep at :354 to the pre-S5f direct `entry.Value.TrySetException(...)`. Every test still passes (713/713 on the AdminP14 / PublicAdminP14 / AdminListTransactionsTwoStage filter, net10.0 Release). But a real sync caller then hangs forever on `AllByBrokerId().Get()` or `All().Get()` whenever the core announces a broker and never answers it:
- the sweep faults that broker's own latch, but `_remaining` never reaches 0;
- `_done` is never set and no `PulseAll` follows;
- the async flavour, by contrast, fails with "The listTransactions result contained no entry for broker N."

**Why it matters:**

- **Plan and type contract:** PLAN §3.3 and §3.5 require the sync fail-fast group to settle exactly as the async `Task.WhenAny` loop does, and §7.2 requires flavour parity. Java's `ListTransactionsResult.java:92-118` resolves `all()` / `allByBrokerId()` once every broker future completes. The `OnAllCallbacksComplete` xmldoc and the type remarks also promise that "no waiter can hang".
- **Severity:** a hang is the most severe failure mode a sync `Get()` can have. It has no timeout, and the caller's thread is lost.
- **Test design:** this is the DoD #12 concern in another form. The tests that cover the sweep exercise one flavour only, so the flavour-specific half of the code under test is unpinned.

**Fix:** make the countdown rows of `AdminListTransactionsTwoStageTests` flavour-parameterized (`[Theory]` over `AdminFlavour.Sync` / `AdminFlavour.Async`, building `ListTransactionsResult.From` for sync), at least:

- `CountdownZero_FaultsEverythingNothingCompleted` (:473), both halves:
  - **undiscovered:** the sync `ByBrokerId().Get()` throws "The listTransactions call completed without delivering a result."
  - **discovered `{4, 6}` with only 6 answered:** after `ReleaseSubmitToken()`, the sync `AllByBrokerId().Get()` and `All().Get()` both return on a `BlockingCall` within the deadline, and throw "The listTransactions result contained no entry for broker 4." with `Code == 0`.
- `AnUnannouncedBroker_ResolvesNothing_AndTheSilentBrokerFaultsAtZero` (:350).
- `PerBrokerCallbacks_FromTwoThreadsAtOnce_EachResolveTheirOwnBroker` (:288), so the monitor's stamp/`_remaining` accounting under concurrent `AdminCallbacks.OnListTransactions` is covered on sync.
- `AnInlineDiscovery_StillWaitsForItsBrokers` (:235).

Assert the sync views through `BlockingCall` / `FutureProbe.SyncProbe`: pending before the last callback, settled within the deadline after it. Then confirm in-suite that the mutation above turns the sync `CountdownZero` row red (a hang caught by the `BlockingCall` 30 s join) and leaves the async row green.


**Resolved in 696927ac:**
- The 14 countdown-driving rows of `AdminListTransactionsTwoStageTests` are now `[Theory]`s over sync and async. The list includes `CountdownZero` (both halves), `AnUnannouncedBroker`, `PerBrokerCallbacks_FromTwoThreadsAtOnce` and `AnInlineDiscovery`.
- The rows still use production's submit, the real `AdminCallbacks` trampolines and `SetPendingCallbacks`/`AddPendingCallbacks`/`ReleaseSubmitToken`. Sync builds `ListTransactionsResult.From` and async builds `AsyncListTransactionsResult.From`. The views are read through `FutureProbe`/`BlockingCall`, with the exact messages and `Code == 0`.
- One new theory, `PerBrokerResults_FromTwoThreadsAtOnce_SettleTheGatheringViews`, covers the sync stamp/`_remaining` accounting under concurrent per-broker callbacks.
- No other `AdminOperation` branches on `AdminFlavour` in its completion code, and production is unchanged.
- **Mutation:** with the sweep reverted to a direct `TrySetException`, the run was 727/729 (net10.0). `CountdownZero(sync)` and `AnUnannouncedBroker(sync)` went red with the 30 s `BlockingCall` join, and both async rows stayed green. The file was then restored (`cmp` identical).
- **Gates:** unit 3788 per TFM (3772 + 16), soak 165 per TFM, two-stage suite 20/20 green on net10.0. Mode B checks (G9) and format are clean.


## 2. The repeated-broker-id half of `ABrokersListings_ReachItsOwnEntry_AndEveryView` has no sync counterpart, so the sync dedup in `PublishBrokers` is unpinned (Low, test coverage; pass 3, verifying 696927ac)

**Where:**
- `dotnet/src/Confluent.Kafka/Internal/ListTransactionsAdminOperation.cs:243-256`: the sync branch's own dedup loop. Its guard `if (completions.ContainsKey(brokerId)) continue;` is at :246, and the loop's output feeds `_group.Arm(discovered)` at :260.
- `dotnet/tests/Confluent.Kafka.UnitTests/Interop/AdminListTransactionsTwoStageTests.cs:493-530`: `ABrokersListings_ReachItsOwnEntry_AndEveryView`, still an async-only `[Fact]`. Its repeated id is at :505.
- The same file at :56-58, the class remark: "The two operation-level gather rows stay async-only: their flavour pairs are `AdminP14SyncCompositionTests`' `listTransactions` rows."

**What is wrong:**
- **Two dedup loops.** `PublishBrokers` dedups in a separate loop per flavour: :216-228 for async, :243-256 for sync. Only the sync loop's output reaches the `BrokerGroup`, and `Arm`'s `indexOf.Add` would throw on a repeated id.
- **Only the async loop is tested with a repeated id.** The one test that publishes a repeated id is `PublishBrokers(new[] { 1, 2, 1 })` at :505, on `AdminFlavour.Async` only. Every seam row (`Discover(...)`) and every composition row (`AdminP14SyncCompositionTests` :2686-2958) publishes distinct ids. The remark's "flavour pair" therefore covers this row's success half (`all-succeed`, `per-broker-independence`) but not its repeated-id half.
- **A mutant survives.** Delete the `ContainsKey` / `continue` guard at :246-249. The 33 two-stage cases and the 22 composition `listTransactions` cases (net10.0 Release, both run green at 696927ac) contain no sync repeated id, so all of them still pass.
- **What that mutant breaks.** Given a repeated id on the sync flavour, `completions.Add` throws inside the discovery trampoline. The trampoline's no-throw catch (`AdminCallbacks.cs:1743-1748`) turns that into `FailDiscovery(ArgumentException)`, so every sync view fails with a non-`KafkaException`. The async flavour still returns the map `{1, 2}`, one entry per id, as Java's map would.

**Why it matters:**
- This is item 1's class (flavour-specific completion code pinned on one flavour only) at its smallest.
- It is Low because the header promises distinct ids, which makes this a defensive path.
- But 696927ac ran every other header-violating defensive row on both flavours: the unannounced broker, neither-value-nor-error, and positive-count-no-ids. This row is the one left out, and the remark that justifies leaving it out is inaccurate for its repeated-id half.
- The rest of the async-only set is fine:
  - This row's unannounced-broker half is flavour-independent: `SetBrokerResult` / `FailBroker` check `completion is not null` before `_group?.Complete`, in shared code.
  - `TheFailFastViews_FailWithTheFirstBrokerToFail_NotTheLowestId` is paired exactly by the composition row `two-fail-first-in-time`.
  - `TheSubmitPInvoke_MatchesTheHeadersParameterOrder` reflects over one declaration that both flavours use.

**Fix:**
1. Make `ABrokersListings_ReachItsOwnEntry_AndEveryView` a `[Theory]` over sync/async through `ListTransactionsHarness.For(...)`, reading the views through `FutureProbe` as `PerBrokerResults_FromTwoThreadsAtOnce_SettleTheGatheringViews` does. At a minimum, add a sync row that publishes a repeated id.
2. Correct the :56-58 remark.
3. Confirm the :246 mutant now turns the sync case red and leaves the async case green.


**Resolved in fce1aacb:**
- A new `[Theory]` over sync and async, `ARepeatedBrokerId_IsOneEntry_AndEveryViewSettles`, sits among the seam rows. It runs through production's submit, the real `AdminCallbacks` trampolines and the countdown (`ListTransactionsHarness.Over`). The discovery announces `{1, 2, 1}`, and the row checks:
  - there is one entry per id;
  - the entry and both gathering views are pending, then settle with exact codes and messages;
  - the repeat's own callback releases a slot and resolves nothing;
  - the handle closes after the third announced callback.
- `ABrokersListings_ReachItsOwnEntry_AndEveryView` is now a `[Theory]` over sync and async, built with `ListTransactionsHarness.For` and read through `FutureProbe`. Its repeated id, listings and unannounced-broker halves run on both flavours.
- The class remark is corrected. It names `TheFailFastViews_FailWithTheFirstBrokerToFail_NotTheLowestId` as the operation-level row that stays async-only, and gives its pair, the `two-fail-first-in-time` composition row.
- **Mutation:** with the sync guard at :246-249 deleted, the two-stage and composition filter on net10.0 Release ran 640/642. Both sync rows went red, and both async rows stayed green:
  - the seam row, because `ByBrokerId()` faulted with `ArgumentException` (the trampoline's `FailDiscovery`);
  - the operation row, on the direct `ArgumentException`.
  The guard was restored with Edit; `cmp` is identical and `git diff --stat -- dotnet/src` is empty. Production code is unchanged.
- **Gates:** unit tests ran 3791 per TFM (3788 + 3) and soak 165 per TFM; the two-stage suite was 36/36 on net8.0 and net10.0. Mode B (G9) and format are clean.

## Critic 95, M15/P14 pass 3 (range `fce1aacb..aad18a0c`: S6 `b3bb9fb3` `e871d03e`, S7 `6243491a` `1567d020` `b85784fa`, `aad18a0c`)

Clean areas, checked and not filed: the two gRPC servicers (same 46 RPCs in the same order; after normalising `await` / `ConfigureAwait` / `Task.FromResult` / the `Async` type prefixes, the bodies differ only in `Close`'s no-timeout branch, `Dispose()` against `DisposeAsync()`, which is correct; no `Task.Run`, `GetAwaiter`, `.Wait()` or `.Result` in the sync file; the Release build has 0 warnings and 0 errors), the `TranslateAdmin` sync overloads, the `Program.cs` registration and `DrainServicer` (the `TryBeginClose` latch makes a second `Dispose` from the DI container a no-op), the Rust `dotnet_async` admin arm (the same as `DotnetGrpcFactory` / `PythonAsyncGrpcFactory`, and no test branches on the backend name), the eight approved texts and the a/b rule (word for word, in the approved places; the only rule files changed are `dotnet/CLAUDE.md`, `ffi-marshalling.md` and `bindings.md`; root `admin-client.md` is untouched), the commit SHAs and the counts in the STATUS entry.

### 1. LOW — STATUS: every line number the new entry cites is now one too high

**Where:** `dotnet/design/current/STATUS.md:27`, `:28`, `:30`, `:47`.

**What is wrong:** `b85784fa` wrote the cites correctly. `aad18a0c` then deleted the line "2. Text 1 (see Rulings) needs a ruling." (old line 48), which sits above every cited line, so each target moved up by one:

| Cite in the entry | Points at now | Correct now |
|---|---|---|
| `:27` "the M15/P1 entry at line 702" | 702 is the entry's first bullet ("The one real design problem…") | **701** |
| `:27` "'Public surface' bullet at line 708" | 708 is "⚠ The PLAN's own code sketch was WRONG twice" | **707** |
| `:28` "residual (a) at line 478" | 478 is "What the local gate does NOT prove" | **477** |
| `:30` "item 4 (line 123)" | 123 is "M11/P3.6 is pushed" | **122** |
| `:47` "item 1 (line 120)" | 120 is item 2 (the out-of-order callbacks) | **119** |

Also at `:30`: "closed … by the S7 sweep (D18)" is not true for one of the four sites. `CreateTopicsResult.cs:30` was rewritten in S4 (`fbe72c47`); the sweep `1567d020` does not touch that file. The other three (`IAdmin.cs`, `ListTopicsResult.cs`, `DescribeClusterResult.cs`) are the sweep's.

**Why it matters:** these cites are how a reader finds the superseded records. Each one now lands on a neighbouring bullet, and two of those bullets are about something else.

**Fix:** change 702 → 701, 708 → 707, 478 → 477, 123 → 122 and 120 → 119. Reword `:30` to "by S4 (`fbe72c47`, `CreateTopicsResult.cs`) and the S7 sweep (D18, the other three)".

**Resolved in a5d9a35a:** the five cites now read 701, 707, 477, 122 and 119, each re-derived with `/usr/bin/grep -n` and checked to land on the intended entry or bullet. The "Closed" bullet now reads "by S4 (`fbe72c47`, `CreateTopicsResult.cs`) and the S7 sweep (D18, `1567d020`, the other three)", checked with `git log -p` on each of the four files. The same `aad18a0c` line shift had also moved the entry's two `dotnet/CLAUDE.md` cites, so those are corrected too: Text 1 now cites `:60-62` (was `:59-61`), and the open half cites `:196` ("item 4's `:195`"). Only lines 22, 27, 28, 30 and 47 of STATUS changed; no older entry is touched.

### 2. LOW — build-file comments still say the async .NET image serves no AdminService

**Where:**
- `dotnet/Dockerfile.grpc.async:27-28`: "The image serves ProducerService + ConsumerService (no AdminService: that is hosted by the sync image only, M15/P12 D1 — see Program.cs)".
- `dotnet/Dockerfile.grpc:21-22`: "(Admin in this sync image only, M15/P12 D1 — see Program.cs)".
- `dotnet/Makefile:46-48` (`grpc-image-async`): "AsyncProducerServiceImpl + AsyncConsumerServiceImpl (AsyncKafkaProducer / AsyncKafkaConsumer; no Admin, M15/P12 D1)".
- Incomplete, not false: `dotnet/Dockerfile.grpc.async:13-15` and `:96-99` list the servicers that the async flavour hosts without `AsyncAdminServiceImpl`.

**What is wrong:** since `b3bb9fb3` the async branch of `Program.cs` registers and maps `AsyncAdminServiceImpl` (`Program.cs:115-118`, `:143`). The image bakes `CONSUMER_FLAVOR=async` (`Dockerfile.grpc.async:100`), so it serves `AdminService`. The comments point the reader at `Program.cs`, which now says the opposite (`Program.cs:107-111`: "both flavors host the producer, consumer and admin services"). `git grep 'M15/P12 D1'` outside `design/history` now finds only these three stale claims plus the deliberately historical records.

**Why it matters:** the CI-only gates (PLAN §8: the Linux container `make test-integration-dotnet` and the `grpc-image-async` build) now run the 79 `__grpc_dotnet_async` admin arms against this image. Anyone looking into a failure there is told by the image's own header that the image has no admin service. S6 updated the twin comments in `Program.cs`, `backend_factory.rs`, `backend_pool.rs` and the macro, but missed these three.

**Fix:** comments only, with no build change. `Dockerfile.grpc.async:27-28`: "…serves ProducerService + ConsumerService + AdminService (admin on both images since M15/P14, over IAsyncAdmin here — see Program.cs)…". Add `AsyncAdminServiceImpl` to the lists at `:13-15` and `:96-99`. `Dockerfile.grpc:21-22`: drop "in this sync image only, M15/P12 D1", or replace it with "over IAdmin; the async image serves it over IAsyncAdmin, M15/P14". `Makefile:46-48`: replace "no Admin, M15/P12 D1" with "+ AsyncAdminServiceImpl over IAsyncAdmin, M15/P14". (PLAN §8's "`Dockerfile.grpc.async` — No change" is about the build. A comment fix does not contradict it.)

**Resolved in a5d9a35a:** comments only. `Dockerfile.grpc.async` now says the image serves ProducerService + ConsumerService + AdminService ("admin on both images since M15/P14, over IAsyncAdmin here"), and both of its servicer lists name `AsyncAdminServiceImpl` / `AsyncKafkaAdminClient`. `Dockerfile.grpc` says "Admin over IAdmin; the async image serves it over IAsyncAdmin, M15/P14". The `grpc-image-async` Makefile comment names `AsyncAdminServiceImpl` and "admin over IAsyncAdmin since M15/P14". Each changed line is a `#` line (checked on `git diff -U0`); no instruction, build arg, recipe or target changed. Outside `design/history`, `git grep 'M15/P12 D1'` now finds only the deliberately historical lines (STATUS, `Program.cs:44`, `:130`, the macro's `:27`).

### 3. LOW — public xmldoc on three shared admin types still says Task / awaitable for what is now a sync `KafkaFuture`

**Where:**
- `dotnet/src/Confluent.Kafka/Admin/DeletedRecords.cs:43-45`: "A partition that failed faults its own `<see cref="System.Threading.Tasks.Task"/>` in `<see cref="DeleteRecordsResult.LowWatermarks"/>`".
- `dotnet/src/Confluent.Kafka/Admin/LogDirDescription.cs:34-37`: "The per-broker task in `<see cref="DescribeLogDirsResult.Descriptions"/>` therefore succeeds … Faulting the task here would be a behavioural divergence".
- `dotnet/src/Confluent.Kafka/Admin/AlterPartitionReassignmentsOptions.cs:43`: "… on that partition's awaitable." The options type is shared by `IAdmin` and `IAsyncAdmin`.

**What is wrong:** `DeleteRecordsResult.LowWatermarks` is `IReadOnlyDictionary<TopicPartition, KafkaFuture<DeletedRecords>>` (`DeleteRecordsResult.cs:84`, since S4). `DescribeLogDirsResult.Descriptions` is `IReadOnlyDictionary<int, KafkaFuture<…>>` (`DescribeLogDirsResult.cs:75`, since S5a `b7ea260b`). Neither holds a `Task`. The first site even crefs `System.Threading.Tasks.Task` as the type found in that property. On the sync flavour, `AlterPartitionReassignmentsResult.Values` holds `KafkaFuture`s, which cannot be awaited (`KafkaFuture` has no `GetAwaiter`, by design: PLAN D15). None of the three files is in `1567d020`. They are model/options types that both flavours share, so the sweep's file list (Results + `IAdmin` + `NativeAdminClient`) did not reach them.

**Why it matters:** this is IntelliSense text on public types. It names a type the referenced property does not have (`DeletedRecords`, `LogDirDescription`), or suggests `await`, which does not compile on the sync flavour (`AlterPartitionReassignmentsOptions`). This is the class D18 set out to remove.

**Fix:** use flavour-neutral wording that names both results:
- `DeletedRecords.cs`: "A partition that failed fails its own future in `<see cref="DeleteRecordsResult.LowWatermarks"/>` (`<see cref="AsyncDeleteRecordsResult.LowWatermarks"/>` on the async flavour) and produces no `DeletedRecords`…".
- `LogDirDescription.cs`: "The per-broker future in `<see cref="DescribeLogDirsResult.Descriptions"/>` / `<see cref="AsyncDescribeLogDirsResult.Descriptions"/>` therefore succeeds … Failing that future here would be…".
- `AlterPartitionReassignmentsOptions.cs`: "… on that partition's future."

To catch any twins left, rerun the sweep's grep (`awaitable|\btasks?\b|cref="(System\.Threading\.Tasks\.)?Task`) over `Admin/*.cs` minus `Async*`, the `*Result.cs` files and the clients. At this tree it matches only these three files, plus the true "background task" / `IAsyncAdmin` lines.

**Resolved in a5d9a35a:** `DeletedRecords.cs` now says a failed partition "fails its own future in `DeleteRecordsResult.LowWatermarks` (`AsyncDeleteRecordsResult.LowWatermarks` on the async flavour)", with both as crefs and the `System.Threading.Tasks.Task` cref gone. `LogDirDescription.cs` says "The per-broker future in `DescribeLogDirsResult.Descriptions` / `AsyncDescribeLogDirsResult.Descriptions` therefore succeeds … Failing that future here would be…". `AlterPartitionReassignmentsOptions.cs` says "on that partition's future". The sweep's grep (`awaitable|\btasks?\b|cref="(System\.Threading\.Tasks\.)?Task|\bawait`), run on all 82 `Admin/` files outside `Async*`, `*Result.cs`, `IAdmin` / `IAsyncAdmin` and the four clients, matched these three files at HEAD and nothing in the other 79, so the extra count is 0. The build has 0 warnings and 0 errors on all three TFMs, and the new crefs resolve in the emitted XML (`P:` entries).


---

## Not applied: the Critic's rule-update suggestion (pass 2)

Left open at close. It is a rule-file text, and the user approved no rule edit beyond the eight M15/P14 texts and the a/b rule.


`ffi-marshalling.md` §B6's shape-4 / single-callback bullets and the P14 PLAN §7.2 parity tests both describe flavour-paired tests per Result type. Neither says that tests driving the **operation core** (countdown, trampolines, sweep) must also run per flavour whenever the core holds flavour-specific completion logic. A one-line addition to PLAN §7.2's successor rule text (S7) would close this class: *"Any `AdminOperation` whose completion code branches on `AdminFlavour` (e.g. ListTransactions' `BrokerGroup`) must have its countdown / trampoline / sweep tests run for both flavours. The Result-level composition rows do not reach `OnAllCallbacksComplete`."*

