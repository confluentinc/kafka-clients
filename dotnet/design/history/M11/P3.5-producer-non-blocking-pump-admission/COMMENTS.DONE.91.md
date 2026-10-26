# COMMENTS.DONE.91 — M11/P3.5, resolved items

## 91.1 The sync `IProducer` remarks still say the async `Send` returns `Task{TResult}`. That has been false since S2 — LOW
- Where: commit `e221319d` (S2) made this false. It did not touch the file. `dotnet/src/Confluent.Kafka/IProducer.cs:54-58`:
  > .NET has only one future type (`Task{TResult}`), which the async `IAsyncProducer.Send(ProducerRecord, CancellationToken)` **already returns**; a `Task` here would clone the async surface …
- Anchor: the approved D1 shape (PLAN §2.1) and `IAsyncProducer.cs:152` / `:220`. Both async overloads now return `ValueTask<Task<RecordMetadata>>`. The `Task<RecordMetadata>` is what the async surface *yields* after its accepted stage. It is no longer what `Send` returns.
- Why it is filed even though S5 has "`IDeliveryCallback` / `IProducer` cross-ref touch-ups" (PLAN §13, port table row `IDeliveryCallback.cs, IProducer.cs`): the S5 criterion is narrow. It says "'the returned Task' → 'the delivery task', only where ambiguous". This sentence does not contain "the returned Task", and it is not ambiguous. It is a factual claim about the return type, and that claim is now wrong. An S5 Actor following that criterion literally would skip it.
- Evidence:
  - `/usr/bin/grep -n -E 'already returns|only one future type' dotnet/src/Confluent.Kafka/IProducer.cs` gives `:54` and `:57`.
  - `git -C <repo> show e221319d --stat` does not list `IProducer.cs`.
- Expected fix: reword it so the async surface *yields* a `Task<RecordMetadata>` from the outer `ValueTask` (accepted stage). For example: "…which the async `Send` yields once the record is accepted (inside its `ValueTask` first stage)". Keep the sync/async-split argument as it is.
  - Either do it as a `fixup!` of `e221319d`, or name `IProducer.cs:54-58` explicitly in the S5 brief.
  - Not blocking for S2: no code or behaviour is affected.
- **Resolution (fixup `fccc23a5`):** `IProducer.cs:54-59` now says the async `Send` "already yields [the `Task{TResult}`] from its first stage (a `ValueTask{TResult}` that completes once the record is accepted)". The sync/async-split argument and every cref are kept. The same sweep also fixed `IAsyncProducer.cs:56` and `:89-90`, `AsyncKafkaProducer.cs:68` and `SendAccumulator.cs:54`. It left `NativeProducer.cs:405/:472/:484/:496`, `SendAccumulatorSettings.cs:57` and `SendAccumulator.cs:308` to S3 (POC `2c9cc0b8` rewrites them) and `IDeliveryCallback.cs:301-302/:307` to the S5 row.

## 91.2 The unowned twin of `SendAccumulator.cs:54` in `NativeMethods.cs:2270/:2273` still says the async `Send` "returns its Task" / "never across the returned Task" — LOW
- Where: `dotnet/src/Confluent.Kafka/Internal/Interop/NativeMethods.cs:2269-2273` (the `ASYNC path (IAsyncProducer / AsyncKafkaProducer / AsyncMockProducer)` bullet of the send-path design comment):
  > Send pins the record's buffers, appends to a binding-side accumulator and **returns its Task**; … The pin is DEFERRED — held from Send until send_batch RETURNS, **never across the returned Task**, …
- Anchor: the D1 shape. Every async-path `Send` now returns `ValueTask<Task<RecordMetadata>>`: `AsyncKafkaProducer.cs:120/:132`, `AsyncMockProducer.cs:124/:137`, and the internal `NativeProducer.SendViaPump` (`NativeProducer.cs:503`). So "returns its Task" is a false return-type claim, the same kind as 91.1.
- "never across the returned Task" is the sentence `fccc23a5` fixed in `SendAccumulator.cs:54`, almost word for word. It is ambiguous now, and one reading is false. The pins are held until `send_batch` returns, which is after `Send` has returned an already-complete stage-1 `ValueTask`. So if "the returned Task" means the `ValueTask` that `Send` returns, the claim is false. It is true only if it means the delivery task. PLAN §4 (line 204) says the same thing: "Stage-1 completion does **not** end the borrow".
- Why it is filed, not scheduled. No slice owns it:
  - POC `2c9cc0b8`'s S3 hunks do not touch `NativeMethods.cs`. `git show --stat 2c9cc0b8` lists 9 files, and this is not one of them.
  - PLAN §13 row 489 excludes `NativeMethods.cs` only for the **lane ABI**.
  - The S5 row (488) covers `IDeliveryCallback.cs` and `IProducer.cs` only.
  - The fixup's own commit message neither fixes it nor lists it under "Not touched, because a later slice rewrites them". The `COMMENTS.DONE.91.md` resolution therefore overstates how complete the sweep was.
- Evidence: `/usr/bin/grep -rn -E 'returns? (its|the|a) (<see cref="(System\.Threading\.Tasks\.)?)?Task' dotnet/src --include='*.cs'`, restricted to producer files, gives only `NativeProducer.cs:405`, `SendAccumulatorSettings.cs:57` (both scheduled) and `NativeMethods.cs:2270`.
- Expected fix (internal `//` comment only; no code or behaviour is affected; not blocking):
  - Reword it the way `SendAccumulator.cs:54` was reworded. For example: "…appends to a binding-side accumulator and yields the record's delivery Task from its ValueTask first stage; … held from Send until send_batch RETURNS, never across the record's delivery Task, …"
  - Either do it as a `fixup!` of `e221319d`, or name `NativeMethods.cs:2269-2273` explicitly in the S3 or S5 brief.
- **Resolution (fixup `c0323fec`):** `NativeMethods.cs:2269-2274` now says `Send` "yields the record's delivery Task from its ValueTask first stage" and the pin is "never across the record's delivery Task" (the `SendAccumulator.cs:54` wording). The class sweep also fixed `NativeMethods.cs:2373-2375` (the `send_batch` xmldoc said "nothing is held across the returned Task"), `grpc-server/AsyncProducerServiceImpl.cs:34` ("`Task`-returning binding method") and `soak/SoakClient/SoakClient.cs:909`/`:930` ("returned Task"). It again left `NativeProducer.cs:405/:472/:484-485/:496`, `SendAccumulatorSettings.cs:57` and `SendAccumulator.cs:308` to S3, and `IDeliveryCallback.cs:301-302/:307` to S5.

## 91.3 `ProducerSendBatchMarshal.cs:40-41`, an unowned twin of the `NativeMethods.cs:2373` sentence fixed in `c0323fec`, still says the pin "never spans the returned Task" — LOW
- Where: `dotnet/src/Confluent.Kafka/Internal/Interop/ProducerSendBatchMarshal.cs:38-41` (type `<summary>`):
  > …the pin has to span `Send` → …accumulator… → `send_batch` returns … It still never spans **the returned** `<see cref="System.Threading.Tasks.Task"/>`
- Anchor: the D1 shape. `Send` returns `ValueTask<Task<RecordMetadata>>` (`IAsyncProducer.cs:152`/`:220`), and its first stage is already complete when it returns (`NativeProducer.cs:625-627`). The pins are released after `send_batch` returns, which is later. So the sentence has two readings:
  - If "the returned Task" means what `Send` returns, the claim is false.
  - It is true only if it means the delivery task.
- This is the same ambiguity `c0323fec` fixed at `NativeMethods.cs:2373-2375`, and the commit message itself says so ("the same ambiguity as 91.2"). It is also the sentence `fccc23a5` fixed at `SendAccumulator.cs:54`.
- Why the sweep missed it: "returned" ends `:40` and the cref is on `:41`, so a line grep for `returned … Task` cannot match it.
  - A joined-comment-block scan of the four re-check directories finds this as the **only** remaining producer hit. The other hits are consumer sites in `AsyncConsumerServiceImpl.cs`.
- No slice owns it:
  - `git show --stat 2c9cc0b8` (S3) lists 9 files, and this is not one of them.
  - PLAN §13 has no row for it. Row 489 excludes only the lane files and the lane ABI of `NativeMethods.cs`.
- Expected fix (doc only, not blocking): "…It still never spans the record's delivery `<see cref="System.Threading.Tasks.Task"/>`…", the `NativeMethods.cs:2373` wording. A `fixup!` of `e221319d` is fine.
- **Resolution (fixup `ff46399a`):** `ProducerSendBatchMarshal.cs:40-41` now says the pin "still never spans the record's delivery `<see cref="System.Threading.Tasks.Task"/>`", the `NativeMethods.cs:2373-2375` wording.

## 91.4 `IProducer.cs:137-139` still says the async callback `Send` "returns a `Task{TResult}`", the same false return-type claim as 91.1, in the same file — LOW
- Where: `dotnet/src/Confluent.Kafka/IProducer.cs:136-139`. This is the `<remarks>` of the public sync `Send(record, IDeliveryCallback)`, so it appears in the shipped XML docs:
  > …the two signatures differ anyway (the async one takes a `CancellationToken` and **returns a** `<see cref="System.Threading.Tasks.Task{TResult}"/>`)
- Anchor: `IAsyncProducer.cs:220-223`. The async callback overload returns `ValueTask<Task<RecordMetadata>>`.
- History:
  - `git blame` gives `0204437a` (before S2).
  - `e221319d` did not touch `IProducer.cs`.
  - `fccc23a5`'s 91.1 fix edited only `:54-59` in this file.
- Scope: this is outside `c0323fec` and outside the four re-check directories. It turned up when the same block scan was extended to the producer-family `dotnet/src` files. It is filed because it is the 91.1 class, and the 91.1 resolution states that file's sweep as complete.
- Ownership: the S5 row (PLAN §13 row 488, `IDeliveryCallback.cs`, `IProducer.cs`) only rewrites "the returned Task" → "the delivery task", and only where ambiguous. This sentence has no "returned Task" and is not ambiguous. That is the same reason 91.1 was filed.
- Expected fix (doc only, not blocking): drop the parenthetical, matching the async twin `IAsyncProducer.cs:166` ("…and the two signatures differ anyway."). Or reword it to "…and returns a `ValueTask{TResult}` whose result is the delivery `Task{TResult}`".
- **Resolution (fixup `ff46399a`):** `IProducer.cs:137-140` now says the async one "returns a `<see cref="System.Threading.Tasks.ValueTask{TResult}"/>` whose result is the delivery `<see cref="System.Threading.Tasks.Task{TResult}"/>`". The D-6 argument and the `:54-59` cref style are kept.

## 91.5 The internal bound statements at `SendAccumulator.cs:270-271` and `:1145` say "pending first stages", which D2 (c) makes false — LOW
- Where: commit `14bf366e` (S3). It rewrote both sites from "parked callers" to "pending first stages":
  - `dotnet/src/Confluent.Kafka/Internal/SendAccumulator.cs:268-271` (the `AdmittedRecordCount` xmldoc): "the quantity it is bounded by is `MaxAdmittedRecords + (pending first stages)`".
  - `SendAccumulator.cs:1145`: `peak accepted-but-unsent <= 2 * (MaxAdmittedRecords + pending first stages)`.
- Anchor: the same commit's own derivation at `SendAccumulator.cs:130-141` (`_admission`). There W is "the sends whose admission wait is still pending — their first stage not yet complete, **or ended early by the caller's own token**, which releases the caller but NOT the wait". The PLAN §4 row for a token that fires says the same thing: "the admission wait continues underneath and takes its permit". So the bound term is the count of pending **admission waits**. A first stage that the token ended is complete, but its wait still counts in W.
- Failure scenario: `MaxAdmittedRecords = 1`, and the batch thread is not taking.
  - Send A, with a cancelable token, takes the free permit, so P = 1.
  - Send B, with token `tB`, appends (P = 2), and its `WaitAsync` pends.
  - Cancel `tB`. B's first stage ends Canceled.
  - Now the pending first stages are 0, so the documented bound is 1. But P is 2.
  - With a short per-send token on a stuck producer (the shape `IAsyncProducer.cs:110-113` warns about), P grows by one per token period while "pending first stages" stays near 0.
- Why it matters: these are the test-facing statements of the bound (`:130` says "stated so it can be tested instead"). A test or reviewer that takes a ceiling from `AdmittedRecordCount`'s xmldoc gets it too low as soon as a token fires, and may "prove" a leak that does not exist. Both statements also contradict `:130-141` in the same file.
- Expected fix: write "pending admission waits" (or "W, see `_admission`") at both sites, as `:140-141` already does. This is a doc change only.
- **Resolution (fixup `2b7b6b9d`):** both sites now state the bound in terms of W, the pending **admission waits** defined at `_admission` (`:130-141`). `SendAccumulator.cs:268-276` (the `AdmittedRecordCount` xmldoc) reads "`MaxAdmittedRecords + W` … where W is the number of pending *admission waits*", and says explicitly that a first stage ended by the caller's own token is complete while its wait still counts in W. `:1148-1152` reads `peak accepted-but-unsent <= 2 * (MaxAdmittedRecords + W)`, with the same note. This is doc only; no code changed.

## 91.6 `IDeliveryCallback.cs:102-106`: the new ⚠ sentence now sits between the ordering claim and "Note this is **stricter** than the Python sibling", so the comparison reads as being about the first-stage unordering — LOW
- Where: `14bf366e` inserted the ⚠ sentence at `dotnet/src/Confluent.Kafka/IDeliveryCallback.cs:102-104`, in the middle of the D3 ordering paragraph:
  > …and before `Send` returns or throws (sync). ⚠ On the async surface the callback and the delivery task are **unordered relative to `Send`'s first stage** (M11/P3.5): a saturated send's first stage can complete after its record was already delivered. Note this is **stricter** than the Python sibling, which resolves its future first …
- Why this is a false statement as written: "this" now naturally refers to the unordering sentence. That unordering is not stricter than Python. Python's async `send` has exactly the same unordering: in `python/producer.py:689-705`, the drain can resolve `ret` while `await space` is still pending. The "stricter" claim is about the callback-before-awaiter order (`producer.py:322-327`), which is two sentences earlier.
- Expected fix: move the ⚠ sentence to after "…before the callback has run.", or begin the comparison with "That callback-before-completion order is stricter …". This is public xmldoc, so the change is to wording only.
- **Resolution (fixup `2b7b6b9d`):** the ⚠ first-stage-unordering sentence now comes after "…before the callback has run." (`IDeliveryCallback.cs:102-106`). So "Note this is **stricter** than the Python sibling" directly follows, and refers to, the callback-before-completion ordering claim. The wording is unchanged apart from re-wrapping.

## 91.7 S4: `ProducerBenchmarkConfig.cs:118-119` justifies the strict `AWAIT_ACCEPTED` parse with "unlike the other flags it defaults to true", which is false — LOW
- Where: `69be3670`, `dotnet/tests/Performance/PerformanceCommon/ProducerBenchmarkConfig.cs:118-119`.
- Evidence, from the same file's `FromEnv`:
  - `DoVerify = PerfEnv.GetBool("DO_VERIFY", true)` (`:147`) and `CreateTopic = PerfEnv.GetBool("CREATE_TOPIC", true)` (`:149`) both default to true. `:102` and the `CreateTopic` summary also say so.
  - `PerfEnv.GetBool` (`PerfEnv.cs:75-84`) returns `value == "True"`. So `DO_VERIFY=true` silently selects false, which is exactly the hazard the sentence claims sets `AWAIT_ACCEPTED` apart.
- The strict parse itself is correct and matches the PLAN S4 spec (`:338-344`): null, `""` or `True` gives true, `False` gives false, and anything else throws the exact `ArgumentException`. Only the stated reason is wrong.
- Expected fix: drop "unlike the other flags it defaults to true, so". For example: "Parsed strictly (`True` or `False`), so a misspelt value cannot silently select the non-default mode." This is a harness doc change only.
- **Resolution (fixup `f9042bb0`):** `ProducerBenchmarkConfig.cs:118-119` now reads "Parsed strictly (`True` or `False`; unset or empty means true, anything else throws), so a misspelt value cannot silently select the non-default mode." The false comparison with the other flags is gone. This is harness doc only; `dotnet format --verify-no-changes` on `PerformanceCommon.csproj` is clean and the perf unit tests pass 39/39 per TFM.

## 91.8 T19 cannot see the token half of M13a, so PLAN §17's "(each half and both) … 8/8" and the test's own "captured and asserted" claim are unsupported — MEDIUM (record honesty)
- Where:
  - `dotnet/tests/Confluent.Kafka.UnitTests/Interop/SendAccumulatorFirstStageTests.cs:374-375`: "What it catches: a Set* where a TrySet* belongs. On the token side that throws out of the caller's own Cancel() (captured and asserted)". The check is `:444` (`Assert.Null(cancelFailure)`).
  - `PLAN.md:589` (M13a): "`SetResult` / `SetCanceled` instead of `TrySet*` (each half and both) | T19 | 8/8 | red | discriminating".
  - The S3b commit `75c1d392` itself lists only "SetResult for TrySetResult - T19 red".
- Why the token half is ungradeable by T19:
  - A token-side `SetCanceled(token)` throws only if the stage is **already settled** when the token callback runs.
  - Only the wait side settles it, at `SendAccumulator.cs:545` (`TrySetResult`). The very next statement, `:546`, disposes the registration, which unlinks the callback from the source.
  - A `Cancel()` before `:545` wins, and `SetCanceled` succeeds. A `Cancel()` after `:546` finds no callback, so nothing runs.
  - Only a `Cancel()` that reaches the callback list in the few instructions between `:545` and `:546` can throw. T19 does not aim at that instant. Its staggers are 200 µs steps (`:401`), and in its slot-won reps (4 of 8, measured below) `Cancel()` lands at a time unrelated to it.
- Evidence:
  - A scratch replica of `CancellableFirstStage`, outside the repo, with the token half mutated to `SetCanceled(token)` and the wait half unchanged (`TrySetResult` + `Dispose`).
  - Setup: the slot (`Release`) and `Cancel()` race from two threads behind a barrier, the canceller swept 0-199 µs in 1 µs steps, 20,000 races.
  - Result: `Cancel()` threw **33/20,000** on net8.0.30 and **10/20,000** on net10.0.11. That is 0.05-0.17 % per race, even when aimed at the decision boundary.
  - T19 as built (net10.0, 3 runs, build 0 errors, 2/2 tests passed each run) logs "token won 4, slot won 4 of 8" every time.
  - So "8/8 isolated" is not credible for that half as a Set-versus-TrySet result. **Confirmed from the S3c Actor's own label-corrections report:** the injected "M13a-cancel" was `TrySetCanceled` → **`SetCanceled()`**, with the token dropped. That is the only parameterless form; `TaskCompletionSource<T>.SetCanceled(CancellationToken)` is .NET 5+, so netstandard2.0 does not have it. It went 8/8 isolated, and the full suite failed **5 of 2954**.
    - That signature is the token-identity check, not Set versus TrySet. `SetCanceled()` produces an OCE with `CancellationToken.None`, which fails `Assert.Equal(cancellation.Token, canceled.CancellationToken)` (`:454`) on every token win.
    - The same check sits in T4, T16 and T17, which explains the other four suite failures.
    - Rerunning with `SetCanceled(token)` (where available) would grade the half the record claims. The replica above predicts it would be green in almost every run.
- The wait half is soundly graded. Whenever the token wins (every odd rep here), a wait-side `SetResult` faults the discarded continuation. The `CancellableFirstStage`-filtered `UnobservedTaskException` listener (`:381-390`) sees that fault after the forced collections. Keep that part of the row.
- Expected fix (record and comment only; no seam is worth adding):
  - Split M13a in §17. Keep the wait half as **discriminating**. Record the token half as **effectively equivalent / a structural guard**: what protects the caller's `Cancel()` is `TrySetCanceled`, plus the continuation disposing the registration straight after `TrySetResult` (`:545-546`).
  - If the S3c injection was `SetCanceled()`, say so.
  - Reword T19 `:374-375` so it does not claim to catch the token half. For example: "a token-side Set* can throw only in the instant between the wait continuation's TrySetResult and its Dispose, so this test does not grade it".
- **Resolution (fixup `2b7b6b9d`), test-comment part only. The PLAN §17 split is the Manager's and was not touched:** the T19 comment (`SendAccumulatorFirstStageTests.cs:374-385`) now says what it grades: a wait-side Set*, through the `CancellableFirstStage`-filtered unobserved-task listener. It also says what it does NOT grade: a token-side Set*, which "can throw only in the instant between the wait continuation's TrySetResult and its Dispose of the registration … and this test's staggers do not aim at that instant". It names what protects the caller's `Cancel()` there (the token side's own `TrySetCanceled`, plus that `Dispose` following `TrySetResult` directly), and calls the `Assert.Null(cancelFailure)` check a sanity check, not that half's grader. No seam and no new test were added.

## 91.9 T21(i): the reported (c)-path figure (+680 | +688 B) holds only when the token's registration node is recycled; a token without a free node pays +760 | +768 B, above the 720 B ceiling, and the test's DEFINITION hides this — LOW
- Where:
  - `SendAccumulatorFirstStageTests.cs:571-577`, the DEFINITION: "what remains is … (for a cancelable token) the stage, its completion source and the token registration. The token is warmed once per accumulator outside the measured window, so the source's one-time registration table is not charged."
  - The ceiling comment at `:558-563`.
  - PLAN §17 `:567-568` ("Saturated + cancelable token (the D2 (c) path) 808 | 816 B (+680 | +688 marginal)").
  - The `75c1d392` message ("with the same warmed token").
- What actually happens:
  - On .NET Core, a `CancellationTokenSource` recycles its registration nodes. `CancellationTokenRegistration.Dispose` pushes the ~80 B `CallbackNode` onto the source's free list, and the next `Register` pops it.
  - The test shares **one** `cancellation` source across all four attempts (`:578`, `:586`).
  - In **attempt 0**, each of the 64 measured stages allocates a fresh node. Attempts 1-3 reuse the 65 nodes that attempt 0's stage continuations released at `SendAccumulator.cs:546`.
  - Best-of-4 therefore always picks a recycled attempt. In the reported figure, "the token registration" costs **0 B**.
  - The warm call keeps only the source's one-time `Registrations` object out of the window. It does not keep out the per-registration node.
- Evidence (scratch probes outside the repo, net8.0.30 and net10.0.11):
  - (a) `Register` on a long-lived source costs 81 B per call in round 0, and 0 B per call once the previous round's registrations were disposed. Same on both runtimes.
  - (b) A replica of both saturated paths, measured the T21 way: a fresh semaphore and gate per attempt, 129 prefilled plain waiters, a warm call, 64 measured calls, and one shared caller source.
    - Plain path: 536 | 544 B.
    - Cancelable path on attempts 1-3: 680 | 688 B. These are exactly the figures the real test prints here (3 runs per TFM, build 0 errors, 3/3 passed: "+680 B" on net8.0, "+688 B" on net10.0).
    - Cancelable path on **attempt 0: 760 | 768 B**, which is over `CancelableStageMarginalCeilingBytes = 720`.
- Why it matters:
  - T21 requires the (c) path's per-send allocation to be "measured and reported with its definition".
  - A caller whose token has no free node pays an extra 80 B on every saturated send. That includes:
    - a fresh `CancellationTokenSource` per send (a per-request timeout, the short-token shape `IAsyncProducer.cs:110-113` discusses);
    - the first registrations on any token;
    - more pending saturated sends than the source has freed nodes.
  - So §17 under-reports the (c) path by 80 B per send for that caller.
  - The test is **not** flaky: recycling is reliable, because the disposals run microseconds after each awaited `TrySetResult` and milliseconds before the next attempt. A +72 B injection still turns it red. The defect is that the definition does not say the figure is for a recycled node, and the test would go red if attempt 0 were the one kept.
- Expected fix (no production change):
  - State in the DEFINITION, in the ceiling comment and in §17 that the figure is for a caller token whose source already has a recycled registration node, so the minimum skips attempt 0 by construction.
  - Report the fresh-node figure as well (+760 | +768 B).
  - Optionally guard that case too: a new `CancellationTokenSource` per measurement, with its own ceiling (e.g. 800 B).
- **Resolution (fixup `2b7b6b9d`), test and comment part. PLAN §17 is the Manager's and was not touched:** the ceiling comment (`SendAccumulatorFirstStageTests.cs:572-578`) and the DEFINITION (`:602-607`) now state that the cancelable figure is for a caller token whose source already has a **recycled** registration node. Attempt 0 allocates the nodes, its drained stages dispose them back to the source, and attempts 1-3 pop them, so best-of-4 skips attempt 0 by construction. A new fresh-node guard, `Admission_SaturatedFirstStage_WithAFreshTokenSource_PerSendAllocation_IsMeasured_AndStaysUnderItsCeiling` (`:636`), uses a new `CancellationTokenSource` per measurement, so every measured `Register` allocates its node. It has its own ceiling, `FreshNodeCancelableStageMarginalCeilingBytes = 800` (`:588`): the larger measurement (768 B) plus 32 B, which is less than one more 72 B `Task<T>`. Measured with the same definition (caller-thread `GC.GetAllocatedBytesForCurrentThread`, per send, over 64 sends, best of 4, marginal over the 128 B fast path), 3 runs per TFM, all identical: **recycled +680 B net8.0 / +688 B net10.0** (ceiling 720 B) and **fresh +760 B net8.0 / +768 B net10.0** (ceiling 800 B). These match the Critic's figures. Sanity injection: one 72 B allocation (`GC.KeepAlive(new byte[48])`) in `CancellableFirstStage.Start`. The build succeeded (0 errors), and both guards went red on both TFMs (recycled +752/+760 > 720, fresh +832/+840 > 800), while the plain guard stayed at +536/+544. The file was reverted with `git checkout --`.

## 91.10 The fresh-node guard and PLAN §17 say +760 | +768 B (ceiling 800 B) is what "a new `CancellationTokenSource` per send / a per-request timeout" pays. That caller also pays the source's one-time registration table on every send, which the guard's warm call deliberately excludes: about +824 | +832 B, above 800 B — LOW (record honesty)
- Where:
  - `dotnet/tests/Confluent.Kafka.UnitTests/Interop/SendAccumulatorFirstStageTests.cs:583-585`, the ceiling comment: "(a new CancellationTokenSource per send — a per-request timeout, say), so every Register allocates one. Measured +760 B on net8.0 and +768 B on net10.0".
  - `:577-578`: "The fresh-node case, which every caller with a new source per send pays".
  - `:645-646`, the fresh test's DEFINITION: "This is the cost of a caller that creates a source per send — a per-request timeout — and of the first registrations on any token."
  - `dotnet/design/history/M11/P3.5-producer-non-blocking-pump-admission/PLAN.md:574`: "**Fresh node** (a new `CancellationTokenSource` per measurement — e.g. a per-send timeout token): +760 | +768 B, ceiling 800 B".
  - The same mis-scope is in 91.9's own text ("That includes: a fresh `CancellationTokenSource` per send … the first registrations on any token"), so the Actor copied it faithfully.
- What the guard actually measures: a `Register` on a source whose registration table **already exists** but which has **no free node**. The test's own warm call creates that state (`:640-641`, "Its warm call still keeps the source's one-time table out of the window"). A caller with a new source per send never reaches that state, because every send it makes is the **first** `Register` on its source. On .NET Core that first `Register` allocates the source's `Registrations` object as well as the node.
- Evidence (scratch probes outside the repo; Release build, 0 errors; net8.0.30 and net10.0.11, all rounds identical after round 0):
  - (a) `Register(Action<object?>, object?)`, the overload `CancellableFirstStage.Start` uses (`SendAccumulator.cs:531`), costs, per call:
    - **144 B** as the first registration on a new source;
    - **80 B** on a source with one live registration and no free node;
    - **0 B** when a node is recycled.
    So the table is 64 B, on both runtimes.
  - (b) I replicated the saturated D2 (c) path: `WaitAsync` on a saturated `SemaphoreSlim`, a stage TCS with RCA, `Register`, and a `ContinueWith` that settles and disposes. Over 64 sends:
    - warmed source, no free node: 760 | 768 B per send;
    - a new source per send, with the sources built outside the window as the caller's own allocation: 825 | 833 B per send.
    - The difference is +65 B per send: the 64 B table, plus 64 B once in total for the fresh gate's first waiter.
  - In the real test's marginal terms, then, a new source per send pays about **+824 B (net8.0) | +832 B (net10.0)**. That is 24 / 32 B over `FreshNodeCancelableStageMarginalCeilingBytes = 800`. "The first registrations on any token" is likewise off by 64 B for the very first one.
- Why it matters:
  - 91.9 exists because the §17 figure and the test's DEFINITION did not cover a named caller shape.
  - The fix names the most common such shape, a per-request timeout token, as covered by the +760 | +768 B figure and the 800 B guard. Neither covers it: that caller is 64 B per send above the figure, and the guard would go red for it.
  - The guard itself is sound and stable for what it measures. Only the scope it claims is wrong.
- Expected fix (no production change). Either option works:
  - Reword the four sites (three in the test, PLAN `:574`). The fresh-node figure is for a source that has already registered once but has no free node, for example more pending saturated sends on one long-lived token than it has freed nodes. A new source per send, such as a per-request timeout, also pays that source's one-time ~64 B registration table on every saturated send: about +824 | +832 B.
  - Or add the per-send-source case as its own guard: a new source for each of the 64 measured sends, built outside the window, with a ceiling of e.g. 864 B (832 + 32). Then the per-request-timeout wording becomes true for that guard.
- Rule suggestion (`ffi-marshalling.md` §A4, "Allocation budget" tests): when a budget's DEFINITION says which caller shape it covers, the measurement must start in that shape's state. A warm-up that removes a one-time cost also removes it for every caller that pays that cost on each call. Name what the warm-up excludes, and which callers pay it anyway.
- **Resolution (fixup `ea7dad3f`), test part. PLAN `:574` is the Manager's and was not touched.** Both expected-fix options were taken.
  - Reworded. The recycled-node ceiling comment (`SendAccumulatorFirstStageTests.cs:572-579`), the first test's DEFINITION (`:620-622`), the fresh-node ceiling comment (`:584-593`) and the fresh-node DEFINITION (`:653-665`) no longer claim the fresh-node figure covers "a new `CancellationTokenSource` per send / a per-request timeout". They say it covers a source that has registered before (its warm call creates the table) and has no free node, for example one long-lived token with more saturated sends pending on it than it has freed nodes. Each one names the per-send-source case as measured separately.
  - New guard `Admission_SaturatedFirstStage_WithANewTokenSourcePerSend_PerSendAllocation_IsMeasured_AndStaysUnderItsCeiling` (`:692`), with its own ceiling `NewSourcePerSendCancelableStageMarginalCeilingBytes = 864` (`:595-602`): the larger measurement (832 B) plus 32 B, which is less than one more 72 B `Task<T>`. Each of the 64 measured sends uses its own new source, so every measured `Register` is the first on its source. The warm call uses one more source of its own. All 65 sources are built before the window, stay referenced through it, and are disposed after every send has resolved (`MeasurePerSendWithANewSourceEach`, `:723`), so neither their construction nor their disposal is charged.
  - Measured with the same definition (caller-thread `GC.GetAllocatedBytesForCurrentThread`, per send, over 64 sends, best of 4, marginal over the 128 B fast path). 3 runs per TFM, all identical:
    - recycled node: **+680 B net8.0 / +688 B net10.0** (ceiling 720 B);
    - fresh node: **+760 B / +768 B** (ceiling 800 B);
    - new source per send: **+824 B / +832 B** (ceiling 864 B). This matches the Critic's estimate.
  - Sanity injection: one 72 B allocation (`GC.KeepAlive(new byte[48])`) in `CancellableFirstStage.Start`. The build succeeded (0 errors). All three cancelable guards went red on both TFMs: recycled +752/+760 > 720, fresh +832/+840 > 800, per-send source +896/+904 > 864. The plain guard stayed at +536/+544. `SendAccumulator.cs` was reverted with `git checkout --`.
  - Gates: `build-dotnet` gave 0 warnings and 0 errors. `test-dotnet` passed 2956/2956 on net8.0 and on net10.0, and soak passed 165/165 on each. Format was clean.

## 91.11 `IAsyncProducer.cs:137-142` and `:205-207` say the buffer borrow ends when the delivery task completes ("either await the delivery task first"). On both cancellation paths that advice is unsafe: the delivery task is Canceled, or never reachable, while the record is still in the chain, pinned and not yet sent — MEDIUM
- Where:
  - `dotnet/src/Confluent.Kafka/IAsyncProducer.cs:137`: "Do not mutate the key / value buffers until the delivery task completes." `:141-142`: "If you need to reuse a buffer, either await the delivery task first or hand each send its own array."
  - `:205-207` (the callback overload): the same claim, "described in full there".
  - The cancellation paragraph `:95-115` says the record "is **still sent** … and its delivery task is cancelled", but says nothing about the buffers.
- Why it is wrong on HEAD:
  1. **Stage-1 OCE (D2 (c), new in S3 `14bf366e`).** The token callback ends the caller's stage with `TrySetCanceled(callerToken)` (`Internal/SendAccumulator.cs:535`) while the wait runs on, so the caller never receives the delivery task. `SendViaPump`'s registration also cancels it (`Internal/NativeProducer.cs:547-552`). The record was appended before the wait (`SendAccumulator.cs:401`). Its pins moved to the node (`:700-702`), and they are released only by the batch thread after `send_batch` (`SendNode`'s `finally`, `:1339`). So after this OCE the buffers are still borrowed and the record is still going out. There is no delivery task to await, and the caveat is not stated anywhere.
  2. **The token fires after stage 1 completed, before the batch thread takes the node.** `NativeProducer.cs:552` completes the delivery task Canceled at once, while the record stays in the chain. "Until the delivery task completes" is then satisfied too early: a caller who follows the doc (await it, catch the OCE, reuse the buffer) puts the mutated bytes on the wire. This has existed since the best-effort cancel (`104b0b2c` `IAsyncProducer.cs:111-115`, "await this task first"). S3 rewrote these paragraphs and kept the claim unqualified.
  - Consequence: this is silent wrong bytes on the wire, not memory unsafety, because the pins stay in place. It is a natural pattern to hit: the doc warns that retrying after an OCE "can duplicate", which invites reusing or re-filling a pooled buffer after the OCE.
- The completion that IS safe, verified on HEAD: every delivery-task completion other than the token's (`NativeProducer.cs:552`) happens after the node's pins are released. These are `SendAccumulator.cs:1438`, `:1448`, `:1561` and the pump's `SendCompletionPump.cs:293`, `:752`, `:764`, `:841`, `:889`, all of which act on nodes or futures after `ReleasePins`. So "completes **without being canceled**" is correct.
- Settle signals after a cancellation, verified on HEAD:
  - **The record's delivery callback firing** (callback overload). Every firing site runs after `ReleasePins`. `SendNode` releases in its `finally` (`:1339`) before `CompleteNode` (`:1355`), which fires the core-error and null-future callbacks (`:1437`, `:1447`) and hands the futures to the pump (`:1491`), which fires them later (`SendCompletionPump.cs:740`, `:751`, `:763`). `FaultNode` fires (`:1558`) only after `ReleasePins` (`:1070`, or `SendNode`'s `finally`). Limit: the callback is at-most-once at the recorded residuals. `FaultNode` destroys a core-accepted future unread and fires nothing (`:1540-1559`). The pins were already released there, but the caller cannot observe it.
  - **A `Flush` that completes successfully.** `FlushAfterDrain` awaits `DrainPendingAsync` (`NativeProducer.cs:326-330`). That completes only once every record appended before the call has been handed to `send_batch` and its future enqueued to the pump (`SendAccumulator.cs:768-771`). The sync form throws if the drain bound expires (`NativeProducer.cs:842-848`). A Flush whose own token canceled it does not count, because that token cancels only the wait (`SendAccumulator.cs:781`).
  - **NOT `Close` / `Dispose` returning.** `Stop`'s join is bounded (`SendAccumulator.cs:954`). When it expires, the batch thread is abandoned and may still be inside `send_batch` (`NativeProducer.cs:1034-1039`).
- `IDeliveryCallback.cs` makes no buffer claim (no mutate, borrow or buffer wording), so it needs no change. An optional addition: its invocation ends the record's borrow.
- Expected fix (doc only, no code change):
  - In `:137-142` and `:205-207`, change "until the delivery task completes" to "until the delivery task completes **without being canceled**". Then add: after a cancellation (an `OperationCanceledException` from either stage, or a Canceled delivery task) the record may still be inside the binding, and its buffers stay borrowed until its delivery callback fires (callback overload) or a later `Flush` completes successfully. `Close` / `Dispose` returning is not that guarantee.
  - Add one cross-reference clause to the cancellation paragraph (`:95-115`), e.g. "… and its buffers stay borrowed; see `Send`'s remarks".
- **Resolution (fixup `e76e88ff`). Doc only, no code change.** Every cited site was re-verified on HEAD `cbecd70c` before writing:
  - the stage-1 token callback (`SendAccumulator.cs:535`) and the delivery-task token callback (`NativeProducer.cs:547-552`);
  - the append before the wait (`SendAccumulator.cs:401`), the pins moved to the node (`:700-702`), and their release in `SendNode`'s `finally` (`:1339`) before `CompleteNode` (`:1355`);
  - the callback sites `:1437`, `:1447` and `:1558`, and the pump's `SendCompletionPump.cs:740`, `:751` and `:763`;
  - `DrainPendingAsync` (`SendAccumulator.cs:768-771`, token at `:781`), which sits behind `FlushAfterDrain` (`NativeProducer.cs:326-330`), and the sync drain bound (`:842-848`);
  - the bounded `Stop` join (`SendAccumulator.cs:954`). `StopPump` and `StopPumpAsync` both reach it through `StopAccumulator` (`NativeProducer.cs:1224` / `:1296`), so `Close`, `Dispose` and `DisposeAsync` all share the bound.
  - `IAsyncProducer.cs:138-144` (the `Send(record, token)` remarks) now reads "until the delivery task completes **without being canceled**". The reuse advice now says to await the delivery task "and see it complete without being canceled".
  - New paragraph `:145-158`: "A cancellation does not end the borrow either." After a cancellation (an `OperationCanceledException` from either stage, or a delivery task that completes canceled), the record may still be inside the binding and is still sent. Its buffers stay borrowed until its delivery callback fires (the callback overload) or a later `Flush` completes successfully. `Close` / `Dispose` / `DisposeAsync` returning is not that guarantee, because teardown's wait for the send-batch thread is bounded. The paragraph also says that an already-canceled token appends nothing and so leaves nothing borrowed, which keeps it consistent with the D2 (c) bullets.
  - `:221-229` (the callback overload) gets the same qualifier and a short form of the caveat: the buffers stay borrowed until `callback` fires or a later `Flush` completes successfully, and `Close` or disposal returning is not that guarantee.
  - The cancellation paragraph in the type remarks (`:101-102`) now ends "… its delivery task is cancelled (as in the Python binding's async `send`). Its buffers stay borrowed too; see `Send(ProducerRecord, CancellationToken)`'s remarks." The cross-reference comes after the Python parenthetical, so it does not claim Python parity for the buffers.
  - `IDeliveryCallback.cs` is unchanged. It makes no buffer claim, and the optional addition was not taken.
  - Every new cref resolves in the emitted XML: `Send(…, IDeliveryCallback, …)`, `Flush(CancellationToken)`, `Close(CancellationToken)`, `IDisposable.Dispose`, `IAsyncDisposable.DisposeAsync` and `OperationCanceledException`. There are 0 unresolved `!:` crefs.
  - Gates: `build-dotnet` gave 0 warnings and 0 errors. `dotnet format --verify-no-changes` was clean. Soak passed 165/165 on net8.0 and on net10.0.
  - Residual, not fixed because it is a rule file and out of scope for this cycle: `ffi-marshalling.md:658` (§A4) still says "a buffer may be reused only after the delivery `Task` completes", with no qualifier. It is the rule-side twin of this item and is flagged for the Manager.

## 91.12 `soak/SoakClient/SoakClient.cs:908-910` still cites "the max.block.ms admission timeout … (ffi-marshalling.md §A1)". There has been no admission timeout since P3.4, and S5's R8 removed the §A1 text it cites — LOW
- Where: `dotnet/soak/SoakClient/SoakClient.cs:906-910`: "the async Send throws synchronously only for preconditions and serialization failures, and surfaces operational failures — including the max.block.ms admission timeout — through the delivery Task instead (ffi-marshalling.md §A1)."
- Why it is wrong: the wait in `SubmitAdmitted` has no timeout. It is `_admission.WaitAsync(_spaceGate.Token)` (`Internal/SendAccumulator.cs:420`), ended only by a permit or by teardown, and stage 1 never fails. `ffi-marshalling.md:311-317` (R8) now says "there is no admission timeout". The buffer-exhaustion outcome comes from the core inside `send_batch`.
  - This phase touched this very line. The S2 fixup `c0323fec` changed "returned Task" to "delivery Task" on `:909` and kept the stale clause. That is my FN from pass 1.
- Expected fix (comment only): replace the clause with the current fact. For example: "…surfaces operational failures — such as the core's buffer-exhaustion (`max.block.ms`) inside `send_batch` — through the delivery Task instead (ffi-marshalling.md §A1)". Or drop the parenthetical example. No code change.
- **Resolution (fixup `0149f6d9`). Comment only, no code change.** `SoakClient.cs:908-910` now reads: "…and surfaces operational failures — such as the core's buffer exhaustion (max.block.ms) inside send_batch — through the delivery Task instead (ffi-marshalling.md §A1)." That is the first form of the expected fix. The §A1 reference is kept because §A1 still states this fact ("Java's buffer-exhaustion outcome comes from the core inside `send_batch` and faults the delivery `Task`"). The no-timeout premise was re-verified at `SendAccumulator.cs:420` (`_admission.WaitAsync(_spaceGate.Token)`).
  - Gates: soak passed 165/165 on net8.0 and on net10.0, with 0 warnings and 0 errors. Format was clean.

## 91.13 `AsyncKafkaProducer.cs:73` and `AsyncMockProducer.cs:70`, unowned twins of 91.11's headline, still say "until its delivery task completes" with no qualifier — LOW
- Where: the class remarks of both public concrete types read: "⚠ **Do not mutate a record's key / value buffers until its delivery task completes** (M11/P3.1 decision D6). … See `IAsyncProducer{TKey, TValue}`'s `Send` for the full note." Both were last touched by S3 `14bf366e`.
- Why it is wrong: `e76e88ff` established, and `IAsyncProducer.cs:138-158` now states, that a delivery task that completes **canceled** does not end the borrow. The record can still be in the chain, pinned and not yet sent: `SendViaPump`'s registration cancels it (`NativeProducer.cs:547-552`), and the pins are released only in `SendNode`'s `finally` (`SendAccumulator.cs:1339`).
  - The fixup qualified only the interface. These two headlines still state the unqualified rule that 91.11 found unsafe.
  - On `AsyncKafkaProducer` the paragraph just above (`:67-70`) says a canceled token cancels "the record's delivery task". Read together, the two paragraphs give the 91.11 recipe: cancel, see the task complete, reuse the buffer, and the mutated bytes go on the wire.
  - These remarks are what a user sees on the type they construct, since the members are `<inheritdoc/>`. A "see … for the full note" pointer does not fix a headline that is false.
- Expected fix (doc only, no code change):
  - Add "without being canceled" to both headlines, e.g. "… until its delivery task completes without being canceled — a cancellation does not end the borrow", and keep the pointer.
  - Afterwards, grep `delivery task completes` across `src/`. On HEAD the only hits are these two and the two already-fixed `IAsyncProducer.cs` sites. The `ffi-marshalling.md:658` rule-side twin is excluded, as already flagged for the Manager.
- **Resolution (fixup `20ac9ea2`). Doc only, no code change.** Both class remarks (`AsyncKafkaProducer.cs:72-78`, `AsyncMockProducer.cs:69-75`) now read: "⚠ **Do not mutate a record's key / value buffers until its delivery task completes without being canceled** (M11/P3.1 decision D6). The async send is deferred — the binding borrows the serialized bytes until a background batch thread hands the record to the core — so a mutation in that window is visible on the wire. A cancellation does not end the borrow; see `IAsyncProducer{TKey, TValue}`'s `Send` for the full note. The **synchronous** producer has no such window." The cancellation clause is merged into the existing cross-reference sentence, so the pointer still appears once.
  - The line break sits after "until", so "its delivery task completes without being canceled" stays on one line and a grep for the headline still finds these sites and filters them.
  - Sweep: a comment-joined scan of every `.cs` under `dotnet/src` (bin/obj excluded) finds 0 unqualified "delivery task completes" and 4 qualified ones: these two and the two `IAsyncProducer.cs` sites. The same scan flags HEAD's `AsyncKafkaProducer.cs` before this fixup, which is the control positive. The brief's literal one-line grep still prints `IAsyncProducer.cs:138` and `:221`. Those sentences are already qualified, but they wrap between "without being" and "canceled". `e76e88ff` fixed them and this fixup did not touch them. `ffi-marshalling.md:658` stays excluded: it is a rule file, already flagged for the Manager.
  - Gates: `build-dotnet` (Release, all three TFMs) gave 0 warnings and 0 errors. The fresh Release XML has the new text in both class remarks. `dotnet format --verify-no-changes` was clean.
