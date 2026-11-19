# COMMENTS.DONE.93 — Actor 93, M11/P4.2

## C93-1 [severity: low] `EnqueueSingle`'s residual-4 note counts its throw sources, and the count is wrong

**Commit:** `b838e120` (M11/P4.2 S2).
**Where:** `dotnet/src/Confluent.Kafka/Internal/SendCompletionPump.cs:320-324` (the `<remarks>` paragraph "Ownership of `future` transfers on return, and only on return"), with a second instance of the same shape at `:335` (`<param name="completion">`).

**Problem.** The residual-4 site note lists the method's throw sources and counts them: "If this method throws — allocating the entry, or the queue growing, **both** out-of-memory only — nothing was queued and nothing was freed". That is a count of throw sources in a site note, which ffi-marshalling.md §A6 form C's round-5 amendment forbids outside the canonical enumeration ("**no** count of residuals, sites or throw sources"). The rule exists because counts like this go stale, and this one is already incomplete as written. The closed-gate branch of the same gate, `EnqueueEntry`, has two more allocation-only throw points. Both come before that branch frees the future:
- `:361` `entry.Fault(TeardownException())`. `TeardownException()` allocates a fresh `KafkaException` on every call.
- `:1185` `PendingSyncSend.Fault` → `SyncCompletion.TrySetException` → `ExceptionDispatchInfo.Capture` (`SyncCompletion.cs:117`). This allocates under the latch's lock, before `_done` is set.

An OOM at either point throws out of `EnqueueSingle` after the core accepted the record, with nothing faulted and nothing freed. That is residual 4's outcome, reached by neither of the two sources the note names. The note's conclusion ("nothing was queued and nothing was freed, so the caller still owns the future") still holds on those paths. Only the list of sources and its "both" are false.

The `completion` param doc at `:335` has the same shape: "completed exactly once, **on the pump or here**". It leaves out Stop's terminal drain (`DrainAndFaultRemaining`, residual 2). That drain completes the latch on the thread calling `Stop`, after the join, so it is neither on the pump thread nor in this call. (In this file "on the pump" means the pump thread: "fires on the pump", T10's "the callback runs on the pump".)

**Evidence.** Reading `EnqueueEntry` `:352-369` and `SyncCompletion.TrySetException` `:108-119`. Every throw point is an allocation, and every one of them comes before `entry.DestroyFutures()`. So the ownership contract is right, and only the enumeration in the note is wrong. The note was introduced by this commit. The pre-existing batch `Enqueue` note (`:270-274`) states only the ownership rule and points at `SendAccumulator.CompleteNode`, with no list of sources.

**Suggested fix (doc only).** Remove the list and the count, and state only the local fact. For example: "If this method throws (out of memory only, on either branch of the gate), nothing was queued and nothing was freed, so the caller still owns the future and must destroy it — recorded residual 4 on `IDeliveryCallback`." For `:335`, write "completed exactly once" with no list of sites, or name all three: the pump, this call, and `Stop`'s terminal drain. Do not add a new count.

Resolved in 346b5f04: doc-only. `EnqueueSingle` remarks now state the local fact only ("If this method throws (out of memory), nothing was queued and nothing was freed, so the caller still owns the future and must destroy it, unread: recorded residual 4 ...") with no list of throw sources and no "both"; the `completion` param reads "completed exactly once" with no site list. Same-shape sweep of the S2-added notes found one more: `ProcessSingle` remarks listed the residual-3 throw sources ("out of the blocking get itself ..., or out of reading what it reported"), incomplete — the inner `finally`'s `RecordMetadataDestroy` can also throw before the success-path `Fire` — so it now reads "A throw that escapes this method before the delivery callback is invoked (for example an EntryPointNotFoundException out of the blocking get against a stale native) ...", and the "no completion has been turned into a result" clause (false on that path) is dropped. Comment-only diff verified; build 0/0, unit 2994/TFM, soak 165/TFM.

## C93-2 [severity: low] Four "the sync `Send` returns `RecordMetadata`" type claims that S3's type-truth pass missed, outside both S5 lists

**Commit:** `c9813476` (M11/P4.2 S3), whose doc scope was "type-truth docs only" (PLAN §8 S3 row).

**Where.** Each item states the return type of the sync `Send`, and since S3 that type is `KafkaFuture<RecordMetadata>`:
1. `dotnet/src/Confluent.Kafka/RecordMetadata.cs:21-23`. This is the public type's `<summary>`, with live crefs:
   ```
   /// Java's <c>org.apache.kafka.clients.producer.RecordMetadata</c>, returned by
   /// <see cref="IAsyncProducer{TKey, TValue}.Send(ProducerRecord{TKey, TValue}, System.Threading.CancellationToken)"/> /
   /// <see cref="IProducer{TKey, TValue}.Send(ProducerRecord{TKey, TValue})"/> — and delivered to an
   ```
   S3 made the `IProducer` half false. The `IAsyncProducer` half has been false since P3.5/P3.6 (`ValueTask<AsyncKafkaFuture<RecordMetadata>>`, last touched by the `777dfa83` move). So after S3 neither cref is true.
2. `dotnet/grpc-server/AsyncProducerServiceImpl.cs:42-46`:
   - It says the sync servicer "blocks on the binding's `<c>RecordMetadata Send(record)</c>`". That is a signature claim, and the sync servicer now calls `producer.Send(record).Get()` (`ProducerServiceImpl.cs:191`).
   - The same sentence says the send pump is "the exact machinery the sync path never covers". Since D4 the sync path runs on the same send-completion pump; what it still never covers is the `TaskCompletionSource` / accumulator half.
   - Twin in the file S3 did edit: `ProducerServiceImpl.cs:33`, "the blocking `<c>Send</c>` parks the handler thread". S3's own rewrite at `:167-169` says `Get()` is now what blocks, so the class summary contradicts the method comment below it.
3. `dotnet/tests/Confluent.Kafka.UnitTests/PublicProducerFirstStageTests.cs:55`: "the synchronous `<c>Send</c>` returns the `<see cref="RecordMetadata"/>` itself, so it has no first stage". The conclusion still holds; the stated reason is now a false type claim.
4. `dotnet/tests/Confluent.Kafka.UnitTests/PublicSyncProducerSendTests.cs:53`: the section comment "Auto-complete: Send blocks briefly and returns the metadata directly". S3 fixed the twin section comment at `:89` of the same file ("Manual mock: Get blocks …") but not this one.

**Why it is wrong.** In PLAN §8, the S3 row assigns type-truth docs to S3. None of these four lines is in the Actor's stale-narrative list (PLAN §16.1, S3 record). None is in the S5 row's file list either: IProducer / KafkaProducer / MockProducer / IDeliveryCallback / NativeProducer / the pump class doc / NativeMethods. So S5 will not reach them. Item 1 is the most visible: IntelliSense shows that summary on every `RecordMetadata` the user holds, and it now points at two methods that do not return one.

**How found.** A joined-comment-block scan of `dotnet/src` for sentences that name a sync `Send` alongside "returned by / returns the / blocks until / inline on". Then `git grep -nE 'RecordMetadata Send\(|returns? the (published )?(record.s )?<see cref="RecordMetadata"|returns the metadata directly|RecordMetadata"/> directly' HEAD -- dotnet ':!dotnet/design/history'`. The remaining hits are approved S5 text (`dotnet/CLAUDE.md:189-190` = PLAN E1, `IProducer.cs:51` = PLAN S5) or already correct.

**Minimal fix (doc only).**
1. RecordMetadata.cs: say what each send actually returns, for example "…`RecordMetadata`, yielded by the delivery handle of `IAsyncProducer.Send` (`AsyncKafkaFuture{T}.Get`'s task) / `IProducer.Send` (`KafkaFuture{T}.Get`) — and delivered to an …". Keep the crefs.
2. AsyncProducerServiceImpl:
   - Change the signature claim to "blocks in the binding's `Send(record).Get()`".
   - Narrow "the exact machinery the sync path never covers" to the `TaskCompletionSource` / accumulator path.
   - ProducerServiceImpl `:33`: change "the blocking `Send`" to "the blocking `Get()`".
3. FirstStageTests: "…the synchronous `Send` returns a `KafkaFuture{T}` once the core accepts the record, with no first stage…".
4. SyncSendTests: "Auto-complete: Send returns at once; Get returns the metadata".

**Resolution:** fixed in `7830cf11` (`fixup!` of `c9813476`), doc/comment only. `RecordMetadata.cs:19-27`: the summary now says the metadata is yielded by the delivery future a `Send` hands back — the `Task` from `AsyncKafkaFuture{T}.Get()` on the future that `IAsyncProducer.Send`'s `ValueTask` yields, and `KafkaFuture{T}.Get()` on the `KafkaFuture{T}` that `IProducer.Send` returns (both `Send` crefs kept; every cref resolves, 0W). `AsyncProducerServiceImpl.cs:42-46`: the sync servicer "blocks in the binding's `Send(record).Get()`"; both servicers' futures are resolved by the same send-completion pump, and what the sync path never covers is narrowed to the send accumulator and the `TaskCompletionSource`-backed `Task<RecordMetadata>`. `ProducerServiceImpl.cs:33-34`: "the blocking `Get()` on the future `Send` returns parks the handler thread". The D-3 unbounded wait (`ProducerServiceImpl.cs:171-185`) is untouched and still unbounded. `PublicProducerFirstStageTests.cs:55-57`: "the synchronous `Send` returns a `KafkaFuture{T}` once the core has accepted the record, so it has no first stage, …". `PublicSyncProducerSendTests.cs:53`: "Auto-complete: Send returns the future at once; Get returns the metadata". Gates at `a65fcf4e`: build 0W/0E; unit 3010/TFM; soak 165/TFM; perf-unit 39/TFM; grpc-server Release 0W/0E and `dotnet format --verify-no-changes` clean.

## C93-3 [severity: low] C11's closed-handle acceptance rests on a false runtime claim, and on net8.0 / net10.0 its kept "DangerousAddRef" disjunct is a string no `Send` can produce

**Commits:**
- `93a42813`: the C11 comment and the `releasedHandle` disjunct.
- `bc5926a5`: `SafeHandleClosedMessage()`.
- `638d12b7`: the S4 and S4m record.

**Where.**
- `dotnet/tests/Confluent.Kafka.UnitTests/PublicSyncProducerKafkaFutureTests.cs:473-476`:
  ```
  // The P/Invoke marshaller's own refusal of the released SafeProducerHandle that Send passes to
  // Producer_send: on net8.0 and net10.0 it names the handle's type, and it is not DangerousAddRef's
  // message above (observed by mutating EnsurePump's re-check away, M11/P4.2 S4m). DangerousAddRef's
  // stays accepted because this project also builds for net462, where the marshaller was not measured.
  ```
- `:672-689`, `SafeHandleClosedMessage()`. Its summary reads "The runtime's own message for a SafeHandle used after release", but it probes a disposed **`SafeWaitHandle`**.
- PLAN §16.1 says the same thing twice:
  - The S4 "Deviations" bullet quotes the accepted SafeHandle outcome as "Safe handle has been closed.".
  - The S4m fixup bullet says the marshaler "instead refuses" with the `SafeProducerHandle`-named message, and that the fix "keeps DangerousAddRef's".

**Evidence.** A scratch console, run on net8.0.30 and on net10.0.11, printed identical output on both:
```
SafeWaitHandle.DangerousAddRef: Cannot access a disposed object.\nObject name: 'Microsoft.Win32.SafeHandles.SafeWaitHandle'.
MyProbeHandle.DangerousAddRef:  Cannot access a disposed object.\nObject name: 'MyProbeHandle'.
P/Invoke, closed MyProbeHandle: Cannot access a disposed object.\nObject name: 'MyProbeHandle'.
```
- The marshaler's refusal **is** DangerousAddRef's message.
- On .NET 7+, DangerousAddRef names the handle's runtime type.
- The two expected strings in C11 differ only because the helper probes a different type.

**Why it is wrong.**
1. The unit tests run on net8.0 and net10.0. On both, `safeHandleClosed` names `SafeWaitHandle`. No `Send` path disposes a `SafeWaitHandle`, so that disjunct can never match: it is dead on every TFM the tests run.
2. The comment's mechanism, "it is not DangerousAddRef's message", is false, and the S4m record repeats it.
   - On net462 the roles swap. .NET Framework's DangerousAddRef message does not depend on the handle type ("Safe handle has been closed."; not measurable here).
   - So on net462 `safeHandleClosed` is the live disjunct and `releasedHandle` is the dead one.
3. Behaviour is right on all three TFMs, so nothing is masked. What is wrong is the stated reason for the accepted outcome set, and that reason is exactly what S4m's C11 diagnosis was about. Anyone reusing `SafeHandleClosedMessage()` for another handle type on .NET 8+ gets a string that matches nothing.

**Minimal fix.**
1. Replace both disjuncts with a single string, probed from the type that `Send` actually passes.
   - Call `DangerousAddRef` on a disposed `SafeProducerHandle`: for example `(SafeHandle)Activator.CreateInstance(typeof(SafeProducerHandle), nonPublic: true)!` after `Dispose()`, or the disposed mock's own handle.
   - This is exact on every TFM, because both runtimes' marshalers go through DangerousAddRef.
2. Correct the comment at `:473-476` and the helper's summary.
3. In the PLAN §16.1 S4 and S4m bullets, say that the marshaler refuses with DangerousAddRef's message, that on .NET 7+ this message names the handle's runtime type, and that the old expected string had been probed on a `SafeWaitHandle`.
4. If the two-string form is kept, still make the comment and record fixes in steps 2 and 3.

**Resolution:** fixed in `a65fcf4e` (`fixup!` of `bc5926a5`), test-only; minimal-fix steps 1–2. C11 (`PublicSyncProducerKafkaFutureTests.cs:472-477`, `:506-510`) now accepts, besides `NativeProducer`'s closed-check message, exactly **one** released-handle string: `ReleasedSafeProducerHandleMessage()` (`:670-712`), the message `DangerousAddRef` throws on the runtime under test for a released `SafeProducerHandle`, the type `Send` passes to `Producer_send`. The probe is `Activator.CreateInstance(typeof(SafeProducerHandle), nonPublic: true)`. **Safety:** that private ctor leaves `handle == IntPtr.Zero`, so `SafeHandleZeroIsInvalid.IsInvalid` is true and `Dispose` closes the handle without calling `ReleaseHandle` — no `Producer_destroy`, no native call at all; the helper checks `IsInvalid` before `Dispose` and, if it were ever false, retires the probe with `SetHandleAsInvalid` (which also skips `ReleaseHandle`) before failing. The `SafeWaitHandle` probe and its dead disjunct are gone; the comment and the helper summary state no per-TFM text and no "not DangerousAddRef's message" claim. Scratch check (a temporary assert, not committed): on net8.0 and net10.0 the probe equals the old hard-coded `new ObjectDisposedException(typeof(SafeProducerHandle).FullName).Message` that S4m observed from the marshaller, so that outcome is still accepted with its exact message and the pump-left-running assertion stays the only red for a leaked pump. C11 + `EnsurePump_AfterTheCloseLatch_Throws_AndStartsNoPump`: 3× per TFM, 2/2 each run. **Step 3 (the PLAN §16.1 S4 / S4m bullets) is not done here:** the Actor must not edit PLAN.md, so that record correction is left to the Manager.

## C93-4 [severity: low] The canonical residual-4 sync entry lists its triggers, and the list leaves out the closed-gate branch

**Commit:** `507ff98e` (M11/P4.2 S5a).
**Where:** `dotnet/src/Confluent.Kafka/IDeliveryCallback.cs:245-248`, the **Sync** half of residual 4:

```
/// <b>Sync</b> (M11/P4.2) — inside <c>Send</c>, on the caller's thread, between
/// <c>Producer_send</c> returning the live future and its hand-over to the pump. In practice an
/// <see cref="System.OutOfMemoryException"/> (the queue entry, or the pump's queue growing). The
/// future is destroyed unread and <c>Send</c> rethrows.
```

**Problem.** The site and its number are right. The trigger list is not. The hand-over is `EnqueueSingle` → `EnqueueEntry`. Its **closed-gate** branch (`Internal/SendCompletionPump.cs:400-409`) can also throw before it frees the future:
- `TeardownException()` allocates a `KafkaException` (`:1121-1122`).
- `PendingSyncSend.Fault` (`:1228`) → `SyncCompletion.TrySetException` allocates `ExceptionDispatchInfo.Capture` (`SyncCompletion.cs:117`).

Either OOM leaves `EnqueueSingle` and lands in `NativeProducer.Send`'s `catch` (`Internal/NativeProducer.cs:715-722`), which is this residual's site. These are the two points C93-1 recorded against the site note. That note now says only "(out of memory)", which is correct. The canonical list now names the open-branch sources only.

This paragraph is the one place allowed to list triggers (ffi §A6 form C, round 5), and the block calls itself exhaustive. So a short list here is the drift that rule exists to prevent.

The **async** half at `:231-234` has the same shape, and it predates this commit: "(the pump's queue growing)". It leaves out the `new PendingSendBatch` allocation (`SendCompletionPump.cs:346`) and the closed-gate branch, both of which reach `FaultNode` the same way. Fix both together, so the twins do not drift apart.

**Suggested fix (doc only).** Do not add a count. In the sync entry, change the parenthetical to "on either branch of the pump's enqueue gate: allocating the queue entry, the queue growing, or faulting the latch in place when the gate has already closed". Alternatively, drop the parenthetical and keep "In practice an `OutOfMemoryException`, on either branch of the pump's enqueue gate". Make the matching change to the async entry's "(the pump's queue growing)".

**Resolution:** fixed in `5d759085` (`fixup!` of `507ff98e`), doc only. `IDeliveryCallback.cs:245-248` (sync half of residual 4): the trigger parenthetical "(the queue entry, or the pump's queue growing)" is dropped; the entry now reads "In practice an `OutOfMemoryException`, whether the pump's enqueue gate is still open or has already closed. The future is destroyed unread and `Send` rethrows." `:231-234` (async twin): "(the pump's queue growing)" is replaced the same way ("…, whether the pump's enqueue gate is still open or has already closed, or a P/Invoke failure from a later chunk of the same node"), so the twins agree. No count and no throw-source list was added; the enumeration's sites, numbers, threads and outcomes are unchanged. Gates at `3fa669e4`: build 0W/0E; Release XML 0 `cref="!:"` on netstandard2.0/net8.0/net10.0 (rebuilt after the edits); `dotnet format --verify-no-changes` clean; unit 3010/TFM (net8.0, net10.0); soak 165/TFM; no `Failed:` > 0, no `Test Run Aborted`.

## C93-5 [severity: low] The NativeMethods send-path section comment still says the sync flavor has no pump and completes on the caller

**Commit:** the text dates from M11/P3.1 (`52c10a6f` / `0204437a` lineage). S3 `c9813476` made it false. S5b `aab3b7fd` rewrote this file's sync-send section 140 lines further down (`:2422-2433`) and left this one. Fix it as a fixup of `aab3b7fd`.
**Where:** `dotnet/src/Confluent.Kafka/Internal/Interop/NativeMethods.cs:2280-2286`, the "COMPLETION side (BOTH flavors)" bullet of the `kafka_producer_Producer_t — the SEND path` section comment:

```
//   * COMPLETION side (BOTH flavors) — UNCHANGED. On the ASYNC flavor that is ffi §A7's
//     pull pump: [...]
//     KafkaFuture_RecordMetadata_destroy_all. The SYNC flavor has NO pump at all (verified
//     NativeProducer.cs:504) — it completes on the caller's own thread via the blocking
//     KafkaFuture_RecordMetadata_get (:603). Neither is touched: [...]
```

**Problem.** This is a present-tense claim about the current design, and it is false since D4 / D3:
- `NativeProducer.Send` calls `EnsurePump()` (`NativeProducer.cs:690`) and hands the future over with `pump.EnqueueSingle(...)` (`:713`).
- The pump, not the caller, makes the blocking singular `get`: `SendCompletionPump.ProcessSingle` → `NativeMethods.FutureRecordMetadataGet` (`SendCompletionPump.cs:737`). It then frees the future with the singular `_destroy` (`:782`).

The block's header (`:2252-2256`) says superseded parts are marked in place. S5b's own fix at `:2422-2433` states the new mechanics, so the file now contradicts itself across one section. The cited line numbers (`:504`, `:603`) are stale too; they predate this phase.

PLAN §16.1's S5 list does not name this line. A case-insensitive `no pump` grep over `Internal/Interop/NativeMethods.cs` does hit it, at `:2283`.

**Suggested fix (comment only).** Mark the sentence as history and point at the current text, without restating the pump's mechanics, for example: "The SYNC flavor had NO pump until M11/P4.2: it completed on the caller's own thread via the blocking KafkaFuture_RecordMetadata_get. Since M11/P4.2 (D4) it hands its future to the same pump as a single — see the section comment above FutureRecordMetadataGet." Drop the two stale `NativeProducer.cs` line cites, or replace them with member names.

**Resolution:** fixed in `3fa669e4` (`fixup!` of `aab3b7fd`), comment only. `NativeMethods.cs:2283-2287`: the present-tense claim is now history — "⚠ The SYNC flavor had NO pump until M11/P4.2: it completed on the caller's own thread via the blocking KafkaFuture_RecordMetadata_get. Since M11/P4.2 (D4) its single future is read by the same pump — see the section comment above FutureRecordMetadataGet. M11/P3.1 touched neither: that phase does NOT reopen pull-vs-push, …". The pump's mechanics are not restated, and the stale `NativeProducer.cs:504` / `:603` cites are dropped. Gates: as in C93-4's resolution.

## C93-6 [severity: low] The pump class doc calls RunLoop's catch a "teardown path"; the canonical enumeration says it is not one

**Commit:** `aab3b7fd` (S5b), new text.
**Where:** `dotnet/src/Confluent.Kafka/Internal/SendCompletionPump.cs:51-54`, the new "One queue for both entry kinds" paragraph:

```
/// practice a pump holds singles or groups, never both. They share one queue anyway, under a common
/// <see cref="PendingEntry"/> base, so one ordering and one set of teardown paths — the enqueue gate
/// (<see cref="EnqueueEntry"/>), the terminal drain (<see cref="DrainAndFaultRemaining"/>) and
/// <see cref="RunLoop"/>'s <c>catch</c> — cover both. <see cref="RunLoop"/> dispatches on the
```

**Problem.** `RunLoop`'s `catch` (`:648-671`) handles a throw out of `ProcessSingle` / `ProcessGroup` at any point in the producer's life: an OOM, or an `EntryPointNotFoundException` from the first read against a stale native. It is residual 3's site, and the canonical enumeration classifies it explicitly:
- `IDeliveryCallback.cs:195-196`: "An unexpected failure on the completion pump … — *not* a teardown path."
- `:269`: "residuals 1 and 2 are teardown; residuals 3 and 4 are unexpected failures".
- CLAUDE.md §4 (`:727-731`) uses the same *Teardown* vs *Non-teardown* split.

ffi §A6 form C (`ffi-marshalling.md:947-950`, and the round-5 amendment at `:955-966`) puts "teardown or not" among the residual comparisons that belong only in that enumeration. M14/P1's review rounds were spent on exactly this mistake: the boundary "under-stated … first by naming only teardown" (CLAUDE.md `:751-752`). A reader who takes this sentence at face value will conclude that the catch cannot fire in steady state.

**Suggested fix (comment only).** Do not re-scope the clause; neutralize it. Change "one set of teardown paths" to "one set of fault paths". That keeps the D11 (a) point, that both kinds share the same handling, and makes no teardown claim. Alternatively, drop the dash-delimited list and point at `IDeliveryCallback`'s remarks for the sites.

**Resolution:** fixed in `3fa669e4` (`fixup!` of `aab3b7fd`), comment only. `SendCompletionPump.cs:52`: "one set of teardown paths" → "one set of fault paths" (the finding's first fix; the dash-delimited site list is kept). Twin: the `_queue` field comment at `SendCompletionPump.cs:193` (from S2 `b838e120`) carried the same clause ("one set of teardown paths cover both entry kinds") and got the same one-word change, so no copy classifies the shared paths as teardown outside `IDeliveryCallback`'s enumeration (ffi §A6 round-4 grep). Gates: as in C93-4's resolution.

## C93-7 [severity: low] `CompleteNode`'s hand-over note says the closed gate "returns normally"; the canonical enumeration now says that branch can throw

**Commit:** text from `0204437a` (pre-phase). This phase made it contradictory: `5d759085`, and S2 `b838e120`. `5d759085` is itself a `fixup!` of `507ff98e`, so fix this as a `fixup!` of `aab3b7fd` (S5b, the internal-docs commit).
**Where:** `dotnet/src/Confluent.Kafka/Internal/SendAccumulator.cs:1487-1488`, the ownership note above `_pump.Enqueue(...)`:

```
// If Enqueue throws (its queue growing under out-of-memory — its own _stopped branch
// frees the futures and returns normally), ownership never transferred, so FaultNode
```

**Problem.** The parenthetical says where `Enqueue` throws: only when the queue grows. It also says the `_stopped` branch never throws. After this phase, both parts are wrong, and the canonical text now contradicts the second one.
- `5d759085` changed the async residual-4 entry (`IDeliveryCallback.cs:231-234`). This note is that entry's caller-side twin. The entry now says the OOM happens "whether the pump's enqueue gate is still open or has already closed". That matches the code. On the closed branch (`SendCompletionPump.cs:400-409`), `entry.Fault(TeardownException())` allocates a `KafkaException` (`:1121`). `PendingSendBatch.Fault` (`:1181-1187`) calls `TrySetException`. Both run before `entry.DestroyFutures()`. So the closed branch can leave by a throw with the futures still live. The note says it "returns normally".
- `b838e120` (S2) moved two things. The `_stopped` branch moved out of `Enqueue` into the shared `EnqueueEntry`, so "its own `_stopped` branch" no longer names code in `Enqueue`. The `new PendingSendBatch(...)` allocation moved from inside the open branch to before the gate (`:346`), so it now runs whatever state the gate is in. "Its queue growing" does not cover it.
- C93-4 removed this same kind of list from the canonical twin, and C93-1 removed it from `EnqueueSingle`'s note (`:365-368`, now "(out of memory)"). This note is the one copy left (ffi §A6 round 5, `ffi-marshalling.md:955-966`).

**The code is correct.** Since the nulling sits below the call, a closed-branch OOM partway through `Fault` still reaches `FaultNode` with live futures. `FaultNode` frees them once, and its `TrySetException` on an awaiter that is already faulted is a no-op. It fires no callback, because the core accepted the record. That case is exactly the reason the note says to keep the nulling below the call.

**Suggested fix (comment only).** Do not re-scope the list; delete it, as C93-1 did. Write "If Enqueue throws (out of memory), ownership never transferred, …". Keep the rest of the note.

**Not part of this finding:** `NativeProducer.cs:659` and `:710-711` ("also returns normally when the gate has closed"). They say what a normal return means on the closed gate: ownership still transfers. They assign no throw source. And `:667`'s "a throw out of the enqueue" covers either branch.

**Resolution:** fixed in `25303014` (`fixup!` of `aab3b7fd`), comment only; the finding's minimal fix. `SendAccumulator.cs:1487` (`CompleteNode`'s ⚠ hand-over note above `_pump.Enqueue(...)`): the parenthetical "(its queue growing under out-of-memory — its own _stopped branch frees the futures and returns normally)" is now "(out of memory)", as C93-1 did, so the sentence reads "If Enqueue throws (out of memory), ownership never transferred, so FaultNode must still see a live future at each of these indices and free it." The rest of the note (the nulling-order rationale and the duplicate-callback hazard) is unchanged, and no count, throw-source list or uniqueness quantifier was added (ffi §A6 round 5). Twin grep (`queue growing | _stopped branch | returns normally` over `dotnet/src`): `NativeProducer.cs:659` / `:710-711`, which the finding excludes, plus one unrelated consumer hit. Gates at `25303014`: build 0W/0E; `dotnet format --verify-no-changes` clean; unit 3010/TFM (net8.0, net10.0); soak 165/TFM; no `Failed:` > 0, no `Test Run Aborted`. Mode A: `git diff --stat d6ce0512..HEAD -- . ':!dotnet'` empty; `internal static extern` 219 + 357; 0 non-comment changed lines (3 changed, all `//`).

## C93-8 [severity: low] `CompleteNode`'s ⚠ hand-over note says nulling first would duplicate callbacks; the code says it would leak the futures and leave their awaiters pending

**Commit:** text from `0204437a` (pre-phase). This phase made it contradictory: S5b `aab3b7fd` added FaultNode's residual-4 remark (`SendAccumulator.cs:1528-1529`), "the throw came before its future was handed to the completion pump, and so nothing will read its completion". `25303014` then kept the sentence, because C93-7 said "keep the rest of the note" — C93-7 missed it. Fix as a `fixup!` of `aab3b7fd`.
**Where:** `dotnet/src/Confluent.Kafka/Internal/SendAccumulator.cs:1488-1491`:

```
// If Enqueue throws (out of memory), ownership never transferred, so FaultNode
// must still see a live future at each of these indices and free it. Nulling first would
// make those indices look like "the core never saw this record", and FaultNode would
// then fire delivery callbacks the pump is also about to fire — DUPLICATES, which the
// exactly-once obligation makes strictly worse than the drop (root CLAUDE.md §11.5).
```

**Problem.** The second sentence is false twice.
- **The pump would fire nothing.** The premise is a throw before the hand-over, so the pump never receives the group. `EnqueueEntry` queues only on the open branch (`SendCompletionPump.cs:412`), and the phase's own FaultNode remark quoted above says the same. With no second firing there is no duplicate. This holds even if only `Futures[i]` were zeroed first: `FaultNode`'s callback would then be a fabricated failure, not a duplicate.
- **`FaultNode` would fire nothing either.** The nulling below (`:1501-1509`) clears `Completions[i]` and `Deliveries[i]` as well as `Futures[i]`. The catch at `:1366` runs `FaultNode` from the unadvanced `settled` (`:1514` has not run). For a null `Completions` slot, `FaultNode` reaches the `continue` at `:1557-1561` before the `Fire` at `:1565`.

What nulling first would actually do: `FaultNode` would find no future to free and no awaiter to fault at those indices. The futures would leak and their awaiters would stay pending — the hang `FaultNode` exists to prevent (`:1518-1519`). The note leaves out this cost and names a hazard that reading `FaultNode` disproves. The ordering and the first sentence are correct, and so is the code.

**Suggested fix (comment only).** Replace the second sentence with the local fact. For example: "Nulling first would zero their Futures slots and null their Completions and Deliveries slots, so FaultNode would find nothing to free and no awaiter to fault at those indices: the futures would leak and their awaiters would stay pending." Keep the rest of the note. Add no count, no list and no uniqueness quantifier (ffi §A6 round 5).

**Not part of this finding:**
- `design/history/M11/P3.2-producer-send-ordering-parity/PLAN.md:688-693` is the dated record the claim came from.
- `SendCompletionPump.cs:1088` (a duplicate in RunLoop's catch) is a different claim, and it is true.

**Resolution:** fixed in `97478072` (`fixup!` of `aab3b7fd`), comment only; the finding's suggested fix. `SendAccumulator.cs:1488-1491` (`CompleteNode`'s ⚠ hand-over note above `_pump.Enqueue(...)`): the second sentence, "Nulling first would make those indices look like "the core never saw this record", and FaultNode would then fire delivery callbacks the pump is also about to fire — DUPLICATES, which the exactly-once obligation makes strictly worse than the drop (root CLAUDE.md §11.5).", now reads "Nulling first would zero their Futures slots and null their Completions and Deliveries slots, so FaultNode would find nothing to free and no awaiter to fault at those indices: the futures would leak and their awaiters would stay pending." The first sentence ("If Enqueue throws (out of memory), ownership never transferred, so FaultNode must still see a live future at each of these indices and free it."), the `⚠ OWNERSHIP TRANSFERS HERE…` header line and the "Unchanged in substance from the pre-grouping form…" paragraph are unchanged. The "root CLAUDE.md §11.5" cite is dropped: it cited the exactly-once duplicate hazard, which the corrected sentence no longer states. No new rule citation was added, and no count, throw-source list or uniqueness quantifier (ffi §A6 round 5). Twin grep (`Nulling first | strictly worse than the drop | look like "the core never saw` over `dotnet/src` + `dotnet/tests`; `OWNERSHIP TRANSFERS | DUPLICATE` over `dotnet/src`; the first two also over `dotnet/.claude/rules`, `dotnet/CLAUDE.md` and `dotnet/design/current`): this note only, plus one unrelated `DUPLICATE_RESOURCE` admin xmldoc hit. Gates at `97478072`: build 0W/0E; `dotnet format --verify-no-changes` clean; unit 3010/TFM (net8.0, net10.0); soak 165/TFM; no `Failed:` > 0, no `Test Run Aborted` (R12 did not fail, so no rerun). Mode A: `git diff --stat d6ce0512..HEAD -- . ':!dotnet'` empty; `internal static extern` 219 + 357; 0 non-comment changed lines (3 lines replaced, all `//`).

## C93-9 [severity: low] The two perf entry points still describe the sync engine as "serial"; S6 made it pipelined

**Commit:** text from `0204437a` (pre-phase). S6 `4f643f4f` made it false. Fix as a `fixup!` of `4f643f4f`.
**Where:**
- `dotnet/tests/Performance/PerfV2/ProducerMain.cs:26-27` (class summary): "run the sync (serial) / or async (pipelined) engine over the ckd backend".
- `dotnet/tests/Performance/PerfV3/ProducerMain.cs:25` (class summary): "run the sync (serial) or async (pipelined) engine over the v3 backend".

**Problem.** After S6, `RunSync` pipelines: it queues each `PerfSendHandle`, and a recorder thread waits the handles in order (`ProducerBenchmark.cs:115-187`). The class remarks S6 wrote say "Both paths pipeline, as Python's do" (`:56`). S6 also rewrote `ProducerBenchmarkConfig.Async`'s summary, which used to say "async (pipelined) … sync (serial-blocking)" and now says "async path (Python `async_main`) … sync path (Python `main`)". The two entry-point summaries still contrast "sync (serial)" with "async (pipelined)", which is the D5 claim S6 was meant to remove. The S6 row named three D5 sites (`ProducerBenchmark.cs:51-58`, `Backends.cs:71-84`, `V2ProducerBackends.cs:153`). These two are twins of the same claim that the row did not name. The code is correct.

**Suggested fix (comment only).** Drop the parentheticals, or follow the `ProducerBenchmarkConfig.Async` wording: "run the sync (Python `main`) or async (Python `async_main`) engine".

**Twin grep:**
- `serial[- ]blocking | \bD5\b | no future to pipeline | blocks on each send | serial measurement` over `dotnet/tests/Performance`, `dotnet/CLAUDE.md`, `dotnet/.claude/rules` and both Makefiles: no hits.
- `\(serial\)` over `dotnet` (without `design/`), `Makefile` and `.semaphore`: exactly these two.

**Resolution:** fixed in `7ab8c01a` (`fixup!` of `4f643f4f`), comment only; the finding's second suggested fix, the `ProducerBenchmarkConfig.Async` wording. `PerfV2/ProducerMain.cs:26-28` (class summary): "run the sync (serial) / or async (pipelined) engine over the ckd backend" now reads "run the sync / (Python <c>main</c>) or async (Python <c>async_main</c>) engine over the ckd backend", reflowed through `:28` with no other word changed. `PerfV3/ProducerMain.cs:25-27` (class summary): "run the sync (serial) or async (pipelined) engine over the v3 backend" now reads "run the sync (Python <c>main</c>) or async (Python <c>async_main</c>) engine over the v3 backend", reflowed through `:27` the same way. Twin grep (`\(serial\)` over `dotnet` without `design/`, bin/obj and the COMMENTS files, plus `Makefile` and `.semaphore`): 0 hits in code or docs; the one remaining hit is the Critic's local agent memory, which quotes this finding. Gates at `7ab8c01a`: `dotnet build -c Release` of `tests/Performance/PerfV2` and of `tests/Performance/PerfV3`, each 0 Warning(s) 0 Error(s); `dotnet format --verify-no-changes` clean on both projects; the diff is 6 lines replaced (12 changed lines), all `///`, 0 non-comment.
