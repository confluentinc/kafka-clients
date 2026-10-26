# M9/P4 — .NET consumer memory-safety & resource-lifetime hardening

**Status:** **APPROVED 2026-08-27** — all six decisions settled by the maintainer (§D). **Zero open questions.** The Actor/Critic loop starts on the maintainer's explicit go-ahead, which is separate from this approval.
**Agent number:** **N=41** (next unused in the binding's own sequence — see §16).
**Branch:** `prashah_dev_dotnet_binding_consumer` @ `8a633048` (= PR #150 head; consumer-only tree, no producer files).
**Mode:** **A** — C# only (`bindings/dotnet/**`). No Rust core, no `src/ffi/`, no `cbindgen.toml`, no header change. **This phase files no Rust-core / Mode B follow-up items at all** (§D, decision Q3).

---

## D · Decisions taken (settled — do not reopen)

All six were settled on 2026-08-27, after review of the plan author's (Manager's)
recommendations. Recorded here so the Actor and the Critic see the settled state
without re-reading the reasoning, and so a future reader can tell who chose what.

⚠ **Read the "Whose call" column literally — the six were not all decided by the
same party.** The **maintainer** (the human) decided the three blocking questions
**Q1, Q2, Q3**. The remaining three (**Q4, Q5, Q6**) were low-stakes and had already
been argued by the Manager, so the **orchestrating agent** applied the Manager's own
recommendations rather than escalating them, and told the maintainer it had done so
and that they could be redirected. They therefore carry **agent**, not human,
authority: if any of Q4/Q5/Q6 turns out to be wrong or contentious, it should be
re-raised with the maintainer rather than defended as a settled human decision.
Q1/Q2/Q3 are human calls and are not to be reopened by the Actor or the Critic.

| # | Question | Decision | Whose call |
|---|---|---|---|
| **Q1** | M4 — is deferred `Consumer_destroy` the intended `Dispose` semantic, or should teardown force cancellation? | **Accept deferred destroy. Do NOT force cancellation. Stays Mode A. Document it as a known residual.** | Maintainer, **as the Manager recommended** (§4.2) |
| **Q2** | H1 — the 34-convert / 2-exempt / 1-excluded split, the 4-slice staging, and the close-family exemption | **Approved exactly as planned.** `Consumer_close` and `Consumer_close_with_timeout` **deliberately stay on `DangerousGetHandle()`**; the §3.4 justification is accepted and stands verbatim; the Actor **must** comment each exempt site so a later reviewer does not "finish the job." | Maintainer, **as the Manager recommended** (§3.3, §3.4, §3.7) |
| **Q3** | The bare deferred destroy in the teardown race (no preceding graceful close) — document + file a Mode B follow-up, or document only? | **⚠ DIVERGES from the Manager's recommendation.** The Manager recommended "document now **and** file the Mode B item." The maintainer chose **document only, with NO tracked follow-up item** — the residual is **permanently acceptable** under the single-owner not-thread-safe contract (Python parity), **not** a deferred fix awaiting core work. | Maintainer, **overriding** the Manager (§4.3) |
| **Q4** | L10 — `ConsumerRecords.GetEnumerator` boxing | **Deferred.** Per-poll not per-record, and the fix needs a public API-shape decision that does not belong in a memory-safety phase. | **Orchestrating agent**, applying the Manager's recommendation (§11) — not escalated to the maintainer |
| **Q5** | `DisableTestParallelization` | **Stays `false`** (parallel execution enabled). The contradictory comment above it **must** be fixed — an itemized deliverable, not a nicety (§9.1). | **Orchestrating agent**, applying the Manager's recommendation; the `AssemblyInfo.cs:17` comment/value contradiction was **independently verified by the orchestrating agent** (not by the maintainer) |
| **Q6** | The per-record header-key `string` (`ConsumerRecordsMarshal.cs:242-243`) | **Deliberately left alone.** Rationale must be recorded so it does not read as an oversight sitting next to the M6 topic fix (§7.3). | **Orchestrating agent**, applying the Manager's recommendation — not escalated to the maintainer |

### D.1 The governing consequence of Q1 + Q3: no Mode B items, anywhere

Q1 and Q3 together set one rule that overrides any contrary reading elsewhere in
this plan:

> **M9/P4 files, schedules, and tracks NO Rust-core / Mode B follow-up work.**
> Every residual this phase leaves behind is **accepted as-is**, permanently, under
> the single-owner not-thread-safe contract — not parked pending core work.

Where a core-side change would be the theoretical clean fix (notably a
dispatcher-join on `Consumer_destroy`), the plan may **explain** that as context for
*why* the residual exists, but must frame it as **explicitly not pursued and not
tracked**, so nobody reads it as pending work. Two concrete obligations follow:

1. **The documentation must carry the full argument on its own.** With no follow-up
   item to point at, the in-code comment and the residual-list entry are the *only*
   places the "this is accepted, here is why, do not file it" case exists. Making
   that case airtight is an explicit deliverable of the L7 work (§9), not a
   by-product of it. The specific risk being managed: the plan author's own warning
   that **a Critic will otherwise flag the bare deferred destroy as a
   close-before-destroy / invariant-I2 violation and stall the loop.**
2. **Pre-existing "unscheduled candidate hardening" text must be neutralized.**
   `STATUS.md` currently carries several lines that read as pending Mode B work —
   see §9's deliverable list. Half of that text describes work **this phase
   ships** (the per-call AddRef); the other half is the dispatcher-join, which Q3
   now settles as not-pursued. Leaving either as-is would contradict D.1. This was
   an inconsistency surfaced by folding Q3 in; §9 resolves it.

---

## 0 · Glossary (read once; the rest of the plan uses these terms freely)

The findings this phase fixes are interop-lifetime bugs, and the vocabulary is not
obvious from the C# alone. Definitions, in the order they matter:

- **`SafeHandle`** — .NET's wrapper around a native pointer that guarantees the
  "free it exactly once" call happens even if an exception unwinds. Ours is
  `SafeConsumerHandle`; its `ReleaseHandle()` calls the C ABI's
  `kafka_consumer_Consumer_destroy`.
- **`SafeHandle` reference counting** — a `SafeHandle` holds an internal counter.
  `Dispose()` decrements it; the native `ReleaseHandle()` runs **only when the
  counter reaches zero**. So "I disposed it" and "the native resource is freed"
  are *not* the same statement whenever something else holds a count.
- **`DangerousAddRef` / `DangerousRelease`** — the manual increment/decrement of
  that counter. "Dangerous" only means *you* are now responsible for pairing them.
  Holding a count is exactly how you say "native must not free this yet, I am
  still inside a call that uses it."
- **`DangerousGetHandle()`** — extracts the raw pointer *without* taking a count.
  This is the unsafe form: the pointer can be freed by another thread the moment
  after you read it, and you get no error — you get a use-after-free.
- **The `SafeHandle`-as-parameter trick** — if a `[DllImport]` declares its
  parameter as `SafeConsumerHandle` instead of `IntPtr`, the .NET interop
  marshaller does the `DangerousAddRef` before the native call and the
  `DangerousRelease` in a `finally` after it, automatically, for the *whole
  duration of the call*. This is `ffi-marshalling.md §A2`'s stated convention:
  **sync = `SafeHandle`-param (automatic, call-scoped); async = manual `AddRef`
  (span-the-op).** It is the entire fix for finding **H1**.
- **`GCHandle`** — a manual "the garbage collector may not collect this object"
  root. We allocate one per async operation so the completion object survives
  while only *native* code holds a reference to it. It must be freed exactly once
  or the object is rooted forever (a leak).
- **Dispatcher thread** — a plain OS thread created by the Rust core
  (`spawn_dispatcher`, `src/ffi/common.rs:223`) that runs each async operation's
  completion callback. It is *foreign* to .NET: no caller frame, so an exception
  escaping into it is undefined behaviour, and any work done on it runs outside
  the app's own threads.
- **Borrow-root** — an owned native container (e.g. the poll batch,
  `ConsumerRecords_t`) whose destruction invalidates every pointer handed out
  from inside it. Everything must be copied out before the root is destroyed.
- **Use-after-free (UAF)** — reading or calling through memory that has already
  been freed. In this codebase it means a native crash or silent corruption, with
  no managed exception, on whichever thread got unlucky.

---

## 1 · Why this phase exists

PR #150 assembled the .NET consumer binding across milestones M0→M9. Every phase
was reviewed, but each review was **phase-scoped** — it saw one slice's diff. A
holistic review of the *assembled* result was then run against the C ABI header,
`ffi-marshalling.md` Part 0 + Part B, and `bindings/dotnet/CLAUDE.md §6.4`,
deliberately hunting for what phase-scoped review structurally cannot catch:

  1. a later phase invalidating an earlier phase's stated invariant;
  2. lifecycle paths that only exist once every phase's pieces are combined;
  3. documented "accepted residuals" that quietly stopped matching the code.

It found 10 issues (2 high, 4 medium, 4 low). The unifying theme, and the reason
this is one phase rather than ten tickets: **the binding's teardown story was
designed when the consumer surface was async-only, and the surface later grew a
blocking synchronous family (M5/P8a, M5/P8b, M6/P1b) that the teardown story was
never re-derived for.** Fixing that is one coherent piece of work.

A second, sharper reason: the shipped commit `073252f3` ("fix `Consumer_destroy`
use-after-free — span-the-op `SafeHandle` AddRef/Release") **half-landed** the
fix — it protected the 5 async submit helpers and simultaneously re-enabled
parallel test execution on the strength of that protection, while ~34 synchronous
native call sites, including a poll that blocks for a caller-supplied timeout,
were left unprotected. That is the single highest-value item here (**H1**).

---

## 2 · Invariants this phase MUST NOT regress

The holistic review traced ~13 axes end to end and found them **clean**. M9/P4
edits the very files those axes live in, so they are listed here as hard
constraints, not as background. A Critic finding that M9/P4 broke one of these
outranks any efficiency or tidiness gain.

| # | Invariant | Where it lives | Why it is fragile *this* phase |
|---|---|---|---|
| I1 | **The completion callback is the SOLE owner of the per-op `GCHandle` free.** Exactly one free, in a `finally`, via the `Interlocked`-guarded `FreeGcHandle` (`OperationCompletionSource.cs:255-266`). The only non-callback free is `AbandonBeforeSubmit` (`:240-244`), reachable **only** when the submitting P/Invoke threw so native never ran. **No teardown-side free exists.** | `OperationCompletionSource.cs`, the 7 trampolines | `ffi §B7` names "free from anywhere but the callback" as a HIGH-severity pattern: a teardown-side free while native still holds the `GCHandle` is a use-after-free. **M3 edits this exact region.** M3's fix must route through `AbandonBeforeSubmit`, never add a new free site. |
| I2 | **Close-before-destroy teardown ordering.** All six teardown entry points run the graceful `Consumer_close*` first, then release the `SafeHandle` in a `finally`, sharing the one-shot `Interlocked.CompareExchange` latch `TryBeginClose` (`NativeConsumer.cs:3299`). The only bare `Consumer_destroy` is `SafeConsumerHandle.ReleaseHandle`. **⚠ CARVE-OUT (decision Q3):** this invariant does **not** extend to the deferred-destroy race, where an unawaited in-flight op makes the graceful close fail and the later deferred destroy is bare. That case is an **accepted permanent residual** (§4.3) — it is **not** an I2 violation and **must not be filed as a finding**. | `NativeConsumer.cs:2357-2381`, `:2451-2473`, `:1656-1667`, `:1687-1697`, `:2411-2422` | This is the **main design constraint on H1**. §3 keeps the close family off the migration for exactly this reason. The carve-out exists because Q3 settled the deferred-bare-destroy case as accepted rather than tracked; without it, this row would contradict §4.3. |
| I3 | **Length-delimited strings are never NUL-scanned.** Exactly 4 accessors are length-delimited (`ConsumerRecord_topic`, `ConsumerRecord_header_key`, `Node_host`, `Node_rack`); all 4 use `out_len` + `Utf8Marshal.PtrToString(ptr, len)`. The other 17 are NUL-terminated and use the scan form. `Utf8Marshal.cs:99-114` refuses to scan and maps `(Zero,_)`/`len<0` → `null`, `len==0` → `string.Empty`. | `ConsumerRecordsMarshal.cs:136, 242`; `NodeMarshal.cs:71, 78` | **M6 edits the topic decode at `:136-137`.** The memo must preserve the length-delimited form and the `?? string.Empty` normalization exactly. |
| I4 | **Category-3 owned results freed exactly once, in a `finally`, after copy-out** — all 11 containers, on every path including exceptions. Includes the owned `char*` from `Consumer_client_id`. | across `NativeConsumer.cs` + the `*Marshal.cs` family | H1 touches the declarations of the functions that *return* these. A signature change must not disturb the `out IntPtr` pre-initialization (`:1494`, `:1535`, `:1560`, `:1594`) that makes the null-safe `_destroy` a no-op on failure. |
| I5 | **Category-4 borrowed views are never freed** (`ConsumerRecord_t`, `Node_t`, every `_get` element, borrowed strings). | the `*Marshal.cs` family | M6 and L9 both edit marshallers. |
| I6 | **Receive-path copy-out precedes `_destroy`, and the borrowed span cannot escape.** The `ReadOnlySpan<byte>` is created and consumed inside `DeserializeSpan` (`ConsumerRecordsMarshal.cs:216-220`); being a `ref struct` it provably cannot be stored, boxed, or awaited. Copy-out precedes destroy on both paths (async `TypedPollCallbacks.cs:88` before `:105`; sync `NativeConsumer.cs:1127` before `:1136`). No stored native-backed `ReadOnlyMemory<byte>` exists anywhere. | `ConsumerRecordsMarshal.cs`, `TypedPollCallbacks.cs` | M6 adds cross-iteration state to `CopyOut`. It must hold **pointers into the live batch only** and must not outlive `CopyOut` (see §7's design note). |
| I7 | **No-throw boundary at every trampoline.** All 7 wrap everything in `try/catch(Exception)` → `context?.TrySetException`, with frees in `finally`. Nothing can unwind into the dispatcher thread. | the 7 trampolines | M3 and M4 both touch this region. |
| I8 | **Delegate rooting.** Every unmanaged delegate is a `static readonly` field, one rooted instance per closed generic type. | `ConsumerCallbacks.cs`, `TypedPollCallbacks.cs` | H1 retypes 3 *sync* delegate types (§3). The **6 async** delegate types must be left alone. |
| I9 | **`RunContinuationsAsynchronously`** on the single TCS construction (`OperationCompletionSource.cs:88-89`), inherited by every op. | `OperationCompletionSource.cs` | M3 edits this file. |
| I10 | **All pins are call-scoped** (`using`/`try…finally`), none held across a `Task`; `PinBytes:3279-3292` implements the §A4 sentinels (`null` → `(Zero, -1)`; empty → non-null + `0`). | `NativeConsumer.cs` | H1's delegate retype touches the pinning lambdas at `:1596`, `:2777`, `:3262`. |
| I11 | **`KafkaException.FromHandle`** reads code/message/flags **before** the free, `ErrorDestroy` in a `finally`, holds copied values, never the handle. | `KafkaException.cs:50-71` | H1 changes the declarations of ~34 functions that return these error handles. |
| I12 | **Marshalling type map (§0.1).** Explicit `EntryPoint` on every `[DllImport]`, all `Cdecl`, no `UIntPtr`/`nint`/`nuint`, no `LPStr`/`LPWStr`/`LPUTF8Str`, no managed `string` params or returns, `[return: MarshalAs(UnmanagedType.I1)]` on every bool-returning fn, opaque `*_t` always `IntPtr` (no mirrored structs). | `NativeMethods.cs` | H1 edits 34 declarations. **`SafeConsumerHandle` as a parameter is the one sanctioned exception** to "opaque `*_t` → `IntPtr`" and is already precedented in-branch (§3). Keep `[return: MarshalAs(I1)]` on `ConsumerCurrentLag` when its first param changes. |
| I13 | **netstandard2.0 floor.** Classic `[DllImport]` only — no `[LibraryImport]`, no `delegate* unmanaged`, no `[UnmanagedCallersOnly]`, no `LPUTF8Str`, no `Marshal.PtrToStringUTF8`. | `NativeMethods.cs` | `SafeHandle`-as-parameter marshalling is supported by the *classic* marshaller on every TFM including netstandard2.0/net462, so H1 is floor-safe. Confirm on the TFM smoke test regardless. |

---

## 3 · H1 (HIGH) — the synchronous consumer surface can be freed out from under it

### 3.1 The problem in plain terms

A .NET `SafeHandle` exists so that a native pointer is freed exactly once, safely.
But you only get that safety if you *ask* for it: you must either take a
reference count for as long as you are using the pointer, or hand the
`SafeHandle` itself to the P/Invoke and let the marshaller take the count for you.

Today, ~34 of the consumer's synchronous native calls do neither. They call
`_handle.DangerousGetHandle()` — "give me the raw pointer, no protection" — and
pass that to native. The only guard is `ThrowIfClosed()`, which reads a flag a few
instructions earlier. Reading a flag and then using a pointer are two separate
steps; nothing stops another thread from destroying the consumer in between.

**How a user hits it — this is documented usage, not exotic misuse.** The only way
to interrupt a blocked synchronous `Poll` is to call `Wakeup()` from *another*
thread. So the canonical two-thread pattern is:

```csharp
// Thread A (the consumer's owner)
var records = consumer.Poll(TimeSpan.FromSeconds(30));   // blocks up to 30 s

// Thread B (a shutdown handler / Ctrl-C / the gRPC test server)
consumer.Wakeup();
consumer.Dispose();
```

Walk it through. Thread A enters `PollTyped` (`NativeConsumer.cs:1114`), passes
`ThrowIfClosed()`, reads the raw pointer, and parks inside the Rust core's
`block_on` for up to 30 seconds. Thread B calls `Dispose()`, which reaches
`_handle.Dispose()` → `ReleaseHandle` → `Consumer_destroy`. That function
(`src/ffi/consumer.rs:505-523`) does `runtime.shutdown_background()`, then
`drop(consumer)` — it frees the consumer. Thread A is still executing inside
`kafka_consumer_Consumer_poll`, on memory that no longer exists.

**Today vs. what should happen:**

| | Today | After the fix |
|---|---|---|
| `Poll` in flight, another thread disposes | native crash or silent corruption, no exception, ~30 s window | the destroy is deferred until the poll returns; the poll completes normally |
| Any sync call made *after* `Dispose()` | a stale pointer is passed to native (undefined) | `ObjectDisposedException` — which is what `ffi §B2` "Tests required" already mandates |

This is not covered by any accepted residual. `STATUS.md`'s residual #2 is scoped
to `Wakeup()`/`GroupId()`; #3 to "submit-vs-destroy". Neither anticipates a
**blocking sync surface**, which landed later. The exposure is understated by
roughly 14× (see **L7**).

### 3.2 The fix

`ffi-marshalling.md §A2` already states the convention: **sync = `SafeHandle`-param
(automatic, call-scoped `DangerousAddRef` by the marshaller); async = manual
`AddRef` (span-the-op)**. The async half shipped in `073252f3`. This phase lands
the sync half.

Mechanically, per call site: change the `[DllImport]`'s first parameter from
`IntPtr consumer` to `SafeConsumerHandle consumer`, and pass `_handle` instead of
`_handle.DangerousGetHandle()`. The marshaller then AddRefs before the call and
Releases in a `finally` after — covering the *entire* native call, including a
30-second block.

**The in-branch precedent to cite** (⚠ correcting the input findings, which cited
a producer file that does not exist on this branch — see §10.1):
`NativeMethods.cs:105-119`

```csharp
[DllImport(DllName, EntryPoint = "kafka_consumer_KafkaConsumer_new", CallingConvention = CallingConvention.Cdecl)]
internal static extern SafeConsumerHandle KafkaConsumerNew(SafeConsumerPropertiesHandle props, out IntPtr outError);
```

— a shipped consumer-side declaration taking a `SafeHandle` as a parameter, whose
own doc comment already names the mechanism: *"so the marshaller keeps it alive
across the call (DangerousAddRef/Release — PLAN D6)"*.

### 3.3 Exact migration set (verified against this branch)

**37** `[DllImport]` declarations take the consumer handle as their first parameter
and are reachable from the sync path. **34 convert; 2 deliberately do not; 1 more
is structurally excluded.** (34 + 2 + 1 = 37.)

> **⚠ Corrected 2026-08-27, during execution.** This paragraph originally said
> **36**, which contradicted the plan's own 34/2/1 split in the very next
> sentence. **37 is correct.** For completeness, independently verified on this
> branch: declarations taking the consumer handle first total **55** = 37
> sync-path + 18 `_async`, and the 18 matches what §3.4 says must NOT change.
> If you are using a total as a checksum while assembling the migration set, use
> 37 — an assembly that reconciles to 36 has dropped a declaration.
>
> **⚠ Also corrected: the line numbers in the tables below point at the
> `internal static extern` line, not the `[DllImport]` attribute line** — they sit
> 1–2 lines apart. Verified examples: `914 Consumer_poll` → the `[DllImport]` is at
> **896**; `1033 Consumer_committed` → **1019**; `885 Consumer_current_lag` →
> **883**. Harmless, but it makes a line-based diff look misaligned. **Match on the
> EntryPoint string, not the line number** — every EntryPoint named below was
> independently confirmed to exist. Do not read the offset as evidence you are
> editing the wrong site.
>
> Independently re-confirmed as **correct** (do not re-derive): **31** sync
> `_handle.DangerousGetHandle()` call sites (36 total in `NativeConsumer.cs` minus
> the 5 async submit helpers at `:2498`, `:2547`, `:2629`, `:2680`, `:2730`);
> "**30 distinct methods**" (`EnforceRebalance` legitimately occupies both `:2300`
> and `:2305`); and **34** migrating declarations, verified slice-by-slice as
> H1a 13 + H1b 10 + H1c 10 + H1d 1.

**Slice H1a — direct call sites on blocking ops (13 declarations).** Highest value:
these are the multi-second windows.

| `NativeMethods.cs` | EntryPoint (`kafka_consumer_…`) | Call site |
|---|---|---|
| 854 | `Consumer_seek` | `:477` |
| 869 | `Consumer_seek_with_metadata` | `:537` |
| 885 | `Consumer_current_lag` | `:588` (keep `[return: MarshalAs(I1)]`) |
| 914 | `Consumer_poll` | **`:1114`** — the worst instance |
| 924 | `Consumer_subscribe` | `:1174` |
| 931 | `Consumer_unsubscribe` | `:1194` |
| 979 | `Consumer_position` | `:1305` |
| 987 | `Consumer_commit_sync` | `:1329` |
| 1000 | `Consumer_commit_sync_offsets` | `:1361` |
| 1050 | `Consumer_offsets_for_times` | `:1499` |
| 1101 | `Consumer_partitions_for` | `:1538` |
| 1112 | `Consumer_list_topics` | `:1561` |
| 1253 | `Consumer_commit_async` | `:1052` (an `_async` *name* but a sync ABI fn — Java `commitAsync`) |

**Slice H1b — delegate-mediated sync ops (10 declarations, 3 delegate types).**
This is the only slice with a structural change, and the reason H1 is staged.

- `NativePartitionOpSync` (`NativeConsumer.cs:212-222`) —
  `private delegate IntPtr NativePartitionOpSync(IntPtr consumer, IntPtr[] topics, int[] partitions, int count);`
  → first param becomes `SafeConsumerHandle`. Covers `Consumer_assign` (631),
  `_pause` (940), `_resume` (949), `_seek_to_beginning` (958), `_seek_to_end` (967).
  **6** method-group binding sites: `:1215`, `:1228`, `:1241`, `:1254`, `:1267`,
  and `:1891` (the mock `Assign(IReadOnlyList<(string,int)>)` driver binds
  `ConsumerAssign` a second time). One invocation point to edit: `:2777`.
- `NativeCollectionQuerySync` (`:224-235`) —
  `private delegate IntPtr NativeCollectionQuerySync(IntPtr consumer, IntPtr[] topics, int[] partitions, int count, out IntPtr outHandle);`
  → first param becomes `SafeConsumerHandle`. Covers `Consumer_committed` (1033),
  `_beginning_offsets` (1068), `_end_offsets` (1084). **3** binding sites: `:1410`,
  `:1432`, `:1454`. One invocation point: `:1596`. ⚠ It has an `out IntPtr` param,
  so it is a genuine `delegate` (not expressible as `Func<>`) — the `out` must
  survive the retype.
- The inline `Func<IntPtr, IntPtr, int, long, IntPtr> update` (`:3244`) →
  `Func<SafeConsumerHandle, IntPtr, int, long, IntPtr>`. Covers
  `MockConsumer_update_beginning_offsets` (748), `_update_end_offsets` (761).
  **2** binding sites: `:3221`, `:3233`. One invocation point: `:3262`.

**Slice H1c — instantaneous state reads + direct mock helpers (10 declarations).**

`Consumer_group_metadata` (229, via `:2179`), `Consumer_metrics` (299, `:2121`),
`Consumer_client_id` (412, `:2158`), `Consumer_assignment` (807, `:2206`),
`Consumer_subscription` (816, `:2232`), `Consumer_paused` (825, `:2257`),
`Consumer_enforce_rebalance` (840, `:2300`+`:2305`),
`MockConsumer_add_record` (776, `:1941`), `MockConsumer_set_poll_error` (794, `:1991`),
`MockConsumer_update_partitions` (1700, `:1835`).

⚠ **One test compile break lands in this slice**, and it is the only
consumer-handle P/Invoke outside `NativeConsumer.cs`:
`tests/Confluent.Kafka.UnitTests/Interop/Utf8RoundTripTests.cs:53`

```csharp
IntPtr meta = NativeMethods.ConsumerGroupMetadata(consumer.Handle.DangerousGetHandle());
```

Fix is one token: pass `consumer.Handle`. `NativeConsumer.Handle` is already
`internal SafeConsumerHandle` (`:242-249`, and it calls `ThrowIfClosed()` itself),
and `InternalsVisibleTo` grants the test access.

**Slice H1d — `Wakeup` (1 declaration), the special contract.**
`Consumer_wakeup` (205, call site `:2026`). See §3.5.

### 3.4 Deliberately NOT converted — and this is the answer to the close-family concern

**`Consumer_close` (138) and `Consumer_close_with_timeout` (147) keep `IntPtr`.**

The maintainer flagged `Dispose:2372`, `CloseSync:1657` and `CloseSyncWithTimeout:1688`
as the delicate sites. They are, and the resolution is that they **need no
protection and must not be converted**:

1. All three teardown paths are gated by the one-shot latch
   `TryBeginClose()` (`Interlocked.CompareExchange`, `:3299`). Only the winner
   proceeds; every other caller returns immediately.
2. The winner calls the graceful close and *then* releases the handle in its own
   `finally`, **on the same thread, in program order**. The close therefore
   provably precedes its own destroy.
3. There is no other destroy path in the codebase: `Consumer_destroy` is reached
   only from `SafeConsumerHandle.ReleaseHandle`, which is reached only from
   `_handle.Dispose()`, which only the latch winner calls.

So a concurrent destroy cannot race these three sites — the thing H1 protects
against does not exist here. Converting them would buy nothing and would cost
something real:

- **`Dispose()` must not throw.** If the marshaller threw `ObjectDisposedException`
  from inside the `try`, it would propagate out of `Dispose` (the existing
  `finally` does not swallow), violating the .NET `Dispose` contract.
- Any conversion of the close family is a change to invariant **I2**, the one the
  input findings explicitly name as "the main design constraint" on H1.

Convenient consequence: neither declaration is called from anywhere else
(`ConsumerClose` only at `:1657`; `ConsumerCloseWithTimeout` only at `:1688` and
`:2372` — both teardown), so keeping them `IntPtr` creates no signature conflict
with the migrated set. **The Actor must add a comment at each of the three sites
stating why it is exempt**, so the next reviewer does not "finish the job" and
break I2.

**`Consumer_destroy` (156) is structurally excluded.** Its only caller is
`SafeConsumerHandle.ReleaseHandle()` (`SafeConsumerHandle.cs:45`), passing the
protected `handle` field. It cannot take a `SafeConsumerHandle` — the marshaller
would `DangerousAddRef` a handle already mid-release.

**The 6 async delegate types and the 18 `_async` declarations must not change.**
They already have span-the-op protection via the explicit
`_handle.DangerousAddRef(...)` + `context.SetHandleRef(_handle)` released in
`FreeGcHandle`. A call-scoped marshaller AddRef is the **wrong lifetime** for them
(it would release when submit returns, long before the callback fires).

### 3.5 `Wakeup` needs a catch, not just a conversion

`Wakeup` is documented as *best-effort: a no-op once closing/closed* (`:2003-2008`),
and it is the one method deliberately callable cross-thread. Finding **L8** shows
the gRPC test server calls it unguarded from a different RPC thread
(`ConsumerServiceImpl.cs:409`, `AsyncConsumerServiceImpl.cs:483`) — deliberately
gate-exempt, because gating it would deadlock behind the very poll it must wake.

A bare conversion would make `Wakeup` *throw* `ObjectDisposedException` in the
race window, breaking its documented no-op contract and surfacing an exception
into a gRPC handler that does not expect one. So:

```csharp
internal void Wakeup()
{
    if (Volatile.Read(ref _closed) != 0)
    {
        return;
    }

    try
    {
        // SafeHandle-param: the marshaller AddRefs for the call, so a concurrent
        // teardown cannot free the consumer mid-wakeup (ffi §A2). Closes the
        // documented Wakeup TOCTOU residual.
        NativeMethods.ConsumerWakeup(_handle);
    }
    catch (ObjectDisposedException)
    {
        // Lost the race with a concurrent teardown: the handle closed between the
        // flag read and the AddRef. Wakeup is best-effort and a no-op once closed,
        // so swallow — do NOT surface a new exception from a documented no-op.
    }
}
```

This closes **L8** as a side effect. (An equivalent manual
`DangerousAddRef`/`try`/`finally DangerousRelease` shape is acceptable if the
Actor prefers it; the `SafeHandle`-param form is preferred because it is the §A2
convention and one line.)

### 3.6 Does the observable error contract change? Mostly no — and that is checkable

The maintainer asked whether swapping `IntPtr` → `SafeConsumerHandle` changes
observable behaviour, since the marshaller throws `ObjectDisposedException` on a
closed handle instead of passing a stale pointer. Verified answer:

- **`ObjectDisposedException` is already the type** these paths throw:
  `ThrowIfClosed()` throws `new ObjectDisposedException(nameof(NativeConsumer))`
  (`:3301-3307`). So the *type* is unchanged.
- **`ThrowIfClosed()` runs first at every migrated site.** 32 `ThrowIfClosed()`
  call sites cover the whole sync surface (verified by grep; the `GroupMetadata`
  helper at `:2179` is reached only from `GroupId`/`GroupMetadata`, which guard at
  `:2050`/`:2086`/`:2153`). For a consumer that is *already* closed, the observable
  exception — type **and message** — is therefore unchanged: `ThrowIfClosed` wins.
- The marshaller's `ObjectDisposedException` (message *"Safe handle has been
  closed"*, empty `ObjectName`) is observable **only** in the narrow race where the
  close lands *after* `ThrowIfClosed` — precisely the window that is a UAF today.
  Trading a UAF for an `ObjectDisposedException` is the improvement being bought.
- **Existing message-asserting tests keep passing** (DoD §3 requires asserting
  message content, so this mattered). The Actor must still run the full suite and
  report any test that asserts on `ObjectName`.
- **`ThrowIfConcurrentNull` is unaffected.** It maps the core's concurrent-access
  rejection to `InvalidOperationException("KafkaConsumer is not safe for
  multi-threaded access.")` based on the **return value** being null
  (`:2325-2338`), not on the handle argument. Changing the parameter type cannot
  touch it. The `Metrics`/`ClientId`/`GroupMetadata`/`Assignment`/`Subscription`/
  `Paused` sites keep their exact `InvalidOperationException` contract.

### 3.7 Atomic or staged? — staged into 4 sub-slices, all within this one phase

**Decided (Q2): staged, 4 commits, all landing in M9/P4.**

Staged, because the 34 sites are not homogeneous and a single 34-site commit
would be unreviewable and unbisectable:

- H1b changes **three delegate types** and 11 method-group binding sites — a
  structural edit with a genuine failure mode (the `out` param in
  `NativeCollectionQuerySync`), unlike H1a/H1c which are one-token-per-site.
- H1d changes an **error contract** (adds a catch) and needs its own reasoning.
- H1c carries the **one test compile break**.
- A Critic must be able to check the "does the error contract change?" argument
  (§3.6) site by site; 34 sites in one diff makes that a re-derivation rather than
  a check.

All within one phase, because a half-migrated surface is worse than either end
state: the residual documentation (**L7** #2) cannot be written truthfully, and the
test-parallelization question (§9) cannot be answered, until the whole sync
surface is protected.

**Sequencing note the maintainer flagged, and I agree with:** H1 **widens M4**
(more paths hold a reference, so more paths where `Dispose` releases nothing), so
**M4's decision must be settled before or with H1, not after.** §4 therefore comes
first in the commit order and its documentation is written before H1a lands.

---

## 4 · M4 (MEDIUM) — `Dispose` is no longer a deterministic native release (decided: Q1 + Q3)

### 4.1 The problem in plain terms

`SafeHandle.Dispose()` only performs the native free when the internal reference
count reaches zero. The `073252f3` fix made in-flight async operations hold a
reference for their whole duration. Correct — that is what stops the
use-after-free. But it has a consequence nobody enumerated:

**if an operation is in flight, `Dispose()` returns having destroyed nothing.**

Concretely:

```csharp
_ = consumer.Poll(TimeSpan.FromMinutes(5));   // started, deliberately not awaited
await consumer.DisposeAsync();                 // returns promptly — and frees nothing
```

`DisposeAsync` tries a graceful close; the core's one-op-at-a-time guard rejects it
(the poll holds the slot); `DisposeAsync` swallows that (`:2413-2417`) and calls
`_handle.Dispose()`, taking the count 2 → 1. No release. The native consumer, its
tokio runtime, its `ConsumerNetworkThread`, its dispatcher thread and its sockets
stay alive until the poll finishes — up to five minutes later. Then the poll's
completion callback fires and `FreeGcHandle` drops the last reference, so
`Consumer_destroy` executes **on the Rust core's dispatcher thread**.

Two things need explicit sign-off:

1. **.NET's `Dispose` contract (deterministic release) is not honoured** whenever
   teardown races an outstanding operation. The fix traded residual #1's *one*
   leaked `GCHandle` for retention of the whole native consumer.
2. **`Consumer_destroy` running on the core's own dispatcher thread** is a shape no
   rule contemplates — `ffi §B2` assumes destroy runs on the caller's teardown
   thread, and destroy does blocking work (`drop(consumer)` joins the internal bg
   task).

### 4.2 Decided (Q1): **accept deferred destroy. Do not force cancellation. Document it.**

Reasoning, in the order that decides it:

**(a) "Force cancellation" is not available in Mode A.** Forcing the destroy while
an operation is in flight *is* the pre-`073252f3` behaviour — i.e. it is the
use-after-free. The only safe way to force it is a core-side cancel-then-join
(the "Rust-core dispatcher-join on `Consumer_destroy`" idea that
`STATUS.md:1950` / `:1989` currently records as unscheduled candidate hardening —
text that **§9 must neutralize**, because per decision **Q3/D.1 that idea is not
being pursued and not being tracked**; it is named here only to explain why the
residual exists). That is **Mode B** and out of scope for this phase. Choosing
"force cancellation"
means deferring H1 behind a Rust-core dependency, which I do not recommend.

**(b) The retention is bounded, not unbounded.** ⚠ Correcting the input findings,
which say "unbounded-in-time retention": the window is bounded by the in-flight
operation's own completion, and for the poll family that is a **caller-supplied
timeout**. `Poll(5 minutes)` retaining the consumer for ≤5 minutes is "as long as
the operation the caller started", which is defensible. It is not indefinite.

**(c) It triggers only on the documented-misuse path.** Submit an operation, do not
await it, then dispose. The clean path — await, then dispose — has a reference
count of 1 at `Dispose` and releases deterministically, immediately. That path is
already the documented one.

**(d) Destroy-on-the-dispatcher-thread is safe by construction.** The input
findings marked this "assessed clean but not proven". I traced it and it can be
stated as proven, on three grounds, all citable:

  1. **No self-join.** `Consumer_destroy` explicitly does **not** join the
     dispatcher — `src/ffi/consumer.rs:518-522` drops the `JoinHandle` with the
     comment *"detach the dispatcher (do NOT join — outstanding completion jobs may
     still hold a cloned `completion_tx`, and the dispatcher exits once all clones
     are released)"*. Dropping a `JoinHandle` for the current thread is a no-op
     detach.
  2. **No producer can block.** The completion queue is an **unbounded**
     `std::sync::mpsc::channel` (`src/ffi/common.rs:224`), so nothing the internal
     bg task does while `drop(consumer)` joins it can block on the dispatcher.
     `runtime.shutdown_background()` (`:515`) is non-blocking by definition.
  3. **No use-after-free of the dispatcher's own state.** `completion_rx` is moved
     into the dispatcher closure (`common.rs:224-231`), separate from the
     `FfiConsumerHandle` box being freed. After the box is gone, the dispatcher's
     `while let Ok(job) = completion_rx.recv()` still holds a valid receiver and
     exits cleanly once the running job returns and releases the last sender clone.

**(e) H1 makes the common case *better*, not worse.** Post-H1, the canonical
`Wakeup()` + `Dispose()` pattern resolves like this: the wakeup interrupts the
poll, the poll returns, the marshaller's `DangerousRelease` drops the count to
zero, and `Consumer_destroy` runs **on the app thread that was blocked in the
poll** — a normal managed thread, at a clean boundary, immediately after the
operation it was waiting on. That is the behaviour a user would want, and today it
is a crash.

### 4.3 The bare deferred destroy — ACCEPTED PERMANENTLY (decision Q3)

In the race, `Dispose()` attempts the graceful close, the core's guard rejects it
(the in-flight op holds the slot), `Dispose` swallows the error, and the eventual
deferred destroy is therefore a **bare** destroy with no preceding graceful close.
That reads like a violation of invariant **I2**, and the plan author's explicit
warning was that **a Critic will flag it and stall the loop**.

**Decision Q3: this is accepted permanently, and NO follow-up item is filed.** It
is an accepted-by-design residual of the single-owner not-thread-safe contract
(Python parity), on the same footing as the residuals `ffi §B2`/`§B7` already
enumerate — **not** work parked pending a Rust-core change.

The full argument, which the L7 documentation must carry verbatim (§9), because
with no tracked item there is nowhere else for it to live:

1. **It is not new, and not caused by this phase.** It is the pre-existing
   consequence of the core's one-op-at-a-time guard on the **unawaited-op path** —
   the same mechanism M4's async scenario describes, which predates `073252f3`.
   H1 widens its reach to the sync surface; it does not create it.
2. **It is reachable only on the documented-misuse path.** Submit an operation,
   do not await it, then dispose. On the clean path — await, then dispose — the
   reference count is 1 at `Dispose`, the graceful close succeeds, and
   close-before-destroy holds exactly as **I2** states.
3. **The alternative is strictly worse.** The only way to guarantee a graceful
   close here is to *block* teardown until the in-flight op finishes (turning
   `Dispose` into an unbounded wait on an operation the caller abandoned) or to
   destroy underneath the live op (the use-after-free `073252f3` fixed). Neither
   is acceptable; deferring a bare destroy is the least-bad of the three.
4. **What is actually lost is bounded and small.** A bare `Consumer_destroy` skips
   the graceful bg-task join (`ffi §B2`); it still frees every native resource —
   `runtime.shutdown_background()`, `drop(consumer)` (which joins the *internal* bg
   task), and the dispatcher detach all run (`src/ffi/consumer.rs:512-522`). The
   loss is the *graceful* leave-group/commit-on-close courtesy, on a path where the
   caller already abandoned an operation.
5. **A core-side close-then-destroy on the deferred path would be the theoretical
   clean fix — and it is explicitly NOT being pursued and NOT tracked** (decision
   Q3 / §D.1). It is named here only so a reader understands the shape of what is
   being given up, not as a hint of pending work. Do **not** file it, schedule it,
   or add it to a candidate-hardening list.

**Instruction to the Critic (N=41):** invariant **I2** in §2 carries an explicit
carve-out for this case. A finding that the deferred bare destroy violates
close-before-destroy is **out of scope for this phase** — it is decided, and the
decision is recorded in `NativeConsumer.cs` and `STATUS.md` by commit 14.

### 4.4 Deliverables

- Rewrite residual #1 in `NativeConsumer.cs:85-108` and in `STATUS.md` (see **L7**)
  to describe the *actual* post-`073252f3` behaviour: no strand, no `GCHandle` leak,
  but a non-deterministic native release and a possible dispatcher-thread destroy,
  bounded by the in-flight operation.
- Note the H1 widening in the same text.
- Fix the stale teardown-test comment
  (`PublicConsumerTeardownTests.cs:52-53`) which still says
  *"the accepted single-owner residual (strand + one-time leak)"* — now wrong in
  both directions.

  > **⚠ Corrected 2026-08-27, during execution (Critic 41 finding 2b).** This bullet
  > originally said **two** comments — *"`PublicConsumerTeardownTests.cs:52-53` **and
  > the sync sibling**"*. **There is only one.** Grepping the test project for
  > `single-owner residual` / `strand + one-time leak` hits
  > `PublicConsumerTeardownTests.cs` alone; `PublicSyncConsumerTeardownTests.cs`
  > **never carried the claim** — correctly, because its ops block, so the sync
  > facade has no unawaited-op case for the residual to describe. The deliverable is
  > fully discharged at the one real site; nothing was skipped. Same correction
  > applies to §9's **L7-e**, which repeats the phrasing.
- Record the safe-by-construction argument (§4.2(d)) with its three citations, so a
  future reviewer does not have to re-derive it.
- **Record the §4.3 bare-deferred-destroy argument in full, all five points**, in
  the `NativeConsumer.cs` residual text — including the explicit statement that it
  is **accepted permanently with no follow-up item** and that the core-side clean
  fix is **not pursued and not tracked** (decision Q3 / §D.1). This is the
  load-bearing deliverable: with no tracked item, the in-code comment is the only
  place the argument exists, and it is what stops the Critic stalling on it.
- Add an explicit `Dispose`/`DisposeAsync` doc remark: *deterministic release
  requires that no operation is in flight; await your operations before disposing.*

### 4.5 Testability — honest limits

- **Testable:** after `Dispose()`, a subsequent sync call throws
  `ObjectDisposedException` (this is `ffi §B2`'s already-mandated test and is only
  *reliably* true post-H1). The existing "returns without hang" tests must keep
  passing.
- **Not deterministically testable in-suite:** "the native resource was still
  alive N ms after `Dispose` returned". There is no handle-count or
  native-liveness probe exposed to the binding, and adding one would be a Rust-core
  change — which decision Q1/§D.1 rules out, and which is **not** being filed as a
  follow-up. This leg is **verified by inspection**, and the plan says so rather
  than shipping a test that cannot fail.
- **Worth adding:** a stress/soak loop (create → start an unawaited op → dispose,
  ×N, on several threads) as a crash canary. It cannot prove timing, but it would
  catch a self-join or a UAF regression in the dispatcher-thread destroy path.
  Note that `ManyConsumers_CreateAndDispose_NoLeakOrCrash`
  (`PublicConsumerTeardownTests.cs:149`) awaits `Subscribe` first, so it never
  reaches this path — and it currently has **no assertion and no timeout guard**
  at all. Fixing that (wrap in `TestTimeout.Run`, add an unawaited-op variant) is
  in scope.

---

## 5 · H2 (HIGH shape, currently latent) — the gRPC `Close` evicts the registry before it can fail

### 5.1 The problem in plain terms

The .NET gRPC test-harness server keeps a dictionary of `consumer_id → consumer`.
The `Close` RPC removes the entry from that dictionary **first**, and only then
tries to close the consumer:

`bindings/dotnet/grpc-server/ConsumerServiceImpl.cs:413-443`

```csharp
if (!_consumers.TryRemove(request.ConsumerId, out ConsumerEntry? entry))   // :416  ← eviction
{
    return Task.FromResult(new Proto.StatusResponse());                     // idempotent "success"
}

try                                                                          // :422
{
    lock (entry.Gate)
    {
        if (request.HasTimeoutMs)
        {
            entry.Consumer.Close(TimeSpan.FromMilliseconds(request.TimeoutMs));  // :430  ← can throw
        }
        else
        {
            entry.Consumer.Close();
        }
    }
    return Task.FromResult(new Proto.StatusResponse());
}
catch (Exception ex)                                                         // :440
{
    return Task.FromResult(new Proto.StatusResponse { Error = Translate.ToProto(ex) });
}
```

Two throw sites sit after the eviction and before any native call:
`TimeSpan.FromMilliseconds` itself (for a `timeout_ms` above ~9.22e15), and the
binding's own precondition — `KafkaConsumer.cs:155-159` / `MockConsumer.cs:161-165`
throw `ArgumentOutOfRangeException("Timeout must not be negative.")` *before* any
native call. The proto field is `optional int64 timeout_ms`
(`multilanguage-test-server/proto/consumer_service.proto:129`) — signed and
presence-tracked, so a negative value both survives the wire and sets
`HasTimeoutMs = true`.

Either way the entry is already gone. The `catch` neither restores it nor disposes
the consumer, and `Get` is the only other reader (`:475-476`) — so the consumer
becomes **unreachable**: `Consumer_close` and `Consumer_destroy` never run. Worse,
a retried `Close` hits the `TryRemove` miss and returns **silent success**. Leaked
per occurrence: a whole native consumer — tokio runtime + `ConsumerNetworkThread`
+ a dedicated dispatcher thread. `Wakeup` also becomes a silent no-op for that id
(it resolves through the same `Get`).

**Today vs. what should happen:** today an invalid timeout leaks a native consumer
and reports success. It should return an error and leave the consumer either
closed or still registered — never orphaned.

### 5.2 ⚠ Severity correction: this is latent, not active

The input findings present the leak as live. It is not, on the current harness:
the Rust test client hardcodes `timeout_ms: None` in **both** close variants
(`tests/common/multilanguage_consumer.rs:620`, `:628`), so `HasTimeoutMs` is always
false and the no-timeout `Close()` branch is always taken. The negative-timeout
path is **unreachable from the shipped harness client**. The async servicer is
unaffected regardless — it ignores `timeout_ms` entirely
(`AsyncConsumerServiceImpl.cs:500-505`).

Also verified, so we do not over-fix: a **failing** `Consumer_close` is *not* a
leak — `NativeConsumer.cs:1663-1667` / `:1694-1697` release the handle in a
`finally`. This is purely a precondition-ordering bug.

It stays in this phase at high priority anyway: the fix is ~10 lines, the shape is
a genuine defect, the harness client could plumb a timeout at any time, and **M5's
shutdown sweep depends on the same ordering discipline**.

### 5.3 The fix

Reorder to **resolve → validate → close → evict**, and make the failure path
non-orphaning. Match the shape every other RPC already uses (`Get` + `lock`,
non-destructive):

```csharp
public override Task<Proto.StatusResponse> Close(Proto.ConsumerCloseRequest request, ServerCallContext context)
{
    ConsumerEntry? entry = Get(request.ConsumerId);          // non-destructive, like every other RPC
    if (entry is null)
    {
        // Close stays idempotent — silent success on an unknown id (Python / Java parity).
        return Task.FromResult(new Proto.StatusResponse());
    }

    try
    {
        lock (entry.Gate)
        {
            if (request.HasTimeoutMs)
            {
                entry.Consumer.Close(TimeSpan.FromMilliseconds(request.TimeoutMs));
            }
            else
            {
                entry.Consumer.Close();
            }
        }

        // Evict only after the close actually ran. Close/Dispose are idempotent, so a
        // double Close RPC is safe either way; what must not happen is eviction without
        // a close (the consumer would become unreachable and never destroyed).
        _consumers.TryRemove(request.ConsumerId, out _);
        return Task.FromResult(new Proto.StatusResponse());
    }
    catch (Exception ex)
    {
        // Never orphan: evict and dispose so the native handle is released even when the
        // close failed or its preconditions rejected the request.
        _consumers.TryRemove(request.ConsumerId, out _);
        try
        {
            entry.Consumer.Dispose();
        }
        catch (Exception)
        {
            // Best-effort teardown; the original failure is what the caller needs.
        }

        return Task.FromResult(new Proto.StatusResponse { Error = Translate.ToProto(ex) });
    }
}
```

Design constraints on the fix:

- **Keep idempotence.** Unknown id → bare `StatusResponse`, no error. That is
  Python/Java parity and the harness relies on it.
- **Evict on the failure path too** — but only *after* disposing. Leaving the entry
  in place would make a retried `Close` operate on a possibly-broken consumer;
  disposing then evicting releases the native handle deterministically. (`Close`
  and `Dispose` are documented idempotent — `IConsumer.cs:325-326` — so the
  double-teardown is safe.)
- **Do not gate `Wakeup`.** L8's TOCTOU is fixed by H1d, not by locking `Wakeup`
  (which would deadlock behind the poll it must wake).

Apply the symmetric reorder to `AsyncConsumerServiceImpl.cs:487-518` (with
`await entry.Gate.WaitAsync()` / `Release()` and `await entry.Consumer.DisposeAsync()`).
It cannot hit the negative-timeout path, but the eviction-before-close shape and
the orphaning `catch` are identical, and M5's sweep needs both servicers
consistent.

### 5.4 Test

A servicer-level unit test in the .NET test project (not a harness scenario — the
Rust client cannot reach the path, §5.2):

1. `CreateConsumer` → `Close` with `TimeoutMs = -1` → assert the response carries
   an `Error` (`IllegalState`, message containing *"Timeout must not be negative"*).
2. Then assert the consumer is **not** silently reported closed on a retry in a way
   that hides a leak — i.e. the entry is gone *and* was disposed. Observable proxy:
   a subsequent `Poll` on that id returns `unknown consumer_id` (the entry was
   evicted) and no second native consumer is left registered.
3. A `Close` with a valid timeout still succeeds and evicts.

---

## 6 · M3 (MEDIUM) — the async helpers leak a `GCHandle` if `DangerousAddRef` throws

### 6.1 The problem in plain terms

Each of the 5 async submit helpers allocates a `GCHandle` (a "GC, do not collect
this" root) for the operation's completion object, then takes a `SafeHandle`
reference count, then submits. The `AddRef` sits **outside** the `try` that owns
the cleanup:

`NativeConsumer.cs:2483-2494` (and `:2532-2543`, `:2614-2625`, `:2665-2676`, `:2715-2726`)

```csharp
GCHandle gcHandle = GCHandle.Alloc(context, GCHandleType.Normal);
context.SetGcHandle(gcHandle);
bool handleRefAdded = false;
_handle.DangerousAddRef(ref handleRefAdded);   // ← throws ObjectDisposedException if closed
if (handleRefAdded)
{
    context.SetHandleRef(_handle);
}
try
{
    submit(...);
}
catch
{
    context.AbandonBeforeSubmit();   // frees the GCHandle — but this path never runs
    throw;
}
```

If another thread disposes between `ThrowIfClosed()` (`:2528`) and the `AddRef`
(`:2539`), `DangerousAddRef` throws `ObjectDisposedException`, which propagates
**without** running `AbandonBeforeSubmit()`. The `GCHandle` allocated two lines
earlier is never freed, so the `OperationCompletionSource` — and on the poll path
the two deserializers travelling on it — are rooted for the process lifetime. The
caller sees a perfectly plausible `ObjectDisposedException`, so nothing looks
wrong. It is a silent, permanent leak.

`ffi §B7` classifies this precisely: the case is *"native never ran"*, whose
mandated route is `AbandonBeforeSubmit` — and this path is not routed there.

### 6.2 The fix

Move the `AddRef` inside the existing `try`, or give it its own guarded block.
The `try`-move is preferred: one edit per helper, and it puts every
"native-never-ran" path through the single sanctioned free.

```csharp
GCHandle gcHandle = GCHandle.Alloc(context, GCHandleType.Normal);
context.SetGcHandle(gcHandle);
try
{
    // Inside the try: a DangerousAddRef throw (concurrent teardown between
    // ThrowIfClosed and here) is a "native never ran" path and MUST route through
    // AbandonBeforeSubmit, or the GCHandle allocated above is rooted forever
    // (ffi §B6/§B7).
    bool handleRefAdded = false;
    _handle.DangerousAddRef(ref handleRefAdded);
    if (handleRefAdded)
    {
        context.SetHandleRef(_handle);
    }

    context.RegisterCancellation(cancellationToken, Wakeup);
    submit(_handle.DangerousGetHandle(), ..., GCHandle.ToIntPtr(gcHandle));
}
catch
{
    context.AbandonBeforeSubmit();
    throw;
}
```

**Why this is safe with respect to invariant I1** (and the Actor must state this in
the commit message): `AbandonBeforeSubmit` → `FreeGcHandle`
(`OperationCompletionSource.cs:240-266`) is `Interlocked`-guarded and **also**
does `_handleRef?.DangerousRelease()`. So it correctly releases the reference if
the `AddRef` succeeded and `submit` then threw, and correctly releases nothing if
the `AddRef` itself threw (`SetHandleRef` was never called). No new free site is
introduced; this is a *narrower* reachability of the existing one. Nothing here
weakens "the callback is the sole owner" — `AbandonBeforeSubmit` is reachable only
when native never ran, so the callback cannot fire.

`CloseWithCallbackInternal` (`:2480-2506`) gets the same edit for uniformity even
though it is called only after `TryBeginClose` has won (so its `AddRef` cannot
realistically throw) — a divergent shape across the 5 helpers is itself a defect
a Critic will file.

> **⚠ Corrected 2026-08-27, during execution (Critic 41 finding 2a).** This
> paragraph originally said **5+1**, i.e. six. **Five is correct.** There are
> exactly **5** `DangerousAddRef` sites in `NativeConsumer`, and §6.1's five cited
> ranges **already include** `CloseWithCallbackInternal` — its `:2480-2506` here is
> a superset of `:2483-2494` there — so it is one *of* the five, not a sixth. The
> "+1" double-counted it. Verified on this branch, post-migration:
>
> | # | Method | `AddRef` |
> |---|---|---|
> | 1 | `CloseWithCallbackInternal` | `:2721` |
> | 2 | `SubmitVoidOperation` | `:2787` |
> | 3 | `SubmitTypedPollOperation` | `:2872` |
> | 4 | `SubmitScalarOperation` | `:2925` |
> | 5 | `SubmitOwnedHandleOperation` | `:2977` |
>
> Independently cross-checked against `073252f3`, which introduced them: the same
> five methods, no others.
>
> **The label "the 5 async submit helpers" — used at §6.1, §3.3 and §4 — is loose
> in the same way**, and this note fixes it once rather than editing each site: the
> *count* (5) is right, but only **four** are `Submit*` helpers; the fifth is
> `CloseWithCallbackInternal`. Read every "5 async submit helpers" in this plan as
> **"the 5 async op-submit sites — the 4 `Submit*` helpers + `CloseWithCallbackInternal`"**.
> If you are using this as a checksum while auditing the AddRef set (as §3.3
> instructs), the number is **5**; an audit that reconciles to 6 has counted
> `CloseWithCallbackInternal` twice.

### 6.3 Test

Deterministically racing `ThrowIfClosed` against `DangerousAddRef` from a unit test
is not reliably achievable. Be honest about it:

- **Testable:** `AbandonBeforeSubmit` frees the `GCHandle` and releases the handle
  reference exactly once when the submit throws — already the existing behaviour;
  add/extend a test that asserts idempotence of `FreeGcHandle` (call it twice, no
  double-free, no double-release).
- **Verified by inspection:** the `AddRef`-throws path itself. Reachability is a
  4-instruction window under cross-thread misuse. Record it as inspection-verified
  with the `ffi §B7` citation rather than shipping a flaky racing test.
- A stress loop (submit-then-dispose from two threads, ×N) is acceptable as a
  crash/leak canary but must not be an assertion the suite depends on.

---

## 7 · M6 (MEDIUM) — a managed `string` allocated for the topic on **every** record

### 7.1 The problem in plain terms

`ConsumerRecordsMarshal.cs:136-137`, inside the per-record loop:

```csharp
IntPtr topicPtr = NativeMethods.ConsumerRecordTopic(record, out int topicLen);
string topic = Utf8Marshal.PtrToString(topicPtr, topicLen) ?? string.Empty;
```

Every record gets a freshly decoded, freshly allocated managed `string` for its
topic name — even though a poll batch typically spans one or a handful of distinct
topics, and the records arrive **grouped by partition** so the same topic repeats
contiguously.

This is not a micro-optimisation nit; it is a named rule violation.
`consumer-threading.md §27` lists it verbatim as an anti-pattern
(*"`String::from_utf8(topic_bytes.clone())` per record — clone the `Arc<str>` …
instead"*), and `ffi §B4` "Tests required" mandates *"no allocation attributable to
**topic name**"*. The Rust core already honours it — `ConsumerRecord::topic` is an
`Arc<str>` allocated once per `CompletedFetch` and cloned per record with an atomic
bump. The binding throws that away at the boundary.

Magnitude: ~56 B/record for a 16-character topic → ~5.6 MB/s of gen-0 garbage at
100 k msg/s.

### 7.2 Why every existing test is structurally blind to it

Both allocation-budget tests measure a *marginal* delta that cancels or drowns it:

- `PublicConsumerTypedAllocationBudgetTests.cs:72-78` computes
  `(64 KiB-value poll − 16 B-value poll) / 64` with the **same record count and the
  same topic in both terms** — the 64 topic strings appear identically on both
  sides and **cancel exactly**. The test's own doc comment says so at `:43-45`.
- `PublicSyncConsumerAllocationBudgetTests.cs:71-83` computes
  `(256 records − 16 records) / 240`, which *does* include the topic string — but
  budgets **1024 B/record** against an actual ~270–300 B/record (`byte[16]` key 40 +
  `byte[64]` value 88 + record object ~88–96 + topic string ~56–64 + list slot ~8).
  Removing ~56–64 B moves ~270 → ~215 against a 1024 ceiling. **It cannot fail on
  this, before or after.**

There is **no absolute (non-delta) allocation assertion anywhere** in the .NET test
suite — verified across all four files that mention a GC counter. That is the gap.

### 7.3 ⚠ The fix shape in the input findings does not work — corrected design

The findings propose a one-entry `(lastTopicPtr, lastTopicLen) → string` memo keyed
on **pointer identity**. Pointer identity holds on the real fetch path and fails on
the mock path:

- **Real consumer:** `ConsumerRecord.topic` is `Arc<str>`, allocated once per
  `CompletedFetch` and cloned per record (`src/consumer/internals/completed_fetch.rs:33-35`,
  `:617-618`). `ConsumerRecord_topic` returns `topic.as_ptr()`
  (`src/ffi/consumer.rs:866-876`) → **identical pointer for every record in a
  partition group.** Memo hit rate ~100%. ✅
- **MockConsumer:** `kafka_consumer_MockConsumer_add_record`
  (`src/ffi/consumer.rs:1191`, `:1210`) does
  `CStr::from_ptr(topic).to_string_lossy().to_string()` **per call** → a **fresh
  `Arc<str>` per record**, so the pointers differ record-to-record. Memo hit rate
  **0%**. ❌

**Every allocation-budget test is `MockConsumer`-based.** So a pointer-keyed memo
would deliver nothing in the unit suite and, critically, **could not be proven by
any broker-free test** — shipping an unverifiable fix.

**Corrected design: pointer fast path, then a byte comparison.** All topic pointers
in a batch point into the same live borrow-root, so comparing the bytes is safe and
allocation-free:

```csharp
// Inside CopyOut, local to the invocation (see the thread note below).
IntPtr lastTopicPtr = IntPtr.Zero;
int lastTopicLen = -1;
string? lastTopic = null;

// ... per record, replacing the unconditional decode:
IntPtr topicPtr = NativeMethods.ConsumerRecordTopic(record, out int topicLen);
string topic;
if (lastTopic is not null && topicLen == lastTopicLen &&
    (topicPtr == lastTopicPtr || SameBytes(topicPtr, lastTopicPtr, topicLen)))
{
    // Same topic as the previous record. Records arrive grouped by partition, so this
    // hits for all but the first record of each group. Reference reuse matches the
    // core's Arc<str> sharing and Java's per-partition topic string.
    topic = lastTopic;
}
else
{
    // §B3: length-delimited slice -> owned string, never NUL-scanned. (Zero,_) -> null,
    // normalized to empty so Topic stays non-null.
    topic = Utf8Marshal.PtrToString(topicPtr, topicLen) ?? string.Empty;
    lastTopicPtr = topicPtr;
    lastTopicLen = topicLen;
    lastTopic = topic;
}
```

where `SameBytes` is an allocation-free span compare over the two live slices
(`new ReadOnlySpan<byte>((void*)a, len).SequenceEqual(new ReadOnlySpan<byte>((void*)b, len))`).
The pointer check short-circuits the real path to zero comparison cost; the byte
check makes the mock path hit too, so the fix is provable broker-free.

Design constraints (each maps to an invariant in §2):

- **Locals in `CopyOut`, never `static` or `[ThreadStatic]`** (invariant **I6**).
  `CopyOut` runs on two different threads — the caller's thread for the sync poll
  (`NativeConsumer.cs:1127`) and the core's **foreign dispatcher thread** for the
  async poll (`TypedPollCallbacks.cs:88`). Per-invocation locals are correct and
  race-free; shared state would be a data race and would hold pointers past the
  batch's lifetime.
- The memoized pointers must **not outlive `CopyOut`** — they borrow into the batch
  and are invalid after `ConsumerRecords_destroy`.
- `CopyRecord` is `private static` with no context object and one call site
  (`:110`). Thread the memo in by `ref` (a small `ref struct` holding the three
  fields is the tidiest) — do not promote it to a field.
- Preserve the exact length-delimited decode and the `?? string.Empty`
  normalization (invariant **I3**). `topic` also flows into
  `IDeserializer<T>.Deserialize(topic, span)` (`:143`, `:146`) and into the
  `SerializationException` message (`:202`) — reference reuse is semantically
  identical there and is what the Rust core and Java both already do.
- **Rejected alternative:** making the Rust mock share one `Arc<str>` per
  `(topic, partition)`. It would let a pointer-only memo be tested, but it is a
  Rust-core change (**Mode B**), which decision Q1/§D.1 rules out — and it is a
  test-visibility problem the byte comparison already solves in Mode A. Rejected,
  **not** filed as a follow-up.

### 7.3.1 The header-key `string` is deliberately left alone (decision Q6)

The header-key decode at `ConsumerRecordsMarshal.cs:242-243` has the identical
shape — a fresh managed `string` per header per record. **Decision Q6: leave it
alone.** Recorded here, and required in the Actor's self-review, so it does not
read as an oversight sitting three lines from the M6 topic fix:

- **The memo would not pay.** The topic memo works because records arrive **grouped
  by partition**, so the same topic repeats contiguously and a one-entry memo hits
  ~100%. Header keys have no such locality — they vary per header *within* a single
  record, so a one-entry memo would mostly miss, and anything better (a per-batch
  dictionary) allocates a dictionary plus its entries and can easily cost more than
  it saves.
- **The scope is different.** Topic is exactly one decode per record, on every
  record, always. Header keys are `0..n` decodes on the subset of records that
  carry headers — commonly zero.
- **`ffi §B4` names the topic clause specifically** ("no allocation attributable to
  **topic name**"); it does not make the same demand of header keys, and
  `consumer-threading.md §27` already accepts owned headers for this milestone
  ("`ConsumerRecord` holds owned headers … allocating eagerly is acceptable …
  Revisit only if profiling shows header allocation is hot").
- **Not tracked as a follow-up** (§D.1). If profiling later shows it is hot, that is
  a new phase with its own evidence.

### 7.4 Test — this must be able to fail on the bug

Non-negotiable per the maintainer's instruction: the M6 fix does not ship with a
test that still cannot fail on it.

**Primary — a topic-name-length-varying delta** (new test, `MockConsumer`-based,
same file as the sync budget test):

- Hold the record count fixed; vary **only** the topic-name length between two
  measurements (e.g. 8 characters vs 1008 characters), same key, same value.
- Pre-fix: one `string` per record, so the delta is ≈ `recordCount × 2 × Δchars`
  (managed `string` is UTF-16) ≈ ~2000 B/record for Δ=1000.
- Post-fix: one `string` per distinct topic per batch, so the per-record delta
  collapses to ≈ 0.
- Assert per-record delta ≤ a small bound (start at **32 B/record**; the pre-fix
  value is ~2 KB, so the margin is ~60×, and the test fails loudly today and
  passes after).
- Caveats the Actor must respect: `Assign`/`Seek` the **same** topic that is
  `AddRecord`ed (the Rust mock rejects unassigned partitions,
  `src/consumer/mock_consumer.rs:153-157`); keep the `#if NET8_0_OR_GREATER` gate
  (no net462 equivalent); keep the warm-up loop.
- ⚠ **`GC.GetAllocatedBytesForCurrentThread()` is load-bearing, not defensive.**
  Decision **Q5** keeps `DisableTestParallelization = false`, so the allocation
  tests genuinely run concurrently with the rest of the assembly. A process-wide
  counter (`GC.GetTotalAllocatedBytes`) would pick up other tests' allocations and
  flake. Every existing and **every new** allocation assertion in this phase MUST
  use the per-thread counter, and the measured loop must not `await` (a continuation
  can resume on a different thread and silently split the measurement). State this
  in the test's doc comment so the constraint survives the next edit.

**Secondary — tighten the existing sync budget.** `PerRecordBudgetBytes = 1024`
(`PublicSyncConsumerAllocationBudgetTests.cs:58`) is a ~4× ceiling and should come
down. Rather than the plan guessing the constant: the Actor measures the post-fix
per-record value, sets the ceiling at ~2× that (expected ~448–512 B), and **reports
the measured number in the commit message**. Do not tighten so far that normal
runtime/TFM variation flakes it — the primary test is the real gate.

**Also update** the now-inaccurate doc comment at
`PublicConsumerTypedAllocationBudgetTests.cs:43-45`, which lists "topic string" as
fixed overhead that cancels — true of that test's construction, but it should point
at the new test as the place the topic clause is actually covered.

---

## 8 · M5 (MEDIUM) — the gRPC consumer registry is never emptied except by an explicit `Close`

### 8.1 The problem in plain terms

Both gRPC servicers hold a `ConcurrentDictionary<ulong, ConsumerEntry>`. Verified:
the **only** thing that ever removes an entry is the `Close` RPC's `TryRemove`
(`ConsumerServiceImpl.cs:416`, `AsyncConsumerServiceImpl.cs:490`). There is no
enumeration, no `Clear`, no idle sweep. And neither servicer implements
`IDisposable`/`IAsyncDisposable`:

```
ConsumerServiceImpl.cs:48:      internal sealed class ConsumerServiceImpl : Proto.ConsumerService.ConsumerServiceBase
AsyncConsumerServiceImpl.cs:72: internal sealed class AsyncConsumerServiceImpl : Proto.ConsumerService.ConsumerServiceBase
```

Both are registered as DI singletons (`Program.cs:66-73`), and shutdown is only
`app.WaitForShutdown()` (`:94`) — no `try/finally`, no `app.DisposeAsync()`, no
`ApplicationStopping` hook. So DI has nothing to call: every consumer still in the
map at shutdown is dropped with a **live native handle** — no graceful close, no
leave-group.

**Who triggers it:** any harness scenario that creates a consumer and does not send
`Close` — a failed assertion, a panicking Rust test, a dropped client. The Rust
test client has `close()` (`tests/common/multilanguage_consumer.rs:618`) but
**no `impl Drop`** (verified: the word "drop" does not appear in that 790-line
file). The gRPC backend process is **shared across scenarios**, so leaked consumers
— each a tokio runtime + `ConsumerNetworkThread` + a dispatcher thread —
accumulate for the process lifetime. Combined with H2, a failed `Close` lands here
too.

Rule: `bindings/CLAUDE.md §2.4` — deterministic cleanup for every opaque handle.

### 8.2 The fix

Make both servicers disposable and drain the registry:

- `ConsumerServiceImpl : ..., IDisposable` — `Dispose()` snapshots the registry,
  clears it, and calls `Consumer.Dispose()` on every remaining entry inside a
  per-entry `try/catch` (one failing consumer must not abort the sweep).
- `AsyncConsumerServiceImpl : ..., IAsyncDisposable, IDisposable` — same, with
  `await Consumer.DisposeAsync()`; `Dispose()` as the sync fallback.
- ~~**Also dispose `entry.Gate`** in the async servicer — its `SemaphoreSlim` is
  created per entry (`AsyncConsumerServiceImpl.cs:666`) and never disposed, so it
  leaks on the `Close` path too. Note the ordering: dispose the consumer first,
  then the gate.~~ — **WITHDRAWN during execution; see the correction below.**

> **⚠ Corrected 2026-08-27, during execution (Critic 41 finding 1).** The
> struck-through bullet was **implemented and then reverted**, because its premise
> was wrong: **nothing was leaking.** `SemaphoreSlim` allocates its only
> OS-handle-backed member (the `ManualResetEvent` behind `AvailableWaitHandle`)
> **lazily, on first read of that property**, and it registers no finalizer.
> `AvailableWaitHandle` is read **nowhere** in `src/`, `tests/` or `grpc-server/`
> (0 grep hits), so every gate in this harness is a pure managed object holding no
> OS resource — dropping the `ConsumerEntry` reclaims it by GC, and
> `Gate.Dispose()` freed nothing. `bindings/CLAUDE.md §2.4` (deterministic cleanup
> for every opaque **handle**) is therefore not engaged, and "it leaked on the
> `Close` path too" was not a resource leak.
>
> Worse, the disposal was **pure downside**: H2 replaced the destructive
> `TryRemove` gate-keeper with a non-destructive `Get`, so two concurrent same-id
> `Close` RPCs now both proceed, and disposal on top of that adds two latent
> failure modes. (a) The loser's inner `finally` `Gate.Release()` throws
> `ObjectDisposedException`, which the outer `catch` turns into a
> `StatusResponse { Error }` — **breaking `Close`'s documented idempotence**
> ("silent success on a duplicate `Close`"). (b) `SemaphoreSlim.Dispose(true)`
> nulls `m_asyncHead`/`m_asyncTail` **without completing or faulting** pending
> `WaitAsync()` tasks, so a third concurrent gated RPC's `Task` never completes —
> the gRPC call hangs to the client deadline with no exception and nothing logged.
> Every async RPC uses that gate shape, so any of them can be the victim. Both
> modes need ≥2 (resp. ≥3) concurrent gated RPCs on the *same* `consumer_id`, which
> today's Rust harness client does not drive — so both were latent, never active.
>
> **Resolution: the gate disposal is removed entirely** (both `Close` paths now
> plain `_consumers.TryRemove(id, out _)`, matching the sync servicer; the
> `EvictAndDisposeGate` helper is deleted; the shutdown sweeps no longer touch the
> gate). The rationale is recorded permanently on `ConsumerEntry.Gate` so it is not
> re-added. Note this is a **withdrawal of a planned change, not a scope
> deferral**: the plan asked for it, it was built, and execution proved the premise
> false. **M5's actual value is untouched** — both servicers still drain the
> registry and dispose their consumers, which was the genuine fix.
- **Do not rely on DI alone.** Add an explicit shutdown hook in `Program.cs` so the
  sweep runs regardless of how the host tears down — e.g. resolve the singleton and
  dispose it after `app.WaitForShutdown()` in a `finally`, or register on
  `IHostApplicationLifetime.ApplicationStopping`. Implementing `IDisposable` without
  a path that actually calls it fixes nothing.
- Idle-eviction sweep: **out of scope** (a timer in test infrastructure is a new
  failure mode, and shutdown disposal covers the accumulation).

Rejected: adding `impl Drop` to the Rust test client. `close()` is `async` and needs
the gRPC channel, so a `Drop` impl would need a block-on or spawn hack. The
server-side sweep is the correct lever.

### 8.3 Test

- A .NET unit test: construct the servicer, `CreateConsumer` ×3 without `Close`,
  `Dispose()` the servicer, assert no throw and that a subsequent `Poll` on those
  ids reports `unknown consumer_id` (proving the registry was drained).
- Async servicer: the same over `DisposeAsync`.
- A test that `Dispose()` is safe to call twice, and safe on an empty registry.

---

## 9 · L7 (LOW severity, but NOT optional) — the documented residuals no longer match the code

This is in scope and load-bearing: the residual list is what a reader consults to
know what is *knowingly* unsafe. All three entries are now wrong, in a phase that
is about to change all three.

`NativeConsumer.cs:85-108` and `design/current/STATUS.md` (⚠ **the input findings
cite `STATUS.md:2122-2130`; on this branch the residual list is at `:2039-2050`** —
that file is shorter here. See §10.2):

| Residual | Status vs. current code | Post-M9/P4 text must say |
|---|---|---|
| **#1** teardown-with-unawaited-op → strand + one-time `GCHandle` leak | **Wrong in both directions.** The `Task` no longer strands (the op runs to completion because destroy is deferred) and the `GCHandle` no longer leaks — but `Dispose` stopped being a deterministic native release and destroy can relocate to the dispatcher thread. | M4's resolution (decisions **Q1** + **Q3**): deferred-but-bounded native release, **accepted**; dispatcher-thread destroy safe by construction (3 citations, §4.2(d)); the deferred destroy is **bare** (no graceful close) in the race — **accepted permanently, all five §4.3 points reproduced, no follow-up filed**; H1 widens the reach to the sync surface. Must state explicitly that the core-side clean fix is **not pursued and not tracked**. |
| **#2** `Wakeup()`/`GroupId()` handle TOCTOU | **Understated by ~14×.** It applied to ~31 sync sites, several blocking for a *caller-supplied* timeout. | **Deleted** — closed by H1 (sync sites) + H1d (`Wakeup`). Replace with the new, accurate statement of what remains (the close family's exemption and why it is safe by the latch, §3.4). |
| **#3** submit-vs-`destroy` handle race | **Obsolete** — closed by the `DangerousAddRef` at `:2490`/`:2539`/`:2621`/`:2672`/`:2722` in the same file. The residual text at `:103-107` describes a race the code below it already fixed. | **Deleted.** Only M3's leak-on-`AddRef`-throw survived, and this phase fixes that too. |

Also required — each an **itemized deliverable**, not an optional tidy-up:

- **L7-a — `STATUS.md` has no entry for `073252f3` at all** (grepped for both the
  hash and the subject line). Add one, plus an M9/P4 entry.
- **L7-b — reverse the earlier explicit decision, on the record.**
  `STATUS.md:1631` and `:2002` both state *"NO per-call `SafeHandle` AddRef, NO
  close/destroy-as-SafeHandle-param"*. H1 overturns the first half. The rewrite must
  say **why** — the decision was taken when the consumer surface was async-only, and
  M5/P8a + M5/P8b + M6/P1b later added a blocking sync family the decision was never
  re-derived for — and must note that the **"close/destroy-as-SafeHandle-param" half
  still stands** (§3.4, decision Q2). Otherwise the next reader sees a contradiction
  with no resolution.
- **L7-c — neutralize the "unscheduled candidate hardening" text (required by
  §D.1).** Four `STATUS.md` sites currently read as pending Mode B work:
  `:1086`, `:1148`, `:1950`, and `:1989` (*"Candidate N=9 hardening (unscheduled):
  the per-call `SafeHandle.DangerousAddRef` / `DangerousRelease` … and/or a
  Rust-core dispatcher-join on `Consumer_destroy`"*). Each must be rewritten to say:
  the **per-call AddRef half SHIPPED in M9/P4** (H1, N=41), and the **dispatcher-join
  half is NOT pursued and NOT tracked** — the residuals it would have addressed are
  accepted permanently (§4.3, decision Q3). Leaving either half as "unscheduled
  candidate" would directly contradict §D.1 and would leave a future reader
  believing this phase parked work it did not park.
  ⚠ Note `:1989`'s text also claims the dispatcher-join *"would … let the suite
  re-enable parallelization"* — parallelization is **already** enabled (decision Q5),
  so that clause is doubly stale and must go.
- **L7-d — `Wakeup`'s doc remark** at `NativeConsumer.cs:2010-2017` (which calls the
  TOCTOU accepted and points at *"any future hardening … renumbers to N≥8"*) →
  rewrite to describe the shipped H1d fix. The "renumbers to N≥8" pointer is now
  false in the same way as L7-c.
- **L7-e — the teardown-test comment** (`PublicConsumerTeardownTests.cs:52-53`),
  which still says *"the accepted single-owner residual
  (strand + one-time leak)"* → per §4.4. **⚠ Corrected 2026-08-27, during execution
  (Critic 41 finding 2b): there is exactly ONE such comment, not two.** The
  "and the sync sibling" here and in §4.4 was wrong —
  `PublicSyncConsumerTeardownTests.cs` never carried the claim (its ops block, so
  there is no unawaited-op case). See §4.4's correction note for the grep evidence.
- **L7-f — `AssemblyInfo.cs` contradicts itself** (NEW; not in the input findings;
  independently verified by the orchestrating agent). The ~16-line comment beginning at
  `AssemblyInfo.cs:17` says *"Run the whole assembly's tests SEQUENTIALLY"* and then
  explains at length why parallel execution intermittently crashes the test host —
  sitting **directly above** `DisableTestParallelization = false`, which **enables**
  parallelism. `073252f3` flipped the value and left the comment untouched. Rewrite
  the comment to state the **actual** current position (decision Q5): parallel
  execution is enabled, and it is safe because H1 protects every synchronous native
  call with a marshaller-held `SafeHandle` reference (§3) — which is precisely the
  host-crash mechanism the old comment was describing. Also fix the **stale second
  reference** at `PublicConsumerCommitTests.cs:54`, which asserts in prose that
  `DisableTestParallelization = true` is the assembly-wide setting.
  **Why this is itemized rather than lumped in:** the comment is *actively
  misleading* today — a future reader debugging a test-host flake would read it and
  conclude the suite is serial, which it is not. That is a worse failure mode than a
  merely stale comment.

**The phase ends with an accurate residual list and no text anywhere claiming
parked Mode B work** — that is the acceptance criterion for L7, not "some comments
were touched".

**Sequencing:** most of L7 can only be written after H1 and M4 land. It is
therefore the **last** commit of the phase (§12).

### 9.1 `DisableTestParallelization` stays `false` (decision Q5)

**Settled: leave it at `false` (parallel execution enabled), fix the contradictory
comment (L7-f), and sequence H1 as the first substantive commits of the phase.**

- Parallel execution was blocked on the D8.8 host-crash residual, which is
  *exactly* what H1 fixes. Re-disabling would signal a regression that will not
  exist by the end of this phase, and would then need re-enabling — churn on a
  setting that has already been flipped once without its comment updated.
- The exposure (parallel tests + an unprotected sync surface) **predates this
  phase** and is closed within it, in the first few commits.
- The comment must be rewritten to state the *actual* current reason parallel
  execution is safe: H1's `SafeHandle`-param protection across every sync call,
  plus the per-thread `GC.GetAllocatedBytesForCurrentThread()` choice in the budget
  tests. Note that per-thread measurement is now **load-bearing**, not merely
  defensive — any new allocation test must preserve it.
- **Fallback, if the Actor/Critic loop cannot land all four H1 slices:** flip to
  `true` as the final commit and record it as a stop-gap with the reason. The plan
  states this so the Actor does not have to improvise.

---

## 10 · Corrections to the input findings (verified on this branch)

The maintainer asked what the findings got wrong. Six items, all verified:

1. **The `SafeHandle`-param precedent is not the producer's.** The review cited
   `NativeMethods.ProducerSend(SafeProducerHandle, …)`; **no producer source exists
   on this branch** (`SafeProducerHandle`, `NativeProducer` — zero hits under
   `bindings/dotnet/src/`). The in-branch precedent is
   `NativeMethods.cs:105-119 KafkaConsumerNew(SafeConsumerPropertiesHandle props, out IntPtr outError)`.
   ⚠ **Trap for the Actor:** the stale build artifact
   `src/Confluent.Kafka/obj/Release/netstandard2.0/Confluent.Kafka.xml` is from a
   *different* branch and **does** contain `SafeProducerHandle` / `NativeProducer`
   / `NativeMethods.ProducerFlush(SafeProducerHandle, …)`. Any grep for producer
   precedent will hit it and give a false positive. It is not in-branch source.
2. **The site count is 31, not 28.** The findings say "28 sync call sites" and then
   list 27 plus 4 mock helpers. Verified: **31** raw `_handle.DangerousGetHandle()`
   sync sites (30 distinct methods — `EnforceRebalance` occupies `:2300` and
   `:2305`), behind **37** `[DllImport]` declarations, of which **34** migrate
   (§3.3). The findings' `:1052 CommitAsync` is correctly a *sync* ABI call despite
   the name.
3. **`STATUS.md` line numbers are wrong.** The residual list is at `:2039-2050` on
   this branch, not `:2122-2130` — the file is 2048 lines here. The findings'
   provenance section says only the two highest `NativeMethods.cs` citations shift;
   `STATUS.md` shifts too. All `NativeConsumer.cs`, `*Marshal.cs` and
   `grpc-server/*.cs` citations were spot-checked and **are** valid as-is.
4. **H2 is latent, not active.** The Rust harness client hardcodes
   `timeout_ms: None` in both close variants
   (`tests/common/multilanguage_consumer.rs:620`, `:628`), so `HasTimeoutMs` is
   always false and the negative-timeout throw is unreachable from the shipped
   suite. The fix stays high-priority, but the plan does not claim a live leak, and
   the regression test must be a **servicer-level .NET test**, not a harness
   scenario (§5.2, §5.4).
5. **M4's "unbounded-in-time retention" is an overstatement.** The window is
   bounded by the in-flight operation's completion, which for the poll family is a
   caller-supplied timeout (§4.2(b)). And the dispatcher-thread-destroy leg,
   flagged as "assessed clean but not proven", can be stated as **proven** on three
   citable grounds (§4.2(d)).
6. **M6's proposed fix would not have worked, and could not have been tested.** A
   pointer-identity memo gets a 0% hit rate on the `MockConsumer` path (a fresh
   `Arc<str>` per `add_record`), and every budget test is `MockConsumer`-based
   (§7.3). The memo needs a byte-comparison fallback.

Two smaller additions, found while verifying:

7. **L9 has a second micro-item.** Besides the missing null-element guard,
   `TopicPartitionListMarshal.cs:47` allocates `new TopicPartition[count]`
   unconditionally, where `Array.Empty<T>()` would not allocate for `count == 0` —
   the pattern `ConsumerRecordsMarshal.cs:96` already uses. Fold both into L9.
8. **`ManyConsumers_CreateAndDispose_NoLeakOrCrash` has no assertion and no
   timeout guard** (`PublicConsumerTeardownTests.cs:149-159`) — unlike every other
   test in that class it is not wrapped in `TestTimeout.Run`. Fixing that is folded
   into M4's test work (§4.5).

### 10.9 Corrections to **this plan**, found during execution (2026-08-27)

Distinct from the eight items above, which correct the *input findings*. These are
defects in **this PLAN**, found by independent verification during execution, and
corrected in place at the section each belongs to. Recorded here because a plan
defect found mid-execution is exactly what the phase record is for.

| # | Defect | Correction |
|---|---|---|
| **P1** | §3.3 said "**36** `[DllImport]` declarations", contradicting its own 34-convert + 2-exempt + 1-excluded split in the next sentence. | **37** is correct. Fixed at §3.3 and at §10 item 2. Total taking the consumer handle first = **55** = 37 sync-path + 18 `_async` (the 18 matching §3.4). |
| **P2** | §3.3's declaration line numbers point at the `internal static extern` line, not the `[DllImport]` attribute line — 1–2 lines apart (`Consumer_poll` 914 → 896; `Consumer_committed` 1033 → 1019; `Consumer_current_lag` 885 → 883). | Not renumbered (the tables remain useful as-is); instead §3.3 now instructs matching on the **EntryPoint string**, not the line number, and warns that the offset is not evidence of editing the wrong site. |
| **P3** | §6.2 said *"a divergent shape across the **5+1** helpers"*, i.e. six `DangerousAddRef` sites. There are **five**: §6.1's five cited ranges **already include** `CloseWithCallbackInternal` (`:2480-2506` ⊃ `:2483-2494`), so the "+1" double-counted it. The label *"the 5 async submit helpers"* (§3.3, §4, §6.1) mis-describes the same five — only **four** are `Submit*`. | **5** is correct: `CloseWithCallbackInternal` (`:2721`) + `SubmitVoidOperation` (`:2787`) + `SubmitTypedPollOperation` (`:2872`) + `SubmitScalarOperation` (`:2925`) + `SubmitOwnedHandleOperation` (`:2977`) — cross-checked against `073252f3`, which introduced exactly these. Fixed at §6.2, whose note also restates the label once for the whole plan. The two places that had inherited it are fixed at the source: `STATUS.md` (three sites, incl. accepted-residual #3) and `NativeConsumer.cs`'s residual #3, which now names all five. |
| **P4** | §4.4 and §9 (L7-e) both required fixing *"the **two** teardown-test comments (`PublicConsumerTeardownTests.cs:52-53` **and the sync sibling**)"*. The sync sibling's comment **does not exist**. | There is exactly **one**: grepping the test project for `single-owner residual` / `strand + one-time leak` hits `PublicConsumerTeardownTests.cs` alone. `PublicSyncConsumerTeardownTests.cs` never carried the claim — correctly, since its ops block, so the sync facade has no unawaited-op case. Both citations corrected; the deliverable was already fully discharged at the one real site. |

No defect here changes the phase's scope, the migration set, or any decision — P3
and P4 are arithmetic/citation drift in the record, not in the code. The
counts the plan got **right** were independently re-confirmed and are listed in the
§3.3 note: 31 sync call sites, 30 distinct methods, and the 34-declaration
slice-by-slice split (13 + 10 + 10 + 1).

Also confirmed during execution: `cargo build --features ffi` is green and current
for this branch (`Finished dev profile in 0.16s`, exit 0, `libconfluent_kafka.dylib`
40,371,880 B, `confluent_kafka.h` 112,281 B), so §14's stage 1 needed no re-run.

---

## 11 · The two remaining low-severity items

### L8 — gRPC `Wakeup` is gate-exempt, making the TOCTOU reachable from two RPCs

`ConsumerServiceImpl.cs:404-411` / `AsyncConsumerServiceImpl.cs:478-485` call
`Get(id)?.Consumer.Wakeup()` with **no** `entry.Gate`. Interleaving: RPC-A passes
`Volatile.Read(ref _closed)` at `NativeConsumer.cs:2021`; RPC-B's `Close` wins
`TryBeginClose` and reaches `_handle.Dispose()`; RPC-A resumes into
`ConsumerWakeup(DangerousGetHandle())` at `:2026` on freed memory. This matters
because the *server* is the reachable caller — it is not "cross-thread misuse by a
user".

⚠ **Gating `Wakeup` is NOT the fix** — it would deadlock behind the very poll it
must wake, and the gate-exemption is deliberate and documented
(`ConsumerServiceImpl.cs:43-45`). **Fixed by H1d** (§3.5); no gRPC change. The
other 22 RPCs correctly serialize per id (`lock (entry.Gate)` /
`SemaphoreSlim(1,1)` with balanced `try`/`finally`). **No separate work item** —
verify closed by H1d and record it in the H1d commit message.

### L9 — `TopicPartitionListMarshal` consistency

`TopicPartitionListMarshal.cs:52-56` passes `TopicPartitionListGet(list, i)`
straight into `TopicPartitionTopic(element)` with no `element == IntPtr.Zero`
guard. It is **not reachable** (guarded by `count`), but it is the only marshaller
without the guard — `ConsumerRecordsMarshal.cs:103`, `OffsetMapMarshal.cs:71`,
`LongOffsetMapMarshal.cs:53`, `PartitionInfoListMarshal.cs:73`,
`TopicPartitionInfoMapMarshal.cs:70` all have it. Add the guard (skip, matching the
siblings' comment) and switch `:47` to `Array.Empty<TopicPartition>()` for
`count <= 0` (§10.7). Consistency + one avoided allocation; no behaviour change.

### L10 — `ConsumerRecords.GetEnumerator` boxes: **DEFERRED (decision Q4)**

`ConsumerRecords.cs:34`/`:48` — the field is typed `IReadOnlyList<T>`, so
`_records.GetEnumerator()` boxes `List<T>.Enumerator`, and the public method
returns the interface, so `foreach` cannot bind a struct enumerator. Two
allocations per `foreach`.

**Decision Q4: deferred, with a recorded note.** It is **per-poll, not
per-record**, so it is outside DoD §10, and the fix is not local: it needs a
concrete field type (the two backing stores differ — `Array.Empty<T>()` on the
empty path, `List<T>` on the populated path) **plus a new public struct
`GetEnumerator()` overload**. That is a public-API-shape change in a memory-safety
phase, and it interacts with the deferred keep-alive/zero-copy option in
`CLAUDE.md §6.4`. It deserves its own phase.

Note the deferral is a **scope decision, not a parked Mode B item** — it is purely
managed C#, so §D.1 does not apply to it. No tracked follow-up is created; it is
simply not this phase's work. The Actor records the rationale in the self-review so
a Critic does not file it.

---

## 12 · Sequencing and commit breakdown

Order is driven by three dependencies: **M4's decision gates H1** (H1 widens it);
**H1 gates most of L7** (the residual text describes the post-H1 state); and the
gRPC pair (H2 + M5) is independent of both and can land in parallel.

| # | Commit | Contents | Depends on |
|---|---|---|---|
| 1 | `dotnet(M9/P4): archive the approved PLAN` | this file | approval |
| 2 | `dotnet(M9/P4): document the deferred-destroy Dispose semantic (M4)` | Decisions **Q1 + Q3** recorded in `NativeConsumer.cs` `Dispose`/`DisposeAsync` remarks: the safe-by-construction argument with its 3 citations (§4.2(d)) **and** the full five-point bare-deferred-destroy acceptance (§4.3), including "not pursued, not tracked". **Docs only — no behaviour change.** Lands first so H1 is reviewed against a settled semantic, and so the Critic meets the accepted-residual argument before it meets the code that widens it. | — (Q1/Q3 settled, §D) |
| 3 | `dotnet(M9/P4): route the AddRef-throw path through AbandonBeforeSubmit (M3)` | move `DangerousAddRef` inside the `try` in all 5 helpers + `CloseWithCallbackInternal`; `FreeGcHandle` idempotence test | — |
| 4 | `dotnet(M9/P4): SafeHandle-param for the blocking sync ops (H1a)` | 13 declarations + 13 call sites; exemption comments at the 3 close-family sites | 2 |
| 5 | `dotnet(M9/P4): SafeHandle-param for the delegate-mediated sync ops (H1b)` | 3 delegate type retypes, 11 binding sites, 3 invocation points, 10 declarations | 4 |
| 6 | `dotnet(M9/P4): SafeHandle-param for the sync state reads and mock helpers (H1c)` | 10 declarations + call sites + the `Utf8RoundTripTests.cs:53` fix | 5 |
| 7 | `dotnet(M9/P4): protect Wakeup with a SafeHandle-param and best-effort catch (H1d)` | `Consumer_wakeup` + the `catch (ObjectDisposedException)`; closes L8 | 6 |
| 8 | `dotnet(M9/P4): post-close ObjectDisposedException + teardown regression tests` | the §3 / §4.5 tests: post-close ODE per migrated family, teardown-test comment fixes, `ManyConsumers_*` timeout guard + unawaited-op variant, stress canary | 7 |
| 9 | `dotnet(M9/P4): fix the gRPC Close ordering (H2)` | both servicers: resolve → validate → close → evict, non-orphaning `catch`; the servicer-level test | — (parallel with 3–8) |
| 10 | `dotnet(M9/P4): make both gRPC servicers disposable and drain the registry (M5)` | `IDisposable`/`IAsyncDisposable`, gate disposal, the `Program.cs` shutdown hook, tests | 9 |
| 11 | `dotnet(M9/P4): memoize the per-record topic string (M6)` | the `CopyOut` memo with the byte-comparison fallback | — |
| 12 | `dotnet(M9/P4): topic-allocation budget test + tighten the sync budget (M6 tests)` | the length-varying delta test, the tightened ceiling with the measured number, the corrected doc comment at the typed test | 11 |
| 13 | `dotnet(M9/P4): add the TopicPartitionList null-element guard (L9)` | the guard + `Array.Empty` | — |
| 14 | `dotnet(M9/P4): rewrite the accepted residuals and STATUS to match the code (L7)` | All six itemized deliverables **L7-a … L7-f** (§9): `NativeConsumer.cs:85-108` residual list, `STATUS.md` (`073252f3` entry, M9/P4 entry, the L7-b reversal rationale, the **L7-c neutralization of `:1086`/`:1148`/`:1950`/`:1989`**), the `Wakeup` remark, the teardown-test comments, `AssemblyInfo.cs:17` comment, `PublicConsumerCommitTests.cs:54` | 2, 7, 11 |

Per `agent-roles.md`, the Actor commits per logical step and re-runs the DoD checks
each time. Fix-cycle commits use `fixup!` referencing the commit that introduced
the issue.

---

## 13 · Open questions: **NONE**

Every question this plan raised is settled — see **§D · Decisions taken** for the
six decisions (Q1–Q6), who made each, and the one place the maintainer deliberately
overrode the plan author (Q3). Nothing blocks the Actor.

Where each decision is applied in the body:

| Decision | Applied in |
|---|---|
| **Q1** accept deferred destroy | §4.2 (reasoning), §4.4 (deliverables), §12 commit 2 |
| **Q2** H1 shape + close-family exemption | §3.3 (migration set), §3.4 (exemption + the required per-site comments), §3.7 (staging), §12 commits 4–7 |
| **Q3** bare deferred destroy accepted, no follow-up | §D.1 (the governing rule), §2 invariant **I2** carve-out, §4.3 (the five-point argument), §9 residual #1 + **L7-c**, §15 |
| **Q4** L10 deferred | §11 |
| **Q5** `DisableTestParallelization = false` + comment fix | §9 **L7-f**, §9.1, §7.4 (per-thread counter is load-bearing) |
| **Q6** header-key `string` left alone | §7.3.1 |

**The Critic (N=41) must treat these six as decided.** A finding that re-argues one
— most likely the bare deferred destroy against invariant I2 (§4.3), or the absence
of a tracked Mode B follow-up (§D.1) — is out of scope for this phase. Findings
about whether the plan's decisions were *implemented faithfully* are, of course,
exactly in scope.

---

## 14 · Verification / Definition of Done

Per `bindings/dotnet/CLAUDE.md §7` and `.claude/rules/definition-of-done.md`.

⚠ **Command correction:** there is **no `make test-dotnet` / `make verify-dotnet`
target on this branch.** `bindings/dotnet/Makefile` contains only `grpc-image` and
`grpc-image-async`; the root `Makefile` only delegates those two. (Those targets
exist on the producer/perf branches — do not assume them here.) Use explicit
commands. `dotnet` is **not on `PATH`** — it is at `~/.dotnet/dotnet` or
`/usr/local/share/dotnet/dotnet`.

**Required, per commit:**

1. `cargo build --features ffi` — **first**, always. Produces the native cdylib and
   regenerates `target/include/confluent_kafka.h`. **Do not commit the header** (a
   gitignored build artifact).
2. `dotnet build bindings/dotnet/Confluent.Kafka.sln` — **0 warnings, 0 errors**
   across the TFM matrix (library `netstandard2.0;net8.0;net10.0`; tests
   `net462;net8.0;net10.0`).
3. `dotnet test bindings/dotnet/tests/Confluent.Kafka.UnitTests -f net10.0` — green.
   net10.0 is the execution gate; net8.0 is build-verified (the .NET 8 runtime is
   not installed locally). Run `-f net8.0` too if the runtime is present.
4. `dotnet format bindings/dotnet/Confluent.Kafka.sln --verify-no-changes` — clean.
5. `dotnet build bindings/dotnet/grpc-server/Confluent.Kafka.GrpcServer.csproj` —
   0W/0E (commits 9, 10 touch it).
6. **TFM smoke test** — `PublicConsumerTfmSmokeTests` green on net462 (via
   netstandard2.0), net8.0, net10.0. **Specifically load-bearing for H1:**
   `SafeHandle`-as-parameter marshalling must be confirmed on the netstandard2.0
   floor (invariant **I13**). This is the one gate H1 could plausibly fail.

**Multilanguage gRPC harness (H2, M5 touch `grpc-server/`) — required, Docker-gated:**

Run `docker info` first (per the established convention: check, do not assume it is
unavailable).

- **If Docker is up:** `cargo build --features ffi --release` for a **Linux amd64**
  `libconfluent_kafka.so` (cross-build on this Mac; a stale `.so` predating a new
  ABI symbol has bitten a previous phase), then
  `make build-grpc-images` (or `make -C bindings/dotnet grpc-image grpc-image-async`),
  then
  `cargo test --features integration-tests,multilanguage-tests --test integration -- __grpc_dotnet`
  and confirm **no regression** on either dotnet backend and that other backends
  stay green. Because H2's negative-timeout path is unreachable from the Rust client
  (§5.2), the harness is a **no-regression** gate here, not the proof of the fix —
  the proof is the servicer-level .NET test.
- **If Docker is down:** record **CI-pending** explicitly and note that the C#
  servicer tests are the local gate. Per `CLAUDE.md §7.5` the multilanguage suite is
  opt-in until CI-stable, so a CI-pending record is acceptable — but say so, do not
  omit it.
- Harness compile check regardless (no Docker needed):
  `cargo test --features integration-tests,multilanguage-tests --test integration --no-run`.

**DoD adjustments to state explicitly in the self-review:**

- **DoD §10 (hot-path allocation audit): APPLIES, and M6 is it.** This is the one
  finding on the per-record receive path. State the measured post-fix per-record
  number. H1/H2/M3/M4/M5/L7/L9 are not on a per-record path.
- **DoD §11 (consumer trait surface check): applies.** Confirm H1 changes no public
  surface: no new `#[async_trait]`-equivalent, no `block_on` façade, `IDeserializer<T>`
  still sync + `ReadOnlySpan<byte>`. The `[DllImport]` signature changes are
  `internal` and invisible publicly.
- **DoD §3 (error-message assertions):** assert message content for the new/changed
  error paths — H2's `ArgumentOutOfRangeException` surfacing, the post-close
  `ObjectDisposedException`, and the preserved
  `InvalidOperationException("KafkaConsumer is not safe for multi-threaded access.")`.
- **No Java class is translated** → `marked_classes.txt` unchanged.
- **Mode A hard line:** the diff must be **empty** over `src/**` (Rust),
  `src/ffi/**`, `cbindgen.toml`, and the generated header. The Actor verifies with
  `git diff --stat <base> -- src/ cbindgen.toml` and reports it. Any finding that
  turns out to need a Rust change → **STOP and flag** as a Rust-core dependency
  (`actor-executor` / `kafka-critic` handoff); do not widen scope quietly.

---

## 15 · Out of scope

- **Any Rust core / `src/ffi/` / `cbindgen.toml` / header change — and, per decision
  Q3 / §D.1, no Mode B item is filed, scheduled, or tracked by this phase.** Three
  core-side changes were considered and rejected. Each is **closed, not parked**:
  - a **core-side dispatcher-join on `Consumer_destroy`** (§4.2(a)) — the only way
    to "force cancellation". Rejected by Q1. The residual it would address is
    accepted (§4.3). `STATUS.md`'s existing "unscheduled candidate" text for it is
    **neutralized by L7-c**, not carried forward.
  - a **core-side close-then-destroy on the deferred-destroy path** (§4.3) —
    rejected by Q3; the bare deferred destroy is accepted permanently.
  - **`Arc<str>` sharing in the Rust mock's `add_record`** (§7.3) — unnecessary; the
    byte-comparison memo solves the same problem in Mode A.
  If the Actor finds a fix that genuinely *cannot* be done in Mode A, that is a
  **STOP and flag** to the Manager (§14), not a follow-up item to file.
- L10's struct-enumerator change (§11) — deferred by Q4. A scope decision on purely
  managed C#, not a Mode B item.
- Producer, admin, transactions — none exist on this branch.
- Any new public API. No new `[DllImport]` **entry points** (H1 changes the *type*
  of an existing parameter on 34 existing declarations; it adds no ABI function).
- Proto changes, Rust harness changes, and changes to existing `tests/integration/**`
  scenarios. The one permitted Rust-side edit is **none**; the `Utf8RoundTripTests.cs`
  fix is C#.
- Idle-eviction sweep in the gRPC servicers (§8.2).

---

## 16 · Execution conventions

- Agent number **N=41** — the next unused number in the **.NET binding's own**
  sequence, which is independent of the repo-root Rust sequence. Verified: the
  binding has used 1–40 (39 = M11/P6 producer in-flight cap, 40 = M11/P7 producer
  in-flight-cap throttle, both on producer branches); `bindings/dotnet/COMMENTS.39.md`
  is reset and archived. The root repo's 41–52 are its **own** counter (root
  Milestone-8 Phase-40 and Milestone-11) and do not collide — the two live in
  different directories (`bindings/dotnet/COMMENTS.<N>.md` vs repo-root
  `COMMENTS.<N>.md`). Per-phase archive:
  `bindings/dotnet/design/history/M9/P4/COMMENTS.DONE.41.md`.
- Personas `dotnet-actor` / `dotnet-critic`. Loop: Actor → Critic
  (`bindings/dotnet/COMMENTS.41.md`) → Manager summary → fix cycle → repeat until
  clean. Resolved items move to `COMMENTS.DONE.41.md`.
- **Per-path `git add` only.** Never stage: `.claude/agents/dotnet-*.md` (the
  repo-root discovery copies), `COMMENTS.*`, `agent-memory/**`, `obj/`, `bin/`,
  built binaries, `target/**`, the regenerated `confluent_kafka.h`, `.DS_Store`.
- Commits `--no-gpg-sign`, with the `Co-Authored-By:` trailer. **Commit locally;
  do NOT push** — the maintainer manages pushes.
- The Critic reviews C# against the **C ABI header** and the **Kafka Java public
  API shape**, never Rust internals (`CLAUDE.md §8.2`). For this phase the
  `ffi-marshalling.md` anti-pattern blocks in §A2 / §B2 / §B4 / §B6 / §B7 and the
  invariant table in §2 above are the operative checklist.
