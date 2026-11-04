> **ALL SIX FINDINGS RESOLVED — M9/P7 fix round (N=60).**
> Closed by `96fcc86c` (fixup! -> `9b314422`, findings 1/2/4/5) and `4fe9b5e5`
> (fixup! -> `c157cc76`, findings 3/6 + the rulebook text finding 4 made stale).
> Autosquash pairing verified in a throwaway worktree: both fixups collapse into
> their originals, leaving exactly 2 commits above the base.
>
> | # | Finding | Resolution |
> |---|---|---|
> | 1 | `submitted` guard — record overstated a latent defect | Comment corrected: trigger is unreachable (`PinnedUtf8String.Dispose` cannot throw), guard kept as defensive hygiene, the four submit helpers named as NOT carrying a defect, and "do not write a test — removing it leaves the suite green" stated. Option (i), as preferred. |
> | 2 | Trace message misattributed non-user failures | `TraceSwallowed` now takes a mandatory site string; `OnCommit` catches the user call separately from everything before it. Two tests pin the attribution both ways. |
> | 3 | `CLAUDE.md:553` cited "§8.2's exception" (a PLAN deliverable number) | Now "the §4 sync-vs-async table's ⚠ exception (third row)". |
> | 4 | Delivered-map destroy untestable — remove the trap | Added `OffsetMapMarshal.CopyOutAndDestroy`; `OnCommit` routed through it with an ownership baton so "destroyed exactly once on every path" is provable. No double free (verified by injection). |
> | 5 | `consumer.py:691-699` citation drift | -> `:686-696`. |
> | 6 | `CommitAsync(callback)` null-rejection divergence unrecorded | Recorded in the §4 commit-callback divergence, beside the `Seek` precedent, incl. the deliberate asymmetry with the offsets overload. |
>
> The three **rule-update suggestions** were deliberately NOT actioned — out of the
> stated scope for this round, and maintainer calls. One of them touches a roadmap
> file this round was told not to modify.
>
> Gates re-run green: `dotnet build` (6 assemblies), `dotnet test -f net10.0`
> **543/543**, `dotnet format --verify-no-changes`, `cargo xtask lint`. Mode-A diff
> against `src/` / `cbindgen.toml` / `tests/` still empty.

---

# COMMENTS.60 — dotnet-critic review of M9/P7 (`IOffsetCommitCallback`)

Commits reviewed: **`9b314422`** (implementation + tests) and **`c157cc76`** (doc-sync), on
`prashah_dev_dotnet_binding_consumer`. Reviewed against the C ABI header
(`target/include/confluent_kafka.h`) and the Java `Consumer`/`OffsetCommitCallback` shape
(`bindings/CLAUDE.md §2.7`, dotnet `CLAUDE.md §8.2`).

**Verdict: P7 is functionally and memory-safety correct. No Major or Blocker findings.**
Six findings, all Minor/Low, and none of them a live memory-safety defect. The free-site
determination — the phase's load-bearing decision — is **correct**, and its reframing into
§B6's third Rule is **correct and complete** for every shipped path.

---

## What I verified as correct (evidence, not assertion)

**The three cites behind the free-site determination — all accurate.**

- `src/ffi/consumer.rs:4001-4009` — verified verbatim: the adapter is built at `:4004`
  (`make_commit_callback`), *before* the fallible `read_offset_map` at `:4005`, and the early
  return at `:4007` drops it. The in-source comment at `:4002-4003` states the intent
  explicitly ("BEFORE anything that can fail, so every early return drops it and fires the
  destroy hook").
- `src/ffi/consumer.rs:3892-3898` — verified: `make_commit_callback`'s rustdoc says
  constructing the adapter "**transfers ownership of `user_data`** to it".
- Header — verified at `confluent_kafka.h:2489-2493`: the hook "fires **even when this
  function returns an error** (the transfer is unconditional)", and at `:2517-2521` for the
  offsets form ("returns the error **without registering the callback** — the callback never
  fires, but `user_data_destroy` still does"). `:524-528` adds the ordering that makes the
  hook safe: it fires "**after** the commit completed and the callback returned".

That ordering is what rules the trampoline out: freeing there would, on the success path, free
a `GCHandle` the core is *still about to hand to the hook*, and on the marshal-failure path
would never run at all. **The determination is right.**

**The reframing is correct AND complete.** I enumerated every `user_data_destroy` in the
header (`/usr/bin/grep -n "user_data_destroy\|destroy_t"`): exactly **three** entry points take
one — `ConsumerRebalanceListener_new` (`:2078`) and the two `commit_async*_with_callback`
(`:2505`, `:2538`). Every other callback-taking entry point — the ~8 one-shot consumer
completions *and* the producer's `Producer_send_async` — takes none. So "does this entry point
take a `user_data_destroy`?" partitions the shipped surface cleanly and misclassifies nothing.
It is also a *better* rule than one-shot-vs-multi-shot, which by construction cannot classify
P7 (one-shot by shape, hook-freed by lifetime).

**The rulebook is now self-consistent, not merely extended.** I re-read §B2/§B5/§B6/§B7 in the
post-`c157cc76` state and applied §5.6's part-(b) check myself. All four category-(b)
violations the Actor reports were **real**, and each is scoped rather than deleted:

1. Part B standing note ("two families", one-shot ⇒ the callback frees) — now three families.
2. §B6 Decision bullet "the callback is the sole owner of the per-op `GCHandle` free" — now
   scoped to the **hookless** ~8, with the *reason* stated (they take no hook and their
   callback fires on every path), so the scoping is derivable rather than asserted.
3. §B6 one-shot Rule "freed **exactly once** by the callback" — same scoping, with an explicit
   forward pointer to the third Rule.
4. §B7's whole-section framing ("callbacks that complete a `Task`") — carve-out at the head,
   plus both "sole owner" sentences scoped inline.

Plus two the roadmap named that I confirm are genuine additions, not restatements: §B2 had no
category for the delivered `OffsetMap_t` (and no record of the `CopyOut` /
`CopyOutAndDestroy` asymmetry), and §B5's "sync failures throw; async failures fault the
`Task`" described *neither* commit channel. **The one-shot rules for the genuinely one-shot
paths are intact** — I checked that nothing in the scoping weakened the hookless family's
"freed exactly once by the callback, including the inline guard-rejection path".

**Leaks / double-frees on the map — one destroy on every path.** `OnCommit`: `FromHandle`
first (frees the error in its own `finally`, before the fallible `CopyOut` — the Actor's claim
is accurate, `KafkaException.cs:136-158`), `CopyOut` (`OffsetMapMarshal.cs:58-91`, verified it
copies borrowed key/value elements and destroys nothing), then `OffsetMapDestroy` in the
trampoline's own `finally`, which also covers a throwing `FromUserData` or a throwing user
callback. `OnCommitDiscard`: `ErrorDestroy` in the `try`, `OffsetMapDestroy` in the `finally`,
`user_data` never dereferenced (it is `IntPtr.Zero` by construction, and
`CommitRegistrationArguments(null)` is what decides that). No path frees the map twice and no
path skips it.

**Registration lifetime.** `Release()` is `Interlocked.Exchange` + `IsAllocated`-guarded, so
the hook and the "native never ran" abandon path cannot double-free. The two production
overloads deliberately do **not** release on a non-null error return, and both put the
`throw failure` *outside* the `try`, so the abandon `catch` cannot fire on that path — correct,
and the thing most likely to be got wrong.

**Mutation results (throwaway `git worktree`, full suite `dotnet test -f net10.0`, tree
restored and verified clean afterwards):**

| Injection | Result |
|---|---|
| Free site moved to the trampoline (hook dropped from `CommitRegistrationArguments`, `Release()` added to `OnCommit`) | **RED 2/541** — `MarshalFailure_CallbackNeverFires_...` **and** `CommitTrampoline_NonNullError_...` |
| Production stops passing the hook (`destroy` → `null`) | **RED 4/541** — exactly the four the Actor reports, incl. `MarshalFailure_...` |
| Double `OffsetMapDestroy` of the delivered map | **Test host aborted** (Rust allocator UB check, `kafka-consumer-callback-dispatcher` thread) |
| Omit the delivered-map destroy | **GREEN 541/541** — the Actor's honest report reproduced |
| Remove the `submitted` guard (unconditional `registration?.Release()` in the `catch`) | **GREEN 541/541** |

**Test 4 (`MarshalFailure_CallbackNeverFires_ButTheRegistrationIsStillReleasedExactlyOnce`)
now goes RED under the mutation that once passed it.** Confirmed directly. The mechanism is
sound and worth keeping: because the test takes its ABI triple from
`NativeConsumer.CommitRegistrationArguments`, *any* production change that stops passing the
hook propagates into the test's own P/Invoke. That is DoD #12 working as intended.

**DoD #12 — can fixture and production still drift?** Only in one narrow way: the tests still
choose *which* P/Invoke to call and pass their own arrays. But the `(callback, user_data,
user_data_destroy)` triple — the only part that decides the free site — has exactly one
producer, and `ProductionCommitAsync_*` covers the call-shape half end-to-end. I could not
construct a drift that leaves a free-site bug undetected.

**Trace precedent (P7-D3).** TFM availability genuinely established, not assumed. I read
`DefineConstants` out of MSBuild for every library TFM × configuration:
`netstandard2.0/net8.0/net10.0` × `Debug/Release` all yield `TRACE;DEBUG` or `TRACE;RELEASE`,
so the `[Conditional("TRACE")]` call is never compiled out (net462 rides the netstandard2.0
asset). The precedent is minimal: one call site, confined to `Internal/Interop/`, no logging
abstraction, `PackageReference` count unchanged at 2, and `System.Diagnostics.Trace` appears in
the public tree only inside XML doc comments — no public API leakage. No `public` type exists
under `Internal/`.

**Public API shape.** `IOffsetCommitCallback.OnComplete(offsets, exception)` is sync `void`,
matching Java's `void onComplete(Map, Exception)`; `offsets` non-nullable, `exception`
`KafkaException?` — Java's "exception == null means success" preserved with precise
nullability. Both overloads land on `IConsumerCommon` and are forwarded verbatim by all four
consumer types. `CommitAsync(null)` binds to the 1-arg overload (a method with no omitted
optional parameters is preferred), so the two-overload set is not ambiguous. No managed
exception can unwind into native from either trampoline or the hook.

---

## Findings

### 1 · Minor — the `submitted`/`consumed` abandon-window guard is now applied inconsistently, and the phase record overstates the defect it closes

`NativeConsumer.CommitAsync(offsets, callback)` (`:1371-1394`) adds a `submitted` flag so the
`catch` releases the registration **only** when native never ran — the stated reason being that
`WithPinnedCommitOffsets` releases its pins in a `finally` *inside* the `try`, so a throw there
would free a `GCHandle` the core still holds.

Two problems with that as recorded.

**(a) The trigger is not reachable in the shipped code.** `WithPinnedCommitOffsets`
(`:3770-3803`) has nothing between `body(...)` and the `finally` that can throw, and the
`finally` is `topicPins[i]?.Dispose()` / `metadataPins[i]?.Dispose()` where
`PinnedUtf8String.Dispose` (`Utf8Marshal.cs:181-187`) is `if (_handle.IsAllocated)
_handle.Free();` — an `IsAllocated`-guarded `GCHandle.Free()` cannot throw. So this is a
belt-and-braces guard against a structural shape, not a "latent defect" that was live.
Injecting its removal leaves **541/541 green**, and — per the "correct but ungradeable"
pattern — no *sequential* test can distinguish it from a broken one, so please do **not**
resolve this by adding a test that cannot bite.

**(b) The identical shape recurs, unguarded, across the shipped async submit helpers.**
`SubmitVoidOperation` (`:3230`, abandon at `:3271-3274`), `SubmitScalarOperation` (`:3381`,
abandon at `:3408-3412`) and `SubmitOwnedHandleOperation` (`:3433`, abandon at `:3460-3465`)
each invoke `submit(...)` inside their `try` and then unconditionally call
`context.AbandonBeforeSubmit()` in the `catch` — and at most of their call sites `submit` is
itself pin-scoped, either via a `WithPinned*` wrapper whose release `finally` is *inside* that
`try` (`:460`, `:508`, `:980`, `:1025`, `:1099`, `:3307`) or via a `using
Utf8Marshal.PinnedUtf8String` declaration inside the lambda, which lowers to the same
try/finally (`:936` position, `:2256` partitions-for). That is exactly the shape `submitted`
exists to close, and by the same argument as (a) it is equally unreachable there.
(`SubmitTypedPollOperation` and the close path pin nothing, so they are not implicated.) The
binding now treats one structure two different ways with no stated criterion.

Not a bug in either place. What is actionable is the inconsistency plus the record: either
(i) state at the `submitted` site (and in the phase record) that the trigger is unreachable
with the current `PinnedUtf8String` and the flag is defensive, matching the four helpers'
existing treatment; or (ii) if the defensive posture is wanted, mirror it in the four helpers
so a reader cannot conclude the commit path is special. Option (i) is cheaper and I'd prefer it
— but please do not leave the phase record describing it as a latent defect that was fixed,
because a future reviewer will then read the four helpers as *carrying* that defect.

*(Caveat for completeness: on net462 a `Thread.Abort` could inject between the P/Invoke
returning and `submitted = true`, so the flag does not fully close even the pathological
window it is written for. `Thread.Abort` is `PlatformNotSupportedException` on .NET Core.)*

### 2 · Minor — the swallowed-exception trace message misattributes non-user failures to the user callback

`ConsumerCallbacks.TraceSwallowedCallbackFailure` (`:896-910`) emits a fixed message:

    "Confluent.Kafka: an IOffsetCommitCallback threw and the exception was swallowed. ..."

But its two callers catch more than the user callback:

- `OnCommit`'s `catch` also covers `KafkaException.FromHandle`, `OffsetMapMarshal.CopyOut`, and
  `CommitCallbackRegistration.FromUserData`. The shipped test
  `CommitTrampoline_WithUnexpectedContext_DoesNotUnwindIntoNative` (`:350-369`) drives exactly
  this: an `InvalidCastException` out of the `GCHandle` cast is logged as *"an
  IOffsetCommitCallback threw"*, which is false and points a debugger at the wrong code.
- `OnCommitDiscard`'s `catch` (`:848-859`) has **no user callback at all** — on that path there
  is no `IOffsetCommitCallback` in existence.

P7-D3's entire justification is that swallowing *silently* "would leave a debugging cliff"; a
confidently-wrong attribution is a worse cliff than a generic one. Suggest either taking a
short site/description string (e.g. `"the offset-commit callback"` /
`"the offset-commit trampoline"` / `"the offset-commit discard trampoline"`) or softening to
"a Confluent.Kafka offset-commit callback boundary swallowed an exception". `ffi-marshalling.md`
§B6's anti-pattern "letting the swallowed exception be *silently* discarded with no diagnostic"
is satisfied either way; this is about the diagnostic being *correct*.

### 3 · Minor (doc) — broken cross-reference in the new `CLAUDE.md` §4 commit-callback divergence

`bindings/dotnet/CLAUDE.md:553`:

> This is a **one-shot** completion, so — unlike the listener — the "takes a completion callback
> → the `Task` replaces the callback" row *would* textually apply; **§8.2's exception** is what
> carves it out, on the grounds that the callback carries offsets a `Task` cannot.

`CLAUDE.md §8.2` is **"Review ground truth (firm)"** and contains no exception. The carve-out
actually lives in the **§4 sync-vs-async table**, third row. The origin is visible in
`design/history/M9/P7/PLAN.md:348`, where "**8.2 — §4 sync-vs-async table**" is a *PLAN
deliverable number*; the deliverable number was carried into the shipped rulebook, where it
reads as a section reference. Suggest: *"the §4 sync-vs-async table's ⚠ exception is what
carves it out"*.

This matters more than a typo because §4 is the decision table an implementing agent consults;
a reader following the pointer lands on the governance section and finds nothing.

### 4 · Low — the delivered-`OffsetMap_t` destroy is untestable; prefer removing the trap structurally over adding a guard

Reproduced the Actor's report exactly: deleting `NativeMethods.OffsetMapDestroy(offsets)` from
`OnCommit`'s `finally` leaves **541/541 green**. The disclosure is honest and both
`ConsumerCallbacks.cs` and `ffi-marshalling.md` §B2 record it, so this is **not** a coverage
gap being hidden.

On "should a guard exist" — my honest read is **no test is worth demanding here**, but the
*trap* is worth removing. A native leak of this size is not observable from managed code; the
only managed guard I can construct is an RSS/`WorkingSet64` growth budget over a churn loop
committing a few thousand partitions with long metadata (~1 MB of native map per fire), which
would work but is noisy, platform-dependent, and unlike the existing
`GC.GetTotalMemory` retention guards has no strong-root lower bound to lean on. I would not
file its absence.

The cheaper and stronger fix is structural: add `OffsetMapMarshal.CopyOutAndDestroy` mirroring
`TopicPartitionListMarshal.CopyOutAndDestroy` (copy, then `_destroy` the root in its own
`finally`) and route `OnCommit` through it. Then "copy the listener trampoline's shape
verbatim" becomes *correct* instead of the leak trap §B2 now has to warn about, the asymmetry
the doc calls "a live leak trap" disappears, and the destroy lives in one place shared with the
path that already gets it right. `CopyOut` stays for the query paths whose caller owns the
root. Optional, and a judgement call for the maintainer — flagged, not demanded.

### 5 · Low (doc) — citation drift on `consumer.py:691-699`

`IOffsetCommitCallback.cs:37-38` cites `consumer.py:691-699` for "Python documents the
identical divergence". The sentence that actually documents it — *"`callback` runs on the Rust
dispatcher thread, not the caller's, and the operation that delivers it ... does not return
until it does"* — is at `bindings/python/consumer.py:686-688`; `:691-699` is the coroutine
caveat plus the `cb = ...` code. Same `.. warning::` block, so a reader lands nearby, but the
range should be `686-696`. (The companion cite `consumer.py:363-372` is fine — the
`except Exception: _log.exception(...)` is at `:371-372`, inside the range.)

### 6 · Low — `CommitAsync(callback)` rejects a `null` callback that Java accepts; record it as a divergence

Java's `commitAsync(OffsetCommitCallback callback)` accepts `null` (it is equivalent to
`commitAsync()`); `NativeConsumer.CommitAsync(IOffsetCommitCallback)` (`:1282-1285`) throws
`ArgumentNullException`. I think the choice is **right** — the parameter is non-nullable under
`#nullable enable`, `CommitAsync()` already expresses "no callback", and §B5 mandates
precondition validation before the FFI call — and the XML doc says "(required; use
`CommitAsync()` for none)". But `CLAUDE.md §4` records every other deliberate Java divergence
explicitly (the `Seek` negative-offset guard is the closest precedent: "the one place .NET is
deliberately stricter than Python"). One sentence in the new §4 commit-callback divergence note
closes it. Note the asymmetry is deliberate and correct in the *other* overload, where
`callback = null` **is** honoured as Java's `commitAsync(Map, null)`.

---

## Gate results (all re-run, none cited from the phase record)

| Gate | Result |
|---|---|
| `cargo build --features ffi` | ✅ |
| `cargo xtask lint` | ✅ exit 0 — "No lint issues found" (both clippy passes) |
| `cargo test --all-features -- --skip __grpc` | ✅ exit 0 |
| `dotnet build` | ✅ 0 warnings / 0 errors — lib `netstandard2.0`+`net8.0`+`net10.0`, tests `net462`+`net8.0`+`net10.0` |
| `dotnet test` | ✅ **541/541** on net8.0 and net10.0; **37/37** under `--filter ~CommitCallback`; net10.0 re-run 3× more, stable |
| `dotnet format --verify-no-changes` | ✅ exit 0 |

Per roadmap §6.4 I did **not** cite `cargo build --all-features` as harness evidence; the two
gates that actually prove it (`xtask lint`, `test --all-features`) are the ones above.

**Environment limit, not a finding:** the `net462` test *run* aborts locally with
"Could not find 'mono' host" — macOS has no Mono. The net462 assembly builds, which is the TFM
smoke obligation this machine can discharge; the net462 *run* is CI-only by design
(`CLAUDE.md §7`).

---

## Rule-update suggestions

Read `COMMENTS.FP.md` (repo root, 54 lines) — it contains no .NET-binding entries, and there is
no `COMMENTS.FN.md`. So the following come from this review, not from a recorded FP/FN.

1. **`ffi-marshalling.md` §B6 — state the deciding question as a property of the *call*, not
   the entry point.** The new third Rule opens: *"does this **entry point** take a
   `user_data_destroy`?"* For the shipped surface this is exact, but the discard path is a
   counter-example in miniature: the entry point takes a hook and the call passes none. It is
   already covered downstream ("allocating a `GCHandle` on the callback-less discard path" is
   listed as an anti-pattern), so nothing is wrong today. Suggest tightening to *"does this
   call pass a `user_data_destroy`? — and if it does not, is there a `GCHandle` at all?"*,
   which makes the fourth shape fall out of the same question instead of needing a separate
   bullet.

2. **`design/current/PLAN-M9-consumer-callback-parity.md` §5.6 — add a part (c) to the
   mechanical check: resolve every cross-reference the doc-sync *introduces*.** Finding 3 is
   a PLAN deliverable number ("8.2") that leaked into the shipped rulebook as a section
   reference, and finding 5 is line-range drift. §5.6's two-part check (does the concept
   appear / does any sentence forbid the shipped code) cannot catch either, because both edits
   pass both halves. A one-line "open each `§x.y` / `file:line` the new text introduces and
   confirm it points where the prose says" would have caught both, and this is the third
   consecutive phase whose only findings are in the documentation category §5.6 exists to
   police.

3. **`bindings/dotnet/CLAUDE.md` §6.2 (Mode-A checklist) — the M9/P6 suggestion is worth
   promoting now that it has paid off twice.** P6 proposed adding *"if the feature introduces
   a new callback/ownership shape, update the matching `ffi-marshalling.md` section in the same
   commit."* P7 did exactly that, voluntarily and well (four real category-(b) violations found
   and scoped), and it is the reason this review has no doc/code contradiction to file. Making
   it a checklist line removes the dependence on the Actor noticing.
