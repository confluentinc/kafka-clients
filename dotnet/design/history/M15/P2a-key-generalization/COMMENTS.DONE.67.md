# COMMENTS.DONE.67 — resolved Critic-67 findings on M15/P2a

**Round:** Critic 67, review of `6aa5fc4a..68615e50` (`90ac7ba6` · `7caaba2d` · `68615e50`).
**Verdict as received:** 0 High · 0 Medium · **3 Low** · 2 Observations — nothing blocking.
**Resolution:** all three Low findings closed, plus Observation A closed by maintainer ruling.
Fixup commit: see `git log --grep "fixup! dotnet(admin): M15/P2a"`.

---

## Finding 1 [Low] — `TopicCollection`'s factories threw with `ParamName == "collection"` — **FIXED**

**Where:** `src/Confluent.Kafka/TopicCollection.cs`.

**Accepted in full.** `OfTopicNames` / `OfTopicIds` performed no explicit null check, so the
`ArgumentNullException` fell out of `new List<T>(topics)` and reported `List<T>`'s **own**
constructor parameter — `collection` — which is an implementation detail of the copy and is
not a parameter this API has. Their xmldoc documents `topics`, so the shipped behaviour
contradicted the documented contract, and a caller writing
`catch (ArgumentNullException e) when (e.ParamName == "topics")` would not have matched.
The two sibling public value types this phase ships (`TopicPartitionInfo`,
`TopicDescription`) already guard with `nameof`, so this was the one new public entry point
that did not.

**Fix.** An explicit `if (topics is null) throw new ArgumentNullException(nameof(topics));`
in **each nested constructor**, before the `List<T>` construction — and the nested
constructors' parameters were **renamed** `topicIds`/`topicNames` → `topics` so the name
reported is the one the public factory documents on *every* path, including the `internal`
one, rather than only through the factories. `using System;` added to match the sibling
types' style (bare `ArgumentNullException`, as `TopicPartitionInfo.cs:114-122` does).
The rationale is recorded on the constructor so a later reader does not "simplify" the
guard away as redundant with `List<T>`'s.

**Test strengthened, and proven sensitive by re-injection.**
`Factories_RejectANullCollection` asserted only the exception *type*, which is exactly why
the defect survived; it is now
`Factories_RejectANullCollection_NamingTheirOwnParameter` and asserts `ParamName` for both
factories. **Injection:** remove both explicit guards (leaving everything else, including
the `using`, intact) → `dotnet test -f net10.0` = **Failed: 1, Passed: 930**, the one red
being the strengthened test, reporting `Expected: "topics" / Actual: "collection"` — i.e.
it reproduces precisely the value the Critic measured against the built DLL. Guards
restored; 931/931 green again.

---

## Finding 2 [Low, design] — the seam's VALUE axis is still `IntPtr`-shaped — **RECORDED AT THE SEAM; DEFERRED TO P2b BY RULING**

**Where:** `src/Confluent.Kafka/Internal/Interop/KeyedResultMarshal.cs`.

**The finding is correct and is not disputed.** G1 generalized the **key** axis only.
`Accessors.GetValue` is an `IndexedAccessor` (returns `IntPtr`) and `Complete`'s
`marshalValue` is `Func<IntPtr, TValue>`, so both can only name an accessor returning a
*pointer* to a borrowed child. `kafka_admin_DeleteRecordsResult_t`'s per-key value is the
**inline scalar** `int64_t …_get_low_watermark(result, i)`, which neither can point at —
and `getValue: null` is **not** an escape, because the walker reads a null `GetValue` as
shape 2 and calls `CompleteWithSuccessNoValue`, which would discard the watermark
*silently* rather than fail. That silent-discard trap is the sharp edge, and it is now
written down.

**Not fixed here, by the Manager's explicit ruling**, and the reasoning is on the record:
unlike the key change — which forced a new generic parameter, a new
`VoidKeyedAdminOperation<TKey>` and an edit at every call site — the value fix is purely
**additive** (a new `Complete` overload taking a `Func<IntPtr, int, TValue>?` reader
symmetric with the key reader) and touches **no existing call site**. So it does not have
the re-open-a-reviewed-foundation property that justified doing the key axis inside P2a,
and building it now would be speculative work for an RPC this phase does not bind. The
Manager has recorded it in P2b's plan (`design/history/M15/P2b-list-partitions-records/PLAN.md`
§3.1) **with the silent-discard trap called out**.

**What was done here** is the Actor's assigned part: the limitation is stated at the seam
site so it cannot be rediscovered as a surprise — a `<para>` in the `KeyedResultMarshal`
class remarks stating that the value axis is pointer-shaped, that a null `GetValue` means
shape 2 (so leaving it null is not how an inline scalar is described), and that the fix is
an additive overload; plus a short cross-referencing `<remarks>` on `Accessors.GetValue`
itself, which is where someone binding a new RPC will actually look. Both are worded
without uniqueness quantifiers or residual counts, per `ffi-marshalling.md` §A6's round-5
amendment.

---

## Finding 3 [Low] — `Submit` allocated its pin list outside the `try` — **FIXED**

**Where:** `src/Confluent.Kafka/Internal/NativeAdminClient.cs`.

**Accepted.** The sibling submit in the same file states the invariant explicitly and
honours it (`IntPtr[] handles = Array.Empty<IntPtr>();` outside, the real allocation
inside): *"Everything between the `GCHandle` allocation above and the try is a window in
which a throw would root the operation for the process lifetime, because neither the catch
nor the finally covers it — so the window is kept to nothing at all."* `Submit`, which now
carries all four of P2a's new ABI entry points, allocated its
`List<Utf8Marshal.PinnedUtf8String>` in exactly that window. Agreed that this is a
robustness/self-consistency defect rather than a practical hazard (`OutOfMemoryException`
is the only realistic trigger) — but the file's own comment claims a property the sibling
path did not hold, and a claim that is false of half the file is worse than no claim.

**Fix.** The local is declared `List<Utf8Marshal.PinnedUtf8String>? pinned = null;`
(allocating nothing) and the `new List<…>(keys.Count)` moved to the first statement inside
the `try`; the `finally` now null-checks, precisely because the allocation is itself inside
the `try`. The comment at the site names the CreateTopics invariant it is mirroring, so the
two paths are legible as one rule rather than two coincidences. Chosen over hoisting the
allocation *above* `GCHandle.Alloc` (the Critic's equally-correct alternative) so the shape
matches the sibling literally — a reviewer comparing the two submits sees the same
allocate-inside-the-try structure rather than two different remedies for one rule.

---

## Observation A — `STATUS.md:24` still spelled the untracked roadmap's path — **FIXED (maintainer ruling)**

The Critic flagged this as maintainer's-call: the dangling *citation* was already gone, and
the remaining occurrence was self-labelling narrative inside the sentence explaining the
drop. **Ruled: reword so the literal path string does not appear at all** — a fresh clone
should have no broken reference, and a reader grepping for the path should get no hit in a
tracked file even from a sentence that is only *describing* it.

Line 24 now says it without spelling it ("cited the milestone roadmap **by path** — a
deliberately untracked working document under `design/current/`"), and states that the path
string is deliberately not spelled in the file, so the property is self-documenting rather
than accidental.

**⚠ Escalated, NOT fixed — a second tracked occurrence exists, outside this phase.**
`git grep -n "PLAN-M15-admin-client"` now returns exactly one hit repo-wide:
`bindings/dotnet/design/history/M15/P1-admin-foundation/PLAN.md:5`, a
`**Parent roadmap:** …` field. That is a **live citation**, not narrative — so by the
ruling's own reasoning it is the stronger case of the two — but it lives in the Manager's
**archived, immutable** P1 plan under `design/history/`, which is out of P2a's scope and
not the Actor's to edit. Flagged for the Manager rather than silently changed.

---

## Non-findings acknowledged (no action)

**Observation B** — `KeyStringsStayPinned_AcrossACollectionInsideTheNativeCall` earns its
place and its one-sided framing is correct. Kept, unchanged.

**False-fail watchlist** — none triggered in this round either (the three named tests
passed in every run above, including the injection run).
