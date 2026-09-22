# COMMENTS.DONE.77 — M15/P7 (SCRAM credentials, delegation tokens, features)

Round 1 of the fix cycle. All five findings from `COMMENTS.77.md` are closed.
Gate after the fixes: `cargo build --features ffi` clean · `dotnet build -c Release
--no-incremental` **0 Warning(s) / 0 Error(s)** across all 6 TFM outputs · `dotnet test`
**2077/2077** on net10.0 **and** net8.0, no `Test Run Aborted` · `dotnet format
--verify-no-changes` clean. Mode A holds: `git diff 88ead79b..HEAD -- src/ cbindgen.toml
generator/` is empty.

---

## 77.1 — High — `UpdateFeatures({})` reports success where Java throws — **FIXED**

The managed precondition was added, before any pin or marshal (ffi §A5), mirroring
`KafkaAdminClient.java:4590-4592`:

```csharp
if (featureUpdates.Count == 0)
{
    throw new ArgumentException(
        "Feature updates can not be null or empty.", nameof(featureUpdates));
}
```

Java's message verbatim; `ArgumentException` per CLAUDE.md §3's idiom map. The bridge is
**unchanged** — the finding's own prescription, and the right one: the zero-key swallow is a
property of `VoidKeyedAdminOperation` shared with every shape-2 RPC, and `updateFeatures` is
simply the one where Java makes the empty request illegal, so the guard belongs at the call
site rather than in the completion machinery.

Covered by `PublicAdminP7Tests.UpdateFeatures_RejectsAnEmptyMap` — asserts the exact message
and that the rejection is **synchronous** (`Assert.Throws`, not a faulted `Task`).

Also recorded on the public surface: `IAdmin.UpdateFeatures`' `<exception>` block now names
both rejections with their Java cites, and `NativeAdminClient.UpdateFeatures`' remarks state
*why* the guard is load-bearing rather than defensive (zero keys ⇒ `WhenAll(<empty>)` ⇒
success).

## 77.2 — Medium — blank feature name — **FIXED**

A blank guard now sits beside the existing null-key guard, inside the same loop and at the
same point Java checks (`KafkaAdminClient.java:4597-4599`), throwing `ArgumentException` with
Java's message verbatim — so `""` moves from *faulted `Task` carrying `KafkaException`* to a
synchronous precondition, and `"   "` is rejected at all.

⚠ **`string.IsNullOrWhiteSpace` is deliberately NOT used.** Java's `Utils.isBlank` is
`str == null || str.trim().isEmpty()`, and `String.trim` strips every char `<= ' '`,
so Java treats a **control character** as blank where `char.IsWhiteSpace` does not. A private
`IsBlank` mirrors Java's rule and cites `Utils.java:1569-1571`.

**Measured, not assumed:** swapping `IsBlank` for `string.IsNullOrWhiteSpace` against a
`0 Error(s)` build turns exactly the `U+0001` case **RED** (1 failed / 3 passed) and is
invisible to the other three — reverted after. That is why the theory carries a control
character alongside `""`, `" "` and `"   \t "`.

`UpdateFeatures_AcceptsANameWithInteriorWhitespace` is the control: the guard is on
blankness, not on the presence of whitespace.

The header's over-statement (`h:9309-9314` claims the ABI rejects a blank name) is
`kafka-critic`'s half and is untouched here.

## 77.3 — Medium — the two P7 key readers had no wiring guard — **FIXED**

**Re-measured rather than restated** (two independent probes, each against a
`0 Error(s)` build, each reverted):

| # | Probe | Result |
|---|---|---|
| 1 | `throw new KafkaException(...)` in `UpdateFeaturesKey`'s body | **GREEN** — but *vacuous*: `CompleteKeyedVoid`'s `catch` routes it to `FailAll`, and the existing test asserts only the exception **type**. |
| 2 | `throw new InvalidOperationException(...)` in the same body | **GREEN** — the discriminating form. The reader is genuinely **never executed**; the whole-call-error branch is taken. |

Probe 1 is recorded because it is the trap: a `KafkaException` probe on this path cannot
distinguish "safe" from "never reached", and would have read as a false negative.

**Fix, both halves:**

1. **The wiring is now pinned, by construction rather than by a name list.** Both readers are
   built through a new `KeyedResultMarshal.StringKeyReader(IndexedAccessor)` factory — the
   `AclRowMarshal.BindingReader` / `ClientQuotaMarshal.EntityReader` shape — so they
   **capture** their ABI symbol and `AdminP4ReaderWiringTests`' closure scan reaches them.
   Both now appear in `TheTrackedSet_CoversEveryFactoryBuiltReader`'s discovered set and in
   `NoTwoFactoryBuiltReaders_CaptureTheSameAccessors`, plus a positional
   `EachP7StringKeyReader_CapturesItsOwnAccessor` theory carrying the member name and its own
   entry point.

   ⚠ Deliberately **not** a name-filtered guard and **not** a set-only assertion: a name
   filter stops guarding at the first differently-named addition (M15/P3 finding 69.4), and a
   sorted set is invariant under the transposition being guarded against (M15/P6 finding
   76.1). Making the *discovery predicate* reach the shape is what stops the next such reader
   repeating this.

   **Verified RED:** cross-wiring `UpdateFeaturesKey` to
   `kafka_admin_AlterUserScramCredentialsResult_get_user` — the Critic's injection 2 shape,
   measured GREEN before — now gives **2 RED** (the positional row and the no-two-alike
   assertion). Reverted.

2. **`UpdateFeaturesKey` is now executed by a test.**
   `PublicAdminP7Tests.UpdateFeatures_WalksEveryKey_OnTheSuccessPath` drives the mock's one
   success path — deleting an unseeded feature, level `0` with `SafeDowngrade`, which passes
   `validate_feature_update` with `cur = min = max = 0` — so the ABI hands back a populated
   result and the per-key walk runs. Measured RED under probe 2 while the pre-existing
   `UpdateFeatures_SurfacesTheRejectionOnEveryKey` stayed green.

3. **The false xmldoc claim is corrected** on both readers: it now says the reader is built
   through the factory *so that* the guard can read the symbol back, and records that as an
   inline lambda it was invisible and a swap went undetected.

`AdminKeySeamShapeTests.TheWalker_ExposesExactlyTheKnownCallables` went red on the new
factory, as designed, and was extended with a note stating that a key-reader factory is **not**
a new result shape and does not touch **D40** — the five `Complete*` walk callables are
unchanged.

## 77.4 — Low — the D44 note over-stated the divergence — **FIXED**

Independently re-checked against `DescribeUserScramCredentialsResult.java`: `all()`'s
`RESOURCE_NOT_FOUND` exclusion at `:65-67` is only from the **first-failure scan**, after
which `:72-74` builds the map from **every** row — so such a user **is** a key in Java's map,
and `All()` matches Java. The javadoc at `:60-64` states the opposite of the code. Confirmed
`description()` **does** diverge: `:128-130` faults on a `RESOURCE_NOT_FOUND` code, with
Java's own comment *"RESOURCE_NOT_FOUND is included here"*.

The note now says **two** observable divergences — `Users` (`:98-100`) and `Description`
(`:128-130`) — and states explicitly that `All` matches Java's **code**, with the javadoc
contradiction called out so the next reader does not re-derive it from the comment.

The second half is fixed too: the canonical note moved **onto the public
`DescribeUserScramCredentialsResult`**, where a user can read it, and the internal
`UserScramCredentialEntry` now points there instead of the reverse. One statement, one place.

## 77.5 — Low — stale Java citations — **FIXED**

All five corrected, each verified against the 4.3.1 source in this tree:

| Site | Was | Now |
|---|---|---|
| `PublicAdminP7Tests.cs` type remarks | `MockAdminClient.java:1188,1193` | `:1254-1255,1259-1260` |
| `FinalizedVersionRange` theory | `FinalizedVersionRange.java:33-37` | `:39` (check) / `:42-44` (message) |
| `SupportedVersionRange` theory | `SupportedVersionRange.java:33-36` | `:39` (check) / `:42` (message) |
| `FeatureUpdate` throws | `FeatureUpdate.java:55-61` | `:69` and `:74` |
| `UserScramCredentialUpsertion.cs:35` | `ScramFormatter.secureRandomBytes` at `:97` | `:98` |

The `MockAdminClient.java` row cites the method *and* its `throw`, so it no longer contradicts
`MockAdminClient.cs:311`/`:324`, which cite the method lines.

---

## PLAN defect — recorded

The plan's §1.0/§1.1 shape-1 classification of `describeUserScramCredentials`, contradicted by
its own §4.3, is recorded in the plan's new **§11 defect log** with the Java and ABI evidence
and the statement that the shipped shape-3 implementation is the correct one. A record, not a
rewrite — no other part of the plan was touched.
