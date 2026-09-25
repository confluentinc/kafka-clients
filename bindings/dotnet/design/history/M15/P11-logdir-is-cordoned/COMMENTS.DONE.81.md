# COMMENTS.81 — Critic 81 · M15/P11 (`LogDirDescription.IsCordoned`)

Scope: `3e051bb0` (CP1) + `7c4fd002` (CP2). Reference: C ABI header `:4286`
(`bool kafka_admin_LogDirDescription_is_cordoned(const kafka_admin_LogDirDescription_t *)`)
and Java `LogDirDescription.java` `:36/:46/:94/:105`.

Verification run: `dotnet test -f net8.0 --filter "…LogDirs|…AdminNativeMethodsMarshalling"`
→ **62 passed, 0 failed**.

---

## Finding 1 — `True`/`False` casing survives one field earlier, inside the very string this phase made Java-faithful

**Severity: Low. Does NOT block.** Pre-existing (not introduced by either commit),
but directly adjacent: it lives inside `LogDirDescription.ToString()`'s own output
and inside the assertion CP2 rewrote.

CP1 correctly renders `isCordoned` lowercase
(`Admin/LogDirDescription.cs:154`, `IsCordoned ? "true" : "false"`). The nested
`ReplicaInfo` rendering it embeds does not:

`src/Confluent.Kafka/Admin/ReplicaInfo.cs:72-78`

```csharp
"ReplicaInfo(size={0}, offsetLag={1}, isFuture={2})",
Size, OffsetLag, IsFuture);          // bool → "True"/"False"
```

Java `ReplicaInfo.java:64-70` is `", isFuture=" + isFuture` → **lowercase**. So the
full `LogDirDescription` rendering is Java-faithful in its last field and not in
its first:

`tests/…/PublicAdminLogDirsTests.cs:307` (rewritten by CP2) asserts
`…{t-0=ReplicaInfo(size=1, offsetLag=2, isFuture=False)}, … isCordoned=true)` — one
string containing both spellings. The xmldoc on `ReplicaInfo.ToString()` claims
"matching Java's `toString()` (`:63`)", which is false for that field.

Same defect, same family, one more site: `Admin/TopicListing.cs` renders
`internal=True/False` (asserted at `PublicAdminP2bShapeParityTests.cs:159`) where
Java `TopicListing.java:67` renders `internal=false`.

Five other admin types already use the `? "true" : "false"` form
(`ClientQuotaFilter`, `ConsumerGroupListing`, `ConsumerGroupDescription`,
`MemberDescription`, and now `LogDirDescription`), so these two are the outliers,
not the convention. Both tests pin the wrong text, so neither will self-correct.

**Suggested disposition:** out of this phase's 2-commit diff; file as a follow-up
rather than reopening M15/P11. Flagging it here because the phase established the
rule and its own test file carries the counter-example 20 lines above.

---

## Verified clean — no finding

Checked against the header / Java and found correct; recorded so a later pass does
not re-derive them.

1. **Lowercase `isCordoned` rendering, asserted independently.** `LogDirDescription.cs:154`
   renders lowercase; `TheRenderings_MatchJavas` asserts the **literal** strings
   `isCordoned=false` and `isCordoned=true` for two separately-constructed
   descriptions. The assertion is literal text, not a re-encoding of the
   implementation's own choice — a regression to `bool.ToString()` turns it red.
2. **`ToString()` field order** matches Java `:105` — `isCordoned` last, after
   `usableBytes`.
3. **Marshalling.** `[return: MarshalAs(UnmanagedType.I1)]` on
   `LogDirDescriptionIsCordoned` is correct for the header's 1-byte C `bool` and is
   consistent with the sibling `LogDirDescriptionReplicaIsFuture`
   (`NativeMethods.Admin.cs:1289-1291`). `CallingConvention.Cdecl` and an explicit
   `EntryPoint` are both set, and both are swept by
   `AdminNativeMethodsMarshallingTests` (a genuine reflection sweep over all
   `kafka_admin_*` imports, carrying its own control-positive floor).
4. **`bool`, not `bool?`.** Java `:94` returns a plain `boolean`; the ABI accessor is
   a plain `bool` with no sentinel, unlike `total_bytes`/`usable_bytes`'s `-1`. The
   property type is asserted in two places.
5. **Tripwire inverted, not deleted, and the positive control survived.**
   `LogDirDescription_HasTheCordonedMember_AndTheSweepThatProvesItFindsTheOthers`
   keeps `TotalBytes` + `Error` asserted via the *same* `GetMembers(Everything)`
   walk. Note the control is now belt-and-braces rather than load-bearing: with the
   assertion flipped from `DoesNotContain` to `Contains`, an empty walk fails on its
   own. It is not a permanently-green sweep.
6. **Constructor stayed `internal`** — `Assert.Empty(GetConstructors())` still pins
   it; only `LogDirMarshal` builds one.
7. **Plan correction 1 (wrong file).** The accessor read is at
   `LogDirMarshal.cs:157-162` (`CopyOutDescription`), the only construction site in
   `src`. It reads the borrowed `description` pointer while the owning root is still
   alive, alongside the other five accessors — correct per ffi §B2 Category 3/4.
   `NativeAdminClient.cs` lost only a stale cross-reference; nothing is half-wired
   (`grep` for `new LogDirDescription(` finds exactly one `src` site).
8. **Plan correction 2 (two extra test files).** `AdminLogDirsMarshalTests.cs:263`
   adds `isCordoned: false` to an existing offline-directory construction and
   changes no assertion; `AdminLogDirsLifetimeTests.cs` drops one sentence of prose
   from a remark. Neither weakens what those tests assert.

---

## Assessment of the split proof (asked for explicitly; the mock's `false`-only
## limitation itself is NOT re-litigated here)

The three legs are:

- **I1 marshalling** — `EveryAdminBoolReturn_IsMarshalledAsI1` (reflection sweep,
  control-positive present).
- **Both values + lowercase rendering** — `TheRenderings_MatchJavas`, constructor-level.
- **The accessor is reached on the real walk** — `DescribeLogDirs_CarriesTheCordonedFlag`.

**Sound, and complete for what is reachable — with one seam nothing exercises, which
is not a live defect.** If the `[DllImport]`'s `EntryPoint` named a *different*
existing `kafka_admin_*` symbol that also returns `false` for the mock's
descriptions, all three legs would stay green: leg 1 checks the attribute, not the
symbol; leg 2 never touches native; leg 3 cannot distinguish a correct `false` from a
wrong-symbol `false`. The suite's `EntryPoint` assertion is only "non-empty".

I verified the symbol by hand against the header — `kafka_admin_LogDirDescription_is_cordoned`
matches `confluent_kafka.h:4286` exactly — so **there is no defect to report**; this
is a coverage note. Closing the seam would require a `true` value, which needs the
core, i.e. the out-of-scope limitation. Recording it so a future reader does not
mistake the three green legs for a wiring proof.

---

**Verdict: PASS.** No blocking finding. One Low-severity, pre-existing,
out-of-diff casing defect (Finding 1) worth a follow-up; the phase's own change is
correct against the header and Java `:36/:46/:94/:105`.
