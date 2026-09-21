# COMMENTS.DONE.76 — Critic 76, .NET binding, M15 / P6 (ACLs & client quotas)

Fix cycle over the 3 findings (0 High / 1 Medium / 2 Low) filed against the chain
`4cbc764f..74e6ff1c`. All three are closed below with the measurement that proves each.

**Verification for this cycle** (run on this host, not quoted):

| Check | Result |
|---|---|
| Baseline before any edit, `dotnet test -f net10.0` | `Passed! Failed: 0, Passed: 1966` |
| Baseline before any edit, `dotnet test -f net8.0` | `Passed! Failed: 0, Passed: 1966` |
| After the fixes, `dotnet test -f net10.0` | `Passed! Failed: 0, Passed: 1970` (+4 new guard tests) |
| After the fixes, `dotnet test -f net8.0` | `Passed! Failed: 0, Passed: 1970` |
| `dotnet build Confluent.Kafka.sln -c Release --no-incremental` | **`0 Warning(s), 0 Error(s)`**, all six TFM outputs (ns2.0 / net8.0 / net10.0 library; net462 / net8.0 / net10.0 tests) |
| `dotnet format Confluent.Kafka.sln --verify-no-changes` | clean, exit 0, no output |
| Mode-A proof | `git status --porcelain -- src/ cbindgen.toml generator/` → **empty**; no Rust / `cbindgen.toml` / `generator/` file touched, so the generated header cannot have changed |
| TODO/FIXME over the four edited files | 0, with a control-positive (47 `summary` hits in the same grep target) |

⚠ One **unrelated pre-existing flake** was observed once and is recorded rather than
hidden: `ConsumerRebalanceListenerBridgeTests.DisposeAsync_WithLiveRegistration_ReturnsWithoutHanging`
failed on one of three net10.0 runs (a consumer-teardown timing test, no ACL/quota code in
its path) and was green on the immediately following run and on the final run. Not caused by
this cycle; not in P6's scope.

---

## 76.1 — MEDIUM — CLOSED. The filter bundle's `Operation` / `PermissionType` wiring is now pinned, and the surviving injection goes RED

**Remedy chosen: extend the T-N9 wiring guard to shared accessor *bundles*.** The Critic
offered two ("either suffices"): change one `PublicAdminDeleteAclsTests` fixture to a pair
whose codes differ, or reflect the bundle's members back to their `EntryPoint`s. The fixture
change fixes **this instance**; the guard fixes **the class**, and the class is what the
finding is about — the guard's discovery reads closure fields on `AdminCallbacks`, so *any*
accessor bundle escapes it, not just this one. The guard also covers the quota bundle in the
same stroke, and — see below — it immediately found a **third, untracked** bundle that the
fixture change could not have reached. The fixture was left alone.

**What changed** — `tests/Confluent.Kafka.UnitTests/Interop/AdminP4ReaderWiringTests.cs` only
(no production file touched by this finding):

1. `NativeFilterAccessors_BindEveryMemberToItsOwnAbiSymbol` — the seven members of
   `AclRowMarshal.NativeFilterAccessors`, each pinned to its own ABI symbol **positionally**,
   one row per member, member name included in the compared string. The reader assertions
   elsewhere in the file compare a **sorted set**, which is invariant under exactly the
   transposition being guarded against and would have proved nothing here; this is the one
   reason the rows carry their member name.
2. `NativeEntityAccessors_BindEveryMemberToItsOwnAbiSymbol` — the same for
   `ClientQuotaMarshal.NativeEntityAccessors` (3 members).
3. `OffsetAndMetadataMapAccessors_BindEveryMemberToItsOwnAbiSymbol` — the same for
   `AdminCallbacks.s_offsetAndMetadataMapAccessors` (7 members). **This bundle was not in the
   finding and was not known to be unpinned**; it predates M15/P6 (M15/P5 lineage, `9a63989a`)
   and was surfaced by item 4 below on its first run. Its same-typed pair is
   `GetTopic`/`GetMetadata` — transposing those swaps a dictionary key for a value.
4. `TheTrackedBundleSet_CoversEveryAccessorBundle` — the bundle-level twin of the existing
   `TheTrackedSet_CoversEveryFactoryBuiltReader`. It **discovers** bundles instead of trusting
   a checklist: an object held in an interop static field that carries **two or more
   same-typed** P/Invoke delegates. That criterion is the hazard's own definition (a
   positional constructor parameter list you can transpose without a compile error), which is
   why `KeyedResultMarshal.Accessors` is correctly **not** discovered — its two members have
   distinct delegate types, so a transposition does not compile. It carries a control-positive
   (`Assert.NotEmpty`) so a criterion that silently matched nothing cannot pass vacuously.

`KeyedResultMarshal.cs` was **not** edited (out of scope, and correctly out of the guard's
criterion).

**The re-run the finding asked for — MEASURED, not claimed.** The injection is the exact M4
the Critic found surviving: transpose the last two constructor arguments of
`AclRowMarshal.NativeFilterAccessors` (`...FilterOperation` ↔ `...FilterPermissionType`).
Run with `bin/`/`obj/` wiped for both projects, so the build was a full restore + compile
(`Restored …` + both `-> …dll` lines present, no build error):

| | Before this fix (Critic, M4) | After this fix (re-run) |
|---|---|---|
| Result | **0 RED — 1966/1966 GREEN** | **1 RED — `Failed: 1, Passed: 1969, Total: 1970`** |
| Which test | — | `NativeFilterAccessors_BindEveryMemberToItsOwnAbiSymbol` |
| Diff reported | — | `Operation=…_permission_type` / `PermissionType=…_operation` at `pos 5` — it names the transposed pair |

The injection was then reverted and `git diff` over `AclRowMarshal.cs` is **empty**; the
final suite is 1970/1970 on both TFMs.

**Unmeasured-danger-model rule honoured (PLAN §6.1 item 7).** The class remark added to the
guard file states only what was measured — that the Operation/PermissionType transposition
survived at 1966/1966 and why (the two message-asserting fixtures both use code **3**). It
makes no segfault claim: the Critic re-measured the cross-wire the earlier close-out cited
and got **2 RED, guard-only, no crash**. That framing is not repeated anywhere in this cycle's
code, commits or records.

---

## 76.2 — LOW — CLOSED. The `alterClientQuotas` duplicate-entity rejection now records that Java accepts the input

No behaviour change (the rejection is correct — the ABI refuses a repeated entity,
`h:8300-8302`, and collapsing would drop an alteration the caller wrote). What was missing was
the divergence record required by `definition-of-done.md` §7 / `bindings/dotnet/CLAUDE.md` §4.
Added at both sites the finding names, one sentence each, per the maintainer's
keep-comments-light constraint:

- `src/Confluent.Kafka/Internal/NativeAdminClient.cs` (the `AlterClientQuotas` remark) — states
  that **Java accepts a repeated entity**, cites `KafkaAdminClient.java:4314-4318` (the
  unconditional `put` collapsing the futures map while every alteration is still sent), and
  labels it a recorded divergence rather than parity.
- `src/Confluent.Kafka/Admin/IAdmin.cs` (`AlterClientQuotas`' `<param name="entries">`) — the
  same fact on the public surface, plus the corollary a user hits: this is the one P6 RPC whose
  Java-faithful `Collection` shape is not accepted verbatim, so code ported from Java may need
  to de-duplicate at the call site. The note is placed on `entries` so it sits beside the two
  sibling RPCs' existing "Duplicates collapse, because Java keys its result on a map"
  (`IAdmin.cs:976`, `:1003`), which is where the asymmetry was visible and unexplained.

---

## 76.3 — LOW — CLOSED. The `CS0618` region now brackets only the method that needs it

`src/Confluent.Kafka/Internal/Interop/AdminCallbacks.cs`: the
`#pragma warning disable CS0618` moved down from above `OnDescribeAcls` to immediately above
`OnListClientMetricsResources`, the one method that uses the deprecated
`ClientMetricsResourceListing` — which is what the suppression's own comment ("Java deprecates
the listing type itself; mirrored, not avoided") has always described. `OnDescribeAcls` and
`OnDescribeClientQuotas` are now outside it, so a future obsolete-API use in either is a real
warning again rather than a silent suppression.

**Measured:** the `-c Release --no-incremental` solution build after the move reports
**`0 Warning(s), 0 Error(s)`** across all six TFM outputs — so neither P6 trampoline needed the
suppression, confirming the Critic's own probe from the build this cycle already had to run.

---

## No approved items remain

`COMMENTS.76.md` holds no open finding after this cycle. Its remaining sections are the
Critic's Observations (O1–O5, explicitly not filed), the categories checked with nothing found,
and the suggested rule update — none of which is an action item for this Actor:

- **O3** (Java's `createAcls` failing an indefinite binding locally vs the ABI documenting it
  sent) is a **Rust-core** matter already reported upward to `kafka-critic`; the binding must
  not add Kafka logic to paper over it (`bindings/CLAUDE.md` §1.2 / the one law).
- **The suggested rule update** targets `.claude/rules/ffi-marshalling.md` and
  `bindings/dotnet/CLAUDE.md`, both off limits to automated agents — it is the Manager's to
  carry. This cycle's fix is the *code-side* satisfaction of exactly that rule: the guard now
  pins every construction site passing two or more same-typed accessor delegates positionally,
  and the pin was shown RED by transposing the **adjacent same-typed pair** rather than an
  arbitrary one.
- **`COMMENTS.FP.md` / `COMMENTS.FN.md` do not exist for this binding** and their absence is
  not a task — only the repo-root `COMMENTS.FP.md` exists, and its entries are Rust-core
  Milestone-11 material with nothing calibrating for the .NET binding.

---

## Addendum — independent verification pass on the fix cycle (`fc532820`/`7ecf95f3`/`ff70692e`)

A second Critic pass re-derived all three findings independently (mutation-tested 76.1's
guard, checked every pinned symbol against the header directly, probed the guard's
discovery criterion for false negatives by widening it) and confirmed **all three genuinely
closed, no new finding**. Full detail in `bindings/dotnet/COMMENTS.76.md`'s own addendum
(kept local, not archived — this is the summary).

One process item it surfaced: the last fixup (`ff70692e`) targeted the wrong parent for
`--autosquash` (matched `describeAcls`'s subject line, but its edit's destination context
only exists in the tree after `describeClientQuotas`) — a dry-run rebase in a scratch
worktree confirmed it would conflict at step 7/11. **Fixed by the Manager**: `git commit
--amend` retargeted the subject to `describeClientQuotas` (message-only, tip commit,
unpushed) → new HEAD `e2e37c72`. Re-verified: the same dry-run now completes 11/11 clean
with an empty post-squash diff against the un-retargeted tip, confirming the change is
metadata-only.

**Verdict: P6 is closed.**
