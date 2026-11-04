# M0 / P1 — Rename binding identity to `Confluent.Kafka`

**Status:** APPROVED 2026-07-29. Agent number **N=1** (personas `dotnet-actor` /
`dotnet-critic`; Manager is the root `project-manager`).

Rename the .NET binding's identity from `Confluent.Kafka.ShareConsumer` to
`Confluent.Kafka` across the M0/P0 scaffolding.

## 1 · Framing

This is not a preference rename — it is **bringing the scaffolding into
compliance with its own rulebook**. `bindings/dotnet/CLAUDE.md` §2 already
specifies the file map as `Confluent.Kafka.sln`, `src/Confluent.Kafka/`,
`tests/Confluent.Kafka.UnitTests/`, and §4's *Namespace / package id* row already
reads **`Confluent.Kafka`**. The rulebook is the target state; the M0/P0
artifacts are the drift. §2/§4 are the review contract, not a judgment call.

Motivation: the original name was chosen when the binding was scoped to
share-consumer functionality only. The scope is now the full client, and
`ShareConsumer` names a KIP-932 feature that `.claude/rules/consumer-threading.md`
§20 puts explicitly out of scope — so the name is actively misleading.

## 2 · Phase identity

Plan location: `bindings/dotnet/design/history/M0/P1-rename-identity/PLAN.md`.

**Why M0/P1:** M0 is the bootstrap milestone (no P/Invoke, no `SafeHandle`, no
Kafka behavior). This work adds **no capability** — it corrects M0/P0's output to
match CLAUDE.md §2/§4. Same milestone; `P0-scaffolding` is taken, so `P1`. The
first *implementation* phase (`STATUS.md` "Next up") is unaffected and
unnumbered; it becomes M1/P0 when it starts. Numbering is binding-local per
CLAUDE.md §8.4, independent of the repo-root Rust `design/`.

**Agent number:** N=1, reused from M0/P0. Safe because the archived record lands
in a different phase directory (`M0/P1-rename-identity/COMMENTS.DONE.1.md`), so
there is no collision with `M0/P0-scaffolding/COMMENTS.DONE.1.md`.

## 3 · Scope

**In:** 7 tracked path renames, 15 in-content edits across 6 build/code files,
`design/current/STATUS.md`, and the governance artifacts (this PLAN + the
archived `COMMENTS.DONE.1.md`).

**Out:** the net462 test-TFM gap, the `TfmSentinelTests` doc-comment overclaim,
and `System.Memory` 4.5.5 vs ckd 2.15.0's 4.6.3. None are pulled in; finding #2
interacts with this rename and gets a compensating verification step (V7)
instead of a fix — see R3.

## 4 · Branch strategy for part (a) — already satisfied

Getting PR #123's renamed rulebooks into PR #130's branch is **already done
locally** via merge commit `c0190ac`; `git merge-base --is-ancestor` confirms
`prashah_dev_dotnet_claude_rules_poc` is a full ancestor of the current branch.
`bindings/dotnet/CLAUDE.md` and `.claude/agents/dotnet-critic.md` contain zero
`ShareConsumer` occurrences on local HEAD.

**Decision: keep the merge. Do not rebase.** PR #130 is a published, stacked PR
based on PR #123's branch. Rebasing would rewrite the 8 poc commits into new
SHAs, require a force-push, detach PR #130's diff base from PR #123 (GitHub
would attribute #123's commits to #130), and stale out inline review comments
anchored to those SHAs. A merge keeps `poc` a genuine ancestor, so #130's diff
shows only the scaffolding + rename and #123 can merge to master independently.
The linear-history argument is weak for a short-lived stacked feature branch.

The merge is **unpushed** (HEAD is 11 commits ahead of `origin`, 0 behind), so
`origin/…bootstrap_verify` still carries the stale `CLAUDE.md` and PR #130 still
shows the staleness on GitHub. **Push is NOT authorized in this phase** — the
user will review the commits and push themselves.

⚠ **`bindings/dotnet/CLAUDE.md`, `bindings/dotnet/.claude/rules/ffi-marshalling.md`,
and `bindings/dotnet/.claude/agents/dotnet-{actor,critic}.md` are already
correct.** If any appears in a `git show --stat` for C1–C4 that is a defect: it
means work was duplicated against PR #123 and will conflict when #123 merges.

## 5 · Ordered deliverables

**D1 — Pre-flight: destroy stale build artifacts.** Before any `git mv`:

```
find bindings/dotnet -type d \( -name obj -o -name bin \) -prune -exec rm -rf {} +
```

Mandatory, not hygiene — see R1.

**D2 — The 7 renames** (6 `git mv` invocations; the two directory moves carry
`TfmSentinelTests.cs` and both `.gitkeep` files):

```
git mv bindings/dotnet/Confluent.Kafka.ShareConsumer.sln  bindings/dotnet/Confluent.Kafka.sln
git mv bindings/dotnet/Confluent.Kafka.ShareConsumer.snk  bindings/dotnet/Confluent.Kafka.snk
git mv bindings/dotnet/src/Confluent.Kafka.ShareConsumer  bindings/dotnet/src/Confluent.Kafka
git mv bindings/dotnet/src/Confluent.Kafka/Confluent.Kafka.ShareConsumer.csproj \
       bindings/dotnet/src/Confluent.Kafka/Confluent.Kafka.csproj
git mv bindings/dotnet/tests/Confluent.Kafka.ShareConsumer.UnitTests \
       bindings/dotnet/tests/Confluent.Kafka.UnitTests
git mv bindings/dotnet/tests/Confluent.Kafka.UnitTests/Confluent.Kafka.ShareConsumer.UnitTests.csproj \
       bindings/dotnet/tests/Confluent.Kafka.UnitTests/Confluent.Kafka.UnitTests.csproj
```

**D3 — The 15 in-content edits.** Every occurrence is
`Confluent.Kafka.ShareConsumer` → `Confluent.Kafka` (and `…ShareConsumer.UnitTests`
→ `Confluent.Kafka.UnitTests`); no other transformation.

| File | Lines | Sites |
|---|---|---|
| `.editorconfig` | 1 | header comment |
| `Confluent.Kafka.sln` | 8, 12 | 2 project display names + 2 `.csproj` paths (4 strings, 2 lines) |
| `Directory.Build.props` | 4, 25, 40 | header comment · `<Product>` · `<AssemblyOriginatorKeyFile>` |
| `src/Confluent.Kafka/Confluent.Kafka.csproj` | 4, 15, 16, 30 | header · `<RootNamespace>` · `<AssemblyName>` · `InternalsVisibleTo Include=` |
| `tests/Confluent.Kafka.UnitTests/Confluent.Kafka.UnitTests.csproj` | 4, 16, 17, 32 | header · `<RootNamespace>` · `<AssemblyName>` · `<ProjectReference Include=>` |
| `tests/…/TfmSentinelTests.cs` | 19 | file-scoped `namespace` |

**D4 — `design/current/STATUS.md`** (6 name sites + narrative): update the
structure map (lines 16, 24, 25, 31, 32), the strong-naming bullet (line 78), add
an M0/P1 phase entry with its verification state, and repoint the governance
pointers at `design/history/M0/P1-rename-identity/`. Plus two approved pickups:

  - `Native` → `NativeMethods` at lines 9 and 103 (stale since poc commit
    `5ead48d`, which renamed it for CA1060). **Line 55's "Native-copy MSBuild
    target" refers to copying the native *library* and is correct as-is — do not
    change it.** There are no `Utf8` occurrences in this file, so the companion
    `Utf8` → `Utf8Marshal` check is a verified no-op.
  - Name the native-copy target's new phase id (**M1/P0**) in the D2 wording.

**D5 — Governance artifacts:** this PLAN, plus the supersession note of §6.

**D6 — Push: NOT AUTHORIZED.** Stop after the Critic loop closes and C4 lands.

## 6 · Archived-history decision

**Leave `design/history/M0/P0-scaffolding/PLAN.md` and `COMMENTS.DONE.1.md`
verbatim. Add one dated supersession note at the top of the archived `PLAN.md`
only.**

Justification: those files are the approved-plan-of-record and the closed review
record of a DONE phase. The archived `PLAN.md` is what was approved on
2026-07-20; rewriting it retroactively falsifies the approval record. Worse,
`COMMENTS.DONE.1.md` line 28 records the Critic's **verified finding** that
`InternalsVisibleTo="Confluent.Kafka.ShareConsumer.UnitTests"` matched — editing
it would make a historical verification claim describe an assembly name that did
not exist when the check ran. `STATUS.md`'s "Review outcome" cites that record by
commit SHA; the two must stay consistent.

Leaving them unannotated is the confusing option. A single **additive** note
resolves it: the reader learns the discrepancy is intentional and where current
truth lives.

The note (≈5 lines, top of the archived `PLAN.md`, below the title) states that
the phase shipped under the `Confluent.Kafka.ShareConsumer` identity, that M0/P1
renamed it to `Confluent.Kafka`, that the body below is preserved verbatim as the
record of what was approved and verified at the time, and points to
`design/history/M0/P1-rename-identity/PLAN.md` and `design/current/STATUS.md`.
`COMMENTS.DONE.1.md` gets **no** note — it is reached via `PLAN.md`/`STATUS.md`,
and annotating a closed review record is a step too far.

**Consequence:** the note names the old identity, so a repo-wide grep will not
return zero. The gate is scoped accordingly — see V2.

## 7 · Strong-name nuance

**Renaming the `.snk` file does not change the key. Do NOT regenerate it. Do NOT
run `sn -k`.** Verified pre-change:

  - `sn -tp` on the key file → public key blob **identical** to the
    `InternalsVisibleTo … Key="0024…16da"` attribute, token `a6a493010a30d243`
  - `.snk` SHA-256 `d33f5c9818998b5ea5bf92b35c46f469a472605274c4ccc460eda8f7068eb197`
    — must be byte-identical after the move
  - The `Key=` blob stays **verbatim**; only the `Include=` simple name changes

A regenerated key would silently change the assembly identity — a
binary-breaking change `Directory.Build.props` explicitly warns against taking
after publish.

## 8 · Commit strategy

**Three commits; every commit builds green.** The "pure `git mv` commit, then
content commit" split is deliberately rejected: it produces an intermediate
commit where the `.sln`, `ProjectReference` and `AssemblyOriginatorKeyFile` all
point at moved paths — a broken bisect point — in exchange for rename
detectability that is verifiable anyway (V8).

| # | Contents | Gate |
|---|---|---|
| **C1** | D2 renames + D3 edits | full V1–V11 green |
| **C2** | D4 — `design/current/STATUS.md` (incl. both pickups) | docs only |
| **C3** | D5 — this PLAN + supersession note on the archived `PLAN.md` | docs only |

**C4** closes the phase: copy the closed `COMMENTS.DONE.1.md` into
`design/history/M0/P1-rename-identity/`. The active `bindings/dotnet/COMMENTS.1.md`
is gitignored (`.gitignore:10`, `COMMENTS\.[0-9]*\.md`), so resetting it is a
**local-only** action with nothing to commit; only the `DONE` copy is tracked.

**Keeping renames detectable:** git records renames at diff time by content
similarity, not in the object store, so the only hazard is similarity dropping
under threshold. It does not here — the `.snk` and both `.gitkeep`s are
byte-identical (100%, caught by git's exact-rename pass), and the csprojs/sln
change 4 lines out of 30–70, well above 50%. Proven by V8, not assumed. Staging
uses **explicit paths**, never `git add -A` (R4).

## 9 · Verification — commands and expected results

Baselines were measured on the pre-change tree, so "expected" means *identical to
the pre-change result*.

```
cd bindings/dotnet
```

**V1 — old directories fully gone:**
`test ! -e src/Confluent.Kafka.ShareConsumer && test ! -e tests/Confluent.Kafka.ShareConsumer.UnitTests`
→ exit 0

**V2 — zero occurrences outside archived history:**
  - `grep -rIn "ShareConsumer" . --exclude-dir=design/history` → **no output** (exit 1)
  - `git ls-files | grep -i shareconsumer` → **no output**
  - `grep -rIn "ShareConsumer" design/history` → **8 hits** (the 7 pre-existing +
    the 1 supersession-note reference), all under `M0/P0-scaffolding/`

**V3 — build** (CLAUDE.md §7.2; `TreatWarningsAsErrors` + `EnforceCodeStyleInBuild`):
`dotnet build Confluent.Kafka.sln` → `Build succeeded. 0 Warning(s) 0 Error(s)`,
**5 assemblies**: `src/Confluent.Kafka/bin/Debug/{netstandard2.0,net8.0,net10.0}/Confluent.Kafka.dll`
and `tests/Confluent.Kafka.UnitTests/bin/Debug/{net8.0,net10.0}/Confluent.Kafka.UnitTests.dll`.
Build is standalone; CLAUDE.md §7.1's Rust-first rule does not bite because M0
has no `[DllImport]`.

**V4 — test:** `dotnet test Confluent.Kafka.sln -f net10.0` → `1 passed, 0 failed`

> **Expected and NOT a blocker:** only the .NET 10 runtime is installed locally
> (`dotnet --list-runtimes` shows only `Microsoft.NETCore.App 10.0.9`).
> `dotnet test -f net8.0` fails with "You must install .NET" — a
> **missing-runtime failure, CI-only**. The net8.0 *build* leg must still pass in
> V3. net462 *runs* are Windows/CI-only. Do not "fix" this by dropping a TFM.

**V5 — format:** `dotnet format Confluent.Kafka.sln --verify-no-changes` → exit 0,
no output

**V6 — strong name unchanged:**

```
shasum -a256 Confluent.Kafka.snk        # d33f5c98…8eb197  (byte-identical)
sn -tp Confluent.Kafka.snk              # Public Key Token: a6a493010a30d243
sn -T src/Confluent.Kafka/bin/Debug/net10.0/Confluent.Kafka.dll                        # a6a493010a30d243
sn -T tests/Confluent.Kafka.UnitTests/bin/Debug/net10.0/Confluent.Kafka.UnitTests.dll  # a6a493010a30d243
```

`sn -T` takes an **assembly**; `sn -tp` takes a **key file**. `sn -T` on the
`.snk` throws `BadImageFormatException` — tool misuse, not a signing failure.

**V7 — IVT grant consistency** (compensates for R3):

```
grep -o 'InternalsVisibleTo Include="[^"]*"' src/Confluent.Kafka/Confluent.Kafka.csproj
grep -o '<AssemblyName>[^<]*</AssemblyName>' tests/Confluent.Kafka.UnitTests/Confluent.Kafka.UnitTests.csproj
```

→ both must name `Confluent.Kafka.UnitTests`. Also confirm the `Key=` blob still
equals `sn -tp` output (whitespace-stripped).

**V8 — renames detected as renames:**
`git show --stat --find-renames=40% <C1>` and
`git log -1 --diff-filter=R --name-status --find-renames=40% <C1>`
→ **7 `R` entries**; **no** `create mode`/`delete mode` pair for those paths in
`git show --summary <C1>`

**V9 — sln GUID integrity:** `git show <C1> -- Confluent.Kafka.sln` → exactly
**2 changed lines** (8 and 12).
`grep -oE '\{[0-9A-F-]{36}\}' Confluent.Kafka.sln | sort -u | wc -l` → **7**
unique GUIDs, unchanged: the two project-type GUIDs (`FAE04EC0-…`, `2150E333-…`),
the library `{AF17E29B-…}`, the test project `{46C515E1-…}`, and the
`src`/`tests`/`build` solution folders (`{827E0CD3-…}`, `{0AB3BF05-…}`,
`{B8EB6799-…}`). `ProjectConfigurationPlatforms`, `NestedProjects` and the
`build` folder's `SolutionItems` must be byte-identical.

**V10 — sln BOM preserved:** `xxd -l 3 Confluent.Kafka.sln` → `efbbbf`. The file
is UTF-8 **with BOM**; dropping it is a spurious diff and can upset VS.

**V11 — untracked personas intact** (from repo root):
`git status --short --untracked-files=all | grep dotnet-` → exactly
`?? .claude/agents/dotnet-actor.md` and `?? .claude/agents/dotnet-critic.md`;
`git show --stat <each commit> | grep -c 'dotnet-actor\|dotnet-critic'` → **0**

## 10 · Risks and mitigations

**R1 — Orphaned `obj/`+`bin/` under the old directory names (high likelihood,
most under-appreciated).** `git mv` on a directory moves only *tracked* files;
untracked `obj/`/`bin/` stay behind, leaving `src/Confluent.Kafka.ShareConsumer/`
**alive on disk** with a stale `Confluent.Kafka.ShareConsumer.dll` and a cached
`project.assets.json` pointing at the old `AssemblyName`. Symptoms range from a
confusing double-directory layout to `dotnet test` resolving a stale assembly.
*Mitigation:* D1 deletes them **before** the moves; V1 asserts the old paths are
gone.

**R2 — `.snk` regeneration.** Mitigated by §7's prohibition + V6's SHA-256 and
token assertions.

**R3 — Silent `InternalsVisibleTo` breakage.** `TfmSentinelTests.cs` references
**no library type**, so if `InternalsVisibleTo Include=` is left as
`…ShareConsumer.UnitTests` while the test assembly is renamed, **the build still
succeeds** and the broken grant surfaces only in a later phase. *Mitigation:*
V7's deterministic string equality — closes the gap without expanding scope. The
real fix (an `internal` type + a test that touches it) is recommended as the
first item of the implementation phase.

**R4 — Committing the untracked root personas.** They are untracked but **not
gitignored**, so `git add -A` / `git add .` from the repo root would stage them,
violating CLAUDE.md §8.4. They exist because the harness does not auto-register
`bindings/dotnet/.claude/agents/*.md`. *Mitigation:* explicit paths only; V11
before **and** after every commit.

**R5 — Touching already-correct upstream files.** See §4's warning box.
Verified by `git show --stat`.

**R6 — Package-id collision gate.** CLAUDE.md §4 flags a ⚠ pre-publish gate:
sharing the `Confluent.Kafka` id with ckd means a project can hold ckd 2.x **or**
this client, never both. This phase makes the *name* collide but publishes
nothing — there is no `PackageId` anywhere and the library sets no `IsPackable`.
*Mitigation:* the gate stays open and is restated in `STATUS.md`; do **not** add
`PackageId`, `Pack*`, or NuGet metadata in this phase.

**R7 — sln rewritten wholesale** (BOM lost, GUIDs reordered, or `.slnx`
regeneration — SDK 10 defaults to `.slnx`, and M0/P0 already recorded a deviation
to keep classic `.sln`). *Mitigation:* surgical two-line edit; never
`dotnet sln add/remove`; V9 + V10.

## 11 · Rollback

Pre-change SHA: **`c0190ac`**.

The 3 commits are unpushed, so before D6:

```
git reset --hard c0190ac
find bindings/dotnet -type d \( -name obj -o -name bin \) -prune -exec rm -rf {} +
```

The artifact cleanup is **required** — `reset --hard` restores tracked files but
leaves untracked `obj/`/`bin/` at whichever paths the build last used,
reintroducing R1 in mirror image.

If rollback is ever needed after a push, do **not** force-push a published
stacked PR: `git revert --no-commit <C3> <C2> <C1>`, one revert commit, then the
same cleanup. Either way the part-(a) merge `c0190ac` is preserved — it is not
part of this phase's commits and must never be reverted.

## 12 · Actor → Critic sequencing

Personas are `dotnet-actor` / `dotnet-critic` (CLAUDE.md §8.1); the Manager is the
root `project-manager`.

1. Save this plan (done).
2. **`dotnet-actor` (N=1)** — D1→D5 as C1–C3; gates V1–V11; check
   `bindings/dotnet/COMMENTS.1.md` first.
3. **`dotnet-critic` (N=1)** — review C1–C3 against CLAUDE.md §2, §4 (incl. the
   pre-publish gate), §7.2/§7.5, §8.4, and `definition-of-done.md`. Findings →
   `bindings/dotnet/COMMENTS.1.md`; code untouched.
4. Summarize; loop 2↔3 until `COMMENTS.1.md` is empty.
5. Final handoff: `STATUS.md` current; C4 archives `COMMENTS.DONE.1.md`; reset the
   gitignored `COMMENTS.1.md`; **stop — no push.**

## 13 · Out of scope (recorded, not actioned)

1. The test project has no `net462` TFM leg, so the net462 smoke test required by
   `ffi-marshalling.md` §0.1 / CLAUDE.md §7.4 cannot run even in CI.
2. `TfmSentinelTests.cs`'s doc comment claims it proves `InternalsVisibleTo`
   wiring, but it references no library type, so it does not (see R3).
3. `System.Memory` is pinned to 4.5.5 vs ckd 2.15.0's 4.6.3.
