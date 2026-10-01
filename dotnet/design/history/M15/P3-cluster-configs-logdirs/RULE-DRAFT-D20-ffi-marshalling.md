# DRAFT for the maintainer to apply — D20 → `ffi-marshalling.md` §0.4

**Status: NOT APPLIED. Handed to the maintainer 2026-09-09.**

`bindings/dotnet/.claude/rules/ffi-marshalling.md` is a **rule file** — automated
agents do not edit it (root `CLAUDE.md`). This file is the Manager's draft of
ruling **D20**'s insertion text so the maintainer can apply it verbatim or edit it
first. **No agent has modified `ffi-marshalling.md`.**

## Why this file, and where

D20 governs *any* reflection surface-set assertion — producer, consumer and admin
alike — so it belongs in **Part 0 · Shared mechanics**, not in a client-specific
Part. Proposed as a new **§0.4**, inserted after §0.3 (Thread topology) and before
`# Part A · Producer`, i.e. at **line 235** of the current 2159-line file.

It does not collide with anything: the file's five existing "reflection" mentions
(`:110`, `:162`, `:164`, `:170`, `:183`) are all about the **native library
loader** and none about test assertions — verified by grep, against a
control-positive of 55 `GCHandle` hits.

---

## Proposed insertion text

```markdown
## 0.4 Reflection surface-set assertions — filter by metadata, never by name

**Decision:** A test that pins "the known members of a surface" as a **set** MUST
select those members with `BindingFlags` plus a `CompilerGeneratedAttribute`
exclusion, and **MUST NOT** narrow the set with a **name predicate** —
no `== nameof(X)`, no `StartsWith("Prefix")`, no name regex. Members that are
legitimately part of the surface but not part of the pinned concept go in the
**expected set**, not into the filter.

```csharp
// WRONG — the filter decides what is visible, so the next differently-named
// member is invisible to the assertion and lands without turning it red.
.GetMethods(BindingFlags.NonPublic | BindingFlags.Static)
.Where(m => m.Name.StartsWith("Complete", StringComparison.Ordinal))

// RIGHT — metadata decides visibility; names live in the expectation.
.GetMethods(BindingFlags.NonPublic | BindingFlags.Static)
.Where(m => !m.IsDefined(typeof(CompilerGeneratedAttribute), inherit: false))
// … then assert set-equality against the full expected name list,
//   including members like ReadStringKey that are not the pinned concept.
```

**Why:** a name filter silently converts a *surface-set* assertion into a
*subset* assertion. The assertion still passes, so nothing signals the loss.

This is not hypothetical — the same defect class occurred **twice inside one
stage** (M15/P3 Stage 1):

  - `KeyedResultMarshal.CompleteList` was added and matched neither
    `== nameof(Complete)` nor `== nameof(CompleteAggregate)`, so it landed
    **without turning any assertion red**.
  - The fix for that then re-pinned the surface with
    `.StartsWith("Complete")` — the same mistake one width wider — **in a test
    whose own remark documented the first occurrence**. Proven by injection:
    a `WalkSomething` method landed **green**; control `CompleteSomething` went
    **red**.

**The objection this rule overrides, because it was measured and is false:**
"widening past the name filter would drag in unrelated helpers and
compiler-generated members." Measured on the real type,
`GetMethods(NonPublic | Static)` returned **five** methods, **none**
compiler-generated — because a lambda's display class is a **nested type**, which
`GetMethods` on the containing type never returns. The only genuinely unrelated
member was one helper, which belongs in the expected set.

**How to apply:**

  - Select by `BindingFlags` (+ `CompilerGeneratedAttribute` exclusion) only.
  - Put every expected member name in the expectation, and assert **set
    equality** — not `Count`, not `Contains`, not `Single`.
  - **Prove the assertion is sensitive to a *differently-named* addition**, not
    just to a same-prefix one. Adding a method sharing no prefix with the
    existing members must turn it red. A guard only verified against a
    same-prefix addition has not been verified for the case that actually broke.

**Anti-patterns to flag in review:**

  - Any name predicate — `==`, `StartsWith`, `Contains`, regex — inside the
    `.Where(...)` of a surface-set assertion.
  - `Single(...)` or `Assert.Equal(N, …Count())` used to pin a surface: both are
    subset assertions wearing a set assertion's clothes, and both break
    *usefully* on the next same-named addition while staying silent on a
    differently-named one.
  - A remark asserting that widening the filter is impractical, without a
    measurement in the same commit showing what widening actually returns.
```

---

## Provenance

  - Raised by **Critic 69** in the M15/P3 Stage-1 round-2 review, on the strength
    of the twice-in-one-stage recurrence, and recorded in that phase's
    `COMMENTS.DONE.69.md` (finding 69.4).
  - ⚠ Critic 69 **revised its own round-1 recommendation** to get here: round 1
    proposed "narrow the sentence, not the test", which it withdrew on new
    measurement as leaving a live hole (`COMMENTS.69.md:257-259`, verbatim: *"so
    I am revising that recommendation rather than restating it"*).
  - **Approved by the maintainer 2026-09-09**, alongside D18 (method vs property)
    and D19 (options at the submit seam).
  - Already implemented in the .NET tree by Actor 69 in `11a24a34`; adoption here
    is about binding everyone else. **The rule is therefore already satisfied by
    the code — applying this text does not create work.**

## ⚠ A related conflict the maintainer should settle before P9

The M15 roadmap's §10 schedules **"`ffi-marshalling.md` Part C · Admin"** as a
**P9 deliverable** — i.e. it tasks an agent with authoring content into this same
rule file, which agents are forbidden to edit. That is a real conflict in the
milestone plan, not a misreading of it.

It needs a decision **before P9 begins**, not during. The obvious resolution,
consistent with how D20 was handled here: **the Manager drafts Part C and the
maintainer applies it.** Recorded in the P3 plan's §14 so it is not lost.
