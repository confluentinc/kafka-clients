---
name: review-verify-mechanics
description: How to verify a .NET binding milestone locally as Critic — standalone dotnet build/test for no-interop work, COMMENTS.N.md is git-ignored, lock via mkdir
metadata:
  type: project
---

Mechanics for reviewing a `bindings/dotnet/` milestone (verified during the
Milestone-1 skeleton review).

**Why:** These are non-obvious process facts that speed up (and de-risk) each
review; getting them wrong wastes a pass or produces a false blocker.

**How to apply:**

- **No-interop milestones verify standalone.** When the change has **no
  `[DllImport]`** (nothing P/Invokes — e.g. a structural skeleton), the two-stage
  Rust→.NET pipeline (`cargo build --features ffi` first) is **not** a prerequisite:
  `dotnet build`, `dotnet test`, and `dotnet format --verify-no-changes` all run on
  their own. Do not raise "native library missing / cargo not built" as a blocker
  for such milestones. (Once real `[DllImport]`s land and a native-copy target
  exists, the native lib IS required to run — reassess then.)
- **Dev machine has only the net10 runtime** (SDK 10.0.301). The net8.0 test leg
  runs via `<RollForward>Major</RollForward>` onto net10; the faithful
  net8-on-net8 and net462-via-ns2.0 runtime legs are **CI obligations**, not local
  gaps — expect them recorded in `COMMENTS.DONE.N.md`, not run locally.
- **`COMMENTS.N.md` is git-ignored** (repo-root `.gitignore`: `COMMENTS\.[0-9]*\.md`);
  `COMMENTS.DONE.N.md` is committed. So the Critic's active comments file is a
  working artifact that won't show in `git status`/history — that is expected.
- **Locking:** the repo has no `.lock` convention. Use an atomic `mkdir
  COMMENTS.N.md.lock` before writing `COMMENTS.N.md`, `rmdir` after (per
  agent-roles.md "exclusive lock").

Related: [[fp-copyright-year]].
