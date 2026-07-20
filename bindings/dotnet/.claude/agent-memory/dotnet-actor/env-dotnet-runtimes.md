---
name: env-dotnet-runtimes
description: Dev machine has only the net10 .NET runtime (SDK 10.0.301, no net8/net462 runtime); net8.0 test legs run via RollForward, net462/net8 native legs are CI obligations
metadata:
  type: project
---

The dev machine has .NET SDK 10.0.301 but only the **net10 runtime**
(`Microsoft.NETCore.App 10.0.9`) installed — no net8.0 runtime, and no .NET
Framework / Mono (it's macOS).

**Why:** it constrains what `dotnet test` can run locally. Builds are fine (net8.0
ref pack restores from NuGet), but running a net8.0 test assembly needs either the
net8.0 runtime or roll-forward.

**How to apply:**
- The unit-test project sets `<RollForward>Major</RollForward>` so net8.0 test
  legs roll forward onto the net10 runtime and the TFM-run matrix executes here.
  Keep this until the net8.0 runtime is installed / CI runs it natively.
- Two runtime legs are **CI obligations**, not locally runnable here, and must not
  be silently skipped: net462 (served by the netstandard2.0 asset — no separate
  net462 target) and net8.0-on-net8.0-runtime. Record such deferrals in
  `COMMENTS.DONE.<N>.md`.
- Verify before relying on this: `dotnet --list-runtimes` (state can change if the
  net8.0 runtime is later installed).

Related: [[workflow-commit-signing]].
