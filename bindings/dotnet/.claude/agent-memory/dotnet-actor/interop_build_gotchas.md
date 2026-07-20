---
name: interop-build-gotchas
description: Non-obvious MSBuild/analyzer facts for the .NET binding csproj + classic [DllImport] scaffolding
metadata:
  type: project
---

Two build gotchas hit while landing the M1/P1 interop scaffolding (csproj +
`Native` [DllImport] class). Neither is derivable from reading the fixed repo.

**1. MSBuild XML comments cannot contain `--` (double-dash).**
Writing a `cargo build --features ffi` string inside a `<!-- ... -->` comment in a
`.csproj` fails project load with `MSB4025: An XML comment cannot contain '--'`.
This is XML-spec, not MSBuild-specific. Arrow-like `->` (single dash) is fine.
- **Why:** the native-copy target comment referenced the cargo command verbatim and
  broke `dotnet build` at load time (before any compile).
- **How to apply:** in any csproj/props/targets comment, reword to avoid literal
  `--` (e.g. "the cargo build ffi step"). `--` is legal in attribute *values*
  (e.g. an `<Error Text="run cargo build --features ffi" />`), only illegal in
  comments.

**2. Classic `[DllImport]` fires NO analyzer warnings under this binding's SDK
config** (AnalysisLevel=latest + EnforceCodeStyleInBuild + TreatWarningsAsErrors,
TFMs netstandard2.0;net8.0;net10.0, SDK 10.0.301). Specifically CA5392
(DefaultDllImportSearchPaths) and SYSLIB1054 (use [LibraryImport]) did NOT fire —
CA5392 is opt-in (not in the default analysis set), SYSLIB1054 is Info-severity
(not elevated to a warning). So the M1/P1 "contingent" analyzer suppressions were
NOT needed and none were added.
- **Why:** the PLAN made suppressions contingent on observing them fire; a clean
  `--no-incremental` build reported 0 diagnostics with `/unsafe+` active.
- **How to apply:** for future phases adding more classic `[DllImport]`s, do NOT
  preemptively add CA5392/SYSLIB1054 suppressions — build first and confirm. If a
  future SDK bump elevates SYSLIB1054 to a warning, re-verify before recommending a
  suppression (a file-scoped `.editorconfig` under Internal/Interop/ or a
  `[SuppressMessage]` on `Native`, narrowly scoped, each with a one-line reason).
