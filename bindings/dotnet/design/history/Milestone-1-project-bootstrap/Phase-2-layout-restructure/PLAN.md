# Milestone 1 · Phase 2 — Layout restructure (A → B) + CLAUDE.md §2 fix

**Status:** executed & reviewed clean.

> Backfilled record. Captured here per CLAUDE.md §8.4.

## Goal
Move the binding from the Phase-1 layout (A: a project-rooted folder with an
inner `src/` and a nested `tests/`) to the idiomatic dotnet/runtime layout
(B: top-level `src/` + `tests/` siblings, one folder per project), and correct
`bindings/dotnet/CLAUDE.md` §2, which described the wrong (A) shape.

## Why B
- Idiomatic .NET (dotnet/runtime, aspnetcore) — instantly familiar.
- No csproj-inside-csproj: the test project leaves the library project's
  directory, so the library's default compile globbing can't pull in test files
  — the `EnableDefaultCompileItems=false` / explicit `Compile Include` scoping
  hack is dropped.
- Clean folder→namespace mapping; `Internal/` stays the visibility marker
  (project root = public, `Internal/` = internal).
- Scales: `tests/…UnitTests` + a later `tests/…IntegrationTests` sit side by side.

## Target tree (B)
```
bindings/dotnet/
├─ Confluent.Kafka.ShareConsumer.sln · Directory.Build.props · .editorconfig · .gitignore
├─ src/Confluent.Kafka.ShareConsumer/
│  ├─ Confluent.Kafka.ShareConsumer.csproj   ← simplified (no glob hack)
│  └─ Internal/Interop/Native.cs             ← shell only
└─ tests/Confluent.Kafka.ShareConsumer.UnitTests/
   ├─ …UnitTests.csproj
   └─ ScaffoldingTests.cs
```

## Rules for the move
- `git mv` to preserve history; everything under
  `Confluent.Kafka.ShareConsumer/src/**` → `src/Confluent.Kafka.ShareConsumer/**`
  (inner `src/` dropped); the test project → top-level `tests/`.
- Namespaces unchanged (`Confluent.Kafka.ShareConsumer.Internal.Interop`) — the
  old inner `src/` was never a namespace segment.
- Simplify the library csproj (drop the scoping hack); keep TFMs
  `netstandard2.0;net8.0;net10.0`, `#nullable enable`, `AllowUnsafeBlocks`,
  `InternalsVisibleTo` → UnitTests. Fix `.sln` paths + the test
  `<ProjectReference>`.
- Stay purely structural — no `[DllImport]`, no native-copy target, no smoke
  test; `Native.cs` stays the shell.

## Commit structure (required)
- The CLAUDE.md §2 fix is its **own standalone commit**, not folded into the
  restructure.
- Do not sweep in the untracked `bindings/.DS_Store` or agent-memory files.

## Verification / DoD
1. `dotnet build` — library on ns2.0 + net8.0 + net10.0, tests on net8.0 +
   net10.0: 0 warnings / 0 errors (confirms default globbing does NOT pull tests
   into the library, so dropping the hack is safe).
2. `dotnet test` — passes on net8.0 + net10.0.
3. `dotnet format --verify-no-changes` clean.

## Implemented by
Commits `c985e66` (restructure), `152d50c` (**separate** CLAUDE.md §2 fix),
`cd6725b` (COMMENTS.DONE.1.md record) on branch
`prashah_dev_dotnet_claude_rules_bootstrap`. Reviewed CLEAN by dotnet-critic
(N=1) — see `bindings/dotnet/COMMENTS.1.md` "Restructure pass (A → B)".
