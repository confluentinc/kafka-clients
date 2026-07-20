# .NET binding — Milestone 0 / Phase 0: "Project scaffolding"

Status: APPROVED (plan pre-approved by user; execution began 2026-07-20)

The .NET binding keeps its own milestone/phase numbering, independent of the
root Rust `design/`. This is Milestone 0 / Phase 0.

## Scope

**Pure structural skeleton.** Stand up the `bindings/dotnet/` project layout +
support matrix. NO `[DllImport]`s, NO `SafeHandle`s, NO managed API, NO Kafka
logic. Projects must restore, build across the TFM matrix, and give `dotnet test`
a green harness. All shape/behavior lands in later phases.

## Deliverables

### Root `bindings/dotnet/`
- `Confluent.Kafka.ShareConsumer.sln` referencing both projects.
- `Directory.Build.props` — shared: `#nullable enable`, `LangVersion=latest`,
  `<EnforceCodeStyleInBuild>true</EnforceCodeStyleInBuild>`,
  `TreatWarningsAsErrors`, common metadata.
- `.editorconfig` — dotnet/runtime style (`_camelCase`/`s_` fields, PascalCase,
  Allman, `System.*` usings first, CA1715 I-prefix), per CLAUDE.md §7.2.
- `.gitignore` — `bin/`, `obj/`, `.DS_Store`, etc.

### Library `src/Confluent.Kafka.ShareConsumer/`
- `Confluent.Kafka.ShareConsumer.csproj` —
  `<TargetFrameworks>netstandard2.0;net8.0;net10.0</TargetFrameworks>` (net462
  satisfied via netstandard2.0 per ffi-marshalling §0.1), `System.Memory` package
  ref on the netstandard2.0 leg, `InternalsVisibleTo` the UnitTests assembly.
- `Internal/` and `Internal/Interop/` folders present via `.gitkeep` — NO types
  yet.

### Tests `tests/Confluent.Kafka.ShareConsumer.UnitTests/`
- `Confluent.Kafka.ShareConsumer.UnitTests.csproj` — xUnit +
  `Microsoft.NET.Test.Sdk`, `ProjectReference` to the library,
  `<TargetFrameworks>net8.0;net10.0</TargetFrameworks>`.
- One trivial TFM-sentinel smoke test so `dotnet test` is green (proves harness +
  `InternalsVisibleTo`).

## Approved decisions

Actor deviates only with a recorded rationale in PLAN/COMMENTS.DONE/code comment,
per CLAUDE.md §4.

- **D1** — Library TFMs `netstandard2.0;net8.0;net10.0` (net462 via ns2.0).
  ffi-marshalling §0.1 default.
- **D2** — Native-copy MSBuild target (ffi-marshalling §0.2 / CLAUDE.md §7.1) is
  **DEFERRED to the first implementation phase**. A skeleton has nothing to
  P/Invoke, so this phase needs NO Rust build (no `cargo build --features ffi`,
  no ABI dependency).
- **D3** — Empty folders via `.gitkeep`, NO placeholder types (purely
  structural).
- **D4** — Test framework = xUnit.

## Verification (skeleton-scoped)

- `dotnet restore` + `dotnet build` green for all library TFMs (SDK 10 on this
  box builds ns2.0/net8.0/net10.0 via reference packs).
- `dotnet test` green on **net10.0** locally; `dotnet format --verify-no-changes`
  clean.
- ⚠️ Local-runtime limit: **only the .NET 10 runtime is installed** on this
  machine (confirmed: `dotnet --list-runtimes` shows only Microsoft.NETCore.App
  10.0.9). Running **net8.0** tests needs the .NET 8 runtime and **net462** needs
  Windows — both are **CI-only** for this phase. Do NOT block DoD on them; note
  them as CI-only. If the net8.0 test *run* fails purely due to a missing .NET 8
  runtime (not a code/build issue), that is acceptable and expected — record it,
  don't chase it.

## Governance (binding-local)

- Review comments: `bindings/dotnet/COMMENTS.1.md`; resolved →
  `bindings/dotnet/COMMENTS.DONE.1.md`.
- Plans & design: `bindings/dotnet/design/` (NOT the repo-root `design/`).
- Agent memory: `bindings/dotnet/.claude/agent-memory/dotnet-{actor,critic}/`.
- Personas: `dotnet-actor` (Actor N=1), `dotnet-critic` (Critic N=1). NEVER the
  Rust `actor-executor` / `kafka-critic`.
