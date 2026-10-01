# Critic review — COMMENTS.1.md (N=1)

## Milestone 0 / Phase 0 — "Project scaffolding"

Commits reviewed: `f1fb7fc`, `f93a4ef`, `c910fca` (branch
`prashah_dev_dotnet_claude_rules_bootstrap_verify`).
Scope contract: `bindings/dotnet/design/history/M0/P0-scaffolding/PLAN.md`.

### Result: NO ISSUES FOUND — no open items.

This is a pure structural skeleton and it matches the contract. Every plan
deliverable and every in-scope rulebook requirement is satisfied. Verified,
not just eyeballed:

- **Build** — `dotnet build` green across all library TFMs
  (`netstandard2.0`, `net8.0`, `net10.0`) and both test TFMs: **0 warnings,
  0 errors** (with `TreatWarningsAsErrors=true` + `EnforceCodeStyleInBuild`).
- **Test** — `dotnet test -f net10.0`: 1 passed, 0 failed (TFM sentinel).
- **Format** — `dotnet format --verify-no-changes`: clean (0 of 11 files).

Checked against CLAUDE.md §2/§4/§7 and ffi-marshalling.md §0.1:

- Layout — top-level `src/` + `tests/` siblings under `bindings/dotnet/`, one
  folder per project, solution + shared config at the root (CLAUDE.md §2). ✓
- Library TFMs `netstandard2.0;net8.0;net10.0` (net462 via ns2.0, no explicit
  net462 target); tests `net8.0;net10.0` (§0.1 matrix, PLAN D1). ✓
- `System.Memory` 4.5.5 conditioned to the `netstandard2.0` leg only. ✓
- `InternalsVisibleTo="Confluent.Kafka.ShareConsumer.UnitTests"` matches the
  test `AssemblyName` exactly; IVT attribute confirmed emitted in generated
  `AssemblyInfo.cs`. ✓
- Namespace / assembly / package id = `Confluent.Kafka.ShareConsumer`
  (CLAUDE.md §4 D). ✓
- `Directory.Build.props` — `<Nullable>enable</Nullable>`,
  `LangVersion=latest`, `EnforceCodeStyleInBuild=true`,
  `TreatWarningsAsErrors=true` all present (PLAN). ✓
- `.editorconfig` — dotnet/runtime style: `_camelCase`/`s_` fields, PascalCase,
  Allman, `System.*` usings first, CA1715 I-prefix (CLAUDE.md §7.2). ✓
- `Internal/` and `Internal/Interop/` present via `.gitkeep`, **no types**
  (PLAN D3). ✓
- No scope creep — no `[DllImport]`, no `Native`, no `SafeHandle`, no managed
  API, no Rust build (all correctly deferred: PLAN D2/D3). ✓
- `.gitignore` ignores `bin/`/`obj/`/`.DS_Store` etc. and does not ignore
  source. ✓

Absent-by-design interop / marshalling / handle code is **not** a finding for
this phase (the whole point of M0/P0 is that none exists yet).
