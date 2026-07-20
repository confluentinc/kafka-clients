# Milestone 1 · Phase 1 — Project scaffold (bootstrap)

**Status:** executed & reviewed clean. **Layout superseded by Phase 2** (the
folder shape below is layout A; Phase 2 moved it to the top-level `src/` +
`tests/` layout — see `../Phase-2-layout-restructure/PLAN.md`). Everything else
in this plan still holds.

> Backfilled record. This plan was approved and executed before
> `bindings/dotnet/design/` existed; it is captured here per CLAUDE.md §8.4.

## Goal
Bootstrap the .NET binding: a compilable, testable **project skeleton** —
folder structure, build config, and the support matrix — per
`bindings/dotnet/CLAUDE.md` §2.

## Scope — purely structural (no native interop exercised)
**In:** the skeleton that compiles and runs its tests on the support matrix.
**Explicitly out (deferred to the first Mode A feature milestone):**
- No `CopyNative.targets` / native-copy MSBuild target — nothing P/Invokes yet,
  so it would be unverifiable this milestone (added with the first `[DllImport]`).
- No `[DllImport]` declarations, no native-load smoke test.
- No `SafeHandle`s, `Utf8` helpers, completion bridge, or managed
  `KafkaProducer`/`KafkaConsumer`/`MockProducer` API.
- No Kafka logic (bindings/dotnet/CLAUDE.md §1).

## Deliverables (layout A — see Phase 2 for the final layout)
```
bindings/dotnet/
├─ Confluent.Kafka.ShareConsumer.sln
├─ Directory.Build.props            ← TFMs, #nullable enable, LangVersion, analyzers
├─ .editorconfig                    ← dotnet/runtime style (§7.2)
└─ Confluent.Kafka.ShareConsumer/
   ├─ Confluent.Kafka.ShareConsumer.csproj   ← compiled src/** only; AllowUnsafeBlocks; InternalsVisibleTo → UnitTests
   ├─ src/Internal/Interop/Native.cs         ← SHELL only: DllName const, NO [DllImport]
   └─ tests/Confluent.Kafka.ShareConsumer.UnitTests/
      ├─ …UnitTests.csproj
      └─ ScaffoldingTests.cs                  ← structural-only; references internal Native, NO native call
```
`Native.cs` is a shell that anchors `Internal/Interop/` and documents where
`[DllImport]`s land (ffi §0.1) without declaring any.

## Support matrix (ffi §0.1/§0.2, aligned with ckd 2.15.0)
- Library TFMs: `netstandard2.0;net8.0;net10.0` — **net462 satisfied via the
  netstandard2.0 asset**, no separate `net462` target.
- `#nullable enable` project-wide; Apache-2.0 header (Confluent Inc.) on every
  new `.cs`.

## Decision defaults
- xUnit; UnitTests project targets `net8.0;net10.0` (ns2.0 isn't runnable).
- Tests named `…UnitTests` so `…IntegrationTests` can sit alongside later.
- Include `.sln` + `Directory.Build.props`.

## Verification / DoD
1. `dotnet build` — all three TFMs compile.
2. `dotnet test` — structural test passes on net8.0 + net10.0.
3. `dotnet format --verify-no-changes` clean.
No `cargo build --features ffi` dependency (nothing loads native). The net462
runtime leg and the net8.0-native leg are CI carry-forwards, not local blockers.

## Implemented by
Commits `eb6a087` (skeleton), `7cb6699` (unit-test project), `e66a803`
(decisions record), `460d693` (agent memory) on branch
`prashah_dev_dotnet_claude_rules_bootstrap`.

Decisions & deferrals recorded in `bindings/dotnet/COMMENTS.DONE.1.md`.
