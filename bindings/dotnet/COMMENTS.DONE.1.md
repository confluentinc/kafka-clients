# Resolved comments / recorded decisions — .NET binding Actor (N = 1)

Resolved review comments and milestone-level decisions/deferrals for the .NET
binding Actor. Active review comments (from the Critic) live in `COMMENTS.1.md`
(git-ignored); resolved items and durable notes land here.

---

## Milestone 1 — project skeleton (bootstrap)

Purely structural project skeleton: folder layout, build config, and the support
matrix. No native interop is exercised (no `[DllImport]`, no `CopyNative.targets`,
no `SafeHandle`/`Utf8`/completion bridge, no managed client API, no native-load
smoke test) — `bindings/dotnet/CLAUDE.md` §1 (shape only, no logic) and the
approved plan's scope.

No Critic comments were raised on this first pass. The decisions and deferred CI
obligations below are recorded so they are not lost.

### D1 — net462 runtime CI leg (deferred, NOT silently skipped)

The support matrix (`ffi-marshalling.md` §0.1/§0.2, aligned with
confluent-kafka-dotnet 2.15.0) is **net462 · netstandard2.0 · net8.0 · net10.0**,
with **net462 satisfied via the `netstandard2.0` asset** — there is deliberately
**no separate `net462` target** (we have no Framework-specific code; §0.1/§0.2).

The net462 **runtime** smoke leg (the binding loads + a Mock round-trip on .NET
Framework 4.6.2 via the ns2.0 asset — CLAUDE.md §7.4) is a **CI obligation**, not
part of this milestone's DoD: it cannot run on macOS (no .NET Framework /
Mono runtime here). It is recorded here so a later CI setup adds a Windows net462
run leg. It does **not** block Milestone 1.

### D2 — net8.0 test run via `RollForward` on this dev machine

Only the **net10** runtime (`Microsoft.NETCore.App 10.0.9`) is installed on the
dev machine; the net8.0 runtime is absent. The unit-test project sets
`<RollForward>Major</RollForward>` so the net8.0 test assembly rolls forward onto
the net10 runtime, letting the TFM-run matrix (net8.0 + net10.0) execute here.
`dotnet test` reports both legs passing (2 tests each).

The faithful **net8.0-on-net8.0-runtime** leg (like D1's net462 leg) is a CI
obligation — CI should install the net8.0 runtime so net8.0 tests run natively
rather than rolled-forward.

### D3 — ABI-boundary decisions recorded now, not coded this milestone

Nothing P/Invokes yet, so these `ffi-marshalling.md` §0.1/§0.2 decisions are
captured only as intent (comments on `Native`, and here); they are coded when the
producer/consumer surfaces land:

- classic `[DllImport(Native.DllName, CallingConvention = CallingConvention.Cdecl)]`
  (the floor is netstandard2.0 → no `[LibraryImport]`/`LPUTF8Str`/function pointers);
- a single `DllName = "confluent_kafka"` for every OS (no NuGet, no per-RID/per-distro
  `Native` variants — the build copies the self-built cdylib to output).

### D4 — TFM / project-shape decisions (approved defaults, applied)

- Library TFMs `netstandard2.0;net8.0;net10.0`; unit-test TFMs `net8.0;net10.0`
  (netstandard2.0 is not runnable). Both TFM sets are defined once in
  `Directory.Build.props` and selected per project.
- Test framework: **xUnit**; the test project is nested under the library project
  per `CLAUDE.md` §2's diagram.
- The library csproj compiles `src/**` only (`EnableDefaultCompileItems=false` +
  explicit `src/**/*.cs`) so the nested `tests/` tree is never globbed into the
  library assembly; `InternalsVisibleTo` grants the test assembly access.
- `AllowUnsafeBlocks=true` is set on the library as forward-setup for the P/Invoke
  boundary; no `unsafe` code exists yet.
- Added `bindings/dotnet/.gitignore` (`bin/`, `obj/`, `TestResults/`, `*.user`):
  the repo-root `.gitignore` ignores `bin` but not `obj`.
