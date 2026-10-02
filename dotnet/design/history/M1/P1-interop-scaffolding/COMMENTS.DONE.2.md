# COMMENTS.DONE.2 — M1/P1 "Interop scaffolding + native-load probe" (closed record)

Durable archived review record for the .NET binding's Milestone 1 / Phase 1.
Actor N=2 (`dotnet-actor`), Critic N=2 (`dotnet-critic`), Manager = root
`project-manager`. Per `bindings/dotnet/CLAUDE.md §8.4`, this file records the
**decisions and deviations made during execution** plus the review outcome. The
forward-looking plan is `PLAN.md` in this same directory.

## Review outcome

**Critic N=2 review of commits `9441a4c`, `149e327`, `9423fa4`: 0 genuine
findings — CLEAN.** No fix cycle was required (one Actor pass → one Critic pass →
close). `COMMENTS.2.md` had no open items at any point.

The Critic verified all 8 `[DllImport]` `EntryPoint`s char-by-char against
`target/include/confluent_kafka.h` (and `src/ffi/common.rs` / `src/ffi/consumer.rs`):
byte-exact ABI symbols, `Cdecl` on every declaration, the ffi §0.1 type map
correct (`int32_t`→`int`; `const char*`→`IntPtr`, never a freed `string` return;
opaque `*_t`→`IntPtr`; `[return: MarshalAs(UnmanagedType.I1)]` on **both**
`IsRetriable` and `IsFatal` — the 1-byte-vs-4-byte-BOOL bug avoided), no
`[LibraryImport]`/`LPStr`/`LPUTF8Str`/`PtrToStringUTF8`, uniform across TFMs (no
`#if`). `Utf8.Pin` = encode + trailing NUL + pinned `GCHandle` + unpin in
`Dispose` (call-scoped `using`); `Utf8.PtrToString` = NUL-scan + `GetString`,
`null` for `IntPtr.Zero`, NUL-terminated form only (length-delimited deferred);
`unsafe` quarantined to `Internal/Interop/`. csproj native-copy target: per-OS
filename via `IsOSPlatform`, profile from `$(Configuration)`, repo root 4 levels
up, `<Content>` (transitive to test output), `Link` preserves `lib`-prefix/OS-suffix,
no hardcoded path/filename, no NuGet/`runtimes/`. Tests genuinely exercise the
boundary (real round-trip, real `Utf8.Pin` into native, non-ASCII incl. a 4-byte
char at the buffer boundary, `PtrToString(Zero)==null`). Scope respected;
Apache-2.0 header on all 3 new `.cs`; no TODO/FIXME.

## DoD verification (both Actor and Critic ran independently — all green)

- `cargo build --features ffi` — cdylib `target/debug/libconfluent_kafka.dylib`
  + header `target/include/confluent_kafka.h` produced (run FIRST).
- `dotnet build` — **0 warnings / 0 errors** across library TFMs
  (`netstandard2.0;net8.0;net10.0`) + test TFMs (`net8.0;net10.0`),
  `TreatWarningsAsErrors` active.
- `dotnet test -f net10.0` — **4 passed** (3 probe tests + the M0/P0 sentinel).
- `dotnet format --verify-no-changes` — clean.
- ⚠️ CI-only: only the .NET 10 runtime is installed on this machine — the net8.0
  test *run* and net462 are deferred to CI; their *build* legs pass.

## Decisions in force (from the approved plan)

- **D1 (un-defers M0/P0 D2)** — native-copy MSBuild target landed; per-OS
  filename computed via MSBuild, profile from `$(Configuration)`, repo root 4
  levels up, `<Content>` (transitive), never a hardcoded path/filename.
- **D2** — `<AllowUnsafeBlocks>` on the LIBRARY csproj only; test project stays
  unsafe-free (`unsafe` confined to `Internal/Interop/`).
- **D3** — classic `[DllImport]`, uniform across all TFMs (netstandard2.0 floor
  forbids `[LibraryImport]`/`PtrToStringUTF8`/`LPUTF8Str`).
- **D4** — `Utf8.Pin` is a disposable call-scoped pin (`using`); `PtrToString`
  is NUL-terminated form only (length-delimited receive-path form deferred to a
  later phase).
- **D5** — analyzer suppressions contingent: **none were needed.** A clean
  `--no-incremental` build reported 0 diagnostics with `/unsafe+` active. CA5392
  is opt-in (not in the default set); SYSLIB1054 is Info-severity (not elevated
  by `TreatWarningsAsErrors`). Per the contingent rule, **no suppressions were
  added** (adding them would have been unjustified).

## Deviations recorded during execution

Two minor, both cosmetic — recorded by the Actor:

1. **XML-comment MSB4025 fix (not a scope change).** The csproj native-copy
   comment initially embedded `cargo build --features ffi`; the literal `--` is
   illegal inside an XML comment (MSB4025). Reworded the comment to avoid the
   double-dash. No behavioral change.
2. **`.gitkeep` deletion grouped into the csproj commit** (`9441a4c`) rather than
   the Native/Utf8 commit, because `git rm` pre-staged
   `Internal/Interop/.gitkeep` before the first commit. Purely commit-grouping;
   the resulting tree is correct.

## Watch-item for future phases (NOT a finding — cannot manifest under M1/P1)

The Critic flagged, and deliberately did **not** file as a finding (correctly,
per the false-positive rule — it cannot occur under this phase's constraints):

- `PinnedUtf8String` is a `readonly struct` holding a `GCHandle`; `Dispose()`
  calls `_handle.Free()`, which operates on a compiler **defensive copy** (a
  non-readonly method invoked on a `readonly` field). Under M1/P1's only usage —
  single `using`-scoped ownership — the pin is released exactly once with no
  leak, so the phase is **correct**. But the "idempotent for a single owner" doc
  claim would break under a manual double-`Dispose()` or a disposed by-value
  copy. **A later phase that stores or copies a `PinnedUtf8String` must revisit
  this** (e.g. make `Dispose` a `readonly` method that no-ops after first free,
  or keep single-owner discipline). Recorded in
  `bindings/dotnet/.claude/agent-memory/dotnet-critic/interop_review_patterns.md`.

## Governance pointers

- Approved plan: `design/history/M1/P1-interop-scaffolding/PLAN.md`.
- Closed review record: this file.
- Active working review file for the next requirement: a fresh
  `bindings/dotnet/COMMENTS.3.md` (N=3; gitignored working file). `COMMENTS.2.md`
  ends this phase with no open items.
