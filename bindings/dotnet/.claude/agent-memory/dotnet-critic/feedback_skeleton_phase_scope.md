---
name: skeleton-phase-scope
description: For a scaffolding/skeleton phase, absent interop/handle/marshalling code is by-design, not a finding — check the phase PLAN scope before flagging "missing" FFI code
metadata:
  type: feedback
---

For a **structural-skeleton / scaffolding phase** of the .NET binding (e.g.
Milestone 0 / Phase 0), do NOT report absent P/Invoke, `Native`, `SafeHandle`,
marshalling, managed API, native-copy MSBuild target, or Rust build as
findings. Their absence is the *point* of the phase.

**Why:** My prime directive is avoiding false positives. The reflex to check
the ffi-marshalling.md anti-patterns (handle lifetime, pinning, callback
safety) fires on every review, but for a skeleton there is no boundary code to
audit yet — flagging its absence is a guaranteed false positive and exactly the
noise the Critic role is told to avoid.

**How to apply:** Read the phase `PLAN.md` (under
`bindings/dotnet/design/history/<M>/<P>/`) *first* and treat its Scope +
"Approved decisions" (Dn) as the contract. Deferrals recorded there (e.g. "D2 —
native-copy target DEFERRED", "D3 — empty folders via `.gitkeep`, no types")
are legitimate; only report deviations *from that contract*. For a skeleton the
real findings are structural: TFMs, `System.Memory` conditioning,
`InternalsVisibleTo` name match, namespace/assembly id, `Directory.Build.props`
props, `.editorconfig` style, `src/`+`tests/` layout, no placeholder type
smuggled into `Internal/`, no scope-creep interop code. Also verify by running
(not just reading): `dotnet build` (all TFMs), `dotnet test`, and
`dotnet format --verify-no-changes` — the DoD gate surfaces warning-as-error
breaks, bad package refs, and IVT failures a visual pass misses.
