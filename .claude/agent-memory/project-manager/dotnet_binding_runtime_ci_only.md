---
name: dotnet-binding-runtime-ci-only
description: .NET binding local box has only the .NET 10 runtime; net8.0/net462 test RUNS are CI-only, not DoD blockers
metadata:
  type: project
---

On this machine only the **.NET 10 runtime** is installed (`dotnet --list-runtimes`
→ Microsoft.NETCore.App 10.0.x only), though SDK 10 *builds* all library TFMs
(netstandard2.0 / net8.0 / net10.0) fine via reference packs.

**Why:** net8.0 test *runs* need the .NET 8 runtime; net462 needs Windows. Both
build legs succeed; only the *execution* is blocked locally.

**How to apply:** When framing .NET binding DoD gates for future phases, treat
`dotnet test` as green when it passes on **net10.0** (`dotnet test -f net10.0`).
net8.0 and net462 test runs are **CI-only** — do NOT block DoD on them, and tell
the Actor a net8.0 run failing purely with a missing-runtime error ("Framework
'Microsoft.NETCore.App', version '8.0.0' ... not found") is EXPECTED, not a bug
to chase. Re-verify with `dotnet --list-runtimes` before relying on this — the
user may install the .NET 8 runtime later. This is the .NET binding (Milestone/
Phase numbering is binding-local, personas are dotnet-actor / dotnet-critic).
