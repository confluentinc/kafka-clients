# .NET binding — current status

Living status for the .NET binding (`bindings/dotnet/`). Update as milestones
land. Mirrors the repo-root `design/current/` convention (CLAUDE.md §8.4).

## Where things stand
- **Milestone 1 — project bootstrap: DONE.** A purely-structural, compilable
  project skeleton exists in the idiomatic top-level `src/` + `tests/` layout,
  builds on `netstandard2.0`/`net8.0`/`net10.0`, and passes its structural unit
  tests on net8.0 + net10.0. `dotnet format` is clean.
  - Phase 1 — scaffold (layout A). See `../history/Milestone-1-project-bootstrap/Phase-1-scaffold/PLAN.md`.
  - Phase 2 — restructure to layout B + CLAUDE.md §2 fix. See
    `../history/Milestone-1-project-bootstrap/Phase-2-layout-restructure/PLAN.md`.

## Current shape (purely structural — no native interop yet)
```
bindings/dotnet/
├─ Confluent.Kafka.ShareConsumer.sln · Directory.Build.props · .editorconfig · .gitignore
├─ src/Confluent.Kafka.ShareConsumer/          ← library (Native.cs is a shell — no [DllImport])
└─ tests/Confluent.Kafka.ShareConsumer.UnitTests/
```

## Branch
`prashah_dev_dotnet_claude_rules_bootstrap`.

## Decisions & deferrals
Tracked in `bindings/dotnet/COMMENTS.DONE.1.md` (support matrix, deferred CI
legs, ABI-boundary intent). Not duplicated here.

## Next up (first Mode A feature milestone)
The interop scaffolding, in the order the dotnet-actor persona prescribes:
`Native` P/Invoke declarations → `SafeHandle`s → the completion bridge
(pump/dispatcher) → `Utf8` helpers → the first managed API surface
(`MockProducer` round-trip). This is where `CopyNative.targets`, real
`[DllImport]`s, and the native-load smoke test land (they were correctly
deferred out of Milestone 1). See `bindings/dotnet/CLAUDE.md` §6.2 (Mode A) and
`.claude/rules/ffi-marshalling.md`.

## CI carry-forwards (from COMMENTS.DONE.1.md)
- A Windows **net462 runtime** smoke leg (can't run on macOS).
- A native **net8.0 runtime** in CI so the net8.0 test leg runs natively instead
  of rolling forward onto net10.
