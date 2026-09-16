# M13/P2 (.NET binding): wire the perf gate into `verify-dotnet`

**Status:** APPROVED 2026-08-19. Both open questions resolved by the user: keep D4
(semaphore.yml comment refresh) AND keep the root `Makefile:270-274` delegate-comment
refresh. Refresh all four stale comments so the repo stays truthful.

**Type:** Mode A / test-infra only — NO product code, NO ABI, NO Rust, NO Python.
**Branch:** `prashah_dev_dotnet_performance` (M13/P1 delivered, not pushed).
**Agent number:** binding keeps its own sequence → **N=36** (M13/P1 was N=35).

## Background

M13/P1 delivered the .NET perf suite under `bindings/dotnet/tests/Performance/`:
the `PerfV3` exe (producer/consumer benchmark engine) plus the
`Confluent.Kafka.PerformanceTests` xUnit smoke (`PerfV3SmokeTests` +
`KafkaBrokerFixture`), driven by `make test-integration-perf-dotnet`. The smoke is a
broker-based p99-latency gate (Testcontainers `apache/kafka:4.2.0`, KIP-848; 100 rps /
10 s / p99 ≤ 70 ms budget; PerfV3 exits non-zero if p99 exceeds budget).

**Gap:** the .NET perf suite is NOT wired into CI. CI runs `make verify-dotnet`
(= `test-dotnet` + `test-integration-dotnet`) with no perf stage. Python DOES wire
perf in (`verify-python` appends `test-integration-perf-python`). The comments that
justified verify-dotnet's missing perf stage are STALE — M13/P1 disproved
".NET has no broker-based p99 latency suite."

## Fact-check corrections (verified against repo)

1. `bindings/dotnet/Makefile` `test-integration-perf-dotnet` is at **L134-138** (not
   ~L137-140); the two legs are L137 (`-f net8.0`) and L138 (`-f net10.0`).
2. There is only ONE `test-integration-perf-dotnet` target (root L275-276 delegates
   to it), so making it net10.0-only inherently covers both the manual invocation and
   the verify-wired one — "single behavior" is automatic.
3. No double-run risk: `Confluent.Kafka.sln` holds only `Confluent.Kafka` +
   `Confluent.Kafka.UnitTests` (the Performance project is deliberately kept out), so
   `test-dotnet` does NOT already run the smoke.
4. Side effect: `test-integration-perf-dotnet` runs the whole PerformanceTests project,
   which also contains `Murmur2Test` (pure murmur2-vector unit test, no broker). Wiring
   the target into verify-dotnet additively pulls both `PerfV3SmokeTests` and
   `Murmur2Test` into the gate (net10.0). Harmless.
5. The "no perf stage / no p99 suite" claim is repeated in FOUR places, all now false:
   root `Makefile:306-311`, root `Makefile:270-274`, `bindings/dotnet/Makefile:127-133`,
   `.semaphore/semaphore.yml:170-171`. No behavioral CI change is needed (CI already
   runs `make verify-dotnet` on a Docker-capable runner), but all four comments must be
   refreshed for accuracy.
6. The perf gate will genuinely EXECUTE on CI (not skip): the .NET CI block runs on
   `s1-prod-ubuntu24-04-amd64-2`, and `test-integration-dotnet: build-grpc-images-dotnet`
   already builds Docker images there, so `DockerAvailable()` passes and the
   Testcontainers broker spins. The amplifier flake risk therefore lands in CI too —
   mitigated by net10.0-only (single broker run) + runner headroom.

## Deliverables

### D1 — Perf gate → net10.0 only (`bindings/dotnet/Makefile`, L134-138)
Delete the net8.0 leg (L137) and refresh the L127-133 comment. Rationale: the perf
gate's job is the p99 latency check, not TFM coverage; net8.0 stays covered
functionally by `test-dotnet` (both TFMs); net10.0 is the designated execution gate.
`PerfV3.csproj` still builds for all TFMs; only the smoke execution is net10.0.

### D2 — Wire perf into `verify-dotnet` + refresh comments (root `Makefile`)
Append `$(MAKE) test-integration-perf-dotnet` as a recipe line to `verify-dotnet`
(after the `test-integration-dotnet` line, L312-313), mirroring `verify-python`
(L303-304). Rewrite the stale block comment at L306-311 and the delegate-target comment
at L270-274 (drop "deliberately NOT part of verify-dotnet").

### D3 — Document the deferred amplifier fix (COMMENT ONLY) — `KafkaBrokerFixture.cs`, `ProducePerfInContainerAsync` (L197-210)
Add a comment at the launch site (the `nohup … &` command, L199-206) capturing:
- The in-container load producer is launched detached (`nohup … &`) and never stopped.
- Under the shared single-node broker, a slow/stalled consumer smoke leaves load
  hammering the broker and can cascade the remaining broker smokes into their timeouts.
  Evidence: a first `make test-integration-perf-dotnet` run failed all 4 broker smokes;
  every warm rerun (isolated and full, both TFMs) passed 15/15.
- Deferred fix (fast-follow, ~a dozen lines, test-infra only, additive): capture the
  launched PID on start and `kill` it on each consumer smoke's teardown
  (`RunConsumerSmokeAsync` in `PerfV3SmokeTests.cs`, via try/finally).
- Why deferred: net10.0-only + CI runner headroom make residual flake risk low.
NO PID-tracking / kill code is written this phase.

### D4 — `.semaphore/semaphore.yml` comment refresh (comment-only) — L170-171
Replace "No perf stage (.NET has no p99 suite; Decision 4)" with a line noting
`verify-dotnet` now includes the Docker-gated net10.0 p99 perf stage. NO behavioral CI
change.

### D5 — STATUS touch-up (`bindings/dotnet/design/current/STATUS.md`) — Manager, at handoff
Add an M13/P2 note: perf smoke is now a `verify-dotnet` gate (net10.0-only); amplifier
fix documented-and-deferred.

## Verification / DoD

Local (before push):
- `make verify-dotnet` runs to completion and reaches the perf leg on net10.0; with
  Docker present the Testcontainers broker spins and the smoke asserts exit 0.
- Confirm net10.0-only: no `dotnet test -f net8.0 $(PERF_TESTS)` anywhere in the path.
- `git diff` contains ONLY: root `Makefile`, `bindings/dotnet/Makefile`,
  `KafkaBrokerFixture.cs` (comment-only), `.semaphore/semaphore.yml` (comment-only),
  `STATUS.md`. No product code, ABI, Rust, Python, `.csproj`, or C# logic changes.
- `make test-dotnet` still green (surface unchanged); build/format/lint clean.
- Format-check caveat: `dotnet format $(SOLUTION) --verify-no-changes` does NOT cover
  `KafkaBrokerFixture.cs` (Performance project not in the sln) — hand-verify the comment
  against `.editorconfig`, and run `make test-integration-perf-dotnet` once with Docker
  to confirm the net10.0 smoke is green and the file compiles.

CI (pending push): first pipeline run confirms `verify-dotnet` reaches the perf leg and
the Docker-backed net10.0 smoke runs green on the amd64 runner.

## Out of scope
- net8.0 perf execution.
- The amplifier fix implementation (PID capture + kill) — documented only (D3).
- Python `conftest.py` parity / gate hardening — separate future task.
- Any ABI / binding / product / Rust change; a separate Semaphore perf job.

## Conventions
Per-path `git add` only; NEVER stage `.claude/agents/dotnet-*.md`, `COMMENTS.*`,
`agent-memory/**`, built binaries, `obj/`/`bin/`; commits `--no-gpg-sign` +
`Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`. Commit locally per the Actor
workflow; do NOT push (the user manages pushes).
