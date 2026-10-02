# M10/P1 — "Wire the .NET binding into CI verification" (mirror `verify-python`)

Status: **APPROVED (maintainer/user 2026-08-12, rev 2).** N=26. Executor: `dotnet-actor` (reviewed by `dotnet-critic`).
Branch: `prashah_dev_dotnet_semaphore` (off `prashah_dev_dotnet_binding_consumer`). Do not disturb `prashah_dev_dotnet_binding_consumer` / PR #150.
Rev 2 bakes in the user's confirmed calls on Decisions 1/2/3/5 + the CKD (`confluent-kafka-dotnet`) precedent.

## 0 · Scope framing (READ FIRST — NOT a Mode-A binding change)

CI / build-wiring milestone. It **legitimately edits repo-root infra files** — root `Makefile`, `.semaphore/semaphore.yml`, a new `.semaphore/install-dotnet.sh`, and `bindings/dotnet/Makefile` (+ optionally `bindings/dotnet/tests/**` csproj for Decision 2). The .NET-binding "whole diff stays under `bindings/dotnet/`" invariant **does NOT apply** to M10/P1 — do not flag the root-infra edits as a breach. **Hard line that DOES hold:** **no Rust-core / `src/ffi/**` / `target/include/confluent_kafka.h` change** — we orchestrate builds and run *existing* tests; no feature, no ABI. If the Actor believes it needs such a change, it STOPS and flags the Manager.

## 1 · Objective & context

The .NET binding builds and tests locally but has **no CI gate**. The "Verify language bindings" block (`.semaphore/semaphore.yml:104-124`) runs `verify-c` + `verify-python` on the pipeline's **arm64** agent (`s1-prod-ubuntu24-04-arm64-2`, `:47`); there is **no `verify-dotnet` job**, and `.semaphore/dependencies.sh:8` installs only `cmake`+`rustup` — **no .NET SDK**. M10/P1 adds a `verify-dotnet` gate that mirrors the *shape* of `verify-python` (build → unit → format → `__grpc_dotnet(+_async)` integration), running on an **amd64** agent (Decision 3).

## 2 · Verified findings (file:line, branch `prashah_dev_dotnet_semaphore`)

- **CI block/agent:** `.semaphore/semaphore.yml:104-124` — "Verify language bindings", `dependencies: []`, shared prologue `./.semaphore/dependencies.sh` (`:117`), jobs `verify-c` (`:119-121`) + `verify-python` (`:122-124`). Pipeline agent `s1-prod-ubuntu24-04-arm64-2` (`:45-47`). Global prologue does `checkout` + branch-switch + `sem-version python 3.10` (`:50-69`) for every job.
- **Per-task agent override is supported:** Semaphore sets machine type at the **pipeline `agent`** or per **`task.agent`** level — there is **no `jobs[].agent`**. (This is why Decision 5's job lives in its own block.)
- **Deps installer:** `.semaphore/dependencies.sh:8` — `sudo apt install -y cmake rustup`; submodule init at `:9`. No .NET. Runs before every job in the Rust + bindings blocks.
- **Root Makefile shape to mirror:** `verify-python: test-python` + `$(MAKE) test-integration-perf-python` (`Makefile:248-249`); `test-python: build-python` delegates to `bindings/python`'s `test` (`:240-242`); `test-integration-python: build-grpc-images-python` → `cargo test --features integration-tests,multilanguage-tests --test integration -- __grpc_python` (`:174-175`; the `__grpc_python` filter matches sync **and** async, `:166-169`); `verify-c: test-c` (`:246`); per-binding image builders `build-grpc-images-python` (`:87`) / `build-grpc-images-c` (`:102`). Only `verify-rust` runs `format-check`+`lint` (`:251`); `verify-python`/`verify-c` don't.
- **.NET Makefile today:** only `grpc-image` + `grpc-image-async` (wired into root `build-grpc-images`, `Makefile:82-83`); **no `test`/`verify` target.**
- **TFMs:** library `netstandard2.0;net8.0;net10.0` (`Confluent.Kafka.csproj:16`; net462 "rides ns2.0", comment `:11`); **tests** `net462;net8.0;net10.0` (`Confluent.Kafka.UnitTests.csproj:17`, net462 `ItemGroup` `:36`).
- **Build gate is real:** `Directory.Build.props:16-18` — `EnforceCodeStyleInBuild=true`, `TreatWarningsAsErrors=true`, `AnalysisLevel=latest`. `.editorconfig` present (7.2 KB) → `dotnet format --verify-no-changes` enforces it.
- **Arms exist & pass locally:** `__grpc_dotnet` (11) + `__grpc_dotnet_async` (11); both dotnet images build via `make build-grpc-images`.
- **CKD precedent** (verified, local checkout `/Users/pranavshah/WorkSpace/Confluent/confluent-kafka-dotnet`): `.semaphore/semaphore.yml` — all Linux jobs on `s1-prod-ubuntu24-04-amd64-*` (**no arm64 Linux job**); SDK via `mise use dotnet@8/@10` (Linux) or `dotnet-install.sh --channel 8.0/10.0` (macOS/Windows), **always both 8 and 10**; per-block `task.agent.machine.type` overrides; test execution `-f net10.0`. `src/Directory.Build.props` lib TFMs `net8.0;net10.0` (+ `netstandard2.0;net462` appended per lib csproj); `test/Directory.Build.props` test TFMs `net8.0;net10.0` (**no net462 in tests**).

## 3 · Scope — the five pieces

- **(A) Build gate** — `cargo build --features ffi --release` (native `.so` + header) FIRST, then `dotnet build -c Release` across the TFM matrix, analyzers on, **0 warnings** (`bindings/dotnet/CLAUDE.md §7.1` firm order).
- **(B) Unit tests** (`dotnet test`, no broker) — mock round-trips, error-message-content assertions (DoD §3), handle-lifecycle / double-dispose / UAF regressions, wakeup + concurrent-use shapes, **allocation-budget** tests (DoD §10, .NET's analog of Python's perf suite). Run on **net8.0 AND net10.0** (Decision 2).
- **(C) Integration** (multilanguage harness, real broker) — build the two dotnet images, then `cargo test --features integration-tests,multilanguage-tests --test integration -- __grpc_dotnet` (both `__grpc_dotnet` + `__grpc_dotnet_async`). All amd64-native (Decision 3).
- **(D) Format** — `dotnet format --verify-no-changes` against the checked-in `.editorconfig` (.NET-specific; no Python analog).
- **(E) Wiring** — `verify-dotnet`/`test-dotnet`/`test-integration-dotnet`/`build-grpc-images-dotnet` Make targets (root + `bindings/dotnet`, mirroring the python/c split); a **new amd64 CI block** carrying one `verify-dotnet` job; job-scoped .NET SDK provisioning.

## 4 · Design decisions (resolved with the user — LOCKED, do not re-litigate)

**Decision 1 — SDK provisioning (LOCKED: `dotnet-install.sh --channel 10.0`, over `mise`).**
A job-scoped **`.semaphore/install-dotnet.sh`** (NOT the shared `dependencies.sh` — the other jobs don't need .NET) does **two** installs into one `DOTNET_ROOT` (`$HOME/.dotnet`):
```bash
dotnet-install.sh --channel 10.0                 # SDK 10: builds all TFMs + RUNS net10.0 tests
dotnet-install.sh --channel 8.0 --runtime dotnet # .NET 8 base runtime: to RUN net8.0 tests
export DOTNET_ROOT="$HOME/.dotnet"; export PATH="$DOTNET_ROOT:$PATH"; export DOTNET_MULTILEVEL_LOOKUP=0
```
**Why the second install is mandatory (the Decision-1 ↔ Decision-2 interaction):** `--channel 10.0` installs only the net10 SDK+runtime. A net8.0 target *builds* fine under SDK 10, but **`dotnet test -f net8.0` needs the .NET 8 runtime present** — a net8.0 test assembly does not roll-forward to the net10 runtime by default. So both are required. **Lean runtime-only for net8** (`--runtime dotnet` — the base `Microsoft.NETCore.App`, all a net8.0 *unit* test needs; the ASP.NET Core runtime is only used inside the gRPC Docker images, which bundle their own `aspnet:8.0`). *(CKD installs both as full SDKs; the lean form is a smaller, faster CI install and sufficient here — deliberate, not an omission.)* `DOTNET_MULTILEVEL_LOOKUP=0` + a single `DOTNET_ROOT` keeps resolution deterministic (both installs coexist in `$HOME/.dotnet`), mirroring CKD's macOS block.

**Decision 2 — net462 build-verify only; RUN unit tests on net8.0 AND net10.0 (LOCKED).**
Honest mechanic: our **library has no explicit net462 target** — it covers net462 consumers via its `netstandard2.0` asset (lib TFMs `netstandard2.0;net8.0;net10.0`). So "net462 build-verify" is specifically the **test project's** net462 leg (`Confluent.Kafka.UnitTests.csproj` TFMs `net462;net8.0;net10.0`). net462 **cannot RUN on Linux**. Resolution:
- **(a, preferred) build-verify the test net462 leg on the amd64 job** via `Microsoft.NETFramework.ReferenceAssemblies` (a cross-platform reference-only compile that checks the net462 API surface); RUN tests on net8.0 + net10.0 only.
- **(b, documented fallback) if the net462 leg fights the Linux toolchain, defer it** — build only `netstandard2.0`/`net8.0`/`net10.0` on this job and defer net462 runtime *and* build to a future Windows runner.
The Actor confirms whether `Microsoft.NETFramework.ReferenceAssemblies` is already referenced (test csproj `:36`); adds it for (a), or falls back to (b) and documents why (Actor's call). **CKD precedent:** CKD does NOT run net462 tests at all (its test projects are `net8.0;net10.0`; CI runs `-f net10.0`; net462 exists only as a *library* target compiled/packed on Windows). So our **net8.0 + net10.0 test run is already ≥ CKD's coverage** — net462 build-verify is a bonus, and deferring it (b) still meets/exceeds CKD.

**Decision 3 — amd64 agent (LOCKED — SUPERSEDES the arm64/QEMU contingency).**
`verify-dotnet` runs on **`s1-prod-ubuntu24-04-amd64-2`**, following CKD (all its Linux jobs are `s1-prod-ubuntu24-04-amd64-*`; no arm64 Linux job). This **dissolves the arm64 `Grpc.Tools` protoc risk entirely**: the native `.so`, the two gRPC images, and the testcontainers broker all build/run **amd64-native** — no QEMU, no buildx, no emulation close-timeout flake. The prior "try-native-arm64-then-fall-back-to-QEMU" contingency is **dropped**, replaced by "pin the job to amd64." Rationale for the mechanism: Semaphore sets machine type per **pipeline/task**, not per **job** — hence Decision 5.

**Decision 4 — `verify-dotnet` contents (LOCKED, UNCHANGED).** `verify-dotnet` = **build + format + unit + integration**. **No broker-based performance stage** — .NET has no p99 latency suite; its allocation-budget assertions live in the **unit** suite (B). This is the one shape difference from `verify-python` (which appends `test-integration-perf-python`, `Makefile:249`); comment the asymmetry in the target so it reads as intentional.

**Decision 5 — job structure: one job in its OWN amd64 block (LOCKED, refined).**
Because machine type is per-task, `verify-dotnet` is a **single job in a new block** `"Verify .NET binding"` that carries `task.agent.machine.type: s1-prod-ubuntu24-04-amd64-2`, `dependencies: []` (parallel). It does **not** join the existing arm64 "Verify language bindings" block (which stays arm64 with verify-c/verify-python **untouched**). This preserves Decision 5's substance (one job, no unit/integration split); the own-block is purely the vehicle for the amd64 override. Sketch:
```yaml
  - name: "Verify .NET binding"
    dependencies: []
    task:
      agent:
        machine:
          type: s1-prod-ubuntu24-04-amd64-2
      prologue:
        commands:
          - ./.semaphore/dependencies.sh       # cmake + rustup → native .so + cargo integration harness
          - ./.semaphore/install-dotnet.sh     # SDK 10 + net8 runtime (Decision 1)
      jobs:
        - name: "verify-dotnet"
          commands:
            - make verify-dotnet
```
(The global prologue — `checkout`, branch-switch, `sem-version python 3.10` — still runs first for this block's job, harmless on amd64.)

## 5 · Deliverables (exact)

- **`bindings/dotnet/Makefile`** — add `build-dotnet` (native-first, then `dotnet build -c Release` matrix), `test-dotnet` (`dotnet build` + `dotnet format --verify-no-changes` + `dotnet test -f net8.0` + `dotnet test -f net10.0`); reuse the existing `grpc-image`/`grpc-image-async`. Keep `RUST_PROJECT_ROOT` threading consistent with python/c. **Do NOT hardcode `--platform` on the image targets** — keep them platform-agnostic (CI is amd64-native; local Apple-Silicon dev sets `DOCKER_DEFAULT_PLATFORM=linux/amd64` in its own environment).
- **Root `Makefile`** — add, mirroring the python/c split: `build-grpc-images-dotnet: build-rust-all-features` (the two dotnet images), `test-integration-dotnet: build-grpc-images-dotnet` → `cargo test --features integration-tests,multilanguage-tests --test integration -- __grpc_dotnet`, `test-dotnet` (delegates to `bindings/dotnet`), and `verify-dotnet: test-dotnet` + `$(MAKE) test-integration-dotnet` (no perf line; comment the asymmetry per Decision 4).
- **`.semaphore/semaphore.yml`** — **add a NEW block `"Verify .NET binding"`** with `task.agent.machine.type: s1-prod-ubuntu24-04-amd64-2`, `dependencies: []`, the two-command prologue, and one `verify-dotnet` job (§4 sketch). **Do NOT** add a job to the existing arm64 block (Decision 3/5).
- **`.semaphore/install-dotnet.sh`** (new, job-scoped, executable) — `dotnet-install.sh --channel 10.0` (SDK) + `dotnet-install.sh --channel 8.0 --runtime dotnet` (net8 runtime) + `DOTNET_ROOT`/`PATH`/`DOTNET_MULTILEVEL_LOOKUP=0` export (Decision 1).
- *(Decision 2a)* **`bindings/dotnet/tests/Confluent.Kafka.UnitTests.csproj`** — `Microsoft.NETFramework.ReferenceAssemblies` for the net462 build, if not already present; else fall back to 2b and document.

## 6 · Definition of Done

- `make verify-dotnet` green on the new amd64 CI block: native+`dotnet build` 0W/0E across the TFM matrix (net462 per Decision 2); `dotnet format --verify-no-changes` clean; `dotnet test` green on net8.0 **and** net10.0; both dotnet images build amd64-native; `__grpc_dotnet` + `__grpc_dotnet_async` arms green against a real broker. (Local `make verify-dotnet` is authoritative-green ONLY if the net8 runtime is actually installed locally — see §7; otherwise the net8 leg is CI-verified-only and MUST be reported as such.)
- The new `"Verify .NET binding"` block runs green in parallel with the arm64 blocks; a failure is attributable to the .NET binding.
- **No Rust-core / `src/ffi/**` / `confluent_kafka.h` change** — `git diff --stat` touches only root `Makefile`, `.semaphore/**`, `bindings/dotnet/Makefile` (+ optional `bindings/dotnet/tests/**` csproj). Cross-cutting root-infra scope is expected (§0), not a breach.
- `verify-c` / `verify-python` / `verify-rust` and the arm64 block are **untouched** and green; `.semaphore/dependencies.sh` unchanged (SDK install is job-scoped).
- net462 disposition (2a or 2b) and the amd64 rationale are documented in the closed record and, where load-bearing, in a Makefile/YAML comment.

## 7 · Validation steps

1. Local: `make verify-dotnet` end-to-end (Docker up, `DOCKER_DEFAULT_PLATFORM=linux/amd64` for the emulated image build) — build/format/unit(net8+net10)/integration. **The net8 leg requires the net8 runtime installed locally** (`dotnet-install.sh --channel 8.0 --runtime dotnet`); if not installed, run net10 locally and mark the net8 leg CI-verified-only — never report a false local-green.
2. Confirm `install-dotnet.sh` on a clean amd64 Linux context resolves SDK 10 + net8 runtime (no NETSDK1045; `dotnet --list-sdks`/`--list-runtimes` show both), and that `dotnet test -f net8.0` actually runs.
3. Confirm Decision 2: the net462 test leg either build-verifies (2a) or is cleanly deferred (2b) with net8/net10 running.
4. Push to `prashah_dev_dotnet_semaphore`; confirm the new amd64 `"Verify .NET binding"` block is scheduled `dependencies: []`, provisions the SDK, goes green, and the arm64 blocks (verify-c/python/rust) are unaffected. **CI amd64 is authoritative**; a local emulated close-timeout flake is not a code defect.
5. `git diff --stat` — assert zero churn to `src/**`, `src/ffi/**`, `target/include/confluent_kafka.h`.

## 8 · Governance / handoff (Manager, N=26)

Executor **`dotnet-actor N=26`** (needs .NET SDK/TFM/`dotnet format` + CI-YAML/Makefile authoring); reviewer **`dotnet-critic N=26`** reviews the **root-infra edits for CI correctness** (amd64 agent override, SDK 10 + net8-runtime pinning, block placement, target wiring, the no-Rust-core line) in addition to the §7 verify contract — an explicitly cross-cutting review, not just C#-against-the-ABI. Per-path `git add` with the guard (never the root `.claude/agents/dotnet-*.md` discovery copies, `COMMENTS.*26.md`, agent-memory, `target-linux*`, staged `.so`/`.dylib`); commits `--no-gpg-sign` + `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`. On close: archive `COMMENTS.DONE.26.md` under `design/history/M10/P1-ci-verify-dotnet/`, update `design/current/STATUS.md` (M10/P1 DONE), reset `COMMENTS.26.md`.
