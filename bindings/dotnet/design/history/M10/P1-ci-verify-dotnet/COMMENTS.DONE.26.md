# COMMENTS.DONE.26 — M10/P1 "Wire the .NET binding into CI verification"

Local working record of decisions/deviations made during execution (first pass;
no Critic comments yet). Not committed (CLAUDE.md §8.4); the Manager archives a
copy under `design/history/M10/P1-ci-verify-dotnet/` on close.

Plan: `design/history/M10/P1-ci-verify-dotnet/PLAN.md` (APPROVED rev 2).
Branch: `prashah_dev_dotnet_semaphore`.

## Deliverables landed

Commits (this pass):
- `01a6b2c8` dotnet(M10/P1): Make wiring for verify-dotnet
- `701ab53a` dotnet(M10/P1): CI gate — new amd64 block + SDK install

1. **`bindings/dotnet/Makefile`** — `build-dotnet` (native `cargo build
   --features ffi --release` FIRST via `--manifest-path`, then `dotnet build -c
   Release` on the solution across the TFM matrix); `test-dotnet: build-dotnet` →
   `dotnet format --verify-no-changes` → `dotnet test -f net8.0` → `dotnet test -f
   net10.0`. Image targets `grpc-image`/`grpc-image-async` reused unchanged,
   platform-agnostic (no baked `--platform`).
2. **Root `Makefile`** — `build-grpc-images-dotnet: build-rust-all-features` (two
   images); `test-integration-dotnet: build-grpc-images-dotnet` → `cargo test
   --features integration-tests,multilanguage-tests --test integration --
   __grpc_dotnet`; `test-dotnet` (delegates to bindings/dotnet); `verify-dotnet:
   test-dotnet` + recipe `$(MAKE) test-integration-dotnet` (NO perf stage;
   asymmetry vs verify-python commented). `.PHONY` updated.
3. **`.semaphore/semaphore.yml`** — new block `"Verify .NET binding"`,
   `dependencies: []`, `task.agent.machine.type: s1-prod-ubuntu24-04-amd64-2`,
   prologue `dependencies.sh` then `install-dotnet.sh`, one `verify-dotnet` job →
   `make verify-dotnet`. Block-layout comment updated. Arm64 "Verify language
   bindings" block + `dependencies.sh` untouched.
4. **`.semaphore/install-dotnet.sh`** — new, executable (`100755`), job-scoped.
   `dotnet-install.sh --channel 10.0` (SDK) + `--channel 8.0 --runtime dotnet`
   (lean net8 runtime); exports `DOTNET_ROOT`/`PATH`/`DOTNET_MULTILEVEL_LOOKUP=0`
   and persists to the login profile.

## Decision 2 disposition — net462 = 2a (NO csproj edit needed)

**2a, already in place.** `Microsoft.NETFramework.ReferenceAssemblies` (v1.0.3,
build-only, `PrivateAssets=all`) is ALREADY referenced in
`tests/Confluent.Kafka.UnitTests/Confluent.Kafka.UnitTests.csproj:36-41` (landed
in an earlier phase, M0/P0-era). So the net462 leg build-verifies cross-platform
with no new edit — confirmed locally: `dotnet build -c Release` on macOS/arm64
emitted `.../net462/Confluent.Kafka.UnitTests.dll` with 0W/0E. Tests RUN on
net8.0 + net10.0 only (net462 can't run on Linux). This meets/exceeds the CKD
precedent (CKD does not run net462 tests at all). No fallback to 2b was needed;
the csproj was NOT modified.

## Env-persistence mechanism in install-dotnet.sh (implementation detail) — SUPERSEDED by Issue 1 (see below)

⚠ **This original approach was wrong — see "## Issue 1 — RESOLVED" below.** The
profile-append is a no-op under Semaphore's single-session model. Kept here for
the execution record; the shipped mechanism is the top-level prologue exports.

The plan sketch runs the prologue command `./.semaphore/install-dotnet.sh`
(executed, not sourced). An executed script's `export`s do not survive into the
next Semaphore command (the script runs in its own subshell), so the script also
appends the exports to `~/.bash_profile` (and `~/.bashrc`) — ~~the Semaphore-
idiomatic way to carry env across a job's commands~~. Kept the in-script `export`s
too (harmless, and correct if a future prologue sources the script) and end with
`dotnet --info` / `--list-sdks` / `--list-runtimes` for in-job validation. This
keeps the prologue exactly as the plan sketches while ensuring `make
verify-dotnet` resolves `dotnet`.

## Solution-scoped commands (not directory-scoped)

`build-dotnet`/`test-dotnet` target `Confluent.Kafka.sln` explicitly. The `.sln`
contains only `Confluent.Kafka` + `Confluent.Kafka.UnitTests`; the `grpc-server`
project is deliberately NOT in the solution (it needs protoc — the arm64 protoc
segfault — and is only ever built inside the gRPC Docker images). Targeting the
solution guarantees `dotnet build`/`format`/`test` never pull grpc-server in.

## RUSTFLAGS note (accepted, not a defect)

`build-dotnet` runs bare `cargo build --features ffi --release` (no
`RUSTFLAGS_NATIVE`), while `build-grpc-images-dotnet` → `build-rust-all-features`
uses `RUSTFLAGS_NATIVE`. On amd64 CI these differ (`-C target-cpu=x86-64-v3` vs
bare) and on the same job cause one extra native recompile between the unit and
integration stages — a CI-time cost only, functionally correct. `--features ffi`
is the minimal correct build for the binding and matches the plan's wording
verbatim; left as-is.

## Local verification (this box, macOS arm64)

- Installed SDK 10 (`10.0.400`) + net8 base runtime (`8.0.30`) + net10 runtime
  into `~/.dotnet` via the same two `dotnet-install.sh` invocations the CI script
  uses, then ran with `DOTNET_ROOT=~/.dotnet`, PATH prepended,
  `DOTNET_MULTILEVEL_LOOKUP=0` — a faithful local mirror of `install-dotnet.sh`.
- `make -C bindings/dotnet build-dotnet`: native `--features ffi --release` OK;
  `dotnet build -c Release` full matrix (lib ns2.0/net8/net10 + tests
  net462/net8/net10) → **0 Warning, 0 Error**.
- `make -C bindings/dotnet test-dotnet`: `dotnet format --verify-no-changes`
  clean; `dotnet test -f net8.0` → **421 passed**; `dotnet test -f net10.0` →
  **421 passed**. BOTH legs actually RAN locally (net8 runtime installed) — not
  CI-only.
- Integration arms (`__grpc_dotnet` + `__grpc_dotnet_async`) run under emulation
  (`DOCKER_DEFAULT_PLATFORM=linux/amd64`); CI amd64 is authoritative. See report
  for the emulated-run outcome.

## Hard invariant

`git diff --stat fc281e21..HEAD` touches only `.semaphore/install-dotnet.sh`,
`.semaphore/semaphore.yml`, `Makefile`, `bindings/dotnet/Makefile`. No `src/**`,
no `src/ffi/**`, no `target/include/confluent_kafka.h`. Confirmed.

---

## Issue 1 — RESOLVED [HIGH · CI-correctness · confirmed] — SDK env now persists to the job session

**Was:** `install-dotnet.sh` set `DOTNET_ROOT`/`PATH`/`DOTNET_MULTILEVEL_LOOKUP`
via in-script `export`s + a `~/.bash_profile`/`~/.bashrc` append. Both are dead
under Semaphore's single-session model: the prologue *executes* the script
(`- ./.semaphore/install-dotnet.sh`), so its exports run in a child subshell and
die on return; and Semaphore runs a job's prologue + job `commands:` in ONE
persistent shell session that sources a profile (if at all) once at session
start — before the append — and never re-sources it, and `make` recipe lines run
via non-login `/bin/sh -c` which sources neither profile. Net effect: `make
verify-dotnet` → `dotnet build` fails with `dotnet: command not found`; the whole
gate never builds/tests the binding. (The script's own trailing `dotnet …`
diagnostics ran inside the subshell where the exports were live, so the install
step looked green — a deceptive symptom.)

**Fix (= the CKD precedent, Critic-preferred).**
- `.semaphore/install-dotnet.sh`: now **install-only**. Removed the three
  `export` lines and the `~/.bash_profile`/`~/.bashrc` append loop (dead), and
  corrected the comment that stated the inverted "login shell sources
  ~/.bash_profile" premise. The trailing validation now calls
  `"${DOTNET_ROOT}/dotnet" --list-sdks` / `--list-runtimes` by **absolute path**
  (dropped `--info`), because `dotnet` is intentionally no longer on PATH inside
  the script.
- `.semaphore/semaphore.yml` `"Verify .NET binding"` prologue: after
  `- ./.semaphore/install-dotnet.sh`, added **top-level `export` commands** —
  `- export DOTNET_ROOT="$HOME/.dotnet"` / `- export PATH="$HOME/.dotnet:$PATH"` /
  `- export DOTNET_MULTILEVEL_LOOKUP=0` — plus `- which dotnet` /
  `- dotnet --list-runtimes` verification. The install dir in the script
  (`${HOME}/.dotnet`) and the prologue `$HOME/.dotnet` are the SAME path.

**Why it's correct.** Top-level `export` commands mutate the job's ONE persistent
shell session directly, so they survive into the later `make verify-dotnet`
command — proven by the confluent-kafka-dotnet precedent
(`confluent-kafka-dotnet/.semaphore/semaphore.yml:46-48` sets the same three
exports as top-level job commands, and its very next commands `:49-50`
(`which dotnet` / `dotnet --version`) + `:52-53` see them; CKD writes NO profile
file). The Semaphore job cannot be fully run locally; the correctness argument is
this CKD precedent. Validated: `bash -n .semaphore/install-dotnet.sh` (syntax OK)
and `yq` parses `.semaphore/semaphore.yml` (prologue = install + 3 exports +
which/list-runtimes). Fixup: see the fixup commit referencing `701ab53a`.
