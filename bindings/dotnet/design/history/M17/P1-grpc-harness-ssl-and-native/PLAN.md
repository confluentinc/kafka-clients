# M17/P1 — .NET gRPC harness: merge master #204, then SSL / SASL_SSL runs and native mode, with CI jobs

> **Approved by the user on 2026-09-30, with all nine decisions D1–D9 taken as recommended in
> §9.** On D5, the user gave no current verify-dotnet duration, so the two full verify-dotnet jobs
> get a 60-minute job-level limit and the protocol jobs inherit the block's 30. Written by the
> Manager on 2026-09-30.
>
> **Base note, added at commit time.** This plan is committed on `3b885c1a` (content-identical to
> it), so its own commit is the merge's first parent. Wherever the gates below say "`3b885c1a`" as
> a diff base, read it as **this plan commit**. Otherwise the plan file itself would show up in the
> `bindings/dotnet` diff. The branch was pushed at `3b885c1a` by the user (19:57 IST), not by an
> agent.

**N = 88.** The highest number used in the binding is 87 (M15/P13.4). Neither `COMMENTS.88.md`
nor `COMMENTS.DONE.88.md` exists, and no `M17` directory or reference exists under
`bindings/dotnet/`.
**Milestone 17 is new** (D1). This work is harness and CI for all three services, so it does
not continue M8, which covered the consumer gRPC backend.
Branch `prashah_dev_dotnet_binding`.

**Bases.**
- Pre-merge HEAD is `3b885c1a`.
- `origin/master` is `d6bf7c76`, and the merge-base is `7ac1391b`.
- The Mode-A base for S2 is the S1 merge commit.

**Mode A.** The phase adds no ABI function, header change or `src/` change. Two scope notes are
recorded rather than left implicit:
1. The merge brings in master's Rust core changes. This phase did not write them, and §3.5 lists
   the two that reach .NET behaviour.
2. The `dotnet-actor` writes harness-test Rust in `tests/common/` and root infra in `Makefile`
   and `.semaphore/`. §5 explains why, and cites the precedents.

The request was: *"plan .NET support for SSL, SASL_SSL and native mode, including CI jobs, and
merge as part of that phase."*

---

## 0. Scope

**In scope:**
1. **S1 — the merge.** Merge `origin/master` (`e565ca74`, `d334cf6e`, `d6bf7c76`) into the branch.
   Resolve the 3 textual conflicts and the 2 semantic ones (§3.4), so that the merge commit builds
   and the container-mode .NET arms behave exactly as they did before.
2. **S2 — the features:**
   - **SSL / SASL_SSL runs.** The `__grpc_dotnet` / `__grpc_dotnet_async` arms run over the SSL
     and SASL_SSL container listeners through `INTEGRATION_TEST_PROTOCOL`, with Make targets that
     follow master's naming and two new Linux CI jobs.
   - **Native mode.** The .NET gRPC servers (sync and async) run as host processes
     (`MULTILANG_BACKEND_MODE=native`), with a build target, a test target and the macOS CI job
     running the native arm.
   - **Corrected claims.** Every stale "no .NET gRPC on macOS / arm64 protoc" claim in the files
     this phase touches is corrected.

**Out of scope** (§8): .NET producer transactions (the 3 skipped tests stay skipped), a Linux
native-mode CI job, native SSL / SASL_SSL CI jobs, any `src/` or ABI change, and any change to
`bindings/python` or `bindings/c`.

**Expected size.** S1 is conflict resolution plus about 10 lines of harness Rust. S2 is about 40
lines of C# (`Program.cs`), 2 Dockerfile lines, a 1-line csproj change, about 20 lines of harness
Rust, about 40 Make lines, 3 CI jobs, and comment corrections. **SSL / SASL_SSL is expected to
need no C# change** (§2, row 8). S2 proves this by running, and does not assume it.

---

## 1. Standing constraints (relay verbatim to the Actor and the Critic)

- No push. The PM or the user pushes after close. No messages to anyone, and in particular none
  to Pratyush.
- Do not edit `PendingAdminClientFindingsForDotnet.md` or the
  `Dotnet-AdminClient-Findings-Workflow/` folder.
- Local notes stay untracked through `.git/info/exclude`, never `.gitignore`. Never stage a
  `COMMENTS.DONE.<N>.md` at the binding root.
- Agents do not edit rule files (`CLAUDE.md`, `bindings/dotnet/CLAUDE.md`, `.claude/rules/*`).
  Put rule suggestions in the report.
- Never ssh or scp to the user's machines.
- Read with a budget: grep and targeted line ranges, not whole large files. Bound every tool
  output with `| head` or `cut -c1-200`.
- **Shell traps on this machine** (each has produced a false PASS before):
  - Start each Bash call with
    `export PATH="/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin:$HOME/.cargo/bin:$HOME/.nix-profile/bin:$PATH"`.
  - For a git **network** operation, call `/usr/local/libexec/airlock-agent/git` directly.
  - `grep` is ugrep, so use `/usr/bin/grep`. There is no `sed`. `cat` is bat, so use `/bin/cat`.
  - zsh does not split an unquoted `$var`, and globs must be quoted (`'--include=*.cs'`).
    `$T:t…` is a zsh modifier, so write `${T}:path`.
  - Set `PYTHONDONTWRITEBYTECODE=1`. Never echo a line that starts with `=====`.
  - A test filter that matches nothing "passes", so check the executed-test count. An aborted
    `dotnet test` can exit 0, so grep for `Test Run Aborted`.
  - **Use `~/.dotnet/dotnet`**, which has SDK 10.0.400 and runtimes NETCore 8.0.30 and 10.0.11.
    `/usr/local/share/dotnet` has only 10.0.10, so net8.0 tests cannot run with it. Neither
    install has **ASP.NET Core 8** (see D3).
- **Stale native artifacts.** `target/release/libconfluent_kafka.dylib` dates from 25 Sep, and
  Release unit tests crash on it. Rebuild it before any Release test or native run. The
  `target/release/libconfluent_kafka.so` on disk is the linux/amd64 build from P13.4 and predates
  this merge, so it is stale for the container gate.
- **Clean up before and after every harness run:** zero orphan broker or backend containers
  (`docker ps`), and zero orphan native servers (`pgrep -f Confluent.Kafka.GrpcServer`). Leftover
  brokers pollute the shared cluster pool and cause spurious reds (M15/P12).

---

## 2. Verified at HEAD `3b885c1a` (Manager, 2026-09-30)

| # | Fact | How verified |
|---|---|---|
| 1 | **The branch is pushed.** `origin/prashah_dev_dotnet_binding` is `3b885c1a` (reflog: "update by push", 19:57 IST), and the branch is 0 commits ahead. The brief's "14 ahead, unpushed" is out of date. The merge commit will sit on top of pushed history, so no force push is involved. | `git rev-list --left-right --count @{u}...HEAD` → `0 0` |
| 2 | `git merge-tree` finds exactly 3 conflicted files: `Makefile` (3 hunks), `.semaphore/semaphore.yml` (4 hunks) and `tests/common/backend_pool.rs` (1 hunk, doc only). | `git merge-tree --write-tree HEAD origin/master` → tree `23033cd6` |
| 3 | Master touches nothing under `src/ffi`, `cbindgen.toml`, `build.rs`, `Cargo.toml`, `Cargo.lock`, `generator/` or `bindings/dotnet`. The header SHA-1 today is `41f48ea837fd6d1348d649482459990c0847848a`. | `git diff --stat 7ac1391b origin/master -- …` is empty; `shasum` |
| 4 | **There are 668 P/Invokes** (`static extern`), not 697. `3b885c1a` removed 29. STATUS.md's "697" figures describe P13.4 and are historical. | `git grep 'static extern'` at `dbaac2e7` / `3b885c1a` gives 697 / 668 |
| 5 | **Compile break after the merge.** `BackendKind::native_command` matches only `Python \| PythonAsync` and `C`, so our `Dotnet` / `DotnetAsync` make it non-exhaustive. | Merged tree `23033cd6`, `backend_pool.rs` ~L174 |
| 6 | **A semantic conflict that still compiles.** Master changed every gRPC factory's `needs_container_bootstrap()` from `true` to `backend_pool::uses_containers()`. Our **5** dotnet impls still return `true`: Producer and Consumer for `DotnetGrpcFactory`, Producer and Consumer for `DotnetAsyncGrpcFactory`, and Admin for `DotnetGrpcFactory`. In native mode they would be handed container-only bootstrap addresses. | Merged `backend_factory.rs`: an awk listing of every impl |
| 7 | **The native readiness contract.** The harness starts the server with `GRPC_HOST=127.0.0.1` and `GRPC_PORT=0`. It waits on **stderr** for a line containing `listening on ` whose last `:` field is a **non-zero** `u16`, and treats the process as dead after `NATIVE_START_TIMEOUT`. Python and C default `GRPC_HOST` to `127.0.0.1`, and their Dockerfiles set `ENV GRPC_HOST=0.0.0.0`. | Merged `backend_pool.rs` `start_native` / `parse_listening_port`; master's diffs of `grpc_server.py`, `server.cc` and both Dockerfiles |
| 8 | **SSL / SASL_SSL need no file mounts.** `TestContext::apply_security` injects `security.protocol`, an **inline PEM** `ssl.truststore.certificates` and, for SASL_SSL, `sasl.mechanism=PLAIN` plus `sasl.jaas.config`. All five .NET servicers forward `request.Config` verbatim (`new Dictionary<string,string>(request.Config)`), and none filters keys. TLS (rustls / aws-lc) is always compiled into the core. No integration test branches on protocol or backend name. | `git show origin/master:tests/common/test_context.rs`; grep over `grpc-server/*.cs`; `Cargo.toml` |
| 9 | **Kestrel binds `GRPC_PORT=0`, but `Program.cs` reports the requested port.** A probe run printed `listening on 0.0.0.0:0` on stderr, while Kestrel's own stdout said `Now listening on: http://[::]:61649`. The harness would reject port 0 and time out. The server also binds all interfaces (`ListenAnyIP`) and ignores `GRPC_HOST`. | Probe run (scratchpad only), 6 s, then killed |
| 10 | **The "arm64 protoc SIGSEGV" is Linux-arm64 only.** Grpc.Tools 2.71.0 ships `linux_arm64`, `linux_x64`, `linux_x86`, `macosx_x64`, `windows_x64` and `windows_x86`, so there is **no macOS arm64 protoc**. On Apple Silicon it runs `macosx_x64/protoc` under Rosetta 2, which works here (`libprotoc 29.0`, and `oahd` is running). A probe `dotnet build -c Release -p:TargetFramework=net10.0 -o <scratch>` of `grpc-server` **succeeded** on this Mac, and `libconfluent_kafka.dylib` landed in the output. The crash in the M8/P1 recipe was the `linux_arm64` protoc inside an arm64 Linux container. **So native .NET on Apple Silicon needs no protoc workaround, only Rosetta.** | `ls ~/.nuget/packages/grpc.tools/2.71.0/tools`; `file` and `--version`; probe build (restore needed an explicit `-p:TargetFramework`; `obj/` was restored back to net8.0 afterwards) |
| 11 | **No ASP.NET Core 8 runtime** is installed locally (either install) or in CI (`install-dotnet.sh` installs SDK 10 plus `--runtime dotnet` 8.0, which is NETCore only). `grpc-server` is `Microsoft.NET.Sdk.Web` on `net8.0`, so as built today it **cannot run natively anywhere we test**. | `--list-runtimes`; `.semaphore/install-dotnet.sh` |
| 12 | **Master adds `execution_time_limit: 30` minutes** to both binding blocks. After the merge that limit covers our `verify-dotnet (Linux amd64)` job, which today has no limit and runs build, format, unit tests on 2 TFMs, soak tests, 2 images, about 146 arms and perf on a 2-core `s1-prod-ubuntu24-04-amd64-2`, and our `verify-dotnet (macOS arm64)` job. I could not read CI durations: the Semaphore MCP returns 404 for this project. | Merged YAML, lines 176 and 264 |
| 13 | **The arm census is unchanged by the merge**, going by the macro invocations in `tests/integration`: 22 `multilanguage_test!`, 14 `multilanguage_consumer_test!` and 80 `multilanguage_admin_test!` on both sides. That is 72 producer/consumer arms (sync and async) plus 80 admin arms (sync only), so **152 `__grpc_dotnet*` arms**. Of those, **146 execute** under the Make targets, because 3 transaction tests × 2 arms are skipped. This matches M15/P12's 152. | `git grep -F` counts at HEAD and at merged tree `23033cd6` |
| 14 | **Master's producer change is not on the .NET path.** `producer_batch.rs` now passes a sentinel `RecordMetadata` (-1s) to the Rust `Callback` thunk. That reaches C only through the push callback (`make_record_callback`). .NET binds only `Producer_send` / `_send_batch` (neither takes a callback) and pulls results with `FutureRecordMetadata_get` / `_get_all`. | `EntryPoint` grep; header prototypes |
| 15 | Docker is up locally. The `dotnet-grpc-server:dev` image is linux/amd64 from P13.4 and stale for this merge. No `dotnet-async-grpc-server:dev` image exists locally. | `docker info`; `docker images` |

---

## 3. S1 — the merge

### 3.1 Who

**The `dotnet-actor` (N=88)**, following the recipe below, with the PM verifying the gates (D2).
In P13.1 and P13.3 the PM merged, but those merges needed no authoring. This one needs Rust match
arms, harness contract changes and CI or Make text, which is authoring.

### 3.2 Steps

1. **Re-check the base.** `/usr/local/libexec/airlock-agent/git fetch origin master`, then
   `git rev-parse origin/master` must still be `d6bf7c76…`. **If it has moved, stop** and come
   back to the PM, because this plan is written against `d6bf7c76` only. `git status --porcelain`
   must show no staged changes (untracked files are normal here).
2. **Record the pre-merge baseline at `3b885c1a`:**
   - `cargo build --release --all-features`, which rebuilds the stale dylib.
   - `~/.dotnet/dotnet test -c Release -f net10.0` and `-f net8.0` over `Confluent.Kafka.sln`.
     Record the passed / total counts. P13.4 closed at 2927, and `3b885c1a` touched no tests, so
     2927 is expected. **Store the log.**
   - The dylib's sha256.
3. `git merge --no-ff --no-commit origin/master`.
4. Resolve the conflicts as §3.3 describes. Apply the semantic fixes in §3.4. No other edits.
5. Check for leftovers: `git diff --check`, and `/usr/bin/grep -rn '^<<<<<<<\|^>>>>>>>' Makefile .semaphore tests/common`
   must be empty.
6. Commit as `Merge origin/master (d6bf7c76) into prashah_dev_dotnet_binding`. The body should
   list the three master commits, name the 3 textual and 2 semantic resolutions, and end with the
   `Co-Authored-By` trailer.

### 3.3 Textual conflicts: keep both sides

| File / hunk | Resolution |
|---|---|
| `Makefile`, `.PHONY` (L19–35) | **Union.** Keep master's list in master's order, and add ours: `build-grpc-images-dotnet`, `test-integration-dotnet`, `test-dotnet` and `test-dotnet-macos-docker`. |
| `Makefile`, hunk 2 (L317–445) | **Keep both blocks.** Our `test-integration-dotnet` block goes first, next to `test-integration-c`, **unchanged**. Master's protocol-scoped and native block follows, **unchanged**. S1 does not rewrite our stale comments; §4.5 does that in S2. |
| `Makefile`, hunk 3 (L540–566) | Keep ours: the `test-dotnet` and `test-dotnet-macos-docker` targets and their comments. For the `test-python-macos-docker` comment, take **master's** text (ours was the pre-#204 wording). |
| `semaphore.yml`, header item 5 (L39–54) | Take master's text, including the paragraph on `execution_time_limit`. Add one clause saying that `verify-dotnet` in this block runs the .NET unit tests (S2 extends it to the native arm). |
| `semaphore.yml`, prologue comment (L180–186) | Take master's text. |
| `semaphore.yml`, Linux jobs (L202–219) | **Union.** Keep master's two `verify-python … ssl / sasl_ssl` jobs, and keep our `verify-dotnet (Linux amd64)` job unchanged. S2 renames it and adds its protocol siblings. |
| `semaphore.yml`, macOS block comment (L244–262) | Take master's comment and block name ("Verify language bindings (macOS arm64)"). Add one line: "verify-dotnet runs the .NET unit tests." Check that our `verify-dotnet (macOS arm64)` job is present **once** in the merged block. The auto-merge keeps it, and it drops our old `MACOS_SKIP_COLIMA` (master's side), so Colima now comes up for the dotnet job too. That is harmless in S1 and needed in S2. |
| `backend_pool.rs`, module doc (L24–48) | Take master's "Backend modes" doc. Extend its port list to `(50051 python / 50052 c / 50053 dotnet and dotnet_async)` and say that the containers are separate. |

### 3.4 Semantic conflicts, fixed in the merge commit

These count as conflict resolution: the merge must compile, and our code must follow master's
changed contract.

1. **`native_command` exhaustiveness** (§2, row 5). Add a
   `BackendKind::Dotnet | BackendKind::DotnetAsync` arm that **panics with an explicit message**,
   for example: *"{label} native gRPC backend is not available yet; run with
   MULTILANG_BACKEND_MODE=container"*. This matches the file's own panic-with-hint style
   (`require_native_artifact`). It keeps the merge commit at the pre-merge capability, where .NET
   had container mode only. S2 replaces the arm with the real launcher. It never ships this way,
   because nothing is pushed before close.
2. **All 5 dotnet `needs_container_bootstrap()` → `crate::common::backend_pool::uses_containers()`**
   (§2, row 6), exactly as master's python and c impls do. In container mode this is identical,
   because `uses_containers()` is `true` there.

### 3.5 Master's behaviour changes that reach .NET (no ABI change)

- **Consumer `commit_async` with empty offsets** (`async_kafka_consumer.rs`) now queues its
  callback at once and keeps callback order through `async_commit_ordering_tail`. The .NET unit
  tests use only `Mock*`, so they are unaffected. The .NET gRPC consumer arms run the real
  consumer and pick up the fix. The Critic checks that no .NET doc describes the old ordering.
- **Producer sentinel metadata**: not on the .NET path (§2, row 14).

### 3.6 S1 gates (the Actor runs them, the PM verifies them before S2)

1. `cargo build --features ffi` succeeds, and the regenerated header's SHA-1 is still `41f48ea8…`.
   **If it changed, stop and escalate.** That would mean the phase is no longer Mode A.
2. The extern count is still **668**.
   `git diff 3b885c1a HEAD -- bindings/dotnet src/ffi cbindgen.toml` is **empty**.
3. `cargo build --release --all-features`. The dylib's sha256 differs from the step-2 baseline,
   and its mtime is later than the merge.
4. `~/.dotnet/dotnet build -c Release Confluent.Kafka.sln` gives 0 warnings and 0 errors. Unit
   tests on **net10.0 and net8.0** give the **same passed / total as the step-2 baseline**.
5. `cargo test --features integration-tests,multilanguage-tests --test integration --no-run`
   **compiles**, which proves the §2 row-5 break is fixed. `cargo test … -- --list` then shows
   **152** names containing `__grpc_dotnet`. Record the exact list in a file for S2's
   reconciliation.
6. `cargo xtask format-check` and `cargo xtask lint` pass. The nix toolchain is present at
   `~/.nix-profile/bin`, so this is **not** CI-only. Check with the full path before declaring a
   tool absent.

---

## 4. S2 — SSL / SASL_SSL, native mode and CI

### 4.1 .NET server contract (`bindings/dotnet/grpc-server/`, Dockerfiles)

1. **`Program.cs` follows master's server contract:**
   - Read **`GRPC_HOST`**, defaulting to `127.0.0.1` because the server has no authentication
     (master's reason). Bind it with `options.Listen(IPAddress.Parse(host), port, h2c)`, keeping
     `HttpProtocols.Http2`.
   - Read **`GRPC_PORT`**, defaulting to 50053. An invalid value makes the server write an error
     to stderr and exit non-zero (D8), as C's `strtol` check and Python's `int()` do. Today it
     silently falls back to 50053.
   - After `app.Start()`, read the **actually bound** port from the server's addresses feature
     (`app.Urls` or `IServerAddressesFeature`). Write `listening on {host}:{boundPort}` to
     **stderr**, then flush. The last `:` field must be the port.
   - Update the remarks on the type and the method to describe the new contract, and name
     `backend_pool.rs` as the consumer of the line.
2. **Both Dockerfiles** (`Dockerfile.grpc` and `Dockerfile.grpc.async`) add `ENV GRPC_HOST=0.0.0.0`
   with master's one-line comment. Without it, the loopback default would make the containerized
   server unreachable through the published port, and every container arm would go red. §7.2's
   container gate exists partly to catch this.
3. **The TFM for the native build (D3, recommended T2).** Change `grpc-server` to
   `<TargetFrameworks>net8.0;net10.0</TargetFrameworks>`. The Dockerfiles then publish with
   `-f net8.0`, so the container image, runtime `aspnet:8.0` and the tested net8.0 asset are all
   unchanged. The native build uses `-f net10.0`, which runs on the ASP.NET Core 10 that SDK 10
   already brings, locally and in CI. This has a side benefit: the library's **net10.0** asset
   then gets broker-level coverage for the first time.

### 4.2 Harness Rust (`tests/common/backend_pool.rs`, recorded exception, §5)

Replace S1's panicking arm with the launcher. It mirrors the Python arm (an interpreter plus a
script) and the C arm (an override env var):

```rust
BackendKind::Dotnet | BackendKind::DotnetAsync => {
    let dotnet = std::env::var_os("MULTILANG_DOTNET").map(PathBuf::from).unwrap_or_else(|| "dotnet".into());
    let server = std::env::var_os("MULTILANG_DOTNET_GRPC_SERVER").map(PathBuf::from)
        .unwrap_or_else(|| root.join("target/grpc-native/dotnet/Confluent.Kafka.GrpcServer.dll"));
    require_native_artifact(self, &server, "build-grpc-native-dotnet");
    let mut command = Command::new(dotnet);
    command.arg(server).env("CONSUMER_FLAVOR", if self == BackendKind::DotnetAsync { "async" } else { "sync" });
    (command, "build-grpc-native-dotnet")
}
```

- `CONSUMER_FLAVOR` is set **explicitly for both kinds**, so a value inherited from the test
  process cannot change the flavor.
- The bare `dotnet` is resolved through `PATH` (the CI job exports `$HOME/.dotnet`).
  `require_native_artifact` already skips bare commands.
- Extend the `native_command` doc comment to cover .NET.
- Leave the upstream wording elsewhere alone (for example, "The two non-native backends").

### 4.3 Make targets

**`bindings/dotnet/Makefile`:**
- Add a `grpc-native` target:
  `$(DOTNET) build -c Release -f net10.0 <grpc-server csproj> -o $(RUST_PROJECT_ROOT)/target/grpc-native/dotnet`.
- `-c Release` is **pinned**, not `$(DOTNET_CONFIG)`. The csproj's `<Content>` copies
  `target/$(CargoProfileDir)/libconfluent_kafka.*`, and only the release dylib is produced by
  `build-rust-all-features`.
- Correct the header comment that says `grpc-server` is "only ever built inside the gRPC Docker
  images".

**Root `Makefile`, mirroring master's names:**

```make
# the three .NET-only transaction skips, defined once (D9)
DOTNET_GRPC_SKIPS = --skip test_transactional_records_are_visible_only_after_commit \
	--skip test_aborted_transaction_records_are_discarded \
	--skip test_consume_transform_produce_with_offsets

test-integration-dotnet-ssl:
	INTEGRATION_TEST_PROTOCOL=ssl $(MAKE) test-integration-dotnet
test-integration-dotnet-sasl-ssl:
	INTEGRATION_TEST_PROTOCOL=sasl_ssl $(MAKE) test-integration-dotnet

build-grpc-native-dotnet: build-rust-all-features
	$(MAKE) -C bindings/dotnet RUST_PROJECT_ROOT=$(RUST_PROJECT_ROOT) grpc-native
test-integration-dotnet-native: build-grpc-native-dotnet
	MULTILANG_BACKEND_MODE=native \
		cargo test --features integration-tests,multilanguage-tests --test integration -- __grpc_dotnet $(DOTNET_GRPC_SKIPS)

verify-dotnet-macos-docker: test-dotnet-macos-docker
	$(MAKE) test-integration-dotnet-native
```

- `test-integration-dotnet` uses `$(DOTNET_GRPC_SKIPS)` instead of its three inline `--skip`
  lines. Its non-Linux self-skip message then points to
  `make test-integration-dotnet-native`, with master's `INTEGRATION_TEST_PROTOCOL` prefix trick.
- Add the 4 new targets to `.PHONY`.
- Update the "REMOVE THESE THREE LINES" comment to say "remove `DOTNET_GRPC_SKIPS`", so the
  instruction still points at something that exists.

### 4.4 CI (`.semaphore/semaphore.yml`, `.semaphore/dependencies-macos.sh`)

- **Linux block.**
  - Rename our job to `verify-dotnet (Linux amd64) — plaintext` (it keeps `make verify-dotnet`).
  - Add `verify-dotnet (Linux amd64) — ssl` running `make test-integration-dotnet-ssl`.
  - Add `verify-dotnet (Linux amd64) — sasl_ssl (plain)` running
    `make test-integration-dotnet-sasl-ssl`.
  - The two protocol jobs **do not install .NET on the host.** The images build inside
    `sdk:10.0`, and the harness is Rust, so they need only cargo and Docker. This mirrors master's
    Python protocol jobs, which install no venv. Gate: `make -n test-integration-dotnet-ssl`
    shows no host `dotnet` call.
  - Update the block comment's job count ("six jobs" becomes nine).
- **macOS block.** `verify-dotnet (macOS arm64)` keeps `make verify-dotnet-macos-docker`, which
  now ends with the native arm. It gains `env_vars: MACOS_ENSURE_ROSETTA=true` (D4), backed by a
  guarded block in `dependencies-macos.sh` that follows the `MACOS_INSTALL_GRPC_CPP` pattern:
  `if ! /usr/bin/arch -x86_64 /usr/bin/true 2>/dev/null; then sudo softwareupdate --install-rosetta --agree-to-license; fi`.
  Only the dotnet job sets it.
- **Time limits (D5).** Add job-level `execution_time_limit: minutes: 60` to the two full
  `verify-dotnet` jobs (Linux plaintext and macOS). The protocol jobs inherit the block's 30.
  Tighten the limit to about twice the measured duration after the first green run.
- **Header comment**, items 3 and 5: list the dotnet jobs and correct the protoc rationale
  (§4.5).

### 4.5 Stale claims to correct (enumerated at the merged tree; re-grep at S2 start)

1. `semaphore.yml` ~L34–35: "Grpc.Tools ships an arm64 protoc that SIGSEGVs … so building it
   needs an amd64-native agent". This is true of **arm64 Linux** only. Say so. The Linux agent is
   amd64 anyway.
2. `semaphore.yml` ~L249: "The gRPC multilanguage arm runs on Linux only". The macOS dotnet job
   now runs it natively.
3. `Makefile` ~L331–332 and ~L365–366, the `test-integration-dotnet` comment and skip message:
   "no -macos variant … arm64 protoc SIGSEGVs". Replace with the native pointer.
4. `Makefile` ~L555–557, `test-dotnet-macos-docker`: "runs on Linux only … Linux-only
   Grpc.Tools/protoc constraint".
5. `Makefile` ~L598, `verify-dotnet-macos-docker`: "drop the Linux-only integration stage".
   Master's C and Python siblings now run the native arm, and so does ours.
6. `bindings/dotnet/Makefile` L9–10 (see §4.3).

The sweep is by **phrase**, not by this list: `SIGSEGV`, `arm64 protoc`, `Linux only`,
`Linux-only`, `only ever built inside`. It runs again at S2 start over `Makefile`, `.semaphore/`,
`bindings/dotnet/` and `tests/common/`. This applies the M15/P13.4 G1-5 lesson that a
narrower count undercounts.

### 4.6 S2 commits (suggested)

1. `feat(dotnet): gRPC server honours GRPC_HOST/GRPC_PORT=0 and reports the bound port (M17/P1)`
   covers `Program.cs`, both Dockerfiles and the csproj TFMs.
2. `test(harness): native launcher for the dotnet backends (M17/P1)` covers `backend_pool.rs`.
3. `build(dotnet): build-grpc-native-dotnet, test-integration-dotnet-{native,ssl,sasl-ssl} (M17/P1)`
   covers both Makefiles.
4. `ci(dotnet): dotnet ssl / sasl_ssl Linux jobs, native arm on macOS, job time limits (M17/P1)`
   covers `.semaphore/*`.
5. `docs(dotnet): correct the macOS/protoc claims (M17/P1)`. The comment fixes may instead ride
   in commits 3 and 4.

### 4.7 Shapes settled in the plan (so the Critic does not flag them as unapproved)

- The launcher is `dotnet <dll>`, not the apphost.
- The output directory is `target/grpc-native/dotnet/`.
- The override variables are `MULTILANG_DOTNET` and `MULTILANG_DOTNET_GRPC_SERVER`.
- `CONSUMER_FLAVOR` is set explicitly for both kinds.
- The Release configuration is pinned for `grpc-native`.
- The protocol jobs run without host .NET.
- Native CI runs PLAINTEXT only (D6).
- The loopback default and fail-fast `GRPC_PORT` apply (D8).
- The S1 panicking arm is transitional and never pushed.

---

## 5. Ownership

| Change | Owner | Basis |
|---|---|---|
| S1: the merge and its resolution | `dotnet-actor` 88 | D2. The resolution is authoring (§3.1). |
| `grpc-server/*.cs`, csproj, Dockerfiles, `bindings/dotnet/Makefile` | `dotnet-actor` 88 | Normal scope (`bindings/dotnet/CLAUDE.md`) |
| Root `Makefile`, `.semaphore/semaphore.yml`, `.semaphore/dependencies-macos.sh` | `dotnet-actor` 88 | Precedent: M10/P1 (N=26), where the `dotnet-actor` edited root `Makefile`, `semaphore.yml` and added `install-dotnet.sh` |
| `tests/common/backend_pool.rs` and `backend_factory.rs` (harness Rust) | `dotnet-actor` 88, **recorded exception** | Precedent: M8/P1 `f910e7b3`, M8/P2 `d1ef8906`, M15/P12 `ece8bb74` (P12 PLAN §3 ruling). This is harness glue, not translation or core Rust. Every edit is a mechanical copy of the python or c arm with the label, path and flavor swapped. |
| `src/**`, the ABI and the header | nobody | Not expected. A changed header SHA stops the phase (§3.6 gate 1). |
| Review of all of the above | `dotnet-critic` 88 | M10/P1: the `dotnet-critic` reviewed root-infra CI correctness. M15/P12: the harness Rust was reviewed as sanctioned. `kafka-critic` is not needed because the ABI does not change. |

---

## 6. The one Critic pass (after S2)

`dotnet-critic` 88 reviews **`3b885c1a..<S2 end>`** once, including the merge commit
(`git show --remerge-diff <merge>`, or a diff against each parent). Its brief includes:

1. **Nothing was dropped in the merge.** Compare the merged `.PHONY` set, target names and CI job
   names against **both** parents (a set difference, not a read-through). Every master python or
   c target and job exists, and every dotnet one exists.
2. All 5 factory impls return `uses_containers()`.
3. **The native contract.** The line goes to stderr, reports the bound port, and its last `:`
   field is the port. The `GRPC_HOST` default is loopback. **Both** Dockerfiles set `0.0.0.0` and
   publish `-f net8.0`. `CONSUMER_FLAVOR` is explicit for both kinds.
4. `DOTNET_GRPC_SKIPS` reaches the container, ssl, sasl-ssl and native paths.
5. **CI correctness.** Job names and commands mirror python / c, the protocol jobs call no host
   `dotnet`, Rosetta is guarded and opt-in, and the time limits are in place.
6. The §4.5 phrase sweep is clean.
7. **Mode A.** No change to `src/`, `bindings/python`, `bindings/c`, the header or the extern
   count.
8. The §3.5 commit-ordering doc check.

The Actor fixes findings with `fixup!` commits. The Critic re-checks only those.

---

## 7. Gates

### 7.1 S1

See §3.6.

### 7.2 S2 (the Actor runs them, the PM verifies them)

1. `grpc-server` builds with **0 warnings and 0 errors for both TFMs**, and
   `~/.dotnet/dotnet format bindings/dotnet/grpc-server/Confluent.Kafka.GrpcServer.csproj --verify-no-changes`
   is clean. The project is not in the `.sln`, so `make test-dotnet` does not format it.
2. `make build-grpc-native-dotnet` puts `Confluent.Kafka.GrpcServer.dll` and a
   `libconfluent_kafka.dylib` whose sha256 matches `target/release`'s in `target/grpc-native/dotnet/`.
3. **Native, local, all three protocols:** `make test-integration-dotnet-native`, then
   `INTEGRATION_TEST_PROTOCOL=ssl …`, then `=sasl_ssl …`.
   - Before each run, **predict 146 executed** from §3.6's stored list and check it. Investigate
     any **surplus** green as well as any red (the M15/P12 green-side reconciliation).
   - All green.
   - SSL and SASL_SSL native are local-only evidence, not CI jobs (D6).
4. **Container, local, all three protocols** (Docker is up; the M8/P1 amd64 recipe):
   - Cross-build a fresh linux/amd64 `.so` at the S2 HEAD in `rust:1-bookworm` with
     `--platform linux/amd64`, and stage it at `target/release/libconfluent_kafka.so`.
   - Prove it is fresh: its sha256 differs from P13.4's `4cc23659…`, and all 668 `EntryPoint`s
     resolve, using `nm -D` in the container and `LC_ALL=C` sorts.
   - Build **both** images with `DOCKER_DEFAULT_PLATFORM=linux/amd64 make -C bindings/dotnet grpc-image grpc-image-async`,
     and check that the in-image `.so` sha256 equals the staged one.
   - Unset the variable, then run
     `MULTILANG_BACKEND_MODE=container INTEGRATION_TEST_PROTOCOL={plaintext,ssl,sasl_ssl} cargo test --features integration-tests,multilanguage-tests --test integration -- __grpc_dotnet $(skips)`.
     This goes around the Make target's non-Linux self-skip. Expect 146 per protocol.
   - Emulated arms are slow, and slow is not the same as hung (P13.4 measured 239 s for the
     compile).
5. **Oracle rule.** If a .NET arm is red under SSL / SASL_SSL and green under PLAINTEXT, run the
   same scenario's Python arm natively (`make build-grpc-native-python`, then
   `INTEGRATION_TEST_PROTOCOL=… make test-integration-python-native` filtered to that scenario)
   **before touching C#**. This tells a harness or broker issue from a .NET one.
6. Unit tests on both TFMs pass with the S1 count (the library is untouched). The extern count is
   668, and the header SHA-1 is `41f48ea8…`.
7. **Mode A.** `git diff <S1 merge>..HEAD -- src/ cbindgen.toml generator/ build.rs Cargo.toml Cargo.lock bindings/python bindings/c`
   is empty. The `tests/` diff touches only `tests/common/backend_pool.rs`.
8. **Wiring.** `make -n` of each new target shows the right env variables and skips, and no host
   `dotnet` in the protocol targets. A YAML parse of `semaphore.yml` (`ruby -ryaml -e 'YAML.load_file(ARGV[0])'`)
   succeeds. `cargo xtask format-check` and `lint` pass.

### 7.3 CI-only (they cannot run locally; they are checked on the user's push after close)

- The Semaphore jobs themselves: 3 Linux dotnet jobs and the macOS native job.
- Whether the macOS agent needs the Rosetta install, and whether it can `sudo`.
- The real job durations against the limits.
- Native mode on **Linux**, which no CI job runs, as for master.

The phase **closes with these pending** (M8/P1 practice) and says so in STATUS and the close
note. If the first CI run is red on a CI-only item, that becomes a fixup inside this phase's
record, not a new phase.

### 7.4 Close

- Update STATUS.md. Include the extern baseline 668 (reconciling "697") and the arm counts.
- Archive `COMMENTS.DONE.88.md` (or a clean-pass `COMMENTS.88.md`) under this directory, and reset
  the root `COMMENTS.88.md`.
- `marked_classes.txt` does not apply (no Java translation).

---

## 8. Out of scope (deliberately not added)

- **.NET producer transactions.** The 3 skips stay. `ProducerServiceImpl` and
  `AsyncProducerServiceImpl` have no transaction RPCs. Removing `DOTNET_GRPC_SKIPS` is the last
  step of that future phase.
- **A Linux native-mode CI job**, and **native SSL / SASL_SSL CI jobs.** Master runs neither.
  Master's macOS jobs are PLAINTEXT only, because the TLS and SASL paths are Rust code already
  covered on macOS by "Verify Rust".
- **A `__grpc_dotnet_async` admin arm.** M15/P12 D1 stands, because .NET has no `IAsyncAdmin`.
- **Adding `grpc-server` to `make test-dotnet`'s format pass.** This is noted in §7.2 gate 1 as a
  manual gate, and it could be a later Makefile change.
- **Upstream issues, not ours:** `tests/common/admin_backend.rs:1909` links to a
  `bootstrap_for` that master renamed (a broken rustdoc link), and `BackendKind`'s doc says
  "two non-native backends".
- **The NUL guard for construction config** (the P13.4 follow-up). PEM values contain newlines,
  not NULs, so this phase is unaffected.

---

## 9. Decisions for the user

| # | Decision | Recommendation |
|---|---|---|
| D1 | Numbering | **M17/P1, N=88.** This is cross-service harness and CI work. It does not continue M8 (the consumer backend) or M10 (the first CI wiring). |
| D2 | Who performs the merge | **`dotnet-actor` 88 as S1**, with the PM verifying §3.6. The alternative is the PM, as in P13.1 and P13.3, but those merges needed no authoring and this one does (§3.4). |
| D3 | TFM of the native .NET server. ASP.NET Core 8 is absent locally and in CI (§2, row 11). | **T2:** multi-target `net8.0;net10.0`, run native on net10.0, and keep the container on net8.0 (Dockerfiles `-f net8.0`). The alternatives: (T3) install ASP.NET Core 8 in `install-dotnet.sh` and on your Mac, so native also tests net8.0; (T4) `DOTNET_ROLL_FORWARD=Major`, which is hacky and matches no shipped configuration; (T5) a custom TFM property, which is not idiomatic. A `-p:TargetFramework=` override proved fragile: implicit restore failed with NETSDK1005 (§2, row 10). |
| D4 | macOS protoc | **Keep Grpc.Tools' pinned `macosx_x64` protoc under Rosetta**, the same generator as every other build, with an opt-in guarded Rosetta install (`MACOS_ENSURE_ROSETTA`). The alternative is Homebrew's native arm64 `protoc` + `grpc_csharp_plugin` through `Protobuf_ProtocFullPath` / `gRPC_PluginFullPath`, which adds version drift from Grpc.Tools' protoc 29. |
| D5 | CI time limits (master's 30 minutes now covers verify-dotnet) | **Job-level 60 minutes on the two full verify-dotnet jobs; the protocol jobs inherit 30.** Tighten to about twice the measured duration after the first green run. If you know the current verify-dotnet duration, tell me and I will size it now. I could not read Semaphore (404). |
| D6 | CI scope | **Mirror master:** Linux container × 3 protocols; macOS native PLAINTEXT only; no Linux native job; no host .NET in the protocol jobs. |
| D7 | Cadence | **2 sub-stages** (S1 merge, S2 features), with the PM verifying gates between them and **no user gate**. **One Critic pass after S2** over the whole range including the merge. The local native and container gates for all three protocols are **required** (Docker is up), and §7.3's items stay CI-only. There are not 3 sub-stages because SSL / SASL_SSL is Make and CI wiring that overlaps native mode in the same Makefile block and YAML file, so splitting it would only add churn. |
| D8 | Server contract details | **The `GRPC_HOST` default becomes `127.0.0.1`** (the Dockerfiles set `0.0.0.0`), and **an invalid `GRPC_PORT` fails fast.** Both mirror master's C and Python servers. |
| D9 | The 3 transaction skips | **Carry them into the ssl, sasl-ssl and native targets through one `DOTNET_GRPC_SKIPS` variable.** Transaction parity is its own phase. |

---

## 10. Risks

1. **The 30-minute limits** could fail CI on the first push (D5 addresses this).
2. **Rosetta may be missing on the Semaphore macOS image,** or `sudo` may be blocked there. The
   job would then fail at protoc with "Bad CPU type". This is CI-only. The fallback is D4's
   alternative.
3. **The macOS job builds the crate twice:** `build-dotnet` uses `--features ffi` and
   `build-grpc-native-dotnet` uses `--all-features`. Python's native job has the same cost.
4. **Base drift.** If `origin/master` moves before S1, stop (§3.2 step 1).
5. **Orphan native servers.** `terminate()` SIGKILLs the servers, and a killed test process
   leaves them running. Gate by `pgrep` before and after each run.
6. **The loopback default breaks container mode silently** if one Dockerfile misses the ENV line.
   §7.2 gate 4 covers **both** images.
7. **The S1 panicking arm** would regress macOS if S1 were pushed alone. It is not pushed alone.

---

## 11. Rule suggestions (for the user; agents do not edit rule files)

- `bindings/dotnet/CLAUDE.md` could record once that **Grpc.Tools has no macOS-arm64 protoc; on
  Apple Silicon it uses `macosx_x64` under Rosetta; the SIGSEGV is `linux_arm64`-only**. The wrong
  generalization spread to at least 6 comment sites.
- The same file could list `grpc-server` among the projects outside the `.sln` that need their
  own `dotnet format --verify-no-changes`, as `test-soak-dotnet` does for the soak projects.
