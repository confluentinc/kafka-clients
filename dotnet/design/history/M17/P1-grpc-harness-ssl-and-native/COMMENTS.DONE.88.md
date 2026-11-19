# COMMENTS.DONE.88 — Critic 88, M17/P1 (gRPC harness: SSL / SASL_SSL jobs + native dotnet backend + master merge)

Range reviewed: `3b885c1a..076041ed`. The commits are 35064058 (plan), 7690945a (merge of origin/master d6bf7c76, reviewed with `--remerge-diff`), 1641bacf, 8bac2d2a, 08eddcba and 076041ed.
Ground truth: `bindings/dotnet/design/history/M17/P1-grpc-harness-ssl-and-native/PLAN.md` §3.3, §4.1–§4.7 and §6, master's python/c harness contract, and the C ABI header.

There are three findings, all low severity. Each one is a stale statement in a file this phase touched. None of them is a behaviour defect. The clean-pass record for the nine §6 checks follows the findings.

---

## 88.1 — `Program.cs` method doc still says the flavor is "not injected by the harness"

- **Severity:** low. This is a stale doc in a file this phase edited, and it contradicts the same file's type remark.
- **Commit:** 1641bacf rewrote the type remark (L41-43) for native mode but left the method doc. 8bac2d2a made the doc false.
- **Location:** `bindings/dotnet/grpc-server/Program.cs:286-288` (`ResolveAsyncFlavor`'s `<summary>`).
- **Evidence:**
  ```
  $ git grep -n 'not injected by the harness' 076041ed -- bindings/dotnet/grpc-server
  076041ed:bindings/dotnet/grpc-server/Program.cs:288:    /// default). The flavor is baked per image, not injected by the harness (PLAN §4).
  $ git grep -n 'CONSUMER_FLAVOR' 076041ed -- tests/common/backend_pool.rs
  076041ed:tests/common/backend_pool.rs:171:    /// same assembly, and `CONSUMER_FLAVOR` is set explicitly to pick the sync
  076041ed:tests/common/backend_pool.rs:220:                command.arg(server).env("CONSUMER_FLAVOR", flavor);
  ```
  The same file's type remark at Program.cs:41-43 already reads: "in native mode the harness sets it explicitly for each backend kind instead (backend_pool.rs `native_command`)".
- **Why it is wrong:**
  - PLAN §4.5 requires correcting every claim this phase made false, and brief check 6 is the stale-phrase sweep.
  - The method doc is the comment sitting next to the code that reads the variable. It now says the opposite of what native mode does.
  - Its citation, "(PLAN §4)", points at the M8/P2 plan's "baked per image, not injected by the harness" (`design/history/M8/P2-async-consumer-grpc-backend/PLAN.md:98`). This phase superseded that decision for native mode.
- **Suggested fix:** replace the last sentence with: "Container mode: the flavor is baked per image (`ENV CONSUMER_FLAVOR` in Dockerfile.grpc / Dockerfile.grpc.async). Native mode: the harness sets it explicitly per backend kind (`tests/common/backend_pool.rs` `native_command`)."

- **Resolution:** fixed in 95d95074 (`fixup! feat(dotnet): gRPC server honours GRPC_HOST/GRPC_PORT=0 and reports the bound port (M17/P1)`). `ResolveAsyncFlavor`'s `<summary>` now states both modes: container mode bakes the flavor per image (`ENV CONSUMER_FLAVOR` in Dockerfile.grpc / Dockerfile.grpc.async), native mode has the harness set it per backend kind (`tests/common/backend_pool.rs` `native_command`, verified at :215-220). The stale "(PLAN §4)" citation is gone. It agrees with the type remark at Program.cs:41-43. "servicer" is also pluralised to "servicers", since the selector picks a set.

---

## 88.2 — `.semaphore/install-dotnet.sh` header names a renamed CI block, and its ASP.NET Core claim is now false

- **Severity:** low. The comments are stale, and CI behaviour is unaffected.
- **Commit:** the file is not edited in the range. 7690945a merged master's block rename (#204), and 1641bacf plus 08eddcba made the ASP.NET Core sentence false.
- **Location:** `.semaphore/install-dotnet.sh:6-7` and `:22-25`.
- **Evidence:**
  ```
  $ git grep -c 'Build + unit test bindings' 076041ed -- .semaphore/semaphore.yml
  (no match, rc=1)
  $ git grep -n 'Verify language bindings (macOS arm64)' 076041ed -- .semaphore/semaphore.yml
  076041ed:.semaphore/semaphore.yml:40:#   5. "Verify language bindings (macOS arm64)" -- verify-c (ctest),
  076041ed:.semaphore/semaphore.yml:250:  - name: "Verify language bindings (macOS arm64)"
  ```
  - install-dotnet.sh:6-7 says the macOS job lives in "Build + unit test bindings (macOS)", but no block by that name exists at HEAD.
  - install-dotnet.sh:22-25 says: "the ASP.NET Core runtime is only used inside the gRPC Docker images, which bundle their own aspnet:8.0."
  - At HEAD, "verify-dotnet (macOS arm64)" ends with `make verify-dotnet-macos-docker`, which runs `test-integration-dotnet-native`. That target runs `dotnet target/grpc-native/dotnet/Confluent.Kafka.GrpcServer.dll`, built `-f net10.0` by `bindings/dotnet/Makefile` `grpc-native`.
  - That DLL runs on the host's Microsoft.AspNetCore.App 10, which comes from the SDK 10 install this script performs. The script's own sibling comment says so: bindings/dotnet/Makefile:41-44, "SDK 10 … brings the ASP.NET Core 10 runtime but no ASP.NET Core 8 one", and the csproj :20 "M17/P1 D3".
- **Why it is wrong:**
  - PLAN §4.5 requires correcting claims this phase made false, and brief check 6 covers `.semaphore/`.
  - This comment justifies the lean install. It now omits the one host-side ASP.NET Core consumer the phase added.
  - A later "lean it further" change that switched the SDK install to a runtime-only one would be justified by this sentence, and it would break the macOS native leg.
- **Suggested fix:**
  - L6-7: change the block name to `"Verify language bindings (macOS arm64)"`.
  - L22-25: "…the ASP.NET Core **8** runtime is needed only inside the gRPC Docker images, which bundle their own aspnet:8.0. The native (host-process) gRPC server is built for net10.0 and runs on the ASP.NET Core 10 runtime the SDK 10 install already brings (M17/P1 D3)."

- **Resolution:** fixed in f7a99ad9 (`fixup! ci(dotnet): dotnet ssl / sasl_ssl Linux jobs, native arm on macOS, job time limits (M17/P1)`). In `.semaphore/install-dotnet.sh`: the macOS block is now named "Verify language bindings (macOS arm64)" (semaphore.yml:250); the Linux job is named exactly ("verify-dotnet (Linux amd64) — plaintext", :209-219); the header says the new Linux ssl / sasl_ssl dotnet jobs (:220-225) do not run the script, because their .NET server runs inside the gRPC images; and the ASP.NET Core sentence now says only ASP.NET Core 8 is Docker-only (aspnet:8.0), while the macOS native net10.0 server runs on the ASP.NET Core 10 runtime that SDK 10 brings (M17/P1 D3). `bash -n` passes.

---

## 88.3 — Dockerfile headers still say the images serve "ConsumerService only" (pre-existing, optional)

- **Severity:** low. **Pre-existing:** introduced by 0204437a4, before the range. This phase touched both files, which is the only reason it is listed. Fix it if convenient.
- **Commit:** 1641bacf touched both files (`-f net8.0` and `ENV GRPC_HOST=0.0.0.0`) and left these headers.
- **Location:**
  - `bindings/dotnet/Dockerfile.grpc:10` and `:21-22`
  - `bindings/dotnet/Dockerfile.grpc.async:10` and `:26-27`
- **Evidence:**
  ```
  $ git grep -n -i 'consumer-backend\|ConsumerService only' 076041ed -- bindings/dotnet/Dockerfile.grpc bindings/dotnet/Dockerfile.grpc.async
  Dockerfile.grpc:10:# Multi-stage build for the .NET gRPC consumer-backend image used by the
  Dockerfile.grpc:21:# The image serves ConsumerService only (consumer-side backend); it drives the
  Dockerfile.grpc.async:10:# Multi-stage build for the *async* .NET gRPC consumer-backend image used by the
  Dockerfile.grpc.async:26:# The image serves ConsumerService only (consumer-side backend); it drives the
  ```
  Program.cs:103-114 registers `AsyncProducerServiceImpl` + `AsyncConsumerServiceImpl` (async image), and `ProducerServiceImpl` + `ConsumerServiceImpl` + `AdminServiceImpl` (sync image, M15/P12 D1). Program.cs:100 says: "both flavors host BOTH services".
- **Why it is wrong:** ProducerService has been hosted since M12/P1 and AdminService since M15/P12, so "ConsumerService only (consumer-side backend)" is false for both images.
- **Suggested fix:** "The image serves ProducerService + ConsumerService (+ AdminService in the sync image, M15/P12 D1) over the binding's synchronous (async: asynchronous) clients…". Also drop "consumer-" from line 10 of each file.

- **Resolution:** fixed in 8226b62a (a second `fixup! feat(dotnet): gRPC server honours GRPC_HOST/GRPC_PORT=0 …`). Both Dockerfile headers now list the hosted services and drop "consumer-" from "consumer-backend". Dockerfile.grpc hosts Producer + Consumer + Admin over KafkaProducer / KafkaConsumer / KafkaAdminClient. Dockerfile.grpc.async hosts Producer + Consumer, with no Admin (M15/P12 D1). Two more spots in Dockerfile.grpc.async made the same claim and are fixed too: its intro (L13-14) and its `ENV CONSUMER_FLAVOR` comment (L89-91), which both named AsyncConsumerServiceImpl only. The sweep also turned up the same stale service-set claim in Program.cs. Its summary (L29-31), the flavor-selector remark (L43-45, which 1641bacf itself rewrote: "hosts both services" / "grpc_server.py registers both") and the DI comment (L100, "both flavors host BOTH services") are fixed in the same commit. One more instance was in `bindings/dotnet/Makefile` (L18, L30-33: "consumer-backend image" and "hosts AsyncConsumerServiceImpl"). 08eddcba touched that file, so it is fixed in cc045b34 (`fixup! build(dotnet): build-grpc-native-dotnet, test-integration-dotnet-{native,ssl,sasl-ssl} (M17/P1)`).

---

## Clean-pass record (§6 checks 1–9)

1. **The merge dropped nothing.**
   - Set-diffs of the `.PHONY` names, Makefile targets, Semaphore job names and backend_pool/backend_factory items between `7690945a^1`, `^2` and the merge all come out empty for "dropped".
   - The remerge-diff shows master's text taken plus unions only (logs are in `target/m17p1/critic/{phony,tgt,names,mk,sem}.*`).
2. **The factories are correct.** All 5 dotnet factories' `needs_container_bootstrap()` return `uses_containers()`, at backend_factory.rs:526/549/591/614/657.
3. **The native contract holds.**
   - GRPC_HOST defaults to 127.0.0.1, and `localhost` maps to Loopback.
   - GRPC_PORT is fail-fast: an invalid host or port exits rc=1 with a clear stderr message (probed).
   - The server prints `listening on host:<bound port>` after `Start()` and flushes. It matches `parse_listening_port` and container `message_on_stderr("listening")`.
   - The S1 panicking arm is transitional and settled.
4. **`DOTNET_GRPC_SKIPS` reaches every dotnet path:** container, ssl, sasl-ssl (all through `test-integration-dotnet`) and native.
5. **CI is correct.**
   - YAML parses.
   - The ssl and sasl_ssl jobs install no host .NET, and native CI is PLAINTEXT only (D6).
   - The Rosetta guard is `arch -x86_64 true || softwareupdate`, gated on `MACOS_ENSURE_ROSETTA`, and `bash -n` passes.
   - `execution_time_limit: 60` is set on exactly the two verify-dotnet jobs.
6. **The stale-phrase sweep is clean** for the plan's list. The misses are 88.1 and 88.2, which key on contracts this phase changed rather than on the listed phrases.
7. **Mode A holds.** There is no ABI or header change: the header SHA is unchanged and the extern count is 668 before and after. There are no new `[DllImport]`s.
8. **§3.5 is clean.** `IConsumerCommon.cs:162-212` and `IOffsetCommitCallback.cs:18-90` state no pre-#211 empty-commit ordering claim.
9. **Master's #208 sentinel metadata reaches C only via the push callbacks** (`send_with_callback` / `send_async` / `send_batch_async`), and .NET binds none of them. #211's empty commit_async enqueues its callback immediately, which the .NET docs do not contradict.

**Independent re-runs** (logs are in `target/m17p1/critic/`):

| Run | Result | Time / notes |
|---|---|---|
| Container-mode SSL | 146/146 passed | 34.35s; dotnet containers confirmed via `docker events` |
| `make test-integration-dotnet-native` | 146/146 passed, rc=0 | 27.78s; no orphans |
| grpc-server `--no-incremental` Release, net8.0 + net10.0 | 0 warnings, 0 errors | |
| `dotnet format --verify-no-changes` (grpc-server) | rc=0 | |

`pgrep -f Confluent.Kafka.GrpcServer` was empty before and after. Only the user's `kafka-perf` container is running.

---

## Re-check clean — fix cycle `076041ed..cc045b34` (95d95074, f7a99ad9, 8226b62a, cc045b34)

Re-check clean, 2026-09-30. No new findings, so `COMMENTS.88.md` stays empty. I checked each fix against the code. `Program.cs:105-132` registers AsyncProducer + AsyncConsumer for async, and Producer + Consumer + Admin for sync. `backend_pool.rs:215-220` `native_command` sets `CONSUMER_FLAVOR`, and the Linux default mode is Container (`:87`). `semaphore.yml:209-225` and `:250`/`:293` carry the exact block and job names, and only the plaintext and macOS verify-dotnet jobs run `install-dotnet.sh`. Install 1 is `--channel 10.0` (SDK 10). The native build is `-f net10.0` (bindings/dotnet/Makefile:54). `grpc_server_async.py:1537` registers Admin. The two Dockerfiles differ in non-comment lines only by `ENV CONSUMER_FLAVOR`, and the D3 / M15/P12 D1 citations exist in their plans. The diff is comment-only: every +/- line is a `#` / `//` / `///` comment, none is tab-prefixed, and `git diff --check` is clean. Mode A holds, with 0 lines under `src/ tests/ bindings/python bindings/c Cargo.*`. `make -n grpc-image grpc-image-async grpc-native` resolves, as do the root `test-integration-dotnet-{ssl,native}`. `bash -n install-dotnet.sh` passes. The grpc-server `--no-incremental` Release build ran CoreCompile on both TFMs with 0 warnings and 0 errors, and `dotnet format --verify-no-changes` gives rc=0. I also replayed the autosquash order in a throwaway worktree (1641bacf, 95d95074, 8226b62a, 8bac2d2a, 08eddcba, cc045b34, 076041ed, f7a99ad9 on 7690945a). It applies cleanly and gives a tree identical to cc045b34. One residual, not filed: `grpc-server/Confluent.Kafka.GrpcServer.csproj:4-12` still describes the server as Producer + Consumer only and never mentions AdminService. It dates from 0204437a4, sits outside this range, and is incomplete rather than false, since it makes no "only" claim. Worth aligning if the file is touched again.

---

## Review record (Manager, at phase close — 2026-09-30)

Per D7, Actor 88 ran S1 (the merge) and then S2 (the features). There was no user gate between
them. The Manager verified each sub-stage's gates from the logs before moving on. Critic 88
reviewed `3b885c1a..076041ed` **once**, the merge included (with `--remerge-diff`). The Actor
fixed the findings with `fixup!` commits, and the Critic re-checked only those.

| Scope | Commit(s) | Critic 88 | Findings |
|---|---|---|---|
| Plan (approved, D1–D9 as recommended) | `35064058` | review: no findings | none |
| S1 — merge `origin/master` `d6bf7c76` (3 textual + 2 semantic conflicts) | `7690945a` | review: no findings (check 1: nothing dropped) | none |
| S2 — server contract (`GRPC_HOST` / `GRPC_PORT=0`, bound-port line, `net8.0;net10.0`) | `1641bacf` + `fixup!` `95d95074`, `8226b62a` | review: 88.1, 88.3 (low); re-check: CLEAN | comment-only fixes |
| S2 — harness native launcher (`native_command`) | `8bac2d2a` | review: no findings | none |
| S2 — Make targets (`DOTNET_GRPC_SKIPS`, ssl / sasl-ssl / native) | `08eddcba` + `fixup!` `cc045b34` | review: no findings; re-check: CLEAN | `cc045b34` was a sweep hit the Actor found itself (same stale claim as 88.3) |
| S2 — CI (3 Linux dotnet jobs, macOS native arm, Rosetta guard, 60-minute limits) | `076041ed` + `fixup!` `f7a99ad9` | review: 88.2 (low); re-check: CLEAN | comment-only fix |

None of 88.1–88.3 touched a ruled decision (D1–D9). All three were stale statements. None was a
behaviour defect.

### Final gates

The last code or config change is `076041ed`. The fix cycle `076041ed..cc045b34` is comment-only
(the Critic checked that every changed line is a comment, and `git diff --check` is clean), so
these gates hold at `cc045b34`.

- **Mode A.**
  - The header SHA-1 is `41f48ea837fd6d1348d649482459990c0847848a`, unchanged across the merge
    and S2.
  - `internal static extern` is **668** throughout. The earlier "697" was reconciled by
    `3b885c1a`, which removed 29 unused flattened-result P/Invokes after the P13.4 close. That
    removal is recorded in the P13.4 STATUS entry.
  - The merge touched no `src/ffi`, `cbindgen.toml`, `build.rs` or `bindings/dotnet` file.
  - `git diff 7690945a cc045b34 -- src/ cbindgen.toml generator/ build.rs Cargo.toml Cargo.lock bindings/python bindings/c`
    is empty. The only `tests/` file S2 touches is `tests/common/backend_pool.rs`, a recorded
    exception (PLAN §5).
- **Unit tests.** 2927/2927 on net10.0 and 2927/2927 on net8.0, on the base, after the merge,
  and after S2. The library is untouched.
- **Arms.**
  - 152 `__grpc_dotnet*` arms at the merge: 116 sync (80 admin, 36 producer/consumer) and 36
    async.
  - The three transaction skips remove 6, so **146 execute** per protocol. They were predicted
    from the stored list (`target/m17p1/s2/expected-146.txt`) and reconciled by name after each
    run, with no surplus.
- **Native mode, local, macOS arm64 (net10.0 server).** Plaintext **146 passed, 0 failed**
  (29.21 s), ssl **146 / 0** (28.08 s), sasl_ssl **146 / 0** (28.43 s). 576 were filtered out in
  each run. `pgrep` was empty before and after.
- **Container mode, local, linux/amd64 emulated (net8.0 images).** Plaintext **146 / 0**
  (33.21 s), ssl **146 / 0** (34.05 s), sasl_ssl **146 / 0** (34.47 s).
  - The fresh `.so` (sha256 `9864fc96…`, different from P13.4's `4cc23659…`) was cross-built at
    the S2 HEAD, and all 668 `EntryPoint`s resolve in it.
  - Both images were rebuilt, and each carries the staged sha256.
- **Native artefact.** `target/grpc-native/dotnet/` holds `Confluent.Kafka.GrpcServer.dll` and
  a `libconfluent_kafka.dylib` whose sha256, `d29266ec…`, equals `target/release`'s.
- **Build and format.** grpc-server builds for net8.0 and net10.0 with 0 warnings and 0 errors,
  `dotnet format --verify-no-changes` is clean, and `cargo xtask format-check` / `lint` pass.
  The `semaphore.yml` YAML parses, and `bash -n` passes on both edited shell scripts.
- **Critic 88's independent re-runs:** container ssl 146/146, native plaintext 146/146, and the
  fixups replayed in autosquash order in a throwaway worktree. The replay applied cleanly and
  matches the `cc045b34` tree exactly.

### CI-only items (pending the user's push, per PLAN §7.3)

The phase closes with these open, as M8/P1 did. If the first CI run is red on one of them, the fix
is a `fixup!` inside this phase's record, not a new phase.

1. **A job-level `execution_time_limit: 60` inside a block whose limit is 30.** Semaphore does
   not document whether a job limit may exceed its block's. This is the highest risk.
2. **Rosetta on the Semaphore macOS agent,** and whether `sudo softwareupdate` is allowed there.
   It is guarded by `MACOS_ENSURE_ROSETTA`. The fallback is D4's alternative, Homebrew's arm64
   `protoc`.
3. **The real job durations** against the limits. After the first green run, tighten them to
   about twice the measured time.
4. **Native mode on Linux.** No CI job runs it, as for master.

### Critic observations, recorded rather than acted on

- **O1.** `grpc-server/Confluent.Kafka.GrpcServer.csproj:4-12` describes the server as Producer +
  Consumer and never mentions AdminService. It dates from `0204437a4` and is outside this range.
  It is incomplete rather than false, since it makes no "only" claim. Align it the next time that
  file is touched.
- **O2.** The Manager's briefs called the binding-root `COMMENTS.DONE.88.md` "gitignored". It is
  not: `.gitignore`'s `COMMENTS\.[0-9]*\.md` matches only `COMMENTS.<N>.md`, which
  `bindings/dotnet/CLAUDE.md` §8.4 already states. It stayed out of every commit by discipline.
  This archived copy is the tracked record.

### Rule suggestions for the user (agents do not edit rule files)

- **S1.** Never `cut -d:` libtest `--list` output. A test name that contains `::` is truncated.
  Strip the `: test` suffix instead.
- **S2.** `grep -c` exits 1 on zero matches, so it must not gate a `set -e` script or an `&&`
  chain.
- **S3.** `bindings/dotnet/CLAUDE.md` could state once that Grpc.Tools has no macOS-arm64 protoc:
  Apple Silicon runs `macosx_x64` under Rosetta, and the SIGSEGV is `linux_arm64`-only. The same
  file could name `make test-integration-dotnet-native` as the local native gate. (PLAN §11.)
- **S4.** A `dotnet format --verify-no-changes` gate for `grpc-server`, which is outside the
  `.sln`, so `make test-dotnet` does not format it. (PLAN §11.)
- **S5.** Key stale-claim sweeps on **each contract a phase changes**, not only on a list of
  phrases, and include files the phase did not edit. 88.1 and 88.2 were contract misses, and the
  `cc045b34` hit was in a file S2 had edited only for another reason.
- **S6** (Critic). The hosted-service and flavor set is restated in five places: `Program.cs`,
  both Dockerfiles, `bindings/dotnet/Makefile` and the csproj. The rulebook could name one of
  them as authoritative, or require that a change to the service set sweeps all five.
