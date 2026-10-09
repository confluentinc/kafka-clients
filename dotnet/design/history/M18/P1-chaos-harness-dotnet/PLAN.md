# M18/P1 — .NET backends for the chaos / fault-injection harness (upstream PR #184)

> **Approved by the user on 2026-10-09.** The user's rulings, and the Python checks that two of
> them depend on, are in the [Approval record](#approval-record). D10 and D11 were reshaped to
> match Python, and the sections below already reflect that. Written by the Manager on
> 2026-10-09.

**N = 94.**
- `dotnet/design/current/STATUS.md:35` gives the next unused N as 94 (M11/P4.2 used 93).
- Neither `COMMENTS.94.md` nor `COMMENTS.DONE.94.md` exists.
- No `M18` directory exists. The only "M18" under `dotnet/design` is mutation-row id M18 in the
  M11/P4.2 plan (`:422`), not a milestone.
- **Milestone 18 is new** (D1).

**Branch:** `prashah_dev_dotnet_binding`.

**Bases** (verified 2026-10-09):
- HEAD is `918219ac`, the merge of PR #201 rebased on master `1f4afc36`.
- `origin/master` is `5d4c6b80`, which is #184, committed 2026-10-09 11:20 +0530.
- The merge-base is `1f4afc36`, so **#184 is the only commit we lack**.

**Mode A.** No ABI function, header change or `rust/src` change. #184 itself touches no `rust/src`
file (§2, row 2). There are two recorded scope notes:
1. **The S0 merge brings in upstream test infrastructure** (the chaos harness, the C and Python
   chaos servers). This phase did not write it.
2. **The `dotnet-actor` edits upstream-owned harness files** under a recorded exception (§9):
   `rust/tests/chaos/workload.rs`, `config.rs`, `rust/xtask/src/main.rs`, the chaos README and
   `design/current/chaos-parity-gap.md`. Every edit copies the `python-async` / `c` arm, swapping
   only the label and the `BackendKind`.

**The request:** add .NET backends to the chaos harness that #184 introduced, i.e. a .NET
`ChaosWorkloadService` in the gRPC server, plus the harness wiring that lets
`--workload producer:dotnet` and the other .NET combinations run.

---

## 0. Scope

**In scope:**
1. **S0.** Bring #184 (`5d4c6b80`) into the branch (§4, D2).
2. **S1. Harness wiring (Rust):**
   - add `Backend::Dotnet` / `Backend::DotnetAsync` to the chaos harness;
   - update the config error text, xtask's feature selection and help text;
   - update the README and the parity doc (§6).
3. **S2. The sync .NET server:** `ChaosWorkloadServiceImpl` over `KafkaProducer` /
   `KafkaConsumer`, with the shared plumbing and unit tests (§5, §7.1).
4. **S3. The async .NET server:** `AsyncChaosWorkloadServiceImpl` over `AsyncKafkaProducer` /
   `AsyncKafkaConsumer`, with unit tests.
5. **S4. End-to-end evidence, reduced to match Python (D11):** at the S3 HEAD, re-run the sync and
   async native smoke runs, then do one container smoke run on this Mac (§7.5). There is no chaos
   matrix.

**Out of scope** (§15):
- chaos in CI;
- any core or ABI change;
- any change to the Python or C chaos servers;
- the upstream-stale README passages (reported in §13 instead);
- a fix to the `IDeliveryCallback` doc inaccuracy found while planning (§12, R3).

**Expected size:** see §14.

---

## 1. Standing constraints (relay verbatim to the Actor and the Critic)

- **Pushing and messages:**
  - No push. The user pushes.
  - No messages to anyone.
  - No `gh` write operations.
- **Files that are not ours to edit:**
  - Do not edit untracked files in the tree; they are the user's notes. That includes
    `PendingAdminClientFindingsForDotnet.md`, the `Dotnet-AdminClient-Findings-Workflow/` folder
    and the repo-root `*.md` notes.
  - Local notes stay untracked through `.git/info/exclude`, never `.gitignore`.
  - Never stage a `COMMENTS.DONE.<N>.md` at the binding root.
  - Agents do not edit rule files (`CLAUDE.md`, `dotnet/CLAUDE.md`, `.claude/rules/*`,
    `dotnet/.claude/rules/*`). Put rule suggestions in the report.
- **Remote hosts:** never ssh or scp to the user's machines. Hand over the commands instead.
- **Paths:**
  - Never read code from, or write anything under, `.claude/worktrees/`.
  - Use absolute paths and `git -C <main tree>`.
  - Never use a bare `cd`; use a subshell, `(cd <dir> && …)`.
- **Reading budget:** use grep and targeted line ranges, not whole large files. Bound every tool
  output with `| head` or `cut -c1-200`. `verifier.rs` is 4,496 lines and `config.rs` is 1,938:
  read neither in full.
- **Shell traps on this machine** (each has produced a false PASS before):
  - Start each Bash call with
    `export PATH="/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin:$HOME/.cargo/bin:$HOME/.dotnet:$PATH"`.
  - For cargo, set `RUSTUP_TOOLCHAIN=1.95.0-aarch64-apple-darwin`.
  - `grep` is ugrep, so use `/usr/bin/grep`. There is no `sed`; use awk. `cat` is bat, so use
    `/bin/cat`.
  - zsh does not split an unquoted `$var`, and globs must be quoted (`'--include=*.cs'`). `$T:t…`
    is a zsh modifier, so write `${T}:path`.
  - Set `PYTHONDONTWRITEBYTECODE=1`.
  - A test filter that matches nothing "passes", so check the executed-test count.
  - An aborted `dotnet test` can exit 0, so grep for `Test Run Aborted`.
  - Use `~/.dotnet/dotnet`: SDK 10.0.400, runtimes NETCore 8.0.30 and 10.0.11, and
    **ASP.NET Core 10.0.11 only, no ASP.NET Core 8** (verified 2026-10-09). The gRPC server
    therefore runs locally only on net10.0. See D10.
- **Stale native artifacts.** Rebuild the release dylib before any Release test or native run.
  `make build-grpc-native-dotnet` does this through `build-rust-all-features`.
- **Chaos-run hygiene:**
  - **Before and after every chaos or integration run**, there must be no leftover `kafka-*`
    broker containers or chaos networks (`docker ps -a`, `docker network ls`), and no orphan
    native servers (`pgrep -f Confluent.Kafka.GrpcServer`).
  - **Run one chaos run at a time.** Each run owns a dedicated Docker cluster, and `chaos-matrix`
    takes a `runner.lock`.
  - Never run chaos at the same time as the integration suite, because they compete for the host.

---

## 2. Verified facts (Manager, 2026-10-09, at `918219ac` / `origin/master 5d4c6b80`)

| # | Fact | Source |
|---|---|---|
| 1 | `git merge-tree --write-tree HEAD origin/master` exits 0, i.e. a **clean merge**. 33 files come in. Four of them changed on both sides since `1f4afc36` and auto-merge: `c/grpc_server/server.cc`, `python/grpc_server.py`, `python/grpc_server_async.py`, `rust/tests/common/backend_pool.rs`. | dry run |
| 2 | #184 is +22,599 / −11 lines across 33 files. **No `rust/src` file**, no header change, no `dotnet/` file. | `git diff --stat HEAD...origin/master` |
| 3 | The contract is `chaos_service.proto` (291 lines, package `confluent.kafka.test`, imports `producer_service.proto`): 4 RPCs, a 14-variant `WorkloadEvent` oneof, `CommitMode`, `RebalanceKind`, `ConsumerOp`. | proto |
| 4 | `dotnet/Dockerfile.grpc` and `Dockerfile.grpc.async` copy the **whole** proto directory, so no Dockerfile change is needed. The csproj lists protos one by one (`Confluent.Kafka.GrpcServer.csproj:73-80`), so it needs **one new `<Protobuf>` line**. | csproj, Dockerfiles |
| 5 | `BackendKind::Dotnet` / `DotnetAsync` exist **only on our branch** (`rust/tests/common/backend_pool.rs:115-120`, native launch `:205-220`). Master's `backend_pool.rs` mentions `dotnet` 0 times. | grep |
| 6 | In the harness, `Backend` is matched in exactly 3 places, all in `workload.rs`: `parse` `:68-76`, `label` `:78-85`, and `build_grpc_workload` `:355-360`. Other places that name backends: `config.rs:1200` (error text), `xtask/src/main.rs:871-880` (`chaos_test_feature`), `:773` (help), `:667` (println), `:2259-2270` (test). `chaos_matrix.rs` builds through `crate::chaos_test_feature`. | grep |
| 7 | **Chaos is not in CI.** It has no root Makefile target and no `.semaphore` entry. `[[test]] name = "chaos"` is `test = false`, and every scenario is `#[ignore]`. It runs only through `cargo xtask chaos` / `chaos-matrix`. | Makefile, semaphore, Cargo.toml |
| 8 | The gRPC servers run natively when `MULTILANG_BACKEND_MODE` is unset on a non-Linux host (`config.rs:1011-1017`). A native server uses the host listeners, so all four security protocols work. A container server cannot use `sasl_plaintext` (`config.rs:428-450`, a generic check that already covers dotnet). | config.rs |
| 9 | Python caps only the **sync** Python server's workers (`PYTHON_SERVER_WORKERS` = 64, with 8 reserved; `config.rs:53-62`, `:1085-1100`). The doc says *"The asyncio server (`python-async`) and the C server have no fixed pool."* .NET needs no cap (D8). | config.rs |
| 10 | **Neither Python nor C has unit tests for its chaos server.** The only chaos server-side tests are Rust's `remote_workload.rs` tests (`:556-1100`), which run the client against a fake tonic server. | `git grep -l chaos origin/master` |
| 11 | Docker 29.8.0 is up, aarch64, 18 CPUs. | `docker info` |
| 12 | The .NET listener is a sync `void` interface (`IConsumerRebalanceListener.cs:88-110`, three methods with no defaults). `ConsumerHandle` exposes `Commit()` / `Committed(...)` / `Assignment()` (`ConsumerHandle.cs:336`, `:404`), checks only that it is not disposed, and ref-counts the consumer (ffi §B2 Category 6). | binding |
| 13 | `Committed(...)` **omits** partitions that have no committed offset (`IConsumer.cs:264-265`, `ConsumerHandle.cs:327`), as Python's `committed` does. | binding |
| 14 | `IDeliveryCallback.OnCompletion(RecordMetadata metadata, KafkaException? exception)`. On failure, `metadata` is a non-null placeholder (ffi §A6 form C). | `IDeliveryCallback.cs:358` |
| 15 | The other .NET servicers pick a `Mock*` client when the config is empty (`ProducerServiceImpl.cs:124-133`). **Python's chaos server always builds the real client** (`grpc_chaos.py:354`, `:409`, `:652`, `:713`), and the proto says "forwarded verbatim". | grep |
| 16 | Server convention: messages the server makes up itself are prefixed `dotnet server:`, `LocalIllegalStateCode = -4` (`Translate.cs:138`), and logging goes to `Console.Error`. Header: `LOCAL_ILLEGAL_ARGUMENT = -3`, `LOCAL_ILLEGAL_STATE = -4`. | Translate.cs, header |

---

## 3. Parity anchor (pinned up front; the Critic reviews against this section)

**The anchor is the proto contract, then `python/grpc_chaos.py` (857 lines), then the C chaos
section of `c/grpc_server/server.cc` (about `:4904-6018`), all at `origin/master 5d4c6b80`.**
Where Python and C differ, the proto decides. If the proto is silent, use Python.

### 3.1 Flavour mapping

| Harness backend (CLI) | `BackendKind` | Server process | .NET servicer | Mirrors |
|---|---|---|---|---|
| `dotnet` | `Dotnet` (`CONSUMER_FLAVOR=sync`, image `dotnet-grpc-server`) | `Confluent.Kafka.GrpcServer` | `ChaosWorkloadServiceImpl`: `KafkaProducer` / `KafkaConsumer`, one dedicated thread per workload | `ChaosWorkloadService` (Python sync), plus C's worker-thread-and-join |
| `dotnet-async` | `DotnetAsync` (`CONSUMER_FLAVOR=async`, image `dotnet-async-grpc-server`) | same binary | `AsyncChaosWorkloadServiceImpl`: `AsyncKafkaProducer` / `AsyncKafkaConsumer`, one `Task` per workload | `AsyncChaosWorkloadService` (Python asyncio) |

### 3.2 RPC by RPC

| RPC | Contract (proto) | Python sync | Python async | C | .NET (both flavours) |
|---|---|---|---|---|---|
| `RunProducer` / `RunConsumer` | Register the id, send headers **at once**, build the client from `config` verbatim, run the loop, stream batches, end with `Finished`. A cancelled RPC stops the workload and closes the client. | `_run`: registry add → `send_initial_metadata` → `add_callback(stop.set)` → daemon thread → batches → `finally: stop.set(); remove`. **Does not wait for the thread.** | Same, with a task, and `finally: … await asyncio.shield(task)`, so it **waits for the drain**. | Worker thread; checks `IsCancelled()` every 100 ms; **joins** the worker; returns `CANCELLED` if the harness went away. | Register → `WriteResponseHeadersAsync` → cancellation registration sets stop → start the workload → batches → `finally`: stop, remove, **await the workload's completion**, which is not cancellable (D8, §5.3). |
| Duplicate `workload_id` | (unspecified) | One batch: `Failed{LOCAL_ILLEGAL_ARGUMENT, "python server: workload_id 'x' is already running"}` | same | same shape | Same: `-3`, `"dotnet server: workload_id 'x' is already running"` |
| `StopWorkload` | Returns at once. An unknown id is OK. | `registry.stop(id)` → `StatusResponse()` | same | same | same |
| `MarkWorkload` | Queue a `Marker` FIFO behind every event queued so far; return `{found}`. | `emit(_marker)` into the same `SimpleQueue` | emitted on the loop thread | same | Write the `Marker` into the same `Channel` (§5.2); `found` = whether the id is registered |

### 3.3 Producer loop (both flavours; the per-flavour differences are in italics)

| Step | Python | .NET |
|---|---|---|
| Construct | `KafkaProducer(dict(config))`. On failure: log, `Failed(e)`. | `new KafkaProducer<byte[], byte[]>(config, ByteArray, ByteArray)` / *`AsyncKafkaProducer`*, verbatim with no mock switch (row 15). On failure: log, `Failed(e)`. |
| Rate | `interval = 1/target_rps` (0 = max). `next_due += interval`. If ahead, wait (*sync: `stop.wait`, async: plain sleep, then `continue`*). If more than **1 s** behind, `next_due = now`. | The same schedule on a `Stopwatch`. *Sync: wait on the stop handle. Async: delay that resumes early on stop* (§5.5; this differs from Python async's plain sleep, recorded in §3.5). |
| Per record | `emit(Sent(i))` **before** `send`, then `send(record, on_delivery)`, not awaited. *Async: `await producer.send(...)` awaits admission only.* | `emit(Sent(i))` **before** `Send`, then *sync:* `_ = producer.Send(record, cb)`; *async:* `await producer.Send(record, cb, CancellationToken.None)`, dropping the `AsyncKafkaFuture`. **Never the stop token** (D6). |
| Throw from send | `_RecordOutcomes.send_raised` → `SendFailed` only if no callback has fired yet | Same state machine (D5) |
| Callback | `_outcome`: error → `SendFailed`; neither → `SendFailed{-4, "… neither metadata nor error"}`; else `Delivered{i, partition, offset}`. An exception while building the event → `Failed`. | Same, in `IDeliveryCallback.OnCompletion` on the pump thread: **enqueue only** (§5.7) |
| Loop end | `ProducerStats{sent = index, elapsed = max(t, 1e-9)}`, **before** close | same |
| Close | `producer.close()` waits until every buffered record's outcome is known, then `Finished`. A close error → `Failed`. A loop crash → close quietly, then `Failed`. | `Close()` / *`await Close()`* then dispose, then `Finished`. A close error → `Failed`. A loop crash → close quietly, then `Failed`. |

### 3.4 Consumer loop (both flavours)

| Step | Python | .NET |
|---|---|---|
| Construct, handle, listener | `KafkaConsumer(config)`, then `handle = consumer.handle()`, then `_ChaosRebalanceListener(emit, handle, check)` | `new KafkaConsumer<byte[], byte[]>` / *`AsyncKafkaConsumer`*, then `consumer.Handle()`, then `ChaosRebalanceListener` |
| Subscribe | `subscribe(topics, listener)`. On failure: close (including the handle), then `Failed`. | same (*async: `await Subscribe`*) |
| Poll | `poll(timeout)`. On error: `ConsumerError{POLL}`, back off **100 ms** (wakes on stop), `continue`. Empty: `continue`. | same, **with no stop token on `Poll`** (D7) |
| Records | One event per record: `Consumed{index, topic, partition, offset}`, or `Corrupted{topic, partition, offset, detail}` | same, with the exact `check_record` texts (§3.6) |
| Commit | SYNC: `commit()`. ASYNC: `commit_async(callback)`, whose callback emits `ConsumerError{COMMIT}` on failure. An exception → `ConsumerError{COMMIT}`, `continue` (skipping the read-back). | SYNC: `Commit()` (*`await Commit()`*). ASYNC: `CommitAsync(IOffsetCommitCallback)` (a sync call in both flavours). |
| Periodic read-back | Only after a **successful SYNC** commit, when `check = commit_check_interval_ms > 0`, every interval: `Committed` events for `committed(assignment())`. An error → `ConsumerError{READ_COMMITTED}`. | same |
| Stop | Final `commit()` (sync, in **both** modes). On error `ConsumerError{COMMIT}`; otherwise, if `check`, read back. Then `listener.closing = True`, `ConsumerClosing`, close, `ConsumerClosed`, `Finished`. A close error → `ConsumerError{CLOSE}`, not `Failed`. | same; `Closing` is a `volatile` field (written by the workload, read on the dispatcher thread) |
| Close and handle | `_close_consumer_sync`: the close spec, **then** `handle.destroy()`, **then** `consumer._destroy()`. This hack exists because Python's `close()` destroys the native consumer at once. | **No hack:** `Close()` → `handle.Dispose()` → `consumer.Dispose()`. The close-time revoke still commits through the live handle. The handle ref-counts the parent, so the final destroy is the immediate §B2 path 1 (§5.6). |
| Listener: assigned | `Rebalance{ASSIGNED}` | same |
| Listener: revoked | `Rebalance{REVOKED}`, then `handle.commit_sync()`. On error: `ConsumerError{REVOKE_COMMIT}`, return. Then, if `check and not closing`, `Committed` events for `handle.committed(partitions)`; on error `ConsumerError{READ_COMMITTED}`. | same |
| Listener: lost | `Rebalance{LOST}` only. This is implemented, not left to the default that delegates to revoked. | same (the .NET interface has no default anyway) |
| Rebalance event | Partitions sorted by `(topic, partition)`; `observed_at_unix_nanos` = wall clock at callback entry | Same; topic order uses `string.CompareOrdinal` |

### 3.5 Deliberate .NET differences: the mechanism differs, the behaviour does not

1. **No worker cap.** The sync flavour uses dedicated threads and the handler is async, so there is
   no fixed pool to exhaust (D8). `config.rs` gets no `check_dotnet_server_streams`.
2. **No "yield every 10 records"** (Python `_ASYNC_YIELD_EVERY`). Python yields because its event
   loop is single-threaded. The .NET async loop runs on the multi-threaded pool, so a loop that
   never yields delays no other workload.
3. **The async rate wait resumes early on stop.** Python async sleeps one interval without waking.
   This only shortens stop latency at low `--rps`.
4. **No close-spec split** (§3.4). The handle ref-count makes the plain order correct.
5. **The settle-once guard is unreachable in .NET.** It is kept for parity (D5, §5.5.3).
6. **Server-side drain wait.** Like Python async and C, and unlike Python sync, the RPC handler
   waits for the workload to finish draining.

### 3.6 Constants and exact strings (shared with `rust/tests/chaos/workload.rs` `check_record` / `build_value`)

- `_MAX_BATCH` is 4096. The poll error backoff is 100 ms. The maximum schedule lag is 1 s.
- **Key:** the 8-byte big-endian index.
- **Value:** the key's 8 bytes zero-padded to `msg_size`, or the first `msg_size` bytes when
  `msg_size < 8`. A fresh buffer is allocated **per record** (§5.5.4).
- `Corrupted.detail` texts, character for character:
  - `key is missing (expected the 8-byte index)`
  - `key is {n} byte(s), expected the 8-byte index`
  - `value is missing (expected {msg_size} byte(s) encoding index {i})`. A missing value with
    `msg_size == 0` is **OK**.
  - `value of {len} byte(s) does not match the producer's encoding of index {i} ({msg_size} byte(s))`
- `ConsumerOp`: POLL=0, COMMIT=1, REVOKE_COMMIT=2, READ_COMMITTED=3, CLOSE=4.
- Server-made errors:
  - `-3` `dotnet server: workload_id '{id}' is already running`
  - `-4` `dotnet server: delivery callback fired with neither metadata nor error`
  - any non-Kafka exception goes through `Translate.ToProto(Exception)`: `-4`,
    `dotnet server: <Type>: <msg>`

---

## 4. S0 — bringing #184 into the branch (D2)

**Recommendation: merge `origin/master` (`5d4c6b80`) directly, as a `--no-ff` merge commit. Do not
wait for a rebased #201.**

**Why:**
- Our branch already contains master's previous head `1f4afc36` (through rebased #201
  `9d6a8a32`), so #184 is the only commit missing.
- The dry run is clean (§2, row 1).
- #184 touches no `rust/src` or `dotnet/` file.
- Waiting would block this phase on an external rebase that brings nothing #184 needs.
- When #201 is later rebased onto a master that contains `5d4c6b80`, merging it here is a normal
  merge, because `5d4c6b80` is then a common ancestor.

**Alternative:** wait for #201 to be rebased onto `5d4c6b80` and merge that, keeping the established
"take master through rebased #201" route. That costs an unbounded wait, and it may pull in unrelated
#201 rework.

**Who:** the PM, because the merge needs no authoring (precedent: M15/P13.1 and P13.3). If a
conflict appears, which would mean master moved, stop and hand it to `dotnet-actor` 94.

**Steps:**
1. `/usr/local/libexec/airlock-agent/git -C <main> fetch origin`. Confirm that
   `origin/master == 5d4c6b80`. **If master moved, stop and re-plan S0.**
2. Confirm the working tree is clean apart from `??` entries and the pre-existing
   `.claude/agent-memory/project-manager/MEMORY.md` modification.
3. `git -C <main> merge --no-ff origin/master -m "Merge 5d4c6b80 (master: #184 chaos harness) into prashah_dev_dotnet_binding"`.

**S0 gates** (the PM runs them; baselines are recorded for S1 to S4):
1. **Mode A:**
   - `git diff HEAD^1 HEAD -- rust/src dotnet` is empty.
   - The header SHA-1 and the dotnet `EntryPoint` count are unchanged from `918219ac`. Record both
     values.
2. From `rust/`:
   - `cargo build`.
   - `cargo test --features integration-tests --test chaos` and
     `cargo test --features multilanguage-tests --test chaos`. These are the non-ignored unit
     tests (config, verifier, remote_workload). **Record both executed counts.**
   - `cargo test -p xtask`. Record the count.
   - `cargo xtask format-check` and `cargo xtask lint`.
3. The .NET side does not change in S0, so `make test-dotnet` is not required at S0. It is
   required from S2 on.
4. **Harness baseline on this machine:** run
   `(cd rust && cargo xtask chaos --cycles 1 --reports)` (Rust only), and then one gRPC row,
   `--workload producer:rust --workload consumer:c --cycles 1` (native C), if
   `make build-grpc-native-c` builds. This separates "the harness or cluster does not work here"
   from any later .NET failure. Record the verdict and the wall time.

---

## 5. .NET server design (`dotnet/grpc-server/`)

### 5.1 Files and registration

| File | Holds |
|---|---|
| `Chaos/ChaosEvents.cs` | Record encoding (`Key`, `Value`) and `CheckRecord` (the §3.6 texts); one static builder per `WorkloadEvent` variant; `DuplicateId`; terminal detection |
| `Chaos/ChaosRegistry.cs` | `workload_id → (stop signal, emit)`, with `TryAdd` / `Get` / `Stop` / `Remove(id, entry)` / `StopAll` |
| `Chaos/ChaosStream.cs` | The shared RPC body (§5.3): channel, headers, batching, cancellation, drain-await |
| `Chaos/ChaosRecordOutcomes.cs` | The per-record `IDeliveryCallback` and the settle-once state (§5.5.3) |
| `Chaos/ChaosRebalanceListener.cs` | `IConsumerRebalanceListener` over the `ConsumerHandle` (§3.4), plus the `IOffsetCommitCallback` |
| `ChaosWorkloadServiceImpl.cs` | Sync flavour: `ChaosWorkloadService.ChaosWorkloadServiceBase`, with the producer and consumer loops on dedicated threads |
| `AsyncChaosWorkloadServiceImpl.cs` | Async flavour: the same base, with the loops as `Task`s |

Other changes:
- **csproj:** add
  `<Protobuf Include="$(ProtoRoot)/chaos_service.proto" ProtoRoot="$(ProtoRoot)" GrpcServices="Server" />`.
- **`Program.cs`:**
  - In the sync branch, `AddSingleton<ChaosWorkloadServiceImpl>()` and `MapGrpcService`; in the
    async branch, the same for `AsyncChaosWorkloadServiceImpl`.
  - `DrainServicer` also disposes the chaos servicer. `Dispose` calls `StopAll()` and then waits,
    with a bound, for the registered workloads (30 s, matching the client close timeouts).
  - Update the flavor-selector xmldoc (`:36-48`) to say the chaos service follows the flavor too.
- **DoD §7, new types:** these are test-server scaffolding with no Java class. Each mirrors a named
  Python or C structure (`_Registry`, `_RecordOutcomes`, `_ChaosRebalanceListener`, `_run`), and
  each file's header says which one.
- **Test seam (D10):** each servicer has an `internal` constructor taking client factories
  (`Func<IReadOnlyDictionary<string,string>, IProducer<byte[],byte[]>>` and the consumer and async
  equivalents), plus a public parameterless constructor that builds the real clients.
  - DI uses only public constructors, so production cannot pick up the seam.
  - Add `InternalsVisibleTo` for the test project.
  - Per DoD §12, the tests drive **the same loop code**; only construction is swapped.

### 5.2 Registry, event channel and FIFO order

- **One `Channel<WorkloadEvent>` per workload:**
  `Channel.CreateUnbounded(new UnboundedChannelOptions { SingleReader = true, SingleWriter = false, AllowSynchronousContinuations = false })`.
- **There are many writers:** the workload thread or task, the producer's **pump thread**
  (delivery callbacks), the consumer's **dispatcher thread** (the listener and the commit callback)
  and the `MarkWorkload` handler.
  - `TryWrite` is linearizable, so the stream order is the order in which the client observed
    events. This is the proto's ordering rule.
  - `TryWrite` never blocks a binding thread.
- **`AllowSynchronousContinuations = false` is required.** Without it, the reader's continuation,
  meaning the gRPC write, could run inline on the pump or dispatcher thread. That would stall every
  other completion, the same hazard that `RunContinuationsAsynchronously` prevents in the binding
  (ffi §A7 / §B7).
- **Listener events come before that poll's records:**
  - sync: the listener runs on the dispatcher thread while `Poll` waits for it, so its `TryWrite`
    happens before `Poll` returns;
  - async: the listener job runs before the poll's completion job on the single serialised
    dispatcher.
  - A unit test asserts this for both flavours (T15).
- **Unbounded** is the same as Python's `SimpleQueue` and C's queue. A harness that stops reading
  grows memory. This is accepted and recorded as R13.
- The registry removes an entry only if it is still the one it added (`Remove(id, entry)`), so a
  re-registered id cannot be removed by the stream that owned it before.

### 5.3 The shared RPC body (`ChaosStream.Run`)

1. **Register.** On a duplicate, write one batch with the duplicate `Failed` and return. Do this
   **before** sending headers, as Python does: the duplicate batch carries the headers.
2. `await context.WriteResponseHeadersAsync(new Metadata())` **at once**. The harness's call
   resolves on the headers (proto lifecycle step 1; `backend_pool.rs` `streaming_channel()` has no
   timeout because of this).
3. `context.CancellationToken.Register(RequestStop)`: if the harness goes away, the workload stops.
4. Start the workload:
   - sync: a dedicated `Thread`, whose completion is a `TaskCompletionSource` created with
     `RunContinuationsAsynchronously`;
   - async: `Task.Run`.
   - Both are wrapped in a catch-all that emits `Failed(e)` and never leaves the stream open
     (Python `_guarded`).
5. **Loop:**
   - read one event (`await ReadAsync(context.CancellationToken)`);
   - then `TryRead` up to **4096** events;
   - then `await responseStream.WriteAsync(batch)`; there is one write at a time, as gRPC requires;
   - if the batch holds `Finished` or `Failed`, return.
6. **`finally`:**
   - request stop;
   - `registry.Remove(id, entry)`;
   - **`await` the workload's completion with no token.** On a normal end it is already finished.
     After a cancellation it is draining: the producer closes, the consumer commits and closes. So
     the client is closed before the handler returns (C / Python async parity; §3.5 item 6).
7. **The stop signal must never resume a waiter inline on the thread that requests the stop.** That
   thread is the `StopWorkload` handler or the RPC-cancellation callback. If it resumed a waiter
   inline, the async producer's continuation could run its whole drain, including
   `await producer.Close()`, inside the `StopWorkload` call. Use either:
   - a stop `TaskCompletionSource` with `RunContinuationsAsynchronously` for the async waits, plus a
     `ManualResetEventSlim` or `WaitHandle` for the sync thread; or
   - an equivalent that has a test.

   Do not use `await Task.Delay(d, cts.Token)` on a CTS that a handler cancels inline. T6b covers
   this.

**Message size.** 4096 events of the largest kind (`Corrupted` with a long detail and a 249-char
topic name) stay well under tonic's default 4 MiB decode limit, and the same constant already works
for Python and C. This is recorded as low risk (R13).

### 5.4 `StopWorkload` / `MarkWorkload`

- **Stop:** `registry.Stop(id)` and return `new StatusResponse()`. An unknown id is OK. It never
  waits for the drain.
- **Mark:** `registry.Get(id)`. If the id is unknown, return `found = false`. Otherwise
  `emit(Marker(marker))` into the same channel, which puts it FIFO behind every event written
  before, and return `found = true`.
- A mark that arrives after `Finished` was queued, but before the entry is removed, returns `true`
  and is never streamed. This is the same as Python (it is not an error). The harness stamps by
  stream position.

### 5.5 Producer loop

#### 5.5.1 Sync (`KafkaProducer`, dedicated thread)

This is §3.3 verbatim:
- The rate wait is `stopHandle.Wait(TimeSpan)`, which returns early on stop.
- `Send` returns once the record is accepted. It blocks only on metadata or `buffer.memory`, for up
  to `max.block.ms` (60 s in the harness). Stop is checked between sends, as in Python.
- The `KafkaFuture` it returns is dropped. The outcome comes through the callback.

#### 5.5.2 Async (`AsyncKafkaProducer`, `Task`)

- `await producer.Send(record, cb, CancellationToken.None)`.
  - The outer `ValueTask` is admission. It completes synchronously while the bound has room.
  - The inner `AsyncKafkaFuture` is dropped. If its delivery `Task` faults, nobody observes the
    fault. That is benign (no `UnobservedTaskException` handler is installed), and the outcome is
    already reported through the callback (R6).
- **The stop token is never passed to `Send`, `Flush` or `Close`** (D6).
  - Reason (M11/P3.5 D2 (c)): a caller token that fires while stage 1 waits ends that stage with
    `OperationCanceledException`, **and the record is still sent**, so its callback fires later.
  - If the stop token were linked, a stop during admission would produce `SendFailed` (from the
    throw) followed by a real outcome. That is a false red in the verifier.
  - With `CancellationToken.None`, a stop requested during admission takes effect after the
    admission completes, as in Python async.

#### 5.5.3 The settle question: can a `Send` that throws still fire its callback later?

**In .NET, no, with one exception that this design excludes.**

- **The callback is managed-only** (ffi §A6 form C). It fires only where the binding reads a core
  completion: the pump's `_get` / `_get_all`. A `Send` that throws hands the pump no future:
  - sync: `Producer_send` returned an error and no future, or the pre-enqueue window destroyed the
    future unread (residual 4);
  - async: it throws only `ArgumentNull`, `ObjectDisposed`, `Serialization` or an already-canceled
    `OperationCanceledException`, all before the append.
  - So **a throw means no callback, ever** (`IDeliveryCallback.cs:126-156`, decision D5).
- **The only "throws, yet the record is sent and the callback fires" case is the stage-1
  `OperationCanceledException` from the caller's token** (D2 (c)). §5.5.2 rules it out by passing
  `CancellationToken.None`.
- **A remaining ambiguity, which is not a double settle:**
  - Where it comes from: the core's post-append error path. `do_send_bytes` →
    `maybe_add_partition` can fail **after** `accumulator.append`, for example for an idempotent
    producer whose `TransactionManager` is in an error state (`transaction_manager.rs:4702-4704`,
    `kafka_producer.rs:1889-1920`; documented as ambiguous at `ffi/producer.rs:700-708`).
  - The sync .NET `Send` then throws a `KafkaException` even though the record is in a batch and
    may be delivered, but no .NET callback ever fires, because no future reached the pump.
  - So the outcome is **settled once, as `SendFailed`**. The verifier fails a run on any
    `SendFailed` anyway, and the in-process Rust workload records `SendFailed` on the same `Err`
    (`workload.rs:403-520`).
  - (Java fires the callback later here: `doSend` rethrows after the append. That divergence
    belongs to the binding and is outside this phase.)
- **D5 recommendation:** mirror `_RecordOutcomes` exactly, as a per-record callback object holding
  `{index, fired count, send-failure-reported}` under a small lock.
  - It costs almost nothing.
  - It keeps the server shaped like the anchor.
  - The "absorb the first callback after a reported throw" branch is unreachable in .NET.
  - When it is taken, it logs at warning level that the D5 contract was violated, so a binding
    regression shows up in the client logs rather than disappearing.
  - The alternative is in D5.

#### 5.5.4 Buffers

- **The async path borrows the caller's key and value buffers until delivery** (ffi §A4, deferred
  send). So every record gets **fresh** key and value arrays, and nothing is reused or mutated after
  `Send`.
- The sync path copies during the call, but it uses the same code for simplicity.
- With `--msg-sizes 1048576`, the cost is 1 MiB of large-object-heap allocation per record. Python
  also allocates per record. This is accepted.

### 5.6 Consumer loop

- This is §3.4 verbatim, on a dedicated thread (sync) or a `Task` (async).
- **No stop token on `Poll`, `Commit`, `Committed` or `Close`** (D7). Stop is checked between polls,
  so stop latency is at most `poll_timeout_ms`, as in Python.
- **`ConsumerHandle` lifetime:**
  - Create it with `consumer.Handle()` before `Subscribe`.
  - Close in this order:
    1. the final commit;
    2. the read-back;
    3. `Closing = true`;
    4. `ConsumerClosing`;
    5. `consumer.Close()` (*`await Close()`*), during which the close-time `OnPartitionsRevoked`
       still commits through the live handle;
    6. `handle.Dispose()`;
    7. `consumer.Dispose()` (*`await DisposeAsync()`*).
  - Disposing the handle drops the parent's count to 1, so step 7's destroy is the immediate path 1
    or 2 of §B2.
  - The handle must **not** be disposed before `Close`: the close-time revoke uses it, and a
    disposed handle throws `ObjectDisposedException`, which would be reported as
    `REVOKE_COMMIT`.
- **The listener runs on the core's dispatcher thread** in both flavours (sync `void`). Its
  `handle.Commit()` and `handle.Committed()` are blocking calls meant for exactly that thread
  (ffi §B1). Every body is a catch-all: a failed commit is **reported, never raised**.
- **`IOffsetCommitCallback.OnComplete(offsets, exception)`:** if `exception` is non-null, emit
  `ConsumerError{COMMIT}`. It runs on the dispatcher, and the binding already makes it no-throw.
- **Absent versus empty bytes (S2 must verify this first):** `CheckRecord` must tell an
  **absent** key or value (`null`) from an **empty** one. If the byte-array deserializer maps an
  absent key to an empty array, the .NET server would say "key is 0 byte(s)" where Rust says
  "key is missing". T14c pins this with `MockConsumer.AddRecord(key: null)`. If the deserializer
  does conflate them, stop and raise it. Do not work around it in the server.

### 5.7 Threading and Kestrel

- **Sync flavour:** one `new Thread { IsBackground = true, Name = "chaos-<id>" }` per workload. Do
  not use the thread pool: `Send` (on `buffer.memory`), `Poll` and `Close` block, and parking pool
  threads would also starve Kestrel. The RPC handler itself is async, so it holds no thread while
  it waits on the channel.
- **Async flavour:** one `Task.Run` per workload. The only blocking calls (the listener and handle
  ops) run on the core's dispatcher thread, not the pool.
- **Callbacks** (delivery on the pump thread; the listener and commit callback on the dispatcher)
  only build an event and `TryWrite` it. The pump runs every delivery callback of the producer, so
  anything slower delays all completions (ffi §A1).
- **Kestrel needs no limit changes.**
  - Each harness workload opens its **own** HTTP/2 connection (`RemoteWorkload` over
    `handle.streaming_channel()`, a fresh `Endpoint::connect` each time). So each connection
    carries about three streams: the run stream plus the unary stop and mark calls. That is far
    below `Http2.MaxStreamsPerConnection` (100).
  - A quiet stream has no pending write, so response-rate limits do not apply. The d1, d2 and c1
    smokes exercise one connection per workload. Neither point is load-tested in this phase,
    because there is no chaos matrix (D11).

### 5.8 Shutdown

`Dispose` on each chaos servicer calls `StopAll()`, then waits for the registered workload
completions, bounded at 30 s. This only matters for container `SIGTERM`: the native servers are
SIGKILLed by `backend_pool`.

---

## 6. Rust harness edits (upstream-owned files; D3 and D4)

| File | Edit |
|---|---|
| `rust/tests/chaos/workload.rs` | `enum Backend`: add `Dotnet` (doc: "Sync .NET binding (`KafkaProducer`/`KafkaConsumer`), run inside the .NET gRPC server with `CONSUMER_FLAVOR=sync`") and `DotnetAsync` ("… `AsyncKafkaProducer`/`AsyncKafkaConsumer` … `CONSUMER_FLAVOR=async`"). `parse`: `"dotnet"` and `"dotnet-async"`. `label`: the same strings. `build_grpc_workload`: `Backend::Dotnet => BackendKind::Dotnet`, `Backend::DotnetAsync => BackendKind::DotnetAsync`. `is_grpc` is unchanged (`!Rust`). |
| `rust/tests/chaos/config.rs` | `:1200`: `"workload backend must be rust, python, python-async, c, dotnet or dotnet-async, got '…'"`, plus its test assertion. One test that `dotnet` / `dotnet-async` parse, and one that the container `sasl_plaintext` rejection names a dotnet workload. Update the `:428-432` comment from "python / c" to "gRPC". No stream cap (§3.5 item 1); add one sentence to the `check_python_server_streams` doc saying the .NET servers have no fixed pool either. |
| `rust/xtask/src/main.rs` | `chaos_test_feature`: `\|\| v.contains(":dotnet")`, which also covers `dotnet-async`; `":c"` does not match `:dotnet`. Help `:773`: `backend=rust\|python\|python-async\|c\|dotnet\|dotnet-async`. Println `:667`: "gRPC (python/c/dotnet) workload requested". Test `:2259-2270`: two more assertions (`producer:dotnet`, `consumer:dotnet-async` → `multilanguage-tests`). The test's name gets `_and_dotnet` appended. |
| `rust/tests/chaos/README.md` | `:89-94`: add `dotnet` (sync binding) and `dotnet-async` (async binding) to the backend list. `:672-674` (Dependencies): add `make build-grpc-images-dotnet`, and for native mode `make build-grpc-native-dotnet`. **Do not touch** `:97-100`, `:201-204` or `:280-282` (§13 Q1). |
| `design/current/chaos-parity-gap.md` §4 | One row: ".NET binding \| — \| ✅ wired: `--workload consumer:dotnet` / `consumer:dotnet-async` starts the .NET gRPC server (sync / async flavor)". |

**These edits are tied to our branch:** `BackendKind::Dotnet` does not exist on master (§2,
row 5), so they cannot be sent upstream before the .NET binding (PR #196) lands. D3 covers this.

---

## 7. Tests

### 7.1 .NET unit tests: a new project `dotnet/tests/Confluent.Kafka.GrpcServer.UnitTests/` (D10)

**Setup:**
- xunit, matching the main suite's package versions.
- `ProjectReference` to the gRPC server.
- **Target `net10.0` only.** ASP.NET Core 8 is absent locally and in CI (§1; M17/P1 D3), and the
  net8.0 server leg is covered by the container smoke run (§7.5, row c1).
- It sits **outside the `.sln`**, like the gRPC server, the soak projects and the perf projects.

**Fixtures:**
- a hand-written `TestServerCallContext : ServerCallContext`, recording header writes and exposing
  a cancellable token;
- a `RecordingStreamWriter : IServerStreamWriter<WorkloadEventBatch>`;
- the internal factory seam (§5.1), with `MockProducer` / `AsyncMockProducer` and
  `MockConsumer` / `AsyncMockConsumer`.

**Every behavioural test runs on both flavours** (`[Theory]` over the flavour). DoD §3 applies:
assert exact codes and messages, not just "it threw".

| # | Test |
|---|---|
| T1 | `Key` / `Value`: index 0, 1, 2^40 and `long.MaxValue`; `msg_size` 0, 3, 8, 9 and 100, matching `build_value` byte for byte |
| T2 | `CheckRecord`: each of the four §3.6 texts **verbatim**, plus the OK cases, including a missing value with `msg_size == 0` |
| T3 | Rebalance event: partitions sorted by (topic ordinal, partition); `observed_at` falls between "before" and "after" |
| T4 | Duplicate `workload_id`: exactly one batch, holding `Failed{-3, "dotnet server: workload_id 'w' is already running"}`; the first workload is unaffected |
| T5 | `StopWorkload` on an unknown id → OK; `MarkWorkload` on an unknown id → `found = false` |
| T6 | Headers are written **before** any message, for a consumer that never receives a record |
| T6b | `StopWorkload` returns promptly while the workload drains (the stop signal does not resume the loop inline, §5.3 item 7). Use a mock producer whose close is held, so the drain cannot finish during the call. |
| T7 | Mark FIFO: events written before the mark come before the `Marker`; events after it come after |
| T8 | Producer happy path (`autoComplete`): for every i, `Sent(i)` comes before `Delivered(i)`; `ProducerStats{sent = N}` comes before `Finished`; `Finished` is last; there is no `SendFailed` |
| T9 | Producer `ErrorNext(code, msg)`: `SendFailed{i, code, msg}` through the callback, settled once (assert after a settle window) |
| T10 | `ChaosRecordOutcomes` state machine, unit-tested directly (all four Python cases): throw-then-callback → the callback is absorbed and logged; callback-then-throw → no `SendFailed`; two callbacks → two outcomes; neither metadata nor error → `SendFailed{-4, …}` |
| T11 | A `Send` that throws (the producer disposed under the loop through the seam) → exactly one `SendFailed` per throwing record and **no later callback** |
| T12 | Async: a structural Critic check that `Send`, `Close` and `Flush` never receive the stop token. Also a behavioural test if `AsyncMockProducer` can be held at admission; otherwise record that it cannot. |
| T13 | Rate schedule as a pure function: `rps = 0` → no wait; on schedule → wait (next_due − now); more than 1 s behind → reset to now |
| T14 | Consumer: a valid record → `Consumed`; a bad key, length or value → `Corrupted` with the exact text; **T14c:** `AddRecord(key: null)` → "key is missing …" (§5.6 absent versus empty); `SetPollError` → `ConsumerError{POLL}` and the loop goes on |
| T15 | Listener: `MockConsumer.Rebalance(...)` → `Rebalance` events **before** that poll's `Consumed` events. A revoke commits through the handle; on a mock handle that **fails with `UnsupportedVersion` and its exact message** (core behaviour, ffi §B5), giving `ConsumerError{REVOKE_COMMIT}`. A lost callback gives `Rebalance{LOST}` and no commit. |
| T16 | Close sequence: final commit → (read-back when check > 0) → `ConsumerClosing` → `ConsumerClosed` → `Finished`, in that order. The handle is disposed after `Close`, and there is no `ObjectDisposedException` during the close-time revoke. |
| T17 | Cancellation: cancel the context token → the workload stops, drains and closes the client; the handler returns only after that; the registry entry is gone |
| T18 | The commit callback driven directly: a non-null exception → `ConsumerError{COMMIT}` with its code and message. The mock always succeeds, so this is the only route. |
| T19 | Read-back: check > 0 and SYNC → `Committed` events after the interval; ASYNC → no periodic read-back, only the final one; check = 0 → none at all |
| T20 | A real-client construction failure (an invalid config through the real factory, with no broker) → `Failed` with the `KafkaException`'s code |

**Wiring:** a `dotnet/Makefile` target `test-grpc-server-dotnet`. It builds, runs
`dotnet format --verify-no-changes` for both the gRPC server and the test project, and tests on
`-f net10.0`.

**It is not called by `test-dotnet`, so it does not run in CI** (Q2, matching Python: Python's
chaos server and gRPC servers have no tests in CI). The Actor and the PM run it as a local gate
(§7.4). Its xmldoc and Makefile comment must say so.

### 7.2 Rust unit tests

- `config.rs`: the new error text, and dotnet parse and rejection (§6).
- `xtask`: `chaos_test_feature` with dotnet.
- `workload.rs`: there is no existing `Backend` test module. Add `Backend::parse` / `label`
  round-trips only if `workload.rs` already has a `#[cfg(test)]` module; otherwise the `config.rs`
  parse test covers it.

### 7.3 What Python and C did

Neither has unit tests for its chaos server (§2, row 10). They rely on the Rust `remote_workload.rs`
fake-server tests (client side) and on end-to-end runs. So §7.1 is **more** than the anchor. Under
DoD §3 that is allowed, because the server is new code with no Java tests to translate. It is
justified because these are the only automated regression net. As in Python, CI does not run them
(Q2); they are a local gate.

### 7.4 Gates (the Actor runs them, the PM verifies them, at every slice from S1)

1. **Rust** (from `rust/`, with `RUSTUP_TOOLCHAIN` set):
   - `cargo build`.
   - `cargo test --features integration-tests --test chaos` and
     `cargo test --features multilanguage-tests --test chaos`: the executed count equals the S0
     baseline plus the new tests.
   - `cargo test -p xtask`.
   - `cargo clippy --features integration-tests --test chaos -- -D warnings` and
     `cargo clippy --features multilanguage-tests --test chaos -- -D warnings`.
   - `cargo xtask format-check` and `cargo xtask lint`.
2. **.NET** (from S2):
   - `~/.dotnet/dotnet build dotnet/grpc-server/Confluent.Kafka.GrpcServer.csproj -c Release`
     with 0 warnings and 0 errors on **both** TFMs;
   - `dotnet format` with `--verify-no-changes` on the gRPC server and the test project;
   - `make -C dotnet test-grpc-server-dotnet` (a local gate, not in CI) with the predicted
     executed count;
   - `make test-dotnet`: the library unit suite on net8.0 and net10.0, with counts **unchanged**
     from S0. `test-dotnet` does not run the new target.
3. **Regression for the existing gRPC arms** (from S2, because `Program.cs` changes):
   `make test-integration-dotnet-native`. The executed count must match the count from the S0
   stored list; investigate a surplus as well as a red.
4. **Mode A:**
   - `git diff <S0 merge>..HEAD -- rust/src rust/cbindgen.toml rust/generator rust/build.rs rust/Cargo.toml rust/Cargo.lock python c`
     is empty;
   - the header SHA-1 and the `EntryPoint` count equal S0's;
   - the `rust/tests` diff touches only `chaos/workload.rs` and `chaos/config.rs`.
5. **No `TODO` or `FIXME`** in the diff.

### 7.5 End-to-end runs (S2, S3, S4), reduced to match Python (D11)

**Scope.** #184 recorded no end-to-end run for the python or python-async backends (§Approval
record, Q3), so this phase runs no chaos matrix. It runs three smokes:

| ID | When | What | Flags |
|---|---|---|---|
| d1 | end of S2, and again at the S3 HEAD in S4 | sync, both roles, broker roll, native | `--workload producer:dotnet --workload consumer:dotnet --cycles 1` |
| d2 | end of S3, and again at the S3 HEAD in S4 | async, both roles, broker roll, native | `--workload producer:dotnet-async --workload consumer:dotnet-async --cycles 1` |
| c1 | S4 | async producer → sync consumer, async commit, **container** on this Mac (Q4) | `--workload producer:dotnet-async --workload consumer:dotnet --commit async --cycles 1` |

PLAINTEXT, `--msg-sizes 100`, `--rps 1000` (the `cargo xtask chaos` defaults unless stated). The
output goes to `rust/target/chaos*`, which is untracked; nothing from these runs is committed.
The evidence (verdict line, delivered / lost / duplicated counts, `rebalance callbacks` and
`in-flight peak` lines) goes once into the STATUS.md entry at Close.

**Acceptance for every run:**
- the verdict passes;
- every workload ends with `Finished`;
- zero `SendFailed` and zero `Corrupted`;
- where a .NET consumer took part, the `rebalance callbacks` line shows non-zero counts and
  `(N consumer(s) with listener)`, with N covering every .NET consumer;
- where a .NET producer took part, `in-flight peak (producer)` is greater than 1, which shows
  pipelining;
- no orphan containers, chaos networks or servers afterwards.

**Oracle rule (from M17/P1):** if a .NET run is red, rerun it with `rust` in the failing role
**before touching C#**. A red that the Rust backend also shows is a harness or cluster problem,
not a .NET one. A red that only .NET shows, and that looks like R1 (a 30 s pump drain) or any
other binding bug, is a **pause-and-ask** for the PM (§12), not something to fix inside this
phase.

**Commands, macOS native (Apple Silicon):**
```
export PATH="/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin:$HOME/.cargo/bin:$HOME/.dotnet:$PATH"
export RUSTUP_TOOLCHAIN=1.95.0-aarch64-apple-darwin
make -C <main> build-grpc-native-dotnet          # rebuilds the dylib, publishes net10.0 into rust/target/grpc-native/dotnet
(cd <main>/rust && MULTILANG_BACKEND_MODE=native MULTILANG_DOTNET=$HOME/.dotnet/dotnet \
   cargo xtask chaos --workload producer:dotnet --workload consumer:dotnet --cycles 1 --reports)
```
(d2 is the same with the `dotnet-async` flags.)

**Commands, container on this Mac (c1):**
1. Cross-build a fresh linux/amd64 `libconfluent_kafka.so` at the S3 HEAD in `rust:1-bookworm`
   with `--platform linux/amd64`, as in the M17/P1 §7.2 gate-4 recipe. Stage it at
   `rust/target/release/libconfluent_kafka.so` and prove it is fresh by its sha256.
2. `DOCKER_DEFAULT_PLATFORM=linux/amd64 make -C dotnet grpc-image grpc-image-async`.
3. **Unset** `DOCKER_DEFAULT_PLATFORM`, so the brokers stay native arm64.
4. `(cd rust && MULTILANG_BACKEND_MODE=container cargo xtask chaos …)` with the c1 flags.

The .NET server then runs emulated: slow is not hung (P13.4 measured 239 s just to compile).

**Not run in this phase** (the user may run them later; none is a gate): a chaos matrix,
cross-binding rows, the SSL / SASL protocols, max rate, large records, `--repeat`, and a Linux
amd64 container run.

### 7.6 CI

- **Chaos does not run in CI** (§2, row 7), and this phase does not add it (§15).
- **The gRPC server unit tests do not run in CI either** (Q2, matching Python, whose chaos server
  and gRPC servers have no tests in CI). `test-grpc-server-dotnet` is a separate target that
  `test-dotnet` does not call, and no `.semaphore` job calls it. It is a local gate (§7.4).
- **Gate:** no `.semaphore/semaphore.yml` change in the diff, and
  `/usr/bin/grep -c test-grpc-server-dotnet` on `.semaphore/semaphore.yml` and on the root
  `Makefile` both return 0. CI still builds the gRPC server as before (the
  `test-integration-dotnet-native` / container image paths), so a compile break in the new
  `Chaos/*` files would still show up on the user's push.

---

## 8. Slices and acceptance criteria

| Slice | Who | Content | Acceptance |
|---|---|---|---|
| **S0** | PM | Merge `5d4c6b80` (§4) | Clean `--no-ff` merge; §4 gates green; baselines recorded (chaos test counts, xtask count, header SHA, `EntryPoint` count, the `test-integration-dotnet-native` count, the harness baseline verdicts) |
| **S1** | `dotnet-actor` 94 | §6: the Rust harness edits and their tests. The csproj gets its `<Protobuf>` line so the generated base classes exist; there is no servicer yet. | §7.4 gates 1, 4 and 5. `cargo xtask chaos --workload producer:dotnet` builds with `multilanguage-tests`, and fails at run time with gRPC `Unimplemented`, as expected before S2. The README and parity rows are present. The grpc-server still builds with 0 warnings. |
| **S2** | `dotnet-actor` 94 | §5.1–§5.8, sync flavour: the `Chaos/*` shared files, `ChaosWorkloadServiceImpl`, the `Program.cs` sync registration and `DrainServicer`; the test project and the T1–T20 sync legs; the Makefile wiring | §7.4 gates 1–5. T14c resolved first (§5.6). A smoke run of d1 (`--cycles 1`) passes locally. |
| **S3** | `dotnet-actor` 94 | `AsyncChaosWorkloadServiceImpl`, the async `Program.cs` registration, the async legs of every test, and the T6b / T12 async specifics | §7.4 gates 1–5. A smoke run of d2 (`--cycles 1`) passes. |
| **S4** | `dotnet-actor` 94 runs; PM verifies | §7.5: d1 and d2 re-run at the S3 HEAD, then c1 (the container smoke on this Mac). Fixes go in as `fixup!` commits. Nothing from the runs is committed. | d1, d2 and c1 meet §7.5's acceptance, or a red has an oracle-rule diagnosis showing it is not .NET. A .NET-only red that looks like R1 or another binding bug is a pause-and-ask. Hygiene is clean. |
| **Critic** | `dotnet-critic` 94 | One pass after S4, over `<S0 merge>..HEAD` (§10) | `COMMENTS.94.md` empty or resolved |
| **Close** | PM | STATUS.md entry (the evidence goes there **once**); archive `COMMENTS.DONE.94.md` here; reset the root `COMMENTS.94.md`. `marked_classes.txt` does not apply (no Java translation). | — |

---

## 9. Ownership

| Change | Owner | Basis |
|---|---|---|
| S0 merge | PM | No authoring (M15/P13.1, P13.3) |
| `dotnet/grpc-server/**`, the new test project, `dotnet/Makefile` | `dotnet-actor` 94 | Normal scope |
| `rust/tests/chaos/workload.rs`, `config.rs`, `rust/xtask/src/main.rs`, the chaos README, `design/current/chaos-parity-gap.md` | `dotnet-actor` 94, **recorded exception** | Precedent: M17/P1 §5 (`backend_pool.rs`), M8/P1 `f910e7b3`, M8/P2 `d1ef8906`, M15/P12 `ece8bb74`. This is harness glue, not translation or core Rust, and every edit copies an existing arm. |
| `rust/src/**`, the ABI, the header, `python/`, `c/` | nobody | A change stops the phase (§7.4, gate 4) |
| Review | `dotnet-critic` 94 | Reviews the harness Rust as sanctioned (M15/P12). `kafka-critic` is not needed because the ABI does not change. |

---

## 10. The one Critic pass (after S4)

`dotnet-critic` 94 reviews `<S0 merge>..HEAD`. For the S0 merge, it checks only that the merge was
clean and has no evil-merge hunks (`git show --remerge-diff`). Its brief:
1. **Fidelity to the §3 anchor,** row by row. In particular:
   - `Sent` comes before `Send`;
   - `ProducerStats` comes before close;
   - the final commit is sync in both modes;
   - the periodic read-back happens only after a SYNC commit;
   - a close error is `ConsumerError{CLOSE}`, not `Failed`;
   - a lost callback produces no commit;
   - the `Closing` gate applies to the revoke read-back;
   - the exact §3.6 strings.
2. **D6 and D7, structurally:** no stop token reaches `Send`, `Flush`, `Close`, `Poll`, `Commit`
   or `Committed`.
3. **Threads:**
   - callbacks only enqueue;
   - `AllowSynchronousContinuations = false`;
   - the worker-completion TCS uses `RunContinuationsAsynchronously`;
   - the stop signal never resumes a waiter inline (§5.3 item 7);
   - the sync loops run on dedicated threads, not the pool.
4. **Handle order:** `Close` → `handle.Dispose` → `consumer.Dispose`. The handle is never disposed
   before `Close`.
5. **The settle state machine** matches `_RecordOutcomes` (D5), and its unreachable branch says so.
6. **Lifecycle:**
   - headers go out before the first event and after the duplicate check;
   - a cancellation drains and the handler awaits it;
   - the registry removal is entry-safe.
7. **The Rust edits** copy existing arms mechanically, and the tests assert the new texts.
8. **Mode A,** and the §7.4 counts reconcile.
9. **DoD §12:** the tests drive production's loops through the seam, not a re-implementation.

Fixes go in as `fixup!` commits, and the Critic re-checks only those.

---

## 11. Decisions for the user

| # | Decision | Recommendation | Alternatives |
|---|---|---|---|
| D1 | Numbering | **M18/P1, N=94.** This is new cross-binding test infrastructure. It does not continue M17 (harness SSL / native) or M8 (the consumer backend). | Fold it into M17 as P2. M17/P2 is already used by the master-#209 merge. |
| D2 | How #184 gets in (§4) | **Merge `origin/master` 5d4c6b80 directly; the PM runs it** | Wait for #201 to be rebased onto 5d4c6b80 and merge that |
| D3 | Where the upstream-file edits live | **On our branch (PR #196)**, because `BackendKind::Dotnet` exists only here (§2, row 5) | (a) A separate upstream PR. It cannot compile on master until #196 lands, so it would wait for #196 anyway. (b) Split it: dotnet-specific edits here, plus a small **backend-agnostic** upstream PR (fix the stale README passages; make `chaos_test_feature` parse backends through `Backend::is_grpc` instead of substring matches). Your call; I do not open PRs. |
| D4 | Backend names | **`dotnet` / `dotnet-async`**, matching `python-async` style. They map to `BackendKind::Dotnet` / `DotnetAsync`; the pool labels stay `dotnet` / `dotnet_async`. | `csharp`, `dotnet-sync`, `net` |
| D5 | Settle-once guard (§5.5.3) | **Mirror `_RecordOutcomes` exactly.** It is unreachable in .NET, is documented as such, and logs a D5-contract warning if it is ever taken. | **Strict:** no absorb; report every callback, so a binding regression shows up as a double settle that the verifier fails. That is a stricter check of .NET's own contract, at the cost of diverging from the anchor. |
| D6 | Async producer and the stop token | **`CancellationToken.None` on `Send`, `Flush` and `Close`; stop is checked between sends** | Link the stop token and treat an `OperationCanceledException` as "accepted, outcome pending" (no `SendFailed`). That adds a branch that exists only to undo D2 (c). |
| D7 | Consumer and the stop token | **Never pass it to `Poll`, `Commit`, `Committed` or `Close`; stop latency ≤ `poll_timeout_ms` (Python parity)** | Pass it to `Poll` for a faster stop. That mixes the wakeup-based cancel into the drain and is not Python parity. |
| D8 | Threading | **Sync: one dedicated background thread per workload. Async: one `Task.Run` per workload. The handler is async, waits on the channel, and awaits the drain without a token.** No worker cap. | Sync on `Task.Factory.StartNew(LongRunning)`, which is the same thing in practice; or on pool threads, rejected for starvation (R5). |
| D9 | Event channel and batching | **An unbounded `Channel<WorkloadEvent>`, `SingleReader`, `AllowSynchronousContinuations = false`; batches of up to 4096** (Python and C) | `BlockingCollection`, or a `ConcurrentQueue` plus `SemaphoreSlim` |
| D10 | Unit-test approach (§7.1) | **A new net10.0-only test project outside the `.sln`, using an internal factory seam over the mock clients; run by its own `test-grpc-server-dotnet` target as a local gate, not by `test-dotnet` and not in CI** (approved; the CI part was reshaped by Q2 to match Python) | (a) Link the chaos sources into a test project without ASP.NET, so it can also run on net8.0. That is unusual and adds a second build of the same files. (b) No .NET unit tests, as Python and C have none (chaos is not in CI, so nothing would guard regressions). (c) The same project wired into `test-dotnet` and so into CI, as first proposed. |
| D11 | E2E scope (§7.5) | **Reduced to match Python (Q3): the d1 and d2 native smokes (`--cycles 1`) at the end of S2 and S3 and again at the S3 HEAD, plus the c1 container smoke on this Mac (Q4). No chaos matrix.** (The plan as submitted proposed a 24-run Tier A matrix; Q3 replaced it.) | The 24-run matrix as first proposed, or a subset of it; the user may run either later |
| D12 | Cadence | **S0 (PM) → S1 → S2 → S3 → S4 (Actor), with the PM verifying gates between slices and no user gate. One Critic pass after S4 over the whole range.** | A Critic pass after S3 (code complete) and another after S4 (fixes from the E2E runs) |

---

## 12. Risks

1. **DV-4, the pump-drain expiry at producer close.**
   - Mechanism: `Close` flushes, then waits up to 30 s for the pump to drain (`NativeProducer.cs`
     `s_pumpDrainTimeout`). Futures still queued after that are faulted **without callbacks**
     (DV-4). Their records stay unsettled, and the verifier fails the run.
   - Where it is most likely: an all-brokers-down outage or max rate. Neither is run in this phase
     (D11), so the smokes are unlikely to show it.
   - If it happens, it is a **binding finding** and a **pause-and-ask**: report it with numbers;
     do not hack around it in the server.
2. **D2 (c) regression.** Anyone who later threads a token into the async `Send` creates double
   settles. Mitigations: D6, T12, and the Critic's structural check.
3. **Ambiguous post-append core error** (§5.5.3).
   - The sync `Send` can throw for a record the core already appended. We report `SendFailed`, and
     the run fails anyway. This is rare: the TM must be in an error state.
   - **A doc inaccuracy found while planning:** `IDeliveryCallback.cs:146-148` says a synchronous
     `KafkaException` means *"the core rejected the record before accepting it"*. That is not true
     on this path (`ffi/producer.rs:700-708`).
   - This phase does not fix it (it is a binding doc, outside chaos). It is recorded as a
     follow-up (Q5).
4. **Kestrel / HTTP/2.** Each workload uses its own connection, so the stream cap does not bind
   (§5.7). In container mode, a `SIGTERM` gives in-flight streams about 30 s of graceful shutdown;
   the harness never stops a server mid-run. This phase does not load-test it (D11).
5. **Thread-pool starvation** if a sync loop ever lands on the pool. Mitigations: D8 and the
   Critic's item 3.
6. **Faults on dropped async delivery `Task`s that nobody observes:** benign (no handler, outcomes
   come through the callback), but noisy if a global handler is added later.
7. **net8.0 is not tested locally** (no ASP.NET Core 8). The server is exercised on net10.0
   natively, and on net8.0 only by the c1 container smoke.
8. **Docker resources.** Emulated amd64 for c1 is slow. Brokers left behind poison later runs
   (hygiene gate).
9. **Upstream drift.** If master gains chaos follow-ups before S0, re-verify the dry run and
   re-plan S0. After close, a later upstream change to `Backend` (a new arm) conflicts trivially
   with ours.
10. **#201 interplay.** A later rebase of #201 onto master that includes #184 merges cleanly here;
    a rebase that rewrites chaos files would need a re-check of §6.
11. **Absent versus empty bytes** (§5.6). If the byte-array deserializer conflates them, the Corrupted
    texts diverge from Rust. This is resolved first in S2.
12. **Timer granularity** in the rate wait (about 1 ms on macOS and Linux). The schedule makes up
    for overshoot by not sleeping while behind, so the average rate holds (Python has the same
    property).
13. **Unbounded memory and message size.** The channel is unbounded, and a batch holds up to 4096
    events. Both are the same as Python and C, and both are low risk (§5.2, §5.3).

---

## 13. Open questions (all answered on 2026-10-09; see the [Approval record](#approval-record))

- **Q1. Upstream-stale README text.** These passages are false for **every** gRPC backend since
  #184 itself:
  - `rust/tests/chaos/README.md:97-100`: "only the Rust backend … registers a rebalance listener
    (the bridge cannot carry one) … pipelines sends";
  - `:201-204`: "A gRPC … producer cannot pipeline … The run prints a notice", though no code
    prints such a notice;
  - `:280-282`: "the gRPC (python / c) backends cannot carry one".

  **Answer:** leave them untouched on our branch for now. No upstream fix is opened in this phase.
- **Q2. Wire the gRPC server unit tests into CI?** **Answer:** only if Python does. It does not, so
  they stay a local gate (D10, §7.1, §7.6).
- **Q3. E2E breadth (D11).** **Answer:** match Python. #184 recorded no Python E2E runs, so the
  matrix is dropped and only the smokes remain (§7.5).
- **Q4. Is the c1 container smoke on this Mac enough?** **Answer:** yes.
- **Q5. A follow-up phase for R3?** **Answer:** not for now.

---

## 14. Size estimate

| Slice | Code | Tests | Time (indicative) |
|---|---|---|---|
| S0 | none (a merge) | baseline runs | ~0.5 h, mostly the harness baseline |
| S1 | ~40 lines of Rust plus ~20 doc lines plus 1 csproj line | ~25 lines of Rust | ~1 h |
| S2 | ~900–1,100 lines of C#, with xmldoc (shared `Chaos/*` plus the sync servicer), plus about 10 lines of Makefile and csproj | ~700–900 lines | ~1 Actor session |
| S3 | ~400–500 lines of C# | ~300–400 lines (mostly theory legs) | ~0.5 Actor session |
| S4 | fixups only | 3 smoke runs (d1, d2, c1) | ~0.5–1 h of machine time, most of it c1 under emulation |
| Critic + fixups | — | — | ~0.5 session |

**Total:** about **1,300–1,600 lines of C# in the server**, about **1,000–1,300 lines of tests**,
and about **65 lines of Rust**. For comparison: Python's `grpc_chaos.py` (both flavours) is 857
lines, and C's chaos section is about 1,100.

---

## 15. Out of scope (deliberately not added)

- **Chaos in CI.** Upstream has not added it for Python or C either. It needs dedicated Docker
  clusters and hours of run time.
- **The gRPC server unit tests in CI** (Q2: Python has none in CI).
- **A chaos matrix,** and any run beyond the d1 / d2 / c1 smokes (Q3: #184 recorded none for
  Python).
- **Any `rust/src`, ABI or header change**, and any Python or C chaos-server change.
- **A worker cap for the .NET servers** (§3.5 item 1).
- **Fixing the upstream-stale README passages** on our branch (Q1).
- **Fixing the `IDeliveryCallback` doc / the Java late-callback divergence** (R3, Q5).
- **Share-consumer (KIP-932) workloads,** which upstream also leaves out (parity-gap §8).

---

## 16. Rule suggestions (for the user; agents do not edit rule files)

- **`dotnet/CLAUDE.md`, gRPC server section:** record that the chaos servicer's sync flavour runs
  **one dedicated thread per workload** by design. ffi §A1's "no per-send thread" is a rule for the
  **binding library**, not for test-server scaffolding. Without this note, a future Critic is likely
  to flag the threads.
- **`dotnet/CLAUDE.md` (or ffi §A6):** "Any caller that settles each record exactly once must not
  pass a token to the async `Send` that can fire after acceptance (M11/P3.5 D2 (c)): the record is
  still sent and its callback still fires." This is the rule the chaos server relies on (D6), and
  it applies to users too.
- **`dotnet/CLAUDE.md`:** add the new `Confluent.Kafka.GrpcServer.UnitTests` project to the list of
  out-of-`.sln` projects that need their own `dotnet format --verify-no-changes` (with the soak
  projects, the perf projects and the gRPC server).

---

## Approval record

**Approved by the user on 2026-10-09**, relayed by the coordinator. The rulings, verbatim where
they were given verbatim:

| Item | Ruling | Effect on this plan |
|---|---|---|
| D1–D12 | Approved as recommended | D10 and D11 then reshaped by Q2 and Q3 (below) |
| Q1 | "my recommendation; leave the README passages for now" | The stale README passages stay untouched (§15) |
| Q2 | "Yes, if python also does it." | Python does not (evidence below), so the new unit tests are **not** wired into `test-dotnet` or CI; they are a local gate (D10, §7.1, §7.4, §7.6) |
| Q3 | "Yes, if same with python." | #184 recorded no Python E2E runs (evidence below), so the 24-run matrix is dropped; d1 / d2 / c1 smokes only (D11, §7.5) |
| Q4 | "the smoke run on the Mac is enough" | c1 runs on this Mac under amd64 emulation; no Linux run is asked for |
| Q5 | "not for now" | No R3 follow-up phase |

**The Python checks behind Q2 and Q3** (Manager, 2026-10-09, against `origin/master 5d4c6b80`):

- **Q2 — Python's chaos server and gRPC servers have no tests in CI.**
  - Python's test files on master are `python/test/unit/{test_admin,test_consumer,
    test_consumer_callbacks,test_producer}.py`, `python/test/static/*`, `python/test/performance/*`
    and `python/soak/test/*`.
  - `git grep -l -E "grpc_server|grpc_chaos|grpc_translate" origin/master -- 'python/test/*'
    'python/soak/*'` matches nothing.
  - The Python CI wiring: the Makefile's `test-python` runs `pytest test/unit`;
    `test-integration-python` runs the Rust multilanguage arm `__grpc_python`; the
    `.semaphore/semaphore.yml` jobs are "verify-python (Linux amd64)" (plaintext, ssl, sasl_ssl)
    and "verify-python (macOS arm64)" (`verify-python-macos-docker`). Nothing runs chaos.
  - So Python's gRPC servers are exercised in CI only as servers driven by the Rust multilanguage
    arm, never by their own unit tests, and the chaos servicer not at all.
- **Q3 — #184 recorded no end-to-end run for python or python-async.**
  - PR #184 (`confluentinc/kafka-clients`) merged 2026-10-09T05:50Z.
  - Its Verification section lists Rust-backend chaos runs only (broker roll clean and unclean,
    reassign, change leader, topic recreate, rebalance, `--repeat 2`), plus one gRPC cross-binding
    run: "Rust producer + C consumer over gRPC — 1338 delivered, 0 lost".
  - Neither the PR body, nor the 556-line squash message, nor its 18 bot reviews records a run of
    the `python` or `python-async` backends.
  - This is read as "Python's E2E breadth is zero recorded runs", so D11 keeps only the smokes the
    slices already needed (d1 at S2, d2 at S3), re-runs them at the S3 HEAD, and adds c1 under Q4.
    The Manager judged this unambiguous enough not to pause; **the user may override it**. A
    matrix can still be run later with `cargo xtask chaos-matrix` and the new backends, with no
    code change.
- **Two stale upstream statements found during the check** (recorded only; Q1 leaves the README
  alone): `design/current/chaos-parity-gap.md:24` says "only rust actually runs today (§4)", and
  `rust/tests/chaos/README.md:236` says secured runs are "Rust workloads only". Both predate
  #184's gRPC chaos servers.
