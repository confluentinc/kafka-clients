# COMMENTS.DONE.62 — M9/P9, the .NET gRPC harness server (consumer callback log)

**Agent:** N=62 (Actor, `dotnet-actor`). **Milestone/Phase:** M9/P9. **Mode: A.**
**Branch:** `prashah_dev_dotnet_binding_consumer`.

No `COMMENTS.62.md` existed at start — this was fresh implementation work, not a
fix round. This file therefore records the phase's **decisions, deviations and
verification evidence** rather than closed review items.

---

## 1 · D11 — `ffi-marshalling.md`: **NONE**

**P9 touches no section of `.claude/rules/ffi-marshalling.md`.** Stated
explicitly per roadmap §5.6, which requires a phase that touches none to *say
so*, because a blank row is indistinguishable from an omission.

Reasoning: the gRPC server is a **consumer of the binding's public API**, not a
boundary change. It declares no `[DllImport]`, defines no `SafeHandle`, marshals
no strings or bytes across the ABI, allocates no `GCHandle`, and registers no
native callback. Every native interaction it triggers happens *inside*
`Confluent.Kafka`, already governed by M9/P6's and M9/P7's §B2/§B5/§B6/§B7
updates. The two callback types it defines (`LoggingRebalanceListener`,
`LoggingCommitCallback`) implement **managed** interfaces; the binding owns the
trampolines, the `user_data_destroy` hooks and the handle lifetimes behind them.

No interop primitive was reached for at any point — which §8 names as the signal
that the phase has strayed out of scope.

---

## 2 · The four ruled decisions, as implemented

| # | Ruling | Where |
|---|---|---|
| **P9-D1** | One `CallbackLog.cs` holding the log + both logging types | `grpc-server/CallbackLog.cs` |
| **P9-D2** | `GetCallbackLog` **is** gate-exempt (2nd exemption after `Wakeup`) | both servicers' `GetCallbackLog` + type remarks |
| **P9-D3** | Unknown/closed id → **empty response, not an error** | both servicers' `GetCallbackLog` remarks |
| **P9-D4** | Log is **unbounded** | `CallbackLog` type remarks |

D3 and D4 both carry their reasoning **in the code**, not only here, per the
ruling: D3 because a future reader/reviewer would otherwise "fix" the deliberate
inconsistency with `Translate.UnknownConsumer` and silently break every
post-`Close` read; D4 because "a map that is never cleared" reads as a leak on
sight.

## 3 · The two structural traps

1. **The log is a servicer field, never on `ConsumerEntry`.** Verified in the
   current source that `Close` calls `_consumers.TryRemove` on the success path,
   the failure path, *and* in the async twin, and that `ConsumerEntry` holds only
   `Consumer` + `Gate`. A log on the entry would vanish at `Close`. Implemented
   as `private readonly CallbackLog _callbackLog` on each servicer, keyed
   internally by consumer id, never evicted.
2. **The log has its own lock** (`CallbackLog._gate`), never `ConsumerEntry.Gate`
   — callbacks fire on the core's dispatcher thread while `GetCallbackLog` is
   served on a gRPC worker. Entries are built *outside* the critical section.

## 4 · Deviations and judgement calls

- **D8 "per-id log allocated at `CreateConsumer`" — implemented as lazy
  allocation on first append instead.** The requirement that carries behaviour is
  "never evicted at `Close`", which is met. Eager per-id allocation is a *C*
  necessity: C needs a stable `LogState*` to hand the ABI as `user_data`. .NET
  has no such need — `LoggingRebalanceListener` / `LoggingCommitCallback` carry
  `(log, consumerId)` as managed fields — so there is nothing to pre-allocate.
  This matches Python, the primary shape anchor, which likewise populates its map
  lazily (`_entries.setdefault`).
- **`LoggingRebalanceListener` implements `IConsumerRebalanceListener` directly**
  rather than deriving from `ConsumerRebalanceListenerBase`, so
  `OnPartitionsLost` is **explicit**. The base's Java-faithful delegating default
  is right for users and wrong here: inheriting it logs a `lost` as `revoked`.
- **No `discard_commit_complete` analogue.** C needs one only because it talks to
  the raw ABI (non-nullable `callback`, no plain `..._commit_async_offsets`).
  M9/P7's binding absorbed that internally, so the server passes `null`. Said at
  the site so a reviewer diffing `server.cc:376-380` does not read it as a gap.
- **`ProtoOffsetsToDictionary` hoisted** out of `CommitSync` into `Translate`
  before being used from four sites, rather than copy-pasted.

## 5 · Out of scope / not done (deliberate)

- **§7.2 / D10's `tests/common/backend_factory.rs` doc refresh — deferred, then
  DONE in a follow-up round (`62b360b4`).** The PLAN is internally inconsistent
  here: §1's scope table admits only "the one doc line in §7.1" under `tests/`,
  while §7.2 + D10 also ask for `backend_factory.rs`. The dispatch brief resolved
  it explicitly — the `callback_log.rs` line only, "**No other Rust**" — so that
  ruling was followed in the main round and the stale text was **flagged to the
  Manager rather than silently dropped**. The Manager then authorized it, and
  both paragraphs are now corrected: `DotnetGrpcFactory` (`:415-418`) and
  `DotnetAsyncGrpcFactory` (`:470-473`) said the .NET callback arms "currently
  **fail at runtime** … does not yet implement the `CommitAsync` /
  `GetCallbackLog` RPCs … closes in M9/P9"; they now state the arms are live and
  passing. `/usr/bin/grep -rn "M9/P9" tests/` bounded the edit to exactly those
  two sites, and no "fail at runtime" / "does not yet implement" /
  "UNIMPLEMENTED" claim remains anywhere under `tests/`. The producer-exemption
  prose above each factory is untouched and still true.
- **Carried, NOT done (conditional instruction, file not otherwise touched):**
  the Critic observed that `CallbackLog`'s separate lock prevents a hard
  **deadlock**, not merely serialisation — during `Poll` the rebalance blocks
  until the listener returns *while the RPC handler holds the gate*, so a shared
  gate would deadlock cross-thread. The comment in `CallbackLog.cs` understates
  this as serialisation. The Manager scoped the follow-up round to
  `backend_factory.rs` only ("no other file"), and the instruction was explicitly
  conditional ("if you touch the file"), so it was left alone. Worth folding into
  the next edit of `grpc-server/CallbackLog.cs`.
- **P8 (`ConsumerHandle`, N=61) remains outstanding**; P9 does not close the
  roadmap. The logging callbacks deliberately never touch the consumer.

## 6 · Verification (real results)

Gate map per roadmap §6.4. `dotnet` = 10.0.302, `cargo` = via `~/.cargo/bin`.

| Gate | Result |
|---|---|
| `dotnet build bindings/dotnet/grpc-server` (net8.0) | **Build succeeded, 0 warnings, 0 errors** (also verified with `-t:Rebuild`) |
| `dotnet format --verify-no-changes` (grpc-server) | **clean (exit 0)** |
| `dotnet test -f net10.0` (binding regression) | **543 passed, 0 failed** — no regression vs P7 |
| `cargo xtask format-check` | **clean** |
| `cargo xtask lint` | **clean** (doc hygiene + clippy) |
| RPC tripwire | proto = **26** RPCs; generated `Unimplemented` = **26**; `public override` = **26** in *both* servicers; MISSING and EXTRA both **empty** |

**The phase gate — the four previously-failing tests:**

```
test test_ml_rebalance_listener_logs_assigned_and_revoked__grpc_dotnet       ... ok
test test_ml_rebalance_listener_logs_assigned_and_revoked__grpc_dotnet_async ... ok
test result: ok. 2 passed; 0 failed; ... finished in 15.22s

test test_ml_commit_async_callback_logs_offsets__grpc_dotnet       ... ok
test test_ml_commit_async_callback_logs_offsets__grpc_dotnet_async ... ok
test result: ok. 2 passed; 0 failed; ... finished in 8.34s
```

**Full `__grpc_dotnet` suite — no regression on the pre-existing .NET arms:**

```
test result: ok. 28 passed; 0 failed; 0 ignored; 289 filtered out; finished in 77.63s
```

All 28 = 14 bodies x {sync, async}: assign_and_consume, commit_and_committed,
commit_async_callback_logs_offsets, commit_explicit_offsets, list_topics,
metrics, offsets_for_times, partitions_for, pause_resume,
rebalance_listener_logs_assigned_and_revoked, seek_and_offsets,
seek_to_beginning_end, subscribe_and_consume, unsubscribe.

**Backends actually exercised for the two callback bodies: 3 of 6.**
`__rust` (native, also green — `rebalance_listener…__rust` ok,
`commit_async_callback_logs_offsets__rust` ok), `__grpc_dotnet`,
`__grpc_dotnet_async`. **`__grpc_python`, `__grpc_python_async` and `__grpc_c`
were NOT run** — their backend images are not built on this machine (only the
two dotnet images are present locally). Those three are unaffected by this
change (no Python/C/proto file was touched) but the claim "identical across all
six" is, on this run, verified for three.

`--list _logs_` confirms the six consumer arms exist and are named `__rust`,
`__grpc_python`, `__grpc_python_async`, `__grpc_c`, `__grpc_dotnet`,
`__grpc_dotnet_async` — which is the independent check on §7.1's corrected
enumeration.

### Environment notes worth carrying forward

- **The `__dotnet` test filter in the brief (and in the M8/P1 memory) is stale:
  it matches 0 tests today.** The names are now `…__grpc_dotnet` /
  `…__grpc_dotnet_async`, so the working filter is **`__grpc_dotnet`** (28
  tests). Verified with `--list`.
- Images built `--platform linux/amd64` (Grpc.Tools' `linux_arm64` protoc
  SIGSEGVs under Apple-Silicon Docker); `DOCKER_DEFAULT_PLATFORM` **unset** for
  the test run so the broker stays native arm64.
- The first image build died with BuildKit `DeadlineExceeded: context deadline
  exceeded` — transient, not a code fault; the retry completed from cache.
- **Both images were verified to actually contain the new code**, not a stale
  layer: the published `Confluent.Kafka.GrpcServer.dll` was extracted from each
  image and checked for `LoggingRebalanceListener`, `LoggingCommitCallback`,
  `GetCallbackLog`, `ProtoOffsetsToDictionary` (all present in both).
- Staged Linux `.so` verified before any image build: x86-64 ELF, with
  `ConsumerHandle` 22, `RebalanceListener` 2, `MockConsumer_rebalance` 1,
  `KafkaError_new` 1.

### DoD notes

- **#10 (hot-path allocation audit): N/A** — a harness server has no per-record
  path; the callback log is per-rebalance / per-commit.
- **#11 (consumer trait surface check): N/A** — no binding trait changed.
- **#12 (test-fixture fidelity): applies and is met** — the logging listener and
  commit callback are registered through the binding's *public* API
  (`Subscribe(topics, listener)`, `CommitAsync(...)`), exactly as a user would;
  no internal shortcut.
- **No new .NET unit tests.** `grpc-server` is still not a member of
  `Confluent.Kafka.sln`, which blocks servicer unit tests
  (`project_grpc_server_not_in_sln_blocks_servicer_tests`) — re-verified: the
  csproj is referenced by neither the solution nor any test project. The four
  harness tests are therefore the **only** gate on this code, which is a fact the
  phase record should carry rather than leave implicit.

## 7 · Mode-A proof

- `git diff <base> HEAD -- src/ cbindgen.toml` → **empty**.
- `git diff <base> HEAD -- tests/` → **exactly one hunk**, the
  `tests/common/callback_log.rs` module-doc line (four → six backends).

## 8 · Commits

- `33b60f13` — Serve the consumer callback log from the .NET gRPC harness
  backends (M9/P9).
- `ee91b6be` — Correct the callback-log backend enumeration to six (M9/P9).
- `62b360b4` — Mark the .NET callback arms live in the backend-factory docs
  (M9/P9). Follow-up round, Manager-authorized. **Standalone, not a `fixup!` of
  `ee91b6be`**: `--autosquash` discards a fixup's body, so squashing would drop
  the rationale and leave `ee91b6be`'s subject ("enumeration to six") describing
  a commit that also rewrote two factory doc-comments; and `ee91b6be` did not
  introduce the text (M9/P5 wrote it, M9/P9 falsified it), so a `fixup!` would
  misstate causality.
