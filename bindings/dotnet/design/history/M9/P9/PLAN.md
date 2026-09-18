# M9/P9 — the .NET gRPC conformance server (consumer callback log)

**Status:** **ACTOR BRIEF — dispatch-ready.** Derived from the approved roadmap
`design/current/PLAN-M9-consumer-callback-parity.md`. **Four P9-local decisions in
§10 need a maintainer ruling before the Actor starts.**

**Agent number:** **N=62.** **Milestone/Phase:** M9/P9. **Mode: A**
(test-harness C# only). **Branch:** `prashah_dev_dotnet_binding_consumer`, on top
of `4fe9b5e5` (M9/P7). **Risk: low-medium.**

**This is the phase where the four `__grpc_dotnet{,_async}` callback tests go
green.** It consumes **both** P6 (listener) and P7 (commit callback) surfaces.

**Prerequisite — IN FLIGHT, verify before starting.** The `linux/amd64` core
cross-build refreshing `target-linux-amd64/` is running. **Confirm the staged `.so`
carries the new symbols before building any image** (§9.3) — a stale `.so` is the
exact trap M9/P3's Actor hit.

---

## 1 · Scope

Three RPC gaps in both servicers, plus the callback log they all feed.

### In scope

1. `Subscribe` honouring `SubscribeRequest.with_listener`.
2. `CommitAsync` — currently unimplemented (generated stub throws `UNIMPLEMENTED`).
3. `GetCallbackLog` — same.
4. `CallbackLog` + `LoggingRebalanceListener` + `LoggingCommitCallback`.
5. `Translate.cs` additions (§2).
6. Doc carry-forwards (§7) and the RPC-count tripwire refresh (§6.4).

### Explicitly OUT of scope

| Out | Why / where |
|---|---|
| `ConsumerHandle` and any reentrancy | **P8 (N=61)** — and the logging callbacks deliberately never touch the consumer (§4.2) |
| Anything under `bindings/dotnet/src/Confluent.Kafka/**` | P6/P7 shipped it; **P9 is a consumer of that API, not a change to it.** If you find yourself editing the binding, stop and escalate |
| `ProducerService` / `ProducerService.GetCallbackLog` | No .NET producer on this branch; `producer_service.proto` is compiled `GrpcServices="None"` (`.csproj:56`) — messages only. The producer callback test emits only 4 arms (`multilanguage_test!`), none of them dotnet |
| `multilanguage-test-server/proto/**` | The contract is fixed; .NET conforms to it |
| Anything under `src/` (Rust), `src/ffi/`, `cbindgen.toml` | Mode A |
| `tests/**` **except** the one doc line in §7.1 | That single comment fix is in scope; nothing else under `tests/` is |

**Mode-A proof obligation:** `git diff <base> HEAD -- src/ cbindgen.toml` must be
**empty**, and the `tests/` diff must be **exactly the one doc-comment hunk** from
§7.1. Mode A has held since P5 — state it in the final commit.

---

## 2 · Deliverables

All under `bindings/dotnet/grpc-server/` unless noted.

| # | Deliverable | Path |
|---|---|---|
| D1 | `CallbackLog` — thread-safe, per-consumer-id, **its own file** (roadmap Q8/D6) | `CallbackLog.cs` (new) |
| D2 | `LoggingRebalanceListener : IConsumerRebalanceListener` | `CallbackLog.cs` or `LoggingCallbacks.cs` (§10, P9-D1) |
| D3 | `LoggingCommitCallback : IOffsetCommitCallback` | same |
| D4 | `Translate.cs` additions — the four `KIND_*` constants, `OffsetKey`, `CallbackLogPartitionToProto`, and a **hoisted** `ProtoOffsetsToDictionary` | `Translate.cs` |
| D5 | `Subscribe` honours `WithListener` — **both** servicers | `ConsumerServiceImpl.cs:127`, `AsyncConsumerServiceImpl.cs:173` |
| D6 | `CommitAsync` override — **both** servicers | both |
| D7 | `GetCallbackLog` override — **both** servicers | both |
| D8 | Per-id log allocated at `CreateConsumer`, **never** evicted at `Close` (§5.1) | both |
| D9 | RPC-count tripwire refresh in both servicer class doc-comments (§6.4) | both |
| D10 | Doc carry-forwards (§7) | `tests/common/callback_log.rs`, `backend_factory.rs` |
| D11 | **`ffi-marshalling.md`: state explicitly that P9 touches none of it** (§8) | phase record |

---

## 3 · The proto contract — pinned exactly

`multilanguage-test-server/proto/producer_service.proto:197-241` (the shared entry
and response types) and `consumer_service.proto:63-68, 102-106, 155-167, 244-252,
385-390`. **All backends must emit identical bytes; the Rust harness compares
them.**

### 3.1 `kind` — exactly five, lowercase

`"assigned"` · `"revoked"` · `"lost"` · `"commit"` · `"delivery"`.
Only the **first four** are reachable on `ConsumerService`; `"delivery"` is
producer-side and out of scope.

### 3.2 Field encoding

| Field | Contract |
|---|---|
| `partitions` | `repeated CallbackLogPartition {topic, partition}` — ⚠ **deliberately NOT `consumer_service.proto`'s `TopicPartition`.** `Translate.TpToProto` **cannot** be reused; write `CallbackLogPartitionToProto` |
| `offsets` | `map<string,int64>`, key **`"<topic>-<partition>"`** (plain hyphen: `"my-topic-0"`). **Empty** for `assigned`/`revoked`/`lost`; one entry per committed partition for `commit` |
| `error` | the callback's error message, or **empty string** when there was none. **Never absent** — always set the field |
| `CallbackLogResponse.entries` | **chronological, oldest first. Reading does NOT clear.** |

### 3.3 Lifecycle contract

- **`SubscribeRequest.with_listener` is per-subscribe**: a later listener-less
  `Subscribe` **releases** the registration; `Unsubscribe` does **not**. Entries
  already logged are unaffected.
- **`CommitAsyncRequest`**: empty `offsets` ⇒ commit current positions.
- **Entries survive `Close`** (roadmap Q9/D7) — see §5.1, which is where this
  becomes a concrete structural instruction rather than a principle.

### 3.4 Cross-check the merged proto, do not trust this table

`.claude/agent-memory/dotnet-critic/feedback_merged_state_rpc_contract_recheck.md`
records this exact class of miss biting M12/P1. **Re-derive the RPC list from the
proto as merged** and diff it against the `public override`s. If master has been
merged again since this brief was written, every "out of scope" claim in §1 needs
re-checking.

---

## 4 · Parity anchors, symbol by symbol

### 4.1 Python — the primary anchor for shape

| P9 deliverable | Python anchor | Cite |
|---|---|---|
| `CallbackLog` | `class CallbackLog` — `append(client_id, kind, partitions=(), offsets=None, error="")` + `response(client_id)`. One `threading.Lock`. **No `clear()`, no `snapshot()`** | `grpc_translate.py:287-332` |
| why the lock is mandatory | *"consumer callbacks are invoked from the Rust dispatcher thread … while GetCallbackLog is served on a gRPC worker thread — and in the async server the event loop is a **fourth** context"* | `grpc_translate.py:292-297` |
| retention | *"Entries are never dropped, **not even when the client is closed**: the callbacks a close() drives … are exactly the ones a test wants to read afterwards"* | `grpc_translate.py:322-328` |
| `OffsetKey` | `f"{topic}-{partition}"` | `grpc_translate.py:282-284` |
| `LoggingRebalanceListener` | three **plain, non-coroutine** methods; **`on_partitions_lost` implemented explicitly**, *not* left to the Java default, "so a lost callback is distinguishable from a revoke in the log" | `grpc_translate.py:344-372` |
| `make_logging_commit_callback` | `partitions=list(offsets.keys())`, `offsets={_offset_key(tp.topic, tp.partition): oam.offset}`, `error="" if exception is None else str(exception)` | `grpc_translate.py:375-388` |
| `Subscribe` handler | builds the listener iff `with_listener`, else passes `listener=None` **explicitly** (that is what releases a prior registration) | `grpc_server.py:288-297`; async `grpc_server_async.py:281-292` |
| `CommitAsync` handler | `c.commit_async(_proto_offsets_to_dict(request.offsets) or None, callback=callback)`. ⚠ **not awaited in the async server** — `commit_async` is a sync local op, so it deliberately bypasses `_run_status` | `grpc_server.py:331-341`; async `grpc_server_async.py:324-341` |
| `GetCallbackLog` handler | returns `self._callback_log.response(request.consumer_id)`; **never errors on an unknown id** (empty response); *"Readable after Close on purpose"* | `grpc_server.py:533-536`; async `:520-523` |

### 4.2 C — the better anchor for `user_data` lifetime

Roadmap §2.4 named this in advance. **C hit a real use-after-free here; Python is
structurally immune and gives no guidance.**

`9465e197` ("fix the LogState use-after-free"): the `Close` handlers `delete`d the
per-client `LogState` right after `..._destroy(client)`, justified by a comment
asserting that destroying the client drops the adapters so no callback can still
reference it. **The premise was false** — neither `Producer_destroy` nor
`Consumer_destroy` *joins* the dispatcher thread; both deliberately detach it, and
the Rust-side callback only *enqueues* the C callback as a dispatcher job. A queued
job could dereference freed memory.

**C's fix:** `LogState` became **session-lifetime** —
`unordered_map<uint64_t, unique_ptr<LogState>>` in both services, and `Close`
**neither erases nor frees it** (`server.cc:253-288`, `:842-853`, `:1281-1299`).

Also from C, worth copying: **the log's mutex is separate from the id-map's mutex**
(`server.cc:216-219`), so a callback firing mid-poll never queues behind a
`CreateConsumer` / `Close`. See §5.2.

⚠ **One C detail .NET must NOT copy:** C passes `user_data_destroy = nullptr`
(`server.cc:965-966`) because **C has no GC handle to free**. .NET's binding
already owns that lifetime internally (P6/P7); the server just holds managed
references.

### 4.3 Which managed API each RPC calls

P9 is the first phase consuming **both** prior surfaces. All verified present:

| RPC | Sync servicer calls | Async servicer calls |
|---|---|---|
| `Subscribe` (with_listener=false) | `IConsumer.Subscribe(topics)` `IConsumer.cs:99` | `IAsyncConsumer.Subscribe(topics, ct)` `:107` |
| `Subscribe` (with_listener=true) | `IConsumer.Subscribe(topics, listener)` **`IConsumer.cs:131`** | `IAsyncConsumer.Subscribe(topics, listener, ct)` **`:141`** |
| `CommitAsync` (no offsets, no cb) | `IConsumerCommon.CommitAsync()` `:160` | same |
| `CommitAsync` (no offsets, cb) | `IConsumerCommon.CommitAsync(callback)` **`:188`** | same |
| `CommitAsync` (offsets, ±cb) | `IConsumerCommon.CommitAsync(offsets, callback)` **`:210`** | same |
| listener implemented against | `IConsumerRebalanceListener` — `OnPartitionsRevoked` / `OnPartitionsAssigned` / `OnPartitionsLost`, all three **required**, all `void` (`IConsumerRebalanceListener.cs:87/98/109`) | same |
| commit callback implemented against | `IOffsetCommitCallback.OnComplete(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>, KafkaException?)` (`:87`) | same |

⚠ **`CommitAsync` is on `IConsumerCommon`, so it is identical in both servicers**
— it is **sync `void` on both flavors**, and the async servicer must **not** `await`
it. That matches Python's deliberate `_run_status` bypass, for the same reason.

⚠ **All three listener methods are required** (P6 settled D1 by shipping the
3-method interface plus `ConsumerRebalanceListenerBase`). `LoggingRebalanceListener`
must implement `OnPartitionsLost` **explicitly** anyway — Python is emphatic that a
`lost` must be distinguishable from a `revoked` in the log, so **do not** inherit
the base's delegating default here.

---

## 5 · The two structural traps

### 5.1 ⚠ `Close` evicts the `ConsumerEntry` — so the log CANNOT live on it

Verified: `ConsumerServiceImpl.Close` calls `_consumers.TryRemove(request.ConsumerId, out _)`
on **both** the success path (`:522`) and the failure path (`:539`); the async twin
does the same (`:594`). `ConsumerEntry` holds only `Consumer` and `Gate`
(`ConsumerServiceImpl.cs:658-663`, `AsyncConsumerServiceImpl.cs:747-754`).

**Therefore the callback log must be a SEPARATE map keyed by consumer id, held by
the servicer and never evicted** — not a field on `ConsumerEntry`. This is the
concrete, mechanical form of roadmap Q9/D7 and of C's `9465e197` fix. Putting the
log on `ConsumerEntry` would make every entry vanish at `Close`, breaking §3.2's
"entries survive Close" and silently failing the harness's post-close reads.

**Corollary:** the managed `LoggingRebalanceListener` / `LoggingCommitCallback`
instances are reachable from that same session-lifetime map, so they cannot be
collected while a straggler dispatcher callback is still queued.

### 5.2 ⚠ The log needs its OWN lock — and `GetCallbackLog` should be gate-exempt

Every RPC on a consumer takes that consumer's `ConsumerEntry.Gate`
(`lock (entry.Gate)` in the sync servicer; `await Gate.WaitAsync()` in the async
one, since a monitor cannot be held across an `await`). **`Wakeup` is already
gate-exempt** — precedent exists.

`GetCallbackLog` reads only the log, never the consumer. Taking the gate would
serialise it behind a long `Poll`, and the harness's `poll_until_kind` alternates
`Poll` and `GetCallbackLog` while waiting for a callback. **Make `GetCallbackLog`
gate-exempt, with the log's own lock** — which is what both anchors do (Python: a
dedicated `threading.Lock`, `grpc_translate.py:299`; C: a mutex separate from the
id-map's, `server.cc:216-219`).

**Four contexts touch the log** and none is the same thread (Python names all four
at `grpc_translate.py:292-297`): the Rust dispatcher thread (callbacks), a gRPC
worker thread (`GetCallbackLog`), the app thread driving an op, and — in the async
servicer — the event loop.

---

## 6 · The `CommitAsync` four-way branch

P7 established the ABI asymmetry; P9 is where it surfaces as a server branch,
exactly as it does in C.

| `offsets` | `with_callback` | Managed call |
|---|---|---|
| empty | false | `CommitAsync()` |
| empty | true | `CommitAsync(callback)` |
| non-empty | false | `CommitAsync(offsets, null)` |
| non-empty | true | `CommitAsync(offsets, callback)` |

**.NET is simpler than C here** — P7's binding already absorbed the non-nullable-
callback problem (the discard trampoline lives inside `NativeConsumer`), so the
server needs **no** `discard_commit_complete` analogue. C needs one only because it
talks to the raw ABI. Say so at the site, or a Critic comparing to `server.cc:376-380`
will ask where it went.

**Hoist `ProtoOffsetsToDictionary`.** Python shares `_proto_offsets_to_dict`
between `CommitSync` and `CommitAsync`; .NET currently **inlines** that conversion
in `CommitSync` (`ConsumerServiceImpl.cs:172-182`). Extract it first, then use it
from both — do not copy-paste.

### 6.4 The RPC-count tripwire

Both servicer class doc-comments state an RPC count. After a clean rebuild:

```
/usr/bin/grep -c Unimplemented obj/*/net8.0/ConsumerServiceGrpc.cs     # expect 26
```

then diff that set against `public override` — the difference must be **empty**.
The checked-in `obj/Release/net8.0/ConsumerServiceGrpc.cs` is **stale** (19 Aug,
24 virtuals); a clean rebuild yields 26. **Update both doc-comment counts in the
same commit** — that count *is* the tripwire.

⚠ **`grep` is aliased to `ugrep`**, which silently emits nothing on a rejected
pattern — a false *pass*. Use `/usr/bin/grep` for every evidence claim. And this
shell is **zsh**: unquoted `$var` is **not** word-split, so `set -- $pair` yields
nothing (this produced a false MISMATCH in a prior verification). Quote and use
arrays.

---

## 7 · Doc carry-forwards (roadmap §5.5)

### 7.1 `tests/common/callback_log.rs:41` — "all four backends"

The module doc enumerates `python / python_async / c` and concludes *"one generic
test body asserts the same thing against all four backends."* **There are now six**
consumer backends. Introduced by merge `a7efb0d5` — **not** by P5, whose Critic
correctly scoped it out. P9 is where .NET actually joins the log-reading backends,
so fix it here: correct the count and add the two dotnet arms to the enumeration.

**This is the only sanctioned `tests/` edit in P9** (§1) — a doc comment, no code.

### 7.2 `backend_factory.rs` — `DotnetGrpcFactory` doc

P5 refreshed it to say the consumer callback tests **do** apply to .NET but are not
yet served. **That caveat is now false** — remove or update it. Check both
`DotnetGrpcFactory` and `DotnetAsyncGrpcFactory`.

---

## 8 · D11 — `ffi-marshalling.md`: state the "none" explicitly

Roadmap §5.6 requires every phase to **name** the §§ it touches, and says a phase
that touches none must **say so** rather than leave the row blank — because a blank
row is indistinguishable from an omission, which is the failure mode that recurred
three times.

**P9's expected answer is: none.** The gRPC server is a **consumer of the binding's
public API**, not a boundary change — it declares no `[DllImport]`, no `SafeHandle`,
no delegate, and no `GCHandle`. Every native interaction is inside
`Confluent.Kafka`, already governed by P6/P7's §B2/§B5/§B6/§B7 updates.

**Record that finding, with that reasoning, in `COMMENTS.DONE.62.md`.** If the
Actor finds itself reaching for interop primitives, that is the signal it has
strayed out of §1's scope — stop and escalate.

---

## 9 · DoD, gates, and the Docker prerequisite

### 9.1 Gates

**`cargo xtask lint` already runs a second `--workspace --all-targets
--all-features` pass** (`xtask/src/main.rs:154-167`, since `4fd1435f` / #139).
**Do not add a separate clippy invocation.**

| Gate | Command |
|---|---|
| gRPC server build | `dotnet build bindings/dotnet/grpc-server` (net8.0) |
| format | `dotnet format --verify-no-changes` |
| binding regression | `dotnet test -f net10.0` (**543/543** at P7 — must not regress) |
| Rust no-regression | `cargo xtask lint` · `cargo test --all-features -- --skip __grpc` |
| **the phase gate** | `cargo test --features integration-tests,multilanguage-tests --test integration -- __grpc_dotnet` |

`dotnet` → `/usr/local/share/dotnet/dotnet` (SDK 10.0.302); `cargo` →
`~/.nix-profile/bin/cargo` (1.95.0). Neither is on the default `PATH`. net8.0 is
build-only locally; net10.0 is the execution gate.

### 9.2 The four tests that must go green

From `tests/integration/multilanguage_consumer_test.rs:595-599`:

- `test_ml_rebalance_listener_logs_assigned_and_revoked__grpc_dotnet{,_async}` —
  asserts an `assigned` entry naming `<topic_a>-0`, then (after a **replacing**
  subscribe to `topic_b`) a `revoked` entry naming `<topic_a>-0`; all entries
  `error`-empty. **This is what exercises §3.3's per-subscribe registration
  semantics** — a listener-less re-subscribe would break it.
- `test_ml_commit_async_callback_logs_offsets__grpc_dotnet{,_async}` — asserts a
  `commit` entry with `offset_for(topic, 0) == Some(2)` and `error` empty, then
  cross-checks against `committed()`.

### 9.3 ⚠ Docker prerequisites — verify, do not assume

1. **The staged Linux `.so` must carry the new symbols.** The image `COPY`s from
   `target-linux-amd64/`. Before building, check:
   `/usr/bin/nm -D target-linux-amd64/release/libconfluent_kafka.so | /usr/bin/grep -c ConsumerRebalanceListener`
   — a zero means the cross-build has not landed and **every test will fail with
   `EntryPointNotFoundException`**, not with a logic error. M9/P3's Actor lost time
   to exactly this.
2. **Build the image `linux/amd64`.** Grpc.Tools' `linux_arm64` protoc **SIGSEGVs**
   inside Apple-Silicon Docker. Set `DOCKER_DEFAULT_PLATFORM=linux/amd64` for the
   **build**.
3. **UNSET `DOCKER_DEFAULT_PLATFORM` for the test run**, so the broker stays native
   arm64. Cross-arch on one Docker network is fine and is the verified recipe.
4. The `sdk:10.0` builder fix is **already on this branch** in both
   `Dockerfile.grpc` and `Dockerfile.grpc.async` — that known NETSDK1045 blocker is
   closed; do not "fix" it again.
5. `target-linux*/` are **root-owned scratch dirs** — do not `git add`, do not
   `chown`.
6. Run `docker info` yourself before declaring any Docker gate CI-only
   (`project_dotnet_harness_ci_only_gates`: Docker availability is
   environment-dependent, and assuming it absent has been wrong before).

### 9.4 Test obligations

Root DoD #1, #2, #3, #5, #8, #9 apply. **#10 N/A** (no per-record path in a
harness server) — state it. **#11 N/A** (no binding trait change) — state it.
**#12 applies**: the logging listener/callback are fixtures standing in for user
code, so they must call the same public API a user would — no internal shortcut.

Beyond the four harness tests: **no new .NET unit tests are expected**, because
`grpc-server` is not in `Confluent.Kafka.sln`
(`bindings/dotnet/.claude/agent-memory/dotnet-actor/project_grpc_server_not_in_sln_blocks_servicer_tests.md`
records that this blocks servicer unit tests). **If that is still true, say so
explicitly** rather than silently shipping without unit coverage — the harness
tests are then the only gate, which is a fact the phase record should carry.

---

## 10 · ⚠ DECISIONS FOR THE MAINTAINER

### P9-D1 — One new file or two?

Roadmap Q8 settled that `CallbackLog` gets its own file (not `Translate.cs`). It
did **not** settle where the two logging *implementations* go.

- **(a) One `CallbackLog.cs`** holding the log + both logging types. Fewest files;
  they are a single cohesive fixture. Python does this (all three in
  `grpc_translate.py`).
- **(b) `CallbackLog.cs` + `LoggingCallbacks.cs`.** Separates the storage
  mechanism from the two `Confluent.Kafka`-interface implementations.

**Recommendation: (a).** They are one fixture with one purpose, and (a) is closer
to both anchors. Flagged only because Q8's ruling was about `Translate.cs` and does
not reach this.

### P9-D2 — Does `GetCallbackLog` take the per-consumer `Gate`?

§5.2 argues it should **not**. But every other consumer-scoped RPC does, and
`Wakeup` is currently the only documented exemption — so adding a second exemption
is a deliberate widening of that precedent.

- **(a) Gate-exempt, own lock** (recommended, §5.2) — matches both anchors; avoids
  serialising a log read behind a long `Poll`.
- **(b) Take the gate** — uniform with every other RPC, at the cost of the above.

**Recommendation: (a)**, with the exemption documented next to `Wakeup`'s.

### P9-D3 — What does `GetCallbackLog` do for an unknown / closed consumer id?

Python **never errors**: it returns an empty response for any unknown id
(`grpc_server.py:533-536`), which is also what makes post-`Close` reads work. But
every other .NET handler returns `Translate.UnknownConsumer(id)`
(`Translate.cs:139`) for an unknown id, so (a) is a deliberate inconsistency.

- **(a) Never error; return whatever the log holds** (empty if nothing). Python
  parity; required for the post-`Close` read in §3.2.
- **(b) Error on unknown id** — consistent with the other handlers, but **breaks
  the post-`Close` contract** unless the log map is consulted first, at which point
  it is (a) with extra steps.

**Recommendation: (a)**, with a comment explaining why this handler differs — the
log outlives the consumer *by design*, so "unknown consumer" is not an error
condition for it.

### P9-D4 — Should the session-lifetime log be bounded?

Python's justification is explicit: *"The server's lifetime is one test session, so
the map cannot grow meaningfully"* (`grpc_translate.py:326-328`). C reasons
identically. Both accept unbounded growth.

- **(a) Unbounded, matching both anchors.** Simplest; the premise (one test
  session) is true for this harness.
- **(b) Cap entries per id.** Defensive, but diverges from both anchors and could
  silently drop the entry a test is waiting for — turning a real failure into a
  confusing one.

**Recommendation: (a).** Flagged because "a map that is never cleared" reads as a
leak to a reviewer who has not seen the anchors' reasoning — so whichever is
chosen, **the reasoning must be in the code**, not just here.

---

## 11 · Mechanics

- Commit per step; `fixup!` referencing the original commit when closing a
  `COMMENTS.62.md` item.
- `bindings/dotnet/COMMENTS.62.md` → `COMMENTS.DONE.62.md`; **never `git add`
  either.** The Manager archives the DONE file to `design/history/M9/P9/`.
- Personas must be copied to the repo-root `.claude/agents/` to be invocable, and
  those root copies must **never** be committed.
- Required reading:
  `.claude/agent-memory/actor-executor/ffi_callback_bridging_phase7_notes.md`
  (item 5 — the proto import-cycle constraint that forced `CallbackLogPartition`
  to exist; item 6 — why `create_with_callback_log` has the shape it does), and
  `.claude/agent-memory/dotnet-critic/feedback_merged_state_rpc_contract_recheck.md`
  (§3.4).
- **P8 (`ConsumerHandle`, N=61) remains outstanding** after this phase — P9 does
  not close the roadmap.
