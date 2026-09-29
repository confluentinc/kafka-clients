# M9/P5–P9 — .NET consumer callback-bridging parity (PR #143 phase 7)

**Status:** **PROPOSED — NOT APPROVED.** No Actor or Critic has been spawned. No
source file has been modified. Nine decisions in §0 need the maintainer's ruling
before any phase starts.

**Branch:** `prashah_dev_dotnet_binding_consumer` @ `a7efb0d5`
(`Merge branch 'master' into prashah_dev_dotnet_binding_consumer`; parents
`5e7e4bae` = branch, `7161aae9` = `Merge pull request #143 from confluentinc/ffi-callback-bridging`).

**Agent numbers:** N=58 · 59 · 60 · 61 · 62 (next free in the binding's own
sequence — highest used is 57; see §9).

**Mode:** **A** for every phase except P5, which touches one Rust file under
`tests/` (harness glue, not the core — see §0 Q2 and §8).

**Location note.** This is a multi-phase roadmap, so it lives in
`design/current/` rather than a single `design/history/<M>/<P>/`. On approval the
Manager splits it: one `PLAN.md` per phase under
`design/history/M9/<Phase>/`, per `bindings/dotnet/CLAUDE.md §8.4`.

---

## 0 · OPEN QUESTIONS — decisions needed before any phase starts

Ordered by how much they change the plan. Q1 is a hard environmental blocker;
Q5 is a genuine contradiction inside `bindings/dotnet/CLAUDE.md` that must be
resolved by a human, not by an Actor picking a side.

### Q1 — RESOLVED 2026-08-29 — stale header/natives. (⚠ The "no toolchain" diagnosis was WRONG.)

**Decision: option (a). Done — the native and header were rebuilt; P6–P9 are unblocked.**

> **⚠ CORRECTION — do not propagate the original text.** This question was first
> written as "there is no Rust toolchain in this environment." **That was false.**
> A nix-installed **Rust 1.95.0** was present the whole time at
> `~/.nix-profile/bin/{cargo,cargo-clippy,rustfmt}` — matching
> `rust-toolchain.toml` (`channel = "1.95.0"`, `components = ["clippy", "rustfmt"]`)
> exactly. The broken shell init hides `~/.nix-profile/bin` from `$PATH`, so
> `command -v cargo` returned nothing and two agents in a row concluded the
> toolchain was missing. **Always add `$HOME/.nix-profile/bin` to `PATH` before
> concluding a tool is absent in this sandbox.**
>
> The *remedy* was nonetheless correct and necessary — the artifacts genuinely
> were stale. `cargo build --features ffi` was run (green, 43 s) and
> `target/include/confluent_kafka.h` regenerated (29 Aug 19:12, 159 563 bytes),
> now carrying **29** `ConsumerHandle_`, **31** `ConsumerRebalanceListener_`,
> **2** `KafkaError_new`, **1** `MockConsumer_rebalance`, **10**
> `commit_async_with_callback`, **11** `subscribe_with_listener`.
>
> **Still outstanding — a P9 prerequisite, not a P6/P7/P8 one:** the two staged
> Linux cross-build trees are *still* stale and must be cross-rebuilt before any
> P9 Docker test can pass. See §5.5.

The original (now-superseded) reasoning is kept below because the artifact-staleness
half of it is what made the rebuild necessary:

- `target/include/confluent_kafka.h` is dated **27 Aug**, predates the merge, and
  contains **zero** of the new symbols
  (`grep -c kafka_consumer_ConsumerHandle_` → 0; no `ConsumerRebalanceListener`,
  no `commit_async_with_callback`, no `Consumer_handle`, no
  `MockConsumer_rebalance`, no `KafkaError_new`).
- `target/debug/libconfluent_kafka.dylib` (27 Aug) and
  `target/release/libconfluent_kafka.dylib` (28 Aug) **also predate the merge**.
  `nm -gU` finds 145 `kafka_consumer_*` exports but **none** matching
  `RebalanceListener|ConsumerHandle|with_callback|with_listener|MockConsumer_rebalance`.
- Therefore **any new `[DllImport]` added in P6–P8 will throw
  `EntryPointNotFoundException` at test time** against the natives currently on
  disk. The .NET unit-test suite cannot go green without a rebuilt native.
- The gRPC Docker images `COPY` a host-cross-built Linux `.so`, and **both
  staged cross-build trees are stale too**:
  `target-linux-amd64/release/libconfluent_kafka.so` (19 Aug) and
  `target-linux/release/libconfluent_kafka.so` (11 Aug) each match **0** of the
  new symbols. So P9's Docker gate is blocked by the same thing.
- The P5 harness fix cannot be compile-verified locally either.

**Options:**
  - **(a)** Install a Rust toolchain on this machine before starting
    (`rustup` + `cargo build --features ffi` / `--release` + a Linux
    cross-target for the images). Cleanest; unblocks everything.
  - **(b)** Have the root `actor-executor` (which may have a toolchain elsewhere)
    produce and stage the header + natives as a prerequisite artifact, and treat
    the regen as a Rust-core dependency (§8).
  - **(c)** Proceed "blind": author P6–P8 against `src/ffi/*.rs` as the source of
    truth (which is legitimate per `bindings/dotnet/CLAUDE.md §1`, "Source of
    truth for the surface = `src/ffi/*.rs` + `cbindgen.toml`"), build the C#,
    and defer **every runtime test** to CI. **Not recommended** — it would defer
    the entire test obligation of three phases, and §7.4's mock round-trip is the
    binding's only real gate.

**Manager's recommendation: (a).** Everything downstream assumes a rebuilt
native; without one, P6–P8 close with an untestable claim.

### Q2 — Does the P5 CI fix ship immediately as its own commit, or wait for the parity work?

The break is a **compile** error, not a test failure (see §1.4). It breaks
`make verify-rust` on **both** CI jobs (Linux amd64 `:106`, macOS arm64 `:149`),
because `verify-rust` → `build-rust-all-features` / `test-rust-all-features` →
`cargo build/test --all-features`, and `--all-features` turns on
`multilanguage-tests`, which compiles `tests/common/backend_factory.rs`.
`--skip __grpc` skips *running*, not *compiling*.

The fix is two four-line method bodies. Everything else in this plan is weeks of
work behind it.

**Sub-question:** the file is Rust. `bindings/dotnet/CLAUDE.md §8.1` says the
`dotnet-actor` "does not author Rust." But there is a standing precedent: M12/P1
(N=34) had the **dotnet-actor author `tests/common/backend_factory.rs` and
`tests/common/multilanguage_test_macro.rs` itself**, as an approved exception on
the M8 precedent, because harness glue is test infrastructure rather than core
translation. Confirm the exception still applies, or route P5 to the root
`actor-executor`.

**Manager's recommendation: ship P5 immediately, standalone, dotnet-actor with
the M12/P1 exception restated in the phase plan.** A red `verify-rust` masks
every other regression on the branch.

### Q3 — Scope: full parity, or a narrow "make the harness green" slice?

The full inventory (§1) is four distinct deliverables. A narrower slice is
coherent:

| Slice | Contents | Makes the 4 dotnet callback tests pass? | Achieves Python parity? |
|---|---|---|---|
| **Narrow** | P5 + P6 (listener) + P7 (commit callback) + P9 (server) | **Yes** | No — no `ConsumerHandle`, no `MockConsumer.Rebalance` |
| **Full** | P5 + P6 + P7 + P8 (`ConsumerHandle`) + P9 | Yes | Yes |

The narrow slice is genuinely sufficient for the harness: the logging listener
and logging commit callback **do not call back into the consumer** (Python's
`LoggingRebalanceListener` is explicitly documented as not touching the consumer;
C's trampolines only append to a log). `ConsumerHandle` is needed for *users*,
and for `consumer-threading.md §31`'s first mandatory regression test.

**Cost of deferring P8:** `consumer-threading.md §31` "Tests required" #1 ("the
listener calls `commit_sync()` from inside the callback and it succeeds") has no
vehicle in .NET without a reentrancy handle — the consumer's own `Commit()` is
rejected with `ConcurrentModification` by the core's access guard, by design.
That obligation would have to be explicitly deferred with a written rationale,
which a Critic will otherwise file as a defect.

**Manager's recommendation: full, but sequenced so the narrow slice lands
first** (P5 → P6 → P7 → P9 → P8), so the harness goes green before the
largest-surface phase starts. If effort must be cut, cut P8, not P9.

### Q4 — Do the four dotnet callback tests get temporarily excluded in the interim?

Once P5 lands, `multilanguage_consumer_test!` generates
`test_ml_rebalance_listener_logs_assigned_and_revoked__grpc_dotnet{,_async}` and
`test_ml_commit_async_callback_logs_offsets__grpc_dotnet{,_async}`. Until P9 they
**panic** (gRPC `UNIMPLEMENTED` → `.expect(...)`).

Mitigating fact, verified: **no CI job runs them today.** `.semaphore/semaphore.yml`
contains no `verify-dotnet` job (`grep -rn dotnet .semaphore/` → empty) and the
root `Makefile` has no `test-integration-dotnet` target (`grep -n dotnet Makefile`
→ only the two `grpc-image` delegations at `:83-84`). They are compiled by
`--all-features` and run only if someone invokes `cargo test … -- __grpc_dotnet`
by hand, as M9/P3 did.

**Options:**
  - **(a)** Do nothing. Accept 4 hand-runnable tests that panic between P5 and
    P9. Zero code cost; the panic message is self-explanatory
    (`IllegalState("dotnet gRPC backend transport error (Unimplemented): ")`).
  - **(b)** Add a per-test backend opt-out to `multilanguage_consumer_test!`
    (a `multilanguage_consumer_test_no_dotnet!` variant, mirroring how
    `multilanguage_test!` keeps .NET out of the producer matrix *by omission*),
    then remove it in P9. Matches the harness's stated "fail-safe by omission"
    philosophy but is a second Rust change plus a revert.
  - **(c)** Guard at runtime on `factory.name()`. Cheapest to write, worst to
    review — a silent early-return that could outlive its reason.

**Manager's recommendation: (a).** The window is bounded by P9, nothing in CI
consumes it, and every alternative adds Rust churn that must then be reverted.
If (a) is chosen, P5's commit message must state the window explicitly.

### Q5 — `OffsetCommitCallback`: `bindings/dotnet/CLAUDE.md` contradicts itself. Which wins?

Two rows in the same rulebook say opposite things:

- **§3 idiom map**: `` | `OffsetCommitCallback` | `IOffsetCommitCallback` (async) | same caller's-task model — consumer-threading §31 | ``
- **§4 Sync-vs-async table**: `` | **Takes a completion callback** — even if non-blocking (`send(record, Callback)`, `commitAsync(OffsetCommitCallback)`) | `Task`/`Task<T>` on the async interface — the `Task` **replaces** the callback; do **not** add a callback-taking overload | ``

§3 says build the interface; §4 says explicitly do not.

**Why it cannot be resolved by an Actor:** the harness contract requires the
*offsets* the callback observed (`CallbackLogEntry.offsets` must contain
`"<topic>-0" -> 2`, asserted at
`tests/integration/multilanguage_consumer_test.rs:582-585`). A bare `Task` from
`CommitAsync()` carries no offsets, so the "Task replaces the callback" reading
would force either a `Task<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>>`
(no Java counterpart) or server-side reconstruction of the offsets (which would
test the harness, not the binding — exactly what phase 7's proto comment says it
is avoiding).

**Options:**
  - **(a)** `IOffsetCommitCallback` wins (§3). `void CommitAsync(IOffsetCommitCallback? callback = null)`
    and `void CommitAsync(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets, IOffsetCommitCallback? callback = null)`
    on `IConsumerCommon`. Exact Java shape (`commitAsync()`, `commitAsync(cb)`,
    `commitAsync(Map, cb)`) and exact Python parity
    (`commit_async(offsets=None, callback=None)`). §4's row gets an explicit
    carve-out amendment noting that the commit family is Java's own sync/async
    *pair* — which §4 already carves out once for `Commit`/`CommitAsync`.
  - **(b)** `Task`-returning wins (§4). Diverges from Java, Python, and C; forces
    an invented return type; the .NET server would then have to synthesize the
    log entry.

**Manager's recommendation: (a), with a documented amendment to §4's row.**
Note this is a rulebook edit — root `CLAUDE.md` forbids agents changing the
prompt, and `bindings/dotnet/CLAUDE.md` is governed the same way, so the
maintainer must sanction the amendment text.

### Q6 — Is `IConsumerRebalanceListener` async or sync?

`bindings/dotnet/CLAUDE.md §3` says "`IConsumerRebalanceListener` (async)", and
`consumer-threading.md §31` defines the Rust trait as `#[async_trait]`. But at
the FFI boundary the three listener callbacks are **synchronous C function
pointers** invoked on the core's dispatcher thread, returning
`kafka_common_KafkaError_t*` (`src/ffi/consumer.rs:2980-3004`), and the rebalance
**does not proceed until the callback returns** (`src/ffi/consumer.rs:2967-2979`).

An `async` .NET listener means the trampoline must block the dispatcher thread on
the `Task` — which is exactly what Python does
(`run_coroutine_threadsafe(...).result()`, `consumer.py:307-317`) and exactly
what Python's own `LoggingRebalanceListener` docstring warns is a deadlock
source, which is why *both* Python servers use plain synchronous methods.

**Options:**
  - **(a)** **Sync** `IConsumerRebalanceListener` (`void OnPartitionsRevoked(IReadOnlyCollection<TopicPartition>)`,
    `…Assigned`, `…Lost` with a default-interface-method delegating to Revoked per
    Java). Matches the ABI 1:1, no dispatcher blocking, no deadlock class. Note
    C# default interface methods require net8.0+; on the netstandard2.0 floor
    the delegation must be implemented in the trampoline instead (which is what
    Rust does at `src/ffi/consumer.rs:3150-3157` when `on_lost` is null).
  - **(b)** **Async** `Task`-returning, with the trampoline doing
    `.GetAwaiter().GetResult()` on the dispatcher thread. Matches §3's wording
    and consumer-threading §31's Rust-side shape, at the cost of a documented
    deadlock hazard for any listener that awaits a consumer op without a handle.

**Manager's recommendation: (a) sync.** The ABI is synchronous; §3's "(async)"
row describes the *Rust core's* trait, which the C ABI has already flattened
(`bindings/CLAUDE.md §1.2` — the shape is flattened at the ABI and restored by
the binding, and here the faithful restoration of "blocks until it returns" *is*
a sync method). This is a declared divergence (§3.2), not an oversight, and must
be written down in the phase plan.

### Q7 — `ConsumerHandle` lifetime: ref-count, or document-and-hope?

The ABI contract (`src/ffi/consumer_handle.rs:59-70`) is explicit: *"Destroy every
handle **before** destroying the consumer."* .NET cannot force user ordering.

**Options:**
  - **(a)** The managed `ConsumerHandle` takes a `DangerousAddRef` on the
    `SafeConsumerHandle` for its own lifetime, released in `Dispose`. This slots
    exactly into the M9/P4 (N=41) ref-counted release: `ReleaseHandle` →
    `Consumer_destroy` already runs only at count zero. A live handle then
    *defers* the consumer destroy rather than dangling it. Costs one more
    deferred-destroy path to document alongside the two already-accepted
    residuals.
  - **(b)** Mirror Python exactly: `IDisposable` + `using`, documented ordering,
    no ref-count. Simpler, but reintroduces a use-after-free class that M9/P4
    deliberately closed for every other path.

**Manager's recommendation: (a).** M9/P4's whole thesis was that the pre-M9/P4
shape let a raw pointer outlive a concurrent destroy; shipping a new long-lived
raw-pointer holder would undo it.

### Q8 — Does P9 own a `CallbackLog` in `Translate.cs`, or a new file?

Python puts `CallbackLog` + `LoggingRebalanceListener` + `make_logging_*` in
`grpc_translate.py` (the shared translate module). C puts them inline in
`server.cc`. .NET's `Translate.cs` is a `static class` of pure functions;
`CallbackLog` is stateful and `LoggingRebalanceListener` is a type.

**Options:** (a) put them in `Translate.cs` for literal Python parity;
(b) a new `CallbackLog.cs` in `grpc-server/`, with the constants + `OffsetKey`
in `Translate.cs`.

**Manager's recommendation: (b).** `Translate.cs`'s doc comment describes it as
translation helpers; a mutable per-service log is a different concern. This is a
file-organization divergence from Python, declared up front per §3.

### Q9 — Should `Program.cs` / the servicers keep the log state alive past `Close`?

Both reference backends say yes, for the same reason and with different
mechanisms. C hit an actual **use-after-free** here and fixed it in `9465e197`:
`Consumer_destroy` does **not** join the dispatcher thread, so a queued callback
job can still dereference `user_data` after `Close`. C's fix made `LogState`
session-lifetime (`unordered_map<uint64_t, unique_ptr<LogState>>`, never erased
on `Close`). Python never had the bug (it appends in-process) but keeps entries
past `Close` deliberately.

.NET is closer to C's exposure than Python's: the trampoline's `GCHandle` is
native-visible and a straggler dispatcher callback would resurrect it.

**Manager's recommendation: yes — session-lifetime log + session-lifetime
`GCHandle`, never freed at `Close`,** matching C. This is a rare case where the
C backend, not Python, is the closer anchor; call it out explicitly so a Critic
comparing only against Python does not file the retained `GCHandle` as a leak.

---

## 1 · Verified gap inventory

Everything below was verified against the working tree at `a7efb0d5`. **Six
claims in the briefing are corrected**; they are marked ⚠ and collected in §1.6.

### 1.1 Layer 1 — Rust core C ABI: **COMPLETE. No Rust core work needed.** ✅

`git diff master HEAD -- src/ffi/` is empty; the branch's FFI is byte-identical
to master. `src/ffi/` = `common.rs` (798) · `consumer.rs` (5077) ·
`consumer_handle.rs` (1035, new in PR #143) · `producer.rs` (4261) · `mod.rs` (32).

| Symbol | Location |
|---|---|
| `kafka_consumer_ConsumerRebalanceListener_t` (opaque) | `src/ffi/consumer.rs:3066-3069` |
| `…_on_partitions_revoked_callback_t` / `…_assigned_…` / `…_lost_…` | `:2980-2981` / `:2991-2992` / `:3003-3004` |
| `…_user_data_destroy_t` | `:3056` |
| `kafka_consumer_ConsumerRebalanceListener_new` (5 params) | `:3215-3236` |
| `kafka_consumer_ConsumerRebalanceListener_destroy` | `:3250-3257` |
| `kafka_consumer_Consumer_subscribe_with_listener` | `:3315-3320` |
| `kafka_consumer_Consumer_subscribe_with_listener_async` | `:3340-3347` |
| `kafka_consumer_Consumer_commit_async_callback_t` `(OffsetMap*, KafkaError*, void*)` | `:3829-3830` |
| `kafka_consumer_Consumer_commit_async_user_data_destroy_t` | `:3848` |
| `kafka_consumer_Consumer_commit_async_with_callback` | `:3958-3963` |
| `kafka_consumer_Consumer_commit_async_offsets_with_callback` | `:3989-4000` |
| `kafka_consumer_MockConsumer_rebalance` | `:1399-1404` |
| `kafka_consumer_Consumer_handle` | `src/ffi/consumer_handle.rs:230` |
| `kafka_consumer_ConsumerHandle_t` (opaque) | `:118-120` |
| the 22 `ConsumerHandle_*` functions | `:247`–`:685` (table in §5.4) |
| ⚠ **`kafka_common_KafkaError_new(i32, const char*)`** | `src/ffi/common.rs:118-121` |
| ⚠ `kafka_consumer_Consumer_seek_with_metadata_async` | `src/ffi/consumer.rs:3486` |

**⚠ Correction 1 — the briefing missed `kafka_common_KafkaError_new`.** It is
required: a listener callback signals failure by *returning* a
`kafka_common_KafkaError_t*` it constructed (`src/ffi/consumer.rs:2960-2965` —
ownership transfers to the client; do **not** destroy a handle you return). A
.NET listener that throws must be marshalled into one of these, or the throw
becomes an unwind into native (UB).

**⚠ Correction 2 — `kafka_consumer_Consumer_seek_with_metadata_async` also
landed in PR #143.** An unrelated gap-fill riding along; out of scope here but
worth knowing it is now available (it was previously listed as a sync-only gap
in `bindings/dotnet/CLAUDE.md §1`).

**Constraint that shapes the test plan (§6.3):** on a **MockConsumer-derived
handle**, every *async* `ConsumerHandle_*` op fails with `UnsupportedVersionError`
(`src/ffi/consumer_handle.rs:82-86`; corroborated by the phase-4 notes). Only
`wakeup` and the three sync getters (`assignment`/`subscription`/`paused`) work.
So in-callback reentrancy **cannot** be unit-tested against `MockConsumer`.

### 1.2 Layer 2 — .NET managed binding: **all six claimed gaps CONFIRMED.**

| # | Gap | Evidence |
|---|---|---|
| 1 | No P/Invoke for any of the 9 new symbol families | `NativeMethods.cs` holds 147 `static extern` decls; **0** match. `ConsumerSubscribe` `:1021`, `ConsumerSubscribeAsync` `:230`, `ConsumerCommitAsync` `:1382` are the plain forms only |
| 2 | No `IConsumerRebalanceListener` | `grep -rni rebalancelistener bindings/dotnet/src` → **0 hits**. Every `rebalance` hit is `EnforceRebalance`. Already flagged as pending at `IAsyncConsumer.cs:60` and `bindings/dotnet/CLAUDE.md §3` |
| 3 | No `Subscribe(topics, listener)` | Exactly one overload each: `IConsumer.cs:99` `void Subscribe(IReadOnlyCollection<string>)`, `IAsyncConsumer.cs:107` `Task Subscribe(IReadOnlyCollection<string>, CancellationToken)` |
| 4 | No managed `ConsumerHandle` | 0 of 22 `ConsumerHandle_*` declared; no managed type |
| 5 | `CommitAsync` not completion-observable | `IConsumerCommon.cs:160` → `void CommitAsync()`. `NativeConsumer.cs:1084-1097` calls only `NativeMethods.ConsumerCommitAsync` (`EntryPoint = "kafka_consumer_Consumer_commit_async"`). All four public classes are one-line forwarders (`KafkaConsumer.cs:200`, `AsyncKafkaConsumer.cs:207`, `MockConsumer.cs:201`, `AsyncMockConsumer.cs:193`) |
| 6 | No `MockConsumer.Rebalance` | Mock helpers are exactly five — `UpdateBeginningOffset`, `UpdateEndOffset`, `UpdatePartitions`, `AddRecord`, `SetPollError` (`MockConsumer.cs:221-280`, `AsyncMockConsumer.cs:213-278`). `EnforceRebalance` is a KIP-848 **logged no-op** and cannot substitute |

**⚠ Correction 3 — the briefing's assumed producer precedent does not exist.**
There is **no producer in the .NET binding on this branch**: no `*Producer*.cs`
source file anywhere under `bindings/dotnet/src/`, no `SendCompletionPump`, no
producer dispatcher. `bindings/dotnet/CLAUDE.md §3`'s producer sketch is a
*target*, and `design/current/producer-send-completion-approaches.html` is a
design doc. **The precedent to mirror is the consumer's own completion bridge**
— see §2.3.

### 1.3 Layer 3 — .NET gRPC server: **all three claimed gaps CONFIRMED.**

`bindings/dotnet/grpc-server/` = `Program.cs` (169) · `ConsumerServiceImpl.cs`
(669) · `AsyncConsumerServiceImpl.cs` (778) · `Translate.cs` (361) ·
`Confluent.Kafka.GrpcServer.csproj` (68).

- **24 `public override` RPCs in each servicer.** Neither list contains
  `CommitAsync` or `GetCallbackLog`.
- **`Subscribe` drops `with_listener` silently** — `ConsumerServiceImpl.cs:127-128`
  and `AsyncConsumerServiceImpl.cs:173-174` are both
  `RunStatus(request.ConsumerId, c => c.Subscribe(new List<string>(request.Topics)))`.
- `grep -rn "CommitAsync|GetCallbackLog|WithListener|CallbackLog|Rebalance"` over
  the four `.cs` sources → **no matches**.
- **The csproj needs no change.** `consumer_service.proto` is already
  `GrpcServices="Server"` (`:57`) and `producer_service.proto` is
  `GrpcServices="None"` (`:56`) — messages-only, which is what makes
  `CallbackLogEntry` / `CallbackLogPartition` / `CallbackLogResponse` available
  without serving `ProducerService`.
- The checked-in `obj/Release/net8.0/ConsumerServiceGrpc.cs` is **stale**
  (19 Aug): 24 virtuals, 24 `Unimplemented` throws. A clean rebuild yields **26**.
- **`ProducerService.GetCallbackLog` is a non-issue here** — the .NET backend has
  no `ProducerBackendFactory` impl and serves no `ProducerService`, so the
  producer-side callback test (`test_delivery_callback_logs_metadata`, registered
  via `multilanguage_test!`, which emits only 4 arms) never reaches it. *This
  assumption was re-derived against the merged proto, not inherited* — per
  `.claude/agent-memory/dotnet-critic/feedback_merged_state_rpc_contract_recheck.md`,
  which records this exact class of mistake biting M12/P1 with `Metrics`.

### 1.4 Layer 4 — Rust harness: the compile break, precisely

`ConsumerBackendFactory::create_with_callback_log`
(`tests/common/backend_factory.rs:64-67`) is a **required** trait method with
**no default body**. `DotnetGrpcFactory` (`:423`) and `DotnetAsyncGrpcFactory`
(`:463`) do not implement it → two `error[E0046]`.

Merge arithmetic confirming the semantic conflict:

```
git diff --stat 5e7e4bae a7efb0d5 -- tests/common/backend_factory.rs
  1 file changed, 111 insertions(+)            # master's create_with_callback_log work

git diff --stat 7161aae9 a7efb0d5 -- tests/common/backend_factory.rs
  1 file changed, 82 insertions(+), 1 deletion(-)   # the two Dotnet factories
```

No textual overlap → clean merge, broken semantics.

**⚠ Correction 4 — this is a *compile-only* break, and it breaks the Rust jobs,
not a .NET job.** There is **no `verify-dotnet` job** in
`.semaphore/semaphore.yml` on this branch and **no `test-integration-dotnet`
target** in the root `Makefile`. The failing path is
`.semaphore/semaphore.yml:106` / `:149` → `make verify-rust` →
`Makefile:337` → `cargo build --all-features --release` +
`cargo test --all-features -- --skip __grpc`.

**⚠ Correction 5 — `8de4dced` is not in PR #143's history.**
`git merge-base --is-ancestor 8de4dced 7161aae9` → **no**. It is the
**pre-rebase identity** of the phase-7 commit; the commit that is actually in
the branch is **`7eb83969`** ("multilanguage: callback-bridging phase 7 —
GetCallbackLog RPC + callback coverage across backends"), plus the fixup
`9465e197`. Cite `7eb83969` in commit messages, not `8de4dced`.

**⚠ Correction 6 — the dotnet arms ARE generated for the consumer callback
tests.** `tests/common/multilanguage_consumer_test_macro.rs:18-22` and `:91-117`
emit **six** unconditional arms per test, including `__grpc_dotnet` and
`__grpc_dotnet_async`. So after P5 there will be **4 new test wrappers** (2
bodies × 2 dotnet backends) that panic until P9:

- `test_ml_rebalance_listener_logs_assigned_and_revoked__grpc_dotnet{,_async}` —
  `Subscribe{with_listener:true}` returns OK (flag ignored), then the first
  `GetCallbackLog` in `poll_until_kind` panics at
  `multilanguage_consumer_test.rs:127` (`read consumer callback log`).
- `test_ml_commit_async_callback_logs_offsets__grpc_dotnet{,_async}` — panics
  earlier, at `:556` (`commit_async with logging callback`).

Both surface as
`IllegalState("dotnet gRPC backend transport error (Unimplemented): ")` via
`tests/common/multilanguage_producer.rs:391-398`.

### 1.5 Was .NET deliberately deferred by phase 7?

**No — it was invisible.** Zero commits in `7161aae9^1..7161aae9^2` touch
`bindings/dotnet/**`; `git diff --stat bcd9e552 6f510a01 -- bindings/dotnet` is
empty. `DotnetGrpcFactory` was introduced by `afbd79c3` on a **parallel**
branch that is neither an ancestor nor a descendant of PR #143. The phase-7
commit body says "each fanned out to **all four** backends" — the author's world
had four. There is **no "deferred to .NET" note to quote**, and PR #143 committed
**no design doc**: its plan lived at `~/.claude/plans/wiggly-bouncing-babbage.md`,
which does not exist on this machine. What it did commit is seven per-phase
memory notes at `.claude/agent-memory/actor-executor/ffi_callback_bridging_phase{1..7}_notes.md`
— those are the closest thing to a design record and are required reading for
the Actors (§5).

### 1.6 Corrections to the briefing, collected

1. Layer 1 is complete **but** the briefing's symbol list missed
   `kafka_common_KafkaError_new` (load-bearing) and
   `kafka_consumer_Consumer_seek_with_metadata_async`.
2. The stale artifact problem is **worse than "stale header"**: the **native
   libraries are stale too**, and **cargo is not installed** (Q1).
3. The .NET binding has **no producer**; the "producer callback precedent" does
   not exist. Use the consumer's own bridge.
4. The CI break is **compile-only** and breaks **`verify-rust`**, not a .NET job;
   there is no dotnet CI job on this branch at all.
5. `8de4dced` is not in PR #143 — the real commit is `7eb83969`.
6. The dotnet arms **are** generated for the two consumer callback tests
   (the briefing was unsure). They are *not* generated for the producer one.

---

## 2 · The parity anchor (pinned up front, reviewable)

Per `.claude/agent-memory/project-manager/feedback_binding_phase_parity_preflight.md`:
pin the anchor before implementation and make the Critic review *against it*
rather than re-derive it. **The anchor is normative for this whole plan. A Critic
finding must cite a specific anchor line or a declared divergence in §3.**

### 2.1 The wire contract (binding on all backends)

`multilanguage-test-server/proto/producer_service.proto:197-241` and
`consumer_service.proto:63-68, 102-106, 155-167, 244-252, 385-390`.

- **`kind` vocabulary, exactly 5, lowercase:** `"assigned"`, `"revoked"`,
  `"lost"`, `"commit"`, `"delivery"`. Only the first four are reachable on
  `ConsumerService`.
- **`offsets` encoding:** `map<string,int64>`, key `"<topic>-<partition>"`
  (plain hyphen, e.g. `"my-topic-0"`). **Empty** for `assigned`/`revoked`/`lost`;
  populated for `commit` (one per committed partition).
- **`error`:** the callback's error message, or **empty string** when no error.
  Never absent.
- **`partitions`:** `CallbackLogPartition{topic, partition}` — deliberately
  **not** `consumer_service.proto`'s `TopicPartition`, so `Translate.TpToProto`
  **cannot** be reused.
- **`CallbackLogResponse.entries`:** chronological, oldest first. **Reading does
  not clear.** Entries survive `Close`.
- **`SubscribeRequest.with_listener`** is per-subscribe: a listener-less
  `Subscribe` *releases* the registration; `Unsubscribe` does **not**.
- **`CommitAsyncRequest`**: empty `offsets` ⇒ commit current positions.

### 2.2 The Python anchor — symbol-by-symbol mapping

Each .NET deliverable must mirror the named Python symbol.

| .NET deliverable | Python anchor | Cite |
|---|---|---|
| `IConsumerRebalanceListener` | duck-typed listener: `on_partitions_revoked` + `on_partitions_assigned` required, `on_partitions_lost` optional (delegates to revoked) | `bindings/python/consumer.py:962-985`, adapter `:263-317` |
| listener registration lifetime | `_subscribe_spec(topics, listener=None)` — `None` ⇒ plain subscribe **and drops** the adapter | `consumer.py:773-788` |
| `Subscribe(topics, listener)` | `Consumer.subscribe(topics, listener=None)` / `AsyncConsumer.subscribe(...)` | `consumer.py:962`, `:1123` |
| `IOffsetCommitCallback` + `CommitAsync` overloads | `_ConsumerBase.commit_async(offsets=None, callback=None)` — covers all three Java overloads; `callback(offsets, exception)` | `consumer.py:668-705`, adapter `:320-372` |
| commit-callback error policy | adapter **swallows and logs** any exception (Java `onComplete` returns void) | `consumer.py:363-372` |
| managed `ConsumerHandle` | `class ConsumerHandle` + `_ConsumerBase.handle()`; context manager; idempotent `destroy()`; **no** callback-taking commit | `consumer.py:378-545`, `:584-593` |
| `MockConsumer.Rebalance` | `_MockConsumerMixin.rebalance(partitions)` | `consumer.py:868-886` |
| server `CallbackLog` | `class CallbackLog` — `append(client_id, kind, partitions, offsets, error)` + `response(client_id)`; one `threading.Lock`; **no** `clear()`/`snapshot()` | `bindings/python/grpc_translate.py:287-332` |
| server `_offset_key` | `f"{topic}-{partition}"` | `grpc_translate.py:282-284` |
| server `LoggingRebalanceListener` | 3 plain (non-coroutine) methods; `on_partitions_lost` implemented **explicitly**, not left to the Java default, so lost ≠ revoked in the log | `grpc_translate.py:344-372` |
| server logging commit callback | `make_logging_commit_callback(log, client_id)` — `partitions=list(offsets.keys())`, `offsets={_offset_key(...): oam.offset}`, `error="" if exception is None else str(exception)` | `grpc_translate.py:375-388` |
| server `Subscribe` handler | builds `LoggingRebalanceListener` iff `with_listener`, else passes `listener=None` **explicitly** | `grpc_server.py:288-297`; async `grpc_server_async.py:281-292` |
| server `CommitAsync` handler | `c.commit_async(_proto_offsets_to_dict(request.offsets) or None, callback=callback)`; **not awaited** in the async server (it is a sync local op) | `grpc_server.py:331-341`; async `grpc_server_async.py:324-341` |
| server `GetCallbackLog` handler | returns `self._callback_log.response(request.consumer_id)`; **never errors on an unknown id** (empty response) | `grpc_server.py:533-536`; async `:520-523` |

### 2.3 The .NET-internal precedent (what the callbacks must look like mechanically)

Since there is no producer (§1.2 correction 3), the pattern to copy is the
consumer's own one-shot completion bridge — it is mature, uniform, and already
Critic-hardened:

- **Delegate types + process-lifetime rooting:** `Internal/Interop/ConsumerCallbacks.cs`
  — 8 `[UnmanagedFunctionPointer(CallingConvention.Cdecl)]` delegate types with
  `static readonly` rooted instances (`:60`, `:130`, `:222`, `:280`, `:334`,
  `:406`, `:466`), plus the per-closed-generic
  `TypedPollCallbacks<TKey,TValue>.Poll` (`TypedPollCallbacks.cs:66`).
  `[UnmanagedCallersOnly]`, `delegate* unmanaged`, and
  `Marshal.GetFunctionPointerForDelegate` are **used nowhere and are forbidden**
  by the netstandard2.0 floor (`ffi-marshalling.md §0.1`).
- **Per-op context via `GCHandle` in `user_data`:** the canonical submit helper
  `NativeConsumer.SubmitVoidOperation` (`:2776-2824`) — alloc `GCHandle`, set on
  the context, `DangerousAddRef` **inside** the `try`, submit, `AbandonBeforeSubmit`
  on throw.
- **Span-the-op `SafeHandle` ref:** `OperationCompletionSource.cs:94-100`, freed
  exactly once in `FreeGcHandle` (`:254-266`) under one `Interlocked.Exchange`.
- **No-throw boundary:** every trampoline is
  `try { … } catch { context?.TrySetException(e); } finally { <Container>Destroy(h); context?.FreeGcHandle(); }`
  — `ConsumerCallbacks.cs:62-82`, `:155-189`, `:232-265`, …
- **`RunContinuationsAsynchronously` is mandatory** —
  `OperationCompletionSource.cs:88-89`, doc `:58-66`.
- **Existing test template:** `tests/Confluent.Kafka.UnitTests/Interop/ConsumerCompletionBridgeTests.cs`
  — notably `InFlightOperation_SurvivesAggressiveGc` (`:169`),
  `FreeGcHandle_CalledTwice_FreesAndReleasesExactlyOnce` (`:187`),
  `Callback_WithUnexpectedContext_DoesNotUnwindIntoNative` (`:273`).

**The one genuinely new dimension:** every existing trampoline is a *one-shot
per-op completion* whose `GCHandle` the callback itself frees. A rebalance
listener is **long-lived and multi-shot**, bound to a *subscription*, not an
operation. `FreeGcHandle`'s "sole owner is the completion callback" invariant
(`OperationCompletionSource.cs:67-80`) **does not transfer** and a new
lifetime rule must be written (§5.2).

### 2.4 Where the C server is the better anchor than Python

Two places, both because .NET (like C) hands a raw pointer to native and Python
does not:

1. **`user_data` lifetime / the use-after-free.** `9465e197` fixed a real UAF in
   `server.cc`: `Consumer_destroy` does **not** join the dispatcher, so a queued
   callback job can dereference `user_data` after `Close`. C's fix: session-lifetime
   `LogState`, `unordered_map<uint64_t, unique_ptr<LogState>>`, **never erased on
   `Close`**, `user_data_destroy == nullptr` everywhere
   (`server.cc:253-288`, `:842-853`, `:1281-1299`). Python is immune (in-process
   append + refcounting) and therefore gives no guidance. See Q9.
2. **The non-nullable commit callback.** `commit_async_offsets_with_callback`'s
   `callback` param is **not** nullable and there is **no** plain
   `Consumer_commit_async_offsets`. C therefore needs a four-way branch and a
   `discard_commit_complete` no-op that still frees the handles it is given
   (`server.cc:333-380`, `:947-970`). Python hides this in its C extension. .NET
   must reproduce C's branch shape — see §5.3.

Also from C, worth copying: the log's mutex is **separate** from the id-map's
mutex (`server.cc:216-219`) so a callback firing mid-poll never queues behind a
`CreateConsumer`/`Close`.

---

## 3 · Declared divergences from the anchor

Declared here, in advance, so a Critic does not file them. Anything **not** on
this list is a defect.

| # | Divergence | Why | Governing rule |
|---|---|---|---|
| D1 | **`IConsumerRebalanceListener` is sync**, not `Task`-returning (subject to Q6) | The ABI callback is a sync C fn pointer returning `KafkaError*` and the rebalance blocks on it. Python's own logging listener uses plain methods for exactly this reason | `bindings/dotnet/CLAUDE.md §4` sync-vs-async; `ffi-marshalling.md §B6`; `src/ffi/consumer.rs:2967-2979` |
| D2 | **`Subscribe` gains an overload**, not an optional parameter | .NET idiom is overloads; Python uses `listener=None`. Java has two `subscribe` overloads, so the overload is the *more* faithful form | `bindings/CLAUDE.md §2.2` |
| D3 | **Listener callbacks run on the core's foreign dispatcher thread**, not "the caller's task" | `consumer-threading.md §31`'s caller's-task model is the **Rust core's** contract; the C ABI flattens it (`common.rs:276-280`). Python documents the identical divergence (`consumer.py:977-985`) | `bindings/CLAUDE.md §1.2`; `ffi-marshalling.md §B1/§B6` |
| D4 | **`IOffsetCommitCallback` exists** despite `bindings/dotnet/CLAUDE.md §4`'s "do not add a callback-taking overload" row | Q5. Needs a maintainer-sanctioned amendment to that row | Q5 |
| D5 | **`ConsumerHandle` takes a `DangerousAddRef`** on the consumer `SafeHandle` (subject to Q7) — Python does not ref-count | .NET cannot enforce destroy-ordering; M9/P4 closed exactly this UAF class | `ffi-marshalling.md §B2` (ref-counted release); M9/P4 H1 |
| D6 | **`CallbackLog` lives in its own `CallbackLog.cs`**, not in `Translate.cs` (subject to Q8) | `Translate.cs` is a pure-function static class; the log is stateful | `bindings/dotnet/CLAUDE.md §2` |
| D7 | **The server's per-consumer `GCHandle` + log state is session-lifetime**, never freed at `Close` (subject to Q9) | Mirrors C's `9465e197` UAF fix; `Consumer_destroy` does not join the dispatcher | §2.4 item 1 |
| D8 | **Copy-out of the delivered `TopicPartitionList` / `OffsetMap` happens inside the trampoline**, before `_destroy`, exactly as the existing one-shot trampolines do | `bindings/dotnet/CLAUDE.md §6.4` copy-out default; borrowed views are never freed | `ffi-marshalling.md §B2` (Cat. 3/4), `§B4` |
| D9 | **No `ConsumerHandle` commit-with-callback**, matching Python and the ABI (the handle family has no callback-taking commit) | `src/ffi/consumer_handle.rs:72-86` excludes them by design | anchor §2.2 |

---

## 4 · Phase breakdown

Milestone **M9** is the consumer milestone on this branch (P1 record accessors,
P2 `Metrics()`/`ClientId()`, P3 the `Metrics` gRPC RPC, P4 memory-safety
hardening — all DONE). The M9/P2→P3 pair is the governing precedent for
"binding API phase, then the gRPC-server phase that consumes it", so the server
work stays in M9 rather than moving to M8.

| Phase | N | Layer | Scope | Depends on | Mode |
|---|---|---|---|---|---|
| **M9/P5** | 58 | 4 | Harness compile fix — `create_with_callback_log` on both dotnet factories | — | Rust (`tests/`), see Q2 |
| **M9/P6** | 59 | 2a | `IConsumerRebalanceListener` + `Subscribe(topics, listener)` ×4 impls + `MockConsumer.Rebalance` ×2 + 6 P/Invokes | Q1 (native) | A |
| **M9/P7** | 60 | 2b | `IOffsetCommitCallback` + `CommitAsync` overloads + 2 P/Invokes | Q1, Q5 | A |
| **M9/P9** | 62 | 3 | gRPC server: `Subscribe.with_listener`, `CommitAsync`, `GetCallbackLog`, `CallbackLog` — both servicers | P6, P7 | A |
| **M9/P8** | 61 | 2c | managed `ConsumerHandle` + 23 P/Invokes | Q1, Q7 | A |

**Recommended order: P5 → P6 → P7 → P9 → P8.** P9 before P8 gets the four
harness tests green as early as possible; P8 is the largest surface and the only
one no other phase depends on. (The numbering keeps P8 = `ConsumerHandle` for
readability even though it executes fifth; if that is confusing, swap the labels
at approval time.)

---

## 5 · Per-phase detail

### 5.1 M9/P5 (N=58) — harness compile fix

**Deliverable.** Two method bodies in `tests/common/backend_factory.rs`,
mirroring `CGrpcFactory:389-394` exactly:

```rust
// into `impl ConsumerBackendFactory for DotnetGrpcFactory` (after :430)
async fn create_with_callback_log(
    &self,
    config: HashMap<String, String>,
) -> Result<(Box<dyn Consumer<Vec<u8>, Vec<u8>>>, ConsumerCallbackLog), KafkaError> {
    consumer_with_log(&self.channel, config, "dotnet").await
}

// into `impl ConsumerBackendFactory for DotnetAsyncGrpcFactory` (after :470)
async fn create_with_callback_log(
    &self,
    config: HashMap<String, String>,
) -> Result<(Box<dyn Consumer<Vec<u8>, Vec<u8>>>, ConsumerCallbackLog), KafkaError> {
    consumer_with_log(&self.channel, config, "dotnet_async").await
}
```

`consumer_with_log` (`:204-213`) is private to the same `grpc_backends` module,
already generic over the backend label, and `MultilanguageConsumer::consumer_id()`
is backend-independent. Nothing else is needed.

**Also in scope:** refresh `DotnetGrpcFactory`'s doc comment (`:406-415`). It
currently explains why .NET dodges the *producer* callback test and is now
misleadingly reassuring — it says nothing about the consumer ones, which .NET
does **not** dodge.

**Reference:** `PythonGrpcFactory:253-277`, `CGrpcFactory:379-401`.

**Decisions:** Q2 (who authors it, does it ship alone), Q4 (interim exclusion).

**DoD:** the four gates in §6.4. **No separate clippy invocation is needed** —
see the correction there.

**Effort:** ~30 min of work; hours of build time. **Risk: very low.**

**OUTCOME — DONE 2026-08-29 (N=58).** Two commits:
`e72fc809` (the two `create_with_callback_log` impls, +14/−0) and `1a6c13f8`
(`fixup!` closing the Critic's single Minor finding — the §5.1 doc refresh had
been silently dropped; 14 lines, doc comments only; autosquash pairing verified).
Critic reproduced all four gates green independently; one finding total, no
correctness or memory-safety defects. `COMMENTS.DONE.58.md` archived at
`design/history/M9/P5/`.

### 5.2 M9/P6 (N=59) — rebalance listener

**Deliverables**

1. **`IConsumerRebalanceListener`** (public, `src/Confluent.Kafka/`) — three
   methods over `IReadOnlyCollection<TopicPartition>`; `OnPartitionsLost`
   delegates to `OnPartitionsRevoked` per Java. Shape per Q6/D1.
2. **`Subscribe` overloads** — `void Subscribe(IReadOnlyCollection<string>, IConsumerRebalanceListener)`
   on `IConsumer` (after `:99`) and
   `Task Subscribe(IReadOnlyCollection<string>, IConsumerRebalanceListener, CancellationToken = default)`
   on `IAsyncConsumer` (after `:107`), plus the four forwarders.
3. **`MockConsumer<K,V>.Rebalance(IReadOnlyCollection<TopicPartition>)`** and the
   `AsyncMockConsumer` twin — inherent mock helpers, **not** on the interfaces
   (matching the existing five).
4. **`NativeMethods` P/Invokes (6):** `ConsumerRebalanceListener_new`,
   `ConsumerRebalanceListener_destroy`, `Consumer_subscribe_with_listener`,
   `Consumer_subscribe_with_listener_async`, `MockConsumer_rebalance`,
   `KafkaError_new`. Each needs `EntryPoint` set to the full ABI symbol.
5. **Three rooted Cdecl trampolines** in `ConsumerCallbacks.cs`, shape
   `IntPtr (IntPtr partitions, IntPtr userData)` returning a `KafkaError*`.
6. **`NativeConsumer` registration state** — the multi-shot `GCHandle` and its
   lifetime rule (below).

**The listener lifetime rule** (the one new invariant; ABI contract at
`src/ffi/consumer.rs:3012-3048` and `:3281-3307`):

- `subscribe_with_listener` **consumes the listener handle unconditionally,
  including on failure** — so never `_destroy` a listener you passed in
  (that is a double free, `:3238-3243`).
- The registration is released by: a **replacing** `subscribe*` (including a
  listener-less one), or consumer destroy. **Not** by `unsubscribe()` and
  **not** by `close()` — verified in phase-5 notes item 1 and phase-6 notes
  item 5; the Rust `SubscriptionState::unsubscribe` deliberately leaves
  `rebalance_listener` in place, Java-faithfully.
- `user_data_destroy` **may run on any thread** (`src/ffi/consumer.rs:3010`), so
  the free hook must be thread-agnostic. The safest .NET shape mirrors C:
  keep the `GCHandle` **binding-lifetime**, pass `user_data_destroy = null`,
  and free at `NativeConsumer` teardown — the alternative (freeing from the
  destroy hook) races a queued dispatcher job, which is precisely the UAF
  `9465e197` fixed in C.
- A managed listener that **throws** must be caught in the trampoline and turned
  into a `kafka_common_KafkaError_new(-1, message)` **returned** to Rust — never
  allowed to unwind (`ffi-marshalling.md §B6`). Python pins the observable
  contract: code **-1**, `str(exc)` verbatim as the message (phase-6 notes
  item 6). The returned handle's ownership transfers; do **not** destroy it.

**`MockConsumer_rebalance` semantics to pin in tests** (`src/ffi/consumer.rs:1372-1397`,
phase-5 notes item 4, phase-6 notes item 6):
requires an `AutoTopics` **subscription** (a manually-assigned consumer fails
with "manual assignment in use"); fires `on_partitions_revoked` **only when
something was removed**; fires `on_partitions_assigned` **unconditionally when a
listener is registered**, with the *added* list (possibly empty), **not** the full
assignment; **never** fires `on_partitions_lost`; and **does not return until the
callbacks have returned**, propagating a callback error as its return value.

**Tests required** (`ffi-marshalling.md §B6` "Tests required" + `§7.4`):
- each of the three callbacks delivers the right partitions;
- an exception thrown in a listener is caught, does not unwind into native, and
  surfaces as the documented `KafkaError` (assert **code -1 and the exact
  message** — `definition-of-done.md §3`);
- aggressive GC during a live registration does not collect the delegate
  (mirror `ConsumerCompletionBridgeTests.InFlightOperation_SurvivesAggressiveGc:169`);
- a replacing listener-less `Subscribe` **releases** the registration; an
  `Unsubscribe` **does not**;
- non-ASCII topic names round-trip through the partitions list
  (`ffi-marshalling.md §B3`);
- `consumer-threading.md §31` "Tests required" #2 — the rebalance does not
  advance until the listener returns. `MockConsumer_rebalance` being synchronous
  makes this directly testable: block the listener on a `ManualResetEventSlim`
  from a worker thread, assert `Rebalance` has not returned, release, assert it
  has. **The phase-5 notes' mutation check applies**: a "has NOT completed yet"
  assertion passes vacuously if the flag is never set for an unrelated reason —
  prove it fails when the block is removed.

**§31 test #1 is deferred to P8** with the rationale in §6.3.

**Effort: large** (the biggest .NET surface after P8). **Risk: medium-high** —
the multi-shot `GCHandle` lifetime is genuinely new for this binding and is the
single most likely place to introduce a leak or a UAF.

### 5.3 M9/P7 (N=60) — offset-commit callback

**Deliverables**

1. **`IOffsetCommitCallback`** — `void OnComplete(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets, KafkaException? exception)`,
   mirroring Java `OffsetCommitCallback.onComplete(Map, Exception)` (subject to Q5).
2. **`CommitAsync` overloads on `IConsumerCommon`** (alongside the existing
   `void CommitAsync()` at `:160`):
   `void CommitAsync(IOffsetCommitCallback callback)` and
   `void CommitAsync(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets, IOffsetCommitCallback? callback = null)`
   — the three Java overloads, exactly as Python's single
   `commit_async(offsets=None, callback=None)` covers them. Plus four forwarders each.
3. **2 P/Invokes:** `Consumer_commit_async_with_callback`,
   `Consumer_commit_async_offsets_with_callback`.
4. **One rooted Cdecl trampoline** `void (IntPtr offsets, IntPtr error, IntPtr userData)`.

**ABI facts that force the shape** (verified; do not re-derive):

- The callback typedef is `(OffsetMap*, KafkaError*, void*)`
  (`src/ffi/consumer.rs:3829-3830`). `offsets` is **always non-null**; `error`
  null ⇒ success; **the callee owns both** and must
  `kafka_consumer_OffsetMap_destroy` / `kafka_common_KafkaError_destroy` them.
- **`…_offsets_with_callback`'s `callback` param is NOT nullable, and there is no
  plain `Consumer_commit_async_offsets`** (phase-6 notes item 4). So
  `CommitAsync(offsets)` **without** a callback must pass a no-op trampoline that
  still frees both handles — C's `discard_commit_complete` (`server.cc:370-380`).
  Passing `null` would be UB.
- Both functions take a separate `user_data_destroy` hook
  (`Option<extern "C" fn(*mut c_void)>`), which **may run on any thread**
  (`:3838-3840`).
- A marshal failure returns the error **without registering the callback** — the
  callback never fires, but `user_data_destroy` still does (`:3978-3981`). The
  `GCHandle` free must be correct on that path.
- On a **MockConsumer** the callback fires **inline during the commit call**
  with `error` always null (`:3939-3942`), so it has already run by the time the
  function returns. That makes the mock tests fully deterministic — no polling.

**Error policy:** Java's `onComplete` returns `void` and has nowhere to report
its own failure, so a throwing callback is **caught and logged, not
propagated** — Python does exactly this (`consumer.py:363-372`). Unlike the
rebalance listener there is **no** `KafkaError` return channel here.

**Tests required:**
- the callback receives the committed offsets and a null exception; assert the
  `OffsetAndMetadata` values, not just non-empty;
- a failing commit delivers a `KafkaException` with the right code/message;
- `CommitAsync(offsets)` with **no** callback still frees both handles exactly
  once (the `discard` path) — the `FreeGcHandle_CalledTwice…:187` pattern;
- a throwing callback is swallowed, does not unwind into native, and the
  consumer stays usable;
- the marshal-failure path frees the `GCHandle` exactly once.

**Effort: medium.** **Risk: medium** — the non-nullable-callback asymmetry is the
trap; it is invisible from the header alone if the writer assumes symmetry with
the no-offsets form.

### 5.4 M9/P8 (N=61) — managed `ConsumerHandle`

**Deliverables:** a public `ConsumerHandle : IDisposable`, a
`ConsumerHandle Handle()` accessor on `IConsumerCommon` (Python's
`_ConsumerBase.handle()`), and **23 P/Invokes** — `Consumer_handle` plus the 22
`ConsumerHandle_*`:

| Group | Functions |
|---|---|
| lifecycle | `destroy` `:247` |
| non-blocking | `wakeup` `:264`, `assignment` `:288`, `subscription` `:304`, `paused` `:320` |
| blocking void | `assign` `:342`, `seek` `:359`, `seek_with_metadata` `:378`, `seek_to_beginning` `:407`, `seek_to_end` `:424`, `pause` `:441`, `resume` `:458` |
| blocking scalar | `position` `:481`, `position_timeout` `:498` |
| blocking map | `committed` `:522`, `beginning_offsets` `:547`, `end_offsets` `:572`, `offsets_for_times` `:598` |
| blocking commit | `commit_sync` `:630`, `commit_sync_offsets` `:645`, `commit_async` `:672`, `commit_async_offsets` `:685` |

**Contract facts:**

- The handle is **caller-owned and arbitrarily long-lived** — *not*
  callback-scoped (`src/ffi/consumer_handle.rs:59-70`). Handles are independent;
  destroying one does not affect another or the consumer. **Destroy every handle
  before destroying the consumer** — hence Q7/D5.
- **It bypasses the access guard by design** (`:27-37`), which is the entire
  reason it exists: a listener or commit callback calling the plain
  `Consumer_*` API is rejected with `ConcurrentModification`.
- Every op is a **synchronous** C function that `block_on`s on the *calling*
  thread; each entry point returns `IllegalStateError` rather than panicking if
  called from inside a tokio runtime (`:143-152`). Safe from the dispatcher
  thread and from any embedder-owned OS thread.
- `poll` / `subscribe` / `unsubscribe` / `close` are **intentionally absent**
  (`:72-86`), as are callback-taking commits (D9).
- **On a MockConsumer-derived handle every async op returns
  `UnsupportedVersionError`** (`:82-86`) — only `wakeup` + the three sync getters
  work. This is core behavior, not an FFI limit. See §6.3.

Every method should take the `SafeHandle` as the P/Invoke parameter (all are
synchronous), per the M9/P4 convention.

**Tests required:** destroy is idempotent; use-after-`Dispose` throws
`ObjectDisposedException`; the three sync getters and `wakeup` work against a
mock; **and** `consumer-threading.md §31` "Tests required" #1 — a listener
calling `handle.CommitSync()` from inside `OnPartitionsRevoked` succeeds — which
**cannot** run against a mock (see §6.3).

**Effort: large** (23 declarations + a public type + marshalling for four result
shapes, though every marshaller already exists). **Risk: medium** — mechanically
repetitive, but the lifetime interaction with `SafeConsumerHandle` (Q7) is
subtle.

### 5.5 M9/P9 (N=62) — the gRPC conformance server

**Deliverables** (both `ConsumerServiceImpl.cs` and `AsyncConsumerServiceImpl.cs`):

1. **`CallbackLog`** (new `grpc-server/CallbackLog.cs`, per Q8/D6) —
   `Append(ulong clientId, string kind, IEnumerable<TopicPartition> partitions, IReadOnlyDictionary<string,long>? offsets, string error)`
   and `Response(ulong clientId)`. `Dictionary<ulong, List<Proto.CallbackLogEntry>>`
   under its **own** lock, separate from the servicer's id-map lock (§2.4).
   No `Clear()`, no `Snapshot()` — reading never clears; entries survive `Close`.
2. **`Translate.cs` additions:** the four consumer `KIND_*` constants,
   `OffsetKey(topic, partition) => $"{topic}-{partition}"`,
   `CallbackLogPartitionToProto` (**cannot** reuse `TpToProto` — different
   message type), and a hoisted `ProtoOffsetsToDictionary` (Python shares
   `_proto_offsets_to_dict` between `CommitSync` and `CommitAsync`; .NET
   currently **inlines** that conversion in `CommitSync` at
   `ConsumerServiceImpl.cs:172-182`, so extract it first).
3. **`LoggingRebalanceListener`** implementing the P6 interface — three methods,
   each one `Append`. **`OnPartitionsLost` implemented explicitly**, not left to
   the Java default, so `lost` is distinguishable from `revoked` in the log
   (both Python and C are emphatic about this).
4. **`LoggingCommitCallback`** implementing the P7 interface —
   `partitions = offsets.Keys`, `offsets = {OffsetKey(tp): oam.Offset}`,
   `error = exception is null ? "" : exception.Message`.
5. **`Subscribe` honors `request.WithListener`** — build the listener when true,
   pass **null explicitly** when false (that is what releases a prior
   registration).
6. **`CommitAsync` override** — the four-way branch forced by the ABI (§5.3):
   `{empty|explicit} offsets × {with|without} callback`. In the **async**
   servicer it must **not** be awaited through `RunStatus` (Python's async server
   deliberately bypasses its `_run_status` because `commit_async` is a sync local
   op — `grpc_server_async.py:324-341`).
7. **`GetCallbackLog` override** — returns the response; **never errors on an
   unknown id** (empty response).
8. **Per-consumer log state allocated in `CreateConsumer`**, session-lifetime,
   **not** freed on `Close` (Q9/D7).

**Verification specific to this phase** (from
`.claude/agent-memory/dotnet-critic/feedback_merged_state_rpc_contract_recheck.md`):
after a clean rebuild, `grep -c Unimplemented obj/*/net8.0/ConsumerServiceGrpc.cs`
must show **26** generated virtuals and the set difference against
`public override` must be **empty**. The servicer class doc-comment's RPC count
must be updated in the same commit.

**P9 prerequisites — recorded now so they are not re-discovered as new:**

1. **Cross-rebuild the Linux natives.** `target-linux-amd64/release/libconfluent_kafka.so`
   (19 Aug) and `target-linux/release/libconfluent_kafka.so` (11 Aug) export **0**
   of the new symbols. The gRPC images `COPY` from these trees, so **no P9 test
   can pass until they are rebuilt.** The host `.dylib` + header were refreshed
   under Q1; these were **not**. The M9/P3 Actor hit exactly this and had to
   re-cross-build before the image build.
2. **`tests/common/callback_log.rs:41` says "all four backends" — there are now
   six.** The module doc enumerates `python / python_async / c` and concludes "one
   generic test body asserts the same thing against all four backends". Introduced
   by merge `a7efb0d5`, **not** by P5 — it was correct when written, and P5's
   Critic correctly scoped it out. Fold it into P9's doc pass, where .NET actually
   joins the log-reading backends.

**Gates:** `dotnet build` (both servicers, net8.0), `dotnet format --verify-no-changes`,
and the Docker gate `cargo test --features integration-tests,multilanguage-tests --test integration -- __grpc_dotnet`.
**Docker IS available in this environment** (`docker info` → rc 0) — per
`.claude/agent-memory/project-manager/project_dotnet_harness_ci_only_gates.md`,
verify rather than assume, and **run** the gate. It is nonetheless blocked by
Q1, because the image `COPY`s a host-cross-built Linux `.so` that must carry the
new symbols. The M9/P3 Actor hit exactly this and had to re-cross-build first.

**Effort: medium.** **Risk: low-medium** — a faithful port of two well-documented
reference implementations; the risks are the four-way `CommitAsync` branch and
the session-lifetime log.

---

## 5.6 · STANDING TEMPLATE ITEM — itemized `ffi-marshalling.md` section-sync

**Added after M9/P6. This is a per-phase deliverable, not a nicety.**

M9/P6's Critic filed the **PLAN template** — not the Actor — as the defect, and
called it the **third recurrence**. Across P5 and P6 *every* finding has been the
same species: a **documented-scope item silently dropped** (P5) or **contract docs
left contradicting shipped code** (P6, where `ffi-marshalling.md` had zero
occurrences of `listener` / `multi-shot` / `user_data_destroy` and **three
normative statements that actively forbade the shipped code** — §B6:1160,
§B6:1167, §B7:1288 — with §B2 lacking any category for a consumed-by-callee
handle).

`CLAUDE.md` doc-sync was already itemized (the D10 slot). `ffi-marshalling.md`
was not, so it was left to the Actor to notice. **Every remaining phase must name
the exact `ffi-marshalling.md` §§ it will touch, in its deliverables table**, the
same way D10 names the `CLAUDE.md` rows:

| Phase | `ffi-marshalling.md` §§ that MUST be reviewed and updated |
|---|---|
| **P7** (commit callback) — ✅ **DONE**, all four §§ updated | **§B6** — a third Rule for the release-hook family. **§B7** — a fire-and-forget callback completing **no** `TaskCompletionSource`. **§B2** — the delivered `OffsetMap_t` category; a commit registration is **not** Category 5. **§B5** — the two error channels (returned `KafkaError*` vs the one delivered to the callback) |
| **P8** (`ConsumerHandle`) — **CONFIRMED after P7/P9; §B1/§B2/§B5 stand, plus an explicit §B6 no-op** | **§B2** — a new category for a *caller-owned, independently-destroyed* handle that ref-counts its parent. **§B1** — the handle deliberately bypasses the access guard, and its ops `block_on` the **calling** thread. **§B5** — `IllegalStateError` when called from inside a runtime, `UnsupportedVersionError` on mock-derived handles, and the guard-bypass contrast (§5.11). **§B6 — NO EDIT**, but the phase record must *state* that no P8 entry point takes a `user_data_destroy`, so none is in the hook family (§5.8) — a blank row is what recurred three times |
| **P9** (gRPC server) | Likely **none** — the server is a harness consumer of the binding, not a boundary change. **State that explicitly** in the phase record rather than leaving the row blank |

**The check is mechanical, so do it:** for each § listed, `/usr/bin/grep` the file
for the phase's key nouns and confirm:

  - **(a)** the concept appears;
  - **(b)** **no existing normative sentence forbids the shipped code** — the half
    that was missed twice (P5, P6);
  - **(c)** ⚠ **does the shipped code satisfy the sentence you just wrote?**
    *Added after M9/P8.* Its **Major** finding was a §B5 rule that was **written and
    then not implemented** — the doc-sync deliverable produced a normative sentence
    the code did not honour. That passes (a) *and* (b) and is caught **only** by
    (c). Writing a rule is not evidence of obeying it; the two are separate acts and
    only one of them is a grep away.

**Corollary to (c), in the sharper form the P8 Actor offered — check the *scope* of
any idiom you cite as justification.** P8's Major finding looked sanctioned because
a `CLAUDE.md` row about **pre-FFI precondition validation of Java exceptions** was
read as covering **an error code returned from the core**. Those are different
things: one is the binding rejecting bad input before it ever calls native, the
other is the binding relaying a core decision. A cited precedent is only a
precedent if its scope actually contains your case — quote the row and check its
subject before leaning on it.

⚠ **Use `/usr/bin/grep`, not bare `grep`** — `grep` is aliased to `ugrep` here,
which **silently emits nothing on a rejected pattern**, producing a false *pass*.

### 5.7 · Carry-forward from M9/P6 — the ref-count is what closes the window

**P8 inherits this invariant directly. Do not let it inherit the disproved
argument.**

M9/P6's implementation was correct but its **stated rationale was wrong**. The
Actor justified freeing the registration `GCHandle` from `user_data_destroy` on
the grounds that an owned `Arc` is held across the callback. The Critic verified
all three invocation sites repo-wide (correct — genuinely not a UAF), then showed
the inference over-reaches: `FfiRebalanceListener::invoke`
(`src/ffi/consumer.rs:3122`) **copies the pointer out before dispatching**, so the
closure carries a **raw copy, not the `Arc`**. A count ≥ 1 holds only while the
awaiting *future* lives.

**Same conclusion, different mechanism.** What actually closes the window is:

1. the **ref-counted `SafeConsumerHandle`** — a listener callback only ever runs
   inside an operation that holds a count, so `Consumer_destroy` cannot run
   concurrently with one; **plus**
2. the **single serialised dispatcher** — the deferred-destroy path fires from
   `FreeGcHandle` **on the dispatcher thread**, the same thread that would run a
   queued job.

Now corrected in `ListenerRegistration.cs`, `COMMENTS.DONE.59.md`, and
`ffi-marshalling.md` §B6, each carrying: *"If either of those two properties is
ever weakened, this rule must be re-derived."* **P8's `ConsumerHandle` sits on
exactly property (1)** — it ref-counts the same `SafeConsumerHandle` (roadmap
Q7/D5). Any P8 reasoning about handle lifetime must cite the ref-count, never the
`Arc`.

### 5.8 · Carry-forward from M9/P7 — the free-site rule, reframed and now complete

**⚠ This supersedes P7's brief §5, which framed the discriminator as *one-shot vs
multi-shot*. That framing was wrong-shaped.**

**The discriminator is: does the entry point take a `user_data_destroy` hook?**
Not the callback's arity, not its family. The Critic enumerated every hook in the
regenerated header; **exactly three** entry points take one (independently
re-verified — `/usr/bin/grep -n "void (\*user_data_destroy)(void\*)"` returns
precisely these):

| Entry point | Header |
|---|---|
| `kafka_consumer_ConsumerRebalanceListener_new` | `h:2078` |
| `kafka_consumer_Consumer_commit_async_with_callback` | `h:2505` |
| `kafka_consumer_Consumer_commit_async_offsets_with_callback` | `h:2538` |

**Nothing else does** — not any of the ~8 one-shot completions, not the producer's
`send_async`. So the rule is total, not a heuristic: *a hook present ⇒ the hook is
the sole free site; no hook ⇒ the callback frees.* Now §B6's third Rule.

**The decisive citation is the ordering, `confluent_kafka.h:524-528`:** the hook
fires *"after the registration is dropped by the consumer (i.e. **after the commit
completed and the callback returned**, or immediately if the call failed before the
callback could be registered)."* P7's brief only established the **failure** path
(the callback never fires, the hook does). The ordering sentence is what also rules
out a trampoline free on the **success** path — the stronger and previously
unproven half.

**Two P7 structural outcomes that change later phases:**

- **`OffsetMapMarshal.CopyOutAndDestroy` now exists**
  (`Internal/Interop/OffsetMapMarshal.cs:69`), mirroring
  `TopicPartitionListMarshal`. P7's brief §3.3 warned at length about the
  `CopyOut`-does-not-destroy asymmetry; **that trap is now removed structurally
  rather than documented**, so P8/P9 cannot fall into it. Prefer
  `CopyOutAndDestroy` at any callback site that owns the map.
- **`WithPinnedCommitOffsets` pin-ordering was NOT a live defect** — correcting the
  record. The same shape recurs in `SubmitVoidOperation` /
  `SubmitScalarOperation` / `SubmitOwnedHandleOperation`, and the trigger is
  unreachable in **all** of them: `PinnedUtf8String.Dispose` is `IsAllocated`-guarded
  and nothing sits between `body(...)` and the `finally`. The `submitted` flag stays
  as defensive hygiene. **No test is possible** — removing the flag leaves the suite
  green. Do not let a later Critic re-file this as a bug.

**Three Critic rule-update suggestions from P7 are unactioned and are the
maintainer's**, not an Actor's. The substantive one: **tighten §B6's deciding
question from "does the *entry point* take a hook" to "does the *call* pass one"** —
it applies to text written this phase and is a genuine improvement, though nothing
is wrong today (the binding passes a hook at every site that can take one).

### 5.9 · Carry-forward from M9/P9 — and two harness-wide gaps

**P9 closed with ZERO findings** — the first phase in the milestone to do so.
Commits `33b60f13` · `ee91b6be` · `62b360b4`. The four gate tests are green
(`28 passed; 0 failed`, reproduced twice by the Critic **with per-test names, not
exit codes** — see the libtest caveat below). **Mode A held across the entire
milestone:** `git diff a7efb0d5 HEAD -- src/ cbindgen.toml` is empty
(independently re-verified).

Two refinements worth carrying:

- **The `Close`-survival guarantee is *static*, not observational.** The log map
  has exactly three touches — `TryGetValue`, insert, `TryGetValue` — with **no
  `Remove` and no `Clear`**, and `GetCallbackLog` never resolves a
  `ConsumerEntry` at all. That second half is what makes **P9-D3** (never error on
  an unknown id) load-bearing rather than merely consistent: the read path does
  not depend on the consumer existing.
- **The separate lock prevents a hard deadlock, not mere serialisation.** My §5.2
  framing understated it, and so does the shipped `CallbackLog.cs` comment: during
  `Poll`, the rebalance **blocks until the listener returns** *while the RPC
  handler holds the gate*. A gate-taking `GetCallbackLog` would therefore not just
  queue — it would deadlock against the very callback it is waiting to observe.
  Carried in `COMMENTS.DONE.62.md` for the next edit of that file.

⚠ **Coverage caveat, honestly disclosed rather than papered over:** only **3 of 6**
backends were runnable here (`__rust`, `__grpc_dotnet`, `__grpc_dotnet_async`) —
the Python and C images do not exist on this machine. Not a defect; a stated limit.

⚠ **libtest filter caveat (cost real time — do not repeat):** *a filter matching
zero tests exits 0 and prints `0 passed`.* A green exit code is **not** evidence a
test ran. The arms are `__grpc_dotnet` / `__grpc_dotnet_async` — a filter of
`__dotnet` matches **nothing** and looks like a pass. **Always assert on per-test
names in the output**, never on the exit code.

### 5.10 · FOLLOW-UP ITEMS — cross-backend, deliberately NOT in P8

Three items are recorded here as their own piece of work, **with CI as the
verifier**. Rationale (the Manager's call, recorded): only 3 of 6 backends run
locally, so folding these into P8 would ship assertions that cannot be verified
against Python and C. If a backend turns out not to honour one of these, that is a
real finding **about that backend** — and it must not surface as noise inside the
last .NET phase.

- **O1 — the `lost` kind has no asserting test on any backend.** `KIND_LOST`
  appears only in its own definition and in the native listener. Nothing asserts
  it end-to-end.
- **O2 — "entries survive `Close`" has zero runtime coverage on any backend.**
  Both callback test bodies end with `close()` and never read the log again. The
  clause **P9-D3 was designed around**, and the property **C's `9465e197` UAF fix
  exists to protect**, is asserted by nothing. **One post-`close()` `entries()`
  assert in the shared body would grade all six backends at once** — the cheapest
  high-value item of the three.
- **O3 — the end-to-end half of `consumer-threading.md` §31 test #1** (a listener
  committing through a reentrancy handle against a real broker). See §5.11; P8
  ships the mechanism proof, this ships the end-to-end one.

All three need a shared-proto or shared-test-body change touching Python and C,
which is exactly why they belong together and away from a .NET phase.

### 5.11 · §31 test #1 — RESOLVED, not deferred a third time

Deferred twice (P6 → P8) on two real blockers: the consumer's own `Commit()` is
`ConcurrentModification` **by design**, and every **async** `ConsumerHandle_*` op
returns `UnsupportedVersionError` on a mock-derived handle
(`src/ffi/consumer_handle.rs:82-86`), while .NET unit tests are mock-only.

**A mock-testable form of the load-bearing property does exist, and P8 ships it.**
The header proves the asymmetry directly:

| Call | Contract |
|---|---|
| `kafka_consumer_Consumer_assignment` (`h:2810`) | *"or **null on a concurrent-access rejection** (the guard could not be acquired)"* — guard-protected |
| `kafka_consumer_ConsumerHandle_assignment` (`h:2950`) | *"Returns … a **non-null** …"*, *"Always empty on a `MockConsumer`-derived handle"* — **no guard**, and it works on a mock |

So inside a listener fired synchronously by `MockConsumer.Rebalance` (which holds
the guard on the app thread for the whole call):

- `consumer.Assignment()` **is rejected** — the guard is held;
- `handle.Assignment()` **succeeds** — it bypasses the guard by design.

That is precisely the property §31 test #1 exists to prove — *the handle is usable
reentrantly where the consumer's own API is not* — and it is exactly the
guard-bypass proof the phase-4 notes (item 3) describe as needing "no threads,
sleeps or `wait_for`". **It is the mechanism, not a weaker proxy.**

What it does **not** cover is the *commit-specific, end-to-end* half ("the commit
actually lands"), which needs a real broker → **O3**.

**Sanctioned split, to be written verbatim into `COMMENTS.DONE.61.md`:**

> `consumer-threading.md` §31 test #1 is satisfied in two parts. **P8 ships the
> mechanism proof**: a listener fired by `MockConsumer.Rebalance` calls
> `handle.Assignment()` successfully while the same listener's
> `consumer.Assignment()` is rejected as concurrent access — establishing that the
> reentrancy handle bypasses the access guard that makes the consumer's own API
> unusable from a callback (`confluent_kafka.h:2810` vs `:2950`). **The
> end-to-end half** — a listener whose `commitSync` through the handle actually
> commits — requires a real broker and a cross-backend harness change, and is
> tracked as follow-up **O3** (roadmap §5.10) alongside O1/O2. This is a scoped
> split with both halves owned, **not** a third deferral.

---

## 6 · Definition of Done and test obligations

### 6.1 Which DoD applies

Root `.claude/rules/definition-of-done.md` items **1, 2, 3, 5, 8, 9** apply in
full to every phase. Notes:

- **#3** — assert **error-message content**, not just `is_err()`/`Assert.Throws`.
  Applies to the listener's code -1 + verbatim message (§5.2) and the mock
  `Rebalance` "manual assignment in use" error.
- **#7** (no structs/traits absent from Java) — `IConsumerRebalanceListener`,
  `IOffsetCommitCallback` and `ConsumerHandle` need justification.
  The first two **are** Java types. `ConsumerHandle` is **not** — it is
  binding scaffolding required because the C ABI's access guard rejects
  reentrant calls; Python has the identical type for the identical reason.
  State this in the phase plan.
- **#10** (hot-path allocation audit) — **N/A** for P5/P9; **applies** to P6/P7
  only in the negative sense (rebalance and commit callbacks are per-rebalance
  and per-commit, not per-record). State it explicitly rather than skipping
  silently.
- **#11** (consumer trait surface) — applies: the new members must not introduce
  a `block_on`-wrapped sync façade, and `IDeserializer` must stay untouched.
- **#12** (test-fixture fidelity, **added by this very merge**) — a fixture
  standing in for a listener must invoke the same primitives production does.

`bindings/dotnet/CLAUDE.md §7.5`: builds on the TFM matrix
(net462-via-netstandard2.0 / net8.0 / net10.0), unit tests pass against
`MockConsumer`, lint/format clean, `ffi-marshalling.md` anti-patterns satisfied.
**net8.0 execution is a standing CI-only gate here** (the .NET 8 runtime is not
installed; net10.0 is the execution gate, net8.0 build-only).

### 6.2 `ffi-marshalling.md` §B6/§B7 "Tests required" mapped to phases

| Obligation | Phase |
|---|---|
| each callback delivers the right result/error and frees the owned handle + `GCHandle` exactly once | P6, P7 |
| an exception in the callback is caught, does not unwind into native, and is surfaced | P6 (as a returned `KafkaError`), P7 (swallowed+logged) |
| aggressive GC during an in-flight/registered callback does not collect the delegate | P6, P7 |
| a concurrent async op faults the `Task`; a concurrent sync state read throws `InvalidOperationException` | P8 (the handle is the documented *escape* from this) |
| `Dispose`/`DisposeAsync` return without hanging with work outstanding | P6 (live registration), P8 (live handle) |

### 6.3 `consumer-threading.md` §31 — the two mandatory regression tests

§31 states the rule is "not considered tested" without both.

- **#2 — "the rebalance does not advance until the listener resolves": SATISFIED
  in P6.** `MockConsumer_rebalance` is synchronous and propagates the callback's
  error, so a blocking listener + a mutation check gives a real proof (§5.2).
- **#1 — "`commit_sync()` from inside `on_partitions_revoked` succeeds":
  CANNOT be satisfied by a .NET unit test.** Two independent reasons:
  (a) the consumer's own `Commit()` is rejected with `ConcurrentModification` by
  the core's access guard — that is *by design*, and the sanctioned route is
  `ConsumerHandle` (P8); and (b) **every async `ConsumerHandle_*` op returns
  `UnsupportedVersionError` on a MockConsumer-derived handle**
  (`src/ffi/consumer_handle.rs:82-86`), and .NET unit tests are Mock-only, no
  broker (`bindings/dotnet/CLAUDE.md §7.3`).

  **Options for #1:** a broker-backed integration test (Testcontainers — the
  M13/P1 precedent exists in the perf suite, but that suite lives on the
  *producer/perf* branch, not here); or a harness scenario; or defer with a
  written rationale. **This must be decided at P8 approval and cannot be silently
  skipped** — it is a named obligation in a rule file, and a Critic will file it.

### 6.4 The Rust gate map — ⚠ CORRECTED, read before citing any gate

An earlier draft of §5.1 claimed `cargo xtask lint` "runs clippy **without**
`--features`, so the integration binaries are unlinted" and demanded a separate
`cargo clippy --all-targets --features integration-tests,multilanguage-tests`
gate. **That is false and has been removed.** It has been false since `4fd1435f`
(#139, 2026-08-05) — which *predates this plan*; the claim was inherited from the
phase-7 actor notes, which were written before that change landed.

`xtask/src/main.rs:154-167` runs clippy in **two** passes:

```
clippy --workspace --all-targets                  -- -D warnings
clippy --workspace --all-targets --all-features   -- -D warnings
```

The second pass covers `src/ffi/*` **and** both integration test binaries. So
`cargo xtask lint` is sufficient on its own — and it is precisely why lint was
one of the two gates that went **red** on the broken tree. **No phase should add
a redundant clippy invocation.**

**The gate map for a change confined to `tests/`** (Critic-verified structurally,
not inferred). `cargo build --all-features` schedules **zero `test`-kind
targets**, so it **cannot** see a break under `tests/`:

| Gate | On a tree broken only under `tests/` |
|---|---|
| `cargo build --all-features [--release]` (`Makefile` `build-rust-all-features`) | **GREEN — blind to it** |
| `cargo xtask format-check` | GREEN |
| `cargo xtask lint` | **RED** (second pass, `--all-targets --all-features`) |
| `cargo test --all-features -- --skip __grpc` (`Makefile:157`, `test-rust-all-features`) | **RED** |

**Never cite a bare `cargo build --all-features` as evidence of harness health.**
The two gates that actually prove it are `cargo xtask lint` and
`test-rust-all-features`.

**The four gates every Rust-touching phase runs:** `cargo build --all-features
--release` · `cargo test --all-features -- --skip __grpc` · `cargo xtask
format-check` · `cargo xtask lint`.

*(Whether this table should be promoted into a rules file is a maintainer call,
not an Actor's — flagged, not actioned.)*

---

## 7 · Effort and risk

| Phase | Effort | Risk | Principal risk |
|---|---|---|---|
| P5 | XS (~30 min + build) | **Very low** | Only that it cannot be verified locally (Q1) |
| P6 | **L** | **Medium-high** | Multi-shot `GCHandle` lifetime — no precedent in this binding; a wrong free site is a UAF against a queued dispatcher job, exactly the bug `9465e197` fixed in C |
| P7 | M | Medium | The non-nullable `callback` on `…_offsets_with_callback` and the absent plain `…_commit_async_offsets`; symmetry assumptions produce UB |
| P8 | **L** | Medium | 23 declarations (mechanical) + the `SafeHandle` ref-count interaction (Q7); mock-only tests cover a small slice of it |
| P9 | M | Low-medium | The four-way `CommitAsync` branch; the async servicer's must-not-await asymmetry; session-lifetime log |

**Cross-cutting risks**

1. **Q1 dominates.** Without a rebuilt native, P6–P8 cannot run a single test.
   Everything else is schedulable; this is not.
2. **Merged-state drift.** The `dotnet-critic` memory records this exact failure
   mode from M12/P1: a later master merge added an RPC and a scenario, the
   servicer's `Unimplemented` stub started being reached, and *nothing in the
   phase record was wrong when written*. **Every phase must re-derive the RPC
   contract from the proto as merged**, not from this plan, if master is merged
   again mid-flight.
3. **Two rulebook conflicts (Q5, Q6)** are live. An Actor that picks a side
   unilaterally will be reworked.
4. **Scope creep into the producer.** PR #143 also added
   `kafka_producer_Producer_send_with_callback` and
   `ProducerService.GetCallbackLog`. Both are **out of scope**: this branch has
   no .NET producer and its gRPC server compiles `producer_service.proto`
   messages-only. Say so in each phase plan so it is not "discovered" later.

---

## 8 · Rust-core dependencies vs. .NET work

Per `bindings/dotnet/CLAUDE.md §8.1`, the `dotnet-actor` builds C# from the
generated header down and does **not** author Rust; a feature needing a new ABI
function is a Rust-core dependency on the root `actor-executor`.

| Item | Owner |
|---|---|
| **New ABI functions** | **None needed.** Layer 1 is complete (§1.1). This whole plan is **Mode A** |
| **Regenerating `target/include/confluent_kafka.h`** | A *build* step, not authorship: `cargo build --features ffi`, emitted by `build.rs:104-127` under `#[cfg(feature = "ffi")]` using `cbindgen.toml`. `cbindgen.toml`'s export allowlist **already names every new type**, so a rebuild emits them with no edit. **Blocked by Q1** |
| **Building the natives** (`.dylib` for local tests, a cross-built Linux `.so` for the images) | Same — blocked by Q1 |
| **`tests/common/backend_factory.rs` (P5)** | Rust, but harness glue. Q2 — recommend the dotnet-actor under the standing M12/P1 exception |
| **Everything in P6–P9** | `dotnet-actor` / `dotnet-critic` |

**Required reading for the Actors** (PR #143 committed no design doc, §1.5):
`.claude/agent-memory/actor-executor/ffi_callback_bridging_phase{3,4,5,6,7}_notes.md`.
They carry contract facts that are **not** in the header — the non-nullable
commit callback, the listener-release timing, the mock `rebalance` semantics,
and the mock-handle `UnsupportedVersion` constraint.

---

## 9 · Numbering and mechanics

- **Binding N sequence** (independent of the root repo's): highest used = **57**
  (M11/P9). Verified by `ls bindings/dotnet/COMMENTS*`. Root-repo N=42–48 and
  50–53 are deliberately skipped on the binding side.
- **Assigned: N=58 (P5) · 59 (P6) · 60 (P7) · 61 (P8) · 62 (P9).** Next free
  after this plan: **63**.
- `COMMENTS.<N>.md` / `COMMENTS.DONE.<N>.md` are **local working files at the
  binding root, never committed**. The tracked record is the Manager's archived
  copy at `design/history/M9/<Phase>/COMMENTS.DONE.<N>.md`
  (`bindings/dotnet/CLAUDE.md §8.4`).
- On approval: split this document into per-phase `PLAN.md` files under
  `design/history/M9/<Phase>/`, and add a `STATUS.md` entry per closed phase.
- **Persona discovery**: `bindings/dotnet/.claude/agents/dotnet-{actor,critic}.md`
  must be copied to the repo-root `.claude/agents/` to be invocable, and those
  root copies must **never** be committed.
- **Toolchain locations in this environment**: `dotnet` is **not** on `PATH` —
  `/usr/local/share/dotnet/dotnet` (SDK 10.0.302) or `~/.dotnet/dotnet`.
  This branch's `bindings/dotnet/Makefile` has only `grpc-image` /
  `grpc-image-async`; there is **no** `test-dotnet` / `verify-dotnet` target
  here (those live on the producer/perf branches), so phase plans must spell out
  explicit `dotnet build` / `dotnet test -f net10.0` /
  `dotnet format --verify-no-changes` commands.
