# .NET binding — living status

Binding-local status for `bindings/dotnet/`. The .NET binding keeps its own
milestone/phase numbering, independent of the repo-root Rust `design/`.

## Current milestone/phase

Newest first.

- **Milestone 4 / Phase 4b — "Async-surface rename (`IAsyncConsumer`, drop the `Async`
  suffix, `WithCallback`)": DONE (2026-08-05).** A pure C# rename of M4/P4a's async
  surface into its final shape — **no ABI change (Mode A), no Rust authored, no new
  `DllImport`, no behavior change; only identifiers, file names, and one new small
  interface**. Commits to the two-interface consumer direction: `IAsyncConsumer` is the
  **async** surface (blocking-in-Java → `Task`), reserving `IConsumer` / `KafkaConsumer`
  for the future **sync** surface (M5). Method names carry **no `Async` suffix** — the
  sync-vs-async distinction is carried by the interface/type, matching Java's method
  names and the Python sibling. Delivered:
  - **Public rename** (`src/Confluent.Kafka/`): `interface IConsumer` →
    `interface IAsyncConsumer : IConsumerCommon, IAsyncDisposable, IDisposable`; **new**
    `interface IConsumerCommon { void Wakeup(); ConsumerGroupMetadata GroupMetadata(); }`
    (the two non-blocking members move off the async interface onto a shared base);
    `class KafkaConsumer` → `AsyncKafkaConsumer`, `class MockConsumer` →
    `AsyncMockConsumer`; drop the `Async` suffix (methods still return `Task`) —
    `PollAsync`→`Poll`, `SubscribeAsync`→`Subscribe`, `UnsubscribeAsync`→`Unsubscribe`,
    `SeekAsync`→`Seek`, `CloseAsync`→`Close`. `Dispose`/`DisposeAsync` unchanged
    (framework contract). File renames: `IConsumer.cs`→`IAsyncConsumer.cs`
    (+ new `IConsumerCommon.cs`), `KafkaConsumer.cs`→`AsyncKafkaConsumer.cs`,
    `MockConsumer.cs`→`AsyncMockConsumer.cs`. `AsyncMockConsumer`'s inherent mock helpers
    (`Assign`/`AddRecord`/`SetPollError`) keep their names. Carried rationale docstrings
    moved onto the renamed members (`Seek`-is-async blocking-`addAndGet` + Python
    divergence; `byte[]` key/value/header; single-owner/not-thread-safe caveat; `Close()`
    surfaces the error unlike `DisposeAsync`; `Wakeup()` cross-thread caveat); CS1591
    intact.
  - **Internal rename** (`Internal/NativeConsumer.cs`): the bridge methods `…Async` →
    `…WithCallback` (their names reflect the callback bridge, not the .NET async
    convention) — `PollWithCallback`, `SubscribeWithCallback`, `UnsubscribeWithCallback`,
    `SeekWithCallback`, `CloseWithCallback`, and `CloseAsyncInternal` →
    `CloseWithCallbackInternal`. `Dispose`/`DisposeAsync`/`SubmitOperation`/
    `SubmitVoidOperation`/`Wakeup`/`GroupMetadata`/`GroupId` unchanged.
  - **Two guardrails held:** (1) the `NativeMethods` P/Invoke declarations and their
    `EntryPoint` strings are untouched — the extern names (`ConsumerPollAsync`, …) mirror
    the C ABI's own `_async` suffix (`kafka_consumer_Consumer_poll_async`, …), reflecting
    the C ABI, not our public naming; `ConsumerCallbacks`, the marshallers, value types,
    `OperationCompletionSource`, the `SafeHandle`s, and `KafkaException` are unchanged.
    (2) The archived M4/P4a docs (`design/history/M4/P4a-public-consumer/`) are NOT
    retro-edited — only this current STATUS moves to the new names.
  - **NOT in scope:** the sync surface (sync `IConsumer`/`KafkaConsumer`/`MockConsumer`,
    sync `Consumer_*` DllImports, sync `NativeConsumer` methods) — later milestone M5; no
    behavior change, no new op, no ABI change; `VSTHRD200` confirmed absent (no analyzer
    enforces the `Async` suffix), so dropping it builds clean under
    `TreatWarningsAsErrors`.
  - **Tests:** all consumer test references renamed (public + internal), **every
    assertion kept** — 122 tests, same count. Parallelism stays disabled (D8.8).
  - **Deviation (recorded):** PLAN sub-steps 1 (interface) and 2 (impl classes) landed as
    **one green commit** — the interface doc crefs the impl-class names and the impls
    implement the renamed interface, so they are the minimal compiling unit for the public
    rename (the library cannot be green with only one half). Sub-steps 3 (internal), 4
    (tests), 5 (docs) are separate commits as planned.
  - **Governance — N=9 is this rename.** Earlier entries (M4/P4a, M3/P3) pre-labeled the
    `Wakeup()`/`GroupId` handle-TOCTOU `DangerousAddRef` hardening a "candidate N=9
    follow-up (unscheduled)". That prediction is superseded: N=9 is the P4b rename, and the
    cross-thread hardening remains **accepted-by-design + unscheduled** (no review number
    assigned) — it is untouched here (pure rename, no behavior change). The three M3/P2
    accepted residuals also remain accepted, unchanged.
  - Approved plan + closed record: `design/history/M4/P4b-async-surface-rename/`. Additive
    commits on `prashah_dev_public_consumer_scaffolding` (the existing M4/P4a stacked PR;
    the PR description is updated to the final `IAsyncConsumer` naming before merge). N=9.
- **Milestone 4 / Phase 4a — "Public Consumer Client (the first usable public cut)":
  DONE (2026-08-04).** The first PUBLIC client surface — promotes the proven internal
  machinery to a Java-shaped, XML-documented public API: subscribe → poll → seek →
  group metadata → close, usable end-to-end broker-free via `MockConsumer` and against
  a real broker with auto-commit. Mode A (every ABI function already ships; no Rust
  authored). Delivered:
  - **Public value types** (namespace `Confluent.Kafka`, library root): promoted
    `ConsumerRecord` / `ConsumerRecords` (internal → `public sealed`); new
    `Header` / `Headers` (read-only view), the `TimestampType` enum, the
    `TopicPartition` `readonly struct` (value equality + Java `"topic-partition"`
    `ToString`), and `ConsumerGroupMetadata` (full four-field set). **`Key`/`Value` +
    `Header.Value` unified on `byte[]?`** (micro-decision A — a deliberate deviation
    from the CLAUDE.md §3 `ReadOnlyMemory` sketch; the copy-out marshaller drops the
    `ReadOnlyMemory` wrap it used, so `unsafe` left `ConsumerRecordsMarshal` too — a net
    simplification, no new copy). The internal `RecordHeader` struct is deleted.
  - **Public interface + clients**: `IConsumer : IAsyncDisposable, IDisposable`
    (minimal, additive-growth, **non-generic** — micro-decision D), and
    `KafkaConsumer` / `MockConsumer` (`public sealed`, both `impl IConsumer`). Both
    **compose** the internal `NativeConsumer` and forward (§2.5 compose-over-absorb) —
    the ~40 internal interop tests + the `unsafe`/`GCHandle` quarantine stay intact.
    `MockConsumer`'s mock helpers (`Assign` / `AddRecord` component-tuple /
    `SetPollError`) are inherent, NOT on `IConsumer` (consumer-threading §2).
  - **`NativeConsumer` edits**: `UnsubscribeAsync` (the ONE new void wire over
    `Consumer_unsubscribe_async`), the `SeekAsync` `offset < 0` precondition
    (`ArgumentOutOfRangeException`, exact Java message `"seek offset must not be a
    negative number"` — the only Java-fidelity behavior fix, PLAN dec. 11),
    `GroupMetadata()` (full four-field read via a new `ConsumerGroupMetadataMarshal` +
    the three group-metadata DllImports), and `CloseAsync(CancellationToken)` (dec. 6
    wiring: one-shot latch → `close_async` → destroy, **surfaces** the close error
    unlike `DisposeAsync`; NO timeout — the ABI has none).
  - **Async/sync split** (from the Java impl, CLAUDE.md §4): `PollAsync` /
    `SubscribeAsync` / `UnsubscribeAsync` / `SeekAsync` / `CloseAsync` async;
    `Wakeup()` / `GroupMetadata()` sync. **`SeekAsync`-is-async** is the load-bearing
    call (Java `seek()` blocks on `addAndGet`) — a deliberate divergence from Python's
    sync `seek`, documented in the docstring.
  - **Tests** (public-surface, PLAN §5): round-trip (incl. non-ASCII out_len path,
    sentinels, churn, byte[] header), async/sync split, SeekAsync exact-message,
    GroupMetadata full-field (real pre-join defaults + non-ASCII + Mock sentinels),
    teardown (Dispose/DisposeAsync/CloseAsync idempotence + unawaited-op residual +
    use-after-dispose), allocation budget, TFM smoke. net462 added to the test TFMs
    (builds cross-platform via `Microsoft.NETFramework.ReferenceAssemblies`; run
    Windows/CI-only). **122 tests, 20/20 full-suite runs green.**
  - **FINDING — pre-existing intermittent host crash under xUnit parallel execution**
    (COMMENTS.DONE.8 D8.8). Bisected to the tracked HEAD *before* any P4a test: the
    accepted single-owner residual (an unawaited-op straggler dispatcher callback after
    the fire-and-forget `Consumer_destroy`) races GC across parallel test collections.
    Fixed with `[assembly: CollectionBehavior(DisableTestParallelization = true)]` — the
    standard setting for a not-thread-safe native-resource suite (no assertion
    weakened; the within-test ops are already serialized by the core guard). A real fix
    of the residual (a Rust-core dispatcher-join on destroy) is out of scope.
  - **Governance — `Wakeup()` is now genuinely public / cross-thread for the first
    time** (the one item P4a changes). Per locked decision 5 = **option (a)**: the
    handle-TOCTOU residual stays accepted-by-design and is documented on the public
    `KafkaConsumer` / `IConsumer` as a not-thread-safe caveat (Python/CKD parity); NO
    per-call `DangerousAddRef` hardening this phase — flagged as a candidate **N=9**
    follow-up (not scheduled). The three M3/P2 accepted residuals remain accepted;
    composition inherits them unchanged.
  - **Deferred (later additive phases, unchanged public shape):** the commit family
    (`Consumer_commit_async` naming minefield), `position` (scalar callback),
    `Assignment`/`Subscription`/`Paused` (owned-list sync), the owned-handle query
    siblings (`committed`/`offsetsForTimes`/`beginning|endOffsets`/`partitionsFor`/
    `listTopics`), `subscribe(pattern)`/`assign`/`pause`/`resume`,
    `ConsumerRebalanceListener`/`OffsetCommitCallback`, serializers + generic
    `IConsumer<TKey,TValue>`, typed `KafkaException` subclasses, `CloseAsync(TimeSpan)`.
  - **⚠ CLAUDE.md §4 package-id pre-publish gate stays OPEN** (M0/P1). P4a adds public
    *types* but does not publish; `IsPackable=false` holds it shut. Decide (own SR
    integration, or diverge the id) before any publish — out of scope here.
  - Approved plan + closed record: `design/history/M4/P4a-public-consumer/`. Additive on
    `prashah_dev_public_consumer_scaffolding` (a NEW PR stacked on the M3/P3 PR).
- **Milestone 3 / Phase 3 — "Poll + the receive path (owned-handle completion
  bridge)": DONE (2026-08-03).** Proves the **result-returning** completion shape
  end-to-end via `poll` — the owned-handle bridge that five sibling query ops reuse
  later (`committed` / `offsetsForTimes` / `beginning|endOffsets` / `partitionsFor` /
  `listTopics`). Delivered: `OperationCompletionSource<TResult>` (the void bridge kept
  as the thin `OperationCompletionSource : <bool>` subclass, all 5 invariants intact);
  the `poll_callback_t` trampoline on `ConsumerCallbacks` (`OnPoll`, free-exactly-once
  in a `finally` on every path — batch destroy after copy-out, `KafkaError` via
  `FromHandle`, per-op `GCHandle`); the **on-dispatcher copy-out** `ConsumerRecordsMarshal`
  (§6.4 default — the native batch is created / copied-out / destroyed entirely inside
  the callback, so no `SafeConsumerRecordsHandle`, no native-backed `ReadOnlyMemory`,
  no leak-on-abandoned-`Task`); internal `ConsumerRecord` / `ConsumerRecords` /
  `RecordHeader` (owned copies; **internal**, under `Internal/`); the receive-path
  DllImports (`poll_async`, the `ConsumerRecords_t` / `ConsumerRecord_t` accessor set
  incl. headers, `MockConsumer_add_record` / `_set_poll_error`, `Consumer_assign`); the
  length-delimited `Utf8Marshal.PtrToString(ptr, len)` (§B3, never NUL-scan, un-defers
  M1/P1 D4); `NativeConsumer.PollAsync`. Mode A (no Rust / header change). NO sync
  `poll` DllImport (decision 5); NO public client type; `ConsumerRecord(s)` stay
  **internal**. Additive on `prashah_dev_asyncbridge_poll_scaffolding` (new PR stacked
  on #135). Approved plan + closed record:
  `design/history/M3/P3-poll-receive-path/`.
  - **Un-deferred → DONE this phase:** the **M3/P1 D1 wakeup-fault one-shot**. `poll` is
    the only op that observes `wakeup()` broker-free, so `Wakeup()` → next `PollAsync`
    faults with a Wakeup `KafkaException` **once**, then a subsequent poll succeeds
    (`ConsumerPollWakeupCancelTests.Wakeup_ThenPoll_FaultsOnce_ThenReusable`) — Java's
    one-shot `WakeupException` semantics, now **deterministic** (was D1-deferred as
    "not reachable without a wakeup-observing op").
  - **Remaining residuals (still deferred — each needs a Rust-core dependency, not a
    .NET change):** (1) the **in-flight `CancellationToken` cancel** (token fires after
    submit but while the poll is mid-flight) — the pre-canceled path is deterministic
    and tested, but the mock poll runs to completion synchronously and exposes no block
    hook, so the in-flight overlap is a genuine race; (2) the **M3/P2 D-Q4 concurrency
    matrix** (a controllable-duration guard-holding op to force a submit→callback
    overlap) — same non-blockable-mock ceiling; (3) a **full end-to-end header
    round-trip** — `MockConsumer_add_record` carries no headers, so only the
    empty-headers case is reachable (the §B3 length-delimited header-key *primitive* is
    directly tested via the record topic + `Utf8MarshalLengthDelimitedTests`). All three
    close when an FFI-exposed blockable mock poll (a `schedule_poll_task` / block hook)
    or a header-carrying `add_record` lands — a Rust-core dependency requested from the
    root `actor-executor`, reviewed by `kafka-critic`. Documented in
    `COMMENTS.DONE.7.md` (the D-Q4 precedent).
  - **Governance (N≥7 → N≥8 renumber):** M3/P3 **takes N=7**, so the STATUS /
    `NativeConsumer` cross-thread **hardening** labels previously pre-labeled "N≥7"
    (the `Wakeup()`/`GroupId()` handle TOCTOU vs teardown; the submit-vs-`destroy`
    handle race) are renumbered to **N≥8**. `poll` makes the wakeup *behavior* testable
    but does NOT make the cross-thread *races* reachable — the binding is still
    internal-only, single-owner, with no public cross-thread `Wakeup()` caller — so
    those items **stay deferred**, now "N≥8, whenever a public client makes `Wakeup()`
    genuinely cross-thread." No dangling "N≥7" label remains in tracked source/STATUS.
- **Milestone 3 / Phase 2 — "Single-owner alignment (drop the managed guard +
  in-flight tracking; keep the completion bridge)": DONE (2026-08-03).** Aligns the
  M3/P1 completion-bridge/teardown machinery to the in-repo Python sibling's
  "single-owner, not thread-safe" contract (`bindings/python/consumer.py`). The Rust
  **core's own** access guard is the serializer; the .NET-only managed mirror
  (`ConsumerAccessGuard`) and single-slot in-flight tracking (`_inFlightContext` /
  `_inFlightOperation`) are removed, as is `OperationCompletionSource.FaultTaskOnly`
  and the `Dispose` snapshot+fault / `DisposeAsync` drain blocks. `Dispose` →
  `close_with_timeout → destroy`; `DisposeAsync` → `close_async → destroy` (no
  separate-op drain — under single-owner the awaiter of an op is its disposer).
  Concurrency now surfaces the core's way: a concurrent **async op** → a faulted
  `Task` (`KafkaException` / ConcurrentModification, delivered by the core inline);
  a concurrent **sync state read** (`GroupId`) → `InvalidOperationException` from the
  core's null-handle path (was `return null`). KEPT (the 5 invariants + bridge core):
  the per-op self-rooting `GCHandle`; the completion callback = sole owner of the
  `GCHandle` free; the atomic `_closed` teardown gate; `SafeHandle` + TCS
  thread-safety; `RunContinuationsAsynchronously`; the whole `ConsumerCallbacks`
  trampoline, all 4 async DllImports, and `Wakeup`'s `if (_closed) return;` check (a
  deliberate divergence safer than Python). **This eliminates the M3/P1 op-submit-
  vs-teardown publish window** (no tracking fields → no window). Mode A (no Rust /
  header change); a localized, reversible simplification. Extends PR #135 as
  additive commits on `prashah_dev_asyncbridge_scaffolding` (no history rewrite).
  Approved plan + closed record:
  `design/history/M3/P2-single-owner-alignment/`.
- **Milestone 3 / Phase 1 — "Completion bridge + first async op (consumer,
  proof-of-plumbing)": DONE (2026-07-27).** The foreign-thread completion callback
  → `Task` bridge — the riskiest new machinery — de-risked BEFORE poll / the
  receive path. Activates the `_async`/callback ABI for the first time (Mode A, no
  Rust authored). Delivered: the void-result bridge (`OperationCompletionSource`,
  `TaskCompletionSource` with `RunContinuationsAsynchronously`, GCHandle keep-alive
  submit→fire, no-throw callback boundary, free-exactly-once); the managed
  one-op-in-flight `ConsumerAccessGuard` (mirrors — does not replace — the core
  guard); two thin proof ops on one bridge — `SubscribeAsync` (SUCCESS) and
  `SeekAsync` unassigned (FAILURE); `Wakeup()` + `CancellationToken` mapping;
  async-aware teardown (`IAsyncDisposable.DisposeAsync` drain→`close_async`→destroy,
  un-defers M2/P1 D3) with the N=5-deferred teardown-thread-safety hardening folded
  in (thread-safe closed flag). NO poll / receive path (Category 3/4 handles,
  `ConsumerRecord(s)`, length-delimited `out_len` strings, copy-out), NO other async
  ops, NO public client type (`KafkaException` remains the only public type) — all
  deferred. Approved plan + closed record:
  `design/history/M3/P1-completion-bridge/`.
- **Milestone 2 / Phase 2 — "SafeHandle marshaller-return hardening": DONE
  (2026-07-22).** The three owned-handle constructors
  (`ConsumerProperties_new` / `KafkaConsumer_new` / `MockConsumer_new`) now
  return their `SafeHandle` subtype **directly** instead of a raw `IntPtr`, so the
  interop marshaller creates-and-sets the handle atomically inside a constrained
  region — closing the M2/P1 `new + SetHandle` allocation-gap window (an async
  abort on net462, or OOM, between obtaining the pointer and `SetHandle` would
  leak the native handle). A **hardening** change, not a bug fix (M2/P1 is correct
  on net8/net10). Mode A (no ABI/Rust change; SafeHandle-return is a classic
  `[DllImport]` feature supported on the netstandard2.0 floor incl. net462).
  `SafeConsumerHandle.FromRaw` removed; `SafeConsumerPropertiesHandle.Create`
  collapses to the marshaller return. NO new public API, NO completion bridge, NO
  poll/subscribe/commit — unchanged from M2/P1 scope. Approved plan + closed
  record: `design/history/M2/P2-safehandle-return-hardening/`.
- **Milestone 2 / Phase 1 — "Error model + first SafeHandle (consumer client
  lifecycle)": DONE (2026-07-21).** The first PUBLIC type (`KafkaException`) plus
  the Category-1 owned-handle consumer lifecycle (create → close → destroy), kept
  INTERNAL (`NativeConsumer`, tested via `InternalsVisibleTo`). Activated the five
  `kafka_common_KafkaError_*` DllImports (declared in M1/P1) as live callers via
  `KafkaException.FromHandle`. Mode A (consumer C ABI already landed — no Rust
  authoring). NO completion bridge, NO poll/subscribe/commit, NO producer, NO
  Category 3/4 receive-path handles, NO public client type yet — all deferred.
  Approved plan + closed record: `design/history/M2/P1-error-model-safehandle/`.
- **Milestone 1 / Phase 1 — "Interop scaffolding + native-load probe": DONE
  (2026-07-20).** The client-agnostic interop FOUNDATION: the `NativeMethods` P/Invoke
  class (8 shared-foundation declarations), the `Utf8Marshal` marshalling helpers, the
  native-copy MSBuild target (un-defers M0/P0 decision D2), and a consumer-namespaced
  native-load probe. Mode A (C ABI already landed — no Rust authoring). NO public
  managed API, NO `SafeHandle`, NO completion bridge, NO Kafka logic yet — all
  deferred to later phases by scope.
- **Milestone 0 / Phase 1 — "Rename binding identity": DONE (2026-07-29).**
  The binding identity is `Confluent.Kafka` (was
  `Confluent.Kafka.ShareConsumer`): solution, strong-name key, both project
  directories and their csprojs, `<RootNamespace>`/`<AssemblyName>`/`<Product>`,
  `<AssemblyOriginatorKeyFile>`, the `InternalsVisibleTo` grant, the
  `ProjectReference`, and the test file-scoped namespace.

  **Compliance work, not preference** — CLAUDE.md §2's file map and §4's
  *Namespace / package id* row already read `Confluent.Kafka`, so the M0/P0
  artifacts were the drift, not the rulebook. Adds **no capability**; same
  milestone because it only corrects M0/P0's output. The old name described a
  KIP-932 feature that `.claude/rules/consumer-threading.md` §20 puts explicitly
  out of scope, so it was actively misleading.

  The strong-name key was **not** regenerated — the `.snk` is byte-identical and
  the public-key token stays `a6a493010a30d243`, so the assembly identity is
  unchanged (only `InternalsVisibleTo Include=` moved; its `Key=` blob is
  verbatim).

  ⚠ **CLAUDE.md §4's package-id pre-publish gate remains OPEN.** The binding now
  shares the `Confluent.Kafka` id with confluent-kafka-dotnet, meaning a project
  can hold ckd 2.x **or** this client, never both (so ckd's Schema-Registry /
  OAuthBearer packages can't be mixed in). This phase makes the *name* collide,
  so the gate is now held shut **structurally** rather than by prose:
  `Microsoft.NET.Sdk` defaults `IsPackable` to **true** for a library and
  `PackageId` to **`$(AssemblyName)`**, so writing neither is the *packable*
  state — a bare `dotnet pack` would emit a package id byte-equal to ckd's. The
  library csproj therefore sets `<IsPackable>false</IsPackable>` explicitly (and
  nothing else packaging-related). Check the **evaluated** property, never the
  absence of an element: `dotnet msbuild <library>.csproj -getProperty:IsPackable`
  → `false`. "No `Pack*` metadata" was never the right
  test either — `Directory.Build.props`'s `<Authors>`/`<Company>`/`<Product>`/
  `<Copyright>` flow into a nuspec on their own. There is also no publish
  automation in the repo (no `.github/workflows`, no `dotnet pack` / `nuget push`
  target anywhere), so nothing can trip the gate today. The decision — own SR
  integration, or diverge the id — is still owed **before any publish**, and
  un-defers by flipping that one line.
- **Milestone 0 / Phase 0 — "Project scaffolding": DONE (2026-07-20).** Pure
  structural skeleton (see `design/history/M0/P0-scaffolding/`).

## What exists now (structure)

```
bindings/dotnet/
├─ Confluent.Kafka.sln                       ← classic .sln, both projects + a "build" solution folder
├─ Directory.Build.props                     ← #nullable enable, LangVersion=latest,
│                                              EnforceCodeStyleInBuild, TreatWarningsAsErrors,
│                                              strong-name signing (shared .snk, both projects)
│                                              — NO AllowUnsafeBlocks here (library-only, M1/P1 D2)
├─ .editorconfig · .gitignore
├─ src/
│  └─ Confluent.Kafka/
│     ├─ Confluent.Kafka.csproj                ← TFMs netstandard2.0;net8.0;net10.0
│     │                                           (net462 via ns2.0), System.Memory on the ns2.0
│     │                                           leg only, GenerateDocumentationFile,
│     │                                           IsPackable=false (M0/P1 — holds §4's id gate shut),
│     │                                           InternalsVisibleTo → UnitTests (public key);
│     │                                           M1/P1: + <AllowUnsafeBlocks> (library only),
│     │                                           + native-copy MSBuild target (per-OS filename via
│     │                                           IsOSPlatform, profile from $(Configuration),
│     │                                           repo root 4 levels up, <Content> transitive,
│     │                                           + <Error> guard if native absent)
│     ├─ KafkaException.cs                      ← M2/P1: FIRST public type. sealed KafkaException :
│     │                                            Exception, flat Code/IsRetriable/IsFatal + Message;
│     │                                            internal FromHandle(IntPtr) (msg before free, copy
│     │                                            out, destroy in finally); flat-now/typed-later
│     └─ Internal/
│        ├─ NativeConsumer.cs                   ← M2/P1: internal lifecycle wrapper (unsafe-free, D4):
│        │                                         config -> ConsumerProperties_put -> KafkaConsumer_new
│        │                                         (FromHandle on out_error) -> SafeConsumerHandle;
│        │                                         graceful Dispose (close_with_timeout -> destroy);
│        │                                         preconditions -> ArgumentNullException/ArgumentException.
│        │                                         M2/P2: consumes the SafeHandle returns (dispose the
│        │                                         IsInvalid handle on error; defensive IsInvalid guard).
│        │                                         M3/P1: proof async ops (SubscribeAsync/SeekAsync via a
│        │                                         shared SubmitVoidOperation), Wakeup(), guarded GroupId()
│        │                                         state read; thread-safe closed flag (folds N=5 deferred);
│        │                                         IAsyncDisposable.DisposeAsync (drain->close_async->destroy).
│        │                                         M3/P2: single-owner alignment — drop the managed guard +
│        │                                         in-flight tracking (_guard/_inFlightContext/_inFlightOp);
│        │                                         Dispose=close_with_timeout->destroy, DisposeAsync=close_async
│        │                                         ->destroy (no separate-op drain); GroupId throws
│        │                                         InvalidOperationException on the null/concurrent-rejection path
│        ├─ OperationCompletionSource.cs        ← M3/P1: per-op callback->TCS context; TCS built with
│        │                                         RunContinuationsAsynchronously; KafkaError->KafkaException
│        │                                         (FromHandle); CancellationToken->wakeup +
│        │                                         OperationCanceledException; idempotent GCHandle free
│        │                                         (Complete/AbandonBeforeSubmit).
│        │                                         M3/P2: guard param/field + FaultTaskOnly removed; callback =
│        │                                         sole owner of the GCHandle free (only other path is
│        │                                         AbandonBeforeSubmit, when native never ran)
│        └─ Interop/                            ← the P/Invoke boundary — `unsafe` lives ONLY here
│           ├─ NativeMethods.cs                        ← internal static class NativeMethods: M1/P1 (8
│           │                                      shared decls) + M2/P1 consumer lifecycle
│           │                                      (KafkaConsumer_new/MockConsumer_new/close/
│           │                                      close_with_timeout/destroy) + group-metadata trio.
│           │                                      M2/P2: the three constructors return their SafeHandle
│           │                                      subtype directly (marshaller create-and-set).
│           │                                      M3/P1: 4 async decls — subscribe_async (topics as
│           │                                      IntPtr[] = const char* const*) / seek_async / wakeup /
│           │                                      close_async (op-callback as a kept-alive Cdecl delegate)
│           ├─ ConsumerCallbacks.cs                   ← M3/P1: [UnmanagedFunctionPointer(Cdecl)]
│           │                                      OperationCallback delegate type + one static readonly
│           │                                      rooted instance + the no-throw callback body
│           ├─ SafeHandleZeroIsInvalid.cs             ← M2/P1: shared base, IsInvalid => handle==Zero (D2)
│           ├─ SafeConsumerPropertiesHandle.cs        ← M2/P1: config handle (-> ConsumerProperties_destroy);
│           │                                            M2/P2: Create collapses to the marshaller return
│           ├─ SafeConsumerHandle.cs                  ← M2/P1: client handle (-> Consumer_destroy, bare
│           │                                            last-resort; graceful close is in NativeConsumer).
│           │                                            M2/P2: FromRaw removed (arrives marshaller-wrapped)
│           └─ Utf8Marshal.cs                          ← internal static class Utf8Marshal: Pin (disposable
│                                                  call-scoped pinned buffer) + PtrToString
│                                                  (NUL-terminated form; null for IntPtr.Zero)
└─ tests/
   └─ Confluent.Kafka.UnitTests/               ← TFMs net8.0;net10.0, unsafe-free
      ├─ TfmSentinelTests.cs                    ← M0/P0 TFM-sentinel smoke test (root: harness-level)
      ├─ KafkaExceptionTests.cs                 ← M2/P1: public-type test (root): classic -> Code 35
      │                                            + I1 both-false + msg; café msg echo; FromHandle(Zero)
      │                                            (M3/P2: ConsumerAccessGuardTests removed with the guard)
      ├─ TestTimeout.cs                         ← M2/P1: fail-fast deadline helper (hang -> test failure).
      │                                            M3/P1: + async Run(Func<Task>) overload (bridge/drain guard)
      └─ Interop/                               ← mirrors the library interop area (public test
         │                                         classes; "Interop" not "Internal/Interop" — the
         │                                         Internal visibility marker is library-only, §2)
         ├─ NativeLoadProbeTests.cs             ← M1/P1: 2 tests, both invoke a native [DllImport]
         │                                         (smoke new/put/destroy; non-ASCII put no-crash).
         │                                         M2/P2: props via `using SafeConsumerPropertiesHandle`
         │                                         (Dispose frees; asserts !IsInvalid)
         ├─ Utf8MarshalTests.cs                 ← M1/P1: managed Utf8Marshal codec round-trip
         │                                           + PtrToString(Zero)==null (no native call)
         ├─ SafeConsumerHandleTests.cs          ← M2/P1: lifecycle (mock + real), double-Dispose,
         │                                           use-after-Dispose, create/dispose many.
         │                                           M2/P2: KEY regression — classic-protocol
         │                                           KafkaConsumer_new -> IsInvalid handle + non-null
         │                                           out_error; Dispose skips ReleaseHandle (no spurious
         │                                           destroy); error round-trips via FromHandle
         ├─ ConsumerConfigMarshalTests.cs       ← M2/P1: config success + preconditions (null dict /
         │                                           null value / post-Dispose)
         ├─ Utf8RoundTripTests.cs               ← M2/P1 (D5 CLOSED): non-ASCII group.id -> group_metadata
         │                                         -> group_id readback == input (broker-free)
         ├─ ConsumerCompletionBridgeTests.cs    ← M3/P1: SUCCESS (subscribe, churned) + FAILURE (seek
         │                                         unassigned -> KafkaException Code -1/flags/Message,
         │                                         churned); no-throw boundary; GCHandle keep-alive under
         │                                         GC; RunContinuationsAsynchronously (bridge driven
         │                                         directly, off the completing thread); chained ops.
         │                                         M3/P2: the four FaultTaskOnly_* component tests removed
         │                                         (the sync-Dispose fault machinery is gone)
         ├─ ConsumerAsyncOperationTests.cs      ← M3/P1: wakeup (safe/reusable/during-op); cancellation
         │                                         (pre-canceled -> OperationCanceledException); GroupId
         │                                         read round-trip (incl. non-ASCII).
         │                                         M3/P2: GroupId now unguarded (single-owner); concurrent
         │                                         -rejection -> InvalidOperationException verified by
         │                                         inspection (not deterministic broker-free, D-Q4)
         └─ ConsumerAsyncTeardownTests.cs       ← M3/P1: DisposeAsync + Dispose with op in flight RETURN;
                                                   double/mixed/concurrent teardown safe; use-after-dispose
                                                   -> ObjectDisposedException.
                                                   M3/P2: op-in-flight teardown cases repurposed to assert
                                                   return-without-hang only (no separate-op drain; the
                                                   unawaited-op strand+leak is an accepted residual)
```

## Verification state (M4/P4b DoD — Actor, all green)

- `cargo build --features ffi` — native cdylib + generated header present (run FIRST,
  CLAUDE.md §7.1). **No ABI change this phase (Mode A).**
- `dotnet build` — **0 warnings, 0 errors** across all library TFMs (netstandard2.0,
  net8.0, net10.0) and all test TFMs (net462, net8.0, net10.0);
  `TreatWarningsAsErrors` + `EnforceCodeStyleInBuild` + `GenerateDocumentationFile`
  active. **CS1591 satisfied on every renamed public member** (`IAsyncConsumer`, the new
  `IConsumerCommon`, `AsyncKafkaConsumer`, `AsyncMockConsumer`, and the un-suffixed
  methods). Apache-2.0 header on the new `IConsumerCommon.cs`; no TODO/FIXME. No analyzer
  suppressions needed — **VSTHRD200 confirmed absent**
  (`Microsoft.VisualStudio.Threading.Analyzers` not referenced), so dropping the `Async`
  suffix builds clean.
- `dotnet test -f net10.0` — **122 passed, 0 failed** (same count as M4/P4a — a pure
  rename, no test weakened, no coverage lost); **20/20 full-suite runs green, 0 crashes /
  0 failures** (the stability gate). Serial execution
  (`[assembly: CollectionBehavior(DisableTestParallelization = true)]`) stays enabled
  (D8.8 — not re-enabled).
- `dotnet format --verify-no-changes` — clean.
- **CI-only (not blocking):** the local runtime is .NET 10; the net8.0 test *run* and
  net462 are CI/Windows-only. **All three test *build* legs (net462 / net8.0 / net10.0)
  pass locally**, and the library's three TFMs build.
- **Docs match code:** this STATUS is updated to the new names + N=9; the archived M4/P4a
  docs are left intact as the historical record (guardrail 2). The `NativeMethods` extern
  names / ABI `EntryPoint` strings are untouched (guardrail 1).

## Verification state (M4/P4a DoD — Actor, all green)

- `cargo build --features ffi` — native cdylib + regenerated header present (run
  FIRST, CLAUDE.md §7.1). **No ABI change this phase (Mode A).**
- `dotnet build` — **0 warnings, 0 errors** across all library TFMs
  (netstandard2.0, net8.0, net10.0) and all test TFMs (**net462**, net8.0, net10.0);
  `TreatWarningsAsErrors` + `EnforceCodeStyleInBuild` + `GenerateDocumentationFile`
  active. **CS1591 satisfied on every new public member** — the first new public
  surface since M2/P1 (`IConsumer`, `KafkaConsumer`, `MockConsumer`, `ConsumerRecord`,
  `ConsumerRecords`, `Header`, `Headers`, `TimestampType`, `TopicPartition`,
  `ConsumerGroupMetadata`). No analyzer suppressions needed (CA1815 satisfied by
  `TopicPartition`'s `==`/`!=` + value equality). Apache-2.0 header on every new file;
  no TODO/FIXME.
- `dotnet test -f net10.0` — **122 passed, 0 failed**; **20/20 full-suite runs green,
  0 crashes / 0 failures** (the §5.8 stability gate). Every awaited op / teardown under
  a `TestTimeout` hang guard. Serial execution
  (`[assembly: CollectionBehavior(DisableTestParallelization = true)]`) fixes a
  pre-existing intermittent host crash (D8.8) that only manifested under xUnit's
  default cross-collection parallelism.
- `dotnet format --verify-no-changes` — clean.
- **CI-only (not blocking):** the local runtime is .NET 10; the net8.0 test *run* and
  net462 (via ns2.0 for the library; a direct net462 test TFM) are CI/Windows-only.
  **All three test *build* legs (net462 / net8.0 / net10.0) pass locally**, and the
  library's three TFMs build.
- **Docs match code:** `ffi-marshalling.md` §B (single-owner, copy-out, the 5
  invariants) and CLAUDE.md §3/§4 (the idiom map, `byte[]` key/value deviation,
  async/sync split) describe the landed public surface; the byte[] deviation +
  CloseAsync wiring + GroupMetadata reachability + the D8.8 finding are recorded in
  `COMMENTS.DONE.8.md`.

## Verification state (M3/P2 DoD — Actor, all green)

- `cargo build --features ffi` — native cdylib + regenerated header present (run
  FIRST, CLAUDE.md §7.1). No ABI change this phase (Mode A).
- `dotnet build` — **0 warnings, 0 errors** across all library TFMs
  (netstandard2.0, net8.0, net10.0) and both test TFMs (net8.0, net10.0);
  `TreatWarningsAsErrors` + `EnforceCodeStyleInBuild` + CS1591 active. No new public
  type → no new CS1591 surface. No dangling references to `ConsumerAccessGuard` /
  `FaultTaskOnly` / `_inFlight*` (verified by grep; the only remaining mentions are
  intentional prose in the `NativeConsumer` docstrings recording what was removed).
- `dotnet test -f net10.0` — **43 passed, 0 failed** (~1 s); the M3/P1 set minus the
  5 `ConsumerAccessGuardTests` + the 4 `FaultTaskOnly_*` bridge tests (52 → 43).
  Every awaited op / teardown under a `TestTimeout` hang guard, so a bridge/drain
  hang fails fast.
- `dotnet format --verify-no-changes` — clean.
- **CI-only (not blocking):** the local runtime here is .NET 10 (SDK 10.0.300,
  runtime 10.0.8); the net8.0 test *run* and net462 (via netstandard2.0) are
  CI-only. Both *build* legs pass.
- **Docs match code:** `ffi-marshalling.md` §B1/§B5/§B7 describe the single-owner /
  no-managed-guard model the landed `NativeConsumer` / `OperationCompletionSource`
  code implements (core-delivered concurrency, `InvalidOperationException` state
  read, `close_(with_timeout|async) → destroy` teardown with no separate-op drain).

## Decisions in force (M3/P2)

- **D-Q1 — `DisposeAsync` no longer drains a separately-submitted in-flight op.**
  Teardown is `close_async → destroy` (`Dispose`: `close_with_timeout → destroy`);
  under single-owner the awaiter of an op is its disposer, so there is no concurrent
  submitter to drain. Matches Python's `close()`. Accepted residual: an *unawaited*
  in-flight op + teardown may strand + leak once (`DisposeAsync` on the awaiting task
  is leak-free).
- **D-Q2 — `GroupId`: `return null` → `throw InvalidOperationException` on the
  core's concurrent-rejection (null-handle) path.** Internal-only (no public
  contract break); mirrors Python `_concurrent_error()` (`None → RuntimeError`) and
  the CLAUDE.md §3 idiom-map row (concurrent sync state read →
  `InvalidOperationException`).
- **D-Q3 — extends PR #135 on `prashah_dev_asyncbridge_scaffolding`** as additive
  commits (no new branch, no separate PR, no history rewrite of M3/P1's commits).
  #135 grows to contain the full "M3/P1 adds the guard + tracking, then M3/P2
  removes it" build-then-simplify arc.
- **D-Q4 — no flaky forced-overlap test for the `GroupId` concurrent →
  `InvalidOperationException` mapping.** Broker-free `MockConsumer` ops resolve
  instantly (the core guard is held only microseconds) and the one guard-holding op
  with a controllable duration is `poll` (out of scope), so the overlap is not
  deterministically reproducible this phase. Tested the reachable seam (the `GroupId`
  round-trip incl. non-ASCII); the null-handle → `InvalidOperationException` mapping
  is verified by code inspection and documented in `COMMENTS.DONE.6.md`, mirroring
  how M3/P1 documented D1/D2. **Honest cost:** removing the managed guard also
  removed M3/P1's deterministic component-level `ConsumerAccessGuardTests`, so this
  one concurrency behavior regresses from deterministic (component-level) to
  non-deterministic — accepted and documented.
- **The 5 invariants + bridge core are KEPT unchanged:** per-op self-rooting
  `GCHandle`; callback = sole owner of the `GCHandle` free; atomic `_closed`
  teardown gate (`TryBeginClose` / `ThrowIfClosed`); `SafeHandle` + TCS
  thread-safety; `RunContinuationsAsynchronously`. The whole `ConsumerCallbacks`
  trampoline, all 4 async DllImports, and `Wakeup`'s `if (_closed) return;` check
  (a deliberate divergence safer than Python) are unchanged.

## Verification state (M3/P1 DoD — Actor, all green)

- `cargo build --features ffi` — native cdylib + regenerated header present (run
  FIRST, CLAUDE.md §7.1).
- `dotnet build` — **0 warnings, 0 errors** across all library TFMs
  (netstandard2.0, net8.0, net10.0) and both test TFMs (net8.0, net10.0);
  `TreatWarningsAsErrors` + `EnforceCodeStyleInBuild` + CS1591 active. No new public
  type → no new CS1591 surface. The ns2.0 leg resolves `IAsyncDisposable` /
  `ValueTask` via `Microsoft.Bcl.AsyncInterfaces` (M3/P1 D3).
- `dotnet test -f net10.0` — **52 passed, 0 failed** (M3/P1 set incl. the N=5
  Finding-1/Finding-3 fixups: 5 access guard, 12 completion bridge — the four
  `FaultTaskOnly_*` among them, 6 async op, teardown, and the carried M0–M2 tests);
  ~370 ms — every awaited op / teardown under a `TestTimeout` hang guard, so a
  bridge/drain hang would fail fast.
- `dotnet format --verify-no-changes` — clean.
- **CI-only (not blocking):** only the .NET 10 runtime is installed locally; the
  net8.0 test *run* and net462 (via netstandard2.0) are CI-only. Both *build* legs
  pass.

## Decisions in force (M3/P1)

- **D1 — wakeup-fault on the in-flight proof op is NOT reachable this phase
  (source-verified deviation from the PLAN's literal wakeup test).** A
  `MockConsumer` observes `wakeup()` **only** inside `poll()`
  (`src/consumer/mock_consumer.rs` poll Step 4); `subscribe`/`seek` never check the
  flag, and `acquire()` (`src/ffi/consumer.rs`) does not either — so the "in-flight
  op faults with a Wakeup `KafkaException` once" assertion needs `poll` (out of
  scope). The full wakeup + cancellation machinery is implemented (correct once poll
  lands); the tested slices are the reachable ones: `Wakeup()` is safe / leaves the
  consumer reusable, and a **pre-canceled** token maps to
  `OperationCanceledException` deterministically. The in-flight-cancel → wakeup →
  `OperationCanceledException` translation is wired (`RegisterCancellation`) but
  only deterministically exercisable once a wakeup-observing op exists.
- **D2 — the concurrency exception-type matrix is tested at the `ConsumerAccessGuard`
  component level (deterministic), not via a forced native op overlap.** Instant
  Mock ops make a genuine submit→callback overlap non-deterministic; the guard is a
  pure managed mirror, so its rejection types (async op → `KafkaException`; state
  read → `InvalidOperationException`) are fully proven as a component. The guard's
  wiring into `NativeConsumer` is exercised by the op / group-metadata tests
  (released between ops; a guarded `GroupId()` round-trips).
- **D3 — `Microsoft.Bcl.AsyncInterfaces` (8.0.0) added for the ns2.0 leg only.** It
  supplies `IAsyncDisposable` + the `ValueTask` async builder absent on the
  netstandard2.0 floor (built-in on net8.0+) — the enabling dependency for the
  primary `DisposeAsync`. A standard facade, conditioned exactly like `System.Memory`
  (ns2.0-only); no NuGet packaging of the binding itself (ffi §0.2 unchanged).
- **D4 — sync `Dispose` kept M2-shape (no drain) + a post-destroy Task-fault.**
  `Dispose` stays `close_with_timeout` → destroy (thread-safe closed flag added),
  NOT sync-over-async; the drain-first path is `DisposeAsync` (primary, ffi §B7).
  **Post-Critic (N=5) fix (Findings 1 + 3):** `close_with_timeout` is a *guarded* sync
  op, so while an async op genuinely holds the core guard the close is rejected
  (ConcurrentModification) and does **not** drain — the following `Consumer_destroy`
  then cancels the op's callback, which (before the fix) stranded the op `Task`
  (Finding 1). `Dispose` now, **after** destroy, faults any pending op's `Task`
  (`OperationCompletionSource.FaultTaskOnly` → `ObjectDisposedException`) so a
  fire-and-forget awaiter cannot strand — via idempotent primitives (`TrySetException`
  no-ops if completed), race-safe against a callback that fired before destroy, and NOT
  sync-over-async (it never waits on the op `Task`). Crucially it does **not** free the
  `GCHandle`: the completion callback is the **sole owner** of that free (Finding 3),
  aligning with the in-repo Python (`Py_DECREF` in the op trampoline; close drains then
  bare `_destroy`) and confluent-kafka-dotnet (`gch.Free()` in the delivery-report
  callback; `Dispose` drains via `callbackTask.Wait()` then destroys). Freeing it from
  `Dispose` was the case-B use-after-free — a completion job queued before destroy fires
  *after* it (the ABI drains queued dispatcher jobs without joining) and must recover a
  live handle. **Accepted residual (case A):** if destroy cancels the op before its
  callback is queued, that one op's `GCHandle` leaks — a rare, one-time, teardown-only
  leak in a misuse case (unawaited in-flight op + sync `Dispose`); the Python/CKD
  siblings accept the same residual, and `DisposeAsync` drains so it has no leak. So an
  op-in-flight sync `Dispose` no longer strands the `Task`; users wanting no leak use
  `DisposeAsync`.
- **N=5 deferred hardening — DONE.** The non-atomic `_disposed` bool is replaced by a
  thread-safe closed flag (`Interlocked`, `TryBeginClose`) + the §B5 access guard, so
  double / concurrent / mixed `Dispose`/`DisposeAsync` are safe. Per the deferred
  note, teardown is guarded the CKD way (thread-safe closed check + access guard) —
  NO per-call `SafeHandle` AddRef, NO close/destroy-as-SafeHandle-param.

## Verification state (M2/P2 DoD — Actor + Critic, all green)

- `cargo build --features ffi` — native cdylib + header present (run FIRST).
- `dotnet build` — **0 warnings, 0 errors** across all library TFMs
  (netstandard2.0, net8.0, net10.0) and both test TFMs (net8.0, net10.0);
  `TreatWarningsAsErrors` + `EnforceCodeStyleInBuild` + CS1591 active. Removing the
  now-unused `using System;` from the two SafeHandle files and `NativeLoadProbeTests`
  kept IDE0005 from failing the build.
- `dotnet test -f net10.0` — **20 passed, 0 failed** (19 M2/P1 carried + 1 new
  M2/P2 failure-path regression); ~250 ms.
- `dotnet format --verify-no-changes` — clean.
- **CI-only (not blocking):** only the .NET 10 runtime is installed locally; the
  net8.0 test *run* and net462 (via netstandard2.0) are CI-only. Both *build* legs
  pass.

## Decisions in force (M2/P2)

- **SafeHandle-return over `new + SetHandle`** — the three owned-handle
  constructors return their `SafeHandle` subtype directly; the marshaller invokes
  the private parameterless ctor and sets the handle atomically. Classic
  `[DllImport]` feature (no `[LibraryImport]`), supported on the netstandard2.0
  floor incl. net462 — the TFM where the async-abort window actually exists.
- **`FromRaw` removed entirely** — the handle now arrives marshaller-wrapped; no
  call site needs a thin non-marshalling helper, so none was kept (PLAN §2).
- **Defensive `IsInvalid`-without-error guard throws `KafkaException`** — the ABI
  contract says a null `out_error` implies a non-null handle, so a `(null handle,
  null error)` return is a core contract violation (not a caller programmer error),
  surfaced on the operational `KafkaException` surface with a descriptive message.
  Can't-happen per the header; the guard exists so an IsInvalid handle is never
  stored (a later `Handle` read would hand back a null pointer).
- **Unchanged from M2/P1** — `ReleaseHandle` bodies, `NativeConsumer.Dispose`
  graceful close→destroy, the `put` loop, D6 (props stays a SafeHandle in-param),
  the `KafkaError` five decls, `KafkaException.FromHandle`.

## Verification state (M2/P1 DoD — Actor + Critic, all green)

- `cargo build --features ffi` — native cdylib + regenerated header present (run
  FIRST, CLAUDE.md §7.1).
- `dotnet build` — **0 warnings, 0 errors** across all library TFMs
  (netstandard2.0, net8.0, net10.0) and both test TFMs (net8.0, net10.0), with
  `TreatWarningsAsErrors` + `EnforceCodeStyleInBuild` + `GenerateDocumentationFile`
  active. CS1591 is enforced on the public `KafkaException`; no analyzer
  suppressions were needed (the Java-style standard exception constructors satisfy
  CA1032).
- `dotnet test -f net10.0` — **19 passed, 0 failed** (4 M1/P1 carried + 15 new:
  6 error/precondition, 5 lifecycle, 3 config-marshal, 1 D5 round-trip); ~266 ms
  total (broker-less close is near-instant, so the fail-fast timeout guard never
  trips).
- `dotnet format --verify-no-changes` — clean.
- **CI-only (not blocking):** only the .NET 10 runtime is installed locally; the
  net8.0 test *run* and net462 (via netstandard2.0) are CI-only. Both *build* legs
  pass.

## Decisions in force (M2/P1)

- **D2** — `SafeHandleZeroIsInvalid` base (`IsInvalid => handle == Zero`), not
  `SafeHandleZeroOrMinusOneIsInvalid` (−1 is not our contract; all `_destroy` are
  null-safe).
- **D3 (deviation from CLAUDE.md §4)** — synchronous `IDisposable.Dispose()` only
  this phase; `IAsyncDisposable.DisposeAsync()` deferred with the completion bridge
  (the only close primitive in scope is the synchronous
  `Consumer_close_with_timeout`; wiring `DisposeAsync` now would be
  sync-over-async or depend on the deferred bridge). Recorded in
  `design/history/M2/P1-error-model-safehandle/COMMENTS.DONE.3.md`.
- **D4** — the lifecycle wrapper (`NativeConsumer`) lives under `Internal/` (not
  `Internal/Interop/`): `unsafe`-free (safe `Utf8Marshal.Pin` + `SafeHandle`),
  keeping `unsafe` quarantined to `Internal/Interop/`.
- **D5 — CLOSED (verified empirically).** A configured non-ASCII `group.id`
  surfaces broker-free / pre-join (the core stubs `group_metadata()` from the
  configured id before join), so the UTF-8 config-value round-trip
  (`Consumer_group_metadata` → `group_id` → `PtrToString` == input) is kept, not
  deferred. Adds the three group-metadata DllImports + an owned Category-3 handle
  marshal-then-destroy in the test.
- **D6** — `props` passed to `KafkaConsumer_new` as the SafeHandle type (marshaller
  does DangerousAddRef/Release); disposed in a `finally` after the call (header:
  caller retains props ownership).
- **Dispose close-error handling** — `Dispose()` consumes the close error via
  `FromHandle` (freed exactly once) but does NOT rethrow (Dispose must not throw;
  surfacing close errors is the future `CloseAsync(TimeSpan)`'s job).
- **Decision reversal (post-M2/P2, PR #134 review) — `KafkaException` un-sealed.**
  `KafkaException` is now `public class` (not `public sealed class`), aligning with
  CLAUDE.md §3's sketch (which already shows `public class KafkaException`) + the
  flat-now/typed-later intent (§4 / ffi §A5) — reversing the M2/P1 PLAN's `sealed`
  choice, per user direction during the PR #134 review. Non-breaking (source +
  binary compatible). The archived M2/P1 PLAN + `COMMENTS.DONE.3` are left intact
  as the historical record; this reversal lives here in current STATUS only.

## Verification state (M1/P1 DoD — Actor AND Critic ran independently, all green)
## Verification state (M1/P1 DoD — Actor AND Critic ran independently, all green; re-verified after the M0/P1 rename merge)

- `cargo build --features ffi` — cdylib `target/debug/libconfluent_kafka.dylib`
  + generated header `target/include/confluent_kafka.h` produced (run FIRST,
  CLAUDE.md §7.1).
- `dotnet build` — **0 warnings, 0 errors** across all library TFMs
  (netstandard2.0, net8.0, net10.0) and both test TFMs (net8.0, net10.0), with
  `TreatWarningsAsErrors` + `EnforceCodeStyleInBuild` active. `/unsafe+` on the
  library triggered **no** analyzer warnings (CA5392 is opt-in; SYSLIB1054 is
  Info-severity) — so **no suppressions were needed** (M1/P1 decision D5).
- `dotnet test -f net10.0` — **4 passed, 0 failed** (2 native-load probe + 1
  `Utf8Marshal` codec + the M0/P0 sentinel). The native loaded, the first
  `[DllImport]` round-tripped, and UTF-8 marshalled into native correctly.
- `dotnet format --verify-no-changes` — clean.
- **CI-only (not blocking this phase):** only the .NET 10 runtime is installed
  locally (`dotnet --list-runtimes` shows only Microsoft.NETCore.App 10.0.x).
  The net8.0 test *run* needs the .NET 8 runtime and net462 needs Windows — both
  are **CI-only**. Both *build* legs succeed; only the *runs* are deferred.
- **Handled by the two-stage pipeline (CLAUDE.md §7.1):** the native-copy target
  uses `<Content>` (not `<None>`) so the cdylib flows transitively to the
  referencing TEST project's output dir, where the probe resolves it via default
  `[DllImport]` probing.

Additional gates specific to M0/P1 (the rename), all green:

- The pre-rename `src/` and `tests/` project directories are fully gone from
  disk — stale `obj/` and `bin/` were destroyed *before* the moves, since
  `git mv` relocates only tracked files and untracked build output would
  otherwise have kept the old directories alive with a stale assembly and a
  cached `project.assets.json` naming the old `AssemblyName`.
- No tracked path carries the old identity, and no build or code file mentions
  it. The exact invariant is **scoped to build and code**, and both halves are
  checkable:
  `grep -rIn "ShareConsumer" . --exclude-dir=design --exclude='COMMENTS*.md'` →
  empty, and `git ls-files | grep -i shareconsumer` → empty. It is scoped rather
  than absolute because **four** documentation surfaces under `design/` name the
  old identity deliberately — the archived M0/P0 `PLAN.md` (6 occurrences,
  including its dated supersession note), the archived M0/P0
  `COMMENTS.DONE.1.md` (2), this phase's own
  `design/history/M0/P1-rename-identity/PLAN.md` (18 — a rename plan must name
  what it renames), and this file itself — its transition narrative above and
  its *Governance pointers* section below. That section links the first three;
  the fourth is this file. The M0/P1 review record `COMMENTS.DONE.1.md` quotes
  them too, hence the second exclusion.
- `.snk` byte-identical across the move (SHA-256 `d33f5c98…8eb197`); the
  `InternalsVisibleTo` `Key=` blob still equals `sn -tp` on the key file, and
  `Include=` matches the test project's `<AssemblyName>`.
- All 7 path changes recorded by git as **renames**, not delete+create.
- `Confluent.Kafka.sln` — a surgical 2-line edit (the two project entries): the
  7 GUIDs, `ProjectConfigurationPlatforms`, `NestedProjects`, the `build`
  folder's `SolutionItems`, and the UTF-8 BOM are all unchanged. The solution
  was **not** regenerated (SDK 10 would emit `.slnx`; classic `.sln` is a
  standing M0/P0 deviation).

## Decisions in force (M1/P1)

- **D1 (un-defers M0/P0 D2)** — native-copy MSBuild target landed; per-OS
  filename via MSBuild, profile from `$(Configuration)`, repo root 4 levels up,
  `<Content>` transitive, never a hardcoded path/filename (ffi §0.2 — the
  **pre-publish** half of the two-phase delivery model).
- **D2** — `<AllowUnsafeBlocks>` on the LIBRARY csproj only; test project stays
  unsafe-free; `unsafe` confined to `Internal/Interop/`.
- **D3** — classic `[DllImport]`, uniform across all TFMs (netstandard2.0 floor
  forbids `[LibraryImport]`/`PtrToStringUTF8`/`LPUTF8Str`).
- **D4** — `Utf8Marshal.Pin` = disposable call-scoped pin (`using`); `Utf8Marshal.PtrToString`
  = NUL-terminated form only (length-delimited receive-path form deferred).
- **D5** — analyzer suppressions contingent; none fired, none added.

## Decisions in force (M0/P0)

- **D1** — Library TFMs `netstandard2.0;net8.0;net10.0` (net462 via ns2.0).
- **D2** — Native-copy MSBuild target + Rust build deferred to the first
  implementation phase — **un-deferred in M1/P1 D1 above**.
- **D3** — Empty folders via `.gitkeep`, no placeholder types.
- **D4** — Test framework = xUnit.

Deviations recorded during execution (see the archived review record under
`design/history/M1/P1-interop-scaffolding/COMMENTS.DONE.2.md`):
- XML-comment MSB4025 fix in the csproj comment (literal `--` illegal in XML
  comments — reworded).
- `.gitkeep` deletion grouped into the csproj commit (commit-grouping only).

## Watch-item for future phases — RESOLVED post-close (`9ae31fa`)

- `PinnedUtf8String` was a `readonly struct` holding a `GCHandle`; `Dispose()`
  freed a compiler defensive copy. Correct under M1/P1's single-`using`
  ownership, but a later phase that **stores or copies** a `PinnedUtf8String`
  would have hit the false "idempotent for a single owner" claim (double-`Dispose`
  / disposed by-value copy would double-free the runtime handle). Recorded in
  `.claude/agent-memory/dotnet-critic/interop_review_patterns.md`.
- **Resolved in `9ae31fa`:** `PinnedUtf8String` is now a `sealed class`, so
  `Dispose()` mutates the real `GCHandle` field (no defensive copy) — the unpin
  is genuinely idempotent and the value-copy double-free hazard is gone. Verified:
  `dotnet build` 0/0 all TFMs, `dotnet test -f net10.0` 4/4, format clean. The
  archived `design/history/M1/P1-interop-scaffolding/COMMENTS.DONE.2.md` is left
  unchanged as the phase-close snapshot.

## Review outcome (M3/P1)

Critic (N=5) review of `285b04c`/`d6f3022`/`3d0245f`/`1243350`/`259d098`: all four
DoD gates independently re-verified green; deviations D1–D4 verified sound; the core
bridge (free-once, GCHandle keep-alive, `RunContinuationsAsynchronously`, no-throw
boundary, `DisposeAsync` drain, marshalling, error classification, scope) had **no
defects**. Two findings, both on the sync-teardown / handle-lifetime edges (not the
async bridge):
- **Finding 1 [MEDIUM] — FIXED (its GCHandle-free part corrected by Finding 3).** Sync
  `Dispose` with an async op in flight stranded the op `Task` (and, originally, leaked
  the per-op `GCHandle`): the guarded `close_with_timeout` is rejected (no drain) while
  the op holds the core guard, then `Consumer_destroy` cancels the callback. Fixed by a
  post-destroy Task-fault (`OperationCompletionSource.FaultTaskOnly`; see D4); the
  masking `Dispose_WithOpInFlight` test now observes the op `Task` to a terminal state
  (+ a churn/GC variant). Not sync-over-async, race-safe.
- **Finding 3 [LOW/latent, memory-safety] — RESOLVED (re-review of the Finding-1 fixup).**
  The Finding-1 fix originally freed the `GCHandle` from `Dispose` (`FaultAndReclaim`);
  that is a case-B use-after-free — a completion job queued before `Consumer_destroy`
  fires *after* it (the ABI drains queued dispatcher jobs without joining) and
  dereferences the freed/recycled handle via `GCHandle.FromIntPtr(userData).Target`.
  Resolved by making the completion callback the **sole owner** of the `GCHandle` free
  (`FaultTaskOnly` faults the `Task` only), matching the in-repo Python +
  confluent-kafka-dotnet callback-frees / teardown-drains-not-reclaims pattern. Accepted
  case-A residual: a one-time teardown-only leak if destroy cancels the op before its
  callback is queued (both siblings accept the same); `DisposeAsync` drains and has no
  leak. New OCS component tests drive the straggler callback through the real
  `GCHandle.FromIntPtr` recovery path (proving no UAF). See COMMENTS.DONE.5.
- **Finding 2 [LOW/latent] — ACCEPTED (documented, no code change).**
  Cross-thread `Wakeup()`/`GroupId()` TOCTOU vs teardown; plan-consistent
  (per-call AddRef deliberately declined) and not reachable while internal-only.
  **M3/P2 re-contextualized this as an accepted-by-design residual of the
  single-owner model** (no longer a pending "N=6" fix; future hardening is N≥8,
  since M3/P3 took N=7) — see "STATUS reconciliation (M3/P2)" below.

## Review outcome (M2/P2)

Critic (N=4) review of commits `3359b70`, `6aa2f92` (via `git log`/`git show`):
**0 genuine findings** — clean. Verified the hardening contract exactly: the three
owned-handle constructors return their `SafeHandle` subtype directly (atomic
marshaller create-and-set — the `new + SetHandle` / `FromRaw` two-step is gone,
repo-wide sweep confirms no stale raw-`IntPtr`-return call site); both SafeHandle
subtypes retain the private parameterless ctor (no `MissingMethodException`);
`ownsHandle:true` + `IsInvalid => Zero` + both `ReleaseHandle` bodies unchanged;
the fallible path disposes the IsInvalid handle (ReleaseHandle skipped — no
spurious `Consumer_destroy`) then throws `FromHandle(outError)`; D6 + the graceful
`Dispose` preserved; the new failure-path regression is sound (drives the real
null-native-return 50× and asserts IsInvalid + non-null `out_error` + the
`FromHandle` round-trip). The Critic **independently re-verified** the DoD on this
machine (read-only): `dotnet build` 0/0 across all TFMs, `dotnet test -f net10.0`
20 passed/0 failed. No fix cycle required (one Actor pass → one Critic pass →
close).

## Review outcome (M2/P1)

Critic (N=3) review of commits `558de6a`..`c45f914` (via `git log`/`git show`,
not `cargo xtask await-commit`): **0 genuine findings** — clean. Verified the
full boundary: every `[DllImport]` matches the header (Cdecl, `int64_t`→`long`,
`[MarshalAs(I1)]` on the bool getters, hand-marshalled UTF-8, `out IntPtr` for
`KafkaError_t**`); `FromHandle` (null=success, message-before-free, copy-out,
`_destroy` in `finally`, freed exactly once); `SafeHandle` lifecycle
(`IsInvalid => Zero`, graceful `close_with_timeout` → release, props as the
SafeHandle D6, D5 group-metadata handle read-then-destroy once); preconditions →
`Argument*`/`ObjectDisposedException` (never `KafkaException`); `KafkaException`
the only new public type; D2/D3/D5/D6 recorded; no persona/agent-memory files
committed. No fix cycle required (one Actor pass → one Critic pass → close, as
M1/P1).

## Post-plan additions (M0/P0, interactive — 2026-07-20)

Made after the Critic (N=1) close, in an interactive review pass — these are
NOT part of the approved plan and were NOT put through a separate Critic cycle:
- **Solution items** — a `build` solution folder embedding
  `Directory.Build.props` + `.editorconfig` (ckd parity).
- **`GenerateDocumentationFile=true`** — added to the *library* csproj (NOT the
  shared props, which would make CS1591 break the test build under
  `TreatWarningsAsErrors`). Under TWAE this forces every public API member to
  carry an XML doc — faithful to CLAUDE.md §4 (javadoc → C# XML docs). This
  reverses the initial "dropped" deviation, better-scoped.
- **Strong-naming** — one shared key `Confluent.Kafka.snk` (the file was renamed
  in M0/P1; the key itself is byte-identical and was never regenerated),
  `SignAssembly` wired in `Directory.Build.props` (both projects), and the
  `InternalsVisibleTo` public key on the library csproj. Decided early on
  purpose: adding a strong name after the first published package is a
  binary-breaking change. The `.snk` is committed (identity, not a secret;
  publisher trust is NuGet/Authenticode signing at publish).
- **Review record renamed** — `COMMENTS.1.closed.md` → `COMMENTS.DONE.1.md`
  (the old name was swallowed by the repo-root `COMMENTS\.[0-9]*\.md` gitignore;
  the `DONE` name is tracked and matches the documented mechanics).

## Review outcome (earlier phases)

- **M1/P1** — Critic (N=2) review of commits `9441a4c`, `149e327`, `9423fa4`:
  **0 genuine findings** — clean, independently build/test/format-verified. No fix
  cycle required (one Actor pass → one Critic pass → close).
- **M0/P1** — Critic (N=1): 3 review cycles, 4 items, **all closed**
  (`COMMENTS.1.md` empty). The substantive one was the `IsPackable` default
  inversion recorded above; items 2–4 were STATUS.md documentation-accuracy
  defects.
- **M0/P0** — Critic (N=1) review of commits `f1fb7fc`, `f93a4ef`, `c910fca`:
  **0 genuine findings** — clean skeleton, verified by an independent
  build/test/format run.

## Governance pointers

Current phase (**M4/P4a** — public consumer client):

- Approved plan: `design/history/M4/P4a-public-consumer/PLAN.md` (current). Prior:
  `design/history/M3/P3-poll-receive-path/PLAN.md`,
  `design/history/M3/P2-single-owner-alignment/PLAN.md`,
  `design/history/M3/P1-completion-bridge/PLAN.md`,
  `design/history/M2/P2-safehandle-return-hardening/PLAN.md`,
  `design/history/M2/P1-error-model-safehandle/PLAN.md`,
  `design/history/M1/P1-interop-scaffolding/PLAN.md`.
- Closed review records:
  `design/history/M3/P3-poll-receive-path/COMMENTS.DONE.7.md`,
  `design/history/M3/P1-completion-bridge/COMMENTS.DONE.5.md`,
  `design/history/M2/P2-safehandle-return-hardening/COMMENTS.DONE.4.md`,
  `design/history/M2/P1-error-model-safehandle/COMMENTS.DONE.3.md`,
  `design/history/M1/P1-interop-scaffolding/COMMENTS.DONE.2.md` (M3/P2's closed record
  is archived alongside its plan). M4/P4a: no Critic review yet — the archived record
  will be `design/history/M4/P4a-public-consumer/COMMENTS.DONE.8.md`.
- Personas: `dotnet-actor` (Actor **N=8**), `dotnet-critic` (Critic **N=8**). NEVER
  the Rust `actor-executor` / `kafka-critic`. The working `COMMENTS.8.md` is gitignored;
  the execution record `COMMENTS.DONE.8.md` is tracked but never `git add`ed into a code
  commit. Lands on `prashah_dev_public_consumer_scaffolding` (a NEW PR stacked on the
  M3/P3 PR) — additive commits, no history rewrite.
- **N-counter reconciliation:** M4/P4a **takes N=8**. The STATUS / `NativeConsumer`
  cross-thread **hardening** items previously labeled "N≥8, whenever a public client
  makes `Wakeup()` genuinely cross-thread" (the `Wakeup()`/`GroupMetadata()` handle
  TOCTOU vs teardown; the submit-vs-`destroy` handle race) are now **reachable in
  principle** — P4a is that public client. Per locked decision 5 = option (a) they stay
  **accepted-by-design, documented** on the public client (not scheduled); a future
  per-call `DangerousAddRef` hardening (and/or the D8.8 dispatcher-join) is renumbered
  **N=9** (candidate follow-up, unscheduled). No dangling "N≥8" label remains.

Previous phase (**M0/P1** — rename identity):

- Approved plan: `design/history/M0/P1-rename-identity/PLAN.md`.
- Closed review record: `design/history/M0/P1-rename-identity/COMMENTS.DONE.1.md`.

Previous phase (**M0/P0** — scaffolding):

- Approved plan: `design/history/M0/P0-scaffolding/PLAN.md`. Carries a dated
  supersession note: the phase shipped under the
  `Confluent.Kafka.ShareConsumer` identity, and its body is preserved verbatim
  as the record of what was approved and verified at the time.
- Closed review record: `design/history/M0/P0-scaffolding/COMMENTS.DONE.1.md` —
  left **verbatim** on purpose. It records the Critic's *verified* finding about
  the then-current `InternalsVisibleTo` name, so editing it would make a
  historical verification claim describe an assembly name that did not exist
  when the check ran.

## Next up (not started)

The **additive consumer op families** that grow the P4a public surface without
changing its shape (still Mode A unless a new ABI function is needed). Candidates, each
its own later phase:
- **Commit family** (`CommitSync` / `CommitAsync`) — needs a naming decision (the ABI
  `Consumer_commit_async` is Java's fire-and-forget *sync* `commitAsync`; the *push*
  variant of `commitSync` is `Consumer_commit_sync_async`).
- **`position`** — the scalar-callback (Category B) completion shape, not yet proven.
- **`Assignment` / `Subscription` / `Paused`** — owned-list sync marshalling
  (`TopicPartitionList_t` / `StringList_t`).
- **Owned-handle query siblings** — `committed` / `offsetsForTimes` /
  `beginning|endOffsets` / `partitionsFor` / `listTopics` (each a new result container).
- **`subscribe(pattern)` / `assign` (public) / `pause` / `resume` /
  `seekToBeginning`/`seekToEnd`**, `ConsumerRebalanceListener` /
  `OffsetCommitCallback`, serializers + generic `IConsumer<TKey,TValue>`, typed
  `KafkaException` subclasses, `CloseAsync(TimeSpan)` (needs a Rust-core
  `close_async_with_timeout` — Mode B).

Candidate N=9 hardening (unscheduled): the per-call `SafeHandle.DangerousAddRef` /
`DangerousRelease` around `Wakeup()` / `GroupMetadata()` now that `Wakeup()` is a public
cross-thread API; and/or a Rust-core dispatcher-join on `Consumer_destroy` (would also
close the parallel-test host-crash residual — D8.8 — and let the suite re-enable
parallelization). Sequence and exact scope to be set in the next PLAN (Manager, with
approval).

### Deferred hardening (N=5) — teardown thread-safety: **DONE (M3/P1, 2026-07-27)**

Delivered this phase (see "Decisions in force (M3/P1)"): the non-atomic `_disposed`
bool is replaced by a thread-safe closed flag (`Interlocked` + `TryBeginClose`) plus
the §B5 access guard, so double / concurrent / mixed `Dispose`/`DisposeAsync` are
safe and use-after-dispose throws. Guarded the CKD way (thread-safe closed check +
access guard) — NO per-call `SafeHandle` AddRef, NO close/destroy-as-SafeHandle
param, matching the deferred note's guidance. `DisposeAsync` is the primary
drain-first path (drain in-flight → `close_async` → destroy); `Dispose` stays the
M2-shape blocking fallback — **now with a post-destroy Task-fault** (Critic N=5
Findings 1 + 3, see D4) so an op-in-flight sync `Dispose` faults the op `Task` (never
freeing the `GCHandle` — the completion callback is the sole owner) instead of
stranding, with an accepted one-time case-A teardown residual (`DisposeAsync` has no
leak). Ops remain single-threaded-with-rejection; only `wakeup()` is cross-thread.

### STATUS reconciliation (M3/P2) — the two pre-labeled "N=6 deferred" items resolved

M3/P2 **takes review counter N=6**, so the two items `STATUS.md` previously
pre-labeled "N=6 deferred" for a *future* review collided with this phase. They are
now resolved (no dangling or contradictory "N=6 deferred" label remains — the only
N=6 is M3/P2 itself):

- **Item 1 — `Wakeup()`/`GroupId()` handle TOCTOU vs teardown (Critic N=5 Finding
  2): re-contextualized as an accepted-by-design residual of the single-owner
  model.** It is now one of the three enumerated accepted residuals (see the
  `NativeConsumer` class doc): under the not-thread-safe contract the closed-flag
  check and the `DangerousGetHandle()` deref are deliberately not atomic, so a
  concurrent teardown between them is a use-after-free reachable only under
  cross-thread misuse. Python has the same, more exposed (its `wakeup` has no closed
  check at all). It is **not** a pending N=6 fix. If a *future* hardening is ever
  wanted (per-call `SafeHandle.DangerousAddRef`/`DangerousRelease` around the native
  call, or a documented no-concurrent-teardown precondition), it renumbers to **N≥8**
  (M3/P3 took N=7) — whenever the public client makes `Wakeup()` genuinely cross-thread.
- **Item 2 — op-submit vs concurrent teardown window: ELIMINATED by M3/P2.** The
  window existed because `SubmitVoidOperation` published `_inFlightContext` /
  `_inFlightOperation` *after* the native `submit(...)`, leaving a gap where a
  concurrent teardown saw `null` and could neither drain nor fault the op. M3/P2
  removes those tracking fields entirely, so **there is nothing to publish and no
  window** — the race is structurally removed, not deferred. Any residual
  submit-vs-`destroy` *handle* race folds into the third accepted-by-design residual
  (`DangerousGetHandle()` in `SubmitVoidOperation` vs a concurrent `Consumer_destroy`,
  cross-thread misuse only); any future hardening is **N≥8** (M3/P3 took N=7).

**Accepted residuals (M3/P2, enumerated in the `NativeConsumer` class doc).** All
three are explicitly accepted, misuse-only, not-reachable-while-internal (Python
parity) under the single-owner not-thread-safe contract:
1. teardown-with-unawaited-in-flight-op → strand + one-time `GCHandle`/context leak
   (the Finding-1 `FaultTaskOnly` machinery is intentionally NOT re-added);
   `DisposeAsync` on the awaiting task is the clean, leak-free path;
2. `Wakeup()` / `GroupId()` handle TOCTOU vs teardown → UAF under cross-thread misuse
   (Item 1 above);
3. submit-vs-`destroy` handle race → UAF under cross-thread misuse (Item 2's residual
   handle race).
