# M4/P4b — Async-surface rename (`IAsyncConsumer`, drop `Async` suffix, `WithCallback`)

**Status:** DRAFT — awaiting human approval before implementation. Do NOT spawn
Actor/Critic or write code until approved. Decisions/deviations made *during*
execution go in `COMMENTS.DONE.9.md`.
**Mode:** A — pure C# rename; **no ABI change**, no Rust authored, no new
`DllImport`, **no behavior change**.
**Review counter:** N=9 (M0/P0=1 … M4/P4a=8, **M4/P4b=9**).
**Personas:** `dotnet-actor` (Actor N=9) implements; `dotnet-critic` (Critic N=9)
reviews. NEVER the Rust `actor-executor` / `kafka-critic`.
**Branch / PR:** land on **`prashah_dev_public_consumer_scaffolding`** (same as
M4/P4a); additive commits on the existing stacked PR — this is a naming reshape of
M4/P4a's surface and ships with it.
**Builds on:** M4/P4a (public consumer client). This phase renames M4/P4a's async
surface into its final shape; it adds no ops and no sync surface.

---

## Why (motivation)

We are committing to a **two-interface** consumer direction:

- **`IConsumer`** — a **sync** surface (blocking-in-Java → blocking C#) — *later
  milestone (M5)*.
- **`IAsyncConsumer`** — the **async** surface (blocking-in-Java → `Task`) — what
  M4/P4a shipped.

Method names carry **no `Async` suffix** on the public surface; the sync-vs-async
distinction is carried by the interface/class, matching Java's method names and the
Python sibling (which ships exactly this two-surface model with un-suffixed names —
`Consumer`/`AsyncConsumer`, both with `poll`, `subscribe`, …). `bindings/CLAUDE.md
§2.2` ("mirror Java method names, adapting only casing") supports dropping the suffix.

M4/P4a shipped the async surface as `IConsumer` with `PollAsync`-style names. **This
phase renames that async surface into its final form — `IAsyncConsumer` with
un-suffixed names — before the async op set grows** (every op added under the old
naming is one more method to rename later) and **before publish** (the package gate
is still open — `IsPackable=false` — so this is internal churn only). It **reserves**
`IConsumer` / `KafkaConsumer` for the future sync surface, so nothing breaks when
that lands.

**Why now, not later:** cheapest window — the surface is small (5 async ops) and
unpublished. Deferring means renaming a larger surface post-hoc.

### In scope
- Rename the public async interface + classes; drop the `Async` suffix on public
  methods.
- Add `IConsumerCommon` (shared non-blocking surface) to reserve the split shape.
- Rename the internal `NativeConsumer` bridge methods `…Async` → `…WithCallback`.
- Update tests, public XML docs, and the **current** `STATUS.md` to the new names.

### Out of scope (do NOT build)
- The **sync** surface: `IConsumer` (sync), sync `KafkaConsumer`/`MockConsumer`, the
  sync `Consumer_*` DllImports, sync `NativeConsumer` methods → **later milestone M5**.
- Any behavior change, any new op, any ABI change.
- `Assignment`/`Subscription`/`Paused` and the other deferred ops (still deferred).

---

## Rename map

### Public (`src/Confluent.Kafka/`)

| Before | After |
|---|---|
| `interface IConsumer` | `interface IAsyncConsumer : IConsumerCommon, IAsyncDisposable, IDisposable` |
| — (new) | `interface IConsumerCommon { void Wakeup(); ConsumerGroupMetadata GroupMetadata(); }` |
| `class KafkaConsumer : IConsumer` | `class AsyncKafkaConsumer : IAsyncConsumer` |
| `class MockConsumer : IConsumer` | `class AsyncMockConsumer : IAsyncConsumer` |
| `Task<ConsumerRecords> PollAsync(…)` | `Task<ConsumerRecords> Poll(…)` |
| `Task SubscribeAsync(…)` | `Task Subscribe(…)` |
| `Task UnsubscribeAsync(…)` | `Task Unsubscribe(…)` |
| `Task SeekAsync(…)` | `Task Seek(…)` |
| `Task CloseAsync(…)` | `Task Close(…)` |
| `void Wakeup()`, `ConsumerGroupMetadata GroupMetadata()` | moved onto `IConsumerCommon` (same signatures) |
| `Dispose()`, `DisposeAsync()` | **unchanged** (framework contract) |

File renames: `IConsumer.cs`→`IAsyncConsumer.cs` (+ new `IConsumerCommon.cs`),
`KafkaConsumer.cs`→`AsyncKafkaConsumer.cs`, `MockConsumer.cs`→`AsyncMockConsumer.cs`.
`AsyncMockConsumer`'s inherent mock helpers (`AddRecord`/`SetPollError`/`Assign`)
keep their names.

### Internal (`src/Confluent.Kafka/Internal/NativeConsumer.cs`)

| Before | After |
|---|---|
| `PollAsync(…)` | `PollWithCallback(…)` |
| `SubscribeAsync(…)` | `SubscribeWithCallback(…)` |
| `UnsubscribeAsync(…)` | `UnsubscribeWithCallback(…)` |
| `SeekAsync(…)` | `SeekWithCallback(…)` |
| `CloseAsync(…)` (public-surfacing close) | `CloseWithCallback(…)` |
| `CloseAsyncInternal()` (bridge helper) | `CloseWithCallbackInternal()` |
| `Dispose()`, `DisposeAsync()`, `SubmitOperation<T>`, `SubmitVoidOperation`, `Wakeup`, `GroupMetadata`, `GroupId` | **unchanged** |

*(Actor confirms the exact current internal names against the file; the map above is
the intended target.)*

---

## What stays unchanged (do NOT touch)

- **`NativeMethods` P/Invoke declarations** — the `EntryPoint` strings are fixed C
  ABI names (`kafka_consumer_Consumer_poll_async`, …) and the extern method names
  (`ConsumerPollAsync`, …) **mirror the ABI's own `_async` suffix**, not the .NET
  async convention. Leave them. (The `Async` there reflects the C ABI, not our public
  naming.)
- **`ConsumerCallbacks`** trampolines (`Poll`/`OnPoll`, `Operation`/`OnOperation`,
  the delegate types) — internal interop, not public method names. Unchanged.
- **Marshallers** (`ConsumerRecordsMarshal`, `ConsumerGroupMetadataMarshal`,
  `Utf8Marshal`), **value types** (`ConsumerRecord(s)`, `Header(s)`, `TimestampType`,
  `TopicPartition`, `ConsumerGroupMetadata`), `OperationCompletionSource`, the
  `SafeHandle`s, `KafkaException` — unchanged.
- **All behavior** — single-owner model, on-dispatcher copy-out, teardown, the 5
  invariants, `Close()` surfaces the error, `Seek` async + negative-offset
  precondition, serial-test setting (D8.8). **Only names change.**

---

## Public API surface after rename

```csharp
namespace Confluent.Kafka;

public interface IConsumerCommon
{
    void Wakeup();
    ConsumerGroupMetadata GroupMetadata();
}

public interface IAsyncConsumer : IConsumerCommon, IAsyncDisposable, IDisposable
{
    Task<ConsumerRecords> Poll(TimeSpan timeout, CancellationToken cancellationToken = default);
    Task Subscribe(IReadOnlyCollection<string> topics, CancellationToken cancellationToken = default);
    Task Unsubscribe(CancellationToken cancellationToken = default);
    Task Seek(TopicPartition partition, long offset, CancellationToken cancellationToken = default);
    Task Close(CancellationToken cancellationToken = default);
}

public sealed class AsyncKafkaConsumer : IAsyncConsumer
{
    public AsyncKafkaConsumer(IReadOnlyDictionary<string, string> config);
    // IAsyncConsumer members …
}

public sealed class AsyncMockConsumer : IAsyncConsumer
{
    public AsyncMockConsumer(string? autoOffsetReset = null);
    // IAsyncConsumer members … + inherent mock helpers (AddRecord/SetPollError/Assign)
}
```

Carried rationale docstrings move onto the renamed members: `Seek`-is-async
(blocking `addAndGet`; deliberate divergence from Python's sync seek), `byte[]`
key/value/header, single-owner / not-thread-safe caveat, `Close()` surfaces the
error (unlike `DisposeAsync`), `Wakeup()` cross-thread caveat.

---

## Test plan

- **Rename all references** in `tests/…` (`IConsumer`→`IAsyncConsumer`,
  `KafkaConsumer`→`AsyncKafkaConsumer`, `MockConsumer`→`AsyncMockConsumer`,
  `.PollAsync`→`.Poll`, `.SubscribeAsync`→`.Subscribe`, etc.). **Keep every
  assertion** — no test weakened, no coverage lost.
- Test class/file names may rename for clarity (optional) but must keep their cases.
- **Re-verify:** `cargo build --features ffi` (unchanged); `dotnet build` **0/0**
  across all six TFM legs (CS1591 on the renamed public members); `dotnet test -f
  net10.0` **~20× serial → 0 failures / 0 crashes** (parallelism stays disabled per
  D8.8); `dotnet format --verify-no-changes`.
- No new tests required (pure rename); existing coverage carries.

---

## DoD + governance

- **DoD gates (in order):** `cargo build --features ffi` (Mode A, no ABI change) →
  `dotnet build` 0/0 all TFMs (CS1591; no TODO/FIXME; Apache-2.0 header on the new
  `IConsumerCommon.cs` and any renamed files) → `dotnet test -f net10.0` ~20× →
  `dotnet format` clean. net8/net462 build legs pass locally; runs CI-only.
- **VSTHRD200 confirmed absent** (`Microsoft.VisualStudio.Threading.Analyzers` not
  referenced anywhere; no analyzer enforces the `Async` suffix), so dropping it
  builds clean under `TreatWarningsAsErrors` + `AnalysisLevel=latest`. If a future
  phase adds that package, suppress VSTHRD200 via one `.editorconfig` line.
- **Do NOT retro-edit the archived M4/P4a docs** (`design/history/M4/
  P4a-public-consumer/PLAN.md`, `COMMENTS.DONE.8.md`) — they are a historical record
  referencing `IConsumer`/`PollAsync` as built at that time. Only the **current**
  `design/current/STATUS.md` is updated to the new names.
- **Governance:** N=9; working `COMMENTS.9.md` → resolved `COMMENTS.DONE.9.md`;
  archive plan + closed record under `design/history/M4/P4b-async-surface-rename/`;
  exclude `.claude/agent-memory/**` from every commit; do NOT push (human opens/
  updates the PR). Commits `dotnet(M4/P4b): …` on the existing branch; `--no-gpg-sign`
  is fine (local setting from M4/P4a).

---

## Sub-step ordering for the Actor (one commit per step, each green)

1. **Interface layer:** add `IConsumerCommon.cs`; rename `IConsumer`→`IAsyncConsumer`
   (move `Wakeup`/`GroupMetadata` onto `IConsumerCommon`; drop the suffix on the async
   methods). **Gate:** build 0/0.
2. **Impl classes:** rename `KafkaConsumer`→`AsyncKafkaConsumer`,
   `MockConsumer`→`AsyncMockConsumer`; drop the suffix on their methods; forward to
   the renamed `NativeConsumer` methods. **Gate:** build 0/0.
3. **Internal `NativeConsumer`:** rename the bridge methods `…Async`→`…WithCallback`
   (+ `CloseAsyncInternal`→`CloseWithCallbackInternal`); leave `NativeMethods` /
   `ConsumerCallbacks` / marshallers untouched. **Gate:** build 0/0.
4. **Tests:** rename references, keep assertions; run ~20× serial → 0 failures.
   **Gate:** all DoD gates green.
5. **Docs + governance:** update `STATUS.md` to the new names + N=9; archive plan +
   closed record. **Gate:** docs match code.

---

## Risks / notes

- **Wide but mechanical churn.** Many files reference the renamed symbols; mitigated
  by a clean `dotnet build` — 0/0 with `TreatWarningsAsErrors` catches every stale
  reference.
- **Two easy-to-get-wrong boundaries** (above): (a) do NOT rename the ABI-mirroring
  `NativeMethods` extern names or the C ABI `EntryPoint`s; (b) do NOT retro-edit
  archived M4/P4a docs.
- **Purely nominal.** The Critic should confirm the diff shows only identifier/file
  renames + the new `IConsumerCommon` (no logic changes), and that the ~20× stability
  + 0/0 hold.
- **PR shape:** additive commits on the existing M4/P4a stacked branch; the M4/P4a PR
  description (already drafted) should be updated to the final `IAsyncConsumer` naming
  before it is opened/merged.
