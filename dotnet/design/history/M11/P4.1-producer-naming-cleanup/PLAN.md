# M11/P4.1 — Producer naming + shape cleanup (PLAN, N=32)

Small, user-approved follow-up after the producer roadmap A→B→C→D
(M11/P1–P4) closed. Two bundled changes, both **Mode A** (managed C# only,
no ABI), non-broker, low blast radius. No re-approval gate — the scope was
approved up front; this file is the archived forward-looking plan, its
companion `COMMENTS.DONE.32.md` is the execution record.

- **Branch:** `prashah_dev_producer_sync` (stacks on `prashah_dev_producer_send`; base `ac824c3c`, the M11/P4 close-out).
- **Agent number:** N=32 (next free after M11/P4's N=31).

## Change 1 — `NativeProducer` sync/async method rename (internal-only, non-breaking)

Goal: align the internal sync family with the bare-name convention (the
consumer precedent — `Subscribe`/`Assign`/`Position` are bare sync,
`*WithCallback` async), and name the async send by its real mechanism.

In `src/Confluent.Kafka/Internal/NativeProducer.cs`:

| Before | After | Kind |
|---|---|---|
| `SendSync(ProducerRecord) → RecordMetadata` | `Send(ProducerRecord) → RecordMetadata` | sync blocking |
| `FlushSync()` | `Flush()` | sync |
| `PartitionsForSync(string)` | `PartitionsFor(string)` | sync |
| `CloseSync()` | `Close()` | sync teardown leg |
| `Send(ProducerRecord, CancellationToken) → Task<RecordMetadata>` | `SendViaPump(ProducerRecord, CancellationToken)` | async pull-pump |

Async peripherals `FlushWithCallback` / `PartitionsForWithCallback` /
`CloseWithCallback` and all `Mock*` helpers / `Dispose` / `DisposeAsync` are
left unchanged.

**Why `SendViaPump`, not `SendWithCallback`:** the async send is the **pull-pump**
path (enqueue `(future, tcs)` to `SendCompletionPump`, drain `get_all`) — it is
NOT a `Producer_send_async` callback, so `SendWithCallback` would misdescribe the
mechanism.

Call sites updated: sync forwarders (`KafkaProducer.cs` / `MockProducer.cs`)
→ bare `Send/Flush/PartitionsFor/Close`; async forwarders
(`AsyncKafkaProducer.cs` / `AsyncMockProducer.cs`) `_native.Send(...)` →
`_native.SendViaPump(...)`; all `<see cref>` / `<c>` xmldoc + prose refs to the
renamed producer methods (including in `NativeMethods.cs` and the sync producer
test files). The consumer's identically-named `CloseSync` / `CloseSyncWithTimeout`
(`NativeConsumer.cs`, `KafkaConsumer.cs`, `MockConsumer.cs`) are deliberately
untouched.

**Correctness invariant:** exactly ONE `Send` in `NativeProducer` after the
rename (the sync one). No async caller may still bind `.Send(...)` expecting a
`Task` — both async forwarders call `SendViaPump`. The names being distinct (and
arities differing) makes a silent overload-resolution flip a compile error.

## Change 2 — `MockProducer.HistoryCount` property → method (public mock API)

Rationale: aligns Python `history_count()` (a method — `bindings/python/producer.py`)
AND the binding's own FDG precedent — `Assignment()` / `Subscription()` /
`Paused()` / `GroupId()` / `Position()` were made **methods** because each does a
P/Invoke and can throw. `HistoryCount` P/Invokes `MockProducerHistoryCount` and
throws `ObjectDisposedException`, so it should be a method too. Fine to do now
(pre-publish); low blast radius (mock-only, inherent).

- `MockProducer.cs` / `AsyncMockProducer.cs`: `public int HistoryCount => …` →
  `public int HistoryCount() => …`. Internal `NativeProducer.MockHistoryCount()`
  is already a method — unchanged.
- All test call sites `producer.HistoryCount` → `producer.HistoryCount()`
  (including the `Assert.Throws<ObjectDisposedException>(() => producer.HistoryCount())`
  cases) in `PublicProducerMockControlTests` / `PublicSyncProducerMockControlTests`
  / `PublicProducerSendTfmSmokeTests` / `PublicSyncProducerTfmSmokeTests`; doc
  crefs fixed.
- `CLAUDE.md §3`: sketch `public int HistoryCount { get; }` →
  `public int HistoryCount();`, note updated to record it as a **method** per the
  FDG precedent + Python parity (a deliberate decision-record update, not drift).

## Definition of Done

`cargo build --features ffi` → `dotnet build` (0W/0E on the TFM matrix) →
`dotnet test -f net10.0` green (net8.0/net462 build-verified, run CI-only) →
`dotnet format --verify-no-changes` clean. Mode-A `git diff` clean over
`src/**` / `src/ffi/**` / `confluent_kafka.h` / `cbindgen.toml`.
