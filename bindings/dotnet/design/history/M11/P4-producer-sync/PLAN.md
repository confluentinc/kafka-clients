# M11/P4 — Sync producer (`IProducer` / `KafkaProducer` / `MockProducer`) — DRAFT PLAN

> **Status: DRAFT — for user review. PLAN-ONLY.** No Actor/Critic spawned, no loop
> run, nothing archived, nothing committed. On approval this becomes the forward
> plan for the M11/P4 execution loop.

---

## 1 · Scope, branch, phase

- **Phase:** **M11/P4** — the **SYNC producer** (the producer roadmap's **"Phase D"**).
- **Branch:** `prashah_dev_producer_sync`, stacked on `prashah_dev_producer_send`
  (already checked out).
- **Mode:** **Mode A only** — C# from the generated header down. No changes to
  `src/**`, `src/ffi/**`, `target/include/confluent_kafka.h`, or `cbindgen.toml`.
  Every sync ABI fn this phase needs is already in the header (§8); the only
  net-new C# interop is a handful of `[DllImport]` declarations for **existing**
  header symbols (Mode A step 1), never new ABI.
- **Agent number (later loop):** **N = 31 — confirmed (user, 2026-08-17), NOT run this session.**
  N=30 was already consumed by M11/P3 (its Critic + archived
  `design/history/M11/P3-producer-send/COMMENTS.DONE.30.md`), so per the PM
  "unique N per requirement" rule the next free number is **N=31** — this phase
  uses it. (Working `COMMENTS.<N>.md` is reset for the new requirement; the P3
  archive is immutable, so N=31 also avoids clobbering it.)

**What the sync producer is.** The blocking mirror of the shipped async producer
(`IAsyncProducer` / `AsyncKafkaProducer` / `AsyncMockProducer`), exactly as the
sync consumer (`IConsumer` / `KafkaConsumer` / `MockConsumer`, M5/P8a) is the
blocking mirror of the async consumer. It is the deferred twin CLAUDE.md §3/§4
already reserves ("A sync `IProducer` … is the **deferred** twin"). Each sync
method calls the **sync C ABI directly**; the core's blocking call parks the
caller thread **inside the Rust core's own multi-thread runtime** (deadlock-free,
ffi §A1) — this is the sync-consumer precedent (`consumer-threading.md §1.1`), and
is **NOT** the forbidden managed sync-over-async (§4 / ffi §A7). Reuses the
existing `NativeProducer` lifecycle wrapper — the sync client types are **siblings**
of the async trio over one `NativeProducer`, not wrappers of it.

**Surface (bytes-only interim, typed generic producer still deferred):**
`Send`, `Flush`, `PartitionsFor`, `Close`, plus the `MockProducer` send-control
helpers `CompleteNext` / `ErrorNext` / `HistoryCount` / `Clear`.

---

## 2 · Parity anchor (mandatory guardrail — the M11/P2 retro rule)

Review every public member of this phase against this anchor. Anything outside it
is a finding.

### (a) The Python sync-producer surface — the public-surface ceiling

**Python DOES ship a separate sync producer** (`bindings/python/producer.py`):
`class Producer(_ProducerBase)` (sync) alongside `class AsyncProducer` (async),
with `KafkaProducer` / `MockProducer` over the sync one and
`AsyncKafkaProducer` / `AsyncMockProducer` over the async one. The sync members:

| Python sync `Producer` | Blocking? | Returns |
|---|---|---|
| `send(record)` | **No** — non-blocking | `concurrent.futures.Future[RecordMetadata]` |
| `flush()` | Yes (`_run_sync` on a `threading.Event`) | `None` |
| `partitions_for(topic)` | Yes (`_run_sync`) | `list[PartitionInfo]` |
| `close()` | Yes (`_run_sync`) | `None` |
| `complete_next` / `error_next` / `history_count` / `clear` | — (mock mixin) | mock control |

**Key divergence to record up front:** Python's sync `send()` returns a
**Future**, not a blocked `RecordMetadata`. Python can do this because it has **two
distinct future types** (`concurrent.futures.Future` for the sync/thread world,
`asyncio.Future` for the loop) — so its sync producer keeps Java's `Future`-shaped
`send` while still being a distinct "sync" class. **.NET has only one future type
(`Task<T>`)**, so the .NET sync producer cannot mirror that shape without becoming
byte-identical to the async producer (§3, decision #1). The .NET sync `IProducer`
is therefore a **binding decision** — mirroring **Java's `Producer` shape** + the
**.NET sync-`IConsumer` "blocking, returns the result directly" precedent** — the
same way the sync consumer facade was a .NET-only amendment
(`consumer-threading.md §1.1`). Python is the ceiling for *which members exist*, not
for the `send` return shape.

### (b) Precedents to mirror

- **Sync `IConsumer` / `KafkaConsumer` / `MockConsumer`** (M5/P8a) — the blocking
  mirror over `NativeConsumer`: blocking-in-Java members return the result
  directly (or `void`), sync C ABI called directly (`block_on` in the core's
  runtime), no `CancellationToken`, `Wakeup()`-only interruption, bare type names
  (`KafkaConsumer`, not `AsyncKafkaConsumer`), `: IDisposable` only.
- **Async producer trio** (`IAsyncProducer` / `AsyncKafkaProducer` /
  `AsyncMockProducer`, M11/P1–P3) — the `NativeProducer` wrapper, the
  `ProducerRecord` / `RecordMetadata` / `PartitionInfo` value types,
  `RecordMetadataMarshal.CopyOut`, `PartitionInfoListMarshal`, the mock
  send-control helpers, the zero-copy send pinning (ffi §A4).

### (c) Explicit NOT-adding list (any of these appearing is a Critic finding)

- Transactions, `Metrics`, `clientInstanceId`, the **typed generic** producer,
  and **headers** — Mode B / later, out of scope.
- The **consumer sync-op → `SafeHandle`-param migration** — a *separate* tracked
  follow-up (P3 close-record part 5), explicitly **out of scope** here. (The *new*
  sync producer ops added here still adopt the convention — §4 decision #3 — but
  this plan touches **no** consumer code.)
- `Close(TimeSpan)` on the sync producer — dropped (§4 decision #5; no
  `Producer_close_with_timeout` ABI, Python-producer parity). Its appearance is a
  finding unless decision #5 is overturned at approval.
- Any `Task` / `ValueTask` / `IAsyncDisposable` / `CancellationToken` on the sync
  surface (that is the async producer's shape).
- A second send-completion pump, TCS, `GCHandle`, or completion callback on the
  sync path (§3 decision #2 — the sync `Send` blocks on its own `get`).

**Critic's added charge (this phase):** flag any public member not in the
Java `Producer` / Python-sibling shape; any divergence from the sync-consumer
precedent without a written rationale; any dead/unused `DllImport`; and any
`Task`/callback/pump machinery leaking onto the sync path.

---

## 3 · The central decision: the sync-`Send` shape + the sync completion mechanism

### Decision #1 — sync `Send(record)` **blocks and returns `RecordMetadata` directly** (= Java `send(record).get()`)

**Options considered**

- **(A) `RecordMetadata Send(ProducerRecord record)`** — blocks until the cluster
  acknowledges, returns the metadata directly (= `producer.send(record).get()`).
- **(B) `Task<RecordMetadata> Send(...)`** — return a Task (Java's `Future` shape).
- **(C)** a bespoke sync-future type mirroring Python's `concurrent.futures.Future`.

**Pick: (A).** Rationale:

- **The sync-consumer precedent is (A).** Java's blocking `poll()` → sync
  `Poll()` returns `ConsumerRecords` directly. The "blocking mirror" surface hands
  back the resolved result, never a Task. `send(record).get()` is the blocking
  form of `send`, and its result is `RecordMetadata` — so sync `Send` returns
  `RecordMetadata`.
- **(B) collapses the sync/async split.** .NET's *only* future type is `Task<T>`;
  the async `Send` already returns `Task<RecordMetadata>`. A sync `Send` returning
  `Task<RecordMetadata>` would be **byte-identical** to `IAsyncProducer.Send`,
  leaving no reason for a separate sync surface. The whole `IProducer` /
  `IAsyncProducer` split (CLAUDE.md §4) exists so the *interface/type* carries the
  async distinction; (B) erases it for `send`.
- **(C) has no .NET home.** Python's sync producer returns a
  `concurrent.futures.Future` only because Python has *two* future types; .NET
  does not. Inventing a sync-future type is non-idiomatic and unjustified.
- **Deliberate divergence from Python's sync `send` (recorded).** Python's sync
  `send` returns a Future (pipelining preserved); .NET's blocks (each `Send`
  round-trips). This is **forced** by .NET's single-future-type reality and is
  **consistent** with how the sync consumer already diverged (blocking result, not
  a future). Users who want pipelining use the **async** producer
  (`IAsyncProducer.Send` → `Task`, with the batching pull-pump). Clean story:
  **sync = simple blocking send-and-confirm; async = pipelined `Task`-returning
  send** — exactly Java's `send(rec).get()` vs holding the `Future`.

### Decision #2 — the sync completion mechanism: **blocking `get`, NO pump**

Sync `Send` completes on the **caller's own thread**, with no pump / TCS /
`GCHandle` / callback:

```
Send(record)  [caller thread]
  validate (null record, disposed, ProducerRecord ctor already validated topic/partition)
  pin key/value CALL-SCOPED (ffi §A4, zero-copy — the core copies during the send)
  future = Producer_send(SafeProducerHandle, topic, partition, timestamp, key, value, out err)
      └─ err != null → throw KafkaException (sync failure)                    (ffi §A5)
  meta = FutureRecordMetadata_get(future, out err2)   ← BLOCKS (block_on in the core runtime)
      └─ err2 != null → FromHandle → throw KafkaException; else RecordMetadataMarshal.CopyOut(meta)
  RecordMetadata_destroy(meta)                                                (ffi §A2 Cat 2)
  FutureRecordMetadata_destroy(future)   (or destroy_all[1]; get does NOT consume it)
  return RecordMetadata
```

- **Verified in the header (§8):** `FutureRecordMetadata_get(future, out_error)`
  is the **blocking** get — "Blocks until the future resolves and returns the
  record metadata" — exactly what (A) needs.
- **This is the direct-sync-ABI pattern, NOT sync-over-async.** The block happens
  inside the Rust core's own multi-thread runtime (deadlock-free, ffi §A1); there
  is no managed `Task.Run` / `.GetAwaiter().GetResult()` over the async binding
  API. Same shape as every sync-consumer op (`consumer-threading.md §1.1`).
- **The sync producer needs NO pump.** The pump (`SendCompletionPump`, ffi §A7
  Option C) exists only for the **async** `Send`, which returns instantly and needs
  one background thread to batch many futures' `get_all` into TCS completions. The
  sync `Send` caller blocks on its **own** single `get`, so there is nothing to
  batch and no thread to spin. A **sync-only `NativeProducer` never starts the
  pump** (only the async `Send` starts it, lazily), so its teardown degenerates to
  the pump-less path (§4 decision #6).

---

## 4 · Enumerated decisions (#1–#7)

Each: **statement → pick → why.**

**#1 — sync `Send` shape.** → **Blocks, returns `RecordMetadata`** (= `send().get()`).
→ Sync-consumer precedent; (B) would clone the async surface (single `Task` type);
Python's Future shape has no .NET analogue. Full rationale §3.

**#2 — sync completion mechanism.** → **`Producer_send` → blocking
`FutureRecordMetadata_get`, no pump / TCS / callback.** → The caller blocks on its
own `get` (`block_on` in the core runtime, deadlock-free); the pump is an
async-only construct. Full rationale §3.

**#3 — sync `Flush` / `Close` / `PartitionsFor` + the SafeHandle-param convention.**
→ Use the **sync** ABI variants (`Producer_flush`, `Producer_close`,
`Producer_partitions_for`), and apply the **sync→`SafeHandle`-param** convention
(ffi §A2, adopted for `Producer_send` in P3) to the **new, non-teardown** public
sync ops:
  - **`FlushSync` / `PartitionsForSync`** — pass the `SafeProducerHandle` as the
    P/Invoke param so the marshaler auto-`DangerousAddRef`/`Release`s it
    call-scoped around the synchronous native call (exactly right — the native use
    ends when the call returns). `Producer_partitions_for` is a **fresh** DllImport
    that adopts `SafeProducerHandle` from the start; `Producer_flush`'s existing
    DllImport (`IntPtr`, used by teardown) is **retyped** to `SafeProducerHandle`
    (its teardown callers pass `_handle` — the call-scoped ref is safe and strictly
    preferable there too, single-winner latch). → Building the sync producer *right*
    per the P3-established convention; a tiny, self-contained change **inside the
    producer's own code** — explicitly **not** the excluded consumer migration.
  - **`CloseSync` (teardown/release op)** — the SafeHandle-param convention does
    **not** cleanly apply: `Close` is the path that *releases* the handle (win
    latch → sync `Producer_close` → `_handle.Dispose()` → `Producer_destroy`), so
    it keeps the existing single-winner raw-handle teardown shape (mirrors
    `NativeProducer.Dispose` / `NativeConsumer.CloseSync`). `Producer_close`'s
    DllImport stays `IntPtr`.
  - **Decision (confirmed, user 2026-08-17): retype `Producer_flush` to
    `SafeProducerHandle`.** The convention-consistent, principled choice — its
    teardown callers pass `_handle` (the call-scoped ref is safe and strictly
    preferable there too, single-winner latch), and it is reversible. The
    conservative zero-churn alternative (leave `Producer_flush` `IntPtr`, adopt the
    convention only on the new `Producer_partitions_for`) was considered and
    **rejected** — no longer an open flag.

**#4 — cancellation.** → **No `CancellationToken` anywhere on the sync surface.**
→ Mirrors the sync `IConsumer`. The consumer's only sync interruption is
`Wakeup()`; the producer has **no `wakeup()`** at all (confirmed — the async
producer documents "the producer has no `wakeup()`", and the sync consumer's
`Wakeup` has no producer analogue), so there is no interruption primitive to
expose. A blocked sync `Send`/`Flush`/`Close`/`PartitionsFor` runs to native
completion (the single-owner model, like the sync consumer).

**#5 — `Close` overloads.** → **`Close()` only — NO `Close(TimeSpan)`.** → There is
**no `Producer_close_with_timeout` ABI** (header has only `Producer_close` +
`Producer_close_async`; verified §8). The async producer's `Close(TimeSpan)` was
**removed in M11/P2.1** for strict Python-producer parity (Python's producer
`close()` takes no timeout). So the sync producer ships `Close()` only. **This
deliberately diverges from the sync `IConsumer`**, which has both `Close()` and
`Close(TimeSpan)` — but only because the *consumer* ABI exposes
`Consumer_close_with_timeout`; the producer's does not. Recorded divergence, ABI-
and Python-grounded.

**#6 — type / lifecycle relationship.** → **Sibling types over one
`NativeProducer`** (the consumer precedent): `KafkaProducer` (sync) /
`MockProducer` (sync) sit alongside `AsyncKafkaProducer` / `AsyncMockProducer`,
each owning a `NativeProducer` created by its own ctor (no client wraps another).
→ Teardown is the **shared** `NativeProducer` teardown, which already handles the
pump-absent case. **Simpler sync teardown, spelled out:** a sync-only
`NativeProducer` never starts the send pump (only async `Send` does), so
`StopPump()` finds `_pump == null` and returns immediately — **no flush-before-join
dance, no pump thread to join**. The sync `Dispose()` degenerates to
*win-latch → sync `Producer_close` (swallow) → `Producer_destroy`*; the sync
`Close()` to *win-latch → sync `Producer_close` (surface error) → `Producer_destroy`*
(a new `NativeProducer.CloseSync()`, mirroring `NativeConsumer.CloseSync`). The
one-shot `_closed` latch already makes `Send`/`Flush`/`PartitionsFor` throw
`ObjectDisposedException` post-teardown. **Single-owner / not thread-safe**, like
the sync consumer (a blocked sync `Send` + concurrent `Dispose` from another
thread is misuse; the future is Arc-backed and `Producer_destroy` tolerates
outstanding futures — P3 round-2 finding — so it is memory-safe even so).

**#7 — backpressure / close known-limitation carryover.** → **No new handling —
inherits the core's `buffer.memory` inline backpressure (ffi §A7 Option C).** → The
sync `Send`'s inline `Producer_send` blocks up to `max.block.ms` when the core
buffer is full (Java's `send()` blocking-on-`buffer.memory` semantics), then the
blocking `get` waits for the ack — same core mechanism the async path relies on. No
managed bound / hand-cap is added. References the recorded Option-C residual (the
P3 close-record + `SendCompletionPump` remarks): a real producer's pending records
are delivered-or-timed-out by `flush`/`close`; the sync producer adds nothing new.

---

## 5 · Public API sketch (`src/Confluent.Kafka/`)

Bare type names (sync = bare, async = `Async`-prefixed — the consumer precedent).
The names `IProducer` / `KafkaProducer` / `MockProducer` are currently **free**
(the shipped async types are `AsyncKafkaProducer` / `AsyncMockProducer`; the
CLAUDE.md §3 sketch showing `KafkaProducer : IAsyncProducer` predates the async
rename and reserved these bare names for exactly this sync trio).

```csharp
namespace Confluent.Kafka;

/// Java org.apache.kafka.clients.producer.Producer (synchronous) — the blocking
/// mirror of IAsyncProducer; each method calls the sync C ABI directly.
public interface IProducer : IDisposable
{
    // Java send(record).get() — BLOCKS, returns the metadata directly (decision #1).
    RecordMetadata Send(ProducerRecord record);

    // Java flush() — blocks until the core resolves the flush.
    void Flush();

    // Java partitionsFor(String) — blocks, returns the owned list directly
    // (empty list on a MockProducer — the honest reachability caveat, as async).
    IReadOnlyList<PartitionInfo> PartitionsFor(string topic);

    // Java close() — graceful close, SURFACES a close failure. No Close(TimeSpan)
    // (decision #5: no Producer_close_with_timeout ABI; Python-producer parity).
    void Close();
}

public sealed class KafkaProducer : IProducer      // Java KafkaProducer (synchronous)
{
    public KafkaProducer(IReadOnlyDictionary<string, string> config);
}

public sealed class MockProducer : IProducer       // Java MockProducer (synchronous)
{
    public MockProducer(bool autoComplete = true);
    // Mock send-control helpers — inherent on the concrete type, NOT on IProducer
    // (mirrors AsyncMockProducer + the consumer's mock-only helpers).
    public bool CompleteNext();
    public bool ErrorNext(int code, string? message = null);
    public int  HistoryCount { get; }
    public void Clear();
}
```

Notes:
- **No `IProducerCommon`.** Unlike the consumer (which needs `IConsumerCommon` for
  its ~8 genuinely-non-blocking members), the producer has **no** non-blocking
  members in scope — `Send`/`Flush`/`PartitionsFor` all block, `Close` is teardown.
  So `IProducer` is flat, `: IDisposable` only (no shared base, no
  `IAsyncDisposable`).
- **`HistoryCount` is a property** (mirrors `AsyncMockProducer.HistoryCount`; the
  ABI exposes only a count, so Java's `history()` record list is not surfaced —
  CLAUDE.md §3).
- **Preconditions (ffi §A5)** identical to the async producer: null `record`/`topic`
  → `ArgumentNullException`; post-`Close`/`Dispose` op → `ObjectDisposedException`;
  operational failures → `KafkaException` (code/retriable/fatal/message).
  `ProducerRecord`'s own ctor already validates topic/partition, so a constructed
  record is valid at `Send`.

---

## 6 · Internal design (`Internal/` + `Internal/Interop/`)

### 6.1 `NativeProducer` — new sync methods (all reuse the existing wrapper)

- **`RecordMetadata SendSync(ProducerRecord record)`** — the blocking-get worker
  (§3): preconditions → `ProducerSendMarshal.Send(_handle, …)` (existing;
  `SafeProducerHandle`-param, call-scoped pin, `out_error`) → blocking
  `FutureRecordMetadata_get(future, out err)` → on error `FromHandle`/throw, else
  `RecordMetadataMarshal.CopyOut(meta)` → `RecordMetadata_destroy(meta)` →
  destroy the future → return. Frees every handle on every path (ffi §A2). **Does
  not touch the pump.**
- **`void FlushSync()`** — `Producer_flush(SafeProducerHandle, out err)` → surface
  the error via `FromHandle` (unlike teardown's swallow).
- **`IReadOnlyList<PartitionInfo> PartitionsForSync(string topic)`** — null-topic
  guard → call-scoped topic pin → `Producer_partitions_for(SafeProducerHandle,
  topicPtr, out list)` → non-null error → `FromHandle`/throw; else
  `PartitionInfoListMarshal` copy-out (existing) → `PartitionInfoList_destroy`.
- **`void CloseSync()`** — win the `_closed` latch → `StopPump()` (no-op for a
  sync-only producer) → `Producer_close(rawHandle, out err)` **surfacing** the
  error → `_handle.Dispose()` (→ `Producer_destroy`) in a `finally`. Mirrors
  `NativeConsumer.CloseSync`; the existing `Dispose()` (swallows) is unchanged and
  reused for the sync `MockProducer`/`KafkaProducer.Dispose()`.

The public sync `KafkaProducer` / `MockProducer` are **thin forwarders** to these
(`Send => _native.SendSync(record)`, `Flush => _native.FlushSync()`,
`Close() => _native.CloseSync()`, `Dispose() => _native.Dispose()`, mock helpers →
the existing `MockCompleteNext`/`MockErrorNext`/`MockHistoryCount`/`MockClear`).

### 6.2 `NativeMethods` — DllImport additions (Mode A: existing header symbols)

| Symbol | Status | Signature (proposed) |
|---|---|---|
| `kafka_producer_FutureRecordMetadata_get` | **NEW** | `IntPtr FutureRecordMetadataGet(IntPtr future, out IntPtr outError)` |
| `kafka_producer_Producer_partitions_for` | **NEW** | `IntPtr ProducerPartitionsFor(SafeProducerHandle producer, IntPtr topic, out IntPtr outList)` (returns the error handle) |
| `kafka_producer_FutureRecordMetadata_destroy` | **NEW (optional)** | `void FutureRecordMetadataDestroy(IntPtr future)` — singular; or reuse the existing `FutureRecordMetadataDestroyAll(new[]{future}, 1)` (the P3 `Send` precedent) |
| `kafka_producer_Producer_flush` | **retype** | `IntPtr → SafeProducerHandle producer` (decision #3) |

**Already declared, reused as-is:** `ProducerSend` (`SafeProducerHandle`-param),
`RecordMetadataOffset`/`Partition`/`Topic`/`Timestamp`/`Destroy`,
`Producer_close` (`IntPtr`, teardown), `MockProducer_complete_next`/`error_next`/
`history_count`/`clear`, `KafkaProducerNew`/`MockProducerNew`/`ProducerDestroy`/
`ProducerProperties_*`, `PartitionInfoList*` accessors (consumer-shared),
`RecordMetadataMarshal.CopyOut`, `PartitionInfoListMarshal`, `Utf8Marshal`.

So the interop delta is **2 required new DllImports** (`FutureRecordMetadata_get`,
`Producer_partitions_for`), **1 optional** (singular `FutureRecordMetadata_destroy`),
and **1 retype** (`Producer_flush` → SafeHandle-param). **All are Mode A** — every
symbol already exists in the checked-in header; **no** `src/ffi` / header /
`cbindgen` change. No `SafeHandle` subclass is new (the producer/properties handles
already exist; futures/metadata/errors are flat transients, ffi §A2 Cat 2).

### 6.3 No new scaffolding types

No new pump, no new completion source, no new marshal helper — the sync path is
strictly leaner than the async path (it drops the TCS, the `GCHandle`, the
cancellation registration, and the pump). This is the smallest surface that
restores the Java sync `Producer` shape.

---

## 7 · Tests + Definition of Done

New test files under `tests/Confluent.Kafka.UnitTests/` (mirroring the async
producer's `PublicProducer*` set; mock-driven, no broker):

- **`PublicSyncProducerSendTests.cs`** — `autoComplete:true` `Send` returns the
  right `RecordMetadata` (offset/partition/topic/timestamp); a manual mock
  (`autoComplete:false`) resolved from a **helper thread** — `CompleteNext` unblocks
  `Send`, `ErrorNext` faults it with a `KafkaException` (**assert code AND message
  content**, DoD §3 / ffi §A5); null `record` → `ArgumentNullException`;
  post-`Dispose`/`Close` `Send` → `ObjectDisposedException`; a non-ASCII topic
  round-trips (UTF-8 guard, ffi §A3).
  > Note: because sync `Send` blocks (decision #1), the manual-mock tests drive
  > completion from another thread (the single-owner "another thread completes"
  > pattern, like the sync consumer's `Wakeup` test) — `Send` on thread A,
  > `CompleteNext`/`ErrorNext` on thread B.
- **`PublicSyncProducerPeripheralTests.cs`** — `Flush` on a mock succeeds
  (no pending → immediate); `PartitionsFor` on a mock returns an **empty** list
  (the honest reachability caveat, as async) and null topic → `ArgumentNullException`,
  empty topic forwarded (not rejected).
- **`PublicSyncProducerTeardownTests.cs`** — `Close()` on a mock succeeds and
  surfaces a close error when one occurs; `Dispose` swallows; double-`Dispose` /
  `Close`-then-`Dispose` are idempotent and return without hanging (the pump-less
  teardown regression); a post-`Close` op → `ObjectDisposedException`.
- **`PublicSyncProducerMockControlTests.cs`** — `CompleteNext` / `ErrorNext`
  (code + message) / `HistoryCount` / `Clear` semantics (mirrors
  `PublicProducerMockControlTests`).
- **`PublicSyncProducerSendAllocationBudgetTests.cs`** — the **send-path
  allocation budget** (DoD §10, ffi §A4): a large value adds **no** value-sized
  managed allocation (call-scoped pin, zero-copy); the sync `Send` allocates only
  the unavoidable `RecordMetadata` + topic string (Java's own behavior) and — being
  pump-less — **no TCS / registration / GCHandle** (strictly leaner than async).
  Include a **mutation-after-`Send`** test: mutate the caller's key/value right
  after `Send` returns; the produced record is unchanged (proves the core copied
  during the call).
- **`PublicSyncProducerTfmSmokeTests.cs`** — the **TFM matrix** smoke
  (net462 via netstandard2.0, net8.0, net10.0): create `MockProducer` → `Send` →
  `Close` round-trip loads and works on every TFM (ffi §0.1/§0.2).

**DoD (per `definition-of-done.md` + CLAUDE.md §7.5):** builds on the TFM matrix;
unit tests pass against `MockProducer` (no broker); `dotnet format` +
`<EnforceCodeStyleInBuild>` clean; `ffi-marshalling.md` anti-patterns satisfied
(SafeHandle-param convention on the new sync ops, free-every-handle-on-every-path,
UTF-8 no-`LPStr`, flat `KafkaException` vs precondition .NET exceptions, no dead
DllImport); error-message content asserted; send-path allocation budget present.
CLAUDE.md §11 (consumer trait surface) N/A. §10 hot-path audit **applies** (the
sync `Send` is on the send path) and is satisfied by the leaner pump-less shape +
the allocation-budget test.

---

## 8 · Mode A confirmation + flagged Mode B gaps

**Mode A — CONFIRMED.** Every in-scope sync ABI fn is present in the checked-in
`target/include/confluent_kafka.h` (verified this session):

| Need | Header symbol | Line |
|---|---|---|
| single send (returns future) | `kafka_producer_Producer_send` | 2353 |
| **blocking get** | `kafka_producer_FutureRecordMetadata_get(future, out_error)` | 2522 |
| future destroy | `kafka_producer_FutureRecordMetadata_destroy` / `_destroy_all` | 2624 / 2645 |
| record-metadata read/free | `kafka_producer_RecordMetadata_{offset,partition,topic,timestamp,destroy}` | 2664+ |
| **sync flush** | `kafka_producer_Producer_flush(producer, out_error)` | 2792 |
| **sync partitions_for** | `kafka_producer_Producer_partitions_for(producer, topic, out_list)` | 2807 |
| **sync close** | `kafka_producer_Producer_close(producer, out_error)` | 2827 |
| mock control | `kafka_producer_MockProducer_{complete_next,error_next,history_count,clear}` | 2896+ |

No new ABI is required for the in-scope surface. The only C# interop additions are
DllImports for the above **existing** symbols (§6.2).

**Flagged Mode B / later gaps (out of scope, no action this phase):**
transactions, `Metrics`, `clientInstanceId`, the typed generic producer, and
headers on `ProducerRecord`/`RecordMetadata` — none are exposed at today's ABI;
each is a future Mode B item, consistent with the async producer's clip. No Mode B
dependency blocks P4.

---

## 9 · Commit-hygiene reminders (for the later N=31 loop — NOT this session)

- **Per-path `git add`** — stage only the intended `bindings/dotnet/{src,tests,
  design}` files.
- **NEVER stage:** the repo-root `.claude/agents/dotnet-{actor,critic}.md`
  discovery copies (untracked workaround copies — CLAUDE.md §8.4); any
  `COMMENTS.*.md` / `COMMENTS.DONE.*.md` at the binding root (local working files;
  the tracked record is the Manager's archived `design/history/M11/P4-producer-sync/`
  copy); `.claude/agent-memory/`; `target-linux*`; any built `.so` / `.dylib`;
  `.DS_Store`.
- Commits: **`--no-gpg-sign`**, and end the message with
  `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.
- Commit per DoD-passing step with clear messages (`dotnet(M11/P4): …`); fixup
  messages for COMMENTS fixes reference the original commit.

---

## Approval checklist (what the user is signing off)

1. **Decision #1** — sync `Send` blocks and returns `RecordMetadata` (= `send().get()`), diverging from Python's Future-returning sync `send` (forced by .NET's single `Task` type).
2. **Decision #2** — blocking `get`, **no pump**.
3. **Decision #3** — SafeHandle-param convention on the new `Flush`/`PartitionsFor` (retype `Producer_flush`; `Producer_partitions_for` fresh); `Close` teardown keeps the raw-handle release shape.
4. **Decision #4** — no `CancellationToken`.
5. **Decision #5** — `Close()` only, **no `Close(TimeSpan)`** (diverges from sync `IConsumer`, ABI/Python-grounded).
6. **Decision #6** — sibling types over `NativeProducer`; pump-less sync teardown.
7. **Decision #7** — inherits Option-C `buffer.memory` backpressure, no new handling.
8. **N = 31** — the loop's agent number (confirmed 2026-08-17; 30 was consumed by M11/P3).
9. **Type names** — bare `IProducer` / `KafkaProducer` / `MockProducer` (consumer precedent).
