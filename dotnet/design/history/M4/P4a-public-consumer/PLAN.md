# M4/P4a — Public Consumer Client (the first *usable* public cut)

**Status:** APPROVED (human, 2026-08-04) — cleared for implementation. This document
is the Manager's forward-looking plan; decisions/deviations made *during* execution go
in `COMMENTS.DONE.8.md`.

**Approved decisions (human, 2026-08-04):** (1) milestone label **M4/P4a**;
(2) micro-decision A — unify `ConsumerRecord.Key`/`Value` on **`byte[]?`** (deliberate
deviation from the CLAUDE.md §3 `ReadOnlyMemory` sketch — record in
`COMMENTS.DONE.8.md`); (3) micro-decision D — `IConsumer` **non-generic** now (generic
sibling types later, additive, not a rename); (4) `MockConsumer.AddRecord` =
**component-tuple** form `(topic, partition, offset, key, value)` — the public
`ConsumerRecord` stays **constructor-less / poll-output-only** this phase (full
Java-aligned ctor + the 4 extra fields deferred); (5) governance — **option (a)**:
`Wakeup()` handle TOCTOU kept accepted-by-design + documented on `KafkaConsumer`,
per-call `DangerousAddRef` hardening flagged as a candidate **N=9** follow-up (not
scheduled); (6) **`CloseAsync()` (no timeout)** this phase — `CloseAsync(TimeSpan)`
deferred to an additive overload once the C ABI exposes an async-timeout close (§3
CloseAsync note + §4 + §10).
**Mode:** A (Mode-A throughout — every ABI function this phase binds already
exists in `src/ffi/consumer.rs` / the generated `confluent_kafka.h`). No Rust is
authored. The one genuinely-new *wire* (`Consumer_unsubscribe_async`) is a
`[DllImport]` over an ABI function that already ships (`src/ffi/consumer.rs:2677`)
— **not** a Mode-B dependency. **No Mode-B (Rust-core) work anywhere in P4a.**
**Review counter:** N=8 (global monotonic; M0/P0=1, M1/P1=2, M2/P1=3, M2/P2=4,
M3/P1=5, M3/P2=6, M3/P3=7, **M4/P4a=8**).
**Personas:** `dotnet-actor` (Actor N=8) implements; `dotnet-critic` (Critic N=8)
reviews. NEVER the Rust `actor-executor` / `kafka-critic`.
**Branch / PR:** land on **`prashah_dev_public_consumer_scaffolding`** (current
HEAD; stacked on the pushed M3/P3 branch `prashah_dev_asyncbridge_poll_scaffolding`)
and open a **NEW PR stacked on top of the M3/P3 PR** — do NOT extend the M3/P3 PR
in place. Additive commits.
**Builds on:** M3/P1 (void completion bridge), M3/P2 (single-owner alignment),
M3/P3 (owned-handle bridge + poll receive path with on-dispatcher copy-out). The
public un-sealed `KafkaException` is the only public type from prior phases.

---

## Why this milestone label (M4, not M3/P4a)

STATUS groups phases into milestones by *coherent capability*: M2 = error model +
consumer lifecycle; M3 = the async completion bridge + poll receive path (P1 void
bridge → P2 single-owner → P3 owned-handle bridge + poll). Every M3 phase is
**internal-only** by design — `KafkaException` stayed the only public type, and
`ConsumerRecord(s)` were explicitly kept `internal` (M3/P3 confirmed decision 1).

P4a introduces the **first public client surface** — a distinct new capability
that STATUS's own "Next up" section frames as separate from the M3 bridge work
("The public `IConsumer` / `KafkaConsumer` / `MockConsumer` types … promoting the
proof ops to the real API surface"). Promoting internal machinery to a public,
Java-shaped, XML-documented API is the milestone boundary, so this is **M4/P4a**
(a new "public consumer" milestone), phase-tagged `P4a` because later op families
(commit, position, the sync-list getters, the owned-handle query siblings) are
**additive P4b/P4c/… phases** that grow the same public surface. This mirrors the
task's stated option and keeps the binding's independent milestone numbering
coherent. Artifacts live at `design/history/M4/P4a-public-consumer/`.

---

## 1 · Motivation & scope

### 1.1 Sequencing rationale (locked — the phase's motivation)

Public client **first**, before the remaining consumer ops, because:

- **All risky machinery is proven and landed.** The three hard mechanisms are
  done and tested: the void completion bridge (M3/P1), single-owner / not-thread-
  safe alignment with the Rust core as the serializer (M3/P2), and the owned-handle
  completion bridge + poll receive path with on-dispatcher copy-out (M3/P3). P4a
  introduces **no new risky mechanism** — it is promotion + public value-type
  design + composition + one trivial new void wire.
- **Nothing is usable yet.** The only public type is `KafkaException`; the consumer
  is reachable only through the internal `NativeConsumer`. P4a delivers the **first
  usable client** (subscribe → poll → seek → group metadata → close), usable
  end-to-end without a broker via `MockConsumer`, and usable against a real broker
  with auto-commit (`enable.auto.commit`) so an explicit commit API is not required
  to consume.
- **It settles the foundational public decisions** that every later phase hangs off:
  namespace/visibility gate, the public value-type shapes (`ConsumerRecord`,
  `ConsumerRecords`, `Headers`/`Header`, `TimestampType`, `TopicPartition`,
  `ConsumerGroupMetadata`), the async/sync split (per the CLAUDE.md idiom map read
  from the Java *implementation*), and the additive-growth `IConsumer` surface.
- **Public API growth is additive**, so later op phases just extend the surface —
  they add `IConsumer` methods and public types without breaking the P4a shape.

### 1.2 In scope

Promotion of the proven internal machinery to a public **Java-shaped** consumer:

- **Public value types** (namespace `Confluent.Kafka`): `ConsumerRecord`,
  `ConsumerRecords`, `Header`, `Headers`, `TimestampType` (enum), `TopicPartition`,
  `ConsumerGroupMetadata` (full field set). (Locked decisions 1–5.)
- **Public interface + classes** (namespace `Confluent.Kafka`): `IConsumer`
  (minimal, additive-growth), `KafkaConsumer` (real KIP-848), `MockConsumer`
  (test helper). Both clients `impl IConsumer`; the internal `NativeConsumer`
  folds in. (Locked decisions 6–7.)
- **Ops exposed** (all reuse proven mechanisms):
  - `PollAsync` → `Task<ConsumerRecords>` (owned-handle bridge, M3/P3). (Dec. 8.)
  - `SubscribeAsync` (void bridge, M3/P1). (Dec. 9.)
  - `UnsubscribeAsync` — the ONE piece of new wire code (~5 lines): a void-bridge
    over `Consumer_unsubscribe_async` (already in the ABI; add the DllImport + a
    `SubmitVoidOperation` call). (Dec. 10.)
  - `SeekAsync` → `Task` — async, Java-faithful (blocking `addAndGet`, §9.1);
    promote the existing internal async `SeekAsync` over `Consumer_seek_async`,
    **plus** the missing negative-*offset* precondition. (Dec. 11.)
  - `CloseAsync` / `DisposeAsync` / `IAsyncDisposable` + `Dispose` / `IDisposable`
    (M3/P1+P2 teardown; single-owner, no drain). (Dec. 12.)
  - Sync getters: `Wakeup()` (proven) and `GroupMetadata()` →
    `ConsumerGroupMetadata` (extend the internal `GroupId` read to all four fields;
    concurrent → `InvalidOperationException`). (Dec. 13.)
- **Public XML docs (CS1591)** on every new public member, with the required
  rationale/residual docstrings (see §7). Apache-2.0 header on new files.
- **Tests** — public round-trip via `KafkaConsumer`/`MockConsumer`; the async/sync
  split; `SeekAsync` negative-offset message; `GroupMetadata` full-field +
  concurrent → `InvalidOperationException`; teardown; TFM smoke; CS1591 compliance
  (§5).
- **STATUS handoff** + N-counter reconciliation (§7 governance).

### 1.3 Out of scope — later phases, all additive (do NOT build)

Each is a later additive phase; none changes the P4a public shape:

- **Commit family** (`CommitSync` / `CommitAsync`) — deferred: the ABI naming is a
  minefield (`Consumer_commit_async` is Java's *fire-and-forget* `commitAsync`, a
  **sync** call returning `KafkaError*`; the *push* variant of `commitSync` is
  `Consumer_commit_sync_async` — CLAUDE.md §1 warns of exactly this). Needs its own
  naming decision. **The P4a consumer is still usable without it** — auto-commit via
  `enable.auto.commit` works, so subscribe→poll→(auto-commit)→close is a complete
  loop.
- **`position`** — scalar-callback shape (Category B, `position_async` with an
  `int64_t` result slot); a distinct callback shape not yet proven.
- **`Assignment` / `Subscription` / `Paused`** — owned-**list** sync marshalling
  (`TopicPartitionList_t` / `StringList_t`); a new sync container shape not yet
  proven.
- **Owned-handle query siblings** — `committed` / `offsetsForTimes` /
  `beginning|endOffsets` / `partitionsFor` / `listTopics` — each needs new container
  marshalling (`OffsetMap` / `OffsetAndTimestampMap` / `LongOffsetMap` /
  `PartitionInfoList` / `TopicPartitionInfoMap`, `Node`); a whole later phase (the
  M3/P3 owned-handle *bridge* is proven, but not these result *containers*).
- **`subscribe(pattern)` / `assign` (public) / `pause` / `resume` /
  `seekToBeginning` / `seekToEnd` / `currentLag` / `clientInstanceId` / `metrics` /
  `enforceRebalance` / metric-subscription methods** — not on the P4a surface.
- **`ConsumerRebalanceListener` / `OffsetCommitCallback`** arguments (consumer-
  threading §31) — no listener parameter on P4a's `SubscribeAsync`.
- **Serializers / generic `IConsumer<TKey,TValue>`** — raw bytes only this phase
  (micro-decision D).
- **Typed `KafkaException` subclasses**; the §A7 producer completion decision.
- **The N≥8 cross-thread hardening items** — still not reachable (see §7).

---

## 2 · File-by-file work breakdown

Naming convention: **New** = created this phase, **Promote** = internal type made
public (moved out of `Internal/` to the library root, namespace → `Confluent.Kafka`),
**Edit** = existing file changed in place. Every new/edited public member carries an
XML doc (CS1591). Apache-2.0 header on new files.

### 2.1 Public value types (library root `src/Confluent.Kafka/`)

| File | Action | What & why |
|---|---|---|
| `ConsumerRecord.cs` | **Promote** from `Internal/ConsumerRecord.cs` | Move to root, namespace `Confluent.Kafka`, `internal sealed` → `public sealed`. Replace the internal `int TimestampType` with the public `TimestampType` enum. Key/Value type per **micro-decision A** (recommend `byte[]`). Headers become the public `Headers`/`Header` type (dec. 2). Drop the internal `RecordHeader` struct (superseded by public `Header`). Full XML docs. |
| `ConsumerRecords.cs` | **Promote** from `Internal/ConsumerRecords.cs` | Move to root, namespace `Confluent.Kafka`, `public sealed`. `IReadOnlyCollection<ConsumerRecord>` unchanged. Full XML docs. |
| `Header.cs` | **New** | Public `sealed class Header` (or `readonly struct`): `string Key`, `byte[]? Value` (dec. 2 — Java-faithful; matches CKD; the copy-out already allocates a `byte[]`, so `byte[]` *removes* a latent copy, it is not a logic change). XML docs incl. the `byte[]` rationale (§7). |
| `Headers.cs` | **New** | Public `Headers : IReadOnlyList<Header>` (or `IEnumerable<Header>` + `Count`), mirroring Java `Headers`/`RecordHeaders` clipped to today's ABI (read-only view of the copied-out headers). XML docs. |
| `TimestampType.cs` | **New** | Public `enum TimestampType { NoTimestampType = -1, CreateTime = 0, LogAppendTime = 1 }` (dec. 3) — maps directly from the `int` the ABI's `ConsumerRecord_timestamp_type` returns. XML docs. |
| `TopicPartition.cs` | **New** | Public value type: `string Topic`, `int Partition` (dec. 4). Recommend a `readonly struct` with value equality + `ToString()` ("topic-partition"), mirroring Java `TopicPartition`. Input type for `SeekAsync`. XML docs. |
| `ConsumerGroupMetadata.cs` | **New** | Public `sealed class` with the full field set (dec. 5): `string GroupId`, `int GenerationId`, `string MemberId`, `string? GroupInstanceId` (nullable — the ABI returns null when absent). VERIFIED reachable: `ConsumerGroupMetadata_group_id/_generation_id/_member_id/_group_instance_id` all exist (`src/ffi/consumer.rs:1613–1657`), plus `Consumer_group_metadata` → handle + `_destroy`. No Mode-B. XML docs. |

### 2.2 Public interface + client classes (library root)

| File | Action | What & why |
|---|---|---|
| `IConsumer.cs` | **New** | Public `interface IConsumer : IAsyncDisposable, IDisposable`. Minimal, additive-growth (dec. 6): only the ops promoted this phase + the sync getters. NO throwing stubs for not-yet-wired ops. Non-generic (micro-decision D). XML docs incl. the additive-growth rationale + the single-owner/not-thread-safe note (mirror the Python sibling). Exact surface in §3. |
| `KafkaConsumer.cs` | **New** | Public `sealed class KafkaConsumer : IConsumer` (real, KIP-848). Constructor `KafkaConsumer(IReadOnlyDictionary<string,string> config)` → `NativeConsumer.Create(config)`. Composes the internal `NativeConsumer` (**recommend compose over absorb** — see §2.5). XML docs. |
| `MockConsumer.cs` | **New** | Public `sealed class MockConsumer : IConsumer` (Java `MockConsumer`). Constructor `MockConsumer(string? autoOffsetReset = null)` → `NativeConsumer.CreateMock(...)`. Mock-only helpers (`AddRecord`, `SetPollError`, `Assign`) are **inherent methods on the concrete type, NOT on `IConsumer`** (consumer-threading §2; micro-decision B). XML docs. |

### 2.3 Internal machinery (edits)

| File | Action | What & why |
|---|---|---|
| `Internal/NativeConsumer.cs` | **Edit** | (a) Add `UnsubscribeAsync` (~5 lines): `SubmitVoidOperation(ct, (c,cb,ud) => NativeMethods.ConsumerUnsubscribeAsync(c, cb, ud))`. (b) **Fix the SeekAsync offset gap**: add the `offset < 0` precondition (see §2.6 — currently only `partition < 0` is validated; Java validates offset). (c) Add `GroupMetadata()` returning a marshalled `ConsumerGroupMetadata` (extend the existing `GroupId()` to read all four fields via a new `ConsumerGroupMetadataMarshal`, or inline). Keep `GroupId()` if any test still uses it, else it can be folded into `GroupMetadata()`. Change signatures returning the internal record types to the public ones (post-promotion). `NativeConsumer` stays `internal`. |
| `Internal/Interop/NativeMethods.cs` | **Edit** | Add DllImports: `Consumer_unsubscribe_async` (op-callback + user_data — reuses `ConsumerCallbacks.Operation`); the three missing group-metadata accessors `ConsumerGroupMetadata_generation_id` (`int`), `_member_id` (`IntPtr`, NUL string), `_group_instance_id` (`IntPtr`, NUL string, may be `Zero`). `group_id` + `_destroy` + the `Consumer_group_metadata` getter are already declared. |
| `Internal/Interop/ConsumerGroupMetadataMarshal.cs` | **New (recommended)** | Small helper: given the owned metadata `IntPtr`, read all four fields (`group_id`/`member_id`/`group_instance_id` via NUL-terminated `Utf8Marshal.PtrToString`; `generation_id` scalar), build a public `ConsumerGroupMetadata`, then `_destroy` in a `finally` (Category-3 owned handle, §B2). Keeps `unsafe`-free marshalling out of `NativeConsumer`. Alternatively inline in `NativeConsumer.GroupMetadata()` (no `unsafe` needed — NUL-scan `PtrToString` is safe). Actor's call; recommend the helper for symmetry with `ConsumerRecordsMarshal`. |
| `Internal/Interop/ConsumerRecordsMarshal.cs` | **Edit** | Update to build the **public** `ConsumerRecord`/`ConsumerRecords`/`Header`/`Headers`/`TimestampType`, and (per micro-decision A) key/value as `byte[]` instead of `ReadOnlyMemory<byte>?`. The copy-out logic (length-delimited §B3 strings, `Marshal.Copy` for bytes, absent/tombstone sentinels) is unchanged — only the produced types change. The `byte[]` change *removes* the `ReadOnlyMemory` wrap, a net simplification. |

Files **unchanged**: `OperationCompletionSource.cs` (generic bridge already carries
any `TResult` — `Task<ConsumerRecords>` works as-is); `ConsumerCallbacks.cs` (the
`Poll`/`Operation` trampolines are reused verbatim — `UnsubscribeAsync` uses the
existing `Operation` void trampoline); `Utf8Marshal.cs` (both NUL + length-delimited
forms already exist); the `SafeHandle` files; `KafkaException.cs`.

### 2.4 Tests (test project `tests/Confluent.Kafka.UnitTests/`)

New public-surface test files (details in §5). The existing internal-surface tests
(`ConsumerCompletionBridgeTests`, `ConsumerAsyncOperationTests`, the M3/P3 poll
tests, etc.) stay green — the internal `NativeConsumer` is unchanged except for the
three additive edits, so its tests carry forward. New tests exercise the **public**
`KafkaConsumer`/`MockConsumer` end-to-end.

### 2.5 Compose vs absorb (micro-decision B follow-through) — **recommend compose**

The public `KafkaConsumer`/`MockConsumer` should **hold** an internal
`NativeConsumer` (`private readonly NativeConsumer _native;`) and forward, rather
than **absorb** its body. Rationale:

- **Preserves the proven internal test surface.** `NativeConsumer` is directly
  driven by ~40 internal interop tests (via `InternalsVisibleTo`) that assert the
  5 FFI invariants, teardown semantics, and the completion bridge. Absorbing would
  either delete that surface or duplicate it against the public type — churn with
  no benefit.
- **Keeps the visibility boundary clean.** `unsafe`/`DangerousGetHandle`/`GCHandle`
  bookkeeping stays quarantined under `Internal/`; the public class is a thin,
  auditable Java-shaped forwarder (`Task PollAsync(...) => _native.PollAsync(...)`),
  which is exactly the CLAUDE.md §2 "public shape auditable at a glance" intent.
- **Teardown composes cleanly.** `KafkaConsumer.DisposeAsync()` →
  `_native.DisposeAsync()`; `Dispose()` → `_native.Dispose()`. The single-owner
  contract, the atomic closed flag, and the accepted residuals all live once in
  `NativeConsumer` and are inherited unchanged.

`MockConsumer`'s inherent helpers forward too (`AddRecord` → `_native.AddRecord`,
`SetPollError` → `_native.SetPollError`, `Assign` → `_native.Assign`).

### 2.6 The one behavioral gap to fix (SeekAsync offset validation)

The internal `SeekAsync` (`NativeConsumer.cs:344`) validates `partition < 0` but
**not** `offset < 0`. Java's `AsyncKafkaConsumer.seek()` (`AsyncKafkaConsumer.java:1055`)
throws `IllegalArgumentException("seek offset must not be a negative number")` on
`offset < 0` *before* the blocking `addAndGet`. Per the CLAUDE.md idiom map
(`IllegalArgumentException` → `ArgumentOutOfRangeException`, validated **before** the
FFI call) and locked decision 11, `SeekAsync` must add:

```csharp
if (offset < 0)
{
    throw new ArgumentOutOfRangeException(
        nameof(offset), offset, "seek offset must not be a negative number");
}
```

A test asserts the exact message (DoD §3 error-message fidelity). This is the only
place P4a corrects internal behavior; flag it in `COMMENTS.DONE.8.md` as a
Java-fidelity fix (not a regression — the internal proof op simply never validated
offset because the M3/P1 proof only exercised the *unassigned-partition* failure).

---

## 3 · Public API surface (exact signatures — Java `Consumer.java` shape, .NET casing)

Namespace `Confluent.Kafka`. All types `public`. Non-generic (micro-decision D).

```csharp
namespace Confluent.Kafka;

// ---- value types ----

public enum TimestampType
{
    NoTimestampType = -1,
    CreateTime = 0,
    LogAppendTime = 1,
}

public readonly struct TopicPartition : IEquatable<TopicPartition>
{
    public TopicPartition(string topic, int partition);
    public string Topic { get; }
    public int Partition { get; }
    public bool Equals(TopicPartition other);
    public override bool Equals(object? obj);
    public override int GetHashCode();
    public override string ToString();      // "topic-partition"
}

public sealed class Header                  // Java Header (byte[] value, dec. 2)
{
    public Header(string key, byte[]? value);
    public string Key { get; }
    public byte[]? Value { get; }           // byte[] — Java-faithful, matches CKD
}

public sealed class Headers : IReadOnlyList<Header>   // Java Headers, clipped
{
    public int Count { get; }
    public Header this[int index] { get; }
    public IEnumerator<Header> GetEnumerator();
}

public sealed class ConsumerRecord          // Java ConsumerRecord, clipped to today's ABI
{
    public string Topic { get; }
    public int Partition { get; }
    public long Offset { get; }
    public long Timestamp { get; }          // -1 = NO_TIMESTAMP
    public TimestampType TimestampType { get; }
    public byte[]? Key { get; }             // micro-decision A: recommend byte[]; null = absent
    public byte[]? Value { get; }           // null = tombstone
    public Headers Headers { get; }
}

public sealed class ConsumerRecords : IReadOnlyCollection<ConsumerRecord>
{
    public int Count { get; }
    public IEnumerator<ConsumerRecord> GetEnumerator();
}

public sealed class ConsumerGroupMetadata   // Java ConsumerGroupMetadata, full field set (dec. 5)
{
    public string GroupId { get; }
    public int GenerationId { get; }
    public string MemberId { get; }
    public string? GroupInstanceId { get; } // null when absent
}

// ---- interface + clients ----

// Minimal, additive-growth (dec. 6). Non-generic now (micro-decision D).
// Single-owner / not thread-safe (mirror the Python sibling note in XML docs).
public interface IConsumer : IAsyncDisposable, IDisposable
{
    // blocking-in-Java / callback-at-ABI → async (Async suffix)
    Task<ConsumerRecords> PollAsync(TimeSpan timeout, CancellationToken cancellationToken = default);
    Task SubscribeAsync(IReadOnlyCollection<string> topics, CancellationToken cancellationToken = default);
    Task UnsubscribeAsync(CancellationToken cancellationToken = default);
    Task SeekAsync(TopicPartition partition, long offset, CancellationToken cancellationToken = default);
    Task CloseAsync(CancellationToken cancellationToken = default);   // Java close(); CloseAsync(TimeSpan) deferred (§3 note)

    // non-blocking / instantaneous in Java → stays sync
    void Wakeup();
    ConsumerGroupMetadata GroupMetadata();
}

public sealed class KafkaConsumer : IConsumer   // Java KafkaConsumer (KIP-848)
{
    public KafkaConsumer(IReadOnlyDictionary<string, string> config);
    // IConsumer members …
}

public sealed class MockConsumer : IConsumer    // Java MockConsumer
{
    public MockConsumer(string? autoOffsetReset = null);
    // IConsumer members …
    // mock-only helpers — inherent, NOT on IConsumer (consumer-threading §2):
    public void Assign(IReadOnlyList<TopicPartition> partitions);
    public void AddRecord(ConsumerRecord record);   // or (topic, partition, offset, key, value) — see note
    public void SetPollError(string message);
}
```

**`SeekAsync` signature note.** Java is `seek(TopicPartition, long)`. The internal
`NativeConsumer.SeekAsync(string topic, int partition, long offset, …)` takes the
components; the public `SeekAsync(TopicPartition, long)` destructures the struct and
forwards. This is the Java shape (dec. 11).

**`MockConsumer.AddRecord` shape note.** The internal `NativeConsumer.AddRecord`
takes `(topic, partition, offset, key, value)`. Two public options: (a) mirror Java
`MockConsumer.addRecord(ConsumerRecord)` taking a public `ConsumerRecord` (Java-
faithful, but `ConsumerRecord` has no public constructor in the §3 surface — would
need one added, `internal` or `public`); (b) keep the component tuple
`AddRecord(string topic, int partition, long offset, byte[]? key, byte[]? value)`.
**Recommend (b)** for P4a: it needs no `ConsumerRecord` public ctor (which Java
*does* expose but which invites a fuller public record surface than P4a scopes), and
matches the internal driver 1:1. Java's `addRecord(ConsumerRecord)` can be added
additively later once `ConsumerRecord` gains a public constructor. Flag in the plan
for confirmation.

**`CloseAsync()` note (no timeout — ABI-verified).** Maps Java `close()`. VERIFIED the
ABI has **no async-close-with-timeout**: `Consumer_close_async` (`consumer.rs:3123`)
takes only a callback (core default timeout); the only timeout-accepting close is the
**sync** `Consumer_close_with_timeout` (`:3108`). So a faithful async
`CloseAsync(TimeSpan)` is **deferred** to an additive overload once a Rust-core
`close_async_with_timeout` exists (Mode-B, out of scope) — rather than shipping a
silently-ignored `TimeSpan`. (Python accepts+ignores its `timeout` kwarg —
`bindings/python/consumer.py:401` `_close_spec` drops it; we prefer CKD parity: CKD's
`Close()` has no timeout, and an ignored `TimeSpan` is a .NET footgun.) P4a wires
`CloseAsync()` to the existing `CloseAsyncInternal()` (bridges `close_async`) but,
unlike `DisposeAsync`, **surfaces** the close `KafkaException`; it takes the
`TryBeginClose` one-shot latch, closes, then destroys in a `finally` (destroy exactly
once even on error); a subsequent `DisposeAsync`/`Dispose` loses the latch → no-op.
Document in `COMMENTS.DONE.8.md`.

---

## 4 · The async/sync split — governing-rule check (per CLAUDE.md idiom map)

Decided from the **Java implementation** (`AsyncKafkaConsumer`), never the Javadoc /
interface / method name (CLAUDE.md §4 "Sync vs async"):

| Member | Java impl signal | C# | Note |
|---|---|---|---|
| `PollAsync` | blocks (event round-trip) | `Task<ConsumerRecords>` | M3/P3 owned-handle bridge |
| `SubscribeAsync` | `addAndGet` (blocks) | `Task` | M3/P1 void bridge |
| `UnsubscribeAsync` | `addAndGet` (blocks) | `Task` | new void wire |
| `SeekAsync` | **`addAndGet` (blocks)** — VERIFIED `AsyncKafkaConsumer.seek():1068` | `Task` | async, Java-faithful; **deliberate divergence from Python's sync `seek`** — note in docstring (§7) |
| `CloseAsync` | blocks | `Task` | teardown |
| `Wakeup` | non-blocking action | `void` | cross-thread by design |
| `GroupMetadata` | non-blocking getter | `ConsumerGroupMetadata` (method, matching Java `groupMetadata()`) | concurrent → `InvalidOperationException` |

`SeekAsync`-is-async is the load-bearing split call: VERIFIED that
`AsyncKafkaConsumer.seek()` returns void but calls `applicationEventHandler.addAndGet(
new SeekUnvalidatedEvent(...))` (line 1068) — a blocking cross-thread event round-trip.
The faithful shape is `Task` (§9.1). We promote the existing internal *async*
`SeekAsync` over `Consumer_seek_async` **unchanged in mechanism** — do NOT switch to
the sync `Consumer_seek`. Python exposes `seek` sync (`bindings/python/consumer.py:286`);
that is a deliberate Python divergence, and we choose Java fidelity — noted in a
docstring per §7.

---

## 5 · Test plan

Test project `tests/Confluent.Kafka.UnitTests/`, xUnit, internals via
`InternalsVisibleTo`, `Interop/`-mirroring layout for interop-touching tests but
public-surface tests live at the test-project root. **Every awaited op and every
teardown runs under a `TestTimeout` hang-guard** (the completion/deadlock regression
guard). Reuse the M3/P3 test precedents (SUCCESS/FAILURE/empty/churn/GC-keep-alive/
RunContinuationsAsynchronously/no-throw/allocation-budget).

### 5.1 Public round-trip (via `KafkaConsumer` / `MockConsumer`)

- **Subscribe → add records → poll → assert** through the **public** `MockConsumer`:
  `Assign` → `AddRecord` (×N) → `await PollAsync` → assert `ConsumerRecords.Count`,
  and per record `Topic`/`Partition`/`Offset`/`Timestamp`/`TimestampType`(enum)/
  `Key`/`Value`/`Headers`. Non-ASCII topic + key/value + header key via the length-
  delimited `out_len` path (§B3 — carried from M3/P3, now through the public types).
- **Empty poll** → non-null `ConsumerRecords`, `Count == 0` (success, not fault).
- **Poll FAILURE** → `SetPollError("boom")` → `await`ing `PollAsync` throws
  `KafkaException` (assert TYPE + `Message` "boom", NOT `Code` — broker-free codes are
  all -1, M3/P3 finding #8).
- **`TimestampType` mapping** — a record with each of `-1`/`0`/`1` maps to
  `NoTimestampType`/`CreateTime`/`LogAppendTime`.
- **`Header.Value` is `byte[]`** — a header round-trips its bytes as `byte[]`; a null
  header value → `null`; empty headers → empty `Headers` (`Count == 0`).

### 5.2 Async/sync split

- `SubscribeAsync` / `UnsubscribeAsync` return `Task` and complete on the
  `MockConsumer` (churned — many iterations, no leak/hang).
- `SeekAsync` returns `Task`; seeking an **unassigned** partition faults the `Task`
  with `KafkaException` (the void bridge's failure path, carried from M3/P1 `SeekAsync`).
- `Wakeup()` is `void`; `GroupMetadata()` returns synchronously.

### 5.3 `SeekAsync` negative-offset (DoD §3 error-message fidelity)

- `SeekAsync(new TopicPartition("t", 0), -1)` throws **`ArgumentOutOfRangeException`**
  with message **`"seek offset must not be a negative number"`** (exact string
  asserted), thrown **before** any native call (a canceled/closed consumer still
  throws the precondition first).
- Negative partition via the `TopicPartition` ctor path still →
  `ArgumentOutOfRangeException` (carried).

### 5.4 `GroupMetadata` full-field + concurrency

- **Full field set:** configure `group.id` (+ where reachable broker-free,
  `group.instance.id`) → `GroupMetadata()` → assert `GroupId` (incl. **non-ASCII**,
  the M2/P1 D5 round-trip carried to the public type), `GenerationId`, `MemberId`,
  and `GroupInstanceId` (null when unset; the configured value when set — verify
  broker-free reachability early, as M2/P1 D5 did; if `member_id`/`generation_id`
  are only meaningful post-join, assert their broker-free defaults and document,
  mirroring M2/P1 D5).
- **Concurrent → `InvalidOperationException`:** VERIFIED `Consumer_group_metadata`
  returns null on the core's concurrent-access rejection (`src/ffi/consumer.rs:3727`);
  `GroupMetadata()` maps null → `InvalidOperationException` ("KafkaConsumer is not safe
  for multi-threaded access."), mirroring the existing internal `GroupId()`. Test the
  reachable seam (the round-trip); the null → `InvalidOperationException` mapping is
  verified by inspection + the forced-overlap slice **if** a poll held open makes it
  deterministic (carry the M3/P3 D-Q4 approach; if not deterministic broker-free,
  document the residual as M3/P3 did — do NOT ship a flaky test).

### 5.5 Teardown (Dispose / DisposeAsync)

- `DisposeAsync` and `Dispose` on the **public** client return without hanging (the
  teardown-returns regression), incl. with an unawaited op in flight (accepted
  single-owner residual — strand + one-time leak; `DisposeAsync` on the awaiting task
  is the clean path).
- Double-dispose / mixed `Dispose`+`DisposeAsync` are safe; use-after-dispose throws
  `ObjectDisposedException` from every public op.
- `CloseAsync()` then `DisposeAsync` is safe (closed-flag idempotence); `CloseAsync()`
  surfaces a close `KafkaException` (unlike `DisposeAsync`, which swallows it).

### 5.6 Allocation budget (carried)

- Per-record receive-path allocation-budget test (DoD §10, §B4, consumer-threading
  §27), now through the public types. The `byte[]` key/value (micro-decision A) is
  the same single copy the copy-out already makes — assert the per-record budget is
  exactly {topic `string`, key `byte[]`, value `byte[]`, header copies, record +
  list objects}, nothing attributable to batch traversal or borrowed-pointer
  marshalling. (`byte[]` *removes* the `ReadOnlyMemory` wrap vs M3/P3, so the budget
  does not increase.)

### 5.7 TFM smoke + CS1591

- **TFM-matrix smoke:** the public `MockConsumer` create → subscribe → add → poll →
  close round-trip runs on **net10.0 locally**; **net8.0** and **net462 (via
  netstandard2.0)** are **CI-only** (both *build* legs must pass locally; do not
  block DoD on those *runs*). Add net8.0 + net462 to the **test** project TFMs if not
  already present, so the smoke test compiles on the floor (the library already
  targets `netstandard2.0;net8.0;net10.0`).
- **CS1591 / public-XML-doc:** `dotnet build` is **0 warnings / 0 errors** with
  `GenerateDocumentationFile` + `TreatWarningsAsErrors` + `EnforceCodeStyleInBuild`.
  **This is the first phase since M2/P1 to add public types**, so every new public
  member (all of §3) must carry an XML doc — this is a real new obligation (M3/P1–P3
  had zero new public surface). CA1815 (`TopicPartition` value equality) and CA1032
  (exception ctors — N/A here) analyzers must be satisfied without suppressions where
  possible; document any needed suppression in `COMMENTS.DONE.8.md`.

### 5.8 Stability

- Run the full suite **~20×** (the noted stability-run expectation) to shake out any
  async-timing flakiness in the public round-trip / teardown paths before Critic
  handoff. Precedent: M3/P3's churn/GC/RunContinuationsAsynchronously tests.

---

## 6 · Open micro-decisions (A–D) — recommendations for human confirmation

### A. `ConsumerRecord.Key`/`Value` type — **recommend unify on `byte[]`**

Today (internal) they are `ReadOnlyMemory<byte>?`. `Header.Value` is now `byte[]`
(dec. 2). **Recommend unifying `Key`/`Value` on `byte[]?`** to match:

- **Java fidelity + CKD parity:** Java `ConsumerRecord.key()/value()` are the
  deserialized types; the raw-bytes interim is `byte[]`, and CKD's `Message` uses
  `byte[]`. Unifying avoids a `ReadOnlyMemory`↔`byte[]` inconsistency between record
  bytes and header bytes.
- **No cost:** the copy-out already allocates an owned `byte[]` and *wraps* it in
  `ReadOnlyMemory`. Returning `byte[]` **removes** the wrap — a net simplification,
  not a new copy. The allocation budget is unchanged.
- **Ripple on the marshaller:** `ConsumerRecordsMarshal.CopyBytes` returns `byte[]?`
  instead of `ReadOnlyMemory<byte>?` (drop the wrap); `ConsumerRecord` fields become
  `byte[]?`. Trivial.

Alternative (keep `ReadOnlyMemory<byte>?`) is the .NET idiom for buffer-y APIs, but
here the buffer is always a fresh owned array with no slicing benefit, so
`ReadOnlyMemory` adds a wrapper type for no gain and diverges from the `byte[]`
header value. **My lean: unify on `byte[]`.** (The CLAUDE.md §3 sketch shows
`ReadOnlyMemory<byte>?`; unifying on `byte[]` is a deliberate, documented deviation
consistent with dec. 2 — record in `COMMENTS.DONE.8.md`.)

### B. `KafkaConsumer` vs `MockConsumer` construction — **recommend compose + inherent mock helpers**

- `KafkaConsumer(config)` → `NativeConsumer.Create(config)` (fallible; surfaces
  `KafkaException` on bad config).
- `MockConsumer(autoOffsetReset = null)` → `NativeConsumer.CreateMock(autoOffsetReset)`
  (non-fallible).
- Both **compose** an internal `NativeConsumer` and forward (§2.5).
- `MockConsumer`'s mock-only helpers (`AddRecord`, `SetPollError`, `Assign`) are
  **inherent methods on the concrete `MockConsumer`, NOT on `IConsumer`** (consumer-
  threading §2, Python `_MockConsumerMixin` parity). Tests hold a `MockConsumer`
  directly and pass it as `IConsumer` where the trait is expected.

### C. Namespace / visibility gate — **recommend `Confluent.Kafka` public; keep marshallers internal**

- All public types (§3) land in namespace **`Confluent.Kafka`** at the library root
  (CLAUDE.md §2 file map).
- **Keep internal:** `NativeConsumer`, `ConsumerRecordsMarshal`,
  `ConsumerGroupMetadataMarshal`, `NativeMethods`, `ConsumerCallbacks`,
  `OperationCompletionSource`, `Utf8Marshal`, the `SafeHandle`s — all stay under
  `Internal/`. The public client is a thin forwarder; no raw marshaller or interop
  type becomes public.
- **The §4 package-id pre-publish gate stays OPEN and untouched** (M0/P1): P4a adds
  public *types* but does not publish; `IsPackable=false` still holds it shut. Note
  the gate in the STATUS handoff but do NOT resolve it (out of scope).
- CS1591 now applies to every new public member (§5.7).

### D. `IConsumer` generic-ness — **recommend non-generic now, generic later (additive)**

- P4a has **no serializers** — the ABI is bytes-only, records carry `byte[]`. Introduce
  `IConsumer` **non-generic** now (raw `byte[]` records).
- Adding `IConsumer<TKey,TValue>` later (with the serdes phase) is **additive** if done
  as a *new* generic interface + generic `KafkaConsumer<TKey,TValue>` /
  `ConsumerRecord<TKey,TValue>`, leaving the non-generic raw-bytes surface in place
  (CLAUDE.md §4 "non-generic bytes now; generic … when serializers land"). It is
  **breaking** only if we later try to *retrofit* generics onto the same names — so we
  will NOT do that; the generic surface arrives as sibling types.
- **Trade-off:** a generic-from-the-start `IConsumer<byte[],byte[]>` would avoid a
  second interface later, but forces a serializer abstraction P4a does not have and
  makes every P4a signature carry two type params for no benefit. **My lean:
  non-generic now** — simplest, matches the raw-bytes reality, and the later generic
  surface is additive (new types), not a rename.

---

## 7 · DoD checklist (mapped to `definition-of-done.md`) + no-new-struct/trait audit

Verification gates run in order (`.NET` DoD, NOT `cargo xtask` / `make verify`):

1. `cargo build --features ffi` — native cdylib + header present (run FIRST,
   CLAUDE.md §7.1). **No ABI change this phase (Mode A).**
2. `dotnet build` — **0 warnings / 0 errors** across library TFMs
   (`netstandard2.0;net8.0;net10.0`) + test TFMs. `TreatWarningsAsErrors` +
   `EnforceCodeStyleInBuild` + `GenerateDocumentationFile` active. **CS1591 on every
   new public member** (§5.7). Apache-2.0 header on new files; no TODO/FIXME.
3. `dotnet test -f net10.0` — all pass, **no hang** (every awaited op / teardown under
   a `TestTimeout` guard). Run ~20× for stability (§5.8).
4. `dotnet format --verify-no-changes` — clean.
5. **CI-only caveat:** net8.0 *run* + net462 (via ns2.0) are CI-only; both *build*
   legs must pass locally.

Mapped `definition-of-done.md` items:

- **§1 (consistent with CLAUDE + rules):** the async/sync split follows the idiom map
  read from the Java *implementation* (§4); single-owner, copy-out, the 5 invariants
  all inherited unchanged.
- **§2 (all methods translated):** the P4a `IConsumer` surface is a deliberate
  **subset** of Java `Consumer<K,V>` (additive-growth, dec. 6) — the deferred methods
  (§1.3) are enumerated with rationale, satisfying §2's "explain why not translated".
- **§3 (tests translated; error-message content asserted):** §5 covers the public
  round-trip, the `SeekAsync` negative-offset **message** assertion, `GroupMetadata`
  full-field, teardown, TFM smoke. Broker-free codes are indistinct (all -1), so tests
  assert TYPE + `Message`, never `Code` (M3/P3 finding #8, carried).
- **§5 (all tests passing):** gate 3.
- **§6 (no duplicated classes):** promotion **moves** the internal record types to
  public (not a copy) — the internal `ConsumerRecord`/`ConsumerRecords`/`RecordHeader`
  are deleted, replaced by the public types. Verify no orphan internal record type
  remains (grep).
- **§7 (no struct/trait not in Java) — the scaffolding audit:** every public type maps
  to a Java `Consumer.java` counterpart: `ConsumerRecord`/`ConsumerRecords`/`Header`/
  `Headers`/`TimestampType`/`TopicPartition`/`ConsumerGroupMetadata`/`IConsumer`/
  `KafkaConsumer`/`MockConsumer` are all Java types (adapted casing). The **only**
  host-only scaffolding (`NativeConsumer`, marshallers, `NativeMethods`,
  `OperationCompletionSource`, `ConsumerCallbacks`, `SafeHandle`s) is **shape-
  restoration plumbing** with no Kafka logic — expected per `bindings/CLAUDE.md §1.2`
  and it all stays **internal**. `IConsumer` (the `I`-prefix + `Async` suffix) is the
  documented .NET idiom-layer adaptation of Java's `Consumer` (CLAUDE.md §4 CA1715),
  not a new abstraction. No new public type lacks a Java counterpart.
- **§8 (no TODO/FIXME):** gate 2.
- **§9 (`make verify`):** N/A — the .NET binding uses the `dotnet` DoD gates above,
  not `make verify` (M3 precedent).
- **§10 (hot-path allocation audit):** the receive-path per-record budget test (§5.6);
  the `byte[]` unify (micro-decision A) does not increase it.
- **§11 (consumer trait-surface check):** `IConsumer` is a single **non-`async_trait`
  .NET interface** with async methods returning `Task` (the C# equivalent of the
  §11 top-level dispatch surface — no enum dispatch wrapping `KafkaConsumer`/
  `MockConsumer`; both `impl IConsumer` directly). Per-record marshalling
  (`ConsumerRecordsMarshal`) is sync, no async trait. **No `block_on`-wrapped sync
  façade** for any async method (consumer-threading §1 — the public API is async where
  Java blocks). Mock-only helpers are inherent, not on `IConsumer` (§2).

**Rationale/residual docstrings required (the "capture rationale + accepted residuals"
convention):**

- The `SeekAsync`-is-async rationale (blocking `addAndGet`, §9.1; deliberate Python
  divergence) — in `SeekAsync`'s XML doc.
- The `byte[]` header/value rationale (Java-faithful, removes a latent copy) — in a
  comment where the marshaller drops the `ReadOnlyMemory` wrap + on `Header.Value`.
- The additive-growth `IConsumer` rationale (pre-publish, no external implementers) —
  in the `IConsumer` XML doc.
- The single-owner / not-thread-safe inheritance — in the `KafkaConsumer`/`IConsumer`
  XML docs (mirror the Python sibling's "single-owner" note).
- The 5 FFI invariants, single-owner teardown, on-dispatcher copy-out — inherited
  from `NativeConsumer`, unchanged; the public client's XML docs point to them.
- **No Mode-B:** every ABI function bound is verified present (§ below). If planning/
  implementation surfaces anything needing a new ABI function, **FLAG it as a Rust-
  core dependency and scope around it — do not author Rust.**

**ABI functions bound (all verified present, Mode A):**
`Consumer_unsubscribe_async` (`consumer.rs:2677`), `Consumer_seek_async` (`:2731`),
`Consumer_group_metadata` (`:3723`), `ConsumerGroupMetadata_group_id` (`:1613`),
`_generation_id` (`:1625`), `_member_id` (`:1637`), `_group_instance_id` (`:1650`),
`_destroy` (`:1665`); plus all M3/P3 poll/record accessors + lifecycle already bound.

---

## 8 · Suggested sub-step ordering for the Actor (clean, commit-per-step)

Each step ends green (`dotnet build` 0/0 + relevant tests) and is one commit:

1. **Value types (public, no client yet).** New/promote `TimestampType`,
   `TopicPartition`, `Header`, `Headers`, `ConsumerGroupMetadata`; promote
   `ConsumerRecord`/`ConsumerRecords` to public (with the public `Headers` + enum +
   `byte[]` per micro-decision A). Update `ConsumerRecordsMarshal` to build the public
   types + `byte[]`. Delete the internal record/`RecordHeader` types. Carry the M3/P3
   poll tests over to the public types (they still drive `NativeConsumer.PollAsync`,
   which now returns the public `ConsumerRecords`). **Gate:** build 0/0 (CS1591 on the
   new public types), M3/P3 tests green through the public types.
2. **`NativeConsumer` edits.** Add `UnsubscribeAsync` (+ the `Consumer_unsubscribe_async`
   DllImport), the `SeekAsync` offset precondition, and `GroupMetadata()` (+ the three
   group-metadata DllImports + `ConsumerGroupMetadataMarshal`). Add internal tests for
   each (unsubscribe churn, seek negative-offset message, group-metadata full-field +
   concurrent → `InvalidOperationException`). **Gate:** build 0/0, new internal tests
   green.
3. **`IConsumer` interface.** Define the minimal additive-growth interface (§3) with
   full XML docs. (No implementers yet — compiles standalone.) **Gate:** build 0/0.
4. **`KafkaConsumer` + `MockConsumer` (composition).** New public classes forwarding to
   an internal `NativeConsumer`; `MockConsumer` inherent helpers. Wire `CloseAsync` /
   `DisposeAsync` / `Dispose`. **Gate:** build 0/0.
5. **Public round-trip + async/sync + teardown tests.** The §5 public-surface tests
   (round-trip, TimestampType/Header/byte[], SeekAsync message, GroupMetadata, teardown,
   allocation budget, TFM smoke). Run ~20× for stability. **Gate:** all §7 gates green.
6. **STATUS handoff + N-counter reconciliation** (§9). **Gate:** docs match code.

(Steps 1–2 are the "settle the value types + wire Unsubscribe/GroupMetadata" core;
3–4 are the public surface; 5 is verification; 6 is handoff. If the Critic (N=8) finds
issues, the Actor fixes per `COMMENTS.8.md` and moves resolved items to
`COMMENTS.DONE.8.md`.)

---

## 9 · Governance

- **Personas:** `dotnet-actor` (Actor **N=8**) implements; `dotnet-critic` (Critic
  **N=8**) reviews. NEVER the Rust `actor-executor` / `kafka-critic`. Nested-agent
  discovery does not work — the root-`.claude/agents/` `dotnet-{actor,critic}.md`
  **discovery copies** already exist (verified in git status) and stay **untracked**;
  the binding-local personas stay **tracked** (edit those; re-copy after editing).
- **Review files:** working `bindings/dotnet/COMMENTS.8.md` → resolved to
  `bindings/dotnet/COMMENTS.DONE.8.md`. `COMMENTS.8.md` is gitignored
  (`COMMENTS\.[0-9]*\.md`); `COMMENTS.DONE.8.md` is **not** gitignored — never
  `git add` it. **Reset for N=8:** the working `COMMENTS.7.md`/`COMMENTS.DONE.7.md`
  (M3/P3, already archived under `design/history/M3/P3-poll-receive-path/`) must be
  cleared before the loop starts. At Final Handoff, copy the closed `COMMENTS.DONE.8.md`
  to `design/history/M4/P4a-public-consumer/COMMENTS.DONE.8.md` and reset the
  binding-root working files.
- **Branch / PR:** land on **`prashah_dev_public_consumer_scaffolding`** (stacked on
  the pushed M3/P3 branch `prashah_dev_asyncbridge_poll_scaffolding`). P4a's commits are
  **additive** on this branch and open a **NEW PR stacked on top of the M3/P3 PR** — do
  NOT extend the M3/P3 PR in place. Small incremental commits, each passing the .NET
  gates; Apache-2.0 header on new files. `bindings/dotnet/.claude/agent-memory/**`
  excluded from every commit/PR (only `.gitkeep`). Verify each commit's staged file
  list.
- **N-counter reconciliation:** M4/P4a **takes N=8**. STATUS's deferred cross-thread
  **hardening** items (`Wakeup`/`GroupId` handle TOCTOU vs teardown; the
  submit-vs-`destroy` handle race) were renumbered to "N≥8, whenever a public client
  makes `Wakeup()` genuinely cross-thread" at M3/P3. **P4a is that public client** — so
  `Wakeup()` is now a **public, genuinely cross-thread** API for the first time. The
  hardening items are therefore **now reachable in principle**. Decide in the STATUS
  handoff: either (a) keep them deferred as accepted-by-design residuals of the single-
  owner not-thread-safe contract (Python parity — its `wakeup` has no closed check at
  all), documented on the public `KafkaConsumer` as a not-thread-safe caveat; or (b)
  schedule the per-call `SafeHandle.DangerousAddRef`/`DangerousRelease` hardening as a
  **P4a-follow-up phase (N=9)**. **Recommend (a)** for P4a — keep the residuals accepted
  and documented (the canonical `wakeup()` usage — thread A blocked in `poll`, thread B
  wakes it, thread A disposes — does not race wakeup against dispose), matching the
  single-owner contract and the Python/CKD siblings; flag (b) as a candidate follow-up.
  **Flag this to the human in the handoff** — it is the one governance item P4a
  genuinely changes (the public client makes the residual reachable), and it is a
  judgment call, not a locked decision.
- **The three M3/P2 accepted residuals remain accepted-by-design** (misuse-only):
  teardown-with-unawaited-in-flight-op strand + one-time leak; `Wakeup`/`GroupId` TOCTOU;
  submit-vs-`destroy` handle race. P4a adds **no new residual** — composition inherits
  them unchanged; on-dispatcher copy-out (M3/P3) already avoids a new leak surface.
- **The §4 package-id pre-publish gate stays OPEN** (M0/P1) — P4a adds public types but
  does not publish; `IsPackable=false` holds it shut. Note in the handoff; do NOT resolve.

---

## 10 · Risks / confirmed decisions

**Confirmed (locked) decisions threaded through this plan:** value types 1–5;
interface + classes 6–7; ops 8–13; the constraints to preserve (5 invariants, single-
owner, on-dispatcher copy-out, `SeekAsync`-is-async, `byte[]` headers, additive
`IConsumer`, no Mode-B). See the task brief; this plan reflects them and does not
relitigate.

**Open micro-decisions A–D** (§6) — presented with recommendations for human
confirmation before implementation: A = unify Key/Value on `byte[]` (lean yes); B =
compose + inherent mock helpers; C = `Confluent.Kafka` public, marshallers internal;
D = non-generic now, generic later (additive).

**Risks:**

- **`GroupMetadata` full-field broker-free reachability.** `group_id` is reachable
  pre-join (M2/P1 D5 verified). `member_id`/`generation_id`/`group_instance_id` may
  only be meaningful post-join on a real broker; on a `MockConsumer` they carry
  broker-free defaults. **Mitigation:** the Actor verifies broker-free values against
  `src/consumer/mock_consumer.rs` early; assert the reachable fields + document the
  post-join-only ones (mirror M2/P1 D5). This does not block the full-field *type* — the
  four properties exist regardless; only their broker-free *values* are constrained.
- **`GroupMetadata` concurrent-overlap determinism (D-Q4 lineage).** The concurrent →
  `InvalidOperationException` mapping needs a guard-holding op to overlap; `poll` held
  open is the controllable op (M3/P3). If not deterministic broker-free, keep the
  reachable slice + verify the mapping by inspection and document (as M3/P3 D-Q4 did) —
  do NOT ship a flaky test.
- **CS1591 surface is large and new.** This is the first phase adding public types since
  M2/P1; ~11 public types × their members all need XML docs. **Mitigation:** promote the
  already-well-documented internal `ConsumerRecord`/`ConsumerRecords` docs; the value
  types are small. Budget a docstring pass in step 1.
- **`CloseAsync()` vs the `TryBeginClose` gate (RESOLVED — no timeout).** ABI-verified
  there is no async-timeout close, so P4a ships `CloseAsync()` (no `TimeSpan`); it takes
  the one-shot latch, closes via `CloseAsyncInternal` (surfacing the error), and
  destroys in a `finally` — a subsequent `Dispose`/`DisposeAsync` loses the latch and
  no-ops. `CloseAsync(TimeSpan)` is a deferred additive overload (§3 note).
- **`Wakeup()` becomes genuinely public/cross-thread (governance §9).** The one item
  P4a changes vs prior phases. Recommend keeping the residuals accepted + documented
  (option a); flag to the human.
- **`MockConsumer.AddRecord` public shape** (§3 note) — recommend the component-tuple
  form for P4a (no `ConsumerRecord` public ctor needed); flag for confirmation.

**No contradiction with any locked decision was found in the repo.** Every ABI function
the locked decisions rely on is present (§7 ABI list, all line-verified); the one gap
(the internal `SeekAsync` not validating `offset < 0`) is *within* locked decision 11's
scope — decision 11 explicitly calls for adding that validation — so it is a planned fix,
not a contradiction.
