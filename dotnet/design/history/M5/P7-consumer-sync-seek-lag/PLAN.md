# M5/P7 — "Consumer sync seek + current-lag" (.NET binding)

Status: APPROVED (2026-08-07). Q1/Q2 resolved (see §11).

---

## 0 · Identity

- **Binding:** `.NET` (`bindings/dotnet/`)
- **Milestone / Phase:** M5 / P7 (binding-local numbering; M5/P1–P6 DONE — commit surface shipped 2026-08-06).
- **Assigned Actor/Critic number `N`:** **16** (`COMMENTS.16.md` / `COMMENTS.DONE.16.md`).
- **Mode:** **A** (`.NET-only`, CLAUDE.md §6.2) — all three ABI functions already exist in `target/include/confluent_kafka.h`; **no Rust core / ABI change** (`cargo build --features ffi` must show no header delta).
- **Personas:** `dotnet-actor` / `dotnet-critic` (copied to repo-root `.claude/agents/` per CLAUDE.md §8.4).

## 1 · Goal

Add two Python-aligned, **synchronous** consumer members, both Mode A, aligning the .NET consumer's
local-seek/lag surface with the Python sibling (`bindings/python/consumer.py` lines 279–296):

1. **`Seek` becomes SYNC, two overloads on `IConsumerCommon`:**
   - `void Seek(TopicPartition partition, long offset)` — calls the **sync** ABI
     `kafka_consumer_Consumer_seek` **directly** (not the async bridge). **Breaking change** to the
     existing `Task Seek(TopicPartition, long, CancellationToken)` (async→sync; also moves off
     `IAsyncConsumer` onto its base `IConsumerCommon`).
   - `void Seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata)` — **NEW** overload —
     calls the **sync** ABI `kafka_consumer_Consumer_seek_with_metadata` directly.
2. **`long? CurrentLag(TopicPartition partition)` on `IConsumerCommon`** — SYNC — calls the sync ABI
   `kafka_consumer_Consumer_current_lag`; maps Java's `OptionalLong.empty` → `null`.

Naming = Python/Java parity: `Seek`, `CurrentLag`.

## 2 · Locked decisions (fixed by the user)

- **Both `Seek` overloads are sync `void`, placed on `IConsumerCommon`** (the non-blocking shared
  base), matching Python — even though `seek` blocks in Java's `AsyncKafkaConsumer`
  (`addAndGet(new SeekUnvalidatedEvent(...))`). **Deliberate divergence from CLAUDE.md §4**
  ("Java `seek` blocks → async `Task`"). Rationale (documented §8): Python parity +
  `seek_with_metadata` has **no `_async` ABI variant**, so a sync method calling the existing sync
  ABI **directly** (no `Task.Run`, no sync-over-async wrapper) is the legitimate realization; the
  calling thread parks inside the core's `block_on` (deadlock-free — multi-thread runtime, ffi
  §A1/§B1), exactly as the shipped `EnforceRebalance`/`CommitAsync`/`UpdateOffset` sync-op paths.
- **`CurrentLag` is sync `long?` on `IConsumerCommon`.** The Rust core's `current_lag` is a
  **non-blocking local read** ("sync, never blocks", header line 3755). Also a §4 divergence (the
  §4 idiom-map row lists `currentLag` as blocking/async, and the §1/§4 "Mode B gap" note lists
  `current_lag` as an un-shippable gap) — both corrected (§9 doc-sync).
- **Naming:** `Seek`, `CurrentLag`.

## 3 · Exact public surface + interface placement

Move the two `Seek` overloads and `CurrentLag` onto **`IConsumerCommon`**
(`src/Confluent.Kafka/IConsumerCommon.cs`). Because `IAsyncConsumer : IConsumerCommon`, they remain
reachable through an `IAsyncConsumer` reference; only the *signature* of the existing `Seek(tp,long)`
changes (the breaking part).

```csharp
// On IConsumerCommon (non-blocking sync surface):
void Seek(TopicPartition partition, long offset);                              // Java seek(tp, long)
void Seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata);      // Java seek(tp, OffsetAndMetadata) — NEW
long? CurrentLag(TopicPartition partition);                                    // Java currentLag(tp) → OptionalLong; empty → null
```

**Removed from `IAsyncConsumer`** (`src/Confluent.Kafka/IAsyncConsumer.cs`): the current
`Task Seek(TopicPartition, long, CancellationToken)` member and its long Java-fidelity `<remarks>`.

Both client classes forward to the internal wrapper (drop the old async forwarders, add three sync):
- `src/Confluent.Kafka/AsyncKafkaConsumer.cs`
- `src/Confluent.Kafka/AsyncMockConsumer.cs`

```csharp
public void Seek(TopicPartition partition, long offset) =>
    _native.Seek(partition.Topic, partition.Partition, offset);
public void Seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata) =>
    _native.SeekWithMetadata(partition.Topic, partition.Partition, offsetAndMetadata);
public long? CurrentLag(TopicPartition partition) =>
    _native.CurrentLag(partition.Topic, partition.Partition);
```

## 4 · ABI verification (exact C signatures — verified in `target/include/confluent_kafka.h`)

```c
// line 1619 — sync seek; returns KafkaError* (null = success)
kafka_common_KafkaError_t *kafka_consumer_Consumer_seek(
    const kafka_consumer_Consumer_t *consumer, const char *topic, int32_t partition, int64_t offset);

// line 1650 — sync seek with metadata / leader epoch; returns KafkaError* (null = success)
// param order: (consumer, topic, partition, offset, leader_epoch, metadata)
// leader_epoch < 0 = "no leader epoch"; metadata == NULL = "no metadata"
kafka_common_KafkaError_t *kafka_consumer_Consumer_seek_with_metadata(
    const kafka_consumer_Consumer_t *consumer, const char *topic, int32_t partition, int64_t offset,
    int32_t leader_epoch, const char *metadata);

// line 2173 — sync current_lag; "sync, never blocks"
// true  → *out_lag written (lag known);  false → lag unknown OR guard not acquired → map to null
bool kafka_consumer_Consumer_current_lag(
    const kafka_consumer_Consumer_t *consumer, const char *topic, int32_t partition, int64_t *out_lag);
```

Rust FFI confirms (`src/ffi/consumer.rs`): `seek` → `sync_void_op(c.seek(tp, offset))` (2714);
`seek_with_metadata` builds `OffsetAndMetadata::with_leader_epoch(offset, epoch, metadata_str)` where
`epoch = leader_epoch < 0 ? None : Some(leader_epoch)` and `metadata NULL → String::new()`, then
`sync_void_op` (2753); `current_lag` → `acquire` fail → `false`; else `Some(lag) → *out_lag + true`,
`None → false` (3763).

## 5 · `NativeMethods` — new P/Invoke declarations + removal

Add to `src/Confluent.Kafka/Internal/Interop/NativeMethods.cs`:

```csharp
[DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_seek", CallingConvention = CallingConvention.Cdecl)]
internal static extern IntPtr ConsumerSeek(IntPtr consumer, IntPtr topic, int partition, long offset);

[DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_seek_with_metadata", CallingConvention = CallingConvention.Cdecl)]
internal static extern IntPtr ConsumerSeekWithMetadata(
    IntPtr consumer, IntPtr topic, int partition, long offset, int leaderEpoch, IntPtr metadata);

[DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_current_lag", CallingConvention = CallingConvention.Cdecl)]
[return: MarshalAs(UnmanagedType.I1)]
internal static extern bool ConsumerCurrentLag(IntPtr consumer, IntPtr topic, int partition, out long outLag);
```

The `IntPtr` return of the two seek fns is a `kafka_common_KafkaError_t*` handle consumed by
`KafkaException.FromHandle` (null = success), like `ConsumerEnforceRebalance`.

**Removal (Q2 = REMOVE):** delete the `ConsumerSeekAsync` DllImport (no remaining consumer once seek
is sync). The Rust `Consumer_seek_async` symbol stays in the header (Rust-owned; Mode A = no Rust
change); we simply stop declaring it on the C# side.

## 6 · `NativeConsumer` — new sync methods + removal (`src/Confluent.Kafka/Internal/NativeConsumer.cs`)

**Remove (Q2):** `SeekWithCallback(string, int, long, CancellationToken)` and its async doc block.

**Add** three sync methods, structurally identical to the shipped `EnforceRebalance` / `CommitAsync`
/ `UpdateOffset` sync-op discipline — `ThrowIfClosed()` → pin call-scoped → P/Invoke →
`KafkaException.FromHandle` throw-iff-non-null. No `GCHandle`, no completion bridge, no
`CancellationToken`.

```csharp
internal void Seek(string topic, int partition, long offset)
{
    if (topic is null) throw new ArgumentNullException(nameof(topic));
    if (partition < 0) throw new ArgumentOutOfRangeException(nameof(partition), partition, "Partition must not be negative.");
    // Q1 = KEEP the Java-fidelity negative-offset guard. Java AsyncKafkaConsumer.seek throws
    // IllegalArgumentException("seek offset must not be a negative number") BEFORE the call.
    // This is the ONE place .NET is stricter than Python (Python's sync seek does no offset
    // validation). Validated before the P/Invoke (§B5); exact message asserted by tests (DoD §3).
    if (offset < 0) throw new ArgumentOutOfRangeException(nameof(offset), offset, "seek offset must not be a negative number");
    ThrowIfClosed();
    using Utf8Marshal.PinnedUtf8String topicPin = Utf8Marshal.Pin(topic);
    KafkaException? failure = KafkaException.FromHandle(
        NativeMethods.ConsumerSeek(_handle.DangerousGetHandle(), topicPin.Pointer, partition, offset));
    if (failure is not null) throw failure;
}

internal void SeekWithMetadata(string topic, int partition, OffsetAndMetadata offsetAndMetadata)
{
    if (topic is null) throw new ArgumentNullException(nameof(topic));
    if (partition < 0) throw new ArgumentOutOfRangeException(nameof(partition), partition, "Partition must not be negative.");
    if (offsetAndMetadata is null) throw new ArgumentNullException(nameof(offsetAndMetadata));
    // Offset cannot be negative — the OffsetAndMetadata ctor already rejects it ("Invalid negative
    // offset"); the ctor is the upstream gate, so no offset guard here.
    ThrowIfClosed();
    int leaderEpoch = offsetAndMetadata.LeaderEpoch ?? -1;                 // Python: epoch or -1
    using Utf8Marshal.PinnedUtf8String topicPin = Utf8Marshal.Pin(topic);
    using Utf8Marshal.PinnedUtf8String metaPin = Utf8Marshal.Pin(offsetAndMetadata.Metadata); // never-null ("")
    KafkaException? failure = KafkaException.FromHandle(
        NativeMethods.ConsumerSeekWithMetadata(
            _handle.DangerousGetHandle(), topicPin.Pointer, partition,
            offsetAndMetadata.Offset, leaderEpoch, metaPin.Pointer));
    if (failure is not null) throw failure;
}

internal long? CurrentLag(string topic, int partition)
{
    if (topic is null) throw new ArgumentNullException(nameof(topic));
    if (partition < 0) throw new ArgumentOutOfRangeException(nameof(partition), partition, "Partition must not be negative.");
    ThrowIfClosed();
    using Utf8Marshal.PinnedUtf8String topicPin = Utf8Marshal.Pin(topic);
    return NativeMethods.ConsumerCurrentLag(_handle.DangerousGetHandle(), topicPin.Pointer, partition, out long lag)
        ? lag : (long?)null;
}
```

Notes for the Actor:
- `OffsetAndMetadata.Metadata` is never-null (coerced to `""`), so always pass a valid pinned
  pointer — matching Python's `offset.metadata or ""`; do NOT special-case NULL.
- `current_lag`'s `false` means *either* unknown lag *or* concurrent-guard-rejection → both map to
  `null`. **Python parity** (`bindings/python/consumer.py:279` returns the raw value; `None` on
  false). `CurrentLag` does **not** get the `InvalidOperationException` concurrent-state-read
  treatment that `GroupId`/`Assignment` get. Document; Critic verifies against Python.
- Same accepted single-owner check-then-use handle TOCTOU vs a concurrent teardown as
  `EnforceRebalance`/`CommitAsync` (documented residual, not a new risk).

## 7 · Error / precondition model (ffi §B5)

- **Operational (seek):** sync ABI returns a `KafkaError*` → `KafkaException.FromHandle` (frees the
  handle once) → `throw` iff non-null. Canonical broker-free failure = seeking an **unassigned**
  partition → **synchronous** `KafkaException` (replaces the old faulted-`Task`).
- **Operational (current_lag):** no error handle — `false` → `null` (never throws for unknown/rejected).
- **Preconditions** (before any pin/P-Invoke): null topic → `ArgumentNullException`; negative
  partition → `ArgumentOutOfRangeException`; null `OffsetAndMetadata` → `ArgumentNullException`;
  negative offset on `Seek(tp,long)` → `ArgumentOutOfRangeException` (Q1, exact Java message);
  post-dispose → `ObjectDisposedException` (`ThrowIfClosed()`).
- **UTF-8 marshalling (ffi §B3):** topic + metadata via `Utf8Marshal.Pin`, **call-scoped**, released
  in `using`/`finally`.
- **Blocking:** the seek calls park the caller inside the core's `block_on`. Acceptable for a
  genuinely-sync method — **no `Task.Run` wrapper**, so not sync-over-async (ffi §B7); identical to
  the shipped `EnforceRebalance`/`UpdateOffset` sync-op paths.

## 8 · §4 divergence — to document (code comments + this PLAN + COMMENTS.DONE.16)

CLAUDE.md §4 maps "Java `seek` blocks → async `Task`", but this phase ships `Seek`/`CurrentLag`
**sync** because (a) Python exposes them synchronously in the shared base, (b) `seek_with_metadata`
and `current_lag` have **no `_async` ABI variant**, and (c) a sync method calling the sync ABI
**directly** (no `Task.Run`) is legitimate — not the sync-over-async footgun §4's "Mode B gap" note
guarded against. Shipping them as genuinely-sync members removes the gap without that footgun.

## 9 · CLAUDE.md doc-sync (`bindings/dotnet/CLAUDE.md`) — required, DoD §1

1. **§3 consumer sketch** — move `Seek`/`CurrentLag` to the `IConsumerCommon` block (both `Seek`
   overloads + `CurrentLag`); drop `Seek` from `IAsyncConsumer`'s "Already wired async"; update the
   "Already wired" prose to include sync seek + current-lag.
2. **§4 idiom-map row** — remove `seek` and `currentLag` from the blocking-async trigger row.
3. **§4 "Stays sync on the consumer — exactly these"** — add `Seek(...)` (both overloads) and
   `CurrentLag(...)`.
4. **§1 status & §4 "The ABI must be able to honor it" note** — the note lists `current_lag` /
   `seek_with_metadata` as sync-only **gaps**; update to **shipped sync (Mode A)**. Keep
   `close_with_timeout` in the gap note (still unaddressed).
5. Async `seek` no longer appears; `SeekToBeginning`/`SeekToEnd` remain async (unchanged).

## 10 · Test plan (`tests/Confluent.Kafka.UnitTests/`)

**Migrations (async→sync):**
- Public `.Seek(...)` callers drop `await`/`Task`: `PublicConsumerRoundTripTests.cs:53`,
  `PublicConsumerTfmSmokeTests.cs:49`, `PublicConsumerAllocationBudgetTests.cs:91`,
  `PublicConsumerPositionTests.cs:51`.
- `PublicConsumerApiTests.cs:78` (unassigned → fault): → synchronous
  `Assert.Throws<KafkaException>(() => consumer.Seek(new TopicPartition("unassigned", 0), 0L))`.
- `PublicConsumerApiTests.cs:104,119` (negative offset): **KEEP** as
  `Assert.Throws<ArgumentOutOfRangeException>` with the exact message (Q1).
- `PublicConsumerTeardownTests.cs:103` (post-dispose): → `Assert.Throws<ObjectDisposedException>`.
- **Interop `SeekWithCallback` setup callers → sync `Seek`:** `ConsumerPollWakeupCancelTests.cs:58`,
  `ConsumerPollReceivePathTests.cs:55`, `ConsumerPollHeadersTests.cs:64`,
  `ConsumerPollAllocationBudgetTests.cs:120` → `consumer.Seek(Topic, Partition, 0)` (no `await`).
- `ConsumerUnsubscribeSeekGroupMetadataTests.cs:99–136` (negative-offset / negative-partition
  preconditions): repoint to sync `Seek`; negative-offset KEPT (Q1); negative-partition stays
  `ArgumentOutOfRangeException`.
- `ConsumerCompletionBridgeTests.cs:68,84`: re-express the unassigned case as a **synchronous**
  `KafkaException` throw (churned → synchronous throws in a loop, no corruption). The void
  completion bridge remains proven by subscribe/unsubscribe/commit — do not lose bridge coverage.
- `ConsumerAsyncOperationTests.cs:101` (`SeekWithCallback_PreCanceledToken_...`): sync `Seek` has no
  `CancellationToken` → **remove** the Seek-specific test; confirm the pre-canceled behavior remains
  covered by a surviving async op (Poll/Subscribe/Commit).

**New tests (new file `PublicConsumerSeekLagTests.cs`, `AsyncMockConsumer`, no broker):**
- **`Seek(tp, long)` success** — offset observable via `Position(tp)` (Assign → Seek → Position == offset).
- **`Seek(tp, OffsetAndMetadata)`** — offset round-trips (Assign → Seek(tp, new OffsetAndMetadata(42,
  "meta", 7)) → Position(tp) == 42, or Commit()→Committed()→offset 42). **Reachability limit
  (verified):** the mock's `seek_with_metadata` (`src/consumer/mock_consumer.rs:746–755`) uses only
  `.offset()` and **discards metadata + leader_epoch**, so their **values are NOT observable
  broker-free**. The metadata/leader-epoch **marshalling** (call-scoped pins, `-1` epoch sentinel
  for `null`, non-ASCII metadata, `""` for empty) is exercised by asserting the call **succeeds
  without error**; strict value read-back is a **documented mock limit** (M5/P4 `Committed`
  non-empty-deferral precedent). Critic confirms against `mock_consumer.rs:753`.
- **Marshalling coverage:** leader-epoch present (`7`) + null (→ `-1`); metadata non-ASCII; metadata
  empty `""` — each asserts success on an assigned partition.
- **`CurrentLag` real value:** Assign([tp]) → `UpdateEndOffset(tp, 100)` → `Seek(tp, 10)` →
  `CurrentLag(tp) == 90` (mock: `Some(end - position)`; also exercises the new sync `Seek`). Verify
  against `mock_consumer.rs:438–457`.
- **`CurrentLag` empty → null:** on an **unassigned** partition → `null`. Optionally cover
  assigned-but-no-end-offset → `0` (caught-up model).
- **Preconditions + error messages (DoD §3):** null topic → `ArgumentNullException`; negative
  partition → `ArgumentOutOfRangeException` (message asserted); null `OffsetAndMetadata` →
  `ArgumentNullException`; negative offset (Q1, exact message); post-dispose →
  `ObjectDisposedException` — for all members, each **before any native call**.
- **Unassigned-partition seek** (both overloads) → synchronous `KafkaException` (code/message asserted).
- **Allocation-budget test** (DoD §10 / ffi §B4): a `Seek(tp,long)` / `CurrentLag` adds only the
  call-scoped topic-UTF-8 encode (no per-call heap beyond the pin).
- **TFM smoke:** `PublicConsumerTfmSmokeTests.cs` still round-trips on net462/net8.0/net10.0.

## 11 · Resolved decisions

- **Q1 = KEEP** the Java-fidelity negative-offset guard on `Seek(tp, long)`
  (`ArgumentOutOfRangeException` with `"seek offset must not be a negative number"`; keep the shipped
  test assertions). Documented as the one place .NET is stricter than Python.
- **Q2 = REMOVE** the now-dead `ConsumerSeekAsync` DllImport and `NativeConsumer.SeekWithCallback`
  (no remaining consumer once seek is sync).

## 12 · Mode A confirmation & Definition of Done gates

- **Mode A:** all three ABI fns present (verified §4); **`cargo build --features ffi` shows no
  `target/include/confluent_kafka.h` delta** (Actor diffs before/after).
- `dotnet build` — **0 warnings / 0 errors** on all TFM legs (net462-via-netstandard2.0, net8.0, net10.0).
- `dotnet test` — green (migrated + new).
- `dotnet format` — clean.
- DoD §1 (CLAUDE.md doc-sync, §9), §3 (tests migrated + error-message + non-ASCII UTF-8 + mock
  reachability recorded), §7 (dead code removed — Q2), §11 (no `#[async_trait]`/enum-dispatch
  concerns; no `block_on` sync façade — the sync ABI is called directly).

## 13 · Risks / Deviations

- **Breaking change** to `Seek(tp,long)`: async→sync + interface relocation (`IAsyncConsumer` →
  `IConsumerCommon`). Acceptable — pre-publish, no external implementers — but MUST be called out in
  `COMMENTS.DONE.16` + STATUS.md.
- **§4 divergence** (sync `seek`/`currentLag`) documented (§8); the §4 "Mode B gap" note corrected (§9).
- **Mock reachability limit:** `seek_with_metadata` metadata/leader-epoch values not observable
  broker-free (`mock_consumer.rs:753`) — round-trip test asserts offset + marshalling success, defers
  strict value read-back with rationale (M5/P4 precedent).
- **`CurrentLag` conflates "unknown" and "concurrent-rejection" as `null`** — Python parity, no
  `InvalidOperationException` split; documented.

## 14 · Comment workflow & handoff (Manager)

- Loop per `agent-roles.md`: Actor 16 implements → Critic 16 reviews each commit (`cargo xtask
  await-commit`), writes real issues (no false positives) to `COMMENTS.16.md` (file-locked) →
  Manager summarizes → Actor 16 fixes, moving resolved items to `COMMENTS.DONE.16.md` → repeat until
  `COMMENTS.16.md` empty and DoD passes.
- On completion: update `design/current/STATUS.md` (new M5/P7 entry) + `design/current`
  structure/design, add the three members to tracking, copy `COMMENTS.DONE.16.md` to
  `design/history/M5/P7-consumer-sync-seek-lag/`, reset `COMMENTS.16.md`.
