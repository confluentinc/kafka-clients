# M5/P8 — "Synchronous consumer (`IConsumer` / `KafkaConsumer` / `MockConsumer`)" (.NET binding)

Status: APPROVED by the maintainer (2026-08-07). Four decisions locked (§0). Split into
**P8a** (N=17, surface + core loop) then **P8b** (N=18, query family). MUST-VERIFY blocker
(sync poll observes wakeup) — **RESOLVED** before implementation (§7).

---

## 0 · Identity & locked decisions

- **Binding:** `.NET` (`bindings/dotnet/`)
- **Branch:** `prashah_dev_public_consumer_remaining_sync`, off `prashah_dev_public_consumer_remaining` (M5/P7 HEAD, `0bce1866`).
- **Milestone / Phase:** M5 / P8 (binding-local; M5/P1–P7 DONE; the earlier P8 "timed async close" draft was cancelled — number reused). **Split: P8a (surface + core loop), P8b (query family).**
- **Assigned N:** **17** (P8a), **18** (P8b).
- **Mode:** **A** (`.NET-only`) — every op has a sync C ABI variant already in the header; **no Rust change**.

**Locked decisions (maintainer):**
1. **Phasing:** two sub-phases — P8a (N=17) = interface + core loop; then P8b (N=18) = query family. P8a first.
2. **Include `Close(TimeSpan)`** (sync `close_with_timeout` ABI, no Rust dep; negative → `ArgumentOutOfRangeException`, `TimeSpan.Zero` valid).
3. **No `CancellationToken`** on the sync blocking methods — interruption via `Wakeup()` only (Java-faithful).
4. **Governance signed off:** proceed WITH the doc-hygiene amendment to repo-root `.claude/rules/consumer-threading.md` §1 (human sign-off satisfied) + routine `bindings/dotnet/CLAUDE.md` §3/§4 doc-sync. "Similar to Python" = **shape, not plumbing** (Python drives the async ABI + interruptible wait; .NET calls the **sync C ABI directly** — intentional and correct).

- **Out of scope (do NOT fold in):** the SafeHandle use-after-free fix (Emanuele's separate branch); generics / typed `IConsumer<TKey,TValue>` / user deserializers (bytes-only); `client_id()`.

## 1 · Goal

Add the **synchronous consumer surface** — the blocking mirror of the shipped async consumer, the most Java-faithful shape (Java's `Consumer` is synchronous):

- **`IConsumer : IConsumerCommon, IDisposable`** — sync interface (blocking mirror of `IAsyncConsumer`).
- **`KafkaConsumer`** — sync KIP-848 impl (mirror of `AsyncKafkaConsumer`).
- **`MockConsumer`** — sync mock (mirror of `AsyncMockConsumer`).

Un-defers the documented "async-only, no sync facade" stance — governance amendment in §9.

## 2 · Verified ABI coverage — every op has a sync variant (Mode A)

Sync ops return `KafkaError*` (null = success); result ops write an owned container handle to an out-param (or direct-return for `poll`):

| Op | Sync ABI (line) | Result shape |
|---|---|---|
| poll | `Consumer_poll` (606) | returns `ConsumerRecords_t*` + `KafkaError** out_error` |
| subscribe / unsubscribe | (1555) / (1581) | `KafkaError*` |
| assign | (850) | `KafkaError*` |
| pause / resume | (1721) / (1749) | `KafkaError*` |
| seek_to_beginning / seek_to_end | (1665) / (1693) | `KafkaError*` |
| position | (1900) | `KafkaError*` + `int64_t* out_position` |
| commit_sync / commit_sync_offsets | (1778) / (1804) | `KafkaError*` |
| committed | (1932) | `KafkaError*` + `OffsetMap_t** out_map` |
| offsets_for_times | (1965) | `KafkaError*` + `OffsetAndTimestampMap_t** out_map` |
| beginning_offsets / end_offsets | (1999) / (2031) | `KafkaError*` + `LongOffsetMap_t** out_map` |
| partitions_for | (2062) | `KafkaError*` + `PartitionInfoList_t** out_list` |
| list_topics | (2089) | `KafkaError*` + `TopicPartitionInfoMap_t** out_map` |
| close / close_with_timeout | (1865) / (1875) | `KafkaError*` |
| seek / seek_with_metadata / current_lag / assignment / subscription / paused / wakeup / enforce_rebalance | shipped sync (M5/P1, P7) | — |

**Consequence:** the sync query ops are *cleaner* than the async ones — no completion bridge, no `GCHandle`, no callback. Each is the shipped sync-op discipline (`EnforceRebalance`/`Seek` precedent) + copy-out-then-destroy via the **existing** marshallers (`ConsumerRecordsMarshal`, `OffsetMapMarshal`, `OffsetAndTimestampMapMarshal`, `LongOffsetMapMarshal`, `PartitionInfoListMarshal`, `TopicPartitionInfoMapMarshal`).

## 3 · Load-bearing implementation rule

Sync methods call the **sync C ABI directly**; the core's `block_on` runs inside the Rust multi-thread runtime, so the caller's thread parks — deadlock-free (M5/P7 precedent).

**Forbidden (Critic rejects on sight):** `…Async(...).GetAwaiter().GetResult()` / `.Result` / `.Wait()`; `Task.Run(...)` wrapping; any managed `block_on` façade over `AsyncKafkaConsumer`. The two client families are **siblings** over the same `NativeConsumer`, not one wrapping the other.

## 4 · Public surface (locked)

`IConsumerCommon` (shared, already sync: `Wakeup`, `Assignment`, `Subscription`, `Paused`, `GroupMetadata`, `EnforceRebalance`, `CommitAsync`, `Seek`×2, `CurrentLag`) reused unchanged. `IConsumer` adds the blocking forms:

```csharp
public interface IConsumer : IConsumerCommon, IDisposable
{
    ConsumerRecords Poll(TimeSpan timeout);
    void Subscribe(IReadOnlyCollection<string> topics);
    void Unsubscribe();
    void Assign(IReadOnlyCollection<TopicPartition> partitions);
    void Pause(IReadOnlyCollection<TopicPartition> partitions);
    void Resume(IReadOnlyCollection<TopicPartition> partitions);
    void SeekToBeginning(IReadOnlyCollection<TopicPartition> partitions);
    void SeekToEnd(IReadOnlyCollection<TopicPartition> partitions);
    long Position(TopicPartition partition);
    void Commit();
    void Commit(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets);
    // ---- P8b (query family) ----
    IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> Committed(IReadOnlyCollection<TopicPartition> partitions);
    IReadOnlyDictionary<TopicPartition, OffsetAndTimestamp> OffsetsForTimes(IReadOnlyDictionary<TopicPartition, long> timestampsToSearch);
    IReadOnlyDictionary<TopicPartition, long> BeginningOffsets(IReadOnlyCollection<TopicPartition> partitions);
    IReadOnlyDictionary<TopicPartition, long> EndOffsets(IReadOnlyCollection<TopicPartition> partitions);
    IReadOnlyList<PartitionInfo> PartitionsFor(string topic);
    IReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>> ListTopics();
    // ---- teardown ----
    void Close();
    void Close(TimeSpan timeout);
}
```

**P8a members:** everything above the "query family" marker + `Close` / `Close(TimeSpan)`. **P8b members:** the six query ops. The interface grows additively across the two sub-phases (pre-publish, no external implementers).

### 4.1 No `CancellationToken` (locked). Interruption via `Wakeup()`; sync `Poll` raises one-shot Wakeup (Java `WakeupException`). More Java-faithful than the async surface.
### 4.2 `Close(TimeSpan)` FREE — sync `close_with_timeout` exists (no Rust dep, unlike the cancelled async P8). Negative → `ArgumentOutOfRangeException`; `TimeSpan.Zero` valid.
### 4.3 `Commit()`/`Commit(offsets)` (blocking) distinct from `IConsumerCommon.CommitAsync()` (fire-and-forget). `Seek`×2/`CurrentLag` inherited from `IConsumerCommon`.

## 5 · `NativeMethods` — new sync DllImports

Reuse the already-declared sync: `close`, `close_with_timeout`, `destroy`, `wakeup`, `group_metadata`, `assign`, `assignment`, `subscription`, `paused`, `enforce_rebalance`, `seek`, `seek_with_metadata`, `current_lag`.

**Add (P8a):** `ConsumerPoll(IntPtr, long, out IntPtr outError) → IntPtr`; `ConsumerSubscribe`; `ConsumerUnsubscribe`; `ConsumerPause`; `ConsumerResume`; `ConsumerSeekToBeginning`; `ConsumerSeekToEnd`; `ConsumerPosition(…, out long) → IntPtr`; `ConsumerCommitSync`; `ConsumerCommitSyncOffsets`. **Add (P8b):** `ConsumerCommitted` / `ConsumerOffsetsForTimes` / `ConsumerBeginningOffsets` / `ConsumerEndOffsets` / `ConsumerPartitionsFor` / `ConsumerListTopics` (each `… out IntPtr outHandle) → IntPtr`). Full ABI symbols as `EntryPoint`, `Cdecl`; parallel-array input shapes match the async DllImports.

## 6 · `NativeConsumer` — new sync wrappers (reuse helpers; no duplication, DoD §6)

Reuse the shipped sync-op discipline, state-read pattern, snapshot/pin helpers (`SnapshotPartitions`, `ExtractPartitions`, `WithPinnedTopics`, `WithPinnedTopicsAndTimestamps`, `WithPinnedCommitOffsets`, `SnapshotCommitOffsets`), and copy-out marshallers.

- **P8a wrappers:** `Poll(TimeSpan) → ConsumerRecords` (copy-out-then-destroy via `ConsumerRecordsMarshal`; negative-timeout precondition per `PollWithCallback`); `Subscribe`/`Unsubscribe`/`Pause`/`Resume`/`SeekToBeginning`/`SeekToEnd` (void); `Position → long` (out-param); `CommitSync()`/`CommitSyncOffsets(offsets)`; `CloseSync()` → `ConsumerClose` + destroy **surfacing** the error, and `CloseSyncWithTimeout(ms)` → `ConsumerCloseWithTimeout` + destroy surfacing — both share the `TryBeginClose` latch + `finally`-destroy (idempotent with `Dispose`). Verify/reuse any existing sync `Assign` wrapper (the sync `Consumer_assign` DllImport is declared).
- **P8b wrappers:** `Committed`/`OffsetsForTimes`/`BeginningOffsets`/`EndOffsets`/`PartitionsFor`/`ListTopics` — out-param handle → existing marshaller → destroy.

## 7 · Wakeup / single-owner / teardown — MUST-VERIFY RESOLVED

- **Wakeup-observes-sync-poll: VERIFIED (blocker cleared, 2026-08-07).** `Consumer_wakeup` (FFI L523) → `handle.wakeup_handle.wakeup()` fires the same rotating `WakeupTrigger` watch-channel token + bg-task notify that `AsyncKafkaConsumer::wakeup` uses (consumer-threading.md §11). Sync `Consumer_poll` (FFI L554) does `h.runtime.block_on(consumer_mut(h).poll(timeout))` — blocking on the **same** `poll()` future the async path awaits, which returns `Err(Wakeup)` when that token cancels. So a `Wakeup()` from another thread interrupts a blocking sync `Poll` → `KafkaException` (Wakeup), one-shot (token rotates). The no-CT/Wakeup model is ABI-supported. **A regression test is still required (§8).**
- **Single-owner:** a sync op holds the core access guard for the blocking call; a concurrent op (another thread) → `KafkaException` (ConcurrentModification) thrown **synchronously** (ffi §B5).
- **Teardown:** shares `SafeConsumerHandle`/`Consumer_destroy`. **Sync ops have no in-flight-after-return window** → the async-op UAF (other branch) does NOT apply to the pure-sync path. Do NOT fold in that fix. `Dispose()`/`Close()` idempotent + use-after-dispose gated by the atomic closed flag.

## 8 · Test plan (P8a)

New sync-consumer test files, all `MockConsumer`, no broker:
- **Round-trip:** `Subscribe`/`Assign` → `MockConsumer.AddRecord` → `Poll` returns owned records; `Assign` → `Seek` → `Position`; `Commit(offsets)` → (P8b `Committed` reads back — deferred to P8b); `Commit()`/`Commit(empty)` broker-free.
- **Blocking Poll:** records when present; empty non-null `ConsumerRecords` (`Count == 0`) on empty poll.
- **Wakeup one-shot (REQUIRED):** thread A blocked in `Poll(30s)`, thread B `Wakeup()` → thread A throws `KafkaException` (Wakeup); a subsequent `Poll` is not still-woken. Bounded by `TestTimeout` (a missed wakeup fails fast, never hangs). Run the suite multiple times for stability (threaded test).
- **Preconditions + exact messages (DoD §3):** null topics/partitions/offsets → `ArgumentNullException`; null element topic → `ArgumentException`; negative partition → `ArgumentOutOfRangeException`; negative `Poll`/`Close` `TimeSpan` → `ArgumentOutOfRangeException`; each **before any native call** (assert even when closed). Unassigned-partition `Position`/`Seek` → synchronous `KafkaException`.
- **Concurrent-use:** second op from another thread while one blocks → `KafkaException` (ConcurrentModification); if not broker-free-observable on the mock, document as a mock limit.
- **Teardown:** `Close()` then `Dispose` → no double-close/destroy; `Close(TimeSpan)`; use-after-close contract matched to the async precedent.
- **Per-op allocation budget:** a `Poll` receive-path budget test mirroring `ConsumerPollAllocationBudgetTests` (receive-path zero-copy, consumer-threading.md §27).
- **TFM smoke:** sync consumer round-trips on net462/net8.0/net10.0.

## 9 · Governance amendment (approved)

- **`bindings/dotnet/CLAUDE.md` §3:** add the `IConsumer`/`KafkaConsumer`/`MockConsumer` sketch beside the async trio (bytes-only, no-CT, `Close`+`Close(TimeSpan)`).
- **`bindings/dotnet/CLAUDE.md` §4 (interface-naming row):** un-defer the sync mirror — "shipped"; sync/async split carried by interface+type (no `Async` suffix on methods), Java-faithful.
- **`.claude/rules/consumer-threading.md` §1** (repo-root; maintainer signed off): add a subsection recording that (1) the sync facade is now added **in the .NET binding** because Java's `Consumer` is synchronous (the most Java-faithful surface; users expect blocking); (2) implemented via **direct sync-C-ABI calls**, where `block_on` runs **inside the Rust core's multi-thread runtime** — **distinct from and not** the forbidden *managed* `block_on`/`Task.Run`/`GetResult` façade over the async binding API (§1's original deadlock/forced-sync-trait concerns don't apply: bytes-only has no user trait; parking is on the core's runtime, not the caller's); (3) the **Rust public API remains async-only**; this governs the binding layer only.

## 10 · DoD gates (P8a)

- `cargo build --features ffi` — no `target/include/confluent_kafka.h` delta (Mode A; Actor diffs before/after).
- `dotnet build` — 0 warnings / 0 errors, all TFM legs.
- `dotnet test` — green (Wakeup-one-shot + blocking-Poll bounded by `TestTimeout`; run the suite multiple times).
- `dotnet format` — clean.
- DoD §1 (governance §9 + doc-sync), §3 (tests + asserted messages + reachability recorded), §6 (reuse helpers/marshallers, no duplication), §7 (no dead code), §11 (**no `GetAwaiter().GetResult()`/`Task.Run`/managed `block_on`** — direct sync-ABI only).

## 11 · Risks / Deviations

- **Un-defers a documented cross-project stance** — repo-root §1 amendment (maintainer signed off).
- **Shared teardown / SafeHandle** — shares `SafeConsumerHandle`/`Consumer_destroy`; UAF fix out of scope (separate branch); pure-sync path has no in-flight-after-return window (note, don't fix).
- **Wakeup-observes-sync-poll** — verified (§7); regression test still required.
- **More Java-faithful than the current async-only .NET surface** — intentional; Java is the shape target.
- **Size** — mitigated by the P8a/P8b split + reuse of shipped machinery.
- **Mock reachability limits** (`offsets_for_times` unsupported [P8b]; concurrent-use observability) — document, assert reachable behavior only (M5/P4/P7 precedent).

## 12 · Comment workflow & handoff (Manager)

Per `agent-roles.md`, per sub-phase: `dotnet-actor N` (incremental commits `dotnet(M5/P8a): …`) → `dotnet-critic N` (review vs the C ABI header + Java `Consumer`; focus: no sync-over-async, copy-out-then-destroy handle lifetime, Wakeup one-shot, governance-amendment correctness) → fix cycle until `COMMENTS.N.md` empty + DoD passes → archive `COMMENTS.DONE.N.md` under `design/history/M5/P8-sync-consumer/`, update STATUS.md, reset `COMMENTS.N.md`. P8b (N=18) starts after P8a closes.
