# COMMENTS.17 — dotnet-critic review of M5/P8a (synchronous consumer — surface + core loop)

**Verdict: CLEAN — no issues filed.**

Scope reviewed: 5 commits on `prashah_dev_public_consumer_remaining_sync`
(`e0c88391` interop · `090775bb` NativeConsumer sync wrappers + shipped-async refactor ·
`6d4bff3d` public IConsumer/KafkaConsumer/MockConsumer · `809de249` tests ·
`b4c06784` governance/doc-sync). Ground truth: C ABI header
`target/include/confluent_kafka.h` + Java `Consumer` shape + the approved PLAN.

## Load-bearing rule (no sync-over-async) — PASS
Grepped + read `IConsumer.cs` / `KafkaConsumer.cs` / `MockConsumer.cs` /
`NativeConsumer.cs` sync wrappers: no `GetAwaiter().GetResult()` / `.Result` /
`.Wait()` / `Task.Run` anywhere in `src/`. Every sync method calls the sync C ABI
directly via `NativeMethods`; the sync and async families are siblings over one
`NativeConsumer`, not one wrapping the other. FFI-verified: `Consumer_poll` does
`h.runtime.block_on(consumer.poll(timeout))` inside the core's multi-thread runtime
(`src/ffi/consumer.rs:568`) — the shipped Seek/CurrentLag/EnforceRebalance precedent,
not the forbidden managed façade.

## HIGH-RISK #1 — shipped-async refactor regression — PASS (no behavioral change)
- `SubscribeWithCallback` (async, shipped): the inline pin loop → `WithPinnedTopicsOnly`
  is behaviorally identical — same call-scoped pin lifetime (pins created → native call
  inside the `try` → unpin in `finally`), same GCHandle/bridge ownership (still through
  `SubmitVoidOperation`, callback/userData untouched), same order.
- Tuple `Assign` (mock helper): `count == topicPartitions.Count`, `partitions.Length ==
  count`; `InvokePartitionOpSync` preserves the exact order (validate → `ThrowIfClosed`
  → `WithPinnedTopics` → P/Invoke → throw-iff-error). Identical to the pre-refactor inline
  body. `WithPinnedTopics` / `SnapshotPartitions` / `ExtractPartitions` are untouched.
- 282 pre-existing tests green + 20× suite loop clean confirm no observable change.

## HIGH-RISK #2 — Wakeup one-shot test honesty — PASS (genuine, honestly documented)
- (a) Mock-poll claim TRUE: `src/consumer/mock_consumer.rs:525` poll runs synchronously
  (drains one task, never awaits) and step 4 (`:540`) `compare_exchange(true,false)` is a
  sticky check-and-clear on an `AtomicBool` wakeup flag.
- (b) Tests genuinely prove the semantics: a broken wakeup fails test 1 (no throw →
  `Assert.Throws<KafkaException>` fails) and test 2 (`Assert.NotNull(observed)` fails).
  One-shot + reusability is real — the faulting poll returns at step 4 *before* step 7
  drains records, so the queued record survives to the follow-up poll (`Assert.Single`).
  The cross-thread test uses a bounded poll loop that deterministically catches the sticky
  flag (correctly handling the "wakeup lands after an instant poll returned" race).
- (c) The determinism ceiling (mock poll never blocks; a genuinely mid-flight interrupt is
  unreachable broker-free) is documented honestly in the test-class remarks, not papered
  over. Wakeup 15×-in-isolation + 20×-in-suite clean.

## P/Invoke + handle lifetime — PASS
10 DllImports match the header exactly (`int`/`long`/`IntPtr` per §0.1; `ConsumerPoll` →
`ConsumerRecords_t*` + `out IntPtr outError`; `ConsumerPosition` → `KafkaError*` +
`out long`; commit/subscribe/pause/resume/seek_to_* → `KafkaError*`). `Poll` is
copy-out-then-destroy exactly once: on success copy via `ConsumerRecordsMarshal.CopyOut`
then `finally`-destroy; on failure `records == null` (ABI contract) so the `finally`
`ConsumerRecordsDestroy(null)` is a verified no-op (`src/ffi/consumer.rs:794`
null-guard) and `KafkaException.FromHandle(error)` frees the error handle once. All
preconditions precede any pin/P-Invoke.

## Surface fidelity / teardown / DoD — PASS
- `IConsumer : IConsumerCommon, IDisposable`; exact P8a member set; **no** query-family
  member leaked in early; **no** `CancellationToken`; `Close()` + `Close(TimeSpan)`
  (negative → `ArgumentOutOfRangeException`, `TimeSpan.Zero` valid).
- `CloseSync`/`CloseSyncWithTimeout` share the `TryBeginClose` latch + `finally`-destroy;
  `_handle.Dispose()` → `ReleaseHandle` → bare `Consumer_destroy`, so `CloseSync` =
  `Consumer_close` (graceful join) + destroy = the existing `Dispose` shape (no
  double-close; idempotent with `Dispose`). SafeHandle UAF fix NOT folded in (out of scope,
  confirmed). Pure-sync ops have no in-flight-after-return window.
- `TestTimeout.Run(Action)` deviation sound: it uses `task.Wait(timeout)` which throws
  `AggregateException` on a faulted task, so faulting polls are called directly (mock poll
  never blocks → no hang risk); success-path polls retain the hang guard.
- Tests (DoD §3): round-trip (all fields incl. non-ASCII via out_len, tombstone/absent→null,
  empty→empty-non-null, multi-in-order, empty non-null `Count==0`, poll-error one-shot),
  Seek→Position, commit family, subscribe/unsubscribe, assign/pause/resume,
  seekToBeginning/End, preconditions with exact asserted messages, teardown, per-op alloc
  budget (measured synchronously on the caller thread — avoids the foreign-dispatcher-thread
  pitfall — with ~10× headroom), TFM smoke (sync legs). No skipped/weakened assertions.
  Concurrent-use documented as a mock limit per PLAN §8 (sanctioned, not a gap).
- Governance/doc-sync (DoD §1): `consumer-threading.md §1.1` amendment matches PLAN §9;
  `CLAUDE.md §3` sketch + §4 interface-naming un-defer; `STATUS.md` M5/P8a entry present.
- DoD §6/§7: helpers reused (`RunPartitionOpSync`/`InvokePartitionOpSync`/
  `WithPinnedTopicsOnly` shared by sync + refactored async); no orphaned helpers; no
  duplicated types.

## DoD gates run locally (net10.0 runtime)
- `cargo build --features ffi` — no `confluent_kafka.h` delta (Mode A confirmed by diff).
- `dotnet build` — 0 warnings / 0 errors across netstandard2.0 / net8.0 / net10.0 / net462
  (test build).
- `dotnet format --verify-no-changes` — clean.
- `dotnet test -f net10.0` — 346 passed / 0 failed; 20× suite loop all Failed=0;
  `PublicSyncConsumerWakeupTests` 15× isolation clean; `PublicSyncConsumerAllocationBudgetTests`
  15× isolation clean.

**M5/P8a meets the Definition of Done.** No comments to fix; nothing to move to
`COMMENTS.DONE.17.md`.
