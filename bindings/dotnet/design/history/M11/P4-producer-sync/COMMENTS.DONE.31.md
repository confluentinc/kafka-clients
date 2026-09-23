# M11/P4 — Sync producer (Phase D) — closed record (N=31)

Manager-archived closed record for the M11/P4 execution loop. The companion of
`PLAN.md` in this directory: `PLAN.md` is the forward-looking plan; this file
records what happened *during* execution (Critic rounds, decisions, deviations).

## Loop outcome

**Closed CLEAN on the first pass — one Actor implementation, one Critic review, no
fix cycle.** No `COMMENTS.31.md` was ever created (the Critic found no real issues),
so there were no comments to resolve into a `COMMENTS.DONE.31.md` at the binding
root. This archive exists for the phase record, not because comments were moved.

- **Branch:** `prashah_dev_producer_sync` (stacks on `prashah_dev_producer_send`).
- **Commits (local only, not pushed):**
  - `8cbd5747` — `dotnet(M11/P4): sync producer — IProducer/KafkaProducer/MockProducer + NativeProducer sync ops (Mode A)`
  - `bbcfe393` — `dotnet(M11/P4): sync producer tests — Send/Peripheral/Teardown/MockControl/AllocationBudget/TfmSmoke`
  - (Manager archive commit `11dd96ef` carries the approved `PLAN.md`.)

## Critic N=31 — round 1: CLEAN (0 issues, all severities)

Reviewed `git diff 11dd96ef..HEAD` against the C ABI header, the Java `Producer`
public-API shape, the PLAN §3–§4 decisions #1–#7, and the `ffi-marshalling.md`
anti-patterns. The checks that mattered, all passed:

- **Handle lifecycle in `SendSync`** — future / metadata / error freed on **every**
  path (future in the outer `finally`; metadata in the inner `finally`, null on the
  error branch so its destroy no-ops; error freed by `FromHandle`). No leak, no
  double-free, no use-after-free.
- **SafeHandle-param convention (decision #3)** — `FlushSync` / `PartitionsForSync`
  pass `_handle` (call-scoped auto-ref); `Producer_flush` retyped `IntPtr →
  SafeProducerHandle` and **both** its callers (`StopPump`, `FlushSync`) updated —
  no stray raw-`IntPtr` caller left. `CloseSync` correctly keeps the raw-handle
  single-winner teardown shape (surfaces error; `Producer_destroy` in `finally`).
- **Zero-copy send** — reuses `ProducerSendMarshal.Send` verbatim (call-scoped pin,
  empty-vs-absent sentinel, non-null pointer for empty arrays).
- **No dead DllImport** — the singular `FutureRecordMetadata_destroy` is genuinely
  used by the single-future sync path.
- **NOT-adding list respected** — `IProducer` is `Send`/`Flush`/`PartitionsFor`/
  `Close` only, `: IDisposable` only; no `Task`/`CancellationToken`/`Close(TimeSpan)`/
  transactions/headers on the sync surface (the `Task`/`CancellationToken` tokens in
  the source are only doc-comment prose explaining their deliberate absence).
- **Preconditions** before any P/Invoke, with null-record/null-topic-before-disposed
  ordering tested; **error-message content asserted** (`ErrorNext` code AND message);
  the send-path allocation-budget test is meaningful (marginal large−small cancels
  value-size-independent allocs, catching a value-sized copy); TFM-smoke present.

**Memory-safety subtlety cleared:** `SendSync` blocks on `FutureRecordMetadata_get`
**without** a producer-handle ref. This is NOT the M11/P3 multi-writer `Send` UAF
(that race was on `Producer_send`, closed here by the `SafeProducerHandle`-param
auto-ref). The `get` is memory-safe with no ref because the future is Arc-backed
and independent of the producer, and `Producer_destroy` tolerates outstanding
futures (verified by prior core inspection — the M11/P3 round-2 "RESOLVED-SAFE
producer future-destroy-after-Producer_destroy" finding). Single-owner / not
thread-safe, like the sync consumer.

## Decisions confirmed at execution (PLAN #1–#7)

All seven approved decisions were implemented as planned:
1. Sync `Send` blocks, returns `RecordMetadata` directly (= `send().get()`).
2. Blocking `FutureRecordMetadata_get`, no pump / TCS / GCHandle / callback.
3. SafeHandle-param convention on the new sync ops; `Producer_flush` retyped;
   `Producer_partitions_for` fresh with `SafeProducerHandle`; `Close` teardown keeps
   the raw-handle release shape.
4. No `CancellationToken` on the sync surface.
5. `Close()` only — no `Close(TimeSpan)`.
6. Sibling types over one `NativeProducer`; pump-less sync teardown.
7. Inherits Option-C `buffer.memory` backpressure, no new handling.

## Recorded deviation

- **Mutation-after-`Send` test placement.** The plan (§7) grouped the
  mutation-after-`Send` assertion in `PublicSyncProducerSendAllocationBudgetTests.cs`.
  It was kept in that file but **ungated**: the allocation-budget `[Fact]` + helpers
  are `#if NET8_0_OR_GREATER`, while the mutation `[Fact]` is not — so it runs on all
  TFMs including net462. Minor placement choice, consistent with the async
  precedent's intent (prove the core copied key/value during the call on every TFM).

## Mode A + hygiene (Manager-verified)

- `git diff 11dd96ef..HEAD` over `src/**` / `src/ffi/**` / `target/include/confluent_kafka.h` / `cbindgen.toml` → **empty**. All 11 changed files under `bindings/dotnet/{src,tests}`.
- No excluded paths in either commit (no `COMMENTS.*`, no `.claude/agent-memory/**`, no repo-root `.claude/agents/dotnet-*.md` discovery copies, no `target-linux*`, no built `.so`/`.dylib`/`.dll`).
- Not pushed — local commits only (the user manages pushes).
