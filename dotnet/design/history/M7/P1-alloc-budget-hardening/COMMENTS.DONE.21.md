# COMMENTS.21 — dotnet-critic review of M7/P1 "Allocation-budget test hardening"

Reviewed commits `568e71e4`, `b7b71512`, `c0571dab`, `9e94966a`
(base `30a10f02`) on `prashah_dev_public_consumer_tests_parity`. Test-only, Mode A.

## Verdict: CLEAN — no issues found. M7/P1 meets the Definition of Done.

No `COMMENTS` items filed (nothing to fix). Details below are the evidence, not findings.

### 1. No PR-#144 / `src` file in any commit — fix files left uncommitted (BLOCKING check — PASS)
- `git diff 30a10f02..HEAD --name-only` = STATUS.md + 11 alloc-budget test files only.
  No `src/` file, no `tests/…/AssemblyInfo.cs`, no `CONSUMER_TEST_COVERAGE_COMPARISON.md`.
- The three PR-#144 fix files (`NativeConsumer.cs`, `OperationCompletionSource.cs`,
  `AssemblyInfo.cs`) are all ` M` (modified-uncommitted, untouched in the working tree).
  Matches PLAN §5 Option 1 exactly.

### 2. HEADLINE gate — independently re-run (PASS)
- Built with the working-tree `AssemblyInfo` flip (`DisableTestParallelization=false`,
  uncommitted) and ran the FULL suite with **NO `--filter`**: 26 runs total (1 + 25),
  every run `Failed: 0, Passed: 437, Total: 437` on net10.0. Zero alloc-test failures,
  no host-crash under-count. Alloc tests confirmed parallel-safe.

### 3. Retained + converted tests are genuine (not false-confidence) (PASS)
- `PublicSyncConsumerAllocationBudgetTests.MeasurePoll` and the converted
  `PublicConsumerTypedAllocationBudgetTests.MeasurePoll` bracket the **synchronous**
  `consumer.Poll(...)` (no `await`) — the per-thread counter surrounds the on-caller-thread
  copy-out, so it measures real work (not ~0 off-thread).
- `PublicSyncConsumerQueryAllocationBudgetTests.Measure` brackets the sync
  `BeginningOffsets`/`PartitionsFor` loops on-thread.
- Marginal large−small subtraction + warmup preserved in all three; thresholds unchanged
  (1024 B / 4096 B / 1024 B). The typed test's 64 KiB-value → `int` marginal budget of
  1024 B/record still catches a value-sized intermediate `byte[]` (~64 KiB/record).

### 4. Converted typed test reaches the shared `CopyOut<K,V>` (PASS)
- Drives the **sync** `MockConsumer<byte[], int>` → `NativeConsumer.PollTyped<byte[],int>`
  (`NativeConsumer.cs:1127`) → `ConsumerRecordsMarshal.CopyOut<TKey,TValue>` — the identical
  generic marshaller the async path uses (`ConsumerCallbacks.cs:101`). `SpanLengthDeserializer`
  returns `data.Length` and allocates nothing (`TestDeserializers.cs:91`), so a value-sized
  copy would surface as the full delta.

### 5. Removals lose no unique per-record coverage (PASS)
- Async poll (`PublicConsumerAllocationBudgetTests`) + interop async poll
  (`Interop/ConsumerPollAllocationBudgetTests`): async duplicates of the retained sync Poll
  budget — same `CopyOut<K,V>`.
- `BeginningOffsets`/`PartitionsFor` per-op: retained sync query budgets cover the same
  `OffsetMapMarshal`/`PartitionInfoListMarshal.CopyOut` on-thread.
- `Position`/`Pause`/`Assignment(read)`/`SeekAndCurrentLag` per-op: §11-amortized per-RPC/
  per-call surfaces, no per-record marshaller path.
- No orphaned helpers/usings: kept helpers still referenced (`BeginningOffsetsOf` 6×,
  `PartitionsForOf` 9×, `PositionOf`/`ReadyForPosition` 22×, `PositionOf`/`ReadyAssigned` 12×);
  build 0 warnings / 0 errors; `dotnet format --verify-no-changes` exit 0. No leftover
  process-wide `GetTotalAllocatedBytes` measurement (remaining matches are doc-comment refs).

### 6/7/8. net462 guard + accepted gap + DoD (PASS)
- All three retained/converted files keep `#if NET8_0_OR_GREATER`; the net462 test leg builds
  clean (0 warnings) — no TFM-matrix regression.
- Accepted async-per-op gap documented in the STATUS §4 entry ("Consciously-accepted gap").
- Mode A confirmed: 4 commits test-only, `confluent_kafka.h` hash stable, no Rust/`src` change.
- `dotnet build` 0/0 all TFM legs; `dotnet format` clean.
