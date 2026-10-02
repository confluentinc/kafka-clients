# M7/P1 — "Allocation-budget test hardening" (.NET binding)

Status: APPROVED (maintainer 2026-08-11). **Option 1** chosen: leave the PR #144 SafeHandle fix
**uncommitted** in the working tree — do NOT commit it. Test-only, Mode A.

---

## 0 · Identity

- **Binding:** `.NET` (`bindings/dotnet/`)
- **Milestone / Phase:** **M7 / P1** — small, **test-only** hardening phase.
- **Assigned N (monotonic):** **21**.
- **Branch:** `prashah_dev_public_consumer_tests_parity`.
- **Mode:** **A**, **test-only** — no `src` / production change, no `confluent_kafka.h` delta; changes confined to `tests/`.
- ⚠ **Working-tree constraint (Option 1, §5):** the PR #144 SafeHandle fix is uncommitted in three
  files — `src/Confluent.Kafka/Internal/NativeConsumer.cs`, `src/Confluent.Kafka/Internal/OperationCompletionSource.cs`,
  and **`tests/Confluent.Kafka.UnitTests/AssemblyInfo.cs`** (note: the third is under `tests/`, NOT
  `src/`). The Actor must **never** stage, revert, or touch these; commit only its own alloc-budget
  test files.

## 1 · Rationale

The receive-path / query allocation-budget tests are **flaky**: they measure a tiny per-record signal
with process-wide `GC.GetTotalAllocatedBytes(precise: true)` (grep-confirmed: all 12 current sites),
which is **contaminated by concurrently-running tests** under parallel execution (hence CI's manual
`--filter !~Allocation`) and **mis-measures the async path**, whose copy-out runs on the **foreign
dispatcher thread**, not the measuring thread (Critic N=7 Finding 2; agent-memory
`alloc-budget-cross-thread-measurement`).

**Key insight — the marshaller is shared between sync and async.** Both `NativeConsumer.PollTyped<K,V>`
(sync) and `TypedPollCallbacks.OnPoll` (async) call the identical `ConsumerRecordsMarshal.CopyOut<K,V>`;
the query family shares `OffsetMapMarshal.CopyOut` / `LongOffsetMapMarshal.CopyOut` /
`PartitionInfoListMarshal.CopyOut`. The **sync** path runs that marshaller **on the caller thread**
(`NativeConsumer.cs:1119/1127` typed poll; query analog `:1396`). So measuring the marshaller through
the **sync** consumer with the **per-thread** counter `GC.GetAllocatedBytesForCurrentThread()` (+ the
existing marginal large−small subtraction + warmup) is on-thread, per-thread-accurate, and **immune to
concurrent tests** → robust under parallel execution. Because the marshaller is shared, the sync
measurement **fully covers the async path's per-record budget**; the async round-trip adds only
**per-op** overhead (Task/GCHandle/state-machine per poll) that the marginal technique already cancels
AND that CLAUDE.md §11 deems amortized/non-hot-path. So the async alloc tests give **no per-record
coverage beyond sync** — they are redundant and flaky, and are removed.

## 2 · Per-thread-counter mechanism (retained shape)

Each retained test keeps its structure and changes only the counter (and, for the typed test, the
consumer):

```
warm up (JIT + first-call one-time allocs);
long before = GC.GetAllocatedBytesForCurrentThread();   // was GetTotalAllocatedBytes(precise:true)
… run the SYNC op invoking the shared marshaller on THIS thread …
long after  = GC.GetAllocatedBytesForCurrentThread();
// marginal: large-payload minus small-payload cancels fixed per-op cost; assert marginal ≈ 0.
```

- **Per-thread accuracy** — counts only this thread's allocations; a concurrent test on another thread
  cannot contaminate it (this is what makes the retained tests parallel-safe).
- **TFM guard unchanged** — `GetAllocatedBytesForCurrentThread()`, like `GetTotalAllocatedBytes`, is
  absent on net462; preserve the existing net462 skip/guard. Retained tests stay net8.0/net10.0-only.
- Marginal + warmup are **kept**; only the counter source (and the typed test's consumer) change.

## 3 · Disposition per test (grep-verified locations)

### KEEP + harden (process-wide → per-thread counter)
| Test | File:line | Change |
|---|---|---|
| `Poll_PerRecordAllocation_WithinCopyOutBudget` | `PublicSyncConsumerAllocationBudgetTests.cs:96/98` | counter → `GetAllocatedBytesForCurrentThread()` |
| begin-offsets / partitionsFor per-op budgets | `PublicSyncConsumerQueryAllocationBudgetTests.cs:104/110` | counter → per-thread |

### CONVERT (async consumer → sync typed consumer) + per-thread counter
| Test | File:line | Change |
|---|---|---|
| `TypedPoll_LargeValue_AddsNoValueSizedIntermediateAllocation` (64 KiB-value → small-type zero-copy proof) | `PublicConsumerTypedAllocationBudgetTests.cs:101/103` | drive via the **sync** typed `KafkaConsumer<byte[],…>` + per-thread counter; must reach the same `CopyOut<K,V>` and prove no value-sized intermediate |

### REMOVE (redundant per-record, or per-op §11-amortized; all flaky/process-wide)
| Test | File:line |
|---|---|
| async poll budget | `PublicConsumerAllocationBudgetTests.cs:101/103` |
| interop async poll budget | `Interop/ConsumerPollAllocationBudgetTests.cs:136/138` |
| `BeginningOffsets_PerOpAllocation` | `PublicConsumerOffsetQueryTests.cs:476/482` |
| `PartitionsFor_PerOpAllocation` | `PublicConsumerPartitionMetadataTests.cs:398/404` |
| `Position_PerOpAllocation` | `PublicConsumerPositionTests.cs:271/277` |
| `Pause_RepeatedOp_MarshallingAllocationIsBounded` | `PublicConsumerPartitionOpsTests.cs:437/447` |
| `Assignment_RepeatedRead_…` | `PublicConsumerSyncReadTests.cs:340/348` |
| `SeekAndCurrentLag_PerOpAllocation` | `PublicConsumerSeekLagTests.cs:331/338` |

**Mandatory pre-delete check:** verify each removal against the KEEP-list — each removed test is either
an **async duplicate** of a retained sync per-record test, or a **per-op/per-RPC** budget
(position/pause/assignment/seek-lag) that exercises a per-*call* marshaller, not the per-*record*
receive path (§11 amortized). Confirm no unique per-record marshaller path loses its only test. Clean
up orphaned helpers / `using`s left by the deletions.

## 4 · Consciously-accepted gap

After this, **no test budgets the async per-op overhead** (Task/GCHandle/state-machine per poll).
Justified: CLAUDE.md §11 classifies per-RPC/per-op cost as amortized/negligible, and the current
marginal tests never budgeted it anyway (the marginal subtraction cancels it by construction). Nothing
real is lost. Record in the STATUS entry as a documented decision.

## 5 · Working-tree constraint (PR #144 — Option 1)

Commit **only** `tests/…` alloc-budget test files, `dotnet(M7/P1): …` (`--no-gpg-sign`,
`Co-Authored-By: Claude Opus 4.8`). **Never** `git add`, revert, or touch the three PR #144 fix files:
`src/…/NativeConsumer.cs`, `src/…/OperationCompletionSource.cs`, **`tests/…/AssemblyInfo.cs`**. ⚠ The
`AssemblyInfo.cs` is under `tests/`, so "commit only tests/ files" is NOT sufficient — the Actor must
**explicitly exclude** it. Use **per-path `git add`** of exactly the alloc-budget test files, then
`git status` before each commit to confirm no fix file (incl. `AssemblyInfo.cs`) and no unrelated
untracked file (e.g. `tests/CONSUMER_TEST_COVERAGE_COMPARISON.md`) is staged. Leave the fix untouched.

## 6 · DoD gates (headline first)

- **HEADLINE — parallel, no alloc filter:** full suite under `DisableTestParallelization=false` with
  **NO `--filter !~Allocation`**, **≥20×**, **zero alloc-test failures**. Report run count + result.
  (Proof the hardening worked and the manual filter is no longer needed.)
- `dotnet build` — 0 warnings / 0 errors, all TFM legs.
- `dotnet test` — green (full suite, no filter).
- `dotnet format` — clean.
- `cargo build --features ffi` — no `confluent_kafka.h` delta (test-only; confirm).
- DoD §3 (retained tests still assert the real budget: marginal ≈ 0; the 64 KiB typed proof still shows
  no value-sized intermediate; no assertion weakened by the counter swap), §6/§7 (no orphans; no `src/`
  change).

## 7 · Risks / notes

- **`GetAllocatedBytesForCurrentThread` on net462** — absent; preserve the existing net462 alloc-test
  guard verbatim (no TFM-matrix change).
- **The convert test** must still exercise the *typed* zero-copy path (64 KiB value → small decoded
  type) through the sync typed `KafkaConsumer<byte[],…>` and reach the identical `CopyOut<K,V>`.
- **Accepted async-per-op gap (§4)** — documented decision.
- **AssemblyInfo-under-tests trap (§5)** — the single highest-risk mistake; explicit per-path staging
  guards it.
- Test-only, minimal surface; removes the CI alloc-filter wart.

## 8 · Comment workflow & handoff (Manager)

`dotnet-actor N=21` (test files only) → headline parallel-no-filter ≥20× gate + rest of DoD →
`dotnet-critic N=21` (focus: retained tests genuinely measure the shared marshaller on-thread and would
fail on a per-record regression — no false-confidence after the swap; each removal loses no unique
per-record coverage; the ≥20× parallel-no-filter gate actually passed with zero alloc failures; **no
`src/`/PR-#144 file — incl. `tests/…/AssemblyInfo.cs` — in any commit**; the accepted async-per-op gap
documented). Fix cycle until `COMMENTS.21.md` empty + DoD passes → archive `COMMENTS.DONE.21.md` under
`design/history/M7/P1-alloc-budget-hardening/`, update STATUS.md (M7/P1 DONE), reset `COMMENTS.21.md`.
