# COMMENTS.18 — Critic review (N=18): M5/P8b "Synchronous consumer — query family"

Branch `prashah_dev_public_consumer_remaining_sync`, commits `446cbdef`..`2fb59565`
(base `8490cc42`, P8a HEAD). Reviewed against the C ABI header
(`target/include/confluent_kafka.h`), the sync FFI (`src/ffi/consumer.rs`), the mock
(`src/consumer/mock_consumer.rs`), Java `Consumer`, and the APPROVED PLAN
(`design/history/M5/P8-sync-consumer/PLAN.md`).

## Verdict: CLEAN — no issues filed.

All eight review axes verified; both high-risk items pass. Details below for the record.

### 1. Out-param pre-init handle safety (primary — leak/UAF/double-free) — SAFE
All six wrappers pre-initialize their owned-container out-local to `IntPtr.Zero` before the
P/Invoke:
- `Committed` / `BeginningOffsets` / `EndOffsets` via the shared `RunContainerQuerySync<T>`
  (`NativeConsumer.cs:1543` — `IntPtr handle = IntPtr.Zero;`).
- `OffsetsForTimes` (`:1457` — `IntPtr map = IntPtr.Zero;`), `PartitionsFor` (`:1493` —
  `IntPtr list = IntPtr.Zero;`), `ListTopics` (`:1520` — `IntPtr map = IntPtr.Zero;`).

The "on failure leaves the out-param untouched" premise is CONFIRMED in the FFI: every one of
the six sync fns (`kafka_consumer_Consumer_committed`/`_offsets_for_times`/`_beginning_offsets`/
`_end_offsets`/`_partitions_for`/`_list_topics`, `src/ffi/consumer.rs:3217`–`3596`) writes
`*out_map`/`*out_list` **only** on the `Ok` arm and returns `box_error(e)` on `Err` — including
the `acquire(h)` concurrent-access rejection, which returns before touching the out-param.
So on any failure the out-local stays `IntPtr.Zero`.

The copy-out-then-destroy tail `ThrowOrCopyOutAndDestroy<T>` (`:1575`) throws (via
`KafkaException.FromHandle`, which frees the error handle exactly once) BEFORE calling
`copyOut(handle)`, so the non-null-safe copy-out never dereferences `Zero`; the `finally`
`destroy(handle)` runs on `Zero`, and all five container `_destroy` FFIs are null-guarded
(`if !map.is_null()`), so it is a no-op. No leak, no UAF, no double-free, no arbitrary-free on
either path. Success path: FFI always boxes a non-null handle on `Ok`, so copy-out gets a valid
root and the `finally` destroys it exactly once. `EndOffsets_AfterThrow_ConsumerReusable` +
`QueryFamily_RepeatedCalls_StayReusable` empirically prove free-exactly-once.

### 2. Shipped async `OffsetsForTimesWithCallback` refactor (regression risk) — NO REGRESSION
`cee1ab9d` extracts the inline validation/snapshot into the shared `SnapshotTimestamps`
(`:3007`), now backing both the async path and the new sync `OffsetsForTimes`. The helper body
is byte-for-byte the deleted inline block: same null-map `ArgumentNullException`; same
per-entry `ArgumentException("Topic names must not be null.", nameof(timestampsToSearch))`;
same `ArgumentOutOfRangeException(..., tp.Partition, "Partition must not be negative.")`; same
array fills; negative timestamp still passed through (no rejection). Validation still runs
eagerly BEFORE `SubmitOwnedHandleOperation` (order preserved). The submit closure now reads
`snapshot.Topics[i]`/`.Partitions`/`.Timestamps` — the same backing arrays (the `readonly
struct` holds references), so pin lifetime through the unchanged `WithPinnedTopicsAndTimestamps`
is identical. `SubmitOwnedHandleOperation` + `ConsumerCallbacks.OffsetsForTimes` untouched.
Matches the P8a `WithPinnedTopicsOnly` refactor bar.

### 3. Load-bearing rule (no sync-over-async) — HELD
No `GetAwaiter().GetResult()` / `.Result` / `.Wait()` / `Task.Run` / managed `block_on` in
`src/` (only doc-comment prose explaining "NOT sync-over-async" and `bin`/`obj` XML artifacts).
Each sync wrapper calls the sync C ABI directly; `block_on` runs inside the Rust core runtime.
`KafkaConsumer`/`MockConsumer` forwarders are one-line `=> _native.X(...)`.

### 4. P/Invoke fidelity — MATCHES HEADER
All six DllImports match `confluent_kafka.h`: `ConsumerCommitted`→`OffsetMap_t** out`;
`ConsumerOffsetsForTimes`→`long[] timestamps` + `OffsetAndTimestampMap_t** out`;
`ConsumerBeginningOffsets`/`ConsumerEndOffsets`→`LongOffsetMap_t** out`; `ConsumerPartitionsFor`→
single `IntPtr topic` + `PartitionInfoList_t** out`; `ConsumerListTopics`→ no input +
`TopicPartitionInfoMap_t** out`. All return `KafkaError*` as `IntPtr`; `out IntPtr` container;
parallel-array shapes identical to the async DllImports; `Cdecl`; full ABI symbol EntryPoints.
No header delta (Mode A confirmed — `cargo build --features ffi` leaves the header unchanged).

### 5. Helper/marshaller reuse (DoD §6) — NO DUPLICATION
`SnapshotPartitions`/`ExtractPartitions`/`WithPinnedTopics`/`WithPinnedTopicsAndTimestamps` and
the five copy-out marshallers reused verbatim (single definitions each). New generics
`RunContainerQuerySync<T>` (one def, 3 callers) and `ThrowOrCopyOutAndDestroy<T>` (one def, 6
callers) are the sync analog of `SubmitOwnedHandleOperation<T>`; both sound in all paths.

### 6. Mock-reachability honesty (§8) — ACCURATE, not over-claimed
- `Committed` full 3-field round-trip (offset 42 / metadata "meta-x" / epoch 7) genuinely
  reachable: `committed()` returns the stored value only for an `is_assigned(tp)` partition
  (`mock_consumer.rs:committed`), and the test Assigns first.
- `OffsetsForTimes` throws `unsupported_version` unconditionally (`mock_consumer.rs` returns
  `Err(unsupported_version("MockConsumer::offsets_for_times is not implemented"))`); the test
  asserts the throw + exact message + code 35 — no faked success.
- `BeginningOffsets`/`EndOffsets` unset-tp → `illegal_state("The partition {tp} does not have a
  beginning/end offset.")` — exact messages matched.
- `PartitionsFor`/`ListTopics` data via `UpdatePartitions`; empty/unregistered cases covered.

### 7. Surface + preconditions — CORRECT
Exactly the six query members added to `IConsumer` (additive; P8a members untouched;
`IAsyncConsumer` untouched). No `CancellationToken`/`Async` suffix. Preconditions before any
pin/P-Invoke with exact messages and correct param names; validation precedes `ThrowIfClosed`
(asserted "even when closed"); post-dispose → `ObjectDisposedException` on all six; negative
timestamp accepted (sentinel pass-through).

### 8. Tests / DoD §3 — REAL ASSERTS
Round-trip, honesty, precondition, reusable-after-throw, per-op allocation budget (net8.0+,
caller-thread measurement), and a six-op TFM smoke leg all present with real assertions.

### DoD gates
`cargo build --features ffi` OK (no header delta) · `dotnet build` 0 warn / 0 err · `dotnet test
-f net10.0` 389 passed / 0 failed (346→389, +43); new query+alloc classes stable 5/5 in
isolation · `dotnet format --verify-no-changes` clean · governance doc-sync (CLAUDE.md §3
sketch, STATUS.md) faithful to the shipped surface.

**M5/P8b meets the Definition of Done.**

_No suggested CLAUDE.md / rule updates: `COMMENTS.FP.md` and `COMMENTS.FN.md` are both empty._
