# M5/P6 — "Consumer commit" (Category D: the commit family)

**Label:** M5/P6 "Consumer commit"
**Review number:** N=15 (last completed N=14 = M5/P5 Category E2, the partition-metadata query family)
**Mode:** A — .NET-only (every ABI function is already emitted in `target/include/confluent_kafka.h`, verified below).
**Scope lock (user):** the two commit operations the Python sibling implements — **no `OffsetCommitCallback` variant, no `TimeSpan` overload** (both are Python "out of scope"). Public signatures are **USER-LOCKED**.

---

## 1 · Scope & the locked public surface

This phase wires the **commit family** — the last operation family before only pattern-subscribe + the rebalance listener + the Mode-B backlog remain unwired on `IAsyncConsumer`. Three public members plus one public constructor on the shipped `OffsetAndMetadata`.

### 1.1 Locked signatures (implement exactly)

Placement (§5, user-resolved): the two `Commit` overloads on `IAsyncConsumer`; `void CommitAsync()` on `IConsumerCommon`. All three are mirrored public on both `AsyncKafkaConsumer` and `AsyncMockConsumer` as thin forwarders to `NativeConsumer`:

```csharp
// ── Confirming commit: awaited, faults on failure ──  (= Java commitSync / commitSync(Map); Python commit())
Task Commit(CancellationToken cancellationToken = default);
Task Commit(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets,
            CancellationToken cancellationToken = default);

// ── Fire-and-forget: returns immediately, best-effort ──  (= Java commitAsync(); Python commit_async())
// (declared on IConsumerCommon — §5)
void CommitAsync();
```

Plus a new **public** constructor on the shipped `OffsetAndMetadata` (today it is output-only — `internal OffsetAndMetadata(long offset, string metadata, int? leaderEpoch)`):

```csharp
public OffsetAndMetadata(long offset, string? metadata = null, int? leaderEpoch = null);
```

### 1.2 Naming rationale (user-chosen — do NOT re-open)

`Commit` + `CommitAsync` = exact Python parity (`commit` / `commit_async`), idiomatic `await consumer.Commit()`, and no blocking-thread footgun. This **supersedes** the CLAUDE.md §3 sketch + §4 note which currently show `void CommitSync()` (genuinely blocking) + `Task Commit()`. The §4 amendment (deliverable §7) records the new mapping and its rationale.

### 1.3 Verified fact — the §3 sketch is NOT yet implemented

The shipped `IAsyncConsumer.cs` does **not** declare `Commit` / `CommitSync` today (the §3 sketch is forward-looking, not shipped). So all three members are genuinely new; there is no shipped commit surface to migrate. The §60-62 "still to come: the commit family …" remark in the shipped interface doc-comment is updated by this phase (deliverable §7).

---

## 2 · ABI ground truth (verified in `target/include/confluent_kafka.h`)

All three functions are emitted. No Mode-B / Rust-core work.

| ABI function (verified line) | Shape | .NET bridge |
|---|---|---|
| `Consumer_commit_sync_async(consumer, op_callback_t cb, ud)` — h.1790 | **VOID** `op_callback_t`; no offsets | **reuse** the void bridge: `SubmitVoidOperation` + `ConsumerCallbacks.Operation` |
| `Consumer_commit_sync_offsets_async(consumer, const char*const* topics, const int32_t* partitions, const int64_t* offsets, const int32_t* leader_epochs, const char*const* metadata, int32_t count, op_callback_t cb, ud)` — h.1823 | **VOID** `op_callback_t`; **5 parallel input arrays** | void bridge + **new 5-array marshaller** (§4) |
| `Consumer_commit_async(consumer) -> kafka_common_KafkaError_t*` — h.1843 | **sync-returning**, fire-and-forget; null = success | **sync FFI call** + `KafkaException.FromHandle`-throw (the `EnforceRebalance` precedent) |

**There is NO new completion-bridge shape.** The confirming commit reuses the proven void `op_callback_t` bridge verbatim; the fire-and-forget is a synchronous FFI call. The header comment on `commit_sync_offsets` documents the input contract exactly: "`metadata` may be null and `leader_epoch < 0` means no epoch" (h.1796-1797) — matching the Python convention.

---

## 3 · The genuinely new work

### 3.1 Offsets-input marshalling — `WithPinnedCommitOffsets` (5 parallel arrays)

`commit_sync_offsets_async` takes 5 parallel input arrays. Extend the M5/P3 `WithPinnedTopics` pattern (in `NativeConsumer.cs`) to a new shared static helper — **`WithPinnedCommitOffsets`** — that differs from the existing `WithPinnedTopics` / `WithPinnedTopicsAndTimestamps` in having **two string arrays** (topics + metadata) and two extra numeric arrays:

- `topics` — `IntPtr[]` of pinned NUL-terminated UTF-8 (`Utf8Marshal.Pin`, one per entry).
- `partitions` — `int[]` (blittable, passed through).
- `offsets` — `long[]` (blittable, passed through).
- `leader_epochs` — `int[]`, **`-1` when `LeaderEpoch` is null** (the Python `_commit_spec` convention: `oam.leader_epoch if … is not None else -1`).
- `metadata` — `IntPtr[]` of pinned UTF-8, **`""` when `Metadata` is null** (Python: `oam.metadata if … is not None else ""`; and Java's own default is `""`, never null — §5). Since `OffsetAndMetadata.Metadata` is never-null by construction (the ctor coerces null → `""`, §5), the pinned value is always a valid UTF-8 buffer; the null-coalesce is defensive.
- `count` — `int`.

Design: pin **both** string arrays call-scoped (two `Utf8Marshal.PinnedUtf8String?[]` passes filling two `IntPtr[]`), pass the three blittable numeric arrays straight through, run the `body`, and release **all** pins in a single `finally` — mirroring `WithPinnedTopics`'s try/finally exactly. **No per-element copy beyond the UTF-8 encode** (CLAUDE.md §12 / ffi §A4/§B4): topic + metadata bytes are pinned, not copied; the `int[]`/`long[]` arrays are blittable. `count == 0` (empty map) runs `body` with empty arrays — a valid pass-through, never a throw (§B5).

**Snapshot + validate before pinning** — a new `SnapshotCommitOffsets(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>)` private helper (the commit analog of the shipped `SnapshotPartitions`) that walks the dictionary once, applies the §B5 preconditions (§6), and produces the four parallel arrays (`string[] topics`, `int[] partitions`, `long[] offsets`, `int[] leaderEpochs`) plus a parallel `string[] metadata`. The submit lambda then calls `WithPinnedCommitOffsets` inside `SubmitVoidOperation`, exactly as `CommittedWithCallback` calls `WithPinnedTopics` inside `SubmitOwnedHandleOperation`.

> **Deliberate choice:** add a **new** `WithPinnedCommitOffsets` rather than generalize `WithPinnedTopics` — keeps the shipped M5/P3–P5 partition-op / offset-query pin paths byte-for-byte untouched (the same "clone a parallel helper" discipline the shipped `SubmitScalarOperation` / `SubmitOwnedHandleOperation` / `WithPinnedTopicsAndTimestamps` used). The two-string-array shape has no existing helper to reuse.

### 3.2 `NativeConsumer` methods

- **`CommitWithCallback(CancellationToken)`** — no offsets. `SubmitVoidOperation(ct, (c, cb, ud) => NativeMethods.ConsumerCommitSyncAsync(c, cb, ud))`. A one-line clone of `UnsubscribeWithCallback` (the shipped no-arg void-bridge precedent).
- **`CommitWithCallback(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets, CancellationToken)`** — snapshot+validate → `SubmitVoidOperation(ct, (c, cb, ud) => WithPinnedCommitOffsets(…, (t, p, o, e, m, cnt) => NativeMethods.ConsumerCommitSyncOffsetsAsync(c, t, p, o, e, m, cnt, cb, ud)))`. Overload resolution by arity, matching the two client `Commit` overloads.
- **`CommitAsync()`** (returns `void`) — the sync fire-and-forget: `ThrowIfClosed()`, then `error = NativeMethods.ConsumerCommitAsync(_handle.DangerousGetHandle())`, then `KafkaException.FromHandle(error)` → throw iff non-null. **Structurally identical to the shipped `EnforceRebalance`** sync-op path (which is the reference for "sync ABI fn returning `KafkaError*`, throw-on-non-null"). Non-blocking: it returns the instant the core has initiated the async commit (ABI: "returns once the async commit is initiated", h.1834). Takes **no** `CancellationToken` (fire-and-forget — nothing to cancel; Python's `commit_async()` takes no args). No pin, no `GCHandle`, no bridge.

### 3.3 `NativeMethods` declarations (two new `[DllImport]`)

Model on the shipped `ConsumerUnsubscribeAsync` (void, no arrays) and `ConsumerCommittedAsync` (parallel arrays + callback):

```csharp
[DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_commit_sync_async", CallingConvention = CallingConvention.Cdecl)]
internal static extern void ConsumerCommitSyncAsync(
    IntPtr consumer, ConsumerCallbacks.OperationCallback callback, IntPtr userData);

[DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_commit_sync_offsets_async", CallingConvention = CallingConvention.Cdecl)]
internal static extern void ConsumerCommitSyncOffsetsAsync(
    IntPtr consumer,
    IntPtr[] topics,
    int[] partitions,
    long[] offsets,
    int[] leaderEpochs,
    IntPtr[] metadata,
    int count,
    ConsumerCallbacks.OperationCallback callback,
    IntPtr userData);

[DllImport(DllName, EntryPoint = "kafka_consumer_Consumer_commit_async", CallingConvention = CallingConvention.Cdecl)]
internal static extern IntPtr ConsumerCommitAsync(IntPtr consumer);
```

Both `IntPtr[]` string arrays map C's `const char*const*` (same as the shipped `topics` param on `ConsumerAssignAsync` / `ConsumerCommittedAsync`). No new callback type — `ConsumerCallbacks.Operation` (the shipped void trampoline) is reused verbatim (no change to `ConsumerCallbacks.cs`).

---

## 4 · `OffsetAndMetadata` public constructor

Today `OffsetAndMetadata` has only an `internal` ctor (used by the copy-out marshaller). The `Commit(offsets)` **input** path needs users to construct it, so add:

```csharp
public OffsetAndMetadata(long offset, string? metadata = null, int? leaderEpoch = null);
```

### 4.1 Java validation to mirror (verified in `kafka/.../consumer/OffsetAndMetadata.java`)

Java's canonical ctor `OffsetAndMetadata(long offset, Optional<Integer> leaderEpoch, String metadata)` (line 48):

```java
if (offset < 0)
    throw new IllegalArgumentException("Invalid negative offset");
...
this.metadata = Objects.requireNonNullElse(metadata, OffsetFetchResponse.NO_METADATA); // NO_METADATA == ""
```

**Mirror exactly:**
- `offset < 0` → **`ArgumentOutOfRangeException(nameof(offset), offset, "Invalid negative offset")`** — the exact Java message, validated in the ctor (CLAUDE.md idiom map: `IllegalArgumentException` → `ArgumentException` family; a bad numeric value → `ArgumentOutOfRangeException`, consistent with the shipped negative-partition / negative-offset precedents in `NativeConsumer`). The exact message is asserted by a test (DoD §3).
- `metadata` null → coerce to `""` (`metadata ?? string.Empty`) so `Metadata` stays never-null (matches Java's `NO_METADATA` and the shipped `OffsetAndMetadata.Metadata` "never null; empty when unset" contract, and the marshaller's copy-out assumption).
- `leaderEpoch` is `int?` — passed through as-is (null = absent), matching Java's `Optional<Integer>` and the shipped `LeaderEpoch` property. No validation (Java does not validate it in the ctor).

The `internal` ctor stays (the marshaller path is unchanged); the public ctor delegates to it after validation/coercion, so there is one field-assignment site.

> **Param order rationale:** `(long offset, string? metadata = null, int? leaderEpoch = null)` is the user-locked order. It differs from Java's canonical `(offset, leaderEpoch, metadata)` because Java overloads on arity (`(offset)`, `(offset, metadata)`, `(offset, leaderEpoch, metadata)`); C# uses one ctor with optional params, and `metadata` before `leaderEpoch` matches Java's most-used 2-arg `(offset, metadata)` overload + the Python `OffsetAndMetadata(offset, metadata="", leader_epoch=None)` order exactly. Recorded here so the Critic does not flag the divergence from Java's 3-arg order.

---

## 5 · `CommitAsync()` interface placement — RESOLVED (user)

`Task Commit(...)` (both overloads) goes on **`IAsyncConsumer`** (async, blocking-style). **`void CommitAsync()` goes on `IConsumerCommon`** (user-resolved — override of the draft's `IAsyncConsumer` recommendation).

Rationale:
1. **The fire-and-forget commit is flavor-independent.** `commitAsync()` is non-blocking and returns nothing to await in EITHER a sync or an async client — its shape is `void CommitAsync()` regardless of flavor. `IConsumerCommon` is defined (shipped doc-comment) as "the members that are non-blocking in Java's consumer implementation and therefore stay synchronous regardless of the async/sync split … the common base of `IAsyncConsumer` and reserves the shape for a future sync `IConsumer`." A fire-and-forget commit fits that definition exactly.
2. **The confirming commit is flavor-DEPENDENT, so it stays flavor-specific.** `Commit` differs by flavor — `Task Commit(...)` on the async `IAsyncConsumer`, and a future sync `IConsumer` would carry a blocking mirror instead — so it belongs on `IAsyncConsumer`, not the shared base.
3. **The future sync `IConsumer` inherits `CommitAsync()` for free.** On `IConsumerCommon`, the sync mirror gets the identical `void CommitAsync()` with no duplication — exactly the reason `Wakeup` / `GroupMetadata` / `EnforceRebalance` live there.

Trade-off noted: `IConsumerCommon` gains its first **data-plane** (network-commit) member alongside the local reads/actions — a mild widening of its charter from "non-blocking *local*" to "non-blocking regardless of network semantics." Accepted per the flavor-independence argument above.

So: `IAsyncConsumer` gains **two** members (`Commit()`, `Commit(offsets)`); **`IConsumerCommon` gains `void CommitAsync()`**.

---

## 6 · Errors & preconditions (ffi §B5) — before any pin / P-Invoke

Validated **before** any pin / P-Invoke (the ABI does not validate preconditions and panics on violation → UB across FFI). Reuse the shipped `SnapshotPartitions`-style walk in the new `SnapshotCommitOffsets`:

- `Commit(null offsets)` → **`ArgumentNullException`** (the shipped `SnapshotPartitions` / `OffsetsForTimes` null-map precedent).
- A dict key (`TopicPartition`) with null `Topic` → **`ArgumentException`** ("Topic names must not be null.").
- A dict key with negative `Partition` → **`ArgumentOutOfRangeException`** ("Partition must not be negative.").
- A **null `OffsetAndMetadata` value** in the dict → **`ArgumentNullException`** / `ArgumentException` ("Offset value must not be null." — a reference-type value; `OffsetAndMetadata` is a `sealed class`, so a null value is possible and must be rejected before deref).
- **Negative offset inside an `OffsetAndMetadata`** cannot reach here — the public ctor (§4) rejects `offset < 0` at construction, so any `OffsetAndMetadata` the user can pass already has `offset >= 0`. (No re-validation needed in the marshaller; note this in a comment.)
- Operational failure → **faulted `Task`** for both `Commit` overloads (the void bridge's error path); **thrown `KafkaException`** for the sync `CommitAsync()` (the `EnforceRebalance` FromHandle-throw path).
- Concurrent use → **faulted `Task`** (`ConcurrentModification`) for `Commit`; for `CommitAsync()` the core's sync path returns a non-null error handle → **thrown `KafkaException`** (single-owner, core-serialized — no managed guard, M3/P2).
- Post-dispose → **`ObjectDisposedException`** (from `ThrowIfClosed()` at the top of every `NativeConsumer` op, incl. the sync `CommitAsync`).
- `Commit(...)` `CancellationToken` = user-initiated cancellation only (→ `wakeup()`; pre-canceled → `OperationCanceledException` synchronously, via the shipped `SubmitVoidOperation` `ThrowIfCancellationRequested` + `RegisterCancellation(ct, Wakeup)`). `CommitAsync()` takes **no** `CancellationToken`.

---

## 7 · Doc-sync — the CLAUDE.md §4 amendment (required deliverable)

The current CLAUDE.md §4 "**Exception — Java sync/async pairs**" note + the §3 consumer sketch say `void CommitSync()` (genuinely sync, blocks) + `Task Commit()` (= commitAsync). **This phase supersedes that.** The Actor must, in `bindings/dotnet/CLAUDE.md` only (the binding-local rulebook — not root CLAUDE.md):

1. **§4 "Exception — Java sync/async pairs" note** — rewrite to the new mapping:
   - `Task Commit(...)` = the **confirming** commit (async-bridged over the void `op_callback_t`; = Java `commitSync` / `commitSync(Map)`; Python `commit()`). Reason it maps to `Task`: Java `commitSync` **blocks** → idiom map → `Task`; async-bridged (not a blocking-thread `CommitSync` façade) avoids the blocking-thread footgun.
   - `void CommitAsync()` = the **fire-and-forget** commit (sync `Consumer_commit_async` returning `KafkaError*`; = Java `commitAsync()`; Python `commit_async()`). Reason it maps to sync `void`: Java `commitAsync` is **non-blocking** → idiom map → sync `void`.
   - Record the naming rationale (exact Python parity `commit`/`commit_async`; idiom-map read from Java's blocking behavior — `commitSync` blocks → `Task`, `commitAsync` non-blocking → sync `void`; async-bridged confirming commit avoids the blocking-thread footgun).
   - Remove the old claim that `CommitSync()` "keeps Java's own name and calls the blocking-native ABI directly" — there is no `CommitSync` member in this design.
2. **§3 consumer sketch** — put the two `Task Commit(...)` overloads on the `IAsyncConsumer` block (replacing the old `Task Commit(...) // Java commitAsync` + `void CommitSync(); // Java commitSync …`), and add `void CommitAsync();` to the **`IConsumerCommon`** block (§5, user-resolved) — all with the corrected Java-mapping comments.
3. **§3 "Already wired" / "Still to come" prose** — move the commit family from "Still to come" to "Already wired" (M5/P6); the remaining unwired list becomes "pattern subscribe, the rebalance listener, and the Mode-B backlog."
4. **The shipped `IAsyncConsumer.cs` doc-comment** (lines ~56-62, "the remaining Java members (the commit family, …)") — drop "the commit family" from the not-yet-wired list; leave pattern-subscribe + rebalance-listener.

The `Commit()` mapping comment currently on the shipped `IAsyncConsumer.Commit` (`// Java commitAsync`) does not exist in the shipped file (the member is not present), so there is nothing to correct there — the member is being added fresh with the correct `// Java commitSync` mapping.

---

## 8 · Reachability & the E1 bonus (the marquee test)

### 8.1 Mock reachability (verified in `src/consumer/mock_consumer.rs`)

- `commit_sync_async` → `commit_sync` → `commit_async_impl(empty-or-current, None)` — resolves broker-free (`ensure_not_closed()` then `self.committed.extend(offsets)`; no assignment required). ✓
- `commit_sync_offsets_async` → `commit_sync_offsets(map)` → `commit_async_impl(map, None)` — **populates the mock's `committed` map** via `self.committed.extend(offsets)` (line 1017). No prior assignment needed to store. ✓
- `commit_async` → `commit_async_impl(empty, None)` — resolves broker-free. ✓
- `committed(partitions)` (line 806) reads back: for each requested TP present in `self.committed`, returns `om.clone()` **iff `subscriptions.is_assigned(tp)`**, else `OffsetAndMetadata::new(0)` (offset 0, empty metadata). ⚠ **So the exact offset/metadata/epoch round-trip requires the TP be assigned first.**

### 8.2 The non-empty `Committed` round-trip test (unblocks E1's deferred assertion)

E1 (M5/P4) could only test `Committed` empty broker-free (`PublicConsumerOffsetQueryTests.cs` lines 176-190, `Committed_UncommittedPartition_ReturnsEmptyMap`, whose comment explicitly defers the non-empty assertion "to the commit-family phase"). This phase unblocks it. Add the **marquee end-to-end test**:

```
using AsyncMockConsumer consumer = new();
var tp = new TopicPartition(Topic, 0);
await consumer.Assign(new[] { tp });                      // so committed() returns the exact value, not offset 0
var oam = new OffsetAndMetadata(42, "meta-x", 7);          // exercises the new public ctor
await consumer.Commit(new Dictionary<TopicPartition, OffsetAndMetadata> { [tp] = oam });  // 5-array marshaller
var result = await consumer.Committed(new[] { tp });       // E1's OffsetMap copy-out with REAL data
Assert.Equal(42, result[tp].Offset);
Assert.Equal("meta-x", result[tp].Metadata);
Assert.Equal(7, result[tp].LeaderEpoch);
```

This is the phase's marquee test: it exercises **both** the new offsets-input marshalling (`WithPinnedCommitOffsets`, incl. non-null metadata + a real leader epoch) **and** E1's `OffsetMap` copy-out with real data — the full round-trip E1 could not reach. Add a second variant with a **null-metadata / null-epoch** `OffsetAndMetadata` (ctor coercion → `""` / `-1` epoch on the wire) asserting `Metadata == ""` and `LeaderEpoch == null` on read-back (the epoch `-1` sentinel is what the mock stores; confirm the copy-out maps it back to null — cross-check against the shipped `OffsetMapMarshal` epoch handling).

> **Note for the Actor:** whether the mock stores/returns the leader epoch faithfully (7 in, 7 out) must be **verified against `mock_consumer.rs` + `OffsetMapMarshal`** during implementation. If the mock does not round-trip a non-negative epoch (e.g. it drops it), assert what the mock actually returns and document the reachability limit in a code comment + COMMENTS.DONE — do NOT weaken the offset/metadata assertions, which are definitely reachable.

### 8.3 Test surface (mirror the shipped `PublicConsumerOffsetQueryTests` conventions)

New public tests (a new `PublicConsumerCommitTests.cs`, or extend the existing offset-query file — Actor's call, prefer a new file for the commit family):

- `Commit()` (no offsets) on the mock resolves (awaited Task completes, `TestTimeout` guard).
- `Commit(offsets)` on the mock resolves; then the §8.2 round-trip.
- `CommitAsync()` returns immediately without throwing on the mock (fire-and-forget).
- **Preconditions (§6):** `Commit(null)` → `ArgumentNullException`; null element topic → `ArgumentException`; negative partition → `ArgumentOutOfRangeException`; null `OffsetAndMetadata` value → `ArgumentNullException`; each **before** any native call.
- Post-dispose: `Commit(...)` → `ObjectDisposedException`; `CommitAsync()` → `ObjectDisposedException`.
- Pre-canceled token: `Commit(..., canceledToken)` → `OperationCanceledException` synchronously.
- Empty-map `Commit(offsets)` (count == 0) resolves (pass-through, no throw).
- **`OffsetAndMetadata` public ctor:** `new OffsetAndMetadata(-1)` → `ArgumentOutOfRangeException` with message `"Invalid negative offset"` (assert the message — DoD §3); `new OffsetAndMetadata(5)` → `Metadata == ""`, `LeaderEpoch == null`; `new OffsetAndMetadata(5, "m", 3)` → all three set.
- **TFM matrix** carries automatically (the new members compile on netstandard2.0/net8.0/net10.0 — no modern-only API used).

---

## 9 · Definition of Done

- All three public members + the `OffsetAndMetadata` public ctor implemented, on both client classes + the interface, with full rustdoc-style XML doc-comments (matching the shipped members' doc density, incl. the `<exception>` list and the async/sync + Java-mapping remarks).
- `WithPinnedCommitOffsets` + `SnapshotCommitOffsets` in `NativeConsumer`; `CommitWithCallback` (×2) + `CommitAsync` methods; two new `NativeMethods` declarations; **no change** to `ConsumerCallbacks.cs` (Operation reused) or the shipped void/owned/scalar/E1/E2 paths.
- `OffsetAndMetadata` public ctor with the Java `offset < 0` precondition + null-metadata coercion; internal ctor + marshaller path untouched.
- CLAUDE.md §4 note + §3 sketch + §3 prose + shipped `IAsyncConsumer.cs` doc-comment amended (§7).
- Tests (§8): the marquee non-empty `Committed` round-trip + the full precondition/lifecycle/ctor suite; error-message content asserted (DoD §3); `@RepeatedTest`-style loops N/A here.
- `cargo build --features ffi` (native present) → `dotnet build` → `dotnet test` green on the TFM matrix; `dotnet format` + analyzers clean; ffi-marshalling anti-patterns satisfied (no per-element byte copy beyond the UTF-8 encode; both string arrays pinned call-scoped and released in `finally`; the sync `CommitAsync` frees the error handle exactly once via `FromHandle`; no `#[async_trait]`/modern-interop; no managed access guard).
- **Allocation note:** commit is low-frequency (not the receive/send hot path), so the per-record allocation-budget audit (DoD §10 / ffi §B4) does not apply; the marshaller's per-entry UTF-8 encodes + parallel arrays are acceptable (matching the shipped `WithPinnedTopics` query paths).

---

## 10 · Files touched (all C#, header-down — Mode A)

- `src/Confluent.Kafka/OffsetAndMetadata.cs` — add public ctor (§4).
- `src/Confluent.Kafka/IAsyncConsumer.cs` — add `Commit()`, `Commit(offsets)` (§5); amend not-yet-wired doc-comment (§7).
- `src/Confluent.Kafka/IConsumerCommon.cs` — add `void CommitAsync()` (§5, user-resolved).
- `src/Confluent.Kafka/AsyncKafkaConsumer.cs` — forward the three members to `NativeConsumer`.
- `src/Confluent.Kafka/AsyncMockConsumer.cs` — forward the three members.
- `src/Confluent.Kafka/Internal/NativeConsumer.cs` — `CommitWithCallback` ×2, `CommitAsync`, `WithPinnedCommitOffsets`, `SnapshotCommitOffsets`.
- `src/Confluent.Kafka/Internal/Interop/NativeMethods.cs` — three new `[DllImport]` (§3.3).
- `tests/Confluent.Kafka.UnitTests/PublicConsumerCommitTests.cs` — new; plus extend/annotate `PublicConsumerOffsetQueryTests.cs`'s deferred-non-empty comment to point at the new round-trip test.
- `bindings/dotnet/CLAUDE.md` — §3 + §4 doc-sync (§7).
- **No Rust, no header, no `ConsumerCallbacks.cs` change.**

---

## 11 · Resolved (user review, 2026-08-06)

All four confirmed:

1. **`CommitAsync` placement → `IConsumerCommon`** (user override of the draft's `IAsyncConsumer` recommendation). The fire-and-forget commit is flavor-independent (always `void`), so it belongs on the shared non-blocking base a future sync `IConsumer` also inherits; the confirming `Commit` (flavor-dependent) stays on `IAsyncConsumer`. §5 + §1.1 + §7 + §10 updated to match.
2. **Leader-epoch** → assert 7-in / 7-out **if the mock round-trips it**; if not, assert offset + metadata strictly and **document** the epoch as a reachability limit (not dropped silently).
3. **Test file** → new `PublicConsumerCommitTests.cs`.
4. **Negative offset** → `ArgumentOutOfRangeException` with Java's exact message `"Invalid negative offset"`.

**Awaiting:** final user approval to run the N=15 Actor → Critic loop.
