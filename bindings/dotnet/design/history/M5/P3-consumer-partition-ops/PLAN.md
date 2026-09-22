# M5/P3 — Consumer partition ops (Category C: void async ops on partition collections)

**Status:** DRAFT — awaiting user approval. **Review number: N=12** (last completed:
N=11, the M5/P2 "Consumer Position", which shipped `Task<long> Position(...)` via the
new scalar completion bridge).

**Milestone / phase label (PROPOSED):** **Milestone 5 / Phase 3 — "Consumer partition
ops"**, slug `P3-consumer-partition-ops`, at
`bindings/dotnet/design/history/M5/P3-consumer-partition-ops/`, consistent with M5/P1
+ M5/P2.

**Mode:** **A (`.NET-only`, no Rust authored).** All five `_async` functions and the
shared `kafka_consumer_Consumer_op_callback_t` void callback already ship in
`target/include/confluent_kafka.h` (verified — see §0). The two mock offset helpers
(`MockConsumer_update_beginning_offsets` / `_update_end_offsets`) also already ship
(single `(topic, partition, offset)` each — verified). `cargo build --features ffi`
remains a *prerequisite build step*, not a change.

**Branch:** commits land on **`prashah_dev_public_consumer_remaining`** (the branch the
user created for post-M4 consumer work; M5/P1 + M5/P2 already shipped there). Treated as
a new PR for M5/P3.

---

## 0 · Scope & ABI ground truth (verified in the generated header)

This phase adds the **five void async ops on partition collections** — "Category C" of
the consumer public-API coverage analysis. All five **block in Java's
`AsyncKafkaConsumer`** (consumer-threading §4 lists `assign` / `pause` / `resume` /
`seekToBeginning` / `seekToEnd` among the ops that "block despite reading as
instantaneous" — each does a cross-thread event round-trip), so per the CLAUDE.md §4
idiom map they are **async** (`Task`). All five reuse the **already-proven void
completion bridge** (`SubmitVoidOperation` + `ConsumerCallbacks.Operation`), so — unlike
M5/P2's scalar bridge — **there is NO new bridge/callback/trampoline this phase.** The
only genuinely new managed work is **marshalling a `TopicPartition` collection into the
ABI's parallel arrays**.

| Member | Java | .NET (proposed) | ABI function (verified) | Callback |
|---|---|---|---|---|
| `Assign` | `assign(Collection<TopicPartition>)` | `Task Assign(IReadOnlyCollection<TopicPartition>, CT)` | `kafka_consumer_Consumer_assign_async` | `op_callback_t` (void) |
| `Pause` | `pause(Collection<TopicPartition>)` | `Task Pause(IReadOnlyCollection<TopicPartition>, CT)` | `kafka_consumer_Consumer_pause_async` | `op_callback_t` (void) |
| `Resume` | `resume(Collection<TopicPartition>)` | `Task Resume(IReadOnlyCollection<TopicPartition>, CT)` | `kafka_consumer_Consumer_resume_async` | `op_callback_t` (void) |
| `SeekToBeginning` | `seekToBeginning(Collection<TopicPartition>)` | `Task SeekToBeginning(IReadOnlyCollection<TopicPartition>, CT)` | `kafka_consumer_Consumer_seek_to_beginning_async` | `op_callback_t` (void) |
| `SeekToEnd` | `seekToEnd(Collection<TopicPartition>)` | `Task SeekToEnd(IReadOnlyCollection<TopicPartition>, CT)` | `kafka_consumer_Consumer_seek_to_end_async` | `op_callback_t` (void) |

**Signatures (exact — verified in `target/include/confluent_kafka.h`, all five identical
in shape):**

```c
void kafka_consumer_Consumer_<op>_async(const kafka_consumer_Consumer_t *consumer,
                                        const char *const *topics,   // array of `count` C strings
                                        const int32_t *partitions,   // parallel array of `count` i32
                                        int32_t count,
                                        kafka_consumer_Consumer_op_callback_t callback,  // (KafkaError*, void*)
                                        void *user_data);
```

`op_callback_t` is the **void** completion callback: `void (*)(kafka_common_KafkaError_t*,
void*)` — non-null `error` = failure, null = success, no result handle. This is the exact
callback already used by `subscribe_async` / `unsubscribe_async` / `seek_async` /
`close_async`. **`ConsumerCallbacks.Operation` is reused verbatim.**

**Two mock offset helpers (verified in the header — used only for the `SeekTo*` poll-after
tests, §6):**

```c
kafka_common_KafkaError_t *kafka_consumer_MockConsumer_update_beginning_offsets(
    const kafka_consumer_Consumer_t *consumer, const char *topic, int32_t partition, int64_t offset);
kafka_common_KafkaError_t *kafka_consumer_MockConsumer_update_end_offsets(
    const kafka_consumer_Consumer_t *consumer, const char *topic, int32_t partition, int64_t offset);
```

Note these are **per-(topic, partition)** (a single offset each), NOT a map — so a
`UpdateBeginningOffset(topic, partition, offset)` / `UpdateEndOffset(...)` mock helper is
one DllImport + one loop over the caller's collection, not a map marshaller.

**Broker-free reachability (verified against `src/consumer/mock_consumer.rs` +
`src/ffi/consumer.rs`):**

  - `assign_async` → `async_void_op(... c.assign(tps))` → mock `assign()` calls
    `subscriptions.assign_from_user(...)` — **infallible / broker-free**.
  - `pause_async` / `resume_async` → mock `pause()` / `resume()` mutate
    `subscriptions` + the `paused` set — **infallible / broker-free**.
  - `seek_to_beginning_async` / `seek_to_end_async` → mock `seek_to_beginning()` /
    `seek_to_end()` call **only** `subscriptions.request_offset_reset_all(partitions,
    EARLIEST|LATEST)` — they set the *reset strategy*, and do **NOT** read
    `beginning_offsets` / `end_offsets` at seek time. Those maps are consulted **lazily**,
    only later in `poll()`'s `reset_offset_position`. **Therefore the seek ops themselves
    resolve successfully broker-free with no offset setup** (the `update_*_offsets` helpers
    are needed only if a test wants to *poll after the seek* and observe the reset
    position — an optional deeper test, §6).

**Nothing in this phase is Mode B.** All ABI symbols exist; the work is header-down C#.

---

## 1 · DECISION 1 (THE KEY ONE) — reconcile the existing sync `Assign`

### 1.1 The collision, stated precisely

Today there are two `Assign`s, at two layers:

  - **`NativeConsumer.Assign(IReadOnlyList<(string Topic, int Partition)>)`** — an
    **internal sync** method (`Internal/NativeConsumer.cs`) calling `Consumer_assign`
    (returns `KafkaError*` directly). It marshals the parallel `(topics[], partitions[],
    count)` arrays — **the exact input marshalling this phase reuses.**
  - **`AsyncMockConsumer.Assign(IReadOnlyList<TopicPartition>)`** — a **public inherent
    mock-setup helper** (sync, `void`), forwarding to the internal one. Used by M3/M4/M5
    tests to set up an assignment before poll/seek/position.

Category C adds the **public async** Java `assign(Collection)` — `Task
Assign(IReadOnlyCollection<TopicPartition>, CancellationToken = default)` on
`IAsyncConsumer` via `assign_async`. That would collide with the public sync mock helper:
**two public `Assign`s on `AsyncMockConsumer` — one sync `void`, one async `Task`,
differing only by collection type** — a classic "forgot to `await`" footgun. This is the
one thing CLAUDE.md-adjacent taste forbids.

### 1.2 Call-site inventory (blast radius — verified)

`grep` of `bindings/dotnet/tests/` for `.Assign(`:

  - **10 public-root call sites** across 5 files use the **public** helper
    (`consumer.Assign(new[] { new TopicPartition(...) })`):
    `PublicConsumerRoundTripTests.cs` (1), `PublicConsumerSyncReadTests.cs` (6),
    `PublicConsumerAllocationBudgetTests.cs` (1), `PublicConsumerTfmSmokeTests.cs` (1),
    `PublicConsumerPositionTests.cs` (1). **These are the migration set.**
  - **4 Interop call sites** (`Interop/ConsumerPoll*Tests.cs`) use the **internal
    tuple** form directly on a `NativeConsumer` — `consumer.Assign(new[] { (topic,
    partition) })` via the `MockReadyToPoll` helper. **These are NOT affected** — they
    target `NativeConsumer.Assign((string,int)[])`, a different (internal) API.

So the internal sync `NativeConsumer.Assign` + the `Consumer_assign` DllImport **must
stay** — the 4 Interop tests depend on them directly, and the async public path reuses the
same array-marshalling logic (§3).

### 1.3 RECOMMENDATION (preferred): promote `Assign` to the public **async** member, migrate the 10 test-setup sites, keep the internal sync path

**Decision: `IAsyncConsumer` gains `Task Assign(IReadOnlyCollection<TopicPartition>,
CancellationToken = default)` (via `assign_async`), and the public inherent sync
`AsyncMockConsumer.Assign(IReadOnlyList<TopicPartition>)` helper is REMOVED.** The 10
public-root test-setup sites migrate `consumer.Assign(x)` → `await consumer.Assign(x)`.

Rationale:

  - **No public sync/async `Assign` overload pair** — the footgun is eliminated. There is
    exactly one public `Assign`, and it is `Task` (Java-faithful: Java's `assign` is on the
    `Consumer` interface, and it blocks in `AsyncKafkaConsumer`).
  - **Java parity.** Java's `assign(Collection)` is a first-class `Consumer` member, not a
    mock-only helper. Putting it on `IAsyncConsumer` (both `AsyncKafkaConsumer` and
    `AsyncMockConsumer`) matches the Java shape and the additive-growth contract already
    documented in `IAsyncConsumer.cs` remarks ("pause / resume, … arrive in later phases as
    additive members").
  - **The mock still gets its assignment setup** — via the *public async* `Assign` (verified
    broker-free: `assign_async` → mock `assign()` → `assign_from_user`, infallible, §0). Test
    setup becomes `await consumer.Assign(new[] { tp })`, which is strictly more Java-faithful
    than the old sync helper and needs no separate mock-only method.
  - **The internal sync `NativeConsumer.Assign((string,int)[])` + `Consumer_assign` DllImport
    STAY** — the 4 Interop tests use them directly, and (see §3) the *async* `AssignWithCallback`
    reuses the same private array-marshalling helper. We do **not** remove the sync ABI path;
    we only remove the *public* sync `void Assign` surface on `AsyncMockConsumer`.

**Fate of the internal sync method:** keep `NativeConsumer.Assign((string,int)[])` as
internal-only (still called by the 4 Interop tests). Add a new internal async
`NativeConsumer.AssignWithCallback(IReadOnlyCollection<TopicPartition>, CT)` for the public
path. Both share one private array-marshalling helper (§3) — no duplicated pinning logic.

**Alternative considered (rejected): keep a sync mock helper under a mock-only name**
(e.g. `SetAssignment(...)`) and add the public async `Assign`. Rejected because (a) it
keeps two ways to assign on the mock with no Java counterpart for the sync one, (b) the
async `Assign` is already broker-free so the sync helper buys nothing, and (c) it grows the
mock's inherent surface for no parity reason. The only cost of the preferred option is a
mechanical 10-site `await` migration — cheap, and it makes the tests more correct.

**Open question for the user (§10 Q1):** confirm the 10-site `await` migration is
acceptable (it is a test-only, mechanical change), and confirm removing the public inherent
sync `AsyncMockConsumer.Assign` is desired (vs. keeping it under a renamed mock-only name).

---

## 2 · DECISION 2 — API shape (the five members)

**Decision: add exactly these five to `IAsyncConsumer` (async, on the interface — both
`AsyncKafkaConsumer` and `AsyncMockConsumer` forward to `NativeConsumer`); no new public
value types.**

```csharp
Task Assign(IReadOnlyCollection<TopicPartition> partitions, CancellationToken cancellationToken = default);
Task Pause(IReadOnlyCollection<TopicPartition> partitions, CancellationToken cancellationToken = default);
Task Resume(IReadOnlyCollection<TopicPartition> partitions, CancellationToken cancellationToken = default);
Task SeekToBeginning(IReadOnlyCollection<TopicPartition> partitions, CancellationToken cancellationToken = default);
Task SeekToEnd(IReadOnlyCollection<TopicPartition> partitions, CancellationToken cancellationToken = default);
```

  - **Names mirror Java** — `SeekToBeginning` / `SeekToEnd` (PascalCase of Java's
    `seekToBeginning` / `seekToEnd`); no `Async` suffix (the async distinction is carried by
    the interface, CLAUDE.md §4).
  - **`IReadOnlyCollection<TopicPartition>`** input (Java `Collection`; the idiom-map row
    `Set`/`Collection` → `IReadOnlyCollection`). Matches `Subscribe`'s
    `IReadOnlyCollection<string>` precedent — order does not matter for these ops, so a
    `Collection`, not a `List`.
  - **All async** — each blocks in Java (§0). No sync variants, no `TimeSpan` overloads (none
    of the five takes a `Duration` in Java).
  - **No new public types** — `TopicPartition` already exists; the void bridge returns
    `Task` (no result).
  - **`Seek` (single-partition, M4) is untouched** — it stays `Task Seek(TopicPartition, long
    offset, CT)`. `SeekToBeginning`/`SeekToEnd` are the collection-reset siblings, distinct
    members.

---

## 3 · DECISION 3 — input marshalling (`TopicPartition` collection → parallel arrays)

**Decision: one shared private helper on `NativeConsumer` marshals a
`TopicPartition` collection into the ABI's `(IntPtr[] topics, int[] partitions, int
count)` parallel arrays, pinned call-scoped around the submit, reusing the exact pattern
from the shipped sync `Assign`.**

  - Extract the array-building + call-scoped UTF-8 pinning already in the sync
    `NativeConsumer.Assign` (lines that build `Utf8Marshal.PinnedUtf8String?[] pins` +
    `IntPtr[] pointers` + `int[] partitions`, pin each topic, and `finally`-dispose the pins)
    into a private helper used by **all five** `<Op>WithCallback` methods and by the retained
    sync `Assign`. Shape:

    ```csharp
    // Pins topics call-scoped, fills the parallel arrays, runs `body`, unpins in finally.
    private void WithPinnedPartitionArrays(
        IReadOnlyCollection<TopicPartition> partitions,
        Action<IntPtr[] /*topics*/, int[] /*partitions*/, int /*count*/> body);
    ```

  - **Call-scoped pin (ffi §A3/§B3/§A4):** each `_async` fn reads the topic strings +
    partition ints **synchronously** into an owned Rust `Vec` during the submit call (before
    spawning) — verified: the FFI wrappers copy `tps` before `async_void_op`. So the pins are
    freed the moment the native submit returns (the `subscribe_async` topics-array precedent,
    already shipped in `SubscribeWithCallback`). Do **not** hold pins across the returned
    `Task`.
  - **No per-element copy beyond the UTF-8 encode** (§A4/§B4) — the topic bytes are pinned,
    not copied; the `int32_t` partitions are a plain blittable `int[]`. Only the encoding
    conversion (UTF-8) allocates, which is the unavoidable string-marshalling cost.
  - Each op is a thin `NativeConsumer.<Op>WithCallback(IReadOnlyCollection<TopicPartition>,
    CT)` that validates preconditions (§5), then
    `SubmitVoidOperation(ct, (consumer, cb, ud) => WithPinnedPartitionArrays(partitions,
    (topics, parts, count) => NativeMethods.Consumer<Op>Async(consumer, topics, parts, count,
    cb, ud)))`.

---

## 4 · DECISION 4 — void bridge reuse (NO new bridge, unlike M5/P2)

**Decision: reuse `NativeConsumer.SubmitVoidOperation` + `ConsumerCallbacks.Operation`
(the `op_callback_t` trampoline) verbatim — confirm NO new callback / delegate /
trampoline / submit helper is added.**

  - The five ops are exactly the `Subscribe` / `Unsubscribe` / `Seek` shape: void result,
    `op_callback_t`, `OperationCompletionSource` (non-generic). M5/P2's scalar bridge
    (`SubmitScalarOperation<T>`, `ConsumerCallbacks.Position`) is **not** touched or reused
    here.
  - Each `<Op>WithCallback` calls `SubmitVoidOperation(ct, submit)` where `submit` is the
    pin-and-P/Invoke lambda from §3. This is the M5/P2 PLAN §1.3 discipline in reverse: we add
    zero bridge machinery — the proven void path is left byte-for-byte untouched, only new
    call sites and new DllImports are introduced.
  - **Critic check (DoD §11 / this PLAN):** verify no new `Native*Submit` delegate, no new
    `ConsumerCallbacks.*` delegate/instance, no new `OperationCompletionSource` variant. If the
    Actor adds any, it is a finding.

---

## 5 · DECISION 5 — error / precondition mapping (ffi §B5)

**Operational (core) failures** → faulted `Task` with `KafkaException` (via the void
bridge's error path, `FromHandle`). Concurrent async op → faulted `Task`
(`ConcurrentModification`, delivered by the core inline). Post-dispose →
`ObjectDisposedException` (`ThrowIfClosed` inside `SubmitVoidOperation`, already there).
Cancellation → `wakeup()` best-effort; a pre-canceled token →
`OperationCanceledException` synchronously (already handled by `SubmitVoidOperation`'s
`cancellationToken.ThrowIfCancellationRequested()`).

**Preconditions — validated BEFORE any pin / P-Invoke (mandatory, ffi §B5; the ABI
panics on violation):**

  - `partitions` collection is null → `ArgumentNullException` (matches `Subscribe`'s null-
    topics precedent).
  - a `TopicPartition` with a null `Topic` → `ArgumentException` (matches `Subscribe`'s per-
    element null-topic precedent) — throw with the collection param name.
  - a negative `Partition` → `ArgumentOutOfRangeException` (the `Seek` / sync-`Assign`
    precedent — the ABI silently maps negative to "unset", so the binding must reject it).

**Empty-collection semantics — RESOLVED against the Java/Rust source (state it in the
member rustdoc):**

  - `assign([])` — Java `assign(emptyList)` **clears the assignment** (assigns to the empty
    set); the Rust mock's `assign_from_user(HashSet::new())` does the same. So `Assign` with an
    empty collection is a **valid clear-assignment**, NOT a no-op error. Handle it: `count ==
    0` → pass `(empty arrays, 0)` to `assign_async`; the op resolves successfully. Do NOT
    special-case-throw on empty.
  - `pause([])` / `resume([])` / `seekToBeginning([])` / `seekToEnd([])` — Java iterates the
    (empty) collection → **no-op success**; the Rust mock loops over an empty slice →
    `Ok(())`. So all four with an empty collection resolve successfully (a benign no-op). Do
    NOT throw.

  So: **an empty collection is always valid** for all five (a clear for `assign`, a no-op
  for the others). The binding passes `count == 0` through; it does not reject empty. (A
  `null` collection is still rejected — null ≠ empty, matching Java's NPE-on-null vs
  no-op-on-empty.)

---

## 6 · Tests (DoD §3 + CLAUDE.md §7.4; broker-free via `AsyncMockConsumer`)

All public-surface tests live at the **test ROOT** (`PublicConsumer…Tests.cs`, NOT
`Interop/`), driven broker-free via `AsyncMockConsumer`. Serial execution (D8.8 —
`[Collection]` no-parallel) stays. Each awaited op uses a timeout (the completion /
deadlock regression guard, §7.4).

New file(s): `PublicConsumerPartitionOpsTests.cs` (root). Cases:

  1. **`Assign` then `Assignment()` reflects it** — `await consumer.Assign(new[] { tp1, tp2
     })`; `Assignment()` returns exactly `{tp1, tp2}`.
  2. **`Assign([])` clears the assignment** — assign a set, then `await Assign(empty)`;
     `Assignment()` is empty (the resolved empty-clear semantics, §5).
  3. **`Pause` then `Paused()` returns the paused set** — assign `{tp}`, `await
     consumer.Pause(new[] { tp })`, assert `Paused()` == `{tp}`. **This finally makes the
     non-empty `Paused()` case reachable** (explicitly deferred in M5/P1's `Paused()` rustdoc —
     "not reachable broker-free until a public `Pause` lands"). **ADD this test** and note in
     the PLAN/COMMENTS.DONE that it closes the M5/P1 gap.
  4. **`Resume` clears the paused set** — after (3), `await consumer.Resume(new[] { tp })`;
     `Paused()` is empty.
  5. **`SeekToBeginning` / `SeekToEnd` resolve broker-free** — assign `{tp}`, `await
     consumer.SeekToBeginning(new[] { tp })` (and `SeekToEnd`) completes successfully with no
     offset setup (the reset-strategy-only path, §0). This is the baseline reachable slice.
  6. **`SeekToBeginning` observable via poll (optional deeper test — needs the mock offset
     helpers)** — to observe that the seek actually reset the position, a follow-up `Poll` must
     resolve the reset offset, which requires `update_beginning_offsets` on the mock. **Wire the
     two mock offset helpers this phase** (they are simple one-arg-each DllImports, §0) and add a
     mock-only `AsyncMockConsumer.UpdateBeginningOffset(topic, partition, offset)` /
     `UpdateEndOffset(...)` inherent helper, so this test can: set the beginning offset, seek to
     beginning, add a record at that offset, poll, and observe the record. If the user prefers to
     defer the offset helpers, keep only case (5) and record the reachable-slice limit — but the
     recommendation is to wire them (small, and it closes the loop on `SeekTo*` observability).
  7. **Empty-collection no-op success** — `pause([])` / `resume([])` / `seekToBeginning([])`
     / `seekToEnd([])` each resolve successfully (§5).
  8. **Error-message asserted on a failure path** — a genuine broker-free failure. Candidate:
     drive an operational failure via the mock and assert the `KafkaException.Message` content
     (DoD §3 — error messages are part of the contract; not just `is_err`). If no
     partition-op has a clean broker-free operational failure on the mock, use the **concurrent
     async op → faulted `Task` (`ConcurrentModification`)** path and assert its message. (The
     Actor determines the cleanest reachable failure and documents the choice.)
  9. **Preconditions (deterministic, before any native call)** — null collection →
     `ArgumentNullException`; element with null topic → `ArgumentException`; negative partition
     → `ArgumentOutOfRangeException`. One per op family (parametrized).
  10. **Post-dispose** — after `DisposeAsync`, each op throws `ObjectDisposedException`.
  11. **Cancellation / wakeup** — a pre-canceled token → `OperationCanceledException`
      synchronously; `Wakeup()` during an in-flight op faults/cancels once (mirrors the
      `Subscribe`/`Poll` precedent).
  12. **Allocation sanity** — a per-op allocation check in the spirit of DoD §10 / §7.4: the
      collection→arrays marshalling adds only the per-topic UTF-8 encode allocation (+ the two
      arrays), no per-element copy of partition ints or extra buffers. Follow the shipped
      `PublicConsumerAllocationBudgetTests` precedent for the assertion style.

**Migration (from §1.3):** update the **10** public-root `consumer.Assign(...)` call
sites to `await consumer.Assign(...)` and remove the public inherent sync
`AsyncMockConsumer.Assign`. The 4 `Interop/` sites are untouched.

**Not-reachable-broker-free flags:** none of the five ops is unreachable broker-free
(all resolve on the mock, §0). Only the *observation* of `SeekTo*` via poll needs the mock
offset helpers (case 6) — that is the sole conditional test, handled per §6.6.

---

## 7 · `NativeConsumer` / interop additions (summary)

  - **`NativeMethods` (5 new `_async` void DllImports + optionally 2 mock offset helpers):**
    `ConsumerAssignAsync`, `ConsumerPauseAsync`, `ConsumerResumeAsync`,
    `ConsumerSeekToBeginningAsync`, `ConsumerSeekToEndAsync` — each `void`, signature `(IntPtr
    consumer, IntPtr[] topics, int[] partitions, int count, ConsumerCallbacks.OperationCallback
    callback, IntPtr userData)`, with the full `EntryPoint = "kafka_consumer_Consumer_<op>_async"`.
    Plus (for §6.6) `MockConsumerUpdateBeginningOffsets` / `MockConsumerUpdateEndOffsets`
    (`IntPtr consumer, IntPtr topic, int partition, long offset` → `IntPtr` error). These mirror
    the existing `ConsumerSubscribeAsync` / `ConsumerAssign` declaration style exactly.
  - **`NativeConsumer` (5 new async methods + 1 shared marshalling helper):**
    `AssignWithCallback` / `PauseWithCallback` / `ResumeWithCallback` /
    `SeekToBeginningWithCallback` / `SeekToEndWithCallback`, each `SubmitVoidOperation` over its
    `_async` DllImport via the shared `WithPinnedPartitionArrays` helper (§3). Keep the existing
    internal sync `Assign((string,int)[])` (Interop tests). Optionally add mock-only
    `UpdateBeginningOffset` / `UpdateEndOffset` forwarders for §6.6.
  - **`IAsyncConsumer` (5 new members)** — §2. **`AsyncKafkaConsumer` + `AsyncMockConsumer`
    (5 new forwarders each)**; remove the public inherent sync `AsyncMockConsumer.Assign`.
  - **Doc-sync:** update `IAsyncConsumer.cs` "additive-growth surface" remark (pause/resume/
    assign/seekTo* now shipped), and the M5/P1 `Paused()` rustdoc note ("non-empty not reachable
    until a public Pause lands") — that limitation is now lifted.

---

## 8 · Definition of Done (Actor must pass all)

1. `cargo build --features ffi` (prerequisite native build) succeeds; the five `_async`
   symbols + the two mock offset symbols are present in the header (verified — §0).
2. `dotnet build` on the TFM matrix (net462 via ns2.0, net8.0, net10.0) is clean;
   `dotnet format` + analyzers clean.
3. `dotnet test` green — all §6 cases pass, incl. the migrated 10 `Assign` sites and the new
   non-empty `Paused()` test (§6.3).
4. **No new bridge machinery** (§4) — the void bridge + `ConsumerCallbacks.Operation` are
   reused verbatim; a Critic finding otherwise.
5. **Zero-copy / call-scoped pinning** honored (§3) — pins freed at submit return, no pin
   across the `Task`, no per-element byte copy.
6. **Preconditions before P/Invoke** for all five (§5); **empty-collection semantics** as
   resolved (§5) — no spurious throw on empty.
7. **Assign reconciliation** complete (§1) — exactly one public `Assign` (async), no
   sync/async overload pair; internal sync path + 4 Interop tests intact.
8. Error-message content asserted on a failure path (§6.8); allocation sanity (§6.12).
9. `ffi-marshalling.md` anti-patterns satisfied (§B5/§B6/§B7, §A3/§A4).

---

## 9 · Governance / mechanics

  - **Personas:** `dotnet-actor` / `dotnet-critic` (copied to repo-root `.claude/agents/` per
    the nested-discovery workaround; re-copy after any edit — CLAUDE.md §8.4). Manager is the
    root `project-manager`.
  - **Comments:** `bindings/dotnet/COMMENTS.12.md` (approved issues) / `COMMENTS.DONE.12.md`
    (resolved) — both local working files, never committed at the binding root. On phase close
    the Manager archives `COMMENTS.DONE.12.md` to
    `bindings/dotnet/design/history/M5/P3-consumer-partition-ops/` and resets
    `COMMENTS.12.md`.
  - **Review ground truth:** the C ABI header + the Kafka Java public-API shape — not Rust
    internals, not Java implementation logic (CLAUDE.md §8.2).
  - **Plan archive:** this PLAN at
    `bindings/dotnet/design/history/M5/P3-consumer-partition-ops/PLAN.md`. Living
    status/structure/design updates go in `bindings/dotnet/design/current/` on phase close.

---

## 10 · Resolved (user review, 2026-08-06)

All three decisions confirmed as recommended:

  1. **Assign reconciliation (§1.3)** — **promote `Assign` to the public async member** on
     `IAsyncConsumer`; **remove** the public inherent sync `AsyncMockConsumer.Assign`; migrate
     the 10 public-root test-setup sites to `await consumer.Assign(...)`. The internal sync
     `NativeConsumer.Assign` + `Consumer_assign` DllImport + the 4 Interop tests stay.
  2. **`SeekTo*` observability (§6.6)** — **wire the two mock offset-update helpers this
     phase** (`MockConsumer_update_beginning_offsets` / `_update_end_offsets`: 2 DllImports +
     2 mock forwarders), so `SeekToBeginning`/`SeekToEnd` are observed end-to-end via a
     follow-up poll (also closes a Python-mock parity gap). Mode A.
  3. **Label/number** — **M5/P3**, slug `P3-consumer-partition-ops`, **N=12**.

**Awaiting:** final user approval to run the N=12 Actor → Critic loop.

---

## Appendix · Reachability & marshalling facts verified during planning

  - Five `_async` fns + `op_callback_t` present in `target/include/confluent_kafka.h`
    (identical `(topics, partitions, count, callback, user_data)` shape).
  - `MockConsumer_update_beginning_offsets` / `_update_end_offsets` present (per-(topic,
    partition, offset), not maps).
  - `src/ffi/consumer.rs`: `assign_async`/`pause_async`/`resume_async`/`seek_to_*_async`
    dispatch to the mock's infallible trait methods (`async_void_op(... c.<op>(tps))`).
  - `src/consumer/mock_consumer.rs`: `seek_to_beginning`/`seek_to_end` call only
    `request_offset_reset_all` (no offset-map read at seek time) → seek resolves broker-free
    with no offset setup; offsets are consulted lazily in `poll()`'s `reset_offset_position`.
  - `assign` → `assign_from_user` (empty set clears the assignment); `pause`/`resume` loop
    over the slice (empty = no-op). All infallible on the mock.
  - Existing sync `NativeConsumer.Assign((string,int)[])` holds the exact parallel-array
    call-scoped pinning pattern to extract/reuse.
  - Test call-site split: 10 public-root `.Assign(` (migration set) vs 4 `Interop/` (internal
    tuple form, untouched).
