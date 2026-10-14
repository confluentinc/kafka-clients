# M5/P4 — Consumer offset-map query siblings (Category E1: owned-handle `Map<TopicPartition, value>` queries)

**Status:** DRAFT — awaiting user approval. **Review number: N=13** (last completed:
N=12, the M5/P3 "Consumer partition ops", which shipped `Assign` / `Pause` / `Resume` /
`SeekToBeginning` / `SeekToEnd` via the void bridge + `WithPinnedTopics`).

**Milestone / phase label (PROPOSED):** **Milestone 5 / Phase 4 — "Consumer offset-map
query siblings"**, slug `P4-consumer-offset-queries`, at
`bindings/dotnet/design/history/M5/P4-consumer-offset-queries/`, consistent with M5/P1–P3.

**Mode:** **A (`.NET-only`, no Rust authored).** All four `_async` functions
(`committed_async` / `offsets_for_times_async` / `beginning_offsets_async` /
`end_offsets_async`), their three container types (`OffsetMap_t` /
`OffsetAndTimestampMap_t` / `LongOffsetMap_t`) + accessors, the two value types
(`OffsetAndMetadata_t` / `OffsetAndTimestamp_t`) + accessors, and the three callback
typedefs (`committed_callback_t` / `offsets_for_times_callback_t` /
`long_offsets_callback_t`) already ship in `target/include/confluent_kafka.h` (verified —
§0). `cargo build --features ffi` remains a *prerequisite build step*, not a change.

**Branch:** commits land on **`prashah_dev_public_consumer_remaining`** (the branch the
user created for post-M4 consumer work; M5/P1–P3 shipped there). Treated as a new PR for
M5/P4.

---

## THE KEY DECISION (up front): split Category E into E1 (now, N=13) and E2 (later)

**Recommendation: SPLIT.** This plan is **E1 — the offset-map family** only:
`Committed` / `OffsetsForTimes` / `BeginningOffsets` / `EndOffsets`. **E2 — the
partition-metadata family** (`PartitionsFor` / `ListTopics`, with the nested
`PartitionInfo` + `Node` value types and the `update_partitions` mock helper) is
deferred to a **separate later plan (N=14)** and is **not drafted in detail here** (a
scope sketch only, §9).

**Rationale (diff size · review focus · type complexity):**

  1. **Two coherent, self-contained value-type + container families.** E1 is *all*
     `Map<TopicPartition, value>` results — one container-marshaller shape (map: count →
     per-entry key `TopicPartition_t` + value), two flat value types (`OffsetAndMetadata`,
     `OffsetAndTimestamp`) plus a bare `long`, TP-collection / TP→timestamp input (the
     `WithPinnedTopics` precedent + one timestamps array). E2 is a *different* shape: a
     **nested** `List<PartitionInfo>` where each `PartitionInfo` carries **three replica
     `Node` lists** — a materially more complex copy-out (list-of-structs-of-lists) and two
     new public types with sub-objects. Bundling them makes one review span two unrelated
     marshalling shapes.
  2. **Diff size.** E1 alone is 4 methods, 3 container marshallers, 2 flat value types, 3
     callback trampolines, 2 input shapes — already a large phase, comparable to M5/P3.
     Adding E2's nested `PartitionInfo`/`Node` copy-out + `update_partitions` wiring +
     `PartitionsFor`/`ListTopics` tests roughly doubles it. A ~2× diff dilutes Critic
     attention exactly where the subtle bugs live (the receive-path copy-out / borrow-root
     discipline, §B2/§B4).
  3. **`PartitionInfo`/`Node` deserve their own review.** They are the first **nested**
     public value types in the binding (every prior type — `TopicPartition`,
     `ConsumerGroupMetadata`, `OffsetAndMetadata`, `OffsetAndTimestamp` — is flat). The
     replica-list copy-out (a `Node[]` per `PartitionInfo`, borrowed `Node_t` elements that
     must NOT be freed — §B2 Category 4) is a distinct correctness surface worth isolating.
  4. **The two families share only the bridge, which E1 establishes.** E1 builds the
     owned-handle **map** copy-out pattern and the per-container submit/trampoline clones;
     E2 reuses that machinery for the **list/nested** shape. Sequencing E1 → E2 means E2
     inherits a proven owned-handle-container template (just as M5/P3 inherited M5/P2's
     bridge), rather than inventing two shapes at once.

**Cost of the split:** two PRs instead of one; a second value-type-family review cycle.
Accepted — the review-focus and type-complexity gains dominate. **This is the main open
question for the user (§10 Q1).**

---

## 0 · Scope & ABI ground truth (verified in the generated header)

E1 adds the **four async offset-map queries** — the `Map<TopicPartition, value>` subset of
"Category E". All four **block in Java's `AsyncKafkaConsumer`** (each does a cross-thread
event round-trip / metadata wait — consumer-threading §1 lists `committed`,
`beginningOffsets`, `endOffsets`, `offsetsForTimes` among the blocking query APIs), so per
the CLAUDE.md §4 idiom map they are **async** (`Task<...>`). All four resolve via the
**already-proven owned-handle completion bridge** (`SubmitOperation<T>` + the `OnPoll`
trampoline shape: callback owns a result container → copy-out to a managed type →
`_destroy`).

| Member | Java | .NET (proposed) | ABI `_async` fn (verified) | Container | Callback | New value type |
|---|---|---|---|---|---|---|
| `Committed` | `committed(Set<TopicPartition>)` → `Map<TP,OffsetAndMetadata>` | `Task<IReadOnlyDictionary<TopicPartition,OffsetAndMetadata>> Committed(IReadOnlyCollection<TopicPartition>, CT)` | `committed_async` | `OffsetMap_t` | `committed_callback_t` | **`OffsetAndMetadata`** |
| `OffsetsForTimes` | `offsetsForTimes(Map<TP,Long>)` → `Map<TP,OffsetAndTimestamp>` | `Task<IReadOnlyDictionary<TopicPartition,OffsetAndTimestamp>> OffsetsForTimes(IReadOnlyDictionary<TopicPartition,long>, CT)` | `offsets_for_times_async` | `OffsetAndTimestampMap_t` | `offsets_for_times_callback_t` | **`OffsetAndTimestamp`** |
| `BeginningOffsets` | `beginningOffsets(Collection<TP>)` → `Map<TP,Long>` | `Task<IReadOnlyDictionary<TopicPartition,long>> BeginningOffsets(IReadOnlyCollection<TopicPartition>, CT)` | `beginning_offsets_async` | `LongOffsetMap_t` | `long_offsets_callback_t` (shared) | — |
| `EndOffsets` | `endOffsets(Collection<TP>)` → `Map<TP,Long>` | `Task<IReadOnlyDictionary<TopicPartition,long>> EndOffsets(IReadOnlyCollection<TP>, CT)` | `end_offsets_async` | `LongOffsetMap_t` | `long_offsets_callback_t` (shared) | — |

**`_async` signatures (exact — verified in `target/include/confluent_kafka.h`):**

```c
void kafka_consumer_Consumer_committed_async(
    const kafka_consumer_Consumer_t*, const char *const *topics, const int32_t *partitions,
    int32_t count, kafka_consumer_Consumer_committed_callback_t callback, void *user_data);

void kafka_consumer_Consumer_offsets_for_times_async(
    const kafka_consumer_Consumer_t*, const char *const *topics, const int32_t *partitions,
    const int64_t *timestamps, int32_t count,
    kafka_consumer_Consumer_offsets_for_times_callback_t callback, void *user_data);

void kafka_consumer_Consumer_beginning_offsets_async(
    const kafka_consumer_Consumer_t*, const char *const *topics, const int32_t *partitions,
    int32_t count, kafka_consumer_Consumer_long_offsets_callback_t callback, void *user_data);

void kafka_consumer_Consumer_end_offsets_async(
    const kafka_consumer_Consumer_t*, const char *const *topics, const int32_t *partitions,
    int32_t count, kafka_consumer_Consumer_long_offsets_callback_t callback, void *user_data);
```

**Callback typedefs (all the owned-handle shape `(container*, error*, ud)` — verified):**

```c
typedef void (*kafka_consumer_Consumer_committed_callback_t)       (kafka_consumer_OffsetMap_t*,             kafka_common_KafkaError_t*, void*);
typedef void (*kafka_consumer_Consumer_offsets_for_times_callback_t)(kafka_consumer_OffsetAndTimestampMap_t*, kafka_common_KafkaError_t*, void*);
typedef void (*kafka_consumer_Consumer_long_offsets_callback_t)     (kafka_consumer_LongOffsetMap_t*,         kafka_common_KafkaError_t*, void*);
```

Success = container non-null, error null; failure (incl. inline core-guard rejection) =
container null, error non-null; **the callback owns whichever is non-null** (free the
container via its `_destroy`, or the error via `KafkaException.FromHandle`). **Exactly the
`OnPoll` contract**, with `ConsumerRecords_t` swapped for the offset-map container. Note
`beginning_offsets_async` and `end_offsets_async` **share** `long_offsets_callback_t` →
**one** trampoline instance serves both.

**Container accessors (verified):**

```c
int32_t  OffsetMap_count(const OffsetMap_t*);
const TopicPartition_t*      OffsetMap_get_key  (const OffsetMap_t*, int32_t i);   // borrowed
const OffsetAndMetadata_t*   OffsetMap_get_value(const OffsetMap_t*, int32_t i);   // borrowed
void     OffsetMap_destroy(OffsetMap_t*);
// OffsetAndTimestampMap_* — identical, value → const OffsetAndTimestamp_t*
// LongOffsetMap_*        — identical, value → int64_t (by value, no handle)
```

**Value-type accessors (verified):**

```c
int64_t OffsetAndMetadata_offset(const OffsetAndMetadata_t*);
const char* OffsetAndMetadata_metadata(const OffsetAndMetadata_t*);          // NUL-terminated, handle-owned
bool    OffsetAndMetadata_leader_epoch(const OffsetAndMetadata_t*, int32_t *out_epoch);  // false = absent
// (no _destroy needed by us — borrowed element of OffsetMap; see §4)
int64_t OffsetAndTimestamp_offset(const OffsetAndTimestamp_t*);
int64_t OffsetAndTimestamp_timestamp(const OffsetAndTimestamp_t*);           // ms since epoch
bool    OffsetAndTimestamp_leader_epoch(const OffsetAndTimestamp_t*, int32_t *out_epoch);
```

`TopicPartition_t` (the map keys) accessors — already used elsewhere:
`TopicPartition_topic` (NUL-terminated `const char*`), `TopicPartition_partition` (i32).

**Nothing in E1 is Mode B.** All ABI symbols exist; the work is header-down C#.

---

## 1 · DECISION 1 — the two new public value types (`OffsetAndMetadata`, `OffsetAndTimestamp`)

**Decision: add two flat public value types in the library-project root
(`Confluent.Kafka` namespace), matching the shipped value-type style; both are
Java-faithful field sets, both `sealed class` with getter-properties (the
`ConsumerGroupMetadata` precedent), immutable.**

```csharp
public sealed class OffsetAndMetadata            // Java org.apache.kafka.clients.consumer.OffsetAndMetadata
{
    public long Offset { get; }
    public string Metadata { get; }              // Java: "" when unset (never null); ABI returns a NUL-terminated string
    public int? LeaderEpoch { get; }             // Java Optional<Integer>; null = absent (ABI presence-flag bool)
}

public sealed class OffsetAndTimestamp           // Java org.apache.kafka.clients.consumer.OffsetAndTimestamp
{
    public long Offset { get; }
    public long Timestamp { get; }               // ms since epoch
    public int? LeaderEpoch { get; }             // Optional<Integer>; null = absent
}
```

  - **`sealed class`, not `readonly struct`.** They are result payloads read once and put
    into a dictionary (never a hot-path per-record type), and `OffsetAndMetadata` carries a
    `string` — a class matches the `ConsumerGroupMetadata` precedent and avoids the
    `default(struct)` footgun `TopicPartition` documents. (`TopicPartition` is a struct only
    because it is a hot map key.)
  - **`LeaderEpoch` → `int?`.** The ABI presence flag (`bool` return + `out_epoch`) maps to
    nullable: `false` → `null`, `true` → `out_epoch`. Java's `Optional<Integer>` → `int?` is
    the idiom-map form. **Critic check:** the presence flag must be honored — a value type that
    hardcodes `LeaderEpoch = out_epoch` ignoring the `bool` return is a finding.
  - **`Metadata` → non-null `string`.** Java's `OffsetAndMetadata.metadata()` is never null
    (defaults to `""`); the ABI accessor returns a NUL-terminated handle-owned string. Marshal
    via `Utf8Marshal.PtrToString(ptr)` (NUL-scan form, §B3) — **copied out** before the owning
    container is destroyed.
  - **No `equals`/`hashCode` obligation surfaced by these tests** (they are dictionary
    *values*, not keys). Add `ToString()` for debuggability (the shipped value-type habit); a
    full `IEquatable` is optional — include it only if a test asserts value equality, else keep
    minimal (the Actor decides, documents in COMMENTS.DONE).
  - **Placement:** flat at the library-project root (`src/Confluent.Kafka/OffsetAndMetadata.cs`,
    `OffsetAndTimestamp.cs`) — the surface is still small (CLAUDE.md §2 — topical folders only
    once a family grows).

**These two types are shared with E2? No.** E2 introduces `PartitionInfo` + `Node`; there is
no type overlap. E1 and E2 are cleanly separable at the type boundary (part of why the split
is clean).

---

## 2 · DECISION 2 — API shape (the four members on `IAsyncConsumer`)

**Decision: add exactly these four to `IAsyncConsumer` (async, on the interface — both
`AsyncKafkaConsumer` and `AsyncMockConsumer` forward to `NativeConsumer`).**

```csharp
Task<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>> Committed(
    IReadOnlyCollection<TopicPartition> partitions, CancellationToken cancellationToken = default);

Task<IReadOnlyDictionary<TopicPartition, OffsetAndTimestamp>> OffsetsForTimes(
    IReadOnlyDictionary<TopicPartition, long> timestampsToSearch, CancellationToken cancellationToken = default);

Task<IReadOnlyDictionary<TopicPartition, long>> BeginningOffsets(
    IReadOnlyCollection<TopicPartition> partitions, CancellationToken cancellationToken = default);

Task<IReadOnlyDictionary<TopicPartition, long>> EndOffsets(
    IReadOnlyCollection<TopicPartition> partitions, CancellationToken cancellationToken = default);
```

  - **Names mirror Java** — no `Async` suffix (interface carries the async distinction,
    CLAUDE.md §4).
  - **Return types per the idiom map** — Java `Map` → `IReadOnlyDictionary`; Java `Long`
    values → `long` (never boxed `Long?`); `Set`/`Collection` inputs →
    `IReadOnlyCollection<TopicPartition>` (matches the M5/P3 partition-ops precedent).
  - **`OffsetsForTimes` input is a `Map<TP,Long>`** → `IReadOnlyDictionary<TopicPartition,long>`
    (Java `Map<TopicPartition, Long>`). The only member here with a **map input**, not a
    collection — needs the timestamps-array marshalling variant (§3).
  - **NO `TimeSpan` overload** for any of the four — the resolved `Position` / `Close`
    precedent: the async ABI exposes **no** timeout parameter, so the timed Java
    `(…, Duration)` overload is **deferred** until the ABI grows one. State this in each
    member's rustdoc. (Do not re-open; §5 restates the resolved timeout stance.)
  - **`CancellationToken` is user-initiated cancellation only — NOT a timeout/deadline.** It
    maps to `wakeup()` (best-effort); a pre-canceled token → `OperationCanceledException`
    synchronously (the shipped `SubmitOperation` precedent). Do not conflate with the
    (deferred) `Duration` timeout.
  - **Remove these from the "not-yet-wired" remark** in `IAsyncConsumer.cs` (the
    "committed, beginningOffsets/endOffsets/offsetsForTimes … arrive in later phases" list) —
    `Committed` + the three offset queries are now wired; leave `partitionsFor`/`listTopics`
    (E2) and pattern-subscribe in the remaining list.

---

## 3 · DECISION 3 — input marshalling (two shapes: TP-collection + TP→timestamp map)

**Decision: reuse the M5/P3 `WithPinnedTopics` helper verbatim for `Committed` /
`BeginningOffsets` / `EndOffsets`; add one parallel variant `WithPinnedTopicsAndTimestamps`
for `OffsetsForTimes` (adds a blittable `long[] timestamps`).**

  - **`Committed` / `BeginningOffsets` / `EndOffsets`** take a `TopicPartition` collection →
    the ABI's parallel `(topics[], partitions[], count)`. This is **exactly** the M5/P3
    partition-ops input. **Reuse `SubmitPartitionOp`'s precondition + snapshot logic and the
    `WithPinnedTopics` pinning helper unchanged** — call-scoped topic pins (§A3/§A4: the FFI
    reads the arrays synchronously into an owned Rust `Vec` before spawning; verified for the
    partition-ops precedent, same read-arrays-then-dispatch shape), blittable `int[]`
    partitions, no per-element copy beyond the UTF-8 encode.
  - **`OffsetsForTimes`** takes a `Map<TP,long>` → the ABI's parallel `(topics[],
    partitions[], timestamps[], count)`. Add a private variant that also fills a blittable
    `long[] timestamps` alongside the topic/partition arrays. The map is iterated once into
    parallel snapshot arrays (order irrelevant — it is a map). Precondition: null map →
    `ArgumentNullException`; a key with null topic → `ArgumentException`; negative partition →
    `ArgumentOutOfRangeException` (the shipped `SubmitPartitionOp` precedent). A **negative
    timestamp** is a Java-valid sentinel (`EARLIEST`/`LATEST` special timestamps are negative
    in Kafka's `ListOffsets`), so **do NOT reject negative timestamps** — pass them through.
    (Contrast the *partition* which the ABI mismaps if negative — the Actor must verify the FFI
    treats the timestamp as an opaque `i64` and document it.)
  - **Empty input** is valid for all four (Java returns an empty map for an empty request; the
    mock loops over an empty slice → empty map). Pass `count == 0` through; never throw on
    empty (the M5/P3 empty-collection precedent). A **null** collection/map is still rejected
    (null ≠ empty).
  - **Factor the shared precondition+snapshot** out of `SubmitPartitionOp` into a small helper
    reused by the three collection-input queries, so the null/null-topic/negative-partition
    validation is not copy-pasted four more times. (The Actor decides the exact factoring;
    Critic checks no divergence from the shipped validation.)

---

## 4 · DECISION 4 — completion bridge: three owned-handle trampolines + per-container submit helpers, plus three container copy-out marshallers

**Decision: clone the proven owned-handle path (`SubmitOperation<T>` + `OnPoll` +
`ConsumerRecordsMarshal.CopyOut`) once per container type, leaving the shipped poll / void /
scalar paths byte-for-byte untouched (the M5/P2 discipline).** Concretely:

### 4.1 Trampolines (in `ConsumerCallbacks.cs`) — clones of `OnPoll`

Three new rooted delegates + trampolines, each an `OnPoll` clone differing only in (a) the
`OperationCompletionSource<TResult>` result type, (b) the copy-out marshaller called on
success, and (c) which container `_destroy` runs in the `finally`:

  - `Committed` → `OnCommitted` : `OperationCompletionSource<IReadOnlyDictionary<TopicPartition,OffsetAndMetadata>>`, `OffsetMapMarshal.CopyOut`, `OffsetMapDestroy`.
  - `OffsetsForTimes` → `OnOffsetsForTimes` : `…<IReadOnlyDictionary<TopicPartition,OffsetAndTimestamp>>`, `OffsetAndTimestampMapMarshal.CopyOut`, `OffsetAndTimestampMapDestroy`.
  - `LongOffsets` → `OnLongOffsets` : `…<IReadOnlyDictionary<TopicPartition,long>>`, `LongOffsetMapMarshal.CopyOut`, `LongOffsetMapDestroy`. **One trampoline** serves BOTH `BeginningOffsets` and `EndOffsets` (they share `long_offsets_callback_t`).

Each preserves the **`OnPoll` correctness invariants verbatim**: no-throw boundary
(foreign dispatcher thread), copy-out on the dispatcher thread **before** `_destroy`,
container `_destroy` null-safe in `finally` (no-op on the failure/null path), error via
`OperationCompletionSource<T>.Complete(error)` (frees the error handle), per-op `GCHandle`
freed exactly once via `FreeGcHandle`, `RunContinuationsAsynchronously` (via the existing
`OperationCompletionSource<T>`). **So three new delegate signatures** (each
`(IntPtr container, IntPtr error, IntPtr userData)` — same shape as `PollCallback`, so they
could even reuse one delegate type; the Actor may define one shared
`OwnedHandleCallback` delegate if the marshaller/destroy differ only by the trampoline body —
Critic confirms no behavioral divergence).

### 4.2 Submit helpers (in `NativeConsumer.cs`) — clones of `SubmitOperation<T>`

`SubmitOperation<TResult>` hardcodes `ConsumerCallbacks.Poll`. Following the **M5/P2
precedent** (which cloned `SubmitOperation` → `SubmitScalarOperation` rather than
generalizing, "so the proven poll / void submit paths are left byte-for-byte untouched"),
add per-callback submit helpers that pass the correct rooted trampoline. **Preferred
refinement (Actor's call, Critic-checked):** generalize `SubmitOperation<TResult>` to take
the **rooted callback delegate as a parameter** (all three new ones + poll share the identical
body — root context via `GCHandle`, `RegisterCancellation`, `submit(handle, callback, ud)`,
`AbandonBeforeSubmit` on throw). This is safe here (unlike M5/P2's scalar, which had a
*different delegate type* `PositionCallback`) because **all owned-handle callbacks share the
`(IntPtr, IntPtr, IntPtr)` shape** — one parameterization covers poll + all three E1
trampolines with zero behavioral change to the poll path. If the Actor prefers exact clones
(zero touch to `SubmitOperation`), that is also acceptable; the plan does not mandate the
refactor, only that the poll path's *behavior* is unchanged. **Critic check:** whichever form,
the shipped poll/void/scalar submit behavior is byte-for-byte preserved.

### 4.3 Container copy-out marshallers (in `Internal/Interop/`) — clones of `ConsumerRecordsMarshal`

Three new `static class *Marshal` with a `CopyOut(IntPtr container) → managed dictionary`,
each following `ConsumerRecordsMarshal.CopyOut` (count → loop → per-entry copy-out → owned
managed container, **no borrowed pointer retained**):

  - `OffsetMapMarshal.CopyOut(IntPtr) → Dictionary<TopicPartition, OffsetAndMetadata>`: for
    each `i`: key via `OffsetMap_get_key(i)` → marshal `TopicPartition_t` (topic NUL-scan copy
    + partition) into a managed `TopicPartition`; value via `OffsetMap_get_value(i)` → read
    `_offset` / `_metadata` (NUL-scan copy) / `_leader_epoch` (presence flag → `int?`) into an
    owned `OffsetAndMetadata`. **The key + value handles are borrowed elements of the map
    (§B2 Category 4) — never `_destroy` them;** only the map root is destroyed (by the
    trampoline, after CopyOut).
  - `OffsetAndTimestampMapMarshal.CopyOut(IntPtr) → Dictionary<TopicPartition, OffsetAndTimestamp>`:
    identical shape; value reads `_offset` / `_timestamp` / `_leader_epoch`.
  - `LongOffsetMapMarshal.CopyOut(IntPtr) → Dictionary<TopicPartition, long>`: value is a bare
    `int64_t` (`LongOffsetMap_get_value` returns by value — no handle, nothing borrowed to copy
    out beyond the scalar).
  - Return `IReadOnlyDictionary<…>` (a `Dictionary<…>` upcasts). **Empty container** (count 0)
    → an empty dictionary (a valid, non-null success — the `ConsumerRecordsMarshal` count≤0
    precedent).
  - **Strings use the NUL-terminated form** (`Utf8Marshal.PtrToString(ptr)`) — the map keys'
    topic and `OffsetAndMetadata.metadata` are handle-owned NUL-terminated strings (NOT the
    length-delimited fetch-batch slices that `ConsumerRecordsMarshal` uses). Copy before the
    root `_destroy`.

**Anti-patterns the Critic must flag (§B2/§B4):** freeing a borrowed key/value element;
destroying the map root before CopyOut returns; retaining any borrowed pointer past CopyOut;
NUL-scanning a length-delimited slice (N/A here — these are NUL-terminated) or vice-versa.

---

## 5 · DECISION 5 — error / precondition / timeout mapping (ffi §B5)

**Operational (core) failures** → faulted `Task<...>` with `KafkaException` (via the
owned-handle trampoline's error path → `OperationCompletionSource<T>.Complete(error)` →
`FromHandle`, asserting message). Concurrent async op → faulted `Task`
(`ConcurrentModification`, delivered by the core inline). Post-dispose →
`ObjectDisposedException` (`ThrowIfClosed` inside `SubmitOperation`, already there).

**Cancellation** → `wakeup()` best-effort; a **pre-canceled token** →
`OperationCanceledException` synchronously (the shipped `SubmitOperation`
`ThrowIfCancellationRequested`). **`CancellationToken` is user cancellation, not a
deadline** (restated — do not re-open).

**Timeout / `Duration`:** the async ABI exposes **no** timeout parameter for any of the four
(verified — the `_async` decls take no `int64_t timeout_ms`). So per the resolved
`Position`/`Close` precedent: **one method each, NO `TimeSpan` overload.** The timed Java
`(…, Duration)` overloads are **deferred** until the ABI exposes a timed async form. State
this in each member's rustdoc.

**Preconditions — validated BEFORE any pin / P-Invoke (mandatory, ffi §B5; the ABI
panics/mismaps on violation):**

  - null collection/map → `ArgumentNullException`.
  - a `TopicPartition` with a null `Topic` (a collection element, or a map key) →
    `ArgumentException` (the param name).
  - a negative `Partition` → `ArgumentOutOfRangeException` (the shipped `SubmitPartitionOp`
    precedent — the ABI silently maps negative to "unset").
  - **NOT** a precondition: a negative *timestamp* in `OffsetsForTimes` (Java-valid sentinel;
    §3) — pass through.

---

## 6 · Broker-free mock reachability (verified against `src/consumer/mock_consumer.rs` + `src/ffi/consumer.rs`) — READ THIS, it drives the tests

Per the D-Q4 / M5 precedent, each of the four is classified as **reachable broker-free (with
data)**, **reachable but empty-only**, or **NOT reachable** (defer with a documented
reachable slice).

| Member | Mock backing (`mock_consumer.rs`) | Reachable broker-free? |
|---|---|---|
| `Committed` | reads `self.committed`, populated **only** by `commit_sync_offsets` / `commit_async_offsets` (`self.committed.extend(offsets)`) | **Empty-only today.** The mock returns whatever has been committed; but **no .NET path populates it** — the **commit-with-offsets family is NOT yet wired** in the binding (no `CommitSync(offsets)` on `IAsyncConsumer`; verified — the interface has neither `Commit` nor `CommitSync` yet). So a .NET test can only observe `Committed` returning an **empty** map (for unassigned/uncommitted TPs) — it cannot set up a non-empty result without first wiring `commit_sync_offsets`. **See DECISION 6 / Q2.** |
| `OffsetsForTimes` | **`Err(unsupported_version)`** — the mock throws Java's `UnsupportedOperationException("Not implemented yet.")`; the Rust mock returns `KafkaError::unsupported_version` unconditionally | **NOT reachable with data — always errors on the mock.** Any `OffsetsForTimes` call on `AsyncMockConsumer` faults the `Task`. **See DECISION 6 / Q3.** |
| `BeginningOffsets` | reads `self.beginning_offsets`, populated by `MockConsumer_update_beginning_offsets` (**shipped M5/P3**, and the `UpdateBeginningOffset` .NET forwarder **already exists**) | **Fully reachable with data.** Set an offset via the shipped `UpdateBeginningOffset`, then `BeginningOffsets` returns it. Errors with `illegal_state` for a TP with no offset set. |
| `EndOffsets` | reads `self.end_offsets`, populated by `MockConsumer_update_end_offsets` (**shipped M5/P3**, `UpdateEndOffset` forwarder exists) | **Fully reachable with data.** Symmetric to `BeginningOffsets`. |

### DECISION 6 — how to handle the two partially/un-reachable members

**`OffsetsForTimes` (NOT reachable — mock always errors):**
**Recommendation: still wire the full .NET member** (interface + forwarders + bridge +
marshaller + input), because the machinery is proven and identical to the others, and the
member is Java-public — but **its only broker-free test is the faulted-`Task` path**: assert
that on `AsyncMockConsumer`, `OffsetsForTimes(...)` faults with a `KafkaException` whose
`Code`/`Message` match the mock's `unsupported_version("MockConsumer::offsets_for_times is
not implemented")`. Document the reachable slice ("the mock does not implement
offsetsForTimes; the success/copy-out path is exercised by the other two offset-map
marshallers of identical shape and by the unit test of `OffsetAndTimestampMapMarshal` if the
Actor adds a direct marshaller test"). **Do NOT silently skip the member** (D-Q4). This
matches how the mock itself deviates (it faithfully mirrors Java's `MockConsumer`, which also
throws).

**`Committed` (reachable but empty-only without the commit family):**
Two options — **the main §10 open question (Q2):**

  - **Option A (recommended) — wire `Committed` now; test the empty + faulted paths; defer the
    non-empty data test to when the commit family lands.** `Committed` on an unassigned/
    uncommitted set returns an empty map (broker-free, verified: the mock loops the requested
    TPs, includes only those in `self.committed`). Test: (a) `Committed(emptyset)` → empty map;
    (b) `Committed({tp})` with nothing committed → empty map (the mock omits absent TPs);
    (c) preconditions/dispose/cancel. The **non-empty** copy-out (a real `OffsetAndMetadata`
    value) is exercised by a **direct `OffsetMapMarshal.CopyOut` unit test** (the Actor can
    build a mock-backed non-empty map only once commit-with-offsets exists — so instead
    unit-test the marshaller against a container the mock CAN produce, or defer the non-empty
    assertion). Record the reachable-slice limit in COMMENTS.DONE.
  - **Option B — also wire `CommitSync(offsets)` this phase** (it is Mode A —
    `commit_sync_offsets` ships, §0) so `Committed` has a broker-free way to observe a non-empty
    result end-to-end (commit offsets → read them back). This **grows the phase** by the commit
    family's surface (a new `IAsyncConsumer.CommitSync(IReadOnlyDictionary<TP,OffsetAndMetadata>)`
    + its input marshalling with the `(offsets, leader_epochs, metadata)` extra arrays) — which
    is arguably its **own** phase (Category D, the commit family). **Recommendation: Option A**
    — keep E1 to the offset-map *queries*; the commit family is a separate coherent phase, and
    bundling half of it here to test `Committed` inverts the split rationale. `OffsetAndMetadata`
    (the value type E1 adds) will be reused by that later commit phase — good, it is landed
    early and independently reviewed.

**Net reachability outcome:** `BeginningOffsets` / `EndOffsets` fully tested with data;
`Committed` tested empty + faulted + marshaller-unit + preconditions (non-empty deferred to
the commit phase, Option A); `OffsetsForTimes` tested faulted + preconditions + marshaller
shape (mock unimplemented, documented).

---

## 7 · Tests (DoD §3 + CLAUDE.md §7.4; broker-free via `AsyncMockConsumer`)

All public-surface tests at the **test ROOT** (`PublicConsumer…Tests.cs`), broker-free via
`AsyncMockConsumer`. Serial execution (`[Collection]` no-parallel) stays. Each awaited op
uses a timeout (the completion / deadlock regression guard).

New file: `PublicConsumerOffsetQueryTests.cs` (root). Plus a direct marshaller unit test
file under `Interop/` if the Actor unit-tests the copy-out (recommended for the non-empty
`OffsetAndMetadata`/`OffsetAndTimestamp` paths the mock can't drive end-to-end).

Cases:

  1. **`BeginningOffsets` returns set offsets** — `UpdateBeginningOffset(t,0,5)` /
     `(t,1,7)`; `await BeginningOffsets({tp0,tp1})` → `{tp0:5, tp1:7}`. (Full data path — the
     primary success + copy-out coverage.)
  2. **`EndOffsets` returns set offsets** — symmetric, via `UpdateEndOffset`.
  3. **`BeginningOffsets` for a TP with no offset set → faulted `Task`** with the mock's
     `illegal_state("The partition <tp> does not have a beginning offset.")` — **assert the
     message content** (DoD §3), incl. the TP in the message.
  4. **`Committed` empty paths** — `Committed(empty)` → empty map; `Committed({tp})` with
     nothing committed → empty map (the mock omits absent TPs). (Option A, §6.)
  5. **`OffsetsForTimes` faults with `unsupported_version`** — `await`ing it on the mock
     faults with `KafkaException`; assert `Message` == the mock's not-implemented message and
     `Code` matches unsupported-version. (The documented reachable slice, §6.)
  6. **Copy-out marshaller unit tests (recommended)** — direct
     `OffsetMapMarshal.CopyOut` / `OffsetAndTimestampMapMarshal.CopyOut` /
     `LongOffsetMapMarshal.CopyOut` against a container the mock CAN produce (the `LongOffsetMap`
     from `BeginningOffsets`; for `OffsetMap`/`OffsetAndTimestampMap`, an `Interop`-level test if a
     container can be constructed) — asserting the borrowed elements are copied (values survive
     after the source is destroyed) and `LeaderEpoch` presence-flag → `int?` is honored (present
     vs absent). If a non-empty `OffsetMap` cannot be produced broker-free, note the deferral.
  7. **`LeaderEpoch` presence flag** — assert both an absent epoch → `null` and (where a
     container with a present epoch is reachable) a present epoch → the value. (Ties DECISION 1.)
  8. **Preconditions (deterministic, before any native call)** — null collection/map →
     `ArgumentNullException`; element/key with null topic → `ArgumentException`; negative
     partition → `ArgumentOutOfRangeException`; a **negative timestamp** in `OffsetsForTimes` is
     **accepted** (no throw — passes to the mock, which then faults unsupported_version). One per
     applicable member.
  9. **Post-dispose** — after `DisposeAsync`, each of the four throws `ObjectDisposedException`.
  10. **Cancellation / wakeup** — a pre-canceled token → `OperationCanceledException`
      synchronously; `Wakeup()` during an in-flight op faults/cancels once (mirrors the
      `Poll`/`Position` precedent).
  11. **Empty-input success** — `BeginningOffsets([])` / `EndOffsets([])` / `Committed([])` /
      `OffsetsForTimes({})` — the first three resolve to an empty map; `OffsetsForTimes({})`
      resolves to an empty map on Java, but the mock faults unsupported_version even for empty
      (the Actor verifies whether the FFI short-circuits empty before the mock call, and
      documents which — do NOT assume).
  12. **Allocation sanity** (DoD §10 / §7.4) — the copy-out adds only the owned managed entries
      (the `TopicPartition` keys, the value objects/`long`s, the dictionary) — no per-entry extra
      buffer, no borrowed pointer retained. Follow the `PublicConsumerAllocationBudgetTests`
      precedent.
  13. **TFM matrix smoke** — the four members marshal on net462 (via ns2.0) / net8.0 / net10.0
      (folded into the existing TFM smoke test).

---

## 8 · `NativeConsumer` / interop additions (summary)

  - **`NativeMethods` (4 new `_async` DllImports + 3 container `_destroy` + 3 container
    accessors × their arities + 2 value-type accessors × arity):**
    - `ConsumerCommittedAsync`, `ConsumerOffsetsForTimesAsync`, `ConsumerBeginningOffsetsAsync`,
      `ConsumerEndOffsetsAsync` — each `void`, with the full `EntryPoint`. The first/last two
      take `(IntPtr consumer, IntPtr[] topics, int[] partitions, int count, <trampoline>, IntPtr ud)`;
      `OffsetsForTimes` adds `long[] timestamps`.
    - `OffsetMapCount/GetKey/GetValue/Destroy`, `OffsetAndTimestampMapCount/GetKey/GetValue/Destroy`,
      `LongOffsetMapCount/GetKey/GetValue/Destroy` (GetValue → `long` for the last).
    - `OffsetAndMetadataOffset/Metadata/LeaderEpoch`, `OffsetAndTimestampOffset/Timestamp/LeaderEpoch`.
    - `TopicPartitionTopic` / `TopicPartitionPartition` if not already declared (check — the map
      keys need them; reuse the existing declaration if present).
    - `[MarshalAs(UnmanagedType.I1)] bool` return + `out int` on the two `_leader_epoch` accessors
      (the presence-flag pattern; §0.1 I1 rule).
  - **`ConsumerCallbacks.cs` (3 new rooted trampolines):** `OnCommitted` / `OnOffsetsForTimes` /
    `OnLongOffsets`, clones of `OnPoll` (§4.1). Possibly one shared `OwnedHandleCallback` delegate
    type.
  - **`Internal/Interop/` (3 new copy-out marshallers):** `OffsetMapMarshal`,
    `OffsetAndTimestampMapMarshal`, `LongOffsetMapMarshal` (§4.3), clones of
    `ConsumerRecordsMarshal`.
  - **`NativeConsumer` (4 new async methods + 1 input-marshalling variant + possibly a
    generalized/extra submit helper):** `CommittedWithCallback` / `OffsetsForTimesWithCallback` /
    `BeginningOffsetsWithCallback` / `EndOffsetsWithCallback`, each `SubmitOperation<TResult>`
    (poll analog) over its `_async` DllImport with the correct trampoline, via `WithPinnedTopics`
    (three) / `WithPinnedTopicsAndTimestamps` (one). Reuse the shipped `UpdateBeginningOffset`/
    `UpdateEndOffset` forwarders (no new mock helpers this phase — the offset-update helpers
    already ship).
  - **`IAsyncConsumer` (4 new members)** — §2. **`AsyncKafkaConsumer` + `AsyncMockConsumer`
    (4 new forwarders each).**
  - **New public value types:** `OffsetAndMetadata.cs`, `OffsetAndTimestamp.cs` (§1).
  - **Doc-sync:** update the `IAsyncConsumer.cs` "additive-growth surface" remark (drop
    `committed` / `beginningOffsets`/`endOffsets`/`offsetsForTimes` from the not-yet-wired list;
    keep `partitionsFor`/`listTopics`).

---

## 9 · E2 scope sketch (the deferred follow-on — NOT part of N=13; a separate later plan, N=14)

Recorded here only so the split is legible; **do not implement in this phase.**

  - **Members:** `Task<IReadOnlyList<PartitionInfo>> PartitionsFor(string topic, CT)` →
    `partitions_for_async` → `PartitionInfoList_t`; `Task<IReadOnlyDictionary<string,
    IReadOnlyList<PartitionInfo>>> ListTopics(CT)` → `list_topics_async` →
    `TopicPartitionInfoMap_t`.
  - **New public value types (nested):** `PartitionInfo { Topic, Partition, Node? Leader,
    IReadOnlyList<Node> Replicas, InSyncReplicas, OfflineReplicas }` and
    `Node { Id, Host, Port, Rack? }`.
  - **`Node` ABI finding (see §11):** the node type is **`kafka_common_Node_t`** (NOT
    `kafka_consumer_Node_*` — that is why the user's grep missed it). Accessors:
    `kafka_common_Node_id` (i32), `kafka_common_Node_host(node, out_len)` (length-delimited
    `(ptr,len)` — **borrows into the owning `PartitionInfo`**, use `out_len`, NOT a NUL-scan),
    `kafka_common_Node_port` (i32), `kafka_common_Node_rack(node, out_len)` (`(null, -1)` if no
    rack → `Rack` is `null`). `Node` has **no `_destroy`** — it is a **borrowed view** (§B2
    Category 4) dying with its owning `PartitionInfo`; never freed by the binding.
    `PartitionInfo`'s replica getters return borrowed `const kafka_common_Node_t*` (null on the
    leader if no leader / out-of-range index).
  - **Input:** `partitions_for` takes one topic string; `list_topics` takes nothing.
  - **Mock reachability:** both read `self.partitions`, populated by
    **`MockConsumer_update_partitions`** (shipped; single-partition-count + one leader
    `(id, host, port)` shape — verified, NOT a `PartitionInfo` list). Wire an
    `UpdatePartitions` mock forwarder that phase (as M5/P3 wired the offset helpers). Both
    reachable broker-free with data.
  - **Bridge:** two more owned-handle trampolines + two nested copy-out marshallers
    (`PartitionInfoList` → `PartitionInfo[]` with three `Node[]` replica lists each;
    `TopicPartitionInfoMap` → topic → `PartitionInfoList` per entry, reusing the
    `PartitionInfoList` marshaller). Reuses E1's owned-handle-container template.

---

## 10 · Resolved (user review, 2026-08-06)

All four decisions confirmed as recommended:

  1. **THE SPLIT** — **split**: E1 now as **M5/P4, N=13** (the four offset-map queries, this
     plan); E2 later as a separate plan (**N=14**: `PartitionsFor`/`ListTopics` +
     `PartitionInfo`/`Node`).
  2. **`Committed` reachability** — **Option A**: wire `Committed`, test empty + faulted +
     marshaller-unit; defer the non-empty end-to-end data test to the later commit-family phase
     (which reuses `OffsetAndMetadata`).
  3. **`OffsetsForTimes` on the mock** — **wire the full member**; its only broker-free test is
     the faulted-`Task` (`unsupported_version`) path, documenting the reachable slice (the mock
     mirrors Java's not-implemented `MockConsumer`).
  4. **Label/number** — **M5/P4**, slug `P4-consumer-offset-queries`, **N=13**.

**Awaiting:** final user approval to run the N=13 Actor → Critic loop.

---

## 11 · `Node` type finding (as requested — verified by reading the header)

The user's grep for `kafka_consumer_Node_*` surfaced nothing because **the node type lives in
the `common` namespace: `kafka_common_Node_t`** (Java `org.apache.kafka.common.Node` →
`kafka_common_Node`, per CLAUDE.md §3 FFI naming). Its accessors (verified in
`target/include/confluent_kafka.h`):

  - `int32_t kafka_common_Node_id(const kafka_common_Node_t*)`
  - `const char* kafka_common_Node_host(const kafka_common_Node_t*, int32_t *out_len)` —
    **length-delimited, borrows into the owning `PartitionInfo`** (§B3 receive-path form: use
    `out_len`, never NUL-scan).
  - `int32_t kafka_common_Node_port(const kafka_common_Node_t*)`
  - `const char* kafka_common_Node_rack(const kafka_common_Node_t*, int32_t *out_len)` —
    **`(null, -1)` if no rack** → the managed `Node.Rack` is `null`.
  - **No `kafka_common_Node_destroy`** — `Node` is a **borrowed view** (§B2 Category 4),
    obtained from a `PartitionInfo` getter and invalidated by `PartitionInfo_destroy`. The
    binding **never** frees it.

`PartitionInfo` (verified): `PartitionInfo_topic` (NUL-terminated), `_partition` (i32),
`_leader` (borrowed `const kafka_common_Node_t*`, **null if no leader**), `_replica_count` +
`_replica(i)`, `_in_sync_replica_count` + `_in_sync_replica(i)`, `_offline_replica_count` +
`_offline_replica(i)` (each `(i)` a borrowed `const kafka_common_Node_t*`, null if out of
range), `_destroy`. This all belongs to **E2** — recorded here only for completeness; E1 uses
none of it.

---

## 12 · Definition of Done (Actor must pass all)

1. `cargo build --features ffi` (prerequisite native build) succeeds; the four `_async`
   symbols + three container types + two value types + three callbacks present in the header
   (verified — §0).
2. `dotnet build` on the TFM matrix (net462 via ns2.0, net8.0, net10.0) clean; `dotnet format`
   + analyzers clean.
3. `dotnet test` green — all §7 cases pass.
4. **Owned-handle bridge cloned, not disturbed** (§4) — the shipped poll / void / scalar
   paths' behavior is byte-for-byte preserved; each new trampoline preserves every `OnPoll`
   invariant (no-throw, copy-out-before-destroy, container `_destroy` null-safe in `finally`,
   error via `Complete`, `GCHandle` freed once, `RunContinuationsAsynchronously`). A Critic
   finding otherwise.
5. **Receive-path copy-out / borrow-root discipline** (§B2/§B4, §4.3) — borrowed key/value
   elements never freed; the map root destroyed only after `CopyOut`; no borrowed pointer
   retained; NUL-terminated strings copied before `_destroy`.
6. **`LeaderEpoch` presence flag** honored (`bool` return → `int?`), not hardcoded (§1).
7. **Timeout stance** (§5) — one method each, NO `TimeSpan` overload; `CancellationToken` is
   cancellation, not a deadline.
8. **Preconditions before P/Invoke** for all four (§5); **negative timestamp accepted**,
   **empty input accepted**; **null rejected**.
9. **Reachability documented** (§6) — `Committed` empty-only (non-empty deferred, Option A),
   `OffsetsForTimes` faulted-only (mock unimplemented) — recorded in COMMENTS.DONE, member not
   silently skipped.
10. Error-message content asserted on the faulted paths (§7.3, §7.5); allocation sanity
    (§7.12); `ffi-marshalling.md` anti-patterns satisfied (§B2/§B3/§B4/§B5/§B6/§B7).

---

## 13 · Governance / mechanics

  - **Personas:** `dotnet-actor` / `dotnet-critic` (copied to repo-root `.claude/agents/` per
    the nested-discovery workaround; re-copy after any edit — CLAUDE.md §8.4). Manager is the
    root `project-manager`.
  - **Comments:** `bindings/dotnet/COMMENTS.13.md` (approved issues) / `COMMENTS.DONE.13.md`
    (resolved) — both local working files, never committed at the binding root. On phase close
    the Manager archives `COMMENTS.DONE.13.md` to
    `bindings/dotnet/design/history/M5/P4-consumer-offset-queries/` and resets `COMMENTS.13.md`.
  - **Review ground truth:** the C ABI header + the Kafka Java public-API shape — not Rust
    internals, not Java implementation logic (CLAUDE.md §8.2).
  - **Plan archive:** this PLAN at
    `bindings/dotnet/design/history/M5/P4-consumer-offset-queries/PLAN.md`. Living
    status/structure/design updates go in `bindings/dotnet/design/current/` on phase close.

---

## Appendix · Facts verified during planning (header + mock source)

  - Four `_async` fns present, all owned-handle callback shape `(container*, error*, ud)`;
    `beginning`/`end` **share** `long_offsets_callback_t` (one trampoline).
  - Containers `OffsetMap_t` / `OffsetAndTimestampMap_t` / `LongOffsetMap_t` each have
    `_count` / `_get_key`(→ borrowed `TopicPartition_t`) / `_get_value` (→ borrowed
    `OffsetAndMetadata_t` / `OffsetAndTimestamp_t` / by-value `int64_t`) / `_destroy`.
  - Value types `OffsetAndMetadata_t` (`_offset` / `_metadata` NUL-terminated / `_leader_epoch`
    presence-flag / `_destroy`) and `OffsetAndTimestamp_t` (`_offset` / `_timestamp` /
    `_leader_epoch` / `_destroy`) — both are **borrowed map elements**; the binding never
    `_destroy`s them (only the map root).
  - Input: `committed`/`beginning`/`end` = `(topics, partitions, count)` (the M5/P3
    `WithPinnedTopics` shape); `offsets_for_times` = `(topics, partitions, timestamps, count)`.
  - Mock (`src/consumer/mock_consumer.rs`): `committed` reads `self.committed` (populated only
    by `commit_sync_offsets`/`commit_async_offsets`; **no .NET commit-with-offsets path wired
    yet**); `offsets_for_times` returns `Err(unsupported_version(...))` unconditionally
    (mirrors Java's not-implemented `MockConsumer`); `beginning_offsets`/`end_offsets` read
    `self.beginning_offsets`/`self.end_offsets` (populated by the shipped
    `update_beginning_offsets`/`update_end_offsets` + the existing `UpdateBeginningOffset`/
    `UpdateEndOffset` .NET forwarders), `illegal_state` for an unset TP.
  - The **`Node` type is `kafka_common_Node_t`** (common namespace) with
    `id`/`host(out_len)`/`port`/`rack(out_len)` accessors and **no `_destroy`** (borrowed
    view) — E2 only.
  - Shipped bridge templates confirmed reusable: `SubmitOperation<T>` (owned-handle submit,
    hardcodes `ConsumerCallbacks.Poll`), `OnPoll` (owned-handle trampoline),
    `ConsumerRecordsMarshal.CopyOut` (container→managed copy-out), `WithPinnedTopics`
    (TP-collection input pinning), `OperationCompletionSource<T>.Complete`/`FreeGcHandle`/
    `RunContinuationsAsynchronously`.
