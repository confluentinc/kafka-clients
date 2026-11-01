# M5/P1 — Consumer sync read surface (`Assignment` / `Subscription` / `Paused` / `EnforceRebalance`)

**Status:** DRAFT — awaiting user approval. **Review number: N=10** (last completed: N=9, the M4/P4b async-surface rename).

**Milestone / phase label (RESOLVED — user review):** **Milestone 5 / Phase 1 —
"Consumer sync read surface"**, slug `P1-consumer-sync-read-surface`, at
`bindings/dotnet/design/history/M5/P1-consumer-sync-read-surface/`. The user opened **M5 as a
new milestone** for completing the consumer surface (Category A + H now; the later op
families follow). *Note:* earlier STATUS prose tentatively reserved "M5" for the future
**sync `IConsumer` facade** — that facade now moves to a later milestone; this milestone
grows the **async** consumer surface via `IConsumerCommon` / `IAsyncConsumer`.

**Mode:** **A (`.NET-only`, no Rust authored).** All four ABI functions and both list
types already ship in `target/include/confluent_kafka.h` (verified — see §0). `cargo build
--features ffi` remains a *prerequisite build step*, not a change.

---

## 0 · Scope & ABI ground truth (verified in the generated header)

This phase adds the **sync consumer state getters** (Category A) and
`enforce_rebalance` (Category H) — the four members CLAUDE.md §4 names in its
"Stays sync — exactly these" list that are still unshipped:

| Member | Java | ABI function (verified) | Result type / accessors |
|---|---|---|---|
| `Assignment` | `assignment()` → `Set<TopicPartition>` | `kafka_consumer_Consumer_assignment(consumer)` → `TopicPartitionList_t*` (owned) or **null** on concurrent-access rejection | `TopicPartitionList_count` / `_get` (borrowed `TopicPartition_t*`) / `_destroy`; element: `TopicPartition_topic` (NUL-term `const char*`), `_partition` (i32) |
| `Subscription` | `subscription()` → `Set<String>` | `kafka_consumer_Consumer_subscription(consumer)` → `StringList_t*` (owned) or **null** | `StringList_count` / `_get` (NUL-term `const char*`, handle-owned) / `_destroy` |
| `Paused` | `paused()` → `Set<TopicPartition>` | `kafka_consumer_Consumer_paused(consumer)` → `TopicPartitionList_t*` (owned) or **null** | same as `Assignment` |
| `EnforceRebalance` | `enforceRebalance()` / `enforceRebalance(String reason)` | `kafka_consumer_Consumer_enforce_rebalance(consumer, reason)` → `KafkaError_t*` (sync; **null = success**) | error out (return value) |

**Signatures (exact):**

```c
kafka_consumer_TopicPartitionList_t *kafka_consumer_Consumer_assignment(const kafka_consumer_Consumer_t *consumer);
kafka_consumer_StringList_t          *kafka_consumer_Consumer_subscription(const kafka_consumer_Consumer_t *consumer);
kafka_consumer_TopicPartitionList_t *kafka_consumer_Consumer_paused(const kafka_consumer_Consumer_t *consumer);
kafka_common_KafkaError_t           *kafka_consumer_Consumer_enforce_rebalance(const kafka_consumer_Consumer_t *consumer, const char *reason);

int32_t                                    kafka_consumer_TopicPartitionList_count(const kafka_consumer_TopicPartitionList_t *list);
const kafka_consumer_TopicPartition_t     *kafka_consumer_TopicPartitionList_get(const kafka_consumer_TopicPartitionList_t *list, int32_t index);
void                                       kafka_consumer_TopicPartitionList_destroy(kafka_consumer_TopicPartitionList_t *list);
const char                                *kafka_consumer_TopicPartition_topic(const kafka_consumer_TopicPartition_t *tp);
int32_t                                    kafka_consumer_TopicPartition_partition(const kafka_consumer_TopicPartition_t *tp);

int32_t      kafka_consumer_StringList_count(const kafka_consumer_StringList_t *list);
const char  *kafka_consumer_StringList_get(const kafka_consumer_StringList_t *list, int32_t index);
void         kafka_consumer_StringList_destroy(kafka_consumer_StringList_t *list);
```

**Not in scope** (unchanged public shape; each is an additive later phase): the commit
family, `position`, the owned-handle query siblings (`committed` / `offsetsForTimes` /
`beginning|endOffsets` / `partitionsFor` / `listTopics`), `subscribe(pattern)` / `assign`
(public) / `pause` / `resume`, `ConsumerRebalanceListener` / `OffsetCommitCallback`,
serializers + generic `IConsumer<TKey,TValue>`, typed `KafkaException` subclasses,
`Close(TimeSpan)`. **No Rust authored, no ABI change, no new op semantics.** The CLAUDE.md
§4 package-id pre-publish gate stays OPEN (held shut by `IsPackable=false`).

---

## 1 · RESOLVED: `enforceRebalance` behavior (the one behavioral unknown)

**Resolution: `enforceRebalance` is a logged no-op that returns success (no throw, no
error) under KIP-848.** The .NET `EnforceRebalance()` is therefore a **synchronous `void`
method that never surfaces a `KafkaException` for the KIP-848 no-op path.**

Source-of-truth chain (all read this phase):

1. **Java** `AsyncKafkaConsumer.java:1439-1446` — both overloads are pure logged no-ops:
   ```java
   public void enforceRebalance()             { log.warn("Operation not supported in new consumer group protocol"); }
   public void enforceRebalance(String reason){ log.warn("Operation not supported in new consumer group protocol"); }
   ```
   Returns `void`, **throws nothing**. (Javadoc says "classic-only"; the *implementation*
   is the source of truth per consumer-threading §20 — and the implementation does not
   throw.)
2. **Rust core** `src/consumer/async_kafka_consumer.rs:3873` — matches Java exactly:
   `log::warn!("Operation not supported in new consumer group protocol"); Ok(())`, with an
   explicit code comment: *"No `KafkaError::unsupported_version` since Java does not
   throw."*
3. **Rust FFI** `src/ffi/consumer.rs:3072` — wraps the core via `sync_void_op`, which
   returns a **null `KafkaError*` on `Ok(())`**. So the ABI returns **null (success)** under
   KIP-848 — it never hands back an error handle for this path.
4. **Mock** `src/consumer/mock_consumer.rs:964` — `should_rebalance = true; Ok(())` (also
   success, testable broker-free). Core test `enforce_rebalance_is_noop` asserts both
   overloads return `Ok`.

**⚠ Stale ABI doc — flag, do not act on.** The header/Rust-FFI doc comment says *"Under
KIP-848 this returns an unsupported-version error, matching Java."* That comment is
**wrong** — the code it calls returns `Ok(())` (null error), and Java does not throw. The
.NET mapping must follow the **actual behavior** (no-op success), not the stale comment.
This is a Rust-core *documentation* defect (a one-line doc fix on `enforce_rebalance` in
`src/ffi/consumer.rs` and its header regeneration); it is **out of scope** for the
`dotnet-actor` (Mode A, C#-only) and should be raised as a Rust-core doc-fix dependency to
the root `actor-executor` / `kafka-critic`. The .NET plan does **not** wait on it.

**.NET mapping (decided):**

- `void EnforceRebalance(string? reason = null)` — synchronous (CLAUDE.md §4 "stays sync"
  list). **One method with an optional `reason`**, not two overloads — Java's two overloads
  are `()` and `(String reason)`; a single `string? reason = null` collapses them
  idiomatically (both Java bodies are byte-identical, so no behavioral distinction is lost),
  and the ABI already accepts a null `reason`. This is the minimal faithful surface.
  **Confirmed against the Python sibling:** `def enforce_rebalance(self, reason=None)` is
  likewise one method with an optional `reason` (in `_ConsumerBase`, sync in both Python
  flavors; it defensively raises if the FFI returns an error, though the core no-ops).
- **Error handling:** it still calls `KafkaException.FromHandle(error)` and throws if the
  handle is non-null — **not** because the KIP-848 path errors (it returns null), but
  because that is the uniform sync-op error discipline (ffi §B5) and a future
  classic-protocol arm *could* return a real error here without a .NET change. Under the
  current KIP-848 core the returned handle is always null, so `EnforceRebalance` is
  observably a no-op that returns normally. The test asserts exactly that (no throw).
- **Placement:** on `IConsumerCommon` (see §2) — it is non-blocking, so it is shared by the
  async surface and the reserved sync surface.

---

## 2 · DECISION — surface shape & interface placement

**Reconcile the CLAUDE.md §3 sketch:** the sketch lists `Assignment` / `Subscription` as
sync *properties* on `IAsyncConsumer`, but M4/P4b already moved the other two non-blocking
members (`Wakeup()` / `GroupMetadata()`) **down onto `IConsumerCommon`** — the shared base
for "non-blocking in Java → stays sync regardless of the async/sync split." The four new
members are in that exact same category.

**Decision (RESOLVED — user review): all four go on `IConsumerCommon`; the three getters are
plain `()` METHODS (not properties); `EnforceRebalance` is a method.**

| Member | Kind | Interface | Return type |
|---|---|---|---|
| `Assignment()` | **method** | `IConsumerCommon` | `IReadOnlyCollection<TopicPartition>` |
| `Subscription()` | **method** | `IConsumerCommon` | `IReadOnlyCollection<string>` |
| `Paused()` | **method** | `IConsumerCommon` | `IReadOnlyCollection<TopicPartition>` |
| `EnforceRebalance(string? reason = null)` | **method** | `IConsumerCommon` | `void` |

**Rationale:**

- **`IConsumerCommon`, not `IAsyncConsumer`.** `IConsumerCommon` is *defined* (its own
  docstring, shipped M4/P4b) as "the members that are non-blocking in Java's consumer
  implementation and therefore stay synchronous regardless of the async/sync split … the
  common base of `IAsyncConsumer` and reserves the shape for a future sync `IConsumer`
  surface." These four are non-blocking sync members — they belong there by that
  definition, and the reserved future sync `IConsumer` (a later milestone) inherits them for
  free with identical signatures (no duplication), exactly the reason `Wakeup`/`GroupMetadata`
  were moved there. **Intentional deviation from the literal §3 sketch** (which still shows
  `Assignment`/`Subscription` on `IAsyncConsumer`); record it in `COMMENTS.DONE.10.md` and
  update the sketch in a doc-sync commit.

- **Methods, not properties, for the three getters (RESOLVED — reverses the §3 sketch's
  property form).** Three independent reasons converge on a method:
  1. **Shipped precedent / internal consistency.** The one non-blocking getter already on
     `IConsumerCommon`, `GroupMetadata()`, is a **method** — its docstring says "a method,
     matching Java's method-not-property shape." Making these three methods keeps
     `IConsumerCommon` uniformly method-shaped and avoids the sketch's internal
     inconsistency (properties for two getters, a method for the third).
  2. **Java + Python parity.** Java exposes `assignment()` / `subscription()` / `paused()`
     as methods; the Python sibling likewise (`def assignment(self)` / `def subscription`
     / `def paused` — plain methods in `_ConsumerBase`, no `@property`). A method keeps the
     name recognizable across bindings (`bindings/CLAUDE.md §2.2`).
  3. **.NET Framework Design Guidelines.** FDG says use a **method**, not a property, when
     the accessor does non-trivial work, can **throw**, or returns a **fresh
     collection/array each call**. All three hold: each does a P/Invoke + marshalling, can
     throw `InvalidOperationException` (concurrent access) / `ObjectDisposedException`
     (closed), and materializes a fresh owned snapshot each call. A property would imply a
     cheap, field-like, non-throwing read and misrepresent them. (CA1024 does **not** push
     back the other way — it excludes collection-returning / throwing / expensive getters.)

  This overrides the idiom-map's generic "non-blocking getter → property" row for these
  specific getters, on the FDG grounds above + the shipped `GroupMetadata()` precedent +
  Java/Python parity. **Documented deviation** — recorded in `COMMENTS.DONE.10.md`; the §3
  sketch is updated to methods in the doc-sync commit.

- **`EnforceRebalance` a method** (it is an action, not a getter) — per the idiom-map row
  "Non-blocking action, no completion signal → sync plain **method**."

- **Return type `IReadOnlyCollection<T>`** (not `IReadOnlySet<T>`): Java returns a `Set`,
  but `IReadOnlySet<T>` post-dates the netstandard2.0 floor (idiom map). Use
  `IReadOnlyCollection<TopicPartition>` / `IReadOnlyCollection<string>`. The core returns a
  set (no duplicates), so the concrete backing type will be a materialized snapshot (a
  `TopicPartition[]` / `string[]` or a `List<>`), copied out of the native list — an owned,
  immutable snapshot, matching Java's "returns a copy" contract (`paused` clones; assignment
  / subscription materialize).

---

## 3 · DECISION — marshalling (two new copy-out marshallers)

Both list results are **Category-3 owned borrow-roots** (ffi §B2): the caller frees the
list once with `_destroy`; elements borrowed via `_get` are **Category-4 borrowed views**
(never freed). Copy every element out **before** `_destroy` (the copy-out discipline, ffi
§B2 default; CLAUDE.md §6.4).

Two new internal marshallers under `Internal/Interop/`, mirroring the shipped
`ConsumerGroupMetadataMarshal.CopyOutAndDestroy` precedent (read all fields, then `_destroy`
in a `finally` even if a read throws — freed exactly once):

1. **`TopicPartitionListMarshal.CopyOutAndDestroy(IntPtr list)` →
   `IReadOnlyCollection<TopicPartition>`**
   - `int n = TopicPartitionList_count(list);`
   - for each `i`: `IntPtr tp = TopicPartitionList_get(list, i);` (borrowed, never freed),
     then `string topic = Utf8Marshal.PtrToString(TopicPartition_topic(tp))` +
     `int partition = TopicPartition_partition(tp)` → `new TopicPartition(topic, partition)`.
   - Materialize into a `TopicPartition[]` (owned snapshot); `finally
     TopicPartitionList_destroy(list)`.
2. **`StringListMarshal.CopyOutAndDestroy(IntPtr list)` → `IReadOnlyCollection<string>`**
   - `int n = StringList_count(list);` for each `i`: `Utf8Marshal.PtrToString(StringList_get(list, i))`.
   - Materialize into a `string[]`; `finally StringList_destroy(list)`.

**String-termination form (verified, decided):** **NUL-terminated, use `Utf8Marshal.PtrToString(ptr)`
(NUL-scan) — NOT the length-delimited receive-path form.** The header states
`StringList_get` returns "the string at `index` as a NUL-terminated C string (owned by the
handle)"; `TopicPartition_topic` is a NUL-terminated getter (the same form as
`ConsumerGroupMetadata_group_id`). These are **not** the length-delimited
`ConsumerRecord_topic` slices that borrow into a fetch batch (ffi §B3 length form). So both
marshallers use `Utf8Marshal.PtrToString(IntPtr)` (the existing NUL-scan overload) — the
`out_len` form (§B3) is **not** used here. (Copy-before-destroy still applies: the pointers
die with the list handle.)

**`SafeHandle` vs read-and-free (decided): read-and-free, no `SafeHandle`.** These are
**transient** owned results, fully consumed on the caller's thread within one synchronous
getter call (get → copy-out → `_destroy`, all before the method returns). Nothing native
escapes; no `Task`, no callback, no cross-thread lifetime. This is exactly the
`ConsumerGroupMetadataMarshal` pattern (a raw `IntPtr` + a `finally _destroy`), not the
long-lived `SafeConsumerHandle` (Category-1) pattern. A per-call `SafeHandle` would allocate
a finalizable object per read for no lifetime benefit (ffi §B2 anti-pattern: "a per-message
`SafeHandle` allocates a finalizable object … transient handles are read-and-freed"). So:
**no new `SafeHandle` subclass this phase.**

`unsafe`: none needed — `Utf8Marshal.PtrToString(IntPtr)` is safe managed API, matching how
`ConsumerGroupMetadataMarshal` avoids `unsafe`. The marshallers live in `Internal/Interop/`
by topical convention (they are P/Invoke-boundary helpers) but carry no `unsafe`.

---

## 4 · DECISION — concurrency / error mapping

**Sync state reads (`Assignment` / `Subscription` / `Paused`):** the core returns a **null
list handle on a concurrent-access rejection** (verified: all three header docs say "or null
on a concurrent-access rejection (the guard could not be acquired)"). Map **null →
`InvalidOperationException`** with the message `"KafkaConsumer is not safe for
multi-threaded access."` — **identical** to the shipped `GroupMetadata` /
`GetGroupMetadataHandleOrThrow` path (ffi §B5; CLAUDE.md §3 idiom-map row "concurrent sync
state read → `InvalidOperationException`"). **Never `KafkaException`** for these reads.

- Reuse the exact shipped shape: a private helper analogous to
  `GetGroupMetadataHandleOrThrow` (fetch handle; if `IntPtr.Zero` throw
  `InvalidOperationException`), then hand the non-null handle to the marshaller. Consider a
  single generic helper `ThrowIfConcurrentNull(IntPtr, string context)` shared by all three
  reads + the existing group-metadata path, or keep three call sites parallel to
  `GetGroupMetadataHandleOrThrow` — Actor's choice, but do not introduce a *new* concurrency
  contract; mirror the existing one.
- **Post-dispose → `ObjectDisposedException`:** each getter calls `ThrowIfClosed()` first
  (the shipped `NativeConsumer` gate), exactly like `GroupMetadata`/`GroupId`.
- **Accepted residual (unchanged):** the same check-then-use handle TOCTOU vs a concurrent
  teardown that `Wakeup`/`GroupId`/`GroupMetadata` already carry (accepted-by-design under
  the single-owner / not-thread-safe contract; N-hardening deferred). These new reads
  inherit it identically — no new residual, no new hardening this phase.

**`EnforceRebalance`:** `KafkaException.FromHandle(error)`; throw iff non-null (ffi §B5
uniform sync-op discipline). Under KIP-848 the handle is always null, so it never throws
(§1). `ThrowIfClosed()` first (post-dispose → `ObjectDisposedException`). `reason` is pinned
call-scoped via `Utf8Marshal.Pin` when non-null; `null` → `IntPtr.Zero` (the ABI accepts a
null `reason`).

**Preconditions:** none beyond `ThrowIfClosed()` — the getters take no args;
`EnforceRebalance`'s `reason` is optional and a null is legal at the ABI.

---

## 5 · DECISION — new P/Invoke declarations (`NativeMethods`)

Add to `NativeMethods` (each with `EntryPoint` = the full ABI symbol per ffi §0.1;
`IntPtr` for opaque handles; `int`/`long` per the type map; no `SafeHandle` params — these
take the raw consumer `IntPtr` like the existing `ConsumerGroupMetadata` read):

- `Consumer_assignment(IntPtr consumer) → IntPtr`
  (`EntryPoint = kafka_consumer_Consumer_assignment`)
- `Consumer_subscription(IntPtr consumer) → IntPtr`
  (`EntryPoint = kafka_consumer_Consumer_subscription`)
- `Consumer_paused(IntPtr consumer) → IntPtr`
  (`EntryPoint = kafka_consumer_Consumer_paused`)
- `Consumer_enforce_rebalance(IntPtr consumer, IntPtr reason) → IntPtr` (returns the
  `KafkaError*` out) (`EntryPoint = kafka_consumer_Consumer_enforce_rebalance`)
- `TopicPartitionList_count(IntPtr list) → int` / `TopicPartitionList_get(IntPtr list, int
  index) → IntPtr` / `TopicPartitionList_destroy(IntPtr list) → void`
- `TopicPartition_topic(IntPtr tp) → IntPtr` / `TopicPartition_partition(IntPtr tp) → int`
  (`TopicPartition_destroy` exists in the header but is **not** declared — list elements are
  borrowed, never freed; only the list root is destroyed)
- `StringList_count(IntPtr list) → int` / `StringList_get(IntPtr list, int index) → IntPtr`
  / `StringList_destroy(IntPtr list) → void`

No new callback delegates (these are all sync). No `[MarshalAs]` needed (no `bool`
returns).

---

## 6 · DECISION — `NativeConsumer` additions + forwarding

**`NativeConsumer` (internal) gains four members**, each mirroring the shipped `GroupMetadata`
sync-read shape (`ThrowIfClosed` → fetch handle → null-check for the reads → marshal →
`_destroy` in `finally`):

- `internal IReadOnlyCollection<TopicPartition> Assignment()` — fetch via
  `Consumer_assignment`, null → `InvalidOperationException`, else
  `TopicPartitionListMarshal.CopyOutAndDestroy`.
- `internal IReadOnlyCollection<string> Subscription()` — via `Consumer_subscription` +
  `StringListMarshal`.
- `internal IReadOnlyCollection<TopicPartition> Paused()` — via `Consumer_paused` +
  `TopicPartitionListMarshal`.
- `internal void EnforceRebalance(string? reason)` — `ThrowIfClosed`; pin `reason`
  call-scoped (or `IntPtr.Zero`); call `Consumer_enforce_rebalance`;
  `KafkaException.FromHandle` throw-if-non-null.

(These are methods on `NativeConsumer`; the client types forward to them — see below.)

**Forwarding — `AsyncKafkaConsumer` and `AsyncMockConsumer`** (§2.5 compose-and-forward,
matching the shipped `Wakeup`/`GroupMetadata` forwards):

```csharp
public IReadOnlyCollection<TopicPartition> Assignment()   => _native.Assignment();
public IReadOnlyCollection<string>         Subscription() => _native.Subscription();
public IReadOnlyCollection<TopicPartition> Paused()       => _native.Paused();
public void EnforceRebalance(string? reason = null)       => _native.EnforceRebalance(reason);
```

Both concrete types add the identical four forwards (they both `impl IAsyncConsumer` which
`: IConsumerCommon`). `AsyncMockConsumer`'s existing inherent `Assign(...)` (mock-only,
takes `IReadOnlyList<TopicPartition>`) is **unrelated** and untouched — do not conflate the
internal sync `Assign` (mock setup) with the new read-only `Assignment()` getter.

**Interface files touched:** `IConsumerCommon.cs` gains the four member declarations (with
full XML docs — CS1591 is enforced). `IAsyncConsumer.cs` is unchanged (it inherits them).

---

## 7 · Tests (DoD §3 + CLAUDE.md §7.4; broker-free via `AsyncMockConsumer`)

Serial execution (`[assembly: CollectionBehavior(DisableTestParallelization = true)]`, D8.8)
stays enabled — not re-enabled, not weakened. Every op/teardown under a `TestTimeout` hang
guard. New test file(s), e.g. `Interop/ConsumerSyncReadTests.cs` (+ marshaller round-trip
coverage), mirroring the existing consumer test layout.

**State-read correctness (mock, broker-free):**

1. **`Assignment()` reflects `Assign`** — construct `AsyncMockConsumer`, `Assign` two
   topic-partitions, assert `Assignment()` returns exactly those two (value equality via the
   `TopicPartition` struct); empty before any assign (mock `assignment()` reads
   `subscriptions.assigned_partitions()`).
2. **`Subscription()` reflects `Subscribe`** — `await Subscribe(["t1","t2"])`, assert
   `Subscription()` returns exactly `{t1,t2}`; empty before subscribe.
3. **`Paused()` reachable states only** — `Pause` is **NOT wired until a later phase**, so the
   *only* broker-free-reachable `Paused()` states are **empty**: assert `Paused()` is empty on a
   fresh consumer and empty after `Assign` (mock `paused()` starts empty and nothing can add
   to it yet). **Explicitly documented:** a non-empty `Paused()` is unreachable until `Pause`
   lands — tested states are empty/assigned-but-not-paused; the non-empty path is a
   later-phase test (record in `COMMENTS.DONE.10.md`, M3/P3 precedent for
   "reachable-slice-only").

**Marshalling (§B3 guard, DoD §3):**

4. **Non-ASCII round-trip through both list marshallers** — subscribe to a non-ASCII topic
   (e.g. `"café-topic"`) and assign a non-ASCII topic-partition, assert both survive
   `Subscription()` (via `StringListMarshal`) and `Assignment()` (via
   `TopicPartitionListMarshal` → `TopicPartition_topic`) byte-for-byte. Guards the
   NUL-terminated `PtrToString` path (catches an `LPStr` regression), the §0.1 non-ASCII
   requirement.

**Concurrency / lifecycle:**

5. **Concurrent access → `InvalidOperationException`** — the concurrent-null → IOE mapping.
   As with the shipped `GroupId`/`GroupMetadata` (D-Q4), a *forced* submit→read overlap is
   not deterministically reproducible broker-free (mock ops resolve instantly; the one
   guard-holding op with a controllable duration is `poll`, out of scope). So: assert the
   reachable seam (the read round-trips on a free guard) and verify the null →
   `InvalidOperationException` mapping **by code inspection**, documented in
   `COMMENTS.DONE.10.md` mirroring the D-Q4 precedent. **Honest cost noted:** same
   non-deterministic ceiling as the shipped state reads.
6. **Post-dispose → `ObjectDisposedException`** — after `Dispose`/`DisposeAsync`, each of
   `Assignment()` / `Subscription()` / `Paused()` / `EnforceRebalance()` throws
   `ObjectDisposedException` (the `ThrowIfClosed` gate). Deterministic.

**`EnforceRebalance` (per §1 resolution):**

7. **`EnforceRebalance` is a no-op that returns normally** — `EnforceRebalance()` and
   `EnforceRebalance("some reason")` both return without throwing (KIP-848 logged-no-op →
   null error). Assert **no exception**; assert the `reason`-overload and the no-arg form
   behave identically. (Do **not** assert an `unsupported-version` `KafkaException` — that
   would encode the stale ABI doc, not the real behavior.)

**Allocation sanity (DoD §10 spirit, §7.4):**

8. **Allocation sanity check on a state read** — a repeated `Assignment()` read on a small
   fixed assignment does not allocate unboundedly per call beyond the owned snapshot array +
   the `TopicPartition` elements it must materialize (the copy-out is *the* allocation, and
   it is bounded by assignment size — Java's own behavior). A light budget assertion in the
   style of the existing receive-path/allocation tests; not a hot path (these are low-freq
   sync reads), so a sanity bound, not a zero-alloc assertion.

**Explicitly flagged as not reachable broker-free with today's mock:** a **non-empty
`Paused()`** (needs `Pause`, later phase) and a **deterministic forced-concurrency overlap**
(needs a blockable mock op, a Rust-core dependency — the M3/P3 / D-Q4 precedent). Both are
recorded, not silently skipped.

---

## 8 · Definition of Done (Actor must pass all)

1. `cargo build --features ffi` — native cdylib + header present (run FIRST, §7.1). **No
   ABI change (Mode A);** the stale `enforce_rebalance` doc is a *separate Rust-core doc-fix
   dependency*, not part of this .NET build.
2. `dotnet build` — 0 warnings / 0 errors across all library TFMs (netstandard2.0, net8.0,
   net10.0) and all test TFMs (net462, net8.0, net10.0);
   `TreatWarningsAsErrors` + `EnforceCodeStyleInBuild` + `GenerateDocumentationFile` active.
   **CS1591 satisfied on every new public member** (the four `IConsumerCommon` members + the
   four forwards on each client type). Apache-2.0 header on every new file; no TODO/FIXME.
3. `dotnet test` — all pass (previous count 122 + the new sync-read tests); the D8.8
   stability gate (multiple full-suite runs green) holds with parallelism disabled.
4. `dotnet format --verify-no-changes` — clean.
5. DoD §3 parity: error/behavior asserted (the IOE message, the `EnforceRebalance` no-throw,
   the non-ASCII round-trip content), not just `is_err()`. Not-reachable Java behavior
   explained (non-empty `Paused()`, forced concurrency).
6. Consumer-trait-surface check (DoD §11): the new members are **sync plain methods** on
   `IConsumerCommon` — no `Task`, no `block_on` façade, no enum dispatch; the two
   marshallers are sync.

---

## 9 · Governance / mechanics

- **N=10** this phase. `dotnet-actor` / `dotnet-critic` personas (copied to repo-root
  `.claude/agents/` per the discovery workaround). Comments: `bindings/dotnet/COMMENTS.10.md`
  (working) → `COMMENTS.DONE.10.md` (resolved, **not** committed at the binding root; the
  Manager archives a copy under this phase directory at handoff).
- **Deviations to record in `COMMENTS.DONE.10.md`:** (a) the four members placed on
  `IConsumerCommon` rather than the literal §3-sketch `IAsyncConsumer` placement; (b) the
  three getters exposed as **methods, not properties** (reverses the §3 sketch), on the FDG
  "throws / does work / fresh-collection-per-call → method" grounds + the shipped
  `GroupMetadata()` precedent + Java/Python parity; (c) `EnforceRebalance` as one method with
  `string? reason = null` (collapsing Java's two overloads); (d) the stale ABI
  `enforce_rebalance` doc raised as a Rust-core doc-fix dependency; (e) the D-Q4-style
  non-deterministic-concurrency + non-empty-`Paused` reachability limits.
- **Doc-sync commit:** update the CLAUDE.md §3 sketch (move `Assignment`/`Subscription`/`Paused`
  onto `IConsumerCommon` **as methods** — `Assignment()` etc., not `{ get; }` properties —
  and add `EnforceRebalance`) and `STATUS.md` (new **M5/P1** phase entry, N=10) so docs match
  code — the M4/P4b pattern (a final small doc-sync commit).
- **Branching:** commits land on the **new branch `prashah_dev_public_consumer_remaining`**
  (created by the user for the post-M4 consumer work), branched off the M4
  `prashah_dev_public_consumer_scaffolding` line. Treated as a **new PR** for M5/P1.

---

## Resolved (user review, 2026-08-06)

All open questions are answered — the plan above already reflects them:

1. **Milestone/phase label → M5/P1 (new milestone).** The user opened **M5** as a new
   milestone; this is its Phase 1. (The earlier tentative "M5 = sync `IConsumer` facade"
   reservation moves to a later milestone.)
2. **Interface placement → all four on `IConsumerCommon`** (this plan's recommendation,
   consistent with the M4/P4b `Wakeup`/`GroupMetadata` move).
3. **Getter shape → methods, not properties** (`Assignment()` / `Subscription()` /
   `Paused()`), confirmed against Python (plain methods) + the shipped `GroupMetadata()`
   method precedent + FDG. Reverses the §3 sketch's property form (recorded as a deviation).
4. **`EnforceRebalance` → one method `EnforceRebalance(string? reason = null)`**, confirmed
   against Python's `enforce_rebalance(self, reason=None)`.
5. **Stale ABI `enforce_rebalance` doc → filed as a separate Rust-core doc-fix dependency**
   (default; keeps this phase pure Mode-A C#). *Flag if you'd rather bundle it.*
6. **Branch → `prashah_dev_public_consumer_remaining`** (new branch, created by the user),
   as a new PR.

**Awaiting:** final user approval of this plan before the Actor (N=10) → Critic (N=10) loop
runs.
