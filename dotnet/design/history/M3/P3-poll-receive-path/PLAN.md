# M3/P3 — Poll + the receive path (owned-handle completion bridge)

**Status:** FINALIZED — decisions recorded (see Confirmed decisions); awaiting
final human approval before implementation. Do NOT spawn Actor/Critic or write
code until approved.
**Mode:** A (consumer C ABI already landed — `poll_async`, `poll` and the whole
`ConsumerRecords_t` / `ConsumerRecord_t` accessor set already exist in
`confluent_kafka.h`; no `src/ffi` / Rust / header change this phase — add
`[DllImport]` decls + read the generated header).
**Review counter:** N=7 (global monotonic; M0/P0=1, M1/P1=2, M2/P1=3, M2/P2=4,
M3/P1=5, M3/P2=6, **M3/P3=7**).
**Personas:** `dotnet-actor` (Actor N=7) implements; `dotnet-critic` (Critic N=7)
reviews. NEVER the Rust `actor-executor` / `kafka-critic`.
**Branch / PR:** land on **`prashah_dev_asyncbridge_poll_scaffolding`** (current
HEAD) and open a **NEW PR stacked on top of PR #135** — do NOT extend #135 in
place. Additive commits. This branch descends from
`prashah_dev_asyncbridge_scaffolding` (PR #135's head), which already contains
M3/P1 (the void completion bridge) + M3/P2 (single-owner alignment). See
Governance for the exact relationship.
**Builds on:** M3/P1 (the **void** completion bridge:
`OperationCompletionSource`, `ConsumerCallbacks`, the `op_callback_t` trampoline,
GCHandle keep-alive submit→fire, no-throw boundary, free-exactly-once) + M3/P2
(single-owner alignment: no managed access guard, no in-flight tracking — the
Rust core is the serializer). The public un-sealed `KafkaException` remains the
only public type from prior phases.

---

## Why this phase exists

M3/P1 proved the **void-result** completion shape (`op_callback_t =
(error*, ud) → Task`) end-to-end with `subscribe`/`seek` on a `MockConsumer`.
M3/P3 proves the **result-returning** completion shape — the **owned-handle
completion bridge** — end-to-end via `poll`. `poll_async` uses
`poll_callback_t = (ConsumerRecords_t*, error*, ud)`: an **owned result handle**
(Category 3, a borrow-root) plus the error slot. This is the shape that **five
more consumer APIs reuse later** (`committed` / `offsetsForTimes` /
`beginning|endOffsets` / `partitionsFor` / `listTopics` — each an owned result
handle per ffi §B6). Proving it once, on `poll`, de-risks all of them, exactly
as M3/P1 de-risked the void shape before poll.

It also un-blocks the deferred **deterministic** wakeup / cancel / concurrency
tests: `poll` is the **only** op that observes `wakeup()` broker-free (M3/P1 D1
established that a `MockConsumer` checks the wakeup flag *only* inside `poll()`;
`subscribe`/`seek`/`acquire()` never do). With `poll` landed, the M3/P1 D1 and
M3/P2 D-Q4 deferrals become testable for the first time.

---

## The key design decision (baked in — not re-litigated)

**Marshalling (the batch copy-out) happens ON the dispatcher thread, inside the
callback** — not deferred to the awaiter's thread.

### Rationale (recorded here as the phase's load-bearing decision)

- For the consumer the "don't do heavy work on the dispatcher thread" concern
  barely applies:
  - the copy-out is **bounded framework work** (topic string + key/value arrays
    + headers per record) — the user's own code runs on the pool afterward via
    `RunContinuationsAsynchronously`, not on the dispatcher;
  - the dispatcher is **per-consumer** (ffi §B1) — it serves only this
    consumer's completions;
  - the consumer is **one-op-in-flight** (M3/P2 single-owner; core-serialized) —
    **nothing is queued behind the copy**, and the app is blocked awaiting the
    result regardless. So the cost is *whose CPU does the copy*, not a stall of
    other work.

- **Decisive safety win.** With on-dispatcher copy-out, the native batch
  (`ConsumerRecords_t`, a Category-3 **borrow-root**) is **created, copied-out,
  and `_destroy`ed entirely inside the callback** — nothing native-backed ever
  escapes to managed code. So there is:
  - **no `SafeConsumerRecordsHandle`** (no per-poll finalizable handle),
  - **no native-backed `ReadOnlyMemory<byte>`** stored on a `ConsumerRecord`,
  - **no leak-on-abandoned-`Task`** (an awaiter that drops the `Task` cannot
    leak the batch — the callback already freed it).

  This sidesteps the entire ffi §B4 / §B2-Category-3/4 lifetime hazard and is the
  **CLAUDE.md §6.4 copy-out default**, applied to the async path.

### The deferred alternative (record, do NOT build)

**Off-dispatcher copy-out / keep-alive zero-copy.** Would require a finalizable
`SafeConsumerRecordsHandle` (a per-poll finalizer) so the batch survives the
thread hop to the awaiter safely; it adds an allocation + a leak/UAF surface, and
is only cheap in Python because of its refcounted `memoryview` (CPython
refcount-driven `tp_dealloc`) — **.NET has no equivalent** (ffi §B4, CLAUDE.md
§6.4). Deferred: revisit **only** if profiling shows the dispatcher-thread copy
is a real bottleneck, and even then prefer the §6.4 **compile-time-safe** escape
hatch (a `ReadOnlySpan<byte>` accessor or a process-in-place `Poll(record => …)`
callback — a ref-struct `Span` can't be stored / awaited / sent cross-thread, so
it can't outlive the batch) over a stored native-backed `ReadOnlyMemory<byte>`.

### Net consequence for the design

- `ConsumerRecords_t` is a **transient owned handle consumed + destroyed inside
  the callback** — read-and-free, like the `KafkaError` handle. It is **NOT**
  wrapped in a `SafeHandle`. (Contrast: ffi §B2 lists it under Category 3 with a
  *deferred* keep-alive `SafeConsumerRecordsHandle` option — this phase takes the
  copy-out default, so no such SafeHandle is introduced.)
- `ConsumerRecord_t` + the key / value / topic / header slices are **Category-4
  borrowed views**, read **only during the copy-out**, never freed, never allowed
  to escape the callback.
- The only thing that stays off the dispatcher is the **user's continuation** —
  already handled by `RunContinuationsAsynchronously` (invariant #5, kept).

### Python-sibling divergence (cite)

`bindings/python/_confluentkafka.c` `consumer_poll_trampoline` (line ~1388)
hands the **raw `records` handle pointer** straight back to Python, which wraps
it in a keep-alive `ConsumerRecords` object exposing per-field
`memoryview(_BorrowedBytes(...))` — an **off-dispatcher, keep-alive, zero-copy**
model. .NET **diverges deliberately**: on-dispatcher copy-out, because .NET lacks
the refcounted `memoryview` that makes Python's keep-alive safe. confluent-
kafka-dotnet's `Consume()` likewise materializes owned managed `Message<K,V>`
values, not native-backed views — consistent with the copy-out choice.

---

## Source-verified findings (ground truth for the Actor)

Verified against `src/ffi/consumer.rs` and
`target/include/confluent_kafka.h` (the generated header — read it, do not trust
this list blindly; it is a pointer, not a substitute):

1. **`poll_async` signature** (header ~L623, `src/ffi/consumer.rs:604`):
   `void kafka_consumer_Consumer_poll_async(const Consumer_t* consumer,
   int64_t timeout_ms, poll_callback_t callback, void* user_data)`. It is the
   **only** `_async` fn taking a timeout.
2. **`poll_callback_t`** (header ~L48, `src/ffi/consumer.rs:590`):
   `void (*)(kafka_consumer_ConsumerRecords_t*, kafka_common_KafkaError_t*,
   void*)`. On **success** `records` is non-null and `error` is null; on
   **failure** `records` is null and `error` is non-null. **The callback takes
   ownership of whichever handle is non-null and must free it** (records via
   `ConsumerRecords_destroy`, error via `KafkaError_destroy` — the latter through
   `KafkaException.FromHandle`). An **empty** successful poll returns a non-null
   `records` handle with `count == 0` (not null) — success, not failure.
3. **Submit / guard / threading** (`src/ffi/consumer.rs:604-668`): identical
   shape to the void path. `acquire()` the core guard; **on guard failure the
   callback fires INLINE on the caller thread** with a `ConcurrentModification`
   error (`records == null`); on success it spawns the op, `poll(timeout).await`,
   builds the result handles with **no `.await` after**, then a completion job
   **releases the core guard BEFORE firing the callback** on the **foreign
   dispatcher thread**. Same single-owner contract M3/P2 aligned to — no managed
   guard needed.
4. **Sync `poll`** (header ~L606, `:554`):
   `ConsumerRecords_t* kafka_consumer_Consumer_poll(const Consumer_t*,
   int64_t timeout_ms, KafkaError_t** out_error)` — non-null records = success,
   null + `*out_error` set = failure. Present but **NOT used by `PollAsync`** and
   **NOT declared** this phase (confirmed decision 5): `PollAsync` uses only
   `poll_async`, and an unused `[DllImport]` would trip
   `TreatWarningsAsErrors`. The `MockConsumer` broker-free drivers already give
   deterministic success/failure/empty through the async path.
5. **`ConsumerRecords_t` accessors** (all `const *` = borrowed except
   `_destroy`):
   - `int32_t ConsumerRecords_count(const ConsumerRecords_t*)` (null-safe → 0)
   - `bool ConsumerRecords_is_empty(const ConsumerRecords_t*)` (null-safe → true)
   - `const ConsumerRecord_t* ConsumerRecords_get(const ConsumerRecords_t*,
     int32_t index)` — **borrowed** (Category 4), null if out of range / index<0
   - `void ConsumerRecords_destroy(ConsumerRecords_t*)` — the **owner** free
     (Category 3), null-safe.
6. **`ConsumerRecord_t` accessors** (all borrowed, Category 4 — never freed):
   - `int32_t ConsumerRecord_partition(const ConsumerRecord_t*)`
   - `int64_t ConsumerRecord_offset(const ConsumerRecord_t*)`
   - `int64_t ConsumerRecord_timestamp(const ConsumerRecord_t*)` (`-1` =
     NO_TIMESTAMP)
   - `int32_t ConsumerRecord_timestamp_type(const ConsumerRecord_t*)` (`-1`
     NoTimestampType / `0` CreateTime / `1` LogAppendTime)
   - `const char* ConsumerRecord_topic(const ConsumerRecord_t*, int32_t*
     out_len)` — **length-delimited** slice borrowing into the batch, **NOT
     NUL-terminated** (ffi §B3: use `out_len`, NEVER NUL-scan)
   - `const uint8_t* ConsumerRecord_key(const ConsumerRecord_t*, int32_t*
     out_len)` — `(ptr, len)`, or `(null, -1)` if the key is absent
   - `const uint8_t* ConsumerRecord_value(const ConsumerRecord_t*, int32_t*
     out_len)` — `(ptr, len)`, or `(null, -1)` if the value is absent
     (tombstone)
   - `int32_t ConsumerRecord_serialized_key_size` / `_serialized_value_size`
     (`-1` if null) — **out of scope this phase** (see §3-clip: not on the §3
     `ConsumerRecord` sketch)
   - `bool ConsumerRecord_leader_epoch(const ConsumerRecord_t*, int32_t*
     out_epoch)` / `bool ConsumerRecord_delivery_count(..., int32_t* out_count)`
     — present but **out of scope** (delivery_count is KIP-932 share-consumer,
     §20; leader_epoch not on the §3 sketch)
   - Headers: `int32_t ConsumerRecord_header_count(const ConsumerRecord_t*)`;
     `const char* ConsumerRecord_header_key(const ConsumerRecord_t*, int32_t
     index, int32_t* out_len)` (length-delimited, §B3); `const uint8_t*
     ConsumerRecord_header_value(const ConsumerRecord_t*, int32_t index,
     int32_t* out_len)` (`(null, -1)` if out of range or value null). Headers are
     **in scope** this phase (confirmed decision 2) — carried on the **internal**
     `ConsumerRecord` (copy-out, internal only; no public `Headers` type). These
     three accessors are declared and the header-key path exercises §B3.
7. **`MockConsumer` broker-free drivers** (all sync, run under the core guard):
   - `KafkaError_t* MockConsumer_add_record(const Consumer_t*, const char* topic,
     int32_t partition, int64_t offset, const uint8_t* key, int32_t key_len,
     const uint8_t* value, int32_t value_len)` — `len < 0` (or null ptr) = absent
     key/value. **The record's partition must already be assigned** (via
     `Consumer_assign`) or `add_record` errors (`src/ffi/consumer.rs:1199`).
   - `KafkaError_t* MockConsumer_set_poll_error(const Consumer_t*, const char*
     message)` — injects an `illegal_state` error returned by the **next**
     `poll` (mirrors Java `setPollException`). This drives the FAILURE test
     broker-free.
   - `KafkaError_t* Consumer_assign(const Consumer_t*, const char* const* topics,
     const int32_t* partitions, int32_t count)` — needed so `add_record` has an
     assigned partition (parallel arrays; already how the void path marshals a
     `const char* const*`).
8. **Broker-free error codes are indistinct** (M3/P1 finding #4, unchanged):
   `Wakeup` / `ConcurrentModification` / `IllegalState` / `IllegalArgument` all
   surface `code() == UnknownServerError == -1`. **Tests assert on exception
   TYPE + `Message` + one-shot/reusability, NEVER on a distinctive `Code`.**
9. **M3/P1 + M3/P2 assets to reuse:** `OperationCompletionSource` (void bridge;
   `RunContinuationsAsynchronously`, cancellation → wakeup →
   `OperationCanceledException`, idempotent `GCHandle` free via
   `Complete`/`AbandonBeforeSubmit`); `ConsumerCallbacks.Operation` (void
   trampoline, no-throw); `NativeConsumer.SubmitVoidOperation` (submit shape,
   `DangerousGetHandle()`, `AbandonBeforeSubmit` on submit-throw);
   `Wakeup()`/`ThrowIfClosed()`/`TryBeginClose()`; `KafkaException.FromHandle`;
   `Utf8Marshal.Pin` (in) / `PtrToString` (NUL form); `TestTimeout.Run` (sync +
   async overloads). Note **there is no length-delimited `PtrToString(ptr, len)`
   yet** (M1/P1 D4 shipped the NUL form only) — this phase adds it (§B3).

---

## Scope

**In scope:**

- The **owned-handle completion bridge** — the result-returning analog of the
  M3 void bridge (see "Bridge design" below). One generic result carrier
  (`OperationCompletionSource<TResult>`, confirmed decision 4) + a
  `poll_callback_t` trampoline on `ConsumerCallbacks`, no-throw boundary, all 5
  invariants kept.
- `ConsumerRecord` + `ConsumerRecords` value types — **internal** (namespace
  `Confluent.Kafka.Internal`, under `Internal/`; confirmed decision 1) — and
  **copy-out** per §6.4 (owned managed key/value arrays; owned topic `string`;
  **headers included, internal only** — confirmed decision 2).
- New `NativeMethods` DllImports: `poll_async` (the sync `poll` is **NOT**
  declared — confirmed decision 5), the full `ConsumerRecords_t` /
  `ConsumerRecord_t` accessor set in scope (item 5/6 above, minus the
  explicitly-out-of-scope accessors), the **header accessors** (`header_count` /
  `header_key` / `header_value`, confirmed decision 2), `MockConsumer_add_record`,
  `MockConsumer_set_poll_error`, `Consumer_assign`.
- `Utf8Marshal.PtrToString(IntPtr ptr, int len)` — the **length-delimited**
  receive-path string form (§B3), un-defers M1/P1 D4's deferred half.
- `NativeConsumer.PollAsync(TimeSpan, CancellationToken)` — the proof op, on a
  `MockConsumer`.
- The **now-testable** deferred slices: wakeup-fault during an in-flight poll
  (one-shot, then reusable), in-flight `CancellationToken` cancel →
  `OperationCanceledException`, and — via a poll held open — a deterministic
  concurrent-op → faulted-`Task` (`KafkaException`/ConcurrentModification) /
  concurrent sync state read → `InvalidOperationException` slice (§B5).
- The **per-record allocation-budget** test (ffi §B4, consumer-threading §27,
  DoD §10).
- STATUS handoff + the N≥8 renumber reconciliation (see Governance).

**Explicitly deferred / out of scope (do NOT build):**

- Any **off-dispatcher / keep-alive** receive path, `SafeConsumerRecordsHandle`,
  native-backed `ReadOnlyMemory<byte>`, `ReadOnlySpan<byte>` deserialize hatch,
  process-in-place `Poll(record => …)` (the whole deferred alternative above).
- The **other five** owned-handle ops (`committed` / `offsetsForTimes` /
  `beginning|endOffsets` / `partitionsFor` / `listTopics`) and their result
  containers (`OffsetMap` / `OffsetAndTimestampMap` / `LongOffsetMap` /
  `PartitionInfoList` / `TopicPartitionInfoMap`, `Node`, `TopicPartition` as a
  map key) — this phase only proves the **shape** on `poll`. **Poll-only this
  phase — do NOT pull any sibling owned-handle op forward** (confirmed
  decision 3).
- The **public** `IConsumer` / `KafkaConsumer` / public `MockConsumer` client
  surface, and promoting the M3/P1 proof ops (`SubscribeAsync`/`SeekAsync`) to
  public. (`PollAsync` lands on the internal `NativeConsumer` like the M3/P1 proof
  ops.) `ConsumerRecord(s)` are **internal** this phase (confirmed decision 1);
  the public shape lands with the first public client.
- `serialized_key_size` / `_serialized_value_size` / `leader_epoch` /
  `delivery_count` accessors (not on the §3 `ConsumerRecord` sketch; delivery
  count is share-consumer, §20).
- `TopicPartition` / `TimestampType` as **public** types (confirmed
  out of scope, decision 1): the internal `ConsumerRecord` uses a bare
  `long Timestamp` + a plain `int` (or internal enum) for timestamp-type — no
  public enum this phase.
- Serializers / generic `Consumer<TKey,TValue>`; typed `KafkaException`
  subclasses; the §A7 producer completion decision; rebalance listeners.
- The N≥8 cross-thread hardening items (still not reachable — see Governance).

---

## Bridge design — the owned-handle completion (ffi §B6/§B7)

**Confirmed (decision 4): generalize `OperationCompletionSource` to carry a result
`T` (`OperationCompletionSource<TResult>`), rather than add a poll-specific
completion source.** The void path is expressed as `<bool>` or a thin subclass
(Actor's call, gated on all prior void-bridge tests staying green). Rationale
below.

The void bridge is `OperationCompletionSource` wrapping a
`TaskCompletionSource<bool>`. The five future owned-handle ops (and `poll`) each
return a **different** managed result type (`ConsumerRecords`,
`IReadOnlyDictionary<...>`, `IReadOnlyList<...>`, …). Introducing a per-op
completion source would duplicate the whole 5-invariant machinery (GCHandle
free-once, cancellation→wakeup, no-throw) N times. A single generic
`OperationCompletionSource<TResult>` (with the existing void bridge either kept
as-is or expressed as `OperationCompletionSource<bool>`) carries all of them.

Concrete shape:

- `OperationCompletionSource<TResult>` wraps `TaskCompletionSource<TResult>`
  (built with `RunContinuationsAsynchronously` — invariant #5). It keeps the
  existing `SetGcHandle` / `RegisterCancellation` / `AbandonBeforeSubmit` /
  `FreeGcHandle` verbatim, and the same idempotent `GCHandle`-free bookkeeping
  (invariant #2: the callback is the **sole owner** of the free; the only other
  path is `AbandonBeforeSubmit`, when native never ran).
- The **result-producing** completion method takes the **already-marshalled
  managed result** (not the native handle): e.g.
  `CompleteWithResult(TResult result)` on success, and the existing error/cancel
  mapping (`Complete(IntPtr error)` → `FromHandle` / `TrySetCanceled`) on
  failure. **The marshalling itself lives in the trampoline** (on the dispatcher
  thread — the key decision), not inside the completion source, so the completion
  source stays result-type-agnostic and does no native reads.
- Migration options for the existing void bridge (Actor's call, record in
  COMMENTS.DONE): either (a) `OperationCompletionSource` becomes a thin subclass
  / alias of `OperationCompletionSource<bool>`, or (b) leave the void class and
  add the generic alongside. Prefer (a) if it does not churn M3/P1/P2 tests; the
  DoD gate is 0-warning build + all prior tests still green.

**`ConsumerCallbacks` gains a `poll_callback_t` trampoline** — a second kept-alive
`[UnmanagedFunctionPointer(Cdecl)]` delegate + one `static readonly` rooted
instance, mirroring `Operation`:

```
[UnmanagedFunctionPointer(CallingConvention.Cdecl)]
internal delegate void PollCallback(IntPtr records, IntPtr error, IntPtr userData);
internal static readonly PollCallback Poll = OnPoll;

private static void OnPoll(IntPtr records, IntPtr error, IntPtr userData)
{
    OperationCompletionSource<ConsumerRecords>? context = null;
    try
    {
        GCHandle handle = GCHandle.FromIntPtr(userData);
        context = (OperationCompletionSource<ConsumerRecords>)handle.Target!;
        if (error != IntPtr.Zero)
        {
            // records is null on failure. FromHandle owns+frees the error.
            context.Complete(error);            // maps to KafkaException / OperationCanceledException
        }
        else
        {
            // SUCCESS. records is a non-null owned borrow-root. Copy-out on THIS
            // (dispatcher) thread, then destroy — the whole §6.4 default.
            ConsumerRecords marshalled = ConsumerRecordsMarshal.CopyOut(records);
            context.CompleteWithResult(marshalled);   // RunContinuationsAsynchronously
        }
    }
    catch (Exception ex) { context?.TrySetException(ex); }   // no-throw boundary
    finally
    {
        // The callback is the SOLE owner of BOTH frees, on EVERY path:
        //   (1) the owned ConsumerRecords_t batch (records), after copy-out;
        //   (2) the per-op GCHandle.
        if (records != IntPtr.Zero) NativeMethods.ConsumerRecordsDestroy(records);
        context?.FreeGcHandle();
    }
}
```

**Free-exactly-once, every path — the phase's central correctness obligation.**
The callback frees, in a `finally` so it runs on the no-throw path too:
1. the owned `ConsumerRecords_t` batch **after** the copy-out (success path;
   `records` is null on failure/rejection so the destroy is a no-op) — via a
   null-safe `ConsumerRecords_destroy`;
2. the `KafkaError` on failure (through `KafkaException.FromHandle`, which
   `_destroy`s in its own `finally`) — inside `Complete(error)`;
3. the per-op `GCHandle` (`FreeGcHandle`, idempotent).

On the **inline core-guard-rejection** path (`records == null`, non-null
`error`, fired on the caller thread) the same `finally` runs: destroy is a no-op,
`Complete(error)` faults the `Task` with the `ConcurrentModification`
`KafkaException`, GCHandle freed once. On the **submit-threw** path,
`AbandonBeforeSubmit` frees the GCHandle (native never ran, callback can't fire) —
unchanged from M3/P1.

**Copy-out marshaller** (`ConsumerRecordsMarshal.CopyOut(IntPtr records)`, under
`Internal/Interop/`, `unsafe`) — runs **on the dispatcher thread**:
- `count = ConsumerRecords_count(records)`; pre-size the managed list.
- For each `i` in `0..count`: `rec = ConsumerRecords_get(records, i)` (Category-4
  borrowed, never freed); read `partition` / `offset` / `timestamp` /
  `timestamp_type`; `topic` via `ConsumerRecord_topic(rec, out len)` →
  `Utf8Marshal.PtrToString(ptr, len)` (**length-delimited**, never NUL-scan,
  §B3); `key` / `value` via `_key`/`_value(rec, out len)` → **copy** into an owned
  `byte[]` when `len >= 0`, else `null` (tombstone / absent); **headers**
  (confirmed decision 2) via `_header_count` then, per index, `_header_key(rec, i,
  out len)` → `Utf8Marshal.PtrToString(ptr, len)` (**length-delimited §B3** — this
  is the path the header round-trip test exercises) + `_header_value(rec, i, out
  len)` → owned `byte[]` (or `null`).
- Assemble owned `ConsumerRecord` values into a `ConsumerRecords`
  (`IReadOnlyCollection<ConsumerRecord>`). No borrowed pointer is retained past
  the loop; nothing references the batch after `CopyOut` returns → the subsequent
  `ConsumerRecords_destroy` is safe.

---

## `ConsumerRecord` / `ConsumerRecords` value types (§6.4 copy-out)

Per the §3 sketch (clipped to today's ABI + this phase's scope):

```csharp
// namespace Confluent.Kafka.Internal — under Internal/ (confirmed decision 1).
// sealed; getters → properties. Owned copy-out — no native backing (§6.4).
internal sealed class ConsumerRecord
{
    public string Topic { get; }                 // owned copy (length-delimited, §B3)
    public int Partition { get; }
    public long Offset { get; }
    public long Timestamp { get; }               // -1 = NO_TIMESTAMP
    public int TimestampType { get; }            // plain int / internal enum (no public enum)
    public ReadOnlyMemory<byte>? Key { get; }    // owned byte[]; null = absent
    public ReadOnlyMemory<byte>? Value { get; }  // owned byte[]; null = tombstone
    // Headers — internal only (confirmed decision 2); see the headers bullet below.
    public IReadOnlyList<(string Key, ReadOnlyMemory<byte>? Value)> Headers { get; }
}

internal sealed class ConsumerRecords : IReadOnlyCollection<ConsumerRecord>
{
    public int Count { get; }
    public IEnumerator<ConsumerRecord> GetEnumerator();   // insertion order
}
```

- **Key/value:** `ReadOnlyMemory<byte>?` wrapping an **owned** `byte[]` copied
  from the borrowed slice (§B4 copy-out default). `len < 0` / null ptr → `null`
  (absent key / tombstone value).
- **Topic:** owned `string` from the length-delimited slice (§B3) — a `string`
  **must** copy (can't borrow native UTF-8), so both copy-out and keep-alive
  behave identically here.
- **Headers (confirmed decision 2 — included, internal only):** `Headers` =
  `IReadOnlyList<(string Key, ReadOnlyMemory<byte>? Value)>` (or a small **owned
  internal `RecordHeader`** type — Actor's call), each key length-delimited (§B3),
  each value an owned `byte[]` (or `null`). No **public** `Headers` type is
  surfaced — the representation stays on the internal `ConsumerRecord`. The header
  round-trip test (incl. a non-ASCII header key via `out_len`) is what exercises
  the §B3 length-delimited header-key path.

`String` / topic and typed deserialize are copy-out regardless of the (deferred)
keep-alive choice (§6.4), so nothing here forecloses a future zero-copy path.

---

## DllImport additions (`NativeMethods`, full ABI symbol as `EntryPoint`; ffi §0.1 type map)

Type map (unchanged from M3/P1): opaque `*_t` / `const char*` / `const uint8_t*`
→ `IntPtr`; `int32_t` → `int`; `int64_t` → `long`; `bool` → return `bool` with
`[MarshalAs(UnmanagedType.I1)]`; `int32_t* out_len` → `out int`; `void* user_data`
→ `IntPtr` (GCHandle); `poll_callback_t` → kept-alive Cdecl delegate. No
`[LibraryImport]` / `delegate* unmanaged` / `LPUTF8Str` (netstandard2.0 floor).

- `void kafka_consumer_Consumer_poll_async(IntPtr consumer, long timeoutMs,
  ConsumerCallbacks.PollCallback cb, IntPtr userData)`
- The sync `kafka_consumer_Consumer_poll` is **NOT** declared (confirmed
  decision 5) — `PollAsync` uses only `poll_async`; an unused `[DllImport]` would
  trip `TreatWarningsAsErrors`.
- `int kafka_consumer_ConsumerRecords_count(IntPtr records)`
- `bool kafka_consumer_ConsumerRecords_is_empty(IntPtr records)` (`[return:
  MarshalAs(I1)]`)
- `IntPtr kafka_consumer_ConsumerRecords_get(IntPtr records, int index)`
- `void kafka_consumer_ConsumerRecords_destroy(IntPtr records)` (null-safe)
- `int kafka_consumer_ConsumerRecord_partition(IntPtr record)`
- `long kafka_consumer_ConsumerRecord_offset(IntPtr record)`
- `long kafka_consumer_ConsumerRecord_timestamp(IntPtr record)`
- `int kafka_consumer_ConsumerRecord_timestamp_type(IntPtr record)`
- `IntPtr kafka_consumer_ConsumerRecord_topic(IntPtr record, out int outLen)`
- `IntPtr kafka_consumer_ConsumerRecord_key(IntPtr record, out int outLen)`
- `IntPtr kafka_consumer_ConsumerRecord_value(IntPtr record, out int outLen)`
- Headers (confirmed decision 2 — included):
  `int kafka_consumer_ConsumerRecord_header_count(IntPtr record)`;
  `IntPtr kafka_consumer_ConsumerRecord_header_key(IntPtr record, int index, out
  int outLen)`;
  `IntPtr kafka_consumer_ConsumerRecord_header_value(IntPtr record, int index,
  out int outLen)`
- Mock drivers: `IntPtr kafka_consumer_MockConsumer_add_record(IntPtr consumer,
  IntPtr topic, int partition, long offset, IntPtr key, int keyLen, IntPtr
  value, int valueLen)` (returns `KafkaError_t*`); `IntPtr
  kafka_consumer_MockConsumer_set_poll_error(IntPtr consumer, IntPtr message)`;
  `IntPtr kafka_consumer_Consumer_assign(IntPtr consumer, IntPtr[] topics, int[]
  partitions, int count)` (parallel arrays — pin call-scoped, same idiom the void
  path uses for `subscribe_async`'s `const char* const*`).

The Actor reads the generated header for the exact spelling/const-ness before
declaring each (ffi §0.1); this list is the shape, the header is the contract.

---

## `NativeConsumer.PollAsync` — the proof op

`internal Task<ConsumerRecords> PollAsync(TimeSpan timeout, CancellationToken
cancellationToken = default)`:

- `ThrowIfClosed()`; `cancellationToken.ThrowIfCancellationRequested()`;
  validate `timeout` (negative → `ArgumentOutOfRangeException`, §B5 precondition,
  before any P/Invoke); convert to `long` ms (Java `Duration` → `int64_t` ms).
- Build `OperationCompletionSource<ConsumerRecords>`; `GCHandle.Alloc(context,
  Normal)`; `SetGcHandle`; `RegisterCancellation(cancellationToken, Wakeup)`.
- P/Invoke `poll_async(_handle.DangerousGetHandle(), ms,
  ConsumerCallbacks.Poll, GCHandle.ToIntPtr(gcHandle))`; on submit-throw
  `context.AbandonBeforeSubmit(); throw;` (mirrors `SubmitVoidOperation`).
- Return `context.Task`. Consider a `SubmitOperation<TResult>` generic sibling to
  `SubmitVoidOperation` so the two share the alloc/register/submit/abandon shape.

Driven broker-free on a `MockConsumer`:
- **SUCCESS:** `assign(topic, partition)` → `add_record(topic, partition,
  offset, key, value)` (one or more) → `PollAsync` → `ConsumerRecords` with the
  round-tripped fields.
- **FAILURE:** `set_poll_error("boom")` → `PollAsync` → faulted `Task` with
  `KafkaException` (message "boom", asserted; Code is -1 — do NOT assert on it).
- **EMPTY:** `assign` with no records → `PollAsync` → non-null `ConsumerRecords`
  with `Count == 0` (success, not fault).

---

## Tests (TEST project; internals via `InternalsVisibleTo`; `Interop/` layout; every awaited op AND teardown under a `TestTimeout` hang-guard)

New / added:

- **Poll SUCCESS round-trip:** topic / partition / offset / timestamp /
  timestamp_type / key / value round-trip (all in scope); **non-ASCII topic and
  key/value** via the length-delimited `out_len` path (a multi-byte char at the
  slice boundary marshals correctly — §B3).
- **Header round-trip (confirmed decision 2):** a record with one or more headers
  round-trips key + value, incl. a **non-ASCII header key** via the
  length-delimited `out_len` path (§B3 — never NUL-scan), a null header value
  (`(null, -1)`), and the empty-headers (`header_count == 0`) case. This is what
  exercises the §B3 header-key path.
- **Poll FAILURE:** `set_poll_error` → faulted `Task` carrying `KafkaException`;
  assert TYPE + `Message` (not Code); the error handle + `GCHandle` freed once.
- **Empty batch:** non-null `ConsumerRecords`, `Count == 0`,
  `is_empty == true`; the (empty) batch handle is still `_destroy`ed once.
- **Churn / no-corruption:** many polls in a loop (records + errors + empties
  interleaved) with a double-free / leak detector — asserts the batch `_destroy`
  and the per-op `GCHandle`/error frees each happen **exactly once** per poll
  (the owned-result-handle analog of M3/P1's void churn test).
- **GC keep-alive:** aggressive `GC.Collect()` / `WaitForPendingFinalizers`
  during an in-flight poll does not collect the `Poll` delegate or the context.
- **`RunContinuationsAsynchronously`:** the awaiter's continuation does NOT run
  inline on the dispatcher thread (assert a different managed thread id, or a
  continuation that re-enters the consumer does not deadlock).
- **No-throw boundary:** an exception forced inside the poll callback / copy-out
  is caught (faults the `Task`), no crash / no unwind into native.
- **Per-record allocation budget** (ffi §B4, §27, DoD §10): assert the
  receive-path copy-out budget — the per-record allocations are exactly the owned
  copies (topic `string`, key `byte[]`, value `byte[]`, and the header copies —
  in scope this phase) and **nothing attributable to batch traversal, borrowed-pointer
  marshalling, or a native-backed view**. Follow the producer send-path
  allocation-test precedent (Phase 6 pattern noted in §7.4 / DoD §10); measure
  with `GC.GetAllocatedBytesForCurrentThread()` deltas around a steady-state
  poll, tolerating a documented fixed overhead.

**Now-unlocked deferred slices** (need a wakeup-observing op — `poll`):

- **Wakeup-fault (M3/P1 D1 un-deferred):** `Wakeup()` during an in-flight
  `PollAsync` → the `Task` faults with a `KafkaException` (Wakeup semantics)
  **once**, then the consumer is **reusable** (a subsequent `PollAsync`
  succeeds). Assert TYPE + one-shot + reusability, not Code.
- **In-flight cancellation:** a `CancellationToken` canceled **while** the
  `PollAsync` is in flight → `wakeup()` → the `Task` surfaces
  `OperationCanceledException` (distinct from a wakeup `KafkaException`); the
  consumer remains reusable. (M3/P1 could only test the *pre-canceled* token
  deterministically; poll makes the *in-flight* cancel deterministic.)
- **Concurrency matrix (M3/P2 D-Q4 un-deferred):** hold a poll open (a
  controllable-duration guard-holding op) and, concurrently, (a) submit a second
  async op → its `Task` faults with `KafkaException`/ConcurrentModification
  (core-delivered, §B5); (b) issue a concurrent sync state read (`GroupId`) →
  `InvalidOperationException`. This is the deterministic overlap M3/P2 D-Q4 said
  was "not reproducible broker-free without a controllable guard-holding op" —
  `poll` (with a slow/blocked mock poll, or a poll awaiting a record that arrives
  on a test signal) is exactly that op. If a fully deterministic block point is
  not achievable on `MockConsumer` broker-free, document the residual the same
  way M3/P2 D-Q4 did and keep the reachable slice — but attempt determinism
  first (this is the phase whose whole point is that poll makes it reachable).

Carried M0–M3/P2 tests remain green (the generic-bridge migration must not
regress the void-bridge tests).

---

## Files

**New (library):**
- `src/Confluent.Kafka/Internal/ConsumerRecord.cs` — the value type, **internal**
  (`Confluent.Kafka.Internal`, under `Internal/` per confirmed decision 1), incl.
  the internal header representation. Apache-2.0 header.
- `src/Confluent.Kafka/Internal/ConsumerRecords.cs` —
  `IReadOnlyCollection<ConsumerRecord>`, **internal**, under `Internal/`.
- `src/Confluent.Kafka/Internal/Interop/ConsumerRecordsMarshal.cs` — the
  `unsafe` copy-out marshaller (dispatcher-thread; §B3/§B4/§6.4). Under
  `Internal/Interop/` (the only place `unsafe` lives).
- (If the generic bridge is a new file) `src/Confluent.Kafka/Internal/
  OperationCompletionSourceT.cs` (i.e. `OperationCompletionSource<TResult>`) —
  the generic completion source; or generalize the existing
  `OperationCompletionSource.cs` in place.

**Modified (library):**
- `src/Confluent.Kafka/Internal/Interop/NativeMethods.cs` — the DllImports above.
- `src/Confluent.Kafka/Internal/Interop/ConsumerCallbacks.cs` — the
  `PollCallback` delegate + rooted `Poll` instance + `OnPoll` no-throw body.
- `src/Confluent.Kafka/Internal/OperationCompletionSource.cs` — generalize to
  `<TResult>` (+ `CompleteWithResult`), keeping the void path working (invariants
  #1–#5 unchanged).
- `src/Confluent.Kafka/Internal/NativeConsumer.cs` — `PollAsync`; optional
  `SubmitOperation<TResult>` generic submit helper.
- `src/Confluent.Kafka/Internal/Interop/Utf8Marshal.cs` — add
  `PtrToString(IntPtr, int len)` (length-delimited, §B3; un-defers M1/P1 D4).

**Modified (tests):**
- `tests/Confluent.Kafka.UnitTests/Interop/**` — new poll/receive-path test
  files (SUCCESS / FAILURE / empty / churn / GC keep-alive /
  RunContinuationsAsynchronously / no-throw / allocation budget / wakeup / cancel
  / concurrency).

**Modified (design):**
- `bindings/dotnet/design/current/STATUS.md` — M3/P3 handoff; the N≥8 renumber
  reconciliation; move the M3/P1 D1 / M3/P2 D-Q4 deferrals from deferred → done.

---

## Verification gates (.NET DoD — NOT `cargo xtask` / `make verify`; in order)

1. `cargo build --features ffi` — native cdylib + regenerated header present
   (run **FIRST**, CLAUDE.md §7.1). No ABI change this phase (Mode A) — this just
   ensures the native the binding P/Invokes exists.
2. `dotnet build` — **0 warnings / 0 errors** across library TFMs
   (`netstandard2.0;net8.0;net10.0`) + test TFMs (`net8.0;net10.0`).
   `TreatWarningsAsErrors` + `EnforceCodeStyleInBuild` active. `ConsumerRecord` /
   `ConsumerRecords` are **internal** (confirmed decision 1), so — as in
   M3/P1/P2 — there is **no new public surface and thus no new CS1591 / XML-doc
   obligation**. Apache-2.0 header on new `.cs` files; no TODO/FIXME.
3. `dotnet test -f net10.0` — all pass, **no hang**; every awaited op / teardown
   under a `TestTimeout` hang guard.
4. `dotnet format --verify-no-changes` — clean.
5. **CI-only caveat:** local env is system dotnet SDK 10.0.302 + net10 runtime at
   `/usr/local/share/dotnet/dotnet`; the **net8 test run** and **net462** are
   CI-only (both *build* legs must pass; do NOT block DoD on those *runs*).

---

## Governance

- **Personas:** `dotnet-actor` (Actor **N=7**) implements; `dotnet-critic`
  (Critic **N=7**) reviews. NEVER the Rust `actor-executor` / `kafka-critic`.
  Nested-agent discovery does not work — the root-`.claude/agents/`
  `dotnet-{actor,critic}.md` **discovery copies** already exist (verified) and
  stay **untracked**; the binding-local personas stay **tracked** (edit those).
- **Review files:** working `bindings/dotnet/COMMENTS.7.md` → resolved to
  `bindings/dotnet/COMMENTS.DONE.7.md`. `COMMENTS.7.md` is gitignored
  (`COMMENTS\.[0-9]*\.md`); `COMMENTS.DONE.7.md` is **not** gitignored — never
  `git add` it. **Reset for N=7:** the existing `bindings/dotnet/COMMENTS.6.md`
  and `COMMENTS.DONE.6.md` (M3/P2's working files) must be cleared/reset before
  the loop starts (the tracked archive is under
  `design/history/M3/P2-single-owner-alignment/`). At Final Handoff, copy the
  closed `COMMENTS.DONE.7.md` to
  `design/history/M3/P3-poll-receive-path/COMMENTS.DONE.7.md` and reset the
  binding-root working files.
- **Branch / PR (confirmed):** land on
  **`prashah_dev_asyncbridge_poll_scaffolding`** (current HEAD; verified to
  descend from `prashah_dev_asyncbridge_scaffolding` = PR #135's head, and
  currently identical to it — no additive commits yet). M3/P3's commits are
  **additive** on this branch, and they open a **NEW PR stacked on top of PR
  #135** — do **NOT** extend #135 in place. (M3/P1+P2 both extended #135 in
  place; M3/P3 is a new stacked PR — the poll-scaffolding branch is its head.)
  Small incremental commits, each passing the .NET gates; Apache-2.0 header on new
  files. `bindings/dotnet/.claude/agent-memory/**` excluded from every commit/PR
  (only `.gitkeep`). Verify each commit's staged file list.
- **STATUS reconciliation / numbering (N≥7 → N≥8):** STATUS's deferred
  cross-thread **hardening** items (`Wakeup`/`GroupId` handle TOCTOU vs teardown;
  the submit-vs-`destroy` handle race) were pre-labeled "N≥7". M3/P3 **takes
  N=7**, so that label now collides. `poll` makes the wakeup-fault *behavior*
  testable, but it does **NOT** make the cross-thread *races* reachable — the
  binding is still internal-only, single-owner, with **no public cross-thread
  `Wakeup()` caller** (there is no public client type this phase). So those
  hardening items **stay deferred** and must be **renumbered to N≥8** in STATUS,
  with a one-line note that N=7 is M3/P3 and the hardening is now "N≥8, whenever a
  public client makes `Wakeup()` genuinely cross-thread." Leave **no dangling or
  contradictory "N≥7" label**. (Mirror the M3/P2 precedent, which resolved the
  earlier "N=6 deferred" collision the same way.)
- **The three M3/P2 accepted residuals remain accepted-by-design** (misuse-only,
  not reachable while internal-only): teardown-with-unawaited-in-flight-op strand
  + one-time leak; `Wakeup`/`GroupId` TOCTOU; submit-vs-`destroy` handle race.
  M3/P3 adds no new residual — the on-dispatcher copy-out specifically **avoids**
  a new leak surface (no `SafeConsumerRecordsHandle`, no batch escaping the
  callback).

---

## Risks / confirmed decisions

**Confirmed decisions (approved):** the six items below were open at draft time and
are now decided by the user. They are threaded through every section above; this
subsection is the authoritative record.

1. **`ConsumerRecord` / `ConsumerRecords` are `internal` this phase.** Namespace
   `Confluent.Kafka.Internal`, files under `Internal/`; not public. This matches
   M3/P1/P2 (whole bridge internal; `KafkaException` the only public type), keeps
   the proof phase's new public surface at zero, and defers the public shape
   (`IConsumer` / `KafkaConsumer` / public `MockConsumer`) to the first-public-
   client phase. Consequence: no new CS1591 / XML-doc obligation (Verification
   gate 2); `TimestampType` / `TopicPartition` stay **out of scope as public
   types** — the internal `ConsumerRecord` uses a bare `long Timestamp` + a plain
   `int` (or internal enum) for timestamp-type.

2. **Headers: included, copy-out, internal only.** The internal `ConsumerRecord`
   carries an internal header representation
   (`IReadOnlyList<(string Key, ReadOnlyMemory<byte>? Value)>` or a small internal
   `RecordHeader` — Actor's call): each key marshalled via the **length-delimited
   §B3** path, each value copied into an owned `byte[]` (or `null`). No public
   `Headers` type is surfaced. The DllImport list keeps the header accessors
   (`header_count` / `header_key` / `header_value`), the copy-out marshaller
   handles headers, and a **header round-trip test** (incl. a non-ASCII header key
   via `out_len`) is in the Tests section — this is what exercises the §B3
   header-key path.

3. **Poll-only this phase.** Do NOT pull any sibling owned-handle op (`committed` /
   `offsetsForTimes` / `beginning|endOffsets` / `partitionsFor` / `listTopics`)
   forward — they stay in "explicitly deferred." The phase proves the owned-handle
   **shape** once, on `poll`; poll + the void bridge already prove both a value
   result and a void result.

4. **Generalize the bridge to `OperationCompletionSource<TResult>`.** The void
   path is expressed as `<bool>` or a thin subclass (Actor's call, gated on all
   prior void-bridge tests staying green). A per-op source would duplicate the 5
   invariants N times for the five future siblings.

5. **Do NOT add the sync `poll` DllImport.** `PollAsync` uses only `poll_async`;
   an unused `[DllImport]` would trip `TreatWarningsAsErrors`, and the
   `MockConsumer` broker-free drivers already give deterministic
   success/failure/empty through the async path. (This resolves the former
   Open-question-4 as **"no".**)

6. **Marshalling ON the dispatcher thread.** The baked-in decision (see "The key
   design decision") stands: the batch copy-out happens on the dispatcher thread
   inside the callback; the off-dispatcher / keep-alive zero-copy path remains the
   deferred alternative.

**Risks:**

- **The deterministic concurrency slice (D-Q4 un-defer) may still be hard
  broker-free.** M3/P2 documented that instant Mock ops make a genuine
  submit→callback overlap non-deterministic; `poll` is the intended
  controllable-duration op, but whether `MockConsumer` can *block* a poll at a
  test-controlled point (vs. returning instantly) needs the Actor to verify
  against `src/consumer/mock_consumer.rs` early. If a fully deterministic block
  point is not reachable, keep the reachable slice + document the residual (as
  D-Q4 did) rather than shipping a flaky test.
- **Allocation-budget test fragility.** `GC.GetAllocatedBytesForCurrentThread()`
  deltas are sensitive to JIT / TFM / warm-up; the test must warm up, measure
  steady-state, and assert a *budget* (owned copies only) with a documented fixed
  overhead — not an exact byte count. Precedent: the producer send-path
  allocation test.
- **Generic-bridge migration churn.** Generalizing `OperationCompletionSource`
  touches M3/P1/P2 code paths; the risk is a silent behavioral change to the void
  bridge (e.g. cancellation mapping). Mitigation: keep the void semantics
  byte-for-byte, express void as `<bool>`, and gate on all carried tests green.
- **Length-delimited marshalling correctness (§B3).** A NUL-scan on the topic /
  header-key slice over-reads past the batch (AV / garbage) — the exact
  anti-pattern §B3 calls out. The new `PtrToString(ptr, len)` must use the length
  and never scan; the non-ASCII / boundary-char test guards this.
- **`add_record` requires an assigned partition** — the SUCCESS test must
  `assign` first, or `add_record` errors. The Actor confirms the assign→add→poll
  order against `src/ffi/consumer.rs` before wiring the test.
