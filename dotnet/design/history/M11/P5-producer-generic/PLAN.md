# M11/P5 — Typed generic producer (`Producer<K,V>` — generic-only conversion) — DRAFT PLAN

> **Status: DRAFT — for user review. PLAN-ONLY.** No Actor/Critic spawned, no loop
> run, nothing archived, nothing committed. On approval this becomes the forward
> plan for the M11/P5 execution loop.

---

## 1 · Scope, branch, phase, agent number

- **Phase:** **M11/P5** — the **typed generic producer** `Producer<K,V>`. The **last
  Mode-A piece of the producer surface** (foundation → async peripherals → send →
  sync → **typed generic**).
- **Branch:** `prashah_dev_producer_generic`, HEAD `85800bb2`, stacked on
  `prashah_dev_producer_sync` (which carries M11/P4 + M11/P4.1). Already checked out.
  All planning + the later loop happen here.
- **Mode:** **Mode A only** — a pure managed serialization layer over the existing
  bytes send path. Hard invariant: **no** change to `src/**`, `src/ffi/**`,
  `target/include/confluent_kafka.h`, or `cbindgen.toml`. Verified Mode-A-sufficient
  (§8). This phase is even leaner at the interop boundary than P3/P4: it adds **zero
  new `[DllImport]`s** (serialization is entirely managed; the serialized bytes flow
  into the *already-shipped* `NativeProducer.Send` / `SendViaPump`). If planning
  reveals any Mode-B need, **STOP and flag it to main** — do not invent an ABI.
- **Agent number (later loop):** **N = 33** — the next free number in the binding's
  own monotonic sequence (N=30 M11/P3, N=31 M11/P4, N=32 M11/P4.1, **N=33 M11/P5**).
  Noted here, **not run** this session. `COMMENTS.33.md` / `COMMENTS.DONE.33.md` are
  the working files once the loop starts.

**Un-gating note (state in the plan).** The typed producer was explicitly **gated
on "the producer completion model is OPEN" (ffi §A7)** — CLAUDE.md §3/§4 and the
M6/P1a serde-foundation plan both record it as *"deferred, gated on the OPEN §A7
decision."* **Option C (inline pull-pump) shipping in M11/P3 closed §A7**, so the
gate is lifted. This phase finally consumes the **`ISerializer<T>` foundation
shipped in M6/P1a** ("ships ready for the (deferred) typed producer, tested
directly"). It is the send-side symmetric twin of the consumer's **M6/P1b** typed
poll (which consumed `IDeserializer<T>`).

---

## 2 · Parity anchor (mandatory guardrail — the M11/P2 retro rule)

> Review **every public member** of this phase against this anchor. Anything outside
> it is a Critic finding. (The retro rule: a green build + green tests + a clean
> Critic still missed invented surface / precedent divergence / dead code in M11/P2 —
> pin the anchor up front and review against it.)

### (a) Anchor #1 — Java `Producer<K,V>` (the reference shape)

- **`KafkaProducer<K, V> implements Producer<K, V>`** (`KafkaProducer.java:241`) —
  a single **generic** producer, no bytes sibling. 3-param ctor
  `KafkaProducer(Map<String,Object>, Serializer<K>, Serializer<V>)`
  (`KafkaProducer.java:300`).
- **`ProducerRecord<K, V>`** — generic key/value; ctor validates topic-non-null,
  partition-non-negative, timestamp-non-negative-or-null (`ProducerRecord.java:70-77`).
- **`MockProducer<K, V> implements Producer<K, V>`** (`MockProducer.java:55`) — the
  mock **takes `Serializer<K>/Serializer<V>`** (`MockProducer.java:104-135`) and
  serializes records into its `sent` history. (Contrast the consumer: Java's
  `MockConsumer` does **not** take deserializers — so the producer's
  mock-takes-serializers is **Java-faithful**, not the deviation it was for the
  consumer — §7.)
- **`KafkaProducer.doSend` serialize path** (`KafkaProducer.java:1003-1019`):
  `serializedKey = keySerializer.serialize(topic, headers, record.key())` and
  `serializedValue = valueSerializer.serialize(topic, headers, record.value())` —
  the serializer is **always invoked** (even for a `null` key/value); a
  `ClassCastException` is wrapped in `SerializationException`. The serialized
  `byte[]` may be `null` (Java `Serializer` javadoc: *"recommended to serialize
  `null` data to the `null` byte array; @return … may be `null`"*), and a `null`
  serialized value/key is a tombstone / no-key.

### (b) Anchor #2 — the consumer's shipped **M6/P1b generic-only conversion** (the direct in-repo precedent to MIRROR)

The consumer went **generic-only** (`design/history/M6/P1b-typed-consumers/`):
Java's single `Consumer<K,V>`, no bytes sibling; the six shipped client types +
records became `<TKey,TValue>` and the non-generic ones were **removed**; bytes
users write `<byte[],byte[]>` + `Serdes.ByteArray`. Its load-bearing decisions,
which this phase mirrors **symmetrically on the send path**:

| Consumer M6/P1b (receive) | Producer M11/P5 (send) — the mirror |
|---|---|
| Generic-only: convert the 6 clients + records, **remove** the non-generic ones | Generic-only: convert the 6 producer types + `ProducerRecord`, **remove** the non-generic ones (§3.1) |
| 3-param ctor `(config, keyDeser, valueDeser)` (decision A) | 3-param ctor `(config, keySer, valueSer)` (decision A) — Java `KafkaProducer.java:300` |
| `NativeConsumer` stays bytes-only; typed poll is a thin skin that **deserializes before destroy** | `NativeProducer` send **logic** unchanged; typed send is a thin skin that **serializes before the P/Invoke** (§3.3, CLAUDE.md §11) |
| null / absent → `default(T)`, deserializer **NOT** invoked (forced: can't build a null span) | null key/value → serializer **IS** invoked (Java-faithful; **not** forced) → its `byte[]?` return drives the absent/empty/present sentinel (§3.4) |
| Mandatory `SerializationException` wrap of a deserializer throw | Mandatory `SerializationException` wrap of a serializer throw (§3.5) |
| `MockConsumer<K,V>` **ctor-takes-deserializers** = a **deviation** (Java's doesn't) | `MockProducer<K,V>` **ctor-takes-serializers** = **Java-faithful** (Java's does — §7) |
| Zero-copy typed path, per-record budget test | Zero-copy typed path (serializer output is the only per-record alloc), send-path budget test (§3.6) |

### (c) Explicit NOT-adding list (any of these appearing is a Critic finding)

- **Headers** on `ProducerRecord<K,V>` — the ABI's send path has no headers field
  (unchanged from P3/P4).
- **Generic `RecordMetadata`** — `RecordMetadata` stays **non-generic** (no K/V);
  do not parameterize it.
- Transactions, `Metrics`, `clientInstanceId`, partitioner injection — Mode B / later.
- A **bytes-specialized sibling** (a non-generic `ProducerRecord` / `KafkaProducer`
  kept "for convenience") — the whole point of generic-only is to remove it; bytes =
  `<byte[],byte[]>` + `Serdes.ByteArray`.
- Any **new `[DllImport]`**, `SafeHandle`, pump, TCS, or completion callback — the
  send/pump machinery is already shipped (P3/P4); this phase adds **only** a managed
  serialization skin.
- A **per-record callback through the ABI** to serialize (CLAUDE.md §11) — serialize
  in the binding, before the P/Invoke.
- An **async `ISerializer<T>`** / `IAsyncSerializer<T>` — the ecosystem-ckd idiom;
  Java's `Serializer<T>` is sync (M6/P1a decision F). Serialize is sync.

**Critic charge for N=33 (state in the review brief):** flag (1) any public surface
beyond Java `Producer<K,V>` / the (a)+(b) anchor; (2) any divergence from the
consumer M6/P1b conversion precedent without a written rationale; (3) any residual
**shadow non-generic** producer type left after the conversion (DoD §6 — removed,
not shadowed); (4) §A4 send-pin / §A5 error / §11 serialize-placement anti-patterns;
(5) any weakened assertion in a migrated producer test.

---

## 3 · Design content — decisions to work out (each: recommendation + FLAG where the user must confirm)

### 3.1 · Decision #1 — GENERIC-ONLY conversion (the big one; **BREAKING** — FLAG for confirmation)

**Statement.** Make the producer types generic and **remove** the non-generic ones,
mirroring the consumer M6/P1b. Both trios convert:

- **Records:** `ProducerRecord` → `ProducerRecord<TKey,TValue>`. (`RecordMetadata`
  stays non-generic — §3.2.)
- **Async trio:** `IAsyncProducer` → `IAsyncProducer<TKey,TValue>`;
  `AsyncKafkaProducer` → `AsyncKafkaProducer<TKey,TValue>`; `AsyncMockProducer` →
  `AsyncMockProducer<TKey,TValue>`.
- **Sync trio:** `IProducer` → `IProducer<TKey,TValue>`; `KafkaProducer` →
  `KafkaProducer<TKey,TValue>`; `MockProducer` → `MockProducer<TKey,TValue>`.

Bytes users write `<byte[],byte[]>` + `Serdes.ByteArray` (Java-style verbosity
accepted, exactly as the consumer: `new KafkaProducer<byte[],byte[]>(config,
Serdes.ByteArray, Serdes.ByteArray)`).

**Recommendation: ADOPT** (it is the consumer precedent, and Java has a single
generic `Producer<K,V>` with no bytes sibling).

**⚠ FLAG — this is a BREAKING change to the current bytes-only producer public API**
(the async trio shipped M11/P1–P3 and the sync trio shipped M11/P4/P4.1 all take/
return `ReadOnlyMemory<byte>?`). After this phase, the bytes surface is only
reachable as `<byte[],byte[]>`. This is **fine pre-publish** and is **exactly how
the consumer did it** (M6/P1b removed its non-generic types pre-publish), but the
user must explicitly confirm the break.

**No `IProducerCommon` (contrast the consumer's `IConsumerCommon`).** The consumer
factored a **non-generic** `IConsumerCommon` so only `Poll` retyped and the ~8
K/V-free members were shared across the sync+async flavors. The producer has **no
such sharing opportunity**: `Flush`/`Close`/`PartitionsFor` have *different*
signatures on the sync vs async interface (`void Flush()` vs `Task Flush(ct)`;
`IReadOnlyList<PartitionInfo> PartitionsFor(string)` vs `Task<…> PartitionsFor(string,
ct)`), so they cannot share a common base. Therefore each generic interface carries
**all** its members; `Send` uses K/V, the other three are K/V-free but simply live on
the generic interface. This matches M11/P4's already-recorded "**no `IProducerCommon`**;
`IProducer` is flat" decision — the generic conversion does not change it.

### 3.2 · Decision #2 — `RecordMetadata` stays NON-generic

**Statement.** `RecordMetadata` carries no K/V (`Topic`/`Partition`/`Offset`/
`Timestamp`), so it does **not** become generic.

**Recommendation: ADOPT** (Java's `RecordMetadata` is non-generic; the consumer's
`RecordMetadata` analog on the receive side is `ConsumerRecord<K,V>`, but the
produce ack carries no payload). No change to `RecordMetadata` / `RecordMetadataMarshal`.
Not a flag — Java-faithful, no ambiguity.

### 3.3 · Decision #3 — where serialization happens + how the bytes reach `NativeProducer`

**Statement.** Serialization is a **pure managed layer above** the bytes-based
`NativeProducer` (CLAUDE.md §11 — serialize in the binding, **before** the P/Invoke;
no per-record callback through the ABI). The generic client serializes
`TKey`/`TValue` → `byte[]?`, then calls the **already-shipped** `NativeProducer.Send`
(sync) / `SendViaPump` (async). Symmetric to the consumer's typed poll deserializing
*above* the bytes-based `NativeConsumer`.

**The one contained refactor this forces (state it precisely).** `NativeProducer.Send`
/ `SendViaPump` today take the **public bytes `ProducerRecord`**. Since generic-only
**removes** that public bytes type (§3.1), those two methods must take an **internal
bytes-carrier** instead. Recommendation:

- Demote the current bytes-`ProducerRecord` shape to an **`internal readonly struct
  SerializedProducerRecord`** under `Internal/` (fields: `Topic`, `int? Partition`,
  `long? Timestamp`, `ReadOnlyMemory<byte>? Key`, `ReadOnlyMemory<byte>? Value` — the
  *exact* field set `NativeProducer.Send`/`SendViaPump` read today).
- `NativeProducer.Send(SerializedProducerRecord)` / `SendViaPump(SerializedProducerRecord,
  ct)` — the send **logic** (call-scoped `fixed` pin, the P3 pull-pump, the P4
  blocking `get`, the absent/empty/present sentinels, teardown) is **unchanged**;
  only the parameter *type* changes from the removed public bytes record to this
  internal struct. A `readonly struct` (not a class) keeps the carrier **off the heap**
  (no per-send allocation — §3.6).

So "`NativeProducer` stays bytes-based and unchanged" is true of its **logic and its
FFI signatures**; its send-method **parameter type** changes from the removed public
`ProducerRecord` to the internal `SerializedProducerRecord`. This is the send-path
analog of the consumer keeping `NativeConsumer` bytes-only while the typed skin sits
above it. No FFI/P/Invoke change — the native call still receives
topic/partition/timestamp/key-ptr/value-ptr exactly as today.

**Recommendation: ADOPT the internal-struct carrier.** (Alternative considered:
re-parameterize `NativeProducer.Send` to raw bytes fields instead of a struct —
rejected as noisier at the call sites and no leaner; the struct is the current shape
demoted.) **Minor FLAG:** confirm the internal-carrier approach (vs raw-fields) — a
low-stakes internal choice, surfaced for completeness.

### 3.4 · Decision #4 — null / tombstone three-state on the send path (invoke-the-serializer; Java-faithful)

**Statement.** `ProducerRecord<TKey,TValue>` expresses a null key / null value
(tombstone) via nullable-annotated generic fields (`TKey? Key`, `TValue? Value`; ctor
`(topic, TValue? value, TKey? key = default, …)`). On send, the serializer's `byte[]?`
return drives the ABI sentinel that `NativeProducer.Send` already understands:

| Serializer `byte[]?` result | Meaning | ABI sentinel (existing `NativeProducer.Send` logic, §A4) |
|---|---|---|
| **`null`** | tombstone value / no key | **absent** → `IntPtr.Zero` + len `-1` |
| **empty** (`byte[0]`) | present, empty | **present-empty** → non-null stack sentinel byte + len `0` |
| **non-empty** | present | **present** → pointer + length |

**The serializer-invocation-on-null sub-decision (the crux; FLAG lightly).** Two options:

- **(Recommended) Option 1 — Java-faithful: always invoke the serializer, even on a
  null `TKey`/`TValue`**, and let its `byte[]?` return drive the sentinel. This is
  exactly `KafkaProducer.doSend` (`serialize(topic, headers, record.key())` is called
  unconditionally). Java's `Serializer` javadoc requires `null → null` (tombstone), so
  the built-in serdes return `null` for `null` input, mapping to **absent**.
- Option 2 — consumer-symmetric short-circuit: `if (value is null) → absent, skip the
  serializer`. **Rejected**, because the consumer's short-circuit was **forced** (you
  cannot construct a null `ReadOnlySpan<byte>` to hand a deserializer); the producer is
  **not** forced (a serializer *can* be handed a null `T`), so short-circuiting would
  **diverge from Java** (which delegates the null decision to the serializer). The
  asymmetry with the consumer is **intentional and documented**: receive short-circuits
  by necessity, send stays Java-faithful.

**Value-type tombstone guidance (doc note, mirrors consumer P1b §3).** For an
unconstrained `T`, `T?` is the "may be default/null" annotation, **not** `Nullable<T>`.
So `ProducerRecord<string, long>.Value` is a `long` (a `0L` is a present value, **not**
a tombstone). A user who needs a value-type tombstone distinguishable from `default`
uses a reference/nullable value type (e.g. `<string, long?>` with a matching serde) —
the same corner the consumer documented. Docs only, no code path.

**Dependency to verify (Actor action).** The tombstone path relies on the **M6/P1a
built-in serdes returning `null` for `null` input** (Java parity). P1a tested
round-trips; the Actor must **confirm (and add a test if missing)** that
`Serdes.String`/`ByteArray`/etc. return `null` on `null` input, and that
`Serdes.Null` (VoidSerializer) always returns `null`. Flag any that don't as a P1a
gap (do not silently patch behavior beyond Java).

**Recommendation: ADOPT Option 1.** **FLAG** the invoke-on-null choice + the
intentional asymmetry with the consumer's short-circuit for the user to confirm.

### 3.5 · Decision #5 — mandatory `SerializationException` wrap (Java-parity; different *reason* than the consumer)

**Statement.** Any throw from a user `ISerializer<T>.Serialize` is caught and wrapped
in `SerializationException` (`: KafkaException`, shipped M6/P1a) with the inner
exception **+ topic context** (Java `doSend` wraps the serializer `ClassCastException`
in `SerializationException` with the topic/class message).

**Why it is mandatory here differs from the consumer (state it, so the Critic
understands).** For the consumer (P1b §6) the wrap is a **memory-safety necessity** —
the async deserialize runs on the foreign dispatcher thread, so an escaping managed
exception into native is UB. For the **producer**, serialization runs on the
**caller's thread, before any P/Invoke** (both sync and async — the async `Send` also
serializes inline before enqueuing to the pump). So there is **no foreign-thread / UB
concern**; the wrap is mandatory for **Java-contract fidelity** (Java wraps serializer
errors in `SerializationException`), not to prevent an unwind-into-native.

**Surfacing (FLAG lightly).** Because serialize is synchronous, pre-native, on the
caller thread:

- **Sync `Send` (`IProducer<K,V>`):** throw `SerializationException` synchronously
  (matches Java + the sync-consumer precedent).
- **Async `Send` (`IAsyncProducer<K,V>`):** **recommendation — throw synchronously
  too**, before returning the `Task` — Java's `send` throws `SerializationException`
  synchronously, and it is consistent with how the async `Send` already throws its
  `ArgumentNullException` precondition synchronously before returning the `Task`. (The
  alternative — fault the returned `Task`, symmetric with the consumer's async
  *deserialize*-faults-the-Task — is available, but the consumer faulted because its
  deserialize ran on the dispatcher thread; the producer serialize is pre-native on the
  caller thread, so a synchronous throw is both possible and more Java-faithful.) FLAG
  for confirmation.

**Recommendation: ADOPT the wrap; synchronous throw both flavors.**

### 3.6 · Decision #6 — zero-copy / allocation budget

**Statement.** The only per-record allocation on the typed send path is the
serializer's output `byte[]` (the user's choice of type). `Serdes.ByteArray` returns
the **user's array with no extra copy** (identity serialize, M6/P1a), so
`<byte[],byte[]>` is allocation-equivalent to the former bytes surface. The internal
carrier is a `readonly struct` (no heap alloc, §3.3); the existing **call-scoped
`fixed` pin** then pins the serializer output (for `Serdes.ByteArray`, that is the
user's original array — true zero-copy). `RecordMetadata` + its topic string on
completion is the same unavoidable allocation as today (Java's own behavior).

**Required test:** a **send-path allocation-budget test** (mirror the M11/P3 budget
test): a large value adds **no allocation beyond the serializer output**; assert the
`<byte[],byte[]>` + `Serdes.ByteArray` path adds **no** value-sized allocation over
the raw send (equivalence to the former bytes path).

**Recommendation: ADOPT.** Not a flag — it is the DoD §10 obligation.

### 3.7 · Decision #7 — serializer-taking constructors (3-param, decision A)

**Statement.** Real clients take the serializers (Java `KafkaProducer.java:300`):

- `KafkaProducer<TKey,TValue>(IReadOnlyDictionary<string,string> config, ISerializer<TKey>
  keySerializer, ISerializer<TValue> valueSerializer)` — 3-param.
- `AsyncKafkaProducer<TKey,TValue>(config, ISerializer<TKey> keySerializer,
  ISerializer<TValue> valueSerializer)` — same 3-param.

Mocks take the serializers **too** (`MockProducer.java:104-135` — Java's mock takes
`Serializer<K>/Serializer<V>` and serializes into its history), with `autoComplete`
kept optional after the required serializers:

- `MockProducer<TKey,TValue>(ISerializer<TKey> keySerializer, ISerializer<TValue>
  valueSerializer, bool autoComplete = true)`.
- `AsyncMockProducer<TKey,TValue>(ISerializer<TKey> keySerializer, ISerializer<TValue>
  valueSerializer, bool autoComplete = true)`.

The mock's `Send` serializes `TKey`/`TValue` → bytes exactly like the real client, then
forwards to the bytes-based native mock (which stores bytes; `HistoryCount()` reflects
the count). **This is Java-faithful** — unlike the consumer, where mock-takes-
deserializers was a deviation, Java's `MockProducer` **does** take serializers (§7).

**Recommendation: ADOPT.** Not a flag on the ctor shape itself (Java-faithful); the
`(keySer, valueSer, autoComplete=true)` **argument order** mirrors the consumer's
`MockConsumer<K,V>(keyDeser, valueDeser, autoOffsetReset?)` — surfaced for consistency.

### 3.8 · Decision #8 — mock helpers unchanged (`HistoryCount()` is a method per M11/P4.1)

**Statement.** The mock send-control helpers stay **inherent, non-generic, unchanged**
on the concrete mocks (they operate on the native bytes mock, no K/V):
`bool CompleteNext()`, `bool ErrorNext(int code, string? message = null)`,
`int HistoryCount()` (a **method** since M11/P4.1 — Python `history_count()` + FDG
parity), `void Clear()`. **Recommendation: ADOPT.** Not a flag.

### 3.9 · Carry-over note (out of scope, flag so the Critic does not misread it)

The current `ProducerRecord` ctor rejects **negative partition** and **null topic** but
does **not** reject a **negative timestamp** (Java's `ProducerRecord.java:72-74`
*does*). This is a **pre-existing** .NET choice, orthogonal to the generic conversion.
**Recommendation: leave as-is** (preserve current behavior; do not expand scope). The
generic `ProducerRecord<K,V>` ctor keeps the same validation as today (topic-non-null
+ partition-non-negative; timestamp forwarded verbatim). If the user wants Java-exact
timestamp validation, that is a separate one-line follow-up — **FLAG** it as an
optional, out-of-scope-by-default item.

---

## 4 · Public API sketch (namespace `Confluent.Kafka`)

```csharp
namespace Confluent.Kafka;

// Java org.apache.kafka.clients.producer.ProducerRecord<K,V> (clipped: no Headers).
public sealed class ProducerRecord<TKey, TValue>
{
    public string Topic { get; }
    public int? Partition { get; }              // null → let the producer choose (>= 0 when set)
    public long? Timestamp { get; }             // null → the producer stamps it
    public TKey? Key { get; }                   // null (ref) → no key; default(T) three-state (§3.4)
    public TValue? Value { get; }               // null (ref) → tombstone; default(T) three-state (§3.4)
    public ProducerRecord(string topic, TValue? value,
        TKey? key = default, int? partition = null, long? timestamp = null);
    // ctor validates: topic non-null (ArgumentNullException), partition >= 0
    // (ArgumentOutOfRangeException) — Java-faithful; timestamp forwarded verbatim (§3.9).
}

public sealed class RecordMetadata            // NON-generic (§3.2) — unchanged
{
    public string Topic { get; }
    public int Partition { get; }
    public long Offset { get; }
    public long Timestamp { get; }
}

// ---- async trio (was IAsyncProducer / AsyncKafkaProducer / AsyncMockProducer) ----
public interface IAsyncProducer<TKey, TValue> : IAsyncDisposable, IDisposable   // Java Producer<K,V>
{
    Task<RecordMetadata> Send(ProducerRecord<TKey, TValue> record, CancellationToken cancellationToken = default);
    Task Flush(CancellationToken cancellationToken = default);
    Task Close(CancellationToken cancellationToken = default);
    Task<IReadOnlyList<PartitionInfo>> PartitionsFor(string topic, CancellationToken cancellationToken = default);
}

public sealed class AsyncKafkaProducer<TKey, TValue> : IAsyncProducer<TKey, TValue>
{
    public AsyncKafkaProducer(IReadOnlyDictionary<string, string> config,
        ISerializer<TKey> keySerializer, ISerializer<TValue> valueSerializer);   // 3-param (decision A)
}

public sealed class AsyncMockProducer<TKey, TValue> : IAsyncProducer<TKey, TValue>
{
    public AsyncMockProducer(ISerializer<TKey> keySerializer, ISerializer<TValue> valueSerializer,
        bool autoComplete = true);                                               // Java-faithful (§7)
    public bool CompleteNext();
    public bool ErrorNext(int code, string? message = null);
    public int  HistoryCount();                 // METHOD (M11/P4.1)
    public void Clear();
}

// ---- sync trio (was IProducer / KafkaProducer / MockProducer) ----
public interface IProducer<TKey, TValue> : IDisposable   // Java Producer<K,V> (synchronous)
{
    RecordMetadata Send(ProducerRecord<TKey, TValue> record);   // blocks = send(record).get() (M11/P4 #1)
    void Flush();
    IReadOnlyList<PartitionInfo> PartitionsFor(string topic);
    void Close();                               // no Close(TimeSpan) — M11/P4 decision #5
}

public sealed class KafkaProducer<TKey, TValue> : IProducer<TKey, TValue>
{
    public KafkaProducer(IReadOnlyDictionary<string, string> config,
        ISerializer<TKey> keySerializer, ISerializer<TValue> valueSerializer);   // 3-param
}

public sealed class MockProducer<TKey, TValue> : IProducer<TKey, TValue>
{
    public MockProducer(ISerializer<TKey> keySerializer, ISerializer<TValue> valueSerializer,
        bool autoComplete = true);                                               // Java-faithful (§7)
    public bool CompleteNext();
    public bool ErrorNext(int code, string? message = null);
    public int  HistoryCount();
    public void Clear();
}
```

Preconditions unchanged (ffi §A5): null `record`/`topic` → `ArgumentNullException`;
negative partition (at ctor) → `ArgumentOutOfRangeException`; post-`Close`/`Dispose`
op → `ObjectDisposedException`; operational failures → `KafkaException`; serializer
throw → `SerializationException` (§3.5).

---

## 5 · Internal design (`Internal/` + `Internal/Interop/`)

### 5.1 · The internal bytes-carrier

- **`internal readonly struct SerializedProducerRecord`** (new, under `Internal/`) —
  `Topic`, `int? Partition`, `long? Timestamp`, `ReadOnlyMemory<byte>? Key`,
  `ReadOnlyMemory<byte>? Value`. Exactly the field set the current bytes send path
  reads; a `struct` so it never hits the heap per send (§3.6).

### 5.2 · `NativeProducer` — send-method parameter retype only (logic unchanged)

- `NativeProducer.Send(SerializedProducerRecord)` and
  `NativeProducer.SendViaPump(SerializedProducerRecord, CancellationToken)` — param
  type changes from the removed public bytes `ProducerRecord` to the internal struct;
  the **body** (call-scoped `fixed` pin §A4, the P3 pull-pump completion, the P4
  blocking `get`, the absent/empty/present sentinels, all teardown) is **unchanged**.
  `Flush`/`FlushWithCallback`/`PartitionsFor`/`PartitionsForWithCallback`/`Close`/
  `CloseWithCallback`/`MockCompleteNext`/`MockErrorNext`/`MockHistoryCount`/`MockClear`
  are **untouched** (K/V-free).

### 5.3 · The generic clients (thin serialize-then-forward skins)

Each generic client holds the two serde instances + one `NativeProducer` (created by
its own ctor — sibling types over one native producer, the M11/P4 precedent; no client
wraps another). `Send` is the only member that touches K/V:

```
Send(ProducerRecord<TKey,TValue> record):
  if record is null → ArgumentNullException            (precondition, before any work)
  byte[]? kb = keySerializer.Serialize(record.Topic, record.Key);     // §3.4 invoke-on-null
  byte[]? vb = valueSerializer.Serialize(record.Topic, record.Value); // wrap any throw → SerializationException (§3.5)
  var sr = new SerializedProducerRecord(record.Topic, record.Partition, record.Timestamp,
             kb is null ? (ReadOnlyMemory<byte>?)null : kb,            // null → absent; else wrap (no copy)
             vb is null ? (ReadOnlyMemory<byte>?)null : vb);
  return _native.Send(sr);           // sync (IProducer<K,V>)   — blocks, returns RecordMetadata
      // or _native.SendViaPump(sr, ct);   async (IAsyncProducer<K,V>) — returns Task<RecordMetadata>
```

`Flush`/`Close`/`PartitionsFor`/`Dispose`/`DisposeAsync` and the four mock helpers
forward to the **existing** `NativeProducer` members unchanged.

### 5.4 · No new interop, no new scaffolding

**Zero** new `[DllImport]`s, `SafeHandle`s, pumps, TCS, completion callbacks, or marshal
helpers — the entire send/pump/teardown stack is shipped (P3/P4). The only new managed
code is: the generic type declarations, the internal carrier struct, and the
serialize-then-forward `Send` skin. Leaner than P4 at the boundary.

---

## 6 · Test plan

### 6.1 · Migration of the shipped producer test corpus (named work item, mirrors consumer P1b §8)

Generic-only converts **14 shipped producer test files** (8 async `PublicProducer*` +
6 sync `PublicSyncProducer*`) at construction + type-ref level — mechanical, bounded:

- `new MockProducer(autoComplete)` → `new MockProducer<byte[],byte[]>(Serdes.ByteArray,
  Serdes.ByteArray, autoComplete)`; `new AsyncMockProducer(...)` likewise;
  `new KafkaProducer(config)` / `new AsyncKafkaProducer(config)` →
  `<byte[],byte[]>(config, Serdes.ByteArray, Serdes.ByteArray)`.
- `new ProducerRecord(topic, value, …)` → `new ProducerRecord<byte[],byte[]>(topic,
  value, …)`; value/key literals move from `ReadOnlyMemory<byte>?` to `byte[]?` (under
  `Serdes.ByteArray`, `TValue = byte[]`). Assertions unchanged.
- **Budget it, not a surprise.** Construction/type-ref only; **no assertion weakened**
  (the Critic verifies this, as in P1b). Interop marshalling tests that don't construct
  public producers are untouched.

### 6.2 · New typed tests (mirror consumer P1b §9, on the send path)

- **Typed round-trip:** `MockProducer<string,long>` + `AsyncMockProducer<string,long>`
  (String/Int64) — `Send(new ProducerRecord<string,long>(topic, 42L, "k"))` succeeds
  and `HistoryCount()` reflects it; a decoding assert on the serialized bytes where
  observable; plus `<byte[],byte[]>` via `Serdes.ByteArray` (equivalence to the former
  bytes surface). Both sync and async mocks.
- **null / tombstone (§3.4):** `ProducerRecord<string,string>(topic, value: null)` →
  the serializer **IS** invoked (assert via a counting/observing serializer — the
  Java-faithful inverse of the consumer's not-invoked assert) → returns `null` →
  **absent** send; empty `byte[]` → **present-empty**; value-type-tombstone doc case
  (`<string, long?>` or reference type) distinguishes tombstone from `default`.
- **Serialize throws → `SerializationException` (§3.5):** a throwing serializer →
  sync `Send` throws `SerializationException` (assert **inner + topic context +
  message**, DoD §3); async `Send` throws synchronously too (per the recommended
  surfacing) — assert the same wrap, and that nothing enqueues to the pump / no native
  send occurs.
- **Send-path allocation budget (§3.6, DoD §10):** typed send adds only the serializer
  output; `<byte[],byte[]>` + `Serdes.ByteArray` adds no value-sized allocation over
  the raw send; a **mutation-after-`Send`** test proves the core copied during the call
  (carried over from P3/P4, now through the generic layer).
- **Built-in serde null-input (§3.4 dependency):** confirm `Serdes.String`/`ByteArray`/
  etc. return `null` on `null` input and `Serdes.Null` always returns `null` (add if
  missing in P1a).
- **TFM smoke:** `MockProducer<byte[],byte[]>` send round-trip loads + works on net462
  (via netstandard2.0) / net8.0 / net10.0.

### 6.3 · Java typed tests to translate/adapt (DoD §3)

Slice the Java `KafkaProducerTest` / `ProducerRecordTest` / `MockProducerTest` cases
that exercise the **typed serialize path** and the generic `ProducerRecord` ctor
(serializer-null handling, key/value serialize, ctor validation). Translate the
relevant ones; state explicitly (self-review) which Java tests are out of scope and
why (broker/transaction/partitioner cases the ABI does not expose — consistent with the
producer clip).

---

## 7 · Definition of Done (state in the plan)

- **Two-stage build:** `cargo build --features ffi` → `dotnet build`, **0/0 all TFM
  legs** (ns2.0 lib; net462/net8.0/net10.0 tests), and a **no-`confluent_kafka.h`-delta**
  diff as the **Mode-A proof**.
- **`dotnet test` green on the TFM matrix** — the typed suites **+ the migrated
  14-file producer corpus** (run the throwing-serializer + mock-timing suites several
  times for stability).
- **`dotnet format --verify-no-changes` + `<EnforceCodeStyleInBuild>` clean.**
- **ffi-marshalling anti-patterns satisfied:** §A4 call-scoped `fixed` pin over the
  serializer output; §A5 error model (flat `KafkaException` operational vs precondition
  .NET exceptions; `SerializationException` for a serializer throw); **no dead
  `DllImport`** (this phase adds none); no per-record callback through the ABI (§11 —
  serialize before the P/Invoke).
- **Error-message assertions**, especially the **`SerializationException` message**
  (inner + topic context).
- **Send-path allocation-budget test** present and passing (DoD §10 — the send path is
  a hot path).
- **DoD §6 — non-generic producer types REMOVED, not shadowed** (grep-verified, as
  P1b); **§7** — no host-only type beyond the sanctioned internal carrier;
  **§8** — no TODO/FIXME.
- **DoD §11 spirit (producer):** async `Send` stays a `Task`-returning method; sync
  `Send` returns the result directly; **no `block_on`-wrapped sync façade**; **no
  per-send `Task.Run`/thread**; serialize is sync (no async serde).
- **CLAUDE.md §3/§4 doc-sync:** update the producer sketch (generic-only; the typed
  producer un-gated by the closed §A7); the Java→C# idiom-map `Producer<K,V>` row
  ("typed producer still deferred" → "shipped generic-only"); record the invoke-on-null
  send three-state (§3.4), the Java-faithful mock-takes-serializers (§7), and the
  send-path `SerializationException` wrap (§3.5).

---

## 8 · Mode A confirmation + flagged Mode B gaps

**Mode A — CONFIRMED.** This phase is a **pure managed serialization layer**. It:

- adds **no** `src/**` / `src/ffi/**` / header / `cbindgen.toml` change;
- adds **no** new `[DllImport]` (the serialized bytes flow into the already-shipped
  `NativeProducer.Send` / `SendViaPump`, whose FFI is unchanged from P3/P4);
- reuses the shipped `ISerializer<T>` / `Serdes` (M6/P1a) and the shipped send/pump/
  teardown stack (M11/P3/P4).

The only interop-adjacent edit is **re-parameterizing `NativeProducer.Send` /
`SendViaPump`** from the removed public bytes `ProducerRecord` to the internal
`SerializedProducerRecord` struct — a managed refactor with **no P/Invoke signature
change**.

**Flagged Mode-B / later gaps (out of scope, no action):** headers on
`ProducerRecord`/`RecordMetadata`, transactions, metrics, `clientInstanceId`,
partitioner injection, async serde — none exposed at today's ABI; each a future
item, consistent with the producer clip. **No Mode-B dependency blocks P5.** If the
loop surfaces a need not covered by the shipped symbols, **STOP and flag it to main**
as a Rust-core dependency — do not invent an ABI.

---

## 9 · Commit-hygiene reminders (for the later N=33 loop — NOT this session)

- **Per-path `git add`** — stage only the intended `bindings/dotnet/{src,tests,design}`
  files this phase touches.
- **NEVER stage:** the repo-root `.claude/agents/dotnet-{actor,critic}.md` discovery
  copies (untracked workaround, CLAUDE.md §8.4); `COMMENTS.33.md` /
  `COMMENTS.DONE.33.md` at the binding root (working files — `COMMENTS.33.md` is
  gitignored, `COMMENTS.DONE.33.md` is **not**, so never `git add` it; the tracked
  record is the Manager's archived `design/history/M11/P5-producer-generic/` copy);
  `.claude/agent-memory/**`; `target-linux*`; any built `.so`/`.dylib`; `.DS_Store`.
- Commits: **`--no-gpg-sign`**, end each message with
  `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`; `dotnet(M11/P5): …`
  subjects; fixup messages reference the original commit.
- **Native-first build** every cycle; re-verify **no header delta** as the Mode-A proof.
- On close-out, the Manager archives the approved plan + `COMMENTS.DONE.33.md` under
  `design/history/M11/P5-producer-generic/`, updates `STATUS.md`, and resets
  `COMMENTS.33.md`.

---

## Approval checklist (what the user is signing off)

1. **Decision #1 — GENERIC-ONLY conversion (BREAKING).** Convert both producer trios
   + `ProducerRecord` to `<TKey,TValue>` and **remove** the non-generic types; bytes =
   `<byte[],byte[]>` + `Serdes.ByteArray`. Breaking to the current bytes-only producer
   API; fine pre-publish; mirrors the consumer M6/P1b. **← the headline confirmation.**
2. **Decision #2 — `RecordMetadata` stays non-generic.**
3. **Decision #3 — serialize above the bytes `NativeProducer`;** its send-method param
   type becomes the internal `SerializedProducerRecord` struct (logic unchanged).
   (Minor flag: internal-struct carrier vs raw fields.)
4. **Decision #4 — null/tombstone three-state: always invoke the serializer** (Java-
   faithful), its `byte[]?` return driving absent(`-1`)/empty(`0`)/present; intentional
   asymmetry with the consumer's not-invoked short-circuit. **← confirm invoke-on-null.**
5. **Decision #5 — mandatory `SerializationException` wrap** (Java-parity, not a UB
   guard here); recommend **synchronous throw** for both sync and async `Send`.
   **← confirm the async surfacing (throw vs fault-the-Task).**
6. **Decision #6 — zero-copy / send-path allocation budget** (serializer output the
   only per-record alloc; `readonly struct` carrier; `Serdes.ByteArray` = no extra copy).
7. **Decision #7 — 3-param serializer-taking ctors** for real + mock (mock-takes-
   serializers is **Java-faithful**); `(keySer, valueSer, autoComplete=true)` order.
8. **Decision #8 — mock helpers unchanged** (`HistoryCount()` a method, M11/P4.1).
9. **§3.9 — negative-timestamp validation left as-is** (out of scope by default; opt-in
   Java-exact tightening available). **← confirm leave-as-is.**
10. **Mode A** — no `src/**`/ffi/header/cbindgen change, **zero new `[DllImport]`**.
11. **N = 33** — the loop's agent number (next free; N=32 was M11/P4.1).

---

*End of DRAFT PLAN — awaiting user review. No Actor/Critic spawned; nothing committed;
nothing archived.*
