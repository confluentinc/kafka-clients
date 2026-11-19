# M6/P1b — "Typed consumers (generic conversion + zero-copy typed poll)" (.NET binding)

Status: APPROVED (rev. 2, maintainer 2026-08-07). Second of two phases in M6. **Depends on
M6/P1a (serde foundation, `../P1a-serde-foundation/PLAN.md`) being closed.** Do NOT start until
P1a closes.

---

## 0 · Identity

- **Binding:** `.NET` (`bindings/dotnet/`)
- **Milestone / Phase:** **M6 / P1b**.
- **Assigned N (monotonic):** **20**.
- **Mode:** **A** (`.NET-only`) — bytes-only ABI; **no Rust change** (`confluent_kafka.h`
  byte-identical). `NativeConsumer`/FFI/UAF layer untouched; genericness is a thin managed skin.
- **Scope (P1b):** generic record types (`ConsumerRecord<K,V>` / `ConsumerRecords<K,V>`); the
  zero-copy typed poll marshaller (`CopyOut<K,V>`) + `NativeConsumer.PollTyped<K,V>` (sync) + the
  async typed poll callback; the **generic-only conversion** of the six shipped client types +
  interfaces; and the **test migration**.

## 1 · Goal

Restore Java's single generic `Consumer<K,V>` shape (`Consumer.java:39`) in C# on top of the
bytes-only ABI, consuming P1a's serde foundation, preserving the receive-path zero-copy contract
(consumer-threading.md §27/§28, ffi §B4).

## 2 · Decision B — GENERIC-ONLY (convert the shipped family; no bytes-specialized sibling)

Java has a single generic `Consumer<K,V>` with no bytes sibling; bytes users write
`Consumer<byte[],byte[]>`. Align:

- **Convert the shipped consumer family to `<K,V>`:** `IConsumer` / `KafkaConsumer` / `MockConsumer`
  (sync) AND `IAsyncConsumer` / `AsyncKafkaConsumer` / `AsyncMockConsumer` (async);
  `ConsumerRecord` → `ConsumerRecord<K,V>`; `ConsumerRecords` → `ConsumerRecords<K,V>`. **Remove**
  the non-generic types — no non-generic convenience sibling.
- **`IConsumerCommon` stays NON-generic** — its members are all K/V-free; both `IConsumer<K,V> :
  IConsumerCommon, IDisposable` and `IAsyncConsumer<K,V> : IConsumerCommon, IAsyncDisposable,
  IDisposable` inherit them unchanged. **Only `Poll` retypes** (`ConsumerRecords<K,V>` /
  `Task<ConsumerRecords<K,V>>`) and the record types become generic — this bounds the churn.
- **`NativeConsumer` stays bytes-only.** Genericness is a thin managed skin: `PollTyped<K,V>`
  deserializes each record from a span over the native batch *before destroy* (ffi §B4). The old
  bytes `Poll` is subsumed by `<byte[],byte[]>` via `Serdes.ByteArray`.
- **Bytes ergonomics:** accept Java-style verbosity — `new KafkaConsumer<byte[],byte[]>(config,
  Serdes.ByteArray, Serdes.ByteArray)`. No shortcut.
- **Pre-publish → non-breaking now** — the only free moment to remove the bytes-specialized deviation.

### Ctor (Decision A, 3-param bound)
`new KafkaConsumer<string,long>(config, Serdes.String, Serdes.Int64)` and
`new AsyncKafkaConsumer<…>(config, keyDeser, valueDeser)` — 3 params, matching
`KafkaConsumer.java:601`. Other pluggables (listener, commit callback) on Subscribe/CommitAsync/config,
not the ctor.

## 3 · Decision C — null / tombstone → `default(T)` without invoking the deserializer

`ReadOnlySpan<byte>` cannot be null; three native states:

| Native `(ptr,len)` | Meaning | Marshaller behavior |
|---|---|---|
| `len < 0` (or `ptr==Zero`) | **absent** (tombstone value / no key) | → `default(T)`, deserializer **NOT** called |
| `len == 0`, `ptr != Zero` | **present, empty** | → deserialize a **0-length span** |
| `len > 0` | **present** | → deserialize the span |

**Documented DEVIATION** (consumer-threading §28 style): Java calls `deserialize(topic, null)` for a
tombstone and its javadoc recommends null→null (`Deserializer.java:55/64`). We can't pass a null
span, so we return `default(T)` without invoking the deserializer — mirrors Java's dominant "null
bytes → null" for reference types, memory-safe, natural .NET shape. **Docs value-type guidance:**
use `T?` (e.g. `long?`) when a tombstone must be distinguishable from `0`/`default`.

## 4 · Generic record + consumer types

- **`ConsumerRecord<TKey,TValue>`** — Topic/Partition/Offset/Timestamp/TimestampType/**Headers** +
  `TKey Key` / `TValue Value`. Poll-output-only (internal ctor).
- **`ConsumerRecords<TKey,TValue> : IReadOnlyCollection<ConsumerRecord<TKey,TValue>>`**.
- **Generic interfaces** mirror their bytes counterparts with only `Poll` retyped; all non-poll
  members inherited from the non-generic `IConsumerCommon` or restated unchanged.
- **Generic clients** hold the serde instances + a `NativeConsumer`; forward non-poll members to the
  same `NativeConsumer` wrappers, and `Poll` to the typed path (§5).

## 5 · The zero-copy typed poll path (the crux) — sibling over bytes-only `NativeConsumer`

**Load-bearing rule:** typed clients must NOT compose a bytes `Poll` that already copied bytes out.
Deserialize from a span over the native batch *before destroy*. (Verified: bytes copy-out allocates
a `byte[]` per key/value via `Marshal.Copy`, `ConsumerRecordsMarshal.cs:167`; accessors
`ConsumerRecordKey/Value/Topic(record, out int len) → IntPtr` borrow the batch,
`NativeMethods.cs:395–414`; async poll copy-out runs in `ConsumerCallbacks.OnPoll` on the dispatcher
thread then `ConsumerRecordsDestroy` in `finally`, `ConsumerCallbacks.cs:127/145/159`.)

1. **`ConsumerRecordsMarshal.CopyOut<TKey,TValue>(records, keyDeser, valueDeser)`** — per record:
   read scalars + owned topic `string` + owned `Headers`; for key/value read `(ptr,len)` and apply
   §3 (absent→`default(T)` no call; present→`unsafe { new ReadOnlySpan<byte>((void*)ptr, len) }` →
   `deser.Deserialize(topic, span)`), wrapping any throw in `SerializationException` (§6). Build
   `ConsumerRecord<K,V>`; caller destroys the batch. **More efficient than the bytes path** — no
   intermediate key/value `byte[]`; only the user's `T` allocates (DoD §10). The `unsafe`
   span-over-`IntPtr` is contained to `Internal/Interop` (§2); the `ref struct` guarantees no escape.
2. **Sync** — `NativeConsumer.PollTyped<K,V>(TimeSpan, keyDeser, valueDeser)`: sync `Consumer_poll`
   returns the batch on the caller's thread → `CopyOut<K,V>` (deserialize on the caller thread) →
   destroy → `ConsumerRecords<K,V>`.
3. **Async** — a typed poll completion callback (`OnPoll<K,V>` analog) runs `CopyOut<K,V>` **on the
   dispatcher thread** before `ConsumerRecordsDestroy` in `finally`, then completes
   `TaskCompletionSource<ConsumerRecords<K,V>>` (`RunContinuationsAsynchronously`, §B6/§B7). Serdes
   captured in the per-op `GCHandle` context alongside the TCS.
4. **Non-poll members** forward to the same bytes-only `NativeConsumer` wrappers; only `Poll` differs.

**⚠ Async typed deserialize runs on the foreign dispatcher thread.** Zero-copy forces
deserialize-before-destroy (inside the async poll callback), so: (a) managed exceptions must be
caught → `SerializationException` → faulted Task (§6); (b) a heavy user serde stalls the single
dispatcher (Java deserializes on the poll thread too; async here pays it on the dispatcher); (c) the
sync typed consumer deserializes on the caller's thread. The copy-then-deserialize-off-thread
alternative is rejected (loses zero-copy).

**Hot-path (§11):** per-record deserialize sync/span/one-virtual-call, no intermediate `byte[]`, no
per-record callback.

## 6 · Decision E — `SerializationException`, mandatory catch-and-wrap

`SerializationException : KafkaException` is defined in P1a. In P1b the typed-poll marshaller **MUST
catch** a user `Deserialize` throw and wrap it (inner exception + topic/partition/offset). Mandatory
because the async deserialize runs inside the poll callback on the foreign dispatcher thread (ffi
§B6) — a managed exception escaping into native is UB. Surfacing: sync → throw from `Poll`; async →
fault the `Task`.

## 7 · MockConsumer deviation (explicit rationale, consumer-threading §28 style)

Java's `MockConsumer<K,V>` (`MockConsumer.java:60`) bypasses serde (ctor takes only offset-reset;
`addRecord(ConsumerRecord<K,V>)` at `:322` stores a typed record returned as-is). Ours cannot — it's
a thin forwarder over the native bytes-based mock:
- **Ctor:** `new MockConsumer<K,V>(keyDeser, valueDeser, offsetReset?)` — **takes deserializers**
  (Java's doesn't), because its `Poll` decodes native bytes exactly as `KafkaConsumer<K,V>` does.
  `AsyncMockConsumer<K,V>` mirrors this.
- **Add-record:** `AddRecord(topic, partition, offset, byte[]? key, byte[]? value)` — **bytes-in**
  (Java's is typed-in). Kept bytes-in: it tests the deserialize path in isolation (control the wire
  bytes, assert the decoded K/V), whereas typed `AddRecord` would need serializers in the mock
  (asymmetric with the real consumer) and test serialize∘deserialize identity. Also forced by the
  bytes-only core (ABI `MockConsumer_add_record` takes byte key/value).

## 8 · Test migration — a named work item (from Decision B)

Generic-only converts every shipped consumer test at **construction only** (mechanical, corpus-wide):
- `new MockConsumer()` → `new MockConsumer<byte[],byte[]>(Serdes.ByteArray, Serdes.ByteArray)`;
  `new AsyncMockConsumer(...)` likewise; `new KafkaConsumer(config)` / `new AsyncKafkaConsumer(config)`
  → `<byte[],byte[]>(config, Serdes.ByteArray, Serdes.ByteArray)`.
- `ConsumerRecords` → `ConsumerRecords<byte[],byte[]>`; `ConsumerRecord` → `ConsumerRecord<byte[],byte[]>`;
  `.Key`/`.Value` stay `byte[]?` under `Serdes.ByteArray` (identity) → assertions unchanged.
- Files: `PublicConsumer*Tests`, `PublicSyncConsumer*Tests`, `Interop/ConsumerPoll*` /
  `Consumer*OperationTests`, TFM smoke, allocation-budget. Bounded, mechanical churn across the whole
  consumer test corpus — budget it, not a surprise.

## 9 · New tests

- **Typed round-trip** on `MockConsumer<string,long>` + `AsyncMockConsumer<string,long>`
  (String/Int64): `AddRecord(raw bytes)` → typed poll → assert decoded Key/Value; plus
  `<byte[],byte[]>` via `Serdes.ByteArray` (equivalence to the former bytes surface).
- **Null/tombstone (§3):** absent value → `default(TValue)`, deserializer **not** invoked (assert via
  a throwing/counting deserializer); present-empty → deserializer gets a 0-length span; `long?`
  distinguishes tombstone from `0`.
- **Serde throws → `SerializationException` (§6):** sync `Poll` throws (inner + tp/offset); async
  `Poll` faults the Task and does NOT unwind into native (the dispatcher-thread safety test).
- **Thread-of-deserialize:** a deserializer recording `Thread.CurrentThread` — sync → caller thread;
  async → dispatcher thread (documents §5). `TestTimeout`-bounded.
- **Per-op allocation budget** (typed poll adds only `T`, no intermediate `byte[]`) + **TFM smoke**.

## 10 · Doc-sync (DoD §1)

`bindings/dotnet/CLAUDE.md` §3/§4: the generic client sketches + `ConsumerRecord<K,V>`/`ConsumerRecords<K,V>`;
§4 "generic `Consumer<K,V>`" → shipped (consumer; generic-only); producer deferred (§A7). Record the
generic-only conversion (§2), null→`default(T)` (§3), MockConsumer deviation (§7), the mandatory
async-callback `SerializationException` wrap (§6), and the dispatcher-thread deserialize (§5).
STATUS.md M6/P1b entry.

## 11 · DoD gates

- `cargo build --features ffi` — no `confluent_kafka.h` delta (Mode A).
- `dotnet build` — 0/0 all TFM legs.
- `dotnet test` — green (typed suites + the migrated consumer corpus; run the threaded
  async-deserialize test several times).
- `dotnet format` — clean.
- DoD §6 (non-generic types **removed**, not shadowed), §10 (typed-poll budget), §11 (per-record
  deserialize sync/span/one-call/no-intermediate-`byte[]`/no-per-record-callback), §3 (Java-wire
  vectors inherited from P1a; error-message asserts).

## 12 · Risks / Deviations

- **Generic-only conversion churn (§2/§8):** corpus-wide construction-site edits; bounded/mechanical;
  non-breaking (pre-publish); the named work item.
- **Async typed deserialize on the foreign dispatcher thread (§5):** headline; `SerializationException`
  wrapping mandatory; heavy serde stalls the dispatcher. Sync deserializes on the caller thread.
- **null/tombstone → `default(T)` (§3)**, **MockConsumer ctor-takes-deserializers + bytes-in
  AddRecord (§7)** — deliberate, forced deviations with explicit rationale.
- **`unsafe` span-over-`IntPtr`** — contained to `Internal/Interop`; `ref struct` guarantees no escape.
- **Size:** 6 generic types + records + marshaller + conversion + migration — mitigated by the
  non-generic `IConsumerCommon` (only `Poll` retypes) + reuse of shipped `NativeConsumer` non-poll
  wrappers + the P1a foundation.

## 13 · Workflow / handoff (Manager)

`dotnet-actor N=20` (after P1a closes; incremental commits `dotnet(M6/P1b): …`) → `dotnet-critic
N=20` (focus: zero-copy typed poll — span-not-escaping + deserialize-before-destroy, mandatory
`SerializationException` wrap / no-throw-into-native on the async dispatcher path, null→`default(T)`
three-state, no intermediate per-record `byte[]`, the generic conversion leaving no shadow
non-generic types, the migration touching only construction) → fix cycle until `COMMENTS.20.md`
empty + DoD passes → archive `COMMENTS.DONE.20.md` under `design/history/M6/P1b-typed-consumers/`,
update STATUS.md, reset `COMMENTS.20.md`.
