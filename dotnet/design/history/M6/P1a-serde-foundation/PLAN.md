# M6/P1a — "Serde foundation" (.NET binding)

Status: APPROVED (rev. 2, maintainer 2026-08-07). First of two phases in M6 (serde foundation
+ typed consumers). This PLAN covers **P1a only**; P1b (generic records + typed consumers +
conversion + migration) is `../P1b-typed-consumers/PLAN.md` and starts after P1a closes.

---

## 0 · Identity

- **Binding:** `.NET` (`bindings/dotnet/`)
- **Milestone / Phase:** **M6 / P1a** — a new milestone (serde is a new subsystem; clean boundary
  from M5's consumer clients).
- **Assigned N (monotonic — NOT reset by the new milestone):** **19**.
- **Mode:** **A** (`.NET-only`) — the C ABI stays **bytes-only**; (de)serialization is a
  binding-/user-layer concern (Java shape, CLAUDE.md §4). **No Rust change** — `confluent_kafka.h`
  byte-identical (confirm via `cargo build --features ffi` diff). `NativeConsumer` / FFI / the UAF
  layer are untouched.
- **Scope (P1a):** the serde **foundation only** — `ISerializer<T>` / `IDeserializer<T>`, the
  built-in `Serdes`, and `SerializationException`. **No** records, marshaller, or clients (those
  are P1b). Standalone-testable via direct serde round-trips + Java-wire byte-level vectors.
- **Out of scope (whole milestone):** typed **producer** — gated on the OPEN producer completion
  model (ffi §A7); lands later reusing this foundation.

## 1 · Goal

Ship the bidirectional serde foundation (Java `Serializer<T>` / `Deserializer<T>` shape,
`Deserializer.java`) that P1b's typed consumers consume, on top of the bytes-only ABI.

## 2 · Decision A — instance-based serdes (foundation shape)

Serde **instances** (no config-class-name reflection). The real-consumer ctor (P1b) is bounded at
3 params `(config, keyDeserializer, valueDeserializer)`, matching Java's
`KafkaConsumer(Map<String,Object>, Deserializer<K>, Deserializer<V>)` (`KafkaConsumer.java:601`).
P1a defines the interfaces those instances implement.

## 3 · Serde interfaces (Java-shaped — `Deserializer.java`, consumer-threading §27/§28)

```csharp
namespace Confluent.Kafka;

public interface ISerializer<T>
{
    byte[] Serialize(string topic, T data);                       // Java Serializer<T>.serialize
}

public interface IDeserializer<T>
{
    T Deserialize(string topic, ReadOnlySpan<byte> data);         // Java Deserializer<T>.deserialize
}
```

- **`IDeserializer<T>` is sync, span-based, zero-copy** (the §6.4 / §27 lock): a `ref struct`
  `ReadOnlySpan` cannot be stored/boxed/awaited/sent cross-thread, so it provably cannot outlive
  the native batch P1b borrows it from → safe, no copy. Returns an **owned `T`** (the only
  allocation). One virtual call/record is Java-parity (DoD §11).
- **Deserializer signature deviation (explicit rationale, consumer-threading §28 style):** ours is
  `Deserialize(topic, ReadOnlySpan<byte>)` vs Java's `deserialize(String topic, byte[] data)`
  (`Deserializer.java:55`). **Forced by the receive-path zero-copy contract** (ffi §B4): a `byte[]`
  param forces a per-record copy; the span borrows the native slice in place. Deliberate, safe.
- **Both interfaces ship.** Only `IDeserializer<T>` is *consumed* this milestone (P1b consumers);
  `ISerializer<T>` + the built-in serializers ship ready-for-the-typed-producer and are **tested
  directly** (round-trip) so they are not dead code (DoD §7).
- **Decision D — defer the deserializer headers overload.** Ship only `Deserialize(topic, span)`.
  Addable later **non-breakingly** via a C# **default-interface-method** forwarding to the
  header-less form — mirroring Java's `default T deserialize(String, Headers, byte[])`
  (`Deserializer.java:84`).
- **Decision F — sync-only serde (NOTE only, do not over-commit).** No async serde this milestone
  (a `ref struct` span can't cross `await`; serde is CPU-bound). Note: async serde is deferred; a
  Schema-Registry path *may* later need one depending on whether we reuse ckd's SR package — decide
  later. Java's SR serdes ARE sync (`KafkaAvroDeserializer` over the sync `Deserializer<T>`,
  blocking-cached HTTP) and fit our sync span interface; ckd's async `IAsyncDeserializer<T>` is a
  .NET-ecosystem idiom that diverges from the Java shape. Note only.

## 4 · Built-in serdes (`Serdes` static class) — Java wire-format parity

Each implements **both** `ISerializer<T>` and `IDeserializer<T>`. Wire format must match
`org.apache.kafka.common.serialization.*` — verify each against Java (DoD §3 **byte-level vectors**,
not just round-trips):

| Serde | Type | Wire format (Java parity) |
|---|---|---|
| `Serdes.String` | `string` | UTF-8 |
| `Serdes.ByteArray` | `byte[]` | identity — deserialize copies the span to an owned `byte[]`; serialize passes through. (The bytes bridge used by P1b's `<byte[],byte[]>`.) |
| `Serdes.Int32` | `int` | **4 bytes big-endian** (`IntegerSerializer`) |
| `Serdes.Int64` | `long` | **8 bytes big-endian** (`LongSerializer`) |
| `Serdes.Double` | `double` | **8 bytes big-endian IEEE-754** (`DoubleSerializer`) |
| `Serdes.Guid` | `Guid` | ⚠ **`UUID.toString()` → UTF-8** (Java `UUIDSerializer` serializes the *string form*, NOT the 16 raw bytes) — verify + match |
| `Serdes.Null` | (unit / `object?`) | Java `VoidSerializer` — serialize null/empty, deserialize default |

Malformed input on deserialize (e.g. an `Int32` slice ≠ 4 bytes) → **`SerializationException`** (§5),
matching Java.

## 5 · `SerializationException : KafkaException` (Decision E — defined in P1a)

`public class SerializationException : KafkaException` (Java parity; a flat `KafkaException`
subclass per the ffi §A5 error-model convention). In P1a the built-in serdes throw it on malformed
input. In P1b the typed-poll marshaller catches-and-wraps a user serde throw in it (mandatory —
the async path deserializes on the foreign dispatcher thread, ffi §B6; details in the P1b PLAN §6).

## 6 · Tests (P1a — standalone)

- **Serde direct round-trips:** String (incl. non-ASCII), ByteArray, Int32/Int64/Double, Guid, Null
  — serialize→deserialize.
- **Byte-level Java-wire-parity vectors (DoD §3):** assert the exact bytes — big-endian layout for
  Int32/Int64/Double, `Guid` = UUID.toString()→UTF-8, String UTF-8. A wrong endianness passes
  round-trips but is wire-incompatible with Java — the vectors catch it.
- **Malformed input → `SerializationException`:** e.g. a 3-byte `Int32` slice; assert the type and
  message.
- **`SerializationException` is a `KafkaException`** (catchable as the base).

## 7 · Doc-sync (DoD §1)

`bindings/dotnet/CLAUDE.md` §3: add the `ISerializer<T>` / `IDeserializer<T>` + `Serdes` sketches;
§4 "Serializers" row → foundation shipped (bidirectional; consumed by P1b). Record the deserializer
span deviation (§3), the deferred headers overload (§3-D), the sync-only-serde note (§3-F), and
`SerializationException` under the error model. STATUS.md M6/P1a entry.

## 8 · DoD gates

- `cargo build --features ffi` — **no `confluent_kafka.h` delta** (Mode A; Actor diffs before/after).
- `dotnet build` — 0 warnings / 0 errors, all TFM legs (ns2.0/net8.0/net10.0 lib; net462/net8.0/net10.0 tests).
- `dotnet test` — green (serde suites).
- `dotnet format` — clean.
- DoD §7 (`ISerializer<T>` + serializers tested directly, not dead), §3 (byte-level Java-wire
  vectors + error-message asserts).

## 9 · Risks / Deviations

- **Java wire-format parity** for `Int*`/`Double`/`Guid` (big-endian; UUID-as-string) — verify
  against Java; byte-level vectors are the guard (DoD §3).
- **`ISerializer<T>` shipped unconsumed** (typed producer deferred, §A7) — tested directly, not
  dead; consumed later.
- **Deserializer span-vs-`byte[]`** — deliberate zero-copy-forced deviation (§3), rationale recorded.
- **Sync-only serde (F)** — a note, not a commitment.

## 10 · Workflow / handoff (Manager)

`dotnet-actor N=19` implements P1a (incremental commits `dotnet(M6/P1a): …`, `--no-gpg-sign`,
`Co-Authored-By: Claude Opus 4.8`) → `dotnet-critic N=19` reviews vs Java `Serializer`/`Deserializer`
+ wire-format parity, files `COMMENTS.19.md` → fix cycle until empty + DoD passes → archive
`COMMENTS.DONE.19.md` under `design/history/M6/P1a-serde-foundation/`, update STATUS.md, reset
`COMMENTS.19.md`. **P1b (N=20) starts only after P1a closes.**
