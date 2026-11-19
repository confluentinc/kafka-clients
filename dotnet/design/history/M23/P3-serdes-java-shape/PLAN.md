# Plan: the .NET serdes in Java's shape (M23/P3)

Status: **approved 2026-11-11** (Pranav, relayed by the coordinator: "Overall I approve the plan and agree on
all the PM leans."; then "Yes now go ahead and begin the actor-critic loop"). Every decision D1–D27 as the lean;
ledger rows 455–480 Agreed. Manager N=124. **M23/P3**. Branch `prashah_dev_dotnet_serdes_java_shape`, cut from
local master `37d8209cb` (#328). Rule drafts R1–R6 (§7) are approved but not yet applied to `dotnet/CLAUDE.md`:
an agent-relayed approval cannot authorize a `CLAUDE.md` change, so they wait for Pranav's own instruction; until
then §7 is the governing text for this phase's serde work.

Scope: the 20 findings of the review page ".NET Serdes Parity Review" (claude.ai artifact
`2qgHKqMkDgetQtQmZdSoos`, written at `15a613859`). Every line number below was re-read on this branch at
`37d8209cb`. The serde sources are unchanged since `15a613859`; the producer and consumer files moved a little.

## How to read this plan

- One section per finding (§2). Each has the same five parts: **Today** (Java, Python, .NET, cited), **Options**
  (with code: the library side and what a user writes), **Lean** (and what it breaks), **Tests**, **Ledger and
  rules** (proposed text only).
- **Goal: Java's shape.** Python breaks ties only. Each section says which option is Python's way.
- **The package is unpublished**, so a breaking change is allowed now. Each lean still says when it breaks.
- **The analyzers are internal.** They are not in the package (`dotnet/Directory.Build.props:24-26`,
  `PrivateAssets="all"`). Nothing here relies on them to protect a user.
- Path prefixes: `J/` = `kafka/clients/src/main/java/org/apache/kafka/`, `JT/` =
  `kafka/clients/src/test/java/org/apache/kafka/common/serialization/`, `P/` = `python/confluent_kafka/`,
  `PT/` = `python/test/unit/`, `N/` = `dotnet/src/Confluent.Kafka/`, `NT/` = `dotnet/tests/Confluent.Kafka.UnitTests/`,
  `D/` = `dotnet/`. `ser/` inside `J/` is `common/serialization/`.
- Nothing in this plan needs a Rust, C ABI, header or Python change. It is all Mode A (`D/CLAUDE.md` §6.2).

## 1. Summary

| # | Finding | Lean | Breaking? |
|---|---|---|---|
| 1 | No null for number and UUID serdes | Replace: `Serdes.Integer/Long/Double/UUID` become `int?`/`long?`/`double?`/`Guid?` serdes | **Yes** (type arguments) |
| 2 | No headers in serialize / deserialize | Rule 10: `ISerializerWithHeaders<T>` / `IDeserializerWithHeaders<T>` + extension methods, plus a construction-time guard | No (additive); the guard is a new throw |
| 3 | `ISerde<T>` is both directions | Java's pair: `ISerde<T>.Serializer()` / `Deserializer()`, `Serdes.WrapperSerde<T>`, `Serdes.SerdeFrom(ser, deser)` | **Yes** (every `Serdes.X()` call site) |
| 4 | Nullability says "never null" | `Serialize(topic, T? data)`, `T? Deserialize(…)`, `ConsumerRecord.Key()`/`Value()` return `T?` | Compile-time only (CS8767 / CS8602 warnings) |
| 5 | No `configure` | Rule 10: `…WithConfigure<T>` + extension methods (reverses ledger 321) | No (additive) |
| 6 | `close()` is optional `IDisposable` | Keep (ledgers 377, 381, 383 hold); `ISerde<T>` itself becomes `IDisposable` under #3 | No |
| 7 | Missing built-ins | Add Boolean, Float, Short, ByteBuffer and `SerdeFrom<T>()`; defer Bytes and ListSerde | No (additive) |
| 8 | Java's public serde classes | Public, sealed, Java-named classes for every built-in shipped | No (additive) |
| 9 | No string encoding | Java's keys through `Configure` on the String and UUID classes | No (UTF-8 default kept) |
| 10 | UUID parsing differs | Port Java's `UUID.fromString` (as Python did) | Behaviour only |
| 11 | Void deserializer accepts data | Throw Java's `IllegalArgumentException` | Behaviour only |
| 12 | Double size message | Java's consumer-path text "…DoubleDeserializer…" | Text only |
| 13 | One shared instance | A new instance per call, as Java | No (identity only) |
| 14 | Serdes by constructor only | Keep (ledger 365 holds); a config route can be added later without a break | No change |
| 15 | Serializers before the metadata wait | Keep, record it (same as Python) | No change |
| 16 | Cast message text | Java's "specified in key.serializer" | Text only |
| 17 | No `ClusterResourceListener` / `Monitorable` | Out of scope, record (Python lacks them too) | No change |
| 18 | MockConsumer takes deserializers and bytes | Keep in this phase; Java's typed mock in its own phase | No change now |
| 19 | MockProducer requires serializers | Keep (ledger 330 holds) | No change |
| 20 | Java's serde tests not translated | Translate `SerializationTest` (14 of 36 fully, 2 in part); List tests stay out with #7 | Tests only |

## 2. The findings

### #1 A null key or value cannot be expressed with the number and UUID serdes

**Today.**
- Java: every built-in is a serde "for nullable" values: `Serdes.Long()` is documented "A serde for nullable
  `Long` type" (`J/ser/Serdes.java:226-231`). `LongSerializer` returns `null` for `null`
  (`J/ser/LongSerializer.java:24-26`); `LongDeserializer` returns `null` for `null` (`J/ser/LongDeserializer.java:28-30`).
  The consumer never calls a deserializer for a null field (`J/clients/consumer/internals/CompletedFetch.java:323`, `:329`),
  so `record.value()` is `null` for a tombstone.
- Python: `int_serializer()` returns `None` for `None` (`P/common/serialization/_int_serializer.py:47-48`), and
  `record.value()` is typed `V_co | None` (`P/consumer/consumer_record.py:203`).
- .NET: `Serdes.Integer()`, `Long()`, `Double()`, `UUID()` are `ISerde<int>`, `ISerde<long>`, `ISerde<double>`,
  `ISerde<Guid>` (`N/Common/Serialization/Serdes.cs:43`, `:46`, `:49`, `:55`). `ProducerRecord`'s key defaults to
  `default` (`N/Producer/ProducerRecord.cs:78`), which is `0` for `long`, so a keyless `ProducerRecord<long, …>` is sent
  with key `0` (`N/Internal/Serialization/Int64Serde.cs:27-38`). The consumer stores `default!` for an absent field
  (`N/Internal/Interop/ConsumerRecordsMarshal.cs:356`, `:371`), so a tombstone reads `Value() == 0`. The record's own
  remarks already tell the user to use `long?` "with a matching serde" (`N/Producer/ProducerRecord.cs:28-31`), but no
  built-in matches.

**Options.**

*A. Replace (Java's types, Python's way).* The four become nullable serdes. New built-ins (#7) follow the same rule:
a Java boxed type (`Long`, `Integer`, `Double`, `UUID`, `Boolean`, `Float`, `Short`) is a C# nullable value type.

```csharp
// library (shown in today's shape; with #3 the same types appear on Serializer()/Deserializer())
public static ISerde<int?>    Integer();
public static ISerde<long?>   Long();
public static ISerde<double?> Double();
public static ISerde<Guid?>   UUID();

public sealed class LongSerializer : ISerializer<long?>
{
    public ReadOnlyMemory<byte>? Serialize(string topic, long? data) =>
        data is long value ? BigEndian(value) : null;          // Java: if (data == null) return null;
}
```

```java
// Java user
Producer<Long, String> p = new KafkaProducer<>(props, new LongSerializer(), new StringSerializer());
p.send(new ProducerRecord<>("orders", "v"));                   // no key
if (record.value() == null) { /* tombstone */ }
```

```csharp
// .NET user, option A
var p = new KafkaProducer<long?, string>(cfg, Serdes.Long().Serializer(), Serdes.String().Serializer());
p.Send(new ProducerRecord<long?, string>("orders", "v"));      // key == null: no key
if (record.Value() is null) { /* tombstone */ }

new KafkaConsumer<string, long>(cfg, Serdes.String().Deserializer(), Serdes.Long().Deserializer());
// error CS1503: cannot convert IDeserializer<long?> to IDeserializer<long>   (a clear migration signal)
```

*B. Add beside (non-Java names).* Keep the four; add `Serdes.NullableLong()` and so on. Not breaking, but two ways,
and the names are not Java's.

```csharp
public static ISerde<long>  Long();          // unchanged: absent = 0
public static ISerde<long?> NullableLong();  // no Java counterpart
```

*C. Keep, document.* The silent `0` stays. Java's `allSerdesShouldSupportNull` stays untranslatable.

*D. Add-on to any option: refuse an absent field for a non-nullable value type.* The consumer would throw
`RecordDeserializationException` when the field is absent and `TValue` is, say, `long`. Neither Java nor Python
does this.

**Lean: A.** Java and Python agree: null in, null out. Breaks every `<…, long>` / `<…, int>` / `<…, double>` /
`<…, Guid>` client that uses a built-in (20 + 10 + 8 + 6 uses in the repo's own tests and tools). Not D: a user's own
`IDeserializer<long>` still gets `default(long)` for an absent field (decision C, `D/CLAUDE.md:434`), documented.
Python's way: A.

**Tests.** Java's `allSerdesShouldSupportNull` (`JT/SerializationTest.java:93`), serializer half for every built-in
(the deserializer half cannot pass a null span; covered by the record path below). New .NET tests: a keyless
`ProducerRecord<long?, …>` sends no key (mock producer: the record's serialized key is null); a consumed tombstone on a
`<string, long?>` consumer reads `Value() == null`; existing `SerdesTests.Long_*` and the like move to `long?`.

**Ledger and rules.** Row 455 (§6). Rule draft R1 (the `Serdes` sketch) and R4 (an idiom-map row).

---

### #2 Serializers and deserializers never see the record headers

**Today.**
- Java: `serialize(String, Headers, T)` is a default method forwarding to the short form
  (`J/ser/Serializer.java:84-86`); the deserializer has two, for `byte[]` and `ByteBuffer`
  (`J/ser/Deserializer.java:86-88`, `:115-117`). The producer calls the headers form with the record's own headers
  (`J/clients/producer/KafkaProducer.java:1080`, `:1088`), and so does `MockProducer`
  (`J/clients/producer/MockProducer.java:614-615`). The consumer builds the record's headers first, then calls the
  `ByteBuffer` form with them, and the same object lands on the record (`CompletedFetch.java:319`, `:323`, `:329`).
- Python: one callable shape, headers always passed, typed `headers: Headers | None`
  (`P/common/serialization/serializer.py:47-48`, `deserializer.py:47-48`). The producer passes the record's own
  headers and seals them after (`P/producer/_base.py:536-543`); the consumer passes them (`P/consumer/_poll.py:173`, `:179`).
- .NET: `Serialize(topic, data)` and `Deserialize(topic, span)` only (`N/Common/Serialization/ISerializer.cs:47`,
  `IDeserializer.cs:38`). The pieces are already there:
  - every `ProducerRecord` allocates its `Headers` (`N/Producer/ProducerRecord.cs:107`), one object when empty
    (`N/Common/Header/Headers.cs:30-31`);
  - `Headers` has `Add` and `Remove` (`Headers.cs:110`, `:131`, `:140`);
  - the producer seals after the serializers (`N/Producer/KafkaProducer.cs:128`, `:136`;
    `AsyncKafkaProducer.cs:158`, `:168`);
  - the consumer builds the headers before it deserializes (`N/Internal/Interop/ConsumerRecordsMarshal.cs:354`, then
    `:356-385`), and the mocks share that code (`N/Internal/NativeConsumer.cs:509`).
- Status: deferred (ledger 153, 404). Rule 10 (`D/CLAUDE.md:861-872`) gives the shape but leaves the name of an
  overloaded default's interface open (`:871-872`).

**Options.**

*A. Rule 10 (Java's shape).* The main interface keeps Java's abstract method. The default is an extension method.
A small extra interface carries the user's version. The client tests the type once, at construction.

```csharp
// library
public interface ISerializer<T>
{
    ReadOnlyMemory<byte>? Serialize(string topic, T? data);                       // Java's abstract method
}

public interface ISerializerWithHeaders<T> : ISerializer<T>
{
    ReadOnlyMemory<byte>? Serialize(string topic, Headers headers, T? data);      // Java's default, overridable
}

public static class SerializerExtensions
{
    public static ReadOnlyMemory<byte>? Serialize<T>(
        this ISerializer<T> serializer, string topic, Headers headers, T? data) =>
        serializer is ISerializerWithHeaders<T> withHeaders
            ? withHeaders.Serialize(topic, headers, data)
            : serializer.Serialize(topic, data);                                  // Java's default body
}

public interface IDeserializerWithHeaders<T> : IDeserializer<T>
{
    T? Deserialize(string topic, Headers headers, ReadOnlySpan<byte> data);       // Java's byte[] and ByteBuffer forms
}
// + DeserializerExtensions.Deserialize(this IDeserializer<T>, string, Headers, ReadOnlySpan<byte>)

// inside KafkaProducer (no per-record cost beyond one null check)
_keyWithHeaders = keySerializer as ISerializerWithHeaders<TKey>;                 // once, in the constructor
var key = _keyWithHeaders is { } h
    ? h.Serialize(topic, record.Headers(), record.Key())
    : _keySerializer.Serialize(topic, record.Key());
```

```java
// Java user
class OrderSerializer implements Serializer<Order> {
    public byte[] serialize(String topic, Order o) { return encode(o); }
    @Override public byte[] serialize(String topic, Headers headers, Order o) {
        headers.add("schema-id", SCHEMA_ID);
        return encode(o);
    }
}
```

```csharp
// .NET user, option A
sealed class OrderSerializer : ISerializerWithHeaders<Order>
{
    public ReadOnlyMemory<byte>? Serialize(string topic, Order? o) => Encode(o);
    public ReadOnlyMemory<byte>? Serialize(string topic, Headers headers, Order? o)
    {
        headers.Add("schema-id", SchemaId);
        return Encode(o);
    }
}
```

The known pitfall of A: a user writes the headers method on a plain `ISerializer<T>`. It compiles, and the producer
never calls it.

```csharp
sealed class OrderSerializer : ISerializer<Order>                  // forgot ...WithHeaders
{
    public ReadOnlyMemory<byte>? Serialize(string topic, Order? o) => Encode(o);
    public ReadOnlyMemory<byte>? Serialize(string topic, Headers headers, Order? o) { … }   // never called
}
```

Mitigations for users (the analyzer cannot be one, it does not ship):

- *M1. A construction-time guard (lean).* Each client that takes the serde checks it once. If the object has a public
  `Serialize(string, Headers, T)` but does not implement `ISerializerWithHeaders<T>`, the constructor throws.

  ```csharp
  // library, internal
  if (serializer is not ISerializerWithHeaders<T>
      && serializer.GetType().GetMethod("Serialize", new[] { typeof(string), typeof(Headers), typeof(T) }) is not null)
  {
      throw new IllegalArgumentException(
          $"{serializer.GetType().FullName} declares Serialize(string, Headers, {typeof(T).Name}) but does not "
          + $"implement ISerializerWithHeaders<{typeof(T).Name}>, so the producer would never call it.");
  }
  ```

  It only reads metadata; it never calls the method by reflection (the Confluence page's AOT objection is about
  calling). Under trimming or Native AOT an uncalled method can be removed, so the guard can miss there: it is best
  effort, and the docs say so. It needs one `[UnconditionalSuppressMessage]` for the trim warning (a polyfill on
  netstandard2.0). The library already uses reflection once (`N/Internal/JavaText.cs:206`). The same guard catches a
  public `Close()` on a serde that is not `IDisposable` (a Java port's `close()`), on the four real clients (D6).
- *M2. Same check, but a `Trace.TraceWarning`.* Users rarely see `Trace` output.
- *M3. Docs only.* Accept the risk, in the `ISerializer<T>` and extension-method docs.
- *M4. Remove the pitfall by shape: option B.*

*B. Headers always (Python's way).* One method, with headers. Java's short form becomes the extension.

```csharp
public interface ISerializer<T>
{
    ReadOnlyMemory<byte>? Serialize(string topic, Headers headers, T? data);
}
public static class SerializerExtensions
{
    public static ReadOnlyMemory<byte>? Serialize<T>(this ISerializer<T> s, string topic, T? data) =>
        s.Serialize(topic, new Headers(), data);
}
// every user serializer takes a Headers parameter, used or not
```

No pitfall. But every implementer breaks, and the method users implement is no longer Java's abstract one.

*C. Both methods on the main interface.* Every implementer writes both. A later Java default breaks them again. This
is the shape rule 10 replaced.

*D. Keep deferred.*

**Naming the extra interface of an overloaded default** (rule 10 leaves it open):

- *N1 (lean): `I<Interface>With<Parameter>`, the parameter the default overload adds:* `ISerializerWithHeaders<T>`,
  `IDeserializerWithHeaders<T>`. Each overload gets its own interface, so a later overload (a keepable
  `ReadOnlyMemory<byte>` form, if `D/CLAUDE.md` §6.4's keep-alive ever lands) is a new interface, not a break. Where two
  overloads add the same parameter name, the parameter's type is added (root `CLAUDE.md` §2's collision rule).
- *N2: `I<Interface>With<Method>`, rule 10 read literally:* `ISerializerWithSerialize<T>`. The analyzer derives this
  today (`D/analyzers/Confluent.Kafka.Analyzers/JavaNameAnalyzer.cs:334-337`, `:1190-1193`). Its `CheckReplacement`
  expects every default overload of that name in the one interface (`:443-476`), so a later overload would break it.
- *N3: `I<Interface>With<Method><Parameter>`:* `ISerializerWithSerializeHeaders<T>`. Unambiguous, long.

**One deserializer form.** Java's two headers overloads (`byte[]`, `ByteBuffer`) become one, taking
`ReadOnlySpan<byte>`, which stands for both (`D/CLAUDE.md:693-697`). A keepable form waits for §6.4.

**Lean: A + N1 + M1.** Additive; the guard is a new throw for a class that has a stray headers method. Re-evaluated
ruling: ledgers 153 and 404 deferred the headers form because (1) default interface methods fail on netstandard2.0
(CS8701), (2) it needed its own design, (3) an empty `Headers` per record would be a hot-path allocation. (1) and (2) are
answered by rule 10; (3) no longer applies, since every record already carries its `Headers`. The deferral no longer
holds. Python's way: B.

**Tests.** Java: `allSerdesShouldRoundtripInput`'s headers half (`JT/SerializationTest.java:81`, `:84`). New: an
`ISerializerWithHeaders` that adds a header, on all four producers (the header is on the sent record; on the real
producers it is sealed after); one on the mocks (Java's mock passes `record.headers()`, `MockProducer.java:614`); an
`IDeserializerWithHeaders` that reads a header on all four consumers and sees the same object that ends on the record and
on a `RecordDeserializationException`; a plain `ISerializer` still gets the short call; the guard's throw and message, for
the serializer, the deserializer and `Close()`; the allocation-budget tests unchanged (`D/CLAUDE.md` §7.4).

**Ledger and rules.** Rows 456 (shape), 457 (naming), 458 (one deserializer form), 459 (guard). Rule drafts R2, R3.

---

### #3 `ISerde<T>` is one object for both directions

**Today.**
- Java: `Serde<T>` hands out two objects, `serializer()` and `deserializer()`, and extends `Closeable`
  (`J/ser/Serde.java:33`, `:55-57`). `Serdes.WrapperSerde<T>` holds a pair, configures and closes both
  (`J/ser/Serdes.java:33-63`). `serdeFrom(ser, deser)` builds one, with Java's null messages (`:215-224`). Every
  built-in is a nested `final` class extending `WrapperSerde` (`:65-135`).
- Python: no serde type; factories return separate serializers and deserializers (`P/common/serialization/_factories.py:64-153`).
- .NET: `ISerde<T> : ISerializer<T>, IDeserializer<T>` (`N/Common/Serialization/ISerde.cs:23`). No accessors, no
  `WrapperSerde`, no `SerdeFrom`. A serde shared by a producer and a consumer is disposed by whichever closes first (the
  disposal rule, `D/CLAUDE.md:898`). Recorded at `D/CLAUDE.md:713-717`.

**Options.**

*A. Java's pair (lean).*

```csharp
// library
public interface ISerde<T> : IDisposable                    // Java: Serde<T> extends Closeable (rule 7)
{
    ISerializer<T> Serializer();
    IDeserializer<T> Deserializer();
}

public static class Serdes
{
    public class WrapperSerde<T> : ISerdeWithConfigure<T>    // ISerde<T> plus #5's Configure
    {
        private readonly ISerializer<T> _serializer;
        private readonly IDeserializer<T> _deserializer;

        public WrapperSerde(ISerializer<T> serializer, IDeserializer<T> deserializer)
        {
            _serializer = serializer;                       // Java's constructor does not check
            _deserializer = deserializer;
        }

        public ISerializer<T> Serializer() => _serializer;
        public IDeserializer<T> Deserializer() => _deserializer;

        public virtual void Configure(IReadOnlyDictionary<string, object?> configs, bool isKey)
        {
            _serializer.Configure(configs, isKey);          // the #5 extension methods
            _deserializer.Configure(configs, isKey);
        }

        public virtual void Dispose()                      // Java's close(): serializer, then deserializer
        {
            (_serializer as IDisposable)?.Dispose();
            (_deserializer as IDisposable)?.Dispose();
        }
    }

    public sealed class StringSerde : WrapperSerde<string>
    {
        public StringSerde() : base(new StringSerializer(), new StringDeserializer()) { }
    }

    public static ISerde<string> String() => new StringSerde();          // a new one per call (#13)

    public static ISerde<T> SerdeFrom<T>(ISerializer<T> serializer, IDeserializer<T> deserializer)
    {
        if (serializer is null) throw new IllegalArgumentException("serializer must not be null");
        if (deserializer is null) throw new IllegalArgumentException("deserializer must not be null");
        return new WrapperSerde<T>(serializer, deserializer);
    }
}
```

```java
// Java user
Serde<Order> orders = Serdes.serdeFrom(new OrderSerializer(), new OrderDeserializer());
producer = new KafkaProducer<>(props, Serdes.String().serializer(), orders.serializer());
consumer = new KafkaConsumer<>(props, Serdes.String().deserializer(), orders.deserializer());
producer.close();                                   // closes OrderSerializer only
```

```csharp
// .NET user, option A
ISerde<Order> orders = Serdes.SerdeFrom(new OrderSerializer(), new OrderDeserializer());
producer = new KafkaProducer<string, Order>(cfg, Serdes.String().Serializer(), orders.Serializer());
consumer = new KafkaConsumer<string, Order>(cfg, Serdes.String().Deserializer(), orders.Deserializer());
producer.Close();                                   // disposes OrderSerializer only, if it is IDisposable

// Java's common subclass pattern also ports:
sealed class OrderSerde : Serdes.WrapperSerde<Order>
{
    public OrderSerde() : base(new OrderSerializer(), new OrderDeserializer()) { }
}
```

*B. Keep "both", add Java's accessors.* `ISerde<T> : ISerializer<T>, IDeserializer<T>` with `Serializer()` returning
`this`. Code ports, but the disposal hazard stays, and so does a configure problem: one `Configure(configs, isKey)`
body would serve both roles and cannot tell `key.serializer.encoding` from `key.deserializer.encoding`.

*C. Keep (recorded deviation).*

**Sub-decision: is `ISerde<T>` itself `IDisposable`?** Lean yes: Java's `Serde extends Closeable`, its `close()`
closes both halves (`Serdes.java:48-52`), and `using var serde = Serdes.SerdeFrom(…)` then works. Implementers are
rare (Java users subclass `WrapperSerde` or call `serdeFrom`). The two halves stay optionally `IDisposable` (#6). The
other choice: only `WrapperSerde` is `IDisposable`.

**Lean: A, with `ISerde<T> : IDisposable`.** Breaks every call site that passes a `Serdes.X()` to a client:
`.Serializer()` / `.Deserializer()` must be added. In the repo that is about 1,470 uses (1,318 of
`Serdes.ByteArray()` in 128 files, 100 of `String()`, 52 others) across the unit, integration and gRPC tests, the gRPC
server, soak, perf and the package smoke tool: mechanical. A user class implementing `ISerde<T>` must be rewritten.
Re-evaluated ruling (recorded, `D/CLAUDE.md:713-717`): no longer holds. It was a convenience; it costs the disposal
hazard, the configure-role problem and Java's shape. `ListSerde` is the one nested class that cannot nest: its name
clashes with the `ListSerde(…)` method (CS0102, checked in a scratch compile), so if it is ever translated it sits
beside `Serdes` (rule 1, `D/CLAUDE.md:836-838`). Python's way: closest to A (separate objects).

**Tests.** Java: `testSerdeFromNotNull` (`JT/SerializationTest.java:112`) with Java's message. New: `SerdeFrom`'s
deserializer-null message; `WrapperSerde.Dispose` disposes both halves in order; a producer and a consumer built from
one serde each dispose only their half; `Serdes.X().Serializer()` returns the same object on each call of that serde.

**Ledger and rules.** Rows 460 (pair shape), 461 (`ISerde<T> : IDisposable`). Rule drafts R1, R2.

---

### #4 Nullability annotations say "never null" where null happens

**Today.**
- Java: no annotations; the Javadoc says "typed data; may be null" and "may be null" for the result
  (`J/ser/Serializer.java:59-62`, `Deserializer.java:61-64`).
- Python: `data: T_contra | None` and `-> T_co | None` (`serializer.py:47-48`, `deserializer.py:47-48`);
  `key() -> K_co | None` (`P/consumer/consumer_record.py:199`, `:203`).
- .NET: nullable reference types are on (`D/Directory.Build.props:11`). `Serialize(string topic, T data)` and
  `T Deserialize(…)` (`ISerializer.cs:47`, `IDeserializer.cs:38`), and the producer passes null anyway with `data!`
  (`N/Internal/SerializedProducerRecord.cs:68`, `:71`, `:87`). `ConsumerRecord.Key()` / `Value()` return `TKey` /
  `TValue` (`N/Consumer/ConsumerRecord.cs:115`, `:122`) although they are `default` for an absent field.
  `ProducerRecord` already returns `TKey?` / `TValue?` (`N/Producer/ProducerRecord.cs:130`, `:134`). Some repo tests
  already work around it with `IDeserializer<string?>` (`NT/RecordDeserializationTests.cs:190`).

**Options.**

*A. `T?` (lean, Python's typing).*

```csharp
public interface ISerializer<T>   { ReadOnlyMemory<byte>? Serialize(string topic, T? data); }
public interface IDeserializer<T> { T? Deserialize(string topic, ReadOnlySpan<byte> data); }
// ConsumerRecord<TKey, TValue>
public TKey? Key();
public TValue? Value();
```

```csharp
// .NET user
sealed class Utf8 : ISerializer<string>
{
    public ReadOnlyMemory<byte>? Serialize(string topic, string data) => …;
    // warning CS8767: nullability of 'data' doesn't match the interface; write "string? data"
}
int len = record.Value().Length;   // warning CS8602: dereference of a possibly null reference
```

For a value type `T`, `T?` on an unconstrained generic is still `T`, so A does not fix #1 alone; with #1's nullable
built-ins the two fit.

*B. Attributes (`[AllowNull] T data`, `[return: MaybeNull] T`).* Same effect for callers, needs polyfill attributes
on netstandard2.0, noisier signatures.

*C. Keep; the summary text says "may be null".*

**Lean: A.** Compile-time only: a user with nullable enabled gets warnings until `T? data`; the repo's own
implementers (about 20 classes in the tests and soak) must update (the repo builds with `TreatWarningsAsErrors`, `D/Directory.Build.props:17`). Python's
way: A.

**Tests.** None can observe annotations at run time. The build is the test: every repo implementer compiles warning-free
after the change.

**Ledger and rules.** Row 462. Rule draft R1 (the sketch) and R5 (the consumer sketch line `D/CLAUDE.md:434`).

---

### #5 No `configure(configs, isKey)`

**Today.**
- Java: a default no-op on `Serializer`, `Deserializer` and `Serde` (`Serializer.java:48`, `Deserializer.java:50`,
  `Serde.java:41`). The producer calls it only on a serializer it built from config
  (`J/clients/producer/KafkaProducer.java:398-403`); a given instance is never configured. Users call it by hand, for
  example to set an encoding (`JT/SerializationTest.java:409-421`). The String and UUID classes implement it
  (`J/ser/StringSerializer.java:36-50`, `UUIDSerializer.java:35-43`, and the deserializers).
- Python: duck-typed; called on the config route only (`P/common/serialization/_supply.py:69-78`). The String and UUID
  serdes implement it (`_string_serializer.py:37`, `_uuid_deserializer.py:47-53`).
- .NET: no member. Ruled: no public form (ledger 321), then no config route at all (ledger 365).

**Options.**

*A. Rule 10 (lean).*

```csharp
// library
public interface ISerializerWithConfigure<T> : ISerializer<T>
{
    void Configure(IReadOnlyDictionary<string, object?> configs, bool isKey);
}
public static partial class SerializerExtensions
{
    public static void Configure<T>(
        this ISerializer<T> serializer, IReadOnlyDictionary<string, object?> configs, bool isKey)
    {
        if (serializer is ISerializerWithConfigure<T> configurable) configurable.Configure(configs, isKey);
        // otherwise Java's default: nothing
    }
}
// + IDeserializerWithConfigure<T> / DeserializerExtensions, ISerdeWithConfigure<T> / SerdeExtensions
```

```java
// Java user
Serializer<String> s = new StringSerializer();
s.configure(Map.of("value.serializer.encoding", "UTF-16"), false);
```

```csharp
// .NET user, option A
ISerializer<string> s = new StringSerializer();
s.Configure(new Dictionary<string, object?> { ["value.serializer.encoding"] = "UTF-16" }, isKey: false);
```

The client never calls it, as Java never does for a given instance. The pitfall of #2 applies (a `Configure` written on
a plain `ISerializer<T>`, called through the interface, runs the no-op), but the client does not call it, so the guard
(M1) does not cover it; the docs say so.

*B. No public form (ledger 321 stays).* Then #9 has no Java-shaped home.

*C. Python's duck typing.* Not possible in C# without reflection.

**Lean: A.** Additive. Needs #13 (a configured built-in must not be shared between clients) and #3 (a pair, so the
serializer and the deserializer each read their own keys). Re-evaluated ruling: ledger 321 picked "no public form"
because a config route (row 320) would build the built-ins and the producer would apply the encoding keys itself
(row 323). Ledger 365 removed that route, so 321's premise is gone and nothing now sets an encoding: it no longer
holds. Python's way: A (its built-ins have `configure`).

**Tests.** Java: `stringSerdeShouldSupportDifferentEncodings`, `stringSerdeConfigureThrowsOnUnknownEncoding`
(`JT/SerializationTest.java:119`, `:136`), with #9. New: `WrapperSerde.Configure` configures both halves; a plain
`ISerializer` is a no-op; the key and value keys and the `serializer.encoding` fallback.

**Ledger and rules.** Row 463. Rule drafts R2, R3.

---

### #6 `close()` is an optional `IDisposable`

**Today.**
- Java: `Serializer` and `Deserializer` extend `Closeable` with a default no-op `close()` (`Serializer.java:38`, `:94`;
  `Deserializer.java:40`, `:125`). The producer closes both at `close()`, then rethrows the first failure as
  `KafkaException("Failed to close kafka producer")` (`J/clients/producer/KafkaProducer.java:1531-1532`, `:1540`).
- Python: `close()` if defined; a failure is logged, never raised (`_supply.py:81`).
- .NET: the four real clients dispose a given serde that is `IDisposable`, key then value; a failure is traced, never
  rethrown; `IAsyncDisposable`-only serdes are not disposed; the mocks never dispose (`N/Internal/SerdeDisposal.cs`;
  ledgers 322, 377, 381, 383; `D/CLAUDE.md:898`).

**Options.**

*A. Keep (lean).* `IDisposable` is the "extra interface" for Java's `close()`: rule 7 maps `Closeable` to `Dispose`
(`D/CLAUDE.md:847-851`), and a user adds it beside the serde interface.

```csharp
sealed class OrderDeserializer : IDeserializerWithConfigure<Order>, IDisposable
{
    public Order? Deserialize(string topic, ReadOnlySpan<byte> data) => …;
    public void Configure(IReadOnlyDictionary<string, object?> configs, bool isKey) => …;
    public void Dispose() => …;                          // Java's close()
}
```

*B. `ISerializer<T> : IDisposable` (Java's `extends Closeable`, literally).* Every implementer writes a `Dispose`,
where Java gives a no-op.

*C. Rule 10 literally: `IDeserializerWithClose<T> { void Close(); }`.* This is the Confluence page's example 5. It
conflicts with rule 7, with ledgers 377 and 383, and with the page's own last bullet (".NET's own interfaces
(`IDisposable`) … stay as they are"). Reject; see §3.

*Errors on dispose:* A' keep Python's (ledger 381, lean); B' Java's: `Close()` rethrows
`KafkaException("Failed to close kafka producer", first)` after disposing both, while `Dispose()` still swallows (the
.NET guideline that `Dispose` should not throw).

**Lean: A and A'.** No change. Re-evaluated rulings: 377 (dispose given serdes), 381 (trace, never rethrow) and 383
(`IDisposable` only) still hold: Java's `close()` is a no-op default that rule 7 turns into an optional `IDisposable`,
and the error handling follows Python where .NET's own guidance agrees. Reversing 381 would look like B'. Under #3 the
serde container itself becomes `IDisposable` (row 461). The guard (M1) adds one check: a public `Close()` on a serde that
is not `IDisposable` throws at construction. Python's way: A and A'.

**Tests.** Existing disposal tests stay (`NT/Internal/SerdeDisposalTests.cs`, `NT/PublicSerializerDisposalTests.cs`,
`NT/PublicDeserializerDisposalTests.cs`). New: the `Close()` guard.

**Ledger and rules.** Row 464 (records that 377/381/383 hold, and the `Close()` guard). Rule draft R3 (resolves example 5).

---

### #7 Missing built-in types

**Today.** Java has 13 types plus `serdeFrom(Class)` and `ListSerde` (`J/ser/Serdes.java:159-315`). Python has
`bool` and `float(size=4)` but no Short, Bytes or List, and only a deserializer for buffers
(`P/common/serialization/_factories.py:76`, `:108-130`). .NET has 7 (`N/Common/Serialization/Serdes.cs:34-61`).

| Java | Bytes / messages (Java) | Python | Lean |
|---|---|---|---|
| `Boolean()` | 1 byte `01`/`00`; "Size of data received by BooleanDeserializer is not 1"; "Unexpected byte received by BooleanDeserializer: " + the byte, **signed** (`J/ser/BooleanDeserializer.java:37`, `:45`) | `bool_serializer` | add, `bool?` |
| `Float()` | 4 bytes, `floatToRawIntBits` (NaN payload kept, `FloatSerializer.java:29`); "Size of data received by Deserializer is not 4" on both paths (`FloatDeserializer.java:32`, `:50`) | `float_serializer(size=4)` | add, `float?` |
| `Short()` | 2 bytes; "Size of data received by ShortDeserializer is not 2" (`ShortDeserializer.java:33`, `:51`) | none | add, `short?` |
| `ByteBuffer()` | serializer writes the buffer; deserializer returns the buffer (`ByteBufferSerializer.java`, `ByteBufferDeserializer.java`) | `memoryview_deserializer` only | add as `ReadOnlyMemory<byte>?`; the deserializer copies (§6.4) |
| `Bytes()` | wraps `org.apache.kafka.common.utils.Bytes`, a `@Public` class .NET has not translated (`J/common/utils/Bytes.java:37-38`) | none | defer |
| `ListSerde(Class, Serde)` | flag + count + entries, two strategies, null index list; 58 Java tests | none | defer to its own phase |
| `serdeFrom(Class)` | picks a built-in by class; else `IllegalArgumentException` "Unknown class for built-in serializer. Supported types are: …" (`Serdes.java:205-206`) | none | add as `SerdeFrom<T>()` |

```csharp
// library
public static ISerde<bool?>  Boolean();
public static ISerde<float?> Float();
public static ISerde<short?> Short();
public static ISerde<ReadOnlyMemory<byte>?> ByteBuffer();
public static ISerde<T> SerdeFrom<T>();      // Java's Class<T> token is the type argument
```

```java
// Java user
Serde<Long> s = Serdes.serdeFrom(Long.class);
Serdes.serdeFrom(long.class);                 // IllegalArgumentException: Long.class is not assignable from long.class
```

```csharp
// .NET user
ISerde<long?> s = Serdes.SerdeFrom<long?>();
Serdes.SerdeFrom<long>();                     // IllegalArgumentException, as Java for a primitive class
```

`SerdeFrom<T>()` tests `typeof(T)` against each built-in in Java's order; no reflection. Its message: Java's text names
`Bytes`; while `Bytes()` is deferred, .NET's list leaves `Bytes` out (a recorded text deviation), or keeps Java's text
verbatim (D11 picks).

Options: *A (lean)* as the table; *B* only Boolean and Float (Python's set); *C* everything, Bytes and ListSerde
included (needs `Bytes` and a large List phase).

**Lean: A.** Additive. Re-evaluated ruling: ledger 403 D7 deferred a `ReadOnlyMemory<byte>` serde ("adding it later is
not breaking"). With #8 making every built-in class public, the pair is small, so add it now; the deserializer copies
the span into an array, as `ByteArray` does (`N/Internal/Serialization/ByteArraySerde.cs:30`), since a memory over the
batch cannot outlive it (§6.4). Whether to reverse D7 is D11. netstandard2.0 has no `BitConverter.SingleToInt32Bits`;
the Float classes reinterpret with `unsafe`, as the binding already does elsewhere. Python's way: B.

**Follow-up (Pranav, 2026-11-11): revisit `Bytes()` and `ListSerde()` after this phase** (§11, F1 and F2). Neither is
large. `Bytes()` first needs Java's public `org.apache.kafka.common.utils.Bytes` class (201 lines) translated with its
tests. `ListSerde()` is Java's `ListSerializer` / `ListDeserializer` (about 415 lines), its own design questions (the
list type as a generic parameter, the reflection its `configure` route needs, a home outside `Serdes` because of CS0102)
and 58 Java tests.

**Tests.** Java: `floatDeserializerShouldThrow…` ×3, `floatSerdeShouldPreserveNaNValues`, `testBooleanSerializer`,
`testBooleanDeserializer` (both booleans each), `booleanDeserializerShouldThrowOnEmptyInput`, `testSerdeFromUnknown`,
the Short/Float/ByteBuffer rows of `allSerdesShouldRoundtripInput` (`JT/SerializationTest.java:72`, `:107`, `:332-354`,
`:425-444`). New: byte vectors for each new type; Boolean's signed-byte message (`0xFF` → `-1`); `SerdeFrom<long>()` throws.

**Ledger and rules.** Row 465. Rule draft R1.

---

### #8 Java's public serializer and deserializer classes have no .NET form

**Today.** Java has 26 public classes (`J/ser/*Serializer.java`, `*Deserializer.java`), each `@InterfaceAudience.Public`.
Python keeps its classes private and exposes factories (root `CLAUDE.md`, Python Binding Conventions, Serialization:
"No others"). .NET's seven are internal (`N/Internal/Serialization/*.cs`). The name analyzer only checks types that
already have a .NET counterpart; its baseline kinds are `default`, `missing`, `overload`, `parameter`, `unmatched`
(`D/analyzers/java-name-baseline.txt`), so the 26 classes never show.

**Options.**

*A. Public, sealed, Java-named classes (lean).* A serializer and a deserializer per built-in shipped: 22 with #7's
lean (String, ByteArray, Integer, Long, Double, UUID, Void, Boolean, Float, Short, ByteBuffer), each with Java's public
no-argument constructor. Java's other four (Bytes and List) wait with #7. Sealed, as ledger 411 did for `SubscriptionPattern`. `WrapperSerde` stays
open (Java users subclass it).

```csharp
// library
public sealed class StringSerializer : ISerializerWithConfigure<string> { … }
public sealed class LongDeserializer : IDeserializer<long?> { … }
```

```java
new KafkaProducer<>(props, new StringSerializer(), new LongSerializer());
```

```csharp
new KafkaProducer<string, long?>(cfg, new StringSerializer(), new LongSerializer());
```

*B. Keep internal (Python's way).*

*Analyzer:* a "missing type" kind would list every Java public type with no .NET type, across every family, not just
serdes. Lean: a separate follow-up, not this phase; once the classes exist, the analyzer checks their members anyway.

**Lean: A.** Additive. Python's way: B.

**Tests.** A layout row per class (`PublicNamespaceLayoutTests`, as M22/P2 did); each class's bytes through its own
type and through `Serdes`.

**Ledger and rules.** Row 466. Rule draft R1.

---

### #9 No way to choose a string encoding

**Today.**
- Java: `StringSerializer.configure` reads `key.serializer.encoding` / `value.serializer.encoding`, then
  `serializer.encoding`, and throws `SerializationException("Unsupported encoding X")` for an unknown name
  (`J/ser/StringSerializer.java:36-50`; the deserializer side `StringDeserializer.java:39-53`). The UUID classes read the
  same keys and fail at use time with their own messages (`UUIDSerializer.java:35-55`, `UUIDDeserializer.java:38-77`).
- Python: the same keys through `configure`, plus `string_serializer(encoding=…)` (`_string_serializer.py:34`, `:37`;
  `_factories.py:82`), with Java's byte rules (`P/common/serialization/_encoding.py:15-27`: `UTF-16` big-endian after
  `FE FF`, none for an empty string; `UTF-32` big-endian; replacement on unmappable input).
- .NET: UTF-8 only (`N/Internal/Serialization/StringSerde.cs:29-33`, `GuidSerde.cs:31-32`). The keys are neither checked
  nor sent (ledger 356). Ledger 323 picked full Java charset parity inside the config-route phase that ledger 365 dropped.

**Options.**

*A. Java's keys through `Configure` (lean; needs #5, #8, #13).*

```csharp
// .NET user, option A (Java's call, Java's key)
var s = new StringSerializer();
s.Configure(new Dictionary<string, object?> { ["serializer.encoding"] = "UTF-16" }, isKey: false);
s.Serialize("t", "hi");                       // FE FF 00 68 00 69, as Java
```

Charset scope inside A:
- *A1. Full Java parity* (ledger 323 (a)): every Java charset name. Non-Unicode code pages need
  `CodePagesEncodingProvider`, and on netstandard2.0 the `System.Text.Encoding.CodePages` package, a new dependency
  (ledger 335).
- *A2 (lean). The Unicode family plus `US-ASCII` and `ISO-8859-1`*, with Java's names and aliases and Java's byte rules
  (BOM handling as Python's `_encoding.py`). Built into every target framework, no new dependency. Any other name throws
  `SerializationException("Unsupported encoding X")`, which for a real Java charset such as `windows-1252` is a recorded
  deviation. A1 can come later without a break.
- *A3. UTF-8 only*, every other name refused.

*B. Python's factory parameter:* `Serdes.String(Encoding encoding)`. Not Java's; .NET's `Encoding.Unicode` is
little-endian with no mark, not Java's `UTF-16`.

*C. Keep UTF-8 only, recorded.*

**Lean: A with A2.** Not breaking: the default stays UTF-8, and nothing changes until a user calls `Configure`.
Re-evaluated ruling: ledger 323's (a) belonged to the config route; with A it lives in `Configure`, Java's own home. A2
narrows 323 (a) on purpose to avoid a dependency now (D13). Python's way: A plus B.

**Tests.** Java's two encoding tests (`JT/SerializationTest.java:119`, `:136`); Python's byte vectors for UTF-16 and
UTF-32 (`PT/test_serialization.py`); the UUID classes' unsupported-encoding messages, the consumer-path text
("…deserializing ByteBuffer to UUID…", as Python, `_uuid_deserializer.py:68-70`).

**Ledger and rules.** Row 467. Rule draft R2.

---

### #10 UUID parsing accepts different input from Java

**Today.**
- Java: `UUID.fromString` (`J/ser/UUIDDeserializer.java:54`, `:70`), wrapped as
  `SerializationException("Error parsing data into UUID")` (`:58`, `:76`).
- Python: a port of `UUID.fromString` with Java's messages (`P/_java.py:259-280`), tested
  (`PT/test_serialization.py:357-372`).
- .NET: `Guid.Parse` (`N/Internal/Serialization/GuidSerde.cs:39`). Re-run here on SDK 10.0.400: `Guid` accepts
  `{…}`, `(…)`, 32 hex digits without dashes and space-padded text, all of which Java rejects, and rejects `1-1-1-1-1`,
  which Java reads as `00000001-0001-0001-0001-000000000001`.

**Options.** *A (lean):* an internal port of Java's parser (length check "UUID string too large", five groups, each
masked to its width, "Invalid UUID string: X"), throwing `IllegalArgumentException`, wrapped as today. *B:* keep
`Guid.Parse`, recorded.

```csharp
// library, internal
internal static Guid FromString(string name)   // java.util.UUID.fromString
{
    if (name.Length > 36) throw new IllegalArgumentException("UUID string too large");
    // five dash-separated groups, Long.parseLong(…, 16) each, masked to 32/16/16/16/48 bits
}
```

**Lean: A.** Behaviour change only: some inputs .NET accepted now fail, `1-1-1-1-1` now reads. Python's way: A.

**Tests.** Python's vectors (`PT/test_serialization.py:357-372`): uppercase accepted, `1-1-1-1-1`, the six bad inputs,
the cause's class and the "too large" message; plus the four `Guid`-only layouts refused.

**Ledger and rules.** Row 468.

---

### #11 The Void deserializer accepts data

**Today.** Java throws `IllegalArgumentException("Data should be null for a VoidDeserializer.")` for any non-null data
(`J/ser/VoidDeserializer.java:28-29`, `:36-37`); the consumer turns it into a `RecordDeserializationException`
(`CompletedFetch.java:325-326`). Python has no Void serde. .NET returns null for any input
(`N/Internal/Serialization/NullSerde.cs:29`), and a test asserts it (`NT/SerdesTests.cs:239`). Java's `Serde<Void>` is
`ISerde<object?>` in .NET, since C# cannot use `System.Void` as a type argument.

**Options.** *A (lean):* throw Java's exception and message. The consumer only calls a deserializer for a present field,
and a span cannot be null, so the .NET Void deserializer always throws when called; the consumer wraps it as
`RecordDeserializationException` (`ConsumerRecordsMarshal.cs:363-368`). *B:* keep lenient, recorded.

```csharp
public sealed class VoidDeserializer : IDeserializer<object?>
{
    public object? Deserialize(string topic, ReadOnlySpan<byte> data) =>
        throw new IllegalArgumentException("Data should be null for a VoidDeserializer.");
}
```

**Lean: A.** Behaviour change: a topic that should hold no values now fails where data appears, as in Java.
Re-evaluated recorded deviation: no reason was ever given (the code comment only states it), so it does not hold.
Python's way: none.

**Tests.** Java's `voidDeserializerShouldThrowOnNotNullValues` and `testSerializeVoid` (`JT/SerializationTest.java:372`,
`:386`); `testDeserializeVoid` (`:379`) passes null data, which a span cannot express: skipped with that reason.
`NT/SerdesTests.cs:239` changes to assert the throw.

**Ledger and rules.** Row 469.

---

### #12 The Double size error has the wrong text

**Today.** Java's `byte[]` overload says "Size of data received by Deserializer is not 8"
(`J/ser/DoubleDeserializer.java:33`); the `ByteBuffer` overload, which the consumer calls (`CompletedFetch.java:323`,
`:329`), says "…by DoubleDeserializer is not 8" (`:51`). Python uses the second (`_float_deserializer.py:24-25`). .NET
uses the first (`N/Internal/Serialization/DoubleSerde.cs:49-50`), asserted at `NT/SerdesTests.cs:268`.

**Options.** *A (lean):* a rule for every built-in: the message is the one of Java's consumer path (the `ByteBuffer`
overload). Only Double's text changes; Float's is "…by Deserializer is not 4" on both paths. *B:* keep.

**Lean: A.** Text only. Python's way: A.

**Tests.** `NT/SerdesTests.cs:264-268` changes; Java's float size tests carry Float's text.

**Ledger and rules.** Row 470 (with the rule). Rule draft R2.

---

### #13 Each `Serdes` member returns one shared instance

**Today.** Java builds a new serde per call (`J/ser/Serdes.java:229-308`, `return new LongSerde();`). Python's factories
build a new object per call (`_factories.py:85`, `:98`). .NET returns one static instance
(`N/Common/Serialization/Serdes.cs:25-31`; recorded at `D/CLAUDE.md:686`).

**Options.** *A (lean):* a new instance per call. *B:* keep shared; only safe while no built-in has state.

**Lean: A.** Needed once `Configure` (#5, #9) gives a built-in state, or one client's encoding leaks into every other
user. Not breaking (reference identity only; no repo test compares identity). Re-evaluated recorded deviation: no longer
holds once #5 lands. Python's way: A.

**Tests.** `Serdes.String()` returns a new object per call; configuring one does not change another.

**Ledger and rules.** Row 471. Rule draft R1.

---

### #14 Serdes come only from constructor arguments

**Today.** Java builds a serializer from `key.serializer` when none is given and configures it
(`J/clients/producer/KafkaProducer.java:398-403`), and has config-only constructors. Python resolves a dotted path and
falls back to the bytes serde (`P/producer/_base.py:447-464`). .NET takes serdes only as constructor arguments; null
throws Java's `ConfigException` text (ledgers 365, 397, 398; `N/Producer/KafkaProducer.cs:88-89`).

**Options.** *A (lean):* keep. *B:* add Java's config-only constructors later. With #8's public classes, a config route
limited to the built-ins needs no reflection: `key.serializer` given as `typeof(StringSerializer)` (a CLASS value the
config checker accepts, `D/CLAUDE.md` §4 Config) maps to `new StringSerializer()` through a fixed table, then
`Configure(configs, isKey)` (#5). A user class would need `Activator.CreateInstance`, which trimming and AOT dislike.

```csharp
// option B, later
new KafkaProducer<string, string>(new Dictionary<string, object?> {
    ["bootstrap.servers"] = "b:9092",
    ["key.serializer"]    = typeof(StringSerializer),
    ["value.serializer"]  = typeof(StringSerializer) });
```

**Lean: A.** No change. Re-evaluated ruling: ledger 365 still holds. Its reasons stand (no reflection, trimming and AOT
open, the Rust core makes the same choice), and B is additive, so keeping A forecloses nothing. What changed: #5 gives a
sync `Configure`, and #8 makes a reflection-free built-in table possible, so B is now cheaper if wanted. Python's way: B.

**Tests.** None.

**Ledger and rules.** Row 472 (records that 365 holds and how B would look).

---

### #15 Serializers run before the metadata wait

**Today.** Java waits for metadata (`J/clients/producer/KafkaProducer.java:1069`), then serializes (`:1080`). Python and
.NET serialize on the caller's thread, then the core waits (`P/producer/_base.py:536-547`;
`N/Producer/KafkaProducer.cs:128`, then `_native.Send`). So with an unknown topic and a throwing serializer, Java raises
`TimeoutException` after `max.block.ms` and .NET raises the serializer's error at once. Ledger 292 placed the sealing
after the serializers, as Python.

**Options.** *A (lean):* keep, record. *B:* reverse, which needs the core to wait before the binding serializes: a C ABI
change (Mode B).

**Lean: A.** No change. Re-evaluated ruling: the order holds; Python records the same deviation. With #2, a serializer
that adds headers also runs before the wait.

**Tests.** None.

**Ledger and rules.** Row 473 (record).

---

### #16 The wrong-type error names no config key

**Today.** Java: "Can't convert key of class X to class Y specified in key.serializer"
(`J/clients/producer/KafkaProducer.java:1082-1084`, `:1090-1092`), for a given instance too: Java puts the given
instance's class into `key.serializer` (`J/clients/producer/ProducerConfig.java:718-719`). .NET: "… specified as the key
serializer" (`N/Internal/SerializedProducerRecord.cs:94-96`), asserted at `NT/PublicProducerSerializerFailureTests.cs:254`,
`:273`, `:290`. Python does not wrap.

**Options.** *A (lean):* Java's text. *B:* keep.

**Lean: A.** Text only. Re-evaluated recorded deviation: its reason ("its serializers never come from a config key") is
wrong, because Java writes the same text for a given instance. Python's way: none.

**Tests.** The three assertions change.

**Ledger and rules.** Row 474.

---

### #17 No cluster or metrics hooks for serdes

**Today.** Java's serde Javadoc invites `ClusterResourceListener` and `Monitorable` (`J/ser/Serializer.java:29-33`); the
producer wires both (`J/clients/producer/KafkaProducer.java:404`, `:422-425`). Both are `@Public` Java types
(`D/analyzers/java-api.txt:2308`, `:3768`). Neither Python nor .NET has them.

**Options.** *A (lean):* out of scope, record. *B:* translate: `IClusterResourceListener` needs the cluster id from the
core, and `Monitorable` needs plugin metrics in the core; both are Mode B.

**Lean: A.** No change.

**Ledger and rules.** Row 475 (record).

---

### #18 MockConsumer takes deserializers, and AddRecord takes bytes

**Today.** Java's `MockConsumer(String)` has no deserializers, and `addRecord(ConsumerRecord<K,V>)` takes a typed record
(`J/clients/consumer/MockConsumer.java:101`, `:358`). Python's mock is a pure Python translation
(`P/consumer/mock_consumer.py:71`). .NET's mock takes deserializers and bytes, and deserializes on `Poll`
(`N/Consumer/MockConsumer.cs:64-70`, `:76`, `:286`), because it runs on the core's mock, which holds bytes
(`D/CLAUDE.md:526-532`; allowlist C1/M9, `D/analyzers/java-name-allowlist.txt:39-49`). .NET's `ConsumerRecord` has no
public constructor (`N/Consumer/ConsumerRecord.cs:60`), where Java's has several.

**Options.**
- *A (lean for this phase):* keep the recorded deviation.
- *B:* Java's shape on the native mock: `AddRecord(ConsumerRecord<TKey,TValue>)` keeps the typed record in a managed queue
  keyed by partition and offset, adds an empty placeholder to the core's mock to drive positions, and `Poll` returns the
  stored object; a constructor without deserializers. Needs a public `ConsumerRecord` constructor.
- *C (Python's way):* a pure managed mock, a translation of Java's `MockConsumer`.

```java
var c = new MockConsumer<String, Long>("earliest");
c.addRecord(new ConsumerRecord<>("t", 0, 0L, "k", 42L));
```

```csharp
// option B or C
var c = new MockConsumer<string, long?>("earliest");
c.AddRecord(new ConsumerRecord<string, long?>("t", 0, 0L, "k", 42L));
```

**Lean: A now; B or C in its own phase.** The reason recorded is still true of the core's mock, but Python shows the mock
need not be FFI-backed, so on the Java-shape goal the deviation no longer holds in principle. It is a mock and
`ConsumerRecord` change, not a serde change, and large, so it should not ride in this phase. D22 asks whether to plan it.

**Follow-up (Pranav, 2026-11-11): revisit option C** (§11, F3): a pure .NET translation of Java's `MockConsumer`
(`J/clients/consumer/MockConsumer.java`, 822 lines), as Python's mock is (`P/consumer/mock_consumer.py`), with a private
translation of the internals it uses (`SubscriptionState`) and Java's public `ConsumerRecord` / `ConsumerRecords`
constructors. It would take the deserializers and the byte-based `AddRecord` off both mock consumers, and the mock would
no longer depend on the core's mock. The 216 `AddRecord(bytes)` calls in 42 test files would move to typed records.

**Ledger and rules.** Row 476.

---

### #19 MockProducer requires both serializers

**Today.** Java's `MockProducer()` passes null serializers and `autoComplete` false (`J/clients/producer/MockProducer.java:151-153`);
the first `send` on an empty cluster dereferences them (`:316-317`). Its other constructors take a `Partitioner` and a
`Cluster` (`:115-143`), which .NET has not translated. Python maps a missing serializer to the bytes serde. .NET requires
both and throws `ArgumentNullException` (`N/Producer/MockProducer.cs:59-65`), with `autoComplete = true` (`:62`). Ruled:
mocks unchanged (ledger 330 (a), following 318 (a)).

**Options.** *A (lean):* keep. *B:* Java's no-argument constructor, with `autoComplete` false, failing at the first `Send`.

**Lean: A.** No change. Re-evaluated ruling: 330 holds. Java's no-argument mock fails at its first send on an empty
cluster, so it adds nothing a test can use, and Java's other constructors need types .NET lacks.

**Ledger and rules.** Row 477 (records that 330 holds).

---

### #20 Java's serde tests are not translated

**Today.** Java: `SerializationTest` (36 tests), `ListSerializerTest` (12), `ListDeserializerTest` (27)
(`JT/`). Python translates `SerializationTest` with Java's data and lists what it skips (`PT/test_serialization.py:1-35`).
.NET's `NT/SerdesTests.cs` has 26 tests of its own, not a translation.

**Lean.** A new `NT/SerializationTests.cs` translating Java's `SerializationTest`, test by test, with Java's data and
messages; `SerdesTests.cs` keeps its byte-vector tests that Java lacks. Each Java test lands in the step that makes it
translatable. With every lean picked:

| Java test (`JT/SerializationTest.java`) | Status |
|---|---|
| `allSerdesShouldRoundtripInput` (:72) | translated, less the `Bytes` row (#7) |
| `allSerdesShouldSupportNull` (:93) | serializer half translated (#1); a null span cannot be passed: the record path covers it |
| `testSerdeFromUnknown` (:107), `testSerdeFromNotNull` (:112) | translated (#3, #7) |
| `stringSerdeShouldSupportDifferentEncodings` (:119), `stringSerdeConfigureThrowsOnUnknownEncoding` (:136) | translated (#5, #9) |
| 19 `listSerde…` (:151-325), all of `ListSerializerTest`, `ListDeserializerTest` | skipped: no List serde (#7) |
| `floatDeserializerShouldThrow…` ×3 (:332-347), `floatSerdeShouldPreserveNaNValues` (:354) | translated (#7) |
| `testSerializeVoid` (:372), `voidDeserializerShouldThrowOnNotNullValues` (:386) | translated (#11) |
| `testDeserializeVoid` (:379) | skipped: passes null data, which a span cannot express |
| `stringDeserializerSupportByteBuffer` (:393) | translated over a span of a larger heap buffer and of native memory |
| `testBooleanSerializer`, `testBooleanDeserializer` (:425, :435, both values), `booleanDeserializerShouldThrowOnEmptyInput` (:444) | translated (#7) |

So of the 36: 14 fully, 2 in part, 20 skipped with a reason (19 List tests and `testDeserializeVoid`); plus the 39
tests of the two List test classes, skipped.

**Ledger and rules.** Row 478.

## 3. Cross-cutting: rule 10 and the serdes

**What rule 10 gives the serdes.** Java's serde interfaces have 9 default methods (Serializer: `configure`, the headers
`serialize`, `close`; Deserializer: `configure`, two headers `deserialize`, `close`; Serde: `configure`, `close`). With the
leans:

| Java default | .NET |
|---|---|
| `Serializer.serialize(String, Headers, T)` | `SerializerExtensions.Serialize` + `ISerializerWithHeaders<T>` |
| `Deserializer.deserialize(String, Headers, byte[])` and `(…, ByteBuffer)` | `DeserializerExtensions.Deserialize` + `IDeserializerWithHeaders<T>` (one span form) |
| `Serializer.configure`, `Deserializer.configure`, `Serde.configure` | `…Extensions.Configure` + `ISerializerWithConfigure<T>`, `IDeserializerWithConfigure<T>`, `ISerdeWithConfigure<T>` |
| `Serializer.close`, `Deserializer.close` | `IDisposable`, optional (rule 7; ledgers 377, 383) |
| `Serde.close` | `ISerde<T> : IDisposable` (row 461) |

Five .NET-only interfaces and three extension classes, each tied to a Java default, as rule 10 says.

**The Confluence page's example 5.** Page 6353289410 ("Java default methods in the .NET client (netstandard2.0)",
version 15), option C, example 5, shows `IDeserializerWithClose<T> { void Close(); }`. That conflicts with rule 7
(`Closeable` maps to `Dispose`, `D/CLAUDE.md:847-851`), with ledgers 377 and 383, and with the page's own last bullet.
Resolution: Java's `close()` overrides `Closeable.close()`, so it is not a rule-10 default in .NET; `IDisposable` itself is
the extra interface, and there is no `I…WithClose`. The example should read
`class OrderDeserializer : IDeserializerWithConfigure<Order>, IDisposable { … public void Dispose() => …; }`. Rule draft
R3 says this. I did not edit the page; Pranav may want to.

**The silent-default pitfall, for users.** The page lists as an accepted cost that a method written on the main interface
only silently runs Java's default, and suggests "a Roslyn analyzer shipped with the package could warn". The analyzer
does not ship. The lean (M1, row 459) is a construction-time check in the clients for the two forms the client itself calls
(the headers forms, and `Close()` without `IDisposable`). It is best effort under trimming and AOT. `Configure` is not
covered, because the client never calls it. The same guard would fit `IConsumerRebalanceListener.OnPartitionsLost` when the
listener moves to rule 10 (the two `default` baseline lines, `D/analyzers/java-name-baseline.txt:3-4`); that move is not in
this phase.

**The analyzer.** Internal only. It needs one change: accept N1's `With<Parameter>` names for an overloaded default
(`JavaNameAnalyzer.cs:303-345`, `:443-476`). Of the 34 serde baseline lines (`java-name-baseline.txt:274-307`), with every
lean: 26 are fixed, 4 are allowlisted with a reason (`Serializer#close`, `Deserializer#close`: optional `IDisposable`;
`deserialize(String,Headers,ByteBuffer)`: one span form; `Serdes#Serdes()`: a static class), and 4 stay as gaps (`Bytes()`,
`ListSerde(Class,Serde)`, `$BytesSerde`, `$ListSerde`). The new public classes may bring allowlist lines for Java's
per-class `ByteBuffer` overrides (one span form). The baseline only shrinks.

## 4. Dependencies and order

- **#3 first.** It changes every call site. Do #8 (public classes), #13 (new per call), #1 (nullable value types) and #4
  (annotations) in the same step, since they set the same types once. Users then change their code once.
- **#1 and #4 interlock.** `T?` alone leaves value types non-nullable; nullable built-ins give `T?` something to mean.
- **#2, #5 and #6 share rule 10.** #2 and #5 add the With interfaces and extension classes; #6 fixes what close is not.
  The naming rule (N1) and the guard (M1) go in S0's rules before code.
- **#5 needs #3 and #13; #9 needs #5 and #8.** Encodings live in `Configure` on the public String and UUID classes, which
  must not be shared.
- **#7 after #3/#8**, so the new types are born in the final shape.
- **Independent small fixes:** #10, #11, #12, #16.
- **No code:** #14, #15, #17, #19 (ledger rows only); #18 (a future phase).
- **#20** lands piece by piece with the step that makes each Java test translatable.

## 5. Steps an Actor would take (after approval)

dotnet-actor 124 and dotnet-critic 124, `dotnet/COMMENTS.124.md`, a Critic pass after every Actor commit (`D/CLAUDE.md`
§8). Gates per commit: the solution, the gRPC server and its tests, soak and perf build with 0 warnings / 0 errors;
`dotnet format --verify-no-changes`; UnitTests on net8.0 and net10.0; `cargo xtask dotnet-java-name-baseline` removes
lines and adds none. Close gate: `make verify-dotnet` (`D/CLAUDE.md` §7.5), or on macOS
`make verify-dotnet-macos-docker` and `make test-integration-dotnet-native`, as M22/P2 ran it.

- **S0 — rules.** Apply R1–R6 verbatim (`D/CLAUDE.md` §3, §4) and the allowlist group texts. No code.
- **S1 — the shape (#3, #8, #13, #1, #4).** `ISerde<T>` pair and `IDisposable`; `Serdes.WrapperSerde<T>` and the nested
  serdes; `Serdes.SerdeFrom(ser, deser)`; the public classes for the seven existing types; `int?`/`long?`/`double?`/`Guid?`;
  `T?` on the interfaces and `ConsumerRecord.Key()`/`Value()`; delete `N/Internal/Serialization/*Serde.cs`; migrate every
  call site (unit, integration and gRPC tests, gRPC server, soak, perf, package smoke). Commit type `feat(dotnet)!`.
  Java tests: `testSerdeFromNotNull`, `allSerdesShouldSupportNull` (serializer half).
- **S2 — small behaviour fixes (#10, #11, #12, #16).** The UUID parser port, the Void throw, the Double text, the cast
  text. Java tests: the two Void tests; Python's UUID vectors.
- **S3 — new built-ins (#7).** Boolean, Float, Short, ByteBuffer and `SerdeFrom<T>()`, with their public classes. Java
  tests: float ×4, boolean ×3, `testSerdeFromUnknown`, `allSerdesShouldRoundtripInput` (less Bytes).
- **S4 — headers (#2).** The two With interfaces and extension classes; the producers and consumers call the headers form
  through a type test cached at construction; the guard (M1) on all eight serde-taking classes for the headers forms and
  on the four real clients for `Close()`; the analyzer's N1 naming. Java test: the headers half of the round trip.
- **S5 — configure and encodings (#5, #9).** The three `…WithConfigure` interfaces and extension methods;
  `WrapperSerde.Configure`; String and UUID classes read Java's keys; the charset layer at the picked scope. Java tests: the
  two encoding tests.
- **S6 — close-out.** The remaining `SerializationTests.cs` skips with their reasons; baseline and allowlist final; the
  STATUS entry; `COMMENTS.DONE.124.md` archived in this folder.

Option: split into two phases, P3 = S0–S3 (shape and built-ins) and P4 = S4–S5 (rule-10 defaults). Lean: one phase, so
users change their serdes once and the interfaces settle together (D26).

## 6. Proposed ledger rows (text only; Pranav fills the ruling)

Numbering continues from row 454, the last in `DOTNET-PORT-DECISIONS.md`. All Pending.

| # | ID | Decision | My pick | Status | Ruling and notes | Date |
|---|---|---|---|---|---|---|
| 455 | new (PM 124, M23/P3 PLAN D1; review #1) | Null for the number and UUID built-ins. Java: "a serde for nullable Long" (`Serdes.java:229`); Python: `int \| None`. .NET: `ISerde<long>`, so absent = 0 | (A) replace: `Integer`/`Long`/`Double`/`UUID` (and #7's new ones) are `int?`/`long?`/`double?`/`Guid?`; a Java boxed type is a C# nullable value type; a user's own `IDeserializer<long>` still gets `default` (decision C). Breaking | Pending | | |
| 456 | new (PM 124, D2; review #2; rows 153, 404) | The headers forms. Java: default methods; Python: always passed | (A) rule 10: `SerializerExtensions.Serialize(…, Headers, …)` + `ISerializerWithHeaders<T>`, `DeserializerExtensions.Deserialize(…, Headers, …)` + `IDeserializerWithHeaders<T>`; all clients pass the record's own `Headers`, the type test cached at construction. Lifts the deferral of rows 153 and 404 | Pending | | |
| 457 | new (PM 124, D3; rule 10's open point) | The name of the extra interface of an overloaded default | (N1) `I<Interface>With<Parameter>`, the parameter the overload adds; one interface per overload; the parameter's type added on a collision; the analyzer learns it | Pending | | |
| 458 | new (PM 124, D4) | Java's two deserializer headers overloads (`byte[]`, `ByteBuffer`) | one `ReadOnlySpan<byte>` form; a keepable form, if §6.4's keep-alive ever lands, is a new interface | Pending | | |
| 459 | new (PM 124, D5) | A user-facing guard for the silent-default pitfall (the analyzer does not ship) | (M1) each serde-taking client checks once at construction: a public `Serialize(string, Headers, T)` / `Deserialize(string, Headers, ReadOnlySpan<byte>)` without the With interface throws `IllegalArgumentException` naming the interface; best effort under trimming and AOT, documented | Pending | | |
| 460 | new (PM 124, D6; review #3) | `ISerde<T>`: both directions or Java's pair | (A) Java's pair: `Serializer()` / `Deserializer()`, `Serdes.WrapperSerde<T>` (open), nested sealed `Serdes.<Type>Serde`, `Serdes.SerdeFrom(ser, deser)` with Java's two messages. Breaking (every call site) | Pending | | |
| 461 | new (PM 124, D7) | Is `ISerde<T>` itself `IDisposable` | yes: Java's `Serde extends Closeable`; `WrapperSerde.Dispose` disposes the serializer, then the deserializer | Pending | | |
| 462 | new (PM 124, D8; review #4) | Nullability of serde data and record fields | (A) `Serialize(string, T? data)`, `T? Deserialize(…)`, `ConsumerRecord.Key()`/`Value()` return `TKey?`/`TValue?` (Python's typing) | Pending | | |
| 463 | new (PM 124, D9; review #5; reverses row 321) | `configure(configs, isKey)` | (A) rule 10: `…Extensions.Configure` + `ISerializerWithConfigure<T>`, `IDeserializerWithConfigure<T>`, `ISerdeWithConfigure<T>`; configs `IReadOnlyDictionary<string, object?>`; the clients never call it (Java: config-built only). Row 321's premise (a config route) was removed by row 365 | Pending | | |
| 464 | new (PM 124, D10; review #6) | `close()` and dispose errors | rows 377, 381, 383 hold: optional `IDisposable` (rule 7), traced, never rethrown; no `I…WithClose` (the Confluence example 5 is corrected by R3); the guard (row 459) also throws for a public `Close()` on a serde that is not `IDisposable`, on the four real clients | Pending | | |
| 465 | new (PM 124, D11; review #7; row 403 D7) | Which built-ins to add | Boolean, Float, Short, ByteBuffer (`ReadOnlyMemory<byte>?`, the deserializer copies; reverses D7's "not now"), `SerdeFrom<T>()` (type argument for Java's `Class<T>`; its message leaves out `Bytes` while `Bytes()` is deferred); defer `Bytes()` and `ListSerde`, each a follow-up to revisit (§11, F1, F2) | Pending | | |
| 466 | new (PM 124, D12; review #8) | Java's public serde classes | (A) public, sealed (row 411's precedent), Java-named, Java's no-argument constructor, for every built-in shipped; an analyzer "missing type" kind is a separate follow-up | Pending | | |
| 467 | new (PM 124, D13; review #9; rows 323, 335, 356) | String and UUID encodings | (A) Java's keys through `Configure`, Java's byte rules (as Python's `_encoding.py`); scope (A2) the Unicode family, `US-ASCII`, `ISO-8859-1`, no new dependency; any other name `SerializationException("Unsupported encoding X")`, recorded; full parity (row 323 (a)) can follow | Pending | | |
| 468 | new (PM 124, D14; review #10) | UUID parsing | (A) port `java.util.UUID.fromString`, Java's messages, as Python's `uuid_from_string` | Pending | | |
| 469 | new (PM 124, D15; review #11) | The Void deserializer | (A) throw `IllegalArgumentException("Data should be null for a VoidDeserializer.")` | Pending | | |
| 470 | new (PM 124, D16; review #12) | Built-in messages | a built-in's message is Java's consumer-path (`ByteBuffer` overload) text; Double: "…by DoubleDeserializer is not 8" | Pending | | |
| 471 | new (PM 124, D17; review #13) | One shared instance or a new one per call | a new one per call, as Java and Python | Pending | | |
| 472 | new (PM 124, D18; review #14) | Config route | row 365 holds; a later built-in-only route (`typeof(StringSerializer)` through a fixed table, then `Configure`) is additive | Pending | | |
| 473 | new (PM 124, D19; review #15) | Serializers before the core's metadata wait | keep, recorded beside row 292 (same as Python); reversing needs a C ABI change | Pending | | |
| 474 | new (PM 124, D20; review #16) | The cast message | Java's "… specified in key.serializer" / "value.serializer" | Pending | | |
| 475 | new (PM 124, D21; review #17) | `ClusterResourceListener` / `Monitorable` for serdes | out of scope, recorded (Python lacks them; Mode B) | Pending | | |
| 476 | new (PM 124, D22; review #18) | The mock consumer's deserializers and bytes | keep in this phase; Java's typed mock (option B or C) in its own phase, with a public `ConsumerRecord` constructor; option C (pure .NET translation of Java's `MockConsumer`) is a follow-up to revisit (§11, F3) | Pending | | |
| 477 | new (PM 124, D23; review #19) | Mock producer serializers | row 330 holds | Pending | | |
| 478 | new (PM 124, D24; review #20) | Java's serde tests | translate `SerializationTest` into `SerializationTests.cs` as plan §2 #20 lists; the List tests skipped with #7 | Pending | | |
| 479 | new (PM 124, D25) | The rule drafts R1–R6 (plan §7) | approve verbatim, as drafted for the picks; a different ruling is redrafted before S0 | Pending | | |
| 480 | new (PM 124, D26–D27) | Phase shape and logistics | one phase M23/P3, S0–S6; N=124; branch `prashah_dev_dotnet_serdes_java_shape` from `37d8209cb`; dotnet-actor / dotnet-critic, a Critic pass after each commit; squashing decided at the end | Pending | | |

## 7. Rule drafts (for approval; not applied)

**R1 — `D/CLAUDE.md` §3, the Serdes sketch (replaces lines 670-686 of the code block at 669-689; the
`SerializationException` line stays).**

```csharp
public interface ISerializer<T> {
    ReadOnlyMemory<byte>? Serialize(string topic, T? data);           // Java Serializer<T>.serialize(String, T)
}
public interface IDeserializer<T> {
    T? Deserialize(string topic, ReadOnlySpan<byte> data);            // Java Deserializer<T>.deserialize — sync, span
}
public interface ISerde<T> : IDisposable {                            // Java Serde<T> extends Closeable
    ISerializer<T> Serializer();
    IDeserializer<T> Deserializer();
}
// Java's defaults (rule 10): ISerializerWithHeaders<T>, IDeserializerWithHeaders<T>, ISerializerWithConfigure<T>,
// IDeserializerWithConfigure<T>, ISerdeWithConfigure<T>, with SerializerExtensions, DeserializerExtensions, SerdeExtensions.

public static class Serdes {     // Java `Serdes`: a new serde per call, Java wire-format parity
    public class WrapperSerde<T> : ISerdeWithConfigure<T> { … }       // Java Serdes.WrapperSerde: one pair, configured and disposed together
    public sealed class StringSerde : WrapperSerde<string> { … }      // and one nested serde per built-in
    public static ISerde<string>  String();      // UTF-8 unless configured (Java's encoding keys)
    public static ISerde<byte[]>  ByteArray();
    public static ISerde<short?>  Short();
    public static ISerde<int?>    Integer();
    public static ISerde<long?>   Long();
    public static ISerde<float?>  Float();
    public static ISerde<double?> Double();
    public static ISerde<bool?>   Boolean();
    public static ISerde<Guid?>   UUID();        // ⚠ UUID.toString() as text, NOT 16 raw bytes
    public static ISerde<ReadOnlyMemory<byte>?> ByteBuffer();   // the deserializer copies (§6.4)
    public static ISerde<object?> Void();        // the deserializer throws for any data, as Java's
    public static ISerde<T> SerdeFrom<T>();
    public static ISerde<T> SerdeFrom<T>(ISerializer<T> serializer, IDeserializer<T> deserializer);
}
// Each built-in has its public, sealed Java class: StringSerializer, StringDeserializer, LongSerializer, …
```

**R2 — `D/CLAUDE.md` §3, the Serdes deviation bullets (replace "Headers overload deferred", "`ISerde<T>` added"; add
three).**

- **A Java boxed type is a C# nullable value type.** `Serdes.Long()` is a serde of `long?`, as Java's is "for nullable
  `Long`": null in, null out, and a keyless record or a tombstone is null, not `0`. A user's own deserializer of a
  non-nullable value type still gets `default(T)` for an absent field, which it is not called for (decision C).
- **Headers.** Java's headers overloads are rule 10's: `ISerializerWithHeaders<T>` and `IDeserializerWithHeaders<T>`,
  with extension methods running Java's default. The client tests the type once, at construction, and passes the record's
  own `Headers`: writable during the call, sealed after it on the real producers. Java's two deserializer overloads are
  one span form.
- **`ISerde<T>` is Java's pair.** `Serializer()` and `Deserializer()` hand out two objects; a client takes and disposes
  only the half it is given. `Serdes.WrapperSerde<T>` holds a pair, and `Serdes.SerdeFrom` builds one.
- **Encodings.** The String and UUID classes read Java's `*.serializer.encoding` keys in `Configure`, with Java's byte
  rules, for the Unicode charsets, `US-ASCII` and `ISO-8859-1`; any other name is refused *(deviation: Java accepts every
  charset. Why: no new dependency on netstandard2.0)*.
- **Messages.** A built-in's message is the one Java's consumer path throws (the `ByteBuffer` overload).

**R3 — `D/CLAUDE.md` §3, Java names rule 10 (replace its last sentence, line 871-872).**

> An overloaded default gets one interface per overload, named after the parameter the overload adds:
> `ISerializerWithHeaders<T>` for `serialize(String, Headers, T)`; when two overloads add a parameter of the same name, its
> type is added too. Java's `close()` is not a rule-10 default: `Closeable` is `IDisposable` (rule 7), which an implementer
> adds beside the interface, and there is no `I…WithClose`. Because the analyzer does not ship, each client that calls a
> rule-10 default checks the object it is given once: a public method of the default's name and parameters without the
> interface that replaces it, or a public `Close()` on a serde that is not `IDisposable`, throws
> `IllegalArgumentException` naming what to implement *(best effort: under trimming or Native AOT an uncalled method can
> be removed, and then the check passes)*.

**R4 — `D/CLAUDE.md` §3, a new idiom-map row.**

| Java | C# idiom | Rule / detail |
|---|---|---|
| a boxed `Long` / `Integer` / `Short` / `Double` / `Float` / `Boolean` / `UUID` as a serde's type | `long?` / `int?` / `short?` / `double?` / `float?` / `bool?` / `Guid?` | null in, null out, as Java's "nullable" serdes (§3 Serdes) |

**R5 — `D/CLAUDE.md:434`, the consumer sketch.** `public TKey? Key();  // deserialized (span → T); default(TKey) if
absent (deser NOT called — decision C)`, and the same for `Value()`.

**R6 — `D/CLAUDE.md:898`, the §4 Serializers row.** Replace "(both, the Java `Serde<T>` shape returned by the `Serdes`
factory)" with "(Java's pair: `Serializer()` / `Deserializer()`, returned new by each `Serdes` member; `WrapperSerde`,
`SerdeFrom`)", and add after the null model: "The headers forms and `Configure` follow §3 Java names rule 10; the client
calls the headers forms and never calls `Configure`, as Java never configures a given instance."

## 8. Decisions for Pranav

1. **D1 (#1):** nullable built-ins by replacing (lean A, breaking), adding beside (B) or keeping (C)?
2. **D2 (#2):** rule 10 (lean A), headers always (B, Python's), or both on the main interface (C)?
3. **D3 (#2):** the extra interface's name: `ISerializerWithHeaders<T>` (lean N1), `ISerializerWithSerialize<T>` (N2) or
   `ISerializerWithSerializeHeaders<T>` (N3)?
4. **D4 (#2):** one span form for Java's two deserializer headers overloads (lean yes)?
5. **D5 (#2):** the user-facing guard: throw at construction (lean M1), trace (M2), or docs only (M3)?
6. **D6 (#3):** Java's pair (lean A, breaking), "both" plus accessors (B), or keep (C)?
7. **D7 (#3):** `ISerde<T> : IDisposable` (lean yes)?
8. **D8 (#4):** `T?` annotations (lean A)?
9. **D9 (#5):** `Configure` under rule 10 (lean A, reverses row 321)?
10. **D10 (#6):** keep rows 377/381/383 (lean) and add the `Close()` guard? Or Java's rethrow from `Close()` (B')?
11. **D11 (#7):** add Boolean, Float, Short, ByteBuffer and `SerdeFrom<T>()` (lean); reverse D7 of row 403 for
    ByteBuffer? `SerdeFrom`'s message: drop `Bytes` or keep Java's text verbatim?
12. **D12 (#8):** public sealed classes (lean A) or internal (B, Python's)? The analyzer's "missing type" kind now or later
    (lean later)?
13. **D13 (#9):** encodings through `Configure` (lean A); charset scope A1 (full, new dependency), A2 (lean) or A3?
14. **D14–D17 (#10–#13):** port the UUID parser; Void throws; Double text; new instance per call (all lean A)?
15. **D18, D19, D21, D23 (#14, #15, #17, #19):** confirm rows 365, 292, 330 hold and #17 stays out (lean)?
16. **D20 (#16):** Java's cast text (lean A)?
17. **D22 (#18):** plan Java's typed mock consumer as its own phase (B on the native mock, or C pure managed)?
18. **D24 (#20):** translate `SerializationTest` into `SerializationTests.cs` (lean)?
19. **D25:** the rule drafts R1–R6 verbatim?
20. **D26:** one phase (lean) or two (shape and built-ins, then the rule-10 defaults)?
21. **D27:** M23/P3 or M24/P1; N=124?

## 9. Logistics

- Manager N=124; dotnet-actor 124, dotnet-critic 124, `dotnet/COMMENTS.124.md` (local, not committed).
- Branch `prashah_dev_dotnet_serdes_java_shape` from local master `37d8209cb`, in the main tree.
- Mode A: no Rust, C ABI, header, Python or proto change.
- This plan and its folder stay uncommitted until approval. `DOTNET-PORT-DECISIONS.md`, `D/CLAUDE.md`, the root
  `CLAUDE.md` and `.claude/rules/*` are untouched; §6 and §7 hold the proposed text.
- After approval: the approved plan is saved here; `COMMENTS.DONE.124.md` is archived here at the close (`D/CLAUDE.md` §8.4).

## 10. Facts that differ from the brief or the review page

- **The `Configure` ambiguity (CS0121).** It arises only from two extension classes alone (checked in a scratch compile:
  `Configure(this ISer<T>)` and `Configure(this IDes<T>)` called on a type extending both gives CS0121). Rule 10 also gives
  Java's own `Serde.configure` an extension on `ISerde<T>`, which is more specific and resolves the call (same scratch
  compile, no error). The real problem with "both directions" is the meaning, not the overload: one `Configure` body would
  serve the serializer's and the deserializer's keys.
- **The analyzer's naming.** Rule 10 leaves the overloaded default's name open, but the analyzer already derives
  `With<Method>` (`JavaNameAnalyzer.cs:334-337`, `:1190-1193`), so `ISerializerWithHeaders<T>` needs an analyzer change.
- **#16's reason.** The recorded reason ("its serializers never come from a config key") does not hold: Java writes
  "specified in key.serializer" for a given instance too (`ProducerConfig.java:718-719`).
- **Java's mock passes headers too** (`MockProducer.java:614-615`), so #2 reaches the mock producers.
- **`ProducerRecord.Key()` / `Value()` are already `TKey?` / `TValue?`** (`ProducerRecord.cs:130`, `:134`); only
  `ConsumerRecord` and the serde interfaces need #4.
- **Line moves since `15a613859`.** `MockConsumer.AddRecord` is at `MockConsumer.cs:286` (the page says `:279`); the
  serializer seals at `KafkaProducer.cs:136` (the page gives `:128-138`, still right). `SerdesTests.cs` has 26 tests (the
  page says 25).
- **`Serdes.Void()` is `ISerde<object?>`** where Java's is `Serde<Void>`; C# cannot use `System.Void` as a type argument.
- **`Serdes.ListSerde`** cannot be a nested class beside the `ListSerde(…)` method (CS0102, scratch compile), so a future
  List serde sits beside `Serdes` (rule 1).

## 11. Follow-ups (outside this phase; Pranav, 2026-11-11)

To revisit once this phase is done. None is started or planned here.

| # | Follow-up | From | What it needs |
|---|---|---|---|
| F1 | `Serdes.Bytes()` | #7 | Translate Java's public `org.apache.kafka.common.utils.Bytes` (`J/common/utils/Bytes.java`, 201 lines: a `byte[]` wrapper with value equality, hash code, lexicographic `compareTo`, `toString`, `increment`, `BYTES_LEXICO_COMPARATOR`) and its tests; then the serde pair and `SerdeFrom<T>()`'s `Bytes` case (and Java's full message text). Python has none. |
| F2 | `Serdes.ListSerde(…)` | #7 | Java's `ListSerializer` / `ListDeserializer` (`J/ser/`, 159 + 255 lines): two strategies and the null-index list; the list type as a generic parameter; whether to translate the `configure` route, which needs reflection (ledger 365 avoided it); a home outside `Serdes` (CS0102); 58 Java tests (`ListSerializerTest`, `ListDeserializerTest`, 19 in `SerializationTest`). Python has none. |
| F3 | Option C for the mock consumer: a pure .NET translation of Java's `MockConsumer` | #18 | Java's `MockConsumer` (`J/clients/consumer/MockConsumer.java`, 822 lines) on both mock consumers, as Python's pure translation (`P/consumer/mock_consumer.py`); a private translation of `SubscriptionState`; Java's public `ConsumerRecord` and `ConsumerRecords` constructors; no deserializers and a typed `AddRecord`; the 216 `AddRecord(bytes)` calls in 42 test files moved to typed records. Replaces allowlist C1/M9. |
