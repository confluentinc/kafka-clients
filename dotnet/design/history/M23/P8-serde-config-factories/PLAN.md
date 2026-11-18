# Plan: serializer and deserializer functions in the configuration

Status: owner decision of 2026-11-19 (the ten points under "Design"). The choices this plan adds are "Proposed"
ledger rows, for the owner to accept or change.

Milestone 23, Phase 8. Agent number N=130. Mode A (.NET only): no Rust, C ABI, header, Python or proto change, and
no new P/Invoke. Branch `esabellico/dotnet_async_serdes_and_zero_copy`, on top of M23/P7 (`53f219229`), in the
worktree `kafka-clients-dotnet-serdes`. No upstream; never pushed by an agent. All paths below are under `dotnet/`.

How it runs: a dotnet-actor 130 implements one step at a time and commits it with `--no-verify` after running the
step's tests (section "Gates per step"). No rule file is edited: the rule sentences this change makes stale are
listed with their replacement in "Draft rule text (not applied)", and until the owner applies them this plan wins
over the rule text it replaces.

## Context

Java's `KafkaProducer(Map)` and `KafkaConsumer(Map)` take no serializer arguments: they read the classes under
`key.serializer` / `value.serializer` (`key.deserializer` / `value.deserializer`), build an instance of each by
reflection and call `configure(configs, isKey)` on it. The .NET binding left these constructors out (ledger 365,
"constructor arguments only"): a C# constructor cannot await, and building a class by reflection closes trimming
and native AOT. So a .NET user must build each serde and, for a configurable one, call `Configure` by hand with the
same configuration the client gets.

The owner's decision of 2026-11-19 reverses that part of ledger 365 without reflection: the value under the key
is a **function that returns the serde**, and the client calls it. The client then configures the instance, as
Java configures the instance it builds, and disposes it at close, as Java closes it.

```csharp
var configs = new Dictionary<string, object?>
{
    ["bootstrap.servers"] = "localhost:9092",
    ["key.serializer"] = () => new StringSerializer(),
    ["value.serializer"] = () => new StringSerializer(),
    ["value.serializer.encoding"] = "UTF-16",
};
using var producer = new KafkaProducer<string, string>(configs);
```

## Design

### Public surface

```csharp
// Confluent.Kafka.Producer
public KafkaProducer(IReadOnlyDictionary<string, object?> configs);        // key.serializer: Func<ISerializer<TKey>>, value.serializer: Func<ISerializer<TValue>>
public AsyncKafkaProducer(IReadOnlyDictionary<string, object?> configs);   // Func<IAsyncSerializer<TKey>>, Func<IAsyncSerializer<TValue>>

// Confluent.Kafka.Consumer
public KafkaConsumer(IReadOnlyDictionary<string, object?> configs);        // key.deserializer: Func<IDeserializer<TKey>>, value.deserializer: Func<IDeserializer<TValue>>
public AsyncKafkaConsumer(IReadOnlyDictionary<string, object?> configs);   // Func<IAsyncDeserializer<TKey>>, Func<IAsyncDeserializer<TValue>>
```

Nothing else is public. The three-argument constructors, the mocks, the serde interfaces and the `Configure`
extension methods keep their signatures and behaviour.

### The ten points of the decision

1. **A function, not a class.** The value under the key is a parameterless delegate. It is accepted when
   `value is Func<ISerializer<TKey>>` holds (the interface of the client's kind and type argument). `Func<out T>` is
   covariant, so `() => new StringSerializer()` stored as an `object` (a `Func<StringSerializer>`) passes.
2. **No reflection.** No `Activator`, no `System.Type`, no class name. A value of another kind (a string, a
   `System.Type`, a function of another interface, a function returning a struct) is a bare `ConfigException` that
   names the key and the expected delegate type.
3. **A missing or null key** is Java's `ConfigException` "Invalid value null for configuration key.serializer:
   must be non-null.", the key before the value, as the first check
   (`ProducerConfig.appendSerializerToConfig`, `ConsumerConfig.appendDeserializerToConfig`). The existing
   `NativeProducer.RequireSerializer` / `NativeConsumer.RequireDeserializer` make it.
4. **`Configure(configs, isKey)`** is called on an instance iff it implements `ISerializerWithConfigure<T>` /
   `IAsyncSerializerWithConfigure<T>` / `IDeserializerWithConfigure<T>` / `IAsyncDeserializerWithConfigure<T>`: the
   key with `true`, then the value with `false`, once each. A serde given as a constructor argument is still never
   configured. `configs` is a copy of the user's configuration (ledger 561).
5. **A failure while the serdes are built** (the function returns null or throws, `Configure` throws) is a
   `KafkaException` "Failed to construct kafka producer" / "Failed to construct kafka consumer" with the cause as
   `InnerException`, as Java's constructor wraps what its `try` throws. What was already built is disposed.
6. **Order.** The binding's configuration checks and the core's configuration validation come first, as bare
   `ConfigException`s, before any function runs.
7. **Disposal.** A serde the client built is disposed at close exactly as a given one (`SerdeDisposal`; on an
   asynchronous client `IAsyncDisposable` first, else `IDisposable`), and `SerdeGuard.Check` applies to it.
8. **The five own-role keys still never reach the core**, and the three-argument constructors keep ignoring them.
9. **The mocks are unchanged**: Java's mocks have no config-only constructor.
10. **The route really configures**: `["key.serializer"] = () => new StringSerializer()` with
    `["key.serializer.encoding"]` set produces bytes in that encoding, and likewise `AsyncStringSerializer` and the
    deserializers.

### What the code looks like today (findings)

- `KafkaProducer` / `AsyncKafkaProducer` (`src/Confluent.Kafka/Producer/`) and `KafkaConsumer` /
  `AsyncKafkaConsumer` (`src/Confluent.Kafka/Consumer/`) each have one public constructor
  `(configs, key serde, value serde)`. It null-checks `configs`, requires both serdes
  (`RequireSerializer` / `RequireDeserializer`: Java's "must be non-null." message), runs `SerdeGuard.Check`, and
  calls `NativeProducer.Create` / `NativeConsumer.Create` with a `SerdeDisposal`.
- `NativeProducer.Create` / `NativeConsumer.Create` (`Internal/`) call `ClientConfigs.Prepare` (type checks, text
  conversion; it skips the five own-role serde keys) and then `ConfigMarshal.NewClient`.
- `ConfigMarshal.NewClient` (`Internal/Interop/ConfigMarshal.cs`) makes two native calls in a row: the core's config
  constructor (`kafka_producer_ProducerConfig_new` / `kafka_consumer_ConsumerConfig_new`), which validates the
  configuration and fails bare, and the client constructor, which wraps what it throws in "Failed to construct
  kafka …" (the consumer's `group.protocol=classic` refusal excepted: it is a bare
  `UnsupportedOperationException`).
- `SerdeDisposal` (`Internal/SerdeDisposal.cs`) disposes a pair once, key then value. `Create` disposes the given
  serdes when the core fails with the wrapped message and not for a bare error (ledger 447).
- The interface test for `Configure` already exists: the public extension methods `SerializerExtensions.Configure`,
  `AsyncSerializerExtensions.Configure`, `DeserializerExtensions.Configure` and
  `AsyncDeserializerExtensions.Configure` call `Configure` iff the instance implements the `…WithConfigure<T>`
  interface. The clients reuse them.
- `DefaultMethodGuard.TypeText` writes a type as C# does
  (`System.Func<Confluent.Kafka.Common.Serialization.ISerializer<string>>`). The new messages use it.
- No FFI function returns the client id the core resolved for a producer or a consumer.
- `analyzers/java-name-baseline.txt` lists `KafkaProducer#KafkaProducer(Map)` and `KafkaConsumer#KafkaConsumer(Map)`
  as "missing" for the four real clients (lines 217, 275, 292, 315). A baseline line that matches no finding is a
  build warning (CKRDA0014), so each line goes in the step that adds its constructor.

### Java's order, and the binding's

Java's `KafkaProducer(Map<String, Object> configs)` is `this(configs, null, null)`:

1. `ProducerConfig.appendSerializerToConfig`: a missing `key.serializer`, then a missing `value.serializer`, is a
   bare `ConfigException`.
2. `new ProducerConfig(…)`: every key is parsed and validated; bare `ConfigException`.
3. Inside the constructor's `try`: the key serializer is built and `configure(config.originals(clientId), true)` is
   called, then the value serializer with `false`; later the rest of the client. Anything thrown is caught, the
   producer is closed with a zero timeout, and `KafkaException("Failed to construct kafka producer", t)` is thrown.

`KafkaConsumer(Map)` is the same with `ConsumerConfig.appendDeserializerToConfig` and
`Deserializers(config, null, null, metrics)` inside `AsyncKafkaConsumer`'s `try`
("Failed to construct kafka consumer").

The binding's order for a config-only constructor:

| # | Check | Result |
|---|---|---|
| 1 | `configs` is null | `ArgumentNullException("configs")`, as the three-argument constructor |
| 2 | `key.*` missing or null, then `value.*` | bare `ConfigException`, Java's message (Java step 1) |
| 3 | `key.*` is not a function of the client's interface, then `value.*` | bare `ConfigException` (ledger 565, 567) |
| 4 | `ClientConfigs.Prepare`: the other keys' types, a null key | bare `ConfigException` (Java step 2) |
| 5 | the core's config constructor: values, ranges | bare `ConfigException` (Java step 2) |
| 6 | key function, null check, `SerdeGuard.Check`, `Configure(configs, true)`; then the value with `false` | "Failed to construct kafka …" with the cause inside (Java step 3) |
| 7 | the core's client constructor | "Failed to construct kafka …"; `group.protocol=classic` bare (ledger 564) |

Steps 6 and 7 are Java's `try`. From step 6 on, a failure disposes every serde the client built so far, key then
value, through the same `SerdeDisposal` the close uses (ledger 562). The functions run between the two native
calls of `ConfigMarshal.NewClient`, so the whole configuration is validated before any user function runs and no
native client exists yet when one fails: nothing native has to be torn down (ledger 568).

### Messages

| Case | Exception | Message |
|---|---|---|
| key missing or null | `ConfigException` | `Invalid value null for configuration key.serializer: must be non-null.` (`value.serializer`, `key.deserializer`, `value.deserializer` likewise) |
| wrong kind of value | `ConfigException` | `Invalid value <value> for configuration key.serializer: Expected value to be a System.Func<Confluent.Kafka.Common.Serialization.ISerializer<string>>, but it was a <type of the value>` — `<value>` as `ConfigException` prints any value, both types as `DefaultMethodGuard.TypeText` writes them |
| function returns null | `KafkaException` | `Failed to construct kafka producer`; `InnerException` is a `ConfigException` `Invalid value null for configuration key.serializer: Expected the function to return a Confluent.Kafka.Common.Serialization.ISerializer<string>, but it returned null` (ledger 566) |
| function throws, `Configure` throws, `SerdeGuard.Check` refuses | `KafkaException` | `Failed to construct kafka producer` / `Failed to construct kafka consumer`; `InnerException` is what was thrown |

### Implementation

One new file, `src/Confluent.Kafka/Internal/ConfiguredSerdes.cs`:

- `ConfiguredSerdes<TKeySerde, TValueSerde>`: built from the user's `configs`, the two key names, the client's
  "Failed to construct" message, the disposal kind (synchronous or asynchronous) and one "initialize" delegate per
  serde (`SerdeGuard.Check`, then the `Configure` extension method). Its constructor makes checks 2 and 3 and keeps
  the two functions. `Build()` is step 6: it returns the `SerdeDisposal` of the built pair, or disposes what it
  built and throws the wrapped `KafkaException`. `Key` and `Value` give the built instances.
- A static `ConfiguredSerdes` with four factories (`Serializers`, `AsyncSerializers`, `Deserializers`,
  `AsyncDeserializers`), which name the keys, the message and the initialize methods of each client kind.

Changes to existing files:

- `Internal/Interop/ConfigMarshal.cs`: `NewClient` takes an optional `Action? configured`, called between the config
  constructor and the client constructor.
- `Internal/NativeProducer.cs`, `Internal/NativeConsumer.cs`: `Create` takes an optional `Func<SerdeDisposal>? build`
  that it passes on as that hook; the serdes it returns are the client's, and are disposed on every later failure
  of `Create`, bare ones included.
- `Internal/SerdeDisposal.cs`: a pair may have a null member (a value that was never built), which is skipped.
- `Internal/ClientConfigs.cs`: the comment on the skipped keys.
- The four client classes: the new constructor, and the XML docs of the existing one, which say the keys cannot be
  set.
- `Common/Serialization/I*Serializer.cs`, `I*Deserializer.cs`: the remarks "No producer calls it" /
  "No consumer calls it" on `Configure`.

The hot paths (`Send`, `Poll`) do not change: a built serde is stored in the same fields a given one is.

## Steps

One commit per step, `--no-verify`, no `Co-Authored-By`, only files under `dotnet/`.

- **S0 — plan.** This file.
- **S1 — producers.** `ConfiguredSerdes`, the hook in `ConfigMarshal.NewClient`, `NativeProducer.Create`,
  `KafkaProducer(configs)`, `AsyncKafkaProducer(configs)`, their XML docs and the stale docs of the serializer
  interfaces, the two producer lines of `java-name-baseline.txt`, unit tests.
- **S2 — consumers.** `NativeConsumer.Create`, `KafkaConsumer(configs)`, `AsyncKafkaConsumer(configs)`, docs, the
  two consumer lines of `java-name-baseline.txt`, unit tests.
- **S3 — docs and the broker test.** A short `README.md` section with one example per client kind; the other
  analyzer lists if they need a change; one integration test that produces and consumes through the config-only
  constructors, synchronous and asynchronous, on the existing Testcontainers fixture.

## Tests

Unit tests (`tests/Confluent.Kafka.UnitTests`), with exact messages, for each of the four clients:

- each function is called exactly once;
- `Configure` is called once with `isKey` true for the key and false for the value, the key first, with the
  configuration's entries;
- an instance without the `…WithConfigure<T>` interface is not configured and nothing fails;
- a serde given as a constructor argument is not configured, and the three-argument constructor ignores the keys;
- a missing key and an explicit null: Java's message, the key before the value, and before a wrong kind of value;
- wrong kinds (a string, a `System.Type`, a function of the wrong interface): the exact `ConfigException`;
- the function returns null, the function throws, `Configure` throws: "Failed to construct kafka …" with the inner
  exception, and the key serde already built is disposed;
- a bad configuration value (`batch.size = "hello"`) is the bare `ConfigException` and no function runs;
- the built serdes are disposed by `Dispose()`, and by `DisposeAsync()` on the asynchronous clients;
- a covariant delegate (`Func<StringSerializer>`) is accepted;
- the encoding test of point 10.

The tests reuse the existing fixtures (`UnreachableProducers`, `TestTimeout`, the disposal test serdes) and build
real clients against an unreachable bootstrap address, as the existing constructor tests do.

## Verification

Before each commit: `dotnet build -c Release -p:CargoProfileDir=debug` of the solution with 0 warnings and 0
errors (CKRDA0013 / CKRDA0014 and the trimming and AOT analyzers included; no new
`[UnconditionalSuppressMessage]`), the unit tests on net8.0 and net10.0 (baseline 9,526 passed, 51 skipped, 0
failed per framework), the analyzer tests (245 per framework), `dotnet format --verify-no-changes`. net462: the
library and the tests build; they cannot run on the development host.

At the end: the integration tests (Docker; 29 per framework before this phase), and the gRPC server tests and
`SoakClient.Tests` if anything they use changed.

## Out of scope

- `KafkaProducer(Properties)` / `KafkaConsumer(Properties)`: the binding has no `Properties` form (§4 **Config**).
- The mocks.
- The other settings Java fills by reflection (`partitioner.class`, `interceptor.classes`, `metric.reporters`, …).
- Any FFI change, the resolved `client.id` included (ledger 561).
- A function that returns a `Task`: a constructor cannot await it.

## Gates per step

| Step | Build | Unit tests | Analyzer tests | Integration | Format |
|---|---|---|---|---|---|
| S0 | — | — | — | — | — |
| S1 | yes | net8.0, net10.0 | yes | — | yes |
| S2 | yes | net8.0, net10.0 | yes | — | yes |
| S3 | yes | net8.0, net10.0 | yes | yes | yes |

## Ledger rows

Rows 553–560 are the owner's decision of 2026-11-19. Rows 561–569 are this plan's own choices and deviations.

| # | Source | Topic | Decision | Status |
|---|---|---|---|---|
| 553 | Owner 2026-11-19 | Config-only constructors | the four real clients get `(IReadOnlyDictionary<string, object?> configs)`, Java's `KafkaProducer(Map)` / `KafkaConsumer(Map)`; reverses the "constructor arguments only" part of ledger 365 for serializers and deserializers | Agreed |
| 554 | Owner 2026-11-19 | Value of `key.serializer` and its peers | a parameterless function returning the serde, tested with `is Func<I…<T>>` (delegate covariance); no reflection, no `System.Type`, no class name; any other value is a `ConfigException` naming the key and what is expected | Agreed |
| 555 | Owner 2026-11-19 | Missing or null key | Java's bare `ConfigException` "Invalid value null for configuration key.serializer: must be non-null.", the key before the value, as the first check | Agreed |
| 556 | Owner 2026-11-19 | `Configure` | called iff the built instance implements the `…WithConfigure<T>` interface, key (`true`) then value (`false`), once each; a serde given as a constructor argument is never configured | Agreed |
| 557 | Owner 2026-11-19 | Failure while building | the function returns null or throws, or `Configure` throws: `KafkaException` "Failed to construct kafka producer" / "… consumer" with the cause inside; what was built is disposed, and the native client destroyed if it exists | Agreed |
| 558 | Owner 2026-11-19 | Order | the binding's configuration checks and the core's validation come first, bare, before the functions run where possible | Agreed |
| 559 | Owner 2026-11-19 | Disposal and guard | a built serde is disposed at close as a given one, and `SerdeGuard.Check` applies to it | Agreed |
| 560 | Owner 2026-11-19 | Unchanged | the five own-role keys never reach the core; the three-argument constructors and the mocks do not change | Agreed |
| 561 | Actor 130 | The `configs` given to `Configure` | a fresh copy of the user's configuration for each call, serde keys included, as Java's `originals()` copies; it does not carry the `client.id` the core resolves, which Java adds (`config.originals(singletonMap(CLIENT_ID_CONFIG, clientId))`). Why: no FFI function returns the resolved id. Goes when: the C ABI returns the client id of a producer and a consumer | Proposed |
| 562 | Actor 130 | Disposal on a construction failure | once a function has run, any failure of the constructor disposes every serde the client built, key then value, the one whose `Configure` threw included. Java closes less: a serializer whose `configure` throws is not yet assigned and is not closed, and a failing `Deserializers` constructor leaves the key deserializer open. Why: the user holds no reference to a built serde, so nobody else can dispose it | Proposed |
| 563 | Actor 130 | `SerdeGuard.Check` on a built serde | its `IllegalArgumentException` is the `InnerException` of "Failed to construct kafka …", where a given serde's is thrown bare. Why: the instance exists only inside Java's `try` | Proposed |
| 564 | Actor 130 | A bare failure of the core's client constructor | the consumer's `group.protocol=classic` refusal (`UnsupportedOperationException`) comes after the functions ran; it stays bare and the built deserializers are disposed, where ledger 447 leaves given ones alone on a bare error. Why: the binding repeats no core check to refuse it earlier. Goes when: the core's config constructor refuses it | Proposed |
| 565 | Actor 130 | A value of the wrong kind | a bare `ConfigException` with the binding's own text "Expected value to be a System.Func<…>, but it was a …". Java has no such case: a class name it cannot load is a bare `ConfigException` "Class … could not be found.", and a class of another interface is a wrapped `KafkaException` "… is not an instance of …" | Proposed |
| 566 | Actor 130 | A function that returns null | the inner exception is a `ConfigException` for the key with the value `null`: "Expected the function to return a …, but it returned null". Java cannot have the case | Proposed |
| 567 | Actor 130 | Position of the kind check | after Java's missing-key check and before the other keys' type checks; Java's place for a CLASS type error is its `ConfigDef` order, which no other binding check follows either | Proposed |
| 568 | Actor 130 | Where the functions run | between the core's config constructor and its client constructor, through an optional hook of `ConfigMarshal.NewClient`; no new FFI entry point, and no native client exists when a function fails | Proposed |
| 569 | Actor 130 | net462 | the library and the unit tests build for net462; no test of this phase ran on it on the development host | Open |

## Draft rule text (not applied)

No rule file was edited. Each section gives the file and line, the sentence that becomes stale, and its
replacement.

### R1 — `dotnet/CLAUDE.md:958`, §4 status table, row **Serializers**: "the client … never calls `Configure`"

Stale: "The headers forms and `Configure` follow §3 Java names rule 10; the client calls the headers forms and never
calls `Configure`, as Java never configures a given instance."

Replacement: "The headers forms and `Configure` follow §3 Java names rule 10; the client calls the headers forms.
It calls `Configure` only on a serde it built from the configuration (**From the configuration**, below), never on
one it was given, as Java never configures a given instance."

### R2 — `dotnet/CLAUDE.md:958`, the same row: "**Constructor arguments only** (ledger 365) … The Rust core makes the same choice.)*"

Stale: the passage from "**Constructor arguments only** (ledger 365): Java's config-only constructors
`KafkaProducer(Map)`, `KafkaProducer(Properties)`, `KafkaConsumer(Map)` and `KafkaConsumer(Properties)` are not
translated, and nothing is built by reflection" to "The Rust core makes the same choice.)*".

Replacement: "**From the configuration** (M23/P8, ledgers 553–568): the four real clients also have Java's
config-only constructor, `KafkaProducer(configs)` / `KafkaConsumer(configs)` (`Map`; there is no `Properties` form,
§4 **Config**). Under `key.serializer` / `value.serializer` (consumer: `key.deserializer` / `value.deserializer`)
the configuration holds a **function that returns the serde**, a `Func<ISerializer<TKey>>` on `KafkaProducer`, a
`Func<IAsyncSerializer<TKey>>` on `AsyncKafkaProducer`, and the deserializer interfaces on the consumers; a
delegate returning a class that implements the interface is accepted, as `Func<out T>` is covariant. Nothing is
built by reflection: a string, a `System.Type` or a function of another interface is a bare `ConfigException`
"Expected value to be a System.Func<…>, but it was a …". The order is Java's: a missing or null key (the key, then
the value) is Java's "Invalid value null for configuration key.serializer: must be non-null."; then the binding's
type checks and the core's validation of the configuration, all bare; then, as inside Java's constructor `try`, the
key function runs and the client calls `Configure(configs, true)` on the instance if it implements the
`…WithConfigure<T>` interface, then the value with `false`, and the core builds the client. `configs` is a copy of
the user's configuration *(deviation: without the `client.id` the core resolves, which Java adds. Goes when: the C
ABI returns it, ledger 561)*. A function that returns null or throws, a `Configure` that throws, or a
`SerdeGuard` refusal is a `KafkaException` "Failed to construct kafka producer" / "… consumer" with the cause as
`InnerException`. A serde given as a constructor argument is still never configured, and the mocks have no
config-only constructor, as Java's. Each other setting Java fills by reflection (`partitioner.class`,
`interceptor.classes`, `metric.reporters`, `config.providers`, the SASL and OAuth handler classes,
`ssl.engine.factory.class`) gets, when it is translated, a setter on `ProducerConfig` / `ConsumerConfig` that takes
an object the user built, as the Rust core's `ProducerConfig::set_partitioner` does. *(Deviation from Java: a
function where Java takes a class or a class name. Why: without reflection, trimming and native AOT stay open, and
the user's function can pass constructor arguments. A function cannot return a `Task`, since a C# constructor
cannot await; a serde that needs asynchronous setup is built by the user and passed as an argument.)*"

### R3 — `dotnet/CLAUDE.md:958`, the same row: "**Disposal** (M22/P2): the four real clients dispose each serializer or deserializer they were given"

Stale: "the four real clients dispose each serializer or deserializer they were given that is `IDisposable`, once,
key then value, after the destroy (§4 **Close/Dispose**, step 6), and when the core fails to build the client with
Java's wrapped "Failed to construct kafka producer" / "… consumer", not for an error it returns bare, such as a
configuration value it cannot parse, which Java raises before its constructor's `try` (ledger 447)".

Replacement: "the four real clients dispose each serializer or deserializer they were given, or built from the
configuration, that is `IDisposable`, once, key then value, after the destroy (§4 **Close/Dispose**, step 6), and
when the core fails to build the client with Java's wrapped "Failed to construct kafka producer" / "… consumer",
not, for a given one, for an error it returns bare, such as a configuration value it cannot parse, which Java
raises before its constructor's `try` (ledger 447). A serde the client built is disposed on every failure of the
constructor after its function ran, the bare `group.protocol=classic` refusal included, since the user holds no
reference to it (ledgers 562, 564)".

### R4 — `dotnet/CLAUDE.md:959`, §4 status table, row **Config**: "nothing reads them"

Stale: "The five own-role serializer keys (`key.serializer`, `value.serializer`, `serializer.encoding`,
`key.serializer.encoding`, `value.serializer.encoding`; consumer: the deserializer ones) are neither checked nor
sent, as in Python; nothing reads them, since a serializer is a constructor argument only (§4 **Serializers**,
ledger 365)."

Replacement: "The five own-role serializer keys (`key.serializer`, `value.serializer`, `serializer.encoding`,
`key.serializer.encoding`, `value.serializer.encoding`; consumer: the deserializer ones) are never sent to the
core, as in Python. The config-only constructor reads `key.serializer` and `value.serializer` as functions and
hands the whole configuration, the encoding keys included, to the `Configure` of the serdes it builds (§4
**Serializers**, ledger 553); the three-argument constructor ignores all five."

### R5 — `dotnet/CLAUDE.md:959`, the same row: "a `System.Type` for CLASS"

Stale: "(… an `IList` for LIST; a `System.Type` for CLASS)". The sentence stays true for the CLASS keys the type
check covers; it needs one addition so it is not read as covering the serde keys.

Replacement: "(… an `IList` for LIST; a `System.Type` for CLASS, the client's own serializer and deserializer keys
excepted, whose value is a function, §4 **Serializers**)".

### R6 — `dotnet/CLAUDE.md:837-841`, the bullet "**Consumer deserializers.**"

Stale: "The config takes no classes, so the consumer has no `key.deserializer` / `value.deserializer` route: a null
deserializer throws Java's `ConfigException` "Invalid value null for configuration key.deserializer: must be
non-null." (the value likewise), on all four consumers, the mocks included *(Java's `MockConsumer` takes no
deserializers)*."

Replacement: "The config takes no classes. A deserializer is a constructor argument or, on `KafkaConsumer` and
`AsyncKafkaConsumer`, a function under `key.deserializer` / `value.deserializer` read by the config-only
constructor (§4 **Serializers**). A null deserializer argument, and a missing or null key on the config-only
constructor, throws Java's `ConfigException` "Invalid value null for configuration key.deserializer: must be
non-null." (the value likewise), on all four consumers, the mocks included *(Java's `MockConsumer` takes no
deserializers, and no mock has a config-only constructor)*."

### R7 — `dotnet/README.md:82` (not a rule file; edited in S3)

"A client takes one serializer or deserializer for keys and one for values, as constructor arguments." gains the
configuration route and a short section with one example per client kind.
